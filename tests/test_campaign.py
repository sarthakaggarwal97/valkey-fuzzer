"""Campaign generation, coverage accounting, and minimization."""

import copy
import json

import pytest

from tests.valkey_packs_support import valkey_runtime
from valkey_fuzzer.campaign import (
    Campaign,
    CampaignError,
    CoverageDB,
    coverage_key,
    failure_signature,
    minimize,
    reduction_candidates,
    run_campaign,
    same_failure,
    static_tuple,
)
from valkey_fuzzer.extensibility import (
    ComponentKind,
    ComponentResult,
    ComponentSpec,
    ComponentStatus,
    Evidence,
    EvidenceRequirement,
    OutcomeStatus,
    RunOutcome,
    RunPlan,
    TopologyProviderIdentity,
)

PROVIDER = TopologyProviderIdentity("valkey-fuzzer.local-topology", "1", "v1")


def _plan(topology="standalone", inputs=None, document=None, fault_event=None):
    components = [
        ComponentSpec(kind=ComponentKind.ACTION, component_id="valkey.bgsave", instance_id="save"),
        ComponentSpec(kind=ComponentKind.ORACLE, component_id="valkey.evidence.assert", instance_id="check"),
    ]
    if fault_event:
        components.append(
            ComponentSpec(
                kind=ComponentKind.FAULT,
                component_id="valkey.process_signal",
                instance_id="kill",
                config={"when_event": fault_event},
            )
        )
    return RunPlan(
        scenario_id="c",
        pack_id="tests.pack",
        pack_version="1",
        topology_provider=PROVIDER,
        seed=1,
        topology=topology,
        components=tuple(components),
        inputs=inputs or {},
        required_evidence=(EvidenceRequirement("tests.path"),),
        topology_document=document or {"type": topology},
    )


def _outcome(plan, status=OutcomeStatus.PASS, failing=(), evidence=("tests.path",), missing=()):
    results = tuple(
        ComponentResult(component_instance_id=instance, status=ComponentStatus.FAIL, summary="x")
        for instance in failing
    )
    diagnostics = {"missing_required_evidence": [{"evidence_id": item} for item in missing]} if missing else {}
    return RunOutcome(
        plan=plan,
        status=status,
        summary=status.value,
        component_results=results,
        evidence=tuple(Evidence(item, "save", "p", 1.0, True) for item in evidence),
        diagnostics=diagnostics,
    )


def test_static_tuple_captures_semantic_dimensions():
    plan = _plan(
        "cluster", {"rate": 5}, {"type": "cluster", "num_shards": 3, "server_config": {"a": 1}}, "bgsave.started"
    )
    data = static_tuple(plan)
    assert data["topology"] == {"type": "cluster", "num_shards": 3, "replicas_per_shard": None, "addressing": None}
    # Core never interprets component config, so a fault is identified by its component ID.
    assert data["faults"] == ["valkey.process_signal"]
    assert data["inputs"] == {"rate": 5}
    assert coverage_key(plan) != coverage_key(_plan("cluster", {"rate": 6}, {"type": "cluster", "num_shards": 3}))
    assert coverage_key(plan) == coverage_key(
        _plan("cluster", {"rate": 5}, {"type": "cluster", "num_shards": 3, "server_config": {"a": 1}}, "bgsave.started")
    )


def test_coverage_db_round_trips(tmp_path):
    path = tmp_path / "coverage.json"
    db = CoverageDB(str(path))
    plan = _plan()
    db.record(plan, _outcome(plan))
    db.record(plan, _outcome(plan, OutcomeStatus.FAIL))
    db.save()
    reloaded = CoverageDB(str(path))
    assert reloaded.runs(coverage_key(plan)) == 2
    assert reloaded.summary()["statuses"] == {"PASS": 1, "FAIL": 1}
    path.write_text(json.dumps({"schema": "other"}))
    with pytest.raises(CampaignError):
        CoverageDB(str(path))


CAMPAIGN = {
    "campaign_id": "unit",
    "seed": 3,
    "base": {
        "feature": {"use": "valkey.forkless-save", "version": "1", "with": {}},
        "topology": {"type": "standalone"},
    },
    "matrix": {
        "topology.type": ["standalone", "replication", "cluster"],
        "feature.with.mutation_rate": [0, 300, 900],
    },
}


def test_campaign_expansion_is_deterministic_and_assigns_paths():
    first = Campaign.from_document(CAMPAIGN).expand()
    second = Campaign.from_document(CAMPAIGN).expand()
    assert len(first) == 9
    assert [case.seed for case in first] == [case.seed for case in second]
    assert first[0].scenario["topology"]["type"] == "standalone"
    assert first[0].scenario["feature"]["with"]["mutation_rate"] == 0
    assert all("api_version" not in case.scenario for case in first)


def test_campaign_schedule_prefers_uncovered_then_boundary_tuples():
    session = valkey_runtime()
    campaign = Campaign.from_document(CAMPAIGN)
    coverage = CoverageDB()
    first_pass = campaign.schedule(session.compile, coverage)
    assert len(first_pass) == 9
    assert len({case.key for case in first_pass}) == 9
    assert first_pass[0].boundary == 2, "boundary combinations come first among uncovered tuples"
    # Mark one tuple covered: it must move to the back of a fresh schedule.
    covered = first_pass[0]
    coverage.record(covered.plan, _outcome(covered.plan))
    again = Campaign.from_document(CAMPAIGN).schedule(session.compile, coverage)
    assert again[-1].key == covered.key


def test_campaign_records_invalid_combinations():
    document = copy.deepcopy(CAMPAIGN)
    document["matrix"] = {"feature.with.mutation_rate": [-1, 10]}
    session = valkey_runtime()
    campaign = Campaign.from_document(document)
    scheduled = campaign.schedule(session.compile, CoverageDB())
    assert len(scheduled) == 1
    assert [case.compile_error is not None for case in campaign.cases] == [True, False]


def test_run_campaign_respects_max_runs_and_stop_on_failure(tmp_path):
    document = dict(CAMPAIGN, max_runs=4, stop_on_failure=True)
    session = valkey_runtime()
    statuses = iter([OutcomeStatus.PASS, OutcomeStatus.FAIL, OutcomeStatus.PASS])
    coverage = CoverageDB(str(tmp_path / "cov.json"))

    summary = run_campaign(
        Campaign.from_document(document),
        compile_fn=session.compile,
        execute_fn=lambda case: _outcome(case.plan, next(statuses)),
        coverage=coverage,
    )
    assert summary["cases_run"] == 2
    assert summary["statuses"] == {"PASS": 1, "FAIL": 1}
    assert summary["new_tuples"] == 2
    assert CoverageDB(str(tmp_path / "cov.json")).summary()["runs"] == 2


@pytest.mark.parametrize(
    "document",
    [
        {"campaign_id": "x", "base": {}},
        {"campaign_id": "x", "base": {"feature": {}}},
        {**CAMPAIGN, "api_version": "v3-campaign"},
        {**CAMPAIGN, "matrix": {"seed": [1, 2]}},
        {**CAMPAIGN, "matrix": {"topology.type": []}},
        {**CAMPAIGN, "extra": 1},
    ],
)
def test_campaign_document_validation(document):
    with pytest.raises(CampaignError):
        Campaign.from_document(document)


def test_failure_signature_and_matching():
    plan = _plan()
    reference = failure_signature(_outcome(plan, OutcomeStatus.FAIL, failing=("check",)), ["tests.path"])
    assert reference["path_evidence"] == ["tests.path"]
    same = failure_signature(_outcome(plan, OutcomeStatus.FAIL, failing=("check",)), ["tests.path"])
    lost_path = failure_signature(_outcome(plan, OutcomeStatus.FAIL, failing=("check",), evidence=()), ["tests.path"])
    other = failure_signature(_outcome(plan, OutcomeStatus.FAIL, failing=("save",)), ["tests.path"])
    assert same_failure(reference, same)
    assert not same_failure(reference, lost_path), "a reduction must keep the path evidence"
    assert not same_failure(reference, other)


def test_reduction_candidates_shrink_topology_inputs_and_config():
    scenario = {
        "feature": {"use": "p", "version": "1", "with": {"keys": 1000, "flag": True}},
        "topology": {
            "type": "cluster",
            "num_shards": 5,
            "replicas_per_shard": 2,
            "server_config": {"maxmemory": "1gb"},
            "node_configs": {"primary": {"a": 1}},
        },
    }
    names = [name for name, _ in reduction_candidates(scenario, {"properties": {"keys": {"minimum": 10}}})]
    assert "topology.replicas_per_shard=0" in names
    assert "topology.num_shards=3" in names
    assert "drop topology.server_config.maxmemory" in names
    assert "drop topology.node_configs.primary" in names
    assert "feature.with.keys=10" in names and "feature.with.keys=500" in names
    assert not [name for name in names if "flag" in name]


def test_minimize_keeps_reductions_that_preserve_the_failure():
    scenario = {
        "feature": {"use": "p", "version": "1", "with": {"keys": 800}},
        "topology": {"type": "standalone"},
    }
    plan = _plan()
    reference = failure_signature(_outcome(plan, OutcomeStatus.FAIL, failing=("check",)), ["tests.path"])

    def execute(candidate, candidate_plan):
        # The bug reproduces only while keys >= 100.
        failing = candidate["feature"]["with"]["keys"] >= 100
        status = OutcomeStatus.FAIL if failing else OutcomeStatus.PASS
        return _outcome(plan, status, failing=("check",) if failing else ())

    result = minimize(
        scenario,
        reference,
        compile_fn=lambda candidate: plan,
        execute_fn=execute,
        input_schema={"properties": {"keys": {"minimum": 1}}},
        required_evidence=["tests.path"],
        max_attempts=40,
    )
    assert 100 <= result["scenario"]["feature"]["with"]["keys"] < 200
    assert result["accepted"]


def test_coverage_report_summarizes_and_lists_untried_campaign_tuples():
    from valkey_fuzzer.campaign import coverage_report

    session = valkey_runtime()
    campaign = Campaign.from_document(CAMPAIGN)
    db = CoverageDB()
    first = campaign.schedule(session.compile, db)[0]
    db.record(first.plan, _outcome(first.plan, OutcomeStatus.FAIL, failing=("check",)))
    report = coverage_report(db, Campaign.from_document(CAMPAIGN), session.compile)
    assert report["summary"]["runs"] == 1
    assert report["failing_tuples"][0]["statuses"] == {"FAIL": 1}
    assert report["campaign"]["covered_tuples"] == 1
    assert report["campaign"]["untried_tuples"] == 8
    assert first.case_id not in {item["case_id"] for item in report["campaign"]["untried"]}
    assert "valkey.bgsave" in report["dimensions"]["actions"]


def test_coverage_report_counts_untried_cases_beyond_max_runs():
    from valkey_fuzzer.campaign import coverage_report

    session = valkey_runtime()
    report = coverage_report(CoverageDB(), Campaign.from_document(dict(CAMPAIGN, max_runs=2)), session.compile)
    assert report["campaign"]["untried_tuples"] == 9
