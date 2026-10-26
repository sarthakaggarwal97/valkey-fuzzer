"""Differential execution: document parsing, evidence extraction, verdicts."""

import pytest

from tests.valkey_packs_support import valkey_runtime
from valkey_fuzzer.differential import (
    Differential,
    DifferentialError,
    compare_outcomes,
    differential_status,
    extract,
    run_differential,
)
from valkey_fuzzer.extensibility import (
    ComponentKind,
    ComponentSpec,
    Evidence,
    OutcomeStatus,
    RunOutcome,
    RunPlan,
    TopologyProviderIdentity,
)
from valkey_fuzzer_packs.workloads import replies_digest

DOCUMENT = {
    "differential_id": "unit",
    "base": {
        "scenario_id": "base",
        "seed": 5,
        "feature": {"use": "valkey.snapshot-equivalence", "version": "1", "with": {"operations": 10}},
        "topology": {"type": "standalone"},
    },
    "variants": {
        "a": {"topology.server_config": {"appendonly": "no"}},
        "b": {"topology.server_config": {"appendonly": "yes"}},
    },
    "compare": {
        "equal": [{"evidence_id": "valkey.dataset.digest", "source": "dataset", "field": "digest"}],
        "differ": [{"evidence_id": "valkey.bgsave.result", "field": "after.type"}],
    },
}


PROVIDER = TopologyProviderIdentity("valkey-fuzzer.local-topology", "1", "v1")


def _plan():
    return RunPlan(
        scenario_id="p",
        pack_id="tests.pack",
        pack_version="1",
        topology_provider=PROVIDER,
        seed=5,
        topology="standalone",
        mode="topology_smoke",
        components=(ComponentSpec(kind=ComponentKind.ORACLE, component_id="valkey.evidence.assert", instance_id="x"),),
    )


def _outcome(status=OutcomeStatus.PASS, digest="d1", save_type="fork", extra=()):
    evidence = (
        Evidence("valkey.dataset.digest", "dataset", "oracle.completed", 1.0, {"digest": digest}),
        Evidence(
            "valkey.bgsave.result", "bgsave", "action.completed", 1.0, {"node": "node-0", "after": {"type": save_type}}
        ),
        *extra,
    )
    return RunOutcome(plan=_plan(), status=status, summary=status.value, evidence=evidence)


def test_variants_patch_the_base_and_share_its_seed():
    differential = Differential.from_document(DOCUMENT)
    a, b = differential.scenario("a"), differential.scenario("b")
    assert a["scenario_id"] == "base-a" and b["scenario_id"] == "base-b"
    assert a["seed"] == b["seed"] == 5
    assert a["topology"]["server_config"] == {"appendonly": "no"}
    assert "api_version" not in a
    assert "server_config" not in differential.base["topology"]


@pytest.mark.parametrize(
    ("patch", "message"),
    [
        ({"variants": {"only": {}}}, "at least two"),
        ({"variants": {"a": {"seed": 1}, "b": {}}}, "must start with"),
        ({"variants": {"a": {"feature.use": "x"}, "b": {}}}, "same feature pack"),
        ({"compare": {}}, "at least one"),
        ({"compare": {"equal": [{"field": "x"}]}}, "evidence_id"),
        ({"compare": {"equal": [{"evidence_id": "a.b", "bogus": 1}]}}, "unknown fields"),
        ({"compare": {"equal": [{"evidence_id": "a.b"}], "require_status": "MAYBE"}}, "require_status"),
        ({"api_version": "v3"}, "unknown fields"),
    ],
)
def test_document_validation(patch, message):
    with pytest.raises(DifferentialError, match=message):
        Differential.from_document({**DOCUMENT, **patch})


def test_variants_may_patch_environment_declarations():
    document = {**DOCUMENT, "variants": {"a": {"artifacts.build.with.path": "/a"}, "b": {}}}
    assert Differential.from_document(document).scenario("a")["artifacts"] == {"build": {"with": {"path": "/a"}}}


def test_extract_walks_mappings_and_lists():
    value = {"a": {"b": [10, {"c": "x"}]}}
    assert extract(value, "a.b.1.c") == "x"
    assert extract(value, "a.b.0") == 10
    assert extract(value, "a.z") == "<missing>"
    assert extract(value, None) == value


def test_equal_and_differ_rules():
    differential = Differential.from_document(DOCUMENT)
    good = {"a": _outcome(save_type="fork"), "b": _outcome(save_type="forkless")}
    comparisons = compare_outcomes(differential, good)
    assert [item["result"] for item in comparisons] == ["ok", "ok"]
    assert differential_status(differential, good, comparisons)[0] == "PASS"

    diverged = {"a": _outcome(digest="d1", save_type="fork"), "b": _outcome(digest="d2", save_type="forkless")}
    comparisons = compare_outcomes(differential, diverged)
    assert comparisons[0]["result"] == "mismatch"
    status, summary = differential_status(differential, diverged, comparisons)
    assert status == "FAIL" and "mismatch" in summary

    same_path = {"a": _outcome(save_type="fork"), "b": _outcome(save_type="fork")}
    comparisons = compare_outcomes(differential, same_path)
    assert comparisons[1]["result"] == "unexpectedly_equal"


def test_multiple_evidence_items_compare_as_a_sorted_list():
    differential = Differential.from_document(
        {**DOCUMENT, "compare": {"equal": [{"evidence_id": "valkey.bgsave.result", "field": "after.type"}]}}
    )
    extra_a = (Evidence("valkey.bgsave.result", "bgsave", "p", 1.0, {"after": {"type": "z"}}),)
    extra_b = (Evidence("valkey.bgsave.result", "bgsave", "p", 1.0, {"after": {"type": "z"}}),)
    outcomes = {"a": _outcome(save_type="a", extra=extra_a), "b": _outcome(save_type="a", extra=extra_b)}
    (comparison,) = compare_outcomes(differential, outcomes)
    assert comparison["values"]["a"] == ["a", "z"]
    assert comparison["result"] == "ok"


@pytest.mark.parametrize(
    ("statuses", "expected"),
    [
        ((OutcomeStatus.ERROR, OutcomeStatus.PASS), "ERROR"),
        ((OutcomeStatus.SKIP_UNSUPPORTED, OutcomeStatus.PASS), "SKIP_UNSUPPORTED"),
        ((OutcomeStatus.FAIL, OutcomeStatus.PASS), "FAIL"),
    ],
)
def test_variant_status_dominates_comparisons(statuses, expected):
    differential = Differential.from_document(DOCUMENT)
    outcomes = {"a": _outcome(statuses[0], save_type="fork"), "b": _outcome(statuses[1], save_type="forkless")}
    assert differential_status(differential, outcomes, compare_outcomes(differential, outcomes))[0] == expected


def test_missing_evidence_fails_the_comparison():
    differential = Differential.from_document(DOCUMENT)
    missing = RunOutcome(plan=_plan(), status=OutcomeStatus.PASS, summary="x")
    outcomes = {"a": _outcome(), "b": missing}
    comparisons = compare_outcomes(differential, outcomes)
    assert {item["result"] for item in comparisons} == {"missing"}


def test_run_differential_compiles_every_variant_before_running():
    session = valkey_runtime()
    document = {**DOCUMENT, "variants": {"a": {}, "b": {"feature.with.operations": -1}}}
    executed = []
    with pytest.raises(Exception, match="below its minimum"):
        run_differential(
            Differential.from_document(document),
            compile_fn=session.compile,
            execute_fn=lambda name, scenario, plan: executed.append(name),
        )
    assert executed == []


def test_run_differential_reports_each_variant():
    session = valkey_runtime()
    outcomes = iter([_outcome(save_type="fork"), _outcome(save_type="forkless")])
    report = run_differential(
        Differential.from_document(DOCUMENT),
        compile_fn=session.compile,
        execute_fn=lambda name, scenario, plan: (next(outcomes), f"/runs/{name}"),
    )
    assert report["status"] == "PASS"
    assert report["seed"] == 5
    assert report["variants"]["b"]["artifact_dir"] == "/runs/b"


def test_replies_digest_ignores_timing_but_not_replies():
    base = [{"id": 1, "op": "get", "key": "k", "outcome": "ok", "reply": "v", "t_start": 1, "t_end": 2}]
    later = [dict(base[0], t_start=50, t_end=60)]
    changed = [dict(base[0], reply="w")]
    assert replies_digest(base) == replies_digest(later)
    assert replies_digest(base) != replies_digest(changed)
