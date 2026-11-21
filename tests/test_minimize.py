"""Coverage keys, failure signatures, and minimization."""

import json
from dataclasses import replace
from pathlib import Path

import pytest

from tests.conform_support import register_copy, register_service, scenario_file, stub_tlc
from valkey_fuzzer.coverage import coverage_tuple
from valkey_fuzzer.deployment.local.rerun import HOST_FIELDS, deployment_reductions
from valkey_fuzzer.environment import EnvironmentSpec, PolicySpec
from valkey_fuzzer.minimize import failure_signature, minimize, reduction_candidates, same_failure
from valkey_fuzzer.plan import ComponentSpec, DeploymentProviderIdentity, EvidenceRequirement, ExecutionPlan
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.references import EnvironmentKind
from valkey_fuzzer.results import ComponentResult, ComponentStatus, Evidence, OutcomeStatus, RunOutcome


def _plan(inputs=None, deployment=None, size_mb=32, threshold=0.5, provider="valkey-fuzzer.deployment.local",
          instances=None):  # fmt: skip
    return ExecutionPlan(
        instances=instances or {},
        scenario_id="c", experiment_id="tests.experiment", seed=1, deployment_mode="standalone",
        deployment_provider=DeploymentProviderIdentity(provider), inputs=inputs or {},
        components=(ComponentSpec(role=ComponentRole.ORACLE, use="valkey.oracle.evidence", id="check"),),
        required_evidence=(EvidenceRequirement("tests.path"),), deployment=deployment or {"mode": "standalone"},
        resources=(EnvironmentSpec(EnvironmentKind.RESOURCE, "disk", "valkey.resource.tmpfs", {"size_mb": size_mb}),),
        policies=(PolicySpec("budget", "tests.latency", {"threshold": threshold}),),
    )  # fmt: skip


PRIMARY = {"valkey-0": {"role": "primary", "shard": 0, "server_config": {"appendonly": "yes"}}}
REPLICA = {"valkey-1": {"role": "replica", "shard": 0, "server_config": {"appendonly": "yes"}}}


@pytest.mark.parametrize(
    ("other", "same"),
    [
        ({}, True),
        ({"inputs": {"rate": 6}}, False),
        ({"deployment": {"mode": "standalone", "valkey_binary": "/opt/b"}}, False),
        ({"provider": "tests.deployment.other"}, False),
        ({"size_mb": 64}, False),
        ({"threshold": 0.9}, False),
        ({"deployment": {"mode": "standalone", "valkey_binary": "/opt/a", "base_port": 7000}}, True),
        ({"instances": {**PRIMARY, **REPLICA}}, False),
        ({"instances": {"valkey-0": {**PRIMARY["valkey-0"], "server_config": {"appendonly": "no"}}}}, False),
    ],
    ids=["same-plan", "inputs", "build", "provider", "resource-config", "policy-config", "host-ports",
         "instances", "instance-config"],
)  # fmt: skip
def test_the_coverage_key_tells_apart_what_differs(other, same):
    first = {
        "inputs": {"rate": 5},
        "deployment": {"mode": "standalone", "valkey_binary": "/opt/a"},
        "instances": PRIMARY,
    }

    def key(plan):
        return coverage_tuple(RunOutcome(plan, OutcomeStatus.PASS, "passed"), HOST_FIELDS)["key"]

    assert (key(_plan(**first)) == key(_plan(**{**first, **other}))) is same


def _signature(status=OutcomeStatus.FAIL, failing=("check",), evidence=("tests.path",), diagnostics=None):
    outcome = RunOutcome(
        plan=_plan(), status=status, summary=status.value, diagnostics=diagnostics or {},
        component_results=tuple(ComponentResult(component_id=name, status=ComponentStatus.FAIL, summary="x") for name in failing),
        evidence=tuple(Evidence(item, "check", "p", 1.0, True) for item in evidence),
    )  # fmt: skip
    return failure_signature(outcome, ["tests.path"])


def test_a_reduction_is_the_same_failure_only_with_the_same_failures_errors_and_path_evidence():
    reference = _signature()
    assert same_failure(reference, _signature())
    assert not same_failure(reference, _signature(evidence=())), "a reduction must keep the path evidence"
    assert not same_failure(reference, _signature(failing=("save",)))
    first, second = (_signature(OutcomeStatus.ERROR, (), diagnostics={"error": {"type": kind}}) for kind in "AB")
    assert same_failure(first, first) and not same_failure(first, second)
    # A crash is the same failure only with the same status: an ERROR run reporting the FAIL's crash is not it.
    crash = {"crashes": {"valkey-0": "debug.c:12 'x' is not true"}}
    failed = _signature(OutcomeStatus.FAIL, diagnostics=crash)
    errored = _signature(OutcomeStatus.ERROR, diagnostics=crash)
    assert failed["crashes"] == errored["crashes"] and same_failure(failed, failed)
    assert not same_failure(failed, errored) and not same_failure(errored, failed)


def test_minimize_keeps_only_reductions_that_preserve_the_failure():
    scenario = {"experiment": {"id": "p", "inputs": {"keys": 800, "flag": True, "shard": 4}},
                "deployment": {"mode": "cluster", "primaries": 5, "replicas_per_primary": 2,
                               "server_config": {"maxmemory": "1gb"}, "instance_configs": {"primary": {"a": 1}}}}  # fmt: skip
    schema = {"properties": {"keys": {"minimum": 10, "shrinkable": True}, "shard": {"minimum": 0}}}
    names = {name for name, _ in reduction_candidates(scenario, schema, deployment_reductions=deployment_reductions)}
    assert {"deployment.replicas_per_primary=0", "deployment.primaries=0", "deployment.primaries=2",
            "drop deployment.server_config.maxmemory", "drop deployment.instance_configs.primary",
            "experiment.inputs.keys=10", "experiment.inputs.keys=400"} <= names  # fmt: skip
    # Only a shrinkable number: a flag has no size, and a smaller shard index is another scenario, not less work.
    assert not [name for name in names if "flag" in name or "shard" in name]
    # The experiment's requirements bound the shape; the provider's own minimum is left to its compile check.
    bounded = {
        name for name, _ in reduction_candidates(scenario, schema, {"primaries": {"minimum": 3}}, deployment_reductions)
    }
    assert {name for name in bounded if "primaries=" in name} == {"deployment.primaries=3"}
    # Only the deployment's provider knows how its document shrinks; without its hook the inputs still do.
    assert {name for name, _ in reduction_candidates(scenario, schema)} == {
        "experiment.inputs.keys=10",
        "experiment.inputs.keys=400",
    }

    def execute(candidate, plan):
        # The bug reproduces only while keys >= 100.
        failing = candidate["experiment"]["inputs"]["keys"] >= 100
        return replace(_signature_outcome(failing), plan=plan)

    result = minimize(
        scenario,
        _signature(),
        compile_fn=lambda candidate: _plan(),
        execute_fn=execute,
        experiment_inputs=schema,
        required_evidence=["tests.path"],
        max_attempts=60,
    )
    assert 100 <= result["scenario"]["experiment"]["inputs"]["keys"] < 200 and result["accepted"]
    assert result["reproduced"] and result["confirmed"]
    steps = [item["step"] for item in result["attempts"]]
    assert steps[0] == "original" and steps[-1] == "confirm"
    # A reduction rejected once (keys=10 lost the failure) is not run again after each kept step.
    assert steps.count("experiment.inputs.keys=10") == 1


def test_minimize_runs_the_original_first_and_reduces_nothing_that_does_not_reproduce():
    scenario = {"experiment": {"id": "p", "inputs": {"keys": 800}}, "deployment": {"mode": "standalone"}}
    runs = []

    def execute(candidate, plan):
        runs.append(candidate)
        return _signature_outcome(False)

    result = minimize(scenario, _signature(), compile_fn=lambda candidate: _plan(), execute_fn=execute,
                      experiment_inputs={"properties": {"keys": {"shrinkable": True}}},
                      required_evidence=["tests.path"], max_attempts=5)  # fmt: skip
    assert runs == [scenario] and result["scenario"] == scenario
    assert (result["reproduced"], result["confirmed"], result["accepted"]) == (False, None, [])
    assert result["attempts"] == [{"step": "original", "result": "not reproduced", "status": "PASS"}]


def test_minimize_confirms_its_result_by_running_it_again():
    scenario = {"experiment": {"id": "p", "inputs": {"keys": 800}}, "deployment": {"mode": "standalone"}}
    # The original and the first reduction fail; the confirmation (the third run) does not.
    statuses = iter([True, True, False])
    result = minimize(scenario, _signature(), compile_fn=lambda candidate: _plan(),
                      execute_fn=lambda candidate, plan: _signature_outcome(next(statuses)),
                      experiment_inputs={"properties": {"keys": {"shrinkable": True}}},
                      required_evidence=["tests.path"], max_attempts=1)  # fmt: skip
    assert result["accepted"] == ["experiment.inputs.keys=0"] and result["reproduced"]
    assert result["confirmed"] is False
    assert result["attempts"][-1] == {"step": "confirm", "result": "not reproduced", "status": "PASS"}


def test_minimize_skips_a_candidate_that_does_not_compile_but_raises_a_bug():
    scenario = {"experiment": {"id": "p", "inputs": {"keys": 800}}, "deployment": {"mode": "standalone"}}
    options = {"execute_fn": lambda candidate, plan: _signature_outcome(True),
               "experiment_inputs": {"properties": {"keys": {"shrinkable": True}}},
               "required_evidence": ["tests.path"], "max_attempts": 5}  # fmt: skip

    def invalid(candidate):
        # The original compiles (it ran once); every reduction of it is refused.
        if candidate == scenario:
            return _plan()
        raise ValueError("keys is too small")

    result = minimize(scenario, _signature(), compile_fn=invalid, **options)
    assert result["accepted"] == [] and {item["result"] for item in result["attempts"][1:]} == {
        "invalid: keys is too small"
    }

    def broken(candidate):
        raise TypeError("a bug")

    # A bug in the compile function propagates; it is not an invalid candidate.
    with pytest.raises(TypeError, match="a bug"):
        minimize(scenario, _signature(), compile_fn=broken, **options)


def _signature_outcome(failing):
    status = OutcomeStatus.FAIL if failing else OutcomeStatus.PASS
    results = (ComponentResult(component_id="check", status=ComponentStatus.FAIL, summary="x"),) if failing else ()
    return RunOutcome(
        plan=_plan(),
        status=status,
        summary=status.value,
        component_results=results,
        evidence=(Evidence("tests.path", "check", "p", 1.0, True),),
    )


def test_minimize_keeps_a_conform_failure_only_while_the_same_claim_fails(tmp_path, monkeypatch):
    # A run FAILs only through spec conform (an approved bundle's claimed violation, no oracle's FAIL): that is the
    # failed component spec-conform:register of its outcome, so the reference signature holds it, and each attempt's
    # own conform check (its report's conform section) decides whether the reduction kept it.
    bundle = register_copy(tmp_path, monkeypatch, approved=True, shrinkable=True)
    stub_tlc(monkeypatch)
    # The crash loses the acknowledged writes only when there are at least two of them.
    service = register_service(tmp_path, monkeypatch, bundle, lost=lambda plan: plan.inputs["writes"] >= 2)
    prepared = service.prepare_run(scenario_file(tmp_path))
    record = service.execute(prepared.plan, scenario=prepared.scenario, report_dir=str(tmp_path / "run"))
    assert record.outcome.status is OutcomeStatus.FAIL

    minimization = service.prepare_minimization(record.report_path)
    assert minimization.needed and minimization.reference["failing_components"] == ["spec-conform:register"]
    result = service.minimize(minimization, max_attempts=5, log=lambda line: None)
    assert result.reproduced and result.confirmed
    # writes 4 -> 1 kept every write, so conform passed and the reduction lost the failure; 4 -> 2 kept it.
    assert [(item["step"], item["result"]) for item in result.attempts] == [
        ("original", "reproduced"),
        ("experiment.inputs.writes=1", "rejected"),
        ("experiment.inputs.writes=2", "kept"),
        ("confirm", "reproduced"),
    ]
    assert result.accepted == ["experiment.inputs.writes=2"] and result.scenario["experiment"]["inputs"]["writes"] == 2
    reports = sorted(Path(result.report_dir).glob("attempt-*/report.json"), key=lambda path: int(path.parent.name[8:]))
    conform = [json.loads(path.read_text())["conform"]["register"]["status"] for path in reports]
    assert conform == ["fail", "pass", "fail", "fail"]
