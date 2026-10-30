"""Coverage keys, failure signatures, and minimization."""

from dataclasses import replace

import pytest

from valkey_fuzzer import contracts as c
from valkey_fuzzer.coverage import coverage_key
from valkey_fuzzer.environment import PolicySpec, ResourceSpec
from valkey_fuzzer.minimize import failure_signature, minimize, reduction_candidates, same_failure


def _plan(inputs=None, deployment=None, size_mb=32, threshold=0.5, provider="valkey-fuzzer.deployment.local"):
    return c.ExecutionPlan(
        scenario_id="c", experiment_id="tests.experiment", seed=1, deployment_mode="standalone",
        deployment_provider=c.DeploymentProviderIdentity(provider), inputs=inputs or {},
        components=(c.ComponentSpec(role=c.ComponentRole.ORACLE, use="valkey.oracle.evidence", id="check"),),
        required_evidence=(c.EvidenceRequirement("tests.path"),), deployment=deployment or {"mode": "standalone"},
        resources=(ResourceSpec("disk", "valkey.resource.tmpfs", {"size_mb": size_mb}),),
        policies=(PolicySpec("budget", "tests.latency", {"threshold": threshold}),),
    )  # fmt: skip


@pytest.mark.parametrize(
    ("other", "same"),
    [
        ({}, True),
        ({"inputs": {"rate": 6}}, False),
        ({"deployment": {"mode": "standalone", "valkey_binary": "/opt/b"}}, False),
        ({"provider": "tests.deployment.other"}, False),
        ({"size_mb": 64}, False),
        ({"threshold": 0.9}, False),
    ],
    ids=["same-plan", "inputs", "build", "provider", "resource-config", "policy-config"],
)
def test_the_coverage_key_tells_apart_what_differs(other, same):
    first = {"inputs": {"rate": 5}, "deployment": {"mode": "standalone", "valkey_binary": "/opt/a"}}
    assert (coverage_key(_plan(**first)) == coverage_key(_plan(**{**first, **other}))) is same


def _signature(status=c.OutcomeStatus.FAIL, failing=("check",), evidence=("tests.path",), diagnostics=None):
    outcome = c.RunOutcome(
        plan=_plan(), status=status, summary=status.value, diagnostics=diagnostics or {},
        component_results=tuple(c.ComponentResult(component_id=name, status=c.ComponentStatus.FAIL, summary="x") for name in failing),
        evidence=tuple(c.Evidence(item, "check", "p", 1.0, True) for item in evidence),
    )  # fmt: skip
    return failure_signature(outcome, ["tests.path"])


def test_a_reduction_is_the_same_failure_only_with_the_same_failures_errors_and_path_evidence():
    reference = _signature()
    assert same_failure(reference, _signature())
    assert not same_failure(reference, _signature(evidence=())), "a reduction must keep the path evidence"
    assert not same_failure(reference, _signature(failing=("save",)))
    first, second = (_signature(c.OutcomeStatus.ERROR, (), diagnostics={"error": {"type": kind}}) for kind in "AB")
    assert same_failure(first, first) and not same_failure(first, second)


def test_minimize_keeps_only_reductions_that_preserve_the_failure():
    scenario = {"experiment": {"id": "p", "inputs": {"keys": 800, "flag": True}},
                "deployment": {"mode": "cluster", "primaries": 5, "replicas_per_primary": 2,
                               "server_config": {"maxmemory": "1gb"}, "instance_configs": {"primary": {"a": 1}}}}  # fmt: skip
    schema = {"properties": {"keys": {"minimum": 10}}}
    names = {name for name, _ in reduction_candidates(scenario, schema)}
    assert {"deployment.replicas_per_primary=0", "deployment.primaries=3", "drop deployment.server_config.maxmemory",
            "drop deployment.instance_configs.primary", "experiment.inputs.keys=10", "experiment.inputs.keys=400"} <= names  # fmt: skip
    assert not [name for name in names if "flag" in name]

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


def _signature_outcome(failing):
    status = c.OutcomeStatus.FAIL if failing else c.OutcomeStatus.PASS
    results = (c.ComponentResult(component_id="check", status=c.ComponentStatus.FAIL, summary="x"),) if failing else ()
    return c.RunOutcome(
        plan=_plan(),
        status=status,
        summary=status.value,
        component_results=results,
        evidence=(c.Evidence("tests.path", "check", "p", 1.0, True),),
    )
