"""Which component results may produce a trustworthy feature FAIL.

Only an oracle FAIL (an invariant violation) or an action FAIL (an expected
transition did not happen) is evidence about the system under test. A FAIL
from a capability probe, a workload or observer start, or a fault arm means the
harness never established the conditions for a verdict, so the run is ERROR.
Cleanup failures are reported as cleanup errors and are never turned into, or
hidden behind, a feature verdict.
"""

from unittest.mock import patch

import pytest

from tests.test_feature_runner import _plan, _Provider, _registries, _spec, _topology_providers
from valkey_fuzzer.bound_plan import BoundRunPlan
from valkey_fuzzer.extensibility import ComponentKind, ComponentResult, ComponentStatus, OutcomeStatus
from valkey_fuzzer.feature_runner import FeatureRunner

PASS = ComponentStatus.PASS
FAIL = ComponentStatus.FAIL


class _Component:
    """Configurable component implementing every role's methods."""

    def __init__(self, primary: ComponentStatus = PASS, cleanup: ComponentStatus = PASS):
        self.primary = primary
        self.cleanup_status = cleanup

    @staticmethod
    def _result(spec, status, phase):
        return ComponentResult(spec.instance_id, status, f"{spec.instance_id} {phase} {status.value}")

    def probe(self, context, spec):
        return self._result(spec, self.primary, "probe")

    def start(self, context, spec):
        return self._result(spec, self.primary, "start")

    def arm(self, context, spec):
        return self._result(spec, self.primary, "arm")

    def run(self, context, spec):
        return self._result(spec, self.primary, "run")

    def evaluate(self, context, spec, evidence):
        return self._result(spec, self.primary, "evaluate")

    def stop(self, context, spec):
        return self._result(spec, self.cleanup_status, "stop")

    def cleanup(self, context, spec):
        return self._result(spec, self.cleanup_status, "cleanup")


def _run(components):
    """Run ``components``: a sequence of (kind, instance_id, primary, cleanup) tuples."""
    calls: list[str] = []
    entries = []
    specs = []
    for kind, instance_id, primary, cleanup in components:
        entries.append(
            (kind, f"tests.{kind.value}", lambda primary=primary, cleanup=cleanup: _Component(primary, cleanup))
        )
        specs.append(_spec(kind, instance_id))
    return FeatureRunner(
        _registries(entries),
        _topology_providers(standalone=lambda: _Provider(calls, "standalone")),
    ).run(_plan(tuple(specs)))


@pytest.mark.parametrize("kind", [ComponentKind.ORACLE, ComponentKind.ACTION])
def test_verdict_roles_produce_a_trustworthy_fail(kind):
    outcome = _run([(kind, "verdict", FAIL, PASS)])

    assert outcome.status is OutcomeStatus.FAIL
    assert "untrusted_component_failures" not in outcome.diagnostics


@pytest.mark.parametrize("kind", [ComponentKind.WORKLOAD, ComponentKind.OBSERVER, ComponentKind.FAULT])
def test_start_or_arm_fail_is_error_not_fail(kind):
    outcome = _run([(kind, "setup", FAIL, PASS), (ComponentKind.ORACLE, "check", PASS, PASS)])

    assert outcome.status is OutcomeStatus.ERROR
    assert outcome.diagnostics["untrusted_component_failures"] == ("setup",)
    assert "no trustworthy verdict" in outcome.summary


def test_required_capability_fail_is_error_and_never_provisions():
    calls: list[str] = []
    outcome = FeatureRunner(
        _registries([(ComponentKind.CAPABILITY, "tests.capability", lambda: _Component(FAIL))]),
        _topology_providers(standalone=lambda: _Provider(calls, "standalone")),
    ).run(_plan((_spec(ComponentKind.CAPABILITY, "probe"),)))

    assert outcome.status is OutcomeStatus.ERROR
    assert "capability 'probe' failed" in outcome.summary
    assert "standalone.provision" not in calls


def test_setup_failure_wins_over_an_oracle_fail():
    outcome = _run(
        [
            (ComponentKind.WORKLOAD, "traffic", FAIL, PASS),
            (ComponentKind.ORACLE, "check", FAIL, PASS),
        ]
    )

    assert outcome.status is OutcomeStatus.ERROR
    assert outcome.diagnostics["untrusted_component_failures"] == ("traffic",)


def test_cleanup_failure_alone_is_error_not_a_feature_fail():
    outcome = _run(
        [
            (ComponentKind.FAULT, "fault", PASS, FAIL),
            (ComponentKind.ORACLE, "check", PASS, PASS),
        ]
    )

    assert outcome.status is OutcomeStatus.ERROR
    assert outcome.summary == "cleanup failed before a trustworthy feature failure was established"
    assert outcome.diagnostics["cleanup_errors"][0]["owner"] == "fault"


def test_trustworthy_fail_keeps_cleanup_errors_visible():
    outcome = _run(
        [
            (ComponentKind.FAULT, "fault", PASS, FAIL),
            (ComponentKind.ORACLE, "check", FAIL, PASS),
        ]
    )

    assert outcome.status is OutcomeStatus.FAIL
    assert outcome.diagnostics["cleanup_errors"][0]["owner"] == "fault"


def test_untrusted_failure_with_cleanup_errors_stays_error():
    outcome = _run(
        [
            (ComponentKind.WORKLOAD, "traffic", FAIL, FAIL),
            (ComponentKind.ORACLE, "check", FAIL, PASS),
        ]
    )

    assert outcome.status is OutcomeStatus.ERROR
    assert outcome.diagnostics["cleanup_errors"][0]["owner"] == "traffic"
    assert outcome.diagnostics["untrusted_component_failures"] == ("traffic",)


def test_oracle_blocked_by_a_failed_action_is_still_a_trustworthy_fail():
    calls: list[str] = []
    outcome = FeatureRunner(
        _registries(
            [
                (ComponentKind.ACTION, "tests.action", lambda: _Component(FAIL)),
                (ComponentKind.ORACLE, "tests.oracle", lambda: _Component(PASS)),
            ]
        ),
        _topology_providers(standalone=lambda: _Provider(calls, "standalone")),
    ).run(
        _plan(
            (
                _spec(ComponentKind.ACTION, "transition"),
                _spec(ComponentKind.ORACLE, "check", depends_on=("transition",)),
            )
        )
    )

    assert outcome.status is OutcomeStatus.FAIL
    results = {result.component_instance_id: result.status for result in outcome.component_results}
    assert results == {"transition": FAIL, "check": ComponentStatus.SKIP}


class _RaisingAction:
    def run(self, context, spec):
        raise RuntimeError("action detail for debugging")


def test_component_exception_messages_are_reported():
    calls: list[str] = []
    outcome = FeatureRunner(
        _registries([(ComponentKind.ACTION, "tests.action", _RaisingAction)]),
        _topology_providers(standalone=lambda: _Provider(calls, "standalone")),
    ).run(_plan((_spec(ComponentKind.ACTION, "activate"),)))

    assert outcome.status is OutcomeStatus.ERROR
    (result,) = outcome.component_results
    assert result.diagnostics["exception"]["message"] == "action detail for debugging"


def test_environment_binding_failure_keeps_the_exception_message():
    calls: list[str] = []
    with patch.object(BoundRunPlan, "materialize", side_effect=RuntimeError("binding exploded")):
        outcome = FeatureRunner(
            _registries([(ComponentKind.ACTION, "tests.action", lambda: _Component(PASS))]),
            _topology_providers(standalone=lambda: _Provider(calls, "standalone")),
        ).run(_plan((_spec(ComponentKind.ACTION, "activate"),)))

    assert outcome.status is OutcomeStatus.ERROR
    assert outcome.diagnostics["error"] == {
        "type": "RuntimeError",
        "message": "binding exploded",
        "phase": "environment binding",
    }
