"""``verdict.decide`` over the combinations of status, role, crash, required, trust and evidence violations."""

import pytest

from tests.support import make_plan
from valkey_fuzzer import verdict
from valkey_fuzzer.plan import ComponentSpec, RunMode
from valkey_fuzzer.results import ComponentStatus, OutcomeStatus, make_result

PASS, FAIL, ERR = ComponentStatus.PASS, ComponentStatus.FAIL, ComponentStatus.ERROR
VIOLATION = ({"message": "emitted malformed 'x'"},)


def _decide(components, results, *, crash=(), violations=(), runner_failure=None, diagnostics=None):
    """``components``: ID -> (role, required, depends_on); ``results``: ID -> status."""

    specs = tuple(
        ComponentSpec(f"tests.{role}.{name}", name, role, required=required, depends_on=depends_on)
        for name, (role, required, depends_on) in components.items()
    )
    plan = make_plan(mode=RunMode.SMOKE, components=specs)
    by_id = {spec.id: spec for spec in specs}
    return verdict.decide(
        verdict.RunState(
            plan=plan,
            results={
                name: make_result(
                    by_id[name], status, "ran", crash=name in crash, diagnostics=(diagnostics or {}).get(name, {})
                )
                for name, status in results.items()
            },
            invoked=set(results),
            evidence=(),
            committed=lambda name: (),
            violations=violations,
            runner_failure=runner_failure,
        )
    )


@pytest.mark.parametrize(
    ("role", "required", "crash", "violations", "expected"),
    [
        ("oracle", True, False, (), OutcomeStatus.FAIL),
        ("oracle", True, True, (), OutcomeStatus.FAIL),
        ("workload", False, False, (), OutcomeStatus.PASS),  # a non-crash FAIL of a component that does not judge
        ("workload", False, True, (), OutcomeStatus.FAIL),  # a crash stands whether or not it is required
        ("observer", False, True, (), OutcomeStatus.FAIL),
        ("fault", False, True, (), OutcomeStatus.FAIL),
        ("action", False, False, (), OutcomeStatus.PASS),  # an optional action's own FAIL is not a finding
        ("action", False, True, (), OutcomeStatus.FAIL),
        ("oracle", True, True, VIOLATION, OutcomeStatus.FAIL),  # a trusted crash stands over malformed evidence
        ("oracle", True, False, VIOLATION, OutcomeStatus.ERROR),
        ("workload", False, True, VIOLATION, OutcomeStatus.FAIL),
        ("workload", True, False, VIOLATION, OutcomeStatus.ERROR),
    ],
)
def test_a_failed_component_decides_by_role_crash_required_and_violations(role, required, crash, violations, expected):
    decision = _decide({"c": (role, required, ())}, {"c": FAIL}, crash={"c"} if crash else (), violations=violations)

    assert decision.status is expected, decision.summary
    if violations:
        assert decision.diagnostics["contract_violations"] == VIOLATION


def test_a_crash_a_teardown_replaced_is_still_a_finding():
    # The runner keeps the crash of a result whose stop failed in diagnostics; the component is ERROR by then.
    components = {"load": ("workload", True, ()), "check": ("oracle", True, ())}
    decision = _decide(
        components,
        {"load": ERR, "check": PASS},
        diagnostics={"load": {verdict.CRASH_BEFORE_TEARDOWN: "server crashed"}},
    )

    assert decision.status is OutcomeStatus.FAIL and decision.trusted


@pytest.mark.parametrize(("depends", "expected"), [((), OutcomeStatus.FAIL), (("broken",), OutcomeStatus.ERROR)])
def test_a_non_crash_fail_stands_over_a_runner_failure_unless_it_depends_on_the_failed_component(depends, expected):
    components = {
        "found": ("action", True, depends),
        "broken": ("action", True, ()),
    }
    decision = _decide(
        components, {"found": FAIL, "broken": ERR}, runner_failure="action.run: RuntimeError: broken blew up"
    )

    assert decision.status is expected, decision.summary
