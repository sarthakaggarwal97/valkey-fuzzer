"""``verdict.decide`` over the combinations of status, role, crash, required, trust, evidence violations, missing
required evidence and events, and an unclean teardown."""

import pytest

from tests.support import make_plan
from valkey_fuzzer import verdict
from valkey_fuzzer.plan import ComponentSpec, EvidenceRequirement, RunMode
from valkey_fuzzer.results import ComponentStatus, OutcomeStatus, make_result

PASS, FAIL, ERR = ComponentStatus.PASS, ComponentStatus.FAIL, ComponentStatus.ERROR
VIOLATION = ({"message": "emitted malformed 'x'"},)


def _decide(components, results, *, invoked=None, crash=(), diagnostics=None, **state):
    """The verdict on ``results`` (ID -> status) of a plan of ``components`` (ID -> (role, required, depends_on));
    the FAILs of the components named in ``crash`` report a crash, ``diagnostics`` (ID -> dict) are the results',
    and ``state`` sets the other run state (``required_events`` and ``required_evidence`` go to the plan)."""

    specs = tuple(
        ComponentSpec(f"tests.{role}.{name}", name, role, required=required, depends_on=depends_on)
        for name, (role, required, depends_on) in components.items()
    )
    requirements = {key: state.pop(key) for key in ("required_events", "required_evidence") if key in state}
    mode = RunMode.EXPERIMENT if requirements.get("required_evidence") else RunMode.SMOKE
    plan = make_plan(mode=mode, components=specs, **requirements)
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
            invoked=set(results) if invoked is None else invoked,
            evidence=(),
            committed=lambda name: (),
            **state,
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


LOOK = {"look": ("observer", True, ()), "act": ("action", True, ()), "check": ("oracle", True, ())}


@pytest.mark.parametrize(
    "requirements",
    [
        pytest.param({"required_events": ("tests.vote.granted",)}, id="event"),
        pytest.param({"required_evidence": (EvidenceRequirement("tests.vote-logged", "look"),)}, id="evidence"),
    ],
)
@pytest.mark.parametrize(
    ("results", "status"),
    [
        pytest.param({"look": PASS, "act": PASS, "check": PASS}, "ERROR", id="nothing-failed"),
        pytest.param({"look": PASS, "act": PASS, "check": FAIL}, "FAIL", id="an-oracle-failed-anyway"),
        pytest.param({"look": PASS, "act": FAIL, "check": PASS}, "FAIL", id="an-action-failed-anyway"),
    ],
)
def test_missing_path_evidence_is_an_error_unless_a_check_failed(requirements, results, status):
    # Missing path evidence or a missing required event means the run did not test what it claims; a FAIL an
    # oracle or action found anyway still stands, and the missing item stays in the diagnostics.
    decision = _decide(LOOK, results, **requirements)

    assert decision.status.value == status, decision.summary
    key = "missing_required_events" if "required_events" in requirements else "missing_required_evidence"
    assert decision.diagnostics[key]
    if status == "ERROR":
        assert "the run did not reach what it tests" in decision.summary


TORN_DOWN = {"setup": ("fault", True, ()), "act": ("action", True, ()), "check": ("oracle", True, ())}
_OBLIGATION = {"owner": "setup", "kind": "network.partition", "target": "valkey-0", "undone_at": 1.0}


@pytest.mark.parametrize(
    "unclean",
    [
        pytest.param({"teardown_failed": True}, id="a-teardown-failed"),
        pytest.param({"open_obligations": ({**_OBLIGATION, "undone_by": "runner"},)}, id="the-runner-undid-a-change"),
    ],
)
@pytest.mark.parametrize(
    ("results", "crash", "status"),
    [
        pytest.param({"setup": PASS, "act": PASS, "check": FAIL}, (), "ERROR", id="an-oracle-fail-is-untrusted"),
        pytest.param({"setup": PASS, "act": PASS, "check": FAIL}, ("check",), "FAIL", id="a-crash-stands"),
        pytest.param({"setup": PASS, "act": FAIL, "check": PASS}, (), "FAIL", id="an-action-fail-stands"),
        pytest.param({"setup": PASS, "act": PASS, "check": PASS}, (), "ERROR", id="nothing-failed"),
    ],
)
def test_oracles_judge_only_a_cleanly_torn_down_run(unclean, results, crash, status):
    decision = _decide(TORN_DOWN, results, crash=crash, **unclean)

    assert decision.status.value == status, decision.summary
    if results["check"] is FAIL and not crash:
        assert decision.diagnostics["untrusted_component_failures"] == ("check",)


def test_an_oracle_fail_after_a_cleanup_that_followed_it_stands():
    # A deployment cleanup after the oracles cannot change what they saw.
    decision = _decide(TORN_DOWN, {"setup": PASS, "act": PASS, "check": FAIL}, cleanup_failed=True)
    assert decision.status is OutcomeStatus.FAIL, decision.summary
    assert _decide(TORN_DOWN, dict.fromkeys(TORN_DOWN, PASS), cleanup_failed=True).status is OutcomeStatus.ERROR


@pytest.mark.parametrize(
    ("role", "depends", "status"),
    [
        pytest.param("fault", True, "ERROR", id="a-fault-it-waited-for-never-fired"),
        pytest.param("workload", True, "ERROR", id="its-workload-stopped"),
        pytest.param("observer", True, "ERROR", id="its-observer-failed"),
        pytest.param("fault", False, "FAIL", id="a-failure-that-does-not-depend-on-the-fault"),
    ],
)
def test_a_failure_that_depends_on_a_component_that_could_not_do_its_part_is_untrusted(role, depends, status):
    # The action waits for the effect of "setup"; a crash found before a fault fired does not depend on it.
    components = {"setup": (role, True, ()), "wait": ("action", True, ("setup",) if depends else ())}
    decision = _decide(components, {"setup": ComponentStatus.ERROR, "wait": FAIL})
    assert decision.status.value == status, decision.summary
