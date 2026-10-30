"""Outcome calculation: how a run's results, events, evidence, and errors become its verdict, and how it prints.

Only an oracle or action FAIL judges the system under test. A required
component that fails in any other role, or that is skipped, leaves no
trustworthy verdict, so the run is ERROR. Missing required evidence is a
finding only when its source looked at the server (an observer, action, or
oracle, see ``RoleLifecycle.witnesses``), did not report that it could not
look (``ComponentResult.unobserved``), and the run reached the state it is
about (``ComponentResult.preconditions``); otherwise nothing was established and,
without another finding, the run is ERROR. :func:`decide` applies the
precedence that picks which of several problems names the outcome, and
:func:`environment_status` the one environment teardown adds on top.
"""

from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Optional, Union

import yaml

from ._data import to_plain_data
from .contracts import (
    ROLES,
    ComponentResult,
    ComponentSpec,
    ComponentStatus,
    Evidence,
    ExecutionPlan,
    OutcomeStatus,
    RunOutcome,
    evidence_of,
)

# The process exit codes of every command (docs/cli.md#exit-codes).
EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_HARNESS_ERROR = 2
EXIT_UNSUPPORTED = 3
EXIT_INTERRUPTED = 130

EXIT_CODES = {
    OutcomeStatus.PASS: EXIT_SUCCESS,
    OutcomeStatus.FAIL: EXIT_FAILURE,
    OutcomeStatus.ERROR: EXIT_HARNESS_ERROR,
    OutcomeStatus.SKIP_UNSUPPORTED: EXIT_UNSUPPORTED,
}


def exit_code(status: Union[OutcomeStatus, str], *, allow_unsupported: bool = False) -> int:
    """The documented exit code for an outcome status; ``allow_unsupported`` turns SKIP_UNSUPPORTED into success."""

    status = OutcomeStatus(status)
    if status is OutcomeStatus.SKIP_UNSUPPORTED and allow_unsupported:
        return EXIT_SUCCESS
    return EXIT_CODES[status]


def required_components(plan: ExecutionPlan) -> frozenset[str]:
    """Required components, evidence sources, and everything they depend on."""

    by_id = {component.id: component for component in plan.components}
    required = {component.id for component in plan.components if component.required}
    required.update(item.source for item in plan.required_evidence if item.source is not None)
    pending = list(required)
    while pending:
        for dependency in by_id[pending.pop()].depends_on:
            if dependency not in required:
                required.add(dependency)
                pending.append(dependency)
    return frozenset(required)


def missing_evidence(plan: ExecutionPlan, evidence: Sequence[Evidence]) -> tuple[Mapping[str, Any], ...]:
    """Each requirement the observed ``evidence`` does not meet, with its observed count."""

    missing = []
    for requirement in plan.required_evidence:
        count = len(evidence_of(evidence, requirement.evidence_id, requirement.source))
        if count < requirement.min_count:
            missing.append({**to_plain_data(requirement), "observed_count": count})
    return tuple(missing)


def unestablished_evidence(
    plan: ExecutionPlan,
    missing: Sequence[Mapping[str, Any]],
    results: Mapping[str, ComponentResult],
    evidence: Sequence[Evidence] = (),
) -> tuple[Mapping[str, Any], ...]:
    """The ``missing`` requirements whose absence is no finding about the server.

    A requirement is unestablished when its source does not witness (a
    workload, fault, or capability), when the component that would record it
    reported it ``unobserved`` (the server did not answer, a log could not be
    read), or when its precondition evidence was never recorded: the run did
    not reach the state it is about. Without a source, only those reports
    count.
    """

    roles = {component.id: component.role for component in plan.components}

    def unobserved(component_id: str, evidence_id: str) -> bool:
        result = results.get(component_id)
        if result is None:
            return False
        precondition = result.preconditions.get(evidence_id)
        return evidence_id in result.unobserved or (
            precondition is not None and not evidence_of(evidence, precondition)
        )

    unestablished = []
    for item in missing:
        source, evidence_id = item.get("source"), item["evidence_id"]
        if source is None:
            if any(unobserved(component_id, evidence_id) for component_id in results):
                unestablished.append(item)
        elif not ROLES[roles[source]].witnesses or unobserved(source, evidence_id):
            unestablished.append(item)
    return tuple(unestablished)


def _named(items: Sequence[Mapping[str, Any]]) -> str:
    named = [f"{item['evidence_id']} from {item.get('source') or 'any component'}" for item in items]
    more = f", and {len(named) - 5} more" if len(named) > 5 else ""
    return ", ".join(named[:5]) + more


@dataclass(frozen=True)
class Verdict:
    """A run's status and summary, the diagnostics behind them, and whether a
    trustworthy failure of the system under test was established."""

    status: OutcomeStatus
    summary: str
    diagnostics: Mapping[str, Any] = field(default_factory=dict)
    trusted: bool = False


def decide(
    plan: ExecutionPlan,
    ordered: Sequence[ComponentSpec],
    required: Collection[str],
    results: Mapping[str, ComponentResult],
    invoked: Collection[str],
    evidence: Sequence[Evidence],
    committed: Callable[[str], Sequence[Any]],
    *,
    terminal: Optional[tuple[OutcomeStatus, str]] = None,
    runner_failure: Optional[str] = None,
    cleanup_failed: bool = False,
    violations: Sequence[Mapping[str, Any]] = (),
    open_obligations: Sequence[Mapping[str, Any]] = (),
) -> Verdict:
    """The verdict for one run.

    ``committed(name)`` returns the committed events with that name;
    ``terminal`` is a status and summary that ended the run early;
    ``runner_failure`` says in which phase and why the runner itself failed,
    if it did; ``cleanup_failed`` reports a failed component or deployment teardown;
    ``violations`` are the evidence contract's, each of which makes the run
    ERROR: what a component emitted against its declarations proves nothing;
    ``open_obligations`` are changes their owner did not undo on its first
    try, which make the run ERROR too: the deployment was not in the state
    the plan describes.
    """

    required_failures = tuple(
        spec.id
        for spec in ordered
        if spec.id in required
        and spec.id in results
        and results[spec.id].status in (ComponentStatus.FAIL, ComponentStatus.SKIP)
    )
    # Root causes are required components that ran and did not pass; blocked
    # components are their consequences. Only a verdict-role FAIL is
    # evidence about the system under test.
    invoked_failures = [spec for spec in ordered if spec.id in required_failures and spec.id in invoked]
    verdict_failures = tuple(
        spec.id
        for spec in invoked_failures
        if ROLES[spec.role].judges and results[spec.id].status is ComponentStatus.FAIL
    )
    untrusted = tuple(spec.id for spec in invoked_failures if spec.id not in verdict_failures)
    missing_events = tuple(name for name in plan.required_events if not committed(name))
    missing = missing_evidence(plan, evidence)
    unestablished = unestablished_evidence(plan, missing, results, evidence)
    trusted = bool(verdict_failures) and not untrusted and terminal is None

    diagnostics: dict[str, Any] = {}
    if violations:
        diagnostics["contract_violations"] = tuple(violations)
    if terminal is None:
        for key, value in (
            ("missing_required_events", missing_events),
            ("missing_required_evidence", missing),
            ("unestablished_required_evidence", unestablished),
            ("required_component_failures", required_failures),
            ("untrusted_component_failures", untrusted),
        ):
            if value:
                diagnostics[key] = value

    if violations:
        more = f" (and {len(violations) - 1} more)" if len(violations) > 1 else ""
        status, summary = OutcomeStatus.ERROR, f"evidence contract violated: {violations[0]['message']}{more}"
        trusted = False
    elif open_obligations:
        first = open_obligations[0]
        how = "is still active" if "undone_at" not in first else f"was undone by the {first['undone_by']}"
        if first.get("errors") and "undone_at" in first:
            how += " after a failed undo"
        more = f" (and {len(open_obligations) - 1} more)" if len(open_obligations) > 1 else ""
        status = OutcomeStatus.ERROR
        summary = f"component {first['owner']!r} did not undo its {first['kind']} on {first['target']}: it {how}{more}"
        trusted = False
    elif runner_failure is not None:
        status, summary = OutcomeStatus.ERROR, f"runner execution failed during {runner_failure}"
    elif terminal is not None and terminal[0] is OutcomeStatus.ERROR:
        status, summary = OutcomeStatus.ERROR, terminal[1]
    elif terminal is None and untrusted:
        status = OutcomeStatus.ERROR
        summary = (
            "no trustworthy verdict is possible; required components failed outside an oracle "
            f"or action, or were skipped: {', '.join(untrusted)}"
        )
    elif cleanup_failed and not trusted:
        status, summary = OutcomeStatus.ERROR, "cleanup failed before a trustworthy failure was established"
    elif terminal is not None:
        status, summary = terminal
    elif unestablished and not (required_failures or missing_events or len(unestablished) < len(missing)):
        # Nothing looked at the server and found it wanting: the harness could not tell.
        status = OutcomeStatus.ERROR
        summary = (
            f"required evidence was not established: {_named(unestablished)}; its source does not judge the "
            "server or could not observe it, so no trustworthy verdict is possible"
        )
    elif required_failures or missing_events or missing:
        reasons = []
        if required_failures:
            reasons.append(f"required components did not pass: {', '.join(required_failures)}")
        if missing_events:
            reasons.append(f"required events were not observed: {', '.join(missing_events)}")
        if missing:
            reasons.append(f"required evidence was not observed: {_named(missing)}")
        status, summary = OutcomeStatus.FAIL, "; ".join(reasons)
    elif cleanup_failed:
        status, summary = OutcomeStatus.ERROR, "cleanup failed after execution"
    else:
        status, summary = OutcomeStatus.PASS, "experiment passed"
    return Verdict(status, summary, diagnostics, trusted)


def environment_status(
    status: OutcomeStatus, summary: str, *, trusted: bool, cleaned: bool
) -> tuple[OutcomeStatus, str]:
    """The status once the environment is released; a failed release can only make it worse.

    A PASS or SKIP_UNSUPPORTED becomes ERROR, and so does a FAIL that was not
    yet a trustworthy failure of the system under test.
    """

    if not cleaned:
        if status in (OutcomeStatus.PASS, OutcomeStatus.SKIP_UNSUPPORTED):
            return OutcomeStatus.ERROR, "environment cleanup failed after execution"
        if status is OutcomeStatus.FAIL and not trusted:
            return (
                OutcomeStatus.ERROR,
                "environment cleanup failed before a trustworthy failure was established",
            )
    return status, summary


# ---------------------------------------------------------------------------
# Printing an outcome (the commands and the batch worker)
# ---------------------------------------------------------------------------


def _duration(outcome: RunOutcome) -> float:
    if outcome.started_at is None or outcome.ended_at is None:
        return 0.0
    return max(0.0, outcome.ended_at - outcome.started_at)


def _print_summary(outcome: RunOutcome) -> None:
    counts = {status: 0 for status in ComponentStatus}
    for result in outcome.component_results:
        counts[result.status] += 1
    print(f"\nScenario: {outcome.plan.scenario_id}")
    print(f"Experiment: {outcome.plan.experiment_id}")
    print(f"Status: {outcome.status.value}")
    print(f"Mode: {outcome.plan.mode.value}")
    print(f"Deployment: {outcome.plan.deployment_mode}")
    print(f"Duration: {_duration(outcome):.2f}s")
    print(
        "Components: "
        f"{counts[ComponentStatus.PASS]} passed, "
        f"{counts[ComponentStatus.FAIL]} failed, "
        f"{counts[ComponentStatus.SKIP]} skipped, "
        f"{counts[ComponentStatus.ERROR]} errors"
    )
    print(f"Evidence: {len(outcome.all_evidence)}")
    if outcome.valkey_sha:
        print(f"Valkey SHA: {outcome.valkey_sha}")
    print(f"Summary: {outcome.summary}")


def _yaml_lines(value: Any) -> list[str]:
    return yaml.safe_dump(to_plain_data(value), default_flow_style=False, sort_keys=False).rstrip().splitlines()


def print_outcome(outcome: RunOutcome, *, verbose: bool = False) -> None:
    """A run's summary; ``verbose`` adds every component's result and the run diagnostics."""

    _print_summary(outcome)
    if not verbose:
        return
    results_by_id = {result.component_id: result for result in outcome.component_results}

    print("\nComponent Results:")
    for component in outcome.plan.components:
        result = results_by_id.get(component.id)
        if result is None:
            print(f"  [NOT_RUN] {component.id} ({component.role.value}: {component.use})")
            continue

        duration = 0.0
        if result.started_at is not None and result.ended_at is not None:
            duration = max(0.0, result.ended_at - result.started_at)
        print(f"  [{result.status.value}] {component.id} ({component.role.value}: {component.use}, {duration:.2f}s)")
        print(f"    Summary: {result.summary}")
        print(f"    Evidence: {len(result.evidence)}")
        if result.diagnostics:
            print("    Diagnostics:")
            for line in _yaml_lines(result.diagnostics):
                print(f"      {line}")

    if outcome.diagnostics:
        print("\nRun Diagnostics:")
        for line in _yaml_lines(outcome.diagnostics):
            print(f"  {line}")
