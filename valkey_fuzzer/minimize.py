"""Failure signatures and the minimizer.

A **failure signature** is what must stay true for two runs to count as the
same failure: the terminal status, each failing component's status and
verdict (error class and phase, or its summary with numbers and hex IDs
masked), the missing required events and evidence, the run error's class and
phase, and the path evidence that was observed. A run in which the system
under test crashed is that crash: a failing component that found one reports
it in its result's diagnostics as ``crashes`` (instance to cause, such as an
assertion's file:line and condition), and the signature is then the causes,
numbers and hex IDs masked, and nothing that failed after them. The
minimizer keeps a reduction only when the signature holds; the batch runner
groups its non-PASS runs by it.

The **minimizer** greedily shrinks a failing scenario (deployment size,
configuration keys, numeric inputs), one reduction at a time, most aggressive
first.
"""

import copy
import re
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Callable, Optional

from ._data import to_plain_data
from .contracts import ExecutionPlan, RunOutcome

# Numbers and hex IDs (counts, ports, instance numbers, digests) differ between
# a run and its reductions without making it a different failure.
_RUN_DETAIL = re.compile(r"\b[0-9a-fA-F]*[0-9][0-9a-fA-F]*\b")


def _masked(text: str) -> str:
    return " ".join(_RUN_DETAIL.sub("N", text).split())


def _verdict(summary: str) -> list[str]:
    """A component's summary as its sorted distinct clauses, with numbers and hex IDs masked."""

    clauses = (_masked(clause) for clause in summary.split(";"))
    return sorted({clause for clause in clauses if clause})


def _crashes(results: Iterable[Any]) -> list[str]:
    """The masked crash causes failing ``results`` report as ``diagnostics.crashes`` (a mapping or a list)."""

    causes = set()
    for result in results:
        reported = to_plain_data(result.diagnostics).get("crashes") or ()
        for cause in reported.values() if isinstance(reported, Mapping) else reported:
            if isinstance(cause, str) and cause.strip():
                causes.add(_masked(cause))
    return sorted(causes)


def empty_signature(status: str, **fields: Any) -> dict[str, Any]:
    """A failure signature with nothing failing; ``fields`` fill in what the failure has."""

    return {
        "status": status,
        "crashes": [],
        "failing_components": [],
        "failures": {},
        "missing_events": [],
        "missing_evidence": [],
        "path_evidence": [],
        "error_type": None,
        "error_phase": None,
        **fields,
    }


def failure_signature(outcome: RunOutcome, required_evidence: Iterable[str] = ()) -> dict[str, Any]:
    """What must stay true for a reduced scenario to count as the same failure.

    ``failures`` holds each failing component's status and verdict: for a call
    that raised, the error class and phase; otherwise its summary's clauses
    with numbers and hex IDs masked, so an oracle failing for another reason
    is another failure. A run whose failing components report crashes
    (``diagnostics.crashes``) is its crash causes alone: ``crashes``, with
    every other field empty, since what failed after a crash varies with
    the experiment and the timing.
    """

    diagnostics = to_plain_data(outcome.diagnostics)
    failing = [result for result in outcome.component_results if result.status.value in ("FAIL", "ERROR")]
    crashes = _crashes(failing)
    if crashes:
        return empty_signature(outcome.status.value, crashes=crashes)
    failures = {}
    for result in failing:
        exception = to_plain_data(result.diagnostics).get("exception")
        if isinstance(exception, Mapping) and exception.get("type"):
            verdict = [f"{exception['type']} during {exception.get('phase')}"]
        else:
            verdict = _verdict(result.summary)
        failures[result.component_id] = {"status": result.status.value, "verdict": verdict}
    missing = sorted(item["evidence_id"] for item in diagnostics.get("missing_required_evidence", ()))
    observed = {item.evidence_id for item in outcome.all_evidence}
    error = diagnostics.get("error")
    return {
        "status": outcome.status.value,
        "crashes": [],
        "failing_components": sorted(failures),
        "failures": failures,
        "missing_events": sorted(diagnostics.get("missing_required_events", ())),
        "missing_evidence": missing,
        "path_evidence": sorted(observed & set(required_evidence)),
        # An ERROR is the same failure only if the same kind of error, in the same phase, caused it.
        "error_type": error.get("type") if isinstance(error, Mapping) else None,
        "error_phase": error.get("phase") if isinstance(error, Mapping) else None,
    }


def same_failure(reference: Mapping[str, Any], candidate: Mapping[str, Any]) -> bool:
    """Whether ``candidate`` fails the way ``reference`` did: the same crashes, if the reference crashed;
    otherwise every signature field but the path evidence matches, and the candidate still observed the
    reference's path evidence."""

    if reference.get("crashes"):
        return candidate.get("crashes") == reference["crashes"]
    keys = (
        "crashes",
        "status",
        "failing_components",
        "failures",
        "missing_events",
        "missing_evidence",
        "error_type",
        "error_phase",
    )
    return all(candidate.get(key) == reference.get(key) for key in keys) and set(reference["path_evidence"]) <= set(
        candidate["path_evidence"]
    )


def reduction_candidates(
    scenario: Mapping[str, Any],
    experiment_inputs: Mapping[str, Any],
    deployment_requirements: Optional[Mapping[str, Any]] = None,
) -> list[tuple[str, dict]]:
    """Single-step simplifications, most aggressive first.

    ``experiment_inputs`` is the experiment's input schema; its minimums bound
    the reductions of numeric inputs. The minimums in the experiment's
    ``deployment_requirements`` bound the reductions of deployment fields.
    """

    candidates: list[tuple[str, dict]] = []
    deployment = scenario.get("deployment", {})
    requirements = deployment_requirements or {}
    replicas = deployment.get("replicas_per_primary")
    minimum_replicas = (requirements.get("replicas_per_primary") or {}).get("minimum", 0)
    if isinstance(replicas, int) and replicas > minimum_replicas:
        for value in sorted({minimum_replicas, replicas - 1}):
            if value < replicas:
                reduced = copy.deepcopy(dict(scenario))
                reduced["deployment"]["replicas_per_primary"] = value
                candidates.append((f"deployment.replicas_per_primary={value}", reduced))
    primaries = deployment.get("primaries")
    minimum_primaries = max(3, (requirements.get("primaries") or {}).get("minimum", 3))
    if deployment.get("mode") == "cluster" and isinstance(primaries, int) and primaries > minimum_primaries:
        reduced = copy.deepcopy(dict(scenario))
        reduced["deployment"]["primaries"] = minimum_primaries
        candidates.append((f"deployment.primaries={minimum_primaries}", reduced))
    for key in sorted(deployment.get("server_config") or {}):
        reduced = copy.deepcopy(dict(scenario))
        del reduced["deployment"]["server_config"][key]
        candidates.append((f"drop deployment.server_config.{key}", reduced))
    for selector in sorted(deployment.get("instance_configs") or {}):
        reduced = copy.deepcopy(dict(scenario))
        del reduced["deployment"]["instance_configs"][selector]
        candidates.append((f"drop deployment.instance_configs.{selector}", reduced))
    properties = (experiment_inputs or {}).get("properties", {})
    inputs = (scenario.get("experiment") or {}).get("inputs") or {}
    for name in sorted(inputs):
        value = inputs[name]
        rule = properties.get(name, {})
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            continue
        minimum = rule.get("minimum", 0)
        for proposal in sorted({minimum, value // 2 if isinstance(value, int) else value / 2}):
            if proposal < value and proposal >= minimum:
                reduced = copy.deepcopy(dict(scenario))
                reduced["experiment"]["inputs"][name] = proposal
                candidates.append((f"experiment.inputs.{name}={proposal}", reduced))
    return candidates


def minimize(
    scenario: Mapping[str, Any],
    reference: Mapping[str, Any],
    *,
    compile_fn: Callable[[dict], ExecutionPlan],
    execute_fn: Callable[[dict, ExecutionPlan], RunOutcome],
    experiment_inputs: Mapping[str, Any],
    required_evidence: Sequence[str],
    deployment_requirements: Optional[Mapping[str, Any]] = None,
    max_attempts: int = 50,
    log: Optional[Callable[[str], None]] = None,
) -> dict[str, Any]:
    """Greedy one-step-at-a-time reduction preserving ``reference``."""

    current = copy.deepcopy(dict(scenario))
    attempts = []
    accepted = []
    budget = max_attempts
    progress = True
    while progress and budget > 0:
        progress = False
        for description, candidate in reduction_candidates(current, experiment_inputs, deployment_requirements):
            if budget <= 0:
                break
            try:
                plan = compile_fn(candidate)
            except Exception as exc:
                attempts.append({"step": description, "result": f"invalid: {exc}"})
                continue
            budget -= 1
            outcome = execute_fn(candidate, plan)
            signature = failure_signature(outcome, required_evidence)
            kept = same_failure(reference, signature)
            attempts.append(
                {"step": description, "result": "kept" if kept else "rejected", "status": signature["status"]}
            )
            if log is not None:
                log(f"  {description}: {'kept' if kept else 'rejected'} ({signature['status']})")
            if kept:
                current = candidate
                accepted.append(description)
                progress = True
                break
    return {"scenario": current, "accepted": accepted, "attempts": attempts, "signature": dict(reference)}


__all__ = ["empty_signature", "failure_signature", "minimize", "reduction_candidates", "same_failure"]
