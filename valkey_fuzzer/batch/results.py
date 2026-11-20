"""A finished job's result: its status, failure signature, and what the plug-ins judged and recorded."""

import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Optional

from .._data import canonical_json
from ..minimize import empty_signature, failure_signature
from ..reports import REPORT_FILE
from ..results import RunOutcome
from . import hooks
from .spec import worst

# What a worker that could not start its run writes instead of a report.
ERROR_FILE = "error.json"


def read_json(path: str) -> Any:
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def results_of(folder: str) -> dict[str, dict[str, Any]]:
    """Every finished job's result in the batch ``folder``, by job ID."""

    directory = os.path.join(folder, "results")
    if not os.path.isdir(directory):
        return {}
    return {
        name[: -len(".json")]: read_json(os.path.join(directory, name))
        for name in sorted(os.listdir(directory))
        if name.endswith(".json")
    }


def signature_key(experiment: Optional[str], signature: Mapping[str, Any]) -> str:
    """A failure signature's group: the experiment and every signature field but the path evidence; for a
    crash, its causes alone, so one crash is one group whichever experiment hit it."""

    if signature.get("crashes"):
        material: dict[str, Any] = {"crashes": signature["crashes"]}
    else:
        material = {"experiment": experiment, **{k: v for k, v in signature.items() if k != "path_evidence"}}
    return hashlib.sha256(canonical_json(material).encode()).hexdigest()[:12]


def signature_label(experiment: Optional[str], signature: Mapping[str, Any]) -> str:
    """One line naming a failure signature: status, experiment, failing components, first verdict or error;
    for a crash, its causes."""

    if signature.get("crashes"):
        return f"{signature['status']} | crash | {'; '.join(signature['crashes'])[:300]}"
    parts = [signature["status"], experiment or "-"]
    failures = signature["failures"]
    if failures:
        parts.append(",".join(f"{name}:{entry['status']}" for name, entry in sorted(failures.items())))
        first = failures[sorted(failures)[0]]["verdict"]
        if first:
            parts.append(first[0][:160])
    for key in ("missing_events", "missing_evidence"):
        if signature[key]:
            parts.append(f"{key.replace('_', ' ')}: {', '.join(signature[key])}")
    if signature["error_type"]:
        parts.append(f"{signature['error_type']} during {signature['error_phase']}")
    return " | ".join(parts)


def _signature(outcome: Optional[RunOutcome], error: Optional[Mapping[str, Any]]) -> dict[str, Any]:
    """:func:`~valkey_fuzzer.minimize.failure_signature` of a run's outcome; of its ``error`` when it left none."""

    if outcome is not None:
        required = [item.evidence_id for item in outcome.plan.required_evidence]
        return failure_signature(outcome, required)
    return empty_signature("ERROR", error_type=error["type"], error_phase=error.get("phase"))


def _leaked(leaks: Sequence[Mapping[str, Any]]) -> str:
    """The kinds of what a run left behind, as its summary and signature label name them."""

    return ", ".join(sorted({item["kind"] for item in leaks}))


@dataclass
class Verdict:
    """A finished job's verdict as the batch forms it, for the plug-ins to amend before its leaks count."""

    status: str
    summary: str
    signature: dict[str, Any]

    def with_leaks(self, leaks: Sequence[Mapping[str, Any]]) -> "Verdict":
        """The verdict once ``leaks`` (the servers, chains, mounts, or directories the run left behind: its cleanup
        failed) count: an ERROR whose summary says them first, so a shortened one still does; itself for none."""

        if not leaks:
            return self
        left = f"left {len(leaks)} leftover{'' if len(leaks) == 1 else 's'}: {_leaked(leaks)}"
        summary = f"passed its checks but {left}" if self.status == "PASS" else f"{left}; {self.summary}"
        return Verdict(worst([self.status, "ERROR"]), summary, self.signature)


def job_result(
    job: Mapping[str, Any],
    run_dir: str,
    *,
    exit_code: Optional[int],
    timed_out: bool,
    leaks: Sequence[Mapping[str, Any]],
    extra: Mapping[str, Any],
) -> dict[str, Any]:
    """The result of one finished job, from its run folder: status, failure signature, and the plug-ins' fields."""

    report_path = os.path.join(run_dir, REPORT_FILE)
    report: Optional[dict[str, Any]] = None
    error: Optional[dict[str, Any]] = None
    if os.path.exists(report_path):
        try:
            report = read_json(report_path)
        except (OSError, ValueError) as exc:
            error = {"type": "UnreadableReport", "message": str(exc), "phase": "batch"}
    elif os.path.exists(os.path.join(run_dir, ERROR_FILE)):
        error = read_json(os.path.join(run_dir, ERROR_FILE))
    if report is None and error is None:
        reason = "it exceeded its deadline" if timed_out else f"the worker exited with code {exit_code}"
        error = {
            "type": "Deadline" if timed_out else "WorkerExit",
            "message": f"no run report: {reason}",
            "phase": "batch",
        }
    status = report["status"] if report is not None else "ERROR"
    scenario = (report or {}).get("scenario") or job.get("scenario") or {}
    experiment = (scenario.get("experiment") or {}).get("id") or job.get("generate", {}).get("experiment")
    outcome = None
    if report is not None:
        try:
            outcome = RunOutcome.from_data(report["outcome"])
        except ValueError as exc:
            status, error = "ERROR", {"type": "UnreadableReport", "message": str(exc), "phase": "batch"}
    verdict = Verdict(
        status,
        report["summary"] if report is not None else f"{error['type']}: {error['message']}",
        _signature(outcome, error),
    )
    judged = {}
    for plugin in hooks.PLUGINS:
        field = plugin.judge(job, run_dir, report, verdict)
        if field is not None:
            judged[plugin.name] = field
    verdict = verdict.with_leaks(leaks)
    signature = verdict.signature
    result = {
        "id": job["id"],
        "source": job["source"],
        "status": verdict.status,
        "summary": verdict.summary,
        "experiment": experiment,
        "seed": scenario.get("seed", job.get("generate", {}).get("seed")),
        # The attempt that drew the seed's scenario: another number of cores can draw another scenario.
        "attempt": ((report or {}).get("reproducer", {}).get("generated") or {}).get("attempt"),
        "mode": (((report or {}).get("outcome") or {}).get("plan") or {}).get("mode"),
        "report": os.path.join("runs", job["id"], REPORT_FILE) if report is not None else None,
        "coverage_key": ((report or {}).get("coverage") or {}).get("key"),
        "exit_code": exit_code,
        "timed_out": timed_out,
        "leaks": [dict(item) for item in leaks],
        **judged,
        "signature": None if verdict.status == "PASS" else signature,
        "signature_key": None if verdict.status == "PASS" else signature_key(experiment, signature),
        "signature_label": None if verdict.status == "PASS" else signature_label(experiment, signature),
        **extra,
    }
    if leaks and not signature["error_type"] and signature["status"] == "PASS":
        result["signature_label"] = f"ERROR | {experiment} | leaked {_leaked(leaks)}"
    for plugin in hooks.PLUGINS:
        field = plugin.record(job, run_dir, report, result)
        if field is not None:
            result[plugin.name] = field
    return result
