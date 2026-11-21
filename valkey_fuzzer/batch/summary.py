"""The summary: counts, failure signatures with their serial re-runs, the plug-ins' entries, leaks, and the batch's
status and exit code."""

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from ..clock import utc_iso
from ..outcomes import EXIT_INTERRUPTED, exit_code
from ..reports import FORMAT_VERSION
from . import hooks
from .results import results_of
from .spec import worst

if TYPE_CHECKING:
    from .pool import BatchRunner


def _rerun_verdict(reruns: Mapping[str, Any]) -> str:
    """How a signature's serial re-runs went: ``reproduced`` (all failed the same way), ``intermittent``
    (some did), ``failed differently`` (none did, but some failed another way), or ``not reproduced``
    (all passed)."""

    ran, reproduced = len(reruns["runs"]), reruns["reproduced"]
    if reproduced == ran:
        return "reproduced"
    if reproduced:
        return "intermittent"
    if any(run["status"] != "PASS" for run in reruns["runs"]):
        return "failed differently"
    return "not reproduced"


def summarize(runner: "BatchRunner") -> dict[str, Any]:
    """The summary of ``runner``'s batch folder, as it stands."""

    results = results_of(runner.folder)
    # The jobs the batch counts: a plug-in's (the zoo's) are its own to judge.
    main = [results[job["id"]] for job in runner.jobs if job["id"] in results and not hooks.plugin_job(job)]
    # Smoke runs (lifecycle only, no coverage claim) are counted apart: their PASS is not an experiment's.
    counts: dict[str, int] = {}
    smoke: dict[str, int] = {}
    for result in main:
        tally = smoke if result.get("mode") == "smoke" else counts
        tally[result["status"]] = tally.get(result["status"], 0) + 1
    groups: dict[str, dict[str, Any]] = {}
    for result in main:
        if not result["signature_key"]:
            continue
        group = groups.setdefault(
            result["signature_key"],
            {
                "key": result["signature_key"],
                "label": result["signature_label"],
                "status": result["status"],
                "experiment": result["experiment"],
                "signature": result["signature"],
                "runs": [],
            },
        )
        group["runs"].append(result["id"])
    for result in results.values():
        if result["source"] != "serial":
            continue
        original = results.get(result["origin"]["job"])
        group = groups.get(original["signature_key"]) if original else None
        if group is None:
            continue
        reruns = group.setdefault("serial_reruns", {"runs": [], "reproduced": 0})
        same = result["signature_key"] == original["signature_key"]
        reruns["runs"].append(
            {
                "id": result["id"],
                "status": result["status"],
                "same_signature": same,
                "signature_key": result["signature_key"],
                "signature_label": result["signature_label"],
            }
        )
        reruns["reproduced"] += same
    for group in groups.values():
        group["count"] = len(group["runs"])
        reruns = group.get("serial_reruns")
        if reruns:
            reruns["verdict"] = _rerun_verdict(reruns)
    plugins = {plugin.name: plugin.summarize(runner, results, main) for plugin in hooks.PLUGINS}
    leaks = [{"run": result["id"], **leak} for result in results.values() for leak in result["leaks"]]
    coverage: dict[str, dict[str, Any]] = {}
    for result in main:
        entry = coverage.setdefault(result["experiment"] or "-", {"runs": 0, "tuples": set()})
        entry["runs"] += 1
        if result["coverage_key"]:
            entry["tuples"].add(result["coverage_key"])
    finished = len([job for job in runner.jobs if job["id"] in results])
    # A matrix combination that does not compile is a configuration error: a misspelled dimension value
    # must not leave a PASS, or a skip, over the other combinations.
    statuses = [result["status"] for result in main] + ["ERROR" for _ in runner.spec["invalid"]]
    for _, added in plugins.values():
        statuses += added
    status = worst(statuses)
    complete = finished == len(runner.jobs) and not runner.stop_requested
    return {
        "format_version": FORMAT_VERSION,
        "batch_id": runner.options.batch_id,
        "folder": runner.folder,
        "profile": runner.options.profile,
        "created_at": runner.spec["created_at"],
        "updated_at": utc_iso(),
        "complete": complete,
        "status": status,
        "exit_code": exit_code(status) if complete else EXIT_INTERRUPTED,
        "jobs": len(runner.jobs),
        "finished": finished,
        "counts": counts,
        "smoke": smoke,
        "cores": runner.run_cores,
        "runs": [
            {
                key: result.get(key)
                for key in (
                    "id",
                    "source",
                    "status",
                    "summary",
                    "experiment",
                    "seed",
                    "attempt",
                    "mode",
                    "report",
                    "signature_key",
                    "duration_s",
                    "worker",
                )
            }
            | {"leaks": len(result["leaks"]), "origin": result.get("origin")}
            for result in results.values()
        ],
        "signatures": sorted(groups.values(), key=lambda group: (-group["count"], group["label"])),
        **{name: entry for name, (entry, _) in plugins.items()},
        "leaks": leaks,
        "invalid": runner.spec["invalid"],
        "coverage": {
            name: {"runs": entry["runs"], "tuples": len(entry["tuples"])} for name, entry in sorted(coverage.items())
        },
    }
