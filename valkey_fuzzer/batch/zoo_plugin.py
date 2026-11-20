"""The bug zoo as a batch plug-in: the ``zoo`` source, each run's observation, and the case verdicts.

The zoo itself (its cases, builds, and how a case is judged) is :mod:`valkey_fuzzer.zoo`.
"""

import itertools
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from ..schema import ConfigField, ConfigSchema
from ..service import RunService
from ..zoo import ZOO_DIRECTORY, ZooRun, case_report, check_build, detected, load_zoo, observe, zoo_runs
from .hooks import BatchPlugin, BatchSource
from .jobs import Expansion
from .spec import BatchError, BatchOptions, absolute

if TYPE_CHECKING:
    from .pool import BatchRunner


def _normalize(settings: dict[str, Any], base_dir: str, where: str) -> dict[str, Any]:
    settings["builds"] = absolute(settings["builds"], base_dir, f"{where}.builds")
    settings["cases"] = settings["cases"] or []
    if settings["directory"] is not None:
        settings["directory"] = absolute(settings["directory"], base_dir, f"{where}.directory")
    return settings


def _zoo_jobs(
    source: Mapping[str, Any], index: int, service: RunService, options: BatchOptions, expansion: Expansion
) -> None:
    directory = Path(source["directory"] or ZOO_DIRECTORY)
    builds = Path(source["builds"])
    commit, cases = load_zoo(directory, source["cases"])
    zoo = expansion.recorded.get("zoo") or {"valkey_commit": commit, "cases": [], "skipped": []}
    if zoo["valkey_commit"] != commit:
        raise BatchError("every zoo source of a batch must pin the same Valkey commit")
    expansion.recorded["zoo"] = zoo
    checked: set[str] = set()
    for case in cases:
        unmet = case.unmet_requirements()
        if unmet:
            zoo["skipped"].append({"case": case.case_id, "reason": f"needs {', '.join(unmet)}"})
            continue
        runs = zoo_runs(case, builds, service.catalog)
        for path in sorted({run.scenario["deployment"]["valkey_binary"] for run in runs} - checked):
            check_build(path, commit)
            checked.add(path)
        zoo["cases"].append({**case.terms(), "directory": str(case.directory)})
        broken = itertools.count()
        for run in runs:
            stem = Path(run.file).stem
            suffix = "control" if run.control else str(next(broken))
            job = {
                "id": f"zoo-{case.case_id}-{stem}-{suffix}",
                "source": "zoo",
                "label": f"{case.case_id} {run.file} {'control' if run.control else 'broken'}",
                "scenario": dict(run.scenario),
                "zoo": {
                    "case": case.case_id,
                    "file": run.file,
                    "control": run.control,
                    "fail": list(run.fail),
                },
            }
            expansion.add(job)


def _zoo_run(job: Mapping[str, Any]) -> ZooRun:
    zoo = job["zoo"]
    return ZooRun(zoo["case"], zoo["file"], zoo["control"], job["scenario"], tuple(zoo["fail"]))


class ZooPlugin(BatchPlugin):
    """The bug zoo: its source expands to a clean control and broken runs of each case; a run's result records
    what it showed the zoo; the summary judges each case by the terms the batch recorded."""

    name = "zoo"
    source = BatchSource(
        "zoo",
        ConfigSchema(
            {
                "builds": ConfigField(str),
                "cases": ConfigField(list, items=ConfigField(str), default=list, allow_none=True),
                "directory": ConfigField(str, default=None, allow_none=True),
            }
        ),
        _normalize,
        _zoo_jobs,
        shorthand=lambda value: {"builds": value} if isinstance(value, str) else None,
    )

    def record(
        self, job: Mapping[str, Any], run_dir: str, report: Optional[Mapping[str, Any]], result: Mapping[str, Any]
    ) -> Any:
        if "zoo" not in job:
            return None
        observed = observe(report["outcome"] if report is not None else None, result["status"])
        run = _zoo_run(job)
        return {**observed, "detected": None if run.control else detected(run, observed)}

    def summarize(
        self, runner: "BatchRunner", results: Mapping[str, Mapping[str, Any]], main: Sequence[Mapping[str, Any]]
    ) -> tuple[Any, list[str]]:
        zoo = runner.spec.get("zoo")
        if zoo is None:
            return None, []
        cases = []
        for entry in zoo["cases"]:
            jobs = [job for job in runner.jobs if job.get("zoo", {}).get("case") == entry["case"]]
            if not all(job["id"] in results for job in jobs):
                cases.append({"case": entry["case"], "holds": None, "pending": True})
                continue
            rows = [
                (
                    _zoo_run(job),
                    {k: results[job["id"]]["zoo"][k] for k in ("status", "failed")},
                    results[job["id"]]["report"],
                )
                for job in jobs
            ]
            cases.append(case_report(entry, rows))
        summary = {
            "valkey_commit": zoo["valkey_commit"],
            "cases": cases,
            "skipped": zoo["skipped"],
            "held": sum(case.get("holds") is True for case in cases),
            "not_held": [case["case"] for case in cases if case.get("holds") is False],
            "undecided": [case["case"] for case in cases if case.get("undecided")],
        }
        # A case that does not hold fails the batch; an undecided one makes it ERROR (or SKIP_UNSUPPORTED).
        statuses = ["FAIL" for case in cases if case.get("holds") is False]
        statuses += [case["undecided"] for case in cases if case.get("undecided")]
        statuses += ["SKIP_UNSUPPORTED" for _ in zoo["skipped"]]
        return summary, statuses

    def describe(self, entry: Any) -> list[str]:
        if entry is None:
            return []
        lines = []
        for case in entry["cases"]:
            if case.get("pending"):
                lines.append(f"  zoo {case['case']}: not finished")
                continue
            rates = ", ".join(f"{file} {item['detection_rate']:.0%}" for file, item in case["scenarios"].items())
            controls = "/".join(row["status"] for row in case["controls"])
            verdict = case["undecided"] or ("OK" if case["holds"] else "FAIL")
            lines.append(f"  zoo {verdict:<5} {case['case']}: control {controls}; detected {rates}")
        lines += [f"  zoo SKIP {item['case']}: {item['reason']}" for item in entry["skipped"]]
        return lines
