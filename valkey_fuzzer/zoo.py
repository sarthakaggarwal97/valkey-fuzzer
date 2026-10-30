"""The bug zoo: deliberately broken Valkey builds and configurations the fuzzer must catch.

Each case is a directory ``zoo/<case id>/`` holding:

- ``case.yaml``: what is broken (``description``), which check must catch it
  (``catches``), the ``scenarios`` that must fail on it, each with the
  components that must report ``FAIL`` (``fail``) and the required evidence
  that must be missing (``missing``), how many ``runs`` to make,
  the ``min_detection_rate`` they must reach, optional ``overrides`` (a
  partial scenario merged onto every scenario of the broken run), and optional
  ``requires`` (``linux``, ``root``);
- ``patch.diff`` (a build-level case): a patch to Valkey at the commit in
  ``zoo/valkey-commit``, which ``zoo/build.sh`` builds into
  ``<builds>/<case id>/src/valkey-server``. A case without one is
  configuration-level: its ``overrides`` break a clean build;
- the scenario files ``case.yaml`` names. They leave out
  ``deployment.valkey_binary``: the zoo picks the build.

Every scenario runs once on the clean build at the pinned commit
(``<builds>/clean/src/valkey-server``) without the overrides, and must PASS:
that control is what makes the case prove anything. It then runs ``runs`` times
broken (patched build, overrides), with seeds ``seed``, ``seed + 1``, and so
on. A run detects the bug when it FAILs, every component in ``fail`` FAILs,
and every evidence ID in ``missing`` is reported as missing required evidence.
A case holds when every control passes and every scenario's detection rate
reaches ``min_detection_rate``.

The zoo runs through the batch runner (``valkey-fuzzer batch --zoo BUILDS``,
:mod:`valkey_fuzzer.batch`): it takes :func:`zoo_runs` (the runs, each with
its complete scenario), records what each run's outcome shows (:func:`observe`),
and judges each case with :func:`case_report`. A case that does not hold fails
the batch.
"""

import os
import re
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from ._data import merge, with_valkey_binary
from .schema import ConfigError, ConfigField, ConfigSchema, load_yaml_mapping

ZOO_DIRECTORY = Path(__file__).resolve().parent.parent / "zoo"
COMMIT_FILE = "valkey-commit"
MIN_RUNS = 5

_IDS = ConfigField(list, items=ConfigField(str), default=list)
_CASE = ConfigSchema(
    {
        "description": ConfigField(str),
        "catches": ConfigField(str),
        "scenarios": ConfigField(
            list,
            items=ConfigField(dict, schema=ConfigSchema({"file": ConfigField(str), "fail": _IDS, "missing": _IDS})),
        ),
        "overrides": ConfigField(dict, default=dict, allow_none=True),
        "runs": ConfigField(int, minimum=MIN_RUNS),
        "min_detection_rate": ConfigField(float, maximum=1),
        "requires": ConfigField(list, items=ConfigField(str, choices=("linux", "root")), default=list, allow_none=True),
    }
)
_CASE_ID = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")
_VERSION_SHA = re.compile(r"\bsha=([0-9a-f]+):")


class ZooError(ValueError):
    """A zoo directory, case, or build that cannot be used."""


@dataclass(frozen=True)
class ZooScenario:
    file: str
    scenario: Mapping[str, Any]
    fail: tuple[str, ...]
    missing: tuple[str, ...]


@dataclass(frozen=True)
class ZooCase:
    case_id: str
    directory: Path
    description: str
    catches: str
    scenarios: tuple[ZooScenario, ...]
    overrides: Mapping[str, Any]
    runs: int
    min_detection_rate: float
    requires: tuple[str, ...]

    @property
    def patched(self) -> bool:
        return (self.directory / "patch.diff").is_file()

    def unmet_requirements(self) -> list[str]:
        unmet = []
        if "linux" in self.requires and not sys.platform.startswith("linux"):
            unmet.append("linux")
        if "root" in self.requires and os.geteuid() != 0:
            unmet.append("root")
        return unmet


@dataclass(frozen=True)
class ZooRun:
    """One run of a case: the clean control (must PASS) or a broken run (must FAIL as expected)."""

    case_id: str
    file: str
    control: bool
    scenario: Mapping[str, Any]
    fail: tuple[str, ...] = ()
    missing: tuple[str, ...] = ()


def load_case(directory: Path) -> ZooCase:
    """Load and check one case directory."""

    case_id = directory.name
    where = f"zoo case {case_id}"
    if not _CASE_ID.match(case_id):
        raise ZooError(f"{where}: the directory name must be lowercase words joined by hyphens")
    document = load_yaml_mapping((directory / "case.yaml").read_text(encoding="utf-8"), source_name=str(directory))
    try:
        case = _CASE.resolve(document, "case")
    except ConfigError as exc:
        raise ZooError(f"{where}: {exc}") from None
    for key in ("description", "catches"):
        if not case[key].strip():
            raise ZooError(f"{where}: {key} must say what is broken and what catches it")
    if case["min_detection_rate"] <= 0:
        raise ZooError(f"{where}: min_detection_rate must be greater than 0")
    overrides = case["overrides"] or {}
    if not overrides and not (directory / "patch.diff").is_file():
        raise ZooError(f"{where}: needs a patch.diff or overrides; otherwise nothing is broken")
    if not case["scenarios"]:
        raise ZooError(f"{where}: scenarios must be a non-empty list")
    scenarios = []
    for entry in case["scenarios"]:
        if not entry["fail"] and not entry["missing"]:
            raise ZooError(
                f"{where}: {entry['file']}: name the components that must FAIL or the evidence that must be missing"
            )
        path = directory / entry["file"]
        if not path.is_file():
            raise ZooError(f"{where}: scenario file {entry['file']} not found")
        scenario = load_yaml_mapping(path.read_text(encoding="utf-8"), source_name=str(path))
        if "valkey_binary" in (scenario.get("deployment") or {}):
            raise ZooError(f"{where}: {entry['file']} must not set deployment.valkey_binary; the zoo picks the build")
        scenarios.append(ZooScenario(entry["file"], scenario, tuple(entry["fail"]), tuple(entry["missing"])))
    return ZooCase(
        case_id=case_id,
        directory=directory,
        description=case["description"],
        catches=case["catches"],
        scenarios=tuple(scenarios),
        overrides=overrides,
        runs=case["runs"],
        min_detection_rate=float(case["min_detection_rate"]),
        requires=tuple(case["requires"] or ()),
    )


def load_zoo(root: Path = ZOO_DIRECTORY, case_ids: Sequence[str] = ()) -> tuple[str, list[ZooCase]]:
    """The pinned Valkey commit and the cases under ``root`` (only ``case_ids`` when given), sorted by ID."""

    commit = (root / COMMIT_FILE).read_text(encoding="utf-8").strip()
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ZooError(f"{root / COMMIT_FILE} must hold one full Valkey commit SHA")
    found = sorted(path.parent.name for path in root.glob("*/case.yaml"))
    missing = sorted(set(case_ids) - set(found))
    if missing:
        raise ZooError(f"no such zoo case: {', '.join(missing)}")
    return commit, [load_case(root / case_id) for case_id in found if not case_ids or case_id in case_ids]


def binary(builds: Path, case: Optional[ZooCase] = None) -> str:
    """The build a case's broken runs use (``case``), or the clean build (no ``case``)."""

    name = case.case_id if case is not None and case.patched else "clean"
    path = builds / name / "src" / "valkey-server"
    if not os.access(path, os.X_OK):
        raise ZooError(f"{path} is missing; build it with zoo/build.sh {builds}")
    return str(path)


def check_build(path: str, commit: str) -> None:
    """Refuse a build whose ``valkey-server --version`` names another commit than the pinned one."""

    version = subprocess.run([path, "--version"], capture_output=True, text=True, timeout=30).stdout
    match = _VERSION_SHA.search(version)
    if not match or not commit.startswith(match.group(1)):
        raise ZooError(f"{path} is not built from the pinned commit {commit[:12]}: {version.strip()!r}")


def zoo_runs(case: ZooCase, builds: Path) -> list[ZooRun]:
    """Every run of ``case``: one clean control per scenario, then ``case.runs`` broken runs of each."""

    clean, broken = binary(builds), binary(builds, case)
    runs = []
    for entry in case.scenarios:
        control = with_valkey_binary(entry.scenario, clean)
        runs.append(ZooRun(case.case_id, entry.file, True, control))
        for index in range(case.runs):
            scenario = with_valkey_binary(merge(entry.scenario, case.overrides), broken)
            scenario["seed"] = int(entry.scenario["seed"]) + index
            scenario["scenario_id"] = f"zoo-{case.case_id}-{index}"
            runs.append(ZooRun(case.case_id, entry.file, False, scenario, entry.fail, entry.missing))
    return runs


def observe(outcome: Optional[Mapping[str, Any]], status: str) -> dict[str, Any]:
    """What a run shows the zoo: its ``status``, the components that FAILed, and the required evidence missing.

    ``outcome`` is the run report's ``outcome`` (None for a run that left no report).
    """

    outcome = outcome or {}
    results = outcome.get("component_results") or ()
    diagnostics = outcome.get("diagnostics") or {}
    return {
        "status": status,
        "failed": sorted(item["component_id"] for item in results if item.get("status") == "FAIL"),
        "missing": sorted({item["evidence_id"] for item in diagnostics.get("missing_required_evidence") or ()}),
    }


def detected(run: ZooRun, observed: Mapping[str, Any]) -> bool:
    """A broken run detects its bug when it FAILs through every expected component and missing evidence."""

    return (
        observed["status"] == "FAIL"
        and set(run.fail) <= set(observed["failed"])
        and set(run.missing) <= set(observed["missing"])
    )


def case_report(case: ZooCase, results: Sequence[tuple[ZooRun, Mapping[str, Any], Optional[str]]]) -> dict[str, Any]:
    """Summarize a case's ``(run, observed, report path)`` results: controls, per-scenario detection, verdict."""

    controls, scenarios = [], {}
    for run, observed, report in results:
        row = {"file": run.file, "seed": run.scenario["seed"], "status": observed["status"], "report": report}
        if run.control:
            controls.append(row)
            continue
        row.update(failed=observed["failed"], missing=observed["missing"], detected=detected(run, observed))
        expected = {"fail": list(run.fail), "missing": list(run.missing)}
        scenarios.setdefault(run.file, {"expected": expected, "runs": []})["runs"].append(row)
    for summary in scenarios.values():
        summary["detection_rate"] = sum(row["detected"] for row in summary["runs"]) / len(summary["runs"])
    holds = (
        len(controls) == len(case.scenarios)
        and all(row["status"] == "PASS" for row in controls)
        and len(scenarios) == len(case.scenarios)
        and all(summary["detection_rate"] >= case.min_detection_rate for summary in scenarios.values())
    )
    return {
        "case": case.case_id,
        "catches": case.catches,
        "min_detection_rate": case.min_detection_rate,
        "controls": controls,
        "scenarios": scenarios,
        "holds": holds,
    }
