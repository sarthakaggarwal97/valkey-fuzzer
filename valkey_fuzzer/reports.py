"""Run reports and report folders: the files a run leaves, writing and reading them, and the report it gets.

Every executed plan, whichever command runs it, leaves one report folder::

    <folder>/plan.json                      the execution plan, written before the run starts
    <folder>/report.json                    the run report (see :func:`run_report`)
    <folder>/components/<component id>/    files the run's components wrote

``run`` puts it at ``<root>/runs/<name>`` and ``replay`` at
``<root>/replays/<name>``. A minimization gets a session
folder ``<root>/minimize/<name>`` holding
one report folder per candidate (``attempt-<n>``); a batch puts each job's at ``runs/<job id>``
of its batch folder. The root is the artifact directory, or ``<tmp>/valkey-fuzzer``. Every ``<name>`` is
``<UTC time to the millisecond>-<scenario or document ID>-<random suffix>``
(:func:`~valkey_fuzzer.hoststate.new_run_folder`, which also names a run executed
without a report folder), created exclusively, so names sort by start time and
never collide, even between processes.
"""

import hashlib
import json
import os
import shlex
import tempfile
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Optional

import yaml

from ._data import to_plain_data
from .catalog import Catalog
from .coverage import coverage_tuple
from .hoststate import new_run_folder
from .plan import ExecutionPlan
from .provenance import recorded_binary
from .provenance import reproducer as provenance_reproducer
from .results import ComponentStatus, RunOutcome
from .schema import load_yaml, plain_config

# Every run report has these keys (see :func:`run_report`), and old reports without the readable ones still load.
REPORT_KEYS = ("generated_at", "status", "summary", "scenario", "outcome", "coverage", "reproducer", "provenance")
# What a report's reproducer records; replay compares each against this host.
REPRODUCER_KEYS = (
    "python",
    "platform",
    "experiment",
    "fuzzer_digest",
    "components",
    "deployment_provider",
    "plan_digest",
    "valkey_sha",
)

# Longest value copied into ``failed_components``; a longer one is cut and carries its length and digest.
MAX_VALUE_CHARS = 120
# Most evidence items listed for one failed component.
MAX_EVIDENCE_LISTED = 5

PLAN_FILE = "plan.json"
REPORT_FILE = "report.json"
# The provenance of a run, written beside its plan.json before the run starts, so a plan.json can be verified.
REPRODUCER_FILE = "reproducer.json"
# A generated scenario, written next to its report so it runs again without the generator.
SCENARIO_FILE = "scenario.yaml"


class RunServiceError(RuntimeError):
    """The service cannot do what was asked; the command-line interface reports a harness error."""


class InputError(RunServiceError):
    """A scenario, report, plan, or document file is missing, unreadable, or of the wrong kind."""


class ReportWriteError(RunServiceError):
    """A report, output, or exported scenario file cannot be written."""


# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------


def _write_text(path: str, text: str, what: str) -> None:
    """Write ``text`` to ``path`` atomically: to a temporary file beside it, renamed over it once synced."""

    output = Path(path)
    temporary_path = None
    try:
        output.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=output.parent,
            prefix=f".{output.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            temporary.write(text)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_path, output)
    except BaseException as exc:
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass
        if isinstance(exc, OSError):
            raise ReportWriteError(f"could not write {what} to {path}: {exc}") from exc
        raise


def write_document(path: str, data: Any, output_format: str = "json") -> None:
    """Atomically write ``data`` to ``path`` as JSON or YAML; ``data`` must be plain data (a value JSON has no
    form for, a set or a path, is a bug in what built it, not written as its ``str()``)."""

    if output_format not in {"json", "yaml"}:
        raise ValueError(f"unsupported output format: {output_format}")
    try:
        if output_format == "json":
            text = json.dumps(data, indent=2, allow_nan=False) + "\n"
        else:
            text = yaml.safe_dump(data, default_flow_style=False, sort_keys=False)
    except (TypeError, ValueError, yaml.YAMLError) as exc:
        raise ReportWriteError(f"could not write the report to {path}: {exc}") from exc
    _write_text(path, text, "the report")


def read_scenario(path: str) -> str:
    source = Path(path)
    if not source.is_file():
        raise InputError(f"scenario file not found: {path}")
    try:
        return source.read_text(encoding="utf-8")
    except OSError as exc:
        raise InputError(f"unable to read scenario file {path}: {exc}") from exc


def load_document(path: str) -> Any:
    source = Path(path)
    if not source.is_file():
        raise InputError(f"file not found: {path}")
    try:
        text = source.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise InputError(f"unable to read {path}: {exc}") from exc
    if source.suffix != ".json":
        return load_yaml(text, path)
    # Not as YAML: a YAML 1.1 loader reads a JSON float such as 5e-05 as a string.
    try:
        return json.loads(text, object_pairs_hook=_unique_keys)
    except ValueError as exc:
        raise InputError(f"{path} is not valid JSON: {exc}") from exc


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    document: dict[str, Any] = {}
    for key, value in pairs:
        if key in document:
            raise ValueError(f"duplicate key {key!r}")
        document[key] = value
    return document


def make_report_folder(path: str) -> str:
    """Create ``path``, which must not exist yet."""

    try:
        os.makedirs(path)
    except OSError as exc:
        raise ReportWriteError(f"could not create the report folder {path}: {exc}") from exc
    return path


def new_report_folder(parent: str, name: str) -> str:
    """Create and return ``<parent>/<UTC time>-<name>-<suffix>`` (:func:`~valkey_fuzzer.hoststate.new_run_folder`)."""

    try:
        return new_run_folder(parent, name)
    except OSError as exc:
        raise ReportWriteError(f"could not create a report folder under {parent}: {exc}") from exc


def write_plan(report_dir: str, plan: ExecutionPlan, reproducer: Optional[Mapping[str, Any]] = None) -> None:
    """Write ``plan.json`` (and ``reproducer.json``, given the run's ``reproducer``), before the run starts."""

    write_document(os.path.join(report_dir, PLAN_FILE), plan.to_data())
    if reproducer is not None:
        write_document(os.path.join(report_dir, REPRODUCER_FILE), reproducer)


def export_scenario(scenario: Mapping[str, Any], path: str, *, generated: Optional[Mapping[str, Any]] = None) -> None:
    """Write a scenario so that ``run`` runs it again; a generated one's header says how ``generated`` drew it."""

    target = Path(path)
    header = f"# Reproduce with: valkey-fuzzer run {target.name}\n"
    if generated is not None:
        header = (
            f"# valkey-fuzzer generated scenario: {generated['experiment']}, seed {generated['seed']} "
            f"(attempt {generated['attempt']}).\n{header}"
        )
    _write_text(path, header + yaml.safe_dump(dict(scenario), sort_keys=False), "the scenario")


# ---------------------------------------------------------------------------
# Run reports
# ---------------------------------------------------------------------------


def replay_command(report_path: str) -> str:
    """The shell command that replays the run whose report is at ``report_path``."""

    return shlex.join(["valkey-fuzzer", "replay", report_path])


def shorten(value: Any, limit: int = MAX_VALUE_CHARS) -> str:
    """``value`` as text of at most ``limit`` characters; a longer one is cut and ends with its length and SHA-256,
    so two cut values can be told apart and the full one found in the report's ``outcome``."""

    text = value if isinstance(value, str) else json.dumps(value, sort_keys=True, default=str)
    if len(text) <= limit:
        return text
    digest = hashlib.sha256(text.encode("utf-8", "replace")).hexdigest()[:12]
    return f"{text[:limit]}... [{len(text)} chars, sha256 {digest}]"


def failed_components(outcome: RunOutcome) -> list[dict[str, Any]]:
    """The components that did not pass, each with its summary and its first evidence values, cut short.

    Only a reading aid: the full results stay in ``outcome``.
    """

    uses = {component.id: (component.role.value, component.use) for component in outcome.plan.components}
    rows = []
    for result in outcome.component_results:
        if result.status not in (ComponentStatus.FAIL, ComponentStatus.ERROR):
            continue
        role, use = uses.get(result.component_id, (None, None))
        rows.append(
            {
                "component_id": result.component_id,
                "role": role,
                "use": use,
                "status": result.status.value,
                "crash": result.crash,
                "summary": shorten(result.summary),
                "evidence_total": len(result.evidence),
                "evidence": [
                    {"evidence_id": item.evidence_id, "value": shorten(to_plain_data(item.value))}
                    for item in result.evidence[:MAX_EVIDENCE_LISTED]
                ],
            }
        )
    return rows


def run_report(
    catalog: Catalog,
    outcome: RunOutcome,
    *,
    scenario: Optional[Mapping[str, Any]] = None,
    provenance: Optional[Mapping[str, Any]] = None,
    generated: Optional[Mapping[str, Any]] = None,
    report_path: Optional[str] = None,
) -> dict[str, Any]:
    """One self-contained, replayable report for a single run.

    ``outcome`` holds the plan (``outcome.plan``) with the component
    results, evidence, and diagnostics; ``coverage`` is the run's coverage
    tuple; ``reproducer`` is the environment it ran in, with
    ``generated`` (seed, experiment, attempt, exclusions, boundary)
    for a generated scenario; ``provenance`` is how a replay
    compared with the run it replayed (None for other runs).

    The keys come in reading order: ``status``, ``summary``, ``failed_components`` (what did not pass, values cut to
    :data:`MAX_VALUE_CHARS`), ``reproduce`` (the command that replays the run, given ``report_path``), ``look_at``
    (where the files are), then the data a tool reads. ``reproducer`` also records the ``command`` and the
    ``valkey_binary``.
    """

    reproducer = provenance_reproducer(catalog, outcome.plan, outcome)
    command = replay_command(report_path) if report_path else None
    reproducer["command"] = command
    reproducer["valkey_binary"] = recorded_binary(catalog, outcome, scenario)
    provider = catalog.deployment_providers.of_plan(outcome.plan)
    host_fields = () if provider is None else provider.host_fields
    if generated is not None:
        reproducer["generated"] = to_plain_data(generated)
    folder = os.path.dirname(report_path) if report_path else None
    return {
        "status": outcome.status.value,
        "summary": outcome.summary,
        "failed_components": failed_components(outcome),
        "reproduce": command,
        "look_at": {
            "report_folder": folder,
            "plan": os.path.join(folder, PLAN_FILE) if folder else None,
            "component_files": os.path.join(folder, "components") if folder else None,
            "full_results": "outcome.component_results",
        },
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "scenario": plain_config(scenario) if scenario is not None else None,
        "outcome": outcome.to_data(),
        "coverage": to_plain_data(coverage_tuple(outcome, host_fields)),
        "reproducer": reproducer,
        "provenance": to_plain_data(provenance) if provenance is not None else None,
    }


def _missing(data: Mapping[str, Any], keys: Sequence[str]) -> list[str]:
    return [key for key in keys if key not in data]


def as_run_report(data: Any, path: str) -> Optional[Mapping[str, Any]]:
    """The run report ``data`` is, or None for anything else.

    A document with ``outcome`` or ``reproducer`` is a run report and must have
    all its keys. Anything else is left to the caller, which reads it as a bare
    plan.
    """

    if not isinstance(data, Mapping) or ("outcome" not in data and "reproducer" not in data):
        return None
    missing = _missing(data, REPORT_KEYS)
    for key, required in (("reproducer", REPRODUCER_KEYS), ("outcome", ("plan",))):
        if missing:
            break
        if not isinstance(data[key], Mapping):
            missing = [key]
        else:
            missing = [f"{key}.{name}" for name in _missing(data[key], required)]
    if missing:
        raise InputError(f"{path} is not a complete run report: it has no {', '.join(missing)}")
    return data


def _beside_plan(path: str, plan: ExecutionPlan) -> tuple[Optional[dict[str, Any]], Optional[dict[str, Any]]]:
    """The scenario and recorded run a bare ``plan.json``'s folder holds: its ``report.json``, else its
    ``reproducer.json`` (a run that did not finish); (None, None) when the folder holds neither."""

    folder = os.path.dirname(os.path.abspath(path))
    if os.path.basename(path) != PLAN_FILE:
        return None, None
    report_path, reproducer_path = os.path.join(folder, REPORT_FILE), os.path.join(folder, REPRODUCER_FILE)
    if os.path.isfile(report_path):
        report = as_run_report(load_document(report_path), report_path)
        if report is not None:
            return report["scenario"], {"reproducer": report["reproducer"], "outcome": report["outcome"]}
    if os.path.isfile(reproducer_path):
        reproducer = load_document(reproducer_path)
        missing = _missing(reproducer, REPRODUCER_KEYS) if isinstance(reproducer, Mapping) else list(REPRODUCER_KEYS)
        if missing:
            raise InputError(f"{reproducer_path} is not a complete reproducer: it has no {', '.join(missing)}")
        return None, {"reproducer": reproducer, "outcome": {"plan": plan.to_data()}}
    return None, None


def recorded_run(path: str) -> tuple[ExecutionPlan, Optional[dict[str, Any]], Optional[dict[str, Any]]]:
    """The plan to replay, its scenario, and the recorded reproducer and outcome (None for a bare plan)."""

    data = load_document(path)
    report = as_run_report(data, path)
    if report is None:
        # A scenario names its experiment under ``experiment``; a plan, under ``experiment_id``.
        if isinstance(data, Mapping) and "experiment" in data and "experiment_id" not in data:
            raise InputError(
                f"{path} is a scenario, not a run report or plan; run it with `valkey-fuzzer run {shlex.quote(path)}`"
            )
        plan = ExecutionPlan.from_data(data)
        return plan, *_beside_plan(path, plan)
    outcome = report["outcome"]
    recorded = {"reproducer": report["reproducer"], "outcome": outcome}
    return ExecutionPlan.from_data(outcome["plan"]), report["scenario"], recorded
