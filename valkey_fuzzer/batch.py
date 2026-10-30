"""The batch runner: the one way to run many scenarios.

A batch expands its **sources** into jobs, one run each:

- ``random``: seeds ``first_seed .. first_seed + seeds - 1`` for each listed
  experiment (or ``all`` that declare ``metadata.generator``), drawn under the
  batch's search profile;
- ``matrix``: a base scenario over every combination of dotted-path values;
- ``scenarios``: scenario files, or directories of them, run as written;
- ``zoo``: the bug zoo's cases, each a clean control and broken runs on its builds;
- ``failures``: the failed runs of an earlier batch, a sample per failure signature.

Every job runs in its own worker process (``python -m valkey_fuzzer.batch
TASK``) on one of ``workers`` slots. Slot ``k`` owns the client ports
``port_base + k * PORT_BLOCK`` up to the next block (and their cluster bus
ports), the per-instance loopback addresses ``127.0.<k+1>.0/24``, and its own
data directory, ``TMPDIR``, and provider directories, which are wiped after
each job, so no two concurrent runs share ports, addresses, logs, or
directories (a partition matches addresses, so a shared address would let one
worker's partition cut another's instances). After each run the host :func:`~valkey_fuzzer.hoststate.sweep`
removes whatever that worker left behind (servers, chains, mounts, data
directories); a leftover is a finding that makes the job an ERROR.

The batch folder::

    batch.json          options, sources, and the zoo cases, as resolved (written once)
    jobs.json           every job, in order (written once)
    workers.json        the jobs running now and their worker processes (for resume)
    results/<id>.json   one finished job: status, failure signature, leaks, zoo observation
    runs/<id>/          the job's run folder: plan.json, report.json, scenario.yaml,
                        components/, logs/ (instance logs), console.log
    summary.json        counts, failure signatures, serial re-runs, zoo verdicts, leaks, exit code
    batch.log           one line per finished job

A batch resumes where it stopped: finished jobs keep their results, a job that
was running is swept and run again, and a worker that outlived a killed batch
is stopped first. Non-PASS runs are grouped by the minimizer's failure
signature (:func:`~valkey_fuzzer.minimize.failure_signature`); with
``serial_reruns`` a sample of each group runs again one at a time, which
separates failures that need load from real ones. The batch's status is the
worst of its runs, its leaks (ERROR), and its zoo cases (FAIL when one does not
hold).
"""

import copy
import fcntl
import hashlib
import itertools
import json
import os
import random
import shutil
import signal
import subprocess
import sys
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

import yaml

from ._data import (
    SCENARIO_SECTIONS,
    canonical_json,
    error_record,
    exception_type,
    merge,
    set_path,
    with_valkey_binary,
)
from .contracts import ExecutionPlan, RunOutcome
from .deployment.local.config import CLUSTER_BUS_PORT_OFFSET, MAX_TCP_PORT
from .deployment.local.ports import ephemeral_overlap
from .generator import Exclusion, GeneratorError, find_profile, opted_in
from .hoststate import Owner, owner_of, sweep
from .minimize import empty_signature, failure_signature
from .runner import interrupt_on_sigterm
from .schema import ConfigError, ConfigField, ConfigSchema, load_yaml_mapping
from .service import REPORT_FILE, SCENARIO_FILE, PreparedRun, RunService, write_document
from .verdict import EXIT_HARNESS_ERROR, EXIT_INTERRUPTED, exit_code, print_outcome
from .zoo import ZooRun, case_report, check_build, detected, load_case, load_zoo, observe, zoo_runs

# Client ports per worker: a cluster of 16 primaries with 5 replicas each is 96 instances.
PORT_BLOCK = 120
DEFAULT_PORT_BASE = 11000
DEFAULT_TIMEOUT_S = 1200.0
MAX_JOBS = 100_000
# A run's own deadline is cooperative; the batch stops its worker this much later
# (or a quarter of the deadline later, if that is longer) with SIGINT, then
# SIGTERM, then SIGKILL, STOP_GRACE_S apart. Stopping a batch skips the wait.
DEADLINE_GRACE_S = 60.0
STOP_GRACE_S = (60.0, 30.0)
POLL_S = 0.2

STATUS_ORDER = ("FAIL", "ERROR", "SKIP_UNSUPPORTED", "PASS")

WORKER_COMMAND = (sys.executable, "-m", "valkey_fuzzer.batch")
ERROR_FILE = "error.json"
CONSOLE_FILE = "console.log"
# What a pruned PASS run keeps.
KEPT_WHEN_PRUNED = (REPORT_FILE, SCENARIO_FILE)


class BatchError(ValueError):
    """A batch spec, source, or folder that cannot be used."""


def worst(statuses: Sequence[str]) -> str:
    """The worst of ``statuses`` (FAIL, then ERROR, then SKIP_UNSUPPORTED); PASS for none."""

    return next((status for status in STATUS_ORDER if status in statuses), "PASS")


def _read_json(path: str) -> Any:
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


# ---------------------------------------------------------------------------
# Options and sources
# ---------------------------------------------------------------------------


# The options a batch spec may set, also checked on BatchOptions; the dataclass holds their defaults.
_OPTION_FIELDS = {
    "batch_id": ConfigField(str, required=False),
    "profile": ConfigField(str, required=False, allow_none=True),
    "workers": ConfigField(int, minimum=1, required=False),
    "timeout_s": ConfigField(float, required=False, allow_none=True),
    "port_base": ConfigField(int, minimum=1024, maximum=MAX_TCP_PORT, required=False),
    "valkey_binary": ConfigField(str, required=False, allow_none=True),
    "prune_pass": ConfigField(bool, required=False),
    "serial_reruns": ConfigField(int, minimum=0, required=False),
}
_SPEC = ConfigSchema({**_OPTION_FIELDS, "sources": ConfigField(list, required=False, allow_none=True)})

_PATHS = ConfigField(list, items=ConfigField(str))
_SOURCES = {
    "random": ConfigSchema(
        {
            "experiments": ConfigField((str, list)),
            "seeds": ConfigField(int, minimum=1, required=False, allow_none=True),
            "first_seed": ConfigField(int, minimum=0, required=False, allow_none=True),
            "excludes": ConfigField(list, items=ConfigField(str), default=list, allow_none=True),
            "base": ConfigField(dict, default=dict, allow_none=True),
        }
    ),
    "matrix": ConfigSchema(
        {
            "base": ConfigField(dict),
            "dimensions": ConfigField(dict, default=dict, schema=ConfigSchema(additional=ConfigField(list))),
            "seeds_per_case": ConfigField(int, minimum=1, default=1),
            "seed": ConfigField(int, minimum=0, default=0),
        }
    ),
    "scenarios": ConfigSchema({"paths": _PATHS}),
    "zoo": ConfigSchema(
        {
            "builds": ConfigField(str),
            "cases": ConfigField(list, items=ConfigField(str), default=list, allow_none=True),
            "directory": ConfigField(str, default=None, allow_none=True),
        }
    ),
    "failures": ConfigSchema(
        {
            "batch": ConfigField(str),
            "statuses": ConfigField(
                list, items=ConfigField(str, choices=STATUS_ORDER[:-1]), default=lambda: ["FAIL", "ERROR"]
            ),
            "per_signature": ConfigField(int, minimum=1, default=None, allow_none=True),
        }
    ),
}
# The one-value forms a source may take: ``zoo: BUILDS``, ``failures: BATCH``, ``scenarios: PATH`` or a list.
_SHORTHANDS = {"zoo": "builds", "failures": "batch", "scenarios": "paths"}


@dataclass
class BatchOptions:
    """How a batch runs; ``timeout_s`` None means the profile's budget, or :data:`DEFAULT_TIMEOUT_S`."""

    batch_id: str = "batch"
    profile: Optional[str] = None
    workers: int = 1
    timeout_s: Optional[float] = None
    port_base: int = DEFAULT_PORT_BASE
    valkey_binary: Optional[str] = None
    prune_pass: bool = False
    serial_reruns: int = 0
    experiment_directories: tuple[str, ...] = ()
    discover_plugins: bool = True

    def check(self) -> None:
        try:
            _SPEC.resolve({name: getattr(self, name) for name in _OPTION_FIELDS}, "batch")
        except ConfigError as exc:
            raise BatchError(str(exc)) from None
        if not self.batch_id.strip():
            raise BatchError("batch.batch_id must name the batch")
        if self.timeout_s is not None and self.timeout_s <= 0:
            raise BatchError("batch.timeout_s must be a number of seconds greater than zero")
        # Bus ports sit CLUSTER_BUS_PORT_OFFSET above the client ports: neither may reach another worker's block.
        span = self.workers * PORT_BLOCK
        if span > CLUSTER_BUS_PORT_OFFSET or self.port_base + span - 1 + CLUSTER_BUS_PORT_OFFSET > MAX_TCP_PORT:
            raise BatchError(
                f"{self.workers} workers need ports {self.port_base}..{self.port_base + span - 1} and their "
                f"cluster bus ports {CLUSTER_BUS_PORT_OFFSET} higher: use fewer workers or a lower port_base"
            )
        overlap = ephemeral_overlap(self.port_base, span, cluster=True, field="port_base")
        if overlap:
            raise BatchError(f"the workers' {overlap}, or use fewer workers")

    def ports(self, slot: int) -> tuple[int, int]:
        """The first and last client port of worker ``slot``."""

        low = self.port_base + slot * PORT_BLOCK
        return low, low + PORT_BLOCK - 1

    @staticmethod
    def loopback_block(slot: int) -> int:
        """The deployment ``loopback_block`` of worker ``slot``: block 0 stays for runs outside a batch."""

        return slot + 1


def _absolute(path: str, base_dir: str, where: str) -> str:
    if not path:
        raise BatchError(f"{where} must be a path")
    return os.path.abspath(os.path.join(base_dir, os.path.expanduser(path)))


def normalize_source(source: Any, base_dir: str) -> dict[str, Any]:
    """One source, checked, with its defaults and its paths absolute (relative ones are read from ``base_dir``)."""

    if not isinstance(source, Mapping) or len(source) != 1 or next(iter(source)) not in _SOURCES:
        raise BatchError(f"each source is a mapping with one key out of {list(_SOURCES)}, got {source!r}")
    kind, value = next(iter(source.items()))
    where = f"source {kind}"
    if kind in _SHORTHANDS and (isinstance(value, str) or (kind == "scenarios" and isinstance(value, list))):
        value = {_SHORTHANDS[kind]: [value] if kind == "scenarios" and isinstance(value, str) else value}
    try:
        settings = _SOURCES[kind].resolve(value, where)
    except ConfigError as exc:
        raise BatchError(str(exc)) from None
    if kind == "scenarios":
        if not settings["paths"]:
            raise BatchError(f"{where} must list scenario files or directories")
        return {"scenarios": [_absolute(path, base_dir, where) for path in settings["paths"]]}
    if kind == "failures":
        settings["batch"] = _absolute(settings["batch"], base_dir, f"{where}.batch")
    elif kind == "zoo":
        settings["builds"] = _absolute(settings["builds"], base_dir, f"{where}.builds")
        settings["cases"] = settings["cases"] or []
        if settings["directory"] is not None:
            settings["directory"] = _absolute(settings["directory"], base_dir, f"{where}.directory")
    elif kind == "random":
        experiments = settings["experiments"]
        if experiments != "all" and (
            not isinstance(experiments, list) or not experiments or not all(isinstance(e, str) for e in experiments)
        ):
            raise BatchError(f"{where}.experiments must be 'all' or a list of experiment IDs")
        try:
            settings["excludes"] = [str(Exclusion.parse(item)) for item in settings["excludes"] or []]
        except GeneratorError as exc:
            raise BatchError(f"{where}.excludes: {exc}") from None
        settings["base"] = settings["base"] or {}
        # A fresh first seed unless one is given; jobs.json records every seed.
        if settings.get("first_seed") is None:
            settings["first_seed"] = random.randrange(0, 2**31)
        settings.setdefault("seeds", None)
    else:
        if "experiment" not in settings["base"] or "deployment" not in settings["base"]:
            raise BatchError(f"{where}.base must be a scenario with experiment and deployment")
        for path, values in settings["dimensions"].items():
            if path.split(".")[0] not in SCENARIO_SECTIONS:
                raise BatchError(f"{where}.dimensions path {path!r} must start with one of {list(SCENARIO_SECTIONS)}")
            if not values:
                raise BatchError(f"{where}.dimensions.{path} needs a non-empty list of values")
    return {kind: settings}


def load_spec(path: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """A batch spec file's options (``batch_id``, ``profile``, ...) and its normalized sources."""

    document = load_yaml_mapping(Path(path).read_text(encoding="utf-8"), source_name=path)
    try:
        document = _SPEC.resolve(document, "batch spec")
    except ConfigError as exc:
        raise BatchError(str(exc)) from None
    base_dir = os.path.dirname(os.path.abspath(path))
    sources = document.pop("sources", None) or []
    if document.get("valkey_binary"):
        document["valkey_binary"] = _absolute(document["valkey_binary"], base_dir, "valkey_binary")
    return document, [normalize_source(source, base_dir) for source in sources]


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------


@dataclass
class Expansion:
    """What a batch's sources expand to: the jobs, matrix cases that do not compile, and the zoo cases."""

    jobs: list[dict[str, Any]] = field(default_factory=list)
    invalid: list[dict[str, Any]] = field(default_factory=list)
    zoo: Optional[dict[str, Any]] = None
    # The profile's run deadline, for a batch that sets none.
    profile_timeout_s: Optional[float] = None
    # RunService.catalog_digest when the batch has random jobs, which draw their scenarios when they run.
    catalog_digest: Optional[str] = None


def _unique(jobs: list[dict[str, Any]], job: dict[str, Any]) -> None:
    taken = {item["id"] for item in jobs}
    base, index = job["id"], 2
    while job["id"] in taken:
        job["id"] = f"{base}-{index}"
        index += 1
    jobs.append(job)


def _scenario_files(path: str) -> list[str]:
    if os.path.isdir(path):
        return sorted(str(item) for item in Path(path).iterdir() if item.suffix in (".yaml", ".yml"))
    if not os.path.isfile(path):
        raise BatchError(f"scenario file or directory not found: {path}")
    return [path]


def _matrix_jobs(source: Mapping[str, Any], index: int, service: RunService, expansion: Expansion) -> None:
    base, dimensions = source["base"], list(source["dimensions"].items())
    name = base.get("scenario_id") or base["experiment"].get("id") or f"matrix{index}"
    rng = random.Random(source["seed"])
    combos = itertools.product(*[list(values) for _, values in dimensions]) if dimensions else [()]
    for combo_index, combo in enumerate(combos):
        assignment = {path: value for (path, _), value in zip(dimensions, combo)}
        for replica in range(source["seeds_per_case"]):
            scenario = copy.deepcopy(base)
            for path, value in assignment.items():
                set_path(scenario, path, copy.deepcopy(value))
            scenario["seed"] = rng.randrange(1, 2**31)
            scenario["scenario_id"] = f"{name}-{combo_index}-{replica}"
            try:
                service.compile(scenario)
            except Exception as exc:
                # A combination the experiment does not support is reported, not run.
                expansion.invalid.append(
                    {
                        "case": scenario["scenario_id"],
                        "assignment": assignment,
                        "error": f"{exception_type(exc)}: {exc}",
                    }
                )
                continue
            label = json.dumps(assignment, sort_keys=True, default=str)
            _unique(
                expansion.jobs,
                {"id": f"matrix-{scenario['scenario_id']}", "source": "matrix", "label": label, "scenario": scenario},
            )


def _zoo_jobs(source: Mapping[str, Any], expansion: Expansion) -> None:
    from .zoo import ZOO_DIRECTORY

    directory = Path(source["directory"] or ZOO_DIRECTORY)
    builds = Path(source["builds"])
    commit, cases = load_zoo(directory, source["cases"])
    zoo = expansion.zoo or {"valkey_commit": commit, "cases": [], "skipped": []}
    if zoo["valkey_commit"] != commit:
        raise BatchError("every zoo source of a batch must pin the same Valkey commit")
    expansion.zoo = zoo
    checked: set[str] = set()
    for case in cases:
        unmet = case.unmet_requirements()
        if unmet:
            zoo["skipped"].append({"case": case.case_id, "reason": f"needs {', '.join(unmet)}"})
            continue
        runs = zoo_runs(case, builds)
        for path in sorted({run.scenario["deployment"]["valkey_binary"] for run in runs} - checked):
            check_build(path, commit)
            checked.add(path)
        zoo["cases"].append({"case": case.case_id, "directory": str(case.directory)})
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
                    "missing": list(run.missing),
                },
            }
            _unique(expansion.jobs, job)


def expand(sources: Sequence[Mapping[str, Any]], service: RunService, options: BatchOptions) -> Expansion:
    """Every job of ``sources``, in order."""

    expansion = Expansion()
    profile = None if options.profile is None else find_profile(service.catalog, options.profile)
    expansion.profile_timeout_s = None if profile is None else profile.timeout_s
    for index, source in enumerate(sources):
        kind, value = next(iter(source.items()))
        if kind == "random":
            excludes = [Exclusion.parse(item) for item in value["excludes"]]
            experiments = value["experiments"]
            if experiments == "all":
                experiments = opted_in(service.catalog, excludes)
            seeds = value["seeds"] or (profile.seeds if profile is not None and profile.seeds else 1)
            for experiment in experiments:
                service.catalog.experiments.resolve(experiment)
                for seed in range(value["first_seed"], value["first_seed"] + seeds):
                    generate = {
                        "seed": seed,
                        "experiment": experiment,
                        "excludes": value["excludes"],
                        "base": value["base"],
                        "profile": options.profile,
                    }
                    _unique(
                        expansion.jobs,
                        {
                            "id": f"random-{experiment}-s{seed}",
                            "source": "random",
                            "label": f"{experiment} seed {seed}",
                            "generate": generate,
                        },
                    )
        elif kind == "matrix":
            _matrix_jobs(value, index, service, expansion)
        elif kind == "scenarios":
            for path in (item for entry in value for item in _scenario_files(entry)):
                try:
                    scenario = load_yaml_mapping(Path(path).read_text(encoding="utf-8"), source_name=path)
                except Exception as exc:
                    raise BatchError(f"cannot read the scenario {path}: {exc}") from None
                _unique(
                    expansion.jobs,
                    {
                        "id": f"scenario-{Path(path).stem}",
                        "source": "scenarios",
                        "label": path,
                        "scenario": json.loads(json.dumps(scenario, default=str)),
                    },
                )
        elif kind == "zoo":
            _zoo_jobs(value, expansion)
        else:
            for job in failure_samples(
                value["batch"], statuses=value["statuses"], per_signature=value["per_signature"]
            ):
                _unique(
                    expansion.jobs,
                    {
                        **job,
                        "id": f"rerun-{job['id']}",
                        "source": "failures",
                        "origin": {"batch": value["batch"], "job": job["id"]},
                    },
                )
        if len(expansion.jobs) > MAX_JOBS:
            raise BatchError(f"the batch has more than {MAX_JOBS} jobs")
    if any("generate" in job for job in expansion.jobs):
        expansion.catalog_digest = service.catalog_digest()
    return expansion


def _job_definition(job: Mapping[str, Any]) -> dict[str, Any]:
    """A job without its batch-specific fields: what runs again the same way."""

    return {key: copy.deepcopy(job[key]) for key in ("id", "label", "generate", "scenario") if key in job}


def failure_samples(
    folder: str, *, statuses: Sequence[str] = ("FAIL", "ERROR"), per_signature: Optional[int] = None
) -> list[dict[str, Any]]:
    """The jobs of the batch in ``folder`` whose results have one of ``statuses``, up to ``per_signature`` each.

    Zoo runs are left out: their failures are expected.
    """

    jobs = {job["id"]: job for job in _read_json(os.path.join(folder, "jobs.json"))}
    picked = _per_signature(
        _results(folder).values(),
        lambda result: result["status"] in statuses and result["id"] in jobs and "zoo" not in jobs[result["id"]],
        per_signature,
    )
    return [_job_definition(jobs[job_id]) for job_id in sorted(picked)]


def _per_signature(
    results: Iterable[Mapping[str, Any]], keep: Callable[[Mapping[str, Any]], bool], count: Optional[int]
) -> list[str]:
    """The IDs of up to ``count`` (all, for None) kept results of each failure signature, by signature then ID."""

    groups: dict[str, list[str]] = {}
    for result in results:
        if keep(result):
            groups.setdefault(result["signature_key"], []).append(result["id"])
    return [job_id for key in sorted(groups) for job_id in sorted(groups[key])[:count]]


# ---------------------------------------------------------------------------
# The worker: one job in its own process
# ---------------------------------------------------------------------------


def _check_port_block(plan: ExecutionPlan) -> None:
    count = len(plan.instances)
    if count > PORT_BLOCK:
        raise BatchError(f"the scenario plans {count} instances; a batch worker's port block holds {PORT_BLOCK}")


def run_worker(task_path: str) -> int:
    """Run the job in ``task_path`` in its report folder; the exit code is the run's (2 when it cannot start)."""

    task = _read_json(task_path)
    job, report_dir = task["job"], task["report_dir"]
    isolation = {
        "deployment": {
            "base_port": task["base_port"],
            "base_data_dir": task["data_dir"],
            "loopback_block": task["loopback_block"],
        }
    }
    service = RunService(
        experiment_directories=task["experiment_directories"],
        discover_plugins=task["discover_plugins"],
        artifact_dir=task["artifact_dir"],
    )
    try:
        if "generate" in job:
            draw = job["generate"]
            prepared = service.prepare_random(
                seed=draw["seed"],
                experiment=draw["experiment"],
                excludes=[Exclusion.parse(item) for item in draw["excludes"]],
                base=merge(with_valkey_binary(draw["base"], task["valkey_binary"], override=False), isolation),
                profile=draw["profile"],
                free_ports=False,
            )
        else:
            scenario = merge(with_valkey_binary(job["scenario"], task["valkey_binary"], override=False), isolation)
            prepared = PreparedRun(scenario=scenario, plan=service.compile(scenario))
        _check_port_block(prepared.plan)
    except Exception as exc:
        error = error_record(exc, "prepare")
        write_document(os.path.join(report_dir, ERROR_FILE), error)
        print(f"Error: the job could not start: {error['type']}: {error.get('message', '')}")
        return EXIT_HARNESS_ERROR
    print(f"Job {job['id']}: {job.get('label', '')}")
    record = service.execute(
        prepared.plan,
        scenario=prepared.scenario,
        timeout=task["timeout_s"],
        report_dir=report_dir,
        generated=prepared.generated,
    )
    if prepared.generated is None:
        Path(report_dir, SCENARIO_FILE).write_text(yaml.safe_dump(prepared.scenario, sort_keys=False), "utf-8")
    print_outcome(record.outcome, verbose=True)
    return exit_code(record.outcome.status)


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------


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


def _error_outcome(plan_data: Optional[Mapping[str, Any]], error: Mapping[str, Any]) -> dict[str, Any]:
    """The plain outcome data of a run that left no report, for its signature."""

    return {"status": "ERROR", "component_results": [], "diagnostics": {"error": dict(error)}, "plan": plan_data}


def _signature(outcome: Mapping[str, Any]) -> dict[str, Any]:
    """:func:`~valkey_fuzzer.minimize.failure_signature` of plain outcome data."""

    if outcome.get("plan") is not None:
        parsed = RunOutcome.from_data(outcome)
        required = [item.evidence_id for item in parsed.plan.required_evidence]
        return failure_signature(parsed, required)
    # No plan: the run never started (it could not be generated or compiled).
    error = outcome["diagnostics"]["error"]
    return empty_signature("ERROR", error_type=error.get("type"), error_phase=error.get("phase"))


def job_result(
    job: Mapping[str, Any],
    run_dir: str,
    *,
    exit_code: Optional[int],
    timed_out: bool,
    leaks: Sequence[Mapping[str, Any]],
    extra: Mapping[str, Any],
) -> dict[str, Any]:
    """The result of one finished job, from its run folder: status, failure signature, zoo observation."""

    report_path = os.path.join(run_dir, REPORT_FILE)
    report: Optional[dict[str, Any]] = None
    error: Optional[dict[str, Any]] = None
    if os.path.exists(report_path):
        try:
            report = _read_json(report_path)
        except (OSError, ValueError) as exc:
            error = {"type": "UnreadableReport", "message": str(exc), "phase": "batch"}
    elif os.path.exists(os.path.join(run_dir, ERROR_FILE)):
        error = _read_json(os.path.join(run_dir, ERROR_FILE))
    if report is None and error is None:
        reason = "it exceeded its deadline" if timed_out else f"the worker exited with code {exit_code}"
        error = {
            "type": "Deadline" if timed_out else "WorkerExit",
            "message": f"no run report: {reason}",
            "phase": "batch",
        }
    outcome = report["outcome"] if report is not None else _error_outcome(None, error)
    status = report["status"] if report is not None else "ERROR"
    scenario = (report or {}).get("scenario") or job.get("scenario") or {}
    experiment = (scenario.get("experiment") or {}).get("id") or job.get("generate", {}).get("experiment")
    try:
        signature = _signature(outcome)
    except Exception as exc:
        status, error = "ERROR", {"type": "UnreadableReport", "message": str(exc), "phase": "batch"}
        signature = _signature(_error_outcome(None, error))
    summary = report["summary"] if report is not None else f"{error['type']}: {error['message']}"
    if leaks:
        # The run left servers, chains, mounts, or directories behind: its cleanup failed.
        kinds = ", ".join(sorted({item["kind"] for item in leaks}))
        left = f"left {len(leaks)} leftover{'' if len(leaks) == 1 else 's'}: {kinds}"
        # Leftovers first, so a shortened summary still says them.
        summary = f"passed its checks but {left}" if status == "PASS" else f"{left}; {summary}"
        status = worst([status, "ERROR"])
    result = {
        "id": job["id"],
        "source": job["source"],
        "status": status,
        "summary": summary,
        "experiment": experiment,
        "seed": scenario.get("seed", job.get("generate", {}).get("seed")),
        "report": os.path.join("runs", job["id"], REPORT_FILE) if report is not None else None,
        "coverage_key": ((report or {}).get("coverage") or {}).get("key"),
        "exit_code": exit_code,
        "timed_out": timed_out,
        "leaks": [dict(item) for item in leaks],
        "signature": None if status == "PASS" else signature,
        "signature_key": None if status == "PASS" else signature_key(experiment, signature),
        "signature_label": None if status == "PASS" else signature_label(experiment, signature),
        **extra,
    }
    if leaks and not signature["error_type"] and signature["status"] == "PASS":
        result["signature_label"] = f"ERROR | {experiment} | leaked {kinds}"
    if "zoo" in job:
        observed = observe(report["outcome"] if report is not None else None, status)
        run = _zoo_run(job)
        result["zoo"] = {**observed, "detected": None if run.control else detected(run, observed)}
    return result


def _zoo_run(job: Mapping[str, Any]) -> ZooRun:
    zoo = job["zoo"]
    return ZooRun(zoo["case"], zoo["file"], zoo["control"], job["scenario"], tuple(zoo["fail"]), tuple(zoo["missing"]))


def _results(folder: str) -> dict[str, dict[str, Any]]:
    directory = os.path.join(folder, "results")
    if not os.path.isdir(directory):
        return {}
    return {
        name[: -len(".json")]: _read_json(os.path.join(directory, name))
        for name in sorted(os.listdir(directory))
        if name.endswith(".json")
    }


# ---------------------------------------------------------------------------
# Running
# ---------------------------------------------------------------------------


@dataclass
class _Running:
    job: dict[str, Any]
    slot: int
    process: Any
    owner: str
    started: float
    deadline: float
    load_start: float
    signals_sent: int = 0
    last_signal: float = 0.0
    timed_out: bool = False


class BatchRunner:
    """Runs a batch folder's jobs on its workers; see the module docstring."""

    def __init__(
        self,
        folder: str,
        *,
        worker_command: Optional[Sequence[str]] = None,
        log: Callable[[str], None] = print,
    ):
        self.folder = os.path.abspath(folder)
        self.worker_command = tuple(worker_command or WORKER_COMMAND)
        self._log = log
        spec = _read_json(os.path.join(self.folder, "batch.json"))
        self.options = BatchOptions(**spec["options"])
        self.spec = spec
        self.jobs: list[dict[str, Any]] = _read_json(os.path.join(self.folder, "jobs.json"))
        self.stop_requested = False
        self._running: dict[int, _Running] = {}

    # -- creating --------------------------------------------------------------

    @classmethod
    def create(
        cls,
        folder: str,
        options: BatchOptions,
        sources: Sequence[Mapping[str, Any]],
        expansion: Expansion,
        **kwargs: Any,
    ) -> "BatchRunner":
        """Write a new batch folder for ``expansion`` and return its runner."""

        if not expansion.jobs:
            raise BatchError("the batch has no jobs")
        os.makedirs(folder, exist_ok=True)
        if os.path.exists(os.path.join(folder, "batch.json")):
            raise BatchError(f"{folder} already holds a batch; resume it instead")
        spec = {
            "options": asdict(options),
            "sources": list(sources),
            "invalid": expansion.invalid,
            "zoo": expansion.zoo,
            "profile_timeout_s": expansion.profile_timeout_s,
            "catalog_digest": expansion.catalog_digest,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        # jobs.json first: a folder with batch.json is complete.
        write_document(os.path.join(folder, "jobs.json"), expansion.jobs)
        write_document(os.path.join(folder, "batch.json"), spec)
        return cls(folder, **kwargs)

    def check_draws(self) -> None:
        """Refuse to resume random jobs that the installed code would draw differently from the batch's seeds."""

        recorded = self.spec["catalog_digest"]
        if recorded is None:
            return
        service = RunService(
            experiment_directories=self.options.experiment_directories,
            discover_plugins=self.options.discover_plugins,
        )
        if service.catalog_digest() != recorded:
            raise BatchError(
                "the installed fuzzer, plugins, or experiments changed since this batch was created, so its "
                "random jobs would draw other scenarios than their seeds drew; start a new batch"
            )

    # -- paths -----------------------------------------------------------------

    def _path(self, *parts: str) -> str:
        return os.path.join(self.folder, *parts)

    def _work(self, slot: int) -> str:
        return self._path("work", f"w{slot}")

    def log(self, line: str) -> None:
        self._log(line)
        with open(self._path("batch.log"), "a", encoding="utf-8") as handle:
            handle.write(f"{time.strftime('%Y-%m-%dT%H:%M:%S')} {line}\n")

    @property
    def timeout_s(self) -> float:
        return float(self.options.timeout_s or self.spec["profile_timeout_s"] or DEFAULT_TIMEOUT_S)

    # -- the run ---------------------------------------------------------------

    def run(self) -> dict[str, Any]:
        """Run every unfinished job, then the serial re-runs; return the summary (also written to summary.json)."""

        with open(self._path("batch.lock"), "w") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                raise BatchError(f"another process is running the batch in {self.folder}") from None
            return self._run_locked()

    def _run_locked(self) -> dict[str, Any]:
        previous = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
        for sig in previous:
            signal.signal(sig, self._request_stop)
        try:
            self._recover()
            done = _results(self.folder)
            self._run_pool([job for job in self.jobs if job["id"] not in done], self.options.workers)
            if not self.stop_requested and self.options.serial_reruns:
                done = _results(self.folder)
                reruns = [job for job in self.serial_jobs() if job["id"] not in done]
                if reruns:
                    self.log(f"serial re-runs: {len(reruns)} job(s), one at a time")
                self._run_pool(reruns, 1)
            if not self._running:
                # Nothing is left running: the next run has no workers to recover.
                for path in (self._path("workers.json"), self._path("work")):
                    shutil.rmtree(path, ignore_errors=True) if os.path.isdir(path) else Path(path).unlink(
                        missing_ok=True
                    )
            summary = self.summary()
            write_document(self._path("summary.json"), summary)
            return summary
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)

    def _request_stop(self, signum: int, _frame: Any) -> None:
        if not self.stop_requested:
            self._log(f"\nStopping: {signal.Signals(signum).name}; interrupting the running jobs (resume to finish)")
        self.stop_requested = True

    def serial_jobs(self) -> list[dict[str, Any]]:
        """``serial_reruns`` jobs of each failure signature of the parallel runs, to run again one at a time."""

        by_id = {job["id"]: job for job in self.jobs}
        picked = _per_signature(
            _results(self.folder).values(),
            lambda result: result["id"] in by_id and "zoo" not in by_id[result["id"]] and bool(result["signature_key"]),
            self.options.serial_reruns,
        )
        return [
            {**_job_definition(by_id[job_id]), "id": f"serial-{job_id}", "source": "serial", "origin": {"job": job_id}}
            for job_id in picked
        ]

    def _recover(self) -> None:
        """Stop workers a killed batch left running, sweep what they left, and requeue their jobs."""

        path = self._path("workers.json")
        if not os.path.exists(path):
            return
        for entry in _read_json(path).values():
            owner = Owner.parse(entry["owner"])
            if owner is not None and owner.alive():
                self.log(f"stopping worker {entry['pid']} of {entry['job']}, left running by an interrupted batch")
                _kill_group(entry["pid"], signal.SIGKILL)
                deadline = time.monotonic() + 30
                while owner.alive() and time.monotonic() < deadline:
                    time.sleep(POLL_S)
            leftovers = self._sweep(entry["slot"], entry["owner"])
            if leftovers:
                self.log(f"removed {len(leftovers)} leftover(s) of interrupted job {entry['job']}")
            shutil.rmtree(self._path("runs", entry["job"]), ignore_errors=True)
        os.remove(path)

    def _save_workers(self) -> None:
        write_document(
            self._path("workers.json"),
            {
                str(slot): {"slot": slot, "job": item.job["id"], "pid": item.process.pid, "owner": item.owner}
                for slot, item in self._running.items()
            },
        )

    def _run_pool(self, todo: list[dict[str, Any]], workers: int) -> None:
        queue = list(todo)
        while (queue and not self.stop_requested) or self._running:
            free = [slot for slot in range(workers) if slot not in self._running]
            while queue and free and not self.stop_requested:
                self._start(queue.pop(0), free.pop(0))
            for slot, item in list(self._running.items()):
                code = item.process.poll()
                if code is None:
                    self._escalate(item)
                    continue
                del self._running[slot]
                self._finish(item, code)
                self._save_workers()
            time.sleep(POLL_S)

    def _start(self, job: dict[str, Any], slot: int) -> None:
        work = self._work(slot)
        shutil.rmtree(work, ignore_errors=True)
        for name in ("data", "tmp", "providers"):
            os.makedirs(os.path.join(work, name))
        run_dir = self._path("runs", job["id"])
        shutil.rmtree(run_dir, ignore_errors=True)
        os.makedirs(run_dir)
        low, _ = self.options.ports(slot)
        task = {
            "job": job,
            "report_dir": run_dir,
            "base_port": low,
            "loopback_block": self.options.loopback_block(slot),
            "data_dir": os.path.join(work, "data"),
            "artifact_dir": os.path.join(work, "providers"),
            "timeout_s": self.timeout_s,
            "valkey_binary": self.options.valkey_binary,
            "experiment_directories": list(self.options.experiment_directories),
            "discover_plugins": self.options.discover_plugins,
        }
        task_path = os.path.join(work, "task.json")
        write_document(task_path, task)
        env = {**os.environ, "TMPDIR": os.path.join(work, "tmp"), "PYTHONUNBUFFERED": "1"}
        with open(os.path.join(run_dir, CONSOLE_FILE), "w", encoding="utf-8") as console:
            process = subprocess.Popen(
                [*self.worker_command, task_path],
                stdout=console,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                env=env,
                start_new_session=True,
            )
        now = time.monotonic()
        self._running[slot] = _Running(
            job=job,
            slot=slot,
            process=process,
            owner=owner_of(process.pid).token,
            started=now,
            deadline=now + self.timeout_s + max(DEADLINE_GRACE_S, 0.25 * self.timeout_s),
            load_start=os.getloadavg()[0],
        )
        self._save_workers()

    def _escalate(self, item: _Running) -> None:
        now = time.monotonic()
        if not self.stop_requested and now < item.deadline:
            return
        if now >= item.deadline and not self.stop_requested:
            item.timed_out = True
        waits = (0.0, *STOP_GRACE_S)
        if item.signals_sent < len(waits) and now - item.last_signal >= waits[item.signals_sent]:
            sig = (signal.SIGINT, signal.SIGTERM, signal.SIGKILL)[item.signals_sent]
            if item.signals_sent == 0 and item.timed_out:
                self.log(f"{item.job['id']}: past its deadline; interrupting it")
            _kill_group(item.process.pid, sig)
            item.signals_sent += 1
            item.last_signal = now

    def _sweep(self, slot: int, owner: str) -> list[dict[str, Any]]:
        work = self._work(slot)
        # /tmp too: a private server's directory is there whatever TMPDIR is, so its unix socket path stays
        # short. Only the worker's own leftovers are removed.
        roots = [os.path.join(work, "data"), os.path.join(work, "tmp"), "/tmp"]
        report = sweep(roots, owners={owner})
        return [asdict(item) for item in report.leftovers]

    def _finish(self, item: _Running, code: int) -> None:
        job, slot = item.job, item.slot
        run_dir = self._path("runs", job["id"])
        leaks = self._sweep(slot, item.owner)
        logs = os.path.join(self._work(slot), "data", "logs")
        if os.path.isdir(logs):
            shutil.copytree(logs, os.path.join(run_dir, "logs"), dirs_exist_ok=True)
        shutil.rmtree(self._work(slot), ignore_errors=True)
        if item.signals_sent and not item.timed_out:
            # Interrupted by the user: no result, so a resume runs it again.
            if leaks:
                self.log(f"{job['id']}: interrupted; removed {len(leaks)} leftover(s)")
            shutil.rmtree(run_dir, ignore_errors=True)
            return
        low, high = self.options.ports(slot)
        extra = {
            "worker": slot,
            # The worker's host-state tag: what the sweep looked for after the run.
            "owner": item.owner,
            "ports": [low, high],
            "loopback_block": self.options.loopback_block(slot),
            "duration_s": round(time.monotonic() - item.started, 1),
            "load": [round(item.load_start, 2), round(os.getloadavg()[0], 2)],
        }
        if job.get("origin"):
            extra["origin"] = job["origin"]
        result = job_result(job, run_dir, exit_code=code, timed_out=item.timed_out, leaks=leaks, extra=extra)
        write_document(self._path("results", f"{job['id']}.json"), result)
        if self.options.prune_pass and result["status"] == "PASS":
            for entry in os.listdir(run_dir):
                if entry not in KEPT_WHEN_PRUNED:
                    target = os.path.join(run_dir, entry)
                    shutil.rmtree(target) if os.path.isdir(target) else os.remove(target)
        leaked = f" LEAKED {len(leaks)}" if leaks else ""
        self.log(
            f"[{result['status']:<16}] {job['id']} {extra['duration_s']}s w{slot}{leaked}: {result['summary'][:200]}"
        )

    # -- the summary -----------------------------------------------------------

    def summary(self) -> dict[str, Any]:
        """Counts, failure signatures with their serial re-runs, zoo verdicts, leaks, and the batch's status."""

        results = _results(self.folder)
        by_id = {job["id"]: job for job in self.jobs}
        main = [results[job["id"]] for job in self.jobs if job["id"] in results and "zoo" not in job]
        counts: dict[str, int] = {}
        for result in main:
            counts[result["status"]] = counts.get(result["status"], 0) + 1
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
        zoo = self._zoo_summary(results, by_id)
        leaks = [{"run": result["id"], **leak} for result in results.values() for leak in result["leaks"]]
        coverage: dict[str, dict[str, Any]] = {}
        for result in main:
            entry = coverage.setdefault(result["experiment"] or "-", {"runs": 0, "tuples": set()})
            entry["runs"] += 1
            if result["coverage_key"]:
                entry["tuples"].add(result["coverage_key"])
        finished = len([job for job in self.jobs if job["id"] in results])
        statuses = [result["status"] for result in main]
        if zoo is not None:
            statuses += ["FAIL" for case in zoo["cases"] if case.get("holds") is False]
            statuses += ["SKIP_UNSUPPORTED" for _ in zoo["skipped"]]
        status = worst(statuses) if self.jobs else "ERROR"
        complete = finished == len(self.jobs) and not self.stop_requested
        return {
            "batch_id": self.options.batch_id,
            "folder": self.folder,
            "profile": self.options.profile,
            "created_at": self.spec["created_at"],
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "complete": complete,
            "status": status,
            "exit_code": exit_code(status) if complete else EXIT_INTERRUPTED,
            "jobs": len(self.jobs),
            "finished": finished,
            "counts": counts,
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
            "zoo": zoo,
            "leaks": leaks,
            "invalid": self.spec["invalid"],
            "coverage": {
                name: {"runs": entry["runs"], "tuples": len(entry["tuples"])}
                for name, entry in sorted(coverage.items())
            },
        }

    def _zoo_summary(self, results: Mapping[str, Any], by_id: Mapping[str, Any]) -> Optional[dict[str, Any]]:
        zoo = self.spec.get("zoo")
        if zoo is None:
            return None
        cases = []
        for entry in zoo["cases"]:
            jobs = [job for job in self.jobs if job.get("zoo", {}).get("case") == entry["case"]]
            if not all(job["id"] in results for job in jobs):
                cases.append({"case": entry["case"], "holds": None, "pending": True})
                continue
            rows = [
                (
                    _zoo_run(job),
                    {k: results[job["id"]]["zoo"][k] for k in ("status", "failed", "missing")},
                    results[job["id"]]["report"],
                )
                for job in jobs
            ]
            cases.append(case_report(load_case(Path(entry["directory"])), rows))
        return {
            "valkey_commit": zoo["valkey_commit"],
            "cases": cases,
            "skipped": zoo["skipped"],
            "held": sum(case.get("holds") is True for case in cases),
            "not_held": [case["case"] for case in cases if case.get("holds") is False],
        }


def _kill_group(pid: int, sig: int) -> None:
    try:
        os.killpg(pid, sig)
    except (ProcessLookupError, PermissionError):
        pass


def main(argv: Optional[Sequence[str]] = None) -> int:
    """``python -m valkey_fuzzer.batch TASK``: the worker process of one job."""

    arguments = sys.argv[1:] if argv is None else list(argv)
    if len(arguments) != 1:
        print("usage: python -m valkey_fuzzer.batch TASK (a batch worker; run batches with valkey-fuzzer batch)")
        return EXIT_HARNESS_ERROR
    try:
        # The batch stops a worker with SIGINT, then SIGTERM: either way its run cleans up first.
        with interrupt_on_sigterm():
            return run_worker(arguments[0])
    except KeyboardInterrupt:
        print("\nThe batch worker was interrupted")
        return EXIT_INTERRUPTED


if __name__ == "__main__":
    sys.exit(main())
