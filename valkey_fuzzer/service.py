"""Run orchestration: compile, execute, report, replay, differentials, and minimization.

:class:`RunService` holds one frozen :class:`~valkey_fuzzer.catalog.ExtensionCatalog`
with its compiler and runner, so compiling and executing always see the
same extensions, and does everything a command does with them: compiling
scenarios to execution plans, executing them, writing reports and reproducers, replaying a
report or plan with provenance and Valkey binary drift checks, and running
differentials and minimization through :mod:`valkey_fuzzer.differential` and
:mod:`valkey_fuzzer.minimize`. Many runs go through the batch runner
(:mod:`valkey_fuzzer.batch`), whose workers each execute one plan here.

It never parses arguments or prints. Methods take plain arguments and return
result objects, progress reaches the caller through optional callbacks, and
failures raise :class:`RunServiceError` subclasses (or the compiler's own
errors) that the command-line interface maps to exit codes. Nothing here knows
about individual experiments, components, or evidence IDs.

Every executed plan, whichever command runs it, leaves one report folder::

    <folder>/plan.json                      the execution plan, written before the run starts
    <folder>/report.json                    the run report (see :meth:`RunService.run_report`)
    <folder>/components/<component id>/    files the run's components wrote

``run`` puts it at ``<root>/runs/<name>`` and ``replay`` at
``<root>/replays/<name>``. A differential or minimization gets a session
folder ``<root>/differential/<name>`` or ``<root>/minimize/<name>`` holding
one report folder per variant (``<variant>``, next to ``differential.json``)
or candidate (``attempt-<n>``); a batch puts each job's at ``runs/<job id>``
of its batch folder. The root is the artifact directory, or ``<tmp>/valkey-fuzzer``. Every ``<name>`` is
``<UTC time to the millisecond>-<scenario or document ID>-<random suffix>``
(:func:`~valkey_fuzzer.runner.new_run_folder`, which also names a run executed
without a report folder), created exclusively, so names sort by start time and
never collide, even between processes.
"""

import copy
import hashlib
import itertools
import json
import os
import platform
import random
import sys
import tempfile
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

import yaml

from ._data import canonical_json, error_record, file_sha256, to_plain_data, with_valkey_binary
from .catalog import ExperimentDirectory, ExtensionCatalog, UnknownExperimentError, build_catalog
from .compiler import ScenarioCompiler
from .contracts import DeploymentProviderRegistration, ExecutionPlan, OutcomeStatus, PluginManifest, RunOutcome
from .coverage import coverage_tuple
from .deployment.local.rerun import binary_drift, deployment_instances, with_free_base_port
from .describe import components_by_role, implementation_name
from .differential import Differential, run_differential
from .generator import Exclusion, Generated, find_profile, generate
from .minimize import failure_signature
from .minimize import minimize as minimize_scenario
from .runner import Runner, default_root, folder_name, new_run_folder, normalize_artifact_root
from .schema import load_yaml, load_yaml_mapping, plain_config

# Every run report has these keys (see :meth:`RunService.run_report`).
REPORT_KEYS = ("generated_at", "status", "summary", "scenario", "outcome", "coverage", "reproducer", "provenance")
# What a report's reproducer records; replay compares each against this host.
REPRODUCER_KEYS = (
    "python",
    "platform",
    "experiment",
    "fuzzer_digest",
    "plugins",
    "components",
    "deployment_provider",
    "plan_digest",
    "valkey_sha",
)

PLAN_FILE = "plan.json"
REPORT_FILE = "report.json"
# A generated scenario, written next to its report so it runs again without the generator.
SCENARIO_FILE = "scenario.yaml"


class RunServiceError(RuntimeError):
    """The service cannot do what was asked; the command-line interface reports a harness error."""


class InputError(RunServiceError):
    """A scenario, report, plan, or document file is missing, unreadable, or of the wrong kind."""


class ReportWriteError(RunServiceError):
    """A report, output, or exported scenario file cannot be written."""


class ReplayDriftError(RunServiceError):
    """This environment differs from the recorded run, and drift was not allowed."""

    def __init__(self, message: str, drift: Sequence[Mapping[str, Any]]):
        super().__init__(message)
        self.drift = [dict(entry) for entry in drift]


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PreparedRun:
    """A scenario as it will be recorded and the execution plan compiled from it.

    ``notes`` are what the command should tell the user about how the
    scenario was prepared (a moved base port, a limit of historical seeds).
    ``generated`` is how a generated scenario was drawn (the run report's
    ``reproducer.generated``); None for a scenario read from a file.
    """

    scenario: dict[str, Any]
    plan: ExecutionPlan
    notes: tuple[str, ...] = ()
    generated: Optional[Mapping[str, Any]] = None


@dataclass(frozen=True)
class RunRecord:
    """One executed plan: its outcome, its run report, and the folder holding both."""

    outcome: RunOutcome
    report: dict[str, Any]
    report_dir: str

    @property
    def report_path(self) -> str:
        return os.path.join(self.report_dir, REPORT_FILE)


@dataclass(frozen=True)
class ReplayPlan:
    """A recorded run (or a bare plan) checked against this environment, ready to replay.

    ``plan`` already uses the ``valkey_binary`` override. ``provenance`` lists
    the drift found before the run and what could not be verified.
    """

    plan: ExecutionPlan
    scenario: Optional[dict[str, Any]]
    provenance: dict[str, Any]
    recorded: Optional[dict[str, Any]]
    recorded_instances: list[Mapping[str, Any]]

    @property
    def verified(self) -> bool:
        return self.recorded is not None

    @property
    def drift(self) -> list[dict[str, Any]]:
        return self.provenance["drift"]

    @property
    def not_verified(self) -> list[str]:
        return self.provenance["not_verified"]


@dataclass(frozen=True)
class ReplayResult:
    """A replayed run; ``late_drift`` is binary drift only the replay's own deployment could reveal."""

    record: RunRecord
    late_drift: list[dict[str, Any]]


@dataclass(frozen=True)
class SessionResult:
    """A differential report, and the folder holding it and every variant's report."""

    report: dict[str, Any]
    report_dir: str


@dataclass(frozen=True)
class Minimization:
    """A recorded run to minimize: the scenario to shrink and the failure signature to keep."""

    scenario: dict[str, Any]
    plan: ExecutionPlan
    reference: dict[str, Any]
    required_evidence: list[str]

    @property
    def needed(self) -> bool:
        """Only a failing or erroring run has a failure to preserve."""

        return self.reference["status"] in (OutcomeStatus.FAIL.value, OutcomeStatus.ERROR.value)


@dataclass(frozen=True)
class MinimizeResult:
    """The smallest scenario that kept the failure, the reductions taken, and every attempt."""

    scenario: dict[str, Any]
    accepted: list[str]
    attempts: list[dict[str, Any]]
    signature: dict[str, Any]
    # None when no candidate compiled, so none ran.
    report_dir: Optional[str]


# ---------------------------------------------------------------------------
# Files and data
# ---------------------------------------------------------------------------


def write_document(path: str, data: Any, output_format: str = "json") -> None:
    """Atomically write ``data`` to ``path`` as JSON or YAML."""

    if output_format not in {"json", "yaml"}:
        raise ValueError(f"unsupported output format: {output_format}")

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
            if output_format == "json":
                json.dump(data, temporary, indent=2, allow_nan=False, default=str)
                temporary.write("\n")
            else:
                yaml.safe_dump(data, temporary, default_flow_style=False, sort_keys=False)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_path, output)
    except BaseException as exc:
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass
        if isinstance(exc, Exception):
            raise ReportWriteError(f"could not write the report to {path}: {exc}") from exc
        raise


def _read_scenario(path: str) -> str:
    source = Path(path)
    if not source.is_file():
        raise InputError(f"scenario file not found: {path}")
    try:
        return source.read_text(encoding="utf-8")
    except OSError as exc:
        raise InputError(f"unable to read scenario file {path}: {exc}") from exc


def _load_document(path: str) -> Any:
    source = Path(path)
    if not source.is_file():
        raise InputError(f"file not found: {path}")
    text = source.read_text(encoding="utf-8")
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


def _new_seed() -> int:
    """A seed for a run that names none; the report records it."""

    return random.randint(0, 2**32 - 1)


def _make_folder(path: str) -> str:
    """Create ``path``, which must not exist yet."""

    try:
        os.makedirs(path)
    except OSError as exc:
        raise ReportWriteError(f"could not create the report folder {path}: {exc}") from exc
    return path


def new_report_folder(parent: str, name: str) -> str:
    """Create and return ``<parent>/<UTC time>-<name>-<suffix>`` (:func:`~valkey_fuzzer.runner.new_run_folder`)."""

    try:
        return new_run_folder(parent, name)
    except OSError as exc:
        raise ReportWriteError(f"could not create a report folder under {parent}: {exc}") from exc


class _Session:
    """A multi-run command's folder, created when the first run (or the summary) needs it."""

    def __init__(self, service: "RunService", kind: str, name: str, timeout: Optional[float]):
        self._service = service
        self._parent = os.path.join(service.report_root, kind)
        self._name = name
        self._timeout = timeout
        self.path: Optional[str] = None

    def folder(self) -> str:
        if self.path is None:
            self.path = new_report_folder(self._parent, self._name)
        return self.path

    def run(
        self,
        name: str,
        plan: ExecutionPlan,
        scenario: Mapping[str, Any],
        generated: Optional[Mapping[str, Any]] = None,
    ) -> RunRecord:
        """Execute one run of the session in its report folder ``name``, which must be unique within it."""

        report_dir = _make_folder(os.path.join(self.folder(), folder_name(name)))
        return self._service.execute(
            plan, scenario=scenario, report_dir=report_dir, timeout=self._timeout, generated=generated
        )


def _implementation(factory: Any) -> dict[str, str]:
    return {"implementation": implementation_name(factory)}


def _top_package(target: Any) -> str:
    return str(getattr(getattr(target, "func", target), "__module__", "") or "").split(".")[0]


def source_digest(packages: Iterable[str]) -> Optional[str]:
    """SHA-256 over every file of each named top-level package (its data files too).

    The whole package, not only the module defining a class, because a
    component's behavior also comes from the helpers it calls and the core
    that judges it. None when a package is not imported or a file cannot be read.
    """

    digest = hashlib.sha256()
    for package in sorted(set(packages)):
        path = getattr(sys.modules.get(package), "__file__", None)
        if not path:
            return None
        root = Path(path)
        if root.name == "__init__.py":
            root = root.parent
            files = sorted(
                file
                for file in root.rglob("*")
                if file.is_file() and "__pycache__" not in file.parts and file.suffix != ".pyc"
            )
        else:
            files = [root]
        for file in files:
            content = file_sha256(str(file))
            if content is None:
                return None
            name = file.name if file == root else file.relative_to(root).as_posix()
            digest.update(f"{package}/{name}\0{content}\n".encode())
    return digest.hexdigest()


def implementation_digest(factory: Any) -> Optional[str]:
    """:func:`source_digest` of the top-level package that defines ``factory``."""

    return source_digest([_top_package(factory)])


def plugin_digest(manifest: PluginManifest) -> Optional[str]:
    """:func:`source_digest` of every top-level package defining something ``manifest`` registers."""

    callables = [
        *(entry.factory for entry in manifest.registrations),
        *(entry.factory for entry in (*manifest.artifact_providers, *manifest.resource_providers)),
        *(entry.factory for entry in manifest.deployment_providers),
        *(entry.hook for entry in manifest.generators),
    ]
    return source_digest(_top_package(target) for target in callables)


# ---------------------------------------------------------------------------
# Replay provenance
# ---------------------------------------------------------------------------


def _drift(item: str, recorded: Any, replay: Any) -> dict[str, Any]:
    return {"item": item, "recorded": recorded, "replay": replay}


def _missing(data: Mapping[str, Any], keys: Sequence[str]) -> list[str]:
    return [key for key in keys if key not in data]


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _rows(value: Any) -> list[Mapping[str, Any]]:
    return [row for row in value if isinstance(row, Mapping)] if isinstance(value, list) else []


def artifact_digests(outcome: Any) -> dict[str, Optional[str]]:
    """Each resolved artifact's ``provenance.sha256`` (None without one), by dependency path, from a plain outcome.

    An artifact can feed a run without any instance starting on it (an
    upgrade target, a file to load), so replay compares these as well as the
    instances' binaries.
    """

    diagnostics = _mapping(_mapping(outcome).get("diagnostics"))
    digests: dict[str, Optional[str]] = {}
    for record in _rows(_mapping(diagnostics.get("environment_report")).get("records")):
        dependency = record.get("dependency")
        if not (isinstance(dependency, str) and dependency.startswith("artifacts.")):
            continue
        if record.get("phase") == "acquire" and record.get("outcome") == "succeeded":
            result = _mapping(_mapping(record.get("details")).get("result"))
            digest = _mapping(result.get("provenance")).get("sha256")
            digests[dependency] = digest if isinstance(digest, str) and digest else None
    return digests


def _run_report(data: Any, path: str) -> Optional[Mapping[str, Any]]:
    """The run report ``data`` is, or None for anything else.

    A document with ``outcome`` or ``reproducer`` is a run report and must have
    all its keys. Anything else is left to the caller, which reads it as a bare
    plan.
    """

    if not isinstance(data, Mapping) or ("outcome" not in data and "reproducer" not in data):
        return None
    missing = _missing(data, REPORT_KEYS)
    if not missing and not isinstance(data["reproducer"], Mapping):
        missing = ["reproducer"]
    elif not missing:
        missing = [f"reproducer.{key}" for key in _missing(data["reproducer"], REPRODUCER_KEYS)]
    if missing:
        raise InputError(f"{path} is not a complete run report: it has no {', '.join(missing)}")
    return data


def _recorded_run(path: str) -> tuple[ExecutionPlan, Optional[dict[str, Any]], Optional[dict[str, Any]]]:
    """The plan to replay, its scenario, and the recorded reproducer and outcome (None for a bare plan)."""

    data = _load_document(path)
    report = _run_report(data, path)
    if report is None:
        return ExecutionPlan.from_data(data), None, None
    outcome = report["outcome"]
    recorded = {"reproducer": report["reproducer"], "outcome": outcome}
    return ExecutionPlan.from_data(outcome["plan"]), report["scenario"], recorded


# ---------------------------------------------------------------------------
# The service
# ---------------------------------------------------------------------------


class RunService:
    """Compile, execute, report, and replay scenarios through one catalog.

    The catalog is built on first use (:func:`~valkey_fuzzer.catalog.build_catalog`)
    from ``experiment_directories``, ``discover_plugins``, ``plugins``, and
    ``deployment_providers``. Report folders go under ``report_root``, which
    defaults to the artifact directory or ``<tmp>/valkey-fuzzer`` (see the
    module docstring), and each run's components write their artifacts into
    its report folder.
    """

    def __init__(
        self,
        *,
        experiment_directories: Sequence[ExperimentDirectory] = (),
        discover_plugins: bool = True,
        plugins: Sequence[PluginManifest] = (),
        deployment_providers: Optional[Sequence[DeploymentProviderRegistration]] = None,
        artifact_dir: str = "",
        report_root: Optional[str] = None,
    ):
        self._catalog_options = {
            "experiment_directories": tuple(experiment_directories),
            "discover_plugins": discover_plugins,
            "plugins": tuple(plugins),
            "deployment_providers": deployment_providers,
        }
        self._artifact_dir = normalize_artifact_root(artifact_dir)
        self._report_root = report_root or artifact_dir or None
        self._catalog: Optional[ExtensionCatalog] = None
        self._compiler: Optional[ScenarioCompiler] = None
        self._runner: Optional[Runner] = None

    @property
    def catalog(self) -> ExtensionCatalog:
        if self._catalog is None:
            self._catalog = build_catalog(**self._catalog_options)
        return self._catalog

    @property
    def runner(self) -> Runner:
        """Runs a plan; unlike :meth:`run_plan`, a preflight error raises."""

        if self._runner is None:
            self._runner = Runner(self.catalog, artifact_dir=self._artifact_dir)
        return self._runner

    def compile(self, scenario: Mapping[str, Any]) -> ExecutionPlan:
        if self._compiler is None:
            self._compiler = ScenarioCompiler(self.catalog)
        return self._compiler.compile(scenario)

    def validate_plan(self, plan: ExecutionPlan) -> None:
        """Check a compiled plan as a run would, without acquiring or provisioning anything."""

        self.runner.validate(plan)

    @property
    def report_root(self) -> str:
        return self._report_root or default_root()

    def new_report_dir(self, kind: str, name: str) -> str:
        """Create a uniquely named report folder ``<report root>/<kind>/<time>-<name>-<suffix>``."""

        return new_report_folder(os.path.join(self.report_root, kind), name)

    # -- catalog -------------------------------------------------------------

    def _component_rows(self) -> list[tuple[dict[str, str], Any]]:
        return [
            (
                {
                    "component_id": registration.component_id,
                    "role": registration.role.value,
                    **_implementation(registration.factory),
                },
                registration.factory,
            )
            for registration in components_by_role(self.catalog)
        ]

    # -- compiling -----------------------------------------------------------

    def prepare_run(self, path: str, *, valkey_binary: Optional[str] = None) -> PreparedRun:
        """Read and compile one scenario file; the recorded scenario includes a ``valkey_binary`` override."""

        scenario = with_valkey_binary(
            plain_config(load_yaml_mapping(_read_scenario(path), source_name=str(path))), valkey_binary
        )
        return PreparedRun(scenario=scenario, plan=self.compile(scenario))

    def generate(
        self,
        seed: int,
        *,
        experiment: Optional[str] = None,
        excludes: Sequence[Exclusion] = (),
        base: Optional[Mapping[str, Any]] = None,
        profile: Optional[str] = None,
    ) -> Generated:
        """A valid scenario drawn from ``seed`` for ``experiment`` (one of the opted-in ones when None).

        ``profile`` names the installed search profile that samples the experiment's legal space.
        """

        catalog = self.catalog
        return generate(
            catalog,
            seed,
            compile_fn=self._checked_plan,
            experiment=experiment,
            excludes=excludes,
            base=base,
            profile=None if profile is None else find_profile(catalog, profile),
        )

    def _checked_plan(self, scenario: Mapping[str, Any]) -> ExecutionPlan:
        """The compiled plan, after the static preflight ``validate`` runs (server config, selectors, ports)."""

        plan = self.compile(scenario)
        self.validate_plan(plan)
        return plan

    def prepare_random(
        self,
        *,
        seed: Optional[int] = None,
        experiment: Optional[str] = None,
        excludes: Sequence[Exclusion] = (),
        base: Optional[Mapping[str, Any]] = None,
        valkey_binary: Optional[str] = None,
        profile: Optional[str] = None,
        free_ports: bool = True,
    ) -> PreparedRun:
        """Generate and compile one scenario (``run --random`` / ``run --seed``); a random seed when None.

        ``base`` is a partial scenario that overrides the draws; a
        ``valkey_binary`` override is part of it, so hooks see the binary.
        ``free_ports`` moves a scenario whose planned ports are busy to free
        ones; the batch runner turns it off, because its workers own their
        port blocks.
        """

        drawn = self.generate(
            _new_seed() if seed is None else seed,
            experiment=experiment,
            excludes=excludes,
            base=with_valkey_binary(base or {}, valkey_binary),
            profile=profile,
        )
        scenario, moved = with_free_base_port(drawn.scenario, drawn.plan) if free_ports else (drawn.scenario, None)
        plan = drawn.plan if moved is None else self.compile(scenario)
        return PreparedRun(scenario=scenario, plan=plan, notes=(moved,) if moved else (), generated=drawn.provenance)

    @staticmethod
    def read_base(path: str) -> dict[str, Any]:
        """A partial scenario file (``run --base``)."""

        document = _load_document(path)
        if not isinstance(document, Mapping):
            raise InputError(f"{path} must hold a partial scenario mapping")
        return plain_config(document)

    def validate(self, path: str, *, valkey_binary: Optional[str] = None) -> ExecutionPlan:
        """Compile a scenario and check it without provisioning."""

        document = load_yaml_mapping(_read_scenario(path), source_name=str(path))
        plan = self.compile(with_valkey_binary(document, valkey_binary))
        self.validate_plan(plan)
        return plan

    # -- executing and reporting ---------------------------------------------

    def run_plan(
        self, plan: ExecutionPlan, *, timeout: Optional[float] = None, run_dir: Optional[str] = None
    ) -> RunOutcome:
        """Execute ``plan`` within ``timeout`` seconds, its components writing into ``run_dir``.

        An error the runner raises becomes an ERROR outcome, so differentials
        and minimization continue: phase ``preflight`` when the plan fails the
        static checks (for example a deployment selector a reduced scenario no
        longer has), ``runner`` when the runner itself fails after them.
        """

        deadline = time.monotonic() + timeout if timeout is not None else None
        try:
            self.validate_plan(plan)
        except Exception as exc:
            return RunOutcome(
                plan=plan,
                status=OutcomeStatus.ERROR,
                summary=f"run failed before execution: {exc}",
                diagnostics={"error": error_record(exc, "preflight")},
            )
        try:
            return self.runner.run(plan, deadline=deadline, run_dir=run_dir)
        except Exception as exc:
            return RunOutcome(
                plan=plan,
                status=OutcomeStatus.ERROR,
                summary=f"the runner failed: {exc}",
                diagnostics={"error": error_record(exc, "runner")},
            )

    def execute(
        self,
        plan: ExecutionPlan,
        *,
        scenario: Optional[Mapping[str, Any]] = None,
        timeout: Optional[float] = None,
        report_dir: Optional[str] = None,
        generated: Optional[Mapping[str, Any]] = None,
    ) -> RunRecord:
        """Run one plan and leave ``plan.json``, ``report.json``, and its artifacts in its report folder.

        ``scenario`` is recorded as run, so the report replays and minimizes on
        its own. Without ``report_dir``, a new folder is made under ``runs``.
        A generated scenario (``generated`` says how it was drawn) is also
        written as ``scenario.yaml``, which runs again without the generator.
        """

        if report_dir is None:
            report_dir = self.new_report_dir("runs", plan.scenario_id)
        self._write_plan(report_dir, plan)
        if generated is not None and scenario is not None:
            self.export_scenario(scenario, os.path.join(report_dir, SCENARIO_FILE), generated=generated)
        outcome = self.run_plan(plan, timeout=timeout, run_dir=report_dir)
        return self.write_run_report(report_dir, outcome, scenario=scenario, generated=generated)

    @staticmethod
    def _write_plan(report_dir: str, plan: ExecutionPlan) -> None:
        write_document(os.path.join(report_dir, PLAN_FILE), plan.to_data())

    def write_run_report(
        self,
        report_dir: str,
        outcome: RunOutcome,
        *,
        scenario: Optional[Mapping[str, Any]] = None,
        provenance: Optional[Mapping[str, Any]] = None,
        generated: Optional[Mapping[str, Any]] = None,
    ) -> RunRecord:
        """Write ``report.json`` for ``outcome``; every command's run reports are written here."""

        report = self.run_report(outcome, scenario=scenario, provenance=provenance, generated=generated)
        write_document(os.path.join(report_dir, REPORT_FILE), report)
        return RunRecord(outcome=outcome, report=report, report_dir=report_dir)

    def run_report(
        self,
        outcome: RunOutcome,
        *,
        scenario: Optional[Mapping[str, Any]] = None,
        provenance: Optional[Mapping[str, Any]] = None,
        generated: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        """One self-contained, replayable report for a single run.

        ``outcome`` holds the plan (``outcome.plan``) with the component
        results, evidence, and diagnostics; ``coverage`` is the run's coverage
        tuple; ``reproducer`` is the environment it ran in, with
        ``generated`` (seed, experiment, attempt, exclusions, boundary)
        for a generated scenario; ``provenance`` is how a replay
        compared with the run it replayed (None for other runs).
        """

        reproducer = self.reproducer(outcome.plan, outcome)
        if generated is not None:
            reproducer["generated"] = to_plain_data(generated)
        return {
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "status": outcome.status.value,
            "summary": outcome.summary,
            "scenario": plain_config(scenario) if scenario is not None else None,
            "outcome": outcome.to_data(),
            "coverage": to_plain_data(coverage_tuple(outcome.plan, outcome)),
            "reproducer": reproducer,
            "provenance": to_plain_data(provenance) if provenance is not None else None,
        }

    def reproducer(self, plan: ExecutionPlan, outcome: Optional[RunOutcome] = None) -> dict[str, Any]:
        """Report-safe provenance needed to reproduce a run exactly."""

        used = {component.use for component in plan.components}
        # Their code is compared through the fuzzer's and each plugin's source digest.
        components = [row for row, _ in self._component_rows() if row["component_id"] in used]
        provider = self.catalog.deployment_providers.get((plan.deployment_mode, plan.deployment_provider.provider_id))
        experiment_digest = None
        try:
            experiment = self.catalog.experiments.resolve(plan.experiment_id)
            experiment_digest = hashlib.sha256(canonical_json(experiment).encode())
        except UnknownExperimentError:
            # A bare plan replayed where its experiment is not installed; the drift check reports it.
            pass
        return {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "experiment": {
                "id": plan.experiment_id,
                "digest": experiment_digest.hexdigest() if experiment_digest is not None else None,
            },
            # Replay compares all the code that ran: the fuzzer's, and each plugin's packages.
            "fuzzer_digest": source_digest([__name__.split(".")[0]]),
            "plugins": [
                {
                    "plugin_id": manifest.plugin_id,
                    "metadata": to_plain_data(manifest.metadata),
                    "source_digest": plugin_digest(manifest),
                }
                for manifest in self.catalog.plugins
            ],
            "components": components,
            # The plan locks the provider's ID; this records which code ran it.
            "deployment_provider": None
            if provider is None
            else {
                "provider_id": provider.provider_id,
                **_implementation(provider.factory),
                "implementation_digest": implementation_digest(provider.factory),
            },
            "plan_digest": hashlib.sha256(plan.to_json().encode("utf-8")).hexdigest(),
            "valkey_sha": outcome.valkey_sha if outcome is not None else None,
        }

    @staticmethod
    def export_scenario(scenario: Mapping[str, Any], path: str, *, generated: Mapping[str, Any]) -> None:
        """Write a generated scenario, as ``generated`` describes it, so that ``run`` runs it again."""

        target = Path(path)
        header = (
            f"# valkey-fuzzer generated scenario: {generated['experiment']}, seed {generated['seed']} "
            f"(attempt {generated['attempt']}).\n"
            f"# Reproduce with: valkey-fuzzer run {target.name}\n"
        )
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(header + yaml.safe_dump(dict(scenario), sort_keys=False), encoding="utf-8")
        except Exception as exc:
            raise ReportWriteError(f"could not export the scenario to {path}: {exc}") from exc

    # -- replay --------------------------------------------------------------

    def provenance_drift(
        self, plan: ExecutionPlan, recorded: Mapping[str, Any]
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """How replaying ``plan`` here differs from a report's ``reproducer``, and what could not be compared.

        The Valkey binary is compared separately by :func:`~valkey_fuzzer.deployment.local.rerun.binary_drift`.
        """

        current = self.reproducer(plan)
        drift: list[dict[str, Any]] = []
        unchecked: list[str] = []
        if recorded["plan_digest"] != current["plan_digest"]:
            drift.append(_drift("plan digest", recorded["plan_digest"], current["plan_digest"]))

        old_digest = _mapping(recorded["experiment"]).get("digest")
        if old_digest != current["experiment"]["digest"]:
            label = f"experiment {plan.experiment_id} digest"
            drift.append(_drift(label, old_digest, current["experiment"]["digest"]))

        def compare_digest(label: str, old_code: Any, new_code: Optional[str]) -> None:
            if old_code is None or new_code is None:
                unchecked.append(f"{label} code: its source cannot be read")
            elif old_code != new_code:
                drift.append(_drift(f"{label} code", old_code, new_code))

        def compare_code(label: str, old_row: Any, new_row: Optional[Mapping[str, Any]], digest: bool) -> None:
            old_row = old_row if isinstance(old_row, Mapping) else None
            old = None if old_row is None else old_row.get("implementation")
            new = None if new_row is None else new_row["implementation"]
            if old != new:
                drift.append(_drift(label, old, new))
            elif digest and old_row is not None and new_row is not None:
                compare_digest(label, old_row.get("implementation_digest"), new_row["implementation_digest"])

        compare_digest("fuzzer", recorded["fuzzer_digest"], current["fuzzer_digest"])
        # A malformed list compares as empty, so every entry it should hold drifts.
        before = {row.get("component_id"): row for row in _rows(recorded["components"])}
        after = {row["component_id"]: row for row in current["components"]}
        for component_id in sorted({component.use for component in plan.components}):
            compare_code(f"component {component_id}", before.get(component_id), after.get(component_id), False)

        # Only plugins the plan uses matter; others may come and go between hosts.
        before = {row.get("plugin_id"): row for row in _rows(recorded["plugins"])}
        used = self._plugins_used(plan)
        for row in current["plugins"]:
            plugin_id = row["plugin_id"]
            old = before.get(plugin_id)
            if plugin_id not in used:
                continue
            if old is None:
                drift.append(_drift(f"plugin {plugin_id}", "not loaded", "loaded"))
            else:
                if old.get("metadata") != row["metadata"]:
                    drift.append(_drift(f"plugin {plugin_id} metadata", old.get("metadata"), row["metadata"]))
                compare_digest(f"plugin {plugin_id}", old.get("source_digest"), row["source_digest"])

        registration = self.catalog.deployment_providers.get(
            (plan.deployment_mode, plan.deployment_provider.provider_id)
        )
        locked = to_plain_data(plan.deployment_provider)
        available = None if registration is None else to_plain_data(registration.identity)
        label = f"deployment provider for {plan.deployment_mode}"
        if available != locked:
            # The runner refuses this plan whatever --allow-drift says.
            drift.append(_drift(label, locked, available))
        else:
            compare_code(label, recorded["deployment_provider"], current["deployment_provider"], True)
        return drift, unchecked

    def catalog_digest(self) -> str:
        """SHA-256 over the code and experiments a draw depends on: the fuzzer, every plugin, every experiment.

        A batch records it, so a resume knows whether its random jobs would
        draw the scenarios their seeds drew when the batch was created.
        """

        material = {
            "fuzzer": source_digest([__name__.split(".")[0]]),
            "plugins": {manifest.plugin_id: plugin_digest(manifest) for manifest in self.catalog.plugins},
            "experiments": {
                experiment_id: hashlib.sha256(canonical_json(experiment).encode()).hexdigest()
                for experiment_id, experiment in self.catalog.experiments.snapshot().items()
            },
        }
        return hashlib.sha256(canonical_json(material).encode()).hexdigest()

    def _plugins_used(self, plan: ExecutionPlan) -> set[str]:
        """Plugins providing the plan's experiment, components, environment providers, or deployment."""

        wanted = {component.use for component in plan.components}
        wanted |= {spec.provider_id for spec in (*plan.artifacts, *plan.resources)}
        used = set()
        for manifest in self.catalog.plugins:
            provided = {entry.component_id for entry in manifest.registrations}
            provided |= {entry.provider_id for entry in (*manifest.artifact_providers, *manifest.resource_providers)}
            if (
                provided & wanted
                or any(spec.experiment_id == plan.experiment_id for spec in manifest.experiments)
                or any(
                    (entry.deployment_mode, entry.provider_id)
                    == (plan.deployment_mode, plan.deployment_provider.provider_id)
                    for entry in manifest.deployment_providers
                )
            ):
                used.add(manifest.plugin_id)
        return used

    def prepare_replay(
        self, path: str, *, valkey_binary: Optional[str] = None, allow_drift: bool = False
    ) -> ReplayPlan:
        """Load a run report (or a bare plan) and check it against this environment.

        Raises :class:`ReplayDriftError` when the fuzzer, experiment,
        components (their import path or code), plugins,
        deployment provider, or Valkey binary differ from the recorded run, or
        when the report's scenario does not compile to the plan it records,
        unless ``allow_drift``. A ``valkey_binary`` override
        is intentional, so it is recorded rather than treated as drift, and
        the replay's scenario uses it, as ``run --valkey-binary`` records it.
        """

        plan, scenario, recorded = _recorded_run(path)
        provenance: dict[str, Any] = {
            "source": path,
            "verified": recorded is not None,
            "drift": [],
            "drift_allowed": allow_drift,
            "not_verified": [],
            "valkey_binary_override": valkey_binary or None,
        }
        recorded_instances: list[Mapping[str, Any]] = []
        if recorded is None:
            provenance["not_verified"].append("everything: the file holds no reproducer")
        else:
            provenance["drift"], provenance["not_verified"] = self._report_drift(
                plan, scenario, recorded, valkey_binary
            )
            recorded_instances = deployment_instances(recorded["outcome"])
        if provenance["drift"] and not allow_drift:
            raise ReplayDriftError("this environment differs from the recorded run", provenance["drift"])
        if valkey_binary:
            data = to_plain_data(plan)
            data["deployment"]["valkey_binary"] = valkey_binary
            plan = ExecutionPlan.from_data(data)
            # The replay report then minimizes and replays with the binary it ran.
            if scenario is not None:
                scenario = with_valkey_binary(scenario, valkey_binary)
        return ReplayPlan(
            plan=plan,
            scenario=scenario,
            provenance=provenance,
            recorded=recorded,
            recorded_instances=recorded_instances,
        )

    def _report_drift(
        self,
        plan: ExecutionPlan,
        scenario: Optional[Mapping[str, Any]],
        recorded: Mapping[str, Any],
        valkey_binary: Optional[str],
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """How running a report's plan here differs from the run it records, and what could not be compared.

        Compared before anything runs: the reproducer, the scenario against
        the plan, and each instance's binary (``valkey_binary`` overrides the
        default one on purpose).
        """

        drift, unchecked = self.provenance_drift(plan, recorded["reproducer"])
        mismatch = self._scenario_mismatch(plan, scenario)
        if mismatch is not None:
            drift.append(mismatch)
        elif scenario is None:
            unchecked.append("scenario: not recorded")
        recorded_instances = deployment_instances(recorded["outcome"])
        binaries, unchecked_binaries = binary_drift(plan, recorded_instances, override=valkey_binary)
        if not recorded_instances and not valkey_binary:
            unchecked_binaries.append("Valkey binary: the recorded run has no per-instance binary digests")
        unchecked_binaries.extend(
            f"{dependency}: the recorded run has no sha256 for it"
            for dependency, digest in sorted(artifact_digests(recorded["outcome"]).items())
            if digest is None
        )
        return drift + binaries, unchecked + unchecked_binaries

    def _scenario_mismatch(
        self, plan: ExecutionPlan, scenario: Optional[Mapping[str, Any]]
    ) -> Optional[dict[str, Any]]:
        """Drift when a report's scenario does not compile, here, to the plan it records.

        Replay runs the plan, and minimize and later replays read the scenario,
        so a report is reproduced only if both describe the same run.
        """

        if scenario is None:
            return None
        item = "the report's scenario and plan"
        try:
            compiled = self.compile(scenario)
        except Exception as exc:
            return _drift(item, "the recorded plan", f"the scenario does not compile here: {exc}")
        recorded, replayed = plan.to_data(), compiled.to_data()
        differing = sorted(key for key in {*recorded, *replayed} if recorded.get(key) != replayed.get(key))
        if not differing:
            return None

        def shown(data: Mapping[str, Any]) -> dict[str, Any]:
            return {
                key: data.get(key) if not isinstance(data.get(key), (Mapping, list, tuple)) else "<differs>"
                for key in differing
            }

        return _drift(item, f"plan {shown(recorded)}", f"scenario compiles to {shown(replayed)}")

    def replay(self, prepared: ReplayPlan, *, timeout: Optional[float] = None) -> ReplayResult:
        """Run a prepared replay and record its provenance, including drift found only by running it."""

        plan = prepared.plan
        override = prepared.provenance["valkey_binary_override"]
        provenance = copy.deepcopy(prepared.provenance)
        report_dir = self.new_report_dir("replays", plan.scenario_id)
        self._write_plan(report_dir, plan)
        outcome = self.run_plan(plan, timeout=timeout, run_dir=report_dir)
        late: list[dict[str, Any]] = []
        if prepared.recorded is not None:
            # A binary an environment provider supplies is only known once the
            # deployment runs; the replay's own deployment metadata settles every instance.
            after, _ = binary_drift(
                plan,
                prepared.recorded_instances,
                override=override,
                replayed_instances=deployment_instances(outcome.to_data()),
            )
            recorded_sha = prepared.recorded["reproducer"].get("valkey_sha")
            if not prepared.recorded_instances and not override and recorded_sha and outcome.valkey_sha:
                if outcome.valkey_sha != recorded_sha:
                    after.append(_drift("Valkey git SHA", recorded_sha, outcome.valkey_sha))
            # Artifacts resolve only when the run starts, so they are compared after it.
            replayed_artifacts = artifact_digests(outcome.to_data())
            for dependency, digest in sorted(artifact_digests(prepared.recorded["outcome"]).items()):
                replayed_digest = replayed_artifacts.get(dependency)
                if digest and replayed_digest and replayed_digest != digest:
                    after.append(_drift(f"{dependency} sha256", digest, replayed_digest))
            late = [entry for entry in after if entry not in provenance["drift"]]
            provenance["drift"].extend(late)
        record = self.write_run_report(report_dir, outcome, scenario=prepared.scenario, provenance=provenance)
        return ReplayResult(record=record, late_drift=late)

    # -- differentials and minimization --------------------------------------

    def differential(
        self,
        path: str,
        *,
        seed: Optional[int] = None,
        valkey_binary: Optional[str] = None,
        timeout: Optional[float] = None,
        on_variant: Optional[Callable[[str, RunOutcome], None]] = None,
    ) -> SessionResult:
        """Run a differential document's variants and compare their evidence."""

        differential = Differential.from_document(_load_document(path))
        if seed is not None:
            differential.base["seed"] = seed
        # Variants patch the base, so a variant that sets its own binary keeps it.
        differential.base = with_valkey_binary(differential.base, valkey_binary)
        # Every variant compiles before the first runs; a bad one leaves no folder.
        session = _Session(self, "differential", differential.differential_id, timeout)

        def execute_variant(name: str, scenario: dict, plan: ExecutionPlan) -> tuple[RunOutcome, str]:
            record = session.run(name, plan, scenario)
            return record.outcome, record.report_dir

        report = run_differential(
            differential,
            compile_fn=self.compile,
            execute_fn=execute_variant,
            components=self.catalog.components,
            on_variant=on_variant,
        )
        write_document(os.path.join(session.folder(), "differential.json"), report)
        return SessionResult(report=report, report_dir=session.folder())

    def prepare_minimization(
        self, path: str, *, valkey_binary: Optional[str] = None, allow_drift: bool = False
    ) -> Minimization:
        """Load a failing run's report: its scenario and the failure signature a reduction must keep.

        Raises :class:`ReplayDriftError` when this environment differs from
        the recorded run, as replay does, unless ``allow_drift``: a reduction
        is kept when it fails the same way, which on other code proves nothing.
        """

        report = _run_report(_load_document(path), path)
        scenario = report.get("scenario") if report is not None else None
        if not isinstance(scenario, Mapping) or "experiment" not in scenario:
            raise InputError("report does not contain the original scenario; minimize needs a 'run' report")
        reference_outcome = RunOutcome.from_data(report["outcome"])
        plan = reference_outcome.plan
        drift, _ = self._report_drift(plan, scenario, report, valkey_binary)
        if drift and not allow_drift:
            raise ReplayDriftError("this environment differs from the recorded run", drift)
        required = [requirement.evidence_id for requirement in plan.required_evidence]
        return Minimization(
            scenario=with_valkey_binary(scenario, valkey_binary),
            plan=plan,
            reference=failure_signature(reference_outcome, required),
            required_evidence=required,
        )

    def minimize(
        self,
        prepared: Minimization,
        *,
        timeout: Optional[float] = None,
        max_attempts: int = 30,
        log: Optional[Callable[[str], None]] = None,
    ) -> MinimizeResult:
        """Greedily shrink the scenario while the run keeps failing the same way."""

        plan = prepared.plan
        spec = self.catalog.experiments.resolve(plan.experiment_id)
        session = _Session(self, "minimize", plan.scenario_id, timeout)
        attempts = itertools.count(1)
        result = minimize_scenario(
            prepared.scenario,
            prepared.reference,
            compile_fn=self.compile,
            execute_fn=lambda scenario, plan: session.run(f"attempt-{next(attempts)}", plan, scenario).outcome,
            experiment_inputs=to_plain_data(spec.inputs),
            required_evidence=prepared.required_evidence,
            deployment_requirements=to_plain_data(spec.deployment_requirements),
            max_attempts=max_attempts,
            log=log,
        )
        return MinimizeResult(
            scenario=plain_config(result["scenario"]),
            accepted=list(result["accepted"]),
            attempts=list(result["attempts"]),
            signature=dict(result["signature"]),
            report_dir=session.path,
        )


__all__ = [
    "PLAN_FILE",
    "REPORT_FILE",
    "REPORT_KEYS",
    "REPRODUCER_KEYS",
    "SCENARIO_FILE",
    "InputError",
    "Minimization",
    "MinimizeResult",
    "PreparedRun",
    "ReplayDriftError",
    "ReplayPlan",
    "ReplayResult",
    "ReportWriteError",
    "RunRecord",
    "RunService",
    "RunServiceError",
    "SessionResult",
    "implementation_digest",
    "new_report_folder",
    "plugin_digest",
    "source_digest",
    "write_document",
]
