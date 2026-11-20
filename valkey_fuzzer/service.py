"""Run orchestration: compile, execute, report, replay, and minimization.

:class:`RunService` holds one frozen :class:`~valkey_fuzzer.catalog.Catalog`
with its compiler and runner, so compiling and executing always see the
same components and experiments, and does everything a command does with them: compiling
scenarios to execution plans, executing them, writing reports and reproducers, replaying a
report or plan with provenance and Valkey binary drift checks, and running
minimization through :mod:`valkey_fuzzer.minimize`. Many runs go through the batch runner
(:mod:`valkey_fuzzer.batch`), whose workers each execute one plan here.

It never parses arguments or prints. Methods take plain arguments and return
result objects, progress reaches the caller through optional callbacks, and
failures raise :class:`~valkey_fuzzer.reports.RunServiceError` subclasses (or the compiler's own
errors) that the command-line interface maps to exit codes. Nothing here knows
about individual experiments, components, or evidence IDs. The report files and
folders are :mod:`valkey_fuzzer.reports`; the reproducer and the replay drift
checks are :mod:`valkey_fuzzer.provenance`.
"""

import copy
import itertools
import os
import random
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from ._data import error_record, to_plain_data
from .catalog import Catalog, ExperimentDirectory, build_catalog
from .compiler import (
    NO_OVERRIDES,
    Overrides,
    ScenarioCompiler,
    binary_fields,
    deployment_registration,
    with_valkey_binary,
)
from .documents import merge
from .generator import Exclusion, Generated, find_profile, generate
from .hoststate import folder_name, normalize_artifact_root
from .minimize import failure_signature
from .minimize import minimize as minimize_scenario
from .plan import ExecutionPlan
from .provenance import replayed_drift, report_drift
from .provenance import reproducer as provenance_reproducer
from .registrations import DeploymentProviderRegistration
from .reports import (
    REPORT_FILE,
    SCENARIO_FILE,
    InputError,
    as_run_report,
    export_scenario,
    load_document,
    make_report_folder,
    new_report_folder,
    read_scenario,
    recorded_run,
    run_report,
    write_document,
    write_plan,
)
from .results import OutcomeStatus, RunOutcome
from .runner import PreflightError, Runner
from .schema import load_yaml_mapping, plain_config
from .spec.conform import conform_run, judge_run

# The deadline of a run that names none and has no search-profile budget (seconds).
DEFAULT_TIMEOUT_S = 1200.0
# How long the TLC run of a run's spec conform check may take (seconds): the batch option conform_tlc_timeout_s,
# whose default this is.
CONFORM_TLC_TIMEOUT_S = 300.0


def derive_timeout(
    plan: ExecutionPlan, *, profile_timeout_s: Optional[float] = None, profile: Optional[str] = None
) -> tuple[float, str]:
    """The run deadline when ``--timeout`` is not given, and where it came from.

    The base is the search profile's ``budget.timeout_s``, else
    :data:`DEFAULT_TIMEOUT_S`. A scenario whose components set ``timeout_s`` may
    need longer: the deadline is never below their sum, so the default does not
    cut a run its own components allowed to take that long.
    """

    if profile_timeout_s is not None:
        base, source = profile_timeout_s, f"search profile {profile!r} budget" if profile else "search profile budget"
    else:
        base, source = DEFAULT_TIMEOUT_S, "default"
    declared = sum(spec.timeout_s for spec in plan.components if spec.timeout_s is not None)
    if declared > base:
        return declared, f"sum of the components' timeout_s, above the {source} of {base:g}s"
    return base, source


# The artifact providers whose path ``--artifact NAME=PATH`` overrides: a binary or a module build on this host,
# so a run recorded elsewhere (a CI batch's parent build, its module builds) resolves it here.
LOCAL_PATH_PROVIDERS = ("valkey.artifact.local-binary", "valkey.artifact.local-module")


def with_artifact_paths(scenario: Mapping[str, Any], paths: Mapping[str, str]) -> dict[str, Any]:
    """A copy of ``scenario`` whose local-binary or local-module artifacts named in ``paths`` resolve those paths."""

    declared = scenario.get("artifacts")
    overrides = {}
    for name, path in paths.items():
        artifact = declared.get(name) if isinstance(declared, Mapping) else None
        if not isinstance(artifact, Mapping):
            raise InputError(f"--artifact {name}: the scenario declares no artifact {name!r}")
        if artifact.get("use") not in LOCAL_PATH_PROVIDERS:
            raise InputError(
                f"--artifact {name}: artifacts.{name} is not a {' or '.join(LOCAL_PATH_PROVIDERS)} artifact"
            )
        overrides[name] = {"with": {"path": path}}
    return merge(scenario, {"artifacts": overrides})


def with_base_port(scenario: Mapping[str, Any], base_port: Optional[int]) -> dict[str, Any]:
    """A copy of ``scenario`` whose deployment starts its ports at ``base_port`` (``--base-port``); every client and
    cluster bus port follows from it. ``scenario`` itself for None."""

    return dict(scenario) if base_port is None else merge(scenario, {"deployment": {"base_port": base_port}})


def _plan_with_artifact_paths(plan: ExecutionPlan, paths: Mapping[str, str]) -> ExecutionPlan:
    """``plan`` with its local-binary or local-module artifacts named in ``paths`` resolving those paths."""

    data = to_plain_data(plan)
    by_id = {spec["id"]: spec for spec in data["artifacts"]}
    for name, path in paths.items():
        spec = by_id.get(name)
        if spec is None:
            raise InputError(f"--artifact {name}: the recorded plan declares no artifact {name!r}")
        if spec["provider_id"] not in LOCAL_PATH_PROVIDERS:
            raise InputError(
                f"--artifact {name}: artifacts.{name} is not a {' or '.join(LOCAL_PATH_PROVIDERS)} artifact"
            )
        spec["config"] = {**spec["config"], "path": path}
    return ExecutionPlan.from_data(data)


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PreparedRun:
    """A scenario as it will be recorded and the execution plan compiled from it.

    ``notes`` are what the command should tell the user about how the
    scenario was prepared (a moved base port).
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
    """A replayed run; ``late_drift`` is drift only running it could reveal (already added to the report's
    ``provenance["drift"]``)."""

    record: RunRecord
    late_drift: list[dict[str, Any]]


@dataclass(frozen=True)
class Minimization:
    """A recorded run to minimize: the scenario to shrink and the failure signature to keep."""

    scenario: dict[str, Any]
    plan: ExecutionPlan
    reference: dict[str, Any]
    required_evidence: list[str]
    drift: list[dict[str, Any]] = field(default_factory=list)

    @property
    def needed(self) -> bool:
        """Only a failing or erroring run has a failure to preserve."""

        return self.reference["status"] in (OutcomeStatus.FAIL.value, OutcomeStatus.ERROR.value)


@dataclass(frozen=True)
class MinimizeResult:
    """The smallest scenario that kept the failure, the reductions taken, every attempt, and whether the failure
    reproduced before anything was reduced (``reproduced``) and the result failed the same way when run again
    (``confirmed``; None when nothing was reduced because it did not reproduce)."""

    scenario: dict[str, Any]
    accepted: list[str]
    attempts: list[dict[str, Any]]
    signature: dict[str, Any]
    reproduced: bool
    confirmed: Optional[bool]
    report_dir: Optional[str]


# ---------------------------------------------------------------------------
# The service
# ---------------------------------------------------------------------------


def _new_seed() -> int:
    """A seed for a run that names none; the report records it."""

    return random.randint(0, 2**32 - 1)


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

        report_dir = make_report_folder(os.path.join(self.folder(), folder_name(name)))
        return self._service.execute(
            plan, scenario=scenario, report_dir=report_dir, timeout=self._timeout, generated=generated
        )


class RunService:
    """Compile, execute, report, and replay scenarios through one catalog.

    The catalog is ``catalog``, or one built on first use
    (:func:`~valkey_fuzzer.catalog.build_catalog`) from ``experiment_directories``
    and ``deployment_providers``. Report folders go under ``report_root``: the
    absolute ``artifact_dir``, or ``<tmp>/valkey-fuzzer`` without one (see the
    module docstring), and each run's components write their artifacts into
    its report folder. ``cores`` is how many cores a run may use, which a
    random draw's deployment must fit (:mod:`~valkey_fuzzer.capacity`); None
    for any number. A scenario read from a file runs as written.
    ``conform_tlc_timeout_s`` bounds the TLC run of each run's spec conform
    check (:meth:`write_run_report`; a batch passes its option).
    """

    def __init__(
        self,
        *,
        experiment_directories: Sequence[ExperimentDirectory] = (),
        deployment_providers: Optional[Sequence[DeploymentProviderRegistration]] = None,
        catalog: Optional[Catalog] = None,
        artifact_dir: str = "",
        cores: Optional[float] = None,
        conform_tlc_timeout_s: float = CONFORM_TLC_TIMEOUT_S,
    ):
        self._catalog_options = {
            "experiment_directories": tuple(experiment_directories),
            "deployment_providers": deployment_providers,
        }
        self._artifact_dir = normalize_artifact_root(artifact_dir)
        self._cores = cores
        self._conform_tlc_timeout_s = conform_tlc_timeout_s
        self._catalog = catalog
        self._compiler: Optional[ScenarioCompiler] = None
        self._runner: Optional[Runner] = None

    @property
    def catalog(self) -> Catalog:
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

    def resolve_timeout(
        self, plan: ExecutionPlan, timeout: Optional[float], *, profile: Optional[str] = None
    ) -> tuple[float, str]:
        """The run deadline and its source: ``--timeout`` if given, else :func:`derive_timeout` with the
        budget of search profile ``profile``."""

        if timeout is not None:
            return timeout, "--timeout"
        budget = None if profile is None else find_profile(self.catalog, profile).timeout_s
        return derive_timeout(plan, profile_timeout_s=budget, profile=profile)

    @property
    def report_root(self) -> str:
        return self._artifact_dir

    # -- compiling -----------------------------------------------------------

    def prepare_run(
        self,
        path: str,
        *,
        overrides: Overrides = NO_OVERRIDES,
    ) -> PreparedRun:
        """Read and compile one scenario file; the recorded scenario includes the ``overrides``: the binary, the
        ``artifacts`` path overrides (``--artifact NAME=PATH``), and a ``base_port`` (``--base-port``)."""

        scenario = with_valkey_binary(
            self.catalog,
            plain_config(load_yaml_mapping(read_scenario(path), source_name=str(path))),
            overrides.binary,
        )
        scenario = with_base_port(with_artifact_paths(scenario, overrides.artifacts), overrides.base_port)
        return PreparedRun(scenario=scenario, plan=self.compile(scenario))

    def generate(
        self,
        seed: int,
        *,
        experiment: Optional[str] = None,
        excludes: Sequence[Exclusion] = (),
        base: Optional[Mapping[str, Any]] = None,
        profile: Optional[str] = None,
        overrides: Overrides = NO_OVERRIDES,
    ) -> Generated:
        """A valid scenario drawn from ``seed`` for ``experiment`` (one of the opted-in ones when None).

        ``profile`` names the installed search profile that samples the experiment's legal space;
        ``overrides.binary`` is the binary the drawn deployment runs, which hooks see (with
        ``overrides.keep_own_binary``, only when ``base`` names none).
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
            overrides=overrides,
            cores=self._cores,
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
        profile: Optional[str] = None,
        free_ports: bool = True,
        overrides: Overrides = NO_OVERRIDES,
    ) -> PreparedRun:
        """Generate and compile one scenario (``run --random`` / ``run --seed``); a random seed when None.

        ``base`` is a partial scenario that overrides the draws; the
        ``overrides.binary`` is applied before the hooks run, so they
        see the binary; with ``overrides.keep_own_binary`` (a batch's binary for
        scenarios that name none) only when ``base`` names none (:meth:`generate`). ``free_ports`` moves a scenario whose planned ports
        are busy to free ones (the deployment provider's ``relocate_ports``
        hook); the batch runner turns it off, because its workers own their
        port blocks. ``overrides.base_port`` (``--base-port``) overrides the ``base``'s.
        """

        if overrides.base_port is not None:
            base = with_base_port(base or {}, overrides.base_port)
        drawn = self.generate(
            _new_seed() if seed is None else seed,
            experiment=experiment,
            excludes=excludes,
            base=base,
            profile=profile,
            overrides=overrides,
        )
        relocate = deployment_registration(drawn.plan, self.catalog).relocate_ports
        moved, note = relocate(drawn.plan) if free_ports and relocate is not None else ({}, None)
        scenario = merge(drawn.scenario, {"deployment": moved}) if moved else drawn.scenario
        plan = self.compile(scenario) if moved else drawn.plan
        return PreparedRun(scenario=scenario, plan=plan, notes=(note,) if note else (), generated=drawn.provenance)

    @staticmethod
    def read_base(path: str) -> dict[str, Any]:
        """A partial scenario file (``run --base``)."""

        document = load_document(path)
        if not isinstance(document, Mapping):
            raise InputError(f"{path} must hold a partial scenario mapping")
        return plain_config(document)

    def validate(self, path: str, *, overrides: Overrides = NO_OVERRIDES) -> ExecutionPlan:
        """Compile a scenario and check it without provisioning."""

        document = load_yaml_mapping(read_scenario(path), source_name=str(path))
        plan = self.compile(with_valkey_binary(self.catalog, document, overrides.binary))
        self.validate_plan(plan)
        return plan

    # -- executing and reporting ---------------------------------------------

    def run_plan(
        self, plan: ExecutionPlan, *, timeout: Optional[float] = None, run_dir: Optional[str] = None
    ) -> RunOutcome:
        """Execute ``plan`` within ``timeout`` seconds, its components writing into ``run_dir``.

        An error the runner raises becomes an ERROR outcome, so batches
        and minimization continue: phase ``preflight`` when the plan fails the
        static checks (for example a deployment selector a reduced scenario no
        longer has), ``runner`` when the runner itself fails after them.
        """

        if timeout is None:
            # Every run is bounded: one that names no deadline gets the derived default.
            timeout = derive_timeout(plan)[0]
        deadline = time.monotonic() + timeout
        try:
            return self.runner.run(plan, deadline=deadline, run_dir=run_dir)
        except PreflightError as exc:
            return RunOutcome(
                plan=plan,
                status=OutcomeStatus.ERROR,
                summary=f"run failed before execution: {exc}",
                diagnostics={"error": error_record(exc.__cause__, "preflight")},
            )
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
        its own, and is also written as ``scenario.yaml``, which ``run`` runs
        again (without the generator, for a generated scenario: ``generated``
        says how it was drawn). Without ``report_dir``, a new folder is made under ``runs``.
        A draw none of whose attempts fit the cores the run may use (its ``shortfall``) is
        ``SKIP_UNSUPPORTED``, saying why, and does not run.
        """

        if report_dir is None:
            report_dir = new_report_folder(os.path.join(self.report_root, "runs"), plan.scenario_id)
        write_plan(report_dir, plan, provenance_reproducer(self.catalog, plan))
        if scenario is not None:
            export_scenario(scenario, os.path.join(report_dir, SCENARIO_FILE), generated=generated)
        shortfall = (generated or {}).get("shortfall")
        if shortfall:
            summary = f"no draw fits the cores this run may use: {shortfall}"
            outcome = RunOutcome(plan=plan, status=OutcomeStatus.SKIP_UNSUPPORTED, summary=summary)
        else:
            try:
                outcome = self.run_plan(plan, timeout=timeout, run_dir=report_dir)
            except BaseException as interrupt:
                # An interrupted run still reports what it found before it (a crash stands); it is not checked
                # against its spec bundles, which could hold the interrupt up for a TLC run.
                partial = getattr(interrupt, "partial_outcome", None)
                if partial is not None:
                    self.write_run_report(report_dir, partial, scenario=scenario, generated=generated, conform=False)
                raise
        return self.write_run_report(report_dir, outcome, scenario=scenario, generated=generated)

    def write_run_report(
        self,
        report_dir: str,
        outcome: RunOutcome,
        *,
        scenario: Optional[Mapping[str, Any]] = None,
        provenance: Optional[Mapping[str, Any]] = None,
        generated: Optional[Mapping[str, Any]] = None,
        conform: bool = True,
    ) -> RunRecord:
        """Write ``report.json`` for ``outcome``; every command's run reports are written here.

        With ``conform``, a run whose plan names a spec bundle is then checked against it (:meth:`conform`), and
        the report written again with the results under ``conform`` and the verdict they give
        (:func:`~valkey_fuzzer.spec.conform.judge_run`): an approved bundle's claimed violation is a failed
        component ``spec-conform:<bundle>``, so the run FAILs. The record holds that outcome and report.
        """

        report_path = os.path.join(report_dir, REPORT_FILE)
        report = self.run_report(
            outcome, scenario=scenario, provenance=provenance, generated=generated, report_path=report_path
        )
        write_document(report_path, report)
        if conform:
            results = self.conform(report_dir, outcome)
            if results:
                outcome = judge_run(outcome, results)
                report = self.run_report(
                    outcome,
                    scenario=scenario,
                    provenance=provenance,
                    generated=generated,
                    report_path=report_path,
                    conform=results,
                )
                write_document(report_path, report)
        return RunRecord(outcome=outcome, report=report, report_dir=report_dir)

    def conform(self, report_dir: str, outcome: RunOutcome) -> dict[str, dict[str, Any]]:
        """The run in ``report_dir`` (its ``report.json`` written) checked against each spec bundle its plan names,
        by bundle (:func:`~valkey_fuzzer.spec.conform.conform_run`, TLC bounded by ``conform_tlc_timeout_s``);
        empty when it names none, so a run without a bundle never looks for java and the jar."""

        if not any((component.config or {}).get("spec") for component in outcome.plan.components):
            return {}
        return conform_run(report_dir, self._conform_tlc_timeout_s)

    def run_report(
        self,
        outcome: RunOutcome,
        *,
        scenario: Optional[Mapping[str, Any]] = None,
        provenance: Optional[Mapping[str, Any]] = None,
        generated: Optional[Mapping[str, Any]] = None,
        report_path: Optional[str] = None,
        conform: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        """:func:`~valkey_fuzzer.reports.run_report` of ``outcome`` through this service's catalog."""

        return run_report(
            self.catalog,
            outcome,
            scenario=scenario,
            provenance=provenance,
            generated=generated,
            report_path=report_path,
            conform=conform,
        )

    # -- replay --------------------------------------------------------------

    def prepare_replay(
        self,
        path: str,
        *,
        overrides: Overrides = NO_OVERRIDES,
    ) -> ReplayPlan:
        """Load a run report (or a bare plan) and compare it with this environment.

        Never refuses on drift: when the fuzzer, experiment, components (their import path or code),
        deployment provider, or Valkey binary differ from the recorded run, or the report's scenario does
        not compile to the plan it records, the differences are returned in ``provenance["drift"]``. A bare
        plan with no provenance beside it replays with ``verified`` false. An ``overrides.binary``
        is intentional, so it is recorded rather than treated as drift, and
        the replay's scenario uses it, as ``run --valkey-binary`` records it.
        ``overrides.artifacts`` (``--artifact NAME=PATH``) resolves local-binary and local-module
        artifacts at other paths; it is recorded, and what runs is still
        digested and compared with the recorded run after it.
        """

        plan, scenario, recorded = recorded_run(path)
        binary, artifacts = overrides.binary, dict(overrides.artifacts)
        provenance: dict[str, Any] = {
            "source": path,
            "verified": recorded is not None,
            "drift": [],
            "not_verified": [],
            "valkey_binary_override": binary or None,
            "artifact_overrides": artifacts,
        }
        if recorded is None:
            provenance["not_verified"].append("everything: the file holds no reproducer")
        else:
            provenance["drift"], provenance["not_verified"] = report_drift(
                self.catalog, self.compile, plan, scenario, recorded, binary
            )
        if binary:
            data = to_plain_data(plan)
            data["deployment"] = merge(
                data["deployment"], binary_fields(deployment_registration(plan, self.catalog), binary)
            )
            plan = ExecutionPlan.from_data(data)
            # The replay report then minimizes and replays with the binary it ran.
            if scenario is not None:
                scenario = with_valkey_binary(self.catalog, scenario, binary)
        if artifacts:
            plan = _plan_with_artifact_paths(plan, artifacts)
            if scenario is not None:
                scenario = with_artifact_paths(scenario, artifacts)
        return ReplayPlan(plan=plan, scenario=scenario, provenance=provenance, recorded=recorded)

    def replay(self, prepared: ReplayPlan, *, timeout: Optional[float] = None) -> ReplayResult:
        """Run a prepared replay and record its provenance, including drift found only by running it."""

        plan = prepared.plan
        override = prepared.provenance["valkey_binary_override"]
        provenance = copy.deepcopy(prepared.provenance)
        report_dir = new_report_folder(os.path.join(self.report_root, "replays"), plan.scenario_id)
        write_plan(report_dir, plan, provenance_reproducer(self.catalog, plan))
        outcome = self.run_plan(plan, timeout=timeout, run_dir=report_dir)
        late: list[dict[str, Any]] = []
        if prepared.recorded is not None:
            after = replayed_drift(self.catalog, plan, prepared.recorded, override, outcome)
            late = [entry for entry in after if entry not in provenance["drift"]]
            provenance["drift"].extend(late)
        record = self.write_run_report(report_dir, outcome, scenario=prepared.scenario, provenance=provenance)
        return ReplayResult(record=record, late_drift=late)

    # -- minimization --------------------------------------

    def prepare_minimization(self, path: str, *, overrides: Overrides = NO_OVERRIDES) -> Minimization:
        """Load a failing run's report: its scenario and the failure signature a reduction must keep.

        Drift from the recorded run is returned in ``Minimization.drift``, not refused: a reduction
        is kept when it fails the same way, which on other code proves less.
        """

        report = as_run_report(load_document(path), path)
        scenario = report.get("scenario") if report is not None else None
        if not isinstance(scenario, Mapping) or "experiment" not in scenario:
            raise InputError("report does not contain the original scenario; minimize needs a 'run' report")
        reference_outcome = RunOutcome.from_data(report["outcome"])
        plan = reference_outcome.plan
        drift, _ = report_drift(self.catalog, self.compile, plan, scenario, report, overrides.binary)
        required = [requirement.evidence_id for requirement in plan.required_evidence]
        return Minimization(
            scenario=with_valkey_binary(self.catalog, scenario, overrides.binary),
            plan=plan,
            reference=failure_signature(reference_outcome, required),
            required_evidence=required,
            drift=drift,
        )

    def minimize(
        self,
        prepared: Minimization,
        *,
        timeout: Optional[float] = None,
        max_attempts: int = 30,
        log: Optional[Callable[[str], None]] = None,
    ) -> MinimizeResult:
        """Greedily shrink the scenario while the run keeps failing the same way; the unreduced scenario runs
        first (``attempt-1``), and the result once more to confirm it."""

        plan = prepared.plan
        spec = self.catalog.experiments.resolve(plan.experiment_id)
        provider = self.catalog.deployment_providers.of_plan(plan)
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
            deployment_reductions=None if provider is None else provider.reductions,
            max_attempts=max_attempts,
            log=log,
        )
        return MinimizeResult(
            scenario=plain_config(result["scenario"]),
            accepted=list(result["accepted"]),
            attempts=list(result["attempts"]),
            signature=dict(result["signature"]),
            reproduced=result["reproduced"],
            confirmed=result["confirmed"],
            report_dir=session.path,
        )
