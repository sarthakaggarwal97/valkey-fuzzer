"""The runner: a generic, deployment-neutral executor of compiled execution plans.

Deployments and what an experiment tests enter only through provider and
component registrations; the runner never dispatches on an individual experiment.

For every component the runner publishes ``<id>.started`` before its
probe/start/inject/run/evaluate call and ``<id>.completed`` (PASS) or
``<id>.failed`` after it; teardown publishes ``<id>.stopping``
and then ``<id>.stopped`` or ``<id>.stop-failed``.
Experiments use these to trigger faults and as required events. Dispatch is
synchronous, and in a run with a deadline the subscribers of these events run
on a watched thread, so a blocking subscriber cannot hold the run past it
(see :meth:`_Execution.emit`).

A run goes: static preflight, acquire the environment, resolve references into
the executable plan, provision the deployment, start observers/workloads/faults/
actions in dependency order, tear them down in reverse, evaluate oracles, clean
up the deployment, and release the environment. Capability probes run before
the environment is acquired when the plan holds no environment references, and
otherwise right after references are resolved; either way before the
deployment is provisioned.
"""

import contextvars
import functools
import os
import random
import re
import secrets
import signal
import tempfile
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Any, Optional

from . import verdict
from ._data import error_record, exception_type, to_plain_data
from .catalog import ExtensionCatalog
from .compiler import check_plan, deployment_registration
from .contracts import (
    DEPLOYMENT_PROVISIONING,
    DEPLOYMENT_READY,
    DEPLOYMENT_RELEASE_EVENTS,
    ROLES,
    RUN_COMPLETED,
    RUN_STARTED,
    VALIDATION_COMPLETED,
    ComponentResult,
    ComponentRole,
    ComponentSpec,
    ComponentStatus,
    DeploymentProvider,
    Evidence,
    ExecutionPlan,
    OutcomeStatus,
    ProvisionedDeployment,
    RunOutcome,
    Stage,
)
from .environment import (
    EnvironmentLease,
    EnvironmentLifecycleError,
    EnvironmentProvisioner,
    EnvironmentReport,
    UnsupportedEnvironmentError,
    collect_environment_references,
)
from .events import EventBus
from .evidence import EvidenceContract
from .hoststate import shared_directory
from .obligations import ObligationLedger


class RunnerError(RuntimeError):
    """A plan, component, or provider broke the runner contract."""


class CooperativeDeadlineError(RunnerError):
    """A component crossed a runner-visible deadline."""


class ComponentTimeoutError(CooperativeDeadlineError):
    """A component call was still running when its deadline passed and was cancelled.

    ``call`` is the call's thread if it was still running after the grace period.
    """

    def __init__(self, message: str, call: Optional[threading.Thread] = None):
        super().__init__(message)
        self.call = call


# How long the runner waits for a cancelled call to return, once right after
# cancelling it and once more after deployment cleanup.
_CANCEL_GRACE_S = 5.0

# How long the subscribers of one event the runner publishes during teardown
# and cleanup may run, when the run has a deadline: teardown ignores the run
# deadline, but a blocking subscriber must not hold the run forever.
_CLEANUP_DISPATCH_S = 60.0


class _ComponentCall(threading.Thread):
    """One component call on a daemon thread, so the runner can stop waiting for it."""

    def __init__(self, function: Callable[[], Any], label: str, deadline: float):
        super().__init__(name=f"valkey-fuzzer-{label}", daemon=True)
        self.label = label
        self.deadline = deadline
        self.cancel_event = threading.Event()
        self.cancelled_at: Optional[float] = None
        self.outcome: dict[str, Any] = {}
        self._function = function

    def run(self) -> None:
        _current_call.set(self)
        try:
            self.outcome["result"] = self._function()
        except BaseException as exc:
            self.outcome["error"] = exc

    def cancel(self, now: float) -> None:
        self.cancelled_at = now
        self.cancel_event.set()


# The watched component call the code runs for: set on the call's thread, and
# seen by every thread the call starts under a copy of its context
# (``contextvars.copy_context().run``), so those share its deadline and cancellation.
_current_call: "contextvars.ContextVar[Optional[_ComponentCall]]" = contextvars.ContextVar(
    "valkey_fuzzer_component_call", default=None
)


def _cancelled_call() -> Optional[_ComponentCall]:
    """The component call this code runs for, if the runner has cancelled it."""

    call = _current_call.get()
    return call if call is not None and call.cancel_event.is_set() else None


def _call_with_watchdog(
    function: Callable[[], Any],
    *,
    deadline: Optional[float],
    label: str,
    clock: Callable[[], float],
    interrupted: Callable[[_ComponentCall], None] = lambda call: None,
) -> Any:
    """Call ``function``; if it is still running at ``deadline``, cancel it.

    Components are synchronous and cannot be interrupted, so the call runs on a
    daemon thread, where ``RunContext.deadline`` is ``deadline``. At the
    deadline the runner cancels it: on that thread ``RunContext.cancelled``
    becomes true, ``RunContext.sleep`` returns early, and
    ``RunContext.deadline`` is the moment of cancellation. The runner waits up
    to ``_CANCEL_GRACE_S`` for the call to return and then raises
    :class:`ComponentTimeoutError` either way, carrying the thread if it is
    still running so the caller can wait for it again after deployment cleanup.
    An interrupt (Ctrl-C) while waiting cancels the call too, hands it to
    ``interrupted``, and is re-raised at once.
    """

    if deadline is None:
        return function()
    call = _ComponentCall(function, label, deadline)
    timeout = deadline - clock()
    call.start()
    try:
        call.join(max(0.0, timeout))
    except BaseException:
        call.cancel(clock())
        interrupted(call)
        raise
    if not call.is_alive():
        if "error" in call.outcome:
            raise call.outcome["error"]
        return call.outcome["result"]
    call.cancel(clock())
    try:
        call.join(_CANCEL_GRACE_S)
    except BaseException:
        interrupted(call)
        raise
    if call.is_alive():
        raise ComponentTimeoutError(
            f"{label} did not return within {timeout:.3f}s and is still running after cancellation", call
        )
    raise ComponentTimeoutError(f"{label} did not return within {timeout:.3f}s and was cancelled")


# Cancelled calls that outlived their grace period, with their run's scenario.
# No run starts in this process while one is alive: it may still be acting on
# a deployment, a port, or a file that the next run would use.
_cancelled_calls: list[tuple[str, _ComponentCall]] = []
_cancelled_calls_lock = threading.Lock()


def _abandoned_calls() -> tuple[Mapping[str, str], ...]:
    """Cancelled calls from any run in this process that are still running."""

    with _cancelled_calls_lock:
        _cancelled_calls[:] = [(scenario, call) for scenario, call in _cancelled_calls if call.is_alive()]
        return tuple({"scenario_id": scenario, "call": call.label} for scenario, call in _cancelled_calls)


@contextmanager
def interrupt_on_sigterm() -> Iterator[None]:
    """Within, SIGTERM raises :class:`KeyboardInterrupt` as Ctrl-C does.

    Its default action ends the process at once, so a run stopped with it
    would tear down nothing and leave its servers running; this way it is
    cleaned up like an interrupted run, and the command exits 130.
    """

    def interrupt(signum: int, _frame: Any) -> None:
        raise KeyboardInterrupt(signal.Signals(signum).name)

    previous = signal.signal(signal.SIGTERM, interrupt)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


def _result(spec: ComponentSpec, status: ComponentStatus, summary: str, **diagnostics: Any) -> ComponentResult:
    return ComponentResult(spec.id, status, summary, diagnostics=diagnostics)


def _component_error(spec: ComponentSpec, exc: BaseException, phase: str) -> ComponentResult:
    summary = f"component execution failed: {type(exc).__name__}: {exc}"
    return _result(spec, ComponentStatus.ERROR, summary, exception=error_record(exc, phase))


def _timed(result: ComponentResult, started: float, ended: float) -> ComponentResult:
    """``result`` with the runner's start and end times where the component left them out."""

    return replace(
        result,
        started_at=started if result.started_at is None else result.started_at,
        ended_at=ended if result.ended_at is None else result.ended_at,
    )


def _close_journal(publish_completed: Callable[[], Any], bus: EventBus) -> list[tuple[str, BaseException]]:
    """Publish ``run.completed`` and quiesce ``bus``, each whatever the other did; the steps that failed, and why."""

    failures = []
    for step, phase in ((publish_completed, RUN_COMPLETED), (bus.quiesce, "event_bus.quiesce")):
        try:
            step()
        except BaseException as exc:
            failures.append((phase, exc))
    return failures


def default_root() -> str:
    """Where runs leave their folders without an artifact directory: ``<tmp>/valkey-fuzzer``."""

    return os.path.join(tempfile.gettempdir(), "valkey-fuzzer")


def normalize_artifact_root(artifact_dir: str) -> str:
    """Absolute artifact root; empty means :func:`default_root`."""

    if not isinstance(artifact_dir, str) or "\x00" in artifact_dir:
        raise ValueError("artifact_dir must be a path string")
    root = os.path.normpath(os.path.abspath(artifact_dir or default_root()))
    if root == os.path.abspath(os.sep):
        raise ValueError("artifact_dir must not resolve to the filesystem root")
    return root


def folder_name(name: str) -> str:
    """``name`` as one safe path segment: anything but letters, digits, '.', '_', and '-' becomes '-'."""

    return re.sub(r"[^A-Za-z0-9_.-]+", "-", name).strip("-.")[:80] or "run"


def _timestamp() -> str:
    now = time.time()
    return f"{time.strftime('%Y%m%dT%H%M%S', time.gmtime(now))}.{int(now * 1000) % 1000:03d}Z"


def new_run_folder(parent: str, name: str) -> str:
    """Create and return ``<parent>/<UTC time>-<name>-<suffix>``; raises ``OSError`` if it cannot.

    The time sorts folders by start; the random suffix and exclusive creation
    keep two runs that start in the same millisecond, in any process, apart.
    Every user's runs share :func:`default_root` and its ``<kind>`` folders
    (``runs``, ``batches``, ...), so ``parent`` there is a shared directory,
    as is the root (:func:`~valkey_fuzzer.hoststate.shared_directory`); the
    run's folder, and a root the caller names, are made as usual.
    """

    if os.path.dirname(os.path.abspath(parent)) == default_root():
        shared_directory(default_root())
        shared_directory(parent)
    else:
        os.makedirs(parent, exist_ok=True)
    for _ in range(8):
        path = os.path.join(parent, f"{_timestamp()}-{folder_name(name)}-{secrets.token_hex(3)}")
        try:
            os.mkdir(path)
        except FileExistsError:
            continue
        return path
    raise FileExistsError(f"could not find an unused folder name under {parent}")


def _has_references(*values: Any) -> bool:
    return any(collect_environment_references(value) for value in values)


def _plan_has_references(plan: ExecutionPlan) -> bool:
    return _has_references(
        plan.inputs,
        plan.deployment,
        plan.instances,
        *(component.config for component in plan.components),
        *(policy.config for policy in plan.policies),
        *(overlay.server_config for overlay in plan.startup_overlays),
    )


def _deployment_has_references(plan: ExecutionPlan) -> bool:
    return _has_references(plan.deployment, *(overlay.server_config for overlay in plan.startup_overlays))


def resolve_plan(plan: ExecutionPlan, lease: EnvironmentLease) -> ExecutionPlan:
    """The executable plan: ``plan`` with every environment reference resolved."""

    if not _plan_has_references(plan):
        return plan
    return replace(
        plan,
        inputs=lease.resolve(plan.inputs),
        deployment=lease.resolve(plan.deployment),
        instances=lease.resolve(plan.instances),
        components=tuple(replace(item, config=lease.resolve(item.config)) for item in plan.components),
        policies=tuple(replace(item, config=lease.resolve(item.config)) for item in plan.policies),
        startup_overlays=tuple(
            replace(item, server_config=lease.resolve(item.server_config)) for item in plan.startup_overlays
        ),
    )


class _Context:
    """The :class:`~valkey_fuzzer.contracts.RunContext` for one run."""

    def __init__(
        self,
        plan: ExecutionPlan,
        event_bus: EventBus,
        obligations: ObligationLedger,
        artifact_root: str,
        deadline: Optional[float],
        run_dir: Optional[str] = None,
    ):
        self.source_plan = plan
        self.plan = plan
        self.event_bus = event_bus
        self._artifact_root = artifact_root
        # Absolute, so evidence names artifact paths that do not depend on the working directory.
        self._run_dir = None if run_dir is None else os.path.abspath(run_dir)
        self._run_dir_lock = threading.Lock()
        self.run_deadline = deadline
        self.cleaning_up = False
        self.services: Mapping[str, Any] = MappingProxyType({"deployment": MappingProxyType({})})
        self.obligations = obligations
        self._rngs: dict[tuple[str, str], random.Random] = {}
        self._rng_lock = threading.Lock()

    @property
    def artifact_dir(self) -> str:
        """This run's folder: the caller's, or a new ``<artifact root>/runs/<time>-<scenario>-<suffix>`` on first use."""

        with self._run_dir_lock:
            if self._run_dir is None:
                self._run_dir = new_run_folder(os.path.join(self._artifact_root, "runs"), self.source_plan.scenario_id)
            return self._run_dir

    @property
    def deadline(self) -> Optional[float]:
        call = _current_call.get()
        if call is not None:
            # A component call sees its own deadline, whatever the runner is
            # doing on other threads: the earlier of the run's and its
            # timeout_s, or a teardown's own timeout_s window. Once it is
            # cancelled, its time is up.
            return call.cancelled_at if call.cancel_event.is_set() else call.deadline
        # The runner's own thread (calls without a deadline, provisioning, and
        # deployment cleanup) and threads that components start outside the
        # call's context see the run deadline, except while the runner tears
        # down or cleans up, which ignores it.
        return None if self.cleaning_up else self.run_deadline

    @property
    def cancelled(self) -> bool:
        return _cancelled_call() is not None

    def sleep(self, seconds: float) -> bool:
        call = _current_call.get()
        if call is not None:
            return not call.cancel_event.wait(max(0.0, seconds))
        time.sleep(max(0.0, seconds))
        return True

    def rng(self, component_id: str, stream: str = "default") -> random.Random:
        """This run's continuing generator for one component stream."""

        with self._rng_lock:
            key = (component_id, stream)
            if key not in self._rngs:
                self._rngs[key] = self.plan.rng(component_id, stream)
            return self._rngs[key]

    def bind_deployment(self, services: Mapping[str, Any]) -> None:
        self.services = MappingProxyType({"deployment": MappingProxyType(dict(services))})


class _EarlyTerminalError(Exception):
    def __init__(self, status: OutcomeStatus, summary: str):
        self.status = status
        self.summary = summary
        super().__init__(summary)


@dataclass
class _Prepared:
    plan: ExecutionPlan
    ordered: tuple[ComponentSpec, ...]
    required: frozenset[str]
    provider: DeploymentProvider


class Runner:
    """Execute compiled plans through the components and providers of one catalog."""

    def __init__(
        self,
        catalog: ExtensionCatalog,
        *,
        artifact_dir: str = "",
        run_id_factory: Callable[[], str] = lambda: uuid.uuid4().hex,
        clock: Callable[[], float] = time.monotonic,
    ):
        self._catalog = catalog
        self._artifact_dir = normalize_artifact_root(artifact_dir)
        self._environment_provisioner = EnvironmentProvisioner(
            artifact_registry=catalog.artifact_providers,
            resource_registry=catalog.resource_providers,
            artifact_root=self._artifact_dir,
        )
        self._run_id_factory = run_id_factory
        self._clock = clock

    # -- preflight -----------------------------------------------------------

    def _prepare(self, plan: ExecutionPlan) -> _Prepared:
        """Static checks that need no environment: the provider, then :func:`~valkey_fuzzer.compiler.check_plan`."""

        registration = deployment_registration(plan, self._catalog, error=RunnerError)
        try:
            provider = registration.factory()
        except Exception as exc:
            raise RunnerError(f"deployment provider factory for {plan.deployment_mode!r} failed: {exc}") from exc
        if isinstance(provider, type) or not isinstance(provider, DeploymentProvider):
            raise RunnerError(
                f"deployment provider factory for {plan.deployment_mode!r} returned {type(provider).__name__}, "
                "which does not implement DeploymentProvider"
            )
        # Plans whose deployment still holds references are validated once bound.
        bound = not _deployment_has_references(plan)
        if bound:
            self._validate_deployment(provider, plan)
        # A plan built without the compiler gets its instances and its components' defaults here.
        plan = check_plan(plan, self._catalog, error=RunnerError, plan_instances=bound)
        return _Prepared(plan, plan.ordered_components, verdict.required_components(plan), provider)

    @staticmethod
    def _validate_deployment(provider: DeploymentProvider, plan: ExecutionPlan) -> None:
        if provider.validate(plan) is not None:
            raise RunnerError("deployment provider validation must return None")

    def validate(self, plan: ExecutionPlan) -> None:
        """Preflight a plan without acquiring, provisioning, or running anything."""

        if not isinstance(plan, ExecutionPlan):
            raise TypeError("plan must be an ExecutionPlan")
        self._prepare(plan)

    # -- execution -----------------------------------------------------------

    def run(
        self,
        plan: ExecutionPlan,
        *,
        deadline: Optional[float] = None,
        run_dir: Optional[str] = None,
    ) -> RunOutcome:
        """Acquire, execute, and fully tear down one plan.

        ``deadline`` is an absolute time from the runner clock. Components see
        it as ``RunContext.deadline``; a call still running at its deadline is
        cancelled and the run is an error (see :func:`_call_with_watchdog`).
        While a cancelled call from any earlier run is still running in this
        process, the run is refused as an ERROR before anything happens.

        ``run_dir`` is the run's own folder, which components see as
        ``RunContext.artifact_dir`` (a run report folder, for example).
        Without it the first component that writes an artifact gets a new
        folder ``<artifact root>/runs/<UTC time>-<scenario_id>-<suffix>``.
        """

        if not isinstance(plan, ExecutionPlan):
            raise TypeError("plan must be an ExecutionPlan")
        started_at = self._clock()
        contract = EvidenceContract(self._catalog.components)
        context = _Context(
            plan,
            EventBus(clock=self._clock, check=contract.check_event),
            ObligationLedger(check=contract.check_change),
            self._artifact_dir,
            deadline,
            run_dir,
        )
        context.event_bus.publish_unchecked(RUN_STARTED, source="runner", payload={"scenario_id": plan.scenario_id})
        abandoned = _abandoned_calls()
        if abandoned:
            return self._finish_early(
                context, OutcomeStatus.ERROR,
                "refused to start: a component call abandoned by an earlier run is still running in this process",
                started_at, {"abandoned_calls": abandoned},
            )  # fmt: skip
        prepared = self._prepare(plan)
        plan = context.plan = context.source_plan = prepared.plan
        contract.bind(plan)
        context.obligations.plan(contract.planned_changes())
        execution = _Execution(self, prepared, context, contract, started_at)
        # Probes see the executable plan. Without environment references that
        # is the compiled plan, so they can decide support before acquisition.
        references = _plan_has_references(plan)
        finished = execution.check(probe=not references)
        if finished is not None:
            return finished[0]
        try:
            lease = self._environment_provisioner.acquire(
                plan.artifacts,
                plan.resources,
                run_id=self._run_id_factory(),
                seed=plan.seed,
                deadline=deadline,
                policies=plan.policies,
            )
        except Exception as exc:
            unsupported = isinstance(exc, UnsupportedEnvironmentError)
            return self._finish_early(
                context,
                OutcomeStatus.SKIP_UNSUPPORTED if unsupported else OutcomeStatus.ERROR,
                "the requested environment is unsupported" if unsupported else "environment acquisition failed",
                started_at,
                self._environment_diagnostics(getattr(exc, "environment_report", None), exc),
                tuple(execution.results.values()),
                execution,
            )

        try:
            try:
                context.plan = resolve_plan(plan, lease)
            except Exception as exc:
                outcome, trusted = (
                    self._finish_early(
                        context, OutcomeStatus.ERROR, "environment binding or runner setup failed", started_at,
                        {"error": error_record(exc, "environment binding")}, (), execution,
                    ),
                    False,
                )  # fmt: skip
            else:
                outcome, trusted = execution.run(resolved=references)
        except BaseException:
            try:
                lease.close()
            except BaseException:
                pass
            raise

        teardown_error = None
        try:
            report = lease.close()
        except EnvironmentLifecycleError as exc:
            teardown_error = exc
            report = exc.environment_report or lease.report
        return self._with_environment_report(outcome, trusted, report, teardown_error)

    def _new_outcome(
        self,
        context: _Context,
        status: OutcomeStatus,
        summary: str,
        started_at: float,
        diagnostics: Optional[Mapping[str, Any]] = None,
        component_results: Sequence[ComponentResult] = (),
        deployment: Optional[ProvisionedDeployment] = None,
    ) -> RunOutcome:
        combined = dict(diagnostics or {})
        if deployment is not None and deployment.metadata:
            combined["deployment"] = deployment.metadata
        # Every published event, and whether its dispatch completed (only committed events count as observed).
        committed = {event.sequence for event in context.event_bus.committed_history()}
        combined["events"] = [
            {**to_plain_data(event), "committed": event.sequence in committed}
            for event in context.event_bus.attempted_history()
        ]
        return RunOutcome(
            plan=context.source_plan,
            status=status,
            summary=summary,
            component_results=tuple(component_results),
            diagnostics=combined,
            started_at=started_at,
            ended_at=self._clock(),
            valkey_sha=None if deployment is None else deployment.valkey_sha,
        )

    def _finish_early(
        self,
        context: _Context,
        status: OutcomeStatus,
        summary: str,
        started_at: float,
        diagnostics: Optional[Mapping[str, Any]] = None,
        component_results: Sequence[ComponentResult] = (),
        execution: Optional["_Execution"] = None,
    ) -> RunOutcome:
        """End a run before execution; ``execution`` dispatches ``run.completed`` to its subscribers under a deadline."""

        diagnostics = dict(diagnostics or {})
        if execution is not None:
            # Probes may have changed state; undo what they left.
            execution.release_obligations(final=True)
            diagnostics.update(execution.obligation_diagnostics())
            if diagnostics.get("open_obligations"):
                status, summary = OutcomeStatus.ERROR, "a component left state it changed for the runner to undo"
        publish = (
            context.event_bus.publish_unchecked
            if execution is None
            else functools.partial(execution.emit, cleanup=True)
        )
        for phase, exc in _close_journal(lambda: publish(RUN_COMPLETED, source="runner"), context.event_bus):
            if not isinstance(exc, Exception):
                raise exc
            status, summary = OutcomeStatus.ERROR, f"event journal {phase} failed"
            diagnostics["error"] = error_record(exc, phase)
        if execution is not None and execution.interrupt is not None:
            raise execution.interrupt
        return self._new_outcome(context, status, summary, started_at, diagnostics, component_results)

    @staticmethod
    def _environment_diagnostics(
        report: Optional[EnvironmentReport], error: Optional[BaseException] = None
    ) -> dict[str, Any]:
        diagnostics: dict[str, Any] = {}
        if report is not None:
            diagnostics["environment_report"] = report.to_data()
        if error is not None:
            diagnostics["environment_error"] = {"type": exception_type(error)}
        return diagnostics

    def _with_environment_report(
        self,
        outcome: RunOutcome,
        trustworthy_failure: bool,
        report: EnvironmentReport,
        teardown_error: Optional[BaseException],
    ) -> RunOutcome:
        diagnostics = dict(to_plain_data(outcome.diagnostics))
        diagnostics.update(self._environment_diagnostics(report, teardown_error))
        status, summary = verdict.environment_status(
            outcome.status,
            outcome.summary,
            trusted=trustworthy_failure,
            cleaned=teardown_error is None and report.status == "cleaned",
        )
        return replace(outcome, status=status, summary=summary, diagnostics=diagnostics, ended_at=self._clock())


class _Execution:
    """Mutable state of one run between environment acquisition and release."""

    def __init__(
        self, runner: Runner, prepared: _Prepared, context: _Context, contract: EvidenceContract, started_at: float
    ):
        self.runner = runner
        self.contract = contract
        self.clock = runner._clock
        self.prepared = prepared
        self.context = context
        self.bus = context.event_bus
        self.started_at = started_at
        self.results: dict[str, ComponentResult] = {}
        self.invoked: set[str] = set()
        self.evidence: list[Evidence] = []
        self.cleanup_errors: list[Mapping[str, Any]] = []
        self.objects: dict[str, Any] = {}
        self.teardowns: list[ComponentSpec] = []
        self.deployment: Optional[ProvisionedDeployment] = None
        self.error: Optional[BaseException] = None
        self.error_phase = "preflight"
        self.terminal: Optional[_EarlyTerminalError] = None
        self.interrupt: Optional[BaseException] = None
        self.current: Optional[ComponentSpec] = None
        self.cancelled_calls: list[_ComponentCall] = []
        self.waited_for_cancelled_calls = False
        # Start calls the runner gave up on that were still running, by
        # component, and the teardowns deferred until they return.
        self.running_starts: dict[str, _ComponentCall] = {}
        self.deferred_teardowns: list[ComponentSpec] = []

    # -- helpers ---------------------------------------------------------------

    def guard(self, step: Callable[[], None]) -> None:
        """Run one phase, turning failures into the run's error or terminal state."""

        try:
            step()
        except _EarlyTerminalError as terminal:
            self.terminal = terminal
        except Exception as exc:
            self.error = exc
            if self.current is not None and self.current.id not in self.results:
                self.results[self.current.id] = _component_error(self.current, exc, self.error_phase)
        except BaseException as exc:
            self.interrupt = exc
        self.current = None

    def cleanup_step(self, owner: str, phase: str, step: Callable[[], Any]) -> bool:
        """Run one teardown step; record its failure and keep tearing down."""

        try:
            step()
            return True
        except BaseException as exc:
            self.record_cleanup_error(owner, phase, exc)
            return False

    def record_cleanup_error(self, owner: str, phase: str, exc: BaseException) -> None:
        self.cleanup_errors.append({"owner": owner, "phase": phase, "exception": error_record(exc)})
        if not isinstance(exc, Exception) and self.interrupt is None:
            self.interrupt = exc

    def blocked(self, spec: ComponentSpec) -> bool:
        reasons = []
        for dependency in spec.depends_on:
            result = self.results.get(dependency)
            if result is None:
                reasons.append(f"dependency {dependency!r} has no result")
            elif result.status is not ComponentStatus.PASS:
                reasons.append(f"dependency {dependency!r} is {result.status.value}")
        if reasons:
            self.results[spec.id] = _result(
                spec, ComponentStatus.SKIP, f"blocked: {', '.join(reasons)}", blocked_by=tuple(reasons)
            )
        return bool(reasons)

    def check_result(self, spec: ComponentSpec, result: Any, label: str) -> None:
        if not isinstance(result, ComponentResult):
            raise RunnerError(f"{label} {spec.id!r} returned {type(result).__name__}, expected ComponentResult")
        if result.component_id != spec.id:
            raise RunnerError(f"{label} {spec.id!r} returned result for {result.component_id!r}")
        self.contract.check_evidence(spec, result.evidence, {event.sequence for event in self.bus.committed_history()})

    def watch(self, function: Callable[[], Any], *, deadline: Optional[float], label: str) -> Any:
        """:func:`_call_with_watchdog`, tracking calls still running after cancellation."""

        def abandon(call: _ComponentCall, cause: BaseException) -> None:
            self.cancelled_calls.append(call)
            with _cancelled_calls_lock:
                _cancelled_calls.append((self.context.source_plan.scenario_id, call))
            # An abandoned call stuck in a subscriber would hold the event
            # bus, and every later event would wait on it.
            self.bus.abort(cause, dispatcher=call.ident)

        def interrupted(call: _ComponentCall) -> None:
            abandon(call, RunnerError(f"{label} was interrupted"))

        try:
            return _call_with_watchdog(
                function, deadline=deadline, label=label, clock=self.clock, interrupted=interrupted
            )
        except ComponentTimeoutError as exc:
            if exc.call is not None:
                abandon(exc.call, exc)
            raise

    def emit(
        self, name: str, *, source: str, payload: Optional[Mapping[str, Any]] = None, cleanup: bool = False
    ) -> None:
        """Publish a runner event, its subscribers bounded like a component call.

        Dispatch stays synchronous: this returns once every subscriber has.
        With a run deadline the subscribers run on a watched thread, until the
        run deadline, or for ``cleanup`` events (teardown, deployment release,
        ``run.completed``) for ``_CLEANUP_DISPATCH_S``, since teardown ignores
        the run deadline. A dispatch still running then is cancelled and the
        event bus aborted: no later event is dispatched or committed, so the
        run is an ERROR. Without a run deadline, they run on this thread.
        """

        publish = functools.partial(self.bus.publish_unchecked, name, source=source, payload=payload)
        deadline = self.context.run_deadline
        if deadline is None:
            publish()
            return
        if cleanup:
            deadline = self.clock() + _CLEANUP_DISPATCH_S
        elif self.clock() >= deadline:
            raise CooperativeDeadlineError(f"deadline expired before event {name!r} could be dispatched")
        try:
            self.watch(publish, deadline=deadline, label=f"dispatch of event {name!r}")
        except ComponentTimeoutError as exc:
            if exc.call is not None:
                self.bus.abort(exc)
            raise

    def call(self, spec: ComponentSpec, method: str, *args: Any, applying: bool = False) -> ComponentResult:
        """Invoke a lifecycle method under the component and run deadlines.

        ``applying`` (a start with a teardown): until the call returns, the
        obligation sweeps leave what the component owns alone.
        """

        started = self.clock()
        deadlines = [item for item in (self.context.run_deadline,) if item is not None]
        if spec.timeout_s is not None:
            deadlines.append(started + spec.timeout_s)
        deadline = min(deadlines) if deadlines else None
        if deadline is not None and started >= deadline:
            raise CooperativeDeadlineError(f"deadline expired before component {spec.id!r} could run")

        def function() -> Any:
            if not applying:
                return getattr(self.objects[spec.id], method)(self.context, spec, *args)
            with self.context.obligations.applying(spec.id):
                return getattr(self.objects[spec.id], method)(self.context, spec, *args)

        result = self.watch(function, deadline=deadline, label=f"component {spec.id!r} {method}")
        ended = self.clock()
        if deadline is not None and ended > deadline:
            raise CooperativeDeadlineError(f"component {spec.id!r} exceeded its cooperative deadline")
        self.check_result(spec, result, "component")
        return _timed(result, started, ended)

    def publish_result(
        self,
        spec: ComponentSpec,
        result: ComponentResult,
        passed: str = "completed",
        failed: str = "failed",
        *,
        cleanup: bool = False,
    ) -> None:
        self.emit(
            f"{spec.id}.{passed if result.status is ComponentStatus.PASS else failed}",
            source=spec.id,
            payload={"status": result.status.value, "summary": result.summary},
            cleanup=cleanup,
        )

    def invoke(
        self, spec: ComponentSpec, method: str, *args: Any, applying: bool = False, raise_error: bool = True
    ) -> ComponentResult:
        """Publish ``<id>.started``, call ``method``, record its result, and publish it; ERROR fails the run
        (unless ``raise_error`` is False: the caller decides)."""

        self.current = spec
        self.emit(f"{spec.id}.started", source=spec.id)
        try:
            result = self.call(spec, method, *args, applying=applying)
        finally:
            if ROLES[spec.role].teardown is None:
                self.contract.finished(spec.id)
        self.results[spec.id] = result
        self.invoked.add(spec.id)
        self.evidence.extend(result.evidence)
        self.publish_result(spec, result)
        if raise_error and result.status is ComponentStatus.ERROR:
            raise RunnerError(f"{spec.role.value} {spec.id!r} returned ERROR: {result.summary}")
        return result

    def instantiate(self) -> None:
        for spec in self.prepared.ordered:
            registration = self.runner._catalog.components.resolve(spec.use)
            try:
                instance = registration.factory()
            except Exception as exc:
                raise RunnerError(f"factory for component {spec.use!r} failed: {exc}") from exc
            protocol = ROLES[spec.role].protocol
            if isinstance(instance, type) or not isinstance(instance, protocol):
                raise RunnerError(
                    f"factory for component {spec.use!r} returned {type(instance).__name__}, "
                    f"which does not implement {protocol.__name__}"
                )
            self.objects[spec.id] = instance

    # -- phases ----------------------------------------------------------------

    def revalidate(self) -> None:
        """Check the executable plan, whose environment references are now resolved, as the compiled one was."""

        plan = self.context.plan
        bound = _deployment_has_references(self.context.source_plan)
        if bound:
            Runner._validate_deployment(self.prepared.provider, plan)
        plan = self.context.plan = check_plan(plan, self.runner._catalog, error=RunnerError, plan_instances=bound)
        self.contract.bind(plan)
        # Execute the resolved component specs, not the compiled ones.
        self.prepared.ordered = plan.ordered_components

    def probe_capabilities(self) -> None:
        self.error_phase = "capability.probe"
        required = self.prepared.required
        for spec in self.prepared.ordered:
            if spec.role is not ComponentRole.CAPABILITY or self.blocked(spec):
                continue
            result = self.invoke(spec, ROLES[spec.role].call)
            if spec.id in required and result.status is ComponentStatus.SKIP:
                raise _EarlyTerminalError(
                    OutcomeStatus.SKIP_UNSUPPORTED, f"required capability {spec.id!r} is unsupported"
                )
            if spec.id in required and result.status is ComponentStatus.FAIL:
                raise _EarlyTerminalError(
                    OutcomeStatus.ERROR,
                    f"required capability {spec.id!r} failed; a capability probe establishes support, "
                    "not the behavior under test, so no trustworthy verdict is possible",
                )
        blocked = [
            spec.id
            for spec in self.prepared.ordered
            if spec.role is ComponentRole.CAPABILITY
            and spec.id in required
            and self.results[spec.id].status is not ComponentStatus.PASS
        ]
        if blocked:
            unsupported = any(self.results[item].status is ComponentStatus.SKIP for item in blocked)
            raise _EarlyTerminalError(
                OutcomeStatus.SKIP_UNSUPPORTED if unsupported else OutcomeStatus.ERROR,
                f"required capabilities were blocked: {', '.join(blocked)}",
            )

    def provision(self) -> None:
        self.error_phase = "deployment.provision"
        plan = self.context.plan
        deadline = self.context.run_deadline
        if deadline is not None and self.clock() >= deadline:
            raise CooperativeDeadlineError("deadline expired before deployment provision")
        provider = self.prepared.provider
        self.emit(DEPLOYMENT_PROVISIONING, source="runner", payload={"deployment_mode": plan.deployment_mode})
        try:
            deployment = provider.provision(self.context)
            if not isinstance(deployment, ProvisionedDeployment):
                raise RunnerError(
                    f"deployment provider {plan.deployment_mode!r} returned {type(deployment).__name__}, "
                    "expected ProvisionedDeployment"
                )
        except BaseException as provision_error:
            self.release_deployment("rollback", provider.rollback)
            if self.interrupt is not None and isinstance(provision_error, Exception):
                raise self.interrupt from provision_error
            raise
        self.deployment = deployment
        self.context.bind_deployment(deployment.services)
        if deadline is not None and self.clock() >= deadline:
            raise CooperativeDeadlineError("deployment provision exceeded the run deadline")
        self.emit(DEPLOYMENT_READY, source="runner", payload={"deployment_mode": plan.deployment_mode})

    def release_deployment(self, step: str, method: Callable[..., Any], *args: Any) -> None:
        """Roll back or clean up the deployment, publishing ``deployment.<step>.started`` and ``.completed``."""

        started, completed = DEPLOYMENT_RELEASE_EVENTS[step]

        def release() -> None:
            if method(self.context, *args) is not None:
                raise RunnerError("deployment provider release methods must return None")

        self.cleanup_step(
            "deployment",
            f"{step} event",
            lambda: self.emit(started, source="runner", cleanup=True),
        )
        self.context.cleaning_up = True
        try:
            released = self.cleanup_step("deployment", step, release)
        finally:
            self.context.cleaning_up = False
        if released:
            self.cleanup_step(
                "deployment", f"{step} result event",
                lambda: self.emit(completed, source="runner", cleanup=True),
            )  # fmt: skip

    def start_components(self) -> None:
        for spec in self.prepared.ordered:
            lifecycle = ROLES[spec.role]
            if lifecycle.stage is not Stage.START or self.blocked(spec):
                continue
            self.error_phase = f"{spec.role.value}.{lifecycle.call}"
            if lifecycle.teardown:
                self.teardowns.append(spec)
            try:
                self.invoke(spec, lifecycle.call, applying=lifecycle.teardown is not None)
            except ComponentTimeoutError as exc:
                if exc.call is not None and lifecycle.teardown:
                    self.running_starts[spec.id] = exc.call
                raise

    def tear_down_components(self) -> None:
        for spec in reversed(self.teardowns):
            call = self.running_starts.get(spec.id)
            if call is not None and call.is_alive():
                # Its start call is still running: a teardown now would race
                # it, and whatever it created afterwards would never be released.
                self.deferred_teardowns.append(spec)
                continue
            self.tear_down(spec)

    def tear_down_deferred(self) -> None:
        """Tear down the components whose start outlived the teardown, once it returned; report the rest.

        Runs after deployment cleanup, which may stop what a start blocks on.
        """

        if not self.deferred_teardowns:
            return
        self.wait_for_cancelled_calls()
        for spec in self.deferred_teardowns:
            if not self.running_starts[spec.id].is_alive():
                self.tear_down(spec)
                continue
            self.record_cleanup_error(
                spec.id,
                ROLES[spec.role].teardown,
                RunnerError(
                    f"component {spec.id!r} was not torn down: its {ROLES[spec.role].call} call is still running, "
                    "so anything it creates is not released"
                ),
            )

    def tear_down(self, spec: ComponentSpec) -> None:
        method = ROLES[spec.role].teardown
        self.cleanup_step(
            spec.id,
            "cleanup event",
            lambda: self.emit(f"{spec.id}.stopping", source=spec.id, cleanup=True),
        )
        started = self.clock()
        self.context.cleaning_up = True
        try:
            # Teardown ignores the run deadline but gets the component's own
            # timeout as a fresh window, so a hung stop cannot hang the run.
            result = self.watch(
                lambda: getattr(self.objects[spec.id], method)(self.context, spec),
                deadline=None if spec.timeout_s is None else started + spec.timeout_s,
                label=f"component {spec.id!r} {method}",
            )
            self.check_result(spec, result, "cleanup for component")
        except BaseException as exc:
            self.record_cleanup_error(spec.id, method, exc)
            return
        finally:
            self.context.cleaning_up = False
            self.contract.finished(spec.id)
        result = _timed(result, started, self.clock())
        self.evidence.extend(result.evidence)
        original = self.results.get(spec.id)
        if original is not None:
            diagnostics = dict(to_plain_data(original.diagnostics))
            diagnostics["teardown"] = {
                "status": result.status.value,
                "summary": result.summary,
                "diagnostics": to_plain_data(result.diagnostics),
            }
            self.results[spec.id] = replace(
                original,
                evidence=original.evidence + result.evidence,
                diagnostics=diagnostics,
                ended_at=result.ended_at,
                unobserved=original.unobserved + result.unobserved,
                preconditions={**original.preconditions, **result.preconditions},
            )
        self.cleanup_step(
            spec.id,
            "cleanup result event",
            lambda: self.publish_result(spec, result, passed="stopped", failed="stop-failed", cleanup=True),
        )
        if result.status is not ComponentStatus.PASS:
            self.cleanup_errors.append(
                {"owner": spec.id, "phase": method, "status": result.status.value, "summary": result.summary}
            )

    def evaluate_oracles(self) -> None:
        """Evaluate every oracle. Each judges on its own, so one that returns ERROR (it could not tell) does not
        stop the rest: when another required oracle FAILs, that finding about the server stands, as when
        ``valkey.oracle.liveness`` finds the server dead that another oracle could not read. Otherwise the
        first ERROR fails the run."""

        errored = []
        for spec in self.prepared.ordered:
            if spec.role is not ComponentRole.ORACLE or self.blocked(spec):
                continue
            self.error_phase = "oracle.evaluate"
            result = self.invoke(spec, ROLES[spec.role].call, tuple(self.evidence), raise_error=False)
            if result.status is ComponentStatus.ERROR:
                errored.append(spec)
        found = any(
            spec.role is ComponentRole.ORACLE
            and spec.id in self.prepared.required
            and self.results[spec.id].status is ComponentStatus.FAIL
            for spec in self.prepared.ordered
            if spec.id in self.results
        )
        if errored and not found:
            first = errored[0]
            raise RunnerError(f"oracle {first.id!r} returned ERROR: {self.results[first.id].summary}")
        self.error_phase = "validation"
        self.emit(VALIDATION_COMPLETED, source="runner")

    def clean_up_deployment(self) -> None:
        if self.deployment is not None:
            self.release_deployment("cleanup", self.prepared.provider.cleanup, self.deployment)

    def release_obligations(self, *, final: bool = False) -> None:
        """Undo every obligation still open, except those of a start call still running; ``final`` closes the
        ledger, so whatever that call, or anything else, changes from now on is undone once it can be."""

        self.context.cleaning_up = True
        try:
            if final:
                self.context.obligations.close()
            else:
                self.context.obligations.sweep()
        except BaseException as exc:
            # Ctrl-C during an undo: the rest are undone, the deployment still cleaned up, and then it is raised.
            self.interrupt = self.interrupt or exc
        finally:
            self.context.cleaning_up = False

    def obligation_diagnostics(self) -> dict[str, Any]:
        ledger = self.context.obligations
        diagnostics: dict[str, Any] = {}
        if not ledger.empty:
            diagnostics["obligations"] = ledger.report()
        failures = ledger.failures()
        if failures:
            diagnostics["open_obligations"] = failures
        return diagnostics

    def wait_for_cancelled_calls(self) -> None:
        """Wait once more for cancelled calls, now that cleanup stopped what they may block on."""

        if self.waited_for_cancelled_calls:
            return
        self.waited_for_cancelled_calls = True
        until = self.clock() + _CANCEL_GRACE_S
        try:
            for call in self.cancelled_calls:
                call.join(max(0.0, until - self.clock()))
        except BaseException as exc:
            # Ctrl-C ends the wait, not the cleanup after it.
            self.interrupt = self.interrupt or exc

    def close_journal(self) -> None:
        for phase, exc in _close_journal(lambda: self.emit(RUN_COMPLETED, source="runner", cleanup=True), self.bus):
            if not isinstance(exc, Exception):
                self.interrupt = self.interrupt or exc
            elif self.error is None:
                self.error, self.error_phase = exc, phase

    # -- verdict ---------------------------------------------------------------

    def check(self, *, probe: bool) -> Optional[tuple[RunOutcome, bool]]:
        """Checks that need nothing acquired; the finished run if one of them ends it."""

        def step() -> None:
            self.instantiate()
            if probe:
                self.probe_capabilities()

        self.guard(step)
        if self.terminal is None and self.error is None and self.interrupt is None:
            return None
        return self.finish()

    def run(self, *, resolved: bool) -> tuple[RunOutcome, bool]:
        """Everything after acquisition; a ``resolved`` plan is revalidated and probed first."""

        def setup() -> None:
            if resolved:
                self.revalidate()
                self.probe_capabilities()
            self.provision()
            self.start_components()

        self.guard(setup)
        self.tear_down_components()
        # What the components did not undo is undone before the oracles judge the deployment.
        self.release_obligations()
        if self.terminal is None and self.error is None and self.interrupt is None:
            self.guard(self.evaluate_oracles)
        self.clean_up_deployment()
        self.tear_down_deferred()
        return self.finish()

    def finish(self) -> tuple[RunOutcome, bool]:
        self.wait_for_cancelled_calls()
        # The final cleanup: retry every undo that failed, now that the deployment is gone.
        self.release_obligations(final=True)
        self.close_journal()
        if self.interrupt is not None:
            raise self.interrupt
        return self.verdict()

    def verdict(self) -> tuple[RunOutcome, bool]:
        ordered = self.prepared.ordered
        plan = self.context.source_plan
        decision = verdict.decide(
            plan,
            ordered,
            self.prepared.required,
            self.results,
            self.invoked,
            self.evidence,
            self.bus.committed_history,
            terminal=None if self.terminal is None else (self.terminal.status, self.terminal.summary),
            runner_failure=None
            if self.error is None
            else f"{self.error_phase}: {type(self.error).__name__}: {self.error}",
            cleanup_failed=bool(self.cleanup_errors),
            violations=tuple(self.contract.violations),
            open_obligations=self.context.obligations.failures(),
        )
        diagnostics: dict[str, Any] = {}
        if self.cleanup_errors:
            diagnostics["cleanup_errors"] = tuple(self.cleanup_errors)
        abandoned = [call.label for call in self.cancelled_calls if call.is_alive()]
        if abandoned:
            # Every later run in this process is refused until these return.
            diagnostics["abandoned_calls"] = tuple(
                {"scenario_id": plan.scenario_id, "call": label} for label in abandoned
            )
        diagnostics.update(decision.diagnostics)
        diagnostics.update(self.obligation_diagnostics())
        if self.error is not None:
            diagnostics["error"] = error_record(self.error, self.error_phase)

        outcome = self.runner._new_outcome(
            self.context,
            decision.status,
            decision.summary,
            self.started_at,
            diagnostics,
            tuple(self.results[spec.id] for spec in ordered if spec.id in self.results),
            self.deployment,
        )
        return outcome, decision.trusted
