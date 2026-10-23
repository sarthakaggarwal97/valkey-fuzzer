"""Generic, topology-neutral executor for compiled feature-pack run plans.

Topologies and feature behavior enter only through provider and component
registries; the runner never dispatches on individual Valkey features.
"""

import inspect
import math
import os
import random
import tempfile
import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Any, Optional, Union

from .bound_plan import BoundComponentSpec, BoundRunPlan, RunServices
from .environment import (
    ArtifactProviderRegistry,
    EnvironmentReference,
    ResourceProviderRegistry,
)
from .environment_lifecycle import (
    EnvironmentLifecycleError,
    EnvironmentManager,
    EnvironmentReport,
    EnvironmentReportStatus,
    EnvironmentSession,
    UnsupportedEnvironmentError,
)
from .extensibility import (
    EXTENSION_API_VERSION,
    Action,
    CapabilityProbe,
    ComponentKind,
    ComponentRegistration,
    ComponentResult,
    ComponentSpec,
    ComponentStatus,
    Evidence,
    EvidenceRequirement,
    Fault,
    LifecycleEvent,
    LifecycleEventBus,
    Observer,
    Oracle,
    OutcomeStatus,
    ProvisionedTopology,
    Registry,
    RunMode,
    RunOutcome,
    RunPlan,
    TopologyProvider,
    TopologyProviderRegistration,
    TopologyProviderRegistry,
    UnknownTopologyProviderError,
    Workload,
    to_plain_data,
)

TopologyProviderFactory = Callable[[], TopologyProvider]
TopologyProviderEntry = TopologyProviderRegistration
TopologyProviderSource = Union[
    Mapping[str, TopologyProviderEntry],
    TopologyProviderRegistry,
]


class FeatureRunnerError(RuntimeError):
    """Base class for generic-runner contract failures."""


class CooperativeDeadlineError(FeatureRunnerError):
    """A component crossed a cooperative, runner-visible deadline."""


class _EarlyTerminalError(Exception):
    def __init__(
        self,
        status: OutcomeStatus,
        summary: str,
        *,
        trustworthy_feature_failure: bool = False,
    ):
        self.status = status
        self.summary = summary
        self.trustworthy_feature_failure = trustworthy_feature_failure
        super().__init__(summary)


@dataclass(frozen=True)
class _ResolvedComponent:
    spec: Union[ComponentSpec, BoundComponentSpec]
    registry: Registry[Any]
    registration: ComponentRegistration[Any]
    instance: Any


@dataclass(frozen=True)
class _ValidatedComponent:
    spec: Union[ComponentSpec, BoundComponentSpec]
    registry: Registry[Any]
    registration: ComponentRegistration[Any]


@dataclass(frozen=True)
class _PreparedPlan:
    ordered: tuple[Union[ComponentSpec, BoundComponentSpec], ...]
    effective_required: frozenset[str]
    registration: TopologyProviderRegistration
    provider: TopologyProvider


@dataclass(frozen=True)
class _CleanupEntry:
    resolved: _ResolvedComponent
    method_name: str


@dataclass(frozen=True)
class _ExecutionResult:
    outcome: RunOutcome
    trustworthy_feature_failure: bool = False


class _RunnerContext:
    def __init__(
        self,
        source_plan: RunPlan,
        plan: Union[RunPlan, BoundRunPlan],
        event_bus: LifecycleEventBus,
        artifact_dir: str,
        deadline: Optional[float],
    ):
        self._source_plan = source_plan
        self._plan = plan
        self._event_bus = event_bus
        self._artifact_dir = artifact_dir
        self._run_deadline = deadline
        self._component_deadline: Optional[float] = None
        self._cleanup_depth = 0
        self._services: RunServices = plan.services if isinstance(plan, BoundRunPlan) else RunServices()
        self._topology: Optional[ProvisionedTopology] = None
        self._rngs: dict[tuple[str, str], random.Random] = {}
        self._rng_lock = threading.Lock()

    @property
    def plan(self) -> Union[RunPlan, BoundRunPlan]:
        return self._plan

    @property
    def source_plan(self) -> RunPlan:
        return self._source_plan

    @property
    def event_bus(self) -> LifecycleEventBus:
        return self._event_bus

    def rng(self, component_instance_id: str, stream: str = "default") -> random.Random:
        """Return this run's continuing generator for one component stream.

        Repeated calls return the same generator, so successive draws advance
        the stream instead of replaying its first values.
        """
        key = (component_instance_id, stream)
        with self._rng_lock:
            generator = self._rngs.get(key)
            if generator is None:
                generator = self._plan.rng(component_instance_id, stream)
                self._rngs[key] = generator
            return generator

    @property
    def services(self) -> Mapping[str, Any]:
        return self._services

    @property
    def artifact_dir(self) -> str:
        return self._artifact_dir

    @property
    def deadline(self) -> Optional[float]:
        if self._cleanup_depth:
            return None
        if self._run_deadline is None:
            return self._component_deadline
        if self._component_deadline is None:
            return self._run_deadline
        return min(self._run_deadline, self._component_deadline)

    def bind_plan(self, plan: BoundRunPlan) -> None:
        if not isinstance(plan, BoundRunPlan):
            raise TypeError("plan must be a BoundRunPlan")
        if plan.source_plan is not self._source_plan:
            raise ValueError("bound plan must retain the runner context source plan")
        self._plan = plan
        self._services = plan.services

    def bind_topology(self, topology: ProvisionedTopology, plan: BoundRunPlan) -> None:
        self._topology = topology
        self.bind_plan(plan)

    def clear_topology(self, plan: BoundRunPlan) -> None:
        self._topology = None
        self.bind_plan(plan)

    def set_component_deadline(self, deadline: Optional[float]) -> Optional[float]:
        previous = self._component_deadline
        self._component_deadline = deadline
        return previous

    def enter_cleanup(self) -> None:
        self._cleanup_depth += 1

    def exit_cleanup(self) -> None:
        if self._cleanup_depth < 1:
            raise RuntimeError("cleanup context is not active")
        self._cleanup_depth -= 1


_PROTOCOLS = {
    ComponentKind.CAPABILITY: CapabilityProbe,
    ComponentKind.WORKLOAD: Workload,
    ComponentKind.ACTION: Action,
    ComponentKind.FAULT: Fault,
    ComponentKind.OBSERVER: Observer,
    ComponentKind.ORACLE: Oracle,
}

_START_METHODS = {
    ComponentKind.OBSERVER: ("start", "observer.starting", "observer.started", "observer.start_failed"),
    ComponentKind.WORKLOAD: ("start", "workload.starting", "workload.started", "workload.start_failed"),
    ComponentKind.FAULT: ("arm", "fault.arming", "fault.started", "fault.arm_failed"),
    ComponentKind.ACTION: ("run", "action.started", "action.completed", "action.failed"),
}

# Only these roles judge the system under test. A FAIL from any other role
# (a capability probe, a workload or observer start, a fault arm) means the
# harness could not establish the conditions for a verdict, so it is an ERROR.
_VERDICT_KINDS = frozenset({ComponentKind.ORACLE, ComponentKind.ACTION})

_CLEANUP_METHODS = {
    ComponentKind.OBSERVER: ("stop", "observer.stopping", "observer.stopped", "observer.stop_failed"),
    ComponentKind.WORKLOAD: ("stop", "workload.quiescing", "workload.quiesced", "workload.stop_failed"),
    ComponentKind.FAULT: ("cleanup", "fault.stopping", "fault.stopped", "fault.stop_failed"),
}


_COMPONENT_METHODS = {
    ComponentKind.CAPABILITY: (("probe", 0),),
    ComponentKind.WORKLOAD: (("start", 0), ("stop", 0)),
    ComponentKind.ACTION: (("run", 0),),
    ComponentKind.FAULT: (("arm", 0), ("cleanup", 0)),
    ComponentKind.OBSERVER: (("start", 0), ("stop", 0)),
    ComponentKind.ORACLE: (("evaluate", 1),),
}


def _require_signature(callable_value: Any, args: Sequence[Any], label: str) -> None:
    if not callable(callable_value):
        raise FeatureRunnerError(f"{label} must be callable")
    callable_target = (
        callable_value.__call__
        if not (
            inspect.isfunction(callable_value) or inspect.ismethod(callable_value) or isinstance(callable_value, type)
        )
        else callable_value
    )
    if (
        inspect.iscoroutinefunction(callable_target)
        or inspect.isasyncgenfunction(callable_target)
        or inspect.isgeneratorfunction(callable_target)
    ):
        raise FeatureRunnerError(f"{label} must be synchronous")
    try:
        inspect.signature(callable_value).bind(*args)
    except (TypeError, ValueError) as exc:
        raise FeatureRunnerError(f"{label} has an incompatible signature: {exc}") from exc


def stable_topological_order(
    components: Sequence[Union[ComponentSpec, BoundComponentSpec]],
) -> tuple[Union[ComponentSpec, BoundComponentSpec], ...]:
    """Return a dependency order, preserving manifest order whenever possible."""

    entries = tuple(components)
    by_id = {component.instance_id: component for component in entries}
    if len(by_id) != len(entries):
        raise ValueError("component instance IDs must be unique")

    index = {component.instance_id: position for position, component in enumerate(entries)}
    indegree = {component.instance_id: len(component.depends_on) for component in entries}
    dependents: dict[str, list[str]] = {component.instance_id: [] for component in entries}
    for component in entries:
        for dependency in component.depends_on:
            if dependency not in by_id:
                raise ValueError(f"component {component.instance_id!r} has unknown dependency {dependency!r}")
            dependents[dependency].append(component.instance_id)

    ready = sorted(
        (component.instance_id for component in entries if indegree[component.instance_id] == 0),
        key=index.__getitem__,
    )
    ordered = []
    while ready:
        instance_id = ready.pop(0)
        ordered.append(by_id[instance_id])
        newly_ready = []
        for dependent in dependents[instance_id]:
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                newly_ready.append(dependent)
        if newly_ready:
            ready.extend(newly_ready)
            ready.sort(key=index.__getitem__)

    if len(ordered) != len(entries):
        raise ValueError("component dependency graph contains a cycle")
    return tuple(ordered)


_MAX_EXCEPTION_MESSAGE = 2000


def _exception_data(
    exc: BaseException,
    *,
    phase: Optional[str] = None,
) -> Mapping[str, str]:
    error_type = type(exc)
    qualified_name = error_type.__qualname__
    if error_type.__module__ != "builtins":
        qualified_name = f"{error_type.__module__}.{qualified_name}"
    result = {"type": qualified_name}
    message = str(exc)
    if message:
        result["message"] = (
            message if len(message) <= _MAX_EXCEPTION_MESSAGE else message[:_MAX_EXCEPTION_MESSAGE] + "..."
        )
    if phase is not None:
        result["phase"] = phase
    return result


_REDACTED = "<redacted>"


def _sensitive_values(binding: Any) -> tuple[str, ...]:
    """Collect resolved environment outputs, which must never appear in reports."""
    values: set[str] = set()

    def collect(value: Any) -> None:
        if isinstance(value, Mapping):
            for nested in value.values():
                collect(nested)
        elif isinstance(value, (list, tuple)):
            for nested in value:
                collect(nested)
        elif value is not None and not isinstance(value, bool):
            text = str(value)
            if text:
                values.add(text)

    for artifact in binding.artifacts.values():
        collect(artifact.outputs)
    for resource in binding.resources.values():
        collect(resource.outputs)
    # Replace longer values first so a value containing another is fully hidden.
    return tuple(sorted(values, key=lambda item: (-len(item), item)))


def _redact(value: Any, sensitive_values: Sequence[str]) -> Any:
    if isinstance(value, str):
        for sensitive in sensitive_values:
            if sensitive in value:
                value = value.replace(sensitive, _REDACTED)
        return value
    if isinstance(value, Mapping):
        return {key: _redact(nested, sensitive_values) for key, nested in value.items()}
    if isinstance(value, (list, tuple)):
        return tuple(_redact(nested, sensitive_values) for nested in value)
    return value


def _redact_component_result(result: ComponentResult, sensitive_values: Sequence[str]) -> ComponentResult:
    return replace(
        result,
        summary=_redact(result.summary, sensitive_values),
        diagnostics=_redact(to_plain_data(result.diagnostics), sensitive_values),
    )


def _event_data(event: LifecycleEvent) -> Mapping[str, Any]:
    return {
        "name": event.name,
        "sequence": event.sequence,
        "timestamp": event.timestamp,
        "source": event.source,
        "payload": to_plain_data(event.payload),
    }


def _component_error(
    spec: Union[ComponentSpec, BoundComponentSpec],
    summary: str,
    exc: Optional[BaseException] = None,
    *,
    phase: str = "component",
) -> ComponentResult:
    diagnostics: Mapping[str, Any] = {}
    if exc is not None:
        diagnostics = {"exception": _exception_data(exc, phase=phase)}
    return ComponentResult(
        component_instance_id=spec.instance_id,
        status=ComponentStatus.ERROR,
        summary=summary,
        diagnostics=diagnostics,
    )


def _blocked_result(
    spec: Union[ComponentSpec, BoundComponentSpec],
    reasons: Sequence[str],
) -> ComponentResult:
    return ComponentResult(
        component_instance_id=spec.instance_id,
        status=ComponentStatus.SKIP,
        summary=f"blocked: {', '.join(reasons)}",
        diagnostics={"blocked_by": tuple(reasons)},
    )


def _validate_phase_dependencies(
    ordered: Sequence[Union[ComponentSpec, BoundComponentSpec]],
) -> None:
    by_id = {component.instance_id: component for component in ordered}
    for component in ordered:
        for dependency_id in component.depends_on:
            dependency = by_id[dependency_id]
            if component.kind is ComponentKind.CAPABILITY and dependency.kind is not ComponentKind.CAPABILITY:
                raise ValueError(
                    f"capability {component.instance_id!r} cannot depend on post-provision component {dependency_id!r}"
                )
            if component.kind in _START_METHODS and dependency.kind is ComponentKind.ORACLE:
                raise ValueError(
                    f"component {component.instance_id!r} cannot depend on post-execution oracle {dependency_id!r}"
                )


def _effective_required_instances(plan: Union[RunPlan, BoundRunPlan]) -> frozenset[str]:
    by_id = {component.instance_id: component for component in plan.components}
    required = {component.instance_id for component in plan.components if component.required}
    required.update(requirement.source for requirement in plan.required_evidence if requirement.source is not None)
    pending = list(required)
    while pending:
        instance_id = pending.pop()
        for dependency in by_id[instance_id].depends_on:
            if dependency not in required:
                required.add(dependency)
                pending.append(dependency)
    return frozenset(required)


def _validate_feature_mode(plan: Union[RunPlan, BoundRunPlan]) -> None:
    if plan.mode is not RunMode.FEATURE:
        return
    if not any(component.kind is ComponentKind.ORACLE and component.required for component in plan.components):
        raise FeatureRunnerError("feature mode requires at least one required oracle")
    if not plan.required_evidence:
        raise FeatureRunnerError("feature mode requires at least one required_evidence requirement")


def _missing_evidence_requirements(
    requirements: Sequence[EvidenceRequirement],
    evidence: Sequence[Evidence],
) -> tuple[Mapping[str, Any], ...]:
    missing = []
    for requirement in requirements:
        count = sum(
            item.evidence_id == requirement.evidence_id
            and (requirement.source is None or item.source == requirement.source)
            for item in evidence
        )
        if count < requirement.min_count:
            missing.append(
                {
                    "evidence_id": requirement.evidence_id,
                    "source": requirement.source,
                    "min_count": requirement.min_count,
                    "observed_count": count,
                }
            )
    return tuple(missing)


def _contains_environment_reference(value: Any) -> bool:
    if isinstance(value, EnvironmentReference):
        return True
    if isinstance(value, Mapping):
        return any(_contains_environment_reference(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_environment_reference(item) for item in value)
    return False


def _plan_contains_environment_reference(plan: Union[RunPlan, BoundRunPlan]) -> bool:
    if isinstance(plan, BoundRunPlan):
        return False
    values = [
        plan.inputs,
        plan.topology_document,
        *(component.config for component in plan.components),
        *(spec.config for spec in plan.artifacts),
        *(spec.config for spec in plan.resources),
        *(spec.config for spec in plan.policies),
        *(overlay.selector.labels for overlay in plan.startup_overlays),
        *(overlay.server_config for overlay in plan.startup_overlays),
    ]
    return any(_contains_environment_reference(value) for value in values)


def _topology_contains_environment_reference(plan: RunPlan) -> bool:
    values = [
        plan.topology_document,
        *(overlay.selector.labels for overlay in plan.startup_overlays),
        *(overlay.server_config for overlay in plan.startup_overlays),
    ]
    return any(_contains_environment_reference(value) for value in values)


def normalize_artifact_root(artifact_dir: str) -> str:
    if not isinstance(artifact_dir, str):
        raise TypeError("artifact_dir must be a string")
    if "\x00" in artifact_dir:
        raise ValueError("artifact_dir must not contain NUL")
    if artifact_dir:
        root = os.path.abspath(artifact_dir)
    else:
        root = os.path.join(
            os.path.abspath(tempfile.gettempdir()),
            "valkey-fuzzer",
            "artifacts",
        )
    root = os.path.normpath(root)
    if root == os.path.abspath(os.sep):
        raise ValueError("artifact_dir must not resolve to the filesystem root")
    return root


def _environment_report(error: BaseException) -> Optional[EnvironmentReport]:
    report = getattr(error, "environment_report", None)
    if report is None:
        report = getattr(error, "report", None)
    return report if isinstance(report, EnvironmentReport) else None


def _environment_diagnostics(
    report: Optional[EnvironmentReport],
    *,
    error: Optional[BaseException] = None,
) -> Mapping[str, Any]:
    diagnostics: dict[str, Any] = {}
    if report is not None:
        diagnostics["environment_report"] = report.to_data()
    if error is not None:
        diagnostics["environment_error"] = {
            "type": (
                type(error).__qualname__
                if type(error).__module__ == "builtins"
                else f"{type(error).__module__}.{type(error).__qualname__}"
            )
        }
    return diagnostics


def _default_run_id() -> str:
    return uuid.uuid4().hex


class FeatureRunner:
    """Execute immutable feature-pack plans through registered generic roles."""

    def __init__(
        self,
        registries: Mapping[ComponentKind, Registry[Any]],
        topology_providers: TopologyProviderSource,
        *,
        artifact_dir: str = "",
        environment_manager: Optional[EnvironmentManager] = None,
        run_id_factory: Callable[[], str] = _default_run_id,
        clock: Callable[[], float] = time.monotonic,
    ):
        if not isinstance(registries, Mapping):
            raise TypeError("registries must be a mapping")
        normalized_registries = {}
        for raw_kind, registry in registries.items():
            kind = ComponentKind(raw_kind)
            if kind in normalized_registries:
                raise ValueError(f"duplicate registry for component kind {kind.value!r}")
            if not isinstance(registry, Registry):
                raise TypeError(f"registry for {kind.value!r} must be a Registry")
            if registry.kind is not kind:
                raise ValueError(f"registry key {kind.value!r} does not match registry kind {registry.kind.value!r}")
            normalized_registries[kind] = registry
        if isinstance(topology_providers, TopologyProviderRegistry):
            api_versions = {registry.api_version for registry in normalized_registries.values()}
            if len(api_versions) > 1:
                raise ValueError("component registries must use one extension API version")
            if api_versions and topology_providers.api_version not in api_versions:
                raise ValueError("topology provider registry API must match the component registry API")
            topology_provider_registry = TopologyProviderRegistry(api_version=topology_providers.api_version)
            topology_provider_registry.register_many(topology_providers.snapshot().values())
        elif isinstance(topology_providers, Mapping):
            component_api_versions = {registry.api_version for registry in normalized_registries.values()}
            if len(component_api_versions) > 1:
                raise ValueError("component registries must use one extension API version")
            registrations = []
            for topology, registration in topology_providers.items():
                if not isinstance(registration, TopologyProviderRegistration):
                    raise TypeError(
                        f"topology provider entry for {topology!r} must be a "
                        "TopologyProviderRegistration with explicit replay "
                        "identity metadata"
                    )
                if registration.topology != topology:
                    raise ValueError(
                        f"topology provider mapping key {topology!r} does not "
                        f"match registration topology {registration.topology!r}"
                    )
                registrations.append(registration)
            topology_api_versions = {registration.api_version for registration in registrations}
            if len(topology_api_versions) > 1:
                raise ValueError("topology provider registrations must use one extension API version")
            if component_api_versions and topology_api_versions and component_api_versions != topology_api_versions:
                component_api_version = next(iter(component_api_versions))
                topology_api_version = next(iter(topology_api_versions))
                raise ValueError(
                    "topology provider API "
                    f"{topology_api_version!r} must match the component registry "
                    f"API {component_api_version!r}"
                )
            api_version = next(
                iter(component_api_versions or topology_api_versions),
                EXTENSION_API_VERSION,
            )
            topology_provider_registry = TopologyProviderRegistry(api_version=api_version)
            topology_provider_registry.register_many(registrations)
        else:
            raise TypeError("topology_providers must be a mapping or TopologyProviderRegistry")
        normalized_artifact_dir = normalize_artifact_root(artifact_dir)
        if environment_manager is not None and not isinstance(environment_manager, EnvironmentManager):
            raise TypeError("environment_manager must be an EnvironmentManager or None")
        if not callable(run_id_factory):
            raise TypeError("run_id_factory must be callable")
        if not callable(clock):
            raise TypeError("clock must be callable")

        self._registries = MappingProxyType(normalized_registries)
        self._topology_provider_registry = topology_provider_registry
        self._artifact_dir = normalized_artifact_dir
        self._environment_manager = (
            EnvironmentManager(
                artifact_registry=ArtifactProviderRegistry(),
                resource_registry=ResourceProviderRegistry(),
                artifact_root=normalized_artifact_dir,
            )
            if environment_manager is None
            else environment_manager
        )
        self._run_id_factory = run_id_factory
        self._clock = clock

    @property
    def topology_provider_registry(self) -> TopologyProviderRegistry:
        return self._topology_provider_registry

    def _new_outcome(
        self,
        *,
        context: _RunnerContext,
        status: OutcomeStatus,
        summary: str,
        component_results: Sequence[ComponentResult],
        started_at: float,
        diagnostics: Optional[Mapping[str, Any]] = None,
    ) -> RunOutcome:
        combined_diagnostics = dict(diagnostics or {})
        combined_diagnostics["events"] = tuple(_event_data(event) for event in context.event_bus.attempted_history())
        combined_diagnostics["committed_events"] = tuple(
            _event_data(event) for event in context.event_bus.committed_history()
        )
        return RunOutcome(
            plan=context.source_plan,
            status=status,
            summary=summary,
            component_results=tuple(component_results),
            diagnostics=combined_diagnostics,
            started_at=started_at,
            ended_at=self._clock(),
        )

    def _validate_component_registration(
        self,
        plan: RunPlan,
        spec: ComponentSpec,
    ) -> _ValidatedComponent:
        registry = self._registries.get(spec.kind)
        if registry is None:
            raise FeatureRunnerError(f"no registry is configured for component kind {spec.kind.value!r}")
        try:
            registration = registry.resolve(spec.component_id)
        except Exception as exc:
            raise FeatureRunnerError(
                f"cannot resolve {spec.kind.value} component {spec.component_id!r}: {exc}"
            ) from exc
        if registration.kind is not spec.kind or registration.api_version != registry.api_version:
            raise FeatureRunnerError(
                f"registration {spec.component_id!r} is incompatible with "
                f"{spec.kind.value} registry API {registry.api_version!r}"
            )
        _require_signature(
            registration.factory,
            (),
            f"factory for component {spec.component_id!r}",
        )
        if registration.validator is not None:
            _require_signature(
                registration.validator,
                (spec, plan),
                f"validator for component {spec.component_id!r}",
            )
            if not _plan_contains_environment_reference(plan):
                try:
                    validation_return = registration.validator(spec, plan)
                except Exception as exc:
                    raise FeatureRunnerError(
                        f"component {spec.component_id!r} rejected instance {spec.instance_id!r}: {exc}"
                    ) from exc
                if validation_return is not None:
                    raise FeatureRunnerError(f"validator for component {spec.component_id!r} must return None")
        return _ValidatedComponent(
            spec=spec,
            registry=registry,
            registration=registration,
        )

    def _validate_component_registrations(
        self,
        plan: RunPlan,
        ordered: Sequence[ComponentSpec],
    ) -> tuple[_ValidatedComponent, ...]:
        validated = tuple(self._validate_component_registration(plan, spec) for spec in ordered)
        self._check_registry_drift(validated)
        return validated

    def _instantiate_component(
        self,
        item: _ValidatedComponent,
        context: _RunnerContext,
    ) -> _ResolvedComponent:
        self._check_registry_drift((item,))
        spec = item.spec
        registration = item.registration
        try:
            instance = registration.factory()
        except Exception as exc:
            raise FeatureRunnerError(f"factory for component {spec.component_id!r} failed: {exc}") from exc
        protocol = _PROTOCOLS[spec.kind]
        if isinstance(instance, type) or not isinstance(instance, protocol):
            raise FeatureRunnerError(
                f"factory for component {spec.component_id!r} returned "
                f"{type(instance).__name__}, which does not implement {protocol.__name__}"
            )
        for method_name, extra_argument_count in _COMPONENT_METHODS[spec.kind]:
            method = getattr(instance, method_name, None)
            args: tuple[Any, ...] = (context, spec)
            if extra_argument_count:
                args += ((),)
            _require_signature(
                method,
                args,
                f"{spec.kind.value} method {spec.component_id!r}.{method_name}",
            )
        resolved = _ResolvedComponent(
            spec=spec,
            registry=item.registry,
            registration=registration,
            instance=instance,
        )
        self._check_registry_drift((resolved,))
        return resolved

    def _preflight_components(
        self,
        plan: RunPlan,
        ordered: Sequence[ComponentSpec],
        context: _RunnerContext,
    ) -> tuple[_ResolvedComponent, ...]:
        resolved = []
        for spec in ordered:
            validated = self._validate_component_registration(plan, spec)
            resolved.append(
                self._instantiate_component(
                    validated,
                    context,
                )
            )
        self._check_registry_drift(resolved)
        return tuple(resolved)

    @staticmethod
    def _check_registry_drift(
        resolved: Sequence[Union[_ValidatedComponent, _ResolvedComponent]],
    ) -> None:
        for item in resolved:
            if item.registry.kind is not item.spec.kind or item.registry.api_version != item.registration.api_version:
                raise FeatureRunnerError(f"registry drift detected for component {item.spec.component_id!r}")
            try:
                current = item.registry.resolve(item.spec.component_id)
            except Exception as exc:
                raise FeatureRunnerError(f"registry drift removed component {item.spec.component_id!r}") from exc
            if current is not item.registration:
                raise FeatureRunnerError(f"registry drift replaced component {item.spec.component_id!r}")

    def _prepare_plan(
        self,
        plan: RunPlan,
        context: _RunnerContext,
        *,
        validate_topology: bool = True,
    ) -> _PreparedPlan:
        _validate_feature_mode(plan)
        ordered = stable_topological_order(plan.components)
        _validate_phase_dependencies(ordered)
        effective_required = _effective_required_instances(plan)
        try:
            registration = self._topology_provider_registry.resolve(plan.topology)
        except UnknownTopologyProviderError as exc:
            raise FeatureRunnerError(f"no topology provider is registered for {plan.topology!r}") from exc
        if plan.topology_provider != registration.identity:
            raise FeatureRunnerError(
                f"topology provider identity for {plan.topology!r} does not match the replay-locked run plan"
            )
        self._check_topology_registry_drift(registration)
        _require_signature(
            registration.factory,
            (),
            f"topology provider factory for {plan.topology!r}",
        )
        try:
            provider = registration.factory()
        except Exception as exc:
            raise FeatureRunnerError(f"topology provider factory for {plan.topology!r} failed: {exc}") from exc
        self._check_topology_registry_drift(registration)
        if isinstance(provider, type) or not isinstance(provider, TopologyProvider):
            raise FeatureRunnerError(
                f"topology provider factory for {plan.topology!r} returned "
                f"{type(provider).__name__}, which does not implement TopologyProvider"
            )
        _require_signature(
            provider.validate,
            (plan,),
            f"topology provider {plan.topology!r}.validate",
        )
        _require_signature(
            provider.provision,
            (context,),
            f"topology provider {plan.topology!r}.provision",
        )
        _require_signature(
            provider.rollback,
            (context,),
            f"topology provider {plan.topology!r}.rollback",
        )
        _require_signature(
            provider.cleanup,
            (context, ProvisionedTopology()),
            f"topology provider {plan.topology!r}.cleanup",
        )
        if validate_topology:
            validation_return = provider.validate(plan)
            if validation_return is not None:
                raise FeatureRunnerError("topology provider validation must return None")
        self._check_topology_registry_drift(registration)

        return _PreparedPlan(
            ordered=ordered,
            effective_required=effective_required,
            registration=registration,
            provider=provider,
        )

    def _check_topology_registry_drift(
        self,
        registration: TopologyProviderRegistration,
    ) -> None:
        if registration.api_version != self._topology_provider_registry.api_version:
            raise FeatureRunnerError(f"topology provider registry drift detected for {registration.topology!r}")
        try:
            current = self._topology_provider_registry.resolve(registration.topology)
        except UnknownTopologyProviderError as exc:
            raise FeatureRunnerError(f"topology provider registry drift removed {registration.topology!r}") from exc
        if current is not registration:
            raise FeatureRunnerError(f"topology provider registry drift replaced {registration.topology!r}")

    def validate(self, plan: RunPlan) -> None:
        """Preflight a plan without provisioning a topology or running components.

        Topology provider factories and signatures are resolved. Plan-specific
        topology validation and component validators are deferred only where
        typed environment references still require post-acquisition binding.
        Component factories are checked for zero-argument compatibility but
        are deliberately not invoked.
        """

        if not isinstance(plan, RunPlan):
            raise TypeError("plan must be a RunPlan")
        context = _RunnerContext(
            plan,
            plan,
            LifecycleEventBus(clock=self._clock),
            self._artifact_dir,
            None,
        )
        prepared = self._prepare_plan(
            plan,
            context,
            validate_topology=not _topology_contains_environment_reference(plan),
        )
        self._validate_component_registrations(plan, prepared.ordered)

    def _deadline_for(self, context: _RunnerContext, spec: ComponentSpec, started_at: float) -> Optional[float]:
        if spec.timeout_s is None:
            return context._run_deadline
        component_deadline = started_at + spec.timeout_s
        if context._run_deadline is None:
            return component_deadline
        return min(context._run_deadline, component_deadline)

    @staticmethod
    def _validate_result_evidence(
        result: ComponentResult,
        spec: ComponentSpec,
        event_bus: LifecycleEventBus,
    ) -> None:
        committed_sequences = {event.sequence for event in event_bus.committed_history()}
        for evidence in result.evidence:
            if evidence.source != spec.instance_id:
                raise FeatureRunnerError(
                    f"component {spec.instance_id!r} emitted evidence owned by {evidence.source!r}"
                )
            if evidence.event_sequence is not None and evidence.event_sequence not in committed_sequences:
                raise FeatureRunnerError(
                    f"evidence {evidence.evidence_id!r} from {spec.instance_id!r} "
                    f"references uncommitted event sequence {evidence.event_sequence}"
                )

    def _call_component(
        self,
        context: _RunnerContext,
        resolved: _ResolvedComponent,
        method_name: str,
        *args: Any,
    ) -> ComponentResult:
        spec = resolved.spec
        self._check_registry_drift((resolved,))
        started_at = self._clock()
        deadline = self._deadline_for(context, spec, started_at)
        if deadline is not None and started_at >= deadline:
            raise CooperativeDeadlineError(f"deadline expired before component {spec.instance_id!r} could run")
        previous_deadline = context.set_component_deadline(deadline)
        try:
            result = getattr(resolved.instance, method_name)(context, spec, *args)
        finally:
            context.set_component_deadline(previous_deadline)
        ended_at = self._clock()
        if deadline is not None and ended_at > deadline:
            raise CooperativeDeadlineError(f"component {spec.instance_id!r} exceeded its cooperative deadline")
        if not isinstance(result, ComponentResult):
            raise FeatureRunnerError(
                f"component {spec.instance_id!r} returned {type(result).__name__}, expected ComponentResult"
            )
        if result.component_instance_id != spec.instance_id:
            raise FeatureRunnerError(
                f"component {spec.instance_id!r} returned result for {result.component_instance_id!r}"
            )
        self._validate_result_evidence(result, spec, context.event_bus)
        self._check_registry_drift((resolved,))
        return ComponentResult(
            component_instance_id=result.component_instance_id,
            status=result.status,
            summary=result.summary,
            evidence=result.evidence,
            diagnostics=result.diagnostics,
            started_at=started_at if result.started_at is None else result.started_at,
            ended_at=ended_at if result.ended_at is None else result.ended_at,
        )

    def _call_cleanup_component(
        self,
        context: _RunnerContext,
        resolved: _ResolvedComponent,
        method_name: str,
    ) -> ComponentResult:
        """Invoke teardown even after deadline expiry or registry drift."""

        spec = resolved.spec
        started_at = self._clock()
        context.enter_cleanup()
        try:
            result = getattr(resolved.instance, method_name)(context, spec)
        finally:
            context.exit_cleanup()
        ended_at = self._clock()
        if not isinstance(result, ComponentResult):
            raise FeatureRunnerError(
                f"cleanup for component {spec.instance_id!r} returned {type(result).__name__}, expected ComponentResult"
            )
        if result.component_instance_id != spec.instance_id:
            raise FeatureRunnerError(
                f"cleanup for component {spec.instance_id!r} returned result for {result.component_instance_id!r}"
            )
        self._validate_result_evidence(result, spec, context.event_bus)
        return ComponentResult(
            component_instance_id=result.component_instance_id,
            status=result.status,
            summary=result.summary,
            evidence=result.evidence,
            diagnostics=result.diagnostics,
            started_at=started_at if result.started_at is None else result.started_at,
            ended_at=ended_at if result.ended_at is None else result.ended_at,
        )

    @staticmethod
    def _dependency_reasons(
        spec: ComponentSpec,
        execution_results: Mapping[str, ComponentResult],
    ) -> tuple[str, ...]:
        reasons = []
        for dependency in spec.depends_on:
            result = execution_results.get(dependency)
            if result is None:
                reasons.append(f"dependency {dependency!r} has no result")
            elif result.status is not ComponentStatus.PASS:
                reasons.append(f"dependency {dependency!r} is {result.status.value}")
        return tuple(reasons)

    @staticmethod
    def _publish_result_event(
        event_bus: LifecycleEventBus,
        spec: ComponentSpec,
        success_event: str,
        failure_event: str,
        result: ComponentResult,
    ) -> None:
        event_bus.publish(
            success_event if result.status is ComponentStatus.PASS else failure_event,
            source=spec.instance_id,
            payload={"status": result.status.value, "summary": result.summary},
        )

    @staticmethod
    def _result_sequence(
        ordered: Sequence[ComponentSpec],
        execution_results: Mapping[str, ComponentResult],
    ) -> tuple[ComponentResult, ...]:
        return tuple(execution_results[spec.instance_id] for spec in ordered if spec.instance_id in execution_results)

    @staticmethod
    def _merge_cleanup_result(
        original: ComponentResult,
        cleanup: ComponentResult,
    ) -> ComponentResult:
        diagnostics = dict(to_plain_data(original.diagnostics))
        diagnostics["teardown"] = {
            "status": cleanup.status.value,
            "summary": cleanup.summary,
            "diagnostics": to_plain_data(cleanup.diagnostics),
        }
        return ComponentResult(
            component_instance_id=original.component_instance_id,
            status=original.status,
            summary=original.summary,
            evidence=original.evidence + cleanup.evidence,
            diagnostics=diagnostics,
            started_at=original.started_at,
            ended_at=cleanup.ended_at,
        )

    @staticmethod
    def _call_provider_release(
        context: _RunnerContext,
        method: Callable[..., Any],
        *args: Any,
    ) -> None:
        context.enter_cleanup()
        try:
            result = method(context, *args)
        finally:
            context.exit_cleanup()
        if result is not None:
            raise FeatureRunnerError("topology provider release methods must return None")

    def _rollback_failed_provision(
        self,
        context: _RunnerContext,
        event_bus: LifecycleEventBus,
        provider: TopologyProvider,
        cleanup_errors: list[Mapping[str, Any]],
    ) -> Optional[BaseException]:
        first_base_exception: Optional[BaseException] = None
        try:
            event_bus.publish("topology.rollback.started", source="runner")
        except BaseException as exc:
            cleanup_errors.append(
                {
                    "owner": "topology",
                    "phase": "rollback event",
                    "exception": _exception_data(exc),
                }
            )
            if not isinstance(exc, Exception):
                first_base_exception = exc
        try:
            self._call_provider_release(context, provider.rollback)
        except BaseException as exc:
            cleanup_errors.append(
                {
                    "owner": "topology",
                    "phase": "rollback",
                    "exception": _exception_data(exc),
                }
            )
            if not isinstance(exc, Exception) and first_base_exception is None:
                first_base_exception = exc
        else:
            try:
                event_bus.publish("topology.rollback.completed", source="runner")
            except BaseException as exc:
                cleanup_errors.append(
                    {
                        "owner": "topology",
                        "phase": "rollback result event",
                        "exception": _exception_data(exc),
                    }
                )
                if not isinstance(exc, Exception) and first_base_exception is None:
                    first_base_exception = exc
        return first_base_exception

    def _finish_without_topology(
        self,
        *,
        context: _RunnerContext,
        status: OutcomeStatus,
        summary: str,
        component_results: Sequence[ComponentResult],
        started_at: float,
        diagnostics: Optional[Mapping[str, Any]] = None,
    ) -> RunOutcome:
        final_diagnostics: dict[str, Any] = dict(diagnostics or {})
        primary_base_exception: Optional[BaseException] = None
        try:
            context.event_bus.publish("run.completed", source="runner")
        except BaseException as exc:
            if isinstance(exc, Exception):
                status = OutcomeStatus.ERROR
                summary = "event journal finalization failed"
                final_diagnostics["error"] = _exception_data(
                    exc,
                    phase="run.completed",
                )
            else:
                primary_base_exception = exc
        try:
            context.event_bus.quiesce()
        except BaseException as exc:
            if isinstance(exc, Exception):
                status = OutcomeStatus.ERROR
                summary = "event journal quiesce failed"
                final_diagnostics["error"] = _exception_data(
                    exc,
                    phase="event_bus.quiesce",
                )
            elif primary_base_exception is None:
                primary_base_exception = exc
        if primary_base_exception is not None:
            raise primary_base_exception
        return self._new_outcome(
            context=context,
            status=status,
            summary=summary,
            component_results=component_results,
            started_at=started_at,
            diagnostics=final_diagnostics,
        )

    def _execute_bound(
        self,
        plan: BoundRunPlan,
        session: EnvironmentSession,
        context: _RunnerContext,
        static_prepared: _PreparedPlan,
        *,
        validate_bound_topology: bool,
        started_at: float,
        deadline: Optional[float],
    ) -> _ExecutionResult:
        event_bus = context.event_bus
        execution_results: dict[str, ComponentResult] = {}
        evidence_ledger: list[Evidence] = []
        invoked_results: set[str] = set()
        cleanup_errors: list[Mapping[str, Any]] = []
        error: Optional[BaseException] = None
        error_phase = "preflight"
        primary_base_exception: Optional[BaseException] = None
        terminal: Optional[_EarlyTerminalError] = None
        topology: Optional[ProvisionedTopology] = None
        provider: Optional[TopologyProvider] = None
        topology_registration: Optional[TopologyProviderRegistration] = None
        cleanups: list[_CleanupEntry] = []
        current_spec: Optional[Union[ComponentSpec, BoundComponentSpec]] = None
        effective_required: frozenset[str] = frozenset()
        ordered = tuple(plan.components)
        resolved: tuple[_ResolvedComponent, ...] = ()

        try:
            _validate_feature_mode(plan)
            ordered = stable_topological_order(plan.components)
            _validate_phase_dependencies(ordered)
            effective_required = _effective_required_instances(plan)
            prepared = _PreparedPlan(
                ordered=ordered,
                effective_required=effective_required,
                registration=static_prepared.registration,
                provider=static_prepared.provider,
            )
            self._check_topology_registry_drift(prepared.registration)
            if validate_bound_topology:
                validation_return = prepared.provider.validate(plan)
                if validation_return is not None:
                    raise FeatureRunnerError("topology provider validation must return None")
                self._check_topology_registry_drift(prepared.registration)
            ordered = prepared.ordered
            effective_required = prepared.effective_required
            topology_registration = prepared.registration
            provider = prepared.provider
            resolved = self._preflight_components(plan, ordered, context)
            by_instance = {item.spec.instance_id: item for item in resolved}

            for spec in ordered:
                if spec.kind is not ComponentKind.CAPABILITY:
                    continue
                current_spec = spec
                error_phase = "capability.probe"
                item = by_instance[spec.instance_id]
                reasons = self._dependency_reasons(spec, execution_results)
                if reasons:
                    execution_results[spec.instance_id] = _blocked_result(spec, reasons)
                    continue
                event_bus.publish("capability.probing", source=spec.instance_id)
                result = self._call_component(context, item, "probe")
                execution_results[spec.instance_id] = result
                invoked_results.add(spec.instance_id)
                evidence_ledger.extend(result.evidence)
                event_bus.publish(
                    "capability.completed",
                    source=spec.instance_id,
                    payload={"status": result.status.value, "summary": result.summary},
                )
                self._check_registry_drift(resolved)
                if result.status is ComponentStatus.ERROR:
                    raise FeatureRunnerError(f"capability {spec.instance_id!r} returned ERROR: {result.summary}")
                if spec.instance_id in effective_required and result.status is ComponentStatus.SKIP:
                    raise _EarlyTerminalError(
                        OutcomeStatus.SKIP_UNSUPPORTED,
                        f"required capability {spec.instance_id!r} is unsupported",
                    )
                if spec.instance_id in effective_required and result.status is ComponentStatus.FAIL:
                    raise _EarlyTerminalError(
                        OutcomeStatus.ERROR,
                        f"required capability {spec.instance_id!r} failed; a capability probe establishes "
                        "support, not feature behavior, so no trustworthy verdict is possible",
                    )
            current_spec = None

            blocked_required_capabilities = tuple(
                spec.instance_id
                for spec in ordered
                if spec.kind is ComponentKind.CAPABILITY
                and spec.instance_id in effective_required
                and execution_results[spec.instance_id].status is not ComponentStatus.PASS
            )
            if blocked_required_capabilities:
                blocked_statuses = {
                    execution_results[instance_id].status for instance_id in blocked_required_capabilities
                }
                outcome_status = (
                    OutcomeStatus.SKIP_UNSUPPORTED if ComponentStatus.SKIP in blocked_statuses else OutcomeStatus.ERROR
                )
                raise _EarlyTerminalError(
                    outcome_status,
                    f"required capabilities were blocked: {', '.join(blocked_required_capabilities)}",
                )

            error_phase = "registry"
            self._check_registry_drift(resolved)
            self._check_topology_registry_drift(topology_registration)
            error_phase = "topology.provision"
            now = self._clock()
            if deadline is not None and now >= deadline:
                raise CooperativeDeadlineError("deadline expired before topology provision")
            event_bus.publish("topology.provisioning", source="runner", payload={"topology": plan.topology})
            try:
                provisioned = provider.provision(context)
            except BaseException as provision_error:
                rollback_base_exception = self._rollback_failed_provision(
                    context,
                    event_bus,
                    provider,
                    cleanup_errors,
                )
                if rollback_base_exception is not None and isinstance(provision_error, Exception):
                    raise rollback_base_exception from provision_error
                raise
            if not isinstance(provisioned, ProvisionedTopology):
                invalid_provision = FeatureRunnerError(
                    f"topology provider {plan.topology!r} returned "
                    f"{type(provisioned).__name__}, expected ProvisionedTopology"
                )
                rollback_base_exception = self._rollback_failed_provision(
                    context,
                    event_bus,
                    provider,
                    cleanup_errors,
                )
                if rollback_base_exception is not None:
                    raise rollback_base_exception
                raise invalid_provision
            topology = provisioned
            self._check_topology_registry_drift(topology_registration)
            topology_bound_plan = BoundRunPlan.materialize(
                context.source_plan,
                session.binding,
                topology_services=topology.services,
            )
            context.bind_topology(topology, topology_bound_plan)
            if deadline is not None and self._clock() >= deadline:
                raise CooperativeDeadlineError("topology provision exceeded the run deadline")
            event_bus.publish("topology.ready", source="runner", payload={"topology": plan.topology})

            for spec in ordered:
                if spec.kind not in _START_METHODS:
                    continue
                current_spec = spec
                item = by_instance[spec.instance_id]
                reasons = self._dependency_reasons(spec, execution_results)
                if reasons:
                    execution_results[spec.instance_id] = _blocked_result(spec, reasons)
                    continue
                method_name, before_event, success_event, failure_event = _START_METHODS[spec.kind]
                error_phase = f"{spec.kind.value}.{method_name}"
                if spec.kind in _CLEANUP_METHODS:
                    cleanups.append(_CleanupEntry(item, _CLEANUP_METHODS[spec.kind][0]))
                event_bus.publish(before_event, source=spec.instance_id)
                result = self._call_component(context, item, method_name)
                execution_results[spec.instance_id] = result
                invoked_results.add(spec.instance_id)
                evidence_ledger.extend(result.evidence)
                self._publish_result_event(
                    event_bus,
                    spec,
                    success_event,
                    failure_event,
                    result,
                )
                if result.status is ComponentStatus.ERROR:
                    raise FeatureRunnerError(f"{spec.kind.value} {spec.instance_id!r} returned ERROR: {result.summary}")
            current_spec = None

        except _EarlyTerminalError as early_terminal:
            terminal = early_terminal
        except Exception as exc:
            error = exc
            if current_spec is not None and current_spec.instance_id not in execution_results:
                execution_results[current_spec.instance_id] = _component_error(
                    current_spec,
                    "component execution failed",
                    exc,
                    phase=error_phase,
                )
        except BaseException as exc:
            primary_base_exception = exc

        for cleanup_entry in reversed(cleanups):
            spec = cleanup_entry.resolved.spec
            method_name, before_event, success_event, failure_event = _CLEANUP_METHODS[spec.kind]
            try:
                event_bus.publish(before_event, source=spec.instance_id)
            except BaseException as exc:
                cleanup_errors.append(
                    {
                        "owner": spec.instance_id,
                        "phase": "cleanup event",
                        "exception": _exception_data(exc),
                    }
                )
                if not isinstance(exc, Exception) and primary_base_exception is None:
                    primary_base_exception = exc
            try:
                cleanup_result = self._call_cleanup_component(
                    context,
                    cleanup_entry.resolved,
                    method_name,
                )
                evidence_ledger.extend(cleanup_result.evidence)
                original = execution_results.get(spec.instance_id)
                if original is not None:
                    execution_results[spec.instance_id] = self._merge_cleanup_result(
                        original,
                        cleanup_result,
                    )
                try:
                    self._publish_result_event(
                        event_bus,
                        spec,
                        success_event,
                        failure_event,
                        cleanup_result,
                    )
                except BaseException as exc:
                    cleanup_errors.append(
                        {
                            "owner": spec.instance_id,
                            "phase": "cleanup result event",
                            "exception": _exception_data(exc),
                        }
                    )
                    if not isinstance(exc, Exception) and primary_base_exception is None:
                        primary_base_exception = exc
                if cleanup_result.status is not ComponentStatus.PASS:
                    cleanup_errors.append(
                        {
                            "owner": spec.instance_id,
                            "phase": method_name,
                            "status": cleanup_result.status.value,
                            "summary": cleanup_result.summary,
                        }
                    )
            except BaseException as exc:
                cleanup_errors.append(
                    {
                        "owner": spec.instance_id,
                        "phase": method_name,
                        "exception": _exception_data(exc),
                    }
                )
                if not isinstance(exc, Exception) and primary_base_exception is None:
                    primary_base_exception = exc

        if terminal is None and error is None and primary_base_exception is None:
            try:
                resolved_by_instance = {item.spec.instance_id: item for item in resolved}
                for spec in ordered:
                    if spec.kind is not ComponentKind.ORACLE:
                        continue
                    current_spec = spec
                    error_phase = "oracle.evaluate"
                    reasons = self._dependency_reasons(spec, execution_results)
                    if reasons:
                        execution_results[spec.instance_id] = _blocked_result(spec, reasons)
                        continue
                    event_bus.publish("oracle.started", source=spec.instance_id)
                    result = self._call_component(
                        context,
                        resolved_by_instance[spec.instance_id],
                        "evaluate",
                        tuple(evidence_ledger),
                    )
                    execution_results[spec.instance_id] = result
                    invoked_results.add(spec.instance_id)
                    evidence_ledger.extend(result.evidence)
                    event_bus.publish(
                        "oracle.completed",
                        source=spec.instance_id,
                        payload={"status": result.status.value, "summary": result.summary},
                    )
                    if result.status is ComponentStatus.ERROR:
                        raise FeatureRunnerError(f"oracle {spec.instance_id!r} returned ERROR: {result.summary}")
                current_spec = None
                error_phase = "validation"
                event_bus.publish("validation.completed", source="runner")
                self._check_registry_drift(resolved)
            except Exception as exc:
                error = exc
                if current_spec is not None and current_spec.instance_id not in execution_results:
                    execution_results[current_spec.instance_id] = _component_error(
                        current_spec,
                        "component execution failed",
                        exc,
                        phase=error_phase,
                    )
            except BaseException as exc:
                primary_base_exception = exc

        if topology is not None and provider is not None:
            try:
                event_bus.publish("topology.cleanup.started", source="runner")
            except BaseException as exc:
                cleanup_errors.append(
                    {
                        "owner": "topology",
                        "phase": "cleanup event",
                        "exception": _exception_data(exc),
                    }
                )
                if not isinstance(exc, Exception) and primary_base_exception is None:
                    primary_base_exception = exc
            try:
                self._call_provider_release(
                    context,
                    provider.cleanup,
                    topology,
                )
            except BaseException as exc:
                cleanup_errors.append(
                    {
                        "owner": "topology",
                        "phase": "cleanup",
                        "exception": _exception_data(exc),
                    }
                )
                if not isinstance(exc, Exception) and primary_base_exception is None:
                    primary_base_exception = exc
            else:
                try:
                    event_bus.publish("topology.cleanup.completed", source="runner")
                except BaseException as exc:
                    cleanup_errors.append(
                        {
                            "owner": "topology",
                            "phase": "cleanup result event",
                            "exception": _exception_data(exc),
                        }
                    )
                    if not isinstance(exc, Exception) and primary_base_exception is None:
                        primary_base_exception = exc
            finally:
                context.clear_topology(plan)

        try:
            event_bus.publish("run.completed", source="runner")
        except BaseException as exc:
            if isinstance(exc, Exception):
                if error is None:
                    error = exc
                    error_phase = "run.completed"
            elif primary_base_exception is None:
                primary_base_exception = exc

        try:
            event_bus.quiesce()
        except BaseException as exc:
            if isinstance(exc, Exception):
                if error is None:
                    error = exc
                    error_phase = "event_bus.quiesce"
            elif primary_base_exception is None:
                primary_base_exception = exc

        if primary_base_exception is None and resolved:
            try:
                self._check_registry_drift(resolved)
            except Exception as exc:
                if error is None:
                    error = exc
                    error_phase = "registry"
        if primary_base_exception is None and topology_registration is not None:
            try:
                self._check_topology_registry_drift(topology_registration)
            except Exception as exc:
                if error is None:
                    error = exc
                    error_phase = "topology registry"

        if primary_base_exception is not None:
            raise primary_base_exception

        component_results = self._result_sequence(ordered, execution_results)
        required_failures = tuple(
            spec.instance_id
            for spec in ordered
            if spec.instance_id in effective_required
            and spec.instance_id in execution_results
            and execution_results[spec.instance_id].status in (ComponentStatus.FAIL, ComponentStatus.SKIP)
        )
        # Root causes are required components that ran and did not pass; blocked
        # components are consequences of those. Only a FAIL from a verdict role
        # is evidence about the system under test.
        verdict_failures = tuple(
            spec.instance_id
            for spec in ordered
            if spec.instance_id in required_failures
            and spec.instance_id in invoked_results
            and spec.kind in _VERDICT_KINDS
            and execution_results[spec.instance_id].status is ComponentStatus.FAIL
        )
        untrusted_failures = tuple(
            spec.instance_id
            for spec in ordered
            if spec.instance_id in required_failures
            and spec.instance_id in invoked_results
            and spec.instance_id not in verdict_failures
        )
        missing_events = tuple(
            event_name for event_name in plan.required_events if not event_bus.committed_history(event_name)
        )
        missing_evidence = _missing_evidence_requirements(
            plan.required_evidence,
            evidence_ledger,
        )
        if terminal is not None:
            trustworthy_feature_failure = terminal.trustworthy_feature_failure
        else:
            trustworthy_feature_failure = bool(verdict_failures) and not untrusted_failures

        diagnostics: dict[str, Any] = {}
        if cleanup_errors:
            diagnostics["cleanup_errors"] = tuple(cleanup_errors)
        if terminal is None:
            if missing_events:
                diagnostics["missing_required_events"] = missing_events
            if missing_evidence:
                diagnostics["missing_required_evidence"] = missing_evidence
            if required_failures:
                diagnostics["required_component_failures"] = required_failures
            if untrusted_failures:
                diagnostics["untrusted_component_failures"] = untrusted_failures
        if error is not None:
            diagnostics["error"] = _exception_data(error, phase=error_phase)

        if error is not None:
            status = OutcomeStatus.ERROR
            summary = f"runner execution failed during {error_phase}"
        elif terminal is not None and terminal.status is OutcomeStatus.ERROR:
            status = OutcomeStatus.ERROR
            summary = terminal.summary
        elif terminal is None and untrusted_failures:
            status = OutcomeStatus.ERROR
            summary = (
                "required non-verdict components did not pass, so no trustworthy verdict is possible: "
                f"{', '.join(untrusted_failures)}"
            )
        elif cleanup_errors and not trustworthy_feature_failure:
            status = OutcomeStatus.ERROR
            summary = "cleanup failed before a trustworthy feature failure was established"
        elif terminal is not None:
            status = terminal.status
            summary = terminal.summary
        elif required_failures or missing_events or missing_evidence:
            status = OutcomeStatus.FAIL
            reasons = []
            if required_failures:
                reasons.append(f"required components did not pass: {', '.join(required_failures)}")
            if missing_events:
                reasons.append(f"required events were not observed: {', '.join(missing_events)}")
            if missing_evidence:
                reasons.append("required evidence was not observed")
            summary = "; ".join(reasons)
        elif cleanup_errors:
            status = OutcomeStatus.ERROR
            summary = "cleanup failed after execution"
        else:
            status = OutcomeStatus.PASS
            summary = "feature run passed"

        return _ExecutionResult(
            outcome=self._new_outcome(
                context=context,
                status=status,
                summary=summary,
                component_results=component_results,
                started_at=started_at,
                diagnostics=diagnostics,
            ),
            trustworthy_feature_failure=trustworthy_feature_failure,
        )

    def _with_environment_report(
        self,
        execution: _ExecutionResult,
        report: EnvironmentReport,
        *,
        teardown_error: Optional[BaseException] = None,
        sensitive_values: Sequence[str] = (),
    ) -> RunOutcome:
        outcome = execution.outcome
        diagnostics = dict(to_plain_data(outcome.diagnostics))
        diagnostics.update(_environment_diagnostics(report, error=teardown_error))

        status = outcome.status
        summary = outcome.summary
        teardown_failed = teardown_error is not None or report.status is not EnvironmentReportStatus.CLEANED
        if teardown_failed and status in (OutcomeStatus.PASS, OutcomeStatus.SKIP_UNSUPPORTED):
            status = OutcomeStatus.ERROR
            summary = "environment cleanup failed after execution"
        elif teardown_failed and status is OutcomeStatus.FAIL and not execution.trustworthy_feature_failure:
            status = OutcomeStatus.ERROR
            summary = "environment cleanup failed before a trustworthy feature failure was established"

        # Exception messages are kept for debugging, but resolved environment
        # outputs (credentials, endpoints, tokens) are scrubbed from them.
        return RunOutcome(
            plan=outcome.plan,
            status=status,
            summary=_redact(summary, sensitive_values),
            component_results=tuple(
                _redact_component_result(result, sensitive_values) for result in outcome.component_results
            ),
            evidence=outcome.evidence,
            diagnostics=_redact(diagnostics, sensitive_values),
            started_at=outcome.started_at,
            ended_at=self._clock(),
        )

    def run(
        self,
        plan: RunPlan,
        *,
        deadline: Optional[float] = None,
        _raise_preflight_errors: bool = False,
    ) -> RunOutcome:
        """Acquire, bind, execute, and fully tear down one declarative plan.

        Deadlines are absolute values from the configured monotonic clock.
        Enforcement is cooperative: the current deadline is exposed through
        ``RunContext.deadline`` and checked before and after synchronous calls.
        No worker threads are abandoned to implement timeouts.
        """

        if not isinstance(plan, RunPlan):
            raise TypeError("plan must be a RunPlan")
        if not isinstance(_raise_preflight_errors, bool):
            raise TypeError("_raise_preflight_errors must be a boolean")
        if deadline is not None and (
            isinstance(deadline, bool) or not isinstance(deadline, (int, float)) or not math.isfinite(deadline)
        ):
            raise TypeError("deadline must be a finite number or None")

        started_at = self._clock()
        event_bus = LifecycleEventBus(clock=self._clock)
        context = _RunnerContext(
            plan,
            plan,
            event_bus,
            self._artifact_dir,
            deadline,
        )
        event_bus.publish("run.started", source="runner", payload={"scenario_id": plan.scenario_id})

        validate_bound_topology = _topology_contains_environment_reference(plan)
        try:
            static_prepared = self._prepare_plan(
                plan,
                context,
                validate_topology=not validate_bound_topology,
            )
            self._validate_component_registrations(
                plan,
                static_prepared.ordered,
            )
        except Exception as exc:
            if _raise_preflight_errors:
                raise
            return self._finish_without_topology(
                context=context,
                status=OutcomeStatus.ERROR,
                summary="static runner preflight failed",
                component_results=(),
                started_at=started_at,
                diagnostics={"error": _exception_data(exc, phase="static preflight")},
            )

        try:
            run_id = self._run_id_factory()
            session = self._environment_manager.acquire(
                plan.artifacts,
                plan.resources,
                run_id=run_id,
                seed=plan.seed,
                deadline=deadline,
                policies=plan.policies,
            )
        except UnsupportedEnvironmentError as exc:
            return self._finish_without_topology(
                context=context,
                status=OutcomeStatus.SKIP_UNSUPPORTED,
                summary="the requested environment is unsupported",
                component_results=(),
                started_at=started_at,
                diagnostics=_environment_diagnostics(_environment_report(exc), error=exc),
            )
        except Exception as exc:
            return self._finish_without_topology(
                context=context,
                status=OutcomeStatus.ERROR,
                summary="environment acquisition failed",
                component_results=(),
                started_at=started_at,
                diagnostics=_environment_diagnostics(_environment_report(exc), error=exc),
            )

        execution: Optional[_ExecutionResult] = None
        sensitive_values: tuple[str, ...] = ()
        try:
            try:
                sensitive_values = _sensitive_values(session.binding)
                bound_plan = BoundRunPlan.materialize(plan, session.binding)
                context.bind_plan(bound_plan)
                execution = self._execute_bound(
                    bound_plan,
                    session,
                    context,
                    static_prepared,
                    validate_bound_topology=validate_bound_topology,
                    started_at=started_at,
                    deadline=deadline,
                )
            except Exception as exc:
                execution = _ExecutionResult(
                    self._finish_without_topology(
                        context=context,
                        status=OutcomeStatus.ERROR,
                        summary="environment binding or runner setup failed",
                        component_results=(),
                        started_at=started_at,
                        diagnostics={"error": _exception_data(exc, phase="environment binding")},
                    )
                )
        except BaseException as primary_error:
            try:
                session.__exit__(
                    type(primary_error),
                    primary_error,
                    primary_error.__traceback__,
                )
            except BaseException:
                pass
            raise

        assert execution is not None
        try:
            report = session.close()
        except EnvironmentLifecycleError as teardown_error:
            report = _environment_report(teardown_error)
            if report is None:
                report = session.report
            return self._with_environment_report(
                execution,
                report,
                teardown_error=teardown_error,
                sensitive_values=sensitive_values,
            )
        return self._with_environment_report(execution, report, sensitive_values=sensitive_values)


def run_feature_plan(
    plan: RunPlan,
    registries: Mapping[ComponentKind, Registry[Any]],
    topology_providers: TopologyProviderSource,
    *,
    artifact_dir: str = "",
    environment_manager: Optional[EnvironmentManager] = None,
    run_id_factory: Callable[[], str] = _default_run_id,
    deadline: Optional[float] = None,
) -> RunOutcome:
    """Functional convenience wrapper around :class:`FeatureRunner`."""

    return FeatureRunner(
        registries,
        topology_providers,
        artifact_dir=artifact_dir,
        environment_manager=environment_manager,
        run_id_factory=run_id_factory,
    ).run(plan, deadline=deadline)


__all__ = [
    "CooperativeDeadlineError",
    "FeatureRunner",
    "FeatureRunnerError",
    "ProvisionedTopology",
    "TopologyProvider",
    "TopologyProviderEntry",
    "TopologyProviderFactory",
    "TopologyProviderSource",
    "normalize_artifact_root",
    "run_feature_plan",
    "stable_topological_order",
]
