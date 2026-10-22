"""Generic, topology-neutral executor for compiled feature-pack run plans.

This module is intentionally isolated from the v1/v2 engine and the concrete
Valkey topology implementations.  Topologies and feature behavior enter only
through provider and component registries.
"""

import inspect
import math
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Optional, Protocol, Union, runtime_checkable

from .extensibility import (
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
    Registry,
    RunContext,
    RunMode,
    RunOutcome,
    RunPlan,
    Workload,
    to_plain_data,
)


@dataclass(frozen=True)
class ProvisionedTopology:
    """Exact runtime handle and services returned by a topology provider."""

    handle: Any = None
    services: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.services, Mapping):
            raise TypeError("provisioned topology services must be a mapping")
        if any(not isinstance(key, str) for key in self.services):
            raise TypeError("provisioned topology service names must be strings")
        object.__setattr__(self, "services", MappingProxyType(dict(self.services)))


@runtime_checkable
class TopologyProvider(Protocol):
    """Topology lifecycle contract used by the generic runner.

    Provider factories and provider constructors MUST only create in-memory
    ownership state. External acquisition belongs in ``provision`` so failed
    validation never needs lifecycle cleanup.

    ``validate`` is pure and must not create processes, files, sockets, or
    other externally visible state.  If ``provision`` fails or returns an
    invalid value, the runner invokes ``rollback``.  Once a valid topology is
    returned, the runner guarantees one final cleanup attempt with that exact
    object.
    """

    def validate(self, plan: RunPlan) -> None: ...

    def provision(self, context: RunContext) -> ProvisionedTopology: ...

    def rollback(self, context: RunContext) -> None: ...

    def cleanup(self, context: RunContext, topology: ProvisionedTopology) -> None: ...


TopologyProviderFactory = Callable[[], TopologyProvider]
TopologyProviderEntry = Union[TopologyProvider, TopologyProviderFactory]


class FeatureRunnerError(RuntimeError):
    """Base class for generic-runner contract failures."""


class CooperativeDeadlineError(FeatureRunnerError):
    """A component crossed a cooperative, runner-visible deadline."""


@dataclass(frozen=True)
class _ResolvedComponent:
    spec: ComponentSpec
    registry: Registry[Any]
    registration: ComponentRegistration[Any]
    instance: Any


@dataclass(frozen=True)
class _ValidatedComponent:
    spec: ComponentSpec
    registry: Registry[Any]
    registration: ComponentRegistration[Any]


@dataclass(frozen=True)
class _PreparedPlan:
    ordered: tuple[ComponentSpec, ...]
    effective_required: frozenset[str]
    provider: TopologyProvider


@dataclass(frozen=True)
class _CleanupEntry:
    resolved: _ResolvedComponent
    method_name: str


class _RunnerContext:
    def __init__(
        self,
        plan: RunPlan,
        event_bus: LifecycleEventBus,
        artifact_dir: str,
        deadline: Optional[float],
    ):
        self._plan = plan
        self._event_bus = event_bus
        self._artifact_dir = artifact_dir
        self._run_deadline = deadline
        self._component_deadline: Optional[float] = None
        self._cleanup_depth = 0
        self._services: Mapping[str, Any] = MappingProxyType({})
        self._topology: Optional[ProvisionedTopology] = None

    @property
    def plan(self) -> RunPlan:
        return self._plan

    @property
    def event_bus(self) -> LifecycleEventBus:
        return self._event_bus

    def rng(self, component_instance_id: str, stream: str = "default"):
        return self._plan.rng(component_instance_id, stream)

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

    def bind_topology(self, topology: ProvisionedTopology) -> None:
        self._topology = topology
        self._services = topology.services

    def clear_topology(self) -> None:
        self._topology = None
        self._services = MappingProxyType({})

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
    try:
        inspect.signature(callable_value).bind(*args)
    except (TypeError, ValueError) as exc:
        raise FeatureRunnerError(f"{label} has an incompatible signature: {exc}") from exc


def stable_topological_order(components: Sequence[ComponentSpec]) -> tuple[ComponentSpec, ...]:
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


def _exception_data(exc: BaseException) -> Mapping[str, str]:
    return {
        "type": type(exc).__name__,
        "message": str(exc),
    }


def _event_data(event: LifecycleEvent) -> Mapping[str, Any]:
    return {
        "name": event.name,
        "sequence": event.sequence,
        "timestamp": event.timestamp,
        "source": event.source,
        "payload": to_plain_data(event.payload),
    }


def _component_error(spec: ComponentSpec, summary: str, exc: Optional[BaseException] = None) -> ComponentResult:
    diagnostics: Mapping[str, Any] = {}
    if exc is not None:
        diagnostics = {"exception": _exception_data(exc)}
    return ComponentResult(
        component_instance_id=spec.instance_id,
        status=ComponentStatus.ERROR,
        summary=summary,
        diagnostics=diagnostics,
    )


def _blocked_result(spec: ComponentSpec, reasons: Sequence[str]) -> ComponentResult:
    return ComponentResult(
        component_instance_id=spec.instance_id,
        status=ComponentStatus.SKIP,
        summary=f"blocked: {', '.join(reasons)}",
        diagnostics={"blocked_by": tuple(reasons)},
    )


def _validate_phase_dependencies(ordered: Sequence[ComponentSpec]) -> None:
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


def _effective_required_instances(plan: RunPlan) -> frozenset[str]:
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


def _validate_feature_mode(plan: RunPlan) -> None:
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


class FeatureRunner:
    """Execute immutable feature-pack plans through registered generic roles."""

    def __init__(
        self,
        registries: Mapping[ComponentKind, Registry[Any]],
        topology_providers: Mapping[str, TopologyProviderEntry],
        *,
        artifact_dir: str = "",
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
        if not isinstance(topology_providers, Mapping):
            raise TypeError("topology_providers must be a mapping")
        normalized_providers = {}
        for topology, provider_entry in topology_providers.items():
            if not isinstance(topology, str) or not topology or topology != topology.strip():
                raise ValueError("topology provider names must be non-empty strings without surrounding whitespace")
            if topology in normalized_providers:
                raise ValueError(f"duplicate topology provider {topology!r}")
            if not isinstance(provider_entry, TopologyProvider) and not callable(provider_entry):
                raise TypeError(
                    f"topology provider entry for {topology!r} must implement TopologyProvider or be a factory"
                )
            normalized_providers[topology] = provider_entry
        if not isinstance(artifact_dir, str):
            raise TypeError("artifact_dir must be a string")
        if not callable(clock):
            raise TypeError("clock must be callable")

        self._registries = MappingProxyType(normalized_registries)
        self._topology_providers = MappingProxyType(normalized_providers)
        self._artifact_dir = artifact_dir
        self._clock = clock

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
            plan=context.plan,
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
        if registry.api_version != plan.api_version:
            raise FeatureRunnerError(
                f"registry for {spec.kind.value!r} uses API {registry.api_version!r}, expected {plan.api_version!r}"
            )
        try:
            registration = registry.resolve(spec.component_id)
        except Exception as exc:
            raise FeatureRunnerError(
                f"cannot resolve {spec.kind.value} component {spec.component_id!r}: {exc}"
            ) from exc
        if registration.kind is not spec.kind or registration.api_version != plan.api_version:
            raise FeatureRunnerError(
                f"registration {spec.component_id!r} is incompatible with {spec.kind.value} API {plan.api_version!r}"
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
    ) -> _PreparedPlan:
        _validate_feature_mode(plan)
        ordered = stable_topological_order(plan.components)
        _validate_phase_dependencies(ordered)
        effective_required = _effective_required_instances(plan)
        provider_entry = self._topology_providers.get(plan.topology)
        if provider_entry is None:
            raise FeatureRunnerError(f"no topology provider is registered for {plan.topology!r}")

        if not isinstance(provider_entry, type) and isinstance(provider_entry, TopologyProvider):
            provider = provider_entry
        else:
            _require_signature(
                provider_entry,
                (),
                f"topology provider factory for {plan.topology!r}",
            )
            try:
                provider = provider_entry()
            except Exception as exc:
                raise FeatureRunnerError(f"topology provider factory for {plan.topology!r} failed: {exc}") from exc
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
        validation_return = provider.validate(plan)
        if validation_return is not None:
            raise FeatureRunnerError("topology provider validation must return None")

        return _PreparedPlan(
            ordered=ordered,
            effective_required=effective_required,
            provider=provider,
        )

    def validate(self, plan: RunPlan) -> None:
        """Preflight a plan without provisioning a topology or running components.

        Topology provider factories are resolved so their pure ``validate``
        method can inspect the complete plan. Component factories are checked
        for zero-argument compatibility but are deliberately not invoked.
        """

        if not isinstance(plan, RunPlan):
            raise TypeError("plan must be a RunPlan")
        context = _RunnerContext(
            plan,
            LifecycleEventBus(clock=self._clock),
            self._artifact_dir,
            None,
        )
        prepared = self._prepare_plan(plan, context)
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
    ) -> RunOutcome:
        diagnostics: dict[str, Any] = {}
        primary_base_exception: Optional[BaseException] = None
        try:
            context.event_bus.publish("run.completed", source="runner")
        except BaseException as exc:
            if isinstance(exc, Exception):
                status = OutcomeStatus.ERROR
                summary = f"runner error while finalizing event journal: {exc}"
                diagnostics["error"] = _exception_data(exc)
            else:
                primary_base_exception = exc
        try:
            context.event_bus.quiesce()
        except BaseException as exc:
            if isinstance(exc, Exception):
                status = OutcomeStatus.ERROR
                summary = f"runner error while quiescing event journal: {exc}"
                diagnostics["error"] = _exception_data(exc)
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
            diagnostics=diagnostics,
        )

    def run(self, plan: RunPlan, *, deadline: Optional[float] = None) -> RunOutcome:
        """Execute ``plan`` and return one uniform terminal outcome.

        Deadlines are absolute values from the configured monotonic clock.
        Enforcement is cooperative: the current deadline is exposed through
        ``RunContext.deadline`` and checked before and after synchronous calls.
        No worker threads are abandoned to implement timeouts.
        """

        if not isinstance(plan, RunPlan):
            raise TypeError("plan must be a RunPlan")
        if deadline is not None and (
            isinstance(deadline, bool) or not isinstance(deadline, (int, float)) or not math.isfinite(deadline)
        ):
            raise TypeError("deadline must be a finite number or None")

        started_at = self._clock()
        event_bus = LifecycleEventBus(clock=self._clock)
        context = _RunnerContext(plan, event_bus, self._artifact_dir, deadline)
        execution_results: dict[str, ComponentResult] = {}
        evidence_ledger: list[Evidence] = []
        invoked_results: set[str] = set()
        cleanup_errors: list[Mapping[str, Any]] = []
        error: Optional[BaseException] = None
        primary_base_exception: Optional[BaseException] = None
        topology: Optional[ProvisionedTopology] = None
        provider: Optional[TopologyProvider] = None
        cleanups: list[_CleanupEntry] = []
        current_spec: Optional[ComponentSpec] = None
        effective_required: frozenset[str] = frozenset()

        event_bus.publish("run.started", source="runner", payload={"scenario_id": plan.scenario_id})

        try:
            prepared = self._prepare_plan(plan, context)
            ordered = prepared.ordered
            effective_required = prepared.effective_required
            provider = prepared.provider
            resolved = self._preflight_components(plan, ordered, context)
            by_instance = {item.spec.instance_id: item for item in resolved}

            for spec in ordered:
                if spec.kind is not ComponentKind.CAPABILITY:
                    continue
                current_spec = spec
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
                    return self._finish_without_topology(
                        context=context,
                        status=OutcomeStatus.SKIP_UNSUPPORTED,
                        summary=f"required capability {spec.instance_id!r} is unsupported",
                        component_results=self._result_sequence(ordered, execution_results),
                        started_at=started_at,
                    )
                if spec.instance_id in effective_required and result.status is ComponentStatus.FAIL:
                    return self._finish_without_topology(
                        context=context,
                        status=OutcomeStatus.FAIL,
                        summary=f"required capability {spec.instance_id!r} failed",
                        component_results=self._result_sequence(ordered, execution_results),
                        started_at=started_at,
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
                    OutcomeStatus.SKIP_UNSUPPORTED if ComponentStatus.SKIP in blocked_statuses else OutcomeStatus.FAIL
                )
                return self._finish_without_topology(
                    context=context,
                    status=outcome_status,
                    summary=(f"required capabilities were blocked: {', '.join(blocked_required_capabilities)}"),
                    component_results=self._result_sequence(ordered, execution_results),
                    started_at=started_at,
                )

            self._check_registry_drift(resolved)
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
            context.bind_topology(topology)
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

        except Exception as exc:
            error = exc
            if current_spec is not None and current_spec.instance_id not in execution_results:
                execution_results[current_spec.instance_id] = _component_error(
                    current_spec,
                    f"runner error: {exc}",
                    exc,
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

        if error is None and primary_base_exception is None and "ordered" in locals():
            try:
                resolved_by_instance = {item.spec.instance_id: item for item in resolved}
                for spec in ordered:
                    if spec.kind is not ComponentKind.ORACLE:
                        continue
                    current_spec = spec
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
                event_bus.publish("validation.completed", source="runner")
                self._check_registry_drift(resolved)
            except Exception as exc:
                error = exc
                if current_spec is not None and current_spec.instance_id not in execution_results:
                    execution_results[current_spec.instance_id] = _component_error(
                        current_spec,
                        f"runner error: {exc}",
                        exc,
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
                context.clear_topology()

        try:
            event_bus.publish("run.completed", source="runner")
        except BaseException as exc:
            if isinstance(exc, Exception):
                if error is None:
                    error = exc
            elif primary_base_exception is None:
                primary_base_exception = exc

        try:
            event_bus.quiesce()
        except BaseException as exc:
            if isinstance(exc, Exception):
                if error is None:
                    error = exc
            elif primary_base_exception is None:
                primary_base_exception = exc

        if primary_base_exception is None and "resolved" in locals():
            try:
                self._check_registry_drift(resolved)
            except Exception as exc:
                if error is None:
                    error = exc

        if primary_base_exception is not None:
            raise primary_base_exception

        component_results = self._result_sequence(
            ordered if "ordered" in locals() else plan.components,
            execution_results,
        )
        required_failures = tuple(
            spec.instance_id
            for spec in (ordered if "ordered" in locals() else ())
            if spec.instance_id in effective_required
            and spec.instance_id in execution_results
            and execution_results[spec.instance_id].status in (ComponentStatus.FAIL, ComponentStatus.SKIP)
        )
        missing_events = tuple(
            event_name for event_name in plan.required_events if not event_bus.committed_history(event_name)
        )
        missing_evidence = _missing_evidence_requirements(
            plan.required_evidence,
            evidence_ledger,
        )
        real_required_failure = any(
            spec.instance_id in effective_required
            and spec.instance_id in invoked_results
            and execution_results[spec.instance_id].status is ComponentStatus.FAIL
            for spec in (ordered if "ordered" in locals() else ())
            if spec.instance_id in execution_results
        )

        diagnostics: dict[str, Any] = {}
        if cleanup_errors:
            diagnostics["cleanup_errors"] = tuple(cleanup_errors)
        if missing_events:
            diagnostics["missing_required_events"] = missing_events
        if missing_evidence:
            diagnostics["missing_required_evidence"] = missing_evidence
        if required_failures:
            diagnostics["required_component_failures"] = required_failures
        if error is not None:
            diagnostics["error"] = _exception_data(error)

        if error is not None:
            status = OutcomeStatus.ERROR
            summary = f"runner error: {error}"
        elif cleanup_errors and not real_required_failure:
            status = OutcomeStatus.ERROR
            summary = "cleanup failed before a trustworthy feature failure was established"
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

        return self._new_outcome(
            context=context,
            status=status,
            summary=summary,
            component_results=component_results,
            started_at=started_at,
            diagnostics=diagnostics,
        )


def run_feature_plan(
    plan: RunPlan,
    registries: Mapping[ComponentKind, Registry[Any]],
    topology_providers: Mapping[str, TopologyProviderEntry],
    *,
    artifact_dir: str = "",
    deadline: Optional[float] = None,
) -> RunOutcome:
    """Functional convenience wrapper around :class:`FeatureRunner`."""

    return FeatureRunner(
        registries,
        topology_providers,
        artifact_dir=artifact_dir,
    ).run(plan, deadline=deadline)


__all__ = [
    "CooperativeDeadlineError",
    "FeatureRunner",
    "FeatureRunnerError",
    "ProvisionedTopology",
    "TopologyProvider",
    "TopologyProviderEntry",
    "TopologyProviderFactory",
    "run_feature_plan",
    "stable_topological_order",
]
