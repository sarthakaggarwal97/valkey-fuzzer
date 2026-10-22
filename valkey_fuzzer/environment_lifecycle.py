"""Transactional runtime lifecycle for generic environment declarations.

This module owns no concrete artifact or resource behavior. It coordinates
extension-provided providers using the contracts in :mod:`valkey_fuzzer.environment`.
The resulting session and binding are deliberately runtime-only: provider
outputs, services, and handles must never be serialized into a run plan or
report.
"""

# ruff: noqa: UP007, UP045

from __future__ import annotations

import hashlib
import inspect
import math
import os
import re
import threading
from collections.abc import Mapping, Sequence
from contextlib import ExitStack
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Optional, Union

from .environment import (
    ENVIRONMENT_REFERENCE_DATA_KEY,
    ArtifactProvider,
    ArtifactProviderRegistration,
    ArtifactProviderRegistry,
    ArtifactSpec,
    EnvironmentDependency,
    EnvironmentKind,
    EnvironmentReference,
    EnvironmentSpec,
    PolicySpec,
    ProviderContractError,
    ProvisionedResource,
    ResolvedArtifact,
    ResourceProvider,
    ResourceProviderRegistration,
    ResourceProviderRegistry,
    ResourceSpec,
    SupportDecision,
    parse_environment_dependency,
    resolve_environment_references,
    stable_environment_order,
)

EnvironmentResult = Union[ResolvedArtifact, ProvisionedResource]
EnvironmentProvider = Union[ArtifactProvider, ResourceProvider]
EnvironmentProviderRegistration = Union[ArtifactProviderRegistration, ResourceProviderRegistration]

_SAFE_PATH_PREFIX = re.compile(r"[^A-Za-z0-9_-]+")
_SAFE_PATH_PREFIX_LIMIT = 40
_SAFE_PATH_READ_LIMIT = 160


def _require_text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{field_name} must be a non-empty string without surrounding whitespace")
    return value


def _require_seed(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("seed must be an integer")
    return value


def _require_deadline(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("deadline must be an absolute monotonic number or None")
    deadline = float(value)
    if not math.isfinite(deadline):
        raise ValueError("deadline must be finite")
    return deadline


def _freeze_report_value(value: Any, field_name: str) -> Any:
    """Freeze strictly JSON-safe report data.

    Runtime bindings and typed environment references are intentionally
    rejected. Providers remain responsible for not placing credentials in
    otherwise valid strings.
    """

    if isinstance(value, EnvironmentReference):
        raise TypeError(f"{field_name} must not contain runtime environment references")
    if isinstance(value, Mapping):
        if ENVIRONMENT_REFERENCE_DATA_KEY in value:
            raise TypeError(
                f"{field_name} must not contain the reserved {ENVIRONMENT_REFERENCE_DATA_KEY!r} reference marker"
            )
        frozen = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{field_name} mapping keys must be strings")
            frozen[key] = _freeze_report_value(item, f"{field_name}.{key}")
        return MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_report_value(item, f"{field_name}[{index}]") for index, item in enumerate(value))
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{field_name} cannot contain NaN or infinite numbers")
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise TypeError(
        f"{field_name} must contain only JSON-safe scalar, mapping, and sequence values; got {type(value).__name__}"
    )


def _freeze_report_mapping(value: Mapping[str, Any], field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must be a mapping")
    return _freeze_report_value(value, field_name)


def _plain_report_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {key: _plain_report_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain_report_value(item) for item in value]
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise TypeError(f"cannot export report value of type {type(value).__name__}")


def _exception_type(error: BaseException) -> str:
    error_type = type(error)
    if error_type.__module__ == "builtins":
        return error_type.__qualname__
    return f"{error_type.__module__}.{error_type.__qualname__}"


def _safe_path_segment(value: str, field_name: str) -> str:
    """Return a readable, collision-resistant, non-special path segment."""

    _require_text(value, field_name)
    readable = _SAFE_PATH_PREFIX.sub("-", value[:_SAFE_PATH_READ_LIMIT]).strip("-_")[:_SAFE_PATH_PREFIX_LIMIT]
    if not readable or readable in (".", ".."):
        readable = "value"
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]
    segment = f"{readable}-{digest}"
    if segment in (".", "..") or os.sep in segment or (os.altsep is not None and os.altsep in segment):
        raise AssertionError("safe path segment construction produced an unsafe value")
    return segment


def _path_is_contained(root: str, candidate: str) -> bool:
    try:
        return os.path.normcase(os.path.commonpath((root, candidate))) == os.path.normcase(root)
    except ValueError:
        return False


def _contains_existing_symlink(root: str, candidate: str) -> bool:
    """Check lifecycle-owned components without following existing aliases."""

    if os.path.lexists(root) and os.path.islink(root):
        return True
    relative = os.path.relpath(candidate, root)
    current = root
    for component in relative.split(os.sep):
        current = os.path.join(current, component)
        if os.path.lexists(current) and os.path.islink(current):
            return True
    return False


class EnvironmentLifecyclePhase(str, Enum):
    CONFIGURE = "configure"
    FACTORY = "factory"
    SUPPORT = "support"
    ACQUIRE = "acquire"
    ROLLBACK = "rollback"
    CLEANUP = "cleanup"
    REGISTRY = "registry"


class EnvironmentLifecycleOutcome(str, Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    UNSUPPORTED = "unsupported"


class EnvironmentReportStatus(str, Enum):
    ACQUIRED = "acquired"
    CLEANED = "cleaned"
    UNSUPPORTED = "unsupported"
    FAILED = "failed"
    CLEANUP_FAILED = "cleanup_failed"


class EnvironmentSessionState(str, Enum):
    OPEN = "open"
    CLOSING = "closing"
    CLOSED = "closed"


@dataclass(frozen=True)
class EnvironmentLifecycleRecord:
    """One secret-safe lifecycle transition."""

    dependency: str
    provider_id: str
    phase: EnvironmentLifecyclePhase
    outcome: EnvironmentLifecycleOutcome
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        parse_environment_dependency(self.dependency)
        _require_text(self.provider_id, "provider_id")
        if not isinstance(self.phase, EnvironmentLifecyclePhase):
            raise TypeError("phase must be an EnvironmentLifecyclePhase")
        if not isinstance(self.outcome, EnvironmentLifecycleOutcome):
            raise TypeError("outcome must be an EnvironmentLifecycleOutcome")
        object.__setattr__(
            self,
            "details",
            _freeze_report_mapping(self.details, "lifecycle record details"),
        )

    def to_data(self) -> Mapping[str, Any]:
        return {
            "dependency": self.dependency,
            "provider_id": self.provider_id,
            "phase": self.phase.value,
            "outcome": self.outcome.value,
            "details": _plain_report_value(self.details),
        }


@dataclass(frozen=True)
class EnvironmentProviderEvent:
    """A provider-published event scoped to one declaration and phase."""

    dependency: str
    provider_id: str
    phase: EnvironmentLifecyclePhase
    name: str
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        parse_environment_dependency(self.dependency)
        _require_text(self.provider_id, "provider_id")
        if not isinstance(self.phase, EnvironmentLifecyclePhase):
            raise TypeError("phase must be an EnvironmentLifecyclePhase")
        _require_text(self.name, "event name")
        object.__setattr__(
            self,
            "details",
            _freeze_report_mapping(self.details, "provider event details"),
        )

    def to_data(self) -> Mapping[str, Any]:
        return {
            "dependency": self.dependency,
            "provider_id": self.provider_id,
            "phase": self.phase.value,
            "name": self.name,
            "details": _plain_report_value(self.details),
        }


@dataclass(frozen=True)
class EnvironmentReport:
    """Immutable, secret-safe snapshot of an environment lifecycle."""

    run_id: str
    status: EnvironmentReportStatus
    records: tuple[EnvironmentLifecycleRecord, ...] = ()
    events: tuple[EnvironmentProviderEvent, ...] = ()

    def __post_init__(self) -> None:
        _require_text(self.run_id, "run_id")
        if not isinstance(self.status, EnvironmentReportStatus):
            raise TypeError("status must be an EnvironmentReportStatus")
        records = tuple(self.records)
        events = tuple(self.events)
        if any(not isinstance(record, EnvironmentLifecycleRecord) for record in records):
            raise TypeError("records must contain EnvironmentLifecycleRecord values")
        if any(not isinstance(event, EnvironmentProviderEvent) for event in events):
            raise TypeError("events must contain EnvironmentProviderEvent values")
        object.__setattr__(self, "records", records)
        object.__setattr__(self, "events", events)

    def to_data(self) -> Mapping[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status.value,
            "records": [record.to_data() for record in self.records],
            "events": [event.to_data() for event in self.events],
        }


@dataclass(frozen=True)
class EnvironmentCleanupFailure:
    """Runtime diagnostic for a failed rollback or cleanup call.

    The original exception is intentionally runtime-only. Reports export only
    its type, never its potentially sensitive message or attributes.
    """

    dependency: str
    provider_id: str
    phase: EnvironmentLifecyclePhase
    exception_type: str
    error: BaseException = field(repr=False, compare=False, metadata={"serialize": False})

    def __post_init__(self) -> None:
        parse_environment_dependency(self.dependency)
        _require_text(self.provider_id, "provider_id")
        if self.phase not in (EnvironmentLifecyclePhase.ROLLBACK, EnvironmentLifecyclePhase.CLEANUP):
            raise ValueError("cleanup failure phase must be rollback or cleanup")
        _require_text(self.exception_type, "exception_type")
        if not isinstance(self.error, BaseException):
            raise TypeError("error must be a BaseException")

    def to_data(self) -> Mapping[str, Any]:
        return {
            "dependency": self.dependency,
            "provider_id": self.provider_id,
            "phase": self.phase.value,
            "exception_type": self.exception_type,
        }


class EnvironmentLifecycleError(RuntimeError):
    """Base class for lifecycle-owned failures."""

    def __init__(self, message: str):
        super().__init__(message)
        self.report: Optional[EnvironmentReport] = None
        self.environment_report: Optional[EnvironmentReport] = None
        self.environment_cleanup_failures: tuple[EnvironmentCleanupFailure, ...] = ()
        self.environment_registry_drift: Optional[EnvironmentRegistryDriftError] = None


class UnsupportedEnvironmentError(EnvironmentLifecycleError):
    """A provider reported that a declaration is unsupported."""

    def __init__(
        self,
        dependency: EnvironmentDependency,
        provider_id: str,
        decision: SupportDecision,
    ):
        if not isinstance(dependency, EnvironmentDependency):
            raise TypeError("dependency must be an EnvironmentDependency")
        if not isinstance(decision, SupportDecision) or decision.supported:
            raise TypeError("decision must be an unsupported SupportDecision")
        self.dependency = dependency
        self.provider_id = provider_id
        self.decision = decision
        super().__init__(f"{dependency.path} is unsupported by provider {provider_id!r}: {decision.reason}")


class EnvironmentRegistryDriftError(EnvironmentLifecycleError):
    """A selected provider registration no longer matches its pinned entry."""

    def __init__(
        self,
        dependency: EnvironmentDependency,
        provider_id: str,
    ):
        if not isinstance(dependency, EnvironmentDependency):
            raise TypeError("dependency must be an EnvironmentDependency")
        self.dependency = dependency
        self.provider_id = provider_id
        super().__init__(
            f"provider registry entry {provider_id!r} selected by {dependency.path} "
            "changed during the environment lifecycle"
        )


class EnvironmentArtifactPathError(EnvironmentLifecycleError):
    """A provider artifact directory could not be contained under its root."""

    def __init__(self, dependency: EnvironmentDependency, reason: str):
        if not isinstance(dependency, EnvironmentDependency):
            raise TypeError("dependency must be an EnvironmentDependency")
        _require_text(reason, "reason")
        self.dependency = dependency
        self.reason = reason
        super().__init__(f"artifact directory for {dependency.path} is unsafe: {reason}")


class EnvironmentCleanupError(EnvironmentLifecycleError):
    """Rollback or cleanup failed after all eligible teardown was attempted."""

    def __init__(
        self,
        failures: Sequence[EnvironmentCleanupFailure],
        *,
        report: EnvironmentReport,
    ):
        entries = tuple(failures)
        if not entries:
            raise ValueError("failures must not be empty")
        if any(not isinstance(failure, EnvironmentCleanupFailure) for failure in entries):
            raise TypeError("failures must contain EnvironmentCleanupFailure values")
        self.failures = entries
        summary = ", ".join(
            f"{failure.dependency}:{failure.phase.value}:{failure.exception_type}" for failure in entries
        )
        super().__init__(f"environment teardown failed ({summary})")
        self.report = report
        self.environment_report = report
        self.environment_cleanup_failures = entries


class EnvironmentSessionClosedError(EnvironmentLifecycleError):
    """A runtime binding was used after its providers were cleaned up."""


class EnvironmentSessionClosingError(EnvironmentLifecycleError):
    """A session operation was attempted reentrantly during cleanup."""


class _ReportBuilder:
    def __init__(self, run_id: str):
        self._run_id = run_id
        self._records: list[EnvironmentLifecycleRecord] = []
        self._events: list[EnvironmentProviderEvent] = []
        self._lock = threading.RLock()

    def record(
        self,
        spec: EnvironmentSpec,
        phase: EnvironmentLifecyclePhase,
        outcome: EnvironmentLifecycleOutcome,
        details: Optional[Mapping[str, Any]] = None,
    ) -> None:
        record = EnvironmentLifecycleRecord(
            dependency=spec.dependency.path,
            provider_id=spec.provider_id,
            phase=phase,
            outcome=outcome,
            details={} if details is None else details,
        )
        with self._lock:
            self._records.append(record)

    def event(
        self,
        spec: EnvironmentSpec,
        phase: EnvironmentLifecyclePhase,
        name: str,
        details: Mapping[str, Any],
    ) -> None:
        event = EnvironmentProviderEvent(
            dependency=spec.dependency.path,
            provider_id=spec.provider_id,
            phase=phase,
            name=name,
            details=details,
        )
        with self._lock:
            self._events.append(event)

    def snapshot(self, status: EnvironmentReportStatus) -> EnvironmentReport:
        with self._lock:
            return EnvironmentReport(
                run_id=self._run_id,
                status=status,
                records=tuple(self._records),
                events=tuple(self._events),
            )


@dataclass(frozen=True)
class _ProviderContext:
    run_id: str
    seed: int
    deadline: Optional[float]
    artifact_dir: str
    dependency_services: Mapping[str, Mapping[str, Any]] = field(
        repr=False,
        compare=False,
        metadata={"serialize": False},
    )
    _spec: EnvironmentSpec = field(
        repr=False,
        compare=False,
        metadata={"serialize": False},
    )
    _phase: EnvironmentLifecyclePhase = field(
        repr=False,
        compare=False,
        metadata={"serialize": False},
    )
    _report: _ReportBuilder = field(
        repr=False,
        compare=False,
        metadata={"serialize": False},
    )

    def __post_init__(self) -> None:
        _require_text(self.run_id, "run_id")
        _require_seed(self.seed)
        object.__setattr__(self, "deadline", _require_deadline(self.deadline))
        if not isinstance(self.artifact_dir, str) or not os.path.isabs(self.artifact_dir):
            raise ValueError("artifact_dir must be an absolute path")
        if not isinstance(self._spec, (ArtifactSpec, ResourceSpec)):
            raise TypeError("spec must be an ArtifactSpec or ResourceSpec")
        if not isinstance(self._phase, EnvironmentLifecyclePhase):
            raise TypeError("phase must be an EnvironmentLifecyclePhase")
        if not isinstance(self._report, _ReportBuilder):
            raise TypeError("report must be a lifecycle report builder")
        if not isinstance(self.dependency_services, Mapping):
            raise TypeError("dependency_services must be a mapping")
        services = {}
        for dependency_path, dependency_values in self.dependency_services.items():
            parse_environment_dependency(dependency_path)
            if not isinstance(dependency_values, Mapping):
                raise TypeError("dependency service values must be mappings")
            service_snapshot = {}
            for service_name, service in dependency_values.items():
                _require_text(service_name, "service name")
                service_snapshot[service_name] = service
            services[dependency_path] = MappingProxyType(service_snapshot)
        object.__setattr__(self, "dependency_services", MappingProxyType(services))

    def publish_event(self, name: str, details: Mapping[str, Any]) -> None:
        self._report.event(self._spec, self._phase, name, details)

    def for_phase(
        self,
        phase: EnvironmentLifecyclePhase,
        *,
        deadline: Optional[float],
    ) -> _ProviderContext:
        return _ProviderContext(
            run_id=self.run_id,
            seed=self.seed,
            deadline=deadline,
            artifact_dir=self.artifact_dir,
            dependency_services=self.dependency_services,
            _spec=self._spec,
            _phase=phase,
            _report=self._report,
        )


@dataclass(frozen=True)
class _ProviderPin:
    dependency: EnvironmentDependency
    provider_id: str
    kind: EnvironmentKind
    registration: EnvironmentProviderRegistration = field(repr=False, compare=False)


class _PinnedRegistrations:
    def __init__(
        self,
        artifact_registry: ArtifactProviderRegistry,
        resource_registry: ResourceProviderRegistry,
        pins: Sequence[_ProviderPin],
    ):
        self._artifact_registry = artifact_registry
        self._resource_registry = resource_registry
        self._pins = tuple(pins)
        self._by_key = MappingProxyType({(pin.kind, pin.provider_id): pin.registration for pin in self._pins})

    def registration_for(self, spec: EnvironmentSpec) -> EnvironmentProviderRegistration:
        return self._by_key[(spec.dependency.kind, spec.provider_id)]

    def assert_unchanged(self) -> None:
        for pin in self._pins:
            registry: Union[ArtifactProviderRegistry, ResourceProviderRegistry]
            if pin.kind is EnvironmentKind.ARTIFACT:
                registry = self._artifact_registry
            else:
                registry = self._resource_registry
            try:
                current = registry.resolve(pin.provider_id)
            except BaseException as error:
                drift = EnvironmentRegistryDriftError(pin.dependency, pin.provider_id)
                raise drift from error
            if current is not pin.registration:
                raise EnvironmentRegistryDriftError(pin.dependency, pin.provider_id)


@dataclass(frozen=True)
class _AcquiredLease:
    spec: EnvironmentSpec
    provider: EnvironmentProvider = field(repr=False, compare=False)
    result: EnvironmentResult = field(repr=False, compare=False)
    context: _ProviderContext = field(repr=False, compare=False)


def _deferred_result_error(
    provider_id: str,
    method_name: str,
    result: Any,
) -> Optional[ProviderContractError]:
    if inspect.iscoroutine(result):
        result.close()
        return ProviderContractError(
            provider_id,
            f"{method_name} returned an awaitable instead of executing synchronously",
        )
    if inspect.isawaitable(result):
        return ProviderContractError(
            provider_id,
            f"{method_name} returned an awaitable instead of executing synchronously",
        )
    if inspect.isasyncgen(result):
        return ProviderContractError(
            provider_id,
            f"{method_name} returned an async generator instead of executing synchronously",
        )
    if inspect.isgenerator(result):
        result.close()
        return ProviderContractError(
            provider_id,
            f"{method_name} returned a generator instead of executing eagerly",
        )
    return None


def _require_eager_result(provider_id: str, method_name: str, result: Any) -> Any:
    error = _deferred_result_error(provider_id, method_name, result)
    if error is not None:
        raise error
    return result


def _require_none_result(provider_id: str, method_name: str, result: Any) -> None:
    _require_eager_result(provider_id, method_name, result)
    if result is not None:
        raise ProviderContractError(provider_id, f"{method_name} must return None")


def _validate_provider_method(
    provider_id: str,
    provider: Any,
    method_name: str,
    positional_argument_count: int,
) -> None:
    try:
        method = getattr(provider, method_name, None)
    except Exception as error:
        raise ProviderContractError(
            provider_id,
            f"cannot access {method_name} method: {type(error).__name__}: {error}",
        ) from error
    if not callable(method):
        raise ProviderContractError(provider_id, f"{method_name} method must be callable")
    try:
        implementation = inspect.unwrap(method)
    except (TypeError, ValueError):
        implementation = method
    if inspect.iscoroutinefunction(implementation) or inspect.isasyncgenfunction(implementation):
        raise ProviderContractError(
            provider_id,
            f"{method_name} method must be synchronous, not async",
        )
    if inspect.isgeneratorfunction(implementation):
        raise ProviderContractError(
            provider_id,
            f"{method_name} method must execute eagerly, not return a generator",
        )
    try:
        inspect.signature(method).bind(*([object()] * positional_argument_count))
    except (TypeError, ValueError) as error:
        raise ProviderContractError(
            provider_id,
            f"{method_name} method has an incompatible signature: {error}",
        ) from error


def _create_pinned_provider(
    spec: EnvironmentSpec,
    registration: EnvironmentProviderRegistration,
) -> EnvironmentProvider:
    try:
        provider = registration.factory()
    except Exception as error:
        raise ProviderContractError(
            spec.provider_id,
            f"factory failed with {type(error).__name__}: {error}",
        ) from error
    provider = _require_eager_result(spec.provider_id, "factory", provider)

    if isinstance(spec, ArtifactSpec):
        if not isinstance(registration, ArtifactProviderRegistration):
            raise ProviderContractError(spec.provider_id, "pinned an incompatible artifact registration")
        protocol = ArtifactProvider
        methods = {
            "check_support": 3,
            "resolve": 3,
            "rollback": 2,
            "cleanup": 3,
        }
    else:
        if not isinstance(registration, ResourceProviderRegistration):
            raise ProviderContractError(spec.provider_id, "pinned an incompatible resource registration")
        protocol = ResourceProvider
        methods = {
            "check_support": 3,
            "provision": 3,
            "rollback": 2,
            "cleanup": 3,
        }

    for method_name, positional_argument_count in methods.items():
        _validate_provider_method(
            spec.provider_id,
            provider,
            method_name,
            positional_argument_count,
        )
    if not isinstance(provider, protocol):
        raise ProviderContractError(
            spec.provider_id,
            f"factory must return an object implementing {protocol.__name__}",
        )
    return provider


def _record_failure(
    report: _ReportBuilder,
    spec: EnvironmentSpec,
    phase: EnvironmentLifecyclePhase,
    error: BaseException,
) -> None:
    report.record(
        spec,
        phase,
        EnvironmentLifecycleOutcome.FAILED,
        {"exception_type": _exception_type(error)},
    )


def _attach_diagnostics(
    error: BaseException,
    *,
    report: EnvironmentReport,
    cleanup_failures: Sequence[EnvironmentCleanupFailure] = (),
    registry_drift: Optional[EnvironmentRegistryDriftError] = None,
) -> None:
    diagnostics = (
        ("report", report),
        ("environment_report", report),
        ("environment_cleanup_failures", tuple(cleanup_failures)),
        ("environment_registry_drift", registry_drift),
    )
    for name, value in diagnostics:
        try:
            setattr(error, name, value)
        except BaseException:
            # Diagnostics must never replace the exception whose identity and
            # traceback the lifecycle is preserving.
            pass


def _first_teardown_base_exception(
    failures: Sequence[EnvironmentCleanupFailure],
) -> Optional[BaseException]:
    return next(
        (failure.error for failure in failures if not isinstance(failure.error, Exception)),
        None,
    )


class EnvironmentBinding:
    """Runtime-only provider outputs for later plan materialization.

    This object intentionally has no data/JSON serializer. Its properties
    become unavailable once the owning session is closed.
    """

    __slots__ = ("_session",)

    def __init__(self, session: EnvironmentSession):
        self._session = session

    @property
    def artifacts(self) -> Mapping[str, ResolvedArtifact]:
        return self._session._active_artifacts()

    @property
    def resources(self) -> Mapping[str, ProvisionedResource]:
        return self._session._active_resources()

    @property
    def report(self) -> EnvironmentReport:
        return self._session.report

    def assert_active(self) -> None:
        """Reject use after the owning session starts or completes cleanup."""

        self._session._assert_binding_active()

    def resolve(self, value: Any) -> Any:
        return self._session.resolve(value)

    def __repr__(self) -> str:
        return "EnvironmentBinding(runtime_only=True)"


class EnvironmentSession:
    """Owns acquired providers until reverse-order cleanup completes."""

    def __init__(
        self,
        *,
        pins: _PinnedRegistrations,
        artifacts: Mapping[str, ResolvedArtifact],
        resources: Mapping[str, ProvisionedResource],
        leases: Sequence[_AcquiredLease],
        report: _ReportBuilder,
    ):
        self._pins = pins
        self._artifacts = MappingProxyType(dict(artifacts))
        self._resources = MappingProxyType(dict(resources))
        self._leases = tuple(leases)
        self._report_builder = report
        self._binding = EnvironmentBinding(self)
        self._state = EnvironmentSessionState.OPEN
        self._close_report: Optional[EnvironmentReport] = None
        self._close_failures: tuple[EnvironmentCleanupFailure, ...] = ()
        self._close_registry_drift: Optional[EnvironmentRegistryDriftError] = None
        self._lock = threading.RLock()
        self._close_condition = threading.Condition(self._lock)
        self._closing_owner: Optional[int] = None

    @property
    def binding(self) -> EnvironmentBinding:
        with self._lock:
            self._ensure_open()
            self._assert_pins()
            return self._binding

    @property
    def report(self) -> EnvironmentReport:
        with self._lock:
            if self._close_report is not None:
                return self._close_report
            return self._report_builder.snapshot(EnvironmentReportStatus.ACQUIRED)

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._state is EnvironmentSessionState.CLOSED

    @property
    def closing(self) -> bool:
        with self._lock:
            return self._state is EnvironmentSessionState.CLOSING

    @property
    def state(self) -> EnvironmentSessionState:
        with self._lock:
            return self._state

    @property
    def cleanup_failures(self) -> tuple[EnvironmentCleanupFailure, ...]:
        with self._lock:
            return self._close_failures

    @property
    def registry_drift(self) -> Optional[EnvironmentRegistryDriftError]:
        with self._lock:
            return self._close_registry_drift

    def _ensure_open(self) -> None:
        if self._state is EnvironmentSessionState.CLOSING:
            error = EnvironmentSessionClosingError("environment session is closing")
            _attach_diagnostics(
                error,
                report=self._report_builder.snapshot(EnvironmentReportStatus.FAILED),
            )
            raise error
        if self._state is EnvironmentSessionState.CLOSED:
            error = EnvironmentSessionClosedError("environment session is closed")
            assert self._close_report is not None
            _attach_diagnostics(
                error,
                report=self._close_report,
                cleanup_failures=self._close_failures,
                registry_drift=self._close_registry_drift,
            )
            raise error

    def _assert_pins(self) -> None:
        try:
            self._pins.assert_unchanged()
        except EnvironmentRegistryDriftError as error:
            _attach_diagnostics(
                error,
                report=self._report_builder.snapshot(EnvironmentReportStatus.FAILED),
                registry_drift=error,
            )
            raise

    def _active_artifacts(self) -> Mapping[str, ResolvedArtifact]:
        with self._lock:
            self._ensure_open()
            self._assert_pins()
            return self._artifacts

    def _assert_binding_active(self) -> None:
        with self._lock:
            self._ensure_open()

    def _active_resources(self) -> Mapping[str, ProvisionedResource]:
        with self._lock:
            self._ensure_open()
            self._assert_pins()
            return self._resources

    def resolve(self, value: Any) -> Any:
        with self._lock:
            self._ensure_open()
            self._assert_pins()
            return resolve_environment_references(
                value,
                artifacts=self._artifacts,
                resources=self._resources,
            )

    def _close(self, primary_error: Optional[BaseException]) -> EnvironmentReport:
        owner = threading.get_ident()
        with self._close_condition:
            while self._state is EnvironmentSessionState.CLOSING:
                if self._closing_owner == owner:
                    error = EnvironmentSessionClosingError("environment session close is already in progress")
                    _attach_diagnostics(
                        error,
                        report=self._report_builder.snapshot(EnvironmentReportStatus.FAILED),
                    )
                    raise error
                self._close_condition.wait()

            if self._state is EnvironmentSessionState.CLOSED:
                assert self._close_report is not None
                if primary_error is not None:
                    _attach_diagnostics(
                        primary_error,
                        report=self._close_report,
                        cleanup_failures=self._close_failures,
                        registry_drift=self._close_registry_drift,
                    )
                return self._close_report

            self._state = EnvironmentSessionState.CLOSING
            self._closing_owner = owner
            leases = self._leases

        failures: tuple[EnvironmentCleanupFailure, ...] = ()
        registry_drift: Optional[EnvironmentRegistryDriftError] = None
        internal_error: Optional[BaseException] = None
        status = EnvironmentReportStatus.FAILED
        try:
            failures = _cleanup_leases(leases, self._report_builder)
            registry_drift = _registry_drift(self._pins)
            if registry_drift is not None:
                self._report_builder.record(
                    _spec_for_drift(registry_drift, leases),
                    EnvironmentLifecyclePhase.REGISTRY,
                    EnvironmentLifecycleOutcome.FAILED,
                    {"exception_type": _exception_type(registry_drift)},
                )

            status = (
                EnvironmentReportStatus.CLEANUP_FAILED
                if failures
                else EnvironmentReportStatus.FAILED
                if registry_drift is not None
                else EnvironmentReportStatus.CLEANED
            )
        except BaseException as error:
            internal_error = error
        finally:
            report = self._report_builder.snapshot(status)
            with self._close_condition:
                self._state = EnvironmentSessionState.CLOSED
                self._closing_owner = None
                self._leases = ()
                self._close_failures = failures
                self._close_registry_drift = registry_drift
                self._close_report = report
                self._close_condition.notify_all()

        teardown_base_error = _first_teardown_base_exception(failures)
        if primary_error is not None:
            _attach_diagnostics(
                primary_error,
                report=report,
                cleanup_failures=failures,
                registry_drift=registry_drift,
            )
            if isinstance(primary_error, Exception):
                promoted_base_error = (
                    teardown_base_error
                    if teardown_base_error is not None
                    else internal_error
                    if internal_error is not None and not isinstance(internal_error, Exception)
                    else None
                )
                if promoted_base_error is not None:
                    _attach_diagnostics(
                        promoted_base_error,
                        report=report,
                        cleanup_failures=failures,
                        registry_drift=registry_drift,
                    )
                    raise promoted_base_error from primary_error
            return report
        if teardown_base_error is not None:
            _attach_diagnostics(
                teardown_base_error,
                report=report,
                cleanup_failures=failures,
                registry_drift=registry_drift,
            )
            raise teardown_base_error
        if internal_error is not None:
            _attach_diagnostics(
                internal_error,
                report=report,
                cleanup_failures=failures,
                registry_drift=registry_drift,
            )
            raise internal_error

        if failures:
            cleanup_error = EnvironmentCleanupError(failures, report=report)
            cleanup_error.environment_registry_drift = registry_drift
            if registry_drift is not None:
                raise cleanup_error from registry_drift
            raise cleanup_error
        if registry_drift is not None:
            registry_drift.report = report
            registry_drift.environment_report = report
            raise registry_drift
        return report

    def close(self) -> EnvironmentReport:
        """Clean every acquired provider in reverse dependency order."""

        return self._close(None)

    def __enter__(self) -> EnvironmentSession:
        with self._lock:
            self._ensure_open()
            self._assert_pins()
        return self

    def __exit__(
        self,
        _error_type: Optional[type[BaseException]],
        error: Optional[BaseException],
        _traceback: Any,
    ) -> bool:
        self._close(error)
        return False

    def __repr__(self) -> str:
        return f"EnvironmentSession(state={self.state.value!r})"


def _registry_drift(pins: _PinnedRegistrations) -> Optional[EnvironmentRegistryDriftError]:
    try:
        pins.assert_unchanged()
    except EnvironmentRegistryDriftError as error:
        return error
    return None


def _spec_for_drift(
    drift: EnvironmentRegistryDriftError,
    leases: Sequence[_AcquiredLease],
) -> EnvironmentSpec:
    for lease in leases:
        if lease.spec.dependency == drift.dependency and lease.spec.provider_id == drift.provider_id:
            return lease.spec
    kind = drift.dependency.kind
    if kind is EnvironmentKind.ARTIFACT:
        return ArtifactSpec(drift.dependency.instance_id, drift.provider_id)
    return ResourceSpec(drift.dependency.instance_id, drift.provider_id)


def _rollback_current(
    spec: EnvironmentSpec,
    provider: EnvironmentProvider,
    context: _ProviderContext,
    report: _ReportBuilder,
) -> tuple[EnvironmentCleanupFailure, ...]:
    try:
        rollback_context = context.for_phase(
            EnvironmentLifecyclePhase.ROLLBACK,
            deadline=None,
        )
        if isinstance(spec, ArtifactSpec):
            result = provider.rollback(rollback_context, spec)
        else:
            result = provider.rollback(rollback_context, spec)
        _require_none_result(spec.provider_id, "rollback", result)
    except BaseException as error:
        _record_failure(
            report,
            spec,
            EnvironmentLifecyclePhase.ROLLBACK,
            error,
        )
        return (
            EnvironmentCleanupFailure(
                dependency=spec.dependency.path,
                provider_id=spec.provider_id,
                phase=EnvironmentLifecyclePhase.ROLLBACK,
                exception_type=_exception_type(error),
                error=error,
            ),
        )
    report.record(
        spec,
        EnvironmentLifecyclePhase.ROLLBACK,
        EnvironmentLifecycleOutcome.SUCCEEDED,
    )
    return ()


def _cleanup_leases(
    leases: Sequence[_AcquiredLease],
    report: _ReportBuilder,
) -> tuple[EnvironmentCleanupFailure, ...]:
    failures = []
    for lease in reversed(tuple(leases)):
        try:
            cleanup_context = lease.context.for_phase(
                EnvironmentLifecyclePhase.CLEANUP,
                deadline=None,
            )
            if isinstance(lease.spec, ArtifactSpec):
                result = lease.provider.cleanup(
                    cleanup_context,
                    lease.spec,
                    lease.result,
                )
            else:
                result = lease.provider.cleanup(
                    cleanup_context,
                    lease.spec,
                    lease.result,
                )
            _require_none_result(lease.spec.provider_id, "cleanup", result)
        except BaseException as error:
            _record_failure(
                report,
                lease.spec,
                EnvironmentLifecyclePhase.CLEANUP,
                error,
            )
            failures.append(
                EnvironmentCleanupFailure(
                    dependency=lease.spec.dependency.path,
                    provider_id=lease.spec.provider_id,
                    phase=EnvironmentLifecyclePhase.CLEANUP,
                    exception_type=_exception_type(error),
                    error=error,
                )
            )
            continue
        report.record(
            lease.spec,
            EnvironmentLifecyclePhase.CLEANUP,
            EnvironmentLifecycleOutcome.SUCCEEDED,
        )
    return tuple(failures)


class EnvironmentManager:
    """Acquires generic artifacts/resources as one transactional environment.

    Artifact directories are deterministic for a
    ``(artifact_root, run_id, provider, declaration)`` tuple. Callers that
    require isolated diagnostics MUST provide unique run IDs; intentionally
    reusing a run ID reuses the same provider directory names.
    """

    def __init__(
        self,
        *,
        artifact_registry: ArtifactProviderRegistry,
        resource_registry: ResourceProviderRegistry,
        artifact_root: Union[str, os.PathLike[str]],
    ):
        if not isinstance(artifact_registry, ArtifactProviderRegistry):
            raise TypeError("artifact_registry must be an ArtifactProviderRegistry")
        if not isinstance(resource_registry, ResourceProviderRegistry):
            raise TypeError("resource_registry must be a ResourceProviderRegistry")
        try:
            root = os.fspath(artifact_root)
        except TypeError as error:
            raise TypeError("artifact_root must be a filesystem path") from error
        if isinstance(root, bytes):
            raise TypeError("artifact_root must be a text filesystem path")
        if not root or "\x00" in root or not os.path.isabs(root):
            raise ValueError("artifact_root must be a non-empty absolute path")
        self._artifact_registry = artifact_registry
        self._resource_registry = resource_registry
        self._artifact_root = os.path.realpath(os.path.normpath(root))

    def _prepare(
        self,
        artifacts: Sequence[ArtifactSpec],
        resources: Sequence[ResourceSpec],
        policies: Sequence[PolicySpec],
    ) -> tuple[tuple[EnvironmentSpec, ...], _PinnedRegistrations]:
        with ExitStack() as locks:
            locks.enter_context(self._artifact_registry.registration_lock)
            locks.enter_context(self._resource_registry.registration_lock)
            ordered = stable_environment_order(
                artifacts,
                resources,
                policies=policies,
                artifact_registry=self._artifact_registry,
                resource_registry=self._resource_registry,
            )
            pins = []
            seen = set()
            for spec in ordered:
                key = (spec.dependency.kind, spec.provider_id)
                if key in seen:
                    continue
                seen.add(key)
                if isinstance(spec, ArtifactSpec):
                    registration = self._artifact_registry.resolve(spec.provider_id)
                else:
                    registration = self._resource_registry.resolve(spec.provider_id)
                pins.append(
                    _ProviderPin(
                        dependency=spec.dependency,
                        provider_id=spec.provider_id,
                        kind=spec.dependency.kind,
                        registration=registration,
                    )
                )
            return ordered, _PinnedRegistrations(
                self._artifact_registry,
                self._resource_registry,
                pins,
            )

    def _artifact_dir(self, run_id: str, spec: EnvironmentSpec) -> str:
        candidate = os.path.normpath(
            os.path.join(
                self._artifact_root,
                _safe_path_segment(run_id, "run_id"),
                "providers",
                _safe_path_segment(spec.provider_id, "provider_id"),
                spec.dependency.kind.value,
                _safe_path_segment(spec.dependency.instance_id, "instance_id"),
            )
        )
        if not _path_is_contained(self._artifact_root, candidate):
            raise EnvironmentArtifactPathError(spec.dependency, "lexical path escapes the artifact root")
        if _contains_existing_symlink(self._artifact_root, candidate):
            raise EnvironmentArtifactPathError(
                spec.dependency,
                "an existing lifecycle-owned path component is a symbolic link",
            )
        resolved_candidate = os.path.realpath(candidate)
        if not _path_is_contained(self._artifact_root, resolved_candidate):
            raise EnvironmentArtifactPathError(spec.dependency, "resolved path escapes the artifact root")
        return resolved_candidate

    def _dependency_services(
        self,
        spec: EnvironmentSpec,
        artifacts: Mapping[str, ResolvedArtifact],
        resources: Mapping[str, ProvisionedResource],
    ) -> Mapping[str, Mapping[str, Any]]:
        scoped = {}
        # Service access is deliberately explicit. References infer ordering
        # for output resolution, but never grant access to runtime services.
        for raw_dependency in spec.depends_on:
            dependency = parse_environment_dependency(raw_dependency)
            if dependency.kind is EnvironmentKind.ARTIFACT:
                result: EnvironmentResult = artifacts[dependency.instance_id]
            else:
                result = resources[dependency.instance_id]
            scoped[dependency.path] = result.services
        return MappingProxyType(scoped)

    def _context(
        self,
        *,
        run_id: str,
        seed: int,
        deadline: Optional[float],
        spec: EnvironmentSpec,
        phase: EnvironmentLifecyclePhase,
        artifacts: Mapping[str, ResolvedArtifact],
        resources: Mapping[str, ProvisionedResource],
        report: _ReportBuilder,
    ) -> _ProviderContext:
        return _ProviderContext(
            run_id=run_id,
            seed=seed,
            deadline=deadline,
            artifact_dir=self._artifact_dir(run_id, spec),
            dependency_services=self._dependency_services(spec, artifacts, resources),
            _spec=spec,
            _phase=phase,
            _report=report,
        )

    @staticmethod
    def _check_support(
        spec: EnvironmentSpec,
        provider: EnvironmentProvider,
        context: _ProviderContext,
        resolved_config: Mapping[str, Any],
    ) -> SupportDecision:
        result = provider.check_support(context, spec, resolved_config)
        decision = _require_eager_result(spec.provider_id, "check_support", result)
        if not isinstance(decision, SupportDecision):
            raise ProviderContractError(
                spec.provider_id,
                "check_support must return SupportDecision",
            )
        return decision

    @staticmethod
    def _acquire_result(
        spec: EnvironmentSpec,
        provider: EnvironmentProvider,
        context: _ProviderContext,
        resolved_config: Mapping[str, Any],
        registration: EnvironmentProviderRegistration,
    ) -> EnvironmentResult:
        if isinstance(spec, ArtifactSpec):
            result = provider.resolve(context, spec, resolved_config)
            result = _require_eager_result(spec.provider_id, "resolve", result)
            assert isinstance(registration, ArtifactProviderRegistration)
            registration.validate_result(spec, result)
        else:
            result = provider.provision(context, spec, resolved_config)
            result = _require_eager_result(spec.provider_id, "provision", result)
            assert isinstance(registration, ResourceProviderRegistration)
            registration.validate_result(spec, result)
        return result

    def acquire(
        self,
        artifacts: Sequence[ArtifactSpec],
        resources: Sequence[ResourceSpec],
        *,
        run_id: str,
        seed: int,
        deadline: Optional[float] = None,
        policies: Sequence[PolicySpec] = (),
    ) -> EnvironmentSession:
        """Acquire all declarations or fully unwind before propagating failure."""

        _require_text(run_id, "run_id")
        _require_seed(seed)
        deadline = _require_deadline(deadline)
        report = _ReportBuilder(run_id)
        acquired_artifacts: dict[str, ResolvedArtifact] = {}
        acquired_resources: dict[str, ProvisionedResource] = {}
        leases: list[_AcquiredLease] = []
        rollback_failures: list[EnvironmentCleanupFailure] = []
        pins: Optional[_PinnedRegistrations] = None

        try:
            ordered, pins = self._prepare(artifacts, resources, policies)
            pins.assert_unchanged()
            for spec in ordered:
                pins.assert_unchanged()
                registration = pins.registration_for(spec)
                try:
                    resolved_config = resolve_environment_references(
                        spec.config,
                        artifacts=acquired_artifacts,
                        resources=acquired_resources,
                    )
                    registration.validate_resolved_config(spec, resolved_config)
                except BaseException as error:
                    _record_failure(
                        report,
                        spec,
                        EnvironmentLifecyclePhase.CONFIGURE,
                        error,
                    )
                    raise

                pins.assert_unchanged()
                try:
                    support_context = self._context(
                        run_id=run_id,
                        seed=seed,
                        deadline=deadline,
                        spec=spec,
                        phase=EnvironmentLifecyclePhase.SUPPORT,
                        artifacts=acquired_artifacts,
                        resources=acquired_resources,
                        report=report,
                    )
                except BaseException as error:
                    _record_failure(
                        report,
                        spec,
                        EnvironmentLifecyclePhase.CONFIGURE,
                        error,
                    )
                    raise
                try:
                    provider = _create_pinned_provider(spec, registration)
                except BaseException as error:
                    _record_failure(
                        report,
                        spec,
                        EnvironmentLifecyclePhase.FACTORY,
                        error,
                    )
                    raise
                pins.assert_unchanged()

                try:
                    decision = self._check_support(
                        spec,
                        provider,
                        support_context,
                        resolved_config,
                    )
                except BaseException as error:
                    _record_failure(
                        report,
                        spec,
                        EnvironmentLifecyclePhase.SUPPORT,
                        error,
                    )
                    raise
                report.record(
                    spec,
                    EnvironmentLifecyclePhase.SUPPORT,
                    (
                        EnvironmentLifecycleOutcome.SUCCEEDED
                        if decision.supported
                        else EnvironmentLifecycleOutcome.UNSUPPORTED
                    ),
                    {"decision": decision.to_data()},
                )
                pins.assert_unchanged()
                if not decision.supported:
                    raise UnsupportedEnvironmentError(
                        spec.dependency,
                        spec.provider_id,
                        decision,
                    )

                acquire_context = support_context.for_phase(
                    EnvironmentLifecyclePhase.ACQUIRE,
                    deadline=deadline,
                )
                try:
                    result = self._acquire_result(
                        spec,
                        provider,
                        acquire_context,
                        resolved_config,
                        registration,
                    )
                except BaseException as error:
                    _record_failure(
                        report,
                        spec,
                        EnvironmentLifecyclePhase.ACQUIRE,
                        error,
                    )
                    rollback_failures.extend(
                        _rollback_current(
                            spec,
                            provider,
                            acquire_context,
                            report,
                        )
                    )
                    raise

                lease = _AcquiredLease(
                    spec=spec,
                    provider=provider,
                    result=result,
                    context=acquire_context,
                )
                leases.append(lease)
                report.record(
                    spec,
                    EnvironmentLifecyclePhase.ACQUIRE,
                    EnvironmentLifecycleOutcome.SUCCEEDED,
                    {"result": result.to_data()},
                )
                # A validated result owns external state, so any failure after
                # this point uses cleanup (through the lease), never rollback.
                pins.assert_unchanged()
                if isinstance(spec, ArtifactSpec):
                    assert isinstance(result, ResolvedArtifact)
                    acquired_artifacts[spec.artifact_id] = result
                else:
                    assert isinstance(result, ProvisionedResource)
                    acquired_resources[spec.resource_id] = result

            pins.assert_unchanged()
            return EnvironmentSession(
                pins=pins,
                artifacts=acquired_artifacts,
                resources=acquired_resources,
                leases=leases,
                report=report,
            )
        except BaseException as primary_error:
            cleanup_failures = [
                *rollback_failures,
                *_cleanup_leases(leases, report),
            ]
            registry_drift = (
                primary_error
                if isinstance(primary_error, EnvironmentRegistryDriftError)
                else _registry_drift(pins)
                if pins is not None
                else None
            )
            if registry_drift is not None:
                drift_spec = _spec_for_drift(registry_drift, leases)
                report.record(
                    drift_spec,
                    EnvironmentLifecyclePhase.REGISTRY,
                    EnvironmentLifecycleOutcome.FAILED,
                    {"exception_type": _exception_type(registry_drift)},
                )

            status = (
                EnvironmentReportStatus.CLEANUP_FAILED
                if cleanup_failures
                else EnvironmentReportStatus.FAILED
                if registry_drift is not None
                else EnvironmentReportStatus.UNSUPPORTED
                if isinstance(primary_error, UnsupportedEnvironmentError)
                else EnvironmentReportStatus.FAILED
            )
            final_report = report.snapshot(status)
            _attach_diagnostics(
                primary_error,
                report=final_report,
                cleanup_failures=cleanup_failures,
                registry_drift=registry_drift,
            )

            teardown_base_error = _first_teardown_base_exception(cleanup_failures)
            if isinstance(primary_error, Exception) and teardown_base_error is not None:
                _attach_diagnostics(
                    teardown_base_error,
                    report=final_report,
                    cleanup_failures=cleanup_failures,
                    registry_drift=registry_drift,
                )
                raise teardown_base_error from primary_error

            if isinstance(primary_error, UnsupportedEnvironmentError) and cleanup_failures:
                cleanup_error = EnvironmentCleanupError(
                    cleanup_failures,
                    report=final_report,
                )
                cleanup_error.environment_registry_drift = registry_drift
                raise cleanup_error from primary_error
            if isinstance(primary_error, UnsupportedEnvironmentError) and registry_drift is not None:
                _attach_diagnostics(
                    registry_drift,
                    report=final_report,
                    registry_drift=registry_drift,
                )
                raise registry_drift from primary_error
            raise


__all__ = [
    "EnvironmentArtifactPathError",
    "EnvironmentBinding",
    "EnvironmentCleanupError",
    "EnvironmentCleanupFailure",
    "EnvironmentLifecycleError",
    "EnvironmentLifecycleOutcome",
    "EnvironmentLifecyclePhase",
    "EnvironmentLifecycleRecord",
    "EnvironmentManager",
    "EnvironmentProviderEvent",
    "EnvironmentRegistryDriftError",
    "EnvironmentReport",
    "EnvironmentReportStatus",
    "EnvironmentSession",
    "EnvironmentSessionClosedError",
    "EnvironmentSessionClosingError",
    "EnvironmentSessionState",
    "UnsupportedEnvironmentError",
]
