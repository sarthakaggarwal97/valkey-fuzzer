"""Acquire a plan's artifacts and resources as one unit, and release them in reverse.

Declarations are acquired in dependency order. Every declaration whose config
needs no acquired output is configured and support-checked before anything is
acquired; one that references another declaration's outputs is checked once
they exist, just before its own acquisition. If any step fails, the
declaration being acquired is rolled back and everything already acquired is
cleaned up in reverse order before the error propagates. A successful
acquisition returns an :class:`EnvironmentLease` whose ``close`` performs the
same reverse cleanup. Every step is recorded in a report-safe
:class:`EnvironmentReport`.
"""

import hashlib
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Optional, Union

from .._data import (
    EnvironmentDependency,
    collect_environment_references,
    exception_type,
    freeze_mapping,
    require_number,
    require_text,
    to_plain_data,
)
from .providers import (
    ArtifactProviderRegistry,
    EnvironmentResult,
    ProviderError,
    ProvisionedResource,
    ResolvedArtifact,
    ResourceProviderRegistry,
    resolve_environment_references,
    stable_environment_order,
)
from .specs import (
    ArtifactSpec,
    EnvironmentSpec,
    PolicySpec,
    ResourceSpec,
    SupportDecision,
)

# Report statuses.
ACQUIRED = "acquired"
CLEANED = "cleaned"
UNSUPPORTED = "unsupported"
FAILED = "failed"
CLEANUP_FAILED = "cleanup_failed"


@dataclass(frozen=True)
class EnvironmentReport:
    """Report-safe journal of one environment lifecycle.

    ``records`` hold ``dependency``, ``provider_id``, ``phase`` (configure,
    factory, support, acquire, rollback, cleanup), ``outcome`` (succeeded,
    failed, unsupported), and ``details``; ``events`` are provider-published.
    Neither ever contains a provider output value.
    """

    run_id: str
    status: str
    records: tuple[Mapping[str, Any], ...] = ()
    events: tuple[Mapping[str, Any], ...] = ()

    def to_data(self) -> Mapping[str, Any]:
        return to_plain_data(self)


class EnvironmentLifecycleError(RuntimeError):
    """A lifecycle failure; ``environment_report`` is the report at that point."""

    environment_report: Optional[EnvironmentReport] = None


class UnsupportedEnvironmentError(EnvironmentLifecycleError):
    """A provider declined a declaration before mutating anything."""

    def __init__(self, dependency: EnvironmentDependency, provider_id: str, decision: SupportDecision):
        self.dependency = dependency
        self.provider_id = provider_id
        self.decision = decision
        super().__init__(f"{dependency.path} is unsupported by provider {provider_id!r}: {decision.reason}")


class EnvironmentCleanupError(EnvironmentLifecycleError):
    """Rollback or cleanup failed after every eligible teardown was attempted."""

    def __init__(self, failures: Sequence[tuple[str, str, BaseException]]):
        self.failures = tuple(failures)
        summary = ", ".join(f"{path}:{phase}:{exception_type(error)}" for path, phase, error in self.failures)
        super().__init__(f"environment teardown failed ({summary})")


class _Journal:
    def __init__(self, run_id: str):
        self.run_id = run_id
        self.records: list[Mapping[str, Any]] = []
        self.events: list[Mapping[str, Any]] = []

    def record(self, spec: EnvironmentSpec, phase: str, outcome: str, **details: Any) -> None:
        self.records.append(
            freeze_mapping(
                {
                    "dependency": spec.dependency.path,
                    "provider_id": spec.provider_id,
                    "phase": phase,
                    "outcome": outcome,
                    "details": details,
                },
                "lifecycle record",
            )
        )

    def report(self, status: str) -> EnvironmentReport:
        return EnvironmentReport(self.run_id, status, tuple(self.records), tuple(self.events))


@dataclass(frozen=True)
class _ProviderContext:
    """The :class:`~valkey_fuzzer.environment.EnvironmentProviderContext` for one declaration."""

    run_id: str
    seed: int
    deadline: Optional[float]
    artifact_dir: str
    _spec: EnvironmentSpec = field(repr=False)
    _journal: _Journal = field(repr=False)
    _phase: str = "support"

    def publish_event(self, name: str, details: Mapping[str, Any]) -> None:
        self._journal.events.append(
            freeze_mapping(
                {
                    "dependency": self._spec.dependency.path,
                    "provider_id": self._spec.provider_id,
                    "phase": self._phase,
                    "name": require_text(name, "event name"),
                    "details": details,
                },
                "provider event",
            )
        )

    def for_phase(self, phase: str, deadline: Optional[float]) -> "_ProviderContext":
        return _ProviderContext(self.run_id, self.seed, deadline, self.artifact_dir, self._spec, self._journal, phase)


@dataclass(frozen=True)
class _Acquisition:
    spec: EnvironmentSpec
    provider: Any
    result: EnvironmentResult
    context: _ProviderContext


def _release(
    spec: EnvironmentSpec, provider: Any, phase: str, context: _ProviderContext, journal: _Journal, *args: Any
) -> Optional[tuple[str, str, BaseException]]:
    """Call ``provider.rollback`` or ``provider.cleanup``, recording the outcome."""

    try:
        getattr(provider, phase)(context.for_phase(phase, None), spec, *args)
    except BaseException as error:
        journal.record(spec, phase, "failed", exception_type=exception_type(error))
        return (spec.dependency.path, phase, error)
    journal.record(spec, phase, "succeeded")
    return None


def _cleanup(acquisitions: Sequence[_Acquisition], journal: _Journal) -> list[tuple[str, str, BaseException]]:
    failures = []
    for acquisition in reversed(acquisitions):
        failure = _release(
            acquisition.spec, acquisition.provider, "cleanup", acquisition.context, journal, acquisition.result
        )
        if failure is not None:
            failures.append(failure)
    return failures


def _raise_first_interrupt(failures: Sequence[tuple[str, str, BaseException]], cause: Optional[BaseException]) -> None:
    """Teardown never swallows a KeyboardInterrupt or similar."""

    for _, _, error in failures:
        if not isinstance(error, Exception):
            raise error from cause


class EnvironmentLease:
    """Acquired providers, owned until :meth:`close` cleans them up in reverse order."""

    def __init__(self, acquisitions: Sequence[_Acquisition], journal: _Journal):
        self._acquisitions = tuple(acquisitions)
        self._journal = journal
        self._report: Optional[EnvironmentReport] = None
        self.artifacts: Mapping[str, ResolvedArtifact] = {
            acquisition.spec.artifact_id: acquisition.result
            for acquisition in self._acquisitions
            if isinstance(acquisition.spec, ArtifactSpec)
        }
        self.resources: Mapping[str, ProvisionedResource] = {
            acquisition.spec.resource_id: acquisition.result
            for acquisition in self._acquisitions
            if isinstance(acquisition.spec, ResourceSpec)
        }

    @property
    def closed(self) -> bool:
        return self._report is not None

    @property
    def report(self) -> EnvironmentReport:
        return self._report or self._journal.report(ACQUIRED)

    def resolve(self, value: Any) -> Any:
        """Replace every environment reference in ``value`` with its acquired output."""

        if self.closed:
            raise EnvironmentLifecycleError("the environment lease is closed")
        return resolve_environment_references(value, artifacts=self.artifacts, resources=self.resources)

    def close(self) -> EnvironmentReport:
        """Clean up every acquired declaration in reverse order (idempotent)."""

        if self._report is not None:
            return self._report
        failures = _cleanup(self._acquisitions, self._journal)
        self._report = self._journal.report(CLEANUP_FAILED if failures else CLEANED)
        _raise_first_interrupt(failures, None)
        if failures:
            error = EnvironmentCleanupError(failures)
            error.environment_report = self._report
            raise error
        return self._report


def _safe_segment(value: str) -> str:
    """A readable, collision-resistant directory name for an arbitrary ID."""

    readable = re.sub(r"[^A-Za-z0-9_-]+", "-", value).strip("-_")[:40] or "value"
    return f"{readable}-{hashlib.sha256(value.encode()).hexdigest()[:12]}"


class EnvironmentProvisioner:
    """Acquires a plan's artifacts and resources with the registered providers."""

    def __init__(
        self,
        *,
        artifact_registry: ArtifactProviderRegistry,
        resource_registry: ResourceProviderRegistry,
        artifact_root: Union[str, "os.PathLike[str]"],
    ):
        root = os.fspath(artifact_root)
        if not isinstance(root, str) or not os.path.isabs(root):
            raise ValueError("artifact_root must be an absolute path")
        self.artifact_registry = artifact_registry
        self.resource_registry = resource_registry
        self.artifact_root = os.path.realpath(root)

    def _artifact_dir(self, run_id: str, spec: EnvironmentSpec) -> str:
        return os.path.join(
            self.artifact_root,
            _safe_segment(run_id),
            "providers",
            _safe_segment(spec.provider_id),
            spec.kind.value,
            _safe_segment(spec.id),
        )

    def acquire(
        self,
        artifacts: Sequence[ArtifactSpec],
        resources: Sequence[ResourceSpec],
        *,
        run_id: str,
        seed: int,
        deadline: Optional[float] = None,
        policies: Sequence[PolicySpec] = (),
    ) -> EnvironmentLease:
        """Acquire every declaration, or unwind completely before raising."""

        require_text(run_id, "run_id")
        require_number(seed, "seed", integer=True)
        journal = _Journal(run_id)
        acquisitions: list[_Acquisition] = []
        acquired: dict[str, dict[str, EnvironmentResult]] = {"artifacts": {}, "resources": {}}
        rollback_failures: list[tuple[str, str, BaseException]] = []
        try:
            ordered = stable_environment_order(
                artifacts,
                resources,
                artifact_registry=self.artifact_registry,
                resource_registry=self.resource_registry,
                policies=policies,
            )

            def check(spec: EnvironmentSpec) -> tuple[Any, Any, Mapping[str, Any], _ProviderContext]:
                registry = self.artifact_registry if isinstance(spec, ArtifactSpec) else self.resource_registry
                registration = registry.resolve(spec.provider_id)
                phase = "configure"
                try:
                    # The provider receives its schema's resolved config, read-only.
                    config = freeze_mapping(
                        registration.resolve_config(
                            spec,
                            resolve_environment_references(
                                spec.config, artifacts=acquired["artifacts"], resources=acquired["resources"]
                            ),
                        ),
                        f"{spec.dependency.path} config",
                    )
                    context = _ProviderContext(run_id, seed, deadline, self._artifact_dir(run_id, spec), spec, journal)
                    phase = "factory"
                    provider = registration.factory()
                    phase = "support"
                    decision = provider.check_support(context, spec, config)
                    if not isinstance(decision, SupportDecision):
                        raise ProviderError(spec.provider_id, "check_support must return a SupportDecision")
                except BaseException as error:
                    journal.record(spec, phase, "failed", exception_type=exception_type(error))
                    raise
                journal.record(
                    spec, "support", "succeeded" if decision.supported else "unsupported", decision=decision.to_data()
                )
                if not decision.supported:
                    raise UnsupportedEnvironmentError(spec.dependency, spec.provider_id, decision)
                return registration, provider, config, context

            # Decide support before acquiring anything wherever the config
            # needs no acquired output; the rest are checked once it exists.
            checked = {
                spec.dependency: check(spec) for spec in ordered if not collect_environment_references(spec.config)
            }
            for spec in ordered:
                registration, provider, config, context = checked.get(spec.dependency) or check(spec)
                acquire_context = context.for_phase("acquire", deadline)
                try:
                    result = getattr(provider, registration.acquire_method)(acquire_context, spec, config)
                    registration.validate_result(spec, result)
                except BaseException as error:
                    journal.record(spec, "acquire", "failed", exception_type=exception_type(error))
                    failure = _release(spec, provider, "rollback", acquire_context, journal)
                    if failure is not None:
                        rollback_failures.append(failure)
                    raise
                # From here on the provider owns external state: failures use cleanup, not rollback.
                acquisitions.append(_Acquisition(spec, provider, result, acquire_context))
                journal.record(spec, "acquire", "succeeded", result=result.to_data())
                acquired[spec.kind.value][spec.id] = result
        except BaseException as error:
            failures = rollback_failures + _cleanup(acquisitions, journal)
            if failures:
                status = CLEANUP_FAILED
            elif isinstance(error, UnsupportedEnvironmentError):
                status = UNSUPPORTED
            else:
                status = FAILED
            report = journal.report(status)
            if isinstance(error, Exception):
                _raise_first_interrupt(failures, error)
            if isinstance(error, UnsupportedEnvironmentError) and failures:
                # A teardown failure outranks "unsupported": the host may be dirty.
                cleanup_error = EnvironmentCleanupError(failures)
                cleanup_error.environment_report = report
                raise cleanup_error from error
            try:
                error.environment_report = report
            except AttributeError:
                pass
            raise
        return EnvironmentLease(acquisitions, journal)
