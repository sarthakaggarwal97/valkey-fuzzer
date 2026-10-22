"""Runtime-only materialization of an acquired environment and a run plan.

The declarative :class:`~src.extensibility.RunPlan` remains the durable,
reportable description of a run.  The models in this module contain resolved
provider outputs and live service handles, so they deliberately provide no
data or JSON serialization API and reject pickling.
"""

# ruff: noqa: UP045

from __future__ import annotations

import math
from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Optional

from .environment import EnvironmentReference, PolicySpec, ProvisionedResource, ResolvedArtifact
from .environment_lifecycle import (
    EnvironmentBinding,
    EnvironmentSessionClosedError,
    EnvironmentSessionClosingError,
)
from .extensibility import (
    ComponentKind,
    ComponentSpec,
    EvidenceRequirement,
    NodeSelector,
    RunMode,
    RunPlan,
    StartupOverlay,
)


class BoundPlanError(ValueError):
    """Base class for invalid runtime-only plan materialization."""


class UnresolvedBoundPlanReferenceError(BoundPlanError):
    """A typed environment reference survived runtime materialization."""

    def __init__(self, location: str, reference: EnvironmentReference):
        self.location = location
        self.reference = reference
        super().__init__(f"{location} still contains unresolved environment reference {reference.path!r}")


class BoundPlanBindingMismatchError(BoundPlanError):
    """An environment binding does not belong to the source plan declarations."""

    def __init__(
        self,
        *,
        kind: str,
        expected: Sequence[str],
        actual: Sequence[str],
    ):
        self.kind = kind
        self.expected = tuple(expected)
        self.actual = tuple(actual)
        missing = tuple(item for item in self.expected if item not in self.actual)
        unexpected = tuple(item for item in self.actual if item not in self.expected)
        details = []
        if missing:
            details.append(f"missing={missing!r}")
        if unexpected:
            details.append(f"unexpected={unexpected!r}")
        super().__init__(f"environment binding {kind} do not match the source plan ({', '.join(details)})")


class BoundPlanExpiredError(BoundPlanError):
    """A runtime-only bound value was accessed after environment cleanup."""


class _RuntimeOnly:
    """Common non-serialization and immutability behavior."""

    __slots__ = ()

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __delattr__(self, name: str) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __reduce_ex__(self, protocol: int) -> Any:
        del protocol
        raise TypeError(f"{type(self).__name__} is runtime-only and cannot be serialized")


class _BoundLease:
    __slots__ = ("_binding",)

    def __init__(self, binding: Optional[EnvironmentBinding] = None):
        if binding is not None and not isinstance(binding, EnvironmentBinding):
            raise TypeError("binding must be an EnvironmentBinding or None")
        self._binding = binding

    def ensure_active(self) -> None:
        if self._binding is None:
            return
        try:
            self._binding.assert_active()
        except (EnvironmentSessionClosingError, EnvironmentSessionClosedError) as error:
            raise BoundPlanExpiredError(
                "runtime-only bound values are unavailable after environment cleanup"
            ) from error


class _LeaseMapping(_RuntimeOnly, Mapping[str, Any]):
    """Immutable mapping view whose every operation checks the environment lease."""

    __slots__ = ("_data", "_lease", "_location")

    def __init__(
        self,
        data: Mapping[str, Any],
        lease: _BoundLease,
        location: str,
    ):
        object.__setattr__(self, "_data", dict(data))
        object.__setattr__(self, "_lease", lease)
        object.__setattr__(self, "_location", location)

    def __getitem__(self, key: str) -> Any:
        self._lease.ensure_active()
        return self._data[key]

    def __iter__(self) -> Iterator[str]:
        keys = tuple(self._data)
        for key in keys:
            self._lease.ensure_active()
            yield key

    def __len__(self) -> int:
        self._lease.ensure_active()
        return len(self._data)

    def __eq__(self, other: object) -> bool:
        self._lease.ensure_active()
        if not isinstance(other, Mapping):
            return False
        return dict(self.items()) == dict(other.items())

    def __repr__(self) -> str:
        return f"_LeaseMapping(location={self._location!r}, values=<redacted>)"


class _LeaseSequence(_RuntimeOnly, Sequence[Any]):
    """Immutable sequence view whose every operation checks the environment lease."""

    __slots__ = ("_data", "_lease", "_location")

    def __init__(
        self,
        data: Sequence[Any],
        lease: _BoundLease,
        location: str,
    ):
        object.__setattr__(self, "_data", tuple(data))
        object.__setattr__(self, "_lease", lease)
        object.__setattr__(self, "_location", location)

    def __getitem__(self, index):
        self._lease.ensure_active()
        return self._data[index]

    def __iter__(self) -> Iterator[Any]:
        for item in self._data:
            self._lease.ensure_active()
            yield item

    def __len__(self) -> int:
        self._lease.ensure_active()
        return len(self._data)

    def __eq__(self, other: object) -> bool:
        self._lease.ensure_active()
        if isinstance(other, (str, bytes, bytearray, Mapping)) or not isinstance(other, Sequence):
            return False
        return tuple(self) == tuple(other)

    def __repr__(self) -> str:
        return f"_LeaseSequence(location={self._location!r}, values=<redacted>)"


def _require_text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{field_name} must be a non-empty string without surrounding whitespace")
    return value


def _string_tuple(value: Sequence[str], field_name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes, bytearray, Mapping)) or not isinstance(value, Sequence):
        raise TypeError(f"{field_name} must be an ordered sequence")
    entries = tuple(value)
    for entry in entries:
        _require_text(entry, f"{field_name} entry")
    return entries


def _assert_no_environment_references(value: Any, location: str) -> None:
    if isinstance(value, EnvironmentReference):
        raise UnresolvedBoundPlanReferenceError(location, value)
    if isinstance(value, Mapping):
        for key, item in value.items():
            _assert_no_environment_references(item, f"{location}.{key}")
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _assert_no_environment_references(item, f"{location}[{index}]")


def _freeze_runtime_value(
    value: Any,
    location: str,
    lease: _BoundLease,
) -> Any:
    """Freeze one resolved executable value without logging its contents."""

    if isinstance(value, EnvironmentReference):
        raise UnresolvedBoundPlanReferenceError(location, value)
    if isinstance(value, Mapping):
        frozen = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{location} mapping keys must be strings")
            frozen[key] = _freeze_runtime_value(item, f"{location}.{key}", lease)
        return _LeaseMapping(frozen, lease, location)
    if isinstance(value, (list, tuple)):
        return _LeaseSequence(
            tuple(_freeze_runtime_value(item, f"{location}[{index}]", lease) for index, item in enumerate(value)),
            lease,
            location,
        )
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{location} cannot contain NaN or infinite numbers")
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise TypeError(f"{location} must contain only resolved JSON/YAML-safe values; got {type(value).__name__}")


def _freeze_runtime_mapping(
    value: Mapping[str, Any],
    location: str,
    lease: _BoundLease,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{location} must be a mapping")
    result = _freeze_runtime_value(value, location, lease)
    assert isinstance(result, Mapping)
    return result


def _freeze_service_namespace(
    value: Mapping[str, Any],
    location: str,
    lease: _BoundLease,
) -> Mapping[str, Any]:
    """Take an immutable namespace snapshot while retaining opaque handles."""

    if not isinstance(value, Mapping):
        raise TypeError(f"{location} must be a mapping")
    frozen = {}
    for name, service in value.items():
        _require_text(name, f"{location} service name")
        # Service values are intentionally opaque. In particular, mapping-like
        # clients must not be traversed or copied. A reference as the service
        # value itself is still always a missed materialization boundary.
        if isinstance(service, EnvironmentReference):
            raise UnresolvedBoundPlanReferenceError(f"{location}.{name}", service)
        frozen[name] = service
    return _LeaseMapping(frozen, lease, location)


class RunServices(_RuntimeOnly, Mapping[str, Any]):
    """Immutable namespaces for live services used during one run.

    Namespace mappings are immutable snapshots.  Their values are provider- or
    topology-owned live objects and retain their own lifecycle and mutability.
    Artifact and resource services are keyed first by declaration ID, while
    topology services use the topology provider's flat service namespace.
    """

    __slots__ = ("_artifacts", "_lease", "_resources", "_topology")

    def __init__(
        self,
        *,
        artifacts: Optional[Mapping[str, Mapping[str, Any]]] = None,
        resources: Optional[Mapping[str, Mapping[str, Any]]] = None,
        topology: Optional[Mapping[str, Any]] = None,
        _lease: Optional[_BoundLease] = None,
    ):
        lease = _BoundLease() if _lease is None else _lease
        if not isinstance(lease, _BoundLease):
            raise TypeError("_lease must be a bound-plan lease")
        object.__setattr__(self, "_lease", lease)
        object.__setattr__(
            self,
            "_artifacts",
            self._freeze_declaration_namespaces(
                {} if artifacts is None else artifacts,
                "services.artifacts",
                lease,
            ),
        )
        object.__setattr__(
            self,
            "_resources",
            self._freeze_declaration_namespaces(
                {} if resources is None else resources,
                "services.resources",
                lease,
            ),
        )
        object.__setattr__(
            self,
            "_topology",
            _freeze_service_namespace(
                {} if topology is None else topology,
                "services.topology",
                lease,
            ),
        )

    @staticmethod
    def _freeze_declaration_namespaces(
        value: Mapping[str, Mapping[str, Any]],
        location: str,
        lease: _BoundLease,
    ) -> Mapping[str, Mapping[str, Any]]:
        if not isinstance(value, Mapping):
            raise TypeError(f"{location} must be a mapping")
        frozen = {}
        for instance_id, services in value.items():
            _require_text(instance_id, f"{location} instance ID")
            frozen[instance_id] = _freeze_service_namespace(
                services,
                f"{location}.{instance_id}",
                lease,
            )
        return _LeaseMapping(frozen, lease, location)

    @classmethod
    def from_binding(
        cls,
        binding: EnvironmentBinding,
        *,
        artifact_ids: Sequence[str],
        resource_ids: Sequence[str],
        topology: Optional[Mapping[str, Any]] = None,
        _lease: Optional[_BoundLease] = None,
    ) -> RunServices:
        if not isinstance(binding, EnvironmentBinding):
            raise TypeError("binding must be an EnvironmentBinding")
        lease = _BoundLease(binding) if _lease is None else _lease
        if not isinstance(lease, _BoundLease):
            raise TypeError("_lease must be a bound-plan lease")
        lease.ensure_active()
        expected_artifacts = _string_tuple(artifact_ids, "artifact_ids")
        expected_resources = _string_tuple(resource_ids, "resource_ids")
        artifacts = binding.artifacts
        resources = binding.resources
        _require_exact_binding_ids(
            kind="artifacts",
            expected=expected_artifacts,
            actual=tuple(artifacts),
        )
        _require_exact_binding_ids(
            kind="resources",
            expected=expected_resources,
            actual=tuple(resources),
        )

        artifact_services = {}
        for artifact_id in expected_artifacts:
            artifact = artifacts[artifact_id]
            if not isinstance(artifact, ResolvedArtifact):
                raise TypeError(f"binding artifact {artifact_id!r} must be a ResolvedArtifact")
            if artifact.artifact_id != artifact_id:
                raise BoundPlanBindingMismatchError(
                    kind="artifacts",
                    expected=expected_artifacts,
                    actual=tuple(item.artifact_id for item in artifacts.values()),
                )
            artifact_services[artifact_id] = artifact.services

        resource_services = {}
        for resource_id in expected_resources:
            resource = resources[resource_id]
            if not isinstance(resource, ProvisionedResource):
                raise TypeError(f"binding resource {resource_id!r} must be a ProvisionedResource")
            if resource.resource_id != resource_id:
                raise BoundPlanBindingMismatchError(
                    kind="resources",
                    expected=expected_resources,
                    actual=tuple(item.resource_id for item in resources.values()),
                )
            resource_services[resource_id] = resource.services

        return cls(
            artifacts=artifact_services,
            resources=resource_services,
            topology=topology,
            _lease=lease,
        )

    @property
    def artifacts(self) -> Mapping[str, Mapping[str, Any]]:
        self._lease.ensure_active()
        return self._artifacts

    @property
    def resources(self) -> Mapping[str, Mapping[str, Any]]:
        self._lease.ensure_active()
        return self._resources

    @property
    def topology(self) -> Mapping[str, Any]:
        self._lease.ensure_active()
        return self._topology

    def artifact(self, artifact_id: str) -> Mapping[str, Any]:
        self._lease.ensure_active()
        _require_text(artifact_id, "artifact_id")
        return self._artifacts[artifact_id]

    def resource(self, resource_id: str) -> Mapping[str, Any]:
        self._lease.ensure_active()
        _require_text(resource_id, "resource_id")
        return self._resources[resource_id]

    def __getitem__(self, namespace: str) -> Mapping[str, Any]:
        self._lease.ensure_active()
        if namespace == "artifacts":
            return self._artifacts
        if namespace == "resources":
            return self._resources
        if namespace == "topology":
            return self._topology
        raise KeyError(namespace)

    def __iter__(self):
        for namespace in ("artifacts", "resources", "topology"):
            self._lease.ensure_active()
            yield namespace

    def __len__(self) -> int:
        self._lease.ensure_active()
        return 3

    def __eq__(self, other: object) -> bool:
        return self is other

    def __ne__(self, other: object) -> bool:
        return self is not other

    __hash__ = object.__hash__

    def __repr__(self) -> str:
        return (
            "RunServices("
            f"artifacts={len(self._artifacts._data)}, "
            f"resources={len(self._resources._data)}, "
            f"topology={len(self._topology._data)}, "
            "values=<redacted>)"
        )


def _require_exact_binding_ids(
    *,
    kind: str,
    expected: Sequence[str],
    actual: Sequence[str],
) -> None:
    expected_entries = tuple(expected)
    actual_entries = tuple(actual)
    if set(expected_entries) != set(actual_entries):
        raise BoundPlanBindingMismatchError(
            kind=kind,
            expected=expected_entries,
            actual=actual_entries,
        )


class BoundComponentSpec(_RuntimeOnly):
    """Executable component declaration with a fully resolved configuration."""

    __slots__ = (
        "_component_id",
        "_config",
        "_depends_on",
        "_instance_id",
        "_kind",
        "_lease",
        "_required",
        "_timeout_s",
        "_when_event",
    )

    def __init__(
        self,
        *,
        kind: ComponentKind,
        component_id: str,
        instance_id: str,
        config: Mapping[str, Any],
        required: bool,
        depends_on: Sequence[str],
        when_event: Optional[str],
        timeout_s: Optional[float],
        _lease: Optional[_BoundLease] = None,
    ):
        lease = _BoundLease() if _lease is None else _lease
        if not isinstance(lease, _BoundLease):
            raise TypeError("_lease must be a bound-plan lease")
        lease.ensure_active()
        if isinstance(kind, str):
            kind = ComponentKind(kind)
        if not isinstance(kind, ComponentKind):
            raise TypeError("kind must be a ComponentKind")
        _require_text(component_id, "component_id")
        _require_text(instance_id, "instance_id")
        if not isinstance(required, bool):
            raise TypeError("required must be a boolean")
        dependencies = _string_tuple(depends_on, "depends_on")
        if when_event is not None:
            _require_text(when_event, "when_event")
        if timeout_s is not None and (
            isinstance(timeout_s, bool)
            or not isinstance(timeout_s, (int, float))
            or not math.isfinite(timeout_s)
            or timeout_s <= 0
        ):
            raise ValueError("timeout_s must be a finite number greater than zero")

        object.__setattr__(self, "_lease", lease)
        object.__setattr__(self, "_kind", kind)
        object.__setattr__(self, "_component_id", component_id)
        object.__setattr__(self, "_instance_id", instance_id)
        object.__setattr__(
            self,
            "_config",
            _freeze_runtime_mapping(
                config,
                f"components.{instance_id}.config",
                lease,
            ),
        )
        object.__setattr__(self, "_required", required)
        object.__setattr__(self, "_depends_on", dependencies)
        object.__setattr__(self, "_when_event", when_event)
        object.__setattr__(self, "_timeout_s", timeout_s)

    @classmethod
    def materialize(
        cls,
        source: ComponentSpec,
        binding: EnvironmentBinding,
        *,
        _lease: Optional[_BoundLease] = None,
    ) -> BoundComponentSpec:
        if not isinstance(source, ComponentSpec):
            raise TypeError("source must be a ComponentSpec")
        if not isinstance(binding, EnvironmentBinding):
            raise TypeError("binding must be an EnvironmentBinding")
        lease = _BoundLease(binding) if _lease is None else _lease
        return cls(
            kind=source.kind,
            component_id=source.component_id,
            instance_id=source.instance_id,
            config=binding.resolve(source.config),
            required=source.required,
            depends_on=source.depends_on,
            when_event=source.when_event,
            timeout_s=source.timeout_s,
            _lease=lease,
        )

    @property
    def kind(self) -> ComponentKind:
        self._lease.ensure_active()
        return self._kind

    @property
    def component_id(self) -> str:
        self._lease.ensure_active()
        return self._component_id

    @property
    def instance_id(self) -> str:
        self._lease.ensure_active()
        return self._instance_id

    @property
    def config(self) -> Mapping[str, Any]:
        self._lease.ensure_active()
        return self._config

    @property
    def required(self) -> bool:
        self._lease.ensure_active()
        return self._required

    @property
    def depends_on(self) -> tuple[str, ...]:
        self._lease.ensure_active()
        return self._depends_on

    @property
    def when_event(self) -> Optional[str]:
        self._lease.ensure_active()
        return self._when_event

    @property
    def timeout_s(self) -> Optional[float]:
        self._lease.ensure_active()
        return self._timeout_s

    def __repr__(self) -> str:
        return (
            "BoundComponentSpec("
            f"kind={self._kind.value!r}, "
            f"component_id={self._component_id!r}, "
            f"instance_id={self._instance_id!r}, "
            "config=<redacted>)"
        )


class BoundNodeSelector(_RuntimeOnly):
    """Resolved selector used by a bound startup overlay."""

    __slots__ = ("_labels", "_lease", "_node_ids", "_roles")

    def __init__(
        self,
        *,
        roles: Sequence[str],
        node_ids: Sequence[str],
        labels: Mapping[str, Any],
        _lease: Optional[_BoundLease] = None,
    ):
        lease = _BoundLease() if _lease is None else _lease
        if not isinstance(lease, _BoundLease):
            raise TypeError("_lease must be a bound-plan lease")
        lease.ensure_active()
        object.__setattr__(self, "_lease", lease)
        object.__setattr__(self, "_roles", _string_tuple(roles, "roles"))
        object.__setattr__(self, "_node_ids", _string_tuple(node_ids, "node_ids"))
        object.__setattr__(
            self,
            "_labels",
            _freeze_runtime_mapping(
                labels,
                "startup overlay selector labels",
                lease,
            ),
        )

    @classmethod
    def materialize(
        cls,
        source: NodeSelector,
        binding: EnvironmentBinding,
        *,
        _lease: Optional[_BoundLease] = None,
    ) -> BoundNodeSelector:
        if not isinstance(source, NodeSelector):
            raise TypeError("source must be a NodeSelector")
        if not isinstance(binding, EnvironmentBinding):
            raise TypeError("binding must be an EnvironmentBinding")
        lease = _BoundLease(binding) if _lease is None else _lease
        return cls(
            roles=source.roles,
            node_ids=source.node_ids,
            labels=binding.resolve(source.labels),
            _lease=lease,
        )

    @property
    def roles(self) -> tuple[str, ...]:
        self._lease.ensure_active()
        return self._roles

    @property
    def node_ids(self) -> tuple[str, ...]:
        self._lease.ensure_active()
        return self._node_ids

    @property
    def labels(self) -> Mapping[str, Any]:
        self._lease.ensure_active()
        return self._labels

    def __repr__(self) -> str:
        return f"BoundNodeSelector(roles={self._roles!r}, node_ids={self._node_ids!r}, labels=<redacted>)"


class BoundStartupOverlay(_RuntimeOnly):
    """Startup overlay whose selector and server configuration are resolved."""

    __slots__ = (
        "_artifact",
        "_conflict_policy",
        "_lease",
        "_overlay_id",
        "_selector",
        "_server_config",
    )

    def __init__(
        self,
        *,
        overlay_id: str,
        selector: BoundNodeSelector,
        server_config: Mapping[str, Any],
        artifact: Optional[str],
        conflict_policy: str,
        _lease: Optional[_BoundLease] = None,
    ):
        lease = _BoundLease() if _lease is None else _lease
        if not isinstance(lease, _BoundLease):
            raise TypeError("_lease must be a bound-plan lease")
        lease.ensure_active()
        _require_text(overlay_id, "overlay_id")
        if not isinstance(selector, BoundNodeSelector):
            raise TypeError("selector must be a BoundNodeSelector")
        if selector._lease is not lease:
            raise ValueError("selector must share the startup overlay bound-plan lease")
        if artifact is not None:
            _require_text(artifact, "artifact")
        _require_text(conflict_policy, "conflict_policy")
        object.__setattr__(self, "_lease", lease)
        object.__setattr__(self, "_overlay_id", overlay_id)
        object.__setattr__(self, "_selector", selector)
        object.__setattr__(
            self,
            "_server_config",
            _freeze_runtime_mapping(
                server_config,
                f"startup_overlays.{overlay_id}.server_config",
                lease,
            ),
        )
        object.__setattr__(self, "_artifact", artifact)
        object.__setattr__(self, "_conflict_policy", conflict_policy)

    @classmethod
    def materialize(
        cls,
        source: StartupOverlay,
        binding: EnvironmentBinding,
        *,
        _lease: Optional[_BoundLease] = None,
    ) -> BoundStartupOverlay:
        if not isinstance(source, StartupOverlay):
            raise TypeError("source must be a StartupOverlay")
        if not isinstance(binding, EnvironmentBinding):
            raise TypeError("binding must be an EnvironmentBinding")
        lease = _BoundLease(binding) if _lease is None else _lease
        return cls(
            overlay_id=source.overlay_id,
            selector=BoundNodeSelector.materialize(
                source.selector,
                binding,
                _lease=lease,
            ),
            server_config=binding.resolve(source.server_config),
            artifact=source.artifact,
            conflict_policy=source.conflict_policy,
            _lease=lease,
        )

    @property
    def overlay_id(self) -> str:
        self._lease.ensure_active()
        return self._overlay_id

    @property
    def selector(self) -> BoundNodeSelector:
        self._lease.ensure_active()
        return self._selector

    @property
    def server_config(self) -> Mapping[str, Any]:
        self._lease.ensure_active()
        return self._server_config

    @property
    def artifact(self) -> Optional[str]:
        self._lease.ensure_active()
        return self._artifact

    @property
    def conflict_policy(self) -> str:
        self._lease.ensure_active()
        return self._conflict_policy

    def __repr__(self) -> str:
        return (
            "BoundStartupOverlay("
            f"overlay_id={self._overlay_id!r}, "
            f"selector={self._selector!r}, "
            "server_config=<redacted>, "
            f"artifact={self._artifact!r}, "
            f"conflict_policy={self._conflict_policy!r})"
        )


class BoundPolicy(_RuntimeOnly):
    """Opaque ordered policy intent with a fully resolved configuration."""

    __slots__ = ("_config", "_lease", "_policy_id", "_policy_type")

    def __init__(
        self,
        *,
        policy_id: str,
        policy_type: str,
        config: Mapping[str, Any],
        _lease: Optional[_BoundLease] = None,
    ):
        lease = _BoundLease() if _lease is None else _lease
        if not isinstance(lease, _BoundLease):
            raise TypeError("_lease must be a bound-plan lease")
        lease.ensure_active()
        _require_text(policy_id, "policy_id")
        _require_text(policy_type, "policy_type")
        object.__setattr__(self, "_lease", lease)
        object.__setattr__(self, "_policy_id", policy_id)
        object.__setattr__(self, "_policy_type", policy_type)
        object.__setattr__(
            self,
            "_config",
            _freeze_runtime_mapping(
                config,
                f"policies.{policy_id}.config",
                lease,
            ),
        )

    @classmethod
    def materialize(
        cls,
        source: PolicySpec,
        binding: EnvironmentBinding,
        *,
        _lease: Optional[_BoundLease] = None,
    ) -> BoundPolicy:
        if not isinstance(source, PolicySpec):
            raise TypeError("source must be a PolicySpec")
        if not isinstance(binding, EnvironmentBinding):
            raise TypeError("binding must be an EnvironmentBinding")
        lease = _BoundLease(binding) if _lease is None else _lease
        return cls(
            policy_id=source.policy_id,
            policy_type=source.policy_type,
            config=binding.resolve(source.config),
            _lease=lease,
        )

    @property
    def policy_id(self) -> str:
        self._lease.ensure_active()
        return self._policy_id

    @property
    def policy_type(self) -> str:
        self._lease.ensure_active()
        return self._policy_type

    @property
    def config(self) -> Mapping[str, Any]:
        self._lease.ensure_active()
        return self._config

    def __repr__(self) -> str:
        return f"BoundPolicy(policy_id={self._policy_id!r}, policy_type={self._policy_type!r}, config=<redacted>)"


class BoundRunPlan(_RuntimeOnly):
    """Immutable runtime-only view of one acquired :class:`RunPlan`.

    The source plan is intentionally retained unchanged and is the only plan
    that callers should place in reports or outcomes.  All executable surfaces
    exposed directly by this object have been resolved through the active
    :class:`EnvironmentBinding`.
    """

    __slots__ = (
        "_components",
        "_components_by_id",
        "_inputs",
        "_lease",
        "_policies",
        "_policies_by_id",
        "_services",
        "_source_plan",
        "_startup_overlays",
        "_topology_document",
    )

    def __init__(
        self,
        *,
        source_plan: RunPlan,
        inputs: Mapping[str, Any],
        components: Sequence[BoundComponentSpec],
        policies: Sequence[BoundPolicy],
        startup_overlays: Sequence[BoundStartupOverlay],
        topology_document: Mapping[str, Any],
        services: RunServices,
        _lease: Optional[_BoundLease] = None,
    ):
        if not isinstance(source_plan, RunPlan):
            raise TypeError("source_plan must be a RunPlan")
        if not isinstance(services, RunServices):
            raise TypeError("services must be RunServices")
        lease = services._lease if _lease is None else _lease
        if not isinstance(lease, _BoundLease):
            raise TypeError("_lease must be a bound-plan lease")
        if services._lease is not lease:
            raise ValueError("services must share the run-plan bound lease")
        lease.ensure_active()
        component_entries = tuple(components)
        if any(not isinstance(component, BoundComponentSpec) for component in component_entries):
            raise TypeError("components must contain BoundComponentSpec values")
        if any(component._lease is not lease for component in component_entries):
            raise ValueError("components must share the run-plan bound lease")
        component_ids = tuple(component.instance_id for component in component_entries)
        if len(set(component_ids)) != len(component_ids):
            raise ValueError("components contains duplicate instance_id values")
        if component_ids != tuple(component.instance_id for component in source_plan.components):
            raise ValueError("bound components must preserve source plan order and instance IDs")
        for source, component in zip(source_plan.components, component_entries):
            if (
                component.kind is not source.kind
                or component.component_id != source.component_id
                or component.required is not source.required
                or component.depends_on != source.depends_on
                or component.when_event != source.when_event
                or component.timeout_s != source.timeout_s
            ):
                raise ValueError(
                    f"bound component {component.instance_id!r} must preserve source plan execution metadata"
                )

        policy_entries = tuple(policies)
        if any(not isinstance(policy, BoundPolicy) for policy in policy_entries):
            raise TypeError("policies must contain BoundPolicy values")
        if any(policy._lease is not lease for policy in policy_entries):
            raise ValueError("policies must share the run-plan bound lease")
        policy_ids = tuple(policy.policy_id for policy in policy_entries)
        if len(set(policy_ids)) != len(policy_ids):
            raise ValueError("policies contains duplicate policy_id values")
        if policy_ids != tuple(policy.policy_id for policy in source_plan.policies):
            raise ValueError("bound policies must preserve source plan order and policy IDs")
        for source, policy in zip(source_plan.policies, policy_entries):
            if policy.policy_type != source.policy_type:
                raise ValueError(f"bound policy {policy.policy_id!r} must preserve source plan policy_type")

        overlay_entries = tuple(startup_overlays)
        if any(not isinstance(overlay, BoundStartupOverlay) for overlay in overlay_entries):
            raise TypeError("startup_overlays must contain BoundStartupOverlay values")
        if any(overlay._lease is not lease for overlay in overlay_entries):
            raise ValueError("startup_overlays must share the run-plan bound lease")
        overlay_ids = tuple(overlay.overlay_id for overlay in overlay_entries)
        if overlay_ids != tuple(overlay.overlay_id for overlay in source_plan.startup_overlays):
            raise ValueError("bound startup overlays must preserve source plan order and overlay IDs")
        for source, overlay in zip(source_plan.startup_overlays, overlay_entries):
            if (
                overlay.selector.roles != source.selector.roles
                or overlay.selector.node_ids != source.selector.node_ids
                or overlay.artifact != source.artifact
                or overlay.conflict_policy != source.conflict_policy
            ):
                raise ValueError(
                    f"bound startup overlay {overlay.overlay_id!r} must preserve source plan selection metadata"
                )
        expected_artifacts = tuple(spec.artifact_id for spec in source_plan.artifacts)
        expected_resources = tuple(spec.resource_id for spec in source_plan.resources)
        if tuple(services.artifacts) != expected_artifacts:
            raise BoundPlanBindingMismatchError(
                kind="artifacts",
                expected=expected_artifacts,
                actual=tuple(services.artifacts),
            )
        if tuple(services.resources) != expected_resources:
            raise BoundPlanBindingMismatchError(
                kind="resources",
                expected=expected_resources,
                actual=tuple(services.resources),
            )

        object.__setattr__(self, "_lease", lease)
        object.__setattr__(self, "_source_plan", source_plan)
        object.__setattr__(
            self,
            "_inputs",
            _freeze_runtime_mapping(inputs, "inputs", lease),
        )
        object.__setattr__(self, "_components", component_entries)
        object.__setattr__(
            self,
            "_components_by_id",
            _LeaseMapping(
                {component.instance_id: component for component in component_entries},
                lease,
                "components_by_id",
            ),
        )
        object.__setattr__(self, "_policies", policy_entries)
        object.__setattr__(
            self,
            "_policies_by_id",
            _LeaseMapping(
                {policy.policy_id: policy for policy in policy_entries},
                lease,
                "policies_by_id",
            ),
        )
        object.__setattr__(self, "_startup_overlays", overlay_entries)
        object.__setattr__(
            self,
            "_topology_document",
            _freeze_runtime_mapping(
                topology_document,
                "topology_document",
                lease,
            ),
        )
        object.__setattr__(self, "_services", services)
        self._assert_fully_bound()

    @classmethod
    def materialize(
        cls,
        source_plan: RunPlan,
        binding: EnvironmentBinding,
        *,
        topology_services: Optional[Mapping[str, Any]] = None,
    ) -> BoundRunPlan:
        if not isinstance(source_plan, RunPlan):
            raise TypeError("source_plan must be a RunPlan")
        if not isinstance(binding, EnvironmentBinding):
            raise TypeError("binding must be an EnvironmentBinding")

        lease = _BoundLease(binding)
        lease.ensure_active()
        services = RunServices.from_binding(
            binding,
            artifact_ids=tuple(spec.artifact_id for spec in source_plan.artifacts),
            resource_ids=tuple(spec.resource_id for spec in source_plan.resources),
            topology=topology_services,
            _lease=lease,
        )
        return cls(
            source_plan=source_plan,
            inputs=binding.resolve(source_plan.inputs),
            components=tuple(
                BoundComponentSpec.materialize(
                    component,
                    binding,
                    _lease=lease,
                )
                for component in source_plan.components
            ),
            policies=tuple(
                BoundPolicy.materialize(
                    policy,
                    binding,
                    _lease=lease,
                )
                for policy in source_plan.policies
            ),
            startup_overlays=tuple(
                BoundStartupOverlay.materialize(
                    overlay,
                    binding,
                    _lease=lease,
                )
                for overlay in source_plan.startup_overlays
            ),
            topology_document=binding.resolve(source_plan.topology_document),
            services=services,
            _lease=lease,
        )

    def _assert_fully_bound(self) -> None:
        _assert_no_environment_references(self.inputs, "inputs")
        _assert_no_environment_references(self.topology_document, "topology_document")
        for component in self.components:
            _assert_no_environment_references(
                component.config,
                f"components.{component.instance_id}.config",
            )
        for policy in self.policies:
            _assert_no_environment_references(
                policy.config,
                f"policies.{policy.policy_id}.config",
            )
        for overlay in self.startup_overlays:
            _assert_no_environment_references(
                overlay.selector.labels,
                f"startup_overlays.{overlay.overlay_id}.selector.labels",
            )
            _assert_no_environment_references(
                overlay.server_config,
                f"startup_overlays.{overlay.overlay_id}.server_config",
            )

    @property
    def source_plan(self) -> RunPlan:
        return self._source_plan

    @property
    def reportable_plan(self) -> RunPlan:
        """Return the unchanged declarative plan suitable for reports."""

        return self._source_plan

    @property
    def scenario_id(self) -> str:
        return self._source_plan.scenario_id

    @property
    def pack_id(self) -> str:
        return self._source_plan.pack_id

    @property
    def pack_version(self) -> str:
        return self._source_plan.pack_version

    @property
    def seed(self) -> int:
        return self._source_plan.seed

    @property
    def topology(self) -> str:
        return self._source_plan.topology

    @property
    def mode(self) -> RunMode:
        return self._source_plan.mode

    @property
    def inputs(self) -> Mapping[str, Any]:
        self._lease.ensure_active()
        return self._inputs

    @property
    def components(self) -> tuple[BoundComponentSpec, ...]:
        self._lease.ensure_active()
        return self._components

    @property
    def components_by_id(self) -> Mapping[str, BoundComponentSpec]:
        self._lease.ensure_active()
        return self._components_by_id

    @property
    def policies(self) -> tuple[BoundPolicy, ...]:
        """Policies in the exact declarative order."""

        self._lease.ensure_active()
        return self._policies

    @property
    def policies_by_id(self) -> Mapping[str, BoundPolicy]:
        """Opaque policies indexed by ID without policy-type dispatch."""

        self._lease.ensure_active()
        return self._policies_by_id

    def policy(self, policy_id: str) -> BoundPolicy:
        self._lease.ensure_active()
        _require_text(policy_id, "policy_id")
        return self._policies_by_id[policy_id]

    @property
    def startup_overlays(self) -> tuple[BoundStartupOverlay, ...]:
        self._lease.ensure_active()
        return self._startup_overlays

    @property
    def required_events(self) -> tuple[str, ...]:
        return self._source_plan.required_events

    @property
    def required_evidence(self) -> tuple[EvidenceRequirement, ...]:
        return self._source_plan.required_evidence

    @property
    def topology_document(self) -> Mapping[str, Any]:
        self._lease.ensure_active()
        return self._topology_document

    @property
    def metadata(self) -> Mapping[str, Any]:
        return self._source_plan.metadata

    @property
    def services(self) -> RunServices:
        self._lease.ensure_active()
        return self._services

    def rng(self, component_instance_id: str, stream: str = "default") -> Any:
        return self._source_plan.rng(component_instance_id, stream)

    def __repr__(self) -> str:
        return (
            "BoundRunPlan("
            f"scenario_id={self._source_plan.scenario_id!r}, "
            f"pack_id={self._source_plan.pack_id!r}, "
            f"pack_version={self._source_plan.pack_version!r}, "
            f"topology={self._source_plan.topology!r}, "
            f"components={len(self._components)}, "
            f"policies={len(self._policies)}, "
            "runtime_values=<redacted>)"
        )


__all__ = [
    "BoundComponentSpec",
    "BoundNodeSelector",
    "BoundPlanBindingMismatchError",
    "BoundPlanError",
    "BoundPlanExpiredError",
    "BoundPolicy",
    "BoundRunPlan",
    "BoundStartupOverlay",
    "RunServices",
    "UnresolvedBoundPlanReferenceError",
]
