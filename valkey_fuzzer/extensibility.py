"""Stable extension contracts for feature packs and pluggable fuzzer components.

This module deliberately has no dependency on the current DSL or execution
engine.  A future runner can consume these contracts without requiring feature
plugins to import engine internals.
"""

import hashlib
import inspect
import json
import math
import random
import re
import threading
import time
from collections import deque
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field, fields, is_dataclass
from enum import Enum
from importlib import metadata as importlib_metadata
from types import MappingProxyType
from typing import Any, Generic, Optional, Protocol, TypeVar, runtime_checkable

from .environment import (
    ENVIRONMENT_REFERENCE_DATA_KEY,
    ArtifactProviderRegistration,
    ArtifactProviderRegistry,
    ArtifactSpec,
    DuplicateProviderRegistrationError,
    EnvironmentReference,
    InvalidEnvironmentReferenceError,
    PolicySpec,
    ResourceProviderRegistration,
    ResourceProviderRegistry,
    ResourceSpec,
    environment_references_from_data,
)
from .extension_contract import EXTENSION_API_VERSION

PLUGIN_ENTRY_POINT_GROUP = "valkey_fuzzer.plugins"

_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")


def _require_text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{field_name} must be a non-empty string without surrounding whitespace")
    return value


def _require_qualified_id(value: str, field_name: str) -> str:
    _require_text(value, field_name)
    if any(character.isspace() for character in value) or not any(separator in value for separator in (".", ":", "/")):
        raise ValueError(f"{field_name} must be an opaque namespaced identifier such as 'example.component'")
    return value


def _require_name(value: str, field_name: str) -> str:
    _require_text(value, field_name)
    if not _NAME_PATTERN.fullmatch(value):
        raise ValueError(f"{field_name} contains unsupported characters: {value!r}")
    return value


def _freeze(
    value: Any,
    *,
    allow_environment_references: bool = False,
) -> Any:
    """Take a recursive immutable snapshot of common configuration values."""

    if isinstance(value, EnvironmentReference):
        if allow_environment_references:
            return value
        raise TypeError("extension data must not contain environment references")
    if isinstance(value, Mapping):
        if ENVIRONMENT_REFERENCE_DATA_KEY in value:
            raise InvalidEnvironmentReferenceError(
                value.get(ENVIRONMENT_REFERENCE_DATA_KEY),
                f"extension data contains the reserved {ENVIRONMENT_REFERENCE_DATA_KEY!r} "
                "serialization marker; pass an EnvironmentReference in memory",
            )
        frozen: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("configuration and metadata mapping keys must be strings")
            frozen[key] = _freeze(
                item,
                allow_environment_references=allow_environment_references,
            )
        return MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        return tuple(
            _freeze(
                item,
                allow_environment_references=allow_environment_references,
            )
            for item in value
        )
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("extension data cannot contain NaN or infinite numbers")
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise TypeError(
        f"extension data must contain only JSON/YAML-safe scalar, mapping, and sequence values; "
        f"got {type(value).__name__}"
    )


def _freeze_mapping(
    value: Mapping[str, Any],
    *,
    allow_environment_references: bool = False,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("expected a mapping")
    return _freeze(
        value,
        allow_environment_references=allow_environment_references,
    )


def _as_tuple(value: Sequence[Any]) -> tuple[Any, ...]:
    if isinstance(value, (str, bytes, bytearray, Mapping)) or not isinstance(value, Sequence):
        raise TypeError("expected an ordered sequence, not a string, bytes, mapping, or arbitrary iterable")
    return tuple(value)


def _reject_environment_metadata_references(
    value: Any,
    location: str = "metadata",
    *,
    authored_markers: bool = False,
) -> None:
    if isinstance(value, EnvironmentReference):
        raise ValueError(f"{location} must not contain environment references")
    if isinstance(value, Mapping):
        reference = value.get("$ref")
        if authored_markers and isinstance(reference, str) and reference.split(".", 1)[0] in {"artifacts", "resources"}:
            raise ValueError(f"{location} must not contain environment references")
        for key, item in value.items():
            _reject_environment_metadata_references(
                item,
                f"{location}.{key}",
                authored_markers=authored_markers,
            )
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _reject_environment_metadata_references(
                item,
                f"{location}[{index}]",
                authored_markers=authored_markers,
            )


def to_plain_data(value: Any) -> Any:
    """Convert extension models and snapshots to JSON/YAML-safe objects.

    ``dataclasses.asdict`` cannot traverse ``MappingProxyType`` values.  Walk
    fields deliberately instead so immutable plans, evidence, and outcomes have
    one canonical serialization path.
    """

    if isinstance(value, ProvisionedTopology):
        raise TypeError("ProvisionedTopology is runtime-only and cannot be serialized")
    if isinstance(value, EnvironmentReference):
        return {ENVIRONMENT_REFERENCE_DATA_KEY: value.path}
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value) and not isinstance(value, type):
        return {
            item.name: to_plain_data(getattr(value, item.name))
            for item in fields(value)
            if item.metadata.get("serialize", True) is not False
        }
    if isinstance(value, Mapping):
        if ENVIRONMENT_REFERENCE_DATA_KEY in value:
            raise InvalidEnvironmentReferenceError(
                value.get(ENVIRONMENT_REFERENCE_DATA_KEY),
                f"raw mappings cannot contain the reserved {ENVIRONMENT_REFERENCE_DATA_KEY!r} serialization marker",
            )
        return {key: to_plain_data(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [to_plain_data(item) for item in value]
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise TypeError(f"cannot convert {type(value).__name__} to plain extension data")


def canonical_json(value: Any) -> str:
    """Serialize extension data in a stable, replay-friendly JSON form."""

    return json.dumps(
        to_plain_data(value),
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _load_json_mapping(serialized: str, field_name: str) -> Mapping[str, Any]:
    if not isinstance(serialized, str):
        raise TypeError(f"{field_name} must be a string")

    def reject_constant(value: str) -> None:
        raise ValueError(f"{field_name} contains unsupported JSON constant {value!r}")

    def unique_object(pairs: Sequence[tuple[str, Any]]) -> Mapping[str, Any]:
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError(f"{field_name} contains duplicate object key {key!r}")
            value[key] = item
        return value

    value = json.loads(
        serialized,
        object_pairs_hook=unique_object,
        parse_constant=reject_constant,
    )
    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must decode to a mapping")
    return value


class ComponentKind(str, Enum):
    """Execution roles supported by the generic feature-pack runner."""

    CAPABILITY = "capability"
    WORKLOAD = "workload"
    ACTION = "action"
    FAULT = "fault"
    OBSERVER = "observer"
    ORACLE = "oracle"


class ComponentStatus(str, Enum):
    """Terminal result of one component invocation."""

    PASS = "PASS"
    FAIL = "FAIL"
    SKIP = "SKIP"
    ERROR = "ERROR"


class OutcomeStatus(str, Enum):
    """Terminal, trustworthy classification for a complete run."""

    PASS = "PASS"
    FAIL = "FAIL"
    SKIP_UNSUPPORTED = "SKIP_UNSUPPORTED"
    ERROR = "ERROR"


class RunMode(str, Enum):
    """Trust contract for a compiled run."""

    FEATURE = "feature"
    TOPOLOGY_SMOKE = "topology_smoke"


@dataclass(frozen=True)
class ComponentSpec:
    """One configured component instance in a feature pack."""

    kind: ComponentKind
    component_id: str
    instance_id: str
    config: Mapping[str, Any] = field(default_factory=dict)
    required: bool = True
    depends_on: tuple[str, ...] = ()
    when_event: Optional[str] = None
    timeout_s: Optional[float] = None

    def __post_init__(self) -> None:
        if isinstance(self.kind, str):
            object.__setattr__(self, "kind", ComponentKind(self.kind))
        if not isinstance(self.kind, ComponentKind):
            raise TypeError("kind must be a ComponentKind")
        _require_qualified_id(self.component_id, "component_id")
        _require_name(self.instance_id, "instance_id")
        object.__setattr__(
            self,
            "config",
            _freeze_mapping(
                self.config,
                allow_environment_references=True,
            ),
        )
        object.__setattr__(self, "depends_on", _as_tuple(self.depends_on))
        for dependency in self.depends_on:
            _require_name(dependency, "depends_on entry")
        if len(set(self.depends_on)) != len(self.depends_on):
            raise ValueError(f"component {self.instance_id!r} contains duplicate dependencies")
        if self.instance_id in self.depends_on:
            raise ValueError(f"component {self.instance_id!r} cannot depend on itself")
        if self.when_event is not None:
            _require_name(self.when_event, "when_event")
            if self.kind is ComponentKind.CAPABILITY:
                raise ValueError("capability probes cannot use when_event")
        if self.timeout_s is not None:
            if (
                isinstance(self.timeout_s, bool)
                or not isinstance(self.timeout_s, (int, float))
                or not math.isfinite(self.timeout_s)
                or self.timeout_s <= 0
            ):
                raise ValueError("timeout_s must be a finite number greater than zero")
        if not isinstance(self.required, bool):
            raise TypeError("required must be a boolean")


@dataclass(frozen=True)
class NodeSelector:
    """Declarative selector for nodes receiving a startup overlay.

    Empty criteria select every node.  Non-empty fields are combined by the
    future runner; selectors contain only data and do not import topology code.
    """

    roles: tuple[str, ...] = ()
    node_ids: tuple[str, ...] = ()
    labels: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        roles = _as_tuple(self.roles)
        node_ids = _as_tuple(self.node_ids)
        for role in roles:
            _require_name(role, "selector role")
        for node_id in node_ids:
            _require_name(node_id, "selector node_id")
        if len(set(roles)) != len(roles):
            raise ValueError("selector roles contains duplicates")
        if len(set(node_ids)) != len(node_ids):
            raise ValueError("selector node_ids contains duplicates")
        object.__setattr__(self, "roles", roles)
        object.__setattr__(self, "node_ids", node_ids)
        object.__setattr__(
            self,
            "labels",
            _freeze_mapping(
                self.labels,
                allow_environment_references=True,
            ),
        )


@dataclass(frozen=True)
class StartupOverlay:
    """Pre-start configuration or artifact assignment for selected nodes."""

    overlay_id: str
    selector: NodeSelector = field(default_factory=NodeSelector)
    server_config: Mapping[str, Any] = field(default_factory=dict)
    artifact: Optional[str] = None
    conflict_policy: str = "override"

    def __post_init__(self) -> None:
        _require_name(self.overlay_id, "overlay_id")
        if not isinstance(self.selector, NodeSelector):
            raise TypeError("selector must be a NodeSelector")
        object.__setattr__(
            self,
            "server_config",
            _freeze_mapping(
                self.server_config,
                allow_environment_references=True,
            ),
        )
        if self.artifact is not None:
            _require_name(self.artifact, "artifact")
        _require_name(self.conflict_policy, "conflict_policy")
        if not self.server_config and self.artifact is None:
            raise ValueError("a startup overlay must provide server_config or an artifact")


def _validate_component_graph(components: Sequence[ComponentSpec]) -> None:
    by_id: dict[str, ComponentSpec] = {}
    for component in components:
        if not isinstance(component, ComponentSpec):
            raise TypeError("components must contain ComponentSpec values")
        if component.instance_id in by_id:
            raise ValueError(f"duplicate component instance_id: {component.instance_id}")
        by_id[component.instance_id] = component

    for component in components:
        unknown = [dependency for dependency in component.depends_on if dependency not in by_id]
        if unknown:
            raise ValueError(f"component {component.instance_id!r} has unknown dependencies: {unknown}")

    visiting = set()
    visited = set()

    def visit(instance_id: str) -> None:
        if instance_id in visiting:
            raise ValueError(f"component dependency cycle includes {instance_id!r}")
        if instance_id in visited:
            return
        visiting.add(instance_id)
        for dependency in by_id[instance_id].depends_on:
            visit(dependency)
        visiting.remove(instance_id)
        visited.add(instance_id)

    for instance_id in by_id:
        visit(instance_id)


def _validate_startup_overlays(overlays: Sequence[StartupOverlay]) -> None:
    seen = set()
    for overlay in overlays:
        if not isinstance(overlay, StartupOverlay):
            raise TypeError("startup_overlays must contain StartupOverlay values")
        if overlay.overlay_id in seen:
            raise ValueError(f"duplicate startup overlay_id: {overlay.overlay_id}")
        seen.add(overlay.overlay_id)


@dataclass(frozen=True)
class EvidenceRequirement:
    """Minimum trusted evidence required for a feature verdict."""

    evidence_id: str
    source: Optional[str] = None
    min_count: int = 1

    def __post_init__(self) -> None:
        _require_qualified_id(self.evidence_id, "evidence_id")
        if self.source is not None:
            _require_name(self.source, "source")
        if isinstance(self.min_count, bool) or not isinstance(self.min_count, int) or self.min_count < 1:
            raise ValueError("min_count must be an integer greater than or equal to one")


def _validate_evidence_requirements(
    requirements: Sequence[EvidenceRequirement],
    components: Sequence[ComponentSpec],
) -> tuple[EvidenceRequirement, ...]:
    entries = _as_tuple(requirements)
    component_ids = {component.instance_id for component in components}
    seen = set()
    for requirement in entries:
        if not isinstance(requirement, EvidenceRequirement):
            raise TypeError("required_evidence must contain EvidenceRequirement values")
        key = (requirement.evidence_id, requirement.source)
        if key in seen:
            raise ValueError(
                f"duplicate evidence requirement for {requirement.evidence_id!r} from {requirement.source!r}"
            )
        seen.add(key)
        if requirement.source is not None and requirement.source not in component_ids:
            raise ValueError(
                f"evidence requirement {requirement.evidence_id!r} references "
                f"unknown source component {requirement.source!r}"
            )
    return entries


def _validate_run_mode(
    mode: RunMode,
    components: Sequence[ComponentSpec],
    required_evidence: Sequence[EvidenceRequirement],
) -> None:
    if mode is not RunMode.FEATURE:
        return
    if not any(component.kind is ComponentKind.ORACLE and component.required for component in components):
        raise ValueError("feature mode requires at least one required oracle")
    if not required_evidence:
        raise ValueError("feature mode requires at least one required_evidence requirement")


@dataclass(frozen=True)
class FeaturePackSpec:
    """Declarative description of a feature test."""

    pack_id: str
    version: str
    supported_topologies: tuple[str, ...]
    components: tuple[ComponentSpec, ...]
    mode: RunMode = RunMode.FEATURE
    input_schema: Mapping[str, Any] = field(default_factory=dict)
    startup_overlays: tuple[StartupOverlay, ...] = ()
    required_events: tuple[str, ...] = ()
    required_evidence: tuple[EvidenceRequirement, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_qualified_id(self.pack_id, "pack_id")
        _require_text(self.version, "version")
        if isinstance(self.mode, str):
            object.__setattr__(self, "mode", RunMode(self.mode))
        if not isinstance(self.mode, RunMode):
            raise TypeError("mode must be a RunMode")

        topologies = _as_tuple(self.supported_topologies)
        if not topologies:
            raise ValueError("supported_topologies must not be empty")
        for topology in topologies:
            _require_name(topology, "supported topology")
        if len(set(topologies)) != len(topologies):
            raise ValueError("supported_topologies contains duplicates")
        object.__setattr__(self, "supported_topologies", topologies)

        components = _as_tuple(self.components)
        _validate_component_graph(components)
        object.__setattr__(self, "components", components)

        required_events = _as_tuple(self.required_events)
        for event_name in required_events:
            _require_name(event_name, "required event")
        if len(set(required_events)) != len(required_events):
            raise ValueError("required_events contains duplicates")
        object.__setattr__(self, "required_events", required_events)
        required_evidence = _validate_evidence_requirements(self.required_evidence, components)
        object.__setattr__(self, "required_evidence", required_evidence)
        _validate_run_mode(self.mode, components, required_evidence)

        startup_overlays = _as_tuple(self.startup_overlays)
        _validate_startup_overlays(startup_overlays)
        object.__setattr__(self, "startup_overlays", startup_overlays)
        object.__setattr__(self, "input_schema", _freeze_mapping(self.input_schema))
        _reject_environment_metadata_references(
            self.metadata,
            authored_markers=True,
        )
        object.__setattr__(self, "metadata", _freeze_mapping(self.metadata))


@dataclass(frozen=True)
class TopologyProviderIdentity:
    """Persisted replay lock for one topology-provider implementation.

    ``provider_id`` identifies the logical provider. ``implementation_version``
    must change whenever replay compatibility changes. Providers may also
    publish an immutable build or content digest for stronger provenance.
    """

    provider_id: str
    implementation_version: str
    api_version: str
    implementation_digest: Optional[str] = None

    def __post_init__(self) -> None:
        _require_qualified_id(self.provider_id, "provider_id")
        _require_text(self.implementation_version, "implementation_version")
        _require_text(self.api_version, "api_version")
        if self.implementation_digest is not None:
            _require_text(self.implementation_digest, "implementation_digest")


@dataclass(frozen=True)
class RunPlan:
    """Fully resolved, replayable plan produced from a feature-pack spec."""

    scenario_id: str
    pack_id: str
    pack_version: str
    seed: int
    topology: str
    components: tuple[ComponentSpec, ...]
    topology_provider: TopologyProviderIdentity
    mode: RunMode = RunMode.FEATURE
    inputs: Mapping[str, Any] = field(default_factory=dict)
    artifacts: tuple[ArtifactSpec, ...] = ()
    resources: tuple[ResourceSpec, ...] = ()
    policies: tuple[PolicySpec, ...] = ()
    startup_overlays: tuple[StartupOverlay, ...] = ()
    required_events: tuple[str, ...] = ()
    required_evidence: tuple[EvidenceRequirement, ...] = ()
    topology_document: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_name(self.scenario_id, "scenario_id")
        _require_qualified_id(self.pack_id, "pack_id")
        _require_text(self.pack_version, "pack_version")
        if isinstance(self.seed, bool) or not isinstance(self.seed, int):
            raise TypeError("seed must be an integer")
        _require_name(self.topology, "topology")
        if not isinstance(self.topology_provider, TopologyProviderIdentity):
            raise TypeError("topology_provider must be a TopologyProviderIdentity")
        if isinstance(self.mode, str):
            object.__setattr__(self, "mode", RunMode(self.mode))
        if not isinstance(self.mode, RunMode):
            raise TypeError("mode must be a RunMode")
        components = _as_tuple(self.components)
        _validate_component_graph(components)
        object.__setattr__(self, "components", components)
        startup_overlays = _as_tuple(self.startup_overlays)
        _validate_startup_overlays(startup_overlays)
        object.__setattr__(self, "startup_overlays", startup_overlays)
        required_events = _as_tuple(self.required_events)
        for event_name in required_events:
            _require_name(event_name, "required event")
        if len(set(required_events)) != len(required_events):
            raise ValueError("required_events contains duplicates")
        object.__setattr__(self, "required_events", required_events)
        required_evidence = _validate_evidence_requirements(self.required_evidence, components)
        object.__setattr__(self, "required_evidence", required_evidence)
        _validate_run_mode(self.mode, components, required_evidence)
        object.__setattr__(
            self,
            "inputs",
            _freeze_mapping(
                self.inputs,
                allow_environment_references=True,
            ),
        )
        artifacts = _as_tuple(self.artifacts)
        if any(not isinstance(spec, ArtifactSpec) for spec in artifacts):
            raise TypeError("artifacts must contain only ArtifactSpec values")
        artifact_ids = tuple(spec.artifact_id for spec in artifacts)
        if len(set(artifact_ids)) != len(artifact_ids):
            raise ValueError("artifacts contains duplicate artifact_id values")
        object.__setattr__(self, "artifacts", artifacts)
        resources = _as_tuple(self.resources)
        if any(not isinstance(spec, ResourceSpec) for spec in resources):
            raise TypeError("resources must contain only ResourceSpec values")
        resource_ids = tuple(spec.resource_id for spec in resources)
        if len(set(resource_ids)) != len(resource_ids):
            raise ValueError("resources contains duplicate resource_id values")
        object.__setattr__(self, "resources", resources)
        policies = _as_tuple(self.policies)
        if any(not isinstance(spec, PolicySpec) for spec in policies):
            raise TypeError("policies must contain only PolicySpec values")
        policy_ids = tuple(spec.policy_id for spec in policies)
        if len(set(policy_ids)) != len(policy_ids):
            raise ValueError("policies contains duplicate policy_id values")
        object.__setattr__(self, "policies", policies)
        if not isinstance(self.topology_document, Mapping):
            raise TypeError("topology_document must be a mapping")
        topology_document = self.topology_document or {"type": self.topology}
        if topology_document.get("type") != self.topology:
            raise ValueError("topology_document.type must match topology")
        object.__setattr__(
            self,
            "topology_document",
            _freeze_mapping(
                topology_document,
                allow_environment_references=True,
            ),
        )
        _reject_environment_metadata_references(self.metadata)
        object.__setattr__(self, "metadata", _freeze_mapping(self.metadata))

    @classmethod
    def from_feature_pack(
        cls,
        spec: FeaturePackSpec,
        *,
        scenario_id: str,
        seed: int,
        topology: str,
        topology_provider: TopologyProviderIdentity,
        inputs: Optional[Mapping[str, Any]] = None,
        topology_document: Optional[Mapping[str, Any]] = None,
        artifacts: Sequence[ArtifactSpec] = (),
        resources: Sequence[ResourceSpec] = (),
        policies: Sequence[PolicySpec] = (),
    ) -> "RunPlan":
        if not isinstance(spec, FeaturePackSpec):
            raise TypeError("spec must be a FeaturePackSpec")
        _require_name(topology, "topology")
        if topology not in spec.supported_topologies:
            raise ValueError(f"topology {topology!r} is not supported by feature pack {spec.pack_id!r}")
        return cls(
            scenario_id=scenario_id,
            pack_id=spec.pack_id,
            pack_version=spec.version,
            seed=seed,
            topology=topology,
            components=spec.components,
            topology_provider=topology_provider,
            mode=spec.mode,
            inputs={} if inputs is None else inputs,
            artifacts=tuple(artifacts),
            resources=tuple(resources),
            policies=tuple(policies),
            startup_overlays=spec.startup_overlays,
            required_events=spec.required_events,
            required_evidence=spec.required_evidence,
            topology_document={"type": topology} if topology_document is None else topology_document,
            metadata=spec.metadata,
        )

    def rng(self, component_instance_id: str, stream: str = "default") -> random.Random:
        """Return an independent deterministic RNG for one component stream."""

        known_instances = {component.instance_id for component in self.components}
        if component_instance_id not in known_instances:
            raise ValueError(f"unknown component instance_id: {component_instance_id!r}")
        return random.Random(derive_component_seed(self.seed, component_instance_id, stream))

    def to_data(self) -> Mapping[str, Any]:
        return to_plain_data(self)

    def to_json(self) -> str:
        return canonical_json(self)

    @classmethod
    def from_data(cls, value: Mapping[str, Any]) -> "RunPlan":
        return run_plan_from_data(value)

    @classmethod
    def from_json(cls, serialized: str) -> "RunPlan":
        return run_plan_from_data(_load_json_mapping(serialized, "run plan JSON"))


@dataclass(frozen=True)
class Evidence:
    """Immutable path evidence or diagnostic observation."""

    evidence_id: str
    source: str
    phase: str
    observed_at: float
    value: Any
    event_sequence: Optional[int] = None
    artifacts: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_qualified_id(self.evidence_id, "evidence_id")
        _require_name(self.source, "source")
        _require_name(self.phase, "phase")
        if (
            isinstance(self.observed_at, bool)
            or not isinstance(self.observed_at, (int, float))
            or not math.isfinite(self.observed_at)
        ):
            raise TypeError("observed_at must be a finite number")
        if self.event_sequence is not None and (
            isinstance(self.event_sequence, bool) or not isinstance(self.event_sequence, int) or self.event_sequence < 1
        ):
            raise ValueError("event_sequence must be a positive integer")
        artifacts = _as_tuple(self.artifacts)
        for artifact in artifacts:
            _require_text(artifact, "artifact")
        object.__setattr__(self, "artifacts", artifacts)
        object.__setattr__(self, "value", _freeze(self.value))
        object.__setattr__(self, "metadata", _freeze_mapping(self.metadata))


@dataclass(frozen=True)
class ComponentResult:
    """Terminal result emitted by a component instance."""

    component_instance_id: str
    status: ComponentStatus
    summary: str
    evidence: tuple[Evidence, ...] = ()
    diagnostics: Mapping[str, Any] = field(default_factory=dict)
    started_at: Optional[float] = None
    ended_at: Optional[float] = None

    def __post_init__(self) -> None:
        _require_name(self.component_instance_id, "component_instance_id")
        if isinstance(self.status, str):
            object.__setattr__(self, "status", ComponentStatus(self.status))
        if not isinstance(self.status, ComponentStatus):
            raise TypeError("status must be a ComponentStatus")
        if not isinstance(self.summary, str):
            raise TypeError("summary must be a string")
        evidence = _as_tuple(self.evidence)
        if any(not isinstance(item, Evidence) for item in evidence):
            raise TypeError("evidence must contain Evidence values")
        object.__setattr__(self, "evidence", evidence)
        object.__setattr__(self, "diagnostics", _freeze_mapping(self.diagnostics))
        for field_name in ("started_at", "ended_at"):
            timestamp = getattr(self, field_name)
            if timestamp is not None and (
                isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)) or not math.isfinite(timestamp)
            ):
                raise TypeError(f"{field_name} must be a finite number")
        if self.started_at is not None and self.ended_at is not None and self.ended_at < self.started_at:
            raise ValueError("ended_at cannot be earlier than started_at")


@dataclass(frozen=True)
class RunOutcome:
    """Terminal classification and result bundle for a complete run."""

    plan: RunPlan
    status: OutcomeStatus
    summary: str
    component_results: tuple[ComponentResult, ...] = ()
    evidence: tuple[Evidence, ...] = ()
    diagnostics: Mapping[str, Any] = field(default_factory=dict)
    started_at: Optional[float] = None
    ended_at: Optional[float] = None

    def __post_init__(self) -> None:
        if not isinstance(self.plan, RunPlan):
            raise TypeError("plan must be a RunPlan")
        if isinstance(self.status, str):
            object.__setattr__(self, "status", OutcomeStatus(self.status))
        if not isinstance(self.status, OutcomeStatus):
            raise TypeError("status must be an OutcomeStatus")
        if not isinstance(self.summary, str):
            raise TypeError("summary must be a string")
        component_results = _as_tuple(self.component_results)
        if any(not isinstance(item, ComponentResult) for item in component_results):
            raise TypeError("component_results must contain ComponentResult values")
        object.__setattr__(self, "component_results", component_results)
        evidence = _as_tuple(self.evidence)
        if any(not isinstance(item, Evidence) for item in evidence):
            raise TypeError("evidence must contain Evidence values")
        object.__setattr__(self, "evidence", evidence)
        object.__setattr__(self, "diagnostics", _freeze_mapping(self.diagnostics))
        for field_name in ("started_at", "ended_at"):
            timestamp = getattr(self, field_name)
            if timestamp is not None and (
                isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)) or not math.isfinite(timestamp)
            ):
                raise TypeError(f"{field_name} must be a finite number")
        if self.started_at is not None and self.ended_at is not None and self.ended_at < self.started_at:
            raise ValueError("ended_at cannot be earlier than started_at")

    @property
    def all_evidence(self) -> tuple[Evidence, ...]:
        component_evidence = tuple(
            item for component_result in self.component_results for item in component_result.evidence
        )
        return self.evidence + component_evidence

    @property
    def success(self) -> bool:
        """Compatibility view for callers that previously consumed a boolean."""

        return self.status is OutcomeStatus.PASS

    def to_data(self) -> Mapping[str, Any]:
        return to_plain_data(self)

    def to_json(self) -> str:
        return canonical_json(self)

    @classmethod
    def from_data(cls, value: Mapping[str, Any]) -> "RunOutcome":
        return run_outcome_from_data(value)

    @classmethod
    def from_json(cls, serialized: str) -> "RunOutcome":
        return run_outcome_from_data(_load_json_mapping(serialized, "run outcome JSON"))


def derive_component_seed(seed: int, component_instance_id: str, stream: str = "default") -> int:
    """Derive a stable seed without relying on Python's process-randomized hash."""

    if isinstance(seed, bool) or not isinstance(seed, int):
        raise TypeError("seed must be an integer")
    _require_name(component_instance_id, "component_instance_id")
    _require_name(stream, "stream")
    material = f"{seed}\0{component_instance_id}\0{stream}".encode()
    return int.from_bytes(hashlib.sha256(material).digest()[:16], "big")


def _model_mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must be a mapping")
    if any(not isinstance(key, str) for key in value):
        raise TypeError(f"{field_name} keys must be strings")
    return value


def _model_sequence(value: Any, field_name: str) -> tuple[Any, ...]:
    if isinstance(value, (str, bytes, bytearray, Mapping)) or not isinstance(value, Sequence):
        raise TypeError(f"{field_name} must be an ordered sequence")
    return tuple(value)


def _model_fields(
    value: Any,
    field_name: str,
    *,
    allowed: Iterable[str],
    required: Iterable[str] = (),
) -> Mapping[str, Any]:
    mapping = _model_mapping(value, field_name)
    allowed_fields = set(allowed)
    unknown = sorted(set(mapping) - allowed_fields)
    if unknown:
        raise ValueError(f"{field_name} contains unknown fields: {unknown}")
    missing = sorted(set(required) - set(mapping))
    if missing:
        raise ValueError(f"{field_name} is missing required fields: {missing}")
    return mapping


def _component_spec_from_data(value: Any) -> ComponentSpec:
    mapping = _model_fields(
        value,
        "component",
        allowed=(
            "kind",
            "component_id",
            "instance_id",
            "config",
            "required",
            "depends_on",
            "when_event",
            "timeout_s",
        ),
        required=("kind", "component_id", "instance_id"),
    )
    return ComponentSpec(
        kind=mapping["kind"],
        component_id=mapping["component_id"],
        instance_id=mapping["instance_id"],
        config=mapping.get("config", {}),
        required=mapping.get("required", True),
        depends_on=mapping.get("depends_on", ()),
        when_event=mapping.get("when_event"),
        timeout_s=mapping.get("timeout_s"),
    )


def _node_selector_from_data(value: Any) -> NodeSelector:
    mapping = _model_fields(
        value,
        "node selector",
        allowed=("roles", "node_ids", "labels"),
    )
    return NodeSelector(
        roles=mapping.get("roles", ()),
        node_ids=mapping.get("node_ids", ()),
        labels=mapping.get("labels", {}),
    )


def _startup_overlay_from_data(value: Any) -> StartupOverlay:
    mapping = _model_fields(
        value,
        "startup overlay",
        allowed=("overlay_id", "selector", "server_config", "artifact", "conflict_policy"),
        required=("overlay_id",),
    )
    return StartupOverlay(
        overlay_id=mapping["overlay_id"],
        selector=_node_selector_from_data(mapping.get("selector", {})),
        server_config=mapping.get("server_config", {}),
        artifact=mapping.get("artifact"),
        conflict_policy=mapping.get("conflict_policy", "override"),
    )


def _evidence_requirement_from_data(value: Any) -> EvidenceRequirement:
    mapping = _model_fields(
        value,
        "evidence requirement",
        allowed=("evidence_id", "source", "min_count"),
        required=("evidence_id",),
    )
    return EvidenceRequirement(
        evidence_id=mapping["evidence_id"],
        source=mapping.get("source"),
        min_count=mapping.get("min_count", 1),
    )


def _topology_provider_identity_from_data(value: Any) -> TopologyProviderIdentity:
    mapping = _model_fields(
        value,
        "topology provider identity",
        allowed=(
            "provider_id",
            "implementation_version",
            "api_version",
            "implementation_digest",
        ),
        required=("provider_id", "implementation_version", "api_version"),
    )
    return TopologyProviderIdentity(
        provider_id=mapping["provider_id"],
        implementation_version=mapping["implementation_version"],
        api_version=mapping["api_version"],
        implementation_digest=mapping.get("implementation_digest"),
    )


def run_plan_from_data(value: Mapping[str, Any]) -> RunPlan:
    """Rebuild a validated immutable plan from its canonical data form."""

    value = environment_references_from_data(value, "run plan")
    mapping = _model_fields(
        value,
        "run plan",
        allowed=(
            "scenario_id",
            "pack_id",
            "pack_version",
            "seed",
            "topology",
            "topology_provider",
            "components",
            "mode",
            "inputs",
            "artifacts",
            "resources",
            "policies",
            "startup_overlays",
            "required_events",
            "required_evidence",
            "topology_document",
            "metadata",
        ),
        required=(
            "scenario_id",
            "pack_id",
            "pack_version",
            "seed",
            "topology",
            "topology_provider",
            "components",
        ),
    )
    components = tuple(
        _component_spec_from_data(component)
        for component in _model_sequence(mapping["components"], "run plan components")
    )
    overlays = tuple(
        _startup_overlay_from_data(overlay)
        for overlay in _model_sequence(mapping.get("startup_overlays", ()), "run plan startup_overlays")
    )
    required_evidence = tuple(
        _evidence_requirement_from_data(requirement)
        for requirement in _model_sequence(mapping.get("required_evidence", ()), "run plan required_evidence")
    )
    artifacts = tuple(
        ArtifactSpec.from_data(artifact)
        for artifact in _model_sequence(mapping.get("artifacts", ()), "run plan artifacts")
    )
    resources = tuple(
        ResourceSpec.from_data(resource)
        for resource in _model_sequence(mapping.get("resources", ()), "run plan resources")
    )
    policies = tuple(
        PolicySpec.from_data(policy) for policy in _model_sequence(mapping.get("policies", ()), "run plan policies")
    )
    return RunPlan(
        scenario_id=mapping["scenario_id"],
        pack_id=mapping["pack_id"],
        pack_version=mapping["pack_version"],
        seed=mapping["seed"],
        topology=mapping["topology"],
        components=components,
        topology_provider=_topology_provider_identity_from_data(mapping["topology_provider"]),
        mode=mapping.get("mode", RunMode.FEATURE),
        inputs=mapping.get("inputs", {}),
        artifacts=artifacts,
        resources=resources,
        policies=policies,
        startup_overlays=overlays,
        required_events=mapping.get("required_events", ()),
        required_evidence=required_evidence,
        topology_document=mapping.get("topology_document", {}),
        metadata=mapping.get("metadata", {}),
    )


def _evidence_from_data(value: Any) -> Evidence:
    mapping = _model_fields(
        value,
        "evidence",
        allowed=(
            "evidence_id",
            "source",
            "phase",
            "observed_at",
            "value",
            "event_sequence",
            "artifacts",
            "metadata",
        ),
        required=("evidence_id", "source", "phase", "observed_at", "value"),
    )
    return Evidence(
        evidence_id=mapping["evidence_id"],
        source=mapping["source"],
        phase=mapping["phase"],
        observed_at=mapping["observed_at"],
        value=mapping["value"],
        event_sequence=mapping.get("event_sequence"),
        artifacts=mapping.get("artifacts", ()),
        metadata=mapping.get("metadata", {}),
    )


def _component_result_from_data(value: Any) -> ComponentResult:
    mapping = _model_fields(
        value,
        "component result",
        allowed=(
            "component_instance_id",
            "status",
            "summary",
            "evidence",
            "diagnostics",
            "started_at",
            "ended_at",
        ),
        required=("component_instance_id", "status", "summary"),
    )
    evidence = tuple(
        _evidence_from_data(item) for item in _model_sequence(mapping.get("evidence", ()), "component result evidence")
    )
    return ComponentResult(
        component_instance_id=mapping["component_instance_id"],
        status=mapping["status"],
        summary=mapping["summary"],
        evidence=evidence,
        diagnostics=mapping.get("diagnostics", {}),
        started_at=mapping.get("started_at"),
        ended_at=mapping.get("ended_at"),
    )


def run_outcome_from_data(value: Mapping[str, Any]) -> RunOutcome:
    """Rebuild a validated immutable outcome from its canonical data form."""

    mapping = _model_fields(
        value,
        "run outcome",
        allowed=(
            "plan",
            "status",
            "summary",
            "component_results",
            "evidence",
            "diagnostics",
            "started_at",
            "ended_at",
        ),
        required=("plan", "status", "summary"),
    )
    component_results = tuple(
        _component_result_from_data(item)
        for item in _model_sequence(mapping.get("component_results", ()), "run outcome component_results")
    )
    evidence = tuple(
        _evidence_from_data(item) for item in _model_sequence(mapping.get("evidence", ()), "run outcome evidence")
    )
    return RunOutcome(
        plan=run_plan_from_data(mapping["plan"]),
        status=mapping["status"],
        summary=mapping["summary"],
        component_results=component_results,
        evidence=evidence,
        diagnostics=mapping.get("diagnostics", {}),
        started_at=mapping.get("started_at"),
        ended_at=mapping.get("ended_at"),
    )


@dataclass(frozen=True)
class LifecycleEvent:
    """One ordered event in the lifecycle journal."""

    name: str
    sequence: int
    timestamp: float
    source: str
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_name(self.name, "event name")
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, int) or self.sequence < 1:
            raise ValueError("event sequence must be a positive integer")
        if (
            isinstance(self.timestamp, bool)
            or not isinstance(self.timestamp, (int, float))
            or not math.isfinite(self.timestamp)
        ):
            raise TypeError("event timestamp must be a finite number")
        _require_name(self.source, "event source")
        object.__setattr__(self, "payload", _freeze_mapping(self.payload))


EventHandler = Callable[[LifecycleEvent], None]


@dataclass(frozen=True)
class EventSubscription:
    """Opaque handle used to remove an event subscription."""

    token: int
    event_name: str


@runtime_checkable
class EventBus(Protocol):
    """Component-facing lifecycle event stream.

    The runner owns dispatch, shutdown, and failure handling. Extensions may
    publish report-safe events, subscribe to events, and inspect only events
    whose subscriber dispatch completed successfully.
    """

    def subscribe(self, event_name: str, handler: EventHandler) -> EventSubscription: ...

    def unsubscribe(self, subscription: EventSubscription) -> bool: ...

    def publish(
        self,
        name: str,
        *,
        source: str,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> LifecycleEvent: ...

    def committed_history(self, event_name: Optional[str] = None) -> tuple[LifecycleEvent, ...]: ...


class EventDispatchError(RuntimeError):
    """A subscriber failed while handling a lifecycle event."""

    def __init__(self, event: LifecycleEvent, subscription: EventSubscription, cause: BaseException):
        self.event = event
        self.subscription = subscription
        self.cause = cause
        super().__init__(f"subscriber {subscription.token} failed while handling {event.name!r}: {cause}")


class EventDispatchAbortedError(RuntimeError):
    """An event was not dispatched because an earlier event failed."""

    def __init__(self, event: LifecycleEvent, cause: BaseException):
        self.event = event
        self.cause = cause
        failed_event = cause.event if isinstance(cause, EventDispatchError) else None
        if failed_event is None:
            reason = f"dispatch terminated with {type(cause).__name__}: {cause}"
        else:
            reason = f"earlier event {failed_event.name!r} (sequence {failed_event.sequence}) failed"
        super().__init__(f"event {event.name!r} (sequence {event.sequence}) was not dispatched because {reason}")


class EventBusQuiescedError(RuntimeError):
    """The event journal has been frozen for final verdict calculation."""


@dataclass(frozen=True)
class _Subscriber:
    subscription: EventSubscription
    handler: EventHandler


@dataclass
class _PendingEvent:
    event: LifecycleEvent
    completed: bool = False
    error: Optional[BaseException] = None


class LifecycleEventBus:
    """Synchronous event journal with deterministic FIFO and subscriber order.

    Event sequence is the synchronization contract.  The clock is injectable so
    replay tests do not depend on wall-clock time.  ``"*"`` subscriptions receive
    every event and retain their global registration order relative to exact
    subscriptions.  A publisher on another thread waits for its own event to
    finish dispatching.  A re-entrant publish from the active dispatch thread is
    queued and returns immediately so subscriber callbacks cannot deadlock.
    """

    def __init__(self, clock: Callable[[], float] = time.monotonic):
        self._clock = clock
        self._lock = threading.RLock()
        self._next_event_sequence = 1
        self._next_subscription_token = 1
        self._subscribers: dict[int, _Subscriber] = {}
        self._history: list[LifecycleEvent] = []
        self._committed_history: list[LifecycleEvent] = []
        self._pending: deque[_PendingEvent] = deque()
        self._dispatching = False
        self._dispatch_thread_id: Optional[int] = None
        self._quiesced = False
        self._condition = threading.Condition(self._lock)

    def subscribe(self, event_name: str, handler: EventHandler) -> EventSubscription:
        if event_name != "*":
            _require_name(event_name, "event_name")
        if not callable(handler):
            raise TypeError("event handler must be callable")
        with self._lock:
            if self._quiesced:
                raise EventBusQuiescedError("cannot subscribe after the event bus is quiesced")
            subscription = EventSubscription(self._next_subscription_token, event_name)
            self._next_subscription_token += 1
            self._subscribers[subscription.token] = _Subscriber(subscription, handler)
            return subscription

    def unsubscribe(self, subscription: EventSubscription) -> bool:
        with self._lock:
            return self._subscribers.pop(subscription.token, None) is not None

    def publish(
        self,
        name: str,
        *,
        source: str,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> LifecycleEvent:
        _require_name(name, "event name")
        _require_name(source, "event source")
        publisher_thread_id = threading.get_ident()
        with self._condition:
            if self._quiesced:
                raise EventBusQuiescedError("cannot publish after the event bus is quiesced")
            event = LifecycleEvent(
                name=name,
                sequence=self._next_event_sequence,
                timestamp=self._clock(),
                source=source,
                payload={} if payload is None else payload,
            )
            self._next_event_sequence += 1
            self._history.append(event)
            pending_event = _PendingEvent(event)
            self._pending.append(pending_event)
            if self._dispatching:
                if self._dispatch_thread_id == publisher_thread_id:
                    return event
                while not pending_event.completed:
                    self._condition.wait()
                if pending_event.error is not None:
                    raise pending_event.error from getattr(pending_event.error, "cause", None)
                return event
            self._dispatching = True
            self._dispatch_thread_id = publisher_thread_id

        current: Optional[_PendingEvent] = None
        try:
            while True:
                with self._condition:
                    if not self._pending:
                        self._dispatching = False
                        self._dispatch_thread_id = None
                        self._condition.notify_all()
                        return event
                    current = self._pending.popleft()
                    subscribers = tuple(
                        subscriber
                        for subscriber in self._subscribers.values()
                        if subscriber.subscription.event_name in (current.event.name, "*")
                    )
                for subscriber in subscribers:
                    try:
                        subscriber.handler(current.event)
                    except Exception as exc:
                        raise EventDispatchError(current.event, subscriber.subscription, exc) from exc
                with self._condition:
                    self._committed_history.append(current.event)
                    current.completed = True
                    self._condition.notify_all()
                current = None
        except BaseException as exc:
            with self._condition:
                if current is not None and not current.completed:
                    if isinstance(exc, EventDispatchError):
                        current.error = EventDispatchError(exc.event, exc.subscription, exc.cause)
                    else:
                        current.error = EventDispatchAbortedError(current.event, exc)
                    current.completed = True
                while self._pending:
                    aborted = self._pending.popleft()
                    aborted.error = EventDispatchAbortedError(aborted.event, exc)
                    aborted.completed = True
                self._dispatching = False
                self._dispatch_thread_id = None
                self._condition.notify_all()
            raise

    def attempted_history(self, event_name: Optional[str] = None) -> tuple[LifecycleEvent, ...]:
        """Return every journaled publish attempt, including failed dispatches."""

        if event_name is not None:
            _require_name(event_name, "event_name")
        with self._lock:
            if event_name is None:
                return tuple(self._history)
            return tuple(event for event in self._history if event.name == event_name)

    def committed_history(self, event_name: Optional[str] = None) -> tuple[LifecycleEvent, ...]:
        """Return only events whose complete subscriber dispatch succeeded."""

        if event_name is not None:
            _require_name(event_name, "event_name")
        with self._lock:
            if event_name is None:
                return tuple(self._committed_history)
            return tuple(event for event in self._committed_history if event.name == event_name)

    def history(self, event_name: Optional[str] = None) -> tuple[LifecycleEvent, ...]:
        """Backward-compatible alias for :meth:`attempted_history`."""

        return self.attempted_history(event_name)

    def quiesce(self) -> None:
        """Wait for active dispatch to finish, then reject future activity."""

        caller_thread_id = threading.get_ident()
        with self._condition:
            if self._quiesced:
                return
            if self._dispatching and self._dispatch_thread_id == caller_thread_id:
                raise RuntimeError("cannot quiesce the event bus from its dispatch thread")
            while self._dispatching:
                self._condition.wait()
            self._quiesced = True

    @property
    def quiesced(self) -> bool:
        with self._lock:
            return self._quiesced

    def replay(
        self,
        handler: EventHandler,
        *,
        event_name: Optional[str] = None,
        after_sequence: int = 0,
    ) -> None:
        if not callable(handler):
            raise TypeError("event handler must be callable")
        if event_name is not None:
            _require_name(event_name, "event_name")
        if isinstance(after_sequence, bool) or not isinstance(after_sequence, int) or after_sequence < 0:
            raise ValueError("after_sequence must be a non-negative integer")
        events = self.attempted_history(event_name)
        for event in events:
            if event.sequence > after_sequence:
                handler(event)


class ExecutableComponentSpec(Protocol):
    """Common component view accepted by validators and lifecycle methods.

    Static validation may receive a declarative :class:`ComponentSpec`;
    post-acquisition validation and execution receive its runtime-only bound
    counterpart. Both expose this feature-neutral surface.
    """

    @property
    def kind(self) -> ComponentKind: ...

    @property
    def component_id(self) -> str: ...

    @property
    def instance_id(self) -> str: ...

    @property
    def config(self) -> Mapping[str, Any]: ...

    @property
    def required(self) -> bool: ...

    @property
    def depends_on(self) -> Sequence[str]: ...

    @property
    def when_event(self) -> Optional[str]: ...

    @property
    def timeout_s(self) -> Optional[float]: ...


class ExecutablePolicySpec(Protocol):
    """Opaque policy intent common to declarative and bound plans."""

    @property
    def policy_id(self) -> str: ...

    @property
    def policy_type(self) -> str: ...

    @property
    def config(self) -> Mapping[str, Any]: ...


class ExecutableNodeSelector(Protocol):
    @property
    def roles(self) -> Sequence[str]: ...

    @property
    def node_ids(self) -> Sequence[str]: ...

    @property
    def labels(self) -> Mapping[str, Any]: ...


class ExecutableStartupOverlay(Protocol):
    @property
    def overlay_id(self) -> str: ...

    @property
    def selector(self) -> ExecutableNodeSelector: ...

    @property
    def server_config(self) -> Mapping[str, Any]: ...

    @property
    def artifact(self) -> Optional[str]: ...

    @property
    def conflict_policy(self) -> str: ...


class ExecutableRunPlan(Protocol):
    """Common plan view used by topology providers, validators, and components."""

    @property
    def scenario_id(self) -> str: ...

    @property
    def pack_id(self) -> str: ...

    @property
    def pack_version(self) -> str: ...

    @property
    def seed(self) -> int: ...

    @property
    def topology(self) -> str: ...

    @property
    def topology_provider(self) -> TopologyProviderIdentity: ...

    @property
    def mode(self) -> RunMode: ...

    @property
    def inputs(self) -> Mapping[str, Any]: ...

    @property
    def components(self) -> Sequence[ExecutableComponentSpec]: ...

    @property
    def policies(self) -> Sequence[ExecutablePolicySpec]: ...

    @property
    def startup_overlays(self) -> Sequence[ExecutableStartupOverlay]: ...

    @property
    def required_events(self) -> Sequence[str]: ...

    @property
    def required_evidence(self) -> Sequence[EvidenceRequirement]: ...

    @property
    def topology_document(self) -> Mapping[str, Any]: ...

    @property
    def metadata(self) -> Mapping[str, Any]: ...

    def rng(self, component_instance_id: str, stream: str = "default") -> random.Random: ...


@runtime_checkable
class RunContext(Protocol):
    """Runner-owned services visible to components without engine coupling."""

    @property
    def plan(self) -> ExecutableRunPlan: ...

    @property
    def event_bus(self) -> EventBus: ...

    def rng(self, component_instance_id: str, stream: str = "default") -> random.Random: ...

    @property
    def services(self) -> Mapping[str, Any]: ...

    @property
    def artifact_dir(self) -> str: ...

    @property
    def deadline(self) -> Optional[float]: ...


@dataclass(frozen=True, eq=False)
class ProvisionedTopology:
    """Exact runtime handle and namespaced services returned by a provider."""

    handle: Any = field(
        default=None,
        repr=False,
        compare=False,
        metadata={"serialize": False},
    )
    services: Mapping[str, Any] = field(
        default_factory=dict,
        repr=False,
        compare=False,
        metadata={"serialize": False},
    )

    def __post_init__(self) -> None:
        if not isinstance(self.services, Mapping):
            raise TypeError("provisioned topology services must be a mapping")
        if any(not isinstance(key, str) for key in self.services):
            raise TypeError("provisioned topology service names must be strings")
        object.__setattr__(self, "services", MappingProxyType(dict(self.services)))

    def __reduce_ex__(self, protocol: int) -> Any:
        del protocol
        raise TypeError("ProvisionedTopology is runtime-only and cannot be serialized")

    def __repr__(self) -> str:
        return f"ProvisionedTopology(handle=<redacted>, services={len(self.services)} value(s) <redacted>)"


@runtime_checkable
class TopologyProvider(Protocol):
    """Feature-neutral topology lifecycle implemented by provider plugins.

    Provider factories and constructors create only in-memory ownership state.
    External acquisition belongs in ``provision``. ``validate`` is pure. If
    provisioning fails, the runner invokes ``rollback``; after a successful
    provision it makes one final ``cleanup`` attempt with the exact returned
    object.
    """

    def validate(self, plan: ExecutableRunPlan) -> None: ...

    def provision(self, context: RunContext) -> ProvisionedTopology: ...

    def rollback(self, context: RunContext) -> None: ...

    def cleanup(self, context: RunContext, topology: ProvisionedTopology) -> None: ...


@runtime_checkable
class FeaturePack(Protocol):
    @property
    def spec(self) -> FeaturePackSpec: ...

    def build_plan(
        self,
        *,
        scenario_id: str,
        seed: int,
        topology: str,
        inputs: Mapping[str, Any],
    ) -> RunPlan: ...


@runtime_checkable
class CapabilityProbe(Protocol):
    def probe(self, context: RunContext, spec: ExecutableComponentSpec) -> ComponentResult: ...


@runtime_checkable
class Workload(Protocol):
    def start(self, context: RunContext, spec: ExecutableComponentSpec) -> ComponentResult: ...

    def stop(self, context: RunContext, spec: ExecutableComponentSpec) -> ComponentResult: ...


@runtime_checkable
class Action(Protocol):
    def run(self, context: RunContext, spec: ExecutableComponentSpec) -> ComponentResult: ...


@runtime_checkable
class Fault(Protocol):
    def arm(self, context: RunContext, spec: ExecutableComponentSpec) -> ComponentResult: ...

    def cleanup(self, context: RunContext, spec: ExecutableComponentSpec) -> ComponentResult: ...


@runtime_checkable
class Observer(Protocol):
    def start(self, context: RunContext, spec: ExecutableComponentSpec) -> ComponentResult: ...

    def stop(self, context: RunContext, spec: ExecutableComponentSpec) -> ComponentResult: ...


@runtime_checkable
class Oracle(Protocol):
    def evaluate(
        self,
        context: RunContext,
        spec: ExecutableComponentSpec,
        evidence: Sequence[Evidence],
    ) -> ComponentResult: ...


T = TypeVar("T")


@dataclass(frozen=True)
class ComponentRegistration(Generic[T]):
    """Typed factory plus compatibility metadata stored by a registry.

    Factories MUST only construct in-memory component state; external resource
    acquisition belongs in component lifecycle methods. Validators MUST be
    deterministic, side-effect-free, idempotent, and re-entrant because the
    compiler, explicit validation, and execution preflight may invoke them
    independently.
    """

    component_id: str
    kind: ComponentKind
    api_version: str
    factory: Callable[[], T]
    validator: Optional[Callable[[ExecutableComponentSpec, ExecutableRunPlan], None]] = None

    def __post_init__(self) -> None:
        _require_qualified_id(self.component_id, "component_id")
        if isinstance(self.kind, str):
            object.__setattr__(self, "kind", ComponentKind(self.kind))
        if not isinstance(self.kind, ComponentKind):
            raise TypeError("kind must be a ComponentKind")
        _require_text(self.api_version, "api_version")
        if not callable(self.factory):
            raise TypeError("component factory must be callable")
        if self.validator is not None and not callable(self.validator):
            raise TypeError("component validator must be callable")


class RegistrationError(ValueError):
    """Base class for registry contract failures."""


class DuplicateRegistrationError(RegistrationError):
    def __init__(self, component_id: str):
        self.component_id = component_id
        super().__init__(f"component {component_id!r} is already registered")


class UnknownRegistrationError(RegistrationError):
    def __init__(self, component_id: str):
        self.component_id = component_id
        super().__init__(f"component {component_id!r} is not registered")


class IncompatibleRegistrationError(RegistrationError):
    def __init__(self, component_id: str, reason: str):
        self.component_id = component_id
        self.reason = reason
        super().__init__(f"component {component_id!r} is incompatible: {reason}")


class Registry(Generic[T]):
    """Thread-safe, typed registry for one component kind and API version."""

    def __init__(self, kind: ComponentKind, api_version: str = EXTENSION_API_VERSION):
        self._kind = ComponentKind(kind)
        self._api_version = _require_text(api_version, "api_version")
        self._registrations: dict[str, ComponentRegistration[T]] = {}
        self._lock = threading.RLock()

    @property
    def kind(self) -> ComponentKind:
        return self._kind

    @property
    def api_version(self) -> str:
        return self._api_version

    def _validate_many(self, registrations: Sequence[ComponentRegistration[T]]) -> None:
        seen = set()
        for registration in registrations:
            if not isinstance(registration, ComponentRegistration):
                raise TypeError("registry entries must be ComponentRegistration values")
            if registration.kind != self.kind:
                raise IncompatibleRegistrationError(
                    registration.component_id,
                    f"kind {registration.kind.value!r} does not match registry kind {self.kind.value!r}",
                )
            if registration.api_version != self.api_version:
                raise IncompatibleRegistrationError(
                    registration.component_id,
                    f"API {registration.api_version!r} does not match registry API {self.api_version!r}",
                )
            if registration.component_id in seen or registration.component_id in self._registrations:
                raise DuplicateRegistrationError(registration.component_id)
            seen.add(registration.component_id)

    def validate_many(self, registrations: Iterable[ComponentRegistration[T]]) -> None:
        entries = tuple(registrations)
        with self._lock:
            self._validate_many(entries)

    def register(self, registration: ComponentRegistration[T]) -> None:
        self.register_many((registration,))

    def register_many(self, registrations: Iterable[ComponentRegistration[T]]) -> None:
        entries = tuple(registrations)
        with self._lock:
            self._validate_many(entries)
            for registration in entries:
                self._registrations[registration.component_id] = registration

    def unregister(self, component_id: str) -> ComponentRegistration[T]:
        _require_qualified_id(component_id, "component_id")
        with self._lock:
            try:
                return self._registrations.pop(component_id)
            except KeyError as exc:
                raise UnknownRegistrationError(component_id) from exc

    def resolve(self, component_id: str) -> ComponentRegistration[T]:
        _require_qualified_id(component_id, "component_id")
        with self._lock:
            try:
                return self._registrations[component_id]
            except KeyError as exc:
                raise UnknownRegistrationError(component_id) from exc

    def create(self, component_id: str) -> T:
        return self.resolve(component_id).factory()

    @property
    def registered_ids(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._registrations))

    def snapshot(self) -> Mapping[str, ComponentRegistration[T]]:
        with self._lock:
            return MappingProxyType(
                {component_id: self._registrations[component_id] for component_id in sorted(self._registrations)}
            )

    @property
    def registration_lock(self) -> threading.RLock:
        """Lock used by cross-registry plugin discovery transactions."""

        return self._lock

    def restore_snapshot(self, snapshot: Mapping[str, ComponentRegistration[T]]) -> None:
        """Restore a prior snapshot while a discovery transaction owns the lock."""

        if not isinstance(snapshot, Mapping):
            raise TypeError("registry snapshot must be a mapping")
        with self._lock:
            self._registrations = dict(snapshot)

    def __contains__(self, component_id: object) -> bool:
        with self._lock:
            return component_id in self._registrations

    def __len__(self) -> int:
        with self._lock:
            return len(self._registrations)


@dataclass(frozen=True)
class TopologyProviderRegistration:
    """Fresh-provider factory registered for one declarative topology ID.

    The factory is deliberately stored as extension data rather than a
    provider instance.  Topology providers own mutable lifecycle state, so a
    runtime must construct a provider for each validation or execution
    attempt.  The runner validates the returned provider against its topology
    lifecycle protocol before invoking it.
    """

    topology: str
    provider_id: str
    implementation_version: str
    api_version: str
    factory: Callable[[], TopologyProvider]
    implementation_digest: Optional[str] = None

    def __post_init__(self) -> None:
        _require_name(self.topology, "topology")
        _require_qualified_id(self.provider_id, "provider_id")
        _require_text(self.implementation_version, "implementation_version")
        _require_text(self.api_version, "api_version")
        if not callable(self.factory):
            raise TypeError("topology provider factory must be callable")
        callable_target = (
            self.factory.__call__
            if not (
                inspect.isfunction(self.factory) or inspect.ismethod(self.factory) or isinstance(self.factory, type)
            )
            else self.factory
        )
        if (
            inspect.iscoroutinefunction(callable_target)
            or inspect.isasyncgenfunction(callable_target)
            or inspect.isgeneratorfunction(callable_target)
        ):
            raise TypeError("topology provider factory must be a synchronous zero-argument callable")
        try:
            inspect.signature(self.factory).bind()
        except (TypeError, ValueError) as exc:
            raise TypeError("topology provider factory must be a synchronous zero-argument callable") from exc
        if self.implementation_digest is not None:
            _require_text(self.implementation_digest, "implementation_digest")

    @property
    def identity(self) -> TopologyProviderIdentity:
        return TopologyProviderIdentity(
            provider_id=self.provider_id,
            implementation_version=self.implementation_version,
            api_version=self.api_version,
            implementation_digest=self.implementation_digest,
        )


class TopologyProviderRegistrationError(ValueError):
    """Base class for topology-provider registry contract failures."""


class DuplicateTopologyProviderRegistrationError(TopologyProviderRegistrationError):
    def __init__(self, topology: str):
        self.topology = topology
        super().__init__(f"topology provider {topology!r} is already registered")


class UnknownTopologyProviderError(TopologyProviderRegistrationError):
    def __init__(self, topology: str):
        self.topology = topology
        super().__init__(f"topology provider {topology!r} is not registered")


class IncompatibleTopologyProviderRegistrationError(TopologyProviderRegistrationError):
    def __init__(self, topology: str, reason: str):
        self.topology = topology
        self.reason = reason
        super().__init__(f"topology provider {topology!r} is incompatible: {reason}")


class TopologyProviderRegistryPinnedError(TopologyProviderRegistrationError):
    """A mutation was attempted while execution pinned a registry snapshot."""


class TopologyProviderRegistry:
    """Thread-safe topology registry participating in extension transactions."""

    def __init__(self, api_version: str = EXTENSION_API_VERSION):
        self._api_version = _require_text(api_version, "api_version")
        self._registrations: dict[str, TopologyProviderRegistration] = {}
        self._lock = threading.RLock()
        self._pin_depth = 0
        self._pinned_snapshot: Optional[Mapping[str, TopologyProviderRegistration]] = None

    @property
    def api_version(self) -> str:
        return self._api_version

    def _validate_many(
        self,
        registrations: Sequence[TopologyProviderRegistration],
    ) -> None:
        seen = set()
        for registration in registrations:
            if not isinstance(registration, TopologyProviderRegistration):
                raise TypeError("topology provider registry entries must be TopologyProviderRegistration values")
            if registration.api_version != self.api_version:
                raise IncompatibleTopologyProviderRegistrationError(
                    registration.topology,
                    f"API {registration.api_version!r} does not match registry API {self.api_version!r}",
                )
            if registration.topology in seen or registration.topology in self._registrations:
                raise DuplicateTopologyProviderRegistrationError(registration.topology)
            seen.add(registration.topology)

    def _reject_pinned_mutation(self) -> None:
        if self._pin_depth:
            raise TopologyProviderRegistryPinnedError(
                "topology provider registry cannot be mutated during a pinned runtime operation"
            )

    def _normalized_snapshot(
        self,
        snapshot: Mapping[str, TopologyProviderRegistration],
    ) -> dict[str, TopologyProviderRegistration]:
        if not isinstance(snapshot, Mapping):
            raise TypeError("topology provider registry snapshot must be a mapping")
        normalized = {}
        for topology, registration in snapshot.items():
            _require_name(topology, "topology provider snapshot key")
            if not isinstance(registration, TopologyProviderRegistration):
                raise TypeError("topology provider registry snapshots must contain TopologyProviderRegistration values")
            if registration.topology != topology:
                raise ValueError(
                    f"topology provider snapshot key {topology!r} does not "
                    f"match registration topology {registration.topology!r}"
                )
            if registration.api_version != self.api_version:
                raise IncompatibleTopologyProviderRegistrationError(
                    topology,
                    f"API {registration.api_version!r} does not match registry API {self.api_version!r}",
                )
            normalized[topology] = registration
        return normalized

    def validate_many(
        self,
        registrations: Iterable[TopologyProviderRegistration],
    ) -> None:
        entries = tuple(registrations)
        with self._lock:
            self._validate_many(entries)

    def register(self, registration: TopologyProviderRegistration) -> None:
        self.register_many((registration,))

    def register_many(
        self,
        registrations: Iterable[TopologyProviderRegistration],
    ) -> None:
        entries = tuple(registrations)
        with self._lock:
            self._reject_pinned_mutation()
            self._validate_many(entries)
            for registration in entries:
                self._registrations[registration.topology] = registration

    def unregister(self, topology: str) -> TopologyProviderRegistration:
        _require_name(topology, "topology")
        with self._lock:
            self._reject_pinned_mutation()
            try:
                return self._registrations.pop(topology)
            except KeyError as exc:
                raise UnknownTopologyProviderError(topology) from exc

    def resolve(self, topology: str) -> TopologyProviderRegistration:
        _require_name(topology, "topology")
        with self._lock:
            try:
                return self._registrations[topology]
            except KeyError as exc:
                raise UnknownTopologyProviderError(topology) from exc

    @property
    def registered_ids(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._registrations))

    def snapshot(self) -> Mapping[str, TopologyProviderRegistration]:
        with self._lock:
            return MappingProxyType(
                {topology: self._registrations[topology] for topology in sorted(self._registrations)}
            )

    @property
    def registration_lock(self) -> threading.RLock:
        return self._lock

    def restore_snapshot(
        self,
        snapshot: Mapping[str, TopologyProviderRegistration],
    ) -> None:
        normalized = self._normalized_snapshot(snapshot)
        with self._lock:
            self._reject_pinned_mutation()
            self._registrations = normalized

    def restore_runtime_snapshot(
        self,
        snapshot: Mapping[str, TopologyProviderRegistration],
    ) -> None:
        """Restore runtime-owned state while an operation pin is active."""

        normalized = self._normalized_snapshot(snapshot)
        with self._lock:
            if self._pin_depth and (
                self._pinned_snapshot is None or self._snapshot_changed(self._pinned_snapshot, normalized)
            ):
                raise TopologyProviderRegistryPinnedError(
                    "a runtime restore cannot replace the pinned topology provider snapshot"
                )
            self._registrations = normalized

    @staticmethod
    def _snapshot_changed(
        expected: Mapping[str, TopologyProviderRegistration],
        current: Mapping[str, TopologyProviderRegistration],
    ) -> bool:
        return set(current) != set(expected) or any(
            current[topology] is not registration for topology, registration in expected.items()
        )

    @contextmanager
    def pin_snapshot(
        self,
        snapshot: Mapping[str, TopologyProviderRegistration],
    ):
        """Reject mutations without holding the registry lock across callbacks."""

        normalized = self._normalized_snapshot(snapshot)
        with self._lock:
            current = self.snapshot()
            if self._snapshot_changed(normalized, current):
                raise TopologyProviderRegistryPinnedError(
                    "topology provider registry changed before its snapshot could be pinned"
                )
            if self._pinned_snapshot is not None and self._snapshot_changed(self._pinned_snapshot, normalized):
                raise TopologyProviderRegistryPinnedError(
                    "topology provider registry is already pinned to a different snapshot"
                )
            if self._pinned_snapshot is None:
                self._pinned_snapshot = MappingProxyType(normalized)
            self._pin_depth += 1
        try:
            yield
        finally:
            with self._lock:
                self._pin_depth -= 1
                if self._pin_depth == 0:
                    self._pinned_snapshot = None

    def __contains__(self, topology: object) -> bool:
        with self._lock:
            return topology in self._registrations

    def __len__(self) -> int:
        with self._lock:
            return len(self._registrations)


@dataclass(frozen=True)
class PluginManifest:
    """Value returned by an external ``valkey_fuzzer.plugins`` entry point."""

    plugin_id: str
    api_version: str
    registrations: tuple[ComponentRegistration[Any], ...] = ()
    feature_packs: tuple[FeaturePackSpec, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)
    artifact_providers: tuple[ArtifactProviderRegistration, ...] = ()
    resource_providers: tuple[ResourceProviderRegistration, ...] = ()
    topology_providers: tuple[TopologyProviderRegistration, ...] = ()

    def __post_init__(self) -> None:
        _require_qualified_id(self.plugin_id, "plugin_id")
        _require_text(self.api_version, "api_version")
        registrations = _as_tuple(self.registrations)
        registration_ids = set()
        for registration in registrations:
            if not isinstance(registration, ComponentRegistration):
                raise TypeError("registrations must contain ComponentRegistration values")
            if registration.api_version != self.api_version:
                raise ValueError(
                    f"registration {registration.component_id!r} uses API {registration.api_version!r}, "
                    f"expected {self.api_version!r}"
                )
            if registration.component_id in registration_ids:
                raise DuplicateRegistrationError(registration.component_id)
            registration_ids.add(registration.component_id)
        object.__setattr__(self, "registrations", registrations)

        feature_packs = _as_tuple(self.feature_packs)
        pack_keys = set()
        for feature_pack in feature_packs:
            if not isinstance(feature_pack, FeaturePackSpec):
                raise TypeError("feature_packs must contain FeaturePackSpec values")
            key = (feature_pack.pack_id, feature_pack.version)
            if key in pack_keys:
                raise ValueError(f"duplicate feature pack version: {feature_pack.pack_id}@{feature_pack.version}")
            pack_keys.add(key)
        object.__setattr__(self, "feature_packs", feature_packs)

        artifact_providers = _as_tuple(self.artifact_providers)
        artifact_provider_ids = set()
        for registration in artifact_providers:
            if not isinstance(registration, ArtifactProviderRegistration):
                raise TypeError("artifact_providers must contain ArtifactProviderRegistration values")
            if registration.api_version != self.api_version:
                raise ValueError(
                    f"artifact provider {registration.provider_id!r} uses API {registration.api_version!r}, "
                    f"expected {self.api_version!r}"
                )
            if registration.provider_id in artifact_provider_ids:
                raise DuplicateProviderRegistrationError(registration.provider_id)
            artifact_provider_ids.add(registration.provider_id)
        object.__setattr__(self, "artifact_providers", artifact_providers)

        resource_providers = _as_tuple(self.resource_providers)
        resource_provider_ids = set()
        for registration in resource_providers:
            if not isinstance(registration, ResourceProviderRegistration):
                raise TypeError("resource_providers must contain ResourceProviderRegistration values")
            if registration.api_version != self.api_version:
                raise ValueError(
                    f"resource provider {registration.provider_id!r} uses API {registration.api_version!r}, "
                    f"expected {self.api_version!r}"
                )
            if registration.provider_id in resource_provider_ids:
                raise DuplicateProviderRegistrationError(registration.provider_id)
            resource_provider_ids.add(registration.provider_id)
        object.__setattr__(self, "resource_providers", resource_providers)

        topology_providers = _as_tuple(self.topology_providers)
        topologies = set()
        for registration in topology_providers:
            if not isinstance(registration, TopologyProviderRegistration):
                raise TypeError("topology_providers must contain TopologyProviderRegistration values")
            if registration.api_version != self.api_version:
                raise ValueError(
                    f"topology provider {registration.topology!r} uses API "
                    f"{registration.api_version!r}, expected {self.api_version!r}"
                )
            if registration.topology in topologies:
                raise DuplicateTopologyProviderRegistrationError(registration.topology)
            topologies.add(registration.topology)
        object.__setattr__(self, "topology_providers", topology_providers)
        object.__setattr__(self, "metadata", _freeze_mapping(self.metadata))


class PluginDiscoveryError(RuntimeError):
    """An entry point could not be loaded or did not satisfy the plugin contract."""

    def __init__(self, entry_point: str, distribution: str, reason: str):
        self.entry_point = entry_point
        self.distribution = distribution
        self.reason = reason
        location = f" from {distribution!r}" if distribution else ""
        super().__init__(f"failed to discover plugin {entry_point!r}{location}: {reason}")


def _distribution_name(entry_point: Any) -> str:
    distribution = getattr(entry_point, "dist", None)
    name = getattr(distribution, "name", None)
    if name:
        return str(name)
    metadata = getattr(distribution, "metadata", None)
    if metadata is not None:
        try:
            return str(metadata["Name"])
        except (KeyError, TypeError):
            pass
    return ""


def _entry_point_sort_key(entry_point: Any) -> tuple[str, str, str]:
    return (
        _distribution_name(entry_point).casefold(),
        str(entry_point.name).casefold(),
        str(getattr(entry_point, "value", "")),
    )


def _selected_entry_points(group: str) -> tuple[Any, ...]:
    entry_points = importlib_metadata.entry_points()
    if hasattr(entry_points, "select"):
        selected = entry_points.select(group=group)
    else:  # Python 3.9 and older importlib-metadata compatibility
        selected = entry_points.get(group, ())
    return tuple(sorted(selected, key=_entry_point_sort_key))


def discover_plugins(
    registries: Mapping[ComponentKind, Registry[Any]],
    *,
    group: str = PLUGIN_ENTRY_POINT_GROUP,
    feature_pack_catalog: Optional[Any] = None,
    artifact_provider_registry: Optional[ArtifactProviderRegistry] = None,
    resource_provider_registry: Optional[ResourceProviderRegistry] = None,
    topology_provider_registry: Optional[TopologyProviderRegistry] = None,
    api_version: str = EXTENSION_API_VERSION,
) -> tuple[PluginManifest, ...]:
    """Load and register external plugins in deterministic distribution/name order.

    Entry points must resolve to either a :class:`PluginManifest` or a zero-arg
    callable returning one.  All entry points are loaded and all registrations
    are preflighted before any registry is mutated.
    """

    _require_text(group, "entry point group")
    _require_text(api_version, "api_version")

    normalized_registries: dict[ComponentKind, Registry[Any]] = {}
    for raw_kind, registry in registries.items():
        try:
            kind = ComponentKind(raw_kind)
        except (TypeError, ValueError) as exc:
            raise PluginDiscoveryError("component-registries", "", f"invalid registry kind {raw_kind!r}") from exc
        if kind in normalized_registries:
            raise PluginDiscoveryError("component-registries", "", f"duplicate registry for kind {kind.value!r}")
        if not isinstance(registry, Registry):
            raise PluginDiscoveryError(
                "component-registries",
                "",
                f"registry for kind {kind.value!r} must be a Registry",
            )
        if registry.kind != kind:
            raise PluginDiscoveryError(
                "component-registries",
                "",
                f"registry key {kind.value!r} does not match registry kind {registry.kind.value!r}",
            )
        if registry.api_version != api_version:
            raise PluginDiscoveryError(
                "component-registries",
                "",
                f"registry API {registry.api_version!r} does not match extension API {api_version!r}",
            )
        normalized_registries[kind] = registry

    if feature_pack_catalog is not None:
        if not all(
            callable(getattr(feature_pack_catalog, method, None))
            for method in ("validate_many", "register_many", "snapshot", "restore_snapshot")
        ) or not hasattr(feature_pack_catalog, "registration_lock"):
            raise PluginDiscoveryError(
                "feature-pack-catalog",
                "",
                "catalog must implement the atomic feature-pack catalog contract",
            )

    for label, registry, registry_type in (
        ("artifact-provider-registry", artifact_provider_registry, ArtifactProviderRegistry),
        ("resource-provider-registry", resource_provider_registry, ResourceProviderRegistry),
        (
            "topology-provider-registry",
            topology_provider_registry,
            TopologyProviderRegistry,
        ),
    ):
        if registry is None:
            continue
        if not isinstance(registry, registry_type):
            raise PluginDiscoveryError(label, "", f"registry must be a {registry_type.__name__}")
        if registry.api_version != api_version:
            raise PluginDiscoveryError(
                label,
                "",
                f"registry API {registry.api_version!r} does not match extension API {api_version!r}",
            )

    manifests = []
    manifest_sources = []
    for entry_point in _selected_entry_points(group):
        distribution = _distribution_name(entry_point)
        try:
            loaded = entry_point.load()
            manifest = loaded if isinstance(loaded, PluginManifest) else loaded()
            if not isinstance(manifest, PluginManifest):
                raise TypeError("entry point must return a PluginManifest")
            if manifest.plugin_id != entry_point.name:
                raise ValueError(
                    f"manifest plugin_id {manifest.plugin_id!r} does not match entry point name {entry_point.name!r}"
                )
        except Exception as exc:
            raise PluginDiscoveryError(str(entry_point.name), distribution, str(exc)) from exc
        manifests.append(manifest)
        manifest_sources.append((str(entry_point.name), distribution))

    seen_plugins = set()
    registrations_by_kind: dict[ComponentKind, list[ComponentRegistration[Any]]] = {}
    new_component_ids = set()
    feature_packs = []
    artifact_providers = []
    resource_providers = []
    topology_providers = []
    new_artifact_provider_ids = set()
    new_resource_provider_ids = set()
    new_topologies = set()
    for manifest, source in zip(manifests, manifest_sources):
        if manifest.plugin_id in seen_plugins:
            raise PluginDiscoveryError(source[0], source[1], f"duplicate plugin ID {manifest.plugin_id!r}")
        seen_plugins.add(manifest.plugin_id)
        if manifest.api_version != api_version:
            raise PluginDiscoveryError(
                source[0],
                source[1],
                f"plugin API {manifest.api_version!r} does not match extension API {api_version!r}",
            )
        for registration in manifest.registrations:
            if registration.component_id in new_component_ids:
                raise DuplicateRegistrationError(registration.component_id)
            new_component_ids.add(registration.component_id)
            registrations_by_kind.setdefault(registration.kind, []).append(registration)
        feature_packs.extend(manifest.feature_packs)
        for registration in manifest.artifact_providers:
            if registration.provider_id in new_artifact_provider_ids:
                raise DuplicateProviderRegistrationError(registration.provider_id)
            new_artifact_provider_ids.add(registration.provider_id)
            artifact_providers.append(registration)
        for registration in manifest.resource_providers:
            if registration.provider_id in new_resource_provider_ids:
                raise DuplicateProviderRegistrationError(registration.provider_id)
            new_resource_provider_ids.add(registration.provider_id)
            resource_providers.append(registration)
        for registration in manifest.topology_providers:
            if registration.topology in new_topologies:
                raise DuplicateTopologyProviderRegistrationError(registration.topology)
            new_topologies.add(registration.topology)
            topology_providers.append(registration)

    if feature_packs and feature_pack_catalog is None:
        manifest_index = next(index for index, manifest in enumerate(manifests) if manifest.feature_packs)
        source = manifest_sources[manifest_index]
        raise PluginDiscoveryError(
            source[0],
            source[1],
            "plugin provides feature packs but no feature_pack_catalog was provided",
        )

    if artifact_providers and artifact_provider_registry is None:
        manifest_index = next(index for index, manifest in enumerate(manifests) if manifest.artifact_providers)
        source = manifest_sources[manifest_index]
        raise PluginDiscoveryError(
            source[0],
            source[1],
            "plugin provides artifact providers but no artifact_provider_registry was provided",
        )

    if resource_providers and resource_provider_registry is None:
        manifest_index = next(index for index, manifest in enumerate(manifests) if manifest.resource_providers)
        source = manifest_sources[manifest_index]
        raise PluginDiscoveryError(
            source[0],
            source[1],
            "plugin provides resource providers but no resource_provider_registry was provided",
        )

    if topology_providers and topology_provider_registry is None:
        manifest_index = next(index for index, manifest in enumerate(manifests) if manifest.topology_providers)
        source = manifest_sources[manifest_index]
        raise PluginDiscoveryError(
            source[0],
            source[1],
            "plugin provides topology providers but no topology_provider_registry was provided",
        )

    participants = [normalized_registries[kind] for kind in sorted(normalized_registries, key=lambda item: item.value)]
    if artifact_provider_registry is not None:
        participants.append(artifact_provider_registry)
    if resource_provider_registry is not None:
        participants.append(resource_provider_registry)
    if feature_pack_catalog is not None:
        participants.append(feature_pack_catalog)
    if topology_provider_registry is not None:
        participants.append(topology_provider_registry)

    with ExitStack() as transaction:
        for participant in participants:
            transaction.enter_context(participant.registration_lock)

        for kind, registrations in registrations_by_kind.items():
            registry = normalized_registries.get(kind)
            plugin_id = next(
                manifest.plugin_id
                for manifest in manifests
                if any(registration.kind == kind for registration in manifest.registrations)
            )
            if registry is None:
                raise PluginDiscoveryError(
                    plugin_id,
                    "",
                    f"no registry was provided for kind {kind.value!r}",
                )
            for other_kind, other_registry in normalized_registries.items():
                if other_kind != kind:
                    for registration in registrations:
                        if registration.component_id in other_registry:
                            raise DuplicateRegistrationError(registration.component_id)
            registry.validate_many(registrations)

        if feature_pack_catalog is not None:
            feature_pack_catalog.validate_many(feature_packs)
        if artifact_provider_registry is not None:
            artifact_provider_registry.validate_many(artifact_providers)
        if resource_provider_registry is not None:
            resource_provider_registry.validate_many(resource_providers)
        if topology_provider_registry is not None:
            topology_provider_registry.validate_many(topology_providers)

        registry_snapshots = {kind: registry.snapshot() for kind, registry in normalized_registries.items()}
        catalog_snapshot = feature_pack_catalog.snapshot() if feature_pack_catalog is not None else None
        artifact_provider_snapshot = (
            artifact_provider_registry.snapshot() if artifact_provider_registry is not None else None
        )
        resource_provider_snapshot = (
            resource_provider_registry.snapshot() if resource_provider_registry is not None else None
        )
        topology_provider_snapshot = (
            topology_provider_registry.snapshot() if topology_provider_registry is not None else None
        )
        try:
            for kind, registrations in registrations_by_kind.items():
                normalized_registries[kind].register_many(registrations)
            if artifact_provider_registry is not None:
                artifact_provider_registry.register_many(artifact_providers)
            if resource_provider_registry is not None:
                resource_provider_registry.register_many(resource_providers)
            if topology_provider_registry is not None:
                topology_provider_registry.register_many(topology_providers)
            if feature_pack_catalog is not None:
                feature_pack_catalog.register_many(feature_packs)
        except BaseException as operation_error:
            restore_operations = []
            if feature_pack_catalog is not None:
                restore_operations.append((feature_pack_catalog.restore_snapshot, catalog_snapshot))
            if topology_provider_registry is not None:
                restore_operations.append(
                    (
                        topology_provider_registry.restore_snapshot,
                        topology_provider_snapshot,
                    )
                )
            if resource_provider_registry is not None:
                restore_operations.append(
                    (
                        resource_provider_registry.restore_snapshot,
                        resource_provider_snapshot,
                    )
                )
            if artifact_provider_registry is not None:
                restore_operations.append(
                    (
                        artifact_provider_registry.restore_snapshot,
                        artifact_provider_snapshot,
                    )
                )
            restore_operations.extend(
                (
                    normalized_registries[kind].restore_snapshot,
                    registry_snapshots[kind],
                )
                for kind in sorted(
                    normalized_registries,
                    key=lambda item: item.value,
                    reverse=True,
                )
            )

            restore_failures = []
            for restore, snapshot in restore_operations:
                try:
                    restore(snapshot)
                except BaseException as restore_error:
                    restore_failures.append(restore_error)
            if restore_failures:
                try:
                    operation_error.plugin_restore_failures = tuple(restore_failures)
                except BaseException:
                    pass
            raise

    return tuple(manifests)


__all__ = [
    "EXTENSION_API_VERSION",
    "PLUGIN_ENTRY_POINT_GROUP",
    "Action",
    "CapabilityProbe",
    "ComponentKind",
    "ComponentRegistration",
    "ComponentResult",
    "ComponentSpec",
    "ComponentStatus",
    "DuplicateRegistrationError",
    "DuplicateTopologyProviderRegistrationError",
    "EventBus",
    "EventBusQuiescedError",
    "EventDispatchAbortedError",
    "EventDispatchError",
    "EventSubscription",
    "Evidence",
    "EvidenceRequirement",
    "ExecutableComponentSpec",
    "ExecutableNodeSelector",
    "ExecutablePolicySpec",
    "ExecutableRunPlan",
    "ExecutableStartupOverlay",
    "Fault",
    "FeaturePack",
    "FeaturePackSpec",
    "IncompatibleRegistrationError",
    "IncompatibleTopologyProviderRegistrationError",
    "LifecycleEvent",
    "LifecycleEventBus",
    "NodeSelector",
    "Observer",
    "Oracle",
    "OutcomeStatus",
    "PluginDiscoveryError",
    "PluginManifest",
    "ProvisionedTopology",
    "RegistrationError",
    "Registry",
    "RunContext",
    "RunMode",
    "RunOutcome",
    "RunPlan",
    "StartupOverlay",
    "TopologyProvider",
    "TopologyProviderIdentity",
    "TopologyProviderRegistration",
    "TopologyProviderRegistrationError",
    "TopologyProviderRegistry",
    "TopologyProviderRegistryPinnedError",
    "UnknownRegistrationError",
    "UnknownTopologyProviderError",
    "Workload",
    "canonical_json",
    "derive_component_seed",
    "discover_plugins",
    "run_outcome_from_data",
    "run_plan_from_data",
    "to_plain_data",
]
