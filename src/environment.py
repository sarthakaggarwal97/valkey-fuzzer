"""Pre-topology environment contracts for artifacts, resources, and policies.

This module is intentionally independent from component types and runner
behavior. It defines the immutable declarations, provider contracts,
registries, reference language, and dependency validation needed by a future
runner-owned environment acquisition phase.

Provider contract versions are internal extension compatibility metadata. They
are not scenario schema versions and are never selected by scenario authors.
"""

import inspect
import json
import math
import re
import threading
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Generic, Optional, Protocol, TypeVar, Union, runtime_checkable

from .extension_contract import EXTENSION_API_VERSION

ENVIRONMENT_EXTENSION_API_VERSION = EXTENSION_API_VERSION
ENVIRONMENT_REFERENCE_DATA_KEY = "$valkey_fuzzer_ref"

_SEGMENT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
_PROVIDER_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*(?:[.:/][A-Za-z0-9][A-Za-z0-9_-]*)+$")


def _require_text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{field_name} must be a non-empty string without surrounding whitespace")
    return value


def _require_segment(value: str, field_name: str) -> str:
    _require_text(value, field_name)
    if not _SEGMENT_PATTERN.fullmatch(value):
        raise ValueError(
            f"{field_name} must start with an alphanumeric character and contain only "
            "alphanumeric characters, underscores, and hyphens"
        )
    return value


def _require_provider_id(value: str, field_name: str = "provider_id") -> str:
    _require_text(value, field_name)
    if not _PROVIDER_ID_PATTERN.fullmatch(value):
        raise ValueError(
            f"{field_name} must be a namespaced identifier such as 'example.provider'; "
            "namespace segments must be non-empty and contain only alphanumeric "
            "characters, underscores, and hyphens"
        )
    return value


def _require_callable_signature(
    value: Any,
    positional_argument_count: int,
    field_name: str,
) -> None:
    if not callable(value):
        raise TypeError(f"{field_name} must be callable")
    call_implementation = next(
        (base.__dict__["__call__"] for base in type(value).__mro__ if "__call__" in base.__dict__),
        None,
    )
    candidates = (value, call_implementation)
    for candidate in candidates:
        if candidate is None:
            continue
        try:
            implementation = inspect.unwrap(candidate)
        except (TypeError, ValueError):
            implementation = candidate
        if inspect.iscoroutinefunction(implementation) or inspect.isasyncgenfunction(implementation):
            raise TypeError(f"{field_name} must be synchronous, not async")
        if inspect.isgeneratorfunction(implementation):
            raise TypeError(f"{field_name} must execute eagerly, not return a generator")
    try:
        inspect.signature(value).bind(*([object()] * positional_argument_count))
    except (TypeError, ValueError) as exc:
        raise TypeError(f"{field_name} has an incompatible signature: {exc}") from exc


def _as_tuple(value: Sequence[Any], field_name: str) -> tuple[Any, ...]:
    if isinstance(value, (str, bytes, bytearray, Mapping)) or not isinstance(value, Sequence):
        raise TypeError(f"{field_name} must be an ordered sequence")
    return tuple(value)


def _freeze_json(
    value: Any,
    field_name: str,
    *,
    allow_environment_references: bool = False,
) -> Any:
    """Take a recursive immutable snapshot of JSON-safe data and typed refs."""

    if isinstance(value, EnvironmentReference):
        if allow_environment_references:
            return value
        raise TypeError(f"{field_name} must not contain environment references")
    if isinstance(value, Mapping):
        if ENVIRONMENT_REFERENCE_DATA_KEY in value:
            raise InvalidEnvironmentReferenceError(
                value.get(ENVIRONMENT_REFERENCE_DATA_KEY),
                f"{field_name} contains the reserved {ENVIRONMENT_REFERENCE_DATA_KEY!r} "
                "serialization marker; pass an EnvironmentReference in memory",
            )
        frozen: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{field_name} mapping keys must be strings")
            frozen[key] = _freeze_json(
                item,
                f"{field_name}.{key}",
                allow_environment_references=allow_environment_references,
            )
        return MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        return tuple(
            _freeze_json(
                item,
                f"{field_name}[{index}]",
                allow_environment_references=allow_environment_references,
            )
            for index, item in enumerate(value)
        )
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{field_name} cannot contain NaN or infinite numbers")
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise TypeError(
        f"{field_name} must contain only JSON-safe scalar, mapping, and sequence values; got {type(value).__name__}"
    )


def _freeze_json_mapping(
    value: Mapping[str, Any],
    field_name: str,
    *,
    allow_environment_references: bool = False,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must be a mapping")
    return _freeze_json(
        value,
        field_name,
        allow_environment_references=allow_environment_references,
    )


def _freeze_services(value: Mapping[str, Any]) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("services must be a mapping")
    snapshot = {}
    for name, service in value.items():
        _require_segment(name, "service name")
        snapshot[name] = service
    return MappingProxyType(snapshot)


def _plain_json(value: Any) -> Any:
    if isinstance(value, EnvironmentReference):
        return {ENVIRONMENT_REFERENCE_DATA_KEY: value.path}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        if ENVIRONMENT_REFERENCE_DATA_KEY in value:
            raise InvalidEnvironmentReferenceError(
                value.get(ENVIRONMENT_REFERENCE_DATA_KEY),
                f"raw mappings cannot contain the reserved {ENVIRONMENT_REFERENCE_DATA_KEY!r} serialization marker",
            )
        return {key: _plain_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain_json(item) for item in value]
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise TypeError(f"cannot export runtime value of type {type(value).__name__}")


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
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

    decoded = json.loads(
        serialized,
        object_pairs_hook=unique_object,
        parse_constant=reject_constant,
    )
    if not isinstance(decoded, Mapping):
        raise TypeError(f"{field_name} must decode to a mapping")
    return decoded


def _model_fields(
    value: Any,
    field_name: str,
    *,
    allowed: Iterable[str],
    required: Iterable[str] = (),
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must be a mapping")
    if any(not isinstance(key, str) for key in value):
        raise TypeError(f"{field_name} keys must be strings")
    allowed_fields = set(allowed)
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ValueError(f"{field_name} contains unknown fields: {unknown}")
    missing = sorted(set(required) - set(value))
    if missing:
        raise ValueError(f"{field_name} is missing required fields: {missing}")
    return value


def _validate_dependencies(
    dependencies: Sequence[str],
    *,
    owner_kind: "EnvironmentKind",
    owner_id: str,
) -> tuple[str, ...]:
    entries = _as_tuple(dependencies, "depends_on")
    parsed_entries = tuple(parse_environment_dependency(item) for item in entries)
    seen = set()
    for parsed in parsed_entries:
        if parsed.path in seen:
            raise ValueError(f"depends_on contains duplicate dependency {parsed.path!r}")
        seen.add(parsed.path)
        if parsed.kind is owner_kind and parsed.instance_id == owner_id:
            raise ValueError(f"{owner_kind.value}.{owner_id} cannot depend on itself")
    return tuple(dependency.path for dependency in parsed_entries)


class EnvironmentKind(str, Enum):
    """Addressable namespaces in the pre-topology environment."""

    ARTIFACT = "artifacts"
    RESOURCE = "resources"


@dataclass(frozen=True)
class EnvironmentDependency:
    """A dependency on one artifact or resource instance."""

    kind: EnvironmentKind
    instance_id: str

    def __post_init__(self) -> None:
        if isinstance(self.kind, str):
            object.__setattr__(self, "kind", EnvironmentKind(self.kind))
        if not isinstance(self.kind, EnvironmentKind):
            raise TypeError("kind must be an EnvironmentKind")
        _require_segment(self.instance_id, "environment dependency instance_id")

    @property
    def path(self) -> str:
        return f"{self.kind.value}.{self.instance_id}"

    def __str__(self) -> str:
        return self.path


@dataclass(frozen=True)
class EnvironmentReference:
    """An exact reference to one declared provider output."""

    kind: EnvironmentKind
    instance_id: str
    output: str

    def __post_init__(self) -> None:
        if isinstance(self.kind, str):
            object.__setattr__(self, "kind", EnvironmentKind(self.kind))
        if not isinstance(self.kind, EnvironmentKind):
            raise TypeError("kind must be an EnvironmentKind")
        _require_segment(self.instance_id, "environment reference instance_id")
        _require_segment(self.output, "environment reference output")

    @property
    def dependency(self) -> EnvironmentDependency:
        return EnvironmentDependency(self.kind, self.instance_id)

    @property
    def path(self) -> str:
        return f"{self.kind.value}.{self.instance_id}.{self.output}"

    def __str__(self) -> str:
        return self.path


class EnvironmentReferenceError(ValueError):
    """Base class for invalid or unresolvable environment references."""


class InvalidEnvironmentReferenceError(EnvironmentReferenceError):
    def __init__(self, reference: Any, reason: str):
        self.reference = reference
        self.reason = reason
        super().__init__(f"invalid environment reference {reference!r}: {reason}")


class UnknownEnvironmentTargetError(EnvironmentReferenceError):
    def __init__(self, source: str, dependency: EnvironmentDependency):
        self.source = source
        self.dependency = dependency
        super().__init__(f"{source} references unknown environment target {dependency.path!r}")


class UndeclaredEnvironmentOutputError(EnvironmentReferenceError):
    def __init__(
        self,
        source: str,
        reference: EnvironmentReference,
        provider_id: str,
        declared_outputs: Sequence[str],
    ):
        self.source = source
        self.reference = reference
        self.provider_id = provider_id
        self.declared_outputs = tuple(declared_outputs)
        available = ", ".join(repr(output) for output in self.declared_outputs) or "none"
        super().__init__(
            f"{source} references undeclared output {reference.output!r} on {reference.dependency.path!r}; "
            f"provider {provider_id!r} declares {available}"
        )


class UnresolvedEnvironmentReferenceError(EnvironmentReferenceError):
    def __init__(self, reference: EnvironmentReference, reason: str):
        self.reference = reference
        self.reason = reason
        super().__init__(f"cannot resolve environment reference {reference.path!r}: {reason}")


class EnvironmentDependencyCycleError(ValueError):
    def __init__(self, cycle: Sequence[EnvironmentDependency]):
        self.cycle = tuple(cycle)
        path = " -> ".join(dependency.path for dependency in self.cycle)
        super().__init__(f"environment dependency graph contains a cycle: {path}")


def parse_environment_dependency(value: str) -> EnvironmentDependency:
    """Parse exactly ``artifacts.<id>`` or ``resources.<id>``."""

    if not isinstance(value, str):
        raise InvalidEnvironmentReferenceError(value, "dependency must be a string")
    if value != value.strip():
        raise InvalidEnvironmentReferenceError(value, "surrounding whitespace is not allowed")
    parts = value.split(".")
    if len(parts) != 2 or parts[0] not in {kind.value for kind in EnvironmentKind}:
        raise InvalidEnvironmentReferenceError(
            value,
            "expected exactly 'artifacts.<id>' or 'resources.<id>'",
        )
    try:
        return EnvironmentDependency(EnvironmentKind(parts[0]), parts[1])
    except (TypeError, ValueError) as exc:
        raise InvalidEnvironmentReferenceError(value, str(exc)) from exc


def parse_environment_reference(value: str) -> EnvironmentReference:
    """Parse exactly ``artifacts.<id>.<output>`` or ``resources.<id>.<output>``."""

    if not isinstance(value, str):
        raise InvalidEnvironmentReferenceError(value, "$ref value must be a string")
    if value != value.strip():
        raise InvalidEnvironmentReferenceError(value, "surrounding whitespace is not allowed")
    parts = value.split(".")
    if len(parts) != 3 or parts[0] not in {kind.value for kind in EnvironmentKind}:
        raise InvalidEnvironmentReferenceError(
            value,
            "expected exactly 'artifacts.<id>.<output>' or 'resources.<id>.<output>'",
        )
    try:
        return EnvironmentReference(EnvironmentKind(parts[0]), parts[1], parts[2])
    except (TypeError, ValueError) as exc:
        raise InvalidEnvironmentReferenceError(value, str(exc)) from exc


def _serialized_reference_marker(
    value: Mapping[str, Any],
    location: str,
) -> Optional[EnvironmentReference]:
    if ENVIRONMENT_REFERENCE_DATA_KEY not in value:
        return None
    if set(value) != {ENVIRONMENT_REFERENCE_DATA_KEY}:
        raise InvalidEnvironmentReferenceError(
            value.get(ENVIRONMENT_REFERENCE_DATA_KEY),
            f"{location} serialized reference marker must contain only the {ENVIRONMENT_REFERENCE_DATA_KEY!r} key",
        )
    try:
        return parse_environment_reference(value[ENVIRONMENT_REFERENCE_DATA_KEY])
    except InvalidEnvironmentReferenceError as exc:
        raise InvalidEnvironmentReferenceError(exc.reference, f"{location}: {exc.reason}") from exc


def environment_references_from_data(value: Any, field_name: str = "environment data") -> Any:
    """Decode canonical serialized reference markers into typed references.

    Only the reserved ``$valkey_fuzzer_ref`` marker has persistence semantics.
    A raw ``{"$ref": ...}`` mapping is ordinary payload at this boundary.
    """

    def decode(item: Any, location: str) -> Any:
        if isinstance(item, EnvironmentReference):
            return item
        if isinstance(item, Mapping):
            for key in item:
                if not isinstance(key, str):
                    raise TypeError(f"{location} mapping keys must be strings")
            marker = _serialized_reference_marker(item, location)
            if marker is not None:
                return marker
            return {key: decode(nested, f"{location}.{key}") for key, nested in item.items()}
        if isinstance(item, (list, tuple)):
            return [decode(nested, f"{location}[{index}]") for index, nested in enumerate(item)]
        if isinstance(item, float) and not math.isfinite(item):
            raise ValueError(f"{location} cannot contain NaN or infinite numbers")
        if item is None or isinstance(item, (str, bool, int, float)):
            return item
        # Preserve invalid outer model values so the owning model can retain
        # its more specific shape/type diagnostics. JSON deserialization
        # cannot produce these values; this only affects direct ``from_data``
        # callers.
        return item

    return decode(value, field_name)


def _walk_environment_references(
    value: Any,
    *,
    location: str,
) -> Iterable[tuple[str, EnvironmentReference]]:
    if isinstance(value, EnvironmentReference):
        yield location, value
        return
    if isinstance(value, Mapping):
        for key in value:
            if not isinstance(key, str):
                raise TypeError(f"{location} mapping keys must be strings")
        for key, item in value.items():
            yield from _walk_environment_references(item, location=f"{location}.{key}")
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            yield from _walk_environment_references(item, location=f"{location}[{index}]")
        return
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{location} cannot contain NaN or infinite numbers")
    if value is None or isinstance(value, (str, bool, int, float)):
        return
    raise TypeError(f"{location} contains non-JSON-safe value of type {type(value).__name__}")


def collect_environment_references(value: Any) -> tuple[EnvironmentReference, ...]:
    """Collect distinct references in deterministic first-observed order."""

    references = []
    seen = set()
    for _location, reference in _walk_environment_references(value, location="$"):
        if reference not in seen:
            seen.add(reference)
            references.append(reference)
    return tuple(references)


@dataclass(frozen=True)
class ArtifactSpec:
    """Immutable declaration for one artifact resolved before topology setup.

    Configuration is serialized with the run plan and MUST NOT contain
    credentials. Use environment references for sensitive runtime values.
    """

    artifact_id: str
    provider_id: str
    config: Mapping[str, Any] = field(default_factory=dict)
    depends_on: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_segment(self.artifact_id, "artifact_id")
        _require_provider_id(self.provider_id)
        object.__setattr__(
            self,
            "config",
            _freeze_json_mapping(
                self.config,
                "artifact config",
                allow_environment_references=True,
            ),
        )
        object.__setattr__(
            self,
            "depends_on",
            _validate_dependencies(
                self.depends_on,
                owner_kind=EnvironmentKind.ARTIFACT,
                owner_id=self.artifact_id,
            ),
        )

    @property
    def dependency(self) -> EnvironmentDependency:
        return EnvironmentDependency(EnvironmentKind.ARTIFACT, self.artifact_id)

    def to_data(self) -> Mapping[str, Any]:
        return {
            "artifact_id": self.artifact_id,
            "provider_id": self.provider_id,
            "config": _plain_json(self.config),
            "depends_on": list(self.depends_on),
        }

    def to_json(self) -> str:
        return _canonical_json(self.to_data())

    @classmethod
    def from_data(cls, value: Mapping[str, Any]) -> "ArtifactSpec":
        value = environment_references_from_data(value, "artifact spec")
        mapping = _model_fields(
            value,
            "artifact spec",
            allowed=("artifact_id", "provider_id", "config", "depends_on"),
            required=("artifact_id", "provider_id"),
        )
        return cls(
            artifact_id=mapping["artifact_id"],
            provider_id=mapping["provider_id"],
            config=environment_references_from_data(
                mapping.get("config", {}),
                "artifact spec.config",
            ),
            depends_on=mapping.get("depends_on", ()),
        )

    @classmethod
    def from_json(cls, serialized: str) -> "ArtifactSpec":
        return cls.from_data(_load_json_mapping(serialized, "artifact spec JSON"))


@dataclass(frozen=True)
class ResourceSpec:
    """Immutable declaration for one managed pre-topology resource.

    Configuration is serialized with the run plan and MUST NOT contain
    credentials. Use environment references for sensitive runtime values.
    """

    resource_id: str
    provider_id: str
    config: Mapping[str, Any] = field(default_factory=dict)
    depends_on: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_segment(self.resource_id, "resource_id")
        _require_provider_id(self.provider_id)
        object.__setattr__(
            self,
            "config",
            _freeze_json_mapping(
                self.config,
                "resource config",
                allow_environment_references=True,
            ),
        )
        object.__setattr__(
            self,
            "depends_on",
            _validate_dependencies(
                self.depends_on,
                owner_kind=EnvironmentKind.RESOURCE,
                owner_id=self.resource_id,
            ),
        )

    @property
    def dependency(self) -> EnvironmentDependency:
        return EnvironmentDependency(EnvironmentKind.RESOURCE, self.resource_id)

    def to_data(self) -> Mapping[str, Any]:
        return {
            "resource_id": self.resource_id,
            "provider_id": self.provider_id,
            "config": _plain_json(self.config),
            "depends_on": list(self.depends_on),
        }

    def to_json(self) -> str:
        return _canonical_json(self.to_data())

    @classmethod
    def from_data(cls, value: Mapping[str, Any]) -> "ResourceSpec":
        value = environment_references_from_data(value, "resource spec")
        mapping = _model_fields(
            value,
            "resource spec",
            allowed=("resource_id", "provider_id", "config", "depends_on"),
            required=("resource_id", "provider_id"),
        )
        return cls(
            resource_id=mapping["resource_id"],
            provider_id=mapping["provider_id"],
            config=environment_references_from_data(
                mapping.get("config", {}),
                "resource spec.config",
            ),
            depends_on=mapping.get("depends_on", ()),
        )

    @classmethod
    def from_json(cls, serialized: str) -> "ResourceSpec":
        return cls.from_data(_load_json_mapping(serialized, "resource spec JSON"))


@dataclass(frozen=True)
class PolicySpec:
    """Immutable policy intent available to providers and verdict components.

    Configuration is serialized with the run plan and MUST NOT contain
    credentials. Use environment references for sensitive runtime values.
    """

    policy_id: str
    policy_type: str
    config: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_segment(self.policy_id, "policy_id")
        _require_provider_id(self.policy_type, "policy_type")
        object.__setattr__(
            self,
            "config",
            _freeze_json_mapping(
                self.config,
                "policy config",
                allow_environment_references=True,
            ),
        )

    def to_data(self) -> Mapping[str, Any]:
        return {
            "policy_id": self.policy_id,
            "policy_type": self.policy_type,
            "config": _plain_json(self.config),
        }

    def to_json(self) -> str:
        return _canonical_json(self.to_data())

    @classmethod
    def from_data(cls, value: Mapping[str, Any]) -> "PolicySpec":
        value = environment_references_from_data(value, "policy spec")
        mapping = _model_fields(
            value,
            "policy spec",
            allowed=("policy_id", "policy_type", "config"),
            required=("policy_id", "policy_type"),
        )
        return cls(
            policy_id=mapping["policy_id"],
            policy_type=mapping["policy_type"],
            config=environment_references_from_data(
                mapping.get("config", {}),
                "policy spec.config",
            ),
        )

    @classmethod
    def from_json(cls, serialized: str) -> "PolicySpec":
        return cls.from_data(_load_json_mapping(serialized, "policy spec JSON"))


@dataclass(frozen=True)
class SupportDecision:
    """Provider support result produced before external mutation.

    ``details`` are report-safe diagnostics and MUST NOT contain credentials.
    """

    supported: bool
    reason: Optional[str] = None
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.supported, bool):
            raise TypeError("supported must be a boolean")
        if self.supported:
            if self.reason is not None:
                raise ValueError("a supported decision cannot include a rejection reason")
        else:
            _require_text(self.reason, "unsupported decision reason")
        object.__setattr__(self, "details", _freeze_json_mapping(self.details, "support decision details"))

    @classmethod
    def allow(cls, *, details: Optional[Mapping[str, Any]] = None) -> "SupportDecision":
        return cls(supported=True, details={} if details is None else details)

    @classmethod
    def deny(
        cls,
        reason: str,
        *,
        details: Optional[Mapping[str, Any]] = None,
    ) -> "SupportDecision":
        return cls(supported=False, reason=reason, details={} if details is None else details)

    def to_data(self) -> Mapping[str, Any]:
        return {
            "supported": self.supported,
            "reason": self.reason,
            "details": _plain_json(self.details),
        }

    def to_json(self) -> str:
        return _canonical_json(self.to_data())


@dataclass(frozen=True)
class ResolvedArtifact:
    """Resolved artifact with runtime bindings and report-safe provenance.

    Output values may be sensitive and are therefore excluded from
    representation, equality, and serialization. Providers MUST put only
    non-sensitive replay metadata in ``provenance``.
    """

    artifact_id: str
    provider_id: str
    outputs: Mapping[str, Any] = field(
        repr=False,
        compare=False,
        metadata={"serialize": False},
    )
    provenance: Mapping[str, Any]
    output_names: tuple[str, ...] = field(init=False)
    services: Mapping[str, Any] = field(
        default_factory=dict,
        repr=False,
        compare=False,
        metadata={"serialize": False},
    )
    handle: Any = field(
        default=None,
        repr=False,
        compare=False,
        metadata={"serialize": False},
    )

    def __post_init__(self) -> None:
        _require_segment(self.artifact_id, "artifact_id")
        _require_provider_id(self.provider_id)
        outputs = _freeze_json_mapping(self.outputs, "artifact outputs")
        for output in outputs:
            _require_segment(output, "artifact output name")
        object.__setattr__(self, "outputs", outputs)
        object.__setattr__(self, "output_names", tuple(sorted(outputs)))
        object.__setattr__(self, "provenance", _freeze_json_mapping(self.provenance, "artifact provenance"))
        object.__setattr__(self, "services", _freeze_services(self.services))

    def to_data(self) -> Mapping[str, Any]:
        """Export a report-safe record without runtime output values or handles."""

        return {
            "artifact_id": self.artifact_id,
            "provider_id": self.provider_id,
            "output_names": list(self.output_names),
            "provenance": _plain_json(self.provenance),
        }

    def to_json(self) -> str:
        return _canonical_json(self.to_data())


@dataclass(frozen=True)
class ProvisionedResource:
    """Managed resource with runtime bindings and report-safe provenance.

    Output values may be sensitive and are therefore excluded from
    representation, equality, and serialization. Providers MUST put only
    non-sensitive replay metadata in ``provenance``.
    """

    resource_id: str
    provider_id: str
    outputs: Mapping[str, Any] = field(
        repr=False,
        compare=False,
        metadata={"serialize": False},
    )
    provenance: Mapping[str, Any]
    output_names: tuple[str, ...] = field(init=False)
    services: Mapping[str, Any] = field(
        default_factory=dict,
        repr=False,
        compare=False,
        metadata={"serialize": False},
    )
    handle: Any = field(
        default=None,
        repr=False,
        compare=False,
        metadata={"serialize": False},
    )

    def __post_init__(self) -> None:
        _require_segment(self.resource_id, "resource_id")
        _require_provider_id(self.provider_id)
        outputs = _freeze_json_mapping(self.outputs, "resource outputs")
        for output in outputs:
            _require_segment(output, "resource output name")
        object.__setattr__(self, "outputs", outputs)
        object.__setattr__(self, "output_names", tuple(sorted(outputs)))
        object.__setattr__(self, "provenance", _freeze_json_mapping(self.provenance, "resource provenance"))
        object.__setattr__(self, "services", _freeze_services(self.services))

    def to_data(self) -> Mapping[str, Any]:
        """Export a report-safe record without runtime output values or handles."""

        return {
            "resource_id": self.resource_id,
            "provider_id": self.provider_id,
            "output_names": list(self.output_names),
            "provenance": _plain_json(self.provenance),
        }

    def to_json(self) -> str:
        return _canonical_json(self.to_data())


@runtime_checkable
class EnvironmentProviderContext(Protocol):
    """Runner-independent services available during provider lifecycle calls.

    The context is scoped to one environment declaration. In particular,
    ``dependency_services`` MUST contain only the services exported by the
    declaration's explicit ``depends_on`` entries. Mapping keys use the exact
    ``artifacts.<id>`` or ``resources.<id>`` dependency grammar, and nested
    keys are service names declared by the dependency's provider registration.
    Service objects and all other context values are runtime-only.

    ``deadline`` is an absolute monotonic timestamp, or ``None`` when no
    cooperative deadline applies. Providers remain responsible for checking
    it during potentially long synchronous operations. ``artifact_dir`` is a
    provider-scoped directory for durable diagnostics and reproducer inputs.
    Published event details MUST be report-safe and must not contain secrets
    or runtime service objects.
    """

    @property
    def run_id(self) -> str: ...

    @property
    def seed(self) -> int: ...

    @property
    def deadline(self) -> Optional[float]: ...

    @property
    def artifact_dir(self) -> str: ...

    @property
    def dependency_services(self) -> Mapping[str, Mapping[str, Any]]: ...

    def publish_event(self, name: str, details: Mapping[str, Any]) -> None: ...


@runtime_checkable
class ArtifactProvider(Protocol):
    """Lifecycle contract for resolving an immutable artifact.

    Provider constructors must only initialize in-memory ownership state.
    ``check_support`` must not mutate external state. ``rollback`` is used
    after failed resolution; ``cleanup`` receives the exact successful result.
    Every lifecycle call receives the declaration-scoped provider context.
    """

    def check_support(
        self,
        context: EnvironmentProviderContext,
        spec: ArtifactSpec,
        resolved_config: Mapping[str, Any],
    ) -> SupportDecision: ...

    def resolve(
        self,
        context: EnvironmentProviderContext,
        spec: ArtifactSpec,
        resolved_config: Mapping[str, Any],
    ) -> ResolvedArtifact: ...

    def rollback(
        self,
        context: EnvironmentProviderContext,
        spec: ArtifactSpec,
    ) -> None: ...

    def cleanup(
        self,
        context: EnvironmentProviderContext,
        spec: ArtifactSpec,
        artifact: ResolvedArtifact,
    ) -> None: ...


@runtime_checkable
class ResourceProvider(Protocol):
    """Lifecycle contract for a managed dependency provisioned before topology.

    Every lifecycle call receives the declaration-scoped provider context.
    """

    def check_support(
        self,
        context: EnvironmentProviderContext,
        spec: ResourceSpec,
        resolved_config: Mapping[str, Any],
    ) -> SupportDecision: ...

    def provision(
        self,
        context: EnvironmentProviderContext,
        spec: ResourceSpec,
        resolved_config: Mapping[str, Any],
    ) -> ProvisionedResource: ...

    def rollback(
        self,
        context: EnvironmentProviderContext,
        spec: ResourceSpec,
    ) -> None: ...

    def cleanup(
        self,
        context: EnvironmentProviderContext,
        spec: ResourceSpec,
        resource: ProvisionedResource,
    ) -> None: ...


ArtifactProviderValidator = Callable[[ArtifactSpec], None]
ResourceProviderValidator = Callable[[ResourceSpec], None]
ArtifactProviderResolvedConfigValidator = Callable[[ArtifactSpec, Mapping[str, Any]], None]
ResourceProviderResolvedConfigValidator = Callable[[ResourceSpec, Mapping[str, Any]], None]


class ProviderRegistrationError(ValueError):
    """Base class for provider registry contract failures."""


class DuplicateProviderRegistrationError(ProviderRegistrationError):
    def __init__(self, provider_id: str):
        self.provider_id = provider_id
        super().__init__(f"provider {provider_id!r} is already registered")


class UnknownProviderError(ProviderRegistrationError):
    def __init__(self, provider_id: str):
        self.provider_id = provider_id
        super().__init__(f"provider {provider_id!r} is not registered")


class IncompatibleProviderRegistrationError(ProviderRegistrationError):
    def __init__(self, provider_id: str, reason: str):
        self.provider_id = provider_id
        self.reason = reason
        super().__init__(f"provider {provider_id!r} is incompatible: {reason}")


class ProviderContractError(TypeError):
    def __init__(self, provider_id: str, reason: str):
        self.provider_id = provider_id
        self.reason = reason
        super().__init__(f"provider {provider_id!r} violates its contract: {reason}")


class EnvironmentRegistryChangedError(RuntimeError):
    """A provider registry changed during a pinned validation operation."""

    def __init__(self, registry_name: str):
        self.registry_name = registry_name
        super().__init__(f"{registry_name} changed during environment validation")


class ProviderResultError(ValueError):
    def __init__(self, provider_id: str, reason: str):
        self.provider_id = provider_id
        self.reason = reason
        super().__init__(f"provider {provider_id!r} returned an invalid result: {reason}")


def _validate_declared_names(
    values: Sequence[str],
    *,
    collection_name: str,
    item_name: str,
) -> tuple[str, ...]:
    entries = _as_tuple(values, collection_name)
    seen = set()
    for value in entries:
        _require_segment(value, f"declared {item_name}")
        if value in seen:
            raise ValueError(f"{collection_name} contains duplicate declaration {value!r}")
        seen.add(value)
    return entries


def _validate_result_names(
    provider_id: str,
    declared_names: Sequence[str],
    actual_names: Iterable[str],
    *,
    item_name: str,
) -> None:
    expected = set(declared_names)
    actual = set(actual_names)
    missing = tuple(name for name in declared_names if name not in actual)
    unexpected = tuple(sorted(actual - expected))
    if not missing and not unexpected:
        return
    differences = []
    if missing:
        differences.append(f"missing declared {item_name}s {missing!r}")
    if unexpected:
        differences.append(f"returned undeclared {item_name}s {unexpected!r}")
    raise ProviderResultError(provider_id, "; ".join(differences))


@dataclass(frozen=True)
class ArtifactProviderRegistration:
    """Internal factory, binding declarations, and compatibility metadata.

    ``validator(spec)`` performs symbolic static validation. It MUST be pure,
    deterministic, idempotent, and accept typed ``EnvironmentReference``
    values wherever the spec permits them.

    ``resolved_config_validator(spec, mapping)`` performs concrete post-bind
    validation after dependency outputs are resolved and before provider
    construction or support checks. It MUST be pure; use it for mandatory
    constraints that require concrete configuration values.
    """

    provider_id: str
    factory: Callable[[], ArtifactProvider]
    outputs: tuple[str, ...] = ()
    services: tuple[str, ...] = ()
    api_version: str = EXTENSION_API_VERSION
    validator: Optional[ArtifactProviderValidator] = None
    resolved_config_validator: Optional[ArtifactProviderResolvedConfigValidator] = None

    def __post_init__(self) -> None:
        _require_provider_id(self.provider_id)
        _require_callable_signature(self.factory, 0, "artifact provider factory")
        object.__setattr__(
            self,
            "outputs",
            _validate_declared_names(
                self.outputs,
                collection_name="outputs",
                item_name="output",
            ),
        )
        object.__setattr__(
            self,
            "services",
            _validate_declared_names(
                self.services,
                collection_name="services",
                item_name="service",
            ),
        )
        _require_text(self.api_version, "api_version")
        if self.validator is not None:
            _require_callable_signature(self.validator, 1, "artifact provider validator")
        if self.resolved_config_validator is not None:
            _require_callable_signature(
                self.resolved_config_validator,
                2,
                "artifact provider resolved config validator",
            )

    @property
    def declared_outputs(self) -> tuple[str, ...]:
        return self.outputs

    @property
    def declared_services(self) -> tuple[str, ...]:
        return self.services

    def validate_result(self, spec: ArtifactSpec, result: ResolvedArtifact) -> None:
        if not isinstance(spec, ArtifactSpec):
            raise TypeError("spec must be an ArtifactSpec")
        if not isinstance(result, ResolvedArtifact):
            raise ProviderResultError(self.provider_id, "expected ResolvedArtifact")
        if spec.provider_id != self.provider_id:
            raise ProviderResultError(
                self.provider_id,
                f"spec selects provider {spec.provider_id!r}",
            )
        if result.provider_id != self.provider_id:
            raise ProviderResultError(
                self.provider_id,
                f"result names provider {result.provider_id!r}",
            )
        if result.artifact_id != spec.artifact_id:
            raise ProviderResultError(
                self.provider_id,
                f"result artifact_id {result.artifact_id!r} does not match spec {spec.artifact_id!r}",
            )
        _validate_result_names(
            self.provider_id,
            self.outputs,
            result.outputs,
            item_name="output",
        )
        _validate_result_names(
            self.provider_id,
            self.services,
            result.services,
            item_name="service",
        )

    def validate_resolved_config(
        self,
        spec: ArtifactSpec,
        resolved_config: Mapping[str, Any],
    ) -> None:
        if not isinstance(spec, ArtifactSpec):
            raise TypeError("spec must be an ArtifactSpec")
        if not isinstance(resolved_config, Mapping):
            raise TypeError("resolved_config must be a mapping")
        references = collect_environment_references(resolved_config)
        if references:
            raise ProviderContractError(
                self.provider_id,
                "resolved config validator received unresolved environment references",
            )
        if self.resolved_config_validator is not None:
            _validate_validator_result(
                self.provider_id,
                self.resolved_config_validator(spec, resolved_config),
            )


@dataclass(frozen=True)
class ResourceProviderRegistration:
    """Internal factory, binding declarations, and compatibility metadata.

    ``validator(spec)`` performs symbolic static validation. It MUST be pure,
    deterministic, idempotent, and accept typed ``EnvironmentReference``
    values wherever the spec permits them.

    ``resolved_config_validator(spec, mapping)`` performs concrete post-bind
    validation after dependency outputs are resolved and before provider
    construction or support checks. It MUST be pure; use it for mandatory
    constraints that require concrete configuration values.
    """

    provider_id: str
    factory: Callable[[], ResourceProvider]
    outputs: tuple[str, ...] = ()
    services: tuple[str, ...] = ()
    api_version: str = EXTENSION_API_VERSION
    validator: Optional[ResourceProviderValidator] = None
    resolved_config_validator: Optional[ResourceProviderResolvedConfigValidator] = None

    def __post_init__(self) -> None:
        _require_provider_id(self.provider_id)
        _require_callable_signature(self.factory, 0, "resource provider factory")
        object.__setattr__(
            self,
            "outputs",
            _validate_declared_names(
                self.outputs,
                collection_name="outputs",
                item_name="output",
            ),
        )
        object.__setattr__(
            self,
            "services",
            _validate_declared_names(
                self.services,
                collection_name="services",
                item_name="service",
            ),
        )
        _require_text(self.api_version, "api_version")
        if self.validator is not None:
            _require_callable_signature(self.validator, 1, "resource provider validator")
        if self.resolved_config_validator is not None:
            _require_callable_signature(
                self.resolved_config_validator,
                2,
                "resource provider resolved config validator",
            )

    @property
    def declared_outputs(self) -> tuple[str, ...]:
        return self.outputs

    @property
    def declared_services(self) -> tuple[str, ...]:
        return self.services

    def validate_result(self, spec: ResourceSpec, result: ProvisionedResource) -> None:
        if not isinstance(spec, ResourceSpec):
            raise TypeError("spec must be a ResourceSpec")
        if not isinstance(result, ProvisionedResource):
            raise ProviderResultError(self.provider_id, "expected ProvisionedResource")
        if spec.provider_id != self.provider_id:
            raise ProviderResultError(
                self.provider_id,
                f"spec selects provider {spec.provider_id!r}",
            )
        if result.provider_id != self.provider_id:
            raise ProviderResultError(
                self.provider_id,
                f"result names provider {result.provider_id!r}",
            )
        if result.resource_id != spec.resource_id:
            raise ProviderResultError(
                self.provider_id,
                f"result resource_id {result.resource_id!r} does not match spec {spec.resource_id!r}",
            )
        _validate_result_names(
            self.provider_id,
            self.outputs,
            result.outputs,
            item_name="output",
        )
        _validate_result_names(
            self.provider_id,
            self.services,
            result.services,
            item_name="service",
        )

    def validate_resolved_config(
        self,
        spec: ResourceSpec,
        resolved_config: Mapping[str, Any],
    ) -> None:
        if not isinstance(spec, ResourceSpec):
            raise TypeError("spec must be a ResourceSpec")
        if not isinstance(resolved_config, Mapping):
            raise TypeError("resolved_config must be a mapping")
        references = collect_environment_references(resolved_config)
        if references:
            raise ProviderContractError(
                self.provider_id,
                "resolved config validator received unresolved environment references",
            )
        if self.resolved_config_validator is not None:
            _validate_validator_result(
                self.provider_id,
                self.resolved_config_validator(spec, resolved_config),
            )


RegistrationT = TypeVar("RegistrationT")
ProviderT = TypeVar("ProviderT")


def _validate_provider_methods(
    provider_id: str,
    provider: Any,
    methods: Mapping[str, int],
) -> None:
    for method_name, positional_argument_count in methods.items():
        try:
            method = getattr(provider, method_name, None)
        except Exception as exc:
            raise ProviderContractError(
                provider_id,
                f"cannot access {method_name} method: {type(exc).__name__}: {exc}",
            ) from exc
        try:
            _require_callable_signature(
                method,
                positional_argument_count,
                f"{method_name} method",
            )
        except TypeError as exc:
            raise ProviderContractError(provider_id, str(exc)) from exc


def _validate_validator_result(provider_id: str, result: Any) -> None:
    if inspect.iscoroutine(result):
        result.close()
        raise ProviderContractError(provider_id, "validator returned an awaitable instead of executing synchronously")
    if inspect.isawaitable(result):
        raise ProviderContractError(provider_id, "validator returned an awaitable instead of executing synchronously")
    if inspect.isasyncgen(result):
        raise ProviderContractError(
            provider_id,
            "validator returned an async generator instead of executing synchronously",
        )
    if inspect.isgenerator(result):
        result.close()
        raise ProviderContractError(provider_id, "validator returned a generator instead of executing eagerly")
    if result is not None:
        raise ProviderContractError(provider_id, "validator must return None")


class _ProviderRegistry(Generic[RegistrationT, ProviderT]):
    def __init__(
        self,
        registration_type: type,
        provider_protocol: type,
        provider_label: str,
        provider_methods: Mapping[str, int],
        api_version: str,
    ):
        self._registration_type = registration_type
        self._provider_protocol = provider_protocol
        self._provider_label = provider_label
        self._provider_methods = MappingProxyType(dict(provider_methods))
        self._api_version = _require_text(api_version, "api_version")
        self._registrations: dict[str, RegistrationT] = {}
        self._lock = threading.RLock()

    @property
    def api_version(self) -> str:
        return self._api_version

    def _validate_many(self, registrations: Sequence[RegistrationT]) -> None:
        seen = set()
        for registration in registrations:
            if not isinstance(registration, self._registration_type):
                raise TypeError(
                    f"{self._provider_label} registry entries must be {self._registration_type.__name__} values"
                )
            provider_id = registration.provider_id
            if registration.api_version != self.api_version:
                raise IncompatibleProviderRegistrationError(
                    provider_id,
                    f"API {registration.api_version!r} does not match registry API {self.api_version!r}",
                )
            if provider_id in seen or provider_id in self._registrations:
                raise DuplicateProviderRegistrationError(provider_id)
            seen.add(provider_id)

    def validate_many(self, registrations: Iterable[RegistrationT]) -> None:
        entries = tuple(registrations)
        with self._lock:
            self._validate_many(entries)

    def register(self, registration: RegistrationT) -> None:
        self.register_many((registration,))

    def register_many(self, registrations: Iterable[RegistrationT]) -> None:
        entries = tuple(registrations)
        with self._lock:
            self._validate_many(entries)
            for registration in entries:
                self._registrations[registration.provider_id] = registration

    def unregister(self, provider_id: str) -> RegistrationT:
        _require_provider_id(provider_id)
        with self._lock:
            try:
                return self._registrations.pop(provider_id)
            except KeyError as exc:
                raise UnknownProviderError(provider_id) from exc

    def resolve(self, provider_id: str) -> RegistrationT:
        _require_provider_id(provider_id)
        with self._lock:
            try:
                return self._registrations[provider_id]
            except KeyError as exc:
                raise UnknownProviderError(provider_id) from exc

    def create(self, provider_id: str) -> ProviderT:
        registration = self.resolve(provider_id)
        try:
            provider = registration.factory()
        except Exception as exc:
            raise ProviderContractError(
                provider_id,
                f"factory failed with {type(exc).__name__}: {exc}",
            ) from exc
        _validate_provider_methods(provider_id, provider, self._provider_methods)
        if not isinstance(provider, self._provider_protocol):  # Defensive protocol-level check.
            raise ProviderContractError(
                provider_id,
                f"factory must return an object implementing {self._provider_protocol.__name__}",
            )
        return provider

    @property
    def registered_ids(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._registrations))

    def snapshot(self) -> Mapping[str, RegistrationT]:
        with self._lock:
            return MappingProxyType(
                {provider_id: self._registrations[provider_id] for provider_id in sorted(self._registrations)}
            )

    @property
    def registration_lock(self) -> threading.RLock:
        return self._lock

    def restore_snapshot(self, snapshot: Mapping[str, RegistrationT]) -> None:
        if not isinstance(snapshot, Mapping):
            raise TypeError("registry snapshot must be a mapping")
        entries = tuple(snapshot.values())
        with self._lock:
            current = self._registrations
            self._registrations = {}
            try:
                self._validate_many(entries)
                if any(provider_id != registration.provider_id for provider_id, registration in snapshot.items()):
                    raise ValueError("registry snapshot keys must match registration provider IDs")
                self._registrations = dict(snapshot)
            except BaseException:
                self._registrations = current
                raise

    def __contains__(self, provider_id: object) -> bool:
        with self._lock:
            return provider_id in self._registrations

    def __len__(self) -> int:
        with self._lock:
            return len(self._registrations)


class ArtifactProviderRegistry(_ProviderRegistry[ArtifactProviderRegistration, ArtifactProvider]):
    """Thread-safe atomic registry for artifact providers."""

    def __init__(self, api_version: str = EXTENSION_API_VERSION):
        super().__init__(
            ArtifactProviderRegistration,
            ArtifactProvider,
            "artifact provider",
            {
                "check_support": 3,
                "resolve": 3,
                "rollback": 2,
                "cleanup": 3,
            },
            api_version,
        )

    def validate_spec(self, spec: ArtifactSpec) -> None:
        if not isinstance(spec, ArtifactSpec):
            raise TypeError("spec must be an ArtifactSpec")
        registration = self.resolve(spec.provider_id)
        if registration.validator is not None:
            _validate_validator_result(registration.provider_id, registration.validator(spec))

    def validate_result(self, spec: ArtifactSpec, result: ResolvedArtifact) -> None:
        if not isinstance(spec, ArtifactSpec):
            raise TypeError("spec must be an ArtifactSpec")
        self.resolve(spec.provider_id).validate_result(spec, result)


class ResourceProviderRegistry(_ProviderRegistry[ResourceProviderRegistration, ResourceProvider]):
    """Thread-safe atomic registry for resource providers."""

    def __init__(self, api_version: str = EXTENSION_API_VERSION):
        super().__init__(
            ResourceProviderRegistration,
            ResourceProvider,
            "resource provider",
            {
                "check_support": 3,
                "provision": 3,
                "rollback": 2,
                "cleanup": 3,
            },
            api_version,
        )

    def validate_spec(self, spec: ResourceSpec) -> None:
        if not isinstance(spec, ResourceSpec):
            raise TypeError("spec must be a ResourceSpec")
        registration = self.resolve(spec.provider_id)
        if registration.validator is not None:
            _validate_validator_result(registration.provider_id, registration.validator(spec))

    def validate_result(self, spec: ResourceSpec, result: ProvisionedResource) -> None:
        if not isinstance(spec, ResourceSpec):
            raise TypeError("spec must be a ResourceSpec")
        self.resolve(spec.provider_id).validate_result(spec, result)


def _resolved_output(
    reference: EnvironmentReference,
    *,
    artifacts: Mapping[str, ResolvedArtifact],
    resources: Mapping[str, ProvisionedResource],
) -> Any:
    if reference.kind is EnvironmentKind.ARTIFACT:
        try:
            record = artifacts[reference.instance_id]
        except KeyError as exc:
            raise UnresolvedEnvironmentReferenceError(reference, "artifact has not been resolved") from exc
        if not isinstance(record, ResolvedArtifact):
            raise TypeError(f"resolved artifact {reference.instance_id!r} must be a ResolvedArtifact")
        if record.artifact_id != reference.instance_id:
            raise ValueError(
                f"resolved artifact mapping key {reference.instance_id!r} does not match "
                f"record ID {record.artifact_id!r}"
            )
    else:
        try:
            record = resources[reference.instance_id]
        except KeyError as exc:
            raise UnresolvedEnvironmentReferenceError(reference, "resource has not been provisioned") from exc
        if not isinstance(record, ProvisionedResource):
            raise TypeError(f"provisioned resource {reference.instance_id!r} must be a ProvisionedResource")
        if record.resource_id != reference.instance_id:
            raise ValueError(
                f"provisioned resource mapping key {reference.instance_id!r} does not match "
                f"record ID {record.resource_id!r}"
            )

    try:
        return record.outputs[reference.output]
    except KeyError as exc:
        raise UnresolvedEnvironmentReferenceError(reference, "provider result does not contain the output") from exc


def resolve_environment_references(
    value: Any,
    *,
    artifacts: Mapping[str, ResolvedArtifact],
    resources: Mapping[str, ProvisionedResource],
) -> Any:
    """Resolve exact references into an immutable runtime-only value.

    The result may contain sensitive provider outputs and must not be included
    in persisted plans, reports, logs, or diagnostics.
    """

    if not isinstance(artifacts, Mapping):
        raise TypeError("artifacts must be a mapping")
    if not isinstance(resources, Mapping):
        raise TypeError("resources must be a mapping")

    def resolve(item: Any, location: str) -> Any:
        if isinstance(item, EnvironmentReference):
            return _freeze_json(
                _resolved_output(item, artifacts=artifacts, resources=resources),
                f"resolved output {item.path}",
            )
        if isinstance(item, Mapping):
            for key in item:
                if not isinstance(key, str):
                    raise TypeError(f"{location} mapping keys must be strings")
            return MappingProxyType({key: resolve(nested, f"{location}.{key}") for key, nested in item.items()})
        if isinstance(item, (list, tuple)):
            return tuple(resolve(nested, f"{location}[{index}]") for index, nested in enumerate(item))
        return _freeze_json(item, location)

    return resolve(value, "$")


EnvironmentSpec = Union[ArtifactSpec, ResourceSpec]


def _ordered_specs(
    artifacts: Sequence[ArtifactSpec],
    resources: Sequence[ResourceSpec],
) -> tuple[EnvironmentSpec, ...]:
    artifact_entries = _as_tuple(artifacts, "artifacts")
    resource_entries = _as_tuple(resources, "resources")

    seen_artifacts = set()
    for spec in artifact_entries:
        if not isinstance(spec, ArtifactSpec):
            raise TypeError("artifacts must contain ArtifactSpec values")
        if spec.artifact_id in seen_artifacts:
            raise ValueError(f"duplicate artifact_id {spec.artifact_id!r}")
        seen_artifacts.add(spec.artifact_id)

    seen_resources = set()
    for spec in resource_entries:
        if not isinstance(spec, ResourceSpec):
            raise TypeError("resources must contain ResourceSpec values")
        if spec.resource_id in seen_resources:
            raise ValueError(f"duplicate resource_id {spec.resource_id!r}")
        seen_resources.add(spec.resource_id)

    return artifact_entries + resource_entries


def _references_for_spec(spec: EnvironmentSpec) -> tuple[EnvironmentReference, ...]:
    return collect_environment_references(spec.config)


def _dependencies_for_spec(spec: EnvironmentSpec) -> tuple[EnvironmentDependency, ...]:
    dependencies = []
    seen = set()
    for raw_dependency in spec.depends_on:
        dependency = parse_environment_dependency(raw_dependency)
        if dependency not in seen:
            seen.add(dependency)
            dependencies.append(dependency)
    for reference in _references_for_spec(spec):
        dependency = reference.dependency
        if dependency not in seen:
            seen.add(dependency)
            dependencies.append(dependency)
    return tuple(dependencies)


def _source_name(spec: EnvironmentSpec) -> str:
    return spec.dependency.path


def _registration_for_target(
    spec: EnvironmentSpec,
    artifact_registry: ArtifactProviderRegistry,
    resource_registry: ResourceProviderRegistry,
) -> Union[ArtifactProviderRegistration, ResourceProviderRegistration]:
    if isinstance(spec, ArtifactSpec):
        return artifact_registry.resolve(spec.provider_id)
    return resource_registry.resolve(spec.provider_id)


def _validate_reference(
    source: str,
    reference: EnvironmentReference,
    nodes: Mapping[EnvironmentDependency, EnvironmentSpec],
    artifact_registry: ArtifactProviderRegistry,
    resource_registry: ResourceProviderRegistry,
) -> None:
    target = nodes.get(reference.dependency)
    if target is None:
        raise UnknownEnvironmentTargetError(source, reference.dependency)
    registration = _registration_for_target(target, artifact_registry, resource_registry)
    if reference.output not in registration.declared_outputs:
        raise UndeclaredEnvironmentOutputError(
            source,
            reference,
            registration.provider_id,
            registration.declared_outputs,
        )


def _find_dependency_cycle(
    ordered_nodes: Sequence[EnvironmentDependency],
    dependencies: Mapping[EnvironmentDependency, Sequence[EnvironmentDependency]],
) -> tuple[EnvironmentDependency, ...]:
    visiting = set()
    visited = set()
    stack = []

    def visit(node: EnvironmentDependency) -> Optional[tuple[EnvironmentDependency, ...]]:
        if node in visited:
            return None
        if node in visiting:
            start = stack.index(node)
            return tuple([*stack[start:], node])
        visiting.add(node)
        stack.append(node)
        for dependency in dependencies[node]:
            cycle = visit(dependency)
            if cycle is not None:
                return cycle
        stack.pop()
        visiting.remove(node)
        visited.add(node)
        return None

    for node in ordered_nodes:
        cycle = visit(node)
        if cycle is not None:
            return cycle
    return ()


def _environment_graph_unpinned(
    artifacts: Sequence[ArtifactSpec],
    resources: Sequence[ResourceSpec],
    policies: Sequence[PolicySpec],
    artifact_registry: ArtifactProviderRegistry,
    resource_registry: ResourceProviderRegistry,
) -> tuple[
    tuple[EnvironmentSpec, ...],
    Mapping[EnvironmentDependency, tuple[EnvironmentDependency, ...]],
]:
    if not isinstance(artifact_registry, ArtifactProviderRegistry):
        raise TypeError("artifact_registry must be an ArtifactProviderRegistry")
    if not isinstance(resource_registry, ResourceProviderRegistry):
        raise TypeError("resource_registry must be a ResourceProviderRegistry")

    ordered_specs = _ordered_specs(artifacts, resources)
    policy_entries = _as_tuple(policies, "policies")
    seen_policies = set()
    for policy in policy_entries:
        if not isinstance(policy, PolicySpec):
            raise TypeError("policies must contain PolicySpec values")
        if policy.policy_id in seen_policies:
            raise ValueError(f"duplicate policy_id {policy.policy_id!r}")
        seen_policies.add(policy.policy_id)

    nodes = {spec.dependency: spec for spec in ordered_specs}
    dependencies: dict[EnvironmentDependency, tuple[EnvironmentDependency, ...]] = {}

    for spec in ordered_specs:
        if isinstance(spec, ArtifactSpec):
            artifact_registry.validate_spec(spec)
        else:
            resource_registry.validate_spec(spec)

        source = _source_name(spec)
        for reference in _references_for_spec(spec):
            _validate_reference(
                source,
                reference,
                nodes,
                artifact_registry,
                resource_registry,
            )

        spec_dependencies = _dependencies_for_spec(spec)
        for dependency in spec_dependencies:
            if dependency not in nodes:
                raise UnknownEnvironmentTargetError(source, dependency)
        dependencies[spec.dependency] = spec_dependencies

    for policy in policy_entries:
        source = f"policies.{policy.policy_id}"
        for reference in collect_environment_references(policy.config):
            _validate_reference(
                source,
                reference,
                nodes,
                artifact_registry,
                resource_registry,
            )

    ordered_nodes = tuple(spec.dependency for spec in ordered_specs)
    cycle = _find_dependency_cycle(ordered_nodes, dependencies)
    if cycle:
        raise EnvironmentDependencyCycleError(cycle)
    return ordered_specs, MappingProxyType(dependencies)


def _snapshot_changed(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
) -> bool:
    return set(before) != set(after) or any(
        after[provider_id] is not registration for provider_id, registration in before.items()
    )


def _changed_registry_name(
    artifact_changed: bool,
    resource_changed: bool,
) -> str:
    if artifact_changed and resource_changed:
        return "artifact and resource provider registries"
    if artifact_changed:
        return "artifact provider registry"
    return "resource provider registry"


def _restore_provider_registry_snapshots(
    artifact_registry: ArtifactProviderRegistry,
    artifact_snapshot: Mapping[str, ArtifactProviderRegistration],
    resource_registry: ResourceProviderRegistry,
    resource_snapshot: Mapping[str, ResourceProviderRegistration],
) -> tuple[BaseException, ...]:
    failures = []
    for registry, snapshot in (
        (resource_registry, resource_snapshot),
        (artifact_registry, artifact_snapshot),
    ):
        try:
            registry.restore_snapshot(snapshot)
        except BaseException as exc:
            failures.append(exc)
    return tuple(failures)


@contextmanager
def _provider_registry_transaction(
    artifact_registry: ArtifactProviderRegistry,
    resource_registry: ResourceProviderRegistry,
) -> Iterator[None]:
    """Pin both provider registries for one static validation operation."""

    if not isinstance(artifact_registry, ArtifactProviderRegistry):
        raise TypeError("artifact_registry must be an ArtifactProviderRegistry")
    if not isinstance(resource_registry, ResourceProviderRegistry):
        raise TypeError("resource_registry must be a ResourceProviderRegistry")

    with ExitStack() as locks:
        locks.enter_context(artifact_registry.registration_lock)
        locks.enter_context(resource_registry.registration_lock)
        artifact_snapshot = artifact_registry.snapshot()
        resource_snapshot = resource_registry.snapshot()
        try:
            yield
        except BaseException as primary:
            artifact_changed = _snapshot_changed(artifact_snapshot, artifact_registry.snapshot())
            resource_changed = _snapshot_changed(resource_snapshot, resource_registry.snapshot())
            if not artifact_changed and not resource_changed:
                raise

            restore_failures = _restore_provider_registry_snapshots(
                artifact_registry,
                artifact_snapshot,
                resource_registry,
                resource_snapshot,
            )
            if not isinstance(primary, Exception):
                for failure in restore_failures:
                    add_note = getattr(primary, "add_note", None)
                    if add_note is not None:
                        add_note(
                            "restoring provider registries after cancellation also failed with "
                            f"{type(failure).__name__}"
                        )
                raise

            error = EnvironmentRegistryChangedError(_changed_registry_name(artifact_changed, resource_changed))
            if restore_failures:
                add_note = getattr(error, "add_note", None)
                if add_note is not None:
                    add_note(
                        "restoring the provider registry snapshot also failed with "
                        + ", ".join(type(failure).__name__ for failure in restore_failures)
                    )
            raise error from primary

        artifact_changed = _snapshot_changed(artifact_snapshot, artifact_registry.snapshot())
        resource_changed = _snapshot_changed(resource_snapshot, resource_registry.snapshot())
        if not artifact_changed and not resource_changed:
            return

        restore_failures = _restore_provider_registry_snapshots(
            artifact_registry,
            artifact_snapshot,
            resource_registry,
            resource_snapshot,
        )
        error = EnvironmentRegistryChangedError(_changed_registry_name(artifact_changed, resource_changed))
        if restore_failures:
            add_note = getattr(error, "add_note", None)
            if add_note is not None:
                add_note(
                    "restoring the provider registry snapshot also failed with "
                    + ", ".join(type(failure).__name__ for failure in restore_failures)
                )
            raise error from restore_failures[0]
        raise error


def _environment_graph(
    artifacts: Sequence[ArtifactSpec],
    resources: Sequence[ResourceSpec],
    policies: Sequence[PolicySpec],
    artifact_registry: ArtifactProviderRegistry,
    resource_registry: ResourceProviderRegistry,
) -> tuple[
    tuple[EnvironmentSpec, ...],
    Mapping[EnvironmentDependency, tuple[EnvironmentDependency, ...]],
]:
    with _provider_registry_transaction(artifact_registry, resource_registry):
        return _environment_graph_unpinned(
            artifacts,
            resources,
            policies,
            artifact_registry,
            resource_registry,
        )


def validate_environment_dependencies(
    artifacts: Sequence[ArtifactSpec],
    resources: Sequence[ResourceSpec],
    *,
    artifact_registry: ArtifactProviderRegistry,
    resource_registry: ResourceProviderRegistry,
    policies: Sequence[PolicySpec] = (),
) -> None:
    """Validate providers, output references, and the combined environment DAG."""

    _environment_graph(
        artifacts,
        resources,
        policies,
        artifact_registry,
        resource_registry,
    )


def stable_environment_order(
    artifacts: Sequence[ArtifactSpec],
    resources: Sequence[ResourceSpec],
    *,
    artifact_registry: ArtifactProviderRegistry,
    resource_registry: ResourceProviderRegistry,
    policies: Sequence[PolicySpec] = (),
) -> tuple[EnvironmentSpec, ...]:
    """Return a dependency order, preserving declaration order among ready nodes.

    The declaration order is artifacts followed by resources. References and
    explicit ``depends_on`` entries participate in the same cross-kind DAG.
    """

    ordered_specs, dependencies = _environment_graph(
        artifacts,
        resources,
        policies,
        artifact_registry,
        resource_registry,
    )
    by_node = {spec.dependency: spec for spec in ordered_specs}
    index = {spec.dependency: position for position, spec in enumerate(ordered_specs)}
    indegree = {node: len(node_dependencies) for node, node_dependencies in dependencies.items()}
    dependents: dict[EnvironmentDependency, list[EnvironmentDependency]] = {node: [] for node in by_node}
    for node, node_dependencies in dependencies.items():
        for dependency in node_dependencies:
            dependents[dependency].append(node)

    ready = sorted(
        (node for node in by_node if indegree[node] == 0),
        key=index.__getitem__,
    )
    result = []
    while ready:
        node = ready.pop(0)
        result.append(by_node[node])
        newly_ready = []
        for dependent in dependents[node]:
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                newly_ready.append(dependent)
        if newly_ready:
            ready.extend(newly_ready)
            ready.sort(key=index.__getitem__)

    if len(result) != len(ordered_specs):  # Defensive: graph validation already detects this.
        cycle = _find_dependency_cycle(tuple(by_node), dependencies)
        raise EnvironmentDependencyCycleError(cycle)
    return tuple(result)


__all__ = [
    "ENVIRONMENT_EXTENSION_API_VERSION",
    "ENVIRONMENT_REFERENCE_DATA_KEY",
    "ArtifactProvider",
    "ArtifactProviderRegistration",
    "ArtifactProviderRegistry",
    "ArtifactProviderResolvedConfigValidator",
    "ArtifactSpec",
    "DuplicateProviderRegistrationError",
    "EnvironmentDependency",
    "EnvironmentDependencyCycleError",
    "EnvironmentKind",
    "EnvironmentProviderContext",
    "EnvironmentReference",
    "EnvironmentReferenceError",
    "EnvironmentRegistryChangedError",
    "EnvironmentSpec",
    "IncompatibleProviderRegistrationError",
    "InvalidEnvironmentReferenceError",
    "PolicySpec",
    "ProviderContractError",
    "ProviderRegistrationError",
    "ProviderResultError",
    "ProvisionedResource",
    "ResolvedArtifact",
    "ResourceProvider",
    "ResourceProviderRegistration",
    "ResourceProviderRegistry",
    "ResourceProviderResolvedConfigValidator",
    "ResourceSpec",
    "SupportDecision",
    "UndeclaredEnvironmentOutputError",
    "UnknownEnvironmentTargetError",
    "UnknownProviderError",
    "UnresolvedEnvironmentReferenceError",
    "collect_environment_references",
    "environment_references_from_data",
    "parse_environment_dependency",
    "parse_environment_reference",
    "resolve_environment_references",
    "stable_environment_order",
    "validate_environment_dependencies",
]
