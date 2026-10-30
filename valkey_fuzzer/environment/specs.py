"""Environment declarations acquired before the deployment: artifacts, resources, and policies.

An artifact (a binary, module, dataset) is *resolved* and a resource (a
filesystem, store, proxy) is *provisioned* by an extension-provided provider
before the deployment starts. Both share one model; the artifact and resource
names below are thin, kind-specific spellings of it. Scenarios reference a
provider output exactly as ``{$ref: artifacts.<id>.<output>}`` or
``{$ref: resources.<id>.<output>}``.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, ClassVar, Optional, Union

from .._data import (
    EnvironmentDependency,
    EnvironmentKind,
    as_tuple,
    freeze_mapping,
    parse_environment_dependency,
    require_qualified_id,
    require_segment,
    require_text,
    to_plain_data,
)

# ---------------------------------------------------------------------------
# Declarations
# ---------------------------------------------------------------------------


def _check_spec(spec: Any, id_field: str) -> None:
    require_segment(getattr(spec, id_field), id_field)
    require_qualified_id(spec.provider_id, "provider_id")
    object.__setattr__(spec, "config", freeze_mapping(spec.config, f"{spec.kind.value} config", references=True))
    dependencies = tuple(parse_environment_dependency(item) for item in as_tuple(spec.depends_on, "depends_on"))
    if len(set(dependencies)) != len(dependencies):
        raise ValueError("depends_on contains duplicate dependencies")
    if spec.dependency in dependencies:
        raise ValueError(f"{spec.dependency.path} cannot depend on itself")
    object.__setattr__(spec, "depends_on", tuple(item.path for item in dependencies))


class _EnvironmentSpec:
    """Behavior shared by artifact and resource declarations."""

    kind: ClassVar[EnvironmentKind]

    @property
    def id(self) -> str:
        raise NotImplementedError

    @property
    def dependency(self) -> EnvironmentDependency:
        return EnvironmentDependency(self.kind, self.id)

    def to_data(self) -> Mapping[str, Any]:
        return to_plain_data(self)


@dataclass(frozen=True)
class ArtifactSpec(_EnvironmentSpec):
    """An artifact resolved before deployment setup. Config must not hold credentials."""

    artifact_id: str
    provider_id: str
    config: Mapping[str, Any] = field(default_factory=dict)
    depends_on: tuple[str, ...] = ()
    kind: ClassVar[EnvironmentKind] = EnvironmentKind.ARTIFACT

    def __post_init__(self) -> None:
        _check_spec(self, "artifact_id")

    @property
    def id(self) -> str:
        return self.artifact_id


@dataclass(frozen=True)
class ResourceSpec(_EnvironmentSpec):
    """A managed resource provisioned before deployment setup."""

    resource_id: str
    provider_id: str
    config: Mapping[str, Any] = field(default_factory=dict)
    depends_on: tuple[str, ...] = ()
    kind: ClassVar[EnvironmentKind] = EnvironmentKind.RESOURCE

    def __post_init__(self) -> None:
        _check_spec(self, "resource_id")

    @property
    def id(self) -> str:
        return self.resource_id


EnvironmentSpec = Union[ArtifactSpec, ResourceSpec]


@dataclass(frozen=True)
class PolicySpec:
    """Opaque policy intent carried to providers and verdict components."""

    policy_id: str
    policy_type: str
    config: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_segment(self.policy_id, "policy_id")
        require_qualified_id(self.policy_type, "policy_type")
        object.__setattr__(self, "config", freeze_mapping(self.config, "policy config", references=True))

    def to_data(self) -> Mapping[str, Any]:
        return to_plain_data(self)


@dataclass(frozen=True)
class SupportDecision:
    """A provider's report-safe answer, before any mutation, to "can you do this here?"."""

    supported: bool
    reason: Optional[str] = None
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.supported, bool):
            raise TypeError("supported must be a boolean")
        if self.supported and self.reason is not None:
            raise ValueError("a supported decision cannot include a rejection reason")
        if not self.supported:
            require_text(self.reason, "unsupported decision reason")
        object.__setattr__(self, "details", freeze_mapping(self.details, "support decision details"))

    @classmethod
    def allow(cls, *, details: Optional[Mapping[str, Any]] = None) -> "SupportDecision":
        return cls(supported=True, details=details or {})

    @classmethod
    def deny(cls, reason: str, *, details: Optional[Mapping[str, Any]] = None) -> "SupportDecision":
        return cls(supported=False, reason=reason, details=details or {})

    def to_data(self) -> Mapping[str, Any]:
        return to_plain_data(self)
