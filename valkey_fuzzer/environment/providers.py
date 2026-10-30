"""Environment provider contracts, results, registrations, and the dependency graph.

Providers resolve artifacts and provision resources; the graph functions
validate a plan's declarations against the registered providers and order
them so every declaration is acquired after what it depends on.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Callable, ClassVar, Optional, Protocol, Union, runtime_checkable

from .._data import (
    BaseRegistry,
    EnvironmentDependency,
    EnvironmentKind,
    EnvironmentReference,
    EnvironmentReferenceError,
    as_tuple,
    collect_environment_references,
    freeze,
    freeze_mapping,
    parse_environment_dependency,
    require_qualified_id,
    require_segment,
    stable_order,
    to_plain_data,
)
from ..schema import ConfigSchema, plain_config
from .specs import (
    ArtifactSpec,
    EnvironmentSpec,
    PolicySpec,
    ResourceSpec,
    SupportDecision,
)


class ProviderError(RuntimeError):
    """A provider broke its contract or returned an invalid result."""

    def __init__(self, provider_id: str, reason: str):
        self.provider_id = provider_id
        self.reason = reason
        super().__init__(f"provider {provider_id!r}: {reason}")


# ---------------------------------------------------------------------------
# Provider results
# ---------------------------------------------------------------------------


class _ProviderResult:
    """Runtime outputs plus report-safe provenance; only provenance is reported."""

    kind: ClassVar[EnvironmentKind]
    id_field: ClassVar[str]

    def _check(self) -> None:
        require_segment(getattr(self, self.id_field), self.id_field)
        require_qualified_id(self.provider_id, "provider_id")
        outputs = freeze_mapping(self.outputs, f"{self.kind.value} outputs")
        for name in outputs:
            require_segment(name, "output name")
        object.__setattr__(self, "outputs", outputs)
        object.__setattr__(self, "provenance", freeze_mapping(self.provenance, f"{self.kind.value} provenance"))

    @property
    def id(self) -> str:
        return getattr(self, self.id_field)

    @property
    def output_names(self) -> tuple[str, ...]:
        return tuple(sorted(self.outputs))

    def to_data(self) -> Mapping[str, Any]:
        """A report-safe record: output names and provenance, never output values."""

        return {
            self.id_field: self.id,
            "provider_id": self.provider_id,
            "output_names": list(self.output_names),
            "provenance": to_plain_data(self.provenance),
        }


@dataclass(frozen=True)
class ResolvedArtifact(_ProviderResult):
    artifact_id: str
    provider_id: str
    outputs: Mapping[str, Any] = field(repr=False, compare=False, metadata={"serialize": False})
    provenance: Mapping[str, Any] = field(default_factory=dict)
    kind: ClassVar[EnvironmentKind] = EnvironmentKind.ARTIFACT
    id_field: ClassVar[str] = "artifact_id"

    def __post_init__(self) -> None:
        self._check()


@dataclass(frozen=True)
class ProvisionedResource(_ProviderResult):
    resource_id: str
    provider_id: str
    outputs: Mapping[str, Any] = field(repr=False, compare=False, metadata={"serialize": False})
    provenance: Mapping[str, Any] = field(default_factory=dict)
    kind: ClassVar[EnvironmentKind] = EnvironmentKind.RESOURCE
    id_field: ClassVar[str] = "resource_id"

    def __post_init__(self) -> None:
        self._check()


EnvironmentResult = Union[ResolvedArtifact, ProvisionedResource]


# ---------------------------------------------------------------------------
# Provider contracts and registrations
# ---------------------------------------------------------------------------


@runtime_checkable
class EnvironmentProviderContext(Protocol):
    """Services available to a provider for one declaration.

    ``deadline`` is an absolute monotonic time or ``None``; ``artifact_dir`` is
    a directory private to this declaration. Event details must be report-safe.
    """

    @property
    def run_id(self) -> str: ...

    @property
    def seed(self) -> int: ...

    @property
    def deadline(self) -> Optional[float]: ...

    @property
    def artifact_dir(self) -> str: ...

    def publish_event(self, name: str, details: Mapping[str, Any]) -> None: ...


@runtime_checkable
class ArtifactProvider(Protocol):
    """Resolves an artifact. ``check_support`` must not mutate anything;
    ``rollback`` follows a failed ``resolve`` and ``cleanup`` a successful one."""

    def check_support(
        self, context: EnvironmentProviderContext, spec: ArtifactSpec, resolved_config: Mapping[str, Any]
    ) -> SupportDecision: ...

    def resolve(
        self, context: EnvironmentProviderContext, spec: ArtifactSpec, resolved_config: Mapping[str, Any]
    ) -> ResolvedArtifact: ...

    def rollback(self, context: EnvironmentProviderContext, spec: ArtifactSpec) -> None: ...

    def cleanup(self, context: EnvironmentProviderContext, spec: ArtifactSpec, artifact: ResolvedArtifact) -> None: ...


@runtime_checkable
class ResourceProvider(Protocol):
    """Provisions a resource, with the same lifecycle as :class:`ArtifactProvider`."""

    def check_support(
        self, context: EnvironmentProviderContext, spec: ResourceSpec, resolved_config: Mapping[str, Any]
    ) -> SupportDecision: ...

    def provision(
        self, context: EnvironmentProviderContext, spec: ResourceSpec, resolved_config: Mapping[str, Any]
    ) -> ProvisionedResource: ...

    def rollback(self, context: EnvironmentProviderContext, spec: ResourceSpec) -> None: ...

    def cleanup(
        self, context: EnvironmentProviderContext, spec: ResourceSpec, resource: ProvisionedResource
    ) -> None: ...


@dataclass(frozen=True)
class _ProviderRegistration:
    """A provider factory, its declared outputs, and the schema of its config.

    :meth:`resolve_config` checks a declaration's config through ``config``
    and fills in its defaults; without ``config`` the config is taken as
    written. The compiler records the result in the plan, and the provisioner
    resolves the concrete config (references replaced by their outputs) the
    same way and passes it to the provider. ``check(spec, config)`` adds the
    provider's own pure checks of the resolved config; at compile time,
    reference fields may still hold references.
    """

    provider_id: str
    factory: Callable[[], Any]
    outputs: tuple[str, ...] = ()
    config: Optional[ConfigSchema] = None
    check: Optional[Callable[[Any, Mapping[str, Any]], None]] = None

    kind: ClassVar[EnvironmentKind]
    result_type: ClassVar[type]
    acquire_method: ClassVar[str]

    def __post_init__(self) -> None:
        require_qualified_id(self.provider_id, "provider_id")
        for name, value in (("factory", self.factory), ("check", self.check)):
            if value is not None and not callable(value):
                raise TypeError(f"provider {name} must be callable")
        if self.config is not None and not isinstance(self.config, ConfigSchema):
            raise TypeError("provider config must be a ConfigSchema")
        outputs = as_tuple(self.outputs, "outputs")
        for output in outputs:
            require_segment(output, "declared output")
        if len(set(outputs)) != len(outputs):
            raise ValueError("outputs contains duplicate declarations")
        object.__setattr__(self, "outputs", outputs)

    def resolve_config(self, spec: EnvironmentSpec, config: Optional[Mapping[str, Any]] = None) -> dict[str, Any]:
        """``config``, by default the declaration's own, checked and with its defaults filled in.

        Raises :class:`~valkey_fuzzer.schema.ConfigError` naming the field
        (under ``<kind>.<id>.with``), or whatever ``check`` raises.
        """

        value = spec.config if config is None else config
        location = f"{spec.dependency.path}.with"
        resolved = plain_config(value) if self.config is None else self.config.resolve(value, location)
        if self.check is not None and self.check(spec, resolved) is not None:
            raise ProviderError(self.provider_id, "check must return None")
        return resolved

    def validate_result(self, spec: EnvironmentSpec, result: Any) -> None:
        if not isinstance(result, self.result_type):
            raise ProviderError(self.provider_id, f"expected {self.result_type.__name__}")
        if result.provider_id != self.provider_id or result.id != spec.id:
            raise ProviderError(self.provider_id, f"returned a result for {result.provider_id}/{result.id}")
        missing = sorted(set(self.outputs) - set(result.outputs))
        unexpected = sorted(set(result.outputs) - set(self.outputs))
        if missing or unexpected:
            raise ProviderError(
                self.provider_id,
                f"returned outputs that do not match its declaration (missing {missing}, extra {unexpected})",
            )


class ArtifactProviderRegistration(_ProviderRegistration):
    kind = EnvironmentKind.ARTIFACT
    result_type = ResolvedArtifact
    acquire_method = "resolve"


class ResourceProviderRegistration(_ProviderRegistration):
    kind = EnvironmentKind.RESOURCE
    result_type = ProvisionedResource
    acquire_method = "provision"


class ArtifactProviderRegistry(BaseRegistry[ArtifactProviderRegistration]):
    label = "artifact provider"
    entry_type = ArtifactProviderRegistration

    def key(self, entry: ArtifactProviderRegistration) -> str:
        return entry.provider_id


class ResourceProviderRegistry(BaseRegistry[ResourceProviderRegistration]):
    label = "resource provider"
    entry_type = ResourceProviderRegistration

    def key(self, entry: ResourceProviderRegistration) -> str:
        return entry.provider_id


# ---------------------------------------------------------------------------
# References and the dependency graph
# ---------------------------------------------------------------------------


def resolve_environment_references(
    value: Any,
    *,
    artifacts: Mapping[str, ResolvedArtifact],
    resources: Mapping[str, ProvisionedResource],
) -> Any:
    """Replace every reference in ``value`` with the output it names (frozen)."""

    def output(reference: EnvironmentReference) -> Any:
        results = artifacts if reference.kind is EnvironmentKind.ARTIFACT else resources
        record = results.get(reference.id)
        if record is None or reference.output not in record.outputs:
            raise EnvironmentReferenceError(f"cannot resolve {reference.path!r}: it has not been acquired")
        return record.outputs[reference.output]

    def resolve(item: Any) -> Any:
        if isinstance(item, EnvironmentReference):
            return output(item)
        if isinstance(item, Mapping):
            return {key: resolve(nested) for key, nested in item.items()}
        if isinstance(item, (list, tuple)):
            return [resolve(nested) for nested in item]
        return item

    return freeze(resolve(value), "resolved value")


def _registration(spec: EnvironmentSpec, artifact_registry: Any, resource_registry: Any) -> _ProviderRegistration:
    registry = artifact_registry if spec.kind is EnvironmentKind.ARTIFACT else resource_registry
    return registry.resolve(spec.provider_id)


def check_references(
    source: str,
    value: Any,
    declarations: Mapping[EnvironmentDependency, EnvironmentSpec],
    artifact_registry: ArtifactProviderRegistry,
    resource_registry: ResourceProviderRegistry,
) -> None:
    """Every reference in ``value`` must name a declaration and a declared output."""

    for reference in collect_environment_references(value):
        target = declarations.get(reference.dependency)
        if target is None:
            raise EnvironmentReferenceError(
                f"{source} references unknown environment target {reference.dependency.path!r}"
            )
        registration = _registration(target, artifact_registry, resource_registry)
        if reference.output not in registration.outputs:
            raise EnvironmentReferenceError(
                f"{source} references undeclared output {reference.output!r} on {reference.dependency.path!r}; "
                f"provider {registration.provider_id!r} declares {list(registration.outputs) or 'none'}"
            )


def stable_environment_order(
    artifacts: Sequence[ArtifactSpec],
    resources: Sequence[ResourceSpec],
    *,
    artifact_registry: ArtifactProviderRegistry,
    resource_registry: ResourceProviderRegistry,
    policies: Sequence[PolicySpec] = (),
) -> tuple[EnvironmentSpec, ...]:
    """Validate the declarations against their providers and return them in dependency order.

    Providers must be registered and accept their declaration, references must
    name declared outputs, and explicit ``depends_on`` plus references must form
    an acyclic graph. Among ready declarations, declaration order (artifacts,
    then resources) is preserved. The plan already holds well-typed, unique
    declarations and policy IDs.
    """

    specs: tuple[EnvironmentSpec, ...] = (*artifacts, *resources)
    declarations = {spec.dependency: spec for spec in specs}

    edges: dict[EnvironmentDependency, tuple[EnvironmentDependency, ...]] = {}
    for spec in specs:
        _registration(spec, artifact_registry, resource_registry).resolve_config(spec)
        check_references(spec.dependency.path, spec.config, declarations, artifact_registry, resource_registry)
        targets = [parse_environment_dependency(item) for item in spec.depends_on]
        targets += [reference.dependency for reference in collect_environment_references(spec.config)]
        for target in targets:
            if target not in declarations:
                raise EnvironmentReferenceError(
                    f"{spec.dependency.path} references unknown environment target {target.path!r}"
                )
        edges[spec.dependency] = tuple(dict.fromkeys(targets))
    for policy in policies:
        check_references(
            f"policies.{policy.policy_id}", policy.config, declarations, artifact_registry, resource_registry
        )

    return tuple(declarations[node] for node in stable_order(tuple(declarations), edges, "environment"))
