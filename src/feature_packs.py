"""Data-only feature-pack catalog and generic scenario compiler.

The compiler turns one strict scenario intent plus an installed feature-pack
manifest into the immutable :class:`~src.extensibility.RunPlan` consumed by
the generic runner. The authored format intentionally has no public schema
generation selector: there is one execution model and one validation path.
"""

import inspect
import math
import re
import threading
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Optional, TextIO, Union

import yaml

from .environment import (
    ENVIRONMENT_REFERENCE_DATA_KEY,
    ArtifactProviderRegistry,
    ArtifactSpec,
    EnvironmentKind,
    EnvironmentReference,
    InvalidEnvironmentReferenceError,
    PolicySpec,
    ResourceProviderRegistry,
    ResourceSpec,
    UndeclaredEnvironmentOutputError,
    UnknownEnvironmentTargetError,
    _provider_registry_transaction,
    collect_environment_references,
    parse_environment_reference,
    validate_environment_dependencies,
)
from .extensibility import (
    EXTENSION_API_VERSION,
    ComponentKind,
    ComponentSpec,
    EvidenceRequirement,
    FeaturePackSpec,
    NodeSelector,
    Registry,
    RunMode,
    RunPlan,
    StartupOverlay,
)

_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_INPUT_SEGMENT = r"[A-Za-z0-9][A-Za-z0-9_-]*"
_INPUT_SEGMENT_PATTERN = re.compile(rf"^{_INPUT_SEGMENT}$")
_REF_PATTERN = re.compile(rf"^inputs\.({_INPUT_SEGMENT}(?:\.{_INPUT_SEGMENT})*)$")
_ENVIRONMENT_REFERENCE_NAMESPACES = frozenset(kind.value for kind in EnvironmentKind)
_LITERAL_MARKER_KEY = "$valkey_fuzzer_literal"


class FeaturePackError(ValueError):
    """Base class for catalog, manifest, and compilation failures."""


class ScenarioCompileError(FeaturePackError):
    """The scenario cannot be compiled safely."""


class FeaturePackDocumentError(ScenarioCompileError):
    """A data-only manifest or scenario document is malformed."""


class DuplicateFeaturePackError(FeaturePackError):
    def __init__(self, pack_id: str, version: str):
        self.pack_id = pack_id
        self.version = version
        super().__init__(f"feature pack {pack_id!r} version {version!r} is already registered")


class UnknownFeaturePackError(FeaturePackError):
    def __init__(self, pack_id: str, version: Optional[str] = None):
        self.pack_id = pack_id
        self.version = version
        if version is None:
            message = f"feature pack {pack_id!r} is not registered"
        else:
            message = f"feature pack {pack_id!r} version {version!r} is not registered"
        super().__init__(message)


class AmbiguousFeaturePackError(FeaturePackError):
    def __init__(self, pack_id: str, versions: Sequence[str]):
        self.pack_id = pack_id
        self.versions = tuple(versions)
        super().__init__(
            f"feature pack {pack_id!r} has multiple registered versions "
            f"{list(self.versions)!r}; select one with feature.version"
        )


class IncompatibleFeaturePackError(FeaturePackError):
    def __init__(self, pack_id: str, version: str, reason: str):
        self.pack_id = pack_id
        self.version = version
        self.reason = reason
        super().__init__(f"feature pack {pack_id!r} version {version!r} is incompatible: {reason}")


class UnresolvedReferenceError(ScenarioCompileError):
    """A manifest reference could not be resolved exactly."""


class ComponentCompatibilityError(ScenarioCompileError):
    """A feature pack references a missing or wrong-kind component."""


class UnsupportedResourceError(ScenarioCompileError):
    """A pack uses an artifact or label the selected topology cannot resolve."""


def _require_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise FeaturePackDocumentError(f"{field_name} must be a non-empty string without surrounding whitespace")
    return value


def _require_name(value: Any, field_name: str) -> str:
    text = _require_text(value, field_name)
    if not _NAME_PATTERN.fullmatch(text):
        raise FeaturePackDocumentError(f"{field_name} contains unsupported characters: {text!r}")
    return text


def _require_input_segment(value: Any, field_name: str) -> str:
    text = _require_text(value, field_name)
    if not _INPUT_SEGMENT_PATTERN.fullmatch(text):
        raise FeaturePackDocumentError(
            f"{field_name} must be one unambiguous path segment containing only "
            f"letters, digits, underscores, or hyphens and starting with a letter or digit: {text!r}"
        )
    return text


def _mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise FeaturePackDocumentError(f"{field_name} must be a mapping")
    if any(not isinstance(key, str) for key in value):
        raise FeaturePackDocumentError(f"{field_name} keys must be strings")
    return value


def _sequence(value: Any, field_name: str) -> tuple[Any, ...]:
    if isinstance(value, (str, bytes, bytearray, Mapping)) or not isinstance(value, Sequence):
        raise FeaturePackDocumentError(f"{field_name} must be a sequence")
    return tuple(value)


def _snapshot_data(value: Any, field_name: str) -> Any:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise FeaturePackDocumentError(f"{field_name} mapping keys must be strings")
        return MappingProxyType({key: _snapshot_data(item, f"{field_name}.{key}") for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_snapshot_data(item, f"{field_name}[{index}]") for index, item in enumerate(value))
    if isinstance(value, float) and not math.isfinite(value):
        raise FeaturePackDocumentError(f"{field_name} cannot contain NaN or infinity")
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise FeaturePackDocumentError(f"{field_name} must contain only JSON/YAML-safe values")


def _fields(
    value: Any,
    field_name: str,
    *,
    allowed: Iterable[str],
    required: Iterable[str] = (),
) -> Mapping[str, Any]:
    mapping = _mapping(value, field_name)
    allowed_fields = set(allowed)
    unknown = sorted(set(mapping) - allowed_fields)
    if unknown:
        raise FeaturePackDocumentError(f"{field_name} contains unknown fields: {unknown}")
    missing = sorted(set(required) - set(mapping))
    if missing:
        raise FeaturePackDocumentError(f"{field_name} is missing required fields: {missing}")
    return mapping


def _parse_artifacts(value: Any) -> tuple[ArtifactSpec, ...]:
    declarations = _mapping(value, "artifacts")
    result = []
    for artifact_id, declaration_value in declarations.items():
        _require_input_segment(artifact_id, "artifact id")
        declaration = _fields(
            declaration_value,
            f"artifacts.{artifact_id}",
            allowed=("use", "with", "depends_on"),
            required=("use",),
        )
        try:
            result.append(
                ArtifactSpec(
                    artifact_id=artifact_id,
                    provider_id=_require_text(declaration["use"], f"artifacts.{artifact_id}.use"),
                    config=_mapping(declaration.get("with", {}), f"artifacts.{artifact_id}.with"),
                    depends_on=_sequence(
                        declaration.get("depends_on", ()),
                        f"artifacts.{artifact_id}.depends_on",
                    ),
                )
            )
        except (TypeError, ValueError) as exc:
            raise FeaturePackDocumentError(f"artifacts.{artifact_id} is invalid: {exc}") from exc
    return tuple(result)


def _parse_resources(value: Any) -> tuple[ResourceSpec, ...]:
    declarations = _mapping(value, "resources")
    result = []
    for resource_id, declaration_value in declarations.items():
        _require_input_segment(resource_id, "resource id")
        declaration = _fields(
            declaration_value,
            f"resources.{resource_id}",
            allowed=("use", "with", "depends_on"),
            required=("use",),
        )
        try:
            result.append(
                ResourceSpec(
                    resource_id=resource_id,
                    provider_id=_require_text(declaration["use"], f"resources.{resource_id}.use"),
                    config=_mapping(declaration.get("with", {}), f"resources.{resource_id}.with"),
                    depends_on=_sequence(
                        declaration.get("depends_on", ()),
                        f"resources.{resource_id}.depends_on",
                    ),
                )
            )
        except (TypeError, ValueError) as exc:
            raise FeaturePackDocumentError(f"resources.{resource_id} is invalid: {exc}") from exc
    return tuple(result)


def _parse_policies(value: Any) -> tuple[PolicySpec, ...]:
    declarations = _mapping(value, "policies")
    result = []
    for policy_id, declaration_value in declarations.items():
        _require_input_segment(policy_id, "policy id")
        declaration = _fields(
            declaration_value,
            f"policies.{policy_id}",
            allowed=("use", "with"),
            required=("use",),
        )
        try:
            result.append(
                PolicySpec(
                    policy_id=policy_id,
                    policy_type=_require_text(declaration["use"], f"policies.{policy_id}.use"),
                    config=_mapping(declaration.get("with", {}), f"policies.{policy_id}.with"),
                )
            )
        except (TypeError, ValueError) as exc:
            raise FeaturePackDocumentError(f"policies.{policy_id} is invalid: {exc}") from exc
    return tuple(result)


def _parse_component(value: Any, index: int) -> ComponentSpec:
    mapping = _fields(
        value,
        f"components[{index}]",
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
    try:
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
    except (TypeError, ValueError) as exc:
        raise FeaturePackDocumentError(f"components[{index}]: {exc}") from exc


def _parse_selector(value: Any, overlay_index: int) -> NodeSelector:
    mapping = _fields(
        value,
        f"startup_overlays[{overlay_index}].selector",
        allowed=("roles", "node_ids", "labels"),
    )
    try:
        return NodeSelector(
            roles=mapping.get("roles", ()),
            node_ids=mapping.get("node_ids", ()),
            labels=mapping.get("labels", {}),
        )
    except (TypeError, ValueError) as exc:
        raise FeaturePackDocumentError(f"startup_overlays[{overlay_index}].selector: {exc}") from exc


def _parse_overlay(value: Any, index: int) -> StartupOverlay:
    mapping = _fields(
        value,
        f"startup_overlays[{index}]",
        allowed=("overlay_id", "selector", "server_config", "artifact", "conflict_policy"),
        required=("overlay_id",),
    )
    try:
        return StartupOverlay(
            overlay_id=mapping["overlay_id"],
            selector=_parse_selector(mapping.get("selector", {}), index),
            server_config=mapping.get("server_config", {}),
            artifact=mapping.get("artifact"),
            conflict_policy=mapping.get("conflict_policy", "override"),
        )
    except FeaturePackDocumentError:
        raise
    except (TypeError, ValueError) as exc:
        raise FeaturePackDocumentError(f"startup_overlays[{index}]: {exc}") from exc


def _parse_evidence_requirement(value: Any, index: int) -> EvidenceRequirement:
    mapping = _fields(
        value,
        f"required_evidence[{index}]",
        allowed=("evidence_id", "source", "min_count"),
        required=("evidence_id",),
    )
    try:
        return EvidenceRequirement(
            evidence_id=mapping["evidence_id"],
            source=mapping.get("source"),
            min_count=mapping.get("min_count", 1),
        )
    except (TypeError, ValueError) as exc:
        raise FeaturePackDocumentError(f"required_evidence[{index}]: {exc}") from exc


def parse_feature_pack_document(value: Mapping[str, Any]) -> FeaturePackSpec:
    """Parse one strict, data-only manifest mapping into an immutable spec."""

    mapping = _fields(
        value,
        "feature pack",
        allowed=(
            "pack_id",
            "version",
            "mode",
            "supported_topologies",
            "components",
            "input_schema",
            "startup_overlays",
            "required_events",
            "required_evidence",
            "metadata",
        ),
        required=("pack_id", "version", "supported_topologies", "components"),
    )
    components = tuple(
        _parse_component(component, index)
        for index, component in enumerate(_sequence(mapping["components"], "components"))
    )
    overlays = tuple(
        _parse_overlay(overlay, index)
        for index, overlay in enumerate(_sequence(mapping.get("startup_overlays", ()), "startup_overlays"))
    )
    required_evidence = tuple(
        _parse_evidence_requirement(requirement, index)
        for index, requirement in enumerate(_sequence(mapping.get("required_evidence", ()), "required_evidence"))
    )
    try:
        return FeaturePackSpec(
            pack_id=mapping["pack_id"],
            version=mapping["version"],
            mode=mapping.get("mode", RunMode.FEATURE),
            supported_topologies=mapping["supported_topologies"],
            components=components,
            input_schema=mapping.get("input_schema", {}),
            startup_overlays=overlays,
            required_events=mapping.get("required_events", ()),
            required_evidence=required_evidence,
            metadata=mapping.get("metadata", {}),
        )
    except (TypeError, ValueError) as exc:
        raise FeaturePackDocumentError(str(exc)) from exc


parse_feature_pack = parse_feature_pack_document


class _UniqueKeySafeLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(
    loader: _UniqueKeySafeLoader,
    node: yaml.nodes.MappingNode,
    deep: bool = False,
) -> Mapping[Any, Any]:
    loader.flatten_mapping(node)
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in mapping
        except TypeError as exc:
            raise yaml.constructor.ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                "found an unhashable key",
                key_node.start_mark,
            ) from exc
        if duplicate:
            raise yaml.constructor.ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"found duplicate key {key!r}",
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _load_unique_yaml(
    source: Union[str, bytes, TextIO],
    *,
    source_name: str,
) -> Any:
    if hasattr(source, "read"):
        source = source.read()
    if not isinstance(source, (str, bytes)):
        raise TypeError("source must be YAML text, bytes, or a text stream")
    try:
        return yaml.load(source, Loader=_UniqueKeySafeLoader)
    except (yaml.YAMLError, TypeError, ValueError) as exc:
        raise FeaturePackDocumentError(f"{source_name}: {exc}") from exc


def load_feature_pack_yaml(
    source: Union[str, bytes, TextIO],
    *,
    source_name: str = "<yaml>",
) -> FeaturePackSpec:
    """Safely parse one YAML feature-pack document.

    Strings are YAML content, not filesystem paths.  Use
    :func:`load_feature_pack_directory` for filesystem discovery.
    """

    try:
        value = _load_unique_yaml(source, source_name=source_name)
        return parse_feature_pack_document(value)
    except (FeaturePackDocumentError, TypeError, ValueError) as exc:
        if isinstance(exc, FeaturePackDocumentError) and str(exc).startswith(f"{source_name}:"):
            raise
        raise FeaturePackDocumentError(f"{source_name}: {exc}") from exc


def load_scenario_yaml(
    source: Union[str, bytes, TextIO],
    *,
    source_name: str = "<yaml>",
) -> Mapping[str, Any]:
    """Safely load one duplicate-free scenario mapping.

    This function only handles YAML safety and document shape.  Pass the
    returned mapping to :meth:`FeaturePackCompiler.compile` for the same
    generic envelope, feature-pack, and component preflight used by in-memory
    scenarios.
    """

    value = _load_unique_yaml(source, source_name=source_name)
    try:
        return _mapping(value, "scenario")
    except FeaturePackDocumentError as exc:
        raise FeaturePackDocumentError(f"{source_name}: {exc}") from exc


class FeaturePackCatalog:
    """Thread-safe feature-pack catalog with atomic batch registration."""

    def __init__(self):
        self._packs: dict[tuple[str, str], FeaturePackSpec] = {}
        self._lock = threading.RLock()

    def _validate_many(self, packs: Sequence[FeaturePackSpec]) -> None:
        seen = set()
        for pack in packs:
            if not isinstance(pack, FeaturePackSpec):
                raise TypeError("catalog entries must be FeaturePackSpec values")
            key = (pack.pack_id, pack.version)
            if key in seen or key in self._packs:
                raise DuplicateFeaturePackError(*key)
            seen.add(key)

    def validate_many(self, packs: Iterable[FeaturePackSpec]) -> None:
        entries = tuple(packs)
        with self._lock:
            self._validate_many(entries)

    def register(self, pack: FeaturePackSpec) -> None:
        self.register_many((pack,))

    def register_many(self, packs: Iterable[FeaturePackSpec]) -> None:
        entries = tuple(packs)
        with self._lock:
            self._validate_many(entries)
            for pack in entries:
                self._packs[(pack.pack_id, pack.version)] = pack

    def resolve(self, pack_id: str, version: Optional[str] = None) -> FeaturePackSpec:
        _require_text(pack_id, "pack_id")
        if version is not None:
            _require_text(version, "version")
        with self._lock:
            if version is not None:
                key = (pack_id, version)
                try:
                    return self._packs[key]
                except KeyError as exc:
                    raise UnknownFeaturePackError(*key) from exc

            matches = tuple(
                pack for (registered_id, _), pack in sorted(self._packs.items()) if registered_id == pack_id
            )
            if not matches:
                raise UnknownFeaturePackError(pack_id)
            if len(matches) > 1:
                raise AmbiguousFeaturePackError(pack_id, tuple(pack.version for pack in matches))
            return matches[0]

    @property
    def registered_keys(self) -> tuple[tuple[str, str], ...]:
        with self._lock:
            return tuple(sorted(self._packs))

    def snapshot(self) -> Mapping[tuple[str, str], FeaturePackSpec]:
        with self._lock:
            return MappingProxyType({key: self._packs[key] for key in sorted(self._packs)})

    @property
    def registration_lock(self) -> threading.RLock:
        """Lock used by cross-registry plugin discovery transactions."""

        return self._lock

    def restore_snapshot(
        self,
        snapshot: Mapping[tuple[str, str], FeaturePackSpec],
    ) -> None:
        """Restore a prior snapshot while a discovery transaction owns the lock."""

        if not isinstance(snapshot, Mapping):
            raise TypeError("catalog snapshot must be a mapping")
        with self._lock:
            self._packs = dict(snapshot)

    def __len__(self) -> int:
        with self._lock:
            return len(self._packs)

    def __contains__(self, key: object) -> bool:
        with self._lock:
            return key in self._packs


def load_feature_pack_directory(
    directory: Union[str, Path],
    *,
    catalog: Optional[FeaturePackCatalog] = None,
) -> tuple[FeaturePackSpec, ...]:
    """Load immediate ``.yaml``/``.yml`` manifests in deterministic order.

    Every file is parsed before the optional catalog is changed, so a malformed
    later file or a duplicate pack leaves the catalog untouched.
    """

    path = Path(directory)
    if not path.is_dir():
        raise FeaturePackDocumentError(f"feature-pack directory does not exist: {path}")
    files = sorted(
        (item for item in path.iterdir() if item.is_file() and item.suffix.lower() in (".yaml", ".yml")),
        key=lambda item: (item.name.casefold(), item.name),
    )
    packs = []
    for manifest_path in files:
        try:
            text = manifest_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise FeaturePackDocumentError(f"{manifest_path}: {exc}") from exc
        packs.append(load_feature_pack_yaml(text, source_name=str(manifest_path)))

    entries = tuple(packs)
    validator = catalog if catalog is not None else FeaturePackCatalog()
    validator.validate_many(entries)
    if catalog is not None:
        catalog.register_many(entries)
    return entries


@dataclass(frozen=True)
class ReferenceInventory:
    """Topology-owned artifact and node-label values available at compile time."""

    artifacts: tuple[str, ...] = ()
    labels: Mapping[str, Optional[tuple[Any, ...]]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        artifacts = _sequence(self.artifacts, "reference inventory artifacts")
        for artifact in artifacts:
            _require_name(artifact, "artifact")
        if len(set(artifacts)) != len(artifacts):
            raise FeaturePackDocumentError("reference inventory artifacts contains duplicates")
        object.__setattr__(self, "artifacts", artifacts)

        labels = _mapping(self.labels, "reference inventory labels")
        normalized = {}
        for name, allowed_values in labels.items():
            _require_name(name, "label name")
            if allowed_values is None:
                normalized[name] = None
                continue
            normalized[name] = tuple(
                _snapshot_data(item, f"allowed values for label {name!r}")
                for item in _sequence(allowed_values, f"allowed values for label {name!r}")
            )
        object.__setattr__(self, "labels", MappingProxyType(normalized))


_INPUT_SCHEMA_FIELDS = {
    "$schema",
    "additionalProperties",
    "description",
    "properties",
    "required",
    "title",
    "type",
}
_INPUT_PROPERTY_FIELDS = {
    "default",
    "description",
    "enum",
    "maximum",
    "minimum",
    "reference_kinds",
    "title",
    "type",
}
_JSON_TYPES = {"array", "boolean", "integer", "null", "number", "object", "string"}
_ENVIRONMENT_REFERENCE_INPUT_TYPE = "environment_reference"
_INPUT_TYPES = _JSON_TYPES | {_ENVIRONMENT_REFERENCE_INPUT_TYPE}
_ENVIRONMENT_REFERENCE_INCOMPATIBLE_FIELDS = {
    "default",
    "enum",
    "maximum",
    "minimum",
}


def _matches_json_type(value: Any, type_name: str) -> bool:
    if type_name == "null":
        return value is None
    if type_name == "boolean":
        return isinstance(value, bool)
    if type_name == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if type_name == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
    if type_name == "string":
        return isinstance(value, str)
    if type_name == "array":
        return isinstance(value, (list, tuple))
    if type_name == "object":
        return isinstance(value, Mapping)
    return False


def _json_equal(left: Any, right: Any) -> bool:
    """Compare values using JSON data-model types, not Python coercions."""

    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) and left == right
    if left is None or right is None:
        return left is None and right is None
    if (
        isinstance(left, (int, float))
        and not isinstance(left, bool)
        and isinstance(right, (int, float))
        and not isinstance(right, bool)
    ):
        return left == right
    if isinstance(left, str) or isinstance(right, str):
        return isinstance(left, str) and isinstance(right, str) and left == right
    if isinstance(left, Mapping) or isinstance(right, Mapping):
        if not isinstance(left, Mapping) or not isinstance(right, Mapping):
            return False
        return set(left) == set(right) and all(_json_equal(left[key], right[key]) for key in left)
    if isinstance(left, (list, tuple)) or isinstance(right, (list, tuple)):
        if not isinstance(left, (list, tuple)) or not isinstance(right, (list, tuple)):
            return False
        return len(left) == len(right) and all(
            _json_equal(left_item, right_item) for left_item, right_item in zip(left, right)
        )
    return False


def _validate_input_rule(name: str, rule_value: Any) -> Mapping[str, Any]:
    rule = _fields(
        rule_value,
        f"input_schema.properties.{name}",
        allowed=_INPUT_PROPERTY_FIELDS,
    )
    if "type" in rule:
        type_name = rule["type"]
        if not isinstance(type_name, str) or type_name not in _INPUT_TYPES:
            raise ScenarioCompileError(f"input {name!r} has unsupported schema type {type_name!r}")
    if "reference_kinds" in rule:
        if rule.get("type") != _ENVIRONMENT_REFERENCE_INPUT_TYPE:
            raise ScenarioCompileError(
                f"input {name!r} can use reference_kinds only with type {_ENVIRONMENT_REFERENCE_INPUT_TYPE!r}"
            )
        reference_kinds = _sequence(
            rule["reference_kinds"],
            f"input_schema.properties.{name}.reference_kinds",
        )
        if not reference_kinds:
            raise ScenarioCompileError(f"input {name!r} has an empty reference_kinds constraint")
        normalized_kinds = []
        for reference_kind in reference_kinds:
            try:
                normalized_kinds.append(EnvironmentKind(reference_kind))
            except (TypeError, ValueError) as exc:
                raise ScenarioCompileError(
                    f"input {name!r} has unsupported environment reference kind "
                    f"{reference_kind!r}; expected 'artifacts' or 'resources'"
                ) from exc
        if len(set(normalized_kinds)) != len(normalized_kinds):
            raise ScenarioCompileError(f"input {name!r} has duplicate reference_kinds")
    if rule.get("type") == _ENVIRONMENT_REFERENCE_INPUT_TYPE:
        incompatible_fields = sorted(set(rule) & _ENVIRONMENT_REFERENCE_INCOMPATIBLE_FIELDS)
        if incompatible_fields:
            raise ScenarioCompileError(
                f"input {name!r} with type {_ENVIRONMENT_REFERENCE_INPUT_TYPE!r} "
                f"does not support fields: {incompatible_fields}"
            )
    if "enum" in rule:
        allowed = _sequence(rule["enum"], f"input_schema.properties.{name}.enum")
        if not allowed:
            raise ScenarioCompileError(f"input {name!r} has an empty enum")
        for index, item in enumerate(allowed):
            if any(_json_equal(item, previous) for previous in allowed[:index]):
                raise ScenarioCompileError(f"input {name!r} has duplicate enum values")
    if "minimum" in rule:
        minimum = rule["minimum"]
        if isinstance(minimum, bool) or not isinstance(minimum, (int, float)) or not math.isfinite(minimum):
            raise ScenarioCompileError(f"input {name!r} has an invalid minimum")
    if "maximum" in rule:
        maximum = rule["maximum"]
        if isinstance(maximum, bool) or not isinstance(maximum, (int, float)) or not math.isfinite(maximum):
            raise ScenarioCompileError(f"input {name!r} has an invalid maximum")
    if "minimum" in rule and "maximum" in rule and rule["minimum"] > rule["maximum"]:
        raise ScenarioCompileError(f"input {name!r} has a minimum greater than its maximum")
    if "default" in rule:
        _validate_input_value(name, rule["default"], rule, subject="default")
    return rule


def _validate_input_value(
    name: str,
    value: Any,
    rule: Mapping[str, Any],
    *,
    subject: str = "input",
) -> None:
    type_name = rule.get("type")
    references = collect_environment_references(value)
    if type_name == _ENVIRONMENT_REFERENCE_INPUT_TYPE:
        if not isinstance(value, EnvironmentReference):
            raise ScenarioCompileError(f"{subject} for input {name!r} must be an exact environment reference")
        if "reference_kinds" in rule:
            allowed_kinds = tuple(EnvironmentKind(item) for item in rule["reference_kinds"])
            if value.kind not in allowed_kinds:
                allowed = ", ".join(repr(kind.value) for kind in allowed_kinds)
                raise ScenarioCompileError(
                    f"{subject} for input {name!r} references kind {value.kind.value!r}; allowed kinds are {allowed}"
                )
        return
    if references:
        location = f"feature.with.{name}" if subject == "input" else f"{subject} for input {name!r}"
        raise ScenarioCompileError(
            f"{location} cannot contain environment references; "
            f"declare type {_ENVIRONMENT_REFERENCE_INPUT_TYPE!r} for an exact binding slot"
        )
    if "type" in rule:
        if not _matches_json_type(value, type_name):
            raise ScenarioCompileError(f"{subject} for input {name!r} must have type {type_name!r}")
    if "enum" in rule:
        allowed = _sequence(rule["enum"], f"input_schema.properties.{name}.enum")
        if not any(_json_equal(value, item) for item in allowed):
            raise ScenarioCompileError(f"{subject} for input {name!r} is not one of its allowed values")
    if "minimum" in rule:
        minimum = rule["minimum"]
        if not _matches_json_type(value, "number") or value < minimum:
            raise ScenarioCompileError(f"{subject} for input {name!r} is below its minimum")
    if "maximum" in rule:
        maximum = rule["maximum"]
        if not _matches_json_type(value, "number") or value > maximum:
            raise ScenarioCompileError(f"{subject} for input {name!r} is above its maximum")


def _resolve_inputs(schema_value: Mapping[str, Any], supplied_value: Mapping[str, Any]) -> Mapping[str, Any]:
    schema = _mapping(schema_value, "input_schema")
    supplied = _mapping(supplied_value, "feature.with")
    if not schema:
        _reject_typed_environment_input_references(supplied, "feature.with")
        return dict(supplied)

    unknown_schema_fields = sorted(set(schema) - _INPUT_SCHEMA_FIELDS)
    if unknown_schema_fields:
        raise ScenarioCompileError(f"input_schema contains unsupported fields: {unknown_schema_fields}")
    if schema.get("type", "object") != "object":
        raise ScenarioCompileError("input_schema.type must be 'object'")

    properties = _mapping(schema.get("properties", {}), "input_schema.properties")
    rules = {}
    for property_name, rule_value in properties.items():
        _require_input_segment(property_name, "input property name")
        rules[property_name] = _validate_input_rule(property_name, rule_value)
    required = _sequence(schema.get("required", ()), "input_schema.required")
    for required_name in required:
        _require_input_segment(required_name, "required input name")
        if required_name not in properties:
            raise ScenarioCompileError(f"required input {required_name!r} is not declared in properties")
    if len(set(required)) != len(required):
        raise ScenarioCompileError("input_schema.required contains duplicates")

    additional = schema.get("additionalProperties", False)
    if not isinstance(additional, bool):
        raise ScenarioCompileError("input_schema.additionalProperties must be a boolean")
    unknown_inputs = sorted(set(supplied) - set(properties))
    if unknown_inputs and not additional:
        raise ScenarioCompileError(f"feature.with contains unknown inputs: {unknown_inputs}")

    resolved = dict(supplied)
    for name, rule in rules.items():
        if name not in resolved and "default" in rule:
            resolved[name] = rule["default"]
        if name not in resolved and name in required:
            raise ScenarioCompileError(f"feature.with is missing required input {name!r}")
        if name in resolved:
            _validate_input_value(name, resolved[name], rule)
    for name in unknown_inputs:
        _reject_typed_environment_input_references(
            resolved[name],
            f"feature.with.{name}",
        )
    return resolved


def _prepare_feature_input_value(value: Any, location: str = "feature.with") -> Any:
    """Parse authored bindings and unwrap explicit opaque literal values."""

    if isinstance(value, EnvironmentReference):
        return value
    if isinstance(value, Mapping):
        if _LITERAL_MARKER_KEY in value:
            if set(value) != {_LITERAL_MARKER_KEY}:
                raise ScenarioCompileError(
                    f"{location} literal marker must contain only the {_LITERAL_MARKER_KEY!r} key"
                )
            return value[_LITERAL_MARKER_KEY]
        if ENVIRONMENT_REFERENCE_DATA_KEY in value:
            raise ScenarioCompileError(
                f"{location} uses the reserved {ENVIRONMENT_REFERENCE_DATA_KEY!r} run-plan serialization marker"
            )
        reference = value.get("$ref")
        if isinstance(reference, str) and reference.split(".", 1)[0] in _ENVIRONMENT_REFERENCE_NAMESPACES:
            if set(value) != {"$ref"}:
                raise ScenarioCompileError(
                    f"{location} uses an environment $ref with sibling fields; references must be exact"
                )
            try:
                return parse_environment_reference(reference)
            except InvalidEnvironmentReferenceError as exc:
                raise ScenarioCompileError(
                    f"{location} contains an invalid environment reference: {exc.reason}"
                ) from exc
        return {key: _prepare_feature_input_value(item, f"{location}.{key}") for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_prepare_feature_input_value(item, f"{location}[{index}]") for index, item in enumerate(value)]
    return value


def _reject_typed_environment_input_references(
    value: Any,
    location: str = "resolved feature inputs",
) -> None:
    if isinstance(value, EnvironmentReference):
        raise ScenarioCompileError(
            f"{location} cannot contain environment references because input constraints "
            "must be validated before acquisition"
        )
    if isinstance(value, Mapping):
        for key, item in value.items():
            _reject_typed_environment_input_references(item, f"{location}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _reject_typed_environment_input_references(item, f"{location}[{index}]")


def _lookup_input_reference(reference: Any, inputs: Mapping[str, Any], location: str) -> Any:
    if not isinstance(reference, str):
        raise UnresolvedReferenceError(f"{location}.$ref must be a string")
    match = _REF_PATTERN.fullmatch(reference)
    if match is None:
        raise UnresolvedReferenceError(f"{location}.$ref must be an exact path such as 'inputs.key_count'")
    current: Any = inputs
    for segment in match.group(1).split("."):
        if isinstance(current, Mapping):
            if segment not in current:
                raise UnresolvedReferenceError(f"{location} references missing input path {reference!r}")
            current = current[segment]
        elif isinstance(current, (list, tuple)) and segment.isdigit():
            index = int(segment)
            if index >= len(current):
                raise UnresolvedReferenceError(f"{location} references missing input path {reference!r}")
            current = current[index]
        else:
            raise UnresolvedReferenceError(f"{location} cannot traverse input path {reference!r}")
    return current


def _resolve_template(value: Any, inputs: Mapping[str, Any], location: str) -> Any:
    if isinstance(value, EnvironmentReference):
        return value
    if isinstance(value, Mapping):
        if _LITERAL_MARKER_KEY in value:
            if set(value) != {_LITERAL_MARKER_KEY}:
                raise UnresolvedReferenceError(
                    f"{location} literal marker must contain only the {_LITERAL_MARKER_KEY!r} key"
                )
            return value[_LITERAL_MARKER_KEY]
        if "$ref" in value:
            if len(value) != 1:
                raise UnresolvedReferenceError(f"{location} uses $ref with sibling fields; references must be exact")
            reference = value["$ref"]
            if isinstance(reference, str) and _REF_PATTERN.fullmatch(reference):
                return _lookup_input_reference(reference, inputs, location)
            try:
                return parse_environment_reference(reference)
            except InvalidEnvironmentReferenceError as exc:
                raise UnresolvedReferenceError(
                    f"{location}.$ref must be an exact path in inputs, artifacts, or resources: {exc.reason}"
                ) from exc
        return {key: _resolve_template(item, inputs, f"{location}.{key}") for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_resolve_template(item, inputs, f"{location}[{index}]") for index, item in enumerate(value)]
    return value


def _resolve_pack_templates(spec: FeaturePackSpec, inputs: Mapping[str, Any]) -> FeaturePackSpec:
    components = tuple(
        ComponentSpec(
            kind=component.kind,
            component_id=component.component_id,
            instance_id=component.instance_id,
            config=_resolve_template(
                component.config,
                inputs,
                f"components.{component.instance_id}.config",
            ),
            required=component.required,
            depends_on=component.depends_on,
            when_event=component.when_event,
            timeout_s=component.timeout_s,
        )
        for component in spec.components
    )
    overlays = tuple(
        StartupOverlay(
            overlay_id=overlay.overlay_id,
            selector=NodeSelector(
                roles=overlay.selector.roles,
                node_ids=overlay.selector.node_ids,
                labels=_resolve_template(
                    overlay.selector.labels,
                    inputs,
                    f"startup_overlays.{overlay.overlay_id}.selector.labels",
                ),
            ),
            server_config=_resolve_template(
                overlay.server_config,
                inputs,
                f"startup_overlays.{overlay.overlay_id}.server_config",
            ),
            artifact=overlay.artifact,
            conflict_policy=overlay.conflict_policy,
        )
        for overlay in spec.startup_overlays
    )
    return FeaturePackSpec(
        pack_id=spec.pack_id,
        version=spec.version,
        mode=spec.mode,
        supported_topologies=spec.supported_topologies,
        components=components,
        input_schema=spec.input_schema,
        startup_overlays=overlays,
        required_events=spec.required_events,
        required_evidence=spec.required_evidence,
        metadata=_resolve_template(spec.metadata, inputs, "metadata"),
    )


def _normalize_registries(
    registries: Mapping[ComponentKind, Registry[Any]],
    api_version: str,
) -> Mapping[ComponentKind, Registry[Any]]:
    if not isinstance(registries, Mapping):
        raise TypeError("registries must be a mapping")
    normalized = {}
    for raw_kind, registry in registries.items():
        try:
            kind = ComponentKind(raw_kind)
        except (TypeError, ValueError) as exc:
            raise ComponentCompatibilityError(f"invalid registry kind {raw_kind!r}") from exc
        if kind in normalized:
            raise ComponentCompatibilityError(f"duplicate registry for kind {kind.value!r}")
        if not isinstance(registry, Registry):
            raise ComponentCompatibilityError(f"registry for kind {kind.value!r} must be a Registry")
        if registry.kind != kind:
            raise ComponentCompatibilityError(
                f"registry key {kind.value!r} does not match registry kind {registry.kind.value!r}"
            )
        if registry.api_version != api_version:
            raise ComponentCompatibilityError(
                f"registry for {kind.value!r} uses API {registry.api_version!r}, expected {api_version!r}"
            )
        normalized[kind] = registry
    return MappingProxyType(normalized)


def _plan_contains_environment_references(plan: RunPlan) -> bool:
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
    return any(collect_environment_references(value) for value in values)


def _validate_components(
    components: Sequence[ComponentSpec],
    registries: Mapping[ComponentKind, Registry[Any]],
    api_version: str,
    plan: RunPlan,
) -> None:
    registrations_by_id = {}
    for kind, registry in registries.items():
        for component_id in registry.registered_ids:
            previous_kind = registrations_by_id.get(component_id)
            if previous_kind is not None and previous_kind != kind:
                raise ComponentCompatibilityError(
                    f"component {component_id!r} is registered for multiple kinds: "
                    f"{previous_kind.value!r} and {kind.value!r}"
                )
            registrations_by_id[component_id] = kind
    for component in components:
        registry = registries.get(component.kind)
        if registry is None or component.component_id not in registry:
            actual_kind = registrations_by_id.get(component.component_id)
            if actual_kind is not None:
                raise ComponentCompatibilityError(
                    f"component {component.component_id!r} is registered as "
                    f"{actual_kind.value!r}, but the pack declares {component.kind.value!r}"
                )
            raise ComponentCompatibilityError(
                f"component {component.component_id!r} is not registered for kind {component.kind.value!r}"
            )
        registration = registry.resolve(component.component_id)
        if registration.kind != component.kind:
            raise ComponentCompatibilityError(
                f"component {component.component_id!r} has wrong registered kind {registration.kind.value!r}"
            )
        if registration.api_version != api_version:
            raise ComponentCompatibilityError(
                f"component {component.component_id!r} uses API {registration.api_version!r}, expected {api_version!r}"
            )
        try:
            inspect.signature(registration.factory).bind()
        except (TypeError, ValueError) as exc:
            raise ComponentCompatibilityError(
                f"factory for component {component.component_id!r} cannot be called without arguments: {exc}"
            ) from exc
        if registration.validator is not None:
            try:
                inspect.signature(registration.validator).bind(component, plan)
            except (TypeError, ValueError) as exc:
                raise ComponentCompatibilityError(
                    f"validator for component {component.component_id!r} cannot accept (spec, plan): {exc}"
                ) from exc
            if not _plan_contains_environment_references(plan):
                try:
                    validation_return = registration.validator(component, plan)
                except Exception as exc:
                    raise ComponentCompatibilityError(
                        f"component {component.component_id!r} rejected instance {component.instance_id!r}: {exc}"
                    ) from exc
                if validation_return is not None:
                    raise ComponentCompatibilityError(
                        f"validator for component {component.component_id!r} must return None"
                    )


def _validate_resources(
    overlays: Sequence[StartupOverlay],
    inventory: ReferenceInventory,
) -> None:
    available_artifacts = set(inventory.artifacts)
    for overlay in overlays:
        if overlay.artifact is not None and overlay.artifact not in available_artifacts:
            raise UnsupportedResourceError(
                f"startup overlay {overlay.overlay_id!r} references unresolved artifact {overlay.artifact!r}"
            )
        for label_name, requested_value in overlay.selector.labels.items():
            if label_name not in inventory.labels:
                raise UnsupportedResourceError(
                    f"startup overlay {overlay.overlay_id!r} references unresolved node label {label_name!r}"
                )
            allowed_values = inventory.labels[label_name]
            if collect_environment_references(requested_value) or allowed_values is None:
                continue
            if not any(_json_equal(requested_value, allowed) for allowed in allowed_values):
                raise UnsupportedResourceError(
                    f"startup overlay {overlay.overlay_id!r} requests unsupported value for node label {label_name!r}"
                )


def _validate_environment_consumers(
    plan: RunPlan,
    artifact_registry: ArtifactProviderRegistry,
    resource_registry: ResourceProviderRegistry,
) -> None:
    artifacts = {spec.artifact_id: spec for spec in plan.artifacts}
    resources = {spec.resource_id: spec for spec in plan.resources}

    def validate(source: str, value: Any) -> None:
        for reference in collect_environment_references(value):
            if reference.kind is EnvironmentKind.ARTIFACT:
                target = artifacts.get(reference.instance_id)
                registry = artifact_registry
            else:
                target = resources.get(reference.instance_id)
                registry = resource_registry
            if target is None:
                raise UnknownEnvironmentTargetError(source, reference.dependency)
            registration = registry.resolve(target.provider_id)
            if reference.output not in registration.declared_outputs:
                raise UndeclaredEnvironmentOutputError(
                    source,
                    reference,
                    registration.provider_id,
                    registration.declared_outputs,
                )

    validate("feature.with", plan.inputs)
    validate("topology", plan.topology_document)
    for artifact in plan.artifacts:
        validate(f"artifacts.{artifact.artifact_id}.config", artifact.config)
    for resource in plan.resources:
        validate(f"resources.{resource.resource_id}.config", resource.config)
    for policy in plan.policies:
        validate(f"policies.{policy.policy_id}.config", policy.config)
    for component in plan.components:
        validate(f"components.{component.instance_id}.config", component.config)
    for overlay in plan.startup_overlays:
        validate(f"startup_overlays.{overlay.overlay_id}.selector.labels", overlay.selector.labels)
        validate(f"startup_overlays.{overlay.overlay_id}.server_config", overlay.server_config)


def validate_run_plan_environment(
    plan: RunPlan,
    *,
    artifact_registry: ArtifactProviderRegistry,
    resource_registry: ResourceProviderRegistry,
) -> None:
    """Validate every environment declaration and typed consumer in a run plan.

    This is the shared compile/replay boundary. It validates provider
    registrations, explicit and reference-derived dependencies, output names,
    the combined artifact/resource DAG, policy references, and references
    consumed by topology, components, and startup overlays.
    """

    if not isinstance(plan, RunPlan):
        raise TypeError("plan must be a RunPlan")
    with _provider_registry_transaction(artifact_registry, resource_registry):
        validate_environment_dependencies(
            plan.artifacts,
            plan.resources,
            artifact_registry=artifact_registry,
            resource_registry=resource_registry,
            policies=plan.policies,
        )
        _validate_environment_consumers(
            plan,
            artifact_registry,
            resource_registry,
        )


class FeaturePackCompiler:
    """Compile strict scenario intents after a complete read-only preflight."""

    def __init__(
        self,
        catalog: FeaturePackCatalog,
        registries: Mapping[ComponentKind, Registry[Any]],
        *,
        reference_inventory: Optional[ReferenceInventory] = None,
        artifact_registry: Optional[ArtifactProviderRegistry] = None,
        resource_registry: Optional[ResourceProviderRegistry] = None,
        extension_api_version: str = EXTENSION_API_VERSION,
    ):
        if not isinstance(catalog, FeaturePackCatalog):
            raise TypeError("catalog must be a FeaturePackCatalog")
        self.extension_api_version = _require_text(
            extension_api_version,
            "extension_api_version",
        )
        self.catalog = catalog
        self.registries = _normalize_registries(registries, self.extension_api_version)
        self.reference_inventory = reference_inventory or ReferenceInventory()
        if not isinstance(self.reference_inventory, ReferenceInventory):
            raise TypeError("reference_inventory must be a ReferenceInventory")
        self.artifact_registry = (
            ArtifactProviderRegistry(api_version=self.extension_api_version)
            if artifact_registry is None
            else artifact_registry
        )
        if not isinstance(self.artifact_registry, ArtifactProviderRegistry):
            raise TypeError("artifact_registry must be an ArtifactProviderRegistry")
        if self.artifact_registry.api_version != self.extension_api_version:
            raise TypeError("artifact_registry API version must match extension_api_version")
        self.resource_registry = (
            ResourceProviderRegistry(api_version=self.extension_api_version)
            if resource_registry is None
            else resource_registry
        )
        if not isinstance(self.resource_registry, ResourceProviderRegistry):
            raise TypeError("resource_registry must be a ResourceProviderRegistry")
        if self.resource_registry.api_version != self.extension_api_version:
            raise TypeError("resource_registry API version must match extension_api_version")

    def compile(self, value: Mapping[str, Any]) -> RunPlan:
        envelope = _fields(
            value,
            "scenario",
            allowed=("scenario_id", "seed", "feature", "topology", "artifacts", "resources", "policies"),
            required=("scenario_id", "seed", "feature", "topology"),
        )
        scenario_id = _require_name(envelope["scenario_id"], "scenario_id")
        seed = envelope["seed"]
        if isinstance(seed, bool) or not isinstance(seed, int):
            raise ScenarioCompileError("seed must be an integer")

        feature = _fields(
            envelope["feature"],
            "feature",
            allowed=("use", "version", "with"),
            required=("use",),
        )
        pack_id = _require_text(feature["use"], "feature.use")
        version = feature.get("version")
        if version is not None:
            version = _require_text(version, "feature.version")
        supplied_inputs = _mapping(
            _prepare_feature_input_value(_mapping(feature.get("with", {}), "feature.with")),
            "feature.with",
        )

        topology_document = _mapping(envelope["topology"], "topology")
        if "type" not in topology_document:
            raise FeaturePackDocumentError("topology is missing required field 'type'")

        spec = self.catalog.resolve(pack_id, version)
        resolved_inputs = _resolve_inputs(spec.input_schema, supplied_inputs)
        resolved_spec = _resolve_pack_templates(spec, resolved_inputs)
        topology_document = _mapping(
            _resolve_template(topology_document, resolved_inputs, "topology"),
            "topology",
        )
        topology = _require_name(topology_document["type"], "topology.type")
        if topology not in spec.supported_topologies:
            raise ScenarioCompileError(f"topology {topology!r} is not supported by feature pack {pack_id!r}")
        artifacts = _parse_artifacts(_resolve_template(envelope.get("artifacts", {}), resolved_inputs, "artifacts"))
        resources = _parse_resources(_resolve_template(envelope.get("resources", {}), resolved_inputs, "resources"))
        policies = _parse_policies(_resolve_template(envelope.get("policies", {}), resolved_inputs, "policies"))
        plan = RunPlan.from_feature_pack(
            resolved_spec,
            scenario_id=scenario_id,
            seed=seed,
            topology=topology,
            inputs=resolved_inputs,
            topology_document=topology_document,
            artifacts=artifacts,
            resources=resources,
            policies=policies,
        )
        _validate_components(
            resolved_spec.components,
            self.registries,
            self.extension_api_version,
            plan,
        )
        validate_run_plan_environment(
            plan,
            artifact_registry=self.artifact_registry,
            resource_registry=self.resource_registry,
        )
        _validate_resources(resolved_spec.startup_overlays, self.reference_inventory)

        return plan

    def compile_yaml(
        self,
        source: Union[str, bytes, TextIO],
        *,
        source_name: str = "<yaml>",
    ) -> RunPlan:
        """Safely load and compile one duplicate-free scenario document."""

        return self.compile(load_scenario_yaml(source, source_name=source_name))


def compile_scenario(
    value: Mapping[str, Any],
    catalog: FeaturePackCatalog,
    registries: Mapping[ComponentKind, Registry[Any]],
    *,
    reference_inventory: Optional[ReferenceInventory] = None,
    artifact_registry: Optional[ArtifactProviderRegistry] = None,
    resource_registry: Optional[ResourceProviderRegistry] = None,
) -> RunPlan:
    """Functional convenience wrapper around :class:`FeaturePackCompiler`."""

    return FeaturePackCompiler(
        catalog,
        registries,
        reference_inventory=reference_inventory,
        artifact_registry=artifact_registry,
        resource_registry=resource_registry,
    ).compile(value)


__all__ = [
    "AmbiguousFeaturePackError",
    "ComponentCompatibilityError",
    "DuplicateFeaturePackError",
    "FeaturePackCatalog",
    "FeaturePackCompiler",
    "FeaturePackDocumentError",
    "FeaturePackError",
    "IncompatibleFeaturePackError",
    "ReferenceInventory",
    "ScenarioCompileError",
    "UnknownFeaturePackError",
    "UnresolvedReferenceError",
    "UnsupportedResourceError",
    "compile_scenario",
    "load_feature_pack_directory",
    "load_feature_pack_yaml",
    "load_scenario_yaml",
    "parse_feature_pack",
    "parse_feature_pack_document",
    "validate_run_plan_environment",
]
