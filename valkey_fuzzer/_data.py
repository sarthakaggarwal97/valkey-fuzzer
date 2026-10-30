"""Shared validation, plain-data, and registry helpers for the extension model.

Every immutable model in :mod:`valkey_fuzzer.environment` and
:mod:`valkey_fuzzer.contracts` validates its fields with these helpers, so
there is one definition of an identifier, one freeze/thaw pair, and one strict
mapping parser.
"""

import hashlib
import json
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any, Callable, Generic, Optional, TypeVar

NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
SEGMENT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
QUALIFIED_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*(?:[.:/][A-Za-z0-9][A-Za-z0-9_-]*)+$")
REFERENCE_DATA_KEY = "$valkey_fuzzer_ref"


def require_text(value: Any, field_name: str, *, error: type = ValueError) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise located(error(f"{field_name} must be a non-empty string without surrounding whitespace"), field_name)
    return str(value)


def require_name(value: Any, field_name: str) -> str:
    """An opaque name: component, event, overlay, deployment mode, or scenario ID."""

    value = require_text(value, field_name)
    if not NAME_PATTERN.fullmatch(value):
        raise ValueError(f"{field_name} contains unsupported characters: {value!r}")
    return value


def require_segment(value: Any, field_name: str) -> str:
    """One path segment: letters, digits, underscores, and hyphens."""

    value = require_text(value, field_name)
    if not SEGMENT_PATTERN.fullmatch(value):
        raise ValueError(
            f"{field_name} must start with a letter or digit and contain only letters, digits, "
            f"underscores, and hyphens: {value!r}"
        )
    return value


def require_qualified_id(value: Any, field_name: str) -> str:
    """A namespaced identifier such as ``example.component``."""

    value = require_text(value, field_name)
    if not QUALIFIED_ID_PATTERN.fullmatch(value):
        raise ValueError(f"{field_name} must be a namespaced identifier such as 'example.component': {value!r}")
    return value


def require_number(
    value: Any,
    field_name: str,
    *,
    positive: bool = False,
    integer: bool = False,
) -> Any:
    kinds = (int,) if integer else (int, float)
    if isinstance(value, bool) or not isinstance(value, kinds) or not math.isfinite(value):
        raise ValueError(f"{field_name} must be a finite {'integer' if integer else 'number'}")
    if positive and value <= 0:
        raise ValueError(f"{field_name} must be greater than zero")
    return value


def as_tuple(value: Any, field_name: str, *, error: type = TypeError) -> tuple[Any, ...]:
    if isinstance(value, (str, bytes, bytearray, Mapping)) or not isinstance(value, Sequence):
        raise located(error(f"{field_name} must be an ordered sequence"), field_name)
    return tuple(value)


def unique_tuple(
    value: Any,
    field_name: str,
    check: Callable[[Any, str], Any] = require_name,
) -> tuple[Any, ...]:
    entries = as_tuple(value, field_name)
    for entry in entries:
        check(entry, f"{field_name} entry")
    if len(set(entries)) != len(entries):
        raise ValueError(f"{field_name} contains duplicates")
    return entries


def require_type(value: Any, expected: type, field_name: str) -> Any:
    if not isinstance(value, expected):
        raise TypeError(f"{field_name} must be a {expected.__name__}")
    return value


def strict_fields(
    value: Any,
    field_name: str,
    *,
    allowed: Iterable[str],
    required: Iterable[str] = (),
    error: type = ValueError,
) -> Mapping[str, Any]:
    """Return ``value`` after rejecting non-mappings and unknown or missing keys."""

    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise located(error(f"{field_name} must be a mapping with string keys"), field_name)
    unknown = sorted(set(value) - set(allowed))
    if unknown:
        raise located(error(f"{field_name} contains unknown fields: {unknown}"), f"{field_name}.{unknown[0]}")
    missing = sorted(set(required) - set(value))
    if missing:
        raise located(error(f"{field_name} is missing required fields: {missing}"), f"{field_name}.{missing[0]}")
    return value


def located(error: Exception, path: str) -> Exception:
    """``error`` tagged with the document ``path`` it is about, as its ``path`` attribute.

    A path set closer to the cause is kept, so an error re-raised through
    several layers names the most precise location. ``valkey-fuzzer validate``
    reports it next to the message.
    """

    if getattr(error, "path", None) is None:
        error.path = path
    return error


def carry_path(error: Exception, cause: BaseException) -> Exception:
    """``error`` with the document path of ``cause``, for an error that wraps another."""

    path = getattr(cause, "path", None)
    return error if path is None else located(error, path)


class DependencyCycleError(ValueError):
    """Declarations depend on each other in a circle; ``cycle`` is its path, its first node repeated last."""

    def __init__(self, what: str, cycle: Sequence[Any]):
        self.cycle = tuple(cycle)
        super().__init__(f"{what} dependency cycle: {' -> '.join(str(node) for node in self.cycle)}")


N = TypeVar("N")


def stable_order(nodes: Sequence[N], edges: Mapping[N, Sequence[N]], what: str) -> tuple[N, ...]:
    """``nodes`` with each after the nodes ``edges`` says it depends on, otherwise in their given order.

    The next node is always the first whose dependencies are placed, so an
    author's order survives wherever the dependencies allow it. Every
    dependency must be one of ``nodes``.
    """

    placed: set[N] = set()
    ordered: list[N] = []
    pending = list(nodes)
    while pending:
        ready = next((node for node in pending if all(item in placed for item in edges.get(node, ()))), None)
        if ready is None:
            # Every pending node waits on another pending one: follow them until one repeats.
            path = [pending[0]]
            while True:
                after = next(item for item in edges.get(path[-1], ()) if item not in placed)
                if after in path:
                    raise DependencyCycleError(what, (*path[path.index(after) :], after))
                path.append(after)
        pending.remove(ready)
        placed.add(ready)
        ordered.append(ready)
    return tuple(ordered)


# ---------------------------------------------------------------------------
# Typed environment references
# ---------------------------------------------------------------------------


class EnvironmentKind(str, Enum):
    """Addressable namespaces in the environment acquired before the deployment."""

    ARTIFACT = "artifacts"
    RESOURCE = "resources"


class EnvironmentReferenceError(ValueError):
    """An environment reference is malformed, unknown, undeclared, or unresolved."""


@dataclass(frozen=True)
class EnvironmentDependency:
    """One artifact or resource declaration: ``artifacts.<id>`` or ``resources.<id>``."""

    kind: EnvironmentKind
    id: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", EnvironmentKind(self.kind))
        require_segment(self.id, "environment dependency id")

    @property
    def path(self) -> str:
        return f"{self.kind.value}.{self.id}"

    def __str__(self) -> str:
        return self.path


@dataclass(frozen=True)
class EnvironmentReference:
    """An exact reference to one declared provider output."""

    kind: EnvironmentKind
    id: str
    output: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", EnvironmentKind(self.kind))
        require_segment(self.id, "environment reference id")
        require_segment(self.output, "environment reference output")

    @property
    def dependency(self) -> EnvironmentDependency:
        return EnvironmentDependency(self.kind, self.id)

    @property
    def path(self) -> str:
        return f"{self.kind.value}.{self.id}.{self.output}"

    def __str__(self) -> str:
        return self.path


def collect_environment_references(value: Any) -> tuple[EnvironmentReference, ...]:
    """Distinct references in ``value``, in first-observed order."""

    found: dict[EnvironmentReference, None] = {}

    def walk(item: Any) -> None:
        if isinstance(item, EnvironmentReference):
            found.setdefault(item)
        elif isinstance(item, Mapping):
            for nested in item.values():
                walk(nested)
        elif isinstance(item, (list, tuple)):
            for nested in item:
                walk(nested)

    walk(value)
    return tuple(found)


def _parse_path(value: Any, parts: int, expected: str) -> list[str]:
    if not isinstance(value, str) or value != value.strip():
        raise EnvironmentReferenceError(f"invalid environment reference {value!r}: expected {expected}")
    segments = value.split(".")
    if len(segments) != parts or segments[0] not in {kind.value for kind in EnvironmentKind}:
        raise EnvironmentReferenceError(f"invalid environment reference {value!r}: expected {expected}")
    return segments


def parse_environment_dependency(value: Any) -> EnvironmentDependency:
    """Parse exactly ``artifacts.<id>`` or ``resources.<id>``."""

    kind, declaration_id = _parse_path(value, 2, "'artifacts.<id>' or 'resources.<id>'")
    try:
        return EnvironmentDependency(EnvironmentKind(kind), declaration_id)
    except ValueError as exc:
        raise EnvironmentReferenceError(f"invalid environment reference {value!r}: {exc}") from exc


def parse_environment_reference(value: Any) -> EnvironmentReference:
    """Parse exactly ``artifacts.<id>.<output>`` or ``resources.<id>.<output>``."""

    kind, declaration_id, output = _parse_path(value, 3, "'artifacts.<id>.<output>' or 'resources.<id>.<output>'")
    try:
        return EnvironmentReference(EnvironmentKind(kind), declaration_id, output)
    except ValueError as exc:
        raise EnvironmentReferenceError(f"invalid environment reference {value!r}: {exc}") from exc


# ---------------------------------------------------------------------------
# Freezing and plain data
# ---------------------------------------------------------------------------


def freeze(value: Any, field_name: str = "value", *, references: bool = False) -> Any:
    """Take a recursive immutable snapshot of JSON/YAML-safe data.

    Mappings become read-only mappings and sequences become tuples. Typed
    environment references are kept only when ``references`` is true.
    """

    if isinstance(value, EnvironmentReference):
        if references:
            return value
        raise TypeError(f"{field_name} must not contain environment references")
    if isinstance(value, Mapping):
        if REFERENCE_DATA_KEY in value:
            raise EnvironmentReferenceError(
                f"{field_name} contains the reserved {REFERENCE_DATA_KEY!r} serialization marker"
            )
        frozen = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{field_name} mapping keys must be strings")
            frozen[key] = freeze(item, f"{field_name}.{key}", references=references)
        return MappingProxyType(frozen)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(freeze(item, f"{field_name}[{index}]", references=references) for index, item in enumerate(value))
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{field_name} cannot contain NaN or infinite numbers")
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise TypeError(f"{field_name} must contain only JSON/YAML-safe values; got {type(value).__name__}")


def freeze_mapping(value: Any, field_name: str, *, references: bool = False) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must be a mapping")
    return freeze(value, field_name, references=references)


def to_plain_data(value: Any) -> Any:
    """Convert models and frozen snapshots to JSON/YAML-safe dicts and lists.

    Dataclass fields whose metadata sets ``serialize=False`` (runtime-only
    handles and services) are omitted. References serialize as the reserved
    ``{"$valkey_fuzzer_ref": path}`` marker.
    """

    if isinstance(value, EnvironmentReference):
        return {REFERENCE_DATA_KEY: value.path}
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value) and not isinstance(value, type):
        return {
            item.name: to_plain_data(getattr(value, item.name))
            for item in fields(value)
            if item.metadata.get("serialize", True)
        }
    if isinstance(value, Mapping):
        return {key: to_plain_data(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_plain_data(item) for item in value]
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise TypeError(f"cannot convert {type(value).__name__} to plain data")


def references_from_data(value: Any) -> Any:
    """Decode serialized ``{"$valkey_fuzzer_ref": path}`` markers into references."""

    if isinstance(value, Mapping):
        if REFERENCE_DATA_KEY in value:
            if set(value) != {REFERENCE_DATA_KEY}:
                raise EnvironmentReferenceError(
                    f"a serialized reference marker must contain only the {REFERENCE_DATA_KEY!r} key"
                )
            return parse_environment_reference(value[REFERENCE_DATA_KEY])
        return {key: references_from_data(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [references_from_data(item) for item in value]
    return value


def jsonable(value: Any) -> Any:
    """Arbitrary reply data as JSON-safe plain data (bytes decoded, sets sorted)."""

    if isinstance(value, Mapping):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        items = [jsonable(item) for item in value]
        if isinstance(value, (set, frozenset)):
            items.sort(key=lambda item: json.dumps(item, sort_keys=True, default=str))
        return items
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if value is None or isinstance(value, (str, bool, int)):
        return value
    return str(value)


# ---------------------------------------------------------------------------
# Scenario documents
# ---------------------------------------------------------------------------

# Scenario sections a batch matrix dimension or differential variant may patch.
SCENARIO_SECTIONS = ("experiment", "deployment", "artifacts", "resources", "policies")


def _copied(value: Any) -> Any:
    """``value`` with each mapping and list in it new, so a merge result shares none with its inputs."""

    if isinstance(value, Mapping):
        return {key: _copied(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_copied(item) for item in value]
    return value


def merge(base: Mapping[str, Any], overlay: Mapping[str, Any]) -> dict[str, Any]:
    """A copy of ``base`` with ``overlay`` merged in: mappings merge key by key, anything else is replaced.

    A ``{$ref: ...}`` mapping on either side is one value, replaced whole:
    fields merged into it would make a reference with sibling fields, which
    the compiler rejects.
    """

    merged = _copied(base)
    for key, value in overlay.items():
        current = merged.get(key)
        if (
            isinstance(value, Mapping)
            and isinstance(current, Mapping)
            and "$ref" not in value
            and "$ref" not in current
        ):
            merged[key] = merge(current, value)
        else:
            merged[key] = _copied(value)
    return merged


def with_valkey_binary(scenario: Mapping[str, Any], path: Optional[str], *, override: bool = True) -> dict[str, Any]:
    """A copy of ``scenario`` whose deployment runs the Valkey binary ``path``, when there is one.

    With ``override`` (``--valkey-binary``) it replaces the scenario's own
    binary; without it (a batch's binary for scenarios that name none) a
    scenario that names its own, such as a zoo run on its build, keeps it.
    """

    deployment = scenario.get("deployment")
    if not path or (not override and isinstance(deployment, Mapping) and "valkey_binary" in deployment):
        return _copied(scenario)
    return merge(scenario, {"deployment": {"valkey_binary": path}})


def set_path(document: dict, dotted: str, value: Any) -> None:
    """Set the dotted path in ``document``, creating mappings on the way; ``None`` removes the key."""

    parts = dotted.split(".")
    current = document
    for part in parts[:-1]:
        nxt = current.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            current[part] = nxt
        current = nxt
    if value is None and parts[-1] in current:
        del current[parts[-1]]
    elif value is not None:
        current[parts[-1]] = value


def canonical_json(value: Any) -> str:
    """Serialize data in a stable, replay-friendly JSON form."""

    return json.dumps(to_plain_data(value), allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def exception_type(error: BaseException) -> str:
    """``error``'s class as reports name it: ``ValueError`` for a builtin, else ``module.Class``."""

    error_type = type(error)
    module = error_type.__module__
    return error_type.__qualname__ if module == "builtins" else f"{module}.{error_type.__qualname__}"


# How much of an error's message a report keeps.
_MAX_ERROR_MESSAGE = 2000


def error_record(error: BaseException, phase: Optional[str] = None) -> dict[str, str]:
    """How a report records ``error``: its ``type``, its ``message`` (cut short), and the ``phase`` it ended.

    Every layer that catches a run's error records it this way, so the same
    error has one failure signature whichever layer caught it.
    """

    record = {"type": exception_type(error)}
    message = str(error)
    if message:
        record["message"] = message[:_MAX_ERROR_MESSAGE] + ("..." if len(message) > _MAX_ERROR_MESSAGE else "")
    if phase is not None:
        record["phase"] = phase
    return record


def file_sha256(path: str) -> Optional[str]:
    """The SHA-256 of a file's contents, or ``None`` when it cannot be read."""

    digest = hashlib.sha256()
    try:
        with open(path, "rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError:
        return None
    return digest.hexdigest()


# ---------------------------------------------------------------------------
# Registries
# ---------------------------------------------------------------------------


class RegistrationError(ValueError):
    """A registration is duplicated, unknown, or incompatible with its registry."""


class DuplicateRegistrationError(RegistrationError):
    def __init__(self, label: str, key: Any):
        self.key = key
        super().__init__(f"{label} {key!r} is already registered")


class UnknownRegistrationError(RegistrationError, KeyError):
    def __init__(self, label: str, key: Any):
        self.key = key
        RegistrationError.__init__(self, f"{label} {key!r} is not registered")

    def __str__(self) -> str:
        return RegistrationError.__str__(self)


class FrozenRegistryError(RegistrationError):
    """A registry was changed after plugin discovery completed."""


T = TypeVar("T")


class BaseRegistry(Generic[T]):
    """Keyed registry that is filled during startup and then frozen.

    Plugin discovery registers everything up front; after :meth:`freeze`
    the registry is read-only, so execution never observes it changing.
    """

    label = "entry"
    entry_type: type = object

    def __init__(self) -> None:
        self._entries: dict[Any, T] = {}
        self._frozen = False

    @property
    def frozen(self) -> bool:
        return self._frozen

    def key(self, entry: T) -> Any:
        raise NotImplementedError

    def check(self, entry: T) -> None:
        """Reject an entry that cannot belong to this registry."""

        if not isinstance(entry, self.entry_type):
            raise TypeError(f"{self.label} registry entries must be {self.entry_type.__name__} values")

    def validate_many(self, entries: Iterable[T]) -> tuple[T, ...]:
        """Check that ``entries`` could be registered now, without registering them."""

        if self._frozen:
            raise FrozenRegistryError(f"the {self.label} registry is frozen after plugin discovery")
        entries = tuple(entries)
        seen = set()
        for entry in entries:
            self.check(entry)
            key = self.key(entry)
            if key in seen or key in self._entries:
                raise DuplicateRegistrationError(self.label, key)
            seen.add(key)
        return entries

    def register_many(self, entries: Iterable[T]) -> None:
        for entry in self.validate_many(entries):
            self._entries[self.key(entry)] = entry

    def register(self, entry: T) -> None:
        self.register_many((entry,))

    def freeze(self) -> None:
        self._frozen = True

    def resolve(self, key: Any) -> T:
        try:
            return self._entries[key]
        except (KeyError, TypeError) as exc:
            raise UnknownRegistrationError(self.label, key) from exc

    def get(self, key: Any) -> Optional[T]:
        return self._entries.get(key)

    @property
    def registered_ids(self) -> tuple[Any, ...]:
        return tuple(sorted(self._entries))

    def snapshot(self) -> Mapping[Any, T]:
        return MappingProxyType({key: self._entries[key] for key in sorted(self._entries)})

    def __contains__(self, key: object) -> bool:
        return key in self._entries

    def __len__(self) -> int:
        return len(self._entries)
