"""Shared helpers for the Valkey component library.

Components only talk to the generic run context from
:mod:`valkey_fuzzer.extensions`.  Topology access goes through the
``valkey.topology`` service published by the local topology provider; nothing
here imports framework internals.
"""

import hashlib
import json
import math
import os
import re
import tempfile
import threading
import time
import uuid
import weakref
from collections.abc import Iterable, Mapping, Sequence
from enum import Enum
from typing import Any, Callable, Optional

from valkey_fuzzer.extensions import ComponentResult, ComponentStatus, EnvironmentReference, Evidence

TOPOLOGY_SERVICE = "valkey.topology"

# Validators and lifecycle methods receive the framework's executable spec and
# plan views; components only rely on their documented attributes.
ComponentSpec = Any
RunPlan = Any


def to_plain_data(value: Any) -> Any:
    """Convert frozen spec/config/evidence values to plain JSON-like data."""

    if isinstance(value, EnvironmentReference):
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {key: to_plain_data(item) for key, item in value.items()}
    # Bound plans expose read-only sequence views, not lists or tuples.
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [to_plain_data(item) for item in value]
    return value


NODE_SELECTORS = ("all", "primaries", "replicas", "primary", "replica")


class ComponentConfigError(ValueError):
    """A component instance has an invalid configuration."""


_MISSING = object()


class Field:
    """Tiny declarative config field used by component validators."""

    def __init__(
        self,
        types: Any,
        *,
        default: Any = _MISSING,
        choices: Optional[Sequence[Any]] = None,
        minimum: Optional[float] = None,
        maximum: Optional[float] = None,
        allow_none: bool = False,
        reference: bool = False,
    ):
        self.types = types if isinstance(types, tuple) else (types,)
        self.default = default
        self.choices = tuple(choices) if choices is not None else None
        self.minimum = minimum
        self.maximum = maximum
        self.allow_none = allow_none
        # True when the value may be an artifacts/resources reference that is
        # bound before execution; compile-time validation then skips it.
        self.reference = reference

    @property
    def required(self) -> bool:
        return self.default is _MISSING

    def check(self, name: str, value: Any) -> None:
        if value is None and self.allow_none:
            return
        if isinstance(value, EnvironmentReference):
            if self.reference:
                return
            raise ComponentConfigError(f"config field {name!r} cannot be an environment reference")
        if bool not in self.types and isinstance(value, bool) and (int in self.types or float in self.types):
            raise ComponentConfigError(f"config field {name!r} must not be a boolean")
        accepted = self.types
        if float in accepted and int not in accepted:
            accepted = (*accepted, int)
        if list in accepted:
            accepted = (*accepted, tuple)
        if dict in accepted:
            accepted = (*accepted, Mapping)
        if not isinstance(value, accepted):
            names = "/".join(getattr(kind, "__name__", str(kind)) for kind in self.types)
            raise ComponentConfigError(f"config field {name!r} must be {names}, got {type(value).__name__}")
        if self.choices is not None and value not in self.choices:
            raise ComponentConfigError(f"config field {name!r} must be one of {list(self.choices)}, got {value!r}")
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if isinstance(value, float) and not math.isfinite(value):
                raise ComponentConfigError(f"config field {name!r} must be finite")
            if self.minimum is not None and value < self.minimum:
                raise ComponentConfigError(f"config field {name!r} must be >= {self.minimum}")
            if self.maximum is not None and value > self.maximum:
                raise ComponentConfigError(f"config field {name!r} must be <= {self.maximum}")


def config_validator(
    fields: Mapping[str, Field],
    *,
    extra: Optional[Callable[[Mapping[str, Any], ComponentSpec, RunPlan], None]] = None,
    topologies: Optional[Iterable[str]] = None,
    requires_event: bool = False,
) -> Callable[[ComponentSpec, RunPlan], None]:
    """Build a pure ``(spec, plan) -> None`` validator from field rules."""

    allowed_topologies = None if topologies is None else frozenset(topologies)
    if requires_event:
        # Event-triggered components name their trigger in their own config;
        # the framework has no scheduling language.
        fields = {"when_event": Field(str), **fields}

    def validate(spec: ComponentSpec, plan: RunPlan) -> None:
        config = to_plain_data(spec.config)
        unknown = sorted(set(config) - set(fields))
        if unknown:
            raise ComponentConfigError(f"unknown config fields: {unknown}")
        for name, rule in fields.items():
            if name in config:
                rule.check(name, config[name])
            elif rule.required:
                raise ComponentConfigError(f"missing required config field {name!r}")
        if allowed_topologies is not None and plan.topology not in allowed_topologies:
            raise ComponentConfigError(
                f"{spec.component_id} supports topologies {sorted(allowed_topologies)}, not {plan.topology!r}"
            )
        if extra is not None:
            # Reference-typed fields may still be unbound references here.
            extra(resolved_config(spec, fields), spec, plan)

    return validate


def server_arguments(config: Mapping[str, Any]) -> list[str]:
    """Render ``{option: value}`` as valkey-server command-line arguments."""

    arguments: list[str] = []
    for option, raw in config.items():
        for value in raw if isinstance(raw, (list, tuple)) else (raw,):
            if isinstance(value, bool):
                value = "yes" if value else "no"
            arguments.extend((f"--{option}", str(value)))
    return arguments


def resolved_config(spec: ComponentSpec, fields: Mapping[str, Field]) -> dict[str, Any]:
    """Return plain config with declared defaults applied."""

    config = dict(to_plain_data(spec.config))
    for name, rule in fields.items():
        if name not in config and not rule.required:
            default = rule.default
            config[name] = default() if callable(default) else default
    return config


def topology(context: Any) -> Any:
    try:
        return context.services["topology"][TOPOLOGY_SERVICE]
    except KeyError as exc:
        raise RuntimeError(f"run context does not provide the {TOPOLOGY_SERVICE!r} topology service") from exc


def node_ids(handle: Any) -> list[str]:
    return [node.node_id for node in handle.nodes]


def validate_node_selector(value: Any, name: str = "nodes") -> None:
    if isinstance(value, str):
        if value in NODE_SELECTORS or re.fullmatch(r"node-\d+", value):
            return
        raise ComponentConfigError(f"config field {name!r} must be one of {list(NODE_SELECTORS)} or a node ID")
    if isinstance(value, (list, tuple)) and value and all(isinstance(item, str) for item in value):
        for item in value:
            if not re.fullmatch(r"node-\d+", item):
                raise ComponentConfigError(f"config field {name!r} contains invalid node ID {item!r}")
        return
    raise ComponentConfigError(f"config field {name!r} must be a selector string or a non-empty list of node IDs")


def select_nodes(handle: Any, selector: Any) -> list[str]:
    """Resolve a node selector against the live topology, in planned order."""

    if isinstance(selector, (list, tuple)):
        known = set(node_ids(handle))
        unknown = [item for item in selector if item not in known]
        if unknown:
            raise ValueError(f"unknown node IDs {unknown}")
        return list(selector)
    if selector == "all":
        return node_ids(handle)
    if selector in ("primaries", "primary"):
        selected = handle.primaries()
        return selected[:1] if selector == "primary" else selected
    if selector in ("replicas", "replica"):
        selected = handle.replicas()
        return selected[:1] if selector == "replica" else selected
    if selector in set(node_ids(handle)):
        return [selector]
    raise ValueError(f"unknown node selector {selector!r}")


def now() -> float:
    return time.time()


def make_evidence(
    spec: ComponentSpec,
    evidence_id: str,
    phase: str,
    value: Any,
    *,
    event_sequence: Optional[int] = None,
    artifacts: Sequence[str] = (),
    metadata: Optional[Mapping[str, Any]] = None,
) -> Evidence:
    return Evidence(
        evidence_id=evidence_id,
        source=spec.instance_id,
        phase=phase,
        observed_at=time.time(),
        value=jsonable(value),
        event_sequence=event_sequence,
        artifacts=tuple(artifacts),
        metadata=jsonable(metadata or {}),
    )


def make_result(
    spec: ComponentSpec,
    status: ComponentStatus,
    summary: str,
    *,
    evidence: Sequence[Evidence] = (),
    diagnostics: Optional[Mapping[str, Any]] = None,
) -> ComponentResult:
    return ComponentResult(
        component_instance_id=spec.instance_id,
        status=status,
        summary=summary,
        evidence=tuple(evidence),
        diagnostics=jsonable(diagnostics or {}),
    )


def jsonable(value: Any) -> Any:
    """Convert arbitrary reply data to JSON-safe plain data."""

    if isinstance(value, Mapping):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        items = [jsonable(item) for item in value]
        if isinstance(value, (set, frozenset)):
            items = sorted(items, key=lambda item: json.dumps(item, sort_keys=True, default=str))
        return items
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if value is None or isinstance(value, (str, bool, int)):
        return value
    return str(value)


_RUN_DIRECTORIES: "weakref.WeakKeyDictionary[Any, str]" = weakref.WeakKeyDictionary()
_RUN_DIRECTORIES_LOCK = threading.Lock()


def run_artifact_dir(context: Any) -> str:
    """Return this run's directory under the shared artifact root.

    The root is shared by every run of a runtime, so each run (identified by
    its event bus) gets its own ``<scenario>-seed<N>-<time>-<id>`` directory.
    """

    with _RUN_DIRECTORIES_LOCK:
        directory = _RUN_DIRECTORIES.get(context.event_bus)
        if directory is None:
            plan = context.plan
            stamp = time.strftime("%Y%m%d-%H%M%S")
            name = f"{plan.scenario_id}-seed{plan.seed}-{stamp}-{uuid.uuid4().hex[:8]}"
            directory = os.path.join(context.artifact_dir or tempfile.gettempdir(), "runs", name)
            _RUN_DIRECTORIES[context.event_bus] = directory
    os.makedirs(directory, exist_ok=True)
    return directory


def component_artifact_dir(context: Any, spec: ComponentSpec) -> str:
    """Return (and create) this run's directory for one component instance."""

    path = os.path.join(run_artifact_dir(context), "components", spec.instance_id)
    os.makedirs(path, exist_ok=True)
    return path


def value_digest(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return hashlib.sha256(value.encode("utf-8", errors="surrogatepass")).hexdigest()[:16]


def remaining(context: Any, fallback: float) -> float:
    """Seconds left before the cooperative deadline, bounded by ``fallback``."""

    deadline = context.deadline
    if deadline is None:
        return fallback
    return max(0.0, min(fallback, deadline - time.monotonic()))


def info_value(raw: Any) -> Any:
    """Normalize INFO values so comparisons are predictable."""

    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    return raw


def compare(actual: Any, rule: Mapping[str, Any]) -> bool:
    """Evaluate one scalar predicate against an observed value."""

    if "exists" in rule:
        return (actual is not None) is bool(rule["exists"])
    if actual is None:
        return False
    if "equals" in rule:
        return str(actual) == str(rule["equals"])
    if "not_equals" in rule:
        return str(actual) != str(rule["not_equals"])
    if "in" in rule:
        return str(actual) in {str(item) for item in rule["in"]}
    if "contains" in rule:
        return str(rule["contains"]) in str(actual)
    numeric = _as_number(actual)
    if numeric is None:
        return False
    if "min" in rule and numeric < float(rule["min"]):
        return False
    if "max" in rule and numeric > float(rule["max"]):
        return False
    return "min" in rule or "max" in rule


PREDICATE_KEYS = ("exists", "equals", "not_equals", "in", "contains", "min", "max")


def validate_predicate(rule: Mapping[str, Any], where: str) -> None:
    present = [key for key in PREDICATE_KEYS if key in rule]
    if not present:
        raise ComponentConfigError(f"{where} needs one predicate of {list(PREDICATE_KEYS)}")
    if len([key for key in present if key not in ("min", "max")]) > 1:
        raise ComponentConfigError(f"{where} may combine only min and max")


def _as_number(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


class StoppableThread:
    """A daemon worker that is always joined by its owner."""

    def __init__(self, name: str, target: Callable[[threading.Event], None]):
        self.stop_event = threading.Event()
        self.error: Optional[BaseException] = None

        def run() -> None:
            try:
                target(self.stop_event)
            except BaseException as exc:  # pragma: no cover - surfaced by join()
                self.error = exc

        self._thread = threading.Thread(target=run, name=name, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def stop(self, timeout: float = 30.0) -> bool:
        self.stop_event.set()
        self._thread.join(timeout)
        return not self._thread.is_alive()


def read_jsonl(path: str) -> list[dict[str, Any]]:
    entries = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                entries.append(json.loads(line))
    return entries


def write_jsonl(path: str, entries: Iterable[Mapping[str, Any]]) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        for entry in entries:
            handle.write(json.dumps(jsonable(entry), sort_keys=True))
            handle.write("\n")
    os.replace(tmp, path)


def canonical_value(client: Any, key: str) -> Optional[list]:
    """Return ``[type, content]`` with order-insensitive types sorted, or None."""

    kind = client.type(key)
    if isinstance(kind, bytes):
        kind = kind.decode()
    if kind == "none":
        return None
    if kind == "string":
        content: Any = client.get(key)
    elif kind == "hash":
        content = sorted(client.hgetall(key).items())
    elif kind == "list":
        content = client.lrange(key, 0, -1)
    elif kind == "set":
        content = sorted(client.smembers(key))
    elif kind == "zset":
        content = [[member, repr(score)] for member, score in client.zrange(key, 0, -1, withscores=True)]
    elif kind == "stream":
        content = [[entry_id, sorted(fields.items())] for entry_id, fields in client.xrange(key)]
    else:
        content = f"<{kind}>"
    return [kind, jsonable(content)]


def keyspace_snapshot(client: Any, match: Optional[str] = None, count: int = 1000) -> dict[str, list]:
    """Canonical content of every key on one node (optionally a SCAN pattern)."""

    snapshot: dict[str, list] = {}
    for key in client.scan_iter(match=match, count=count):
        if isinstance(key, bytes):
            key = key.decode("utf-8", errors="surrogateescape")
        value = canonical_value(client, key)
        if value is not None:
            snapshot[key] = value
    return snapshot


def keyspace_digest(snapshot: Mapping[str, Any]) -> str:
    material = json.dumps(sorted(snapshot.items()), separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(material.encode("utf-8", errors="surrogatepass")).hexdigest()


def evidence_of(
    evidence: Sequence[Evidence],
    evidence_id: str,
    source: Optional[str] = None,
) -> list[Evidence]:
    return [item for item in evidence if item.evidence_id == evidence_id and (source is None or item.source == source)]
