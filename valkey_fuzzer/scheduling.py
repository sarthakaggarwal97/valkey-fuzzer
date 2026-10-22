"""Feature-neutral activation scheduling with exact deterministic replay.

The scheduler is deliberately independent from the extension model and runner.
It owns only activation declarations, committed-event matching, scheduler state
transitions, and a replayable operation trace. It never invokes component code.

Only events carrying an explicit committed identity satisfy
:class:`CommittedEvent`. Admission also does not imply invocation: cleanup
ownership begins only after an explicit ``invocation_started`` transition.

Event matching is incremental. Each selector has an occurrence stream and
waiters are bucketed by target occurrence. Due activations and trigger
deadlines live in heaps, so unrelated events do not scan declarations or
history.
"""

import heapq
import json
import math
import re
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Optional, Protocol, Union, runtime_checkable

from .run_state import canonical_digest, canonical_json, canonical_snapshot

SCHEDULE_JOURNAL_SCHEMA_ID = "valkey-fuzzer.schedule-journal"
SCHEDULE_JOURNAL_SCHEMA_VERSION = 1

_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_DIGEST_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")


class SchedulingError(ValueError):
    """Base class for invalid scheduling state or replay data."""


class DuplicateActivationError(SchedulingError):
    """A component has more than one activation declaration."""


class UnknownActivationError(KeyError, SchedulingError):
    """A scheduler operation references an unknown component instance."""


class InvalidScheduleTransitionError(SchedulingError):
    """A component cannot make the requested scheduler transition."""


class ScheduleReplayError(SchedulingError):
    """A persisted scheduler trace cannot be reproduced."""


class ActivationMode(str, Enum):
    """How a runner should invoke an admitted component."""

    SERIAL = "serial"
    CONCURRENT = "concurrent"


class TriggerKind(str, Enum):
    """Boundary that makes a ready component eligible for admission."""

    READY = "ready"
    EVENT = "event"


class EventScope(str, Enum):
    """Which committed occurrences count for an event trigger."""

    AFTER_READY = "after_ready"
    RUN = "run"


class ScheduleTransition(str, Enum):
    """Persisted component transitions owned by the scheduler."""

    READY = "ready"
    ADMITTED = "admitted"
    INVOCATION_STARTED = "invocation_started"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    ADMISSION_ABANDONED = "admission_abandoned"


class ScheduleOperationKind(str, Enum):
    """Successful planner calls represented in the exact operation trace."""

    OBSERVE = "observe"
    READY = "ready"
    ADVANCE = "advance"
    CANCEL = "cancel"
    START = "start"
    ABANDON = "abandon"


@runtime_checkable
class CommittedEvent(Protocol):
    """Structural event contract accepted by the scheduler.

    ``event_sequence`` identifies the original lifecycle event. It is retained
    as provenance and is not a scheduler ordering key. ``commit_index`` is the
    contiguous order in which successful events became visible.
    """

    @property
    def name(self) -> str: ...

    @property
    def source(self) -> str: ...

    @property
    def payload(self) -> Mapping[str, Any]: ...

    @property
    def event_sequence(self) -> int: ...

    @property
    def commit_index(self) -> int: ...

    @property
    def occurred_at(self) -> float: ...

    @property
    def committed_at(self) -> float: ...


def _exact_str(value: str) -> str:
    """Detach a string subclass without invoking its Python overrides."""

    return str.__str__(value)


def _require_name(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string without surrounding whitespace")
    value = _exact_str(value)
    if not value or value != value.strip():
        raise ValueError(f"{field_name} must be a non-empty string without surrounding whitespace")
    if not _NAME_PATTERN.fullmatch(value):
        raise ValueError(f"{field_name} contains unsupported characters: {value!r}")
    return value


def _require_optional_text(value: Any, field_name: str) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string without surrounding whitespace")
    value = _exact_str(value)
    if not value or value != value.strip():
        raise ValueError(f"{field_name} must be a non-empty string without surrounding whitespace")
    return value


def _require_positive_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be a positive integer")
    result = int.__int__(value)
    if result < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return result


def _require_nonnegative_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be a non-negative integer")
    result = int.__int__(value)
    if result < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return result


def _require_timestamp(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a finite number")
    try:
        result = float(int.__int__(value)) if isinstance(value, int) else float.__float__(value)
    except OverflowError as exc:
        raise ValueError(f"{field_name} must be a finite number") from exc
    if not math.isfinite(result):
        raise ValueError(f"{field_name} must be a finite number")
    return result


def _require_nonnegative_duration(value: Any, field_name: str) -> float:
    result = _require_timestamp(value, field_name)
    if result < 0:
        raise ValueError(f"{field_name} must be greater than or equal to zero")
    return result


def _require_positive_duration(value: Any, field_name: str) -> float:
    result = _require_timestamp(value, field_name)
    if result <= 0:
        raise ValueError(f"{field_name} must be greater than zero")
    return result


def _checked_time_add(left: float, right: float, field_name: str) -> float:
    result = left + right
    if not math.isfinite(result):
        raise InvalidScheduleTransitionError(f"{field_name} exceeds the finite scheduler time range")
    return result


def _ordered(value: Any, field_name: str) -> tuple[Any, ...]:
    if isinstance(value, (str, bytes, bytearray, Mapping)) or not isinstance(value, Sequence):
        raise TypeError(f"{field_name} must be an ordered sequence")
    return tuple(value)


def _fields(
    value: Any,
    field_name: str,
    *,
    required: Sequence[str],
    optional: Sequence[str] = (),
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must be a mapping")
    normalised = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise TypeError(f"{field_name} keys must be strings")
        key = _exact_str(key)
        if key in normalised:
            raise ValueError(f"{field_name} contains duplicate field {key!r} after key normalization")
        normalised[key] = item
    allowed = set(required) | set(optional)
    unknown = sorted(set(normalised) - allowed)
    if unknown:
        raise ValueError(f"{field_name} contains unknown fields: {unknown}")
    missing = sorted(set(required) - set(normalised))
    if missing:
        raise ValueError(f"{field_name} is missing required fields: {missing}")
    return normalised


def _load_json_mapping(serialized: str, field_name: str) -> Mapping[str, Any]:
    if not isinstance(serialized, str):
        raise TypeError(f"{field_name} must be a string")

    def reject_constant(value: str) -> None:
        raise ValueError(f"{field_name} contains unsupported JSON constant {value!r}")

    def unique_object(pairs: Sequence[tuple[str, Any]]) -> Mapping[str, Any]:
        result = {}
        for key, item in pairs:
            if key in result:
                raise ValueError(f"{field_name} contains duplicate object key {key!r}")
            result[key] = item
        return result

    decoded = json.loads(
        serialized,
        object_pairs_hook=unique_object,
        parse_constant=reject_constant,
    )
    if not isinstance(decoded, Mapping):
        raise TypeError(f"{field_name} must decode to a mapping")
    return decoded


def _freeze_json(value: Any, field_name: str) -> Any:
    """Take an immutable, detached snapshot of JSON-safe data."""

    if isinstance(value, Mapping):
        frozen = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{field_name} mapping keys must be strings")
            key = _exact_str(key)
            if key in frozen:
                raise ValueError(f"{field_name} contains duplicate key {key!r} after key normalization")
            frozen[key] = _freeze_json(item, field_name)
        return MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item, field_name) for item in value)
    if isinstance(value, str):
        return _exact_str(value)
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int):
        return int.__int__(value)
    if isinstance(value, float):
        result = float.__float__(value)
        if not math.isfinite(result):
            raise ValueError(f"{field_name} cannot contain NaN or infinite numbers")
        return result
    raise TypeError(f"{field_name} must contain only JSON-safe values; got {type(value).__name__}")


@dataclass(frozen=True)
class EventSelector:
    """Exact selection of one committed event occurrence."""

    name: str
    source: Optional[str] = None
    occurrence: int = 1

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _require_name(self.name, "event selector name"))
        if self.source is not None:
            object.__setattr__(self, "source", _require_name(self.source, "event selector source"))
        object.__setattr__(
            self,
            "occurrence",
            _require_positive_int(self.occurrence, "event selector occurrence"),
        )

    def matches(self, event: CommittedEvent) -> bool:
        record = CommittedEventRecord.from_event(event)
        return record.name == self.name and (self.source is None or record.source == self.source)

    def to_data(self) -> Mapping[str, Any]:
        return canonical_snapshot(self)

    @classmethod
    def from_data(cls, value: Mapping[str, Any]) -> "EventSelector":
        mapping = _fields(
            value,
            "event selector",
            required=("name",),
            optional=("source", "occurrence"),
        )
        return cls(
            name=mapping["name"],
            source=mapping.get("source"),
            occurrence=mapping.get("occurrence", 1),
        )


@dataclass(frozen=True)
class TriggerSpec:
    """A readiness or committed-event admission boundary."""

    kind: TriggerKind = TriggerKind.READY
    event: Optional[EventSelector] = None
    scope: EventScope = EventScope.AFTER_READY
    offset_s: float = 0.0
    timeout_s: Optional[float] = None

    def __post_init__(self) -> None:
        if isinstance(self.kind, str):
            object.__setattr__(self, "kind", TriggerKind(_exact_str(self.kind)))
        if not isinstance(self.kind, TriggerKind):
            raise TypeError("trigger kind must be a TriggerKind")
        if isinstance(self.scope, str):
            object.__setattr__(self, "scope", EventScope(_exact_str(self.scope)))
        if not isinstance(self.scope, EventScope):
            raise TypeError("trigger scope must be an EventScope")
        object.__setattr__(
            self,
            "offset_s",
            _require_nonnegative_duration(self.offset_s, "trigger offset_s"),
        )
        if self.timeout_s is not None:
            object.__setattr__(
                self,
                "timeout_s",
                _require_positive_duration(self.timeout_s, "trigger timeout_s"),
            )

        if self.kind is TriggerKind.READY:
            if self.event is not None:
                raise ValueError("ready triggers cannot declare an event selector")
            if self.scope is not EventScope.AFTER_READY:
                raise ValueError("ready triggers cannot change event scope")
            if self.timeout_s is not None:
                raise ValueError("ready triggers cannot declare a trigger timeout")
        elif type(self.event) is not EventSelector:
            raise TypeError("event triggers require an EventSelector")

    def to_data(self) -> Mapping[str, Any]:
        return canonical_snapshot(self)

    @classmethod
    def from_data(cls, value: Mapping[str, Any]) -> "TriggerSpec":
        mapping = _fields(
            value,
            "trigger",
            required=("kind",),
            optional=("event", "scope", "offset_s", "timeout_s"),
        )
        event = mapping.get("event")
        return cls(
            kind=mapping["kind"],
            event=None if event is None else EventSelector.from_data(event),
            scope=mapping.get("scope", EventScope.AFTER_READY),
            offset_s=mapping.get("offset_s", 0.0),
            timeout_s=mapping.get("timeout_s"),
        )


@dataclass(frozen=True)
class ActivationSpec:
    """Compiler/runner seam for one component's activation behavior."""

    trigger: TriggerSpec = field(default_factory=TriggerSpec)
    mode: ActivationMode = ActivationMode.SERIAL

    def __post_init__(self) -> None:
        if type(self.trigger) is not TriggerSpec:
            raise TypeError("activation trigger must be a TriggerSpec")
        if isinstance(self.mode, str):
            object.__setattr__(self, "mode", ActivationMode(_exact_str(self.mode)))
        if not isinstance(self.mode, ActivationMode):
            raise TypeError("activation mode must be an ActivationMode")

    def to_data(self) -> Mapping[str, Any]:
        return canonical_snapshot(self)

    @classmethod
    def from_data(cls, value: Mapping[str, Any]) -> "ActivationSpec":
        mapping = _fields(
            value,
            "activation",
            required=(),
            optional=("trigger", "mode"),
        )
        return cls(
            trigger=(TriggerSpec() if "trigger" not in mapping else TriggerSpec.from_data(mapping["trigger"])),
            mode=mapping.get("mode", ActivationMode.SERIAL),
        )


def is_default_activation(value: Any) -> bool:
    """Return whether *value* is the exact ready/serial declaration."""

    if type(value) is not ActivationSpec:
        return False
    trigger = value.trigger
    return (
        type(trigger) is TriggerSpec
        and trigger.kind is TriggerKind.READY
        and trigger.event is None
        and trigger.scope is EventScope.AFTER_READY
        and trigger.offset_s == 0.0
        and trigger.timeout_s is None
        and value.mode is ActivationMode.SERIAL
    )


@dataclass(frozen=True)
class ScheduledActivation:
    """Associate one immutable activation policy with a component instance."""

    component_instance_id: str
    activation: ActivationSpec = field(default_factory=ActivationSpec)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "component_instance_id",
            _require_name(self.component_instance_id, "component_instance_id"),
        )
        if type(self.activation) is not ActivationSpec:
            raise TypeError("activation must be an ActivationSpec")

    def to_data(self) -> Mapping[str, Any]:
        return canonical_snapshot(self)

    @classmethod
    def from_data(cls, value: Mapping[str, Any]) -> "ScheduledActivation":
        mapping = _fields(
            value,
            "scheduled activation",
            required=("component_instance_id",),
            optional=("activation",),
        )
        return cls(
            component_instance_id=mapping["component_instance_id"],
            activation=(
                ActivationSpec() if "activation" not in mapping else ActivationSpec.from_data(mapping["activation"])
            ),
        )


@dataclass(frozen=True)
class CommittedEventRecord:
    """Immutable persisted snapshot of a committed event."""

    name: str
    source: str
    payload: Mapping[str, Any]
    event_sequence: int
    commit_index: int
    occurred_at: float
    committed_at: float

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _require_name(self.name, "committed event name"))
        object.__setattr__(self, "source", _require_name(self.source, "committed event source"))
        if not isinstance(self.payload, Mapping):
            raise TypeError("committed event payload must be a mapping")
        object.__setattr__(
            self,
            "payload",
            _freeze_json(self.payload, "committed event payload"),
        )
        object.__setattr__(
            self,
            "event_sequence",
            _require_positive_int(self.event_sequence, "committed event event_sequence"),
        )
        object.__setattr__(
            self,
            "commit_index",
            _require_positive_int(self.commit_index, "committed event commit_index"),
        )
        object.__setattr__(
            self,
            "occurred_at",
            _require_timestamp(self.occurred_at, "committed event occurred_at"),
        )
        object.__setattr__(
            self,
            "committed_at",
            _require_timestamp(self.committed_at, "committed event committed_at"),
        )
        if self.committed_at < self.occurred_at:
            raise ValueError("committed event committed_at cannot precede occurred_at")

    @classmethod
    def from_event(cls, event: CommittedEvent) -> "CommittedEventRecord":
        if not isinstance(event, CommittedEvent):
            raise TypeError("event must implement CommittedEvent")
        return cls(
            name=event.name,
            source=event.source,
            payload=event.payload,
            event_sequence=event.event_sequence,
            commit_index=event.commit_index,
            occurred_at=event.occurred_at,
            committed_at=event.committed_at,
        )

    def to_data(self) -> Mapping[str, Any]:
        return canonical_snapshot(self)

    @classmethod
    def from_data(cls, value: Mapping[str, Any]) -> "CommittedEventRecord":
        mapping = _fields(
            value,
            "committed event",
            required=(
                "name",
                "source",
                "payload",
                "event_sequence",
                "commit_index",
                "occurred_at",
                "committed_at",
            ),
        )
        return cls(
            name=mapping["name"],
            source=mapping["source"],
            payload=mapping["payload"],
            event_sequence=mapping["event_sequence"],
            commit_index=mapping["commit_index"],
            occurred_at=mapping["occurred_at"],
            committed_at=mapping["committed_at"],
        )


@dataclass(frozen=True)
class ScheduleJournalEntry:
    """One component state transition emitted by a planner operation."""

    sequence: int
    operation_sequence: int
    batch_index: int
    component_instance_id: str
    transition: ScheduleTransition
    timestamp: float
    event_cursor: int
    trigger_event_sequence: Optional[int] = None
    trigger_event_commit_index: Optional[int] = None
    scheduled_for: Optional[float] = None
    reason: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "sequence",
            _require_positive_int(self.sequence, "schedule journal sequence"),
        )
        object.__setattr__(
            self,
            "operation_sequence",
            _require_positive_int(self.operation_sequence, "schedule journal operation_sequence"),
        )
        object.__setattr__(
            self,
            "batch_index",
            _require_nonnegative_int(self.batch_index, "schedule journal batch_index"),
        )
        object.__setattr__(
            self,
            "component_instance_id",
            _require_name(self.component_instance_id, "component_instance_id"),
        )
        if isinstance(self.transition, str):
            object.__setattr__(
                self,
                "transition",
                ScheduleTransition(_exact_str(self.transition)),
            )
        if not isinstance(self.transition, ScheduleTransition):
            raise TypeError("transition must be a ScheduleTransition")
        object.__setattr__(
            self,
            "timestamp",
            _require_timestamp(self.timestamp, "schedule journal timestamp"),
        )
        object.__setattr__(
            self,
            "event_cursor",
            _require_nonnegative_int(self.event_cursor, "schedule journal event_cursor"),
        )
        if (self.trigger_event_sequence is None) != (self.trigger_event_commit_index is None):
            raise ValueError("trigger event sequence and commit index must be declared together")
        if self.trigger_event_sequence is not None:
            object.__setattr__(
                self,
                "trigger_event_sequence",
                _require_positive_int(
                    self.trigger_event_sequence,
                    "schedule journal trigger_event_sequence",
                ),
            )
            object.__setattr__(
                self,
                "trigger_event_commit_index",
                _require_positive_int(
                    self.trigger_event_commit_index,
                    "schedule journal trigger_event_commit_index",
                ),
            )
            if self.trigger_event_commit_index > self.event_cursor:
                raise ValueError("trigger event commit index cannot exceed event_cursor")
        if self.scheduled_for is not None:
            object.__setattr__(
                self,
                "scheduled_for",
                _require_timestamp(self.scheduled_for, "schedule journal scheduled_for"),
            )
        object.__setattr__(self, "reason", _require_optional_text(self.reason, "schedule journal reason"))

        if self.transition is ScheduleTransition.READY:
            self._require_empty_decision_fields("ready")
        elif self.transition is ScheduleTransition.ADMITTED:
            if self.scheduled_for is None:
                raise ValueError("admitted journal entries require scheduled_for")
            if self.reason is not None:
                raise ValueError("admitted journal entries cannot contain a reason")
        elif self.transition is ScheduleTransition.INVOCATION_STARTED:
            self._require_empty_decision_fields("invocation_started")
        elif self.transition is ScheduleTransition.EXPIRED:
            if self.scheduled_for is None:
                raise ValueError("expired journal entries require their trigger deadline")
            if self.trigger_event_sequence is not None or self.reason is not None:
                raise ValueError("expired journal entries cannot contain trigger or reason fields")
        elif self.transition is ScheduleTransition.CANCELLED:
            self._require_empty_decision_fields("cancelled")
        elif self.scheduled_for is not None or self.trigger_event_sequence is not None:
            raise ValueError("admission_abandoned entries cannot contain trigger or scheduling fields")

    def _require_empty_decision_fields(self, transition: str) -> None:
        if self.trigger_event_sequence is not None or self.scheduled_for is not None or self.reason is not None:
            raise ValueError(f"{transition} journal entries cannot contain trigger, scheduling, or reason fields")

    def to_data(self) -> Mapping[str, Any]:
        return canonical_snapshot(self)

    @classmethod
    def from_data(cls, value: Mapping[str, Any]) -> "ScheduleJournalEntry":
        mapping = _fields(
            value,
            "schedule journal entry",
            required=(
                "sequence",
                "operation_sequence",
                "batch_index",
                "component_instance_id",
                "transition",
                "timestamp",
                "event_cursor",
                "trigger_event_sequence",
                "trigger_event_commit_index",
                "scheduled_for",
                "reason",
            ),
        )
        return cls(
            sequence=mapping["sequence"],
            operation_sequence=mapping["operation_sequence"],
            batch_index=mapping["batch_index"],
            component_instance_id=mapping["component_instance_id"],
            transition=mapping["transition"],
            timestamp=mapping["timestamp"],
            event_cursor=mapping["event_cursor"],
            trigger_event_sequence=mapping["trigger_event_sequence"],
            trigger_event_commit_index=mapping["trigger_event_commit_index"],
            scheduled_for=mapping["scheduled_for"],
            reason=mapping["reason"],
        )


@dataclass(frozen=True)
class ScheduleOperation:
    """One successful public planner call, including zero-output calls."""

    sequence: int
    kind: ScheduleOperationKind
    timestamp: float
    event_cursor: int
    component_instance_ids: tuple[str, ...] = ()
    all_pending: bool = False
    committed_event: Optional[CommittedEventRecord] = None
    reason: Optional[str] = None
    entry_sequences: tuple[int, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "sequence",
            _require_positive_int(self.sequence, "schedule operation sequence"),
        )
        if isinstance(self.kind, str):
            object.__setattr__(
                self,
                "kind",
                ScheduleOperationKind(_exact_str(self.kind)),
            )
        if not isinstance(self.kind, ScheduleOperationKind):
            raise TypeError("schedule operation kind must be a ScheduleOperationKind")
        object.__setattr__(
            self,
            "timestamp",
            _require_timestamp(self.timestamp, "schedule operation timestamp"),
        )
        object.__setattr__(
            self,
            "event_cursor",
            _require_nonnegative_int(self.event_cursor, "schedule operation event_cursor"),
        )
        identifiers = _ordered(self.component_instance_ids, "schedule operation component_instance_ids")
        if any(not isinstance(identifier, str) for identifier in identifiers):
            raise TypeError("schedule operation component_instance_ids must contain strings")
        identifiers = tuple(_require_name(identifier, "component_instance_id") for identifier in identifiers)
        if len(set(identifiers)) != len(identifiers):
            raise ValueError("schedule operation component_instance_ids cannot contain duplicates")
        object.__setattr__(self, "component_instance_ids", identifiers)
        if not isinstance(self.all_pending, bool):
            raise TypeError("schedule operation all_pending must be a bool")
        if self.committed_event is not None and not isinstance(self.committed_event, CommittedEventRecord):
            raise TypeError("schedule operation committed_event must be a CommittedEventRecord")
        object.__setattr__(self, "reason", _require_optional_text(self.reason, "schedule operation reason"))
        entry_sequences = _ordered(self.entry_sequences, "schedule operation entry_sequences")
        entry_sequences = tuple(
            _require_positive_int(entry_sequence, "schedule operation entry sequence")
            for entry_sequence in entry_sequences
        )
        object.__setattr__(self, "entry_sequences", entry_sequences)

        if self.kind is ScheduleOperationKind.OBSERVE:
            if self.committed_event is None:
                raise ValueError("observe operations require a committed event")
            if self.timestamp < self.committed_event.committed_at:
                raise ValueError("observe operation timestamp cannot precede event committed_at")
            if self.event_cursor != self.committed_event.commit_index:
                raise ValueError("observe operation cursor must equal event commit_index")
            if identifiers or self.all_pending or self.reason is not None or entry_sequences:
                raise ValueError("observe operations cannot contain component, cancellation, reason, or entry fields")
        elif self.committed_event is not None:
            raise ValueError(f"{self.kind.value} operations cannot contain a committed event")
        elif self.kind is ScheduleOperationKind.READY:
            if self.all_pending or self.reason is not None:
                raise ValueError("ready operations cannot contain cancellation or reason fields")
            if len(entry_sequences) != len(identifiers):
                raise ValueError("ready operations must emit exactly one entry per component")
        elif self.kind is ScheduleOperationKind.ADVANCE:
            if identifiers or self.all_pending or self.reason is not None:
                raise ValueError("advance operations cannot contain component, cancellation, or reason fields")
        elif self.kind is ScheduleOperationKind.CANCEL:
            if self.reason is not None:
                raise ValueError("cancel operations cannot contain a reason")
            if self.all_pending and identifiers:
                raise ValueError("cancel-all operations cannot also name components")
        elif self.kind is ScheduleOperationKind.START:
            if not identifiers:
                raise ValueError("start operations require at least one component")
            if self.all_pending or self.reason is not None:
                raise ValueError("start operations cannot contain cancellation or reason fields")
            if len(entry_sequences) != len(identifiers):
                raise ValueError("start operations must emit exactly one entry per component")
        else:
            if not identifiers:
                raise ValueError("abandon operations require at least one component")
            if self.all_pending:
                raise ValueError("abandon operations cannot use all_pending")
            if len(entry_sequences) != len(identifiers):
                raise ValueError("abandon operations must emit exactly one entry per component")

    def to_data(self) -> Mapping[str, Any]:
        return canonical_snapshot(self)

    @classmethod
    def from_data(cls, value: Mapping[str, Any]) -> "ScheduleOperation":
        mapping = _fields(
            value,
            "schedule operation",
            required=(
                "sequence",
                "kind",
                "timestamp",
                "event_cursor",
                "component_instance_ids",
                "all_pending",
                "committed_event",
                "reason",
                "entry_sequences",
            ),
        )
        event = mapping["committed_event"]
        return cls(
            sequence=mapping["sequence"],
            kind=mapping["kind"],
            timestamp=mapping["timestamp"],
            event_cursor=mapping["event_cursor"],
            component_instance_ids=_ordered(
                mapping["component_instance_ids"],
                "schedule operation component_instance_ids",
            ),
            all_pending=mapping["all_pending"],
            committed_event=None if event is None else CommittedEventRecord.from_data(event),
            reason=mapping["reason"],
            entry_sequences=_ordered(mapping["entry_sequences"], "schedule operation entry_sequences"),
        )


_EXPECTED_TRANSITIONS = {
    ScheduleOperationKind.OBSERVE: frozenset(),
    ScheduleOperationKind.READY: frozenset((ScheduleTransition.READY,)),
    ScheduleOperationKind.ADVANCE: frozenset((ScheduleTransition.ADMITTED, ScheduleTransition.EXPIRED)),
    ScheduleOperationKind.CANCEL: frozenset((ScheduleTransition.CANCELLED,)),
    ScheduleOperationKind.START: frozenset((ScheduleTransition.INVOCATION_STARTED,)),
    ScheduleOperationKind.ABANDON: frozenset((ScheduleTransition.ADMISSION_ABANDONED,)),
}


def _trace_digest(
    activation_digest: str,
    operations: Sequence[ScheduleOperation],
    entries: Sequence[ScheduleJournalEntry],
) -> str:
    return canonical_digest(
        {
            "schema_id": SCHEDULE_JOURNAL_SCHEMA_ID,
            "schema_version": SCHEDULE_JOURNAL_SCHEMA_VERSION,
            "activation_digest": activation_digest,
            "operations": operations,
            "entries": entries,
        }
    )


@dataclass(frozen=True)
class ScheduleJournalSnapshot:
    """Immutable exact scheduler history suitable for diagnostics and replay."""

    activation_digest: str
    operations: tuple[ScheduleOperation, ...] = ()
    entries: tuple[ScheduleJournalEntry, ...] = ()
    schema_id: str = SCHEDULE_JOURNAL_SCHEMA_ID
    schema_version: int = SCHEDULE_JOURNAL_SCHEMA_VERSION
    operation_high_water_mark: Optional[int] = None
    entry_high_water_mark: Optional[int] = None
    trace_digest: Optional[str] = None

    def __post_init__(self) -> None:
        schema_id = _require_name(self.schema_id, "schedule journal schema_id")
        object.__setattr__(self, "schema_id", schema_id)
        if schema_id != SCHEDULE_JOURNAL_SCHEMA_ID:
            raise ValueError(f"unsupported schedule journal schema_id {schema_id!r}")
        schema_version = _require_positive_int(self.schema_version, "schedule journal schema_version")
        object.__setattr__(self, "schema_version", schema_version)
        if schema_version != SCHEDULE_JOURNAL_SCHEMA_VERSION:
            raise ValueError(f"unsupported schedule journal schema_version {schema_version!r}")
        if not isinstance(self.activation_digest, str):
            raise ValueError("activation_digest must use the form 'sha256:<64 lowercase hexadecimal characters>'")
        activation_digest = _exact_str(self.activation_digest)
        if not _DIGEST_PATTERN.fullmatch(activation_digest):
            raise ValueError("activation_digest must use the form 'sha256:<64 lowercase hexadecimal characters>'")
        object.__setattr__(self, "activation_digest", activation_digest)
        operations = _ordered(self.operations, "schedule journal operations")
        entries = _ordered(self.entries, "schedule journal entries")
        if any(not isinstance(operation, ScheduleOperation) for operation in operations):
            raise TypeError("schedule journal operations must contain ScheduleOperation values")
        if any(not isinstance(entry, ScheduleJournalEntry) for entry in entries):
            raise TypeError("schedule journal entries must contain ScheduleJournalEntry values")
        object.__setattr__(self, "operations", operations)
        object.__setattr__(self, "entries", entries)

        operation_hwm = len(operations) if self.operation_high_water_mark is None else self.operation_high_water_mark
        entry_hwm = len(entries) if self.entry_high_water_mark is None else self.entry_high_water_mark
        operation_hwm = _require_nonnegative_int(
            operation_hwm,
            "schedule journal operation_high_water_mark",
        )
        entry_hwm = _require_nonnegative_int(
            entry_hwm,
            "schedule journal entry_high_water_mark",
        )
        if operation_hwm != len(operations):
            raise ValueError("operation_high_water_mark must equal the number of operations")
        if entry_hwm != len(entries):
            raise ValueError("entry_high_water_mark must equal the number of entries")
        object.__setattr__(self, "operation_high_water_mark", operation_hwm)
        object.__setattr__(self, "entry_high_water_mark", entry_hwm)

        if tuple(operation.sequence for operation in operations) != tuple(range(1, len(operations) + 1)):
            raise ValueError("schedule operation sequences must be contiguous and start at one")
        if tuple(entry.sequence for entry in entries) != tuple(range(1, len(entries) + 1)):
            raise ValueError("schedule journal entry sequences must be contiguous and start at one")
        self._validate_trace_shape()

        expected_digest = _trace_digest(self.activation_digest, operations, entries)
        if self.trace_digest is None:
            object.__setattr__(self, "trace_digest", expected_digest)
        else:
            if not isinstance(self.trace_digest, str):
                raise ValueError("trace_digest must use the form 'sha256:<64 lowercase hexadecimal characters>'")
            trace_digest = _exact_str(self.trace_digest)
            if not _DIGEST_PATTERN.fullmatch(trace_digest):
                raise ValueError("trace_digest must use the form 'sha256:<64 lowercase hexadecimal characters>'")
            object.__setattr__(self, "trace_digest", trace_digest)
            if trace_digest != expected_digest:
                raise ValueError("schedule journal trace_digest does not match its operation trace")

    def _validate_trace_shape(self) -> None:
        entries_by_sequence = {entry.sequence: entry for entry in self.entries}
        flattened_entry_sequences = []
        previous_timestamp: Optional[float] = None
        previous_committed_at: Optional[float] = None
        current_cursor = 0
        states: dict[str, ScheduleTransition] = {}

        for operation in self.operations:
            if previous_timestamp is not None and operation.timestamp < previous_timestamp:
                raise ValueError("schedule operation timestamps must be non-decreasing")
            previous_timestamp = operation.timestamp
            if operation.kind is ScheduleOperationKind.OBSERVE:
                assert operation.committed_event is not None
                event = operation.committed_event
                if event.commit_index != current_cursor + 1:
                    raise ValueError("committed event commit indexes must be contiguous and start at one")
                if previous_committed_at is not None and event.committed_at < previous_committed_at:
                    raise ValueError("committed event commit timestamps must be non-decreasing")
                previous_committed_at = event.committed_at
                current_cursor = event.commit_index
            elif operation.event_cursor != current_cursor:
                raise ValueError("schedule operation event_cursor does not match the observed commit stream")

            flattened_entry_sequences.extend(operation.entry_sequences)
            expected_transitions = _EXPECTED_TRANSITIONS[operation.kind]
            for batch_index, entry_sequence in enumerate(operation.entry_sequences):
                entry = entries_by_sequence.get(entry_sequence)
                if entry is None:
                    raise ValueError(f"schedule operation references unknown entry {entry_sequence}")
                if entry.operation_sequence != operation.sequence:
                    raise ValueError("schedule entry operation_sequence does not match its operation")
                if entry.batch_index != batch_index:
                    raise ValueError("schedule entry batch indexes must be contiguous within each operation")
                if entry.timestamp != operation.timestamp or entry.event_cursor != operation.event_cursor:
                    raise ValueError("schedule entry decision boundary does not match its operation")
                if entry.transition not in expected_transitions:
                    raise ValueError(f"{operation.kind.value} operation cannot emit {entry.transition.value}")
                previous = states.get(entry.component_instance_id)
                if entry.transition is ScheduleTransition.READY:
                    if previous is not None:
                        raise ValueError(f"component {entry.component_instance_id!r} became ready more than once")
                elif entry.transition is ScheduleTransition.CANCELLED:
                    if previous not in (None, ScheduleTransition.READY):
                        raise ValueError(
                            f"component {entry.component_instance_id!r} cannot be cancelled after {previous.value}"
                        )
                elif entry.transition in (ScheduleTransition.ADMITTED, ScheduleTransition.EXPIRED):
                    if previous is not ScheduleTransition.READY:
                        raise ValueError(
                            f"component {entry.component_instance_id!r} must be ready before {entry.transition.value}"
                        )
                elif previous is not ScheduleTransition.ADMITTED:
                    raise ValueError(
                        f"component {entry.component_instance_id!r} must be admitted before {entry.transition.value}"
                    )
                states[entry.component_instance_id] = entry.transition

        if tuple(flattened_entry_sequences) != tuple(range(1, len(self.entries) + 1)):
            raise ValueError("schedule operation batches must partition journal entries in exact order")

    @property
    def admission_order(self) -> tuple[str, ...]:
        return tuple(
            entry.component_instance_id for entry in self.entries if entry.transition is ScheduleTransition.ADMITTED
        )

    @property
    def invocation_start_order(self) -> tuple[str, ...]:
        return tuple(
            entry.component_instance_id
            for entry in self.entries
            if entry.transition is ScheduleTransition.INVOCATION_STARTED
        )

    @property
    def cleanup_order(self) -> tuple[str, ...]:
        """Reverse actual invocation-start order."""

        return tuple(reversed(self.invocation_start_order))

    @property
    def digest(self) -> str:
        assert self.trace_digest is not None
        return self.trace_digest

    def to_data(self) -> Mapping[str, Any]:
        return canonical_snapshot(self)

    def to_json(self) -> str:
        return canonical_json(self)

    @classmethod
    def from_data(cls, value: Mapping[str, Any]) -> "ScheduleJournalSnapshot":
        mapping = _fields(
            value,
            "schedule journal",
            required=(
                "schema_id",
                "schema_version",
                "activation_digest",
                "operation_high_water_mark",
                "entry_high_water_mark",
                "trace_digest",
                "operations",
                "entries",
            ),
        )
        for field_name in (
            "operation_high_water_mark",
            "entry_high_water_mark",
            "trace_digest",
        ):
            if mapping[field_name] is None:
                raise TypeError(f"schedule journal {field_name} must not be null")
        return cls(
            schema_id=mapping["schema_id"],
            schema_version=mapping["schema_version"],
            activation_digest=mapping["activation_digest"],
            operation_high_water_mark=mapping["operation_high_water_mark"],
            entry_high_water_mark=mapping["entry_high_water_mark"],
            trace_digest=mapping["trace_digest"],
            operations=tuple(
                ScheduleOperation.from_data(operation)
                for operation in _ordered(mapping["operations"], "schedule journal operations")
            ),
            entries=tuple(
                ScheduleJournalEntry.from_data(entry)
                for entry in _ordered(mapping["entries"], "schedule journal entries")
            ),
        )

    @classmethod
    def from_json(cls, serialized: str) -> "ScheduleJournalSnapshot":
        return cls.from_data(_load_json_mapping(serialized, "schedule journal JSON"))


@dataclass(frozen=True)
class SchedulerWorkCounters:
    """Algorithmic work counters used for regression and diagnostics."""

    observed_events: int
    stream_appends: int
    waiter_bucket_probes: int
    waiter_registrations: int
    waiter_candidates_processed: int
    heap_pushes: int
    heap_pops: int
    stale_heap_pops: int


class _Phase(str, Enum):
    INITIAL = "initial"
    READY = "ready"
    ADMITTED = "admitted"
    TERMINAL = "terminal"


@dataclass
class _ActivationState:
    declaration_index: int
    declaration: ScheduledActivation
    phase: _Phase = _Phase.INITIAL
    ready_at: Optional[float] = None
    ready_cursor: int = 0
    matched_event: Optional[CommittedEventRecord] = None
    scheduled_for: Optional[float] = None
    deadline: Optional[float] = None
    terminal_transition: Optional[ScheduleTransition] = None


def _normalise_activations(
    activations: Sequence[ScheduledActivation],
) -> tuple[ScheduledActivation, ...]:
    entries = _ordered(activations, "scheduled activations")
    if any(type(entry) is not ScheduledActivation for entry in entries):
        raise TypeError("scheduled activations must contain ScheduledActivation values")
    seen = set()
    for entry in entries:
        if entry.component_instance_id in seen:
            raise DuplicateActivationError(
                f"component {entry.component_instance_id!r} has more than one activation declaration"
            )
        seen.add(entry.component_instance_id)
    return entries


def _normalise_committed_events(
    committed_events: Sequence[CommittedEvent],
) -> tuple[CommittedEventRecord, ...]:
    values = _ordered(committed_events, "committed events")
    records = tuple(CommittedEventRecord.from_event(event) for event in values)
    previous_committed_at: Optional[float] = None
    for index, event in enumerate(records, 1):
        if event.commit_index != index:
            raise ValueError("committed event commit indexes must be contiguous and start at one")
        if previous_committed_at is not None and event.committed_at < previous_committed_at:
            raise ValueError("committed event commit timestamps must be non-decreasing")
        previous_committed_at = event.committed_at
    return records


class DeterministicActivationPlanner:
    """Thread-safe deterministic activation state machine.

    Public calls are successful transactions: arguments are validated before
    state changes, then one :class:`ScheduleOperation` is appended. Failed
    calls leave no operation or component transition behind.
    """

    def __init__(self, activations: Sequence[ScheduledActivation]) -> None:
        declarations = _normalise_activations(activations)
        self._lock = threading.RLock()
        self._activation_digest = canonical_digest(declarations)
        self._states = {
            declaration.component_instance_id: _ActivationState(index, declaration)
            for index, declaration in enumerate(declarations)
        }
        self._streams: dict[tuple[str, Optional[str]], list[CommittedEventRecord]] = {}
        self._waiters: dict[tuple[str, Optional[str]], dict[int, list[str]]] = {}
        self._due_heap: list[tuple[float, int, str]] = []
        self._deadline_heap: list[tuple[float, int, str]] = []
        self._events: list[CommittedEventRecord] = []
        self._journal: list[ScheduleJournalEntry] = []
        self._operations: list[ScheduleOperation] = []
        self._last_commit_index = 0
        self._last_operation_timestamp: Optional[float] = None
        self._counter_values = {
            "observed_events": 0,
            "stream_appends": 0,
            "waiter_bucket_probes": 0,
            "waiter_registrations": 0,
            "waiter_candidates_processed": 0,
            "heap_pushes": 0,
            "heap_pops": 0,
            "stale_heap_pops": 0,
        }

    def _state(self, component_instance_id: str) -> _ActivationState:
        try:
            return self._states[component_instance_id]
        except KeyError as exc:
            raise UnknownActivationError(component_instance_id) from exc

    def _control_timestamp(self, value: Any) -> float:
        timestamp = _require_timestamp(value, "scheduler timestamp")
        if self._last_operation_timestamp is not None and timestamp < self._last_operation_timestamp:
            raise InvalidScheduleTransitionError(
                f"scheduler timestamp {timestamp} regresses below operation time {self._last_operation_timestamp}"
            )
        return timestamp

    def _record(
        self,
        *,
        operation_sequence: int,
        batch_index: int,
        component_instance_id: str,
        transition: ScheduleTransition,
        timestamp: float,
        trigger_event: Optional[CommittedEventRecord] = None,
        scheduled_for: Optional[float] = None,
        reason: Optional[str] = None,
    ) -> ScheduleJournalEntry:
        entry = ScheduleJournalEntry(
            sequence=len(self._journal) + 1,
            operation_sequence=operation_sequence,
            batch_index=batch_index,
            component_instance_id=component_instance_id,
            transition=transition,
            timestamp=timestamp,
            event_cursor=self._last_commit_index,
            trigger_event_sequence=None if trigger_event is None else trigger_event.event_sequence,
            trigger_event_commit_index=None if trigger_event is None else trigger_event.commit_index,
            scheduled_for=scheduled_for,
            reason=reason,
        )
        self._journal.append(entry)
        return entry

    def _append_operation(
        self,
        *,
        kind: ScheduleOperationKind,
        timestamp: float,
        component_instance_ids: Sequence[str] = (),
        all_pending: bool = False,
        committed_event: Optional[CommittedEventRecord] = None,
        reason: Optional[str] = None,
        entries: Sequence[ScheduleJournalEntry] = (),
    ) -> ScheduleOperation:
        operation = ScheduleOperation(
            sequence=len(self._operations) + 1,
            kind=kind,
            timestamp=timestamp,
            event_cursor=self._last_commit_index,
            component_instance_ids=tuple(component_instance_ids),
            all_pending=all_pending,
            committed_event=committed_event,
            reason=reason,
            entry_sequences=tuple(entry.sequence for entry in entries),
        )
        self._operations.append(operation)
        self._last_operation_timestamp = timestamp
        return operation

    @staticmethod
    def _selector_key(selector: EventSelector) -> tuple[str, Optional[str]]:
        return selector.name, selector.source

    def _push_due(self, state: _ActivationState) -> None:
        assert state.scheduled_for is not None
        heapq.heappush(
            self._due_heap,
            (
                state.scheduled_for,
                state.declaration_index,
                state.declaration.component_instance_id,
            ),
        )
        self._counter_values["heap_pushes"] += 1

    def _push_deadline(self, state: _ActivationState) -> None:
        assert state.deadline is not None
        heapq.heappush(
            self._deadline_heap,
            (
                state.deadline,
                state.declaration_index,
                state.declaration.component_instance_id,
            ),
        )
        self._counter_values["heap_pushes"] += 1

    def _match_event(self, state: _ActivationState, event: CommittedEventRecord) -> None:
        if state.phase is not _Phase.READY or state.matched_event is not None:
            return
        if state.deadline is not None and event.committed_at > state.deadline:
            return
        assert state.ready_at is not None
        trigger = state.declaration.activation.trigger
        event_boundary = _checked_time_add(
            event.committed_at,
            trigger.offset_s,
            "event activation boundary",
        )
        state.matched_event = event
        state.scheduled_for = max(state.ready_at, event_boundary)
        self._push_due(state)

    def _event_target(
        self,
        state: _ActivationState,
    ) -> tuple[tuple[str, Optional[str]], int, Optional[CommittedEventRecord]]:
        trigger = state.declaration.activation.trigger
        assert trigger.kind is TriggerKind.EVENT
        assert trigger.event is not None
        key = self._selector_key(trigger.event)
        stream = self._streams.get(key, ())
        if trigger.scope is EventScope.RUN:
            target_occurrence = trigger.event.occurrence
        else:
            target_occurrence = len(stream) + trigger.event.occurrence
        selected = stream[target_occurrence - 1] if len(stream) >= target_occurrence else None
        return key, target_occurrence, selected

    def _preflight_readiness(self, state: _ActivationState, timestamp: float) -> None:
        trigger = state.declaration.activation.trigger
        if trigger.kind is TriggerKind.READY:
            _checked_time_add(timestamp, trigger.offset_s, "ready activation boundary")
            return
        deadline = (
            None
            if trigger.timeout_s is None
            else _checked_time_add(timestamp, trigger.timeout_s, "event trigger deadline")
        )
        _key, _target_occurrence, selected = self._event_target(state)
        if selected is not None and (deadline is None or selected.committed_at <= deadline):
            _checked_time_add(
                selected.committed_at,
                trigger.offset_s,
                "event activation boundary",
            )

    def _preflight_event_observation(self, event: CommittedEventRecord) -> None:
        for key in ((event.name, None), (event.name, event.source)):
            stream_size = len(self._streams.get(key, ())) + 1
            buckets = self._waiters.get(key)
            waiting = () if not buckets else buckets.get(stream_size, ())
            for identifier in waiting:
                state = self._states[identifier]
                if state.phase is not _Phase.READY or state.matched_event is not None:
                    continue
                if state.deadline is not None and event.committed_at > state.deadline:
                    continue
                trigger = state.declaration.activation.trigger
                _checked_time_add(
                    event.committed_at,
                    trigger.offset_s,
                    "event activation boundary",
                )

    def _prepare_event_trigger(self, state: _ActivationState) -> None:
        key, target_occurrence, selected = self._event_target(state)
        if selected is not None:
            self._match_event(state, selected)
            return
        buckets = self._waiters.setdefault(key, {})
        buckets.setdefault(target_occurrence, []).append(state.declaration.component_instance_id)
        self._counter_values["waiter_registrations"] += 1

    def mark_ready(
        self,
        component_instance_id: str,
        *,
        timestamp: float,
    ) -> ScheduleJournalEntry:
        return self.mark_ready_many((component_instance_id,), timestamp=timestamp)[0]

    def mark_ready_many(
        self,
        component_instance_ids: Sequence[str],
        *,
        timestamp: float,
    ) -> tuple[ScheduleJournalEntry, ...]:
        """Mark a dependency-satisfied batch ready in declaration order."""

        raw_identifiers = _ordered(component_instance_ids, "component_instance_ids")
        if any(not isinstance(identifier, str) for identifier in raw_identifiers):
            raise TypeError("component_instance_ids must contain strings")
        identifiers = tuple(_exact_str(identifier) for identifier in raw_identifiers)
        if len(set(identifiers)) != len(identifiers):
            raise InvalidScheduleTransitionError("component_instance_ids contains duplicates")
        with self._lock:
            timestamp = self._control_timestamp(timestamp)
            states = [self._state(identifier) for identifier in identifiers]
            for state in states:
                if state.phase is not _Phase.INITIAL:
                    raise InvalidScheduleTransitionError(
                        f"component {state.declaration.component_instance_id!r} is not awaiting readiness"
                    )
            states.sort(key=lambda state: state.declaration_index)
            for state in states:
                self._preflight_readiness(state, timestamp)
            operation_sequence = len(self._operations) + 1
            entries = []
            for batch_index, state in enumerate(states):
                state.phase = _Phase.READY
                state.ready_at = timestamp
                state.ready_cursor = self._last_commit_index
                entry = self._record(
                    operation_sequence=operation_sequence,
                    batch_index=batch_index,
                    component_instance_id=state.declaration.component_instance_id,
                    transition=ScheduleTransition.READY,
                    timestamp=timestamp,
                )
                entries.append(entry)
                trigger = state.declaration.activation.trigger
                if trigger.kind is TriggerKind.READY:
                    state.scheduled_for = _checked_time_add(
                        timestamp,
                        trigger.offset_s,
                        "ready activation boundary",
                    )
                    self._push_due(state)
                else:
                    if trigger.timeout_s is not None:
                        state.deadline = _checked_time_add(
                            timestamp,
                            trigger.timeout_s,
                            "event trigger deadline",
                        )
                        self._push_deadline(state)
                    self._prepare_event_trigger(state)
            self._append_operation(
                kind=ScheduleOperationKind.READY,
                timestamp=timestamp,
                component_instance_ids=identifiers,
                entries=entries,
            )
            return tuple(entries)

    def observe_committed(
        self,
        event: CommittedEvent,
        *,
        timestamp: float,
    ) -> None:
        """Observe one committed event at an explicit scheduler time.

        ``timestamp`` is when the coordinator consumed the envelope. It may be
        later than ``event.committed_at`` when an event is drained after
        another scheduler decision, but it cannot precede the commit.
        """

        record = CommittedEventRecord.from_event(event)
        with self._lock:
            timestamp = self._control_timestamp(timestamp)
            if timestamp < record.committed_at:
                raise InvalidScheduleTransitionError(
                    "committed event observation timestamp cannot precede committed_at"
                )
            if record.commit_index != self._last_commit_index + 1:
                raise InvalidScheduleTransitionError(
                    "committed event commit indexes must be contiguous and start at one"
                )
            if self._events and record.committed_at < self._events[-1].committed_at:
                raise InvalidScheduleTransitionError("committed event commit timestamps must be non-decreasing")
            self._preflight_event_observation(record)

            self._events.append(record)
            self._last_commit_index = record.commit_index
            self._counter_values["observed_events"] += 1
            keys = ((record.name, None), (record.name, record.source))
            for key in keys:
                stream = self._streams.setdefault(key, [])
                stream.append(record)
                self._counter_values["stream_appends"] += 1
                self._counter_values["waiter_bucket_probes"] += 1
                buckets = self._waiters.get(key)
                if not buckets:
                    continue
                waiting = buckets.pop(len(stream), ())
                if not buckets:
                    self._waiters.pop(key, None)
                self._counter_values["waiter_candidates_processed"] += len(waiting)
                for identifier in waiting:
                    self._match_event(self._states[identifier], record)
            self._append_operation(
                kind=ScheduleOperationKind.OBSERVE,
                timestamp=timestamp,
                committed_event=record,
            )

    def _pop_due_candidates(
        self,
        timestamp: float,
    ) -> list[tuple[float, int, ScheduleTransition, _ActivationState]]:
        candidates = []
        while self._due_heap and self._due_heap[0][0] <= timestamp:
            boundary, declaration_index, identifier = heapq.heappop(self._due_heap)
            self._counter_values["heap_pops"] += 1
            state = self._states[identifier]
            if state.phase is not _Phase.READY or state.scheduled_for != boundary:
                self._counter_values["stale_heap_pops"] += 1
                continue
            candidates.append((boundary, declaration_index, ScheduleTransition.ADMITTED, state))
        while self._deadline_heap and self._deadline_heap[0][0] <= timestamp:
            boundary, declaration_index, identifier = heapq.heappop(self._deadline_heap)
            self._counter_values["heap_pops"] += 1
            state = self._states[identifier]
            if state.phase is not _Phase.READY or state.deadline != boundary or state.matched_event is not None:
                self._counter_values["stale_heap_pops"] += 1
                continue
            candidates.append((boundary, declaration_index, ScheduleTransition.EXPIRED, state))
        candidates.sort(key=lambda item: (item[0], item[1]))
        return candidates

    def advance(self, timestamp: float) -> tuple[ScheduleJournalEntry, ...]:
        """Admit due components and expire missed event triggers."""

        with self._lock:
            timestamp = self._control_timestamp(timestamp)
            candidates = self._pop_due_candidates(timestamp)
            operation_sequence = len(self._operations) + 1
            entries = []
            for batch_index, (boundary, _index, transition, state) in enumerate(candidates):
                trigger_event = state.matched_event if transition is ScheduleTransition.ADMITTED else None
                entry = self._record(
                    operation_sequence=operation_sequence,
                    batch_index=batch_index,
                    component_instance_id=state.declaration.component_instance_id,
                    transition=transition,
                    timestamp=timestamp,
                    trigger_event=trigger_event,
                    scheduled_for=boundary,
                )
                entries.append(entry)
                if transition is ScheduleTransition.ADMITTED:
                    state.phase = _Phase.ADMITTED
                else:
                    state.phase = _Phase.TERMINAL
                    state.terminal_transition = transition
            self._append_operation(
                kind=ScheduleOperationKind.ADVANCE,
                timestamp=timestamp,
                entries=entries,
            )
            return tuple(entries)

    def cancel_pending(
        self,
        *,
        timestamp: float,
        component_instance_ids: Optional[Sequence[str]] = None,
    ) -> tuple[ScheduleJournalEntry, ...]:
        """Cancel initial or ready declarations in stable declaration order."""

        with self._lock:
            timestamp = self._control_timestamp(timestamp)
            all_pending = component_instance_ids is None
            if all_pending:
                identifiers: tuple[str, ...] = ()
                states = [state for state in self._states.values() if state.phase in (_Phase.INITIAL, _Phase.READY)]
            else:
                raw_identifiers = _ordered(component_instance_ids, "component_instance_ids")
                if any(not isinstance(identifier, str) for identifier in raw_identifiers):
                    raise TypeError("component_instance_ids must contain strings")
                identifiers = tuple(_exact_str(identifier) for identifier in raw_identifiers)
                if len(set(identifiers)) != len(identifiers):
                    raise InvalidScheduleTransitionError("component_instance_ids contains duplicates")
                states = [self._state(identifier) for identifier in identifiers]
                for state in states:
                    if state.phase not in (_Phase.INITIAL, _Phase.READY):
                        raise InvalidScheduleTransitionError(
                            f"component {state.declaration.component_instance_id!r} cannot be cancelled "
                            f"from {state.phase.value}"
                        )
            states.sort(key=lambda state: state.declaration_index)
            operation_sequence = len(self._operations) + 1
            entries = []
            for batch_index, state in enumerate(states):
                entry = self._record(
                    operation_sequence=operation_sequence,
                    batch_index=batch_index,
                    component_instance_id=state.declaration.component_instance_id,
                    transition=ScheduleTransition.CANCELLED,
                    timestamp=timestamp,
                )
                entries.append(entry)
                state.phase = _Phase.TERMINAL
                state.terminal_transition = ScheduleTransition.CANCELLED
            self._append_operation(
                kind=ScheduleOperationKind.CANCEL,
                timestamp=timestamp,
                component_instance_ids=identifiers,
                all_pending=all_pending,
                entries=entries,
            )
            return tuple(entries)

    def mark_invocation_started(
        self,
        component_instance_id: str,
        *,
        timestamp: float,
    ) -> ScheduleJournalEntry:
        return self.mark_invocation_started_many((component_instance_id,), timestamp=timestamp)[0]

    def mark_invocation_started_many(
        self,
        component_instance_ids: Sequence[str],
        *,
        timestamp: float,
    ) -> tuple[ScheduleJournalEntry, ...]:
        """Transfer cleanup ownership in caller-provided start order."""

        raw_identifiers = _ordered(component_instance_ids, "component_instance_ids")
        if not raw_identifiers:
            raise InvalidScheduleTransitionError("invocation start requires at least one component")
        if any(not isinstance(identifier, str) for identifier in raw_identifiers):
            raise TypeError("component_instance_ids must contain strings")
        identifiers = tuple(_exact_str(identifier) for identifier in raw_identifiers)
        if len(set(identifiers)) != len(identifiers):
            raise InvalidScheduleTransitionError("component_instance_ids contains duplicates")
        with self._lock:
            timestamp = self._control_timestamp(timestamp)
            states = [self._state(identifier) for identifier in identifiers]
            for state in states:
                if state.phase is not _Phase.ADMITTED:
                    raise InvalidScheduleTransitionError(
                        f"component {state.declaration.component_instance_id!r} is not admitted"
                    )
            operation_sequence = len(self._operations) + 1
            entries = []
            for batch_index, state in enumerate(states):
                entry = self._record(
                    operation_sequence=operation_sequence,
                    batch_index=batch_index,
                    component_instance_id=state.declaration.component_instance_id,
                    transition=ScheduleTransition.INVOCATION_STARTED,
                    timestamp=timestamp,
                )
                entries.append(entry)
                state.phase = _Phase.TERMINAL
                state.terminal_transition = ScheduleTransition.INVOCATION_STARTED
            self._append_operation(
                kind=ScheduleOperationKind.START,
                timestamp=timestamp,
                component_instance_ids=identifiers,
                entries=entries,
            )
            return tuple(entries)

    def abandon_admission(
        self,
        component_instance_id: str,
        *,
        timestamp: float,
        reason: Optional[str] = None,
    ) -> ScheduleJournalEntry:
        return self.abandon_admissions((component_instance_id,), timestamp=timestamp, reason=reason)[0]

    def abandon_admissions(
        self,
        component_instance_ids: Sequence[str],
        *,
        timestamp: float,
        reason: Optional[str] = None,
    ) -> tuple[ScheduleJournalEntry, ...]:
        """Finish admitted components that the runner did not invoke."""

        raw_identifiers = _ordered(component_instance_ids, "component_instance_ids")
        if not raw_identifiers:
            raise InvalidScheduleTransitionError("admission abandonment requires at least one component")
        if any(not isinstance(identifier, str) for identifier in raw_identifiers):
            raise TypeError("component_instance_ids must contain strings")
        identifiers = tuple(_exact_str(identifier) for identifier in raw_identifiers)
        if len(set(identifiers)) != len(identifiers):
            raise InvalidScheduleTransitionError("component_instance_ids contains duplicates")
        reason = _require_optional_text(reason, "admission abandonment reason")
        with self._lock:
            timestamp = self._control_timestamp(timestamp)
            states = [self._state(identifier) for identifier in identifiers]
            for state in states:
                if state.phase is not _Phase.ADMITTED:
                    raise InvalidScheduleTransitionError(
                        f"component {state.declaration.component_instance_id!r} is not admitted"
                    )
            operation_sequence = len(self._operations) + 1
            entries = []
            for batch_index, state in enumerate(states):
                entry = self._record(
                    operation_sequence=operation_sequence,
                    batch_index=batch_index,
                    component_instance_id=state.declaration.component_instance_id,
                    transition=ScheduleTransition.ADMISSION_ABANDONED,
                    timestamp=timestamp,
                    reason=reason,
                )
                entries.append(entry)
                state.phase = _Phase.TERMINAL
                state.terminal_transition = ScheduleTransition.ADMISSION_ABANDONED
            self._append_operation(
                kind=ScheduleOperationKind.ABANDON,
                timestamp=timestamp,
                component_instance_ids=identifiers,
                reason=reason,
                entries=entries,
            )
            return tuple(entries)

    def _discard_stale_heap_heads(self) -> None:
        while self._due_heap:
            boundary, _index, identifier = self._due_heap[0]
            state = self._states[identifier]
            if state.phase is _Phase.READY and state.scheduled_for == boundary:
                break
            heapq.heappop(self._due_heap)
            self._counter_values["heap_pops"] += 1
            self._counter_values["stale_heap_pops"] += 1
        while self._deadline_heap:
            boundary, _index, identifier = self._deadline_heap[0]
            state = self._states[identifier]
            if state.phase is _Phase.READY and state.deadline == boundary and state.matched_event is None:
                break
            heapq.heappop(self._deadline_heap)
            self._counter_values["heap_pops"] += 1
            self._counter_values["stale_heap_pops"] += 1

    @property
    def next_boundary(self) -> Optional[float]:
        """Earliest valid due or deadline boundary, without scanning states."""

        with self._lock:
            self._discard_stale_heap_heads()
            boundaries = []
            if self._due_heap:
                boundaries.append(self._due_heap[0][0])
            if self._deadline_heap:
                boundaries.append(self._deadline_heap[0][0])
            return min(boundaries) if boundaries else None

    @property
    def committed_events(self) -> tuple[CommittedEventRecord, ...]:
        with self._lock:
            return tuple(self._events)

    @property
    def nonterminal_component_ids(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(
                state.declaration.component_instance_id
                for state in self._states.values()
                if state.phase is not _Phase.TERMINAL
            )

    @property
    def admitted_component_ids(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(
                state.declaration.component_instance_id
                for state in self._states.values()
                if state.phase is _Phase.ADMITTED
            )

    @property
    def latest_operation(self) -> Optional[ScheduleOperation]:
        with self._lock:
            return None if not self._operations else self._operations[-1]

    @property
    def work_counters(self) -> SchedulerWorkCounters:
        with self._lock:
            return SchedulerWorkCounters(**self._counter_values)

    def snapshot(self) -> ScheduleJournalSnapshot:
        with self._lock:
            return ScheduleJournalSnapshot(
                activation_digest=self._activation_digest,
                operations=tuple(self._operations),
                entries=tuple(self._journal),
            )


def _operation_entries(
    operation: ScheduleOperation,
    entries_by_sequence: Mapping[int, ScheduleJournalEntry],
) -> tuple[ScheduleJournalEntry, ...]:
    return tuple(entries_by_sequence[sequence] for sequence in operation.entry_sequences)


def validate_schedule_replay(
    activations: Sequence[ScheduledActivation],
    journal: Union[ScheduleJournalSnapshot, Mapping[str, Any]],
    committed_events: Optional[Sequence[CommittedEvent]] = None,
    *,
    require_terminal: bool = True,
) -> None:
    """Re-execute an exact scheduler trace and compare every operation/output."""

    declarations = _normalise_activations(activations)
    if isinstance(journal, Mapping):
        try:
            journal = ScheduleJournalSnapshot.from_data(journal)
        except (TypeError, ValueError) as exc:
            raise ScheduleReplayError(f"invalid schedule journal: {exc}") from exc
    if not isinstance(journal, ScheduleJournalSnapshot):
        raise TypeError("journal must be a ScheduleJournalSnapshot or mapping")
    expected_activation_digest = canonical_digest(declarations)
    if journal.activation_digest != expected_activation_digest:
        raise ScheduleReplayError("journal activation digest does not match the replayed activation declarations")

    recorded_events = tuple(
        operation.committed_event for operation in journal.operations if operation.kind is ScheduleOperationKind.OBSERVE
    )
    if committed_events is not None:
        expected_events = _normalise_committed_events(committed_events)
        if recorded_events != expected_events:
            raise ScheduleReplayError("journal observe operations do not match committed event history")

    planner = DeterministicActivationPlanner(declarations)
    entries_by_sequence = {entry.sequence: entry for entry in journal.entries}
    for operation in journal.operations:
        try:
            if operation.kind is ScheduleOperationKind.OBSERVE:
                assert operation.committed_event is not None
                planner.observe_committed(
                    operation.committed_event,
                    timestamp=operation.timestamp,
                )
                emitted: tuple[ScheduleJournalEntry, ...] = ()
            elif operation.kind is ScheduleOperationKind.READY:
                emitted = planner.mark_ready_many(
                    operation.component_instance_ids,
                    timestamp=operation.timestamp,
                )
            elif operation.kind is ScheduleOperationKind.ADVANCE:
                emitted = planner.advance(operation.timestamp)
            elif operation.kind is ScheduleOperationKind.CANCEL:
                emitted = planner.cancel_pending(
                    timestamp=operation.timestamp,
                    component_instance_ids=None if operation.all_pending else operation.component_instance_ids,
                )
            elif operation.kind is ScheduleOperationKind.START:
                emitted = planner.mark_invocation_started_many(
                    operation.component_instance_ids,
                    timestamp=operation.timestamp,
                )
            else:
                emitted = planner.abandon_admissions(
                    operation.component_instance_ids,
                    timestamp=operation.timestamp,
                    reason=operation.reason,
                )
        except (KeyError, TypeError, ValueError) as exc:
            raise ScheduleReplayError(
                f"operation {operation.sequence} ({operation.kind.value}) cannot be replayed: {exc}"
            ) from exc

        generated_operation = planner.latest_operation
        if generated_operation != operation:
            raise ScheduleReplayError(
                f"operation {operation.sequence} ({operation.kind.value}) was not reproduced exactly"
            )
        expected_entries = _operation_entries(operation, entries_by_sequence)
        if emitted != expected_entries:
            raise ScheduleReplayError(
                f"operation {operation.sequence} ({operation.kind.value}) emitted different entries"
            )

    replayed = planner.snapshot()
    if replayed.operations != journal.operations or replayed.entries != journal.entries:
        raise ScheduleReplayError("replayed schedule trace differs from the persisted journal")
    if require_terminal and planner.nonterminal_component_ids:
        raise ScheduleReplayError(
            f"components have no terminal scheduler transition: {list(planner.nonterminal_component_ids)!r}"
        )


__all__ = [
    "SCHEDULE_JOURNAL_SCHEMA_ID",
    "SCHEDULE_JOURNAL_SCHEMA_VERSION",
    "ActivationMode",
    "ActivationSpec",
    "CommittedEvent",
    "CommittedEventRecord",
    "DeterministicActivationPlanner",
    "DuplicateActivationError",
    "EventScope",
    "EventSelector",
    "InvalidScheduleTransitionError",
    "ScheduleJournalEntry",
    "ScheduleJournalSnapshot",
    "ScheduleOperation",
    "ScheduleOperationKind",
    "ScheduleReplayError",
    "ScheduleTransition",
    "ScheduledActivation",
    "SchedulerWorkCounters",
    "SchedulingError",
    "TriggerKind",
    "TriggerSpec",
    "UnknownActivationError",
    "validate_schedule_replay",
]
