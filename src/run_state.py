"""Runner-owned operation-history and checkpoint primitives.

The types in this module are intentionally independent from topology and
command implementations.  A runner can record the intent and externally
visible outcome of an operation without retaining request or response payloads.
Feature authors may use the resulting history for recovery, consistency,
replay, or other policy-specific assertions; the core assigns no feature
semantics to it.

``OperationLedger`` is an append-only event log:

* beginning an attempt appends an :class:`OperationAttempt`;
* completing it appends an :class:`OperationRecord`;
* attempts without a completion remain visible as ``INCOMPLETE`` records; and
* an ``AMBIGUOUS`` completion is terminal and is never rewritten as success.

All public snapshots are immutable or detached from their caller-owned input.
No type in this module performs external I/O.
"""

import hashlib
import json
import math
import re
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, fields, is_dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any, Callable, Optional, Union

_DIGEST_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_UNSET = object()


class RunStateError(Exception):
    """Base class for run-state contract violations."""


class UnknownOperationAttemptError(KeyError, RunStateError):
    """Raised when a ledger does not own the supplied attempt."""


class OperationAlreadyCompletedError(RunStateError):
    """Raised when code tries to complete an attempt more than once."""


class InvalidOperationTransitionError(RunStateError):
    """Raised when a requested operation transition is not meaningful."""


def _require_text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{field_name} must be a non-empty string without surrounding whitespace")
    return value


def _require_nonnegative_int(value: int, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _require_positive_int(value: int, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _require_timestamp(value: float, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{field_name} must be a finite number")
    return float(value)


def _normalise_digest(value: Optional[str], field_name: str) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalised = value.lower()
    if not _DIGEST_PATTERN.fullmatch(normalised):
        raise ValueError(f"{field_name} must use the form 'sha256:<64 lowercase hexadecimal characters>'")
    return normalised


def _freeze(value: Any) -> Any:
    """Take an immutable snapshot of JSON-safe state."""

    if isinstance(value, Mapping):
        frozen = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("state mapping keys must be strings")
            frozen[key] = _freeze(item)
        return MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("state cannot contain NaN or infinite numbers")
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise TypeError(
        f"state must contain only JSON-safe scalar, mapping, and sequence values; got {type(value).__name__}"
    )


def _freeze_mapping(value: Mapping[str, Any], field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must be a mapping")
    return _freeze(value)


def _as_ordered_tuple(value: Sequence[Any], field_name: str) -> tuple[Any, ...]:
    if isinstance(value, (str, bytes, bytearray, Mapping)) or not isinstance(value, Sequence):
        raise TypeError(f"{field_name} must be an ordered sequence")
    return tuple(value)


def canonical_snapshot(value: Any) -> Any:
    """Return a detached, JSON-safe snapshot with deterministic key order.

    This helper is intended for report and state serialization.  Raw byte
    payloads are deliberately excluded; pass them to :func:`canonical_digest`
    when only their identity should be retained.
    """

    if isinstance(value, Enum):
        return canonical_snapshot(value.value)
    if is_dataclass(value) and not isinstance(value, type):
        return {item.name: canonical_snapshot(getattr(value, item.name)) for item in fields(value)}
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("state mapping keys must be strings")
        snapshot = {}
        for key in sorted(value):
            snapshot[key] = canonical_snapshot(value[key])
        return snapshot
    if isinstance(value, (list, tuple)):
        return [canonical_snapshot(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("state cannot contain NaN or infinite numbers")
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise TypeError(f"cannot create a canonical snapshot of {type(value).__name__}")


def canonical_json(value: Any) -> str:
    """Serialize state using a stable JSON representation."""

    return json.dumps(
        canonical_snapshot(value),
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def canonical_digest(value: Any) -> str:
    """Return a domain-separated SHA-256 digest for a payload or state value."""

    if isinstance(value, (bytes, bytearray, memoryview)):
        material = b"bytes\0" + bytes(value)
    else:
        material = b"json\0" + canonical_json(value).encode("utf-8")
    return f"sha256:{hashlib.sha256(material).hexdigest()}"


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


def _require_fields(
    value: Mapping[str, Any],
    *,
    required: set[str],
    optional: set[str],
    field_name: str,
) -> None:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must be a mapping")
    keys = set(value)
    missing = required - keys
    unknown = keys - required - optional
    if missing:
        raise ValueError(f"{field_name} is missing required fields: {sorted(missing)!r}")
    if unknown:
        raise ValueError(f"{field_name} contains unknown fields: {sorted(unknown)!r}")


class OperationCompletion(str, Enum):
    """The runner's final knowledge about one operation attempt."""

    ACKNOWLEDGED = "acknowledged"
    REJECTED = "rejected"
    AMBIGUOUS = "ambiguous"
    INCOMPLETE = "incomplete"
    ERROR = "error"


@dataclass(frozen=True)
class OperationAttempt:
    """Immutable intent recorded when an operation begins."""

    attempt_id: str
    operation_id: str
    sequence: int
    started_at: float
    lifecycle_epoch: int
    topology_epoch: int
    request_digest: Optional[str] = None
    logical_effect: Optional[Mapping[str, Any]] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_text(self.attempt_id, "attempt_id")
        _require_text(self.operation_id, "operation_id")
        _require_positive_int(self.sequence, "sequence")
        object.__setattr__(self, "started_at", _require_timestamp(self.started_at, "started_at"))
        _require_nonnegative_int(self.lifecycle_epoch, "lifecycle_epoch")
        _require_nonnegative_int(self.topology_epoch, "topology_epoch")
        object.__setattr__(
            self,
            "request_digest",
            _normalise_digest(self.request_digest, "request_digest"),
        )
        if self.logical_effect is not None:
            if not isinstance(self.logical_effect, Mapping):
                raise TypeError("logical_effect must be a mapping")
            if not self.logical_effect:
                raise ValueError("logical_effect must not be empty")
            object.__setattr__(self, "logical_effect", _freeze_mapping(self.logical_effect, "logical_effect"))
        object.__setattr__(self, "metadata", _freeze_mapping(self.metadata, "metadata"))
        if self.request_digest is None and self.logical_effect is None:
            raise ValueError("an operation attempt requires a request digest, a logical effect, or both")

    def to_data(self) -> Mapping[str, Any]:
        return canonical_snapshot(self)

    def to_json(self) -> str:
        return canonical_json(self)

    @classmethod
    def from_data(cls, value: Mapping[str, Any]) -> "OperationAttempt":
        _require_fields(
            value,
            required={
                "attempt_id",
                "operation_id",
                "sequence",
                "started_at",
                "lifecycle_epoch",
                "topology_epoch",
                "request_digest",
                "logical_effect",
                "metadata",
            },
            optional=set(),
            field_name="operation attempt",
        )
        return cls(
            attempt_id=value["attempt_id"],
            operation_id=value["operation_id"],
            sequence=value["sequence"],
            started_at=value["started_at"],
            lifecycle_epoch=value["lifecycle_epoch"],
            topology_epoch=value["topology_epoch"],
            request_digest=value["request_digest"],
            logical_effect=value["logical_effect"],
            metadata=value["metadata"],
        )

    @classmethod
    def from_json(cls, serialized: str) -> "OperationAttempt":
        return cls.from_data(_load_json_mapping(serialized, "operation attempt JSON"))


@dataclass(frozen=True)
class OperationRecord:
    """Immutable current result for an operation attempt."""

    attempt: OperationAttempt
    completion: OperationCompletion
    completion_sequence: Optional[int] = None
    completed_at: Optional[float] = None
    response_digest: Optional[str] = None
    lifecycle_epoch: Optional[int] = None
    topology_epoch: Optional[int] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.attempt, OperationAttempt):
            raise TypeError("attempt must be an OperationAttempt")
        if isinstance(self.completion, str):
            object.__setattr__(self, "completion", OperationCompletion(self.completion))
        if not isinstance(self.completion, OperationCompletion):
            raise TypeError("completion must be an OperationCompletion")
        object.__setattr__(
            self,
            "response_digest",
            _normalise_digest(self.response_digest, "response_digest"),
        )
        object.__setattr__(self, "metadata", _freeze_mapping(self.metadata, "metadata"))

        if self.completion is OperationCompletion.INCOMPLETE:
            if self.completion_sequence is not None:
                raise ValueError("an incomplete record cannot have a completion_sequence")
            if self.completed_at is not None:
                raise ValueError("an incomplete record cannot have completed_at")
            if self.response_digest is not None:
                raise ValueError("an incomplete record cannot have a response_digest")
            if self.lifecycle_epoch is not None or self.topology_epoch is not None:
                raise ValueError("an incomplete record cannot have completion epoch metadata")
            if self.metadata:
                raise ValueError("an incomplete record cannot have completion metadata")
            return

        if self.completion_sequence is None:
            raise ValueError("a terminal record requires a completion_sequence")
        _require_positive_int(self.completion_sequence, "completion_sequence")
        if self.completion_sequence <= self.attempt.sequence:
            raise ValueError("completion_sequence must be greater than the attempt sequence")
        if self.completed_at is None:
            raise ValueError("a terminal record requires completed_at")
        object.__setattr__(self, "completed_at", _require_timestamp(self.completed_at, "completed_at"))
        if self.completed_at < self.attempt.started_at:
            raise ValueError("completed_at cannot precede the attempt started_at")
        if self.lifecycle_epoch is None or self.topology_epoch is None:
            raise ValueError("a terminal record requires lifecycle_epoch and topology_epoch")
        _require_nonnegative_int(self.lifecycle_epoch, "lifecycle_epoch")
        _require_nonnegative_int(self.topology_epoch, "topology_epoch")
        if self.lifecycle_epoch < self.attempt.lifecycle_epoch:
            raise ValueError("completion lifecycle_epoch cannot precede the attempt lifecycle_epoch")
        if self.topology_epoch < self.attempt.topology_epoch:
            raise ValueError("completion topology_epoch cannot precede the attempt topology_epoch")

    @property
    def attempt_id(self) -> str:
        return self.attempt.attempt_id

    @property
    def operation_id(self) -> str:
        return self.attempt.operation_id

    @property
    def terminal(self) -> bool:
        return self.completion is not OperationCompletion.INCOMPLETE

    def to_data(self) -> Mapping[str, Any]:
        return canonical_snapshot(self)

    def to_json(self) -> str:
        return canonical_json(self)

    @classmethod
    def incomplete(cls, attempt: OperationAttempt) -> "OperationRecord":
        return cls(attempt=attempt, completion=OperationCompletion.INCOMPLETE)

    @classmethod
    def from_data(cls, value: Mapping[str, Any]) -> "OperationRecord":
        _require_fields(
            value,
            required={
                "attempt",
                "completion",
                "completion_sequence",
                "completed_at",
                "response_digest",
                "lifecycle_epoch",
                "topology_epoch",
                "metadata",
            },
            optional=set(),
            field_name="operation record",
        )
        return cls(
            attempt=OperationAttempt.from_data(value["attempt"]),
            completion=value["completion"],
            completion_sequence=value["completion_sequence"],
            completed_at=value["completed_at"],
            response_digest=value["response_digest"],
            lifecycle_epoch=value["lifecycle_epoch"],
            topology_epoch=value["topology_epoch"],
            metadata=value["metadata"],
        )

    @classmethod
    def from_json(cls, serialized: str) -> "OperationRecord":
        return cls.from_data(_load_json_mapping(serialized, "operation record JSON"))


LedgerEntry = Union[OperationAttempt, OperationRecord]


@dataclass(frozen=True)
class LedgerSnapshot:
    """Immutable, complete view of an operation ledger."""

    high_water_mark: int
    records: tuple[OperationRecord, ...] = ()

    def __post_init__(self) -> None:
        _require_nonnegative_int(self.high_water_mark, "high_water_mark")
        records = _as_ordered_tuple(self.records, "records")
        if any(not isinstance(record, OperationRecord) for record in records):
            raise TypeError("records must contain only OperationRecord values")
        object.__setattr__(self, "records", records)

        attempt_ids = [record.attempt_id for record in records]
        if len(set(attempt_ids)) != len(attempt_ids):
            raise ValueError("ledger snapshot contains duplicate attempt_id values")

        begin_sequences = [record.attempt.sequence for record in records]
        if begin_sequences != sorted(begin_sequences) or len(set(begin_sequences)) != len(begin_sequences):
            raise ValueError("ledger records must have unique, increasing attempt sequences")

        event_sequences = list(begin_sequences)
        event_sequences.extend(
            record.completion_sequence for record in records if record.completion_sequence is not None
        )
        expected_sequences = list(range(1, self.high_water_mark + 1))
        if sorted(event_sequences) != expected_sequences:
            raise ValueError("ledger event sequences must contain every value through high_water_mark exactly once")

    @property
    def pending(self) -> tuple[OperationAttempt, ...]:
        return tuple(record.attempt for record in self.records if record.completion is OperationCompletion.INCOMPLETE)

    @property
    def digest(self) -> str:
        return canonical_digest(self)

    def to_data(self) -> Mapping[str, Any]:
        return canonical_snapshot(self)

    def to_json(self) -> str:
        return canonical_json(self)

    @classmethod
    def from_data(cls, value: Mapping[str, Any]) -> "LedgerSnapshot":
        _require_fields(
            value,
            required={"high_water_mark", "records"},
            optional=set(),
            field_name="ledger snapshot",
        )
        records = _as_ordered_tuple(value["records"], "records")
        return cls(
            high_water_mark=value["high_water_mark"],
            records=tuple(OperationRecord.from_data(record) for record in records),
        )

    @classmethod
    def from_json(cls, serialized: str) -> "LedgerSnapshot":
        return cls.from_data(_load_json_mapping(serialized, "ledger snapshot JSON"))


class OperationLedger:
    """Thread-safe append-only operation history owned by one runner."""

    def __init__(self, *, clock: Optional[Callable[[], float]] = None) -> None:
        if clock is not None and not callable(clock):
            raise TypeError("clock must be callable")
        self._lock = threading.RLock()
        self._clock = time.monotonic if clock is None else clock
        self._sequence = 0
        self._attempts: dict[str, OperationAttempt] = {}
        self._completed: dict[str, OperationRecord] = {}
        self._entries: list[LedgerEntry] = []

    @property
    def high_water_mark(self) -> int:
        with self._lock:
            return self._sequence

    def __len__(self) -> int:
        with self._lock:
            return len(self._attempts)

    def begin(
        self,
        operation_id: str,
        *,
        attempt_id: Optional[str] = None,
        request: Any = _UNSET,
        request_digest: Optional[str] = None,
        logical_effect: Optional[Mapping[str, Any]] = None,
        lifecycle_epoch: int = 0,
        topology_epoch: int = 0,
        started_at: Optional[float] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> OperationAttempt:
        """Append an operation attempt and return its immutable completion token."""

        _require_text(operation_id, "operation_id")
        if attempt_id is not None:
            _require_text(attempt_id, "attempt_id")
        if request is not _UNSET and request_digest is not None:
            raise ValueError("provide request or request_digest, not both")
        resolved_request_digest = (
            canonical_digest(request) if request is not _UNSET else _normalise_digest(request_digest, "request_digest")
        )
        if logical_effect is not None:
            _freeze_mapping(logical_effect, "logical_effect")
        resolved_metadata = {} if metadata is None else metadata
        _freeze_mapping(resolved_metadata, "metadata")
        _require_nonnegative_int(lifecycle_epoch, "lifecycle_epoch")
        _require_nonnegative_int(topology_epoch, "topology_epoch")
        resolved_started_at = None if started_at is None else _require_timestamp(started_at, "started_at")
        if resolved_request_digest is None and logical_effect is None:
            raise ValueError("begin requires request, request_digest, a logical_effect, or a combination")

        with self._lock:
            next_sequence = self._sequence + 1
            if resolved_started_at is None:
                resolved_started_at = _require_timestamp(self._clock(), "clock result")
            resolved_attempt_id = attempt_id or f"attempt-{next_sequence:020d}"
            if resolved_attempt_id in self._attempts:
                raise ValueError(f"attempt_id {resolved_attempt_id!r} is already present in this ledger")
            attempt = OperationAttempt(
                attempt_id=resolved_attempt_id,
                operation_id=operation_id,
                sequence=next_sequence,
                started_at=resolved_started_at,
                lifecycle_epoch=lifecycle_epoch,
                topology_epoch=topology_epoch,
                request_digest=resolved_request_digest,
                logical_effect=logical_effect,
                metadata=resolved_metadata,
            )
            self._sequence = next_sequence
            self._attempts[attempt.attempt_id] = attempt
            self._entries.append(attempt)
            return attempt

    def complete(
        self,
        attempt: OperationAttempt,
        completion: OperationCompletion,
        *,
        response: Any = _UNSET,
        response_digest: Optional[str] = None,
        lifecycle_epoch: Optional[int] = None,
        topology_epoch: Optional[int] = None,
        completed_at: Optional[float] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> OperationRecord:
        """Append exactly one terminal result for an owned operation attempt."""

        if not isinstance(attempt, OperationAttempt):
            raise TypeError("attempt must be the OperationAttempt returned by begin")
        if isinstance(completion, str):
            try:
                completion = OperationCompletion(completion)
            except ValueError as error:
                raise ValueError(f"unknown operation completion {completion!r}") from error
        if not isinstance(completion, OperationCompletion):
            raise TypeError("completion must be an OperationCompletion")
        if completion is OperationCompletion.INCOMPLETE:
            raise InvalidOperationTransitionError(
                "INCOMPLETE is a ledger-derived state; leave the attempt open instead of completing it"
            )
        if response is not _UNSET and response_digest is not None:
            raise ValueError("provide response or response_digest, not both")
        resolved_response_digest = (
            canonical_digest(response)
            if response is not _UNSET
            else _normalise_digest(response_digest, "response_digest")
        )
        resolved_metadata = {} if metadata is None else metadata
        _freeze_mapping(resolved_metadata, "metadata")
        resolved_completed_at = None if completed_at is None else _require_timestamp(completed_at, "completed_at")

        with self._lock:
            owned_attempt = self._attempts.get(attempt.attempt_id)
            if owned_attempt is None:
                raise UnknownOperationAttemptError(f"attempt {attempt.attempt_id!r} does not belong to this ledger")
            if owned_attempt is not attempt:
                raise UnknownOperationAttemptError(
                    f"attempt {attempt.attempt_id!r} does not match the attempt owned by this ledger"
                )
            if attempt.attempt_id in self._completed:
                raise OperationAlreadyCompletedError(f"attempt {attempt.attempt_id!r} is already complete")

            resolved_lifecycle_epoch = attempt.lifecycle_epoch if lifecycle_epoch is None else lifecycle_epoch
            resolved_topology_epoch = attempt.topology_epoch if topology_epoch is None else topology_epoch
            if resolved_completed_at is None:
                resolved_completed_at = _require_timestamp(self._clock(), "clock result")
            next_sequence = self._sequence + 1
            record = OperationRecord(
                attempt=attempt,
                completion=completion,
                completion_sequence=next_sequence,
                completed_at=resolved_completed_at,
                response_digest=resolved_response_digest,
                lifecycle_epoch=resolved_lifecycle_epoch,
                topology_epoch=resolved_topology_epoch,
                metadata=resolved_metadata,
            )
            self._sequence = next_sequence
            self._completed[attempt.attempt_id] = record
            self._entries.append(record)
            return record

    def entries(self) -> tuple[LedgerEntry, ...]:
        """Return all begin and terminal events in append order."""

        with self._lock:
            return tuple(self._entries)

    def attempts(self) -> tuple[OperationAttempt, ...]:
        with self._lock:
            return tuple(self._attempts.values())

    def pending_attempts(self) -> tuple[OperationAttempt, ...]:
        with self._lock:
            return tuple(attempt for attempt_id, attempt in self._attempts.items() if attempt_id not in self._completed)

    def records(self) -> tuple[OperationRecord, ...]:
        """Return one terminal-or-incomplete record for every attempt."""

        with self._lock:
            return tuple(
                self._completed.get(attempt.attempt_id, OperationRecord.incomplete(attempt))
                for attempt in self._attempts.values()
            )

    def record_for(self, attempt: OperationAttempt) -> OperationRecord:
        if not isinstance(attempt, OperationAttempt):
            raise TypeError("attempt must be an OperationAttempt")
        with self._lock:
            owned_attempt = self._attempts.get(attempt.attempt_id)
            if owned_attempt is None or owned_attempt is not attempt:
                raise UnknownOperationAttemptError(f"attempt {attempt.attempt_id!r} does not belong to this ledger")
            return self._completed.get(attempt.attempt_id, OperationRecord.incomplete(attempt))

    def snapshot(self) -> LedgerSnapshot:
        with self._lock:
            return LedgerSnapshot(
                high_water_mark=self._sequence,
                records=tuple(
                    self._completed.get(attempt.attempt_id, OperationRecord.incomplete(attempt))
                    for attempt in self._attempts.values()
                ),
            )

    def to_data(self) -> Mapping[str, Any]:
        return self.snapshot().to_data()

    def to_json(self) -> str:
        return self.snapshot().to_json()

    @classmethod
    def from_snapshot(
        cls,
        snapshot: Union[LedgerSnapshot, Mapping[str, Any]],
        *,
        clock: Optional[Callable[[], float]] = None,
    ) -> "OperationLedger":
        if isinstance(snapshot, Mapping):
            snapshot = LedgerSnapshot.from_data(snapshot)
        if not isinstance(snapshot, LedgerSnapshot):
            raise TypeError("snapshot must be a LedgerSnapshot or mapping")

        ledger = cls(clock=clock)
        with ledger._lock:
            ledger._sequence = snapshot.high_water_mark
            for record in snapshot.records:
                attempt = record.attempt
                ledger._attempts[attempt.attempt_id] = attempt
                ledger._entries.append(attempt)
                if record.terminal:
                    ledger._completed[attempt.attempt_id] = record
                    ledger._entries.append(record)
            ledger._entries.sort(
                key=lambda entry: entry.sequence if isinstance(entry, OperationAttempt) else entry.completion_sequence
            )
        return ledger

    @classmethod
    def from_json(
        cls,
        serialized: str,
        *,
        clock: Optional[Callable[[], float]] = None,
    ) -> "OperationLedger":
        return cls.from_snapshot(LedgerSnapshot.from_json(serialized), clock=clock)


@dataclass(frozen=True)
class Checkpoint:
    """Immutable boundary between operation, event, and topology history."""

    checkpoint_id: str
    sequence: int
    ledger_high_water_mark: int
    event_high_water_mark: int
    ledger_digest: str
    lifecycle_epoch: int
    topology_epoch: int
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_text(self.checkpoint_id, "checkpoint_id")
        _require_positive_int(self.sequence, "sequence")
        _require_nonnegative_int(self.ledger_high_water_mark, "ledger_high_water_mark")
        _require_nonnegative_int(self.event_high_water_mark, "event_high_water_mark")
        ledger_digest = _normalise_digest(self.ledger_digest, "ledger_digest")
        if ledger_digest is None:
            raise ValueError("ledger_digest is required")
        object.__setattr__(self, "ledger_digest", ledger_digest)
        _require_nonnegative_int(self.lifecycle_epoch, "lifecycle_epoch")
        _require_nonnegative_int(self.topology_epoch, "topology_epoch")
        object.__setattr__(self, "metadata", _freeze_mapping(self.metadata, "metadata"))

    def to_data(self) -> Mapping[str, Any]:
        return canonical_snapshot(self)

    def to_json(self) -> str:
        return canonical_json(self)

    @classmethod
    def from_data(cls, value: Mapping[str, Any]) -> "Checkpoint":
        _require_fields(
            value,
            required={
                "checkpoint_id",
                "sequence",
                "ledger_high_water_mark",
                "event_high_water_mark",
                "ledger_digest",
                "lifecycle_epoch",
                "topology_epoch",
                "metadata",
            },
            optional=set(),
            field_name="checkpoint",
        )
        return cls(
            checkpoint_id=value["checkpoint_id"],
            sequence=value["sequence"],
            ledger_high_water_mark=value["ledger_high_water_mark"],
            event_high_water_mark=value["event_high_water_mark"],
            ledger_digest=value["ledger_digest"],
            lifecycle_epoch=value["lifecycle_epoch"],
            topology_epoch=value["topology_epoch"],
            metadata=value["metadata"],
        )

    @classmethod
    def from_json(cls, serialized: str) -> "Checkpoint":
        return cls.from_data(_load_json_mapping(serialized, "checkpoint JSON"))


@dataclass(frozen=True)
class CheckpointSnapshot:
    """Immutable complete view of a checkpoint store."""

    high_water_mark: int
    checkpoints: tuple[Checkpoint, ...] = ()

    def __post_init__(self) -> None:
        _require_nonnegative_int(self.high_water_mark, "high_water_mark")
        checkpoints = _as_ordered_tuple(self.checkpoints, "checkpoints")
        if any(not isinstance(checkpoint, Checkpoint) for checkpoint in checkpoints):
            raise TypeError("checkpoints must contain only Checkpoint values")
        object.__setattr__(self, "checkpoints", checkpoints)

        sequences = [checkpoint.sequence for checkpoint in checkpoints]
        if sequences != list(range(1, self.high_water_mark + 1)):
            raise ValueError("checkpoint sequences must contain every value through high_water_mark in order")
        checkpoint_ids = [checkpoint.checkpoint_id for checkpoint in checkpoints]
        if len(set(checkpoint_ids)) != len(checkpoint_ids):
            raise ValueError("checkpoint snapshot contains duplicate checkpoint_id values")
        for previous, current in zip(checkpoints, checkpoints[1:]):
            if current.ledger_high_water_mark < previous.ledger_high_water_mark:
                raise ValueError("checkpoint ledger high-water marks cannot regress")
            if current.event_high_water_mark < previous.event_high_water_mark:
                raise ValueError("checkpoint event high-water marks cannot regress")
            if current.lifecycle_epoch < previous.lifecycle_epoch:
                raise ValueError("checkpoint lifecycle epochs cannot regress")
            if current.topology_epoch < previous.topology_epoch:
                raise ValueError("checkpoint topology epochs cannot regress")

    @property
    def latest(self) -> Optional[Checkpoint]:
        return self.checkpoints[-1] if self.checkpoints else None

    @property
    def digest(self) -> str:
        return canonical_digest(self)

    def to_data(self) -> Mapping[str, Any]:
        return canonical_snapshot(self)

    def to_json(self) -> str:
        return canonical_json(self)

    @classmethod
    def from_data(cls, value: Mapping[str, Any]) -> "CheckpointSnapshot":
        _require_fields(
            value,
            required={"high_water_mark", "checkpoints"},
            optional=set(),
            field_name="checkpoint snapshot",
        )
        checkpoints = _as_ordered_tuple(value["checkpoints"], "checkpoints")
        return cls(
            high_water_mark=value["high_water_mark"],
            checkpoints=tuple(Checkpoint.from_data(item) for item in checkpoints),
        )

    @classmethod
    def from_json(cls, serialized: str) -> "CheckpointSnapshot":
        return cls.from_data(_load_json_mapping(serialized, "checkpoint snapshot JSON"))


class CheckpointStore:
    """Thread-safe append-only checkpoint history for one runner."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._sequence = 0
        self._checkpoints: list[Checkpoint] = []

    @property
    def high_water_mark(self) -> int:
        with self._lock:
            return self._sequence

    def __len__(self) -> int:
        with self._lock:
            return len(self._checkpoints)

    @property
    def latest(self) -> Optional[Checkpoint]:
        with self._lock:
            return self._checkpoints[-1] if self._checkpoints else None

    def capture(
        self,
        ledger: OperationLedger,
        *,
        event_high_water_mark: int,
        lifecycle_epoch: int = 0,
        topology_epoch: int = 0,
        metadata: Optional[Mapping[str, Any]] = None,
        checkpoint_id: Optional[str] = None,
    ) -> Checkpoint:
        """Atomically snapshot the ledger, then append its checkpoint boundary."""

        if not isinstance(ledger, OperationLedger):
            raise TypeError("ledger must be an OperationLedger")
        _require_nonnegative_int(event_high_water_mark, "event_high_water_mark")
        _require_nonnegative_int(lifecycle_epoch, "lifecycle_epoch")
        _require_nonnegative_int(topology_epoch, "topology_epoch")
        if checkpoint_id is not None:
            _require_text(checkpoint_id, "checkpoint_id")
        resolved_metadata = {} if metadata is None else metadata
        _freeze_mapping(resolved_metadata, "metadata")
        ledger_snapshot = ledger.snapshot()

        with self._lock:
            if self._checkpoints:
                previous = self._checkpoints[-1]
                if ledger_snapshot.high_water_mark < previous.ledger_high_water_mark:
                    raise ValueError("ledger high-water mark cannot regress")
                if event_high_water_mark < previous.event_high_water_mark:
                    raise ValueError("event high-water mark cannot regress")
                if lifecycle_epoch < previous.lifecycle_epoch:
                    raise ValueError("lifecycle_epoch cannot regress")
                if topology_epoch < previous.topology_epoch:
                    raise ValueError("topology_epoch cannot regress")

            next_sequence = self._sequence + 1
            resolved_checkpoint_id = checkpoint_id or f"checkpoint-{next_sequence:020d}"
            if any(item.checkpoint_id == resolved_checkpoint_id for item in self._checkpoints):
                raise ValueError(f"checkpoint_id {resolved_checkpoint_id!r} is already present in this store")
            checkpoint = Checkpoint(
                checkpoint_id=resolved_checkpoint_id,
                sequence=next_sequence,
                ledger_high_water_mark=ledger_snapshot.high_water_mark,
                event_high_water_mark=event_high_water_mark,
                ledger_digest=ledger_snapshot.digest,
                lifecycle_epoch=lifecycle_epoch,
                topology_epoch=topology_epoch,
                metadata=resolved_metadata,
            )
            self._sequence = next_sequence
            self._checkpoints.append(checkpoint)
            return checkpoint

    def checkpoints(self) -> tuple[Checkpoint, ...]:
        with self._lock:
            return tuple(self._checkpoints)

    def snapshot(self) -> CheckpointSnapshot:
        with self._lock:
            return CheckpointSnapshot(
                high_water_mark=self._sequence,
                checkpoints=tuple(self._checkpoints),
            )

    def to_data(self) -> Mapping[str, Any]:
        return self.snapshot().to_data()

    def to_json(self) -> str:
        return self.snapshot().to_json()

    @classmethod
    def from_snapshot(
        cls,
        snapshot: Union[CheckpointSnapshot, Mapping[str, Any]],
    ) -> "CheckpointStore":
        if isinstance(snapshot, Mapping):
            snapshot = CheckpointSnapshot.from_data(snapshot)
        if not isinstance(snapshot, CheckpointSnapshot):
            raise TypeError("snapshot must be a CheckpointSnapshot or mapping")
        store = cls()
        with store._lock:
            store._sequence = snapshot.high_water_mark
            store._checkpoints.extend(snapshot.checkpoints)
        return store

    @classmethod
    def from_json(cls, serialized: str) -> "CheckpointStore":
        return cls.from_snapshot(CheckpointSnapshot.from_json(serialized))


__all__ = [
    "Checkpoint",
    "CheckpointSnapshot",
    "CheckpointStore",
    "InvalidOperationTransitionError",
    "LedgerEntry",
    "LedgerSnapshot",
    "OperationAlreadyCompletedError",
    "OperationAttempt",
    "OperationCompletion",
    "OperationLedger",
    "OperationRecord",
    "RunStateError",
    "UnknownOperationAttemptError",
    "canonical_digest",
    "canonical_json",
    "canonical_snapshot",
]
