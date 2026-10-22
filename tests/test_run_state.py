"""Tests for generic runner-owned operation history and checkpoints."""

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError, replace
from itertools import count

import pytest

from src.run_state import (
    CheckpointSnapshot,
    CheckpointStore,
    InvalidOperationTransitionError,
    LedgerSnapshot,
    OperationAlreadyCompletedError,
    OperationAttempt,
    OperationCompletion,
    OperationLedger,
    OperationRecord,
    UnknownOperationAttemptError,
    canonical_digest,
    canonical_json,
    canonical_snapshot,
)


def test_canonical_helpers_are_stable_detached_and_domain_separated():
    source = {"z": [2, {"b": True, "a": None}], "a": 1}
    snapshot = canonical_snapshot(source)

    source["z"][1]["a"] = "changed"

    assert list(snapshot) == ["a", "z"]
    assert snapshot["z"][1]["a"] is None
    assert canonical_json({"z": 2, "a": 1}) == '{"a":1,"z":2}'
    assert canonical_digest({"a": 1, "z": 2}) == canonical_digest({"z": 2, "a": 1})
    assert canonical_digest(b"value") == canonical_digest(bytearray(b"value"))
    assert canonical_digest(b"value") != canonical_digest("value")


@pytest.mark.parametrize("value", (float("nan"), float("inf"), {"bad": object()}))
def test_canonical_helpers_reject_non_portable_state(value):
    with pytest.raises((TypeError, ValueError)):
        canonical_json(value)


def test_canonical_helpers_reject_non_string_mapping_keys_clearly():
    with pytest.raises(TypeError, match="mapping keys must be strings"):
        canonical_json({"valid": 1, 2: "invalid"})


def test_operation_attempt_is_immutable_and_round_trips():
    effect = {"kind": "logical-write", "subject": {"ids": [1, 2]}}
    metadata = {"worker": {"id": 3}}
    attempt = OperationAttempt(
        attempt_id="attempt-1",
        operation_id="operation-1",
        sequence=1,
        started_at=12.5,
        lifecycle_epoch=2,
        topology_epoch=4,
        request_digest=canonical_digest({"request": 1}),
        logical_effect=effect,
        metadata=metadata,
    )

    effect["subject"]["ids"].append(3)
    metadata["worker"]["id"] = 9

    assert attempt.logical_effect["subject"]["ids"] == (1, 2)
    assert attempt.metadata["worker"]["id"] == 3
    assert OperationAttempt.from_data(attempt.to_data()) == attempt
    assert OperationAttempt.from_json(attempt.to_json()) == attempt
    with pytest.raises(TypeError):
        attempt.metadata["new"] = True
    with pytest.raises(FrozenInstanceError):
        attempt.sequence = 9


def test_operation_attempt_validates_identity_effect_digest_and_epochs():
    digest = canonical_digest("request")
    with pytest.raises(ValueError, match="non-empty string"):
        OperationAttempt("", "operation", 1, 1.0, 0, 0, request_digest=digest)
    with pytest.raises(ValueError, match="positive integer"):
        OperationAttempt("attempt", "operation", 0, 1.0, 0, 0, request_digest=digest)
    with pytest.raises(ValueError, match="non-negative integer"):
        OperationAttempt("attempt", "operation", 1, 1.0, -1, 0, request_digest=digest)
    with pytest.raises(ValueError, match="requires a request digest"):
        OperationAttempt("attempt", "operation", 1, 1.0, 0, 0)
    with pytest.raises(ValueError, match="must not be empty"):
        OperationAttempt("attempt", "operation", 1, 1.0, 0, 0, logical_effect={})
    with pytest.raises(ValueError, match="sha256"):
        OperationAttempt("attempt", "operation", 1, 1.0, 0, 0, request_digest="not-a-digest")


def test_attempt_timestamp_must_be_finite():
    digest = canonical_digest("request")
    with pytest.raises(ValueError, match="started_at must be a finite number"):
        OperationAttempt("attempt", "operation", 1, float("nan"), 0, 0, request_digest=digest)


def test_ledger_preserves_incomplete_ambiguous_and_acknowledged_attempts():
    timestamps = iter((10.0, 11.0, 12.0, 13.0, 14.0))
    ledger = OperationLedger(clock=lambda: next(timestamps))
    pending = ledger.begin(
        "pending-operation",
        request={"payload": 1},
        lifecycle_epoch=1,
        topology_epoch=2,
    )
    ambiguous = ledger.begin(
        "ambiguous-operation",
        logical_effect={"kind": "mutation", "subject": "opaque-id"},
        lifecycle_epoch=1,
        topology_epoch=2,
    )
    acknowledged = ledger.begin("acknowledged-operation", request=b"request")

    ambiguous_record = ledger.complete(
        ambiguous,
        OperationCompletion.AMBIGUOUS,
        response=b"partial-response",
        topology_epoch=3,
        metadata={"reason": "transport-ended"},
    )
    acknowledged_record = ledger.complete(
        acknowledged,
        "acknowledged",
        response={"accepted": True},
    )

    assert [
        entry.sequence if isinstance(entry, OperationAttempt) else entry.completion_sequence
        for entry in ledger.entries()
    ] == [
        1,
        2,
        3,
        4,
        5,
    ]
    assert ledger.high_water_mark == 5
    assert ledger.pending_attempts() == (pending,)
    assert pending.started_at == 10.0
    assert ledger.record_for(pending).completion is OperationCompletion.INCOMPLETE
    assert ledger.record_for(ambiguous) is ambiguous_record
    assert ambiguous_record.completion is OperationCompletion.AMBIGUOUS
    assert ambiguous_record.completed_at == 13.0
    assert ambiguous_record.topology_epoch == 3
    assert acknowledged_record.completion is OperationCompletion.ACKNOWLEDGED
    assert [record.completion for record in ledger.records()] == [
        OperationCompletion.INCOMPLETE,
        OperationCompletion.AMBIGUOUS,
        OperationCompletion.ACKNOWLEDGED,
    ]


def test_completion_misuse_fails_without_mutating_the_ledger():
    ledger = OperationLedger()
    attempt = ledger.begin("operation", request=None)
    other_ledger = OperationLedger()
    foreign = other_ledger.begin("operation", request=None)

    with pytest.raises(UnknownOperationAttemptError, match="owned by this ledger"):
        ledger.complete(foreign, OperationCompletion.ERROR)
    with pytest.raises(UnknownOperationAttemptError, match="owned by this ledger"):
        ledger.complete(
            replace(attempt, operation_id="tampered"),
            OperationCompletion.ERROR,
        )
    with pytest.raises(InvalidOperationTransitionError, match="ledger-derived"):
        ledger.complete(attempt, OperationCompletion.INCOMPLETE)
    with pytest.raises(ValueError, match="request or request_digest"):
        ledger.begin(
            "invalid",
            request={"value": 1},
            request_digest=canonical_digest({"value": 1}),
        )

    assert ledger.high_water_mark == 1
    completed = ledger.complete(attempt, OperationCompletion.REJECTED)
    assert completed.completion_sequence == 2
    with pytest.raises(OperationAlreadyCompletedError, match="already complete"):
        ledger.complete(attempt, OperationCompletion.ACKNOWLEDGED)
    assert ledger.high_water_mark == 2


def test_completion_epochs_cannot_move_backwards():
    ledger = OperationLedger()
    attempt = ledger.begin(
        "operation",
        request="payload",
        lifecycle_epoch=3,
        topology_epoch=4,
    )

    with pytest.raises(ValueError, match="cannot precede"):
        ledger.complete(
            attempt,
            OperationCompletion.ERROR,
            lifecycle_epoch=2,
            topology_epoch=4,
        )

    assert ledger.high_water_mark == 1
    assert ledger.record_for(attempt).completion is OperationCompletion.INCOMPLETE


def test_ledger_supports_explicit_timestamps_without_consuming_its_clock():
    def unexpected_clock_call():
        raise AssertionError("explicit timestamps must not call the clock")

    ledger = OperationLedger(clock=unexpected_clock_call)
    attempt = ledger.begin(
        "operation",
        request="payload",
        started_at=20,
    )
    record = ledger.complete(
        attempt,
        OperationCompletion.ACKNOWLEDGED,
        completed_at=21.5,
    )

    assert attempt.started_at == 20.0
    assert record.completed_at == 21.5


def test_timestamp_failures_do_not_append_ledger_events():
    invalid_clock_ledger = OperationLedger(clock=lambda: float("inf"))
    with pytest.raises(ValueError, match="clock result must be a finite number"):
        invalid_clock_ledger.begin("operation", request="payload")
    assert invalid_clock_ledger.high_water_mark == 0

    ledger = OperationLedger(clock=lambda: 9.0)
    attempt = ledger.begin("operation", request="payload", started_at=10.0)
    with pytest.raises(ValueError, match="completed_at cannot precede"):
        ledger.complete(attempt, OperationCompletion.ERROR)
    with pytest.raises(ValueError, match="completed_at must be a finite number"):
        ledger.complete(
            attempt,
            OperationCompletion.ERROR,
            completed_at=float("nan"),
        )

    assert ledger.high_water_mark == 1
    assert ledger.record_for(attempt).completion is OperationCompletion.INCOMPLETE


def test_concurrent_begins_allocate_one_contiguous_sequence():
    timestamp_counter = count(1)
    ledger = OperationLedger(clock=lambda: float(next(timestamp_counter)))

    def begin(index):
        return ledger.begin(
            f"operation-{index}",
            request={"index": index},
            metadata={"worker": index % 4},
        )

    with ThreadPoolExecutor(max_workers=12) as executor:
        attempts = list(executor.map(begin, range(200)))

    sequences = sorted(attempt.sequence for attempt in attempts)
    assert sequences == list(range(1, 201))
    assert sorted(attempt.started_at for attempt in attempts) == [float(value) for value in range(1, 201)]
    assert len({attempt.attempt_id for attempt in attempts}) == 200
    assert ledger.high_water_mark == 200
    assert len(ledger.entries()) == 200
    assert len(ledger.pending_attempts()) == 200


def test_concurrent_completions_remain_append_only_and_serializable():
    timestamp_counter = count(1)
    ledger = OperationLedger(clock=lambda: float(next(timestamp_counter)))
    attempts = [ledger.begin(f"operation-{index}", logical_effect={"index": index}) for index in range(100)]

    def complete(attempt):
        return ledger.complete(
            attempt,
            OperationCompletion.ACKNOWLEDGED,
            response={"attempt_id": attempt.attempt_id},
        )

    with ThreadPoolExecutor(max_workers=12) as executor:
        records = list(executor.map(complete, attempts))

    assert sorted(record.completion_sequence for record in records) == list(range(101, 201))
    assert sorted(record.completed_at for record in records) == [float(value) for value in range(101, 201)]
    assert ledger.high_water_mark == 200
    assert len(ledger.entries()) == 200
    assert not ledger.pending_attempts()
    assert LedgerSnapshot.from_json(ledger.to_json()) == ledger.snapshot()


def test_ledger_snapshot_round_trip_restores_pending_state_and_sequence():
    source_timestamps = iter((1.0, 2.0, 3.0))
    ledger = OperationLedger(clock=lambda: next(source_timestamps))
    first = ledger.begin("first", request={"value": 1})
    second = ledger.begin("second", logical_effect={"kind": "remove"})
    ledger.complete(first, OperationCompletion.AMBIGUOUS)
    original_snapshot = ledger.snapshot()

    restored_timestamps = iter((4.0, 5.0))
    restored = OperationLedger.from_json(
        original_snapshot.to_json(),
        clock=lambda: next(restored_timestamps),
    )
    restored_second = restored.pending_attempts()[0]
    completion = restored.complete(restored_second, OperationCompletion.REJECTED)
    third = restored.begin("third", request=b"payload")

    assert original_snapshot.pending == (second,)
    assert restored_second == second
    assert completion.completion_sequence == 4
    assert completion.completed_at == 4.0
    assert third.sequence == 5
    assert third.started_at == 5.0
    assert restored.high_water_mark == 5
    assert original_snapshot.high_water_mark == 3
    assert original_snapshot.digest == LedgerSnapshot.from_data(original_snapshot.to_data()).digest


def test_ledger_snapshot_rejects_gaps_duplicates_and_unknown_fields():
    ledger = OperationLedger()
    ledger.begin("operation", request=1)
    data = ledger.to_data()

    with pytest.raises(ValueError, match="every value"):
        LedgerSnapshot(
            high_water_mark=2,
            records=ledger.records(),
        )

    duplicate = list(ledger.records()) * 2
    with pytest.raises(ValueError, match="duplicate attempt_id"):
        LedgerSnapshot(high_water_mark=2, records=tuple(duplicate))

    unknown = dict(data)
    unknown["api_version"] = "legacy"
    with pytest.raises(ValueError, match="unknown fields"):
        LedgerSnapshot.from_data(unknown)

    with pytest.raises(ValueError, match="duplicate object key"):
        LedgerSnapshot.from_json('{"high_water_mark":0,"records":[],"records":[]}')


def test_operation_record_serialization_and_terminal_invariants():
    ledger = OperationLedger()
    attempt = ledger.begin("operation", request={"value": 1})
    record = ledger.complete(
        attempt,
        OperationCompletion.ERROR,
        response_digest=canonical_digest({"error": "worker"}),
        metadata={"category": "runner"},
    )

    assert record.terminal
    assert OperationRecord.from_data(record.to_data()) == record
    assert OperationRecord.from_json(record.to_json()) == record
    assert not OperationRecord.incomplete(attempt).terminal

    with pytest.raises(ValueError, match="requires a completion_sequence"):
        OperationRecord(attempt, OperationCompletion.ERROR)
    with pytest.raises(ValueError, match="requires completed_at"):
        OperationRecord(
            attempt,
            OperationCompletion.ERROR,
            completion_sequence=2,
        )
    with pytest.raises(ValueError, match="cannot precede"):
        OperationRecord(
            attempt,
            OperationCompletion.ERROR,
            completion_sequence=2,
            completed_at=attempt.started_at - 1,
            lifecycle_epoch=attempt.lifecycle_epoch,
            topology_epoch=attempt.topology_epoch,
        )
    with pytest.raises(ValueError, match="cannot have a response_digest"):
        OperationRecord(
            attempt,
            OperationCompletion.INCOMPLETE,
            response_digest=canonical_digest("response"),
        )
    with pytest.raises(ValueError, match="cannot have completed_at"):
        OperationRecord(
            attempt,
            OperationCompletion.INCOMPLETE,
            completed_at=attempt.started_at,
        )


def test_checkpoint_captures_ledger_boundary_digest_epochs_and_metadata():
    ledger = OperationLedger()
    ledger.begin("operation", request={"value": 1})
    store = CheckpointStore()
    metadata = {"phase": {"name": "before-fault"}}

    checkpoint = store.capture(
        ledger,
        event_high_water_mark=7,
        lifecycle_epoch=2,
        topology_epoch=3,
        metadata=metadata,
    )
    metadata["phase"]["name"] = "changed"

    assert checkpoint.sequence == 1
    assert checkpoint.checkpoint_id == "checkpoint-00000000000000000001"
    assert checkpoint.ledger_high_water_mark == 1
    assert checkpoint.event_high_water_mark == 7
    assert checkpoint.ledger_digest == ledger.snapshot().digest
    assert checkpoint.lifecycle_epoch == 2
    assert checkpoint.topology_epoch == 3
    assert checkpoint.metadata["phase"]["name"] == "before-fault"
    assert store.latest is checkpoint


def test_checkpoint_store_rejects_regressions_and_duplicate_ids():
    ledger = OperationLedger()
    ledger.begin("first", request=1)
    store = CheckpointStore()
    store.capture(
        ledger,
        checkpoint_id="before",
        event_high_water_mark=3,
        lifecycle_epoch=2,
        topology_epoch=2,
    )
    ledger.begin("second", request=2)

    with pytest.raises(ValueError, match="event high-water mark cannot regress"):
        store.capture(
            ledger,
            event_high_water_mark=2,
            lifecycle_epoch=2,
            topology_epoch=2,
        )
    with pytest.raises(ValueError, match="lifecycle_epoch cannot regress"):
        store.capture(
            ledger,
            event_high_water_mark=3,
            lifecycle_epoch=1,
            topology_epoch=2,
        )
    with pytest.raises(ValueError, match="already present"):
        store.capture(
            ledger,
            checkpoint_id="before",
            event_high_water_mark=3,
            lifecycle_epoch=2,
            topology_epoch=2,
        )

    assert store.high_water_mark == 1


def test_checkpoint_requires_a_ledger_digest():
    checkpoint = CheckpointStore().capture(
        OperationLedger(),
        event_high_water_mark=0,
    )
    data = dict(checkpoint.to_data())
    data["ledger_digest"] = None

    with pytest.raises(ValueError, match="ledger_digest is required"):
        type(checkpoint).from_data(data)


def test_checkpoint_store_round_trip_continues_sequence():
    ledger = OperationLedger()
    ledger.begin("first", request=1)
    store = CheckpointStore()
    first = store.capture(ledger, event_high_water_mark=1)

    restored = CheckpointStore.from_json(store.to_json())
    ledger.begin("second", request=2)
    second = restored.capture(
        ledger,
        event_high_water_mark=2,
        lifecycle_epoch=1,
        topology_epoch=1,
    )

    assert restored.checkpoints() == (first, second)
    assert second.sequence == 2
    assert restored.snapshot().latest is second
    assert CheckpointSnapshot.from_data(restored.to_data()) == restored.snapshot()
    assert restored.snapshot().digest == CheckpointSnapshot.from_json(restored.to_json()).digest


def test_concurrent_checkpoint_capture_allocates_contiguous_sequences():
    ledger = OperationLedger()
    ledger.begin("operation", request="payload")
    store = CheckpointStore()

    def capture(index):
        return store.capture(
            ledger,
            event_high_water_mark=10,
            metadata={"worker": index},
        )

    with ThreadPoolExecutor(max_workers=12) as executor:
        checkpoints = list(executor.map(capture, range(100)))

    assert sorted(item.sequence for item in checkpoints) == list(range(1, 101))
    assert len({item.checkpoint_id for item in checkpoints}) == 100
    assert store.high_water_mark == 100
    assert len(store.checkpoints()) == 100


def test_snapshot_collection_fields_reject_unordered_inputs():
    with pytest.raises(TypeError, match="ordered sequence"):
        LedgerSnapshot(high_water_mark=0, records=set())
    with pytest.raises(TypeError, match="ordered sequence"):
        CheckpointSnapshot(high_water_mark=0, checkpoints={})


def test_json_serialization_is_plain_and_stable():
    ledger = OperationLedger()
    attempt = ledger.begin(
        "operation",
        request={"z": 2, "a": 1},
        logical_effect={"kind": "mutation", "ids": [2, 1]},
    )
    ledger.complete(attempt, OperationCompletion.REJECTED, response=None)
    serialized = ledger.to_json()

    assert json.loads(serialized) == ledger.to_data()
    assert serialized == OperationLedger.from_json(serialized).to_json()
