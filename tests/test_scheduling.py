"""Tests for the generic deterministic activation scheduler."""

import json
import random
from dataclasses import FrozenInstanceError, dataclass, replace

import pytest

from valkey_fuzzer.extensibility import CommittedLifecycleEvent, LifecycleEvent
from valkey_fuzzer.scheduling import (
    SCHEDULE_JOURNAL_SCHEMA_ID,
    SCHEDULE_JOURNAL_SCHEMA_VERSION,
    ActivationMode,
    ActivationSpec,
    CommittedEvent,
    CommittedEventRecord,
    DeterministicActivationPlanner,
    DuplicateActivationError,
    EventScope,
    EventSelector,
    InvalidScheduleTransitionError,
    ScheduledActivation,
    ScheduleJournalSnapshot,
    ScheduleOperationKind,
    ScheduleReplayError,
    ScheduleTransition,
    TriggerKind,
    TriggerSpec,
    UnknownActivationError,
    validate_schedule_replay,
)


@dataclass(frozen=True)
class _Committed:
    name: str
    source: str
    payload: dict
    event_sequence: int
    commit_index: int
    occurred_at: float
    committed_at: float


class _PluginStr(str):
    def __new__(cls, value, runtime_handle):
        instance = super().__new__(cls, value)
        instance.runtime_handle = runtime_handle
        return instance


class _PluginInt(int):
    def __new__(cls, value, runtime_handle):
        instance = super().__new__(cls, value)
        instance.runtime_handle = runtime_handle
        return instance


class _PluginFloat(float):
    def __new__(cls, value, runtime_handle):
        instance = super().__new__(cls, value)
        instance.runtime_handle = runtime_handle
        return instance


class _HostilePluginStr(_PluginStr):
    def __getitem__(self, _key):
        return self

    def __str__(self):
        return self


class _HostilePluginInt(_PluginInt):
    def __int__(self):
        return 0

    def __lt__(self, _other):
        return False


class _HostilePluginFloat(_PluginFloat):
    def __float__(self):
        return float("inf")


class _HostileEnumText(str):
    def __new__(cls, value, forged_value):
        instance = super().__new__(cls, value)
        instance.forged_value = forged_value
        return instance

    def __hash__(self):
        return hash(self.forged_value)

    def __eq__(self, other):
        return other == self.forged_value


def _event(
    name: str,
    commit_index: int,
    committed_at: float,
    *,
    source: str = "runner",
    event_sequence=None,
    occurred_at=None,
    payload=None,
) -> _Committed:
    return _Committed(
        name=name,
        source=source,
        payload={} if payload is None else payload,
        event_sequence=commit_index if event_sequence is None else event_sequence,
        commit_index=commit_index,
        occurred_at=committed_at if occurred_at is None else occurred_at,
        committed_at=committed_at,
    )


def _assert_exact_json_builtins(value):
    if isinstance(value, dict) or hasattr(value, "items"):
        for key, item in value.items():
            assert type(key) is str
            assert not hasattr(key, "runtime_handle")
            _assert_exact_json_builtins(item)
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            _assert_exact_json_builtins(item)
        return
    assert type(value) in (str, bool, int, float, type(None))
    assert not hasattr(value, "runtime_handle")


def _ready(
    component: str,
    *,
    offset_s: float = 0.0,
    mode: ActivationMode = ActivationMode.SERIAL,
) -> ScheduledActivation:
    return ScheduledActivation(
        component,
        ActivationSpec(trigger=TriggerSpec(offset_s=offset_s), mode=mode),
    )


def _on_event(
    component: str,
    name: str = "action.started",
    *,
    source=None,
    occurrence: int = 1,
    scope: EventScope = EventScope.AFTER_READY,
    offset_s: float = 0.0,
    timeout_s=None,
    mode: ActivationMode = ActivationMode.CONCURRENT,
) -> ScheduledActivation:
    return ScheduledActivation(
        component,
        ActivationSpec(
            trigger=TriggerSpec(
                kind=TriggerKind.EVENT,
                event=EventSelector(name=name, source=source, occurrence=occurrence),
                scope=scope,
                offset_s=offset_s,
                timeout_s=timeout_s,
            ),
            mode=mode,
        ),
    )


def _terminalize_admitted(planner: DeterministicActivationPlanner, timestamp: float) -> None:
    if planner.admitted_component_ids:
        planner.mark_invocation_started_many(planner.admitted_component_ids, timestamp=timestamp)


def test_activation_contracts_are_immutable_strict_and_round_trip():
    selector = EventSelector("feature.path-entered", source="observer", occurrence=2)
    trigger = TriggerSpec(
        kind="event",
        event=selector,
        scope="run",
        offset_s=0.25,
        timeout_s=5,
    )
    activation = ActivationSpec(trigger=trigger, mode="concurrent")
    scheduled = ScheduledActivation("inject", activation)

    assert activation.mode is ActivationMode.CONCURRENT
    assert trigger.kind is TriggerKind.EVENT
    assert trigger.scope is EventScope.RUN
    assert EventSelector.from_data(selector.to_data()) == selector
    assert TriggerSpec.from_data(trigger.to_data()) == trigger
    assert ActivationSpec.from_data(activation.to_data()) == activation
    assert ScheduledActivation.from_data(scheduled.to_data()) == scheduled

    with pytest.raises(FrozenInstanceError):
        selector.name = "changed"
    with pytest.raises(ValueError, match="unknown fields"):
        ActivationSpec.from_data({"unknown": True})
    with pytest.raises(TypeError, match="trigger must be a mapping"):
        ActivationSpec.from_data({"trigger": None})
    with pytest.raises(TypeError, match="activation must be a mapping"):
        ScheduledActivation.from_data(
            {
                "component_instance_id": "inject",
                "activation": None,
            }
        )
    with pytest.raises(ValueError, match="missing required fields"):
        EventSelector.from_data({})
    with pytest.raises(TypeError, match="mapping"):
        TriggerSpec.from_data([])


@pytest.mark.parametrize(
    "factory,match",
    [
        (lambda: EventSelector("bad event"), "unsupported characters"),
        (lambda: EventSelector("event", occurrence=0), "positive integer"),
        (lambda: TriggerSpec(offset_s=-1), "greater than or equal to zero"),
        (lambda: TriggerSpec(timeout_s=1), "ready triggers cannot declare"),
        (
            lambda: TriggerSpec(kind=TriggerKind.READY, event=EventSelector("event")),
            "cannot declare an event",
        ),
        (lambda: TriggerSpec(kind=TriggerKind.EVENT), "require an EventSelector"),
        (
            lambda: TriggerSpec(kind=TriggerKind.READY, scope=EventScope.RUN),
            "cannot change event scope",
        ),
        (
            lambda: TriggerSpec(
                kind=TriggerKind.EVENT,
                event=EventSelector("event"),
                timeout_s=0,
            ),
            "greater than zero",
        ),
    ],
)
def test_activation_contracts_reject_ambiguous_semantics(factory, match):
    with pytest.raises((TypeError, ValueError), match=match):
        factory()


def test_declarative_contracts_normalize_hostile_numeric_and_enum_subclasses_first():
    runtime_handle = object()

    selector = EventSelector(
        "event",
        occurrence=_HostilePluginInt(2, runtime_handle),
    )
    trigger = TriggerSpec(
        kind=TriggerKind.EVENT,
        event=selector,
        scope=_HostileEnumText("after_ready", "run"),
        offset_s=_HostilePluginFloat(1.25, runtime_handle),
    )
    activation = ActivationSpec(
        trigger=trigger,
        mode=_HostileEnumText("serial", "concurrent"),
    )

    assert type(selector.occurrence) is int
    assert selector.occurrence == 2
    assert trigger.scope is EventScope.AFTER_READY
    assert type(trigger.offset_s) is float
    assert trigger.offset_s == 1.25
    assert activation.mode is ActivationMode.SERIAL

    with pytest.raises(ValueError, match="positive integer"):
        EventSelector(
            "event",
            occurrence=_HostilePluginInt(0, runtime_handle),
        )
    with pytest.raises(ValueError, match="ready triggers cannot declare"):
        TriggerSpec(
            kind=_HostileEnumText("ready", "event"),
            event=EventSelector("event"),
        )


def test_declarative_contracts_reject_dataclass_subclasses():
    class TriggerSubclass(TriggerSpec):
        pass

    class ActivationSubclass(ActivationSpec):
        pass

    with pytest.raises(TypeError, match="activation trigger"):
        ActivationSpec(trigger=TriggerSubclass())
    with pytest.raises(TypeError, match="activation must be"):
        ScheduledActivation("component", ActivationSubclass())


@pytest.mark.parametrize(
    "overflowing",
    (
        _ready("overflowing", offset_s=1e308),
        _on_event("overflowing", name="trigger", timeout_s=1e308),
    ),
)
def test_ready_and_deadline_overflow_fail_before_batch_mutation(overflowing):
    declarations = (_ready("safe"), overflowing)
    planner = DeterministicActivationPlanner(declarations)
    before = planner.snapshot()

    with pytest.raises(InvalidScheduleTransitionError, match="finite scheduler time range"):
        planner.mark_ready_many(("safe", "overflowing"), timestamp=1e308)

    assert planner.snapshot() == before
    assert planner.next_boundary is None
    ready = planner.mark_ready_many(("safe", "overflowing"), timestamp=0)
    assert [entry.component_instance_id for entry in ready] == ["safe", "overflowing"]


def test_run_scoped_event_offset_overflow_fails_before_readiness_mutation():
    declarations = (
        _on_event("safe", name="trigger", scope=EventScope.RUN),
        _on_event(
            "overflowing",
            name="trigger",
            scope=EventScope.RUN,
            offset_s=1e308,
        ),
    )
    planner = DeterministicActivationPlanner(declarations)
    planner.observe_committed(_event("trigger", 1, 1e308), timestamp=1e308)
    before = planner.snapshot()

    with pytest.raises(InvalidScheduleTransitionError, match="finite scheduler time range"):
        planner.mark_ready_many(("safe", "overflowing"), timestamp=1e308)

    assert planner.snapshot() == before
    assert planner.next_boundary is None
    assert planner.mark_ready("safe", timestamp=1e308).transition is ScheduleTransition.READY


def test_observed_event_offset_overflow_is_atomic_for_all_waiters():
    declarations = (
        _on_event("safe", name="trigger"),
        _on_event("overflowing", name="trigger", offset_s=1e308),
    )
    planner = DeterministicActivationPlanner(declarations)
    planner.mark_ready_many(("safe", "overflowing"), timestamp=0)
    before = planner.snapshot()
    counters_before = planner.work_counters

    with pytest.raises(InvalidScheduleTransitionError, match="finite scheduler time range"):
        planner.observe_committed(_event("trigger", 1, 1e308), timestamp=1e308)

    assert planner.snapshot() == before
    assert planner.committed_events == ()
    assert planner.work_counters == counters_before
    assert planner.next_boundary is None
    cancelled = planner.cancel_pending(timestamp=1e308)
    assert [entry.component_instance_id for entry in cancelled] == ["safe", "overflowing"]


def test_only_committed_event_envelopes_match_scheduler_protocol():
    committed = _event("ready", 1, 2, payload={"node": "n1"})
    raw = LifecycleEvent(
        name="ready",
        sequence=1,
        timestamp=2,
        source="runner",
        payload={"node": "n1"},
    )

    assert isinstance(committed, CommittedEvent)
    assert not isinstance(raw, CommittedEvent)
    with pytest.raises(TypeError, match="CommittedEvent"):
        DeterministicActivationPlanner(()).observe_committed(raw, timestamp=2)

    envelope = CommittedLifecycleEvent(event=raw, commit_index=1, committed_at=3)
    assert isinstance(envelope, CommittedEvent)
    planner = DeterministicActivationPlanner((_on_event("component", name="ready"),))
    planner.mark_ready("component", timestamp=1)
    planner.observe_committed(envelope, timestamp=3)
    assert planner.advance(3)[0].trigger_event_sequence == raw.sequence


def test_committed_event_record_is_detached_immutable_and_round_trips():
    payload = {"nodes": ["n1"], "nested": {"ok": True}}
    record = CommittedEventRecord.from_event(_event("topology.ready", 1, 4, payload=payload, occurred_at=3))
    payload["nodes"].append("n2")

    assert record.payload == {"nodes": ("n1",), "nested": {"ok": True}}
    assert CommittedEventRecord.from_data(record.to_data()) == record
    with pytest.raises(TypeError):
        record.payload["new"] = True
    with pytest.raises(ValueError, match="cannot precede"):
        replace(record, committed_at=2)


def test_plugin_scalar_subclasses_are_erased_at_event_record_ingestion():
    runtime_handle = object()
    name = _PluginStr("topology.ready", runtime_handle)
    source = _PluginStr("runner", runtime_handle)
    payload_key = _PluginStr("node", runtime_handle)
    payload_text = _PluginStr("n1", runtime_handle)
    payload_int = _PluginInt(7, runtime_handle)
    payload_float = _PluginFloat(1.25, runtime_handle)
    event_sequence = _PluginInt(9, runtime_handle)
    commit_index = _PluginInt(1, runtime_handle)
    occurred_at = _PluginFloat(2, runtime_handle)
    committed_at = _PluginFloat(3, runtime_handle)
    event = _Committed(
        name=name,
        source=source,
        payload={
            payload_key: {
                _PluginStr("values", runtime_handle): [
                    payload_text,
                    payload_int,
                    payload_float,
                ]
            }
        },
        event_sequence=event_sequence,
        commit_index=commit_index,
        occurred_at=occurred_at,
        committed_at=committed_at,
    )

    record = CommittedEventRecord.from_event(event)

    assert type(record.name) is str
    assert type(record.source) is str
    assert type(record.event_sequence) is int
    assert type(record.commit_index) is int
    assert type(record.occurred_at) is float
    assert type(record.committed_at) is float
    assert record.name is not name
    assert record.source is not source
    assert record.event_sequence is not event_sequence
    assert record.commit_index is not commit_index
    assert record.occurred_at is not occurred_at
    assert record.committed_at is not committed_at
    assert next(iter(record.payload)) is not payload_key
    _assert_exact_json_builtins(record.to_data())


def test_hostile_numeric_subclasses_cannot_change_event_record_values_during_conversion():
    runtime_handle = object()
    record = CommittedEventRecord.from_event(
        _Committed(
            name="event",
            source="runner",
            payload={
                "count": _HostilePluginInt(7, runtime_handle),
                "ratio": _HostilePluginFloat(1.5, runtime_handle),
            },
            event_sequence=_HostilePluginInt(4, runtime_handle),
            commit_index=_HostilePluginInt(1, runtime_handle),
            occurred_at=_HostilePluginFloat(2, runtime_handle),
            committed_at=_HostilePluginFloat(3, runtime_handle),
        )
    )

    assert record.event_sequence == 4
    assert record.commit_index == 1
    assert record.occurred_at == 2
    assert record.committed_at == 3
    assert record.payload == {"count": 7, "ratio": 1.5}
    _assert_exact_json_builtins(record.to_data())


def test_plugin_scalar_subclasses_cannot_survive_scheduler_snapshot():
    runtime_handle = object()
    event = _Committed(
        name=_HostilePluginStr("trigger", runtime_handle),
        source=_HostilePluginStr("runner", runtime_handle),
        payload={
            _HostilePluginStr("count", runtime_handle): _PluginInt(3, runtime_handle),
            _HostilePluginStr("ratio", runtime_handle): _PluginFloat(0.5, runtime_handle),
            _HostilePluginStr("label", runtime_handle): _HostilePluginStr("plugin", runtime_handle),
        },
        event_sequence=_PluginInt(4, runtime_handle),
        commit_index=_PluginInt(1, runtime_handle),
        occurred_at=_PluginFloat(1, runtime_handle),
        committed_at=_PluginFloat(2, runtime_handle),
    )
    declaration = ScheduledActivation(
        _HostilePluginStr("component", runtime_handle),
        ActivationSpec(
            trigger=TriggerSpec(
                kind=_HostilePluginStr("event", runtime_handle),
                event=EventSelector(
                    _HostilePluginStr("trigger", runtime_handle),
                    source=_HostilePluginStr("runner", runtime_handle),
                    occurrence=_PluginInt(1, runtime_handle),
                ),
                offset_s=_PluginFloat(1, runtime_handle),
            ),
            mode=_HostilePluginStr("concurrent", runtime_handle),
        ),
    )
    planner = DeterministicActivationPlanner((declaration,))
    planner.mark_ready(
        _HostilePluginStr("component", runtime_handle),
        timestamp=_PluginFloat(1, runtime_handle),
    )
    planner.observe_committed(event, timestamp=_PluginFloat(2, runtime_handle))
    planner.advance(_PluginFloat(3, runtime_handle))
    planner.mark_invocation_started(
        _HostilePluginStr("component", runtime_handle),
        timestamp=_PluginFloat(3, runtime_handle),
    )

    snapshot = planner.snapshot()
    assert type(declaration.component_instance_id) is str
    assert type(declaration.activation.trigger.event.name) is str
    assert type(declaration.activation.trigger.event.occurrence) is int
    assert type(declaration.activation.trigger.offset_s) is float
    _assert_exact_json_builtins(snapshot.to_data())

    restored_root = ScheduleJournalSnapshot(
        activation_digest=_HostilePluginStr(snapshot.activation_digest, runtime_handle),
        operations=snapshot.operations,
        entries=snapshot.entries,
        schema_id=_HostilePluginStr(SCHEDULE_JOURNAL_SCHEMA_ID, runtime_handle),
        schema_version=_PluginInt(SCHEDULE_JOURNAL_SCHEMA_VERSION, runtime_handle),
        operation_high_water_mark=_PluginInt(len(snapshot.operations), runtime_handle),
        entry_high_water_mark=_PluginInt(len(snapshot.entries), runtime_handle),
        trace_digest=_HostilePluginStr(snapshot.trace_digest, runtime_handle),
    )
    assert type(restored_root.activation_digest) is str
    assert type(restored_root.schema_id) is str
    assert type(restored_root.schema_version) is int
    assert type(restored_root.operation_high_water_mark) is int
    assert type(restored_root.entry_high_water_mark) is int
    assert type(restored_root.trace_digest) is str
    _assert_exact_json_builtins(restored_root.to_data())

    abandoned = DeterministicActivationPlanner((_ready("abandoned"),))
    abandoned.mark_ready("abandoned", timestamp=1)
    abandoned.advance(1)
    entry = abandoned.abandon_admission(
        "abandoned",
        timestamp=1,
        reason=_HostilePluginStr("worker unavailable", runtime_handle),
    )
    assert type(entry.reason) is str
    assert not hasattr(entry.reason, "runtime_handle")


def test_ready_offsets_admit_by_boundary_then_declaration_order():
    planner = DeterministicActivationPlanner(
        (
            _ready("manifest-first", offset_s=2),
            _ready("earliest", offset_s=1),
            _ready("manifest-third", offset_s=2),
        )
    )

    ready = planner.mark_ready_many(
        ("manifest-third", "earliest", "manifest-first"),
        timestamp=10,
    )
    assert [entry.component_instance_id for entry in ready] == [
        "manifest-first",
        "earliest",
        "manifest-third",
    ]
    assert planner.next_boundary == 11
    assert planner.advance(10.5) == ()
    assert [entry.component_instance_id for entry in planner.advance(12)] == [
        "earliest",
        "manifest-first",
        "manifest-third",
    ]
    assert planner.snapshot().admission_order == (
        "earliest",
        "manifest-first",
        "manifest-third",
    )


def test_exact_source_and_nth_occurrence_are_matched_incrementally():
    planner = DeterministicActivationPlanner((_on_event("inject", source="activate", occurrence=2, offset_s=0.5),))
    planner.observe_committed(
        _event("action.started", 1, 1, source="activate"),
        timestamp=1,
    )
    ready = planner.mark_ready("inject", timestamp=2)
    assert ready.event_cursor == 1

    planner.observe_committed(
        _event("action.started", 2, 3, source="other"),
        timestamp=3,
    )
    planner.observe_committed(
        _event("action.completed", 3, 4, source="activate"),
        timestamp=4,
    )
    planner.observe_committed(
        _event("action.started", 4, 5, source="activate"),
        timestamp=5,
    )
    planner.observe_committed(
        _event("action.started", 5, 6, source="activate", event_sequence=9),
        timestamp=6,
    )

    admitted = planner.advance(6.5)
    assert len(admitted) == 1
    assert admitted[0].trigger_event_sequence == 9
    assert admitted[0].trigger_event_commit_index == 5
    assert admitted[0].scheduled_for == 6.5


def test_run_scope_can_select_an_occurrence_committed_before_readiness():
    planner = DeterministicActivationPlanner(
        (
            _on_event(
                "late-ready",
                name="feature.trigger",
                scope=EventScope.RUN,
                offset_s=10,
            ),
        )
    )
    planner.observe_committed(
        _event("feature.trigger", 1, 2, source="action", event_sequence=3),
        timestamp=2,
    )
    planner.mark_ready("late-ready", timestamp=20)

    admitted = planner.advance(20)
    assert admitted[0].trigger_event_sequence == 3
    assert admitted[0].scheduled_for == 20


def test_after_ready_scope_uses_commit_cursor_not_equal_timestamps():
    planner = DeterministicActivationPlanner((_on_event("fault", name="trigger"),))
    planner.observe_committed(_event("trigger", 1, 10), timestamp=10)
    ready = planner.mark_ready("fault", timestamp=10)
    planner.observe_committed(_event("trigger", 2, 10), timestamp=10)
    admitted = planner.advance(10)

    assert ready.event_cursor == 1
    assert admitted[0].trigger_event_commit_index == 2


def test_offset_boundary_never_precedes_event_commit():
    planner = DeterministicActivationPlanner((_on_event("fault", name="trigger", offset_s=3),))
    planner.mark_ready("fault", timestamp=1)
    planner.observe_committed(
        _event("trigger", 1, 10, occurred_at=2, event_sequence=4),
        timestamp=10,
    )

    assert planner.next_boundary == 13
    assert planner.advance(13)[0].scheduled_for == 13


def test_occurred_at_may_regress_while_commit_time_remains_authoritative():
    planner = DeterministicActivationPlanner(())
    planner.observe_committed(_event("first", 1, 10, occurred_at=9), timestamp=10)
    planner.observe_committed(_event("second", 2, 11, occurred_at=3), timestamp=11)

    assert [event.occurred_at for event in planner.committed_events] == [9, 3]


def test_event_at_deadline_is_eligible_only_when_observed_before_advance():
    activation = _on_event("fault", name="trigger", timeout_s=5)

    observed_first = DeterministicActivationPlanner((activation,))
    observed_first.mark_ready("fault", timestamp=10)
    observed_first.observe_committed(_event("trigger", 1, 15), timestamp=15)
    assert observed_first.advance(15)[0].transition is ScheduleTransition.ADMITTED

    advanced_first = DeterministicActivationPlanner((activation,))
    advanced_first.mark_ready("fault", timestamp=10)
    assert advanced_first.advance(15)[0].transition is ScheduleTransition.EXPIRED
    advanced_first.observe_committed(_event("trigger", 1, 15), timestamp=15)
    assert advanced_first.advance(15) == ()


def test_late_commit_expires_even_if_coordinator_has_not_advanced_yet():
    planner = DeterministicActivationPlanner((_on_event("fault", name="trigger", timeout_s=4),))
    planner.mark_ready("fault", timestamp=1)
    planner.observe_committed(_event("trigger", 1, 6, occurred_at=2), timestamp=6)

    expired = planner.advance(6)
    assert expired[0].transition is ScheduleTransition.EXPIRED
    assert expired[0].scheduled_for == 5


def test_trigger_timeout_bounds_commit_observation_not_activation_offset():
    planner = DeterministicActivationPlanner((_on_event("fault", name="trigger", timeout_s=4, offset_s=10),))
    planner.mark_ready("fault", timestamp=1)
    planner.observe_committed(_event("trigger", 1, 5, occurred_at=2), timestamp=5)

    assert planner.advance(5) == ()
    assert planner.next_boundary == 15
    assert planner.advance(15)[0].transition is ScheduleTransition.ADMITTED


def test_advance_merges_due_and_deadline_heaps_deterministically():
    planner = DeterministicActivationPlanner(
        (
            _on_event("admitted-first", name="hit", timeout_s=5),
            _on_event("expired-second", name="miss", timeout_s=5),
            _ready("earlier-boundary", offset_s=2),
        )
    )
    planner.mark_ready_many(
        ("expired-second", "earlier-boundary", "admitted-first"),
        timestamp=10,
    )
    planner.observe_committed(_event("hit", 1, 15), timestamp=15)

    entries = planner.advance(15)
    assert [(entry.component_instance_id, entry.transition, entry.scheduled_for) for entry in entries] == [
        ("earlier-boundary", ScheduleTransition.ADMITTED, 12),
        ("admitted-first", ScheduleTransition.ADMITTED, 15),
        ("expired-second", ScheduleTransition.EXPIRED, 15),
    ]


def test_admission_start_abandonment_and_cleanup_ownership_are_distinct():
    planner = DeterministicActivationPlanner(
        (_ready("started"), _ready("abandoned"), _on_event("expired", timeout_s=1))
    )
    planner.mark_ready_many(("expired", "abandoned", "started"), timestamp=1)
    entries = planner.advance(2)
    assert [entry.transition for entry in entries] == [
        ScheduleTransition.ADMITTED,
        ScheduleTransition.ADMITTED,
        ScheduleTransition.EXPIRED,
    ]
    assert planner.snapshot().cleanup_order == ()

    planner.mark_invocation_started("started", timestamp=2)
    planner.abandon_admission("abandoned", timestamp=2, reason="worker unavailable")
    snapshot = planner.snapshot()

    assert snapshot.invocation_start_order == ("started",)
    assert snapshot.cleanup_order == ("started",)
    assert snapshot.entries[-1].transition is ScheduleTransition.ADMISSION_ABANDONED
    assert snapshot.entries[-1].reason == "worker unavailable"


def test_cleanup_order_uses_actual_start_order_not_admission_order():
    planner = DeterministicActivationPlanner((_ready("first"), _ready("second"), _ready("third")))
    planner.mark_ready_many(("first", "second", "third"), timestamp=1)
    planner.advance(1)
    planner.mark_invocation_started_many(("third", "first", "second"), timestamp=2)

    snapshot = planner.snapshot()
    assert snapshot.admission_order == ("first", "second", "third")
    assert snapshot.invocation_start_order == ("third", "first", "second")
    assert snapshot.cleanup_order == ("second", "first", "third")


def test_cancellation_is_stable_and_does_not_cancel_admitted_components():
    planner = DeterministicActivationPlanner((_ready("first"), _on_event("second"), _ready("third")))
    planner.mark_ready("second", timestamp=1)
    cancelled = planner.cancel_pending(timestamp=2)

    assert [entry.component_instance_id for entry in cancelled] == [
        "first",
        "second",
        "third",
    ]
    assert all(entry.transition is ScheduleTransition.CANCELLED for entry in cancelled)

    admitted = DeterministicActivationPlanner((_ready("component"),))
    admitted.mark_ready("component", timestamp=1)
    admitted.advance(1)
    with pytest.raises(InvalidScheduleTransitionError, match="cannot be cancelled"):
        admitted.cancel_pending(timestamp=1, component_instance_ids=("component",))


def test_every_successful_call_is_traced_including_zero_output_batches():
    planner = DeterministicActivationPlanner(())
    assert planner.mark_ready_many((), timestamp=1) == ()
    assert planner.advance(2) == ()
    assert planner.cancel_pending(timestamp=3, component_instance_ids=()) == ()
    planner.observe_committed(_event("heartbeat", 1, 4), timestamp=4)
    assert planner.cancel_pending(timestamp=5) == ()

    snapshot = planner.snapshot()
    assert [operation.kind for operation in snapshot.operations] == [
        ScheduleOperationKind.READY,
        ScheduleOperationKind.ADVANCE,
        ScheduleOperationKind.CANCEL,
        ScheduleOperationKind.OBSERVE,
        ScheduleOperationKind.CANCEL,
    ]
    assert all(operation.entry_sequences == () for operation in snapshot.operations)
    assert snapshot.operations[2].all_pending is False
    assert snapshot.operations[4].all_pending is True
    validate_schedule_replay((), snapshot, planner.committed_events)


def test_operation_trace_preserves_input_batches_and_output_boundaries():
    planner = DeterministicActivationPlanner((_ready("first"), _ready("second"), _ready("third")))
    planner.mark_ready_many(("third", "first", "second"), timestamp=1)
    planner.advance(1)
    planner.mark_invocation_started_many(("second", "third", "first"), timestamp=2)
    snapshot = planner.snapshot()

    assert snapshot.operations[0].component_instance_ids == ("third", "first", "second")
    assert snapshot.operations[0].entry_sequences == (1, 2, 3)
    assert [entry.component_instance_id for entry in snapshot.entries[:3]] == [
        "first",
        "second",
        "third",
    ]
    assert snapshot.operations[2].component_instance_ids == ("second", "third", "first")
    assert [entry.batch_index for entry in snapshot.entries[6:]] == [0, 1, 2]


def test_failed_operations_are_atomic_and_do_not_enter_trace():
    planner = DeterministicActivationPlanner((_ready("known"), _ready("other")))
    before = planner.snapshot()

    with pytest.raises(UnknownActivationError):
        planner.mark_ready_many(("known", "missing"), timestamp=1)
    assert planner.snapshot() == before

    planner.mark_ready("known", timestamp=2)
    after_ready = planner.snapshot()
    with pytest.raises(InvalidScheduleTransitionError, match="awaiting readiness"):
        planner.mark_ready_many(("other", "known"), timestamp=2)
    assert planner.snapshot() == after_ready

    with pytest.raises(InvalidScheduleTransitionError, match="regresses"):
        planner.advance(1)
    assert planner.snapshot() == after_ready


def test_hostile_equal_text_identifier_subclasses_cannot_bypass_batch_duplicates():
    class HostileIdentifier(str):
        __hash__ = str.__hash__

        def __eq__(self, other):
            if type(other) is HostileIdentifier:
                return self is other
            return str.__eq__(self, other)

    duplicate_ids = (
        HostileIdentifier("component"),
        HostileIdentifier("component"),
    )
    assert len(set(duplicate_ids)) == 2

    ready = DeterministicActivationPlanner((_ready("component"),))
    before = ready.snapshot()
    with pytest.raises(InvalidScheduleTransitionError, match="duplicates"):
        ready.mark_ready_many(duplicate_ids, timestamp=1)
    assert ready.snapshot() == before

    cancelled = DeterministicActivationPlanner((_ready("component"),))
    before = cancelled.snapshot()
    with pytest.raises(InvalidScheduleTransitionError, match="duplicates"):
        cancelled.cancel_pending(
            timestamp=1,
            component_instance_ids=duplicate_ids,
        )
    assert cancelled.snapshot() == before

    started = DeterministicActivationPlanner((_ready("component"),))
    started.mark_ready("component", timestamp=1)
    started.advance(1)
    before = started.snapshot()
    with pytest.raises(InvalidScheduleTransitionError, match="duplicates"):
        started.mark_invocation_started_many(duplicate_ids, timestamp=1)
    assert started.snapshot() == before

    abandoned = DeterministicActivationPlanner((_ready("component"),))
    abandoned.mark_ready("component", timestamp=1)
    abandoned.advance(1)
    before = abandoned.snapshot()
    with pytest.raises(InvalidScheduleTransitionError, match="duplicates"):
        abandoned.abandon_admissions(duplicate_ids, timestamp=1)
    assert abandoned.snapshot() == before


def test_commit_order_is_authoritative_and_invalid_commit_metadata_is_atomic():
    planner = DeterministicActivationPlanner(())
    planner.observe_committed(_event("first", 1, 1, event_sequence=20), timestamp=1)
    planner.observe_committed(_event("second", 2, 2, event_sequence=2), timestamp=2)
    before = planner.snapshot()

    with pytest.raises(InvalidScheduleTransitionError, match="contiguous"):
        planner.observe_committed(
            _event("third", 4, 3, event_sequence=100),
            timestamp=3,
        )
    assert planner.snapshot() == before
    with pytest.raises(InvalidScheduleTransitionError, match="commit timestamps"):
        planner.observe_committed(
            _event("third", 3, 1.5, event_sequence=3),
            timestamp=3,
        )
    assert planner.snapshot() == before


def test_delayed_committed_event_observation_after_a_newer_decision_is_replayable():
    activation = _on_event(
        "component",
        name="trigger",
        scope=EventScope.RUN,
    )
    planner = DeterministicActivationPlanner((activation,))
    planner.mark_ready("component", timestamp=0)
    assert planner.advance(10) == ()

    delayed = _event("trigger", 1, 9, event_sequence=4)
    planner.observe_committed(delayed, timestamp=11)
    admitted = planner.advance(11)
    planner.mark_invocation_started("component", timestamp=11)

    assert admitted[0].scheduled_for == 9
    assert planner.snapshot().operations[2].timestamp == 11
    assert planner.snapshot().operations[2].committed_event.committed_at == 9
    validate_schedule_replay((activation,), planner.snapshot(), (delayed,))


def test_committed_event_observation_time_cannot_precede_its_commit():
    planner = DeterministicActivationPlanner(())
    before = planner.snapshot()

    with pytest.raises(InvalidScheduleTransitionError, match="cannot precede committed_at"):
        planner.observe_committed(_event("event", 1, 5), timestamp=4)

    assert planner.snapshot() == before
    assert planner.committed_events == ()
    planner.observe_committed(_event("event", 1, 5), timestamp=6)


def test_next_boundary_lazily_discards_cancelled_and_matched_deadlines():
    planner = DeterministicActivationPlanner(
        (
            _ready("cancelled", offset_s=1),
            _on_event("matched", name="hit", timeout_s=2, offset_s=5),
        )
    )
    planner.mark_ready_many(("cancelled", "matched"), timestamp=1)
    planner.cancel_pending(timestamp=1, component_instance_ids=("cancelled",))
    planner.observe_committed(_event("hit", 1, 2), timestamp=2)

    assert planner.next_boundary == 7
    planner.advance(7)
    assert planner.next_boundary is None
    assert planner.work_counters.stale_heap_pops >= 2


def _complex_completed_schedule():
    activations = (
        _ready("workload", offset_s=1),
        _on_event(
            "fault",
            name="action.started",
            source="activate",
            occurrence=2,
            offset_s=0.25,
            timeout_s=10,
        ),
        _on_event("never", name="missing", timeout_s=3),
        _ready("cancelled"),
    )
    planner = DeterministicActivationPlanner(activations)
    planner.mark_ready_many(("fault", "workload", "never"), timestamp=1)
    planner.observe_committed(
        _event("action.started", 1, 2, source="activate"),
        timestamp=2,
    )
    first = planner.advance(2)
    planner.mark_invocation_started_many(
        tuple(entry.component_instance_id for entry in first),
        timestamp=2,
    )
    planner.observe_committed(
        _event("action.started", 2, 3, source="activate", event_sequence=4),
        timestamp=3,
    )
    second = planner.advance(3.25)
    planner.mark_invocation_started_many(
        tuple(entry.component_instance_id for entry in second),
        timestamp=3.25,
    )
    planner.advance(4)
    planner.cancel_pending(timestamp=4, component_instance_ids=("cancelled",))
    return activations, planner


def test_journal_has_versioned_root_canonical_json_and_exact_replay():
    activations, planner = _complex_completed_schedule()
    snapshot = planner.snapshot()
    restored = ScheduleJournalSnapshot.from_json(snapshot.to_json())

    assert restored == snapshot
    assert restored.schema_id == SCHEDULE_JOURNAL_SCHEMA_ID
    assert restored.schema_version == SCHEDULE_JOURNAL_SCHEMA_VERSION
    assert restored.operation_high_water_mark == len(restored.operations)
    assert restored.entry_high_water_mark == len(restored.entries)
    assert restored.digest == snapshot.digest
    assert json.loads(snapshot.to_json()) == snapshot.to_data()
    assert snapshot.admission_order == ("workload", "fault")
    assert snapshot.cleanup_order == ("fault", "workload")
    validate_schedule_replay(activations, restored, planner.committed_events)
    validate_schedule_replay(activations, restored.to_data(), planner.committed_events)


@pytest.mark.parametrize(
    "field,value,match",
    [
        ("schema_id", "other", "schema_id"),
        ("schema_version", 99, "schema_version"),
        ("operation_high_water_mark", 99, "number of operations"),
        ("entry_high_water_mark", 99, "number of entries"),
    ],
)
def test_journal_rejects_unsupported_or_inconsistent_root(field, value, match):
    data = DeterministicActivationPlanner(()).snapshot().to_data()
    data[field] = value
    with pytest.raises(ValueError, match=match):
        ScheduleJournalSnapshot.from_data(data)


def test_journal_rejects_duplicate_keys_constants_and_unknown_fields():
    with pytest.raises(ValueError, match="duplicate object key"):
        ScheduleJournalSnapshot.from_json('{"schema_id":"x","schema_id":"y"}')
    with pytest.raises(ValueError, match="unsupported JSON constant"):
        ScheduleJournalSnapshot.from_json('{"schema_id":NaN}')
    data = DeterministicActivationPlanner(()).snapshot().to_data()
    data["unknown"] = True
    with pytest.raises(ValueError, match="unknown fields"):
        ScheduleJournalSnapshot.from_data(data)


def test_replay_rejects_changed_activation_declarations():
    activations, planner = _complex_completed_schedule()
    changed = (
        ScheduledActivation(
            "workload",
            ActivationSpec(
                trigger=TriggerSpec(offset_s=1),
                mode=ActivationMode.CONCURRENT,
            ),
        ),
        *activations[1:],
    )

    with pytest.raises(ScheduleReplayError, match="activation digest"):
        validate_schedule_replay(changed, planner.snapshot(), planner.committed_events)


def test_replay_rejects_external_committed_event_mismatch():
    activations, planner = _complex_completed_schedule()
    changed_events = list(planner.committed_events)
    changed_events[0] = replace(changed_events[0], payload={"changed": True})

    with pytest.raises(ScheduleReplayError, match="committed event history"):
        validate_schedule_replay(activations, planner.snapshot(), changed_events)


def test_replay_reconstructs_entries_instead_of_trusting_them():
    activations = (_ready("component", offset_s=2),)
    planner = DeterministicActivationPlanner(activations)
    planner.mark_ready("component", timestamp=1)
    planner.advance(3)
    planner.mark_invocation_started("component", timestamp=3)
    snapshot = planner.snapshot()
    entries = list(snapshot.entries)
    entries[1] = replace(entries[1], scheduled_for=2)
    semantically_tampered = ScheduleJournalSnapshot(
        activation_digest=snapshot.activation_digest,
        operations=snapshot.operations,
        entries=tuple(entries),
    )

    with pytest.raises(ScheduleReplayError, match="emitted different entries"):
        validate_schedule_replay(activations, semantically_tampered)


def test_replay_reconstructs_operations_instead_of_trusting_entry_shape():
    activations = (_ready("first"), _ready("second"), _ready("third"))
    planner = DeterministicActivationPlanner(activations)
    planner.mark_ready_many(("first", "second"), timestamp=1)
    planner.advance(1)
    planner.mark_invocation_started_many(("first", "second"), timestamp=1)
    planner.cancel_pending(timestamp=1, component_instance_ids=("third",))
    snapshot = planner.snapshot()
    operations = list(snapshot.operations)
    operations[0] = replace(
        operations[0],
        component_instance_ids=("first", "third"),
    )
    semantically_tampered = ScheduleJournalSnapshot(
        activation_digest=snapshot.activation_digest,
        operations=tuple(operations),
        entries=snapshot.entries,
    )

    with pytest.raises(ScheduleReplayError, match="emitted different entries"):
        validate_schedule_replay(activations, semantically_tampered)


def test_serialized_trace_digest_detects_zero_output_operation_tampering():
    planner = DeterministicActivationPlanner(())
    planner.advance(1)
    planner.advance(2)
    data = planner.snapshot().to_data()
    data["operations"].pop(0)
    data["operation_high_water_mark"] = 1
    data["operations"][0]["sequence"] = 1

    with pytest.raises(ValueError, match="trace_digest"):
        ScheduleJournalSnapshot.from_data(data)


@pytest.mark.parametrize(
    "field_name",
    (
        "operation_high_water_mark",
        "entry_high_water_mark",
        "trace_digest",
    ),
)
def test_serialized_journal_integrity_fields_cannot_be_null(field_name):
    planner = DeterministicActivationPlanner(())
    planner.advance(1)
    data = planner.snapshot().to_data()
    data[field_name] = None

    with pytest.raises(TypeError, match=rf"{field_name} must not be null"):
        ScheduleJournalSnapshot.from_data(data)


def test_replay_terminal_requirement_distinguishes_admitted_from_started():
    activation = _ready("component")
    planner = DeterministicActivationPlanner((activation,))
    planner.mark_ready("component", timestamp=1)
    planner.advance(1)

    with pytest.raises(ScheduleReplayError, match="no terminal"):
        validate_schedule_replay((activation,), planner.snapshot())
    validate_schedule_replay(
        (activation,),
        planner.snapshot(),
        require_terminal=False,
    )


def test_snapshot_rejects_broken_operation_entry_partition():
    activation = _ready("component")
    planner = DeterministicActivationPlanner((activation,))
    planner.mark_ready("component", timestamp=1)
    snapshot = planner.snapshot()
    with pytest.raises(ValueError, match="one entry per component"):
        replace(snapshot.operations[0], entry_sequences=())


def test_planner_rejects_duplicate_declarations_and_unordered_inputs():
    with pytest.raises(DuplicateActivationError):
        DeterministicActivationPlanner((_ready("same"), _ready("same")))
    with pytest.raises(TypeError, match="ordered sequence"):
        DeterministicActivationPlanner({_ready("one")})
    with pytest.raises(TypeError, match="ordered sequence"):
        DeterministicActivationPlanner(iter((_ready("one"),)))


def test_state_specific_methods_reject_wrong_transitions():
    planner = DeterministicActivationPlanner((_ready("component"),))
    with pytest.raises(InvalidScheduleTransitionError, match="not admitted"):
        planner.mark_invocation_started("component", timestamp=1)
    planner.mark_ready("component", timestamp=1)
    with pytest.raises(InvalidScheduleTransitionError, match="not admitted"):
        planner.abandon_admission("component", timestamp=1)
    planner.advance(1)
    planner.abandon_admission("component", timestamp=1)
    with pytest.raises(InvalidScheduleTransitionError, match="not admitted"):
        planner.mark_invocation_started("component", timestamp=1)


def test_deterministic_script_produces_byte_identical_journal():
    activations = (
        _on_event("one", name="tick", occurrence=2, timeout_s=10),
        _ready("two", offset_s=4),
    )

    def run():
        planner = DeterministicActivationPlanner(activations)
        planner.mark_ready_many(("two", "one"), timestamp=1)
        planner.advance(2)
        planner.observe_committed(
            _event("tick", 1, 3, event_sequence=2),
            timestamp=3,
        )
        planner.observe_committed(
            _event("tick", 2, 4, event_sequence=5),
            timestamp=4,
        )
        admitted = planner.advance(5)
        planner.mark_invocation_started_many(
            tuple(entry.component_instance_id for entry in admitted),
            timestamp=5,
        )
        return planner.snapshot().to_json()

    assert run() == run()


def test_randomized_scheduler_matches_independent_occurrence_oracle():
    randomizer = random.Random(94731)
    names = ("alpha", "beta", "gamma")
    sources = ("action", "observer")
    declarations = []
    for index in range(120):
        declarations.append(
            _on_event(
                f"component-{index}",
                name=randomizer.choice(names),
                source=randomizer.choice((*sources, None)),
                occurrence=randomizer.randint(1, 4),
                scope=randomizer.choice((EventScope.RUN, EventScope.AFTER_READY)),
                offset_s=randomizer.choice((0, 0.25, 2)),
            )
        )

    events = []
    for commit_index in range(1, 81):
        committed_at = float(commit_index)
        events.append(
            _event(
                randomizer.choice(names),
                commit_index,
                committed_at,
                source=randomizer.choice(sources),
                event_sequence=commit_index * 2,
                occurred_at=max(0, committed_at - randomizer.choice((0, 0.5, 3))),
            )
        )

    planner = DeterministicActivationPlanner(tuple(declarations))
    for event in events[:20]:
        planner.observe_committed(event, timestamp=event.committed_at)
    planner.mark_ready_many(
        tuple(declaration.component_instance_id for declaration in reversed(declarations)),
        timestamp=20,
    )
    for event in events[20:]:
        planner.observe_committed(event, timestamp=event.committed_at)
    admitted = planner.advance(200)

    expected = []
    for declaration_index, declaration in enumerate(declarations):
        trigger = declaration.activation.trigger
        assert trigger.event is not None
        candidates = [
            event
            for event in events
            if (
                (trigger.scope is EventScope.RUN or event.commit_index > 20)
                and event.name == trigger.event.name
                and (trigger.event.source is None or event.source == trigger.event.source)
            )
        ]
        if len(candidates) < trigger.event.occurrence:
            continue
        selected = candidates[trigger.event.occurrence - 1]
        expected.append(
            (
                max(20, selected.committed_at + trigger.offset_s),
                declaration_index,
                declaration.component_instance_id,
                selected.event_sequence,
            )
        )
    expected.sort()

    assert [
        (
            entry.scheduled_for,
            next(
                index
                for index, declaration in enumerate(declarations)
                if declaration.component_instance_id == entry.component_instance_id
            ),
            entry.component_instance_id,
            entry.trigger_event_sequence,
        )
        for entry in admitted
    ] == expected


def test_unrelated_events_do_constant_work_not_waiter_cross_product():
    waiter_count = 1500
    event_count = 1000
    declarations = tuple(_on_event(f"waiter-{index}", name="target") for index in range(waiter_count))
    planner = DeterministicActivationPlanner(declarations)
    planner.mark_ready_many(
        tuple(declaration.component_instance_id for declaration in declarations),
        timestamp=0,
    )
    for commit_index in range(1, event_count + 1):
        planner.observe_committed(
            _event("noise", commit_index, commit_index),
            timestamp=commit_index,
        )

    counters = planner.work_counters
    assert counters.observed_events == event_count
    assert counters.stream_appends == event_count * 2
    assert counters.waiter_bucket_probes == event_count * 2
    assert counters.waiter_candidates_processed == 0
    assert counters.waiter_registrations == waiter_count

    planner.observe_committed(
        _event("target", event_count + 1, event_count + 1),
        timestamp=event_count + 1,
    )
    assert planner.work_counters.waiter_candidates_processed == waiter_count


def test_nth_occurrence_does_not_rescan_event_history():
    occurrence = 2000
    planner = DeterministicActivationPlanner((_on_event("late", name="tick", occurrence=occurrence),))
    planner.mark_ready("late", timestamp=0)
    for commit_index in range(1, occurrence + 1):
        planner.observe_committed(
            _event("tick", commit_index, commit_index),
            timestamp=commit_index,
        )

    counters = planner.work_counters
    assert counters.waiter_registrations == 1
    assert counters.waiter_candidates_processed == 1
    assert counters.stream_appends == occurrence * 2
    assert planner.advance(occurrence)[0].trigger_event_commit_index == occurrence


def test_heap_work_is_proportional_to_due_candidates():
    declarations = tuple(_ready(f"component-{index}", offset_s=index) for index in range(1000))
    planner = DeterministicActivationPlanner(declarations)
    planner.mark_ready_many(
        tuple(declaration.component_instance_id for declaration in declarations),
        timestamp=0,
    )

    assert len(planner.advance(10)) == 11
    assert planner.work_counters.heap_pops == 11
    assert planner.next_boundary == 11
