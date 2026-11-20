"""Event-triggered faults.

``inject`` installs a fault's trigger: it subscribes to the ``when_event``
named in its config (often ``<action>.completed`` or ``<action>.save-started``)
and injects the fault synchronously inside that event's dispatch, so placement
relative to the triggering action is replayable rather than sleep-based.
``occurrence`` fires on the nth event that passes the ``when_payload`` filter,
and ``instance: event`` targets the instance named in the triggering event's
payload (for example the instance whose log line a
``valkey.observer.log-trigger`` observer matched). Every fault reports whether,
when, and where it was applied, publishes ``<id>.injected``, and ``recover``
undoes it and fails if it never fired; one that never fires, cannot be applied, or
refuses to fire because firing would be unsafe (:class:`FaultRefusedError`)
is an ERROR: a fault never judges Valkey.

Each change a fault applies is owned by an undo obligation it opens as it
applies the change (:meth:`EventFault._own`); undoing early (a heal or
release event, a pause that ends, an immediate restart) discharges it, and
``recover`` discharges whatever is left. What a fault cannot undo the runner
undoes, and the run is ERROR.

A fault that holds one change from its trigger until a release (a skewed
clock, a cgroup limit, a cut or degraded link) is a :class:`HeldFault`: the
release, on the event its config names, after ``duration_s``, or at recovery,
and the record of when and why it happened are written once there.
"""

import signal
import threading
from collections.abc import Mapping
from typing import Any, Optional

from valkey_fuzzer._data import jsonable
from valkey_fuzzer.clock import now
from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.results import ComponentStatus, make_evidence, make_result
from valkey_fuzzer.schema import ConfigField, ConfigSchema, plain_config

from .instances import EXPECTED_DOWN, deployment, select_instance
from .polling import remaining
from .predicates import payload_matches
from .processes import (
    KILL_EXIT_TIMEOUT_S,
    KILL_FIELDS,
    KILL_RECORD_FIELDS,
    check_cluster_vars,
    pause_owned,
    record_restart,
    wait_killed,
)
from .registration import component_registration
from .schemas import FLAG, INTEGER, MAPPING, NUMBER, TEXT, declared, mapping_of, nullable, optional

FAULT_INJECTED = "valkey.fault.injected"
# ``instance: event`` targets the instance named by the triggering event's ``instance_id`` field.
EVENT_TARGET = "event"
# Record fields copied into the <id>.injected event, so a trace of the
# event stream alone says where a fault landed and what state it left on disk.
_EVENT_RECORD_KEYS = ("target", "targets", "signal", "disk_vars_at_kill")

_SIGNALS = {"SIGKILL": signal.SIGKILL, "SIGTERM": signal.SIGTERM, "SIGSTOP": signal.SIGSTOP}
# How long recovery waits, at most, for a firing still in progress.
_FIRE_WAIT_S = 600.0

# The <id>.injected payload: whether and when it fired, and the record fields
# _EVENT_RECORD_KEYS copies.
INJECTED_PAYLOAD = ConfigSchema(
    {
        "applied": FLAG,
        "trigger": TEXT,
        "applied_at": NUMBER,
        "target": optional(str),
        "targets": optional(list),
        "signal": optional(str),
        "disk_vars_at_kill": optional(dict, allow_none=True),
    }
)
# Every fault record: the trigger it fired on, and how its recovery went.
_FIRED = {
    "applied": FLAG,
    "event": TEXT,
    "event_sequence": INTEGER,
    "occurrence": INTEGER,
    "trigger_payload": MAPPING,
    "trigger_received_at": NUMBER,
    "applied_at": NUMBER,
    "completed_at": NUMBER,
    "error": optional(str),
    "refused": optional(bool),
    # The undos that failed at recovery; the obligations stay open for the runner.
    "recovery_error": optional(str),
}


def fault_declares(
    fields: Mapping[str, ConfigField],
    events: Optional[Mapping[str, ConfigSchema]] = None,
    changes: tuple[str, ...] = (),
    evidence: Optional[Mapping[str, Any]] = None,
) -> Declarations:
    """A fault's declarations: ``<id>.injected``, ``valkey.fault.injected`` with its own record ``fields``, and
    any further ``evidence`` (:meth:`EventFault._evidence`)."""

    return Declarations(
        events={"{id}.injected": INJECTED_PAYLOAD, **(events or {})},
        evidence={FAULT_INJECTED: declared(mapping_of({**_FIRED, **fields})), **(evidence or {})},
        changes=changes,
    )


# What a fault that kills an instance declares besides its record: the event of a restart that started it again,
# and the evidence of a kill without restart (:meth:`EventFault._expected_down`).
KILL_DECLARES = {
    "events": {"{id}.recovered": ConfigSchema({"instance_id": TEXT})},
    "evidence": {EXPECTED_DOWN: declared(mapping_of({"instances": ConfigField(list, items=TEXT)}))},
}

# What a held fault on one instance records of its release (:meth:`HeldFault._settled`, under the default field
# names) and publishes with ``<id>.released``; its own ``seconds_field`` goes beside them.
RELEASED_FIELDS = {
    "released_at": optional(float),
    "release_reason": optional(str),
    "release_error": optional(str),
}
RELEASED_EVENT = ConfigSchema({"instance_id": TEXT, "reason": TEXT})


class FaultRefusedError(Exception):
    """Raised by ``_fire`` when firing would be unsafe; the fault reports ERROR, not FAIL."""


class EventFault:
    """Shared inject/fire/recover bookkeeping for event-triggered faults."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._subscription = None
        # Further subscriptions (release or heal events), ended when the fault recovers.
        self._subscriptions: list[Any] = []
        self._record: Optional[dict[str, Any]] = None
        self._event_sequence: Optional[int] = None
        self._config: Mapping[str, Any] = {}
        self._context: Any = None
        self._spec: Optional[ComponentSpec] = None
        self._recovering = False
        self._matches = 0
        self._fire_done = threading.Event()
        self._fire_done.set()
        # The changes this fault applied, oldest first.
        self._obligations: list[Any] = []
        # The instance a fault on one instance acts on (:meth:`_select_target`, :meth:`_target_instance`).
        self._target: Optional[str] = None

    def inject(self, context: Any, spec: ComponentSpec):
        self._config = spec.config
        self._context = context
        self._spec = spec
        self._prepare(context, spec)
        self._subscription = context.event_bus.subscribe(self._config["when_event"], self._on_event)
        return make_result(spec, ComponentStatus.PASS, f"trigger installed on {self._config['when_event']}")

    def _prepare(self, context: Any, spec: ComponentSpec) -> None:
        """Resolve targets before the trigger so firing does minimal work."""

    def _select_target(self, context: Any) -> None:
        """Resolve the ``instance`` the fault acts on before the trigger, unless it is the triggering event's
        (``instance: event``), which :meth:`_target_instance` reads from the event when the fault fires."""

        if self._config["instance"] != EVENT_TARGET:
            self._target = select_instance(deployment(context), self._config["instance"])

    def _target_instance(self, handle: Any, event: Any) -> str:
        """The instance the fault acts on: the one selected before the trigger, else the one ``event`` names."""

        if self._target is None:
            self._target = self._event_target(handle, event)
        return self._target

    def _own(self, kind: str, target: str, undo: Any) -> Any:
        """Open the obligation for a change of ``kind`` to ``target`` just applied, undone by ``undo``."""

        obligation = self._context.obligations.open(self._spec, kind, target, undo)
        self._obligations.append(obligation)
        return obligation

    def _undo_early(self, obligation: Any, undone: str, payload: Mapping[str, Any]) -> Optional[str]:
        """Undo ``obligation``'s change before recovery and publish ``<id>.<undone>`` with ``payload``; the error
        when the undo failed, else None.

        It runs in an event's dispatch or on a timer thread, so it never raises: a change it could not undo
        stays owned, and recovery retries the undo and reports it.
        """

        try:
            obligation.discharge()
        except Exception as exc:
            return f"{type(exc).__name__}: {exc}"
        self._context.event_bus.publish(f"{self._spec.id}.{undone}", source=self._spec.id, payload=payload)
        return None

    def _subscribe(self, event: str, callback: Any) -> None:
        """Subscribe ``callback`` to ``event`` until the fault recovers."""

        self._subscriptions.append(self._context.event_bus.subscribe(event, callback))

    def _on_event(self, event: Any) -> None:
        # Triggers may be published from any thread (workload, timers).  Once
        # recovery has begun the fault must never fire, and recovery waits for a
        # firing that is already in progress so nothing is installed after it.
        received_at = now()
        with self._lock:
            if self._record is not None or self._recovering:
                return
            if not payload_matches(event.payload, self._config["when_payload"]):
                return
            self._matches += 1
            if self._matches < self._config["occurrence"]:
                return
            self._record = {"status": "firing"}
            self._fire_done.clear()
        try:
            self._fire_once(event, received_at)
        except BaseException as exc:
            # An interrupt (Ctrl-C) in the dispatch: recovery reads a record, not a firing that never ends.
            with self._lock:
                if self._record == {"status": "firing"}:
                    self._record = {
                        "applied": False,
                        "error": f"interrupted while firing: {type(exc).__name__}: {exc}",
                        "event": event.name,
                        "event_sequence": event.sequence,
                        "occurrence": self._matches,
                    }
            raise
        finally:
            self._fire_done.set()

    def _fire_once(self, event: Any, received_at: float) -> None:
        delayed = not self._config["delay_s"] or self._context.sleep(self._config["delay_s"])
        fired_at = now()
        try:
            if not delayed:
                record = {"applied": False, "error": "cancelled during delay_s, before the fault fired"}
            else:
                record = self._fire(self._context, self._spec, event)
                record.setdefault("applied", True)
        except FaultRefusedError as exc:
            record = {"applied": False, "refused": True, "error": str(exc)}
        except Exception as exc:
            record = {"applied": False, "error": f"{type(exc).__name__}: {exc}"}
        record.update(
            {
                "event": event.name,
                "event_sequence": event.sequence,
                "occurrence": self._matches,
                "trigger_payload": jsonable(event.payload),
                "trigger_received_at": received_at,
                "applied_at": fired_at,
                "completed_at": now(),
            }
        )
        self._event_sequence = event.sequence
        with self._lock:
            self._record = record
        if not record.pop("lands_later", False):
            self._announce(record)
        self._after_fire(self._context, self._spec, record)

    def _announce(self, record: dict[str, Any]) -> None:
        """Publish ``<id>.injected`` for ``record``. A fault whose ``_fire`` only arms it (its record has
        ``lands_later``) calls this itself once the fault took effect, with ``applied_at`` set to that time."""

        payload = {"applied": record["applied"], "trigger": record["event"], "applied_at": record["applied_at"]}
        payload.update({key: record[key] for key in _EVENT_RECORD_KEYS if key in record})
        self._context.event_bus.publish(f"{self._spec.id}.injected", source=self._spec.id, payload=payload)

    def _after_fire(self, context: Any, spec: ComponentSpec, record: dict[str, Any]) -> None:
        """Follow-up work that must come after ``<id>.injected`` (a restart)."""

    def _fire(self, context: Any, spec: ComponentSpec, event: Any) -> dict[str, Any]:
        raise NotImplementedError

    def _event_target(self, handle: Any, event: Any) -> str:
        instance_id = event.payload.get("instance_id")
        if instance_id not in {instance.instance_id for instance in handle.instances}:
            raise ValueError(
                f"trigger event {event.name!r} names no known instance (payload instance_id={instance_id!r})"
            )
        return instance_id

    def _quiesce(self) -> None:
        """Stop the fault's own workers (timers, restarters) before recovery undoes its changes, so none of
        them acts or publishes once teardown began."""

    def _settled(self, context: Any, spec: ComponentSpec, record: dict[str, Any]) -> None:
        """Bookkeeping once every change is undone: record when it ended; raise to report an ERROR."""

    def _evidence(self, spec: ComponentSpec, record: dict[str, Any]) -> tuple:
        """Evidence an applied fault records besides ``valkey.fault.injected``."""
        return ()

    def _expected_down(self, spec: ComponentSpec, record: dict[str, Any]) -> tuple:
        """The evidence of a kill without restart: its target is down on purpose until the deployment is removed,
        so ``valkey.oracle.liveness`` and the data oracles skip it."""

        return (make_evidence(spec, EXPECTED_DOWN, "fault.recovered", {"instances": [record["target"]]}),)

    def _restart_after_kill(self, stopped: Any, record: dict[str, Any]) -> None:
        """Restart the killed target after ``restart_delay_s``: discharge ``stopped``, the kill's obligation, whose
        undo starts the instance again and records it (:func:`record_restart`), then publish ``<id>.recovered``.

        It runs after ``<id>.injected``, on the trigger's dispatch or a worker thread. A cancelled delay leaves the
        restart to recovery; a restart that raised is recorded as ``restart_error`` and stays owned, so recovery
        retries it and reports it.
        """

        if not self._context.sleep(self._config["restart_delay_s"]):
            return
        try:
            stopped.discharge()
        except Exception as exc:
            record["restart_error"] = f"{type(exc).__name__}: {exc}"
            return
        if record.get("restarted"):
            self._context.event_bus.publish(
                f"{self._spec.id}.recovered", source=self._spec.id, payload={"instance_id": self._target}
            )

    def recover(self, context: Any, spec: ComponentSpec):
        if self._subscription is not None:
            context.event_bus.unsubscribe(self._subscription)
            self._subscription = None
        with self._lock:
            self._recovering = True
        # A firing in progress ends within its own call's deadline; past this call's, recovery cannot wait for it.
        if not self._fire_done.wait(timeout=remaining(context, _FIRE_WAIT_S)):
            return make_result(spec, ComponentStatus.ERROR, "fault was still firing when recovery timed out")
        for subscription in self._subscriptions:
            context.event_bus.unsubscribe(subscription)
        self._subscriptions.clear()
        self._quiesce()
        errors = []
        for obligation in reversed(self._obligations):
            try:
                obligation.discharge()
            except Exception as exc:
                errors.append(f"undo of {obligation.kind} on {obligation.target} failed: {type(exc).__name__}: {exc}")
            else:
                if obligation.errors:
                    # Undone on a retry: the change outlasted the fault's own undo.
                    first = obligation.errors[0]
                    errors.append(
                        f"undo of {obligation.kind} on {obligation.target} failed before a retry succeeded: "
                        f"{first['type']}: {first.get('message', '')}"
                    )
        record = self._record
        if record is None:
            seen = self._matches
            detail = (
                "was not observed"
                if not seen
                else f"matched {seen} time(s), occurrence {self._config['occurrence']} needed"
            )
            if self._config["when_payload"]:
                detail += f" (payload filter {plain_config(self._config['when_payload'])})"
            # A fault does not judge Valkey: one that never fired leaves the run untested.
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"fault never triggered: event {self._config['when_event']!r} {detail}",
            )
        try:
            self._settled(context, spec, record)
        except Exception as exc:
            errors.append(f"{type(exc).__name__}: {exc}")
        recovery_error = "; ".join(errors) or None
        if recovery_error is not None:
            record["recovery_error"] = recovery_error
        evidence = ()
        if record.get("applied"):
            evidence = (
                make_evidence(
                    spec,
                    FAULT_INJECTED,
                    "fault.recovered",
                    record,
                    event_sequence=self._event_sequence,
                ),
                *self._evidence(spec, record),
            )
        if recovery_error is not None:
            return make_result(
                spec, ComponentStatus.ERROR, f"fault recovery failed: {recovery_error}", evidence=evidence
            )
        if record.get("refused"):
            return make_result(
                spec, ComponentStatus.ERROR, f"fault refused to fire: {record['error']}", diagnostics={"record": record}
            )
        if not record.get("applied"):
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"fault could not be applied: {record.get('error')}",
                diagnostics={"record": record},
            )
        return make_result(spec, ComponentStatus.PASS, f"fault applied on {record.get('event')}", evidence=evidence)


class HeldFault(EventFault):
    """A fault that holds one change from its trigger until its release: the event its config names
    (``release_event``, or ``heal_event`` for the link faults), ``duration_s`` where the fault has it, or recovery.

    Subclasses resolve their targets in ``_resolve``, apply the change in ``_install`` (opening its obligation with
    ``_own`` as ``self._change`` as soon as the change exists, so recovery undoes what no release did), and say what
    the ``<id>.<released_event>`` payload is and what a failed release reports. A release before recovery discharges
    the obligation and then publishes that event; recovery discharges it and publishes nothing. The record says
    when the change was released, why (``event``, ``duration``, or ``recover``), and for how long it held, under
    the field names the class attributes give.
    """

    # The config field naming the release event, the event published on an early release, and the record fields
    # for when the change was released, why, and for how long it held.
    release_field = "release_event"
    released_event = "released"
    released_at_field = "released_at"
    reason_field = "release_reason"
    seconds_field = ""
    # What the ERROR of a failed release says before the error itself ("restoring the clock failed, so the skew
    # lasted until recovery"), for the faults that take the default :meth:`_release_failed`.
    release_failed_text = ""

    def _prepare(self, context: Any, spec: ComponentSpec) -> None:
        # Reentrant: a subscriber of <id>.released, which is published under it, may release again.
        self._lock_release = threading.RLock()
        # Set once recovery begins: a late timer or release event no longer acts.
        self._closed = False
        # The change's obligation, from the moment the change exists; installed once it holds whole.
        self._change: Any = None
        self._installed = False
        self._released_at: Optional[float] = None
        self._release_reason: Optional[str] = None
        self._release_error: Optional[str] = None
        self._timer: Optional[threading.Timer] = None
        self._resolve(context, spec)
        if self._config[self.release_field]:
            self._subscribe(self._config[self.release_field], self._release_on_event)

    def _resolve(self, context: Any, spec: ComponentSpec) -> None:
        """Resolve targets before the trigger so firing does minimal work."""

    def _install(self, event: Any) -> dict[str, Any]:
        """Apply the change at the trigger ``event``, opening its obligation as ``self._change`` as soon as the
        change exists; the record fields."""

        raise NotImplementedError

    def _own_change(self, kind: str, target: str, undo: Any) -> Any:
        """Open the obligation of the fault's one change (an ``Own``): :meth:`_own`, kept as ``self._change``."""

        self._change = self._own(kind, target, undo)
        return self._change

    def _released_payload(self, reason: str) -> dict[str, Any]:
        """The ``<id>.<released_event>`` payload of a release for ``reason``: a fault on one instance names it
        (:data:`RELEASED_EVENT`); the others say what they released."""

        return {"instance_id": self._target, "reason": reason}

    def _release_failed(self, record: dict[str, Any], error: str) -> str:
        """Record ``error``, the release that failed, on ``record``; what the ERROR says: the change lasted until
        recovery."""

        record["release_error"] = error
        return f"{self.release_failed_text}: {error}"

    def _fire(self, context: Any, spec: ComponentSpec, event: Any) -> dict[str, Any]:
        record = self._install(event)
        self._installed = True
        self._release_later()
        return {"applied": True, **record}

    def _release_later(self) -> None:
        """Arm the ``duration_s`` release, where the fault's config has one."""

        duration = self._config.get("duration_s")
        if duration is None:
            return
        self._timer = threading.Timer(duration, self._release, kwargs={"reason": "duration"})
        self._timer.daemon = True
        self._timer.start()

    def _release(self, reason: str) -> None:
        with self._lock_release:
            if self._closed or not self._installed or self._released_at is not None:
                return
            error = self._undo_early(self._change, self.released_event, self._released_payload(reason))
            if error:
                self._release_error = error
                return
            self._released_at = now()
            self._release_reason = reason

    def _release_on_event(self, event: Any) -> None:
        self._release("event")

    def _quiesce(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
        # Waits for a release in progress; recovery undoes the change from here on, and publishes nothing.
        with self._lock_release:
            self._closed = True

    def _settled(self, context: Any, spec: ComponentSpec, record: dict[str, Any]) -> None:
        with self._lock_release:
            if self._installed and self._released_at is None and not self._change.open:
                self._released_at = now()
                self._release_reason = "recover"
        if self._release_error:
            raise RuntimeError(self._release_failed(record, self._release_error))
        if self._released_at is not None:
            record[self.released_at_field] = self._released_at
            record[self.reason_field] = self._release_reason
            record[self.seconds_field] = round(self._released_at - record.get("applied_at", self._released_at), 3)


# ---------------------------------------------------------------------------
# valkey.fault.process-signal
# ---------------------------------------------------------------------------

_SIGNAL_FIELDS = {
    "instance": ConfigField(str, default="primary"),
    "signal": ConfigField(str, default="SIGKILL", choices=tuple(_SIGNALS)),
    **KILL_FIELDS,
    # immediate: restart inside the trigger's dispatch; background: kill inside
    # it and restart on a worker so the publisher (the event bus) is not held up;
    # recover: restart when the fault recovers at teardown.
    "restart_mode": ConfigField(str, default="recover", choices=("recover", "immediate", "background")),
    "pause_s": ConfigField(float, default=1.0, minimum=0, maximum=600),
}


class ProcessSignalFault(EventFault):
    """Signal one instance at a lifecycle event: ``SIGKILL`` or ``SIGTERM`` (then restart it), or ``SIGSTOP``
    for ``pause_s``.

    The kill or pause lands inside the event's dispatch, so it hits the moment the event names (a vote, a save
    in progress) rather than whenever a sleep ends.
    """

    def _prepare(self, context: Any, spec: ComponentSpec) -> None:
        self._restarter: Optional[threading.Thread] = None
        self._restarter_stuck = False
        # The kill's obligation, discharged by the restart.
        self._stopped: Any = None
        self._select_target(context)

    def _fire(self, context: Any, spec: ComponentSpec, event: Any) -> dict[str, Any]:
        handle = deployment(context)
        self._target_instance(handle, event)
        name = self._config["signal"]
        record: dict[str, Any] = {"target": self._target, "signal": name, "pid": handle.pid(self._target)}
        if name in ("SIGKILL", "SIGTERM"):
            try:
                handle.send_signal(self._target, _SIGNALS[name])
                record["signal_sent_at"] = now()
                record["applied"] = True
            except ProcessLookupError as exc:
                record["applied"] = False
                record["error"] = str(exc)
            if record["applied"] and self._config["restart"]:
                # A kill without restart lasts until the deployment is removed.
                self._stopped = self._own(
                    "process.stopped", self._target, lambda: self._restart_if_down(handle, record)
                )
            if record["applied"]:
                record.update(
                    wait_killed(handle, self._target, _SIGNALS[name], cluster_vars=self._config["capture_cluster_vars"])
                )
                if not record["exited"]:
                    # The kill did not take effect: the fault was not applied, and there is nothing to restart.
                    record["applied"] = False
                    record["error"] = f"{self._target} did not exit within {KILL_EXIT_TIMEOUT_S:g}s of {name}"
                elif self._config["restart"] and self._config["restart_mode"] in ("immediate", "background"):
                    # Restart in _after_fire, so <id>.injected comes before <id>.recovered.
                    record["restart_pending"] = True
        else:
            paused = pause_owned(self._own, handle, self._target)
            record["applied"] = True
            try:
                context.sleep(self._config["pause_s"])
            finally:
                # Whatever interrupted the pause, never leave the instance
                # stopped; if this fails, recovery tries again.
                try:
                    paused.discharge()
                    record["resumed"] = True
                except Exception:
                    record["resumed"] = False
        return record

    def _evidence(self, spec: ComponentSpec, record: dict[str, Any]) -> tuple:
        if record["signal"] == "SIGSTOP" or self._config["restart"]:
            return ()
        return self._expected_down(spec, record)

    def _restart_if_down(self, handle: Any, record: dict[str, Any]) -> None:
        if not handle.is_alive(self._target):
            self._restart(handle, record)

    def _after_fire(self, context: Any, spec: ComponentSpec, record: dict[str, Any]) -> None:
        if not record.pop("restart_pending", False):
            return
        if self._config["restart_mode"] == "immediate":
            # Recovery is part of the fault here so later actions in the plan
            # observe the restarted instance. A failed restart keeps the kill
            # owned; recovery retries it.
            self._restart_after_kill(self._stopped, record)
            return
        # Restart off the dispatch thread so the publisher (the event bus) is not held up.
        self._restarter = threading.Thread(
            target=self._restart_after_kill,
            args=(self._stopped, record),
            name=f"restart-{spec.id}",
            daemon=True,
        )
        self._restarter.start()

    def _restart(self, handle: Any, record: dict[str, Any]) -> None:
        record_restart(
            handle,
            self._target,
            record,
            lambda: handle.restart(self._target, wait_ready=True, ready_timeout=self._config["ready_timeout_s"]),
        )

    def _quiesce(self) -> None:
        if self._restarter is not None:
            # The background restart's delay and readiness wait, plus a minute to stop the old process and start
            # the new one: recovery then finds the instance restarted after restart_delay_s, as planned.
            self._restarter.join(timeout=self._config["restart_delay_s"] + self._config["ready_timeout_s"] + 60)
            self._restarter_stuck = self._restarter.is_alive()

    def _settled(self, context: Any, spec: ComponentSpec, record: dict[str, Any]) -> None:
        if self._restarter_stuck:
            raise RuntimeError("the background restart was still running when recovery restarted the instance")


_SIGNAL_DECLARES = fault_declares(
    {
        "target": TEXT,
        "signal": TEXT,
        "pid": nullable(int),
        "signal_sent_at": optional(float),
        **KILL_RECORD_FIELDS,
        "resumed": optional(bool),
    },
    changes=("process.stopped", "process.paused"),
    **KILL_DECLARES,
)

REGISTRATIONS = (
    component_registration(
        "valkey.fault.process-signal",
        ProcessSignalFault,
        _SIGNAL_FIELDS,
        check=check_cluster_vars,
        selectors={"instance": (EVENT_TARGET,)},
        triggered=True,
        declares=_SIGNAL_DECLARES,
    ),
)
