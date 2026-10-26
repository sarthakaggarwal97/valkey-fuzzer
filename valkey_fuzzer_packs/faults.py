"""Event-triggered faults.

Faults arm by subscribing to the ``when_event`` named in their config and fire
synchronously inside that event's dispatch, so placement relative to the
triggering action is replayable rather than sleep-based.  Every fault reports whether, when, and
where it was applied; a required fault that never fired fails the run.
"""

import signal
import threading
import time
from typing import Any, ClassVar, Optional

from valkey_fuzzer.extensions import ComponentStatus

from .common import (
    ComponentConfigError,
    ComponentSpec,
    Field,
    RunPlan,
    config_validator,
    jsonable,
    make_evidence,
    make_result,
    resolved_config,
    select_nodes,
    topology,
    validate_node_selector,
)

FAULT_APPLIED = "valkey.fault.applied"

_SIGNALS = {"SIGKILL": signal.SIGKILL, "SIGTERM": signal.SIGTERM, "SIGSTOP": signal.SIGSTOP}


class _EventFault:
    """Shared arm/fire/cleanup bookkeeping for event-triggered faults."""

    fields: ClassVar[dict] = {}

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._subscription = None
        self._record: Optional[dict[str, Any]] = None
        self._event_sequence: Optional[int] = None
        self._config: dict[str, Any] = {}
        self._context: Any = None
        self._spec: Optional[ComponentSpec] = None
        self._cleaning = False
        self._fire_done = threading.Event()
        self._fire_done.set()

    def arm(self, context: Any, spec: ComponentSpec):
        self._config = resolved_config(spec, self.fields)
        self._context = context
        self._spec = spec
        self._prepare(context, spec)
        self._subscription = context.event_bus.subscribe(self._config["when_event"], self._on_event)
        return make_result(spec, ComponentStatus.PASS, f"armed on {self._config['when_event']}")

    def _prepare(self, context: Any, spec: ComponentSpec) -> None:
        """Resolve targets before the trigger so firing does minimal work."""

    def _on_event(self, event: Any) -> None:
        # Triggers may be published from any thread (workload, timers).  Once
        # cleanup has begun the fault must never fire, and cleanup waits for a
        # firing that is already in progress so nothing is installed after it.
        with self._lock:
            if self._record is not None or self._cleaning:
                return
            self._record = {"status": "firing"}
            self._fire_done.clear()
        try:
            self._fire_once(event)
        finally:
            self._fire_done.set()

    def _fire_once(self, event: Any) -> None:
        if self._config.get("delay_s"):
            time.sleep(self._config["delay_s"])
        fired_at = time.time()
        try:
            record = self._fire(self._context, self._spec, event)
            record.setdefault("applied", True)
        except Exception as exc:
            record = {"applied": False, "error": f"{type(exc).__name__}: {exc}"}
        record.update(
            {
                "event": event.name,
                "event_sequence": event.sequence,
                "applied_at": fired_at,
                "completed_at": time.time(),
            }
        )
        self._event_sequence = event.sequence
        with self._lock:
            self._record = record
        self._context.event_bus.publish(
            "fault.injected",
            source=self._spec.instance_id,
            payload={"applied": record["applied"], "trigger": event.name},
        )

    def _fire(self, context: Any, spec: ComponentSpec, event: Any) -> dict[str, Any]:
        raise NotImplementedError

    def _recover(self, context: Any, spec: ComponentSpec, record: dict[str, Any]) -> None:
        """Undo the fault during cleanup (restart, resume, ...)."""

    def cleanup(self, context: Any, spec: ComponentSpec):
        if self._subscription is not None:
            context.event_bus.unsubscribe(self._subscription)
            self._subscription = None
        with self._lock:
            self._cleaning = True
        if not self._fire_done.wait(timeout=600):
            return make_result(spec, ComponentStatus.ERROR, "fault was still firing when cleanup timed out")
        record = self._record
        if record is None:
            return make_result(
                spec,
                ComponentStatus.FAIL,
                f"fault never triggered: event {self._config['when_event']!r} was not observed",
            )
        recovery_error = None
        try:
            self._recover(context, spec, record)
        except Exception as exc:
            recovery_error = f"{type(exc).__name__}: {exc}"
            record["recovery_error"] = recovery_error
        evidence = ()
        if record.get("applied"):
            evidence = (
                make_evidence(
                    spec,
                    FAULT_APPLIED,
                    "fault.stopped",
                    record,
                    event_sequence=self._event_sequence,
                ),
            )
        if recovery_error is not None:
            return make_result(
                spec, ComponentStatus.ERROR, f"fault recovery failed: {recovery_error}", evidence=evidence
            )
        if not record.get("applied"):
            return make_result(
                spec,
                ComponentStatus.FAIL,
                f"fault could not be applied: {record.get('error')}",
                diagnostics={"record": record},
            )
        return make_result(spec, ComponentStatus.PASS, f"fault applied on {record.get('event')}", evidence=evidence)


# ---------------------------------------------------------------------------
# valkey.process_signal
# ---------------------------------------------------------------------------

_SIGNAL_FIELDS = {
    "node": Field(str, default="primary"),
    "signal": Field(str, default="SIGKILL", choices=tuple(_SIGNALS)),
    "delay_s": Field(float, default=0.0, minimum=0, maximum=600),
    "restart": Field(bool, default=True),
    "restart_mode": Field(str, default="cleanup", choices=("cleanup", "immediate")),
    "restart_delay_s": Field(float, default=0.0, minimum=0, maximum=600),
    "pause_s": Field(float, default=1.0, minimum=0, maximum=600),
    "ready_timeout_s": Field(float, default=60.0, minimum=1, maximum=600),
}


def _validate_signal(config: dict, spec: ComponentSpec, plan: RunPlan) -> None:
    validate_node_selector(config["node"], "node")


class ProcessSignalFault(_EventFault):
    fields = _SIGNAL_FIELDS

    def _prepare(self, context: Any, spec: ComponentSpec) -> None:
        handle = topology(context)
        self._target = select_nodes(handle, self._config["node"])[0]

    def _fire(self, context: Any, spec: ComponentSpec, event: Any) -> dict[str, Any]:
        handle = topology(context)
        name = self._config["signal"]
        record: dict[str, Any] = {"target": self._target, "signal": name, "pid": handle.pid(self._target)}
        if name in ("SIGKILL", "SIGTERM"):
            try:
                handle.send_signal(self._target, _SIGNALS[name])
                record["applied"] = True
            except ProcessLookupError as exc:
                record["applied"] = False
                record["error"] = str(exc)
            if record["applied"]:
                record["exited"] = handle.wait_exit(self._target, timeout=30)
                if self._config["restart"] and self._config["restart_mode"] == "immediate":
                    # Recovery is part of the fault here so later actions in the
                    # plan observe the restarted node.
                    time.sleep(self._config["restart_delay_s"])
                    self._restart(handle, record)
                    context.event_bus.publish("node.recovered", source=spec.instance_id, payload={"node": self._target})
        else:
            handle.send_signal(self._target, _SIGNALS[name])
            record["applied"] = True
            time.sleep(self._config["pause_s"])
            handle.send_signal(self._target, signal.SIGCONT)
            record["resumed"] = True
        return record

    def _restart(self, handle: Any, record: dict[str, Any]) -> None:
        started = time.time()
        handle.restart(self._target, wait_ready=True)
        record["restarted"] = True
        record["restart_seconds"] = round(time.time() - started, 3)
        record["restart_pid"] = handle.pid(self._target)

    def _recover(self, context: Any, spec: ComponentSpec, record: dict[str, Any]) -> None:
        handle = topology(context)
        if record.get("signal") in ("SIGKILL", "SIGTERM") and self._config["restart"]:
            if not handle.is_alive(self._target):
                self._restart(handle, record)


# ---------------------------------------------------------------------------
# valkey.command_fault
# ---------------------------------------------------------------------------

_COMMAND_FAULT_FIELDS = {
    "node": Field(str, default="primary"),
    "command": Field(list),
    "delay_s": Field(float, default=0.0, minimum=0, maximum=600),
    "expect_error": Field(bool, default=False),
    "undo": Field(list, allow_none=True, default=None),
}


def _validate_command_fault(config: dict, spec: ComponentSpec, plan: RunPlan) -> None:
    validate_node_selector(config["node"], "node")
    for name in ("command", "undo"):
        value = config[name]
        if value is not None and (not value or not all(isinstance(item, (str, int, float)) for item in value)):
            raise ComponentConfigError(f"{name} must be a non-empty list of strings or numbers")


class CommandFault(_EventFault):
    """Run a command (e.g. BGSAVE CANCEL, CLIENT KILL) at a lifecycle event.

    ``node`` may select several nodes (e.g. ``primaries``); the fault counts
    as applied only if the command behaved as expected on every target.
    """

    fields = _COMMAND_FAULT_FIELDS

    def _prepare(self, context: Any, spec: ComponentSpec) -> None:
        self._targets = select_nodes(topology(context), self._config["node"])

    def _fire(self, context: Any, spec: ComponentSpec, event: Any) -> dict[str, Any]:
        handle = topology(context)
        replies = {}
        applied = True
        for target in self._targets:
            client = handle.new_client(target, timeout=10.0)
            try:
                replies[target] = {
                    "reply": jsonable(client.execute_command(*[str(item) for item in self._config["command"]]))
                }
                applied = applied and not self._config["expect_error"]
            except Exception as exc:
                replies[target] = {"error": f"{type(exc).__name__}: {exc}"}
                applied = applied and self._config["expect_error"]
            finally:
                client.close()
        record: dict[str, Any] = {
            "targets": self._targets,
            "command": self._config["command"],
            "replies": replies,
            "applied": applied,
        }
        if not applied:
            record["error"] = f"unexpected replies: {replies}"
        return record

    def _recover(self, context: Any, spec: ComponentSpec, record: dict[str, Any]) -> None:
        if self._config["undo"]:
            undo = {}
            for target in self._targets:
                client = topology(context).new_client(target, timeout=10.0)
                try:
                    undo[target] = jsonable(client.execute_command(*[str(item) for item in self._config["undo"]]))
                finally:
                    client.close()
            record["undo_replies"] = undo


REGISTRATIONS = (
    (
        "valkey.process_signal",
        "fault",
        ProcessSignalFault,
        config_validator(_SIGNAL_FIELDS, extra=_validate_signal, requires_event=True),
    ),
    (
        "valkey.command_fault",
        "fault",
        CommandFault,
        config_validator(_COMMAND_FAULT_FIELDS, extra=_validate_command_fault, requires_event=True),
    ),
)
