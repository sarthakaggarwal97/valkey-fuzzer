"""``redis.fault.process-signal``: signal the server when an event fires, then start it again.

``inject`` subscribes to ``when_event``; the first time it is published the fault sends ``signal`` inside that
event's dispatch, waits for the process to exit, publishes ``<id>.injected``, restarts the server at once, and
publishes ``<id>.recovered`` once it answers. The kill is owned by a ``process.stopped`` obligation from the
moment the signal lands until the restart succeeds, so a restart this fault could not do is the runner's to retry
and the run is ERROR. A fault that never fired is an ERROR too: it judges nothing, and the run did not test what
it claims.
"""

import signal
import threading
from typing import Any, Optional

from valkey_fuzzer.clock import now
from valkey_fuzzer.declarations import Declarations, EvidenceDeclaration
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.registrations import ComponentRegistration
from valkey_fuzzer.results import ComponentStatus, make_evidence, make_result
from valkey_fuzzer.schema import ConfigField, ConfigSchema

from .deployment import INSTANCE_ID
from .workload import deployment

FAULT_INJECTED = "redis.fault.injected"
SIGNALS = {"SIGKILL": signal.SIGKILL, "SIGTERM": signal.SIGTERM}
# How long the process may take to exit after the signal.
EXIT_TIMEOUT_S = 10.0

CONFIG = ConfigSchema(
    {
        "when_event": ConfigField(str),
        "instance": ConfigField(str, default=INSTANCE_ID),
        "signal": ConfigField(str, default="SIGKILL", choices=tuple(SIGNALS)),
        "ready_timeout_s": ConfigField(float, default=30.0, minimum=1, maximum=600),
    }
)

RECORD = ConfigSchema(
    {
        "applied": ConfigField(bool),
        "event": ConfigField(str),
        "event_sequence": ConfigField(int),
        "target": ConfigField(str),
        "signal": ConfigField(str),
        "pid": ConfigField(int, allow_none=True),
        "signal_sent_at": ConfigField(float, required=False),
        "exited": ConfigField(bool, required=False),
        "exited_at": ConfigField(float, required=False),
        "restarted": ConfigField(bool, required=False),
        "ready_after_s": ConfigField(float, required=False),
        "error": ConfigField(str, required=False),
    }
)

DECLARES = Declarations(
    events={
        "{id}.injected": ConfigSchema(
            {
                "applied": ConfigField(bool),
                "trigger": ConfigField(str),
                "signal": ConfigField(str),
                "target": ConfigField(str),
            }
        ),
        "{id}.recovered": ConfigSchema({"instance_id": ConfigField(str)}),
    },
    evidence={FAULT_INJECTED: EvidenceDeclaration(ConfigField(dict, schema=RECORD))},
    changes=("process.stopped",),
)


class ProcessSignalFault:
    """Kill the server on an event and restart it."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._subscription: Any = None
        self._record: Optional[dict[str, Any]] = None
        self._stopped: Any = None
        self._context: Any = None
        self._spec: Optional[ComponentSpec] = None

    def inject(self, context: Any, spec: ComponentSpec):
        self._context, self._spec = context, spec
        self._subscription = context.event_bus.subscribe(spec.config["when_event"], self._on_event)
        return make_result(spec, ComponentStatus.PASS, f"trigger installed on {spec.config['when_event']}")

    def _on_event(self, event: Any) -> None:
        with self._lock:
            if self._record is not None:
                return
            self._record = {"applied": False, "event": event.name, "event_sequence": event.sequence}
        record = self._record
        context, spec, config = self._context, self._spec, self._spec.config
        handle = deployment(context)
        target = config["instance"]
        record.update(target=target, signal=config["signal"], pid=handle.pid(target))
        try:
            handle.send_signal(target, SIGNALS[config["signal"]])
        except ProcessLookupError as exc:
            record["error"] = str(exc)
            return
        record.update(applied=True, signal_sent_at=now())
        # Owned from here: the server is down until the restart below (or the runner, after it) brings it back.
        self._stopped = context.obligations.open(spec, "process.stopped", target, lambda: self._restart(handle, record))
        record["exited"] = handle.wait_exit(target, timeout=EXIT_TIMEOUT_S)
        if record["exited"]:
            record["exited_at"] = now()
        context.event_bus.publish(
            f"{spec.id}.injected",
            source=spec.id,
            payload={"applied": True, "trigger": event.name, "signal": config["signal"], "target": target},
        )
        try:
            self._stopped.discharge()
        except Exception as exc:
            # Kept owned; recovery, and then the runner, retry the restart.
            record["error"] = f"restart failed: {type(exc).__name__}: {exc}"
            return
        context.event_bus.publish(f"{spec.id}.recovered", source=spec.id, payload={"instance_id": target})

    def _restart(self, handle: Any, record: dict[str, Any]) -> None:
        started = now()
        handle.restart(record["target"], ready_timeout=self._spec.config["ready_timeout_s"])
        record.update(restarted=True, ready_after_s=now() - started)

    def recover(self, context: Any, spec: ComponentSpec):
        if self._subscription is not None:
            context.event_bus.unsubscribe(self._subscription)
            self._subscription = None
        record = self._record
        if record is None:
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"fault never triggered: event {spec.config['when_event']!r} was not observed",
            )
        if not record["applied"]:
            return make_result(spec, ComponentStatus.ERROR, f"fault could not be applied: {record.get('error')}")
        if self._stopped is not None:
            try:
                self._stopped.discharge()
            except Exception as exc:
                return make_result(spec, ComponentStatus.ERROR, f"fault recovery failed: {type(exc).__name__}: {exc}")
        evidence = (
            make_evidence(spec, FAULT_INJECTED, "fault.recovered", record, event_sequence=record["event_sequence"]),
        )
        if not record.get("exited"):
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"{record['target']} did not exit within {EXIT_TIMEOUT_S:g}s of {record['signal']}",
                evidence=evidence,
            )
        return make_result(spec, ComponentStatus.PASS, f"fault applied on {record['event']}", evidence=evidence)


REGISTRATIONS = (
    ComponentRegistration(
        "redis.fault.process-signal", ComponentRole.FAULT, ProcessSignalFault, config=CONFIG, declares=DECLARES
    ),
)
