"""``valkey.fault.syscall``: kill, delay, or fail one instance at the nth matching system call.

A kill from outside lands wherever the process happens to be; one at a system call lands between two calls of one
event-loop pass, or between the writes and the rename of a save. When its trigger fires, the fault arms a rule
for the instance's syscall fault library (``deployment/local/syscall_faults.c``, loaded by every server of a
deployment with ``syscall_faults: true``); the library counts the matching calls of the server and its fork
children from then on and acts on the nth. The fault lands then, not at its trigger: ``<id>.injected`` and the
record's ``applied_at`` are the time the library acted. A kill takes the server down (with the fork child that
made the call), and the fault restarts it after ``restart_delay_s``, publishing ``<id>.recovered``, as
``valkey.fault.process-signal`` does. A rule that never acted by recovery is removed, and the fault is an ERROR:
the run did not test what it meant to.
"""

import errno
import json
import os
import signal
import threading
import time
from typing import Any, Optional

from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.results import make_evidence
from valkey_fuzzer.schema import ConfigError, ConfigField, ConfigSchema, plain_config

from .faults import EVENT_TARGET, EventFault, fault_declares
from .instances import EXPECTED_DOWN, deployment, select_instance, select_instances
from .processes import KILL_EXIT_TIMEOUT_S, restart_once, wait_killed
from .registration import component_registration
from .schemas import NUMBER, TEXT, declared, mapping_of, nullable, optional

CALLS = ("write", "fsync", "fdatasync", "rename")
ACTIONS = ("kill", "delay", "fail")
# How often the fault looks for its rule's action.
_POLL_S = 0.002

_FIELDS = {
    "instance": ConfigField(str, default="primary"),
    "calls": ConfigField(list, items=ConfigField(str, choices=CALLS)),
    # fnmatch pattern of the file's path; relative patterns are under the instance's data directory.
    "path": ConfigField(str, allow_none=True, default=None),
    # Sockets connected to these instances' client or cluster bus port.
    "peer": ConfigField(str, allow_none=True, default=None),
    "nth": ConfigField(int, default=1, minimum=1, maximum=10_000_000),
    "action": ConfigField(str, default="kill", choices=ACTIONS),
    "when": ConfigField(str, default="before", choices=("before", "after")),
    "delay_ms": ConfigField(int, default=1000, minimum=1, maximum=600_000),
    "errno": ConfigField(str, default="EIO"),
    "restart": ConfigField(bool, default=True),
    "restart_delay_s": ConfigField(float, default=0.0, minimum=0, maximum=600),
    "ready_timeout_s": ConfigField(float, default=30.0, minimum=1, maximum=600),
    "capture_cluster_vars": ConfigField(bool, default=False),
}


def _validate(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    if not plain_config(plan.deployment).get("syscall_faults"):
        raise ConfigError("valkey.fault.syscall requires deployment.syscall_faults: true")
    if not config["calls"]:
        raise ConfigError("calls must name at least one of " + ", ".join(CALLS))
    if config["peer"] is not None and set(config["calls"]) - {"write"}:
        raise ConfigError("peer matches sockets, which only write")
    if not isinstance(getattr(errno, config["errno"], None), int):
        raise ConfigError(f"errno {config['errno']!r} is not an errno name")
    if config["capture_cluster_vars"] and plan.deployment_mode != "cluster":
        raise ConfigError("capture_cluster_vars needs a cluster deployment")
    if any(char.isspace() for char in config["path"] or ""):
        raise ConfigError("path must not contain whitespace")


def rule_text(config: Any, path: Optional[str], ports: list[int]) -> str:
    """The library's rule for ``config``, its path pattern made absolute, its peer resolved to ``ports``."""

    fields = [f"calls={','.join(config['calls'])}", f"nth={config['nth']}", f"action={config['action']}"]
    fields.append(f"when={config['when']}")
    if path:
        fields.append(f"path={path}")
    if ports:
        fields.append(f"peer_ports={','.join(map(str, ports))}")
    if config["action"] == "delay":
        fields.append(f"delay_ms={config['delay_ms']}")
    if config["action"] == "fail":
        fields.append(f"errno={getattr(errno, config['errno'])}")
    return " ".join(fields) + "\n"


def read_calls(log_path: str) -> list[dict[str, Any]]:
    """The calls the library logged for a rule: every matching call up to the nth, which names its action."""

    try:
        with open(log_path, encoding="utf-8", errors="replace") as log:
            lines = log.read().splitlines()
    except FileNotFoundError:
        return []
    calls = []
    for line in lines:
        try:
            calls.append(json.loads(line))
        except ValueError:
            continue  # A line cut short by the kill it logged.
    return calls


class SyscallFault(EventFault):
    """Arm a syscall rule on one instance at the trigger; it lands at the nth matching call (see the module)."""

    def _prepare(self, context: Any, spec: ComponentSpec) -> None:
        handle = deployment(context)
        self._target: Optional[str] = None
        self._watcher: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._watched: Optional[tuple[Any, str, str]] = None
        self._ports: list[int] = []
        if self._config["peer"] is not None:
            peers = set(select_instances(handle, self._config["peer"]))
            if not peers:
                raise LookupError(f"no live instance matches peer {self._config['peer']!r}")
            for instance in handle.instances:
                if instance.instance_id in peers:
                    self._ports += [instance.port, instance.bus_port]
        if self._config["instance"] != EVENT_TARGET:
            self._target = select_instance(handle, self._config["instance"])

    def _files(self, handle: Any) -> tuple[str, str, str]:
        rules = handle.syscall_fault_dir(self._target)
        if rules is None:
            raise RuntimeError("the deployment does not load the syscall fault library (syscall_faults)")
        base = os.path.join(rules, self._spec.id)
        return f"{base}.rule", f"{base}.fired", f"{base}.log"

    def _fire(self, context: Any, spec: ComponentSpec, event: Any) -> dict[str, Any]:
        handle = deployment(context)
        if self._target is None:
            self._target = self._event_target(handle, event)
        rule, fired, log = self._files(handle)
        path = self._config["path"]
        if path and not os.path.isabs(path):
            path = os.path.join(handle.data_dir(self._target), path)
        text = rule_text(self._config, path, self._ports)
        partial = f"{rule}.tmp"
        with open(partial, "w", encoding="utf-8") as out:
            out.write(text)
        # Owned from before it exists: an undo of a rule that is not there does nothing.
        self._own("syscall.rule", self._target, lambda: _remove(rule))
        os.replace(partial, rule)
        record: dict[str, Any] = {
            "target": self._target,
            "rule": text.strip(),
            "armed_at": time.time(),
            "pid": handle.pid(self._target),
            "lands_later": True,
        }
        self._watched = (handle, fired, log)
        return record

    def _after_fire(self, context: Any, spec: ComponentSpec, record: dict[str, Any]) -> None:
        # Started once the record is complete, so a call the library acts on at once still lands after it.
        if record.get("applied") and self._watched is not None:
            self._watcher = threading.Thread(
                target=self._watch,
                args=(*self._watched,),
                kwargs={"record": record},
                name=f"syscall-{spec.id}",
                daemon=True,
            )
            self._watcher.start()

    def _watch(self, handle: Any, fired: str, log: str, *, record: dict[str, Any]) -> None:
        """Wait for the library to act, record where, and for a kill, wait for the exit and restart."""

        while not os.path.exists(fired):
            if self._stop.wait(_POLL_S):
                return
        calls = read_calls(log)
        acted = next((call for call in calls if call.get("action") != "pass"), None) or {}
        record.update(
            {
                "acted": True,
                "calls_matched": len(calls),
                "call": acted.get("call"),
                "path": acted.get("path"),
                "call_pid": acted.get("pid"),
                "applied_at": float(acted.get("t") or time.time()),
            }
        )
        if self._config["action"] != "kill":
            self._announce(record)
            return
        record["signal"] = "SIGKILL"
        stopped = None
        if self._config["restart"]:
            restart = restart_once(handle, self._target, wait_ready=True, ready_timeout=self._config["ready_timeout_s"])
            stopped = self._own("process.stopped", self._target, lambda: self._restart(handle, record, restart))
        record.update(
            wait_killed(handle, self._target, signal.SIGKILL, cluster_vars=self._config["capture_cluster_vars"])
        )
        if not record["exited"]:
            record["error"] = f"{self._target} did not exit within {KILL_EXIT_TIMEOUT_S:g}s of the library's kill"
        self._announce(record)
        if stopped is None or not record["exited"] or not self._context.sleep(self._config["restart_delay_s"]):
            return  # Recovery restarts it.
        try:
            stopped.discharge()
        except Exception as exc:
            record["restart_error"] = f"{type(exc).__name__}: {exc}"
            return
        self._context.event_bus.publish(
            f"{self._spec.id}.recovered", source=self._spec.id, payload={"instance_id": self._target}
        )

    def _restart(self, handle: Any, record: dict[str, Any], restart: Any) -> None:
        started = time.time()
        restart()
        record["restarted"] = True
        # The new process can vote and acknowledge from its spawn on, before it is ready.
        record["restart_spawned_at"] = handle.started_at(self._target)
        record["restart_ready_at"] = time.time()
        record["restart_seconds"] = round(time.time() - started, 3)
        record["restart_pid"] = handle.pid(self._target)

    def _quiesce(self) -> None:
        self._stop.set()
        if self._watcher is not None:
            # Its exit wait, restart delay, and readiness wait, plus a minute to start the new process.
            self._watcher.join(
                timeout=KILL_EXIT_TIMEOUT_S + self._config["restart_delay_s"] + self._config["ready_timeout_s"] + 60
            )

    def _settled(self, context: Any, spec: ComponentSpec, record: dict[str, Any]) -> None:
        if self._watcher is not None and self._watcher.is_alive():
            raise RuntimeError("the fault was still restarting its instance when recovery restarted it")
        if not record.get("applied") or record.get("acted"):
            return
        _, _, log = self._files(deployment(context))
        record["applied"] = False
        record["calls_matched"] = len(read_calls(log))
        record["error"] = (
            f"the rule never acted on {self._target}: {record['calls_matched']} matching call(s), "
            f"nth {self._config['nth']}"
        )

    def _evidence(self, spec: ComponentSpec, record: dict[str, Any]) -> tuple:
        if self._config["action"] != "kill" or self._config["restart"]:
            return ()
        # Down on purpose until the deployment is removed: valkey.oracle.liveness and the data oracles skip it.
        return (make_evidence(spec, EXPECTED_DOWN, "fault.recovered", {"instances": [record["target"]]}),)


def _remove(path: str) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


_DECLARES = fault_declares(
    {
        "target": TEXT,
        "rule": TEXT,
        "armed_at": NUMBER,
        "pid": nullable(int),
        "acted": optional(bool),
        "calls_matched": optional(int),
        "call": optional(str, allow_none=True),
        "path": optional(str, allow_none=True),
        "call_pid": optional(int, allow_none=True),
        "signal": optional(str),
        "exited": optional(bool),
        "exited_at": optional(float),
        "disk_vars_at_kill": optional(dict, allow_none=True),
        "restarted": optional(bool),
        "restart_spawned_at": optional(float),
        "restart_ready_at": optional(float),
        "restart_seconds": optional(float),
        "restart_pid": optional(int, allow_none=True),
        "restart_error": optional(str),
    },
    events={"{id}.recovered": ConfigSchema({"instance_id": TEXT})},
    changes=("syscall.rule", "process.stopped"),
    evidence={EXPECTED_DOWN: declared(mapping_of({"instances": ConfigField(list, items=TEXT)}))},
)

REGISTRATIONS = (
    component_registration(
        "valkey.fault.syscall",
        SyscallFault,
        _FIELDS,
        check=_validate,
        selectors={"instance": (EVENT_TARGET,), "peer": ()},
        triggered=True,
        declares=_DECLARES,
    ),
)
