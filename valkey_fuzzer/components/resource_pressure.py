"""``valkey.fault.resource-pressure``: cgroup v2 CPU and memory limits on one instance, for a window.

When its trigger fires, the fault moves the target's server (and the children it forks from then on) into a
cgroup leaf of the run's own (:mod:`valkey_fuzzer.cgroups`) with ``cpu.max`` from ``cpu_percent`` and
``memory.max`` from ``memory_max_mb``. The instance keeps running, slower: a CPU quota throttles it for the rest
of each period once the quota is spent (a slow, alive primary, where ``valkey.fault.process-signal`` with
``SIGSTOP`` is a stopped one); a memory limit makes the kernel reclaim its page cache and, when its anonymous
memory alone exceeds the limit, kills it (the liveness oracle reports that). On ``release_event`` or at recovery
the server moves back to the unlimited harness leaf, the leaf is removed, and its counters are recorded. A limit
that never bit (``cpu.stat`` counted no throttled period, ``memory.events`` no ``max``) leaves the fault not
applied: the run did not test what it meant to.

The fault needs a delegated cgroup v2 of the run's own and refuses anything else (``valkey.fault.injected`` is
never produced, the run is an ERROR with the reason): see :func:`valkey_fuzzer.cgroups.delegated`. Nothing is
written outside that cgroup, and no process but the run's own is moved. ``valkey.fault.process-signal`` with
``SIGSTOP`` already covers a fully stopped instance, so there is no fallback without cgroups.
"""

import os
import threading
import uuid
from typing import Any, Optional

from valkey_fuzzer import cgroups
from valkey_fuzzer.clock import now
from valkey_fuzzer.hoststate import cgroup_name
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.schema import ConfigError, ConfigField, ConfigSchema

from .faults import EVENT_TARGET, EventFault, FaultRefusedError, fault_declares
from .instances import deployment, select_instance
from .registration import component_registration
from .schemas import MAPPING, TEXT, nullable, optional

_FIELDS = {
    "instance": ConfigField(str, default="primary"),
    # Of one CPU: 100 is a whole core, 1 is 1 ms of every 100 ms. Null: no CPU limit.
    "cpu_percent": ConfigField(float, allow_none=True, default=None, minimum=0.1, maximum=100.0),
    # Null: no memory limit.
    "memory_max_mb": ConfigField(int, allow_none=True, default=None, minimum=1, maximum=1 << 20),
    "release_event": ConfigField(str, allow_none=True, default=None),
}


def controllers_for(config: dict) -> tuple[str, ...]:
    """The cgroup controllers ``config``'s limits need."""

    return tuple(
        name for name, field in (("cpu", "cpu_percent"), ("memory", "memory_max_mb")) if config[field] is not None
    )


def _validate(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    if not controllers_for(config):
        raise ConfigError("set cpu_percent, memory_max_mb, or both")


def _probe(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    directory, reason = cgroups.delegated(controllers_for(config))
    if directory is None:
        raise RuntimeError(reason)


def cpu_max(percent: float) -> str:
    """``cpu.max`` for ``percent`` of a CPU: the quota per 100 ms period, or per second below the kernel's 1 ms
    minimum quota."""

    period = 100_000
    quota = round(percent / 100 * period)
    if quota < 1000:
        period = 1_000_000
        quota = round(percent / 100 * period)
    return f"{quota} {period}"


def limits_for(config: dict) -> dict[str, str]:
    """The cgroup control files ``config`` sets and their values."""

    limits = {}
    if config["cpu_percent"] is not None:
        limits["cpu.max"] = cpu_max(config["cpu_percent"])
    if config["memory_max_mb"] is not None:
        limits["memory.max"] = str(config["memory_max_mb"] * 1024 * 1024)
    return limits


class ResourcePressureFault(EventFault):
    """Limit one instance's CPU or memory with cgroup v2 at a lifecycle event, until ``release_event`` or
    recovery (see the module)."""

    def _prepare(self, context: Any, spec: ComponentSpec) -> None:
        self._target: Optional[str] = None
        self._leaf: Optional[str] = None
        self._harness: Optional[str] = None
        self._limited: Any = None
        self._stats: Optional[dict[str, Any]] = None
        self._lock_release = threading.Lock()
        self._closed = False
        self._released_at: Optional[float] = None
        self._release_reason: Optional[str] = None
        self._release_error: Optional[str] = None
        if self._config["instance"] != EVENT_TARGET:
            self._target = select_instance(deployment(context), self._config["instance"])
        if self._config["release_event"]:
            self._subscribe(self._config["release_event"], self._release_on_event)

    def _fire(self, context: Any, spec: ComponentSpec, event: Any) -> dict[str, Any]:
        handle = deployment(context)
        if self._target is None:
            self._target = self._event_target(handle, event)
        controllers = controllers_for(self._config)
        # Checked again at the trigger: the cgroup must still be the run's to use.
        directory, reason = cgroups.delegated(controllers)
        if directory is None:
            raise FaultRefusedError(reason)
        pid = handle.pid(self._target)
        limits = limits_for(self._config)
        self._harness = cgroups.enter(directory, controllers)
        name = cgroup_name(f"{spec.id}/{uuid.uuid4().hex}")
        self._leaf = os.path.join(directory, name)
        # Owned from before the leaf exists: the undo of a leaf that is not there does nothing.
        self._limited = self._own("cgroup.limited", self._target, self._release_leaf)
        cgroups.limit(directory, name, pid, limits)
        return {"target": self._target, "pid": pid, "cgroup": self._leaf, "limits": limits}

    def _release_leaf(self) -> None:
        """The undo: read the leaf's counters, move its processes back to the harness leaf, remove it."""

        if self._stats is None and os.path.isdir(self._leaf):
            self._stats = cgroups.stats(self._leaf)
        cgroups.remove_leaf(self._leaf, self._harness)

    def _release(self, reason: str) -> None:
        with self._lock_release:
            if self._closed or self._limited is None or self._released_at is not None:
                return
            error = self._undo_early(self._limited, "released", {"instance_id": self._target, "reason": reason})
            if error:
                self._release_error = error
                return
            self._released_at = now()
            self._release_reason = reason

    def _release_on_event(self, event: Any) -> None:
        self._release("event")

    def _quiesce(self) -> None:
        with self._lock_release:
            self._closed = True

    def _settled(self, context: Any, spec: ComponentSpec, record: dict[str, Any]) -> None:
        with self._lock_release:
            if self._limited is not None and self._released_at is None and not self._limited.open:
                self._released_at = now()
                self._release_reason = "recover"
        if self._release_error:
            record["release_error"] = self._release_error
            raise RuntimeError(f"releasing the limits failed, so they lasted until recovery: {self._release_error}")
        if self._released_at is not None:
            record["released_at"] = self._released_at
            record["release_reason"] = self._release_reason
            record["pressure_seconds"] = round(self._released_at - record.get("applied_at", self._released_at), 3)
        if self._stats is None or not record.get("applied"):
            return
        record["stats"] = self._stats
        limits = record["limits"]
        if "cpu.max" in limits and not self._stats.get("cpu.stat.nr_throttled"):
            record["applied"] = False
            record["error"] = (
                f"cpu.max {limits['cpu.max']} never throttled {self._target}: it used less CPU than the quota; "
                "lower cpu_percent or load the instance"
            )
        elif "memory.max" in limits and not self._stats.get("memory.events.max"):
            record["applied"] = False
            record["error"] = (
                f"memory.max {limits['memory.max']} was never reached by {self._target} "
                f"(memory.current {self._stats.get('memory.current')} bytes at release); lower memory_max_mb"
            )


_DECLARES = fault_declares(
    {
        "target": TEXT,
        "pid": nullable(int),
        "cgroup": TEXT,
        "limits": MAPPING,
        "released_at": optional(float),
        "release_reason": optional(str),
        "pressure_seconds": optional(float),
        # cpu.stat and memory.events as {file.key: count}, memory.current and memory.peak, read at release.
        "stats": optional(dict),
        "release_error": optional(str),
    },
    events={"{id}.released": ConfigSchema({"instance_id": TEXT, "reason": TEXT})},
    changes=("cgroup.limited",),
)

REGISTRATIONS = (
    component_registration(
        "valkey.fault.resource-pressure",
        ResourcePressureFault,
        _FIELDS,
        check=_validate,
        probe=_probe,
        selectors={"instance": (EVENT_TARGET,)},
        triggered=True,
        declares=_DECLARES,
    ),
)
