"""``valkey.fault.clock-skew``: shift one instance's view of the wall clock, without touching the host clock.

The deployment starts every server under libfaketime (``deployment.clock_faults: true``,
:mod:`valkey_fuzzer.deployment.local.clock_faults`), each reading a timestamp file of its own on every time call.
When its trigger fires, the fault writes ``+<offset> x<rate>`` to the target's file: from then on the server's
wall clock reads ``offset_s`` seconds ahead of (or behind) the host's and runs ``rate`` times as fast. Only the
wall clock moves: key expiry, ``TIME``, the cluster and replication timeouts measured in ``mstime()``; the event
loop keeps the host's monotonic clock. The fault then reads ``TIME`` on the instance and compares it with the
host's clock: a skew the server does not show (libfaketime not loaded) leaves the fault not applied, an ERROR.
The real time is restored on ``release_event`` or at recovery, and the clock is measured again before it is.

A rate makes libfaketime scale the time since the server started, which would step the clock by ``(rate - 1) x
uptime`` when the file changes; the offset written compensates for that, so the skew at the moment the fault lands
is ``offset_s`` exactly, and grows (or shrinks) at ``rate`` from there.
"""

import time
from typing import Any, Optional

from valkey_fuzzer.clock import now
from valkey_fuzzer.deployment.local import clock_faults
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.schema import ConfigError, ConfigField, plain_config

from .faults import EVENT_TARGET, RELEASED_EVENT, RELEASED_FIELDS, HeldFault, fault_declares
from .instances import close_quietly, deployment
from .registration import component_registration
from .schemas import NUMBER, TEXT, nullable, optional

# How far the measured skew may be from the one asked for: a TIME round trip, plus the rate's drift meanwhile.
TOLERANCE_S = 1.0
_YEAR_S = 366 * 86400

_FIELDS = {
    "instance": ConfigField(str, default="primary"),
    # Seconds the instance's clock is ahead (positive) or behind (negative) the host's when the fault lands.
    "offset_s": ConfigField(float, default=0.0, minimum=-_YEAR_S, maximum=_YEAR_S),
    # How fast the instance's clock runs from then on, relative to the host's.
    "rate": ConfigField(float, default=1.0, minimum=0.01, maximum=100.0),
    "release_event": ConfigField(str, allow_none=True, default=None),
}


def _validate(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    if not plain_config(plan.deployment).get("clock_faults"):
        raise ConfigError("valkey.fault.clock-skew requires deployment.clock_faults: true")
    if config["offset_s"] == 0 and config["rate"] == 1:
        raise ConfigError("a skew needs an offset_s other than 0 or a rate other than 1")


def _probe(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    clock_faults.find_library()


def time_spec(offset_s: float, rate: float, uptime_s: float) -> str:
    """libfaketime's spec for a clock ``offset_s`` ahead now and running at ``rate``, for a server up for
    ``uptime_s``: the offset is lowered by what the rate adds to the time since the server started."""

    written = offset_s - (rate - 1.0) * uptime_s
    rate_text = f"{rate:.6f}".rstrip("0").rstrip(".")
    return f"{written:+.6f} x{rate_text}\n"


def measure_skew(handle: Any, instance_id: str) -> float:
    """``TIME`` on ``instance_id`` minus the host's wall clock, in seconds; raises when the instance does not
    answer."""

    client = None
    try:
        client = handle.new_client(instance_id, timeout=2.0)
        before = time.time()
        seconds, micros = client.execute_command("TIME")
        after = time.time()
    finally:
        close_quietly(client)
    return int(seconds) + int(micros) / 1_000_000 - (before + after) / 2


class ClockSkewFault(HeldFault):
    """Skew one instance's wall clock at a lifecycle event, until ``release_event`` or recovery (see the module)."""

    seconds_field = "skew_seconds"
    release_failed_text = "restoring the clock failed, so the skew lasted until recovery"

    def _resolve(self, context: Any, spec: ComponentSpec) -> None:
        self._path: Optional[str] = None
        self._skew_at_release: Optional[float] = None
        self._select_target(context)

    def _install(self, event: Any) -> dict[str, Any]:
        handle = deployment(self._context)
        self._target_instance(handle, event)
        self._path = handle.clock_fault_file(self._target)
        if self._path is None:
            raise RuntimeError("the deployment does not start its servers under libfaketime (clock_faults)")
        offset, rate = self._config["offset_s"], self._config["rate"]
        applied_at = now()
        spec_text = time_spec(offset, rate, applied_at - handle.started_at(self._target))
        # Owned from before the write: the undo restores the real time whatever the file holds.
        self._own_change("clock.skewed", self._target, self._restore)
        clock_faults.write_timestamp(self._path, spec_text)
        record: dict[str, Any] = {
            "target": self._target,
            "pid": handle.pid(self._target),
            "timestamp_file": self._path,
            "spec": spec_text.strip(),
            "offset_s": offset,
            "rate": rate,
            "skew_measured_s": None,
        }
        try:
            measured = measure_skew(handle, self._target)
        except Exception as exc:
            record["applied"] = False
            record["error"] = f"cannot read TIME on {self._target}: {type(exc).__name__}: {exc}"
            return record
        expected = offset + (rate - 1.0) * (now() - applied_at)
        record.update(skew_measured_s=round(measured, 3), skew_expected_s=round(expected, 3))
        if abs(measured - expected) > TOLERANCE_S:
            record["applied"] = False
            record["error"] = (
                f"TIME on {self._target} is {measured:+.3f} s from the host's clock, not {expected:+.3f} s: "
                "is libfaketime loaded (deployment.clock_faults)?"
            )
        return record

    def _restore(self) -> None:
        """The undo: measure the skew once more, then write the real time."""

        if self._skew_at_release is None:
            try:
                self._skew_at_release = round(measure_skew(deployment(self._context), self._target), 3)
            except Exception:
                self._skew_at_release = None  # Down, or the deployment is gone: the file still gets restored.
        clock_faults.write_timestamp(self._path, clock_faults.REAL_TIME)

    def _settled(self, context: Any, spec: ComponentSpec, record: dict[str, Any]) -> None:
        super()._settled(context, spec, record)
        if self._released_at is not None:
            record["skew_at_release_s"] = self._skew_at_release


_DECLARES = fault_declares(
    {
        "target": TEXT,
        "pid": nullable(int),
        "timestamp_file": TEXT,
        "spec": TEXT,
        "offset_s": NUMBER,
        "rate": NUMBER,
        "skew_measured_s": nullable(float),
        "skew_expected_s": optional(float),
        **RELEASED_FIELDS,
        "skew_at_release_s": optional(float, allow_none=True),
        "skew_seconds": optional(float),
    },
    events={"{id}.released": RELEASED_EVENT},
    changes=("clock.skewed",),
)

REGISTRATIONS = (
    component_registration(
        "valkey.fault.clock-skew",
        ClockSkewFault,
        _FIELDS,
        check=_validate,
        probe=_probe,
        selectors={"instance": (EVENT_TARGET,)},
        triggered=True,
        declares=_DECLARES,
    ),
)
