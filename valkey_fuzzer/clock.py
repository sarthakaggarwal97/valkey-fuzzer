"""The run clock: where the harness reads a timestamp it will compare with another.

An action's or fault's ``applied_at``, ``started_at``, ``completed_at`` and ``ended_at`` are on this clock; the
report-only fields stay on wall time: an obligation's ``opened_at`` and ``undone_at``, the ``started_at`` and
``ended_at`` the runner gives a run and each result, and an event's ``timestamp``.
"""

import time

# The wall clock as this process started, less the monotonic clock then.
_OFFSET = time.time() - time.monotonic()


def now() -> float:
    """Seconds since the epoch, read from the monotonic clock anchored to the wall clock at process start.

    Operation times, loss windows, and fault times are compared on it. A wall-clock step (NTP, a resumed VM)
    would reorder the ledger and move its windows; this clock never steps. It reads as wall time until the wall
    clock drifts or steps, so a comparison with Valkey's own log timestamps (wall time) holds until then.
    """

    return time.monotonic() + _OFFSET
