"""libfaketime for ``valkey.fault.clock-skew``: finding the library, and the environment a server loads it in.

A deployment with ``clock_faults: true`` starts every server with libfaketime in ``LD_PRELOAD``, reading its
time spec from a timestamp file beside the instance's data directory on every call (``FAKETIME_NO_CACHE``). The
file starts as ``+0 x1``, the real time; ``valkey.fault.clock-skew`` rewrites it to skew one instance's clock and
writes ``+0 x1`` again to restore it. Only the wall clock is faked: ``DONT_FAKE_MONOTONIC`` keeps the event loop
and its timers on the host's monotonic clock, so a skew is what a stepped or drifting system clock looks like to
the server, not a faster event loop. The host clock is never touched: libfaketime is a per-process preload.

The library is the distribution's ``libfaketime`` package (``libfaketime.so.1`` under ``/usr/lib/*/faketime``,
``/usr/lib64/faketime``, ``/usr/lib/faketime`` or ``/usr/local/lib/faketime``), or whatever
``VALKEY_FUZZER_LIBFAKETIME`` names. Without it the deployment does not start, and the fault's probe says so.
"""

import glob
import os

from .models import LocalDeploymentError
from .syscall_faults import preload

LIBRARY_ENV = "VALKEY_FUZZER_LIBFAKETIME"
TIMESTAMP_FILE_ENV = "FAKETIME_TIMESTAMP_FILE"
# Where distributions install the library: Debian and Ubuntu (multiarch), Fedora, and `make install`.
LIBRARY_PATTERNS = (
    "/usr/lib/*/faketime/libfaketime.so.1",
    "/usr/lib64/faketime/libfaketime.so.1",
    "/usr/lib/faketime/libfaketime.so.1",
    "/usr/local/lib/faketime/libfaketime.so.1",
)
# The real time, as libfaketime reads it; the rate is written out because libfaketime keeps the last rate it read.
REAL_TIME = "+0 x1\n"


def find_library() -> str:
    """The libfaketime library to preload; :class:`LocalDeploymentError` naming the package when there is none."""

    named = os.environ.get(LIBRARY_ENV)
    if named:
        if os.path.isfile(named):
            return named
        raise LocalDeploymentError(f"{LIBRARY_ENV}={named} is not a file")
    for pattern in LIBRARY_PATTERNS:
        found = sorted(glob.glob(pattern))
        if found:
            return found[0]
    raise LocalDeploymentError(
        "libfaketime is not installed (no libfaketime.so.1 under "
        + ", ".join(os.path.dirname(pattern) for pattern in LIBRARY_PATTERNS)
        + f"): install the libfaketime package (apt install libfaketime, dnf install libfaketime) or set "
        f"{LIBRARY_ENV} to the library"
    )


def timestamp_file(data_dir: str) -> str:
    """The timestamp file of the instance whose data directory is ``data_dir``."""

    return os.path.join(os.path.dirname(os.path.abspath(data_dir)), "faketime.rc")


def write_timestamp(path: str, spec: str) -> None:
    """Replace the timestamp file at ``path`` with ``spec``, whole: libfaketime reads it on every time call."""

    partial = f"{path}.{os.getpid()}.tmp"
    with open(partial, "w", encoding="utf-8") as out:
        out.write(spec)
    os.replace(partial, path)


def server_environment(env: dict[str, str], library: str, timestamp: str) -> dict[str, str]:
    """``env`` with ``library`` appended to its preload list (:func:`~.syscall_faults.preload`), reading
    ``timestamp`` on every call."""

    env = preload(env, library)
    env.update({TIMESTAMP_FILE_ENV: timestamp, "FAKETIME_NO_CACHE": "1", "DONT_FAKE_MONOTONIC": "1"})
    return env
