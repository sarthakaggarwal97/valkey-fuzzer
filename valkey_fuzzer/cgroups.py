"""The run's own cgroup v2 subtree, for ``valkey.fault.resource-pressure``.

A fuzzer process runs in some cgroup ``D`` (``/proc/self/cgroup``). The fault limits one instance by moving its
server into a leaf of ``D`` with ``cpu.max`` or ``memory.max`` set. cgroup v2 only lets ``D`` hand controllers
to its children while ``D`` itself holds no process, so first every process of ``D`` moves into an unlimited leaf
``D/vf-harness``, then the controllers go into ``D/cgroup.subtree_control``, then the instance moves into its own
leaf ``D/vf-<owner>-<hash>``. Releasing moves it back into the harness leaf and removes its leaf; the harness leaf
and the enabled controllers stay (disabling them would drop a concurrent run's limits), and go with ``D``.

Nothing is ever written outside ``D``, and ``D`` must be the run's: the cgroup v2 hierarchy is mounted at
``/sys/fs/cgroup``, ``D`` is not the root cgroup, is writable by this user (delegated: a ``systemd-run --user
--scope`` scope, or a slice an administrator delegated), has the controllers the fault needs, and holds only
processes of this user; otherwise :func:`delegated` says why and the fault is an ERROR. A leftover leaf of a
fuzzer that died is removed by ``valkey-fuzzer sweep`` (:mod:`valkey_fuzzer.hoststate`).
"""

import errno
import os
from typing import Any, Optional

from .deadlines import wait_for

ROOT = "/sys/fs/cgroup"
HARNESS = "vf-harness"
# How long moving every process out of D may take: new ones can appear meanwhile.
_MOVE_TIMEOUT_S = 5.0


class CgroupError(Exception):
    """A cgroup step failed, or the cgroup is not the run's to use."""


def own_cgroup(proc: str = "/proc") -> Optional[str]:
    """This process's cgroup v2 path (``/user.slice/...``); None without a v2 hierarchy."""

    try:
        with open(os.path.join(proc, "self", "cgroup"), encoding="utf-8") as handle:
            lines = handle.read().splitlines()
    except OSError:
        return None
    for line in lines:
        if line.startswith("0::"):
            return line[3:].strip() or "/"
    return None


def _read(path: str) -> str:
    with open(path, encoding="utf-8") as handle:
        return handle.read()


# The writes that ask the kernel for something (a move, a controller, a limit) and the cgroup directories made
# and removed: the tests stand in a fake kernel for them.
def _write(path: str, text: str) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(text)


def _mkdir(path: str, exist_ok: bool = False) -> None:
    os.makedirs(path, exist_ok=True) if exist_ok else os.mkdir(path)


def _rmdir(path: str) -> None:
    os.rmdir(path)


def procs(directory: str) -> list[int]:
    """The PIDs in cgroup ``directory``."""

    return [int(pid) for pid in _read(os.path.join(directory, "cgroup.procs")).split()]


def _uid(pid: int, proc: str) -> Optional[int]:
    try:
        return os.stat(os.path.join(proc, str(pid))).st_uid
    except OSError:
        return None  # Gone meanwhile.


def delegated(controllers: tuple[str, ...], root: str = ROOT, proc: str = "/proc") -> tuple[Optional[str], str]:
    """``(D, "")`` when this process's cgroup ``D`` (a directory under ``root``) is the run's to use with
    ``controllers``, else ``(None, why not)``."""

    if not os.path.exists(os.path.join(root, "cgroup.controllers")):
        return None, f"cgroup v2 is not mounted at {root} (a cgroup v1 or hybrid host)"
    path = own_cgroup(proc)
    if path is None:
        return None, "this process is in no cgroup v2 hierarchy"
    hint = "run the fuzzer in a delegated cgroup of its own (systemd-run --user --scope valkey-fuzzer ...)"
    if path == "/":
        return None, f"this process runs in the root cgroup; {hint}"
    directory = os.path.join(root, path.lstrip("/"))
    if not os.access(directory, os.W_OK) or not os.access(os.path.join(directory, "cgroup.procs"), os.W_OK):
        return None, f"cgroup {path} is not writable by uid {os.geteuid()} (not delegated); {hint}"
    try:
        available = _read(os.path.join(directory, "cgroup.controllers")).split()
    except OSError as exc:
        return None, f"cannot read the controllers of cgroup {path}: {exc}"
    missing = [controller for controller in controllers if controller not in available]
    if missing:
        return None, (
            f"cgroup {path} lacks the {', '.join(missing)} controller(s): its parent's cgroup.subtree_control "
            f"must enable them (systemd: Delegate= on the slice or user@.service)"
        )
    others = sorted({uid for uid in (_uid(pid, proc) for pid in procs(directory)) if uid not in (None, os.geteuid())})
    if others:
        return None, f"cgroup {path} holds processes of other users (uid {', '.join(map(str, others))}); {hint}"
    return directory, ""


def _move(pid: int, directory: str) -> bool:
    """Move process ``pid`` into cgroup ``directory``; False once it is gone."""

    try:
        _write(os.path.join(directory, "cgroup.procs"), str(pid))
    except OSError as exc:
        if exc.errno == errno.ESRCH:
            return False
        raise
    return True


def _empty(directory: str, into: str) -> None:
    """Move every process of cgroup ``directory`` into ``into`` until none is left."""

    def moved() -> tuple[bool, list[int]]:
        pids = procs(directory)
        for pid in pids:
            _move(pid, directory=into)
        return not pids, pids

    done, pids = wait_for(moved, timeout_s=_MOVE_TIMEOUT_S, interval_s=0.05, reason=f"cgroup {directory} to empty")
    if not done:
        raise CgroupError(f"cgroup {directory} still holds processes {pids[:5]} after {_MOVE_TIMEOUT_S:g}s")


def enter(delegated_dir: str, controllers: tuple[str, ...]) -> str:
    """Make ``D`` (``delegated_dir``) able to limit children with ``controllers``: its processes move into the
    harness leaf, which is returned, and the controllers go into its ``cgroup.subtree_control``. Idempotent."""

    harness = os.path.join(delegated_dir, HARNESS)
    _mkdir(harness, exist_ok=True)
    enabled = set(_read(os.path.join(delegated_dir, "cgroup.subtree_control")).split())
    wanted = [controller for controller in controllers if controller not in enabled]
    if not wanted:
        _empty(delegated_dir, harness)  # Our own process may still be in D, from before the fault.
        return harness

    def enabled() -> tuple[bool, Optional[OSError]]:
        _empty(delegated_dir, harness)
        try:
            _write(os.path.join(delegated_dir, "cgroup.subtree_control"), " ".join(f"+{name}" for name in wanted))
        except OSError as exc:
            # EBUSY: a process appeared in D meanwhile (a fork of one that moved); move it and try again at once.
            return exc.errno != errno.EBUSY, exc
        return True, None

    _, error = wait_for(enabled, timeout_s=_MOVE_TIMEOUT_S, interval_s=0.0, reason="the controllers to enable")
    if error is not None:
        raise CgroupError(
            f"cannot enable {', '.join(wanted)} in {delegated_dir}/cgroup.subtree_control: {error}"
        ) from error
    return harness


def limit(delegated_dir: str, name: str, pid: int, limits: dict[str, str]) -> str:
    """Make leaf ``D/name`` with ``limits`` (``{"cpu.max": "1000 100000", ...}``) and move ``pid`` into it; its
    path. A leaf that could not be set up is removed again."""

    leaf = os.path.join(delegated_dir, name)
    _mkdir(leaf)
    try:
        for control, value in limits.items():
            _write(os.path.join(leaf, control), value)
        if not _move(pid, leaf):
            raise CgroupError(f"process {pid} is gone")
    except OSError as exc:
        remove_leaf(leaf, os.path.join(delegated_dir, HARNESS))
        raise CgroupError(f"cannot limit process {pid} in {leaf}: {exc}") from exc
    except CgroupError:
        remove_leaf(leaf, os.path.join(delegated_dir, HARNESS))
        raise
    return leaf


def stats(leaf: str) -> dict[str, Any]:
    """What the kernel counted for ``leaf``: ``cpu.stat`` and ``memory.events`` as ``{file.key: int}``, and
    ``memory.current`` and ``memory.peak`` (where the kernel has it) as integers; files a controller does not
    provide are left out."""

    read: dict[str, Any] = {}
    for name in ("cpu.stat", "memory.events"):
        try:
            text = _read(os.path.join(leaf, name))
        except OSError:
            continue
        for line in text.splitlines():
            key, _, value = line.partition(" ")
            if value.strip().isdigit():
                read[f"{name}.{key}"] = int(value)
    for name in ("memory.current", "memory.peak"):
        try:
            read[name] = int(_read(os.path.join(leaf, name)).strip())
        except (OSError, ValueError):
            continue
    return read


def remove_leaf(leaf: str, into: str) -> None:
    """Move every process of ``leaf`` into ``into`` and remove ``leaf``; a leaf that is already gone is fine."""

    if not os.path.isdir(leaf):
        return
    if procs(leaf):
        _mkdir(into, exist_ok=True)
        _empty(leaf, into)
    _rmdir(leaf)
