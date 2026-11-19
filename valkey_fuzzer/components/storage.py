"""Storage faults for durability experiments: a full disk and read-only data directories."""

import errno
import os
from typing import Any, Optional

from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.references import EnvironmentKind
from valkey_fuzzer.schema import ConfigError, ConfigField, ConfigSchema, plain_config

from . import providers
from .faults import EventFault, FaultRefusedError, fault_declares
from .registration import component_registration
from .schemas import INTEGER, TEXT, optional

# ---------------------------------------------------------------------------
# valkey.fault.disk-fill — fill a valkey.resource.tmpfs mount until ENOSPC
# ---------------------------------------------------------------------------

_FILL_FIELDS = {
    # ``{$ref: resources.<id>.path}`` of a valkey.resource.tmpfs resource: only a
    # size-limited mount the runner provisioned is ever filled, never host disk.
    "path": ConfigField(str, reference=True),
    "leave_bytes": ConfigField(int, default=0, minimum=0),
    # The most bytes written; the tmpfs size by default, and never more.
    "max_bytes": ConfigField(int, allow_none=True, default=None, minimum=1),
    "chunk_bytes": ConfigField(int, default=1024 * 1024, minimum=4096, maximum=64 * 1024 * 1024),
    "release_event": ConfigField(str, allow_none=True, default=None),
}

_NOT_TMPFS = "path must be {$ref: resources.<id>.path} of a valkey.resource.tmpfs resource"


def _validate_fill(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    path = config["path"]
    tmpfs = {resource.id for resource in plan.resources if resource.provider_id == providers.TMPFS_PROVIDER}
    if isinstance(path, str):
        if not os.path.isabs(path):
            raise ConfigError("path must be an absolute directory path")
        # The executable plan holds the resolved path; the fault checks the
        # mount itself before it writes.
        if not tmpfs:
            raise ConfigError(_NOT_TMPFS)
    elif path.kind is not EnvironmentKind.RESOURCE or path.output != "path" or path.id not in tmpfs:
        raise ConfigError(_NOT_TMPFS)
    # Filling a filesystem no instance writes to would test nothing.
    roots = plain_config(plan.deployment).get("instance_data_roots") or {}
    if path not in roots.values():
        raise ConfigError("path must also be a deployment.instance_data_roots entry so instance data lives on it")


def _tmpfs_limit(path: str) -> int:
    """The size of the valkey.resource.tmpfs mount at ``path``; refuses anything else."""

    limit = providers.provisioned_tmpfs_bytes(path)
    if limit is None:
        raise FaultRefusedError(f"{path} is not a mount provisioned by a valkey.resource.tmpfs resource")
    size = providers.mounted_tmpfs_bytes(path)
    if size is None:
        raise FaultRefusedError(f"{path} is no longer a tmpfs mount point")
    if size > limit:
        raise FaultRefusedError(
            f"the tmpfs at {path} holds {size} bytes, more than the {limit} it was provisioned with"
        )
    return limit


class FillFilesystemFault(EventFault):
    """Fill the tmpfs at ``path`` (at most ``max_bytes``); optionally free it again on ``release_event``.

    Before writing it checks that ``path`` is still the size-limited tmpfs a
    ``valkey.resource.tmpfs`` resource mounted, and refuses (ERROR) any other path.
    """

    def _prepare(self, context: Any, spec: ComponentSpec) -> None:
        self._ballast = os.path.join(self._config["path"], f".ballast-{spec.id}")
        self._filled: Any = None
        # The sequence number of the event that released the ballast, once it has.
        self._released_on: Optional[int] = None
        if self._config["release_event"]:
            self._subscribe(self._config["release_event"], self._release)

    def _remove_ballast(self) -> None:
        if os.path.exists(self._ballast):
            os.remove(self._ballast)

    def _release(self, event: Any) -> None:
        if self._filled is None or self._released_on is not None:
            return
        # An error raised here would fail the release event's publisher (a bgsave's save-completed).
        if self._undo_early(self._filled, "released", {"ballast": self._ballast}) is None:
            self._released_on = event.sequence

    def _fire(self, context: Any, spec: ComponentSpec, event: Any) -> dict[str, Any]:
        limit = _tmpfs_limit(self._config["path"])
        ceiling = min(limit, self._config["max_bytes"] or limit)
        written = 0
        chunk = b"\0" * self._config["chunk_bytes"]
        full = cancelled = False
        self._filled = self._own("disk.filled", self._ballast, self._remove_ballast)
        with open(self._ballast, "wb", buffering=0) as handle:
            while written < ceiling:
                # The fill runs inside the trigger's dispatch: stop when that call is cancelled.
                if context.cancelled:
                    cancelled = True
                    break
                stats = os.statvfs(os.path.dirname(self._ballast))
                free = stats.f_bavail * stats.f_frsize
                budget = free - self._config["leave_bytes"]
                if budget <= 0:
                    full = True
                    break
                try:
                    written += handle.write(chunk[: min(len(chunk), budget, ceiling - written)])
                except OSError as exc:
                    if exc.errno != errno.ENOSPC:
                        raise
                    full = True
                    break
        stats = os.statvfs(os.path.dirname(self._ballast))
        record = {
            "ballast": self._ballast,
            "bytes_written": written,
            "max_bytes": ceiling,
            "free_bytes_after": stats.f_bavail * stats.f_frsize,
            "applied": full,
        }
        if cancelled:
            record["error"] = f"cancelled after {written} bytes, before the filesystem filled"
        elif not full:
            record["error"] = f"stopped at max_bytes ({ceiling}) before the filesystem filled"
        return record

    def _settled(self, context: Any, spec: ComponentSpec, record: dict[str, Any]) -> None:
        if self._released_on is not None:
            record["released_on_event_sequence"] = self._released_on
        elif self._filled is not None:
            record["ballast_removed"] = not self._filled.open


_FILL_DECLARES = fault_declares(
    {
        "ballast": TEXT,
        "bytes_written": INTEGER,
        "max_bytes": INTEGER,
        "free_bytes_after": INTEGER,
        "released_on_event_sequence": optional(int),
        "ballast_removed": optional(bool),
    },
    events={"{id}.released": ConfigSchema({"ballast": TEXT})},
    changes=("disk.filled",),
)

REGISTRATIONS = (
    component_registration(
        "valkey.fault.disk-fill",
        FillFilesystemFault,
        _FILL_FIELDS,
        check=_validate_fill,
        triggered=True,
        declares=_FILL_DECLARES,
    ),
)
