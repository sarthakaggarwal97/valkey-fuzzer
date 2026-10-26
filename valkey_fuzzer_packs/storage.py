"""Host capability probes and storage faults for durability packs."""

import errno
import os
import platform
import stat
from typing import Any

from valkey_fuzzer.extensions import ComponentStatus

from .common import (
    ComponentConfigError,
    ComponentSpec,
    Field,
    RunPlan,
    config_validator,
    make_evidence,
    make_result,
    resolved_config,
    select_nodes,
    to_plain_data,
    topology,
    validate_node_selector,
)
from .faults import _EventFault

_HOST_REQUIREMENTS = ("linux", "root", "not_root", "tmpfs", "iptables", "per_node_loopback")

_HOST_FIELDS = {"requires": Field(list)}


def _validate_host(config: dict, spec: ComponentSpec, plan: RunPlan) -> None:
    unknown = sorted(set(config["requires"]) - set(_HOST_REQUIREMENTS))
    if unknown or not config["requires"]:
        raise ComponentConfigError(f"requires must list some of {list(_HOST_REQUIREMENTS)}")


class HostCapabilityProbe:
    """``SKIP`` when the host cannot run a scenario (e.g. no tmpfs mounts)."""

    def probe(self, context: Any, spec: ComponentSpec):
        from .providers import tmpfs_supported

        config = resolved_config(spec, _HOST_FIELDS)
        unmet = []
        for requirement in config["requires"]:
            if requirement == "linux" and platform.system() != "Linux":
                unmet.append("requires Linux")
            elif requirement == "root" and os.geteuid() != 0:
                unmet.append("requires root")
            elif requirement == "not_root" and os.geteuid() == 0:
                unmet.append("permission faults are ineffective as root")
            elif requirement == "tmpfs":
                supported, reason = tmpfs_supported()
                if not supported:
                    unmet.append(reason)
            elif requirement in ("iptables", "per_node_loopback"):
                from .network import iptables_supported, per_node_loopback_supported

                check = iptables_supported if requirement == "iptables" else per_node_loopback_supported
                supported, reason = check()
                if not supported:
                    unmet.append(reason)
        if unmet:
            return make_result(spec, ComponentStatus.SKIP, "; ".join(unmet))
        return make_result(
            spec,
            ComponentStatus.PASS,
            "host satisfies " + ", ".join(config["requires"]),
            evidence=(
                make_evidence(
                    spec,
                    "valkey.host.supported",
                    "capability.completed",
                    {"platform": platform.platform(), "requires": config["requires"]},
                ),
            ),
        )


# ---------------------------------------------------------------------------
# valkey.fs.fill — fill a filesystem until ENOSPC
# ---------------------------------------------------------------------------

_FILL_FIELDS = {
    # Usually ``{$ref: resources.<id>.path}`` for a size-limited filesystem.
    "path": Field(str, reference=True),
    "leave_bytes": Field(int, default=0, minimum=0),
    "chunk_bytes": Field(int, default=1024 * 1024, minimum=4096, maximum=64 * 1024 * 1024),
    "delay_s": Field(float, default=0.0, minimum=0, maximum=600),
    "release_event": Field(str, allow_none=True, default=None),
}


def _validate_fill(config: dict, spec: ComponentSpec, plan: RunPlan) -> None:
    path = config["path"]
    if isinstance(path, str) and not os.path.isabs(path):
        raise ComponentConfigError("path must be an absolute directory path")
    # Filling a filesystem no node writes to would test nothing.
    roots = to_plain_data(plan.topology_document).get("node_data_roots") or {}
    if path not in roots.values():
        raise ComponentConfigError("path must also be a topology.node_data_roots entry so node data lives on it")


class FillFilesystemFault(_EventFault):
    """Fill a filesystem; optionally free it again on ``release_event``."""

    fields = _FILL_FIELDS

    def _prepare(self, context: Any, spec: ComponentSpec) -> None:
        self._ballast = os.path.join(self._config["path"], f".ballast-{spec.instance_id}")
        self._released_at = None
        if self._config["release_event"]:
            self._release_subscription = context.event_bus.subscribe(self._config["release_event"], self._release)

    def _release(self, event: Any) -> None:
        if self._released_at is None and os.path.exists(self._ballast):
            os.remove(self._ballast)
            self._released_at = event.sequence
            self._context.event_bus.publish(
                "storage.released", source=self._spec.instance_id, payload={"ballast": self._ballast}
            )

    def _fire(self, context: Any, spec: ComponentSpec, event: Any) -> dict[str, Any]:
        written = 0
        chunk = b"\0" * self._config["chunk_bytes"]
        full = False
        with open(self._ballast, "wb", buffering=0) as handle:
            while True:
                stats = os.statvfs(os.path.dirname(self._ballast))
                free = stats.f_bavail * stats.f_frsize
                budget = free - self._config["leave_bytes"]
                if budget <= 0:
                    full = True
                    break
                try:
                    written += handle.write(chunk[: min(len(chunk), budget)])
                except OSError as exc:
                    if exc.errno != errno.ENOSPC:
                        raise
                    full = True
                    break
        stats = os.statvfs(os.path.dirname(self._ballast))
        return {
            "ballast": self._ballast,
            "bytes_written": written,
            "free_bytes_after": stats.f_bavail * stats.f_frsize,
            "applied": full,
        }

    def _recover(self, context: Any, spec: ComponentSpec, record: dict[str, Any]) -> None:
        subscription = getattr(self, "_release_subscription", None)
        if subscription is not None:
            context.event_bus.unsubscribe(subscription)
        if self._released_at is not None:
            record["released_on_event_sequence"] = self._released_at
        if os.path.exists(self._ballast):
            os.remove(self._ballast)
            record["ballast_removed"] = True


# ---------------------------------------------------------------------------
# valkey.fs.deny_writes — make node data directories read-only
# ---------------------------------------------------------------------------

_DENY_FIELDS = {
    "nodes": Field((str, list), default="primaries"),
    "delay_s": Field(float, default=0.0, minimum=0, maximum=600),
}


class DenyWritesFault(_EventFault):
    fields = _DENY_FIELDS

    def _prepare(self, context: Any, spec: ComponentSpec) -> None:
        handle = topology(context)
        self._dirs = {node_id: handle.data_dir(node_id) for node_id in select_nodes(handle, self._config["nodes"])}
        self._modes: dict[str, int] = {}

    def _fire(self, context: Any, spec: ComponentSpec, event: Any) -> dict[str, Any]:
        for path in self._dirs.values():
            self._modes[path] = stat.S_IMODE(os.stat(path).st_mode)
            os.chmod(path, 0o500)
        return {"directories": self._dirs, "applied": os.geteuid() != 0}

    def _recover(self, context: Any, spec: ComponentSpec, record: dict[str, Any]) -> None:
        for path, mode in self._modes.items():
            os.chmod(path, mode)
        record["restored"] = True


REGISTRATIONS = (
    ("valkey.host.capability", "capability", HostCapabilityProbe, config_validator(_HOST_FIELDS, extra=_validate_host)),
    (
        "valkey.fs.fill",
        "fault",
        FillFilesystemFault,
        config_validator(_FILL_FIELDS, extra=_validate_fill, requires_event=True),
    ),
    (
        "valkey.fs.deny_writes",
        "fault",
        DenyWritesFault,
        config_validator(
            _DENY_FIELDS, extra=lambda config, spec, plan: validate_node_selector(config["nodes"]), requires_event=True
        ),
    ),
)
