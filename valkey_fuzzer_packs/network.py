"""Network faults between topology nodes (Linux iptables, per-node loopback).

``valkey.net.partition`` isolates selected nodes from the rest of the topology:
it drops every packet between each isolated node's address and each other
node's address.  Client traffic from the harness (127.0.0.1) is unaffected, so
clients can keep talking to both sides — the classic split-brain setup.

Rules live in a dedicated iptables chain jumped to from INPUT, so healing is
atomic and cleanup never touches unrelated rules.  The topology must use
``addressing: per_node_loopback``; the ``valkey.host.capability`` probe with
``requires: [linux, root, iptables, per_node_loopback]`` turns hosts that
cannot do this into ``SKIP_UNSUPPORTED`` before anything starts.
"""

import hashlib
import os
import re
import shutil
import socket
import subprocess
import threading
import time
from typing import Any, ClassVar, Optional

from .common import (
    ComponentConfigError,
    ComponentSpec,
    Field,
    RunPlan,
    config_validator,
    select_nodes,
    to_plain_data,
    topology,
    validate_node_selector,
)
from .faults import _EventFault


def iptables_supported() -> tuple[bool, str]:
    if os.uname().sysname != "Linux":
        return False, "network partitions require Linux iptables"
    if os.geteuid() != 0:
        return False, "network partitions require root"
    binary = shutil.which("iptables")
    if binary is None:
        return False, "iptables is not installed"
    probe = subprocess.run([binary, "-w", "-L", "INPUT", "-n"], capture_output=True, text=True)
    if probe.returncode != 0:
        return False, f"iptables is not usable: {probe.stderr.strip()[:200]}"
    return True, ""


def per_node_loopback_supported() -> tuple[bool, str]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.2", 0))
    except OSError as exc:
        return False, f"cannot bind 127.0.0.2 ({exc}); per-node loopback addressing is unavailable"
    finally:
        sock.close()
    return True, ""


def _iptables(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["iptables", "-w", *args], capture_output=True, text=True)


_CHAIN_PATTERN = re.compile(r"^-N (VF-(\d+)-[0-9a-f]{8})$")


def chain_name(instance_id: str, pid: Optional[int] = None) -> str:
    """``VF-<owner pid>-<hash>``: unique per fault instance, under 28 characters."""

    digest = hashlib.sha256(instance_id.encode("utf-8")).hexdigest()[:8]
    return f"VF-{os.getpid() if pid is None else pid}-{digest}"


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def remove_stale_chains() -> list[str]:
    """Delete partition chains left by runs whose process no longer exists."""

    listing = _iptables("-S")
    removed = []
    if listing.returncode != 0:
        return removed
    for line in listing.stdout.splitlines():
        match = _CHAIN_PATTERN.match(line.strip())
        if match is None or _pid_alive(int(match.group(2))):
            continue
        chain = match.group(1)
        while _iptables("-C", "INPUT", "-j", chain).returncode == 0:
            if _iptables("-D", "INPUT", "-j", chain).returncode != 0:
                break
        _iptables("-F", chain)
        if _iptables("-X", chain).returncode == 0:
            removed.append(chain)
    return removed


_PARTITION_FIELDS = {
    "isolate": Field((str, list)),
    "peers": Field((str, list), default="others"),
    "heal_event": Field(str, allow_none=True, default=None),
    "duration_s": Field(float, allow_none=True, default=None, minimum=0.01, maximum=3600),
    "delay_s": Field(float, default=0.0, minimum=0, maximum=600),
}


def _validate_partition(config: dict, spec: ComponentSpec, plan: RunPlan) -> None:
    validate_node_selector(config["isolate"], "isolate")
    if config["peers"] != "others":
        validate_node_selector(config["peers"], "peers")
    document = to_plain_data(plan.topology_document)
    if document.get("addressing") != "per_node_loopback":
        raise ComponentConfigError("valkey.net.partition requires topology.addressing: per_node_loopback")
    if config["heal_event"] is not None and config["duration_s"] is not None:
        raise ComponentConfigError("set heal_event or duration_s, not both")


class PartitionFault(_EventFault):
    fields: ClassVar[dict] = _PARTITION_FIELDS

    def _prepare(self, context: Any, spec: ComponentSpec) -> None:
        handle = topology(context)
        isolated = select_nodes(handle, self._config["isolate"])
        if self._config["peers"] == "others":
            peers = [node.node_id for node in handle.nodes if node.node_id not in isolated]
        else:
            peers = [node_id for node_id in select_nodes(handle, self._config["peers"]) if node_id not in isolated]
        if not isolated or not peers:
            raise ValueError("a partition needs at least one isolated node and one peer")
        self._isolated = isolated
        self._peers = peers
        self._addresses = {node.node_id: node.host for node in handle.nodes}
        if len(set(self._addresses.values())) != len(self._addresses):
            raise ValueError("nodes share an address; use topology.addressing: per_node_loopback")
        self._chain = chain_name(spec.instance_id)
        self._stale_chains_removed = remove_stale_chains()
        if _iptables("-S", self._chain).returncode == 0:
            # Same process, same instance: a previous run in this process leaked it.
            self._remove_chain()
        self._lock_heal = threading.Lock()
        self._installed = False
        self._healed_at = None
        self._timer = None
        self._heal_subscription = None
        if self._config["heal_event"]:
            self._heal_subscription = context.event_bus.subscribe(self._config["heal_event"], self._heal_on_event)

    def _fire(self, context: Any, spec: ComponentSpec, event: Any) -> dict[str, Any]:
        rules = []
        result = _iptables("-N", self._chain)
        if result.returncode != 0:
            return {"applied": False, "error": f"cannot create chain: {result.stderr.strip()}"}
        for isolated in self._isolated:
            for peer in self._peers:
                a, b = self._addresses[isolated], self._addresses[peer]
                for source, destination in ((a, b), (b, a)):
                    rule = ("-A", self._chain, "-s", source, "-d", destination, "-j", "DROP")
                    added = _iptables(*rule)
                    if added.returncode != 0:
                        self._remove_chain()
                        return {"applied": False, "error": f"iptables {' '.join(rule)}: {added.stderr.strip()}"}
                    rules.append(f"{source}->{destination}")
        jump = _iptables("-I", "INPUT", "1", "-j", self._chain)
        if jump.returncode != 0:
            self._remove_chain()
            return {"applied": False, "error": f"cannot install jump: {jump.stderr.strip()}"}
        self._installed = True
        context.event_bus.publish(
            "net.partitioned",
            source=spec.instance_id,
            payload={"isolated": self._isolated, "peers": self._peers},
        )
        if self._config["duration_s"] is not None:
            self._timer = threading.Timer(self._config["duration_s"], self._heal, kwargs={"reason": "duration"})
            self._timer.daemon = True
            self._timer.start()
        return {
            "applied": True,
            "targets": self._isolated,
            "isolated": self._isolated,
            "peers": self._peers,
            "addresses": self._addresses,
            "chain": self._chain,
            "rules": rules,
            "stale_chains_removed": self._stale_chains_removed,
        }

    def _remove_chain(self) -> list[str]:
        errors = []
        while _iptables("-C", "INPUT", "-j", self._chain).returncode == 0:
            removed = _iptables("-D", "INPUT", "-j", self._chain)
            if removed.returncode != 0:
                errors.append(removed.stderr.strip())
                break
        for args in (("-F", self._chain), ("-X", self._chain)):
            result = _iptables(*args)
            if result.returncode != 0 and "No chain" not in result.stderr and "does not exist" not in result.stderr:
                errors.append(result.stderr.strip())
        return errors

    def _heal(self, reason: str) -> None:
        with self._lock_heal:
            if not self._installed or self._healed_at is not None:
                return
            errors = self._remove_chain()
            if errors:
                raise RuntimeError(f"partition heal failed: {errors}")
            self._healed_at = time.time()
            self._heal_reason = reason
        self._context.event_bus.publish(
            "net.healed", source=self._spec.instance_id, payload={"reason": reason, "isolated": self._isolated}
        )

    def _heal_on_event(self, event: Any) -> None:
        self._heal("event")

    def _recover(self, context: Any, spec: ComponentSpec, record: dict[str, Any]) -> None:
        if self._heal_subscription is not None:
            context.event_bus.unsubscribe(self._heal_subscription)
        if self._timer is not None:
            self._timer.cancel()
        if self._healed_at is None and self._installed:
            # Healing at cleanup must not publish into a finishing run.
            with self._lock_heal:
                if self._healed_at is None:
                    errors = self._remove_chain()
                    if errors:
                        raise RuntimeError(f"partition cleanup failed: {errors}")
                    self._healed_at = time.time()
                    self._heal_reason = "cleanup"
        if self._healed_at is not None:
            record["healed_at"] = self._healed_at
            record["heal_reason"] = getattr(self, "_heal_reason", None)
            record["partition_seconds"] = round(self._healed_at - record.get("applied_at", self._healed_at), 3)


REGISTRATIONS = (
    (
        "valkey.net.partition",
        "fault",
        PartitionFault,
        config_validator(_PARTITION_FIELDS, extra=_validate_partition, requires_event=True),
    ),
)
