import errno
import logging
import os
import re
import shutil
import socket
import subprocess
import time
import traceback
import uuid
from collections.abc import Iterable, Mapping
from typing import Any, Optional

import valkey

from ..models import ClusterConfig, ClusterConnection, NodeInfo, NodePlan, TopologyType
from ..utils.server_config import (
    build_server_config_arguments,
    canonicalize_server_config,
    normalize_server_option,
    server_config_values,
)
from ..utils.valkey_utils import query_cluster_info, valkey_client

logger = logging.getLogger()

# Matches the `sha=<hex>:<dirty>` field from `valkey-server --version`.
# Example output: "Valkey server v=8.1.0 sha=abc1234:0 malloc=jemalloc-5.3.0 ..."
_VERSION_SHA_RE = re.compile(r"sha=([0-9a-f]{7,40}):", re.IGNORECASE)


# Hard cap that keeps a fuzzing run from exhausting the host. It is deliberately
# paired with Valkey's default ``noeviction`` policy so that hitting the cap
# surfaces as write errors instead of silently evicting the data the
# validators check. Scenarios may override it through ``server_config``.
DEFAULT_MAXMEMORY = "500mb"


class PortUnavailableError(RuntimeError):
    """A planned node port is already in use by a process we did not start."""


class NodeIdentityError(RuntimeError):
    """A node port answered, but not from the Valkey process this orchestrator owns."""


def _port_unavailable_reason(host: str, port: int) -> Optional[str]:
    """Return why ``host:port`` cannot be used for a new server, or None if it is free.

    A connect probe catches listeners on any local address (including wildcard
    binds). A bind probe with SO_REUSEADDR, which Valkey itself uses, catches
    sockets bound to the exact address while ignoring TIME_WAIT leftovers.
    """
    try:
        with socket.create_connection((host, port), timeout=0.25):
            return "a server is already accepting connections"
    except OSError:
        pass

    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind((host, port))
    except OSError as exc:
        return errno.errorcode.get(exc.errno, str(exc)) if exc.errno else str(exc)
    finally:
        probe.close()
    return None


def verify_node_process_identity(node: NodeInfo, client: valkey.Valkey) -> None:
    """Raise NodeIdentityError unless the server behind ``client`` is ``node``'s own process."""
    expected_pid = node.pid
    if node.process is not None:
        expected_pid = node.process.pid
        if node.process.poll() is not None:
            raise NodeIdentityError(
                f"{node.node_id}: port {node.port} answered, but the process this orchestrator started "
                f"(pid {expected_pid}) has exited"
            )

    server_info = client.info("server")
    reported_pid = server_info.get("process_id") if isinstance(server_info, Mapping) else None
    try:
        reported_pid = int(reported_pid)
    except (TypeError, ValueError):
        raise NodeIdentityError(
            f"{node.node_id}: server on port {node.port} did not report a usable process_id ({reported_pid!r})"
        ) from None

    if reported_pid != expected_pid:
        raise NodeIdentityError(
            f"{node.node_id}: port {node.port} is served by pid {reported_pid}, not by the process this "
            f"orchestrator started (pid {expected_pid}); refusing to use a foreign server"
        )


def _decode(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def verify_cluster_node_identity(node: NodeInfo, client: valkey.Valkey) -> str:
    """Return CLUSTER MYID, raising NodeIdentityError if it differs from the saved node ID."""
    cluster_node_id = _decode(client.execute_command("CLUSTER", "MYID"))
    if node.cluster_node_id and cluster_node_id != node.cluster_node_id:
        raise NodeIdentityError(
            f"{node.node_id}: restarted server reports cluster node ID {cluster_node_id}, "
            f"expected {node.cluster_node_id}"
        )
    return cluster_node_id


def _coerce_topology_type(topology_type: Any) -> TopologyType:
    """Normalize enum/string topology values at orchestration boundaries."""
    if isinstance(topology_type, TopologyType):
        return topology_type
    return TopologyType(topology_type)


def detect_valkey_sha(binary: str) -> Optional[str]:
    """Return the git SHA the binary was built from, or None if unavailable.

    Runs ``<binary> --version`` and parses the ``sha=<hex>:<dirty>`` field.
    Release builds without git context emit ``sha=00000000:0`` — those return
    None so callers don't try to clone an all-zero commit.
    """
    try:
        out = subprocess.run(
            [binary, "--version"],
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        ).stdout
    except (OSError, subprocess.SubprocessError) as exc:
        logger.debug("Could not run %s --version: %s", binary, exc)
        return None
    m = _VERSION_SHA_RE.search(out)
    if not m:
        return None
    sha = m.group(1).lower()
    if set(sha) == {"0"}:
        return None
    return sha


class PortManager:
    """Manages port allocation for Valkey cluster nodes"""

    def __init__(self, base_port: int = 7000, max_ports: int = 1000):
        self.base_port = base_port
        self.available_ports = set(range(base_port, base_port + max_ports))
        self.allocated_ports: dict[str, tuple[int, int]] = {}

    def allocate_ports(self, node_id: str) -> tuple[int, int]:
        """Allocate client and bus ports for a node"""
        if not self.available_ports:
            raise Exception("No available ports!")

        client_port = min(self.available_ports)
        bus_port = client_port + 10000

        self.available_ports.discard(client_port)
        self.allocated_ports[node_id] = (client_port, bus_port)

        logger.debug(f"Allocated ports: {client_port}, {bus_port}")
        return client_port, bus_port

    def release_ports(self, node_id: str) -> None:
        """Release allocated ports for a node"""
        if node_id in self.allocated_ports:
            client_port, bus_port = self.allocated_ports[node_id]
            self.available_ports.add(client_port)
            del self.allocated_ports[node_id]
            logger.debug(f"Released ports: {client_port}, {bus_port}")


class ConfigurationManager:
    """Manages Valkey cluster planning, node spawning, and resource cleanup"""

    def __init__(self, clusterConfig: ClusterConfig, port_manager: PortManager, seed: Optional[int] = None):
        self.clusterConfig = clusterConfig
        self.port_manager = port_manager
        self.cluster_id = str(uuid.uuid4())[:8]
        self.seed = seed
        self._spawned_nodes: dict[str, NodeInfo] = {}
        # Cluster directories created under per-node data roots; removed with
        # the main cluster directory during cleanup.
        self._extra_cluster_dirs: set[str] = set()

    @property
    def spawned_nodes(self) -> list[NodeInfo]:
        """Return nodes currently owned by this manager."""
        return list(self._spawned_nodes.values())

    @staticmethod
    def resolve_binary_path(configured_binary: str, label: str = "valkey_binary") -> str:
        """Return an absolute, executable path for a configured binary path or command name."""
        # For absolute/relative paths, check existence and permissions directly
        if os.path.sep in configured_binary or configured_binary.startswith("."):
            if not os.path.isfile(configured_binary):
                raise FileNotFoundError(f"Configured {label} '{configured_binary}' does not exist.")
            if not os.access(configured_binary, os.X_OK):
                raise PermissionError(f"Configured {label} '{configured_binary}' exists but is not executable.")
            return os.path.abspath(configured_binary)
        # Bare command name — search PATH
        resolved_binary = shutil.which(configured_binary)
        if resolved_binary:
            return resolved_binary
        raise FileNotFoundError(f"Configured {label} '{configured_binary}' could not be found on PATH.")

    def resolve_node_binaries(self) -> None:
        """Resolve every per-node binary override to an executable path, in place."""
        self.clusterConfig.node_binaries = {
            selector: self.resolve_binary_path(binary, f"node_binaries.{selector}")
            for selector, binary in (self.clusterConfig.node_binaries or {}).items()
        }

    def setup_valkey_from_source(self, base_dir: str = "/tmp/valkey-build") -> str:
        """Clone and build Valkey binary, return path to valkey-server binary"""
        configured_binary = self.clusterConfig.valkey_binary
        if configured_binary:
            return self.resolve_binary_path(configured_binary)

        valkey_binary = shutil.which("valkey-server")
        if valkey_binary:
            return valkey_binary

        valkey_dir = os.path.join(base_dir, "valkey")
        valkey_binary = os.path.join(valkey_dir, "src", "valkey-server")

        # Check if binary already exists from previous build
        if os.path.exists(valkey_binary):
            return valkey_binary

        os.makedirs(base_dir, exist_ok=True)

        subprocess.run(["git", "clone", "https://github.com/valkey-io/valkey.git", valkey_dir], check=True)
        subprocess.run(["make"], cwd=valkey_dir, check=True)

        if not os.path.exists(valkey_binary):
            raise Exception(f"Build completed but binary not found at {valkey_binary}")

        return valkey_binary

    def create_node_plan(
        self,
        node_counter: int,
        role: str,
        shard_id: int,
        slot_start: Optional[int] = None,
        slot_end: Optional[int] = None,
        master_node_id: Optional[str] = None,
    ) -> NodePlan:
        """Create a node plan with allocated ports and configuration"""
        node_id = f"node-{node_counter}"
        client_port, bus_port = self.port_manager.allocate_ports(node_id)
        server_config = self.resolve_node_server_config(node_id, role)
        host = "127.0.0.1"
        if self.clusterConfig.addressing == "per_node_loopback":
            host = f"127.0.0.{node_counter + 2}"

        return NodePlan(
            node_id=node_id,
            role=role,
            shard_id=shard_id,
            port=client_port,
            bus_port=bus_port,
            slot_start=slot_start,
            slot_end=slot_end,
            master_node_id=master_node_id,
            server_config=server_config,
            valkey_binary=self._node_value(self.clusterConfig.node_binaries, node_id, role),
            data_root=self._node_value(self.clusterConfig.node_data_roots, node_id, role),
            host=host,
        )

    @staticmethod
    def _node_value(values: Optional[Mapping[str, str]], node_id: str, role: str) -> Optional[str]:
        """Return the exact-node value, else the role value, else None."""
        values = values or {}
        return values.get(node_id) or values.get(role)

    def resolve_node_server_config(self, node_id: str, role: str) -> dict[str, Any]:
        """Resolve global, role, and exact-node server configuration layers."""
        resolved: dict[str, Any] = {}
        node_configs = self.clusterConfig.node_configs or {}
        layers = (
            ("server_config", self.clusterConfig.server_config or {}),
            (f"node_configs[{role!r}]", node_configs.get(role, {})),
            (f"node_configs[{node_id!r}]", node_configs.get(node_id, {})),
        )

        for layer_name, layer in layers:
            if not isinstance(layer, Mapping):
                raise TypeError(f"{layer_name} must be a mapping, got {type(layer).__name__}")
            try:
                resolved.update(canonicalize_server_config(layer))
            except (TypeError, ValueError) as exc:
                error_type = TypeError if isinstance(exc, TypeError) else ValueError
                raise error_type(f"{layer_name}: {exc}") from exc

        return resolved

    def plan_topology(self) -> list[NodePlan]:
        """Plan nodes for cluster, standalone, or primary/replica topology."""
        topology_type = _coerce_topology_type(self.clusterConfig.topology_type)

        if topology_type == TopologyType.STANDALONE:
            logger.info("Planning standalone topology")
            return [self.create_node_plan(0, "primary", 0)]

        if topology_type == TopologyType.REPLICATION:
            logger.info(
                "Planning primary/replica topology with %s replica(s)",
                self.clusterConfig.replicas_per_shard,
            )
            primary = self.create_node_plan(0, "primary", 0)
            nodes = [primary]
            for node_counter in range(1, self.clusterConfig.replicas_per_shard + 1):
                nodes.append(
                    self.create_node_plan(
                        node_counter,
                        "replica",
                        0,
                        master_node_id=primary.node_id,
                    )
                )
            return nodes

        nodes = []
        node_counter = 0

        total_slots = 16384
        slots_per_shard = total_slots // self.clusterConfig.num_shards

        logger.info(
            f"Planning topology for {self.clusterConfig.num_shards} shards with {self.clusterConfig.replicas_per_shard} replica(s) each"
        )

        for shard_num in range(self.clusterConfig.num_shards):
            slot_start = shard_num * slots_per_shard

            if shard_num == self.clusterConfig.num_shards - 1:
                slot_end = total_slots - 1
            else:
                slot_end = slot_start + slots_per_shard - 1

            primary_node_plan = self.create_node_plan(node_counter, "primary", shard_num, slot_start, slot_end)
            nodes.append(primary_node_plan)
            logger.debug(
                f"{primary_node_plan.node_id}: primary, shard {shard_num}, "
                f"port {primary_node_plan.port}, slots {slot_start}-{slot_end}"
            )
            node_counter += 1

            for _ in range(self.clusterConfig.replicas_per_shard):
                replica_plan = self.create_node_plan(
                    node_counter, "replica", shard_num, master_node_id=primary_node_plan.node_id
                )
                nodes.append(replica_plan)
                logger.debug(
                    f"{replica_plan.node_id}: replica, shard {shard_num}, "
                    f"port {replica_plan.port}, master={primary_node_plan.node_id}"
                )
                node_counter += 1

        return nodes

    def create_node_directories(self, node_id: str, data_root: Optional[str] = None) -> tuple[str, str]:
        """Create directories for a node and return path.

        ``data_root`` places the node's data (RDB, AOF, nodes.conf) on another
        filesystem, e.g. a size-limited resource; logs stay under base_data_dir.
        """
        cluster_dir = os.path.join(data_root or self.clusterConfig.base_data_dir, f"cluster-{self.cluster_id}")
        if data_root:
            self._extra_cluster_dirs.add(cluster_dir)
        node_data_dir = os.path.join(cluster_dir, node_id, "data")
        log_dir = os.path.join(self.clusterConfig.base_data_dir, "logs")

        os.makedirs(node_data_dir, exist_ok=True)
        os.makedirs(log_dir, exist_ok=True)

        log_file = os.path.join(log_dir, f"{self.seed}-{node_id}.log")
        return node_data_dir, log_file

    @staticmethod
    def _normalize_server_option(raw_option: object) -> str:
        return normalize_server_option(raw_option)

    @staticmethod
    def _server_config_values(option: str, raw_value: object) -> Iterable[str]:
        return server_config_values(option, raw_value)

    def _build_server_config_arguments(self, server_config: Optional[Mapping[str, Any]]) -> list[str]:
        return build_server_config_arguments(server_config)

    def build_node_command(
        self,
        port: int,
        data_dir: str,
        log_file: str,
        server_config: Optional[Mapping[str, Any]] = None,
        valkey_binary: Optional[str] = None,
        host: str = "127.0.0.1",
    ) -> list[str]:
        """Build a shell-safe Valkey command for the configured topology."""
        topology_type = _coerce_topology_type(self.clusterConfig.topology_type)
        cluster_enabled = topology_type == TopologyType.CLUSTER
        command = [
            valkey_binary or self.clusterConfig.valkey_binary,
            "--port",
            str(port),
            "--bind",
            host,
        ]
        if host != "127.0.0.1":
            # Replication and cluster-bus connections must originate from the
            # node's own address so network faults can match node pairs.
            command.extend(["--bind-source-addr", host])
        command += [
            "--protected-mode",
            "no",
            "--cluster-enabled",
            "yes" if cluster_enabled else "no",
        ]

        if cluster_enabled:
            command.extend(
                [
                    "--cluster-config-file",
                    os.path.join(data_dir, "nodes.conf"),
                    "--cluster-node-timeout",
                    "1000",
                    "--cluster-require-full-coverage",
                    "no",
                ]
            )

        command.extend(
            [
                "--dir",
                data_dir,
                "--logfile",
                log_file,
                "--loglevel",
                "notice",
                "--appendonly",
                "yes",
                "--appendfilename",
                "appendonly.aof",
                "--save",
                "",
                "--maxmemory",
                DEFAULT_MAXMEMORY,
            ]
        )
        command.extend(self._build_server_config_arguments(server_config))
        return command

    def spawn_all_nodes(self, node_plans: list[NodePlan]) -> list[NodeInfo]:
        """Spawn all Valkey processes and wait for them to be ready"""
        logger.info("")
        logger.info(f"Spawning {len(node_plans)} nodes")
        node_info_list = []

        # Refuse to start anything if a planned port is already taken: a server
        # we did not start would otherwise answer readiness checks and receive
        # topology commands such as CLUSTER RESET HARD.
        for plan in node_plans:
            self.ensure_ports_available(plan.node_id, plan.port, plan.bus_port, host=plan.host)

        try:
            for plan in node_plans:
                logger.debug(f"Spawning {plan.node_id} on port {plan.port}")
                node_data_dir, log_file = self.create_node_directories(plan.node_id, plan.data_root)
                # Build command using the configuration resolved while planning.
                cmd = self.build_node_command(
                    plan.port,
                    node_data_dir,
                    log_file,
                    server_config=plan.server_config,
                    valkey_binary=plan.valkey_binary,
                    host=plan.host,
                )
                process = None
                try:
                    process = self._start_process(cmd, log_file)

                    node_info = NodeInfo(
                        node_id=plan.node_id,
                        host=plan.host,
                        role=plan.role,
                        shard_id=plan.shard_id,
                        port=plan.port,
                        bus_port=plan.bus_port,
                        pid=process.pid,
                        process=process,
                        data_dir=node_data_dir,
                        log_file=log_file,
                        slot_start=plan.slot_start,
                        slot_end=plan.slot_end,
                        master_node_id=plan.master_node_id,
                        server_config=dict(plan.server_config),
                        valkey_binary=plan.valkey_binary,
                    )

                    self._spawned_nodes[node_info.node_id] = node_info
                    node_info_list.append(node_info)
                except BaseException:
                    if process is not None and plan.node_id not in self._spawned_nodes:
                        self._terminate_untracked_process(process, plan.node_id)
                    raise
                logger.debug(f"Spawned {plan.node_id} with PID {process.pid}")

            logger.info("Waiting for all nodes to be ready")
            for node in node_info_list:
                deadline = time.monotonic() + 30
                ready = False

                while time.monotonic() < deadline:
                    if node.process.poll() is not None:
                        logger.info(f"Node {node.node_id} process died")
                        break

                    try:
                        with valkey_client(node.host, node.port, timeout=3.0, decode_responses=True) as client:
                            if client.ping():
                                verify_node_process_identity(node, client)
                                logger.debug(f"Node {node.node_id} is active")
                                ready = True
                                break
                    except (valkey.ConnectionError, valkey.TimeoutError):
                        pass

                    time.sleep(0.5)

                if not ready:
                    raise Exception(f"Node {node.node_id} failed to start{self._process_output_tail(node.log_file)}")

        except BaseException as exc:
            logger.error("Failed while spawning or waiting for nodes: %s", exc)
            self._cleanup_failed_spawn(node_info_list, node_plans)
            raise

        logger.info(f"All {len(node_info_list)} nodes are active")
        return node_info_list

    def ensure_ports_available(self, node_id: str, port: int, bus_port: int, host: str = "127.0.0.1") -> None:
        """Fail clearly when a node's client (or cluster bus) port is already in use."""
        ports = [("client", port)]
        if _coerce_topology_type(self.clusterConfig.topology_type) == TopologyType.CLUSTER:
            ports.append(("cluster bus", bus_port))

        for label, candidate in ports:
            reason = _port_unavailable_reason(host, candidate)
            if reason is not None:
                raise PortUnavailableError(
                    f"Cannot start {node_id}: {label} port {candidate} on {host} is unavailable ({reason}). "
                    "Stop the process using it or choose a different base_port."
                )

    @staticmethod
    def _process_output_path(log_file: str) -> Optional[str]:
        """Per-node file that captures valkey-server stdout/stderr."""
        if not log_file:
            return None
        root, _ = os.path.splitext(log_file)
        return f"{root}.stdio.log"

    def _start_process(self, cmd: list[str], log_file: str) -> subprocess.Popen:
        """Start valkey-server with stdout/stderr appended to a per-node file.

        Output is never left in an unread pipe, which could block the server
        once the pipe buffer fills.
        """
        output_path = self._process_output_path(log_file)
        if output_path is None:
            return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "ab") as output:
            return subprocess.Popen(cmd, stdout=output, stderr=subprocess.STDOUT)

    def _process_output_tail(self, log_file: str, lines: int = 20) -> str:
        """Return a short suffix with the last process output lines, for error messages."""
        output_path = self._process_output_path(log_file)
        if output_path is None:
            return ""
        try:
            with open(output_path, encoding="utf-8", errors="replace") as output:
                tail = output.read().splitlines()[-lines:]
        except OSError:
            return ""
        if not tail:
            return ""
        return f" (process output from {output_path}: " + " | ".join(line.strip() for line in tail) + ")"

    @staticmethod
    def _terminate_untracked_process(process: subprocess.Popen, node_id: str) -> None:
        """Best-effort termination before a process has a NodeInfo owner."""
        try:
            if process.poll() is not None:
                return
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        except BaseException as exc:
            logger.error("Failed to terminate untracked process for %s: %s", node_id, exc)

    def _cleanup_failed_spawn(self, spawned_nodes: list[NodeInfo], node_plans: list[NodePlan]) -> None:
        """Best-effort cleanup when spawning fails before ownership is returned.

        This method deliberately reports cleanup failures instead of raising:
        its caller is already handling a spawn/readiness exception or
        interruption, which must remain the exception observed by the caller.
        """
        nodes_by_id = dict(self._spawned_nodes)
        nodes_by_id.update({node.node_id: node for node in spawned_nodes})
        self._cleanup_resources(
            nodes_by_id.values(),
            [plan.node_id for plan in node_plans],
            context="failed spawn",
            raise_on_failure=False,
        )

    def terminate_node(self, node: NodeInfo) -> None:
        """Terminate a Valkey node process"""
        if node.process is None or node.process.poll() is not None:
            return

        logger.debug(f"Terminating {node.node_id} (PID {node.pid})")

        node.process.terminate()
        try:
            node.process.wait(timeout=5)
            logger.debug(f"{node.node_id} terminated")
        except subprocess.TimeoutExpired:
            node.process.kill()
            node.process.wait()
            logger.debug(f"{node.node_id} terminated")

    def restart_node(self, node: NodeInfo, wait_ready: bool = True, ready_timeout: float = 30.0) -> NodeInfo:
        """Restart a Valkey node process."""
        logger.info(f"Restarting {node.node_id} on port {node.port}")

        # Build command using centralized configuration. The node keeps its
        # address, binary, and resolved startup config across restarts.
        cmd = self.build_node_command(
            node.port,
            node.data_dir,
            node.log_file,
            server_config=node.server_config,
            valkey_binary=node.valkey_binary,
            host=node.host,
        )
        topology_type = _coerce_topology_type(self.clusterConfig.topology_type)
        if topology_type == TopologyType.REPLICATION and node.role == "replica":
            if not node.master_node_id:
                raise ValueError(f"Replica {node.node_id} has no configured primary")

            primary = self._spawned_nodes.get(node.master_node_id)
            if primary is None or primary.role != "primary":
                raise ValueError(
                    f"Cannot restart replica {node.node_id}: configured primary {node.master_node_id!r} is unavailable"
                )

            # Replication configured at runtime is not persisted by the process
            # command assembled above. Inject the orchestrator-owned startup
            # directive so even a non-blocking restart cannot briefly become a
            # writable primary.
            cmd.extend(["--replicaof", primary.host, str(primary.port)])

        # A restart replaces the node's process. Stop a still-running one first
        # so two servers never compete for the same port and data directory.
        if node.process is not None and node.process.poll() is None:
            logger.info(f"Stopping running {node.node_id} (PID {node.pid}) before restart")
            self.terminate_node(node)
        self.ensure_ports_available(node.node_id, node.port, node.bus_port, host=node.host)

        process = None
        try:
            # Start new process
            process = self._start_process(cmd, node.log_file)

            # Update node info with new process
            node.process = process
            node.pid = process.pid
            self._spawned_nodes[node.node_id] = node

            logger.info(f"Restarted {node.node_id} with new PID {process.pid}")

            # Wait for node to be ready if requested
            if wait_ready:
                deadline = time.monotonic() + max(ready_timeout, 0.0)
                ready = False

                while time.monotonic() < deadline:
                    if node.process.poll() is not None:
                        raise Exception(f"Node {node.node_id} process died during restart")

                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break

                    try:
                        expected_round_trips = 4 if topology_type == TopologyType.CLUSTER else 2
                        attempt_timeout = min(0.5, max(0.001, remaining / expected_round_trips))
                        with valkey_client(
                            node.host,
                            node.port,
                            timeout=attempt_timeout,
                            decode_responses=True,
                        ) as client:
                            if self._restart_topology_is_ready(node, client, topology_type):
                                logger.info(f"Node {node.node_id} is ready after restart")
                                ready = True
                                break
                    except NodeIdentityError:
                        raise
                    except Exception as exc:
                        logger.debug("Node %s is not topology-ready after restart: %s", node.node_id, exc)

                    remaining = deadline - time.monotonic()
                    if remaining > 0:
                        time.sleep(min(0.5, remaining))

                if not ready:
                    self.terminate_node(node)
                    process = None
                    raise Exception(
                        f"Node {node.node_id} failed to become ready within {ready_timeout:.2f}s"
                        f"{self._process_output_tail(node.log_file)}"
                    )

            return node

        except BaseException as e:
            logger.error(f"Failed to restart {node.node_id}: {e}")
            if process is not None and process.poll() is None:
                try:
                    self.terminate_node(node)
                except BaseException as cleanup_error:
                    logger.error("Failed to terminate %s after restart failure: %s", node.node_id, cleanup_error)
            raise

    @staticmethod
    def _normalize_info_value(value: object) -> str:
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="replace")
        return str(value).lower()

    def _restart_topology_is_ready(
        self,
        node: NodeInfo,
        client: valkey.Valkey,
        topology_type: TopologyType,
    ) -> bool:
        """Return whether a restarted node has rejoined its expected topology.

        Raises NodeIdentityError if the answering server is not the restarted
        process or, for cluster nodes, does not carry the node's saved ID.
        """
        if not client.ping():
            return False
        verify_node_process_identity(node, client)

        if topology_type == TopologyType.CLUSTER:
            server_info = client.info("cluster")
            cluster_enabled = self._normalize_info_value(server_info.get("cluster_enabled", 0))
            cluster_info = query_cluster_info(client)
            cluster_state = self._normalize_info_value(cluster_info.get("cluster_state", ""))
            try:
                known_nodes = int(cluster_info.get("cluster_known_nodes", 0))
            except (TypeError, ValueError):
                return False

            expected_nodes = max(len(self._spawned_nodes), 1)
            if cluster_enabled not in {"1", "yes", "true"} or cluster_state != "ok" or known_nodes < expected_nodes:
                return False

            cluster_node_id = verify_cluster_node_identity(node, client)
            cluster_nodes = client.execute_command("CLUSTER", "NODES")
            if isinstance(cluster_nodes, bytes):
                cluster_nodes = cluster_nodes.decode("utf-8", errors="replace")

            self_is_healthy = False
            connected_members = 0
            for line in str(cluster_nodes).splitlines():
                fields = line.split()
                if len(fields) < 8:
                    continue
                flags = set(fields[2].split(","))
                unhealthy_flags = {"fail", "fail?", "handshake", "noaddr"}
                member_is_healthy = flags.isdisjoint(unhealthy_flags) and fields[7] == "connected"
                if member_is_healthy:
                    connected_members += 1
                if fields[0] != str(cluster_node_id):
                    continue
                self_is_healthy = "myself" in flags and member_is_healthy

            minimum_connected_members = 1 if expected_nodes == 1 else 2
            return self_is_healthy and connected_members >= minimum_connected_members

        replication_info = client.info("replication")
        role = self._normalize_info_value(replication_info.get("role", ""))

        if topology_type == TopologyType.REPLICATION and node.role == "replica":
            master_link_status = self._normalize_info_value(replication_info.get("master_link_status", ""))
            if role not in {"slave", "replica"} or master_link_status != "up":
                return False

            if not node.master_node_id:
                return False
            primary = self._spawned_nodes.get(node.master_node_id)
            if primary is None:
                return False

            master_host = self._normalize_info_value(replication_info.get("master_host", ""))
            try:
                master_port = int(replication_info.get("master_port"))
            except (TypeError, ValueError):
                return False
            return master_host == primary.host.lower() and master_port == primary.port

        if topology_type == TopologyType.REPLICATION and node.role == "primary":
            if role not in {"master", "primary"}:
                return False
            expected_replicas = sum(
                other.role == "replica" and other.master_node_id in {None, node.node_id}
                for other in self._spawned_nodes.values()
            )
            connected_replicas = replication_info.get(
                "connected_slaves",
                replication_info.get("connected_replicas", 0),
            )
            try:
                return int(connected_replicas) >= expected_replicas
            except (TypeError, ValueError):
                return False

        return node.role == "primary" and role in {"master", "primary"}

    def cleanup_cluster(self, nodes_in_cluster: list[NodeInfo]) -> None:
        """Clean up every node and resource, then report any failures."""
        logger.info(f"Cleaning up cluster {self.cluster_id}")
        self._cleanup_resources(
            nodes_in_cluster,
            [node.node_id for node in nodes_in_cluster],
            context=f"cluster {self.cluster_id}",
            raise_on_failure=True,
        )
        logger.debug(f"Cluster {self.cluster_id} cleaned up")

    def _cleanup_resources(
        self,
        nodes: Iterable[NodeInfo],
        node_ids: Iterable[str],
        *,
        context: str,
        raise_on_failure: bool,
    ) -> list[tuple[str, BaseException]]:
        """Attempt all lifecycle cleanup actions regardless of individual failures."""
        failures: list[tuple[str, BaseException]] = []

        for node in nodes:
            try:
                self.terminate_node(node)
            except BaseException as exc:
                failures.append((f"terminate {node.node_id}", exc))

        for node_id in dict.fromkeys(node_ids):
            try:
                self.port_manager.release_ports(node_id)
            except BaseException as exc:
                failures.append((f"release ports for {node_id}", exc))
            finally:
                self._spawned_nodes.pop(node_id, None)

        if self.clusterConfig.enable_cleanup:
            cluster_dirs = [os.path.join(self.clusterConfig.base_data_dir, f"cluster-{self.cluster_id}")]
            cluster_dirs.extend(sorted(self._extra_cluster_dirs))
            for cluster_dir in cluster_dirs:
                try:
                    cluster_dir_exists = os.path.exists(cluster_dir)
                except BaseException as exc:
                    failures.append((f"inspect {cluster_dir}", exc))
                    cluster_dir_exists = False

                if cluster_dir_exists:
                    try:
                        shutil.rmtree(cluster_dir)
                        logger.info("Deleted data directory")
                    except BaseException as exc:
                        failures.append((f"delete {cluster_dir}", exc))

        for action, exc in failures:
            logger.error("Cleanup action failed during %s (%s): %s", context, action, exc)

        if failures and raise_on_failure:
            interrupt = next((exc for _, exc in failures if not isinstance(exc, Exception)), None)
            if interrupt is not None:
                raise interrupt

            actions = ", ".join(action for action, _ in failures)
            raise RuntimeError(f"Cleanup for {context} completed with failures: {actions}") from failures[0][1]

        return failures


class ClusterManager:
    """Manages Valkey cluster formation and validates cluster health. Works with nodes that have been spawned by ConfigurationManager"""

    def __init__(self):
        self.connections: dict[str, valkey.Valkey] = {}

    def get_client(self, node: NodeInfo) -> valkey.Valkey:
        """Get or create Valkey client connection for a node"""
        if node.node_id not in self.connections:
            self.connections[node.node_id] = valkey.Valkey(
                host=node.host, port=node.port, socket_timeout=5, decode_responses=True
            )
        return self.connections[node.node_id]

    def get_cluster_info(self, node: NodeInfo) -> dict[str, str]:
        """Get cluster info from a node and parse into dictionary"""
        client = self.get_client(node)
        return query_cluster_info(client)

    def cluster_meet(self, nodes_in_cluster: list[NodeInfo], timeout: int = 30) -> None:
        """Connect cluster nodes and wait for convergence"""
        if len(nodes_in_cluster) < 2:
            return

        first_node = nodes_in_cluster[0]
        first_node_client = self.get_client(first_node)

        logger.info("")
        logger.info(
            f"Connecting {len(nodes_in_cluster)} nodes with CLUSTER MEET using {first_node.node_id} as starting node"
        )

        for node in nodes_in_cluster[1:]:
            logger.debug(f"Meeting {node.node_id} (port {node.port})")
            first_node_client.execute_command("CLUSTER", "MEET", node.host, node.port)
            time.sleep(0.1)

        logger.info("CLUSTER MEET complete")

        # Wait for convergence
        expected_count = len(nodes_in_cluster)
        deadline = time.time() + timeout

        logger.info(f"Waiting for cluster convergence for all {expected_count} nodes (timeout: {timeout:.2f}s)")

        while time.time() < deadline:
            all_converged = True

            for node in nodes_in_cluster:
                try:
                    info_dict = self.get_cluster_info(node)
                    known_nodes = int(info_dict.get("cluster_known_nodes", 0))

                    if known_nodes < expected_count:
                        all_converged = False

                except Exception as e:
                    logger.error(f"Error checking {node.node_id}: {e}")
                    all_converged = False

            if all_converged:
                logger.info("All nodes see each other and are apart of the cluster")
                return

            time.sleep(1)

        raise Exception(f"Cluster failed to converge within {timeout:.2f}s")

    def reset_cluster_state(self, nodes_in_cluster: list[NodeInfo]) -> None:
        """Reset cluster state on all nodes"""
        logger.debug("Resetting cluster state")
        for node in nodes_in_cluster:
            client = self.get_client(node)
            # CLUSTER RESET HARD wipes a node's identity and data. Only ever send
            # it to a process this orchestrator started.
            verify_node_process_identity(node, client)
            client.execute_command("CLUSTER", "RESET", "HARD")
        time.sleep(1)

    def assign_and_verify_slots(self, nodes_in_cluster: list[NodeInfo]) -> dict[str, str]:
        """Assign hash slots to primary nodes and verify assignment"""
        primary_nodes = [node for node in nodes_in_cluster if node.role == "primary"]

        if not primary_nodes:
            logger.info("No primary nodes to assign slots to")
            return {}

        logger.info("")
        logger.info(f"Assigning slots to {len(primary_nodes)} primaries")

        node_ids = {}

        for primary in primary_nodes:
            client = self.get_client(primary)

            slots = list(range(primary.slot_start, primary.slot_end + 1))
            logger.debug(f"Assigning slots {primary.slot_start}-{primary.slot_end} to {primary.node_id}")

            client.execute_command("CLUSTER", "ADDSLOTS", *slots)

            cluster_node_id = client.execute_command("CLUSTER", "MYID")
            node_ids[primary.node_id] = cluster_node_id
            primary.cluster_node_id = cluster_node_id

            logger.debug(f"{primary.node_id} cluster ID: {cluster_node_id[:8]}")

        logger.info("Slot assignment complete")

        # Verify slots were assigned correctly with retry logic
        max_retries = 10
        retry_delay = 2.0

        for attempt in range(max_retries):
            time.sleep(retry_delay)

            info_dict = self.get_cluster_info(nodes_in_cluster[0])
            slots_assigned = int(info_dict.get("cluster_slots_assigned", 0))
            slots_fail = int(info_dict.get("cluster_slots_fail", 0))

            if attempt == 0 or attempt == max_retries - 1:
                logger.debug(f"Slot verification (attempt {attempt + 1}/{max_retries}):")
                logger.debug(f"Slots assigned: {slots_assigned}/16384")
                logger.debug(f"Slots failed: {slots_fail}")

            if slots_assigned == 16384 and slots_fail == 0:
                logger.info(f"Slot assignment verified successfully after {attempt + 1} attempts")
                return node_ids

            if attempt < max_retries - 1:
                logger.debug(f"Waiting for slot propagation... ({slots_assigned}/16384)")

        # Final check failed
        raise Exception(
            f"Slot assignment failed after {max_retries} attempts: {slots_assigned}/16384 assigned, {slots_fail} failed"
        )

    def setup_and_sync_replication(
        self, nodes_in_cluster: list[NodeInfo], primary_ids: dict[str, str], timeout: int = 60
    ) -> None:
        """Configure replication and wait for replicas to sync"""
        replicas = [node for node in nodes_in_cluster if node.role == "replica"]

        if not replicas:
            logger.info("No replicas to configure")
            return

        logger.info("")
        logger.info(f"Configuring {len(replicas)} replicas")

        for replica in replicas:
            master_cluster_id = primary_ids.get(replica.master_node_id)

            if not master_cluster_id:
                logger.info(f"Could not find master for {replica.node_id}")
                continue

            logger.debug(f"Configuring {replica.node_id} to replicate {replica.master_node_id}")

            client = self.get_client(replica)
            client.execute_command("CLUSTER", "REPLICATE", master_cluster_id)
            replica.cluster_node_id = client.execute_command("CLUSTER", "MYID")

            logger.debug("Replication configured")

        logger.info(f"Waiting for {len(replicas)} replicas to sync (timeout: {timeout:.2f}s)")
        deadline = time.time() + timeout

        while time.time() < deadline:
            all_synced = True

            for replica in replicas:
                try:
                    client = self.get_client(replica)
                    info = client.info("replication")

                    master_link = info.get("master_link_status")

                    if master_link != "up":
                        all_synced = False

                except Exception as e:
                    logger.info(f"Error checking {replica.node_id}: {e}")
                    all_synced = False

            if all_synced:
                logger.info("All replicas are in sync with their master")
                return

            time.sleep(1)

        raise Exception(f"Replicas failed to sync within {timeout:.2f}s")

    @staticmethod
    def _primary_replica_layout(nodes_in_cluster: list[NodeInfo]) -> tuple[NodeInfo, list[NodeInfo]]:
        primaries = [node for node in nodes_in_cluster if node.role == "primary"]
        replicas = [node for node in nodes_in_cluster if node.role == "replica"]

        if len(primaries) != 1:
            raise ValueError(f"Primary/replica topology requires exactly one primary, found {len(primaries)}")
        if len(primaries) + len(replicas) != len(nodes_in_cluster):
            raise ValueError("Primary/replica topology contains a node with an unsupported role")

        primary = primaries[0]
        for replica in replicas:
            if replica.master_node_id and replica.master_node_id != primary.node_id:
                raise ValueError(f"Replica {replica.node_id} targets unknown primary {replica.master_node_id!r}")
        return primary, replicas

    def setup_primary_replica_replication(self, nodes_in_cluster: list[NodeInfo]) -> None:
        """Configure non-cluster replication using REPLICAOF."""
        primary, replicas = self._primary_replica_layout(nodes_in_cluster)
        self.get_client(primary).execute_command("REPLICAOF", "NO", "ONE")

        for replica in replicas:
            logger.debug(
                "Configuring %s to replicate %s at %s:%s",
                replica.node_id,
                primary.node_id,
                primary.host,
                primary.port,
            )
            self.get_client(replica).execute_command(
                "REPLICAOF",
                primary.host,
                primary.port,
            )

    @staticmethod
    def _replication_role(info: Mapping[str, Any]) -> str:
        role = info.get("role", "")
        if isinstance(role, bytes):
            role = role.decode("utf-8", errors="replace")
        return str(role).lower()

    def _standalone_is_healthy(self, node: NodeInfo) -> bool:
        info = self.get_client(node).info("replication")
        return self._replication_role(info) in {"master", "primary"}

    def validate_standalone(
        self,
        nodes_in_topology: list[NodeInfo],
        timeout: float = 30.0,
        interval: float = 1.0,
    ) -> bool:
        """Validate a single reachable, writable standalone primary."""
        if len(nodes_in_topology) != 1 or nodes_in_topology[0].role != "primary":
            return False

        node = nodes_in_topology[0]
        deadline = time.time() + max(timeout, 0.0)
        while True:
            try:
                if self._standalone_is_healthy(node):
                    logger.info("Standalone topology is healthy")
                    return True
            except Exception as exc:
                logger.debug("Standalone node %s is not ready: %s", node.node_id, exc)

            remaining = deadline - time.time()
            if remaining <= 0:
                return False
            time.sleep(min(max(interval, 0.0), remaining))

    def _primary_replica_is_healthy(self, primary: NodeInfo, replicas: list[NodeInfo]) -> bool:
        primary_info = self.get_client(primary).info("replication")
        if self._replication_role(primary_info) not in {"master", "primary"}:
            return False

        connected_replicas = primary_info.get(
            "connected_slaves",
            primary_info.get("connected_replicas", len(replicas)),
        )
        try:
            if int(connected_replicas) < len(replicas):
                return False
        except (TypeError, ValueError):
            return False

        for replica in replicas:
            replica_info = self.get_client(replica).info("replication")
            if self._replication_role(replica_info) not in {"slave", "replica"}:
                return False
            if replica_info.get("master_link_status") != "up":
                return False

            master_host = replica_info.get("master_host")
            if isinstance(master_host, bytes):
                master_host = master_host.decode("utf-8", errors="replace")
            if str(master_host) != primary.host:
                return False

            try:
                master_port = int(replica_info.get("master_port"))
            except (TypeError, ValueError):
                return False
            if master_port != primary.port:
                return False

        return True

    def validate_primary_replica(
        self,
        nodes_in_topology: list[NodeInfo],
        timeout: float = 30.0,
        interval: float = 1.0,
    ) -> bool:
        """Validate primary/replica roles, links, and configured upstreams."""
        if not nodes_in_topology:
            return False
        try:
            primary, replicas = self._primary_replica_layout(nodes_in_topology)
        except ValueError as exc:
            logger.warning("Invalid primary/replica topology: %s", exc)
            return False

        deadline = time.time() + max(timeout, 0.0)
        while True:
            try:
                if self._primary_replica_is_healthy(primary, replicas):
                    logger.info("Primary/replica topology is healthy")
                    return True
            except Exception as exc:
                logger.debug("Primary/replica topology is not ready: %s", exc)

            remaining = deadline - time.time()
            if remaining <= 0:
                return False
            time.sleep(min(max(interval, 0.0), remaining))

    def check_replication_links(self, nodes_in_cluster: list[NodeInfo]) -> bool:
        """Check if all replica master_link_status is 'up'"""
        for node in nodes_in_cluster:
            try:
                client = self.get_client(node)
                info = client.info("replication")

                if info.get("role") == "slave" and info.get("master_link_status") != "up":
                    return False
            except Exception:
                return False

        return True

    def validate_cluster(
        self,
        nodes_in_cluster: list[NodeInfo],
        timeout: float = 30.0,
        interval: float = 1.0,
        topology_type: TopologyType = TopologyType.CLUSTER,
    ) -> bool:
        topology_type = _coerce_topology_type(topology_type)
        if topology_type == TopologyType.STANDALONE:
            return self.validate_standalone(nodes_in_cluster, timeout=timeout, interval=interval)
        if topology_type == TopologyType.REPLICATION:
            return self.validate_primary_replica(nodes_in_cluster, timeout=timeout, interval=interval)

        if not nodes_in_cluster:
            return False

        logger.info("")
        logger.info("CLUSTER VALIDATION")
        deadline = time.time() + timeout
        last_states: list[dict[str, object]] = []
        last_unreachable: list[str] = []

        while time.time() < deadline:
            node_states = []
            unreachable_nodes = []

            for node in nodes_in_cluster:
                try:
                    info_dict = self.get_cluster_info(node)

                    cluster_state = info_dict.get("cluster_state")
                    slots_assigned = int(info_dict.get("cluster_slots_assigned", 0))
                    slots_fail = int(info_dict.get("cluster_slots_fail", 0))

                    node_state = {
                        "node_id": node.node_id,
                        "state": cluster_state,
                        "slots_assigned": slots_assigned,
                        "slots_fail": slots_fail,
                    }
                    node_states.append(node_state)

                    logger.debug(
                        f"{node.node_id}: state={cluster_state}, slots={slots_assigned}/16384, fail={slots_fail}"
                    )

                except Exception as e:
                    unreachable_nodes.append(node.node_id)
                    logger.debug(f"Expected node {node.node_id} is unreachable: {e}")

            if unreachable_nodes:
                last_unreachable = unreachable_nodes
                last_states = node_states
                time.sleep(interval)
                continue

            if not node_states:
                time.sleep(interval)
                continue

            first_state = node_states[0]
            consensus = all(
                state["state"] == first_state["state"]
                and state["slots_assigned"] == first_state["slots_assigned"]
                and state["slots_fail"] == first_state["slots_fail"]
                for state in node_states
            )

            if consensus:
                is_healthy = (
                    first_state["state"] == "ok"
                    and first_state["slots_assigned"] == 16384
                    and first_state["slots_fail"] == 0
                )

                # Check replica sync status
                replicas_synced = self.check_replication_links(nodes_in_cluster)

                if is_healthy and replicas_synced:
                    logger.info(f"Cluster is Healthy (all {len(node_states)} nodes reachable and consistent)")
                    return True

                # Consensus but cluster not yet healthy - wait a bit more before failing
                last_states = node_states
            else:
                last_states = node_states

            time.sleep(interval)

        # If we reach here, validation failed after timeout
        if last_unreachable:
            logger.warning(
                f"Validation failed: {len(last_unreachable)} expected node(s) unreachable: {', '.join(last_unreachable)}"
            )
            return False

        if last_states:
            first_state = last_states[0]
            consensus = all(
                state["state"] == first_state["state"]
                and state["slots_assigned"] == first_state["slots_assigned"]
                and state["slots_fail"] == first_state["slots_fail"]
                for state in last_states
            )

            if not consensus:
                logger.warning("Validation failed: Cluster nodes have inconsistent views:")
                for state in last_states:
                    logger.warning(
                        f"  {state['node_id']}: {state['state']}, {state['slots_assigned']}/16384, fail={state['slots_fail']}"
                    )
                return False

            logger.info(
                f"Cluster is Not Healthy: state={first_state['state']}, slots={first_state['slots_assigned']}/16384"
            )

        else:
            logger.warning("Could not reach any nodes for validation")

        return False

    def form_standalone(self, nodes_in_topology: list[NodeInfo], cluster_id: str) -> ClusterConnection:
        """Form and validate a standalone deployment."""
        if not self.validate_standalone(nodes_in_topology):
            raise Exception("Standalone validation failed")
        return ClusterConnection(nodes_in_topology, cluster_id, topology_type=TopologyType.STANDALONE)

    def form_primary_replica(self, nodes_in_topology: list[NodeInfo], cluster_id: str) -> ClusterConnection:
        """Form and validate a non-cluster primary/replica deployment."""
        self.setup_primary_replica_replication(nodes_in_topology)
        if not self.validate_primary_replica(nodes_in_topology):
            raise Exception("Primary/replica validation failed")
        return ClusterConnection(nodes_in_topology, cluster_id, topology_type=TopologyType.REPLICATION)

    def form_cluster(
        self,
        nodes_in_cluster: list[NodeInfo],
        cluster_id: str,
        topology_type: TopologyType = TopologyType.CLUSTER,
    ) -> ClusterConnection:
        """Form and validate the requested deployment topology."""
        topology_type = _coerce_topology_type(topology_type)
        try:
            if topology_type == TopologyType.STANDALONE:
                return self.form_standalone(nodes_in_cluster, cluster_id)
            if topology_type == TopologyType.REPLICATION:
                return self.form_primary_replica(nodes_in_cluster, cluster_id)

            self.reset_cluster_state(nodes_in_cluster)
            self.cluster_meet(nodes_in_cluster)

            primary_ids = self.assign_and_verify_slots(nodes_in_cluster)

            self.setup_and_sync_replication(nodes_in_cluster, primary_ids)

            if not self.validate_cluster(nodes_in_cluster):
                raise Exception("Cluster validation failed")

            return ClusterConnection(nodes_in_cluster, cluster_id, topology_type=TopologyType.CLUSTER)

        except BaseException as e:
            logger.info(f"{topology_type.value} topology formation failed: {e}")
            if isinstance(e, Exception):
                traceback.print_exc()
            self.close_connections()
            if not isinstance(e, Exception):
                raise
            return None

    def form_topology(
        self,
        nodes_in_topology: list[NodeInfo],
        topology_id: str,
        topology_type: TopologyType,
    ) -> ClusterConnection:
        """Explicit topology-oriented alias for form_cluster."""
        return self.form_cluster(nodes_in_topology, topology_id, topology_type=topology_type)

    def validate_topology(
        self,
        nodes_in_topology: list[NodeInfo],
        topology_type: TopologyType,
        timeout: float = 30.0,
        interval: float = 1.0,
    ) -> bool:
        """Explicit topology-oriented alias for validate_cluster."""
        return self.validate_cluster(
            nodes_in_topology,
            timeout=timeout,
            interval=interval,
            topology_type=topology_type,
        )

    def close_connections(self) -> None:
        """Close and discard every cached client, preserving interruptions."""
        failures: list[BaseException] = []
        clients = list(self.connections.values())
        self.connections.clear()

        for client in clients:
            try:
                client.close()
            except BaseException as exc:
                failures.append(exc)
                logger.error("Failed to close Valkey client: %s", exc)

        interrupt = next((exc for exc in failures if not isinstance(exc, Exception)), None)
        if interrupt is not None:
            raise interrupt
