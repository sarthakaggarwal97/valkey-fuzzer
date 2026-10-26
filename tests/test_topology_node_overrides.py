"""Per-node binaries, data roots, and loopback addressing for local topologies."""

import hashlib
import os
import signal
import subprocess
import sys
from unittest.mock import Mock

import pytest

from valkey_fuzzer.cluster_orchestrator.orchestrator import ConfigurationManager, PortManager
from valkey_fuzzer.extensibility import ProvisionedTopology
from valkey_fuzzer.local_topology_provider import LocalValkeyTopologyHandle, topology_metadata
from valkey_fuzzer.models import ClusterConfig, ClusterInstance, NodeInfo, TopologyType
from valkey_fuzzer.utils.topology_config import cluster_config_from_topology_document


def _manager(**overrides):
    config = ClusterConfig(num_shards=3, replicas_per_shard=1, topology_type=TopologyType.CLUSTER, **overrides)
    return ConfigurationManager(config, PortManager(base_port=7000), seed=1)


def test_topology_document_accepts_node_binaries_data_roots_and_addressing():
    config = cluster_config_from_topology_document(
        {
            "type": "cluster",
            "node_binaries": {"replica": "/opt/old/valkey-server", "node-0": "/opt/new/valkey-server"},
            "node_data_roots": {"primary": "/mnt/small"},
            "addressing": "per_node_loopback",
        },
        strict=True,
    )

    assert config.node_binaries == {"replica": "/opt/old/valkey-server", "node-0": "/opt/new/valkey-server"}
    assert config.node_data_roots == {"primary": "/mnt/small"}
    assert config.addressing == "per_node_loopback"


@pytest.mark.parametrize(
    ("document", "message"),
    [
        ({"node_binaries": {"node-9": "/x"}}, "Unknown node_binaries selector"),
        ({"node_binaries": {"replica": ""}}, "non-empty path"),
        ({"node_data_roots": ["/x"]}, "must map roles or node IDs"),
        ({"addressing": "public"}, "addressing must be one of"),
    ],
)
def test_topology_document_rejects_invalid_node_overrides(document, message):
    with pytest.raises(ValueError, match=message):
        cluster_config_from_topology_document({"type": "replication", **document}, strict=True)


def test_node_plans_take_exact_node_binary_over_role_binary():
    manager = _manager(node_binaries={"replica": "/old", "node-1": "/exact"})

    plans = {plan.node_id: plan for plan in manager.plan_topology()}

    assert plans["node-0"].valkey_binary is None
    assert plans["node-1"].valkey_binary == "/exact"
    assert plans["node-3"].valkey_binary == "/old"


def test_per_node_loopback_assigns_one_address_per_node():
    manager = _manager(addressing="per_node_loopback")

    hosts = [plan.host for plan in manager.plan_topology()]

    assert hosts == [f"127.0.0.{index + 2}" for index in range(6)]


def test_shared_loopback_keeps_every_node_on_127_0_0_1():
    assert {plan.host for plan in _manager().plan_topology()} == {"127.0.0.1"}


def test_node_command_uses_node_binary_and_binds_its_source_address():
    manager = _manager(valkey_binary="/default")

    shared = manager.build_node_command(7000, "/tmp/d", "/tmp/l")
    isolated = manager.build_node_command(7000, "/tmp/d", "/tmp/l", valkey_binary="/node", host="127.0.0.3")

    assert shared[0] == "/default"
    assert "--bind-source-addr" not in shared
    assert isolated[0] == "/node"
    assert isolated[isolated.index("--bind") + 1] == "127.0.0.3"
    assert isolated[isolated.index("--bind-source-addr") + 1] == "127.0.0.3"


def test_data_root_moves_node_data_and_is_removed_on_cleanup(tmp_path):
    base = tmp_path / "base"
    root = tmp_path / "small"
    manager = _manager(base_data_dir=str(base))

    data_dir, log_file = manager.create_node_directories("node-0", str(root))

    assert data_dir.startswith(str(root))
    assert log_file.startswith(str(base))
    manager._cleanup_resources([], ["node-0"], context="test", raise_on_failure=True)
    assert not os.path.exists(os.path.join(str(root), f"cluster-{manager.cluster_id}"))


def test_resolve_node_binaries_rejects_missing_paths(tmp_path):
    manager = _manager(node_binaries={"replica": str(tmp_path / "missing")})

    with pytest.raises(FileNotFoundError, match=r"node_binaries\.replica"):
        manager.resolve_node_binaries()


def _process():
    return subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])


def _instance(processes, binary="/default"):
    nodes = [
        NodeInfo(
            node_id=f"node-{index}",
            role="primary" if index == 0 else "replica",
            shard_id=0,
            port=7000 + index,
            bus_port=17000 + index,
            pid=process.pid,
            process=process,
            data_dir=f"/tmp/data-{index}",
            log_file=f"/tmp/log-{index}",
            master_node_id=None if index == 0 else "node-0",
        )
        for index, process in enumerate(processes)
    ]
    nodes[1].valkey_binary = "/replica-binary"
    config = ClusterConfig(
        num_shards=1, replicas_per_shard=1, topology_type=TopologyType.REPLICATION, valkey_binary=binary
    )
    return ClusterInstance(cluster_id="c1", config=config, nodes=nodes, is_ready=True)


def test_handle_exposes_process_control_and_node_files():
    processes = [_process(), _process()]
    try:
        handle = LocalValkeyTopologyHandle(Mock(), _instance(processes), {"node-0": 42})

        assert handle.pid("node-0") == processes[0].pid
        assert handle.is_alive("node-1")
        assert handle.binary("node-0") == "/default"
        assert handle.binaries() == {"node-0": "/default", "node-1": "/replica-binary"}
        assert handle.log_start_offset("node-0") == 42
        assert handle.log_start_offset("node-1") == 0
        assert handle.data_dir("node-1") == "/tmp/data-1"

        handle.send_signal("node-1", signal.SIGKILL)
        assert handle.wait_exit("node-1", timeout=10)
        assert not handle.is_alive("node-1")
        with pytest.raises(ProcessLookupError):
            handle.send_signal("node-1", signal.SIGTERM)
    finally:
        for process in processes:
            process.kill()
            process.wait()


def test_handle_set_binary_validates_and_applies_on_next_restart(tmp_path):
    binary = tmp_path / "valkey-server"
    binary.write_text("#!/bin/sh\n")
    binary.chmod(0o755)
    processes = [_process(), _process()]
    try:
        handle = LocalValkeyTopologyHandle(Mock(), _instance(processes))

        assert handle.set_binary("node-0", str(binary)) == str(binary)
        assert handle.binary("node-0") == str(binary)
        with pytest.raises(FileNotFoundError):
            handle.set_binary("node-0", str(tmp_path / "missing"))
    finally:
        for process in processes:
            process.kill()
            process.wait()


def test_handle_closes_component_clients_when_the_topology_closes():
    processes = [_process(), _process()]
    try:
        handle = LocalValkeyTopologyHandle(Mock(), _instance(processes))
        client = handle.new_client("node-0")
        client.close = Mock()

        handle._begin_close()
        handle._mark_closed()

        client.close.assert_called_once()
        with pytest.raises(Exception, match="cleaned up"):
            handle.new_client("node-0")
    finally:
        for process in processes:
            process.kill()
            process.wait()


def test_topology_metadata_records_each_node_binary_digest(tmp_path):
    binary = tmp_path / "valkey-server"
    binary.write_bytes(b"binary")
    processes = [_process(), _process()]
    try:
        instance = _instance(processes, binary=str(binary))
        instance.nodes[1].valkey_binary = None

        metadata = topology_metadata(instance)

        assert [node["node_id"] for node in metadata["nodes"]] == ["node-0", "node-1"]
        assert {node["binary_sha256"] for node in metadata["nodes"]} == {hashlib.sha256(b"binary").hexdigest()}
        # Metadata is plain data: it can be frozen onto a provisioned topology.
        assert ProvisionedTopology(metadata=metadata).metadata["addressing"] == "shared_loopback"
    finally:
        for process in processes:
            process.kill()
            process.wait()


def test_refresh_roles_records_a_failed_over_layout():
    processes = [_process(), _process()]
    try:
        handle = LocalValkeyTopologyHandle(Mock(), _instance(processes))
        handle._live_replication = lambda: {
            "node-0": ("replica", "127.0.0.1", 7001),
            "node-1": ("primary", None, None),
        }

        assert handle.refresh_roles() == {"node-0": "replica", "node-1": "primary"}
        assert handle.primaries() == ["node-1"]
        infos = {node.node_id: node for node in handle._node_infos}
        assert infos["node-0"].master_node_id == "node-1"
        assert infos["node-1"].master_node_id is None
    finally:
        for process in processes:
            process.kill()
            process.wait()


def test_failover_requires_a_cluster_topology():
    processes = [_process(), _process()]
    try:
        handle = LocalValkeyTopologyHandle(Mock(), _instance(processes))
        with pytest.raises(Exception, match="only available for cluster"):
            handle.failover(0)
    finally:
        for process in processes:
            process.kill()
            process.wait()


def test_handle_exposes_each_nodes_process_output_file():
    processes = [_process(), _process()]
    try:
        handle = LocalValkeyTopologyHandle(Mock(), _instance(processes), output_start_offsets={"node-1": 7})

        assert handle.process_output_file("node-0") == "/tmp/log-0.stdio.log"
        assert handle.process_output_start_offset("node-0") == 0
        assert handle.process_output_start_offset("node-1") == 7
    finally:
        for process in processes:
            process.kill()
            process.wait()
