from contextlib import ExitStack
from typing import Optional
from unittest.mock import Mock, patch

import pytest

from src.cluster_orchestrator.orchestrator import ClusterManager, ConfigurationManager, PortManager
from src.fuzzer_engine.cluster_coordinator import ClusterCoordinator
from src.models import ClusterConfig, ClusterInstance, NodeInfo, TopologyType


def make_node(
    node_id: str,
    role: str,
    port: int,
    *,
    master_node_id: Optional[str] = None,
    server_config: Optional[dict] = None,
) -> NodeInfo:
    process = Mock()
    process.poll.return_value = None
    return NodeInfo(
        node_id=node_id,
        role=role,
        shard_id=0,
        port=port,
        bus_port=port + 10000,
        pid=port,
        process=process,
        data_dir=f"/tmp/{node_id}",
        log_file=f"/tmp/{node_id}.log",
        master_node_id=master_node_id,
        server_config=server_config or {},
    )


def command_value(command: list[str], option: str) -> str:
    return command[command.index(option) + 1]


def test_plan_standalone_topology_resolves_global_role_and_node_config():
    config = ClusterConfig(
        num_shards=9,
        replicas_per_shard=4,
        topology_type=TopologyType.STANDALONE,
        server_config={"appendonly": False, "loglevel": "warning"},
        node_configs={
            "primary": {"loglevel": "verbose", "save": "900 1"},
            "node-0": {"loglevel": "notice"},
        },
    )

    nodes = ConfigurationManager(config, PortManager()).plan_topology()

    assert len(nodes) == 1
    assert nodes[0].role == "primary"
    assert nodes[0].slot_start is None
    assert nodes[0].server_config == {
        "appendonly": False,
        "loglevel": "notice",
        "save": "900 1",
    }


def test_plan_replication_topology_has_one_primary_and_resolved_replicas():
    config = ClusterConfig(
        num_shards=5,
        replicas_per_shard=2,
        topology_type=TopologyType.REPLICATION,
        server_config={"appendonly": True},
        node_configs={
            "replica": {"replica-read-only": True, "replica-priority": 100},
            "node-2": {"replica-priority": 25},
        },
    )

    nodes = ConfigurationManager(config, PortManager()).plan_topology()

    assert [node.role for node in nodes] == ["primary", "replica", "replica"]
    assert [node.master_node_id for node in nodes[1:]] == ["node-0", "node-0"]
    assert nodes[1].server_config["replica-priority"] == 100
    assert nodes[2].server_config["replica-priority"] == 25


@pytest.mark.parametrize(
    ("topology_type", "expected"),
    [
        (TopologyType.CLUSTER, "yes"),
        (TopologyType.STANDALONE, "no"),
        (TopologyType.REPLICATION, "no"),
    ],
)
def test_build_node_command_sets_cluster_mode_and_appends_generic_config(topology_type, expected):
    config = ClusterConfig(
        num_shards=1,
        replicas_per_shard=0,
        topology_type=topology_type,
        valkey_binary="/opt/valkey-server",
    )
    manager = ConfigurationManager(config, PortManager())

    command = manager.build_node_command(
        7000,
        "/tmp/node-0",
        "/tmp/node-0.log",
        server_config={"loglevel": "debug", "replica_read_only": False, "save": ""},
    )

    assert command_value(command, "--cluster-enabled") == expected
    assert command[-6:] == ["--loglevel", "debug", "--replica-read-only", "no", "--save", ""]
    assert ("--cluster-config-file" in command) is (topology_type == TopologyType.CLUSTER)


@pytest.mark.parametrize(
    "option",
    [
        "port",
        "dir",
        "logfile",
        "cluster-config-file",
        "cluster_enabled",
        "replicaof",
        "daemonize",
        "include",
    ],
)
def test_build_node_command_rejects_orchestrator_managed_options(option):
    manager = ConfigurationManager(
        ClusterConfig(num_shards=1, replicas_per_shard=0),
        PortManager(),
    )

    with pytest.raises(ValueError, match="managed by the orchestrator"):
        manager.build_node_command(7000, "/tmp/node-0", "/tmp/node-0.log", {option: "unsafe"})


def test_spawn_and_restart_preserve_resolved_server_config(tmp_path):
    config = ClusterConfig(
        num_shards=1,
        replicas_per_shard=0,
        topology_type=TopologyType.STANDALONE,
        base_data_dir=str(tmp_path),
        valkey_binary="/opt/valkey-server",
        server_config={"appendonly": False},
    )
    manager = ConfigurationManager(config, PortManager())
    plan = manager.plan_topology()[0]
    process = Mock(pid=1234)
    process.poll.return_value = None
    ping_client = Mock()
    ping_client.ping.return_value = True
    client_context = Mock()
    client_context.__enter__ = Mock(return_value=ping_client)
    client_context.__exit__ = Mock(return_value=False)

    with ExitStack() as stack:
        build_command = stack.enter_context(patch.object(manager, "build_node_command", return_value=["valkey-server"]))
        stack.enter_context(patch("src.cluster_orchestrator.orchestrator.subprocess.Popen", return_value=process))
        stack.enter_context(patch("src.cluster_orchestrator.orchestrator.valkey_client", return_value=client_context))
        node = manager.spawn_all_nodes([plan])[0]
        manager.restart_node(node, wait_ready=False)

    assert node.server_config == {"appendonly": False}
    assert build_command.call_args_list[0].kwargs["server_config"] == {"appendonly": False}
    assert build_command.call_args_list[1].kwargs["server_config"] == {"appendonly": False}


def test_form_primary_replica_uses_replicaof_and_info_replication():
    primary = make_node("node-0", "primary", 7000)
    replica = make_node("node-1", "replica", 7001, master_node_id=primary.node_id)
    primary_client = Mock()
    primary_client.info.return_value = {"role": "master", "connected_slaves": 1}
    replica_client = Mock()
    replica_client.info.return_value = {
        "role": "slave",
        "master_link_status": "up",
        "master_host": primary.host,
        "master_port": primary.port,
    }
    clients = {primary.node_id: primary_client, replica.node_id: replica_client}
    manager = ClusterManager()
    manager.get_client = Mock(side_effect=lambda node: clients[node.node_id])

    connection = manager.form_cluster(
        [primary, replica],
        "replication-test",
        topology_type=TopologyType.REPLICATION,
    )

    assert connection.topology_type == TopologyType.REPLICATION
    primary_client.execute_command.assert_called_once_with("REPLICAOF", "NO", "ONE")
    replica_client.execute_command.assert_called_once_with("REPLICAOF", primary.host, primary.port)
    primary_client.info.assert_called_with("replication")
    replica_client.info.assert_called_with("replication")


def test_validate_primary_replica_rejects_wrong_upstream():
    primary = make_node("node-0", "primary", 7000)
    replica = make_node("node-1", "replica", 7001, master_node_id=primary.node_id)
    primary_client = Mock()
    primary_client.info.return_value = {"role": "master", "connected_slaves": 1}
    replica_client = Mock()
    replica_client.info.return_value = {
        "role": "slave",
        "master_link_status": "up",
        "master_host": primary.host,
        "master_port": 7999,
    }
    clients = {primary.node_id: primary_client, replica.node_id: replica_client}
    manager = ClusterManager()
    manager.get_client = Mock(side_effect=lambda node: clients[node.node_id])

    assert (
        manager.validate_cluster(
            [primary, replica],
            timeout=0,
            topology_type=TopologyType.REPLICATION,
        )
        is False
    )


def test_standalone_readiness_does_not_require_cluster_slots():
    node = make_node("node-0", "primary", 7000)
    config = ClusterConfig(
        num_shards=1,
        replicas_per_shard=0,
        topology_type=TopologyType.STANDALONE,
    )
    cluster = ClusterInstance(cluster_id="standalone-test", config=config, nodes=[node], is_ready=True)
    coordinator = ClusterCoordinator()
    coordinator.cluster_manager = Mock()
    coordinator.cluster_manager.validate_cluster.return_value = True
    coordinator.active_clusters[cluster.cluster_id] = {
        "instance": cluster,
        "config_manager": Mock(),
    }

    status = coordinator.get_cluster_status(cluster.cluster_id)

    assert status.total_slots_assigned == 0
    assert coordinator.validate_cluster_readiness(cluster.cluster_id) is True
    coordinator.cluster_manager.validate_cluster.assert_called_with(
        [node],
        topology_type=TopologyType.STANDALONE,
    )


def test_replication_status_is_unhealthy_when_an_expected_replica_is_dead():
    primary = make_node("node-0", "primary", 7000)
    replica = make_node("node-1", "replica", 7001, master_node_id=primary.node_id)
    replica.process.poll.return_value = 1
    config = ClusterConfig(
        num_shards=1,
        replicas_per_shard=1,
        topology_type=TopologyType.REPLICATION,
    )
    cluster = ClusterInstance(
        cluster_id="replication-test",
        config=config,
        nodes=[primary, replica],
        is_ready=True,
    )
    coordinator = ClusterCoordinator()
    coordinator.cluster_manager = Mock()
    coordinator.active_clusters[cluster.cluster_id] = {
        "instance": cluster,
        "config_manager": Mock(),
    }

    status = coordinator.get_cluster_status(cluster.cluster_id)

    assert status.is_healthy is False
    coordinator.cluster_manager.validate_cluster.assert_not_called()
