from typing import Optional
from unittest.mock import Mock, call, patch

import pytest

from src.cluster_orchestrator.orchestrator import ClusterManager, ConfigurationManager, PortManager
from src.fuzzer_engine.cluster_coordinator import ClusterCoordinator
from src.models import ClusterConfig, NodeInfo, TopologyType


def make_node(
    node_id: str,
    role: str,
    port: int,
    *,
    master_node_id: Optional[str] = None,
) -> NodeInfo:
    process = Mock(pid=port)
    process.poll.return_value = None
    return NodeInfo(
        node_id=node_id,
        role=role,
        shard_id=0,
        port=port,
        bus_port=port + 10000,
        pid=process.pid,
        process=process,
        data_dir=f"/tmp/{node_id}",
        log_file=f"/tmp/{node_id}.log",
        master_node_id=master_node_id,
    )


def client_context(client: Mock) -> Mock:
    context = Mock()
    context.__enter__ = Mock(return_value=client)
    context.__exit__ = Mock(return_value=False)
    return context


def test_spawn_keyboard_interrupt_cleans_partial_nodes(tmp_path):
    config = ClusterConfig(
        num_shards=1,
        replicas_per_shard=1,
        topology_type=TopologyType.REPLICATION,
        base_data_dir=str(tmp_path),
        valkey_binary="/opt/valkey-server",
    )
    port_manager = PortManager(base_port=7000)
    manager = ConfigurationManager(config, port_manager)
    plans = manager.plan_topology()
    first_process = Mock(pid=101)
    first_process.poll.return_value = None

    with (
        patch(
            "src.cluster_orchestrator.orchestrator.subprocess.Popen",
            side_effect=[first_process, KeyboardInterrupt()],
        ),
        pytest.raises(KeyboardInterrupt),
    ):
        manager.spawn_all_nodes(plans)

    first_process.terminate.assert_called_once()
    first_process.wait.assert_called_once_with(timeout=5)
    assert port_manager.allocated_ports == {}
    assert manager.spawned_nodes == []
    assert not (tmp_path / f"cluster-{manager.cluster_id}").exists()


def test_spawn_readiness_keyboard_interrupt_cleans_node(tmp_path):
    config = ClusterConfig(
        num_shards=1,
        replicas_per_shard=0,
        topology_type=TopologyType.STANDALONE,
        base_data_dir=str(tmp_path),
        valkey_binary="/opt/valkey-server",
    )
    port_manager = PortManager(base_port=7000)
    manager = ConfigurationManager(config, port_manager)
    plans = manager.plan_topology()
    process = Mock(pid=101)
    process.poll.return_value = None

    with (
        patch("src.cluster_orchestrator.orchestrator.subprocess.Popen", return_value=process),
        patch(
            "src.cluster_orchestrator.orchestrator.valkey_client",
            side_effect=KeyboardInterrupt(),
        ),
        pytest.raises(KeyboardInterrupt),
    ):
        manager.spawn_all_nodes(plans)

    process.terminate.assert_called_once()
    assert port_manager.allocated_ports == {}
    assert manager.spawned_nodes == []


def test_spawn_interrupt_after_process_creation_terminates_untracked_process(tmp_path):
    config = ClusterConfig(
        num_shards=1,
        replicas_per_shard=0,
        topology_type=TopologyType.STANDALONE,
        base_data_dir=str(tmp_path),
        valkey_binary="/opt/valkey-server",
    )
    port_manager = PortManager(base_port=7000)
    manager = ConfigurationManager(config, port_manager)
    plans = manager.plan_topology()
    process = Mock(pid=101)
    process.poll.return_value = None

    with (
        patch("src.cluster_orchestrator.orchestrator.subprocess.Popen", return_value=process),
        patch(
            "src.cluster_orchestrator.orchestrator.NodeInfo",
            side_effect=KeyboardInterrupt(),
        ),
        pytest.raises(KeyboardInterrupt),
    ):
        manager.spawn_all_nodes(plans)

    process.terminate.assert_called_once()
    assert port_manager.allocated_ports == {}
    assert manager.spawned_nodes == []


def test_coordinator_keyboard_interrupt_during_formation_cleans_and_reraises():
    node = make_node("node-0", "primary", 7000)
    config_manager = Mock()
    config_manager.setup_valkey_from_source.return_value = "/usr/bin/valkey-server"
    config_manager.plan_topology.return_value = [Mock()]
    config_manager.spawn_all_nodes.return_value = [node]
    config_manager.spawned_nodes = [node]
    config_manager.cluster_id = "partial"
    cluster_manager = Mock()
    cluster_manager.form_cluster.side_effect = KeyboardInterrupt()

    with patch(
        "src.fuzzer_engine.cluster_coordinator.ConfigurationManager",
        return_value=config_manager,
    ):
        coordinator = ClusterCoordinator()
        coordinator.cluster_manager = cluster_manager

        with pytest.raises(KeyboardInterrupt):
            coordinator.create_cluster(ClusterConfig(num_shards=1, replicas_per_shard=0))

    cluster_manager.close_connections.assert_called_once()
    config_manager.cleanup_cluster.assert_called_once_with([node])
    assert coordinator.active_clusters == {}


def test_coordinator_cleans_tracked_nodes_when_spawn_does_not_return():
    node = make_node("node-0", "primary", 7000)
    config_manager = Mock()
    config_manager.setup_valkey_from_source.return_value = "/usr/bin/valkey-server"
    config_manager.plan_topology.return_value = [Mock()]
    config_manager.spawn_all_nodes.side_effect = KeyboardInterrupt()
    config_manager.spawned_nodes = [node]
    cluster_manager = Mock()

    with patch(
        "src.fuzzer_engine.cluster_coordinator.ConfigurationManager",
        return_value=config_manager,
    ):
        coordinator = ClusterCoordinator()
        coordinator.cluster_manager = cluster_manager

        with pytest.raises(KeyboardInterrupt):
            coordinator.create_cluster(ClusterConfig(num_shards=1, replicas_per_shard=0))

    cluster_manager.close_connections.assert_called_once()
    config_manager.cleanup_cluster.assert_called_once_with([node])


def test_failed_formation_closes_cached_clients():
    node = make_node("node-0", "primary", 7000)
    client = Mock()
    manager = ClusterManager()
    manager.connections[node.node_id] = client

    with patch.object(manager, "reset_cluster_state", side_effect=RuntimeError("formation failed")):
        assert manager.form_cluster([node], "test") is None

    client.close.assert_called_once()
    assert manager.connections == {}


def test_formation_keyboard_interrupt_closes_clients_and_reraises():
    node = make_node("node-0", "primary", 7000)
    client = Mock()
    manager = ClusterManager()
    manager.connections[node.node_id] = client

    with (
        patch.object(manager, "reset_cluster_state", side_effect=KeyboardInterrupt()),
        pytest.raises(KeyboardInterrupt),
    ):
        manager.form_cluster([node], "test")

    client.close.assert_called_once()
    assert manager.connections == {}


def test_cleanup_attempts_all_resources_before_reporting_failures(tmp_path):
    config = ClusterConfig(
        num_shards=1,
        replicas_per_shard=1,
        base_data_dir=str(tmp_path),
        valkey_binary="/opt/valkey-server",
    )
    port_manager = PortManager(base_port=7000)
    port_manager.allocate_ports("node-0")
    port_manager.allocate_ports("node-1")
    manager = ConfigurationManager(config, port_manager)
    node_0 = make_node("node-0", "primary", 7000)
    node_1 = make_node("node-1", "replica", 7001, master_node_id="node-0")
    manager._spawned_nodes = {"node-0": node_0, "node-1": node_1}
    cluster_dir = tmp_path / f"cluster-{manager.cluster_id}"
    cluster_dir.mkdir()

    with (
        patch.object(
            manager,
            "terminate_node",
            side_effect=[RuntimeError("terminate failed"), None],
        ) as terminate_node,
        patch.object(
            port_manager,
            "release_ports",
            side_effect=[RuntimeError("release failed"), None],
        ) as release_ports,
        patch(
            "src.cluster_orchestrator.orchestrator.shutil.rmtree",
            side_effect=OSError("rmtree failed"),
        ) as rmtree,
        pytest.raises(RuntimeError, match="completed with failures"),
    ):
        manager.cleanup_cluster([node_0, node_1])

    assert terminate_node.call_args_list == [call(node_0), call(node_1)]
    assert release_ports.call_args_list == [call("node-0"), call("node-1")]
    rmtree.assert_called_once_with(str(cluster_dir))
    assert manager.spawned_nodes == []


def test_cleanup_attempts_all_resources_before_reraising_interrupt(tmp_path):
    config = ClusterConfig(
        num_shards=1,
        replicas_per_shard=1,
        base_data_dir=str(tmp_path),
        valkey_binary="/opt/valkey-server",
    )
    port_manager = PortManager(base_port=7000)
    manager = ConfigurationManager(config, port_manager)
    node_0 = make_node("node-0", "primary", 7000)
    node_1 = make_node("node-1", "replica", 7001, master_node_id="node-0")

    with (
        patch.object(
            manager,
            "terminate_node",
            side_effect=[KeyboardInterrupt(), None],
        ) as terminate_node,
        patch.object(port_manager, "release_ports") as release_ports,
        pytest.raises(KeyboardInterrupt),
    ):
        manager.cleanup_cluster([node_0, node_1])

    assert terminate_node.call_args_list == [call(node_0), call(node_1)]
    assert release_ports.call_args_list == [call("node-0"), call("node-1")]


def test_replication_replica_restart_waits_for_role_upstream_and_link(tmp_path):
    config = ClusterConfig(
        num_shards=1,
        replicas_per_shard=1,
        topology_type=TopologyType.REPLICATION,
        base_data_dir=str(tmp_path),
        valkey_binary="/opt/valkey-server",
    )
    manager = ConfigurationManager(config, PortManager(base_port=7000))
    primary = make_node("node-0", "primary", 7000)
    replica = make_node("node-1", "replica", 7001, master_node_id=primary.node_id)
    manager._spawned_nodes = {primary.node_id: primary, replica.node_id: replica}
    restarted_process = Mock(pid=999)
    restarted_process.poll.return_value = None
    client = Mock()
    client.ping.return_value = True
    client.info.return_value = {
        "role": "slave",
        "master_link_status": "up",
        "master_host": primary.host,
        "master_port": primary.port,
    }

    with (
        patch("src.cluster_orchestrator.orchestrator.subprocess.Popen", return_value=restarted_process),
        patch(
            "src.cluster_orchestrator.orchestrator.valkey_client",
            return_value=client_context(client),
        ),
    ):
        result = manager.restart_node(replica, wait_ready=True, ready_timeout=1.0)

    assert result.pid == 999
    client.info.assert_called_once_with("replication")


def test_replication_replica_restart_rejects_wrong_upstream(tmp_path):
    config = ClusterConfig(
        num_shards=1,
        replicas_per_shard=1,
        topology_type=TopologyType.REPLICATION,
        base_data_dir=str(tmp_path),
        valkey_binary="/opt/valkey-server",
    )
    manager = ConfigurationManager(config, PortManager(base_port=7000))
    primary = make_node("node-0", "primary", 7000)
    replica = make_node("node-1", "replica", 7001, master_node_id=primary.node_id)
    manager._spawned_nodes = {primary.node_id: primary, replica.node_id: replica}
    restarted_process = Mock(pid=999)
    restarted_process.poll.return_value = None
    client = Mock()
    client.ping.return_value = True
    client.info.return_value = {
        "role": "slave",
        "master_link_status": "up",
        "master_host": primary.host,
        "master_port": 7999,
    }

    with (
        patch("src.cluster_orchestrator.orchestrator.subprocess.Popen", return_value=restarted_process),
        patch(
            "src.cluster_orchestrator.orchestrator.valkey_client",
            return_value=client_context(client),
        ),
        patch(
            "src.cluster_orchestrator.orchestrator.time.monotonic",
            side_effect=[0.0, 0.0, 0.0, 11.0, 11.0],
        ),
        pytest.raises(Exception, match="failed to become ready"),
    ):
        manager.restart_node(replica, wait_ready=True, ready_timeout=10.0)

    restarted_process.terminate.assert_called_once()


def test_restart_readiness_keyboard_interrupt_terminates_new_process(tmp_path):
    config = ClusterConfig(
        num_shards=1,
        replicas_per_shard=0,
        topology_type=TopologyType.STANDALONE,
        base_data_dir=str(tmp_path),
        valkey_binary="/opt/valkey-server",
    )
    manager = ConfigurationManager(config, PortManager(base_port=7000))
    node = make_node("node-0", "primary", 7000)
    restarted_process = Mock(pid=999)
    restarted_process.poll.return_value = None

    with (
        patch("src.cluster_orchestrator.orchestrator.subprocess.Popen", return_value=restarted_process),
        patch(
            "src.cluster_orchestrator.orchestrator.valkey_client",
            side_effect=KeyboardInterrupt(),
        ),
        pytest.raises(KeyboardInterrupt),
    ):
        manager.restart_node(node, wait_ready=True, ready_timeout=1.0)

    restarted_process.terminate.assert_called_once()


def test_restart_topology_checks_standalone_role_and_cluster_membership():
    standalone = ConfigurationManager(
        ClusterConfig(
            num_shards=1,
            replicas_per_shard=0,
            topology_type=TopologyType.STANDALONE,
        ),
        PortManager(),
    )
    standalone_node = make_node("node-0", "primary", 7000)
    standalone_client = Mock()
    standalone_client.ping.return_value = True
    standalone_client.info.return_value = {"role": "master"}

    assert standalone._restart_topology_is_ready(
        standalone_node,
        standalone_client,
        TopologyType.STANDALONE,
    )
    standalone_client.info.return_value = {"role": "slave"}
    assert not standalone._restart_topology_is_ready(
        standalone_node,
        standalone_client,
        TopologyType.STANDALONE,
    )

    cluster = ConfigurationManager(
        ClusterConfig(num_shards=1, replicas_per_shard=0),
        PortManager(),
    )
    cluster_node = make_node("node-0", "primary", 7000)
    cluster._spawned_nodes = {cluster_node.node_id: cluster_node}
    cluster_client = Mock()
    cluster_client.ping.return_value = True
    cluster_client.info.return_value = {"cluster_enabled": 1}
    cluster_client.execute_command.side_effect = [
        "cluster_state:ok\r\ncluster_known_nodes:1\r\n",
        "abc123",
        "abc123 127.0.0.1:7000@17000 myself,master - 0 0 1 connected 0-16383",
    ]

    assert cluster._restart_topology_is_ready(
        cluster_node,
        cluster_client,
        TopologyType.CLUSTER,
    )

    cluster_client.execute_command.side_effect = [
        b"cluster_state:ok\ncluster_known_nodes:1\n",
        "abc123",
        "abc123 127.0.0.1:7000@17000 myself,master,fail - 0 0 1 connected",
    ]
    assert not cluster._restart_topology_is_ready(
        cluster_node,
        cluster_client,
        TopologyType.CLUSTER,
    )


def test_restart_topology_rejects_isolated_cluster_node():
    manager = ConfigurationManager(
        ClusterConfig(num_shards=2, replicas_per_shard=0),
        PortManager(),
    )
    node = make_node("node-0", "primary", 7000)
    peer = make_node("node-1", "primary", 7001)
    manager._spawned_nodes = {node.node_id: node, peer.node_id: peer}
    client = Mock()
    client.ping.return_value = True
    client.info.return_value = {"cluster_enabled": 1}
    client.execute_command.side_effect = [
        "cluster_state:fail\r\ncluster_known_nodes:2\r\n",
        "cluster_state:ok\r\ncluster_known_nodes:2\r\n",
        "abc123",
        (
            "abc123 127.0.0.1:7000@17000 myself,master - 0 0 1 connected 0-8191\n"
            "def456 127.0.0.1:7001@17001 master - 0 0 2 disconnected 8192-16383"
        ),
    ]

    assert not manager._restart_topology_is_ready(node, client, TopologyType.CLUSTER)
    client.execute_command.assert_called_once_with("CLUSTER", "INFO")
    assert not manager._restart_topology_is_ready(node, client, TopologyType.CLUSTER)


def test_restart_topology_replication_primary_waits_for_expected_replicas():
    manager = ConfigurationManager(
        ClusterConfig(
            num_shards=1,
            replicas_per_shard=1,
            topology_type=TopologyType.REPLICATION,
        ),
        PortManager(),
    )
    primary = make_node("node-0", "primary", 7000)
    replica = make_node("node-1", "replica", 7001, master_node_id=primary.node_id)
    manager._spawned_nodes = {primary.node_id: primary, replica.node_id: replica}
    client = Mock()
    client.ping.return_value = True
    client.info.return_value = {"role": "master", "connected_slaves": 0}

    assert not manager._restart_topology_is_ready(primary, client, TopologyType.REPLICATION)

    client.info.return_value = {"role": "master", "connected_slaves": 1}
    assert manager._restart_topology_is_ready(primary, client, TopologyType.REPLICATION)
