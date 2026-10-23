"""Process and port safety for orchestrator-owned Valkey nodes."""

import shutil
import socket
import sys
from contextlib import closing, contextmanager
from typing import Optional
from unittest.mock import Mock, call, patch

import pytest

from valkey_fuzzer.cluster_orchestrator.orchestrator import (
    DEFAULT_MAXMEMORY,
    ClusterManager,
    ConfigurationManager,
    NodeIdentityError,
    PortManager,
    PortUnavailableError,
)
from valkey_fuzzer.models import ClusterConfig, NodeInfo, TopologyType
from valkey_fuzzer.utils.valkey_utils import valkey_client

ORCHESTRATOR = "valkey_fuzzer.cluster_orchestrator.orchestrator"


def _node(node_id: str = "node-0", port: int = 7000, pid: int = 100, role: str = "primary") -> NodeInfo:
    process = Mock(pid=pid)
    process.poll.return_value = None
    return NodeInfo(
        node_id=node_id,
        role=role,
        shard_id=0,
        port=port,
        bus_port=port + 10000,
        pid=pid,
        process=process,
        data_dir=f"/tmp/{node_id}",
        log_file="",
    )


def _client_context(client: Mock) -> Mock:
    context = Mock()
    context.__enter__ = Mock(return_value=client)
    context.__exit__ = Mock(return_value=False)
    return context


def _manager(tmp_path, topology_type=TopologyType.STANDALONE, base_port=7000, **config) -> ConfigurationManager:
    cluster_config = ClusterConfig(
        num_shards=1,
        replicas_per_shard=0,
        topology_type=topology_type,
        base_port=base_port,
        base_data_dir=str(tmp_path),
        valkey_binary="/opt/valkey-server",
        **config,
    )
    return ConfigurationManager(cluster_config, PortManager(base_port=base_port))


def _free_port(minimum: int = 20000, maximum: int = 55000, *, with_bus_port: bool = False) -> int:
    for port in range(minimum, maximum, 7):
        candidates = [port, port + 10000] if with_bus_port else [port]
        sockets = []
        try:
            for candidate in candidates:
                probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sockets.append(probe)
                probe.bind(("127.0.0.1", candidate))
            return port
        except OSError:
            continue
        finally:
            for probe in sockets:
                probe.close()
    raise RuntimeError("no free port found")


@contextmanager
def _listening_on(port: int):
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    with closing(listener):
        listener.bind(("127.0.0.1", port))
        listener.listen()
        yield listener


class TestPortPreflight:
    def test_spawn_refuses_a_client_port_held_by_another_server(self, tmp_path):
        port = _free_port()
        manager = _manager(tmp_path, base_port=port)
        plans = manager.plan_topology()

        with (
            _listening_on(port),
            patch(f"{ORCHESTRATOR}.subprocess.Popen") as popen,
            pytest.raises(PortUnavailableError, match=f"client port {port}"),
        ):
            manager.spawn_all_nodes(plans)

        popen.assert_not_called()

    def test_spawn_refuses_a_cluster_bus_port_held_by_another_server(self, tmp_path):
        port = _free_port(with_bus_port=True)
        manager = _manager(tmp_path, topology_type=TopologyType.CLUSTER, base_port=port)
        manager.clusterConfig.num_shards = 3
        plans = manager.plan_topology()

        with (
            _listening_on(port + 10000),
            patch(f"{ORCHESTRATOR}.subprocess.Popen") as popen,
            pytest.raises(PortUnavailableError, match=f"cluster bus port {port + 10000}"),
        ):
            manager.spawn_all_nodes(plans)

        # Every planned port is checked before any process starts.
        popen.assert_not_called()

    def test_non_cluster_topologies_do_not_reserve_the_bus_port(self, tmp_path):
        port = _free_port(with_bus_port=True)
        manager = _manager(tmp_path, base_port=port)

        with _listening_on(port + 10000):
            manager.ensure_ports_available("node-0", port, port + 10000)

    def test_restart_refuses_a_port_taken_while_the_node_was_down(self, tmp_path):
        port = _free_port()
        manager = _manager(tmp_path, base_port=port)
        node = _node(port=port)
        node.process.poll.return_value = 0  # already exited

        with (
            _listening_on(port),
            patch(f"{ORCHESTRATOR}.subprocess.Popen") as popen,
            pytest.raises(PortUnavailableError),
        ):
            manager.restart_node(node, wait_ready=False)

        popen.assert_not_called()


@pytest.fixture
def free_ports():
    with patch(f"{ORCHESTRATOR}._port_unavailable_reason", return_value=None):
        yield


@pytest.mark.usefixtures("free_ports")
class TestProcessIdentity:
    def test_spawn_rejects_a_foreign_server_answering_on_the_port(self, tmp_path):
        manager = _manager(tmp_path)
        plans = manager.plan_topology()
        process = Mock(pid=101)
        process.poll.return_value = None
        foreign = Mock()
        foreign.ping.return_value = True
        foreign.info.return_value = {"process_id": 4242}

        with (
            patch(f"{ORCHESTRATOR}.subprocess.Popen", return_value=process),
            patch(f"{ORCHESTRATOR}.valkey_client", return_value=_client_context(foreign)),
            pytest.raises(NodeIdentityError, match="served by pid 4242"),
        ):
            manager.spawn_all_nodes(plans)

        process.terminate.assert_called_once()
        assert manager.spawned_nodes == []

    def test_restart_stops_a_live_process_before_starting_a_new_one(self, tmp_path):
        manager = _manager(tmp_path)
        node = _node(pid=100)
        old_process = node.process
        new_process = Mock(pid=200)
        new_process.poll.return_value = None
        events = Mock()
        events.attach_mock(old_process.terminate, "terminate_old")

        def start(*args, **kwargs):
            events.popen()
            return new_process

        with patch(f"{ORCHESTRATOR}.subprocess.Popen", side_effect=start):
            manager.restart_node(node, wait_ready=False)

        assert events.mock_calls[:2] == [call.terminate_old(), call.popen()]
        old_process.wait.assert_called_once_with(timeout=5)
        assert node.process is new_process
        assert node.pid == 200

    def test_restart_does_not_touch_an_already_exited_process(self, tmp_path):
        manager = _manager(tmp_path)
        node = _node(pid=100)
        exited_process = node.process
        exited_process.poll.return_value = -9

        with patch(f"{ORCHESTRATOR}.subprocess.Popen", return_value=Mock(pid=200)):
            manager.restart_node(node, wait_ready=False)

        exited_process.terminate.assert_not_called()
        exited_process.kill.assert_not_called()

    def test_restart_readiness_rejects_a_different_pid_immediately(self, tmp_path):
        manager = _manager(tmp_path)
        node = _node(pid=100)
        node.process.poll.return_value = -9
        new_process = Mock(pid=200)
        new_process.poll.return_value = None
        foreign = Mock()
        foreign.ping.return_value = True
        foreign.info.return_value = {"process_id": 4242, "role": "master"}

        with (
            patch(f"{ORCHESTRATOR}.subprocess.Popen", return_value=new_process),
            patch(f"{ORCHESTRATOR}.valkey_client", return_value=_client_context(foreign)),
            pytest.raises(NodeIdentityError, match="served by pid 4242"),
        ):
            manager.restart_node(node, wait_ready=True, ready_timeout=30.0)

        new_process.terminate.assert_called_once()

    def test_cluster_restart_rejects_a_changed_cluster_node_id(self, tmp_path):
        manager = _manager(tmp_path, topology_type=TopologyType.CLUSTER)
        node = _node(pid=100)
        node.cluster_node_id = "saved-id"
        manager._spawned_nodes = {node.node_id: node}
        client = Mock()
        client.ping.return_value = True
        client.info.return_value = {"process_id": 100, "cluster_enabled": 1}
        client.execute_command.side_effect = [
            "cluster_state:ok\r\ncluster_known_nodes:1\r\n",
            "other-id",
        ]

        with pytest.raises(NodeIdentityError, match="expected saved-id"):
            manager._restart_topology_is_ready(node, client, TopologyType.CLUSTER)

    def test_cluster_restart_accepts_the_saved_cluster_node_id(self, tmp_path):
        manager = _manager(tmp_path, topology_type=TopologyType.CLUSTER)
        node = _node(pid=100)
        node.cluster_node_id = "saved-id"
        manager._spawned_nodes = {node.node_id: node}
        client = Mock()
        client.ping.return_value = True
        client.info.return_value = {"process_id": 100, "cluster_enabled": 1}
        client.execute_command.side_effect = [
            "cluster_state:ok\r\ncluster_known_nodes:1\r\n",
            b"saved-id",
            "saved-id 127.0.0.1:7000@17000 myself,master - 0 0 1 connected 0-16383",
        ]

        assert manager._restart_topology_is_ready(node, client, TopologyType.CLUSTER)

    def test_cluster_reset_is_never_sent_to_a_foreign_process(self):
        manager = ClusterManager()
        ours = _node("node-0", 7000, pid=100)
        theirs = _node("node-1", 7001, pid=101)
        our_client = Mock()
        our_client.info.return_value = {"process_id": 100}
        foreign_client = Mock()
        foreign_client.info.return_value = {"process_id": 999}
        clients = {"node-0": our_client, "node-1": foreign_client}

        with (
            patch.object(manager, "get_client", side_effect=lambda node: clients[node.node_id]),
            pytest.raises(NodeIdentityError, match="node-1"),
        ):
            manager.reset_cluster_state([ours, theirs])

        our_client.execute_command.assert_called_once_with("CLUSTER", "RESET", "HARD")
        foreign_client.execute_command.assert_not_called()

    def test_cluster_reset_refuses_when_our_process_has_exited(self):
        manager = ClusterManager()
        node = _node(pid=100)
        node.process.poll.return_value = 0
        client = Mock()
        client.info.return_value = {"process_id": 100}

        with (
            patch.object(manager, "get_client", return_value=client),
            pytest.raises(NodeIdentityError, match="has exited"),
        ):
            manager.reset_cluster_state([node])

        client.execute_command.assert_not_called()


class TestProcessOutputAndDefaults:
    def test_process_output_goes_to_a_per_node_file_not_a_pipe(self, tmp_path):
        manager = _manager(tmp_path)
        log_file = str(tmp_path / "logs" / "1-node-0.log")
        command = [sys.executable, "-c", "import sys; print('to-stdout'); print('to-stderr', file=sys.stderr)"]

        process = manager._start_process(command, log_file)
        assert process.stdout is None and process.stderr is None
        process.wait(timeout=10)

        output = (tmp_path / "logs" / "1-node-0.stdio.log").read_text()
        assert "to-stdout" in output
        assert "to-stderr" in output

    def test_restart_appends_to_the_same_output_file(self, tmp_path):
        manager = _manager(tmp_path)
        log_file = str(tmp_path / "1-node-0.log")
        for marker in ("first", "second"):
            manager._start_process([sys.executable, "-c", f"print({marker!r})"], log_file).wait(timeout=10)

        assert (tmp_path / "1-node-0.stdio.log").read_text().split() == ["first", "second"]

    def test_startup_failure_reports_process_output(self, tmp_path):
        manager = _manager(tmp_path)
        (tmp_path / "1-node-0.stdio.log").write_text("FATAL CONFIG FILE ERROR\n")

        assert "FATAL CONFIG FILE ERROR" in manager._process_output_tail(str(tmp_path / "1-node-0.log"))

    @pytest.mark.parametrize("topology_type", list(TopologyType))
    def test_no_hidden_eviction_policy(self, tmp_path, topology_type):
        manager = _manager(tmp_path, topology_type=topology_type)

        command = manager.build_node_command(7000, "/tmp/node-0", "/tmp/node-0.log")

        assert "--maxmemory-policy" not in command
        assert command[command.index("--maxmemory") + 1] == DEFAULT_MAXMEMORY

    def test_scenario_memory_settings_override_the_default_cap(self, tmp_path):
        manager = _manager(tmp_path)

        command = manager.build_node_command(
            7000,
            "/tmp/node-0",
            "/tmp/node-0.log",
            server_config={"maxmemory": "2gb", "maxmemory-policy": "allkeys-lru"},
        )

        # Valkey applies the last occurrence of a directive.
        maxmemory_values = [command[i + 1] for i, arg in enumerate(command) if arg == "--maxmemory"]
        assert maxmemory_values[-1] == "2gb"
        assert command[command.index("--maxmemory-policy") + 1] == "allkeys-lru"


def _valkey_server() -> Optional[str]:
    return shutil.which("valkey-server")


@pytest.mark.skipif(_valkey_server() is None, reason="valkey-server is not on PATH")
def test_real_restart_replaces_a_live_server_and_verifies_its_pid(tmp_path):
    port = _free_port(7700, 7800)
    manager = _manager(tmp_path, base_port=port)
    manager.clusterConfig.valkey_binary = _valkey_server()
    nodes = manager.spawn_all_nodes(manager.plan_topology())
    node = nodes[0]
    try:
        old_process = node.process
        old_pid = node.pid

        manager.restart_node(node, wait_ready=True, ready_timeout=30.0)

        assert old_process.poll() is not None, "the live server must be stopped before restart"
        assert node.pid != old_pid
        with valkey_client(node.host, node.port, timeout=2.0) as client:
            assert int(client.info("server")["process_id"]) == node.pid
    finally:
        manager.cleanup_cluster(nodes)
        if node.process is not None and node.process.poll() is None:
            node.process.kill()
            node.process.wait(timeout=5)
