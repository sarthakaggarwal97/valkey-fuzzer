"""Real integration tests exercising chaos injection against real Valkey clusters."""
from __future__ import annotations

import logging
import subprocess
import time
import pytest
from dataclasses import dataclass
from typing import Iterable, List

from src.chaos_engine import ChaosCoordinator, ChaosScenarioState, ProcessChaosEngine
from src.cluster_orchestrator.orchestrator import ClusterManager, ConfigurationManager, PortManager
from src.models import (
    ChaosConfig,
    ChaosCoordination,
    ChaosTiming,
    ChaosType,
    ClusterConfig,
    ClusterConnection,
    NodeInfo,
    Operation,
    OperationTiming,
    OperationType,
    ProcessChaosType,
    TargetSelection,
)

logging.basicConfig(format="%(levelname)-5s | %(filename)s:%(lineno)-3d | %(message)s", level=logging.INFO, force=True)

CLUSTER_NODE_TIMEOUT_MS = 5000
CLUSTER_NODE_TIMEOUT_SEC = CLUSTER_NODE_TIMEOUT_MS / 1000.0


def wait_for_process_death(process: subprocess.Popen, node_id: str, timeout: float = 10.0) -> None:
    """Wait until the given process terminates, raising on timeout."""
    try:
        process.wait(timeout=timeout)
        logging.info("OK: Process for %s terminated (pid=%s)", node_id, process.pid)
    except subprocess.TimeoutExpired:  # pragma: no cover - defensive cleanup
        logging.warning(
            "Process %s (pid=%s) did not exit within %.1fs – force killing",
            node_id,
            process.pid,
            timeout,
        )
        process.kill()
        process.wait()
        raise AssertionError(
            f"Process {process.pid} for {node_id} did not exit within {timeout}s"
        )


@dataclass
class ClusterTestContext:
    """Container bundling cluster state used by the chaos integration tests."""

    nodes: List[NodeInfo]
    config_mgr: ConfigurationManager
    cluster_mgr: ClusterManager
    connection: ClusterConnection

    def cleanup(self) -> None:
        self.cluster_mgr.close_connections()
        self.config_mgr.cleanup_cluster(self.nodes)

    def primaries(self) -> List[NodeInfo]:
        return [node for node in self.nodes if node.role == "primary"]

    def replicas(self) -> List[NodeInfo]:
        return [node for node in self.nodes if node.role == "replica"]

    def primary_for_shard(self, shard_id: int) -> NodeInfo:
        for node in self.primaries():
            if node.shard_id == shard_id:
                return node
        raise ValueError(f"No primary found for shard {shard_id}")

    def replicas_for_primary(self, primary: NodeInfo) -> List[NodeInfo]:
        return [
            node
            for node in self.replicas()
            if node.shard_id == primary.shard_id
        ]

    def find_node_by_cluster_id(self, cluster_node_id: str) -> NodeInfo:
        for node in self.nodes:
            if node.cluster_node_id == cluster_node_id:
                return node
        raise ValueError(f"Cluster node id {cluster_node_id} not found")

    def register_all_nodes(self, chaos_engine: ProcessChaosEngine) -> None:
        for node in self.nodes:
            chaos_engine.register_node_process(node.node_id, node.pid)

    def validate(self, nodes: Iterable[NodeInfo] | None = None) -> bool:
        target_nodes = list(nodes) if nodes is not None else self.nodes
        return self.cluster_mgr.validate_cluster(target_nodes)


def _build_cluster(num_shards: int, replicas: int, base_port: int) -> ClusterTestContext:
    """Helper used by fixtures to create a live Valkey cluster."""
    bootstrap_mgr = ConfigurationManager(
        ClusterConfig(num_shards=num_shards, replicas_per_shard=replicas),
        PortManager(),
    )
    valkey_binary = bootstrap_mgr.setup_valkey_from_source()

    config = ClusterConfig(
        num_shards=num_shards,
        replicas_per_shard=replicas,
        base_port=base_port,
        valkey_binary=valkey_binary,
        enable_cleanup=True,
    )
    port_mgr = PortManager(base_port=base_port)
    config_mgr = ConfigurationManager(config, port_mgr)
    cluster_mgr = ClusterManager()

    topology = config_mgr.plan_topology()
    nodes = config_mgr.spawn_all_nodes(topology)

    cluster_connection = cluster_mgr.form_cluster(nodes, config_mgr.cluster_id)
    if not cluster_connection:
        cluster_mgr.close_connections()
        config_mgr.cleanup_cluster(nodes)
        pytest.fail("Failed to form cluster")

    return ClusterTestContext(nodes, config_mgr, cluster_mgr, cluster_connection)


@pytest.fixture(autouse=True)
def test_separator(request):
    print(f"\n{'=' * 80}")
    print(f"TEST: {request.node.name}")
    print(f"{'=' * 80}")
    yield
    print(f"{'=' * 80}")
    print(f"COMPLETED: {request.node.name}")
    print(f"{'=' * 80}\n")


@pytest.fixture(scope="function")
def real_cluster() -> Iterable[ClusterTestContext]:
    context = _build_cluster(num_shards=3, replicas=1, base_port=7300)
    try:
        yield context
    finally:
        context.cleanup()


@pytest.fixture(scope="function")
def multi_shard_cluster() -> Iterable[ClusterTestContext]:
    context = _build_cluster(num_shards=3, replicas=1, base_port=7500)
    try:
        yield context
    finally:
        context.cleanup()


@pytest.fixture(scope="function")
def large_cluster() -> Iterable[ClusterTestContext]:
    context = _build_cluster(num_shards=3, replicas=2, base_port=7400)
    try:
        yield context
    finally:
        context.cleanup()


class TestRealChaosIntegration:
    """Integration tests validating chaos behaviour with real processes."""

    def test_cluster_registration_and_live_discovery(self, real_cluster: ClusterTestContext):
        chaos_engine = ProcessChaosEngine()

        assert real_cluster.validate(), "Cluster must be healthy before chaos"

        real_cluster.register_all_nodes(chaos_engine)
        assert len(chaos_engine.node_processes) == len(real_cluster.nodes)

        discovered = real_cluster.connection.get_current_nodes()
        assert len(discovered) == len(real_cluster.nodes)

        primary_ports = {node.port for node in real_cluster.primaries()}
        discovered_primary_ports = {
            node["port"] for node in real_cluster.connection.get_primary_nodes()
        }
        assert primary_ports.issubset(discovered_primary_ports)

        for node in real_cluster.nodes:
            assert chaos_engine.node_processes[node.node_id] == node.pid
            assert node.process.poll() is None

        chaos_engine.cleanup_chaos(real_cluster.connection.cluster_id)

    def test_failover_scenario_with_coordinator(self, real_cluster: ClusterTestContext) -> None:
        chaos_engine = ProcessChaosEngine()
        coordinator = ChaosCoordinator(chaos_engine)
        real_cluster.register_all_nodes(chaos_engine)

        primary_info = real_cluster.connection.get_primary_nodes()[0]
        primary = real_cluster.find_node_by_cluster_id(primary_info["node_id"])
        replica_candidates = real_cluster.replicas_for_primary(primary)
        assert replica_candidates, "Primary must have at least one replica"
        replica = replica_candidates[0]

        failover_operation = Operation(
            type=OperationType.FAILOVER,
            target_node=primary.node_id,
            parameters={"force": True},
            timing=OperationTiming(timeout=30.0),
        )

        chaos_config = ChaosConfig(
            chaos_type=ChaosType.PROCESS_KILL,
            target_selection=TargetSelection(
                strategy="specific",
                specific_nodes=[primary.node_id],
            ),
            timing=ChaosTiming(delay_before_operation=1.0),
            coordination=ChaosCoordination(
                chaos_before_operation=True,
                chaos_during_operation=False,
                chaos_after_operation=False,
            ),
            process_chaos_type=ProcessChaosType.SIGKILL,
        )

        scenario = coordinator.create_scenario(
            operation=failover_operation,
            chaos_config=chaos_config,
            target_node=primary,
        )

        result = coordinator.execute_scenario(scenario)
        assert result.state == ChaosScenarioState.COMPLETED
        assert result.chaos_results and result.chaos_results[0].success is True

        wait_for_process_death(primary.process, primary.node_id, timeout=5.0)

        promoted = False
        deadline = time.time() + CLUSTER_NODE_TIMEOUT_SEC * 6
        while time.time() < deadline:
            primary_ports = {
                node["port"] for node in real_cluster.connection.get_primary_nodes()
            }
            if replica.port in primary_ports:
                promoted = True
                break
            time.sleep(2)

        assert promoted, "Replica should be promoted to primary after failover"

        coordinator.cleanup_all_scenarios()

    def test_cascading_replica_failures(self, real_cluster: ClusterTestContext) -> None:
        chaos_engine = ProcessChaosEngine()
        real_cluster.register_all_nodes(chaos_engine)

        assert real_cluster.validate(), "Cluster must be healthy before chaos"

        for replica in real_cluster.replicas():
            result = chaos_engine.inject_process_chaos(replica, ProcessChaosType.SIGKILL)
            assert result.success is True
            wait_for_process_death(replica.process, replica.node_id, timeout=5.0)

        for primary in real_cluster.primaries():
            assert primary.process.poll() is None

        assert real_cluster.validate(real_cluster.primaries())

    def test_multi_phase_chaos_coordination(self, real_cluster: ClusterTestContext) -> None:
        chaos_engine = ProcessChaosEngine()
        coordinator = ChaosCoordinator(chaos_engine)
        real_cluster.register_all_nodes(chaos_engine)

        replica_info = real_cluster.connection.get_replica_nodes()[0]
        replica = real_cluster.find_node_by_cluster_id(replica_info["node_id"])
        primary = real_cluster.primary_for_shard(replica.shard_id)

        operation = Operation(
            type=OperationType.FAILOVER,
            target_node=primary.node_id,
            parameters={},
            timing=OperationTiming(),
        )

        chaos_config = ChaosConfig(
            chaos_type=ChaosType.PROCESS_KILL,
            target_selection=TargetSelection(
                strategy="specific",
                specific_nodes=[replica.node_id],
            ),
            timing=ChaosTiming(delay_before_operation=0.5),
            coordination=ChaosCoordination(
                chaos_before_operation=True,
                chaos_during_operation=True,
                chaos_after_operation=True,
            ),
            process_chaos_type=ProcessChaosType.SIGKILL,
        )

        scenario = coordinator.create_scenario(
            operation=operation,
            chaos_config=chaos_config,
            target_node=replica,
        )

        result = coordinator.execute_scenario(scenario)
        assert result.state == ChaosScenarioState.COMPLETED
        assert len(result.chaos_results) == 3
        assert any(res.success for res in result.chaos_results)

        coordinator.cleanup_all_scenarios()

    def test_chaos_target_selection_strategies(self, real_cluster: ClusterTestContext) -> None:
        """Test that different target selection strategies work correctly"""
        chaos_engine = ProcessChaosEngine()
        real_cluster.register_all_nodes(chaos_engine)
        
        chaos_engine.target_selector.update_cluster_topology(real_cluster.connection.cluster_id, real_cluster.nodes)
        
        # Specific node that exists
        target_node = real_cluster.nodes[0]
        selected = chaos_engine._select_chaos_target(
            real_cluster.connection.cluster_id,
            TargetSelection(strategy="specific", specific_nodes=[target_node.node_id])
        )
        assert selected.node_id == target_node.node_id
        
        # Specific node that doesn't exist
        selected = chaos_engine._select_chaos_target(
            real_cluster.connection.cluster_id,
            TargetSelection(strategy="specific", specific_nodes=["node-999"])
        )
        assert selected is None
        
        # Primary Node Only
        selected = chaos_engine._select_chaos_target(real_cluster.connection.cluster_id, TargetSelection(strategy="primary_only"))
        assert selected is not None
        assert selected.role == "primary"
        
        # Replica Node Only
        selected = chaos_engine._select_chaos_target(real_cluster.connection.cluster_id, TargetSelection(strategy="replica_only"))
        assert selected is not None
        assert selected.role == "replica"
        
        # Random Node
        selected = chaos_engine._select_chaos_target(real_cluster.connection.cluster_id, TargetSelection(strategy="random"))
        assert selected is not None
        assert selected in real_cluster.nodes

    def test_target_selection_with_chaos_injection(self, real_cluster: ClusterTestContext) -> None:
        """Integration test: select target and inject chaos"""
        chaos_engine = ProcessChaosEngine()
        real_cluster.register_all_nodes(chaos_engine)
        
        chaos_engine.target_selector.update_cluster_topology(real_cluster.connection.cluster_id, real_cluster.nodes)
        
        target = chaos_engine._select_chaos_target(real_cluster.connection.cluster_id, TargetSelection(strategy="replica_only"))
        
        assert target is not None
        assert target.role == "replica"
        
        result = chaos_engine.inject_process_chaos(target, ProcessChaosType.SIGKILL)
        assert result.success is True
        
        wait_for_process_death(target.process, target.node_id, timeout=5.0)
        
        # Cluster should remain healthy after injecting chaos
        remaining = [n for n in real_cluster.nodes if n.node_id != target.node_id]
        assert real_cluster.validate(remaining)


class TestClusterScalability:
    """Additional scenarios that operate on larger clusters."""

    def test_large_cluster_resilience(self, large_cluster: ClusterTestContext) -> None:
        chaos_engine = ProcessChaosEngine()
        large_cluster.register_all_nodes(chaos_engine)

        assert large_cluster.validate(), "Cluster must be healthy before chaos"

        discovered = large_cluster.connection.get_current_nodes()
        assert len(discovered) == len(large_cluster.nodes)

        for primary in large_cluster.primaries():
            replicas = large_cluster.replicas_for_primary(primary)
            if not replicas:
                continue
            target = replicas[0]
            result = chaos_engine.inject_process_chaos(target, ProcessChaosType.SIGKILL)
            assert result.success is True
            wait_for_process_death(target.process, target.node_id, timeout=5.0)

        surviving_nodes = [node for node in large_cluster.nodes if node.process.poll() is None]
        assert large_cluster.validate(surviving_nodes)

        primary_ports = {node.port for node in large_cluster.primaries()}
        discovered_primary_ports = {
            node["port"] for node in large_cluster.connection.get_primary_nodes()
        }
        assert primary_ports.issubset(discovered_primary_ports)


class TestAutomaticFailover:
    """Focus on validating automatic failover detection."""

    def test_primary_failover_promotion(self, multi_shard_cluster: ClusterTestContext) -> None:
        chaos_engine = ProcessChaosEngine()
        multi_shard_cluster.register_all_nodes(chaos_engine)

        primary = multi_shard_cluster.primary_for_shard(0)
        replicas = multi_shard_cluster.replicas_for_primary(primary)
        assert replicas, "Primary must have at least one replica"

        result = chaos_engine.inject_process_chaos(primary, ProcessChaosType.SIGKILL)
        assert result.success is True

        wait_for_process_death(primary.process, primary.node_id, timeout=5.0)

        promoted = False
        deadline = time.time() + CLUSTER_NODE_TIMEOUT_SEC * 6
        while time.time() < deadline:
            primaries = multi_shard_cluster.connection.get_primary_nodes()
            if any(node["port"] == replicas[0].port for node in primaries):
                promoted = True
                break
            time.sleep(2)

        assert promoted, "Replica should be promoted automatically"
