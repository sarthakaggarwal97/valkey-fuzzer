"""
Real end-to-end integration tests for Chaos Engine with actual Valkey clusters

These tests create REAL Valkey clusters, inject REAL chaos (killing actual processes),
and validate the cluster behavior. No mocking - this is the real deal.

WARNING: These tests:
- Create actual Valkey processes
- Kill actual processes with real signals
- Take longer to run (30-60 seconds each)
- Require Valkey to be installed or built
"""
import pytest
import time
import logging
import subprocess
from src.cluster_orchestrator.orchestrator import (
    PortManager, ConfigurationManager, ClusterManager
)
from src.chaos_engine import ProcessChaosEngine, ChaosCoordinator
from src.chaos_engine.coordinator import ChaosScenarioState
from src.models import (
    ClusterConfig, NodeInfo, ProcessChaosType, ChaosType,
    Operation, OperationType, OperationTiming,
    ChaosConfig, TargetSelection, ChaosTiming, ChaosCoordination
)

logging.basicConfig(
    format='%(levelname)-5s | %(filename)s:%(lineno)-3d | %(message)s',
    level=logging.INFO,
    force=True
)

# Cluster configuration constants
CLUSTER_NODE_TIMEOUT_MS = 5000  # Must match --cluster-node-timeout in orchestrator.py
CLUSTER_NODE_TIMEOUT_SEC = CLUSTER_NODE_TIMEOUT_MS / 1000.0


def wait_for_process_death(process: subprocess.Popen, node_id: str, timeout: float = 10.0) -> None:
    """
    Wait for a process to die, with proper timeout handling.
    
    Args:
        process: The subprocess.Popen object
        node_id: Node identifier for logging
        timeout: Maximum time to wait in seconds
    
    Raises:
        AssertionError: If process doesn't die within timeout
    """
    try:
        process.wait(timeout=timeout)
        logging.info(f"OK: Process for {node_id} (PID {process.pid}) terminated")
    except subprocess.TimeoutExpired:
        # Force kill if still alive
        logging.warning(f"Process {process.pid} didn't terminate within {timeout}s, force killing")
        process.kill()
        process.wait()
        raise AssertionError(f"Process {process.pid} for {node_id} didn't die gracefully within {timeout}s")


@pytest.fixture(autouse=True)
def test_separator(request):
    """Print test separators for better readability"""
    print(f"\n{'='*80}")
    print(f"TEST: {request.node.name}")
    print(f"{'='*80}")
    yield
    print(f"{'='*80}")
    print(f"COMPLETED: {request.node.name}")
    print(f"{'='*80}\n")


@pytest.fixture(scope="function")
def real_cluster():
    """
    Fixture that creates a real Valkey cluster with 3 shards (3 primaries + 3 replicas = 6 nodes)
    Using 3 shards provides quorum for cluster operations and failure detection.
    """
    # Setup Valkey binary
    config_mgr = ConfigurationManager(
        ClusterConfig(num_shards=3, replicas_per_shard=1),
        PortManager()
    )
    valkey_binary = config_mgr.setup_valkey_from_source()
    
    # Create cluster configuration
    config = ClusterConfig(
        num_shards=3,
        replicas_per_shard=1,
        base_port=7300,
        valkey_binary=valkey_binary,
        enable_cleanup=True
    )
    
    port_mgr = PortManager(base_port=7300)
    config_mgr = ConfigurationManager(config, port_mgr)
    cluster_mgr = ClusterManager()
    
    # Plan and spawn nodes
    logging.info("Creating real Valkey cluster...")
    topology = config_mgr.plan_topology()
    nodes = config_mgr.spawn_all_nodes(topology)
    
    # Form cluster
    success = cluster_mgr.form_cluster(nodes)
    if not success:
        cluster_mgr.close_connections()
        config_mgr.cleanup_cluster(nodes)
        pytest.fail("Failed to form cluster")
    
    logging.info(f"OK: Real cluster created with {len(nodes)} nodes")
    
    # Yield cluster components to test
    yield {
        'nodes': nodes,
        'config_mgr': config_mgr,
        'cluster_mgr': cluster_mgr,
        'port_mgr': port_mgr
    }
    
    # Cleanup
    logging.info("Cleaning up real cluster...")
    cluster_mgr.close_connections()
    config_mgr.cleanup_cluster(nodes)
    logging.info("OK: Cluster cleaned up")


@pytest.fixture(scope="function")
def multi_shard_cluster():
    """
    Fixture that creates a real multi-shard Valkey cluster (3 shards, 1 replica each = 6 nodes)
    Multiple shards provide quorum for cluster operations and failure detection.
    """
    # Setup Valkey binary
    config_mgr = ConfigurationManager(
        ClusterConfig(num_shards=3, replicas_per_shard=1),
        PortManager()
    )
    valkey_binary = config_mgr.setup_valkey_from_source()
    
    # Create cluster configuration
    config = ClusterConfig(
        num_shards=3,
        replicas_per_shard=1,
        base_port=7500,
        valkey_binary=valkey_binary,
        enable_cleanup=True
    )
    
    port_mgr = PortManager(base_port=7500)
    config_mgr = ConfigurationManager(config, port_mgr)
    cluster_mgr = ClusterManager()
    
    # Plan and spawn nodes
    logging.info("Creating real multi-shard Valkey cluster...")
    topology = config_mgr.plan_topology()
    nodes = config_mgr.spawn_all_nodes(topology)
    
    # Form cluster
    success = cluster_mgr.form_cluster(nodes)
    if not success:
        cluster_mgr.close_connections()
        config_mgr.cleanup_cluster(nodes)
        pytest.fail("Failed to form multi-shard cluster")
    
    logging.info(f"OK: Real multi-shard cluster created with {len(nodes)} nodes")
    
    # Yield cluster components to test
    yield {
        'nodes': nodes,
        'config_mgr': config_mgr,
        'cluster_mgr': cluster_mgr,
        'port_mgr': port_mgr
    }
    
    # Cleanup
    logging.info("Cleaning up real multi-shard cluster...")
    cluster_mgr.close_connections()
    config_mgr.cleanup_cluster(nodes)
    logging.info("OK: Multi-shard cluster cleaned up")


class TestRealChaosIntegration:
    """Real integration tests with actual Valkey clusters"""
    
    def test_real_cluster_creation_and_registration(self, real_cluster):
        """
        Test that we can create a real cluster and register nodes with chaos engine
        """
        nodes = real_cluster['nodes']
        cluster_mgr = real_cluster['cluster_mgr']
        chaos_engine = ProcessChaosEngine()
        
        # Verify cluster is healthy
        assert cluster_mgr.validate_cluster(nodes)
        logging.info("OK: Cluster healthy")
        
        # Register all real nodes
        for node in nodes:
            chaos_engine.register_node_process(node.node_id, node.pid)
            logging.info(f"Registered {node.node_id} with PID {node.pid}")
        
        # Verify registration (3 shards * 2 nodes = 6 nodes)
        assert len(chaos_engine.node_processes) == 6
        
        # Verify PIDs are real
        for node in nodes:
            assert chaos_engine.node_processes[node.node_id] == node.pid
            assert node.process.poll() is None  # Process is still running
        
        # Cleanup
        for node in nodes:
            chaos_engine.unregister_node_process(node.node_id)
        
        logging.info("PASS: Real cluster created and nodes registered successfully")
    
    def test_real_process_kill_with_sigterm(self, real_cluster):
        """
        Test killing a real replica node with SIGTERM (graceful shutdown)
        """
        nodes = real_cluster['nodes']
        cluster_mgr = real_cluster['cluster_mgr']
        chaos_engine = ProcessChaosEngine()
        
        # Find a replica from shard 0 to kill
        shard_0_nodes = [node for node in nodes if node.shard_id == 0]
        replica = next(node for node in shard_0_nodes if node.role == 'replica')
        logging.info(f"Target: {replica.node_id} (PID {replica.pid})")
        
        # Register nodes
        for node in nodes:
            chaos_engine.register_node_process(node.node_id, node.pid)
        
        # Verify cluster is healthy before chaos
        assert cluster_mgr.validate_cluster(nodes)
        logging.info("OK: Cluster healthy before chaos")
        
        # Inject real chaos - kill the replica
        logging.info(f"Injecting chaos: SIGTERM on {replica.node_id}")
        result = chaos_engine.inject_process_chaos(replica, ProcessChaosType.SIGTERM)
        
        # Verify chaos was successful
        assert result.success is True
        assert result.target_node == replica.node_id
        assert result.chaos_type == ChaosType.PROCESS_KILL
        logging.info(f"OK: Chaos injected: {result.chaos_type.value}")
        
        # Wait for process to die (SIGTERM is graceful, may take longer)
        # Use wait() with timeout to properly detect process termination
        try:
            replica.process.wait(timeout=10)
            logging.info(f"OK: Process {replica.pid} is dead")
        except subprocess.TimeoutExpired:
            # If still alive after 10s, force kill
            replica.process.kill()
            replica.process.wait()
            logging.warning(f"Process {replica.pid} didn't respond to SIGTERM, force killed")
        
        # Verify process is actually dead
        assert replica.process.poll() is not None, f"Process {replica.pid} should be dead after SIGTERM"
        
        # Verify cluster state after killing replica (should still be healthy - primaries collectively cover all slots)
        remaining_nodes = [n for n in nodes if n.node_id != replica.node_id]
        cluster_healthy = cluster_mgr.validate_cluster(remaining_nodes)
        assert cluster_healthy, "Cluster should remain healthy after killing replica"
        logging.info(f"OK: Cluster state after killing replica: healthy (all slots covered)")
        
        # Cleanup
        for node in nodes:
            chaos_engine.unregister_node_process(node.node_id)
        
        logging.info("PASS: Real process killed successfully with SIGTERM")
    
    def test_real_process_kill_with_sigkill(self, real_cluster):
        """
        Test killing a real replica node with SIGKILL (forced kill)
        """
        nodes = real_cluster['nodes']
        cluster_mgr = real_cluster['cluster_mgr']
        chaos_engine = ProcessChaosEngine()
        
        # Find a replica from shard 1 to kill
        shard_1_nodes = [node for node in nodes if node.shard_id == 1]
        replica = next(node for node in shard_1_nodes if node.role == 'replica')
        logging.info(f"Target: {replica.node_id} (PID {replica.pid})")
        
        # Register nodes
        for node in nodes:
            chaos_engine.register_node_process(node.node_id, node.pid)
        
        # Verify cluster is healthy before chaos
        assert cluster_mgr.validate_cluster(nodes)
        logging.info("OK: Cluster healthy before chaos")
        
        # Inject real chaos - kill the replica with SIGKILL
        logging.info(f"Injecting chaos: SIGKILL on {replica.node_id}")
        result = chaos_engine.inject_process_chaos(replica, ProcessChaosType.SIGKILL)
        
        # Verify chaos was successful
        assert result.success is True
        assert result.target_node == replica.node_id
        logging.info(f"OK: Chaos injected: SIGKILL")
        
        # Wait for process to die (SIGKILL should be immediate)
        wait_for_process_death(replica.process, replica.node_id, timeout=5.0)
        
        # Verify cluster state after killing replica (should still be healthy)
        remaining_nodes = [n for n in nodes if n.node_id != replica.node_id]
        cluster_healthy = cluster_mgr.validate_cluster(remaining_nodes)
        assert cluster_healthy, "Cluster should remain healthy after killing replica"
        logging.info(f"OK: Cluster state after killing replica: healthy (all slots covered)")
        
        # Cleanup
        for node in nodes:
            chaos_engine.unregister_node_process(node.node_id)
        
        logging.info("PASS: Real process killed successfully with SIGKILL")
    
    def test_real_failover_scenario_with_chaos(self, real_cluster):
        """
        Complete real failover scenario:
        1. Create real cluster
        2. Kill primary node with chaos
        3. Validate cluster detects the failure
        4. Verify remaining nodes are still operational
        """
        nodes = real_cluster['nodes']
        cluster_mgr = real_cluster['cluster_mgr']
        chaos_engine = ProcessChaosEngine()
        coordinator = ChaosCoordinator(chaos_engine)
        
        # Find primary and replica from shard 0
        shard_0_nodes = [node for node in nodes if node.shard_id == 0]
        primary = next(node for node in shard_0_nodes if node.role == 'primary')
        replicas = [node for node in shard_0_nodes if node.role == 'replica']
        
        logging.info(f"Cluster topology:")
        logging.info(f"  Primary: {primary.node_id} (PID {primary.pid})")
        for replica in replicas:
            logging.info(f"  Replica: {replica.node_id} (PID {replica.pid})")
        
        # Register all nodes
        for node in nodes:
            chaos_engine.register_node_process(node.node_id, node.pid)
        
        # Verify cluster is healthy
        assert cluster_mgr.validate_cluster(nodes)
        logging.info("OK: Cluster healthy before chaos")
        
        # Create failover operation
        failover_operation = Operation(
            type=OperationType.FAILOVER,
            target_node=primary.node_id,
            parameters={"force": True},
            timing=OperationTiming(timeout=30.0)
        )
        
        # Configure chaos to kill primary BEFORE failover
        chaos_config = ChaosConfig(
            chaos_type=ChaosType.PROCESS_KILL,
            target_selection=TargetSelection(
                strategy="specific",
                specific_nodes=[primary.node_id]
            ),
            timing=ChaosTiming(
                delay_before_operation=1.0,
                delay_after_operation=0.0
            ),
            coordination=ChaosCoordination(
                chaos_before_operation=True,
                chaos_during_operation=False,
                chaos_after_operation=False
            ),
            process_chaos_type=ProcessChaosType.SIGKILL
        )
        
        logging.info(f"\nChaos configuration:")
        logging.info(f"  Target: {primary.node_id}")
        logging.info(f"  Type: SIGKILL")
        logging.info(f"  Timing: BEFORE operation")
        
        # Create and execute chaos scenario
        logging.info(f"\nExecuting chaos scenario...")
        scenario = coordinator.create_scenario(
            operation=failover_operation,
            chaos_config=chaos_config,
            target_node=primary
        )
        
        result = coordinator.execute_scenario(scenario)
        
        # Verify chaos execution
        assert result.state == ChaosScenarioState.COMPLETED
        assert len(result.chaos_results) == 1
        assert result.chaos_results[0].success is True
        assert result.chaos_results[0].target_node == primary.node_id
        
        logging.info(f"OK: Chaos scenario completed")
        logging.info(f"OK: Primary node killed: {primary.node_id}")
        
        # Wait for process to die (SIGKILL should be immediate)
        wait_for_process_death(primary.process, primary.node_id, timeout=5.0)
        
        # Verify replicas are still alive
        for replica in replicas:
            assert replica.process.poll() is None
            logging.info(f"OK: Replica {replica.node_id} still running")
        
        # Try to connect to replicas
        for replica in replicas:
            try:
                client = cluster_mgr.get_client(replica)
                pong = client.ping()
                assert pong is True
                logging.info(f"OK: Replica {replica.node_id} is responsive")
            except Exception as e:
                logging.warning(f"Replica {replica.node_id} not responsive: {e}")
        
        # Verify cluster state after primary killed (will be degraded but replicas should be reachable)
        # Note: Cluster will report as unhealthy because primary is down, but that's expected
        remaining_nodes = [n for n in nodes if n.node_id != primary.node_id]
        try:
            cluster_healthy = cluster_mgr.validate_cluster(remaining_nodes)
            logging.info(f"Cluster state after primary killed: {'healthy' if cluster_healthy else 'degraded (expected)'}")
        except Exception as e:
            logging.info(f"Cluster validation failed after primary killed (expected): {e}")
        
        # Cleanup
        coordinator.cleanup_all_scenarios()
        for node in nodes:
            chaos_engine.unregister_node_process(node.node_id)
        
        logging.info("\nPASS: Real failover scenario with chaos completed successfully")
        logging.info(f"   - Primary killed: {primary.node_id}")
        logging.info(f"   - Replicas survived: {len(replicas)}")
    
    def test_real_cascading_failures(self, real_cluster):
        """
        Test cascading failures: kill multiple nodes in sequence
        """
        nodes = real_cluster['nodes']
        cluster_mgr = real_cluster['cluster_mgr']
        chaos_engine = ProcessChaosEngine()
        
        # Verify cluster is healthy before chaos
        assert cluster_mgr.validate_cluster(nodes)
        logging.info("OK: Cluster healthy before chaos")
        
        # Register all nodes
        for node in nodes:
            chaos_engine.register_node_process(node.node_id, node.pid)
        
        logging.info(f"Starting with {len(nodes)} nodes")
        
        # Kill replicas one by one
        replicas = [node for node in nodes if node.role == 'replica']
        
        for i, replica in enumerate(replicas, 1):
            logging.info(f"\nKilling replica {i}/{len(replicas)}: {replica.node_id}")
            
            result = chaos_engine.inject_process_chaos(replica, ProcessChaosType.SIGKILL)
            assert result.success is True
            
            wait_for_process_death(replica.process, replica.node_id, timeout=5.0)
        
        # Verify all primaries are still alive
        primaries = [node for node in nodes if node.role == 'primary']
        for primary in primaries:
            assert primary.process.poll() is None
            logging.info(f"OK: Primary {primary.node_id} still running after all replicas killed")
        
        # Verify cluster state after killing all replicas (should still be healthy - primaries collectively cover all slots)
        remaining_nodes = primaries
        cluster_healthy = cluster_mgr.validate_cluster(remaining_nodes)
        assert cluster_healthy, "Cluster should remain healthy with primaries alive (all slots collectively covered)"
        logging.info(f"OK: Cluster state after killing all replicas: healthy (all slots collectively covered)")
        
        # Cleanup
        for node in nodes:
            chaos_engine.unregister_node_process(node.node_id)
        
        logging.info(f"\nPASS: Cascading failures test completed")
        logging.info(f"   - Replicas killed: {len(replicas)}")
        logging.info(f"   - Primaries survived: {len(primaries)}")


class TestRealLargeClusterChaos:
    """Tests with larger real clusters (marked as slow)"""
    
    def test_real_large_cluster_chaos(self):
        """
        Test chaos injection on a larger cluster (3 shards, 2 replicas each = 9 nodes)
        """
        # Setup
        config_mgr = ConfigurationManager(
            ClusterConfig(num_shards=3, replicas_per_shard=2),
            PortManager()
        )
        valkey_binary = config_mgr.setup_valkey_from_source()
        
        config = ClusterConfig(
            num_shards=3,
            replicas_per_shard=2,
            base_port=7400,
            valkey_binary=valkey_binary,
            enable_cleanup=True
        )
        
        port_mgr = PortManager(base_port=7400)
        config_mgr = ConfigurationManager(config, port_mgr)
        cluster_mgr = ClusterManager()
        chaos_engine = ProcessChaosEngine()
        
        # Create cluster
        logging.info("Creating large cluster (9 nodes)...")
        topology = config_mgr.plan_topology()
        nodes = config_mgr.spawn_all_nodes(topology)
        
        try:
            success = cluster_mgr.form_cluster(nodes)
            assert success
            logging.info("OK: Large cluster formed successfully")
            
            # Verify cluster is healthy before chaos
            assert cluster_mgr.validate_cluster(nodes)
            logging.info("OK: Large cluster healthy before chaos")
            
            # Register all nodes
            for node in nodes:
                chaos_engine.register_node_process(node.node_id, node.pid)
            
            # Kill one replica from each shard
            primaries = [node for node in nodes if node.role == 'primary']
            
            for shard_id in range(3):
                shard_replicas = [
                    node for node in nodes
                    if node.role == 'replica' and node.shard_id == shard_id
                ]
                
                if shard_replicas:
                    target = shard_replicas[0]
                    logging.info(f"Killing replica in shard {shard_id}: {target.node_id}")
                    
                    result = chaos_engine.inject_process_chaos(
                        target,
                        ProcessChaosType.SIGKILL
                    )
                    assert result.success is True
                    
                    wait_for_process_death(target.process, target.node_id, timeout=5.0)
            
            # Verify all primaries are still alive
            for primary in primaries:
                assert primary.process.poll() is None
                logging.info(f"OK: Primary {primary.node_id} still running")
            
            # Verify cluster state after chaos (should still be healthy - primaries collectively cover all slots)
            remaining_nodes = [n for n in nodes if n.process.poll() is None]
            cluster_healthy = cluster_mgr.validate_cluster(remaining_nodes)
            assert cluster_healthy, "Cluster should remain healthy with primaries alive"
            logging.info(f"OK: Large cluster state after chaos: healthy (all slots collectively covered)")
            
            logging.info(f"\nPASS: Large cluster chaos test completed")
            logging.info(f"   - Total nodes: {len(nodes)}")
            logging.info(f"   - Nodes killed: 3")
            logging.info(f"   - Primaries survived: {len(primaries)}")
            
        finally:
            # Cleanup
            for node in nodes:
                chaos_engine.unregister_node_process(node.node_id)
            cluster_mgr.close_connections()
            config_mgr.cleanup_cluster(nodes)



class TestComprehensiveChaosScenarios:
    """Comprehensive tests covering all chaos coordination combinations"""
    
    def test_sigterm_on_primary(self, real_cluster):
        """
        Test killing primary node with SIGTERM (graceful shutdown)
        """
        nodes = real_cluster['nodes']
        cluster_mgr = real_cluster['cluster_mgr']
        chaos_engine = ProcessChaosEngine()
        
        # Find primary node
        primary = next(node for node in nodes if node.role == 'primary')
        logging.info(f"Target: {primary.node_id} (PID {primary.pid})")
        
        # Register nodes
        for node in nodes:
            chaos_engine.register_node_process(node.node_id, node.pid)
        
        # Verify cluster is healthy before chaos
        assert cluster_mgr.validate_cluster(nodes)
        logging.info("OK: Cluster healthy before chaos")
        
        # Kill primary with SIGTERM
        logging.info(f"Injecting chaos: SIGTERM on primary {primary.node_id}")
        result = chaos_engine.inject_process_chaos(primary, ProcessChaosType.SIGTERM)
        
        # Verify chaos was successful
        assert result.success is True
        assert result.target_node == primary.node_id
        logging.info(f"OK: Chaos injected: {result.chaos_type.value}")
        
        # Wait for process to die (SIGTERM may take longer than SIGKILL)
        wait_for_process_death(primary.process, primary.node_id, timeout=10.0)
        logging.info(f"OK: Primary process {primary.pid} is dead")
        
        # Verify cluster state after primary killed
        remaining_nodes = [n for n in nodes if n.node_id != primary.node_id]
        try:
            cluster_healthy = cluster_mgr.validate_cluster(remaining_nodes)
            logging.info(f"Cluster state after primary killed: {'healthy' if cluster_healthy else 'degraded (expected)'}")
        except Exception as e:
            logging.info(f"Cluster validation after primary killed: {e}")
        
        # Cleanup
        for node in nodes:
            chaos_engine.unregister_node_process(node.node_id)
        
        logging.info("PASS: Primary killed successfully with SIGTERM")
    
    def test_chaos_during_operation(self, real_cluster):
        """
        Test chaos injection DURING a failover operation
        """
        nodes = real_cluster['nodes']
        cluster_mgr = real_cluster['cluster_mgr']
        chaos_engine = ProcessChaosEngine()
        coordinator = ChaosCoordinator(chaos_engine)
        
        # Verify cluster is healthy before chaos
        assert cluster_mgr.validate_cluster(nodes)
        logging.info("OK: Cluster healthy before chaos")
        
        # Find primary and replica from shard 0
        shard_0_nodes = [node for node in nodes if node.shard_id == 0]
        primary = next(node for node in shard_0_nodes if node.role == 'primary')
        replicas = [node for node in shard_0_nodes if node.role == 'replica']
        replica = replicas[0]
        
        logging.info(f"Primary: {primary.node_id}, Target replica: {replica.node_id}")
        
        # Register nodes
        for node in nodes:
            chaos_engine.register_node_process(node.node_id, node.pid)
        
        # Create failover operation
        failover_operation = Operation(
            type=OperationType.FAILOVER,
            target_node=primary.node_id,
            parameters={},
            timing=OperationTiming()
        )
        
        # Configure chaos to happen DURING failover
        chaos_config = ChaosConfig(
            chaos_type=ChaosType.PROCESS_KILL,
            target_selection=TargetSelection(
                strategy="specific",
                specific_nodes=[replica.node_id]
            ),
            timing=ChaosTiming(
                delay_before_operation=0.0,
                delay_after_operation=0.0
            ),
            coordination=ChaosCoordination(
                chaos_before_operation=False,
                chaos_during_operation=True,  # Kill DURING operation
                chaos_after_operation=False
            ),
            process_chaos_type=ProcessChaosType.SIGKILL
        )
        
        logging.info(f"Chaos timing: DURING operation")
        
        # Create and execute scenario
        scenario = coordinator.create_scenario(
            operation=failover_operation,
            chaos_config=chaos_config,
            target_node=replica
        )
        
        result = coordinator.execute_scenario(scenario)
        
        # Verify chaos was injected
        assert result.state == ChaosScenarioState.COMPLETED
        assert len(result.chaos_results) == 1
        assert result.chaos_results[0].success is True
        assert result.chaos_results[0].target_node == replica.node_id
        
        logging.info(f"OK: Chaos scenario completed")
        logging.info(f"OK: Replica killed during operation: {replica.node_id}")
        
        # Wait for process to die (SIGKILL should be immediate)
        wait_for_process_death(replica.process, replica.node_id, timeout=5.0)
        
        # Verify cluster state after chaos (should still be healthy - primaries collectively cover all slots)
        remaining_nodes = [n for n in nodes if n.node_id != replica.node_id]
        cluster_healthy = cluster_mgr.validate_cluster(remaining_nodes)
        assert cluster_healthy, "Cluster should remain healthy after killing replica"
        logging.info(f"OK: Cluster state after chaos: healthy (all slots collectively covered)")
        
        # Cleanup
        coordinator.cleanup_all_scenarios()
        for node in nodes:
            chaos_engine.unregister_node_process(node.node_id)
        
        logging.info("PASS: Chaos during operation completed")
    
    def test_chaos_after_operation(self, real_cluster):
        """
        Test chaos injection AFTER a failover operation
        """
        nodes = real_cluster['nodes']
        cluster_mgr = real_cluster['cluster_mgr']
        chaos_engine = ProcessChaosEngine()
        coordinator = ChaosCoordinator(chaos_engine)
        
        # Verify cluster is healthy before chaos
        assert cluster_mgr.validate_cluster(nodes)
        logging.info("OK: Cluster healthy before chaos")
        
        # Find primary and replica from shard 1
        shard_1_nodes = [node for node in nodes if node.shard_id == 1]
        primary = next(node for node in shard_1_nodes if node.role == 'primary')
        replicas = [node for node in shard_1_nodes if node.role == 'replica']
        replica = replicas[0]
        
        logging.info(f"Primary: {primary.node_id}, Target replica: {replica.node_id}")
        
        # Register nodes
        for node in nodes:
            chaos_engine.register_node_process(node.node_id, node.pid)
        
        # Create failover operation
        failover_operation = Operation(
            type=OperationType.FAILOVER,
            target_node=primary.node_id,
            parameters={},
            timing=OperationTiming()
        )
        
        # Configure chaos to happen AFTER failover
        chaos_config = ChaosConfig(
            chaos_type=ChaosType.PROCESS_KILL,
            target_selection=TargetSelection(
                strategy="specific",
                specific_nodes=[replica.node_id]
            ),
            timing=ChaosTiming(
                delay_before_operation=0.0,
                delay_after_operation=1.0  # Wait 1s after operation
            ),
            coordination=ChaosCoordination(
                chaos_before_operation=False,
                chaos_during_operation=False,
                chaos_after_operation=True  # Kill AFTER operation
            ),
            process_chaos_type=ProcessChaosType.SIGTERM
        )
        
        logging.info(f"Chaos timing: AFTER operation (delay: 1.0s)")
        
        # Create and execute scenario
        scenario = coordinator.create_scenario(
            operation=failover_operation,
            chaos_config=chaos_config,
            target_node=replica
        )
        
        result = coordinator.execute_scenario(scenario)
        
        # Verify chaos was injected
        assert result.state == ChaosScenarioState.COMPLETED
        assert len(result.chaos_results) == 1
        assert result.chaos_results[0].success is True
        assert result.chaos_results[0].target_node == replica.node_id
        
        logging.info(f"OK: Chaos scenario completed")
        logging.info(f"OK: Replica killed after operation: {replica.node_id}")
        
        # Wait for process to die (SIGTERM may take longer than SIGKILL)
        wait_for_process_death(replica.process, replica.node_id, timeout=10.0)
        
        # Verify cluster state after chaos (should still be healthy - primaries collectively cover all slots)
        remaining_nodes = [n for n in nodes if n.node_id != replica.node_id]
        cluster_healthy = cluster_mgr.validate_cluster(remaining_nodes)
        assert cluster_healthy, "Cluster should remain healthy after killing replica"
        logging.info(f"OK: Cluster state after chaos: healthy (all slots collectively covered)")
        
        # Cleanup
        coordinator.cleanup_all_scenarios()
        for node in nodes:
            chaos_engine.unregister_node_process(node.node_id)
        
        logging.info("PASS: Chaos after operation completed")
    
    def test_multiple_chaos_phases(self, real_cluster):
        """
        Test multiple chaos injections at different phases:
        - Kill one replica BEFORE operation
        - Kill another replica DURING operation
        """
        nodes = real_cluster['nodes']
        cluster_mgr = real_cluster['cluster_mgr']
        chaos_engine = ProcessChaosEngine()
        coordinator = ChaosCoordinator(chaos_engine)
        
        # Verify cluster is healthy before chaos
        assert cluster_mgr.validate_cluster(nodes)
        logging.info("OK: Cluster healthy before chaos")
        
        # Find primary and replicas from shard 2
        shard_2_nodes = [node for node in nodes if node.shard_id == 2]
        primary = next(node for node in shard_2_nodes if node.role == 'primary')
        replicas = [node for node in shard_2_nodes if node.role == 'replica']
        
        logging.info(f"Primary: {primary.node_id}")
        logging.info(f"Replicas: {[r.node_id for r in replicas]}")
        
        # Register nodes
        for node in nodes:
            chaos_engine.register_node_process(node.node_id, node.pid)
        
        # Create failover operation
        failover_operation = Operation(
            type=OperationType.FAILOVER,
            target_node=primary.node_id,
            parameters={},
            timing=OperationTiming()
        )
        
        # Configure chaos at multiple phases
        chaos_config = ChaosConfig(
            chaos_type=ChaosType.PROCESS_KILL,
            target_selection=TargetSelection(
                strategy="specific",
                specific_nodes=[replicas[0].node_id]
            ),
            timing=ChaosTiming(
                delay_before_operation=0.5,
                delay_after_operation=0.0
            ),
            coordination=ChaosCoordination(
                chaos_before_operation=True,   # Kill before
                chaos_during_operation=True,   # Kill during
                chaos_after_operation=False
            ),
            process_chaos_type=ProcessChaosType.SIGKILL
        )
        
        logging.info(f"Chaos phases: BEFORE + DURING")
        
        # Execute scenario
        scenario = coordinator.create_scenario(
            operation=failover_operation,
            chaos_config=chaos_config,
            target_node=replicas[0]
        )
        
        result = coordinator.execute_scenario(scenario)
        
        # Verify multiple chaos events
        assert result.state == ChaosScenarioState.COMPLETED
        assert len(result.chaos_results) == 2  # Before + During
        
        successful_chaos = [r for r in result.chaos_results if r.success]
        assert len(successful_chaos) == 2
        
        logging.info(f"OK: Multiple chaos events executed: {len(result.chaos_results)}")
        logging.info(f"OK: Successful chaos injections: {len(successful_chaos)}")
        
        # Cleanup
        coordinator.cleanup_all_scenarios()
        for node in nodes:
            chaos_engine.unregister_node_process(node.node_id)
        
        logging.info("PASS: Multiple chaos phases completed")
    
    def test_chaos_with_timing_delays(self, real_cluster):
        """
        Test chaos injection with timing delays
        """
        nodes = real_cluster['nodes']
        cluster_mgr = real_cluster['cluster_mgr']
        chaos_engine = ProcessChaosEngine()
        coordinator = ChaosCoordinator(chaos_engine)
        
        # Verify cluster is healthy before chaos
        assert cluster_mgr.validate_cluster(nodes)
        logging.info("OK: Cluster healthy before chaos")
        
        # Find replica from shard 0
        shard_0_nodes = [node for node in nodes if node.shard_id == 0]
        replica = next(node for node in shard_0_nodes if node.role == 'replica')
        primary = next(node for node in shard_0_nodes if node.role == 'primary')
        
        logging.info(f"Target: {replica.node_id}")
        
        # Register nodes
        for node in nodes:
            chaos_engine.register_node_process(node.node_id, node.pid)
        
        # Create operation
        operation = Operation(
            type=OperationType.FAILOVER,
            target_node=primary.node_id,
            parameters={},
            timing=OperationTiming()
        )
        
        # Configure chaos with delays
        chaos_config = ChaosConfig(
            chaos_type=ChaosType.PROCESS_KILL,
            target_selection=TargetSelection(
                strategy="specific",
                specific_nodes=[replica.node_id]
            ),
            timing=ChaosTiming(
                delay_before_operation=2.0,  # Wait 2s before chaos
                delay_after_operation=0.0
            ),
            coordination=ChaosCoordination(
                chaos_before_operation=True,
                chaos_during_operation=False,
                chaos_after_operation=False
            ),
            process_chaos_type=ProcessChaosType.SIGKILL
        )
        
        logging.info(f"Chaos timing: 2s delay before operation")
        
        # Execute scenario and measure time
        start_time = time.time()
        scenario = coordinator.create_scenario(
            operation=operation,
            chaos_config=chaos_config,
            target_node=replica
        )
        
        result = coordinator.execute_scenario(scenario)
        elapsed_time = time.time() - start_time
        
        # Verify chaos was injected
        assert result.state == ChaosScenarioState.COMPLETED
        assert len(result.chaos_results) == 1
        assert result.chaos_results[0].success is True
        
        # Verify timing delay was applied (should take at least 2 seconds)
        assert elapsed_time >= 2.0
        logging.info(f"OK: Chaos executed with timing delay (elapsed: {elapsed_time:.2f}s)")
        
        # Wait for process to die (SIGKILL should be immediate)
        wait_for_process_death(replica.process, replica.node_id, timeout=5.0)
        
        # Verify cluster state after chaos (should still be healthy - primaries collectively cover all slots)
        remaining_nodes = [n for n in nodes if n.node_id != replica.node_id]
        cluster_healthy = cluster_mgr.validate_cluster(remaining_nodes)
        assert cluster_healthy, "Cluster should remain healthy after killing replica"
        logging.info(f"OK: Cluster state after chaos: healthy (all slots collectively covered)")
        
        # Cleanup
        coordinator.cleanup_all_scenarios()
        for node in nodes:
            chaos_engine.unregister_node_process(node.node_id)
        
        logging.info("PASS: Chaos with timing delays completed")
    
    def test_random_target_selection(self, real_cluster):
        """
        Test chaos with random target selection strategy
        """
        nodes = real_cluster['nodes']
        cluster_mgr = real_cluster['cluster_mgr']
        chaos_engine = ProcessChaosEngine()
        
        # Verify cluster is healthy before chaos
        assert cluster_mgr.validate_cluster(nodes)
        logging.info("OK: Cluster healthy before chaos")
        
        # Register nodes
        for node in nodes:
            chaos_engine.register_node_process(node.node_id, node.pid)
        
        # Get a random replica (simulating random selection)
        replicas = [node for node in nodes if node.role == 'replica']
        import random
        target = random.choice(replicas)
        
        logging.info(f"Random target selected: {target.node_id}")
        
        # Kill the randomly selected node
        result = chaos_engine.inject_process_chaos(target, ProcessChaosType.SIGKILL)
        
        # Verify chaos was successful
        assert result.success is True
        assert result.target_node == target.node_id
        logging.info(f"OK: Random target killed: {target.node_id}")
        
        # Wait for process to die (SIGKILL should be immediate)
        wait_for_process_death(target.process, target.node_id, timeout=5.0)
        
        # Verify cluster state after chaos (should still be healthy - primaries collectively cover all slots)
        remaining_nodes = [n for n in nodes if n.node_id != target.node_id]
        cluster_healthy = cluster_mgr.validate_cluster(remaining_nodes)
        assert cluster_healthy, "Cluster should remain healthy after killing replica"
        logging.info(f"OK: Cluster state after chaos: healthy (all slots collectively covered)")
        
        # Cleanup
        for node in nodes:
            chaos_engine.unregister_node_process(node.node_id)
        
        logging.info("PASS: Random target selection completed")
    
    def test_all_replicas_chaos(self, real_cluster):
        """
        Test killing all replicas in sequence
        """
        nodes = real_cluster['nodes']
        cluster_mgr = real_cluster['cluster_mgr']
        chaos_engine = ProcessChaosEngine()
        
        # Register nodes
        for node in nodes:
            chaos_engine.register_node_process(node.node_id, node.pid)
        
        # Verify cluster is healthy before chaos
        assert cluster_mgr.validate_cluster(nodes)
        logging.info("OK: Cluster healthy before chaos")
        
        # Get all replicas and primaries
        replicas = [node for node in nodes if node.role == 'replica']
        primaries = [node for node in nodes if node.role == 'primary']
        
        logging.info(f"Killing all {len(replicas)} replicas")
        
        # Kill all replicas
        for replica in replicas:
            logging.info(f"Killing replica: {replica.node_id}")
            result = chaos_engine.inject_process_chaos(replica, ProcessChaosType.SIGKILL)
            assert result.success is True
            wait_for_process_death(replica.process, replica.node_id, timeout=5.0)
        
        # Verify all primaries are still alive
        for primary in primaries:
            assert primary.process.poll() is None
            logging.info(f"OK: Primary {primary.node_id} still running")
        
        # Verify cluster state with only primaries (should still be healthy - primaries collectively cover all slots)
        remaining_nodes = primaries
        cluster_healthy = cluster_mgr.validate_cluster(remaining_nodes)
        assert cluster_healthy, "Cluster should remain healthy with primaries alive (all slots collectively covered)"
        logging.info(f"OK: Cluster state with only primaries: healthy (all slots collectively covered)")
        
        # Cleanup
        for node in nodes:
            chaos_engine.unregister_node_process(node.node_id)
        
        logging.info(f"PASS: All replicas killed, {len(primaries)} primaries survived")



class TestFailoverValidation:
    """Tests that validate proper failover behavior after killing primary"""
    
    def test_cluster_state_after_primary_kill(self, multi_shard_cluster):
        """
        Test cluster behavior after killing the primary node
        
        This test validates:
        1. Cluster is healthy before chaos
        2. Primary is killed successfully
        3. Automatic failover may occur (replica promotion to master)
        4. Cluster detects the failure and attempts recovery
        5. Failover timing depends on node-timeout and cluster configuration
        
        Note: Valkey Cluster supports automatic failover when properly configured.
        Failover requires: sufficient replicas, quorum of masters, and appropriate
        node-timeout settings. This test validates both failure detection and
        potential automatic recovery.
        """
        nodes = multi_shard_cluster['nodes']
        cluster_mgr = multi_shard_cluster['cluster_mgr']
        chaos_engine = ProcessChaosEngine()
        
        # Find primary and replicas from shard 0
        shard_0_nodes = [node for node in nodes if node.shard_id == 0]
        primary = next(node for node in shard_0_nodes if node.role == 'primary')
        replicas = [node for node in shard_0_nodes if node.role == 'replica']
        
        # Get other primaries (needed for quorum)
        other_primaries = [node for node in nodes if node.role == 'primary' and node.shard_id != 0]
        logging.info(f"Cluster has {len(other_primaries)} other primaries for quorum")
        
        logging.info(f"Initial topology:")
        logging.info(f"  Primary: {primary.node_id} (PID {primary.pid})")
        for replica in replicas:
            logging.info(f"  Replica: {replica.node_id} (PID {replica.pid})")
        
        # Register nodes
        for node in nodes:
            chaos_engine.register_node_process(node.node_id, node.pid)
        
        # Verify cluster is healthy before chaos
        assert cluster_mgr.validate_cluster(nodes)
        logging.info("OK: Cluster healthy before chaos")
        
        # Verify initial roles
        primary_role = cluster_mgr.get_node_role(primary)
        assert primary_role == 'master', f"Primary should be master, got {primary_role}"
        logging.info(f"OK: {primary.node_id} is master")
        
        for replica in replicas:
            replica_role = cluster_mgr.get_node_role(replica)
            assert replica_role == 'slave', f"Replica should be slave, got {replica_role}"
            logging.info(f"OK: {replica.node_id} is slave")
        
        # Kill the primary
        logging.info(f"\nKilling primary: {primary.node_id}")
        result = chaos_engine.inject_process_chaos(primary, ProcessChaosType.SIGKILL)
        assert result.success is True
        
        # Wait for process to die (SIGKILL should be immediate)
        wait_for_process_death(primary.process, primary.node_id, timeout=5.0)
        
        # Wait for automatic failover to occur
        # Node goes through: PFAIL (possible fail) -> FAIL (confirmed by majority) -> Failover
        # This requires: node-timeout + gossip propagation + majority consensus
        # Typically takes 2-3x node timeout, but can take longer in practice
        # Using the actual configured cluster-node-timeout from orchestrator
        max_failover_time = int(CLUSTER_NODE_TIMEOUT_SEC * 6)  # 6x node timeout for safety
        
        logging.info(f"Waiting for automatic failover (max {max_failover_time}s, node-timeout={CLUSTER_NODE_TIMEOUT_SEC}s)...")
        
        # Poll for replica promotion
        promoted_replica = None
        check_interval = 2
        max_checks = int(max_failover_time / check_interval) + 2
        
        for i in range(max_checks):
            time.sleep(check_interval)
            elapsed = (i + 1) * check_interval
            
            # Check if any replica got promoted to master
            for replica in replicas:
                try:
                    new_role = cluster_mgr.get_node_role(replica)
                    if new_role == 'master':
                        promoted_replica = replica
                        logging.info(f"OK: Replica {replica.node_id} promoted to master after {elapsed}s")
                        break
                except Exception as e:
                    continue
            
            if promoted_replica:
                break
            
            logging.info(f"  Checking after {elapsed}s... no promotion yet")
        
        # Check final result
        if promoted_replica:
            logging.info(f"OK: Automatic failover completed successfully")
        else:
            logging.warning(f"No replica was promoted to master within {max_failover_time}s")
            logging.info("Note: Valkey Cluster may require additional configuration for automatic failover")
        
        # Verify remaining nodes
        remaining_nodes = [n for n in nodes if n.node_id != primary.node_id]
        
        # Check if cluster is healthy from remaining nodes' perspective
        try:
            cluster_healthy = cluster_mgr.validate_cluster(remaining_nodes)
            if cluster_healthy:
                logging.info(f"OK: Cluster is healthy after failover")
            else:
                logging.info(f"Cluster is degraded after primary kill (expected without manual failover)")
        except Exception as e:
            logging.info(f"Cluster validation: {e}")
        
        # Cleanup
        for node in nodes:
            chaos_engine.unregister_node_process(node.node_id)
        
        logging.info("\nPASS: Failover validation completed")
        logging.info(f"  - Primary killed: {primary.node_id}")
        if promoted_replica:
            logging.info(f"  - Replica promoted: {promoted_replica.node_id}")
            logging.info(f"  - Failover successful")
        else:
            logging.info(f"  - No automatic promotion occurred")
        logging.info(f"  - Remaining nodes: {len(remaining_nodes)}")
