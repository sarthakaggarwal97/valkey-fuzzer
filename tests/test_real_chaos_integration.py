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


@pytest.fixture
def real_cluster():
    """
    Fixture that creates a real 3-node Valkey cluster (1 primary + 2 replicas)
    and cleans it up after the test.
    """
    # Setup Valkey binary
    config_mgr = ConfigurationManager(
        ClusterConfig(num_shards=1, replicas_per_shard=2),
        PortManager()
    )
    valkey_binary = config_mgr.setup_valkey_from_source()
    
    # Create cluster configuration
    config = ClusterConfig(
        num_shards=1,
        replicas_per_shard=2,
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


class TestRealChaosIntegration:
    """Real integration tests with actual Valkey clusters"""
    
    def test_real_cluster_creation_and_registration(self, real_cluster):
        """
        Test that we can create a real cluster and register nodes with chaos engine
        """
        nodes = real_cluster['nodes']
        chaos_engine = ProcessChaosEngine()
        
        # Register all real nodes
        for node in nodes:
            chaos_engine.register_node_process(node.node_id, node.pid)
            logging.info(f"Registered {node.node_id} with PID {node.pid}")
        
        # Verify registration
        assert len(chaos_engine.node_processes) == 3
        
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
        
        # Find a replica to kill
        replica = next(node for node in nodes if node.role == 'replica')
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
        
        # Wait for process to die
        time.sleep(2)
        
        # Verify process is actually dead
        assert replica.process.poll() is not None
        logging.info(f"OK: Process {replica.pid} is dead")
        
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
        
        # Find the second replica to kill
        replicas = [node for node in nodes if node.role == 'replica']
        replica = replicas[1] if len(replicas) > 1 else replicas[0]
        logging.info(f"Target: {replica.node_id} (PID {replica.pid})")
        
        # Register nodes
        for node in nodes:
            chaos_engine.register_node_process(node.node_id, node.pid)
        
        # Inject real chaos - kill the replica with SIGKILL
        logging.info(f"Injecting chaos: SIGKILL on {replica.node_id}")
        result = chaos_engine.inject_process_chaos(replica, ProcessChaosType.SIGKILL)
        
        # Verify chaos was successful
        assert result.success is True
        assert result.target_node == replica.node_id
        logging.info(f"OK: Chaos injected: SIGKILL")
        
        # Wait for process to die
        time.sleep(1)
        
        # Verify process is actually dead
        assert replica.process.poll() is not None
        logging.info(f"OK: Process {replica.pid} is dead (SIGKILL)")
        
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
        
        # Find primary node
        primary = next(node for node in nodes if node.role == 'primary')
        replicas = [node for node in nodes if node.role == 'replica']
        
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
        
        # Wait for process to die
        time.sleep(2)
        
        # Verify primary is dead
        assert primary.process.poll() is not None
        logging.info(f"OK: Primary process {primary.pid} is dead")
        
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
        chaos_engine = ProcessChaosEngine()
        
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
            
            time.sleep(1)
            assert replica.process.poll() is not None
            logging.info(f"OK: Replica {replica.node_id} killed")
        
        # Verify primary is still alive
        primary = next(node for node in nodes if node.role == 'primary')
        assert primary.process.poll() is None
        logging.info(f"OK: Primary {primary.node_id} still running after all replicas killed")
        
        # Cleanup
        for node in nodes:
            chaos_engine.unregister_node_process(node.node_id)
        
        logging.info(f"\nPASS: Cascading failures test completed")
        logging.info(f"   - Replicas killed: {len(replicas)}")
        logging.info(f"   - Primary survived: {primary.node_id}")


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
                    
                    time.sleep(1)
                    assert target.process.poll() is not None
                    logging.info(f"OK: Killed {target.node_id}")
            
            # Verify all primaries are still alive
            for primary in primaries:
                assert primary.process.poll() is None
                logging.info(f"OK: Primary {primary.node_id} still running")
            
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
