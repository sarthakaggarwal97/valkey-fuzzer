"""
Tests for core data models
"""
import pytest
from src.models import (
    ClusterConfig, NodeConfig, NodeRole, OperationType, ChaosType,
    Scenario, Operation, OperationTiming, ChaosConfig,
    TargetSelection, ChaosTiming, ChaosCoordination, ValidationConfig
)


def test_cluster_config_validation():
    """Test cluster configuration validation"""
    # Valid configuration
    nodes = [
        NodeConfig("node1", "localhost", 7000, NodeRole.PRIMARY),
        NodeConfig("node2", "localhost", 7001, NodeRole.REPLICA, "node1")
    ]
    config = ClusterConfig(shard_count=3, replica_count=1, node_configs=nodes)
    assert config.shard_count == 3
    assert config.replica_count == 1
    
    # Invalid shard count
    with pytest.raises(ValueError, match="Shard count must be between 3 and 16"):
        ClusterConfig(shard_count=2, replica_count=1, node_configs=nodes)
    
    # Invalid replica count
    with pytest.raises(ValueError, match="Replica count must be between 0 and 2"):
        ClusterConfig(shard_count=3, replica_count=3, node_configs=nodes)


def test_node_config_creation():
    """Test node configuration creation"""
    # Primary node
    primary = NodeConfig("node1", "localhost", 7000, NodeRole.PRIMARY)
    assert primary.node_id == "node1"
    assert primary.role == NodeRole.PRIMARY
    assert primary.primary_id is None
    
    # Replica node
    replica = NodeConfig("node2", "localhost", 7001, NodeRole.REPLICA, "node1")
    assert replica.node_id == "node2"
    assert replica.role == NodeRole.REPLICA
    assert replica.primary_id == "node1"


def test_operation_creation():
    """Test operation creation"""
    timing = OperationTiming(delay_before=1.0, timeout=30.0, delay_after=2.0)
    operation = Operation(
        type=OperationType.FAILOVER,
        target_node="node1",
        parameters={"force": True},
        timing=timing
    )
    
    assert operation.type == OperationType.FAILOVER
    assert operation.target_node == "node1"
    assert operation.parameters["force"] is True
    assert operation.timing.delay_before == 1.0


def test_chaos_config_creation():
    """Test chaos configuration creation"""
    target_selection = TargetSelection(strategy="random")
    timing = ChaosTiming(delay_before_operation=1.0, chaos_duration=10.0)
    coordination = ChaosCoordination(coordinate_with_operation=True)
    
    chaos_config = ChaosConfig(
        chaos_type=ChaosType.PROCESS_KILL,
        target_selection=target_selection,
        timing=timing,
        coordination=coordination
    )
    
    assert chaos_config.chaos_type == ChaosType.PROCESS_KILL
    assert chaos_config.target_selection.strategy == "random"
    assert chaos_config.timing.chaos_duration == 10.0
    assert chaos_config.coordination.coordinate_with_operation is True


def test_scenario_creation():
    """Test complete test scenario creation"""
    # Create cluster config
    nodes = [NodeConfig("node1", "localhost", 7000, NodeRole.PRIMARY)]
    cluster_config = ClusterConfig(shard_count=3, replica_count=0, node_configs=nodes)
    
    # Create operation
    operation = Operation(
        type=OperationType.FAILOVER,
        target_node="node1",
        parameters={},
        timing=OperationTiming()
    )
    
    # Create chaos config
    chaos_config = ChaosConfig(
        chaos_type=ChaosType.PROCESS_KILL,
        target_selection=TargetSelection(strategy="random"),
        timing=ChaosTiming(),
        coordination=ChaosCoordination()
    )
    
    # Create validation config
    validation_config = ValidationConfig()
    
    # Create test scenario
    scenario = Scenario(
        scenario_id="test-001",
        cluster_config=cluster_config,
        operations=[operation],
        chaos_config=chaos_config,
        validation_config=validation_config,
        seed=12345
    )
    
    assert scenario.scenario_id == "test-001"
    assert scenario.seed == 12345
    assert len(scenario.operations) == 1
    assert scenario.operations[0].type == OperationType.FAILOVER


if __name__ == "__main__":
    pytest.main([__file__])