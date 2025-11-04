"""
Tests for Chaos Coordinator
"""
import pytest
from unittest.mock import Mock, patch
from src.fuzzer_engine.chaos_coordinator import ChaosCoordinator
from src.models import (
    Operation, OperationType, OperationTiming, ChaosConfig, ChaosType,
    ProcessChaosType, TargetSelection, ChaosTiming, ChaosCoordination,
    NodeInfo, ChaosResult
)


def test_chaos_coordinator_initialization():
    """Test chaos coordinator initialization"""
    coordinator = ChaosCoordinator()
    
    assert coordinator.chaos_engine is not None
    assert coordinator.active_chaos_scenarios == {}
    assert coordinator.chaos_history == []


def test_register_cluster_nodes():
    """Test registering cluster nodes"""
    coordinator = ChaosCoordinator()
    
    nodes = [
        NodeInfo(
            node_id="node-0",
            role="primary",
            shard_id=0,
            port=7000,
            bus_port=17000,
            pid=12345,
            process=Mock(),
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        ),
        NodeInfo(
            node_id="node-1",
            role="replica",
            shard_id=0,
            port=7001,
            bus_port=17001,
            pid=12346,
            process=Mock(),
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        )
    ]
    
    coordinator.register_cluster_nodes("test-cluster", nodes)
    
    # Verify nodes are registered in chaos engine
    assert "node-0" in coordinator.chaos_engine.node_processes
    assert "node-1" in coordinator.chaos_engine.node_processes
    assert coordinator.chaos_engine.node_processes["node-0"] == 12345
    assert coordinator.chaos_engine.node_processes["node-1"] == 12346


def test_select_chaos_target_random():
    """Test selecting chaos target with random strategy"""
    coordinator = ChaosCoordinator()
    
    nodes = [
        NodeInfo(
            node_id="node-0",
            role="primary",
            shard_id=0,
            port=7000,
            bus_port=17000,
            pid=12345,
            process=Mock(),
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        ),
        NodeInfo(
            node_id="node-1",
            role="replica",
            shard_id=0,
            port=7001,
            bus_port=17001,
            pid=12346,
            process=Mock(),
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        )
    ]
    
    target_selection = TargetSelection(strategy="random")
    target = coordinator._select_chaos_target(nodes, target_selection)
    
    assert target is not None
    assert target.node_id in ["node-0", "node-1"]


def test_select_chaos_target_primary_only():
    """Test selecting chaos target with primary_only strategy"""
    coordinator = ChaosCoordinator()
    
    nodes = [
        NodeInfo(
            node_id="node-0",
            role="primary",
            shard_id=0,
            port=7000,
            bus_port=17000,
            pid=12345,
            process=Mock(),
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        ),
        NodeInfo(
            node_id="node-1",
            role="replica",
            shard_id=0,
            port=7001,
            bus_port=17001,
            pid=12346,
            process=Mock(),
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        )
    ]
    
    target_selection = TargetSelection(strategy="primary_only")
    target = coordinator._select_chaos_target(nodes, target_selection)
    
    assert target is not None
    assert target.node_id == "node-0"
    assert target.role == "primary"


def test_select_chaos_target_replica_only():
    """Test selecting chaos target with replica_only strategy"""
    coordinator = ChaosCoordinator()
    
    nodes = [
        NodeInfo(
            node_id="node-0",
            role="primary",
            shard_id=0,
            port=7000,
            bus_port=17000,
            pid=12345,
            process=Mock(),
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        ),
        NodeInfo(
            node_id="node-1",
            role="replica",
            shard_id=0,
            port=7001,
            bus_port=17001,
            pid=12346,
            process=Mock(),
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        )
    ]
    
    target_selection = TargetSelection(strategy="replica_only")
    target = coordinator._select_chaos_target(nodes, target_selection)
    
    assert target is not None
    assert target.node_id == "node-1"
    assert target.role == "replica"


def test_select_chaos_target_specific():
    """Test selecting chaos target with specific strategy"""
    coordinator = ChaosCoordinator()
    
    nodes = [
        NodeInfo(
            node_id="node-0",
            role="primary",
            shard_id=0,
            port=7000,
            bus_port=17000,
            pid=12345,
            process=Mock(),
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        ),
        NodeInfo(
            node_id="node-1",
            role="replica",
            shard_id=0,
            port=7001,
            bus_port=17001,
            pid=12346,
            process=Mock(),
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        )
    ]
    
    target_selection = TargetSelection(strategy="specific", specific_nodes=["node-1"])
    target = coordinator._select_chaos_target(nodes, target_selection)
    
    assert target is not None
    assert target.node_id == "node-1"


def test_select_chaos_target_empty_nodes():
    """Test selecting chaos target with empty node list"""
    coordinator = ChaosCoordinator()
    
    target_selection = TargetSelection(strategy="random")
    target = coordinator._select_chaos_target([], target_selection)
    
    assert target is None


def test_select_chaos_target_no_primary():
    """Test selecting chaos target when no primary nodes exist"""
    coordinator = ChaosCoordinator()
    
    nodes = [
        NodeInfo(
            node_id="node-0",
            role="replica",
            shard_id=0,
            port=7000,
            bus_port=17000,
            pid=12345,
            process=Mock(),
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        )
    ]
    
    target_selection = TargetSelection(strategy="primary_only")
    target = coordinator._select_chaos_target(nodes, target_selection)
    
    assert target is None


def test_get_chaos_history():
    """Test getting chaos history"""
    coordinator = ChaosCoordinator()
    
    # Add some chaos results to history
    result1 = ChaosResult(
        chaos_id="chaos-1",
        chaos_type=ChaosType.PROCESS_KILL,
        target_node="node-0",
        success=True,
        start_time=0.0,
        end_time=1.0
    )
    result2 = ChaosResult(
        chaos_id="chaos-2",
        chaos_type=ChaosType.PROCESS_KILL,
        target_node="node-1",
        success=True,
        start_time=2.0,
        end_time=3.0
    )
    
    coordinator.chaos_history = [result1, result2]
    
    history = coordinator.get_chaos_history()
    assert len(history) == 2
    assert history[0].chaos_id == "chaos-1"
    assert history[1].chaos_id == "chaos-2"


def test_cleanup_chaos():
    """Test cleaning up chaos for a cluster"""
    coordinator = ChaosCoordinator()
    
    # Add active chaos scenario
    coordinator.active_chaos_scenarios["test-cluster"] = []
    
    result = coordinator.cleanup_chaos("test-cluster")
    
    assert result is True
    assert "test-cluster" not in coordinator.active_chaos_scenarios


def test_get_active_chaos_count():
    """Test getting active chaos count"""
    coordinator = ChaosCoordinator()
    
    # Initially should be 0
    assert coordinator.get_active_chaos_count() == 0
    
    # Add some active chaos
    coordinator.chaos_engine.active_chaos["chaos-1"] = Mock()
    coordinator.chaos_engine.active_chaos["chaos-2"] = Mock()
    
    assert coordinator.get_active_chaos_count() == 2


def test_stop_all_chaos():
    """Test stopping all active chaos"""
    coordinator = ChaosCoordinator()
    
    # Add some active chaos
    coordinator.chaos_engine.active_chaos["chaos-1"] = Mock()
    coordinator.chaos_engine.active_chaos["chaos-2"] = Mock()
    
    coordinator.stop_all_chaos()
    
    assert len(coordinator.chaos_engine.active_chaos) == 0


@patch('src.fuzzer_engine.chaos_coordinator.time.sleep')
def test_coordinate_chaos_with_operation_no_target(mock_sleep):
    """Test coordinating chaos when no suitable target found"""
    coordinator = ChaosCoordinator()
    
    operation = Operation(
        type=OperationType.FAILOVER,
        target_node="node-0",
        parameters={},
        timing=OperationTiming()
    )
    
    chaos_config = ChaosConfig(
        chaos_type=ChaosType.PROCESS_KILL,
        target_selection=TargetSelection(strategy="primary_only"),
        timing=ChaosTiming(),
        coordination=ChaosCoordination(chaos_during_operation=True),
        process_chaos_type=ProcessChaosType.SIGKILL
    )
    
    # Empty node list - no target will be found
    results = coordinator.coordinate_chaos_with_operation(operation, chaos_config, [])
    
    assert len(results) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
