"""
Tests for Operation Orchestrator
"""
import pytest
from src.fuzzer_engine.operation_orchestrator import OperationOrchestrator
from src.models import (
    Operation, OperationType, OperationTiming, ClusterStatus,
    NodeInfo, ClusterConfig, ClusterConnection
)


def test_operation_orchestrator_initialization():
    """Test operation orchestrator initialization"""
    orchestrator = OperationOrchestrator()
    
    assert orchestrator.cluster_connection is None
    assert orchestrator.active_operations == {}
    assert orchestrator.operation_counter == 0


def test_set_cluster_connection():
    """Test setting cluster connection"""
    orchestrator = OperationOrchestrator()
    
    # Create mock cluster connection
    nodes = [
        NodeInfo(
            node_id="node-0",
            role="primary",
            shard_id=0,
            port=7000,
            bus_port=17000,
            pid=12345,
            process=None,
            data_dir="/tmp/test",
            log_file="/tmp/test.log",
            slot_start=0,
            slot_end=5461
        )
    ]
    connection = ClusterConnection(nodes, "test-cluster")
    
    orchestrator.set_cluster_connection(connection)
    assert orchestrator.cluster_connection == connection


def test_validate_operation_preconditions_healthy_cluster():
    """Test validating operation preconditions with healthy cluster"""
    orchestrator = OperationOrchestrator()
    
    # Create operation
    operation = Operation(
        type=OperationType.FAILOVER,
        target_node="node-0",
        parameters={},
        timing=OperationTiming()
    )
    
    # Create cluster status
    nodes = [
        NodeInfo(
            node_id="node-0",
            role="primary",
            shard_id=0,
            port=7000,
            bus_port=17000,
            pid=12345,
            process=None,
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        )
    ]
    
    cluster_status = ClusterStatus(
        cluster_id="test",
        nodes=nodes,
        total_slots_assigned=16384,
        is_healthy=True,
        formation_complete=True
    )
    
    assert orchestrator.validate_operation_preconditions(operation, cluster_status) is True


def test_validate_operation_preconditions_unhealthy_cluster():
    """Test validating operation preconditions with unhealthy cluster"""
    orchestrator = OperationOrchestrator()
    
    operation = Operation(
        type=OperationType.FAILOVER,
        target_node="node-0",
        parameters={},
        timing=OperationTiming()
    )
    
    cluster_status = ClusterStatus(
        cluster_id="test",
        nodes=[],
        total_slots_assigned=0,
        is_healthy=False,
        formation_complete=False
    )
    
    assert orchestrator.validate_operation_preconditions(operation, cluster_status) is False


def test_validate_operation_preconditions_target_not_primary():
    """Test validating operation when target is not primary"""
    orchestrator = OperationOrchestrator()
    
    operation = Operation(
        type=OperationType.FAILOVER,
        target_node="node-1",
        parameters={},
        timing=OperationTiming()
    )
    
    nodes = [
        NodeInfo(
            node_id="node-0",
            role="primary",
            shard_id=0,
            port=7000,
            bus_port=17000,
            pid=12345,
            process=None,
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
            process=None,
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        )
    ]
    
    cluster_status = ClusterStatus(
        cluster_id="test",
        nodes=nodes,
        total_slots_assigned=16384,
        is_healthy=True,
        formation_complete=True
    )
    
    assert orchestrator.validate_operation_preconditions(operation, cluster_status) is False


def test_validate_operation_preconditions_target_not_found():
    """Test validating operation when target node not found"""
    orchestrator = OperationOrchestrator()
    
    operation = Operation(
        type=OperationType.FAILOVER,
        target_node="node-999",
        parameters={},
        timing=OperationTiming()
    )
    
    nodes = [
        NodeInfo(
            node_id="node-0",
            role="primary",
            shard_id=0,
            port=7000,
            bus_port=17000,
            pid=12345,
            process=None,
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        )
    ]
    
    cluster_status = ClusterStatus(
        cluster_id="test",
        nodes=nodes,
        total_slots_assigned=16384,
        is_healthy=True,
        formation_complete=True
    )
    
    assert orchestrator.validate_operation_preconditions(operation, cluster_status) is False


def test_execute_operation_no_cluster_connection():
    """Test executing operation without cluster connection"""
    orchestrator = OperationOrchestrator()
    
    operation = Operation(
        type=OperationType.FAILOVER,
        target_node="node-0",
        parameters={},
        timing=OperationTiming()
    )
    
    result = orchestrator.execute_operation(operation, "test-cluster")
    assert result is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


def test_validate_operation_preconditions_exact_node_match():
    """Test that node matching uses exact equality, not substring matching"""
    orchestrator = OperationOrchestrator()
    
    # Create operation targeting "node-1"
    operation = Operation(
        type=OperationType.FAILOVER,
        target_node="node-1",
        parameters={},
        timing=OperationTiming()
    )
    
    # Create cluster with node-1 and node-10
    # This tests that "node-1" doesn't incorrectly match "node-10"
    nodes = [
        NodeInfo(
            node_id="node-0",
            role="primary",
            shard_id=0,
            port=7000,
            bus_port=17000,
            pid=12345,
            process=None,
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        ),
        NodeInfo(
            node_id="node-1",
            role="primary",
            shard_id=1,
            port=7001,
            bus_port=17001,
            pid=12346,
            process=None,
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        ),
        NodeInfo(
            node_id="node-10",
            role="primary",
            shard_id=2,
            port=7010,
            bus_port=17010,
            pid=12347,
            process=None,
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        )
    ]
    
    cluster_status = ClusterStatus(
        cluster_id="test",
        nodes=nodes,
        total_slots_assigned=16384,
        is_healthy=True,
        formation_complete=True
    )
    
    # Should succeed - exact match found
    assert orchestrator.validate_operation_preconditions(operation, cluster_status) is True


def test_validate_operation_preconditions_no_substring_false_positive():
    """Test that substring matching doesn't cause false positives"""
    orchestrator = OperationOrchestrator()
    
    # Create operation targeting "node-1" which doesn't exist
    operation = Operation(
        type=OperationType.FAILOVER,
        target_node="node-1",
        parameters={},
        timing=OperationTiming()
    )
    
    # Create cluster with only node-10, node-11, node-12
    # "node-1" should NOT match any of these
    nodes = [
        NodeInfo(
            node_id="node-10",
            role="primary",
            shard_id=0,
            port=7010,
            bus_port=17010,
            pid=12345,
            process=None,
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        ),
        NodeInfo(
            node_id="node-11",
            role="primary",
            shard_id=1,
            port=7011,
            bus_port=17011,
            pid=12346,
            process=None,
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        ),
        NodeInfo(
            node_id="node-12",
            role="primary",
            shard_id=2,
            port=7012,
            bus_port=17012,
            pid=12347,
            process=None,
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        )
    ]
    
    cluster_status = ClusterStatus(
        cluster_id="test",
        nodes=nodes,
        total_slots_assigned=16384,
        is_healthy=True,
        formation_complete=True
    )
    
    # Should fail - no exact match found (substring matching would incorrectly succeed)
    assert orchestrator.validate_operation_preconditions(operation, cluster_status) is False
