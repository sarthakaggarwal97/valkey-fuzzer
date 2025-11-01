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
