"""
Tests for State Validator
"""
import pytest
from src.fuzzer_engine.state_validator import StateValidator
from src.models import (
    ValidationConfig, ClusterStatus, NodeInfo, ReplicationStatus,
    ConnectivityStatus, ConsistencyStatus
)


def test_state_validator_initialization():
    """Test state validator initialization"""
    validator = StateValidator()
    
    assert validator.validation_config is not None
    assert validator.validation_config.check_slot_coverage is True
    assert validator.validation_cache == {}


def test_state_validator_with_custom_config():
    """Test state validator with custom configuration"""
    config = ValidationConfig(
        check_slot_coverage=True,
        check_slot_conflicts=False,
        check_replica_sync=True,
        convergence_timeout=30.0
    )
    
    validator = StateValidator(config)
    
    assert validator.validation_config == config
    assert validator.validation_config.convergence_timeout == 30.0


def test_validate_slot_coverage_full():
    """Test validating slot coverage when all slots assigned"""
    validator = StateValidator()
    
    cluster_status = ClusterStatus(
        cluster_id="test",
        nodes=[],
        total_slots_assigned=16384,
        is_healthy=True,
        formation_complete=True
    )
    
    assert validator.validate_slot_coverage(cluster_status) is True


def test_validate_slot_coverage_partial():
    """Test validating slot coverage when not all slots assigned"""
    validator = StateValidator()
    
    cluster_status = ClusterStatus(
        cluster_id="test",
        nodes=[],
        total_slots_assigned=10000,
        is_healthy=False,
        formation_complete=True
    )
    
    assert validator.validate_slot_coverage(cluster_status) is False


def test_validate_replica_sync_simplified():
    """Test simplified replica sync validation"""
    validator = StateValidator()
    
    cluster_status = ClusterStatus(
        cluster_id="test",
        nodes=[],
        total_slots_assigned=16384,
        is_healthy=True,
        formation_complete=True
    )
    
    # Simplified interface always returns True for now
    assert validator.validate_replica_sync(cluster_status) is True


def test_validate_data_consistency_simplified():
    """Test simplified data consistency validation"""
    validator = StateValidator()
    
    cluster_status = ClusterStatus(
        cluster_id="test",
        nodes=[],
        total_slots_assigned=16384,
        is_healthy=True,
        formation_complete=True
    )
    
    # Simplified interface always returns True for now
    assert validator.validate_data_consistency(cluster_status) is True


def test_validation_config_defaults():
    """Test validation configuration defaults"""
    config = ValidationConfig()
    
    assert config.check_slot_coverage is True
    assert config.check_slot_conflicts is True
    assert config.check_replica_sync is True
    assert config.check_node_connectivity is True
    assert config.check_data_consistency is True
    assert config.convergence_timeout == 60.0
    assert config.max_replication_lag == 5.0


def test_validation_config_custom():
    """Test validation configuration with custom values"""
    config = ValidationConfig(
        check_slot_coverage=False,
        check_replica_sync=False,
        convergence_timeout=120.0,
        max_replication_lag=10.0
    )
    
    assert config.check_slot_coverage is False
    assert config.check_replica_sync is False
    assert config.convergence_timeout == 120.0
    assert config.max_replication_lag == 10.0


def test_replication_status_all_synced():
    """Test replication status when all replicas synced"""
    status = ReplicationStatus(
        all_replicas_synced=True,
        max_lag=1.5,
        lagging_replicas=[]
    )
    
    assert status.all_replicas_synced is True
    assert status.max_lag == 1.5
    assert len(status.lagging_replicas) == 0


def test_replication_status_with_lag():
    """Test replication status with lagging replicas"""
    status = ReplicationStatus(
        all_replicas_synced=False,
        max_lag=10.0,
        lagging_replicas=["node-1", "node-2"]
    )
    
    assert status.all_replicas_synced is False
    assert status.max_lag == 10.0
    assert len(status.lagging_replicas) == 2


def test_connectivity_status_all_connected():
    """Test connectivity status when all nodes connected"""
    status = ConnectivityStatus(
        all_nodes_connected=True,
        disconnected_nodes=[],
        partition_groups=[]
    )
    
    assert status.all_nodes_connected is True
    assert len(status.disconnected_nodes) == 0
    assert len(status.partition_groups) == 0


def test_connectivity_status_with_partitions():
    """Test connectivity status with network partitions"""
    status = ConnectivityStatus(
        all_nodes_connected=False,
        disconnected_nodes=["node-3"],
        partition_groups=[["node-1", "node-2"], ["node-3"]]
    )
    
    assert status.all_nodes_connected is False
    assert len(status.disconnected_nodes) == 1
    assert len(status.partition_groups) == 2


def test_consistency_status_consistent():
    """Test consistency status when data is consistent"""
    status = ConsistencyStatus(
        consistent=True,
        inconsistent_keys=[],
        node_data_mismatches={}
    )
    
    assert status.consistent is True
    assert len(status.inconsistent_keys) == 0
    assert len(status.node_data_mismatches) == 0


def test_consistency_status_inconsistent():
    """Test consistency status when data is inconsistent"""
    status = ConsistencyStatus(
        consistent=False,
        inconsistent_keys=["key1", "key2"],
        node_data_mismatches={"node-1": ["key1"], "node-2": ["key2"]}
    )
    
    assert status.consistent is False
    assert len(status.inconsistent_keys) == 2
    assert len(status.node_data_mismatches) == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
