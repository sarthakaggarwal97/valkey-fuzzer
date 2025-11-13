"""
Tests for State Validator
"""
import pytest
import time
from src.fuzzer_engine.state_validator import StateValidator
from src.models import (
    ValidationConfig, StateValidationConfig, ClusterStatus, NodeInfo, ReplicationStatus,
    ConnectivityStatus, ConsistencyStatus
)


def test_state_validator_initialization():
    """Test state validator initialization"""
    config = StateValidationConfig()
    validator = StateValidator(config)
    
    assert validator.config is not None
    assert validator.config.check_slot_coverage is True
    assert validator.replication_validator is not None
    assert validator.cluster_status_validator is not None
    assert validator.slot_validator is not None


def test_state_validator_with_custom_config():
    """Test state validator with custom configuration"""
    config = StateValidationConfig(
        check_slot_coverage=True,
        check_replication=True,
        validation_timeout=30.0
    )
    
    validator = StateValidator(config)
    
    assert validator.config == config
    assert validator.config.validation_timeout == 30.0


def test_state_validator_has_sub_validators():
    """Test that StateValidator initializes all sub-validators"""
    config = StateValidationConfig()
    validator = StateValidator(config)
    
    # Verify all sub-validators are initialized
    assert validator.replication_validator is not None
    assert validator.cluster_status_validator is not None
    assert validator.slot_validator is not None
    assert validator.topology_validator is not None
    assert validator.view_consistency_validator is not None


def test_state_validation_config_defaults():
    """Test StateValidationConfig default values"""
    config = StateValidationConfig()
    
    # Check that all checks are enabled by default
    assert config.check_replication is True
    assert config.check_cluster_status is True
    assert config.check_slot_coverage is True
    assert config.check_topology is True
    assert config.check_view_consistency is True
    
    # Check default timing values
    assert config.stabilization_wait == 5.0  # Increased for failover convergence
    assert config.validation_timeout == 30.0
    
    # Check default behavior
    assert config.blocking_on_failure is False
    assert config.retry_on_transient_failure is True
    assert config.max_retries == 3


def test_state_validation_config_custom():
    """Test StateValidationConfig with custom values"""
    config = StateValidationConfig(
        check_replication=False,
        check_slot_coverage=True,
        stabilization_wait=5.0,
        max_retries=5,
        blocking_on_failure=True
    )
    
    assert config.check_replication is False
    assert config.check_slot_coverage is True
    assert config.stabilization_wait == 5.0
    assert config.max_retries == 5
    assert config.blocking_on_failure is True


def test_state_validation_config_selective_checks():
    """Test StateValidationConfig with selective checks enabled"""
    config = StateValidationConfig(
        check_replication=True,
        check_cluster_status=False,
        check_slot_coverage=True,
        check_topology=False,
        check_view_consistency=False
    )
    
    # Only replication and slot coverage should be enabled
    assert config.check_replication is True
    assert config.check_cluster_status is False
    assert config.check_slot_coverage is True
    assert config.check_topology is False
    assert config.check_view_consistency is False


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


def test_replication_validation_config_defaults():
    """Test ReplicationValidationConfig default values"""
    from src.models import ReplicationValidationConfig
    
    config = ReplicationValidationConfig()
    
    assert config.max_acceptable_lag == 5.0
    assert config.require_all_replicas_synced is False
    assert config.check_replication_offset is True
    assert config.timeout == 10.0


def test_replication_validation_config_strict_mode():
    """Test ReplicationValidationConfig in strict mode"""
    from src.models import ReplicationValidationConfig
    
    config = ReplicationValidationConfig(
        max_acceptable_lag=2.0,
        require_all_replicas_synced=True,
        timeout=5.0
    )
    
    assert config.max_acceptable_lag == 2.0
    assert config.require_all_replicas_synced is True
    assert config.timeout == 5.0


def test_replication_validation_result_structure():
    """Test ReplicationValidation result structure"""
    from src.models import ReplicationValidation, ReplicaLagInfo
    
    lag_info = ReplicaLagInfo(
        replica_node_id="replica-1",
        replica_address="127.0.0.1:7001",
        primary_node_id="primary-1",
        primary_address="127.0.0.1:7000",
        lag_seconds=6.5,
        replication_offset_diff=1000,
        link_status="up"
    )
    
    result = ReplicationValidation(
        success=False,  # Should be False when lagging replicas detected
        all_replicas_synced=False,
        max_lag=6.5,
        lagging_replicas=[lag_info],
        disconnected_replicas=[],
        error_message="1 replica(s) lagging beyond acceptable threshold"
    )
    
    assert result.success is False
    assert result.all_replicas_synced is False
    assert result.max_lag == 6.5
    assert len(result.lagging_replicas) == 1
    assert len(result.disconnected_replicas) == 0
    assert "lagging" in result.error_message


def test_replication_validation_unreachable_cluster():
    """Test ReplicationValidation when cluster is unreachable"""
    from src.models import ReplicationValidation
    
    # Simulate result when cluster cannot be contacted
    result = ReplicationValidation(
        success=False,
        all_replicas_synced=False,
        max_lag=-1.0,
        lagging_replicas=[],
        disconnected_replicas=[],
        error_message="Unable to contact cluster - no nodes reachable"
    )
    
    assert result.success is False
    assert result.all_replicas_synced is False
    assert result.max_lag == -1.0
    assert len(result.lagging_replicas) == 0
    assert len(result.disconnected_replicas) == 0
    assert "Unable to contact cluster" in result.error_message


def test_replication_validation_no_replicas_configured():
    """Test ReplicationValidation when no replicas are configured"""
    from src.models import ReplicationValidation
    
    # Simulate result when cluster has no replicas
    result = ReplicationValidation(
        success=True,
        all_replicas_synced=True,
        max_lag=0.0,
        lagging_replicas=[],
        disconnected_replicas=[],
        error_message=None
    )
    
    assert result.success is True
    assert result.all_replicas_synced is True
    assert result.max_lag == 0.0
    assert len(result.lagging_replicas) == 0
    assert len(result.disconnected_replicas) == 0
    assert result.error_message is None


def test_state_validation_result_with_exception():
    """Test StateValidationResult when unexpected exception occurs"""
    from src.models import StateValidationResult
    
    # Simulate result when catch-all exception handler is triggered
    result = StateValidationResult(
        overall_success=False,  # Should be False when exception occurs
        validation_timestamp=time.time(),
        validation_duration=1.0,
        replication=None,
        cluster_status=None,
        slot_coverage=None,
        topology=None,
        view_consistency=None,
        data_consistency=None,  # Added for new validator
        failed_checks=["validation_framework"],  # Should include framework failure
        error_messages=["Unexpected validation error: Test exception"]
    )
    
    assert result.overall_success is False
    assert "validation_framework" in result.failed_checks
    assert len(result.error_messages) == 1
    assert "Unexpected validation error" in result.error_messages[0]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


def test_killed_nodes_tracking():
    """Test that StateValidator tracks killed nodes correctly"""
    config = StateValidationConfig()
    validator = StateValidator(config)
    
    # Initially no killed nodes
    assert len(validator.killed_nodes) == 0
    
    # Register killed nodes
    validator.register_killed_node("127.0.0.1:6379")
    validator.register_killed_node("127.0.0.1:6380")
    
    assert len(validator.killed_nodes) == 2
    assert "127.0.0.1:6379" in validator.killed_nodes
    assert "127.0.0.1:6380" in validator.killed_nodes
    
    # Clear killed nodes
    validator.clear_killed_nodes()
    assert len(validator.killed_nodes) == 0


def test_replication_validator_accepts_killed_nodes():
    """Test that ReplicationValidator accepts killed_nodes parameter"""
    from src.fuzzer_engine.state_validator import ReplicationValidator
    from src.models import ReplicationValidationConfig
    
    config = ReplicationValidationConfig()
    validator = ReplicationValidator()
    
    # Verify the validate method accepts killed_nodes parameter
    import inspect
    sig = inspect.signature(validator.validate)
    params = list(sig.parameters.keys())
    
    assert 'killed_nodes' in params, "ReplicationValidator.validate should accept killed_nodes parameter"
