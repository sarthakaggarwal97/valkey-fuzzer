"""
Tests for Cluster Coordinator
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from src.fuzzer_engine.cluster_coordinator import ClusterCoordinator
from src.models import ClusterConfig, ClusterInstance, NodeInfo


def test_cluster_coordinator_initialization():
    """Test cluster coordinator initialization"""
    coordinator = ClusterCoordinator()
    
    assert coordinator.active_clusters == {}
    assert coordinator.port_manager is not None
    assert coordinator.cluster_manager is not None


def test_get_cluster_status_not_found():
    """Test getting status for non-existent cluster"""
    coordinator = ClusterCoordinator()
    
    status = coordinator.get_cluster_status("non-existent")
    assert status is None


def test_validate_cluster_readiness_not_found():
    """Test validating readiness for non-existent cluster"""
    coordinator = ClusterCoordinator()
    
    is_ready = coordinator.validate_cluster_readiness("non-existent")
    assert is_ready is False


def test_get_node_info_cluster_not_found():
    """Test getting node info when cluster doesn't exist"""
    coordinator = ClusterCoordinator()
    
    node_info = coordinator.get_node_info("non-existent", "node-0")
    assert node_info is None


def test_get_all_nodes_cluster_not_found():
    """Test getting all nodes when cluster doesn't exist"""
    coordinator = ClusterCoordinator()
    
    nodes = coordinator.get_all_nodes("non-existent")
    assert nodes == []


def test_destroy_cluster_not_found():
    """Test destroying non-existent cluster"""
    coordinator = ClusterCoordinator()
    
    result = coordinator.destroy_cluster("non-existent")
    assert result is False


def test_restart_node_cluster_not_found():
    """Test restarting node when cluster doesn't exist"""
    coordinator = ClusterCoordinator()
    
    result = coordinator.restart_node("non-existent", "node-0")
    assert result is False


@patch('src.fuzzer_engine.cluster_coordinator.ConfigurationManager')
@patch('src.fuzzer_engine.cluster_coordinator.ClusterManager')
def test_create_cluster_success(mock_cluster_manager_class, mock_config_manager_class):
    """Test successful cluster creation"""
    # Setup mocks
    mock_config_manager = Mock()
    mock_config_manager.setup_valkey_from_source.return_value = "/usr/bin/valkey-server"
    mock_config_manager.plan_topology.return_value = []
    
    mock_nodes = [
        NodeInfo(
            node_id="node-0",
            role="primary",
            shard_id=0,
            port=7000,
            bus_port=17000,
            pid=12345,
            process=Mock(),
            data_dir="/tmp/test",
            log_file="/tmp/test.log",
            slot_start=0,
            slot_end=5461
        )
    ]
    mock_config_manager.spawn_all_nodes.return_value = mock_nodes
    mock_config_manager_class.return_value = mock_config_manager
    
    mock_cluster_manager = Mock()
    mock_cluster_connection = Mock()
    mock_cluster_connection.cluster_id = "test-cluster"
    mock_cluster_manager.form_cluster.return_value = mock_cluster_connection
    mock_cluster_manager_class.return_value = mock_cluster_manager
    
    # Create coordinator and cluster
    coordinator = ClusterCoordinator()
    coordinator.cluster_manager = mock_cluster_manager
    
    config = ClusterConfig(
        num_shards=1,
        replicas_per_shard=0,
        base_port=7000
    )
    
    cluster_instance = coordinator.create_cluster(config)
    
    assert cluster_instance is not None
    assert cluster_instance.cluster_id == "test-cluster"
    assert cluster_instance.is_ready is True
    assert len(cluster_instance.nodes) == 1


def test_get_node_info_success():
    """Test getting node info successfully"""
    coordinator = ClusterCoordinator()
    
    # Create mock cluster instance
    mock_node = NodeInfo(
        node_id="node-0",
        role="primary",
        shard_id=0,
        port=7000,
        bus_port=17000,
        pid=12345,
        process=Mock(),
        data_dir="/tmp/test",
        log_file="/tmp/test.log"
    )
    
    cluster_instance = ClusterInstance(
        cluster_id="test-cluster",
        config=ClusterConfig(num_shards=1, replicas_per_shard=0),
        nodes=[mock_node],
        creation_time=0.0,
        is_ready=True
    )
    
    coordinator.active_clusters["test-cluster"] = {
        'instance': cluster_instance,
        'config_manager': Mock()
    }
    
    node_info = coordinator.get_node_info("test-cluster", "node-0")
    assert node_info is not None
    assert node_info.node_id == "node-0"


def test_get_all_nodes_success():
    """Test getting all nodes successfully"""
    coordinator = ClusterCoordinator()
    
    # Create mock cluster instance
    mock_nodes = [
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
    
    cluster_instance = ClusterInstance(
        cluster_id="test-cluster",
        config=ClusterConfig(num_shards=1, replicas_per_shard=1),
        nodes=mock_nodes,
        creation_time=0.0,
        is_ready=True
    )
    
    coordinator.active_clusters["test-cluster"] = {
        'instance': cluster_instance,
        'config_manager': Mock()
    }
    
    nodes = coordinator.get_all_nodes("test-cluster")
    assert len(nodes) == 2
    assert nodes[0].node_id == "node-0"
    assert nodes[1].node_id == "node-1"


def test_cleanup_all_clusters():
    """Test cleaning up all clusters"""
    coordinator = ClusterCoordinator()
    
    # Create mock clusters
    for i in range(3):
        cluster_id = f"cluster-{i}"
        cluster_instance = ClusterInstance(
            cluster_id=cluster_id,
            config=ClusterConfig(num_shards=1, replicas_per_shard=0),
            nodes=[],
            creation_time=0.0,
            is_ready=True
        )
        
        mock_config_manager = Mock()
        coordinator.active_clusters[cluster_id] = {
            'instance': cluster_instance,
            'config_manager': mock_config_manager
        }
    
    assert len(coordinator.active_clusters) == 3
    
    coordinator.cleanup_all_clusters()
    
    assert len(coordinator.active_clusters) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
