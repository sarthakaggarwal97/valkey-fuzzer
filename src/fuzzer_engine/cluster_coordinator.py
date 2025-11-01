"""
Cluster Coordinator - Manages cluster lifecycle and interfaces with Cluster Orchestrator
"""
import logging
import time
from typing import List, Optional
from ..models import (
    ClusterConfig, ClusterInstance, ClusterStatus, NodeInfo, ClusterConnection
)
from ..cluster_orchestrator.orchestrator import (
    ConfigurationManager, ClusterManager, PortManager
)

logging.basicConfig(format='%(levelname)-5s | %(filename)s:%(lineno)-3d | %(message)s', level=logging.INFO, force=True)
logger = logging.getLogger(__name__)


class ClusterCoordinator:
    """
    Coordinates cluster lifecycle management by interfacing with Cluster Orchestrator.
    Provides high-level cluster operations for the Fuzzer Engine.
    """
    
    def __init__(self):
        self.active_clusters: dict[str, ClusterInstance] = {}
        self.port_manager = PortManager()
        self.cluster_manager = ClusterManager()
    
    def create_cluster(self, config: ClusterConfig) -> ClusterInstance:
        """
        Create a new Valkey cluster with the specified configuration.
        
        Args:
            config: Cluster configuration specifying topology and settings
            
        Returns:
            ClusterInstance with cluster details and node information
            
        Raises:
            Exception: If cluster creation or formation fails
        """
        logger.info(f"Creating cluster with {config.num_shards} shards, {config.replicas_per_shard} replicas per shard")
        
        try:
            # Initialize configuration manager
            config_manager = ConfigurationManager(config, self.port_manager)
            
            # Setup Valkey binary
            valkey_binary = config_manager.setup_valkey_from_source()
            config.valkey_binary = valkey_binary
            logger.info(f"Using Valkey binary: {valkey_binary}")
            
            # Plan topology
            node_plans = config_manager.plan_topology()
            
            # Spawn all nodes
            nodes = config_manager.spawn_all_nodes(node_plans)
            
            # Form cluster
            cluster_connection = self.cluster_manager.form_cluster(nodes)
            
            if not cluster_connection:
                raise Exception("Failed to form cluster")
            
            # Create cluster instance
            cluster_instance = ClusterInstance(
                cluster_id=cluster_connection.cluster_id,
                config=config,
                nodes=nodes,
                creation_time=time.time(),
                is_ready=True
            )
            
            # Store cluster instance and config manager for later cleanup
            self.active_clusters[cluster_instance.cluster_id] = {
                'instance': cluster_instance,
                'config_manager': config_manager
            }
            
            logger.info(f"Cluster {cluster_instance.cluster_id} created successfully with {len(nodes)} nodes")
            return cluster_instance
            
        except Exception as e:
            logger.error(f"Failed to create cluster: {e}")
            raise
    
    def get_cluster_status(self, cluster_id: str) -> Optional[ClusterStatus]:
        """
        Get current status of a cluster.
        
        Args:
            cluster_id: Unique identifier for the cluster
            
        Returns:
            ClusterStatus with current cluster state, or None if cluster not found
        """
        if cluster_id not in self.active_clusters:
            logger.warning(f"Cluster {cluster_id} not found")
            return None
        
        cluster_data = self.active_clusters[cluster_id]
        cluster_instance = cluster_data['instance']
        
        try:
            # Validate cluster health
            is_healthy = self.cluster_manager.validate_cluster(cluster_instance.nodes)
            
            # Count assigned slots
            total_slots = sum(
                (node.slot_end - node.slot_start + 1) 
                for node in cluster_instance.nodes 
                if node.role == 'primary' and node.slot_start is not None
            )
            
            status = ClusterStatus(
                cluster_id=cluster_id,
                nodes=cluster_instance.nodes,
                total_slots_assigned=total_slots,
                is_healthy=is_healthy,
                formation_complete=cluster_instance.is_ready
            )
            
            return status
            
        except Exception as e:
            logger.error(f"Failed to get cluster status for {cluster_id}: {e}")
            return None
    
    def validate_cluster_readiness(self, cluster_id: str) -> bool:
        """
        Validate that cluster is ready for testing.
        
        Args:
            cluster_id: Unique identifier for the cluster
            
        Returns:
            True if cluster is ready, False otherwise
        """
        status = self.get_cluster_status(cluster_id)
        
        if not status:
            return False
        
        # Check all required conditions for readiness
        is_ready = (
            status.formation_complete and
            status.is_healthy and
            status.total_slots_assigned == 16384 and
            len(status.nodes) > 0
        )
        
        if is_ready:
            logger.info(f"Cluster {cluster_id} is ready for testing")
        else:
            logger.warning(f"Cluster {cluster_id} is not ready: "
                         f"formation={status.formation_complete}, "
                         f"healthy={status.is_healthy}, "
                         f"slots={status.total_slots_assigned}")
        
        return is_ready
    
    def get_node_info(self, cluster_id: str, node_id: str) -> Optional[NodeInfo]:
        """
        Get information about a specific node in the cluster.
        
        Args:
            cluster_id: Unique identifier for the cluster
            node_id: Identifier for the node
            
        Returns:
            NodeInfo for the specified node, or None if not found
        """
        if cluster_id not in self.active_clusters:
            return None
        
        cluster_instance = self.active_clusters[cluster_id]['instance']
        
        for node in cluster_instance.nodes:
            if node.node_id == node_id:
                return node
        
        return None
    
    def get_all_nodes(self, cluster_id: str) -> List[NodeInfo]:
        """
        Get all nodes in the cluster.
        
        Args:
            cluster_id: Unique identifier for the cluster
            
        Returns:
            List of NodeInfo for all nodes in the cluster
        """
        if cluster_id not in self.active_clusters:
            return []
        
        cluster_instance = self.active_clusters[cluster_id]['instance']
        return cluster_instance.nodes
    
    def restart_node(self, cluster_id: str, node_id: str, wait_ready: bool = True) -> bool:
        """
        Restart a specific node in the cluster.
        
        Args:
            cluster_id: Unique identifier for the cluster
            node_id: Identifier for the node to restart
            wait_ready: Whether to wait for node to be ready after restart
            
        Returns:
            True if restart successful, False otherwise
        """
        if cluster_id not in self.active_clusters:
            logger.error(f"Cluster {cluster_id} not found")
            return False
        
        cluster_data = self.active_clusters[cluster_id]
        config_manager = cluster_data['config_manager']
        
        node = self.get_node_info(cluster_id, node_id)
        if not node:
            logger.error(f"Node {node_id} not found in cluster {cluster_id}")
            return False
        
        try:
            logger.info(f"Restarting node {node_id} in cluster {cluster_id}")
            config_manager.restart_node(node, wait_ready=wait_ready)
            logger.info(f"Node {node_id} restarted successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to restart node {node_id}: {e}")
            return False
    
    def destroy_cluster(self, cluster_id: str) -> bool:
        """
        Destroy a cluster and clean up all resources.
        
        Args:
            cluster_id: Unique identifier for the cluster
            
        Returns:
            True if cleanup successful, False otherwise
        """
        if cluster_id not in self.active_clusters:
            logger.warning(f"Cluster {cluster_id} not found")
            return False
        
        try:
            cluster_data = self.active_clusters[cluster_id]
            cluster_instance = cluster_data['instance']
            config_manager = cluster_data['config_manager']
            
            logger.info(f"Destroying cluster {cluster_id}")
            
            # Close cluster manager connections
            self.cluster_manager.close_connections()
            
            # Cleanup cluster resources
            config_manager.cleanup_cluster(cluster_instance.nodes)
            
            # Remove from active clusters
            del self.active_clusters[cluster_id]
            
            logger.info(f"Cluster {cluster_id} destroyed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to destroy cluster {cluster_id}: {e}")
            return False
    
    def cleanup_all_clusters(self) -> None:
        """Clean up all active clusters."""
        cluster_ids = list(self.active_clusters.keys())
        
        for cluster_id in cluster_ids:
            self.destroy_cluster(cluster_id)
        
        logger.info("All clusters cleaned up")
