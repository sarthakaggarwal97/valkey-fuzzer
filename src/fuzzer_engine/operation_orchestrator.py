"""
Operation Orchestrator - Executes cluster operations with timing and state management
"""
import time
import logging
import valkey
from typing import Dict, Optional
from ..models import Operation, OperationType, ClusterStatus, NodeInfo, ClusterConnection
from ..interfaces import IOperationOrchestrator
from ..cluster_orchestrator.orchestrator import ClusterManager

logging.basicConfig(level=logging.INFO)


class OperationOrchestrator(IOperationOrchestrator):
    """Orchestrates execution of cluster operations"""
    
    def __init__(self, cluster_connection: Optional[ClusterConnection] = None):
        """
        Initialize operation orchestrator
        """
        self.cluster_manager = ClusterManager()
        self.cluster_connection = cluster_connection
        self.active_operations: Dict[str, Operation] = {}
        self.operation_counter = 0
    
    def set_cluster_connection(self, cluster_connection: ClusterConnection):
        """Set or update cluster connection"""
        self.cluster_connection = cluster_connection
    
    def execute_operation(self, operation: Operation, cluster_id: str) -> bool:
        """
        Execute a single cluster operation
        """
        if not self.cluster_connection:
            logging.error("No cluster connection available")
            return False
        
        # Generate operation ID
        self.operation_counter += 1
        operation_id = f"op-{self.operation_counter}"
        self.active_operations[operation_id] = operation
        
        try:
            # Wait before operation if specified
            if operation.timing.delay_before > 0:
                logging.info(f"Waiting {operation.timing.delay_before:.2f}s before operation")
                time.sleep(operation.timing.delay_before)
            
            # Execute based on operation type
            success = False
            if operation.type == OperationType.FAILOVER:
                success = self._execute_failover(operation)
            else:
                logging.error(f"Unsupported operation type: {operation.type}")
                return False
            
            # Wait after operation if specified
            if operation.timing.delay_after > 0:
                logging.info(f"Waiting {operation.timing.delay_after:.2f}s after operation")
                time.sleep(operation.timing.delay_after)
            
            # Remove from active operations
            del self.active_operations[operation_id]
            
            return success
            
        except Exception as e:
            logging.error(f"Operation execution failed: {e}")
            if operation_id in self.active_operations:
                del self.active_operations[operation_id]
            return False
    
    def _execute_failover(self, operation: Operation) -> bool:
        """
        Execute failover operation
        """
        logging.info(f"Executing failover on {operation.target_node}")
        
        # Get only live cluster nodes for operation execution
        current_nodes = self.cluster_connection.get_live_nodes()
        
        # Find target node using exact matching
        target_node = None
        
        # Strategy 1: Try exact node_id match
        for node in current_nodes:
            if node.get('node_id') == operation.target_node:
                target_node = node
                break
        
        # Strategy 2: Try exact port match
        if not target_node:
            try:
                target_port = int(operation.target_node)
                for node in current_nodes:
                    if node.get('port') == target_port:
                        target_node = node
                        break
            except ValueError:
                pass  # target_node is not a port number
        
        # Strategy 3: Try shard-based matching (e.g., "shard-0-primary")
        if not target_node:
            try:
                if 'shard-' in operation.target_node and '-primary' in operation.target_node:
                    # Extract shard number from "shard-X-primary" format
                    shard_num = int(operation.target_node.split('shard-')[1].split('-primary')[0])
                    # Find primary node with matching shard_id
                    for node in current_nodes:
                        if node['role'] == 'primary' and node.get('shard_id') == shard_num:
                            target_node = node
                            logging.info(f"Matched target to shard {shard_num} primary at port {target_node['port']}")
                            break
            except (ValueError, IndexError):
                pass
        
        if not target_node:
            logging.error(f"Target node {operation.target_node} not found in cluster")
            return False
        
        # Check if target is a primary node
        if target_node['role'] != 'primary':
            logging.error(f"Target node is not a primary: {target_node['role']}")
            return False
        
        # Get replicas of this primary to execute failover
        # Use cluster_connection to find replicas from any live node (resilient to dead primary)
        target_node_id = target_node['node_id']
        target_shard_id = target_node.get('shard_id')
        
        logging.info(f"Finding replicas for primary {operation.target_node} (node_id: {target_node_id}, shard: {target_shard_id})")
        
        try:
            # Get fresh cluster topology from any live node
            current_nodes = self.cluster_connection.get_current_nodes()
            
            if not current_nodes:
                logging.error("Cannot get current cluster nodes - all nodes may be down")
                return False
            
            # Find replicas of the target primary by shard_id or by querying a live node
            replica_nodes = []
            
            # Strategy 1: Find replicas by shard_id (if available)
            if target_shard_id is not None:
                for node in current_nodes:
                    if node.get('role') == 'replica' and node.get('shard_id') == target_shard_id:
                        replica_nodes.append({
                            'host': node['host'],
                            'port': node['port'],
                            'node_id': node['node_id']
                        })
                        logging.info(f"Found replica by shard_id: {node['node_id']} at port {node['port']}")
            
            # Strategy 2: Query a live node for cluster topology
            # Try the target primary first for determinism, then fall back to other nodes
            if not replica_nodes:
                logging.info("Querying live nodes for replica information")
                
                # Build query order: target primary first, then other nodes
                nodes_to_query = []
                
                # Add target primary first (if it's in current_nodes)
                for node in current_nodes:
                    if node.get('node_id') == target_node_id or node.get('port') == target_node.get('port'):
                        nodes_to_query.append(node)
                        break
                
                # Add remaining nodes as fallback
                for node in current_nodes:
                    if node not in nodes_to_query:
                        nodes_to_query.append(node)
                
                # Query nodes in priority order
                for node in nodes_to_query:
                    try:
                        client = valkey.Valkey(
                            host=node['host'],
                            port=node['port'],
                            socket_timeout=3,
                            decode_responses=True
                        )
                        
                        # Get cluster nodes to find replicas
                        cluster_nodes_raw = client.execute_command('CLUSTER', 'NODES')
                        
                        # Parse to find replicas of our target primary
                        for line in cluster_nodes_raw.split('\n'):
                            if not line.strip():
                                continue
                            parts = line.split()
                            if len(parts) >= 4:
                                # Check if this is a replica of our target primary
                                if 'slave' in parts[2] and parts[3] == target_node_id:
                                    host_port = parts[1].split('@')[0].split(':')
                                    replica_nodes.append({
                                        'host': host_port[0],
                                        'port': int(host_port[1]),
                                        'node_id': parts[0]
                                    })
                                    logging.info(f"Found replica via CLUSTER NODES from {node['port']}: port {host_port[1]}")
                        
                        client.close()
                        
                        # If we found replicas, break out of the loop
                        if replica_nodes:
                            break
                            
                    except Exception as e:
                        logging.debug(f"Could not query node {node.get('node_id', 'unknown')} at port {node.get('port')}: {e}")
                        continue
            
            if not replica_nodes:
                logging.error(f"Cannot execute failover: No replicas found for primary {operation.target_node}. "
                             f"Failover requires at least one replica to promote.")
                return False
            
            # Execute failover from first replica
            replica = replica_nodes[0]
            logging.info(f"Executing failover from replica at port {replica['port']}")
            
            replica_client = valkey.Valkey(
                host=replica['host'],
                port=replica['port'],
                socket_timeout=5,
                decode_responses=True
            )
            
            # Execute CLUSTER FAILOVER command
            force = operation.parameters.get('force', False)
            if force:
                replica_client.execute_command('CLUSTER', 'FAILOVER', 'FORCE')
                logging.info("Executed FORCE failover")
            else:
                replica_client.execute_command('CLUSTER', 'FAILOVER')
                logging.info("Executed graceful failover")
            
            replica_client.close()
            
            # Wait for failover to complete then validate cluster slots and replication links
            return self.wait_for_operation_completion(operation, self.cluster_connection.cluster_id, operation.timing.timeout)
            
        except Exception as e:
            logging.error(f"Failover execution failed: {e}")
            return False
    

    def validate_operation_preconditions(self, operation: Operation, cluster_status: ClusterStatus) -> bool:
        """
        Validate that operation can be executed
        """
        if not cluster_status.is_healthy:
            logging.warning("Cluster is not healthy")
            return False
        
        if not cluster_status.formation_complete:
            logging.warning("Cluster formation not complete")
            return False
        
        if operation.type == OperationType.FAILOVER:
            # Check if target node exists
            target_found = False
            for node in cluster_status.nodes:
                if operation.target_node == node.node_id:
                    target_found = True
                    # Check if it's a primary node
                    if node.role != 'primary':
                        logging.warning(f"Target node {operation.target_node} is not a primary")
                        return False
                    break
            
            if not target_found:
                logging.warning(f"Target node {operation.target_node} not found")
                return False
        
        return True
    
    def wait_for_operation_completion(self, operation: Operation, cluster_id: str, timeout: float) -> bool:
        """
        Wait for operation to complete by checking cluster health
        """
        if not self.cluster_connection:
            return False
        
        logging.info(f"Waiting for operation completion (timeout: {timeout:.2f}s)")
        start_time = time.time()
        
        # Get live nodes and validate cluster is healthy
        live_nodes = [n for n in self.cluster_connection.initial_nodes if n.process is None or n.process.poll() is None]
                
        if not self.cluster_manager.validate_cluster(live_nodes, timeout=timeout):
            logging.warning(f"Operation did not complete within {timeout:.2f}s")
            return False
        
        # Validate replication links after cluster is healthy
        max_retries = 3
        for attempt in range(max_retries):
            live_nodes = [n for n in self.cluster_connection.initial_nodes if n.process is None or n.process.poll() is None]
            if self.cluster_manager.check_replication_links(live_nodes):
                elapsed = time.time() - start_time
                logging.info(f"Operation completed successfully in {elapsed:.2f}s")
                return True
            if attempt < max_retries - 1:
                logging.debug(f"Replication link check attempt {attempt + 1} failed, retrying in 3s")
                time.sleep(3)
        
        logging.warning("Replication link check failed after all retries")
        return False
    