"""
Operation Orchestrator - Executes cluster operations with timing and state management
"""
import time
import logging
import valkey
from typing import Dict, Optional
from ..models import (
    Operation, OperationType, ClusterStatus, NodeInfo, ClusterConnection
)
from ..interfaces import IOperationOrchestrator

logging.basicConfig(level=logging.INFO)


class OperationOrchestrator(IOperationOrchestrator):
    """Orchestrates execution of cluster operations"""
    
    def __init__(self, cluster_connection: Optional[ClusterConnection] = None):
        """
        Initialize operation orchestrator
        """
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
        
        # Get current cluster nodes
        current_nodes = self.cluster_connection.get_current_nodes()
        
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
        
        # Get a replica of this primary to execute failover
        # First, we need to find replicas by checking cluster topology
        try:
            client = valkey.Valkey(
                host=target_node['host'],
                port=target_node['port'],
                socket_timeout=5,
                decode_responses=True
            )
            
            # Get cluster nodes to find replicas
            cluster_nodes_raw = client.execute_command('CLUSTER', 'NODES')
            target_node_id = target_node['node_id']
            
            # Parse to find replicas of this primary
            replica_nodes = []
            for line in cluster_nodes_raw.split('\n'):
                if not line.strip():
                    continue
                parts = line.split()
                if len(parts) >= 4:
                    # Check if this is a replica of our target primary
                    if 'slave' in parts[2] and len(parts) >= 4 and parts[3] == target_node_id:
                        host_port = parts[1].split('@')[0].split(':')
                        replica_nodes.append({
                            'host': host_port[0],
                            'port': int(host_port[1])
                        })
            
            client.close()
            
            if not replica_nodes:
                logging.warning(f"No replicas found for primary {operation.target_node}")
                # Try to execute failover on the primary itself (manual failover)
                return self._execute_manual_failover(target_node, operation)
            
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
            
            # Wait for failover to complete
            return self.wait_for_operation_completion(operation, self.cluster_connection.cluster_id, operation.timing.timeout)
            
        except Exception as e:
            logging.error(f"Failover execution failed: {e}")
            return False
    
    def _execute_manual_failover(self, target_node: Dict, operation: Operation) -> bool:
        """
        Execute manual failover when no replicas are available
        """
        logging.info(f"Attempting manual failover on primary at port {target_node['port']}")
        
        try:
            client = valkey.Valkey(
                host=target_node['host'],
                port=target_node['port'],
                socket_timeout=5,
                decode_responses=True
            )
            
            # For a primary with no replicas, we can't really do a failover
            # This is more of a graceful handling case
            logging.warning("Cannot failover primary with no replicas")
            client.close()
            return False
            
        except Exception as e:
            logging.error(f"Manual failover failed: {e}")
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
        Wait for operation to complete
        """
        if not self.cluster_connection:
            return False
        
        logging.info(f"Waiting for operation completion (timeout: {timeout:.2f}s)")
        
        start_time = time.time()
        deadline = start_time + timeout
        
        # For failover, we wait for cluster to stabilize
        while time.time() < deadline:
            try:
                # Get current cluster state
                current_nodes = self.cluster_connection.get_current_nodes()
                
                if not current_nodes:
                    time.sleep(1)
                    continue
                
                # Check if cluster has stabilized
                # For failover, we expect the cluster to have all nodes connected
                # and slots properly assigned
                
                # Simple check: verify we can connect to nodes
                any_node = current_nodes[0]
                client = valkey.Valkey(
                    host=any_node['host'],
                    port=any_node['port'],
                    socket_timeout=3,
                    decode_responses=True
                )
                
                # Check cluster state
                info = client.execute_command('CLUSTER', 'INFO')
                info_dict = {}
                for line in info.split('\r\n'):
                    if ':' in line:
                        key, value = line.split(':', 1)
                        info_dict[key] = value
                
                cluster_state = info_dict.get('cluster_state')
                slots_assigned = int(info_dict.get('cluster_slots_assigned', 0))
                
                client.close()
                
                if cluster_state == 'ok' and slots_assigned == 16384:
                    elapsed = time.time() - start_time
                    logging.info(f"Operation completed successfully in {elapsed:.2f}s")
                    return True
                
                time.sleep(1)
                
            except Exception as e:
                logging.debug(f"Waiting for stabilization: {e}")
                time.sleep(1)
        
        logging.warning(f"Operation did not complete within {timeout:.2f}s")
        return False
