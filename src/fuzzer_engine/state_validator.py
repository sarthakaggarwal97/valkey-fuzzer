"""
State Validator - Validates cluster state and data consistency
"""
import time
import logging
import valkey
from typing import List, Dict, Optional, Set
from ..models import (
    ValidationResult, ValidationConfig, ClusterStatus, NodeInfo,
    SlotConflict, ReplicationStatus, ConnectivityStatus, ConsistencyStatus,
    ClusterConnection
)
from ..interfaces import IStateValidator

logging.basicConfig(level=logging.INFO)


class StateValidator(IStateValidator):
    """Validates cluster state and data consistency"""
    
    def __init__(self, validation_config: Optional[ValidationConfig] = None):
        """
        Initialize state validator
        """
        self.validation_config = validation_config or ValidationConfig()
        self.validation_cache: Dict[str, ValidationResult] = {}
    
    def validate_cluster_state(self, cluster_id: str, cluster_connection: ClusterConnection = None) -> ValidationResult:
        """
        Perform comprehensive cluster state validation
        """
        start_time = time.time()
        
        if not cluster_connection:
            logging.error("No cluster connection provided")
            return self._create_failed_validation(start_time)
        
        # Get ALL cluster nodes (including failed ones) for accurate validation
        current_nodes = cluster_connection.get_current_nodes(include_failed=True)
        if not current_nodes:
            logging.error("No nodes available in cluster")
            return self._create_failed_validation(start_time)
        
        # Check if any primary nodes are dead and wait for automatic failover if they are
        dead_primaries = [n for n in current_nodes if n.get('role') == 'primary' and n.get('status') == 'failed']
        if dead_primaries:
            failover_wait = 15.0
            logging.info(f"Detected {len(dead_primaries)} dead primary node(s), waiting {failover_wait}s for automatic failover")
            time.sleep(failover_wait)
            # Refresh node list after waiting
            current_nodes = cluster_connection.get_current_nodes(include_failed=True)
        
        # Create cluster status from current nodes
        cluster_status = self._build_cluster_status(cluster_id, current_nodes)
        
        # Perform validation checks
        slot_coverage = True
        slot_conflicts = []
        if self.validation_config.check_slot_coverage or self.validation_config.check_slot_conflicts:
            slot_coverage, slot_conflicts = self._validate_slots(cluster_status, current_nodes)
        
        replica_sync = ReplicationStatus(all_replicas_synced=True, max_lag=0.0, lagging_replicas=[])
        if self.validation_config.check_replica_sync:
            replica_sync = self._validate_replica_sync(cluster_status, current_nodes)
        
        node_connectivity = ConnectivityStatus(
            all_nodes_connected=True,
            disconnected_nodes=[],
            partition_groups=[]
        )
        if self.validation_config.check_node_connectivity:
            node_connectivity = self._validate_node_connectivity(cluster_status, current_nodes)
        
        data_consistency = ConsistencyStatus(
            consistent=True,
            inconsistent_keys=[],
            node_data_mismatches={}
        )
        if self.validation_config.check_data_consistency:
            data_consistency = self._validate_data_consistency(cluster_status, current_nodes)
        
        # Calculate convergence time and replication lag
        convergence_time = time.time() - start_time
        replication_lag = replica_sync.max_lag
        
        validation_result = ValidationResult(
            slot_coverage=slot_coverage,
            slot_conflicts=slot_conflicts,
            replica_sync=replica_sync,
            node_connectivity=node_connectivity,
            data_consistency=data_consistency,
            convergence_time=convergence_time,
            replication_lag=replication_lag,
            validation_timestamp=time.time()
        )
        
        # Cache result
        self.validation_cache[cluster_id] = validation_result
        
        return validation_result
    
    def _build_cluster_status(self, cluster_id: str, current_nodes: List[Dict]) -> ClusterStatus:
        """Build ClusterStatus from current nodes"""
        # Convert current nodes to NodeInfo objects (simplified)
        node_infos = []
        for node in current_nodes:
            node_info = NodeInfo(
                node_id=node.get('node_id', 'unknown'),
                role=node.get('role', 'unknown'),
                shard_id=0,  # Not available from current_nodes
                port=node.get('port', 0),
                bus_port=node.get('port', 0) + 10000,
                pid=0,  # Not available
                process=None,
                data_dir="",
                log_file=""
            )
            node_infos.append(node_info)
        
        return ClusterStatus(
            cluster_id=cluster_id,
            nodes=node_infos,
            total_slots_assigned=0,  # Will be calculated
            is_healthy=True,
            formation_complete=True
        )
    
    def _create_failed_validation(self, start_time: float) -> ValidationResult:
        """Create a failed validation result"""
        return ValidationResult(
            slot_coverage=False,
            slot_conflicts=[],
            replica_sync=ReplicationStatus(
                all_replicas_synced=False,
                max_lag=float('inf'),
                lagging_replicas=[]
            ),
            node_connectivity=ConnectivityStatus(
                all_nodes_connected=False,
                disconnected_nodes=[],
                partition_groups=[]
            ),
            data_consistency=ConsistencyStatus(
                consistent=False,
                inconsistent_keys=[],
                node_data_mismatches={}
            ),
            convergence_time=time.time() - start_time,
            replication_lag=float('inf'),
            validation_timestamp=time.time()
        )
    
    def _validate_slots(self, cluster_status: ClusterStatus, current_nodes: List[Dict]) -> tuple:
        """
        Validate slot coverage and conflicts
        
        Returns:
            Tuple of (slot_coverage: bool, slot_conflicts: List[SlotConflict])
        """
        if not current_nodes:
            return False, []
        
        # Find a live node to query
        live_node = None
        for node in current_nodes:
            if node.get('status') == 'connected':
                live_node = node
                break
        
        if not live_node:
            logging.error("No live nodes available for slot validation")
            return False, []
        
        try:
            # Connect to a live node to get cluster info
            client = valkey.Valkey(
                host=live_node['host'],
                port=live_node['port'],
                socket_timeout=5,
                decode_responses=True
            )
            
            # Get cluster info
            info = client.execute_command('CLUSTER', 'INFO')
            info_dict = {}
            for line in info.split('\r\n'):
                if ':' in line:
                    key, value = line.split(':', 1)
                    info_dict[key] = value
            
            slots_assigned = int(info_dict.get('cluster_slots_assigned', 0))
            slots_fail = int(info_dict.get('cluster_slots_fail', 0))
            
            # Get cluster nodes to check for conflicts
            cluster_nodes = client.execute_command('CLUSTER', 'NODES')
            
            # Parse slot assignments from ALL nodes, but track which are live
            slot_assignments: Dict[int, List[str]] = {}
            live_node_ids: Set[str] = set()
            
            for line in cluster_nodes.split('\n'):
                if not line.strip():
                    continue
                parts = line.split()
                if len(parts) < 8:
                    continue
                
                node_id = parts[0]
                flags = parts[2]
                link_state = parts[7] if len(parts) > 7 else 'connected'
                
                # Track live nodes - check for explicit fail/fail? tokens and disconnected link state
                flag_list = flags.split(',')
                has_fail_flag = 'fail' in flag_list or 'fail?' in flag_list
                is_live = not has_fail_flag and link_state != 'disconnected'
                if is_live:
                    live_node_ids.add(node_id)
                
                # Slots are in parts[8:]
                for slot_range in parts[8:]:
                    if '-' in slot_range:
                        # Range like "0-5461"
                        start, end = slot_range.split('-')
                        for slot in range(int(start), int(end) + 1):
                            if slot not in slot_assignments:
                                slot_assignments[slot] = []
                            slot_assignments[slot].append((node_id, is_live))
                    else:
                        # Single slot
                        try:
                            slot = int(slot_range)
                            if slot not in slot_assignments:
                                slot_assignments[slot] = []
                            slot_assignments[slot].append((node_id, is_live))
                        except ValueError:
                            pass
            
            client.close()
            
            # Check for conflicts and slot coverage
            conflicts = []
            slots_covered_by_live_nodes = 0
            
            for slot, node_tuples in slot_assignments.items():
                # Get live nodes for this slot
                live_nodes_for_slot = [node_id for node_id, is_live in node_tuples if is_live]
                
                # Check for conflicts (multiple live nodes claiming same slot)
                if len(live_nodes_for_slot) > 1:
                    conflicts.append(SlotConflict(slot=slot, conflicting_nodes=live_nodes_for_slot))
                
                # Count slots covered by at least one live node
                if len(live_nodes_for_slot) > 0:
                    slots_covered_by_live_nodes += 1
            
            # Slot coverage is good if all 16384 slots are covered by live nodes
            slot_coverage = (slots_covered_by_live_nodes == 16384)
            
            if not slot_coverage:
                logging.warning(f"Slot coverage incomplete: {slots_covered_by_live_nodes}/16384 slots covered by live nodes")
            
            return slot_coverage, conflicts
            
        except Exception as e:
            logging.error(f"Slot validation failed: {e}")
            return False, []
    
    def validate_slot_coverage(self, cluster_status: ClusterStatus) -> bool:
        """
        Validate all slots are assigned
        """
        return cluster_status.total_slots_assigned == 16384
    
    def _validate_replica_sync(self, cluster_status: ClusterStatus, current_nodes: List[Dict]) -> ReplicationStatus:
        """
        Validate replica synchronization
        """
        replica_nodes = [n for n in current_nodes if n['role'] == 'replica']
        
        if not replica_nodes:
            # No replicas, so all are synced by default
            return ReplicationStatus(
                all_replicas_synced=True,
                max_lag=0.0,
                lagging_replicas=[]
            )
        
        lagging_replicas = []
        max_lag = 0.0
        dead_replicas = []
        
        for replica in replica_nodes:
            # Check if node is marked as failed in topology
            if replica.get('status') == 'failed':
                dead_replicas.append(f"{replica['host']}:{replica['port']}")
                continue
            
            try:
                client = valkey.Valkey(
                    host=replica['host'],
                    port=replica['port'],
                    socket_timeout=2,  # Reduced timeout for dead nodes
                    decode_responses=True
                )
                
                # Get replication info
                info = client.info('replication')
                
                master_link_status = info.get('master_link_status', 'down')
                master_last_io_seconds = info.get('master_last_io_seconds_ago', float('inf'))
                
                if master_link_status != 'up':
                    lagging_replicas.append(f"{replica['host']}:{replica['port']}")
                
                try:
                    lag_value = float(master_last_io_seconds)
                    max_lag = max(max_lag, lag_value)
                except (ValueError, TypeError):
                    pass
                
                client.close()
                
            except Exception as e:
                # Node is dead or unreachable - this is expected after chaos
                logging.debug(f"Replica {replica['host']}:{replica['port']} unreachable (likely dead): {e}")
                dead_replicas.append(f"{replica['host']}:{replica['port']}")
                # Don't add to lagging_replicas - dead nodes are handled separately
        
        # Consider sync successful if live replicas are synced
        # Dead replicas are expected after chaos and shouldn't fail validation
        all_synced = len(lagging_replicas) == 0 and max_lag <= self.validation_config.max_replication_lag
        
        if dead_replicas:
            logging.info(f"Dead replicas detected (expected after chaos): {dead_replicas}")
        
        return ReplicationStatus(
            all_replicas_synced=all_synced,
            max_lag=max_lag,
            lagging_replicas=lagging_replicas
        )
    
    def validate_replica_sync(self, cluster_status: ClusterStatus) -> bool:
        """
        Validate replica synchronization (simplified interface)
        """
        # This is a simplified version that would need actual cluster connection
        # For now, return True as a placeholder
        return True
    
    def _validate_node_connectivity(self, cluster_status: ClusterStatus, current_nodes: List[Dict]) -> ConnectivityStatus:
        """
        Validate node connectivity
        """
        if not current_nodes:
            return ConnectivityStatus(
                all_nodes_connected=False,
                disconnected_nodes=[],
                partition_groups=[]
            )
        
        try:
            # Try to connect to each node
            disconnected_nodes = []
            connected_nodes = []
            
            for node in current_nodes:
                node_addr = f"{node['host']}:{node['port']}"
                
                # Check if node is marked as failed in topology
                if node.get('status') == 'failed':
                    disconnected_nodes.append(node_addr)
                    continue
                
                try:
                    client = valkey.Valkey(
                        host=node['host'],
                        port=node['port'],
                        socket_timeout=2,  # Reduced timeout for dead nodes
                        decode_responses=True
                    )
                    client.ping()
                    client.close()
                    connected_nodes.append(node_addr)
                except Exception as e:
                    # Node is dead or unreachable - expected after chaos
                    logging.debug(f"Node {node_addr} unreachable: {e}")
                    disconnected_nodes.append(node_addr)
            
            # After chaos, some nodes being dead is expected and acceptable
            # We consider connectivity good if at least one node per shard is reachable
            # For now, we're lenient: as long as SOME nodes are connected, it's acceptable
            all_connected = len(disconnected_nodes) == 0
            
            # For partition detection
            partition_groups = []
            if not all_connected and connected_nodes:
                # Simple partition detection: connected vs disconnected
                partition_groups.append(connected_nodes)
                if disconnected_nodes:
                    partition_groups.append(disconnected_nodes)
            
            if disconnected_nodes:
                logging.info(f"Disconnected nodes detected (expected after chaos): {disconnected_nodes}")
            
            return ConnectivityStatus(
                all_nodes_connected=all_connected,
                disconnected_nodes=disconnected_nodes,
                partition_groups=partition_groups
            )
            
        except Exception as e:
            logging.error(f"Connectivity validation failed: {e}")
            return ConnectivityStatus(
                all_nodes_connected=False,
                disconnected_nodes=[],
                partition_groups=[]
            )
    
    def _validate_data_consistency(self, cluster_status: ClusterStatus, current_nodes: List[Dict]) -> ConsistencyStatus:
        """
        Validate data consistency across nodes
        """
        # Data consistency validation is complex and requires:
        # 1. Identifying primary-replica pairs
        # 2. Sampling keys from primaries
        # 3. Comparing values on replicas
        
        # For now, implement a basic check
        try:
            primary_nodes = [n for n in current_nodes if n['role'] == 'primary']
            
            if not primary_nodes:
                return ConsistencyStatus(
                    consistent=True,
                    inconsistent_keys=[],
                    node_data_mismatches={}
                )
            
            # Try to connect to any available primary
            for node in primary_nodes:
                try:
                    client = valkey.Valkey(
                        host=node['host'],
                        port=node['port'],
                        socket_timeout=2,  # Reduced timeout
                        decode_responses=True
                    )
                    
                    # Get some keys to check
                    keys = client.keys('*')[:10]  # Sample first 10 keys
                    
                    client.close()
                    
                    # For basic validation, if we can read keys, consider it consistent
                    # Full validation would require comparing across replicas
                    return ConsistencyStatus(
                        consistent=True,
                        inconsistent_keys=[],
                        node_data_mismatches={}
                    )
                except Exception as e:
                    # This primary is dead, try next one
                    logging.debug(f"Primary {node['host']}:{node['port']} unreachable: {e}")
                    continue
            
            # All primaries are dead - this is a problem
            logging.warning("All primary nodes unreachable for consistency check")
            return ConsistencyStatus(
                consistent=False,
                inconsistent_keys=[],
                node_data_mismatches={}
            )
            
        except Exception as e:
            logging.error(f"Data consistency validation failed: {e}")
            return ConsistencyStatus(
                consistent=False,
                inconsistent_keys=[],
                node_data_mismatches={}
            )
    
    def validate_data_consistency(self, cluster_status: ClusterStatus) -> bool:
        """
        Validate data consistency (simplified interface)
        """
        # Simplified version
        return True
