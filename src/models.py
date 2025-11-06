"""
Core data models for the Cluster Bus Fuzzer
"""
from dataclasses import dataclass
from typing import Dict, List, Any, Optional
from enum import Enum
import subprocess
import valkey


@dataclass
class ClusterConfig:
    """Configuration for a Valkey cluster"""
    num_shards: int
    replicas_per_shard: int
    base_port: int = 6379
    base_data_dir: str = "/tmp/valkey-fuzzer"
    valkey_binary: str = "/usr/local/bin/valkey-server"
    enable_cleanup: bool = True


@dataclass
class NodePlan:
    """Plan for a node before it's spawned"""
    node_id: str
    role: str 
    shard_id: int
    port: int
    bus_port: int
    slot_start: Optional[int] = None 
    slot_end: Optional[int] = None   
    master_node_id: Optional[str] = None


@dataclass
class NodeInfo:
    """Information about a running cluster node"""
    node_id: str
    role: str
    shard_id: int
    port: int
    bus_port: int
    pid: int
    process: subprocess.Popen
    data_dir: str
    log_file: str
    slot_start: Optional[int] = None
    slot_end: Optional[int] = None
    master_node_id: Optional[str] = None
    cluster_node_id: Optional[str] = None

class OperationType(Enum):
    """Types of operations supported by the fuzzer"""
    FAILOVER = "failover"
    # Future extensions: ADD_REPLICA, REMOVE_REPLICA, RESHARD, SCALE_OUT, SCALE_IN, CONFIG_CHANGE


class ChaosType(Enum):
    """Types of chaos injection supported"""
    PROCESS_KILL = "process_kill"
    # Future extensions: NETWORK_PARTITION, PACKET_DROP, LATENCY_INJECTION


class ProcessChaosType(Enum):
    """Types of process chaos injection"""
    SIGKILL = "sigkill"
    SIGTERM = "sigterm"


@dataclass
class OperationTiming:
    """Timing configuration for operations"""
    delay_before: float = 0.0  # Seconds to wait before operation
    timeout: float = 30.0  # Operation timeout in seconds
    delay_after: float = 0.0  # Seconds to wait after operation


@dataclass
class Operation:
    """Represents a cluster operation to be executed"""
    type: OperationType
    target_node: str
    parameters: Dict[str, Any]
    timing: OperationTiming


@dataclass
class TargetSelection:
    """Configuration for chaos target selection"""
    strategy: str  # "random", "primary_only", "replica_only", "specific"
    specific_nodes: Optional[List[str]] = None


@dataclass
class ChaosTiming:
    """Timing configuration for chaos injection"""
    delay_before_operation: float = 0.0
    delay_after_operation: float = 0.0
    chaos_duration: float = 10.0


@dataclass
class ChaosCoordination:
    """Configuration for chaos coordination with operations"""
    chaos_before_operation: bool = False
    chaos_during_operation: bool = True
    chaos_after_operation: bool = False


@dataclass
class ChaosConfig:
    """Configuration for chaos injection"""
    chaos_type: ChaosType
    target_selection: TargetSelection
    timing: ChaosTiming
    coordination: ChaosCoordination
    process_chaos_type: Optional[ProcessChaosType] = None  # For process chaos


@dataclass
class ValidationConfig:
    """Configuration for cluster state validation"""
    check_slot_coverage: bool = True
    check_slot_conflicts: bool = True
    check_replica_sync: bool = True
    check_node_connectivity: bool = True
    check_data_consistency: bool = True
    convergence_timeout: float = 60.0
    max_replication_lag: float = 5.0


@dataclass
class Scenario:
    """Complete test scenario configuration"""
    scenario_id: str
    cluster_config: ClusterConfig
    operations: List[Operation]
    chaos_config: ChaosConfig
    validation_config: ValidationConfig
    seed: Optional[int] = None  # For reproducibility


@dataclass
class SlotConflict:
    """Represents a slot assignment conflict"""
    slot: int
    conflicting_nodes: List[str]


@dataclass
class ReplicationStatus:
    """Status of replica synchronization"""
    all_replicas_synced: bool
    max_lag: float
    lagging_replicas: List[str]


@dataclass
class ConnectivityStatus:
    """Status of node connectivity"""
    all_nodes_connected: bool
    disconnected_nodes: List[str]
    partition_groups: List[List[str]]


@dataclass
class ConsistencyStatus:
    """Status of data consistency across nodes"""
    consistent: bool
    inconsistent_keys: List[str]
    node_data_mismatches: Dict[str, List[str]]


@dataclass
class ValidationResult:
    """Result of cluster state validation"""
    slot_coverage: bool
    slot_conflicts: List[SlotConflict]
    replica_sync: ReplicationStatus
    node_connectivity: ConnectivityStatus
    data_consistency: ConsistencyStatus
    convergence_time: float
    replication_lag: float
    validation_timestamp: float


@dataclass
class ClusterStatus:
    """Overall cluster status information"""
    cluster_id: str
    nodes: List[NodeInfo]
    total_slots_assigned: int
    is_healthy: bool
    formation_complete: bool


@dataclass
class ClusterInstance:
    """Represents a created cluster instance"""
    cluster_id: str
    config: ClusterConfig
    nodes: List[NodeInfo]
    creation_time: float
    is_ready: bool = False


@dataclass
class WorkloadConfig:
    """Configuration for workload generation"""
    clients: int = 10
    requests_per_client: int = 1000
    data_size: int = 1024
    pipeline: int = 1
    key_pattern: str = "test:key:*"


@dataclass
class WorkloadMetrics:
    """Metrics from workload execution"""
    total_requests: int
    successful_requests: int
    failed_requests: int
    average_latency: float
    max_latency: float
    requests_per_second: float


@dataclass
class WorkloadSession:
    """Active workload session"""
    session_id: str
    config: WorkloadConfig
    start_time: float
    is_active: bool = True


@dataclass
class ChaosResult:
    """Result of chaos injection"""
    chaos_id: str
    chaos_type: ChaosType
    target_node: str
    success: bool
    start_time: float
    end_time: Optional[float] = None
    error_message: Optional[str] = None


@dataclass
class ExecutionResult:
    """Complete test execution result"""
    scenario_id: str
    success: bool
    start_time: float
    end_time: float
    operations_executed: int
    chaos_events: List[ChaosResult]
    validation_results: List[ValidationResult]
    error_message: Optional[str] = None
    seed: Optional[int] = None


@dataclass
class DSLConfig:
    """DSL-based test configuration"""
    config_text: str
    parsed_scenario: Optional[Scenario] = None


@dataclass
class ClusterConnection:
    """Stores cluster connection information after orchestrator creates cluster"""
    initial_nodes: List[NodeInfo]
    cluster_id: str
    
    def __post_init__(self):
        self.startup_nodes = [{'host': '127.0.0.1', 'port': node.port} for node in self.initial_nodes]
    
    def get_current_nodes(self, include_failed: bool = True) -> List[Dict]:
        """
        Get current cluster topology via CLUSTER NODES. List of node dictionaries with keys: node_id, host, port, role, shard_id, status
        """
        # Build a mapping from port to shard_id using initial_nodes
        port_to_shard = {node.port: node.shard_id for node in self.initial_nodes}
        
        for node_info in self.startup_nodes:
            try:
                client = valkey.Valkey(host=node_info['host'], port=node_info['port'], socket_timeout=2)
                cluster_nodes_raw = client.execute_command('CLUSTER', 'NODES')
                client.close()
                
                # First pass: build shard mapping from current topology
                # This handles cases where failovers have changed which nodes are primaries
                shard_to_primary_node_id = {}
                node_id_to_shard = {}
                
                for line in cluster_nodes_raw.decode().strip().split('\n'):
                    if not line.strip():
                        continue
                    parts = line.split()
                    if len(parts) < 8:
                        continue
                    
                    node_id = parts[0]
                    flags = parts[2]
                    host_port = parts[1].split('@')[0].split(':')
                    port = int(host_port[1])
                    
                    # Get initial shard_id from port mapping
                    initial_shard_id = port_to_shard.get(port)
                    
                    # For replicas, get shard from their master
                    if 'slave' in flags and len(parts) >= 4:
                        master_node_id = parts[3]
                        # We'll resolve this in second pass
                        node_id_to_shard[node_id] = ('replica', master_node_id, port, flags)
                    elif 'master' in flags:
                        # Primary nodes: use initial shard_id
                        if initial_shard_id is not None:
                            shard_to_primary_node_id[initial_shard_id] = node_id
                            node_id_to_shard[node_id] = ('primary', initial_shard_id, port, flags)
                
                # Second pass: resolve replica shards and build final node list
                current_nodes = []
                for line in cluster_nodes_raw.decode().strip().split('\n'):
                    if not line.strip():
                        continue
                    parts = line.split()
                    if len(parts) < 8:
                        continue
                    
                    node_id = parts[0]
                    flags = parts[2]
                    link_state = parts[7] if len(parts) > 7 else 'connected'
                    host_port = parts[1].split('@')[0].split(':')
                    port = int(host_port[1])
                    
                    # Determine node status
                    # Check for explicit fail/fail? tokens (not substring match to avoid matching 'nofailover')
                    flag_list = flags.split(',')
                    has_fail_flag = 'fail' in flag_list or 'fail?' in flag_list
                    is_failed = has_fail_flag or link_state == 'disconnected'
                    status = 'failed' if is_failed else 'connected'
                    
                    # Skip failed/disconnected nodes if not requested
                    if not include_failed and is_failed:
                        continue
                    
                    # Determine shard_id and role
                    if node_id in node_id_to_shard:
                        node_info_tuple = node_id_to_shard[node_id]
                        if node_info_tuple[0] == 'primary':
                            role = 'primary'
                            shard_id = node_info_tuple[1]
                        else:  # replica
                            role = 'replica'
                            master_node_id = node_info_tuple[1]
                            # Find shard_id from master
                            shard_id = None
                            if master_node_id in node_id_to_shard:
                                master_info = node_id_to_shard[master_node_id]
                                if master_info[0] == 'primary':
                                    shard_id = master_info[1]
                            # Fallback to port mapping
                            if shard_id is None:
                                shard_id = port_to_shard.get(port)
                    else:
                        # Fallback for nodes not in our mapping
                        role = 'primary' if 'master' in flags else 'replica'
                        shard_id = port_to_shard.get(port)
                    
                    current_nodes.append({
                        'node_id': node_id,
                        'host': host_port[0],
                        'port': port,
                        'role': role,
                        'shard_id': shard_id,
                        'status': status
                    })
                return current_nodes
            except Exception as e:
                continue
        return []
    
    def get_live_nodes(self) -> List[Dict]:
        """Get only live (connected) nodes from the cluster"""
        return self.get_current_nodes(include_failed=False)
    
    def get_primary_nodes(self) -> List[Dict]:
        """Get current primary nodes"""
        return [node for node in self.get_current_nodes() if node['role'] == 'primary']
    
    def get_replica_nodes(self) -> List[Dict]:
        """Get current replica nodes"""
        return [node for node in self.get_current_nodes() if node['role'] == 'replica']
