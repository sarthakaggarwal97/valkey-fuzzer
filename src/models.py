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
    
    def get_current_nodes(self) -> List[Dict]:
        """Get current cluster topology via CLUSTER NODES"""
        # Used for real-time cluster topology for chaos testing
        
        # Build a mapping from port to shard_id using initial_nodes
        port_to_shard = {node.port: node.shard_id for node in self.initial_nodes}
        
        for node_info in self.startup_nodes:
            try:
                client = valkey.Valkey(host=node_info['host'], port=node_info['port'])
                cluster_nodes_raw = client.execute_command('CLUSTER', 'NODES')
                client.close()
                
                current_nodes = []
                for line in cluster_nodes_raw.decode().strip().split('\n'):
                    parts = line.split()
                    host_port = parts[1].split('@')[0].split(':')
                    port = int(host_port[1])
                    
                    # Get shard_id from our mapping
                    shard_id = port_to_shard.get(port)
                    
                    current_nodes.append({
                        'node_id': parts[0],
                        'host': host_port[0],
                        'port': port,
                        'role': 'primary' if 'master' in parts[2] else 'replica',
                        'shard_id': shard_id
                    })
                return current_nodes # list of dictionaries representing all nodes currently active in cluster
            except:
                continue
        return []
    
    def get_primary_nodes(self) -> List[Dict]:
        """Get current primary nodes"""
        return [node for node in self.get_current_nodes() if node['role'] == 'primary']
    
    def get_replica_nodes(self) -> List[Dict]:
        """Get current replica nodes"""
        return [node for node in self.get_current_nodes() if node['role'] == 'replica']
