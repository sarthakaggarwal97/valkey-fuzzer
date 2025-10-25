"""
Core data models for the Cluster Bus Fuzzer
"""
from dataclasses import dataclass
from typing import Dict, List, Any, Optional
from enum import Enum


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


class NodeRole(Enum):
    """Valkey node roles"""
    PRIMARY = "primary"
    REPLICA = "replica"
    FAILED = "failed"


@dataclass
class NodeConfig:
    """Configuration for a single Valkey node"""
    node_id: str
    host: str
    port: int
    role: NodeRole
    primary_id: Optional[str] = None  # For replicas, ID of their primary


@dataclass
class ClusterConfig:
    """Configuration for a Valkey cluster"""
    shard_count: int  # 3-16 shards
    replica_count: int  # 0-2 replicas per shard
    node_configs: List[NodeConfig]
    
    def __post_init__(self):
        """Validate cluster configuration"""
        if not (3 <= self.shard_count <= 16):
            raise ValueError("Shard count must be between 3 and 16")
        if not (0 <= self.replica_count <= 2):
            raise ValueError("Replica count must be between 0 and 2")


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
    coordinate_with_operation: bool = True
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
class NodeInfo:
    """Information about a cluster node"""
    node_id: str
    host: str
    port: int
    role: NodeRole
    slots: List[int]
    primary_id: Optional[str] = None
    replica_ids: List[str] = None
    is_healthy: bool = True
    
    def __post_init__(self):
        if self.replica_ids is None:
            self.replica_ids = []


@dataclass
class ClusterStatus:
    """Overall cluster status information"""
    cluster_id: str
    nodes: List[NodeInfo]
    total_slots_assigned: int
    is_healthy: bool
    formation_complete: bool


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
class ClusterInstance:
    """Represents a created cluster instance"""
    cluster_id: str
    config: ClusterConfig
    nodes: List[NodeInfo]
    creation_time: float
    is_ready: bool = False