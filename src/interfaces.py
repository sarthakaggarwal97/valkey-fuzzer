"""
Base interfaces and abstract classes for all major components
"""
from abc import ABC, abstractmethod
from typing import List, Optional
from dataclasses import dataclass
from .models import (
    Scenario, ExecutionResult, DSLConfig, ValidationResult,
    ClusterConfig, ClusterInstance, ClusterStatus, NodeInfo,
    ChaosConfig, ChaosResult, ProcessChaosType,
    WorkloadConfig, WorkloadSession, WorkloadMetrics,
    Operation
)


class IFuzzerEngine(ABC):
    """Interface for the main Fuzzer Engine orchestrator"""
    
    @abstractmethod
    def generate_random_scenario(self, seed: Optional[int] = None) -> Scenario:
        """Generate a randomized test scenario"""
        pass
    
    @abstractmethod
    def execute_dsl_scenario(self, dsl_config: DSLConfig) -> ExecutionResult:
        """Execute a test scenario from DSL configuration"""
        pass
    
    @abstractmethod
    def execute_test(self, scenario: Scenario) -> ExecutionResult:
        """Execute a complete test scenario"""
        pass
    
    @abstractmethod
    def validate_cluster_state(self, cluster_id: str) -> ValidationResult:
        """Validate current cluster state"""
        pass
    
    @abstractmethod
    def coordinate_chaos(self, operation: Operation, chaos_config: ChaosConfig) -> ChaosResult:
        """Coordinate chaos injection with operation execution"""
        pass


class IClusterOrchestrator(ABC):
    """Interface for cluster lifecycle management"""
    
    @abstractmethod
    def create_cluster(self, config: ClusterConfig) -> ClusterInstance:
        """Create a new Valkey cluster"""
        pass
    
    @abstractmethod
    def destroy_cluster(self, cluster_id: str) -> bool:
        """Destroy an existing cluster"""
        pass
    
    @abstractmethod
    def get_cluster_status(self, cluster_id: str) -> ClusterStatus:
        """Get current cluster status"""
        pass
    
    @abstractmethod
    def get_node_info(self, cluster_id: str) -> List[NodeInfo]:
        """Get information about all nodes in cluster"""
        pass
    
    @abstractmethod
    def wait_for_cluster_ready(self, cluster_id: str, timeout: float = 60.0) -> bool:
        """Wait for cluster to be ready for testing"""
        pass


class IChaosEngine(ABC):
    """Interface for chaos injection"""
    
    @abstractmethod
    def inject_process_chaos(self, target_node: NodeInfo, chaos_type: ProcessChaosType) -> ChaosResult:
        """Inject process-level chaos on target node"""
        pass
    
    @abstractmethod
    def coordinate_with_operation(self, operation: Operation, chaos_config: ChaosConfig) -> ChaosResult:
        """Coordinate chaos injection with operation execution"""
        pass
    
    @abstractmethod
    def stop_chaos(self, chaos_id: str) -> bool:
        """Stop active chaos injection"""
        pass
    
    @abstractmethod
    def cleanup_chaos(self, cluster_id: str) -> bool:
        """Clean up any remaining chaos effects"""
        pass


class IValkeyClient(ABC):
    """Interface for workload generation"""
    
    @abstractmethod
    def start_workload(self, cluster_info: ClusterStatus, workload_config: WorkloadConfig) -> WorkloadSession:
        """Start workload generation against cluster"""
        pass
    
    @abstractmethod
    def stop_workload(self, session_id: str) -> WorkloadMetrics:
        """Stop workload and return metrics"""
        pass
    
    @abstractmethod
    def get_workload_metrics(self, session_id: str) -> WorkloadMetrics:
        """Get current workload metrics"""
        pass
    
    @abstractmethod
    def is_workload_active(self, session_id: str) -> bool:
        """Check if workload session is active"""
        pass


class ITestCaseGenerator(ABC):
    """Interface for test case generation"""
    
    @abstractmethod
    def generate_random_scenario(self, seed: Optional[int] = None) -> Scenario:
        """Generate randomized test scenario"""
        pass
    
    @abstractmethod
    def parse_dsl_config(self, dsl_text: str) -> Scenario:
        """Parse DSL configuration into test scenario"""
        pass
    
    @abstractmethod
    def validate_scenario(self, scenario: Scenario) -> bool:
        """Validate test scenario configuration"""
        pass


class IOperationOrchestrator(ABC):
    """Interface for operation execution"""
    
    @abstractmethod
    def execute_operation(self, operation: Operation, cluster_id: str) -> bool:
        """Execute a single cluster operation"""
        pass
    
    @abstractmethod
    def validate_operation_preconditions(self, operation: Operation, cluster_status: ClusterStatus) -> bool:
        """Validate that operation can be executed"""
        pass
    
    @abstractmethod
    def wait_for_operation_completion(self, operation: Operation, cluster_id: str, timeout: float) -> bool:
        """Wait for operation to complete"""
        pass


class IStateValidator(ABC):
    """Interface for cluster state validation"""
    
    @abstractmethod
    def validate_cluster_state(self, cluster_id: str) -> ValidationResult:
        """Perform comprehensive cluster state validation"""
        pass
    
    @abstractmethod
    def validate_slot_coverage(self, cluster_status: ClusterStatus) -> bool:
        """Validate all slots are assigned"""
        pass
    
    @abstractmethod
    def validate_replica_sync(self, cluster_status: ClusterStatus) -> bool:
        """Validate replica synchronization"""
        pass
    
    @abstractmethod
    def validate_data_consistency(self, cluster_status: ClusterStatus) -> bool:
        """Validate data consistency across nodes"""
        pass


class ILogger(ABC):
    """Interface for test logging and reporting"""
    
    @abstractmethod
    def log_test_start(self, scenario: Scenario) -> None:
        """Log test scenario start"""
        pass
    
    @abstractmethod
    def log_operation(self, operation: Operation, success: bool, details: str) -> None:
        """Log operation execution"""
        pass
    
    @abstractmethod
    def log_chaos_event(self, chaos_result: ChaosResult) -> None:
        """Log chaos injection event"""
        pass
    
    @abstractmethod
    def log_validation_result(self, validation_result: ValidationResult) -> None:
        """Log validation result"""
        pass
    
    @abstractmethod
    def log_test_completion(self, test_result: ExecutionResult) -> None:
        """Log test completion"""
        pass
    
    @abstractmethod
    def generate_report(self, test_results: List[ExecutionResult]) -> str:
        """Generate summary report"""
        pass


# Additional data class needed for cluster instances
@dataclass
class ClusterInstance:
    """Represents a created cluster instance"""
    cluster_id: str
    config: ClusterConfig
    nodes: List[NodeInfo]
    creation_time: float
    is_ready: bool = False