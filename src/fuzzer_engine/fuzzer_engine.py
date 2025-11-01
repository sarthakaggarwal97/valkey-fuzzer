"""
Fuzzer Engine - Main orchestrator for test scenario execution
"""
import time
import logging
from typing import Optional
from ..models import (
    Scenario, ExecutionResult, DSLConfig, ValidationResult,
    ClusterConnection, ClusterStatus
)
from ..interfaces import IFuzzerEngine
from .test_case_generator import ScenarioGenerator
from .cluster_coordinator import ClusterCoordinator
from .chaos_coordinator import ChaosCoordinator
from .operation_orchestrator import OperationOrchestrator
from .state_validator import StateValidator
from .test_logger import FuzzerLogger

logging.basicConfig(format='%(levelname)-5s | %(filename)s:%(lineno)-3d | %(message)s', level=logging.INFO, force=True)
logger = logging.getLogger(__name__)


class FuzzerEngine(IFuzzerEngine):
    """
    Main orchestrator for the Cluster Bus Fuzzer.
    Coordinates all components to execute test scenarios end-to-end.
    """
    
    def __init__(self):
        """Initialize the fuzzer engine with all component coordinators"""
        self.scenario_generator = ScenarioGenerator()
        self.cluster_coordinator = ClusterCoordinator()
        self.chaos_coordinator = ChaosCoordinator()
        self.operation_orchestrator = OperationOrchestrator()
        self.state_validator = StateValidator()
        self.logger = FuzzerLogger()
        
        logger.info("Fuzzer Engine initialized")
    
    def generate_random_scenario(self, seed: Optional[int] = None) -> Scenario:
        """Generate a randomized test scenario with optional seed for reproducibility."""
        logger.info(f"Generating random scenario with seed: {seed}")
        return self.scenario_generator.generate_random_scenario(seed)
    
    def execute_dsl_scenario(self, dsl_config: DSLConfig) -> ExecutionResult:
        """Execute a test scenario from DSL configuration."""
        logger.info("Executing DSL-based scenario")
        
        try:
            # Parse DSL configuration into scenario
            scenario = self.scenario_generator.parse_dsl_config(dsl_config.config_text)
            
            # Validate scenario
            self.scenario_generator.validate_scenario(scenario)
            
            # Execute the scenario
            return self.execute_test(scenario)
            
        except ValueError as e:
            logger.error(f"DSL parsing failed: {e}")
            return ExecutionResult(
                scenario_id="dsl-parse-error",
                success=False,
                start_time=time.time(),
                end_time=time.time(),
                operations_executed=0,
                chaos_events=[],
                validation_results=[],
                error_message=f"DSL parsing error: {e}"
            )
        except Exception as e:
            logger.error(f"DSL scenario execution failed: {e}")
            return ExecutionResult(
                scenario_id="dsl-execution-error",
                success=False,
                start_time=time.time(),
                end_time=time.time(),
                operations_executed=0,
                chaos_events=[],
                validation_results=[],
                error_message=f"Execution error: {e}"
            )
    
    def execute_test(self, scenario: Scenario) -> ExecutionResult:
        """
        Execute a complete test scenario end-to-end.
        
        This is the main execution pipeline that:
        1. Creates the cluster
        2. Validates cluster readiness
        3. Executes operations with chaos coordination
        4. Validates cluster state after each operation
        5. Cleans up resources
        
        Args:
            scenario: Test scenario to execute
            
        Returns:
            Execution result with test outcomes
        """
        start_time = time.time()
        operations_executed = 0
        chaos_events = []
        validation_results = []
        cluster_instance = None
        cluster_connection = None
        
        logger.info(f"Starting test execution: {scenario.scenario_id}")
        
        # Log test start
        self.logger.log_test_start(scenario)
        
        try:
            # Step 1: Create cluster
            logger.info("Step 1: Creating cluster")
            cluster_instance = self._create_cluster_with_retry(scenario)
            
            if not cluster_instance:
                raise Exception("Failed to create cluster after retries")
            
            # Create cluster connection for operations
            cluster_connection = ClusterConnection(
                initial_nodes=cluster_instance.nodes,
                cluster_id=cluster_instance.cluster_id
            )
            
            # Log initial cluster state
            cluster_status = self.cluster_coordinator.get_cluster_status(cluster_instance.cluster_id)
            if cluster_status:
                self.logger.log_cluster_state_snapshot(cluster_status, "initial_state")
            
            # Step 2: Validate cluster readiness
            logger.info("Step 2: Validating cluster readiness")
            if not self._validate_cluster_readiness_with_retry(cluster_instance.cluster_id):
                raise Exception("Cluster failed readiness validation")
            
            # Register cluster nodes with chaos coordinator
            self.chaos_coordinator.register_cluster_nodes(
                cluster_instance.cluster_id,
                cluster_instance.nodes
            )
            
            # Set cluster connection for operation orchestrator
            self.operation_orchestrator.set_cluster_connection(cluster_connection)
            
            # Step 3: Execute operations with chaos coordination
            logger.info(f"Step 3: Executing {len(scenario.operations)} operations")
            
            for i, operation in enumerate(scenario.operations):
                logger.info(f"Executing operation {i+1}/{len(scenario.operations)}: {operation.type.value}")
                
                try:
                    # Log cluster state before operation
                    cluster_status = self.cluster_coordinator.get_cluster_status(cluster_instance.cluster_id)
                    if cluster_status:
                        self.logger.log_cluster_state_snapshot(
                            cluster_status, 
                            f"before_operation_{i+1}"
                        )
                    
                    # Coordinate chaos with operation
                    operation_chaos_events = self.chaos_coordinator.coordinate_chaos_with_operation(
                        operation,
                        scenario.chaos_config,
                        cluster_instance.nodes
                    )
                    chaos_events.extend(operation_chaos_events)
                    
                    # Execute operation
                    operation_success = self.operation_orchestrator.execute_operation(
                        operation,
                        cluster_instance.cluster_id
                    )
                    
                    # Log operation result
                    self.logger.log_operation(
                        operation,
                        operation_success,
                        f"Operation {'succeeded' if operation_success else 'failed'}"
                    )
                    
                    if operation_success:
                        operations_executed += 1
                    else:
                        logger.warning(f"Operation {i+1} failed, continuing with graceful degradation")
                    
                    # Log cluster state after operation
                    cluster_status = self.cluster_coordinator.get_cluster_status(cluster_instance.cluster_id)
                    if cluster_status:
                        self.logger.log_cluster_state_snapshot(
                            cluster_status,
                            f"after_operation_{i+1}"
                        )
                    
                    # Validate cluster state after operation
                    validation_result = self.state_validator.validate_cluster_state(
                        cluster_instance.cluster_id,
                        cluster_connection
                    )
                    validation_results.append(validation_result)
                    self.logger.log_validation_result(validation_result)
                    
                    # Check if validation failed critically
                    if not validation_result.slot_coverage:
                        logger.error("Critical validation failure: slot coverage lost")
                        # Continue with graceful degradation
                    
                except Exception as e:
                    logger.error(f"Operation {i+1} execution failed: {e}")
                    self.logger.log_error(
                        f"Operation {i+1} failed",
                        {"operation": operation.type.value, "error": str(e)}
                    )
                    # Continue with next operation (graceful degradation)
            
            # Step 4: Final cluster validation
            logger.info("Step 4: Final cluster validation")
            final_validation = self.state_validator.validate_cluster_state(
                cluster_instance.cluster_id,
                cluster_connection
            )
            validation_results.append(final_validation)
            self.logger.log_validation_result(final_validation)
            
            # Determine overall success
            success = (
                operations_executed > 0 and
                final_validation.slot_coverage and
                final_validation.replica_sync.all_replicas_synced
            )
            
            end_time = time.time()
            
            # Create execution result
            result = ExecutionResult(
                scenario_id=scenario.scenario_id,
                success=success,
                start_time=start_time,
                end_time=end_time,
                operations_executed=operations_executed,
                chaos_events=chaos_events,
                validation_results=validation_results,
                seed=scenario.seed
            )
            
            logger.info(f"Test execution completed: {'SUCCESS' if success else 'FAILED'}")
            
            # Log test completion
            self.logger.log_test_completion(result)
            
            return result
            
        except Exception as e:
            logger.error(f"Test execution failed: {e}")
            
            end_time = time.time()
            
            # Log error
            self.logger.log_error(f"Test execution failed: {e}")
            
            # Create failure result
            result = ExecutionResult(
                scenario_id=scenario.scenario_id,
                success=False,
                start_time=start_time,
                end_time=end_time,
                operations_executed=operations_executed,
                chaos_events=chaos_events,
                validation_results=validation_results,
                error_message=str(e),
                seed=scenario.seed
            )
            
            # Log test completion
            self.logger.log_test_completion(result)
            
            return result
            
        finally:
            # Step 5: Cleanup
            logger.info("Step 5: Cleaning up resources")
            self._cleanup_resources(cluster_instance, cluster_connection)
    
    def validate_cluster_state(self, cluster_id: str) -> ValidationResult:
        """Validate current cluster state and return validation result."""
        logger.info(f"Validating cluster state: {cluster_id}")
        
        # Get cluster connection from active clusters
        if cluster_id not in self.cluster_coordinator.active_clusters:
            logger.error(f"Cluster {cluster_id} not found")
            raise ValueError(f"Cluster {cluster_id} not found")
        
        cluster_data = self.cluster_coordinator.active_clusters[cluster_id]
        cluster_instance = cluster_data['instance']
        
        cluster_connection = ClusterConnection(
            initial_nodes=cluster_instance.nodes,
            cluster_id=cluster_id
        )
        
        return self.state_validator.validate_cluster_state(cluster_id, cluster_connection)
    
    def _create_cluster_with_retry(self, scenario: Scenario, max_retries: int = 3):
        """
        Create cluster with retry logic for failure recovery.
        
        Args:
            scenario: Test scenario containing cluster configuration
            max_retries: Maximum number of retry attempts
            
        Returns:
            ClusterInstance if successful, None otherwise
        """
        for attempt in range(max_retries):
            try:
                logger.info(f"Creating cluster (attempt {attempt + 1}/{max_retries})")
                cluster_instance = self.cluster_coordinator.create_cluster(scenario.cluster_config)
                logger.info(f"Cluster created successfully: {cluster_instance.cluster_id}")
                return cluster_instance
                
            except Exception as e:
                logger.error(f"Cluster creation attempt {attempt + 1} failed: {e}")
                
                if attempt < max_retries - 1:
                    # Exponential backoff
                    wait_time = 2 ** attempt
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error("All cluster creation attempts failed")
                    self.logger.log_error(
                        "Cluster creation failed after all retries",
                        {"attempts": max_retries, "error": str(e)}
                    )
        
        return None
    
    def _validate_cluster_readiness_with_retry(self, cluster_id: str, max_retries: int = 5) -> bool:
        """
        Validate cluster readiness with retry logic.
        
        Args:
            cluster_id: Cluster identifier
            max_retries: Maximum number of retry attempts
            
        Returns:
            True if cluster is ready, False otherwise
        """
        for attempt in range(max_retries):
            try:
                logger.info(f"Validating cluster readiness (attempt {attempt + 1}/{max_retries})")
                
                if self.cluster_coordinator.validate_cluster_readiness(cluster_id):
                    logger.info("Cluster is ready")
                    return True
                
                if attempt < max_retries - 1:
                    logger.info("Cluster not ready, waiting...")
                    time.sleep(2)
                    
            except Exception as e:
                logger.error(f"Readiness validation attempt {attempt + 1} failed: {e}")
                
                if attempt < max_retries - 1:
                    time.sleep(2)
        
        logger.error("Cluster failed readiness validation after all retries")
        self.logger.log_error(
            "Cluster readiness validation failed",
            {"attempts": max_retries}
        )
        return False
    
    def _cleanup_resources(self, cluster_instance, cluster_connection):
        """
        Clean up all resources with graceful degradation.
        
        Args:
            cluster_instance: Cluster instance to clean up
            cluster_connection: Cluster connection to close
        """
        try:
            # Stop all active chaos
            logger.info("Stopping active chaos injections")
            self.chaos_coordinator.stop_all_chaos()
            
            # Cleanup chaos for cluster
            if cluster_instance:
                self.chaos_coordinator.cleanup_chaos(cluster_instance.cluster_id)
            
            # Destroy cluster
            if cluster_instance:
                logger.info(f"Destroying cluster: {cluster_instance.cluster_id}")
                self.cluster_coordinator.destroy_cluster(cluster_instance.cluster_id)
            
            logger.info("Resource cleanup completed")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            # Continue cleanup even if errors occur (graceful degradation)
