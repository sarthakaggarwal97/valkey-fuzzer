"""
Fuzzer Engine - Main orchestrator for test scenario execution
"""
import time
import logging
from typing import Optional
from ..models import Scenario, ExecutionResult, DSLConfig, ValidationResult, ClusterConnection, ClusterStatus
from ..interfaces import IFuzzerEngine
from ..valkey_client.load_data import load_all_slots
from .test_case_generator import ScenarioGenerator
from .cluster_coordinator import ClusterCoordinator
from .chaos_coordinator import ChaosCoordinator
from .operation_orchestrator import OperationOrchestrator
from .state_validator import StateValidator
from .test_logger import FuzzerLogger
from .error_handler import ErrorHandler, ErrorContext, ErrorCategory, ErrorSeverity, RetryConfig

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
        self.error_handler = ErrorHandler()
        
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
            print()
            logger.info("Step 2: Validating cluster readiness")
            if not self._validate_cluster_readiness_with_retry(cluster_instance.cluster_id):
                raise Exception("Cluster failed readiness validation")
            
            # Step 3: Load test data
            print()
            logger.info("Step 3: Loading test data")
            self._load_test_data(cluster_connection)
            
            self.chaos_coordinator.register_cluster_nodes(cluster_instance.cluster_id, cluster_instance.nodes)
            
            # Set cluster connection for operation orchestrator
            self.operation_orchestrator.set_cluster_connection(cluster_connection)
            
            # Step 4: Execute operations with chaos coordination
            print()
            logger.info(f"Step 4: Executing {len(scenario.operations)} operations")
            
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
            
            # Step 5: Final cluster validation
            print()
            logger.info("Step 5: Final cluster validation")
            final_validation = self.state_validator.validate_cluster_state(
                cluster_instance.cluster_id,
                cluster_connection
            )
            validation_results.append(final_validation)
            self.logger.log_validation_result(final_validation)
            
            # Determine overall success
            # Success means: operations executed and cluster remains operational (all slots covered)
            # We don't require all replicas to be synced because chaos may have killed nodes
            success = (
                operations_executed > 0 and
                final_validation.slot_coverage
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
        """
        retry_config = RetryConfig(
            max_attempts=max_retries,
            initial_delay=1.0,
            exponential_base=2.0,
            jitter=True
        )
        
        def create_cluster_operation():
            return self.cluster_coordinator.create_cluster(scenario.cluster_config)
        
        success, cluster_instance = self.error_handler.retry_with_backoff(
            operation=create_cluster_operation,
            config=retry_config,
            error_category=ErrorCategory.CLUSTER_CREATION,
            operation_name="cluster creation"
        )
        
        if success:
            logger.info(f"Cluster created successfully - Cluster ID: {cluster_instance.cluster_id}")
        else:
            logger.error("All cluster creation attempts failed")
            self.logger.log_error("Cluster creation failed after all retries", {"attempts": max_retries})
        
        return cluster_instance if success else None
    
    def _validate_cluster_readiness_with_retry(self, cluster_id: str, max_retries: int = 5) -> bool:
        """
        Validate cluster readiness with retry logic.
        """
        retry_config = RetryConfig(
            max_attempts=max_retries,
            initial_delay=2.0,
            exponential_base=1.5,
            jitter=False
        )
        
        def validate_readiness_operation():
            if self.cluster_coordinator.validate_cluster_readiness(cluster_id):
                return True
            else:
                raise Exception("Cluster not ready")
        
        success, _ = self.error_handler.retry_with_backoff(
            operation=validate_readiness_operation,
            config=retry_config,
            error_category=ErrorCategory.CLUSTER_VALIDATION,
            operation_name="cluster readiness validation"
        )
        
        if not success:
            logger.error("Cluster failed readiness validation after all retries")
            self.logger.log_error("Cluster readiness validation failed", {"attempts": max_retries, "cluster_id": cluster_id})
        
        return success
    
    def _load_test_data(self, cluster_connection: ClusterConnection):
        """Load test data into the cluster for validation purposes"""
        load_all_slots(cluster_connection, keys_per_slot=5)
    
    def _cleanup_resources(self, cluster_instance, cluster_connection):
        """
        Clean up all resources with graceful degradation.
        """
        try:
            cleanup_success = self.error_handler.cleanup_after_failure(
                cluster_instance=cluster_instance,
                cluster_connection=cluster_connection,
                chaos_coordinator=self.chaos_coordinator,
                cluster_coordinator=self.cluster_coordinator
            )
            
            if cleanup_success:
                logger.info("Resource cleanup completed successfully")
            else:
                logger.warning("Resource cleanup completed with errors")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            # Log error but don't raise - cleanup should always complete
            error_context = ErrorContext(
                category=ErrorCategory.RESOURCE_CLEANUP,
                severity=ErrorSeverity.MEDIUM,
                message=f"Cleanup error: {e}",
                exception=e,
                cluster_id=cluster_instance.cluster_id if cluster_instance else None
            )
            self.error_handler.handle_error(error_context)
