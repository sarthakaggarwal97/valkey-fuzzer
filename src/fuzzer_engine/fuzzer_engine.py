"""
Fuzzer Engine - Main orchestrator for test scenario execution
"""
import time
import logging
from copy import deepcopy
from typing import Optional
from ..models import Scenario, ExecutionResult, DSLConfig, ClusterConnection, ClusterStatus
from ..interfaces import IFuzzerEngine
from ..valkey_client.load_data import load_all_slots
from .test_case_generator import ScenarioGenerator
from .cluster_coordinator import ClusterCoordinator
from .chaos_coordinator import ChaosCoordinator
from .operation_orchestrator import OperationOrchestrator
from .test_logger import FuzzerLogger
from .error_handler import ErrorHandler, ErrorContext, ErrorCategory, ErrorSeverity, RetryConfig
from .state_validator import StateValidator
from ..models import (
    StateValidationConfig, ExpectedTopology, OperationContext, ChaosType
)

logging.basicConfig(format='%(levelname)-5s | %(filename)s:%(lineno)-3d | %(message)s', level=logging.INFO, force=True)
logger = logging.getLogger(__name__)

cli_logger = logging.getLogger('cli')
cli_logger.addHandler(logging.StreamHandler())
cli_logger.propagate = False

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
        self.logger = FuzzerLogger()
        self.error_handler = ErrorHandler()
        
        logger.info("Fuzzer Engine Initialized")
    
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
        validation_results = []  # Collect validation results for each operation
        final_validation = None
        cluster_instance = None
        cluster_connection = None
        
        logger.info(f"Starting test execution: {scenario.scenario_id}")
        
        # Reinitialize chaos coordinator with scenario seed for deterministic chaos selection
        self.chaos_coordinator = ChaosCoordinator(seed=scenario.seed)
        
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
            cli_logger.info("")
            logger.info("Step 2: Validating cluster readiness")
            if not self._validate_cluster_readiness_with_retry(cluster_instance.cluster_id):
                raise Exception("Cluster failed readiness validation")
            
            # Step 3: Load test data
            cli_logger.info("")
            logger.info("Step 3: Loading test data")
            self._load_test_data(cluster_connection)
            
            self.chaos_coordinator.register_cluster_nodes(cluster_instance.cluster_id, cluster_instance.nodes)
            
            # Set cluster connection for operation orchestrator
            self.operation_orchestrator.set_cluster_connection(cluster_connection)
            
            # Create StateValidator with config from scenario or use defaults
            if hasattr(scenario, 'state_validation_config') and scenario.state_validation_config:
                validation_config = deepcopy(scenario.state_validation_config)
                logger.info("Using scenario-specific StateValidationConfig (copied)")
            else:
                validation_config = StateValidationConfig()
                logger.info("Using default StateValidationConfig")
            
            # Adjust replication validation config based on cluster topology
            # If the cluster has no replicas, disable the min_replicas_per_shard check
            expected_replicas_per_shard = scenario.cluster_config.replicas_per_shard
            if expected_replicas_per_shard == 0:
                validation_config.replication_config.min_replicas_per_shard = 0
                logger.info("Cluster has no replicas - disabled min_replicas_per_shard check")
            elif validation_config.replication_config.min_replicas_per_shard > expected_replicas_per_shard:
                # Don't require more replicas than the cluster is configured to have
                validation_config.replication_config.min_replicas_per_shard = expected_replicas_per_shard
                logger.info(
                    f"Adjusted min_replicas_per_shard to {expected_replicas_per_shard} "
                    f"to match cluster configuration"
                )
            
            state_validator = StateValidator(validation_config)
            logger.info("State validation initialized")
            
            # Write test data for data consistency validation
            if validation_config.check_data_consistency:
                logger.info("Writing test data for data consistency validation")
                test_data_written = state_validator.write_test_data(cluster_connection)
                if test_data_written:
                    logger.info("Test data written successfully")
                else:
                    logger.warning("Failed to write test data - disabling data consistency validation for this run")
                    state_validator.config.check_data_consistency = False
            
            # Track killed nodes for chaos-aware validation
            killed_nodes_tracker = set()
            
            # Step 4: Execute operations with chaos coordination
            cli_logger.info("")
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
                        cluster_connection,
                        cluster_instance.cluster_id
                    )
                    chaos_events.extend(operation_chaos_events)
                    
                    # Register killed nodes with state validator for chaos-aware validation
                    for chaos_event in operation_chaos_events:
                        if chaos_event.success and chaos_event.chaos_type == ChaosType.PROCESS_KILL:
                            # Find the node address from target_node (node_id)
                            target_node_id = chaos_event.target_node
                            for node in cluster_instance.nodes:
                                if node.node_id == target_node_id:
                                    node_address = f"{node.host}:{node.port}"
                                    state_validator.register_killed_node(node_address)
                                    killed_nodes_tracker.add(node_address)
                                    logger.info(f"Registered killed node for validation: {node_address}")
                                    break
                    
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
                    
                    # State validation
                    logger.info(f"Running state validation after operation {i+1}")
                    
                    # Build expected topology from cluster config
                    expected_topology = ExpectedTopology(
                        num_primaries=scenario.cluster_config.num_shards,
                        num_replicas=scenario.cluster_config.num_shards * scenario.cluster_config.replicas_per_shard,
                        shard_structure={}
                    )
                    
                    # Create operation context
                    operation_context = OperationContext(
                        operation_type=operation.type,
                        target_node=operation.target_node,
                        operation_success=operation_success,
                        operation_timestamp=time.time()
                    )
                    
                    # Execute state validation with retry
                    validation_result = state_validator.validate_with_retry(
                        cluster_connection,
                        expected_topology,
                        operation_context
                    )
                    
                    # Collect validation result for API consumers
                    validation_results.append(validation_result)
                    
                    # Log state validation result
                    self.logger.log_state_validation_result(validation_result, i+1)
                    
                    # NOTE: We do NOT clear killed_nodes here!
                    # Killed nodes should remain tracked throughout the scenario because:
                    # 1. Nodes killed in operation N may still be down in operation N+1 (expected)
                    # 2. Final validation needs to know which nodes were killed
                    # 3. Clearing would cause false positives for nodes that haven't recovered yet
                    
                    # Check for validation failures
                    if not validation_result.overall_success:
                        # Determine severity
                        is_critical = validation_result.is_critical_failure()
                        severity = "CRITICAL" if is_critical else "NON-CRITICAL"
                        
                        logger.error(
                            f"{severity} VALIDATION FAILURE after operation {i+1}: "
                            f"{', '.join(validation_result.failed_checks)}"
                        )
                        
                        # Check if we should halt execution on ANY failure (not just critical)
                        if state_validator.config.blocking_on_failure:
                            logger.error(
                                f"Halting execution due to validation failure "
                                f"(blocking_on_failure=True)"
                            )
                            raise Exception(
                                f"Validation failure after operation {i+1}: "
                                f"{', '.join(validation_result.failed_checks)}"
                            )
                    
                except Exception as e:
                    logger.error(f"Operation {i+1} execution failed: {e}")
                    self.logger.log_error(
                        f"Operation {i+1} failed",
                        {"operation": operation.type.value, "error": str(e)}
                    )
                    
                    # Check if this is a validation failure that should halt execution
                    if "Validation failure" in str(e) and state_validator.config.blocking_on_failure:
                        logger.error("Re-raising validation failure to halt scenario execution")
                        raise  # Re-raise to stop the scenario
                    
                    # For other errors, continue with next operation (graceful degradation)
            
            # Step 5: Final cluster validation
            cli_logger.info("")
            logger.info("Step 5: Final cluster validation")
            
            # Build expected topology for final validation
            expected_topology = ExpectedTopology(
                num_primaries=scenario.cluster_config.num_shards,
                num_replicas=scenario.cluster_config.num_shards * scenario.cluster_config.replicas_per_shard,
                shard_structure={}
            )
            
            # Execute final validation with retry (consistent with per-operation validation)
            final_validation_result = state_validator.validate_with_retry(
                cluster_connection,
                expected_topology,
                None
            )
            
            # Store final validation for API consumers
            final_validation = final_validation_result
            
            # Log final validation result
            self.logger.log_state_validation_result(final_validation_result, "final")
            
            # Determine overall success
            # Success means: operations executed and validation passed
            # Use the validation's overall_success which respects enabled/disabled checks
            success = (
                operations_executed > 0 and
                final_validation_result.overall_success
            )
            
            end_time = time.time()
            
            # Create execution result with validation results
            result = ExecutionResult(
                scenario_id=scenario.scenario_id,
                success=success,
                start_time=start_time,
                end_time=end_time,
                operations_executed=operations_executed,
                chaos_events=chaos_events,
                seed=scenario.seed,
                validation_results=validation_results,
                final_validation=final_validation
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
    
    def validate_cluster_state(self, cluster_id: str):
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
        
        # Use StateValidator for validation
        # Disable data consistency check for standalone validation (no test data seeded)
        validation_config = StateValidationConfig()
        validation_config.check_data_consistency = False
        
        # Adjust replication validation config based on actual cluster topology
        # This ensures zero-replica clusters can pass validation
        actual_replicas = len([n for n in cluster_instance.nodes if n.role == 'replica'])
        actual_primaries = len([n for n in cluster_instance.nodes if n.role == 'primary'])
        
        # Calculate replicas per shard from actual topology
        if actual_primaries > 0:
            replicas_per_shard = actual_replicas // actual_primaries
        else:
            replicas_per_shard = 0
        
        if replicas_per_shard == 0:
            validation_config.replication_config.min_replicas_per_shard = 0
            logger.info("Cluster has no replicas - disabled min_replicas_per_shard check")
        elif validation_config.replication_config.min_replicas_per_shard > replicas_per_shard:
            # Don't require more replicas than the cluster actually has
            validation_config.replication_config.min_replicas_per_shard = replicas_per_shard
            logger.info(
                f"Adjusted min_replicas_per_shard to {replicas_per_shard} "
                f"to match actual cluster topology"
            )
        
        validator = StateValidator(validation_config)
        
        # Build expected topology
        expected_topology = ExpectedTopology(
            num_primaries=actual_primaries,
            num_replicas=actual_replicas,
            shard_structure={}
        )
        
        logger.info("Running standalone validation (data consistency check disabled - no test data)")
        return validator.validate_state(cluster_connection, expected_topology, None)
    
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
