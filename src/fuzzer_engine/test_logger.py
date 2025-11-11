"""
Test Logger - Comprehensive logging and reporting for test execution
"""
import json
import time
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
from ..models import Scenario, Operation, ChaosResult, ValidationResult, ExecutionResult, ClusterStatus, NodeInfo


logging.basicConfig(format='%(levelname)-5s | %(filename)s:%(lineno)-3d | %(message)s', level=logging.INFO, force=True)
logger = logging.getLogger(__name__)


class FuzzerLogger:
    """
    Immutable test execution logging system with comprehensive reporting.
    Logs all test execution details including operations, chaos events, and validation results.
    """
    
    def __init__(self, log_dir: str = "/tmp/valkey-fuzzer/logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        self.current_test_id: Optional[str] = None
        self.test_logs: Dict[str, Dict[str, Any]] = {}
        self.test_start_times: Dict[str, float] = {}
        
        logger.info(f"Test logger initialized with log directory: {self.log_dir}")
    
    def log_test_start(self, scenario: Scenario) -> None:
        """Log the start of a test scenario with immutable configuration."""
        self.current_test_id = scenario.scenario_id
        self.test_start_times[scenario.scenario_id] = time.time()
        
        # Initialize test log structure
        self.test_logs[scenario.scenario_id] = {
            'scenario_id': scenario.scenario_id,
            'seed': scenario.seed,
            'start_time': self.test_start_times[scenario.scenario_id],
            'start_timestamp': datetime.now().isoformat(),
            'cluster_config': self._serialize_cluster_config(scenario.cluster_config),
            'operations': self._serialize_operations(scenario.operations),
            'chaos_config': self._serialize_chaos_config(scenario.chaos_config),
            'validation_config': self._serialize_validation_config(scenario.validation_config),
            'operation_logs': [],
            'chaos_events': [],
            'validation_results': [],
            'cluster_state_snapshots': [],
            'errors': [],
            'status': 'running'
        }
                
        # Log human-readable scenario summary
        self._log_scenario_summary(scenario)
        
        self._write_log_to_disk(scenario.scenario_id)
    
    def log_operation(self, operation: Operation, success: bool, details: str) -> None:
        """Log a cluster operation execution with success status and details."""
        if not self.current_test_id:
            logger.warning("No active test to log operation to")
            return
        
        operation_log = {
            'timestamp': time.time(),
            'datetime': datetime.now().isoformat(),
            'operation_type': operation.type.value,
            'target_node': operation.target_node,
            'parameters': operation.parameters,
            'timing': {
                'delay_before': operation.timing.delay_before,
                'timeout': operation.timing.timeout,
                'delay_after': operation.timing.delay_after
            },
            'success': success,
            'details': details
        }
        
        self.test_logs[self.current_test_id]['operation_logs'].append(operation_log)
        
        logger.info(f"Logged operation: {operation.type.value} on {operation.target_node} - {'SUCCESS' if success else 'FAILED'}")
        self._write_log_to_disk(self.current_test_id)
    
    def log_chaos_event(self, chaos_result: ChaosResult) -> None:
        """Log a chaos injection event with result details."""
        if not self.current_test_id:
            logger.warning("No active test to log chaos event to")
            return
        
        chaos_log = {
            'chaos_id': chaos_result.chaos_id,
            'chaos_type': chaos_result.chaos_type.value,
            'target_node': chaos_result.target_node,
            'success': chaos_result.success,
            'start_time': chaos_result.start_time,
            'start_timestamp': datetime.fromtimestamp(chaos_result.start_time).isoformat(),
            'end_time': chaos_result.end_time,
            'end_timestamp': datetime.fromtimestamp(chaos_result.end_time).isoformat() if chaos_result.end_time else None,
            'duration': chaos_result.end_time - chaos_result.start_time if chaos_result.end_time else None,
            'error_message': chaos_result.error_message
        }
        
        self.test_logs[self.current_test_id]['chaos_events'].append(chaos_log)
        
        logger.info(f"Logged chaos event: {chaos_result.chaos_type.value} on {chaos_result.target_node} - {'SUCCESS' if chaos_result.success else 'FAILED'}")
        self._write_log_to_disk(self.current_test_id)
    
    def log_validation_result(self, validation_result: ValidationResult) -> None:
        """Log a cluster state validation result."""
        if not self.current_test_id:
            logger.warning("No active test to log validation result to")
            return
        
        validation_log = {
            'timestamp': validation_result.validation_timestamp,
            'datetime': datetime.fromtimestamp(validation_result.validation_timestamp).isoformat(),
            'slot_coverage': validation_result.slot_coverage,
            'slot_conflicts': [
                {
                    'slot': conflict.slot,
                    'conflicting_nodes': conflict.conflicting_nodes
                }
                for conflict in validation_result.slot_conflicts
            ],
            'replica_sync': {
                'all_replicas_synced': validation_result.replica_sync.all_replicas_synced,
                'max_lag': validation_result.replica_sync.max_lag,
                'lagging_replicas': validation_result.replica_sync.lagging_replicas
            },
            'node_connectivity': {
                'all_nodes_connected': validation_result.node_connectivity.all_nodes_connected,
                'disconnected_nodes': validation_result.node_connectivity.disconnected_nodes,
                'partition_groups': validation_result.node_connectivity.partition_groups
            },
            'data_consistency': {
                'consistent': validation_result.data_consistency.consistent,
                'inconsistent_keys': validation_result.data_consistency.inconsistent_keys,
                'node_data_mismatches': validation_result.data_consistency.node_data_mismatches
            },
            'convergence_time': validation_result.convergence_time,
            'replication_lag': validation_result.replication_lag
        }
        
        self.test_logs[self.current_test_id]['validation_results'].append(validation_log)
        
        logger.info(f"Logged validation result: slot_coverage={validation_result.slot_coverage}, "
                   f"replicas_synced={validation_result.replica_sync.all_replicas_synced}")
        self._write_log_to_disk(self.current_test_id)
    
    def log_cluster_state_snapshot(self, cluster_status: ClusterStatus, label: str = "") -> None:
        """Log a snapshot of cluster state at a specific point in time."""
        if not self.current_test_id:
            logger.warning("No active test to log cluster state to")
            return
        
        snapshot = {
            'timestamp': time.time(),
            'datetime': datetime.now().isoformat(),
            'label': label,
            'cluster_id': cluster_status.cluster_id,
            'is_healthy': cluster_status.is_healthy,
            'formation_complete': cluster_status.formation_complete,
            'total_slots_assigned': cluster_status.total_slots_assigned,
            'nodes': [
                {
                    'node_id': node.node_id,
                    'role': node.role,
                    'shard_id': node.shard_id,
                    'port': node.port,
                    'pid': node.pid,
                    'slot_range': f"{node.slot_start}-{node.slot_end}" if node.slot_start is not None else None
                }
                for node in cluster_status.nodes
            ]
        }
        
        self.test_logs[self.current_test_id]['cluster_state_snapshots'].append(snapshot)
        
        logger.info(f"Logged cluster state snapshot: {label} - healthy={cluster_status.is_healthy}")
        self._write_log_to_disk(self.current_test_id)
    
    def log_error(self, error_message: str, error_details: Optional[Dict[str, Any]] = None) -> None:
        """Log an error that occurred during test execution."""
        if not self.current_test_id:
            logger.warning("No active test to log error to")
            return
        
        error_log = {
            'timestamp': time.time(),
            'datetime': datetime.now().isoformat(),
            'message': error_message,
            'details': error_details or {}
        }
        
        self.test_logs[self.current_test_id]['errors'].append(error_log)
        
        logger.error(f"Logged error: {error_message}")
        self._write_log_to_disk(self.current_test_id)
    
    def log_test_completion(self, test_result: ExecutionResult) -> None:
        """Log the completion of a test scenario with final execution results."""
        if test_result.scenario_id not in self.test_logs:
            logger.warning(f"No log found for test {test_result.scenario_id}")
            return
        
        self.test_logs[test_result.scenario_id].update({
            'end_time': test_result.end_time,
            'end_timestamp': datetime.fromtimestamp(test_result.end_time).isoformat(),
            'duration': test_result.end_time - test_result.start_time,
            'success': test_result.success,
            'operations_executed': test_result.operations_executed,
            'final_error_message': test_result.error_message,
            'status': 'completed' if test_result.success else 'failed'
        })
        
        logger.info(f"Completed test {test_result.scenario_id} - {'SUCCESS' if test_result.success else 'FAILED'} "
                   f"(duration: {test_result.end_time - test_result.start_time:.2f}s)")
        
        self._write_log_to_disk(test_result.scenario_id)
        
        if self.current_test_id == test_result.scenario_id:
            self.current_test_id = None
    
    def generate_report(self, test_results: List[ExecutionResult]) -> str:
        """Generate a summary report from multiple test executions."""
        if not test_results:
            return "No test results to report"
        
        total_tests = len(test_results)
        successful_tests = sum(1 for result in test_results if result.success)
        failed_tests = total_tests - successful_tests
        
        total_operations = sum(result.operations_executed for result in test_results)
        total_chaos_events = sum(len(result.chaos_events) for result in test_results)
        
        total_duration = sum(result.end_time - result.start_time for result in test_results)
        avg_duration = total_duration / total_tests if total_tests > 0 else 0
        
        report_lines = [
            "=" * 80,
            "CLUSTER BUS FUZZER - TEST EXECUTION REPORT",
            "=" * 80,
            "",
            f"Total Tests:        {total_tests}",
            f"Successful:         {successful_tests} ({successful_tests/total_tests*100:.1f}%)",
            f"Failed:             {failed_tests} ({failed_tests/total_tests*100:.1f}%)",
            "",
            f"Total Operations:   {total_operations}",
            f"Total Chaos Events: {total_chaos_events}",
            "",
            f"Total Duration:     {total_duration:.2f}s",
            f"Average Duration:   {avg_duration:.2f}s",
            "",
            "=" * 80,
            "TEST DETAILS",
            "=" * 80,
            ""
        ]
        
        for result in test_results:
            duration = result.end_time - result.start_time
            status = "PASS" if result.success else "FAIL"
            
            report_lines.extend([
                f"{status} | {result.scenario_id}",
                f"     Duration: {duration:.2f}s | Operations: {result.operations_executed} | "
                f"Chaos Events: {len(result.chaos_events)} | Validations: {len(result.validation_results)}",
            ])
            
            if result.error_message:
                report_lines.append(f"     Error: {result.error_message}")
            
            if result.seed:
                report_lines.append(f"     Seed: {result.seed} (for reproduction)")
            
            report_lines.append("")
        
        report_lines.append("=" * 80)
        
        report = "\n".join(report_lines)
        
        report_file = self.log_dir / f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        report_file.write_text(report)
        logger.info(f"Generated report: {report_file}")
        
        return report
    
    def _serialize_cluster_config(self, config) -> Dict[str, Any]:
        """Serialize cluster configuration to dictionary"""
        return {
            'num_shards': config.num_shards,
            'replicas_per_shard': config.replicas_per_shard,
            'base_port': config.base_port,
            'base_data_dir': config.base_data_dir,
            'valkey_binary': config.valkey_binary,
            'enable_cleanup': config.enable_cleanup
        }
    
    def _serialize_operations(self, operations: List[Operation]) -> List[Dict[str, Any]]:
        """Serialize operations to list of dictionaries"""
        return [
            {
                'type': op.type.value,
                'target_node': op.target_node,
                'parameters': op.parameters,
                'timing': {
                    'delay_before': op.timing.delay_before,
                    'timeout': op.timing.timeout,
                    'delay_after': op.timing.delay_after
                }
            }
            for op in operations
        ]
    
    def _serialize_chaos_config(self, config) -> Dict[str, Any]:
        """Serialize chaos configuration to dictionary"""
        return {
            'chaos_type': config.chaos_type.value,
            'target_selection': {
                'strategy': config.target_selection.strategy,
                'specific_nodes': config.target_selection.specific_nodes
            },
            'timing': {
                'delay_before_operation': config.timing.delay_before_operation,
                'delay_after_operation': config.timing.delay_after_operation,
                'chaos_duration': config.timing.chaos_duration
            },
            'coordination': {
                'chaos_before_operation': config.coordination.chaos_before_operation,
                'chaos_during_operation': config.coordination.chaos_during_operation,
                'chaos_after_operation': config.coordination.chaos_after_operation
            },
            'process_chaos_type': config.process_chaos_type.value if config.process_chaos_type else None
        }
    
    def _serialize_validation_config(self, config) -> Dict[str, Any]:
        """Serialize validation configuration to dictionary"""
        return {
            'check_slot_coverage': config.check_slot_coverage,
            'check_slot_conflicts': config.check_slot_conflicts,
            'check_replica_sync': config.check_replica_sync,
            'check_node_connectivity': config.check_node_connectivity,
            'check_data_consistency': config.check_data_consistency,
            'convergence_timeout': config.convergence_timeout,
            'max_replication_lag': config.max_replication_lag
        }
    
    def _log_scenario_summary(self, scenario: Scenario) -> None:
        """Log a human-readable summary of the scenario before execution"""
        summary_lines = [
            "",
            "=" * 80,
            f"SCENARIO SUMMARY: {scenario.scenario_id}",
            "=" * 80,
            f"Seed: {scenario.seed}",
            "",
            "CLUSTER CONFIGURATION:",
            f"  - Shards: {scenario.cluster_config.num_shards}",
            f"  - Replicas per shard: {scenario.cluster_config.replicas_per_shard}",
            f"  - Base port: {scenario.cluster_config.base_port}",
            f"  - Total nodes: {scenario.cluster_config.num_shards * (1 + scenario.cluster_config.replicas_per_shard)}",
            "",
            f"OPERATIONS ({len(scenario.operations)} total):",
        ]
        
        for i, op in enumerate(scenario.operations, 1):
            summary_lines.append(f"  {i}. {op.type.value} on {op.target_node}")
            if op.parameters:
                summary_lines.append(f"     Parameters: {op.parameters}")
            summary_lines.append(f"     Timing: delay_before={op.timing.delay_before:.2f}s, timeout={op.timing.timeout:.2f}s, delay_after={op.timing.delay_after:.2f}s")
        
        summary_lines.extend([
            "",
            "CHAOS CONFIGURATION:",
            f"  - Type: {scenario.chaos_config.chaos_type.value}",
            f"  - Target strategy: {scenario.chaos_config.target_selection.strategy}",
        ])
        
        if scenario.chaos_config.process_chaos_type:
            summary_lines.append(f"  - Process chaos type: {scenario.chaos_config.process_chaos_type.value}")
        
        summary_lines.extend([
            f"  - Chaos timing: before={scenario.chaos_config.coordination.chaos_before_operation}, "
            f"during={scenario.chaos_config.coordination.chaos_during_operation}, "
            f"after={scenario.chaos_config.coordination.chaos_after_operation}",
            f"  - Duration: {scenario.chaos_config.timing.chaos_duration:.2f}s",
            "",
            "=" * 80,
            ""
        ])
        
        for line in summary_lines:
            logger.info(line)
    
    def _write_log_to_disk(self, test_id: str) -> None:
        """Write test log to disk as JSON"""
        if test_id not in self.test_logs:
            return
        
        log_file = self.log_dir / f"{test_id}.json"
        
        try:
            with open(log_file, 'w') as f:
                json.dump(self.test_logs[test_id], f, indent=2)
        except Exception as e:
            logger.error(f"Failed to write log to disk: {e}")
    
    def get_test_log(self, test_id: str) -> Optional[Dict[str, Any]]:
        """Get the log for a specific test, or None if not found."""
        return self.test_logs.get(test_id)
    
    def get_all_test_logs(self) -> Dict[str, Dict[str, Any]]:
        """Get all test logs"""
        return self.test_logs.copy()
