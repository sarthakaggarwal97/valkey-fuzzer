"""
Chaos coordination and scenario management components
"""
import time
import uuid
import logging
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from ..models import (
    Operation, ChaosConfig, ChaosResult, ChaosType, ProcessChaosType,
    NodeInfo, ChaosTiming, ChaosCoordination
)


logger = logging.getLogger(__name__)


class ChaosScenarioState(Enum):
    """States of a chaos scenario"""
    PENDING = "pending"
    ACTIVE = "active"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ChaosScenario:
    """Represents a complete chaos scenario with timing and coordination"""
    scenario_id: str
    operation: Operation
    chaos_config: ChaosConfig
    target_node: Optional[NodeInfo] = None
    state: ChaosScenarioState = ChaosScenarioState.PENDING
    chaos_results: List[ChaosResult] = field(default_factory=list)
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    error_message: Optional[str] = None


class ChaosCoordinator:
    """Coordinates chaos injection with operation execution"""
    
    def __init__(self, chaos_engine):
        self.chaos_engine = chaos_engine
        self.active_scenarios: Dict[str, ChaosScenario] = {}
        self.scenario_history: List[ChaosScenario] = []
        self.operation_callbacks: Dict[str, List[Callable]] = {}
    
    def create_scenario(self, operation: Operation, chaos_config: ChaosConfig, 
                       target_node: Optional[NodeInfo] = None) -> ChaosScenario:
        """Create a new chaos scenario"""
        scenario_id = str(uuid.uuid4())
        scenario = ChaosScenario(
            scenario_id=scenario_id,
            operation=operation,
            chaos_config=chaos_config,
            target_node=target_node
        )
        
        self.active_scenarios[scenario_id] = scenario
        logger.info(f"Created chaos scenario {scenario_id} for operation {operation.type.value}")
        return scenario
    
    def execute_scenario(self, scenario: ChaosScenario) -> ChaosScenario:
        """Execute a chaos scenario with full coordination"""
        scenario.state = ChaosScenarioState.ACTIVE
        scenario.start_time = time.time()
        
        try:
            # Execute chaos coordination based on configuration
            coordination = scenario.chaos_config.coordination
            timing = scenario.chaos_config.timing
            
            if coordination.chaos_before_operation:
                self._execute_chaos_before_operation(scenario)
            
            if coordination.chaos_during_operation:
                self._execute_chaos_during_operation(scenario)
            
            if coordination.chaos_after_operation:
                self._execute_chaos_after_operation(scenario)
            
            scenario.state = ChaosScenarioState.COMPLETED
            scenario.end_time = time.time()
            logger.info(f"Completed chaos scenario {scenario.scenario_id}")
            
        except Exception as e:
            scenario.state = ChaosScenarioState.FAILED
            scenario.end_time = time.time()
            scenario.error_message = str(e)
            logger.error(f"Chaos scenario {scenario.scenario_id} failed: {e}")
        
        finally:
            # Move to history
            if scenario.scenario_id in self.active_scenarios:
                del self.active_scenarios[scenario.scenario_id]
            self.scenario_history.append(scenario)
        
        return scenario
    
    def cancel_scenario(self, scenario_id: str) -> bool:
        """Cancel an active chaos scenario"""
        if scenario_id not in self.active_scenarios:
            logger.warning(f"Scenario {scenario_id} not found in active scenarios")
            return False
        
        scenario = self.active_scenarios[scenario_id]
        scenario.state = ChaosScenarioState.CANCELLED
        scenario.end_time = time.time()
        
        # Stop any active chaos from this scenario
        for chaos_result in scenario.chaos_results:
            if chaos_result.end_time is None:  # Still active
                self.chaos_engine.stop_chaos(chaos_result.chaos_id)
        
        # Move to history
        del self.active_scenarios[scenario_id]
        self.scenario_history.append(scenario)
        
        logger.info(f"Cancelled chaos scenario {scenario_id}")
        return True
    
    def cleanup_all_scenarios(self) -> bool:
        """Clean up all active scenarios"""
        try:
            active_scenario_ids = list(self.active_scenarios.keys())
            for scenario_id in active_scenario_ids:
                self.cancel_scenario(scenario_id)
            
            # Clean up chaos engine
            self.chaos_engine.cleanup_chaos("all")
            
            logger.info("Cleaned up all chaos scenarios")
            return True
        except Exception as e:
            logger.error(f"Failed to cleanup all scenarios: {e}")
            return False
    
    def get_scenario_status(self, scenario_id: str) -> Optional[ChaosScenario]:
        """Get status of a chaos scenario"""
        if scenario_id in self.active_scenarios:
            return self.active_scenarios[scenario_id]
        
        # Check history
        for scenario in self.scenario_history:
            if scenario.scenario_id == scenario_id:
                return scenario
        
        return None
    
    def register_operation_callback(self, operation_type: str, callback: Callable) -> None:
        """Register a callback for operation events"""
        if operation_type not in self.operation_callbacks:
            self.operation_callbacks[operation_type] = []
        self.operation_callbacks[operation_type].append(callback)
    
    def _execute_chaos_before_operation(self, scenario: ChaosScenario) -> None:
        """Execute chaos before operation starts"""
        timing = scenario.chaos_config.timing
        
        if timing.delay_before_operation > 0:
            logger.info(f"Waiting {timing.delay_before_operation}s before chaos injection")
            time.sleep(timing.delay_before_operation)
        
        chaos_result = self._inject_chaos(scenario)
        if chaos_result:
            scenario.chaos_results.append(chaos_result)
        
        # Wait for chaos to take effect before operation
        if chaos_result and chaos_result.success:
            time.sleep(1.0)  # Brief delay for chaos to take effect
    
    def _execute_chaos_during_operation(self, scenario: ChaosScenario) -> None:
        """Execute chaos during operation execution"""
        # This would typically be called by the operation orchestrator
        # For now, we'll inject chaos immediately
        chaos_result = self._inject_chaos(scenario)
        if chaos_result:
            scenario.chaos_results.append(chaos_result)
    
    def _execute_chaos_after_operation(self, scenario: ChaosScenario) -> None:
        """Execute chaos after operation completes"""
        timing = scenario.chaos_config.timing
        
        if timing.delay_after_operation > 0:
            logger.info(f"Waiting {timing.delay_after_operation}s after operation")
            time.sleep(timing.delay_after_operation)
        
        chaos_result = self._inject_chaos(scenario)
        if chaos_result:
            scenario.chaos_results.append(chaos_result)
    
    def _inject_chaos(self, scenario: ChaosScenario) -> Optional[ChaosResult]:
        """Inject chaos based on scenario configuration"""
        if not scenario.target_node:
            logger.warning(f"No target node for chaos injection in scenario {scenario.scenario_id}")
            return None
        
        chaos_config = scenario.chaos_config
        
        if chaos_config.chaos_type == ChaosType.PROCESS_KILL:
            return self.chaos_engine.inject_process_chaos(
                scenario.target_node, 
                chaos_config.process_chaos_type
            )
        else:
            logger.error(f"Unsupported chaos type: {chaos_config.chaos_type}")
            return None


class ChaosRecoveryManager:
    """Manages chaos cleanup and recovery mechanisms"""
    
    def __init__(self, chaos_engine, cluster_orchestrator=None):
        self.chaos_engine = chaos_engine
        self.cluster_orchestrator = cluster_orchestrator
        self.recovery_strategies: Dict[ChaosType, Callable] = {}
        self.recovery_history: List[Dict] = []
    
    def register_recovery_strategy(self, chaos_type: ChaosType, strategy: Callable) -> None:
        """Register a recovery strategy for a chaos type"""
        self.recovery_strategies[chaos_type] = strategy
        logger.info(f"Registered recovery strategy for {chaos_type.value}")
    
    def recover_from_chaos(self, chaos_result: ChaosResult, cluster_id: str) -> bool:
        """Attempt to recover from chaos effects"""
        recovery_start = time.time()
        
        try:
            if chaos_result.chaos_type in self.recovery_strategies:
                strategy = self.recovery_strategies[chaos_result.chaos_type]
                success = strategy(chaos_result, cluster_id)
            else:
                success = self._default_recovery(chaos_result, cluster_id)
            
            recovery_time = time.time() - recovery_start
            
            self.recovery_history.append({
                'chaos_id': chaos_result.chaos_id,
                'chaos_type': chaos_result.chaos_type.value,
                'target_node': chaos_result.target_node,
                'recovery_success': success,
                'recovery_time': recovery_time,
                'timestamp': time.time()
            })
            
            if success:
                logger.info(f"Successfully recovered from chaos {chaos_result.chaos_id} in {recovery_time:.2f}s")
            else:
                logger.warning(f"Failed to recover from chaos {chaos_result.chaos_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"Recovery attempt failed for chaos {chaos_result.chaos_id}: {e}")
            return False
    
    def _default_recovery(self, chaos_result: ChaosResult, cluster_id: str) -> bool:
        """Default recovery strategy"""
        if chaos_result.chaos_type == ChaosType.PROCESS_KILL:
            return self._recover_from_process_kill(chaos_result, cluster_id)
        
        logger.warning(f"No recovery strategy for chaos type {chaos_result.chaos_type}")
        return False
    
    def _recover_from_process_kill(self, chaos_result: ChaosResult, cluster_id: str) -> bool:
        """Recover from process kill chaos"""
        # For process kills, recovery typically involves:
        # 1. Restarting the killed process
        # 2. Waiting for it to rejoin the cluster
        # 3. Validating cluster state
        
        if not self.cluster_orchestrator:
            logger.warning("No cluster orchestrator available for process recovery")
            return False
        
        try:
            # This would restart the node process
            # For now, we'll simulate recovery
            logger.info(f"Attempting to recover node {chaos_result.target_node}")
            
            # Simulate recovery time
            time.sleep(2.0)
            
            # In a real implementation, this would:
            # 1. Restart the Valkey process
            # 2. Wait for cluster convergence
            # 3. Validate cluster health
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to recover from process kill: {e}")
            return False
    
    def get_recovery_statistics(self) -> Dict:
        """Get recovery statistics"""
        if not self.recovery_history:
            return {
                'total_recoveries': 0,
                'successful_recoveries': 0,
                'success_rate': 0.0,
                'average_recovery_time': 0.0
            }
        
        total = len(self.recovery_history)
        successful = sum(1 for r in self.recovery_history if r['recovery_success'])
        success_rate = successful / total if total > 0 else 0.0
        avg_time = sum(r['recovery_time'] for r in self.recovery_history) / total
        
        return {
            'total_recoveries': total,
            'successful_recoveries': successful,
            'success_rate': success_rate,
            'average_recovery_time': avg_time
        }


class ChaosTimingManager:
    """Manages precise timing coordination for chaos injection"""
    
    def __init__(self):
        self.timing_events: Dict[str, Dict] = {}
        self.synchronization_points: Dict[str, float] = {}
    
    def schedule_chaos_event(self, event_id: str, delay: float, callback: Callable) -> None:
        """Schedule a chaos event with precise timing"""
        execution_time = time.time() + delay
        
        self.timing_events[event_id] = {
            'execution_time': execution_time,
            'callback': callback,
            'scheduled_at': time.time(),
            'executed': False
        }
        
        logger.debug(f"Scheduled chaos event {event_id} for execution in {delay}s")
    
    def execute_scheduled_events(self) -> List[str]:
        """Execute any scheduled events that are due"""
        current_time = time.time()
        executed_events = []
        
        for event_id, event_info in list(self.timing_events.items()):
            if not event_info['executed'] and current_time >= event_info['execution_time']:
                try:
                    event_info['callback']()
                    event_info['executed'] = True
                    executed_events.append(event_id)
                    logger.debug(f"Executed scheduled chaos event {event_id}")
                except Exception as e:
                    logger.error(f"Failed to execute chaos event {event_id}: {e}")
        
        # Clean up executed events
        for event_id in executed_events:
            if event_id in self.timing_events:
                del self.timing_events[event_id]
        
        return executed_events
    
    def set_synchronization_point(self, point_name: str) -> None:
        """Set a synchronization point for coordinated timing"""
        self.synchronization_points[point_name] = time.time()
        logger.debug(f"Set synchronization point: {point_name}")
    
    def wait_for_synchronization_point(self, point_name: str, timeout: float = 30.0) -> bool:
        """Wait for a synchronization point to be set"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if point_name in self.synchronization_points:
                return True
            time.sleep(0.1)
        
        logger.warning(f"Timeout waiting for synchronization point: {point_name}")
        return False
    
    def clear_synchronization_point(self, point_name: str) -> None:
        """Clear a synchronization point"""
        if point_name in self.synchronization_points:
            del self.synchronization_points[point_name]
            logger.debug(f"Cleared synchronization point: {point_name}")