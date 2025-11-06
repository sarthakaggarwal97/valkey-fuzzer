"""
Chaos coordination and scenario management components

This module provides high-level orchestration for chaos injection,
coordinating chaos timing with cluster operations.

- Scenario-based chaos management
- Timing coordination (before/during/after operations)
- Process chaos injection coordination

This is a higher-level wrapper around the core chaos coordinator
in fuzzer_engine.chaos_coordinator for scenario-based testing.
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

logging.basicConfig(format='%(levelname)-5s | %(filename)s:%(lineno)-3d | %(message)s', level=logging.INFO, force=True)
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
    """Scenario-based chaos coordinator for advanced testing"""
    
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
            
            # Check if any chaos was successfully injected
            successful_chaos = [r for r in scenario.chaos_results if r and r.success]
            
            if not scenario.chaos_results:
                # No chaos was attempted
                scenario.state = ChaosScenarioState.FAILED
                scenario.error_message = "No chaos was injected (no target node or no chaos configured)"
                logger.warning(f"Chaos scenario {scenario.scenario_id} failed: no chaos injected")
            elif not successful_chaos:
                # Chaos was attempted but all failed
                scenario.state = ChaosScenarioState.FAILED
                scenario.error_message = "All chaos injection attempts failed"
                logger.warning(f"Chaos scenario {scenario.scenario_id} failed: all chaos attempts unsuccessful")
            else:
                # At least one chaos injection succeeded
                scenario.state = ChaosScenarioState.COMPLETED
                logger.info(f"Completed chaos scenario {scenario.scenario_id} with {len(successful_chaos)}/{len(scenario.chaos_results)} successful chaos injections")
            
            scenario.end_time = time.time()
            
        except Exception as e:
            scenario.state = ChaosScenarioState.FAILED
            scenario.end_time = time.time()
            scenario.error_message = f"Exception during scenario execution: {str(e)}"
            logger.error(f"Chaos scenario {scenario.scenario_id} failed with exception: {e}")
        
        finally:
            # Move to history
            if scenario.scenario_id in self.active_scenarios:
                del self.active_scenarios[scenario.scenario_id]
            self.scenario_history.append(scenario)
        
        return scenario
    
    def cancel_scenario(self, scenario_id: str) -> bool:
        """Cancel an active chaos scenario"""
        if scenario_id not in self.active_scenarios:
            logger.warning(f"Can't cancel scenario {scenario_id} because it's not an active scenario")
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
            logger.info(f"Waiting {timing.delay_before_operation:.2f}s before chaos injection")
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
            logger.info(f"Waiting {timing.delay_after_operation:.2f}s after operation")
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
            process_chaos_type = chaos_config.process_chaos_type or ProcessChaosType.SIGKILL
            return self.chaos_engine.inject_process_chaos(
                scenario.target_node, 
                process_chaos_type
            )
        else:
            logger.error(f"Unsupported chaos type: {chaos_config.chaos_type}")
            return ChaosResult(
                chaos_id="unsupported",
                chaos_type=chaos_config.chaos_type,
                target_node=scenario.target_node.node_id,
                success=False,
                start_time=time.time(),
                end_time=time.time(),
                error_message=f"Unsupported chaos type: {chaos_config.chaos_type}"
            )