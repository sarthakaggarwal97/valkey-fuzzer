"""
Unit tests for Chaos Coordinator components

Phase 1 Focus:
- ChaosCoordinator: Scenario management and timing coordination
- ChaosScenario: Scenario data structure

Note: ChaosRecoveryManager and ChaosTimingManager tests removed
as these components are not needed for Phase 1 (process chaos).
They will be added back when implementing network chaos in Phase 2+.
"""
import time
import pytest
from unittest.mock import Mock, patch, MagicMock
from src.chaos_engine.coordinator import (
    ChaosCoordinator,
    ChaosScenario,
    ChaosScenarioState
)
from src.models import (
    Operation, OperationType, ChaosConfig, ChaosType, ProcessChaosType,
    TargetSelection, ChaosTiming, ChaosCoordination, NodeInfo, NodeRole,
    ChaosResult
)


class TestChaosCoordinator:
    """Test cases for ChaosCoordinator"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.mock_chaos_engine = Mock()
        self.coordinator = ChaosCoordinator(self.mock_chaos_engine)
        
        self.test_operation = Operation(
            type=OperationType.FAILOVER,
            target_node="test_node",
            parameters={},
            timing=Mock()
        )
        
        self.test_chaos_config = ChaosConfig(
            chaos_type=ChaosType.PROCESS_KILL,
            target_selection=TargetSelection(strategy="random"),
            timing=ChaosTiming(delay_before_operation=1.0, delay_after_operation=2.0),
            coordination=ChaosCoordination(
                chaos_before_operation=True,
                chaos_during_operation=True,
                chaos_after_operation=True
            ),
            process_chaos_type=ProcessChaosType.SIGKILL
        )
        
        self.test_node = NodeInfo(
            node_id="test_node_1",
            host="localhost",
            port=7000,
            role=NodeRole.PRIMARY,
            slots=[0, 1, 2],
            is_healthy=True
        )
    
    def test_init(self):
        """Test coordinator initialization"""
        assert self.coordinator.chaos_engine == self.mock_chaos_engine
        assert self.coordinator.active_scenarios == {}
        assert self.coordinator.scenario_history == []
        assert self.coordinator.operation_callbacks == {}
    
    def test_create_scenario(self):
        """Test creating a chaos scenario"""
        scenario = self.coordinator.create_scenario(
            self.test_operation, 
            self.test_chaos_config, 
            self.test_node
        )
        
        assert scenario.operation == self.test_operation
        assert scenario.chaos_config == self.test_chaos_config
        assert scenario.target_node == self.test_node
        assert scenario.state == ChaosScenarioState.PENDING
        assert scenario.scenario_id in self.coordinator.active_scenarios
    
    @patch('time.time')
    @patch('time.sleep')
    def test_execute_scenario_success(self, mock_sleep, mock_time):
        """Test successful scenario execution"""
        mock_time.return_value = 1234567890.0
        
        # Mock chaos injection
        mock_chaos_result = ChaosResult(
            chaos_id="test_chaos",
            chaos_type=ChaosType.PROCESS_KILL,
            target_node=self.test_node.node_id,
            success=True,
            start_time=1234567890.0
        )
        self.mock_chaos_engine.inject_process_chaos.return_value = mock_chaos_result
        
        scenario = self.coordinator.create_scenario(
            self.test_operation,
            self.test_chaos_config,
            self.test_node
        )
        
        result = self.coordinator.execute_scenario(scenario)
        
        assert result.state == ChaosScenarioState.COMPLETED
        assert result.start_time == 1234567890.0
        assert result.end_time == 1234567890.0
        assert len(result.chaos_results) == 3  # before, during, after
        assert result.scenario_id not in self.coordinator.active_scenarios
        assert result in self.coordinator.scenario_history
    
    @patch('time.time')
    def test_execute_scenario_failure(self, mock_time):
        """Test scenario execution with failure"""
        mock_time.return_value = 1234567890.0
        
        # Mock chaos injection to raise exception
        self.mock_chaos_engine.inject_process_chaos.side_effect = Exception("Test error")
        
        scenario = self.coordinator.create_scenario(
            self.test_operation,
            self.test_chaos_config,
            self.test_node
        )
        
        result = self.coordinator.execute_scenario(scenario)
        
        assert result.state == ChaosScenarioState.FAILED
        assert result.error_message == "Test error"
        assert result.scenario_id not in self.coordinator.active_scenarios
        assert result in self.coordinator.scenario_history
    
    def test_cancel_scenario(self):
        """Test cancelling an active scenario"""
        scenario = self.coordinator.create_scenario(
            self.test_operation,
            self.test_chaos_config,
            self.test_node
        )
        
        # Add a mock chaos result
        chaos_result = ChaosResult(
            chaos_id="test_chaos",
            chaos_type=ChaosType.PROCESS_KILL,
            target_node=self.test_node.node_id,
            success=True,
            start_time=time.time()
        )
        scenario.chaos_results.append(chaos_result)
        
        result = self.coordinator.cancel_scenario(scenario.scenario_id)
        
        assert result is True
        assert scenario.state == ChaosScenarioState.CANCELLED
        assert scenario.scenario_id not in self.coordinator.active_scenarios
        assert scenario in self.coordinator.scenario_history
        self.mock_chaos_engine.stop_chaos.assert_called_once_with("test_chaos")
    
    def test_cancel_scenario_not_found(self):
        """Test cancelling non-existent scenario"""
        result = self.coordinator.cancel_scenario("non_existent")
        assert result is False
    
    def test_cleanup_all_scenarios(self):
        """Test cleaning up all scenarios"""
        # Create multiple scenarios
        scenario1 = self.coordinator.create_scenario(
            self.test_operation, self.test_chaos_config, self.test_node
        )
        scenario2 = self.coordinator.create_scenario(
            self.test_operation, self.test_chaos_config, self.test_node
        )
        
        result = self.coordinator.cleanup_all_scenarios()
        
        assert result is True
        assert len(self.coordinator.active_scenarios) == 0
        assert len(self.coordinator.scenario_history) == 2
        self.mock_chaos_engine.cleanup_chaos.assert_called_once_with("all")
    
    def test_get_scenario_status_active(self):
        """Test getting status of active scenario"""
        scenario = self.coordinator.create_scenario(
            self.test_operation, self.test_chaos_config, self.test_node
        )
        
        result = self.coordinator.get_scenario_status(scenario.scenario_id)
        assert result == scenario
    
    def test_get_scenario_status_history(self):
        """Test getting status of scenario in history"""
        scenario = self.coordinator.create_scenario(
            self.test_operation, self.test_chaos_config, self.test_node
        )
        
        # Move to history
        self.coordinator.cancel_scenario(scenario.scenario_id)
        
        result = self.coordinator.get_scenario_status(scenario.scenario_id)
        assert result == scenario
    
    def test_get_scenario_status_not_found(self):
        """Test getting status of non-existent scenario"""
        result = self.coordinator.get_scenario_status("non_existent")
        assert result is None
    
    def test_register_operation_callback(self):
        """Test registering operation callbacks"""
        callback = Mock()
        self.coordinator.register_operation_callback("failover", callback)
        
        assert "failover" in self.coordinator.operation_callbacks
        assert callback in self.coordinator.operation_callbacks["failover"]
    
    @patch('time.sleep')
    def test_execute_chaos_before_operation(self, mock_sleep):
        """Test executing chaos before operation"""
        mock_chaos_result = ChaosResult(
            chaos_id="test_chaos",
            chaos_type=ChaosType.PROCESS_KILL,
            target_node=self.test_node.node_id,
            success=True,
            start_time=time.time()
        )
        self.mock_chaos_engine.inject_process_chaos.return_value = mock_chaos_result
        
        scenario = ChaosScenario(
            scenario_id="test_scenario",
            operation=self.test_operation,
            chaos_config=self.test_chaos_config,
            target_node=self.test_node
        )
        
        self.coordinator._execute_chaos_before_operation(scenario)
        
        mock_sleep.assert_called()  # Should sleep for delay_before_operation and brief delay
        assert len(scenario.chaos_results) == 1
        assert scenario.chaos_results[0] == mock_chaos_result
    
    def test_execute_chaos_during_operation(self):
        """Test executing chaos during operation"""
        mock_chaos_result = ChaosResult(
            chaos_id="test_chaos",
            chaos_type=ChaosType.PROCESS_KILL,
            target_node=self.test_node.node_id,
            success=True,
            start_time=time.time()
        )
        self.mock_chaos_engine.inject_process_chaos.return_value = mock_chaos_result
        
        scenario = ChaosScenario(
            scenario_id="test_scenario",
            operation=self.test_operation,
            chaos_config=self.test_chaos_config,
            target_node=self.test_node
        )
        
        self.coordinator._execute_chaos_during_operation(scenario)
        
        assert len(scenario.chaos_results) == 1
        assert scenario.chaos_results[0] == mock_chaos_result
    
    @patch('time.sleep')
    def test_execute_chaos_after_operation(self, mock_sleep):
        """Test executing chaos after operation"""
        mock_chaos_result = ChaosResult(
            chaos_id="test_chaos",
            chaos_type=ChaosType.PROCESS_KILL,
            target_node=self.test_node.node_id,
            success=True,
            start_time=time.time()
        )
        self.mock_chaos_engine.inject_process_chaos.return_value = mock_chaos_result
        
        scenario = ChaosScenario(
            scenario_id="test_scenario",
            operation=self.test_operation,
            chaos_config=self.test_chaos_config,
            target_node=self.test_node
        )
        
        self.coordinator._execute_chaos_after_operation(scenario)
        
        mock_sleep.assert_called_with(2.0)  # delay_after_operation
        assert len(scenario.chaos_results) == 1
        assert scenario.chaos_results[0] == mock_chaos_result
    
    def test_inject_chaos_no_target(self):
        """Test chaos injection with no target node"""
        scenario = ChaosScenario(
            scenario_id="test_scenario",
            operation=self.test_operation,
            chaos_config=self.test_chaos_config,
            target_node=None
        )
        
        result = self.coordinator._inject_chaos(scenario)
        assert result is None
    
    def test_inject_chaos_unsupported_type(self):
        """Test chaos injection with unsupported chaos type"""
        # Create config with unsupported chaos type (future extension)
        unsupported_config = ChaosConfig(
            chaos_type=ChaosType.PROCESS_KILL,  # We'll modify this
            target_selection=TargetSelection(strategy="random"),
            timing=ChaosTiming(),
            coordination=ChaosCoordination(),
            process_chaos_type=ProcessChaosType.SIGKILL
        )
        # Simulate unsupported type by changing enum value
        unsupported_config.chaos_type = "NETWORK_PARTITION"  # Not implemented yet
        
        scenario = ChaosScenario(
            scenario_id="test_scenario",
            operation=self.test_operation,
            chaos_config=unsupported_config,
            target_node=self.test_node
        )
        
        result = self.coordinator._inject_chaos(scenario)
        assert result is None
    """Test cases for ChaosRecoveryManager"""
    
