"""
Unit tests for Chaos Coordinator components
"""
import time
import pytest
from unittest.mock import Mock, patch, MagicMock
from src.chaos_engine.coordinator import (
    ChaosCoordinator, ChaosRecoveryManager, ChaosTimingManager,
    ChaosScenario, ChaosScenarioState
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


class TestChaosRecoveryManager:
    """Test cases for ChaosRecoveryManager"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.mock_chaos_engine = Mock()
        self.mock_cluster_orchestrator = Mock()
        self.recovery_manager = ChaosRecoveryManager(
            self.mock_chaos_engine, 
            self.mock_cluster_orchestrator
        )
        
        self.test_chaos_result = ChaosResult(
            chaos_id="test_chaos",
            chaos_type=ChaosType.PROCESS_KILL,
            target_node="test_node",
            success=True,
            start_time=time.time()
        )
    
    def test_init(self):
        """Test recovery manager initialization"""
        assert self.recovery_manager.chaos_engine == self.mock_chaos_engine
        assert self.recovery_manager.cluster_orchestrator == self.mock_cluster_orchestrator
        assert self.recovery_manager.recovery_strategies == {}
        assert self.recovery_manager.recovery_history == []
    
    def test_register_recovery_strategy(self):
        """Test registering recovery strategy"""
        strategy = Mock()
        self.recovery_manager.register_recovery_strategy(ChaosType.PROCESS_KILL, strategy)
        
        assert ChaosType.PROCESS_KILL in self.recovery_manager.recovery_strategies
        assert self.recovery_manager.recovery_strategies[ChaosType.PROCESS_KILL] == strategy
    
    @patch('time.time')
    def test_recover_from_chaos_with_strategy(self, mock_time):
        """Test recovery with registered strategy"""
        mock_time.return_value = 1234567890.0
        
        strategy = Mock(return_value=True)
        self.recovery_manager.register_recovery_strategy(ChaosType.PROCESS_KILL, strategy)
        
        result = self.recovery_manager.recover_from_chaos(self.test_chaos_result, "test_cluster")
        
        assert result is True
        strategy.assert_called_once_with(self.test_chaos_result, "test_cluster")
        assert len(self.recovery_manager.recovery_history) == 1
        
        history_entry = self.recovery_manager.recovery_history[0]
        assert history_entry['chaos_id'] == "test_chaos"
        assert history_entry['recovery_success'] is True
    
    @patch('time.time')
    @patch('time.sleep')
    def test_recover_from_chaos_default_strategy(self, mock_sleep, mock_time):
        """Test recovery with default strategy"""
        mock_time.return_value = 1234567890.0
        
        result = self.recovery_manager.recover_from_chaos(self.test_chaos_result, "test_cluster")
        
        assert result is True  # Default process kill recovery
        mock_sleep.assert_called_with(2.0)  # Recovery simulation
        assert len(self.recovery_manager.recovery_history) == 1
    
    def test_recover_from_chaos_no_orchestrator(self):
        """Test recovery without cluster orchestrator"""
        recovery_manager = ChaosRecoveryManager(self.mock_chaos_engine, None)
        
        result = recovery_manager.recover_from_chaos(self.test_chaos_result, "test_cluster")
        
        assert result is False
        assert len(recovery_manager.recovery_history) == 1
        assert recovery_manager.recovery_history[0]['recovery_success'] is False
    
    @patch('time.time')
    def test_recover_from_chaos_exception(self, mock_time):
        """Test recovery with exception"""
        mock_time.return_value = 1234567890.0
        
        strategy = Mock(side_effect=Exception("Recovery failed"))
        self.recovery_manager.register_recovery_strategy(ChaosType.PROCESS_KILL, strategy)
        
        result = self.recovery_manager.recover_from_chaos(self.test_chaos_result, "test_cluster")
        
        assert result is False
    
    def test_get_recovery_statistics_empty(self):
        """Test getting statistics with no history"""
        stats = self.recovery_manager.get_recovery_statistics()
        
        assert stats['total_recoveries'] == 0
        assert stats['successful_recoveries'] == 0
        assert stats['success_rate'] == 0.0
        assert stats['average_recovery_time'] == 0.0
    
    @patch('time.time')
    def test_get_recovery_statistics_with_data(self, mock_time):
        """Test getting statistics with recovery history"""
        mock_time.return_value = 1234567890.0
        
        # Add some recovery history
        self.recovery_manager.recovery_history = [
            {'recovery_success': True, 'recovery_time': 1.0},
            {'recovery_success': False, 'recovery_time': 2.0},
            {'recovery_success': True, 'recovery_time': 3.0}
        ]
        
        stats = self.recovery_manager.get_recovery_statistics()
        
        assert stats['total_recoveries'] == 3
        assert stats['successful_recoveries'] == 2
        assert stats['success_rate'] == 2/3
        assert stats['average_recovery_time'] == 2.0


class TestChaosTimingManager:
    """Test cases for ChaosTimingManager"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.timing_manager = ChaosTimingManager()
    
    def test_init(self):
        """Test timing manager initialization"""
        assert self.timing_manager.timing_events == {}
        assert self.timing_manager.synchronization_points == {}
    
    @patch('time.time')
    def test_schedule_chaos_event(self, mock_time):
        """Test scheduling chaos event"""
        mock_time.return_value = 1234567890.0
        callback = Mock()
        
        self.timing_manager.schedule_chaos_event("test_event", 5.0, callback)
        
        assert "test_event" in self.timing_manager.timing_events
        event = self.timing_manager.timing_events["test_event"]
        assert event['execution_time'] == 1234567895.0  # current + 5.0
        assert event['callback'] == callback
        assert event['executed'] is False
    
    @patch('time.time')
    def test_execute_scheduled_events_due(self, mock_time):
        """Test executing events that are due"""
        callback = Mock()
        
        # Schedule event in the past
        mock_time.return_value = 1234567890.0
        self.timing_manager.schedule_chaos_event("test_event", -1.0, callback)
        
        # Execute events
        mock_time.return_value = 1234567891.0
        executed = self.timing_manager.execute_scheduled_events()
        
        assert "test_event" in executed
        callback.assert_called_once()
        assert "test_event" not in self.timing_manager.timing_events
    
    @patch('time.time')
    def test_execute_scheduled_events_not_due(self, mock_time):
        """Test executing events that are not yet due"""
        callback = Mock()
        
        # Schedule event in the future
        mock_time.return_value = 1234567890.0
        self.timing_manager.schedule_chaos_event("test_event", 10.0, callback)
        
        # Try to execute events
        mock_time.return_value = 1234567891.0  # Still before execution time
        executed = self.timing_manager.execute_scheduled_events()
        
        assert executed == []
        callback.assert_not_called()
        assert "test_event" in self.timing_manager.timing_events
    
    @patch('time.time')
    def test_execute_scheduled_events_exception(self, mock_time):
        """Test executing events with callback exception"""
        callback = Mock(side_effect=Exception("Callback failed"))
        
        # Schedule event in the past
        mock_time.return_value = 1234567890.0
        self.timing_manager.schedule_chaos_event("test_event", -1.0, callback)
        
        # Execute events
        mock_time.return_value = 1234567891.0
        executed = self.timing_manager.execute_scheduled_events()
        
        assert executed == []  # Event not marked as executed due to exception
        callback.assert_called_once()
        assert "test_event" in self.timing_manager.timing_events  # Event still there
    
    @patch('time.time')
    def test_set_synchronization_point(self, mock_time):
        """Test setting synchronization point"""
        mock_time.return_value = 1234567890.0
        
        self.timing_manager.set_synchronization_point("test_point")
        
        assert "test_point" in self.timing_manager.synchronization_points
        assert self.timing_manager.synchronization_points["test_point"] == 1234567890.0
    
    def test_wait_for_synchronization_point_exists(self):
        """Test waiting for existing synchronization point"""
        self.timing_manager.synchronization_points["test_point"] = 1234567890.0
        
        result = self.timing_manager.wait_for_synchronization_point("test_point")
        
        assert result is True
    
    @patch('time.time')
    @patch('time.sleep')
    def test_wait_for_synchronization_point_timeout(self, mock_sleep, mock_time):
        """Test waiting for synchronization point with timeout"""
        # Mock time to simulate timeout after a few iterations
        start_time = 1234567890.0
        # First few calls should be within timeout, then exceed it
        mock_time.side_effect = [
            start_time,           # Initial call
            start_time + 5.0,     # First check - within timeout
            start_time + 10.0,    # Second check - within timeout  
            start_time + 31.0,    # Third check - exceeds timeout
        ] + [start_time + 31.0] * 10  # Additional calls for logging
        
        result = self.timing_manager.wait_for_synchronization_point("test_point", timeout=30.0)
        
        assert result is False
        # Should have called sleep at least once during the wait loop
        mock_sleep.assert_called_with(0.1)
    
    def test_clear_synchronization_point(self):
        """Test clearing synchronization point"""
        self.timing_manager.synchronization_points["test_point"] = 1234567890.0
        
        self.timing_manager.clear_synchronization_point("test_point")
        
        assert "test_point" not in self.timing_manager.synchronization_points
    
    def test_clear_synchronization_point_not_exists(self):
        """Test clearing non-existent synchronization point"""
        # Should not raise exception
        self.timing_manager.clear_synchronization_point("non_existent")
        assert "non_existent" not in self.timing_manager.synchronization_points