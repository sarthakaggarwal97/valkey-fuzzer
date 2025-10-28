"""
Unit tests for Chaos Engine components
"""
import os
import signal
import time
import pytest
from unittest.mock import Mock, patch, MagicMock
from src.chaos_engine.base import ProcessChaosEngine, ChaosTargetSelector
from src.models import (
    NodeInfo, NodeRole, ProcessChaosType, ChaosType, ChaosConfig,
    TargetSelection, ChaosTiming, ChaosCoordination, Operation, OperationType,
    ChaosResult
)


class TestProcessChaosEngine:
    """Test cases for ProcessChaosEngine"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.chaos_engine = ProcessChaosEngine()
        self.test_node = NodeInfo(
            node_id="test_node_1",
            host="localhost",
            port=7000,
            role=NodeRole.PRIMARY,
            slots=[0, 1, 2],
            is_healthy=True
        )
    
    def test_init(self):
        """Test chaos engine initialization"""
        assert self.chaos_engine.active_chaos == {}
        assert self.chaos_engine.chaos_history == []
        assert self.chaos_engine.coordination_enabled is True
        assert self.chaos_engine.node_processes == {}
    
    def test_register_node_process(self):
        """Test registering node process"""
        self.chaos_engine.register_node_process("test_node", 12345)
        assert self.chaos_engine.node_processes["test_node"] == 12345
    
    def test_unregister_node_process(self):
        """Test unregistering node process"""
        self.chaos_engine.register_node_process("test_node", 12345)
        self.chaos_engine.unregister_node_process("test_node")
        assert "test_node" not in self.chaos_engine.node_processes
    
    def test_validate_chaos_target_valid(self):
        """Test validating valid chaos target"""
        self.chaos_engine.register_node_process(self.test_node.node_id, 12345)
        assert self.chaos_engine._validate_chaos_target(self.test_node) is True
    
    def test_validate_chaos_target_invalid_none(self):
        """Test validating None target"""
        assert self.chaos_engine._validate_chaos_target(None) is False
    
    def test_validate_chaos_target_unhealthy(self):
        """Test validating unhealthy target"""
        unhealthy_node = NodeInfo(
            node_id="unhealthy_node",
            host="localhost",
            port=7001,
            role=NodeRole.PRIMARY,
            slots=[3, 4, 5],
            is_healthy=False
        )
        self.chaos_engine.register_node_process(unhealthy_node.node_id, 12345)
        assert self.chaos_engine._validate_chaos_target(unhealthy_node) is False
    
    def test_validate_chaos_target_no_process(self):
        """Test validating target with no registered process"""
        assert self.chaos_engine._validate_chaos_target(self.test_node) is False
    
    def test_get_node_process_id_exists(self):
        """Test getting process ID for existing node"""
        self.chaos_engine.register_node_process("test_node", 12345)
        assert self.chaos_engine._get_node_process_id(self.test_node) is None  # node_id mismatch
        
        # Test with correct node_id
        self.chaos_engine.register_node_process(self.test_node.node_id, 12345)
        assert self.chaos_engine._get_node_process_id(self.test_node) == 12345
    
    def test_get_node_process_id_not_exists(self):
        """Test getting process ID for non-existing node"""
        assert self.chaos_engine._get_node_process_id(self.test_node) is None
    
    @patch('os.kill')
    def test_execute_process_kill_sigkill_success(self, mock_kill):
        """Test successful SIGKILL execution"""
        mock_kill.return_value = None
        result = self.chaos_engine._execute_process_kill(12345, ProcessChaosType.SIGKILL)
        assert result is True
        mock_kill.assert_called_once_with(12345, signal.SIGKILL)
    
    @patch('os.kill')
    def test_execute_process_kill_sigterm_success(self, mock_kill):
        """Test successful SIGTERM execution"""
        mock_kill.return_value = None
        result = self.chaos_engine._execute_process_kill(12345, ProcessChaosType.SIGTERM)
        assert result is True
        mock_kill.assert_called_once_with(12345, signal.SIGTERM)
    
    @patch('os.kill')
    def test_execute_process_kill_process_not_found(self, mock_kill):
        """Test process kill when process not found"""
        mock_kill.side_effect = ProcessLookupError()
        result = self.chaos_engine._execute_process_kill(12345, ProcessChaosType.SIGKILL)
        assert result is False
    
    @patch('os.kill')
    def test_execute_process_kill_permission_denied(self, mock_kill):
        """Test process kill with permission denied"""
        mock_kill.side_effect = PermissionError()
        result = self.chaos_engine._execute_process_kill(12345, ProcessChaosType.SIGKILL)
        assert result is False
    
    @patch('os.kill')
    def test_execute_process_kill_general_exception(self, mock_kill):
        """Test process kill with general exception"""
        mock_kill.side_effect = Exception("Test error")
        result = self.chaos_engine._execute_process_kill(12345, ProcessChaosType.SIGKILL)
        assert result is False
    
    @patch('time.time')
    @patch.object(ProcessChaosEngine, '_execute_process_kill')
    @patch.object(ProcessChaosEngine, '_get_node_process_id')
    @patch.object(ProcessChaosEngine, '_validate_chaos_target')
    def test_inject_process_chaos_success(self, mock_validate, mock_get_pid, mock_execute, mock_time):
        """Test successful process chaos injection"""
        mock_time.return_value = 1234567890.0
        mock_validate.return_value = True
        mock_get_pid.return_value = 12345
        mock_execute.return_value = True
        
        result = self.chaos_engine.inject_process_chaos(self.test_node, ProcessChaosType.SIGKILL)
        
        assert result.success is True
        assert result.target_node == self.test_node.node_id
        assert result.chaos_type == ChaosType.PROCESS_KILL
        assert result.start_time == 1234567890.0
        assert result.end_time == 1234567890.0
        assert result.chaos_id in self.chaos_engine.active_chaos
        assert len(self.chaos_engine.chaos_history) == 1
    
    @patch('time.time')
    @patch.object(ProcessChaosEngine, '_validate_chaos_target')
    def test_inject_process_chaos_invalid_target(self, mock_validate, mock_time):
        """Test process chaos injection with invalid target"""
        mock_time.return_value = 1234567890.0
        mock_validate.return_value = False
        
        result = self.chaos_engine.inject_process_chaos(self.test_node, ProcessChaosType.SIGKILL)
        
        assert result.success is False
        assert "Invalid chaos target" in result.error_message
        assert len(self.chaos_engine.active_chaos) == 0
        assert len(self.chaos_engine.chaos_history) == 1
    
    @patch('time.time')
    @patch.object(ProcessChaosEngine, '_get_node_process_id')
    @patch.object(ProcessChaosEngine, '_validate_chaos_target')
    def test_inject_process_chaos_no_process(self, mock_validate, mock_get_pid, mock_time):
        """Test process chaos injection when no process found"""
        mock_time.return_value = 1234567890.0
        mock_validate.return_value = True
        mock_get_pid.return_value = None
        
        result = self.chaos_engine.inject_process_chaos(self.test_node, ProcessChaosType.SIGKILL)
        
        assert result.success is False
        assert "Could not find process" in result.error_message
        assert len(self.chaos_engine.active_chaos) == 0
        assert len(self.chaos_engine.chaos_history) == 1
    
    def test_stop_chaos_success(self):
        """Test stopping active chaos"""
        # Add a chaos to active list
        chaos_result = ChaosResult(
            chaos_id="test_chaos",
            chaos_type=ChaosType.PROCESS_KILL,
            target_node="test_node",
            success=True,
            start_time=time.time()
        )
        self.chaos_engine.active_chaos["test_chaos"] = chaos_result
        
        result = self.chaos_engine.stop_chaos("test_chaos")
        assert result is True
        assert "test_chaos" not in self.chaos_engine.active_chaos
        assert chaos_result.end_time is not None
    
    def test_stop_chaos_not_found(self):
        """Test stopping non-existent chaos"""
        result = self.chaos_engine.stop_chaos("non_existent")
        assert result is False
    
    def test_cleanup_chaos(self):
        """Test cleaning up all chaos effects"""
        # Add some active chaos
        chaos_result = ChaosResult(
            chaos_id="test_chaos",
            chaos_type=ChaosType.PROCESS_KILL,
            target_node="test_node",
            success=True,
            start_time=time.time()
        )
        self.chaos_engine.active_chaos["test_chaos"] = chaos_result
        self.chaos_engine.register_node_process("test_node", 12345)
        
        result = self.chaos_engine.cleanup_chaos("test_cluster")
        assert result is True
        assert len(self.chaos_engine.active_chaos) == 0
        assert len(self.chaos_engine.node_processes) == 0
    
    @patch('time.sleep')
    def test_apply_chaos_timing_before_operation(self, mock_sleep):
        """Test applying timing delays before operation"""
        timing = ChaosTiming(delay_before_operation=2.0)
        coordination = ChaosCoordination(chaos_before_operation=True)
        
        self.chaos_engine._apply_chaos_timing(timing, coordination)
        mock_sleep.assert_called_once_with(2.0)
    
    @patch('time.sleep')
    def test_apply_chaos_timing_after_operation(self, mock_sleep):
        """Test applying timing delays after operation"""
        timing = ChaosTiming(delay_after_operation=3.0)
        coordination = ChaosCoordination(chaos_after_operation=True)
        
        self.chaos_engine._apply_chaos_timing(timing, coordination)
        mock_sleep.assert_called_once_with(3.0)
    
    @patch.object(ProcessChaosEngine, '_select_chaos_target')
    @patch.object(ProcessChaosEngine, '_apply_chaos_timing')
    @patch.object(ProcessChaosEngine, 'inject_process_chaos')
    def test_coordinate_with_operation_success(self, mock_inject, mock_timing, mock_select):
        """Test successful chaos coordination with operation"""
        # Setup mocks
        mock_select.return_value = self.test_node
        from src.models import ChaosResult
        mock_inject.return_value = ChaosResult(
            chaos_id="test_chaos",
            chaos_type=ChaosType.PROCESS_KILL,
            target_node=self.test_node.node_id,
            success=True,
            start_time=time.time()
        )
        
        # Create test operation and chaos config
        operation = Operation(
            type=OperationType.FAILOVER,
            target_node="test_node",
            parameters={},
            timing=Mock()
        )
        
        chaos_config = ChaosConfig(
            chaos_type=ChaosType.PROCESS_KILL,
            target_selection=TargetSelection(strategy="random"),
            timing=ChaosTiming(),
            coordination=ChaosCoordination(),
            process_chaos_type=ProcessChaosType.SIGKILL
        )
        
        result = self.chaos_engine.coordinate_with_operation(operation, chaos_config)
        
        assert result.success is True
        mock_select.assert_called_once()
        mock_timing.assert_called_once()
        mock_inject.assert_called_once_with(self.test_node, ProcessChaosType.SIGKILL)
    
    @patch.object(ProcessChaosEngine, '_select_chaos_target')
    def test_coordinate_with_operation_no_target(self, mock_select):
        """Test chaos coordination when no target found"""
        mock_select.return_value = None
        
        operation = Operation(
            type=OperationType.FAILOVER,
            target_node="test_node",
            parameters={},
            timing=Mock()
        )
        
        chaos_config = ChaosConfig(
            chaos_type=ChaosType.PROCESS_KILL,
            target_selection=TargetSelection(strategy="random"),
            timing=ChaosTiming(),
            coordination=ChaosCoordination(),
            process_chaos_type=ProcessChaosType.SIGKILL
        )
        
        result = self.chaos_engine.coordinate_with_operation(operation, chaos_config)
        
        assert result.success is False
        assert "No suitable target node found" in result.error_message
    
    def test_coordinate_with_operation_disabled(self):
        """Test chaos coordination when disabled"""
        self.chaos_engine.coordination_enabled = False
        
        operation = Operation(
            type=OperationType.FAILOVER,
            target_node="test_node",
            parameters={},
            timing=Mock()
        )
        
        chaos_config = ChaosConfig(
            chaos_type=ChaosType.PROCESS_KILL,
            target_selection=TargetSelection(strategy="random"),
            timing=ChaosTiming(),
            coordination=ChaosCoordination(),
            process_chaos_type=ProcessChaosType.SIGKILL
        )
        
        result = self.chaos_engine.coordinate_with_operation(operation, chaos_config)
        
        assert result.success is False
        assert "Chaos coordination is disabled" in result.error_message


class TestChaosTargetSelector:
    """Test cases for ChaosTargetSelector"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.selector = ChaosTargetSelector()
        self.primary_node = NodeInfo(
            node_id="primary_1",
            host="localhost",
            port=7000,
            role=NodeRole.PRIMARY,
            slots=[0, 1, 2],
            is_healthy=True
        )
        self.replica_node = NodeInfo(
            node_id="replica_1",
            host="localhost",
            port=7001,
            role=NodeRole.REPLICA,
            slots=[],
            primary_id="primary_1",
            is_healthy=True
        )
        self.unhealthy_node = NodeInfo(
            node_id="unhealthy_1",
            host="localhost",
            port=7002,
            role=NodeRole.PRIMARY,
            slots=[3, 4, 5],
            is_healthy=False
        )
    
    def test_init(self):
        """Test selector initialization"""
        assert self.selector.cluster_nodes == {}
    
    def test_update_cluster_topology(self):
        """Test updating cluster topology"""
        nodes = [self.primary_node, self.replica_node]
        self.selector.update_cluster_topology("test_cluster", nodes)
        
        assert "test_cluster" in self.selector.cluster_nodes
        assert len(self.selector.cluster_nodes["test_cluster"]) == 2
    
    def test_get_primary_nodes(self):
        """Test getting primary nodes"""
        nodes = [self.primary_node, self.replica_node, self.unhealthy_node]
        self.selector.update_cluster_topology("test_cluster", nodes)
        
        primary_nodes = self.selector.get_primary_nodes("test_cluster")
        assert len(primary_nodes) == 2  # primary_node and unhealthy_node
        assert all(node.role == NodeRole.PRIMARY for node in primary_nodes)
    
    def test_get_replica_nodes(self):
        """Test getting replica nodes"""
        nodes = [self.primary_node, self.replica_node, self.unhealthy_node]
        self.selector.update_cluster_topology("test_cluster", nodes)
        
        replica_nodes = self.selector.get_replica_nodes("test_cluster")
        assert len(replica_nodes) == 1
        assert replica_nodes[0].role == NodeRole.REPLICA
    
    def test_get_healthy_nodes(self):
        """Test getting healthy nodes"""
        nodes = [self.primary_node, self.replica_node, self.unhealthy_node]
        self.selector.update_cluster_topology("test_cluster", nodes)
        
        healthy_nodes = self.selector.get_healthy_nodes("test_cluster")
        assert len(healthy_nodes) == 2  # primary_node and replica_node
        assert all(node.is_healthy for node in healthy_nodes)
    
    def test_get_nodes_nonexistent_cluster(self):
        """Test getting nodes for non-existent cluster"""
        assert self.selector.get_primary_nodes("nonexistent") == []
        assert self.selector.get_replica_nodes("nonexistent") == []
        assert self.selector.get_healthy_nodes("nonexistent") == []
    
    def test_select_target_placeholder(self):
        """Test target selection (placeholder implementation)"""
        operation = Operation(
            type=OperationType.FAILOVER,
            target_node="test_node",
            parameters={},
            timing=Mock()
        )
        
        target_selection = TargetSelection(strategy="random")
        result = self.selector.select_target(operation, target_selection)
        
        # Current implementation returns None (placeholder)
        assert result is None