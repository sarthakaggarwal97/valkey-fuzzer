"""
Base classes for Chaos Engine components
"""
import os
import signal
import time
import uuid
import random
import logging
from abc import ABC
from typing import Dict, List, Optional
from ..interfaces import IChaosEngine
from ..models import (
    NodeInfo, ChaosResult, ChaosType, ProcessChaosType, 
    Operation, ChaosConfig, TargetSelection
)


logger = logging.getLogger(__name__)


class BaseChaosEngine(IChaosEngine, ABC):
    """Base implementation for chaos injection with common functionality"""
    
    def __init__(self):
        self.active_chaos: Dict[str, ChaosResult] = {}
        self.chaos_history: List[ChaosResult] = []
        self.coordination_enabled = True
        self.node_processes: Dict[str, int] = {}  # node_id -> process_id mapping
    
    def inject_process_chaos(self, target_node: NodeInfo, chaos_type: ProcessChaosType) -> ChaosResult:
        """Inject process-level chaos on target node"""
        chaos_id = str(uuid.uuid4())
        start_time = time.time()
        
        chaos_result = ChaosResult(
            chaos_id=chaos_id,
            chaos_type=ChaosType.PROCESS_KILL,
            target_node=target_node.node_id,
            success=False,
            start_time=start_time
        )
        
        try:
            # Validate target node
            if not self._validate_chaos_target(target_node):
                chaos_result.error_message = f"Invalid chaos target: {target_node.node_id}"
                self.chaos_history.append(chaos_result)
                return chaos_result
            
            # Get process ID for the target node
            process_id = self._get_node_process_id(target_node)
            if not process_id:
                chaos_result.error_message = f"Could not find process for node {target_node.node_id}"
                self.chaos_history.append(chaos_result)
                return chaos_result
            
            # Execute process chaos
            success = self._execute_process_kill(process_id, chaos_type)
            
            chaos_result.success = success
            chaos_result.end_time = time.time()
            
            if success:
                logger.info(f"Successfully injected {chaos_type.value} chaos on node {target_node.node_id} (PID: {process_id})")
                self.active_chaos[chaos_id] = chaos_result
            else:
                chaos_result.error_message = f"Failed to kill process {process_id} with {chaos_type.value}"
                logger.error(chaos_result.error_message)
            
        except Exception as e:
            chaos_result.error_message = f"Exception during chaos injection: {str(e)}"
            chaos_result.end_time = time.time()
            logger.error(f"Chaos injection failed: {e}")
        
        self.chaos_history.append(chaos_result)
        return chaos_result
    
    def coordinate_with_operation(self, operation: Operation, chaos_config: ChaosConfig) -> ChaosResult:
        """Coordinate chaos injection with operation execution"""
        if not self.coordination_enabled:
            return ChaosResult(
                chaos_id="disabled",
                chaos_type=chaos_config.chaos_type,
                target_node="none",
                success=False,
                start_time=time.time(),
                error_message="Chaos coordination is disabled"
            )
        
        # Select target node based on configuration
        target_node = self._select_chaos_target(operation, chaos_config.target_selection)
        if not target_node:
            return ChaosResult(
                chaos_id="no_target",
                chaos_type=chaos_config.chaos_type,
                target_node="none",
                success=False,
                start_time=time.time(),
                error_message="No suitable target node found for chaos injection"
            )
        
        # Apply timing coordination
        self._apply_chaos_timing(chaos_config.timing, chaos_config.coordination)
        
        # Execute chaos based on type
        if chaos_config.chaos_type == ChaosType.PROCESS_KILL:
            return self.inject_process_chaos(target_node, chaos_config.process_chaos_type)
        else:
            return ChaosResult(
                chaos_id="unsupported",
                chaos_type=chaos_config.chaos_type,
                target_node=target_node.node_id,
                success=False,
                start_time=time.time(),
                error_message=f"Unsupported chaos type: {chaos_config.chaos_type}"
            )
    
    def stop_chaos(self, chaos_id: str) -> bool:
        """Stop active chaos injection"""
        if chaos_id not in self.active_chaos:
            logger.warning(f"Chaos {chaos_id} not found in active chaos")
            return False
        
        chaos_result = self.active_chaos[chaos_id]
        chaos_result.end_time = time.time()
        
        # For process chaos, there's nothing to actively stop since the process is already killed
        # Future chaos types (like network chaos) might need active cleanup
        
        del self.active_chaos[chaos_id]
        logger.info(f"Stopped chaos {chaos_id}")
        return True
    
    def cleanup_chaos(self, cluster_id: str) -> bool:
        """Clean up any remaining chaos effects"""
        try:
            # Stop all active chaos
            active_chaos_ids = list(self.active_chaos.keys())
            for chaos_id in active_chaos_ids:
                self.stop_chaos(chaos_id)
            
            # Clear process tracking for this cluster
            self.node_processes.clear()
            
            logger.info(f"Cleaned up chaos effects for cluster {cluster_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to cleanup chaos for cluster {cluster_id}: {e}")
            return False
    
    def _validate_chaos_target(self, target_node: NodeInfo) -> bool:
        """Validate that chaos can be injected on target node"""
        if not target_node:
            return False
        
        # Check if node has a valid process ID
        if target_node.node_id not in self.node_processes:
            logger.warning(f"No process ID found for node {target_node.node_id}")
            return False
        
        return True
    
    def _get_node_process_id(self, target_node: NodeInfo) -> Optional[int]:
        """Get the process ID for a target node"""
        # In a real implementation, this would query the actual process
        # For now, we'll simulate by checking our process tracking
        return self.node_processes.get(target_node.node_id)
    
    def _execute_process_kill(self, process_id: int, chaos_type: ProcessChaosType) -> bool:
        """Execute process termination"""
        try:
            if chaos_type == ProcessChaosType.SIGKILL:
                os.kill(process_id, signal.SIGKILL)
            elif chaos_type == ProcessChaosType.SIGTERM:
                os.kill(process_id, signal.SIGTERM)
            else:
                logger.error(f"Unsupported process chaos type: {chaos_type}")
                return False
            
            return True
        except ProcessLookupError:
            logger.warning(f"Process {process_id} not found (may have already exited)")
            return False
        except PermissionError:
            logger.error(f"Permission denied when trying to kill process {process_id}")
            return False
        except Exception as e:
            logger.error(f"Failed to kill process {process_id}: {e}")
            return False
    
    def _select_chaos_target(self, operation: Operation, target_selection: TargetSelection) -> Optional[NodeInfo]:
        """Select target node for chaos injection based on cluster topology"""
        # This is a placeholder implementation
        # In a real implementation, this would query the cluster orchestrator
        # for current cluster topology and select appropriate targets
        
        if target_selection.strategy == "specific" and target_selection.specific_nodes:
            # For specific node selection, we'd need cluster state
            # This is a simplified implementation
            return None
        
        # For now, return None to indicate no target selected
        # This will be properly implemented when cluster orchestrator is available
        return None
    
    def _apply_chaos_timing(self, timing, coordination) -> None:
        """Apply timing delays for chaos coordination"""
        if coordination.chaos_before_operation and timing.delay_before_operation > 0:
            logger.info(f"Waiting {timing.delay_before_operation}s before operation")
            time.sleep(timing.delay_before_operation)
        
        if coordination.chaos_after_operation and timing.delay_after_operation > 0:
            logger.info(f"Waiting {timing.delay_after_operation}s after operation")
            time.sleep(timing.delay_after_operation)
    
    def register_node_process(self, node_id: str, process_id: int) -> None:
        """Register a process ID for a node (for testing purposes)"""
        self.node_processes[node_id] = process_id
        logger.debug(f"Registered process {process_id} for node {node_id}")
    
    def unregister_node_process(self, node_id: str) -> None:
        """Unregister a process ID for a node"""
        if node_id in self.node_processes:
            del self.node_processes[node_id]
            logger.debug(f"Unregistered process for node {node_id}")


class ProcessChaosEngine(BaseChaosEngine):
    """Concrete implementation of process chaos injection"""
    
    def __init__(self):
        super().__init__()
        self.target_selector = ChaosTargetSelector()
    
    def _select_chaos_target(self, operation: Operation, target_selection: TargetSelection) -> Optional[NodeInfo]:
        """Select target node for chaos injection based on cluster topology"""
        return self.target_selector.select_target(operation, target_selection)


class ChaosTargetSelector:
    """Utility class for selecting chaos targets based on cluster topology"""
    
    def __init__(self):
        self.cluster_nodes: Dict[str, List[NodeInfo]] = {}
    
    def update_cluster_topology(self, cluster_id: str, nodes: List[NodeInfo]) -> None:
        """Update cluster topology information"""
        self.cluster_nodes[cluster_id] = nodes
        logger.debug(f"Updated topology for cluster {cluster_id} with {len(nodes)} nodes")
    
    def select_target(self, operation: Operation, target_selection: TargetSelection) -> Optional[NodeInfo]:
        """Select target node based on selection strategy"""
        # For now, this is a placeholder since we don't have cluster orchestrator integration
        # In a real implementation, this would:
        # 1. Get current cluster topology
        # 2. Filter nodes based on strategy (primary_only, replica_only, etc.)
        # 3. Select target based on strategy (random, specific, etc.)
        
        if target_selection.strategy == "specific" and target_selection.specific_nodes:
            # Would select from specific nodes
            pass
        elif target_selection.strategy == "random":
            # Would randomly select from available nodes
            pass
        elif target_selection.strategy == "primary_only":
            # Would select only from primary nodes
            pass
        elif target_selection.strategy == "replica_only":
            # Would select only from replica nodes
            pass
        
        # Return None for now - will be implemented when cluster orchestrator is available
        return None
    
    def get_primary_nodes(self, cluster_id: str) -> List[NodeInfo]:
        """Get all primary nodes in cluster"""
        if cluster_id not in self.cluster_nodes:
            return []
        
        return [node for node in self.cluster_nodes[cluster_id] if node.role == "primary"]
    
    def get_replica_nodes(self, cluster_id: str) -> List[NodeInfo]:
        """Get all replica nodes in cluster"""
        if cluster_id not in self.cluster_nodes:
            return []
        
        return [node for node in self.cluster_nodes[cluster_id] if node.role == "replica"]
    
    def get_healthy_nodes(self, cluster_id: str) -> List[NodeInfo]:
        """Get all healthy nodes in cluster (all nodes with registered processes)"""
        if cluster_id not in self.cluster_nodes:
            return []
        
        # Return all nodes - health tracking not implemented in current NodeInfo model
        return self.cluster_nodes[cluster_id]