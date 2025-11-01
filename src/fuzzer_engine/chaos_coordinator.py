"""
Chaos Coordinator - Core chaos injection coordination for fuzzer engine

This is the main chaos coordinator used by the fuzzer engine for operation-based
chaos injection. It handles:
- Target node selection
- Timing coordination (before/during/after operations)
- Chaos injection execution
- Chaos history tracking

For scenario-based testing with state management, see chaos_engine.coordinator.
"""
import logging
import time
import random
from typing import Optional, List
from ..models import (
    Operation, ChaosConfig, ChaosResult, NodeInfo, 
    ProcessChaosType, ChaosType, TargetSelection
)
from ..chaos_engine.base import ProcessChaosEngine

logging.basicConfig(format='%(levelname)-5s | %(filename)s:%(lineno)-3d | %(message)s', level=logging.INFO, force=True)
logger = logging.getLogger(__name__)


class ChaosCoordinator:
    """
    Core chaos coordinator for operation-based chaos injection.
    
    This coordinator is used by the fuzzer engine to inject chaos in coordination
    with cluster operations. It provides:
    - Node registration and target selection
    - Timing-based chaos coordination
    - Direct chaos injection
    - History tracking
    
    For scenario-based testing with state management and callbacks,
    use chaos_engine.coordinator.ChaosCoordinator instead.
    """
    
    def __init__(self):
        self.chaos_engine = ProcessChaosEngine()
        self.active_chaos_scenarios: dict[str, List[ChaosResult]] = {}
        self.chaos_history: List[ChaosResult] = []
    
    def register_cluster_nodes(self, cluster_id: str, nodes: List[NodeInfo]) -> None:
        """
        Register cluster nodes with the chaos engine for chaos injection.
        
        Args:
            cluster_id: Unique identifier for the cluster
            nodes: List of nodes in the cluster
        """
        logger.info(f"Registering {len(nodes)} nodes for chaos injection in cluster {cluster_id}")
        
        for node in nodes:
            # Register node process with chaos engine
            self.chaos_engine.register_node_process(node.node_id, node.pid)
        
        # Update cluster topology for target selection
        self.chaos_engine.target_selector.update_cluster_topology(cluster_id, nodes)
        
        logger.info(f"Registered {len(nodes)} nodes for chaos injection")
    
    def coordinate_chaos_with_operation(
        self, 
        operation: Operation, 
        chaos_config: ChaosConfig,
        cluster_nodes: List[NodeInfo]
    ) -> List[ChaosResult]:
        """
        Coordinate chaos injection with a cluster operation based on timing configuration.
        
        Args:
            operation: The cluster operation being executed
            chaos_config: Configuration for chaos injection
            cluster_nodes: List of nodes in the cluster
            
        Returns:
            List of ChaosResult from chaos injections
        """
        chaos_results = []
        
        logger.info(f"Coordinating chaos with operation {operation.type.value} on {operation.target_node}")
        
        try:
            # Select target node for chaos
            target_node = self._select_chaos_target(cluster_nodes, chaos_config.target_selection)
            
            if not target_node:
                logger.warning("No suitable chaos target found")
                return chaos_results
            
            # Execute chaos based on coordination configuration
            coordination = chaos_config.coordination
            timing = chaos_config.timing
            
            # Chaos before operation
            if coordination.chaos_before_operation:
                logger.info(f"Injecting chaos before operation (delay: {timing.delay_before_operation}s)")
                time.sleep(timing.delay_before_operation)
                
                result = self._inject_chaos(target_node, chaos_config)
                chaos_results.append(result)
                
                if result.success:
                    logger.info(f"Pre-operation chaos injected on {target_node.node_id}")
            
            # Chaos during operation (most common for failover testing)
            if coordination.chaos_during_operation:
                logger.info("Chaos will be injected during operation execution")
                # Note: Actual injection happens in execute_chaos_during_operation
                # This is just logging the intent
            
            # Chaos after operation
            if coordination.chaos_after_operation:
                logger.info(f"Injecting chaos after operation (delay: {timing.delay_after_operation}s)")
                time.sleep(timing.delay_after_operation)
                
                result = self._inject_chaos(target_node, chaos_config)
                chaos_results.append(result)
                
                if result.success:
                    logger.info(f"Post-operation chaos injected on {target_node.node_id}")
            
            # Store chaos results for this scenario
            self.chaos_history.extend(chaos_results)
            
        except Exception as e:
            logger.error(f"Failed to coordinate chaos with operation: {e}")
        
        return chaos_results
    
    def execute_chaos_during_operation(
        self,
        target_node: NodeInfo,
        chaos_config: ChaosConfig
    ) -> ChaosResult:
        """
        Execute chaos injection during operation execution.
        This is typically called by the operation orchestrator at the appropriate time.
        
        Args:
            target_node: Node to inject chaos on
            chaos_config: Configuration for chaos injection
            
        Returns:
            ChaosResult from the chaos injection
        """
        logger.info(f"Executing chaos during operation on {target_node.node_id}")
        
        result = self._inject_chaos(target_node, chaos_config)
        self.chaos_history.append(result)
        
        return result
    
    def _inject_chaos(self, target_node: NodeInfo, chaos_config: ChaosConfig) -> ChaosResult:
        """
        Inject chaos on the target node based on configuration.
        
        Args:
            target_node: Node to inject chaos on
            chaos_config: Configuration for chaos injection
            
        Returns:
            ChaosResult from the chaos injection
        """
        if chaos_config.chaos_type == ChaosType.PROCESS_KILL:
            # Determine process chaos type
            process_chaos_type = chaos_config.process_chaos_type or ProcessChaosType.SIGKILL
            
            logger.info(f"Injecting {process_chaos_type.value} on {target_node.node_id}")
            
            result = self.chaos_engine.inject_process_chaos(target_node, process_chaos_type)
            
            if result.success:
                logger.info(f"Successfully injected chaos on {target_node.node_id}")
            else:
                logger.error(f"Failed to inject chaos on {target_node.node_id}: {result.error_message}")
            
            return result
        else:
            # Future chaos types (network chaos, etc.)
            logger.warning(f"Unsupported chaos type: {chaos_config.chaos_type}")
            return ChaosResult(
                chaos_id="unsupported",
                chaos_type=chaos_config.chaos_type,
                target_node=target_node.node_id,
                success=False,
                start_time=time.time(),
                end_time=time.time(),
                error_message=f"Unsupported chaos type: {chaos_config.chaos_type}"
            )
    
    def _select_chaos_target(
        self, 
        cluster_nodes: List[NodeInfo], 
        target_selection: TargetSelection
    ) -> Optional[NodeInfo]:
        """
        Select a target node for chaos injection based on selection strategy.
        
        Args:
            cluster_nodes: List of nodes in the cluster
            target_selection: Target selection configuration
            
        Returns:
            Selected NodeInfo, or None if no suitable target found
        """
        if not cluster_nodes:
            return None
        
        strategy = target_selection.strategy
        
        if strategy == "specific":
            # Select from specific nodes
            if target_selection.specific_nodes:
                for node in cluster_nodes:
                    if node.node_id in target_selection.specific_nodes:
                        return node
            return None
        
        elif strategy == "primary_only":
            # Select only from primary nodes
            primary_nodes = [node for node in cluster_nodes if node.role == 'primary']
            if primary_nodes:
                return random.choice(primary_nodes)
            return None
        
        elif strategy == "replica_only":
            # Select only from replica nodes
            replica_nodes = [node for node in cluster_nodes if node.role == 'replica']
            if replica_nodes:
                return random.choice(replica_nodes)
            return None
        
        elif strategy == "random":
            # Select randomly from all nodes
            return random.choice(cluster_nodes)
        
        else:
            logger.warning(f"Unknown target selection strategy: {strategy}")
            return None
    
    def get_chaos_history(self) -> List[ChaosResult]:
        """
        Get the history of all chaos injections.
        
        Returns:
            List of all ChaosResult from chaos injections
        """
        return self.chaos_history.copy()
    
    def cleanup_chaos(self, cluster_id: str) -> bool:
        """
        Clean up chaos effects for a cluster.
        
        Args:
            cluster_id: Unique identifier for the cluster
            
        Returns:
            True if cleanup successful, False otherwise
        """
        logger.info(f"Cleaning up chaos for cluster {cluster_id}")
        
        try:
            success = self.chaos_engine.cleanup_chaos(cluster_id)
            
            if cluster_id in self.active_chaos_scenarios:
                del self.active_chaos_scenarios[cluster_id]
            
            logger.info(f"Chaos cleanup completed for cluster {cluster_id}")
            return success
            
        except Exception as e:
            logger.error(f"Failed to cleanup chaos for cluster {cluster_id}: {e}")
            return False
    
    def get_active_chaos_count(self) -> int:
        """Get the number of active chaos injections."""
        return len(self.chaos_engine.active_chaos)
    
    def stop_all_chaos(self) -> None:
        """Stop all active chaos injections."""
        active_chaos_ids = list(self.chaos_engine.active_chaos.keys())
        
        for chaos_id in active_chaos_ids:
            self.chaos_engine.stop_chaos(chaos_id)
        
        logger.info(f"Stopped {len(active_chaos_ids)} active chaos injections")
