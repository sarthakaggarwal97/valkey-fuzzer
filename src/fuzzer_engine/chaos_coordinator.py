"""
Chaos Coordinator - Core chaos injection coordination for fuzzer engine
For scenario-based testing with state management, see chaos_engine.coordinator.
"""
import logging
import time
import random
from typing import Optional, List
from copy import deepcopy
from ..models import (
    Operation, ChaosConfig, ChaosResult, NodeInfo, ChaosCoordination,
    ProcessChaosType, ChaosType, TargetSelection
)
from ..chaos_engine.base import ProcessChaosEngine

logging.basicConfig(format='%(levelname)-5s | %(filename)s:%(lineno)-3d | %(message)s', level=logging.INFO, force=True)
logger = logging.getLogger(__name__)


class ChaosCoordinator:
    def __init__(self):
        self.chaos_engine = ProcessChaosEngine()
        self.active_chaos_scenarios: dict[str, List[ChaosResult]] = {}
        self.chaos_history: List[ChaosResult] = []
    
    def register_cluster_nodes(self, cluster_id: str, nodes: List[NodeInfo]) -> None:
        """
        Register cluster nodes with the chaos engine for chaos injection.
        """
        logger.info(f"Registering {len(nodes)} nodes for chaos injection in cluster {cluster_id}")
        
        for node in nodes:
            # Register node process with chaos engine
            self.chaos_engine.register_node_process(node.node_id, node.pid)
        
        # Update cluster topology for target selection
        self.chaos_engine.target_selector.update_cluster_topology(cluster_id, nodes)
        
        logger.info(f"Successfully registered {len(nodes)} nodes for chaos injection")
    
    def update_node_registration(self, node: NodeInfo) -> None:
        """
        Update the chaos engine registration for a restarted node with its new PID.
        This should be called after a node restart to ensure chaos injections target the correct process.
        """
        logger.info(f"Updating chaos registration for {node.node_id} with new PID {node.pid}")
        self.chaos_engine.register_node_process(node.node_id, node.pid)
        logger.debug(f"Node {node.node_id} chaos registration updated")
    
    def coordinate_chaos_with_operation(
        self, 
        operation: Operation, 
        chaos_config: ChaosConfig,
        cluster_connection,
        cluster_id: str = "default",
        randomize_per_operation: Optional[bool] = None
    ) -> List[ChaosResult]:
        """
        Coordinate chaos injection with a cluster operation based on timing configuration.
        """
        chaos_results = []
        
        logger.info(f"Coordinating chaos with operation {operation.type.value} on {operation.target_node}")
        
        try:
            # Determine if randomization should be enabled
            # Priority: explicit parameter > config flag > default (False)
            should_randomize = randomize_per_operation if randomize_per_operation is not None else chaos_config.randomize_per_operation

            # Randomize chaos configuration for this operation if enabled
            if should_randomize:
                chaos_config = self._randomize_chaos_config(chaos_config)

            # Refresh topology from live cluster connection
            live_nodes_dict = cluster_connection.get_live_nodes()
            if live_nodes_dict:
                # Convert dict nodes to NodeInfo objects for the target selector
                live_nodes = self._convert_dict_nodes_to_nodeinfo(live_nodes_dict, cluster_connection.initial_nodes)
                self.chaos_engine.target_selector.update_cluster_topology(cluster_id, live_nodes)
                logger.debug(f"Updated topology with {len(live_nodes)} live nodes from cluster")
            else:
                logger.warning("No live nodes available from cluster connection")

            # Select target node for chaos using ChaosTargetSelector
            target_node = self.chaos_engine.target_selector.select_target(cluster_id, chaos_config.target_selection)
            
            if not target_node:
                logger.warning("No suitable chaos target found")
                return chaos_results
            
            # Execute chaos based on coordination configuration
            coordination = chaos_config.coordination
            timing = chaos_config.timing
            
            # Add slight randomization to timing if enabled (±20%)
            if should_randomize:
                delay_before = timing.delay_before_operation * random.uniform(0.8, 1.2)
                delay_after = timing.delay_after_operation * random.uniform(0.8, 1.2)
            else:
                delay_before = timing.delay_before_operation
                delay_after = timing.delay_after_operation

            # Chaos before operation
            if coordination.chaos_before_operation:
                logger.info(f"Injecting chaos before operation (delay: {delay_before:.2f}s)")
                time.sleep(delay_before)
                
                result = self._inject_chaos(target_node, chaos_config, should_randomize)
                chaos_results.append(result)
                
                if result.success:
                    logger.debug(f"Pre-operation chaos injected on {target_node.node_id}")
            
            # Chaos during operation (most common for failover testing)
            if coordination.chaos_during_operation:
                logger.info("Injecting chaos during operation execution")
                
                result = self._inject_chaos(target_node, chaos_config, should_randomize)
                chaos_results.append(result)
                
                if result.success:
                    logger.debug(f"During-operation chaos injected on {target_node.node_id}")
            
            # Chaos after operation
            if coordination.chaos_after_operation:
                logger.info(f"Injecting chaos after operation (delay: {delay_after:.2f}s)")
                time.sleep(delay_after)
                
                result = self._inject_chaos(target_node, chaos_config, should_randomize)
                chaos_results.append(result)
                
                if result.success:
                    logger.debug(f"Post-operation chaos injected on {target_node.node_id}")
            
            # Store chaos results for this scenario
            self.chaos_history.extend(chaos_results)
            
        except Exception as e:
            logger.error(f"Failed to coordinate chaos with operation: {e}")
        
        return chaos_results
    
    def execute_chaos_during_operation(
        self,
        target_node: NodeInfo,
        chaos_config: ChaosConfig,
        randomize: bool = False
    ) -> ChaosResult:
        """
        Execute chaos injection during operation execution.
        This is typically called by the operation orchestrator at the appropriate time.
        """
        logger.info(f"Executing chaos during operation on {target_node.node_id}")
        
        result = self._inject_chaos(target_node, chaos_config, randomize)
        self.chaos_history.append(result)
        
        return result
    
    def _randomize_chaos_config(self, chaos_config: ChaosConfig) -> ChaosConfig:
        """Create a randomized copy of the chaos config for this operation."""
        # Create a copy to avoid modifying the original
        randomized_config = deepcopy(chaos_config)

        # Randomize process chaos type (if process kill chaos)
        if randomized_config.chaos_type == ChaosType.PROCESS_KILL:
            # 50/50 chance between SIGKILL and SIGTERM
            randomized_config.process_chaos_type = random.choice([
                ProcessChaosType.SIGKILL,
                ProcessChaosType.SIGTERM
            ])
            logger.info(f"Randomized chaos type: {randomized_config.process_chaos_type.value}")

        # Randomize chaos timing coordination (30% chance for each timing option)
        # At least one must be True
        chaos_before = random.random() < 0.3
        chaos_during = random.random() < 0.5  # Higher probability for during
        chaos_after = random.random() < 0.3

        # Ensure at least one is True
        if not (chaos_before or chaos_during or chaos_after):
            chaos_during = True

        randomized_config.coordination = ChaosCoordination(
            chaos_before_operation=chaos_before,
            chaos_during_operation=chaos_during,
            chaos_after_operation=chaos_after
        )

        timing_desc = []
        if chaos_before:
            timing_desc.append("before")
        if chaos_during:
            timing_desc.append("during")
        if chaos_after:
            timing_desc.append("after")
        logger.info(f"Randomized chaos timing: {', '.join(timing_desc)}")

        # Randomize target selection strategy (if not specific nodes)
        if randomized_config.target_selection.strategy != "specific":
            # Choose randomly between different strategies
            strategies = ["random", "primary_only", "replica_only"]
            new_strategy = random.choice(strategies)

            randomized_config.target_selection = TargetSelection(
                strategy=new_strategy,
                specific_nodes=None
            )
            logger.info(f"Randomized target strategy: {new_strategy}")

        return randomized_config

    def _inject_chaos(
        self,
        target_node: NodeInfo,
        chaos_config: ChaosConfig,
        randomize: bool = False
    ) -> ChaosResult:
        """
        Inject chaos on the target node based on configuration.
        """
        if chaos_config.chaos_type == ChaosType.PROCESS_KILL:
            # Use process chaos type from config (may have been randomized)
            if chaos_config.process_chaos_type:
                process_chaos_type = chaos_config.process_chaos_type
            else:
                # Fallback behavior depends on randomization flag
                if randomize:
                    # Randomize when explicitly requested
                    process_chaos_type = random.choice([ProcessChaosType.SIGKILL, ProcessChaosType.SIGTERM])
                    logger.info(f"Randomized fallback chaos type: {process_chaos_type.value}")
                else:
                    # Deterministic default for backward compatibility
                    process_chaos_type = ProcessChaosType.SIGKILL
                    logger.debug(f"Using default chaos type: {process_chaos_type.value}")
            
            logger.info(f"Injecting {process_chaos_type.value} on {target_node.node_id}")
            
            result = self.chaos_engine.inject_process_chaos(target_node, process_chaos_type)

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

    def _convert_dict_nodes_to_nodeinfo(self, dict_nodes: List[dict], initial_nodes: List[NodeInfo]) -> List[NodeInfo]:
        """
        Convert dictionary nodes from ClusterConnection.get_live_nodes() to NodeInfo objects.
        Uses initial_nodes to fill in missing process/pid information.
        """
        # Create a mapping from port to initial NodeInfo for quick lookup
        port_to_nodeinfo = {node.port: node for node in initial_nodes}
        
        converted_nodes = []
        for node_dict in dict_nodes:
            port = node_dict['port']
            
            # Get the initial NodeInfo for this port to retrieve process/pid info
            initial_node = port_to_nodeinfo.get(port)
            
            if initial_node:
                # Create a NodeInfo object with current topology data and initial process info
                node_info = NodeInfo(
                    node_id=initial_node.node_id,  # Keep orchestrator ID for process registration
                    role=node_dict['role'],  # Use current role (may change after failover)
                    shard_id=node_dict.get('shard_id'),  # Use current shard_id
                    port=node_dict['port'],
                    bus_port=initial_node.bus_port,
                    pid=initial_node.pid,  # Use initial PID (may be stale after restart)
                    process=initial_node.process,  # Use initial process handle
                    data_dir=initial_node.data_dir,
                    log_file=initial_node.log_file,
                    cluster_node_id=node_dict['node_id']  # Store Valkey cluster node ID
                )
                converted_nodes.append(node_info)
            else:
                logger.warning(f"No initial node info found for port {port}, skipping node {node_dict['node_id']}")
        
        return converted_nodes

    def get_chaos_history(self) -> List[ChaosResult]:
        """
        Get the history of all chaos injections.
        """
        return self.chaos_history.copy()
    
    def cleanup_chaos(self, cluster_id: str) -> bool:
        """
        Clean up chaos effects for a cluster.
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
