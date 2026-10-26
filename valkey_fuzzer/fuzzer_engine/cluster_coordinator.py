"""
Cluster Coordinator - Manages cluster lifecycle and interfaces with Cluster Orchestrator
"""

import logging
from collections.abc import Callable
from typing import Optional

from ..cluster_orchestrator.orchestrator import (
    ClusterManager,
    ConfigurationManager,
    PortManager,
    detect_valkey_sha,
)
from ..models import ClusterConfig, ClusterInstance, ClusterStatus, NodeInfo, NodePlan, TopologyType

logger = logging.getLogger()
NodePlanConfigurator = Callable[[list[NodePlan]], None]


class ClusterCoordinator:
    """
    Coordinates cluster lifecycle management by interfacing with Cluster Orchestrator.
    Provides high-level cluster operations for the Fuzzer Engine.
    """

    def __init__(self):
        self.active_clusters: dict[str, ClusterInstance] = {}
        self.cluster_manager = ClusterManager()

    def create_cluster(
        self,
        config: ClusterConfig,
        seed: Optional[int] = None,
        *,
        configure_node_plans: Optional[NodePlanConfigurator] = None,
    ) -> ClusterInstance:
        """Create a new Valkey deployment with the specified topology."""
        topology_type = TopologyType(config.topology_type)
        logger.info(
            "Creating %s topology with %s shard(s), %s replica(s) per shard",
            topology_type.value,
            config.num_shards,
            config.replicas_per_shard,
        )

        config_manager = None
        nodes = []

        try:
            # Create a fresh PortManager for this cluster using config's base_port
            port_manager = PortManager(base_port=config.base_port)

            # Initialize configuration manager with seed for log file naming
            config_manager = ConfigurationManager(config, port_manager, seed=seed)

            # Setup Valkey binary
            valkey_binary = config_manager.setup_valkey_from_source()
            config.valkey_binary = valkey_binary
            config_manager.resolve_node_binaries()
            logger.debug(f"Using Valkey binary: {valkey_binary}")

            valkey_sha = detect_valkey_sha(valkey_binary)
            if valkey_sha:
                logger.info(f"Valkey binary built from commit: {valkey_sha}")
            else:
                logger.debug("Could not determine valkey-server build SHA")

            # Plan topology
            node_plans = config_manager.plan_topology()
            if configure_node_plans is not None:
                configure_result = configure_node_plans(node_plans)
                if configure_result is not None:
                    raise TypeError("configure_node_plans must mutate plans in place and return None")

            # Spawn all nodes
            nodes = config_manager.spawn_all_nodes(node_plans)

            # Form the selected topology. form_cluster remains the compatibility
            # entry point and dispatches internally for non-cluster deployments.
            cluster_connection = self.cluster_manager.form_cluster(
                nodes,
                config_manager.cluster_id,
                topology_type=topology_type,
            )

            if not cluster_connection:
                raise Exception(f"Failed to form {topology_type.value} topology")

            # Create cluster instance
            cluster_instance = ClusterInstance(
                cluster_id=cluster_connection.cluster_id,
                config=config,
                nodes=nodes,
                is_ready=True,
                valkey_sha=valkey_sha,
            )

            # Store cluster instance and config manager for later cleanup
            self.active_clusters[cluster_instance.cluster_id] = {
                "instance": cluster_instance,
                "config_manager": config_manager,
            }

            return cluster_instance

        except BaseException as e:
            logger.error(f"Failed to create cluster: {e}")

            # A failed attempt must not leave clients cached for node IDs that
            # a later attempt will reuse.
            try:
                self.cluster_manager.close_connections()
            except BaseException as cleanup_error:
                logger.error(f"Error while closing clients after creation failure: {cleanup_error}")

            # Include nodes tracked before spawn_all_nodes returned. This is
            # important when an interruption lands partway through spawning.
            if config_manager:
                cleanup_nodes = {node.node_id: node for node in nodes}
                tracked_nodes = config_manager.spawned_nodes
                if isinstance(tracked_nodes, (list, tuple)):
                    cleanup_nodes.update({node.node_id: node for node in tracked_nodes})

                logger.info("Cleaning up spawned nodes after cluster formation failure")
                try:
                    config_manager.cleanup_cluster(list(cleanup_nodes.values()))
                except BaseException as cleanup_error:
                    logger.error(f"Error during cleanup: {cleanup_error}")

            raise

    def create_topology(
        self,
        config: ClusterConfig,
        seed: Optional[int] = None,
        *,
        configure_node_plans: Optional[NodePlanConfigurator] = None,
    ) -> ClusterInstance:
        """Topology-oriented alias for the backwards-compatible create_cluster API."""
        return self.create_cluster(
            config,
            seed=seed,
            configure_node_plans=configure_node_plans,
        )

    def get_cluster_status(self, cluster_id: str) -> Optional[ClusterStatus]:
        """Get current status of a cluster."""
        if cluster_id not in self.active_clusters:
            logger.warning(f"Cluster {cluster_id} not found")
            return None

        cluster_data = self.active_clusters[cluster_id]
        cluster_instance = cluster_data["instance"]
        topology_type = TopologyType(cluster_instance.config.topology_type)

        try:
            # Filter out dead nodes before validation
            # After chaos, some nodes may be dead - only validate live nodes
            live_nodes = []
            for node in cluster_instance.nodes:
                if node.process and node.process.poll() is None:
                    # Process is still running
                    live_nodes.append(node)

            # If no live nodes, use all nodes (cluster might have restarted nodes)
            nodes_to_validate = live_nodes if live_nodes else cluster_instance.nodes

            # Validate cluster health with live nodes only
            all_expected_nodes_live = len(live_nodes) == len(cluster_instance.nodes)
            if topology_type != TopologyType.CLUSTER and not all_expected_nodes_live:
                is_healthy = False
            else:
                is_healthy = self.cluster_manager.validate_cluster(
                    nodes_to_validate,
                    topology_type=topology_type,
                )

            # Count assigned slots from live primary nodes only
            total_slots = 0
            if topology_type == TopologyType.CLUSTER:
                total_slots = sum(
                    (node.slot_end - node.slot_start + 1)
                    for node in nodes_to_validate
                    if node.role == "primary" and node.slot_start is not None
                )

            status = ClusterStatus(
                cluster_id=cluster_id,
                nodes=cluster_instance.nodes,
                total_slots_assigned=total_slots,
                is_healthy=is_healthy,
                formation_complete=cluster_instance.is_ready,
            )

            return status

        except Exception as e:
            logger.error(f"Failed to get cluster status for {cluster_id}: {e}")
            return None

    def validate_cluster_readiness(self, cluster_id: str) -> bool:
        """Validate that cluster is ready for testing."""
        status = self.get_cluster_status(cluster_id)

        if not status:
            return False

        cluster_instance = self.active_clusters[cluster_id]["instance"]
        topology_type = TopologyType(cluster_instance.config.topology_type)

        # Check all required conditions for readiness
        slots_ready = topology_type != TopologyType.CLUSTER or status.total_slots_assigned == 16384
        is_ready = status.formation_complete and status.is_healthy and slots_ready and len(status.nodes) > 0

        if is_ready:
            logger.info(f"Cluster {cluster_id} is ready for testing")
        else:
            logger.warning(
                f"Cluster {cluster_id} is not ready: "
                f"formation={status.formation_complete}, "
                f"healthy={status.is_healthy}, "
                f"slots={status.total_slots_assigned}, "
                f"topology={topology_type.value}"
            )

        return is_ready

    def get_node_info(self, cluster_id: str, node_id: str) -> Optional[NodeInfo]:
        """Get information about a specific node in the cluster."""
        if cluster_id not in self.active_clusters:
            return None

        cluster_instance = self.active_clusters[cluster_id]["instance"]

        for node in cluster_instance.nodes:
            if node.node_id == node_id:
                return node

        return None

    def get_all_nodes(self, cluster_id: str) -> list[NodeInfo]:
        """Get all nodes in the cluster."""
        if cluster_id not in self.active_clusters:
            return []

        cluster_instance = self.active_clusters[cluster_id]["instance"]
        return cluster_instance.nodes

    def restart_node(self, cluster_id: str, node_id: str, wait_ready: bool = True, chaos_coordinator=None) -> bool:
        """Restart a specific node in the cluster. Optional ChaosCoordinator to update node registration after restart"""
        if cluster_id not in self.active_clusters:
            logger.error(f"Cluster {cluster_id} not found")
            return False

        cluster_data = self.active_clusters[cluster_id]
        config_manager = cluster_data["config_manager"]

        node = self.get_node_info(cluster_id, node_id)
        if not node:
            logger.error(f"Node {node_id} not found in cluster {cluster_id}")
            return False

        try:
            logger.info(f"Restarting node {node_id} in cluster {cluster_id}")

            # Restart the node (this updates node.pid with the new process ID)
            config_manager.restart_node(node, wait_ready=wait_ready)

            logger.info(f"Node {node_id} restarted successfully with new PID {node.pid}")

            # Update chaos engine registration with new PID if chaos_coordinator provided
            if chaos_coordinator:
                chaos_coordinator.update_node_registration(cluster_id, node)
                logger.info(f"Updated chaos registration for restarted node {node_id}")

            return True

        except Exception as e:
            logger.error(f"Failed to restart node {node_id}: {e}")
            return False

    def destroy_cluster(self, cluster_id: str) -> bool:
        """Destroy a cluster, attempting every cleanup action before reporting."""
        if cluster_id not in self.active_clusters:
            logger.warning(f"Cluster {cluster_id} not found")
            return False

        cluster_data = self.active_clusters[cluster_id]
        cluster_instance = cluster_data["instance"]
        config_manager = cluster_data["config_manager"]
        failures: list[tuple[str, BaseException]] = []

        try:
            self.cluster_manager.close_connections()
        except BaseException as exc:
            failures.append(("close client connections", exc))

        try:
            config_manager.cleanup_cluster(cluster_instance.nodes)
        except BaseException as exc:
            failures.append(("clean up node resources", exc))

        if not failures:
            del self.active_clusters[cluster_id]
            return True

        for action, exc in failures:
            logger.error("Failed to destroy cluster %s while attempting to %s: %s", cluster_id, action, exc)

        interrupt = next((exc for _, exc in failures if not isinstance(exc, Exception)), None)
        if interrupt is not None:
            raise interrupt
        return False
