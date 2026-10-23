"""Local Valkey topology provider for the generic feature runner."""

import copy
import logging
import threading
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional, Union

from .bound_plan import BoundRunPlan, BoundStartupOverlay
from .cluster_orchestrator.orchestrator import ConfigurationManager, PortManager
from .extensibility import (
    EXTENSION_API_VERSION,
    ProvisionedTopology,
    RunContext,
    RunPlan,
    StartupOverlay,
    TopologyProviderRegistration,
    to_plain_data,
)
from .feature_runner import TopologyProviderFactory
from .fuzzer_engine.cluster_coordinator import ClusterCoordinator
from .models import ClusterConfig, ClusterConnection, ClusterInstance, NodeInfo, NodePlan, TopologyType
from .utils.server_config import (
    canonicalize_server_config,
    server_config_values,
)
from .utils.topology_config import cluster_config_from_topology_document

logger = logging.getLogger(__name__)

LOCAL_TOPOLOGY_SERVICE = "valkey.topology"
LOCAL_TOPOLOGY_PROVIDER_ID = "valkey-fuzzer.local-topology"
LOCAL_TOPOLOGY_PROVIDER_IMPLEMENTATION_VERSION = "1"
_SUPPORTED_CONFLICT_POLICIES = frozenset({"override", "error"})


class LocalTopologyError(RuntimeError):
    """Base class for local topology-provider lifecycle failures."""


class LocalTopologyValidationError(ValueError):
    """A local topology document or startup overlay is invalid."""


@dataclass(frozen=True)
class LocalTopologyNode:
    """Immutable, process-free description of one locally provisioned node."""

    node_id: str
    role: str
    shard_id: int
    host: str
    port: int
    bus_port: int
    slot_start: Optional[int] = None
    slot_end: Optional[int] = None
    master_node_id: Optional[str] = None
    cluster_node_id: Optional[str] = None

    @classmethod
    def from_node_info(cls, node: NodeInfo) -> "LocalTopologyNode":
        return cls(
            node_id=node.node_id,
            role=node.role,
            shard_id=node.shard_id,
            host=node.host,
            port=node.port,
            bus_port=node.bus_port,
            slot_start=node.slot_start,
            slot_end=node.slot_end,
            master_node_id=node.master_node_id,
            cluster_node_id=node.cluster_node_id,
        )


class _LocalTopologyHandleState(str, Enum):
    OPEN = "open"
    CLOSING = "closing"
    CLOSED = "closed"


def _active_cluster_ids(coordinator: ClusterCoordinator) -> Optional[set[str]]:
    active_clusters = getattr(coordinator, "active_clusters", None)
    if not isinstance(active_clusters, Mapping):
        return None
    return {cluster_id for cluster_id in active_clusters if isinstance(cluster_id, str)}


def _exception_type(error: BaseException) -> str:
    error_type = type(error)
    if error_type.__module__ == "builtins":
        return error_type.__qualname__
    return f"{error_type.__module__}.{error_type.__qualname__}"


def _validate_provisioned_instance(
    instance: ClusterInstance,
    expected_config: ClusterConfig,
    expected_plans: Sequence[NodePlan],
) -> None:
    if not isinstance(instance.cluster_id, str) or not instance.cluster_id:
        raise LocalTopologyError("local coordinator returned an empty or invalid topology ID")
    if instance.is_ready is not True:
        raise LocalTopologyError(f"local coordinator returned topology {instance.cluster_id!r} before it was ready")
    if not isinstance(instance.config, ClusterConfig):
        raise LocalTopologyError("local coordinator returned an invalid topology configuration")

    try:
        actual_topology_type = TopologyType(instance.config.topology_type)
    except (TypeError, ValueError) as exc:
        raise LocalTopologyError("local coordinator returned an invalid topology type") from exc

    expected_topology_type = TopologyType(expected_config.topology_type)
    if (
        actual_topology_type is not expected_topology_type
        or instance.config.num_shards != expected_config.num_shards
        or instance.config.replicas_per_shard != expected_config.replicas_per_shard
        or instance.config.base_port != expected_config.base_port
        or instance.config.base_data_dir != expected_config.base_data_dir
        or instance.config.enable_cleanup != expected_config.enable_cleanup
        or instance.config.server_config != expected_config.server_config
        or instance.config.node_configs != expected_config.node_configs
    ):
        raise LocalTopologyError("local coordinator returned a topology that does not match the requested plan")

    try:
        nodes = tuple(instance.nodes)
    except TypeError as exc:
        raise LocalTopologyError("local coordinator returned an invalid node collection") from exc
    if not nodes:
        raise LocalTopologyError("local coordinator returned a topology without nodes")
    if any(not isinstance(node, NodeInfo) for node in nodes):
        raise LocalTopologyError("local coordinator returned a non-NodeInfo topology node")

    node_ids = [node.node_id for node in nodes]
    if len(set(node_ids)) != len(node_ids):
        raise LocalTopologyError("provisioned node IDs must be unique")

    expected_by_id = {plan.node_id: plan for plan in expected_plans}
    expected_node_ids = set(expected_by_id)
    if len(nodes) != len(expected_plans) or set(node_ids) != expected_node_ids:
        raise LocalTopologyError(
            f"local coordinator returned {len(nodes)} nodes with IDs {sorted(node_ids)}, "
            f"expected {len(expected_plans)} nodes with IDs {sorted(expected_node_ids)}"
        )

    nodes_by_id = {node.node_id: node for node in nodes}
    for node_id, expected in expected_by_id.items():
        node = nodes_by_id[node_id]
        if node.host != "127.0.0.1":
            raise LocalTopologyError(f"local node {node_id!r} must use the loopback host")
        if node.port != expected.port or node.bus_port != expected.bus_port:
            raise LocalTopologyError(f"local node {node_id!r} does not use its planned ports")
        if (
            node.shard_id != expected.shard_id
            or node.role != expected.role
            or node.slot_start != expected.slot_start
            or node.slot_end != expected.slot_end
            or node.master_node_id != expected.master_node_id
        ):
            raise LocalTopologyError(
                f"local node {node_id!r} does not match its planned role, shard, slots, or primary"
            )

        try:
            actual_server_config = canonicalize_server_config(node.server_config)
            expected_server_config = canonicalize_server_config(expected.server_config)
            actual_tokens = {option: _value_tokens(option, value) for option, value in actual_server_config.items()}
            expected_tokens = {option: _value_tokens(option, value) for option, value in expected_server_config.items()}
        except (TypeError, ValueError) as exc:
            raise LocalTopologyError(f"local node {node_id!r} returned an invalid server config") from exc
        if actual_tokens != expected_tokens:
            raise LocalTopologyError(f"local node {node_id!r} does not use its planned server config")


def _value_tokens(option: str, value: Any) -> tuple[str, ...]:
    """Return the exact argv values used to compare config semantics."""
    return tuple(server_config_values(option, value))


def _selected_plans(
    node_plans: Sequence[NodePlan],
    overlay: Union[StartupOverlay, BoundStartupOverlay],
) -> tuple[NodePlan, ...]:
    selector = overlay.selector
    known_roles = {plan.role for plan in node_plans}
    known_node_ids = {plan.node_id for plan in node_plans}

    unknown_roles = sorted(set(selector.roles) - known_roles)
    if unknown_roles:
        raise LocalTopologyValidationError(
            f"startup overlay {overlay.overlay_id!r} references unknown roles: {unknown_roles}"
        )

    unknown_node_ids = sorted(set(selector.node_ids) - known_node_ids)
    if unknown_node_ids:
        raise LocalTopologyValidationError(
            f"startup overlay {overlay.overlay_id!r} references unknown node IDs: {unknown_node_ids}"
        )

    selected = tuple(
        plan
        for plan in node_plans
        if (not selector.roles or plan.role in selector.roles)
        and (not selector.node_ids or plan.node_id in selector.node_ids)
    )
    if not selected:
        raise LocalTopologyValidationError(f"startup overlay {overlay.overlay_id!r} does not match any planned node")
    return selected


def apply_startup_overlays(
    node_plans: Sequence[NodePlan],
    overlays: Sequence[Union[StartupOverlay, BoundStartupOverlay]],
) -> None:
    """Atomically apply ordered startup overlays to planned node configs.

    Values within a selector field are alternatives, while non-empty selector
    fields are combined with AND. An empty selector targets every node.
    ``override`` replaces the complete value for an option; ``error`` permits
    an identical value but rejects a different value already supplied by the
    topology config or an earlier overlay.
    """
    plans = tuple(node_plans)
    if any(not isinstance(plan, NodePlan) for plan in plans):
        raise TypeError("node_plans must contain NodePlan values")

    node_ids = [plan.node_id for plan in plans]
    if len(set(node_ids)) != len(node_ids):
        raise LocalTopologyValidationError("planned node IDs must be unique")

    overlay_entries = tuple(overlays)
    if any(not isinstance(overlay, (StartupOverlay, BoundStartupOverlay)) for overlay in overlay_entries):
        raise TypeError("overlays must contain StartupOverlay or BoundStartupOverlay values")

    # Work entirely on detached mappings. No NodePlan is changed unless every
    # selector, policy, conflict, and value has passed validation.
    resolved_configs = {plan.node_id: canonicalize_server_config(dict(plan.server_config)) for plan in plans}

    for overlay in overlay_entries:
        if overlay.selector.labels:
            raise LocalTopologyValidationError(
                f"startup overlay {overlay.overlay_id!r} uses node labels, "
                "which the local topology provider does not support yet"
            )
        if overlay.artifact is not None:
            raise LocalTopologyValidationError(
                f"startup overlay {overlay.overlay_id!r} selects artifact {overlay.artifact!r}, "
                "but per-node artifacts are not supported by the local topology provider yet"
            )
        if overlay.conflict_policy not in _SUPPORTED_CONFLICT_POLICIES:
            raise LocalTopologyValidationError(
                f"startup overlay {overlay.overlay_id!r} has unsupported conflict policy "
                f"{overlay.conflict_policy!r}; expected one of {sorted(_SUPPORTED_CONFLICT_POLICIES)}"
            )

        selected = _selected_plans(plans, overlay)
        overlay_config = canonicalize_server_config(to_plain_data(overlay.server_config))

        for plan in selected:
            resolved = resolved_configs[plan.node_id]
            for option, value in overlay_config.items():
                if overlay.conflict_policy == "error" and option in resolved:
                    if _value_tokens(option, resolved[option]) != _value_tokens(option, value):
                        raise LocalTopologyValidationError(
                            f"startup overlay {overlay.overlay_id!r} conflicts on option "
                            f"{option!r} for node {plan.node_id!r}"
                        )
                    continue
                resolved[option] = value

    for plan in plans:
        plan.server_config = resolved_configs[plan.node_id]


class LocalValkeyTopologyHandle:
    """Narrow component-facing handle for one locally provisioned topology."""

    def __init__(
        self,
        coordinator: ClusterCoordinator,
        instance: ClusterInstance,
    ):
        if not isinstance(instance, ClusterInstance):
            raise TypeError("instance must be a ClusterInstance")
        node_ids = [node.node_id for node in instance.nodes]
        if len(set(node_ids)) != len(node_ids):
            raise LocalTopologyError("provisioned node IDs must be unique")

        self._coordinator = coordinator
        self._instance = instance
        self._node_infos = tuple(instance.nodes)
        self._node_infos_by_id = {node.node_id: node for node in self._node_infos}
        self._nodes = tuple(LocalTopologyNode.from_node_info(node) for node in self._node_infos)
        self._nodes_by_id = {node.node_id: node for node in self._nodes}
        self._connection = ClusterConnection(
            list(instance.nodes),
            instance.cluster_id,
            topology_type=instance.config.topology_type,
        )
        self._state = _LocalTopologyHandleState.OPEN
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._active_operations = 0
        self._operation_owners: dict[int, int] = {}

    @property
    def cluster_id(self) -> str:
        return self._instance.cluster_id

    @property
    def topology_type(self) -> TopologyType:
        return TopologyType(self._instance.config.topology_type)

    @property
    def nodes(self) -> tuple[LocalTopologyNode, ...]:
        with self._lock:
            self._ensure_open()
            return self._nodes

    @property
    def valkey_sha(self) -> Optional[str]:
        return self._instance.valkey_sha

    @property
    def is_closed(self) -> bool:
        with self._lock:
            return self._state is _LocalTopologyHandleState.CLOSED

    @property
    def is_closing(self) -> bool:
        with self._lock:
            return self._state is _LocalTopologyHandleState.CLOSING

    def _ensure_open(self) -> None:
        if self._state is _LocalTopologyHandleState.CLOSING:
            raise LocalTopologyError(f"topology {self.cluster_id!r} is being cleaned up")
        if self._state is _LocalTopologyHandleState.CLOSED:
            raise LocalTopologyError(f"topology {self.cluster_id!r} has already been cleaned up")

    @contextmanager
    def _operation(self):
        owner = threading.get_ident()
        with self._condition:
            self._ensure_open()
            self._active_operations += 1
            self._operation_owners[owner] = self._operation_owners.get(owner, 0) + 1
        try:
            yield
        finally:
            with self._condition:
                self._active_operations -= 1
                remaining = self._operation_owners[owner] - 1
                if remaining:
                    self._operation_owners[owner] = remaining
                else:
                    del self._operation_owners[owner]
                if self._active_operations == 0:
                    self._condition.notify_all()

    def _begin_close(self) -> None:
        owner = threading.get_ident()
        with self._condition:
            self._ensure_open()
            if self._operation_owners.get(owner, 0):
                raise LocalTopologyError("cannot clean up a local topology reentrantly from an active operation")
            self._state = _LocalTopologyHandleState.CLOSING
            while self._active_operations:
                self._condition.wait()

    def _abort_close(self) -> None:
        with self._condition:
            if self._state is _LocalTopologyHandleState.CLOSING:
                self._state = _LocalTopologyHandleState.OPEN
                self._condition.notify_all()

    def _mark_closed(self) -> None:
        with self._condition:
            if self._state is not _LocalTopologyHandleState.CLOSING:
                raise LocalTopologyError("local topology handle was not closing")
            self._state = _LocalTopologyHandleState.CLOSED
            self._condition.notify_all()

    def node(self, node_id: str) -> LocalTopologyNode:
        """Return one initially planned node by its stable harness ID."""
        with self._lock:
            self._ensure_open()
            try:
                return self._nodes_by_id[node_id]
            except KeyError as exc:
                raise KeyError(f"unknown node ID {node_id!r}") from exc

    def current_nodes(self, include_failed: bool = True) -> list[dict]:
        """Return the topology's current authoritative node view."""
        with self._operation():
            return self._connection.get_current_nodes(include_failed=include_failed)

    def execute_command(self, node_id: str, *args: Any, **kwargs: Any) -> Any:
        """Execute one command without exporting a reconnectable raw client."""

        with self._operation():
            try:
                node = self._node_infos_by_id[node_id]
            except KeyError as exc:
                raise KeyError(f"unknown node ID {node_id!r}") from exc
            client = self._coordinator.cluster_manager.get_client(node)
            return client.execute_command(*args, **kwargs)

    def restart(self, node_id: str, wait_ready: bool = True) -> LocalTopologyNode:
        """Restart one node while preserving its resolved startup config."""
        with self._operation():
            if node_id not in self._node_infos_by_id:
                raise KeyError(f"unknown node ID {node_id!r}")
            if not self._coordinator.restart_node(
                self.cluster_id,
                node_id,
                wait_ready=wait_ready,
            ):
                raise LocalTopologyError(f"failed to restart node {node_id!r} in topology {self.cluster_id!r}")
            return self._nodes_by_id[node_id]


class LocalValkeyTopologyProvider:
    """Provision standalone, replication, and cluster topologies locally."""

    def __init__(
        self,
        coordinator_factory: Callable[[], ClusterCoordinator] = ClusterCoordinator,
    ):
        if not callable(coordinator_factory):
            raise TypeError("coordinator_factory must be callable")
        self._coordinator_factory = coordinator_factory
        self._active_coordinator: Optional[ClusterCoordinator] = None
        self._active_topology: Optional[ProvisionedTopology] = None
        self._pending_coordinator: Optional[ClusterCoordinator] = None
        self._pending_cluster_ids: set[str] = set()
        self._pending_baseline_cluster_ids: set[str] = set()

    @staticmethod
    def _prepare(
        plan: Union[RunPlan, BoundRunPlan],
    ) -> tuple[ClusterConfig, tuple[NodePlan, ...]]:
        if not isinstance(plan, (RunPlan, BoundRunPlan)):
            raise TypeError("plan must be a RunPlan or BoundRunPlan")

        topology_document = to_plain_data(plan.topology_document)
        if not isinstance(topology_document, Mapping):  # defensive: RunPlan already enforces this
            raise TypeError("plan.topology_document must be a mapping")
        try:
            config = cluster_config_from_topology_document(
                topology_document,
                strict=True,
            )
            if config.topology_type.value != plan.topology:
                raise LocalTopologyValidationError(
                    f"topology document selects {config.topology_type.value!r}, "
                    f"but the run plan selects {plan.topology!r}"
                )

            # Planning only mutates these throwaway in-memory objects. It does
            # not resolve/build a binary, create directories, spawn processes,
            # or open sockets.
            dry_manager = ConfigurationManager(
                config,
                PortManager(base_port=config.base_port),
                seed=plan.seed,
            )
            dry_plans = dry_manager.plan_topology()
            apply_startup_overlays(dry_plans, plan.startup_overlays)
        except LocalTopologyValidationError:
            raise
        except (TypeError, ValueError) as exc:
            raise LocalTopologyValidationError(str(exc)) from exc
        return config, tuple(copy.deepcopy(dry_plans))

    def validate(self, plan: Union[RunPlan, BoundRunPlan]) -> None:
        """Perform a complete, externally side-effect-free preflight."""
        self._prepare(plan)

    def provision(self, context: RunContext) -> ProvisionedTopology:
        """Provision a preflighted topology, retaining partial state for rollback."""
        if self._active_topology is not None:
            raise LocalTopologyError("this provider already owns a provisioned topology")
        if self._pending_coordinator is not None:
            raise LocalTopologyError("this provider still owns a partial topology; rollback it before provisioning")

        plan = context.plan
        config, expected_plans = self._prepare(plan)
        expected_config = copy.deepcopy(config)
        coordinator = self._coordinator_factory()
        self._pending_baseline_cluster_ids = _active_cluster_ids(coordinator) or set()
        self._pending_coordinator = coordinator

        created_instance = coordinator.create_topology(
            config,
            seed=plan.seed,
            configure_node_plans=lambda node_plans: apply_startup_overlays(
                node_plans,
                plan.startup_overlays,
            ),
        )
        possible_cluster_id = getattr(created_instance, "cluster_id", None)
        if isinstance(possible_cluster_id, str) and possible_cluster_id:
            if possible_cluster_id in self._pending_baseline_cluster_ids:
                raise LocalTopologyError(f"local coordinator returned pre-existing topology {possible_cluster_id!r}")
            self._pending_cluster_ids.add(possible_cluster_id)
        if not isinstance(created_instance, ClusterInstance):
            raise LocalTopologyError(
                f"local coordinator returned {type(created_instance).__name__}, expected ClusterInstance"
            )
        instance = created_instance
        _validate_provisioned_instance(instance, expected_config, expected_plans)

        handle = LocalValkeyTopologyHandle(coordinator, instance)
        topology = ProvisionedTopology(
            handle=handle,
            services={LOCAL_TOPOLOGY_SERVICE: handle},
            valkey_sha=instance.valkey_sha,
        )

        self._active_coordinator = coordinator
        self._active_topology = topology
        self._pending_coordinator = None
        self._pending_cluster_ids.clear()
        self._pending_baseline_cluster_ids.clear()
        return topology

    def rollback(self, context: RunContext) -> None:
        """Best-effort, idempotent cleanup of a failed provision attempt."""
        del context
        coordinator = self._pending_coordinator
        if coordinator is None:
            return

        active_cluster_ids = _active_cluster_ids(coordinator)
        if active_cluster_ids is not None:
            self._pending_cluster_ids.update(active_cluster_ids - self._pending_baseline_cluster_ids)

        failures: list[tuple[str, BaseException]] = []
        for cluster_id in sorted(self._pending_cluster_ids):
            try:
                destroyed = coordinator.destroy_cluster(cluster_id)
            except BaseException as exc:
                failures.append((cluster_id, exc))
                continue

            current_active_ids = _active_cluster_ids(coordinator)
            if current_active_ids is not None:
                destroyed = cluster_id not in current_active_ids
            if destroyed:
                self._pending_cluster_ids.discard(cluster_id)
            else:
                failures.append(
                    (
                        cluster_id,
                        LocalTopologyError(f"rollback failed for topology {cluster_id!r}"),
                    )
                )

        current_active_ids = _active_cluster_ids(coordinator)
        if current_active_ids is not None:
            self._pending_cluster_ids.update(current_active_ids - self._pending_baseline_cluster_ids)

        if not self._pending_cluster_ids:
            self._pending_coordinator = None
            self._pending_baseline_cluster_ids.clear()

        if failures:
            descriptions = "; ".join(f"{cluster_id}: {_exception_type(failure)}" for cluster_id, failure in failures)
            logger.error("Local topology rollback failed: %s", descriptions)
            interrupt = next(
                (failure for _, failure in failures if not isinstance(failure, Exception)),
                None,
            )
            if interrupt is not None:
                raise interrupt
            first_failure = failures[0][1]
            raise LocalTopologyError(
                f"local topology rollback failed for {len(failures)} topology operation(s)"
            ) from first_failure

    def cleanup(self, context: RunContext, topology: ProvisionedTopology) -> None:
        """Clean up the exact topology returned by this provider."""
        if topology is not self._active_topology or self._active_coordinator is None:
            raise LocalTopologyError("cleanup received a topology not owned by this provider")
        if not isinstance(topology.handle, LocalValkeyTopologyHandle):
            raise LocalTopologyError("owned topology has an invalid local handle")

        topology.handle._begin_close()
        try:
            destroyed = self._active_coordinator.destroy_cluster(topology.handle.cluster_id)
            active_cluster_ids = _active_cluster_ids(self._active_coordinator)
            if not destroyed or (active_cluster_ids is not None and topology.handle.cluster_id in active_cluster_ids):
                raise LocalTopologyError(f"failed to clean up topology {topology.handle.cluster_id!r}")
        except BaseException:
            topology.handle._abort_close()
            raise

        topology.handle._mark_closed()
        self._active_topology = None
        self._active_coordinator = None


def local_topology_provider_factories(
    coordinator_factory: Callable[[], ClusterCoordinator] = ClusterCoordinator,
) -> dict[str, TopologyProviderFactory]:
    """Return provider factories for every topology supported locally."""

    def factory() -> LocalValkeyTopologyProvider:
        return LocalValkeyTopologyProvider(coordinator_factory=coordinator_factory)

    return {topology_type.value: factory for topology_type in TopologyType}


def local_topology_provider_registrations(
    coordinator_factory: Callable[[], ClusterCoordinator] = ClusterCoordinator,
    *,
    api_version: str = EXTENSION_API_VERSION,
) -> dict[str, TopologyProviderRegistration]:
    """Return replay-identifiable registrations for local Valkey topologies."""

    return {
        topology: TopologyProviderRegistration(
            topology=topology,
            provider_id=LOCAL_TOPOLOGY_PROVIDER_ID,
            implementation_version=LOCAL_TOPOLOGY_PROVIDER_IMPLEMENTATION_VERSION,
            api_version=api_version,
            factory=factory,
        )
        for topology, factory in local_topology_provider_factories(coordinator_factory).items()
    }


__all__ = [
    "LOCAL_TOPOLOGY_PROVIDER_ID",
    "LOCAL_TOPOLOGY_PROVIDER_IMPLEMENTATION_VERSION",
    "LOCAL_TOPOLOGY_SERVICE",
    "LocalTopologyError",
    "LocalTopologyNode",
    "LocalTopologyValidationError",
    "LocalValkeyTopologyHandle",
    "LocalValkeyTopologyProvider",
    "apply_startup_overlays",
    "local_topology_provider_factories",
    "local_topology_provider_registrations",
]
