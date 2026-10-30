"""Planning: the instances a deployment needs, with their roles, shards, slots, ports, and server config."""

import logging
from collections.abc import Mapping, Sequence
from typing import Any, Optional

from ...contracts import StartupOverlay
from ...schema import plain_config
from .cluster_nodes import HASH_SLOTS
from .models import (
    CLUSTER_BUS_PORT_OFFSET,
    DeploymentConfig,
    DeploymentMode,
    InstancePlan,
    LocalDeploymentValidationError,
)
from .server_config import canonicalize_server_config, server_config_values

logger = logging.getLogger(__name__)


class DeploymentPlanner:
    """Plans the instances of one deployment without starting anything."""

    def __init__(self, config: DeploymentConfig):
        self.config = config

    def create_instance_plan(
        self,
        index: int,
        role: str,
        shard_id: int,
        slot_start: Optional[int] = None,
        slot_end: Optional[int] = None,
        primary_id: Optional[str] = None,
    ) -> InstancePlan:
        """Plan instance ``valkey-<index>``: client port ``base_port + index``, cluster bus port
        :data:`CLUSTER_BUS_PORT_OFFSET` above it."""
        instance_id = f"valkey-{index}"
        client_port = self.config.base_port + index
        bus_port = client_port + CLUSTER_BUS_PORT_OFFSET
        server_config = self.resolve_instance_server_config(instance_id, role)
        host = "127.0.0.1"
        if self.config.addressing == "per_instance_loopback":
            host = f"127.0.{self.config.loopback_block}.{index + 2}"

        return InstancePlan(
            instance_id=instance_id,
            role=role,
            shard_id=shard_id,
            port=client_port,
            bus_port=bus_port,
            slot_start=slot_start,
            slot_end=slot_end,
            primary_id=primary_id,
            server_config=server_config,
            valkey_binary=self._instance_value(self.config.instance_binaries, instance_id, role),
            data_root=self._instance_value(self.config.instance_data_roots, instance_id, role),
            host=host,
        )

    @staticmethod
    def _instance_value(values: Optional[Mapping[str, str]], instance_id: str, role: str) -> Optional[str]:
        """Return the exact-instance value, else the role value, else None."""
        values = values or {}
        return values.get(instance_id) or values.get(role)

    def resolve_instance_server_config(self, instance_id: str, role: str) -> dict[str, Any]:
        """The global, role, and exact-instance server config layers, later ones winning.

        The layers were canonicalized when the deployment document was read.
        """
        instance_configs = self.config.instance_configs or {}
        return {
            **(self.config.server_config or {}),
            **instance_configs.get(role, {}),
            **instance_configs.get(instance_id, {}),
        }

    def plan(self) -> list[InstancePlan]:
        """Plan the instances: each shard's primary and its replicas, in ``valkey-N`` order.

        Standalone mode has one shard and no hash slots.
        """
        cluster = self.config.mode is DeploymentMode.CLUSTER
        if cluster:
            logger.info(
                f"Planning a cluster of {self.config.primaries} primaries with {self.config.replicas_per_primary} replica(s) each"
            )
        else:
            logger.info("Planning a standalone deployment with %s replica(s)", self.config.replicas_per_primary)

        slots_per_shard = HASH_SLOTS // self.config.primaries
        instances = []
        for shard_num in range(self.config.primaries):
            slot_start = slot_end = None
            if cluster:
                slot_start = shard_num * slots_per_shard
                if shard_num == self.config.primaries - 1:
                    slot_end = HASH_SLOTS - 1
                else:
                    slot_end = slot_start + slots_per_shard - 1

            primary_plan = self.create_instance_plan(len(instances), "primary", shard_num, slot_start, slot_end)
            instances.append(primary_plan)
            logger.debug(
                f"{primary_plan.instance_id}: primary, shard {shard_num}, "
                f"port {primary_plan.port}, slots {slot_start}-{slot_end}"
            )

            for _ in range(self.config.replicas_per_primary):
                replica_plan = self.create_instance_plan(
                    len(instances), "replica", shard_num, primary_id=primary_plan.instance_id
                )
                instances.append(replica_plan)
                logger.debug(
                    f"{replica_plan.instance_id}: replica, shard {shard_num}, "
                    f"port {replica_plan.port}, primary={primary_plan.instance_id}"
                )

        return instances


_SUPPORTED_CONFLICT_POLICIES = frozenset({"override", "error"})


def _value_tokens(option: str, value: Any) -> tuple[str, ...]:
    """Return the exact argv values used to compare config semantics."""
    return tuple(server_config_values(option, value))


def _selected_plans(instance_plans: Sequence[InstancePlan], overlay: StartupOverlay) -> tuple[InstancePlan, ...]:
    selector = overlay.selector
    known_roles = {plan.role for plan in instance_plans}
    known_instance_ids = {plan.instance_id for plan in instance_plans}

    unknown_roles = sorted(set(selector.roles) - known_roles)
    if unknown_roles:
        raise LocalDeploymentValidationError(
            f"startup overlay {overlay.overlay_id!r} references unknown roles: {unknown_roles}"
        )

    unknown_instance_ids = sorted(set(selector.instance_ids) - known_instance_ids)
    if unknown_instance_ids:
        raise LocalDeploymentValidationError(
            f"startup overlay {overlay.overlay_id!r} references unknown instance IDs: {unknown_instance_ids}"
        )

    by_id = {plan.instance_id: plan for plan in instance_plans}
    selected_ids = selector.resolve(
        [plan.instance_id for plan in instance_plans], {plan.instance_id: plan.role for plan in instance_plans}
    )
    selected = tuple(by_id[instance_id] for instance_id in selected_ids)
    if not selected:
        raise LocalDeploymentValidationError(
            f"startup overlay {overlay.overlay_id!r} does not match any planned instance"
        )
    return selected


def apply_startup_overlays(instance_plans: Sequence[InstancePlan], overlays: Sequence[StartupOverlay]) -> None:
    """Atomically apply ordered startup overlays to planned instance configs.

    Values within a selector field are alternatives, while non-empty selector
    fields are combined with AND. An empty selector targets every instance.
    ``override`` replaces the complete value for an option; ``error`` permits
    an identical value but rejects a different value already supplied by the
    deployment config or an earlier overlay.
    """
    plans = tuple(instance_plans)
    if any(not isinstance(plan, InstancePlan) for plan in plans):
        raise TypeError("instance_plans must contain InstancePlan values")

    instance_ids = [plan.instance_id for plan in plans]
    if len(set(instance_ids)) != len(instance_ids):
        raise LocalDeploymentValidationError("planned instance IDs must be unique")

    overlay_entries = tuple(overlays)
    if any(not isinstance(overlay, StartupOverlay) for overlay in overlay_entries):
        raise TypeError("overlays must contain StartupOverlay values")

    # Work entirely on detached mappings. No InstancePlan is changed unless every
    # selector, policy, conflict, and value has passed validation.
    resolved_configs = {plan.instance_id: dict(plan.server_config) for plan in plans}

    for overlay in overlay_entries:
        if overlay.conflict_policy not in _SUPPORTED_CONFLICT_POLICIES:
            raise LocalDeploymentValidationError(
                f"startup overlay {overlay.overlay_id!r} has unsupported conflict policy "
                f"{overlay.conflict_policy!r}; expected one of {sorted(_SUPPORTED_CONFLICT_POLICIES)}"
            )

        selected = _selected_plans(plans, overlay)
        # References stay as they are: a compiled plan's overlay may still hold one.
        overlay_config = canonicalize_server_config(plain_config(overlay.server_config))

        for plan in selected:
            resolved = resolved_configs[plan.instance_id]
            for option, value in overlay_config.items():
                if overlay.conflict_policy == "error" and option in resolved:
                    if _value_tokens(option, resolved[option]) != _value_tokens(option, value):
                        raise LocalDeploymentValidationError(
                            f"startup overlay {overlay.overlay_id!r} conflicts on option "
                            f"{option!r} for instance {plan.instance_id!r}"
                        )
                    continue
                resolved[option] = value

    for plan in plans:
        plan.server_config = resolved_configs[plan.instance_id]
