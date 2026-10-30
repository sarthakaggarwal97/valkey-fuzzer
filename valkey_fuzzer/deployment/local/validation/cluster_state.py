"""Cluster state checks: CLUSTER INFO state, quorum, failed nodes, and hash-slot coverage."""

import logging
from collections.abc import Collection
from typing import Optional

from ..cluster_nodes import HASH_SLOTS, format_node_address, group_slots_into_ranges, parse_slot_tokens
from ..models import (
    ClusterStatusValidation,
    ClusterStatusValidationConfig,
    SlotConflict,
    SlotCoverageValidation,
    SlotCoverageValidationConfig,
)
from .snapshot import Snapshot

logger = logging.getLogger(__name__)


def _stopped_vs_failed(stopped: set[str], failed: list[str]) -> Optional[str]:
    """Why the nodes in the fail state are not exactly the ones stopped on purpose; None when they are."""
    not_failed = stopped - set(failed)
    if not_failed:
        return (
            f"cluster_status: Stopped nodes not in fail state: {not_failed}. "
            f"These nodes were stopped on purpose but are not marked as failed. "
            f"This indicates improper node termination or cluster state tracking bug."
        )
    not_stopped = set(failed) - stopped
    if not_stopped:
        return (
            f"cluster_status: Unexpected nodes in fail state: {not_stopped}. "
            f"These nodes failed but were not stopped on purpose. "
            f"This indicates cascading failures or other clustering bugs. "
            f"Expected failed: {stopped}, Actual failed: {set(failed)}"
        )
    return None


def check_cluster_status(
    snapshot: Snapshot, config: ClusterStatusValidationConfig, stopped: Collection[str] = ()
) -> ClusterStatusValidation:
    """Check every node's cluster_state, the failed nodes, and the slot-owning primaries' quorum."""
    stopped = set(stopped)
    all_nodes = snapshot.rows
    if not all_nodes:
        return ClusterStatusValidation(
            success=False, degraded_reason="No nodes reachable", error_message="Unable to connect to any cluster nodes"
        )

    cluster_states: dict[str, str] = {}
    missing_cluster_state_nodes: list[str] = []
    cluster_info_query_failures: list[str] = []
    reachable_addresses: set[str] = set()
    for node in all_nodes:
        node_address = format_node_address(node)
        cluster_info = snapshot.cluster_info.get(node_address)
        if cluster_info is None:
            # A node still reported as connected must not drop out of the check because it did not answer.
            if node.get("status") != "failed":
                cluster_info_query_failures.append(node_address)
            continue
        reachable_addresses.add(node_address)
        raw_cluster_state = cluster_info.get("cluster_state")
        if raw_cluster_state is None or not raw_cluster_state.strip():
            missing_cluster_state_nodes.append(node_address)
            cluster_states[node_address] = "unknown"
        else:
            cluster_states[node_address] = raw_cluster_state.strip().lower()
    nodes_in_fail_state = [format_node_address(node) for node in all_nodes if node["status"] == "failed"]

    # Do not majority-vote away a disagreement: expose it and judge every report.
    reported_states = set(cluster_states.values())
    state_disagreement = len(reported_states) > 1
    if state_disagreement:
        overall_cluster_state = "mixed"
        logger.warning(f"Cluster state disagreement detected: {cluster_states}")
    else:
        overall_cluster_state = next(iter(reported_states), "unknown")

    # Quorum as Valkey counts it: a majority of the primaries that own slots. A primary without slots, such
    # as a stopped primary whose replica took over its slots, counts neither for nor against it.
    primary_nodes = [node for node in all_nodes if node["role"] == "primary" and node.get("slot_count") != 0]
    reachable_primaries = [node for node in primary_nodes if format_node_address(node) in reachable_addresses]
    has_quorum = True
    quorum_error = None
    if config.require_quorum and primary_nodes:
        quorum_threshold = (len(primary_nodes) // 2) + 1
        has_quorum = len(reachable_primaries) >= quorum_threshold
        if not has_quorum:
            unreachable = [
                address + (" (stopped on purpose)" if address in stopped else "")
                for address in sorted(format_node_address(node) for node in primary_nodes)
                if address not in reachable_addresses
            ]
            quorum_error = (
                f"Cluster does not have quorum: {len(reachable_primaries)}/{len(primary_nodes)} "
                f"slot-owning primaries reachable (need {quorum_threshold}); "
                f"unreachable: {', '.join(unreachable)}"
            )
            logger.warning(quorum_error)

    validation_errors: list[str] = []
    degraded_reason = None
    explicit_acceptable_states = {
        str(state).strip().lower() for state in config.acceptable_states if str(state).strip()
    }
    effective_acceptable_states = set(explicit_acceptable_states)
    if config.allow_degraded:
        effective_acceptable_states.update({"fail", "degraded"})

    if missing_cluster_state_nodes:
        validation_errors.append(
            "Reachable nodes returned a missing or blank cluster_state from CLUSTER INFO: "
            + ", ".join(sorted(missing_cluster_state_nodes))
        )
    if cluster_info_query_failures:
        validation_errors.append(
            "Connected nodes could not be queried with CLUSTER INFO: " + ", ".join(sorted(cluster_info_query_failures))
        )
    unacceptable_reports = {
        address: state
        for address, state in cluster_states.items()
        if address not in missing_cluster_state_nodes and state not in effective_acceptable_states
    }
    if not cluster_states and "unknown" not in effective_acceptable_states:
        validation_errors.append(f"Cluster state 'unknown' not in acceptable states: {config.acceptable_states}")
    elif unacceptable_reports:
        reports = ", ".join(f"{address}={state}" for address, state in sorted(unacceptable_reports.items()))
        validation_errors.append(
            f"Cluster nodes reported unacceptable states ({reports}); acceptable states: {config.acceptable_states}"
        )

    # A mixed report is allowed only when every reported state is listed explicitly: allow_degraded permits a
    # unanimous degraded state, not inconsistent views.
    if state_disagreement:
        if not reported_states.issubset(explicit_acceptable_states):
            reports = ", ".join(f"{address}={state}" for address, state in sorted(cluster_states.items()))
            validation_errors.append(f"Cluster state disagreement across reachable nodes: {reports}")
        elif not validation_errors:
            degraded_reason = "Cluster state disagreement explicitly allowed by acceptable_states"
    elif overall_cluster_state in {"fail", "degraded"} and config.allow_degraded:
        degraded_reason = f"Cluster in {overall_cluster_state} state"
        logger.info(f"Cluster in degraded state: {overall_cluster_state}")

    if nodes_in_fail_state:
        logger.warning(f"Nodes in fail state: {nodes_in_fail_state}")
        if stopped:
            failed_error = _stopped_vs_failed(stopped, nodes_in_fail_state)
            if failed_error:
                logger.error(failed_error)
                validation_errors.append(failed_error)
            elif not degraded_reason:
                degraded_reason = f"{len(nodes_in_fail_state)} node(s) in fail state (stopped on purpose) - expected"
        elif overall_cluster_state in explicit_acceptable_states and not config.allow_degraded:
            # The cluster reports itself acceptable with nodes failed that nobody stopped.
            validation_errors.append(
                f"Spontaneous node failures detected: {len(nodes_in_fail_state)} node(s) in fail state "
                f"but were not stopped on purpose. "
                f"Failed nodes: {nodes_in_fail_state}. "
                f"This indicates unexpected node failures or failed failover attempts. "
                f"Cluster state: {overall_cluster_state}"
            )
        elif overall_cluster_state in explicit_acceptable_states:
            degraded_reason = f"{len(nodes_in_fail_state)} node(s) in fail state (degraded state allowed by config)"
            logger.warning(f"Nodes in fail state but degraded state is allowed: {nodes_in_fail_state}")
        elif not degraded_reason:
            degraded_reason = f"{len(nodes_in_fail_state)} node(s) in fail state"

    if quorum_error:
        validation_errors.append(quorum_error)

    # Keep every independent failure rather than letting a later one overwrite the first cause.
    success = not validation_errors
    error_message = "; ".join(validation_errors) if validation_errors else None
    logger.log(
        logging.INFO if success else logging.ERROR,
        f"Cluster status validation {'passed' if success else 'failed'}: state={overall_cluster_state}, "
        f"quorum={has_quorum}, failed_nodes={len(nodes_in_fail_state)}",
    )
    return ClusterStatusValidation(
        success=success,
        cluster_state=overall_cluster_state,
        nodes_in_fail_state=nodes_in_fail_state,
        has_quorum=has_quorum,
        degraded_reason=degraded_reason,
        error_message=error_message,
    )


def check_slot_coverage(
    snapshot: Snapshot,
    config: SlotCoverageValidationConfig,
    stopped: Collection[str] = (),
    stopped_shards: Collection[int] = (),
) -> SlotCoverageValidation:
    """Check that every live node's view assigns every slot, to one primary, and none to a stopped node.

    ``stopped_shards`` are the live shards whose every member was stopped on
    purpose: no node is left to take their slots over.
    """
    stopped = set(stopped)
    all_nodes = snapshot.rows
    if not all_nodes:
        logger.error("Unable to fetch any nodes from cluster for slot coverage validation")
        return SlotCoverageValidation(success=False, error_message="Unable to contact cluster - no nodes reachable")
    primary_nodes = [node for node in all_nodes if node["role"] == "primary"]
    if not primary_nodes:
        return SlotCoverageValidation(success=False, error_message="No primary nodes available in cluster")

    # Each live node's view: slot -> the primary that owns it.
    node_perspectives = {
        address: {
            slot: node["node_id"] for node in view if node["is_primary"] for slot in parse_slot_tokens(node["slots"])
        }
        for address, view in snapshot.views().items()
    }
    if not node_perspectives:
        return SlotCoverageValidation(success=False, error_message="Unable to query slot assignments from any node")

    slot_assignments = {slot: sorted(owners) for slot, owners in snapshot.slot_claims().items()}
    slot_distribution: dict[str, list[int]] = {}
    for slot, owners in sorted(slot_assignments.items()):
        if len(owners) == 1:
            slot_distribution.setdefault(owners[0], []).append(slot)
    unassigned_slots = [slot for slot in range(HASH_SLOTS) if slot not in slot_assignments]
    conflicting_slots = [
        SlotConflict(slot=slot, conflicting_nodes=owners)
        for slot, owners in sorted(slot_assignments.items())
        if len(owners) > 1
    ]
    for conflict in conflicting_slots:
        logger.warning(f"Slot {conflict.slot} has conflicting assignments: {conflict.conflicting_nodes}")
    if unassigned_slots:
        logger.warning(f"Found {len(unassigned_slots)} unassigned slots: {group_slots_into_ranges(unassigned_slots)}")

    success = True
    error_message = None

    # The slots of shards whose every member was stopped, including slots now unassigned that a view still
    # gives one of their nodes.
    stopped_shard_nodes = {node["node_id"] for node in all_nodes if node.get("shard_id") in set(stopped_shards)}
    slots_on_stopped_shards = {
        slot for view in node_perspectives.values() for slot, owner in view.items() if owner in stopped_shard_nodes
    }
    slots_on_stopped_shards.update(
        slot
        for node in all_nodes
        if node["role"] == "primary" and node["node_id"] in stopped_shard_nodes
        for slot in slot_distribution.get(node["node_id"], [])
    )
    if slots_on_stopped_shards:
        logger.info(
            f"Excluding {len(slots_on_stopped_shards)} slots of shard(s) {sorted(stopped_shards)}, whose every "
            f"member was stopped on purpose: {group_slots_into_ranges(sorted(slots_on_stopped_shards))}"
        )

    # Slots still assigned to a stopped node: its failover did not complete.
    stopped_node_ids = {node["node_id"] for node in all_nodes if format_node_address(node) in stopped}
    slots_on_stopped_nodes = [slot for node_id in stopped_node_ids for slot in slot_distribution.get(node_id, [])]
    unexpected_on_stopped = [slot for slot in slots_on_stopped_nodes if slot not in slots_on_stopped_shards]
    if unexpected_on_stopped:
        success = False
        error_message = (
            f"CRITICAL: {len(unexpected_on_stopped)} slots still assigned to stopped nodes. "
            f"Stopped nodes: {stopped}. "
            f"This indicates failover did not complete or slots were not reassigned. "
            f"Affected slots: {group_slots_into_ranges(unexpected_on_stopped)}"
        )
        logger.error(error_message)
    elif slots_on_stopped_nodes:
        logger.info(f"{len(slots_on_stopped_nodes)} slots on stopped nodes belong to fully stopped shards")

    if success and config.require_full_coverage:
        unexpected_unassigned = [slot for slot in unassigned_slots if slot not in slots_on_stopped_shards]
        if unexpected_unassigned:
            success = False
            error_message = (
                f"{len(unexpected_unassigned)} slots are unassigned (expected all {HASH_SLOTS} slots to be assigned)"
            )
            logger.error(error_message)

    if success and config.require_full_coverage:
        # The union of the views can cover every slot while one node still misses some.
        for node_address, slot_view in sorted(node_perspectives.items()):
            node_missing = [
                slot for slot in range(HASH_SLOTS) if slot not in slot_view and slot not in slots_on_stopped_shards
            ]
            if node_missing:
                success = False
                error_message = (
                    f"Node {node_address} sees {len(node_missing)} unassigned slots "
                    f"(expected all {HASH_SLOTS} slots to be assigned): {group_slots_into_ranges(node_missing)}"
                )
                logger.error(error_message)
                break

    if success and not config.allow_slot_conflicts and conflicting_slots:
        success = False
        error_message = f"{len(conflicting_slots)} slots have conflicting assignments"
        logger.error(error_message)

    logger.log(
        logging.INFO if success else logging.ERROR,
        f"Slot coverage validation {'passed' if success else 'failed'}: {len(slot_assignments)}/{HASH_SLOTS} slots "
        f"assigned, {len(unassigned_slots)} unassigned, {len(conflicting_slots)} conflicts",
    )
    return SlotCoverageValidation(
        success=success,
        total_slots_assigned=len(slot_assignments),
        unassigned_slots=unassigned_slots,
        conflicting_slots=conflicting_slots,
        slot_distribution=slot_distribution,
        error_message=error_message,
    )
