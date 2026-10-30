"""Replication check: replica links, lag, offsets, upstreams, and per-shard redundancy."""

import logging
from collections.abc import Collection
from typing import Optional

from ..cluster_nodes import format_node_address
from ..models import DeploymentMode, ReplicaLagInfo, ReplicationValidation, ReplicationValidationConfig
from ..queries import Replication
from .snapshot import Snapshot

logger = logging.getLogger(__name__)


def check_replication(
    snapshot: Snapshot, config: ReplicationValidationConfig, stopped: Collection[str] = ()
) -> ReplicationValidation:
    """Check every replica's link, lag, and offset, and every shard's live replicas.

    ``stopped`` holds the addresses of the instances stopped on purpose: their
    replicas' links may be down, and they may be missing.
    """
    stopped = set(stopped)
    all_nodes = snapshot.rows
    if not all_nodes:
        logger.error("Unable to fetch any nodes from cluster for replication validation")
        return ReplicationValidation(success=False, error_message="Unable to contact cluster - no nodes reachable")

    replica_nodes = [node for node in all_nodes if node["role"] == "replica"]
    if not replica_nodes:
        if config.min_replicas_per_shard > 0:
            # Replicas are expected: all of them have disappeared.
            message = (
                f"All replicas missing: Expected at least {config.min_replicas_per_shard} replica(s) per shard "
                f"but found 0 replicas in entire cluster. This indicates complete replica loss."
            )
            logger.error(message)
            return ReplicationValidation(success=False, error_message=message)
        logger.info("No replicas configured in cluster (none expected)")
        return ReplicationValidation(success=True, all_replicas_synced=True, max_lag=0.0)

    primary_nodes = {node["node_id"]: node for node in all_nodes if node["role"] == "primary"}
    standalone = snapshot.mode is DeploymentMode.STANDALONE
    expected_primary = None
    replication_errors: list[str] = []
    replica_to_primary: dict[str, str] = {}

    if standalone:
        try:
            expected_primary = _recorded_primary(snapshot)
        except ValueError as exc:
            return ReplicationValidation(success=False, error_message=str(exc))
        if expected_primary.get("status") != "connected":
            replication_errors.append(f"Recorded primary {format_node_address(expected_primary)} is not connected")
        if expected_primary["node_id"] not in primary_nodes:
            replication_errors.append(f"Recorded primary {expected_primary['node_id']} is not reported as a primary")

    lagging_replicas: list[ReplicaLagInfo] = []
    disconnected_replicas: list[str] = []
    # Replicas that answer but whose link is down, unlike ones that do not answer.
    link_down_replicas: set[str] = set()
    max_lag = 0.0
    all_replicas_synced = True

    for replica in replica_nodes:
        replica_address = format_node_address(replica)
        repl_info = snapshot.replication.get(replica_address)
        if replica.get("status") == "failed" or repl_info is None:
            if replica.get("status") != "failed":
                logger.error(f"Replica {replica_address} did not answer INFO replication")
            disconnected_replicas.append(replica_address)
            all_replicas_synced = False
            continue

        if standalone:
            primary_id = expected_primary["node_id"]
            upstream_error = _upstream_error(replica, repl_info, expected_primary)
            if upstream_error:
                replication_errors.append(upstream_error)
                all_replicas_synced = False
                logger.error(upstream_error)
                continue
            replica_to_primary[replica_address] = primary_id
        else:
            # Cluster mode takes the replica's primary from its own view, which failovers keep current.
            primary_id = snapshot.primary_of(replica)

        if primary_id and primary_id in primary_nodes:
            primary_node = primary_nodes[primary_id]
        elif standalone:
            primary_node = expected_primary
        else:
            primary_node = None
        primary_address = format_node_address(primary_node) if primary_node else "unknown"

        if not repl_info.link_up:
            disconnected_replicas.append(replica_address)
            link_down_replicas.add(replica_address)
            all_replicas_synced = False
            logger.warning(f"Replica {replica_address} disconnected from primary (link_status=down)")
            continue

        # Seconds since the primary was last heard from; unknown counts as none.
        lag_seconds = float(max(repl_info.last_io_seconds_ago or 0, 0))
        offset_diff = 0
        if config.check_replication_offset and primary_id:
            primary_info = snapshot.replication.get(primary_address) if primary_node else None
            # A missing offset is unknown, not zero lag; a replica ahead of its primary is not synced either,
            # unless it was read after the primary (Snapshot.caught_up). A replica read later at the primary's
            # offset or beyond only applied what the primary sent between the two reads.
            later = snapshot.caught_up.get(replica_address)
            caught_up = primary_info is not None and primary_info.offset is not None and later is not None
            caught_up = caught_up and later >= primary_info.offset
            if not caught_up and (
                primary_info is None
                or primary_info.offset is None
                or repl_info.replica_offset is None
                or primary_info.offset < repl_info.replica_offset
            ):
                offset_error = (
                    f"Unable to compare replication offsets for replica {replica_address} and primary {primary_address}"
                )
                replication_errors.append(offset_error)
                all_replicas_synced = False
                logger.error(offset_error)
                continue
            offset_diff = 0 if caught_up else primary_info.offset - repl_info.replica_offset

        # A replica behind by bytes is not caught up merely because it heard from the primary recently.
        is_lagging = offset_diff > 0 if config.check_replication_offset else lag_seconds > config.max_acceptable_lag
        max_lag = max(max_lag, lag_seconds)
        if is_lagging:
            lagging_replicas.append(
                ReplicaLagInfo(
                    replica_node_id=replica["node_id"],
                    replica_address=replica_address,
                    primary_node_id=primary_id or "unknown",
                    primary_address=primary_address,
                    lag_seconds=lag_seconds,
                    replication_offset_diff=offset_diff,
                    link_status="up",
                )
            )
            all_replicas_synced = False
            logger.warning(
                f"Replica {replica_address} lagging behind primary (lag={lag_seconds:.2f}s, offset_diff={offset_diff})"
            )

    # Each primary's own count of connected replicas: advisory in cluster mode, where ownership moves during a
    # failover, and a failure for a standalone deployment's fixed primary and replicas.
    if not standalone:
        for replica in replica_nodes:
            replica_address = format_node_address(replica)
            primary_id = snapshot.primary_of(replica) if replica_address not in disconnected_replicas else None
            if primary_id:
                replica_to_primary[replica_address] = primary_id
    replication_errors.extend(
        _cross_check_primaries(snapshot, primary_nodes, replica_to_primary, disconnected_replicas, strict=standalone)
    )

    success = not replication_errors
    redundancy_lost = False
    error_message = "; ".join(replication_errors) if replication_errors else None
    if replication_errors:
        all_replicas_synced = False

    # Replicas whose primary was stopped and not yet replaced answer with their link down: expected. A replica
    # that does not answer is still unexpected, even when its primary was stopped.
    expected_link_down = set()
    if stopped:
        stopped_primary_shards = {
            node["shard_id"]
            for node in all_nodes
            if node["role"] == "primary" and node.get("shard_id") is not None and format_node_address(node) in stopped
        }
        live_primary_shards = {
            node["shard_id"]
            for node in all_nodes
            if node["role"] == "primary"
            and node.get("shard_id") is not None
            and format_node_address(node) not in stopped
            and node.get("status") != "failed"
        }
        orphaned_shards = stopped_primary_shards - live_primary_shards
        expected_link_down = {
            format_node_address(replica)
            for replica in replica_nodes
            if replica.get("shard_id") in orphaned_shards and format_node_address(replica) in link_down_replicas
        }
        if expected_link_down:
            logger.info(
                f"Replicas with expected replication link down (primary stopped on purpose, replica alive): "
                f"{expected_link_down}"
            )

    if stopped and disconnected_replicas:
        disconnected = set(disconnected_replicas)
        unexpected = disconnected - stopped - expected_link_down
        if unexpected:
            success = False
            error_message = (
                f"Unexpected replica disconnections detected: {unexpected}. "
                f"These replicas failed but were not stopped on purpose. "
                f"Expected stopped nodes: {stopped}"
            )
            logger.error(error_message)

        # A replica stopped on purpose that is still connected was not stopped, or came back.
        survivors = (stopped & {format_node_address(replica) for replica in replica_nodes}) - disconnected
        if survivors and success:
            success = False
            error_message = (
                f"Stopped replicas unexpectedly still connected: {survivors}. "
                f"These nodes were stopped on purpose but are still reachable/synced. "
                f"This indicates improper node termination or unexpected recovery."
            )
            logger.error(error_message)

    if success and config.min_replicas_per_shard > 0:
        redundancy_error, redundancy_lost = _redundancy_problem(
            all_nodes,
            replica_nodes,
            [replica for replica in disconnected_replicas if replica not in expected_link_down],
            stopped,
            config.min_replicas_per_shard,
            link_down_replicas,
        )
        if redundancy_error:
            success = False
            error_message = redundancy_error

    if success and config.require_all_replicas_synced:
        if not all_replicas_synced:
            success = False
            error_message = (
                f"Not all replicas synced: {len(lagging_replicas)} lagging, {len(disconnected_replicas)} disconnected"
            )
    elif success:
        if lagging_replicas:
            success = False
            if config.check_replication_offset:
                max_offset_diff = max(replica.replication_offset_diff for replica in lagging_replicas)
                error_message = (
                    f"{len(lagging_replicas)} replica(s) behind their primary "
                    f"(max_offset_diff={max_offset_diff}, required=0)"
                )
            else:
                error_message = (
                    f"{len(lagging_replicas)} replica(s) lagging beyond acceptable threshold "
                    f"(max_lag={max_lag:.2f}s > {config.max_acceptable_lag:.2f}s)"
                )
        elif len(disconnected_replicas) == len(replica_nodes):
            if set(disconnected_replicas) - stopped - expected_link_down:
                success = False
                redundancy_lost = True
                error_message = "All replicas are disconnected"
            else:
                logger.info(f"All {len(replica_nodes)} replica(s) disconnected, all stopped on purpose (expected)")
        elif disconnected_replicas:
            logger.info(f"{len(disconnected_replicas)} replica(s) disconnected but cluster still functional")

    return ReplicationValidation(
        success=success,
        all_replicas_synced=all_replicas_synced,
        max_lag=max_lag,
        lagging_replicas=lagging_replicas,
        disconnected_replicas=disconnected_replicas,
        error_message=error_message,
        redundancy_lost=redundancy_lost,
    )


def _cross_check_primaries(
    snapshot: Snapshot,
    primary_nodes: dict[str, dict],
    replica_to_primary: dict[str, str],
    disconnected_replicas: list[str],
    *,
    strict: bool,
) -> list[str]:
    """Compare each primary's count of connected replicas with the replicas that follow it."""
    errors: list[str] = []
    for primary_id, primary in primary_nodes.items():
        address = format_node_address(primary)
        repl_info = snapshot.replication.get(address)
        if repl_info is None:
            if strict:
                message = f"Unable to query replication INFO from primary {address}"
                errors.append(message)
                logger.error(message)
            continue
        connected_replicas = repl_info.connected_replicas or 0
        expected = sum(
            1
            for replica, followed in replica_to_primary.items()
            if followed == primary_id and replica not in disconnected_replicas
        )
        if connected_replicas != expected:
            message = f"Primary {address} reports {connected_replicas} connected replicas, expected {expected}"
            if strict:
                errors.append(message)
                logger.error(message)
            else:
                logger.debug(f"{message} (may be transient during failover)")
    return errors


def _recorded_primary(snapshot: Snapshot) -> dict:
    """The one primary a standalone deployment's records name, as a topology row."""
    primaries = [instance for instance in snapshot.instances if instance.role == "primary"]
    if len(primaries) > 1:
        raise ValueError(f"A standalone deployment requires exactly one recorded primary, found {len(primaries)}")
    if primaries:
        primary = primaries[0]
        row = next((node for node in snapshot.rows if node.get("node_id") == primary.instance_id), None)
        return {
            "node_id": primary.instance_id,
            "host": primary.host,
            "port": primary.port,
            "role": "primary",
            "shard_id": primary.shard_id,
            "status": row.get("status", "failed") if row else "failed",
        }
    current = [node for node in snapshot.rows if node["role"] == "primary"]
    if len(current) != 1:
        raise ValueError(f"A standalone deployment requires exactly one primary, found {len(current)}")
    return current[0]


def _upstream_error(replica: dict, repl_info: Replication, expected_primary: dict) -> Optional[str]:
    """Why a standalone replica is not attached to the recorded primary, or None when it is."""
    expected = (str(expected_primary["host"]), int(expected_primary["port"]))
    if (str(repl_info.primary_host), repl_info.primary_port) == expected:
        return None
    reported = f"{repl_info.primary_host}:{repl_info.primary_port}" if repl_info.primary_host is not None else "unknown"
    return f"Replica {format_node_address(replica)} is attached to {reported}, expected {expected[0]}:{expected[1]}"


def _redundancy_problem(
    all_nodes: list[dict],
    replica_nodes: list[dict],
    disconnected_replicas: list[str],
    stopped: set[str],
    min_replicas_per_shard: int,
    link_down_replicas: Collection[str],
) -> tuple[Optional[str], bool]:
    """Why a shard has fewer than ``min_replicas_per_shard`` connected replicas (None when none has), and
    whether replicas were lost to failures that were not stops.

    A disconnected replica stopped on purpose is expected. One that answers but
    whose link is down, e.g. during a full resync, has not failed: the
    shortfall fails without counting as lost, so a retry can see the resync
    finish.
    """
    shards = {node["shard_id"] for node in all_nodes if node["role"] == "primary" and node.get("shard_id") is not None}
    members: dict[int, dict[str, list[str]]] = {
        shard: {"alive": [], "stopped": [], "lost": [], "link_down": []} for shard in shards
    }
    for replica in replica_nodes:
        if replica.get("shard_id") not in members:
            continue
        address = format_node_address(replica)
        if address not in disconnected_replicas:
            kind = "alive"
        elif address in stopped:
            kind = "stopped"
        elif address in link_down_replicas:
            kind = "link_down"
        else:
            kind = "lost"
        members[replica["shard_id"]][kind].append(address)

    lost, link_down = [], []
    for shard, replicas in sorted(members.items()):
        alive = len(replicas["alive"])
        if alive >= min_replicas_per_shard:
            continue
        if replicas["lost"]:
            lost.append(f"shard {shard} ({alive} alive, {len(replicas['lost'])} unexpected failures)")
        elif replicas["link_down"]:
            link_down.append(f"shard {shard} ({', '.join(sorted(replicas['link_down']))})")
        else:
            logger.warning(f"Shard {shard} has {alive} live replica(s): {len(replicas['stopped'])} stopped on purpose")
    if lost:
        return (
            f"replication: CRITICAL: {len(lost)} shard(s) have insufficient redundancy due to UNEXPECTED failures "
            f"(< {min_replicas_per_shard} alive replica per shard). Affected shards: {', '.join(lost)}. "
            f"These replicas failed without being stopped on purpose.",
            True,
        )
    if link_down:
        return (
            f"replication: {len(link_down)} shard(s) have fewer than {min_replicas_per_shard} replica(s) with their "
            f"replication link up; these replicas answer but their link is down: {'; '.join(link_down)}",
            False,
        )
    return None, False
