"""Failover: promote a replica of a shard's current primary and wait for the cluster to settle."""

import logging
import random
import time
from collections.abc import Collection
from typing import Optional

from .models import DeploymentConnection
from .queries import poll_until, query_replication, valkey_client

logger = logging.getLogger(__name__)

# How often a failover looks again for a replica that has synced with the shard's primary.
SYNC_POLL_S = 0.2


def _shard_primary(nodes: list[dict], shard_id: int) -> Optional[dict]:
    return next((node for node in nodes if node.get("role") == "primary" and node.get("shard_id") == shard_id), None)


def replica_sync_problem(replica: dict, primary: dict) -> Optional[str]:
    """Why ``replica`` holds no synced copy of ``primary``'s data, or None when it does.

    Synced means its replication link to that primary is up, no sync is in
    progress, and its offset is past zero. ``CLUSTER FAILOVER FORCE`` checks
    none of this by design, so a replica that just migrated to an orphaned
    primary (offset 0, still loading its first sync) would win and replace the
    shard's data with nothing.
    """
    try:
        with valkey_client(replica["host"], replica["port"], timeout=2.0) as client:
            info = query_replication(client)
    except Exception as exc:
        return f"unreachable ({type(exc).__name__})"
    if info.role != "replica":
        return f"role {info.role}"
    if (info.primary_host, info.primary_port) != (primary["host"], primary["port"]):
        return f"replicates {info.primary_host}:{info.primary_port}"
    if not info.link_up:
        return "link down"
    if info.sync_in_progress:
        return "sync in progress"
    if (info.replica_offset or 0) <= 0:
        return "offset 0"
    return None


def _answers(node: dict) -> bool:
    try:
        with valkey_client(node["host"], node["port"], timeout=1.0, decode_responses=True) as client:
            return bool(client.ping())
    except Exception:
        return False


def _first_synced(
    replicas: list[dict],
    primary: dict,
    rng: Optional[random.Random],
    timeout: float,
    stopped_ports: Collection[int] = (),
) -> tuple[Optional[dict], dict[int, str]]:
    """The first replica, in a random order, that has synced with ``primary`` (see :func:`replica_sync_problem`),
    waiting up to ``timeout`` seconds for one while the primary answers; ``(None, why each was refused)`` when
    none has.

    A seeded ``rng`` tries them in the same order every run. A replica sent
    ``SIGTERM`` still answers until it exits, and Valkey still accepts
    ``CLUSTER FAILOVER`` on it, so one in ``stopped_ports`` is skipped. Every
    replica is shuffled either way, so skipping one does not change the order
    of the others.
    """
    # Shuffle from port order: ports, unlike cluster node IDs, are the same in every run.
    order = sorted(replicas, key=lambda node: (node["host"], node["port"]))
    shuffle = random.shuffle if rng is None else rng.shuffle
    shuffle(order)
    candidates = [node for node in order if node["port"] not in stopped_ports]
    chosen: list[dict] = []
    refused: dict[int, str] = {}
    deadline = time.monotonic() + timeout

    def decided() -> bool:
        refused.clear()
        for node in candidates:
            problem = replica_sync_problem(node, primary)
            if problem is None:
                chosen.append(node)
                return True
            refused[node["port"]] = problem
        if candidates and time.monotonic() < deadline and not _answers(primary):
            # Nothing syncs from a primary that is gone; Valkey's own failover takes over the shard.
            refused[primary["port"]] = "the primary does not answer"
            return True
        return not candidates

    poll_until(decided, timeout=timeout, interval=SYNC_POLL_S, what="a synced replica")
    return (chosen[0], {}) if chosen else (None, dict(refused))


def failover_shard(
    connection: DeploymentConnection,
    shard_id: int,
    *,
    force: bool = False,
    timeout: float = 30.0,
    stopped_instances: Collection[str] = (),
    rng: Optional[random.Random] = None,
) -> bool:
    """Promote a replica of ``shard_id``'s current primary and wait up to ``timeout`` seconds for it to settle.

    ``stopped_instances`` are instance IDs stopped on purpose. A seeded
    ``rng`` makes the replica choice replayable.
    """
    try:
        return _failover(connection, shard_id, force, timeout, stopped_instances, rng)
    except Exception as e:
        logger.error(f"Failover of shard {shard_id} failed: {e}")
        return False


def _failover(
    connection: DeploymentConnection,
    shard_id: int,
    force: bool,
    timeout: float,
    stopped_instances: Collection[str],
    rng: Optional[random.Random],
) -> bool:
    logger.info(f"Executing failover on shard {shard_id}")
    stopped_ports = {node.port for node in connection.instances if node.instance_id in set(stopped_instances)}
    # One view of the topology: after a previous failover the shard's primary may be a promoted replica.
    rows = connection.get_current_nodes(include_failed=True)
    live = [node for node in rows if node.get("status") != "failed"]
    target = _shard_primary(live, shard_id)

    if target is None:
        stopped_primary = _shard_primary(rows, shard_id)
        if stopped_primary and stopped_primary.get("status") == "failed" and stopped_primary["port"] in stopped_ports:
            # Valkey's own failover promotes a live replica of a primary stopped on purpose; with none
            # left the whole shard is down, which is not a failed failover.
            if any(
                node.get("role") == "replica" and node.get("primary_id") == stopped_primary["node_id"] for node in live
            ):
                return wait_until_settled(
                    connection,
                    timeout,
                    shard_id=shard_id,
                    old_primary=stopped_primary["node_id"],
                    stopped_ports=stopped_ports,
                )
            return True
        logger.error(f"No primary of shard {shard_id} found in cluster")
        return False

    replica_nodes = [
        node for node in rows if node.get("role") == "replica" and node.get("primary_id") == target["node_id"]
    ]
    if not replica_nodes:
        logger.error(
            f"Cannot execute failover: No replicas found for the primary of shard {shard_id}. "
            f"Failover requires at least one replica to promote."
        )
        return False

    # A random replica that holds the primary's data, waiting (within the timeout) for one to sync.
    replica, refused = _first_synced(replica_nodes, target, rng, timeout, stopped_ports)
    if not replica:
        if stopped_ports and all(node["port"] in stopped_ports for node in replica_nodes):
            return True
        logger.error(
            f"Cannot execute failover: no replica of the primary of shard {shard_id} synced within "
            f"{timeout:.2f}s: {refused}"
        )
        return False

    logger.info(
        f"Executing CLUSTER {'FAILOVER FORCE' if force else 'FAILOVER'} from the replica at port {replica['port']}"
    )
    with valkey_client(replica["host"], replica["port"], timeout=5.0) as replica_client:
        replica_client.execute_command("CLUSTER", "FAILOVER", *(["FORCE"] if force else []))
    return wait_until_settled(
        connection, timeout, shard_id=shard_id, old_primary=target["node_id"], stopped_ports=stopped_ports
    )


def wait_until_settled(
    connection: DeploymentConnection,
    timeout: float,
    *,
    shard_id: int,
    old_primary: Optional[str],
    stopped_ports: Collection[int] = (),
) -> bool:
    """Wait up to ``timeout`` seconds for the shard's failover to settle; True once it has.

    Settled means the shard has one live primary, not ``old_primary``,
    and every live replica of the shard not stopped on purpose is linked
    to it. Other shards do not count: their link health is the state
    validation's to judge, and another shard's primary killed in the same
    wave would otherwise fail this failover.
    """
    logger.info(f"Waiting for the failover of shard {shard_id} to settle (timeout: {timeout:.2f}s)")
    if poll_until(
        lambda: _shard_settled(connection, shard_id, old_primary, stopped_ports),
        timeout=timeout,
        interval=0.5,
        what=f"shard {shard_id} failover",
    ):
        return True
    logger.warning(f"Shard {shard_id} did not settle within {timeout:.2f}s")
    return False


def _shard_settled(
    connection: DeploymentConnection, shard_id: int, old_primary: Optional[str], stopped_ports: Collection[int]
) -> bool:
    live = connection.get_current_nodes(include_failed=False)
    members = [node for node in live if node.get("shard_id") == shard_id and node["port"] not in stopped_ports]
    primaries = [node for node in members if node.get("role") == "primary"]
    if len(primaries) != 1 or primaries[0].get("node_id") == old_primary:
        return False
    primary = primaries[0]
    for replica in members:
        if replica is primary:
            continue
        try:
            with valkey_client(replica["host"], replica["port"], timeout=2.0) as client:
                info = query_replication(client)
        except Exception:
            return False
        if info.role != "replica" or not info.link_up or info.primary_port != primary["port"]:
            return False
    return True
