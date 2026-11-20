"""Short-lived Valkey clients and the INFO and CLUSTER queries the local deployment runs."""

import logging
import re
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Optional, TypeVar

import valkey

from .cluster_nodes import parse_cluster_nodes

logger = logging.getLogger(__name__)

T = TypeVar("T")


# INFO replication reports the role as "master" or "slave".
_REPLICATION_ROLES = {"master": "primary", "primary": "primary", "slave": "replica", "replica": "replica"}


def response_text(value: object) -> str:
    """Normalize RESP2/RESP3 response components to text."""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8", errors="replace")
    return str(value)


def replication_role(info: Mapping[str, Any]) -> Optional[str]:
    """The role in an INFO replication reply, as "primary" or "replica"; None if it reports neither."""
    return _REPLICATION_ROLES.get(response_text(info.get("role", "")).lower())


def _int(value: Any) -> Optional[int]:
    try:
        return int(response_text(value))
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class ReplicaEntry:
    """One replica a primary's INFO replication lists (a ``slaveN`` line), with Valkey's field names.

    A field the line lacks is None; ``fields`` holds every field the line
    has, as text, including ones not named here.
    """

    ip: Optional[str]
    port: Optional[int]
    state: Optional[str]  # e.g. "online", "wait_bgsave"
    offset: Optional[int]
    lag: Optional[int]
    type: Optional[str]  # e.g. "replica", or "rdb-channel" during a dual-channel sync
    fields: Mapping[str, str]


# A replica line of INFO replication: slave0, slave1, ...
_REPLICA_KEY = re.compile(r"slave(\d+)")


def _replica_entries(info: Mapping[str, Any]) -> tuple[ReplicaEntry, ...]:
    """The ``slaveN`` lines of an INFO replication reply, by N."""
    entries = []
    for key, value in info.items():
        match = _REPLICA_KEY.fullmatch(response_text(key))
        if match and isinstance(value, Mapping):
            text = {response_text(name): response_text(item) for name, item in value.items()}
            entry = ReplicaEntry(
                ip=text.get("ip"),
                port=_int(value.get("port")),
                state=text.get("state"),
                offset=_int(value.get("offset")),
                lag=_int(value.get("lag")),
                type=text.get("type"),
                fields=text,
            )
            entries.append((int(match.group(1)), entry))
    return tuple(entry for _, entry in sorted(entries, key=lambda item: item[0]))


@dataclass(frozen=True)
class Replication:
    """One instance's INFO replication, in our terms.

    Valkey's own field names (``master_link_status``, ``slave_repl_offset``,
    ``connected_slaves``, ``master_replid``, ``slaveN``, ...) are read here
    and nowhere else. A field the reply lacks is None, never a guess.
    """

    role: Optional[str]  # "primary" or "replica"
    primary_host: Optional[str] = None
    primary_port: Optional[int] = None
    link_up: bool = False
    sync_in_progress: bool = False
    last_io_seconds_ago: Optional[int] = None
    replica_offset: Optional[int] = None  # how far this replica has applied its primary's stream
    offset: Optional[int] = None  # this instance's replication offset (a primary's, for a primary)
    connected_replicas: Optional[int] = None
    replication_id: Optional[str] = None  # the replication ID this instance's offset counts in
    failover_state: Optional[str] = None  # a FAILOVER's progress, e.g. "no-failover"
    replicas: tuple[ReplicaEntry, ...] = ()  # a primary's connected replicas (``slaveN``), by N


def parse_replication(info: Mapping[str, Any]) -> Replication:
    """An INFO replication reply as a :class:`Replication`."""

    host = info.get("master_host")
    replication_id = info.get("master_replid")
    failover_state = info.get("master_failover_state")
    return Replication(
        role=replication_role(info),
        primary_host=None if host is None else response_text(host),
        primary_port=_int(info.get("master_port")),
        link_up=response_text(info.get("master_link_status", "")).lower() == "up",
        sync_in_progress=response_text(info.get("master_sync_in_progress", 0)) not in ("0", "False"),
        last_io_seconds_ago=_int(info.get("master_last_io_seconds_ago")),
        replica_offset=_int(info.get("slave_repl_offset")),
        offset=_int(info.get("master_repl_offset")),
        connected_replicas=_int(info.get("connected_slaves")),
        replication_id=None if replication_id is None else response_text(replication_id),
        failover_state=None if failover_state is None else response_text(failover_state),
        replicas=_replica_entries(info),
    )


def query_replication(client: valkey.Valkey) -> Replication:
    """INFO replication from ``client``'s server."""

    return parse_replication(client.info("replication"))


def parse_info_text(response: object) -> dict[str, str]:
    """The ``name:value`` fields of an INFO or CLUSTER INFO ``response``: the server's text (one field per line;
    a missing reply, as ``""``, has none), or the mapping a decoding client made of it, as text."""
    if isinstance(response, Mapping):
        return {
            response_text(key).strip(): response_text(value).strip()
            for key, value in response.items()
            if response_text(key).strip()
        }

    info: dict[str, str] = {}
    for raw_line in response_text(response).splitlines():
        line = raw_line.strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        if key:
            info[key] = value.strip()
    return info


def query_cluster_info(client: valkey.Valkey) -> dict[str, str]:
    """Query the canonical CLUSTER INFO command and normalize its response."""
    return parse_info_text(client.execute_command("CLUSTER", "INFO"))


@contextmanager
def valkey_client(host: str, port: int, timeout: float, decode_responses: bool = True) -> Iterator[valkey.Valkey]:
    client = None
    try:
        client = valkey.Valkey(
            host=host,
            port=port,
            socket_timeout=timeout,
            socket_connect_timeout=timeout,
            decode_responses=decode_responses,
        )
        yield client
    finally:
        close_quietly(client)


def close_quietly(*clients: Any) -> None:
    """Close every client that is not None; a connection that is already gone is not an error."""

    for client in clients:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass


# How many GETs one read pipeline sends.
READ_BATCH = 1000


def read_keys(client: Any, keys: Sequence[str], *, readonly: bool = False, raise_on_error: bool = True) -> list:
    """GET every key of ``keys`` over non-transactional pipelines of :data:`READ_BATCH` (cluster nodes reject a
    cross-slot MGET): the values, in order. With ``readonly``, ``READONLY`` is sent once first: a cluster replica
    answers for its shard's keys only after it. With ``raise_on_error`` false a key's error reply is its exception,
    in its place, instead of being raised."""

    if readonly:
        client.execute_command("READONLY")
    values: list = []
    for start in range(0, len(keys), READ_BATCH):
        pipeline = client.pipeline(transaction=False)
        for key in keys[start : start + READ_BATCH]:
            pipeline.get(key)
        values.extend(pipeline.execute(raise_on_error=raise_on_error))
    if len(values) != len(keys):
        raise RuntimeError(f"Pipelined read returned {len(values)} values for {len(keys)} keys")
    return values


def query_or_none(host: str, port: int, timeout: float, query: Callable[[valkey.Valkey], T]) -> Optional[T]:
    """``query`` on a short-lived client of ``host:port``; None when the server did not answer it."""
    try:
        with valkey_client(host, port, timeout) as client:
            return query(client)
    except (valkey.ValkeyError, OSError) as exc:
        logger.debug("%s:%s did not answer: %s", host, port, exc)
        return None


def query_cluster_nodes(node: dict, timeout: float = 2.0) -> Optional[list[dict]]:
    def query(client: valkey.Valkey) -> list[dict]:
        return parse_cluster_nodes(response_text(client.execute_command("CLUSTER", "NODES")))

    return query_or_none(node["host"], node["port"], timeout, query)
