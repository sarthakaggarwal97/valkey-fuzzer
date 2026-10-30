"""Short-lived Valkey clients and the INFO and CLUSTER queries the local deployment runs."""

import logging
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Optional, TypeVar

import valkey

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


def poll_until(
    check: Callable[[], bool],
    *,
    timeout: float,
    interval: float,
    what: str,
    retry: tuple[type[Exception], ...] = (),
) -> bool:
    """Call ``check`` until it returns True (then True) or ``timeout`` seconds pass (then False).

    Time is measured on the monotonic clock, so a wall clock that moves
    never cuts a wait short or stretches it. ``check`` runs at least once; an
    exception of a ``retry`` type counts as not yet, any other propagates.
    """
    deadline = time.monotonic() + max(timeout, 0.0)
    while True:
        try:
            if check():
                return True
        except retry as exc:
            logger.debug("%s: not yet: %s", what, exc)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(max(interval, 0.0), remaining))


def _int(value: Any) -> Optional[int]:
    try:
        return int(response_text(value))
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class Replication:
    """One instance's INFO replication, in our terms.

    Valkey's own field names (``master_link_status``, ``slave_repl_offset``,
    ``connected_slaves``, ...) are read here and nowhere else. A field the
    reply lacks is None, never a guess.
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


def parse_replication(info: Mapping[str, Any]) -> Replication:
    """An INFO replication reply as a :class:`Replication`."""

    host = info.get("master_host")
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
    )


def query_replication(client: valkey.Valkey) -> Replication:
    """INFO replication from ``client``'s server."""

    return parse_replication(client.info("replication"))


def parse_cluster_info(response: object) -> dict[str, str]:
    """Parse CLUSTER INFO output from raw or mapping-based client responses."""
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
    return parse_cluster_info(client.execute_command("CLUSTER", "INFO"))


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
        if client is not None:
            try:
                client.close()
            except Exception:
                pass  # Ignore errors during cleanup


def safe_query_node(
    node: dict, timeout: float, query_func: Callable[[valkey.Valkey], T], error_message: str = "Error querying node"
) -> Optional[T]:
    try:
        with valkey_client(node["host"], node["port"], timeout) as client:
            return query_func(client)
    except Exception as e:
        logger.debug(f"{error_message} {node['host']}:{node['port']}: {e}")
        return None


def query_cluster_nodes(node: dict, timeout: float = 2.0) -> Optional[list[dict]]:
    from .cluster_nodes import parse_cluster_nodes

    def query_func(client: valkey.Valkey) -> list[dict]:
        return parse_cluster_nodes(response_text(client.execute_command("CLUSTER", "NODES")))

    return safe_query_node(node, timeout, query_func, error_message="Error querying CLUSTER NODES from")
