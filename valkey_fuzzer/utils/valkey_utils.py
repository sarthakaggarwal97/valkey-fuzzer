"""
Valkey client utilities for safe connection management and common query patterns
"""

import logging
from collections.abc import Mapping
from contextlib import contextmanager
from typing import Callable, Optional, TypeVar

import valkey

logger = logging.getLogger(__name__)

T = TypeVar("T")


def _response_text(value: object) -> str:
    """Normalize RESP2/RESP3 response components to text."""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8", errors="replace")
    return str(value)


def parse_cluster_info_response(response: object) -> dict[str, str]:
    """Parse CLUSTER INFO output from raw or mapping-based client responses."""
    if isinstance(response, Mapping):
        return {
            _response_text(key).strip(): _response_text(value).strip()
            for key, value in response.items()
            if _response_text(key).strip()
        }

    info: dict[str, str] = {}
    for raw_line in _response_text(response).splitlines():
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
    return parse_cluster_info_response(client.execute_command("CLUSTER", "INFO"))


@contextmanager
def valkey_client(host: str, port: int, timeout: float, decode_responses: bool = True):
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
    from .cluster_parser import parse_cluster_nodes_raw

    def query_func(client: valkey.Valkey) -> list[dict]:
        cluster_nodes_raw = client.execute_command("CLUSTER", "NODES")
        # Handle both bytes and string responses
        if isinstance(cluster_nodes_raw, bytes):
            cluster_nodes_raw = cluster_nodes_raw.decode()
        return parse_cluster_nodes_raw(cluster_nodes_raw)

    return safe_query_node(node, timeout, query_func, error_message="Error querying CLUSTER NODES from")


def is_node_alive(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with valkey_client(host, port, timeout, decode_responses=True) as client:
            client.ping()
            return True
    except Exception:
        return False
