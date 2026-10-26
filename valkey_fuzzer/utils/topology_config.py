"""Shared parsing and validation for local Valkey topology configuration."""

import copy
from collections.abc import Mapping
from typing import Any

from ..models import ClusterConfig, TopologyType
from .server_config import canonicalize_server_config, validate_server_config

MAX_TCP_PORT = 65535
CLUSTER_BUS_PORT_OFFSET = 10000
LOCAL_TOPOLOGY_DOCUMENT_FIELDS = frozenset(
    {
        "type",
        "num_shards",
        "replicas_per_shard",
        "base_port",
        "base_data_dir",
        "valkey_binary",
        "enable_cleanup",
        "server_config",
        "node_configs",
        "node_binaries",
        "node_data_roots",
        "addressing",
    }
)
ADDRESSING_MODES = ("shared_loopback", "per_node_loopback")
# node-N binds 127.0.0.(N+2); the last usable address is 127.0.0.254.
MAX_PER_NODE_LOOPBACK_NODES = 253


def validate_topology_dimensions(
    topology_type: TopologyType,
    num_shards: Any,
    replicas_per_shard: Any,
) -> None:
    """Validate node-count dimensions for a supported local topology."""
    if not isinstance(num_shards, int) or isinstance(num_shards, bool):
        raise ValueError(f"num_shards must be an integer, got {num_shards}")
    if not isinstance(replicas_per_shard, int) or isinstance(replicas_per_shard, bool):
        raise ValueError(f"replicas_per_shard must be an integer, got {replicas_per_shard}")

    if topology_type == TopologyType.CLUSTER:
        if not (3 <= num_shards <= 16):
            raise ValueError(f"num_shards must be between 3 and 16, got {num_shards}")
        if not (0 <= replicas_per_shard <= 5):
            raise ValueError(f"replicas_per_shard must be between 0 and 5, got {replicas_per_shard}")
    elif topology_type == TopologyType.REPLICATION:
        if num_shards != 1:
            raise ValueError(f"replication topology requires num_shards to be 1, got {num_shards}")
        if not (1 <= replicas_per_shard <= 5):
            raise ValueError(
                f"replication topology requires replicas_per_shard between 1 and 5, got {replicas_per_shard}"
            )
    elif topology_type == TopologyType.STANDALONE:
        if num_shards != 1:
            raise ValueError(f"standalone topology requires num_shards to be 1, got {num_shards}")
        if replicas_per_shard != 0:
            raise ValueError(f"standalone topology requires replicas_per_shard to be 0, got {replicas_per_shard}")
    else:  # pragma: no cover - TopologyType construction normally prevents this
        raise ValueError(f"unsupported topology type: {topology_type}")


def valid_node_config_selectors(
    topology_type: TopologyType,
    num_shards: int,
    replicas_per_shard: int,
) -> set[str]:
    """Return role and exact-node selectors valid for a planned topology."""
    role_selectors = {"primary"}
    if replicas_per_shard > 0:
        role_selectors.add("replica")

    if topology_type == TopologyType.CLUSTER:
        node_count = num_shards * (1 + replicas_per_shard)
    else:
        node_count = 1 + replicas_per_shard

    return role_selectors | {f"node-{node_index}" for node_index in range(node_count)}


def _validate_runtime_field_types(topology_document: Mapping[str, Any]) -> None:
    """Validate fields consumed directly by the local process orchestrator."""
    base_data_dir = topology_document.get("base_data_dir", "/tmp/valkey-fuzzer")
    if not isinstance(base_data_dir, str) or not base_data_dir or "\0" in base_data_dir:
        raise ValueError("base_data_dir must be a non-empty string without null bytes")

    valkey_binary = topology_document.get("valkey_binary", "")
    if not isinstance(valkey_binary, str) or "\0" in valkey_binary:
        raise ValueError("valkey_binary must be a string without null bytes")

    enable_cleanup = topology_document.get("enable_cleanup", True)
    if not isinstance(enable_cleanup, bool):
        raise ValueError("enable_cleanup must be a boolean")


def _parse_node_paths(
    topology_document: Mapping[str, Any],
    field_name: str,
    selectors: set[str],
    topology_type: TopologyType,
) -> dict[str, str]:
    """Validate a ``{role-or-node-id: path}`` mapping (pure)."""
    value = topology_document.get(field_name, {})
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must map roles or node IDs to paths")
    parsed = {}
    for selector, path in value.items():
        if not isinstance(selector, str):
            raise ValueError(f"{field_name} keys must be role or node ID strings")
        if selector not in selectors:
            raise ValueError(f"Unknown {field_name} selector '{selector}' for {topology_type.value} topology")
        if not isinstance(path, str) or not path or "\0" in path:
            raise ValueError(f"{field_name}.{selector} must be a non-empty path without null bytes")
        parsed[selector] = path
    return parsed


def cluster_config_from_topology_document(
    topology_document: Mapping[str, Any],
    *,
    strict: bool = False,
) -> ClusterConfig:
    """Build a fresh :class:`ClusterConfig` from a topology document.

    This function is pure: it does not resolve binaries, allocate operating
    system resources, create directories, or open sockets. ``strict=False``
    preserves the existing v2 parser's behavior. Local v3 providers use strict
    mode to reject fields and runtime values they cannot safely interpret.
    """
    if not isinstance(topology_document, Mapping):
        raise ValueError("topology: Must be a mapping")
    if "type" not in topology_document:
        raise ValueError("Missing required topology field: type")

    if strict:
        unknown_fields = sorted(set(topology_document) - LOCAL_TOPOLOGY_DOCUMENT_FIELDS)
        if unknown_fields:
            raise ValueError(f"Unsupported local topology fields: {unknown_fields}")
        _validate_runtime_field_types(topology_document)

    try:
        topology_type = TopologyType(topology_document["type"])
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid topology type: {topology_document['type']}") from exc

    defaults = {
        TopologyType.STANDALONE: (1, 0),
        TopologyType.REPLICATION: (1, 1),
        TopologyType.CLUSTER: (3, 1),
    }
    default_shards, default_replicas = defaults[topology_type]
    num_shards = topology_document.get("num_shards", default_shards)
    replicas_per_shard = topology_document.get("replicas_per_shard", default_replicas)

    validate_topology_dimensions(topology_type, num_shards, replicas_per_shard)
    base_port = topology_document.get("base_port", 7000)
    validate_topology_base_port(
        topology_type,
        num_shards,
        replicas_per_shard,
        base_port,
    )

    server_config = topology_document.get("server_config", {})
    if not isinstance(server_config, Mapping):
        raise ValueError("server_config must be a mapping")
    try:
        if strict:
            copied_server_config = copy.deepcopy(canonicalize_server_config(server_config))
        else:
            validate_server_config(server_config)
            copied_server_config = copy.deepcopy(dict(server_config))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid server_config: {exc}") from exc

    node_configs = topology_document.get("node_configs", {})
    if not isinstance(node_configs, Mapping):
        raise ValueError("node_configs must be a mapping")

    selectors = valid_node_config_selectors(
        topology_type,
        num_shards,
        replicas_per_shard,
    )
    copied_node_configs = {}
    for node_id, node_config in node_configs.items():
        if not isinstance(node_id, str):
            raise ValueError("node_configs keys must be node ID strings")
        if not isinstance(node_config, Mapping):
            raise ValueError(f"node_configs.{node_id} must be a mapping")
        if node_id not in selectors:
            raise ValueError(f"Unknown node_configs selector '{node_id}' for {topology_type.value} topology")
        try:
            if strict:
                copied_node_config = copy.deepcopy(canonicalize_server_config(node_config))
            else:
                validate_server_config(node_config)
                copied_node_config = copy.deepcopy(dict(node_config))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid node_configs.{node_id}: {exc}") from exc
        copied_node_configs[node_id] = copied_node_config

    node_binaries = _parse_node_paths(topology_document, "node_binaries", selectors, topology_type)
    node_data_roots = _parse_node_paths(topology_document, "node_data_roots", selectors, topology_type)
    addressing = topology_document.get("addressing", "shared_loopback")
    if addressing not in ADDRESSING_MODES:
        raise ValueError(f"addressing must be one of {list(ADDRESSING_MODES)}, got {addressing!r}")
    node_count = (
        num_shards * (1 + replicas_per_shard) if topology_type == TopologyType.CLUSTER else 1 + replicas_per_shard
    )
    if addressing == "per_node_loopback" and node_count > MAX_PER_NODE_LOOPBACK_NODES:
        raise ValueError(f"per_node_loopback supports at most {MAX_PER_NODE_LOOPBACK_NODES} nodes")

    kwargs = {
        "num_shards": num_shards,
        "replicas_per_shard": replicas_per_shard,
        "base_port": base_port,
        "topology_type": topology_type,
        "server_config": copied_server_config,
        "node_configs": copied_node_configs,
        "node_binaries": node_binaries,
        "node_data_roots": node_data_roots,
        "addressing": addressing,
    }
    if "base_data_dir" in topology_document:
        kwargs["base_data_dir"] = topology_document["base_data_dir"]
    if "valkey_binary" in topology_document:
        kwargs["valkey_binary"] = topology_document["valkey_binary"]
    if "enable_cleanup" in topology_document:
        kwargs["enable_cleanup"] = topology_document["enable_cleanup"]

    return ClusterConfig(**kwargs)


def validate_base_port_number(base_port: Any) -> None:
    """Validate that a base port is a usable TCP port number."""
    if not isinstance(base_port, int) or isinstance(base_port, bool):
        raise ValueError(f"base_port must be an integer, got {base_port}")
    if not (1 <= base_port <= MAX_TCP_PORT):
        raise ValueError(f"base_port must be between 1 and {MAX_TCP_PORT}, got {base_port}")


def validate_topology_base_port(
    topology_type: TopologyType,
    num_shards: int,
    replicas_per_shard: int,
    base_port: Any,
) -> None:
    """Ensure every client and cluster bus port fits in the TCP port range."""
    validate_base_port_number(base_port)

    if topology_type == TopologyType.CLUSTER:
        node_count = num_shards * (1 + replicas_per_shard)
    else:
        node_count = 1 + replicas_per_shard

    highest_client_port = base_port + node_count - 1
    if highest_client_port > MAX_TCP_PORT:
        raise ValueError(
            f"base_port {base_port} with {node_count} nodes would require client port "
            f"{highest_client_port}; maximum is {MAX_TCP_PORT}"
        )

    if topology_type == TopologyType.CLUSTER:
        highest_bus_port = highest_client_port + CLUSTER_BUS_PORT_OFFSET
        if highest_bus_port > MAX_TCP_PORT:
            raise ValueError(
                f"base_port {base_port} with {node_count} cluster nodes would require cluster bus port "
                f"{highest_bus_port}; maximum is {MAX_TCP_PORT}"
            )
