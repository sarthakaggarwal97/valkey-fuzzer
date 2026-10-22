"""Shared validation and argv conversion for Valkey server configuration."""

import re
from collections.abc import Iterable, Mapping
from enum import Enum
from typing import Any, Optional

_SERVER_OPTION_RE = re.compile(r"[a-z0-9][a-z0-9-]*")

# These options describe process/topology metadata owned by the orchestrator.
# Feature scenarios must not override them because doing so would invalidate
# node identity, lifecycle, or the requested topology.
MANAGED_SERVER_OPTIONS = frozenset(
    {
        "bind",
        "cluster-announce-bus-port",
        "cluster-announce-hostname",
        "cluster-announce-human-nodename",
        "cluster-announce-ip",
        "cluster-announce-port",
        "cluster-announce-tls-port",
        "cluster-config-file",
        "cluster-enabled",
        "cluster-port",
        "daemonize",
        "dir",
        "include",
        "logfile",
        "pidfile",
        "port",
        "protected-mode",
        "replica-announce-ip",
        "replica-announce-port",
        "replicaof",
        "slave-announce-ip",
        "slave-announce-port",
        "slaveof",
        "tls-port",
        "unixsocket",
        "unixsocketperm",
    }
)


def normalize_server_option(raw_option: object) -> str:
    """Normalize a config key and reject unsafe orchestrator-owned options."""
    if not isinstance(raw_option, str):
        raise TypeError(f"Server configuration keys must be strings, got {type(raw_option).__name__}")

    option = raw_option.strip().lower()
    while option.startswith("--"):
        option = option[2:]
    option = option.replace("_", "-")

    if not option or not _SERVER_OPTION_RE.fullmatch(option):
        raise ValueError(f"Invalid Valkey server configuration option: {raw_option!r}")
    if option in MANAGED_SERVER_OPTIONS:
        raise ValueError(f"Server configuration option {option!r} is managed by the orchestrator")
    return option


def server_config_values(option: str, raw_value: object) -> Iterable[str]:
    """Yield complete scalar values for one or more occurrences of an option.

    A list represents repeated config directives. Multi-token Valkey directives
    should use one scalar string per occurrence, for example
    ``"replica 256mb 64mb 60"``.
    """
    values: Iterable[object]
    if isinstance(raw_value, (list, tuple)):
        values = raw_value
    else:
        values = (raw_value,)

    for value in values:
        if value is None:
            raise ValueError(f"Server configuration option {option!r} cannot have a null value")
        if isinstance(value, (dict, list, tuple, set)):
            raise TypeError(f"Server configuration option {option!r} has an unsupported nested value")
        if isinstance(value, Enum):
            value = value.value
        if isinstance(value, bool):
            yield "yes" if value else "no"
        elif isinstance(value, bytes):
            yield value.decode("utf-8")
        else:
            yield str(value)


def canonicalize_server_config(server_config: Optional[Mapping[str, Any]]) -> dict[str, Any]:
    """Validate a config mapping and return one entry per normalized option.

    Repeated directives must be represented by one list or tuple value. Two
    mapping keys such as ``repl_compression`` and ``repl-compression`` are
    ambiguous because both produce the same Valkey command-line option, so
    they are rejected instead of relying on argument order.
    """
    if server_config is None:
        return {}
    if not isinstance(server_config, Mapping):
        raise TypeError(f"Server configuration must be a mapping, got {type(server_config).__name__}")

    canonical = {}
    original_keys = {}
    for raw_option, raw_value in server_config.items():
        option = normalize_server_option(raw_option)
        # Materialize once to validate the complete value without changing its
        # representation; callers may need to preserve repeated directives.
        tuple(server_config_values(option, raw_value))
        if option in canonical:
            raise ValueError(
                f"Server configuration keys {original_keys[option]!r} and {raw_option!r} both normalize to {option!r}"
            )
        canonical[option] = raw_value
        original_keys[option] = raw_option
    return canonical


def build_server_config_arguments(server_config: Optional[Mapping[str, Any]]) -> list[str]:
    """Validate a configuration mapping and convert it to process arguments."""
    if server_config is None:
        return []
    if not isinstance(server_config, Mapping):
        raise TypeError(f"Server configuration must be a mapping, got {type(server_config).__name__}")

    arguments: list[str] = []
    for raw_option, raw_value in server_config.items():
        option = normalize_server_option(raw_option)
        for value in server_config_values(option, raw_value):
            arguments.extend((f"--{option}", value))
    return arguments


def validate_server_config(server_config: Optional[Mapping[str, Any]]) -> None:
    """Validate syntax, safety, and value shape without requiring a binary."""
    build_server_config_arguments(server_config)
