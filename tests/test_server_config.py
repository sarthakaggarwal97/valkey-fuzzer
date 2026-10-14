"""Tests for shared server-configuration validation and argv conversion."""

import pytest

from src.utils.server_config import build_server_config_arguments


def test_server_config_normalizes_keys_and_booleans():
    assert build_server_config_arguments(
        {
            "--REPL_COMPRESSION": True,
            "save": "",
        }
    ) == [
        "--repl-compression",
        "yes",
        "--save",
        "",
    ]


def test_server_config_list_represents_repeated_directives():
    assert build_server_config_arguments(
        {
            "client-output-buffer-limit": [
                "normal 0 0 0",
                "replica 256mb 64mb 60",
            ]
        }
    ) == [
        "--client-output-buffer-limit",
        "normal 0 0 0",
        "--client-output-buffer-limit",
        "replica 256mb 64mb 60",
    ]


@pytest.mark.parametrize(
    "option",
    [
        "port",
        "cluster_enabled",
        "--replicaof",
        "include",
        "bind",
        "protected_mode",
        "tls-port",
        "unixsocket",
        "cluster-announce-ip",
        "cluster-announce-port",
        "cluster-announce-bus-port",
        "replica-announce-ip",
        "replica-announce-port",
    ],
)
def test_server_config_rejects_orchestrator_owned_options(option):
    with pytest.raises(ValueError, match="managed by the orchestrator"):
        build_server_config_arguments({option: "value"})
