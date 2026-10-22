from unittest.mock import Mock

import pytest

from valkey_fuzzer.utils.valkey_utils import parse_cluster_info_response, query_cluster_info


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        (
            "cluster_state:ok\r\ncluster_known_nodes:3\r\n",
            {"cluster_state": "ok", "cluster_known_nodes": "3"},
        ),
        (
            b"cluster_state:fail\ncluster_known_nodes:1\n",
            {"cluster_state": "fail", "cluster_known_nodes": "1"},
        ),
        (
            {b"cluster_state": b"ok", "cluster_known_nodes": 3},
            {"cluster_state": "ok", "cluster_known_nodes": "3"},
        ),
        (
            "cluster_state:ok\nfuture_field:value:with:colons\nmalformed-line\n",
            {"cluster_state": "ok", "future_field": "value:with:colons"},
        ),
    ],
)
def test_parse_cluster_info_response_accepts_client_response_variants(response, expected):
    assert parse_cluster_info_response(response) == expected


def test_query_cluster_info_uses_canonical_command():
    client = Mock()
    client.execute_command.return_value = "cluster_state:ok\r\n"

    assert query_cluster_info(client) == {"cluster_state": "ok"}
    client.execute_command.assert_called_once_with("CLUSTER", "INFO")
