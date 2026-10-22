"""
Tests for State Validator
"""

import time
from unittest.mock import Mock, patch

import pytest

from valkey_fuzzer.fuzzer_engine.state_validator import ClusterStatusValidator, DataConsistencyValidator, StateValidator
from valkey_fuzzer.models import (
    ClusterStatusValidationConfig,
    DataConsistencyValidationConfig,
    StateValidationConfig,
    TopologyType,
)


def test_killed_nodes_tracking():
    """Test that StateValidator tracks killed nodes correctly"""
    config = StateValidationConfig()
    validator = StateValidator(config)

    assert len(validator.killed_nodes) == 0

    validator.register_killed_node("127.0.0.1:6379", "primary", 0)
    validator.register_killed_node("127.0.0.1:6380", "replica", 1)

    assert len(validator.killed_nodes) == 2
    validator.clear_killed_nodes()
    assert len(validator.killed_nodes) == 0


def _client_context(client):
    context = Mock()
    context.__enter__ = Mock(return_value=client)
    context.__exit__ = Mock(return_value=False)
    return context


def test_cluster_status_uses_cluster_info_command_for_health_fields():
    validator = ClusterStatusValidator()
    connection = Mock()
    connection.get_current_nodes.return_value = [
        {
            "node_id": "primary-1",
            "host": "127.0.0.1",
            "port": 7000,
            "role": "primary",
            "shard_id": 0,
            "status": "connected",
        }
    ]
    client = Mock()
    client.info.return_value = {"cluster_enabled": 1}
    client.execute_command.return_value = (
        "cluster_state:ok\r\n"
        "cluster_slots_assigned:16384\r\n"
        "cluster_known_nodes:1\r\n"
        "cluster_stats_messages_ping_sent:12:34\r\n"
    )

    with patch(
        "valkey_fuzzer.fuzzer_engine.state_validator.valkey_client",
        return_value=_client_context(client),
    ):
        result = validator.validate(connection, ClusterStatusValidationConfig())

    assert result.success is True
    assert result.cluster_state == "ok"
    client.execute_command.assert_called_once_with("CLUSTER", "INFO")
    client.info.assert_not_called()


def test_cluster_status_rejects_missing_state_even_when_failed_node_matches_chaos():
    validator = ClusterStatusValidator()
    connection = Mock()
    connection.get_current_nodes.return_value = [
        {
            "node_id": "primary-1",
            "host": "127.0.0.1",
            "port": 7000,
            "role": "primary",
            "shard_id": 0,
            "status": "connected",
        },
        {
            "node_id": "replica-1",
            "host": "127.0.0.1",
            "port": 7001,
            "role": "replica",
            "shard_id": 0,
            "status": "failed",
        },
    ]
    malformed_client = Mock()
    malformed_client.execute_command.return_value = "cluster_known_nodes:2\r\n"

    with patch(
        "valkey_fuzzer.fuzzer_engine.state_validator.valkey_client",
        side_effect=[
            _client_context(malformed_client),
            ConnectionError("killed replica"),
        ],
    ):
        result = validator.validate(
            connection,
            ClusterStatusValidationConfig(acceptable_states=["ok", "unknown"]),
            killed_nodes={"127.0.0.1:7001"},
        )

    assert result.success is False
    assert result.cluster_state == "unknown"
    assert "missing or blank cluster_state" in result.error_message


def test_cluster_status_does_not_implicitly_allow_reported_unknown_after_chaos():
    validator = ClusterStatusValidator()
    connection = Mock()
    connection.get_current_nodes.return_value = [
        {
            "node_id": "primary-1",
            "host": "127.0.0.1",
            "port": 7000,
            "role": "primary",
            "shard_id": 0,
            "status": "connected",
        },
        {
            "node_id": "replica-1",
            "host": "127.0.0.1",
            "port": 7001,
            "role": "replica",
            "shard_id": 0,
            "status": "failed",
        },
    ]
    client = Mock()
    client.execute_command.return_value = "cluster_state:unknown\r\n"

    with patch(
        "valkey_fuzzer.fuzzer_engine.state_validator.valkey_client",
        side_effect=[
            _client_context(client),
            ConnectionError("killed replica"),
        ],
    ):
        result = validator.validate(
            connection,
            ClusterStatusValidationConfig(),
            killed_nodes={"127.0.0.1:7001"},
        )

    assert result.success is False
    assert result.cluster_state == "unknown"
    assert "unacceptable states" in result.error_message


def test_cluster_status_rejects_mixed_node_reports_instead_of_majority_voting():
    validator = ClusterStatusValidator()
    connection = Mock()
    connection.get_current_nodes.return_value = [
        {
            "node_id": f"primary-{index}",
            "host": "127.0.0.1",
            "port": 7000 + index,
            "role": "primary",
            "shard_id": index,
            "status": "connected",
        }
        for index in range(3)
    ]
    clients = [Mock(), Mock(), Mock()]
    for client, state in zip(clients, ("ok", "fail", "ok")):
        client.execute_command.return_value = f"cluster_state:{state}\r\n"

    with patch(
        "valkey_fuzzer.fuzzer_engine.state_validator.valkey_client",
        side_effect=[_client_context(client) for client in clients],
    ):
        result = validator.validate(connection, ClusterStatusValidationConfig())

    assert result.success is False
    assert result.cluster_state == "mixed"
    assert "Cluster state disagreement" in result.error_message
    assert "127.0.0.1:7001=fail" in result.error_message


def test_cluster_status_allows_mixed_reports_only_when_each_state_is_explicitly_accepted():
    validator = ClusterStatusValidator()
    connection = Mock()
    connection.get_current_nodes.return_value = [
        {
            "node_id": f"primary-{index}",
            "host": "127.0.0.1",
            "port": 7000 + index,
            "role": "primary",
            "shard_id": index,
            "status": "connected",
        }
        for index in range(2)
    ]
    ok_client = Mock()
    ok_client.execute_command.return_value = "cluster_state:ok\r\n"
    fail_client = Mock()
    fail_client.execute_command.return_value = "cluster_state:fail\r\n"

    with patch(
        "valkey_fuzzer.fuzzer_engine.state_validator.valkey_client",
        side_effect=[_client_context(ok_client), _client_context(fail_client)],
    ):
        result = validator.validate(
            connection,
            ClusterStatusValidationConfig(acceptable_states=["ok", "fail"]),
        )

    assert result.success is True
    assert result.cluster_state == "mixed"
    assert result.degraded_reason == "Cluster state disagreement explicitly allowed by acceptable_states"


def test_standalone_data_consistency_uses_direct_primary_client():
    validator = DataConsistencyValidator()
    connection = Mock(topology_type=TopologyType.STANDALONE)
    connection.get_live_nodes.return_value = [
        {
            "node_id": "primary-1",
            "host": "127.0.0.1",
            "port": 6379,
            "role": "primary",
            "shard_id": 0,
            "status": "connected",
        }
    ]
    direct_client = Mock()
    direct_client.get.side_effect = lambda key: validator.test_keys[key]
    config = DataConsistencyValidationConfig(
        num_test_keys=3,
        check_cross_replica_consistency=False,
    )

    with (
        patch(
            "valkey_fuzzer.fuzzer_engine.state_validator.valkey_client", return_value=_client_context(direct_client)
        ) as direct,
        patch("valkey_fuzzer.fuzzer_engine.state_validator.valkey.ValkeyCluster") as cluster_client,
    ):
        assert validator.write_test_keys(connection, config) is True
        result = validator.validate(connection, config)

    assert result.success is True
    assert result.test_keys_checked == 3
    assert direct.call_count == 2
    assert all(call.args == ("127.0.0.1", 6379, config.timeout) for call in direct.call_args_list)
    cluster_client.assert_not_called()


def test_replication_data_consistency_reads_primary_and_checks_replicas():
    validator = DataConsistencyValidator()
    validator.test_keys = {"fuzzer:test:0": "expected"}
    connection = Mock(topology_type=TopologyType.REPLICATION)
    connection.get_live_nodes.return_value = [
        {
            "node_id": "primary-1",
            "host": "127.0.0.1",
            "port": 6379,
            "role": "primary",
            "shard_id": 0,
            "status": "connected",
        },
        {
            "node_id": "replica-1",
            "host": "127.0.0.1",
            "port": 6380,
            "role": "replica",
            "shard_id": 0,
            "status": "connected",
        },
    ]
    primary_client = Mock()
    primary_client.get.return_value = "expected"
    replica_client = Mock()
    replica_client.get.return_value = "diverged"

    with (
        patch(
            "valkey_fuzzer.fuzzer_engine.state_validator.valkey_client",
            side_effect=[_client_context(primary_client), _client_context(replica_client)],
        ) as direct,
        patch("valkey_fuzzer.fuzzer_engine.state_validator.valkey.ValkeyCluster") as cluster_client,
    ):
        result = validator.validate(connection, DataConsistencyValidationConfig())

    assert result.success is False
    assert result.inconsistent_keys[0].inconsistency_type == "replica_divergence"
    assert [call.args[:2] for call in direct.call_args_list] == [
        ("127.0.0.1", 6379),
        ("127.0.0.1", 6380),
    ]
    cluster_client.assert_not_called()


def test_cluster_data_consistency_keeps_cluster_aware_routing():
    validator = DataConsistencyValidator()
    validator.test_keys = {"fuzzer:test:0": "expected"}
    connection = Mock(topology_type=TopologyType.CLUSTER)
    connection.get_live_nodes.return_value = [
        {
            "node_id": "primary-1",
            "host": "127.0.0.1",
            "port": 7000,
            "role": "primary",
            "shard_id": 0,
            "status": "connected",
        }
    ]
    cluster_client = Mock()
    cluster_client.get.return_value = "expected"

    with (
        patch(
            "valkey_fuzzer.fuzzer_engine.state_validator.valkey.ValkeyCluster", return_value=cluster_client
        ) as cluster_factory,
        patch("valkey_fuzzer.fuzzer_engine.state_validator.valkey_client") as direct,
    ):
        result = validator.validate(
            connection,
            DataConsistencyValidationConfig(check_cross_replica_consistency=False),
        )

    assert result.success is True
    cluster_factory.assert_called_once()
    direct.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


def test_topology_validation_zero_replicas_with_killed_primary():
    """Test that topology validation handles zero-replica clusters correctly when primaries are killed"""
    from unittest.mock import Mock

    from valkey_fuzzer.fuzzer_engine.state_validator import TopologyValidator
    from valkey_fuzzer.models import ExpectedTopology, TopologyValidationConfig

    config = TopologyValidationConfig()
    validator = TopologyValidator()

    # Mock cluster connection with zero replicas
    mock_connection = Mock()

    # Simulate a 3-primary, 0-replica cluster where 1 primary was killed
    mock_nodes = [
        {"node_id": "node1", "host": "127.0.0.1", "port": 7000, "role": "primary", "status": "ok"},
        {"node_id": "node2", "host": "127.0.0.1", "port": 7001, "role": "primary", "status": "ok"},
        {"node_id": "node3", "host": "127.0.0.1", "port": 7002, "role": "primary", "status": "failed"},
    ]
    mock_connection.get_current_nodes.return_value = mock_nodes

    # Expected topology: 3 primaries, 0 replicas
    expected_topology = ExpectedTopology(num_primaries=3, num_replicas=0, shard_structure=None)

    # Killed nodes set (node3 was killed)
    killed_nodes = {"127.0.0.1:7002"}

    # Validate topology
    result = validator.validate(
        cluster_connection=mock_connection,
        config=config,
        expected_topology=expected_topology,
        killed_nodes=killed_nodes,
    )

    # The adjusted expected replica count should be clamped at 0, not -1
    # With 0 replicas initially and 1 killed node, adjusted = max(0, 0 - 1) = 0
    assert result.expected_replicas == 0, "Expected replicas should be clamped at 0, not negative"
    assert result.actual_replicas == 0, "Actual replicas should be 0"

    # Should succeed since actual matches expected (both 0)
    assert result.success is True, "Topology validation should succeed for zero-replica cluster with killed primary"


def test_topology_validation_zero_replicas_multiple_killed():
    """Test that topology validation handles zero-replica clusters when multiple primaries are killed"""
    from unittest.mock import Mock

    from valkey_fuzzer.fuzzer_engine.state_validator import TopologyValidator
    from valkey_fuzzer.models import ExpectedTopology, TopologyValidationConfig

    config = TopologyValidationConfig()
    validator = TopologyValidator()

    # Mock cluster connection with zero replicas
    mock_connection = Mock()

    # Simulate a 3-primary, 0-replica cluster where 2 primaries were killed
    mock_nodes = [
        {"node_id": "node1", "host": "127.0.0.1", "port": 7000, "role": "primary", "status": "ok"},
        {"node_id": "node2", "host": "127.0.0.1", "port": 7001, "role": "primary", "status": "failed"},
        {"node_id": "node3", "host": "127.0.0.1", "port": 7002, "role": "primary", "status": "failed"},
    ]
    mock_connection.get_current_nodes.return_value = mock_nodes

    # Expected topology: 3 primaries, 0 replicas
    expected_topology = ExpectedTopology(num_primaries=3, num_replicas=0, shard_structure=None)

    # Killed nodes set (node2 and node3 were killed)
    killed_nodes = {"127.0.0.1:7001", "127.0.0.1:7002"}

    # Validate topology
    result = validator.validate(
        cluster_connection=mock_connection,
        config=config,
        expected_topology=expected_topology,
        killed_nodes=killed_nodes,
    )

    # The adjusted expected replica count should be clamped at 0, not -2
    # With 0 replicas initially and 2 killed nodes, adjusted = max(0, 0 - 2) = 0
    assert result.expected_replicas == 0, "Expected replicas should be clamped at 0, not negative"
    assert result.actual_replicas == 0, "Actual replicas should be 0"

    # Should succeed since actual matches expected (both 0)
    assert result.success is True, (
        "Topology validation should succeed for zero-replica cluster with multiple killed primaries"
    )


def test_validate_cluster_state_zero_replicas():
    """Test that validate_cluster_state correctly handles zero-replica clusters"""
    from unittest.mock import Mock, patch

    from valkey_fuzzer.fuzzer_engine.fuzzer_engine import FuzzerEngine

    # Create a fuzzer instance
    fuzzer = FuzzerEngine()

    # Mock cluster instance with zero replicas
    mock_cluster_instance = Mock()
    # Create mock nodes with just the role attribute we need
    mock_node1 = Mock(role="primary")
    mock_node2 = Mock(role="primary")
    mock_node3 = Mock(role="primary")
    mock_cluster_instance.nodes = [mock_node1, mock_node2, mock_node3]

    # Mock cluster coordinator
    fuzzer.cluster_coordinator = Mock()
    fuzzer.cluster_coordinator.active_clusters = {"test-cluster": {"instance": mock_cluster_instance}}

    # Mock the ClusterConnection and validator
    with (
        patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.ClusterConnection") as mock_conn_class,
        patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.StateValidator") as mock_validator_class,
    ):
        mock_connection = Mock()
        mock_conn_class.return_value = mock_connection

        mock_validator = Mock()
        mock_validator_class.return_value = mock_validator

        # Mock validation result
        from valkey_fuzzer.models import StateValidationResult

        mock_result = StateValidationResult(
            overall_success=True,
            validation_timestamp=time.time(),
            validation_duration=1.0,
            replication=None,
            cluster_status=None,
            slot_coverage=None,
            topology=None,
            view_consistency=None,
            data_consistency=None,
            failed_checks=[],
            error_messages=[],
        )
        mock_validator.validate_state.return_value = mock_result

        # Call validate_cluster_state
        fuzzer.validate_cluster_state("test-cluster")

        # Verify StateValidator was created with correct config
        assert mock_validator_class.called
        config = mock_validator_class.call_args[0][0]

        # Verify min_replicas_per_shard was set to 0 for zero-replica cluster
        assert config.replication_config.min_replicas_per_shard == 0, (
            "min_replicas_per_shard should be 0 for zero-replica cluster"
        )

        # Verify data consistency check was disabled
        assert config.check_data_consistency is False, (
            "Data consistency check should be disabled for standalone validation"
        )

        # Verify validation was called with correct expected topology
        assert mock_validator.validate_state.called
        call_args = mock_validator.validate_state.call_args
        expected_topology = call_args[0][1]
        assert expected_topology.num_primaries == 3
        assert expected_topology.num_replicas == 0


def test_validate_cluster_state_with_replicas():
    """Test that validate_cluster_state correctly handles clusters with replicas"""
    from unittest.mock import Mock, patch

    from valkey_fuzzer.fuzzer_engine.fuzzer_engine import FuzzerEngine

    # Create a fuzzer instance
    fuzzer = FuzzerEngine()

    # Mock cluster instance with replicas (3 primaries, 3 replicas = 1 replica per shard)
    mock_cluster_instance = Mock()
    # Create mock nodes with just the role attribute we need
    mock_nodes = [
        Mock(role="primary"),
        Mock(role="primary"),
        Mock(role="primary"),
        Mock(role="replica"),
        Mock(role="replica"),
        Mock(role="replica"),
    ]
    mock_cluster_instance.nodes = mock_nodes

    # Mock cluster coordinator
    fuzzer.cluster_coordinator = Mock()
    fuzzer.cluster_coordinator.active_clusters = {"test-cluster": {"instance": mock_cluster_instance}}

    # Mock the ClusterConnection and validator
    with (
        patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.ClusterConnection") as mock_conn_class,
        patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.StateValidator") as mock_validator_class,
    ):
        mock_connection = Mock()
        mock_conn_class.return_value = mock_connection

        mock_validator = Mock()
        mock_validator_class.return_value = mock_validator

        # Mock validation result
        from valkey_fuzzer.models import StateValidationResult

        mock_result = StateValidationResult(
            overall_success=True,
            validation_timestamp=time.time(),
            validation_duration=1.0,
            replication=None,
            cluster_status=None,
            slot_coverage=None,
            topology=None,
            view_consistency=None,
            data_consistency=None,
            failed_checks=[],
            error_messages=[],
        )
        mock_validator.validate_state.return_value = mock_result

        # Call validate_cluster_state
        fuzzer.validate_cluster_state("test-cluster")

        # Verify StateValidator was created with correct config
        assert mock_validator_class.called
        config = mock_validator_class.call_args[0][0]

        # Verify min_replicas_per_shard was set to 1 (3 replicas / 3 primaries)
        assert config.replication_config.min_replicas_per_shard == 1, (
            "min_replicas_per_shard should be 1 for cluster with 1 replica per shard"
        )

        # Verify validation was called with correct expected topology
        assert mock_validator.validate_state.called
        call_args = mock_validator.validate_state.call_args
        expected_topology = call_args[0][1]
        assert expected_topology.num_primaries == 3
        assert expected_topology.num_replicas == 3


def test_replication_validator_ignores_expected_link_down_when_primary_killed():
    """Reachable replicas with link-down are expected if their primary was killed."""
    from valkey_fuzzer.fuzzer_engine.state_validator import ReplicationValidator
    from valkey_fuzzer.models import ReplicationValidationConfig

    validator = ReplicationValidator()
    cluster_connection = Mock()
    cluster_connection.get_current_nodes.return_value = [
        {
            "node_id": "primary-1",
            "host": "127.0.0.1",
            "port": 7000,
            "role": "primary",
            "shard_id": 0,
            "status": "failed",
        },
        {"node_id": "replica-1", "host": "127.0.0.1", "port": 7001, "role": "replica", "shard_id": 0, "status": "ok"},
    ]

    replica_client = Mock()
    replica_client.info.return_value = {
        "master_link_status": "down",
        "master_last_io_seconds_ago": "-1",
    }
    client_cm = Mock()
    client_cm.__enter__ = Mock(return_value=replica_client)
    client_cm.__exit__ = Mock(return_value=False)

    with (
        patch("valkey_fuzzer.fuzzer_engine.state_validator.valkey_client", return_value=client_cm),
        patch.object(ReplicationValidator, "_get_master_node_id", return_value="primary-1"),
        patch("valkey_fuzzer.fuzzer_engine.state_validator.safe_query_node", return_value={"connected_slaves": "0"}),
    ):
        result = validator.validate(
            cluster_connection,
            ReplicationValidationConfig(require_all_replicas_synced=False),
            killed_nodes={"127.0.0.1:7000"},
        )

    assert result.success is True
    assert result.error_message is None
    assert result.all_replicas_synced is False
    assert result.disconnected_replicas == ["127.0.0.1:7001"]


def test_replication_validator_stops_suppressing_once_new_primary_is_live():
    """Historical primary kills should not mask later replica link-down regressions."""
    from valkey_fuzzer.fuzzer_engine.state_validator import ReplicationValidator
    from valkey_fuzzer.models import ReplicationValidationConfig

    validator = ReplicationValidator()
    cluster_connection = Mock()
    cluster_connection.get_current_nodes.return_value = [
        {
            "node_id": "old-primary",
            "host": "127.0.0.1",
            "port": 7000,
            "role": "primary",
            "shard_id": 0,
            "status": "failed",
        },
        {"node_id": "new-primary", "host": "127.0.0.1", "port": 7002, "role": "primary", "shard_id": 0, "status": "ok"},
        {"node_id": "replica-1", "host": "127.0.0.1", "port": 7001, "role": "replica", "shard_id": 0, "status": "ok"},
    ]

    replica_client = Mock()
    replica_client.info.return_value = {
        "master_link_status": "down",
        "master_last_io_seconds_ago": "-1",
    }
    client_cm = Mock()
    client_cm.__enter__ = Mock(return_value=replica_client)
    client_cm.__exit__ = Mock(return_value=False)

    with (
        patch("valkey_fuzzer.fuzzer_engine.state_validator.valkey_client", return_value=client_cm),
        patch.object(ReplicationValidator, "_get_master_node_id", return_value="new-primary"),
        patch("valkey_fuzzer.fuzzer_engine.state_validator.safe_query_node", return_value={"connected_slaves": "0"}),
    ):
        result = validator.validate(
            cluster_connection,
            ReplicationValidationConfig(require_all_replicas_synced=False),
            killed_nodes={"127.0.0.1:7000"},
        )

    assert result.success is False
    assert "Unexpected replica disconnections detected" in result.error_message


def test_replication_validator_respects_offset_guard_for_lag_detection():
    """Lag alone should not fail when offset checks are enabled and diff is zero."""
    from valkey_fuzzer.fuzzer_engine.state_validator import ReplicationValidator
    from valkey_fuzzer.models import ReplicationValidationConfig

    validator = ReplicationValidator()
    cluster_connection = Mock()
    cluster_connection.get_current_nodes.return_value = [
        {"node_id": "primary-1", "host": "127.0.0.1", "port": 7000, "role": "primary", "shard_id": 0, "status": "ok"},
        {"node_id": "replica-1", "host": "127.0.0.1", "port": 7001, "role": "replica", "shard_id": 0, "status": "ok"},
    ]

    replica_client = Mock()
    replica_client.info.return_value = {
        "master_link_status": "up",
        "master_last_io_seconds_ago": "10",
    }
    client_cm = Mock()
    client_cm.__enter__ = Mock(return_value=replica_client)
    client_cm.__exit__ = Mock(return_value=False)

    with (
        patch("valkey_fuzzer.fuzzer_engine.state_validator.valkey_client", return_value=client_cm),
        patch.object(ReplicationValidator, "_get_master_node_id", return_value="primary-1"),
        patch.object(ReplicationValidator, "_get_offset_difference", return_value=0),
        patch("valkey_fuzzer.fuzzer_engine.state_validator.safe_query_node", return_value={"connected_slaves": "1"}),
    ):
        result = validator.validate(
            cluster_connection,
            ReplicationValidationConfig(
                max_acceptable_lag=5.0,
                check_replication_offset=True,
                require_all_replicas_synced=False,
            ),
        )

    assert result.success is True
    assert result.error_message is None
    assert result.lagging_replicas == []
    assert result.max_lag == 10.0
