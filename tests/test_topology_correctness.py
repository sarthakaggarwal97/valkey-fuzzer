import random
from typing import Optional
from unittest.mock import Mock, patch

import pytest

from valkey_fuzzer.cluster_orchestrator.orchestrator import ConfigurationManager, PortManager
from valkey_fuzzer.fuzzer_engine.state_validator import DataConsistencyValidator, ReplicationValidator
from valkey_fuzzer.fuzzer_engine.state_validator_helpers import compute_cluster_slot
from valkey_fuzzer.models import (
    ClusterConfig,
    DataConsistencyValidationConfig,
    NodeInfo,
    ReplicationValidationConfig,
    TopologyType,
)


def _node(node_id: str, role: str, port: int, master_node_id: Optional[str] = None) -> NodeInfo:
    process = Mock(pid=port)
    process.poll.return_value = None
    return NodeInfo(
        node_id=node_id,
        role=role,
        shard_id=0,
        port=port,
        bus_port=port + 10000,
        pid=port,
        process=process,
        data_dir=f"/tmp/{node_id}",
        log_file=f"/tmp/{node_id}.log",
        master_node_id=master_node_id,
    )


def _client_context(client: Mock) -> Mock:
    context = Mock()
    context.__enter__ = Mock(return_value=client)
    context.__exit__ = Mock(return_value=False)
    return context


def _replication_connection(primary: NodeInfo, replica: NodeInfo) -> Mock:
    connection = Mock(
        topology_type=TopologyType.REPLICATION,
        initial_nodes=[primary, replica],
    )
    connection.get_current_nodes.return_value = [
        {
            "node_id": primary.node_id,
            "host": primary.host,
            "port": primary.port,
            "role": "primary",
            "shard_id": 0,
            "status": "connected",
        },
        {
            "node_id": replica.node_id,
            "host": replica.host,
            "port": replica.port,
            "role": "replica",
            "shard_id": 0,
            "status": "connected",
        },
    ]
    return connection


def test_replication_validator_rejects_replica_attached_to_wrong_primary():
    primary = _node("node-0", "primary", 7000)
    replica = _node("node-1", "replica", 7001, master_node_id=primary.node_id)
    connection = _replication_connection(primary, replica)
    replica_client = Mock()
    replica_client.info.return_value = {
        "master_link_status": "up",
        "master_last_io_seconds_ago": "0",
        "master_host": primary.host,
        "master_port": 7999,
    }

    with (
        patch(
            "valkey_fuzzer.fuzzer_engine.state_validator.valkey_client",
            return_value=_client_context(replica_client),
        ),
        patch("valkey_fuzzer.fuzzer_engine.state_validator.query_cluster_nodes") as cluster_nodes,
        patch(
            "valkey_fuzzer.fuzzer_engine.state_validator.safe_query_node",
            return_value={"connected_slaves": "0"},
        ),
    ):
        result = ReplicationValidator().validate(
            connection,
            ReplicationValidationConfig(check_replication_offset=False),
        )

    assert result.success is False
    assert "attached to 127.0.0.1:7999" in result.error_message
    assert "expected 127.0.0.1:7000" in result.error_message
    cluster_nodes.assert_not_called()


def test_replication_validator_checks_offset_against_planned_primary_independent_of_io_age():
    primary = _node("node-0", "primary", 7000)
    replica = _node("node-1", "replica", 7001, master_node_id=primary.node_id)
    connection = _replication_connection(primary, replica)
    replica_client = Mock()
    replica_client.info.return_value = {
        "master_link_status": "up",
        "master_last_io_seconds_ago": "0",
        "master_host": primary.host,
        "master_port": str(primary.port),
    }
    expected_primary = connection.get_current_nodes.return_value[0]

    with (
        patch(
            "valkey_fuzzer.fuzzer_engine.state_validator.valkey_client",
            return_value=_client_context(replica_client),
        ),
        patch.object(
            ReplicationValidator,
            "_get_offset_difference",
            return_value=7,
        ) as offset_difference,
        patch(
            "valkey_fuzzer.fuzzer_engine.state_validator.safe_query_node",
            return_value={"connected_slaves": "1"},
        ),
        patch("valkey_fuzzer.fuzzer_engine.state_validator.query_cluster_nodes") as cluster_nodes,
    ):
        result = ReplicationValidator().validate(
            connection,
            ReplicationValidationConfig(
                max_acceptable_lag=5,
                check_replication_offset=True,
                require_all_replicas_synced=False,
            ),
        )

    assert result.success is False
    assert result.lagging_replicas[0].replication_offset_diff == 7
    offset_primary = offset_difference.call_args.args[1]
    assert offset_primary["node_id"] == expected_primary["node_id"]
    assert offset_primary["port"] == expected_primary["port"]
    cluster_nodes.assert_not_called()


def test_replication_validator_fails_when_offset_cannot_be_measured():
    primary = _node("node-0", "primary", 7000)
    replica = _node("node-1", "replica", 7001, master_node_id=primary.node_id)
    connection = _replication_connection(primary, replica)
    replica_client = Mock()
    replica_client.info.return_value = {
        "master_link_status": "up",
        "master_last_io_seconds_ago": "0",
        "master_host": primary.host,
        "master_port": primary.port,
    }

    with (
        patch(
            "valkey_fuzzer.fuzzer_engine.state_validator.valkey_client",
            side_effect=[
                _client_context(replica_client),
                RuntimeError("offset query failed"),
            ],
        ),
        patch(
            "valkey_fuzzer.fuzzer_engine.state_validator.safe_query_node",
            return_value={"connected_slaves": "1"},
        ),
    ):
        result = ReplicationValidator().validate(
            connection,
            ReplicationValidationConfig(check_replication_offset=True),
        )

    assert result.success is False
    assert "Unable to compare replication offsets" in result.error_message


def test_cluster_replica_count_mismatch_remains_advisory_during_failover():
    primary = _node("node-0", "primary", 7000)
    replica = _node("node-1", "replica", 7001, master_node_id=primary.node_id)
    connection = _replication_connection(primary, replica)
    connection.topology_type = TopologyType.CLUSTER
    replica_client = Mock()
    replica_client.info.return_value = {
        "master_link_status": "up",
        "master_last_io_seconds_ago": "0",
    }

    with (
        patch(
            "valkey_fuzzer.fuzzer_engine.state_validator.valkey_client",
            return_value=_client_context(replica_client),
        ),
        patch.object(ReplicationValidator, "_get_master_node_id", return_value=primary.node_id),
        patch.object(ReplicationValidator, "_get_offset_difference", return_value=0),
        patch(
            "valkey_fuzzer.fuzzer_engine.state_validator.safe_query_node",
            return_value={"connected_slaves": "0"},
        ),
    ):
        result = ReplicationValidator().validate(
            connection,
            ReplicationValidationConfig(check_replication_offset=True),
        )

    assert result.success is True
    assert result.error_message is None


def test_replication_validator_fails_primary_replica_count_mismatch():
    primary = _node("node-0", "primary", 7000)
    replica = _node("node-1", "replica", 7001, master_node_id=primary.node_id)
    connection = _replication_connection(primary, replica)
    replica_client = Mock()
    replica_client.info.return_value = {
        "master_link_status": "up",
        "master_last_io_seconds_ago": "0",
        "master_host": primary.host,
        "master_port": primary.port,
    }

    with (
        patch(
            "valkey_fuzzer.fuzzer_engine.state_validator.valkey_client",
            return_value=_client_context(replica_client),
        ),
        patch(
            "valkey_fuzzer.fuzzer_engine.state_validator.safe_query_node",
            return_value={"connected_slaves": "0"},
        ),
    ):
        result = ReplicationValidator().validate(
            connection,
            ReplicationValidationConfig(check_replication_offset=False),
        )

    assert result.success is False
    assert "reports 0 connected replicas, expected 1" in result.error_message


def test_replication_validator_rejects_disconnected_planned_primary_without_offset_check():
    primary = _node("node-0", "primary", 7000)
    replica = _node("node-1", "replica", 7001, master_node_id=primary.node_id)
    connection = _replication_connection(primary, replica)
    connection.get_current_nodes.return_value[0]["status"] = "failed"
    replica_client = Mock()
    replica_client.info.return_value = {
        "master_link_status": "up",
        "master_last_io_seconds_ago": "0",
        "master_host": primary.host,
        "master_port": primary.port,
    }

    with (
        patch(
            "valkey_fuzzer.fuzzer_engine.state_validator.valkey_client",
            return_value=_client_context(replica_client),
        ),
        patch("valkey_fuzzer.fuzzer_engine.state_validator.safe_query_node", return_value=None),
    ):
        result = ReplicationValidator().validate(
            connection,
            ReplicationValidationConfig(check_replication_offset=False),
        )

    assert result.success is False
    assert "Planned primary 127.0.0.1:7000 is not connected" in result.error_message


def test_replication_validator_rejects_unqueryable_planned_primary():
    primary = _node("node-0", "primary", 7000)
    replica = _node("node-1", "replica", 7001, master_node_id=primary.node_id)
    connection = _replication_connection(primary, replica)
    replica_client = Mock()
    replica_client.info.return_value = {
        "master_link_status": "up",
        "master_last_io_seconds_ago": "0",
        "master_host": primary.host,
        "master_port": primary.port,
    }

    with (
        patch(
            "valkey_fuzzer.fuzzer_engine.state_validator.valkey_client",
            return_value=_client_context(replica_client),
        ),
        patch("valkey_fuzzer.fuzzer_engine.state_validator.safe_query_node", return_value=None),
    ):
        result = ReplicationValidator().validate(
            connection,
            ReplicationValidationConfig(check_replication_offset=False),
        )

    assert result.success is False
    assert "Unable to query replication INFO from primary 127.0.0.1:7000" in result.error_message


def test_replication_replica_restart_reapplies_upstream(tmp_path):
    config = ClusterConfig(
        num_shards=1,
        replicas_per_shard=1,
        topology_type=TopologyType.REPLICATION,
        base_data_dir=str(tmp_path),
        valkey_binary="/opt/valkey-server",
    )
    manager = ConfigurationManager(config, PortManager(base_port=7000))
    plans = manager.plan_topology()
    processes = []
    for pid in (101, 102, 103):
        process = Mock(pid=pid)
        process.poll.return_value = None
        processes.append(process)

    ping_client = Mock()
    ping_client.ping.return_value = True

    with (
        patch(
            "valkey_fuzzer.cluster_orchestrator.orchestrator.subprocess.Popen",
            side_effect=processes,
        ) as popen,
        patch(
            "valkey_fuzzer.cluster_orchestrator.orchestrator.valkey_client",
            return_value=_client_context(ping_client),
        ),
    ):
        nodes = manager.spawn_all_nodes(plans)
        replica = next(node for node in nodes if node.role == "replica")
        manager.restart_node(replica, wait_ready=False)

    restart_command = popen.call_args_list[-1].args[0]
    replicaof_index = restart_command.index("--replicaof")
    assert restart_command[replicaof_index : replicaof_index + 3] == [
        "--replicaof",
        "127.0.0.1",
        "7000",
    ]
    assert replica.pid == 103


def test_spawn_failure_releases_ports_processes_and_data(tmp_path):
    config = ClusterConfig(
        num_shards=1,
        replicas_per_shard=1,
        topology_type=TopologyType.REPLICATION,
        base_data_dir=str(tmp_path),
        valkey_binary="/opt/valkey-server",
    )
    port_manager = PortManager(base_port=7000)
    manager = ConfigurationManager(config, port_manager)
    plans = manager.plan_topology()
    first_process = Mock(pid=101)
    first_process.poll.return_value = None

    with (
        patch(
            "valkey_fuzzer.cluster_orchestrator.orchestrator.subprocess.Popen",
            side_effect=[first_process, OSError("spawn failed")],
        ),
        pytest.raises(OSError, match="spawn failed"),
    ):
        manager.spawn_all_nodes(plans)

    first_process.terminate.assert_called_once()
    first_process.wait.assert_called_once_with(timeout=5)
    assert port_manager.allocated_ports == {}
    assert manager._spawned_nodes == {}
    assert not (tmp_path / f"cluster-{manager.cluster_id}").exists()


def test_write_test_keys_requires_every_requested_write():
    validator = DataConsistencyValidator()
    connection = Mock(topology_type=TopologyType.STANDALONE)
    connection.get_live_nodes.return_value = [
        {
            "node_id": "node-0",
            "host": "127.0.0.1",
            "port": 7000,
            "role": "primary",
            "shard_id": 0,
            "status": "connected",
        }
    ]
    primary_client = Mock()
    primary_client.set.side_effect = [True, RuntimeError("SET failed"), True]

    with patch(
        "valkey_fuzzer.fuzzer_engine.state_validator.valkey_client",
        return_value=_client_context(primary_client),
    ):
        result = validator.write_test_keys(
            connection,
            DataConsistencyValidationConfig(num_test_keys=3),
        )

    assert result is False
    assert len(validator.test_keys) == 2


def test_test_values_are_reproducible_with_an_isolated_rng():
    connection = Mock(topology_type=TopologyType.STANDALONE)
    connection.get_live_nodes.return_value = [
        {
            "node_id": "node-0",
            "host": "127.0.0.1",
            "port": 7000,
            "role": "primary",
            "shard_id": 0,
            "status": "connected",
        }
    ]
    first_client = Mock()
    first_client.set.return_value = True
    second_client = Mock()
    second_client.set.return_value = True
    config = DataConsistencyValidationConfig(num_test_keys=3)

    with patch(
        "valkey_fuzzer.fuzzer_engine.state_validator.valkey_client",
        side_effect=[_client_context(first_client), _client_context(second_client)],
    ):
        first = DataConsistencyValidator(rng=random.Random(99))
        second = DataConsistencyValidator(rng=random.Random(99))
        assert first.write_test_keys(connection, config) is True
        assert second.write_test_keys(connection, config) is True

    assert first.test_keys == second.test_keys


def test_cluster_test_key_seeding_covers_every_authoritative_primary():
    validator = DataConsistencyValidator(rng=random.Random(7))
    primaries = [
        {
            "node_id": "primary-0",
            "host": "127.0.0.1",
            "port": 7000,
            "role": "primary",
            "shard_id": 0,
            "status": "connected",
        },
        {
            "node_id": "primary-1",
            "host": "127.0.0.1",
            "port": 7002,
            "role": "primary",
            "shard_id": 1,
            "status": "connected",
        },
    ]
    connection = Mock(topology_type=TopologyType.CLUSTER)
    connection.get_live_nodes.return_value = primaries
    key_zero = "fuzzer:test:0"
    key_one = "fuzzer:test:1"
    slot_map = {
        compute_cluster_slot(key_zero): "127.0.0.1:7000",
        compute_cluster_slot(key_one): "127.0.0.1:7002",
    }
    cluster_client = Mock()
    cluster_client.set.return_value = True

    with (
        patch(
            "valkey_fuzzer.fuzzer_engine.state_validator.fetch_cluster_slot_assignments",
            return_value=slot_map,
        ),
        patch(
            "valkey_fuzzer.fuzzer_engine.state_validator.valkey.ValkeyCluster",
            return_value=cluster_client,
        ),
    ):
        assert validator.write_test_keys(
            connection,
            DataConsistencyValidationConfig(num_test_keys=2),
        )

    owners = {slot_map[compute_cluster_slot(key)] for key in validator.test_keys}
    assert owners == {"127.0.0.1:7000", "127.0.0.1:7002"}


def test_cluster_test_key_seeding_rejects_too_few_keys_for_live_primaries(caplog):
    validator = DataConsistencyValidator()
    connection = Mock(topology_type=TopologyType.CLUSTER)
    connection.get_live_nodes.return_value = [
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

    with patch("valkey_fuzzer.fuzzer_engine.state_validator.valkey.ValkeyCluster") as cluster_client:
        assert (
            validator.write_test_keys(
                connection,
                DataConsistencyValidationConfig(num_test_keys=1),
            )
            is False
        )

    assert "at least one key per primary is required" in caplog.text
    cluster_client.assert_not_called()


def test_cross_replica_get_error_is_recorded_and_fails_validation():
    validator = DataConsistencyValidator()
    validator.test_keys = {"fuzzer:test:0": "expected"}
    connection = Mock(topology_type=TopologyType.REPLICATION)
    connection.get_live_nodes.return_value = [
        {
            "node_id": "node-0",
            "host": "127.0.0.1",
            "port": 7000,
            "role": "primary",
            "shard_id": 0,
            "status": "connected",
        },
        {
            "node_id": "node-1",
            "host": "127.0.0.1",
            "port": 7001,
            "role": "replica",
            "shard_id": 0,
            "status": "connected",
        },
    ]
    primary_client = Mock()
    primary_client.get.return_value = "expected"

    with patch(
        "valkey_fuzzer.fuzzer_engine.state_validator.valkey_client",
        side_effect=[
            _client_context(primary_client),
            RuntimeError("replica read failed"),
        ],
    ):
        result = validator.validate(connection, DataConsistencyValidationConfig())

    assert result.success is False
    assert result.unreachable_keys == ["fuzzer:test:0"]
    assert result.inconsistent_keys[0].inconsistency_type == "replica_unreachable"
    assert "replica read failed" in next(iter(result.inconsistent_keys[0].actual_values.values()))


def test_cross_replica_sample_is_stable_across_retries():
    validator = DataConsistencyValidator()
    validator.test_keys = {f"key-{index:02d}": f"value-{index:02d}" for index in reversed(range(25))}
    replica = {
        "node_id": "node-1",
        "host": "127.0.0.1",
        "port": 7001,
        "role": "replica",
        "shard_id": 0,
        "status": "connected",
    }
    connection = Mock(topology_type=TopologyType.REPLICATION)
    connection.get_live_nodes.return_value = [replica]
    observed_keys = []
    replica_client = Mock()
    replica_client.get.side_effect = lambda key: observed_keys.append(key) or validator.test_keys[key]

    with (
        patch(
            "valkey_fuzzer.fuzzer_engine.state_validator.valkey_client",
            return_value=_client_context(replica_client),
        ),
        patch("valkey_fuzzer.fuzzer_engine.state_validator.random.sample") as random_sample,
    ):
        validator._check_cross_replica_consistency(
            connection,
            DataConsistencyValidationConfig(),
            [],
            [],
        )
        validator._check_cross_replica_consistency(
            connection,
            DataConsistencyValidationConfig(),
            [],
            [],
        )

    expected_sample = sorted(validator.test_keys)[:20]
    assert observed_keys[:20] == expected_sample
    assert observed_keys[20:] == expected_sample
    random_sample.assert_not_called()


def test_cluster_cross_replica_reads_only_owned_keys_in_readonly_mode():
    validator = DataConsistencyValidator()
    validator.test_keys = {
        "owned-key": "owned-value",
        "other-key": "other-value",
    }
    primary = {
        "node_id": "primary-0",
        "host": "127.0.0.1",
        "port": 7000,
        "role": "primary",
        "shard_id": 0,
        "status": "connected",
    }
    replica = {
        "node_id": "replica-0",
        "host": "127.0.0.1",
        "port": 7001,
        "role": "replica",
        "shard_id": 0,
        "status": "connected",
    }
    connection = Mock(topology_type=TopologyType.CLUSTER)
    connection.get_live_nodes.return_value = [primary, replica]
    replica_client = Mock()
    replica_client.get.return_value = "owned-value"
    slot_map = {
        compute_cluster_slot("owned-key"): "127.0.0.1:7000",
        compute_cluster_slot("other-key"): "127.0.0.1:7999",
    }
    inconsistent_keys = []
    unreachable_keys = []

    with (
        patch(
            "valkey_fuzzer.fuzzer_engine.state_validator.fetch_cluster_slot_assignments",
            return_value=slot_map,
        ),
        patch(
            "valkey_fuzzer.fuzzer_engine.state_validator.valkey_client",
            return_value=_client_context(replica_client),
        ),
    ):
        validator._check_cross_replica_consistency(
            connection,
            DataConsistencyValidationConfig(),
            inconsistent_keys,
            unreachable_keys,
        )

    replica_client.execute_command.assert_called_once_with("READONLY")
    replica_client.get.assert_called_once_with("owned-key")
    assert inconsistent_keys == []
    assert unreachable_keys == []


def test_cluster_cross_replica_validation_fails_when_shard_has_no_seeded_key():
    validator = DataConsistencyValidator()
    validator.test_keys = {"other-shard-key": "value"}
    primary = {
        "node_id": "primary-0",
        "host": "127.0.0.1",
        "port": 7000,
        "role": "primary",
        "shard_id": 0,
        "status": "connected",
    }
    replica = {
        "node_id": "replica-0",
        "host": "127.0.0.1",
        "port": 7001,
        "role": "replica",
        "shard_id": 0,
        "status": "connected",
    }
    connection = Mock(topology_type=TopologyType.CLUSTER)
    connection.get_live_nodes.return_value = [primary, replica]
    slot_map = {
        compute_cluster_slot("other-shard-key"): "127.0.0.1:7999",
        compute_cluster_slot("owned-but-unseeded"): "127.0.0.1:7000",
    }

    with (
        patch(
            "valkey_fuzzer.fuzzer_engine.state_validator.fetch_cluster_slot_assignments",
            return_value=slot_map,
        ),
        pytest.raises(RuntimeError, match="cross-replica coverage is incomplete"),
    ):
        validator._check_cross_replica_consistency(
            connection,
            DataConsistencyValidationConfig(),
            [],
            [],
        )
