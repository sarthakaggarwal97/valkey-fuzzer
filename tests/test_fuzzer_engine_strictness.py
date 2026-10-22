"""
Unit tests for strict run success criteria and wave validation behavior.
"""

import time
from unittest.mock import Mock, patch

import pytest

from valkey_fuzzer.fuzzer_engine.fuzzer_engine import FuzzerEngine
from valkey_fuzzer.fuzzer_engine.parallel_executor import ParallelExecutor
from valkey_fuzzer.fuzzer_engine.state_validator import StateValidator
from valkey_fuzzer.models import (
    ChaosConfig,
    ChaosCoordination,
    ChaosResult,
    ChaosTiming,
    ChaosType,
    ClusterConfig,
    ClusterStatusValidation,
    ExpectedTopology,
    Operation,
    OperationTiming,
    OperationType,
    ProcessChaosType,
    Scenario,
    StateValidationConfig,
    StateValidationResult,
    TargetSelection,
    TopologyType,
)


def build_scenario() -> Scenario:
    return Scenario(
        scenario_id="strict-test",
        cluster_config=ClusterConfig(num_shards=3, replicas_per_shard=1),
        operations=[
            Operation(
                type=OperationType.FAILOVER,
                target_node="shard-0-primary",
                parameters={"force": False},
                timing=OperationTiming(),
            ),
            Operation(
                type=OperationType.FAILOVER,
                target_node="shard-1-primary",
                parameters={"force": False},
                timing=OperationTiming(),
            ),
        ],
        chaos_config=ChaosConfig(
            chaos_type=ChaosType.PROCESS_KILL,
            target_selection=TargetSelection(strategy="random"),
            timing=ChaosTiming(),
            coordination=ChaosCoordination(),
            process_chaos_type=ProcessChaosType.SIGKILL,
        ),
    )


def build_validation_result(
    overall_success: bool,
    failed_checks=None,
    cluster_state: str = "ok",
) -> StateValidationResult:
    failed_checks = failed_checks or []
    error_messages = [f"{check} failed" for check in failed_checks]

    cluster_status = ClusterStatusValidation(
        success=overall_success,
        cluster_state=cluster_state,
        nodes_in_fail_state=[],
        has_quorum=True,
        degraded_reason=None,
        error_message=", ".join(error_messages) if error_messages else None,
    )

    return StateValidationResult(
        overall_success=overall_success,
        validation_timestamp=time.time(),
        validation_duration=0.5,
        replication=None,
        cluster_status=cluster_status,
        slot_coverage=None,
        topology=None,
        view_consistency=None,
        data_consistency=None,
        failed_checks=failed_checks,
        error_messages=error_messages,
    )


def build_cluster_instance():
    node = Mock()
    node.node_id = "node-0"
    node.host = "127.0.0.1"
    node.port = 7000
    return Mock(cluster_id="cluster-1", nodes=[node])


@patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.load_all_slots")
@patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.StateValidator")
@patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.ClusterConnection")
@patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.ParallelExecutor")
def test_execute_test_fails_when_not_all_operations_succeed(
    mock_executor_class,
    mock_connection_class,
    mock_validator_class,
    mock_load_all_slots,
):
    engine = FuzzerEngine()
    scenario = build_scenario()
    scenario.seed = 123
    cluster_instance = build_cluster_instance()

    engine._create_cluster_with_retry = Mock(return_value=cluster_instance)
    engine._validate_cluster_readiness_with_retry = Mock(return_value=True)
    engine._cleanup_resources = Mock()

    mock_connection = Mock()
    mock_connection_class.return_value = mock_connection

    mock_validator = Mock()
    mock_validator.write_test_data.return_value = True
    mock_validator.validate_with_retry.return_value = build_validation_result(True)
    mock_validator_class.return_value = mock_validator

    mock_executor = Mock()
    mock_executor.execute_operations_parallel.return_value = (
        1,
        [],
        [build_validation_result(True)],
    )
    mock_executor_class.return_value = mock_executor

    result = engine.execute_test(scenario)

    assert result.success is False
    assert result.error_message == "1 operation(s) failed"
    preload_rng = mock_load_all_slots.call_args.kwargs["rng"]
    validation_rng = mock_validator_class.call_args.kwargs["rng"]
    assert preload_rng.getstate() == engine._scenario_rng(123, "slot-preload").getstate()
    assert validation_rng.getstate() == engine._scenario_rng(123, "data-validation").getstate()
    assert preload_rng.getstate() != validation_rng.getstate()


@patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.load_all_slots")
@patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.StateValidator")
@patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.ClusterConnection")
@patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.ParallelExecutor")
def test_execute_test_fails_when_post_operation_validation_fails(
    mock_executor_class,
    mock_connection_class,
    mock_validator_class,
    mock_load_all_slots,
):
    engine = FuzzerEngine()
    scenario = build_scenario()
    cluster_instance = build_cluster_instance()

    engine._create_cluster_with_retry = Mock(return_value=cluster_instance)
    engine._validate_cluster_readiness_with_retry = Mock(return_value=True)
    engine._cleanup_resources = Mock()

    mock_connection_class.return_value = Mock()

    mock_validator = Mock()
    mock_validator.write_test_data.return_value = True
    mock_validator.validate_with_retry.return_value = build_validation_result(True)
    mock_validator_class.return_value = mock_validator

    mock_executor = Mock()
    mock_executor.execute_operations_parallel.return_value = (
        len(scenario.operations),
        [],
        [build_validation_result(False, failed_checks=["replication"])],
    )
    mock_executor_class.return_value = mock_executor

    result = engine.execute_test(scenario)

    assert result.success is False
    assert "post-operation validation wave(s) failed" in result.error_message


@patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.load_all_slots")
@patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.StateValidator")
@patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.ClusterConnection")
@patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.ParallelExecutor")
def test_execute_test_fails_when_chaos_injection_fails(
    mock_executor_class,
    mock_connection_class,
    mock_validator_class,
    mock_load_all_slots,
):
    engine = FuzzerEngine()
    scenario = build_scenario()
    cluster_instance = build_cluster_instance()

    engine._create_cluster_with_retry = Mock(return_value=cluster_instance)
    engine._validate_cluster_readiness_with_retry = Mock(return_value=True)
    engine._cleanup_resources = Mock()

    mock_connection_class.return_value = Mock()

    mock_validator = Mock()
    mock_validator.write_test_data.return_value = True
    mock_validator.validate_with_retry.return_value = build_validation_result(True)
    mock_validator_class.return_value = mock_validator

    failed_chaos = ChaosResult(
        chaos_id="chaos-1",
        chaos_type=ChaosType.PROCESS_KILL,
        target_node="node-0",
        success=False,
        start_time=time.time(),
        end_time=time.time(),
        error_message="kill failed",
    )

    mock_executor = Mock()
    mock_executor.execute_operations_parallel.return_value = (
        len(scenario.operations),
        [failed_chaos],
        [build_validation_result(True)],
    )
    mock_executor_class.return_value = mock_executor

    result = engine.execute_test(scenario)

    assert result.success is False
    assert result.error_message == "1 chaos injection(s) failed"


@patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.load_all_slots")
@patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.StateValidator")
@patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.ClusterConnection")
@patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.ParallelExecutor")
def test_execute_test_fails_when_data_consistency_setup_fails(
    mock_executor_class,
    mock_connection_class,
    mock_validator_class,
    mock_load_all_slots,
):
    engine = FuzzerEngine()
    scenario = build_scenario()
    cluster_instance = build_cluster_instance()

    engine._create_cluster_with_retry = Mock(return_value=cluster_instance)
    engine._validate_cluster_readiness_with_retry = Mock(return_value=True)
    engine._cleanup_resources = Mock()

    mock_connection_class.return_value = Mock()

    mock_validator = Mock()
    mock_validator.write_test_data.return_value = False
    mock_validator_class.return_value = mock_validator

    result = engine.execute_test(scenario)

    assert result.success is False
    assert "Failed to write test data required for data consistency validation" in result.error_message
    mock_executor_class.assert_not_called()


@patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.load_all_slots")
@patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.StateValidator")
@patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.ClusterConnection")
@patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.ParallelExecutor")
def test_execute_test_keeps_default_cluster_state_validation_strict(
    mock_executor_class,
    mock_connection_class,
    mock_validator_class,
    mock_load_all_slots,
):
    engine = FuzzerEngine()
    scenario = build_scenario()
    cluster_instance = build_cluster_instance()

    engine._create_cluster_with_retry = Mock(return_value=cluster_instance)
    engine._validate_cluster_readiness_with_retry = Mock(return_value=True)
    engine._cleanup_resources = Mock()

    mock_connection_class.return_value = Mock()

    mock_validator = Mock()
    mock_validator.write_test_data.return_value = True
    mock_validator.validate_with_retry.return_value = build_validation_result(True)
    mock_validator_class.return_value = mock_validator

    mock_executor = Mock()
    mock_executor.execute_operations_parallel.return_value = (
        len(scenario.operations),
        [],
        [build_validation_result(True)],
    )
    mock_executor_class.return_value = mock_executor

    engine.execute_test(scenario)

    config = mock_validator_class.call_args[0][0]
    assert config.cluster_status_config.acceptable_states == ["ok"]


@pytest.mark.parametrize(
    ("topology_type", "num_shards", "replicas_per_shard", "expected_primaries", "expected_replicas"),
    [
        (TopologyType.CLUSTER, 3, 2, 3, 6),
        (TopologyType.REPLICATION, 4, 2, 1, 2),
        (TopologyType.STANDALONE, 4, 3, 1, 0),
    ],
)
def test_expected_topology_counts_follow_deployment_mode(
    topology_type,
    num_shards,
    replicas_per_shard,
    expected_primaries,
    expected_replicas,
):
    engine = FuzzerEngine()
    scenario = build_scenario()
    scenario.cluster_config = ClusterConfig(
        num_shards=num_shards,
        replicas_per_shard=replicas_per_shard,
        topology_type=topology_type,
    )

    topology = engine._build_expected_topology(scenario)

    assert topology.num_primaries == expected_primaries
    assert topology.num_replicas == expected_replicas


@pytest.mark.parametrize(
    ("topology_type", "expected_replication"),
    [
        (TopologyType.REPLICATION, True),
        (TopologyType.STANDALONE, False),
    ],
)
def test_non_cluster_validation_defaults_only_keep_meaningful_checks(
    topology_type,
    expected_replication,
):
    engine = FuzzerEngine()
    config = StateValidationConfig()

    engine._adapt_validation_config_for_topology(config, topology_type)

    assert config.check_replication is expected_replication
    assert config.check_data_consistency is True
    assert config.check_cluster_status is False
    assert config.check_slot_coverage is False
    assert config.check_topology is True
    assert config.check_view_consistency is False
    assert config.check_logs is False


def test_standalone_liveness_check_is_forced_on_when_all_optional_checks_are_disabled():
    engine = FuzzerEngine()
    config = StateValidationConfig(
        check_replication=False,
        check_cluster_status=False,
        check_slot_coverage=False,
        check_topology=False,
        check_view_consistency=False,
        check_data_consistency=False,
        check_logs=False,
        stabilization_wait=0,
    )
    engine._adapt_validation_config_for_topology(config, TopologyType.STANDALONE)
    connection = Mock()
    connection.get_current_nodes.return_value = [
        {
            "node_id": "node-0",
            "host": "127.0.0.1",
            "port": 7000,
            "role": "primary",
            "shard_id": 0,
            "status": "connected",
        }
    ]

    result = StateValidator(config).validate_state(
        connection,
        ExpectedTopology(num_primaries=1, num_replicas=0, shard_structure={}),
    )

    assert result.overall_success is True
    assert result.topology is not None
    connection.get_current_nodes.assert_called_once_with(include_failed=True)


def test_scenario_rng_streams_are_reproducible_and_independent():
    first_slot_rng = FuzzerEngine._scenario_rng(123, "slot-preload")
    second_slot_rng = FuzzerEngine._scenario_rng(123, "slot-preload")
    validation_rng = FuzzerEngine._scenario_rng(123, "data-validation")

    first_slot_values = [first_slot_rng.random() for _ in range(5)]
    second_slot_values = [second_slot_rng.random() for _ in range(5)]
    validation_values = [validation_rng.random() for _ in range(5)]

    assert first_slot_values == second_slot_values
    assert first_slot_values != validation_values


@patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.load_all_slots")
@patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.StateValidator")
@patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.ClusterConnection")
@patch("valkey_fuzzer.fuzzer_engine.fuzzer_engine.ParallelExecutor")
def test_zero_operation_standalone_skips_slot_preload_and_runs_final_validation(
    mock_executor_class,
    mock_connection_class,
    mock_validator_class,
    mock_load_all_slots,
):
    engine = FuzzerEngine()
    scenario = build_scenario()
    scenario.operations = []
    scenario.cluster_config = ClusterConfig(
        num_shards=1,
        replicas_per_shard=0,
        topology_type=TopologyType.STANDALONE,
    )
    cluster_instance = build_cluster_instance()

    engine._create_cluster_with_retry = Mock(return_value=cluster_instance)
    engine._validate_cluster_readiness_with_retry = Mock(return_value=True)
    engine._cleanup_resources = Mock()

    mock_connection = Mock()
    mock_connection_class.return_value = mock_connection

    mock_validator = Mock()
    mock_validator.write_test_data.return_value = True
    mock_validator.validate_with_retry.return_value = build_validation_result(True)
    mock_validator_class.return_value = mock_validator

    result = engine.execute_test(scenario)

    assert result.success is True
    assert result.operations_executed == 0
    assert result.validation_results == []
    assert result.final_validation.overall_success is True
    mock_connection_class.assert_called_once_with(
        initial_nodes=cluster_instance.nodes,
        cluster_id=cluster_instance.cluster_id,
        topology_type=TopologyType.STANDALONE,
    )
    mock_load_all_slots.assert_not_called()
    mock_executor_class.assert_not_called()
    mock_validator.write_test_data.assert_called_once_with(mock_connection)
    mock_validator.validate_with_retry.assert_called_once_with(
        mock_connection,
        engine._build_expected_topology(scenario),
        None,
    )


def test_parallel_executor_runs_validation_after_each_wave():
    operation_orchestrator = Mock()
    operation_orchestrator.execute_operation.return_value = True

    chaos_coordinator = Mock()
    chaos_coordinator.coordinate_chaos_with_operation.return_value = []

    fuzzer_logger = Mock()
    executor = ParallelExecutor(operation_orchestrator, chaos_coordinator, fuzzer_logger)

    operations = [
        Operation(
            type=OperationType.FAILOVER,
            target_node="shard-0-primary",
            parameters={},
            timing=OperationTiming(),
        ),
        Operation(
            type=OperationType.FAILOVER,
            target_node="shard-1-primary",
            parameters={},
            timing=OperationTiming(),
        ),
        Operation(
            type=OperationType.FAILOVER,
            target_node="shard-0-primary",
            parameters={},
            timing=OperationTiming(),
        ),
    ]

    validation_runner = Mock(
        side_effect=[
            build_validation_result(True),
            build_validation_result(True),
        ]
    )

    operations_executed, chaos_events, validation_results = executor.execute_operations_parallel(
        operations,
        build_scenario().chaos_config,
        Mock(get_live_nodes=Mock(return_value=[])),
        "cluster-1",
        validation_runner=validation_runner,
    )

    assert operations_executed == 3
    assert chaos_events == []
    assert len(validation_results) == 2
    assert validation_runner.call_count == 2

    first_wave_results = validation_runner.call_args_list[0].args[1]
    second_wave_results = validation_runner.call_args_list[1].args[1]

    assert [result[0] for result in first_wave_results] == [0, 1]
    assert [result[0] for result in second_wave_results] == [2]
