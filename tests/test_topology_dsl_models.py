"""Focused tests for generic topology models and the v2 scenario DSL."""

from unittest.mock import Mock, patch

import pytest
import yaml

from src.fuzzer_engine.dsl_utils import DSLLoader, DSLValidator
from src.fuzzer_engine.test_case_generator import ScenarioGenerator
from src.models import (
    ChaosConfig,
    ChaosCoordination,
    ChaosTiming,
    ChaosType,
    ClusterConfig,
    ClusterConnection,
    NodeInfo,
    ProcessChaosType,
    Scenario,
    TargetSelection,
    TopologyType,
)


def _node(node_id: str, role: str, port: int, process=None) -> NodeInfo:
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
    )


def _chaos_config() -> ChaosConfig:
    return ChaosConfig(
        chaos_type=ChaosType.PROCESS_KILL,
        target_selection=TargetSelection(strategy="random"),
        timing=ChaosTiming(),
        coordination=ChaosCoordination(),
    )


def test_model_defaults_preserve_cluster_compatibility():
    config = ClusterConfig(num_shards=3, replicas_per_shard=1)
    scenario = Scenario("legacy", config, [], _chaos_config())

    assert config.topology_type == TopologyType.CLUSTER
    assert config.server_config == {}
    assert config.node_configs == {}
    assert scenario.api_version == "v1"


def test_parse_minimal_standalone_topology_without_operations():
    scenario = ScenarioGenerator().parse_dsl_config(
        """
api_version: v2
scenario_id: standalone
topology:
  type: standalone
"""
    )

    assert scenario.api_version == "v2"
    assert scenario.cluster_config.topology_type == TopologyType.STANDALONE
    assert scenario.cluster_config.num_shards == 1
    assert scenario.cluster_config.replicas_per_shard == 0
    assert scenario.operations == []
    assert scenario.chaos_config.coordination.chaos_before_operation is False
    assert scenario.chaos_config.coordination.chaos_during_operation is False
    assert scenario.chaos_config.coordination.chaos_after_operation is False
    assert ScenarioGenerator().validate_scenario(scenario)


def test_parse_replication_topology_and_server_config():
    scenario = ScenarioGenerator().parse_dsl_config(
        """
scenario_id: replication
topology:
  type: replication
  replicas_per_shard: 2
  server_config:
    repl-diskless-sync: "yes"
    repl-backlog-size: 4mb
  node_configs:
    replica:
      client-output-buffer-limit: replica 256mb 64mb 60
    node-1:
      repl-diskless-load: swapdb
"""
    )

    config = scenario.cluster_config
    assert scenario.api_version == "v2"
    assert config.topology_type == TopologyType.REPLICATION
    assert config.num_shards == 1
    assert config.replicas_per_shard == 2
    assert config.server_config["repl-diskless-sync"] == "yes"
    assert config.node_configs["replica"]["client-output-buffer-limit"] == "replica 256mb 64mb 60"
    assert config.node_configs["node-1"]["repl-diskless-load"] == "swapdb"


@pytest.mark.parametrize(
    ("topology", "message"),
    [
        ({"type": "standalone", "replicas_per_shard": 1}, "standalone topology requires"),
        ({"type": "standalone", "num_shards": 2}, "standalone topology requires"),
        ({"type": "replication", "replicas_per_shard": 0}, "replication topology requires"),
        ({"type": "replication", "num_shards": 2}, "replication topology requires"),
        ({"type": "cluster", "num_shards": 2}, "num_shards must be between 3 and 16"),
    ],
)
def test_topology_specific_dimensions_are_validated(topology, message):
    dsl = yaml.safe_dump({"scenario_id": "invalid", "topology": topology})

    with pytest.raises(ValueError, match=message):
        ScenarioGenerator().parse_dsl_config(dsl)


def test_failover_is_rejected_outside_cluster():
    with pytest.raises(ValueError, match="only supported for cluster topology"):
        ScenarioGenerator().parse_dsl_config(
            """
api_version: v2
scenario_id: invalid-failover
topology:
  type: replication
operations:
  - type: failover
    target_node: node-0
"""
        )


@pytest.mark.parametrize(
    ("topology", "selector"),
    [
        ({"type": "standalone"}, "replica"),
        ({"type": "standalone"}, "node-1"),
        ({"type": "replication", "replicas_per_shard": 2}, "node-3"),
        ({"type": "cluster", "num_shards": 3, "replicas_per_shard": 0}, "replica"),
        ({"type": "cluster", "num_shards": 3, "replicas_per_shard": 1}, "node-6"),
    ],
)
def test_unknown_node_config_selectors_are_rejected(topology, selector):
    topology["node_configs"] = {selector: {"maxmemory": "128mb"}}
    dsl = yaml.safe_dump({"scenario_id": "invalid-selector", "topology": topology, "operations": []})

    with pytest.raises(ValueError, match="Unknown node_configs selector"):
        ScenarioGenerator().parse_dsl_config(dsl)


def test_role_and_exact_node_config_selectors_are_accepted():
    scenario = ScenarioGenerator().parse_dsl_config(
        """
scenario_id: valid-selectors
topology:
  type: cluster
  num_shards: 3
  replicas_per_shard: 1
  node_configs:
    primary:
      appendonly: "yes"
    replica:
      replica-read-only: "yes"
    node-5:
      maxmemory: 128mb
operations:
  - type: failover
    target_node: shard-0-primary
"""
    )

    assert set(scenario.cluster_config.node_configs) == {"primary", "replica", "node-5"}


def test_legacy_cluster_still_requires_operations():
    with pytest.raises(ValueError, match="Missing required field: operations"):
        ScenarioGenerator().parse_dsl_config(
            """
scenario_id: legacy
cluster:
  num_shards: 3
  replicas_per_shard: 1
"""
        )


def test_topology_dsl_round_trip_preserves_configuration(tmp_path):
    scenario = ScenarioGenerator().parse_dsl_config(
        """
api_version: v2
scenario_id: round-trip
topology:
  type: standalone
  server_config:
    save: ""
    appendonly: "no"
  node_configs:
    node-0:
      maxmemory: 128mb
"""
    )
    output_path = tmp_path / "scenario.yaml"

    DSLLoader.save_scenario_as_dsl(scenario, output_path)
    serialized = yaml.safe_load(output_path.read_text())
    recreated = ScenarioGenerator().parse_dsl_config(output_path.read_text())

    assert "cluster" not in serialized
    assert serialized["api_version"] == "v2"
    assert serialized["topology"]["type"] == "standalone"
    assert "operations" not in serialized
    assert "chaos" not in serialized
    assert recreated.cluster_config.server_config == scenario.cluster_config.server_config
    assert recreated.cluster_config.node_configs == scenario.cluster_config.node_configs


def test_check_logs_false_survives_parse_and_round_trip(tmp_path):
    scenario = ScenarioGenerator().parse_dsl_config(
        """
scenario_id: no-log-validation
topology:
  type: standalone
state_validation:
  check_logs: false
"""
    )
    output_path = tmp_path / "scenario.yaml"

    DSLLoader.save_scenario_as_dsl(scenario, output_path)
    serialized = yaml.safe_load(output_path.read_text())
    recreated = ScenarioGenerator().parse_dsl_config(output_path.read_text())

    assert scenario.state_validation_config.check_logs is False
    assert serialized["state_validation"]["check_logs"] is False
    assert recreated.state_validation_config.check_logs is False


def test_structure_validator_accepts_operation_free_standalone():
    errors = DSLValidator.validate_structure(
        {
            "api_version": "v2",
            "scenario_id": "standalone",
            "topology": {"type": "standalone"},
        }
    )

    assert errors == []


def test_v2_cluster_can_run_as_operation_free_protocol_smoke(tmp_path):
    dsl = """
api_version: v2
scenario_id: cluster-protocol-smoke
topology:
  type: cluster
  num_shards: 3
  replicas_per_shard: 0
"""
    scenario = ScenarioGenerator().parse_dsl_config(dsl)
    output_path = tmp_path / "scenario.yaml"

    assert scenario.operations == []
    assert ScenarioGenerator().validate_scenario(scenario)
    assert DSLValidator.validate_structure(yaml.safe_load(dsl)) == []

    DSLLoader.save_scenario_as_dsl(scenario, output_path)
    serialized = yaml.safe_load(output_path.read_text())
    recreated = ScenarioGenerator().parse_dsl_config(output_path.read_text())

    assert "operations" not in serialized
    assert recreated.cluster_config.topology_type == TopologyType.CLUSTER
    assert recreated.operations == []


def test_operation_free_topology_rejects_explicit_chaos():
    config = {
        "api_version": "v2",
        "scenario_id": "ambiguous-chaos",
        "topology": {"type": "replication"},
        "operations": [],
        "chaos": {"type": "process_kill"},
    }

    with pytest.raises(ValueError, match="requires at least one operation"):
        ScenarioGenerator().parse_dsl_config(yaml.safe_dump(config))

    assert "chaos: Requires at least one operation" in DSLValidator.validate_structure(config)


def test_operation_free_topology_rejects_chaos_when_operations_are_omitted():
    config = {
        "api_version": "v2",
        "scenario_id": "ambiguous-chaos",
        "topology": {"type": "standalone"},
        "chaos": {"type": "process_kill"},
    }

    with pytest.raises(ValueError, match="requires at least one operation"):
        ScenarioGenerator().parse_dsl_config(yaml.safe_dump(config))

    assert "chaos: Requires at least one operation" in DSLValidator.validate_structure(config)


@pytest.mark.parametrize(
    ("topology", "message"),
    [
        ({"type": "standalone", "base_port": True}, "base_port must be an integer"),
        ({"type": "standalone", "base_port": 0}, "base_port must be between 1 and 65535"),
        (
            {"type": "replication", "replicas_per_shard": 1, "base_port": 65535},
            "would require client port 65536",
        ),
        (
            {"type": "cluster", "num_shards": 3, "replicas_per_shard": 0, "base_port": 55534},
            "would require cluster bus port 65536",
        ),
    ],
)
def test_base_port_is_validated_before_topology_execution(topology, message):
    config = {
        "api_version": "v2",
        "scenario_id": "invalid-port",
        "topology": topology,
    }

    with pytest.raises(ValueError, match=message):
        ScenarioGenerator().parse_dsl_config(yaml.safe_dump(config))

    assert any(message in error for error in DSLValidator.validate_structure(config))


def test_legacy_cluster_base_port_checks_cluster_bus_range():
    config = {
        "scenario_id": "legacy-invalid-port",
        "cluster": {
            "num_shards": 3,
            "replicas_per_shard": 0,
            "base_port": 55534,
        },
        "operations": [
            {
                "type": "failover",
                "target_node": "shard-0-primary",
            }
        ],
    }

    with pytest.raises(ValueError, match="would require cluster bus port 65536"):
        ScenarioGenerator().parse_dsl_config(yaml.safe_dump(config))

    assert any("would require cluster bus port 65536" in error for error in DSLValidator.validate_structure(config))


@pytest.mark.parametrize(
    "topology",
    [
        {"type": "standalone", "base_port": 65535},
        {"type": "replication", "replicas_per_shard": 1, "base_port": 65534},
        {"type": "cluster", "num_shards": 3, "replicas_per_shard": 0, "base_port": 55533},
    ],
)
def test_base_port_accepts_highest_valid_topology_boundary(topology):
    config = {
        "api_version": "v2",
        "scenario_id": "valid-port",
        "topology": topology,
    }

    scenario = ScenarioGenerator().parse_dsl_config(yaml.safe_dump(config))

    assert scenario.cluster_config.base_port == topology["base_port"]
    assert DSLValidator.validate_structure(config) == []


@pytest.mark.parametrize(
    ("topology_type", "num_shards", "replicas_per_shard", "base_port", "message"),
    [
        (TopologyType.STANDALONE, 1, 0, False, "base_port must be an integer"),
        (TopologyType.REPLICATION, 1, 2, 65534, "would require client port 65536"),
        (TopologyType.CLUSTER, 3, 1, 55531, "would require cluster bus port 65536"),
    ],
)
def test_programmatic_scenario_validation_checks_all_allocated_ports(
    topology_type,
    num_shards,
    replicas_per_shard,
    base_port,
    message,
):
    scenario = Scenario(
        "invalid-port",
        ClusterConfig(
            num_shards=num_shards,
            replicas_per_shard=replicas_per_shard,
            topology_type=topology_type,
            base_port=base_port,
        ),
        [],
        _chaos_config(),
        api_version="v2",
    )

    with pytest.raises(ValueError, match=message):
        ScenarioGenerator().validate_scenario(scenario)


def test_safe_dsl_serialization_canonicalizes_nested_tuples_and_enums(tmp_path):
    scenario = ScenarioGenerator().parse_dsl_config(
        """
api_version: v2
scenario_id: safe-yaml
topology:
  type: standalone
"""
    )
    scenario.cluster_config.server_config = {
        "save": (
            "900 1",
            ProcessChaosType.SIGKILL,
        )
    }
    output_path = tmp_path / "scenario.yaml"

    DSLLoader.save_scenario_as_dsl(scenario, output_path)
    serialized_text = output_path.read_text()
    serialized = yaml.safe_load(serialized_text)
    recreated = ScenarioGenerator().parse_dsl_config(serialized_text)

    assert "!!python" not in serialized_text
    assert serialized["topology"]["server_config"]["save"] == ["900 1", "sigkill"]
    assert recreated.cluster_config.server_config["save"] == ["900 1", "sigkill"]


def test_structure_validator_rejects_unknown_node_selector_and_bad_check_logs():
    errors = DSLValidator.validate_structure(
        {
            "scenario_id": "invalid",
            "topology": {
                "type": "standalone",
                "node_configs": {"node-1": {"appendonly": "yes"}},
            },
            "state_validation": {"check_logs": "no"},
        }
    )

    assert "topology.node_configs: Unknown selector 'node-1'" in errors
    assert "state_validation.check_logs: Must be a boolean" in errors


@pytest.mark.parametrize(
    ("config_fragment", "message"),
    [
        ({"server_config": {"port": 9000}}, "managed by the orchestrator"),
        ({"server_config": {"bad option": "yes"}}, "Invalid Valkey server configuration option"),
        ({"server_config": {"repl-compression": None}}, "cannot have a null value"),
        (
            {"node_configs": {"node-0": {"cluster-enabled": "yes"}}},
            "managed by the orchestrator",
        ),
    ],
)
def test_server_config_safety_is_validated_before_execution(config_fragment, message):
    topology = {"type": "standalone", **config_fragment}
    config = {
        "api_version": "v2",
        "scenario_id": "unsafe-config",
        "topology": topology,
    }

    with pytest.raises(ValueError, match=message):
        ScenarioGenerator().parse_dsl_config(yaml.safe_dump(config))

    assert any(message in error for error in DSLValidator.validate_structure(config))


def test_non_cluster_connection_uses_process_and_replication_info():
    running_process = Mock()
    running_process.poll.return_value = None
    stopped_process = Mock()
    stopped_process.poll.return_value = 1
    nodes = [
        _node("node-0", "replica", 7000, running_process),
        _node("node-1", "replica", 7001, stopped_process),
    ]
    connection = ClusterConnection(nodes, "replication", TopologyType.REPLICATION)

    with patch.object(connection, "_query_replication_info", return_value={"role": "master"}) as query_info:
        current = connection.get_current_nodes()

    assert current[0]["role"] == "primary"
    assert current[0]["status"] == "connected"
    assert current[1]["status"] == "failed"
    query_info.assert_called_once_with(nodes[0])


def test_non_cluster_live_nodes_exclude_unreachable_processes():
    running_process = Mock()
    running_process.poll.return_value = None
    stopped_process = Mock()
    stopped_process.poll.return_value = 1
    connection = ClusterConnection(
        [
            _node("node-0", "primary", 7000, running_process),
            _node("node-1", "replica", 7001, stopped_process),
        ],
        "replication",
        TopologyType.REPLICATION,
    )

    with patch.object(connection, "_query_replication_info", return_value={"role": "master"}):
        live_nodes = connection.get_live_nodes()

    assert [node["node_id"] for node in live_nodes] == ["node-0"]
