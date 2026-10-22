"""
DSL Utilities - Helper functions for DSL configuration handling
"""

from collections.abc import Mapping
from enum import Enum
from pathlib import Path
from typing import Any, Optional, Union

import yaml

from ..models import DSLConfig, Scenario, TopologyType
from ..utils.server_config import validate_server_config
from ..utils.topology_config import validate_base_port_number, validate_topology_base_port


def _canonicalize_for_safe_yaml(value: Any) -> Any:
    """Convert generated DSL values to structures supported by safe YAML."""
    if isinstance(value, Enum):
        return _canonicalize_for_safe_yaml(value.value)
    if isinstance(value, Mapping):
        return {_canonicalize_for_safe_yaml(key): _canonicalize_for_safe_yaml(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonicalize_for_safe_yaml(item) for item in value]
    if isinstance(value, (set, frozenset)):
        items = [_canonicalize_for_safe_yaml(item) for item in value]
        return sorted(items, key=repr)
    return value


class DSLLoader:
    """Utility class for loading and validating DSL configurations"""

    @staticmethod
    def load_from_file(file_path: Union[str, Path]) -> DSLConfig:
        """Load DSL configuration from a YAML file."""
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"DSL file not found: {file_path}")

        try:
            with open(file_path) as f:
                config_text = f.read()

            yaml.safe_load(config_text)
            return DSLConfig(config_text=config_text)

        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML syntax in {file_path}: {e}") from e
        except Exception as e:
            raise ValueError(f"Error loading DSL file {file_path}: {e}") from e

    @staticmethod
    def load_from_string(config_text: str) -> DSLConfig:
        """Load DSL configuration from a YAML string."""
        try:
            yaml.safe_load(config_text)
            return DSLConfig(config_text=config_text)

        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML syntax: {e}") from e

    @staticmethod
    def validate_dsl_config(dsl_config: DSLConfig, scenario_generator) -> bool:
        """Validate a DSL configuration by parsing and validating the scenario."""
        scenario = scenario_generator.parse_dsl_config(dsl_config.config_text)
        scenario_generator.validate_scenario(scenario)
        return True

    @staticmethod
    def _serialize_state_validation_config(config) -> dict:
        """Serialize StateValidationConfig and all nested configs to dictionary."""
        result = {
            "check_replication": config.check_replication,
            "check_cluster_status": config.check_cluster_status,
            "check_slot_coverage": config.check_slot_coverage,
            "check_topology": config.check_topology,
            "check_view_consistency": config.check_view_consistency,
            "check_data_consistency": config.check_data_consistency,
            "check_logs": config.check_logs,
            "stabilization_wait": config.stabilization_wait,
            "validation_timeout": config.validation_timeout,
            "blocking_on_failure": config.blocking_on_failure,
            "retry_on_transient_failure": config.retry_on_transient_failure,
            "max_retries": config.max_retries,
            "retry_delay": config.retry_delay,
        }

        # Serialize replication config
        if config.replication_config:
            result["replication_config"] = {
                "max_acceptable_lag": config.replication_config.max_acceptable_lag,
                "require_all_replicas_synced": config.replication_config.require_all_replicas_synced,
                "check_replication_offset": config.replication_config.check_replication_offset,
                "min_replicas_per_shard": config.replication_config.min_replicas_per_shard,
                "timeout": config.replication_config.timeout,
            }

        # Serialize cluster status config
        if config.cluster_status_config:
            result["cluster_status_config"] = {
                "acceptable_states": config.cluster_status_config.acceptable_states,
                "allow_degraded": config.cluster_status_config.allow_degraded,
                "require_quorum": config.cluster_status_config.require_quorum,
                "timeout": config.cluster_status_config.timeout,
            }

        # Serialize slot coverage config
        if config.slot_coverage_config:
            result["slot_coverage_config"] = {
                "require_full_coverage": config.slot_coverage_config.require_full_coverage,
                "allow_slot_conflicts": config.slot_coverage_config.allow_slot_conflicts,
                "timeout": config.slot_coverage_config.timeout,
            }

        # Serialize topology config
        if config.topology_config:
            result["topology_config"] = {
                "strict_mode": config.topology_config.strict_mode,
                "allow_failed_nodes": config.topology_config.allow_failed_nodes,
                "timeout": config.topology_config.timeout,
            }

        # Serialize view consistency config
        if config.view_consistency_config:
            result["view_consistency_config"] = {
                "require_full_consensus": config.view_consistency_config.require_full_consensus,
                "allow_transient_inconsistency": config.view_consistency_config.allow_transient_inconsistency,
                "max_inconsistency_duration": config.view_consistency_config.max_inconsistency_duration,
                "timeout": config.view_consistency_config.timeout,
            }

        # Serialize data consistency config
        if config.data_consistency_config:
            result["data_consistency_config"] = {
                "check_test_keys": config.data_consistency_config.check_test_keys,
                "check_cross_replica_consistency": config.data_consistency_config.check_cross_replica_consistency,
                "num_test_keys": config.data_consistency_config.num_test_keys,
                "key_prefix": config.data_consistency_config.key_prefix,
                "timeout": config.data_consistency_config.timeout,
            }

        return result

    @staticmethod
    def save_scenario_as_dsl(scenario: Scenario, file_path: Union[str, Path]) -> None:
        """Save a scenario as a DSL YAML file for reproducibility."""
        file_path = Path(file_path)

        topology_type = scenario.cluster_config.topology_type
        if isinstance(topology_type, str):
            topology_type = TopologyType(topology_type)

        use_topology_format = scenario.api_version == "v2" or topology_type != TopologyType.CLUSTER
        api_version = "v2" if use_topology_format else "v1"

        topology_dict = {
            "num_shards": scenario.cluster_config.num_shards,
            "replicas_per_shard": scenario.cluster_config.replicas_per_shard,
            "base_port": scenario.cluster_config.base_port,
            "base_data_dir": scenario.cluster_config.base_data_dir,
            "valkey_binary": scenario.cluster_config.valkey_binary,
            "enable_cleanup": scenario.cluster_config.enable_cleanup,
        }
        if scenario.cluster_config.server_config:
            topology_dict["server_config"] = scenario.cluster_config.server_config
        if scenario.cluster_config.node_configs:
            topology_dict["node_configs"] = scenario.cluster_config.node_configs
        if use_topology_format:
            topology_dict = {"type": topology_type.value, **topology_dict}

        dsl_dict = {
            "scenario_id": scenario.scenario_id,
            "seed": scenario.seed,
        }
        if use_topology_format:
            dsl_dict["api_version"] = api_version
            dsl_dict["topology"] = topology_dict
        else:
            dsl_dict["cluster"] = topology_dict

        serialized_operations = [
            {
                "type": op.type.value,
                "target_node": op.target_node,
                "parameters": op.parameters,
                "timing": {
                    "delay_before": op.timing.delay_before,
                    "timeout": op.timing.timeout,
                    "delay_after": op.timing.delay_after,
                },
            }
            for op in scenario.operations
        ]
        if serialized_operations or not use_topology_format:
            dsl_dict["operations"] = serialized_operations

        if scenario.operations:
            dsl_dict["chaos"] = {
                "type": scenario.chaos_config.chaos_type.value,
                "process_chaos_type": scenario.chaos_config.process_chaos_type.value
                if scenario.chaos_config.process_chaos_type
                else None,
                "randomize_per_operation": scenario.chaos_config.randomize_per_operation,
                "target_selection": {
                    "strategy": scenario.chaos_config.target_selection.strategy,
                    "specific_nodes": scenario.chaos_config.target_selection.specific_nodes,
                },
                "timing": {
                    "delay_before_operation": scenario.chaos_config.timing.delay_before_operation,
                    "delay_after_operation": scenario.chaos_config.timing.delay_after_operation,
                    "chaos_duration": scenario.chaos_config.timing.chaos_duration,
                },
                "coordination": {
                    "chaos_before_operation": scenario.chaos_config.coordination.chaos_before_operation,
                    "chaos_during_operation": scenario.chaos_config.coordination.chaos_during_operation,
                    "chaos_after_operation": scenario.chaos_config.coordination.chaos_after_operation,
                },
            }

        dsl_dict["state_validation"] = (
            DSLLoader._serialize_state_validation_config(scenario.state_validation_config)
            if scenario.state_validation_config
            else None
        )

        with open(file_path, "w") as f:
            yaml.safe_dump(
                _canonicalize_for_safe_yaml(dsl_dict),
                f,
                default_flow_style=False,
                sort_keys=False,
            )


class DSLValidator:
    """Validator for DSL configurations with detailed error reporting"""

    @staticmethod
    def validate_structure(config_dict: dict) -> list:
        """
        Validate the structure of a DSL configuration dictionary.
        """
        errors = []

        if not isinstance(config_dict, dict):
            return ["DSL configuration must be a mapping"]

        if "scenario_id" not in config_dict:
            errors.append("Missing required field: scenario_id")

        api_version = config_dict.get("api_version")
        if api_version is not None and api_version not in {"v1", "v2"}:
            errors.append(f"Unsupported api_version: {api_version}")

        has_legacy_cluster = "cluster" in config_dict
        has_topology = "topology" in config_dict
        if has_legacy_cluster and has_topology:
            errors.append("Specify only one of 'cluster' or 'topology'")
        elif not has_legacy_cluster and not has_topology:
            errors.append("Missing required field: cluster or topology")

        topology_type = TopologyType.CLUSTER
        operations_optional = False
        if has_legacy_cluster:
            errors.extend(DSLValidator._validate_cluster(config_dict["cluster"]))
            if "operations" not in config_dict:
                errors.append("Missing required field: operations")
        elif has_topology:
            if api_version not in {None, "v2"}:
                errors.append("The top-level 'topology' format requires api_version: v2")
            topology_errors, topology_type = DSLValidator._validate_topology(config_dict["topology"])
            errors.extend(topology_errors)
            operations_optional = True

        # Validate operations
        if "operations" in config_dict:
            operations_errors = DSLValidator._validate_operations(
                config_dict["operations"],
                allow_empty=operations_optional,
                topology_type=topology_type,
            )
            errors.extend(operations_errors)

        # Validate chaos configuration (optional)
        if "chaos" in config_dict:
            chaos_errors = DSLValidator._validate_chaos(config_dict["chaos"])
            errors.extend(chaos_errors)
            if config_dict.get("chaos") and not config_dict.get("operations"):
                errors.append("chaos: Requires at least one operation")

        # Validate state validation configuration (optional)
        if "state_validation" in config_dict:
            validation_errors = DSLValidator._validate_state_validation(config_dict["state_validation"])
            errors.extend(validation_errors)

        return errors

    @staticmethod
    def _validate_cluster(cluster_dict: dict) -> list:
        """Validate cluster configuration"""
        errors = []

        if not isinstance(cluster_dict, dict):
            return ["cluster: Must be a mapping"]

        required_fields = ["num_shards", "replicas_per_shard"]
        for field in required_fields:
            if field not in cluster_dict:
                errors.append(f"cluster: Missing required field '{field}'")

        # Validate ranges
        if "num_shards" in cluster_dict:
            num_shards = cluster_dict["num_shards"]
            if not isinstance(num_shards, int) or not (3 <= num_shards <= 16):
                errors.append(f"cluster.num_shards: Must be an integer between 3 and 16, got {num_shards}")

        if "replicas_per_shard" in cluster_dict:
            replicas = cluster_dict["replicas_per_shard"]
            if not isinstance(replicas, int) or isinstance(replicas, bool) or not (0 <= replicas <= 5):
                errors.append(f"cluster.replicas_per_shard: Must be an integer between 0 and 5, got {replicas}")

        valid_node_selectors = None
        num_shards = cluster_dict.get("num_shards")
        replicas = cluster_dict.get("replicas_per_shard")
        if (
            isinstance(num_shards, int)
            and not isinstance(num_shards, bool)
            and 3 <= num_shards <= 16
            and isinstance(replicas, int)
            and not isinstance(replicas, bool)
            and 0 <= replicas <= 5
        ):
            valid_node_selectors = DSLValidator._valid_node_config_selectors(
                TopologyType.CLUSTER,
                num_shards,
                replicas,
            )

        errors.extend(
            DSLValidator._validate_base_port(
                cluster_dict,
                "cluster",
                TopologyType.CLUSTER,
                num_shards,
                replicas,
                valid_node_selectors is not None,
            )
        )
        errors.extend(
            DSLValidator._validate_server_configs(
                cluster_dict,
                "cluster",
                valid_node_selectors,
            )
        )
        return errors

    @staticmethod
    def _validate_topology(topology_dict: dict) -> tuple[list, TopologyType]:
        """Validate a v2 topology configuration."""
        errors = []
        if not isinstance(topology_dict, dict):
            return ["topology: Must be a mapping"], TopologyType.CLUSTER
        if "type" not in topology_dict:
            return ["topology: Missing required field 'type'"], TopologyType.CLUSTER

        try:
            topology_type = TopologyType(topology_dict["type"])
        except ValueError:
            return [f"topology.type: Invalid topology type '{topology_dict['type']}'"], TopologyType.CLUSTER

        defaults = {
            TopologyType.STANDALONE: (1, 0),
            TopologyType.REPLICATION: (1, 1),
            TopologyType.CLUSTER: (3, 1),
        }
        default_shards, default_replicas = defaults[topology_type]
        num_shards = topology_dict.get("num_shards", default_shards)
        replicas = topology_dict.get("replicas_per_shard", default_replicas)

        if not isinstance(num_shards, int) or isinstance(num_shards, bool):
            errors.append(f"topology.num_shards: Must be an integer, got {num_shards}")
        elif topology_type == TopologyType.CLUSTER and not (3 <= num_shards <= 16):
            errors.append(f"topology.num_shards: Must be between 3 and 16, got {num_shards}")
        elif topology_type != TopologyType.CLUSTER and num_shards != 1:
            errors.append(f"topology.num_shards: Must be 1 for {topology_type.value} topology, got {num_shards}")

        if not isinstance(replicas, int) or isinstance(replicas, bool):
            errors.append(f"topology.replicas_per_shard: Must be an integer, got {replicas}")
        elif topology_type == TopologyType.CLUSTER and not (0 <= replicas <= 5):
            errors.append(f"topology.replicas_per_shard: Must be between 0 and 5, got {replicas}")
        elif topology_type == TopologyType.REPLICATION and not (1 <= replicas <= 5):
            errors.append(f"topology.replicas_per_shard: Must be between 1 and 5 for replication, got {replicas}")
        elif topology_type == TopologyType.STANDALONE and replicas != 0:
            errors.append(f"topology.replicas_per_shard: Must be 0 for standalone, got {replicas}")

        dimensions_valid = (
            isinstance(num_shards, int)
            and not isinstance(num_shards, bool)
            and isinstance(replicas, int)
            and not isinstance(replicas, bool)
            and (
                (topology_type == TopologyType.CLUSTER and 3 <= num_shards <= 16 and 0 <= replicas <= 5)
                or (topology_type == TopologyType.REPLICATION and num_shards == 1 and 1 <= replicas <= 5)
                or (topology_type == TopologyType.STANDALONE and num_shards == 1 and replicas == 0)
            )
        )
        valid_node_selectors = (
            DSLValidator._valid_node_config_selectors(topology_type, num_shards, replicas) if dimensions_valid else None
        )
        errors.extend(
            DSLValidator._validate_base_port(
                topology_dict,
                "topology",
                topology_type,
                num_shards,
                replicas,
                dimensions_valid,
            )
        )
        errors.extend(
            DSLValidator._validate_server_configs(
                topology_dict,
                "topology",
                valid_node_selectors,
            )
        )
        return errors, topology_type

    @staticmethod
    def _valid_node_config_selectors(
        topology_type: TopologyType,
        num_shards: int,
        replicas_per_shard: int,
    ) -> set[str]:
        role_selectors = {"primary"}
        if replicas_per_shard > 0:
            role_selectors.add("replica")

        if topology_type == TopologyType.CLUSTER:
            node_count = num_shards * (1 + replicas_per_shard)
        else:
            node_count = 1 + replicas_per_shard

        return role_selectors | {f"node-{node_index}" for node_index in range(node_count)}

    @staticmethod
    def _validate_base_port(
        config_dict: dict,
        prefix: str,
        topology_type: TopologyType,
        num_shards: Any,
        replicas_per_shard: Any,
        dimensions_valid: bool,
    ) -> list:
        base_port = config_dict.get("base_port", 7000)
        try:
            validate_base_port_number(base_port)
        except ValueError as exc:
            return [f"{prefix}.base_port: {exc}"]

        if not dimensions_valid:
            return []

        try:
            validate_topology_base_port(
                topology_type,
                num_shards,
                replicas_per_shard,
                base_port,
            )
        except ValueError as exc:
            return [f"{prefix}.base_port: {exc}"]
        return []

    @staticmethod
    def _validate_server_configs(
        config_dict: dict,
        prefix: str,
        valid_node_selectors: Optional[set[str]] = None,
    ) -> list:
        errors = []
        if "server_config" in config_dict:
            server_config = config_dict["server_config"]
            if not isinstance(server_config, dict):
                errors.append(f"{prefix}.server_config: Must be a mapping")
            else:
                try:
                    validate_server_config(server_config)
                except (TypeError, ValueError) as exc:
                    errors.append(f"{prefix}.server_config: {exc}")

        if "node_configs" in config_dict:
            node_configs = config_dict["node_configs"]
            if not isinstance(node_configs, dict):
                errors.append(f"{prefix}.node_configs: Must be a mapping")
            else:
                for node_id, node_config in node_configs.items():
                    if not isinstance(node_id, str):
                        errors.append(f"{prefix}.node_configs: Keys must be node ID strings")
                    if not isinstance(node_config, dict):
                        errors.append(f"{prefix}.node_configs.{node_id}: Must be a mapping")
                    else:
                        try:
                            validate_server_config(node_config)
                        except (TypeError, ValueError) as exc:
                            errors.append(f"{prefix}.node_configs.{node_id}: {exc}")
                    if (
                        isinstance(node_id, str)
                        and valid_node_selectors is not None
                        and node_id not in valid_node_selectors
                    ):
                        errors.append(f"{prefix}.node_configs: Unknown selector '{node_id}'")
        return errors

    @staticmethod
    def _validate_operations(
        operations_list: list,
        allow_empty: bool = False,
        topology_type: TopologyType = TopologyType.CLUSTER,
    ) -> list:
        """Validate operations list"""
        errors = []

        if not isinstance(operations_list, list):
            errors.append("operations: Must be a list")
            return errors

        if len(operations_list) == 0 and not allow_empty:
            errors.append("operations: Must contain at least one operation")

        for i, op in enumerate(operations_list):
            if not isinstance(op, dict):
                errors.append(f"operations[{i}]: Must be a dictionary")
                continue

            # Check required fields
            if "type" not in op:
                errors.append(f"operations[{i}]: Missing required field 'type'")
            elif op["type"] not in ["failover"]:  # Add more types as they're implemented
                errors.append(f"operations[{i}]: Invalid operation type '{op['type']}'")
            elif op["type"] == "failover" and topology_type != TopologyType.CLUSTER:
                errors.append(f"operations[{i}]: FAILOVER is only supported for cluster topology")

            if "target_node" not in op:
                errors.append(f"operations[{i}]: Missing required field 'target_node'")

        return errors

    @staticmethod
    def _validate_chaos(chaos_dict: dict) -> list:
        """Validate chaos configuration"""
        errors = []

        if not isinstance(chaos_dict, dict):
            errors.append("chaos: Must be a dictionary")
            return errors

        # Validate chaos type
        if "type" in chaos_dict and chaos_dict["type"] not in ["process_kill"]:  # Add more types as implemented
            errors.append(f"chaos.type: Invalid chaos type '{chaos_dict['type']}'")

        # Validate target selection
        if "target_selection" in chaos_dict:
            target_sel = chaos_dict["target_selection"]
            if "strategy" in target_sel:
                valid_strategies = ["random", "primary_only", "replica_only", "specific"]
                if target_sel["strategy"] not in valid_strategies:
                    errors.append(f"chaos.target_selection.strategy: Must be one of {valid_strategies}")

        return errors

    @staticmethod
    def _validate_state_validation(validation_dict: dict) -> list:
        """Validate state validation configuration"""
        errors = []

        if not isinstance(validation_dict, dict):
            errors.append("state_validation: Must be a dictionary")
            return errors

        # All fields are optional booleans or floats, just check types if present
        bool_fields = [
            "check_replication",
            "check_cluster_status",
            "check_slot_coverage",
            "check_topology",
            "check_view_consistency",
            "check_data_consistency",
            "check_logs",
            "blocking_on_failure",
            "retry_on_transient_failure",
        ]

        for field in bool_fields:
            if field in validation_dict and not isinstance(validation_dict[field], bool):
                errors.append(f"state_validation.{field}: Must be a boolean")

        # Fields that must be strictly positive (> 0)
        positive_fields = ["validation_timeout"]
        for field in positive_fields:
            if field in validation_dict:
                value = validation_dict[field]
                if not isinstance(value, (int, float)) or value <= 0:
                    errors.append(f"state_validation.{field}: Must be a positive number")

        # Fields that must be non-negative (>= 0)
        non_negative_fields = ["stabilization_wait", "retry_delay"]
        for field in non_negative_fields:
            if field in validation_dict:
                value = validation_dict[field]
                if not isinstance(value, (int, float)) or value < 0:
                    errors.append(f"state_validation.{field}: Must be a non-negative number")

        int_fields = ["max_retries"]
        for field in int_fields:
            if field in validation_dict:
                value = validation_dict[field]
                if not isinstance(value, int) or value < 0:
                    errors.append(f"state_validation.{field}: Must be a non-negative integer")

        return errors
