"""
Test Case Generator - Generates randomized and DSL-based test scenarios
"""

import random
from typing import Any, Optional

import yaml

from ..interfaces import ITestCaseGenerator
from ..models import (
    ChaosConfig,
    ChaosCoordination,
    ChaosTiming,
    ChaosType,
    ClusterConfig,
    ClusterStatusValidationConfig,
    DataConsistencyValidationConfig,
    Operation,
    OperationTiming,
    OperationType,
    ProcessChaosType,
    ReplicationValidationConfig,
    Scenario,
    SlotCoverageValidationConfig,
    StateValidationConfig,
    TargetSelection,
    TopologyType,
    TopologyValidationConfig,
    ViewConsistencyValidationConfig,
)
from ..utils.server_config import validate_server_config
from ..utils.topology_config import (
    cluster_config_from_topology_document,
    validate_topology_base_port,
)


class ScenarioGenerator(ITestCaseGenerator):
    """Generates randomized and DSL-based test scenarios"""

    def __init__(self, random_seed: Optional[int] = None):
        self.random_seed = random_seed
        if random_seed is not None:
            random.seed(random_seed)

    def generate_random_scenario(self, seed: Optional[int] = None) -> Scenario:
        """Generate a randomized test scenario with optional seed for reproducibility."""
        if seed is not None:
            random.seed(seed)
            scenario_seed = seed
        elif self.random_seed is not None:
            scenario_seed = self.random_seed
        else:
            scenario_seed = random.randint(0, 2**32 - 1)
            random.seed(scenario_seed)

        cluster_config = self._generate_random_cluster_config()
        operations = self._generate_random_operations(cluster_config)
        chaos_config = self._generate_random_chaos_config()

        scenario = Scenario(
            scenario_id=str(scenario_seed),
            cluster_config=cluster_config,
            operations=operations,
            chaos_config=chaos_config,
            seed=scenario_seed,
        )

        return scenario

    def _generate_random_cluster_config(self) -> ClusterConfig:
        """Generate random cluster configuration"""
        num_shards = random.randint(3, 16)
        # Ensure at least 1 replica per shard for failover operations
        replicas_per_shard = random.randint(1, 5)
        base_port = random.randint(7000, 8000)

        return ClusterConfig(num_shards=num_shards, replicas_per_shard=replicas_per_shard, base_port=base_port)

    def _generate_random_operations(self, cluster_config: ClusterConfig) -> list[Operation]:
        """Generate random failover operations."""
        max_safe_operations = cluster_config.replicas_per_shard * cluster_config.num_shards
        num_operations = random.randint(1, min(5, max_safe_operations))
        operations = []
        num_primaries = cluster_config.num_shards

        for _ in range(num_operations):
            target_shard = random.randint(0, num_primaries - 1)
            target_node = f"shard-{target_shard}-primary"

            timing = OperationTiming(
                delay_before=random.uniform(0, 5), timeout=random.uniform(20, 60), delay_after=random.uniform(0, 5)
            )

            operation = Operation(
                type=OperationType.FAILOVER,
                target_node=target_node,
                parameters={"force": random.choice([True, False])},
                timing=timing,
            )

            operations.append(operation)

        return operations

    def _generate_random_chaos_config(self) -> ChaosConfig:
        """Generate random chaos configuration"""
        strategy = random.choice(["random", "primary_only", "replica_only"])
        target_selection = TargetSelection(strategy=strategy)

        timing = ChaosTiming(
            delay_before_operation=random.uniform(0, 2),
            delay_after_operation=random.uniform(0, 2),
            chaos_duration=random.uniform(5, 15),
        )

        chaos_phases = [
            {"chaos_before_operation": True, "chaos_during_operation": False, "chaos_after_operation": False},
            {"chaos_before_operation": False, "chaos_during_operation": True, "chaos_after_operation": False},
            {"chaos_before_operation": False, "chaos_during_operation": False, "chaos_after_operation": True},
        ]
        selected_phase = random.choice(chaos_phases)
        coordination = ChaosCoordination(**selected_phase)

        process_chaos_type = random.choice([ProcessChaosType.SIGKILL, ProcessChaosType.SIGTERM])

        return ChaosConfig(
            chaos_type=ChaosType.PROCESS_KILL,
            target_selection=target_selection,
            timing=timing,
            coordination=coordination,
            process_chaos_type=process_chaos_type,
            randomize_per_operation=False,
        )

    def parse_dsl_config(self, dsl_text: str) -> Scenario:
        """Parse YAML DSL configuration into test scenario."""
        try:
            config = yaml.safe_load(dsl_text)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML syntax: {e}") from e

        if not isinstance(config, dict):
            raise ValueError("DSL configuration must be a mapping")
        if "scenario_id" not in config:
            raise ValueError("Missing required field: scenario_id")

        has_legacy_cluster = "cluster" in config
        has_topology = "topology" in config
        if has_legacy_cluster and has_topology:
            raise ValueError("Specify only one of 'cluster' or 'topology'")
        if not has_legacy_cluster and not has_topology:
            raise ValueError("Missing required field: cluster or topology")

        if has_legacy_cluster:
            api_version = self._parse_api_version(config.get("api_version", "v1"))
            if "operations" not in config:
                raise ValueError("Missing required field: operations")
            cluster_config = self._parse_cluster_config(config["cluster"])
            operations = self._parse_operations(config["operations"])
        else:
            api_version = self._parse_api_version(config.get("api_version", "v2"))
            if api_version != "v2":
                raise ValueError("The top-level 'topology' format requires api_version: v2")
            cluster_config = self._parse_topology_config(config["topology"])
            operations = self._parse_operations(config.get("operations", []), allow_empty=True)

        if cluster_config.topology_type != TopologyType.CLUSTER and any(
            operation.type == OperationType.FAILOVER for operation in operations
        ):
            raise ValueError("FAILOVER operations are only supported for cluster topology")

        if not operations and config.get("chaos"):
            raise ValueError("Chaos configuration requires at least one operation")

        chaos_config = self._parse_chaos_config(config.get("chaos", {}))
        if not operations:
            chaos_config.coordination = ChaosCoordination(
                chaos_before_operation=False,
                chaos_during_operation=False,
                chaos_after_operation=False,
            )
        state_validation_config = self._parse_state_validation_config(config.get("state_validation"))

        seed = config.get("seed")
        if seed is None:
            seed = random.randint(0, 2**32 - 1)

        return Scenario(
            scenario_id=config["scenario_id"],
            cluster_config=cluster_config,
            operations=operations,
            chaos_config=chaos_config,
            state_validation_config=state_validation_config,
            seed=seed,
            api_version=api_version,
        )

    @staticmethod
    def _parse_api_version(value: Any) -> str:
        if value not in {"v1", "v2"}:
            raise ValueError(f"Unsupported api_version: {value}")
        return value

    def _parse_cluster_config(self, cluster_dict: dict[str, Any]) -> ClusterConfig:
        """Parse the legacy cluster configuration from DSL."""
        if not isinstance(cluster_dict, dict):
            raise ValueError("cluster: Must be a mapping")
        if "num_shards" not in cluster_dict:
            raise ValueError("Missing required cluster field: num_shards")
        if "replicas_per_shard" not in cluster_dict:
            raise ValueError("Missing required cluster field: replicas_per_shard")

        return self._build_topology_config(
            cluster_dict,
            TopologyType.CLUSTER,
            num_shards=cluster_dict["num_shards"],
            replicas_per_shard=cluster_dict["replicas_per_shard"],
        )

    def _parse_topology_config(self, topology_dict: dict[str, Any]) -> ClusterConfig:
        """Parse the v2 topology configuration from DSL."""
        return cluster_config_from_topology_document(topology_dict)

    def _build_topology_config(
        self,
        config_dict: dict[str, Any],
        topology_type: TopologyType,
        num_shards: Any,
        replicas_per_shard: Any,
    ) -> ClusterConfig:
        self._validate_topology_dimensions(topology_type, num_shards, replicas_per_shard)
        base_port = config_dict.get("base_port", 7000)
        validate_topology_base_port(
            topology_type,
            num_shards,
            replicas_per_shard,
            base_port,
        )

        server_config = config_dict.get("server_config", {})
        if not isinstance(server_config, dict):
            raise ValueError("server_config must be a mapping")
        try:
            validate_server_config(server_config)
        except (TypeError, ValueError) as e:
            raise ValueError(f"Invalid server_config: {e}") from e

        node_configs = config_dict.get("node_configs", {})
        if not isinstance(node_configs, dict):
            raise ValueError("node_configs must be a mapping")
        valid_node_selectors = self._valid_node_config_selectors(
            topology_type,
            num_shards,
            replicas_per_shard,
        )
        for node_id, node_config in node_configs.items():
            if not isinstance(node_id, str):
                raise ValueError("node_configs keys must be node ID strings")
            if not isinstance(node_config, dict):
                raise ValueError(f"node_configs.{node_id} must be a mapping")
            if node_id not in valid_node_selectors:
                raise ValueError(f"Unknown node_configs selector '{node_id}' for {topology_type.value} topology")
            try:
                validate_server_config(node_config)
            except (TypeError, ValueError) as e:
                raise ValueError(f"Invalid node_configs.{node_id}: {e}") from e

        kwargs = {
            "num_shards": num_shards,
            "replicas_per_shard": replicas_per_shard,
            "base_port": base_port,
            "topology_type": topology_type,
            "server_config": dict(server_config),
            "node_configs": {node_id: dict(node_config) for node_id, node_config in node_configs.items()},
        }
        if "base_data_dir" in config_dict:
            kwargs["base_data_dir"] = config_dict["base_data_dir"]
        if "valkey_binary" in config_dict:
            kwargs["valkey_binary"] = config_dict["valkey_binary"]
        if "enable_cleanup" in config_dict:
            kwargs["enable_cleanup"] = config_dict["enable_cleanup"]

        return ClusterConfig(**kwargs)

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
    def _validate_topology_dimensions(topology_type: TopologyType, num_shards: Any, replicas_per_shard: Any) -> None:
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
        else:
            if num_shards != 1:
                raise ValueError(f"standalone topology requires num_shards to be 1, got {num_shards}")
            if replicas_per_shard != 0:
                raise ValueError(f"standalone topology requires replicas_per_shard to be 0, got {replicas_per_shard}")

    def _parse_operations(self, operations_list: list[dict[str, Any]], allow_empty: bool = False) -> list[Operation]:
        """Parse operations from DSL"""
        if not isinstance(operations_list, list):
            raise ValueError("operations must be a list")
        if not operations_list and not allow_empty:
            raise ValueError("At least one operation is required")

        operations = []
        for i, op_dict in enumerate(operations_list):
            if "type" not in op_dict:
                raise ValueError(f"Operation {i}: missing required field 'type'")
            if "target_node" not in op_dict:
                raise ValueError(f"Operation {i}: missing required field 'target_node'")

            try:
                op_type = OperationType(op_dict["type"])
            except ValueError as e:
                raise ValueError(f"Operation {i}: invalid operation type '{op_dict['type']}'") from e
            timing_dict = op_dict.get("timing", {})
            timing = OperationTiming(
                delay_before=timing_dict.get("delay_before", 0.0),
                timeout=timing_dict.get("timeout", 30.0),
                delay_after=timing_dict.get("delay_after", 0.0),
            )

            operation = Operation(
                type=op_type,
                target_node=op_dict["target_node"],
                parameters=op_dict.get("parameters", {}),
                timing=timing,
            )

            operations.append(operation)

        return operations

    def _parse_chaos_config(self, chaos_dict: dict[str, Any]) -> ChaosConfig:
        """Parse chaos configuration from DSL"""
        if not chaos_dict:
            return ChaosConfig(
                chaos_type=ChaosType.PROCESS_KILL,
                target_selection=TargetSelection(strategy="random"),
                timing=ChaosTiming(),
                coordination=ChaosCoordination(),
                process_chaos_type=ProcessChaosType.SIGKILL,
            )

        chaos_type_str = chaos_dict.get("type", "process_kill")
        try:
            chaos_type = ChaosType(chaos_type_str)
        except ValueError as e:
            raise ValueError(f"Invalid chaos type: {chaos_type_str}") from e

        target_dict = chaos_dict.get("target_selection", {})
        target_selection = TargetSelection(
            strategy=target_dict.get("strategy", "random"), specific_nodes=target_dict.get("specific_nodes")
        )

        timing_dict = chaos_dict.get("timing", {})
        timing = ChaosTiming(
            delay_before_operation=timing_dict.get("delay_before_operation", 0.0),
            delay_after_operation=timing_dict.get("delay_after_operation", 0.0),
            chaos_duration=timing_dict.get("chaos_duration", 10.0),
        )

        coord_dict = chaos_dict.get("coordination", {})
        coordination = ChaosCoordination(
            chaos_before_operation=coord_dict.get("chaos_before_operation", False),
            chaos_during_operation=coord_dict.get("chaos_during_operation", True),
            chaos_after_operation=coord_dict.get("chaos_after_operation", False),
        )

        process_chaos_type = None
        if chaos_type == ChaosType.PROCESS_KILL:
            pct_str = chaos_dict.get("process_chaos_type", "sigkill")
            # Allow None for randomization (will be set at runtime)
            if pct_str is None:
                process_chaos_type = None
            else:
                try:
                    process_chaos_type = ProcessChaosType(pct_str)
                except ValueError as e:
                    raise ValueError(f"Invalid process chaos type: {pct_str}") from e

        randomize_per_operation = chaos_dict.get("randomize_per_operation", False)

        return ChaosConfig(
            chaos_type=chaos_type,
            target_selection=target_selection,
            timing=timing,
            coordination=coordination,
            process_chaos_type=process_chaos_type,
            randomize_per_operation=randomize_per_operation,
        )

    def _parse_state_validation_config(
        self, validation_dict: Optional[dict[str, Any]]
    ) -> Optional[StateValidationConfig]:
        """Parse state validation configuration from DSL"""
        if not validation_dict:
            return None

        # Validate numeric fields
        if "stabilization_wait" in validation_dict:
            value = validation_dict["stabilization_wait"]
            if not isinstance(value, (int, float)) or value < 0:
                raise ValueError("state_validation.stabilization_wait: Must be a non-negative number")

        if "validation_timeout" in validation_dict:
            value = validation_dict["validation_timeout"]
            if not isinstance(value, (int, float)) or value <= 0:
                raise ValueError("state_validation.validation_timeout: Must be a positive number")

        if "retry_delay" in validation_dict:
            value = validation_dict["retry_delay"]
            if not isinstance(value, (int, float)) or value < 0:
                raise ValueError("state_validation.retry_delay: Must be a non-negative number")

        # Parse replication config if present
        replication_config = None
        if validation_dict.get("replication_config"):
            rep_dict = validation_dict["replication_config"]
            replication_config = ReplicationValidationConfig(
                max_acceptable_lag=rep_dict.get("max_acceptable_lag", 5.0),
                require_all_replicas_synced=rep_dict.get("require_all_replicas_synced", False),
                check_replication_offset=rep_dict.get("check_replication_offset", True),
                min_replicas_per_shard=rep_dict.get("min_replicas_per_shard", 1),
                timeout=rep_dict.get("timeout", 10.0),
            )

        # Parse cluster status config if present
        cluster_status_config = None
        if validation_dict.get("cluster_status_config"):
            cs_dict = validation_dict["cluster_status_config"]
            cluster_status_config = ClusterStatusValidationConfig(
                acceptable_states=cs_dict.get("acceptable_states"),
                allow_degraded=cs_dict.get("allow_degraded", False),
                require_quorum=cs_dict.get("require_quorum", True),
                timeout=cs_dict.get("timeout", 10.0),
            )

        # Parse slot coverage config if present
        slot_coverage_config = None
        if validation_dict.get("slot_coverage_config"):
            sc_dict = validation_dict["slot_coverage_config"]
            slot_coverage_config = SlotCoverageValidationConfig(
                require_full_coverage=sc_dict.get("require_full_coverage", True),
                allow_slot_conflicts=sc_dict.get("allow_slot_conflicts", False),
                timeout=sc_dict.get("timeout", 10.0),
            )

        # Parse topology config if present
        topology_config = None
        if validation_dict.get("topology_config"):
            topo_dict = validation_dict["topology_config"]
            topology_config = TopologyValidationConfig(
                strict_mode=topo_dict.get("strict_mode", True),
                allow_failed_nodes=topo_dict.get("allow_failed_nodes", True),
                timeout=topo_dict.get("timeout", 10.0),
            )

        # Parse view consistency config if present
        view_consistency_config = None
        if validation_dict.get("view_consistency_config"):
            vc_dict = validation_dict["view_consistency_config"]
            view_consistency_config = ViewConsistencyValidationConfig(
                require_full_consensus=vc_dict.get("require_full_consensus", True),
                allow_transient_inconsistency=vc_dict.get("allow_transient_inconsistency", True),
                max_inconsistency_duration=vc_dict.get("max_inconsistency_duration", 5.0),
                timeout=vc_dict.get("timeout", 15.0),
            )

        # Parse data consistency config if present
        data_consistency_config = None
        if validation_dict.get("data_consistency_config"):
            dc_dict = validation_dict["data_consistency_config"]
            data_consistency_config = DataConsistencyValidationConfig(
                check_test_keys=dc_dict.get("check_test_keys", True),
                check_cross_replica_consistency=dc_dict.get("check_cross_replica_consistency", True),
                num_test_keys=dc_dict.get("num_test_keys", 100),
                key_prefix=dc_dict.get("key_prefix", "fuzzer:test:"),
                timeout=dc_dict.get("timeout", 10.0),
            )

        return StateValidationConfig(
            check_replication=validation_dict.get("check_replication", True),
            check_cluster_status=validation_dict.get("check_cluster_status", True),
            check_slot_coverage=validation_dict.get("check_slot_coverage", True),
            check_topology=validation_dict.get("check_topology", True),
            check_view_consistency=validation_dict.get("check_view_consistency", True),
            check_data_consistency=validation_dict.get("check_data_consistency", True),
            check_logs=validation_dict.get("check_logs", True),
            stabilization_wait=validation_dict.get("stabilization_wait", 5.0),
            validation_timeout=validation_dict.get("validation_timeout", 30.0),
            blocking_on_failure=validation_dict.get("blocking_on_failure", False),
            retry_on_transient_failure=validation_dict.get("retry_on_transient_failure", True),
            max_retries=validation_dict.get("max_retries", 3),
            retry_delay=validation_dict.get("retry_delay", 2.0),
            replication_config=replication_config,
            cluster_status_config=cluster_status_config,
            slot_coverage_config=slot_coverage_config,
            topology_config=topology_config,
            view_consistency_config=view_consistency_config,
            data_consistency_config=data_consistency_config,
        )

    def validate_scenario(self, scenario: Scenario) -> bool:
        """Validate test scenario configuration, raises ValueError if invalid."""
        if scenario.api_version not in {"v1", "v2"}:
            raise ValueError(f"Unsupported api_version: {scenario.api_version}")

        topology_type = scenario.cluster_config.topology_type
        if isinstance(topology_type, str):
            try:
                topology_type = TopologyType(topology_type)
            except ValueError as e:
                raise ValueError(f"Invalid topology type: {topology_type}") from e

        try:
            self._validate_topology_dimensions(
                topology_type,
                scenario.cluster_config.num_shards,
                scenario.cluster_config.replicas_per_shard,
            )
        except ValueError as e:
            if topology_type == TopologyType.CLUSTER and str(e).startswith("num_shards"):
                raise ValueError(f"Invalid num_shards: {scenario.cluster_config.num_shards}") from e
            if topology_type == TopologyType.CLUSTER and str(e).startswith("replicas_per_shard"):
                raise ValueError(f"Invalid replicas_per_shard: {scenario.cluster_config.replicas_per_shard}") from e
            raise

        validate_topology_base_port(
            topology_type,
            scenario.cluster_config.num_shards,
            scenario.cluster_config.replicas_per_shard,
            scenario.cluster_config.base_port,
        )

        operations_optional = scenario.api_version == "v2"
        if not scenario.operations and not operations_optional:
            raise ValueError("Scenario must have at least one operation")

        # Check if any failover operations exist
        has_failover = any(op.type == OperationType.FAILOVER for op in scenario.operations)

        if has_failover and topology_type != TopologyType.CLUSTER:
            raise ValueError("FAILOVER operations are only supported for cluster topology")

        # Validate that failover operations have replicas available
        if has_failover and scenario.cluster_config.replicas_per_shard == 0:
            raise ValueError(
                "Failover operations require at least 1 replica per shard. "
                "Set replicas_per_shard >= 1 or remove failover operations."
            )

        for i, operation in enumerate(scenario.operations):
            if not operation.target_node:
                raise ValueError(f"Operation {i}: target_node cannot be empty")
            if operation.timing.timeout <= 0:
                raise ValueError(f"Operation {i}: timeout must be positive")

        if scenario.chaos_config.chaos_type == ChaosType.PROCESS_KILL:
            if scenario.chaos_config.process_chaos_type is None:
                # Allow None if randomization is enabled (will be randomized at runtime)
                if not scenario.chaos_config.randomize_per_operation:
                    raise ValueError(
                        "process_chaos_type required for PROCESS_KILL chaos (or enable randomize_per_operation)"
                    )

        return True
