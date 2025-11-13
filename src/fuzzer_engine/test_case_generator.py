"""
Test Case Generator - Generates randomized and DSL-based test scenarios
"""
import random
import yaml
from typing import Optional, List, Dict, Any
from ..models import (
    Scenario, ClusterConfig, Operation, OperationType, OperationTiming,
    ChaosConfig, ChaosType, ProcessChaosType, TargetSelection, ChaosTiming,
    ChaosCoordination, ValidationConfig
)
from ..interfaces import ITestCaseGenerator


class ScenarioGenerator(ITestCaseGenerator):
    """Generates randomized and DSL-based test scenarios"""
    
    def __init__(self, random_seed: Optional[int] = None):
        """
        Initialize test case generator
        """
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
        validation_config = ValidationConfig()
        
        scenario = Scenario(
            scenario_id=str(scenario_seed),
            cluster_config=cluster_config,
            operations=operations,
            chaos_config=chaos_config,
            validation_config=validation_config,
            seed=scenario_seed
        )
        
        return scenario
    
    def _generate_random_cluster_config(self) -> ClusterConfig:
        """Generate random cluster configuration"""
        num_shards = random.randint(3, 16)
        # Ensure at least 1 replica per shard for failover operations
        replicas_per_shard = random.randint(1, 2)
        base_port = random.randint(7000, 8000)
        
        return ClusterConfig(
            num_shards=num_shards,
            replicas_per_shard=replicas_per_shard,
            base_port=base_port
        )
    
    def _generate_random_operations(self, cluster_config: ClusterConfig) -> List[Operation]:
        """Generate random failover operations"""
        num_operations = random.randint(1, 5)
        operations = []
        num_primaries = cluster_config.num_shards
        
        for i in range(num_operations):
            target_shard = random.randint(0, num_primaries - 1)
            target_node = f"shard-{target_shard}-primary"
            
            timing = OperationTiming(
                delay_before=random.uniform(0, 5),
                timeout=random.uniform(20, 60),
                delay_after=random.uniform(0, 5)
            )
            
            operation = Operation(
                type=OperationType.FAILOVER,
                target_node=target_node,
                parameters={"force": random.choice([True, False])},
                timing=timing
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
            chaos_duration=random.uniform(5, 15)
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
            randomize_per_operation=True
        )
    
    def parse_dsl_config(self, dsl_text: str) -> Scenario:
        """Parse YAML DSL configuration into test scenario."""
        try:
            config = yaml.safe_load(dsl_text)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML syntax: {e}")
        
        if "scenario_id" not in config:
            raise ValueError("Missing required field: scenario_id")
        if "cluster" not in config:
            raise ValueError("Missing required field: cluster")
        if "operations" not in config:
            raise ValueError("Missing required field: operations")
        
        cluster_config = self._parse_cluster_config(config["cluster"])
        operations = self._parse_operations(config["operations"])
        chaos_config = self._parse_chaos_config(config.get("chaos", {}))
        validation_config = self._parse_validation_config(config.get("validation", {}))
        seed = config.get("seed")
        
        return Scenario(
            scenario_id=config["scenario_id"],
            cluster_config=cluster_config,
            operations=operations,
            chaos_config=chaos_config,
            validation_config=validation_config,
            seed=seed
        )
    
    def _parse_cluster_config(self, cluster_dict: Dict[str, Any]) -> ClusterConfig:
        """Parse cluster configuration from DSL"""
        if "num_shards" not in cluster_dict:
            raise ValueError("Missing required cluster field: num_shards")
        if "replicas_per_shard" not in cluster_dict:
            raise ValueError("Missing required cluster field: replicas_per_shard")
        
        num_shards = cluster_dict["num_shards"]
        replicas_per_shard = cluster_dict["replicas_per_shard"]
        
        if not (3 <= num_shards <= 16):
            raise ValueError(f"num_shards must be between 3 and 16, got {num_shards}")
        if not (0 <= replicas_per_shard <= 2):
            raise ValueError(f"replicas_per_shard must be between 0 and 2, got {replicas_per_shard}")
        
        return ClusterConfig(
            num_shards=num_shards,
            replicas_per_shard=replicas_per_shard,
            base_port=cluster_dict.get("base_port", 6379),
            base_data_dir=cluster_dict.get("base_data_dir", "/tmp/valkey-fuzzer"),
            valkey_binary=cluster_dict.get("valkey_binary", "/usr/local/bin/valkey-server"),
            enable_cleanup=cluster_dict.get("enable_cleanup", True)
        )
    
    def _parse_operations(self, operations_list: List[Dict[str, Any]]) -> List[Operation]:
        """Parse operations from DSL"""
        if not operations_list:
            raise ValueError("At least one operation is required")
        
        operations = []
        for i, op_dict in enumerate(operations_list):
            if "type" not in op_dict:
                raise ValueError(f"Operation {i}: missing required field 'type'")
            if "target_node" not in op_dict:
                raise ValueError(f"Operation {i}: missing required field 'target_node'")
            
            try:
                op_type = OperationType(op_dict["type"])
            except ValueError:
                raise ValueError(f"Operation {i}: invalid operation type '{op_dict['type']}'")
            
            timing_dict = op_dict.get("timing", {})
            timing = OperationTiming(
                delay_before=timing_dict.get("delay_before", 0.0),
                timeout=timing_dict.get("timeout", 30.0),
                delay_after=timing_dict.get("delay_after", 0.0)
            )
            
            operation = Operation(
                type=op_type,
                target_node=op_dict["target_node"],
                parameters=op_dict.get("parameters", {}),
                timing=timing
            )
            
            operations.append(operation)
        
        return operations
    
    def _parse_chaos_config(self, chaos_dict: Dict[str, Any]) -> ChaosConfig:
        """Parse chaos configuration from DSL"""
        if not chaos_dict:
            return ChaosConfig(
                chaos_type=ChaosType.PROCESS_KILL,
                target_selection=TargetSelection(strategy="random"),
                timing=ChaosTiming(),
                coordination=ChaosCoordination(),
                process_chaos_type=ProcessChaosType.SIGKILL
            )
        
        chaos_type_str = chaos_dict.get("type", "process_kill")
        try:
            chaos_type = ChaosType(chaos_type_str)
        except ValueError:
            raise ValueError(f"Invalid chaos type: {chaos_type_str}")
        
        target_dict = chaos_dict.get("target_selection", {})
        target_selection = TargetSelection(
            strategy=target_dict.get("strategy", "random"),
            specific_nodes=target_dict.get("specific_nodes")
        )
        
        timing_dict = chaos_dict.get("timing", {})
        timing = ChaosTiming(
            delay_before_operation=timing_dict.get("delay_before_operation", 0.0),
            delay_after_operation=timing_dict.get("delay_after_operation", 0.0),
            chaos_duration=timing_dict.get("chaos_duration", 10.0)
        )
        
        coord_dict = chaos_dict.get("coordination", {})
        coordination = ChaosCoordination(
            chaos_before_operation=coord_dict.get("chaos_before_operation", False),
            chaos_during_operation=coord_dict.get("chaos_during_operation", True),
            chaos_after_operation=coord_dict.get("chaos_after_operation", False)
        )
        
        process_chaos_type = None
        if chaos_type == ChaosType.PROCESS_KILL:
            pct_str = chaos_dict.get("process_chaos_type", "sigkill")
            if pct_str is not None:
                try:
                    process_chaos_type = ProcessChaosType(pct_str)
                except ValueError:
                    raise ValueError(f"Invalid process chaos type: {pct_str}")
        
        randomize_per_operation = chaos_dict.get("randomize_per_operation", False)
        
        return ChaosConfig(
            chaos_type=chaos_type,
            target_selection=target_selection,
            timing=timing,
            coordination=coordination,
            process_chaos_type=process_chaos_type,
            randomize_per_operation=randomize_per_operation
        )
    
    def _parse_validation_config(self, validation_dict: Dict[str, Any]) -> ValidationConfig:
        """Parse validation configuration from DSL"""
        return ValidationConfig(
            check_slot_coverage=validation_dict.get("check_slot_coverage", True),
            check_slot_conflicts=validation_dict.get("check_slot_conflicts", True),
            check_replica_sync=validation_dict.get("check_replica_sync", True),
            check_node_connectivity=validation_dict.get("check_node_connectivity", True),
            check_data_consistency=validation_dict.get("check_data_consistency", True),
            convergence_timeout=validation_dict.get("convergence_timeout", 60.0),
            max_replication_lag=validation_dict.get("max_replication_lag", 5.0)
        )
    
    def validate_scenario(self, scenario: Scenario) -> bool:
        """Validate test scenario configuration, raises ValueError if invalid."""
        if not (3 <= scenario.cluster_config.num_shards <= 16):
            raise ValueError(f"Invalid num_shards: {scenario.cluster_config.num_shards}")
        if not (0 <= scenario.cluster_config.replicas_per_shard <= 2):
            raise ValueError(f"Invalid replicas_per_shard: {scenario.cluster_config.replicas_per_shard}")
        
        if not scenario.operations:
            raise ValueError("Scenario must have at least one operation")
        
        # Check if any failover operations exist
        has_failover = any(op.type == OperationType.FAILOVER for op in scenario.operations)
        
        # Validate that failover operations have replicas available
        if has_failover and scenario.cluster_config.replicas_per_shard == 0:
            raise ValueError("Failover operations require at least 1 replica per shard. "
                           "Set replicas_per_shard >= 1 or remove failover operations.")
        
        for i, operation in enumerate(scenario.operations):
            if not operation.target_node:
                raise ValueError(f"Operation {i}: target_node cannot be empty")
            if operation.timing.timeout <= 0:
                raise ValueError(f"Operation {i}: timeout must be positive")
        
        if scenario.chaos_config.chaos_type == ChaosType.PROCESS_KILL:
            if scenario.chaos_config.process_chaos_type is None:
                # Allow None if randomization is enabled (will be randomized at runtime)
                if not scenario.chaos_config.randomize_per_operation:
                    raise ValueError("process_chaos_type required for PROCESS_KILL chaos (or enable randomize_per_operation)")
        
        return True
