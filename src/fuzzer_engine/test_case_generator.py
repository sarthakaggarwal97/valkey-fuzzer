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
        
        Args:
            random_seed: Optional seed for reproducible random generation
        """
        self.random_seed = random_seed
        if random_seed is not None:
            random.seed(random_seed)
    
    def generate_random_scenario(self, seed: Optional[int] = None) -> Scenario:
        """
        Generate a randomized test scenario
        
        Args:
            seed: Optional seed for reproducibility
            
        Returns:
            Scenario: Generated test scenario
        """
        # Use provided seed or generate new one
        if seed is not None:
            random.seed(seed)
            scenario_seed = seed
        elif self.random_seed is not None:
            scenario_seed = self.random_seed
        else:
            scenario_seed = random.randint(0, 2**32 - 1)
            random.seed(scenario_seed)
        
        # Generate random cluster configuration
        cluster_config = self._generate_random_cluster_config()
        
        # Generate random operations (focus on failover for prototype)
        operations = self._generate_random_operations(cluster_config)
        
        # Generate random chaos configuration
        chaos_config = self._generate_random_chaos_config()
        
        # Create validation configuration
        validation_config = ValidationConfig()
        
        # Create scenario
        scenario = Scenario(
            scenario_id=f"random-{scenario_seed}",
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
        replicas_per_shard = random.randint(0, 2)
        base_port = random.randint(7000, 8000)
        
        return ClusterConfig(
            num_shards=num_shards,
            replicas_per_shard=replicas_per_shard,
            base_port=base_port
        )
    
    def _generate_random_operations(self, cluster_config: ClusterConfig) -> List[Operation]:
        """Generate random failover operations"""
        # Generate 1-5 operations
        num_operations = random.randint(1, 5)
        operations = []
        
        # Calculate total number of primary nodes
        num_primaries = cluster_config.num_shards
        
        for i in range(num_operations):
            # Select random primary node as target
            target_shard = random.randint(0, num_primaries - 1)
            target_node = f"shard-{target_shard}-primary"
            
            # Generate random timing
            timing = OperationTiming(
                delay_before=random.uniform(0, 5),
                timeout=random.uniform(20, 60),
                delay_after=random.uniform(0, 5)
            )
            
            # Create failover operation
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
        # Random target selection strategy
        strategy = random.choice(["random", "primary_only", "replica_only"])
        target_selection = TargetSelection(strategy=strategy)
        
        # Random timing
        timing = ChaosTiming(
            delay_before_operation=random.uniform(0, 2),
            delay_after_operation=random.uniform(0, 2),
            chaos_duration=random.uniform(5, 15)
        )
        
        # Random coordination
        chaos_phases = [
            {"chaos_before_operation": True, "chaos_during_operation": False, "chaos_after_operation": False},
            {"chaos_before_operation": False, "chaos_during_operation": True, "chaos_after_operation": False},
            {"chaos_before_operation": False, "chaos_during_operation": False, "chaos_after_operation": True},
        ]
        selected_phase = random.choice(chaos_phases)
        coordination = ChaosCoordination(**selected_phase)
        
        # Random process chaos type
        process_chaos_type = random.choice([ProcessChaosType.SIGKILL, ProcessChaosType.SIGTERM])
        
        return ChaosConfig(
            chaos_type=ChaosType.PROCESS_KILL,
            target_selection=target_selection,
            timing=timing,
            coordination=coordination,
            process_chaos_type=process_chaos_type
        )
    
    def parse_dsl_config(self, dsl_text: str) -> Scenario:
        """
        Parse DSL configuration into test scenario
        
        Args:
            dsl_text: YAML-formatted DSL configuration
            
        Returns:
            Scenario: Parsed test scenario
            
        Raises:
            ValueError: If DSL configuration is invalid
        """
        try:
            config = yaml.safe_load(dsl_text)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML syntax: {e}")
        
        # Validate required fields
        if "scenario_id" not in config:
            raise ValueError("Missing required field: scenario_id")
        if "cluster" not in config:
            raise ValueError("Missing required field: cluster")
        if "operations" not in config:
            raise ValueError("Missing required field: operations")
        
        # Parse cluster configuration
        cluster_config = self._parse_cluster_config(config["cluster"])
        
        # Parse operations
        operations = self._parse_operations(config["operations"])
        
        # Parse chaos configuration (optional)
        chaos_config = self._parse_chaos_config(config.get("chaos", {}))
        
        # Parse validation configuration (optional)
        validation_config = self._parse_validation_config(config.get("validation", {}))
        
        # Get seed if provided
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
        
        # Validate ranges
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
            
            # Parse operation type
            try:
                op_type = OperationType(op_dict["type"])
            except ValueError:
                raise ValueError(f"Operation {i}: invalid operation type '{op_dict['type']}'")
            
            # Parse timing
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
        # Default chaos configuration if not provided
        if not chaos_dict:
            return ChaosConfig(
                chaos_type=ChaosType.PROCESS_KILL,
                target_selection=TargetSelection(strategy="random"),
                timing=ChaosTiming(),
                coordination=ChaosCoordination(),
                process_chaos_type=ProcessChaosType.SIGKILL
            )
        
        # Parse chaos type
        chaos_type_str = chaos_dict.get("type", "process_kill")
        try:
            chaos_type = ChaosType(chaos_type_str)
        except ValueError:
            raise ValueError(f"Invalid chaos type: {chaos_type_str}")
        
        # Parse target selection
        target_dict = chaos_dict.get("target_selection", {})
        target_selection = TargetSelection(
            strategy=target_dict.get("strategy", "random"),
            specific_nodes=target_dict.get("specific_nodes")
        )
        
        # Parse timing
        timing_dict = chaos_dict.get("timing", {})
        timing = ChaosTiming(
            delay_before_operation=timing_dict.get("delay_before_operation", 0.0),
            delay_after_operation=timing_dict.get("delay_after_operation", 0.0),
            chaos_duration=timing_dict.get("chaos_duration", 10.0)
        )
        
        # Parse coordination
        coord_dict = chaos_dict.get("coordination", {})
        coordination = ChaosCoordination(
            chaos_before_operation=coord_dict.get("chaos_before_operation", False),
            chaos_during_operation=coord_dict.get("chaos_during_operation", True),
            chaos_after_operation=coord_dict.get("chaos_after_operation", False)
        )
        
        # Parse process chaos type
        process_chaos_type = None
        if chaos_type == ChaosType.PROCESS_KILL:
            pct_str = chaos_dict.get("process_chaos_type", "sigkill")
            try:
                process_chaos_type = ProcessChaosType(pct_str)
            except ValueError:
                raise ValueError(f"Invalid process chaos type: {pct_str}")
        
        return ChaosConfig(
            chaos_type=chaos_type,
            target_selection=target_selection,
            timing=timing,
            coordination=coordination,
            process_chaos_type=process_chaos_type
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
        """
        Validate test scenario configuration
        
        Args:
            scenario: Test scenario to validate
            
        Returns:
            bool: True if scenario is valid
            
        Raises:
            ValueError: If scenario is invalid
        """
        # Validate cluster configuration
        if not (3 <= scenario.cluster_config.num_shards <= 16):
            raise ValueError(f"Invalid num_shards: {scenario.cluster_config.num_shards}")
        if not (0 <= scenario.cluster_config.replicas_per_shard <= 2):
            raise ValueError(f"Invalid replicas_per_shard: {scenario.cluster_config.replicas_per_shard}")
        
        # Validate operations
        if not scenario.operations:
            raise ValueError("Scenario must have at least one operation")
        
        for i, operation in enumerate(scenario.operations):
            if not operation.target_node:
                raise ValueError(f"Operation {i}: target_node cannot be empty")
            if operation.timing.timeout <= 0:
                raise ValueError(f"Operation {i}: timeout must be positive")
        
        # Validate chaos configuration
        if scenario.chaos_config.chaos_type == ChaosType.PROCESS_KILL:
            if scenario.chaos_config.process_chaos_type is None:
                raise ValueError("process_chaos_type required for PROCESS_KILL chaos")
        
        return True
