"""
DSL Utilities - Helper functions for DSL configuration handling
"""
import yaml
from pathlib import Path
from typing import Union
from ..models import DSLConfig, Scenario


class DSLLoader:
    """Utility class for loading and validating DSL configurations"""
    
    @staticmethod
    def load_from_file(file_path: Union[str, Path]) -> DSLConfig:
        """Load DSL configuration from a YAML file."""
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"DSL file not found: {file_path}")
        
        try:
            with open(file_path, 'r') as f:
                config_text = f.read()
            
            yaml.safe_load(config_text)
            return DSLConfig(config_text=config_text)
            
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML syntax in {file_path}: {e}")
        except Exception as e:
            raise ValueError(f"Error loading DSL file {file_path}: {e}")
    
    @staticmethod
    def load_from_string(config_text: str) -> DSLConfig:
        """Load DSL configuration from a YAML string."""
        try:
            yaml.safe_load(config_text)
            return DSLConfig(config_text=config_text)
            
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML syntax: {e}")
    
    @staticmethod
    def validate_dsl_config(dsl_config: DSLConfig, scenario_generator) -> bool:
        """Validate a DSL configuration by parsing and validating the scenario."""
        scenario = scenario_generator.parse_dsl_config(dsl_config.config_text)
        scenario_generator.validate_scenario(scenario)
        return True
    
    @staticmethod
    def save_scenario_as_dsl(scenario: Scenario, file_path: Union[str, Path]) -> None:
        """Save a scenario as a DSL YAML file for reproducibility."""
        file_path = Path(file_path)
        
        dsl_dict = {
            'scenario_id': scenario.scenario_id,
            'seed': scenario.seed,
            'cluster': {
                'num_shards': scenario.cluster_config.num_shards,
                'replicas_per_shard': scenario.cluster_config.replicas_per_shard,
                'base_port': scenario.cluster_config.base_port,
                'base_data_dir': scenario.cluster_config.base_data_dir,
                'valkey_binary': scenario.cluster_config.valkey_binary,
                'enable_cleanup': scenario.cluster_config.enable_cleanup
            },
            'operations': [
                {
                    'type': op.type.value,
                    'target_node': op.target_node,
                    'parameters': op.parameters,
                    'timing': {
                        'delay_before': op.timing.delay_before,
                        'timeout': op.timing.timeout,
                        'delay_after': op.timing.delay_after
                    }
                }
                for op in scenario.operations
            ],
            'chaos': {
                'type': scenario.chaos_config.chaos_type.value,
                'process_chaos_type': scenario.chaos_config.process_chaos_type.value if scenario.chaos_config.process_chaos_type else None,
                'target_selection': {
                    'strategy': scenario.chaos_config.target_selection.strategy,
                    'specific_nodes': scenario.chaos_config.target_selection.specific_nodes
                },
                'timing': {
                    'delay_before_operation': scenario.chaos_config.timing.delay_before_operation,
                    'delay_after_operation': scenario.chaos_config.timing.delay_after_operation,
                    'chaos_duration': scenario.chaos_config.timing.chaos_duration
                },
                'coordination': {
                    'chaos_before_operation': scenario.chaos_config.coordination.chaos_before_operation,
                    'chaos_during_operation': scenario.chaos_config.coordination.chaos_during_operation,
                    'chaos_after_operation': scenario.chaos_config.coordination.chaos_after_operation
                }
            },
            'validation': {
                'check_slot_coverage': scenario.validation_config.check_slot_coverage,
                'check_slot_conflicts': scenario.validation_config.check_slot_conflicts,
                'check_replica_sync': scenario.validation_config.check_replica_sync,
                'check_node_connectivity': scenario.validation_config.check_node_connectivity,
                'check_data_consistency': scenario.validation_config.check_data_consistency,
                'convergence_timeout': scenario.validation_config.convergence_timeout,
                'max_replication_lag': scenario.validation_config.max_replication_lag
            }
        }
        
        with open(file_path, 'w') as f:
            yaml.dump(dsl_dict, f, default_flow_style=False, sort_keys=False)


class DSLValidator:
    """Validator for DSL configurations with detailed error reporting"""
    
    @staticmethod
    def validate_structure(config_dict: dict) -> list:
        """
        Validate the structure of a DSL configuration dictionary.
        """
        errors = []
        
        # Check required top-level fields
        required_fields = ['scenario_id', 'cluster', 'operations']
        for field in required_fields:
            if field not in config_dict:
                errors.append(f"Missing required field: {field}")
        
        # Validate cluster configuration
        if 'cluster' in config_dict:
            cluster_errors = DSLValidator._validate_cluster(config_dict['cluster'])
            errors.extend(cluster_errors)
        
        # Validate operations
        if 'operations' in config_dict:
            operations_errors = DSLValidator._validate_operations(config_dict['operations'])
            errors.extend(operations_errors)
        
        # Validate chaos configuration (optional)
        if 'chaos' in config_dict:
            chaos_errors = DSLValidator._validate_chaos(config_dict['chaos'])
            errors.extend(chaos_errors)
        
        # Validate validation configuration (optional)
        if 'validation' in config_dict:
            validation_errors = DSLValidator._validate_validation(config_dict['validation'])
            errors.extend(validation_errors)
        
        return errors
    
    @staticmethod
    def _validate_cluster(cluster_dict: dict) -> list:
        """Validate cluster configuration"""
        errors = []
        
        required_fields = ['num_shards', 'replicas_per_shard']
        for field in required_fields:
            if field not in cluster_dict:
                errors.append(f"cluster: Missing required field '{field}'")
        
        # Validate ranges
        if 'num_shards' in cluster_dict:
            num_shards = cluster_dict['num_shards']
            if not isinstance(num_shards, int) or not (3 <= num_shards <= 16):
                errors.append(f"cluster.num_shards: Must be an integer between 3 and 16, got {num_shards}")
        
        if 'replicas_per_shard' in cluster_dict:
            replicas = cluster_dict['replicas_per_shard']
            if not isinstance(replicas, int) or not (0 <= replicas <= 2):
                errors.append(f"cluster.replicas_per_shard: Must be an integer between 0 and 2, got {replicas}")
        
        return errors
    
    @staticmethod
    def _validate_operations(operations_list: list) -> list:
        """Validate operations list"""
        errors = []
        
        if not isinstance(operations_list, list):
            errors.append("operations: Must be a list")
            return errors
        
        if len(operations_list) == 0:
            errors.append("operations: Must contain at least one operation")
        
        for i, op in enumerate(operations_list):
            if not isinstance(op, dict):
                errors.append(f"operations[{i}]: Must be a dictionary")
                continue
            
            # Check required fields
            if 'type' not in op:
                errors.append(f"operations[{i}]: Missing required field 'type'")
            elif op['type'] not in ['failover']:  # Add more types as they're implemented
                errors.append(f"operations[{i}]: Invalid operation type '{op['type']}'")
            
            if 'target_node' not in op:
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
        if 'type' in chaos_dict:
            if chaos_dict['type'] not in ['process_kill']:  # Add more types as implemented
                errors.append(f"chaos.type: Invalid chaos type '{chaos_dict['type']}'")
        
        # Validate target selection
        if 'target_selection' in chaos_dict:
            target_sel = chaos_dict['target_selection']
            if 'strategy' in target_sel:
                valid_strategies = ['random', 'primary_only', 'replica_only', 'specific']
                if target_sel['strategy'] not in valid_strategies:
                    errors.append(f"chaos.target_selection.strategy: Must be one of {valid_strategies}")
        
        return errors
    
    @staticmethod
    def _validate_validation(validation_dict: dict) -> list:
        """Validate validation configuration"""
        errors = []
        
        if not isinstance(validation_dict, dict):
            errors.append("validation: Must be a dictionary")
            return errors
        
        # All fields are optional booleans or floats, just check types if present
        bool_fields = [
            'check_slot_coverage', 'check_slot_conflicts', 'check_replica_sync',
            'check_node_connectivity', 'check_data_consistency'
        ]
        
        for field in bool_fields:
            if field in validation_dict and not isinstance(validation_dict[field], bool):
                errors.append(f"validation.{field}: Must be a boolean")
        
        float_fields = ['convergence_timeout', 'max_replication_lag']
        for field in float_fields:
            if field in validation_dict:
                value = validation_dict[field]
                if not isinstance(value, (int, float)) or value <= 0:
                    errors.append(f"validation.{field}: Must be a positive number")
        
        return errors
