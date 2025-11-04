"""
Tests for Scenario Generator
"""
import pytest
import yaml
from src.fuzzer_engine.test_case_generator import ScenarioGenerator
from src.models import (
    Scenario, OperationType, ChaosType, ProcessChaosType
)


def test_generate_random_scenario_with_seed():
    """Test generating random scenario with seed produces consistent results"""
    generator = ScenarioGenerator()
    
    # Generate two scenarios with same seed
    scenario1 = generator.generate_random_scenario(seed=12345)
    scenario2 = generator.generate_random_scenario(seed=12345)
    
    # Should have same configuration
    assert scenario1.cluster_config.num_shards == scenario2.cluster_config.num_shards
    assert scenario1.cluster_config.replicas_per_shard == scenario2.cluster_config.replicas_per_shard
    assert len(scenario1.operations) == len(scenario2.operations)
    assert scenario1.seed == scenario2.seed == 12345


def test_generate_random_scenario_without_seed():
    """Test generating random scenario without seed"""
    generator = ScenarioGenerator()
    
    scenario = generator.generate_random_scenario()
    
    # Should have valid configuration
    assert 3 <= scenario.cluster_config.num_shards <= 16
    # Ensure at least 1 replica for failover operations
    assert 1 <= scenario.cluster_config.replicas_per_shard <= 2
    assert len(scenario.operations) >= 1
    assert scenario.seed is not None


def test_random_cluster_config_ranges():
    """Test random cluster configuration stays within valid ranges"""
    generator = ScenarioGenerator(random_seed=42)
    
    for _ in range(10):
        scenario = generator.generate_random_scenario()
        assert 3 <= scenario.cluster_config.num_shards <= 16
        # Ensure at least 1 replica for failover operations
        assert 1 <= scenario.cluster_config.replicas_per_shard <= 2


def test_random_operations_are_failover():
    """Test random operations are failover type"""
    generator = ScenarioGenerator(random_seed=42)
    scenario = generator.generate_random_scenario()
    
    for operation in scenario.operations:
        assert operation.type == OperationType.FAILOVER
        assert operation.target_node is not None
        assert operation.timing.timeout > 0


def test_random_chaos_config():
    """Test random chaos configuration is valid"""
    generator = ScenarioGenerator(random_seed=42)
    scenario = generator.generate_random_scenario()
    
    assert scenario.chaos_config.chaos_type == ChaosType.PROCESS_KILL
    assert scenario.chaos_config.process_chaos_type in [ProcessChaosType.SIGKILL, ProcessChaosType.SIGTERM]
    assert scenario.chaos_config.target_selection.strategy in ["random", "primary_only", "replica_only"]


def test_parse_dsl_config_valid():
    """Test parsing valid DSL configuration"""
    dsl_text = """
scenario_id: test-scenario-001
seed: 12345
cluster:
  num_shards: 3
  replicas_per_shard: 1
  base_port: 7000
operations:
  - type: failover
    target_node: shard-0-primary
    parameters:
      force: true
    timing:
      delay_before: 1.0
      timeout: 30.0
      delay_after: 2.0
chaos:
  type: process_kill
  process_chaos_type: sigkill
  target_selection:
    strategy: random
  timing:
    delay_before_operation: 0.5
    chaos_duration: 10.0
  coordination:
    chaos_during_operation: true
validation:
  check_slot_coverage: true
  convergence_timeout: 60.0
"""
    
    generator = ScenarioGenerator()
    scenario = generator.parse_dsl_config(dsl_text)
    
    assert scenario.scenario_id == "test-scenario-001"
    assert scenario.seed == 12345
    assert scenario.cluster_config.num_shards == 3
    assert scenario.cluster_config.replicas_per_shard == 1
    assert len(scenario.operations) == 1
    assert scenario.operations[0].type == OperationType.FAILOVER
    assert scenario.chaos_config.chaos_type == ChaosType.PROCESS_KILL


def test_parse_dsl_config_minimal():
    """Test parsing minimal DSL configuration"""
    dsl_text = """
scenario_id: minimal-test
cluster:
  num_shards: 3
  replicas_per_shard: 0
operations:
  - type: failover
    target_node: shard-0-primary
"""
    
    generator = ScenarioGenerator()
    scenario = generator.parse_dsl_config(dsl_text)
    
    assert scenario.scenario_id == "minimal-test"
    assert scenario.cluster_config.num_shards == 3
    assert len(scenario.operations) == 1


def test_parse_dsl_config_invalid_yaml():
    """Test parsing invalid YAML"""
    dsl_text = """
scenario_id: test
cluster:
  num_shards: 3
  invalid yaml here
"""
    
    generator = ScenarioGenerator()
    with pytest.raises(ValueError, match="Invalid YAML syntax"):
        generator.parse_dsl_config(dsl_text)


def test_parse_dsl_config_missing_required_field():
    """Test parsing DSL with missing required fields"""
    dsl_text = """
scenario_id: test
cluster:
  num_shards: 3
"""
    
    generator = ScenarioGenerator()
    with pytest.raises(ValueError, match="Missing required field: operations"):
        generator.parse_dsl_config(dsl_text)


def test_parse_dsl_config_invalid_shard_count():
    """Test parsing DSL with invalid shard count"""
    dsl_text = """
scenario_id: test
cluster:
  num_shards: 20
  replicas_per_shard: 1
operations:
  - type: failover
    target_node: shard-0-primary
"""
    
    generator = ScenarioGenerator()
    with pytest.raises(ValueError, match="num_shards must be between 3 and 16"):
        generator.parse_dsl_config(dsl_text)


def test_validate_scenario_valid():
    """Test validating valid scenario"""
    generator = ScenarioGenerator(random_seed=42)
    scenario = generator.generate_random_scenario()
    
    assert generator.validate_scenario(scenario) is True


def test_validate_scenario_invalid_shards():
    """Test validating scenario with invalid shard count"""
    generator = ScenarioGenerator()
    scenario = generator.generate_random_scenario(seed=42)
    scenario.cluster_config.num_shards = 20
    
    with pytest.raises(ValueError, match="Invalid num_shards"):
        generator.validate_scenario(scenario)


def test_validate_scenario_no_operations():
    """Test validating scenario with no operations"""
    generator = ScenarioGenerator()
    scenario = generator.generate_random_scenario(seed=42)
    scenario.operations = []
    
    with pytest.raises(ValueError, match="at least one operation"):
        generator.validate_scenario(scenario)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
