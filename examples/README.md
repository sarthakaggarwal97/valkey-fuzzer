# Cluster Bus Fuzzer Examples

This directory contains example configurations and scripts for using the Cluster Bus Fuzzer.

## Quick Start

### 1. Validate a DSL Configuration

Before running tests, you can validate your DSL configuration:

```bash
python examples/run_fuzzer.py validate examples/example_scenario.yaml
```

### 2. Run a Random Test

Generate and execute a randomized test scenario:

```bash
python examples/run_fuzzer.py random
```

Run with a specific seed for reproducibility:

```bash
python examples/run_fuzzer.py random --seed 42
```

### 3. Run a DSL-Based Test

Execute a predefined test scenario from a YAML file:

```bash
python examples/run_fuzzer.py dsl examples/simple_failover.yaml
```

## Example DSL Files

### simple_failover.yaml

A minimal failover test without chaos injection. Good for testing basic cluster failover functionality.

**Features:**
- 3 shards with 1 replica per shard
- Single failover operation
- No chaos injection
- Basic validation checks

### example_scenario.yaml

A comprehensive test with chaos injection during failover operations.

**Features:**
- 3 shards with 1 replica per shard
- Multiple failover operations with different parameters
- Process chaos injection (SIGKILL) during operations
- Full validation suite

## DSL Configuration Format

A DSL configuration file consists of the following sections:

### Required Fields

```yaml
scenario_id: "unique-test-identifier"
cluster:
  num_shards: 3              # 3-16 shards
  replicas_per_shard: 1      # 0-2 replicas per shard
operations:
  - type: "failover"
    target_node: "shard-0-primary"
    parameters:
      force: false
    timing:
      delay_before: 0.0
      timeout: 30.0
      delay_after: 0.0
```

### Optional Fields

```yaml
seed: 42  # For reproducibility

cluster:
  base_port: 7000
  base_data_dir: "/tmp/valkey-fuzzer"
  valkey_binary: "/usr/local/bin/valkey-server"
  enable_cleanup: true

chaos:
  type: "process_kill"
  process_chaos_type: "sigkill"  # or "sigterm"
  target_selection:
    strategy: "random"  # random, primary_only, replica_only, specific
  timing:
    delay_before_operation: 0.5
    delay_after_operation: 0.5
    chaos_duration: 10.0
  coordination:
    chaos_before_operation: false
    chaos_during_operation: true
    chaos_after_operation: false

validation:
  check_slot_coverage: true
  check_slot_conflicts: true
  check_replica_sync: true
  check_node_connectivity: true
  check_data_consistency: true
  convergence_timeout: 60.0
  max_replication_lag: 5.0
```

## Programmatic Usage

You can also use the fuzzer programmatically in your Python code:

```python
from src.main import ClusterBusFuzzer
from src.fuzzer_engine import DSLLoader

# Initialize fuzzer
fuzzer = ClusterBusFuzzer()

# Run random test
result = fuzzer.run_random_test(seed=42)
print(f"Test {'passed' if result.success else 'failed'}")

# Run DSL test
dsl_config = DSLLoader.load_from_file("examples/simple_failover.yaml")
result = fuzzer.run_dsl_test(dsl_config)
print(f"Operations executed: {result.operations_executed}")
```

## Converting Random Tests to DSL

You can convert a random test scenario to a DSL file for reproducibility:

```python
from src.fuzzer_engine import FuzzerEngine, DSLLoader

engine = FuzzerEngine()

# Generate random scenario
scenario = engine.generate_random_scenario(seed=123)

# Save as DSL
DSLLoader.save_scenario_as_dsl(scenario, "my_scenario.yaml")

# Now you can reproduce this exact test
dsl_config = DSLLoader.load_from_file("my_scenario.yaml")
result = engine.execute_dsl_scenario(dsl_config)
```

## Test Logs

Test execution logs are stored in `/tmp/valkey-fuzzer/logs/` by default. Each test creates:

- A JSON log file with complete test execution details
- Cluster state snapshots at key points
- Operation and chaos event logs
- Validation results

## Creating Custom Scenarios

To create your own test scenario:

1. Copy one of the example YAML files
2. Modify the configuration to match your test requirements
3. Validate the configuration: `python examples/run_fuzzer.py validate your_scenario.yaml`
4. Run the test: `python examples/run_fuzzer.py dsl your_scenario.yaml`

### Tips for Custom Scenarios

- Start with fewer operations and add complexity gradually
- Use `chaos_during_operation: false` initially to test without chaos
- Adjust `convergence_timeout` based on your cluster size
- Use specific seeds for reproducible debugging
- Check logs in `/tmp/valkey-fuzzer/logs/` for detailed execution information

## Troubleshooting

### Cluster Creation Fails

- Ensure Valkey binary is available at the specified path
- Check that ports in the range are not already in use
- Verify sufficient system resources (memory, file descriptors)

### Operations Timeout

- Increase `timeout` in operation timing configuration
- Check cluster health before operations
- Review logs for specific error messages

### Validation Failures

- Increase `convergence_timeout` for larger clusters
- Adjust `max_replication_lag` based on your environment
- Disable specific validation checks if not needed

## Next Steps

- Review the [main README](../README.md) for architecture details
- Check the [design document](../.kiro/specs/cluster-bus-fuzzer/design.md) for component details
- Explore the [test suite](../tests/) for more examples
