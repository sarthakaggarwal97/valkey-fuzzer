# Fuzzer Engine Usage Guide

## Overview

The Fuzzer Engine is the main orchestrator for the Cluster Bus Fuzzer. It coordinates all components to execute end-to-end test scenarios that validate Valkey cluster bus robustness through chaos engineering and randomized testing.

## Architecture

The Fuzzer Engine integrates the following components:

- **Test Case Generator**: Creates randomized or DSL-based test scenarios
- **Cluster Coordinator**: Manages cluster lifecycle (create, monitor, destroy)
- **Chaos Coordinator**: Injects process failures coordinated with operations
- **Operation Orchestrator**: Executes cluster operations (failover, etc.)
- **State Validator**: Validates cluster state and data consistency
- **Test Logger**: Comprehensive logging and reporting

## Basic Usage

### Python API

```python
from src.main import ClusterBusFuzzer

# Initialize the fuzzer
fuzzer = ClusterBusFuzzer()

# Run a random test
result = fuzzer.run_random_test(seed=42)

# Check results
if result.success:
    print(f"Test passed! Executed {result.operations_executed} operations")
else:
    print(f"Test failed: {result.error_message}")
```

### Command Line

```bash
# Run random test
python examples/run_fuzzer.py random --seed 42

# Run DSL-based test
python examples/run_fuzzer.py dsl examples/example_scenario.yaml

# Validate DSL configuration
python examples/run_fuzzer.py validate examples/example_scenario.yaml
```

## Test Execution Pipeline

The Fuzzer Engine follows this execution pipeline:

### 1. Cluster Creation
- Creates Valkey cluster based on configuration
- Spawns nodes and forms cluster
- Validates cluster readiness
- Retries on failure with exponential backoff

### 2. Cluster Validation
- Verifies all slots assigned (0-16383)
- Checks node connectivity
- Validates cluster health
- Logs initial cluster state

### 3. Operation Execution
- Executes operations sequentially
- Coordinates chaos injection with operations
- Logs cluster state before/after each operation
- Validates cluster state after each operation
- Continues with graceful degradation on failures

### 4. Final Validation
- Performs comprehensive cluster validation
- Checks slot coverage, replica sync, connectivity
- Validates data consistency
- Determines overall test success

### 5. Cleanup
- Stops active chaos injections
- Destroys cluster and releases resources
- Logs test completion
- Generates test report

## Error Handling and Recovery

The Fuzzer Engine implements graceful degradation:

### Cluster Creation Failures
- Retries up to 3 times with exponential backoff
- Logs detailed error information
- Returns failure result if all retries exhausted

### Operation Failures
- Logs operation failure but continues with next operation
- Validates cluster state after each operation
- Allows partial test completion

### Chaos Injection Failures
- Logs chaos failure but continues test execution
- Does not block operation execution
- Maintains test validity without chaos

### Validation Failures
- Logs validation issues but continues test
- Distinguishes critical vs non-critical failures
- Allows test to complete for analysis

## Test Results

### ExecutionResult Object

```python
@dataclass
class ExecutionResult:
    scenario_id: str              # Unique test identifier
    success: bool                 # Overall test success
    start_time: float            # Test start timestamp
    end_time: float              # Test end timestamp
    operations_executed: int     # Number of operations completed
    chaos_events: List[ChaosResult]        # Chaos injection results
    validation_results: List[ValidationResult]  # Validation results
    error_message: Optional[str]  # Error message if failed
    seed: Optional[int]          # Seed for reproducibility
```

### Success Criteria

A test is considered successful if:
- At least one operation executed successfully
- Final validation shows slot coverage (all 16384 slots assigned)
- All replicas are synchronized with primaries

## DSL Configuration

### Complete Example

```yaml
scenario_id: "my-test"
seed: 42

cluster:
  num_shards: 3
  replicas_per_shard: 1
  base_port: 7000

operations:
  - type: "failover"
    target_node: "shard-0-primary"
    parameters:
      force: false
    timing:
      delay_before: 1.0
      timeout: 30.0
      delay_after: 2.0

chaos:
  type: "process_kill"
  process_chaos_type: "sigkill"
  target_selection:
    strategy: "primary_only"
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

### DSL Utilities

```python
from src.fuzzer_engine import DSLLoader, DSLValidator

# Load from file
dsl_config = DSLLoader.load_from_file("test.yaml")

# Load from string
dsl_config = DSLLoader.load_from_string(yaml_text)

# Validate configuration
from src.fuzzer_engine import ScenarioGenerator
generator = ScenarioGenerator()
is_valid = DSLLoader.validate_dsl_config(dsl_config, generator)

# Convert scenario to DSL
DSLLoader.save_scenario_as_dsl(scenario, "output.yaml")
```

## Logging and Reporting

### Log Files

Test logs are stored in `/tmp/valkey-fuzzer/logs/` with the following structure:

```json
{
  "scenario_id": "test-123",
  "seed": 42,
  "start_time": 1234567890.0,
  "cluster_config": {...},
  "operations": [...],
  "operation_logs": [...],
  "chaos_events": [...],
  "validation_results": [...],
  "cluster_state_snapshots": [...],
  "errors": [...],
  "status": "completed"
}
```

### Accessing Logs

```python
from src.fuzzer_engine import FuzzerLogger

logger = FuzzerLogger()

# Get specific test log
test_log = logger.get_test_log("test-123")

# Get all test logs
all_logs = logger.get_all_test_logs()

# Generate report from multiple tests
report = logger.generate_report(test_results)
print(report)
```

## Advanced Usage

### Custom Scenario Generation

```python
from src.fuzzer_engine import FuzzerEngine
from src.models import (
    Scenario, ClusterConfig, Operation, OperationType,
    OperationTiming, ChaosConfig, ValidationConfig
)

engine = FuzzerEngine()

# Create custom scenario
scenario = Scenario(
    scenario_id="custom-test",
    cluster_config=ClusterConfig(
        num_shards=5,
        replicas_per_shard=2
    ),
    operations=[
        Operation(
            type=OperationType.FAILOVER,
            target_node="shard-0-primary",
            parameters={"force": True},
            timing=OperationTiming(timeout=60.0)
        )
    ],
    chaos_config=ChaosConfig(...),
    validation_config=ValidationConfig()
)

# Execute custom scenario
result = engine.execute_test(scenario)
```

### Batch Testing

```python
from src.main import ClusterBusFuzzer

fuzzer = ClusterBusFuzzer()
results = []

# Run multiple tests with different seeds
for seed in range(100, 110):
    result = fuzzer.run_random_test(seed=seed)
    results.append(result)
    print(f"Test {seed}: {'PASS' if result.success else 'FAIL'}")

# Generate summary report
from src.fuzzer_engine import FuzzerLogger
logger = FuzzerLogger()
report = logger.generate_report(results)
print(report)
```

### Reproducibility

```python
# Run test and save configuration
result = fuzzer.run_random_test(seed=42)

if not result.success:
    # Convert to DSL for debugging
    scenario = engine.generate_random_scenario(seed=42)
    DSLLoader.save_scenario_as_dsl(scenario, "failed_test.yaml")
    
    # Later, reproduce the exact test
    dsl_config = DSLLoader.load_from_file("failed_test.yaml")
    result = fuzzer.run_dsl_test(dsl_config)
```

## Performance Considerations

### Cluster Size
- Larger clusters (more shards) take longer to create and validate
- Recommended: Start with 3-5 shards for development

### Operation Timing
- Adjust timeouts based on cluster size and hardware
- Larger clusters may need longer convergence times

### Validation Checks
- Data consistency checks are most expensive
- Disable if not needed for faster testing

### Resource Usage
- Each node requires ~50MB memory
- Monitor system resources for large clusters
- Use cleanup to release resources between tests

## Troubleshooting

### Common Issues

**Cluster creation timeout**
- Increase retry count or backoff time
- Check Valkey binary path
- Verify port availability

**Operation failures**
- Check cluster health before operations
- Increase operation timeout
- Review operation logs for details

**Validation failures**
- Increase convergence timeout
- Check for network issues
- Review cluster state snapshots

**Chaos injection failures**
- Verify process permissions
- Check node process IDs
- Review chaos coordinator logs

## Best Practices

1. **Start Simple**: Begin with basic scenarios without chaos
2. **Use Seeds**: Always use seeds for reproducible debugging
3. **Monitor Logs**: Check logs for detailed execution information
4. **Validate DSL**: Always validate DSL files before running
5. **Cleanup**: Ensure proper cleanup between tests
6. **Batch Testing**: Run multiple tests to find edge cases
7. **Convert to DSL**: Save interesting random scenarios as DSL

## Next Steps

- Review [examples](../examples/) for more usage patterns
- Check [design document](../.kiro/specs/cluster-bus-fuzzer/design.md) for architecture details
- Explore [test suite](../tests/) for testing patterns
- Read [requirements](../.kiro/specs/cluster-bus-fuzzer/requirements.md) for feature details
