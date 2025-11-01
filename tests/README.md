# Test Suite Documentation

This directory contains the test suite for the Cluster Bus Fuzzer project.

## Test Categories

### Unit Tests
Fast tests that test individual components in isolation, often with mocks.

- `test_models.py` - Data model validation
- `test_test_case_generator.py` - Scenario generation logic
- `test_test_logger.py` - Logging functionality
- `test_chaos_coordinator.py` - Chaos coordination logic
- `test_cluster_coordinator.py` - Cluster lifecycle management
- `test_state_validator.py` - State validation logic

### Integration Tests
Tests that verify component interactions without creating real clusters.

- `test_fuzzer_engine_integration.py` - Fuzzer engine component integration
  - Random scenario generation
  - DSL parsing and validation
  - Error handling
  - Reproducibility

### End-to-End Tests (E2E)
Comprehensive tests that create real Valkey clusters and execute complete scenarios.

- `test_fuzzer_e2e.py` - Full fuzzer engine execution with real clusters
  - Simple random scenario execution
  - Minimal failover without chaos
  - DSL scenario from file
  - Failover with chaos injection
  - Multiple sequential operations
  - Reproducibility with seeds
  - Error recovery
  - Log file creation

### System Tests
Tests that verify the complete system with real Valkey clusters.

- `test_orchestrator.py` - Cluster orchestrator with real clusters
- `test_chaos.py` - Chaos injection with real clusters
- `test_node_restart.py` - Node restart scenarios
- `test_load_data.py` - Data loading and validation

## Running Tests

### Run All Tests
```bash
pytest
```

### Run Specific Test Categories

**Unit and Integration Tests (Fast)**
```bash
pytest -m "not slow"
```

**End-to-End Tests (Slow)**
```bash
pytest -m slow
```

**Specific Test File**
```bash
pytest tests/test_fuzzer_e2e.py -v
```

**Specific Test**
```bash
pytest tests/test_fuzzer_e2e.py::TestFuzzerEngineE2E::test_minimal_failover_without_chaos -v
```

### Verbose Output
```bash
pytest -v -s
```

### Show Test Output
```bash
pytest -s
```

### Stop on First Failure
```bash
pytest -x
```

## Test Markers

Tests can be marked with custom markers:

- `@pytest.mark.slow` - Marks tests that take significant time (E2E tests)
- `@pytest.mark.e2e` - Marks end-to-end tests with real clusters

### Selecting Tests by Marker

**Run only slow tests:**
```bash
pytest -m slow
```

**Exclude slow tests:**
```bash
pytest -m "not slow"
```

## E2E Test Details

### TestFuzzerEngineE2E

**test_simple_random_scenario_execution**
- Creates cluster with random configuration
- Executes randomized operations
- Validates cluster state
- ~78 seconds

**test_minimal_failover_without_chaos**
- Creates 3-shard cluster with 1 replica per shard
- Executes single failover operation
- No chaos injection
- Validates slot coverage and replica sync
- ~15 seconds

**test_dsl_scenario_from_file**
- Loads DSL configuration from YAML file
- Executes predefined scenario
- Validates reproducibility
- ~15 seconds

**test_failover_with_chaos_injection**
- Creates cluster with chaos coordination
- Injects process chaos during failover
- Validates cluster recovery
- ~20 seconds

**test_multiple_operations_sequence**
- Executes multiple sequential failover operations
- Validates cluster state after each operation
- Tests operation coordination
- ~25 seconds

**test_reproducibility_with_seed**
- Runs same scenario twice with same seed
- Verifies reproducible behavior
- ~150 seconds (2 full test runs)

### TestFuzzerErrorRecovery

**test_cluster_creation_retry**
- Tests retry logic for cluster creation
- Handles port conflicts gracefully
- ~15 seconds

### TestFuzzerLogging

**test_log_files_created**
- Verifies log file creation
- Validates log content structure
- Checks JSON format
- ~15 seconds

## Test Execution Flow

### E2E Test Execution Steps

1. **Cluster Creation**
   - Spawn Valkey nodes
   - Form cluster with CLUSTER MEET
   - Assign slots to primaries
   - Configure replicas
   - Wait for replication sync
   - Validate cluster health

2. **Operation Execution**
   - Log cluster state before operation
   - Coordinate chaos injection (if configured)
   - Execute cluster operation (e.g., failover)
   - Wait for operation completion
   - Log cluster state after operation
   - Validate cluster state

3. **Final Validation**
   - Check slot coverage (all 16384 slots)
   - Verify replica synchronization
   - Validate node connectivity
   - Check data consistency (if enabled)

4. **Cleanup**
   - Stop active chaos injections
   - Terminate all node processes
   - Release allocated ports
   - Delete data directories
   - Close connections

## Test Logs

E2E tests generate detailed logs in `/tmp/valkey-fuzzer/logs/`:

- JSON log files with complete test execution details
- Cluster state snapshots at key points
- Operation and chaos event logs
- Validation results

Example log file: `/tmp/valkey-fuzzer/logs/e2e-minimal-failover.json`

## Troubleshooting Tests

### Port Conflicts

If tests fail with port conflicts:
```bash
# Check for processes using ports
lsof -i :6379-6400

# Kill processes if needed
pkill -f valkey-server
```

### Cleanup Issues

If cleanup fails:
```bash
# Manual cleanup
rm -rf /tmp/valkey-fuzzer
pkill -f valkey-server
```

### Slow Tests

E2E tests are intentionally slow because they:
- Create real Valkey clusters
- Wait for cluster convergence
- Execute actual failover operations
- Validate cluster state thoroughly

To skip slow tests during development:
```bash
pytest -m "not slow"
```

## Test Coverage

To generate test coverage report:
```bash
pytest --cov=src --cov-report=html
```

View coverage report:
```bash
open htmlcov/index.html
```

## Continuous Integration

Tests are run automatically on GitHub Actions:
- Unit and integration tests on every push
- E2E tests on pull requests
- Full test suite on main branch

See `.github/workflows/test.yml` for CI configuration.

## Writing New Tests

### Unit Test Template

```python
import pytest
from src.component import Component

class TestComponent:
    def test_basic_functionality(self):
        component = Component()
        result = component.do_something()
        assert result is not None
```

### E2E Test Template

```python
import pytest
from src.main import ClusterBusFuzzer

@pytest.mark.slow
class TestNewFeatureE2E:
    def test_new_feature(self):
        fuzzer = ClusterBusFuzzer()
        result = fuzzer.run_random_test(seed=42)
        assert result.success
```

## Best Practices

1. **Use Seeds**: Always use seeds for reproducible tests
2. **Mark Slow Tests**: Use `@pytest.mark.slow` for E2E tests
3. **Clean Up**: Ensure proper cleanup in finally blocks
4. **Descriptive Names**: Use clear, descriptive test names
5. **Log Output**: Use logging for debugging E2E tests
6. **Assertions**: Include meaningful assertion messages
7. **Isolation**: Tests should not depend on each other

## Performance Benchmarks

Typical test execution times on MacBook Pro M1:

- Unit tests: < 1 second each
- Integration tests: < 1 second each
- E2E minimal test: ~15 seconds
- E2E with chaos: ~20 seconds
- E2E multiple operations: ~25 seconds
- Full E2E suite: ~5 minutes

## Next Steps

- Add more E2E scenarios for edge cases
- Implement performance benchmarking tests
- Add stress tests with larger clusters
- Create chaos injection variation tests
- Add network partition simulation tests
