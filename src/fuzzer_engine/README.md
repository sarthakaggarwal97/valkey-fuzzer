# Fuzzer Engine Components

This directory contains the core components of the Fuzzer Engine for the Cluster Bus Fuzzer.

## Components

### 1. Scenario Generator (`test_case_generator.py`)

Generates randomized and DSL-based test scenarios for cluster testing.

**Key Features:**
- Randomized scenario generation with configurable seeds for reproducibility
- DSL (YAML) configuration parsing for test scenario specification
- Cluster configuration randomization (3-16 shards, 0-2 replicas)
- Failover operation generation with randomized parameters
- Chaos configuration generation (process kills, timing, coordination)
- Scenario validation

**Usage:**
```python
from src.fuzzer_engine import ScenarioGenerator

# Generate random scenario
generator = ScenarioGenerator()
scenario = generator.generate_random_scenario(seed=12345)

# Parse DSL configuration
dsl_text = """
scenario_id: test-001
cluster:
  num_shards: 3
  replicas_per_shard: 1
operations:
  - type: failover
    target_node: shard-0-primary
"""
scenario = generator.parse_dsl_config(dsl_text)
```

### 2. Operation Orchestrator (`operation_orchestrator.py`)

Executes cluster operations with timing coordination and state management.

**Key Features:**
- Failover operation execution with randomized parameters
- Operation timing management (delay before/after, timeout)
- Operation state tracking
- Precondition validation
- Completion waiting with timeout

**Usage:**
```python
from src.fuzzer_engine import OperationOrchestrator
from src.models import Operation, OperationType, OperationTiming

orchestrator = OperationOrchestrator(cluster_connection)

operation = Operation(
    type=OperationType.FAILOVER,
    target_node="shard-0-primary",
    parameters={"force": False},
    timing=OperationTiming(timeout=30.0)
)

success = orchestrator.execute_operation(operation, cluster_id)
```

### 3. State Validator (`state_validator.py`)

Validates cluster state and data consistency.

**Key Features:**
- Comprehensive cluster state validation
- Slot coverage and conflict detection
- Replica synchronization validation
- Node connectivity checking
- Data consistency validation
- Convergence time and replication lag monitoring

**Usage:**
```python
from src.fuzzer_engine import StateValidator
from src.models import ValidationConfig

config = ValidationConfig(
    check_slot_coverage=True,
    check_replica_sync=True,
    convergence_timeout=60.0
)

validator = StateValidator(config)
result = validator.validate_cluster_state(cluster_id, cluster_connection)

print(f"Slot coverage: {result.slot_coverage}")
print(f"Replicas synced: {result.replica_sync.all_replicas_synced}")
print(f"Convergence time: {result.convergence_time}s")
```

## Testing

All components have comprehensive unit tests:
- `tests/test_test_case_generator.py` - 13 tests
- `tests/test_operation_orchestrator.py` - 7 tests
- `tests/test_state_validator.py` - 14 tests

Run tests with:
```bash
python -m pytest tests/test_test_case_generator.py -v
python -m pytest tests/test_operation_orchestrator.py -v
python -m pytest tests/test_state_validator.py -v
```

## Requirements Satisfied

These components satisfy the following requirements from the spec:

**Scenario Generator:**
- Requirement 2.1: DSL configuration parsing
- Requirement 2.2: DSL validation and error reporting
- Requirement 4.1: Randomized failover operation generation

**Operation Orchestrator:**
- Requirement 4.1: Failover operation execution
- Requirement 4.2: Operation timing coordination
- Requirement 4.3: Operation state validation

**State Validator:**
- Requirement 6.1: Slot coverage and conflict validation
- Requirement 6.2: Node connectivity validation
- Requirement 6.3: Data consistency validation
- Requirement 6.4: Convergence time and replication lag monitoring
