# valkey-fuzzer

A topology-aware foundation for running seeded Valkey scenarios across
standalone, primary–replica, and cluster deployments.

## Overview

valkey-fuzzer helps identify bugs that are difficult to catch through traditional
integration testing. Existing cluster failover and process-chaos scenarios
remain supported, while the v2 DSL adds topology-neutral lifecycle, layered
server configuration, data seeding, and correctness checks.

This is the foundation for feature packs such as forkless save, replication
compression, and future cluster protocols. A complete feature pack still needs
an activation action, workload lifecycle, path-evidence observer, and independent
oracles; setting a server option alone does not prove that a feature ran.

The target onboarding contract is “manifest, plus an optional small plugin”:
once the generic runner is wired in, a new feature must not require a new CLI
command, parser branch, engine switch, topology special case, or reporter field.
The repository now contains the topology foundation and an isolated extension
API/registry contract; the generic feature-pack compiler and runner are the
remaining one-time integration work.

### Key Features

- **Seeded Scenarios**: Generate and replay scenario descriptions using configurable seeds
- **DSL-Based Configuration**: Create and reproduce specific test scenarios using a domain-specific language
- **Multiple Topologies**: Run standalone, primary–replica, and cluster deployments
- **Layered Server Configuration**: Configure all nodes, a role, or one exact node
- **Current Chaos Support**: Inject process failures coordinated with cluster failover operations
- **Topology-Aware Validation**: Apply only the checks meaningful for the selected deployment
- **Extensible Foundation**: Add feature actions, workloads, observers, oracles, and additional faults

## Architecture

The current execution path has five main responsibilities. The generic feature
design extends them with workload, action, observer, and oracle registries:

### Components

- **Fuzzer Engine**: Coordinates scenario execution and result collection
- **Topology Orchestrator**: Manages standalone, replication, and cluster lifecycle
- **Operation Orchestrator**: Runs the currently supported cluster failover action
- **Chaos Engine**: Injects process failures around cluster operations
- **State Validator and Data Seeder**: Writes validation keys and checks topology,
  replication, logs, and data consistency

There is not yet a continuous workload engine. The current data writes are
validation traffic, not proof that a configured feature-specific path was used.

## Installation

### Prerequisites

- Python 3.9+

### Setup

```bash
# Clone the repository
git clone <repository-url>
cd valkey-fuzzer

# Install dependencies
pip install -r requirements.txt

# Verify Valkey installation
valkey-server --version
```

## Usage

### CLI Help

View all available commands and options:

```bash
# Show all commands and options
valkey-fuzzer --help

# Show cluster command options
valkey-fuzzer cluster --help

# Show topology-neutral DSL command options
valkey-fuzzer run --help

# Show validate command options
valkey-fuzzer validate --help
```

### Topology-aware DSL execution

Version 1 `cluster:` scenarios continue to work unchanged. Version 2 uses a
`topology:` block:

```bash
# Standalone topology/configuration and data-consistency smoke test
valkey-fuzzer run examples/standalone_scenario.yaml \
  --valkey-binary /tmp/valkey/src/valkey-server

# Primary–replica topology/configuration smoke with compression settings
valkey-fuzzer run examples/replication_compression.yaml \
  --valkey-binary /tmp/valkey/src/valkey-server
```

```yaml
api_version: v2
scenario_id: replication-feature-smoke

topology:
  type: replication
  replicas_per_shard: 2
  server_config:
    repl-compression: lz4
  node_configs:
    primary:
      repl-diskless-sync: yes
    node-2:
      repl-compression: no

operations: []

state_validation:
  check_replication: true
  check_data_consistency: true
```

Configuration is merged from `server_config`, then a role entry such as
`node_configs.primary`, then an exact entry such as `node_configs.node-2`.
The example only verifies that the topology starts and that generic replication
and data checks pass. It does **not** prove that compression was negotiated or
used; that requires a feature-specific observer or protocol counter. The option
names and supported values also depend on the Valkey build under test.

See [the generic feature fuzzer design](docs/generic-feature-fuzzer-design.md)
for the stable extension boundary, proposed v3 feature envelope, example test
matrices, path-evidence rules, and implementation sequence.

### Random Test Execution

Generate and execute randomized test scenarios:

```bash
# Run with random seed
valkey-fuzzer cluster --random

# Run with specific seed for reproducibility
valkey-fuzzer cluster --seed 12345

# Run multiple iterations
valkey-fuzzer cluster --random --iterations 10

# Verbose output
valkey-fuzzer cluster --seed 42 --verbose
valkey-fuzzer cluster --random --verbose

# Run with iterations 
valkey-fuzzer cluster --seed 42 --iterations 2 --verbose

# Run against a specific Valkey binary
valkey-fuzzer cluster --random --valkey-binary /tmp/valkey/src/valkey-server

# Run a DSL scenario against a specific Valkey binary
valkey-fuzzer cluster --dsl examples/simple_failover.yaml --valkey-binary /tmp/valkey/src/valkey-server

```

### PR-Triggered Fuzzer Runs

This repo includes a reusable GitHub Actions workflow at `.github/workflows/valkey-pr-fuzzer.yml` that can:

- build Valkey from a specific ref, including a PR merge ref such as `refs/pull/123/merge`
- fan out a configurable number of random fuzzer runs, defaulting to `10`
- upload per-run artifacts with the console log, node logs, and JSON result payload
- fail the overall workflow if any run fails, while still letting all matrix runs complete
- treat a run as failed if any operation fails, any chaos injection fails, any post-operation validation wave fails, or the final validation fails

#### Manual Trigger

You can manually trigger the fuzzer workflow from the GitHub Actions UI:

1. Navigate to **Actions** -> **Valkey Cluster PR Fuzzer**
2. Click **Run workflow**
3. Fill in the required inputs:
   - `valkey_repository`: Repository containing the Valkey code (e.g., `valkey-io/valkey` or `username/valkey`)
   - `valkey_ref`: Git ref to test (e.g., `refs/pull/123/merge`, `main`, or a commit SHA)
   - `pr_number`: (Optional) PR number for summary display
   - `pr_url`: (Optional) PR URL for summary display
   - `run_count`: Number of random fuzzer runs (default: 10)
   - `fuzzer_repository`: (Optional) Override fuzzer repo to use
   - `fuzzer_ref`: (Optional) Override fuzzer ref to use

Example manual trigger inputs:
```
valkey_repository: valkey-io/valkey
valkey_ref: refs/pull/456/merge
pr_number: 456
pr_url: https://github.com/valkey-io/valkey/pull/456
run_count: 5
```

Each fuzzer run uploads artifacts containing:
- `console.log` - Full fuzzer execution output
- `result.json` - Structured test results
- `metadata.json` - Run metadata (status, seed, duration, failures)
- `node-logs/` - Individual Valkey node logs from `/tmp/valkey-fuzzer/logs`

To access these:
1. Navigate to the workflow run in **Actions**
2. Scroll to the **Artifacts** section at the bottom
3. Download `valkey-pr-fuzzer-run-N` for each run
4. Extract and examine the logs to debug failures

You can also see the full Fuzzer Run and other information in the `Execute Fuzzer Run` job

#### Label-Triggered Workflow

If you want this to run when a label is applied in the Valkey repo, the Valkey repo needs a small caller workflow. Example:

```yaml
name: Label-triggered Valkey Cluster Fuzzer

on:
  pull_request_target:
    types: [labeled]

permissions:
  contents: read

jobs:
  pr-fuzzer:
    permissions:
      contents: read
      issues: write
      pull-requests: write
    if: github.event.label.name == 'run-cluster-fuzzer'
    uses: valkey-io/valkey-fuzzer/.github/workflows/valkey-pr-fuzzer.yml@main
    with:
      fuzzer_repository: valkey-io/valkey-fuzzer
      fuzzer_ref: main
      valkey_repository: ${{ github.repository }}
      valkey_ref: refs/pull/${{ github.event.pull_request.number }}/merge
      pr_number: ${{ github.event.pull_request.number }}
      pr_url: ${{ github.event.pull_request.html_url }}
      label_name: ${{ github.event.label.name }}
      run_count: 10
```

For the simplest setup, point both the reusable workflow ref and `fuzzer_ref` at `main`, along with the matching `fuzzer_repository`, so the caller executes the current upstream workflow and fuzzer code. If you want tighter reproducibility, switch both references to the same tag or commit SHA instead.

This keeps the status visible on the Valkey PR as a normal workflow check, and the reusable workflow updates a sticky PR comment with the latest run summary. If the PR cannot produce a merge ref, for example due to merge conflicts, the workflow will fail during the Valkey checkout/build stage.

You can also add a small cleanup job in the caller workflow to remove `run-cluster-fuzzer` after the run finishes, so maintainers can rerun by simply reapplying the label.

### DSL-Based Test Execution

Execute predefined test scenarios using DSL configuration:

```bash
# Run DSL-based test
valkey-fuzzer cluster --dsl test_scenario.yaml

# Validate DSL configuration
valkey-fuzzer validate test_scenario.yaml
```

### Exporting Random Scenarios

Capture randomly generated test scenarios as DSL files for reproducibility and sharing:

```bash
# Export a random scenario
valkey-fuzzer cluster --random --export-dsl captured_scenario.yaml

# Export with specific seed
valkey-fuzzer cluster --seed 12345 --export-dsl seed_12345_scenario.yaml

# Replay the captured scenario
valkey-fuzzer cluster --dsl captured_scenario.yaml
```

### DSL Configuration Examples

#### Basic Failover Test

```yaml
# simple_failover.yaml
scenario_id: "basic_failover_test"
seed: 42

cluster:
  num_shards: 3
  replicas_per_shard: 1

operations:
  - type: "failover"
    target_node: "shard-0-primary"
    parameters:
      force: false
    timing:
      delay_before: 0.0
      timeout: 30.0
      delay_after: 0.0

chaos:
  type: "process_kill"
  process_chaos_type: "sigkill"
  target_selection:
    strategy: "replica_only"
  timing:
    delay_before_operation: 0.0
    delay_after_operation: 0.0
    chaos_duration: 5.0
  coordination:
    chaos_before_operation: true
    chaos_during_operation: false
    chaos_after_operation: false

state_validation:
  check_slot_coverage: true
  check_cluster_status: true
  check_replication: true
  check_data_consistency: true
  validation_timeout: 30.0
```

The current DSL supports failover operations and one process-chaos policy that
is applied around those operations. Continuous `workload:` blocks and
feature-specific actions are part of the feature-pack roadmap, not the current
parser. See `examples/simple_failover.yaml` for the complete legacy cluster
form and the v2 examples above for topology/configuration smokes.

### Expected Output Formats

#### Test Execution Summary

```
=== valkey-fuzzer Test Results ===
Scenario: simple-failover-test
Status: PASSED
Duration: 30.88s
Operations: 1
Chaos Events: 0
Seed: 12345 (use to reproduce)

Final Validation: PASSED

Final Validation Details:
  Replication: PASS
  Cluster Status: PASS
  Slot Coverage: PASS
  Topology: PASS
  View Consistency: PASS
```

#### Failure Report

```
=== Test Failure Report ===
Scenario: 4226257627
Status: FAILED
Duration: 211.85s
Operations: 2
Chaos Events: 2
Seed: 4226257627 (use to reproduce)

Final Validation: FAILED
Failed Checks: slot_coverage, topology, data_consistency
Validation Errors: 3

Chaos Events:
  [PASS] process_kill on node-0 (8.40s)
  [PASS] process_kill on node-1 (5.43s)

Final Validation Details:
  Replication: PASS
  Cluster Status: PASS
  Slot Coverage: FAIL
    → CRITICAL: 5461 slots still assigned to killed nodes. Killed nodes: {'127.0.0.1:7623', '127.0.0.1:7622'}. This indicates failover did not complete or slots were not reassigned. Affected slots: 0-5460
  Topology: FAIL
    → Topology validation failed in strict mode: 2 mismatch(es) found
  View Consistency: PASS
  Data Consistency: FAIL
    → 35 test key(s) unreachable (threshold: 10)

  Validation Error Messages:
    • Slot Coverage: CRITICAL: 5461 slots still assigned to killed nodes. Killed nodes: {'127.0.0.1:7623', '127.0.0.1:7622'}. This indicates failover did not complete or slots were not reassigned. Affected slots: 0-5460
    • Topology: Topology validation failed in strict mode: 2 mismatch(es) found
    • Data Consistency: 35 test key(s) unreachable (threshold: 10)

Reproduction Command:
valkey-fuzzer cluster --seed 4226257627
```

## Current Scope

The current implementation focuses on:

### Supported Operations
- **Failover Operations**: Primary node failover with replica promotion
- **Process Chaos**: Random process termination (SIGKILL, SIGTERM)

### Supported Configurations
- **Standalone**: One writable node
- **Primary–Replica**: One primary with 1-5 replicas
- **Cluster Sizes**: 3-16 shards with 0-5 replicas per shard
- **Server Configuration**: Global, per-role, and exact-node startup settings
- **Deployment**: Single metal instance with multiple processes

### Validation Capabilities
- Slot coverage and assignment validation
- Replica synchronization monitoring
- Data consistency verification
- Convergence time and replication lag tracking
- Actual Valkey node log validation

## Feature-pack roadmap

The topology foundation is the first layer. Feature packs should add an
activation action, continuous workload, path-evidence observer, faults, and
independent correctness oracles.

The forkless and replication-compression YAML files in `examples/` are therefore
configuration/topology smokes. They are not yet complete feature packs.

The isolated contracts in `src/extensibility.py` define the six stable
component roles, immutable feature packs and run plans, lifecycle events,
uniform outcomes, typed registries, and `valkey_fuzzer.plugins` entry-point
discovery. They are intentionally not connected to the current v1/v2 runner
yet, so the repository does not claim drop-in feature execution until the
manifest compiler, generic runner, and legacy adapter are complete.

### Additional Operations
- `add_replica`: Add new replica nodes to existing shards
- `remove_replica`: Remove replica nodes from shards
- `reshard`: Redistribute slots across shards
- `scale_out`: Add new shards to the cluster
- `scale_in`: Remove shards and redistribute data
- `configuration_change`: Modify cluster configuration parameters

### Enhanced Chaos Engineering
- **Network Chaos**: Packet filtering, latency simulation, selective packet drops
- **Resource Chaos**: Memory pressure, CPU throttling, disk I/O limitations
- **Time Chaos**: Clock skew simulation, network time protocol disruption

### Extended Validation
- **Performance Regression**: Benchmark comparison across test runs
- **Memory Leak Detection**: Long-running test memory profiling
- **Security Validation**: Authentication and authorization testing

### Mixed Clusters
- Test the fuzzer on different versions of Valkey
- Create chaos while the cluster is upgrading from an older branch to unstable

## Contributing

1. Fork the repository
2. Create a feature branch
3. Implement changes with comprehensive tests
4. Submit a pull request with detailed description

## Daily Test Runs
- We run a randomly generated scenario every 4 hours and the results are listed here: https://github.com/valkey-io/valkey-fuzzer/actions/workflows/fuzzer-run.yml
- Each scheduled run uploads a `fuzzer-run-artifacts-*` bundle containing the structured `results.json`, the exported scenario DSL, and the collected node/test logs from `/tmp/valkey-fuzzer/logs`
- These results can be analyzed to find potential bugs in Valkey

## License

This project is licensed under the BSD 3-Clause License.
