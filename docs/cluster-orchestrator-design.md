# Cluster Orchestrator Detailed Design Document

## Overview

The Cluster Orchestrator is a foundational component of the Cluster Bus Fuzzer responsible for Valkey cluster lifecycle management. It handles cluster creation, formation, configuration, and cleanup operations. The orchestrator provides the stable cluster infrastructure that other components (Fuzzer Engine, Chaos Engine) operate with.

## Core Responsibilities

### Primary Functions
1. **Cluster Planning**: Design cluster topology based on configuration requirements given by Fuzzer Engine
2. **Node Management**: Spawn, configure, and terminate Valkey server processes
3. **Cluster Formation**: Coordinate cluster initialization, slot assignment, and replication setup
4. **Resource Management**: Manages ports
5. **State Monitoring**: Track cluster health and provide status information
6. **Cleanup Operations**: Ensure proper resource cleanup after testing
7. **Dynamic Operations**: Support runtime cluster modifications for testing scenarios

### Design Principles
- Use built valkey-server binary from unstable branch for latest features
- All Valkey nodes run as separate processes on single metal instance
- Generate deterministic cluster topologies from identical configuration inputs
- Prevent resource contention through port management and CPU core pinning (in the future)
- Guarantee clean state between tests with no zombie processes or resource leaks

## Core Components

### 1. Port Manager
**Manages port allocation and deallocation for cluster nodes**

#### Responsibilities:
- Allocate unique client and bus ports for each node
- Track port usage to prevent conflicts
- Release ports when nodes are terminated

### 2. Configuration Manager
**Handles cluster planning, node spawning, and resource management**

#### Core Responsibilities:
- Parse cluster specifications from Fuzzer Engine
- Plan topology and determine shard membership
- Generate slot distribution across primaries
- Validate resource availability before execution
- Spawn Valkey server processes with proper configuration
- Manage node directories and log files
- Handle Valkey binary discovery and building
- Track process IDs and status and coordinate resource cleanup

#### Topology Planning:
The topology planner distributes the 16,384 hash slots evenly across the requested number of shards. For each shard, it creates one primary node responsible for a specific slot range, followed by the specified number of replica nodes. The last shard receives any remaining slots to ensure complete coverage. Each node gets allocated ports and directory paths during planning.

#### Node Spawning Process:
For each planned node, the Configuration Manager creates dedicated data directories and log files, then spawns a Valkey server process with cluster-specific configuration. Key configuration includes cluster mode enabled, protected mode disabled for testing, and snapshot saving disabled. After spawning all processes, it waits for each node to become responsive before proceeding.


#### Example Topology Planning:
For a configuration with 4 shards and 2 replicas per shard:
- **Total Nodes**: 12 (4 primaries + 8 replicas)
- **Slot Distribution**: Primary 0 gets slots 0-4095, Primary 1 gets 4096-8191, etc.
- **Port Assignment**: Sequential from base_port (6379, 6380, 6381...)
- **Shard Membership**: Each shard contains 1 primary + 2 replicas
- **Directory Structure**: `/tmp/valkey-fuzzer/cluster-{id}/node-{n}/data`

### 3. Resource Manager (Priority One Requirement)
**Manages finite resources on the metal instance**

#### CPU Core Management:
- Reserve cores for Fuzzer Engine, Chaos Engine, and Valkey Client
- Round-robin assignment of remaining cores to Valkey nodes
- Utilize core pinning (through taskset) to bind each Valkey process to specific CPU core (Future requirement)
- Track core assignments and support dynamic pinning/unpinning
- Prevent CPU contention between cluster nodes and fuzzer components

### 4. Cluster Manager
**Manages cluster formation, validation, and health monitoring**

#### Core Responsibilities:
- Form clusters from spawned nodes using CLUSTER commands
- Execute node introduction through CLUSTER MEET
- Assign hash slots to primary nodes
- Configure replication between primary and replica nodes
- Validate cluster health and state
- Provide cluster status information
- Support dynamic cluster operations

#### Cluster Formation Process:
Cluster formation follows a five-step process: 
- 1. Reset any existing cluster state on all nodes 
- 2. Connect all nodes using `CLUSTER MEET` commands to establish cluster membership
- 3. Assign hash slots to primary nodes and verify complete coverage using `CLUSTER ADDSLOTS`
- 4. Configure replication relationships between primary and replica nodes using `CLUSTER REPLICATE`
- 5. Validate the complete cluster state to ensure proper operation through `CLUSTER INFO`

#### Node Introduction Process:
The cluster meet process follows these steps:
1. Wait for all nodes to accept PING commands
2. Select first node to execute CLUSTER MEET 127.0.0.1 <port> for each other node
3. Wait for cluster gossip to propagate node information
4. Poll all nodes until each sees the expected member count via CLUSTER NODES
5. Ensure cluster bus is fully connected before proceeding

#### Slot Assignment Process:
1. Divide 16,384 slots evenly across primary nodes
2. Each primary executes CLUSTER ADDSLOTS <slot_range>
3. Gather cluster node IDs for replication setup
4. Check CLUSTER INFO shows cluster_slots_assigned:16384 and cluster_slots_fail:0
5. Allow slot assignments to propagate through cluster gossip

#### Replication Configuration:
1. Group nodes by shard for replication setup
2. Get primary's cluster node ID from CLUSTER NODES
3. Each replica executes CLUSTER REPLICATE <primary_cluster_id>
4. Wait for replication sync by checking INFO replication for master_link_status:up
5. Ensure all replicas are connected and synchronized

#### Final Validation Process:
1. **Cluster State**: Verify CLUSTER INFO shows cluster_state:ok
2. **Slot Coverage**: Confirm all 16,384 slots are assigned (cluster_slots_assigned:16384)
3. **Slot Health**: Ensure no failed slots (cluster_slots_fail:0)
4. **Replication Links**: Check all replicas are connected to their primaries
5. **Configuration Validation**: Verify nodes are running with expected configuration settings

#### Configuration Validation:
The orchestrator validates node configurations by querying each node's settings and comparing against expected values. This catches issues like incorrect startup parameters, ensuring proper cluster operation.

## Data Models

### Configuration Models
The orchestrator uses three primary data models:

**ClusterConfig** defines the desired cluster characteristics including number of shards, replicas per shard, port ranges, data directories, and cleanup preferences.

**NodePlan** represents the blueprint for a node before spawning, containing role assignment, port allocations, slot ranges for primaries, and master relationships for replicas.

**NodeInfo** tracks running node details including the information set in `NodePlan`, process information, file system paths, and cluster-specific identifiers needed for ongoing management and operations.

## Error Handling & Recovery

### Node Startup Failures
When nodes fail to start, the orchestrator detects process death or connection timeouts and immediately terminates all spawned processes to prevent resource leaks. This fail-fast approach ensures clean state for retry attempts and prevents partial cluster states that could interfere with subsequent tests.

### Cluster Formation Failures
- **Convergence Timeout**: Nodes fail to discover each other
- **Slot Assignment Failure**: Slots cannot be assigned to primaries
- **Replication Failure**: Replicas cannot sync with masters
- **Configuration Mismatch**: Nodes started with incorrect configuration

### Recovery Strategies
- **Automatic Retry**: Retry failed operations
- **Partial Cleanup**: Clean up only failed components, preserve working ones
- **Continuation**: Continue with reduced cluster size if possible
- **Complete Rollback**: Full cleanup and restart on critical failures
- All recovery operations will be logged

## Testing Strategy
- We will use pytests to test port allocation and release, topology planning, node configuration, and cluster formation on various inputs

<br>

This comprehensive design ensures the Cluster Orchestrator provides a robust, reliable, and scalable foundation for Valkey cluster testing while maintaining simplicity and efficiency in its core operations.