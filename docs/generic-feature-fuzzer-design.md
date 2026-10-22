# Generic Valkey feature fuzzer design

The fuzzer should treat cluster, primary–replica, and standalone as deployment
topologies rather than as separate test frameworks. A feature test then combines
one topology with configuration, workload, actions, faults, observations, and
oracles.

## Test contract

Every feature pack should answer the same questions:

1. **Where can it run?** Supported topologies, versions, roles, and build
   capabilities.
2. **How is it enabled?** Startup configuration, runtime configuration, and
   per-node differences.
3. **How is it activated?** The command or state transition that reaches the
   feature's code path.
4. **How do we prove it ran?** An `INFO` field, metric, trace, log message, or
   protocol observation. Configuration alone is not sufficient evidence.
5. **What stresses it?** Data shape, concurrency, timing boundaries, topology
   changes, and resource pressure.
6. **What can fail around it?** Process, network, disk, memory, and restart
   faults placed before, during, and after activation.
7. **What must remain true?** Data, availability, convergence, persistence, and
   protocol invariants.

This makes a new feature mostly data and small plugins instead of another
end-to-end test harness.

## Stable extension boundary

The acceptance criterion is intentionally strict: after the one-time generic
runner refactor, onboarding a feature must not require edits to the fuzzer
engine, DSL envelope or parser, CLI, topology providers, result serializer, or
reporter. It must not require adding a feature-specific enum member or switch
branch either.

There are only two supported onboarding paths:

1. **Manifest-only pack:** compose registered capability probes, workloads,
   actions, faults, observers, and oracles.
2. **Plugin-backed pack:** install a trusted Python package that registers only
   the genuinely new component behavior, then reference those namespaced
   components from the same manifest format.

Most features should use the first path. For example, a generic command action,
continuous key-value workload, `INFO` observer, log observer, counter-delta
oracle, and data-consistency oracle should be enough to describe many new
features without writing Python. A new protocol decoder or a platform-specific
fault injector may justify a plugin, but it still must not require a core
dispatch change.

The stable flow is:

```text
generic scenario envelope
          |
          v
 feature-pack catalog + opaque feature inputs
          |
          v
       RunPlan
          |
          v
 generic lifecycle runner + component registries
          |
          v
 uniform component results, evidence, and reproducer
```

The core recognizes only generic component roles, never individual Valkey
features:

- capability probe
- workload
- action
- fault
- observer
- oracle

Core liveness, crash/assertion detection, topology safety, cleanup, and
reproduction metadata remain mandatory and cannot be disabled by a pack.

## Scenario pipeline

```text
capability probe
      |
      v
topology -> node configuration -> seed workload -> activate feature
                                                  |
                                      inject fault / transition
                                                  |
                                                  v
                             path evidence + invariant oracles
                                                  |
                                                  v
                                  result bundle + reproducer
```

The important distinction is between:

- **Path evidence:** proves the intended implementation was exercised.
- **Correctness oracles:** prove it behaved correctly.

A run must have both. For example, matching data on a primary and replica is not
evidence that replication compression was negotiated; a negotiated-link field
or counter supplies that evidence.

Every attempted scenario has one terminal outcome:

- `PASS`: activation completed, all required path evidence was observed, and
  every blocking oracle passed.
- `FAIL`: the system under test violated an invariant, activation did not
  complete as required, or required path evidence was missing.
- `SKIP_UNSUPPORTED`: a capability probe established before execution that the
  requested version, build, platform, or topology cannot run the scenario.
- `ERROR`: the harness, environment, setup, observer, or fault injector failed
  in a way that prevents a trustworthy verdict.

Unsupported capability and failed evidence are deliberately different:
unsupported scenarios are skipped before mutation, while a supposedly supported
feature that does not produce required evidence fails.

## Lifecycle and synchronization

The runner should publish named lifecycle events:

```text
topology.ready
workload.seeded
workload.started
action.started
action.path_entered
fault.started
action.completed
workload.quiesced
validation.completed
```

Actions may publish more specific events such as
`bgsave.forkless_copy_started` or `replication.full_sync_started`. Faults and
observers bind to an event plus a bounded offset or deadline. This makes phase
placement replayable and avoids using arbitrary sleeps as the synchronization
contract. Missing required events are recorded explicitly and contribute to
`FAIL` or `ERROR`, depending on whether the feature or harness was responsible.

## Core extension points

### Topology provider

Owns node planning, process lifecycle, connection discovery, readiness, and
cleanup. The initial providers are:

- `standalone`: one writable node
- `replication`: one primary and one or more replicas
- `cluster`: multiple shards with optional replicas

### Feature profile

Declares capability requirements and configuration dimensions. Configuration
has three layers:

1. topology-wide `server_config`
2. role configuration such as `node_configs.primary`
3. exact-node configuration such as `node_configs.node-1`

Later layers override earlier layers. This permits homogeneous configurations
and mixed capability tests without changing the orchestrator.

Configuration keys are passed to the Valkey binary under test. A feature pack
must probe the selected build and reject or skip unsupported keys and values;
the generic topology layer cannot infer feature availability from configuration
alone.

### Artifact provider

Mixed-version and new-protocol tests require selecting a binary per node, not
only selecting server configuration. A proposed artifact layer names immutable
binaries and then assigns them globally, by role, or by exact node:

```yaml
# Proposed artifact-provider extension; the current v3 envelope does not accept it yet.
artifacts:
  old:
    binary: /opt/valkey-old/src/valkey-server
    expected_sha: abc123
  candidate:
    binary: /opt/valkey-candidate/src/valkey-server
    expected_sha: def456

topology:
  type: cluster
  artifact: candidate
  node_artifacts:
    node-0: old
```

The resolved artifact record should include the binary digest or commit,
reported version, build capabilities, platform, and relevant transport support.
Restarts and rolling upgrades must preserve or intentionally transition both
the artifact and resolved startup configuration.

### Workload

Runs continuously across the activation and fault window. Workloads should be
selectable by semantic purpose, not only command rate:

- compressible and incompressible values
- small and large objects
- key churn and expiry
- hot-key and uniform distributions
- transactions, scripts, streams, pub/sub, and blocking commands
- snapshot-sized datasets and replication backlog boundaries

The workload records an operation ledger, including operation ID, target or
routing decision, argument digest, start/end time, reply, timeout, and
acknowledgement semantics. Each scenario also declares its consistency policy:

- which acknowledged writes must survive
- how ambiguous timeout or disconnect results are reconciled
- whether expiry or asynchronous replication permits bounded loss
- the convergence deadline and any explicit loss budget

The default correctness policy should require zero loss of unambiguously
acknowledged, unexpired writes. This prevents an oracle from treating every
missing value as either corruption or permitted asynchronous loss without
knowing what the client observed.

### Action

Triggers a feature or topology transition. Actions should be registered by
name and have a deterministic parameter generator. Examples include:

- `bgsave`
- `config_set`
- `disconnect_replica`
- `reconnect_replica`
- `restart_node`
- `failover`
- `reshard`
- `rolling_upgrade`

An action may expose lifecycle hooks so a fault can be synchronized with a
specific internal phase instead of relying only on sleeps.

### Fault

Perturbs the system at a deterministic point:

- process kill, pause, and restart
- one-way or two-way network partition
- latency, packet loss, duplication, and reordering
- disk full, slow I/O, and write errors
- CPU and memory pressure

The result must record the exact target, phase, start/end time, and whether the
fault was successfully applied.

### Observer

Collects path evidence and diagnostic state:

- `INFO` sections and selected fields
- `CONFIG GET`
- cluster views from every reachable node
- logs with required and forbidden patterns
- process resource metrics
- protocol counters or packet traces

Observers should sample before, during, and after the action so transient paths
are not missed.

Evidence expectations should normally compare a baseline with later samples.
For example, a counter must advance during the action rather than merely be
nonzero from an earlier event. Each pack declares required and optional
evidence; missing required evidence cannot produce `PASS`.

### Oracle

Evaluates invariants independently of the feature implementation:

- read-after-write and dataset digest agreement
- replication offsets and link convergence
- cluster slot ownership and view convergence
- restart/reload correctness
- bounded unavailability and latency
- absence of crashes, assertions, sanitizer findings, and unexpected errors

Feature packs should also support differential and metamorphic oracles. The same
generated plan can run with a feature disabled and enabled, or against a
baseline and candidate artifact, and compare normalized command results, final
dataset digests, and topology state. Feature-specific evidence should differ in
the expected way while externally visible correctness remains equivalent.
Performance comparisons should be environment-calibrated and reported
separately from deterministic correctness checks.

## DSL foundation

Version 1 cluster scenarios remain valid. Version 2 introduces a topology-neutral
form:

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

The current foundation creates all three topologies, applies layered server
configuration, writes validation data, and runs topology-appropriate validation.
The repository also contains the isolated extension contracts, component
registries, lifecycle event journal, uniform outcomes, and trusted Python
entry-point discovery in `src/extensibility.py`. They deliberately do not yet
alter the v1/v2 execution path.

The generic pack compiler, runner, and local topology provider now have a
CLI-facing runtime. The `run` and `validate` commands dispatch v3 scenarios to
that runtime while v1/v2 remain on the legacy engine. This completes the basic
CLI slice only. Reusable built-ins, mandatory baseline safety oracles,
provenance, managed artifact/resource providers, per-node binaries, and
complete feature packs remain pending. The core v3 envelope is intentionally
small and versioned; future resources and artifacts may require an additive
extension or a later scenario API rather than feature-specific top-level
branches:

```yaml
api_version: v3
scenario_id: forkless-standalone
seed: 123

feature:
  use: valkey.forkless-save
  version: "1"
  with:
    key_count: 100000
    mutation_rate: 500

topology:
  type: standalone
  base_port: 7400
```

Explicit manifests are loaded from repeatable `--feature-pack-dir` options,
then trusted installed code is discovered from the `valkey_fuzzer.plugins`
entry-point group. Plugin discovery can be disabled:

```bash
valkey-fuzzer validate scenario.yaml \
  --feature-pack-dir ./feature-packs \
  --no-plugins

valkey-fuzzer run scenario.yaml \
  --feature-pack-dir ./feature-packs \
  --output result.json
```

Validation compiles the complete plan, runs component validation hooks, and
calls the selected topology provider's pure `validate` contract. It does not
construct component instances, provision a topology, resolve the Valkey binary,
or prove environment readiness. Provider factories, component factories, and
validators are contractually side-effect-free; validators are also
deterministic, idempotent, and re-entrant because compilation and execution
preflight may both invoke them. Execution returns the uniform `PASS`, `FAIL`,
`SKIP_UNSUPPORTED`, or `ERROR` outcome and serializes the complete plan,
component results, evidence, and diagnostics through the same generic reporter.
Plugin distribution versions, manifest digests, and tested-binary digests are
not yet included, so the report is not a complete reproducer.

The v3 parser validates only this envelope. The selected feature pack validates
the opaque `feature.with` values and compiles them into a `RunPlan`; registered
components validate their own configuration. Safe, type-preserving references
such as `{$ref: inputs.key_count}` may be used by manifests. Templates must not
execute Python, Jinja, shell, or `eval`.

This example is a configuration/topology smoke only. It does not prove that
compression was negotiated or used.

## Registry and discovery contract

Component IDs are opaque and namespaced, for example
`core.valkey.command`, `core.valkey.info`, or
`example.cluster_protocol.trace`. Feature packs are keyed by namespaced pack ID
and version.

Built-in and explicitly configured data-only manifests should be loaded into a
feature-pack catalog. External component code is discovered through the
`valkey_fuzzer.plugins` Python entry-point group. Entry-point plugins are
trusted installed code, whereas manifests remain data-only.

Discovery must:

- be deterministic
- reject duplicate component IDs and duplicate pack ID/version pairs
- reject incompatible extension API versions before topology mutation
- prevent external plugins from replacing built-in registrations
- report a broken provider without silently changing the selected test
- record pack, plugin, component, and tested-binary versions or digests in the
  reproducer

The current narrow run context exposes the immutable plan, lifecycle event bus,
deterministic per-component randomness, named topology services, a raw artifact
directory path, and a cooperative deadline. Structured node selection and
client access are currently provided through topology services. A managed
evidence/artifact store, workload ledger, cancellation token, cleanup
registration, and structured logging remain target additions. Plugins do not
import the current engine internals.

## Resources beyond Valkey nodes

Node communication is only one feature domain. Durability, data tiering,
search, modules, and externally backed features may depend on filesystems,
object stores, proxies, sidecars, resource limits, or other services.

The execution plan should therefore support lifecycle-managed resource
providers in addition to topology providers:

```text
resolve artifacts
      |
provision resources (filesystem, object store, proxy, resource envelope)
      |
provision Valkey topology using resource outputs
      |
workload, actions, faults, observers, and oracles
      |
cleanup topology, then resources in reverse order
```

Resource outputs are referenced as typed values in topology configuration and
components. Resource providers own readiness, rollback, cleanup, and immutable
reproduction metadata. This keeps storage setup out of feature-specific actions.

For durability, the generic contracts additionally need an acknowledged-write
ledger, explicit durability/loss policy, crash and storage faults, recovery
checkpoints, and post-restart oracles. For data tiering, they need tier-capacity
controls, backend latency/failure injection, residency/migration evidence, and
data/availability oracles across hot and cold tiers.

## Feature examples

The configuration names, `INFO` fields, counters, log patterns, and protocol
signals below are illustrative and version-dependent. A real pack must discover
or declare the exact evidence contract for the tested artifact and return
`SKIP_UNSUPPORTED` when that contract is unavailable.

### Forkless save

Configuration dimensions:

- `forkless-infrastructure-enabled`: `yes` and `no`
- `bgsave-default-method`: `forkless` and `fork`
- standalone, replication primary, and cluster primary
- dataset size, object count, mutation rate, and memory headroom

Activation:

- preload data
- start continuous mutations
- issue `BGSAVE`

Path evidence:

- persistence `INFO` shows the current or last background-save type as forkless
- forkless progress counters advance
- required start/completion log messages appear

Faults and transitions:

- kill or restart during the save
- cancel a save
- disconnect/reconnect replicas during a replication-triggered save
- run with memory pressure and slow disk

Oracles:

- the server remains responsive within a bound
- acknowledged data remains readable
- the generated RDB reloads successfully
- replicas converge
- no crash, assertion, or leaked in-progress state remains

### Replication compression

Configuration matrix:

- primary and replica modes: `no`, `lz4`, and supported optional codecs
- matching and mixed capabilities
- diskless and disk-based full synchronization
- dual-channel replication on and off
- fork and forkless snapshot methods
- same-version and mixed-version nodes

Activation:

- stream compressible and incompressible writes
- force full and partial synchronization
- change compression configuration while links are active

Path evidence:

- the negotiated link mode is the expected codec or plaintext fallback
- compressed/uncompressed byte counters move in the expected direction
- the full-sync path reports the selected transport

Oracles:

- primary and replicas converge byte-for-byte
- reconnect and renegotiation complete
- unsupported combinations fall back or fail in the documented way
- latency, CPU, and memory stay within campaign thresholds

### New cluster protocol

Configuration matrix:

- old-only, new-only, and mixed protocol nodes
- rolling enablement by role and shard
- same-version and mixed-version binaries

Activation and stress:

- cluster meet/handshake
- gossip under key traffic
- failover, node restart, resharding, and rolling upgrade
- message loss, delay, duplication, partition, and reconnect

Path evidence:

- protocol negotiation/version counters
- message-type counters or trace classification
- every reachable node reports the intended peer mode

Oracles:

- all healthy nodes converge on membership and slot ownership
- no split brain or duplicate ownership appears
- failover remains safe
- an incompatible peer is rejected cleanly

## Coverage model

Count tested semantic tuples, not only run count:

```text
(feature, topology, artifact/version/build-capability mix,
 platform, transport, storage mode, resource envelope, config mix,
 workload and data shape, action, fault, fault phase,
 observer evidence, oracle)
```

The scheduler should prioritize uncovered tuples and boundary values, then use
seeded randomness inside each tuple. Failures should be minimized by removing
operations, nodes, configuration dimensions, and workload intensity while
preserving both path evidence and the violated invariant.

## Implementation sequence

1. Finish topology-neutral lifecycle and configuration. **Implemented.**
2. Define the isolated feature-pack plan, terminal outcomes, component
   protocols, registries, named events, and result bundle. **Implemented.**
3. Add the manifest catalog/compiler and generic v3 envelope. **Implemented as
   a programmatic API.**
4. Add the generic lifecycle runner and local standalone/replication/cluster
   provider. **Implemented and unit tested.**
5. Wire v3 compilation/execution/result reporting into the CLI and add a legacy
   compatibility adapter. **Implemented and unit tested.**
6. Wrap current failover, process kill, and state validations as built-in
   components.
7. Build a vertical forkless-save pack for standalone: managed mutation
   workload, `BGSAVE` action, required evidence, data/RDB oracles, and one
   event-triggered process fault.
8. Generalize the working primitives and extend the forkless pack to
   replication and cluster.
9. Add resource providers and durability policies for durability and
   data-tiering packs.
10. Add per-node immutable artifacts, then implement
   replication-compression and mixed-version cluster-protocol packs.
11. Add differential execution, semantic coverage accounting,
   and scenario minimization.

The extensibility milestone is complete only when two tests pass:

1. A new data-only manifest is dropped into a configured pack directory and
   runs without modifying or branching in the engine, parser, CLI, topology
   code, or reporter.
2. A separately packaged wheel contributes a new entry-point component, a
   manifest uses it, and the same unmodified execution path runs and reports
   the scenario.

These final gates remain pending. Current in-process acceptance coverage proves
the unchanged CLI-to-plugin-to-provider-to-report path, but there are no
reusable built-in components for a manifest-only feature and no isolated
installed-wheel subprocess test yet.
