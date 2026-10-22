# Generic Valkey feature fuzzer design

## Objective

valkey-fuzzer should test an open-ended set of Valkey features without becoming
a collection of feature-specific harnesses. Standalone, primary–replica, and
cluster are deployment topologies. Forkless save, replication compression,
future cluster protocols, durability, and data tiering are feature packs
composed on top of the same execution model.

The project is greenfield. There is one strict scenario document, one compiler,
one run plan, one lifecycle runner, and one result model. The public format has
no schema-generation router and no alternate execution branch.

The normal onboarding contract is:

1. write a data-only manifest using reusable components; or
2. add a small trusted plugin for behavior that is genuinely new, then compose
   it from the same manifest format.

Onboarding a feature must not require a feature enum, CLI subcommand, parser
branch, topology special case, or result-format fork.

## Feature test contract

Every pack must answer:

1. **Eligibility:** Which topologies, artifacts, platforms, roles, and build
   capabilities can run it?
2. **Enablement:** Which startup settings, runtime settings, resources, and
   per-node differences are required?
3. **Activation:** Which command or transition enters the feature path?
4. **Path evidence:** What independent signal proves the path ran?
5. **Stress:** Which data shapes, concurrency levels, timing boundaries,
   topology changes, and resource limits matter?
6. **Faults:** Which process, network, storage, memory, CPU, or dependency
   failures should occur before, during, or after activation?
7. **Oracles:** Which data, availability, convergence, durability, persistence,
   and protocol invariants must hold?

Configuration is not path evidence. Successful reads are not necessarily path
evidence either. A compression pack, for example, must observe negotiation or
counter changes in addition to verifying replica correctness.

## One execution graph

```text
scenario intent
      │
      ├── pack identity + inputs
      ├── topology intent
      ├── artifact bindings             target
      ├── managed resource bindings     target
      └── policy/coverage intent         target
      │
      ▼
strict compiler + complete preflight
      │
      ▼
immutable RunPlan
      │
      ├── resolve immutable artifacts
      ├── provision resources
      ├── provision topology from resource/artifact outputs
      ├── probe capabilities before mutation
      ├── start observers and workloads
      ├── arm event-triggered faults
      ├── run actions and transitions
      ├── quiesce workloads and observers
      ├── evaluate evidence and oracles
      └── cleanup topology, resources, and artifacts in reverse order
      │
      ▼
uniform outcome + evidence + complete reproducer
```

The currently implemented runner covers strict compilation, component
registries, dependency-ordered lifecycle phases, the local topology lifecycle,
evidence requirements, uniform outcomes, rollback, and cleanup. Artifact
resolution, managed resources, a full event-driven scheduler, policy/ledger
services, and complete provenance remain target work.

## Scenario and identity model

The only authored scenario envelope is:

```yaml
scenario_id: replication-compression-candidate
seed: 202

feature:
  use: valkey.replication-compression
  version: "2026.10"
  with:
    codec: lz4
    value_shape: compressible

topology:
  type: replication
  replicas_per_shard: 1
  base_port: 7500
```

The compiler rejects unknown top-level fields, missing required fields,
duplicate YAML keys, invalid inputs, unavailable packs, incompatible
topologies, missing components, and unresolved overlay resources before
topology mutation.

There is deliberately no scenario `api_version`. These identities solve
different problems:

- **feature-pack version:** authored release identity used to select exact pack
  behavior; implemented
- **manifest content digest:** immutable hash of the loaded pack; pending
- **plugin distribution version/digest:** executable extension provenance;
  pending
- **tested artifact digest/commit/build metadata:** system-under-test
  provenance; pending
- **internal plugin contract compatibility:** an implementation boundary, not
  a user-selectable scenario language

The scenario may omit `feature.version` only when one version of that pack is
registered. Reproducible campaigns should pin it. A complete reproducer should
ultimately record both human-readable releases and immutable digests.

## Feature-pack model

A pack declares:

- namespaced `pack_id` and pack `version`
- `feature` or `topology_smoke` trust mode
- supported topologies
- a strict input schema
- ordered startup overlays
- configured component instances and dependencies
- required lifecycle events
- required path evidence
- metadata used by coverage and reporting

The six core component roles are:

- **capability:** determine support before mutation
- **workload:** create seed state or sustained client activity
- **action:** activate a feature or change topology state
- **fault:** perturb the system at a deterministic phase
- **observer:** collect path evidence and diagnostic state
- **oracle:** evaluate invariants independently of feature implementation

Component IDs are opaque and namespaced. The core never switches on individual
feature IDs.

`feature` mode cannot pass without a required oracle and required evidence.
`topology_smoke` mode is useful for infrastructure acceptance but is not
feature coverage.

## Registries and plugins

Built-in registrations and separately packaged plugins populate one registry
per component role. Data-only manifests reference those registrations.
External code is discovered through the `valkey_fuzzer.plugins` Python
entry-point group and is trusted executable code.

Discovery and compilation must:

- be deterministic
- reject duplicate component identities
- reject duplicate pack ID/version pairs
- prevent plugins from replacing built-ins
- fail atomically if one plugin registration is invalid
- reject incompatible internal contracts before topology mutation
- pin the resolved registry environment for validation and execution
- record pack, plugin, component, and artifact identities in results

Explicit manifest directories are loaded in caller order and files within each
directory are loaded deterministically.

## Plans, dependencies, and scheduling

Compilation resolves pack inputs and safe `{$ref: inputs.name}` substitutions
into an immutable `RunPlan`. Templates are data-only; Python, Jinja, shell, and
`eval` are forbidden.

Component dependencies form a directed acyclic graph. Dependencies express
semantic readiness, not timing by themselves. Every component receives an
independent deterministic random stream derived from the scenario seed,
component instance ID, and stream name so adding one component does not
perturb another component’s randomness.

The final scheduler should react to committed lifecycle events:

```text
topology.ready
workload.seeded
workload.started
action.started
feature.path-entered
fault.started
action.completed
workload.quiesced
validation.completed
```

An action may publish feature-specific events such as
`bgsave.copy-started` or `replication.full-sync-started`. Faults and observers
should bind to an event, deterministic offset, and deadline. Arbitrary sleeps
are not a synchronization contract.

The current runner records named events and supports dependency-ordered
lifecycle phases, but the general event-driven scheduler and concurrent
placement semantics are pending.

## Topology provider

A topology provider owns:

- node planning and identity
- process lifecycle
- readiness and client discovery
- restart behavior
- rollback after partial provisioning
- cleanup after a successful provision

The local provider implements:

- `standalone`: one writable node
- `replication`: one primary and one or more replicas
- `cluster`: multiple shards with optional replicas

Configuration layers are:

1. topology-wide `server_config`
2. role-specific configuration
3. exact-node configuration
4. ordered feature-pack startup overlays

Later layers override earlier ones unless an overlay declares conflict-as-error.
This supports homogeneous and mixed settings without topology-specific feature
logic.

Structural validation is intentionally side-effect-free. It checks plans,
component registrations, validators, provider shape, node dimensions, ports,
selectors, and configuration without starting Valkey or creating external
resources.

## Artifact provider target

New protocols and mixed-version behavior require immutable per-node artifacts:

```yaml
artifacts:
  baseline:
    source: /opt/valkey-baseline/src/valkey-server
    expected_sha256: "..."
  candidate:
    source: /opt/valkey-candidate/src/valkey-server
    expected_sha256: "..."

topology:
  type: cluster
  artifact: candidate
  node_artifacts:
    node-0: baseline
```

This is a target shape, not an accepted scenario field today.

An artifact provider should resolve and attest binaries before resource or
topology setup. Its immutable record should include digest, source commit,
reported version, build flags/capabilities, platform, and relevant protocol
support. Restarts and rolling upgrades must preserve or deliberately transition
both artifact and resolved configuration.

## Resource provider target

Durability, data tiering, modules, and externally backed features need more than
Valkey processes. They may require filesystems, object stores, proxies,
sidecars, cgroups, capacity limits, or faultable dependencies.

```text
artifact resolver
      │
      ▼
resource graph
  ├── filesystem / block device
  ├── object store / emulator
  ├── network proxy
  ├── sidecar / module dependency
  └── CPU, memory, or I/O envelope
      │ typed outputs
      ▼
topology provider ── component services
```

Resource declarations and typed outputs are also target work, not accepted
scenario fields today.

Each resource provider must own validation, provisioning, readiness, immutable
provenance, fault hooks, rollback, and idempotent cleanup. A resource graph
allows dependencies between resources and supports reverse-order release. Pack
actions must not hide shell setup for storage or services.

## Workload and operation ledger target

Workloads should describe semantic purpose, not only request rate:

- compressible and incompressible values
- small and large objects
- hot-key and uniform distributions
- key churn and expiry
- transactions, scripts, streams, pub/sub, and blocking commands
- snapshot-sized datasets and replication backlog boundaries
- tiering working sets larger than each storage tier

Every client operation should enter an append-only ledger with:

- operation and client ID
- logical key/routing decision
- argument digest and expected model transition
- start/end monotonic time
- reply, timeout, disconnect, or error
- acknowledgement classification
- topology epoch and relevant fault/event sequence

A correctness policy must define which acknowledged writes survive, how
ambiguous outcomes are reconciled, which asynchronous-loss windows are allowed,
expiry semantics, convergence deadlines, and explicit loss budgets.

The default should require zero loss of unambiguously acknowledged, unexpired
writes. Without a ledger and policy, a durability verdict is not trustworthy.
The current runtime has evidence aggregation but not this operation ledger.

## Evidence and oracle model

Observers may collect:

- selected `INFO` and `CONFIG GET` values
- cluster views from every reachable node
- required or forbidden log patterns
- process and resource metrics
- protocol counters, negotiated modes, or traces
- file/object-store manifests and tier residency

Evidence expectations should usually compare a baseline and a later sample.
A counter being nonzero before the action does not prove the current run used
the path.

Oracles may evaluate:

- read-after-write and dataset model agreement
- replica offsets and convergence
- cluster membership, slot ownership, and view convergence
- restart/reload correctness
- bounded unavailability and latency
- absence of crashes, assertions, sanitizer reports, and unexpected errors
- persistence guarantees under the declared durability policy
- hot/cold-tier correctness and backend failure behavior

Mandatory core safety oracles should run for every feature pack and must not be
disableable by pack authors. That baseline set is still pending.

Differential and metamorphic execution should compare the same normalized plan
with a feature disabled/enabled or a baseline/candidate artifact. Feature
evidence should differ as expected while externally visible behavior remains
equivalent.

## Outcome semantics

Every attempted run has one terminal classification:

- `PASS`: activation and required components completed, required path evidence
  exists, and every blocking oracle passed
- `FAIL`: the system under test violated an invariant, required activation
  failed, or expected evidence was absent
- `SKIP_UNSUPPORTED`: a capability probe established before mutation that the
  selected target cannot run the plan
- `ERROR`: setup, environment, provider, component contract, observation, or
  harness failure prevents a trustworthy verdict

Unsupported capability is not missing evidence. Unsupported targets skip before
mutation; a target that claims support but fails to produce evidence fails.

Cleanup failures must be retained in diagnostics and can upgrade an otherwise
successful run to an error. Results should include attempted and committed
events, all component results, evidence, cleanup diagnostics, and the resolved
plan.

## Feature sketches

The names below are illustrative and build-dependent. A real pack must probe
the exact target and skip unsupported combinations before mutation.

### Forkless save

Matrix:

- standalone, replication primary, and cluster primary
- fork and forkless methods
- dataset size, object count, mutation rate, and memory headroom

Activation and evidence:

- preload and continuously mutate data
- issue `BGSAVE`
- observe the selected save path, progress, and completion

Faults and oracles:

- kill/restart or cancel during the save
- slow/full storage and memory pressure
- maintain responsiveness and acknowledged data
- reload the generated RDB and converge replicas
- detect crashes, assertions, leaks, and stuck save state

### Replication compression

Matrix:

- supported codecs and plaintext
- compressible and incompressible streams
- diskless and disk-based full sync
- full and partial sync
- fork and forkless snapshot methods
- matching and mixed artifacts/capabilities

Evidence and oracles:

- observe negotiated transport and counter deltas
- verify exact replica convergence
- reconnect and renegotiate
- test documented fallback/rejection for incompatible peers
- record performance separately from deterministic correctness

### New cluster protocol

Matrix and transitions:

- old-only, new-only, and mixed protocol nodes
- meet/handshake, gossip, failover, restart, reshard, rolling upgrade
- loss, delay, duplication, partition, and reconnect

Evidence and oracles:

- observe negotiation and message-type counters/traces
- verify every healthy node’s peer-mode view
- converge membership and slot ownership
- prevent split brain and duplicate ownership
- reject incompatible peers cleanly

### Durability

Required framework services:

- acknowledged-operation ledger and explicit loss policy
- fsync/persistence configuration matrix
- process, storage, and power-like failure models
- crash-consistent checkpoints and restart sequencing
- recovery-time, data-survival, and file-integrity oracles

The current framework can host durability components, but without the ledger,
managed storage resources, and policy-aware recovery oracle it cannot make a
complete durability claim.

### Data tiering

Required framework services:

- managed capacity-limited local and remote tiers
- controllable backend latency, errors, outages, and corruption signals
- hot/cold working-set generation and access-distribution changes
- residency, migration, eviction, and recall evidence
- correctness and availability oracles across both tiers

The generic component roles are sufficient, but the resource graph and these
providers/oracles are pending.

## Coverage and campaigns

Coverage should count semantic tuples rather than raw run totals:

```text
(feature pack + digest,
 topology and node-role layout,
 artifact/build/capability mix,
 platform and transport,
 resource envelope and storage mode,
 configuration mix,
 workload/data shape,
 action/transition,
 fault and event placement,
 observed path evidence,
 oracle set and policy)
```

A campaign scheduler should prioritize uncovered tuples and boundary values,
then use seeded randomness within a tuple. Failure minimization should remove
operations, nodes, configuration dimensions, and workload intensity while
preserving both the path evidence and violated invariant.

Campaign scheduling, semantic coverage persistence, and minimization are
pending.

## Implementation sequence

1. **Single-path foundation — implemented.** Strict scenario compiler,
   feature-pack catalog, component contracts/registries, immutable plans,
   deterministic seeds, lifecycle events, uniform outcomes, generic reporter,
   and local topology provider.
2. **Public cutover — implemented in the greenfield branch.** `run` and
   `validate` use only the generic runtime; examples and CI no longer expose a
   separate compatibility command.
3. **Built-in vertical slice.** Wrap reusable command, keyspace workload,
   process-fault, observation, crash detection, and data-validation primitives.
   Deliver a complete standalone forkless-save pack.
4. **Event-driven lifecycle.** Add bounded event subscriptions, deterministic
   offsets, cancellation, concurrent long-lived components, and cleanup
   registration. Extend forkless coverage to replication and cluster.
5. **Provenance and artifacts.** Add managed result artifacts, manifest/plugin
   digests, tested-binary attestations, and per-node artifact assignment.
6. **Resources and policies.** Add the resource graph, operation ledger,
   durability/loss policies, storage faults, and recovery checkpoints.
7. **Feature expansion.** Build replication-compression, mixed-version cluster
   protocol, durability, and data-tiering packs using the same engine.
8. **Campaign intelligence.** Add differential execution, semantic coverage,
   corpus persistence, and failure minimization.

## Extensibility gates

The architecture is credible only when these end-to-end tests pass:

1. A new data-only manifest dropped into a configured directory runs using only
   reusable built-ins, without core code changes.
2. A separately packaged wheel registers a new component, and an unchanged
   CLI/compiler/runner executes and reports it.
3. A mixed-artifact scenario assigns immutable binaries per node and survives a
   rolling transition without provider special cases.
4. A durability pack uses the generic ledger, policy, resource, and recovery
   services without pack-local infrastructure code.
5. A data-tiering pack provisions and faults an external tier through resource
   providers without core feature dispatch.

The repository currently has in-process coverage for the generic
CLI-to-plugin-to-provider-to-report path. Reusable built-ins, an isolated-wheel
acceptance test, and gates 3–5 remain pending.
