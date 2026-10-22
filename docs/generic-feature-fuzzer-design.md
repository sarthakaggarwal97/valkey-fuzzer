# Generic Valkey feature fuzzer design

## Objective

valkey-fuzzer should test an open-ended set of Valkey features without becoming
a collection of feature-specific harnesses. Standalone, primary–replica, and
cluster are deployment topologies. Forkless save, replication compression,
durability, data tiering, and possible future protocols are stress cases used
to test whether the extension model is broad enough. They are not planned
core-owned feature packs.

The project is greenfield. There is one strict scenario document, one compiler,
one run plan, one lifecycle runner, and one result model. The public format has
no schema-generation router and no alternate execution branch.

The normal onboarding contract is:

1. write a data-only manifest using reusable components; or
2. add a small trusted plugin for behavior that is genuinely new, then compose
   it from the same manifest format.

Onboarding a feature must not require a feature enum, CLI subcommand, parser
branch, topology special case, or result-format fork.

Core owns the scenario language, compiler, registries, execution lifecycle,
topology and environment contracts, evidence transport, policy declaration and
transport contracts, reporting, and feature-neutral test primitives. An
extension owns feature configuration, capability probes, activation, path
evidence, domain-specific faults, policy semantics, and oracles. No roadmap item
should require the core project to implement a particular Valkey feature before
the framework can be considered usable.

## Feature test contract

Every feature extension must answer:

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
evidence either. An extension testing compression, for example, must observe
negotiation or counter changes in addition to verifying replica correctness.
That example tests the abstraction; it is not a commitment to ship a
compression implementation in core.

## One execution graph

```text
scenario intent
      │
      ├── pack identity + inputs
      ├── topology intent
      ├── artifact declarations          accepted
      ├── managed resource declarations accepted
      └── policy intent                  accepted
      │
      ▼
strict compiler + complete static preflight
      │
      ▼
immutable RunPlan with symbolic environment references
      │
      ├── validate providers, outputs, references, and dependency graph
      ├── resolve immutable artifacts                     implemented
      ├── provision resources transactionally             implemented
      ▼
immutable BoundRunPlan with runtime-only bindings          implemented
      │
      ├── probe capabilities before topology mutation
      ├── provision topology from resource/artifact outputs
      ├── run dependency-ordered component phases          implemented
      ├── event-triggered concurrent placement             target
      └── cleanup topology, resources, and artifacts in reverse order
      │
      ▼
uniform outcome + evidence + currently available reproducer metadata
```

The compiler now accepts optional artifact, resource, and policy declarations,
preserves exact environment references symbolically, and validates provider
selection, declared outputs, reference consumers, and the combined dependency
graph without mutation. Artifact and resource provider contracts and
registries also exist.

The runner acquires artifacts and resources transactionally, materializes typed
references into a runtime-only `BoundRunPlan`, repeats topology and component
validation against resolved values, exposes immutable `artifacts`, `resources`,
and `topology` service namespaces, and cleans up in reverse ownership order.
Support denial becomes `SKIP_UNSUPPORTED` only after a clean unwind. Policies
are ordered opaque context on the bound plan: core transports them but never
dispatches on their names or asserts that their semantics were enforced.

## Scenario and identity model

The only authored scenario envelope is:

```yaml
scenario_id: candidate-feature-replication
seed: 202

feature:
  use: example.candidate-feature
  version: "2026.10"
  with:
    workload_profile: sustained-writes
    feature_mode: enabled

topology:
  type: replication
  replicas_per_shard: 1
  base_port: 7500
```

The accepted top-level fields are `scenario_id`, `seed`, `feature`, `topology`,
`artifacts`, `resources`, and `policies`; the last three are optional. The
compiler rejects unknown fields, missing required fields, duplicate YAML keys,
invalid inputs, unavailable packs, incompatible topologies, missing components,
unknown providers, dependency cycles, and invalid environment references
before topology mutation.

Environment declarations use one neutral shape:

```yaml
# Illustrative: the named providers must come from an installed trusted plugin.
scenario_id: environment-wiring-check
seed: 202

feature:
  use: example.topology-smoke
  version: "1"
  with: {}

artifacts:
  candidate:
    use: example.local-artifact
    with:
      source: /opt/candidate/valkey-server

resources:
  helper:
    use: example.managed-helper
    depends_on: [artifacts.candidate]
    with:
      executable: {$ref: artifacts.candidate.path}

policies:
  run-budget:
    use: example.run-budget
    with:
      endpoint: {$ref: resources.helper.endpoint}

topology:
  type: standalone
  valkey_binary: {$ref: artifacts.candidate.path}
```

Artifact and resource entries accept `use`, optional `with`, and optional
`depends_on`. Policy entries accept `use` and optional `with`; their
environment edges are inferred from references because policies have no
provider lifecycle. Dependencies use exact `artifacts.<id>` or
`resources.<id>` paths. References use exact
`artifacts.<id>.<output>` or `resources.<id>.<output>` paths.

Authored bindings use exact `{$ref: artifacts.<id>.<output>}` or
`{$ref: resources.<id>.<output>}` mappings. Compilation converts them to typed
references. Canonical plan data and JSON persist the typed form as
`{$valkey_fuzzer_ref: artifacts.<id>.<output>}` or
`{$valkey_fuzzer_ref: resources.<id>.<output>}`. The
`$valkey_fuzzer_ref` key is an internal serialization marker, not authored
scenario syntax.

A reusable feature pack opts into environment binding through an ordinary
`feature.with` input; there is no parallel bindings envelope:

```yaml
input_schema:
  type: object
  properties:
    candidate_binary:
      type: environment_reference
      reference_kinds: [artifacts]

components:
  - kind: action
    component_id: example.action
    instance_id: exercise
    config:
      binary: {$ref: inputs.candidate_binary}
```

A scenario can then set
`feature.with.candidate_binary: {$ref: artifacts.candidate.path}`. The optional
`reference_kinds` constraint accepts only `artifacts` and/or `resources`.
Ordinary input rules continue to reject typed environment references, including
nested references.

The current input-schema language is a strict top-level subset, not general
JSON Schema. The root supports `type: object`, `properties`, `required`, and
boolean `additionalProperties`. A property supports `type`, `default`, `enum`,
`minimum`, and `maximum`, with `reference_kinds` reserved for
`type: environment_reference`. At the root, `title`, `description`, and
`$schema` are accepted as metadata; property rules also accept `title` and
`description`. Object and array rules validate only the container type. Nested
`properties`, `items`, composition keywords, patterns, and length constraints
require extension-owned component or provider validation.

Any template-capable authored value that intentionally contains an ordinary
`$ref` dictionary must use the literal escape:

```yaml
payload:
  $valkey_fuzzer_literal:
    $ref: "#/components/schemas/Request"
```

The `{$valkey_fuzzer_literal: <value>}` marker must contain no sibling keys.
Its content is ordinary opaque data in feature inputs, topology, environment
configuration, component configuration, and startup overlays. When the marker
appears in `feature.with`, the unwrapped value is still checked against the
feature pack's input schema.

Typed references may remain in environment configuration, topology
configuration, component configuration, and startup overlays. Persisted
declarations must not contain credentials. Provider output values, runtime
handles, and services are runtime-only and excluded from serialization; only
output names and explicitly report-safe provenance may be recorded.

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
- an input schema using the documented strict subset
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
per component role plus registries for artifact, resource, and topology
providers.
Registrations may be added directly to a `FeatureRuntime` or supplied by
external code discovered through the `valkey_fuzzer.plugins` Python entry-point
group. Plugins are trusted executable code.

Discovery and compilation must:

- be deterministic
- reject duplicate component identities
- reject duplicate artifact, resource, or topology provider identities
- reject duplicate pack ID/version pairs
- prevent plugins from replacing built-ins
- fail atomically across components, packs, and providers if one plugin
  registration is invalid
- reject incompatible internal contracts before topology mutation
- pin the resolved registry environment for validation and execution
- record available pack, component, provider, and artifact declaration
  identities in results; immutable plugin and tested-artifact provenance remains
  pending

Explicit manifest directories are loaded in caller order and files within each
directory are loaded deterministically.

The README's “Minimal trusted plugin” section gives the concrete packaging and
code shape. External plugins import author-facing contracts only from
`valkey_fuzzer.extensions`; other `valkey_fuzzer` modules are framework
internals rather than extension APIs.
The entry-point name must equal `PluginManifest.plugin_id`. Component and
provider factories are zero-argument; component validators receive
`(spec, plan)`, environment-provider static validators receive `(spec)`, and
post-binding validators receive `(spec, resolved_config)`. Artifact providers
implement `check_support`, `resolve`, `rollback`, and `cleanup`; resource
providers implement the same lifecycle with `provision` replacing `resolve`.
Topology providers implement pure `validate`, followed by `provision`,
`rollback`, and `cleanup`.

## Plans, dependencies, and scheduling

Compilation resolves pack inputs and safe `{$ref: inputs.name}` substitutions
into an immutable `RunPlan`. Templates are data-only; Python, Jinja, shell, and
`eval` are forbidden. Exact artifact and resource references deliberately
remain typed symbolic bindings until the runtime acquisition phase; canonical
plan serialization uses the reserved `$valkey_fuzzer_ref` marker rather than
the authored `$ref` spelling.

Component dependencies form a directed acyclic graph. Dependencies express
semantic readiness, not timing by themselves. Every component receives an
independent deterministic random stream derived from the scenario seed,
component instance ID, and stream name so adding one component does not
perturb another component’s randomness.

Artifact and resource declarations form a second, cross-kind directed acyclic
graph. The compiler validates explicit `depends_on` edges, dependencies implied
by configuration references, selected provider registrations, and every
referenced provider output. This is pure preflight: it does not instantiate a
provider, call support checks, acquire an artifact, or provision a resource.

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

Topology providers are first-class plugin registrations rather than a
constructor-only runner option. Each topology ID resolves to a fresh-provider
factory with an API version and immutable replay identity. The compiler records
that identity in the `RunPlan`; validation and execution reject registry or
implementation drift before trusting the run. A plugin can therefore introduce
a new topology document and lifecycle without adding a topology switch to the
CLI, compiler, or runner.

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

## Artifact declarations and providers

Artifact declarations and their immutable `ArtifactSpec` plan records are
implemented. Each declaration selects a registered provider, carries
JSON/YAML-safe configuration, and may depend on other artifacts or resources.
Providers declare output names up front, so a reference such as
`artifacts.candidate.path` can be rejected during compilation if `path` is not
available.

The artifact provider contract defines side-effect-free support probing,
resolution, rollback after failed resolution, cleanup after success, declared
runtime services, and result validation. Providers can be registered directly
or through the trusted plugin transaction. Framework-owned plan and result
serialization omits resolved output values, handles, and services. Extensions
must not copy sensitive values into provenance, events, component summaries,
evidence, or diagnostics, because those extension-owned fields are reportable.

The acquisition manager invokes these contracts in stable dependency order,
pins selected registrations, rolls back failed acquisition, and retains
successful results only for the lifetime of the owning environment session.
Complete artifact attestations, managed result storage, per-node artifact
assignment, and rolling transitions remain target work. Mixed-build and
protocol-transition scenarios are architecture stress cases, not core feature
deliverables.

## Resource declarations and providers

Some feature extensions need more than Valkey processes. Durability and data
tiering are useful stress cases, but the same requirement applies to modules
and any externally backed feature. Extensions may require filesystems, object
stores, proxies, sidecars, cgroups, capacity limits, or faultable
dependencies.

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

Resource declarations, immutable `ResourceSpec` records, exact output
references, the combined dependency graph, provider contracts, provider
registries, and result records that exclude runtime output values, services,
and handles are implemented. Extension-supplied configurations, provenance,
events, and other report fields must remain non-secret. A resource provider
owns support probing, provisioning, rollback, cleanup, report provenance, and
any runtime-only outputs or services it exports.

Transactional graph acquisition now probes before mutation, provisions in
stable dependency order, rolls back partial setup, creates an immutable
runtime-only `BoundRunPlan`, and releases topology, resources, and artifacts in
reverse ownership order. Pack actions must not hide shell setup for storage or
services.

## Policy declarations

Policy declarations are accepted as immutable intent with `use` and `with`.
Their references participate in static target and output validation. Policies
are materialized in declaration order and exposed through the bound plan. This
is transport, not enforcement: core does not interpret policy types or claim
their semantics were applied. Generic or extension-owned components and oracles
consume policies without turning policy names into core feature dispatch.

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
The current code has standalone operation-ledger and checkpoint primitives, but
they are not yet bound into run execution, reports, or policy evaluation.

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

## Architecture stress cases, not core deliverables

The sketches below are requirements probes for the generic contracts. They are
not implementation commitments, planned built-in packs, or framework-owned
feature test suites. A feature owner may implement one as an external manifest
or plugin; that extension must probe the exact target and skip unsupported
combinations before mutation.

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

An extension cannot make a complete durability claim without these generic
services. The feature-specific persistence configuration, failure semantics,
and recovery oracle remain extension-owned.

### Data tiering

Required framework services:

- managed capacity-limited local and remote tiers
- controllable backend latency, errors, outages, and corruption signals
- hot/cold working-set generation and access-distribution changes
- residency, migration, eviction, and recall evidence
- correctness and availability oracles across both tiers

The generic resource graph and lifecycle contracts belong in the framework.
Concrete tier providers, residency interpretation, and feature oracles remain
extension-owned.

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
3. **Declarative environment preflight — implemented.** Accept optional
   artifact, resource, and policy intent; preserve symbolic references; validate
   providers, declared outputs, and the cross-kind graph; register providers
   directly or through the atomic plugin transaction.
4. **Transactional environment materialization — implemented.** Provider
   support probes, stable acquisition order, rollback, reverse cleanup,
   immutable namespaced runtime services, passive policy transport, and an
   immutable runtime-only `BoundRunPlan`.
5. **Generic onboarding surface.** Add feature-neutral command, workload,
   process-fault, observation, crash-detection, and data-validation primitives.
   Prove data-only onboarding with neutral acceptance fixtures rather than a
   core-owned Valkey feature pack.
6. **Event-driven lifecycle.** Add bounded event subscriptions, deterministic
   offsets, cancellation, concurrent long-lived components, and cleanup
   registration. Prove timing and overlap semantics with synthetic components.
7. **Provenance and state services.** Add managed result artifacts,
   manifest/plugin digests, tested-binary attestations, and per-node artifact
   assignment.
   Integrate the existing operation-ledger and checkpoint primitives and add
   configurable consistency/loss policies and generic resource faults.
8. **Extension acceptance.** Test an unchanged core against a
   data-only manifest, an independently packaged plugin, per-node artifact
   bindings, a lifecycle-managed dependency, and a stateful recovery extension.
   Keep every fixture feature-neutral or extension-owned.
9. **Campaign intelligence.** Add differential execution, semantic coverage,
   corpus persistence, and failure minimization.

## Extensibility gates

The architecture is credible only when these end-to-end tests pass:

1. A new data-only manifest dropped into a configured directory runs using only
   reusable built-ins, without core code changes.
2. A separately packaged wheel registers a new component, and an unchanged
   CLI/compiler/runner executes and reports it.
3. A mixed-artifact scenario assigns immutable binaries per node and survives a
   rolling transition without provider special cases.
4. An extension uses the generic ledger, policy, resource, and recovery
   services to express its own state-survival contract without pack-local
   infrastructure setup.
5. An extension provisions, observes, and faults an external dependency through
   resource providers without core feature dispatch.
6. A plugin introduces new configuration, events, evidence, and an oracle
   without changing the scenario parser, scheduler, topology provider, or
   result schema.

The repository currently covers the generic CLI/component path, static
environment compilation, direct and plugin provider registration (including
topology providers), atomic discovery failure, provider/output/reference
validation, transactional acquisition, `BoundRunPlan` materialization, passive
policy transport, service namespace isolation, rollback, teardown precedence,
and an isolated-wheel test that adds a topology and feature pack through the
unchanged CLI. Reusable built-ins, per-node artifact assignment, and the
end-to-end portions of gates 3–6 remain pending.
