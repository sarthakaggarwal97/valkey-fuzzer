# Generic Valkey feature fuzzer design

## Goal

valkey-fuzzer provides one feature-neutral path for testing Valkey behavior in
standalone, primary–replica, cluster, and extension-defined deployments.

A feature author should be able to add a manifest or install a Python wheel,
then use the existing `valkey-fuzzer validate` and `valkey-fuzzer run` commands.
Normal onboarding must not require a feature enum, CLI command, scenario-parser
branch, topology special case, or result-format fork.

Forkless save, replication compression, durability, data tiering, and future
protocols are architecture probes, not core-owned feature packs. The framework
provides the execution seams; an extension provides the behavior and knowledge
specific to the feature being tested.

The project is intentionally greenfield:

- one strict scenario format
- one compiler and immutable plan model
- one runner lifecycle
- one extension API
- one outcome and report model

There are no public v1/v2/v3 execution paths.

## Feature test contract

Every meaningful feature extension answers seven questions:

1. **Support:** Which builds, platforms, topologies, roles, and capabilities can
   run the test?
2. **Configuration:** Which startup settings, runtime settings, artifacts,
   resources, policies, and per-node differences are required?
3. **Transition:** Which command, workload, fault, or state change exercises the
   intended implementation?
4. **Path evidence:** What independent signal proves that implementation ran?
5. **Stress:** Which data shapes, concurrency levels, timing boundaries,
   topology changes, and resource limits matter?
6. **Faults:** Which process, network, storage, memory, CPU, or dependency
   failures should be applied?
7. **Oracles:** Which externally visible invariants establish correctness?

Configuration is not path evidence. A successful read or write may prove an
outcome without proving which implementation produced it. For example, a
replication-compression extension would need a negotiation signal, trace, or
counter delta in addition to replica-data agreement.

## Execution model

```text
scenario
  │ selects feature pack + inputs + topology
  │ optionally declares artifacts + resources + policies
  ▼
strict compiler + registries
  │ validates identities, dependencies, outputs, and references
  ▼
immutable RunPlan
  │
  ├── probe and acquire artifacts/resources transactionally
  ├── materialize runtime-only references and services
  ▼
immutable BoundRunPlan
  │
  ├── run capability checks
  ├── provision the selected topology
  ├── execute registered components in dependency order
  ├── collect events and evidence
  ├── evaluate required oracles
  └── clean up in reverse ownership order
        │
        ▼
uniform outcome + report-safe reproducer data
```

The persisted plan contains declarations and typed symbolic references, never
provider handles, clients, credentials, or resolved secret values. Runtime
bindings exist only in `BoundRunPlan`.

## What core owns

Core owns stable mechanics that every extension can share:

- strict scenario and feature-pack parsing
- feature-pack selection and input validation
- immutable plans and deterministic component seeds
- component and provider registries
- dependency validation and stable execution ordering
- standalone, replication, and cluster topology providers
- plugin-defined topology support
- startup overlays by role or exact node
- artifact and resource declarations, typed output references, acquisition,
  rollback, services, and cleanup
- passive policy declaration and transport
- component events, evidence, and required-evidence checks
- uniform outcomes and JSON/YAML reporting
- trusted plugin discovery as one atomic registration transaction

Core deliberately does not interpret feature IDs, policy names, evidence
values, or domain-specific configuration.

## What an extension owns

An extension owns knowledge that cannot be inferred generically:

- capability probes for its target implementation
- feature configuration and input schema
- workloads, transitions, and feature-specific faults
- path-evidence collection and interpretation
- correctness, compatibility, or performance oracles
- any feature-specific external service or storage model
- policy semantics, including allowed data-loss or convergence windows

An extension may use only components already registered by other packages and
remain a data-only feature pack. If it needs new behavior, it installs a
trusted package through the `valkey_fuzzer.plugins` entry-point group.

External packages import author-facing contracts only from
`valkey_fuzzer.extensions`. Other package modules are framework internals.

## Scenario, pack, and component model

The scenario selects a pack and supplies its inputs:

```yaml
scenario_id: candidate-feature
seed: 202

feature:
  use: example.candidate-feature
  version: "1"
  with:
    message: exercise-this-path

topology:
  type: standalone
```

The optional top-level `artifacts`, `resources`, and `policies` sections let an
extension describe dependencies without adding feature-specific setup to the
runner. Exact provider outputs are referenced as
`{$ref: artifacts.<id>.<output>}` or
`{$ref: resources.<id>.<output>}`.

A feature pack composes configured component instances. Core recognizes six
roles:

- **capability:** reject unsupported targets before mutation
- **workload:** create initial or sustained traffic
- **action:** exercise a command or state transition
- **fault:** perturb the system or one of its dependencies
- **observer:** produce path evidence or diagnostics
- **oracle:** evaluate an invariant independently of the implementation

Component IDs are opaque and namespaced. Dependencies form a directed acyclic
graph and determine a stable execution order. Each instance gets a deterministic
random stream derived from the scenario seed and its identity.

Components can publish and observe named events as ordinary execution data.
Core intentionally has no speculative event-scheduling language or schedule
journal. If a real feature needs overlap or event-triggered fault placement, its
extension can first implement that coordination locally. A common scheduler
belongs in core only after multiple real extensions establish the same
semantics.

`mode: feature` requires at least one required oracle and one required evidence
declaration. `mode: topology_smoke` tests infrastructure and cannot be presented
as feature coverage.

## Providers and external systems

Providers are the generic boundary for systems outside component-local memory:

- **artifact providers** resolve binaries, modules, datasets, or other immutable
  inputs
- **resource providers** provision filesystems, object stores, proxies,
  sidecars, capacity envelopes, and other managed dependencies
- **topology providers** plan nodes and own topology provisioning, readiness,
  restart, rollback, and cleanup

Providers declare output and service names before execution so invalid
references fail during compilation. Acquisition follows the combined
dependency graph. Partial failure rolls back already acquired objects, and
normal cleanup runs in reverse ownership order.

Components receive collision-free `artifacts`, `resources`, and `topology`
service namespaces. Handles and service objects are runtime-only.

## Contributor template and onboarding proof

The installable extension under
[`examples/extensions/neutral-plugin/`](../examples/extensions/neutral-plugin/)
is the contributor template and the end-to-end acceptance fixture. It
registers:

- a custom no-op topology provider
- a configured action
- an independent oracle
- a `feature`-mode feature pack with required event and evidence

Its scenario passes an authored input into component execution. The test builds
and installs the template wheel, then validates and runs it through the
unchanged CLI. The final report must show the configured component, required
event, evidence, oracle, and `PASS` outcome.

That test is the foundation’s central extensibility gate. A contributor can
copy the package, rename its identities, replace the neutral behavior, and
onboard a new feature without changing core.

## Can this model test broad future features?

Yes. The framework does not need to know their names, but each extension must
bring enough evidence and oracle logic to make an honest claim.

| Feature shape | Existing generic seams | Extension-owned work |
| --- | --- | --- |
| Node communication or replication behavior | topology, startup overlays, actions, faults, events, evidence, oracles | protocol probe, traffic/fault logic, negotiation evidence, convergence oracle |
| New cluster protocol | plugin topology if needed, mixed config, node services, actions, faults, evidence | handshake/transition logic, protocol observations, membership and ownership invariants |
| Durability or recovery | policies, workloads, faults, resources, services, evidence, cleanup | operation model, acknowledgement classification, loss policy, checkpoints, recovery oracle |
| Data tiering | resource graph, capacity-limited services, faults, observers, policies | tier provider or adapter, workload distribution, residency evidence, backend-failure oracle |
| Mixed binaries or rolling transitions | artifact declarations and topology abstraction | per-node assignment/transition support until a reusable contract is justified |

Durability illustrates the boundary. A credible test may need an append-only
operation history, acknowledgement classification, recovery checkpoints, and a
loss policy. Those are essential to that test, but they are not automatically
universal core APIs. The first extension can own them. If multiple durability,
replication, or migration extensions converge on the same model, it can be
promoted into a reusable library without changing the scenario or runner.

Data tiering follows the same rule. Resource providers already supply lifecycle,
dependency, service, rollback, and cleanup seams. A tiering extension can bring
an object-store emulator or backend adapter and expose it as a resource. A
shared provider should move into the framework only when it is demonstrably
generic.

## Outcome semantics

Every attempted run has one terminal classification:

- `PASS`: all required components completed, required path evidence exists, and
  every blocking oracle passed
- `FAIL`: the system under test violated an invariant, an expected transition
  failed, or required evidence was absent
- `SKIP_UNSUPPORTED`: a capability or provider support check established before
  mutation that the target cannot run the plan
- `ERROR`: setup, environment, provider, component contract, observation, or
  harness failure prevented a trustworthy verdict

Unsupported capability is not missing evidence. A target that claims support
but does not exercise the required path fails.

Cleanup failures remain visible and can upgrade an otherwise successful run to
`ERROR`.

## Optional capabilities

The following may be valuable, but they are not prerequisites for generic
onboarding:

- reusable Valkey command, workload, fault, and safety-oracle libraries
- event-driven concurrent components and deterministic fault placement
- per-node immutable binaries and rolling upgrade helpers
- shared operation histories, checkpoints, and consistency policies
- managed filesystem, object-store, proxy, and sidecar providers
- immutable plugin, manifest, and tested-binary attestations
- differential and metamorphic execution
- campaign scheduling, semantic coverage, corpus management, and minimization

Promote one into core only when:

1. at least one real extension needs it now;
2. its contract can remain feature-neutral;
3. its lifecycle and failure semantics are understood; and
4. sharing it removes meaningful duplication without forcing unrelated
   extensions to use it.

Until then, keep it in the extension. This prevents hypothetical flexibility
from becoming permanent framework complexity.

## Foundation definition of done

The generic foundation is complete when:

1. an external wheel can atomically register a feature pack, configured
   components, and a topology provider;
2. an authored scenario reaches those components through the unchanged CLI;
3. dependencies, support decisions, events, evidence, oracles, cleanup, and
   outcomes work through the common lifecycle; and
4. the extension requires no feature-specific branch in core.

The neutral contributor template proves external onboarding, configuration,
events, evidence, oracle evaluation, topology cleanup, and the common outcome
path. Focused lifecycle tests cover dependencies, support decisions, rollback,
and non-pass outcomes. Together they prove the conditions above. Future work
should be driven by concrete feature extensions, not by adding unused framework
surfaces.
