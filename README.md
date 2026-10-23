# valkey-fuzzer

valkey-fuzzer is a topology-neutral framework for testing Valkey features in
standalone, primary–replica, cluster, and extension-defined deployments.

The project is intentionally greenfield. There is one scenario format and one
execution path: a scenario selects a feature pack, the pack compiles to an
immutable plan, and the generic runner executes registered capabilities,
workloads, actions, faults, observers, and oracles. Adding a feature must not
require a feature-specific CLI command or a switch in the core runner.

The framework owns test mechanics, not Valkey feature implementations.
Forkless save, replication compression, durability, data tiering, and possible
future protocols are architecture stress cases only; they are not planned
core-owned packs. Feature authors bring the feature-specific manifest,
configuration, probes, actions, evidence, and oracles. Core supplies stable
onboarding contracts and reusable, feature-neutral services.

## Project status

The lean generic foundation is complete for extension development:

- strict, duplicate-free YAML scenarios and feature-pack manifests
- exact feature-pack selection and input validation
- deterministic component seeds and dependency ordering
- standalone, replication, and cluster topology providers, plus trusted-plugin
  registration for entirely new topology types
- startup configuration overlays by role or exact node
- optional artifact, resource, and policy declarations in the compiled plan
- static provider, dependency-graph, declared-output, and reference validation
- component, artifact, resource, topology, and feature-pack registrations
  discovered atomically through trusted Python plugins in
  `valkey_fuzzer.plugins`
- symbolic environment references and provider results that exclude runtime
  values from serialization; extension-supplied configs and report fields must
  remain non-secret
- transactional artifact/resource acquisition with support checks, rollback,
  reverse cleanup, immutable runtime-only `BoundRunPlan` materialization, and
  namespaced artifact/resource/topology services
- uniform `PASS`, `FAIL`, `SKIP_UNSUPPORTED`, and `ERROR` outcomes
- component evidence, lifecycle events, structural validation, and cleanup
- an installable, feature-neutral contributor extension that registers a
  configured component, feature pack, and topology provider and runs through
  the unchanged CLI
- JSON or YAML result bundles

The repository deliberately does **not** ship a core-owned Valkey feature pack.
The manifest under `examples/feature-packs/` is an honest topology smoke: it
proves that the generic compiler and topology provider can be composed, but it
does not claim to exercise a Valkey feature. Feature implementations belong in
extension-owned manifests or plugins; adding a core-owned pack is not a
framework milestone.

The foundation intentionally stops here. Event-driven concurrency, mixed
binaries, common durability ledgers, managed object stores, reusable Valkey
components, differential execution, semantic coverage, and minimization are
optional libraries or future framework capabilities. Add one only when a real
extension needs it and the resulting abstraction is reusable; none is required
to onboard a feature today.

## The model

```text
scenario
  │ selects pack + inputs + topology
  │ optionally declares artifacts + resources + policies
  ▼
compiler + component/environment/topology registries
  │ statically validates dependencies, outputs, and symbolic references
  │
  ▼
immutable RunPlan                         (implemented)
  │
  ├── acquire artifacts/resources transactionally      (implemented)
  ├── materialize runtime-only bindings
  ▼
immutable BoundRunPlan                    (implemented)
  │
  ├── probe pre-topology capabilities
  ├── provision Valkey topology
  ├── run workloads/actions/faults/observers
  ├── evaluate evidence and oracles
  └── cleanup topology/resources/artifacts in reverse order
        │
        ▼
uniform outcome + reproducer metadata
```

A trustworthy feature run needs both:

- **path evidence**, proving that the intended implementation ran; and
- **correctness oracles**, proving that externally visible invariants held.

For example, an extension testing replication compression could not treat
matching primary and replica data as proof that compression was negotiated. It
would also need a protocol field, trace, or counter delta. This is an
illustration of the evidence contract, not a planned built-in compression pack.

## Installation

Requirements:

- Python 3.9+
- a Valkey build when executing a built-in Valkey-backed topology

```bash
git clone <repository-url>
cd valkey-fuzzer
python -m pip install -e .
valkey-fuzzer --help
```

For development:

```bash
python -m pip install -e '.[dev]'
pytest
```

## Scenario format

There is no `api_version` selector. A scenario has one strict shape:

```yaml
scenario_id: standalone-topology-smoke
seed: 101

feature:
  use: example.topology-smoke
  version: "1"
  with: {}

topology:
  type: standalone
  base_port: 7400
  enable_cleanup: true
```

The accepted top-level fields are `scenario_id`, `seed`, `feature`, `topology`,
`artifacts`, `resources`, and `policies`. The last three are optional. Unknown
fields and duplicate YAML keys are rejected.

Artifact and resource entries use `use`, optional `with`, and optional
`depends_on`; policy entries use `use` and optional `with`. Dependencies name
exact declarations, while `$ref` values name exact provider outputs. With the
named pack and providers registered, this complete scenario can be validated or
executed:

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

Authored environment bindings always use exact
`{$ref: artifacts.<id>.<output>}` or
`{$ref: resources.<id>.<output>}` mappings. The compiler converts each binding
to a typed reference. Canonical `RunPlan` data and JSON encode that type with
the reserved `{$valkey_fuzzer_ref: artifacts.<id>.<output>}` or
`{$valkey_fuzzer_ref: resources.<id>.<output>}` marker; scenario authors do not
write that internal marker.

A reusable pack can expose an explicit binding slot through its existing input
schema:

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

The scenario supplies that slot through `feature.with`, just like any other
pack input:

```yaml
feature:
  use: example.reusable-pack
  with:
    candidate_binary: {$ref: artifacts.candidate.path}
```

`reference_kinds` is optional and accepts the exact namespaces `artifacts`
and/or `resources`. Inputs without `type: environment_reference` reject typed
environment references, including references nested inside object or array
inputs.

The input-schema language is a deliberately small, strict subset rather than a
general JSON Schema implementation. The root supports `type: object`,
`properties`, `required`, and boolean `additionalProperties`; each top-level
property supports `type`, `default`, `enum`, `minimum`, and `maximum`, plus
`reference_kinds` for `type: environment_reference`. At the root, `title`,
`description`, and `$schema` are accepted as metadata; property rules also
accept `title` and `description`. Object and array inputs are checked only for
their container type: nested `properties`, `items`, composition keywords,
patterns, and length constraints are not implemented. Use a component or
provider validator for deeper, extension-owned configuration checks.

If any template-capable value intentionally contains an ordinary dictionary
that looks like a template or environment reference, escape it:

```yaml
payload:
  $valkey_fuzzer_literal:
    $ref: "#/components/schemas/Request"
```

The `{$valkey_fuzzer_literal: <value>}` wrapper must be the entire mapping at
its location, with no sibling keys. It works in `feature.with`, topology,
artifact/resource/policy configuration, pack component configuration, and
startup overlays. The compiler unwraps its content as ordinary opaque data;
when used in `feature.with`, the pack's input schema still validates that
unwrapped value.

The compiler verifies provider registration, cross-kind dependency cycles,
declared output names, and references from environment declarations, topology,
component configuration, and startup overlays. Environment references remain
symbolic and typed in the persisted `RunPlan`; provider output values, runtime
handles, and services are excluded from report-safe serialization.

Execution acquires declared artifacts and resources in dependency order,
materializes every typed reference into a runtime-only `BoundRunPlan`, performs
mandatory post-bind topology and component validation, and unwinds components,
topology, resources, and artifacts in ownership order. Policies are transported
as ordered opaque bound context; core does not dispatch on policy names or claim
to enforce their semantics. Extension components and oracles decide how to
consume them.

`feature.version` is a feature-pack release identity, not a scenario-schema
generation. It may be omitted only when exactly one version of the selected
pack is registered. Pin it for repeatable campaigns. Reports currently record
the resolved pack version. Extensions that need stronger supply-chain
reproducibility may also add manifest, plugin, and tested-binary attestations.

The local topology provider accepts:

- `type`: `standalone`, `replication`, or `cluster`
- `num_shards` and `replicas_per_shard`
- `base_port`, `base_data_dir`, `valkey_binary`, and `enable_cleanup`
- topology-wide `server_config`
- role or exact-node `node_configs`

Configuration precedence is topology-wide, then role, then exact node. Feature
packs can add ordered startup overlays on top of the scenario topology.

## Feature-pack format

A feature pack is data. It composes generic registered behavior:

```yaml
pack_id: example.command-feature
version: "1"
mode: feature
supported_topologies: [standalone, replication]

input_schema:
  type: object
  additionalProperties: false
  properties:
    key_count:
      type: integer
      minimum: 1
      default: 1000

startup_overlays:
  - overlay_id: enable-feature
    selector:
      roles: [primary]
    server_config:
      example-feature-enabled: "yes"

components:
  - kind: capability
    component_id: core.valkey.config-capability
    instance_id: capability
    config:
      option: example-feature-enabled

  - kind: workload
    component_id: core.valkey.keyspace-workload
    instance_id: traffic
    depends_on: [capability]
    config:
      key_count: {$ref: inputs.key_count}

  - kind: action
    component_id: example.feature.activate
    instance_id: activate
    depends_on: [traffic]

  - kind: observer
    component_id: example.feature.path-observer
    instance_id: path
    depends_on: [capability]

  - kind: oracle
    component_id: core.valkey.data-oracle
    instance_id: correctness
    depends_on: [activate]

required_events:
  - action.completed

required_evidence:
  - evidence_id: example.feature.path-entered
    source: path
```

The IDs above are illustrative rather than promised built-ins. A manifest is
executable only when every referenced component is registered. Dependencies
determine the stable execution order. Components may publish and consume domain
events, but the core does not impose a speculative concurrency or
event-scheduling model.

`mode: feature` requires at least one required oracle and one required evidence
declaration. `mode: topology_smoke` is reserved for infrastructure checks and
must not be reported as feature coverage.

Use a data-only manifest when existing components are sufficient. A separately
installed Python package may register genuinely new behavior—such as a protocol
decoder or platform-specific fault injector—through the
`valkey_fuzzer.plugins` entry-point group. Plugins are trusted executable code;
use `--no-plugins` when only explicit manifests should be considered.

Artifact, resource, and topology provider registrations use the same public
extension boundary. They may be supplied directly to a `FeatureRuntime` or by a
trusted plugin; plugin discovery commits component, pack, and every provider
registration atomically. A plugin-defined topology is selected through the
same `topology.type` field and does not require a CLI or runner branch.

### Contributor extension template

[`examples/extensions/neutral-plugin/`](examples/extensions/neutral-plugin/)
is the canonical starting point for an extension. It is a real installable
wheel, imports only the stable `valkey_fuzzer.extensions` facade, and registers
a no-op topology provider, a configured action, an oracle, and a feature pack.
Its scenario passes an input into the action and exercises the unchanged
`validate` and `run` commands without requiring a Valkey binary.

```bash
python -m pip install -e examples/extensions/neutral-plugin
valkey-fuzzer validate examples/extensions/neutral-plugin/scenario.yaml
valkey-fuzzer run examples/extensions/neutral-plugin/scenario.yaml
```

A separately packaged extension declares an entry point in the
`valkey_fuzzer.plugins` group. Its entry-point name must exactly match
`PluginManifest.plugin_id`; the target may be a manifest object or a
zero-argument callable returning one. Copy the template, rename its identities,
replace its neutral action and oracle, and keep feature-specific behavior out of
the core package.

Component factories are zero-argument and construct in-memory state; the
returned object implements the selected role (`run` for an action, for example).
An optional component validator has signature `(spec, plan)`. Artifact,
resource, and topology provider factories are also zero-argument. Artifact and
resource registrations may declare exact `outputs` and runtime `services`, a
pure symbolic `validator(spec)`, and a pure
`resolved_config_validator(spec, resolved_config)`. Artifact providers implement
`check_support`, `resolve`, `rollback`, and `cleanup`; resource providers use
`provision` in place of `resolve` and return `ProvisionedResource`. Topology
providers implement pure `validate`, followed by `provision`, `rollback`, and
`cleanup`, and each registration carries a replay identity. Support checks and
validators must not mutate external state. Provider event details, provenance,
and every component-supplied summary, evidence value, diagnostic, and event
payload must remain non-secret.

During execution, components receive three collision-free service namespaces:

```python
artifact_service = context.services["artifacts"]["candidate"]["launcher"]
resource_service = context.services["resources"]["helper"]["client"]
topology_service = context.services["topology"]["valkey.topology"]
```

Only declared provider dependencies are visible while another provider is
being acquired. Component code sees the complete acquired environment plus
topology services. These are runtime capabilities, not report data: do not
serialize or retain them after the component call. The built-in local topology
service exposes immutable process-free node descriptors, `current_nodes()`,
`execute_command(node_id, ...)`, and `restart(node_id)`. It deliberately does
not export raw reconnectable clients or process handles; cleanup rejects new
operations and waits for operations already in flight.

## CLI

Validate a scenario and its resolved plan without provisioning:

```bash
valkey-fuzzer validate examples/standalone_scenario.yaml \
  --feature-pack-dir examples/feature-packs \
  --no-plugins
```

Validation checks the scenario, selected pack, component and provider
registrations, both dependency graphs, declared provider outputs, symbolic
environment references, startup overlays, and pure topology preflight. It does
not instantiate component, artifact, or resource provider factories, resolve or
execute the Valkey binary, create directories, open sockets, or prove
environment readiness. It does create a fresh topology-provider instance and
invoke its pure `validate` method; topology-provider constructors and validation
therefore must not acquire external state.

Run the example topology smoke:

```bash
valkey-fuzzer run examples/standalone_scenario.yaml \
  --feature-pack-dir examples/feature-packs \
  --no-plugins \
  --valkey-binary /absolute/path/to/valkey-server \
  --artifact-dir ./artifacts \
  --output result.json
```

Other examples use the same path:

```bash
valkey-fuzzer validate examples/replication_scenario.yaml \
  --feature-pack-dir examples/feature-packs --no-plugins

valkey-fuzzer validate examples/cluster_scenario.yaml \
  --feature-pack-dir examples/feature-packs --no-plugins
```

Important options:

- `--feature-pack-dir DIR`: add an explicit manifest directory; repeatable
- `--no-plugins`: disable installed plugin discovery
- `--valkey-binary PATH`: override the binary in the topology document
- `--timeout SECONDS`: cooperative execution deadline
- `--artifact-dir DIR`: root for component artifacts and provider-scoped
  lifecycle directories; intentionally not a managed artifact store
- `--output PATH --format json|yaml`: save the complete outcome
- `--allow-unsupported`: treat `SKIP_UNSUPPORTED` as a successful matrix cell

Exit codes:

- `0`: pass, or an allowed unsupported target
- `1`: the feature contract failed
- `2`: compiler, provider, runner, or reporting error
- `3`: required capability unsupported
- `130`: interrupted

### Random cluster fuzzing

The `cluster` command runs the randomized cluster fuzzer (failovers plus
process chaos, followed by replication, slot, topology, view, data, and log
validation). The scheduled and PR fuzzer workflows use it, and a seed
reproduces the same scenario:

```bash
valkey-fuzzer cluster --random --valkey-binary /absolute/path/to/valkey-server
valkey-fuzzer cluster --seed 42 --iterations 10
valkey-fuzzer cluster --random --export-dsl scenario.yaml --output results.json
valkey-fuzzer cluster --dsl examples/simple_failover.yaml
valkey-fuzzer validate examples/simple_failover.yaml
```

Cluster DSL files use the `cluster`, `operations`, `chaos`, and
`state_validation` sections shown in `examples/simple_failover.yaml` and
`examples/example_scenario.yaml`; `validate` routes any file without a
top-level `feature` to this format. The command exits `0` when every iteration
passes and `1` otherwise. Node logs (`<seed>-<node>.log`) and captured
process output (`<seed>-<node>.stdio.log`) are written under
`/tmp/valkey-fuzzer/logs`.

Before starting a node, the orchestrator checks that its client port (and, for
cluster topologies, its bus port) is free and refuses to run against a server
it did not start. Nodes run with a `maxmemory` cap of 500mb and Valkey's default
`noeviction` policy; override either through `server_config`.

## Programmatic use

`examples/run_fuzzer.py` demonstrates the same runtime used by the CLI:

```bash
python examples/run_fuzzer.py validate examples/standalone_scenario.yaml
python examples/run_fuzzer.py run examples/standalone_scenario.yaml \
  --valkey-binary /absolute/path/to/valkey-server
```

## Adding a feature

1. Define the feature’s supported topologies and capability probes.
2. Identify the action or transition, path evidence, stress dimensions, faults,
   and independent invariants.
3. Reuse generic components wherever possible.
4. Add only irreducible behavior as namespaced component or provider
   registrations.
5. Compose the behavior in a manifest, define inputs with the supported schema
   subset, and add declarative environment inputs only when needed.
6. Add focused acceptance tests that prove unsupported targets skip before
   mutation, the path evidence is real, and safety oracles catch violations.
7. Record enough provenance to replay the exact pack, plugin, binary, inputs,
   seed, topology, and fault placement.

The engine, parser, CLI, topology providers, and result serializer should not
change during normal feature onboarding.

## Stateful recovery and external resources

Stateful recovery, durability, and data-tiering extensions can already declare
policies, provision external resources, bind provider outputs, run faults and
oracles, and retain evidence. An extension may initially own its operation
history, checkpoints, storage emulator, or consistency model. If several real
extensions need the same contract, that proven implementation can graduate
into a reusable feature-neutral library.

The core supplies the lifecycle seams: declarations, providers, symbolic
references, static graph validation, transactional acquisition,
`BoundRunPlan` materialization, namespaced services, rollback, and reverse
cleanup. It does not prescribe a durability model or tiering backend. See
[`docs/generic-feature-fuzzer-design.md`](docs/generic-feature-fuzzer-design.md)
for the architecture and the boundary between foundation and optional
capabilities.

## Contributing

1. Fork the repository.
2. Create a feature branch.
3. Add generic framework infrastructure or an extension-owned scenario/plugin,
   with focused acceptance coverage.
4. Run `pytest` and `ruff check .`.
5. Submit a pull request describing the exercised path, oracles, and
   reproduction contract.

## License

BSD 3-Clause License.
