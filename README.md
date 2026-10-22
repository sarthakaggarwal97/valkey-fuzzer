# valkey-fuzzer

valkey-fuzzer is a topology-neutral framework for testing Valkey features in
standalone, primary–replica, and cluster deployments.

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

The generic foundation is usable for extension development:

- strict, duplicate-free YAML scenarios and feature-pack manifests
- exact feature-pack selection and input validation
- deterministic component seeds and dependency ordering
- standalone, replication, and cluster topology providers
- startup configuration overlays by role or exact node
- optional artifact, resource, and policy declarations in the compiled plan
- static provider, dependency-graph, declared-output, and reference validation
- artifact and resource providers registered directly or through trusted Python
  plugins in `valkey_fuzzer.plugins`
- symbolic environment references and provider results that exclude runtime
  values from serialization; extension-supplied configs and report fields must
  remain non-secret
- transactional artifact/resource acquisition with support checks, rollback,
  reverse cleanup, immutable runtime-only `BoundRunPlan` materialization, and
  namespaced artifact/resource/topology services
- uniform `PASS`, `FAIL`, `SKIP_UNSUPPORTED`, and `ERROR` outcomes
- component evidence, lifecycle events, structural validation, and cleanup
- append-only operation-ledger and recovery-checkpoint primitives
- JSON or YAML result bundles

The repository deliberately does **not** ship a core-owned Valkey feature pack.
The manifest under `examples/feature-packs/` is an honest topology smoke: it
proves that the generic compiler and topology provider can be composed, but it
does not claim to exercise a Valkey feature. Feature implementations belong in
extension-owned manifests or plugins; adding a core-owned pack is not a
framework milestone.

The following major pieces remain:

- reusable feature-neutral components and onboarding acceptance fixtures
- an event-driven scheduler for precise fault and observer placement
- mandatory baseline crash, liveness, and data-safety oracles
- a managed artifact store and complete binary/plugin/manifest provenance
- per-node immutable binaries for mixed-version tests
- reusable policy-consuming components and lifecycle-managed filesystem, object
  store, proxy, and sidecar providers
- runner integration for the operation ledger and checkpoints, plus
  configurable consistency/loss policies
- storage and resource faults, differential execution, coverage accounting,
  and failure minimization

Until those exist, CI runs structural and unit acceptance checks rather than
presenting a configuration smoke as feature fuzzing. Framework acceptance
should use neutral fixtures that prove onboarding behavior, not make claims
about a particular Valkey feature.

## The model

```text
scenario
  │ selects pack + inputs + topology
  │ optionally declares artifacts + resources + policies
  ▼
compiler + component/provider registries
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
- a Valkey build when executing a topology

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
the resolved pack version; content digests for manifests, plugins, and tested
binaries are still pending.

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

The IDs above illustrate the intended built-in/plugin boundary; those
components are not shipped yet. A manifest is executable only when every
referenced component is registered.

`mode: feature` requires at least one required oracle and one required evidence
declaration. `mode: topology_smoke` is reserved for infrastructure checks and
must not be reported as feature coverage.

Use a data-only manifest when existing components are sufficient. A separately
installed Python package may register genuinely new behavior—such as a protocol
decoder or platform-specific fault injector—through the
`valkey_fuzzer.plugins` entry-point group. Plugins are trusted executable code;
use `--no-plugins` when only explicit manifests should be considered.

Artifact and resource provider registrations use the same internal extension
contract. They may be added directly to a `FeatureRuntime` registry or supplied
by a trusted plugin; plugin discovery commits component, pack, and provider
registrations atomically.

### Minimal trusted plugin

A separately packaged extension declares one entry point. Its entry-point name
must exactly match `PluginManifest.plugin_id`; the target may be a manifest
object or a zero-argument callable returning one:

```toml
[project.entry-points."valkey_fuzzer.plugins"]
"example.neutral" = "neutral_extension:plugin"
```

This neutral example registers one action and one artifact provider without
adding a feature-specific core path:

```python
from src.environment import (
    ArtifactProviderRegistration,
    ResolvedArtifact,
    SupportDecision,
)
from src.extensibility import (
    EXTENSION_API_VERSION,
    ComponentKind,
    ComponentRegistration,
    ComponentResult,
    ComponentStatus,
    PluginManifest,
)


class MarkerAction:
    def run(self, context, spec):
        return ComponentResult(
            component_instance_id=spec.instance_id,
            status=ComponentStatus.PASS,
            summary="neutral action completed",
        )


class ConfiguredArtifact:
    def check_support(self, context, spec, resolved_config):
        return SupportDecision.allow()

    def resolve(self, context, spec, resolved_config):
        return ResolvedArtifact(
            artifact_id=spec.artifact_id,
            provider_id=spec.provider_id,
            outputs={"path": resolved_config["path"]},
            provenance={"source": "configured-path"},
        )

    def rollback(self, context, spec):
        pass

    def cleanup(self, context, spec, artifact):
        pass


def validate_artifact_config(spec, resolved_config):
    path = resolved_config.get("path")
    if not isinstance(path, str) or not path:
        raise ValueError("path must resolve to a non-empty string")


def plugin():
    return PluginManifest(
        plugin_id="example.neutral",
        api_version=EXTENSION_API_VERSION,
        registrations=(
            ComponentRegistration(
                component_id="example.marker-action",
                kind=ComponentKind.ACTION,
                api_version=EXTENSION_API_VERSION,
                factory=MarkerAction,
            ),
        ),
        artifact_providers=(
            ArtifactProviderRegistration(
                provider_id="example.configured-artifact",
                api_version=EXTENSION_API_VERSION,
                factory=ConfiguredArtifact,
                outputs=("path",),
                resolved_config_validator=validate_artifact_config,
            ),
        ),
    )
```

A data-only pack can reference `example.marker-action`, and a scenario can
select `example.configured-artifact`; neither requires a core-code change.

Component factories are zero-argument and construct in-memory state; the
returned object implements the selected role (`run` for an action, for example).
An optional component validator has signature `(spec, plan)`. Artifact and
resource provider factories are also zero-argument. They may declare exact
`outputs` and runtime `services`, a pure symbolic `validator(spec)`, and a pure
`resolved_config_validator(spec, resolved_config)`. Artifact providers implement
`check_support`, `resolve`, `rollback`, and `cleanup`; resource providers use
`provision` in place of `resolve` and return `ProvisionedResource`. Support
checks and validators must not mutate external state. Provider event details,
provenance, and every component-supplied summary, evidence value, diagnostic,
and event payload must remain non-secret.

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
not instantiate component or provider factories, resolve or execute the Valkey
binary, create directories, open sockets, or prove environment readiness.

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
  lifecycle directories; not yet a managed store
- `--output PATH --format json|yaml`: save the complete outcome
- `--allow-unsupported`: treat `SKIP_UNSUPPORTED` as a successful matrix cell

Exit codes:

- `0`: pass, or an allowed unsupported target
- `1`: the feature contract failed
- `2`: compiler, provider, runner, or reporting error
- `3`: required capability unsupported
- `130`: interrupted

## Programmatic use

`examples/run_fuzzer.py` demonstrates the same runtime used by the CLI:

```bash
python examples/run_fuzzer.py validate examples/standalone_scenario.yaml
python examples/run_fuzzer.py run examples/standalone_scenario.yaml \
  --valkey-binary /absolute/path/to/valkey-server
```

## Adding a feature

1. Define the feature’s supported topologies and capability probes.
2. Identify activation, path evidence, stress dimensions, faults, and
   independent invariants.
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

Some extensions need stronger generic services than command execution and
process topology. Stateful recovery tests need an operation ledger, explicit
consistency/loss policy, crash and storage faults, recovery checkpoints, and
post-restart oracle hooks. Extensions backed by another storage or network
service need lifecycle-managed, capacity-limited resources, fault injection,
and evidence spanning Valkey and the dependency.

The core should provide those contracts without encoding durability, data
tiering, or any other feature. A feature extension supplies its own
configuration, resources, evidence interpretation, and domain oracle.
Declaration models, provider contracts, provider registries, symbolic
references, static graph validation, transactional acquisition, `BoundRunPlan`
materialization, namespaced topology/component services, rollback after partial
acquisition, and reverse-order cleanup now exist. Reusable providers and
components that apply particular policy semantics remain extension or future
generic-library work. See
[`docs/generic-feature-fuzzer-design.md`](docs/generic-feature-fuzzer-design.md)
for the full architecture and implementation sequence.

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
