# valkey-fuzzer

valkey-fuzzer is a topology-neutral framework for testing Valkey features in
standalone, primary–replica, and cluster deployments.

The project is intentionally greenfield. There is one scenario format and one
execution path: a scenario selects a feature pack, the pack compiles to an
immutable plan, and the generic runner executes registered capabilities,
workloads, actions, faults, observers, and oracles. Adding a feature must not
require a feature-specific CLI command or a switch in the core runner.

## Project status

The generic foundation is usable for extension development:

- strict, duplicate-free YAML scenarios and feature-pack manifests
- exact feature-pack selection and input validation
- deterministic component seeds and dependency ordering
- standalone, replication, and cluster topology providers
- startup configuration overlays by role or exact node
- trusted Python plugin discovery through `valkey_fuzzer.plugins`
- uniform `PASS`, `FAIL`, `SKIP_UNSUPPORTED`, and `ERROR` outcomes
- component evidence, lifecycle events, structural validation, and cleanup
- JSON or YAML result bundles

The repository does **not** yet ship a complete built-in feature pack. The
manifest under `examples/feature-packs/` is an honest topology smoke: it proves
that the generic compiler and topology provider can be composed, but it does
not claim to exercise a Valkey feature.

The following major pieces remain:

- reusable built-in components and real forkless/replication-compression packs
- an event-driven scheduler for precise fault and observer placement
- mandatory baseline crash, liveness, and data-safety oracles
- a managed artifact store and complete binary/plugin/manifest provenance
- per-node immutable binaries for mixed-version tests
- lifecycle-managed filesystems, object stores, proxies, and sidecars
- an acknowledged-operation ledger and explicit durability/loss policies
- storage and resource faults, differential execution, coverage accounting,
  and failure minimization

Until those exist, CI runs structural and unit acceptance checks rather than
presenting a configuration smoke as feature fuzzing.

## The model

```text
scenario
  │ selects pack + inputs + topology
  ▼
feature-pack catalog ── trusted plugin registrations
  │
  ▼
immutable RunPlan
  │
  ├── resolve artifacts                         (target)
  ├── provision managed resources               (target)
  ├── provision Valkey topology                 (implemented locally)
  ├── probe capabilities
  ├── run workloads/actions/faults/observers
  ├── evaluate evidence and oracles
  └── cleanup topology/resources in reverse order
        │
        ▼
uniform outcome + reproducer metadata
```

A trustworthy feature run needs both:

- **path evidence**, proving that the intended implementation ran; and
- **correctness oracles**, proving that externally visible invariants held.

For example, matching data on a primary and replica does not prove replication
compression was negotiated. A protocol field, trace, or counter delta must
provide that evidence.

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

The accepted top-level fields are `scenario_id`, `seed`, `feature`, and
`topology`. Unknown fields and duplicate YAML keys are rejected.

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

## CLI

Validate a scenario and its resolved plan without provisioning:

```bash
valkey-fuzzer validate examples/standalone_scenario.yaml \
  --feature-pack-dir examples/feature-packs \
  --no-plugins
```

Validation checks the scenario, selected pack, component registrations,
component validators, dependency graph, startup overlays, and pure topology
preflight. It does not instantiate component factories, resolve or execute the
Valkey binary, create directories, open sockets, or prove environment
readiness.

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
- `--artifact-dir DIR`: expose a path to components; not yet a managed store
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
4. Add only irreducible behavior as namespaced plugin registrations.
5. Compose the behavior in a manifest and define a strict input schema.
6. Add focused acceptance tests that prove unsupported targets skip before
   mutation, the path evidence is real, and safety oracles catch violations.
7. Record enough provenance to replay the exact pack, plugin, binary, inputs,
   seed, topology, and fault placement.

The engine, parser, CLI, topology providers, and result serializer should not
change during normal feature onboarding.

## Durability and data tiering

The component model can express their actions, observers, and oracles, but the
resource and correctness foundations are not complete.

Durability needs an acknowledged-operation ledger, an explicit loss/ambiguity
policy, crash and storage faults, recovery checkpoints, and post-restart
oracles. Data tiering additionally needs managed capacity-limited backends,
latency/failure injection, residency and migration evidence, and correctness
checks spanning hot and cold tiers.

These are framework concerns, not pack-local shell setup. The target design
introduces lifecycle-managed resource providers whose outputs feed topology
provisioning and components, with rollback on partial setup and reverse-order
cleanup. See
[`docs/generic-feature-fuzzer-design.md`](docs/generic-feature-fuzzer-design.md)
for the full architecture and implementation sequence.

## Contributing

1. Fork the repository.
2. Create a feature branch.
3. Add implementation and acceptance coverage.
4. Run `pytest` and `ruff check .`.
5. Submit a pull request describing the exercised path, oracles, and
   reproduction contract.

## License

BSD 3-Clause License.
