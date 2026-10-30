# Extension guide

This guide is for people who add tests: a data-only experiment, a new
component, a provider, or a plugin that ships them. The formats are in
[Scenarios and experiments](scenarios-and-experiments.md), the existing
components in [Valkey components](components.md), and the framework's
structure in [Architecture](architecture.md).

- [Choose what to write](#choose-what-to-write)
- [Design the test](#design-the-test)
- [Write a data-only experiment](#write-a-data-only-experiment)
- [Write a component](#write-a-component)
- [The local deployment service](#the-local-deployment-service)
- [Write a provider](#write-a-provider)
- [Write a plugin](#write-a-plugin)
- [Testing](#testing)
- [Plugin trust](#plugin-trust)

## Choose what to write

| You need | Write | Python |
| --- | --- | --- |
| a new combination of existing workloads, actions, faults, observers, and oracles | a [data-only experiment](#write-a-data-only-experiment) | none |
| behavior no component has (a new command sequence, a new fault, a new invariant) | a [component](#write-a-component), shipped in a plugin | yes |
| an input that must be acquired or set up before the run (a binary, a filesystem, a service) | an artifact or resource [provider](#write-a-provider) | yes |
| a system under test the local deployment cannot start | a deployment [provider](#write-a-provider) | yes |

A test for this repository goes in `valkey_fuzzer_experiments/`: the
experiment in `valkey_fuzzer_experiments/experiments/`, its components in the
module for their role, and an example scenario in `examples/valkey/`. A test
that lives elsewhere goes in its own plugin.

## Design the test

Answer these before you write YAML:

1. **Support.** Which builds, hosts, and deployments can run it? Encode build
   and host needs as capability probes, so an unsupported target is
   `SKIP_UNSUPPORTED`, not `FAIL`, and deployment needs as `deployment:`
   rules, which reject an unsuitable scenario when it compiles.
2. **Configuration.** Which server settings, inputs, artifacts, and
   resources does it need?
3. **Transition.** Which command, workload, or fault exercises the code you
   want to test?
4. **Path evidence.** What independent signal shows that code ran: an INFO
   field, a counter delta, a log line? Configuration alone does not show it.
   Mark that evidence `required: true`.
5. **Oracles.** Which externally visible invariant establishes correctness:
   no acknowledged write lost, a clean reload, a converged deployment?

Then plan a **negative control**: a variant where the feature is configured
but not exercised, or the oracle gets a known-bad input, and the experiment
must fail. It shows the evidence and oracles can detect a problem.

## Write a data-only experiment

A data-only experiment uses only registered components and needs no Python.
This one restarts a replica under writes and requires that the replica syncs
again and that no acknowledged write is lost:

```yaml
# experiments/example.replica-restart.yaml
extends: [valkey.base]
id: example.replica-restart
deployment:
  modes: [standalone]
  replicas_per_primary: {minimum: 1}
metadata:
  description: Restart a replica under writes and check that it catches up
inputs:
  properties:
    run_before_s: {type: number, minimum: 0, maximum: 60, default: 1}
components:
  - use: valkey.observer.log
    id: logs
    config:
      instances: replicas
      expect:
        - evidence_id: example.replica.synced-logged
          pattern: "sync: Finished with success|Successful partial resynchronization"
          required: true

  - use: valkey.fault.process-signal
    id: restart-replica
    config:
      when_event: run-before.completed
      instance: replica
      signal: SIGTERM
      restart_mode: immediate

  - use: valkey.action.sleep
    id: run-before
    depends_on: [writes, logs, restart-replica]
    config: {seconds: {$ref: inputs.run_before_s}}

  - use: valkey.action.poll
    id: link-up
    depends_on: [run-before]
    config:
      instances: replicas
      section: replication
      field: master_link_status
      predicate: {equals: up}
      timeout_s: 60
required_events: [restart-replica.recovered]
required_evidence:
  - {evidence_id: valkey.fault.injected, source: restart-replica}
```

How the pieces fit:

- `extends: [valkey.base]` adds the `writes` workload, the `data` ledger
  oracle, the `deployment-state` oracle, and the `liveness` oracle. The
  actions depend on `writes`, so writes continue through the restart.
- `deployment.replicas_per_primary: {minimum: 1}` rejects a scenario without
  a replica at compile time.
- The fault's trigger is installed before the actions start, and the fault
  fires inside the dispatch of `run-before.completed`, so the restart lands
  at the same point in every run. `restart_mode: immediate` restarts the
  replica before `link-up` runs.
- The `logs` expectation is marked `required: true`, so the run fails if the
  replica never logs a new sync. That is the path evidence.
- `required_events` and `required_evidence` make the run fail if the restart
  never happened.

A scenario for it:

```yaml
# replica_restart.yaml
scenario_id: replica-restart
seed: 5
experiment:
  id: example.replica-restart
  inputs: {run_before_s: 2}
deployment:
  mode: standalone
  replicas_per_primary: 1
  base_port: 7700
```

Load the experiment from its directory with `--experiment-dir`:

```bash
valkey-fuzzer validate replica_restart.yaml --experiment-dir experiments --verbose
valkey-fuzzer run replica_restart.yaml --experiment-dir experiments --verbose
```

`validate --verbose` lists every component of the execution plan, which
shows what the fragments added. An experiment loaded from a directory gets
the `liveness` oracle only through its fragments; extend `valkey.liveness`
directly if you extend nothing else. Without it, a `valkey.observer.log`
`forbidden` match is recorded but fails nothing.

Faults that must fire at an exact moment can trigger on a log line: a
`valkey.observer.log-trigger` observer publishes an event for every matching
line, and a fault with `instance: event` targets the instance that logged it.
`valkey.cluster-election-durability` uses this to kill a primary right after
its "Failover auth granted" line.

## Write a component

A component is a Python class with the lifecycle methods of its role. Each
method returns a `ComponentResult`, usually built with `make_result`.

| Role | Methods | Runs |
| --- | --- | --- |
| capability | `probe(context, spec)` | before the deployment starts; `SKIP` means unsupported |
| workload | `start(context, spec)`, `stop(context, spec)` | from start until teardown |
| observer | `start(context, spec)`, `stop(context, spec)` | from start until teardown |
| fault | `inject(context, spec)`, `recover(context, spec)` | `inject` installs the trigger and the fault is injected when it fires; `recover` undoes it at teardown and fails if it never fired |
| action | `run(context, spec)` | once, in dependency order |
| oracle | `evaluate(context, spec, evidence)` | after teardown, with all evidence recorded so far |

This action increments a key and records the result; the oracle checks it:

```python
from valkey_fuzzer.extensions import (
    ComponentRegistration,
    ComponentRole,
    ComponentStatus,
    ConfigField,
    ConfigSchema,
    Declarations,
    EvidenceDeclaration,
    evidence_of,
    make_evidence,
    make_result,
)

TOUCH_CONFIG = ConfigSchema({"key": ConfigField(str), "count": ConfigField(int, default=1, minimum=1)})
CHECK_CONFIG = ConfigSchema({"expect": ConfigField(int, minimum=1)})
VALUE = ConfigSchema({"value": ConfigField(int)})
# What Touch publishes and emits, with schemas; the runner rejects anything else.
TOUCH_DECLARES = Declarations(
    events={"{id}.touched": VALUE},
    evidence={"example.touched": EvidenceDeclaration(ConfigField(dict, schema=VALUE))},
)


class Touch:
    def run(self, context, spec):
        config = spec.config  # checked, with defaults filled in, by the compiler
        deployment = context.services["deployment"]["valkey.deployment"]
        client = deployment.new_client("valkey-0")
        try:
            for _ in range(config["count"]):
                value = client.incr(config["key"])
        finally:
            client.close()
        context.event_bus.publish(f"{spec.id}.touched", source=spec.id, payload={"value": value})
        evidence = make_evidence(spec, "example.touched", "action.completed", {"value": value})
        return make_result(spec, ComponentStatus.PASS, f"{config['key']} is {value}", evidence=[evidence])


class CheckTouched:
    def evaluate(self, context, spec, evidence):
        seen = [item.value["value"] for item in evidence_of(evidence, "example.touched")]
        if seen and seen[-1] == spec.config["expect"]:
            return make_result(spec, ComponentStatus.PASS, f"value is {seen[-1]}")
        return make_result(spec, ComponentStatus.FAIL, f"expected {spec.config['expect']}, saw {seen}")


REGISTRATIONS = (
    ComponentRegistration(
        "example.action.touch", ComponentRole.ACTION, Touch, config=TOUCH_CONFIG, declares=TOUCH_DECLARES
    ),
    ComponentRegistration(
        "example.oracle.touched", ComponentRole.ORACLE, CheckTouched, config=CHECK_CONFIG, declares=Declarations()
    ),
)
```

A `ComponentRegistration` has:

| Field | Meaning |
| --- | --- |
| `component_id` | `<namespace>.<role>.<name>`: lowercase segments, words joined by hyphens, and the middle segment equal to the role. The catalog rejects anything else. |
| `role`, `factory` | The `ComponentRole`, and a zero-argument callable that builds the component. |
| `config` | A `ConfigSchema` of `ConfigField`s. The compiler checks the config with it, rejects unknown and missing fields, fills in defaults, and records the result in the plan; the component reads `spec.config`. Without a schema the config is taken as written. |
| `check` | Optional `check(config, spec, plan)` for cross-field rules that need the resolved config or the plan; raise `ConfigError` to reject. |
| `deployment_modes` | Optional tuple of the deployment modes the component supports. |
| `required_evidence` | Optional `required_evidence(spec)` returning the evidence IDs the config marks as required; the compiler adds them with the component as their source. |
| `references_evidence` | Optional `references_evidence(spec)` returning the `(evidence_id, source)` pairs the config names without requiring them (`source` `None`: any component); the compiler rejects a pair that its source, or no component, declares, as it does for required evidence. |
| `declares` | Required: the events it publishes, the evidence it emits, and the state it changes ([below](#declare-what-a-component-emits-and-changes)); `Declarations()` for a component that does none of these. A registration without it is rejected. |

`ConfigField(types, default=..., required=..., choices=..., minimum=...,
maximum=..., allow_none=..., reference=..., reference_kinds=..., schema=...,
items=..., variants=...)` describes one field: `float` also accepts integers,
`list` accepts tuples, and `dict` any mapping; a field without a default is
required unless `required=False`; `schema` checks a nested mapping, `items`
each list item, and `variants=(tag, {value: ConfigSchema})` a mapping whose
`tag` field picks its schema (the wave operations of the chaos actions are
one, tagged by `op`). Only a field with `reference=True` may hold an
`{$ref: artifacts.<id>.<output>}` or `resources` reference.

Rules:

- **Factories** take no arguments and build only in-memory state. Do external
  work in the lifecycle methods.
- **`check` is pure.** It runs at compile time, when reference fields may
  still hold unresolved references, and again on the executable plan.
- **Statuses.** Only actions and oracles judge the system under test: return
  `FAIL` from them when the system misbehaved. Return `ERROR` when the harness
  could not do its job. A `FAIL` from any other role makes the run `ERROR`
  (see [Outcomes and verdicts](contracts.md#outcomes-and-verdicts)). An
  observer that could not look for an evidence item (the server did not
  answer, a log could not be read) names it in `make_result(...,
  unobserved=[...])`, so its absence is not taken for a finding.
- **Evidence and results** belong to their component: `make_evidence(spec,
  ...)` and `make_result(spec, ...)` set the source. Evidence that names an
  event sequence must name a committed event.
- **Declare everything** a component publishes, emits, and changes, and own
  every change it will undo (see the next two sections).
- **Random choices** come from `context.rng(spec.id, stream)`, so a seed
  repeats them.
- **Report data.** Summaries, evidence, diagnostics, and event payloads go
  into reports. Do not put secrets in them.
- **Crashes.** A component that finds that the system under test crashed
  returns `FAIL` with `crashes` in its diagnostics: a mapping (or list) of
  crash causes, such as an assertion's `file:line 'condition'`. The
  [failure signature](cli.md#minimize) of such a run is its causes alone, so
  one crash groups as one failure whatever else failed after it.

### Declare what a component emits and changes

`Declarations(events=..., evidence=..., changes=...)`:

| Field | Holds |
| --- | --- |
| `events` | `{event name: ConfigSchema}`: every event the component publishes, besides the runner's `<id>.started`, `.completed`, `.failed` (and `.stopping`, `.stopped`, `.stop-failed` for roles with a teardown), with the schema of its payload. |
| `evidence` | `{evidence ID: EvidenceDeclaration(value, many=False)}`: every evidence ID it emits, with a `ConfigField` for its value. Without `many`, the ID is a single value: one item per component use and run. |
| `changes` | The kinds of host or server state it changes (`process.stopped`, `network.partition`, ...), each owned by an [undo obligation](#own-every-state-change). |

A name may hold `{id}` (the component use's `id`) or `{config.<path>}`
(every string a config path names; `rules[].publish` steps into each list
item), so a component whose config names its events declares them. What the
runner does with anything undeclared or malformed is in
[Evidence contract](contracts.md#evidence-contract).
`valkey-fuzzer list components --format json` shows each component's
declarations with their schemas.

### Own every state change

A component that changes host or server state and will undo the change (a
stopped or paused process, a partition, a filled disk, a setting it
restores, a private server) opens an obligation the moment it applies it,
and discharges it when it undoes it:

```python
paused = context.obligations.open(spec, "process.paused", instance_id, lambda: resume(instance_id))
...
paused.discharge()  # runs the undo; a no-op once it succeeded, raises if it fails
```

The undo must be idempotent: a failed one leaves the obligation open and is
called again. Discharge at the latest in `recover` or `stop` (an action, in
`run`); whatever the runner has to undo for you makes the run `ERROR` (see
[Fault ownership](contracts.md#fault-ownership)).

### The run context

| Attribute | Meaning |
| --- | --- |
| `context.plan` | The executable plan, with references resolved. |
| `context.event_bus` | `publish(name, source=, payload=)`, `subscribe(name, handler)`, `unsubscribe`, and `committed_history(name)`. |
| `context.rng(component_id, stream)` | A random stream derived from the seed; it continues across calls in one run. |
| `context.services["deployment"]` | The provisioned deployment's services. The local deployment publishes [one service](#the-local-deployment-service) under `"valkey.deployment"`. |
| `context.artifact_dir` | This run's own folder: its report folder when a command runs it, else a new `<artifact root>/runs/<UTC time>-<scenario_id>-<suffix>/`. `component_artifact_dir(context, spec)` returns this component's folder in it, `components/<component id>/`. |
| `context.deadline` | This call's deadline (monotonic clock), or `None`: the earlier of the run's and the component's `timeout_s`, and in `stop` and `recover` the `timeout_s` window alone. A thread the component starts sees the run's deadline. |
| `context.cancelled`, `context.sleep(seconds)` | Cancellation; see below. |
| `context.obligations` | The run's undo obligations: `open(spec, kind, target, undo)` returns one to `discharge()`. |

Event dispatch is synchronous: `publish` returns after every subscriber ran,
and a publish from inside a subscriber is queued and returns at once. A
subscriber that raises fails the publish.

### Deadlines and cancellation

A call that outlives its deadline is cancelled (see
[Deadlines and cancellation](contracts.md#deadlines-and-cancellation)). A
cooperative component:

- waits with `context.sleep(seconds)`, never `time.sleep`; it returns `False`
  as soon as the call is cancelled;
- checks `context.cancelled` in long loops and returns promptly once it is
  true (the result is discarded);
- bounds its own waits by `context.deadline`;
- runs work it hands to its own threads (a thread pool) under a copy of the
  call's context, `pool.submit(contextvars.copy_context().run, work)`, so
  those threads share the call's deadline and cancellation; any other thread
  sees the run's deadline and is never cancelled.

## The local deployment service

The local deployment publishes `context.services["deployment"]["valkey.deployment"]`.
Instance IDs are `valkey-N`.

| Group | Members |
| --- | --- |
| instances | `mode`, `deployment_id`, `valkey_sha`; `instances` (each with `instance_id`, `role`, `shard_id`, `host`, `port`, `bus_port`, `slot_start`, `slot_end`, `primary_id`, `cluster_node_id`), `instance(id)` |
| clients | `execute_command(id, *args)`, `new_client(id, timeout=5.0)`, and in cluster mode `cluster_client(timeout=5.0)`; every client is closed at cleanup |
| processes | `pid(id)`, `started_at(id)`, `is_alive(id)`, `send_signal(id, signum)`, `wait_exit(id, timeout=10.0)`, `restart(id, wait_ready=True)` |
| topology | `replicate(id, primary_id)` (standalone): record the instance as a replica of `primary_id` (a primary with `None`) and send `REPLICAOF` if it runs, so a later `restart` keeps the role |
| files | `log_file(id)` and `process_output_file(id)` (its stdout and stderr), both written by this deployment alone, and `data_dir(id)` |
| binaries | `binary(id)`, `set_binary(id, binary)` (used from the next restart) |
| roles | `roles()` (each reachable instance's live role), `primaries()`, `replicas()`, `refresh_roles()`, `live_instances()` |
| cluster | `failover(shard_id, force=, timeout=, stopped_instances=, rng=)`, `seed_slots(keys_per_slot, seed=)` |
| checks | `validate_state(...)` (one pass of the state validators, as `valkey.oracle.deployment-state` runs it) and `state_check(validation, seed=)`, which returns a check with `write_test_keys()`, `load_test_keys(keys)`, `mark_stopped(id, role)`, and `validate(operations)` |

`restart` keeps the instance's address, binary (unless `set_binary` changed
it), data directory, and resolved config; a replica in standalone mode
restarts with `--replicaof` so it never comes up as a writable primary.

`valkey_fuzzer.extensions` also exports the deployment's own readers, so a
plugin parses what Valkey replies the way the deployment does:
`parse_cluster_nodes(text)` (one record per `CLUSTER NODES` line: `node_id`,
`address`, `host`, `port`, `flags`, `primary_id`, `config_epoch`,
`link_state`, the raw `slots` tokens, and `is_myself`, `is_primary`,
`is_replica`, `is_fail`), `parse_cluster_info(reply)`,
`parse_replication(info)` (an `INFO replication` reply as a record: `role`,
`primary_host`, `primary_port`, `link_up`, `sync_in_progress`,
`last_io_seconds_ago`, `replica_offset`, `offset`, `connected_replicas`; a
field the reply lacks is `None`), and `replication_role(info)` (the reply's
role as `primary` or `replica`, or `None`). `STATE_VALIDATION_SCHEMA` checks a `validation`
mapping for `state_check` when the scenario compiles.

## Write a provider

### Artifact and resource providers

An artifact provider resolves an immutable input, such as a binary or a
dataset; a resource provider sets up a managed dependency, such as a
filesystem or a service, and removes it afterwards.

| Provider | Methods |
| --- | --- |
| artifact | `check_support(context, spec, config)`, `resolve(context, spec, config)`, `rollback(context, spec)`, `cleanup(context, spec, artifact)` |
| resource | `check_support(context, spec, config)`, `provision(context, spec, config)`, `rollback(context, spec)`, `cleanup(context, spec, resource)` |

`check_support` must not change anything; returning `SupportDecision.deny(reason)`
makes the run `SKIP_UNSUPPORTED` before anything starts. `resolve` returns a
`ResolvedArtifact` and `provision` a `ProvisionedResource`, each with exactly
the declared `outputs` and report-safe `provenance`. `rollback` follows a
failed acquisition and `cleanup` a successful one.

```python
ArtifactProviderRegistration(
    provider_id="example.artifact.dataset",
    factory=DatasetProvider,
    outputs=("path",),
    config=ConfigSchema({"name": ConfigField(str), "rows": ConfigField(int, default=1000, minimum=1)}),
    check=check_dataset,  # optional check(spec, config)
)
```

The registration's `config` schema checks a declaration's `with:` and fills
in defaults, which the plan records; `check(spec, config)` adds pure checks.
By convention provider IDs are `<namespace>.<kind>.<name>`.

### Deployment providers

A deployment provider starts the system under test for one deployment mode:

| Method | Meaning |
| --- | --- |
| `validate(plan)` | A pure preflight; raise to reject the plan. |
| `provision(context)` | Start the deployment and return a `ProvisionedDeployment` with its `services` (what components see under `context.services["deployment"]`), report-safe `metadata`, and optionally `valkey_sha`. |
| `rollback(context)` | Clean up after a failed `provision`. |
| `cleanup(context, deployment)` | Clean up after a successful one. |

A `DeploymentProviderRegistration` has `deployment_mode`, `provider_id`, and
`factory`, plus an optional `implementation_digest`, an optional pure
`normalizer(document)` that fills in the provider's defaults (the plan records
the normalized document, and the experiment's `deployment:` rules are checked
against it), and an optional pure `plan_instances(document, overlays)` that
returns each instance it will start as `{role, shard, server_config}`, with
the effective server config: the provider's own server defaults under the
document's config and the startup overlays. The plan records it as
`instances`, which compile-time checks read, and a plan whose provider plans
other instances is refused. The plan records the provider ID and, when given, the
`implementation_digest`, and runs only on a provider with the same ones. Every
run report also records the SHA-256 of the provider's code, so `replay`
reports a changed provider as drift.

Several providers may implement one mode. A scenario then selects one with
`deployment.provider`, and may leave it out when only one is registered.
[`examples/extensions/neutral-plugin/`](../examples/extensions/neutral-plugin/)
registers a provider for its own mode, `example-neutral`.

## Write a plugin

A plugin is an installed Python package with an entry point in the
`valkey_fuzzer.plugins` group. The entry point resolves to a `PluginManifest`,
or to a function that returns one. Import only from
`valkey_fuzzer.extensions` (and `valkey_fuzzer.extensions.testing` in tests);
every other module is a framework internal.

```python
from valkey_fuzzer.extensions import PluginManifest, load_experiment_yaml

EXPERIMENT_YAML = """
id: example.touch
deployment:
  modes: [standalone, cluster]
inputs:
  properties:
    count: {type: integer, minimum: 1, default: 3}
components:
  - use: example.action.touch
    id: touch
    config: {key: touched, count: {$ref: inputs.count}}
  - use: example.oracle.touched
    id: check
    depends_on: [touch]
    config: {expect: {$ref: inputs.count}}
required_events: [touch.touched]
required_evidence:
  - {evidence_id: example.touched, source: touch}
"""

PLUGIN = PluginManifest(
    plugin_id="example.touch-plugin",
    registrations=REGISTRATIONS,
    experiments=(load_experiment_yaml(EXPERIMENT_YAML, source_name="example.touch"),),
)
```

A plugin that draws part of a random scenario with code registers a
[generator hook](scenarios-and-experiments.md#generator-hooks) in
`generators`: `GeneratorRegistration("example.generator.touch-count", hook)`,
where `hook(rng, scenario)` returns the value. Hooks have their own registry,
named `<namespace>.generator.<name>`; they are not components and never
appear in a plan. `valkey-fuzzer list generators` prints them.

```toml
# pyproject.toml
[project]
dependencies = ["valkey-fuzzer>=0.2.0"]

[project.entry-points."valkey_fuzzer.plugins"]
"example.touch-plugin" = "example_touch_plugin:PLUGIN"
```

- A plugin provides one thing: a `PluginManifest` whose `plugin_id` equals
  the entry point name. Every entry in it must be the registration type its
  field names (`ComponentRegistration` in `registrations`, `ExperimentSpec` in
  `experiments`, and so on), and every component ID must name its role. A
  plugin that does not import, returns something else, or breaks one of these
  rules fails every command at start-up. Nothing else is checked: a plugin
  is accepted for what it registers.
- A manifest can also carry `experiment_fragments` (named fragments other
  experiments `extends`), `artifact_providers`, `resource_providers`,
  `deployment_providers`, and `metadata`. `load_experiment_yaml` accepts
  `fragments=` to resolve `extends` while loading.
- All installed plugins register together, or none do. A component ID, an
  experiment ID, a provider, or a fragment name registered twice
  stops every command. After registration the catalog is frozen.

[`examples/extensions/neutral-plugin/`](../examples/extensions/neutral-plugin/)
is a complete, installable template, and
`tests/test_neutral_plugin_template.py` builds, installs, and runs it through
the CLI.

## Testing

### In-process

`valkey_fuzzer.extensions.testing` runs experiments and components without
starting servers:

- `run_experiment(PLUGIN, "experiment.id", inputs=..., services=...)`
  compiles and runs an experiment through the real compiler and runner. Modes
  the plugin does not provide itself get a fake deployment whose services you
  supply; installed plugins are not loaded.
- `resolved_spec(registration, spec)` returns a component spec with its config
  checked and its defaults filled in, as a compiled plan holds it.
- `FakeContext(*specs, services=...)` is a run context for calling one
  component's methods directly; `cancel()` does what the runner does when a
  call outlives its deadline, and `obligations` holds what the component
  opened. This repository's unit tests run every plugin component they call
  this way under the runner's evidence contract (`tests/conftest.py`).

```python
from valkey_fuzzer.extensions import ComponentRole, ComponentSpec
from valkey_fuzzer.extensions.testing import FakeContext, resolved_spec, run_experiment


class FakeClient:
    values = {}

    def incr(self, key):
        self.values[key] = self.values.get(key, 0) + 1
        return self.values[key]

    def close(self):
        pass


class FakeDeployment:
    def new_client(self, instance_id, timeout=5.0):
        return FakeClient()


outcome = run_experiment(PLUGIN, "example.touch", inputs={"count": 3}, services={"valkey.deployment": FakeDeployment()})
assert outcome.status.value == "PASS"

spec = resolved_spec(
    REGISTRATIONS[0],
    ComponentSpec(use="example.action.touch", id="touch", role=ComponentRole.ACTION, config={"key": "k"}),
)
result = Touch().run(FakeContext(spec, services={"valkey.deployment": FakeDeployment()}), spec)
```

### Live

`tests/test_live_valkey_experiments.py` runs every Valkey experiment against
real servers. It uses `VALKEY_FUZZER_BINARY` (or `valkey-server` on `PATH`),
and `VALKEY_FUZZER_OLD_BINARY` for the mixed-version, rolling-upgrade, and
baseline-versus-candidate tests. Tests that need forkless save, root, tmpfs,
or iptables skip when the host lacks them.

```bash
VALKEY_FUZZER_BINARY=/path/to/valkey-server \
VALKEY_FUZZER_OLD_BINARY=/path/to/older/valkey-server \
  python -m pytest -m live tests/test_live_valkey_experiments.py
```

`python -m pytest -m "not slow"` runs the unit tests only; `-m slow` runs
every test that starts real servers, and `-m live` the live experiment tests.

Give every new experiment two live tests: one where it passes, and a
negative control where it must fail (see [Design the test](#design-the-test)).

## Plugin trust

A plugin is arbitrary Python code. The fuzzer imports every package
installed with a `valkey_fuzzer.plugins` entry point on every command except
`cleanup-logs`, including `list` and `validate`, with the privileges of the
user running it.
The experiments that change the host run as root. Building or installing a
plugin from source also runs its build code.

- Install only plugins you trust, from sources you trust.
- `--no-plugins` turns off entry-point discovery for `run`, `batch` (and its
  workers), `validate`, `list`, `replay`, `diff`, and `minimize`. It also
  turns off the in-repo Valkey plugin, so the `valkey.*` experiments and
  components are not available; only experiments from `--experiment-dir`
  remain, and they can use only components that need no plugin (the core
  registers none). It is mainly useful for deployment smoke tests, such as
  `examples/standalone_scenario.yaml` with `--experiment-dir examples/experiments`.
- [`examples/run_fuzzer.py`](../examples/run_fuzzer.py), which calls the
  run service from Python, loads no plugins unless you pass `--enable-plugins`.
- A plugin that fails to load stops every command with a plugin discovery
  error that names the entry point and its distribution.
