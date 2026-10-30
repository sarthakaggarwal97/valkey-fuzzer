# Architecture

This page is for people who change the framework itself. It describes the
goals and boundaries, the code layout, how a scenario becomes an outcome, and
how the built-in local deployment provider starts Valkey. To use the fuzzer,
start with [Getting started](getting-started.md); to add tests, see the
[extension guide](extending.md).

- [Goals and boundaries](#goals-and-boundaries)
- [Code layout](#code-layout)
- [From scenario to outcome](#from-scenario-to-outcome)
- [The extension catalog](#the-extension-catalog)
- [The compiler](#the-compiler)
- [The runner](#the-runner)
- [The run service and the commands](#the-run-service-and-the-commands)
- [The local deployment provider](#the-local-deployment-provider)
- [The Valkey plugin](#the-valkey-plugin)
- [Not implemented](#not-implemented)
- [When to grow the core](#when-to-grow-the-core)

## Goals and boundaries

valkey-fuzzer gives every Valkey test one path: one strict scenario format,
one compiler, one immutable execution plan, one runner lifecycle, one
extension API, and one outcome and report model.

Adding a test must not need a change to the core: no feature enum, no new
CLI command, no scenario parser branch, no deployment special case, and no
report format of its own. An author writes a data-only experiment, or
installs a plugin wheel, and uses the existing `validate` and `run` commands.

The core owns mechanics every test shares:

- strict parsing of scenarios and experiments, and experiment fragments;
- one config schema type (`ConfigSchema`) that validates experiment inputs,
  deployment field rules, component config, and provider config, with
  defaults recorded in the plan, and the batch specs, search profiles, zoo
  cases, and differentials;
- immutable plans and deterministic per-component random streams;
- the component, experiment, and provider registries, and trusted plugin
  discovery as one atomic registration, after which the catalog is frozen;
- dependency checks and a stable execution order;
- the local deployment provider for standalone and cluster mode, and
  plugin-defined deployment providers;
- startup overlays by role or instance;
- artifact and resource declarations, typed references, acquisition,
  rollback, and cleanup; policies passed through uninterpreted;
- events, evidence, required events and evidence, and the verdict;
- the [base contracts](contracts.md): the evidence contract, fault
  ownership, effective server config, legal space and search profiles,
  deadlines, and host-state tags;
- the generator and its hook registry, run reports, replay with provenance
  checks, differential runs, minimization, the batch runner, the sweep, the
  spec bridge, and the bug zoo.

The core does not interpret experiment IDs, component IDs, evidence IDs, or
policy types. Extensions own what is specific to a test: capability probes,
inputs, workloads, transitions and faults, path evidence, oracles, and any
external service or loss model.

External packages import only from `valkey_fuzzer.extensions` (and
`valkey_fuzzer.extensions.testing`); every other module is internal. The
Valkey experiments follow the same rule (see [The Valkey plugin](#the-valkey-plugin)).

## Code layout

| Path | Holds |
| --- | --- |
| `valkey_fuzzer/_data.py` | Shared data helpers: frozen mappings, name and ID checks, environment references, the registry base class. |
| `valkey_fuzzer/schema.py` | `ConfigSchema` and `ConfigField`; the duplicate-key-rejecting YAML loaders; the experiment error types. |
| `valkey_fuzzer/contracts.py` | `ComponentSpec`, `InstanceSelector`, `StartupOverlay`, `ExperimentSpec`, `ExecutionPlan`, results and `RunOutcome`, the component and deployment provider protocols, registrations, `PluginManifest`, and the result and evidence helpers. |
| `valkey_fuzzer/events.py` | `RunEvent` and `EventBus`, the run's event journal. |
| `valkey_fuzzer/evidence.py` | The evidence contract: `check_requirements` at compile time, `EvidenceContract` at run time. |
| `valkey_fuzzer/obligations.py` | `ObligationLedger` and `Obligation`: owned state changes and their undo. |
| `valkey_fuzzer/catalog.py` | `ExtensionCatalog` and `build_catalog`: experiment documents and fragments, the registries (one for components, one for generator hooks), plugin discovery. |
| `valkey_fuzzer/describe.py` | The catalog as data: what `list --format json` prints. |
| `valkey_fuzzer/compiler.py` | `ScenarioCompiler`: scenario plus catalog to execution plan. |
| `valkey_fuzzer/runner.py` | `Runner`: the run lifecycle, deadlines, cancellation, and containment. |
| `valkey_fuzzer/verdict.py` | The outcome rules, the exit codes, and how an outcome prints. |
| `valkey_fuzzer/environment/` | Artifact, resource, and policy specs; provider registrations and protocols; `EnvironmentProvisioner` and `EnvironmentLease`. |
| `valkey_fuzzer/deployment/local/` | The local deployment provider (see [below](#the-local-deployment-provider)). |
| `valkey_fuzzer/service.py` | `RunService`: one frozen catalog with its compiler and runner; compile, execute, report, replay, differentials, and minimization. |
| `valkey_fuzzer/batch.py` | The batch runner: sources to jobs, isolated worker processes, leak sweeps, signature groups, serial re-runs, resume, and the zoo verdicts. |
| `valkey_fuzzer/commands/`, `valkey_fuzzer/cli.py` | The command handlers; `cli.py` only builds the parser and dispatches. |
| `valkey_fuzzer/minimize.py`, `valkey_fuzzer/coverage.py`, `valkey_fuzzer/differential.py` | Failure signatures and minimization; a run's coverage tuple; differential runs. |
| `valkey_fuzzer/generator.py` | Random scenarios for experiments that declare `metadata.generator`, and the search profiles that sample them. |
| `valkey_fuzzer/hoststate.py` | Host-state owner tags and `valkey-fuzzer sweep`. |
| `valkey_fuzzer/zoo.py`, `zoo/` | The [bug zoo](bug-zoo.md): its cases and how a case is judged. |
| `valkey_fuzzer/extensions/` | The public API, and `testing` for in-process tests. |
| `valkey_fuzzer/spec/`, `formal/` | The [spec bridge](spec-bridge.md) (`valkey-fuzzer spec check`, `translate`, `conform`) and its TLA+ bundles. |
| `valkey_fuzzer_experiments/` | The Valkey plugin: components by role, providers, private probe servers, `experiments/`, and `fragments/`. |

## From scenario to outcome

```text
scenario YAML
  │  ScenarioCompiler, against the frozen ExtensionCatalog
  │    inputs, components, deployment, artifacts, resources, policies resolved
  ▼
ExecutionPlan (immutable; environment references stay symbolic)
  │  RunService.execute writes <folder>/plan.json
  │  Runner.run
  │    static preflight
  │    capability probes (here when the plan has no environment references)
  │    acquire artifacts and resources ─▶ executable plan, references resolved
  │    capability probes (here when it has)
  │    provision the deployment
  │    start observers, workloads, faults, and actions in dependency order
  │    tear down in reverse; evaluate oracles; clean up the deployment
  │    release artifacts and resources
  ▼
RunOutcome (verdict.decide)
  │  RunService writes <folder>/report.json
  ▼
run report: scenario, outcome, coverage, reproducer, provenance
```

Only the compiled plan is persisted, reported, or replayed. The runner
executes a copy in which every reference is replaced by the acquired output,
so a report never holds a run-time path it did not declare.

## The extension catalog

`build_catalog` builds one `ExtensionCatalog`, which a `RunService` builds on
first use and keeps, with its compiler and runner:

1. it registers the local deployment provider for `standalone` and `cluster`
   (`valkey-fuzzer.deployment.local`);
2. it loads every `valkey_fuzzer.plugins` entry point (unless plugins are
   off) and registers them, with any manifests passed in, as one transaction:
   every entry is validated before anything changes, so a conflicting plugin
   leaves the catalog untouched;
3. it parses the experiment directories, which can extend the fragments
   plugins provide;
4. it freezes every registry.

Components are keyed by ID in one registry, and the catalog rejects an ID
that is not `<namespace>.<role>.<name>` for the role it registers. Experiments are keyed
by ID, artifact and resource providers by ID, and deployment
providers by deployment mode and provider ID. Plugin IDs and fragment names
must be unique.

## The compiler

`ScenarioCompiler.compile` is pure. For one scenario it:

1. reads the strict envelope (`scenario_id`, `seed`, `experiment`,
   `deployment`, `artifacts`, `resources`, `policies`);
2. resolves the experiment by ID;
3. checks the inputs against the experiment's `inputs` schema
   (`ConfigSchema.from_document`) and fills in defaults;
4. substitutes `{$ref: inputs.*}` into component config, startup overlays,
   metadata, and the scenario's deployment and environment sections, and
   parses `{$ref: artifacts.*}` and `{$ref: resources.*}` into typed
   references;
5. takes each component's role from its registration, and derives required
   evidence from registrations that mark it required;
6. checks the deployment mode, selects the deployment provider (named by
   `deployment.provider`, or the only one for the mode), runs its normalizer,
   and checks the experiment's deployment rules
   (`ConfigSchema.from_rules`) against the normalized document;
7. resolves each artifact and resource declaration's config through its
   provider's schema and `check`;
8. builds the `ExecutionPlan`, which checks the composition (unique IDs, an
   acyclic dependency graph, the stage rules: a capability depends only on
   capabilities and nothing that runs before the oracles depends on an
   oracle, required evidence sources, and in `experiment` mode a required
   oracle and required evidence);
9. records the instances the deployment provider plans (`plan_instances`):
   each instance's role, shard, and effective server config, the provider's
   defaults under the deployment's layers and the overlays;
10. resolves every component's config through its registration (`config`
    schema, `deployment_modes`, and `check`, which reads the recorded
    instances) and records it with defaults;
11. checks that every required event and evidence ID is declared by a
    component of the plan, or published by the runner;
12. validates every environment reference against the declarations and their
    providers' outputs.

Steps 9 to 12 are `check_plan`, which the runner runs again on every plan it
is given, so a plan built or edited outside the compiler meets the same rules.

## The runner

`Runner.run` executes one plan and returns an outcome; a static
preflight error raises instead. `RunService.run_plan` runs the preflight
first and records its error as an `ERROR` outcome with phase `preflight`; an
exception the runner raises after it is recorded with phase `runner`.

- **Preflight** checks the deployment provider's identity, builds a fresh
  provider, calls its `validate` (unless the deployment holds references),
  and runs `check_plan`: it records the instances the provider plans for a
  plan built without the compiler, refuses one whose recorded instances its
  provider no longer plans, resolves component configs, and repeats the
  compiler's requirement and environment reference checks. It refuses to
  start while a call abandoned by an earlier run in the process is still
  running.
- **Instantiation** builds every component from its factory and checks it
  implements its role's protocol.
- **Evidence contract.** `EvidenceContract` (`evidence.py`) is the event
  bus's check on every published event and the check on every result's
  evidence before an oracle sees it; the rules are in
  [Evidence contract](contracts.md#evidence-contract).
- **Obligations.** `ObligationLedger` (`obligations.py`) is
  `context.obligations`: it checks each opened change against the declared
  `changes`, undoes what is still open after teardown (before the oracles),
  and retries failed undos after deployment cleanup, which closes it: later
  changes, and those of a start call still running then, are undone as soon
  as they are made or the call returns; see
  [Fault ownership](contracts.md#fault-ownership).
- **Environment.** `EnvironmentProvisioner.acquire` support-checks every
  declaration whose config needs no acquired output before acquiring
  anything, then acquires in dependency order. A failure rolls back the
  current declaration and cleans up everything acquired, in reverse. The
  lease's `close` releases them at the end, and every step goes into the
  report-safe environment report.
- **Execution.** Provisioning binds the deployment's services into the run
  context. Components start in plan order; one whose dependency did not pass
  is skipped. Teardowns run in reverse, and every teardown error is recorded
  without stopping the others. Oracles run only when setup succeeded. The
  deployment is cleaned up whatever happened.
- **Deadlines.** A call with a deadline runs on a daemon thread under a
  watchdog, and `context.deadline` on that thread is the call's own deadline.
  At the deadline the runner cancels it (`context.cancelled`,
  `context.sleep` returning early), waits a grace period, and records the run
  as `ERROR`. A call still running after deployment cleanup is recorded in a
  process-wide list of abandoned calls. With a run deadline, the subscribers
  of runner events run on a watched thread too, and a dispatch that outlives
  its window aborts the event bus. A component whose start call was given up
  on is torn down after deployment cleanup, once that call returns. See
  [Deadlines and cancellation](contracts.md#deadlines-and-cancellation).
- **Verdict.** `verdict.decide` applies the rules in
  [Outcomes and verdicts](contracts.md#outcomes-and-verdicts), and
  `verdict.environment_status` lets a failed environment release only make the
  outcome worse.

## The run service and the commands

`RunService` does everything a command does with its catalog: compiling,
executing, writing report folders and run reports, the reproducer, replay
with its provenance and binary drift checks, differential runs, and
minimization. It never parses arguments or prints: methods take
plain arguments, report progress through callbacks, and raise
`RunServiceError` subclasses (`InputError`, `ReportWriteError`,
`ReplayDriftError`) or the compiler's errors.

The handlers in `valkey_fuzzer/commands/` (`run.py` for `run` and
`cleanup-logs`; `batch.py`; `validate.py`; `listing.py`; `replay.py`;
`diff.py`; `minimize.py`; `sweep.py`; `spec.py`) call the service or the
batch runner, print, and return exit codes. The batch runner runs each job in
a worker process, `python -m valkey_fuzzer.batch TASK`, which executes one
plan through the service. `cli.py` builds the parser from them, and reports
every error a handler raises, once, as a harness error (exit 2); an
interrupt (Ctrl-C, or `SIGTERM`, which the command and a batch worker turn
into one) is cleaned up like any other and exits 130.

Every executed plan gets a report folder with `plan.json` and `report.json`,
and the service passes it to the runner as the run's folder, so the run's
component artifacts land in it too; the layout is in
[Report folders](cli.md#report-folders). Folder names start with the UTC time
to the millisecond and end with a random suffix, and are created exclusively,
so they sort by start time and never collide between processes. A plan run
through `RunService.run_plan` or `Runner.run` without a folder gets
one named the same way under `<artifact root>/runs/` when a component first
writes an artifact; the artifact root defaults to `<temp dir>/valkey-fuzzer`,
the same root the service uses.

## The local deployment provider

`valkey_fuzzer/deployment/local/` runs a deployment as `valkey-server`
processes on this host. Its fields are listed under
[The local deployment](scenarios-and-experiments.md#the-local-deployment).

| File | Does |
| --- | --- |
| `provider.py` | `LocalDeploymentProvider`: validate, provision, roll back, clean up; the report-safe deployment metadata. |
| `config.py` | The deployment document: defaults (the registration's normalizer), validation, `DeploymentConfig`. |
| `planner.py` | Instance planning and startup overlays. |
| `server_config.py` | Server config normalization, managed options, command-line arguments. |
| `ports.py` | Port availability checks. |
| `processes.py` | Binary resolution and the source build, process start, readiness, restart, and stop. |
| `formation.py` | Standalone and cluster formation and health checks. |
| `failover.py` | `failover_shard`: shard failovers. |
| `service.py` | `ValkeyDeploymentService`, the `valkey.deployment` service components use, and the per-slot key loader. |
| `models.py`, `queries.py`, `cluster_nodes.py` | Data models, client helpers, `CLUSTER NODES` parsing, and the slot hash tags. |
| `validation/` | The state validators and `DeploymentStateCheck`. |

### Provisioning

`validate` plans the deployment in memory, with startup overlays applied, and
touches nothing else. `provision`:

1. **plans the instances** (`valkey-N`, roles, shards, slot ranges, ports,
   addresses, layered server config) and applies the startup overlays; each
   instance starts with `DEFAULT_SERVER_CONFIG` (`appendonly yes`,
   `appendfsync everysec`, `save ""`, `maxmemory 500mb`, `loglevel notice`,
   and in cluster mode `cluster-node-timeout 1000` and
   `cluster-require-full-coverage no`) under that config, exactly the
   effective config the plan records;
2. **checks every planned port** right after planning, before resolving
   binaries: every client port and, in cluster mode, cluster bus port. If any
   is in use, it names every busy port and its instance and starts nothing, so
   a server it did not start never receives commands such as
   `CLUSTER RESET HARD`. The check repeats when each instance is spawned;
3. **resolves the binary**: the configured `valkey_binary`, `valkey-server`
   on `PATH`, a previous build at `/tmp/valkey-build/valkey/src/valkey-server`,
   or a shallow clone of `valkey-io/valkey` at release 9.1.3, built with
   `make -j` aside and moved into place, under a lock so concurrent runs build
   it once. Every
   `instance_binaries` entry is resolved to an absolute path, and the git SHA
   is read from `valkey-server --version`;
4. **starts every instance** and waits up to 30 seconds for each to answer
   `PING`, checking that the `process_id` it reports is the process it
   started. An instance that exits or does not answer fails the provision,
   with the tail of its captured output in the error;
5. **forms the deployment**:
   - standalone without replicas: the instance must report the primary role;
   - standalone with replicas: `REPLICAOF NO ONE` on the primary and
     `REPLICAOF <host> <port>` on each replica, then every link must be up
     and point at the primary;
   - cluster: `CLUSTER RESET HARD` on every node (after checking it is the
     process it started), `CLUSTER MEET` from `valkey-0` until every node
     knows all the others (up to 30 seconds), `CLUSTER ADDSLOTS` with each
     primary's range, polling up to 10 times, 2 seconds apart, until
     `cluster_slots_assigned:16384` and `cluster_slots_fail:0`, then
     `CLUSTER REPLICATE` on each replica and up to 60 seconds for every link to
     come up;
6. **validates** the formed deployment: within 30 seconds every instance must
   be reachable and, in cluster mode, agree on `cluster_state:ok` with all
   slots assigned and none failed.

If any step fails, the provider stops and removes every process it started
before the error propagates, and the error names the step and its cause;
nothing is retried, and the run is `ERROR`. Formation's clients are closed
when it ends, either way. A failed cleanup leaves the attempt for the runner's
`rollback`, which retries it.

The outcome records the deployment ID, mode, addressing, `valkey_sha`, and
each instance's role, shard, address, PID, binary, and binary SHA-256 under
`diagnostics.deployment`; replay compares those digests.

### Restart, failover, and cleanup

`restart(id)` records the live roles first, so an instance whose replica was
promoted restarts in its current role. It stops a still-running process,
checks the ports again, and starts the instance with the same address, binary
(unless `set_binary` changed it), data directory, and resolved config; a
replica in standalone mode gets `--replicaof`, so it never comes up as a
writable primary. With `wait_ready` it waits up to 30 seconds until the
instance is ready for its role and checks the process identity, and in cluster
mode that the node's cluster ID is unchanged. A standalone replica is ready
once its link to its primary is up, or, while that primary's process is not
running (killed in the same wave), once it follows it: it links when the
primary is back. A standalone primary is ready once every replica whose
process runs is connected. An instance that does not
become ready is stopped and the restart fails. Restarts of the same instance
run one at a time.

`replicate(id, primary_id)` (standalone mode) records an instance as a
replica of `primary_id`, or a primary with `None`, whether or not it is
running, then sends `REPLICAOF host port` or `REPLICAOF NO ONE` to a running
instance (an error or a 5-second timeout is raised, with the record kept).
So a primary that was killed and replaced restarts with `--replicaof` as a
replica of its successor, not as a second, empty primary, and the
replication validator, which takes the expected primary from the recorded
roles, follows the new topology. `valkey.action.replication-chaos` and
`valkey.action.replicaof` change the topology only through it.

`failover(shard)` (cluster mode) tries the replicas of the shard's current
primary in an order drawn from the caller's seeded random stream and picks
the first that has synced with that primary: its link is up, no sync is in
progress, and its offset is past 0. It waits up to the timeout for one,
while the primary answers, and fails without sending anything if none
syncs. `CLUSTER FAILOVER FORCE` checks
no data by design, so a replica that had just migrated to an orphaned
primary would otherwise win with an empty dataset. It then sends
`CLUSTER FAILOVER` or `CLUSTER FAILOVER FORCE` and waits for the promotion.
Instances stopped on purpose are passed in, so a shard whose replicas were
all killed is not a failed failover.

Cleanup tries every step even when one fails, then reports the failures: each
instance gets `SIGTERM`, then `SIGKILL` if it has not exited after 5 seconds;
its ports are released; with `enable_cleanup` the deployment's data
directories are deleted, including those under `instance_data_roots`. Logs
are kept. A server whose termination failed stays tracked, with its ports and
data directories, so a retry can stop it.

### State validation

`validation/` holds the checks that `valkey.oracle.deployment-state`,
`valkey.action.cluster-chaos`, and `valkey.oracle.chaos-validation` use
through the service's `validate_state` and `state_check`: replication links
and lag, cluster status and slot coverage, roles and view consistency, data
consistency (test keys written before a run), and the failover lines in the
logs of affected shards, grouped by the live shards (an election for epoch 0
fails the check; a replica's election that expired or timed out is only a
warning, since another replica may have won and the state checks judge the
result). `DeploymentStateCheck` records the instances stopped on purpose,
and each validation attempt reads the deployment once (the topology, then
each node's INFO replication and, in cluster mode, CLUSTER INFO and CLUSTER
NODES), so every check judges the same moment. It runs the enabled checks
within `validation_timeout` and retries, classifies critical failures by
typed flags, and returns report-safe summaries. Outside cluster mode only the
replication check (when there are replicas) and the role check run.

## The Valkey plugin

`valkey_fuzzer_experiments` holds everything specific to Valkey features: the
component library ([full list](components.md)), the providers
`valkey.artifact.local-binary`, `valkey.resource.tmpdir`, and
`valkey.resource.tmpfs`, the generator hooks, the
[fragments](scenarios-and-experiments.md#fragments) (the search profiles
among them), and the experiments. It registers through the public entry
point like any external plugin, and a test asserts that it imports nothing
from `valkey_fuzzer` except `valkey_fuzzer.extensions`.

What it needed from the core was feature-neutral: per-instance binaries,
data roots, and loopback addresses in the local deployment; a deployment
service with process control, instance files, live roles, tracked clients,
shard failover, and the state validators; report-safe deployment metadata for
provenance; and tooling (`diff`, `batch`, `minimize`, `replay`, `list`)
that works only on scenarios, plans, outcomes, and opaque evidence.

Cluster chaos is one experiment on the common runner, generated from a
seed like every other ([Cluster chaos and CI](cluster-chaos-and-ci.md)).

### The wave engine

`valkey.action.cluster-chaos` and `valkey.action.replication-chaos` are two
registrations over one engine, `valkey_fuzzer_experiments/waves.py`. Both
take `waves`: each a list of operations that start together (after their
`delay_s`, or after an earlier operation of the wave finished), then a heal,
then validation. `WaveRun` plans every wave (targets in operation order,
operations that need a role last), runs it, heals it, validates it, and
judges it with one set of rules; every operation draws from its own random
stream, `wave-<n>-op-<i>`, before the wave and while it runs.
`valkey.generator.waves` draws both experiments' waves from the operations
of the drawn deployment mode.

- **Operations** are the building blocks, one entry each in `OPERATIONS`:
  the deployment modes it supports, the targets it takes, its own config
  fields, and what it does: `removes` an instance (kill safety
  applies), `disrupts` one (later picks of the wave avoid it), is a `held`
  fault (undone after `duration_s`), changes the `topology` (only an
  interfering fault excuses its failure), its durability effect
  `loss_window` (only an operation that declares it may open a loss window),
  and `mutates`, the state it changes, its conflict keys. Each mode's
  `waves` config field is lists of `operation_field(mode)`, a union tagged
  by `op` with every kind's timing and fields and their bounds, so the
  compiler rejects a malformed operation with the path to its field. The implementation
  is a `_<name>` method: `kill`, `stall`, and `partition` are the engine's;
  `failover` is the cluster's; `restart`, `promote`, `repoint`,
  `backlog_pressure`, and `compression` are standalone's.
- **Ownership**: `WaveRun.own` opens an
  [undo obligation](contracts.md#fault-ownership) for every change the
  engine applies and will undo, recorded with the operation that owns it: a
  stall (undone by `resume_if_alive`), a partition (its own chain, through
  the same `create_chain`, `drop_between`, and `remove_partition` as
  `valkey.fault.network-partition`), a setting, the validation's
  `CLIENT PAUSE`, and a kill the mode restarts (through `restart_once`;
  `kill_undo` is None in cluster mode, where a killed node stays down). The
  operation discharges its own at the end of `duration_s`, the heal
  (`release`, stalls first) what is still open, and the final cleanup the
  rest. The obligations are the one record of what is still in place:
  `held_on` and each wave's `healed` and `still_active` operations read
  them.
- **Concurrency**: a held fault claims the state it changes (its conflict
  keys) from before it saves anything until its undo succeeds, and a kill
  claims its target while it signals and waits for the exit, so faults of a
  wave on the same state run one after the other; a fault whose state is
  held by one whose undo failed is skipped. An undo, heal, validation, or
  cleanup the harness could not complete is an `ERROR`, and each wave
  records its planned, applied, healed, still-active, and skipped
  operations separately.
- **Policies** are per mode, in a `WaveRun` subclass: the kill safety of
  the one `TargetSelector` (a live member in every shard, plus, with
  `quorum`, a majority of slot-owning primaries in cluster mode; a
  standalone deployment is one shard), what the run prepares before its
  test keys (cluster mode seeds every slot), the live members targets
  resolve against, the heal
  (`ClusterWaves` lets Valkey fail over; `StandaloneWaves` restores one
  primary that every instance follows), the validation, which faults excuse
  a failed topology change, and whether a failed wave stops the run.

Both actions run through `WaveRun.execute`: resolve the waves, check the
host, start the run, write the test keys, run every wave, and finish. A new
operation is an `OPERATIONS` entry and a `_<name>` method on the runs of its
modes (a test checks that every declared operation has one). A new mode, such
as Sentinel for a standalone primary and its replicas, is a `WaveRun`
subclass, a `WaveAction` for it, and a `wave_registration`.

## Not implemented

These were part of earlier designs and are not built. Nothing depends on
them.

- **CPU management.** Reserving cores for the fuzzer and pinning instances to
  cores. All instances and the fuzzer share every CPU.
- **Recovery during provisioning.** Retrying a failed formation step, or
  continuing with a smaller deployment. Any failure stops and removes the
  whole deployment; the only retries are the bounded polls above.
- **Reading config back.** Comparing each instance's `CONFIG GET` with the
  effective config the plan records, after start. Experiments that need a
  setting check it before the run with `valkey.capability.build-probe`.
- **Multiple hosts.** Every instance runs on the local host.
- **Object-store, proxy, and sidecar providers, and corpus management.**
- **Hermetic replay.** Replay checks provenance and refuses on drift, but it
  runs with what is installed and cannot reproduce timing.

## When to grow the core

Some capabilities live in the Valkey plugin today: event-triggered fault
placement (`when_event` in a fault's config), the write ledger and loss
windows, and the filesystem providers. Promote a capability into the core
only when:

1. at least one real extension needs it now;
2. its contract can stay feature-neutral;
3. its lifecycle and failure semantics are understood; and
4. sharing it removes real duplication without forcing unrelated extensions
   to use it.

Until then, keep it in the extension, so hypothetical flexibility does not
become permanent framework complexity.

The neutral plugin template in
[`examples/extensions/neutral-plugin/`](../examples/extensions/neutral-plugin/)
is the acceptance test for this boundary. It registers a deployment provider
for its own mode, a configured action, an oracle, and an experiment with a
required event and evidence; `tests/test_neutral_plugin_template.py` builds
and installs it as a wheel and runs it through the unchanged CLI to `PASS`.
