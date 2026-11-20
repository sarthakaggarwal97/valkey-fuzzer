# Valkey component reference

The Valkey components in `valkey_fuzzer.components` are the 45 components
and 4 providers in the [index](#index) below, one page per role:
[capabilities](capabilities.md), [workloads](workloads.md),
[actions](actions.md), [faults](faults.md), [observers](observers.md),
[oracles](oracles.md), and [providers](providers.md). Experiments configure
them; how is in
[Scenarios and experiments](../scenarios-and-experiments.md#components).
`valkey-fuzzer list components` and `valkey-fuzzer list generators` print what
is installed. The generator hooks are listed under
[Generator hooks](../scenarios-and-experiments.md#generator-hooks); they are not
components.

Component IDs have the form `<namespace>.<role>.<name>`. Every config is
checked when the scenario compiles, and the execution plan records it with
every default filled in, so a report shows exactly what ran. The one
exception is the `validation` settings of `valkey.action.cluster-chaos`,
`valkey.action.replication-chaos`, and `valkey.oracle.deployment-state`, whose
left-out keys the deployment fills in when the run starts. In the tables,
a field without a default is required, ranges are inclusive, and "or null"
means `null` is accepted.

- [Common config forms](#common-config-forms)
- [Index](#index)
- [Events, evidence, and state changes](#events-evidence-and-state-changes)
- [Capabilities](capabilities.md)
- [Workloads](workloads.md)
- [Actions](actions.md)
- [Faults](faults.md)
- [Observers](observers.md)
- [Oracles](oracles.md)
- [Providers](providers.md)

## Common config forms

### Instance selectors

Fields that pick instances take a selector:

| Selector | Selects |
| --- | --- |
| `all` | every instance |
| `primaries` / `primary` | every primary / the first primary |
| `replicas` / `replica` | every replica / the first replica |
| `valkey-2` | that instance |
| `[valkey-0, valkey-3]` | those instances, in that order |

The fields `instances`, `isolate`, `peers`, and the trace observer's `nodes`
take any of these forms. The single-instance fields (`instance` of the
process-signal fault and `target` of the failover action) take a word or an
instance ID, not a list, and use the first instance it selects. The
process-signal fault also accepts `event`, the instance named by the `instance_id` field of the
triggering event's payload. Startup overlays also accept a mapping
`{roles, instance_ids, first}` (see
[startup_overlays](../scenarios-and-experiments.md#startup_overlays)). Every
component checks its selector fields when the scenario compiles, so a
malformed selector, or an instance ID not of the form `valkey-N`, is a
compile error.

Roles are the deployment's live roles at the time the selector is resolved,
so a promoted replica counts as a primary; instances come in plan order. An
action resolves its selector when it runs and an observer when it starts. A
fault resolves its selector when it installs its trigger at the start of the
run, not when the trigger fires, so `instance: primary` names the instance
that was primary then.

### Predicates

A predicate is one of `{exists: true|false}`, `{equals: V}`,
`{not_equals: V}`, `{in: [V, ...]}`, `{contains: S}`, or a numeric
`{min: N}`, `{max: N}`, or both. Values compare as strings, except `min` and
`max`, which compare as numbers.

### Trigger fields

Every fault takes these fields, and injects once, synchronously inside the
dispatch of the matching event, after `delay_s`:

| Field | Default | Meaning |
| --- | --- | --- |
| `when_event` | | The event that triggers the fault, for example `run-before-crash.completed` or `bgsave.save-started`. |
| `occurrence` | 1 | Fire on the n-th matching event (1 to 1,000,000). |
| `when_payload` | `{}` | Payload fields the event must match: a scalar (compared as a string), a list (any of), or a predicate. |
| `delay_s` | 0 | Seconds (0 to 600) to wait inside the trigger's dispatch before firing. A fault whose call is cancelled during the wait does not fire and is not applied. |

A fault publishes `<id>.injected` when it fires (with `applied`, `trigger`,
`applied_at`, and its target) and records `valkey.fault.injected` evidence
when it applied. Its `recover` returns `ERROR` if it never fired, which
becomes the fault's result, and then a failure of anything that depends on
it is not trusted: the run is `ERROR`. A fault that refuses to
fire because firing would be unsafe, such as filling a path that is not the
run's tmpfs, also makes the run `ERROR`. Each change a fault applies is owned
by an undo obligation (its kind is in the [declarations table](#events-evidence-and-state-changes));
an early undo (a heal, a release, the end of a pause, an immediate restart)
discharges it, `recover` discharges the rest, and an undo that failed before
a retry succeeded makes the fault, and the run, `ERROR`.

## Index

Every component and provider, by role, with the deployment modes it runs in
(`any`: standalone and cluster). The tables are generated from the catalog by
`scripts/gen_component_docs.py`, which `tests/test_docs_declarations.py` checks
in CI; the one-line descriptions are written by hand in this file.

<!-- BEGIN index: generated by scripts/gen_component_docs.py -->
### [Capabilities](capabilities.md)

| Component | Modes | Does |
| --- | --- | --- |
| [`valkey.capability.build-probe`](capabilities.md#valkeycapabilitybuild-probe) | any | checks each binary supports the experiment |
| [`valkey.capability.host-probe`](capabilities.md#valkeycapabilityhost-probe) | any | checks the host (Linux, root, tmpfs, iptables) |

### [Workloads](workloads.md)

| Component | Modes | Does |
| --- | --- | --- |
| [`valkey.workload.benchmark`](workloads.md#valkeyworkloadbenchmark) | any | keeps instances busy with their build's `valkey-benchmark` |
| [`valkey.workload.command-table`](workloads.md#valkeyworkloadcommand-table) | any | sends commands drawn from the whole command table |
| [`valkey.workload.config-churn`](workloads.md#valkeyworkloadconfig-churn) | any | changes configs with `CONFIG SET` during the run |
| [`valkey.workload.key-value`](workloads.md#valkeyworkloadkey-value) | any | writes keys and records every client-visible result |

### [Actions](actions.md)

| Component | Modes | Does |
| --- | --- | --- |
| [`valkey.action.bgsave`](actions.md#valkeyactionbgsave) | any | runs BGSAVE to completion |
| [`valkey.action.cluster-chaos`](actions.md#valkeyactioncluster-chaos) | cluster | failover waves with process kills |
| [`valkey.action.cluster-election`](actions.md#valkeyactioncluster-election) | cluster | forces one failover election |
| [`valkey.action.command`](actions.md#valkeyactioncommand) | any | sends a command and checks the reply |
| [`valkey.action.commands`](actions.md#valkeyactioncommands) | any | sends a command sequence on one connection, and compares the replies of two builds |
| [`valkey.action.failover`](actions.md#valkeyactionfailover) | cluster | fails over one shard |
| [`valkey.action.migrate-slots`](actions.md#valkeyactionmigrate-slots) | cluster | moves one hash slot to another shard with `CLUSTER MIGRATESLOTS` |
| [`valkey.action.poll`](actions.md#valkeyactionpoll) | any | waits for an INFO field or reply to match |
| [`valkey.action.replication-chaos`](actions.md#valkeyactionreplication-chaos) | standalone | waves of kills, promotions, repoints, stalls, partitions, and degradations, healed and validated |
| [`valkey.action.rolling-upgrade`](actions.md#valkeyactionrolling-upgrade) | standalone, cluster | moves every instance to a new build |
| [`valkey.action.sleep`](actions.md#valkeyactionsleep) | any | waits |
| [`valkey.action.slot-migration-chaos`](actions.md#valkeyactionslot-migration-chaos) | cluster | waves of slot migrations with kills, partitions and cancels on them, healed and validated |
| [`valkey.action.tools`](actions.md#valkeyactiontools) | any | runs a Valkey tool of two builds and compares what they do |
| [`valkey.action.wait-event`](actions.md#valkeyactionwait-event) | any | waits for an event |

### [Faults](faults.md)

| Component | Modes | Does |
| --- | --- | --- |
| [`valkey.fault.clock-skew`](faults.md#valkeyfaultclock-skew) | any (needs `clock_faults`) | shifts one instance's wall clock with libfaketime |
| [`valkey.fault.disk-fill`](faults.md#valkeyfaultdisk-fill) | any | fills the run's tmpfs |
| [`valkey.fault.network-degrade`](faults.md#valkeyfaultnetwork-degrade) | any (needs per-instance addresses) | loss, delay, reordering, duplication, or a rate limit between instances |
| [`valkey.fault.network-partition`](faults.md#valkeyfaultnetwork-partition) | any (needs per-instance addresses) | drops traffic between instances |
| [`valkey.fault.process-signal`](faults.md#valkeyfaultprocess-signal) | any | kills, stops, or pauses an instance |
| [`valkey.fault.resource-pressure`](faults.md#valkeyfaultresource-pressure) | any (needs a delegated cgroup v2) | limits one instance's CPU or memory with cgroup v2 |
| [`valkey.fault.syscall`](faults.md#valkeyfaultsyscall) | any (needs `syscall_faults`) | kills, delays, or fails an instance at its nth matching system call |

### [Observers](observers.md)

| Component | Modes | Does |
| --- | --- | --- |
| [`valkey.observer.info`](observers.md#valkeyobserverinfo) | any | samples INFO and checks expectations |
| [`valkey.observer.log`](observers.md#valkeyobserverlog) | any | scans instance logs |
| [`valkey.observer.log-trigger`](observers.md#valkeyobserverlog-trigger) | any | turns log lines into events |
| [`valkey.observer.trace`](observers.md#valkeyobservertrace) | any | records events and per-node state samples as JSONL |

### [Oracles](oracles.md)

| Component | Modes | Does |
| --- | --- | --- |
| [`valkey.oracle.binary-check`](oracles.md#valkeyoraclebinary-check) | any | each instance runs its assigned binary |
| [`valkey.oracle.cluster-election-safety`](oracles.md#valkeyoraclecluster-election-safety) | cluster | the election model's invariants hold |
| [`valkey.oracle.command-metadata`](oracles.md#valkeyoraclecommand-metadata) | any | the command-table workload's commands do what the build's command metadata says |
| [`valkey.oracle.dataset-digest`](oracles.md#valkeyoracledataset-digest) | any | records a digest of the dataset |
| [`valkey.oracle.deployment-state`](oracles.md#valkeyoracledeployment-state) | any | the deployment's state validators pass, with a chaos action's kills and keys |
| [`valkey.oracle.error-counters`](oracles.md#valkeyoracleerror-counters) | any | no error counter in INFO rose without a cause in the scenario |
| [`valkey.oracle.evidence`](oracles.md#valkeyoracleevidence) | any | requires or forbids evidence |
| [`valkey.oracle.history`](oracles.md#valkeyoraclehistory) | any | the workload's history, single- or multi-client, is linearizable (Porcupine) |
| [`valkey.oracle.ledger`](oracles.md#valkeyoracleledger) | any | data matches the workload ledger |
| [`valkey.oracle.liveness`](oracles.md#valkeyoracleliveness) | any | instances alive, no crash reports or forbidden log lines |
| [`valkey.oracle.quiet-end`](oracles.md#valkeyoraclequiet-end) | any | every instance comes to rest: no blocked client or child process, links up, not spinning |
| [`valkey.oracle.rdb-snapshot`](oracles.md#valkeyoraclerdb-snapshot) | any | captured RDBs reload with plausible data |
| [`valkey.oracle.reply-schema`](oracles.md#valkeyoraclereply-schema) | any | the command-table workload's replies match Valkey's reply schemas |
| [`valkey.oracle.search-results`](oracles.md#valkeyoraclesearch-results) | standalone | valkey-search answers queries as the hashes it indexes say |

### [Providers](providers.md)

| Component | Kind | Does |
| --- | --- | --- |
| [`valkey.artifact.local-binary`](providers.md#valkeyartifactlocal-binary) | artifacts | a `valkey-server` on this host, its SHA-256, version, and git SHA recorded |
| [`valkey.artifact.local-module`](providers.md#valkeyartifactlocal-module) | artifacts | a module (`.so`) on this host, its SHA-256 recorded |
| [`valkey.resource.tmpdir`](providers.md#valkeyresourcetmpdir) | resources | a private directory, removed after the run |
| [`valkey.resource.tmpfs`](providers.md#valkeyresourcetmpfs) | resources | a size-limited tmpfs mount for disk-full testing (Linux, root) |
<!-- END index -->

## Events, evidence, and state changes

What each component declares it publishes (besides the runner's
`<id>.started`, `.completed`, `.failed`, and for roles with a teardown
`.stopping`, `.stopped`, `.stop-failed`), the evidence it emits (one item
per run unless marked many), and the kinds of state it changes, each owned
by an undo obligation. `{config.<path>}` names come from the component's
config. The runner rejects anything else as `ERROR` (see
[Evidence contract](../contracts.md#evidence-contract));
`valkey-fuzzer list components --format json` shows the payload and value
schemas.

<!-- BEGIN declarations: generated by scripts/gen_component_docs.py -->
| Component | Publishes | Emits | Changes |
| --- | --- | --- | --- |
| `valkey.action.bgsave` | `<id>.save-completed`<br>`<id>.save-started` | `valkey.bgsave.result` (many) | none |
| `valkey.action.cluster-chaos` | `<id>.instance-killed`<br>`<id>.wave-validated` | `valkey.chaos.schedule`<br>`valkey.chaos.test-keys`<br>`valkey.instances.expected-down` | none |
| `valkey.action.cluster-election` | `<id>.election-completed`<br>`<id>.election-started`<br>`<id>.injected` | `valkey.cluster.election.result`<br>`valkey.fault.injected` | `process.paused`<br>`process.stopped` |
| `valkey.action.command` | none | `valkey.command.replies` | none |
| `valkey.action.commands` | none | `valkey.command.sequence` (many) | `process.private-server` |
| `valkey.action.failover` | none | `valkey.failover.result` | none |
| `valkey.action.migrate-slots` | `<id>.migration-completed`<br>`<id>.migration-started` | `valkey.cluster.slot-migration.result` | none |
| `valkey.action.poll` | none | `valkey.poll.satisfied` | none |
| `valkey.action.replication-chaos` | `<id>.instance-killed`<br>`<id>.promoted`<br>`<id>.wave-validated` | `valkey.chaos.schedule`<br>`valkey.chaos.test-keys`<br>`valkey.data.loss-window` (many)<br>`valkey.instances.expected-down` | `network.degrade`<br>`network.partition`<br>`process.paused`<br>`process.stopped`<br>`server.client-pause`<br>`server.config` |
| `valkey.action.rolling-upgrade` | `<id>.failed-over`<br>`<id>.instance-upgraded` | `valkey.upgrade.completed` | `process.stopped` |
| `valkey.action.sleep` | none | none | none |
| `valkey.action.slot-migration-chaos` | `<id>.endpoint-killed`<br>`<id>.instance-killed`<br>`<id>.migration-completed`<br>`<id>.migration-started`<br>`<id>.partitioned`<br>`<id>.wave-validated` | `valkey.chaos.schedule`<br>`valkey.chaos.test-keys`<br>`valkey.instances.expected-down` | `network.partition`<br>`process.stopped` |
| `valkey.action.tools` | none | `valkey.tool.runs` | `process.private-server` |
| `valkey.action.wait-event` | none | `valkey.wait.event-observed` | none |
| `valkey.capability.build-probe` | none | `valkey.capability.supported` | `process.private-server` |
| `valkey.capability.host-probe` | none | `valkey.host.supported` | none |
| `valkey.fault.clock-skew` | `<id>.injected`<br>`<id>.released` | `valkey.fault.injected` | `clock.skewed` |
| `valkey.fault.disk-fill` | `<id>.injected`<br>`<id>.released` | `valkey.fault.injected` | `disk.filled` |
| `valkey.fault.network-degrade` | `<id>.degraded`<br>`<id>.healed`<br>`<id>.injected` | `valkey.fault.injected` | `network.degrade` |
| `valkey.fault.network-partition` | `<id>.healed`<br>`<id>.injected`<br>`<id>.partitioned` | `valkey.fault.injected` | `network.partition` |
| `valkey.fault.process-signal` | `<id>.injected`<br>`<id>.recovered` | `valkey.fault.injected`<br>`valkey.instances.expected-down` | `process.paused`<br>`process.stopped` |
| `valkey.fault.resource-pressure` | `<id>.injected`<br>`<id>.released` | `valkey.fault.injected` | `cgroup.limited` |
| `valkey.fault.syscall` | `<id>.injected`<br>`<id>.recovered` | `valkey.fault.injected`<br>`valkey.instances.expected-down` | `process.stopped`<br>`syscall.rule` |
| `valkey.observer.info` | none | `{config.expectations[].evidence_id}` | none |
| `valkey.observer.log` | none | `valkey.log.forbidden-match`<br>`{config.expect[].evidence_id}` | none |
| `valkey.observer.log-trigger` | `{config.rules[].publish}` | `{config.rules[].evidence_id}` | none |
| `valkey.observer.trace` | none | `valkey.trace.recorded` | none |
| `valkey.oracle.binary-check` | none | `valkey.binary.verified` | none |
| `valkey.oracle.cluster-election-safety` | none | `valkey.cluster.election-safety.verified`<br>`valkey.cluster.vote.unsaved-at-crash` | none |
| `valkey.oracle.command-metadata` | none | `valkey.command.metadata` | `process.private-server` |
| `valkey.oracle.dataset-digest` | none | `valkey.dataset.digest` | `process.private-server` |
| `valkey.oracle.deployment-state` | none | `valkey.deployment.validated` | none |
| `valkey.oracle.error-counters` | none | `valkey.error-counters.verified` | none |
| `valkey.oracle.evidence` | none | none | none |
| `valkey.oracle.history` | none | `valkey.data.linearizable` | none |
| `valkey.oracle.ledger` | none | `valkey.data.ledger-verified` | none |
| `valkey.oracle.liveness` | none | `valkey.liveness.verified` | none |
| `valkey.oracle.quiet-end` | none | `valkey.quiet-end.verified` | none |
| `valkey.oracle.rdb-snapshot` | none | `valkey.rdb.reload-verified` | `process.private-server` |
| `valkey.oracle.reply-schema` | none | none | none |
| `valkey.oracle.search-results` | none | `valkey.search.results-verified` | none |
| `valkey.workload.benchmark` | none | `valkey.workload.benchmark` | none |
| `valkey.workload.command-table` | none | `valkey.workload.command-table` | none |
| `valkey.workload.config-churn` | none | `valkey.workload.config-churn` | none |
| `valkey.workload.key-value` | `<id>.drained`<br>`<id>.seeded` | `valkey.workload.ledger` | none |
<!-- END declarations -->
