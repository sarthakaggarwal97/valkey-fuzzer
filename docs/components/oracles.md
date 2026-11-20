# Oracles

Part of the [Valkey component reference](README.md): the
[index](README.md#index) lists every component, [Common config
forms](README.md#common-config-forms) the selectors, predicates, and trigger
fields the tables below use.

Oracles run after teardown, with all the evidence recorded, and judge the
system under test.

## `valkey.oracle.liveness`

Every instance must be running, answer `PING`, and not be loading, within
one shared `timeout_s`. Instances listed in `valkey.instances.expected-down`
evidence (recorded by the chaos actions for the instances they killed and
left down) may be down; any other instance that is not running fails the run.
It also scans what each instance wrote during the run to its log and to its
captured stdout and stderr, including the instances that are down, for
`=== VALKEY BUG REPORT START` (or `REDIS`), `ASSERTION FAILED`, the
assertion's `==> file:line 'condition' is not true`, `Guru Meditation`,
`crashed by signal`, `ERROR: AddressSanitizer`, `ERROR: LeakSanitizer`,
`WARNING: ThreadSanitizer`, and `runtime error: `. Each crash is reported by
its cause, in the summary and as `crashes` (instance to cause) in the
diagnostics: the first assertion's `file:line 'condition' is not true`, else
the panic message, the sanitizer finding, the UBSan error, or the signal (an
assertion raised while the bug report is written is not the cause). An
instance that died unexpectedly without a crash report exited on a fatal
error: its cause is the last warning-level (`#`) line it logged since it
last started that names a fatal exit, such as `Unrecoverable error, aborting
now`, `aborting.`, `Exiting.`, `Fatal error`, or `Terminating server` (a
later `resuming operation` line means it carried on), reported in the
summary as `fatal exit (cause)`. The
[failure signature](../cli.md#minimize) of a run with crashes is those causes.
A `FAIL` for any of these (an instance not running, not answering, still
loading, a crash report, or a fatal exit) reports a crash
(`ComponentResult.crash`), so it stands even when a teardown before the
oracles failed (see [Base contracts](../contracts.md#outcomes-and-verdicts)).
And it fails when a [`valkey.observer.log`](observers.md#valkeyobserverlog) in the run
recorded `valkey.log.forbidden-match`, naming each pattern, the instances
that logged it, and the observer; that alone is no crash.

A run that failed before the oracles judged it, such as one whose server
crashed while the deployment was provisioned or an action restarted it, gets
a post-mortem (see [Base contracts](../contracts.md#outcomes-and-verdicts)):
the same scan of every instance's log and output, the failed provision's
included, and the same crash `FAIL` with the same causes. A server gone
without a crash report (a port in use, a fatal exit, a timeout) leaves the
run's `ERROR` as it was.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `timeout_s` | number, 0.1 to 600 | 30 | |
| `extra_crash_patterns` | list | `[]` | More regular expressions that count as crash reports. |

Evidence: `valkey.liveness.verified`.

## `valkey.oracle.error-counters`

No error counter in INFO rose during the run without something in the
scenario that explains the rise. The oracle needs no model of the feature
under test: [`valkey.liveness`](../scenarios-and-experiments.md#fragments) adds
it to every experiment. It has no config.

It reads `INFO everything` of every running instance at the end of the run. A
process starts with every counter at zero, and the deployment started every
process during the run, so a counter above zero rose during the run, within
that process's lifetime. What an instance counted before a restart is gone
with the process, and an instance that is not running is not read.

It watches the counters whose names say something went wrong: a name with the
word `err`, `error`, `fail`, `failed`, `failure`, `corrupt`, `lost`,
`dropped`, `rejected`, `unexpected`, or `denied` (a module's too), the
`errorstat_<code>` entries, the fields ending in `_status` that read `err`,
and `client_query_buffer_limit_disconnections` and
`client_output_buffer_limit_disconnections`. `total_error_replies` is the sum
of the `errorstat` entries, which are judged one by one. `expired_*`,
`evicted_*`, and the like are not errors.

A rise is explained by a cause, which the oracle derives from what the
scenario did:

| Cause | The scenario |
| --- | --- |
| `links` | broke replication or cluster links: a component stopped, paused, partitioned, or degraded the links of an instance (an [undo obligation](../contracts.md#fault-ownership) of kind `process.stopped`, `process.paused`, `network.partition`, or `network.degrade`), killed one for good or moved a role (`valkey.instances.expected-down` naming an instance, `valkey.chaos.schedule`, or `valkey.failover.result` evidence), or is configured with a command that does (`REPLICAOF`, `SLAVEOF`, `FAILOVER`, or any `CLIENT`, `CLUSTER`, or `DEBUG` command of a [`valkey.action.command`](actions.md#valkeyactioncommand)) |
| `storage` | made writes to disk fail: an undo obligation of kind `disk.filled` |
| `commands` | sent a command that Valkey answered with an error reply: the [`valkey.workload.command-table`](workloads.md#valkeyworkloadcommand-table) workload counted one, [`valkey.workload.config-churn`](workloads.md#valkeyworkloadconfig-churn) had a `CONFIG SET` refused, a configured command's recorded reply is one, or a [`valkey.action.bgsave`](actions.md#valkeyactionbgsave) was refused (`refusals`, or status `rejected`) |
| `memory` | set a `maxmemory` itself, in the deployment's server config, a startup overlay, or a configured `CONFIG SET`; the deployment provider's default is one no experiment's dataset reaches |

Which causes explain which counter:

| Counter | Explained by |
| --- | --- |
| `unexpected_error_replies` (an error reply on a replication link or while loading the AOF) | nothing |
| `client_query_buffer_limit_disconnections`, `rejected_connections`, `rejected_priority_connections` | nothing: no component sends a query near the limit or opens connections near `maxclients` |
| `rdb_last_bgsave_status`, `aof_last_bgrewrite_status`, `aof_last_write_status`, and any other `*_status` that reads `err`; `aof_rewrites_consecutive_failures` | `storage` (a cancelled save and a killed server leave the status `ok`) |
| `sync_partial_err` | `links`; and, on any instance, as many refusals as its shard has replicas: a node made a replica when the deployment forms asks to continue its own history, which its primary refuses once |
| `client_output_buffer_limit_disconnections` | `links` |
| `cluster_*` (FAIL messages, failed nodes and slots) | `links` |
| `acl_access_denied_*` | `commands` |
| `errorstat_OOM` | `memory`, `commands`, `links`, `storage` |
| every other `errorstat_<code>`, and any other watched counter (a module's, a newer build's) | `commands`, `links`, `storage`: once the scenario disturbed the deployment, clients meet `READONLY`, `LOADING`, `CLUSTERDOWN`, `MISCONF`, and the like |

`FAIL` names each unexplained rise, as `valkey-0: unexpected_error_replies
rose to 2`. A running instance whose INFO cannot be read within 30 seconds
makes the oracle `ERROR`, unless another instance's rise is a finding.
Evidence: `valkey.error-counters.verified`, with every risen counter of every
instance read (`value`, `explained_by`) and the scenario's causes, each with
what brought it (`scenario`); a `FAIL` has the same record in its
diagnostics.

## `valkey.oracle.quiet-end`

Once the workload and the faults are over, every running instance comes to
rest within `timeout_s`. [`valkey.liveness`](../scenarios-and-experiments.md#fragments)
adds it to every experiment. An instance is at rest when:

- it has no blocked client (`blocked_clients` is 0);
- no child process or load is in progress: every INFO field ending in
  `_in_progress` (`rdb_bgsave_in_progress`, `aof_rewrite_in_progress`,
  `module_fork_in_progress`, `slot_migration_fork_in_progress`), `loading`,
  and `async_loading` is 0;
- a replica's replication link is up, unless its primary is not running;
- in cluster mode its `CLUSTER NODES` shows every other running node
  `connected` and none failed (`fail`, `fail?`);
- its main thread used at most `max_cpu` of one core over the last
  `cpu_window_s` of wall-clock time: a server that keeps a core busy with
  nothing to do is spinning. The CPU time is `used_cpu_user_main_thread` plus
  `used_cpu_sys_main_thread` where INFO reports them (Linux), else
  `used_cpu_user` plus `used_cpu_sys`, the whole process's, divided by
  `io-threads` when the instance's planned config sets
  `io-threads-always-active yes`: those IO threads spin whenever the main
  thread handles an event, by design. An instance whose active
  defragmentation ran in the window (`active_defrag_running` above 0 at its
  end, or `total_active_defrag_time` grown since its start: a server whose
  fragmentation sits at `active-defrag-threshold-lower` ends and starts cycles
  between two reads) is not held to `max_cpu`: defragmentation uses what its
  cycle settings give it
  (`active-defrag-cycle-min` to `-max` of a core, in duty cycles of
  `active-defrag-cycle-us`), and a duty cycle shorter than the event loop's
  millisecond timeouts leaves the loop polling between them.

The oracle reads every instance once per window until all are at rest in the
same round, so work that ends within the bound (a rewrite finishing, a
replica catching up) is no finding. What is left when the time is up is a
`FAIL` that names it, as `valkey-1: used 0.98 of a core over 1.0s`. An
instance that is not running is not read
([`valkey.oracle.liveness`](#valkeyoracleliveness) judges it); one that did
not answer in the last round, or whose CPU use had no full window, makes the
oracle `ERROR`, unless another instance is a finding.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `timeout_s` | number, 1 to 600 | 30 | How long the deployment has to come to rest. |
| `cpu_window_s` | number, 0.1 to 60 | 1 | The window CPU use is measured over; the oracle takes at least this long. |
| `max_cpu` | number, 0.01 to 64 | 0.5 | The share of one core an instance at rest may use. An idle server uses about 0.01 (its cron at `hz`, replication keepalives, the cluster bus); active defragmentation and expiry may each use up to a quarter. |

Evidence: `valkey.quiet-end.verified`, each instance's last `cpu` share.

## `valkey.oracle.ledger`

Reads every key the workload touched and checks that its value is one the
ledger allows: the value of the last acknowledged write, or of a later write
whose outcome was ambiguous. Inside a loss window an acknowledged write may
also be lost, including a preloaded key whose batch was acknowledged inside
it. A loss window runs from `loss_window_s` before a fault was
applied until `loss_window_after_s` after it ended: a partition ends when it
heals, a kill when the process is confirmed gone (`exited_at`), and a forced
[failover](actions.md#valkeyactionfailover) when it completes. Reads narrow the allowed
values: a value a read proved overwritten may not reappear, so reads are
monotonic. With `check_reads`, every acknowledged read must have returned a
value acceptable at that time. With `check_replicas`, every replica must
converge on its primary's values. Reads are retried within
`converge_timeout_s`; a read that keeps failing (a timeout, `LOADING`,
`CLUSTERDOWN`, an unreachable replica) makes the oracle `ERROR`, not `FAIL`:
the harness could not read, and [`valkey.oracle.liveness`](#valkeyoracleliveness)
judges a server that is down.

With a workload of several `clients` the operations overlap and the ledger
holds them in completion order, not the order the server applied them. The
oracle then accepts, for a key, the value of every write that overlaps the last
acknowledged one, and does not check reads (`unchecked_reads` counts them):
[`valkey.oracle.history`](#valkeyoraclehistory) orders them by overlap.

Any component can also publish `valkey.data.loss-window` evidence `{start,
end, instance_id?}`, as `valkey.action.replication-chaos` does for a forced
promotion: writes acknowledged inside `(start, end]` may be lost, with
`instance_id` (and `loss_window_scope: fault_targets`) only those that
instance acknowledged. The window fields below widen and filter these
windows like fault windows (`loss_window_faults: []` turns them all off); a
window without numeric `start` and `end` permits no loss. When the workload
ran with `wait_replicas`, a write whose `WAIT` reply counted every planned
replica is never lost, inside a window or not.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `workload` | string | | The workload component's `id`. |
| `acknowledged` | `must_survive` or `may_be_lost` | `must_survive` | `may_be_lost` allows any acknowledged write to be lost. |
| `loss_window_s` | number, 0 to 3600 | 0 | Acknowledged writes that completed from this long before a fault was applied ... |
| `loss_window_after_s` | number, 0 to 3600 | 0 | ... until this long after it ended may be lost. |
| `loss_window_until` | list | none | Components whose evidence marks when a fault's effect ended, such as a poll that saw the isolated primary demoted: each fault window runs at least until their last evidence was recorded. |
| `loss_window_faults` | list or null | `null` | The faults (and forced failovers) whose windows count; all of them by default. |
| `loss_window_scope` | `all`, `fault_target_slots`, or `fault_targets` | `all` | `fault_target_slots` limits the windows to keys in the slots of the shards the faults targeted (cluster mode). `fault_targets` also lets only writes the faults' targets acknowledged be lost. |
| `lost_writes_budget` | integer, at least 0 | 0 | Keys allowed to be missing. |
| `check_reads` | boolean | `true` | |
| `window_reads` | `strict` or `written` | `strict` | `written`: inside a loss window, a read only has to return some value written to the key. |
| `check_replicas` | boolean | `true` | |
| `converge_timeout_s` | number, 0 to 600 | 30 | How long the dataset read is retried, and one budget shared by all replicas. |

With `acknowledged: must_survive`, the scenario does not compile when:

- a forced failover is not listed in `loss_window_faults` (when that list is
  set), because it can lose writes the old primary acknowledged;
- every instance runs `appendonly yes` with `appendfsync always`, the windowed faults are all
  `valkey.fault.process-signal`, no replica can take over (standalone, or no
  replicas), and `loss_window_s` or `loss_window_after_s` is above 0. Such a
  crash cannot lose an acknowledged write;
- the same, but with `appendfsync everysec` and a `SIGKILL`, and
  `loss_window_s` is below 2.5: everysec may lose what it acknowledged up to
  two seconds before the kill.

Both read each instance's effective config, the local deployment's
defaults (`appendonly yes`, `appendfsync everysec`) included.

`FAIL` when a key holds a value outside its acceptable set, more keys are
missing than the budget allows, a read returned an unacceptable value, or a
replica does not converge. Evidence: `valkey.data.ledger-verified`, with
`wait_confirmed_replicas` and `wait_protected_ops` under `wait_replicas`.

## `valkey.oracle.history`

Checks that the workload's history is linearizable: that every mutable key
behaves as one register, each operation taking effect at one instant between
its invocation and its completion. It reads every mutable key once more,
appends those reads to the ledger of the `ledger` oracle's workload, writes
the history to `history.jsonl` in its artifact directory, and hands it to
[Porcupine](https://github.com/anishathalye/porcupine), which checks each
key's history separately. Beyond the ledger oracle's final read, this
catches a read that returned a value already overwritten (a stale read
after a failover or a restart) and an acknowledged write that a read saw and
that later vanished, through any order of concurrent and indeterminate
operations.

**Scope of the claim.** What a PASS covers depends on the workload's
`clients`, which the oracle reads from the workload's evidence and records
(`clients`). With `clients: 1` (the default) no two operations overlap, so a
PASS reads "linearizable (single-client history)" and proves "no stale read
after a failover or a restart" for that one client, which the ledger oracle's
sequential model finds too. With `clients` above 1, operations on a key
overlap, Porcupine searches for an order of them, and a PASS reads
"linearizable (N-client history)". Neither covers replica reads (`READONLY`),
`WAIT`, or client tracking: the workload reads only the primary, and replica
reads are not linearizable against it, so they are not part of this history.
Set `min_clients` to make the oracle `ERROR` when the workload ran fewer
clients than the scenario needs, and the scenario fail to compile when the
workload's `clients` is below it.

It judges under the loss policy of the [`valkey.oracle.ledger`](#valkeyoracleledger)
component named by `ledger` (its loss windows, scope, `window_reads`, and
`WAIT` protection), so it never flags a loss that policy allows:

- an ambiguous write (a timeout or a lost connection), and an acknowledged
  write the policy lets be lost, are indeterminate: each may take effect at
  any point after its invocation, or never; one whose value no checked read
  returned is left out, as never applying it changes no verdict;
- a read that returned the value of a write the policy lets be lost is not
  checked, since the write may be lost after the read; with
  `window_reads: written`, neither is a read that completed inside a loss
  window;
- a rejected write (`error`) is left out: it was not applied;
- the preload is acknowledged writes on connection 0; stable keys are left
  to the ledger oracle.

The scenario does not compile when `ledger` is not a `valkey.oracle.ledger`
component, or when that oracle allows lost writes (`lost_writes_budget`) or
skips reads (`check_reads: false`), which a history check cannot honor.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `ledger` | string | | The `valkey.oracle.ledger` component's `id`; its `workload` and `converge_timeout_s` apply too. |
| `timeout_s` | number, 1 to 3600 | 300 | How long Porcupine may search. |
| `min_clients` | integer, 1 to 64 | 1 | The fewest workload `clients` this check is worth; fewer is `ERROR`. |

`FAIL` when a key's history is not linearizable, naming the keys
(`illegal_keys`) and writing Porcupine's visualization of their histories
to `illegal.html` (`visualization_path`). `ERROR` when the `porcupine`
checker is neither on `PATH` nor fetched, the final read keeps failing, the check times out,
or the history is too large to check: Porcupine's memory grows with the
square of a key's operation count (about 370 MB for 50,000 operations on one
key), and the oracle refuses a history whose estimate exceeds 2 GB. Evidence:
`valkey.data.linearizable`, with the operation, key, indeterminate, and
unchecked-read counts, `clients`, and `history_path`.

The `porcupine` checker is a small Go program in the repository's
`porcupine/` directory that wraps the Porcupine library with a register
model. Once per host, fetch the prebuilt binary of the pinned release into
`~/.cache/valkey-fuzzer/bin` (its SHA-256 is checked against
`valkey_fuzzer/components/porcupine_sha256sums.txt`), or build it onto `PATH`
with Go 1.21 or later:

```bash
valkey-fuzzer-fetch-porcupine
go -C porcupine build -o "$HOME/.local/bin/porcupine"
```

The oracle uses the one on `PATH`, else the fetched one
([Get porcupine](../getting-started.md#get-porcupine)).

The [`valkey.history`](../scenarios-and-experiments.md#fragments) fragment adds
it, as `history` with `ledger: data`.

## `valkey.oracle.rdb-snapshot`

Loads every RDB a `valkey.action.bgsave` captured into a private server
running that instance's binary and loading its modules (the `loadmodule` of
its server config, unless `load_config` sets one). Every bgsave target must
have produced an RDB.
With `workload`, each RDB must hold only keys the workload wrote, every stable
key must hold its value, the RDB must be a consistent cut of the write
history taken while `BGSAVE` ran, and each of the workload's lists it holds
must have the length and elements the workload wrote (`lists.jsonl`). The cut
allows no lost write, so `workload` does not compile next to a component that
can lose writes Valkey acknowledged: a `valkey.fault.process-signal` other
than `SIGSTOP`, a `valkey.fault.syscall` whose `action` is `kill`, a
`valkey.action.failover` with `force: true`, or a chaos or rolling-upgrade
action; judge such a run with [`valkey.oracle.ledger`](#valkeyoracleledger)
and its loss windows. A bgsave with `capture_rdb: false` is rejected.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `bgsave` | string | | The `id` of the `valkey.action.bgsave` whose captured RDBs to check; checked when the scenario compiles. |
| `workload` | string or null | `null` | The workload's `id`. |
| `timeout_s` | number, 1 to 3600 | 60 | |
| `load_binary` | string or reference, or null | `null` | Load with this binary instead of the saving instance's. |
| `load_config` | mapping | `{}` | Server config of the loading server, such as `{rdb-version-check: relaxed}`. |
| `expect` | `loads` or `refused` | `loads` | `refused`: the loading server must refuse every RDB cleanly (exit status 1, `Fatal error loading the DB`, no crash report); a configuration error is not a refusal. |
| `refused_log` | regex or null | `null` | With `expect: refused`: the refusal must log this. |

`FAIL` when a target captured no RDB, one fails to load (or loads when it
must be refused), or its contents are wrong. `ERROR`
when the loading server did not start or was still loading after
`timeout_s`: whether the RDB loads is unknown; and when a target's save was
`unanswered` or `unavailable`, or its RDB could not be copied.
Evidence: `valkey.rdb.reload-verified` (per-snapshot results and a
`content_digest` of the loaded data).

## `valkey.oracle.deployment-state`

Runs the deployment's state validators at the end of the run, with retries:
replication links, cluster state, slot coverage, roles (the planned number
of primaries and replicas), cluster views, data consistency, and node logs.
Outside cluster mode only the replication check (when there are replicas)
and the role check run, and the role check runs even with
`check_roles: false`. After a chaos action (`valkey.cluster-chaos`,
`valkey.replication-chaos`) these are the validators the action runs after
every wave: the action's kills count as stopped on purpose, the data check
reads its test keys, and the log check reads the logs of the shards its
failovers touched. Without the action's test keys the data check is off (a
note says so), and without its schedule the log check is.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `validation` | mapping | `{}` | The [state validation](../cluster-chaos-and-ci.md#state-validation) settings, as the chaos actions take them (`stabilization_wait`, `validation_timeout`, the `check_*` flags, ...). `valkey.base` sets `stabilization_wait: 1` and `validation_timeout: 60`. |
| `chaos` | string or null | `null` | The chaos action whose kills, failovers, and test keys to use; any by default. |

`FAIL` when a check finds the deployment wrong; `ERROR` when the validation
stayed [inconclusive](../cluster-chaos-and-ci.md#state-validation) (a running
instance did not answer, or the time ran out). A `validation` whose
`stabilization_wait` is not below its `validation_timeout` does not
compile. A wave whose
validation stayed inconclusive is an error of the chaos action, not a
failure. Evidence: `valkey.deployment.validated`, the validation summary
with the oracle's `notes`.

## `valkey.oracle.cluster-election-safety`

Cluster mode, Valkey 9.2 or later (the first release that logs each counted
vote, `Failover auth ACK from ... (ACKs N, quorum Q)`; on an older build the
quorum check cannot run, so every run that elected is `ERROR`). Checks the
invariants of the
[TLA+ election model](../../formal/cluster-election/README.md) on the live run, from vote and
win events published by a log-trigger observer, the crash faults' records,
the trace, and the final cluster views:

- **VoteOncePerEpoch:** no primary voted for two candidates in one epoch;
- **OneWinnerPerEpoch:** no epoch promoted two replicas;
- **Win:** every winner counted distinct votes from a quorum (the larger of
  its logged quorum and a majority of shards), from the
  `Failover auth ACK` lines Valkey logs since 9.2;
- **votes saved before they leave:** when durable votes are required, no
  vote that a crashed primary had not saved to `nodes.conf` reached a
  candidate before the primary's new process was spawned (an ACK logged
  after that may be the new process's own vote, even before it is ready);
- **no split primary:** no slots claimed by two primaries, and no two
  primaries with overlapping slots and the same `configEpoch`;
- the views converge on one owner per slot.

The first two are claimed by the bundle's `claims.yaml` only under its
`holds_when` constants (`PersistBeforeAck`, from every node running
`cluster-config-save-behavior sync`; `FlushBeforeSave`, pinned). The oracle
derives the same constants from the nodes' settings and the plan's inputs,
as [`spec conform`](../spec-bridge.md#spec-conform) derives them from a run, so
the two agree: a double vote or two winners under `best-effort` is recorded
under `outside_claim` and in the summary, not held against the run. The
other checks are not conditioned. A win's epoch is the one of the election
its winner last started (the `Failover election won` line names none).

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `trace` | string or null | `null` | The trace observer's `id`. |
| `crash_faults` | list | `[]` | Fault `id`s whose kills to check. |
| `spec` | string | `cluster-election` | The [spec bundle](../spec-bridge.md) whose `mapping.yaml` names the election, vote, ACK, and win events (its `started`, `granted`, `acked`, and `won` evidence) and their fields, and whose `claims.yaml` says when `VoteOncePerEpoch` and `OneWinnerPerEpoch` are claimed. |
| `spec_sha256` | string or null | `null` | Filled in when the scenario compiles: the SHA-256 of `spec`'s `mapping.yaml`, so the plan pins the mapping it was compiled against. A run whose mapping has another digest stops with `ERROR`; compile the scenario again. |
| `require_durable_votes` | `auto`, `always`, or `never` | `auto` | `auto` requires them when every node runs `cluster-config-save-behavior sync`; a node whose setting cannot be read then leaves a vote that left before it was saved unjudged (ERROR). |
| `converge_timeout_s` | number, 0 to 600 | 60 | |

`ERROR` when elections were won but no ACK line was published as the
mapping's `acked` event, so the quorum check cannot run (the oracle needs
Valkey 9.2 or later, which logs `Failover auth ACK from ... (ACKs N, quorum
Q)`; an older build gives this `ERROR` on every run that elected); when a win
follows no `started` event of its winner, so its epoch is unknown; when a
double vote or two winners were found but a node's setting the claim depends
on could not be read; when the views did not converge while a running node's
view could not be read, or while the views were still changing at the
`converge_timeout_s` deadline (a liveness bound reached, not a disagreement:
views that hold still and disagree are the `FAIL`); and when `trace` names an
observer whose trace is missing or unreadable.

Evidence: `valkey.cluster.election-safety.verified`; also
`valkey.cluster.vote.unsaved-at-crash` when a crash caught a vote its
`nodes.conf` did not hold.

## `valkey.oracle.binary-check`

Each instance's `INFO server` must report the executable, version, git SHA,
and process ID of the binary and process the deployment assigned it, and at
least `min_distinct_builds` distinct builds must be running. Reads are
retried for 30 seconds; a running instance that never answers makes the
oracle ERROR, and one that is not running is left to `valkey.oracle.liveness`.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `min_distinct_builds` | integer, 1 to 64 | 1 | Distinct (version, git SHA) builds that must be running. |
| `expect_binary` | string, reference, or null | `null` | Every instance must be assigned this binary. |

Evidence: `valkey.binary.verified`.

## `valkey.oracle.dataset-digest`

Records a canonical digest of every primary's dataset (all keys, with values
compared by type), for comparing builds, and writes a key listing as
`keyspace.jsonl`. The digest itself never fails a run. The oracle fails only
when no instance is a primary, in cluster mode a key is held by two primaries
(a key of a slot that `CLUSTER NODES` shows migrating or importing is on both
sides until the handoff: it is listed in the evidence's `migrating_keys`, not
failed), or, with
`include_replicas`, a replica does not converge on its primary's digest.
Reads are retried within `converge_timeout_s`; an instance that stays
unreachable or unreadable makes it `ERROR`, since a digest that left it out
would compare as a different (or the same) dataset.

A value of a module's type is digested from its `DUMP` payload, less the
trailing 2-byte RDB version and 8-byte CRC64, so the same value written by two
builds digests the same. This compares the module's serialization, not its
logical data: a module that changes its format between builds changes the
digest without a bug. A module's save may write a hash table in iteration
order, which differs between processes, so before judging any digest the
oracle probes each module type it found: one key's payload is `RESTORE`d on
two private servers of its primary's binary, started with the deployment's
`loadmodule`, and the two `DUMP`s must be the same. A type that serializes
differently is `ERROR` ("this module's serialization is not deterministic
across processes"), never a `FAIL` across replicas.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `match` | string or null | `null` | A `SCAN` pattern. |
| `include_replicas` | boolean | `false` | |
| `converge_timeout_s` | number, 0 to 600 | 30 | |
| `max_keys_listed` | integer, at least 0 | 100,000 | Keys written to the listing. |

Evidence: `valkey.dataset.digest` (`digest`, key count, per-instance digests,
and `module_types`, the module types it digested and probed).

## `valkey.oracle.command-metadata`

Holds the commands of a
[`valkey.workload.command-table`](workloads.md#valkeyworkloadcommand-table) workload to
what the build declares about them. `COMMAND` lists every command's arity,
flags, and key specs (a module's commands too), and `COMMAND GETKEYS` names
the keys of one command. Clients, proxies, ACL rules, and the cluster's
routing act on what they say, so a command that does otherwise is a bug in
Valkey: valkey-io/valkey#4936 is one, an `MSETEX` that has keys by its key
specs and none where the cluster routes it.

Where the workload sends commands (its `rate` is above 0), the oracle starts
two private servers of the deployment's build, with its `loadmodule`,
`enable-debug-command yes`, and an AOF: node 0 with node 1 as its read-only
replica, or on a cluster deployment node 0 owning every slot and node 1
owning none. It draws the workload's first `commands` commands again from
the run's seed, with the workload's `prefix` and `keys`, so they are the
same commands in the same order, and sends each to node 0, and to node 1 as
the checks below say. Nothing else writes to these servers, so whatever
changes there, a command changed.

| Check | Deployment | What must hold | Finding |
| --- | --- | --- | --- |
| `keys` | any | `COMMAND GETKEYS` names the keys the command's key specs find (`begin_search` and `find_keys`), and a command without the `movablekeys` flag has them where `first`, `last`, and `step` say. | `keys-mismatch` |
| `arity` | any | A command with a number of arguments its arity excludes replies `wrong number of arguments`. | `arity-accepted` |
| `readonly` | any | A command with the `readonly` flag leaves node 0's `DEBUG DIGEST` as it was and propagates nothing: its `master_repl_offset` and the bytes fed to its AOF stay the same. | `readonly-wrote` |
| `replica` | standalone | The replica refuses a command with the `write` flag with `READONLY`, and does not refuse one with the `readonly` flag. | `replica-wrote`, `replica-refused` |
| `routing` | cluster | Node 1, which owns no slot, answers a command `MOVED` to the slot of the keys and shard channels it declares (`CROSSSLOT` when they are of several slots), and serves a command that declares none. | `misrouted` |

What is left unjudged, each for what Valkey itself says or does by design:

- `keys`, and `routing` with them: a command whose key specs do not say
  where its keys are (an `incomplete` or `unknown` spec, as `SORT`'s), and
  arguments `COMMAND GETKEYS` refuses (their count, a `numkeys` beyond
  them). `keys` alone: a module command that finds its keys by a callback
  (`getkeys-api`: the `movablekeys` flag and no key spec), which is routed
  by the keys `COMMAND GETKEYS` names.
- `readonly`: a command whose own key specs declare a key written
  (`PFCOUNT`'s says it may change its key and propagate); a script
  (`EVAL_RO`, `FCALL_RO`), which runs other commands; a command during
  which a key or a hash field expired, by `expired_keys` and
  `expired_fields` (a key found expired is deleted, and the deletion
  propagated); and a digest that changed with nothing propagated while a
  hash holds fields that expire, since `DEBUG DIGEST` leaves a field out
  from its expire time on. A transaction is judged as a whole, when every
  command in it has the flag.
- `replica`: an error Valkey replies before it reads a command's flags
  (its arity).
- `routing`: an error node 0, which owns the slot, replies too. Valkey
  refuses a wrong number of arguments before it routes, and looks for no
  keys in arguments it cannot parse (an `XREAD` with more streams than
  IDs), letting the command say so. A command queued in a transaction has
  no reply of its own from node 0, so any error of its own is left alone.
  So is `CLUSTERDOWN`, from a node 1 that got no ping from node 0 in time.
- A command with the `pubsub` flag confirms each channel with a reply of
  its own, so the next command takes a new connection.

`FAIL` names each command that broke its metadata once, in the same words
whatever its arguments (`MSETEX was not routed by the keys it declares on a
node that owns no slot`), so a batch groups runs by the commands at fault;
`diagnostics.findings` holds the first occurrence of each: its `reason`,
`name`, `index` (the command's line in the sequence), `command`, what the
build `declared`, what the oracle `observed`, and `count`. A server that
exits is a `FAIL` and a crash; a command that got no reply, or servers that
did not start, is `ERROR`.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `workload` | string | | The `valkey.workload.command-table` component whose commands it sends again. |
| `commands` | integer, 1 to 1,000,000 | 1000 | How many of them, from the first. |
| `timeout_s` | number, 0.1 to 3600 | 10 | Client timeout per command, and how long the private cluster may take to form. |

The `valkey.command-table` fragment adds it as `command-metadata`.

Evidence: `valkey.command.metadata`, with `commands` (how many it sent),
`judged` (how many each check judged), `findings`, and `sequence`, the path
of `commands.jsonl`: every command sent to node 0, one JSON array per line,
to send again by hand. The artifacts also hold both servers' logs.

## `valkey.oracle.search-results`

Judges valkey-search's query results against the hashes it indexes. On every
instance not stopped on purpose, it reads the hashes under the index's key
prefix and answers the same random queries, drawn from the run's seed, twice:
by `FT.SEARCH ... NOCONTENT` on that instance, and by evaluating each query
over the hashes in the client. The queries combine tag matchers (exact and
prefix, several values), numeric ranges (open, closed, infinite) and text
matchers (term, prefix, suffix, fuzzy, exact phrase; in one text field or in
all of them) with AND, OR, and negation, which also selects the keys without
the field. A share of
them are KNN queries of a `FLAT` vector field, whose search is exact, with a
filter or over every key, and with each `HYBRID_POLICY`. A filter query must
return exactly the matching keys and their total. A KNN query must return
`min(K, candidates)` distinct candidates, none farther than the Kth nearest,
so ties may go either way.

The index is built in the background, so each instance is retried within
`converge_timeout_s` until it agrees; one that still differs is `FAIL`, with
each differing query, its missing and unexpected keys, and the servers'
errors for any query it rejected. A hash the oracle cannot evaluate as
valkey-search would (a numeric field that is not a number, a vector of the
wrong size, text with a backslash escape or a non-ASCII character) makes it
`ERROR`. Tags are split on `,` and compared without case, as a `TAG` field
without `SEPARATOR` or `CASESENSITIVE` does. Text is split at whitespace,
control characters and the default English punctuation, lowercased, and
stripped of the default English stop words (those of valkey-search's
`english.h`, whose documentation leaves out "the"), which take no position; a
suffix matcher searches only the fields declared `WITHSUFFIXTRIE`; with any
text field every query is `VERBATIM`, so a term matches only the word itself,
and fuzzy words are within the optimal string alignment distance.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `index` | string | required | The index queried. |
| `prefix` | string | required | The index's key prefix. |
| `tag_fields` | list of strings | `[]` | Its `TAG` fields. |
| `numeric_fields` | list of strings | `[]` | Its `NUMERIC` fields. |
| `text_fields` | list of strings | `[]` | All its `TEXT` fields, as a text matcher of no field searches them all. |
| `suffix_fields` | list of strings | `[]` | Those declared `WITHSUFFIXTRIE` (not the default), which suffix matchers search. |
| `vector_field` | string or null | `null` | A `FLAT`, `FLOAT32`, `L2` vector field. |
| `dim` | integer, 0 to 4,096 | 0 | The vector field's dimensions. |
| `queries` | integer, 1 to 10,000 | 200 | Queries per instance. |
| `knn_share` | number, 0 to 1 | 0.3 | The share of KNN queries, with a vector field. |
| `converge_timeout_s` | number, 0 to 600 | 60 | |

Evidence: `valkey.search.results-verified` (`index`, the query count, and
each instance's key count).

## `valkey.oracle.reply-schema`

`FAIL` when a reply the
[`valkey.workload.command-table`](workloads.md#valkeyworkloadcommand-table) workload
held to its command's reply schema did not match it. That is a finding about
Valkey either way: the reply is wrong, or the schema Valkey declares for the
command is. The summary names each such command and protocol (`ZRANGE
(RESP2) reply does not match its reply schema`), so runs group by them; the
diagnostics hold the workload's `schema_mismatches`, with the command, the
reply, what in it does not match, and the schema. `ERROR` when the workload
recorded no evidence.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `workload` | string | | The `id` of the command-table workload. |

The `valkey.command-table` fragment adds it as `reply-schemas`.

## `valkey.oracle.evidence`

Judges what other components recorded: `FAIL` when required evidence is
missing or forbidden evidence was recorded. Observers only record, so this is
where an experiment checks behaviour an observer saw, such as a partial
resync counted or no save left in progress.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `require` | list | `[]` | Rules `{evidence_id, source, min_count}`: the evidence `source` (a component `id`) must have recorded at least `min_count` times (at least 1, default 1). |
| `forbid` | list | `[]` | Rules `{evidence_id, source}`: evidence `source` must not have recorded. At least one `require` or `forbid` rule. |
| `preconditions` | list | `[]` | Rules `{evidence_id, source, min_count}`: evidence the run must have recorded for the other rules to apply. Without it the run never reached the state they are about, and the oracle returns `ERROR`. |

Every rule's `source` must be a component the oracle depends on (in its
`depends_on`, directly or through others), and must declare the rule's
`evidence_id`; otherwise the scenario does not compile. A source that could
not read the server returns `ERROR`, and a `FAIL` that depends on it is not
trusted. A misspelt ID would otherwise make a `forbid` rule pass vacuously
and a `require` rule fail every run.

```yaml
- use: valkey.oracle.evidence
  id: psync
  depends_on: [info, logs]
  config:
    require:
      - {evidence_id: valkey.replication.partial-resync-counted, source: info}
      - {evidence_id: valkey.replication.psync-accepted-logged, source: logs}
```
