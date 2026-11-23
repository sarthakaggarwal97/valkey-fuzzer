# Changelog

## Unreleased

### Changed: the command sequence is its own module, and its checks tokenise once (PR-R3-6)

- `valkey.action.commands` and everything it runs on (`CommandsAction`, `RawConnection`, the sides and their
  private servers, the verdict helpers, its evidence declarations) moved from `components/actions.py` (2,564
  lines) to `components/command_sequence.py` (1,662), unchanged; `actions.py` (941) keeps the other actions.
  `checks.py`, `command_table.py`, `tools.py` and `command_metadata.py` import the sequence machinery from the
  new module and nothing from `actions`. The component's id, config, evidence and registration order by id are
  the same.
- `checks.py` tokenises each command once (`_Checks.words`) instead of once per predicate per check (24 call
  sites of `_words` are two), reads the CONFIG SET pairs of a command in one place (`_config_sets`) instead of
  six, and says once why a sequence may not have a replica of the checks' own (`_Checks.replica_blocker`): the
  `replica`, `concurrency` and `faults` checks each said it themselves. The `acl` check's skip is the shared
  `_manages_users`. Same skips, same reasons, same outcomes, with one exception: the `faults` check of a sequence
  whose `server_config` sets `masterauth`, `masteruser` or their `primary*` aliases ran a replica of its own,
  which could not AUTH to the check's primary, and errored "the replica did not sync before the sequence"; it now
  runs on the single node, as the `replica` and `concurrency` checks already left the replica out (fuzzer#147).
- One `info_fields()` (`components/server.py`) parses INFO and CLUSTER INFO replies, where `checks.py` and the
  sequence's cluster formation and restart each had a parser; the faults' restart record is written by
  `processes.record_restart()` and declared by `processes.RESTART_FIELDS`, which `valkey.fault.process-signal`
  and `valkey.fault.syscall` had copied; and `_validate_commands` validates each command through
  `_validate_sequence_item`, with the same messages in the same order.

### Changed: the Valkey pack registers the local deployment provider, and the spec bridge imports plainly (PR-R3-5)

- `build_catalog` registers no deployment provider of its own: the `valkey` pack registers the local
  deployment's (`valkey-fuzzer.deployment.local`, for `standalone` and `cluster`) with its components, as
  `docs/extending.md` has packs do, and `valkey_fuzzer/catalog.py` no longer imports `valkey_fuzzer.deployment.local`.
  A catalog built without a pack has no provider; `build_catalog(deployment_providers=...)` and
  `RunService(deployment_providers=...)` still register the caller's. `build_catalog()` is unchanged, since the pack
  loads them.
- The spec bridge's tools are imported where they are used: `valkey_fuzzer/commands/spec.py` and
  `valkey_fuzzer/batch/conform_plugin.py` import `spec.check`, `spec.conform` and `spec.translate` at the top, and
  `valkey_fuzzer.spec` no longer resolves names on first use; import each from its module (`spec.bundle`,
  `spec.check`, `spec.conform`, `spec.tla`, `spec.translate`). The three modules cost 1.6 ms to import; `spec.bundle`
  and `spec.tla` were already imported by every catalog build. What holds from the lazy loading: a base install
  needs neither Java nor `tla2tools.jar` (importing the tools runs nothing), `run`, `validate` and `batch` work
  without them, and a `spec` command without them exits `2` with the message naming what is missing and the jar's
  download command. A `RunService` used as a library still loads `spec.conform` only for a run whose plan names a
  bundle.

### Changed: the batch's plug-ins are entry points, and the command prints what each describes (PR-R3-4)

- The bug zoo and spec conformance are no longer registered by `valkey_fuzzer.batch` by name: the batch registers
  the `valkey_fuzzer.batch_plugins` entry points, one per pack and named after it, each a function returning the
  pack's `BatchPlugin` instances, in name order (the fuzzer's own is `valkey`, `valkey_fuzzer.batch:shipped_plugins`).
  A pack can now bring a batch source or plug-in (docs/extending.md, "Ship a pack").
- `BatchPlugin.describe(entry)` gives the lines `valkey-fuzzer batch` prints of the plug-in's summary entry; the
  zoo's case verdicts and the `spec conform` digests moved from the command into their plug-ins, so the command
  names neither. `Verdict.with_leaks(leaks)` is the leak adjustment of a finished job's verdict.
- No change to `batch.json`, `jobs.json`, `results/*.json`, `conformance/*.json`, `summary.json`, `batch.log`, the
  console output, or the exit codes for the same seeds.

### Added: chaos during slot migration (PR-C1)

- `valkey.action.slot-migration-chaos`, the slot-migration mode of the wave engine: each wave moves one hash slot
  with `CLUSTER MIGRATESLOTS` (`migrate {key, shard, timeout_s}`; without a shard the slot goes to the shard
  after its owner's, round and round) and disturbs the migration with `kill` of its `source` or `target` (or a
  role or instance, under cluster chaos's kill safety), a `partition` between the endpoints
  (`{target: source, peers: target}`; Linux, root, iptables, per-instance loopback), a `cancel` (`CLUSTER
  CANCELSLOTMIGRATIONS` on the source), and `restart` of a killed instance. Each of the first three may wait for
  a phase of the migration (`at`: `started`, `snapshot`, `handoff`, `finished`), which the migrate operation
  reads from both endpoints' `CLUSTER GETSLOTMIGRATIONS` every 10 ms, and from the bundle's `slot-migration.*`
  events when the experiment publishes them; its record says the phase it waited for, the one it saw, whether
  it missed it, and for a kill the phase at the signal. After every wave the heal removes the partitions and
  restarts the killed instances, and the validation runs the deployment's state checks (the cluster converges,
  every slot has one owner in every view) and reads back every key the action wrote into the migrated slots
  before the first wave (`keys_per_slot`, default 20) through a slot-routing client: a missing or changed key
  fails the wave (`slot_keys` in its validation) and is written again for the next. A migration that did not
  succeed is tolerated only when a kill, partition or cancel of its wave landed before it ended.
- `valkey.slot-migration-chaos`, the experiment: the migration's steps published with the slot-migration
  bundle's patterns (so `spec conform slot-migration` replays its runs), the trace, the action, and the final
  deployment-state oracle. Its default waves need no root: the target killed at the handoff, the source killed
  as it snapshots and restarted, a cancel mid-way, an undisturbed migration. `examples/valkey/slot_migration_chaos.yaml`
  runs them; `--random` draws the deployment and `keys_per_slot` and keeps the waves. The live test
  `slot-migration-chaos-cluster` runs them on a real cluster (`tests/test_live_valkey_experiments.py`).
- `formal/slot-migration/mapping.yaml`: a `finished` evidence (event `slot-migration.finished`, the
  `finished. State: <state>` line `finishSlotMigrationJob` logs on each endpoint, with the `operation` and
  `state` captured), for faults to trigger on once a migration ended and for conform's record of how each side
  ended; the `paused` pattern also matches 9.0.0's line (no `remaining_repl_size`); the comments name the lines
  faults trigger on (start, handoff, end, cancel) and the Valkey versions they come from (9.0.0 to 9.1.3 and
  unstable). `scenario.unmapped` names the chaos action's operations as the real components of `Cancel`,
  `DropLink` and `LoseBus`, and says why the spec experiment carries only `CrashT`: a scenario injects every
  fault component of its plan, and no counterexample takes two of these actions. `valkey.slot-migration-spec`
  publishes the new event. A `crash-wave` evidence reads the chaos action's `<id>.endpoint-killed` event with
  `endpoint: target` as the model's `CrashT`, so `spec conform slot-migration` replays a one-wave chaos run
  (its transition check holds; the model has one migration, so a run of several waves is reported as a
  violation at its second).
- Wave operations: `partition` takes `peers` (`others`, the default and the only value outside the
  slot-migration mode); `restart` runs in the slot-migration mode too. `WaveRun.resolves_later` lets a mode
  pick an operation's target when it runs rather than before the wave.
- Fixed: `valkey.action.migrate-slots` read the oldest export job of its slot from `CLUSTER GETSLOTMIGRATIONS`,
  which lists the newest first, so a second migration of a slot its source had exported before was judged by
  the old job. It reads the newest now (`job_of`), and `migration_endpoints` and `hash_tag` in
  `slot_migration.py` are shared with the chaos action.


### Added: degraded links with tc netem (PR-C3)

- `valkey.fault.network-degrade`: packet loss, delay with jitter, reordering, duplication, and a rate limit on the
  links between selected `instances` and their `peers`, for `duration_s` or until `heal_event`, then healed. The
  netem qdisc is the root of an `ifb` device of the fault's own, named after the fuzzer process
  (`vf<pid><start><n>`), and flower filters on `lo`'s ingress redirect to it only the TCP packets whose source and
  destination are both the exact addresses of an instance pair; nothing else on the host matches (the fault refuses
  127.0.0.1, a non-IPv4 or non-loopback address, or two instances on one address before any call). The filters of a
  fault sit at one claimed `pref`; `lo`'s ingress qdisc is added only when missing, marked as the fuzzer's, and
  removed by whichever fuzzer process removes the last filter, under a lock in `/tmp/valkey-fuzzer`. Healing removes
  the filters, then the qdisc when the fuzzer added it and it is empty, then the device. The host probe's new
  `netem` requirement needs `tc` and `ip` with `CAP_NET_ADMIN`; the fuzzer never asks for `sudo`, and a kernel
  that lacks a module (`ifb`, `sch_netem`, `cls_flower`, `act_mirred`) is named in the error. The partition and the degradation share one heal
  implementation (`_HealingFault`); the partition's behaviour and record are unchanged.
- `valkey.action.replication-chaos` gains the `degrade` wave operation (`target`, `duration_s`, and the same
  effects), undone with the partitions; `valkey.generator.waves` draws it with per-instance loopback addresses, lossy
  or slow, never both. `valkey.oracle.error-counters` counts a `network.degrade` change as a link cause.
- Experiments `valkey.replication-degraded-link` (loss, delay, reordering on the replication links under writes; no
  full resync, links up again, replicas converge, ledger and linearizable history, no loss window) and
  `valkey.cluster-gossip-delay` (one primary's links delayed below `cluster-node-timeout`; no node marked failing,
  no FAIL message, no election won, `cluster_state ok` and agreeing views after the heal); examples
  `replication_degraded_link.yaml` and `cluster_gossip_delay.yaml`.
- `valkey-fuzzer sweep` removes a dead owner's netem device (kind `netem`) with the `lo` filters that redirect to
  it, and the ingress qdisc when the fuzzer added it and nothing is left on it. `tests/test_live_network_degrade.py`
  checks the rules against the real kernel where `tc` and `CAP_NET_ADMIN` are available (skipped otherwise); the
  daily live workflow runs the two experiments as root.

### Changed: the spec bridge's TLC layer: bounds under a budget, a checked jar, JSON traces, the election quorum (PR-P2-c)

- `formal/cluster-election/model.cfg` checks two crashes (`MaxCrashes = 2`; 330,207 distinct states, 15 s on four
  TLC workers, from 220,138 and 8.5 s): a bug that needs one primary to crash twice, or two to crash, is where the
  bound matters. A third epoch (`MaxEpoch = 3`) is 100 times the states (33.7 million with two crashes, 17 minutes)
  and lives in `nightly/model.cfg`; `formal/replication/nightly/` holds `MaxCrashes = 2` for both of its safe
  configs (10.2 million states under dual channel, 3 minutes here and past five on CI's runner). `spec check
  --configs nightly` checks a bundle's `nightly/*.cfg` instead of its own, which may raise an integer bound of
  `claims.yaml` and nothing else; the new `tla-model-check-nightly.yml` workflow runs it daily on every bundle
  that has the directory. The pull-request budget is five minutes per TLC run and fifteen for the job. The
  election's negative controls keep one crash: their counterexamples need one, and a second doubles the search
  for the same trace.
- The jar is probed before anything runs (`java -cp tla2tools.jar tlc2.TLC -h`): a jar that does not run TLC, or
  runs one older than 2.19, is an error naming the jar; its SHA-256 is looked up among the releases the bridge
  knows (`TLA_TOOLS_RELEASES`: v1.7.4, the one CI pins, and v1.8.0), and another jar runs with a warning, or not
  at all with the new `--strict`, which the model-check workflows pass. `spec check` prints the tools it ran with
  and records `release`, `tlc_version`, `json_trace` and `warnings` in the report's `tools`.
- A TLC that can (`-dumpTrace json`, v1.8.0 and later) writes the counterexample as JSON, and the bridge reads the
  trace from that file (`read_json_trace`), with the config's model values given back their type; otherwise, and
  when the dump cannot be read (an error the result shows), from the text as before. The text reader now takes the
  action's name alone from a 2026 TLC's `<Win(r1) line ...>` labels. `spec translate --trace` takes the JSON file
  too. Both readers are covered by recorded fixtures of one TLC 2026 run, and translate gives the same scenarios
  from either.
- `formal/cluster-election/model.tla` has a `Quorum` constant: `Win` needs `Quorum` votes (`HasQuorum`), Valkey's
  `needed_quorum`, instead of a majority of `Primaries`. The mapping reads it from the `quorum` the candidate's
  ACK lines log (a new `field` constant form: `{field: {evidence: acked, name: quorum}}`, the largest value any
  such record carries, `at_least` without one), since the mapping's `Primaries` holds every node that ever voted
  and a majority of them overshoots after a promotion. The two-election smoke traces of PR #205 now hold (they
  violated at the second `Win`); the known-bad runs and the negative controls still fail. `claims.yaml` bounds
  gain `Quorum: 2`.

### Changed: keyspace snapshots read each SCAN batch in two round trips, and the workload draws a value in one call (PR-R3-3)

- `keyspace_snapshot` (`valkey_fuzzer.components.keyspace`), which `valkey.oracle.dataset-digest` reads from every
  primary and replica and `valkey.oracle.rdb-snapshot` from every reloaded RDB, pipelines each SCAN batch of up to
  1000 keys: one pipeline of `TYPE`s, then one pipeline of the per-type reads (`GET`, `HGETALL`, `LRANGE`, `SMEMBERS`,
  `ZRANGE WITHSCORES`, `XRANGE`, `DUMP`), instead of two round trips per key. The snapshot is byte-identical (same
  canonical values, same SCAN order), so ledgers and digests do not change; a client that does not pipeline (a
  test's) is still read key by key with `canonical_value`. 100,000 keys over TCP: 10.0 s -> 1.7 s; 10,000 keys:
  0.98 s -> 0.15 s.
- `make_value` (`valkey.workload.key-value`) draws a value with one `rng.choices` call instead of one `rng.choice`
  per byte: 64 B 16.5 µs -> 6.6 µs, 4 KB 1.02 ms -> 0.33 ms, 16 KB 4.08 ms -> 1.29 ms. The random bodies a seed
  draws change (every id, operation, key, outcome, tag and length is as before); nothing compares them to fixed
  values, only to the run's own ledger.

### Changed: readiness and formation polls at 50-100 ms, role sweeps that ask every instance at once (PR-R3-2)

- A spawned or restarted server is polled every 0.05 s instead of 0.5 s, and cluster formation (meet, slot
  assignment, replica links, the health check) every 0.1 s instead of 1-2 s; the deadlines are unchanged. The first
  check always ran before the server bound, so the interval set the cost: a standalone provision takes 0.10 s instead
  of 0.50 s, a restart 0.17 s instead of 0.62 s, and a 3x2 cluster forms in 7.1 s instead of 10.2 s (the rest is
  the replicas' sync), measured against valkey-server 9.1.3 on one host.
- `roles()`, `primaries()` and `replicas()` query the instances concurrently (`ThreadPoolExecutor`, at most 32 at
  once), with the same answers in the same order. Under a held `stall` fault a stopped instance accepts the connect
  and its INFO times out after 2 s; the sweep now costs one such timeout, not one per stalled instance: 2.0 s instead
  of 6.0 s with three of nine nodes stopped.
- `select_instances` reads the roles once for a selector that names both roles (`{roles: [primary, replica]}`),
  instead of one `primaries()` and one `replicas()` sweep: 9 INFO round trips instead of 18 on nine nodes, 2.0 s
  instead of 12.0 s with three nodes stopped. A deployment where no instance answers still falls back to the planned
  roles.

### Added: `spec translate` searches the model for counterexamples (PR-P2-a)

- `spec translate` took one counterexample, from a negative control or a saved TLC output, so a bundle gave one
  scenario family per control. It now searches the model as well: `--reach TARGET` (repeatable, `all` for every
  target of `claims.yaml`) runs TLC on each safe config with the invariant `~TARGET`, so the counterexample is a
  shortest path that reaches the target, and translates it; `--mutate` runs each safe config with each boolean
  constant flipped, and with each pair flipped when the single flips were cheap, and translates every violation.
  A violation that takes none of the mapped faults (the experiment has nothing to inject) is set aside and the
  mutation runs again without that property, until a violation with a fault, or none, is found. Traces with the
  same action sequence are translated once. `--tlc-timeout` (default 300 s, as a batch's `conform_tlc_timeout_s`)
  bounds each TLC run of the search, within `--timeout`; a run that runs out is recorded and the search goes on.
  `--max-scenarios` (default 50) caps the scenarios written. `translation.json` is then a `spec-search` report:
  each target's and mutation's outcome (`reached`, `unreachable`, `violated`, `held`, `no-fault`, `duplicate`,
  `untranslatable`, `timed_out`, `skipped`, `not-run`), the summary lists `unreachable` and `held`, the
  translations with their `source`, and the scenario ids. `--control` and `--trace` work as before, with the same
  output. On the shipped bundles `--reach all --mutate` writes 15 election scenarios in 12 s (the two deep
  controls that give the same scenarios take 31 and 34 s each), two replication scenarios in 19 s, and three
  slot-migration scenarios in 15 s (`docs/spec-bridge.md`, "Searching the model").

### Added: a run checks itself against its spec bundles, and the result is in its report (PR-P2-b)

- A run whose plan names a spec bundle (a component's `spec`) ran `spec conform` only in a batch, whose worker
  wrote `conform.json` beside the run and whose verdict carried the finding; the run's own `report.json` never
  mentioned it, so `minimize` could not shrink a conform-only failure, `replay` did not show it, and the bug zoo
  could not pin one. Every run now checks itself as its report is written (`run`, `replay`, each `minimize`
  attempt, a batch's worker), after its trace is complete, with TLC bounded by `conform_tlc_timeout_s` (the batch
  option; a run outside a batch uses its default, 300 s), and `report.json` gains a `conform` section, by bundle:
  `approved`, `status`, `violated` (the claimed properties violated, and `transitions` for a step the model
  cannot take), `first_violated` (`claim`, the observed `state`, the `step`), `tlc_seconds`, and the full check.
  `null` for a run that names no bundle; every old key stays.
- For an approved bundle, the check is part of the run's verdict: a claimed violation is the failed component
  `spec-conform:<bundle>` of the outcome (its summary the violated claims), so the run `FAIL`s, its summary,
  `failed_components` and failure signature name it, `minimize` keeps a reduction only while the same claims fail,
  `replay` and `run` print a `Spec conform` line, and a zoo case names the check in `fail`. A check that could not
  decide (TLC missing for a bundle that needs it, with the existing message) makes the run an `ERROR` through the
  same component. A draft's violation is recorded, not held against the run. The report is written before the
  check and again after it, so a run killed during TLC leaves a report whose `conform` is `null`.
- The batch's conform plug-in reuses the report's section instead of running TLC twice, takes the run's verdict
  as it is, and keeps its own path (`conform.json` after the run, judged by the batch) for a run that predates
  the section. `spec conform` results gain `violated`, `first_violated`, and `tlc_seconds` per run everywhere.
- The shipped bundles are drafts, so no shipped zoo case can FAIL by conform yet; `tests/test_zoo.py` pins a case
  on the test register bundle with a stub trace whose crash loses an acknowledged write.


### Changed: the catalog builds five times faster, a run hashes its code once, and CI caches what it downloads (PR-R3-1)

- The document loader (`valkey_fuzzer.schema`) runs on libyaml's scanner (`yaml.CSafeLoader`) when PyYAML has it,
  with the same YAML 1.2 scalar rules, duplicate-key refusal, and `!!binary` reading; it falls back to the pure-Python
  scanner otherwise. `build_catalog()` takes 31 ms instead of 158 ms, for every command and every batch worker. A
  test reads every shipped YAML document and the catalog with both loaders and holds them to the same output.
- `provenance.source_digest` reuses each package's hashed files while none of them changes size or modification
  time, so the plan's reproducer, the report's, and the catalog digest read the package once per process: 11 ms per
  run instead of 58 ms, and 0.3 s instead of 1.7 s over a 30-attempt `minimize`. The digest itself is unchanged.
- An `--exclude` value is parsed as YAML once, when the exclusion is, not at every value the generator draws
  (0.5 µs instead of 32 µs per comparison).
- The tests share one catalog per process (`tests/conftest.py` `catalog`, `tests/valkey_experiments_support.py`
  `shared_catalog`) instead of building 382 of them: the unit suite runs in 146 s instead of 212 s here.
- CI caches pip's downloads (keyed on `pyproject.toml`) and Go's modules and build (keyed on `porcupine/go.sum`) in
  every workflow, and `test.yml`'s server tests restore the Valkey build at `zoo/valkey-commit` (about 90 s to
  make) instead of building it on every run.

### Added: clock-skew and resource-pressure faults, each host-safe (PR-C2)

- `valkey.fault.clock-skew` shifts one instance's view of the wall clock without touching the host clock. A
  deployment with `clock_faults: true` starts every server with [libfaketime](https://github.com/wolfcw/libfaketime)
  in `LD_PRELOAD`, each reading a timestamp file of its own on every call (`FAKETIME_NO_CACHE`); the fault rewrites
  the target's file to `+<offset> x<rate>` and restores the real time on `release_event` or at recovery.
  `DONT_FAKE_MONOTONIC` keeps the event loop on the host's monotonic clock, so only the wall clock (key expiry,
  `TIME`, wall-clock timeouts) moves. The library is the host's `libfaketime` package or `VALKEY_FUZZER_LIBFAKETIME`;
  without it the fault's probe is an `ERROR` naming the package. The fault reads `TIME` and compares it with the
  host's clock: a skew the server does not show (libfaketime not loaded) is not applied. Used by
  `valkey.clock-skew-expiry`: the primary's clock is set back so a volatile key's absolute expiry is in the
  replicas' past, and every replica reads it as expired (TTL -2) but deletes it only on the primary's `DEL`.
- `valkey.fault.resource-pressure` puts cgroup v2 `cpu.max`/`memory.max` limits on one instance for a window, then
  releases them. It moves the target's server into a cgroup leaf of the run's own, needs a delegated cgroup v2 of
  the run's own (cgroup v2 mounted, this process's cgroup not the root one, writable, with the controllers and only
  this user's processes), and refuses anything else with the reason (`ERROR`). Nothing is written outside that
  cgroup, no other process is moved, and `valkey-fuzzer sweep` removes a leaf a dead fuzzer left. A limit that
  never bit (no throttled period, no `memory.events` `max`) is not applied. No fallback without cgroups: a fully
  stopped instance is already `valkey.fault.process-signal` with `SIGSTOP`. Used by
  `valkey.replication-cpu-starvation`: the primary's CPU is throttled under writes for less than `repl-timeout`,
  and the replica links must survive it with no resync and every write converge.
- New deployment option `clock_faults`; the cgroup subtree lives in `valkey_fuzzer/cgroups.py`; the host probes
  `libfaketime_supported` and `cgroup_v2_supported` back the two faults' detection. `docs/components.md`,
  `docs/scenarios-and-experiments.md`, and `examples/valkey/` cover both. The live tests
  (`tests/test_live_valkey_experiments.py`) are guarded by capability detection, so CI's server-tests job skips an
  unmet one as `SKIP_UNSUPPORTED`/`ERROR` rather than failing.

### Added: `spec conform` checks the run's steps against the model (PR-P1-a)

- `spec conform` checked only that every observed state satisfies the claimed properties; a run that took a step
  the model forbids but ended in a safe state passed. The same TLC run now also checks the transitions: from the
  first observed state, with the hidden variables at the mapping's `hidden` value, each record that shows a model
  action must be that action (any of its parameters, from the model's own `Next`) to a state that agrees with what
  the record says (its `set`/`incr`/`clear` updates exactly, its `add`/`remove` as membership, its `step` rule as
  `changes`/`unchanged`, and as many steps as it stands for with `step: {repeats: true}`), or no step when earlier
  steps already account for it; a record without an action (a
  sample) raises what it reports instead. Between two records the model may take up to `max_unobserved_steps`
  (new in `claims.yaml`, default 1) steps of the actions the mapping lists as `unobservable`, and no other. A pass
  is observed-projection refinement: the run's observed steps are steps of the model (`docs/spec-bridge.md`, "The
  transition check", says what stays unchecked).
- `conform.json` gains `transitions: {checked, max_unobserved_steps, holds, tlc_states}` per run, with
  `first_violation: {index, from, to, event}` when some record has no model step; the run then `fail`s under the
  same rule as a violated property (an approved bundle's run `FAIL`s in a batch, a draft's is reported). The
  bundle's report counts `transitions.violated_in`; a batch's `conformance` digest lists `transition_violations`.
  `--no-transitions` gives the observed-state check alone. Without TLC the check is reported as `skipped` for a
  draft and is an error for an approved bundle; a bundle whose mapping cannot support it says so with
  `transitions: {excluded: <why>}` in `claims.yaml`, as `formal/replication` does (its samples carry IDs as hex
  and data sets as digests, which the model's steps cannot compute).
- `formal/cluster-election/mapping.yaml`: a `granted` record now follows (`after`) the `started` record of the
  election it answers, as an ACK follows its grant: two nodes' lines can be read in either order, and a vote the
  model grants needs the request. A grant whose election start was never logged is a record conform cannot apply.
  The transition check holds on the shipped experiments' runs with one election; with two, the second `Win` can
  violate because the model's `IsMajority` counts `Primaries`, which the mapping fills with every node that ever
  voted, while Valkey's quorum counts the slot-owning primaries at the time (the ACK line's `quorum`). The fix is
  a quorum constant in the model, left for a follow-up.
- `conform.json`'s `states` and the invariant results are unchanged; `transitions.checked` counts the records
  that are steps or change the state, so a denied vote counts as a step there and not as a state.


### Added: the trace carries data samples, and `spec conform` checks a replication data claim on them

- `valkey.observer.trace` (was `valkey.observer.cluster-trace`, cluster only; the evidence is
  `valkey.trace.recorded`) runs on any deployment and samples from every node what the spec bundle's
  `mapping.yaml` asks for in its new `sample:` section: INFO sections (`info`), `DEBUG DIGEST` (`digest`) and
  `DEBUG DIGEST-VALUE` of named keys (`digest_keys`), the cluster view (`cluster`), `CLUSTER GETSLOTMIGRATIONS`
  (`slot_migrations`, the jobs by kind), and the `vars` line of `nodes.conf` read from the file (`nodes_conf`), after
  the events the section's `on` globs name and on `interval_s`. A node's INFO, digests and slot migrations are read
  in one `MULTI`/`EXEC`. The header records what was sampled (`samples`). A sample that fails at the start is an
  `ERROR`; a bundle that samples `DEBUG DIGEST` needs `enable-debug-command yes`, checked when the scenario
  compiles. Without a bundle, a cluster deployment is sampled as before (the view and `nodes.conf`).
- A mapping field can carry a `map` that renames captured values (`{master: primary}`); a sample evidence reads
  only what the `sample:` section records, checked when the bundle loads.
- `spec conform` reads a sample as an observed state, not a step. A sample that sets a variable to another value
  than the events gave it is reported in the run's `sample_mismatches` (variable, key, inferred and sampled value),
  not overwritten in silence.
- `formal/replication`: `SameOffsetSameData`, `OneHistoryPerId` at equal offsets (two nodes that could continue one
  replication ID at one offset hold one data set), with the control `write_without_offset`; `conform: true`, checked
  on the trace's `INFO replication` and `DEBUG DIGEST` samples, which now observe `role`, `off`, `replid`, `replid2`,
  `secondOff`, `backlog` and `data`. `DualChannel` comes from the trace header's
  `dual-channel-replication-enabled`, so a dual-channel run is outside the claim instead of pinned inside it. The
  two claims over data sets as sequences stay model only, with the reason. The `valkey.replication-steps` fragment
  adds the `replication-trace` observer (once a second: `DEBUG DIGEST` costs a node about 3 µs a key) and enables
  `DEBUG`; its three experiments list it.
- `formal/cluster-election` declares `sample: {cluster, nodes_conf}`; `formal/slot-migration` declares
  `sample: {cluster, slot_migrations}` and observes `exp`, `imp` and `track` from the jobs' states.
- `docs/spec-bridge.md` has a [Samples](docs/spec-bridge.md#samples) section: semantics, cost, and which variables
  each bundle observes from samples.
### Fixed: the minimizer's verdicts, oracle edge cases, write retries, provenance pins, and small engine checks (PR-H2)

- `minimize` runs the recorded scenario before any reduction and exits `2` when it does not fail the same way
  (nothing is minimized), runs its result once more and exits `2` when that run does not fail the same way, and
  no longer re-runs a reduction it already rejected. A crash signature matches only with the same outcome
  status: an `ERROR` run that reports the `FAIL`'s crash is another failure. Numeric inputs shrink only when the
  experiment marks them `shrinkable: true` (a new input rule, for a count, a rate, or a size); a shard index, a
  delay, or a loss window is left alone. The shipped sizes (`stable_keys`, `mutable_keys`, `mutation_rate`,
  `value_size`, `operations`, `keys_per_slot`, `clients`, `config_changes`, `command_rate`) are marked.
- `valkey.oracle.dataset-digest` no longer fails a key held by two primaries while `CLUSTER NODES` shows its
  slot migrating or importing; such keys are listed as `migrating_keys` in its evidence.
- `valkey.oracle.cluster-election-safety` reports `ERROR`, not `FAIL`, when the cluster views were still changing
  at the `converge_timeout_s` deadline; `FAIL` stays for views that hold still and disagree. Its minimum Valkey
  version, 9.2 (the first to log `Failover auth ACK`), is documented and named in the `ERROR` for a run without
  ACK lines.
- `valkey.oracle.rdb-snapshot` with `workload` (the cut check, which allows no lost write) does not compile next
  to a component that can lose acknowledged writes: a process kill, a syscall fault that kills, a forced
  failover, or a chaos or rolling-upgrade action. It was a false `FAIL` at the end of such a run.
- `valkey.action.commands` and `valkey.action.tools` mark an `expect` both builds violate alike with
  `pre_existing: true` in the `expect-disputed` diagnostics.
- `valkey.action.command` retries a send only while its connection cannot be opened, decided by which step
  failed (the connection is opened first, sending nothing), not by the client's error text.
- `valkey.artifact.local-binary` requires `expected_sha` to be 7 to 64 hex characters and a prefix of the git SHA
  or SHA-256; a digest that was a prefix of the expectation no longer matches. The source build checks that the
  `9.1.3` tag resolves to the pinned commit before building, and records the commit in `build.log`. A report's
  `fuzzer_digest` covers the package of every component the plan uses, so a plug-in pack's code drifts like the
  fuzzer's own.
- A batch worker's deadline adds the plug-ins' after-run budgets (spec conform's `conform_tlc_timeout_s`), so a
  run that used most of its `timeout_s` is not killed while it conforms and recorded `ERROR`.
- Reports and the coverage key refuse a value JSON cannot write (a set, a path) instead of writing its `str()`;
  a failed undo is logged when it happens; a command handler that returns no exit code is a harness error (exit
  `2`), not a success; a scenario's `seed` is `0` to `2**64-1`; a duplicate experiment ID across
  `--experiment-dir` directories names both files.

### Added: every spec bundle translates into scenarios and has shipped experiments that publish its steps

- `formal/replication`, `formal/persistence` and `formal/slot-migration` have a `scenario` section, so
  `spec translate --control <name>` writes scenarios for each of the four bundles, and `spec check`'s `experiment`
  gate compiles them. A section's new `unmapped` field lists the model's fault actions the scenario does not inject,
  each with the reason (no fault component does it, or no counterexample takes it), and a section may have no
  `faults`: the scenario then sets the deployment and the inputs, and the experiment runs its own schedule. The
  bundle loader checks `unmapped` names model actions that are not also faults, and `translation.json` records it.
- `valkey.slot-migration-spec`: a new experiment that migrates the slot of a hash tag from shard 0 to shard 1 under
  writes to it, kills the target the moment it logs its claim (the slot migration model's `CrashT`), restarts it once
  its replica is elected, and checks the data (ledger, with a loss window for writes the target had not replicated)
  and the cluster. It publishes the model's steps (`spec: slot-migration`) and has a generator block, so `batch
  --random all` and the PR batch run it and check its runs against the bundle. `valkey.action.migrate-slots` is the
  action behind it: `CLUSTER MIGRATESLOTS` of one slot to a shard's primary, waited for until the source's export
  ends.
- `valkey.persistence-steps`: a fragment whose observer publishes the persistence model's steps (`spec:
  persistence`); `valkey.aof-durability` and `valkey.save-crash-points` extend it, so a batch checks their runs
  against `formal/persistence`. `valkey.save-crash-points` with `BGREWRITEAOF` is the experiment the persistence
  scenarios run.
- The checked-in counterexamples `tests/fixtures/spec/{persistence-manifest_before_base,
  replication-keep_old_id_on_load, slot-migration-replica_misses_claim}.out` test translate per bundle without TLC.

### Changed: the election oracle and `spec conform` agree, and the spec bridge's blind spots from the audit are closed

- `valkey.oracle.cluster-election-safety` claims `VoteOncePerEpoch` and `OneWinnerPerEpoch` only where
  `formal/cluster-election/claims.yaml` does (`holds_when`): it derives the same constants `spec conform` derives
  from a run (`PersistBeforeAck` from every node's `cluster-config-save-behavior`, `FlushBeforeSave` pinned), so a
  double vote or two winners under `best-effort` is recorded under `outside_claim` (and in the PASS summary), not
  a FAIL, as conform reports it. Under `sync` both still FAIL. A finding whose claim depends on a setting no node
  could answer is an `ERROR`. The quorum, saved-vote, and split-primary checks are not conditioned.
- The election's `won` evidence is the line Valkey logs for every win, `Failover election won: I'm the new
  primary.`; `configEpoch set to N after successful failover` is logged only when the winner's `configEpoch` was
  below `N`, so a win could have no event, and the experiments' required `election-won-logged` evidence could be
  missing on a real win. The line names no epoch: `mapping.yaml`'s `after` may now `take: [field, ...]` from the
  latest record it follows, so the win is at the epoch of the winner's latest `started`; the oracle resolves the
  epoch the same way, and a win with no earlier `started` of its winner is an `ERROR` (conform: a record it cannot
  apply), never a win at a made-up epoch.
- `valkey.observer.log-trigger` rules take `logged_by: server` (default `any`): the lines of a fork child (an RDB
  or AOF child, role `C`) are skipped. A rule without a pattern takes it from the mapping's evidence, which may say
  `logged_by: server`; replication's `saved` does, so one BGSAVE (the child's `DB saved on disk` and the server's
  `Background saving terminated with success`) is one `Save`, not two.
- The TLC limit of a batch worker's `spec conform` is the batch option `conform_tlc_timeout_s` (default 300, the
  value that was hard-coded); a check it stops is a conform `error` whose reason names the option.
  `BatchPlugin.after_run` takes the worker's task as its second argument.
- A conform entry that does not say whether its claims are `approved` is read as a draft's: only an explicit
  `approved: true` decides a run. A worker's `spec conform` that fails outright still writes `conform.json` with
  the exception as each bundle's error, and conform reports a trace the run recorded that is missing or unreadable
  as that run's error instead of raising, so a missing `conform.json` on an approved bundle no longer hides why.
- `spec check` has a `log_patterns` gate: every `log` pattern of `mapping.yaml` must match a line of the bundle's
  new `sample.log` (lines Valkey logged, from the format strings in its source). The oracle, the log-trigger
  rules, and conform all read their patterns from the mapping, so one wrong pattern would blind them all. Every
  shipped bundle and the register fixture have one.

### Fixed: process cleanup, readiness waits, the ledger's clock, and seeded ports (PR-H1)

- A fault's `SIGKILL` (`valkey.fault.process-signal`, the chaos actions' kills) now takes the server's forked
  children (a BGSAVE, an AOF rewrite) with it, as cleanup always did: one left behind kept writing into the data
  directory while the instance restarted. A `valkey-benchmark` the benchmark workload starts carries the owner
  tag in its `argv[0]`, so `valkey-fuzzer sweep` finds one a killed harness left looping. An interrupt (Ctrl-C)
  while a server is stopped at cleanup kills it with its children instead of leaving it on `SIGTERM` alone.
- Cluster formation polls for readiness instead of pausing: no fixed 0.1 s per `CLUSTER MEET` (the convergence
  poll follows), and `CLUSTER RESET HARD` waits until every node knows only itself and holds no slot (10 s at
  most) rather than 1 s. A restarted cluster node is ready only once a running peer agrees on its role and,
  for a replica, its link is up: an old primary that still read itself as primary from `nodes.conf` was
  "ready" before.
- A fault interrupted while firing records the interrupt; its recovery no longer waits up to 600 s and reports
  `fault could not be applied: None`. Recovery's wait for a firing ends at its own deadline.
- Operation times, loss windows, fault times, and evidence `observed_at` are read from one run clock
  (`valkey_fuzzer.clock.now`): the monotonic clock anchored to wall time at process start, so a wall-clock step
  mid-run reorders no ledger and moves no window. Values still read as epoch seconds.
- A private server's start wait (capability probes, RDB loads, command sequences) ends with the run's deadline
  or a cancelled call, instead of running out its own timeout (15 s or more).
- A private server's port outside a worker's slice is drawn from the run's `ports` stream, so a replay draws
  the same ports.
- The batch runner skips the `SIGKILL` of a finished worker's process group when a new process already leads a
  group of that id (a reused PID).

### Changed: the spec bridge loads only when used, and says how to get TLC when it is missing

- `run`, `validate`, `batch` and the other commands no longer import the spec bridge's tools
  (`valkey_fuzzer.spec.check`, `.translate`, `.conform`). The components still read a bundle's mapping
  (`valkey_fuzzer.spec.bundle`, which parses TLA+ values with `.tla`), which needs no Java. A base `pip install`
  needs neither Java nor `tla2tools.jar`; there is no `[spec]` extra, since the bridge has no Python dependency.
- Without java or the jar, a `spec` command prints one message naming what is missing, Java 11 or later, how to
  pass the jar, and the `curl` command for the release CI pins, and exits `2`. The message replaces
  `tla2tools.jar not found: pass --tla2tools or set TLA2TOOLS_JAR` and `java not found on PATH`.
- In a batch without TLC, a run checked against an approved bundle that needs TLC is now an `ERROR` (it was
  `skipped`), with the same message as the reason. A draft bundle stays `skipped`. No shipped bundle is approved
  now, so no shipped batch changes.
- `CONFORMANCE_FILE` moved from `valkey_fuzzer.spec.conform` to `valkey_fuzzer.batch.conform_plugin`, and `TRACE_FILE` from
  `valkey_fuzzer.spec.conform` to `valkey_fuzzer.spec.bundle` (`conform` still has it).

### Changed: `batch.py` is the `valkey_fuzzer.batch` package; the zoo and spec conformance are its plug-ins

- The 1,467-line module is split by concern: `spec.py` (options, sources), `sources.py` (random, matrix, scenarios,
  failures), `jobs.py`, `worker.py`, `results.py`, `pool.py` (`BatchRunner`), `summary.py`. The names callers
  import (`BatchOptions`, `BatchRunner`, `expand`, `load_spec`, `normalize_source`, `job_result`, `signature_key`,
  `signature_label`, `run_worker`, `main`) stay importable from `valkey_fuzzer.batch`, and the worker command is
  still `python -m valkey_fuzzer.batch TASK`.
- `batch/hooks.py` is one plug-in interface: a `BatchSource` (a kind of `sources` entry) and a `BatchPlugin` with
  the steps around a run (`after_run` in the worker, `judge` and `record` on a finished job, `summarize`). The bug
  zoo (`batch/zoo_plugin.py`) and spec conformance (`batch/conform_plugin.py`) are registered through it; the batch
  loop no longer names either. `Expansion.zoo` is `Expansion.recorded["zoo"]`.
- No change to `batch.json`, `jobs.json`, `results/*.json`, `conformance/*.json`, `summary.json`, `batch.log`, the
  console output, or the exit codes for the same seeds. The one visible difference is the order of the source kinds
  in the error for an unknown source (`zoo` is now listed last).

### Changed: packs register through the `valkey_fuzzer.packs` entry point

- `build_catalog` no longer imports the Valkey components by name: it calls the `register(catalog)` of every entry
  point of the `valkey_fuzzer.packs` group, in name order. The Valkey components are the `valkey` entry point
  (`valkey_fuzzer.components:register`), so `run`, `batch`, `validate`, and `list` see the same catalog as before.
  A package of your own can ship components, providers, and experiments the same way, with no edit to the fuzzer
  (`docs/extending.md`, "Ship a pack"). `register_valkey_components` is removed; call
  `valkey_fuzzer.components.register(catalog)`.
- An existing editable install must be reinstalled (`pip install -e .`) to record the entry point; without it no
  Valkey component is registered.
- A scenario that uses a component whose namespace no installed pack registers now fails with
  `no installed pack registers <namespace>.* components; install the package whose 'valkey_fuzzer.packs' entry
  point is named '<namespace>'` instead of a "did you mean" naming an unrelated Valkey component.

### Changed: the replication and slot-migration claims say what their mapping pins

- `formal/replication/claims.yaml` claimed its properties under `AdoptOnLoad = TRUE`, a fix Valkey does not have,
  while `mapping.yaml` pinned `AdoptOnLoad` to `FALSE` for every run. The claims now hold for Valkey as it is over one
  channel (`holds_when: {DualChannel: false, AdoptOnLoad: false}`, what `model.cfg` checks and what the mapping
  pins); over two channels the properties hold only with the fix (`dual-channel.cfg`). The bundle goes back to
  `status: draft`: no property can be checked on a run (every variable both read is hidden in the mapping, so
  conform could never fail them), and the approval enforced nothing. No shipped bundle is approved now.
- `formal/slot-migration/mapping.yaml` pinned `CancelKeepsPause`, `ExpiryAfterClaim`, and `ClaimReachesReplica` to
  Valkey as it is, the reverse of every property's `holds_when`, so no run could ever be checked as claimed. It now
  pins the claims' values, as `cluster-election` pins `FlushBeforeSave`: a run in which Valkey as it is lets two
  nodes write, or loses an acknowledged write, violates the claim once a property is conformed.
- A batch's conform entry for a run whose worker wrote no `conform.json` (or whose bundle does not load) now says
  whether the bundle's claims are `approved`, as the worker's entries do, and the verdict reads it instead of
  defaulting to approved: a draft bundle's missing check is reported, an approved bundle's is an `ERROR` for the run,
  and a bundle that does not load is held against the run.

### Removed: unreferenced example scenarios

- `examples/valkey/replication_full_resync.yaml` and `replication_partial_resync.yaml` only pinned a seed, ports and
  the defaults of the shipped experiments of the same id; run them with `--experiment valkey.replication-full-resync`
  or `--experiment valkey.replication-partial-resync`. `examples/cluster_scenario.yaml` and
  `examples/standalone_replicas_scenario.yaml` had no reference anywhere.

### Removed: four components no shipped experiment, example, zoo entry, or workflow used

- `valkey.action.replicaof`, `valkey.action.restart-instance`, `valkey.fault.command`, and `valkey.fault.deny-writes`
  are removed, with their tests and `docs/components.md` entries. An experiment that names one is now rejected as an
  unknown component. To send a command at a lifecycle event, use `valkey.action.command` after the event; to restart
  an instance, use `valkey.fault.process-signal` (`restart_mode`) or `valkey.action.rolling-upgrade`.

### Removed: the one-off search-abort-probes workflow

- `.github/workflows/search-abort-probes.yml` and `.github/scripts/search_abort_probes.py` are removed. Nothing
  imported the script.

### Removed: coverage-guided batches (`--guided`, `--corpus-from`)

- `batch --guided N`, `batch --corpus-from BATCH`, and the batch spec keys `guided` (in a `random` source) and
  `corpus_from` are gone, with `corpus.py`, the gcov feature collection (`coverage.gcov_features`), the
  `code_coverage` summary key and result field, and `corpus.json`. A batch spec that names `guided` or
  `corpus_from` is now rejected as an unknown key. No workflow used them, and they needed a gcov build of Valkey.
- `reproducer.generated` in a run report no longer carries `draws`, and `Overrides.pinned` and
  `GeneratorBlock.kept` are removed: only guided batches read them.

### Removed: the `diff` command and the differential engine

- `valkey-fuzzer diff`, differential documents (`differential_id`, `variants`, `compare`), `RunService.differential`
  and `valkey_fuzzer/differential.py` are removed, with `examples/valkey/differential_baseline_vs_candidate.yaml`
  and the `<root>/differential/` report folder. There is no deprecation shim: `diff` is now an unknown command.
  To compare two builds, use `compare_with` of `valkey.action.commands` (or of `valkey.action.tools`), which runs
  one sequence on a candidate and a parent server and compares the replies. Batch `matrix` dimensions are unchanged.

### Changed: `replay` and `minimize` warn about drift and never refuse

- A drifted `replay` or `minimize` runs, prints each difference to stderr, and `replay` records it in the
  report's `provenance.drift` (`item`, `recorded`, `replay`). `--allow-drift` is removed from both commands,
  `provenance.drift_allowed` is gone, and a bare `plan.json` replays unverified (`provenance.verified: false`).
  A drifted replay exits with the run's own verdict code; it no longer exits `2` for drift or `64` for a bare plan.

### Changed: readable run output and `report.json` layout (keys preserved)

- `report.json` now starts with `status`, `summary`, `failed_components` (values cut to 120 characters with
  their length and SHA-256), `reproduce`, and `look_at`; the data tools read follows. New keys only: no key
  was removed or renamed, and `outcome` is unchanged. `reproducer` gains `command` and `valkey_binary`.
- Every `run` prints `Reproduce: valkey-fuzzer replay <report>` (a generated scenario also prints it as an
  alternative), and a FAIL or ERROR prints a `Look at:` line.
- The local deployment's planning lines are logged at DEBUG, and a source build's output goes to
  `<build dir>/build.log` after a one-line notice.

### Changed: a missing `porcupine` is reported at `validate` and at run start

- A scenario with `valkey.oracle.history` now fails `validate` (exit `2`) and a `run` fails as `ERROR` before
  anything is acquired or started, when `porcupine` is not on `PATH`. The message gives the build command
  (`go -C porcupine build -o <a directory on PATH>`) and says Go >= 1.21 is needed when `go` is missing too.
  It was an `ERROR` of the oracle after the whole run. Components can register a `probe` for this
  (`ComponentRegistration.probe`); it runs at validate and run start, never at compile.
- `pytest` lists every skip and its reason (`-rs`). `test_every_excluded_config_is_one_the_build_under_test_has`
  runs only on a Valkey tree at the commit in `zoo/valkey-commit`, so another release on `PATH` no longer fails it.

### Changed: every run has a deadline, every wait a watchdog

- A run without `--timeout` gets a derived deadline: the search profile's `budget.timeout_s`, else 1200 s,
  and for `run` never below the sum of the components' `timeout_s`. `run` prints it with its source.
- The deployment provider's `cleanup` and `rollback`, each obligation's `undo`, and the teardown of a
  component without `timeout_s` are cut after 60 s. The run is `ERROR` and names the wait that expired.

### Changed: first-run docs, spec-bridge claims

- README and getting-started state Go 1.21 and the `porcupine` build, the real Valkey build behavior (only four
  experiments probe and report `ERROR`; the rest build Valkey on first use), and the interim clone branch. New page `docs/reading-a-fail.md`.
- `spec check` reports the `approved_claim` gate of a draft as `skip` (`[SKIP]`), not `pass`. Docs and bundle READMEs
  say that only `cluster-election` is checked against live runs; `replication`, `persistence`, and `slot-migration`
  are model only.

### Changed: a server that dies at startup

- A server that exits before its first `PING` is `ERROR` with `exited with code N after Ts` and the last five
  output lines (it said `failed to start within 60s` after half a second). If its output holds a crash
  signature the run is `FAIL`, and the summary names the crash. See `docs/contracts.md`.

### Added: first-run gaps (read by `valkey-ci-agent`)

- `--base-port` on `run` and `diff` (and as an alias of `batch --port-base`); `diff --artifact NAME=PATH`.
- Errors that name a field, component, or experiment now offer the closest names and list the valid fields;
  `replay` of a scenario file says to use `run`; `validate --strict` warns when no component of the experiment's
  own is required evidence.
- `run` prints `cores` and `attempt` beside a generated scenario's seed, and the batch summary prints the cores
  per run: a seed draws the same scenario only at the same cores (`docs/contracts.md`, Seeds).
- A `smoke` run prints `Status: PASS (mode: smoke; no coverage claim)`; the batch summary counts smoke runs
  apart (`smoke`, `cores`, and per-run `attempt` and `mode` in `summary.json`; `counts` leaves smoke runs out).

### Changed: exit codes and replay verification (read by `valkey-ci-agent`)

- New exit code `64` (`EX_USAGE`) for usage and input errors: a bad option or option mix, no command,
  a missing or unreadable input file, a bare `plan.json` replayed without `--allow-drift`. These
  exited `2` before; `2` now means harness `ERROR` only. `batch` and `validate` keep `2`.
- `replay` of a run that `FAIL`s on a different Valkey binary or artifact exits `1` (was `2`); the drift
  is in the report's `provenance.drift`. A PASS or ERROR on drifted code still exits `2`.
- `minimize` exits `2` when no reduction kept the failure (was `0`).
- `replay` of a bare `plan.json` is refused unless `--allow-drift`; a run now writes `reproducer.json` beside
  `plan.json` so `replay <folder>/plan.json` is verified. Python `major.minor`, platform, and a digest
  missing on either side now count as drift.

### Added: multi-client workload for the history oracle

- `valkey.workload.key-value` takes `clients` (1 to 64, default 1): that many writer threads, each on its
  own connection, over the same keys, so operations overlap and `valkey.oracle.history` checks a concurrent
  history. The workload evidence records `clients`; the oracle records it too, words its verdict by it
  ("single-client history" or "N-client history"), and takes `min_clients` to ERROR on a workload with fewer.
- With `clients` above 1, `valkey.oracle.ledger` accepts every value an overlapping write could leave and
  leaves reads to the history oracle; `mix.rename` and a `valkey.oracle.rdb-snapshot` `workload` need
  `clients: 1`. Replica reads are not part of the history (they are not linearizable against the primary).
