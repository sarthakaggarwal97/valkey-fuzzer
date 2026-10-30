# Base contracts

These are the rules the core enforces for every scenario, whatever produced
it. An experiment or a plugin cannot opt out of them. The terms are defined
in [Concepts and glossary](concepts.md); how to meet each rule in code is in
the [extension guide](extending.md).

- [One execution path](#one-execution-path)
- [Outcomes and verdicts](#outcomes-and-verdicts)
- [Evidence contract](#evidence-contract)
- [Fault ownership](#fault-ownership)
- [Effective server config](#effective-server-config)
- [Legal space and search profiles](#legal-space-and-search-profiles)
- [Deadlines and cancellation](#deadlines-and-cancellation)
- [Host state](#host-state)
- [Seeds, replay, and provenance](#seeds-replay-and-provenance)

## One execution path

Every scenario is an ordinary scenario file, compiled to one immutable
execution plan and run by one runner, with one report format and one
verdict. That holds for a handwritten scenario, one `run --random` or a
batch draws, a matrix case, a minimized one, a bug zoo run, and one that
`spec translate` derives from a counterexample. A generated scenario is
saved as `scenario.yaml` next to its report, and `valkey-fuzzer run` runs it
again without the generator.

`valkey-fuzzer run` runs one scenario; `valkey-fuzzer batch` is the one way
to run many. Nothing else starts a deployment.

## Outcomes and verdicts

| Outcome | Meaning | Exit code |
| --- | --- | --- |
| `PASS` | Every required component passed, and the required events and evidence were observed. | 0 |
| `FAIL` | A required oracle or action failed, or required events or evidence are missing. This is a finding about the system under test. | 1 |
| `ERROR` | The harness could not reach a trustworthy verdict. | 2 |
| `SKIP_UNSUPPORTED` | A required capability probe, or an artifact or resource provider, declined the target before anything changed. | 3 |

The rules behind them:

- **Only oracles and actions judge the system under test.** A `FAIL` from a
  capability probe, a workload or observer start, or a fault injection makes
  the run `ERROR`, because the conditions for a verdict were never
  established. A required capability that returns `SKIP` makes the run
  `SKIP_UNSUPPORTED`; any other required component that returns `SKIP` makes
  it `ERROR`. A component skipped because a dependency did not pass changes
  nothing by itself: the dependency's result decides.
- **A component that returns `ERROR` or raises makes the run `ERROR`**, and so
  does a call that outlives its deadline. The one exception is an oracle's
  `ERROR` next to another oracle's finding: each oracle judges on its own, so
  all of them are evaluated even after one returns `ERROR`, and a `FAIL` from
  another required oracle (say `valkey.oracle.liveness` finding the server
  dead that another oracle could not read) stands.
- **Missing evidence never passes.** In `experiment` mode a plan needs at
  least one required oracle and one required evidence item. A run that
  never published a required event or recorded a required evidence item
  fails, and its summary names each missing item and its source, for example
  `required evidence was not observed: valkey.rdb.reload-verified from snapshot`.
  Missing evidence is a finding only when its source looked at the server:
  an observer, action, or oracle that did not report the item as one it
  could not observe (`ComponentResult.unobserved`: the server did not
  answer, a log could not be read), and whose precondition evidence, if it
  names one (`ComponentResult.preconditions`), was recorded. When every missing item comes from a
  workload, fault, or capability, or was unobserved, and nothing else failed,
  the run is `ERROR`:
  `required evidence was not established: valkey.fault.injected from partition; ...`.
- **A contract violation makes the run `ERROR`**: evidence or an event that
  breaks its component's declarations, or a state change left for the runner
  to undo (or undone only after a failed try). No verdict can rest on it.
- **A failed teardown makes the run `ERROR`** unless a required oracle or
  action failed and nothing else did, in which case the run stays `FAIL` and
  the teardown error is kept in the diagnostics. Oracles run after teardown,
  so this is decided at the end. Teardown failures include a fault that never
  fired (its `recover` fails) and a deployment or environment cleanup error.
  A forbidden log pattern matched by `valkey.observer.log` is not one: the
  observer records it as evidence, and `valkey.oracle.liveness` fails the run
  on it.
- **Required components** are those with `required: true` (the default), the
  sources of required evidence, and everything they depend on. A `FAIL` or
  `SKIP` from a component that is not required does not decide the outcome,
  but its `ERROR`, an exception, a timeout, or a failed teardown still makes
  the run `ERROR`.

An outcome's diagnostics name the cause: `missing_required_events`,
`missing_required_evidence` (and, of those, `unestablished_required_evidence`),
`required_component_failures`, `untrusted_component_failures`,
`contract_violations`, `open_obligations`, `cleanup_errors`,
`abandoned_calls`, or `error`.

## Evidence contract

Every component registration declares the events it publishes and the
evidence it emits, each with a schema, and the kinds of state it changes. A
registration without declarations is rejected when the plugin loads.
`valkey-fuzzer list components --format json` prints them, and
[Valkey components](components.md#events-evidence-and-state-changes) lists
them for the Valkey plugin.

At compile time, a required event or evidence ID that no component of the
plan declares (and the runner does not publish) is an error.

At run time the runner checks every event and evidence item as it is
produced. It makes the run `ERROR`, naming the component, when one:

- publishes an undeclared event, a malformed payload, a runner lifecycle
  event, or anything after its last call returned (the event is never
  dispatched);
- emits undeclared or malformed evidence, a single-valued evidence ID twice,
  or evidence naming an uncommitted event (the item is dropped before any
  oracle sees it and listed under `contract_violations`);
- changes state of a kind it did not declare, or after its last call
  returned.

## Fault ownership

Every change a component makes to host or server state and will undo (a
stopped or paused process, a partition, a filled disk, a setting it
restores, a private server) is owned by an **undo obligation** from the
moment it is applied until it is undone. The undo is idempotent; a failed
undo leaves the obligation open and is tried again.

The runner undoes, newest first, whatever is still open once the components
are torn down, and retries failed undos after the deployment is cleaned up.
A start call the runner gave up on (see
[Deadlines and cancellation](#deadlines-and-cancellation)) may still be
applying changes, so both passes leave what its component owns alone; that
is undone the moment the call returns. After the last pass, a change opened
by anything still running (an abandoned call, a thread a component left) is
undone at once, and `open` raises.
Any obligation its owner did not undo on the first try makes the run
`ERROR`, even when a retry succeeded: the deployment was not in the state
the plan describes. `diagnostics.obligations` in the report lists the
`planned` (declared) changes, and every obligation opened, as `undone` or
`still_active`.

A change meant to last until the deployment is removed (a kill without
restart, a new binary, a `CONFIG SET` an action makes) needs no obligation.

## Effective server config

The execution plan records, under `instances`, every instance's role, shard,
and effective server config: the deployment provider's defaults under the
deployment's `server_config`, the per-role and per-instance configs, and the
experiment's startup overlays. Compile-time checks read it, so relying on a
default is checked like writing it out. For example,
`valkey.oracle.ledger` rejects a crash loss window that the local
deployment's default `appendfsync everysec` makes too short. A plan whose
provider would now start other instances is refused.

## Legal space and search profiles

An experiment's `metadata.generator` block and its inputs schema are its
**legal space**: what may vary, within which ranges, and which combinations
are rejected. A **search profile** changes only how hard that space is
sampled (the share of edge values, narrower ranges, value weights) and the
batch budgets. It never widens a range, adds a value, or changes what is
checked. Every drawn scenario is compiled and statically preflighted before
it runs; one outside the legal space is redrawn, never run. The formats are
in [Scenarios and experiments](scenarios-and-experiments.md#declaring-what-may-be-randomized).

## Deadlines and cancellation

A component call has a deadline when the component sets `timeout_s` in the
experiment or the command has `--timeout SECONDS` (a deadline for the whole
run); with both, the earlier one applies. Components see it as
`context.deadline` and are expected to finish before it. Each call sees its
own deadline; a thread a component starts sees the run's, unless it runs
under a copy of the call's context (`contextvars.copy_context().run`), which
shares the call's deadline and cancellation. The wave engine runs its
operations that way.

- A call with no deadline runs on the runner's own thread. If it hangs, the
  run hangs.
- A call with a deadline runs on a separate thread. When the deadline passes,
  the runner **cancels** it: on that thread `context.cancelled` becomes true,
  `context.sleep()` returns `False` at once, and `context.deadline` is in the
  past. The runner waits up to 5 seconds for the call to return, and the run
  is `ERROR` either way.
- Python cannot kill a thread, so a cancelled call that ignores cancellation
  keeps running. The runner waits for it once more, up to 5 seconds, after the
  deployment is cleaned up (which usually ends calls blocked on a server).
  If it is still running, the outcome lists it under `abandoned_calls`, and
  every later run in the same process is refused as `ERROR` until it returns.
  A batch runs every job in its own process, so one abandoned call never
  blocks the next job.
- An interrupt (Ctrl-C) while a call with a deadline runs cancels that call
  the same way. The runner still tears down, undoes every open obligation
  (an interrupt during one undo does not skip the rest), and cleans up the
  deployment, and then exits `130`.
- With a run deadline, the subscribers of the events the runner publishes
  also run on a watched thread. Dispatch is still synchronous, so a fault
  still fires right after its trigger. Teardown and cleanup events
  (`<id>.stopping`, `<id>.stopped`, `<id>.stop-failed`, the deployment
  rollback and cleanup events, `run.completed`) get a 60-second window each.
  A dispatch still running at its deadline aborts the event bus: no later
  event is dispatched, and the run is `ERROR`.
- Teardown (`stop`, `recover`) ignores `--timeout` and gets the component's
  own `timeout_s` as a fresh window. A hung teardown without `timeout_s`
  hangs the run. A component whose start call the runner gave up on is torn
  down only after that call returns: the runner waits for it after
  deployment cleanup, and if it is still running then, the run records a
  cleanup error saying the component was not torn down.
- Provisioning the deployment and acquiring artifacts and resources are not
  interrupted. The runner checks the run deadline before and after
  provisioning.

## Host state

Every server, data directory, iptables chain, and tmpfs mount the fuzzer
creates is tagged with the fuzzer process that owns it. `valkey-fuzzer sweep`
removes what a fuzzer that is gone left behind, and never touches anything
untagged or owned by a running fuzzer (see [sweep](cli.md#sweep)). A batch
sweeps each worker's leftovers after every job; anything it finds is
recorded and makes the job `ERROR`.

## Seeds, replay, and provenance

Every component draws its random choices from `context.rng(component_id,
stream)`, a stream derived from the scenario seed, the component `id`, and a
stream name. The same seed repeats every choice: the keys a workload writes,
the replica a failover promotes, the instance a chaos action kills. The
generator draws each dimension of a random scenario from its own stream, so
the same seed and profile always draw the same scenario.

A seed does not fix timing. Thread scheduling, process start-up, and the
server's own timers still vary, so a fault that races with the server can
land differently on a rerun or a replay.

A run report records the digests of the plan, the experiment, the fuzzer's
source, each plugin's packages, the deployment provider, and each instance's
binary. `replay` refuses to run when any of them differ from what is
installed now, unless `--allow-drift` is given (see [replay](cli.md#replay)).
Replay is not hermetic: it runs with what is installed and cannot reproduce
timing.
