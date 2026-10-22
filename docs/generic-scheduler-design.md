# Generic activation scheduler

## Purpose

Feature authors need to place arbitrary components relative to observable
runtime boundaries without adding feature names or timing branches to the
compiler or runner. A scenario should be able to declare:

- activation after dependencies are ready;
- activation on an exact committed event occurrence;
- optional source filtering;
- run-wide or after-readiness occurrence scope;
- a deterministic offset;
- a bounded trigger timeout; and
- serial or concurrent invocation.

`valkey_fuzzer/scheduling.py` implements the feature-neutral declaration model,
incremental matcher, activation state machine, exact operation journal, and
replay verifier. The declarations are connected to component authoring,
compilation, durable plans, and bound plans. The planner does not invoke
extension code and is not yet connected to the runner lifecycle.

## Declarative contract

The component field is one nested activation document:

```yaml
activation:
  mode: concurrent
  trigger:
    kind: event
    event:
      name: action.started
      source: activate
      occurrence: 2
    scope: after_ready
    offset_s: 0.25
    timeout_s: 10
```

Defaults describe serial activation immediately after dependency readiness:

```yaml
activation:
  mode: serial
  trigger:
    kind: ready
```

`ActivationSpec` is the persisted policy. `ScheduledActivation` binds it to a
component instance. The scheduler has no component-kind or feature-specific
branches.

`scope: after_ready` selects the Nth matching commit strictly after the commit
cursor captured by `mark_ready`. `scope: run` selects the Nth matching commit
from the beginning of the run and can use an occurrence committed before
readiness. A run-scoped component still cannot be admitted before readiness.

`activation.trigger.timeout_s` bounds how long an event trigger may wait after
readiness. It does not limit component execution. The separate component-level
`timeout_s` is the runner's cooperative invocation deadline. `offset_s` is
measured from the successful event commit:

```text
scheduled_for = max(ready_at, event.committed_at + offset_s)
```

For a ready trigger, `scheduled_for = ready_at + offset_s`.

## Committed-event boundary

The scheduler accepts a structural `CommittedEvent` with all of:

- `name`
- `source`
- `payload`
- `event_sequence`
- `commit_index`
- `occurred_at`
- `committed_at`

A raw lifecycle event intentionally does not satisfy this protocol.
`event_sequence` and `occurred_at` are provenance from the attempted event.
`commit_index` and `committed_at` are the scheduler ordering boundary.

`event_sequence` is recorded only as provenance and may contain gaps.
`commit_index` is contiguous and starts at one. Scheduling never derives order
or time from `event_sequence` or `occurred_at`. This distinction prevents a
failed subscriber dispatch from becoming an activation input.

`observe_committed(event, timestamp=...)` records when the scheduler
coordinator consumed the envelope. That observation time must be at or after
`committed_at` and must not regress behind an earlier scheduler operation.
Keeping the two times separate is necessary because an event can commit while
the coordinator is processing another decision; draining that event later must
not wedge the contiguous commit cursor. Event offsets and trigger deadlines
still use `committed_at`, while operation ordering uses the observation time.

Equal-time ordering follows operation order. If an event committed exactly at
a trigger deadline is observed before `advance(deadline)`, it is eligible. If
the advance operation is recorded first, the trigger expires and the later
observe operation cannot revive it.

## State machine

Admission and invocation are separate decisions:

```text
                    ┌──────────────> CANCELLED
                    │
INITIAL ──ready──> READY ──advance due──> ADMITTED ──start──> INVOCATION_STARTED
                    │                       │
                    │                       └──abandon──> ADMISSION_ABANDONED
                    │
                    └──deadline──> EXPIRED
```

`CANCELLED`, `EXPIRED`, `INVOCATION_STARTED`, and
`ADMISSION_ABANDONED` are terminal scheduler states.

The distinctions prevent several lifecycle bugs:

- an admitted task that was never submitted does not acquire cleanup ownership;
- a failed worker submission can be represented as admission abandonment;
- cancellation cannot silently swallow an already admitted component; and
- trigger expiration remains different from dependency or shutdown
  cancellation.

Cleanup order is the reverse of `INVOCATION_STARTED` entry order. Admission
order is diagnostic only.

## Incremental matching and heap scheduling

The scheduler does not rescan all declarations for each event and does not
rescan event history for each waiter.

For each event selector key `(name, source-or-wildcard)`, it maintains:

- an append-only occurrence stream; and
- waiter buckets indexed by absolute target occurrence.

Readiness converts an event declaration to one absolute target:

```text
run target         = declared occurrence
after_ready target = current stream length + declared occurrence
```

If the target already exists, lookup is direct by list index. Otherwise the
component is inserted into exactly one waiter bucket. Observing an event
appends it to two streams—name wildcard and exact source—and probes only the
two buckets for the new stream lengths.

Ready offsets and matched event offsets enter a due min-heap. Trigger timeouts
enter a deadline min-heap. `advance(timestamp)` pops only boundaries at or
before the supplied timestamp, merges them by `(boundary, declaration_order)`,
and emits deterministic entries. Cancelled, admitted, or matched states leave
lazy heap records that are discarded when they reach the heap head.

The resulting scheduler work is proportional to:

```text
events + waiter registrations + matched waiters + heap pushes/pops
```

It is not proportional to `events × components` or
`events × occurrence-history`.

`SchedulerWorkCounters` exposes algorithmic counts so tests can guard these
properties without unstable wall-clock benchmarks.

## Exact operation trace

Every successful public mutation appends one `ScheduleOperation`:

- `observe`
- `ready`
- `advance`
- `cancel`
- `start`
- `abandon`

This includes operations that emit zero journal entries. Each operation
records its exact decision or observation timestamp, commit cursor, input
component batch, cancellation mode, committed-event envelope or abandonment
reason, and ordered output entry sequences.

Each `ScheduleJournalEntry` records:

- global entry sequence;
- producing operation sequence;
- batch index within that operation;
- component instance;
- transition;
- decision timestamp and commit cursor;
- selected event sequence and commit index, when applicable;
- calculated due/deadline boundary; and
- abandonment reason, when applicable.

The operation-to-entry partition preserves batch boundaries. For example, two
separate empty `advance` calls remain two distinct replay inputs rather than
disappearing from history.

The journal root persists:

- schema id `valkey-fuzzer.schedule-journal`;
- schema version;
- activation declaration digest;
- operation and entry high-water marks;
- trace digest;
- exact operations; and
- exact entries.

Strict decoding rejects unknown fields, duplicate JSON keys, non-finite
numbers, unsupported schemas, broken sequences, invalid state shapes, and
operation/entry partition drift.

## Replay

Replay does not validate entries in isolation. `validate_schedule_replay`
creates a fresh planner and applies each persisted operation through the public
state-machine API. It compares:

1. the reconstructed operation;
2. the complete emitted entry batch;
3. the final operation stream; and
4. the final entry stream.

An optional external committed-event stream is compared with all recorded
observe operations before replay. This binds the scheduler trace to the event
bus history rather than trusting an embedded event copy.

The activation digest detects use with different declarations. The trace
digest detects accidental serialized mutation, including removal or editing of
zero-output operations. Fresh reconstruction detects semantically impossible
but structurally well-formed operation/entry combinations.

Replay verification is not a signature or an authenticity mechanism. A caller
that needs adversarial tamper evidence must sign or otherwise protect the
complete run artifact.

## Runner integration

Current safety boundary: non-default activation declarations compile and
persist, but `FeatureRunner` rejects them during static preflight. That
rejection happens before topology-registry resolution, topology provider
construction or validation, component factories or validators, and environment
acquisition. Bound execution repeats the check defensively. The exact default
ready/serial activation continues through the existing serial phase runner
until the coordinator below replaces it.

The integrated runner should have one scheduler coordinator. Event publishers
and component workers may use other threads, but scheduler mutations flow
through the coordinator:

```text
committed event stream ─┐
worker completions ─────┼──> coordinator ──> activation planner
deadline/timer wakeups ─┘         │
                                  ├── persist operation and entries
                                  ├── publish runner lifecycle event
                                  ├── invoke serially or submit worker
                                  └── mark start or abandon admission
```

The coordinator should:

1. mark dependency-satisfied components ready in stable declaration order;
2. consume only committed event envelopes, recording the coordinator's current
   scheduler time separately from each envelope's commit time;
3. wait until an event, worker completion, cancellation, or
   `planner.next_boundary`;
4. call `advance`;
5. persist the decision before attempting invocation;
6. call `mark_invocation_started` immediately before extension code can run;
7. call `abandon_admission` when invocation will not start; and
8. register cleanup from actual invocation-start entries.

Serial invocation blocks further coordinator scheduling by design. Components
that must overlap another component use `mode: concurrent`.

Before topology cleanup or environment release, the coordinator must:

1. cancel initial and ready activations;
2. abandon admissions that will not be submitted;
3. signal cooperative cancellation to running invocations;
4. wait for all invocations to return;
5. clean components in reverse invocation-start order;
6. quiesce committed-event and scheduler activity;
7. clean the topology; and
8. release environment resources.

The runner must not clean topology or revoke a bound plan while a worker can
still access it.

## Result semantics

- A required expired trigger means its required scenario path was not
  exercised and should fail the run.
- An optional expired trigger is a reported skip.
- A failed or skipped dependency cancels not-yet-admitted dependents.
- Failure to submit admitted work becomes `ADMISSION_ABANDONED`.
- A scheduler invariant, worker protocol violation, or non-cooperative
  shutdown is an error.
- A trustworthy system-under-test failure remains the primary failure if later
  cleanup also fails; cleanup diagnostics are retained separately.

## Integration stages

### 1. Model and compiler cutover — complete

- `activation: ActivationSpec` is present on declarative and bound component
  specs.
- Feature-pack manifests parse the strict nested document.
- Canonical run-plan data requires and persists the declaration.
- Capability probes accept only the exact ready/serial default.
- The old `when_event` field has been removed.

### 2. Committed-event stream — complete

- The event bus emits `CommittedLifecycleEvent` envelopes only after successful
  dispatch.
- Cursor-based reads and waits expose the committed stream without callbacks.
- Failed attempted events never enter the committed stream.
- Quiescence wakes blocked committed-event readers.

### 3. Coordinator and serial scheduling — pending

- Replace the post-topology kind-phase loop with the coordinator.
- Start with serial ready and event activations.
- Persist the complete schedule journal in run diagnostics.
- Map expiration, cancellation, and abandonment to explicit component results.

### 4. Concurrent invocation

- Add a bounded runner-owned executor.
- Add a cancellation token to `RunContext`.
- Record invocation start only when work can actually begin.
- Join every worker before cleanup, topology teardown, or resource release.
- Add non-cooperative shutdown diagnostics and isolation guidance.

### 5. Replay and reporting

- Verify schedule traces during normal replay.
- Report admission, start, expiration, cancellation, and abandonment
  separately.
- Include operation and entry sequences in failure diagnostics.
- Add replay-drive mode only after verification mode is established.

## Acceptance properties

The focused scheduler tests establish that:

1. raw lifecycle events cannot activate components;
2. commit index and commit time define scheduler ordering;
3. run and after-readiness occurrence scopes are exact;
4. same-boundary decisions use declaration order;
5. admission and invocation-start ownership remain separate;
6. cleanup is reverse actual start order;
7. every successful call, including zero-output calls, is persisted;
8. replay reconstructs operations and entries on a fresh planner;
9. randomized schedules agree with an independent occurrence oracle; and
10. work counters rule out declaration/event and history/event cross-products.
