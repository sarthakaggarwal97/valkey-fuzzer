# Cluster failover election (TLA+)

A [spec bridge](../../docs/spec-bridge.md) bundle. `model.tla` models the failover vote on the Valkey cluster bus:
replicas of a failed primary request votes in a new epoch, each primary votes
at most once per epoch (`lastVoteEpoch`, saved in `nodes.conf`), and a replica
with a majority wins. Messages can be lost, delayed or reordered, and a
primary can crash and restart from `nodes.conf`.

| Model action    | Valkey code (`src/cluster_legacy.c`)                                                                  |
|-----------------|-------------------------------------------------------------------------------------------------------|
| `StartElection` | `clusterHandleReplicaFailover`, `clusterRequestFailoverAuth`                                          |
| `HandleRequest` | `clusterSendFailoverAuthIfNeeded`: grants the vote, schedules the save, and queues the ACK            |
| `SaveConfig`    | `clusterBeforeSleep` saving `nodes.conf`, after which the queued ACKs are sent                        |
| `BioWrite`      | the background writer thread writing a queued `nodes.conf` save (`best-effort` only)                  |
| `FlushEarly`    | a queued ACK sent before the save: the link's write handler was already registered, or a QoS poll flushed it (9.2+) |
| `ReceiveVote`   | `FAILOVER_AUTH_ACK` handling in `clusterProcessPacket`                                                |

## Files

| File | Holds |
| --- | --- |
| `model.tla` | The model, its invariants, and the target `VoteLostInCrash` |
| `model.cfg` | `sync`: every vote is saved before it leaves; must find no error |
| `negative-controls/vote-before-save.cfg`, `flush-before-save.cfg` | must violate `OneWinnerPerEpoch` |
| `negative-controls/double-vote.cfg` | must violate `VoteOncePerEpoch` |
| `negative-controls/vote-sent-before-save.cfg` | must violate `VotesSavedBeforeSent` |
| `negative-controls/vote-lost-in-crash.cfg` | must violate `NoVoteLostInCrash` |
| `claims.yaml` | the properties, scope, assumptions, bounds, and exclusions (status `draft`) |
| `mapping.yaml` | the log patterns, events, and samples each model action and variable corresponds to |

## Run

Needs Java 11 or later and `tla2tools.jar` from
<https://github.com/tlaplus/tlaplus/releases>. CI uses release v1.7.4
(TLC 2.19); download and check it with:

```bash
curl -fsSL -o tla2tools.jar https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar
echo "936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88  tla2tools.jar" | sha256sum -c -
valkey-fuzzer spec check cluster-election --tla2tools tla2tools.jar --output assurance.json
```

`spec check` runs SANY, TLC on `model.cfg` (no error, 220,138 distinct
states), and TLC on each negative control, which must report its property
violated; on a 12-core laptop the whole check takes about three minutes.
[`.github/workflows/tla-model-check.yml`](../../.github/workflows/tla-model-check.yml)
runs it on every push and pull request to `main` that changes `formal/` or the
spec bridge. In a safe config `BioWrite` and `FlushEarly` never fire: both
are disabled by `sync` (`PersistBeforeAck = TRUE`, `FlushBeforeSave = FALSE`).

## Results (3 primaries, 2 replicas, epochs ≤ 2, 1 crash)

- `model.cfg`: `nodes.conf` is saved before the vote leaves, as with
  `cluster-config-save-behavior sync` (the default). No violation in 220,138
  distinct states. This config also checks `VotesSavedBeforeSent` (every vote
  in flight is already on disk), which is why the crash cannot undo a vote,
  and `NoVoteLostInCrash` (no vote that left is missing from its voter's
  memory), the form of it a live run observes exactly.
- `negative-controls/vote-before-save.cfg`: the save is only queued for the
  background writer thread before the vote leaves, as with `best-effort`.
  `OneWinnerPerEpoch` fails in 17 steps: p1 votes for r1, its save is queued
  and the vote sent, p1 crashes before the background write, restarts with
  `lastVoteEpoch = 0`, and votes for r2 in epoch 1. With p2's vote r1 wins
  epoch 1, and with p3's vote r2 wins epoch 1 too.
- `negative-controls/flush-before-save.cfg`: `sync`, but a queued vote may leave
  before the save (`FlushEarly`). `OneWinnerPerEpoch` also fails in 17 steps:
  p3's vote for r1 leaves before its save, p3 crashes, restarts with
  `lastVoteEpoch = 0`, and votes for r2, so r1 and r2 both win epoch 1.

The model leaves out the 2 s `safe_to_join` window after restart, which refuses
votes for a short time and narrows the gap in `best-effort` mode.

The config name and its two values (`sync`, `best-effort`) are confirmed in the
local source: `cluster-config-save-behavior` in `src/config.c`, used in
`clusterBeforeSleep` (`src/cluster_legacy.c`), where `sync` drains the BIO
writer and saves synchronously while `best-effort` saves asynchronously via a
BIO job. `clusterSendFailoverAuthIfNeeded` grants the vote: it sets
`lastVoteEpoch`, schedules the save (`CLUSTER_TODO_SAVE_CONFIG`), and queues
the ACK on the link. The ACK is sent after `clusterBeforeSleep` saves. Under
`sync` the save is on disk by then; `best-effort` only queues it for the
background writer thread, so the ACK can leave before `lastVoteEpoch` reaches
disk.

## From model to fuzzer

The `valkey.cluster-election-durability` experiment runs this counterexample's
fault schedule against real `valkey-server` processes. The experiment and an
example scenario are
[`valkey_fuzzer_experiments/experiments/valkey.cluster-election-durability.yaml`](../../valkey_fuzzer_experiments/experiments/valkey.cluster-election-durability.yaml)
and [`examples/valkey/cluster_election_durability.yaml`](../../examples/valkey/cluster_election_durability.yaml);
the components are described in [Valkey components](../../docs/components.md).
The experiment needs Valkey 9.2.0 or later, the first to log each counted
vote with the count and quorum; older builds report `SKIP_UNSUPPORTED`.

`mapping.yaml` holds every log pattern the experiment uses: `election-log`
(`valkey.observer.log-trigger` with `spec: cluster-election`) publishes each
pattern's line as its event, and `election-safety` reads the event names
from the same file.

| Model action or variable | Implementation evidence (`mapping.yaml`) |
| --- | --- |
| `StartElection(c)` | `started`: `Starting a failover election for epoch E`; driven by `valkey.action.cluster-election` (`first-election`, `second-election`) |
| `HandleRequest(p)` | `granted`: `Failover auth granted to …` (sets `lastVote`); `denied`: `Failover auth denied to …` |
| `ReceiveVote(c)` | `acked`: `Failover auth ACK from … (ACKs N, quorum Q)`, logged by the candidate: the vote left the voter |
| `Win(c)`, `winners` | `won`: `configEpoch set to N after successful failover` |
| `Crash(p)` | `crash`: the `*.injected` event of `valkey.fault.process-signal` (`kill-voter`), with the `nodes.conf` vars read at the kill |
| `currentEpoch`, `diskEpoch`, `diskVote` | `sample`: `valkey.observer.cluster-trace` (`trace`) samples of `CLUSTER INFO` and the `vars` line of `nodes.conf` |
| `SaveConfig`, `BioWrite`, `FlushEarly`, `LearnEpoch` | unobservable (not logged) |
| `bio` | hidden: queued background saves are not observable |

`valkey.oracle.cluster-election-safety` checks `OneWinnerPerEpoch`,
`VoteOncePerEpoch`, the `Win` quorum, votes saved before they leave, and no
split primary inside the run.

The fault fires inside the triggering event's synchronous dispatch, so the kill
lands right after the vote line rather than after a sleep: median kill latency
from the log line to the `SIGKILL` was **3–10 ms** across the batch below.
Under `sync`, `election-safety` requires every delivered vote to be on disk
(`require_durable_votes`); under `best-effort` it only records how often a vote
was delivered before its save, the state the model flags as risky.

### Checking a run against the spec

Each run writes `components/trace/trace.jsonl` (events plus per-node epoch
observations and `nodes.conf` vars) in its report folder, next to `plan.json`
and `report.json`. `spec conform` rebuilds the model's state from it through
`mapping.yaml` and has TLC check `OneWinnerPerEpoch`, `VoteOncePerEpoch`, and
`NoVoteLostInCrash` on every observed state, and report whether the run reached
`VoteLostInCrash`:

```bash
valkey-fuzzer spec conform cluster-election RUN --tla2tools tla2tools.jar
```

`VotesSavedBeforeSent` is excluded there: `nodes.conf` samples lag behind the
file, so an observed `diskVote` is a lower bound. The oracle checks it
exactly at each crash. This is an observed-state check and action coverage,
not refinement: observed steps are not checked against the model's `Next`.

## Live batch results

```bash
valkey-fuzzer batch examples/valkey/cluster_election_batch.yaml --out ./election-batch
valkey-fuzzer spec conform cluster-election ./election-batch \
  --group-by save_behavior,election --tla2tools tla2tools.jar
```

The batch leaves one run folder per case under `./election-batch/runs/`;
`spec conform --group-by` tabulates the runs by those inputs. The results
below were measured with one-off scripts that `spec conform` has since
replaced.

108 runs (36 combinations, 3 seeds each, every run on a cluster of 3
primaries with 2 replicas each) over both save behaviors, the three ways to
start the election (`force_failover`, `kill_primary`, `pause_primary`), which
granted vote the crash follows, and whether the second election falls inside
the restarted primary's 2 s `safe_to_join` window, on Valkey 9.2.0
(`6a6841cf`) on macOS:

| save_behavior | runs | pass | reached "unsaved at crash" | vote delivered before save | safety violations |
|---------------|------|------|----------------------------|----------------------------|-------------------|
| `sync`        | 54   | 54   | 53                         | 0                          | 0                 |
| `best-effort` | 54   | 54   | 45                         | 41                         | 0                 |

- **No `OneWinnerPerEpoch` violation reproduced** on this build. All 108 runs
  passed, and the trace checks found no invariant violation in any trace.
- Under `sync`, votes were held in memory at the crash in 53 runs but **never
  delivered to a candidate before the save**, matching `PersistBeforeAck = TRUE`.
- Under `best-effort`, **41 of 54 runs delivered a vote before `lastVoteEpoch`
  reached disk**, the model's risky `PersistBeforeAck = FALSE` state, yet none
  went on to a double vote in one epoch.

So the batch reproduced the vote-before-save sequence live under
`best-effort` (a vote that left before it was saved, then a crash that loses
it), not a double-winner violation. The model over-approximates: it omits the
2 s `safe_to_join` window that refuses votes right after a restart, and it
lets a re-election reuse an epoch, whereas a real restarted primary learns the
higher `currentEpoch` by gossip and the next election runs in a new epoch, so
the re-vote lands in a different epoch than the one it already voted in. The
slot `configEpoch` check the model omits is a further guard. The experiment
drives the vote-before-save sequence and fails on a second winner or a split
primary, so it can serve as a regression test if those protections change.
No Valkey bug was found.
