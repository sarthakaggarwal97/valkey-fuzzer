# Atomic slot migration (TLA+)

A [spec bridge](../../docs/spec-bridge.md) bundle. `model.tla` models Valkey's atomic slot migration
(`CLUSTER MIGRATESLOTS`, `src/cluster_migrateslots.c`) of one slot from a source primary S to a target primary T,
which has one replica R: the handshake, the forked snapshot with the slot's writes streamed behind it, the pause and
the failover grant, T's claim under a new config epoch, S learning the claim on the cluster bus, T's cleanup of a
failed import, and R following the import through T's replication stream and taking over when T crashes.

| Model action | Valkey code |
|---|---|
| `Write` | a client write to the node that takes writes to the slot; S feeds it to T from the fork on (`clusterFeedSlotExportJobs`) |
| `Start` | `CLUSTER MIGRATESLOTS` on S: connect and send `SYNCSLOTS ESTABLISH` |
| `TEstablish` | `clusterCommandSyncSlotsEstablish`: T hides the slot (`setSlotImportingStateInAllDbs`), propagates ESTABLISH, replies `+OK` |
| `SFork` | `slotMigrationJobReadEstablishResponse`, `slotExportJobBeginSnapshotToTargetSocket` |
| `TApply` | T applies the snapshot or a fed write and streams it to R; after `SNAPSHOT-EOF` it sends `REQUEST-PAUSE` |
| `SPause` | `slotExportTryDoPause`: pause client writes, send `PAUSED` |
| `TPaused` | `clusterCommandSyncSlotsPaused`: send `REQUEST-FAILOVER` |
| `SGrant` | `clusterCommandSyncSlotsRequestFailover`: send `FAILOVER-GRANTED` |
| `TClaim` | `clusterCommandSyncSlotsFailoverGranted`, then `performSlotImportJobFailover` and `finishSlotMigrationJob` |
| `TClose`, `TCleanup` | `clusterHandleSlotMigrationClientClose` on T; the deferred cleanup (`delKeysNotOwnedByMyself`) and FINISH |
| `RApply` | R applies ESTABLISH (a tracking job), the keys, the deletes and `SYNCSLOTS FINISH` |
| `Deliver`, `LoseBus` | a bus message read (`clusterUpdateSlotsConfigWith`, `clusterUpdateSlotExportsOnOwnershipChange`), or lost with its dead sender |
| `DropLink` | the migration connection drops; S stays paused after the grant |
| `Cancel` | `CLUSTER CANCELSLOTMIGRATIONS` on S |
| `PauseExpires` | the pause ends at its timeout (`isSlotExportPauseTimedOut`) |
| `CrashT`, `Promote` | T crashes; R wins the election (`clusterFailoverReplaceYourPrimary`, `clusterCleanSlotImportsOnPromotion`, `verifyClusterConfigWithData`) |

## Properties

- `OneWriter`: at most one node takes client writes to the slot.
- `NoAckedWriteLost`: every acknowledged write is held by a live node, or is on its way to R in T's stream.

Both hold only under three constants that are not Valkey as it is (`claims.yaml` lists them as assumptions):

- `CancelKeepsPause`: a cancel after S granted the failover keeps S paused until it learns the owner. Valkey
  unpauses (`finishSlotMigrationJob`), though `FAILOVER-GRANTED` has left; upstream's test "Export unpauses when
  cancelled" asserts the unpause, with T held by `DEBUG SLOTMIGRATION PREVENT-FAILOVER` so that it never claims.
  When T reads the grant and claims the slot before it reads the close, S and T both take writes until S learns the
  claim, and then S deletes the writes it took (negative control `cancel_unpauses`).
- `ExpiryAfterClaim = FALSE`: S's pause outlasts T's claim. Valkey's pause ends at its timeout and logs "Write loss
  risk!" (`pause_expires_after_claim`): a documented window.
- `ClaimReachesReplica`: R learns T's claim before T acts on it, as legacy `CLUSTER SETSLOT` replicates to the
  replicas first. Valkey sends the claim to R only on the bus, and `SYNCSLOTS FINISH` does not change R's view of
  the owner, so R promoted after T crashed without that message deletes the slot's keys
  (`replica_misses_claim`, a crash and a lost message).

## Files

| File | Holds |
|---|---|
| `model.tla` | the model and both properties |
| `model.cfg` | the three fixes: must find no error |
| `negative-controls/cancel_unpauses.cfg` | Valkey's cancel: must violate `OneWriter` |
| `negative-controls/pause_expires_after_claim.cfg` | the pause ends after the claim: must violate `OneWriter` |
| `negative-controls/replica_misses_claim.cfg` | Valkey's replica: must violate `NoAckedWriteLost` |
| `negative-controls/grant_without_pause.tla` | PAUSED without a pause: must violate `OneWriter` |
| `negative-controls/grant_after_pause_ends.tla` | the grant after the pause ended: must violate `OneWriter` |
| `negative-controls/drop_unpauses_after_grant.tla` | a lost connection after the grant unpauses: must violate `OneWriter` |
| `negative-controls/feed_after_fork.tla` | writes fed only from the pause on: must violate `NoAckedWriteLost` |
| `claims.yaml` | the properties, scope, assumptions, bounds and exclusions (status `draft`) |
| `mapping.yaml` | the log lines that show each step |

## Results (3 writes, 3 faults)

`valkey-fuzzer spec check slot-migration` passes every gate in about ten seconds on 12 cores: `model.cfg` has 8,236
distinct states and no error, every action fires, and each control's counterexample is 10 to 17 states long.

`cancel_unpauses` is a known bug: upstream PR valkey-io/valkey#5327 (open) reports it, with a reproduction, and
fixes it as `CancelKeepsPause` models, by leaving exports in `failover-granted` running. `replica_misses_claim`
comes from the model and the code (a crash and a lost bus message); it is not reproduced, and no upstream issue or
PR mentions it.

## Checking runs

`spec conform slot-migration RUN` replays a run's mapped log lines (published by `valkey.observer.log-trigger` with
`spec: slot-migration`) and reports which steps it took. Both properties need each node's view of the owner and the
writes it holds, which a run does not show in the model's terms, so conform checks neither (`conform: excluded`).
No experiment migrates slots yet.

## Run

```bash
curl -fsSL -o tla2tools.jar https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar
echo "936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88  tla2tools.jar" | sha256sum -c -
valkey-fuzzer spec check slot-migration --tla2tools tla2tools.jar --output assurance.json
```
