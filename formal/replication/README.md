# Primary-replica replication (TLA+)

A [spec bridge](../../docs/spec-bridge.md) bundle. `model.tla` models Valkey's replication between a primary and
its replicas (`src/replication.c`): PSYNC on two replication IDs, full sync over one channel or two (dual-channel
replication), the backlog, promotion and `REPLICAOF`, failed loads, and restarts from an RDB file. A node's data set
is the sequence of writes it applied and its offset is `primary_repl_offset`; a replication ID with an offset names
a data set, and PSYNC trusts that naming. The model checks that Valkey's rules keep it unique.

| Model action | Valkey code |
|---|---|
| `Write` | a client write; `feedReplicationBuffer` advances the offset only on a node with a backlog |
| `Stream` | a replica applying the stream |
| `Trim`, `FreeBacklog` | `incrementalTrimReplicationBacklog`; `replicationCron` freeing the backlog after `repl-backlog-ttl` under a new ID |
| `PartialSync` | `primaryTryPartialResynchronization` accepting (`+CONTINUE`), `replicaProcessPsyncReply` moving a changed ID to replid2 |
| `FullSync` | `+FULLRESYNC` to the end of the load; `syncCommand` gives a primary without a backlog a new ID first |
| `DualFork` | `+DUALCHANNELSYNC`, then the RDB channel's fork and `$ENDOFF` |
| `DualContinueEarly`, `DualLoadContinued` | the main channel's `+CONTINUE` before the load ends, then `dualChannelSyncSuccess` |
| `DualLoadFirst`, `DualContinueLate` | the load ends first (`replicaAfterLoadPrimaryRDB` with the RDB channel at `RDB_LOADED`), then `+CONTINUE` |
| `LoadFails` | a load that fails after the flush: `Discarding the half-loaded data` |
| `Disconnect` | the link drops; the replica caches its primary (`replicationCachePrimary`) |
| `Promote` | `replicationUnsetPrimary`, `shiftReplicationId` |
| `Follow` | `replicationSetPrimary`, `replicationCachePrimaryUsingMyself` for a primary |
| `Save` | `rdbPopulateSaveInfo`: the ID and offset only with a backlog (primary) or a primary, live or cached (replica) |
| `Crash` | a restart through `loadDataFromDisk`: a primary keeps the file's ID as replid2, a replica caches itself |

## Properties

- `OneHistoryPerId`: any two claims on one replication ID (a primary with a backlog, a replica streaming or holding a
  cached primary, a secondary ID with a backlog, an RDB file with replication info) name the same data set at every
  offset both reach.
- `ReplicaIsPrefix`: a streaming replica holds a prefix of its primary's history at the same offsets.

## Files

| File | Holds |
|---|---|
| `model.tla` | the model and both properties |
| `model.cfg` | single-channel replication: must find no error |
| `dual-channel.cfg` | dual-channel replication with the fix of fuzzer issue #133 (`AdoptOnLoad`): must find no error |
| `negative-controls/keep_old_id_on_load.cfg` | Valkey as it is: must violate `OneHistoryPerId` (#133) |
| `negative-controls/keep_id_without_backlog.tla` | freeing the backlog keeps the ID: must violate `OneHistoryPerId` |
| `negative-controls/no_shift_on_promotion.tla` | a promoted replica keeps its ID: must violate `OneHistoryPerId` |
| `negative-controls/save_info_without_primary.tla` | an RDB file always records the ID: must violate `OneHistoryPerId` |
| `negative-controls/no_second_offset_check.tla` | PSYNC on replid2 ignores `second_replid_offset`: must violate `ReplicaIsPrefix` |
| `claims.yaml` | the properties, scope, assumptions, bounds and exclusions (status `approved`) |
| `mapping.yaml` | the log lines that show each step |

Each `.tla` control `EXTENDS model` and replaces one action, so it checks the model's own property.

## Results (3 nodes, 2 writes, 5 replication IDs, 1 crash, 2 promotions or `REPLICAOF`s)

`valkey-fuzzer spec check replication` passes every gate in about three minutes on 12 cores: `model.cfg` has
564,909 distinct states and `dual-channel.cfg` 7,368,305, with no error; each control's counterexample is 5 to 10
states long.

`keep_old_id_on_load` is Valkey today, and it is a bug: when a dual-channel replica's snapshot finishes loading
before the main channel's `+CONTINUE`, `replicaAfterLoadPrimaryRDB` creates a backlog after the replica's old offset
but keeps its old ID and offset until `+CONTINUE`. Promoted in that window (or after the sync then fails), the
replica shifts its old ID onto the primary's data, and a former replica at that old ID and offset continues with 0
bytes and keeps its old data. TLC's counterexample has 10 states; it was reproduced on Valkey unstable and filed as fuzzer issue
[#133](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/133). The safe `dual-channel.cfg` checks the fix
that adopts the snapshot's ID and offset when the load ends.

The model does not show the replica exit of fuzzer issue #12: there `LoadFails` is what Valkey should do on a disk
load whose main channel closes, and Valkey exits instead.

## Checking runs

`spec conform replication RUN` replays a run's mapped log lines (published by `valkey.observer.log-trigger` with
`spec: replication`) and reports which steps it took. Both properties compare data sets, which a run does not show in
the model's terms, so conform checks neither (`conform: excluded` in `claims.yaml`); the ledger and data-consistency
oracles compare data after a run. The four dual-channel steps are unobservable as such: Valkey logs `+CONTINUE` and the end of the
load, and which step a line shows depends on their order.

## Run

```bash
curl -fsSL -o tla2tools.jar https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar
echo "936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88  tla2tools.jar" | sha256sum -c -
valkey-fuzzer spec check replication --tla2tools tla2tools.jar --output assurance.json
```
