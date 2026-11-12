# Persistence ordering (TLA+)

A [spec bridge](../../docs/spec-bridge.md) bundle. `model.tla` models the order in which one Valkey server writes its
multi-part AOF (a manifest naming a base file and incremental files), rewrites it in the background, turns it on and
off at runtime (`CONFIG SET appendonly`), saves RDB files, and restarts from the AOF or the RDB file after it is
killed. A kill keeps what the process wrote (the page cache), so the model is about the order of the writes,
renames, manifest updates and deletions, each of which a kill can land between; fsync and power loss are left out.

| Model action | Valkey code |
|---|---|
| `Write` | a client write; fed to `aof_buf` with the AOF on, or waiting for its rewrite while the child runs (`feedAppendOnlyFile`) |
| `Flush` | `flushAppendOnlyFile` in `beforeSleep`, before the replies go out |
| `StartRewrite` | `rewriteAppendOnlyFileBackground`: `flushAppendOnlyFile(1)`, `openNewIncrAofForAppend`, fork |
| `RenameBase`, `PersistManifest`, `DeleteHistory` | `backgroundRewriteDoneHandler`: rename the base, `markRewrittenIncrAofAsHistory` and `persistAofManifest`, `aofDelHistoryFiles` |
| `RewriteFails` | the child fails; waiting, the temporary incremental file and `aof_buf` are dropped (`aofDelTempIncrAofFile`) |
| `StopAof`, `StartAof` | `stopAppendOnly`, `startAppendOnly` (`AOF_WAIT_REWRITE`) |
| `StartSave`, `SaveDone` | `BGSAVE`; `rdbSave` renames the fsynced temporary file |
| `Crash` | a kill and a restart: `loadDataFromDisk`, `loadAppendOnlyFiles`, `aofOpenIfNeededOnServerStart` |

## Properties

- `ManifestFilesExist`: with appendonly yes, every file the manifest names exists, so a restart can load the AOF.
- `AofIsPrefix`: with the AOF on, its files hold a prefix of the data set: no write twice, none out of order, none
  missing before one that is there.
- `AckedRestored`: while appendonly is yes, a restart loads every write replied to while the AOF was on, since it
  was last turned on.

Writes replied to while `CONFIG SET appendonly yes` waits for its rewrite are not promised: a kill then restarts
from what an earlier period of the AOF left (or from nothing, ignoring the RDB file), as Valkey's documentation warns.
An RDB file is a snapshot of an earlier data set; after a kill it may hold writes the AOF never got, since their
replies never left.

## Files

| File | Holds |
|---|---|
| `model.tla` | the model and the three properties |
| `model.cfg` | kills at any step: must find no error |
| `negative-controls/manifest_before_base.tla` | the manifest names the base before its rename: must violate `ManifestFilesExist` |
| `negative-controls/delete_history_first.tla` | history deleted before the manifest drops it: must violate `ManifestFilesExist` |
| `negative-controls/no_flush_before_rewrite.tla` | the rewrite forks with writes in `aof_buf`: must violate `AofIsPrefix` |
| `negative-controls/feed_while_waiting.tla` | `aof_buf` fed while waiting without a child: must violate `AofIsPrefix` |
| `negative-controls/ack_before_flush.tla` | a reply before the write reaches the file: must violate `AckedRestored` |
| `negative-controls/current_incr_to_history.tla` | the current incremental file becomes history: must violate `AckedRestored` |
| `claims.yaml` | the properties, scope, assumptions, bounds and exclusions (status `draft`) |
| `mapping.yaml` | the log lines that show each step |

## Results (3 writes, 9 files, 2 kills, 2 `CONFIG SET appendonly`, 2 rewrites)

`valkey-fuzzer spec check persistence` passes every gate in about ten seconds on 12 cores: `model.cfg` has 306,147
distinct states and no error, every action fires, and each control's counterexample is 6 to 9 states long. The
model found no Valkey bug: each guard it relies on is in the code (the controls remove one each).

## Checking runs

`spec conform persistence RUN` replays a run's mapped log lines (published by `valkey.observer.log-trigger` with
`spec: persistence`) and reports which steps it took. The properties compare the files' writes with the data set's,
which a run does not show in the model's terms, so conform checks none of them (`conform: excluded`); the ledger
oracle checks acknowledged writes after a kill and restart.

## Run

```bash
curl -fsSL -o tla2tools.jar https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar
echo "936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88  tla2tools.jar" | sha256sum -c -
valkey-fuzzer spec check persistence --tla2tools tla2tools.jar --output assurance.json
```
