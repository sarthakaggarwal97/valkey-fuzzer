# Actions

Part of the [Valkey component reference](README.md): the
[index](README.md#index) lists every component, [Common config
forms](README.md#common-config-forms) the selectors, predicates, and trigger
fields the tables below use.

Actions run once, in dependency order, and judge what they did: a `FAIL` from
an action is a finding about the system under test.

## `valkey.action.command`

Sends `command` to each selected instance `repeat` times and checks each reply.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `instances` | selector | `primaries` | |
| `command` | list of strings or numbers | | For example `[CONFIG, SET, maxmemory-policy, allkeys-lru]`. |
| `expect` | mapping or null | `null` | A [predicate](README.md#predicates) on the reply, or `{error: true}`, `{error: false}`, or `{error: "<substring>"}`. `null` accepts any reply that is not an error. |
| `repeat` | integer, 1 to 100,000 | 1 | |
| `interval_s` | number, 0 to 3600 | 0 | Pause between repeats. |
| `timeout_s` | number, 0.1 to 3600 | 10 | Client timeout, and how long to keep trying to open a connection. |
| `allow_unanswered` | boolean | `false` | The command may set off a fault that kills the server before it replies, as a crash point inside `BGREWRITEAOF` does: a reply that never came is recorded, not an `ERROR`. |

Each command is sent once. A connection that could not be opened sent
nothing (under load the host can run out of local ports), so opening it is
retried for up to `timeout_s`; only then is its error recorded, with
`unanswered: true`, as is a connection lost or a reply that timed out.
`FAIL` when any reply does not match; `ERROR` when every reply that came back
matched but some command got none. Evidence: `valkey.command.replies`, one
record per reply.

## `valkey.action.commands`

Sends a command sequence on one connection, so `MULTI`/`EXEC`, `WATCH`,
`SELECT`, and client state span the commands (a command may name another
connection, for a subscriber or a second client), and with `compare_with`
compares every reply with the same sequence on another build; with `modules`
and `modules_parent`, on another build of a module.

```yaml
- use: valkey.action.commands
  id: lset
  config:
    compare_with: {$ref: artifacts.parent.binary}   # the parent commit's build
    server_config: {list-max-listpack-size: 4}      # both servers start with it
    resp: 2
    commands:
      - {command: [RPUSH, l, a, b, c], expect: {equals: 3}}
      - {command: [LSET, l, 9223372036854775807, x], expect_difference: true,
         expect: {error: "index out of range"}}
      - {command: [LRANGE, l, 0, -1], expect_difference: true, expect: {equals: [a, b, c]}}
      - {command: [HGETALL, h], compare: unordered-pairs}
      - {command: [RESTORE, k, 0, !!binary "AA4BAgMEBQ=="], expect: {equals: OK}}
```

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `compare_with` | binary path or null | `null` | A build to compare with, usually `{$ref: artifacts.parent.binary}`. With it the sequence runs on two private servers the action starts, one of the deployment's binary (the candidate) and one of this (the parent), both from an empty dataset, each from a config file (`valkey.conf`) in its own directory, which is also its working directory, so `CONFIG REWRITE` works; the deployment's instances are never touched. |
| `modules` | list of paths or references | `[]` | Modules both private servers load (`--loadmodule` each, a bare path: see [`valkey.artifact.local-module`](providers.md#valkeyartifactlocal-module)), usually `[{$ref: artifacts.module.path}]`; recorded in the evidence. Needs `compare_with` or `modules_parent`: the deployment's instances load modules through `deployment.server_config.loadmodule`, which `server_config` here refuses. |
| `modules_parent` | list of paths or references, or null | `null` | The parent server loads these instead of `modules`, usually `[{$ref: artifacts.module_parent.path}]`. Without `compare_with`, both servers run the deployment's binary, so the sequence compares two builds of a module on one Valkey build; with it (a core change to the module API), each Valkey build loads its own commit's modules. Needs `modules`. |
| `instances` | selector | `primaries` | Without `compare_with`: the instances the sequence runs on, one connection each. |
| `cluster` | boolean | `false` | With `compare_with`: every private server runs in cluster mode (`cluster-enabled yes`); node 0 serves every slot (`CLUSTER ADDSLOTSRANGE 0 16383`), every other node meets it (`CLUSTER MEET`) as an empty primary, and each node's `cluster_state:ok`, knowing every node, is awaited within `timeout_s`, so a sequence can test `CLUSTER` subcommands, slot-keyed replies, and, with `nodes`, slot migration and replication it sets up itself. Without `compare_with` it is a config error: the deployment decides the mode. |
| `nodes` | integer, 1 to 6 | 1 | With `compare_with`: each side runs on this many private servers, its nodes, and a command names its node with `node`. Without `cluster` they are standalone primaries; the sequence sends `REPLICAOF` itself. |
| `server_config` | mapping | `{}` | Server options both private servers start with, as the deployment's `server_config`; the options the deployment manages (`dir`, `port`, `cluster-enabled`, ...) are refused. `sanitize-dump-payload yes` and `enable-debug-command yes` unless set. An option the parent rejects is dropped on both sides and recorded as `dropped_config`; test an option the change adds with `CONFIG SET` inside the sequence, marked `expect_difference`. |
| `files` | mapping | `{}` | With `compare_with`: files written into every private server's directory before it starts, by plain relative path (no `..`, not the harness's own `valkey.conf`, `server.log`, `output.log`, `s.sock`): a string as UTF-8, or `!!binary`. `server_config` may name them, as `{aclfile: users.acl}`; the default user must stay usable without a password, since the harness connects as it. |
| `resp` | 2 or 3 | 2 | The protocol of every connection. |
| `timeout_s` | number, 0.1 to 3600 | 10 | Per command: a reply that takes longer is `unanswered`, and the sequence stops there. |
| `commands` | list of mappings | | The sequence, below. |
| `judge_replies` | bool | `true` | `false`: replies are recorded, never judged; only a crash, a protocol error, a command with no reply, and the checks decide the verdict (a sequence replayed only for its checks). |
| `checks` | mapping or null | `null` | With `compare_with` (or `modules_parent`): `{variants: bool, replica: bool, acl: bool, concurrency: 0 to 16, compat: bool, faults: bool}`, each off unless set, below. `compat` needs `compare_with`. |

Each command has:

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `command` | list of strings, numbers, or `!!binary` values | | A `!!binary` argument is sent as bytes (it loads as `{$base64: ...}`). A string argument may hold placeholders, below. The sequence waits for its reply (`timeout_s`) before the next command, so a command that holds its connection until a later one releases it, such as `EVAL "while true do end" 0` until `SCRIPT KILL`, goes as `raw` on a `connection` of its own, and a later `read` there takes its reply. |
| `raw` | string or `!!binary` value | | Instead of `command`: bytes sent as they are, a string as UTF-8 (placeholders replaced), for the inline protocol and malformed requests: `raw: "SET k v\0 EX 100\r\n"`. One reply is read; when none begins within a second (the bytes are not a whole request, or the server holds them), the command is recorded as `{no_reply: true}`: an `expect` sees a null reply, and it compares unequal to any reply. A further reply stays on the connection for the next command (or a `read`), and bytes the server left unanswered stay in its buffer, so send raw bytes on a `connection` of their own. |
| `restart` | boolean | `false` | Instead of `command`, with `compare_with`: stop the node gracefully (`SIGTERM`, as `SHUTDOWN` does, which a sequence may not send) and start it again from its directory and config file, so it reads back what the sequence rewrote there: `CONFIG REWRITE`, `ACL SAVE`, a `SAVE`d RDB, `nodes.conf`. Recorded as the reply `OK` once the node answers `PING` again, and in a cluster once its `cluster_state` is `ok` again (a node that stays otherwise is recorded as that error, within `timeout_s`); its connections open anew on their next use. A node that does not stop within 30 s, or does not answer again, is a command that got no reply; one that exits at startup has exited. |
| `read` | boolean | `false` | Instead of `command`: read the replies waiting on the connection, and each that begins within a second of the last, as one list (an error among them as `{$error: ...}`); for Pub/Sub messages and RESP3 pushes such as client-side caching invalidations. An empty list when none came. |
| `node` | integer | 0 | The node the command goes to, by index; less than `nodes`. |
| `connection` | string | `main` | The connection to the node it goes on. Every other name opens a connection of its own on first use, which keeps its state (a `SUBSCRIBE`, a `CLIENT TRACKING`, bytes `raw` left unanswered) apart from the main one. `SUBSCRIBE`, `PSUBSCRIBE`, and `SSUBSCRIBE` go on a named connection, one channel each, and so do their unsubscribing commands (one channel each, on any connection): every channel is confirmed by a reply of its own. |
| `wait_s` | number, 0.1 to 3600, or null | `null` | Send the command again every 0.1 s until its reply meets `expect`, for this long at most: for asynchronous work the sequence started, such as `[GET, k]` on the target node until a slot migration (`CLUSTER MIGRATESLOTS`) finished, or `[INFO, replication]` until `master_link_status:up`. Its last reply is recorded, judged, and compared like any other, so a candidate that never meets `expect` violates it. Needs `expect`. |
| `expect` | mapping or null | `null` | A [predicate](README.md#predicates) on the reply (`equals` also takes a list or a mapping, compared as JSON data, or null, a null reply), or `{error: true}`, `{error: false}`, or `{error: "<substring>"}`. `null` accepts any reply that is not an error. |
| `expect_difference` | boolean | `false` | The change under test means this reply to differ from the parent's: the command the change is in, or a later read it carries into. Needs `expect` and `compare_with`. |
| `compare` | `equal`, `unordered`, `unordered-pairs`, `json`, `skip` | `equal` | How the two builds' replies compare: as sent; a flat reply with its items sorted; a flat field-value reply with its pairs sorted (values swapped between fields still differ; a RESP3 map compares as a map); the JSON value a bulk string (or each item of an array) holds, for valkey-json's `JSON.GET` and `JSON.MGET` (key order, spacing, and `1.0` against `1` do not count; `true` is not `1`; a reply that is not JSON compares as itself; an `expect: {equals: ...}` may give the JSON text or the value it holds, so `{equals: [1]}` matches `JSON.GET`'s `"[1]"`); or recorded and never compared (`TIME`, `TTL`, `INFO`, `CLIENT ID`, `SCAN` cursors, `OBJECT ENCODING`, `MEMORY USAGE`, `DEBUG OBJECT`). Avoid commands that also change state at random (`SPOP`, `SRANDMEMBER`, `HRANDFIELD`, `RANDOMKEY`) in a compared sequence: Valkey seeds its hash function per process. |

With `compare_with`, a string argument may name a node of the side it runs
on: `<node.1.id>` (its cluster node ID, with `cluster`), `<node.1.host>`,
`<node.1.port>`, `<node.1.cluster-port>`, and `<node.1.shard-id>` (its shard's ID
as the cluster formed; both with `cluster`) are replaced by
each side's own values, for example `[CLUSTER, SETSLOT, 7, NODE, <node.1.id>]`
or `[REPLICAOF, <node.0.host>, <node.0.port>]`. Angle brackets, not braces:
YAML reads `{node.1.id}` as a mapping, and Valkey braces as a hash tag. Every
node listens on its own loopback port (and cluster bus port), so the nodes can
meet and replicate; the sequence itself talks to each over its unix socket.
Before the two sides are compared, each side's node IDs, shard IDs, and ports in its replies
and errors (a port also as an integer, as `CLUSTER SLOTS` replies it) are
replaced back by their placeholders, so `CLUSTER NODES` and `MOVED` errors
compare equal when the builds behave the same, and an `expect` names them the
same way (`{contains: "<node.1.id>"}`); the evidence records the replies so
named. Other random values (`CLUSTER SHARDS` shard IDs, replication IDs) still
differ: compare those with `skip`.

Replies are recorded as the server sent them, over a raw connection without
valkey-py's response callbacks: `SET` replies `OK`, not `true`; `HGETALL` a
flat list on RESP2 and a map on RESP3; a bulk string that is valid UTF-8 as a
string, any other as `{$base64: ...}`, so two different binary replies never
compare equal; an error inside an array (a transaction's) as `{$error: ...}`.
No timestamps, so two runs' replies compare equal. A connection a private
server closes without a reply (`QUIT`, `CLIENT KILL`, a slot migration
client's `RESET`) is recorded as `{closed: true}` when its node still runs 5 s
later: an `expect` sees a null reply (`{exists: false}` expects it), it
compares unequal to any reply, and the sequence goes on, the connection opening
anew on its next use. When the node exited, the connection was lost to a
crash; on a deployment instance, a closed connection is a command without a
reply. A reply the client cannot parse (a RESP3 double `,abc`) is recorded as
`{protocol_error, raw}`, the client's message and the first 4 KB of the bytes
the command gets again on a fresh connection (same RESP and `SELECT`),
escaped; it is a `FAIL` with `diagnostics: {reason: protocol-error, commands:
[...]}`, even where the parent gives the same, and without `compare_with`;
not on a connection from its `SYNC` or `PSYNC` on, which reads the
replication stream.

The outcome, with `compare_with`:

- `FAIL`: a candidate reply that violates its `expect` where the parent
  replied otherwise, or a node of the candidate exiting (a crash, a sanitizer abort);
  its cause is `crashes` in the diagnostics; or a failing check (below).
  Also either build crashing when the action first starts it with
  `server_config` to see that it starts (its log or output holds a crash or
  sanitizer report, read as `valkey.oracle.liveness` reads one), before any
  command: `crashes` names the side, `candidate` or `parent`.
  Nothing else: a difference between builds is never, by itself, a Valkey
  failure.
- `ERROR` with `diagnostics: {reason: expect-disputed, pre_existing: true,
  commands: [...]}`: an `expect` the candidate violates but the parent
  replied the same to. The expectation is about behaviour the change did not
  touch, nearly always the test's own mistake, or else a bug both builds
  share (`pre_existing`: not the change's, filed once); each command is given
  with its `expect` and both replies.
- `ERROR` with `diagnostics: {reason: unreached-change}`: a marked command
  whose parent reply equals the candidate's, the parent alive. The sequence
  did not reach the change; for a bug fix, the parent is a build with the
  bug, so a test of the fix must see it there.
- `ERROR` with `diagnostics: {reason: unexplained-difference, commands:
  [...]}`: an unmarked command whose replies differ, both replies given.
  Usually a write the change affects carried into a later read that was not
  marked; sometimes a change the pull request did not intend. Not a `FAIL`:
  the fuzzer did not see Valkey do something wrong.
- A parent that dies on a marked command, the candidate replying as
  expected, counts as reached: the bug was a crash. A parent that dies on an
  unmarked command is an unexplained difference.
- `ERROR` for a private server that does not start without a crash report,
  or a command with no reply on the candidate (a connection lost without a
  crash, a timeout).

`checks` run the sequence again on fresh candidate servers once the compared
run completed on the candidate. A check that sees Valkey do something wrong
is `FAIL` with `diagnostics.reason` and what reproduces it; a candidate that
exits during one is a crash. `variants`: compact encodings off
(`*-max-listpack-entries 0`, `set-max-intset-entries 0`,
`list-max-listpack-size 0`; not with `modules`), then the other `resp` (Valkey's commands only: a module's, named with a dot, may reply by protocol; not on a connection first used while the sequence set a `requirepass`, which a RESP3 client's `HELLO 3` cannot open);
replies compare as multisets of their values shaped as RESP2's, except
`compare: skip`, `raw`, `read`, `DEBUG`, `OBJECT`, `MEMORY`, `CONFIG`,
`CLIENT INFO`/`LIST`, `CLUSTER INFO`/`GETSLOTMIGRATIONS`, `HELLO`, `INFO`, and `PSYNC`, and a script that calls one of
them (`variant-mismatch`); a node or shard ID the sequence makes (a
`CLUSTER RESET`'s) is named by its first appearance in `CLUSTER` replies.
`replica`: node 0 gets a synced replica, whose offset and `DEBUG DIGEST` must
reach the primary's within 30 s of the sequence (`replica-diverged`). `acl`:
as the default user with `+@all -@<category>` (from `COMMAND INFO`, the one
most commands but not all are in, never `@fast` or `@slow`), then with `+@all
resetkeys ~nomatch-*`, a refused command (in the category; with keys, from
`COMMAND GETKEYS`) must reply NOPERM or an error Valkey replies before it
checks permissions (`acl-bypass`), and no other NOPERM (`acl-overreach`);
scripts and `no_auth` commands (`HELLO`, which no rule refuses) are not
judged, and a sequence sending `ACL`, `AUTH`, or `HELLO ... AUTH` is skipped. `concurrency`: N connections run it at once, replies
unjudged; a command unanswered while the server answers no `PING` within
`timeout_s` is `hang` (a crash), and with `replica` the replica must catch
up. `compat`: a fresh parent runs it and `SAVE`s; its RDB (kept as
`parent.compat.rdb`) must load into a fresh candidate with the parent's
digest, compared after a reload as a replica, which keeps keys expired since
the save (`compat-load`), and a fresh candidate replicating the parent must
reach it (`compat-sync`). `faults`: the sequence runs again on servers that
persist (an RDB saved at shutdown, or the AOF, drawn from the run's seed), a
single standalone node with a replica, and before one to three commands,
also drawn, the check restarts a node or sends it `DEBUG RELOAD`; replies are
unjudged, but a node that does not come back is a crash, a failed `DEBUG
RELOAD` is `reload-failed`, a node whose `DEBUG DIGEST` differs afterwards is
`lost-data` (judged only when it holds no expiring key or hash field before
the fault and, once it expired what its AOF replayed, after it, its
digest holds still for a moment before the fault, and in a cluster it owns the
same slots before and after), and a
replica must reach its primary's digest after its restarts (its offset alone after a `DEBUG
RELOAD` or a restart of the replica while anything may expire, which drops
expired data the other node keeps) and at the end
(`replica-diverged`); a passing outcome lists the faults and the persistence; a sequence debugging a script with `SCRIPT DEBUG SYNC`,
which holds the server, is skipped, and so is one that sets `rdb-key-save-delay` or `key-load-delay` above 0
(in `server_config` or by `CONFIG SET`), as every save or load then takes the delay once per key, and one that
manages users or a password (`ACL`, `AUTH`, `HELLO ... AUTH`, or a `CONFIG SET` of `requirepass`,
`primaryauth`/`masterauth` or `primaryuser`/`masteruser`).
`acl` and
`concurrency` leave restarts out and do not wait (`wait_s`); `replica` and
`compat` skip several nodes and `cluster`, `compat` also a module loaded on
one build only. A check that could not run
is recorded as `error` and leaves the verdict alone.

Without `compare_with`, `expect` alone judges the sequence: `FAIL` on a
violated reply, `ERROR` on a command with no reply.

Refused when the scenario compiles, naming the command: `SHUTDOWN`; `DEBUG
RESTART`, `DEBUG CRASH-AND-RECOVER`, `DEBUG RELOAD`, and `DEBUG SLEEP` longer
than `timeout_s`; `MODULE LOAD`, `LOADEX`, `UNLOAD`; `CONFIG SET` of an option
the deployment manages; `MONITOR`; `SUBSCRIBE`, `PSUBSCRIBE`, and
`SSUBSCRIBE` on the main connection, and those and their unsubscribing
commands with other than one channel.
The private servers are disposable, so `DEBUG SEGFAULT` and friends are
allowed and simply crash the server they are sent to (a candidate crash is a
`FAIL`, which is what such a test means); `REPLICAOF`, `CLUSTER`, `SYNC`,
`PSYNC`, and `REPLCONF` are allowed on them too, and refused on the
deployment's instances.

Evidence: `valkey.command.sequence`, `{candidate: {replies: [{index, reply |
error | no_reply | closed | protocol_error and raw, unanswered?}], digest, exited?, exited_node?}, parent: {...} | null, modules:
{candidate: [paths], parent: [paths]} | null, cluster, nodes, dropped_config,
reached: [indexes], unexplained: [indexes], checks?}` (and
`instance_id` without `compare_with`, one item per instance). `checks` has
each enabled check's outcome by name: `{outcome: pass | skipped | error |
fail}`, with `reason` (skipped, error) or `summary` and `diagnostics` (fail),
and for a check of two parts each part's (`encodings` and `protocol`,
`category` and `keys`, `load` and `sync`). Every check server's logs are kept
beside the others (`candidate.encodings.server.log`). `digest` is the
SHA-256 of the canonical JSON of the compared replies. `exited` is the exit
status of a side's node that exited, and `exited_node` that node; any node of
the candidate that exits is a `FAIL`. Every private server's
`server.log` and `output.log` are copied into the run's component folder
before they are stopped (`candidate.server.log`, or `candidate.node1.server.log`
with several nodes), so a crash or sanitizer report survives, and are the
evidence's artifacts.

## `valkey.action.tools`

Runs Valkey's own tools (`valkey-cli`, `valkey-benchmark`,
`valkey-check-rdb`, `valkey-check-aof`) of two builds with the same
arguments, stdin, and files, and compares their exit status, stdout, and
stderr: a test of a change to a tool. Each build's tools are the ones beside
its `valkey-server`: the deployment's binary (the candidate) and
`compare_with` (the parent). The deployment's instances are never touched.

```yaml
- use: valkey.action.tools
  id: check-rdb
  config:
    compare_with: {$ref: artifacts.parent.binary}   # the parent commit's build
    files:
      empty-db2.rdb: !!binary UkVESVMwMDA5/gL/AAAAAAAAAAA=   # SELECTDB 2, then EOF
    ignore: ['^\d+:[A-Z] .*\n']                        # Valkey log lines: pid and time
    runs:
      - {tool: valkey-check-rdb, args: [empty-db2.rdb, --stats, --format, info],
         expect_difference: true, expect: {exit: {equals: 0}, stdout: {contains: "db.2.type.string.keys.total:0"}}}
- use: valkey.action.tools
  id: cli
  config:
    compare_with: {$ref: artifacts.parent.binary}
    server: true                                      # each side's own private server
    runs:
      - {tool: valkey-cli, args: [-p, $port, SET, k, v], expect: {stdout: {equals: "OK\n"}}}
      - {tool: valkey-cli, args: [-p, $port, PING], env: {VALKEYCLI_AUTH: secret}}
```

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `compare_with` | binary path | | The parent's `valkey-server`, usually `{$ref: artifacts.parent.binary}`; its tools sit beside it. A build without a tool a run names is `ERROR`. |
| `server` | boolean | `false` | Each side starts a private server of its own build, from an empty dataset, on a free loopback TCP port (`127.0.0.1`, and `::1` where the host has it) and a unix socket, with `enable-debug-command yes` and `sanitize-dump-payload yes`, and the side's working directory as its `dir`, so a `SAVE` writes `dump.rdb` where `valkey-check-rdb` reads it, and a `dump.rdb` in `files` is what the server loads. A run's arguments name it as `$port` and `$socket`. |
| `cluster` | boolean | `false` | Both private servers run as a one-node cluster serving every slot, for `valkey-cli --cluster`. Needs `server`. |
| `files` | mapping | `{}` | `{name: text or !!binary}`, written into each side's working directory before the first run. A name is a plain file name. |
| `ignore` | list of regular expressions | `[]` | Removed from stdout and stderr (`^` and `$` match at line ends) before the two sides compare, for what differs between processes: the pid and time of a Valkey log line, a benchmark's throughput. Recorded output keeps them. |
| `timeout_s` | number, 0.1 to 3600 | 30 | Per run: a tool still running then is killed and recorded `timed_out`; also how long a private server may take to start. A tool whose resident memory passes 4 GiB is killed too, and recorded `memory_exceeded`: on the build under test, a crash. |
| `runs` | list of mappings | | The runs, below, in order; each side runs them all. |

Each run has:

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `tool` | `valkey-cli`, `valkey-benchmark`, `valkey-check-rdb`, `valkey-check-aof` | | Run with its bare name as `argv[0]`, never through a shell, in the side's working directory. |
| `args` | list of strings or numbers | `[]` | `$port` and `$socket` (also inside an argument, as in `[::1]:$port`) become the side's own; they need `server`. An argument that could name a path outside the working directory (it starts with `/` or `~`, or has a `..` part) is refused. |
| `stdin` | text, `!!binary`, or null | `null` | What the tool reads; null is an empty stdin. |
| `env` | mapping | `{}` | Environment variables for this run. The fuzzer's own environment is passed on without `VALKEYCLI_*` and `REDISCLI_*`, with `HOME` the working directory. |
| `expect` | mapping or null | `null` | `{exit, stdout, stderr}`, each optional, each a [predicate](README.md#predicates): `exit` on the exit status (null when a signal ended the tool), `stdout` and `stderr` on the output as recorded. `null` accepts any run. |
| `expect_difference` | boolean | `false` | The change under test means this run to differ from the parent's. Needs `expect`. |
| `compare` | `equal`, `unordered`, `skip` | `equal` | How the two sides' runs compare: exit status, signal, `timed_out`, `memory_exceeded`, stdout, and stderr as recorded (after `ignore`); the same with stdout's and stderr's lines sorted; or recorded and never compared. |

Output is recorded with the side's port, socket path, and working directory
written back as `$port`, `$socket`, and `$dir`, so the two sides compare
equal; output that is not valid UTF-8 is recorded as `{$base64: ...}`. With
`cluster`, its private server's node ID, in full or as the `1a2b3c4d...`
valkey-cli abbreviates it to, is written back as `<node.0.id>`, as
[`valkey.action.commands`](#valkeyactioncommands) names it.

The outcome, as for [`valkey.action.commands`](#valkeyactioncommands):

- `FAIL`: a candidate tool that crashes (a signal ends it, or its output
  has a crash or sanitizer report) or a candidate private server that exits;
  their causes are `crashes` in the diagnostics. Also a candidate run that
  violates its `expect` where the parent ran otherwise.
- `ERROR` with `diagnostics: {reason: expect-disputed, pre_existing: true,
  runs: [...]}`: an `expect` the candidate violates but the parent ran the
  same.
- `ERROR` with `diagnostics: {reason: unreached-change}`: a marked run the
  same on both builds.
- `ERROR` with `diagnostics: {reason: unexplained-difference, runs: [...]}`:
  an unmarked run that differs, both sides given.
- A parent tool that crashes or times out where the candidate does not
  differs: reached on a marked run, unexplained on an unmarked one.
- `ERROR` for a candidate run that does not exit within `timeout_s`, a
  missing tool, or a private server that does not start.

Hints and other interactive features need a terminal; `valkey-cli
--test_hint` and `--test_hint_file` print a hint without one.

Evidence: `valkey.tool.runs`, `{candidate: {runs: [{index, tool, exit,
signal?, timed_out?, stdout, stderr}], digest, server_exited?}, parent:
{...}, server, reached: [indexes], unexplained: [indexes]}`. `digest` is the
SHA-256 of the canonical JSON of the compared runs. Each side's working
directory, with its files and what the tools wrote, stays in the run's
component folder as `candidate/` and `parent/`; with `server`, both private
servers' `server.log` and `output.log` are the evidence's artifacts.

## `valkey.action.bgsave`

Issues `BGSAVE` on the selected instances, follows every save until it
finishes or `timeout_s` passes, and compares each result with `expect`. A
save's status is `ok` (the RDB file was replaced), `err` (Valkey reported
`rdb_last_bgsave_status:err`), `not_completed` (it finished without replacing
the file, as a cancelled save does), `timeout`, `rejected` (Valkey replied
to `BGSAVE` with an error), `unanswered` (`BGSAVE` got no reply: a lost
connection or a client timeout), `unavailable` (`INFO persistence` could
not be read until `timeout_s`; a read that fails is tried again on the next
poll), or `busy`. While another child process runs (a replication RDB, an
AOF rewrite), Valkey refuses `BGSAVE` with `Background save already in
progress`; the action sends it again until `timeout_s`, and a save still
refused that way then is `busy`, not a rejection. `FAIL` when a save Valkey
reported does not reach `expect`; `ERROR` when, with an `expect` other than
`any`, a save is `unanswered`, `unavailable`, or `busy`: the harness did not
see what Valkey did, or never got a save started.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `instances` | selector | `primaries` | |
| `args` | list of strings | `[]` | Extra `BGSAVE` arguments. |
| `timeout_s` | number, 0.5 to 7200 | 120 | |
| `poll_interval_s` | number, 0.001 to 10 | 0.02 | |
| `expect` | `ok`, `err`, `not_completed`, or `any` | `ok` | The status every save must reach. |
| `capture_rdb` | boolean | `true` | Copy each `ok` save's RDB into the component's artifact folder, for `valkey.oracle.rdb-snapshot`. |

Evidence: `valkey.bgsave.result` per instance (status, observed save types,
persistence fields before and after, the captured RDB, the instance's `role`
(`primary` or `replica`), `refusals` (how many `BGSAVE`s Valkey refused while
another child ran, before the one it accepted),
and `sent_at`, `replied_at`, and `finished_at`: when `BGSAVE` was sent, when
its reply arrived, and when the save was seen finished). Events:
`<id>.save-started` once the commands were sent (faults that must hit a save
in progress trigger on it), `<id>.save-completed` once every save finished or
`timeout_s` passed.

## `valkey.action.failover`

Cluster mode. Promotes a replica of one shard's current primary with
`CLUSTER FAILOVER`, or `CLUSTER FAILOVER FORCE`, and waits for the cluster to
settle. The replica is the first, in an order drawn from the component's
seeded random stream, that has synced with the primary (link up, no sync in
progress, offset past 0); the failover waits up to `timeout_s` for one,
then gives Valkey another `timeout_s` from the command to settle the shard.
A primary the cluster flagged failed (`fail`, not one node's `fail?`), or one
that stops answering before a replica synced, is left to Valkey's own
failover, which must settle within `timeout_s`. It is `ERROR` when the
failover could not be observed: no node answered, the shard has no replica
or none synced in time, a command failed, or the shard could not be read at
the end of the wait.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `shard` | integer or null | `null` | The shard to fail over, below `primaries`; shard 0 when neither field is set. |
| `target` | selector or null | `null` | An instance whose planned shard to fail over. Set `shard` or `target`, not both. |
| `force` | boolean | `false` | Use `CLUSTER FAILOVER FORCE`. |
| `timeout_s` | number, 1 to 600 | 60 | |

Evidence: `valkey.failover.result` (target, force, success, roles after,
`primary_before`, `started_at`, and `completed_at`). With `force`, the
failover is a loss window for its old primary (see
[`valkey.oracle.ledger`](oracles.md#valkeyoracleledger)).

## `valkey.action.sleep`

Waits, for example so a workload runs for a while before a fault triggers on
`<id>.completed`. A deadline or cancellation cuts the wait short.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `seconds` | number, 0 to 3600 | | |

## `valkey.action.poll`

Polls an INFO field or a command reply on every selected instance until the
predicate holds on all of them.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `instances` | selector | `all` | |
| `section`, `field` | string or null | `null` | The INFO section and field to read. |
| `command` | list or null | `null` | A command whose reply to read. Set `section` and `field`, or `command`. |
| `predicate` | mapping | | A [predicate](README.md#predicates). |
| `timeout_s` | number, 0.1 to 7200 | 60 | |
| `poll_interval_s` | number, 0.001 to 60 | 0.1 | |

`FAIL` on timeout when an instance was read and does not satisfy the
predicate; `ERROR` when every instance that does not was unreadable at its
last read, since nothing was seen. Evidence: `valkey.poll.satisfied` (the
last values).

## `valkey.action.wait-event`

Waits until an event has been dispatched `min_count` times.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `event` | string | | For example `writes.drained`. |
| `min_count` | integer, 1 to 1,000,000 | 1 | |
| `timeout_s` | number, 0.01 to 7200 | 120 | |
| `poll_interval_s` | number, 0.001 to 10 | 0.02 | |
| `only_after` | string or null | `null` | Wait only if this event was already dispatched; otherwise pass at once. |

`ERROR` on timeout: an event that did not come in time says nothing about the
server. Evidence: `valkey.wait.event-observed` (event and count).

## `valkey.action.rolling-upgrade`

Moves every instance to `to_binary`, one shard at a time: each replica
restarts on the new build and resynchronizes, the shard fails over gracefully
to the first upgraded replica (`CLUSTER FAILOVER` in cluster mode; in
standalone mode `FAILOVER TO` with `step_timeout_s` as its timeout, aborted
with `FAILOVER ABORT` if it does not complete, after which the other replicas
follow the new primary), and the former primary restarts on the new build. Without replicas,
or with `failover: false`, the primary restarts in place, with downtime. In
standalone mode it needs `replicas_per_primary` of at least 1. Instances
already on the target are skipped. `FAIL` when Valkey is seen doing something
wrong (an instance that does not exit or does not resynchronize, or a
failover that does not complete, each while it answers); `ERROR` when the
upgrade cannot run or the harness cannot tell what Valkey did (a shard
without one live primary, an instance that restarts on another version than
the harness put in place, a client error, a `FAILOVER TO` that got no reply
and roles that did not swap, roles that could not be read at the end of a
wait, a cancelled run).

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `to_binary` | string or reference | | Usually `{$ref: inputs.to_binary}` fed by an artifact. |
| `failover` | boolean | `true` | Fail over to an upgraded replica before restarting a primary. |
| `settle_s` | number, 0 to 600 | 1 | Pause after each instance. |
| `step_timeout_s` | number, 1 to 3600 | 60 | Limit for each exit, resync, and failover. Until an instance exits it gets `SIGTERM` again every second: Valkey drops one it cannot act on yet, such as while it writes its initial AOF. |

Evidence: `valkey.upgrade.completed` (target version and every step). Events:
`<id>.instance-upgraded` per instance, `<id>.failed-over` per failover.

## `valkey.action.cluster-chaos`

Cluster mode, at least one replica per primary. Runs `waves` of `failover`
and `kill` operations on the [wave engine](../architecture.md#the-wave-engine)
(operations, timing, evidence, and verdicts as for
[`valkey.action.replication-chaos`](#valkeyactionreplication-chaos)); Valkey
heals the cluster, and the action validates it after every wave with the
killed instances counted as stopped on purpose. How targets are chosen and
when a failed failover is tolerated is in
[Kill safety and verdicts](../cluster-chaos-and-ci.md#kill-safety-and-verdicts).

| Operation | Fields (defaults) | What it does |
| --- | --- | --- |
| `failover` | `shard` (required, below `primaries`), `force` (`false`), `timeout_s` (30) | `CLUSTER FAILOVER` (or `FORCE`) on a replica of the shard's current primary that has synced with it (as for [`valkey.action.failover`](#valkeyactionfailover)), never one killed on purpose; done once the shard has a new primary that its live replicas follow. At most one per shard per wave. |
| `kill` | `target` (`replica`; `primary`, `any`, an instance ID, or a list of IDs), `signal` (`SIGKILL`) | As in replication chaos. Kills are never restarted. |

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `waves` | list of lists | | The waves. The experiment's default is one failover of shard 0. |
| `validation` | mapping | `{}` | The [state validation](../cluster-chaos-and-ci.md#state-validation) settings. A critical failure, or any with `blocking_on_failure`, stops the waves. |
| `keys_per_slot` | integer, 0 to 100 | 5 | Keys written into every hash slot before the first wave. |

## `valkey.action.cluster-election`

Cluster mode. Forces one failover election in a shard and waits for a stable
winner.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `shard` | integer | 0 | The shard, below `primaries`. |
| `trigger` | `force_failover`, `kill_primary`, or `pause_primary` | `force_failover` | `force_failover` sends `CLUSTER FAILOVER FORCE` to every candidate at once, so they compete in one epoch and the old primary votes; `kill_primary` and `pause_primary` first wait, up to `timeout_s`, until every candidate replicates the primary with its link up (Valkey's failure detection elects only a replica whose data is recent, which one restarted moments ago and still in its first sync is not; `ERROR` if they do not link), then send `SIGKILL` or `SIGSTOP` to the primary and let failure detection start the election. |
| `candidates` | `all` or `one` | `all` | Replicas that compete. |
| `attempts` | integer, 1 to 20 | 3 | `force_failover` attempts. |
| `timeout_s` | number, 1 to 600 | 30 | |
| `settle_s` | number, 0 to 60 | 1 | How long the winner must stay the shard's only primary. |
| `pause_s` | number, 0.1 to 600 | 10 | Longest `SIGSTOP` for `pause_primary`; it ends once a winner is stable, which must happen within it. An election that loses a vote retries 4 s later, so `valkey.cluster-election-durability`, whose vote killer takes votes, sets 30. |
| `restart_killed` | boolean | `true` | Restart the primary `kill_primary` killed. |
| `wait_synced` | boolean | `true` | Wait until the shard's other instances replicate from the winner. |

Before the election it reads the shard until exactly one primary owns its
slots, up to `timeout_s`: an old primary restarted from its `nodes.conf`
claims its old slots until gossip corrects it. `FAIL` when no replica wins
after every running candidate answered; `ERROR` when the shard still has
another number of slot owners then (no election started, so the oracles
judge the shard), a node did not answer before the election or at its end,
the shard has no live replica, or the trigger never took effect (no
candidate accepted `CLUSTER FAILOVER FORCE`, or the killed primary did not
exit). Evidence: `valkey.cluster.election.result`, and for
`kill_primary` `valkey.fault.injected` (the kill, with the `nodes.conf` vars
read once the process exited, as the restart loads them), so `valkey.oracle.cluster-election-safety` can list the
action in `crash_faults`. Events: `<id>.election-started`,
`<id>.election-completed` (with the winner and its `configEpoch`), and for
`kill_primary` `<id>.injected`, which `valkey-fuzzer spec conform` reads as a
crash.

## `valkey.action.migrate-slots`

Cluster mode. Moves one hash slot, the slot of `key`, from the primary that
owns it to the primary of shard `shard` with `CLUSTER MIGRATESLOTS SLOTSRANGE
<slot> <slot> NODE <target>`, and waits until the source's export job
(`CLUSTER GETSLOTMIGRATIONS`) ends: `success`, `failed`, or `cancelled`. A
hash tag as the key (`{s}`, slot 3828) names the slot every key carrying the
tag is in, so a `valkey.workload.key-value` with that `prefix` writes to the
slot that moves. `valkey.slot-migration-spec` kills the target as it claims
the slot; the oracles judge the data and the cluster state, this action only
that the migration ran to an end.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `key` | string | | The key whose slot moves. |
| `shard` | integer | 1 | The shard whose primary imports the slot, below `primaries`. |
| `timeout_s` | number, 1 to 3600 | 60 | How long the export may take to end. |
| `poll_interval_s` | number, 0.01 to 10 | 0.05 | |

`PASS` once the export ended, whatever its state; `ERROR` when the shard has
no primary that answers, the target already owns the slot, the source refused
`CLUSTER MIGRATESLOTS`, or the export did not end within `timeout_s`: the run
never reached what it tests. Evidence: `valkey.cluster.slot-migration.result`
(the slot, `source` and `target`, their node IDs, the reply, the export job
as the source listed it last, its `state` and `message`). Events:
`<id>.migration-started` once the source accepted the command, and
`<id>.migration-completed` (with the `state`, and `ended`) once the export
ended or `timeout_s` passed.

## `valkey.action.slot-migration-chaos`

Cluster mode, at least one replica per primary, Valkey 9.0 or later (atomic
slot migration). The slot-migration mode of the
[wave engine](../architecture.md#the-wave-engine): each wave moves one hash slot
with `CLUSTER MIGRATESLOTS` and disturbs the migration (operations, timing,
evidence, and verdicts as for
[`valkey.action.replication-chaos`](#valkeyactionreplication-chaos); kill
targets and safety as for [`valkey.action.cluster-chaos`](#valkeyactioncluster-chaos)).
Before the first wave it writes `keys_per_slot` keys carrying each migrated
key's hash tag, so they are in its slot whoever owns it. After every wave it
heals (removes the partitions, restarts the killed instances, which rejoin
their shard from their `nodes.conf`) and validates: the deployment's state
checks (the cluster converges, every slot has one owner in every view, roles,
replication, the check's own keys), then the slot keys, every one of which
must read back through a slot-routing client with the value written
(`slot_keys` in the wave's validation; keys lost in a wave are written again
for the next). A migration that did not succeed (`failed`, `cancelled`, or
not ended within its `timeout_s`) is tolerated only when a kill, partition
or cancel of its wave landed before it ended and touched one of its
endpoints; otherwise it fails the wave. Faults that must land on a log line
are `valkey.fault.process-signal` and `valkey.fault.network-partition`
triggered on the `slot-migration.*` events of
[`valkey.observer.log-trigger`](observers.md#valkeyobserverlog-trigger) with `spec:
slot-migration` (the bundle's `started`, `paused`, `failover.requested`,
`finished`, `cancelled` lines; see
[`formal/slot-migration/mapping.yaml`](../../formal/slot-migration/mapping.yaml)).

| Operation | Fields (defaults) | What it does |
| --- | --- | --- |
| `migrate` | `key` (required), `shard` (null), `timeout_s` (60) | `CLUSTER MIGRATESLOTS` of `key`'s slot from the primary that owns it to shard `shard`'s primary, or with `shard` null to the shard after the owner's planned one (so a slot migrated wave after wave goes round the shards), waited for until the source's export ends, as [`valkey.action.migrate-slots`](#valkeyactionmigrate-slots) does; when the source was killed, until the target's import ends. One per wave; its record's `migration` holds the slot, `source`, `target`, their node IDs, the final `state` and both jobs' last states. While it runs it reads both endpoints' `CLUSTER GETSLOTMIGRATIONS` every 10 ms for the migration's phase, which the wave's other operations wait for; an experiment that publishes the bundle's events (`slot-migration.paused`, `.failover.requested`, ...) shows a phase on the line too. |
| `kill` | `target` (`replica`; `source`, `target`, `primary`, `any`, an instance ID, or a list of IDs), `signal` (`SIGKILL`), `at` (null), `phase_timeout_s` (60) | As in cluster chaos, under its kill safety (a shard's last live member and the primaries' quorum are never taken), with `source` and `target` the wave's migration endpoints, picked once the migration started. With `at`, waits for the migration to reach that phase first. Killed instances are restarted by the heal. |
| `partition` | `target` (`replica`; as `kill`'s, one instance), `peers` (`others`; `source`, `target`, or instance IDs), `duration_s` (1), `at`, `phase_timeout_s` | Drops the traffic between the target and its peers for `duration_s` (Linux, root, iptables, `addressing: per_instance_loopback`); `{target: source, peers: target}` cuts the migration connection and the cluster bus between the endpoints. Undone by the operation, or by the heal. Publishes `<id>.partitioned`. |
| `cancel` | `at`, `phase_timeout_s` | `CLUSTER CANCELSLOTMIGRATIONS` on the source. One sent after the migration ended gets `No migrations ongoing`, recorded in the operation's `reply` and not a success. |
| `restart` | `target` (`killed`; or an instance ID) | Restarts an instance killed earlier in the wave; the heal restarts whatever is still down. |

The phases (`at`): `started` once the source accepted the command;
`snapshot` while the export snapshots or streams (export states
`snapshotting`, `replicating`, `waiting-to-pause`; import
`receiving-snapshot`, `waiting-for-paused`); `handoff` from the source's
pause to the target's claim (`failover-paused`, `failover-granted`;
`failover-requested`); `finished` once either job ended. A phased
operation's record carries `phase`: the phase `wanted`, the one `seen` when
it went ahead, `missed` when the migration passed or ended before the phase,
and for a kill `landed_in`, the phase at the signal. A handoff lasts a few
milliseconds on one host, so a kill `at: handoff` often lands `finished`,
just after the claim; `valkey.slot-migration-spec` kills the target on the
claim's log line itself.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `waves` | list of lists | | The waves. The experiment's default: the target killed at the handoff; the source killed as it snapshots and restarted; a cancel mid-way; an undisturbed migration. |
| `validation` | mapping | `{}` | The [state validation](../cluster-chaos-and-ci.md#state-validation) settings. A critical failure, or any with `blocking_on_failure`, stops the waves. |
| `keys_per_slot` | integer, 0 to 1000 | 20 | Keys written into every migrated slot before the first wave. |
| `heal_timeout_s` | number, 1 to 3600 | 60 | How long a restarted instance may take to be ready. |

Events: `<id>.migration-started` and `<id>.migration-completed` (with the
wave, slot, endpoints, and the final `state`), `<id>.endpoint-killed` (a
kill of the migration's `source` or `target`, which the slot-migration
bundle's mapping reads as the model's `CrashT` when it is the target),
`<id>.partitioned`, and the engine's `<id>.instance-killed` and
`<id>.wave-validated`. Evidence: the
engine's `valkey.chaos.schedule` (each wave summary has the wave's
`migration` record; the schedule has `slots` and `slot_keys`), `valkey.chaos.test-keys`,
and `valkey.instances.expected-down` (empty, every kill restarts), which
[`valkey.oracle.deployment-state`](oracles.md#valkeyoracledeployment-state) reads.

## `valkey.action.replication-chaos`

Standalone, at least one replica. Runs `waves` on the
[wave engine](../architecture.md#the-wave-engine); each wave is a list of
operations that start together, each after its own `delay_s` (0 to 600,
default 0), or, with `after: <i>`, once operation `i` of the wave (an
earlier one) finished and then its `delay_s`. Targets are `primary`,
`replica`, `any`, or an instance ID, and resolve against the roles at the
start of the wave; each operation draws from its own random stream,
`wave-<n>-op-<i>`.

| Operation | Fields (defaults) | What it does |
| --- | --- | --- |
| `kill` | `target` (`replica`, or a list of instance IDs: one of them), `signal` (`SIGKILL` or `SIGTERM`; `SIGKILL`) | Signals one instance and waits up to 30 seconds for it to exit, sending the signal again every second (Valkey drops a `SIGTERM` it cannot act on yet); one that never exits fails the wave after `SIGTERM`, and is an `ERROR` after `SIGKILL`, which Valkey cannot refuse. |
| `restart` | `target` (`killed`, or an instance ID) | Restarts an instance killed earlier in the same wave as a replica of the current primary. Needs a `kill` of the wave that surely starts before it: one it waits for (`after`, directly or through others), or one without `after` whose `delay_s` is below the restart's earliest start; skipped while the primary is down. |
| `promote` | `target` (`replica`), `mode` (`graceful` or `force`; `graceful`), `split_s` (0 to 60; 0), `timeout_s` (10) | `graceful`: `FAILOVER TO` on the primary, then the other replicas follow the new primary; it succeeded when the target leads, or when the old primary follows the target with its failover state cleared (a target killed right after taking over was still promoted). A `FAILOVER TO` that got no reply, with roles that did not swap, or roles that could not be read at the end, is an `ERROR`: Valkey may never have seen it. `force`: `REPLICAOF NO ONE` on the replica; the old primary keeps serving for `split_s`, then it and every other replica follow the new primary. Refused when the target is killed, stalled, partitioned, or (for `graceful`) not linked, and for `force` when it was restarted in the wave and has not resynced: it holds only what it loaded from disk, so writes no loss window covers (from earlier terms, or WAIT-confirmed) could be lost. |
| `repoint` | `target` (`replica`), `to` (`replica`, `primary`, or an ID; `replica`) | `REPLICAOF` from one replica to another (a chain) or to the primary. `to: replica` needs two replicas. |
| `partition` | `target` (`replica`), `duration_s` (1) | Drops traffic between the instance and every other instance (iptables), then heals. Needs `addressing: per_instance_loopback`; on a host without Linux, root, and iptables the action is `ERROR`. |
| `degrade` | `target` (`replica`), `duration_s` (1), `loss_pct`, `delay_ms`, `jitter_ms`, `reorder_pct`, `duplicate_pct` (all 0), `rate_kbit` (`null`) | Degrades the traffic between the instance and every other instance with `tc` netem instead of dropping it, as [`valkey.fault.network-degrade`](faults.md#valkeyfaultnetwork-degrade) does (set at least one effect; `jitter_ms` and `reorder_pct` need `delay_ms`), then heals. Needs `addressing: per_instance_loopback`; on a host without Linux, `tc`, and `CAP_NET_ADMIN` the action is `ERROR`. A degraded instance is still a promotion target: its links are slow, not cut. |
| `stall` | `target` (`replica`), `duration_s` (1) | `SIGSTOP`, then `SIGCONT`. |
| `backlog_pressure` | `repl_backlog_size` (16384), `output_limit` (65536 bytes), `duration_s` (1) | On the current primary: shrinks `repl-backlog-size` and the replica class of `client-output-buffer-limit`, then restores both. |

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `waves` | list of lists | | The waves, each a non-empty list of operations. |
| `validation` | mapping | `{}` | The [state validation](../cluster-chaos-and-ci.md#state-validation) settings, as for `valkey.action.cluster-chaos`. |
| `heal_timeout_s` | number, 1 to 3600 | 60 | How long the heal waits for every replica to link to the primary. |

Kill safety (the standalone policy of the
[wave engine](../architecture.md#the-wave-engine)): a kill never takes the last
instance that is not down, so one copy of the latest synced data always
survives; promotions and repoints pick their targets after the wave's kills,
stalls, partitions, and degradations, and avoid the replicas those disrupt.

Ownership: every change the action makes and will undo is an
[undo obligation](../contracts.md#fault-ownership) from the moment it
applies it: a stall (`process.paused`), a partition (`network.partition`),
a degradation (`network.degrade`, its netem device),
each instance a `backlog_pressure` changes
(`server.config`), a kill, which the heal or a `restart` undoes
(`process.stopped`), and the validation's `CLIENT PAUSE`
(`server.client-pause`). An undo that fails leaves its obligation open for
the heal, the final cleanup, and then the runner to try again, and the run
is `ERROR` even if a retry succeeds.

Overlapping faults: two held faults of a wave on the same state (a stall and
a config change of one instance, two config changes of one key) run one
after the other, and a kill waits for the held faults on its target to be
undone, so no undo restores a value another fault saved or meets a process
that is going away. A fault whose state is held by one whose undo failed is
skipped.

After every wave the action heals: it undoes stalls, then partitions and
degradations, then
config changes (an undo that fails stays owned by its fault and is tried
again by the next heal and by the final cleanup); if the primary is down it promotes the live instance with
the highest offset in the dead primary's replication history
(`REPLICAOF NO ONE`) and points the others at it; it restarts every killed
instance as a replica of the primary (`replicate`, then `restart`); and it
points any instance it had told to follow someone else at the primary. An
instance told to follow the primary that does not is left alone, so
validation reports it. Then, with writes paused (`CLIENT PAUSE WRITE`), it
waits for every replica's offset to equal the primary's and validates: one
primary, every replica linked to it, no offset difference, and equal test
keys.

Loss windows: during a wave the action samples the primary's replication
offset every 0.1 seconds (and right before killing it). A forced promotion,
and the promotion that heals a dead primary, publishes
`valkey.data.loss-window` evidence `{start, end, instance_id, reason}`:
`instance_id` is the old primary; `start` is the latest sample whose offset
the new primary had reached, read before it was promoted, in the same
replication history (the old primary's term start if none); `end` is when
the old primary stopped acknowledging writes: its confirmed exit, or the
`REPLICAOF` that demoted it. A graceful promotion publishes no window.

Verdict (both chaos actions): `FAIL` when a wave's validation fails, an
instance exits without being killed, an instance does not exit after
`SIGTERM` (after `SIGKILL` it is an `ERROR`), or
Valkey refuses or does not complete a promotion, repoint, restart, or
failover with no interfering operation (replication chaos: an earlier,
landed operation of the wave that acted on an instance it involves; cluster
chaos: a kill in the failover's shard, or of a primary in the same wave).
`ERROR` when an operation had no target at all, a fault could not be
injected, an operation raised with no interfering operation (the harness
did not see what Valkey did with it: no reply, a target that did not answer,
a topology it could not read), a wave ran no operation, or the harness could
not undo a fault (even one a later heal undid), heal the deployment,
validate it, or clean up at the end; a held fault that raised is undone at
once; a wave
whose heal could not complete stops the run. `SKIP` when faults were planned
and the safety rules refused every one. A `FAIL` summary lists the `ERROR`
problems too. Replication chaos stops after the first failed wave.

Evidence: `valkey.chaos.schedule` (`operations`, each with its `wave`,
`op`, `shard`, `target`, `config`, `success`, `applied_at`, and `refused`,
`skipped`, `missing`, `error`, `undo_error`, `undone_at`/`undone_by`,
or `excused_by`; `kills`, the kills whose instance is still down; `all_kills`,
every kill tried; `waves`, each with its `planned`, `applied`, `healed`
(landed, and every change it owned is undone: a held fault, or a kill the
heal restarted), `still_active` (a change it owns is still in place: its
obligation is open), and `skipped` operations, heal problems, validation,
and, for cluster chaos, `views` (the cluster view reads of the wave: `reads`, the `last`
read, and the `disputed` ones, each with the nodes that `answered`, those that `agreed`, the
`skipped` ones, and the winning `view` of primaries and config epochs) and, for replication chaos, its primary; `still_active` and
`cleanup_problems` after the final cleanup, which also restarts the
instances replication chaos killed; `heal_promotions` and `loss_windows`), `valkey.instances.expected-down`
(still down), `valkey.chaos.test-keys`, and one `valkey.data.loss-window`
per window. Events: `<id>.instance-killed`, `<id>.promoted` (`from`, `to`,
`mode`), `<id>.wave-validated`.
