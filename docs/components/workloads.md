# Workloads

Part of the [Valkey component reference](README.md): the
[index](README.md#index) lists every component, [Common config
forms](README.md#common-config-forms) the selectors, predicates, and trigger
fields the tables below use.

## `valkey.workload.key-value`

Preloads `stable_keys`, which never change afterwards, and `mutable_keys`,
then mutates the mutable keys at `rate` operations per second until teardown,
from one writer thread. For every operation it records what the client
observed in a ledger: `ok` (acknowledged, with the `instance_id` that
acknowledged it), `error` (an error reply such as `OOM` or `READONLY`, or no
reachable server, so never applied), or `ambiguous` (a timeout or connection error, which may or
may not have applied). Oracles derive the acceptable final values from that
ledger: each entry is invoked at `t_start` and completes at `t_end` on
connection `process` (numbered from 1; the writer opens a new connection after
any failure), and a `get` entry's `reply` is the value it read. The preload counts as acknowledged writes too: each preloaded key
records when its batch was acknowledged and by which instance, so a key
preloaded inside a loss window may be lost like any other write. In cluster
mode it writes through a cluster client that makes one
attempt per command and still follows `MOVED`, `ASK`, and `TRYAGAIN`; in
standalone mode it writes to the current primary. Keys are named
`<prefix>:stable:<n>` and `<prefix>:m:<n>`.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `stable_keys` | integer, 0 to 5,000,000 | 0 | Keys written once in the preload. |
| `mutable_keys` | integer, 0 to 1,000,000 | 100 | Keys the writer mutates. At least one of the two counts must be above 0. |
| `preload_mutable` | boolean | `true` | Also write an initial value for every mutable key in the preload. |
| `rate` | number, 0 to 100,000 | 200 | Operations per second; 0 writes the preload only. |
| `value_size` | integer, 1 to 1,048,576 | 64 | Value size in bytes. |
| `compressible` | boolean | `false` | Repetitive values instead of random ones. |
| `mix` | mapping | `{set: 0.7, del: 0.1, get: 0.2}` | Weights of `set`, `del`, `get`, `rename`, `lrange`, and `lindex`; one left out never runs. `rename` is standalone only (a cluster answers `CROSSSLOT`) and needs 2 mutable keys; `lrange` and `lindex` need `list_keys`. |
| `list_keys` | integer, 0 to 1000 | 0 | Lists preloaded beside the keys, named `<prefix>:list:<n>` and never mutated; `lrange` and `lindex` read them. |
| `list_length` | integer, 1 to 10,000,000 | 100000 | Elements per list. |
| `list_element_size` | integer, 1 to 65,536 | 120 | Element size in bytes. Elements repeat a tag, so quicklist nodes compress under `list-compress-depth`. |
| `list_read_batch` | integer, 1 to 1000 | 1 | Reads pipelined per `lrange` or `lindex` operation. |
| `prefix` | string or null | `null` | Key prefix; `vf:<id>` by default. |
| `op_timeout_s` | number, 0.05 to 60 | 2 | Client timeout per operation. |
| `preload_batch` | integer, 1 to 100,000 | 1000 | Keys per preload batch. |
| `max_ops` | integer or null | `null` | Stop after this many operations and publish `<id>.drained`. |
| `drain_on_stop` | boolean | `false` | At teardown, wait until `max_ops` operations completed; needs `max_ops`, `rate` above 0, and `mutable_keys` above 0. |
| `drain_timeout_s` | number, 0.1 to 7200 | 120 | How long `drain_on_stop` waits. |
| `wait_replicas` | integer, 0 to 100 | 0 | Standalone, at most `replicas_per_primary`: follow every acknowledged `SET` or `DEL` with `WAIT wait_replicas wait_timeout_ms`, recorded in its ledger entry as `wait` (or `wait_error`, which leaves the write acknowledged). |
| `wait_timeout_ms` | integer, 1 to 60,000 | 100 | |

The `valkey.writes` fragment sets `stable_keys`, `mutable_keys`, `rate`, and
`value_size` from its inputs (defaults 1000, 200, 200, and 64).

A `rename` operation is `RENAME src dst` between two mutable keys of one
name length whose values the workload knows for certain (their last
operation was an acknowledged `SET`, or the preload), so `dst` exists. The
ledger records it as two entries with one outcome, `id`, `t_end`, and
`reply`, both carrying `rename: [src, dst]`: a `del` of `src` and a `set` of
`dst` with the value `src` held, which is what the oracles' model needs. A
rename whose source or destination is not known for certain (its last write
was ambiguous, or it was deleted) runs as a `SET` instead. Renames count as
two operations toward `max_ops`.

`lrange` reads 30 elements from a random index of a random list, and
`lindex` one element; each operation pipelines `list_read_batch` such reads.
List reads are not ledger entries: the summary counts them (`list_reads`,
`ok` and `failed`), and `lists.jsonl` describes each list (`key`, `length`,
and the `digest` of its elements in order), which `valkey.oracle.rdb-snapshot`
checks against the reloaded RDB.

Evidence: `valkey.workload.ledger`, with the operation counts by outcome,
`ledger_path` and `preload_path` (JSONL artifacts; each preload entry holds
`t_end`, its acknowledgement time, and `instance_id`), `lists_path` (null
without lists), `list_keys`, `list_reads`, `replies_digest` (a digest of every
client-visible reply), and `drained`. Events: `<id>.seeded` after the preload,
`<id>.drained` after `max_ops` operations.

## `valkey.workload.command-table`

Sends commands drawn from the whole command table of the build under test,
from one thread at `rate` commands per second until teardown, so faults meet
every data type, transactions, scripting, Pub/Sub, and introspection, not
only `SET` and `GET`. At start it reads `COMMAND` (each command's flags) and
`COMMAND DOCS` (its arguments), which Valkey builds from its
`src/commands/*.json` specs, module commands included; a container command
(`CONFIG`) contributes its subcommands (`CONFIG GET`). Each command is drawn
uniformly from the table, and its arguments from its documented ones: an
optional argument half the time, one to three of a repeated one, as many keys
as its `numkeys`, and values from small domains (`member:<n>`, `field:<n>`)
so commands meet the data others wrote. A blocking command waits at most
0.1 s. `MULTI` runs as a transaction: `MULTI`, one to three commands, `EXEC`.
Each command, or transaction, goes over RESP2 or RESP3, on a connection of
each to its instance. Commands, arguments, and protocols come from the run's
seed (`unix-time` arguments are the current time plus up to a minute).

Error replies are expected and only counted; no reply (a lost connection or
a timeout) drops the connection, and in standalone mode the workload sends
to the current primary. It judges nothing: the liveness and crash oracles
judge what the commands do to the server, the evidence records any reply
the client could not parse, and
[`valkey.oracle.command-metadata`](oracles.md#valkeyoraclecommand-metadata) sends the
same commands again to private servers and holds each to its metadata.

Every other reply is held to the reply schema of its command, and the
evidence records each command and protocol with a reply that does not match,
for [`valkey.oracle.reply-schema`](oracles.md#valkeyoraclereply-schema) to judge. The
schemas are the `reply_schema` of Valkey's `src/commands/*.json`, which
Valkey holds its own test suite to (`utils/req-res-log-validator.py`, over
RESP3). An ordinary build does not report them, so they ship with the
fuzzer, in `valkey_fuzzer/components/reply_schemas.json`, which names the
Valkey commit they were read from; `python -m
valkey_fuzzer.components.reply_schemas <valkey source tree> "<its commit>"`
writes the file again from a newer tree. A command the file does not have (a
module's, or one newer than the file) is not held to anything. A build that
replies in a shape Valkey has since changed does not match the newer schema:
of the releases since 7.2, `LATENCY LATEST` and `MEMORY STATS` are such.

- An error reply has no schema, and is not judged. Inside a transaction a
  command is answered `QUEUED`; its reply is its item of the `EXEC`'s reply,
  which is held to the command's schema unless it is an error.
- A schema describes the RESP3 reply. A RESP2 reply matches when it is that
  reply in the form RESP2 has for it: a double as a string, a map as a flat
  array of its keys and values (or, for `XREAD` and `XREADGROUP`, as an array
  of `[key, value]` pairs), and an array of pairs as one flat array
  (`WITHSCORES`, `WITHVALUES`); and it may match more than one alternative
  of a `oneOf`, where a RESP3 reply must match exactly one.
- An integer is an integer reply: a RESP3 double does not match `integer`,
  even one with no fraction.
- A reply Valkey is known to send against its schema is taken as matching
  until the schema describes it, so it does not fail every run:
  `KNOWN_GAPS` in `command_table.py` lists each (`GEOHASH`'s null for a
  member the key does not hold; `OBJECT FREQ`, `OBJECT IDLETIME`, and
  `OBJECT REFCOUNT`'s null for a key that does not exist).

Every key is `<prefix>:{<tag>}:<type>:<n>`, with one of 32 hash tags per
command and `<type>` the command's group (a random one for generic
commands), and a pattern argument is `<prefix>:*`. The prefix keeps the
keys apart from the other components' keys and oracles; the one hash tag
keeps a command's keys in one slot, so a cluster runs multi-key commands. In
cluster mode a command goes to the primary serving its tag, learned from
`MOVED` replies.

Commands it never sends:

- Admin commands (the `admin` flag), which change the server, its
  replication, or the cluster's topology that the experiment's components
  drive and judge (`SHUTDOWN`, `REPLICAOF`, `FAILOVER`, `CLUSTER RESET`,
  `CLUSTER SETSLOT`, `CONFIG SET`, `DEBUG`, `BGSAVE`, `CLIENT KILL`,
  `CLIENT PAUSE`, `ACL SETUSER`, `MODULE LOAD`, ...), except those that only
  read: `ACL DRYRUN`, `ACL GETUSER`, `ACL LIST`, `ACL LOG`, `ACL USERS`,
  `CLIENT LIST`, `CLUSTER COUNT-FAILURE-REPORTS`, `CLUSTER REPLICAS`,
  `CLUSTER SLAVES`, `COMMANDLOG GET`, `COMMANDLOG LEN`, `CONFIG GET`, the
  `LATENCY` reports, `MODULE LIST`, `SLOWLOG GET`, and `SLOWLOG LEN`.
- `FLUSHALL`, `FLUSHDB`, and `SWAPDB`: they delete or move every
  component's keys.
- `KEYS`: it walks every key, the other workloads' too, blocking the server
  as long.
- `MIGRATE`: it blocks the server while it connects to the host it names,
  and moves keys out of the deployment.
- `SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE`, `HELLO`, and `CLIENT REPLY`: they
  change what the connection replies, so replies would no longer pair with
  commands.
- `SCRIPT DEBUG`: `SYNC` blocks the server for the whole debugging session.

After `RESET` (the connection is back in RESP2), `CLIENT TRACKING` (the
server pushes invalidations between replies), and `UNSUBSCRIBE`,
`PUNSUBSCRIBE`, and `SUNSUBSCRIBE` (answered once per channel), replies no
longer pair with commands in the connection's protocol: once one is answered
the workload closes the connection, and sends nothing more of its
transaction.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `rate` | number, 0 to 100,000 | 100 | Commands per second; 0 sends none. |
| `keys` | integer, 1 to 10,000 | 5 | Keys per hash tag and type. |
| `prefix` | string or null | `null` | Key prefix; `vf:<id>` by default. |
| `op_timeout_s` | number, 0.05 to 60 | 1 | Client timeout per command. |

The `valkey.command-table` fragment adds it as `commands`, with `rate` from
the input `command_rate` (default 0).

Evidence: `valkey.workload.command-table`, with `commands` (how many the
table offered), `skipped` (the names it never sends, as this build lists
them), `sent`, `distinct_sent`, `outcomes` (`replied`, `error`,
`unanswered`, `protocol_error`), `protocol_errors` (up to 10 commands
whose reply the client could not parse, with the parser's error),
`validated` (how many replies were held to a reply schema), and
`schema_mismatches`: for each command and protocol with a reply that did not
match, its `name`, `resp`, the `count` of such replies, and the first of
them: the `command` as sent, the `reply` (up to 4 KB of its JSON text), the
`problem` (where in the reply, and why), and the `schema`.

## `valkey.workload.config-churn`

Changes `changes` configs with `CONFIG SET` on the running instances, one
every `interval_s`, beside the other workloads and the faults until teardown.
Each change picks an instance, then a config: one of its build's config
space (the configs [random scenarios](../scenarios-and-experiments.md#declaring-what-may-be-randomized)
draw from the build's own config table, `valkey.pool.config-space`) that can
change at run time (neither `IMMUTABLE_CONFIG` nor `PROTECTED_CONFIG`) and
that the instance's planned config leaves to Valkey's default, so it never
undoes what the experiment, a pool, or the table draw set; or one of `keys`,
the keys a hand-written pool drew at the start, even though the planned config
sets them. A table config's value is drawn as the generator draws one, a key's
uniformly from its values. A change lasts until the deployment is removed
(no undo obligation), and a restarted instance starts with its planned config
again. What Valkey answers is recorded, not judged: an error reply (a value a
validator refuses, such as `forkless-policy prefer-forkless` on a server
started without `forkless-infrastructure-enabled`, or a config refused while
loading) is a refusal Valkey declares, which explains the error replies it counts
([`valkey.oracle.error-counters`](oracles.md#valkeyoracleerror-counters)), and a
connection that fails meets an instance a fault stopped. The experiment's
oracles judge what the changes do: no acknowledged write lost, no crash,
replication intact, replies only as Valkey declares. The
[`valkey.config-churn`](../scenarios-and-experiments.md#fragments) fragment adds
it to every shipped experiment with `changes` from the `config_changes` input,
which random scenarios draw from 0 to 20, and `keys` from the
`config_churn_keys` input: in a random scenario, the keys it drew from the
encoding, memory (eviction policy, lazy freeing, defragmentation) and cron
pools, each with its pool's values.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `changes` | integer, 0 to 1000 | 0 | How many changes; 0 makes none. |
| `keys` | mapping of config key to a list of values | `{}` | Keys to change besides the table's, where the planned config sets them too. |
| `interval_s` | number, 0.05 to 600 | 1 | The wait before each change. |
| `op_timeout_s` | number, 0.05 to 60 | 2 | Each `CONFIG SET`'s timeout. |

Evidence: `valkey.workload.config-churn`: `changes` (each with `at`,
`instance`, `config`, `value`, and its `reply` or `error`, `unanswered` when
the instance did not answer; or `skipped` when its build has no config table
or no config left to change), `errors` (how many Valkey refused), and
`unanswered`.

## `valkey.workload.benchmark`

Runs each selected instance's own `valkey-benchmark` (beside its
`valkey-server`) against it from start to teardown: `clients` connections,
each sending `command` again as soon as it is answered (`-l`). The server's
event loop then wakes with many ready connections at once, which is the
load under which Valkey (9.2 and later) polls its priority connections
(cluster bus, replication) again within one event-loop pass
(`priority-preemptive-poll-interval-us`). A benchmark that exits, because a
fault killed its instance, starts again. Nothing it sees is judged.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `instances` | selector | `primary` | |
| `clients` | integer, 1 to 1000 | 32 | Connections per instance (`-c`). |
| `pipeline` | integer, 1 to 1000 | 1 | Commands in flight per connection (`-P`). |
| `command` | list of strings | `[PING]` | For example `[DEBUG, SLEEP, "0.001"]`, which needs `enable-debug-command yes`. |

`ERROR` when no instance matches or a build has no `valkey-benchmark`.
Evidence: `valkey.workload.benchmark` (`targets`, `command`, and `starts` per
instance).
