# Observers

Part of the [Valkey component reference](README.md): the
[index](README.md#index) lists every component, [Common config
forms](README.md#common-config-forms) the selectors, predicates, and trigger
fields the tables below use.

Observers only record: they collect evidence for the whole run, record an
evidence item when a declared expectation is met, and never judge the
server. An expectation or rule marked `required: true` becomes required
evidence: path evidence, without which the run did not reach what it tests
and is `ERROR`. Mark only path evidence required, such as a save seen
in progress. An expectation about behaviour, such as no save left in
progress or a partial resync counted, is left unmarked and checked by a
[`valkey.oracle.evidence`](oracles.md#valkeyoracleevidence) oracle that depends on the
observer, which fails the run when it is missing. An observer that could not
judge an unmet expectation, because every INFO sample it needed failed or a
log could not be read, returns `ERROR`: it did not do its part, so a failure
that depends on it is not trusted. Raw samples and excerpts are saved in the
component's artifact folder.

## `valkey.observer.info`

Samples INFO on the selected instances at start (`baseline`), every
`interval_s`, on each event in `on_events`, and at stop (`final`), writes the
samples to `info-samples.jsonl`, and checks its expectations.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `instances` | selector | `primaries` | |
| `sections` | list | `[persistence]` | INFO sections to read. |
| `interval_s` | number, 0 to 3600 | 0.05 | 0 turns periodic sampling off. |
| `on_events` | list | `[]` | Events that trigger a sample. |
| `expectations` | list | `[]` | See below. |

An expectation has `evidence_id` (namespaced, unique in the list), `field`,
and either a [predicate](README.md#predicates) or `delta_min` and `delta_max` (the
change from an instance's `baseline` sample to its `final` one). Counters
restart with the process, so a delta cannot be judged (the observer returns
`ERROR` if it is unmet) when either sample failed, the instance's process
changed between them, or the value went down. `when` picks the samples
a predicate is checked on: `any` (the default), `baseline`, `final`, or
`during` (periodic and event samples). `instances` is `any` (the default: one
instance suffices) or `all`. `required` defaults to `false`.

A `field` can read into the `key=value,...` lists some INFO fields hold with a
dotted path: `slave0.state` in `slave0:ip=…,state=online`. A `*` in the first
segment stands for an index, so `slave*.state` names that entry of
every replica line; a predicate then holds when the path names at least one
entry and every entry matches, and a delta sums the entries.

```yaml
expectations:
  - {evidence_id: valkey.save.in-progress, field: rdb_bgsave_in_progress, equals: "1", when: during, required: true}
  - {evidence_id: valkey.persistence.saves-advanced, field: rdb_saves, delta_min: 1, instances: all}
  - {evidence_id: valkey.repl.links-online, field: slave*.state, equals: online, when: final}
```

Evidence: one item per met expectation, under its `evidence_id`.

## `valkey.observer.log`

Notes the size of each selected instance's log at start, and at stop reads
what was written since, saves the excerpts, and checks its rules. A
`forbidden` match, such as `Bad file format reading the append only file`, is
a finding about the server, not an observer failure: the observer passes and
records it, and [`valkey.oracle.liveness`](oracles.md#valkeyoracleliveness), which
every Valkey experiment includes, fails the run on it. A log that cannot be
read makes the observer `ERROR` when an unmet `expect` rule or a `forbidden`
rule needs it: it cannot show what the server did or did not log.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `instances` | selector | `all` | |
| `expect` | list | `[]` | Rules `{pattern, evidence_id, min_count, instances, required}`: a regular expression, the evidence to record, the matches needed on an instance (default 1), `any` or `all` instances (default `any`), and `required` (default `false`). |
| `forbidden` | list | `[]` | Rules `{pattern}`. |
| `max_excerpt_lines` | integer, 1 to 100,000 | 200 | Lines kept in each excerpt. |

Evidence: one item per met `expect` rule; `valkey.log.forbidden-match` on a
forbidden match, mapping each matched pattern to the instances that logged it
and up to five of their matching lines.

## `valkey.observer.log-trigger`

Tails the selected instances' logs and publishes an event for every line a
rule matches, so a fault can trigger on a log line. The payload carries
`instance_id`, `line`, `logged_at` (the line's own timestamp), `detected_at`,
`rule` (the rule's index), and the pattern's named groups; all-digit groups
become integers. Combined with `occurrence`, `when_payload`, and
`instance: event`, a fault can, for example, kill the instance that granted
the second failover vote the moment it logs it.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `instances` | selector | `all` | |
| `rules` | list | | At least one `{pattern, publish, evidence_id, min_count, max_publishes, required, logged_by}`: a regular expression, the event name to publish, optional evidence to record once an instance matched `min_count` times (default 1), the most events to publish (default `null`: no limit), `required` (needs `evidence_id`), and `logged_by` (`any`, the default, or `server`: the lines of a fork child, an RDB or AOF child with role `C` in the line's `pid:role` prefix, are skipped, so a line both log, `DB saved on disk`, is one event). Named groups may not be called `instance_id`, `line`, `logged_at`, `detected_at`, or `rule`. A rule without `pattern` takes the `log` pattern, and its `logged_by`, that `spec`'s mapping gives for its `publish` event. |
| `spec` | string or null | `null` | A [spec bundle](../spec-bridge.md) (`formal/<protocol>/`) whose `mapping.yaml` holds the patterns of rules that have none. |
| `spec_sha256` | string or null | `null` | Filled in when the scenario compiles: the SHA-256 of `spec`'s `mapping.yaml`, so the plan pins the mapping it was compiled against. A run whose mapping has another digest stops with `ERROR`; compile the scenario again. |
| `poll_interval_s` | number, 0.0005 to 10 | 0.002 | |
| `switch_interval_s` | number, 0.0001 to 0.1, or null | `null` | Sets Python's thread switch interval for the process while tailing, which shortens the delay before a fault fires; restored at stop. |

Evidence: one item per rule with `evidence_id` that was met. Events: the
`publish` name of each matching rule. When a log could not be read (or a
poll failed), lines may have been missed: if a rule never matched (its
evidence or its event may be in those lines), the observer returns `ERROR`.

## `valkey.observer.trace`

Writes the run's events and per-node samples of the servers' state to
`trace.jsonl`: a `header` record (the node map, `config_keys`, and what is
sampled), an `event` record per event, and `observation` records, one per
node per sampling round. What a round reads from a node is what the `spec`
bundle's `mapping.yaml` asks for in its `sample:` section ([Samples](../spec-bridge.md#samples)):
INFO sections (`info`, as text fields), `DEBUG DIGEST` (`digest`) and
`DEBUG DIGEST-VALUE` of named keys (`digest_values`), the node's cluster view
(`current_epoch`, `config_epoch`, role, slots, the primaries it knows),
`CLUSTER GETSLOTMIGRATIONS` (`slot_migrations`: the jobs by kind, `export`,
`import`, and a replica's `tracking`, each in creation order), and the `vars`
line of its `nodes.conf` (`disk`, read from the file, so a killed node still
has one).
A node's INFO, digests and slot migrations are read in one `MULTI`/`EXEC`,
so the fields of one sample are of one instant. Without a `spec`, a cluster
deployment is sampled for its view and `nodes.conf`, and a standalone one
records events only. A node that does not answer gets an `error` record.
Sampling rounds run periodically and shortly after events (every event, or
those the section's `on` globs name), on a background thread, so the
observer never delays a fault. A sample that fails at the start (`DEBUG`
refused, say) is an `ERROR`: every node is up then, and the claims that read
the sample would be checked on nothing. A bundle that samples `DEBUG DIGEST`
needs `enable-debug-command yes` in the deployment's `server_config`; the
scenario fails to compile without it.
[`valkey-fuzzer spec conform`](../spec-bridge.md#spec-conform) reads the file.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `nodes` | selector | `all` | |
| `interval_s` | number, 0 to 3600 | 0.25 | The sampling period; 0 turns periodic rounds off. |
| `sample_on_events` | boolean | `true` | Also sample after events. |
| `event_delay_s` | number, 0 to 60 | 0.05 | How long after an event its round starts. |
| `max_rounds` | integer, 1 to 1,000,000 | 20,000 | |
| `sample_timeout_s` | number, 0.05 to 30 | 0.5 | Client timeout per sample. |
| `config_keys` | list | `[cluster-config-save-behavior, cluster-node-timeout]` | Configs recorded per node in the header; a node whose configs could not be read gets `config_error` instead. |
| `spec` | string or null | `null` | A [spec bundle](../spec-bridge.md); its mapping's `sample:` section says what to sample, and the header records the mapping's SHA-256 (and the plan's inputs), so `spec conform` can flag mapping drift. |
| `spec_sha256` | string or null | `null` | Filled in when the scenario compiles: the SHA-256 of `spec`'s `mapping.yaml`, so the plan pins the mapping it was compiled against. A run whose mapping has another digest stops with `ERROR`; compile the scenario again. |

Evidence: `valkey.trace.recorded` (the path and record counts).
