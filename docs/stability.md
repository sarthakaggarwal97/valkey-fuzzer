# What is stable

What you can hold the fuzzer to from one release to the next, and what you
cannot. Versions follow [semantic versioning](https://semver.org/) once
`1.0.0` is tagged; before it, a minor release may change a stable surface,
and [CHANGELOG.md](../CHANGELOG.md) says so.

## Stable

A minor release may add to these (a command, an option, a key, an id); it
never removes or renames one, or changes what one means. That needs a major
release, and the changelog names it.

| Surface | What is held |
| --- | --- |
| CLI | The commands `run`, `batch`, `validate`, `list`, `replay`, `minimize`, `sweep`, `cleanup-logs`, and `spec`, their options as [the reference](cli.md) documents them, and the [exit codes](cli.md#exit-codes): `0` PASS, `1` FAIL, `2` ERROR, `3` SKIP_UNSUPPORTED, `64` usage, `130` interrupted. |
| Scenario and experiment YAML | The documents of [Scenarios and experiments](scenarios-and-experiments.md): a scenario's `scenario_id`, `seed`, `experiment`, `deployment`, `artifacts`; an experiment's `extends`, `id`, `deployment`, `inputs`, `components`, `startup_overlays`, `required_events`, `required_evidence`, `metadata`. |
| Files a run writes | Every key of [`report.json`](cli.md#run-reports) and of a batch's [`summary.json`](cli.md#the-batch-folder), `plan.json`, `scenario.yaml`, and `conform.json`. Tools read keys by name, never by position. |
| `format_version` | Each of those files, and a `trace.jsonl` header, carries `format_version` (an integer, now `1`). A reader accepts that version, and a file from before the field (read as `1`); it refuses another with one message naming the file and both versions. A key removed or renamed bumps it; a key added does not. |
| Component ids | The ids of the shipped components (`valkey.workload.*`, `valkey.action.*`, `valkey.fault.*`, `valkey.observer.*`, `valkey.oracle.*`, `valkey.capability.*`), their config fields, and the event and evidence ids they declare, as [Valkey components](components.md) lists them. |
| Entry-point groups | `valkey_fuzzer.packs` (a `register(catalog)` function) and `valkey_fuzzer.batch_plugins` (a function returning `BatchPlugin` instances), as [Adding tests](extending.md) describes them. |

## Not stable

Everything else can change in any release without notice, in particular:

- Python internals: every module, class, and function under `valkey_fuzzer`
  not named above, including `valkey_fuzzer.testing`, the `RunService` and
  `Catalog` methods, and the component base classes. Write components against
  the registration API in [Adding tests](extending.md) and the entry-point
  groups, not by importing the engine.
- The console output of every command, and the text of warnings and errors.
- The layout of the files under a run's `components/`, apart from the
  `trace.jsonl` header, and of a batch folder's `batch.json`, `jobs.json`,
  `results/`, and `work/`.
- The failure signatures `minimize` and `batch` compute, and the
  `signature_key` values they give.
- The search profiles' seeds and budgets, and which scenario a seed draws.
