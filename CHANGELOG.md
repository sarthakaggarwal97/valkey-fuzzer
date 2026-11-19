# Changelog

## Unreleased

### Changed: `batch.py` is the `valkey_fuzzer.batch` package; the zoo and spec conformance are its plug-ins

- The 1,467-line module is split by concern: `spec.py` (options, sources), `sources.py` (random, matrix, scenarios,
  failures), `jobs.py`, `worker.py`, `results.py`, `pool.py` (`BatchRunner`), `summary.py`. The names callers
  import (`BatchOptions`, `BatchRunner`, `expand`, `load_spec`, `normalize_source`, `job_result`, `signature_key`,
  `signature_label`, `run_worker`, `main`) stay importable from `valkey_fuzzer.batch`, and the worker command is
  still `python -m valkey_fuzzer.batch TASK`.
- `batch/hooks.py` is one plug-in interface: a `BatchSource` (a kind of `sources` entry) and a `BatchPlugin` with
  the steps around a run (`after_run` in the worker, `judge` and `record` on a finished job, `summarize`). The bug
  zoo (`batch/zoo_plugin.py`) and spec conformance (`batch/conform_plugin.py`) are registered through it; the batch
  loop no longer names either. `Expansion.zoo` is `Expansion.recorded["zoo"]`.
- No change to `batch.json`, `jobs.json`, `results/*.json`, `conformance/*.json`, `summary.json`, `batch.log`, the
  console output, or the exit codes for the same seeds. The one visible difference is the order of the source kinds
  in the error for an unknown source (`zoo` is now listed last).

### Changed: packs register through the `valkey_fuzzer.packs` entry point

- `build_catalog` no longer imports the Valkey components by name: it calls the `register(catalog)` of every entry
  point of the `valkey_fuzzer.packs` group, in name order. The Valkey components are the `valkey` entry point
  (`valkey_fuzzer.components:register`), so `run`, `batch`, `validate`, and `list` see the same catalog as before.
  A package of your own can ship components, providers, and experiments the same way, with no edit to the fuzzer
  (`docs/extending.md`, "Ship a pack"). `register_valkey_components` is removed; call
  `valkey_fuzzer.components.register(catalog)`.
- An existing editable install must be reinstalled (`pip install -e .`) to record the entry point; without it no
  Valkey component is registered.
- A scenario that uses a component whose namespace no installed pack registers now fails with
  `no installed pack registers <namespace>.* components; install the package whose 'valkey_fuzzer.packs' entry
  point is named '<namespace>'` instead of a "did you mean" naming an unrelated Valkey component.

### Changed: the replication and slot-migration claims say what their mapping pins

- `formal/replication/claims.yaml` claimed its properties under `AdoptOnLoad = TRUE`, a fix Valkey does not have,
  while `mapping.yaml` pinned `AdoptOnLoad` to `FALSE` for every run. The claims now hold for Valkey as it is over one
  channel (`holds_when: {DualChannel: false, AdoptOnLoad: false}`, what `model.cfg` checks and what the mapping
  pins); over two channels the properties hold only with the fix (`dual-channel.cfg`). The bundle goes back to
  `status: draft`: no property can be checked on a run (every variable both read is hidden in the mapping, so
  conform could never fail them), and the approval enforced nothing. No shipped bundle is approved now.
- `formal/slot-migration/mapping.yaml` pinned `CancelKeepsPause`, `ExpiryAfterClaim`, and `ClaimReachesReplica` to
  Valkey as it is, the reverse of every property's `holds_when`, so no run could ever be checked as claimed. It now
  pins the claims' values, as `cluster-election` pins `FlushBeforeSave`: a run in which Valkey as it is lets two
  nodes write, or loses an acknowledged write, violates the claim once a property is conformed.
- A batch's conform entry for a run whose worker wrote no `conform.json` (or whose bundle does not load) now says
  whether the bundle's claims are `approved`, as the worker's entries do, and the verdict reads it instead of
  defaulting to approved: a draft bundle's missing check is reported, an approved bundle's is an `ERROR` for the run,
  and a bundle that does not load is held against the run.

### Removed: unreferenced example scenarios

- `examples/valkey/replication_full_resync.yaml` and `replication_partial_resync.yaml` only pinned a seed, ports and
  the defaults of the shipped experiments of the same id; run them with `--experiment valkey.replication-full-resync`
  or `--experiment valkey.replication-partial-resync`. `examples/cluster_scenario.yaml` and
  `examples/standalone_replicas_scenario.yaml` had no reference anywhere.

### Removed: four components no shipped experiment, example, zoo entry, or workflow used

- `valkey.action.replicaof`, `valkey.action.restart-instance`, `valkey.fault.command`, and `valkey.fault.deny-writes`
  are removed, with their tests and `docs/components.md` entries. An experiment that names one is now rejected as an
  unknown component. To send a command at a lifecycle event, use `valkey.action.command` after the event; to restart
  an instance, use `valkey.fault.process-signal` (`restart_mode`) or `valkey.action.rolling-upgrade`.

### Removed: the one-off search-abort-probes workflow

- `.github/workflows/search-abort-probes.yml` and `.github/scripts/search_abort_probes.py` are removed. Nothing
  imported the script.

### Removed: coverage-guided batches (`--guided`, `--corpus-from`)

- `batch --guided N`, `batch --corpus-from BATCH`, and the batch spec keys `guided` (in a `random` source) and
  `corpus_from` are gone, with `corpus.py`, the gcov feature collection (`coverage.gcov_features`), the
  `code_coverage` summary key and result field, and `corpus.json`. A batch spec that names `guided` or
  `corpus_from` is now rejected as an unknown key. No workflow used them, and they needed a gcov build of Valkey.
- `reproducer.generated` in a run report no longer carries `draws`, and `Overrides.pinned` and
  `GeneratorBlock.kept` are removed: only guided batches read them.

### Removed: the `diff` command and the differential engine

- `valkey-fuzzer diff`, differential documents (`differential_id`, `variants`, `compare`), `RunService.differential`
  and `valkey_fuzzer/differential.py` are removed, with `examples/valkey/differential_baseline_vs_candidate.yaml`
  and the `<root>/differential/` report folder. There is no deprecation shim: `diff` is now an unknown command.
  To compare two builds, use `compare_with` of `valkey.action.commands` (or of `valkey.action.tools`), which runs
  one sequence on a candidate and a parent server and compares the replies. Batch `matrix` dimensions are unchanged.

### Changed: `replay` and `minimize` warn about drift and never refuse

- A drifted `replay` or `minimize` runs, prints each difference to stderr, and `replay` records it in the
  report's `provenance.drift` (`item`, `recorded`, `replay`). `--allow-drift` is removed from both commands,
  `provenance.drift_allowed` is gone, and a bare `plan.json` replays unverified (`provenance.verified: false`).
  A drifted replay exits with the run's own verdict code; it no longer exits `2` for drift or `64` for a bare plan.

### Changed: readable run output and `report.json` layout (keys preserved)

- `report.json` now starts with `status`, `summary`, `failed_components` (values cut to 120 characters with
  their length and SHA-256), `reproduce`, and `look_at`; the data tools read follows. New keys only: no key
  was removed or renamed, and `outcome` is unchanged. `reproducer` gains `command` and `valkey_binary`.
- Every `run` prints `Reproduce: valkey-fuzzer replay <report>` (a generated scenario also prints it as an
  alternative), and a FAIL or ERROR prints a `Look at:` line.
- The local deployment's planning lines are logged at DEBUG, and a source build's output goes to
  `<build dir>/build.log` after a one-line notice.

### Changed: a missing `porcupine` is reported at `validate` and at run start

- A scenario with `valkey.oracle.history` now fails `validate` (exit `2`) and a `run` fails as `ERROR` before
  anything is acquired or started, when `porcupine` is not on `PATH`. The message gives the build command
  (`go -C porcupine build -o <a directory on PATH>`) and says Go >= 1.21 is needed when `go` is missing too.
  It was an `ERROR` of the oracle after the whole run. Components can register a `probe` for this
  (`ComponentRegistration.probe`); it runs at validate and run start, never at compile.
- `pytest` lists every skip and its reason (`-rs`). `test_every_excluded_config_is_one_the_build_under_test_has`
  runs only on a Valkey tree at the commit in `zoo/valkey-commit`, so another release on `PATH` no longer fails it.

### Changed: every run has a deadline, every wait a watchdog

- A run without `--timeout` gets a derived deadline: the search profile's `budget.timeout_s`, else 1200 s,
  and for `run` never below the sum of the components' `timeout_s`. `run` prints it with its source.
- The deployment provider's `cleanup` and `rollback`, each obligation's `undo`, and the teardown of a
  component without `timeout_s` are cut after 60 s. The run is `ERROR` and names the wait that expired.

### Changed: first-run docs, spec-bridge claims

- README and getting-started state Go 1.21 and the `porcupine` build, the real Valkey build behavior (only four
  experiments probe and report `ERROR`; the rest build Valkey on first use), and the interim clone branch. New page `docs/reading-a-fail.md`.
- `spec check` reports the `approved_claim` gate of a draft as `skip` (`[SKIP]`), not `pass`. Docs and bundle READMEs
  say that only `cluster-election` is checked against live runs; `replication`, `persistence`, and `slot-migration`
  are model only.

### Changed: a server that dies at startup

- A server that exits before its first `PING` is `ERROR` with `exited with code N after Ts` and the last five
  output lines (it said `failed to start within 60s` after half a second). If its output holds a crash
  signature the run is `FAIL`, and the summary names the crash. See `docs/contracts.md`.

### Added: first-run gaps (read by `valkey-ci-agent`)

- `--base-port` on `run` and `diff` (and as an alias of `batch --port-base`); `diff --artifact NAME=PATH`.
- Errors that name a field, component, or experiment now offer the closest names and list the valid fields;
  `replay` of a scenario file says to use `run`; `validate --strict` warns when no component of the experiment's
  own is required evidence.
- `run` prints `cores` and `attempt` beside a generated scenario's seed, and the batch summary prints the cores
  per run: a seed draws the same scenario only at the same cores (`docs/contracts.md`, Seeds).
- A `smoke` run prints `Status: PASS (mode: smoke; no coverage claim)`; the batch summary counts smoke runs
  apart (`smoke`, `cores`, and per-run `attempt` and `mode` in `summary.json`; `counts` leaves smoke runs out).

### Changed: exit codes and replay verification (read by `valkey-ci-agent`)

- New exit code `64` (`EX_USAGE`) for usage and input errors: a bad option or option mix, no command,
  a missing or unreadable input file, a bare `plan.json` replayed without `--allow-drift`. These
  exited `2` before; `2` now means harness `ERROR` only. `batch` and `validate` keep `2`.
- `replay` of a run that `FAIL`s on a different Valkey binary or artifact exits `1` (was `2`); the drift
  is in the report's `provenance.drift`. A PASS or ERROR on drifted code still exits `2`.
- `minimize` exits `2` when no reduction kept the failure (was `0`).
- `replay` of a bare `plan.json` is refused unless `--allow-drift`; a run now writes `reproducer.json` beside
  `plan.json` so `replay <folder>/plan.json` is verified. Python `major.minor`, platform, and a digest
  missing on either side now count as drift.

### Added: multi-client workload for the history oracle

- `valkey.workload.key-value` takes `clients` (1 to 64, default 1): that many writer threads, each on its
  own connection, over the same keys, so operations overlap and `valkey.oracle.history` checks a concurrent
  history. The workload evidence records `clients`; the oracle records it too, words its verdict by it
  ("single-client history" or "N-client history"), and takes `min_clients` to ERROR on a workload with fewer.
- With `clients` above 1, `valkey.oracle.ledger` accepts every value an overlapping write could leave and
  leaves reads to the history oracle; `mix.rename` and a `valkey.oracle.rdb-snapshot` `workload` need
  `clients: 1`. Replica reads are not part of the history (they are not linearizable against the primary).
