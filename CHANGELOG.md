# Changelog

## Unreleased

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
