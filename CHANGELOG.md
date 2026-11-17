# Changelog

## Unreleased

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
