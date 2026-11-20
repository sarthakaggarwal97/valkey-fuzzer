### Added

- Every file the fuzzer writes and reads back carries `format_version` (an integer, `1`): `report.json` (first
  key), `plan.json`, a batch's `summary.json` and `batch.json`, a worker's `conform.json`, and the header of a
  `trace.jsonl`. `replay`, `minimize`, and `spec conform` refuse a report, plan, or trace of another version, and
  `batch --resume` a folder, with one message naming the file and both versions (an input error, exit `64`;
  `2` for the batch); a file from before the field is read as version `1`. A key removed or renamed bumps it; a
  key added does not. `valkey_fuzzer.reports.FORMAT_VERSION` and `format_version_problem()` hold the rule.
- `docs/stability.md`: what a release keeps (the commands and exit codes, scenario and experiment YAML, the keys of
  `report.json` and `summary.json`, the shipped component ids and their events and evidence, the entry-point
  groups) and what it does not (Python internals, console text, the layout under `components/`, failure
  signatures, what a seed draws).
- Changelog fragments: a pull request adds `changelog.d/<branch-slug>.md` (bullets, under an optional `### Added`,
  `### Changed`, `### Removed`, or `### Fixed`) instead of editing `CHANGELOG.md`, and
  `python scripts/fold_changelog.py <version>` moves them under a `## <version> - <date>` heading at a release
  (`changelog.d/README.md`).

### Changed

- A worker's `conform.json` is `{"format_version": 1, "bundles": {<bundle>: <result>}}`; the results were the file.
  Only the batch that wrote it reads it.
