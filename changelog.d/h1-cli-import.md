### Changed: the CLI import leaves the `valkey` client out (PR-H1)

- `import valkey_fuzzer.cli` no longer loads the `valkey` client package: `valkey-fuzzer --help`, a usage error, and
  a command that builds no catalog (`fetch-valkey`, `sweep`, `findings`) start about 50 ms sooner (167 ms to 116 ms
  on a warm import, measured). The client came in through three paths, each now imported where it runs:
  `deployment/local/ports.py` imported the planner (and so `cluster_nodes` and `valkey.crc`) for
  `busy_planned_ports`, which moved to `deployment/local/rerun.py`, its only caller; the `fetch-valkey` command
  imports `processes.fetch_valkey` in its handler, and the pinned release and commit it names in its help
  (`VALKEY_SOURCE_REF`, `VALKEY_SOURCE_SHA`) live in `deployment/local/models.py`; `zoo.check_build` imports
  `version_output` and `parse_version_output` when it checks a build. `RunService` imports `spec.conform`'s
  `conform_run` and `judge_run` for a run whose plan names a bundle, and the commands import `describe` for a
  report that has conform results. A command that builds a catalog (`run`, `list`, `validate`, `batch`, `replay`,
  `minimize`) still loads the client and `spec.bundle` with the components, as before. The spec package is still
  imported plainly by `commands/spec.py` and the batch package (PR-R3-5); the PR-R3-5 changelog entry no longer says
  that the CLI parser leaves the local deployment and the client out, which it did not. `--help` of every command is
  unchanged.
- One import of `key_slot`: `valkey.crc`, as the other five modules had it; `slot_migration.py` and
  `slot_migration_chaos.py` imported it from `valkey.cluster` (the same function).
