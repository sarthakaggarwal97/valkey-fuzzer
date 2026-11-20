### Changed

- The `spec` commands raise their errors and `cli.main` reports them, as every other command's are: the same
  `Error: ...` line on stderr and exit 2 for a tool that did not run, a TLC run cut short, no counterexample, a
  bundle without targets, or no runs found. The three option mixes of `spec translate` (`--control` or `--trace`
  with `--reach` or `--mutate`; none of the four; `--trace` without `--config`) are usage errors and exit 64, as
  `docs/cli.md` says a usage error does; they exited 2.
- `minimize --output` writes the scenario as every other `--output` is written: to a temporary file beside it,
  renamed over it once synced, so an interrupted write leaves no half-written file. Same YAML.
- `findings show` prints its JSON with sorted keys, as `list --format json`, `sweep --format json` and
  `validate --format json` do.
- One definition of the `--exclude PATH=VALUE` option (`commands._common.EXCLUDE_OPTION`) that `run` and `batch`
  register with their own help text. `--help` of every command is unchanged.
