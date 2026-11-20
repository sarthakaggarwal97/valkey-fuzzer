### Added: a findings store across batches, and a cached Valkey build (PR-T7, P-07, P-08)

- `findings.json`: when a batch finishes it appends its failure signatures to a findings store (the one beside its
  `summary.json`, or the one `--findings PATH` or `findings:` in a batch spec names, which several batches share),
  one entry per signature key with its label and signature, `first_seen`, `last_seen`, `count`, the runs that hit
  it (batch folder and job id, so a resumed batch counts nothing twice), a `reproduce` command (the latest run's
  replay), and a `status`: `new` until a second batch sees it (`known`), `fixed` when set by hand, and `new` again
  when a fixed one comes back. `format_version` 1, read and written whole (docs/stability.md).
- `valkey-fuzzer findings list [--status new|known|fixed] [--store PATH]` and `findings show KEY [--store PATH]`
  read a store or a batch folder's; `KEY` may be a prefix that names one finding.
- `valkey-fuzzer fetch-valkey [REF]` builds a tag, branch, or full commit of `valkey-io/valkey` once into
  `$XDG_CACHE_HOME/valkey-fuzzer/valkey/<commit>/` (`~/.cache` by default) and prints its `valkey-server`; run
  again, it prints the same path and builds nothing. Without `REF`, the pinned 9.1.3 commit a deployment builds
  when it has no binary.

### Changed

- A deployment with no `valkey_binary` and no `valkey-server` on `PATH` uses the pinned commit's build in the
  cache, and makes it there on the first run (one log line says which: `Using the cached Valkey build of commit
  ...` or `Building Valkey commit ... into ...`), instead of a clone under `/tmp/valkey-build` every user of the
  host shared. The commit itself is fetched, so a moved tag can no longer refuse the build. The report's
  `reproducer.valkey_sha` records the commit as before.
- One signature derivation: `signature_key` and `signature_label` live in `valkey_fuzzer.minimize` beside
  `failure_signature`, which takes the plan's required evidence itself; the batch, the minimizer, and the findings
  store call the same functions, and `minimize` prints the key with the signature it holds.
