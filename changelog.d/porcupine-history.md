### Removed: the history oracle and the porcupine checker

- `valkey.oracle.history`, the `valkey.history` fragment, the `porcupine/` Go module with its vendored Porcupine
  library, `valkey_fuzzer/components/history.py` and `porcupine.py`, the pinned `porcupine_sha256sums.txt`, the
  `valkey-fuzzer-fetch-porcupine` command, `THIRD_PARTY.md` (the MIT notice it carried), the `Porcupine checker
  binaries` workflow, and the Go stage of the `Dockerfile` are gone. In five weeks of soak the oracle found no bug:
  the ledger oracle's sequential model already judges the single-client history the workload produces, and the
  oracle needed a Go binary on `PATH` that `pip install` could not provide.
- The seven experiments that extended `valkey.history` (`valkey.aof-durability`, `valkey.cluster-partition`,
  `valkey.mixed-version-cluster`, `valkey.replication-chaos`, `valkey.replication-degraded-link`,
  `valkey.replication-full-resync`, `valkey.rolling-upgrade`) keep every other oracle; `valkey.oracle.ledger` stays
  their durability oracle. No experiment needs anything beyond Python and a `valkey-server` build any more: the
  `Set up Go` and `Build the porcupine checker` steps are out of every workflow, and the image no longer carries
  the checker.
- The multi-client key-value workload (`clients` on `valkey.workload.key-value`, the ledger model's concurrent
  mode, and the `mix.rename` and `valkey.oracle.rdb-snapshot` refusals of several clients) goes with it: the
  history oracle was its only consumer.
- The way back: `git checkout 9ffe200 -- valkey_fuzzer/components/history.py valkey_fuzzer/components/porcupine.py
  porcupine/ valkey_fuzzer/components/fragments/valkey.history.yaml` restores the oracle, its checker and the
  fragment, self-contained.
