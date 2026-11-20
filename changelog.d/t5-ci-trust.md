### Changed: CI trust hardening

- The PR fuzzer posts its comment from a new `comment` job, the only job with a write token; it checks out
  nothing, runs no shell, and downloads the one file the read-only `summarize` job rendered. The workflow's
  default token is `contents: read`.
- The PR fuzzer's summary and comment show every value a run wrote (status, seed, stage, duration, outcome,
  failed components, and the build's resolved SHA) as a code span, so a run's files cannot put a link, mention,
  or HTML in the comment. Each run's ID comes from its artifact's name, not from its `metadata.json`, and the
  workflow fails when the runs job failed, whatever the runs' metadata says. The summary gains a
  `- Fuzzer runs:` line.
- `search-chaos.yml` checks valkey-search's dependency `.deb` against a pinned SHA-256 before `dpkg -i`.
- Every workflow has a `concurrency` group (none cancels a run in progress), every job a `timeout-minutes`
  (`lint` 10, the unit test matrix 30, the PR fuzzer's small jobs 10 to 15), and the four soak schedules
  (`fuzzer-run`, `valkey-builds`, `search-chaos`, `valkey-experiments-live`) run only in
  `valkey-io/valkey-fuzzer` and `sarthakaggarwal97/valkey-fuzzer`; a dispatch runs anywhere.
- `docs/cluster-chaos-and-ci.md`: the label caller pins the reusable workflow and `fuzzer_ref` to one commit SHA
  instead of `@main`, and passes the PR's head SHA from the label event instead of `refs/pull/N/merge`; a new
  "Trust model" section says which job holds which token and why.
