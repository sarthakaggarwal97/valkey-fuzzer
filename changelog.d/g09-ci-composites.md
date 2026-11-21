### Changed

- Three composite actions under `.github/actions/` hold what the workflows repeated: `setup-fuzzer` (Python with
  the pip cache on `pyproject.toml`, then `pip install -e .` or `.[dev]`) in ten workflows, `tla-tools` (Java 17 and
  the pinned `tla2tools.jar`, checked against its SHA-256) in the five jobs that run TLC, and `build-valkey` (a
  shallow fetch of a branch, tag or commit of valkey-io/valkey built with `make`, with an optional cache key) in
  six workflows. The TLA+ release is pinned in one place, the action, and `valkey_fuzzer/spec/tla.py` names the
  same one; `tests/test_ci_workflows.py` keeps them equal and checks every composite follows a checkout. The
  nightly live workflow's Valkey 8.1 build keeps its own restore and save cache steps, which save right after the
  build so a failed night still saves. Every workflow's triggers, jobs, permissions and concurrency are as before;
  Dependabot now updates the pinned actions inside `.github/actions/*` too.
- `tests/test_ci_workflows.py` pins the repositories each schedule runs in (`SCHEDULE_OWNERS`): the four soaks in
  `valkey-io/valkey-fuzzer` and `sarthakaggarwal97/valkey-fuzzer`, the bug zoo and the nightly TLA+ model check
  upstream only, as before. A workflow moves between the two lists there and in its job's `if`.
