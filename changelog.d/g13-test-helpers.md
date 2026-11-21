### Changed

- One home per test helper: `tests/conform_support.py` writes every trace file the model tests replay
  (`write_trace`, `events`, `write_register_trace`), holds the election trace's node IDs and first round, the
  register bundle copy (`register_copy`, draft or approved) and the file edit the bundle tests make;
  `tests/support.py` holds the runner's plan and fake component builders, the config-space stand-in build, a
  deployment provider registration and the format-version error message; `tests/valkey_experiments_support.py`
  the COMMAND DOCS reply. The CLI tests run through `conftest.cli`, the verdict tests of the evidence contract
  moved to `tests/test_verdict.py` with one `_decide`, and `tests/test_cli_gaps.py` joined `tests/test_cli.py`
  and `tests/test_batch.py`. The components-layer copy of "missing required evidence is ERROR" keeps the row the
  runner and verdict tests do not have. No test imports another test module for these helpers any more.
