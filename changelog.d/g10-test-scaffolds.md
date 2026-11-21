### Changed

- Tests: `tests/support.py` holds the repository root (`ROOT`, `EXAMPLES`, `FORMAL`), the one fake executable
  (`fake_executable`), and the one real-time poll (`wait_until`, over `deadlines.wait_for`);
  `tests/valkey_experiments_support.py` holds the one config-rejection check (`rejects_config`) and the one
  experiment-directory scaffold (`experiment_directory`, `experiment_runtime`); the four heaviest users of
  `VirtualClock` take it as a `clock` fixture; and a seed sweep, a workers file, a log-trigger plan, a batch
  summary, a slot-migration view patch and a server-log writer are one helper each in their files, in place of the
  copies about 30 test files carried. No test changes what it checks.
