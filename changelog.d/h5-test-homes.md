### Changed

- The last two test helpers one test module imported from another have support homes:
  `tests.valkey_experiments_support.FakeHandle` (the Valkey components' fake deployment, a primary and replicas
  with log files and one `FakeClient` per node) and `spec_of` (the resolved spec of a component by use and id, the
  role the id names; `test_valkey_components._spec` before), which `test_modules.py` and `test_search_oracle.py`
  import from there. `tests/test_helper_homes.py` keeps it so: no test module imports `tests.test_*`. The three
  `builds` fixtures of the command-sequence, command-metadata and tools tests are one
  `tests.command_sequence_support.fake_builds(monkeypatch, tmp_path, labels, tools=...)`, the tools' script
  (`TOOL`) beside it.
