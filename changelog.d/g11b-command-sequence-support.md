### Changed

- The fake builds, private servers and connection the command-sequence, command-metadata and tools tests share
  (`Build`, `BUILDS`, `SERVERS`, `FakeServer`, `FakeWire`, `fake_open_raw_connection`, the reply markers and
  `nodes_of`) live in `tests/command_sequence_support.py`; `test_command_metadata.py` and `test_tools.py` import
  them from there instead of from `test_command_sequence.py`.
