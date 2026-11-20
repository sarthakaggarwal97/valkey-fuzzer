### Changed

- `valkey.election-steps`: a fragment with the `election-log` observer (`valkey.observer.log-trigger`, `spec:
  cluster-election`) and the `trace` observer (`valkey.observer.trace`, `spec: cluster-election`), which
  `valkey.cluster-election-durability` and `valkey.cluster-vote-save-crash` extend instead of each carrying the
  same two components; both resolve to the same documents as before (component IDs, order, and `depends_on`
  unchanged).
- `formal/replication/mapping.yaml`: `sample-replica` aliases `sample-primary`'s `fields` (a YAML anchor), so a
  field added to one sample is added to both; the parsed mapping is unchanged.
- Tests: `tests/conftest.py` holds the one TLC-availability check (`needs_tlc`, the `tla_tools` fixture) and the
  one CLI runner (`cli(*argv, via_main=...)`), and `tests/valkey_experiments_support.py` the one `injected_fault`
  helper, in place of the copies the model, CLI, and fault tests carried. No test changes what it checks.
