### Changed

- `valkey.observer.trace` reads a node's cluster view (CLUSTER INFO, CLUSTER NODES) in the same MULTI/EXEC as its
  INFO, digests and slot migrations, so a sample is of one instant as its docstring says: one round trip per node
  per round instead of three with a slot-migration bundle (two with the cluster-election bundle). The header reads
  every `config_keys` entry in one CONFIG GET, and the BGSAVE action reads `dir` and `dbfilename` in one.
  `components.cluster` gains `parse_cluster_view(info_reply, nodes_reply)`; `cluster_view(client)` wraps it.
- The trace's slot-migration jobs are parsed by `slot_migration.jobs_of`, the one reader of the
  CLUSTER GETSLOTMIGRATIONS reply, and grouped by kind in the trace.
- `valkey.action.migrate-slots` records `started_at` and `ended_at` on the run clock (`valkey_fuzzer.clock.now`),
  as every other action and fault does; `clock.py`'s docstring says which fields are on the run clock and which
  report-only fields stay on wall time.
