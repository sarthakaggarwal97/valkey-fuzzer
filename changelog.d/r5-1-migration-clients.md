### Changed

- `valkey.action.slot-migration-chaos` follows each wave's migration through one client per endpoint, opened at the
  first read and closed when the migration ended, instead of a new TCP client (connect, two CLIENT SETINFO, the
  command, close) for every 10 ms read of CLUSTER GETSLOTMIGRATIONS; `valkey.action.migrate-slots` holds the
  source's client for its wait the same way. A client whose connection failed or was lost is closed and replaced
  at the next read. One read drops from about 0.36 ms to 0.05 ms against a real server, and the endpoints under
  chaos see no connect per poll. `slot_migration.read_jobs(handle, clients, instance_id, context=)` is the one
  reader both actions use.
- The slot keys of `valkey.action.slot-migration-chaos` are written and read through one slot-routing client for
  the whole run (opened in `prepare`, closed in `finish`) instead of a new `ValkeyCluster` (CLUSTER SLOTS
  discovery, a connection pool per node) per MSET or MGET; it follows MOVED after a migration as before. One
  slot-key command drops from about 5.2 ms to 0.08 ms on a 3-primary cluster.
- The follow reads the selector's `down` set directly instead of copying it on every poll.
- `WaveRun.restart_killed(instance_id, stage)` finds a killed instance's open `process.stopped` obligation and
  discharges it as `stage`; the standalone mode's `restart` and the migration mode's `restart` and heal all call it
  instead of each spelling the lookup. No behaviour change.
