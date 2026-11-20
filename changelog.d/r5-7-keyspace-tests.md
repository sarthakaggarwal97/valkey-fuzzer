### Changed

- A keyspace snapshot builds each value's canonical content once: `exact()` already returns JSON data (bytes as
  str or `{"$base64": ...}`, tuples as lists, zset scores `repr`ed, a DUMP as a hex digest), so the second walk
  over it is gone. The snapshot and its `keyspace_digest` are byte-identical to before; 100k mixed keys on one
  server snapshot in about 1.54 s instead of about 1.77 s.
- `FakeContext(time_scale=)` divides the seconds a component asks `sleep` for, so a test of a component whose
  config holds real delays runs them faster without changing the config; the four slot-migration chaos tests that
  walk whole migrations and the default waves' 2 s restart delay use it and take 1.3 s instead of 7.6 s.

### Fixed

- `test_provision_names_every_busy_planned_port_before_starting_anything` binds two adjacent ephemeral ports
  before planning on them, instead of assuming the port below the one it listens on is free: it failed when that
  port was in use on the host.
