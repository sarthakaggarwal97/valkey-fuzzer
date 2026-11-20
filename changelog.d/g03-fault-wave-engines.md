### Changed: one fault and wave engine

- The wave engine owns the restart path its standalone and slot-migration modes each copied: one
  `WaveRun._restart`, one `kill_undo` (in a mode with `restarts_killed`, waiting `heal_timeout_s` for the
  instance to be ready), one `restart_down` the heals call, and one `host_ready` check for partitions and
  degradations. The standalone mode fills two hooks, `before_restart` (its "primary is down" skip, verbatim) and
  `before_kill_undo` (its `REPLICAOF` to the primary); `valkey.action.replication-chaos` reads the restart's
  readiness wait from its `heal_timeout_s` (default 60, what it hard-coded before).
- `network.partition` and `network.degrade` create the chain or device, open its undo obligation, and apply the
  rules, for the two link faults and the two wave operations alike; the obligation opens where it did, after the
  chain or device exists and before the rules. The two link faults differ in one selector field and one payload
  key (`targets_field`, `targets_key` on `_HealingFault`).
- A fault on one instance resolves its target through `EventFault._select_target` and `_target_instance`; a held
  fault on one instance takes `HeldFault`'s default `_released_payload` and `_release_failed` (its text from
  `release_failed_text`) and declares `RELEASED_FIELDS` and `RELEASED_EVENT`. A fault that kills an instance
  shares `KILL_FIELDS`, `check_cluster_vars`, `KILL_RECORD_FIELDS`, `KILL_DECLARES`, and
  `EventFault._restart_after_kill`, which publishes `<id>.recovered` after `<id>.injected` as both did.
- `valkey.fault.disk-fill` is a `HeldFault`: its record names the release as every other held fault does,
  `released_at`, `release_reason` (`event` or `recover`), and `filled_seconds`, instead of
  `released_on_event_sequence`; `ballast_removed` still says whether recovery got the ballast off a busy tmpfs.
  Every other evidence record of these components is unchanged.
