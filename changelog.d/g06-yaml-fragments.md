### Changed

- Two new experiment fragments hold what five experiments and the `module-chaos` example each carried:
  `valkey.second-election` (the `voter-recovered`, `before-second-election`, `second-election`, `election-safety`,
  and `deployment-state` steps and the `shard`, `restart_delay_s`, and `second_election_delay_s` inputs of
  `valkey.cluster-election-durability` and `valkey.cluster-vote-save-crash`; it extends `valkey.election-steps`,
  which gains the `save-behavior-support` build probe) and `valkey.links-up` (the `links-up` poll of
  `valkey.reconnect-replicas`, `valkey.replication-cpu-starvation`, `valkey.replication-degraded-link`,
  `valkey.replication-full-resync`, and the example). Each experiment lists the component IDs it inherits in its own
  order with its own `depends_on` and overrides (cpu-starvation keeps `timeout_s: 60`; the election experiments keep
  their `trigger` and `crash_faults`), so every compiled plan is byte for byte the one it was before.
