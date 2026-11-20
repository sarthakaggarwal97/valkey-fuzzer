### Changed

- The `waves` config check of the chaos actions hands each operation's own rules (its targets and peers, what it
  needs of the wave's migration and of the deployment, its fields) to one `_check_operation` call per operation;
  `check_waves` keeps the rules that read the rest of the wave (one migrate per wave, one failover per shard, a
  restart's kill). Same errors in the same order.
- The names that `components/checks.py`, `command_metadata.py`, `tools.py` and `server_stats.py` take from
  `command_sequence.py`, `checks.py` and `command_table.py` are public and listed in each module's `__all__`
  (`command_sequence.free_ports`, `port_block`, `PRIVATE_SERVER_DEFAULTS`, `Side`, `compare`, `run_sequence`,
  `reply_at`, `step_text`, `validate_sequence_command`, `without_index`, `is_binary`, `text`, `SUBSCRIBE`,
  `UNSUBSCRIBE`; `checks.ask`, `words`, `sets_replication`, `limits_memory`, `BEFORE_ACL`, `QUICK_SYNC`,
  `RUNS_OTHERS`, `SYNC_TIMEOUT_S`; `command_table.pairs`), where they were underscore-private names reached across
  the module boundary. Names used only inside their own module keep the underscore. No behaviour changes.
