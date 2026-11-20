### Removed

- The config-rename machinery of `deployment/local/server_config.py` (`RENAMED_CONFIGS`, `knows_config`,
  `config_for_build`), whose table has been empty since the forkless and compression experiments left: every
  server start and the build probe's `config_get` and `config_set` now use the config name as written, which
  is what they did with an empty table. `arguments_for_build` still replaces or drops an unobservable config
  the build refuses.
- `ValkeyDeploymentService.execute_command` and its per-instance client cache; no component called it, every
  component goes through `new_client`.
- The `valkey.full-sync` experiment fragment, which no shipped experiment or example extended (the
  full-resync experiment forces its full sync with a backlog overflow, not `DEBUG CHANGE-REPL-ID`).
- The `server-stats` oracle's two names nothing ever produced: the `directory.read-only` obligation kind
  (no component opens one; `disk.filled` is the storage change) and the `valkey.replication.replicaof`
  evidence (no component declares it; the replication model's `replication.replicaof` log event is a
  different thing and stays).
- `Target.meaning` of a spec bundle's claims, read by nothing; `claims.yaml` still documents each target's
  `meaning`, and the loader still requires it.
