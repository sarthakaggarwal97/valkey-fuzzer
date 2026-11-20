### Changed

- One home per helper the components and the local deployment had copied, each in `deployment/` so that the
  deployment never imports a component: `--version` is run and parsed once (`processes.version_output`,
  `parse_version_output`; `detect_valkey_sha`, the binary provenance and the zoo's build check use them), one
  INFO parser (`queries.parse_info_text`, formerly `parse_cluster_info`) reads INFO and CLUSTER INFO replies line
  by line, so a value holding a space (an `executable` path) is read whole where `info_fields` cut it at the
  space, one renderer (`server_config.build_server_config_arguments`) turns a config mapping into server
  arguments, and `close_quietly` lives beside the deployment's clients. The capability probe resolves the default
  binary as the deployment does (`installed_valkey_binary`) and renders its `startup_config` as the deployment
  does, so a null value is refused instead of being sent as the text `None`.
- The components share `running_instances` and `read_each` (one client per running instance, polled within one
  budget) instead of five copies of the read closure, `shard_primary`, `validate_regex`, `keyspace.batches`,
  `keyspace.exact` for bytes, `command_entries` for the `COMMAND` reply walk, and the preload environment of the
  syscall-fault and libfaketime libraries (`syscall_faults.preload`). A `valkey.action.tools` `ignore` pattern
  that is not a regular expression is reported as "not a valid regex", as every other pattern is. No evidence,
  record or report changes.
