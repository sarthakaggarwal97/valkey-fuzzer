### Changed

- A spec bundle's `claims.yaml` may list a target without a `meaning`: the loader read nothing from the field
  (`Target.meaning` left in G01) and still required it. The shipped bundle keeps its `meaning` as documentation.
- A bundle file the loader cannot read is named again in the error (`cannot read <bundle>/model.tla: ...`); since
  G07 the error named only the bundle directory.
- The syscall-fault watcher looks for its rule's action every 10 ms instead of 2: the library reloads its rules at
  most once a millisecond and the record's `applied_at` comes from the library's own line, so the fivefold fewer
  `stat()` calls cost the record nothing.
- Small cuts with no change in output: `check_cluster_status` keeps one name for its acceptable-states set; the
  failover passes `skip_ports` to `get_current_nodes` directly; `/proc/mounts` has one reader
  (`hoststate.list_mounts`, which the tmpfs size check now uses); `parse_cluster_view` takes
  `parse_slot_ranges`'s tuples (every JSON record of a view is the same bytes); the CLI description and the
  runner's docstring no longer say "generic" or "deployment-neutral", and `wave_evidence` says the three chaos
  actions, not "both".
