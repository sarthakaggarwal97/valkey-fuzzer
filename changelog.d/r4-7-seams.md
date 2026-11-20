### Changed

- A batch plug-in carries its own options into every worker's task: `BatchPlugin.task_options(options)` returns
  them by names of its own, the pool merges every plug-in's into the task, and `BatchOptions.check` asks each
  plug-in to check them, so a value a plug-in cannot run under is refused before the batch starts. Spec
  conformance's `conform_tlc_timeout_s` (still a batch option) is the first; the batch core no longer names it.
  A task's contents are unchanged.
- The RDB snapshot oracle decides which components of a plan can lose acknowledged writes (and so cannot be
  paired with its `workload` cut check) by what they declare: a `process.stopped` change, or
  `valkey.failover.result` or `valkey.chaos.schedule` evidence, instead of a list of component ids. A pause
  (SIGSTOP), a syscall fault that does not kill, and a failover that is not forced still lose nothing.
  `valkey.action.cluster-election` and `valkey.action.slot-migration-chaos`, which kill nodes and were missing
  from the list, are now refused with the cut check; every component the list named is classed as before.
