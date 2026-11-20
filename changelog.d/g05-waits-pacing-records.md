### Changed

- One wait in the deployment: its spawn, restart, formation, failover, exit and sync waits call `deadlines.wait_for`
  directly; `queries.wait_until` is gone. The two waits that retried through a client error (an instance's start,
  the cluster meet) still count one as "not yet". A run deadline or a cancellation still ends every one of them
  with `WaitInterruptedError`, as before.
- One paced loop (`polling.paced_loop`) drives the key-value and command-table workloads, with the drift
  correction both had; one `polling.stop_or_error` stops a workload's workers and returns the `workload ...`
  ERROR while one still runs. The INFO observer, the slot-migration action and the slot-migration chaos follow
  hold their per-instance clients in one `instances.ClientCache`, dropped on a failed read as before.
- The ledger oracle and the data-consistency check read keys through one `queries.read_keys`: pipelines of 1000
  GETs (the ledger oracle sent 500), and a cluster replica is sent `READONLY` once per connection instead of
  once per pipeline.
- `handle.primaries()` and `handle.replicas()` are one role list each over the planned instances, as before.
  `LocalInstance` stays, with its docstring now saying why: it carries the planned roles the oracles and the
  state check reason from, while `InstanceInfo.role` follows the deployment's recorded role changes.
- `cgroups` and `valkey-fuzzer sweep` wait through `deadlines.wait_for` too: emptying a cgroup pauses 50 ms
  between rounds of moving its processes instead of spinning; the other two waits keep their timing.
