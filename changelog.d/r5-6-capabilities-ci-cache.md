### Changed

- `valkey.capability.host-probe` takes two more requirements, `libfaketime` (the library `valkey.fault.clock-skew`
  preloads, or `VALKEY_FUZZER_LIBFAKETIME`) and `cgroup_v2` (a delegated cgroup v2 of the run's own with the `cpu`
  and `memory` controllers), the checks `capabilities.libfaketime_supported` and `cgroup_v2_supported` already
  made for the tests. `valkey.clock-skew-expiry` requires `[linux, libfaketime]` and
  `valkey.replication-cpu-starvation` `[linux, cgroup_v2]`: on a host without the library or the cgroup they
  report `SKIP_UNSUPPORTED` with the reason, as the tmpfs, iptables, and netem experiments do, where the faults'
  own probes made them an `ERROR`. Those probes stay: a scenario that uses either fault without the host-probe
  is still an `ERROR` naming the component.
- The runner probes the capabilities before the components' own host probes (`probe=` of a registration), so a
  host-probe's `SKIP_UNSUPPORTED` is what a run reports when both would refuse; a component probe's refusal is an
  `ERROR` of phase `preflight`, as before. `validate` probes both as before.
- The nightly live workflow caches its Valkey 8.1 build (`actions/cache/restore` and `save`, keyed on
  `git ls-remote refs/heads/8.1`, saved right after the build) and builds it only when the branch tip moved;
  unstable is still cloned and built every night.
