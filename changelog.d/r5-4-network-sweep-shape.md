### Changed

- `valkey.fault.network-degrade` (and the `degrade` wave operation) adds its redirect filters in one
  `tc -force -batch -` process after the one call that claims the fault's `pref`, instead of one `tc filter add`
  process per flow: a degradation of one instance against five peers starts 6 processes instead of 14. An error
  names the batch line and the filter that failed (`tc -force -batch - (line N: filter add ...): ...`) where it
  named the single `tc filter add` before.
- `valkey.fault.network-partition` (and the `partition` wave operation) adds its DROP rules and the jump from
  INPUT in one `iptables-restore --noflush` commit after `iptables -N` creates the chain, instead of one
  `iptables -A` process per rule and one for the jump: a partition of one node in a six-node cluster starts 2
  processes instead of 12, and the rules land together with the jump or not at all. An error names the restore's
  line and the rule that failed (`iptables-restore -w -n (line N: -A ...): ...`) where it named the single
  `iptables -A` (or `cannot install jump`) before.
- One `hoststate.remove_degradation(device, prefs)` removes a degradation for both the fault's own undo and the
  sweep: the `lo` filters first, then `lo`'s ingress qdisc once the fuzzer added it and no filter is left, then
  the device, the first two under the `lo` ingress lock. Same steps, same order, written once.
- `valkey-fuzzer sweep` finds leftover iptables chains and netem devices through `_find_chains` and
  `_find_netem_devices`, shaped like its other `_find_*` helpers; same leftovers, same `unchecked` notes.
