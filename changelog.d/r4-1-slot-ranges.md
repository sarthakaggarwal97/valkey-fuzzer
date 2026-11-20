### Changed

- The cluster state checks keep hash slots as the `(first, last)` ranges CLUSTER NODES reports instead of one
  integer per slot: `TopologyNode.slots` holds ranges (with `slot_count` and `owns(slot)`), `split_slots` finds
  split slots by range overlap, and slot coverage reads the gaps of the merged range list. Every check's result and
  message is unchanged; one validation attempt's slot checks drop from about 110 ms to under 1 ms on 6 nodes and
  from about 1 s to under 15 ms on 60.
- The data-consistency check reads its test keys back from the primaries over one pipeline (one round trip per
  primary) instead of one GET per key, as it already did from the replicas.
