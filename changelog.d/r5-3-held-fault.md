### Changed

- `HeldFault` in `components/faults.py` carries the "held change, released on an event, after `duration_s`, or at
  recovery" lifecycle once: the release lock, the close at recovery, the one early release and its `<id>.released`
  (or `<id>.healed`) event, and the record of when the change was released, why (`event`, `duration`, `recover`),
  and for how long it held. `valkey.fault.clock-skew` and `valkey.fault.resource-pressure` sit on it in place of
  the copy each carried, and the network partition and degradation's shared heal implementation is a thin subclass
  of it that keeps its all-or-nothing installation and its own `<id>.partitioned` / `<id>.degraded` event. Every
  fault's obligations, events, record fields, release reasons, and error texts are unchanged.
