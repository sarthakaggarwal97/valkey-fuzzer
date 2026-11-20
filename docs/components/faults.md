# Faults

Part of the [Valkey component reference](README.md): the
[index](README.md#index) lists every component, [Common config
forms](README.md#common-config-forms) the selectors, predicates, and trigger
fields the tables below use.

Every fault also takes the [trigger fields](README.md#trigger-fields).

## `valkey.fault.process-signal`

Sends a signal to one instance. After `SIGKILL` or `SIGTERM` it waits up to 30
seconds for the process to exit, sending the signal again every second, records when it was confirmed gone (`exited_at`), and, with
`restart`, starts it again. A process that did not exit leaves the fault not
applied (`ERROR`), with nothing to restart. After `SIGSTOP` it sends `SIGCONT` once `pause_s`
has passed, or as soon as the pause is cut short; if that `SIGCONT` fails,
recovery sends it again. A kill without `restart` lasts until the deployment
is removed, so it is not undone; the fault records the instance as
`valkey.instances.expected-down`, which the liveness, ledger, and
dataset-digest oracles honor.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `instance` | selector or `event` | `primary` | The first selected instance. |
| `signal` | `SIGKILL`, `SIGTERM`, or `SIGSTOP` | `SIGKILL` | |
| `restart` | boolean | `true` | Restart a killed instance. |
| `restart_mode` | `recover`, `immediate`, or `background` | `recover` | When to restart: at teardown (`recover`), inside the trigger's dispatch right after `<id>.injected` so later actions see the restarted instance (`immediate`), or on a worker thread right after `<id>.injected` so the publisher is not held up (`background`). |
| `restart_delay_s` | number, 0 to 600 | 0 | Wait before an `immediate` or `background` restart. |
| `pause_s` | number, 0 to 600 | 1 | `SIGSTOP` only. |
| `ready_timeout_s` | number, 1 to 600 | 30 | How long a restart waits for the instance to become ready; the restart fails after that. Teardown waits for a `background` restart up to this plus `restart_delay_s` plus 60 seconds. |
| `capture_cluster_vars` | boolean | `false` | Cluster mode: record the `currentEpoch` and `lastVoteEpoch` the killed node's `nodes.conf` holds once the process exited, as the restart will load them. |

Events: `<id>.injected`, then `<id>.recovered` after an `immediate` or
`background` restart that started the instance again. Evidence: `valkey.fault.injected` (target, signal, PID,
`exited_at`, restart timing, and the `nodes.conf` vars when captured).

## `valkey.fault.syscall`

Kills, delays, or fails one instance at the nth system call that matches a
filter. A kill from outside lands wherever the process happens to be, while
this one lands between two calls of one event-loop pass, or between the
writes and the rename of a save. It needs a deployment with
`syscall_faults: true`, whose servers start with the syscall fault library
(`valkey_fuzzer/deployment/local/syscall_faults.c`, built with the host's C
compiler, `$CC` or `cc`) in `LD_PRELOAD`. When the trigger fires, the fault
writes a rule to the instance's rule directory. From then on the library
counts the matching calls of the server and of its fork children (an RDB or
AOF rewrite child) and acts on the nth. A kill takes the server down, with the
child that made the call. It is a process crash: what the kernel already
holds of the server's writes survives, as it does after any `SIGKILL`. The
library wraps `write`, `writev`, `fsync`, `fdatasync`, and `rename`, so
writes through stdio (`fwrite`, which RDB and AOF-rewrite children use for
the file itself) are not counted, while their `fsync` and `rename` are.

The fault lands when the library acts, not at its trigger: `<id>.injected`
is published then, and the record's `applied_at` is that moment. A kill
waits for the server to exit, as `valkey.fault.process-signal` does, then
restarts it after `restart_delay_s` and publishes `<id>.recovered`. A rule
that never acted by teardown is removed, and the fault is `ERROR`, with the
number of matching calls it saw.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `instance` | selector or `event` | `primary` | The first selected instance. |
| `calls` | list of `write`, `fsync`, `fdatasync`, `rename` | | `write` covers `writev`. |
| `path` | string or null | `null` | An `fnmatch` pattern of the file's path, in which `*` also matches `/`. A relative pattern is under the instance's data directory (`nodes.conf.tmp-*`). A `rename` matches if either path does. `null` matches any path. |
| `peer` | selector or null | `null` | Only sockets connected to these instances' client or cluster bus port, which are the connections the server opened. `write` only. |
| `nth` | integer, 1 to 10,000,000 | 1 | |
| `action` | `kill`, `delay`, or `fail` | `kill` | |
| `when` | `before` or `after` | `before` | Kill or delay before the call, or once it returned. |
| `delay_ms` | integer, 1 to 600,000 | 1000 | For `delay`. The calling thread sleeps. |
| `errno` | errno name | `EIO` | For `fail`: the call returns -1 with it, without being made. |
| `restart` | boolean | `true` | Restart a killed instance. Without it the instance is `valkey.instances.expected-down`. |
| `restart_delay_s` | number, 0 to 600 | 0 | |
| `ready_timeout_s` | number, 1 to 600 | 30 | |
| `capture_cluster_vars` | boolean | `false` | Cluster mode: record the `nodes.conf` vars the killed node restarts with. |

Events: `<id>.injected` when the library acted (with `signal: SIGKILL` for a
kill, and the `nodes.conf` vars when captured, so `spec conform` reads it as a
crash), then `<id>.recovered`. Evidence: `valkey.fault.injected` (the rule,
`armed_at`, the call and path it acted on and the PID that made it,
`calls_matched`, and for a kill `exited_at`, `disk_vars_at_kill`, and the
restart timing).

## `valkey.fault.network-partition`

Drops every packet between each isolated instance and each peer with
iptables rules in a chain of its own, jumped to from `INPUT`: `iptables -N`
creates the chain, then one `iptables-restore --noflush` adds its rules and
the jump in one commit. Client traffic
from the harness (127.0.0.1) still reaches both sides. It needs Linux, root,
iptables, and `deployment.addressing: per_instance_loopback`; pair it with
`valkey.capability.host-probe` and `requires: [linux, root, iptables,
per_instance_loopback]`. The partition heals on `heal_event`, after
`duration_s`, or at teardown. Installing is all-or-nothing: if any iptables
step fails, the chain is removed again and the fault is not applied. Every
iptables call times out after 30 seconds. A heal that fails is not raised
into the event's dispatch; recovery removes the chain and reports the
failure. A later partition removes chains left by a process that no longer
exists.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `isolate` | selector | | |
| `peers` | selector or `others` | `others` | Every instance that is not isolated, or a selection. |
| `heal_event` | string or null | `null` | |
| `duration_s` | number, 0.01 to 3600, or null | `null` | Set `heal_event` or `duration_s`, not both. |

Events: `<id>.injected`, `<id>.partitioned`, `<id>.healed`. Evidence:
`valkey.fault.injected` (with `healed_at`), which `valkey.oracle.ledger` uses
for loss windows.

## `valkey.fault.network-degrade`

Degrades the links between the selected `instances` and their `peers` instead
of cutting them: packet loss, delay with jitter, reordering, duplication, and
a rate limit, both ways, with `tc` netem. Clients from the harness (127.0.0.1)
are not affected. It needs Linux, `tc` and `ip` (iproute2) with
`CAP_NET_ADMIN` (root has it; the fuzzer never asks for `sudo`), and
`deployment.addressing: per_instance_loopback`; pair it with
`valkey.capability.host-probe` and `requires: [linux, netem,
per_instance_loopback]`. The degradation heals on `heal_event`, after
`duration_s`, or at teardown.

What runs on the host, and why it can touch no other flow:

1. `ip link add dev vf<pid><start><n> type ifb` and `ip link set ... up`: a
   virtual device of the fault's own, named after the fuzzer process
   (`vf`, its PID in six hex digits, the six characters of its start, one
   suffix), which receives nothing by itself.
2. `tc qdisc add dev vf... root netem <loss|delay|reorder|duplicate|rate>`:
   netem is the root of that device, never of `lo`, so it queues only what is
   redirected to the device.
3. `tc qdisc add dev lo ingress`, only when `lo` has no ingress qdisc yet; a
   marker in the shared directory (`lo-ingress.added`) records that the fuzzer
   added it. The qdisc itself changes nothing: it only gives filters a place.
4. One `tc filter add dev lo ingress protocol ip pref <P> handle <k> flower
   ip_proto tcp src_ip <A> dst_ip <B> action mirred egress redirect dev vf...`
   per direction of each instance pair: the first in a `tc` call of its own
   (it claims the `pref`), the rest as the lines of one `tc -force -batch -`
   on its stdin. A filter matches a packet only when
   its source and destination are both the exact addresses of the pair, so a
   flow between two addresses of this run's instances is redirected and
   nothing else is: not the harness's clients (127.0.0.1), not another run's
   instances (its own 127.0.B.x block), not anything else on the host. Before
   any call the fault refuses an address that is not an IPv4 loopback address,
   is the harness's 127.0.0.1, or is shared by two instances. All filters of
   one fault sit at one `pref` the fault claims (an explicit handle turns a
   taken `pref` into `File exists`, so two runs never share one).

Healing removes, in order, the filters (`tc filter del dev lo ingress protocol
ip pref <P>`), the ingress qdisc if the fuzzer added it and no filter is left
on it, and the device (`ip link del`, which takes its netem with it); the
first two under a lock every fuzzer process takes, so a run never removes the
qdisc under another run's first filter. A device whose filters could not be
removed stays until they are (a filter whose device is gone would drop what it
matches); its obligation (`network.degrade`) then stays open for recovery and
the runner. Installing is all-or-nothing: if any step fails, what was
installed is removed again and the fault is not applied; a kernel that lacks
a module (`ifb`, `sch_netem`, `cls_flower`, `act_mirred`) is named in the error. A heal that fails is not
raised into the event's dispatch; recovery removes the device and reports the
failure. Every `tc` and `ip` call times out after 30 seconds. A device a
killed fuzzer left is found by its name, with the filters that redirect to it,
by [`valkey-fuzzer sweep`](../cli.md#sweep). Netem acts on loopback's large
segments (up to 64 kB) as single packets, so a loss percentage drops whole
segments, and its queue holds 1000 packets; a `rate_kbit` low enough to fill
it drops the rest.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `instances` | selector | | The instances whose links are degraded. |
| `peers` | selector or `others` | `others` | Every instance that is not selected, or a selection. |
| `loss_pct` | number, 0 to 100 | 0 | Packets dropped. |
| `delay_ms` | number, 0 to 60,000 | 0 | Added to every packet, each way. |
| `jitter_ms` | number, 0 to 60,000 | 0 | Random variation of `delay_ms`; needs `delay_ms`. |
| `reorder_pct` | number, 0 to 100 | 0 | Packets sent at once instead of delayed, so they overtake the rest; needs `delay_ms`. |
| `duplicate_pct` | number, 0 to 100 | 0 | Packets sent twice. |
| `rate_kbit` | integer, 8 to 100,000,000, or null | `null` | A rate limit. |
| `heal_event` | string or null | `null` | |
| `duration_s` | number, 0.01 to 3600, or null | `null` | Set `heal_event` or `duration_s`, not both. |

Set at least one of the five effects. Events: `<id>.injected`,
`<id>.degraded` (`instances`, `peers`, and the `netem` arguments),
`<id>.healed`. Evidence: `valkey.fault.injected` (the `device`, `pref`,
`netem` arguments, `rules`, and `healed_at`). A degradation opens no loss
window of its own: list it in `valkey.oracle.ledger`'s `loss_window_faults`
only if the experiment accepts loss under it, and set `loss_window_faults:
[]` when nothing else in the plan may lose writes.

## `valkey.fault.disk-fill`

Fills the filesystem at `path` with a ballast file until it is full, and
removes the ballast on `release_event` or at teardown. `path` must be
`{$ref: resources.<id>.path}` of a [`valkey.resource.tmpfs`](providers.md#valkeyresourcetmpfs)
resource and also a `deployment.instance_data_roots` entry, so instance data
lives on it. Before it writes, the fault checks that `path` is still that
size-limited mount, and refuses (`ERROR`) otherwise; it never fills host
disk.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `path` | reference | | |
| `leave_bytes` | integer, at least 0 | 0 | Free space to leave. |
| `max_bytes` | integer or null | `null` | Most bytes to write; the tmpfs size by default, never more. A fill that stops here before the disk is full is not applied, and neither is one whose call is cancelled. |
| `chunk_bytes` | integer, 4096 to 67,108,864 | 1,048,576 | |
| `release_event` | string or null | `null` | Free the space when this event is published. |

Events: `<id>.injected`, `<id>.released`. Evidence: `valkey.fault.injected`
(bytes written, free bytes after).

## `valkey.fault.clock-skew`

Shifts one instance's view of the wall clock, without touching the host
clock. It needs a deployment with `clock_faults: true`, whose servers start
with [libfaketime](https://github.com/wolfcw/libfaketime) in `LD_PRELOAD`,
each reading its time from a timestamp file of its own on every call
(`FAKETIME_NO_CACHE`). When the trigger fires, the fault rewrites the target's
file to `+<offset> x<rate>`: the server's wall clock then reads `offset_s`
seconds ahead of (or behind) the host's and runs `rate` times as fast. Only
the wall clock moves — `DONT_FAKE_MONOTONIC` keeps the event loop and its
timers on the host's monotonic clock — so a skew looks to the server like a
stepped or drifting system clock, not a faster event loop. It affects key
expiry, `TIME`, and the timeouts Valkey measures against the wall clock.

The library is the host's `libfaketime` package (`libfaketime.so.1` under
`/usr/lib/*/faketime`, `/usr/lib64/faketime`, `/usr/lib/faketime`, or
`/usr/local/lib/faketime`), or whatever `VALKEY_FUZZER_LIBFAKETIME` names.
Pair it with `valkey.capability.host-probe` and `requires: [linux,
libfaketime]`, as `valkey.clock-skew-expiry` does: a host without the library
is then `SKIP_UNSUPPORTED` naming the package; without the host-probe, the
fault's own probe makes the run an `ERROR` that names it.
After writing the file the fault reads `TIME` on the instance and compares it
with the host's clock: a skew the server does not show (libfaketime not
loaded) leaves the fault not applied (`ERROR`). The real time is restored on
`release_event` or at recovery, the clock measured once more before it is. A
restart keeps the instance's file, so a skewed instance restarts skewed until
the fault restores it.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `instance` | selector or `event` | `primary` | The first selected instance. |
| `offset_s` | number, -31,622,400 to 31,622,400 | 0 | Seconds the clock is ahead (positive) or behind (negative) when the fault lands. |
| `rate` | number, 0.01 to 100 | 1 | How fast the clock runs from then on, relative to the host's. |
| `release_event` | string or null | `null` | Restore the real time when this event is published. |

Set an `offset_s` other than 0, a `rate` other than 1, or both. Events:
`<id>.injected`, `<id>.released`. Evidence: `valkey.fault.injected` (target,
the spec written, the skew measured when it landed and when it was released).

## `valkey.fault.resource-pressure`

Puts cgroup v2 limits on one instance for a window, then releases them: it
moves the target's server (and the children it forks from then on) into a
cgroup leaf of the run's own with `cpu.max` from `cpu_percent` and
`memory.max` from `memory_max_mb`. The instance keeps running, slower: a CPU
quota throttles it for the rest of each period once the quota is spent (a
slow but live primary, where `valkey.fault.process-signal` with `SIGSTOP` is a
stopped one); a memory limit makes the kernel reclaim its page cache and, when
its anonymous memory alone exceeds the limit, kills it (the liveness oracle
reports that). On `release_event` or at recovery the server moves back to an
unlimited leaf, the leaf is removed, and its counters are recorded. A limit
that never bit (`cpu.stat` counted no throttled period, `memory.events` no
`max`) leaves the fault not applied (`ERROR`): the run did not test what it
meant to.

It needs a delegated cgroup v2 of the run's own (for example under
`systemd-run --user --scope valkey-fuzzer ...`): cgroup v2 mounted at
`/sys/fs/cgroup`, this process's cgroup not the root one, writable by this
user, holding the controllers the limits need and only this user's processes.
Pair it with `valkey.capability.host-probe` and `requires: [linux,
cgroup_v2]`, as `valkey.replication-cpu-starvation` does: a host without such
a cgroup is then `SKIP_UNSUPPORTED` with the reason. Without the host-probe,
the fault's probe and its firing refuse with the reason, and the run is an
`ERROR`. Nothing is ever written outside that cgroup, no process but the
run's own is moved, and a leaf a dead fuzzer left is removed by
`valkey-fuzzer sweep`. There is no fallback without cgroups: a fully stopped
instance is already `valkey.fault.process-signal` with `SIGSTOP`.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `instance` | selector or `event` | `primary` | The first selected instance. |
| `cpu_percent` | number, 0.1 to 100, or null | `null` | Percent of one CPU (100 is a whole core); `null` sets no CPU limit. |
| `memory_max_mb` | integer, 1 to 1,048,576, or null | `null` | `memory.max` in MiB; `null` sets no memory limit. |
| `release_event` | string or null | `null` | Release the limits when this event is published. |

Set `cpu_percent`, `memory_max_mb`, or both. Events: `<id>.injected`,
`<id>.released`. Evidence: `valkey.fault.injected` (target, the limits, and
the `cpu.stat`, `memory.events` and `memory.current` counters read at
release).
