# Operations

This page is for whoever runs the fuzzer for hours on a host they own: what
the host needs, how to start a soak, where its results go, what it costs in
disk and ports, how to stop it, and the budget and timeout rules. Running one
scenario is in [Getting started](getting-started.md); every option is in the
[command-line reference](cli.md); what a failure means is in
[Triage](triage.md).

- [What the host needs](#what-the-host-needs)
- [Run a soak](#run-a-soak)
- [Where the results go](#where-the-results-go)
- [Disk, ports, cores, and memory](#disk-ports-cores-and-memory)
- [Stop and resume](#stop-and-resume)
- [Clean up after a killed fuzzer](#clean-up-after-a-killed-fuzzer)
- [Budgets and timeouts](#budgets-and-timeouts)
- [Logs](#logs)

## What the host needs

A disposable Linux host or VM (see [Safety](../README.md#safety) and the
[threat model](threat-model.md)). The base needs are in the README's
[Before you start](../README.md#before-you-start): Python 3.10 or later,
a `valkey-server` build, `porcupine` on `PATH`. Each fault that touches the
host checks for what it needs before anything starts, and a run without it
ends as below rather than failing:

| Experiment needs | For | Without it |
| --- | --- | --- |
| root, `mount`, `umount` | `valkey.resource.tmpfs` and `valkey.fault.disk-fill` (`valkey.bgsave-disk-full`) | `SKIP_UNSUPPORTED`: `the requested environment is unsupported` |
| root, `iptables` | `valkey.fault.network-partition` (`valkey.cluster-partition`, the partitions of `valkey.replication-chaos`) | `SKIP_UNSUPPORTED`: `required capability 'host' is unsupported: requires root; network partitions require root` |
| `CAP_NET_ADMIN`, `tc`, `ip` (iproute2), and the kernel modules `ifb`, `sch_netem`, `cls_flower`, `act_mirred` | `valkey.fault.network-degrade` (`valkey.replication-degraded-link`, `valkey.cluster-gossip-delay`) | `SKIP_UNSUPPORTED`: `required capability 'host' is unsupported: tc and ip not installed (iproute2); network degradation needs tc netem` |
| a delegated cgroup v2: the fuzzer's own cgroup under `/sys/fs/cgroup`, not the root one, writable, with the `cpu` and `memory` controllers, holding only this user's processes (`systemd-run --user --scope valkey-fuzzer ...`, or a slice an administrator delegated) | `valkey.fault.resource-pressure` (`valkey.replication-cpu-starvation`) | `ERROR`: `component 'starve' (valkey.fault.resource-pressure) cannot run on this host: cgroup v2 is not mounted at /sys/fs/cgroup (a cgroup v1 or hybrid host)`, or the reason the cgroup is not the run's |
| libfaketime (`apt install libfaketime`, `dnf install libfaketime`, or `VALKEY_FUZZER_LIBFAKETIME=/path/to/libfaketime.so.1`) | `valkey.fault.clock-skew` (`valkey.clock-skew-expiry`) | `ERROR`: `component 'skew' (valkey.fault.clock-skew) cannot run on this host: libfaketime is not installed (...)` |
| a C compiler (`$CC`, else `cc`) | `deployment.syscall_faults` (`valkey.save-crash-points`) builds the preload library | `ERROR`: `cannot build the syscall fault library` |
| Java 11 and `tla2tools.jar` (`TLA2TOOLS_JAR`) | the spec conform check of runs whose experiment names a bundle | the check is `skipped` for a draft bundle, `ERROR` for an approved one |
| a second build, declared as an `artifacts` entry in a `--base` scenario (see `examples/valkey/rolling_upgrade.yaml`) | `valkey.mixed-version-cluster`, `valkey.rolling-upgrade` | `ERROR`: `scenario needs 2 distinct builds, the deployment provides 1` |

The fuzzer never asks for `sudo`: run it as root for the root-only
experiments, or leave them out. A soak that lacks a need loses only those
experiments; the rest run. Check a host with the example scenario of each:

```bash
valkey-fuzzer run examples/valkey/bgsave_disk_full.yaml
valkey-fuzzer run examples/valkey/replication_degraded_link.yaml
valkey-fuzzer run examples/valkey/replication_cpu_starvation.yaml
valkey-fuzzer run examples/valkey/clock_skew_expiry.yaml
```

## Run a soak

A soak is a [batch](cli.md#batch) of random seeds under the `nightly-soak`
profile: every experiment's whole legal space, 1800 seconds per run. One
batch runs on a host at a time (it holds `/tmp/valkey-fuzzer-locks/batch`).

```bash
valkey-fuzzer batch --random all --seeds 1 --profile nightly-soak --workers 8 \
  --exclude experiment=valkey.mixed-version-cluster --exclude experiment=valkey.rolling-upgrade \
  --prune-pass --serial-reruns 1 --out "soak/$(date -u +%Y%m%dT%H%M)"
```

- `--random all --seeds N`: `N` fresh seeds of every experiment that
  declares a generator (19 today). Left out, `--seeds` is the profile's
  budget, 50 per experiment. Repeat `--random ID` to soak a few experiments.
- `--exclude experiment=...`: the two experiments that need a second build
  (above); give them one with `--base` instead when you have it.
- `--prune-pass`: a run that passed keeps `report.json` and
  `scenario.yaml` only. Its ledgers (megabytes per run) are the bulk of a
  batch folder, and a passing run's are never read again.
- `--serial-reruns 1`: after the parallel runs, every `FAIL` and `ERROR`
  signature runs once more alone, so the summary says whether load explains
  it ([Triage](triage.md#was-it-load)).
- `--out DIR`: the batch folder. Without it, under `<temp dir>/valkey-fuzzer/batches/`.

The same soak as a batch spec, which a timer can run unchanged
(`valkey-fuzzer batch soak.yaml --out "soak/$(date -u +%Y%m%dT%H%M)"`):

```yaml
# soak.yaml
batch_id: soak
profile: nightly-soak
workers: 8
timeout_s: 1800
prune_pass: true
serial_reruns: 1
sources:
  - random:
      experiments: all
      seeds: 1
      excludes: [experiment=valkey.mixed-version-cluster, experiment=valkey.rolling-upgrade]
```

Add `--dry-run` to either command to list the jobs and run nothing. The exit
code is the batch's worst result (`1` any `FAIL`, `2` any `ERROR` or
leftover, `3` any `SKIP_UNSUPPORTED`, `130` stopped), so a script can act on
it; `--allow-unsupported` turns `3` into `0` on a host that lacks the
root-only experiments on purpose. The bug zoo (`--zoo BUILDS`) can be a
source of the same batch, so one soak also proves the oracles still catch
the known bugs ([Bug zoo](bug-zoo.md#running)).

## Where the results go

The batch prints one line per finished job and, at the end, the counts, the
failure signatures with a run of each, and `Batch: <folder>`. The folder
([layout](cli.md#the-batch-folder)) holds `summary.json`, `batch.log`, and
`runs/<job id>/` with each run's `report.json`, `scenario.yaml`, and (unless
pruned) `components/` and `logs/`. Read the summary first:

```bash
jq '{status, exit_code, complete, counts, leaks: (.leaks | length), signatures: (.signatures | length)}' soak/*/summary.json
```

`status` is the worst result; `signatures` groups every run that did not
pass; `leaks` is what runs left on the host and the batch removed (each one
is an `ERROR` for its run). A `SKIP_UNSUPPORTED` row names what the host
lacks, so a soak's first summary is also the host check. From there,
[Triage](triage.md).

## Disk, ports, cores, and memory

**Disk.** Measured on this host with Valkey 9.1.3: a passing run's folder is
about 150 KB with `--prune-pass` (the report alone) and 1 to 12 MB without
(write ledgers and INFO samples under `components/`, growing with the
workload's operations); a 19-job soak batch with `--prune-pass` is 2.3 MB. Instance logs are kept outside the batch folder,
under `/tmp/valkey-fuzzer/logs/` (`base_data_dir`), about 2 KB per instance
per run plus whatever the server logs; `valkey-fuzzer cleanup-logs SEED
--force` removes one seed's, and they are safe to delete by age. The first run
without a `valkey-server` builds one under `/tmp/valkey-build` (278 MB). Put
batch folders on a disk you can fill: `--out`, or `--artifact-dir DIR` for
every default folder.

**Ports.** A batch with `W` workers uses client ports `port_base` to
`port_base + 120W - 1` (default `port_base` 11000), their cluster bus ports
10,000 higher, and the loopback blocks `127.0.1.0/24` to `127.0.W.0/24`. All
of them must stay below the host's ephemeral range (Linux: 32768 by default),
so `W` is at most 83 and `--base-port` at most `22768 - 120W`; the batch
refuses anything else before it starts ([isolation](cli.md#isolation-deadlines-and-leaks)).
A hand-run scenario uses its own `base_port` (7000 and up in the examples)
and refuses a port in use.

**Cores.** A random job's deployment is drawn to fit its worker's share of the
cores the batch may use: `cores / workers`, printed as `each run may use 6.0
of the batch's 48 cores (8 worker(s))`. More workers mean smaller clusters
per run and a different scenario for the same seed, so a bug report names
the seed with its `cores` and `attempt`
([Seeds](contracts.md#seeds-replay-and-provenance)). The CI workflows use 2
workers on a 4-core runner and 1 for a sanitizer build; 8 workers on 48 cores
ran the soak above.

**Memory.** Every instance starts with `maxmemory 500mb` unless the scenario
says otherwise, and a `nightly-soak` cluster draw has up to 16 primaries with
5 replicas each. Nothing sizes draws to RAM: the worst case is every
instance at `maxmemory`, 48 GB for a 96-instance draw. Watch `free` during
the first soak, and lower `--workers` or pass `--base` with a smaller
`deployment` (`replicas_per_primary`, `primaries`) if it runs short. An ASan
build holds more per server
([`quarantine_size_mb`](cluster-chaos-and-ci.md#scheduled-and-repository-workflows)).
A `valkey.action.tools` run is stopped at 4 GB resident.

## Stop and resume

Ctrl-C or `SIGTERM` to the batch process stops it cleanly: it interrupts the
running jobs (their runs stop their servers and undo their host changes),
records no result for them, sweeps what they left, prints
`Stopping: SIGTERM; interrupting the running jobs (resume to finish)`, and
exits `130`. A second Ctrl-C skips to the next signal of the escalation
below. Finish it later with:

```bash
valkey-fuzzer batch --resume soak/20261120T0808
```

A resume takes only `--workers` and `--allow-unsupported`; the folder holds
every other option, and a finished job never runs again. `SIGKILL` (or a
power loss) stops nothing cleanly: the resume first stops the workers it
finds, sweeps their leftovers, and runs their jobs again
([Resuming](cli.md#resuming)).

## Clean up after a killed fuzzer

Every server, data directory, iptables chain, netem device, cgroup leaf, and
tmpfs mount is tagged with the fuzzer process that made it. After a fuzzer
died without cleaning up, list and then remove what dead fuzzers left:

```bash
valkey-fuzzer sweep --dry-run
valkey-fuzzer sweep
```

Run it as root to also list chains and remove netem devices and mounts;
without root they are reported, not removed. The sweep never touches a
running fuzzer's state, anything untagged, or anything a fuzzer in another
PID namespace owns ([sweep](cli.md#sweep)). A batch sweeps its own workers
after every job, so a soak needs this only after the batch itself was
killed, and it refuses to start while a dead batch's chain or mount it cannot
remove is in the way.

## Budgets and timeouts

**Per run.** Every run has a deadline, printed before it starts:

| Source | Printed as | When |
| --- | --- | --- |
| `--timeout SECONDS` | `Timeout: 90s (--timeout)` | always wins |
| the search profile's `budget.timeout_s` | `Timeout: 600s (search profile 'pr-quick' budget)` | `--profile`, or a batch's `profile` |
| the default | `Timeout: 1200s (default)` | neither |
| the components' own `timeout_s` | `Timeout: 1800s (sum of the components' timeout_s, above the default of 1200s)` | `run` never cuts below the sum the components asked for |

```bash
valkey-fuzzer run examples/valkey/aof_durability.yaml --timeout 90
```

At the deadline the running call is cancelled and the run is `ERROR` with the
teardown still done ([Deadlines and cancellation](contracts.md#deadlines-and-cancellation)).
The profiles' budgets: `pr-quick` 600 s and 3 seeds, `nightly-soak` and
`adversarial` 1800 s (50 and 20 seeds), `candidate` 1200 s
([search profiles](scenarios-and-experiments.md#legal-space-and-search-profiles)).
A profile never widens what an experiment may draw; raise `--timeout` for a
host that is slow, not the profile.

**Per batch.** `timeout_s` (`--timeout`) is each run's deadline. A worker
still running 60 seconds after it (or a quarter of the deadline, if longer),
plus the `conform_tlc_timeout_s` (300 s) its spec check may take, gets
`SIGINT`, then `SIGTERM` after 60 more seconds, then `SIGKILL` to its process
group 30 seconds later, and the job is an `ERROR` (`Deadline`). So the worst
case for one job under the default soak budget is about 1800 + 450 + 300 +
90 seconds; size `--seeds` and `--workers` to the window you have, or stop
and resume.

## Logs

The fuzzer logs at `INFO` to stderr (deployment steps, each job's line) and
has no log-level option; `--verbose` on `run`, `replay`, and `minimize`
prints every component's result, evidence count, and diagnostics, the run
diagnostics (deployment, obligations, events), and a traceback when the
command itself fails:

```bash
valkey-fuzzer run examples/valkey/aof_durability.yaml --verbose
```

Each server logs at Valkey's `notice` level into its own file under
`<base_data_dir>/logs/` (in a batch, the run folder's `logs/`), with its
stdout and stderr, where sanitizer reports land, in `.stdio.log` beside it.
For more detail set the server's level in the scenario:

```yaml
deployment:
  server_config: {loglevel: debug}
```

`verbose` and `debug` grow the logs fast under load, and the crash reports
and forbidden patterns `valkey.observer.log` and `valkey.oracle.liveness`
look for are logged at `notice`, so raise the level only to read a specific
failure.
