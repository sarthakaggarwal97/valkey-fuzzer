# Threat model

What the fuzzer does to the host it runs on, what keeps each action inside
the run that asked for it, what runs as root and why, what an experiment file
can make it do, and what is left to the operator. Every claim here is read
from the code named beside it. The short version is the README's
[Safety](../README.md#safety) section: use a disposable host.

- [What it does to the host](#what-it-does-to-the-host)
- [Scoping guards](#scoping-guards)
- [What runs as root, and why](#what-runs-as-root-and-why)
- [A hostile experiment file](#a-hostile-experiment-file)
- [CI trust](#ci-trust)
- [What is left to you](#what-is-left-to-you)

## What it does to the host

Every change a component makes to the host is a declared **state change**
with an undo obligation ([Fault ownership](contracts.md#fault-ownership));
the catalog lists them, so this command is the complete list of who changes
what:

```bash
valkey-fuzzer list components --format json | jq -r '.[] | select(.declares.changes | length > 0) | "\(.component_id)\t\(.declares.changes | join(", "))"'
```

| Change | Mechanism | Needs | Code |
| --- | --- | --- | --- |
| **Servers.** Starts `valkey-server` processes from the binary you name, `PATH`, or its own build under `/tmp/valkey-build`; each binds one loopback address (`127.0.0.1`, or `127.0.B.N` with per-instance addressing), `protected-mode no`, no password, `maxmemory 500mb`, and a process title that names the fuzzer process as owner. It refuses a port already in use. At the end: `SIGTERM`, then `SIGKILL` after 5 s, and the data directory is deleted (logs stay). | `subprocess.Popen`, signals | nothing | `deployment/local/processes.py` (`build_instance_command`, `_stop_process`), `deployment/local/server_config.py` |
| **Private servers.** Capability probes, `valkey.action.commands`, `valkey.action.tools`, and the `rdb-snapshot`, `dataset-digest`, and `command-metadata` oracles start short-lived servers on a Unix socket (`--port 0`, `--unixsocketperm 700`) in a `/tmp/vfp-*` directory of their own. | as above | nothing | `components/server.py` (`PrivateServer`) |
| **Signals** (`process.stopped`, `process.paused`). `SIGKILL`, `SIGTERM`, `SIGSTOP`, `SIGCONT` to the run's own servers; a kill also kills the children the server forked (an RDB save, an AOF rewrite), read from `/proc/<pid>/task/*/children`, or from a `ps` listing where `/proc` lacks it; the server is `SIGSTOP`ped first so it cannot reap a child before the kill lands. | `os.kill` | nothing | `components/faults.py`, `components/processes.py`, `deployment/local/processes.py` (`kill_server`) |
| **Partitions** (`network.partition`). One iptables chain `VF-<owner>-<hash>` per fault with `DROP` rules in both directions between the exact loopback addresses of the isolated instances and their peers, jumped to from the top of `INPUT`; the chain and the jump are removed at heal or recovery. | `iptables` | root; `deployment.addressing: per_instance_loopback` | `components/network.py` (`drop_between`, `remove_partition`), `hoststate.py` (`remove_chain`) |
| **Degraded links** (`network.degrade`). An `ifb` device `vf<pid><start><n>` of the fault's own with a netem qdisc (loss, delay, reorder, duplicate, rate); `flower` filters on `lo`'s ingress redirect to it only TCP packets whose source and destination are both an instance's exact address. The ingress qdisc on `lo` is added only when `lo` has none, marked as the fuzzer's, and removed by whoever removes the last filter; one you added stays. | `tc`, `ip` | `CAP_NET_ADMIN`; kernel modules `ifb`, `sch_netem`, `cls_flower`, `act_mirred` | `components/network.py` (`Degradation`), `hoststate.py` (`ensure_lo_ingress`, `release_lo_ingress`) |
| **Resource pressure** (`cgroup.limited`). Moves every process of the fuzzer's own cgroup `D` into `D/vf-harness`, enables `cpu` and `memory` in `D/cgroup.subtree_control`, and moves one server into a leaf `D/vf-<owner>-<hash>` with `cpu.max` or `memory.max`. Release moves it back and removes the leaf; the harness leaf and the enabled controllers stay with `D`. Nothing is written outside `D`. | `/sys/fs/cgroup` writes | a delegated cgroup v2 ([what the host needs](operations.md#what-the-host-needs)) | `cgroups.py` (`delegated`, `enter`, `limit`, `remove_leaf`), `components/resource_pressure.py` |
| **tmpfs mounts** (`valkey.resource.tmpfs`) and **disk fill** (`disk.filled`). `mount -t tmpfs -o size=<bytes>,mode=0755` under `<tmp>/valkey-fuzzer-mounts/<owner>/`, instance data on it, then files written until `ENOSPC`. Unmounted and removed at cleanup, retried while busy. | `mount`, `umount` | root | `components/providers.py` (`TmpfsProvider`), `components/storage.py` |
| **Syscall faults** (`syscall.rule`). With `deployment.syscall_faults: true`, every server of the deployment starts with a library the fuzzer compiles from `syscall_faults.c` (`cc -O2 -shared -fPIC`) in `LD_PRELOAD`. It delays, fails with an errno, or `SIGKILL`s its own process at the n-th `write`, `writev`, `fsync`, `fdatasync`, or `rename` that matches a rule file the fault writes beside the instance's data directory. No `ptrace`; nothing outside the server's process. | `LD_PRELOAD` | a C compiler | `deployment/local/syscall_faults.py`, `syscall_faults.c`, `components/syscall_faults.py` |
| **Clock skew** (`clock.skewed`). With `deployment.clock_faults: true`, every server starts with the distribution's libfaketime in `LD_PRELOAD`, reading a timestamp file of its own; the fault rewrites one instance's file (`+<offset> x<rate>`) and restores `+0 x1`. Only that process's wall clock moves (`DONT_FAKE_MONOTONIC`); the host clock is never touched. | `LD_PRELOAD` | libfaketime | `deployment/local/clock_faults.py`, `components/clock_skew.py` |
| **Server config and client pause** (`server.config`, `server.client-pause`). `CONFIG SET` of a setting the chaos action restores, `CLIENT PAUSE` lifted at recovery. | Valkey commands | nothing | `components/replication_chaos.py` |
| **Files.** Report folders under `<temp dir>/valkey-fuzzer/`, instance data and logs under `/tmp/valkey-fuzzer/` (`base_data_dir`), a source build under `/tmp/valkey-build`, locks under `/tmp/valkey-fuzzer-locks`. The shared roots are made `1777` like `/tmp`; a run's own folders `0700`. | file writes | nothing | `hoststate.py` (`shared_directory`, `new_run_folder`) |
| **Tools.** `valkey.action.tools` runs a build's `valkey-cli`, `valkey-benchmark`, `valkey-check-rdb`, or `valkey-check-aof` against a private server, with `VALKEYCLI_*` and `REDISCLI_*` stripped from the environment and the run stopped at 4 GB resident; `valkey.workload.benchmark` runs `valkey-benchmark` against the deployment. | `subprocess` | nothing | `components/tools.py`, `components/workloads.py` |
| **Network.** Without a `valkey-server`, most experiments `git clone` Valkey 9.1.3 from GitHub and build it (refused when the tag no longer names the pinned commit). Nothing else leaves the host. | `git`, `make` | network | `processes.py` (`build_valkey`) |

## Scoping guards

These are what keeps a fault inside its run. Most came with the host-safety
work in [#176](https://github.com/sarthakaggarwal97/valkey-fuzzer/pull/176).

- **Loopback only.** A partition or degradation refuses, before any
  `iptables` or `tc` call, an address that is not a loopback IP, two
  instances with one address, or a pair that is one address; a degradation
  also refuses IPv6 and `127.0.0.1` itself, the harness's own address. So a
  rule can never match the operator's traffic, and `127.0.0.1` with itself
  (which would drop every loopback packet on the host) cannot be written
  (`network.py`: `_refuse_unsafe_addresses`, `_refuse_unsafe_links`).
- **Owner tags.** Every server title, chain name, netem device, cgroup leaf,
  mount point, and data directory names its owner `<pid>-<start>`, the
  fuzzer's PID and a hash of its start time, so a reused PID is not taken for
  the owner. A directory tag also records the owner's PID namespace: an owner
  in another namespace (a container sharing `/tmp`) is taken to be alive and
  never swept. Chain, device, and cgroup leaf names have no room for the
  namespace and are judged by PID alone (`hoststate.py`: `Owner`,
  `tag_directory`).
- **Sweep rules.** `valkey-fuzzer sweep` touches only tagged items whose
  owner is gone, among the sweeping user's own processes; never a sticky
  shared directory whatever tag it holds, never a directory whose tag file
  another user wrote, never one that reaches into another filesystem. It
  re-reads a server's tag right before the kill and a directory's right before
  the removal, and opens directories with `O_NOFOLLOW` so a swapped-in
  symlink is not followed ([sweep](cli.md#sweep); `hoststate.py`:
  `sweep`, `_remove_directory`, `_make_removable`).
- **`/tmp` trust.** A shared directory that already exists is used only if it
  is a real directory (not a symlink), owned by you or root, and sticky when
  group- or world-writable; otherwise the run refuses it by name. Anyone can
  pre-create `/tmp/valkey-fuzzer`, and a run as root would write wherever a
  symlink there pointed (`hoststate.py`: `shared_directory`). Lock files are
  opened `O_NOFOLLOW` and never created over an existing one
  (`open_shared_lock`).
- **The fill stays on the tmpfs.** `valkey.fault.disk-fill` fills only a
  path that is the `{$ref: resources.<id>.path}` of a `valkey.resource.tmpfs`
  this run provisioned, still a tmpfs mount, no larger than it was mounted,
  and that an instance's data lives on; anything else is refused and the run
  is `ERROR` (`storage.py`: `_validate_fill`, `_tmpfs_limit`).
- **The cgroup is the run's.** Resource pressure refuses the root cgroup, a
  cgroup not writable by this user, one that holds another user's process,
  or a hybrid or v1 host, both when the plan is checked and again when the
  fault fires (`cgroups.py`: `delegated`).
- **Managed server options.** `bind`, `port`, `dir`, `logfile`, `pidfile`,
  `daemonize`, `protected-mode`, `proc-title-template`, `unixsocket`,
  `replicaof`, the announce options, and `include` are set by the deployment
  and rejected in a scenario's `server_config` and in `CONFIG SET` of a
  `valkey.action.commands` sequence; the config-churn workload never draws
  them (`server_config.py`: `MANAGED_SERVER_OPTIONS`; `config_space.py`).
- **Cleanup that outlives the run.** A run stopped by Ctrl-C or `SIGTERM`
  still undoes every open obligation and stops its servers; a batch sweeps
  each worker's leftovers after every job and refuses to start while a dead
  batch's chain or mount cannot be removed ([Deadlines and
  cancellation](contracts.md#deadlines-and-cancellation),
  [isolation](cli.md#isolation-deadlines-and-leaks)).

## What runs as root, and why

The fuzzer never calls `sudo`. It needs root only for `mount`/`umount`
(`valkey.resource.tmpfs`) and `iptables` (`valkey.fault.network-partition`),
and in practice for `tc`/`ip` (`CAP_NET_ADMIN`); `valkey.capability.host-probe`
turns a run without it into `SKIP_UNSUPPORTED` before anything starts
(`providers.py`: `as_root`, `tmpfs_supported`). Nothing drops privileges:
when the fuzzer runs as root, every `valkey-server` it starts, every private
server, and every tool runs as root too. The sweep lists chains and removes
netem devices and mounts only as root.

In CI, `fuzzer-pr-batch.yml` runs its batch as root so the tmpfs and
iptables experiments run, and `valkey-experiments-live.yml` runs only the two
root-only live tests as root, in a separate step
([CI workflows](cluster-chaos-and-ci.md#scheduled-and-repository-workflows)).

## A hostile experiment file

A scenario or experiment YAML is configuration for code that runs as you.
Treat one from elsewhere like a script: `validate` compiles it without
starting anything; `run` does what it says. What it can do:

| It can | Through |
| --- | --- |
| run any executable as the fuzzer's user | `deployment.valkey_binary`, `instance_binaries`, `valkey.artifact.local-binary` |
| load any shared object into a server | `deployment.server_config.loadmodule`, `valkey.artifact.local-module` |
| send any command to the servers it started, `CONFIG SET dir` + `SAVE` (write a file where the server's user may), `DEBUG`, `ACL`, `SHUTDOWN` included | `valkey.action.command` (no refusals; `valkey.workload.command-table` skips admin commands, `FLUSHALL`, `MIGRATE`, and the like) |
| write under any directory the fuzzer's user may | `deployment.base_data_dir`, `instance_data_roots`, `valkey.resource.tmpdir` `parent`, `--artifact-dir` |
| hold the ports and cores it names, for the run's deadline | `base_port`, a 96-instance cluster, `timeout_s` |

What refuses or bounds it: the managed server options above; the loopback,
tmpfs, and cgroup guards (a scenario cannot partition, fill, or limit
anything that is not its own); `valkey.action.commands` refuses `SHUTDOWN`,
`MONITOR`, `DEBUG RESTART|CRASH-AND-RECOVER|RELOAD`, `MODULE LOAD|LOADEX|UNLOAD`,
`CONFIG SET` of a managed option, and topology commands on the deployment's
instances (`command_sequence.py`: `_refusal`); every run has a deadline and
every batch worker an escalation to `SIGKILL`
([Budgets and timeouts](operations.md#budgets-and-timeouts)); every
component's config is checked against its schema when the scenario compiles,
and an unknown field, component, or experiment is a compile error. None of
that makes a file from a stranger safe to `run`: read it first, and run it
as an unprivileged user on a host you can throw away.

## CI trust

The GitHub workflows build untrusted code (a PR's Valkey) and take
scenarios, experiments, and spec bundles as inputs, and run them as root on a
runner. The [trust model](cluster-chaos-and-ci.md#trust-model) of the PR
fuzzer says which job holds the write token and which holds PR code (never
the same one), and [Scheduled and repository
workflows](cluster-chaos-and-ci.md#scheduled-and-repository-workflows) what
each workflow accepts and refuses: inputs reach the scripts only through
environment variables, a scenario that names a binary or an artifact of its
own is refused, experiments must be `proposed.*`, every action is pinned to a
commit SHA and every downloaded jar and `.deb` checked against a pinned
SHA-256, and `tests/test_ci_workflows.py` guards each of these. A caller that
reuses `valkey-pr-fuzzer.yml` pins both the workflow and `fuzzer_ref` to one
reviewed commit ([Label trigger](cluster-chaos-and-ci.md#label-trigger)).

## What is left to you

- **Any local user can talk to the servers.** They listen on loopback with
  `protected-mode no` and no password. A fuzzer run as root therefore lets any local user make root write a file
  (`CONFIG SET dir`, `dbfilename`, `SAVE`). Run as root only on a host where
  you are the only user.
- **`/tmp/valkey-build` is shared.** A run with no `--valkey-binary` and no
  `valkey-server` on `PATH` executes `/tmp/valkey-build/valkey/src/valkey-server`
  if it exists, whoever built it; the directory is `1777`, and only the build
  itself is locked. On a shared host pass `--valkey-binary`.
- **A `SIGKILL`ed fuzzer cleans nothing up.** Its servers keep running and its
  chains, devices, leaves, and mounts stay until `valkey-fuzzer sweep` (as
  root for the chains, devices, and mounts). A partition left behind drops
  traffic between two loopback addresses of the dead run's block only.
- **Memory is not bounded by the fuzzer.** Each server has `maxmemory 500mb`
  by default, but a draw of 96 instances may take 48 GB, and a sanitizer
  build more ([Disk, ports, cores, and memory](operations.md#disk-ports-cores-and-memory)).
- **The build is trusted as given.** `--valkey-binary`, an artifact's path,
  and a module's path are run as named; `valkey.artifact.local-binary` can
  pin a version or SHA (`expected_version`, `expected_sha`), nothing checks
  one otherwise.
- **One namespace.** The sweep judges chains, netem devices, and cgroup
  leaves by PID alone. A fuzzer in a container that shares the host's network namespace but has
  its own PID namespace tags its chains with PIDs the host's sweep reads in
  the wrong namespace: it may remove a live run's chain, or keep a dead one's.
  Give such containers their own network namespace, or sweep from inside
  them.
