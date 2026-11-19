# valkey-fuzzer

valkey-fuzzer runs seeded fault-injection tests against Valkey. Most tests are
YAML experiments that combine workloads, actions, faults, observers, and
oracles. The same runner supports standalone and cluster deployments, with or
without replicas.

A scenario names an experiment, its inputs, a deployment, and a seed. The
runner starts local `valkey-server` processes, runs the experiment, and
reports one outcome: `PASS`, `FAIL`, `ERROR`, or `SKIP_UNSUPPORTED`.
`valkey-fuzzer run --random` generates scenarios from a seed for any
experiment, cluster chaos (failovers with process kills) included.

## Before you start

| You need | Why | If you lack it |
| --- | --- | --- |
| Linux (macOS for most experiments) | `valkey-server` processes, `SIGKILL` and `SIGSTOP`. Root and Linux only for the experiments that change the host (see [Safety](#safety)). | Those experiments report `SKIP_UNSUPPORTED`. |
| Python 3.10 or later, git | the fuzzer and its install | n/a |
| A `valkey-server` build | every experiment starts real servers | Build one (below), or pass `--valkey-binary PATH`. |
| Go 1.21 or later | builds the `porcupine` linearizability checker. These experiments need it: `valkey.aof-durability`, `valkey.cluster-partition`, `valkey.mixed-version-cluster`, `valkey.replication-chaos`, `valkey.replication-full-resync`, `valkey.rolling-upgrade`. | `validate` exits `2`, and a run ends `ERROR` before it starts anything, with `porcupine is not on PATH` and the build command. |
| Free ports | the quickstart uses 7430 and 7431; generated scenarios use ports from 7000 | The run ends `ERROR` and names the port. |

**Build Valkey.** Use the binary where it was built. A `valkey-server` copied
out of its build tree can crash at start (it cannot find its Lua module), and
the run then reports `FAIL`. The fuzzer finds a binary on `PATH`, or the build
it makes itself under `/tmp/valkey-build`:

```bash
git clone --depth 1 --branch 9.1.3 https://github.com/valkey-io/valkey.git && make -C valkey -j"$(nproc)"
export PATH="$PWD/valkey/src:$PATH"
```

That needs git, make, and a C compiler, and takes about a minute. Without a
binary, most experiments clone and build Valkey 9.1.3 for you on the first
run, in the same way, under `/tmp/valkey-build` (about 70 seconds and 280 MB),
and log one line when it starts. Four experiments never build one and
report `ERROR` instead: `valkey.cluster-election-durability`,
`valkey.cluster-vote-save-crash`, `valkey.mixed-version-cluster`, and
`valkey.rolling-upgrade`. [Getting started](docs/getting-started.md#choose-a-valkey-binary)
has the lookup order.

**Build `porcupine`.** From the `valkey-fuzzer` checkout (see the
Quickstart), put the checker on `PATH` once per host:

```bash
mkdir -p "$HOME/.local/bin"
go -C porcupine build -o "$HOME/.local/bin/porcupine"
export PATH="$HOME/.local/bin:$PATH"
```

If the build cannot reach `proxy.golang.org` (a corporate TLS proxy, for
example), run it as `GOPROXY=direct GOSUMDB=off go -C porcupine build ...`.

## Quickstart

Until this code lands in `valkey-io/valkey-fuzzer`, clone the branch
`generic-fuzzer` of the fork. The `main` branch of `valkey-io/valkey-fuzzer`
is an older layout without these docs or experiments.

```bash
git clone -b generic-fuzzer https://github.com/sarthakaggarwal97/valkey-fuzzer.git
cd valkey-fuzzer
python -m pip install -e .
```

Install in a virtual environment (`python -m venv .venv && . .venv/bin/activate`)
if your system Python refuses `pip install`. Then build `porcupine` as shown
above, from this directory.

List the installed experiments, then check a scenario without starting any
servers:

```bash
valkey-fuzzer list experiments
valkey-fuzzer validate examples/valkey/aof_durability.yaml
```

Run it, with `valkey-server` and `porcupine` on `PATH` (or add
`--valkey-binary PATH`). This starts a primary and a replica on ports 7430 and
7431, writes keys, kills the primary with `SIGKILL`, restarts it from its
append-only file, and checks that no acknowledged write was lost. It takes
about 20 seconds when a binary is ready. When it has to build Valkey first,
add about 70 seconds.

```bash
valkey-fuzzer run examples/valkey/aof_durability.yaml --output report.json
```

A green run ends like this, and the exit code is `0`:

```text
Scenario: aof-durability
Experiment: valkey.aof-durability
Status: PASS
Mode: experiment
Deployment: standalone
Duration: 16.50s
Components: 13 passed, 0 failed, 0 skipped, 0 errors
Evidence: 12
Valkey SHA: 29173b67
Summary: experiment passed
Report: .../runs/20261118T174158.601Z-aof-durability-67a126/report.json
Reproduce: valkey-fuzzer replay .../runs/20261118T174158.601Z-aof-durability-67a126/report.json

Results saved to report.json
```

The exit code is `0` for `PASS`, `1` for `FAIL`, `2` for `ERROR`, and `3` for
`SKIP_UNSUPPORTED`. `--output` saves a copy of the report. Run the recorded
execution plan again:

```bash
valkey-fuzzer replay report.json
```

Replay uses the fuzzer and binaries installed now. When the fuzzer, the experiment, the components, or the
Valkey binary differ from the recorded run, it still runs, warns, and records the difference in the report.
It does not reproduce timing.

Run one generated cluster chaos scenario (3 primaries with 1 replica each, a
process kill and a failover, about 30 seconds):

```bash
valkey-fuzzer run --seed 173 --experiment valkey.cluster-chaos
```

If a run says `FAIL`, [Reading a FAIL](docs/reading-a-fail.md) shows where its
evidence is and what to attach to a Valkey issue. [Getting
started](docs/getting-started.md) explains the output, the report files, and
what to try next.

## Commands

| Command | Does |
| --- | --- |
| `run FILE` / `run --seed N` | Run one scenario, or the one a seed draws. |
| `batch --random all --seeds 5` | Run many scenarios in parallel. |
| `validate FILE` | Check a scenario without starting anything. `--strict` also warns when only shared fragments supply the required evidence. |
| `replay REPORT` | Run a recorded run again. |
| `minimize REPORT` | Shrink a failing scenario. |

`run` and `batch` take `--base-port N` to move
the ports. Experiments that live in a directory, such as the ones in
[`examples/experiments`](examples/experiments), need `--experiment-dir DIR`;
each example's first lines show the command that runs it.

## Safety

The fuzzer acts on the machine it runs on. Use a disposable host, VM, or
container, not a server that runs anything you care about.

- It starts real `valkey-server` processes on local ports (from 7000 by
  default, bound to loopback addresses, with protected mode off), sends them
  `SIGKILL`, `SIGTERM`, and `SIGSTOP`, and restarts them. It refuses to start
  when a planned port is already in use.
- It writes instance data and logs under `/tmp/valkey-fuzzer`, reports under
  the temporary directory, and may build Valkey under `/tmp/valkey-build`.
- On Linux as root, `valkey.bgsave-disk-full` mounts a tmpfs and fills it,
  and `valkey.cluster-partition` and the partitions of
  `valkey.replication-chaos` add iptables rules. Both remove what they added
  when the run ends, but a fuzzer process that is killed can leave a mount or
  a rule behind.
- Every server, data directory, iptables chain, and tmpfs mount the fuzzer
  creates is tagged with the fuzzer process that owns it. After a fuzzer was
  killed (`SIGKILL`, a power loss), `valkey-fuzzer sweep` kills its servers
  and removes its chains, mounts, and data directories; it never touches
  anything untagged or anything a running fuzzer owns (see
  [sweep](docs/cli.md#sweep)).

## Documentation

| Document | For | Contents |
| --- | --- | --- |
| [Getting started](docs/getting-started.md) | first-time users | install, first runs, output and report files, troubleshooting |
| [Reading a FAIL](docs/reading-a-fail.md) | anyone with a failed run | where the verdict, logs, and replay command are, and what to attach to a Valkey issue |
| [Concepts and glossary](docs/concepts.md) | everyone | scenarios, execution plans, component roles, events, evidence, and the terms these docs use |
| [Base contracts](docs/contracts.md) | everyone | the rules every run is held to: verdicts, the evidence contract, fault ownership, effective config, search profiles, deadlines, host state, replay |
| [Scenarios and experiments](docs/scenarios-and-experiments.md) | scenario and experiment authors | file formats, the local deployment, the Valkey experiments, random scenarios and search profiles |
| [Valkey components](docs/components.md) | experiment authors | every Valkey component and provider, with its config, declarations, and defaults |
| [Adding tests](docs/extending.md) | experiment and component authors | writing experiments, components, and providers, registering them, and testing them |
| [Command-line reference](docs/cli.md) | users and scripts | every command and option, exit codes, report folders and formats |
| [Cluster chaos and CI](docs/cluster-chaos-and-ci.md) | cluster chaos users, CI maintainers | running cluster chaos, kill safety, the GitHub workflows, reproducing CI failures |
| [Spec bridge](docs/spec-bridge.md) | model and experiment authors | TLA+ bundles: `spec check`, `spec translate`, `spec conform`, the assurance report |
| [Bug zoo](docs/bug-zoo.md) | experiment authors, soak maintainers | deliberately broken Valkey builds the experiments must catch |
| [Architecture](docs/architecture.md) | framework contributors | code layout, the compiler and runner, the local deployment provider, the wave engine |
| [TLA+ election model](formal/cluster-election/README.md) | Valkey cluster maintainers | the failover election model and its live counterpart |

## Contributing

Fork the repository, add focused tests (see
[Testing](docs/extending.md#testing)), and run the checks before you open a
pull request:

```bash
python -m pip install -e '.[dev]'
python -m pytest
ruff check .
ruff format --check .
```

Describe the code path you exercised, the oracles that judge it, and how to
reproduce a failure. Sign off every commit (`git commit -s`).

## License

BSD 3-Clause. See [LICENSE](LICENSE).
