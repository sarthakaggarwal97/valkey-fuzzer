# Getting started

This page takes you from a fresh checkout to reading your first report. It
assumes you have read the [README](../README.md), including its safety notes.
The terms it uses are defined in [Concepts and glossary](concepts.md).

- [Install](#install)
- [Choose a Valkey binary](#choose-a-valkey-binary)
- [Build porcupine](#build-porcupine)
- [Run an experiment](#run-an-experiment)
- [Read the output](#read-the-output)
- [Find the files a run leaves](#find-the-files-a-run-leaves)
- [Replay a run](#replay-a-run)
- [Run cluster chaos](#run-cluster-chaos)
- [Try other experiments](#try-other-experiments)
- [Running many tests](#running-many-tests)
- [Troubleshooting](#troubleshooting)

## Install

Until this code lands in `valkey-io/valkey-fuzzer`, clone the `generic-fuzzer`
branch of the fork; `main` of `valkey-io/valkey-fuzzer` is an older layout.
You need Python 3.10 or later and git; use a virtual environment if your
system Python refuses `pip install`.

```bash
git clone -b generic-fuzzer https://github.com/sarthakaggarwal97/valkey-fuzzer.git
cd valkey-fuzzer
python -m pip install -e .
```

This installs the `valkey-fuzzer` command and the Valkey experiments, which
are part of the fuzzer (`valkey_fuzzer.components`). Install
`'.[dev]'` instead of `.` to also get pytest and ruff.

Check the installation:

```bash
valkey-fuzzer --version
valkey-fuzzer list experiments
valkey-fuzzer list components
```

`list experiments` prints one line per experiment: its ID, the
deployment modes it supports, and a description. `list components` prints the
role, registration ID, and Python class of every registered component.

## Choose a Valkey binary

The local deployment looks for `valkey-server` in this order:

1. `--valkey-binary PATH`, or `deployment.valkey_binary` in the scenario;
2. `valkey-server` on `PATH`;
3. a previous build at `/tmp/valkey-build/valkey/src/valkey-server`;
4. otherwise it clones release 9.1.3 of `https://github.com/valkey-io/valkey.git`
   and builds it with `make` into `/tmp/valkey-build/valkey`, one build at a
   time.

Only four experiments probe the binary before the deployment starts
(`valkey.capability.build-probe`): `valkey.cluster-election-durability`,
`valkey.cluster-vote-save-crash`, `valkey.mixed-version-cluster`, and
`valkey.rolling-upgrade`. The probe goes through steps 1 to 3, so without a
binary these report `ERROR` instead of building one. Every other experiment,
including the quickstart's `valkey.aof-durability`, reaches step 4 on a host
with no binary: it clones and builds Valkey 9.1.3 before it starts, which
takes about 70 seconds and 280 MB, and needs git, make, a C compiler, and
network access. It logs one line when it starts and writes the clone and make
output to `/tmp/valkey-build/build.log`. The build is reused afterwards. To
build Valkey yourself once, or point at a build:

```bash
git clone --depth 1 --branch 9.1.3 https://github.com/valkey-io/valkey.git && make -C valkey -j"$(nproc)"
valkey-fuzzer run examples/valkey/aof_durability.yaml --valkey-binary "$PWD/valkey/src/valkey-server"
```

Use a binary where it was built. A `valkey-server` copied out of its build
tree can crash at start because it cannot find its Lua module, and the run
then reports `FAIL`.

Some experiments need features that only some builds have (such as
`cluster-config-save-behavior`). They probe the binary first and report
`SKIP_UNSUPPORTED` instead of failing when it lacks a feature they name.

## Build porcupine

Six experiments check that the history of reads and writes is linearizable
with [Porcupine](https://github.com/anishathalye/porcupine), through the
`valkey.history` fragment: `valkey.aof-durability` (the quickstart),
`valkey.cluster-partition`, `valkey.mixed-version-cluster`,
`valkey.replication-chaos`, `valkey.replication-full-resync`, and
`valkey.rolling-upgrade`. The checker is a small Go program in `porcupine/`
that pip does not build, so install Go 1.21 or later and build it once per
host, from the checkout:

```bash
mkdir -p "$HOME/.local/bin"
go -C porcupine build -o "$HOME/.local/bin/porcupine"
export PATH="$HOME/.local/bin:$PATH"
```

The build downloads the Porcupine module. Behind a proxy that breaks TLS to
`proxy.golang.org` (`x509: certificate is valid for ...`), run it as
`GOPROXY=direct GOSUMDB=off go -C porcupine build -o "$HOME/.local/bin/porcupine"`.
Without `porcupine` on `PATH`, `validate` of these experiments exits `2` and a
run ends `ERROR` before it starts anything, with `porcupine is not on PATH` and
the build command; the other experiments do not need it.

## Run an experiment

Check the scenario first. `validate` compiles it and runs every check that
needs no server: fields, inputs, references, component config, and instance
planning.

```bash
valkey-fuzzer validate examples/valkey/aof_durability.yaml --verbose
```

`--verbose` lists the components the execution plan will run, including those
the experiment inherited from shared fragments.

Then run it:

```bash
valkey-fuzzer run examples/valkey/aof_durability.yaml --output report.json
```

The scenario file is short:

```yaml
scenario_id: aof-durability
seed: 11
experiment:
  id: valkey.aof-durability
  inputs:
    appendfsync: always
    loss_window_s: 0
deployment:
  mode: standalone
  replicas_per_primary: 1
  base_port: 7430
```

It selects the `valkey.aof-durability` experiment, sets two of its inputs, and
asks for a standalone deployment with one replica. The full format is in
[Scenarios and experiments](scenarios-and-experiments.md#scenarios).

## Read the output

The end of a run looks like this:

```text
Scenario: aof-durability
Experiment: valkey.aof-durability
Status: PASS
Mode: experiment
Deployment: standalone
Duration: 16.58s
Components: 13 passed, 0 failed, 0 skipped, 0 errors
Evidence: 12
Valkey SHA: dff4c58e
Summary: experiment passed
Report: /tmp/valkey-fuzzer/runs/20261028T041350.730Z-aof-durability-5405f2/report.json
Reproduce: valkey-fuzzer replay /tmp/valkey-fuzzer/runs/20261028T041350.730Z-aof-durability-5405f2/report.json

Results saved to report.json
```

`Status` is the outcome:

| Outcome | Meaning | Exit code |
| --- | --- | --- |
| `PASS` | Every required check passed and the run reached the code path it claims to test. | 0 |
| `FAIL` | An oracle or action observed Valkey do something wrong. | 1 |
| `ERROR` | No trustworthy verdict: setup, a provider, a timeout, or cleanup failed, or the code path the experiment targets never ran. | 2 |
| `SKIP_UNSUPPORTED` | The host or the binary lacks a feature this experiment names; nothing was started. | 3 |

`Summary` says why. A run that missed required evidence names each item
and the component that should have produced it, for example
`required evidence was not observed: valkey.rdb.reload-verified from snapshot`.
[Outcomes and verdicts](contracts.md#outcomes-and-verdicts) has the rules.
For a `FAIL`, [Reading a FAIL](reading-a-fail.md) shows where its evidence and
logs are and what to attach to a Valkey issue.
Add `--verbose` to `run` to print every component's result, its evidence
count, and its diagnostics.

## Find the files a run leaves

| What | Where |
| --- | --- |
| The execution plan and the run report | a report folder `<root>/runs/<UTC time>-<scenario_id>-<suffix>/` holding `plan.json` and `report.json` |
| Files components write (captured RDBs, ledgers, log excerpts, traces) | `components/<component id>/` in the same report folder |
| Instance logs and captured stdout and stderr | `/tmp/valkey-fuzzer/logs/<seed>-<deployment id>-valkey-<N>.log` and `.stdio.log` |
| Instance data (AOF, RDB, `nodes.conf`) | `/tmp/valkey-fuzzer/deployment-<id>/valkey-<N>/data/`, deleted after the run |

`<root>` is `--artifact-dir DIR`, or `<temp dir>/valkey-fuzzer` without it;
`<temp dir>` is `/tmp` on most Linux hosts. Every user's runs share the
default directories, so the fuzzer creates them with mode `1777`, like `/tmp`
(see [Report folders](cli.md#report-folders)).

`report.json` holds the scenario as run, the execution plan, every
component's result and evidence, the event log, the deployment's instances
and binaries, and digests of the code that produced the run. Its format is
in [Run reports](cli.md#run-reports).

Remove the logs of one seed with:

```bash
valkey-fuzzer cleanup-logs 11 --force
```

## Replay a run

```bash
valkey-fuzzer replay report.json
```

Replay runs the recorded execution plan again with the fuzzer and
Valkey binaries installed now. It first compares them with the provenance the
report recorded and refuses to run if any differ; `--allow-drift` replays
anyway and records the differences. The seed fixes every random choice the
components make, but not timing, so a fault that races with the server can
land differently in a replay. Details are in [replay](cli.md#replay).

## Run cluster chaos

```bash
valkey-fuzzer run --seed 173 --experiment valkey.cluster-chaos
```

A seed generates a whole scenario. For cluster chaos that is the number of
primaries and replicas and waves of failovers with process kills. Seed 173
is a small one: 3 primaries with 1 replica each, and one wave that
`SIGKILL`s an instance and then fails shard 2 over. The output names the
seed, the outcome, the report, and two ways to run the scenario again:

```text
Generated scenario: valkey.cluster-chaos, seed 173 (attempt 0)
...
Status: PASS
...
Report: /tmp/valkey-fuzzer/runs/20261029T031913.990Z-valkey.cluster-chaos-seed173-7aad6a/report.json
Reproduce: valkey-fuzzer run --seed 173 --experiment valkey.cluster-chaos
  or, without the generator: valkey-fuzzer run /tmp/valkey-fuzzer/runs/20261029T031913.990Z-valkey.cluster-chaos-seed173-7aad6a/scenario.yaml
```

`valkey-fuzzer run --random --experiment valkey.cluster-chaos` picks a random
seed. Everything about cluster chaos is in
[Cluster chaos and CI](cluster-chaos-and-ci.md).

## Try other experiments

[`examples/valkey/`](../examples/valkey/) has a scenario for every Valkey
experiment, and the [experiment table](scenarios-and-experiments.md#the-valkey-experiments)
says what each one checks and what it needs. Some need a second build, or
Linux and root; on a host without them they report
`SKIP_UNSUPPORTED`. Edit `base_port` when you run several scenarios at once by
hand, so their ports do not overlap, or let a [batch](#running-many-tests) run
them.

The same runner also drives two tools over runs:

```bash
valkey-fuzzer diff examples/valkey/differential_baseline_vs_candidate.yaml
valkey-fuzzer minimize report.json --output minimized.yaml
```

`diff` runs one seeded scenario under several variants and compares their
evidence, and `minimize` shrinks a failing scenario while it keeps failing the
same way. See the [command-line reference](cli.md).

To write your own experiment, start with
[Adding tests](extending.md#write-a-data-only-experiment).

## Running many tests

`valkey-fuzzer batch` is the one way to run many scenarios. It takes sources,
from flags or a batch spec file, and runs every job on parallel workers that
never share ports, data directories, or `TMPDIR`:

```bash
# Five seeds of every experiment that declares what it randomizes, quickly
valkey-fuzzer batch --random all --seeds 5 --profile pr-quick --workers 4 --out quick
# Scenario files, a whole directory of them, as written
valkey-fuzzer batch --scenarios examples/valkey/aof_durability.yaml --scenarios proposals/
# A matrix over a base scenario, from a spec
valkey-fuzzer batch examples/valkey/cluster_election_batch.yaml --workers 2
# The bug zoo: every case's clean control must pass and its broken build must be caught
valkey-fuzzer batch --zoo BUILDS --workers 2
# The failures of an earlier batch again, then one of each kind once more, alone
valkey-fuzzer batch --failures quick --serial-reruns 1
# Finish a batch that was stopped or killed
valkey-fuzzer batch --resume quick
```

A **search profile** (`--profile`) chooses how hard to sample each
experiment's legal space and the budgets: `pr-quick` (small, edge-heavy,
short), `nightly-soak` (the whole space, many seeds), `adversarial` (edges
first), and `candidate` (given scenarios as written); see
[Legal space and search profiles](scenarios-and-experiments.md#legal-space-and-search-profiles).

The batch folder holds every run's ordinary report folder under `runs/<job
id>/` and `summary.json`: counts by status, the failures grouped by signature
(with how the serial re-runs went), the zoo verdicts, and anything a run left
on the host, which the batch removes and reports as an error. The exit code
is the worst result, and a zoo case that does not hold fails the batch. Every
option and file is in [batch](cli.md#batch).

## Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| `ERROR`, and `--verbose` shows `Cannot provision the local deployment, planned ports are in use: valkey-0: client port 7430 on 127.0.0.1 is unavailable (...)` | Another process holds the port, often a server left by an interrupted run. Stop it, or change `deployment.base_port`. |
| `ERROR` with `valkey-server binary 'valkey-server' was not found` | One of the four experiments that probe the build before they start anything (see [Choose a Valkey binary](#choose-a-valkey-binary)); the probe does not build from source. Put `valkey-server` on `PATH` or pass `--valkey-binary`. |
| `ERROR` (or `validate` exit `2`) with `porcupine is not on PATH` | The experiment checks linearizability and the checker is not built. See [Build porcupine](#build-porcupine). |
| `go: ... x509: certificate is valid for ..., not proxy.golang.org` while building `porcupine` | A proxy intercepts TLS. Build with `GOPROXY=direct GOSUMDB=off`. |
| `FAIL` with `crash report` in the component's diagnostics, right after start | The `valkey-server` cannot run where it is, often a binary copied away from its build tree. Run it in place. See [Reading a FAIL](reading-a-fail.md). |
| A batch job is `ERROR` although its run report passed, and `batch.log` shows `LEAKED` | The run left a server, chain, mount, or directory behind; the batch removed it and lists it under `leaks`. See [Isolation, deadlines, and leaks](cli.md#isolation-deadlines-and-leaks). |
| `SKIP_UNSUPPORTED` with `build rejects startup configuration` or `does not support config` | The binary lacks a feature the experiment needs. Use a build that has it. |
| `ERROR` with `not a setting the experiment says the build may lack` | The build rejects a setting of the scenario's server config, often a misspelt name or value. Fix the scenario. |
| `SKIP_UNSUPPORTED` with `requires Linux` or `requires root` | The experiment changes the host; run it on a disposable Linux VM as root. |
| `experiment '...' is not registered` | The scenario names an experiment that is neither shipped nor in a directory passed with `--experiment-dir`. Check the ID with `valkey-fuzzer list experiments`, or pass the directory that holds it. |
| `refusing to replay because this environment differs` | See [Replay a run](#replay-a-run). |
| A run hangs | A component without a deadline is stuck. Rerun with `--timeout SECONDS`; see [Deadlines and cancellation](contracts.md#deadlines-and-cancellation). |
