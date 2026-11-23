# Command-line reference

This page lists every `valkey-fuzzer` command and option, the exit codes, the
folders and files the commands write, and the report formats. It is for
people who run the fuzzer by hand and for scripts and CI jobs that call it.
The scenario format is in [Scenarios and experiments](scenarios-and-experiments.md),
and cluster chaos and the CI workflows are in [Cluster chaos and CI](cluster-chaos-and-ci.md).

- [Commands](#commands)
- [Common options](#common-options)
- [Exit codes](#exit-codes)
- [Report folders](#report-folders)
- [run](#run) (and [generated scenarios](#generated-scenarios-run---random)),
  [batch](#batch), [validate](#validate), [list](#list), [replay](#replay),
  [minimize](#minimize), [findings](#findings), [cleanup-logs](#cleanup-logs),
  [sweep](#sweep), [fetch-valkey](#fetch-valkey), [spec](#spec)
- [Run reports](#run-reports)

## Commands

| Command | Does |
| --- | --- |
| `run [SCENARIO]` | Compile and run one scenario, or a [generated one](#generated-scenarios-run---random) with `--random` or `--seed`. |
| `batch [SPEC]` | Run many scenarios (random seeds, a matrix, scenario files, the bug zoo, earlier failures) on parallel, isolated workers. |
| `validate FILE...` | Compile scenarios and check them without starting anything. |
| `list experiments\|components\|generators` | List the installed experiments, components, or generator hooks, or print the catalog as JSON. |
| `replay REPORT` | Check a recorded run against this environment, then run its execution plan again. |
| `minimize REPORT` | Shrink a failing scenario while its failure signature holds. |
| `findings list\|show KEY` | Read the failure signatures batches recorded in a [findings store](#findings). |
| `cleanup-logs SEED` | Delete the instance logs of one seed. |
| `sweep` | Remove what fuzzer processes that are gone left on this host. |
| `fetch-valkey [REF]` | Build a Valkey tag, branch, or commit once into the cache and print its `valkey-server`. |
| `spec check\|translate\|conform` | Check a TLA+ spec bundle, turn its counterexamples into scenarios, and check runs against it. |

`valkey-fuzzer --version` prints the version, and `valkey-fuzzer COMMAND
--help` the options of one command.

## Common options

`run`, `batch`, `validate`, `list`, `replay`, and `minimize` accept:

| Option | Meaning |
| --- | --- |
| `--experiment-dir DIR` | Also load the data-only experiments in `DIR` (`*.yaml` and `*.yml`). Repeatable. |

Each of these is accepted only by the commands that use it; any other command
rejects it:

| Option | Commands | Meaning |
| --- | --- | --- |
| `--valkey-binary PATH` | `run`, `batch`, `validate`, `replay`, `minimize` | Use this `valkey-server` for the deployment (the local deployment's `deployment.valkey_binary`; a deployment provider without a `binary_override` hook refuses it). For `batch` it applies only to scenarios that name no binary of their own; for `replay` it is an intentional override; see [replay](#replay). |
| `--base-port PORT` | `run`, `batch` | The first client port of the deployment, instead of the scenario's or the draw's. Every other client port, and in cluster mode every bus port, follows from it, so two scenarios started by hand do not overlap. For `batch` it is the first client port of worker 0. A generated run's `Reproduce:` line keeps it. |
| `--artifact NAME=PATH` | `run`, `replay` | Resolve the scenario's `valkey.artifact.local-binary` or `valkey.artifact.local-module` artifact `NAME` at `PATH` instead of the path it declares, so a run recorded elsewhere (a CI batch's parent build, `artifacts.parent`, or its module builds, `artifacts.module` and `artifacts.module_parent`) runs on this host's build of it. Repeatable, once per artifact; an artifact the scenario does not declare, or of another provider, is an error. The report records the scenario with the override; see [replay](#replay) for how the binary is still compared. |
| `--artifact-dir DIR` | `run`, `batch`, `replay`, `minimize` | Root for report folders, which hold each run's component artifacts, and for provider directories; for `batch`, where the default batch folder goes. See [Report folders](#report-folders). |
| `--timeout SECONDS` | `run`, `batch`, `replay`, `minimize` | Deadline for each run. Without it, a run gets a derived default: the search profile's `budget.timeout_s`, else 1200 s, and for `run` never less than the sum of the components' `timeout_s`. See [Deadlines and cancellation](contracts.md#deadlines-and-cancellation). |
| `--allow-unsupported` | `run`, `batch`, `replay` | Exit `0` instead of `3` on `SKIP_UNSUPPORTED`. |

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | `PASS`, or `SKIP_UNSUPPORTED` with `--allow-unsupported` |
| `1` | `FAIL` |
| `2` | `ERROR`, or the command itself failed (a compile error, `minimize` keeping no reduction, a report that cannot be written) |
| `3` | `SKIP_UNSUPPORTED` |
| `64` | usage or input error (`EX_USAGE`): a bad option or option mix, no command, or a missing or unreadable input file. Nothing was judged. |
| `130` | interrupted (Ctrl-C or `SIGTERM`; the run is cleaned up first) |

Errors and warnings go to stderr, so stdout (`--format json`, say) holds only the command's output.

These hold for `run` and `replay`. The other cases:

- `run` and `replay`: an `--output` (or `--export-scenario`) that
  cannot be written counts as an `ERROR`, unless the run failed.
- `batch` exits with its worst result: `1` if any run failed or a zoo case did
  not hold, else `2` if any run errored, left something behind, a zoo case
  was undecided, or the batch could not start, else `3` if any run was unsupported, a zoo case was
  skipped, or a matrix combination did not compile, else `0`; `130` when it was stopped before it finished (resume it).
  A `--dry-run` exits `0`. See [batch](#batch).
- `validate` and `list` exit `0` on success and `2` on error. `minimize` exits
  `0` when it kept a reduction or the run had nothing to minimize, and `2` when
  no reduction kept the failure (the reference failure did not reproduce, or the
  scenario is already minimal).
- `replay` and `minimize` never refuse because of drift. A `replay` that drifted but ran exits with the run's own
  code (`0`, `1`, `2` or `3`); the drift is in the report's `provenance.drift` and on stderr.
- `batch` and `validate` keep `2` for their own bad input; the `64` above is for the other commands.
- `cleanup-logs` exits `0`, or `2` when the prompt is declined or a file could
  not be deleted.
- `sweep` exits `2` when it could not remove something it found.
- `spec check` and `spec conform` exit `1` when a gate failed, a claimed
  property was violated, or a run took a step the model cannot, else `2` on a
  harness error (a tool that did not run,
  a bundle that does not exist); see [spec](#spec).
- `valkey-fuzzer` without a command prints its help and exits `64`.

## Report folders

Every execution plan a command runs gets its own report folder holding:

- `plan.json`: the execution plan, written before the run starts;
- `report.json`: the [run report](#run-reports), written when it ends. A run
  interrupted with Ctrl-C or `SIGTERM` leaves only `plan.json`;
- `components/<component id>/`: the files the run's components wrote
  (captured RDBs, ledgers, INFO samples, log excerpts, traces), if any;
- `scenario.yaml`: the scenario as run (a generated one too), which `run`
  accepts as is.

| Command | Report folders |
| --- | --- |
| `run` | `<root>/runs/<name>/` |
| `replay` | `<root>/replays/<name>/` |
| `batch` | `<batch folder>/runs/<job id>/`, with `summary.json` in the batch folder; the folder is `--out`, or `<root>/batches/<name>/` |
| `minimize` | `<root>/minimize/<name>/attempt-<n>/` |

`<root>` is `--artifact-dir`, or `<temp dir>/valkey-fuzzer` without it
(`<temp dir>` is `/tmp` on most Linux hosts). `<name>` is the UTC start time
to the millisecond, the scenario or batch ID, and a random
suffix, for example `20261028T041350.730Z-aof-durability-5405f2`. Folders are
created exclusively, so names sort by start time and never collide, even
between processes. `run` and `replay` print `Report: <path>`;
`minimize` prints `Reports: <folder>`, and `batch` prints
`Batch: <folder>`.

Other files go elsewhere:

| Files | Location |
| --- | --- |
| Artifact and resource provider directories | `<root>/<run id>/providers/<provider>/<kind>/<id>/` |
| Instance logs | `<base_data_dir>/logs/<seed>-<deployment id>-valkey-N.log` and `.stdio.log` |
| Instance data | `<base_data_dir>/deployment-<id>/valkey-N/data/`, or under `instance_data_roots` |

`base_data_dir` is a deployment field, `/tmp/valkey-fuzzer` by default.
Evidence in `report.json` names the artifact files it refers to.

Every user's runs on a host share the default directories: `<temp dir>/valkey-fuzzer`
and its `<kind>` folders (`runs/`, `batches/`, ...), the default
`base_data_dir` and its `logs/`, `<temp dir>/valkey-fuzzer-mounts`,
and `/tmp/valkey-fuzzer-locks`. One the fuzzer creates
gets mode `1777`, sticky and world-writable like `/tmp`, so a run as root
(partitions and tmpfs need it) does not leave it unwritable for other users,
and no user can remove another's files in it. A directory that already exists
keeps its mode: one left `0755` by an older version needs `sudo chmod 1777`,
or removing. A run's own folders, a deployment directory, and an
`--artifact-dir` or `base_data_dir` you name, with what is made in it, get the
usual mode.

## run

```bash
valkey-fuzzer run examples/valkey/aof_durability.yaml --output report.json --output-format json --verbose
valkey-fuzzer run --seed 42 --experiment valkey.aof-durability --profile pr-quick
```

Compiles the scenario, runs it, and writes its report folder. Without a
file, `--random` or `--seed` generates the scenario (see [Generated
scenarios](#generated-scenarios-run---random)). To run many, use
[batch](#batch).

| Option | Meaning |
| --- | --- |
| `--output PATH` | Also save the run report here. |
| `--output-format json\|yaml` | Format of `--output` (which it needs); default `json`. |
| `--verbose` | Print every component's result, evidence count, and diagnostics, and the run diagnostics. |

`run` prints the deadline it uses and where it came from, before the run
starts: `Timeout: 1200s (default)`, `Timeout: 600s (search profile 'pr-quick'
budget)`, `Timeout: 1800s (sum of the components' timeout_s, above the default
of 1200s)`, or `Timeout: 90s (--timeout)`. A run is never unbounded; pass a
larger `--timeout` for one that needs it.

The command prints the scenario, the experiment, the outcome, component
counts, the evidence count, the Valkey SHA, the summary, and the report
path. A `smoke` run (it judges no experiment) prints `Status: PASS (mode: smoke; no coverage claim)`. The summary names why a run did not pass; for missing required
evidence it names each item and its source, such as `required evidence was
not observed: valkey.rdb.reload-verified from snapshot`.

The report records the scenario as run, including a `--valkey-binary` or
`--artifact` override, so `replay` and `minimize` can use it on their own.
`--artifact` needs a scenario file: a generated scenario declares no artifacts.

### Generated scenarios: run --random

```bash
valkey-fuzzer run --random                                    # an experiment drawn from those that opt in
valkey-fuzzer run --seed 42 --experiment valkey.replication-partial-resync --profile pr-quick
valkey-fuzzer run --random --experiment valkey.snapshot-equivalence --exclude server_config.io-threads=4
valkey-fuzzer run --random --experiment valkey.cluster-chaos
valkey-fuzzer run --random --export-scenario s.yaml
```

`--random` or `--seed N` replaces the scenario file: the generator draws a
whole scenario (deployment shape, inputs, server config pools, fault timing)
for an experiment that declares `metadata.generator` (see
[Declaring what may be randomized](scenarios-and-experiments.md#declaring-what-may-be-randomized)),
compiles it, and runs it like any other. The same seed (and profile) always
draws the same scenario on a host with the same number of usable cores (see
[Seeds](contracts.md#seeds-replay-and-provenance)): the header prints
`seed 42 (attempt 0, cores 4)`, and a bug report that names a seed should name
them too. `batch --random EXPERIMENT --seeds K` runs many seeds (see
[batch](#batch)).

| Option | Meaning |
| --- | --- |
| `--random` | Draw from a random seed (recorded in the report). |
| `--seed N` | Draw from seed N. |
| `--experiment ID` | Generate for this experiment. Without it each seed draws one of the opted-in experiments, so the choice for a seed changes when experiments are added. |
| `--profile NAME` | Draw under this [search profile](scenarios-and-experiments.md#legal-space-and-search-profiles) (`pr-quick`, `nightly-soak`, `adversarial`). |
| `--exclude PATH=VALUE` | Never generate VALUE at PATH: `inputs.<name>`, `deployment.<field>`, `server_config.<key>` (any value a planned instance starts with: the deployment provider's defaults, the deployment's config, one instance's, or an experiment's overlay), or `experiment` (not drawn when choosing one). A string compares as written, anything else as YAML reads it. Repeatable. |
| `--base FILE` | A partial scenario merged over every draw, for what the generator cannot know: a second build, a port range, a pinned input. |
| `--export-scenario FILE` | Write the generated scenario to FILE. |

Every candidate is compiled and statically preflighted (what `validate`
checks) before it runs. A candidate that is invalid, matches one of the
experiment's `reject` rules, or holds an excluded value is redrawn from the
next attempt's streams, up to 50 attempts; then the run is a harness error
(exit 2) naming the last reason. So is a candidate whose deployment needs
more cores than this process may use (its CPU affinity, or its cgroup's CPU
quota when that is lower), so a small host draws smaller deployments: the
local deployment counts a sixteenth of a core for each instance's main thread
and half a core for each IO thread (`io-threads` above 1), which a busy node
spins (see [Host-sized draws](scenarios-and-experiments.md#host-sized-draws)).
When no attempt fits, the run is the first attempt's scenario, and it is
`SKIP_UNSUPPORTED` (exit 3), saying how many cores it needed. A scenario
file runs as written, whatever it needs. A scenario whose planned ports are in use,
or reach the host's ephemeral port range, moves to free ones below that
range, and the scenario records the port it ran on.

Every run prints a `Reproduce:` line, and a failed or errored run a `Look at:`
line with the report folder and the server log directory. For a scenario file
the `Reproduce:` command is `valkey-fuzzer replay <report>`. A generated
scenario's `Reproduce:` command, shell-quoted, draws the same
scenario: the seed, experiment, exclusions, profile, and every option the
draw depends on (`--base`, `--valkey-binary`, `--experiment-dir`). Each run's report folder also holds `scenario.yaml`, the generated scenario
with a header naming its seed and experiment; `valkey-fuzzer run
<folder>/scenario.yaml` runs it again without the generator. The run report's
`reproducer.generated` records `seed`, `experiment`, `attempt`,
`excludes`, `boundary` (how many drawn values were a
range edge or the first or last listed value), `profile`, `cores` (what the
deployment had to fit), and `shortfall` (why none fit, or null).

## batch

```bash
valkey-fuzzer batch --random all --seeds 5 --profile pr-quick --workers 4
valkey-fuzzer batch examples/valkey/cluster_election_batch.yaml --workers 2 --out election-batch
valkey-fuzzer batch --scenarios proposals/ --profile candidate --valkey-binary ./valkey-server
valkey-fuzzer batch --zoo BUILDS --workers 2
valkey-fuzzer batch --failures election-batch --serial-reruns 2
valkey-fuzzer batch --resume election-batch
```

The one way to run many scenarios. A batch expands its sources into jobs, one
run each, runs them on `--workers` parallel workers that never share ports,
logs, or directories, removes whatever each run left on the host, and writes
one batch folder with every run's report folder and a summary. It resumes
after an interruption.

### Sources

A batch spec (`SPEC`) lists sources and options; the flags add sources to it
and override its options. Relative paths in a spec are read from the spec's
directory.

```yaml
batch_id: nightly                    # names the default folder
profile: nightly-soak                # a search profile: sampling and budgets
workers: 8
timeout_s: 1800                      # per run; default the profile's, else 1200
port_base: 11000
valkey_binary: /opt/valkey/src/valkey-server   # for scenarios that name no binary
prune_pass: true
serial_reruns: 2
conform_tlc_timeout_s: 300           # TLC's limit per run in the worker's spec conform
sources:
  - random:
      experiments: all               # or a list of experiment IDs
      seeds: 50                      # per experiment; default the profile's, else 1
      first_seed: 0                  # default: a fresh one, recorded in jobs.json
      excludes: [server_config.io-threads=4]
      base: {deployment: {replicas_per_primary: 1}}   # a partial scenario over every draw
  - matrix:
      seed: 2026
      seeds_per_case: 1
      base: {scenario_id: snapshot, experiment: {id: valkey.snapshot-equivalence}, deployment: {mode: standalone}}
      dimensions:
        deployment.replicas_per_primary: [0, 1]
        experiment.inputs.mutation_rate: [0, 1500]
  - scenarios: [proposals/, extra.yaml]
  - zoo: {builds: BUILDS, cases: [replica-skips-writes], directory: zoo}
  - failures: {batch: earlier-batch, statuses: [FAIL, ERROR], per_signature: 3}
```

| Source | Flag | Jobs |
| --- | --- | --- |
| `random` | `--random EXPERIMENT` (repeatable, or `all`), `--seeds N`, `--first-seed N`, `--exclude PATH=VALUE`, `--base FILE` | `random-<experiment>-s<seed>`: seeds `first_seed` to `first_seed + seeds - 1` of each experiment, drawn under the batch's profile exactly as `run --seed N --experiment ID --profile P` draws them. `all` is every experiment that declares `metadata.generator`, less those an `experiment=` exclusion names. |
| `matrix` | (spec only) | `matrix-<base scenario_id>-<combination>-<n>`: every combination of the dotted-path `dimensions` (paths start with `experiment`, `deployment`, `artifacts`, or `resources`; under `experiment` only `experiment.inputs` and the paths below it; a `null` value removes a key), `seeds_per_case` times, each with a seed drawn from `seed`. A combination that does not compile is listed under `invalid` and not run, and counts as `ERROR` in the batch's status (exit `2`): a misspelt value never skips. |
| `scenarios` | `--scenarios PATH` (repeatable) | `scenario-<file stem>`: each file, or each `*.yaml` and `*.yml` file of a directory, run as written. |
| `zoo` | `--zoo BUILDS`, `--zoo-case ID` (repeatable) | `zoo-<case>-<file stem>-control`, and `-<n>` for the broken runs, numbered from 0 across the case: each [bug zoo](bug-zoo.md) case's clean control and broken runs, on the builds `zoo/build.sh` made in `BUILDS`. `directory` defaults to the repository's `zoo/`, which only a checkout has (an installed package does not ship it). Each build must name the pinned commit, or nothing runs. |
| `failures` | `--failures BATCH` | `rerun-<job id>`: the jobs of an earlier batch whose result had one of `statuses` (default `FAIL` and `ERROR`), at most `per_signature` of each failure signature (default all), each as it ran: the same scenario, and for a random job the scenario its seed drew (its run's `scenario.yaml`), so a re-run after a code change runs what failed. A random job that never drew a scenario draws again. Zoo runs are left out. |

A job that names no binary uses `valkey_binary` (`--valkey-binary`); a zoo
run names its build.

| Option | Flag | Meaning |
| --- | --- | --- |
| `batch_id` | `--batch-id` | Name of the default folder; default `batch`. |
| `profile` | `--profile NAME` | The [search profile](scenarios-and-experiments.md#legal-space-and-search-profiles) every random job is drawn under, and the budgets. |
| `workers` | `--workers N` | Parallel workers; default 1. On resume, the only option that may change. A random job's deployment is drawn to fit its worker's share of the batch's cores: the cores the creating process may use, over `workers`. |
| `timeout_s` | `--timeout SECONDS` | The deadline of each run. |
| `port_base` | `--base-port PORT` | The first client port of worker 0; default 11000. |
| `valkey_binary` | `--valkey-binary PATH` | See above. |
| `prune_pass` | `--prune-pass` | Keep only `report.json` and `scenario.yaml` of a run that passed and left nothing behind. |
| `serial_reruns` | `--serial-reruns N` | After the parallel runs, run `N` jobs of each `FAIL` or `ERROR` signature again, one at a time (load can explain those, not an unsupported run). |
| `conform_tlc_timeout_s` | (spec only) | How long each run's own [`spec conform`](spec-bridge.md#in-a-run) check may run TLC on its observed states; default 300, which a run outside a batch uses. A check it stops is a conform `error` whose reason names this option (an `ERROR` for the run if the bundle's claims are approved). |
| `findings` | `--findings PATH` | The [findings store](#findings) the batch appends its failure signatures to when it finishes; default `findings.json` in the batch folder. Several batches given one `PATH` share it. A relative `PATH` in a spec is read from the spec's directory. |
| | `--out DIR` | The batch folder; default `<root>/batches/<time>-<batch id>-<suffix>`. |
| | `--resume DIR` | Finish the batch in `DIR`. Takes only `--workers` and `--allow-unsupported`: the folder holds the sources and every other option. |
| | `--dry-run` | List the jobs, the invalid matrix cases, and the skipped zoo cases; run nothing. |

### Isolation, deadlines, and leaks

Each job runs in its own worker process (`python -m valkey_fuzzer.batch
TASK`) on one of the workers' slots. Slot `k` owns the client ports
`port_base + 120k` to `port_base + 120k + 119` and their cluster bus ports,
the per-instance loopback addresses `127.0.<k+1>.0/24`, its own
`base_data_dir`, `TMPDIR`, and provider directories under `work/w<k>/`,
which are wiped after each job. The batch overrides each scenario's
`base_port`, `loopback_block` (`k + 1`), and `base_data_dir` with its slot's,
so a partition (whose rules match addresses) in one worker never cuts
another worker's instances, and never moves a scenario to other ports; the
job's result records the slot's `ports` and `loopback_block`. A scenario
that plans more than 120 instances is an `ERROR` before anything starts.
The workers' blocks, and their bus ports 10000 higher, must fit below port
65536 without overlapping, so a batch has at most 83 workers. They must also
stay below the host's ephemeral port range (read from
`/proc/sys/net/ipv4/ip_local_port_range` on Linux, where it defaults to
32768-60999, and `sysctl net.inet.ip.portrange` on macOS, 49152-65535), where
an outgoing connection can take a port before Valkey binds it: `W` workers
need `port_base + 120W + 10000` at most the range's first port, so on a
default Linux host `port_base` at most `22768 - 120W` (the default 11000
fits all 83). The batch
refuses a block that reaches the range and names the highest `port_base`
that fits. One batch runs on a host at a time: every batch's workers use
loopback blocks 1, 2, ... (see `loopback_block` in
[The local deployment](scenarios-and-experiments.md#the-local-deployment)),
so a batch started while another runs is refused before it starts a job
(it holds `/tmp/valkey-fuzzer-locks/batch` while it runs).

A run's deadline is cooperative (see
[Deadlines and cancellation](contracts.md#deadlines-and-cancellation)); a
worker still running 60 seconds after it (or a quarter of the deadline, if
that is longer), plus the time the run's spec conform check may take after it
(`conform_tlc_timeout_s`, 300 seconds by default, so a run that used
its whole budget is not killed while it conforms), gets `SIGINT`, then `SIGTERM` 60 seconds later (both to the
worker alone: its run stops its servers), then `SIGKILL` to its process group
30 seconds later, and the job is an `ERROR` (`Deadline`). A second Ctrl-C
skips to the next signal. A worker
that exits without a run report is an `ERROR` (`WorkerExit`), and one whose
job could not be generated or compiled leaves `error.json` and is an `ERROR`.

After every run, the [sweep](#sweep), limited to that worker's owner tag and
its directories (and `/tmp`, where private servers' directories are), removes what the run left: servers, iptables chains, netem devices, tmpfs
mounts, data directories. Anything it finds is recorded under the job's
`leaks` and the summary's `leaks`, and makes the job an `ERROR`. The job's
summary (in its result, `summary.json`, `batch.log`, and on the console) then
names them: `passed its checks but left 2 leftovers: directory, server` for a
run that passed, or `left 1 leftover: server; <the run's summary>` otherwise.
A slot whose sweep could not remove a leftover (a mount that will not
unmount, say) runs no more jobs, and keeps its `work/w<k>/`: its ports or
addresses may still be taken. Once no slot is left the batch stops; sweep
the host, then resume it. Before its first job, a batch (a resumed one too)
removes the servers, iptables chains, netem devices, and tmpfs mounts of every fuzzer
process that is gone: a chain (or netem filter) a killed batch left matches the addresses of
this batch's slots, and would cut (or slow) its instances apart. If one cannot be
removed, the batch refuses to start. Dead runs' directories stay.

### The batch folder

```text
<batch folder>/
  batch.json          options, sources, invalid matrix cases, zoo cases, the cores (written once)
  jobs.json           every job, in order
  results/<id>.json   one finished job
  runs/<id>/          the job's run folder: plan.json, report.json, scenario.yaml,
                      components/, logs/ (instance logs), console.log (or error.json)
  experiments/        a copy of every --experiment-dir's documents (only with one)
  conformance/<b>.json  the spec conform report of bundle <b> over the runs checked against it
  summary.json        the summary, rewritten when the batch stops
  findings.json       the failure signatures across batches (see findings), unless --findings names a store
  batch.log           one line per finished job
  workers.json, work/ the jobs running now and the workers' scratch (while running)
```

Each `runs/<id>/report.json` is an ordinary [run report](#run-reports):
`replay`, `minimize`, and `spec conform` read it, and `valkey-fuzzer run
runs/<id>/scenario.yaml` runs the job again without the batch. A batch given
`--experiment-dir` copies the directories' `*.yaml` and `*.yml` documents
into `experiments/` (two directories holding the same file name are refused),
so `replay runs/<id>/report.json --experiment-dir experiments` reruns a job
from the folder alone.

A run whose plan names a [spec bundle](spec-bridge.md#bundles) (a
component's `spec`, as the election and replication experiments' observers
do) checks itself against it as its report is written, as every run does
([`spec conform` in a run](spec-bridge.md#in-a-run)): on its trace, with TLC
from `$TLA2TOOLS_JAR` for at most `conform_tlc_timeout_s`. The result is the
report's `conform` section (kept when a passing run is pruned) and its
result's `conformance`. For a bundle whose claims are approved, a claimed
property violated on the run's observed states, or a step the model cannot
take, makes the run a `FAIL`, and a check that could not decide (TLC
missing, too, for a bundle that needs it; TLC stopped at
`conform_tlc_timeout_s`; the trace the run recorded missing) makes it an
`ERROR`; either is in the run's summary (`spec conform replication: ...`)
and failure signature (the failing component `spec-conform:<bundle>`). A
draft bundle's findings are reported and change nothing. A run that reached
its verdict but whose report has no `conform` section (its worker was
stopped during the check) is an `ERROR` for an approved bundle, with that as
its reason, and reported only for a draft.

`summary.json` has:

| Key | Contents |
| --- | --- |
| `format_version` | The version of the files the fuzzer writes, `1`, as in a [run report](#run-reports). `batch.json` carries it too, and `--resume` refuses a folder another version wrote. |
| `status`, `exit_code`, `complete` | The worst result (see [Exit codes](#exit-codes)), its exit code, and whether every job finished. |
| `batch_id`, `folder`, `profile`, `created_at`, `updated_at`, `jobs`, `finished` | The batch and how far it got. |
| `counts` | Runs by status, zoo runs, serial re-runs, and smoke runs left out. |
| `smoke` | Runs of `smoke` mode by status: they only start and stop a deployment and make no coverage claim, so their `PASS` is not counted with an experiment's. |
| `cores` | The cores each run could use. A random job's scenario depends on it (see [Seeds](contracts.md#seeds-replay-and-provenance)). |
| `runs` | Every finished job: `id`, `source`, `status`, `summary`, `experiment`, `seed`, `attempt` (the draw that gave the seed's scenario, for a generated run), `mode`, `report` (its run report, relative to the folder; `null` when it left none), `signature_key`, `duration_s`, `worker`, `leaks` (a count), `origin` (the job a re-run repeats). |
| `signatures` | The non-PASS runs grouped by the minimizer's [failure signature](#minimize) and experiment (a crash by its causes alone, across experiments), most runs first: `key`, `label`, `status`, `experiment`, `signature`, `count`, `runs`, and `serial_reruns`: `runs` (each re-run's `id`, `status`, `same_signature`, and its own `signature_key` and `signature_label`), `reproduced` (how many failed the same way), and `verdict`: `reproduced` (all did), `intermittent` (some did), `failed differently` (none did, but some failed another way; the summary printout gives their signatures), or `not reproduced` (all passed, which points at load). |
| `zoo` | `valkey_commit`, `cases` (each case's controls, per-scenario detection rates, and whether it `holds`: true, false, or null for a case with an unobserved `ERROR` or `SKIP_UNSUPPORTED` run, which `undecided` then names), `skipped` (cases this host cannot run), `held`, `not_held`, and `undecided`. |
| `conformance` | Per spec bundle the runs' plans name: `status` (`pass`, `fail`, or `error` over the checked runs; `skipped`, `unexercised`, or `not_run` when none was checked), `approved`, `counts` (runs by conform status: `pass`, `fail`, `error`, `skipped` without TLC, `unexercised` for a run that took no mapped step, `not_run` for a run that ended `ERROR` or `SKIP_UNSUPPORTED`), `skipped` (why), `violations` (each run, property, whether it was `claimed`, and the `first` failing step), `transition_violations` (each run whose steps the model cannot take: the first such record, `index`, `from`, `to`, `event`), `errors`, `action_coverage` (runs observing each mapped action), `never_observed`, `targets_reached`, and `report`, the bundle's full conform report under `conformance/`. |
| `leaks` | Every leftover the sweeps removed, with the run that left it. |
| `invalid` | Matrix combinations that did not compile. |
| `coverage` | Per experiment, the runs and the distinct coverage tuples they covered. |

### Resuming

A batch stopped with Ctrl-C or `SIGTERM` interrupts its running jobs,
records no result for them, removes what they left, and exits `130`;
`batch --resume DIR` runs what is left. A batch killed outright (`SIGKILL`,
a power loss) can leave its workers running: the resume stops them first,
sweeps what they left, and runs their jobs again; a slot whose sweep could
not remove what its job left runs no more jobs. A finished job never runs
again. Only one process runs a batch folder at a time.

A resume refuses every option but `--workers` and `--allow-unsupported`
(the batch keeps the options it was created with), and `--dry-run`. A batch
with random jobs records a digest of the fuzzer and the
experiments its draws depend on; a resume where they changed is refused,
since its random jobs would draw other scenarios than their seeds did.

## validate

```bash
valkey-fuzzer validate examples/valkey/cluster_chaos.yaml --verbose
valkey-fuzzer validate drafts/*.yaml --format json
```

Compiles each scenario and runs every check that needs no server: fields,
inputs, references, component config, deployment fields, instance planning,
and startup overlay selectors. Checks that need acquired artifacts, running
components, or the Valkey binary happen when the scenario runs. `--verbose`
lists each component with its role, registration, `required` flag, and
dependencies. The exit code is 0 only when every file is valid, and 2
otherwise.

`--strict` adds warnings that do not change the exit code. Today it warns when
none of the experiment's own components is required evidence: the experiment
then passes on what `valkey.base` and the other fragments require alone, which
never shows that the components it adds ran (`--format json` lists them under
`warnings` for each file).

`--format json` is for tools that write scenarios (an AI agent proposing
them, say). All files are checked with one catalog:

```json
{
  "ok": false,
  "files": [
    {"file": "drafts/a.yaml", "ok": true, "errors": [], "experiment": "valkey.aof-durability", "scenario_id": "a"},
    {"file": "drafts/b.yaml", "ok": false,
     "errors": [{"path": "components.data.config",
                 "message": "component 'valkey.oracle.ledger' rejected 'data': every instance runs appendfsync everysec, ...: loss_window_s must be at least 2.5 (got 1)"}]}
  ]
}
```

`path` is the dotted location in the scenario (`""` for the whole file, such
as a YAML syntax error or a missing file); a path that starts with
`components.<id>` or `startup_overlays.<id>` names the experiment's component
or overlay whose config, after the scenario's inputs were substituted, was
rejected. Compilation stops at the first error, so each failing file lists
one.

## list

```bash
valkey-fuzzer list experiments
valkey-fuzzer list components --format json
valkey-fuzzer list generators
```

`experiments` prints `<id> [<modes>] <description>`, `components` the
role, registration ID, and Python class, and `generators` each generator
hook's ID and Python function.

`--format json` prints the machine-readable catalog, for tools that write
scenarios. Everything is derived from the installed catalog and the same
`ConfigSchema`s the compiler checks with, so it cannot drift from what a
scenario must meet. Keys are sorted and rows are in a fixed order
(experiments and generators by ID, components by role and then ID), so the output of one
installation is byte-for-byte stable.

Each experiment (abbreviated):

```json
{
  "id": "valkey.snapshot-equivalence",
  "description": "Deterministic workload, quiesced BGSAVE, and dataset/RDB/reply digests ...",
  "mode": "experiment",
  "directory": null,
  "deployment_modes": ["standalone", "cluster"],
  "deployment_requirements": {"type": "object", "properties": {}, "required": [], "additionalProperties": true},
  "inputs": {
    "type": "object", "required": [], "additionalProperties": false,
    "properties": {"operations": {"type": "integer", "minimum": 1, "maximum": 1000000, "default": 3000}, "...": {}}
  },
  "generator": {
    "declaration": {"inputs": {"value_size": {"values": [1, 64, 1024, 8192]}}, "server_config": ["valkey.pool.persistence"]},
    "pools": {"valkey.pool.persistence": {"appendonly": ["yes", "no"], "appendfsync": ["always", "everysec", "no"]}}
  },
  "components": [{"id": "writes", "use": "valkey.workload.key-value", "role": "workload", "depends_on": [],
                  "required": true, "timeout_s": null, "config": {"max_ops": {"$ref": "inputs.operations"}}}],
  "startup_overlays": [],
  "required_events": ["writes.drained"],
  "required_evidence": [{"evidence_id": "valkey.dataset.digest", "source": "dataset", "min_count": 1}]
}
```

- `inputs` and `deployment_requirements` are JSON Schemas: `type` (a list
  when a field takes several; `environment_reference` for a field that takes
  `{$ref: artifacts|resources...}`, with `reference_kinds`), `enum`,
  `minimum`, `maximum`, `default`, `description`, and the `required` names.
- `generator` is null for an experiment without `metadata.generator`;
  otherwise `declaration` is the block after its fragments are merged in and
  `pools` the server config pools its `server_config` names.
- `directory` is the `--experiment-dir` the experiment was loaded from;
  `null` for a shipped experiment.
- `required_evidence` is what the experiment itself requires. A compiled
  plan also requires what each component's config marks as required (such as
  an observer expectation with `required: true`), which depends on the
  scenario's inputs, so it shows only in the plan.

Each component has `component_id`, `role`, `implementation`,
`deployment_modes` (null: every mode), `config`, the JSON Schema of its
config, and `declares`: its `events` (name to payload schema), `evidence`
(ID to `{many, value}` schema), and `changes` (see
[Evidence contract](contracts.md#evidence-contract)).

Each generator hook has `generator_id` and `implementation` (the function). Generator hooks are not components and `list components` does not
show them.

## replay

```bash
valkey-fuzzer replay report.json --output replay.json
```

Reads a run report (any command's `report.json`, or a `--output` copy of one)
or a bare `plan.json`, and runs the recorded execution plan again with the
fuzzer and binaries installed now. Each replay writes its own
report folder under `<root>/replays/`; `--output` saves a copy of its report.

Before it starts anything, replay compares this environment with the report's
`reproducer` and the deployment's recorded instances:

| Compared | Recorded as |
| --- | --- |
| execution plan digest | `reproducer.plan_digest` |
| the installed experiment's digest | `reproducer.experiment.digest` |
| each used component's import path | `reproducer.components[].implementation` |
| the fuzzer's code (SHA-256 over every file of the `valkey_fuzzer` package, the Valkey components included) | `reproducer.fuzzer_digest` |
| the deployment provider's identity | the plan's `deployment_provider` |
| the deployment provider's import path and code (its whole top-level package) | `reproducer.deployment_provider` |
| each instance's Valkey binary, by SHA-256 | `outcome.diagnostics.deployment.instances` |
| each artifact, by the SHA-256 its provider recorded (after the run) | `outcome.diagnostics.environment_report` |

The report's scenario must also compile, here, to the plan it records. If
anything differs, replay still runs. It prints each difference to stderr as a warning and records it in the
replay report under `provenance.drift` (see [run reports](#run-reports)):

```text
Warning: drift from the recorded run: fuzzer code: recorded 3f1c9a0e5b2d..., replay uses 8d07e4c1a96f...
```

| Option | Meaning |
| --- | --- |
| `--valkey-binary PATH` | Run the instances that use the deployment's default binary on this binary instead. It is recorded as an intentional override, not as drift; instances with their own `instance_binaries` entry are still compared. The replay report's `scenario` carries it as `deployment.valkey_binary`, so `minimize` and `replay` of that report run it too. |
| `--artifact NAME=PATH` | Resolve the recorded plan's `valkey.artifact.local-binary` or `valkey.artifact.local-module` artifact `NAME` at `PATH` (a downloaded CI batch's `artifacts.parent` or `artifacts.module`, rebuilt here). Recorded under `provenance.artifact_overrides` and in the replay report's `scenario`; the binary that ran is still digested and compared with the recorded run after it, so a rebuild whose bytes differ is reported as drift. |
| `--output PATH` | Save a copy of the replay report. |

A binary that an artifact provider supplies is only known once the replay's
deployment runs, so it is compared after the run, and so is every artifact
whose provider recorded a `sha256` in its provenance (such as an upgrade
target no instance starts on); a difference is warned about and recorded like the rest; it does not change the exit code. A
deployment provider whose identity differs from the plan's still stops the run, as `ERROR`. When the recorded run lists no
instances at all, the Valkey git SHA is compared after the run instead. What
could not be compared, such as an instance recorded without a digest, is
printed as `Note: not verified: ...`. A digest missing on either side, and a
different Python `major.minor` or platform, are drift. A run writes
`reproducer.json` beside its `plan.json` before it starts, so `replay
<folder>/plan.json` is verified (against the folder's `report.json` when it has
one). A bare `plan.json` with neither beside it has no provenance: replay runs it,
warns that nothing could be verified, and records `provenance.verified` as `false`.

Replay is not hermetic even when nothing drifted: the seed fixes each
component's random choices, not timing.

## Comparing two builds

There is no command for it: an experiment gives `valkey.action.commands` a
`compare_with` binary, as [Comparing two builds](scenarios-and-experiments.md#comparing-two-builds) describes.

## minimize

```bash
valkey-fuzzer minimize report.json --output minimized.yaml --max-attempts 30
```

Needs a run report that contains the scenario (any command's `report.json`,
or a `--output` copy) and a `FAIL` or `ERROR` outcome. It first runs the
scenario as recorded: when that run does not fail the same way, nothing is
minimized and the command exits `2`. Then it tries one reduction at a time,
most aggressive first. Steps 1 and 2 are the local deployment's (its
provider's `reductions` hook); a deployment provider without the hook leaves
the deployment as it is:

1. fewer replicas, then fewer primaries: the experiment's minimum
   `replicas_per_primary` or `primaries` (0 without one), then half; a size
   the deployment provider refuses (a cluster of fewer than 3 primaries)
   does not compile;
2. dropping each `deployment.server_config` key and each
   `deployment.instance_configs` entry;
3. each positive numeric input the scenario sets whose experiment marks it
   `shrinkable` (a count, rate, or size, where smaller means less of the same
   work; never a shard index, a delay, or a loss window), lowered to the
   input's minimum, then halved.

A reduction is kept when the failure signature still matches: the same
outcome, the same failing components with the same status and verdict each
(the error type and phase of a call that raised, or else its summary with
numbers and hex IDs masked), the same missing required events and evidence,
at least the same required evidence observed, and for `ERROR` the same error
type and phase. A run whose failing components report crashes of the system
under test (`crashes` in their diagnostics; `valkey.oracle.liveness` reports
each server crash by its assertion, panic, sanitizer finding, signal, or
fatal exit log line) has
those causes, numbers and hex IDs masked, and its outcome as its whole
signature: what failed
after the crash is left out, so the same crash in another experiment, or
with other components failing after it, is the same failure, and a run that
fails without crashing, or that reports the crash as an `ERROR` where the
original was a `FAIL`, is another.
After a kept reduction the search starts over from the first candidate,
skipping the reductions it already rejected.
`--max-attempts` (default 30, at least 1) bounds the candidate runs; a
candidate that does not compile does not count, and neither does the first
run of the recorded scenario nor the last run, which runs the minimized
scenario once more to confirm it. The command exits `0` only when a smaller
scenario was written and that confirmation failed the same way; it exits `2`
when the recorded failure did not reproduce, when no reduction kept it, or
when the result did not fail again. Before anything runs, minimize compares this
environment with the report's `reproducer` as [replay](#replay) does, and
warns on stderr when anything differs: a reduction that fails the
same way on other code proves less. It still runs. The minimized scenario goes to `--output`, or to
stdout. Each candidate run gets a report folder under `<root>/minimize/`
(`attempt-1` is the recorded scenario's run).

## findings

```bash
valkey-fuzzer findings list --store soak/20261120T0100
valkey-fuzzer findings list --store soak/findings.json --status new
valkey-fuzzer findings show 3f1c2a9b7d4e --store soak/findings.json
```

A **findings store** is a `findings.json` that batches append to when they
finish: one entry per failure signature, across batches. Each batch writes
the one beside its `summary.json`; `batch --findings PATH` (or `findings:` in
a batch spec) points several batches, a nightly soak's say, at one store. An
entry is one [signature key](#the-batch-folder) (`signature_key` in
`summary.json`, the key `minimize` prints) with:

| Key | Contents |
| --- | --- |
| `key`, `label`, `experiment`, `outcome`, `signature` | The signature as the batch's `summary.json` groups it (a crash by its causes alone, across experiments). |
| `status` | `new`: no earlier batch in this store had seen it. `known`: a second batch has seen it since (set when that batch records it). `fixed`: set by hand, in the file, when the bug is fixed; a fixed finding a batch sees again is `new` again. The command only reads. |
| `first_seen`, `last_seen` | When a batch first and last recorded a run of it (the summary's `updated_at`). |
| `count`, `runs` | How many runs hit it, and each one's batch folder and job id. A run counts once: a resumed or re-summarized batch adds nothing. |
| `reproduce` | The command that runs the latest of them again: `valkey-fuzzer replay <its report.json>`, or `valkey-fuzzer run <its scenario.yaml>` for a run that left no report; `null` when neither exists. |

The file is `{"format_version": 1, "findings": {<key>: <entry>}}`, rewritten
whole and atomically; a store another `format_version` wrote is refused as a
[run report](#run-reports) is. `list` prints one line per finding (status,
runs, first and last seen, key, label), `new` before `known` before `fixed`
and the last seen first; `--status` keeps one status. `show KEY` prints one
entry as JSON; `KEY` may be a prefix that names exactly one finding. `--store`
is the file, or a batch folder, whose `findings.json` is read; without it,
`findings.json` in the working directory. A missing store is an input error
(exit `64`).

## cleanup-logs

```bash
valkey-fuzzer cleanup-logs 176 --force
```

Deletes `<seed>-*.log` (instance logs and captured output) from
`/tmp/valkey-fuzzer/logs`, or from `DIR/logs` with `--data-dir DIR`. Without
`--force` it lists the files and asks first. It fails (exit `2`) when the
prompt is declined or a file could not be deleted, such as another user's
in the shared `logs/`.

## sweep

```bash
valkey-fuzzer sweep --dry-run
valkey-fuzzer sweep --older-than 3600 --format json
sudo valkey-fuzzer sweep            # Linux: also iptables chains, netem devices, and tmpfs mounts
```

Removes what fuzzer processes that are gone left on this host. A fuzzer
killed with `SIGKILL` (or by a power loss) never cleans up, so every host
change it makes names its owner: the fuzzer's PID plus a hash of that
process's start time, written `<pid>-<start>`, so a reused PID is not taken
for the owner.

| Host change | Tag |
| --- | --- |
| deployment directory, `valkey.resource.tmpdir` directory, private server directory | a `.valkey-fuzzer-owner` file (JSON: `owner`, `pid`, `kind`, `label`, `created_at`) |
| `valkey-server` process, and the children it forks (`valkey-rdb-bgsave`, `valkey-aof-rewrite`, ...), which outlive it | `valkey-fuzzer-owner=<owner>` in its process title (`--proc-title-template`, which scenarios may not set) |
| iptables chain (`valkey.fault.network-partition`) | the name `VF-<owner>-<hash>` |
| netem device (`valkey.fault.network-degrade`) | the `ifb` device name `vf<pid in hex><start><suffix>`; the `lo` ingress filters that redirect to it are found by that name, and `lo`'s ingress qdisc by the `lo-ingress.added` marker in `/tmp/valkey-fuzzer` |
| tmpfs mount (`valkey.resource.tmpfs`) | the mount point `<tmp>/valkey-fuzzer-mounts/<owner>/<name>`; an empty one (made before its mount) is removed too |

The sweep lists every tagged item whose owner is no longer running and,
without `--dry-run`, kills the servers (`SIGKILL`), deletes the chains (and
their jumps from `INPUT`), removes the netem devices (their `lo` filters first,
then `lo`'s ingress qdisc once the fuzzer added it and nothing is left on it,
then the device), unmounts the mounts, and removes the directories,
in that order. It never touches anything untagged, anything whose owner is
running, or anything whose owner it cannot rule out; it re-checks a server's
tag right before killing it and a directory's tag right before removing it
(one whose tag changed in between is reported as `skipped`, with the reason),
and refuses a directory that reaches into another filesystem. A directory
counts only when it and its tag file belong to the user running the sweep
(root too), so a tag another user wrote is never acted on. A sticky one never
counts, whatever tag is in it: it is a [shared directory](#report-folders),
such as `/tmp/valkey-fuzzer` or its `runs/`, holding every user's runs. Before removing a
directory it makes each one inside writable through a descriptor opened
without following symlinks, so a symlink swapped in never has its target
changed.

| Option | Meaning |
| --- | --- |
| `--dry-run` | List, remove nothing. |
| `--older-than SECONDS` | Only leftovers at least this old (servers by process age, directories by their tag, mounts by their owner directory); a chain or a netem device has no age, so it is left alone. |
| `--root DIR` | Look for tagged directories in the children of `DIR` instead of the defaults (`/tmp/valkey-fuzzer`, `/tmp`, the temporary directory); may be repeated. Use it for a custom `base_data_dir` or `valkey.resource.tmpdir` `parent`. Servers, chains, netem devices, and mounts are found host-wide, servers only among the processes of the user running the sweep, in its PID namespace (a fuzzer in a container is left to its own sweep). |
| `--format text\|json` | JSON: `{dry_run, ok, leftovers: [{kind, target, owner, age_s, detail, action, reason}], unchecked}`; `kind` is `server`, `chain`, `netem`, `mount`, `mount point`, or `directory`. |

Listing chains, removing netem devices, and unmounting need root. Without it,
a leftover mount or netem device is reported as `failed` and chains as not
checked. The exit code is 0 when
everything found was removed (or with `--dry-run`), and 2 when anything could
not be. A deployment kept with `enable_cleanup: false`, and a
`valkey.resource.tmpdir` with `keep: true`, lose their tag when the run ends,
so the sweep leaves them. Instance logs under `<base_data_dir>/logs` are
evidence, not leftovers (`cleanup-logs` removes them).

## fetch-valkey

```bash
valkey-fuzzer fetch-valkey                 # the pinned 9.1.3 commit a deployment builds without a binary
valkey-fuzzer fetch-valkey 9.1.3           # a tag
valkey-fuzzer fetch-valkey unstable        # a branch, as it is now
valkey-fuzzer fetch-valkey 29173b67508b2de4c0f0ed551f3b47cf19fc3ca8
```

Builds one `REF` of `valkey-io/valkey` once and prints the path of its
`valkey-server`, and nothing else on stdout, so a script can capture it
(`--valkey-binary "$(valkey-fuzzer fetch-valkey 9.1.3)"`). A tag or branch
is resolved to its commit first (`git ls-remote`; an annotated tag to the
commit it tags; a tag before a branch of the same name); a full 40-character
commit hash is taken as it is, and a shorter one is refused. The build goes
to `$XDG_CACHE_HOME/valkey-fuzzer/valkey/<commit>/` (`~/.cache/valkey-fuzzer/valkey/<commit>/`
by default): the commit itself is fetched into an empty repository and
checked out, so a moved tag changes nothing; `make -j` builds it aside and
the tree is renamed into place only once the build succeeded. The git and
make output goes to `<commit>.build.log` beside it, and one log line on
stderr says whether the cached build is used or a build starts and where.
The whole tree is kept: the config table is read from the `config.c` beside
the binary. Builds of different commits sit side by side; one at a time
builds (a lock in the cache), and the build is bounded to 30 minutes. The
command needs git, make, a C compiler, and network access.

A deployment that names no `valkey_binary` and finds no `valkey-server` on
`PATH` uses the pinned commit's build in this cache, and makes it on its
first run when it is not there
([Choose a Valkey binary](getting-started.md#choose-a-valkey-binary));
`valkey-fuzzer fetch-valkey` makes it ahead of time. The cache is the user's
own and safe to delete.

## spec

```text
valkey-fuzzer spec check BUNDLE [--tla2tools JAR] [--workers N] [--strict] [--configs DIR] [--require-approved] [--output FILE]
valkey-fuzzer spec translate BUNDLE (--control NAME | --trace TLC_OUTPUT_OR_JSON --config CFG) --out DIR [--variants N] [--seed N]
valkey-fuzzer spec translate BUNDLE (--reach TARGET... | --mutate | both) --out DIR [--tlc-timeout SECONDS] [--max-scenarios N]
valkey-fuzzer spec conform BUNDLE RUN... [--tla2tools JAR] [--group-by INPUT,...] [--no-transitions] [--output FILE]
```

Checks a TLA+ spec bundle (`formal/<protocol>/`, or a bundle directory),
translates its counterexamples into scenarios, and checks real runs against it:
its claims on the states a run shows, and (unless `--no-transitions`) that
the steps a run shows are steps of the model. `--tla2tools` defaults to
`$TLA2TOOLS_JAR`; the jar is probed (`tlc2.TLC -h`) for TLC's version, 2.19
or later, and a jar that is no release the bridge knows by SHA-256 is a
warning, or an error with `--strict`. `check --configs nightly` checks the
bounds raised in the bundle's `nightly/` configs instead of its own. `translate`
takes the catalog option (`--experiment-dir`) to compile what it
writes; with `--reach` or `--mutate` it searches the model for
counterexamples itself ([spec translate](spec-bridge.md#spec-translate)).
Exit codes: 0 pass, 1 a gate failed, a claimed property was
violated, or a step the model cannot take, 2 a harness error (no java or jar, which the command names with
the jar's download command, a jar that does not run TLC or runs one older than 2.19; a bundle that does not exist, a
SANY or TLC run that never started or was cut short by `--timeout`, a run
without mapped evidence or with a mapped record it cannot apply, or a
`translate` whose experiments do not load, whose TLC run timed out, or that
has no counterexample; a search whose TLC run failed). A failed gate outranks an error. See [Spec bridge](spec-bridge.md).

## Run reports

`report.json`, `run --output`, and `replay --output` hold one run report.
Every key below is always present, and
`replay` and `minimize` reject a report (or a reproducer) that lacks one,
except the reading aids (`failed_components`, `reproduce`, `look_at`,
`conform`, `reproducer.command`, `reproducer.valkey_binary`) and
`format_version`, which older reports lack.
The keys come in reading order: the format version, the verdict and where to
look first, then the data a tool reads. Tools should read keys by name, not by
position.

| Key | Contents |
| --- | --- |
| `format_version` | The version of the files the fuzzer writes, `1` (see [What is stable](stability.md)). `replay`, `minimize`, and `spec conform` refuse a report of another version, naming the file and both versions; a report without one is read as `1`. `plan.json` carries the same key, and so do a batch's `summary.json` and `batch.json`, and a `trace.jsonl` header. |
| `status`, `summary` | The outcome and its one-line summary. |
| `failed_components` | Each component that did not pass: `component_id`, `role`, `use`, `status`, `crash`, `summary`, `evidence_total`, and the first five `evidence` items (`evidence_id`, `value`). Text longer than 120 characters is cut and ends with its length and SHA-256; the full text is in `outcome`. Empty for a pass. A spec bundle's conform check that failed the run is the component `spec-conform:<bundle>` (no `role` or `use`: it is not a plan component), its `summary` the violated claims. |
| `reproduce` | The `valkey-fuzzer replay <report>` command that replays this run; `null` when the report was not written to a file. |
| `look_at` | `report_folder`, `plan`, `component_files`, and where the full results are (`outcome.component_results`). |
| `conform` | The run checked against each [spec bundle](spec-bridge.md#bundles) its plan names, by bundle ([`spec conform` in a run](spec-bridge.md#in-a-run)): `approved`, `status` (`pass`, `fail`, `error`, `skipped`, `unexercised`, `not_run`), `violated` (the claimed properties violated, and `transitions` for a step the model cannot take), `first_violated` (`claim`, the observed `state` that violates it, and that `step`), `tlc_seconds`, and the full check (`properties`, `transitions`, `states`, `actions`, `error` or `reason`, ...). `null` when the plan names no bundle. |
| `generated_at` | UTC timestamp |
| `scenario` | The scenario as run, including a `--valkey-binary` override. A replay records the scenario of the report it replayed with its own `--valkey-binary` applied (also under `provenance`), or `null` for a bare plan. |
| `outcome` | The full outcome: `plan` (the execution plan), `status`, `summary`, `component_results`, `evidence`, `diagnostics`, `started_at`, `ended_at`, and `valkey_sha`. |
| `coverage` | The run's coverage `key` (a digest of its `tuple`), the `tuple`, `status`, and the evidence IDs observed. |
| `reproducer` | `python`, `platform`, `experiment` (`id`, `digest`), `fuzzer_digest`, `components` (`component_id`, `role`, `implementation`), `deployment_provider` (`provider_id`, `implementation`, `implementation_digest`), `plan_digest`, `valkey_sha`, `command` (the same as the top-level `reproduce`), and `valkey_binary` (the binary the run used, or `null` when the server came from `PATH`); for a generated scenario also `generated` (`seed`, `experiment`, `attempt`, `excludes`, `boundary`, `profile`). |
| `provenance` | For a replay: `source`, `verified`, `drift`, `not_verified`, `valkey_binary_override`, and `artifact_overrides` (the `--artifact` paths by artifact name). `null` otherwise. `drift` is a list, empty when nothing differed, of `{"item", "recorded", "replay"}`: what differed, the value the recorded run had, and the value this replay has. It includes differences only running reveals (a binary or artifact digest). Replay and `minimize` never refuse because of it. |

Each entry of `outcome.component_results` has `component_id`, `status`,
`summary`, `evidence`, `diagnostics`, `started_at`, and `ended_at`; a
long-running component's teardown result is under `diagnostics.teardown`.
`outcome.diagnostics` holds:

- `deployment`: the deployment ID, mode, addressing, `valkey_sha`, and each
  instance's ID, role, shard, host, port, PID, binary, and `binary_sha256`;
- `events`: every published event, with `name`, `sequence`, `timestamp`,
  `source`, `payload`, and `committed` (whether its dispatch completed; only
  committed events count as observed);
- `environment_report`: artifact and resource acquisition and release;
- `obligations`: the `planned` changes, and the [undo
  obligations](contracts.md#fault-ownership) `undone` and `still_active`;
- when the run did not pass, the cause: `missing_required_events`,
  `missing_required_evidence`, `required_component_failures`,
  `untrusted_component_failures`, `contract_violations`, `open_obligations`,
  `cleanup_errors`, `abandoned_calls`, or `error`.

A failed state validation (of `valkey.oracle.deployment-state` or a chaos
action's wave) records
`failed_checks`, `error_messages`, `inconclusive` (nothing was judged), and each check's result under `checks`,
plus `topology`: every instance's live role, shard, primary, status, and
slot count as the checks saw it. A failed data-consistency check names up to
ten missing test keys in its error and lists up to five wrong keys under
`checks.data_consistency.examples`, each with what every instance held.

A coverage tuple holds:

| Tuple field | Holds |
| --- | --- |
| `experiment` | the experiment ID |
| `deployment` | `mode`, `provider`, `roles` (how many planned instances have each role), and `shards` |
| `config_mix` | a digest of each planned instance's role and effective server config, and the startup overlays |
| `deployment_mix` | a digest of the deployment document but for its provider's host fields (the local deployment's `base_port`, `base_data_dir`, `instance_data_roots`, `loopback_block`, `enable_cleanup`), so another binary is another combination and other ports are not |
| `inputs` | the resolved inputs |
| `artifact_mix` | a digest of the artifact declarations with their config |
| `resources` | resource provider IDs |
| `workloads`, `actions`, `faults`, `observers`, `oracles` | the component registration IDs of each role |
| `resource_mix` | a digest of the resource config |
