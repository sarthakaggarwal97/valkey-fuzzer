# Fuzzing a module from its own repository

This page is for the maintainers of a Valkey module (valkey-json,
valkey-bloom, valkey-search): how to run the fuzzer against the module from
the module's repository with one short workflow, what the run uploads, and
what to do with a `FAIL`. It replaces copying the fuzzer's own setup (build
Valkey, build porcupine, install the fuzzer, write an experiment) into every
module repository.

- [The caller workflow](#the-caller-workflow)
- [The base file: the module's commands](#the-base-file-the-modules-commands)
- [Inputs](#inputs)
- [The image](#the-image)
- [What gets uploaded](#what-gets-uploaded)
- [Reading a FAIL](#reading-a-fail)
- [Your own experiment](#your-own-experiment)

## The caller workflow

`.github/workflows/module-fuzz.yml` is a
[reusable workflow](https://docs.github.com/en/actions/how-tos/reuse-automations/reuse-workflows).
A module repository calls it from a workflow of its own, pinned to a release
tag or a commit SHA of the fuzzer, never to a branch:

```yaml
name: Fuzz
on:
  schedule: [{cron: '15 3 * * *'}]
  workflow_dispatch:
permissions: {contents: read}
jobs:
  fuzz:
    uses: valkey-io/valkey-fuzzer/.github/workflows/module-fuzz.yml@v0.3.0
    with:
      fuzzer_ref: v0.3.0            # the same tag or SHA as `uses:`
      build: ./build.sh             # builds the module in your checkout
      module: .build-release/libsearch.so
      base: .github/fuzz/base.yaml  # the module's commands (next section)
```

The job checks out the module repository and the fuzzer at `fuzzer_ref`,
builds Valkey (`valkey_version`, 9.1.3 by default) and the porcupine checker,
runs `build` in the module's checkout, and runs a [batch](cli.md#batch) of
`seeds` random seeds of `experiment` with the module loaded into every
server (`deployment.server_config.loadmodule`). A called workflow's
`github.repository` and `github.sha` are the caller's, which is why
`fuzzer_ref` repeats the tag `uses:` names: the job has no other way to know
which fuzzer commit it is.

The job fails only on a `FAIL`: an oracle saw the module, or Valkey with it,
do something wrong. An `ERROR` (the harness could not reach a verdict: a
server that did not start, a timeout) or a `SKIP_UNSUPPORTED` (the build
lacks `module_command`, or no module was loaded) is written to the job
summary with a warning and does not fail the job, unless `fail_on_error` is
`true`. The exit codes are [the batch's](cli.md#exit-codes).

## The base file: the module's commands

The default experiment, `example.module-chaos`
([`examples/valkey/experiments/module-chaos.yaml`](../examples/valkey/experiments/module-chaos.yaml)),
is the shape the fuzzer's own
[search chaos workflow](../.github/workflows/search-chaos.yml) runs for
valkey-search, with the module's commands as inputs: the module's data is
written before and while a replica is killed, stopped or restarted (and the
primary stalled), read back on every instance, and every replica's values
digested against its primary's. The module repository supplies the data in
a partial scenario file, the `base` input, merged over every random draw:

```yaml
# .github/fuzz/base.yaml
experiment:
  inputs:
    module_command: JSON.SET          # a command of the module; a build without it is skipped
    load:                             # written before the faults
      - {command: [JSON.SET, "j:1", "$", '{"a": 1, "tags": ["x"]}'], expect: {equals: OK}}
      - {command: [JSON.ARRAPPEND, "j:1", "$.tags", '"y"']}
    during:                           # written while the replica is hit and the primary stalled
      - {command: [JSON.SET, "j:1", "$.a", "2"]}
      - {command: [JSON.SET, "j:2", "$", '{"b": [1, 2, 3]}'], expect: {equals: OK}}
    verify:                           # read on every instance once the replicas are linked
      - {command: [JSON.GET, "j:1", "$.a"], expect: {equals: "[2]"}}
      - {command: [JSON.ARRLEN, "j:1", "$.tags"], expect: {equals: [2]}}
      - {command: [JSON.ARRLEN, "j:2", "$.b"], expect: {equals: [3]}}
    forbidden_logs:                   # optional: a log line matching one fails the run
      - {pattern: "Index validation failed"}
```

Each item is a [`valkey.action.commands`](components/actions.md#valkeyactioncommands)
command: `command` and an optional `expect`. A `verify` read runs on the
primary and every replica, so its reply must be the same everywhere. A few
hundred commands is the usual size; the fuzzer's own
[`module_chaos.py`](../.github/scripts/module_chaos.py) shows what it writes
for valkey-json, valkey-bloom and valkey-search. Server config the module
needs at start goes beside `loadmodule`, which the workflow sets (a base
that sets `loadmodule` itself is refused):

```yaml
deployment:
  server_config: {search.rdb-validate-on-write: "yes"}
```

## Inputs

| Input | Default | Meaning |
| --- | --- | --- |
| `module` | required | Path of the module's `.so` in the checkout, once built. |
| `fuzzer_ref` | required | The fuzzer tag or SHA `uses:` names. |
| `build` | none | Shell command that builds the module, run in the checkout (`sudo apt-get install ...` works on the runner). Empty when the module already exists. |
| `module_artifact` | none | An artifact an earlier job uploaded, unpacked into the checkout before `build`: a prebuilt module, which the [image](#the-image) needs. |
| `experiment` | `example.module-chaos` | A shipped experiment, the example, or a `proposed.*` one in `experiment_dir`. |
| `experiment_dir` | none | Directory of data-only experiments in the checkout ([your own experiment](#your-own-experiment)). |
| `base` | none | The partial scenario file above. |
| `seeds`, `workers` | 20, 2 | Fresh seeds, and parallel workers; a run takes about four cores, a GitHub runner has four. |
| `profile` | `pr-quick` | A [search profile](scenarios-and-experiments.md#legal-space-and-search-profiles): `pr-quick`, `nightly-soak`, `adversarial`. |
| `run_timeout_s`, `timeout_minutes` | 900, 120 | One run's deadline, and the job's. |
| `fail_on_error` | `false` | Fail the job on `ERROR` and `SKIP_UNSUPPORTED` too. |
| `image` | none | Run in the [image](#the-image) instead of building Valkey and porcupine on the runner. |
| `valkey_version`, `valkey_make_args` | `9.1.3`, none | On the runner: the valkey-io/valkey tag, branch or commit to build, and `make` arguments (`SANITIZER=address CC=clang`). |
| `fuzzer_repository` | `valkey-io/valkey-fuzzer` | Where `fuzzer_ref` is checked out from. |
| `artifact_name` | `module-fuzz-<run id>` | The uploaded batch folder. Set it when one run calls the workflow twice. |

## The image

`ghcr.io/valkey-io/valkey-fuzzer:<tag>` ([`Dockerfile`](../Dockerfile)) holds
the fuzzer, a pinned Valkey build (`valkey-server` and its tools on `PATH`,
`/opt/valkey/COMMIT` names the commit), the porcupine checker, and what the
faults call: `iptables`, `tc`, `libfaketime`, a C compiler. It is built by
[`build-image.yml`](../.github/workflows/build-image.yml) on every pull
request that touches it and pushed on a release tag; about 380 MB.

```bash
docker run --rm -v "$PWD:/work" ghcr.io/valkey-io/valkey-fuzzer:v0.3.0 \
  batch --random example.module-chaos --seeds 20 --base /work/base.yaml \
  --experiment-dir /opt/valkey-fuzzer/examples/valkey/experiments --out /work/batch
```

The entrypoint is `valkey-fuzzer`; the examples are under
`/opt/valkey-fuzzer/examples`. The container runs as user `fuzzer` (uid
1000): add `--user "$(id -u)"` when a mounted directory must stay writable
by you. Process signals, the clock skew, the syscall faults and the
permission faults run as that user. A network partition (`iptables`) or
degradation (`tc netem`) needs `--user root --cap-add NET_ADMIN`, and a
size-limited tmpfs needs root and `--cap-add SYS_ADMIN`; the
resource-pressure fault (cgroup v2) does not run in a container. A fault the
container cannot run makes the run `SKIP_UNSUPPORTED` or `ERROR`, never
`FAIL`.

In the workflow, `image:` runs the job in that container, so nothing is
built: the job skips the Valkey and porcupine builds and installs the fuzzer
at `fuzzer_ref` over the image's. The container has no `sudo`, `cmake` or
`clang`, so build the module in a job of your own, upload it, and pass
`module_artifact`:

```yaml
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@<sha>
      - run: ./build.sh
      - uses: actions/upload-artifact@<sha>
        with: {name: libsearch, path: .build-release/libsearch.so}
  fuzz:
    needs: build
    uses: valkey-io/valkey-fuzzer/.github/workflows/module-fuzz.yml@v0.3.0
    with:
      fuzzer_ref: v0.3.0
      image: ghcr.io/valkey-io/valkey-fuzzer:v0.3.0
      module_artifact: libsearch
      module: libsearch.so
      base: .github/fuzz/base.yaml
```

Use the same tag for `uses:`, `fuzzer_ref` and `image`.

## What gets uploaded

The job writes the batch summary to the job summary: the batch status, each
run's seed, status, Valkey SHA and failed components, and the failure
signatures. It uploads `module-fuzz-<run id>` (60 days), holding:

| File | Contents |
| --- | --- |
| `base.yaml` | The base the batch ran with: your base plus the module's path. |
| `batch/summary.json` | The [batch folder](cli.md#the-batch-folder)'s summary: counts, every run, the failures grouped by signature. |
| `batch/runs/<run>/` | One folder per run: `scenario.yaml` (rerun it with `valkey-fuzzer run`), `plan.json`, `report.json`, the components' files, and `logs/`, every instance's log. |

## Reading a FAIL

A `FAIL` means an oracle saw something wrong: a `verify` read that replied
otherwise than expected on some instance, a replica whose values differ from
its primary's (`digest`), an acknowledged write that was lost (`data`), a
crash or a forbidden log line (`liveness`). [Reading a FAIL](reading-a-fail.md)
shows where the evidence is in a run folder and what to attach to an issue;
`report.json` starts with `failed_components` and `reproduce`.

To rerun a failed run, download the artifact, build the module at the same
path `base.yaml` names (or edit `loadmodule` in the run's `scenario.yaml`),
and run `valkey-fuzzer run batch/runs/<run>/scenario.yaml --experiment-dir
examples/valkey/experiments` from a fuzzer checkout at `fuzzer_ref`, with a
`valkey-server` of the same version on `PATH`, or in the image with the
artifact and the module mounted. The seed and the draw are in the scenario,
so the same faults fire at the same points.

## Your own experiment

An experiment of your own is a data-only YAML file in your repository
(`experiment_dir`), with an `id` starting with `proposed.`, named by
`experiment`. Copy [`module-chaos.yaml`](../examples/valkey/experiments/module-chaos.yaml)
and change what you need: another fault, a longer settle, an oracle of the
module's own, such as `valkey.oracle.search-results`
([Scenarios and experiments](scenarios-and-experiments.md) has the format,
[Valkey components](components/README.md) every component). The `base` input works
the same way; its `experiment.inputs` must match what your experiment
declares.
