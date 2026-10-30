# Bug zoo

The bug zoo is a permanent set of deliberately broken Valkey builds and
configurations. Each case is paired with the scenarios that must FAIL on it,
and with the components that must report the failure. It proves that the
fuzzer catches bugs, and it shows when a check stops seeing them.

- [Layout](#layout)
- [case.yaml](#caseyaml)
- [How a case is judged](#how-a-case-is-judged)
- [Building](#building)
- [Running](#running)
- [Adding a case](#adding-a-case)

## Layout

```text
zoo/
  valkey-commit            # the one Valkey commit every patch applies to
  build.sh                 # clones Valkey and builds the clean and patched binaries
  <case id>/
    case.yaml              # what is broken, what must catch it, how often
    patch.diff             # a build-level case: a patch to Valkey at valkey-commit
    scenario.yaml          # the scenario(s) case.yaml names
```

A case with a `patch.diff` is build-level. A case without one is
configuration-level: its `overrides` break a clean build. The loader and the
verdict are in `valkey_fuzzer/zoo.py`; the cases run through the
[batch runner](cli.md#batch).

## case.yaml

```yaml
description: A replica silently skips every 20th SET it receives from its primary.
catches: valkey.oracle.ledger replica convergence (check_replicas)
scenarios:
  - file: scenario.yaml
    fail: [data]                          # component IDs that must FAIL
    missing: []                           # required evidence IDs that must be reported missing
overrides:                                # optional: merged onto every broken run's scenario
  deployment:
    server_config: {maxmemory: 4mb}
runs: 5                                   # broken runs per scenario, at least 5
min_detection_rate: 1.0                   # share of broken runs that must detect the bug
requires: [linux, root]                   # optional: skipped elsewhere
```

Each scenario names at least one component in `fail` or one evidence ID in
`missing`. Some checks are not components: an `info` observer's expectation
or a `logs` observer's expected line fails the run as missing required
evidence (`outcome.diagnostics.missing_required_evidence`).

Unknown fields are rejected. A scenario file is an ordinary scenario without
`deployment.valkey_binary`, because the zoo picks the build. The batch runs it
on its worker's port block and data directory.

## How a case is judged

- **Control.** Each scenario runs once on the clean build at the pinned commit,
  without the overrides. It must PASS. A control that fails or errors means
  the case proves nothing, so the case does not hold.
- **Broken runs.** Each scenario runs `runs` times on the patched build (or
  the clean build with the overrides), with seeds `seed`, `seed + 1`, and so
  on. A run detects the bug only when its outcome is FAIL, every component
  in `fail` reports FAIL, and every ID in `missing` is reported as missing
  required evidence. A run that fails for some other reason does not count:
  that check has gone blind.
- **Verdict.** A case holds when every control passes and every scenario's
  detection rate is at least `min_detection_rate`.

## Building

```bash
zoo/build.sh BUILDS [CASE ...]
```

The script clones `https://github.com/valkey-io/valkey` into `BUILDS/source`
once. It builds the commit in `zoo/valkey-commit` into `BUILDS/clean`, then
builds each case's patch into `BUILDS/<case id>` from a copy of the clean
tree, so only the patched files recompile. It runs on Linux and macOS. `JOBS`
sets the parallelism, and `MAKE_ARGS` passes extra make variables. Before a
case runs, the zoo checks that each binary's `valkey-server --version` names
the pinned commit.

## Running

```bash
valkey-fuzzer batch --zoo BUILDS --workers 2                     # every case
valkey-fuzzer batch --zoo BUILDS --zoo-case replica-skips-writes  # one case
VALKEY_ZOO_BUILDS=BUILDS pytest -m live -k zoo tests/test_live_zoo.py
```

The zoo runs through the batch runner, alone or as one source of a larger
batch (a `zoo:` source in a [batch spec](cli.md#sources), as every soak
does). Each control and broken run is one job; a case whose `requires` this
host does not meet is listed as skipped. The batch summary's `zoo` holds each
case's controls, its broken runs (status, failed components, missing
evidence, whether each detected the bug, report), its detection rate per
scenario, and whether it holds. The zoo's broken runs are expected to fail,
so they are left out of the batch's counts and failure signatures; a case
that does not hold makes the batch `FAIL` (exit `1`), and a skipped case
makes it `SKIP_UNSUPPORTED` at worst. Each run's report folder is
`runs/zoo-<case>-<scenario file stem>-control` or `-<n>` in the batch folder.

## Adding a case

1. Make the smallest patch that produces the misbehavior, against a clone at
   `zoo/valkey-commit`. Mark it with a `Bug zoo:` comment, and save it with
   `git diff > zoo/<case id>/patch.diff`. A configuration-level case sets
   `overrides` instead.
2. Choose scenarios whose clean run passes and name the component that must
   catch the bug.
3. Run `zoo/build.sh BUILDS <case id>` and then
   `valkey-fuzzer batch --zoo BUILDS --zoo-case <case id>`. Set
   `min_detection_rate` from what the runs show.

Moving `zoo/valkey-commit` means rebasing every patch and rerunning the whole
zoo.
