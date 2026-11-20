# Reading a FAIL

A run that ends `Status: FAIL` (exit code `1`) means an oracle or action saw
Valkey, or the server's configuration, do something wrong. This page shows
where the evidence is and what to send with a Valkey issue. `ERROR` (exit
`2`) is different: no verdict was reached, so look at the `Summary` line and
[Troubleshooting](getting-started.md#troubleshooting) first.

The example is a run whose scenario sets `maxmemory 4mb` with
`maxmemory-policy allkeys-random` on a data set of about 8 MB, so the server
evicts keys it already acknowledged:

```text
Scenario: evict-demo
Experiment: valkey.snapshot-equivalence
Status: FAIL
...
Summary: required components did not pass: snapshot, data
Report: /tmp/valkey-fuzzer/runs/20261118T174148.212Z-evict-demo-3e8e46/report.json
Reproduce: valkey-fuzzer replay /tmp/valkey-fuzzer/runs/20261118T174148.212Z-evict-demo-3e8e46/report.json
Look at: /tmp/valkey-fuzzer/runs/20261118T174148.212Z-evict-demo-3e8e46 (plan.json, components/), report.json keys failed_components and reproduce
  server logs: /tmp/valkey-fuzzer/logs/ (data directories are deleted after the run)
```

## 1. Find which check failed

`Summary` names the components that did not pass. Open the report folder
(the directory of the `Report:` path). `report.json` starts with `status`,
`summary`, `failed_components`, `reproduce`, and `look_at`. Each entry of
`failed_components` has the component's `use` (the oracle or action), a
one-sentence `summary` (cut to 120 characters, with its length and SHA-256),
and whether it `crash`ed:

```bash
python -c "
import json, sys
for c in json.load(open(sys.argv[1]))['failed_components']:
    print(c['component_id'], c['use'], c['status'], c['summary'])
" report.json
```

For the example this prints the `snapshot` component (`the snapshot is not a
consistent cut of the write history ...`) and the `data` component
(`4168 acknowledged write(s) lost (budget 0) ...`). The full text and the
`diagnostics` of every component, with the keys, values, and times behind the
sentence, are in `outcome.component_results`. A server that crashed has its
crash report under `crashes` in the diagnostics of the failed component
(`valkey.oracle.liveness` reports them), not in the `Summary` line.

A run of an experiment that names a [spec bundle](spec-bridge.md#bundles)
(`spec: cluster-election`, say) also checks its trace against the model
([`spec conform` in a run](spec-bridge.md#in-a-run)), and prints the result
after the summary:

```text
Summary: spec conform cluster-election: OneWinnerPerEpoch violated; experiment passed
Spec conform cluster-election: fail: OneWinnerPerEpoch violated
```

Such a run FAILs through the component `spec-conform:<bundle>`, listed in
`failed_components` with the violated claims as its `summary` (it has no
`use`: it is the model's check, not a plan component), and the report's
`conform` key holds the check itself: `violated`, `first_violated` (the
claim, the observed state that violates it, and the step that reached it,
with its event and fields), and every property's result. A bundle whose
claims are a draft reports the same under `conform` without failing the run:
`Spec conform cluster-election (draft claims): fail: ...`.

## 2. Run it again

The report records the scenario and the seed. These commands run the same plan
again, or shrink it while it keeps failing the same way:

```bash
valkey-fuzzer replay report.json
valkey-fuzzer minimize report.json --output minimized.yaml
```

Both need the same fuzzer and the same `valkey-server` that produced the
report. When they differ, `replay` still runs, prints each difference as a
warning, and records it under `provenance.drift` in the replay report. Replay does not reproduce timing, so a race can land
differently. Every run prints a `Reproduce:` line; a generated run
(`--seed N --experiment ID`) also prints the command that makes the same
scenario. The report keeps it as `reproduce`. A FAIL by `spec-conform:<bundle>`
replays and minimizes like any other: the replay checks its own trace and
prints its `Spec conform` line, and `minimize` keeps a reduction only while
the same claims fail.

## 3. Find the server logs

| What | Where |
| --- | --- |
| The verdict, every component's result, the event log | `report.json` in the report folder |
| The scenario as run and the execution plan | `scenario.yaml` and `plan.json` in the report folder |
| Files components wrote (captured RDBs, ledgers, traces) | `components/<component id>/` in the report folder |
| Each server's log and captured output | `/tmp/valkey-fuzzer/logs/<seed>-<deployment id>-valkey-<N>.log` and `.stdio.log` |
| Each server's data (AOF, RDB, `nodes.conf`) | deleted when the run ends |

The log files are named by seed, so find a run's logs with `ls
/tmp/valkey-fuzzer/logs | grep <seed>`. `valkey-fuzzer cleanup-logs <seed>
--force` removes them.

## 4. Decide whether it is a Valkey bug

Check the scenario before you blame Valkey:

- A scenario that changes server settings can make the server lose data on
  purpose. The example does: eviction is allowed to drop keys. The
  [bug zoo](bug-zoo.md) lists such cases.
- A binary copied away from its build tree can crash at start; the same
  binary works in its build directory.
- A `FAIL` that replays with the same summary under the same binary is not
  noise.

## 5. What to attach to a Valkey issue

- The `Summary` line and the failed component's `summary` from step 1.
- `scenario.yaml` from the report folder, so the maintainer can run it.
- `report.json`, or at least its `outcome.component_results` entries that
  failed and `reproducer.valkey_sha`. It can be large (tens of kilobytes).
- The log of the server that failed, from step 3, and its `.stdio.log` if the
  server crashed.
- The output of `valkey-fuzzer --version` and `valkey-server --version`.
- The `replay` result: whether the same failure came back.
