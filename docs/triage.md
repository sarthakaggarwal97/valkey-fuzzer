# Triage

From a soak's `FAIL` or `ERROR` to a filed Valkey bug, a fixed fuzzer bug, or
a dismissed host problem. For one hand-run scenario, [Reading a
FAIL](reading-a-fail.md) shows where the verdict, logs, and replay command are;
this page starts from a batch folder ([Operations](operations.md)) and ends
at the issue tracker and the [bug zoo](bug-zoo.md).

- [Start from the signatures](#start-from-the-signatures)
- [Was it load?](#was-it-load)
- [Replay, then minimize](#replay-then-minimize)
- [The failure signature](#the-failure-signature)
- [Fuzzer bug or Valkey bug](#fuzzer-bug-or-valkey-bug)
- [What to attach](#what-to-attach)
- [Add it to the zoo](#add-it-to-the-zoo)

## Start from the signatures

`summary.json` groups every run that did not pass by its
[failure signature](#the-failure-signature), most runs first, with the
serial re-runs' verdict:

```bash
jq -r '.signatures[] | "\(.count)\t\(.status)\t\(.experiment)\t\(.serial_reruns.verdict // "-")\t\(.label)"' soak/20261120T0829/summary.json
```

```text
1	ERROR	valkey.clock-skew-expiry	reproduced	ERROR | valkey.clock-skew-expiry | valkey_fuzzer.runner.RunnerError during prepare
1	SKIP_UNSUPPORTED	valkey.bgsave-disk-full	-	SKIP_UNSUPPORTED | valkey.bgsave-disk-full
```

Take them in this order: `FAIL` with a crash, other `FAIL`, `ERROR`, and
`SKIP_UNSUPPORTED` last (the host lacks something; see
[What the host needs](operations.md#what-the-host-needs)). Each signature's
`runs` names its run folders, `runs/<job id>/`, and the console printout
gives one example with its seed and attempt.

## Was it load?

A batch runs its workers in parallel, so a timeout, a replica that never
caught up, or a missed deadline can be the host, not Valkey.
`--serial-reruns N` in the soak already ran each signature alone `N` times;
its `verdict` is `reproduced` (every re-run failed the same way),
`intermittent`, `failed differently` (none the same way, some another way:
read both verdicts, see below), or `not reproduced` (all passed: load, or a
race worth more seeds). To re-run the failures of a finished batch, each
with the scenario its seed drew, alone after the parallel pass:

```bash
valkey-fuzzer batch --failures soak/20261120T0829 --serial-reruns 1 --out soak/20261120T0829-rerun
```

A run that passes alone and fails under load is still a finding when the
failing component is an oracle that judged data (`valkey.oracle.ledger`,
`valkey.oracle.history`): load changes timing, not correctness. One that
fails by a deadline or a `cannot read` is the host's; raise `--timeout` or
lower `--workers` ([Budgets and timeouts](operations.md#budgets-and-timeouts)).

## Replay, then minimize

Replay runs the recorded plan again with what is installed now, and warns
about any drift from the recorded fuzzer, experiment, or binary. Minimize
shrinks the scenario while the failure signature holds:

```bash
valkey-fuzzer replay runs/<job id>/report.json
valkey-fuzzer minimize runs/<job id>/report.json --output minimized.yaml
```

A batch's runs of an `--experiment-dir` experiment need
`--experiment-dir experiments` (the batch folder's copy). Minimize first runs
the scenario as recorded and prints the signature it will hold, then one
reduction per line:

```text
Minimizing crash-demo: signature {"status": "FAIL", "crashes": ["crashed by signal: N"], ...}
  original: reproduced (FAIL)
  deployment.replicas_per_primary=0: kept (FAIL)
  drop deployment.server_config.appendfsync: kept (FAIL)
  drop deployment.server_config.enable-debug-command: rejected (FAIL)
  experiment.inputs.operations=10: kept (FAIL)
  confirm: reproduced (FAIL)
Minimized scenario written to minimized.yaml
```

`kept` means the failure still matched without that part, so the part was
not needed; `rejected` means it was. The `minimized.yaml` that comes out is
the smallest scenario found and is what to attach. Exit `2` with
`the failure did not reproduce, so nothing was minimized` means the first
run failed with another signature (or passed): the verdict text names a
random key, say, so each run's signature differs. Then compare the two
verdicts by eye, and attach the original scenario.

## The failure signature

The signature is what `batch` groups by, `--failures` counts
`per_signature`, and `minimize` holds. It is the run's outcome, the failing
components with each one's status and verdict (its summary with numbers
and hex IDs masked as `N`; for a raised exception, the error type and
phase), the missing required events and evidence, and the required evidence
observed. A run whose failing components report a crash of the server
(`crash: true` in `failed_components`) has only its outcome and the crash
causes as its signature, masked the same way (`crash report (crashed by
signal: N)`, `server.c:N`): the same crash in another experiment, or with
other components failing after it, is the same failure, and `minimize` may
drop everything that is not needed to reach it ([minimize](cli.md#minimize)).

In `summary.json` every run has a `signature_key` (a digest) and the
signatures a `key`, `label`, and the full `signature`. Two signatures can be
one bug: a verdict that names a key or an instance ID changes between runs
(`vf:writes:m:N` and `vf:writes:stable:N` above). Two runs with one
signature are one bug until a fix shows otherwise.

## Fuzzer bug or Valkey bug

| You see | It is | Do |
| --- | --- | --- |
| `FAIL` with `crash: true`, or a crash, assertion, or sanitizer report in the server's log or `.stdio.log` | a Valkey bug, unless the binary runs outside its build tree (it cannot find its Lua module and panics at start) | file it |
| `FAIL` from `valkey.oracle.ledger`, `history`, `rdb-snapshot`, `deployment-state`, or `cluster-election-safety` that replays | a Valkey bug, unless `scenario.yaml` allows the behaviour: `maxmemory` with an evicting policy, `appendfsync no`, a `loss_window_s` above 0 | read the scenario's `server_config` and `inputs` first, then file it |
| `FAIL` from `valkey.oracle.evidence` or a forbidden log pattern | Valkey did what the experiment forbids; check the pattern or rule is not broader than the claim | file it, or narrow the rule |
| `FAIL` whose verdict is the fuzzer's own expectation being wrong: `0/1 replies matched` on a command the build does not have, a check that misreads a reply, a predicate on the wrong field | a fuzzer false positive | fix it at its source with a test that pins the case, and never file it |
| `ERROR` | no verdict: `outcome.diagnostics` names the cause (`error`, `missing_required_evidence`, `cleanup_errors`, `abandoned_calls`, `contract_violations`, a leak) | a port in use, a timeout, or a missing need is the host's; anything else is a fuzzer bug to fix as above |
| `SKIP_UNSUPPORTED` | the host or build lacks a named feature | nothing, or add the feature |

A `FAIL` that depends on a component that did not do its part is already an
`ERROR` ([Outcomes and verdicts](contracts.md#outcomes-and-verdicts)), so a
`FAIL` is always a judgement about Valkey or about the experiment, never a
harness failure.

## What to attach

The list for one run is in [Reading a FAIL](reading-a-fail.md#5-what-to-attach-to-a-valkey-issue).
From a batch, take these from the run folder `runs/<job id>/`:

| Attach | From |
| --- | --- |
| the verdict, the failed components' summaries, the seed and how it was drawn, the Valkey commit | `report.json`: `status`, `summary`, `failed_components`, `reproducer.generated` (`seed`, `attempt`, `cores`, `profile`), `reproducer.valkey_sha` |
| the scenario, as run | `scenario.yaml`, or `minimized.yaml` after a minimize |
| the server logs | `logs/` in the run folder (a pruned passing run has none; a hand run's are under `/tmp/valkey-fuzzer/logs/`) |
| the trace, for a cluster or replication run | `components/<observer id>/trace.jsonl` (`valkey.observer.trace`): events and per-node state samples; `conform` in `report.json` when a spec bundle judged it |
| the write history, for a data oracle | `components/writes/ledger.jsonl` and `preload.jsonl`, `components/history/history.jsonl` |
| whether it came back | the signature's `serial_reruns.verdict`, or the `replay` result |
| the versions | `valkey-fuzzer --version`, `valkey-server --version` |

One command gives the first row:

```bash
jq '{status, summary, failed_components, reproducer}' runs/<job id>/report.json
```

A seed is stable only per profile and cores, so quote all three; the
`scenario.yaml` replays anywhere.

## Add it to the zoo

A confirmed and fixed Valkey bug becomes a regression case: the fix's revert
as a patch against the zoo's pinned commit, the minimized scenario, and the
component that must catch it. The format and the judging rules are in
[Adding a case](bug-zoo.md#adding-a-case); the check is:

```bash
zoo/build.sh BUILDS <case id>
valkey-fuzzer batch --zoo BUILDS --zoo-case <case id> --workers 2
```

The clean build must pass the scenario and the broken one must fail it in
the named component at least `min_detection_rate` of `runs` times (five runs
at least). A bug the pinned commit still has, because its fix is newer, gets
`skipped_until: "valkey-io/valkey#<fix>"` instead of a patch until the pin
moves. A fuzzer false positive never goes in the zoo: the zoo proves the
oracles catch Valkey bugs, and a false positive's fix is a unit test.
