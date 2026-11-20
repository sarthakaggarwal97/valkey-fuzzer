# Second target: Redis OSS, as a pack

The fuzzer calls itself generic. A second target is what proves it: a system
under test that is not Valkey, run by the same core (compiler, runner, evidence
contract, reports, batch, sweep) through the same entry point every pack uses,
with no edit to the core that names it. This page is that target, the `redis`
pack under [`packs/redis`](../packs/redis/README.md), what running it taught
about the core, and how to add a third.

- [Why Redis OSS](#why-redis-oss)
- [What the pack is](#what-the-pack-is)
- [Run it](#run-it)
- [What the core assumed about Valkey](#what-the-core-assumed-about-valkey)
- [Add a third target](#add-a-third-target)

## Why Redis OSS

Redis OSS 7.2 speaks RESP, so the `valkey` client talks to it unchanged, and it
builds from source with `make` in about two minutes on a GitHub runner and on a
developer host, like Valkey does. Its behaviour is close enough that the same
kind of experiment (kill under writes, restart, check the data) means the same
thing, and different enough to be a real second target: its crash report starts
`=== REDIS BUG REPORT START`, its `INFO` says `redis_version`, there are no
`valkey-*` tools beside it, and nothing in the fuzzer's Valkey pack applies to
it. The 7.2 line is the last under the BSD-3 licence; CI builds the commit
[`packs/redis/redis-commit`](../packs/redis/redis-commit) pins (tag 7.2.17).

## What the pack is

A Python package of its own, `redis_pack`, with its own `pyproject.toml`,
installed next to the fuzzer; `pip install packs/redis` records it as the
`redis` entry point of the `valkey_fuzzer.packs` group, and every command then
registers it after the Valkey pack (packs register in name order). It imports
the fuzzer's core alone, never `valkey_fuzzer.components` or
`valkey_fuzzer.deployment`.

```
packs/redis/
  pyproject.toml                   name, the entry point, the package data
  redis-commit                     the Redis OSS commit CI builds
  README.md
  redis_pack/__init__.py           register(catalog): components, the provider, the experiments
  redis_pack/deployment.py         the deployment provider and the service it publishes
  redis_pack/workload.py           redis.workload.set-get
  redis_pack/fault.py              redis.fault.process-signal
  redis_pack/actions.py            redis.action.sleep
  redis_pack/oracles.py            redis.oracle.liveness, .crash, .ledger
  redis_pack/experiments/redis.kill-restart.yaml
  tests/fake_redis_server.py       a redis-server the tests can kill
  tests/test_redis_pack.py
```

| Part | Does |
| --- | --- |
| `redis.deployment.local`, mode `redis` | Starts one `redis-server` from `redis_binary` (a path, or `redis-server` on `PATH`) on `base_port`, with its data under `base_data_dir/deployment-<id>/` and its log under `base_data_dir/logs/`, the options of `server_config` and of the plan's startup overlays as `--name value` arguments, and the sweep's tag in its process title and its deployment directory. It waits until the server on the port reports its own PID and is not loading. The service it publishes under `context.services["deployment"]["redis.deployment"]` gives the components a raw connection, a client, the PID, liveness, a signal, a restart, and the log files. It implements `normalizer`, `plan_instances`, `binary_override`, `recorded_instances`, `binary_drift`, and `host_fields`, so `--valkey-binary`, `--base-port`, `replay` (binary drift included) and the batch work on it. |
| `redis.workload.set-get` | One writer, one connection: SET and GET on `keys` keys at `rate` operations a second. Every operation is a ledger entry whose outcome is `ok`, `error` (the connect failed or the server refused it: not applied) or `ambiguous` (sent, no answer: perhaps applied). Values are `v<id>`, unique per operation. `stop` writes `ledger.jsonl` and emits `redis.workload.ledger`. |
| `redis.fault.process-signal` | On `when_event`, SIGKILL (or SIGTERM) the server inside the event's dispatch, wait for the exit, publish `<id>.injected`, restart it, publish `<id>.recovered`. The kill is a `process.stopped` obligation until the restart succeeds. Never firing is an ERROR. |
| `redis.action.sleep` | A wait, so the fault triggers on `<id>.completed` after the workload ran. |
| `redis.oracle.liveness` | The process runs, answers PING, and is not loading, within `timeout_s`. |
| `redis.oracle.crash` | No crash report or sanitizer report in the server's log or process output; a FAIL names the cause (`crashed by signal: 11`) as `crashes` in its diagnostics, so one crash is one failure signature. Its `post_mortem` judges a run that failed before the oracles ran from the files the provider names. |
| `redis.oracle.ledger` | Replays the workload's ledger in the writer's order into the values each key may hold (an acknowledged write replaces them, an ambiguous one adds, a read must return one and narrows them), then MGETs every key and checks. |
| `redis.kill-restart` | Writes with `appendonly yes` and `appendfsync always` (startup overlay), SIGKILL after `run_before_kill_s`, immediate restart, `run_after_restart_s` more writes, then the three oracles. `appendfsync always` fsyncs before the reply, so the policy is strict: every acknowledged write survives. Required: the kill fired and the restart happened (`kill.injected`, `kill.recovered`, `redis.fault.injected`), and the ledger was checked (`redis.ledger.verified`). |

The pack has a mode of its own, `redis`, rather than a second provider of
`standalone`: deployment modes are one namespace across packs, and a scenario
of a mode with two providers must name one with `deployment.provider`, every
Valkey scenario included (see [Ship a pack](extending.md#ship-a-pack)).

A negative control shows the oracle sees what it claims: the same experiment
with `appendonly "no"` (a copy under `--experiment-dir`) ends `FAIL`, the
ledger oracle naming the keys that came back empty.

The tests ([`packs/redis/tests`](../packs/redis/tests/test_redis_pack.py))
need no Redis: the components run on an in-memory deployment, and the provider
and the whole experiment run on
[`fake_redis_server.py`](../packs/redis/tests/fake_redis_server.py), a process
that takes `redis-server`'s options, speaks enough RESP, keeps its data across a
kill when `--appendonly yes`, and can exit with a crash report on request
(`--fake-exit-after N`), which the crash oracle's post-mortem turns into a FAIL.
One `slow` test runs the experiment on a real server
(`VALKEY_FUZZER_REDIS_BINARY`, or `redis-server` on `PATH`).

## Run it

```bash
pip install -e . -e packs/redis
valkey-fuzzer list experiments | grep redis            # redis.kill-restart [redis]
valkey-fuzzer run examples/redis/kill_restart.yaml      # redis-server on PATH
valkey-fuzzer run examples/redis/kill_restart.yaml --valkey-binary /path/to/redis-server
python -m pytest packs/redis
```

In CI the `redis-pack` job of `test.yml` builds Redis at the pinned commit
(cached on the commit, as the Valkey build is), installs the pack, runs its
tests, and runs the example once; a PASS exits 0. The unit-test jobs run
without the pack: the component reference and the catalog checks hold the
Valkey components alone, so a second pack must never be installed in the
environment that runs `tests/`.

## What the core assumed about Valkey

Running the pack found one assumption hard-coded in the core, and several the
core still carries by name. The fix:

- **A batch worker set the local deployment's fields on every job.** The
  worker merged `base_port`, `base_data_dir` and `loopback_block` into each
  scenario's deployment; the `redis` provider's document has no
  `loopback_block`, so every job of a batch was an ERROR at compile. The
  worker now sets the ones the scenario's provider names in its `host_fields`
  (`valkey_fuzzer/batch/worker.py`); the local provider names all three, so
  a Valkey batch is unchanged. A draw whose base names no mode still gets all
  three: the generator draws a local deployment.

Left, each by name, none of which stops a second target:

| Assumption | Where | What it means for a pack |
| --- | --- | --- |
| Deployment modes are one namespace | `compiler.scenario_provider` | A pack registers a mode of its own (`redis`), or every scenario of a shared mode names its provider. |
| `valkey_sha` on the outcome, in `report.json`, and `Valkey SHA:` in the printed outcome | `results.RunOutcome`, `reports`, `outcomes` | The `redis` pack leaves it `None` and records `redis_version` in the deployment metadata instead. |
| `--valkey-binary`, and the error `cannot run another Valkey binary` for a provider without `binary_override` | `cli`, `compiler.binary_fields` | Works through the hook: `--valkey-binary /path/redis-server` sets `redis_binary`. The name is the first target's. |
| `reproducer.valkey_binary` falls back to the scenario's `deployment.valkey_binary` | `provenance.recorded_binary` | Unused when the provider implements `recorded_instances`, as the `redis` pack does. |
| The core imports the local deployment's constants: the sweep's root `DEFAULT_BASE_DATA_DIR`, the batch's `LOCK_DIRECTORY`, `CLUSTER_BUS_PORT_OFFSET`, `ephemeral_overlap` | `hoststate`, `commands/run.py`, `batch/pool.py`, `batch/spec.py` | The pack puts its data under the same root so the sweep finds it, and a batch reserves a cluster-bus range it does not use. The two-distribution split (P-12) moves these. |
| No component of any role ships with the core | `valkey_fuzzer/components` is the Valkey pack | A pack writes its own sleep, workload, and oracles; `redis.action.sleep` is ten lines. |
| Random scenarios (`run --random`, `batch` `random` sources) draw a local deployment | `generator` | A pack's experiments run from scenarios and `scenarios` sources; opting into the generator is open. |
| `valkey-fuzzer --version` says `Valkey Fuzzer`, and `--help` says Valkey | `cli` | Cosmetic. |
| The component reference is generated from the installed catalog | `scripts/gen_component_docs.py`, `tests/test_docs_declarations.py` | `tests/` must run without other packs installed. |

## Add a third target

Copy `packs/redis` to `packs/<name>` and change what names the target. In order:

1. **Namespace and entry point.** Rename the package, and in `pyproject.toml`
   the distribution, the entry point (`<name> = "<package>:register"`), and
   the package data. Every ID the pack registers starts with `<name>.`:
   `require_qualified_id` and the pack rule hold it to that, and a scenario
   that uses an ID of an uninstalled pack names the pack to install.
2. **The deployment provider** (`deployment.py`). Keep the shape: a
   `ConfigSchema` for the document (`mode` first, a field for the binary,
   `base_port`, `base_data_dir`, `server_config`), a `normalizer` that
   resolves it, a `plan_instances` that returns each instance with its
   effective server config (the defaults under the document under the
   overlays), `provision` that starts the process with the sweep's tag
   (`hoststate.server_tag_arguments()` if the server has a process-title
   template, else at least `tag_directory` on the deployment directory),
   waits until the server on the port is the process it started, and returns
   `ProvisionedDeployment(services={"<name>.deployment": service},
   metadata=...)` with one row per instance (`instance_id`, `port`, `pid`,
   `binary`, `binary_sha256`), `log_files` for the post-mortem, and
   `rollback` and `cleanup` that stop the process and remove the data but not
   the logs. Name in `host_fields` the fields a batch worker may set:
   `base_port` and `base_data_dir` at least. Pick a mode of your own.
3. **The service.** What your components need, and only that. The `redis`
   one is a raw connection, a client, `pid`, `is_alive`, `send_signal`,
   `wait_exit`, `restart`, and the three file paths. A fake of it in the tests
   is a page.
4. **Workload and ledger.** The `operation` function is where the target's
   protocol lives: how a connect fails before anything is sent (an `error`),
   what a reply that refuses the command looks like (`LOADING`, `MISCONF`:
   an `error`), and everything else after the send (`ambiguous`). Keep the
   three outcomes and the ledger entry's fields and the ledger oracle needs no
   change.
5. **Oracles.** The crash patterns and the cause patterns are the target's
   (`CRASH_PATTERNS`, `CRASH_CAUSES`); the liveness check is whatever "up and
   serving" means for it (Redis: PING and `loading:0`).
6. **The experiment.** One YAML under `experiments/`; the startup overlay is
   where the durability setting the policy depends on goes. Mark the fault's
   firing and the oracle's check as required events and evidence, so a run
   that did not kill or did not check is an ERROR, not a PASS.
7. **Tests on a fake process.** Teach `fake_redis_server.py` your target's
   options and replies, or write its twin; test the provider on it, and the
   whole experiment through `run_experiment(..., deployment_providers=(...))`
   with the real provider and the fake binary. Add the negative controls: a
   restart that loses the data fails the ledger oracle; an exit with a crash
   report fails the crash oracle through the post-mortem.
8. **CI and docs.** A `<name>-pack` job in `test.yml` modelled on
   `redis-pack`: build the target at a pinned commit under a cache keyed on
   the pin file, install the pack, run its tests, run the example once.
   An example scenario under `examples/<name>/`, a row in
   [the docs index](README.md), and a changelog fragment.

What you should not need: an edit to `valkey_fuzzer/`. If you do, it is a
core assumption; fix it in the core where it is hard-coded, and add it to the
table above where it is a name.
