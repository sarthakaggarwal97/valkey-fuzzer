# valkey-fuzzer-redis-pack

Redis OSS as a second target of [valkey-fuzzer](../../README.md), shipped as a
pack of its own: it registers through the `valkey_fuzzer.packs` entry point and
imports nothing of the Valkey components. How it is built, what it proves, and
how to add a third target are in [docs/second-target.md](../../docs/second-target.md).

What it ships, all under the `redis.*` namespace:

| Kind | ID | Does |
| --- | --- | --- |
| deployment provider | `redis.deployment.local` (mode `redis`) | one `redis-server` on this host, tagged for `valkey-fuzzer sweep` |
| workload | `redis.workload.set-get` | SET and GET a bounded key set on one connection, with a ledger of what the server said |
| fault | `redis.fault.process-signal` | SIGKILL or SIGTERM the server on an event, then restart it |
| action | `redis.action.sleep` | wait, so the fault triggers after the workload ran |
| oracle | `redis.oracle.liveness` | the server runs, answers, and is not loading |
| oracle | `redis.oracle.crash` | no `REDIS BUG REPORT` or sanitizer report in its log; judges a failed run post mortem too |
| oracle | `redis.oracle.ledger` | every key holds a value the ledger allows; no acknowledged read returned one it did not |
| experiment | `redis.kill-restart` | the kill and restart under writes with `appendfsync always`: every acknowledged write survives |

## Run it

```bash
pip install -e . -e packs/redis            # from the fuzzer's checkout; or pip install packs/redis
valkey-fuzzer list experiments | grep redis
valkey-fuzzer run examples/redis/kill_restart.yaml      # redis-server from PATH
valkey-fuzzer run examples/redis/kill_restart.yaml --valkey-binary /path/to/redis-server
```

`redis-commit` pins the Redis OSS commit CI builds (`.github/workflows/test.yml`,
job `redis-pack`): tag 7.2.17, the BSD-licensed line.

## Test it

```bash
python -m pytest packs/redis                             # on a fake redis-server process, no Redis needed
VALKEY_FUZZER_REDIS_BINARY=/path/to/redis-server python -m pytest -m slow packs/redis
```
