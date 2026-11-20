### Added: Redis OSS as a second target, a pack of its own (PR-T8; P-12)

- `packs/redis`: the `redis` pack, a Python package with its own `pyproject.toml` that registers through the
  `valkey_fuzzer.packs` entry point and imports the fuzzer's core alone. It ships a deployment provider for one
  `redis-server` (mode `redis`: `redis_binary`, `base_port`, `base_data_dir`, `server_config`, with
  `--valkey-binary`, `--base-port`, `replay` and the batch working on it), `redis.workload.set-get` (one writer,
  a ledger of what the server said), `redis.fault.process-signal` (kill on an event, restart), `redis.action.sleep`,
  the `redis.oracle.liveness`, `redis.oracle.crash` (post-mortem included) and `redis.oracle.ledger` oracles, and
  the experiment `redis.kill-restart`: SIGKILL under writes with `appendfsync always`, restart from the AOF, every
  acknowledged write survives. `examples/redis/kill_restart.yaml` runs it; the pack's tests run on a fake
  `redis-server` process. `docs/second-target.md` says why Redis, what the core assumed about Valkey, and how to
  add a third target; `test.yml` gains the `redis-pack` job, which builds Redis OSS at the commit
  `packs/redis/redis-commit` pins (cached), installs the pack, and runs its tests and the experiment once.

### Fixed

- A batch worker set the local deployment's placement fields (`base_port`, `base_data_dir`, `loopback_block`) on
  every job's deployment, so a scenario of another pack's provider, whose document has no `loopback_block`, was an
  ERROR at compile. The worker now sets the fields the scenario's provider names in its `host_fields`; the local
  provider names all three, so a Valkey batch is unchanged.
