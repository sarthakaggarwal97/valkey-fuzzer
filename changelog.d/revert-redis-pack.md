### Removed: the Redis pack; the fuzzer targets Valkey only

- PR #233 (`packs/redis/`, `examples/redis/`, `docs/second-target.md`, the `redis-pack` CI job, the batch worker's
  per-pack placement, and the second-target rows of `docs/README.md` and `docs/extending.md`) is reverted in full.
  Redis code may not be viewed or built, so the fuzzer ships no pack for it; Valkey is the only target. The packs
  entry point (`valkey_fuzzer.packs`) and the batch plug-in group stay as they were before #233, and the toy pack of
  `tests/test_service.py` remains the proof that an out-of-tree pack loads through them.
