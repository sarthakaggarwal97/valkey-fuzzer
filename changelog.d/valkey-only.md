### Removed: the pack and plug-in entry-point layer; the fuzzer targets Valkey only

- The `valkey_fuzzer.packs` entry-point group (PR #197) and `catalog.installed_packs` are gone: `build_catalog`
  registers the local deployment providers, then calls `valkey_fuzzer.components.register` for the Valkey
  components, generator hooks, environment providers, fragments, and experiments, as it did before #197. The
  `valkey` pack registering the local provider itself (PR #216) goes with it: `components.register` registers no
  deployment provider, and `build_catalog(deployment_providers=...)` again means "these instead of the local ones".
- The `valkey_fuzzer.batch_plugins` entry-point group, `hooks.register_installed`, and `batch.shipped_plugins`
  (PR #215) are gone: `valkey_fuzzer.batch` registers the bug zoo, then spec conformance, in code. The batch
  package of PR #198 and its plain-Python plug-in interface (`BatchSource`, `BatchPlugin`, `describe` included)
  stay; only the discovery through installed metadata goes.
- A scenario naming an unregistered component gets a "did you mean" over the registered IDs again, never "no
  installed pack registers `<ns>.*`"; `reproducer.fuzzer_digest` is the digest of the `valkey_fuzzer` package
  alone (a plan's components are all in it). The toy pack of `tests/test_service.py` and `tests/test_batch.py`,
  the "Ship a pack" section of `docs/extending.md`, the entry-point row of `docs/stability.md`, and the
  `[project.entry-points]` tables of `pyproject.toml` are removed. `valkey-fuzzer list` and a batch's folder are
  byte-identical to before.
