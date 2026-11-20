### Added: a Docker image, and a reusable workflow for module repositories (PR-T3; P-05, P-06)

- `Dockerfile`: the fuzzer, Valkey at a pinned version (`--build-arg VALKEY_VERSION`, 9.1.3 by default, with
  `valkey-server` and its tools on `PATH` and the `config.c` beside them), the porcupine checker built in a Go
  stage, and what the faults call (`iptables`, `tc`, `libfaketime`, a C compiler, `mount`, `ps`), about 380 MB,
  running as a non-root user; the header says which faults need `--user root --cap-add NET_ADMIN`.
  `.github/workflows/build-image.yml` builds it and runs `aof_durability.yaml` in it on a pull request that touches
  it, and pushes `ghcr.io/<owner>/valkey-fuzzer:<tag>` and `:latest` on a `v*` tag only, with `packages: write`
  on that job alone.
- `.github/workflows/module-fuzz.yml` (`workflow_call`): a module repository runs the fuzzer against its module
  with about ten lines of YAML (`docs/module-repos.md`): it builds the module (or takes a prebuilt one from an
  artifact), loads it into every server of a batch of random seeds of an experiment, on the runner (Valkey at
  `valkey_version`, porcupine built there) or in the image (`image`), uploads the batch folder, and fails only
  on a `FAIL` (`fail_on_error` adds `ERROR` and `SKIP_UNSUPPORTED`). `ci.py module-base` writes its base file:
  the caller's, with `deployment.server_config.loadmodule` set to the module it built.
- `examples/valkey/experiments/module-chaos.yaml` (`example.module-chaos`): the shape `.github/scripts/
  module_chaos.py` writes for valkey-json, valkey-bloom and valkey-search, as one data-only experiment whose
  inputs are the module's commands (`module_command`, `load`, `during`, `verify`, `forbidden_logs`), so a module
  repository supplies data, not Python. `search-chaos.yml` and `module_chaos.py` are unchanged: the search
  experiment also has the `search-results` oracle and the ASan build of Valkey `unstable`, which the reusable
  workflow does not carry.
