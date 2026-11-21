### Added: a release workflow and a releasing guide (PR-T2)

- `.github/workflows/release.yml`: a pushed `v*` tag builds the sdist and the wheel (`python -m build`, `twine
  check --strict`), fails unless the tag names `valkey_fuzzer.__version__`, creates the GitHub release with both
  attached, and publishes them to PyPI through trusted publishing (`pypa/gh-action-pypi-publish` in the `pypi`
  environment with `id-token: write`; no API token anywhere). A `workflow_dispatch` run builds and stops. Every
  action is pinned to a commit. Nothing is published yet: the PyPI name `valkey-fuzzer` was free on 2026-11-20 and
  the pending publisher and the `pypi` environment are the owner's one-time setup, in `docs/releasing.md` with the
  release steps.

### Changed: one version source, PEP 639 license metadata, and complete archives (PR-T2)

- `pyproject.toml` declares the license as the SPDX string `license = "BSD-3-Clause"` with
  `license-files = ["LICENSE"]` (PEP 639) instead of the `{text = ...}` table setuptools deprecated and refuses
  from 2027-02-18; the build needs setuptools 77, which `build-system.requires` and the dev extra now pin, and
  `wheel` is no longer listed (setuptools builds wheels itself). The archives' metadata says
  `License-Expression: BSD-3-Clause`.
- The sdist carries the checkout (`MANIFEST.in`): `tests/` with its fixtures and the `.github/` workflows they
  read, `zoo/`, `examples/`, `docs/` and `CHANGELOG.md`, where it had the package, the test modules
  without their fixtures, and `formal/`. The wheel is unchanged: the package and the data it reads.
- `tests/test_packaging.py` builds both archives and checks what they ship (every tracked file of those directories
  in the sdist; every data file and spec bundle file in the wheel, and nothing beyond the package), that
  `__version__` is the version's one source and `valkey-fuzzer --version` prints it, and (slow) that the wheel
  installed in a clean virtual environment lists the same experiments as the checkout and finds a spec bundle
  inside the install. The wheel check in `test.yml`, which it supersedes, is gone. The dev extra adds `build`.
