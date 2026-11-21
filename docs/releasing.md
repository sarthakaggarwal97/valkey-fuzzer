# Releasing

How a version of `valkey-fuzzer` gets from the `generic-fuzzer` branch to a
git tag, a GitHub release and PyPI. For maintainers; installing is in
[Getting started](getting-started.md).

## The version

`valkey_fuzzer/__init__.py` holds the one copy of the version,
`__version__`. `pyproject.toml` reads it (`dynamic = ["version"]`),
`valkey-fuzzer --version` prints it, and a release is the tag `v<version>`.
`tests/test_packaging.py` keeps it the only copy.

## Steps

1. Fold the changelog fragments. The pull requests since the last release each
   left a file under `changelog.d/`; `python scripts/fold_changelog.py
   <version>` moves them into `CHANGELOG.md` under a `## <version> - <date>`
   heading and deletes them (`changelog.d/README.md`). Read the result: it is
   the release notes.
2. Bump `__version__` in `valkey_fuzzer/__init__.py`.
3. Run the checks, as for any change:

   ```bash
   python -m pip install -e '.[dev]'
   python -m pytest
   ruff check .
   ruff format --check .
   ```

   The unit tests build the sdist and the wheel and check what they ship; the
   slow ones install the wheel in a clean virtual environment and run it.
4. Commit (signed off), open a pull request, merge it.
5. Tag the merge commit on `generic-fuzzer` and push the tag:

   ```bash
   git fetch origin
   git tag -a v0.3.0 -m "valkey-fuzzer 0.3.0" origin/generic-fuzzer
   git push origin v0.3.0
   ```

6. Watch the [Release](../.github/workflows/release.yml) workflow the tag
   starts (`gh run watch`). It has three jobs, each gated on the one before:
   - `build`: `python -m build` makes the sdist and the wheel, `twine check
     --strict` reads their metadata, and the job fails unless the tag names
     the version the package carries (`dist/valkey_fuzzer-<tag without
     v>.tar.gz` exists). A mismatch releases nothing: delete the tag, fix the
     version, tag again.
   - `github-release`: creates the GitHub release of the tag, titled with the
     tag, with the sdist and the wheel attached and a note pointing at
     `CHANGELOG.md` at that tag. It is the one job with a write token, and it
     checks out nothing and runs no shell: `actions/github-script` posts the
     downloaded files through the API (the rule `tests/test_ci_workflows.py`
     holds every workflow to).
   - `pypi`: publishes the same files to PyPI through
     [trusted publishing](https://docs.pypi.org/trusted-publishers/): the job
     runs in the `pypi` environment with `id-token: write` and no API token,
     with `pypa/gh-action-pypi-publish`. Only a pushed `v*` tag reaches this
     job and the one before: a `workflow_dispatch` run builds and stops, a dry
     run of the packaging on any ref (`gh workflow run release.yml --ref
     <branch>`).
7. Check the result from a clean virtual environment:

   ```bash
   python -m venv /tmp/vf && /tmp/vf/bin/python -m pip install valkey-fuzzer==0.3.0
   /tmp/vf/bin/valkey-fuzzer --version
   ```

PyPI never takes a version twice: a release that needs a fix gets the next
patch version, never a re-upload.

## What the archives hold

The wheel is what `pip install` puts on a host: the package, and the data it
reads at run time: the experiment and fragment YAML and the reply schemas under
`valkey_fuzzer/components/`, the syscall fault library's source under
`valkey_fuzzer/deployment/local/`, and the spec bundles of `formal/`, shipped
as `valkey_fuzzer/formal/` so `spec: cluster-election` resolves in any install.
`tool.setuptools.package-data` in `pyproject.toml` lists them.

The sdist is the checkout: besides the package, `tests/` and the `.github/`
workflows they read, `zoo/`, `examples/`, `docs/`, `CHANGELOG.md`.
`MANIFEST.in` lists them. `pytest` and `zoo/build.sh` work from an unpacked
sdist as from a clone.

## One-time setup

Done by hand by an owner of the GitHub repository and the PyPI account that
will own the project. Nothing in the repository holds a credential.

1. **The PyPI name.** On 2026-11-20 `valkey-fuzzer` was not registered on
   PyPI: `pip index versions valkey-fuzzer` said "No matching distribution
   found for valkey-fuzzer", and `https://pypi.org/simple/valkey-fuzzer/` was
   a 404. Nothing has been registered; the first publish from the workflow
   creates the project, through a *pending publisher*.
2. **The pending publisher.** At <https://pypi.org/manage/account/publishing/>,
   add a pending publisher for GitHub with: PyPI project name `valkey-fuzzer`;
   owner and repository name of the repository the tag is pushed to (for the
   upstream, `valkey-io` and `valkey-fuzzer`); workflow name `release.yml`;
   environment name `pypi`. PyPI matches the OIDC token the workflow presents
   against all four, so a fork cannot publish as the project and a tag pushed
   to another repository releases nothing.
3. **The GitHub environment.** In the repository's *Settings → Environments*,
   create `pypi`. Restrict its deployment branches and tags to tags matching
   `v*`, and add required reviewers: the `pypi` job then waits for one of them
   to approve before anything is uploaded, so a tag pushed by mistake can be
   stopped.
4. **Tag protection.** Under *Settings → Rules*, protect tags matching `v*` so
   that only maintainers create them.

After a release, PyPI shows the publisher under the project's *Publishing*
settings; nothing else needs doing for the next one.
