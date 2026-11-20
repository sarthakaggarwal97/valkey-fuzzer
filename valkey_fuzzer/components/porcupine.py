"""The ``porcupine`` checker binary: found on PATH or in the cache, or fetched from a release of this repository.

``valkey.oracle.history`` runs ``porcupine``, the Go program in ``porcupine/``. A host needs no Go toolchain for
it: ``valkey-fuzzer-fetch-porcupine`` downloads the binary the porcupine workflow
(``.github/workflows/porcupine.yml``) built for this platform from the pinned release, checks its SHA-256 against
``porcupine_sha256sums.txt`` (the sums that workflow reproduces on every change), and installs it under
``~/.cache/valkey-fuzzer/bin``, where :func:`find_checker` looks after PATH. Building it from a checkout with
``go -C porcupine build -o <a directory on PATH>`` is the other way, and needs no network: the module is vendored.
"""

import argparse
import hashlib
import os
import platform
import shutil
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Optional
from urllib.request import urlopen

from valkey_fuzzer import cache
from valkey_fuzzer.outcomes import EXIT_HARNESS_ERROR, EXIT_SUCCESS

CHECKER = "porcupine"
# The release the binaries are fetched from, and the sums the workflow's build must reproduce: a change to
# porcupine/ or to the Go toolchain the workflow pins changes the sums, and this pin moves with them.
RELEASE_TAG = "porcupine-v1.3.1"
RELEASE_URL = f"https://github.com/sarthakaggarwal97/valkey-fuzzer/releases/download/{RELEASE_TAG}"
SUMS = Path(__file__).with_name("porcupine_sha256sums.txt")
FETCH_COMMAND = "valkey-fuzzer-fetch-porcupine"
BUILD_COMMAND = "go -C porcupine build -o <a directory on PATH>"
_GOOS = {"Linux": "linux", "Darwin": "darwin"}
_GOARCH = {"x86_64": "amd64", "amd64": "amd64", "aarch64": "arm64", "arm64": "arm64"}


def cache_dir() -> str:
    """Where the fetched checker goes: ``$XDG_CACHE_HOME/valkey-fuzzer/bin``, by default ``~/.cache/valkey-fuzzer/bin``."""

    return cache.cache_dir("bin")


def asset_name(system: Optional[str] = None, machine: Optional[str] = None) -> str:
    """The release asset for this host, ``porcupine-<goos>-<goarch>``; a ``ValueError`` names the build for others."""

    system, machine = system or platform.system(), machine or platform.machine()
    goos, goarch = _GOOS.get(system), _GOARCH.get(machine)
    if goos is None or goarch is None:
        raise ValueError(f"no release binary of {CHECKER} for {system}/{machine}: build it with `{BUILD_COMMAND}`")
    return f"{CHECKER}-{goos}-{goarch}"


def pinned_sha256(asset: str) -> str:
    """The SHA-256 ``porcupine_sha256sums.txt`` pins for ``asset`` (``sha256sum`` format: digest, two spaces, name)."""

    for line in SUMS.read_text(encoding="utf-8").splitlines():
        digest, _, name = line.partition("  ")
        if name == asset:
            return digest
    raise LookupError(f"{SUMS.name} pins no sum for {asset}")


def fetch_checker(directory: str) -> str:
    """Download this host's checker from the pinned release into ``directory`` and return its path.

    The download is read over HTTPS only and hashed as it is written to a temporary file beside the target; one
    whose SHA-256 is not the pinned one is deleted, and a ``ValueError`` says so. Nothing is installed until the
    sum matches.
    """

    asset = asset_name()
    expected = pinned_sha256(asset)
    url = f"{RELEASE_URL}/{asset}"
    if not url.startswith("https://"):
        raise ValueError(f"refusing to fetch {CHECKER} over {url.partition(':')[0]}: {url}")
    os.makedirs(directory, exist_ok=True)
    target = os.path.join(directory, CHECKER)
    digest = hashlib.sha256()
    handle, temporary = tempfile.mkstemp(prefix=f"{CHECKER}.", dir=directory)
    try:
        with os.fdopen(handle, "wb") as out:
            try:
                response = urlopen(url)
            except OSError as exc:  # URLError, and the HTTPError of a release or asset that does not exist
                raise OSError(
                    f"cannot download {url}: {exc}; build the checker with `{BUILD_COMMAND}` instead"
                ) from exc
            with response:
                if not response.geturl().startswith("https://"):
                    raise ValueError(f"refusing to fetch {CHECKER} from where {url} redirects: {response.geturl()}")
                while chunk := response.read(1 << 20):
                    digest.update(chunk)
                    out.write(chunk)
        if digest.hexdigest() != expected:
            raise ValueError(
                f"the SHA-256 of {url} is {digest.hexdigest()}, not the pinned {expected}: the download is corrupt "
                f"or tampered with, so nothing was installed; build the checker with `{BUILD_COMMAND}` instead"
            )
        os.chmod(temporary, 0o755)
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return target


def find_checker() -> str:
    """The checker on PATH, else the fetched one in the cache; a ``FileNotFoundError`` names both ways to get one."""

    checker = shutil.which(CHECKER) or shutil.which(CHECKER, path=cache_dir())
    if checker is None:
        try:
            fetch = f"fetch the {RELEASE_TAG} release binary ({asset_name()}, SHA-256 checked) with `{FETCH_COMMAND}`"
        except ValueError:
            fetch = f"no release binary exists for this platform ({platform.system()}/{platform.machine()})"
        have_go = shutil.which("go") is not None
        raise FileNotFoundError(
            f"{CHECKER} is not on PATH or in {cache_dir()}: {fetch}, or build it from a valkey-fuzzer checkout "
            f"with `{BUILD_COMMAND}`"
            + ("" if have_go else " (this needs Go >= 1.21, which is not on PATH either: https://go.dev/dl/)")
        )
    return checker


def main(argv: Optional[Sequence[str]] = None) -> int:
    """The ``valkey-fuzzer-fetch-porcupine`` console script (``pyproject.toml``): fetch into the cache, exit 0 or 2."""

    parser = argparse.ArgumentParser(
        prog=FETCH_COMMAND,
        description=f"Download the {CHECKER} linearizability checker of the {RELEASE_TAG} release for this platform, "
        f"check its SHA-256 against the pinned sum, and install it in {cache_dir()}, where valkey-fuzzer finds it.",
    )
    parser.parse_args(argv)
    try:
        path = fetch_checker(cache_dir())
    except (OSError, LookupError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return EXIT_HARNESS_ERROR
    print(f"Installed {path} ({asset_name()} of {RELEASE_TAG}, SHA-256 verified)")
    return EXIT_SUCCESS
