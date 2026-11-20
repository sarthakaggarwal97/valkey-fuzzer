"""The syscall fault library (``syscall_faults.c``): building it, and the environment a server loads it in.

A deployment with ``syscall_faults: true`` builds the library once into its own directory with the host's C
compiler (``$CC``, else ``cc``) and starts every server with it in ``LD_PRELOAD``, each with its own rule
directory beside its data directory. ``valkey.fault.syscall`` writes the rules there; without a rule the library
passes every call through.
"""

import hashlib
import os
import subprocess
from collections.abc import Mapping
from pathlib import Path

from .models import LocalDeploymentError

RULE_DIR_ENV = "VALKEY_FUZZ_SYSCALL_FAULTS"
SOURCE = Path(__file__).with_name("syscall_faults.c")
BUILD_TIMEOUT_S = 120.0


def rule_dir(data_dir: str) -> str:
    """The rule directory of the instance whose data directory is ``data_dir``."""

    return os.path.join(os.path.dirname(os.path.abspath(data_dir)), "syscall-faults")


def build_library(directory: str) -> str:
    """The library built from :data:`SOURCE` into ``directory``; built once per source, atomically."""

    source = SOURCE.read_bytes()
    path = os.path.join(directory, f"syscall-faults-{hashlib.sha256(source).hexdigest()[:16]}.so")
    if os.path.exists(path):
        return path
    os.makedirs(directory, exist_ok=True)
    partial = f"{path}.{os.getpid()}.tmp"
    command = [os.environ.get("CC") or "cc", "-O2", "-shared", "-fPIC", "-o", partial, str(SOURCE), "-ldl", "-lpthread"]
    try:
        built = subprocess.run(command, capture_output=True, text=True, timeout=BUILD_TIMEOUT_S)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise LocalDeploymentError(f"cannot build the syscall fault library: {type(exc).__name__}: {exc}") from exc
    if built.returncode != 0:
        raise LocalDeploymentError(f"cannot build the syscall fault library: {built.stderr.strip()[-2000:]}")
    os.replace(partial, path)
    return path


def preload(env: Mapping[str, str], library: str) -> dict[str, str]:
    """``env`` with ``library`` appended to its preload list. A gcc AddressSanitizer build links its runtime
    dynamically and refuses to start behind another preloaded library, though the runtime still intercepts what it
    must, so that check is turned off (once, however many libraries are preloaded)."""

    env = dict(env)
    env["LD_PRELOAD"] = " ".join(part for part in (env.get("LD_PRELOAD", ""), library) if part)
    options = env.get("ASAN_OPTIONS", "")
    if "verify_asan_link_order=0" not in options:
        env["ASAN_OPTIONS"] = ":".join(part for part in (options, "verify_asan_link_order=0") if part)
    return env


def server_environment(library: str, rules: str) -> dict[str, str]:
    """The environment of a server that loads ``library`` (:func:`preload`) and reads its rules from ``rules``."""

    return {**preload(os.environ, library), RULE_DIR_ENV: rules}
