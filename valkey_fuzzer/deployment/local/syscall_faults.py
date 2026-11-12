"""The syscall fault library (``syscall_faults.c``): building it, and the environment a server loads it in.

A deployment with ``syscall_faults: true`` builds the library once into its own directory with the host's C
compiler (``$CC``, else ``cc``) and starts every server with it in ``LD_PRELOAD``, each with its own rule
directory beside its data directory. ``valkey.fault.syscall`` writes the rules there; without a rule the library
passes every call through.
"""

import hashlib
import os
import subprocess
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


def server_environment(library: str, rules: str) -> dict[str, str]:
    """The environment of a server that loads ``library`` and reads its rules from ``rules``.

    A gcc AddressSanitizer build links its runtime dynamically and refuses to start when another library comes
    first in the preload list; the runtime still intercepts what it must with the library first, so that check
    is turned off.
    """

    env = dict(os.environ)
    env["LD_PRELOAD"] = " ".join(part for part in (env.get("LD_PRELOAD", ""), library) if part)
    env[RULE_DIR_ENV] = rules
    env["ASAN_OPTIONS"] = ":".join(part for part in (env.get("ASAN_OPTIONS", ""), "verify_asan_link_order=0") if part)
    return env
