"""Artifact and resource providers for Valkey feature packs.

``valkey.local-binary`` resolves a valkey-server binary on this host,
fingerprints it, and optionally verifies its version or build SHA; its
``binary`` output is referenced from topologies and components with
``{$ref: artifacts.<id>.binary}``.

``valkey.fs.tmpdir`` and ``valkey.fs.tmpfs`` provision a private directory or a
size-limited tmpfs mount whose ``path`` output can hold node data
(``topology.node_data_roots``) for disk-full testing.
"""

import hashlib
import os
import platform
import re
import shutil
import subprocess
import tempfile
from collections.abc import Mapping
from typing import Any

from valkey_fuzzer.extensions import (
    EXTENSION_API_VERSION,
    ArtifactProviderRegistration,
    ProvisionedResource,
    ResolvedArtifact,
    ResourceProviderRegistration,
    SupportDecision,
)

LOCAL_BINARY_PROVIDER = "valkey.local-binary"
TMPDIR_PROVIDER = "valkey.fs.tmpdir"
TMPFS_PROVIDER = "valkey.fs.tmpfs"

_VERSION_RE = re.compile(r"\bv=(\S+)")
_SHA_RE = re.compile(r"sha=([0-9a-f]{7,40}):", re.IGNORECASE)


def _checked(spec: Any, config: Mapping[str, Any], allowed: Mapping[str, Any], required: tuple = ()) -> dict:
    unknown = sorted(set(config) - set(allowed))
    if unknown:
        raise ValueError(f"{spec.provider_id} config has unknown fields: {unknown}")
    missing = [name for name in required if name not in config]
    if missing:
        raise ValueError(f"{spec.provider_id} config is missing required fields: {missing}")
    merged = dict(allowed)
    merged.update(config)
    return merged


# ---------------------------------------------------------------------------
# valkey.local-binary
# ---------------------------------------------------------------------------

_BINARY_FIELDS = {"path": None, "expected_version": None, "expected_sha": None}


def resolve_binary(path: str) -> str:
    """Return an absolute executable path for a binary path or command name."""

    if os.path.sep in path or path.startswith("."):
        if not os.path.isfile(path):
            raise FileNotFoundError(f"valkey-server binary {path!r} does not exist")
        if not os.access(path, os.X_OK):
            raise PermissionError(f"valkey-server binary {path!r} is not executable")
        return os.path.abspath(path)
    resolved = shutil.which(path)
    if resolved is None:
        raise FileNotFoundError(f"valkey-server binary {path!r} is not on PATH")
    return resolved


def describe_binary(path: str) -> dict[str, Any]:
    """Immutable provenance for one binary: digest, version, and git SHA."""

    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    try:
        output = subprocess.run([path, "--version"], capture_output=True, text=True, timeout=10, check=True).stdout
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(f"cannot run {path} --version: {exc}") from exc
    version = _VERSION_RE.search(output)
    sha = _SHA_RE.search(output)
    return {
        "sha256": digest.hexdigest(),
        "version": version.group(1) if version else None,
        "git_sha": sha.group(1).lower() if sha else None,
        "version_output": output.strip(),
    }


def _validate_binary_spec(spec: Any) -> None:
    config = dict(spec.config)
    _checked(spec, config, _BINARY_FIELDS, required=("path",))


def _validate_binary_config(spec: Any, config: Mapping[str, Any]) -> None:
    merged = _checked(spec, config, _BINARY_FIELDS, required=("path",))
    for name in _BINARY_FIELDS:
        value = merged[name]
        if name == "path" or value is not None:
            if not isinstance(value, str) or not value or "\0" in value:
                raise ValueError(f"{spec.provider_id} config field {name!r} must be a non-empty string")


class LocalBinaryProvider:
    def check_support(self, context: Any, spec: Any, resolved_config: Mapping[str, Any]) -> SupportDecision:
        return SupportDecision.allow()

    def resolve(self, context: Any, spec: Any, resolved_config: Mapping[str, Any]) -> ResolvedArtifact:
        config = _checked(spec, resolved_config, _BINARY_FIELDS, required=("path",))
        path = resolve_binary(config["path"])
        record = describe_binary(path)
        expected_version = config["expected_version"]
        if expected_version and record["version"] != expected_version:
            raise ValueError(
                f"artifact {spec.artifact_id!r} ({path}) reports version {record['version']!r}, "
                f"expected {expected_version!r}"
            )
        expected_sha = (config["expected_sha"] or "").lower()
        if expected_sha:
            candidates = [value for value in (record["git_sha"], record["sha256"]) if value]
            if not any(value.startswith(expected_sha) or expected_sha.startswith(value) for value in candidates):
                raise ValueError(
                    f"artifact {spec.artifact_id!r} ({path}) does not match expected_sha {expected_sha!r}: "
                    f"git sha {record['git_sha']!r}, sha256 {record['sha256'][:16]}..."
                )
        return ResolvedArtifact(
            artifact_id=spec.artifact_id,
            provider_id=spec.provider_id,
            outputs={"binary": path},
            provenance={key: value for key, value in record.items() if key != "version_output"},
        )

    def rollback(self, context: Any, spec: Any) -> None:
        return None

    def cleanup(self, context: Any, spec: Any, artifact: ResolvedArtifact) -> None:
        return None


# ---------------------------------------------------------------------------
# valkey.fs.tmpdir / valkey.fs.tmpfs
# ---------------------------------------------------------------------------

_TMPDIR_FIELDS = {"prefix": "valkey-fuzzer-", "parent": None, "keep": False}
_TMPFS_FIELDS = {"size_mb": 64, "prefix": "valkey-fuzzer-tmpfs-"}


def _validate_tmpdir_config(spec: Any, config: Mapping[str, Any]) -> None:
    merged = _checked(spec, config, _TMPDIR_FIELDS)
    if not isinstance(merged["prefix"], str) or "/" in merged["prefix"]:
        raise ValueError("prefix must be a plain file-name prefix")
    if merged["parent"] is not None and not isinstance(merged["parent"], str):
        raise ValueError("parent must be a directory path")
    if not isinstance(merged["keep"], bool):
        raise ValueError("keep must be a boolean")


def _validate_tmpfs_config(spec: Any, config: Mapping[str, Any]) -> None:
    merged = _checked(spec, config, _TMPFS_FIELDS)
    size = merged["size_mb"]
    if isinstance(size, bool) or not isinstance(size, int) or not 1 <= size <= 65536:
        raise ValueError("size_mb must be an integer between 1 and 65536")
    if not isinstance(merged["prefix"], str) or "/" in merged["prefix"]:
        raise ValueError("prefix must be a plain file-name prefix")


def tmpfs_supported() -> tuple[bool, str]:
    if platform.system() != "Linux":
        return False, "size-limited tmpfs mounts require Linux"
    if os.geteuid() != 0:
        return False, "mounting tmpfs requires root"
    if shutil.which("mount") is None or shutil.which("umount") is None:
        return False, "mount/umount are not installed"
    return True, ""


class TempDirectoryProvider:
    """A private directory removed after the run."""

    def __init__(self) -> None:
        self._paths: dict[str, str] = {}

    def check_support(self, context: Any, spec: Any, resolved_config: Mapping[str, Any]) -> SupportDecision:
        return SupportDecision.allow()

    def provision(self, context: Any, spec: Any, resolved_config: Mapping[str, Any]) -> ProvisionedResource:
        config = _checked(spec, resolved_config, _TMPDIR_FIELDS)
        path = tempfile.mkdtemp(prefix=config["prefix"], dir=config["parent"])
        self._paths[spec.resource_id] = path
        return ProvisionedResource(
            resource_id=spec.resource_id,
            provider_id=spec.provider_id,
            outputs={"path": path},
            provenance={"kind": "directory", "keep": config["keep"]},
        )

    def rollback(self, context: Any, spec: Any) -> None:
        path = self._paths.pop(spec.resource_id, None)
        if path:
            shutil.rmtree(path, ignore_errors=True)

    def cleanup(self, context: Any, spec: Any, resource: ProvisionedResource) -> None:
        path = self._paths.pop(spec.resource_id, resource.outputs["path"])
        if not resource.provenance.get("keep"):
            shutil.rmtree(path)


class TmpfsProvider:
    """A size-limited tmpfs mount for disk-full testing (Linux, root)."""

    def __init__(self) -> None:
        self._paths: dict[str, str] = {}

    def check_support(self, context: Any, spec: Any, resolved_config: Mapping[str, Any]) -> SupportDecision:
        supported, reason = tmpfs_supported()
        return SupportDecision.allow() if supported else SupportDecision.deny(reason)

    def provision(self, context: Any, spec: Any, resolved_config: Mapping[str, Any]) -> ProvisionedResource:
        config = _checked(spec, resolved_config, _TMPFS_FIELDS)
        path = tempfile.mkdtemp(prefix=config["prefix"])
        self._paths[spec.resource_id] = path
        size = config["size_mb"] * 1024 * 1024
        subprocess.run(
            ["mount", "-t", "tmpfs", "-o", f"size={size},mode=0755", "tmpfs", path],
            check=True,
            capture_output=True,
        )
        return ProvisionedResource(
            resource_id=spec.resource_id,
            provider_id=spec.provider_id,
            outputs={"path": path},
            provenance={"kind": "tmpfs", "size_mb": config["size_mb"]},
        )

    @staticmethod
    def _release(path: str) -> None:
        if os.path.ismount(path):
            subprocess.run(["umount", path], check=True, capture_output=True)
        os.rmdir(path)

    def rollback(self, context: Any, spec: Any) -> None:
        path = self._paths.pop(spec.resource_id, None)
        if path and os.path.exists(path):
            self._release(path)

    def cleanup(self, context: Any, spec: Any, resource: ProvisionedResource) -> None:
        self._release(self._paths.pop(spec.resource_id, resource.outputs["path"]))


ARTIFACT_PROVIDERS = (
    ArtifactProviderRegistration(
        provider_id=LOCAL_BINARY_PROVIDER,
        factory=LocalBinaryProvider,
        outputs=("binary",),
        api_version=EXTENSION_API_VERSION,
        validator=_validate_binary_spec,
        resolved_config_validator=_validate_binary_config,
    ),
)

RESOURCE_PROVIDERS = (
    ResourceProviderRegistration(
        provider_id=TMPDIR_PROVIDER,
        factory=TempDirectoryProvider,
        outputs=("path",),
        api_version=EXTENSION_API_VERSION,
        resolved_config_validator=_validate_tmpdir_config,
    ),
    ResourceProviderRegistration(
        provider_id=TMPFS_PROVIDER,
        factory=TmpfsProvider,
        outputs=("path",),
        api_version=EXTENSION_API_VERSION,
        resolved_config_validator=_validate_tmpfs_config,
    ),
)
