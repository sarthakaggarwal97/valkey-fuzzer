"""Capability probes that run before any topology mutation.

``valkey.build_probe`` starts each distinct binary the run will use as a
private, TCP-less server on a unix socket with the requested startup
configuration, then checks versions, configuration values, INFO fields, and
commands.  An unknown directive or missing field is ``SKIP`` (unsupported);
a probe that cannot run at all is ``ERROR``.
"""

import os
import re
import shutil
import subprocess
import tempfile
import time
from collections.abc import Mapping
from typing import Any, Optional

import valkey

from valkey_fuzzer.extensions import ComponentStatus

from .common import (
    ComponentConfigError,
    ComponentSpec,
    Field,
    RunPlan,
    config_validator,
    make_evidence,
    make_result,
    resolved_config,
    server_arguments,
    to_plain_data,
)

_UNSUPPORTED_PATTERNS = (
    "Bad directive or wrong number of arguments",
    "FATAL CONFIG FILE ERROR",
    "Unknown option",
    "can only be selected when",
    "Invalid argument",
    "argument must be",
    "Unrecognized",
)

_PROBE_FIELDS = {
    "startup_config": Field(dict, default=dict),
    "config_get": Field(dict, default=dict),
    "info_fields": Field(dict, default=dict),
    "commands": Field(list, default=list),
    "min_version": Field(str, allow_none=True, default=None),
    "min_distinct_builds": Field(int, default=1, minimum=1, maximum=64),
    # Binaries the run starts later (e.g. a rolling-upgrade target), probed
    # alongside the topology's own; usually artifact references.
    "binaries": Field(list, default=list),
    "timeout_s": Field(float, default=15.0, minimum=1, maximum=120),
}


def _version_tuple(text: str) -> tuple[int, ...]:
    parts = re.findall(r"\d+", text)
    return tuple(int(part) for part in parts[:3])


def _validate_probe(config: Mapping[str, Any], spec: ComponentSpec, plan: RunPlan) -> None:
    for key, allowed in config["config_get"].items():
        if allowed is not None and not isinstance(allowed, list):
            raise ComponentConfigError(f"config_get.{key} must be null or a list of accepted values")
    for section, field_names in config["info_fields"].items():
        if not isinstance(field_names, list) or not all(isinstance(item, str) for item in field_names):
            raise ComponentConfigError(f"info_fields.{section} must be a list of field names")
    if not all(isinstance(item, str) for item in config["commands"]):
        raise ComponentConfigError("commands must be a list of command names")
    if config["min_version"] is not None and not _version_tuple(config["min_version"]):
        raise ComponentConfigError("min_version must look like '8.1.0'")


def _planned_roles(document: Mapping[str, Any], topology: str) -> dict[str, str]:
    """``{node_id: role}`` in the local topology provider's planning order."""

    defaults = {"standalone": (1, 0), "replication": (1, 1), "cluster": (3, 1)}
    shards, replicas = defaults.get(topology, (1, 0))
    shards = document.get("num_shards", shards) if topology == "cluster" else 1
    replicas = document.get("replicas_per_shard", replicas)
    if not isinstance(shards, int) or not isinstance(replicas, int):
        return {}
    roles = {}
    for index in range(shards * (1 + replicas)):
        roles[f"node-{index}"] = "primary" if index % (1 + replicas) == 0 else "replica"
    return roles


def plan_binaries(plan: RunPlan) -> dict[str, str]:
    """Return {label: binary} for every binary the plan's topology starts.

    ``node_binaries`` overrides (by node ID or role) add one entry each; the
    topology default (``valkey_binary``, else the orchestrator's default) is
    included when at least one planned node uses it. Artifact references are
    bound before capability probes run.
    """

    document = to_plain_data(plan.topology_document)
    overrides = document.get("node_binaries") or {}
    if not isinstance(overrides, Mapping):
        overrides = {}
    roles = _planned_roles(document, plan.topology)
    uses_default = not roles or any(node not in overrides and role not in overrides for node, role in roles.items())
    binaries: dict[str, str] = {}
    if uses_default:
        binaries["default"] = document.get("valkey_binary") or ""
    for selector, binary in sorted(overrides.items()):
        if isinstance(binary, str):
            binaries[f"node_binaries.{selector}"] = binary
    return binaries


# Where the local topology builds valkey-server when none is configured or on PATH.
_SOURCE_BUILD = os.path.join("/tmp/valkey-build", "valkey", "src", "valkey-server")


def resolve_binary(binary: str) -> Optional[str]:
    if binary:
        if os.path.sep in binary or binary.startswith("."):
            return os.path.abspath(binary) if os.path.isfile(binary) else None
        return shutil.which(binary)
    return shutil.which("valkey-server") or (_SOURCE_BUILD if os.path.isfile(_SOURCE_BUILD) else None)


class _ProbeServer:
    def __init__(self, binary: str, startup_config: Mapping[str, Any], timeout_s: float):
        self.binary = binary
        self.startup_config = startup_config
        self.timeout_s = timeout_s
        # Keep the socket path short: unix socket paths are limited to ~104
        # bytes on macOS.
        self.workdir = tempfile.mkdtemp(prefix="vfp-", dir="/tmp" if os.path.isdir("/tmp") else None)
        self.socket_path = os.path.join(self.workdir, "s.sock")
        self.log_path = os.path.join(self.workdir, "probe.log")
        self.process: Optional[subprocess.Popen] = None
        self.client: Optional[valkey.Valkey] = None

    def start(self) -> Optional[str]:
        """Start the server; return an unsupported-reason or None when ready."""

        command = [
            self.binary,
            "--port",
            "0",
            "--unixsocket",
            self.socket_path,
            "--unixsocketperm",
            "700",
            "--dir",
            self.workdir,
            "--logfile",
            self.log_path,
            "--save",
            "",
            "--appendonly",
            "no",
            "--daemonize",
            "no",
        ]
        command.extend(server_arguments(self.startup_config))
        self.process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + self.timeout_s
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                return self._exit_reason()
            if os.path.exists(self.socket_path):
                try:
                    client = valkey.Valkey(unix_socket_path=self.socket_path, socket_timeout=5, decode_responses=True)
                    client.ping()
                    self.client = client
                    return None
                except valkey.exceptions.ConnectionError:
                    pass
            time.sleep(0.05)
        raise TimeoutError(f"probe server did not become ready within {self.timeout_s}s")

    def _exit_reason(self) -> str:
        output = ""
        if self.process is not None and self.process.stdout is not None:
            output = self.process.stdout.read().decode("utf-8", errors="replace")
        try:
            with open(self.log_path, encoding="utf-8", errors="replace") as handle:
                output += handle.read()
        except OSError:
            pass
        return output.strip()[-2000:] or "server exited without output"

    def stop(self) -> None:
        if self.client is not None:
            try:
                self.client.close()
            except Exception:
                pass
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        if self.process is not None and self.process.stdout is not None:
            self.process.stdout.close()
        shutil.rmtree(self.workdir, ignore_errors=True)


def probe_binary(binary: str, config: Mapping[str, Any]) -> tuple[ComponentStatus, str, dict[str, Any]]:
    """Probe one binary; return (status, summary, facts)."""

    resolved = resolve_binary(binary)
    if resolved is None:
        hint = "" if binary else "; set topology.valkey_binary or put valkey-server on PATH"
        return ComponentStatus.ERROR, f"valkey-server binary {binary or 'valkey-server'!r} was not found{hint}", {}
    server = _ProbeServer(resolved, config["startup_config"], config["timeout_s"])
    facts: dict[str, Any] = {"binary": resolved}
    try:
        try:
            exit_reason = server.start()
        except (OSError, TimeoutError) as exc:
            return ComponentStatus.ERROR, f"probe server failed: {exc}", facts
        if exit_reason is not None:
            facts["exit_output"] = exit_reason
            if any(pattern.lower() in exit_reason.lower() for pattern in _UNSUPPORTED_PATTERNS):
                return ComponentStatus.SKIP, f"build rejects startup configuration: {exit_reason[-300:]}", facts
            return ComponentStatus.ERROR, f"probe server exited: {exit_reason[-300:]}", facts
        client = server.client
        info_server = client.info("server")
        version = str(info_server.get("valkey_version") or info_server.get("redis_version") or "")
        facts["version"] = version
        facts["git_sha1"] = str(info_server.get("valkey_git_sha1") or info_server.get("redis_git_sha1") or "")
        facts["server_name"] = str(info_server.get("server_name") or "")
        if config["min_version"] is not None and _version_tuple(version) < _version_tuple(config["min_version"]):
            return ComponentStatus.SKIP, f"version {version} is older than {config['min_version']}", facts

        config_values: dict[str, Any] = {}
        for name, accepted in config["config_get"].items():
            reply = client.config_get(name)
            if name not in reply:
                return ComponentStatus.SKIP, f"build does not support config {name!r}", facts
            config_values[name] = reply[name]
            if accepted is not None and str(reply[name]) not in {str(item) for item in accepted}:
                return (
                    ComponentStatus.SKIP,
                    f"config {name!r} is {reply[name]!r}, not one of {accepted}",
                    {**facts, "config": config_values},
                )
        facts["config"] = config_values

        info_values: dict[str, Any] = {}
        for section, field_names in config["info_fields"].items():
            section_info = client.info(section)
            for field_name in field_names:
                if field_name not in section_info:
                    return (
                        ComponentStatus.SKIP,
                        f"INFO {section} does not report {field_name!r}",
                        {**facts, "info": info_values},
                    )
                info_values[f"{section}.{field_name}"] = section_info[field_name]
        facts["info"] = info_values

        for command in config["commands"]:
            try:
                reply = client.execute_command("COMMAND", "INFO", command)
            except TypeError:
                # valkey-py cannot parse the nil entry an unknown command returns.
                reply = None
            if not isinstance(reply, Mapping) or command.lower() not in {name.lower() for name in reply}:
                return ComponentStatus.SKIP, f"build does not implement command {command!r}", facts
        return ComponentStatus.PASS, "capability probe passed", facts
    finally:
        server.stop()


class BuildProbe:
    """Probe every binary the plan will start against declared capabilities."""

    def probe(self, context: Any, spec: ComponentSpec):
        config = resolved_config(spec, _PROBE_FIELDS)
        results = {}
        worst = ComponentStatus.PASS
        summaries = []
        order = {ComponentStatus.PASS: 0, ComponentStatus.SKIP: 1, ComponentStatus.FAIL: 2, ComponentStatus.ERROR: 3}
        binaries = plan_binaries(context.plan)
        for index, binary in enumerate(config["binaries"]):
            binaries[f"binaries[{index}]"] = binary
        for label, binary in binaries.items():
            status, summary, facts = probe_binary(binary, config)
            results[label] = {"status": status.value, "summary": summary, **facts}
            if order[status] > order[worst]:
                worst = status
            if status is not ComponentStatus.PASS:
                summaries.append(f"{label}: {summary}")
        if worst is ComponentStatus.PASS:
            builds = {(facts.get("version"), facts.get("git_sha1")) for facts in results.values()}
            if len(builds) < config["min_distinct_builds"]:
                worst = ComponentStatus.SKIP
                summaries.append(
                    f"scenario needs {config['min_distinct_builds']} distinct builds, topology provides {len(builds)}"
                )
        evidence = ()
        if worst is ComponentStatus.PASS:
            evidence = (make_evidence(spec, "valkey.capability.supported", "capability.completed", results),)
        return make_result(
            spec,
            worst,
            "; ".join(summaries) or "every binary supports the requested capabilities",
            evidence=evidence,
            diagnostics={"binaries": results},
        )


REGISTRATIONS = (
    ("valkey.build_probe", "capability", BuildProbe, config_validator(_PROBE_FIELDS, extra=_validate_probe)),
)
