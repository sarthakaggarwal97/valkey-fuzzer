"""Running a scenario again on this host: free ports for a generated one, and the binary each instance runs.

A generated scenario's ports are not part of what it tests, so when they are
taken it moves to free ones. A replay must run the binaries the recorded run
ran, which the local deployment records per instance (``binary`` and
``binary_sha256`` in its metadata).
"""

import copy
import shutil
from collections.abc import Mapping
from typing import Any, Optional

from ..._data import file_sha256, to_plain_data
from ...contracts import ExecutionPlan
from . import config, ports
from .models import DeploymentMode
from .processes import resolve_binary_path

# How many other base ports a generated scenario may move to when its planned ports are taken.
_BASE_PORT_TRIES = 64


def with_free_base_port(scenario: dict[str, Any], plan: ExecutionPlan) -> tuple[dict[str, Any], Optional[str]]:
    """``scenario`` (compiled to ``plan``) with planned client and bus ports that are free now, and a note when it moved.

    Only ``deployment.base_port`` changes, so a seed's draws stay the same,
    and the scenario records the port it ran on. Ports in the host's
    ephemeral range count as taken: an outgoing connection can take one
    before Valkey binds it. A deployment whose ports cannot be planned
    before its environment exists (one naming an artifact or resource) is
    left as it is.
    """

    document = to_plain_data(plan.deployment)
    try:
        resolved = config.deployment_config_from_document(document)
        busy = ports.busy_planned_ports(document)
    except (TypeError, ValueError):
        return scenario, None
    original, count = resolved.base_port, len(plan.instances)
    highest = ports.highest_base_port(count, cluster=resolved.mode is DeploymentMode.CLUSTER)
    if busy:
        shown = ", ".join(str(item) for item in busy[:5]) + (", ..." if len(busy) > 5 else "")
        problem = f"{shown} in use"
    elif original > highest:
        problem = f"its ports reach this host's ephemeral port range (base_port at most {highest})"
    else:
        return scenario, None
    # Free blocks above the original first, then below it, never past ``highest``.
    bases = [*range(original + count, highest + 1, count), *range(min(original - count, highest), 1023, -count)]
    for base in bases[:_BASE_PORT_TRIES]:
        if not ports.busy_planned_ports({**document, "base_port": base}):
            moved = copy.deepcopy(scenario)
            moved["deployment"]["base_port"] = base
            return moved, (
                f"seed {scenario['seed']} plans base_port {original}, but {problem}; "
                f"using base_port {base}, which the scenario records"
            )
    return scenario, f"seed {scenario['seed']} plans base_port {original}, but {problem}"


def deployment_instances(outcome: Any) -> list[Mapping[str, Any]]:
    """The per-instance binary provenance the local deployment left in an outcome's (plain) diagnostics."""

    diagnostics = outcome.get("diagnostics") if isinstance(outcome, Mapping) else None
    deployment = diagnostics.get("deployment") if isinstance(diagnostics, Mapping) else None
    instances = deployment.get("instances") if isinstance(deployment, Mapping) else None
    if not isinstance(instances, (list, tuple)):
        return []
    return [
        instance
        for instance in instances
        if isinstance(instance, Mapping) and isinstance(instance.get("instance_id"), str)
    ]


def _instance_binary(plan: ExecutionPlan, instance: Mapping[str, Any]) -> tuple[Any, bool]:
    """The binary setting ``instance`` starts with (a path or an environment reference), and whether it is the
    deployment default."""

    overrides = plan.deployment.get("instance_binaries") or {}
    # As in the local deployment: an exact instance ID wins over a role.
    for selector in (instance["instance_id"], instance.get("role")):
        if isinstance(selector, str) and selector in overrides:
            return overrides[selector], False
    return plan.deployment.get("valkey_binary") or "", True


def _resolve_binary(setting: str) -> Optional[str]:
    if not setting:
        return shutil.which("valkey-server")
    try:
        return resolve_binary_path(setting)
    except OSError:
        return None


def binary_drift(
    plan: ExecutionPlan,
    recorded_instances: list[Mapping[str, Any]],
    *,
    override: Optional[str] = None,
    replayed_instances: Optional[list[Mapping[str, Any]]] = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Compare each instance's recorded binary digest with the replay's, and say what could not be.

    Before the run the binary an instance would start is resolved and hashed;
    given ``replayed_instances`` the replay's own deployment metadata is used. Instances
    on the deployment default are skipped under an explicit ``override``.
    """

    replayed = (
        None if replayed_instances is None else {instance["instance_id"]: instance for instance in replayed_instances}
    )
    digests: dict[str, Optional[str]] = {}
    mismatched: dict[tuple[str, str], list[str]] = {}
    unchecked: dict[str, list[str]] = {}
    for instance in recorded_instances:
        instance_id, recorded_digest = instance["instance_id"], instance.get("binary_sha256")
        setting, default = _instance_binary(plan, instance)
        if default and override:
            continue
        if not recorded_digest:
            unchecked.setdefault("the recorded run has no digest for it", []).append(instance_id)
            continue
        if replayed is not None:
            replay_instance = replayed.get(instance_id) or {}
            path, digest = replay_instance.get("binary"), replay_instance.get("binary_sha256")
        elif not isinstance(setting, str):
            unchecked.setdefault("it comes from an environment provider at run time", []).append(instance_id)
            continue
        else:
            path = _resolve_binary(setting)
            if path is not None and path not in digests:
                digests[path] = file_sha256(path)
            digest = None if path is None else digests[path]
        if not digest:
            # A reference names its path (artifacts.<id>.<output>).
            unchecked.setdefault(f"the replay's binary ({setting or 'valkey-server'}) cannot be read", []).append(
                instance_id
            )
        elif digest != recorded_digest:
            before = f"{instance.get('binary')} (sha256 {recorded_digest})"
            mismatched.setdefault((before, f"{path} (sha256 {digest})"), []).append(instance_id)
    drift = [
        {"item": f"Valkey binary of {', '.join(instances)}", "recorded": before, "replay": after}
        for (before, after), instances in mismatched.items()
    ]
    return drift, [f"Valkey binary of {', '.join(instances)}: {reason}" for reason, instances in unchecked.items()]
