"""A run's coverage tuple: the semantic combination its plan exercises.

Every run report carries the tuple and its key (``coverage``), and the batch
summary counts the distinct keys each experiment covered. A tuple is built only
from the plan's generic structure: experiment, deployment provider and shape,
config mix, inputs, the workload/action/fault/observer/oracle composition, and
the artifact, resource, and policy declarations with their config. Nothing here
interprets experiment or component IDs.
"""

import hashlib
import json
from typing import Any, Optional

from ._data import to_plain_data
from .contracts import ComponentRole, ExecutionPlan, RunOutcome


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:16]


def static_tuple(plan: ExecutionPlan) -> dict[str, Any]:
    """The semantic tuple a plan targets, independent of its result."""

    document = to_plain_data(plan.deployment)
    components = to_plain_data(plan.components)

    def of_role(role: ComponentRole) -> list:
        return sorted({component["use"] for component in components if component["role"] == role.value})

    config_mix = {
        "server_config": document.get("server_config", {}),
        "instance_configs": document.get("instance_configs", {}),
        "overlays": [to_plain_data(overlay) for overlay in plan.startup_overlays],
    }
    # Different builds are different combinations, whether a scenario names a
    # binary directly or through an artifact declaration.
    artifact_mix = {
        "valkey_binary": document.get("valkey_binary"),
        "instance_binaries": document.get("instance_binaries") or {},
        "artifacts": sorted(
            (artifact.artifact_id, artifact.provider_id, _digest(to_plain_data(artifact.config)))
            for artifact in plan.artifacts
        ),
    }
    data = {
        "experiment": plan.experiment_id,
        # The same deployment shape on another deployment provider is another
        # combination.
        "deployment": {
            "mode": plan.deployment_mode,
            "provider": plan.deployment_provider.provider_id,
            "primaries": document.get("primaries"),
            "replicas_per_primary": document.get("replicas_per_primary"),
            "addressing": document.get("addressing"),
        },
        "config_mix": _digest(config_mix),
        "inputs": to_plain_data(plan.inputs),
        "artifact_mix": _digest(artifact_mix),
        "resources": sorted(resource.provider_id for resource in plan.resources),
        "policies": sorted(policy.policy_type for policy in plan.policies),
        "workloads": of_role(ComponentRole.WORKLOAD),
        "actions": of_role(ComponentRole.ACTION),
        "faults": of_role(ComponentRole.FAULT),
        "observers": of_role(ComponentRole.OBSERVER),
        "oracles": of_role(ComponentRole.ORACLE),
        # A different tmpfs size or policy threshold is a different combination.
        "resource_mix": _digest(
            sorted((resource.provider_id, _digest(to_plain_data(resource.config))) for resource in plan.resources)
        ),
        "policy_mix": _digest(
            sorted((policy.policy_type, _digest(to_plain_data(policy.config))) for policy in plan.policies)
        ),
    }
    return data


def coverage_key(plan: ExecutionPlan) -> str:
    return _digest(static_tuple(plan))


def coverage_tuple(plan: ExecutionPlan, outcome: Optional[RunOutcome] = None) -> dict[str, Any]:
    static = static_tuple(plan)
    data = {"key": _digest(static), "tuple": static}
    if outcome is not None:
        data["status"] = outcome.status.value
        data["evidence"] = sorted({item.evidence_id for item in outcome.all_evidence})
    return data


__all__ = ["coverage_key", "coverage_tuple", "static_tuple"]
