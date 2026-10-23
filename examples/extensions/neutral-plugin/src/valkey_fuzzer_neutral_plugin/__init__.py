"""Minimal, feature-neutral external extension for valkey-fuzzer."""

from valkey_fuzzer.extensions import (
    EXTENSION_API_VERSION,
    ComponentKind,
    ComponentRegistration,
    ComponentResult,
    ComponentSpec,
    ComponentStatus,
    Evidence,
    EvidenceRequirement,
    FeaturePackSpec,
    PluginManifest,
    ProvisionedTopology,
    RunMode,
    TopologyProviderRegistration,
)

TOPOLOGY = "example-neutral"
ACTION_COMPONENT_ID = "example.neutral.action"
ORACLE_COMPONENT_ID = "example.neutral.oracle"
FEATURE_EVENT = "example.neutral.input-observed"
FEATURE_EVIDENCE = "example.neutral.input-observed"


def _configured_message(spec):
    message = spec.config.get("message")
    if not isinstance(message, str) or not message:
        raise ValueError(f"{spec.instance_id!r} requires a non-empty string config field named 'message'")
    return message


def _validate_message_component(spec, plan):
    del plan
    _configured_message(spec)


class NeutralTopologyProvider:
    """No-op provider showing that topologies are ordinary extensions."""

    def validate(self, plan):
        if plan.topology != TOPOLOGY:
            raise ValueError(f"expected topology {TOPOLOGY!r}, got {plan.topology!r}")

    def provision(self, context):
        del context
        return ProvisionedTopology(services={"kind": TOPOLOGY})

    def rollback(self, context):
        del context

    def cleanup(self, context, topology):
        del context, topology


class ConfiguredAction:
    """Publish and preserve evidence derived from resolved scenario input."""

    def run(self, context, spec):
        message = _configured_message(spec)
        topology = context.services["topology"]["kind"]
        event = context.event_bus.publish(
            FEATURE_EVENT,
            source=spec.instance_id,
            payload={"message": message, "topology": topology},
        )
        return ComponentResult(
            component_instance_id=spec.instance_id,
            status=ComponentStatus.PASS,
            summary="resolved scenario input was executed",
            evidence=(
                Evidence(
                    evidence_id=FEATURE_EVIDENCE,
                    source=spec.instance_id,
                    phase=FEATURE_EVENT,
                    observed_at=event.timestamp,
                    value={"message": message, "topology": topology},
                    event_sequence=event.sequence,
                ),
            ),
        )


class EvidenceOracle:
    """Give a deterministic verdict over evidence produced by the action."""

    def evaluate(self, context, spec, evidence):
        del context
        expected_message = _configured_message(spec)
        matched = any(
            item.evidence_id == FEATURE_EVIDENCE
            and item.source == "exercise"
            and item.value == {"message": expected_message, "topology": TOPOLOGY}
            for item in evidence
        )
        return ComponentResult(
            component_instance_id=spec.instance_id,
            status=ComponentStatus.PASS if matched else ComponentStatus.FAIL,
            summary="action evidence matched resolved input"
            if matched
            else "action evidence did not match resolved input",
        )


FEATURE_PACK = FeaturePackSpec(
    pack_id="example.neutral",
    version="1",
    mode=RunMode.FEATURE,
    supported_topologies=(TOPOLOGY,),
    input_schema={
        "type": "object",
        "properties": {
            "message": {
                "type": "string",
                "description": "Value passed through the compiler into component execution.",
            },
        },
        "required": ["message"],
        "additionalProperties": False,
    },
    components=(
        ComponentSpec(
            kind=ComponentKind.ACTION,
            component_id=ACTION_COMPONENT_ID,
            instance_id="exercise",
            config={"message": {"$ref": "inputs.message"}},
        ),
        ComponentSpec(
            kind=ComponentKind.ORACLE,
            component_id=ORACLE_COMPONENT_ID,
            instance_id="verify",
            config={"message": {"$ref": "inputs.message"}},
            depends_on=("exercise",),
        ),
    ),
    required_events=(FEATURE_EVENT,),
    required_evidence=(
        EvidenceRequirement(
            evidence_id=FEATURE_EVIDENCE,
            source="exercise",
        ),
    ),
    metadata={"purpose": "external-extension-onboarding-template"},
)


PLUGIN = PluginManifest(
    plugin_id="example.neutral-plugin",
    api_version=EXTENSION_API_VERSION,
    registrations=(
        ComponentRegistration(
            component_id=ACTION_COMPONENT_ID,
            kind=ComponentKind.ACTION,
            api_version=EXTENSION_API_VERSION,
            factory=ConfiguredAction,
            validator=_validate_message_component,
        ),
        ComponentRegistration(
            component_id=ORACLE_COMPONENT_ID,
            kind=ComponentKind.ORACLE,
            api_version=EXTENSION_API_VERSION,
            factory=EvidenceOracle,
            validator=_validate_message_component,
        ),
    ),
    feature_packs=(FEATURE_PACK,),
    topology_providers=(
        TopologyProviderRegistration(
            topology=TOPOLOGY,
            provider_id="example.neutral.topology-provider",
            implementation_version="1",
            api_version=EXTENSION_API_VERSION,
            factory=NeutralTopologyProvider,
        ),
    ),
    metadata={"template": True},
)

__all__ = ["PLUGIN"]
