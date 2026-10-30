"""Minimal, neutral external extension for valkey-fuzzer."""

from valkey_fuzzer.extensions import (
    ComponentRegistration,
    ComponentResult,
    ComponentRole,
    ComponentSpec,
    ComponentStatus,
    ConfigError,
    ConfigField,
    ConfigSchema,
    Declarations,
    DeploymentProviderRegistration,
    Evidence,
    EvidenceDeclaration,
    EvidenceRequirement,
    ExperimentSpec,
    PluginManifest,
    ProvisionedDeployment,
    RunMode,
)

DEPLOYMENT_MODE = "example-neutral"
ACTION_COMPONENT_ID = "example.action.neutral"
ORACLE_COMPONENT_ID = "example.oracle.neutral"
OBSERVED_EVENT = "example.neutral.input-observed"
OBSERVED_EVIDENCE = "example.neutral.input-observed"


# Both components take one string, ``message``; the compiler checks it
# against this schema and the check below.
MESSAGE_CONFIG = ConfigSchema({"message": ConfigField(str)})


# What the action publishes and emits, with their schemas; the runner rejects
# anything else, and anything malformed, as an ERROR.
OBSERVATION = {"message": ConfigField(str), "deployment": ConfigField(str)}
ACTION_DECLARES = Declarations(
    events={OBSERVED_EVENT: ConfigSchema(OBSERVATION)},
    evidence={OBSERVED_EVIDENCE: EvidenceDeclaration(ConfigField(dict, schema=ConfigSchema(OBSERVATION)))},
)


def _check_message(config, spec, plan):
    del plan
    if not config["message"]:
        raise ConfigError(f"{spec.id!r} requires a non-empty message")


class NeutralDeploymentProvider:
    """No-op provider showing that deployments are ordinary extensions."""

    def validate(self, plan):
        if plan.deployment_mode != DEPLOYMENT_MODE:
            raise ValueError(f"expected deployment mode {DEPLOYMENT_MODE!r}, got {plan.deployment_mode!r}")

    def provision(self, context):
        del context
        return ProvisionedDeployment(services={"kind": DEPLOYMENT_MODE})

    def rollback(self, context):
        del context

    def cleanup(self, context, deployment):
        del context, deployment


class ConfiguredAction:
    """Publish and preserve evidence derived from resolved scenario input."""

    def run(self, context, spec):
        message = spec.config["message"]
        deployment = context.services["deployment"]["kind"]
        event = context.event_bus.publish(
            OBSERVED_EVENT,
            source=spec.id,
            payload={"message": message, "deployment": deployment},
        )
        return ComponentResult(
            component_id=spec.id,
            status=ComponentStatus.PASS,
            summary="resolved scenario input was executed",
            evidence=(
                Evidence(
                    evidence_id=OBSERVED_EVIDENCE,
                    source=spec.id,
                    phase=OBSERVED_EVENT,
                    observed_at=event.timestamp,
                    value={"message": message, "deployment": deployment},
                    event_sequence=event.sequence,
                ),
            ),
        )


class EvidenceOracle:
    """Give a deterministic verdict over evidence produced by the action."""

    def evaluate(self, context, spec, evidence):
        del context
        expected_message = spec.config["message"]
        matched = any(
            item.evidence_id == OBSERVED_EVIDENCE
            and item.source == "exercise"
            and item.value == {"message": expected_message, "deployment": DEPLOYMENT_MODE}
            for item in evidence
        )
        return ComponentResult(
            component_id=spec.id,
            status=ComponentStatus.PASS if matched else ComponentStatus.FAIL,
            summary="action evidence matched resolved input"
            if matched
            else "action evidence did not match resolved input",
        )


# Components name their registration; the role comes from it.
EXPERIMENT = ExperimentSpec(
    experiment_id="example.neutral",
    mode=RunMode.EXPERIMENT,
    deployment_modes=(DEPLOYMENT_MODE,),
    inputs={
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
            use=ACTION_COMPONENT_ID,
            id="exercise",
            config={"message": {"$ref": "inputs.message"}},
        ),
        ComponentSpec(
            use=ORACLE_COMPONENT_ID,
            id="verify",
            config={"message": {"$ref": "inputs.message"}},
            depends_on=("exercise",),
        ),
    ),
    required_events=(OBSERVED_EVENT,),
    required_evidence=(
        EvidenceRequirement(
            evidence_id=OBSERVED_EVIDENCE,
            source="exercise",
        ),
    ),
    metadata={"purpose": "external-extension-onboarding-template"},
)


PLUGIN = PluginManifest(
    plugin_id="example.neutral-plugin",
    registrations=(
        ComponentRegistration(
            component_id=ACTION_COMPONENT_ID,
            role=ComponentRole.ACTION,
            factory=ConfiguredAction,
            config=MESSAGE_CONFIG,
            check=_check_message,
            declares=ACTION_DECLARES,
        ),
        ComponentRegistration(
            component_id=ORACLE_COMPONENT_ID,
            role=ComponentRole.ORACLE,
            factory=EvidenceOracle,
            config=MESSAGE_CONFIG,
            check=_check_message,
            declares=Declarations(),
        ),
    ),
    experiments=(EXPERIMENT,),
    deployment_providers=(
        DeploymentProviderRegistration(
            deployment_mode=DEPLOYMENT_MODE,
            provider_id="example.deployment.neutral",
            factory=NeutralDeploymentProvider,
        ),
    ),
    metadata={"template": True},
)

__all__ = ["PLUGIN"]
