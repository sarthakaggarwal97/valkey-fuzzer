"""Contract tests for the external plugin import boundary."""

import valkey_fuzzer
from valkey_fuzzer import environment, extensibility, extensions, scheduling
from valkey_fuzzer.extensions import (
    EXTENSION_API_VERSION,
    ActivationMode,
    ActivationSpec,
    ArtifactProviderRegistration,
    ComponentKind,
    ComponentRegistration,
    ComponentResult,
    ComponentStatus,
    FeaturePackSpec,
    PluginManifest,
    ProvisionedTopology,
    RunMode,
    TopologyProviderRegistration,
    TriggerKind,
    TriggerSpec,
)

EXPECTED_PUBLIC_NAMES = {
    "EXTENSION_API_VERSION",
    "PLUGIN_ENTRY_POINT_GROUP",
    "Action",
    "ActivationMode",
    "ActivationSpec",
    "ArtifactProvider",
    "ArtifactProviderRegistration",
    "ArtifactProviderResolvedConfigValidator",
    "ArtifactSpec",
    "CapabilityProbe",
    "ComponentKind",
    "ComponentRegistration",
    "ComponentResult",
    "ComponentSpec",
    "ComponentStatus",
    "EnvironmentDependency",
    "EnvironmentKind",
    "EnvironmentProviderContext",
    "EnvironmentReference",
    "EventBus",
    "EventSubscription",
    "Evidence",
    "EvidenceRequirement",
    "EventScope",
    "EventSelector",
    "ExecutableComponentSpec",
    "ExecutableNodeSelector",
    "ExecutablePolicySpec",
    "ExecutableRunPlan",
    "ExecutableStartupOverlay",
    "Fault",
    "FeaturePackSpec",
    "LifecycleEvent",
    "NodeSelector",
    "Observer",
    "Oracle",
    "PluginManifest",
    "PolicySpec",
    "ProvisionedResource",
    "ProvisionedTopology",
    "ResolvedArtifact",
    "ResourceProvider",
    "ResourceProviderRegistration",
    "ResourceProviderResolvedConfigValidator",
    "ResourceSpec",
    "RunContext",
    "RunMode",
    "StartupOverlay",
    "SupportDecision",
    "TopologyProvider",
    "TopologyProviderIdentity",
    "TopologyProviderRegistration",
    "TriggerKind",
    "TriggerSpec",
    "Workload",
}

FORBIDDEN_HOST_NAMES = {
    "ArtifactProviderRegistry",
    "LifecycleEventBus",
    "OutcomeStatus",
    "PluginDiscoveryError",
    "Registry",
    "ResourceProviderRegistry",
    "RunOutcome",
    "RunPlan",
    "TopologyProviderRegistry",
    "canonical_json",
    "discover_plugins",
    "run_outcome_from_data",
    "run_plan_from_data",
    "to_plain_data",
}

FORBIDDEN_SCHEDULER_NAMES = {
    "DeterministicActivationPlanner",
    "ScheduleJournalEntry",
    "ScheduleJournalSnapshot",
    "ScheduledActivation",
    "validate_schedule_replay",
}

DECLARATIVE_SCHEDULER_NAMES = {
    "ActivationMode",
    "ActivationSpec",
    "EventScope",
    "EventSelector",
    "TriggerKind",
    "TriggerSpec",
}


class _Action:
    def run(self, context, spec):
        return ComponentResult(
            component_instance_id=spec.instance_id,
            status=ComponentStatus.PASS,
            summary="public API action completed",
        )


class _ArtifactProvider:
    pass


class _TopologyProvider:
    def validate(self, plan):
        pass

    def provision(self, context):
        return ProvisionedTopology()

    def rollback(self, context):
        pass

    def cleanup(self, context, topology):
        pass


def test_public_extension_surface_is_explicit_and_author_facing():
    assert set(extensions.__all__) == EXPECTED_PUBLIC_NAMES
    assert len(extensions.__all__) == len(set(extensions.__all__))
    assert all(getattr(extensions, name) is not None for name in extensions.__all__)
    assert FORBIDDEN_HOST_NAMES.isdisjoint(extensions.__all__)
    assert FORBIDDEN_SCHEDULER_NAMES.isdisjoint(extensions.__all__)
    assert set(extensions.__all__).intersection(scheduling.__all__) == DECLARATIVE_SCHEDULER_NAMES


def test_public_exports_preserve_runtime_type_identity():
    assert extensions.ComponentSpec is extensibility.ComponentSpec
    assert extensions.ArtifactSpec is environment.ArtifactSpec
    assert extensions.EventBus is extensibility.EventBus
    assert extensions.ActivationSpec is scheduling.ActivationSpec
    assert valkey_fuzzer.EXTENSION_API_VERSION == EXTENSION_API_VERSION


def test_plugin_manifest_can_be_authored_using_only_the_public_facade():
    manifest = PluginManifest(
        plugin_id="example.public-api",
        api_version=EXTENSION_API_VERSION,
        registrations=(
            ComponentRegistration(
                component_id="example.public-action",
                kind=ComponentKind.ACTION,
                api_version=EXTENSION_API_VERSION,
                factory=_Action,
            ),
        ),
        feature_packs=(
            FeaturePackSpec(
                pack_id="example.public-pack",
                version="1",
                supported_topologies=("example-topology",),
                components=(),
                mode=RunMode.TOPOLOGY_SMOKE,
            ),
        ),
        artifact_providers=(
            ArtifactProviderRegistration(
                provider_id="example.public-artifact",
                api_version=EXTENSION_API_VERSION,
                factory=_ArtifactProvider,
            ),
        ),
        topology_providers=(
            TopologyProviderRegistration(
                topology="example-topology",
                provider_id="example.public-topology",
                implementation_version="1",
                api_version=EXTENSION_API_VERSION,
                factory=_TopologyProvider,
            ),
        ),
    )

    assert manifest.plugin_id == "example.public-api"


def test_public_activation_contract_is_declarative_without_exposing_scheduler_internals():
    activation = ActivationSpec(
        trigger=TriggerSpec(kind=TriggerKind.READY, offset_s=0.5),
        mode=ActivationMode.SERIAL,
    )

    assert activation.to_data() == {
        "mode": "serial",
        "trigger": {
            "event": None,
            "kind": "ready",
            "offset_s": 0.5,
            "scope": "after_ready",
            "timeout_s": None,
        },
    }
