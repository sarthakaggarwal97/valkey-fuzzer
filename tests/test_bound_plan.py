"""Acceptance tests for runtime-only bound run plans."""

import json
import pickle
from types import MappingProxyType

import pytest

from valkey_fuzzer.bound_plan import (
    BoundComponentSpec,
    BoundNodeSelector,
    BoundPlanBindingMismatchError,
    BoundPlanExpiredError,
    BoundPolicy,
    BoundRunPlan,
    BoundStartupOverlay,
    RunServices,
    UnresolvedBoundPlanReferenceError,
)
from valkey_fuzzer.environment import (
    ArtifactSpec,
    PolicySpec,
    ProvisionedResource,
    ResolvedArtifact,
    ResourceSpec,
    collect_environment_references,
    parse_environment_reference,
    resolve_environment_references,
)
from valkey_fuzzer.environment_lifecycle import EnvironmentBinding, EnvironmentSessionClosedError
from valkey_fuzzer.extensibility import (
    ComponentKind,
    ComponentSpec,
    EvidenceRequirement,
    NodeSelector,
    RunMode,
    RunPlan,
    StartupOverlay,
    TopologyProviderIdentity,
    to_plain_data,
)


def _ref(path):
    return parse_environment_reference(path)


class _BindingSession:
    def __init__(self, artifacts, resources):
        self._artifacts = MappingProxyType(dict(artifacts))
        self._resources = MappingProxyType(dict(resources))
        self._active = True

    def _assert_binding_active(self):
        if not self._active:
            raise EnvironmentSessionClosedError("binding is closed")

    def _active_artifacts(self):
        self._assert_binding_active()
        return self._artifacts

    def _active_resources(self):
        self._assert_binding_active()
        return self._resources

    def resolve(self, value):
        self._assert_binding_active()
        return resolve_environment_references(
            value,
            artifacts=self._artifacts,
            resources=self._resources,
        )

    def close(self):
        self._active = False


class _PassThroughSession(_BindingSession):
    def resolve(self, value):
        return value


class _SecretService:
    def __repr__(self):
        return "SECRET_SERVICE_VALUE"

    def __eq__(self, other):
        raise AssertionError(f"service equality must not run: {other!r}")


def _binding(*, extra_artifact=False):
    launcher = _SecretService()
    client = _SecretService()
    artifacts = {
        "candidate": ResolvedArtifact(
            artifact_id="candidate",
            provider_id="tests.artifact",
            outputs={"path": "/private/candidate", "tag": "candidate-secret-tag"},
            provenance={"digest": "report-safe"},
            services={"launcher": launcher},
        )
    }
    if extra_artifact:
        artifacts["unexpected"] = ResolvedArtifact(
            artifact_id="unexpected",
            provider_id="tests.artifact",
            outputs={"path": "/private/unexpected"},
            provenance={},
        )
    resources = {
        "store": ProvisionedResource(
            resource_id="store",
            provider_id="tests.resource",
            outputs={"endpoint": "secret://store", "zone": "test-zone"},
            provenance={"implementation": "fake"},
            services={"client": client},
        )
    }
    return EnvironmentBinding(_BindingSession(artifacts, resources)), launcher, client


def _plan():
    return RunPlan(
        scenario_id="bound-scenario",
        pack_id="tests.bound-pack",
        pack_version="1",
        seed=41,
        topology="standalone",
        topology_provider=TopologyProviderIdentity(
            provider_id="tests.topology-provider",
            implementation_version="1",
            api_version="v1",
        ),
        mode=RunMode.FEATURE,
        inputs={
            "candidate_path": _ref("artifacts.candidate.path"),
            "store": {"endpoint": _ref("resources.store.endpoint")},
        },
        artifacts=(
            ArtifactSpec(
                artifact_id="candidate",
                provider_id="tests.artifact",
            ),
        ),
        resources=(
            ResourceSpec(
                resource_id="store",
                provider_id="tests.resource",
            ),
        ),
        components=(
            ComponentSpec(
                kind=ComponentKind.WORKLOAD,
                component_id="tests.workload",
                instance_id="writer",
                config={
                    "binary": _ref("artifacts.candidate.path"),
                    "endpoint": _ref("resources.store.endpoint"),
                },
            ),
            ComponentSpec(
                kind=ComponentKind.ORACLE,
                component_id="tests.oracle",
                instance_id="oracle",
                config={"zone": _ref("resources.store.zone")},
                depends_on=("writer",),
            ),
        ),
        policies=(
            PolicySpec(
                policy_id="retention",
                policy_type="tests.retention",
                config={"endpoint": _ref("resources.store.endpoint")},
            ),
            PolicySpec(
                policy_id="verification",
                policy_type="tests.verification",
                config={"artifact": _ref("artifacts.candidate.tag")},
            ),
        ),
        startup_overlays=(
            StartupOverlay(
                overlay_id="candidate-startup",
                selector=NodeSelector(
                    roles=("primary",),
                    labels={"zone": _ref("resources.store.zone")},
                ),
                server_config={"module": _ref("artifacts.candidate.path")},
                artifact="candidate",
            ),
        ),
        required_events=("workload.completed",),
        required_evidence=(
            EvidenceRequirement(
                "tests.verdict",
                source="oracle",
            ),
        ),
        topology_document={
            "type": "standalone",
            "binary": _ref("artifacts.candidate.path"),
            "storage": {"endpoint": _ref("resources.store.endpoint")},
        },
        metadata={"suite": "bound-plan"},
    )


def _all_executable_values(bound):
    return (
        bound.inputs,
        bound.topology_document,
        *(component.config for component in bound.components),
        *(policy.config for policy in bound.policies),
        *(overlay.selector.labels for overlay in bound.startup_overlays),
        *(overlay.server_config for overlay in bound.startup_overlays),
    )


def test_materialize_resolves_every_executable_surface_and_preserves_source_plan():
    source = _plan()
    binding, launcher, client = _binding()
    topology_client = _SecretService()

    bound = BoundRunPlan.materialize(
        source,
        binding,
        topology_services={"client": topology_client},
    )

    assert bound.source_plan is source
    assert bound.reportable_plan is source
    assert collect_environment_references(source.inputs)
    assert all(not collect_environment_references(value) for value in _all_executable_values(bound))
    assert bound.inputs == {
        "candidate_path": "/private/candidate",
        "store": {"endpoint": "secret://store"},
    }
    assert bound.topology_document == {
        "type": "standalone",
        "binary": "/private/candidate",
        "storage": {"endpoint": "secret://store"},
    }
    assert bound.components[0].config == {
        "binary": "/private/candidate",
        "endpoint": "secret://store",
    }
    assert bound.components[1].config == {"zone": "test-zone"}
    assert bound.startup_overlays[0].selector.labels == {"zone": "test-zone"}
    assert bound.startup_overlays[0].server_config == {"module": "/private/candidate"}
    assert bound.services.artifact("candidate")["launcher"] is launcher
    assert bound.services.resource("store")["client"] is client
    assert bound.services.topology["client"] is topology_client


def test_bound_plan_delegates_only_stable_identity_and_evidence_metadata():
    source = _plan()
    binding, _launcher, _client = _binding()
    bound = BoundRunPlan.materialize(source, binding)

    assert bound.scenario_id == source.scenario_id
    assert bound.pack_id == source.pack_id
    assert bound.pack_version == source.pack_version
    assert bound.seed == source.seed
    assert bound.topology == source.topology
    assert bound.topology_provider is source.topology_provider
    assert bound.mode is source.mode
    assert bound.required_events is source.required_events
    assert bound.required_evidence is source.required_evidence
    assert bound.metadata is source.metadata
    assert bound.rng("writer", "operations").random() == source.rng("writer", "operations").random()
    assert not hasattr(bound, "artifacts")
    assert not hasattr(bound, "resources")


def test_policies_are_available_in_declaration_order_and_by_opaque_id():
    bound = BoundRunPlan.materialize(_plan(), _binding()[0])

    assert [policy.policy_id for policy in bound.policies] == ["retention", "verification"]
    assert tuple(bound.policies_by_id) == ("retention", "verification")
    assert bound.policy("retention") is bound.policies[0]
    assert bound.policy("retention").policy_type == "tests.retention"
    assert bound.policy("retention").config == {"endpoint": "secret://store"}
    with pytest.raises(KeyError):
        bound.policy("missing")


def test_runtime_models_are_immutable_redacted_and_not_serializable():
    source = _plan()
    binding, _launcher, _client = _binding()
    topology_service = _SecretService()
    first = BoundRunPlan.materialize(
        source,
        binding,
        topology_services={"client": topology_service},
    )
    second = BoundRunPlan.materialize(
        source,
        binding,
        topology_services={"client": topology_service},
    )

    rendered = " ".join(
        repr(value)
        for value in (
            first,
            first.components[0],
            first.policies[0],
            first.startup_overlays[0],
            first.startup_overlays[0].selector,
            first.services,
        )
    )
    assert "/private/candidate" not in rendered
    assert "secret://store" not in rendered
    assert "candidate-secret-tag" not in rendered
    assert "SECRET_SERVICE_VALUE" not in rendered
    assert "<redacted>" in rendered
    assert first is not second
    assert first != second
    assert first.services != second.services

    with pytest.raises(AttributeError, match="immutable"):
        first._inputs = {}
    with pytest.raises(TypeError):
        first.inputs["new"] = "value"
    with pytest.raises(TypeError):
        first.inputs["store"]["endpoint"] = "value"
    with pytest.raises(TypeError):
        first.services.artifacts["new"] = {}
    with pytest.raises(TypeError):
        first.services.artifacts["candidate"]["new"] = object()

    assert not hasattr(first, "to_data")
    assert not hasattr(first, "to_json")
    with pytest.raises(TypeError, match="cannot convert BoundRunPlan"):
        to_plain_data(first)
    with pytest.raises(TypeError):
        json.dumps(first)
    with pytest.raises(TypeError, match="runtime-only"):
        pickle.dumps(first)


def test_distinct_bound_models_cannot_be_confused_with_declarative_models():
    bound = BoundRunPlan.materialize(_plan(), _binding()[0])

    assert isinstance(bound.components[0], BoundComponentSpec)
    assert not isinstance(bound.components[0], ComponentSpec)
    assert isinstance(bound.policies[0], BoundPolicy)
    assert not isinstance(bound.policies[0], PolicySpec)
    assert isinstance(bound.startup_overlays[0], BoundStartupOverlay)
    assert not isinstance(bound.startup_overlays[0], StartupOverlay)
    assert isinstance(bound.startup_overlays[0].selector, BoundNodeSelector)
    assert not isinstance(bound.startup_overlays[0].selector, NodeSelector)
    assert isinstance(bound.services, RunServices)


def test_binding_must_exactly_match_source_environment_declarations():
    with pytest.raises(BoundPlanBindingMismatchError) as raised:
        BoundRunPlan.materialize(_plan(), _binding(extra_artifact=True)[0])

    assert raised.value.kind == "artifacts"
    assert raised.value.expected == ("candidate",)
    assert raised.value.actual == ("candidate", "unexpected")
    assert "unexpected" in str(raised.value)


def test_materialization_fails_closed_if_binding_leaves_a_typed_reference():
    source = _plan()
    binding = EnvironmentBinding(_PassThroughSession({}, {}))

    with pytest.raises(UnresolvedBoundPlanReferenceError) as raised:
        BoundComponentSpec.materialize(source.components[0], binding)

    assert raised.value.location == "components.writer.config.binary"
    assert raised.value.reference == _ref("artifacts.candidate.path")


def test_run_services_reject_typed_references_and_keep_namespaces_separate():
    service = _SecretService()
    mapping_client = {}
    mapping_client["self"] = mapping_client
    services = RunServices(
        artifacts={"same": {"client": service}},
        resources={"same": {"client": service}},
        topology={"client": service, "mapping_client": mapping_client},
    )

    assert services.artifacts["same"]["client"] is service
    assert services.resources["same"]["client"] is service
    assert services.topology["client"] is service
    assert services.topology["mapping_client"] is mapping_client
    assert tuple(services) == ("artifacts", "resources", "topology")
    assert services["artifacts"] is services.artifacts
    assert services["resources"] is services.resources
    assert services["topology"] is services.topology
    assert len({id(services.artifacts), id(services.resources), id(services.topology)}) == 3

    with pytest.raises(UnresolvedBoundPlanReferenceError) as raised:
        RunServices(topology={"invalid": _ref("resources.store.endpoint")})
    assert raised.value.location == "services.topology.invalid"


def test_bound_runtime_views_expire_with_the_owning_environment_session():
    source = _plan()
    session = _BindingSession(
        *(
            {
                "candidate": ResolvedArtifact(
                    artifact_id="candidate",
                    provider_id="tests.artifact",
                    outputs={"path": "/private/candidate", "tag": "candidate-secret-tag"},
                    provenance={},
                    services={"launcher": object()},
                )
            },
            {
                "store": ProvisionedResource(
                    resource_id="store",
                    provider_id="tests.resource",
                    outputs={"endpoint": "secret://store", "zone": "test-zone"},
                    provenance={},
                    services={"client": object()},
                )
            },
        )
    )
    bound = BoundRunPlan.materialize(
        source,
        EnvironmentBinding(session),
        topology_services={"client": object()},
    )
    retained_config = bound.components[0].config
    retained_topology = bound.topology_document
    retained_services = bound.services
    retained_resource_namespace = retained_services.resources

    session.close()

    for access in (
        lambda: bound.components,
        lambda: bound.topology_document,
        lambda: retained_config["binary"],
        lambda: retained_topology["binary"],
        lambda: retained_services["resources"],
        lambda: retained_resource_namespace["store"],
    ):
        with pytest.raises(BoundPlanExpiredError):
            access()
    assert "secret://store" not in repr(bound)
    assert "SECRET_SERVICE_VALUE" not in repr(retained_services)
