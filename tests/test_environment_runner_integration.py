"""End-to-end contracts between environment acquisition and the generic runner."""

import os
import tempfile
from typing import Any

import pytest

from valkey_fuzzer.bound_plan import (
    BoundComponentSpec,
    BoundPlanExpiredError,
    BoundPolicy,
    BoundRunPlan,
)
from valkey_fuzzer.environment import (
    ArtifactProviderRegistration,
    ArtifactProviderRegistry,
    ArtifactSpec,
    PolicySpec,
    ProvisionedResource,
    ResolvedArtifact,
    ResourceProviderRegistration,
    ResourceProviderRegistry,
    ResourceSpec,
    SupportDecision,
    parse_environment_reference,
)
from valkey_fuzzer.environment_lifecycle import EnvironmentManager
from valkey_fuzzer.extensibility import (
    ComponentKind,
    ComponentRegistration,
    ComponentResult,
    ComponentSpec,
    ComponentStatus,
    OutcomeStatus,
    Registry,
    RunMode,
    RunPlan,
    TopologyProviderIdentity,
    TopologyProviderRegistration,
)
from valkey_fuzzer.feature_runner import (
    FeatureRunner,
    ProvisionedTopology,
    normalize_artifact_root,
)


def _ref(path):
    return parse_environment_reference(path)


def _manager(
    tmp_path,
    *,
    artifact_registration=None,
    resource_registration=None,
):
    artifacts = ArtifactProviderRegistry()
    resources = ResourceProviderRegistry()
    if artifact_registration is not None:
        artifacts.register(artifact_registration)
    if resource_registration is not None:
        resources.register(resource_registration)
    return EnvironmentManager(
        artifact_registry=artifacts,
        resource_registry=resources,
        artifact_root=tmp_path,
    )


def _runner(
    tmp_path,
    *,
    topology_provider,
    environment_manager=None,
    registrations=(),
):
    registries = {}
    for registration in registrations:
        registry = registries.setdefault(
            registration.kind,
            Registry[Any](registration.kind),
        )
        registry.register(registration)
    topology_registration = TopologyProviderRegistration(
        topology="standalone",
        provider_id="tests.environment-topology",
        implementation_version="1",
        api_version="v1",
        factory=lambda: topology_provider,
    )
    return FeatureRunner(
        registries,
        {"standalone": topology_registration},
        artifact_dir=str(tmp_path),
        environment_manager=environment_manager,
        run_id_factory=lambda: "integration-run",
    )


def _plan(
    *,
    components=(),
    artifacts=(),
    resources=(),
    policies=(),
    topology_document=None,
):
    return RunPlan(
        scenario_id="environment-runner",
        pack_id="tests.environment-runner",
        pack_version="1",
        seed=17,
        topology="standalone",
        components=tuple(components),
        topology_provider=TopologyProviderIdentity(
            provider_id="tests.environment-topology",
            implementation_version="1",
            api_version="v1",
        ),
        mode=RunMode.TOPOLOGY_SMOKE,
        artifacts=tuple(artifacts),
        resources=tuple(resources),
        policies=tuple(policies),
        topology_document=({"type": "standalone"} if topology_document is None else topology_document),
    )


def test_bound_execution_preserves_namespaces_and_exact_cleanup_order(tmp_path):
    calls = []
    artifact_client = object()
    resource_client = object()
    topology_client = object()

    class ArtifactProvider:
        def check_support(self, context, spec, resolved_config):
            calls.append("artifact.support")
            assert context.dependency_services == {}
            return SupportDecision.allow()

        def resolve(self, context, spec, resolved_config):
            calls.append("artifact.acquire")
            assert context.artifact_dir.startswith(str(tmp_path))
            return ResolvedArtifact(
                artifact_id=spec.artifact_id,
                provider_id=spec.provider_id,
                outputs={"path": "/private/candidate"},
                provenance={"digest": "report-safe"},
                services={"client": artifact_client},
                handle=object(),
            )

        def rollback(self, context, spec):
            calls.append("artifact.rollback")

        def cleanup(self, context, spec, artifact):
            calls.append("artifact.cleanup")

    class ResourceProvider:
        def check_support(self, context, spec, resolved_config):
            calls.append("resource.support")
            assert context.dependency_services["artifacts.same"]["client"] is artifact_client
            return SupportDecision.allow()

        def provision(self, context, spec, resolved_config):
            calls.append("resource.acquire")
            assert resolved_config == {"binary": "/private/candidate"}
            return ProvisionedResource(
                resource_id=spec.resource_id,
                provider_id=spec.provider_id,
                outputs={"endpoint": "private://store"},
                provenance={"kind": "test"},
                services={"client": resource_client},
                handle=object(),
            )

        def rollback(self, context, spec):
            calls.append("resource.rollback")

        def cleanup(self, context, spec, resource):
            calls.append("resource.cleanup")

    class TopologyProvider:
        def validate(self, plan):
            calls.append("topology.validate")
            assert isinstance(plan, BoundRunPlan)
            assert plan.topology_document["binary"] == "/private/candidate"
            assert plan.policies[0].config["endpoint"] == "private://store"

        def provision(self, context):
            calls.append("topology.provision")
            assert context.services["artifacts"]["same"]["client"] is artifact_client
            assert context.services["resources"]["same"]["client"] is resource_client
            assert context.services["topology"] == {}
            return ProvisionedTopology(services={"client": topology_client})

        def rollback(self, context):
            calls.append("topology.rollback")

        def cleanup(self, context, topology):
            calls.append("topology.cleanup")
            assert context.services["topology"]["client"] is topology_client

    class Workload:
        def start(self, context, spec):
            calls.append("workload.start")
            assert isinstance(context.plan, BoundRunPlan)
            assert isinstance(spec, BoundComponentSpec)
            assert spec.config == {
                "binary": "/private/candidate",
                "endpoint": "private://store",
            }
            assert context.services["artifacts"]["same"]["client"] is artifact_client
            assert context.services["resources"]["same"]["client"] is resource_client
            assert context.services["topology"]["client"] is topology_client
            return ComponentResult(spec.instance_id, ComponentStatus.PASS, "started")

        def stop(self, context, spec):
            calls.append("workload.stop")
            assert context.deadline is None
            assert context.services["topology"]["client"] is topology_client
            return ComponentResult(spec.instance_id, ComponentStatus.PASS, "stopped")

    def validate_workload(spec, plan):
        calls.append("workload.validate")
        assert isinstance(spec, BoundComponentSpec)
        assert isinstance(plan, BoundRunPlan)
        assert spec.config["binary"] == "/private/candidate"
        assert isinstance(plan.policies[0], BoundPolicy)
        assert plan.policies[0].config["endpoint"] == "private://store"

    manager = _manager(
        tmp_path,
        artifact_registration=ArtifactProviderRegistration(
            "tests.artifact",
            ArtifactProvider,
            outputs=("path",),
            services=("client",),
        ),
        resource_registration=ResourceProviderRegistration(
            "tests.resource",
            ResourceProvider,
            outputs=("endpoint",),
            services=("client",),
        ),
    )
    source = _plan(
        components=(
            ComponentSpec(
                ComponentKind.WORKLOAD,
                "tests.workload",
                "load",
                config={
                    "binary": _ref("artifacts.same.path"),
                    "endpoint": _ref("resources.same.endpoint"),
                },
            ),
        ),
        artifacts=(ArtifactSpec("same", "tests.artifact"),),
        resources=(
            ResourceSpec(
                "same",
                "tests.resource",
                config={"binary": _ref("artifacts.same.path")},
                depends_on=("artifacts.same",),
            ),
        ),
        policies=(
            PolicySpec(
                "retention",
                "tests.retention",
                config={"endpoint": _ref("resources.same.endpoint")},
            ),
        ),
        topology_document={
            "type": "standalone",
            "binary": _ref("artifacts.same.path"),
        },
    )
    registration = ComponentRegistration[Any](
        "tests.workload",
        ComponentKind.WORKLOAD,
        "v1",
        Workload,
        validator=validate_workload,
    )

    outcome = _runner(
        tmp_path,
        topology_provider=TopologyProvider(),
        environment_manager=manager,
        registrations=(registration,),
    ).run(source)

    assert outcome.status is OutcomeStatus.PASS
    assert outcome.plan is source
    assert calls == [
        "artifact.support",
        "artifact.acquire",
        "resource.support",
        "resource.acquire",
        "topology.validate",
        "workload.validate",
        "topology.provision",
        "workload.start",
        "workload.stop",
        "topology.cleanup",
        "resource.cleanup",
        "artifact.cleanup",
    ]
    assert outcome.diagnostics["environment_report"]["status"] == "cleaned"
    assert "/private/candidate" not in outcome.to_json()
    assert "private://store" not in outcome.to_json()


@pytest.mark.parametrize(
    ("cleanup_error", "expected_status"),
    (
        (None, OutcomeStatus.SKIP_UNSUPPORTED),
        (RuntimeError("cleanup-secret"), OutcomeStatus.ERROR),
    ),
)
def test_environment_support_denial_skips_only_after_clean_unwind(
    tmp_path,
    cleanup_error,
    expected_status,
):
    calls = []

    class ArtifactProvider:
        def check_support(self, context, spec, resolved_config):
            calls.append("artifact.support")
            return SupportDecision.allow()

        def resolve(self, context, spec, resolved_config):
            calls.append("artifact.acquire")
            return ResolvedArtifact(
                spec.artifact_id,
                spec.provider_id,
                {"path": "/candidate"},
                {},
            )

        def rollback(self, context, spec):
            calls.append("artifact.rollback")

        def cleanup(self, context, spec, artifact):
            calls.append("artifact.cleanup")
            if cleanup_error is not None:
                raise cleanup_error

    class ResourceProvider:
        def check_support(self, context, spec, resolved_config):
            calls.append("resource.support")
            return SupportDecision.deny("not available")

        def provision(self, context, spec, resolved_config):
            pytest.fail("unsupported resources must not be provisioned")

        def rollback(self, context, spec):
            calls.append("resource.rollback")

        def cleanup(self, context, spec, resource):
            calls.append("resource.cleanup")

    class TopologyProvider:
        def validate(self, plan):
            calls.append("topology.validate")

        def provision(self, context):
            calls.append("topology.provision")
            return ProvisionedTopology()

        def rollback(self, context):
            calls.append("topology.rollback")

        def cleanup(self, context, topology):
            calls.append("topology.cleanup")

    manager = _manager(
        tmp_path,
        artifact_registration=ArtifactProviderRegistration(
            "tests.artifact",
            ArtifactProvider,
            outputs=("path",),
        ),
        resource_registration=ResourceProviderRegistration(
            "tests.resource",
            ResourceProvider,
        ),
    )
    source = _plan(
        artifacts=(ArtifactSpec("candidate", "tests.artifact"),),
        resources=(
            ResourceSpec(
                "optional",
                "tests.resource",
                depends_on=("artifacts.candidate",),
            ),
        ),
    )

    outcome = _runner(
        tmp_path,
        topology_provider=TopologyProvider(),
        environment_manager=manager,
    ).run(source)

    assert outcome.status is expected_status
    assert calls == [
        "topology.validate",
        "artifact.support",
        "artifact.acquire",
        "resource.support",
        "artifact.cleanup",
    ]
    assert "topology.provision" not in calls
    assert outcome.diagnostics["environment_report"]["status"] == (
        "unsupported" if cleanup_error is None else "cleanup_failed"
    )


def test_replayed_plan_is_acquired_bound_and_reported_as_the_replayed_source(tmp_path):
    class ArtifactProvider:
        def check_support(self, context, spec, resolved_config):
            return SupportDecision.allow()

        def resolve(self, context, spec, resolved_config):
            return ResolvedArtifact(
                spec.artifact_id,
                spec.provider_id,
                {"path": "/replayed/candidate"},
                {"digest": "safe"},
            )

        def rollback(self, context, spec):
            return None

        def cleanup(self, context, spec, artifact):
            return None

    class TopologyProvider:
        def validate(self, plan):
            assert isinstance(plan, BoundRunPlan)
            assert plan.topology_document["binary"] == "/replayed/candidate"

        def provision(self, context):
            return ProvisionedTopology()

        def rollback(self, context):
            return None

        def cleanup(self, context, topology):
            return None

    manager = _manager(
        tmp_path,
        artifact_registration=ArtifactProviderRegistration(
            "tests.artifact",
            ArtifactProvider,
            outputs=("path",),
        ),
    )
    original = _plan(
        artifacts=(ArtifactSpec("candidate", "tests.artifact"),),
        topology_document={
            "type": "standalone",
            "binary": _ref("artifacts.candidate.path"),
        },
    )
    replayed = RunPlan.from_json(original.to_json())

    outcome = _runner(
        tmp_path,
        topology_provider=TopologyProvider(),
        environment_manager=manager,
    ).run(replayed)

    assert outcome.status is OutcomeStatus.PASS
    assert outcome.plan is replayed
    assert outcome.plan.to_json() == original.to_json()


def test_environment_exception_messages_and_runtime_values_never_enter_outcome(tmp_path):
    sentinel = "SUPER_SECRET_SENTINEL"

    class ArtifactProvider:
        def check_support(self, context, spec, resolved_config):
            return SupportDecision.allow()

        def resolve(self, context, spec, resolved_config):
            raise RuntimeError(f"provider failed with {sentinel}")

        def rollback(self, context, spec):
            return None

        def cleanup(self, context, spec, artifact):
            return None

    class TopologyProvider:
        def validate(self, plan):
            return None

        def provision(self, context):
            pytest.fail("topology must not be reached")

        def rollback(self, context):
            pytest.fail("topology must not be reached")

        def cleanup(self, context, topology):
            pytest.fail("topology must not be reached")

    manager = _manager(
        tmp_path,
        artifact_registration=ArtifactProviderRegistration(
            "tests.artifact",
            ArtifactProvider,
        ),
    )
    outcome = _runner(
        tmp_path,
        topology_provider=TopologyProvider(),
        environment_manager=manager,
    ).run(_plan(artifacts=(ArtifactSpec("candidate", "tests.artifact"),)))

    serialized = outcome.to_json()
    assert outcome.status is OutcomeStatus.ERROR
    assert sentinel not in serialized
    assert sentinel not in repr(outcome)
    assert outcome.diagnostics["environment_error"]["type"] == "RuntimeError"
    assert outcome.diagnostics["environment_report"]["records"][-1]["details"] == {}
    assert all(
        set(record["details"]) <= {"decision", "exception_type", "result"}
        for record in outcome.diagnostics["environment_report"]["records"]
    )


@pytest.mark.parametrize(
    "failure_phase",
    (
        "topology.validate",
        "topology.provision",
        "action.run",
        "workload.stop",
        "topology.cleanup",
    ),
)
def test_post_bind_exception_messages_never_echo_resolved_values(
    tmp_path,
    failure_phase,
):
    sentinel = f"SUPER_SECRET_POST_BIND_{failure_phase}"

    class ArtifactProvider:
        def check_support(self, context, spec, resolved_config):
            return SupportDecision.allow()

        def resolve(self, context, spec, resolved_config):
            return ResolvedArtifact(
                spec.artifact_id,
                spec.provider_id,
                {"token": sentinel},
                {},
            )

        def rollback(self, context, spec):
            return None

        def cleanup(self, context, spec, artifact):
            return None

    class TopologyProvider:
        def validate(self, plan):
            if failure_phase == "topology.validate":
                raise RuntimeError(f"validation echoed {plan.topology_document['token']}")

        def provision(self, context):
            if failure_phase == "topology.provision":
                raise RuntimeError(f"provision echoed {context.plan.topology_document['token']}")
            return ProvisionedTopology()

        def rollback(self, context):
            return None

        def cleanup(self, context, topology):
            if failure_phase == "topology.cleanup":
                raise RuntimeError(f"cleanup echoed {context.plan.topology_document['token']}")

    class Action:
        def run(self, context, spec):
            if failure_phase == "action.run":
                raise RuntimeError(f"action echoed {spec.config['token']}")
            return ComponentResult(spec.instance_id, ComponentStatus.PASS, "passed")

    class Workload:
        def start(self, context, spec):
            return ComponentResult(spec.instance_id, ComponentStatus.PASS, "started")

        def stop(self, context, spec):
            if failure_phase == "workload.stop":
                raise RuntimeError(f"workload cleanup echoed {spec.config['token']}")
            return ComponentResult(spec.instance_id, ComponentStatus.PASS, "stopped")

    registrations = ()
    components = ()
    if failure_phase == "action.run":
        registrations = (
            ComponentRegistration[Any](
                "tests.action",
                ComponentKind.ACTION,
                "v1",
                Action,
            ),
        )
        components = (
            ComponentSpec(
                ComponentKind.ACTION,
                "tests.action",
                "exercise",
                config={"token": _ref("artifacts.candidate.token")},
            ),
        )
    elif failure_phase == "workload.stop":
        registrations = (
            ComponentRegistration[Any](
                "tests.workload",
                ComponentKind.WORKLOAD,
                "v1",
                Workload,
            ),
        )
        components = (
            ComponentSpec(
                ComponentKind.WORKLOAD,
                "tests.workload",
                "exercise",
                config={"token": _ref("artifacts.candidate.token")},
            ),
        )

    manager = _manager(
        tmp_path,
        artifact_registration=ArtifactProviderRegistration(
            "tests.artifact",
            ArtifactProvider,
            outputs=("token",),
        ),
    )
    outcome = _runner(
        tmp_path,
        topology_provider=TopologyProvider(),
        environment_manager=manager,
        registrations=registrations,
    ).run(
        _plan(
            components=components,
            artifacts=(ArtifactSpec("candidate", "tests.artifact"),),
            topology_document={
                "type": "standalone",
                "token": _ref("artifacts.candidate.token"),
            },
        )
    )

    serialized = outcome.to_json()
    assert outcome.status is OutcomeStatus.ERROR
    assert sentinel not in serialized
    assert sentinel not in outcome.summary
    assert '"message"' not in serialized


def test_retained_bound_views_expire_after_environment_cleanup(tmp_path):
    sentinel = "SUPER_SECRET_RETAINED_BOUND_VALUE"
    retained = {}

    class ArtifactProvider:
        def check_support(self, context, spec, resolved_config):
            return SupportDecision.allow()

        def resolve(self, context, spec, resolved_config):
            return ResolvedArtifact(
                spec.artifact_id,
                spec.provider_id,
                {"token": sentinel},
                {},
                services={"client": object()},
            )

        def rollback(self, context, spec):
            return None

        def cleanup(self, context, spec, artifact):
            return None

    class TopologyProvider:
        def validate(self, plan):
            retained["validation_plan"] = plan
            retained["validation_topology"] = plan.topology_document

        def provision(self, context):
            retained["pre_topology_plan"] = context.plan
            retained["services"] = context.services
            retained["artifact_namespaces"] = context.services["artifacts"]
            return ProvisionedTopology(services={"client": object()})

        def rollback(self, context):
            return None

        def cleanup(self, context, topology):
            retained["topology_plan"] = context.plan
            retained["topology_services"] = context.services["topology"]
            assert context.plan.topology_document["token"] == sentinel

    class Action:
        def run(self, context, spec):
            retained["component"] = spec
            retained["component_config"] = spec.config
            retained["component_plan"] = context.plan
            assert spec.config["token"] == sentinel
            return ComponentResult(spec.instance_id, ComponentStatus.PASS, "passed")

    manager = _manager(
        tmp_path,
        artifact_registration=ArtifactProviderRegistration(
            "tests.artifact",
            ArtifactProvider,
            outputs=("token",),
            services=("client",),
        ),
    )
    outcome = _runner(
        tmp_path,
        topology_provider=TopologyProvider(),
        environment_manager=manager,
        registrations=(
            ComponentRegistration[Any](
                "tests.action",
                ComponentKind.ACTION,
                "v1",
                Action,
            ),
        ),
    ).run(
        _plan(
            components=(
                ComponentSpec(
                    ComponentKind.ACTION,
                    "tests.action",
                    "exercise",
                    config={"token": _ref("artifacts.candidate.token")},
                ),
            ),
            artifacts=(ArtifactSpec("candidate", "tests.artifact"),),
            topology_document={
                "type": "standalone",
                "token": _ref("artifacts.candidate.token"),
            },
        )
    )

    assert outcome.status is OutcomeStatus.PASS
    accesses = (
        lambda: retained["validation_plan"].topology_document,
        lambda: retained["validation_topology"]["token"],
        lambda: retained["pre_topology_plan"].services,
        lambda: retained["services"]["artifacts"],
        lambda: retained["artifact_namespaces"]["candidate"],
        lambda: retained["topology_plan"].topology_document,
        lambda: retained["topology_services"]["client"],
        lambda: retained["component"].config,
        lambda: retained["component_config"]["token"],
        lambda: retained["component_plan"].services,
    )
    for access in accesses:
        with pytest.raises(BoundPlanExpiredError):
            access()
    assert sentinel not in outcome.to_json()


def test_policy_only_plan_is_transport_not_core_policy_dispatch(tmp_path):
    seen = []

    class TopologyProvider:
        def validate(self, plan):
            assert isinstance(plan, RunPlan)
            assert isinstance(plan.policies[0], PolicySpec)
            seen.append(
                (
                    plan.policies[0].policy_id,
                    plan.policies[0].policy_type,
                    plan.policies[0].config["keep"],
                )
            )

        def provision(self, context):
            assert context.plan.policies[0].config == {"keep": 3}
            return ProvisionedTopology()

        def rollback(self, context):
            return None

        def cleanup(self, context, topology):
            return None

    outcome = _runner(
        tmp_path,
        topology_provider=TopologyProvider(),
    ).run(
        _plan(
            policies=(
                PolicySpec(
                    "retention",
                    "tests.retention",
                    config={"keep": 3},
                ),
            ),
        )
    )

    assert outcome.status is OutcomeStatus.PASS
    assert seen == [("retention", "tests.retention", 3)]
    assert outcome.diagnostics["environment_report"]["records"] == ()


@pytest.mark.parametrize(
    ("component_status", "expected_status"),
    (
        (ComponentStatus.PASS, OutcomeStatus.ERROR),
        (ComponentStatus.FAIL, OutcomeStatus.FAIL),
    ),
)
def test_environment_cleanup_failure_upgrades_success_but_preserves_trustworthy_fail(
    tmp_path,
    component_status,
    expected_status,
):
    cleanup_secret = "SECRET_CLEANUP_MESSAGE"
    cleanup_error = RuntimeError(cleanup_secret)

    class ArtifactProvider:
        def check_support(self, context, spec, resolved_config):
            return SupportDecision.allow()

        def resolve(self, context, spec, resolved_config):
            return ResolvedArtifact(
                spec.artifact_id,
                spec.provider_id,
                {},
                {},
            )

        def rollback(self, context, spec):
            return None

        def cleanup(self, context, spec, artifact):
            raise cleanup_error

    class TopologyProvider:
        def validate(self, plan):
            return None

        def provision(self, context):
            return ProvisionedTopology()

        def rollback(self, context):
            return None

        def cleanup(self, context, topology):
            return None

    class Action:
        def run(self, context, spec):
            return ComponentResult(
                spec.instance_id,
                component_status,
                "feature verdict",
            )

    manager = _manager(
        tmp_path,
        artifact_registration=ArtifactProviderRegistration(
            "tests.artifact",
            ArtifactProvider,
        ),
    )
    registration = ComponentRegistration[Any](
        "tests.action",
        ComponentKind.ACTION,
        "v1",
        Action,
    )
    outcome = _runner(
        tmp_path,
        topology_provider=TopologyProvider(),
        environment_manager=manager,
        registrations=(registration,),
    ).run(
        _plan(
            components=(
                ComponentSpec(
                    ComponentKind.ACTION,
                    "tests.action",
                    "exercise",
                ),
            ),
            artifacts=(ArtifactSpec("candidate", "tests.artifact"),),
        )
    )

    assert outcome.status is expected_status
    assert outcome.diagnostics["environment_report"]["status"] == "cleanup_failed"
    assert cleanup_secret not in outcome.to_json()


def test_component_keyboard_interrupt_unwinds_topology_then_environment(tmp_path):
    calls = []
    interrupt = KeyboardInterrupt("component interrupt")

    class ArtifactProvider:
        def check_support(self, context, spec, resolved_config):
            calls.append("artifact.support")
            return SupportDecision.allow()

        def resolve(self, context, spec, resolved_config):
            calls.append("artifact.acquire")
            return ResolvedArtifact(
                spec.artifact_id,
                spec.provider_id,
                {},
                {},
            )

        def rollback(self, context, spec):
            calls.append("artifact.rollback")

        def cleanup(self, context, spec, artifact):
            calls.append("artifact.cleanup")

    class TopologyProvider:
        def validate(self, plan):
            calls.append("topology.validate")

        def provision(self, context):
            calls.append("topology.provision")
            return ProvisionedTopology()

        def rollback(self, context):
            calls.append("topology.rollback")

        def cleanup(self, context, topology):
            calls.append("topology.cleanup")

    class Action:
        def run(self, context, spec):
            calls.append("action.run")
            raise interrupt

    manager = _manager(
        tmp_path,
        artifact_registration=ArtifactProviderRegistration(
            "tests.artifact",
            ArtifactProvider,
        ),
    )
    runner = _runner(
        tmp_path,
        topology_provider=TopologyProvider(),
        environment_manager=manager,
        registrations=(
            ComponentRegistration[Any](
                "tests.action",
                ComponentKind.ACTION,
                "v1",
                Action,
            ),
        ),
    )

    with pytest.raises(KeyboardInterrupt) as raised:
        runner.run(
            _plan(
                components=(
                    ComponentSpec(
                        ComponentKind.ACTION,
                        "tests.action",
                        "exercise",
                    ),
                ),
                artifacts=(ArtifactSpec("candidate", "tests.artifact"),),
            )
        )

    assert raised.value is interrupt
    assert calls == [
        "topology.validate",
        "artifact.support",
        "artifact.acquire",
        "topology.provision",
        "action.run",
        "topology.cleanup",
        "artifact.cleanup",
    ]
    assert interrupt.environment_report.status.value == "cleaned"


def test_environment_keyboard_interrupt_unwinds_prior_acquisitions_and_keeps_identity(
    tmp_path,
):
    calls = []
    interrupt = KeyboardInterrupt("environment interrupt")

    class ArtifactProvider:
        def check_support(self, context, spec, resolved_config):
            calls.append("artifact.support")
            return SupportDecision.allow()

        def resolve(self, context, spec, resolved_config):
            calls.append("artifact.acquire")
            return ResolvedArtifact(
                spec.artifact_id,
                spec.provider_id,
                {},
                {},
            )

        def rollback(self, context, spec):
            calls.append("artifact.rollback")

        def cleanup(self, context, spec, artifact):
            calls.append("artifact.cleanup")

    class ResourceProvider:
        def check_support(self, context, spec, resolved_config):
            calls.append("resource.support")
            raise interrupt

        def provision(self, context, spec, resolved_config):
            pytest.fail("resource acquisition must not run")

        def rollback(self, context, spec):
            calls.append("resource.rollback")

        def cleanup(self, context, spec, resource):
            calls.append("resource.cleanup")

    class TopologyProvider:
        def validate(self, plan):
            calls.append("topology.validate")

        def provision(self, context):
            pytest.fail("topology must not be reached")

        def rollback(self, context):
            pytest.fail("topology must not be reached")

        def cleanup(self, context, topology):
            pytest.fail("topology must not be reached")

    manager = _manager(
        tmp_path,
        artifact_registration=ArtifactProviderRegistration(
            "tests.artifact",
            ArtifactProvider,
        ),
        resource_registration=ResourceProviderRegistration(
            "tests.resource",
            ResourceProvider,
        ),
    )

    with pytest.raises(KeyboardInterrupt) as raised:
        _runner(
            tmp_path,
            topology_provider=TopologyProvider(),
            environment_manager=manager,
        ).run(
            _plan(
                artifacts=(ArtifactSpec("candidate", "tests.artifact"),),
                resources=(
                    ResourceSpec(
                        "store",
                        "tests.resource",
                        depends_on=("artifacts.candidate",),
                    ),
                ),
            )
        )

    assert raised.value is interrupt
    assert calls == [
        "topology.validate",
        "artifact.support",
        "artifact.acquire",
        "resource.support",
        "artifact.cleanup",
    ]
    assert interrupt.environment_report.status.value == "failed"


def test_environment_rollback_interrupt_preempts_ordinary_acquisition_error(
    tmp_path,
):
    primary = RuntimeError("ordinary acquisition failure")
    interrupt = KeyboardInterrupt("rollback interrupt")

    class ArtifactProvider:
        def check_support(self, context, spec, resolved_config):
            return SupportDecision.allow()

        def resolve(self, context, spec, resolved_config):
            raise primary

        def rollback(self, context, spec):
            raise interrupt

        def cleanup(self, context, spec, artifact):
            pytest.fail("failed acquisition must use rollback")

    class TopologyProvider:
        def validate(self, plan):
            return None

        def provision(self, context):
            pytest.fail("topology must not be reached")

        def rollback(self, context):
            pytest.fail("topology must not be reached")

        def cleanup(self, context, topology):
            pytest.fail("topology must not be reached")

    manager = _manager(
        tmp_path,
        artifact_registration=ArtifactProviderRegistration(
            "tests.artifact",
            ArtifactProvider,
        ),
    )

    with pytest.raises(KeyboardInterrupt) as raised:
        _runner(
            tmp_path,
            topology_provider=TopologyProvider(),
            environment_manager=manager,
        ).run(
            _plan(
                artifacts=(ArtifactSpec("candidate", "tests.artifact"),),
            )
        )

    assert raised.value is interrupt
    assert raised.value.__cause__ is primary
    assert interrupt.environment_report.status.value == "cleanup_failed"
    assert interrupt.environment_cleanup_failures[0].error is interrupt


def test_default_artifact_root_is_absolute_scoped_and_deterministic():
    root = normalize_artifact_root("")

    assert os.path.isabs(root)
    assert root == os.path.normpath(os.path.join(tempfile.gettempdir(), "valkey-fuzzer", "artifacts"))
