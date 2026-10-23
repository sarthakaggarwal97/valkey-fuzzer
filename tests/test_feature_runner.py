"""Focused contract tests for the isolated generic feature runner."""

import time
from collections.abc import Mapping, Sequence
from typing import Any, Optional

import pytest

from valkey_fuzzer.extensibility import (
    ComponentKind,
    ComponentRegistration,
    ComponentResult,
    ComponentSpec,
    ComponentStatus,
    EventBusQuiescedError,
    EventDispatchError,
    Evidence,
    EvidenceRequirement,
    FeaturePackSpec,
    OutcomeStatus,
    PluginManifest,
    Registry,
    RunMode,
    RunPlan,
    TopologyProviderIdentity,
    TopologyProviderRegistration,
    TopologyProviderRegistry,
    discover_plugins,
)
from valkey_fuzzer.feature_packs import FeaturePackCatalog, compile_scenario
from valkey_fuzzer.feature_runner import (
    FeatureRunner,
    ProvisionedTopology,
    stable_topological_order,
)


def _result(
    spec: ComponentSpec,
    *,
    status: ComponentStatus = ComponentStatus.PASS,
    summary: str = "ok",
    evidence_suffix: Optional[str] = None,
) -> ComponentResult:
    evidence = ()
    if evidence_suffix is not None:
        evidence = (
            Evidence(
                evidence_id=f"tests.{evidence_suffix}",
                source=spec.instance_id,
                phase=f"{spec.kind.value}.completed",
                observed_at=time.monotonic(),
                value={"instance": spec.instance_id},
            ),
        )
    return ComponentResult(
        component_instance_id=spec.instance_id,
        status=status,
        summary=summary,
        evidence=evidence,
    )


class _Provider:
    def __init__(
        self,
        calls: list[str],
        topology: str,
        *,
        reject: bool = False,
        cleanup_error: Optional[BaseException] = None,
    ):
        self.calls = calls
        self.topology = topology
        self.reject = reject
        self.cleanup_error = cleanup_error
        self.provisioned: Optional[ProvisionedTopology] = None

    def validate(self, plan: RunPlan) -> None:
        self.calls.append(f"{self.topology}.validate")
        assert plan.topology == self.topology
        if self.reject:
            raise ValueError("provider-specific topology document is invalid")

    def provision(self, context) -> ProvisionedTopology:
        self.calls.append(f"{self.topology}.provision")
        assert tuple(context.services) == ("artifacts", "resources", "topology")
        assert all(not namespace for namespace in context.services.values())
        self.provisioned = ProvisionedTopology(
            handle=object(),
            services={"topology": self.topology, "client": object()},
        )
        return self.provisioned

    def rollback(self, context) -> None:
        self.calls.append(f"{self.topology}.rollback")
        assert all(not namespace for namespace in context.services.values())
        assert context.deadline is None

    def cleanup(self, context, topology: ProvisionedTopology) -> None:
        self.calls.append(f"{self.topology}.cleanup")
        assert topology is self.provisioned
        assert context.services["topology"] == topology.services
        assert context.deadline is None
        if self.cleanup_error is not None:
            raise self.cleanup_error


class _Probe:
    def __init__(
        self,
        calls: list[str],
        status: ComponentStatus = ComponentStatus.PASS,
    ):
        self.calls = calls
        self.status = status

    def probe(self, context, spec: ComponentSpec) -> ComponentResult:
        self.calls.append("capability.probe")
        assert all(not namespace for namespace in context.services.values())
        return _result(
            spec,
            status=self.status,
            summary="capability result",
            evidence_suffix="capability",
        )


class _Observer:
    def __init__(
        self,
        calls: list[str],
        *,
        start_error: Optional[BaseException] = None,
        stop_error: Optional[BaseException] = None,
    ):
        self.calls = calls
        self.start_error = start_error
        self.stop_error = stop_error

    def start(self, context, spec: ComponentSpec) -> ComponentResult:
        self.calls.append("observer.start")
        if self.start_error is not None:
            raise self.start_error
        assert context.services["topology"]
        return _result(spec, evidence_suffix="observer_start")

    def stop(self, context, spec: ComponentSpec) -> ComponentResult:
        self.calls.append("observer.stop")
        assert context.deadline is None
        if self.stop_error is not None:
            raise self.stop_error
        return _result(spec, evidence_suffix="observer_stop")


class _Workload:
    def __init__(
        self,
        calls: list[str],
        *,
        stop_error: Optional[BaseException] = None,
    ):
        self.calls = calls
        self.stop_error = stop_error

    def start(self, context, spec: ComponentSpec) -> ComponentResult:
        self.calls.append("workload.start")
        return _result(spec, evidence_suffix="workload_start")

    def stop(self, context, spec: ComponentSpec) -> ComponentResult:
        self.calls.append("workload.stop")
        assert context.deadline is None
        if self.stop_error is not None:
            raise self.stop_error
        return _result(spec, evidence_suffix="workload_stop")


class _Fault:
    def __init__(
        self,
        calls: list[str],
        *,
        cleanup_error: Optional[BaseException] = None,
    ):
        self.calls = calls
        self.cleanup_error = cleanup_error

    def arm(self, context, spec: ComponentSpec) -> ComponentResult:
        self.calls.append("fault.arm")
        return _result(spec, evidence_suffix="fault_arm")

    def cleanup(self, context, spec: ComponentSpec) -> ComponentResult:
        self.calls.append("fault.cleanup")
        assert context.deadline is None
        if self.cleanup_error is not None:
            raise self.cleanup_error
        return _result(spec, evidence_suffix="fault_cleanup")


class _Action:
    def __init__(
        self,
        calls: list[str],
        *,
        status: ComponentStatus = ComponentStatus.PASS,
        error: Optional[BaseException] = None,
        invalid_result: bool = False,
    ):
        self.calls = calls
        self.status = status
        self.error = error
        self.invalid_result = invalid_result

    def run(self, context, spec: ComponentSpec):
        self.calls.append(f"{spec.instance_id}.run")
        if self.error is not None:
            raise self.error
        if self.invalid_result:
            return object()
        return _result(
            spec,
            status=self.status,
            summary="action result",
            evidence_suffix=spec.instance_id,
        )


class _Oracle:
    def __init__(self, calls: list[str], seen_evidence: list[tuple[str, ...]]):
        self.calls = calls
        self.seen_evidence = seen_evidence

    def evaluate(
        self,
        context,
        spec: ComponentSpec,
        evidence: Sequence[Evidence],
    ) -> ComponentResult:
        self.calls.append("oracle.evaluate")
        self.seen_evidence.append(tuple(item.evidence_id for item in evidence))
        return _result(spec, evidence_suffix="oracle")


def _registration(
    kind: ComponentKind,
    component_id: str,
    factory,
    *,
    validator=None,
) -> ComponentRegistration[Any]:
    return ComponentRegistration(
        component_id=component_id,
        kind=kind,
        api_version="v1",
        factory=factory,
        validator=validator,
    )


def _registries(
    entries: Sequence[tuple[ComponentKind, str, Any]],
) -> Mapping[ComponentKind, Registry[Any]]:
    registries: dict[ComponentKind, Registry[Any]] = {}
    for kind, component_id, factory in entries:
        registry = registries.setdefault(kind, Registry[Any](kind))
        registry.register(_registration(kind, component_id, factory))
    return registries


def _topology_identity(
    *,
    api_version: str = "v1",
    implementation_digest: Optional[str] = None,
) -> TopologyProviderIdentity:
    return TopologyProviderIdentity(
        provider_id="tests.topology-provider",
        implementation_version="1",
        api_version=api_version,
        implementation_digest=implementation_digest,
    )


def _topology_providers(
    *,
    api_version: str = "v1",
    implementation_digest: Optional[str] = None,
    **factories,
) -> Mapping[str, TopologyProviderRegistration]:
    return {
        topology: TopologyProviderRegistration(
            topology=topology,
            provider_id="tests.topology-provider",
            implementation_version="1",
            api_version=api_version,
            factory=factory,
            implementation_digest=implementation_digest,
        )
        for topology, factory in factories.items()
    }


def _spec(
    kind: ComponentKind,
    instance_id: str,
    *,
    required: bool = True,
    depends_on: tuple[str, ...] = (),
) -> ComponentSpec:
    return ComponentSpec(
        kind=kind,
        component_id=f"tests.{kind.value}",
        instance_id=instance_id,
        required=required,
        depends_on=depends_on,
    )


def _error_identity(error):
    """Exception diagnostics minus the free-form message, which must be present."""
    assert error.get("message"), error
    return {key: value for key, value in error.items() if key != "message"}


def _plan(
    components: Sequence[ComponentSpec],
    *,
    topology: str = "standalone",
    required_events: tuple[str, ...] = (),
    required_evidence: tuple[EvidenceRequirement, ...] = (),
    mode: RunMode = RunMode.TOPOLOGY_SMOKE,
) -> RunPlan:
    pack = FeaturePackSpec(
        pack_id="tests.feature",
        version="1",
        mode=mode,
        supported_topologies=("standalone", "replication", "cluster"),
        components=tuple(components),
        required_events=required_events,
        required_evidence=required_evidence,
    )
    return RunPlan.from_feature_pack(
        pack,
        scenario_id="runner-test",
        seed=7,
        topology=topology,
        topology_provider=_topology_identity(),
        topology_document={"type": topology, "provider_option": "valid"},
    )


def _event_names(outcome) -> tuple[str, ...]:
    return tuple(event["name"] for event in outcome.diagnostics["events"])


def test_stable_topological_order_preserves_manifest_order_among_ready_components():
    components = (
        _spec(ComponentKind.ACTION, "last", depends_on=("first",)),
        _spec(ComponentKind.ACTION, "independent"),
        _spec(ComponentKind.ACTION, "first"),
    )

    assert tuple(item.instance_id for item in stable_topological_order(components)) == (
        "independent",
        "first",
        "last",
    )


def test_topology_mapping_rejects_a_singleton_provider_instance():
    calls: list[str] = []
    provider = _Provider(calls, "standalone")

    with pytest.raises(TypeError, match="TopologyProviderRegistration"):
        FeatureRunner({}, {"standalone": provider})

    assert calls == []


def test_topology_only_mapping_infers_a_non_default_extension_api():
    registrations = _topology_providers(
        api_version="v2",
        standalone=lambda: _Provider([], "standalone"),
    )

    runner = FeatureRunner({}, registrations)

    assert runner.topology_provider_registry.api_version == "v2"
    runner.validate(
        RunPlan.from_feature_pack(
            FeaturePackSpec(
                pack_id="tests.topology-only",
                version="1",
                mode=RunMode.TOPOLOGY_SMOKE,
                supported_topologies=("standalone",),
                components=(),
            ),
            scenario_id="topology-only",
            seed=1,
            topology="standalone",
            topology_provider=_topology_identity(api_version="v2"),
        )
    )


def test_topology_mapping_rejects_mixed_extension_apis():
    registrations = {
        "standalone": TopologyProviderRegistration(
            topology="standalone",
            provider_id="tests.standalone-provider",
            implementation_version="1",
            api_version="v1",
            factory=lambda: _Provider([], "standalone"),
        ),
        "cluster": TopologyProviderRegistration(
            topology="cluster",
            provider_id="tests.cluster-provider",
            implementation_version="1",
            api_version="v2",
            factory=lambda: _Provider([], "cluster"),
        ),
    }

    with pytest.raises(
        ValueError,
        match="topology provider registrations must use one extension API version",
    ):
        FeatureRunner({}, registrations)


def test_topology_factory_creates_fresh_lifecycle_owners_per_attempt():
    instances = []
    call_logs = []

    def factory():
        calls = []
        provider = _Provider(calls, "standalone")
        call_logs.append(calls)
        instances.append(provider)
        return provider

    runner = FeatureRunner(
        {},
        _topology_providers(standalone=factory),
    )
    plan = _plan(())

    runner.validate(plan)
    outcome = runner.run(plan)

    assert outcome.status is OutcomeStatus.PASS
    assert len(instances) == 2
    assert instances[0] is not instances[1]
    assert call_logs == [
        ["standalone.validate"],
        [
            "standalone.validate",
            "standalone.provision",
            "standalone.cleanup",
        ],
    ]


@pytest.mark.parametrize("topology", ("standalone", "replication", "cluster"))
def test_generic_lifecycle_is_identical_across_registered_topology_providers(topology):
    calls: list[str] = []
    seen_evidence: list[tuple[str, ...]] = []
    components = (
        _spec(ComponentKind.ACTION, "activate", depends_on=("inject",)),
        _spec(ComponentKind.ORACLE, "validate", depends_on=("activate",)),
        _spec(ComponentKind.FAULT, "inject", depends_on=("load",)),
        _spec(ComponentKind.WORKLOAD, "load", depends_on=("observe",)),
        _spec(ComponentKind.OBSERVER, "observe", depends_on=("supported",)),
        _spec(ComponentKind.CAPABILITY, "supported"),
    )
    registries = _registries(
        (
            (ComponentKind.CAPABILITY, "tests.capability", lambda: _Probe(calls)),
            (ComponentKind.OBSERVER, "tests.observer", lambda: _Observer(calls)),
            (ComponentKind.WORKLOAD, "tests.workload", lambda: _Workload(calls)),
            (ComponentKind.FAULT, "tests.fault", lambda: _Fault(calls)),
            (ComponentKind.ACTION, "tests.action", lambda: _Action(calls)),
            (
                ComponentKind.ORACLE,
                "tests.oracle",
                lambda: _Oracle(calls, seen_evidence),
            ),
        )
    )
    runner = FeatureRunner(
        registries,
        _topology_providers(
            standalone=lambda: _Provider(calls, "standalone"),
            replication=lambda: _Provider(calls, "replication"),
            cluster=lambda: _Provider(calls, "cluster"),
        ),
    )

    outcome = runner.run(
        _plan(
            components,
            topology=topology,
            required_events=("topology.ready", "action.completed", "validation.completed"),
            required_evidence=(
                EvidenceRequirement(
                    evidence_id="tests.activate",
                    source="activate",
                ),
            ),
            mode=RunMode.FEATURE,
        )
    )

    assert outcome.status is OutcomeStatus.PASS
    assert calls == [
        f"{topology}.validate",
        "capability.probe",
        f"{topology}.provision",
        "observer.start",
        "workload.start",
        "fault.arm",
        "activate.run",
        "fault.cleanup",
        "workload.stop",
        "observer.stop",
        "oracle.evaluate",
        f"{topology}.cleanup",
    ]
    assert tuple(result.component_instance_id for result in outcome.component_results) == (
        "supported",
        "observe",
        "load",
        "inject",
        "activate",
        "validate",
    )
    assert seen_evidence == [
        (
            "tests.capability",
            "tests.observer_start",
            "tests.workload_start",
            "tests.fault_arm",
            "tests.activate",
            "tests.fault_cleanup",
            "tests.workload_stop",
            "tests.observer_stop",
        )
    ]
    assert _event_names(outcome) == (
        "run.started",
        "capability.probing",
        "capability.completed",
        "topology.provisioning",
        "topology.ready",
        "observer.starting",
        "observer.started",
        "workload.starting",
        "workload.started",
        "fault.arming",
        "fault.started",
        "action.started",
        "action.completed",
        "fault.stopping",
        "fault.stopped",
        "workload.quiescing",
        "workload.quiesced",
        "observer.stopping",
        "observer.stopped",
        "oracle.started",
        "oracle.completed",
        "validation.completed",
        "topology.cleanup.started",
        "topology.cleanup.completed",
        "run.completed",
    )


def test_required_capability_skip_is_unsupported_without_provider_mutation():
    calls: list[str] = []
    provider = _Provider(calls, "standalone")
    runner = FeatureRunner(
        _registries(
            (
                (
                    ComponentKind.CAPABILITY,
                    "tests.capability",
                    lambda: _Probe(calls, ComponentStatus.SKIP),
                ),
            )
        ),
        _topology_providers(standalone=lambda: provider),
    )

    outcome = runner.run(_plan((_spec(ComponentKind.CAPABILITY, "supported"),)))

    assert outcome.status is OutcomeStatus.SKIP_UNSUPPORTED
    assert calls == ["standalone.validate", "capability.probe"]
    assert "topology.provisioning" not in _event_names(outcome)
    assert provider.provisioned is None


def test_every_component_factory_and_protocol_is_preflighted_before_provision():
    calls: list[str] = []

    class CheckingProvider(_Provider):
        def provision(self, context) -> ProvisionedTopology:
            assert calls == [
                "standalone.validate",
                "observer.factory",
                "action.factory",
            ]
            return super().provision(context)

    def observer_factory():
        calls.append("observer.factory")
        return _Observer(calls)

    def action_factory():
        calls.append("action.factory")
        return _Action(calls)

    outcome = FeatureRunner(
        _registries(
            (
                (ComponentKind.OBSERVER, "tests.observer", observer_factory),
                (ComponentKind.ACTION, "tests.action", action_factory),
            )
        ),
        _topology_providers(standalone=lambda: CheckingProvider(calls, "standalone")),
    ).run(
        _plan(
            (
                _spec(ComponentKind.OBSERVER, "observe"),
                _spec(ComponentKind.ACTION, "activate"),
            )
        )
    )

    assert outcome.status is OutcomeStatus.PASS


def test_optional_failure_is_nonblocking_until_a_required_dependent_needs_it():
    calls: list[str] = []
    provider = _Provider(calls, "standalone")
    entries = (
        (
            ComponentKind.ACTION,
            "tests.action",
            lambda: _Action(calls, status=ComponentStatus.FAIL),
        ),
    )
    optional = _spec(ComponentKind.ACTION, "optional", required=False)
    independent_plan = _plan((optional,))

    independent = FeatureRunner(
        _registries(entries),
        _topology_providers(standalone=lambda: provider),
    ).run(independent_plan)

    assert independent.status is OutcomeStatus.PASS

    calls.clear()
    provider = _Provider(calls, "standalone")
    dependent = ComponentSpec(
        kind=ComponentKind.ACTION,
        component_id="tests.action",
        instance_id="required",
        depends_on=("optional",),
    )
    blocked = FeatureRunner(
        _registries(entries),
        _topology_providers(standalone=lambda: provider),
    ).run(_plan((dependent, optional)))

    assert blocked.status is OutcomeStatus.FAIL
    results = {result.component_instance_id: result for result in blocked.component_results}
    assert results["optional"].status is ComponentStatus.FAIL
    assert results["required"].status is ComponentStatus.SKIP
    assert results["required"].diagnostics["blocked_by"] == ("dependency 'optional' is FAIL",)


def test_required_component_skip_is_error_not_unsupported_after_provision():
    calls: list[str] = []
    outcome = FeatureRunner(
        _registries(
            (
                (
                    ComponentKind.ACTION,
                    "tests.action",
                    lambda: _Action(calls, status=ComponentStatus.SKIP),
                ),
            )
        ),
        _topology_providers(standalone=lambda: _Provider(calls, "standalone")),
    ).run(_plan((_spec(ComponentKind.ACTION, "activate"),)))

    # A skipped action never exercised the transition, so there is no verdict.
    assert outcome.status is OutcomeStatus.ERROR
    assert outcome.diagnostics["untrusted_component_failures"] == ("activate",)
    assert "standalone.provision" in calls
    assert calls[-1] == "standalone.cleanup"


def test_missing_feature_event_fails_even_when_standard_action_event_exists():
    calls: list[str] = []
    outcome = FeatureRunner(
        _registries(((ComponentKind.ACTION, "tests.action", lambda: _Action(calls)),)),
        _topology_providers(standalone=lambda: _Provider(calls, "standalone")),
    ).run(
        _plan(
            (_spec(ComponentKind.ACTION, "activate"),),
            required_events=("feature.path_entered",),
        )
    )

    assert outcome.status is OutcomeStatus.FAIL
    assert "action.completed" in _event_names(outcome)
    assert outcome.diagnostics["missing_required_events"] == ("feature.path_entered",)


def test_action_exception_unwinds_long_lived_components_and_provider():
    calls: list[str] = []
    components = (
        _spec(ComponentKind.OBSERVER, "observe"),
        _spec(ComponentKind.ACTION, "activate", depends_on=("observe",)),
    )
    outcome = FeatureRunner(
        _registries(
            (
                (ComponentKind.OBSERVER, "tests.observer", lambda: _Observer(calls)),
                (
                    ComponentKind.ACTION,
                    "tests.action",
                    lambda: _Action(calls, error=RuntimeError("boom")),
                ),
            )
        ),
        _topology_providers(standalone=lambda: _Provider(calls, "standalone")),
    ).run(_plan(components))

    assert outcome.status is OutcomeStatus.ERROR
    assert calls == [
        "standalone.validate",
        "standalone.provision",
        "observer.start",
        "activate.run",
        "observer.stop",
        "standalone.cleanup",
    ]
    results = {result.component_instance_id: result for result in outcome.component_results}
    assert results["activate"].status is ComponentStatus.ERROR
    assert outcome.diagnostics["error"]["type"] == "RuntimeError"


def test_teardown_registered_before_start_is_attempted_when_start_raises():
    calls: list[str] = []
    outcome = FeatureRunner(
        _registries(
            (
                (
                    ComponentKind.OBSERVER,
                    "tests.observer",
                    lambda: _Observer(
                        calls,
                        start_error=RuntimeError("cannot start observer"),
                    ),
                ),
            )
        ),
        _topology_providers(standalone=lambda: _Provider(calls, "standalone")),
    ).run(_plan((_spec(ComponentKind.OBSERVER, "observe"),)))

    assert outcome.status is OutcomeStatus.ERROR
    assert calls[-3:] == [
        "observer.start",
        "observer.stop",
        "standalone.cleanup",
    ]


def test_all_cleanup_is_attempted_and_cleanup_error_does_not_mask_feature_fail():
    calls: list[str] = []
    provider = _Provider(
        calls,
        "standalone",
        cleanup_error=RuntimeError("provider cleanup failed"),
    )
    components = (
        _spec(ComponentKind.OBSERVER, "observe"),
        _spec(ComponentKind.WORKLOAD, "load", depends_on=("observe",)),
        _spec(ComponentKind.FAULT, "inject", depends_on=("load",)),
        _spec(ComponentKind.ACTION, "activate", depends_on=("inject",)),
    )
    outcome = FeatureRunner(
        _registries(
            (
                (
                    ComponentKind.OBSERVER,
                    "tests.observer",
                    lambda: _Observer(calls, stop_error=RuntimeError("observer stop failed")),
                ),
                (
                    ComponentKind.WORKLOAD,
                    "tests.workload",
                    lambda: _Workload(calls, stop_error=RuntimeError("workload stop failed")),
                ),
                (
                    ComponentKind.FAULT,
                    "tests.fault",
                    lambda: _Fault(calls, cleanup_error=RuntimeError("fault cleanup failed")),
                ),
                (
                    ComponentKind.ACTION,
                    "tests.action",
                    lambda: _Action(calls, status=ComponentStatus.FAIL),
                ),
            )
        ),
        _topology_providers(standalone=lambda: provider),
    ).run(_plan(components))

    assert outcome.status is OutcomeStatus.FAIL
    assert calls[-4:] == [
        "fault.cleanup",
        "workload.stop",
        "observer.stop",
        "standalone.cleanup",
    ]
    assert tuple(error["owner"] for error in outcome.diagnostics["cleanup_errors"]) == (
        "inject",
        "load",
        "observe",
        "topology",
    )


def test_cleanup_failure_after_success_is_error():
    calls: list[str] = []
    outcome = FeatureRunner(
        _registries(
            (
                (
                    ComponentKind.OBSERVER,
                    "tests.observer",
                    lambda: _Observer(calls, stop_error=RuntimeError("cannot stop")),
                ),
            )
        ),
        _topology_providers(standalone=lambda: _Provider(calls, "standalone")),
    ).run(_plan((_spec(ComponentKind.OBSERVER, "observe"),)))

    assert outcome.status is OutcomeStatus.ERROR
    assert outcome.summary == "cleanup failed before a trustworthy feature failure was established"


def test_keyboard_interrupt_runs_every_cleanup_then_reraises_original():
    calls: list[str] = []
    original = KeyboardInterrupt("stop now")
    components = (
        _spec(ComponentKind.OBSERVER, "observe"),
        _spec(ComponentKind.WORKLOAD, "load", depends_on=("observe",)),
        _spec(ComponentKind.ACTION, "activate", depends_on=("load",)),
    )
    runner = FeatureRunner(
        _registries(
            (
                (
                    ComponentKind.OBSERVER,
                    "tests.observer",
                    lambda: _Observer(calls, stop_error=RuntimeError("stop error")),
                ),
                (ComponentKind.WORKLOAD, "tests.workload", lambda: _Workload(calls)),
                (
                    ComponentKind.ACTION,
                    "tests.action",
                    lambda: _Action(calls, error=original),
                ),
            )
        ),
        _topology_providers(standalone=lambda: _Provider(calls, "standalone")),
    )

    with pytest.raises(KeyboardInterrupt) as caught:
        runner.run(_plan(components))

    assert caught.value is original
    assert calls[-3:] == [
        "workload.stop",
        "observer.stop",
        "standalone.cleanup",
    ]


@pytest.mark.parametrize(
    ("factory", "expected_phase"),
    [
        (lambda calls: lambda: object(), "preflight"),
        (lambda calls: lambda: _Action, "preflight"),
        (lambda calls: lambda: _Action(calls, invalid_result=True), "action.run"),
    ],
)
def test_bad_plugin_protocol_or_result_becomes_error(factory, expected_phase):
    calls: list[str] = []
    provider = _Provider(calls, "standalone")
    outcome = FeatureRunner(
        _registries(((ComponentKind.ACTION, "tests.action", factory(calls)),)),
        _topology_providers(standalone=lambda: provider),
    ).run(_plan((_spec(ComponentKind.ACTION, "activate"),)))

    assert outcome.status is OutcomeStatus.ERROR
    assert _error_identity(outcome.diagnostics["error"]) == {
        "type": "valkey_fuzzer.feature_runner.FeatureRunnerError",
        "phase": expected_phase,
    }
    if expected_phase == "preflight":
        assert provider.provisioned is None
    else:
        assert calls[-1] == "standalone.cleanup"


def test_provider_validation_rejects_topology_before_component_factory_or_mutation():
    calls: list[str] = []

    def component_factory():
        calls.append("component.factory")
        return _Action(calls)

    provider = _Provider(calls, "standalone", reject=True)
    outcome = FeatureRunner(
        _registries(((ComponentKind.ACTION, "tests.action", component_factory),)),
        _topology_providers(standalone=lambda: provider),
    ).run(_plan((_spec(ComponentKind.ACTION, "activate"),)))

    assert outcome.status is OutcomeStatus.ERROR
    assert calls == ["standalone.validate"]
    assert provider.provisioned is None


def test_registry_drift_during_factory_is_error_before_provider_mutation():
    calls: list[str] = []
    registry = Registry[Any](ComponentKind.ACTION)

    def replacement_factory():
        registry.unregister("tests.action")
        registry.register(
            _registration(
                ComponentKind.ACTION,
                "tests.action",
                lambda: _Action(calls),
            )
        )
        return _Action(calls)

    registry.register(
        _registration(
            ComponentKind.ACTION,
            "tests.action",
            replacement_factory,
        )
    )
    provider = _Provider(calls, "standalone")
    outcome = FeatureRunner(
        {ComponentKind.ACTION: registry},
        _topology_providers(standalone=lambda: provider),
    ).run(_plan((_spec(ComponentKind.ACTION, "activate"),)))

    assert outcome.status is OutcomeStatus.ERROR
    assert outcome.diagnostics["error"]["phase"] == "preflight"
    assert calls == ["standalone.validate"]
    assert provider.provisioned is None


def test_expired_cooperative_deadline_is_error_without_provision():
    calls: list[str] = []
    provider = _Provider(calls, "standalone")
    outcome = FeatureRunner(
        _registries(((ComponentKind.ACTION, "tests.action", lambda: _Action(calls)),)),
        _topology_providers(standalone=lambda: provider),
    ).run(
        _plan((_spec(ComponentKind.ACTION, "activate"),)),
        deadline=time.monotonic() - 1,
    )

    assert outcome.status is OutcomeStatus.ERROR
    assert _error_identity(outcome.diagnostics["error"]) == {
        "type": "valkey_fuzzer.feature_runner.CooperativeDeadlineError",
        "phase": "topology.provision",
    }
    assert provider.provisioned is None


def test_evidence_must_be_owned_by_the_component_that_returned_it():
    calls: list[str] = []

    class MisownedAction:
        def run(self, context, spec: ComponentSpec) -> ComponentResult:
            return ComponentResult(
                component_instance_id=spec.instance_id,
                status=ComponentStatus.PASS,
                summary="misowned evidence",
                evidence=(
                    Evidence(
                        evidence_id="tests.path",
                        source="another-component",
                        phase="action.completed",
                        observed_at=time.monotonic(),
                        value=True,
                    ),
                ),
            )

    outcome = FeatureRunner(
        _registries(
            (
                (
                    ComponentKind.ACTION,
                    "tests.action",
                    MisownedAction,
                ),
            )
        ),
        _topology_providers(standalone=lambda: _Provider(calls, "standalone")),
    ).run(_plan((_spec(ComponentKind.ACTION, "activate"),)))

    assert outcome.status is OutcomeStatus.ERROR
    assert outcome.diagnostics["error"]["phase"] == "action.run"
    assert calls[-1] == "standalone.cleanup"


def test_evidence_cannot_reference_an_attempted_but_uncommitted_event():
    calls: list[str] = []

    class UncommittedEventAction:
        def run(self, context, spec: ComponentSpec) -> ComponentResult:
            failed_sequences: list[int] = []

            def reject(event):
                failed_sequences.append(event.sequence)
                raise RuntimeError("subscriber rejected event")

            context.event_bus.subscribe("feature.attempt", reject)
            try:
                context.event_bus.publish("feature.attempt", source=spec.instance_id)
            except EventDispatchError:
                pass
            else:
                raise AssertionError("feature.attempt unexpectedly committed")
            return ComponentResult(
                component_instance_id=spec.instance_id,
                status=ComponentStatus.PASS,
                summary="references failed event",
                evidence=(
                    Evidence(
                        evidence_id="tests.path",
                        source=spec.instance_id,
                        phase="feature.attempt",
                        observed_at=time.monotonic(),
                        value=True,
                        event_sequence=failed_sequences[0],
                    ),
                ),
            )

    outcome = FeatureRunner(
        _registries(
            (
                (
                    ComponentKind.ACTION,
                    "tests.action",
                    UncommittedEventAction,
                ),
            )
        ),
        _topology_providers(standalone=lambda: _Provider(calls, "standalone")),
    ).run(_plan((_spec(ComponentKind.ACTION, "activate"),)))

    assert outcome.status is OutcomeStatus.ERROR
    assert outcome.diagnostics["error"]["phase"] == "action.run"
    attempted = [event for event in outcome.diagnostics["events"] if event["name"] == "feature.attempt"]
    committed = [event for event in outcome.diagnostics["committed_events"] if event["name"] == "feature.attempt"]
    assert len(attempted) == 1
    assert committed == []


def test_missing_required_evidence_count_fails_a_real_feature_run():
    calls: list[str] = []
    seen_evidence: list[tuple[str, ...]] = []
    components = (
        _spec(ComponentKind.ACTION, "activate"),
        _spec(ComponentKind.ORACLE, "validate", depends_on=("activate",)),
    )
    outcome = FeatureRunner(
        _registries(
            (
                (ComponentKind.ACTION, "tests.action", lambda: _Action(calls)),
                (
                    ComponentKind.ORACLE,
                    "tests.oracle",
                    lambda: _Oracle(calls, seen_evidence),
                ),
            )
        ),
        _topology_providers(standalone=lambda: _Provider(calls, "standalone")),
    ).run(
        _plan(
            components,
            mode=RunMode.FEATURE,
            required_evidence=(
                EvidenceRequirement(
                    evidence_id="tests.activate",
                    source="activate",
                    min_count=2,
                ),
            ),
        )
    )

    assert outcome.status is OutcomeStatus.FAIL
    assert outcome.diagnostics["missing_required_evidence"] == (
        {
            "evidence_id": "tests.activate",
            "source": "activate",
            "min_count": 2,
            "observed_count": 1,
        },
    )
    assert seen_evidence == [("tests.activate",)]


def test_runner_component_validator_rejects_before_factory_or_provision():
    calls: list[str] = []
    registry = Registry[Any](ComponentKind.ACTION)

    def validator(spec, plan):
        calls.append(f"validator:{spec.instance_id}:{plan.scenario_id}")
        raise ValueError("unsupported action configuration")

    def factory():
        calls.append("component.factory")
        return _Action(calls)

    registry.register(
        _registration(
            ComponentKind.ACTION,
            "tests.action",
            factory,
            validator=validator,
        )
    )
    outcome = FeatureRunner(
        {ComponentKind.ACTION: registry},
        _topology_providers(standalone=lambda: _Provider(calls, "standalone")),
    ).run(_plan((_spec(ComponentKind.ACTION, "activate"),)))

    assert outcome.status is OutcomeStatus.ERROR
    assert _error_identity(outcome.diagnostics["error"]) == {
        "type": "valkey_fuzzer.feature_runner.FeatureRunnerError",
        "phase": "static preflight",
    }
    assert calls == [
        "standalone.validate",
        "validator:activate:runner-test",
    ]


def test_component_method_signature_is_checked_before_provision():
    calls: list[str] = []

    class BadSignatureAction:
        def run(self, context):
            raise AssertionError("must not be invoked")

    outcome = FeatureRunner(
        _registries(
            (
                (
                    ComponentKind.ACTION,
                    "tests.action",
                    BadSignatureAction,
                ),
            )
        ),
        _topology_providers(standalone=lambda: _Provider(calls, "standalone")),
    ).run(_plan((_spec(ComponentKind.ACTION, "activate"),)))

    assert outcome.status is OutcomeStatus.ERROR
    assert outcome.diagnostics["error"]["phase"] == "preflight"
    assert calls == ["standalone.validate"]


def test_provider_method_signature_is_checked_before_validation_or_component_factory():
    calls: list[str] = []

    class BadSignatureProvider:
        def validate(self, plan):
            calls.append("provider.validate")

        def provision(self):
            raise AssertionError("must not be invoked")

        def rollback(self, context):
            return None

        def cleanup(self, context, topology):
            return None

    def component_factory():
        calls.append("component.factory")
        return _Action(calls)

    outcome = FeatureRunner(
        _registries(
            (
                (
                    ComponentKind.ACTION,
                    "tests.action",
                    component_factory,
                ),
            )
        ),
        _topology_providers(standalone=BadSignatureProvider),
    ).run(_plan((_spec(ComponentKind.ACTION, "activate"),)))

    assert outcome.status is OutcomeStatus.ERROR
    assert outcome.diagnostics["error"]["phase"] == "static preflight"
    assert calls == []


@pytest.mark.parametrize(
    ("capability_status", "expected_outcome"),
    (
        (ComponentStatus.SKIP, OutcomeStatus.SKIP_UNSUPPORTED),
        (ComponentStatus.FAIL, OutcomeStatus.ERROR),
    ),
)
def test_effective_required_capability_dependency_blocks_before_provision(
    capability_status,
    expected_outcome,
):
    calls: list[str] = []
    provider = _Provider(calls, "standalone")
    components = (
        _spec(ComponentKind.ACTION, "activate", depends_on=("supported",)),
        _spec(ComponentKind.CAPABILITY, "supported", required=False),
    )
    outcome = FeatureRunner(
        _registries(
            (
                (
                    ComponentKind.CAPABILITY,
                    "tests.capability",
                    lambda: _Probe(calls, capability_status),
                ),
                (ComponentKind.ACTION, "tests.action", lambda: _Action(calls)),
            )
        ),
        _topology_providers(standalone=lambda: provider),
    ).run(_plan(components))

    assert outcome.status is expected_outcome
    assert calls == ["standalone.validate", "capability.probe"]
    assert provider.provisioned is None


def test_independent_optional_capability_skip_is_nonblocking():
    calls: list[str] = []
    components = (
        _spec(ComponentKind.CAPABILITY, "supported", required=False),
        _spec(ComponentKind.ACTION, "activate"),
    )
    outcome = FeatureRunner(
        _registries(
            (
                (
                    ComponentKind.CAPABILITY,
                    "tests.capability",
                    lambda: _Probe(calls, ComponentStatus.SKIP),
                ),
                (ComponentKind.ACTION, "tests.action", lambda: _Action(calls)),
            )
        ),
        _topology_providers(standalone=lambda: _Provider(calls, "standalone")),
    ).run(_plan(components))

    assert outcome.status is OutcomeStatus.PASS
    assert calls == [
        "standalone.validate",
        "capability.probe",
        "standalone.provision",
        "activate.run",
        "standalone.cleanup",
    ]


@pytest.mark.parametrize("provision_behavior", ("raise", "invalid"))
def test_failed_or_invalid_provision_rolls_back_without_normal_cleanup(
    provision_behavior,
):
    calls: list[str] = []

    class BrokenProvisionProvider(_Provider):
        def provision(self, context):
            self.calls.append("standalone.provision")
            assert all(not namespace for namespace in context.services.values())
            if provision_behavior == "raise":
                raise RuntimeError("provision failed after partial mutation")
            return object()

    provider = BrokenProvisionProvider(calls, "standalone")
    outcome = FeatureRunner(
        _registries(((ComponentKind.ACTION, "tests.action", lambda: _Action(calls)),)),
        _topology_providers(standalone=lambda: provider),
    ).run(_plan((_spec(ComponentKind.ACTION, "activate"),)))

    assert outcome.status is OutcomeStatus.ERROR
    assert calls == [
        "standalone.validate",
        "standalone.provision",
        "standalone.rollback",
    ]
    assert "standalone.cleanup" not in calls
    assert "topology.rollback.completed" in _event_names(outcome)


def test_rollback_failure_is_recorded_without_masking_the_provision_error():
    calls: list[str] = []

    class BrokenRollbackProvider(_Provider):
        def provision(self, context):
            self.calls.append("standalone.provision")
            raise RuntimeError("primary provision error")

        def rollback(self, context):
            self.calls.append("standalone.rollback")
            assert all(not namespace for namespace in context.services.values())
            assert context.deadline is None
            raise RuntimeError("rollback also failed")

    outcome = FeatureRunner(
        _registries(((ComponentKind.ACTION, "tests.action", lambda: _Action(calls)),)),
        _topology_providers(standalone=lambda: BrokenRollbackProvider(calls, "standalone")),
    ).run(_plan((_spec(ComponentKind.ACTION, "activate"),)))

    assert outcome.status is OutcomeStatus.ERROR
    assert _error_identity(outcome.diagnostics["error"]) == {
        "type": "RuntimeError",
        "phase": "topology.provision",
    }
    assert outcome.diagnostics["cleanup_errors"][0]["phase"] == "rollback"


def test_provision_keyboard_interrupt_rolls_back_then_reraises_original():
    calls: list[str] = []
    original = KeyboardInterrupt("provision interrupted")

    class InterruptedProvider(_Provider):
        def provision(self, context):
            self.calls.append("standalone.provision")
            raise original

    runner = FeatureRunner(
        _registries(((ComponentKind.ACTION, "tests.action", lambda: _Action(calls)),)),
        _topology_providers(standalone=lambda: InterruptedProvider(calls, "standalone")),
    )

    with pytest.raises(KeyboardInterrupt) as caught:
        runner.run(_plan((_spec(ComponentKind.ACTION, "activate"),)))

    assert caught.value is original
    assert calls == [
        "standalone.validate",
        "standalone.provision",
        "standalone.rollback",
    ]


def test_cleanup_error_overrides_required_skip_without_a_real_feature_failure():
    calls: list[str] = []
    components = (
        _spec(ComponentKind.OBSERVER, "observe"),
        _spec(ComponentKind.ACTION, "activate", depends_on=("observe",)),
    )
    outcome = FeatureRunner(
        _registries(
            (
                (
                    ComponentKind.OBSERVER,
                    "tests.observer",
                    lambda: _Observer(calls, stop_error=RuntimeError("cannot stop")),
                ),
                (
                    ComponentKind.ACTION,
                    "tests.action",
                    lambda: _Action(calls, status=ComponentStatus.SKIP),
                ),
            )
        ),
        _topology_providers(standalone=lambda: _Provider(calls, "standalone")),
    ).run(_plan(components))

    assert outcome.status is OutcomeStatus.ERROR
    assert outcome.diagnostics["required_component_failures"] == ("activate",)
    assert outcome.diagnostics["cleanup_errors"][0]["owner"] == "observe"


def test_event_bus_is_quiesced_before_the_outcome_is_returned():
    calls: list[str] = []
    buses = []

    class CapturingAction:
        def run(self, context, spec: ComponentSpec) -> ComponentResult:
            buses.append(context.event_bus)
            return _result(spec)

    outcome = FeatureRunner(
        _registries(
            (
                (
                    ComponentKind.ACTION,
                    "tests.action",
                    CapturingAction,
                ),
            )
        ),
        _topology_providers(standalone=lambda: _Provider(calls, "standalone")),
    ).run(_plan((_spec(ComponentKind.ACTION, "activate"),)))

    assert outcome.status is OutcomeStatus.PASS
    assert buses[0].quiesced
    with pytest.raises(EventBusQuiescedError):
        buses[0].publish("feature.late", source="activate")


def test_required_capability_registry_drift_is_error_not_unsupported():
    calls: list[str] = []
    registry = Registry[Any](ComponentKind.CAPABILITY)

    class DriftingProbe:
        def probe(self, context, spec: ComponentSpec) -> ComponentResult:
            calls.append("capability.probe")

            def replace_registration(_event):
                registry.unregister("tests.capability")
                registry.register(
                    _registration(
                        ComponentKind.CAPABILITY,
                        "tests.capability",
                        lambda: _Probe(calls),
                    )
                )

            context.event_bus.subscribe(
                "capability.completed",
                replace_registration,
            )
            return _result(spec, status=ComponentStatus.SKIP)

    registry.register(
        _registration(
            ComponentKind.CAPABILITY,
            "tests.capability",
            DriftingProbe,
        )
    )
    outcome = FeatureRunner(
        {ComponentKind.CAPABILITY: registry},
        _topology_providers(standalone=lambda: _Provider(calls, "standalone")),
    ).run(_plan((_spec(ComponentKind.CAPABILITY, "supported"),)))

    assert outcome.status is OutcomeStatus.ERROR
    assert outcome.diagnostics["error"]["phase"] == "capability.probe"
    assert calls == ["standalone.validate", "capability.probe"]


class _Distribution:
    name = "external-distribution"


class _EntryPoint:
    name = "external.plugin"
    value = "external:plugin"
    dist = _Distribution()

    def __init__(self, manifest: PluginManifest):
        self.manifest = manifest

    def load(self):
        return self.manifest


class _EntryPoints:
    def __init__(self, entry_point: _EntryPoint):
        self.entry_point = entry_point

    def select(self, *, group: str):
        assert group == "valkey_fuzzer.plugins"
        return (self.entry_point,)


def test_external_plugin_compiles_and_executes_without_runner_changes(monkeypatch):
    calls: list[str] = []
    seen_evidence: list[tuple[str, ...]] = []
    pack = FeaturePackSpec(
        pack_id="external.feature",
        version="1",
        supported_topologies=("standalone",),
        components=(
            ComponentSpec(
                kind=ComponentKind.OBSERVER,
                component_id="external.observer",
                instance_id="observe",
            ),
            ComponentSpec(
                kind=ComponentKind.ACTION,
                component_id="external.action",
                instance_id="activate",
                depends_on=("observe",),
            ),
            ComponentSpec(
                kind=ComponentKind.ORACLE,
                component_id="external.oracle",
                instance_id="validate",
                depends_on=("activate",),
            ),
        ),
        required_events=("action.completed",),
        required_evidence=(
            EvidenceRequirement(
                evidence_id="tests.activate",
                source="activate",
            ),
        ),
    )
    manifest = PluginManifest(
        plugin_id="external.plugin",
        api_version="v1",
        registrations=(
            _registration(
                ComponentKind.OBSERVER,
                "external.observer",
                lambda: _Observer(calls),
            ),
            _registration(
                ComponentKind.ACTION,
                "external.action",
                lambda: _Action(calls),
            ),
            _registration(
                ComponentKind.ORACLE,
                "external.oracle",
                lambda: _Oracle(calls, seen_evidence),
            ),
        ),
        feature_packs=(pack,),
    )
    monkeypatch.setattr(
        "valkey_fuzzer.extensibility.importlib_metadata.entry_points",
        lambda: _EntryPoints(_EntryPoint(manifest)),
    )
    catalog = FeaturePackCatalog()
    action_registry = Registry[Any](ComponentKind.ACTION)
    observer_registry = Registry[Any](ComponentKind.OBSERVER)
    oracle_registry = Registry[Any](ComponentKind.ORACLE)
    registries = {
        ComponentKind.ACTION: action_registry,
        ComponentKind.OBSERVER: observer_registry,
        ComponentKind.ORACLE: oracle_registry,
    }
    discover_plugins(
        registries,
        feature_pack_catalog=catalog,
    )
    topology_providers = _topology_providers(standalone=lambda: _Provider(calls, "standalone"))
    topology_registry = TopologyProviderRegistry()
    topology_registry.register_many(topology_providers.values())
    plan = compile_scenario(
        {
            "scenario_id": "external-run",
            "seed": 1,
            "feature": {
                "use": "external.feature",
                "version": "1",
                "with": {},
            },
            "topology": {"type": "standalone"},
        },
        catalog,
        registries,
        topology_provider_registry=topology_registry,
    )

    outcome = FeatureRunner(
        registries,
        topology_registry,
    ).run(plan)

    assert outcome.status is OutcomeStatus.PASS
    assert "activate.run" in calls
    assert "oracle.evaluate" in calls
