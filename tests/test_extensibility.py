"""Focused tests for the isolated feature-extension contracts."""

import json
import threading
from collections.abc import Mapping
from dataclasses import FrozenInstanceError
from types import SimpleNamespace
from typing import Any, Optional

import pytest
import yaml

from valkey_fuzzer.extensibility import (
    Action,
    ComponentKind,
    ComponentRegistration,
    ComponentResult,
    ComponentSpec,
    ComponentStatus,
    DuplicateRegistrationError,
    EventBusQuiescedError,
    EventDispatchAbortedError,
    EventDispatchError,
    Evidence,
    EvidenceRequirement,
    FeaturePackSpec,
    IncompatibleRegistrationError,
    LifecycleEventBus,
    NodeSelector,
    OutcomeStatus,
    PluginDiscoveryError,
    PluginManifest,
    Registry,
    RunMode,
    RunOutcome,
    RunPlan,
    StartupOverlay,
    TopologyProviderIdentity,
    UnknownRegistrationError,
    discover_plugins,
    to_plain_data,
)


def _component(
    instance_id: str = "activate",
    *,
    component_id: str = "example.action",
    kind: ComponentKind = ComponentKind.ACTION,
    config: Optional[Mapping[str, Any]] = None,
    depends_on: tuple[str, ...] = (),
) -> ComponentSpec:
    return ComponentSpec(
        kind=kind,
        component_id=component_id,
        instance_id=instance_id,
        config=config or {},
        depends_on=depends_on,
    )


def _feature_pack(*components: ComponentSpec) -> FeaturePackSpec:
    return FeaturePackSpec(
        pack_id="example.feature",
        version="1.0",
        mode=RunMode.TOPOLOGY_SMOKE,
        supported_topologies=("standalone", "replication"),
        components=components or (_component(),),
        startup_overlays=(
            StartupOverlay(
                overlay_id="all-nodes",
                server_config={"feature-enabled": "yes"},
            ),
        ),
    )


def _registration(
    component_id: str = "example.action",
    *,
    kind: ComponentKind = ComponentKind.ACTION,
    api_version: str = "v1",
    factory=None,
) -> ComponentRegistration[Any]:
    resolved_factory = factory or (lambda: object())
    return ComponentRegistration(
        component_id=component_id,
        kind=kind,
        api_version=api_version,
        factory=resolved_factory,
    )


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


def test_specs_and_plans_take_deep_immutable_snapshots():
    mutable_config = {"nested": {"items": [1, 2]}}
    mutable_startup = {"feature-enabled": ["yes"]}
    component = _component(config=mutable_config)
    pack = FeaturePackSpec(
        pack_id="example.feature",
        version="1",
        mode=RunMode.TOPOLOGY_SMOKE,
        supported_topologies=["standalone"],
        components=[component],
        startup_overlays=[
            StartupOverlay(
                overlay_id="replicas",
                selector=NodeSelector(roles=("replica",)),
                server_config=mutable_startup,
                artifact="candidate",
            )
        ],
    )
    plan = RunPlan.from_feature_pack(
        pack,
        scenario_id="scenario-1",
        seed=42,
        topology="standalone",
        topology_provider=_topology_identity(),
        inputs={"sizes": [1, 2]},
    )

    mutable_config["nested"]["items"].append(3)
    mutable_startup["feature-enabled"].append("no")

    assert component.config["nested"]["items"] == (1, 2)
    assert pack.startup_overlays[0].server_config["feature-enabled"] == ("yes",)
    assert plan.startup_overlays == pack.startup_overlays
    assert plan.inputs["sizes"] == (1, 2)
    with pytest.raises(TypeError):
        component.config["new"] = True
    with pytest.raises(FrozenInstanceError):
        plan.seed = 7


def test_pack_and_plan_contracts_have_no_public_schema_generation_fields():
    pack = _feature_pack()
    plan = RunPlan.from_feature_pack(
        pack,
        scenario_id="scenario-1",
        seed=1,
        topology="standalone",
        topology_provider=_topology_identity(),
    )

    assert "api_version" not in FeaturePackSpec.__dataclass_fields__
    assert "api_version" not in RunPlan.__dataclass_fields__
    assert "scenario_api_version" not in RunPlan.__dataclass_fields__
    assert "api_version" not in plan.to_data()
    assert "scenario_api_version" not in plan.to_data()

    serialized = dict(plan.to_data())
    serialized["api_version"] = "legacy"
    with pytest.raises(ValueError, match="unknown fields"):
        RunPlan.from_data(serialized)


def test_run_plan_requires_a_replay_locked_topology_provider_identity():
    with pytest.raises(TypeError, match="topology_provider"):
        RunPlan(
            scenario_id="scenario-1",
            pack_id="example.feature",
            pack_version="1",
            seed=1,
            topology="standalone",
            components=(),
            mode=RunMode.TOPOLOGY_SMOKE,
        )

    with pytest.raises(TypeError, match="TopologyProviderIdentity"):
        RunPlan(
            scenario_id="scenario-1",
            pack_id="example.feature",
            pack_version="1",
            seed=1,
            topology="standalone",
            components=(),
            topology_provider=None,
            mode=RunMode.TOPOLOGY_SMOKE,
        )

    plan = RunPlan.from_feature_pack(
        _feature_pack(),
        scenario_id="scenario-1",
        seed=1,
        topology="standalone",
        topology_provider=_topology_identity(),
    )
    serialized = dict(plan.to_data())
    del serialized["topology_provider"]
    with pytest.raises(
        ValueError,
        match=r"missing required fields.*topology_provider",
    ):
        RunPlan.from_data(serialized)


@pytest.mark.parametrize(
    ("field_name", "value"),
    (
        ("activation", {"mode": "concurrent"}),
        ("when_event", "action.started"),
    ),
)
def test_durable_component_data_rejects_removed_scheduler_fields(field_name, value):
    plan = RunPlan.from_feature_pack(
        _feature_pack(),
        scenario_id="scenario-1",
        seed=1,
        topology="standalone",
        topology_provider=_topology_identity(),
    )
    serialized = plan.to_data()
    component = serialized["components"][0]

    assert "activation" not in component
    assert "when_event" not in component
    assert RunPlan.from_data(serialized) == plan

    invalid = dict(serialized)
    invalid["components"] = [dict(component)]
    invalid["components"][0][field_name] = value
    with pytest.raises(ValueError, match=r"component.*unknown fields"):
        RunPlan.from_data(invalid)


@pytest.mark.parametrize(
    "value_factory",
    (
        lambda: {"seed"},
        lambda: (item for item in ("seed",)),
        lambda: {"seed": True},
        lambda: "seed",
    ),
)
def test_immutable_sequence_fields_reject_unordered_and_arbitrary_iterables(value_factory):
    with pytest.raises(TypeError, match="ordered sequence"):
        ComponentSpec(
            kind=ComponentKind.ACTION,
            component_id="example.action",
            instance_id="activate",
            depends_on=value_factory(),
        )


@pytest.mark.parametrize(
    "value_factory",
    (
        lambda: set(),
        lambda: iter(()),
        lambda: {},
        lambda: "",
    ),
)
def test_model_deserialization_rejects_non_sequence_component_collections(value_factory):
    plan = RunPlan.from_feature_pack(
        _feature_pack(),
        scenario_id="scenario-1",
        seed=1,
        topology="standalone",
        topology_provider=_topology_identity(),
    )
    serialized = dict(plan.to_data())
    serialized["components"] = value_factory()

    with pytest.raises(TypeError, match="ordered sequence"):
        RunPlan.from_data(serialized)


def test_feature_pack_rejects_unknown_and_cyclic_component_dependencies():
    with pytest.raises(ValueError, match="unknown dependencies"):
        _feature_pack(_component(depends_on=("missing",)))

    first = _component("first", component_id="example.first", depends_on=("second",))
    second = _component("second", component_id="example.second", depends_on=("first",))
    with pytest.raises(ValueError, match="dependency cycle"):
        _feature_pack(first, second)


def test_run_plan_rejects_unsupported_topology():
    with pytest.raises(ValueError, match="not supported"):
        RunPlan.from_feature_pack(
            _feature_pack(),
            scenario_id="scenario-1",
            seed=1,
            topology="cluster",
            topology_provider=_topology_identity(),
        )


def test_evidence_results_and_outcome_are_immutable_and_aggregate_evidence():
    mutable_value = {"negotiated": ["lz4"]}
    evidence = Evidence(
        evidence_id="example.negotiated",
        source="observer-1",
        phase="action.completed",
        observed_at=2.0,
        event_sequence=3,
        value=mutable_value,
    )
    result = ComponentResult(
        component_instance_id="observer-1",
        status="PASS",
        summary="codec observed",
        evidence=[evidence],
        diagnostics={"samples": [1]},
    )
    outcome = RunOutcome(
        plan=RunPlan.from_feature_pack(
            _feature_pack(),
            scenario_id="scenario-1",
            seed=1,
            topology="standalone",
            topology_provider=_topology_identity(),
        ),
        status="PASS",
        summary="complete",
        component_results=[result],
    )
    mutable_value["negotiated"].append("none")

    assert result.status is ComponentStatus.PASS
    assert outcome.status is OutcomeStatus.PASS
    assert outcome.success
    assert evidence.value["negotiated"] == ("lz4",)
    assert outcome.all_evidence == (evidence,)
    plain_diagnostics = to_plain_data(result.diagnostics)
    assert plain_diagnostics == {"samples": [1]}
    assert json.loads(json.dumps(plain_diagnostics)) == plain_diagnostics
    assert yaml.safe_load(yaml.safe_dump(plain_diagnostics)) == plain_diagnostics


def test_extension_data_rejects_non_serializable_and_non_finite_values():
    with pytest.raises(TypeError, match="JSON/YAML-safe"):
        _component(config={"bad": object()})
    with pytest.raises(ValueError, match="NaN"):
        _component(config={"bad": float("nan")})
    with pytest.raises(ValueError, match="timeout_s"):
        ComponentSpec(
            kind=ComponentKind.ACTION,
            component_id="example.action",
            instance_id="activate",
            timeout_s="soon",
        )


def test_run_plan_does_not_coerce_wrong_falsy_container_types():
    with pytest.raises(TypeError, match="mapping"):
        RunPlan.from_feature_pack(
            _feature_pack(),
            scenario_id="scenario-1",
            seed=1,
            topology="standalone",
            topology_provider=_topology_identity(),
            inputs=[],
        )
    with pytest.raises(TypeError, match="topology_document"):
        RunPlan.from_feature_pack(
            _feature_pack(),
            scenario_id="scenario-1",
            seed=1,
            topology="standalone",
            topology_provider=_topology_identity(),
            topology_document=[],
        )


def test_startup_overlays_are_distinct_from_post_start_components():
    overlay = StartupOverlay(
        overlay_id="candidate-replicas",
        selector=NodeSelector(roles=("replica",), labels={"zone": "a"}),
        server_config={"repl-compression": "lz4"},
        artifact="candidate",
        conflict_policy="override",
    )
    pack = FeaturePackSpec(
        pack_id="example.compression",
        version="1",
        mode=RunMode.TOPOLOGY_SMOKE,
        supported_topologies=("replication",),
        components=(_component(),),
        startup_overlays=(overlay,),
    )

    assert pack.startup_overlays == (overlay,)
    assert overlay.selector.roles == ("replica",)
    assert overlay.selector.labels["zone"] == "a"
    assert "startup_config" not in ComponentSpec.__dataclass_fields__


def test_feature_mode_requires_a_required_oracle_and_explicit_evidence():
    with pytest.raises(ValueError, match="required oracle"):
        FeaturePackSpec(
            pack_id="example.feature",
            version="1",
            supported_topologies=("standalone",),
            components=(_component(),),
        )

    oracle = _component(
        "validate",
        component_id="example.oracle",
        kind=ComponentKind.ORACLE,
    )
    with pytest.raises(ValueError, match="required_evidence"):
        FeaturePackSpec(
            pack_id="example.feature",
            version="1",
            supported_topologies=("standalone",),
            components=(oracle,),
        )

    smoke = FeaturePackSpec(
        pack_id="example.smoke",
        version="1",
        mode=RunMode.TOPOLOGY_SMOKE,
        supported_topologies=("standalone",),
        components=(),
    )
    assert smoke.mode is RunMode.TOPOLOGY_SMOKE


def test_evidence_requirements_validate_and_roundtrip_with_run_mode():
    observer = _component(
        "observe",
        component_id="example.observer",
        kind=ComponentKind.OBSERVER,
    )
    oracle = _component(
        "validate",
        component_id="example.oracle",
        kind=ComponentKind.ORACLE,
        depends_on=("observe",),
    )
    requirement = EvidenceRequirement(
        evidence_id="example.path",
        source="observe",
        min_count=2,
    )
    pack = FeaturePackSpec(
        pack_id="example.feature",
        version="1",
        supported_topologies=("standalone",),
        components=(observer, oracle),
        required_evidence=(requirement,),
    )
    plan = RunPlan.from_feature_pack(
        pack,
        scenario_id="feature-run",
        seed=1,
        topology="standalone",
        topology_provider=_topology_identity(implementation_digest="sha256:topology-build"),
    )

    assert plan.mode is RunMode.FEATURE
    assert plan.required_evidence == (requirement,)
    replayed = RunPlan.from_json(plan.to_json())
    assert replayed == plan
    assert replayed.topology_provider == _topology_identity(implementation_digest="sha256:topology-build")

    with pytest.raises(ValueError, match="greater than or equal"):
        EvidenceRequirement(evidence_id="example.path", min_count=0)
    with pytest.raises(ValueError, match="unknown source"):
        FeaturePackSpec(
            pack_id="example.feature",
            version="1",
            supported_topologies=("standalone",),
            components=(observer, oracle),
            required_evidence=(EvidenceRequirement(evidence_id="example.path", source="missing"),),
        )


def test_event_bus_orders_events_and_subscribers_and_snapshots_payloads():
    times = iter((10.0, 11.0))
    bus = LifecycleEventBus(clock=lambda: next(times))
    calls = []

    bus.subscribe("*", lambda event: calls.append(("all", event.name, event.sequence)))

    def publish_nested(event):
        calls.append(("first", event.name, event.sequence))
        bus.publish("action.completed", source="action-1", payload={"result": ["ok"]})

    bus.subscribe("action.started", publish_nested)
    bus.subscribe("action.started", lambda event: calls.append(("second", event.name, event.sequence)))
    original_payload = {"target": ["node-1"]}
    started = bus.publish("action.started", source="runner", payload=original_payload)
    original_payload["target"].append("node-2")

    assert calls == [
        ("all", "action.started", 1),
        ("first", "action.started", 1),
        ("second", "action.started", 1),
        ("all", "action.completed", 2),
    ]
    assert started.timestamp == 10.0
    assert started.payload["target"] == ("node-1",)
    assert [event.sequence for event in bus.history()] == [1, 2]
    assert [event.sequence for event in bus.committed_history()] == [1, 2]
    assert bus.history("action.completed")[0].timestamp == 11.0


def test_event_bus_normalizes_hostile_names_before_subscription_routing():
    class ForgedName(str):
        def __new__(cls, value, forged_value):
            instance = super().__new__(cls, value)
            instance.forged_value = forged_value
            return instance

        def __hash__(self):
            return hash(self.forged_value)

        def __eq__(self, other):
            return other == self.forged_value

    bus = LifecycleEventBus(clock=lambda: 1.0)
    target_calls = []
    exact_subscription_calls = []
    bus.subscribe("target", lambda event: target_calls.append(event.name))
    subscription = bus.subscribe(
        ForgedName("actual", "*"),
        lambda event: exact_subscription_calls.append(event.name),
    )

    evil = bus.publish(
        ForgedName("evil", "target"),
        source=ForgedName("producer", "runner"),
    )
    actual = bus.publish("actual", source="producer")

    assert target_calls == []
    assert exact_subscription_calls == ["actual"]
    assert type(subscription.event_name) is str
    assert subscription.event_name == "actual"
    assert type(evil.name) is str
    assert type(evil.source) is str
    assert evil.name == "evil"
    assert evil.source == "producer"
    assert bus.attempted_history(ForgedName("evil", "target")) == (evil,)
    assert bus.committed_history() == (evil, actual)


def test_event_bus_unsubscribe_replay_and_dispatch_error():
    bus = LifecycleEventBus(clock=lambda: 1.0)
    calls = []
    subscription = bus.subscribe("topology.ready", lambda event: calls.append(event.sequence))
    bus.publish("topology.ready", source="runner")
    assert bus.unsubscribe(subscription)
    assert not bus.unsubscribe(subscription)
    bus.publish("topology.ready", source="runner")

    replayed = []
    bus.replay(lambda event: replayed.append(event.sequence), after_sequence=1)
    assert calls == [1]
    assert replayed == [2]

    def fail(_event):
        raise RuntimeError("observer failed")

    failing = bus.subscribe("action.started", fail)
    with pytest.raises(EventDispatchError, match="observer failed") as error:
        bus.publish("action.started", source="runner")
    assert error.value.subscription == failing
    assert isinstance(error.value.__cause__, RuntimeError)
    assert [event.name for event in bus.attempted_history()][-1] == "action.started"
    assert not bus.committed_history("action.started")


def test_event_bus_quiesce_freezes_publish_and_subscription():
    bus = LifecycleEventBus(clock=lambda: 1.0)
    bus.publish("topology.ready", source="runner")

    bus.quiesce()

    assert bus.quiesced
    assert tuple(event.name for event in bus.committed_history()) == ("topology.ready",)
    with pytest.raises(EventBusQuiescedError):
        bus.publish("late.event", source="late")
    with pytest.raises(EventBusQuiescedError):
        bus.subscribe("late.event", lambda _event: None)
    assert tuple(event.name for event in bus.attempted_history()) == ("topology.ready",)


def test_event_bus_concurrent_publisher_returns_only_after_its_event_dispatches():
    bus = LifecycleEventBus(clock=lambda: 1.0)
    first_handler_entered = threading.Event()
    release_first_handler = threading.Event()
    second_handler_ran = threading.Event()
    second_publisher_ready = threading.Event()
    results = {}

    def block_first(_event):
        first_handler_entered.set()
        assert release_first_handler.wait(timeout=5)

    bus.subscribe("first", block_first)
    bus.subscribe("second", lambda _event: second_handler_ran.set())

    def publish_first():
        try:
            results["first"] = bus.publish("first", source="first-publisher")
        except BaseException as exc:
            results["first"] = exc

    def publish_second():
        try:
            # Owning the shared RLock until publish enters makes the concurrency
            # boundary deterministic: after this signal, the dispatcher cannot
            # resume until this publisher has either queued or returned.
            with bus._lock:
                second_publisher_ready.set()
                event = bus.publish("second", source="second-publisher")
            results["second"] = (event, second_handler_ran.is_set())
        except BaseException as exc:
            results["second"] = exc

    first_thread = threading.Thread(target=publish_first)
    second_thread = threading.Thread(target=publish_second)
    first_thread.start()
    assert first_handler_entered.wait(timeout=5)
    second_thread.start()
    assert second_publisher_ready.wait(timeout=5)
    release_first_handler.set()

    first_thread.join(timeout=5)
    second_thread.join(timeout=5)
    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert not isinstance(results["first"], BaseException)
    second_event, returned_after_dispatch = results["second"]
    assert second_event.name == "second"
    assert returned_after_dispatch


def test_event_bus_propagates_failure_and_aborts_later_waiting_publishers():
    bus = LifecycleEventBus(clock=lambda: 1.0)
    first_handler_entered = threading.Event()
    release_first_handler = threading.Event()
    second_publisher_ready = threading.Event()
    third_publisher_ready = threading.Event()
    results = {}

    def block_first(_event):
        first_handler_entered.set()
        assert release_first_handler.wait(timeout=5)

    def fail_second(_event):
        raise RuntimeError("second event failed")

    bus.subscribe("first", block_first)
    bus.subscribe("second", fail_second)

    def publisher(name, ready=None):
        try:
            if ready is None:
                results[name] = bus.publish(name, source=f"{name}-publisher")
            else:
                # See the successful-concurrency test above. Starting these in
                # order while each owns the RLock guarantees FIFO queue order.
                with bus._lock:
                    ready.set()
                    results[name] = bus.publish(name, source=f"{name}-publisher")
        except BaseException as exc:
            results[name] = exc

    first_thread = threading.Thread(target=publisher, args=("first",))
    second_thread = threading.Thread(target=publisher, args=("second", second_publisher_ready))
    third_thread = threading.Thread(target=publisher, args=("third", third_publisher_ready))
    first_thread.start()
    assert first_handler_entered.wait(timeout=5)
    second_thread.start()
    assert second_publisher_ready.wait(timeout=5)
    third_thread.start()
    assert third_publisher_ready.wait(timeout=5)
    release_first_handler.set()

    for thread in (first_thread, second_thread, third_thread):
        thread.join(timeout=5)
        assert not thread.is_alive()

    assert isinstance(results["first"], EventDispatchError)
    assert results["first"].event.name == "second"
    assert isinstance(results["second"], EventDispatchError)
    assert results["second"].event.name == "second"
    assert isinstance(results["second"].cause, RuntimeError)
    assert isinstance(results["third"], EventDispatchAbortedError)
    assert results["third"].event.name == "third"
    assert isinstance(results["third"].cause, EventDispatchError)
    assert results["third"].cause.event.name == "second"
    assert [event.name for event in bus.history()] == ["first", "second", "third"]


def test_typed_registry_registers_creates_and_lists_deterministically():
    registry = Registry[dict](ComponentKind.ACTION)
    registry.register(_registration("zeta.action", factory=lambda: {"name": "zeta"}))
    registry.register(_registration("alpha.action", factory=lambda: {"name": "alpha"}))

    assert registry.registered_ids == ("alpha.action", "zeta.action")
    assert registry.create("alpha.action") == {"name": "alpha"}
    assert tuple(registry.snapshot()) == ("alpha.action", "zeta.action")


def test_registry_has_specific_duplicate_unknown_and_compatibility_errors():
    registry = Registry[Any](ComponentKind.ACTION)
    registry.register(_registration())

    with pytest.raises(DuplicateRegistrationError) as duplicate:
        registry.register(_registration())
    assert duplicate.value.component_id == "example.action"

    with pytest.raises(UnknownRegistrationError) as unknown:
        registry.resolve("missing.action")
    assert unknown.value.component_id == "missing.action"

    with pytest.raises(IncompatibleRegistrationError, match="kind"):
        Registry[Any](ComponentKind.ORACLE).register(_registration())
    with pytest.raises(IncompatibleRegistrationError, match="API"):
        Registry[Any](ComponentKind.ACTION, api_version="v2").register(_registration())


def test_registry_batch_is_atomic():
    registry = Registry[Any](ComponentKind.ACTION)
    with pytest.raises(DuplicateRegistrationError):
        registry.register_many(
            (
                _registration("example.first"),
                _registration("example.first"),
            )
        )
    assert len(registry) == 0


class _Distribution:
    def __init__(self, name: str):
        self.name = name


class _EntryPoint:
    def __init__(self, name: str, distribution: str, loaded: Any):
        self.name = name
        self.value = f"tests:{name}"
        self.dist = _Distribution(distribution)
        self._loaded = loaded
        self.load_calls = 0

    def load(self):
        self.load_calls += 1
        if isinstance(self._loaded, BaseException):
            raise self._loaded
        return self._loaded


class _SelectableEntryPoints:
    def __init__(self, entry_points):
        self.entry_points = entry_points
        self.selected_group = None

    def select(self, *, group):
        self.selected_group = group
        return self.entry_points


class _FeaturePackCatalog:
    def __init__(self):
        self.registration_lock = threading.RLock()
        self._packs = []

    def validate_many(self, feature_packs):
        tuple(feature_packs)

    def register_many(self, feature_packs):
        self._packs.extend(feature_packs)

    def snapshot(self):
        return tuple(self._packs)

    def restore_snapshot(self, snapshot):
        self._packs = list(snapshot)

    @property
    def registered(self):
        return tuple(self._packs)


def test_plugin_discovery_is_sorted_and_registers_manifests(monkeypatch):
    creation_order = []

    def manifest(plugin_id: str, component_id: str) -> PluginManifest:
        creation_order.append(plugin_id)
        return PluginManifest(
            plugin_id=plugin_id,
            api_version="v1",
            registrations=(_registration(component_id),),
            feature_packs=(_feature_pack(),) if plugin_id == "alpha.plugin" else (),
        )

    beta = _EntryPoint("beta.plugin", "z-distribution", lambda: manifest("beta.plugin", "beta.action"))
    alpha = _EntryPoint("alpha.plugin", "a-distribution", lambda: manifest("alpha.plugin", "alpha.action"))
    entry_points = _SelectableEntryPoints([beta, alpha])
    monkeypatch.setattr("valkey_fuzzer.extensibility.importlib_metadata.entry_points", lambda: entry_points)
    registry = Registry[Any](ComponentKind.ACTION)
    feature_pack_catalog = _FeaturePackCatalog()

    discovered = discover_plugins(
        {ComponentKind.ACTION: registry},
        feature_pack_catalog=feature_pack_catalog,
    )

    assert entry_points.selected_group == "valkey_fuzzer.plugins"
    assert [item.plugin_id for item in discovered] == ["alpha.plugin", "beta.plugin"]
    assert creation_order == ["alpha.plugin", "beta.plugin"]
    assert registry.registered_ids == ("alpha.action", "beta.action")
    assert discovered[0].feature_packs[0].pack_id == "example.feature"
    assert feature_pack_catalog.registered == discovered[0].feature_packs


def test_plugin_discovery_rejects_feature_packs_without_catalog(monkeypatch):
    plugin = PluginManifest(
        plugin_id="example.plugin",
        api_version="v1",
        registrations=(_registration(),),
        feature_packs=(_feature_pack(),),
    )
    monkeypatch.setattr(
        "valkey_fuzzer.extensibility.importlib_metadata.entry_points",
        lambda: _SelectableEntryPoints([_EntryPoint("example.plugin", "distribution", plugin)]),
    )
    registry = Registry[Any](ComponentKind.ACTION)

    with pytest.raises(PluginDiscoveryError, match="no feature_pack_catalog was provided") as error:
        discover_plugins({ComponentKind.ACTION: registry})

    assert error.value.entry_point == "example.plugin"
    assert len(registry) == 0


def test_plugin_load_failure_is_wrapped_and_leaves_registries_unchanged(monkeypatch):
    valid = _EntryPoint(
        "alpha.plugin",
        "a-distribution",
        PluginManifest(
            plugin_id="alpha.plugin",
            api_version="v1",
            registrations=(_registration("alpha.action"),),
        ),
    )
    broken = _EntryPoint("beta.plugin", "b-distribution", RuntimeError("cannot import dependency"))
    monkeypatch.setattr(
        "valkey_fuzzer.extensibility.importlib_metadata.entry_points",
        lambda: _SelectableEntryPoints([valid, broken]),
    )
    registry = Registry[Any](ComponentKind.ACTION)

    with pytest.raises(PluginDiscoveryError, match="cannot import dependency") as error:
        discover_plugins({ComponentKind.ACTION: registry})

    assert error.value.entry_point == "beta.plugin"
    assert error.value.distribution == "b-distribution"
    assert len(registry) == 0


def test_plugin_preflight_failure_leaves_all_registries_unchanged(monkeypatch):
    action_registry = Registry[Any](ComponentKind.ACTION)
    oracle_registry = Registry[Any](ComponentKind.ORACLE)
    action_registry.register(_registration("existing.action"))
    plugin = PluginManifest(
        plugin_id="example.plugin",
        api_version="v1",
        registrations=(
            _registration("new.oracle", kind=ComponentKind.ORACLE),
            _registration("existing.action"),
        ),
    )
    monkeypatch.setattr(
        "valkey_fuzzer.extensibility.importlib_metadata.entry_points",
        lambda: _SelectableEntryPoints([_EntryPoint("example.plugin", "distribution", plugin)]),
    )

    with pytest.raises(DuplicateRegistrationError):
        discover_plugins(
            {
                ComponentKind.ACTION: action_registry,
                ComponentKind.ORACLE: oracle_registry,
            }
        )

    assert action_registry.registered_ids == ("existing.action",)
    assert len(oracle_registry) == 0


def test_plugin_discovery_supports_python_39_mapping_entry_points(monkeypatch):
    plugin = PluginManifest(
        plugin_id="example.plugin",
        api_version="v1",
        registrations=(_registration(),),
    )
    monkeypatch.setattr(
        "valkey_fuzzer.extensibility.importlib_metadata.entry_points",
        lambda: {"valkey_fuzzer.plugins": [_EntryPoint("example.plugin", "distribution", plugin)]},
    )
    registry = Registry[Any](ComponentKind.ACTION)

    assert discover_plugins({ComponentKind.ACTION: registry}) == (plugin,)
    assert "example.action" in registry


class _Python39EntryPoint:
    """Python 3.9 entry points have no ``dist`` attribute."""

    def __init__(self, name: str, value: str, loaded: Any):
        self.name = name
        self.value = value
        self._loaded = loaded

    def load(self):
        return self._loaded


def test_plugin_discovery_dedupes_entry_points_seen_through_two_path_entries(monkeypatch):
    plugin = PluginManifest(
        plugin_id="example.plugin",
        api_version="v1",
        registrations=(_registration(),),
    )
    # A distribution visible through two sys.path entries is reported twice.
    monkeypatch.setattr(
        "valkey_fuzzer.extensibility.importlib_metadata.entry_points",
        lambda: {
            "valkey_fuzzer.plugins": [
                _Python39EntryPoint("example.plugin", "example_plugin:manifest", plugin),
                _Python39EntryPoint("example.plugin", "example_plugin:manifest", plugin),
            ]
        },
    )
    registry = Registry[Any](ComponentKind.ACTION)

    assert discover_plugins({ComponentKind.ACTION: registry}) == (plugin,)
    assert registry.registered_ids == ("example.action",)


def test_plugin_discovery_dedupes_by_distribution_and_entry_point_name(monkeypatch):
    plugin = PluginManifest(
        plugin_id="example.plugin",
        api_version="v1",
        registrations=(_registration(),),
    )
    monkeypatch.setattr(
        "valkey_fuzzer.extensibility.importlib_metadata.entry_points",
        lambda: _SelectableEntryPoints(
            [
                _EntryPoint("example.plugin", "Example_Plugin", plugin),
                _EntryPoint("example.plugin", "example-plugin", plugin),
            ]
        ),
    )
    registry = Registry[Any](ComponentKind.ACTION)

    assert discover_plugins({ComponentKind.ACTION: registry}) == (plugin,)


def test_plugin_discovery_still_rejects_one_name_from_two_distributions(monkeypatch):
    plugin = PluginManifest(
        plugin_id="example.plugin",
        api_version="v1",
        registrations=(_registration(),),
    )
    monkeypatch.setattr(
        "valkey_fuzzer.extensibility.importlib_metadata.entry_points",
        lambda: _SelectableEntryPoints(
            [
                _EntryPoint("example.plugin", "first-distribution", plugin),
                _EntryPoint("example.plugin", "second-distribution", plugin),
            ]
        ),
    )

    with pytest.raises(PluginDiscoveryError, match="duplicate plugin ID"):
        discover_plugins({ComponentKind.ACTION: Registry[Any](ComponentKind.ACTION)})


def test_action_protocol_is_runtime_checkable():
    class ExampleAction:
        def run(self, context, spec):
            return ComponentResult(
                component_instance_id=spec.instance_id,
                status=ComponentStatus.PASS,
                summary="done",
            )

    assert isinstance(ExampleAction(), Action)
    assert not isinstance(SimpleNamespace(), Action)
