"""The extension contracts: immutable plans, the event bus, registries, and plugin discovery."""

import json
from dataclasses import FrozenInstanceError

import pytest

from tests.support import component_registration, deployment_identity
from valkey_fuzzer._data import (
    DuplicateRegistrationError,
    FrozenRegistryError,
    RegistrationError,
)
from valkey_fuzzer.catalog import ExtensionCatalog
from valkey_fuzzer.contracts import (
    ComponentRole,
    ComponentSpec,
    DeploymentProviderRegistration,
    EvidenceRequirement,
    ExecutionPlan,
    ExperimentSpec,
    InstanceSelector,
    PluginManifest,
    RunMode,
)
from valkey_fuzzer.environment import ArtifactSpec, PolicySpec
from valkey_fuzzer.events import EventBus, EventBusQuiescedError, EventDispatchAbortedError, EventDispatchError


def _component(component_id="activate", *, use="example.action.fake", role=ComponentRole.ACTION, **fields):
    return ComponentSpec(role=role, use=use, id=component_id, **fields)


ORACLE = _component("validate", use="example.oracle.fake", role=ComponentRole.ORACLE)


def _experiment(*components, mode=RunMode.SMOKE, **fields):
    components = components or (_component(),)
    return ExperimentSpec(
        experiment_id="example.feature", mode=mode, deployment_modes=("standalone",), components=components, **fields
    )


def _staged(role, dependency_role):
    """An experiment whose ``role`` component depends on a ``dependency_role`` one."""

    dependency = _component("dependency", use=f"example.{dependency_role.value}.fake", role=dependency_role)
    dependent = _component("dependent", use=f"example.{role.value}.fake", role=role, depends_on=("dependency",))
    return _experiment(dependency, dependent)


def _plan(experiment=None, **fields):
    fields = {"scenario_id": "s", "seed": 1, "deployment_mode": "standalone", **fields}
    return ExecutionPlan.from_experiment(
        experiment or _experiment(), deployment_provider=deployment_identity(), **fields
    )


def test_plans_take_deep_immutable_snapshots_and_round_trip_through_json():
    config = {"nested": {"items": [1, 2]}}
    component = _component(config=config)
    requirement = EvidenceRequirement("example.path", source="activate", min_count=2)
    plan = _plan(_experiment(component, ORACLE, mode=RunMode.EXPERIMENT, required_evidence=(requirement,)))
    config["nested"]["items"].append(3)

    assert component.config["nested"]["items"] == (1, 2)
    with pytest.raises(TypeError):
        component.config["new"] = True
    with pytest.raises(FrozenInstanceError):
        plan.seed = 7
    assert ExecutionPlan.from_data(json.loads(plan.to_json())) == plan


@pytest.mark.parametrize(
    ("build", "error", "message"),
    [
        (lambda: _experiment(_component(depends_on=("missing",))), ValueError, "unknown dependencies"),
        (lambda: _experiment(_component("a", depends_on=("b",)), _component("b", use="x.action.b", depends_on=("a",))), ValueError, "cycle"),
        (lambda: _plan(_experiment(mode=RunMode.EXPERIMENT)), ValueError, "required oracle"),
        (lambda: _plan(_experiment(ORACLE, mode=RunMode.EXPERIMENT)), ValueError, "required_evidence"),
        (lambda: EvidenceRequirement("example.path", min_count=0), ValueError, "greater than or equal"),
        (lambda: _experiment(ORACLE, required_evidence=(EvidenceRequirement("x.y", source="missing"),)), ValueError, "unknown source"),
        (lambda: _plan(inputs=[]), TypeError, "inputs must be a mapping"),
        (lambda: _component(config={"bad": object()}), TypeError, "JSON/YAML-safe"),
        (lambda: _component(config={"bad": float("nan")}), ValueError, "NaN"),
        (lambda: _component(timeout_s="soon"), ValueError, "timeout_s"),
        (lambda: _component(depends_on={"seed"}), TypeError, "ordered sequence"),
        (lambda: ExecutionPlan.from_data({**_plan().to_data(), "components": set()}), TypeError, "ordered sequence"),
        (lambda: ExecutionPlan.from_data({**_plan().to_data(), "deployment_provider": None}), (TypeError, ValueError), "deployment provider"),
        (lambda: ExecutionPlan.from_data({**_plan().to_data(), "retries": 3}), ValueError, "unknown fields"),
        (lambda: _plan(deployment=[]), TypeError, "deployment must be a mapping"),
        # The stage rule holds for every plan, so compiling (a batch matrix, spec translate) rejects what the runner would.
        (lambda: _plan(_staged(ComponentRole.CAPABILITY, ComponentRole.ACTION)), ValueError, "capability 'dependent' cannot depend on post-provision"),
        (lambda: _plan(_staged(ComponentRole.CAPABILITY, ComponentRole.ORACLE)), ValueError, "capability 'dependent' cannot depend on post-provision"),
        (lambda: _plan(_staged(ComponentRole.FAULT, ComponentRole.ORACLE)), ValueError, "'dependent' cannot depend on post-execution oracle"),
        (lambda: _plan(artifacts=(ArtifactSpec("b", "x.artifact"),) * 2), ValueError, "artifact_id contains duplicates"),
        (lambda: _plan(policies=(PolicySpec("p", "x.policy"),) * 2), ValueError, "policy_id contains duplicates"),
        (lambda: _plan().rng("missing"), ValueError, "unknown component id"),
        (lambda: _experiment(deployment_requirements={"mode": {}}), ValueError, "must name a deployment field"),
        (lambda: InstanceSelector.parse(["valkey-9"]).resolve(["valkey-0"], {}), ValueError, r"unknown instance IDs \['valkey-9'\]"),
        (lambda: InstanceSelector.parse([1]), ValueError, "an instance selector is one of"),
        # A manifest's entries are checked when the catalog registers them.
        (lambda: ExtensionCatalog().register_plugins((PluginManifest("x.p", deployment_providers=(_deployment(),) * 2),)), DuplicateRegistrationError, "standalone"),
    ],
)  # fmt: skip
def test_a_malformed_spec_or_plan_is_rejected(build, error, message):
    with pytest.raises(error, match=message):
        build()


def test_the_event_bus_orders_subscribers_snapshots_payloads_and_commits_only_dispatched_events():
    bus = EventBus(clock=lambda: 1.0)
    calls = []
    bus.subscribe("*", lambda event: calls.append(("all", event.name)))
    bus.subscribe("action.started", lambda event: bus.publish("action.completed", source="action-1"))
    bus.subscribe("action.started", lambda event: calls.append(("second", event.name)))
    payload = {"target": ["valkey-1"]}
    started = bus.publish("action.started", source="runner", payload=payload)
    payload["target"].append("valkey-2")

    # A nested publish dispatches after the event that triggered it.
    assert calls == [("all", "action.started"), ("second", "action.started"), ("all", "action.completed")]
    assert started.payload["target"] == ("valkey-1",)

    failing = bus.subscribe("deployment.ready", lambda event: (_ for _ in ()).throw(RuntimeError("observer failed")))
    with pytest.raises(EventDispatchError, match="observer failed") as error:
        bus.publish("deployment.ready", source="runner")
    assert error.value.subscription == failing
    assert [event.name for event in bus.committed_history()] == ["action.started", "action.completed"]
    # Once a dispatch is given up on, nothing more is published.
    assert bus.abort(RuntimeError("gave up"))
    with pytest.raises(EventDispatchAbortedError, match="gave up"):
        bus.publish("later.event", source="runner")
    bus.quiesce()
    with pytest.raises(EventBusQuiescedError):
        bus.publish("late.event", source="late")
    with pytest.raises(EventBusQuiescedError):
        bus.subscribe("late.event", calls.append)


def _deployment(provider_id="example.deployment.fake"):
    return DeploymentProviderRegistration(deployment_mode="standalone", provider_id=provider_id, factory=object)


@pytest.mark.parametrize(
    ("existing", "plugins", "error"),
    [
        (None, [PluginManifest("x.p", (component_registration("example.oracle_check", role=ComponentRole.ORACLE),))], RegistrationError),
        ("frozen", [PluginManifest("x.p", (component_registration(),), experiment_fragments={"x.base": {}})], FrozenRegistryError),
        ("action", [PluginManifest("x.p", (component_registration("new.oracle.fake"), component_registration()))], DuplicateRegistrationError),
        ("frozen-providers", [PluginManifest("x.p", (component_registration(),), deployment_providers=(_deployment(),))], FrozenRegistryError),
        (None, [PluginManifest("x.a", (component_registration(),), experiment_fragments={"x.base": {}}), PluginManifest("x.b", experiment_fragments={"x.base": {}})], RegistrationError),
        (None, [PluginManifest("x.a", deployment_providers=(_deployment(),)), PluginManifest("x.b", (component_registration(),), deployment_providers=(_deployment(),))], DuplicateRegistrationError),
        (None, [PluginManifest("x.a"), PluginManifest("x.a")], RegistrationError),
    ],
    ids=["misnamed-component", "frozen-catalog", "duplicate-component", "frozen-provider-registry", "duplicate-fragment", "duplicate-provider", "duplicate-plugin-id"],
)  # fmt: skip
def test_a_rejected_plugin_set_registers_nothing(existing, plugins, error):
    catalog = ExtensionCatalog()
    if existing == "frozen":
        catalog.freeze()
    elif existing == "frozen-providers":
        catalog.deployment_providers.freeze()
    elif existing == "action":
        catalog.components.register(component_registration())

    with pytest.raises(error):
        catalog.register_plugins(plugins)

    assert len(catalog.components) == (existing == "action")
    assert catalog.plugins == () and dict(catalog.fragments) == {}
