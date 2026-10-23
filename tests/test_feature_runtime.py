"""Focused tests for the generic runtime bootstrap."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from typing import Any

import pytest

from valkey_fuzzer.extensibility import (
    ComponentKind,
    ComponentRegistration,
    ComponentResult,
    ComponentStatus,
    OutcomeStatus,
    PluginManifest,
    RunMode,
    RunPlan,
    TopologyProviderRegistration,
)
from valkey_fuzzer.feature_packs import (
    DuplicateFeaturePackError,
    FeaturePackDocumentError,
    load_feature_pack_yaml,
)
from valkey_fuzzer.feature_runner import ProvisionedTopology
from valkey_fuzzer.feature_runtime import FeatureRuntime, FeatureRuntimeError


def _manifest(pack_id: str = "example.feature") -> str:
    return f"""
pack_id: {pack_id}
version: "1"
mode: topology_smoke
supported_topologies: [standalone]
components:
  - kind: action
    component_id: example.action
    instance_id: activate
required_events: [action.completed]
"""


def _scenario(*, valkey_binary: str = "/original/valkey-server") -> str:
    return f"""
scenario_id: runtime-test
seed: 41
feature:
  use: example.feature
  version: "1"
  with: {{}}
topology:
  type: standalone
  valkey_binary: {valkey_binary}
  base_port: 7410
"""


class _PassingAction:
    def __init__(self, calls: list[str]):
        self._calls = calls

    def run(self, context, spec):
        self._calls.append("action.run")
        return ComponentResult(
            component_instance_id=spec.instance_id,
            status=ComponentStatus.PASS,
            summary="done",
        )


class _Provider:
    def __init__(self, calls: list[str]):
        self._calls = calls

    def validate(self, plan):
        self._calls.append(f"provider.validate:{plan.topology_document.get('valkey_binary')}")

    def provision(self, context):
        self._calls.append("provider.provision")
        return ProvisionedTopology()

    def rollback(self, context):
        self._calls.append("provider.rollback")

    def cleanup(self, context, topology):
        self._calls.append("provider.cleanup")


def _topology_providers(
    factory,
    *,
    topology: str = "standalone",
    api_version: str = "v1",
):
    registration = TopologyProviderRegistration(
        topology=topology,
        provider_id="tests.topology-provider",
        implementation_version="1",
        api_version=api_version,
        factory=factory,
    )
    return {topology: registration}


class _EntryPoint:
    def __init__(self, manifest: PluginManifest):
        self.name = manifest.plugin_id
        self.value = "tests:plugin"
        self.dist = SimpleNamespace(name="test-distribution")
        self._manifest = manifest

    def load(self):
        return self._manifest


class _EntryPoints:
    def __init__(self, entry_point: _EntryPoint):
        self._entry_point = entry_point
        self.selected_group = None

    def select(self, *, group):
        self.selected_group = group
        return (self._entry_point,)


def _register_action(runtime: FeatureRuntime, calls: list[str]) -> None:
    runtime.registries[ComponentKind.ACTION].register(
        ComponentRegistration[Any](
            component_id="example.action",
            kind=ComponentKind.ACTION,
            api_version="v1",
            factory=lambda: _PassingAction(calls),
        )
    )


def test_runtime_constructs_all_registries_and_uses_local_provider_factories(monkeypatch):
    def coordinator_factory():
        return object()

    provider = _Provider([])
    observed = {}

    def local_registrations(received_factory, *, api_version):
        observed["coordinator_factory"] = received_factory
        observed["api_version"] = api_version
        return _topology_providers(lambda: provider)

    monkeypatch.setattr(
        "valkey_fuzzer.feature_runtime.local_topology_provider_registrations",
        local_registrations,
    )
    monkeypatch.setattr(
        "valkey_fuzzer.extensibility.importlib_metadata.entry_points",
        lambda: pytest.fail("plugin discovery must be optional"),
    )

    runtime = FeatureRuntime(
        discover_entry_points=False,
        coordinator_factory=coordinator_factory,
    )

    assert set(runtime.registries) == set(ComponentKind)
    assert all(registry.kind is kind and registry.api_version == "v1" for kind, registry in runtime.registries.items())
    assert observed["coordinator_factory"] is coordinator_factory
    assert observed["api_version"] == "v1"
    assert runtime.topology_providers.registered_ids == ("standalone",)
    assert runtime.topology_providers.resolve("standalone").factory() is provider
    assert runtime.plugins == ()
    assert runtime.manifest_feature_packs == ()
    with pytest.raises(AttributeError):
        runtime.registries[ComponentKind.ACTION].kind = ComponentKind.ORACLE
    with pytest.raises(AttributeError):
        runtime.registries[ComponentKind.ACTION].api_version = "v999"
    assert not hasattr(runtime.catalog, "api_version")


def test_runtime_discovers_plugin_components_before_compiling_directory_manifest(
    tmp_path,
    monkeypatch,
):
    plugin_calls = []
    manifest = PluginManifest(
        plugin_id="example.plugin",
        api_version="v1",
        registrations=(
            ComponentRegistration[Any](
                component_id="example.action",
                kind=ComponentKind.ACTION,
                api_version="v1",
                factory=lambda: _PassingAction(plugin_calls),
            ),
        ),
    )
    entry_points = _EntryPoints(_EntryPoint(manifest))
    monkeypatch.setattr(
        "valkey_fuzzer.extensibility.importlib_metadata.entry_points",
        lambda: entry_points,
    )
    (tmp_path / "feature.yaml").write_text(_manifest(), encoding="utf-8")

    runtime = FeatureRuntime(
        tmp_path,
        topology_providers=_topology_providers(lambda: _Provider([])),
    )
    plan = runtime.compile_yaml(_scenario(), source_name="scenario.yaml")

    assert entry_points.selected_group == "valkey_fuzzer.plugins"
    assert runtime.plugins == (manifest,)
    assert runtime.catalog.registered_keys == (("example.feature", "1"),)
    assert runtime.manifest_feature_packs[0].pack_id == "example.feature"
    assert plan.pack_id == "example.feature"
    assert plan.components[0].component_id == "example.action"


def test_multi_directory_manifest_load_is_atomic_and_ordered(tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    (first / "b.yaml").write_text(_manifest("example.b"), encoding="utf-8")
    (first / "a.yaml").write_text(_manifest("example.a"), encoding="utf-8")
    (second / "duplicate.yaml").write_text(_manifest("example.a"), encoding="utf-8")
    runtime = FeatureRuntime(
        discover_entry_points=False,
        topology_providers=_topology_providers(lambda: _Provider([])),
    )

    with pytest.raises(DuplicateFeaturePackError):
        runtime.load_feature_pack_directories((first, second))

    assert runtime.catalog.registered_keys == ()
    assert runtime.manifest_feature_packs == ()

    (second / "duplicate.yaml").write_text(_manifest("example.c"), encoding="utf-8")
    loaded = runtime.load_feature_pack_directories((first, second))

    assert tuple(pack.pack_id for pack in loaded) == (
        "example.a",
        "example.b",
        "example.c",
    )
    assert runtime.catalog.registered_keys == (
        ("example.a", "1"),
        ("example.b", "1"),
        ("example.c", "1"),
    )


def test_manifest_staging_is_independent_of_runtime_extension_api(tmp_path):
    (tmp_path / "feature.yaml").write_text(_manifest(), encoding="utf-8")

    runtime = FeatureRuntime(
        tmp_path,
        discover_entry_points=False,
        topology_providers=_topology_providers(
            lambda: _Provider([]),
            api_version="v2",
        ),
        extension_api_version="v2",
    )

    assert all(registry.api_version == "v2" for registry in runtime.registries.values())
    assert not hasattr(runtime.catalog, "api_version")
    assert not hasattr(runtime.manifest_feature_packs[0], "api_version")


def test_manifest_failure_happens_before_plugin_discovery(tmp_path, monkeypatch):
    valid = tmp_path / "valid"
    invalid = tmp_path / "invalid"
    valid.mkdir()
    invalid.mkdir()
    (valid / "feature.yaml").write_text(_manifest(), encoding="utf-8")
    (invalid / "broken.yaml").write_text(
        """
pack_id: example.first
pack_id: example.second
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "valkey_fuzzer.extensibility.importlib_metadata.entry_points",
        lambda: pytest.fail("plugins must not load after a manifest failure"),
    )

    with pytest.raises(FeaturePackDocumentError, match="duplicate key"):
        FeatureRuntime(
            (valid, invalid),
            topology_providers=_topology_providers(lambda: _Provider([])),
        )


def test_validate_plan_applies_overrides_without_provisioning_or_component_creation(
    tmp_path,
):
    (tmp_path / "feature.yaml").write_text(_manifest(), encoding="utf-8")
    provider_calls = []
    provider_factory_calls = []
    component_factory_calls = []
    validator_calls = []

    def component_factory():
        component_factory_calls.append("created")
        raise AssertionError("validate_plan must not create component instances")

    def provider_factory():
        provider_factory_calls.append("created")
        return _Provider(provider_calls)

    runtime = FeatureRuntime(
        tmp_path,
        discover_entry_points=False,
        topology_providers=_topology_providers(provider_factory),
    )
    runtime.registries[ComponentKind.ACTION].register(
        ComponentRegistration[Any](
            component_id="example.action",
            kind=ComponentKind.ACTION,
            api_version="v1",
            factory=component_factory,
            validator=lambda spec, plan: validator_calls.append(plan.topology_document["valkey_binary"]),
        )
    )
    scenario = {
        "scenario_id": "runtime-test",
        "seed": 41,
        "feature": {
            "use": "example.feature",
            "version": "1",
            "with": {},
        },
        "topology": {
            "type": "standalone",
            "valkey_binary": "/original/valkey-server",
            "server_config": {"appendonly": "yes"},
        },
    }
    overrides = {
        "valkey_binary": "/override/valkey-server",
        "server_config": {"appendonly": "no"},
    }

    plan = runtime.compile(scenario, topology_overrides=overrides)
    runtime.validate_plan(plan)

    assert scenario["topology"] == {
        "type": "standalone",
        "valkey_binary": "/original/valkey-server",
        "server_config": {"appendonly": "yes"},
    }
    assert overrides == {
        "valkey_binary": "/override/valkey-server",
        "server_config": {"appendonly": "no"},
    }
    assert plan.topology_document["valkey_binary"] == "/override/valkey-server"
    assert plan.topology_document["server_config"] == {"appendonly": "no"}
    assert provider_factory_calls == ["created"]
    assert provider_calls == ["provider.validate:/override/valkey-server"]
    assert component_factory_calls == []
    assert validator_calls == [
        "/override/valkey-server",
        "/override/valkey-server",
    ]


def test_run_yaml_compiles_and_executes_through_generic_runner(tmp_path):
    (tmp_path / "feature.yaml").write_text(_manifest(), encoding="utf-8")
    calls = []
    runtime = FeatureRuntime(
        tmp_path,
        discover_entry_points=False,
        topology_providers=_topology_providers(lambda: _Provider(calls)),
    )
    _register_action(runtime, calls)

    outcome = runtime.run_yaml(
        _scenario(),
        source_name="scenario.yaml",
        topology_overrides={"valkey_binary": "/override/valkey-server"},
    )

    assert outcome.status is OutcomeStatus.PASS
    assert outcome.plan.topology_document["valkey_binary"] == "/override/valkey-server"
    assert calls == [
        "provider.validate:/override/valkey-server",
        "provider.provision",
        "action.run",
        "provider.cleanup",
    ]


def test_runtime_rejects_registration_replacement_after_compile(tmp_path):
    (tmp_path / "feature.yaml").write_text(_manifest(), encoding="utf-8")
    provider_calls = []
    runtime = FeatureRuntime(
        tmp_path,
        discover_entry_points=False,
        topology_providers=_topology_providers(lambda: _Provider(provider_calls)),
    )
    _register_action(runtime, [])
    plan = runtime.compile_yaml(_scenario())

    action_registry = runtime.registries[ComponentKind.ACTION]
    action_registry.unregister("example.action")
    action_registry.register(
        ComponentRegistration[Any](
            component_id="example.action",
            kind=ComponentKind.ACTION,
            api_version="v1",
            factory=lambda: _PassingAction([]),
        )
    )

    with pytest.raises(FeatureRuntimeError, match="changed after"):
        runtime.run_plan(plan)
    assert provider_calls == []


def test_componentless_plan_is_independent_of_runtime_extension_api():
    topology_providers = _topology_providers(
        lambda: _Provider([]),
        api_version="v999",
    )
    runtime = FeatureRuntime(
        discover_entry_points=False,
        topology_providers=topology_providers,
        extension_api_version="v999",
    )
    plan = RunPlan(
        scenario_id="foreign-plan",
        pack_id="example.foreign",
        pack_version="1",
        seed=1,
        topology="standalone",
        components=(),
        topology_provider=topology_providers["standalone"].identity,
        mode=RunMode.TOPOLOGY_SMOKE,
        topology_document={"type": "standalone"},
    )

    runtime.validate_plan(plan)
    assert runtime.run_plan(plan).status is OutcomeStatus.PASS


def test_runtime_serializes_provider_lifecycle_across_concurrent_runs(tmp_path):
    (tmp_path / "feature.yaml").write_text(_manifest(), encoding="utf-8")
    state_lock = threading.Lock()
    active = 0
    max_active = 0

    class SharedProvider(_Provider):
        def provision(self, context):
            nonlocal active, max_active
            with state_lock:
                active += 1
                max_active = max(max_active, active)
            time.sleep(0.02)
            return ProvisionedTopology()

        def cleanup(self, context, topology):
            nonlocal active
            with state_lock:
                active -= 1

    runtime = FeatureRuntime(
        tmp_path,
        discover_entry_points=False,
        topology_providers=_topology_providers(lambda: SharedProvider([])),
    )
    _register_action(runtime, [])
    plan = runtime.compile_yaml(_scenario())

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(executor.map(lambda _: runtime.run_plan(plan), range(2)))

    assert [outcome.status for outcome in outcomes] == [
        OutcomeStatus.PASS,
        OutcomeStatus.PASS,
    ]
    assert max_active == 1
    assert active == 0


def test_runtime_rejects_recursive_execution_from_component(tmp_path):
    (tmp_path / "feature.yaml").write_text(_manifest(), encoding="utf-8")
    nested_errors = []
    runtime = FeatureRuntime(
        tmp_path,
        discover_entry_points=False,
        topology_providers=_topology_providers(lambda: _Provider([])),
    )

    class ReentrantAction:
        def run(self, context, spec):
            try:
                runtime.run_plan(context.plan.source_plan)
            except FeatureRuntimeError as exc:
                nested_errors.append(str(exc))
            return ComponentResult(
                component_instance_id=spec.instance_id,
                status=ComponentStatus.PASS,
                summary="nested run rejected",
            )

    runtime.registries[ComponentKind.ACTION].register(
        ComponentRegistration[Any](
            component_id="example.action",
            kind=ComponentKind.ACTION,
            api_version="v1",
            factory=ReentrantAction,
        )
    )
    plan = runtime.compile_yaml(_scenario())

    assert runtime.run_plan(plan).status is OutcomeStatus.PASS
    assert nested_errors == ["recursive validation or execution on one FeatureRuntime is not supported"]


def test_invalid_runtime_configuration_fails_before_plugin_discovery(monkeypatch):
    monkeypatch.setattr(
        "valkey_fuzzer.extensibility.importlib_metadata.entry_points",
        lambda: pytest.fail("invalid runtime configuration must not execute plugins"),
    )

    with pytest.raises(TypeError, match="artifact_dir must be a string"):
        FeatureRuntime(artifact_dir=object())


def test_explicit_manifest_identity_preempts_plugin_pack(
    tmp_path,
    monkeypatch,
):
    (tmp_path / "feature.yaml").write_text(_manifest(), encoding="utf-8")
    plugin = PluginManifest(
        plugin_id="example.plugin",
        api_version="v1",
        registrations=(),
        feature_packs=(load_feature_pack_yaml(_manifest()),),
    )
    entry_points = _EntryPoints(_EntryPoint(plugin))
    monkeypatch.setattr(
        "valkey_fuzzer.extensibility.importlib_metadata.entry_points",
        lambda: entry_points,
    )

    with pytest.raises(DuplicateFeaturePackError):
        FeatureRuntime(
            tmp_path,
            topology_providers=_topology_providers(lambda: _Provider([])),
        )

    assert entry_points.selected_group == "valkey_fuzzer.plugins"


def test_components_can_resolve_registrations_from_their_own_threads(tmp_path):
    """Registration locks must not be held while a plan runs (worker threads would deadlock)."""
    (tmp_path / "feature.yaml").write_text(_manifest(), encoding="utf-8")
    runtime = FeatureRuntime(
        tmp_path,
        discover_entry_points=False,
        topology_providers=_topology_providers(lambda: _Provider([])),
    )
    resolved = []

    def resolve_from_worker():
        resolved.append(runtime.registries[ComponentKind.ACTION].resolve("example.action").component_id)
        resolved.append(runtime.topology_providers.resolve("standalone").topology)

    class ThreadedAction:
        def run(self, context, spec):
            worker = threading.Thread(target=resolve_from_worker, daemon=True)
            worker.start()
            worker.join(timeout=2)
            return ComponentResult(
                component_instance_id=spec.instance_id,
                status=ComponentStatus.ERROR if worker.is_alive() else ComponentStatus.PASS,
                summary="worker blocked on a registration lock" if worker.is_alive() else "worker resolved",
            )

    runtime.registries[ComponentKind.ACTION].register(
        ComponentRegistration[Any](
            component_id="example.action",
            kind=ComponentKind.ACTION,
            api_version="v1",
            factory=ThreadedAction,
        )
    )

    outcome = runtime.run_plan(runtime.compile_yaml(_scenario()))

    assert outcome.status is OutcomeStatus.PASS, outcome.summary
    assert resolved == ["example.action", "standalone"]


def test_registration_changed_by_another_thread_during_a_run_is_rolled_back(tmp_path):
    (tmp_path / "feature.yaml").write_text(_manifest(), encoding="utf-8")
    runtime = FeatureRuntime(
        tmp_path,
        discover_entry_points=False,
        topology_providers=_topology_providers(lambda: _Provider([])),
    )
    action_registry = runtime.registries[ComponentKind.ACTION]

    def register_intruder():
        action_registry.register(
            ComponentRegistration[Any](
                component_id="example.intruder",
                kind=ComponentKind.ACTION,
                api_version="v1",
                factory=lambda: _PassingAction([]),
            )
        )

    class MutatingAction:
        def run(self, context, spec):
            worker = threading.Thread(target=register_intruder, daemon=True)
            worker.start()
            worker.join(timeout=2)
            return ComponentResult(spec.instance_id, ComponentStatus.PASS, "done")

    action_registry.register(
        ComponentRegistration[Any](
            component_id="example.action",
            kind=ComponentKind.ACTION,
            api_version="v1",
            factory=MutatingAction,
        )
    )
    plan = runtime.compile_yaml(_scenario())

    with pytest.raises(FeatureRuntimeError, match="during the runtime operation"):
        runtime.run_plan(plan)
    assert "example.intruder" not in action_registry
