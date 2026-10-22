"""Provider registration through the trusted plugin entry-point transaction."""

from types import SimpleNamespace
from typing import Any

import pytest

from src.environment import (
    ArtifactProviderRegistration,
    ArtifactProviderRegistry,
    DuplicateProviderRegistrationError,
    ResourceProviderRegistration,
    ResourceProviderRegistry,
)
from src.extensibility import (
    ComponentKind,
    ComponentRegistration,
    ComponentSpec,
    DuplicateTopologyProviderRegistrationError,
    FeaturePackSpec,
    IncompatibleTopologyProviderRegistrationError,
    OutcomeStatus,
    PluginDiscoveryError,
    PluginManifest,
    Registry,
    RunMode,
    TopologyProviderRegistration,
    TopologyProviderRegistry,
    TopologyProviderRegistryPinnedError,
    UnknownTopologyProviderError,
    discover_plugins,
)
from src.feature_packs import FeaturePackCatalog, ScenarioCompileError
from src.feature_runner import ProvisionedTopology
from src.feature_runtime import FeatureRuntime


class _EntryPoint:
    def __init__(self, manifest: PluginManifest, distribution: str = "test-distribution"):
        self.name = manifest.plugin_id
        self.value = f"tests:{manifest.plugin_id}"
        self.dist = SimpleNamespace(name=distribution)
        self._manifest = manifest
        self.load_calls = 0

    def load(self):
        self.load_calls += 1
        return self._manifest


class _EntryPoints:
    def __init__(self, *entry_points: _EntryPoint):
        self._entry_points = entry_points
        self.selected_group = None

    def select(self, *, group):
        self.selected_group = group
        return self._entry_points


def _install_entry_points(monkeypatch, *manifests: PluginManifest) -> _EntryPoints:
    entry_points = _EntryPoints(*(_EntryPoint(manifest) for manifest in manifests))
    monkeypatch.setattr(
        "src.extensibility.importlib_metadata.entry_points",
        lambda: entry_points,
    )
    return entry_points


def _component_registration(
    component_id: str = "example.action",
    *,
    api_version: str = "v1",
) -> ComponentRegistration[Any]:
    return ComponentRegistration[Any](
        component_id=component_id,
        kind=ComponentKind.ACTION,
        api_version=api_version,
        factory=lambda: object(),
    )


def _artifact_registration(
    provider_id: str = "example.artifact",
    *,
    api_version: str = "v1",
) -> ArtifactProviderRegistration:
    return ArtifactProviderRegistration(
        provider_id=provider_id,
        api_version=api_version,
        outputs=("path",),
        factory=lambda: object(),
    )


def _resource_registration(
    provider_id: str = "example.resource",
    *,
    api_version: str = "v1",
) -> ResourceProviderRegistration:
    return ResourceProviderRegistration(
        provider_id=provider_id,
        api_version=api_version,
        outputs=("endpoint",),
        factory=lambda: object(),
    )


def _topology_registration(
    topology: str = "example-topology",
    *,
    api_version: str = "v1",
    factory=None,
) -> TopologyProviderRegistration:
    if factory is None:
        factory = object
    return TopologyProviderRegistration(
        topology=topology,
        provider_id="example.topology-provider",
        implementation_version="1",
        api_version=api_version,
        factory=factory,
    )


def _feature_pack() -> FeaturePackSpec:
    return FeaturePackSpec(
        pack_id="example.feature",
        version="1",
        mode=RunMode.TOPOLOGY_SMOKE,
        supported_topologies=("standalone",),
        components=(
            ComponentSpec(
                kind=ComponentKind.ACTION,
                component_id="example.action",
                instance_id="exercise",
            ),
        ),
    )


def test_topology_provider_registry_is_typed_deterministic_and_atomic():
    registry = TopologyProviderRegistry()
    zeta = _topology_registration("zeta-topology")
    alpha = _topology_registration("alpha-topology")

    registry.register_many((zeta, alpha))

    assert registry.registered_ids == ("alpha-topology", "zeta-topology")
    assert registry.resolve("alpha-topology") is alpha
    snapshot = registry.snapshot()

    duplicate = _topology_registration("new-topology")
    with pytest.raises(DuplicateTopologyProviderRegistrationError):
        registry.register_many((duplicate, duplicate))
    assert registry.snapshot() == snapshot

    with pytest.raises(TypeError, match="TopologyProviderRegistration"):
        registry.register_many((object(),))
    with pytest.raises(IncompatibleTopologyProviderRegistrationError):
        registry.register(_topology_registration("other-topology", api_version="v2"))
    assert registry.snapshot() == snapshot

    assert registry.unregister("alpha-topology") is alpha
    with pytest.raises(UnknownTopologyProviderError):
        registry.resolve("alpha-topology")
    registry.restore_snapshot(snapshot)
    assert registry.resolve("alpha-topology") is alpha


def test_topology_provider_registry_pin_allows_reads_but_no_snapshot_rewrite():
    registry = TopologyProviderRegistry()
    original = _topology_registration("example-topology")
    replacement = _topology_registration("replacement-topology")
    registry.register(original)
    snapshot = registry.snapshot()

    with registry.pin_snapshot(snapshot):
        assert registry.resolve("example-topology") is original
        assert registry.snapshot() == snapshot
        with pytest.raises(TopologyProviderRegistryPinnedError):
            registry.register(replacement)
        with pytest.raises(TopologyProviderRegistryPinnedError):
            registry.unregister("example-topology")
        with pytest.raises(TopologyProviderRegistryPinnedError):
            registry.restore_snapshot(snapshot)
        with pytest.raises(TopologyProviderRegistryPinnedError):
            registry.restore_runtime_snapshot({"replacement-topology": replacement})
        registry.restore_runtime_snapshot(snapshot)

    assert registry.snapshot() == snapshot


@pytest.mark.parametrize(
    "factory",
    (
        lambda required: object(),
        (lambda: (yield object())),
    ),
)
def test_topology_provider_registration_rejects_non_sync_zero_arg_factories(
    factory,
):
    with pytest.raises(TypeError, match="synchronous zero-argument"):
        _topology_registration(factory=factory)


def test_topology_provider_registration_rejects_async_factory():
    async def factory():
        return object()

    with pytest.raises(TypeError, match="synchronous zero-argument"):
        _topology_registration(factory=factory)


def test_provider_only_manifest_validates_registration_types_versions_and_duplicates():
    artifact = _artifact_registration()
    resource = _resource_registration()
    topology = _topology_registration()

    manifest = PluginManifest(
        plugin_id="example.plugin",
        api_version="v1",
        artifact_providers=(artifact,),
        resource_providers=(resource,),
        topology_providers=(topology,),
    )

    assert manifest.registrations == ()
    assert manifest.artifact_providers == (artifact,)
    assert manifest.resource_providers == (resource,)
    assert manifest.topology_providers == (topology,)

    with pytest.raises(TypeError, match="ArtifactProviderRegistration"):
        PluginManifest(
            plugin_id="example.plugin",
            api_version="v1",
            artifact_providers=(resource,),
        )
    with pytest.raises(TypeError, match="ResourceProviderRegistration"):
        PluginManifest(
            plugin_id="example.plugin",
            api_version="v1",
            resource_providers=(artifact,),
        )
    with pytest.raises(ValueError, match=r"uses API 'v2'.*expected 'v1'"):
        PluginManifest(
            plugin_id="example.plugin",
            api_version="v1",
            artifact_providers=(_artifact_registration(api_version="v2"),),
        )
    with pytest.raises(DuplicateProviderRegistrationError, match=r"example\.resource"):
        PluginManifest(
            plugin_id="example.plugin",
            api_version="v1",
            resource_providers=(resource, resource),
        )
    with pytest.raises(TypeError, match="TopologyProviderRegistration"):
        PluginManifest(
            plugin_id="example.plugin",
            api_version="v1",
            topology_providers=(artifact,),
        )
    with pytest.raises(ValueError, match=r"uses API 'v2'.*expected 'v1'"):
        PluginManifest(
            plugin_id="example.plugin",
            api_version="v1",
            topology_providers=(_topology_registration(api_version="v2"),),
        )
    with pytest.raises(
        DuplicateTopologyProviderRegistrationError,
        match="example-topology",
    ):
        PluginManifest(
            plugin_id="example.plugin",
            api_version="v1",
            topology_providers=(topology, topology),
        )


def test_discovery_registers_artifact_resource_and_topology_providers(
    monkeypatch,
):
    manifest = PluginManifest(
        plugin_id="example.plugin",
        api_version="v1",
        artifact_providers=(_artifact_registration(),),
        resource_providers=(_resource_registration(),),
        topology_providers=(_topology_registration(),),
    )
    entry_points = _install_entry_points(monkeypatch, manifest)
    artifacts = ArtifactProviderRegistry()
    resources = ResourceProviderRegistry()
    topologies = TopologyProviderRegistry()

    discovered = discover_plugins(
        {},
        artifact_provider_registry=artifacts,
        resource_provider_registry=resources,
        topology_provider_registry=topologies,
    )

    assert entry_points.selected_group == "valkey_fuzzer.plugins"
    assert discovered == (manifest,)
    assert artifacts.registered_ids == ("example.artifact",)
    assert resources.registered_ids == ("example.resource",)
    assert topologies.registered_ids == ("example-topology",)


@pytest.mark.parametrize(
    ("manifest", "discovery_kwargs", "message"),
    (
        (
            PluginManifest(
                plugin_id="example.plugin",
                api_version="v1",
                registrations=(_component_registration(),),
                artifact_providers=(_artifact_registration(),),
            ),
            {},
            "no artifact_provider_registry was provided",
        ),
        (
            PluginManifest(
                plugin_id="example.plugin",
                api_version="v1",
                registrations=(_component_registration(),),
                resource_providers=(_resource_registration(),),
            ),
            {"artifact_provider_registry": ArtifactProviderRegistry()},
            "no resource_provider_registry was provided",
        ),
        (
            PluginManifest(
                plugin_id="example.plugin",
                api_version="v1",
                registrations=(_component_registration(),),
                topology_providers=(_topology_registration(),),
            ),
            {
                "artifact_provider_registry": ArtifactProviderRegistry(),
                "resource_provider_registry": ResourceProviderRegistry(),
            },
            "no topology_provider_registry was provided",
        ),
    ),
)
def test_discovery_requires_each_provider_registry_before_mutating_components(
    monkeypatch,
    manifest,
    discovery_kwargs,
    message,
):
    _install_entry_points(monkeypatch, manifest)
    components = Registry[Any](ComponentKind.ACTION)

    with pytest.raises(PluginDiscoveryError, match=message) as error:
        discover_plugins(
            {ComponentKind.ACTION: components},
            **discovery_kwargs,
        )

    assert error.value.entry_point == "example.plugin"
    assert error.value.distribution == "test-distribution"
    assert len(components) == 0


def test_duplicate_provider_preflight_leaves_all_participants_unchanged(monkeypatch):
    existing_artifact = _artifact_registration()
    artifacts = ArtifactProviderRegistry()
    artifacts.register(existing_artifact)
    resources = ResourceProviderRegistry()
    topologies = TopologyProviderRegistry()
    existing_topology = _topology_registration()
    topologies.register(existing_topology)
    components = Registry[Any](ComponentKind.ACTION)
    manifest = PluginManifest(
        plugin_id="example.plugin",
        api_version="v1",
        registrations=(_component_registration(),),
        artifact_providers=(_artifact_registration(),),
        resource_providers=(_resource_registration(),),
        topology_providers=(_topology_registration(),),
    )
    _install_entry_points(monkeypatch, manifest)

    with pytest.raises(DuplicateProviderRegistrationError, match=r"example\.artifact"):
        discover_plugins(
            {ComponentKind.ACTION: components},
            artifact_provider_registry=artifacts,
            resource_provider_registry=resources,
            topology_provider_registry=topologies,
        )

    assert components.registered_ids == ()
    assert artifacts.snapshot() == {"example.artifact": existing_artifact}
    assert resources.registered_ids == ()
    assert topologies.snapshot() == {"example-topology": existing_topology}


def test_duplicate_provider_across_plugins_is_rejected_before_mutation(monkeypatch):
    first = PluginManifest(
        plugin_id="alpha.plugin",
        api_version="v1",
        artifact_providers=(_artifact_registration(),),
    )
    second = PluginManifest(
        plugin_id="beta.plugin",
        api_version="v1",
        artifact_providers=(_artifact_registration(),),
    )
    _install_entry_points(monkeypatch, second, first)
    artifacts = ArtifactProviderRegistry()

    with pytest.raises(DuplicateProviderRegistrationError, match=r"example\.artifact"):
        discover_plugins({}, artifact_provider_registry=artifacts)

    assert artifacts.registered_ids == ()


def test_duplicate_topology_provider_across_plugins_is_rejected_before_mutation(
    monkeypatch,
):
    first = PluginManifest(
        plugin_id="alpha.plugin",
        api_version="v1",
        topology_providers=(_topology_registration(),),
    )
    second = PluginManifest(
        plugin_id="beta.plugin",
        api_version="v1",
        topology_providers=(_topology_registration(),),
    )
    _install_entry_points(monkeypatch, second, first)
    topologies = TopologyProviderRegistry()

    with pytest.raises(
        DuplicateTopologyProviderRegistrationError,
        match="example-topology",
    ):
        discover_plugins(
            {},
            topology_provider_registry=topologies,
        )

    assert topologies.registered_ids == ()


def test_commit_failure_rolls_back_components_packs_and_all_provider_registries(
    monkeypatch,
):
    manifest = PluginManifest(
        plugin_id="example.plugin",
        api_version="v1",
        registrations=(_component_registration(),),
        feature_packs=(_feature_pack(),),
        artifact_providers=(_artifact_registration(),),
        resource_providers=(_resource_registration(),),
        topology_providers=(_topology_registration(),),
    )
    _install_entry_points(monkeypatch, manifest)
    components = Registry[Any](ComponentKind.ACTION)
    catalog = FeaturePackCatalog()
    artifacts = ArtifactProviderRegistry()
    resources = ResourceProviderRegistry()
    topologies = TopologyProviderRegistry()

    def fail_catalog_commit(_feature_packs):
        raise RuntimeError("catalog commit failed")

    monkeypatch.setattr(catalog, "register_many", fail_catalog_commit)

    with pytest.raises(RuntimeError, match="catalog commit failed"):
        discover_plugins(
            {ComponentKind.ACTION: components},
            feature_pack_catalog=catalog,
            artifact_provider_registry=artifacts,
            resource_provider_registry=resources,
            topology_provider_registry=topologies,
        )

    assert components.registered_ids == ()
    assert catalog.registered_keys == ()
    assert artifacts.registered_ids == ()
    assert resources.registered_ids == ()
    assert topologies.registered_ids == ()


@pytest.mark.parametrize(
    ("registry_arg", "registry"),
    (
        ("artifact_provider_registry", ArtifactProviderRegistry(api_version="v2")),
        ("resource_provider_registry", ResourceProviderRegistry(api_version="v2")),
        ("topology_provider_registry", TopologyProviderRegistry(api_version="v2")),
    ),
)
def test_provider_registry_api_mismatch_fails_before_loading_entry_points(
    monkeypatch,
    registry_arg,
    registry,
):
    manifest = PluginManifest(plugin_id="example.plugin", api_version="v1")
    entry_points = _install_entry_points(monkeypatch, manifest)

    with pytest.raises(PluginDiscoveryError, match=r"registry API 'v2'.*extension API 'v1'"):
        discover_plugins({}, **{registry_arg: registry})

    assert entry_points._entry_points[0].load_calls == 0


def test_feature_runtime_wires_discovered_provider_registrations(monkeypatch):
    manifest = PluginManifest(
        plugin_id="example.plugin",
        api_version="v1",
        artifact_providers=(_artifact_registration(),),
        resource_providers=(_resource_registration(),),
        topology_providers=(_topology_registration(),),
    )
    _install_entry_points(monkeypatch, manifest)

    runtime = FeatureRuntime(topology_providers={})

    assert runtime.plugins == (manifest,)
    assert runtime.artifact_providers.registered_ids == ("example.artifact",)
    assert runtime.resource_providers.registered_ids == ("example.resource",)
    assert runtime.topology_providers.registered_ids == ("example-topology",)


def test_runtime_compile_rejects_a_pack_topology_without_a_registered_provider():
    runtime = FeatureRuntime(
        discover_entry_points=False,
        topology_providers={},
    )
    runtime.catalog.register(
        FeaturePackSpec(
            pack_id="example.missing-topology",
            version="1",
            mode=RunMode.TOPOLOGY_SMOKE,
            supported_topologies=("example-topology",),
            components=(),
        )
    )

    with pytest.raises(
        ScenarioCompileError,
        match="has no registered topology provider",
    ):
        runtime.compile(
            {
                "scenario_id": "missing-topology",
                "seed": 7,
                "feature": {
                    "use": "example.missing-topology",
                    "version": "1",
                    "with": {},
                },
                "topology": {"type": "example-topology"},
            }
        )


def test_plugin_topology_provider_executes_without_runtime_core_wiring(
    monkeypatch,
):
    calls = []

    class PluginTopologyProvider:
        def validate(self, plan):
            calls.append(("validate", plan.topology))

        def provision(self, context):
            calls.append(("provision", context.plan.topology))
            return ProvisionedTopology(services={"kind": "plugin"})

        def rollback(self, context):
            calls.append(("rollback", context.plan.topology))

        def cleanup(self, context, topology):
            calls.append(("cleanup", topology.services["kind"]))

    manifest = PluginManifest(
        plugin_id="example.plugin",
        api_version="v1",
        feature_packs=(
            FeaturePackSpec(
                pack_id="example.plugin-topology",
                version="1",
                mode=RunMode.TOPOLOGY_SMOKE,
                supported_topologies=("example-topology",),
                components=(),
            ),
        ),
        topology_providers=(
            _topology_registration(
                factory=PluginTopologyProvider,
            ),
        ),
    )
    _install_entry_points(monkeypatch, manifest)
    runtime = FeatureRuntime(topology_providers={})
    plan = runtime.compile(
        {
            "scenario_id": "plugin-topology",
            "seed": 7,
            "feature": {
                "use": "example.plugin-topology",
                "version": "1",
                "with": {},
            },
            "topology": {"type": "example-topology"},
        }
    )

    outcome = runtime.run_plan(plan)

    assert outcome.status is OutcomeStatus.PASS
    assert calls == [
        ("validate", "example-topology"),
        ("provision", "example-topology"),
        ("cleanup", "plugin"),
    ]
