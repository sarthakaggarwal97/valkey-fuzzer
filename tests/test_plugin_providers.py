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
    FeaturePackSpec,
    PluginDiscoveryError,
    PluginManifest,
    Registry,
    RunMode,
    discover_plugins,
)
from src.feature_packs import FeaturePackCatalog
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


def test_provider_only_manifest_validates_registration_types_versions_and_duplicates():
    artifact = _artifact_registration()
    resource = _resource_registration()

    manifest = PluginManifest(
        plugin_id="example.plugin",
        api_version="v1",
        artifact_providers=(artifact,),
        resource_providers=(resource,),
    )

    assert manifest.registrations == ()
    assert manifest.artifact_providers == (artifact,)
    assert manifest.resource_providers == (resource,)

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


def test_discovery_registers_artifact_and_resource_providers(monkeypatch):
    manifest = PluginManifest(
        plugin_id="example.plugin",
        api_version="v1",
        artifact_providers=(_artifact_registration(),),
        resource_providers=(_resource_registration(),),
    )
    entry_points = _install_entry_points(monkeypatch, manifest)
    artifacts = ArtifactProviderRegistry()
    resources = ResourceProviderRegistry()

    discovered = discover_plugins(
        {},
        artifact_provider_registry=artifacts,
        resource_provider_registry=resources,
    )

    assert entry_points.selected_group == "valkey_fuzzer.plugins"
    assert discovered == (manifest,)
    assert artifacts.registered_ids == ("example.artifact",)
    assert resources.registered_ids == ("example.resource",)


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
    components = Registry[Any](ComponentKind.ACTION)
    manifest = PluginManifest(
        plugin_id="example.plugin",
        api_version="v1",
        registrations=(_component_registration(),),
        artifact_providers=(_artifact_registration(),),
        resource_providers=(_resource_registration(),),
    )
    _install_entry_points(monkeypatch, manifest)

    with pytest.raises(DuplicateProviderRegistrationError, match=r"example\.artifact"):
        discover_plugins(
            {ComponentKind.ACTION: components},
            artifact_provider_registry=artifacts,
            resource_provider_registry=resources,
        )

    assert components.registered_ids == ()
    assert artifacts.snapshot() == {"example.artifact": existing_artifact}
    assert resources.registered_ids == ()


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


def test_commit_failure_rolls_back_components_packs_and_both_provider_registries(
    monkeypatch,
):
    manifest = PluginManifest(
        plugin_id="example.plugin",
        api_version="v1",
        registrations=(_component_registration(),),
        feature_packs=(_feature_pack(),),
        artifact_providers=(_artifact_registration(),),
        resource_providers=(_resource_registration(),),
    )
    _install_entry_points(monkeypatch, manifest)
    components = Registry[Any](ComponentKind.ACTION)
    catalog = FeaturePackCatalog()
    artifacts = ArtifactProviderRegistry()
    resources = ResourceProviderRegistry()

    def fail_catalog_commit(_feature_packs):
        raise RuntimeError("catalog commit failed")

    monkeypatch.setattr(catalog, "register_many", fail_catalog_commit)

    with pytest.raises(RuntimeError, match="catalog commit failed"):
        discover_plugins(
            {ComponentKind.ACTION: components},
            feature_pack_catalog=catalog,
            artifact_provider_registry=artifacts,
            resource_provider_registry=resources,
        )

    assert components.registered_ids == ()
    assert catalog.registered_keys == ()
    assert artifacts.registered_ids == ()
    assert resources.registered_ids == ()


@pytest.mark.parametrize(
    ("registry_arg", "registry"),
    (
        ("artifact_provider_registry", ArtifactProviderRegistry(api_version="v2")),
        ("resource_provider_registry", ResourceProviderRegistry(api_version="v2")),
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
    )
    _install_entry_points(monkeypatch, manifest)

    runtime = FeatureRuntime(topology_providers={})

    assert runtime.plugins == (manifest,)
    assert runtime.artifact_providers.registered_ids == ("example.artifact",)
    assert runtime.resource_providers.registered_ids == ("example.resource",)
