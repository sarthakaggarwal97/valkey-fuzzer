"""CLI-friendly bootstrap for the generic v3 feature-pack runtime.

The runtime owns one complete extension environment: all component
registries, the exact-version feature-pack catalog, plugin discovery,
data-only manifest loading, compilation, and execution. Feature behavior
remains entirely in registrations and manifests; this module contains no
feature-specific dispatch.
"""

import copy
import threading
from collections.abc import Mapping, Sequence
from contextlib import ExitStack, contextmanager
from os import PathLike
from pathlib import Path
from types import MappingProxyType
from typing import Any, Optional, TextIO, Union

from .extensibility import (
    EXTENSION_API_VERSION,
    PLUGIN_ENTRY_POINT_GROUP,
    ComponentKind,
    FeaturePackSpec,
    PluginManifest,
    Registry,
    RunOutcome,
    RunPlan,
    discover_plugins,
)
from .feature_packs import (
    SCENARIO_API_VERSION,
    FeaturePackCatalog,
    FeaturePackCompiler,
    FeaturePackDocumentError,
    ReferenceInventory,
    load_feature_pack_directory,
    load_v3_scenario_yaml,
)
from .feature_runner import FeatureRunner, TopologyProviderEntry
from .fuzzer_engine.cluster_coordinator import ClusterCoordinator
from .local_topology_provider import local_topology_provider_factories

FeaturePackDirectory = Union[str, PathLike[str]]
YamlSource = Union[str, bytes, TextIO]


class FeatureRuntimeError(RuntimeError):
    """The configured extension environment cannot safely execute a plan."""


def _ordered_directories(
    directories: Union[FeaturePackDirectory, Sequence[FeaturePackDirectory]],
) -> tuple[Path, ...]:
    if isinstance(directories, (str, PathLike)):
        entries = (directories,)
    elif isinstance(directories, Sequence) and not isinstance(directories, (bytes, bytearray)):
        entries = tuple(directories)
    else:
        raise TypeError("feature_pack_directories must be a path or an ordered sequence of paths")

    normalized = []
    for directory in entries:
        if isinstance(directory, bytes) or not isinstance(directory, (str, PathLike)):
            raise TypeError("feature_pack_directories entries must be filesystem paths")
        normalized.append(Path(directory))
    return tuple(normalized)


def _detached_copy(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _detached_copy(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_detached_copy(item) for item in value]
    return copy.deepcopy(value)


def _with_topology_overrides(
    scenario: Mapping[str, Any],
    topology_overrides: Optional[Mapping[str, Any]],
) -> Mapping[str, Any]:
    if topology_overrides is None:
        return scenario
    if not isinstance(topology_overrides, Mapping):
        raise TypeError("topology_overrides must be a mapping or None")
    if any(not isinstance(key, str) for key in topology_overrides):
        raise TypeError("topology_overrides keys must be strings")

    document = _detached_copy(scenario)
    topology = document.get("topology")
    if not isinstance(topology, Mapping):
        raise FeaturePackDocumentError("topology must be a mapping before topology overrides can be applied")

    merged_topology = _detached_copy(topology)
    merged_topology.update(_detached_copy(topology_overrides))
    document["topology"] = merged_topology
    return document


class FeatureRuntime:
    """One isolated, generic extension environment for v3 scenarios."""

    def __init__(
        self,
        feature_pack_directories: Union[
            FeaturePackDirectory,
            Sequence[FeaturePackDirectory],
        ] = (),
        *,
        discover_entry_points: bool = True,
        plugin_group: str = PLUGIN_ENTRY_POINT_GROUP,
        reference_inventory: Optional[ReferenceInventory] = None,
        topology_providers: Optional[Mapping[str, TopologyProviderEntry]] = None,
        coordinator_factory=ClusterCoordinator,
        artifact_dir: str = "",
        extension_api_version: str = EXTENSION_API_VERSION,
        scenario_api_version: str = SCENARIO_API_VERSION,
    ):
        if not isinstance(discover_entry_points, bool):
            raise TypeError("discover_entry_points must be a boolean")

        self._extension_api_version = extension_api_version
        self._scenario_api_version = scenario_api_version
        self._lifecycle_lock = threading.RLock()
        self._lifecycle_owner: Optional[int] = None
        self._registry_pin: Optional[Mapping[ComponentKind, Mapping[str, Any]]] = None

        registries = {kind: Registry[Any](kind, api_version=extension_api_version) for kind in ComponentKind}
        self._registries: Mapping[ComponentKind, Registry[Any]] = MappingProxyType(registries)
        self._catalog = FeaturePackCatalog(api_version=extension_api_version)
        self._manifest_feature_packs: tuple[FeaturePackSpec, ...] = ()
        self.load_feature_pack_directories(feature_pack_directories)

        self._compiler = FeaturePackCompiler(
            self._catalog,
            self._registries,
            reference_inventory=reference_inventory,
            extension_api_version=extension_api_version,
            scenario_api_version=scenario_api_version,
        )

        if topology_providers is None:
            if not callable(coordinator_factory):
                raise TypeError("coordinator_factory must be callable")
            provider_entries = local_topology_provider_factories(coordinator_factory)
        else:
            if not isinstance(topology_providers, Mapping):
                raise TypeError("topology_providers must be a mapping or None")
            provider_entries = dict(topology_providers)
        self._topology_providers: Mapping[str, TopologyProviderEntry] = MappingProxyType(dict(provider_entries))
        self._runner = FeatureRunner(
            self._registries,
            self._topology_providers,
            artifact_dir=artifact_dir,
        )
        self._plugins = (
            discover_plugins(
                self._registries,
                group=plugin_group,
                feature_pack_catalog=self._catalog,
                api_version=extension_api_version,
            )
            if discover_entry_points
            else ()
        )

    @property
    def registries(self) -> Mapping[ComponentKind, Registry[Any]]:
        return self._registries

    @property
    def catalog(self) -> FeaturePackCatalog:
        return self._catalog

    @property
    def plugins(self) -> tuple[PluginManifest, ...]:
        return self._plugins

    @property
    def manifest_feature_packs(self) -> tuple[FeaturePackSpec, ...]:
        return self._manifest_feature_packs

    @property
    def topology_providers(self) -> Mapping[str, TopologyProviderEntry]:
        return self._topology_providers

    def _ordered_registries(self) -> tuple[Registry[Any], ...]:
        return tuple(self._registries[kind] for kind in sorted(self._registries, key=lambda item: item.value))

    def _registry_snapshot(self) -> Mapping[ComponentKind, Mapping[str, Any]]:
        return MappingProxyType({kind: registry.snapshot() for kind, registry in self._registries.items()})

    def _assert_registry_snapshot(
        self,
        expected: Mapping[ComponentKind, Mapping[str, Any]],
    ) -> None:
        for kind, registry in self._registries.items():
            if registry.kind is not kind or registry.api_version != self._extension_api_version:
                raise FeatureRuntimeError(
                    f"component registry metadata for {kind.value!r} changed after runtime construction"
                )
            current = registry.snapshot()
            pinned = expected[kind]
            if set(current) != set(pinned) or any(
                current[component_id] is not pinned[component_id] for component_id in pinned
            ):
                raise FeatureRuntimeError(
                    f"component registry for {kind.value!r} changed after the extension environment was pinned"
                )

    def _expected_registry_snapshot(self) -> Mapping[ComponentKind, Mapping[str, Any]]:
        if self._registry_pin is not None:
            self._assert_registry_snapshot(self._registry_pin)
            return self._registry_pin
        return self._registry_snapshot()

    def _commit_registry_pin(
        self,
        expected: Mapping[ComponentKind, Mapping[str, Any]],
    ) -> None:
        self._assert_registry_snapshot(expected)
        if self._registry_pin is None:
            self._registry_pin = expected

    def _validate_plan_api(self, plan: RunPlan) -> None:
        if not isinstance(plan, RunPlan):
            raise TypeError("plan must be a RunPlan")
        if plan.api_version != self._extension_api_version:
            raise FeatureRuntimeError(
                f"plan extension API {plan.api_version!r} does not match runtime API {self._extension_api_version!r}"
            )
        if plan.scenario_api_version != self._scenario_api_version:
            raise FeatureRuntimeError(
                f"plan scenario API {plan.scenario_api_version!r} does not match runtime API "
                f"{self._scenario_api_version!r}"
            )

    @contextmanager
    def _lifecycle_operation(self):
        with self._lifecycle_lock:
            owner = threading.get_ident()
            if self._lifecycle_owner == owner:
                raise FeatureRuntimeError("recursive validation or execution on one FeatureRuntime is not supported")
            if self._lifecycle_owner is not None:  # defensive; the lock serializes other threads
                raise FeatureRuntimeError("FeatureRuntime lifecycle ownership is inconsistent")
            self._lifecycle_owner = owner
            try:
                yield
            finally:
                self._lifecycle_owner = None

    def load_feature_pack_directories(
        self,
        directories: Union[
            FeaturePackDirectory,
            Sequence[FeaturePackDirectory],
        ],
    ) -> tuple[FeaturePackSpec, ...]:
        """Atomically load an ordered set of manifest directories.

        Every file in every directory is parsed and validated before the
        shared catalog is changed. Directory order is caller-defined; files
        within each directory use the deterministic loader order.
        """

        staging_catalog = FeaturePackCatalog(api_version=self._catalog.api_version)
        packs = []
        for directory in _ordered_directories(directories):
            packs.extend(
                load_feature_pack_directory(
                    directory,
                    catalog=staging_catalog,
                )
            )

        entries = tuple(packs)
        self._catalog.register_many(entries)
        self._manifest_feature_packs += entries
        return entries

    def compile(
        self,
        scenario: Mapping[str, Any],
        *,
        topology_overrides: Optional[Mapping[str, Any]] = None,
    ) -> RunPlan:
        """Compile a v3 scenario after applying detached topology overrides."""

        if not isinstance(scenario, Mapping):
            raise TypeError("scenario must be a mapping")
        with ExitStack() as locks:
            for registry in self._ordered_registries():
                locks.enter_context(registry.registration_lock)
            expected = self._expected_registry_snapshot()
            plan = self._compiler.compile(_with_topology_overrides(scenario, topology_overrides))
            self._commit_registry_pin(expected)
            return plan

    def compile_yaml(
        self,
        source: YamlSource,
        *,
        source_name: str = "<yaml>",
        topology_overrides: Optional[Mapping[str, Any]] = None,
    ) -> RunPlan:
        """Strict-load and compile one duplicate-free v3 YAML scenario."""

        scenario = load_v3_scenario_yaml(source, source_name=source_name)
        return self.compile(scenario, topology_overrides=topology_overrides)

    def validate_plan(self, plan: RunPlan) -> None:
        """Structurally validate a compiled plan without provisioning."""

        self._validate_plan_api(plan)
        with self._lifecycle_operation(), ExitStack() as locks:
            for registry in self._ordered_registries():
                locks.enter_context(registry.registration_lock)
            expected = self._expected_registry_snapshot()
            self._runner.validate(plan)
            self._commit_registry_pin(expected)

    def run_plan(
        self,
        plan: RunPlan,
        *,
        deadline: Optional[float] = None,
    ) -> RunOutcome:
        self._validate_plan_api(plan)
        with self._lifecycle_operation(), ExitStack() as locks:
            for registry in self._ordered_registries():
                locks.enter_context(registry.registration_lock)
            expected = self._expected_registry_snapshot()
            outcome = self._runner.run(plan, deadline=deadline)
            self._commit_registry_pin(expected)
            return outcome

    def run_yaml(
        self,
        source: YamlSource,
        *,
        source_name: str = "<yaml>",
        topology_overrides: Optional[Mapping[str, Any]] = None,
        deadline: Optional[float] = None,
    ) -> RunOutcome:
        plan = self.compile_yaml(
            source,
            source_name=source_name,
            topology_overrides=topology_overrides,
        )
        return self.run_plan(plan, deadline=deadline)


__all__ = [
    "FeaturePackDirectory",
    "FeatureRuntime",
    "FeatureRuntimeError",
    "YamlSource",
]
