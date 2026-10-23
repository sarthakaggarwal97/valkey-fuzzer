"""CLI-friendly bootstrap for the generic feature-pack runtime.

The runtime owns one complete extension environment: all component
registries, the exact-version feature-pack catalog, plugin discovery,
data-only manifest loading, compilation, and execution. Feature behavior
remains entirely in registrations and manifests; this module contains no
feature-specific dispatch.
"""

import copy
import threading
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import ExitStack, contextmanager
from os import PathLike
from pathlib import Path
from types import MappingProxyType
from typing import Any, Optional, TextIO, Union

from .environment import (
    ArtifactProviderRegistry,
    ResourceProviderRegistry,
)
from .environment_lifecycle import EnvironmentManager
from .extensibility import (
    EXTENSION_API_VERSION,
    PLUGIN_ENTRY_POINT_GROUP,
    ComponentKind,
    FeaturePackSpec,
    PluginManifest,
    Registry,
    RunOutcome,
    RunPlan,
    TopologyProviderRegistration,
    TopologyProviderRegistry,
    discover_plugins,
)
from .feature_packs import (
    FeaturePackCatalog,
    FeaturePackCompiler,
    FeaturePackDocumentError,
    ReferenceInventory,
    load_feature_pack_directory,
    load_scenario_yaml,
    validate_run_plan_environment,
)
from .feature_runner import (
    FeatureRunner,
    TopologyProviderSource,
    normalize_artifact_root,
)
from .fuzzer_engine.cluster_coordinator import ClusterCoordinator
from .local_topology_provider import local_topology_provider_registrations

FeaturePackDirectory = Union[str, PathLike[str]]
YamlSource = Union[str, bytes, TextIO]
_ExtensionSnapshot = tuple[
    Mapping[ComponentKind, Mapping[str, Any]],
    Mapping[str, Any],
    Mapping[str, Any],
    Mapping[str, Any],
    Mapping[tuple[str, str], FeaturePackSpec],
]


class FeatureRuntimeError(RuntimeError):
    """The configured extension environment cannot safely execute a plan."""


class _RuntimeFeaturePackCatalog(FeaturePackCatalog):
    """Catalog that closes its public mutation surface after runtime pinning."""

    def __init__(self, is_pinned: Callable[[], bool]):
        super().__init__()
        self._is_pinned = is_pinned

    def _reject_pinned_mutation(self) -> None:
        if self._is_pinned():
            raise FeatureRuntimeError(
                "feature-pack catalog cannot be mutated after the extension environment was pinned"
            )

    def register_many(self, packs: Iterable[FeaturePackSpec]) -> None:
        entries = tuple(packs)
        with self.registration_lock:
            self._reject_pinned_mutation()
            super().register_many(entries)

    def restore_snapshot(
        self,
        snapshot: Mapping[tuple[str, str], FeaturePackSpec],
    ) -> None:
        with self.registration_lock:
            if self._is_pinned():
                current = self.snapshot()
                if set(current) != set(snapshot) or any(current[key] is not pack for key, pack in snapshot.items()):
                    self._reject_pinned_mutation()
                return
            super().restore_snapshot(snapshot)

    def restore_runtime_snapshot(
        self,
        snapshot: Mapping[tuple[str, str], FeaturePackSpec],
    ) -> None:
        """Restore a runtime-owned transaction without crossing the public guard."""

        super().restore_snapshot(snapshot)


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
    """One isolated, generic extension environment for feature scenarios."""

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
        topology_providers: Optional[TopologyProviderSource] = None,
        coordinator_factory=ClusterCoordinator,
        artifact_dir: str = "",
        extension_api_version: str = EXTENSION_API_VERSION,
    ):
        if not isinstance(discover_entry_points, bool):
            raise TypeError("discover_entry_points must be a boolean")

        self._extension_api_version = extension_api_version
        self._lifecycle_lock = threading.RLock()
        self._lifecycle_owner: Optional[int] = None
        self._extension_pin: Optional[_ExtensionSnapshot] = None

        registries = {kind: Registry[Any](kind, api_version=extension_api_version) for kind in ComponentKind}
        self._registries: Mapping[ComponentKind, Registry[Any]] = MappingProxyType(registries)
        self._catalog = _RuntimeFeaturePackCatalog(lambda: self._extension_pin is not None)
        self._manifest_feature_packs: tuple[FeaturePackSpec, ...] = ()
        self.load_feature_pack_directories(feature_pack_directories)
        self._artifact_providers = ArtifactProviderRegistry(api_version=extension_api_version)
        self._resource_providers = ResourceProviderRegistry(api_version=extension_api_version)
        artifact_root = normalize_artifact_root(artifact_dir)
        self._environment_manager = EnvironmentManager(
            artifact_registry=self._artifact_providers,
            resource_registry=self._resource_providers,
            artifact_root=artifact_root,
        )

        if topology_providers is None:
            if not callable(coordinator_factory):
                raise TypeError("coordinator_factory must be callable")
            provider_entries = local_topology_provider_registrations(
                coordinator_factory,
                api_version=extension_api_version,
            )
        elif isinstance(topology_providers, TopologyProviderRegistry):
            provider_entries = topology_providers
        else:
            if not isinstance(topology_providers, Mapping):
                raise TypeError("topology_providers must be a mapping, TopologyProviderRegistry, or None")
            provider_entries = dict(topology_providers)
            for topology, registration in provider_entries.items():
                if not isinstance(registration, TopologyProviderRegistration):
                    raise TypeError(
                        f"FeatureRuntime topology provider {topology!r} must be "
                        "a TopologyProviderRegistration with explicit replay "
                        "identity metadata"
                    )
        self._runner = FeatureRunner(
            self._registries,
            provider_entries,
            artifact_dir=artifact_root,
            environment_manager=self._environment_manager,
        )
        self._topology_providers = self._runner.topology_provider_registry
        self._compiler = FeaturePackCompiler(
            self._catalog,
            self._registries,
            reference_inventory=reference_inventory,
            artifact_registry=self._artifact_providers,
            resource_registry=self._resource_providers,
            topology_provider_registry=self._topology_providers,
            extension_api_version=extension_api_version,
        )
        self._plugins = (
            discover_plugins(
                self._registries,
                group=plugin_group,
                feature_pack_catalog=self._catalog,
                artifact_provider_registry=self._artifact_providers,
                resource_provider_registry=self._resource_providers,
                topology_provider_registry=self._topology_providers,
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
    def artifact_providers(self) -> ArtifactProviderRegistry:
        return self._artifact_providers

    @property
    def resource_providers(self) -> ResourceProviderRegistry:
        return self._resource_providers

    @property
    def plugins(self) -> tuple[PluginManifest, ...]:
        return self._plugins

    @property
    def manifest_feature_packs(self) -> tuple[FeaturePackSpec, ...]:
        return self._manifest_feature_packs

    @property
    def topology_providers(self) -> TopologyProviderRegistry:
        return self._topology_providers

    def _ordered_extension_participants(self) -> tuple[Any, ...]:
        component_registries = tuple(
            self._registries[kind] for kind in sorted(self._registries, key=lambda item: item.value)
        )
        return (
            *component_registries,
            self._artifact_providers,
            self._resource_providers,
            self._catalog,
        )

    def _extension_snapshot(self) -> _ExtensionSnapshot:
        return (
            MappingProxyType({kind: registry.snapshot() for kind, registry in self._registries.items()}),
            self._artifact_providers.snapshot(),
            self._resource_providers.snapshot(),
            self._topology_providers.snapshot(),
            self._catalog.snapshot(),
        )

    @staticmethod
    def _snapshot_mapping_changed(expected: Mapping[Any, Any], current: Mapping[Any, Any]) -> bool:
        return set(current) != set(expected) or any(
            current[entry_id] is not pinned_entry for entry_id, pinned_entry in expected.items()
        )

    def _extension_drift(self, expected: _ExtensionSnapshot) -> tuple[str, ...]:
        (
            expected_components,
            expected_artifacts,
            expected_resources,
            expected_topologies,
            expected_catalog,
        ) = expected
        changed = []
        for kind, registry in self._registries.items():
            if registry.kind is not kind or registry.api_version != self._extension_api_version:
                raise FeatureRuntimeError(
                    f"component registry metadata for {kind.value!r} changed after runtime construction"
                )
            current = registry.snapshot()
            pinned = expected_components[kind]
            if self._snapshot_mapping_changed(pinned, current):
                changed.append(f"{kind.value} component registry")
        for label, registry, pinned in (
            ("artifact provider", self._artifact_providers, expected_artifacts),
            ("resource provider", self._resource_providers, expected_resources),
            ("topology provider", self._topology_providers, expected_topologies),
        ):
            if registry.api_version != self._extension_api_version:
                raise FeatureRuntimeError(f"{label} registry metadata changed after runtime construction")
            current = registry.snapshot()
            if self._snapshot_mapping_changed(pinned, current):
                changed.append(f"{label} registry")
        if self._snapshot_mapping_changed(expected_catalog, self._catalog.snapshot()):
            changed.append("feature-pack catalog")
        return tuple(changed)

    def _restore_extension_snapshot(self, snapshot: _ExtensionSnapshot) -> tuple[BaseException, ...]:
        components, artifacts, resources, topologies, catalog = snapshot
        restore_operations = [
            (self._catalog.restore_runtime_snapshot, catalog),
            (self._topology_providers.restore_runtime_snapshot, topologies),
            (self._resource_providers.restore_snapshot, resources),
            (self._artifact_providers.restore_snapshot, artifacts),
        ]
        restore_operations.extend(
            (self._registries[kind].restore_snapshot, components[kind])
            for kind in sorted(self._registries, key=lambda item: item.value, reverse=True)
        )

        failures = []
        for restore, participant_snapshot in restore_operations:
            try:
                restore(participant_snapshot)
            except BaseException as exc:  # Restoration must never mask the operation's exception.
                failures.append(exc)
        return tuple(failures)

    @staticmethod
    def _drift_error(changed: Sequence[str], *, when: str) -> FeatureRuntimeError:
        participants = ", ".join(changed)
        return FeatureRuntimeError(
            f"{participants} changed {when}; the extension environment was restored to the expected snapshot"
        )

    @contextmanager
    def _extension_locks(self):
        with ExitStack() as locks:
            for participant in self._ordered_extension_participants():
                locks.enter_context(participant.registration_lock)
            yield

    @contextmanager
    def _extension_operation(self):
        """Run one operation against a pinned snapshot of the extension environment.

        Registration locks are held only while the snapshot is captured or
        checked, never while compiling, validating, or running: components and
        providers may resolve registrations from their own threads. Changes made
        during the operation are detected afterwards, rolled back, and reported.
        """
        with self._lifecycle_operation(), ExitStack() as pins:
            with self._extension_locks():
                if self._extension_pin is None:
                    expected = self._extension_snapshot()
                else:
                    expected = self._extension_pin
                    changed = self._extension_drift(expected)
                    if changed:
                        error = self._drift_error(changed, when="after the extension environment was pinned")
                        restore_failures = self._restore_extension_snapshot(expected)
                        if restore_failures:
                            raise FeatureRuntimeError(
                                f"{error}; restoring the pinned extension environment also failed"
                            ) from restore_failures[0]
                        raise error

                try:
                    pins.enter_context(self._topology_providers.pin_snapshot(expected[3]))
                except BaseException as exc:
                    restore_failures = self._restore_extension_snapshot(expected)
                    if restore_failures:
                        raise FeatureRuntimeError(
                            "topology provider registry could not be pinned and "
                            "restoring the extension environment also failed"
                        ) from restore_failures[0]
                    raise FeatureRuntimeError(
                        "topology provider registry changed while the runtime operation was starting"
                    ) from exc

            try:
                yield
            except BaseException:
                with self._extension_locks():
                    self._restore_extension_snapshot(expected)
                raise

            with self._extension_locks():
                changed = self._extension_drift(expected)
                if changed:
                    error = self._drift_error(changed, when="during the runtime operation")
                    restore_failures = self._restore_extension_snapshot(expected)
                    if restore_failures:
                        raise FeatureRuntimeError(
                            f"{error}; restoring the extension environment also failed"
                        ) from restore_failures[0]
                    raise error
                if self._extension_pin is None:
                    self._extension_pin = expected

    def _require_plan(self, plan: RunPlan) -> None:
        if not isinstance(plan, RunPlan):
            raise TypeError("plan must be a RunPlan")
        if plan.topology_provider.api_version != self._extension_api_version:
            raise FeatureRuntimeError("run plan topology provider API does not match the runtime extension API")

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

        with self._lifecycle_lock:
            if self._lifecycle_owner is not None:
                raise FeatureRuntimeError(
                    "feature-pack directories cannot be loaded during runtime compilation, validation, or execution"
                )
            if self._extension_pin is not None:
                raise FeatureRuntimeError(
                    "feature-pack directories cannot be loaded after the extension environment was pinned"
                )

            staging_catalog = FeaturePackCatalog()
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
        """Compile a scenario after applying detached topology overrides."""

        if not isinstance(scenario, Mapping):
            raise TypeError("scenario must be a mapping")
        with self._extension_operation():
            plan = self._compiler.compile(_with_topology_overrides(scenario, topology_overrides))
            return plan

    def compile_yaml(
        self,
        source: YamlSource,
        *,
        source_name: str = "<yaml>",
        topology_overrides: Optional[Mapping[str, Any]] = None,
    ) -> RunPlan:
        """Strict-load and compile one duplicate-free YAML scenario."""

        scenario = load_scenario_yaml(source, source_name=source_name)
        return self.compile(scenario, topology_overrides=topology_overrides)

    def validate_plan(self, plan: RunPlan) -> None:
        """Structurally validate a compiled plan without provisioning."""

        self._require_plan(plan)
        with self._extension_operation():
            validate_run_plan_environment(
                plan,
                artifact_registry=self._artifact_providers,
                resource_registry=self._resource_providers,
            )
            self._runner.validate(plan)

    def run_plan(
        self,
        plan: RunPlan,
        *,
        deadline: Optional[float] = None,
    ) -> RunOutcome:
        self._require_plan(plan)
        with self._extension_operation():
            validate_run_plan_environment(
                plan,
                artifact_registry=self._artifact_providers,
                resource_registry=self._resource_providers,
            )
            outcome = self._runner.run(
                plan,
                deadline=deadline,
                _raise_preflight_errors=True,
            )
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
