"""Installed extensions: experiments, component, generator, and provider registries, plugins, and fragments.

:class:`ExtensionCatalog` owns everything one run service registers. Data-only
experiment documents are parsed here (fragments merged in through
``extends``); plugins are discovered through entry points and registered
atomically, so a conflicting plugin leaves the catalog unchanged.
"""

import importlib.util
import re
from collections.abc import Iterable, Mapping, Sequence
from importlib import metadata as importlib_metadata
from os import PathLike
from pathlib import Path
from types import MappingProxyType
from typing import Any, Optional, TextIO, Union

from ._data import (
    BaseRegistry,
    DuplicateRegistrationError,
    FrozenRegistryError,
    RegistrationError,
    as_tuple,
    merge,
)
from .contracts import (
    PLUGIN_ENTRY_POINT_GROUP,
    ComponentRegistration,
    ComponentSpec,
    DeploymentProviderRegistration,
    EvidenceRequirement,
    ExperimentSpec,
    GeneratorRegistration,
    InstanceSelector,
    PluginManifest,
    StartupOverlay,
)
from .deployment.local.provider import local_deployment_provider_registrations
from .environment import ArtifactProviderRegistry, ResourceProviderRegistry
from .schema import (
    ENVIRONMENT_NAMESPACES,
    ConfigError,
    ConfigSchema,
    ExperimentDocumentError,
    ExperimentError,
    build_document_model,
    document_fields,
    document_mapping,
    load_yaml,
)

ExperimentDirectory = Union[str, "PathLike[str]"]


class DuplicateExperimentError(ExperimentError):
    def __init__(self, experiment_id: str):
        self.experiment_id = experiment_id
        super().__init__(f"experiment {experiment_id!r} is already registered")


class UnknownExperimentError(ExperimentError):
    def __init__(self, experiment_id: str, hint: Optional[str] = None):
        self.experiment_id = experiment_id
        self.hint = hint
        super().__init__(f"experiment {experiment_id!r} is not registered" + (f"; {hint}" if hint else ""))


# ---------------------------------------------------------------------------
# Experiment documents
# ---------------------------------------------------------------------------

_COMPONENT_FIELDS = ("use", "id", "config", "required", "depends_on", "timeout_s")
_EXPERIMENT_FIELDS = (
    "extends",
    "id",
    "mode",
    "deployment",
    "components",
    "inputs",
    "startup_overlays",
    "required_events",
    "required_evidence",
    "metadata",
)


def _parse_component(value: Any, location: str) -> ComponentSpec:
    """One ``{use, id, ...}`` entry; the role comes from the registration at compile time."""

    mapping = dict(document_fields(value, location, allowed=_COMPONENT_FIELDS, required=("use", "id")))
    return build_document_model(ComponentSpec, location, use=mapping.pop("use"), id=mapping.pop("id"), **mapping)


def _parse_deployment(value: Any, location: str) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """``{modes: [...], <field>: <rule>, ...}``: the supported modes, then rules for deployment fields."""

    mapping = dict(
        document_fields(
            value, location, allowed=tuple(value) if isinstance(value, Mapping) else (), required=("modes",)
        )
    )
    modes = as_tuple(mapping.pop("modes"), f"{location}.modes", error=ExperimentDocumentError)
    try:
        ConfigSchema.from_rules(mapping, location)
    except ConfigError as exc:
        raise ExperimentDocumentError(str(exc)) from None
    return modes, mapping


def _parse_overlay(value: Any, location: str) -> StartupOverlay:
    mapping = dict(
        document_fields(
            value,
            location,
            allowed=("overlay_id", "selector", "server_config", "conflict_policy"),
            required=("overlay_id",),
        )
    )
    try:
        selector = InstanceSelector.parse(mapping.pop("selector", {}))
    except (TypeError, ValueError) as exc:
        raise ExperimentDocumentError(f"{location}.selector: {exc}") from exc
    return build_document_model(StartupOverlay, location, selector=selector, **mapping)


def _parse_requirement(value: Any, location: str) -> EvidenceRequirement:
    mapping = document_fields(
        value, location, allowed=("evidence_id", "source", "min_count"), required=("evidence_id",)
    )
    return build_document_model(EvidenceRequirement, location, **mapping)


def _merge_by_id(base: Sequence[Any], overlay: Sequence[Any], key: str, location: str) -> list[Any]:
    """Merge two lists of mappings by ``key``.

    The overlay's entries come first, in its order; an entry whose ID is also
    in ``base`` deep-merges onto it. Remaining ``base`` entries follow, so the
    experiment author's ordering wins and fragments fill in after it.
    """

    by_id = {item.get(key): item for item in base if isinstance(item, Mapping)}
    merged = []
    for item in overlay:
        if not isinstance(item, Mapping) or key not in item:
            raise ExperimentDocumentError(f"{location} entries must be mappings with {key!r}")
        merged.append(merge(by_id.pop(item[key]), item) if item[key] in by_id else dict(item))
    return merged + [dict(item) for item in base if isinstance(item, Mapping) and item.get(key) in by_id]


def _union(base: Sequence[Any], overlay: Sequence[Any]) -> list[Any]:
    merged = list(base)
    for item in overlay:
        if item not in merged:
            merged.append(item)
    return merged


def merge_experiment_documents(base: Mapping[str, Any], overlay: Mapping[str, Any]) -> dict[str, Any]:
    """Merge an experiment (or fragment) ``overlay`` onto ``base``.

    ``components`` and ``startup_overlays`` merge by ID (a same-ID entry
    deep-merges, so an experiment can override one config field of a
    fragment's component, and the overlay's order comes first); ``inputs``
    and ``metadata`` deep-merge; ``required_events``, ``required_evidence``,
    and ``inputs.required`` are unions; every other field is replaced.
    """

    merged = dict(base)
    for key, value in overlay.items():
        if key == "components":
            merged[key] = _merge_by_id(
                base.get(key, ()), as_tuple(value, key, error=ExperimentDocumentError), "id", key
            )
        elif key == "startup_overlays":
            merged[key] = _merge_by_id(
                base.get(key, ()), as_tuple(value, key, error=ExperimentDocumentError), "overlay_id", key
            )
        elif key in ("required_events", "required_evidence"):
            merged[key] = _union(base.get(key, ()), as_tuple(value, key, error=ExperimentDocumentError))
        elif key in ("inputs", "metadata"):
            merged[key] = merge(document_mapping(base.get(key, {}), key), document_mapping(value, key))
            if key == "inputs" and "required" in base.get(key, {}) and "required" in value:
                merged[key]["required"] = _union(
                    base[key]["required"], as_tuple(value["required"], key, error=ExperimentDocumentError)
                )
        else:
            merged[key] = value
    return merged


def apply_fragments(
    value: Mapping[str, Any],
    fragments: Mapping[str, Mapping[str, Any]],
    location: str = "experiment",
    _seen: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Resolve ``extends: [fragment, ...]`` by merging each fragment, then the document itself."""

    document = dict(document_mapping(value, location))
    names = document.pop("extends", ())
    names = (
        [names]
        if isinstance(names, str)
        else list(as_tuple(names, f"{location}.extends", error=ExperimentDocumentError))
    )
    merged: dict[str, Any] = {}
    for name in names:
        if name in _seen:
            raise ExperimentDocumentError(f"{location} has a fragment cycle: {' -> '.join((*_seen, name))}")
        if name not in fragments:
            raise ExperimentDocumentError(f"{location} extends unknown fragment {name!r}")
        fragment = apply_fragments(fragments[name], fragments, f"fragment {name!r}", (*_seen, name))
        unknown = sorted(set(fragment) - set(_EXPERIMENT_FIELDS))
        if unknown:
            raise ExperimentDocumentError(f"fragment {name!r} contains unknown fields: {unknown}")
        merged = merge_experiment_documents(merged, fragment)
    return merge_experiment_documents(merged, document)


def parse_experiment_document(
    value: Mapping[str, Any],
    *,
    fragments: Optional[Mapping[str, Mapping[str, Any]]] = None,
    default_extends: Sequence[str] = (),
) -> ExperimentSpec:
    """Parse one strict, data-only experiment mapping into an immutable spec.

    ``extends`` names fragments from ``fragments`` to merge first (see
    :func:`merge_experiment_documents`); ``default_extends`` are merged before
    them, which lets a plugin give every experiment it ships a common base.
    """

    document = dict(document_mapping(value, "experiment"))
    if default_extends:
        own = document.get("extends", ())
        own = [own] if isinstance(own, str) else list(as_tuple(own, "extends", error=ExperimentDocumentError))
        document["extends"] = [*default_extends, *own]
    merged = apply_fragments(document, fragments or {})
    # Fragments' metadata.generator only fills in the block of an experiment
    # that declares one: declaring it is how an experiment opts in to random scenarios.
    own_metadata = document.get("metadata")
    if not (isinstance(own_metadata, Mapping) and "generator" in own_metadata):
        if isinstance(merged.get("metadata"), Mapping) and "generator" in merged["metadata"]:
            merged["metadata"] = {key: item for key, item in merged["metadata"].items() if key != "generator"}
    mapping = dict(
        document_fields(
            merged,
            "experiment",
            allowed=_EXPERIMENT_FIELDS,
            required=("id", "deployment", "components"),
        )
    )
    mapping["experiment_id"] = mapping.pop("id")
    mapping["deployment_modes"], mapping["deployment_requirements"] = _parse_deployment(
        mapping.pop("deployment"), "deployment"
    )
    mapping["components"] = tuple(
        _parse_component(item, f"components[{index}]")
        for index, item in enumerate(as_tuple(mapping["components"], "components", error=ExperimentDocumentError))
    )
    mapping["startup_overlays"] = tuple(
        _parse_overlay(item, f"startup_overlays[{index}]")
        for index, item in enumerate(
            as_tuple(mapping.get("startup_overlays", ()), "startup_overlays", error=ExperimentDocumentError)
        )
    )
    mapping["required_evidence"] = tuple(
        _parse_requirement(item, f"required_evidence[{index}]")
        for index, item in enumerate(
            as_tuple(mapping.get("required_evidence", ()), "required_evidence", error=ExperimentDocumentError)
        )
    )
    _reject_environment_references(mapping.get("metadata", {}), "metadata")
    return build_document_model(ExperimentSpec, "experiment", **mapping)


def _reject_environment_references(value: Any, location: str) -> None:
    """Persisted experiment data such as metadata cannot bind run-time environment outputs."""

    if isinstance(value, Mapping):
        reference = value.get("$ref")
        if isinstance(reference, str) and reference.split(".", 1)[0] in ENVIRONMENT_NAMESPACES:
            raise ExperimentDocumentError(f"{location} must not contain environment references")
        for key, item in value.items():
            _reject_environment_references(item, f"{location}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _reject_environment_references(item, f"{location}[{index}]")


def load_experiment_yaml(
    source: Union[str, bytes, TextIO],
    *,
    source_name: str = "<yaml>",
    fragments: Optional[Mapping[str, Mapping[str, Any]]] = None,
    default_extends: Sequence[str] = (),
) -> ExperimentSpec:
    """Parse one YAML experiment document (``source`` is content, not a path)."""

    value = load_yaml(source, source_name)
    try:
        return parse_experiment_document(value, fragments=fragments, default_extends=default_extends)
    except ExperimentError as exc:
        raise ExperimentDocumentError(f"{source_name}: {exc}") from exc


class ExperimentRegistry(BaseRegistry[ExperimentSpec]):
    """Installed experiments, keyed by ``experiment_id``.

    ``hint`` says why an experiment may be missing (see :func:`build_catalog`);
    an unknown experiment's error carries it.
    """

    label = "experiment"
    entry_type = ExperimentSpec

    def __init__(self) -> None:
        super().__init__()
        self.hint: Optional[str] = None

    def key(self, entry: ExperimentSpec) -> str:
        return entry.experiment_id

    def validate_many(self, entries: Any) -> tuple[ExperimentSpec, ...]:
        try:
            return super().validate_many(entries)
        except DuplicateRegistrationError as exc:
            raise DuplicateExperimentError(exc.key) from exc

    def resolve(self, experiment_id: str) -> ExperimentSpec:
        experiment = self.get(experiment_id)
        if experiment is None:
            raise UnknownExperimentError(experiment_id, hint=self.hint)
        return experiment


def load_experiment_directory(
    directory: ExperimentDirectory, *, fragments: Optional[Mapping[str, Mapping[str, Any]]] = None
) -> tuple[ExperimentSpec, ...]:
    """Parse the immediate ``.yaml``/``.yml`` experiments of ``directory`` in name order, registering nothing."""

    path = Path(directory)
    if not path.is_dir():
        raise ExperimentDocumentError(f"experiment directory does not exist: {path}")
    files = sorted(
        (item for item in path.iterdir() if item.is_file() and item.suffix.lower() in (".yaml", ".yml")),
        key=lambda item: (item.name.casefold(), item.name),
    )
    experiments = []
    for document in files:
        try:
            text = document.read_text(encoding="utf-8")
        except OSError as exc:
            raise ExperimentDocumentError(f"{document}: {exc}") from exc
        experiments.append(load_experiment_yaml(text, source_name=str(document), fragments=fragments))
    return tuple(experiments)


# ---------------------------------------------------------------------------
# Component, generator, and deployment-provider registries
# ---------------------------------------------------------------------------

# One segment of a component registration ID: lowercase words joined by hyphens.
_COMPONENT_ID_SEGMENT = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")


def _check_id(label: str, entry_id: str, kind: str) -> None:
    """Reject a registration ID that is not ``<namespace>.<kind>.<name>``."""

    segments = entry_id.split(".")
    if (
        len(segments) != 3
        or segments[1] != kind
        or not all(_COMPONENT_ID_SEGMENT.fullmatch(segment) for segment in segments)
    ):
        raise RegistrationError(
            f"{label} {entry_id!r} must be named '<namespace>.{kind}.<name>' "
            "with lowercase segments and words joined by hyphens"
        )


class ComponentRegistry(BaseRegistry[ComponentRegistration[Any]]):
    """Component registrations of every role, each named ``<namespace>.<role>.<name>`` after its role."""

    label = "component"
    entry_type = ComponentRegistration

    def key(self, entry: ComponentRegistration[Any]) -> str:
        return entry.component_id

    def check(self, entry: ComponentRegistration[Any]) -> None:
        super().check(entry)
        _check_id("component", entry.component_id, entry.role.value)


class GeneratorRegistry(BaseRegistry[GeneratorRegistration]):
    """Generator hooks, named ``<namespace>.generator.<name>``."""

    label = "generator"
    entry_type = GeneratorRegistration

    def key(self, entry: GeneratorRegistration) -> str:
        return entry.generator_id

    def check(self, entry: GeneratorRegistration) -> None:
        super().check(entry)
        _check_id("generator", entry.generator_id, "generator")


class DeploymentProviderRegistry(BaseRegistry[DeploymentProviderRegistration]):
    """Deployment providers, keyed by ``(deployment_mode, provider_id)``.

    Several providers may implement one deployment mode; a scenario selects
    one with ``deployment.provider`` and may omit it when only one is registered.
    """

    label = "deployment provider"
    entry_type = DeploymentProviderRegistration

    def key(self, entry: DeploymentProviderRegistration) -> tuple[str, str]:
        return (entry.deployment_mode, entry.provider_id)

    def provider_ids(self, deployment_mode: str) -> tuple[str, ...]:
        """IDs of the providers registered for ``deployment_mode``, sorted."""

        return tuple(provider_id for registered, provider_id in self.registered_ids if registered == deployment_mode)


# ---------------------------------------------------------------------------
# Plugins
# ---------------------------------------------------------------------------


class PluginDiscoveryError(RuntimeError):
    """An entry point could not be loaded or did not satisfy the plugin contract."""

    def __init__(self, entry_point: str, distribution: str, reason: str):
        self.entry_point = entry_point
        self.distribution = distribution
        self.reason = reason
        location = f" from {distribution!r}" if distribution else ""
        super().__init__(f"failed to discover plugin {entry_point!r}{location}: {reason}")


def _distribution_name(entry_point: Any) -> str:
    distribution = getattr(entry_point, "dist", None)
    name = getattr(distribution, "name", None)
    if not name and distribution is not None:
        try:
            name = distribution.metadata["Name"]
        except (AttributeError, KeyError, TypeError):
            name = None
    return str(name or "")


def _entry_points(group: str) -> tuple[Any, ...]:
    selected = importlib_metadata.entry_points()
    selected = selected.select(group=group) if hasattr(selected, "select") else selected.get(group, ())
    # Python 3.9 can report one distribution once per sys.path entry that
    # contains it, so keep the first of each (distribution, name, value).
    unique: dict[tuple[str, str, str], Any] = {}
    for entry_point in selected:
        distribution = re.sub(r"[-_.]+", "-", _distribution_name(entry_point)).lower()
        identity = (distribution, entry_point.name, getattr(entry_point, "value", ""))
        unique.setdefault(identity, entry_point)
    return tuple(sorted(unique.values(), key=lambda item: (_distribution_name(item).casefold(), item.name.casefold())))


def load_plugins() -> tuple[PluginManifest, ...]:
    """Load the installed plugins in deterministic order without registering them.

    An entry point resolves to a :class:`PluginManifest` or a zero-argument
    callable returning one; its ``plugin_id`` must equal the entry point name.
    """

    manifests = []
    for entry_point in _entry_points(PLUGIN_ENTRY_POINT_GROUP):
        distribution = _distribution_name(entry_point)
        try:
            loaded = entry_point.load()
            manifest = loaded if isinstance(loaded, PluginManifest) else loaded()
            if not isinstance(manifest, PluginManifest):
                raise TypeError("entry point must return a PluginManifest")
            if manifest.plugin_id != entry_point.name:
                raise ValueError(
                    f"manifest plugin_id {manifest.plugin_id!r} does not match entry point name {entry_point.name!r}"
                )
        except Exception as exc:
            raise PluginDiscoveryError(str(entry_point.name), distribution, str(exc)) from exc
        manifests.append(manifest)
    return tuple(manifests)


# ---------------------------------------------------------------------------
# The catalog
# ---------------------------------------------------------------------------


class ExtensionCatalog:
    """Everything one run service registers: components, generator hooks,
    experiments, artifact, resource, and deployment providers, plugins, and
    experiment fragments.

    Plugins and experiment directories are registered at startup, each batch
    as one transaction: a conflicting plugin or a bad experiment file leaves
    the catalog unchanged. :meth:`freeze` then makes it read-only, so
    compilation and execution always see the same extensions.
    """

    def __init__(self) -> None:
        self._components = ComponentRegistry()
        self._generators = GeneratorRegistry()
        self._experiments = ExperimentRegistry()
        self._artifact_providers = ArtifactProviderRegistry()
        self._resource_providers = ResourceProviderRegistry()
        self._deployment_providers = DeploymentProviderRegistry()
        self._plugins: tuple[PluginManifest, ...] = ()
        self._fragments: dict[str, Mapping[str, Any]] = {}
        self._fragment_owners: dict[str, str] = {}
        # ("experiment" | "component" | "generator", ID) -> {"plugin": ..., "directory": ...}
        self._origins: dict[tuple[str, str], Mapping[str, Optional[str]]] = {}
        self._frozen = False

    @property
    def components(self) -> ComponentRegistry:
        return self._components

    @property
    def generators(self) -> GeneratorRegistry:
        """The generator hooks random scenarios draw with."""

        return self._generators

    @property
    def experiments(self) -> ExperimentRegistry:
        return self._experiments

    @property
    def artifact_providers(self) -> ArtifactProviderRegistry:
        return self._artifact_providers

    @property
    def resource_providers(self) -> ResourceProviderRegistry:
        return self._resource_providers

    @property
    def deployment_providers(self) -> DeploymentProviderRegistry:
        return self._deployment_providers

    @property
    def plugins(self) -> tuple[PluginManifest, ...]:
        return self._plugins

    @property
    def fragments(self) -> Mapping[str, Mapping[str, Any]]:
        """Experiment fragments the registered plugins provide, by name."""

        return MappingProxyType(dict(self._fragments))

    @property
    def frozen(self) -> bool:
        return self._frozen

    def origin(self, kind: str, entry_id: str) -> Mapping[str, Optional[str]]:
        """Where an ``experiment``, ``component``, or ``generator`` came from: ``{plugin, directory}``.

        ``plugin`` is the ID of the plugin that registered it and
        ``directory`` the experiment directory it was loaded from; both are
        None for an entry registered on a registry directly.
        """

        return self._origins.get((kind, entry_id), {"plugin": None, "directory": None})

    def _check_open(self) -> None:
        if self._frozen:
            raise FrozenRegistryError("the extension catalog is frozen after plugin discovery")

    def register_plugins(self, manifests: Sequence[PluginManifest]) -> None:
        """Register every manifest, or nothing if any entry conflicts."""

        manifests = tuple(manifests)
        self._check_open()
        plugin_ids = [manifest.plugin_id for manifest in (*self._plugins, *manifests)]
        if len(set(plugin_ids)) != len(plugin_ids):
            raise RegistrationError(f"duplicate plugin IDs in {plugin_ids}")
        # Experiments ``extends`` fragments by name, so one plugin must not shadow another's.
        fragment_owners = dict(self._fragment_owners)
        for manifest in manifests:
            for name in manifest.experiment_fragments:
                if name in fragment_owners:
                    raise RegistrationError(
                        f"experiment fragment {name!r} is provided by both {fragment_owners[name]!r} "
                        f"and {manifest.plugin_id!r}"
                    )
                fragment_owners[name] = manifest.plugin_id
        batches: list[tuple[BaseRegistry[Any], list[Any]]] = [
            (self._components, [entry for manifest in manifests for entry in manifest.registrations]),
            (self._generators, [entry for manifest in manifests for entry in manifest.generators]),
            (self._experiments, [spec for manifest in manifests for spec in manifest.experiments]),
            (self._artifact_providers, [entry for manifest in manifests for entry in manifest.artifact_providers]),
            (self._resource_providers, [entry for manifest in manifests for entry in manifest.resource_providers]),
            (
                self._deployment_providers,
                [entry for manifest in manifests for entry in manifest.deployment_providers],
            ),
        ]
        batches = [(registry, entries) for registry, entries in batches if entries]
        # Validate everything before changing anything.
        for registry, entries in batches:
            registry.validate_many(entries)
        for registry, entries in batches:
            registry.register_many(entries)
        self._plugins += manifests
        self._fragment_owners = fragment_owners
        for manifest in manifests:
            origin = MappingProxyType({"plugin": manifest.plugin_id, "directory": None})
            self._origins.update((("component", entry.component_id), origin) for entry in manifest.registrations)
            self._origins.update((("generator", entry.generator_id), origin) for entry in manifest.generators)
            self._origins.update((("experiment", spec.experiment_id), origin) for spec in manifest.experiments)
        self._fragments.update(
            (name, fragment) for manifest in manifests for name, fragment in manifest.experiment_fragments.items()
        )

    def load_experiment_directories(self, directories: Iterable[ExperimentDirectory]) -> tuple[ExperimentSpec, ...]:
        """Parse the experiments in ``directories`` with the registered fragments, then register them together.

        A file that does not parse, or two experiments with one ID, leave the catalog unchanged.
        """

        self._check_open()
        experiments: list[ExperimentSpec] = []
        origins = {}
        for directory in directories:
            loaded = load_experiment_directory(directory, fragments=self._fragments)
            origin = MappingProxyType({"plugin": None, "directory": str(directory)})
            origins.update((("experiment", spec.experiment_id), origin) for spec in loaded)
            experiments.extend(loaded)
        self._experiments.register_many(experiments)
        self._origins.update(origins)
        return tuple(experiments)

    def freeze(self) -> None:
        """Make every registry read-only."""

        for registry in (
            self._components,
            self._generators,
            self._experiments,
            self._artifact_providers,
            self._resource_providers,
            self._deployment_providers,
        ):
            registry.freeze()
        self._frozen = True


# The Valkey experiments ship in this repository as a plugin that loads only
# through its entry point, which exists once the checkout is pip-installed.
_VALKEY_EXPERIMENTS_PACKAGE = "valkey_fuzzer_experiments"
_VALKEY_EXPERIMENTS_PLUGIN = "valkey-fuzzer.valkey-experiments"


def _undiscovered_plugin_hint(plugins: Sequence[PluginManifest], discovered: bool) -> Optional[str]:
    """Why the Valkey experiments are missing, when their package is importable but was not loaded.

    The package is located, never imported: the core does not load plugins
    except through their entry points.
    """

    if any(manifest.plugin_id == _VALKEY_EXPERIMENTS_PLUGIN for manifest in plugins):
        return None
    try:
        spec = importlib.util.find_spec(_VALKEY_EXPERIMENTS_PACKAGE)
    except (ImportError, ValueError):
        spec = None
    if spec is None:
        return None
    if not discovered:
        return f"plugins are disabled (--no-plugins), so the Valkey experiments in {_VALKEY_EXPERIMENTS_PACKAGE} were not loaded"
    location = next(iter(spec.submodule_search_locations or ()), None) or spec.origin
    return (
        f"{_VALKEY_EXPERIMENTS_PACKAGE} is importable (from {location}) but its plugin entry point "
        f"{_VALKEY_EXPERIMENTS_PLUGIN!r} is not installed, so its experiments were not loaded; "
        "install the checkout with `pip install -e .` (or install the valkey-fuzzer package)"
    )


def build_catalog(
    experiment_directories: Sequence[ExperimentDirectory] = (),
    *,
    discover_plugins: bool = True,
    plugins: Sequence[PluginManifest] = (),
    deployment_providers: Optional[Iterable[DeploymentProviderRegistration]] = None,
) -> ExtensionCatalog:
    """A frozen catalog: the deployment providers, ``plugins`` and the installed ones, then ``experiment_directories``.

    ``deployment_providers`` default to the local deployment's. Plugins
    register as one transaction, so explicit and installed plugins cannot
    shadow each other; the experiment directories load after them, since
    data-only experiments may extend the fragments plugins provide.
    """

    catalog = ExtensionCatalog()
    if deployment_providers is None:
        deployment_providers = local_deployment_provider_registrations().values()
    catalog.deployment_providers.register_many(deployment_providers)
    catalog.register_plugins((*plugins, *(load_plugins() if discover_plugins else ())))
    catalog.load_experiment_directories(experiment_directories)
    catalog.experiments.hint = _undiscovered_plugin_hint(catalog.plugins, discover_plugins)
    catalog.freeze()
    return catalog
