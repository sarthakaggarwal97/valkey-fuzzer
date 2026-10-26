"""Valkey component library, providers, and feature packs for valkey-fuzzer.

This package is an ordinary plugin: it imports only the public contracts in
:mod:`valkey_fuzzer.extensions` and registers through the
``valkey_fuzzer.plugins`` entry point, exactly as an external extension would.
Its feature packs are the data-only YAML manifests under ``packs/``.
"""

from pathlib import Path
from typing import Any

from valkey_fuzzer.extensions import (
    EXTENSION_API_VERSION,
    ComponentKind,
    ComponentRegistration,
    PluginManifest,
    load_feature_pack_yaml,
)

from . import actions, capabilities, faults, network, observers, oracles, storage, workloads
from .providers import ARTIFACT_PROVIDERS, RESOURCE_PROVIDERS

PLUGIN_ID = "valkey-fuzzer.valkey-packs"
PACK_DIRECTORY = Path(__file__).resolve().parent / "packs"
_MODULES = (capabilities, storage, workloads, actions, faults, network, observers, oracles)


def component_registrations() -> tuple[ComponentRegistration[Any], ...]:
    return tuple(
        ComponentRegistration(
            component_id=component_id,
            kind=ComponentKind(kind),
            api_version=EXTENSION_API_VERSION,
            factory=factory,
            validator=validator,
        )
        for module in _MODULES
        for component_id, kind, factory, validator in module.REGISTRATIONS
    )


def feature_packs() -> tuple[Any, ...]:
    return tuple(
        load_feature_pack_yaml(path.read_text(encoding="utf-8"), source_name=str(path))
        for path in sorted(PACK_DIRECTORY.glob("*.yaml"))
    )


PLUGIN = PluginManifest(
    plugin_id=PLUGIN_ID,
    api_version=EXTENSION_API_VERSION,
    registrations=component_registrations(),
    feature_packs=feature_packs(),
    artifact_providers=ARTIFACT_PROVIDERS,
    resource_providers=RESOURCE_PROVIDERS,
    metadata={"description": "Valkey workloads, faults, observers, oracles, and feature packs"},
)

__all__ = ["PACK_DIRECTORY", "PLUGIN", "PLUGIN_ID", "component_registrations", "feature_packs"]
