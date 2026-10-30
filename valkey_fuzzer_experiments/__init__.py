"""Valkey component library, providers, and experiments for valkey-fuzzer.

This package is an ordinary plugin: it imports only the public contracts in
:mod:`valkey_fuzzer.extensions` and registers through the
``valkey_fuzzer.plugins`` entry point, exactly as an external extension would.
Its experiments are the data-only YAML documents under ``experiments/``; they
extend the fragments under ``fragments/`` (``valkey.writes``, ``valkey.base``,
the procedures they share, such as ``valkey.psync``), and every experiment
extends ``valkey.liveness``, which adds the ``valkey.oracle.liveness`` oracle.
Data-only experiments elsewhere can ``extends`` the same fragments.
"""

import importlib
from pathlib import Path
from typing import Any

from valkey_fuzzer.extensions import (
    ComponentRegistration,
    PluginManifest,
    load_experiment_yaml,
    load_yaml_mapping,
)

from .providers import ARTIFACT_PROVIDERS, RESOURCE_PROVIDERS

PLUGIN_ID = "valkey-fuzzer.valkey-experiments"
EXPERIMENT_DIRECTORY = Path(__file__).resolve().parent / "experiments"
FRAGMENT_DIRECTORY = Path(__file__).resolve().parent / "fragments"
# The component modules, in registration order.
_MODULES = tuple(
    importlib.import_module(f"{__name__}.{name}")
    for name in (
        "capabilities",
        "storage",
        "workloads",
        "actions",
        "faults",
        "network",
        "observers",
        "oracles",
        "ledger",
        "snapshots",
        "compression",
        "cluster",
    )
)


def component_registrations() -> tuple[ComponentRegistration[Any], ...]:
    """Every module's ``REGISTRATIONS``, in module order."""

    return tuple(registration for module in _MODULES for registration in module.REGISTRATIONS)


def experiment_fragments() -> dict[str, Any]:
    """``{name: fragment}`` for every ``fragments/<name>.yaml``."""

    return {
        path.stem: load_yaml_mapping(path.read_text(encoding="utf-8"), source_name=str(path))
        for path in sorted(FRAGMENT_DIRECTORY.glob("*.yaml"))
    }


def experiments() -> tuple[Any, ...]:
    fragments = experiment_fragments()
    return tuple(
        load_experiment_yaml(
            path.read_text(encoding="utf-8"),
            source_name=str(path),
            fragments=fragments,
            default_extends=("valkey.liveness",),
        )
        for path in sorted(EXPERIMENT_DIRECTORY.glob("*.yaml"))
    )


PLUGIN = PluginManifest(
    plugin_id=PLUGIN_ID,
    registrations=component_registrations(),
    experiments=experiments(),
    artifact_providers=ARTIFACT_PROVIDERS,
    resource_providers=RESOURCE_PROVIDERS,
    experiment_fragments=experiment_fragments(),
    metadata={"description": "Valkey workloads, faults, observers, oracles, and experiments"},
)

__all__ = [
    "EXPERIMENT_DIRECTORY",
    "FRAGMENT_DIRECTORY",
    "PLUGIN",
    "PLUGIN_ID",
    "component_registrations",
    "experiment_fragments",
    "experiments",
]
