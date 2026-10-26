"""Build runtimes that discover exactly the in-repo Valkey packs plugin.

Tests must not depend on whether the package is installed, so discovery is
pointed at a fake entry point that loads ``valkey_fuzzer_packs.PLUGIN``.
"""

from types import SimpleNamespace
from unittest.mock import patch

from valkey_fuzzer.feature_runtime import FeatureRuntime
from valkey_fuzzer_packs import PLUGIN


class _EntryPoint:
    name = PLUGIN.plugin_id
    value = "valkey_fuzzer_packs:PLUGIN"
    dist = SimpleNamespace(name="valkey-fuzzer")

    @staticmethod
    def load():
        return PLUGIN


class _EntryPoints:
    @staticmethod
    def select(*, group):
        return (_EntryPoint(),) if group == "valkey_fuzzer.plugins" else ()


def valkey_entry_points():
    """Patch plugin discovery so it finds only the Valkey packs plugin."""

    return patch("valkey_fuzzer.extensibility.importlib_metadata.entry_points", lambda: _EntryPoints())


def valkey_runtime(**kwargs) -> FeatureRuntime:
    with valkey_entry_points():
        return FeatureRuntime(**kwargs)
