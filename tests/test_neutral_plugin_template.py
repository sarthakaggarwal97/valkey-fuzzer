"""The external extension template: it imports only the public facade, and its installed wheel validates and
runs through the unchanged CLI."""

import ast
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from valkey_fuzzer import extensions

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_ROOT = REPOSITORY_ROOT / "examples" / "extensions" / "neutral-plugin"
EXPECTED_MESSAGE = "configuration-reached-the-plugin"
HOST_INTERNALS = {
    "ArtifactProviderRegistry", "DeploymentProviderRegistry", "ExtensionCatalog", "OutcomeStatus",
    "PluginDiscoveryError", "ComponentRegistry", "ResourceProviderRegistry", "RunOutcome", "canonical_json",
    "discover_plugins", "execution_plan_from_data", "run_outcome_from_data", "to_plain_data",
}  # fmt: skip


def test_the_template_imports_only_the_public_facade_which_exports_nothing_host_internal():
    source = TEMPLATE_ROOT / "src" / "valkey_fuzzer_neutral_plugin" / "__init__.py"
    imported = set()
    for node in ast.walk(ast.parse(source.read_text(encoding="utf-8"))):
        if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("valkey_fuzzer"):
            imported.add(node.module)
        elif isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names if alias.name.startswith("valkey_fuzzer"))

    assert imported == {"valkey_fuzzer.extensions"}
    assert all(getattr(extensions, name) is not None for name in extensions.__all__)
    assert HOST_INTERNALS.isdisjoint(extensions.__all__)


def _ok(command, cwd, environment=None):
    result = subprocess.run(command, cwd=cwd, env=environment, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    return result


def _offline_build_interpreter():
    for candidate in dict.fromkeys(filter(None, (sys.executable, shutil.which("python3"), shutil.which("python")))):
        probe = [candidate, "-c", "import setuptools.build_meta, wheel"]
        if subprocess.run(probe, capture_output=True, timeout=60).returncode == 0:
            return candidate
    raise AssertionError("no local Python interpreter provides the offline setuptools and wheel build dependencies")


def test_installed_template_wheel_validates_and_runs_through_the_unchanged_cli(tmp_path):
    source, wheelhouse, installed, report = (tmp_path / name for name in ("plugin", "wheels", "installed", "r.json"))
    shutil.copytree(TEMPLATE_ROOT, source)
    pip = ["-m", "pip", "--disable-pip-version-check"]
    build = [_offline_build_interpreter(), *pip, "wheel", "--no-deps", "--no-build-isolation", "-w", str(wheelhouse)]
    _ok([*build, str(source)], tmp_path)
    (wheel,) = wheelhouse.glob("valkey_fuzzer_neutral_plugin-*.whl")
    _ok([sys.executable, *pip, "install", "--no-deps", "--target", str(installed), str(wheel)], tmp_path)
    path = os.pathsep.join(filter(None, (str(installed), os.environ.get("PYTHONPATH"))))
    environment, cli = {**os.environ, "PYTHONPATH": path}, [sys.executable, "-m", "valkey_fuzzer.cli"]

    validate = _ok([*cli, "validate", str(source / "scenario.yaml")], REPOSITORY_ROOT, environment)
    assert "Scenario passed structural validation!" in validate.stdout
    run = [*cli, "run", str(source / "scenario.yaml"), "--artifact-dir", str(tmp_path / "a"), "--output", str(report)]
    _ok([*run, "--format", "json"], REPOSITORY_ROOT, environment)

    result = json.loads(report.read_text(encoding="utf-8"))["outcome"]
    assert result["status"] == "PASS"
    action, oracle = result["component_results"]
    assert action["evidence"][0]["value"] == {"message": EXPECTED_MESSAGE, "deployment": "example-neutral"}
    assert (oracle["component_id"], oracle["status"]) == ("verify", "PASS")
