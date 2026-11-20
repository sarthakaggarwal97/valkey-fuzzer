"""Spec bundles ship in the package (``formal/`` is mapped to ``valkey_fuzzer/formal``) and are found by name."""

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from setuptools import find_packages

from valkey_fuzzer.spec.bundle import SpecBundleError, find_bundle

ROOT = Path(__file__).resolve().parents[1]


def test_pyproject_lists_every_package_and_the_bundles():
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    block = re.search(r"^\[tool\.setuptools\]\npackages = \[(.*?)\]", text, flags=re.MULTILINE | re.DOTALL)
    listed = sorted(re.findall(r'"([^"]+)"', block.group(1)))
    assert listed == sorted([*find_packages(where=str(ROOT), include=["valkey_fuzzer*"]), "valkey_fuzzer.formal"])


def test_the_data_files_of_the_components_ship_with_them():
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    shipped = re.search(r'^"valkey_fuzzer\.components" = \[(.*)\]$', text, flags=re.MULTILINE).group(1)
    patterns = re.findall(r'"([^"]+)"', shipped)
    components = ROOT / "valkey_fuzzer" / "components"
    data = [path for path in components.rglob("*") if path.is_file() and path.suffix in (".yaml", ".json")]
    assert len(data) > 20
    for path in data:
        assert any(path.match(pattern) for pattern in patterns), path


def test_the_syscall_fault_library_source_ships_with_the_deployment():
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    shipped = re.search(r'^"valkey_fuzzer\.deployment\.local" = \[(.*)\]$', text, flags=re.MULTILINE).group(1)
    sources = [path.name for path in (ROOT / "valkey_fuzzer" / "deployment" / "local").glob("*.c")]
    assert sources == ["syscall_faults.c"] and re.findall(r'"([^"]+)"', shipped) == sources


def test_bundles_are_found_by_directory_or_by_protocol_name_on_the_spec_path(tmp_path, monkeypatch):
    fixtures = ROOT / "tests" / "fixtures" / "spec"
    monkeypatch.delenv("VALKEY_FUZZER_SPEC_PATH", raising=False)
    assert find_bundle(str(fixtures / "register")) == fixtures / "register"
    assert find_bundle("cluster-election") == ROOT / "formal" / "cluster-election"
    with pytest.raises(SpecBundleError, match=r"no spec bundle named 'register'"):
        find_bundle("register")
    with pytest.raises(SpecBundleError, match="no spec bundle at"):
        find_bundle(str(tmp_path / "missing"))
    # The search path comes before the shipped bundles.
    shutil.copytree(fixtures / "register", tmp_path / "cluster-election")
    monkeypatch.setenv("VALKEY_FUZZER_SPEC_PATH", f"{tmp_path}{os.pathsep}{fixtures}")
    assert find_bundle("register") == fixtures / "register"
    assert find_bundle("cluster-election") == tmp_path / "cluster-election"
    # A protocol name is never a directory of the current one; a path is.
    monkeypatch.delenv("VALKEY_FUZZER_SPEC_PATH")
    monkeypatch.chdir(tmp_path)
    assert find_bundle("cluster-election") == ROOT / "formal" / "cluster-election"
    assert find_bundle("./cluster-election") == tmp_path / "cluster-election"


def test_a_base_install_needs_java_and_the_jar_only_when_a_spec_tool_runs(tmp_path):
    # With no java on PATH and no jar, the CLI builds, the batch package loads, and a scenario naming a bundle
    # compiles (the spec bridge imports plainly; only SANY and TLC need the tools). A spec command then exits 2
    # and says what is missing and how to get the jar.
    loaded = """
import sys
from valkey_fuzzer import batch
from valkey_fuzzer.cli import create_parser
from valkey_fuzzer.spec.tla import TOOLS_HINT
import yaml
from tests.valkey_experiments_support import valkey_service
parser = create_parser()
with open("examples/valkey/cluster_election_durability.yaml") as handle:
    plan = valkey_service().compile(yaml.safe_load(handle))
assert any(component.config.get("spec") == "cluster-election" for component in plan.components)
args = parser.parse_args(["spec", "check", "cluster-election"])
assert args.handler(args) == 2
print(TOOLS_HINT)
"""
    environment = {key: value for key, value in os.environ.items() if key != "TLA2TOOLS_JAR"}
    environment["PATH"] = str(tmp_path)  # a directory without java
    result = subprocess.run(
        [sys.executable, "-c", loaded], cwd=ROOT, env=environment, capture_output=True, text=True, check=True
    )
    hint = result.stdout.strip()
    assert result.stderr.strip() == f"Error: tla2tools.jar not found and java not found on PATH: {hint}"
