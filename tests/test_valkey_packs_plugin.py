"""The Valkey packs plugin onboards exactly like an external extension."""

import ast
import os
import sys
from pathlib import Path

import pytest

from tests.valkey_packs_support import valkey_runtime
from valkey_fuzzer.extensibility import ComponentKind, RunMode
from valkey_fuzzer.extensions import SupportDecision
from valkey_fuzzer_packs import PACK_DIRECTORY, PLUGIN, component_registrations
from valkey_fuzzer_packs.providers import LocalBinaryProvider, TempDirectoryProvider, TmpfsProvider, tmpfs_supported

REPOSITORY = Path(__file__).resolve().parents[1]
PLUGIN_SOURCES = sorted((REPOSITORY / "valkey_fuzzer_packs").glob("*.py"))
EXAMPLES = sorted((REPOSITORY / "examples" / "valkey").glob("*.yaml"))
SCENARIO_EXAMPLES = [path for path in EXAMPLES if "campaign" not in path.name and "differential" not in path.name]


@pytest.mark.parametrize("path", PLUGIN_SOURCES, ids=lambda path: path.name)
def test_plugin_imports_only_the_public_extension_api(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    framework_imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            if node.module.split(".")[0] == "valkey_fuzzer":
                framework_imports.add(node.module)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] == "valkey_fuzzer":
                    framework_imports.add(alias.name)
    assert framework_imports <= {"valkey_fuzzer.extensions"}


def test_manifest_registers_components_providers_and_packs():
    kinds = {}
    for registration in PLUGIN.registrations:
        kinds.setdefault(registration.kind, []).append(registration.component_id)
    assert len(PLUGIN.registrations) == len(component_registrations()) == 24
    assert set(kinds) == set(ComponentKind)
    assert [provider.provider_id for provider in PLUGIN.artifact_providers] == ["valkey.local-binary"]
    assert [provider.provider_id for provider in PLUGIN.resource_providers] == ["valkey.fs.tmpdir", "valkey.fs.tmpfs"]
    assert len(PLUGIN.feature_packs) == len(list(PACK_DIRECTORY.glob("*.yaml"))) == 11
    assert not PLUGIN.topology_providers


def test_pyproject_registers_the_plugin_entry_point():
    pyproject = (REPOSITORY / "pyproject.toml").read_text(encoding="utf-8")
    assert '[project.entry-points."valkey_fuzzer.plugins"]' in pyproject
    assert f'"{PLUGIN.plugin_id}" = "valkey_fuzzer_packs:PLUGIN"' in pyproject
    assert 'valkey_fuzzer_packs = ["packs/*.yaml"]' in pyproject


@pytest.mark.parametrize("pack", PLUGIN.feature_packs, ids=lambda pack: pack.pack_id)
def test_every_pack_is_a_feature_test_with_a_liveness_oracle(pack):
    assert pack.mode is RunMode.FEATURE
    liveness = [component for component in pack.components if component.component_id == "valkey.liveness"]
    assert len(liveness) == 1 and liveness[0].required
    assert pack.required_evidence


@pytest.fixture(scope="module")
def runtime():
    return valkey_runtime()


@pytest.mark.parametrize("path", SCENARIO_EXAMPLES, ids=lambda path: path.name)
def test_example_scenarios_compile_and_preflight(runtime, path):
    plan = runtime.compile_yaml(path.read_text(encoding="utf-8"), source_name=str(path))
    runtime.validate_plan(plan)
    assert plan.pack_id.startswith("valkey.")


@pytest.mark.parametrize(
    "pack",
    [pack for pack in PLUGIN.feature_packs if not pack.input_schema.get("required")],
    ids=lambda pack: pack.pack_id,
)
def test_packs_without_required_inputs_compile_on_every_supported_topology(runtime, pack):
    for topology in pack.supported_topologies:
        document = {"type": topology}
        if pack.pack_id == "valkey.cluster-partition":
            document["addressing"] = "per_node_loopback"
        plan = runtime.compile(
            {
                "scenario_id": "compile-check",
                "seed": 1,
                "feature": {"use": pack.pack_id, "version": pack.version},
                "topology": document,
            }
        )
        runtime.validate_plan(plan)


def test_runtime_discovers_the_plugin_and_lists_its_packs(runtime):
    assert [manifest.plugin_id for manifest in runtime.plugins] == [PLUGIN.plugin_id]
    assert ("valkey.forkless-save", "1") in runtime.catalog


# ---------------------------------------------------------------------------
# Providers
# ---------------------------------------------------------------------------


class _Spec:
    def __init__(self, provider_id, config, identifier="subject"):
        self.provider_id = provider_id
        self.config = config
        self.artifact_id = identifier
        self.resource_id = identifier


def _fake_binary(tmp_path, version="9.1.0", sha="abc12345"):
    binary = tmp_path / "valkey-server"
    binary.write_text(f"#!{sys.executable}\nprint('Valkey server v={version} sha={sha}:0 malloc=libc bits=64')\n")
    binary.chmod(0o755)
    return str(binary)


def test_local_binary_resolves_fingerprints_and_verifies(tmp_path):
    binary = _fake_binary(tmp_path)
    provider = LocalBinaryProvider()
    spec = _Spec("valkey.local-binary", {})

    artifact = provider.resolve(None, spec, {"path": binary, "expected_version": "9.1.0", "expected_sha": "abc1"})

    assert artifact.outputs == {"binary": binary}
    assert artifact.provenance["version"] == "9.1.0"
    assert artifact.provenance["git_sha"] == "abc12345"
    assert len(artifact.provenance["sha256"]) == 64
    with pytest.raises(ValueError, match=r"expected '8\.1\.4'"):
        provider.resolve(None, spec, {"path": binary, "expected_version": "8.1.4"})
    with pytest.raises(ValueError, match="does not match expected_sha"):
        provider.resolve(None, spec, {"path": binary, "expected_sha": "ffff"})
    with pytest.raises(FileNotFoundError):
        provider.resolve(None, spec, {"path": str(tmp_path / "missing")})


def test_tmpdir_provider_lifecycle(tmp_path):
    provider = TempDirectoryProvider()
    spec = _Spec("valkey.fs.tmpdir", {})
    resource = provider.provision(None, spec, {"parent": str(tmp_path)})
    path = resource.outputs["path"]
    assert os.path.isdir(path) and path.startswith(str(tmp_path))
    provider.cleanup(None, spec, resource)
    assert not os.path.exists(path)


def test_tmpfs_provider_reports_support_before_mutation():
    decision = TmpfsProvider().check_support(None, _Spec("valkey.fs.tmpfs", {}), {})
    assert isinstance(decision, SupportDecision)
    assert decision.supported is tmpfs_supported()[0]


def test_unsupported_tmpfs_is_skip_unsupported_through_the_runtime(runtime, monkeypatch):
    from valkey_fuzzer_packs import providers

    monkeypatch.setattr(providers, "tmpfs_supported", lambda: (False, "requires Linux"))
    path = REPOSITORY / "examples" / "valkey" / "bgsave_disk_full.yaml"
    plan = runtime.compile_yaml(path.read_text(encoding="utf-8"), source_name=str(path))
    outcome = runtime.run_plan(plan)
    assert outcome.status.value == "SKIP_UNSUPPORTED"
