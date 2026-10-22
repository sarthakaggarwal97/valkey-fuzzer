"""Installed-wheel acceptance for topology-provider plugin onboarding."""

import json
import os
import subprocess
import sys
import zipfile
from pathlib import Path


def _build_plugin_wheel(wheel_path: Path, marker_path: Path) -> None:
    distribution = "valkey_fuzzer_topology_test"
    dist_info = f"{distribution}-1.0.dist-info"
    module_name = "valkey_fuzzer_topology_test_plugin"
    files = {
        f"{module_name}.py": f"""
from pathlib import Path

from valkey_fuzzer.extensions import (
    FeaturePackSpec,
    PluginManifest,
    ProvisionedTopology,
    RunMode,
    TopologyProviderRegistration,
)

MARKER = Path({str(marker_path)!r})


def mark(value):
    with MARKER.open("a", encoding="utf-8") as stream:
        stream.write(value + "\\n")


class WheelTopologyProvider:
    def validate(self, plan):
        mark("validate:" + plan.topology)

    def provision(self, context):
        mark("provision:" + context.plan.topology)
        return ProvisionedTopology(services={{"origin": "installed-wheel"}})

    def rollback(self, context):
        mark("rollback:" + context.plan.topology)

    def cleanup(self, context, topology):
        mark("cleanup:" + topology.services["origin"])


PLUGIN = PluginManifest(
    plugin_id="wheel.plugin",
    api_version="v1",
    feature_packs=(
        FeaturePackSpec(
            pack_id="wheel.feature",
            version="1",
            mode=RunMode.TOPOLOGY_SMOKE,
            supported_topologies=("wheel-topology",),
            components=(),
        ),
    ),
    topology_providers=(
        TopologyProviderRegistration(
            topology="wheel-topology",
            provider_id="wheel.topology-provider",
            implementation_version="1.0",
            api_version="v1",
            factory=WheelTopologyProvider,
        ),
    ),
)
""",
        f"{dist_info}/METADATA": """Metadata-Version: 2.1
Name: valkey-fuzzer-topology-test
Version: 1.0
""",
        f"{dist_info}/WHEEL": """Wheel-Version: 1.0
Generator: valkey-fuzzer-test
Root-Is-Purelib: true
Tag: py3-none-any
""",
        f"{dist_info}/entry_points.txt": """[valkey_fuzzer.plugins]
wheel.plugin = valkey_fuzzer_topology_test_plugin:PLUGIN
""",
    }
    record_path = f"{dist_info}/RECORD"
    files[record_path] = "".join(f"{name},,\n" for name in (*files, record_path))

    with zipfile.ZipFile(wheel_path, "w", zipfile.ZIP_DEFLATED) as wheel:
        for name, content in files.items():
            wheel.writestr(name, content)


def test_installed_wheel_can_add_a_topology_and_run_it_through_the_cli(tmp_path):
    wheel = tmp_path / "valkey_fuzzer_topology_test-1.0-py3-none-any.whl"
    installed = tmp_path / "installed"
    marker = tmp_path / "provider-calls.txt"
    scenario = tmp_path / "scenario.yaml"
    report = tmp_path / "report.json"
    artifacts = tmp_path / "artifacts"
    _build_plugin_wheel(wheel, marker)

    install = subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "--disable-pip-version-check",
            "install",
            "--no-deps",
            "--target",
            str(installed),
            str(wheel),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert install.returncode == 0, install.stderr

    scenario.write_text(
        """
scenario_id: installed-wheel-topology
seed: 17
feature:
  use: wheel.feature
  version: "1"
  with: {}
topology:
  type: wheel-topology
""",
        encoding="utf-8",
    )
    environment = dict(os.environ)
    existing_pythonpath = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        str(installed) if not existing_pythonpath else os.pathsep.join((str(installed), existing_pythonpath))
    )

    run = subprocess.run(
        [
            sys.executable,
            "-m",
            "valkey_fuzzer.cli",
            "run",
            str(scenario),
            "--artifact-dir",
            str(artifacts),
            "--output",
            str(report),
            "--format",
            "json",
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert run.returncode == 0, f"stdout:\n{run.stdout}\nstderr:\n{run.stderr}"
    result = json.loads(report.read_text(encoding="utf-8"))["results"][0]
    assert result["status"] == "PASS"
    assert result["plan"]["topology"] == "wheel-topology"
    assert marker.read_text(encoding="utf-8").splitlines() == [
        "validate:wheel-topology",
        "provision:wheel-topology",
        "cleanup:installed-wheel",
    ]
