"""Installed-wheel acceptance for the external extension template."""

import ast
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_ROOT = REPOSITORY_ROOT / "examples" / "extensions" / "neutral-plugin"
EXPECTED_MESSAGE = "configuration-reached-the-plugin"


def _run(command, *, cwd, environment=None):
    return subprocess.run(
        command,
        cwd=cwd,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )


def _offline_build_interpreter():
    candidates = tuple(
        dict.fromkeys(
            candidate
            for candidate in (
                sys.executable,
                shutil.which("python"),
                shutil.which("python3"),
                shutil.which("python3.9"),
            )
            if candidate
        )
    )
    for candidate in candidates:
        probe = _run(
            [candidate, "-c", "import setuptools.build_meta, wheel"],
            cwd=REPOSITORY_ROOT,
        )
        if probe.returncode == 0:
            return candidate
    raise AssertionError("no local Python interpreter provides the offline setuptools and wheel build dependencies")


def test_neutral_extension_template_uses_only_the_public_authoring_facade():
    source_path = TEMPLATE_ROOT / "src" / "valkey_fuzzer_neutral_plugin" / "__init__.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    valkey_imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("valkey_fuzzer"):
            valkey_imports.add(node.module)
        elif isinstance(node, ast.Import):
            valkey_imports.update(alias.name for alias in node.names if alias.name.startswith("valkey_fuzzer"))

    assert valkey_imports == {"valkey_fuzzer.extensions"}
    readme = (TEMPLATE_ROOT / "README.md").read_text(encoding="utf-8")
    assert (TEMPLATE_ROOT / "scenario.yaml").is_file()
    assert "valkey-fuzzer validate scenario.yaml" in readme
    assert "valkey-fuzzer run scenario.yaml --output report.json --format json" in readme


def test_installed_template_wheel_validates_and_runs_through_the_unchanged_cli(tmp_path):
    source = tmp_path / "neutral-plugin"
    wheelhouse = tmp_path / "wheelhouse"
    installed = tmp_path / "installed"
    report = tmp_path / "report.json"
    artifacts = tmp_path / "artifacts"
    shutil.copytree(TEMPLATE_ROOT, source)
    wheelhouse.mkdir()
    build_interpreter = _offline_build_interpreter()

    build = _run(
        [
            build_interpreter,
            "-m",
            "pip",
            "--disable-pip-version-check",
            "wheel",
            "--no-deps",
            "--no-build-isolation",
            "--wheel-dir",
            str(wheelhouse),
            str(source),
        ],
        cwd=tmp_path,
    )
    assert build.returncode == 0, f"stdout:\n{build.stdout}\nstderr:\n{build.stderr}"
    wheels = tuple(wheelhouse.glob("valkey_fuzzer_neutral_plugin-*.whl"))
    assert len(wheels) == 1

    install = _run(
        [
            sys.executable,
            "-m",
            "pip",
            "--disable-pip-version-check",
            "install",
            "--no-deps",
            "--target",
            str(installed),
            str(wheels[0]),
        ],
        cwd=tmp_path,
    )
    assert install.returncode == 0, f"stdout:\n{install.stdout}\nstderr:\n{install.stderr}"

    environment = dict(os.environ)
    existing_pythonpath = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        str(installed) if not existing_pythonpath else os.pathsep.join((str(installed), existing_pythonpath))
    )
    scenario = source / "scenario.yaml"

    validate = _run(
        [
            sys.executable,
            "-m",
            "valkey_fuzzer.cli",
            "validate",
            str(scenario),
        ],
        cwd=REPOSITORY_ROOT,
        environment=environment,
    )
    assert validate.returncode == 0, f"stdout:\n{validate.stdout}\nstderr:\n{validate.stderr}"
    assert "Scenario passed structural validation!" in validate.stdout

    run = _run(
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
        cwd=REPOSITORY_ROOT,
        environment=environment,
    )
    assert run.returncode == 0, f"stdout:\n{run.stdout}\nstderr:\n{run.stderr}"

    result = json.loads(report.read_text(encoding="utf-8"))["results"][0]
    assert result["status"] == "PASS"
    assert result["plan"]["mode"] == "feature"
    assert result["plan"]["topology"] == "example-neutral"
    assert result["plan"]["inputs"] == {"message": EXPECTED_MESSAGE}
    assert result["plan"]["required_events"] == ["example.neutral.input-observed"]
    assert result["plan"]["required_evidence"] == [
        {
            "evidence_id": "example.neutral.input-observed",
            "source": "exercise",
            "min_count": 1,
        }
    ]
    assert [component["config"] for component in result["plan"]["components"]] == [
        {"message": EXPECTED_MESSAGE},
        {"message": EXPECTED_MESSAGE},
    ]

    action, oracle = result["component_results"]
    assert (action["component_instance_id"], action["status"]) == ("exercise", "PASS")
    assert action["evidence"][0]["evidence_id"] == "example.neutral.input-observed"
    assert action["evidence"][0]["value"] == {
        "message": EXPECTED_MESSAGE,
        "topology": "example-neutral",
    }
    assert (oracle["component_instance_id"], oracle["status"]) == ("verify", "PASS")
    assert oracle["summary"] == "action evidence matched resolved input"
    committed_event_names = {event["name"] for event in result["diagnostics"]["committed_events"]}
    assert "example.neutral.input-observed" in committed_event_names
    assert "topology.cleanup.completed" in committed_event_names
