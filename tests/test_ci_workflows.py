"""Guards for the GitHub workflows that run the fuzzer.

The PR fuzzer is a reusable workflow called from other repositories, and the
scheduled run produces artifacts consumed by analysis tooling, so their inputs,
jobs, and CLI invocations are part of a public contract.
"""

import re
import shlex
from pathlib import Path

import pytest
import yaml

from valkey_fuzzer.cli import create_parser

ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS = ROOT / ".github" / "workflows"


def _workflow(name: str) -> dict:
    document = yaml.safe_load((WORKFLOWS / name).read_text(encoding="utf-8"))
    # PyYAML reads the bare `on` key as boolean True.
    document["on"] = document.pop(True, document.get("on"))
    return document


def _run_scripts(workflow: dict) -> list[str]:
    return [step["run"] for job in workflow["jobs"].values() for step in job.get("steps", ()) if "run" in step]


def _fuzzer_invocations(script: str) -> list[list[str]]:
    joined = re.sub(r"\\\n\s*", " ", script)
    invocations = []
    for line in joined.splitlines():
        line = line.strip()
        if not line.startswith("valkey-fuzzer "):
            continue
        line = re.split(r"\s(?:2>&1|\||>)\s", line)[0]
        tokens = shlex.split(line)
        invocations.append(["VALUE" if "$" in token else token for token in tokens[1:]])
    return invocations


def test_pr_fuzzer_keeps_its_reusable_workflow_inputs():
    workflow = _workflow("valkey-pr-fuzzer.yml")
    inputs = workflow["on"]["workflow_call"]["inputs"]

    for name in (
        "fuzzer_repository",
        "fuzzer_ref",
        "valkey_repository",
        "valkey_ref",
        "pr_number",
        "pr_url",
        "label_name",
        "run_count",
    ):
        assert name in inputs, name
    assert inputs["valkey_ref"]["required"] is True
    # Existing callers do not pass these, so they must stay optional.
    for name in ("fuzzer_repository", "fuzzer_ref", "pr_number", "pr_url", "label_name", "run_count"):
        assert inputs[name]["required"] is False, name
    assert inputs["run_count"]["default"] == 10


def test_pr_fuzzer_runs_a_random_cluster_matrix_and_reports_back():
    workflow = _workflow("valkey-pr-fuzzer.yml")
    jobs = workflow["jobs"]

    assert {"resolve-inputs", "prepare-matrix", "build-valkey", "random-fuzzer", "summarize"} <= set(jobs)
    assert "framework-acceptance" in jobs

    random_fuzzer = jobs["random-fuzzer"]
    assert "prepare-matrix.outputs.runs" in random_fuzzer["strategy"]["matrix"]["include"]
    steps = {step.get("name"): step for step in random_fuzzer["steps"]}
    assert steps["Download Valkey binaries"]["with"]["name"] == "valkey-pr-binaries"
    script = steps["Execute fuzzer run"]["run"]
    assert "valkey-fuzzer cluster" in script
    assert "--random" in script
    assert '--valkey-binary "${VALKEY_BINARY}"' in script
    assert "node-logs" in script
    assert "metadata.json" in script

    summarize = jobs["summarize"]
    assert summarize["if"] == "${{ always() }}"
    assert summarize["permissions"]["issues"] == "write"
    assert "<!-- valkey-cluster-fuzzer-results -->" in "\n".join(_run_scripts({"jobs": {"summarize": summarize}}))


def test_scheduled_run_fuzzes_and_bundles_artifacts():
    workflow = _workflow("fuzzer-run.yml")

    assert workflow["on"]["schedule"]
    jobs = workflow["jobs"]
    assert {"random-fuzzer", "framework-acceptance"} <= set(jobs)
    script = "\n".join(_run_scripts({"jobs": {"random-fuzzer": jobs["random-fuzzer"]}}))
    assert "valkey-fuzzer cluster --random" in script
    assert "--export-dsl" in script
    assert '"schema_version": 2' in script
    assert '"valkey_sha"' in script
    assert "manifest.json" in script


@pytest.mark.parametrize("workflow_name", ["valkey-pr-fuzzer.yml", "fuzzer-run.yml"])
def test_every_ci_fuzzer_invocation_parses_with_the_current_cli(workflow_name):
    invocations = [
        invocation for script in _run_scripts(_workflow(workflow_name)) for invocation in _fuzzer_invocations(script)
    ]

    assert invocations
    parser = create_parser()
    for arguments in invocations:
        parser.parse_args(arguments)


def test_lint_workflow_targets_the_package():
    scripts = "\n".join(_run_scripts(_workflow("lint.yml")))

    assert "ruff check valkey_fuzzer/ valkey_fuzzer_packs/ tests/ examples/" in scripts
    assert "ruff format --check valkey_fuzzer/ valkey_fuzzer_packs/ tests/ examples/" in scripts
    assert "src/" not in scripts


def test_scheduled_run_exercises_the_valkey_packs_live_as_root():
    job = _workflow("fuzzer-run.yml")["jobs"]["valkey-packs-live"]
    script = "\n".join(_run_scripts({"jobs": {"valkey-packs-live": job}}))

    # Root lets the tmpfs and iptables packs run instead of skipping.
    assert "sudo -E" in script
    assert "-m live" in script and "tests/test_live_valkey_packs.py" in script
    assert "VALKEY_FUZZER_BINARY=" in script and "VALKEY_FUZZER_OLD_BINARY=" in script


def test_acceptance_jobs_reference_files_that_exist():
    for workflow_name in ("valkey-pr-fuzzer.yml", "fuzzer-run.yml"):
        script = "\n".join(_run_scripts(_workflow(workflow_name)))
        for path in re.findall(r"(?:tests|examples)/[\w./-]+\.(?:py|yaml)", script):
            assert (ROOT / path).exists(), f"{workflow_name} references missing {path}"
