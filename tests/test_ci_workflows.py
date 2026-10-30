"""Guards for the GitHub workflows: their inputs, jobs, and CLI invocations are a contract with their callers."""

import contextlib
import io
import json
import os
import re
import shlex
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from valkey_fuzzer.cli import create_parser
from valkey_fuzzer.contracts import ComponentResult, ComponentStatus, OutcomeStatus, RunOutcome
from valkey_fuzzer.service import write_document

from .generator_support import generate_scenario
from .valkey_experiments_support import valkey_service

ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS = ROOT / ".github" / "workflows"
REGISTER = ROOT / "tests" / "fixtures" / "spec" / "register"
# The live tests that need root: a tmpfs mount and iptables partitions.
ROOT_ONLY_LIVE_TESTS = (
    "test_disk_full_degrades_gracefully_and_recovers",
    "test_cluster_partition_fails_over_heals_and_bounds_loss",
)


def _workflow(name):
    document = yaml.safe_load((WORKFLOWS / name).read_text(encoding="utf-8"))
    document["on"] = document.pop(True, document.get("on"))  # PyYAML reads the bare `on` key as True.
    return document


def _run_scripts(workflow):
    return [step["run"] for job in workflow["jobs"].values() for step in job.get("steps", ()) if "run" in step]


def _python_script(workflow, job, step):
    """The ``python - <<'PY'`` script of one step, to run as CI does."""

    script = next(item["run"] for item in _workflow(workflow)["jobs"][job]["steps"] if item.get("name") == step)
    (body,) = re.findall(r"python - <<'PY'\n(.*?)\n\s*PY\n", script, re.S)
    return textwrap.dedent(body)


def _exec(script, **env):
    """Run a workflow script in this process (it only reads its environment and writes files); its error or None."""

    with patch.dict(os.environ, env), contextlib.redirect_stdout(io.StringIO()):
        try:
            exec(compile(script, "workflow-step", "exec"), {"__name__": "__main__"})
        except SystemExit as exc:
            return str(exc) if exc.code else None
    return None


def test_pr_fuzzer_keeps_its_reusable_workflow_inputs_and_jobs():
    workflow = _workflow("valkey-pr-fuzzer.yml")
    inputs = workflow["on"]["workflow_call"]["inputs"]
    assert inputs["valkey_ref"]["required"] is True
    # Existing callers do not pass these, so they must stay optional.
    for name in (
        "fuzzer_repository",
        "fuzzer_ref",
        "valkey_repository",
        "pr_number",
        "pr_url",
        "label_name",
        "run_count",
    ):
        assert name in inputs and not inputs[name].get("required"), name
    assert {"resolve-inputs", "prepare-matrix", "build-valkey", "random-fuzzer", "summarize"} <= set(workflow["jobs"])


@pytest.mark.parametrize("name", sorted(path.name for path in WORKFLOWS.glob("*.yml")))
def test_every_workflow_parses_with_the_cli_references_real_files_and_pins_its_actions(name):
    scripts = "\n".join(_run_scripts(_workflow(name)))
    commands = [line.strip() for line in re.sub(r"\\\n\s*", " ", scripts).splitlines()]
    assert any(line.startswith("valkey-fuzzer ") for line in commands) or name in ("lint.yml", "test.yml")
    for line in commands:
        if line.startswith("valkey-fuzzer "):
            tokens = shlex.split(re.split(r"\s(?:2>&1|\|\||\||>)\s", line)[0])
            create_parser().parse_args(["VALUE" if "$" in token else token for token in tokens[1:]])
    for path in re.findall(r"(?:tests|examples)/[\w./-]+\.(?:py|yaml)", scripts):
        assert (ROOT / path).exists(), f"{name} references missing {path}"
    for action in re.findall(r"^\s*(?:-\s+)?uses:\s*(\S.*)$", (WORKFLOWS / name).read_text(), re.M):
        assert action.startswith("./") or re.fullmatch(r"[\w.-]+/[\w./-]+@[0-9a-f]{40} # v\d+(\.\d+)*", action), action


def test_daily_live_workflow_runs_only_the_root_only_tests_as_root():
    steps = _workflow("valkey-experiments-live.yml")["jobs"]["valkey-experiments-live"]["steps"]
    unprivileged, privileged = [step["run"] for step in steps if "-m live" in step.get("run", "")]
    selection = " or ".join(ROOT_ONLY_LIVE_TESTS)
    assert "sudo" not in unprivileged and f'-k "not ({selection})"' in unprivileged
    # Root lets the tmpfs and iptables experiments run instead of skipping, and the step fails if they skip anyway.
    assert "sudo -E" in privileged and f'-k "{selection}"' in privileged and "SKIPPED" in privileged
    source = (ROOT / "tests" / "test_live_valkey_experiments.py").read_text(encoding="utf-8")
    assert all(f"def {name}(" in source for name in ROOT_ONLY_LIVE_TESTS)


def test_pr_fuzzer_metadata_reads_a_real_run_report_or_the_batch_verdict(tmp_path):
    script = _python_script("valkey-pr-fuzzer.yml", "random-fuzzer", "Execute fuzzer run")
    batch = tmp_path / "run-1" / "batch"
    folder = batch / "runs" / "random-valkey.cluster-chaos-s31"
    folder.mkdir(parents=True)
    results = (ComponentResult("chaos", ComponentStatus.FAIL, "wave 1 validation failed (replication)"),)
    outcome = RunOutcome(
        plan=valkey_service().compile(generate_scenario(31)),
        status=OutcomeStatus.FAIL,
        summary="chaos failed",
        component_results=results,
        started_at=10.0,
        ended_at=52.5,
        valkey_sha="deadbeef",
    )
    write_document(str(folder / "report.json"), valkey_service().run_report(outcome))
    env = {"RUN_ROOT": str(tmp_path), "RUN_ID": "1", "STATUS": "failure", "STAGE": "fuzzer", "EXIT_CODE": "1"}
    for run, expected in (
        ({"id": folder.name, "status": "FAIL", "seed": 31, "summary": "x", "report": f"runs/{folder.name}/report.json"},
         {"outcome": "FAIL", "seed": 31, "duration": 42.5, "valkey_sha": "deadbeef",
          "failed_components": ["chaos: wave 1 validation failed (replication)"]}),
        # A run that left no report still reports the batch's verdict and summary.
        ({"id": "s7", "status": "ERROR", "seed": 7, "summary": "WorkerExit: no run report", "report": None},
         {"outcome": "ERROR", "seed": 7, "summary": "WorkerExit: no run report"}),
    ):  # fmt: skip
        (batch / "summary.json").write_text(json.dumps({"status": run["status"], "runs": [run]}))
        assert _exec(script, **env) is None
        metadata = json.loads((tmp_path / "run-1" / "metadata.json").read_text())
        assert {key: metadata[key] for key in expected} == expected


def test_the_pr_batch_workflow_holds_no_credentials_and_splices_no_input_into_a_shell():
    workflow = _workflow("fuzzer-pr-batch.yml")
    assert set(workflow["on"]) == {"workflow_dispatch"} and workflow["permissions"] == {"contents": "read"}
    (job,) = workflow["jobs"].values()
    text = (WORKFLOWS / "fuzzer-pr-batch.yml").read_text(encoding="utf-8")
    assert "permissions" not in job and "secrets." not in text and "github.token" not in text
    for step in job["steps"]:
        assert step.get("with", {}).get("persist-credentials", False) is False
        assert "${{ inputs." not in step.get("run", "")


SCENARIO = yaml.safe_dump(
    {"scenario_id": "s", "seed": 1, "experiment": {"id": "toy.register"}, "deployment": {"mode": "standalone"}}
)
BUNDLE = {str(path.relative_to(REGISTER)): path.read_text() for path in sorted(REGISTER.rglob("*")) if path.is_file()}
BINARY = yaml.safe_dump({"deployment": {"valkey_binary": "/bin/sh"}})


@pytest.mark.parametrize(
    ("scenarios", "inputs", "message"),
    [
        ({"proposal-1": SCENARIO, "spec.register": SCENARIO}, {"bundles": {"register": BUNDLE}}, None),
        ("x" * 60001, {}, "the limit is 60,000"),
        ("not json", {}, "not JSON"),
        ({}, {}, "non-empty JSON object"),
        ({"../escape": SCENARIO}, {}, "must be letters"),
        ({"a": "[1, 2]"}, {}, "must be a mapping"),
        ({"a": BINARY}, {}, "runs valkey_sha's build"),
        ({"a": SCENARIO}, {"sha": "main"}, "full 40-character lowercase commit SHA"),
        ({"a": SCENARIO}, {"repository": "evil; rm -rf /"}, "OWNER/NAME"),
        ({"a": SCENARIO}, {"bundles": {"../up": {}}}, "must be lowercase letters"),
        ({"a": SCENARIO}, {"bundles": {"register": {"model.tla": "x"}}}, "has no claims.yaml"),
        ({"a": SCENARIO}, {"bundles": {"register": {**BUNDLE, "../evil.cfg": "x"}}}, "must be claims.yaml"),
        ({"a": SCENARIO}, {"bundles": {"register": {**BUNDLE, "model.tla": "---- MODULE x ----\n===="}}}, "is invalid"),
    ],
)
def test_the_pr_batch_writes_the_scenarios_and_bundles_it_is_sent_and_refuses_bad_inputs(
    tmp_path, scenarios, inputs, message
):
    script = _python_script("fuzzer-pr-batch.yml", "batch", "Check the inputs and write the scenarios and bundles")
    error = _exec(
        script,
        INPUT_SCENARIOS=scenarios if isinstance(scenarios, str) else json.dumps(scenarios),
        INPUT_SPEC_BUNDLES=json.dumps(inputs["bundles"]) if "bundles" in inputs else "",
        INPUT_VALKEY_REPOSITORY=inputs.get("repository", "valkey-io/valkey"),
        INPUT_VALKEY_SHA=inputs.get("sha", "a" * 40),
        CANDIDATES_DIR=str(tmp_path / "candidates"),
        SPEC_DIR=str(tmp_path / "spec"),
    )
    if message:
        assert error and message in error, (message, error)
        return
    assert error is None and sorted(path.name for path in (tmp_path / "candidates").iterdir()) == [
        "proposal-1.yaml",
        "spec.register.yaml",
    ]
    written = tmp_path / "spec" / "register"
    assert {str(path.relative_to(written)) for path in written.rglob("*") if path.is_file()} == set(BUNDLE)
