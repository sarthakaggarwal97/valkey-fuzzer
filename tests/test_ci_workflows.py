"""Guards for the GitHub workflows: their inputs, jobs, and CLI invocations are a contract with their callers."""

import ast
import contextlib
import importlib.util
import io
import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from _pytest.mark.expression import Expression

from valkey_fuzzer._data import to_plain_data
from valkey_fuzzer.cli import create_parser
from valkey_fuzzer.generator import GeneratorBlock, GeneratorError, generate, installed_pools, opted_in
from valkey_fuzzer.reports import write_document
from valkey_fuzzer.results import ComponentResult, ComponentStatus, OutcomeStatus, RunOutcome
from valkey_fuzzer.spec.tla import TLA_TOOLS_RELEASES, TLA_TOOLS_VERSION

from .generator_support import generate_scenario
from .valkey_experiments_support import valkey_service

ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS = ROOT / ".github" / "workflows"
REGISTER = ROOT / "tests" / "fixtures" / "spec" / "register"
# The live tests that need root: a tmpfs mount, iptables partitions, and tc netem degradations.
ROOT_ONLY_LIVE_TESTS = (
    "test_disk_full_degrades_gracefully_and_recovers",
    "test_cluster_partition_fails_over_heals_and_bounds_loss",
    "test_a_degraded_link_experiment_passes_and_leaves_no_netem_rule",
)


def _workflow(name):
    document = yaml.safe_load((WORKFLOWS / name).read_text(encoding="utf-8"))
    document["on"] = document.pop(True, document.get("on"))  # PyYAML reads the bare `on` key as True.
    return document


def _run_scripts(workflow):
    return [step["run"] for job in workflow["jobs"].values() for step in job.get("steps", ()) if "run" in step]


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


WORKFLOW_NAMES = sorted(path.name for path in WORKFLOWS.glob("*.yml"))
ACTIONS = ROOT / ".github" / "actions"
ACTION_NAMES = sorted(path.parent.name for path in ACTIONS.glob("*/action.yml"))
# The workflows and the composite actions they use, as paths under .github: what the step-level guards read.
GITHUB_FILES = sorted([f"workflows/{name}" for name in WORKFLOW_NAMES] + [f"actions/{name}/action.yml" for name in ACTION_NAMES])  # fmt: skip


def _action(name):
    return yaml.safe_load((ACTIONS / name / "action.yml").read_text(encoding="utf-8"))


def _steps(name):
    """Every step of the workflow or composite action at ``.github/<name>``."""

    document = yaml.safe_load((ROOT / ".github" / name).read_text(encoding="utf-8"))
    if "runs" in document:
        return document["runs"]["steps"]
    return [step for job in document["jobs"].values() for step in job.get("steps", ())]


def _fuzzer_commands(name):
    """Every valkey-fuzzer command of workflow ``name``, parsed by the CLI."""

    scripts = "\n".join(_run_scripts(_workflow(name)))
    # A command run as root keeps the runner's PATH: read it as the command it runs.
    commands = [
        re.sub(r'^sudo -E env "PATH=\$\{?PATH\}?" ', "", line.strip())
        for line in re.sub(r"\\\n\s*", " ", scripts).splitlines()
    ]
    # A bash array ("${name[@]}") stands for the arguments its literal lists; one assigned in a loop stands
    # for none.
    # An array literal ends with ")" at the end of a line; a ")" inside, as in "$((16#...))", does not end it.
    arrays = {name: shlex.split(body) for name, body in re.findall(r"^\s*(\w+)=\((.*?)\)[ \t]*$", scripts, re.M | re.S)}
    parsed = []
    for line in commands:
        if line.startswith("valkey-fuzzer "):
            tokens = shlex.split(re.split(r"\s(?:2>&1|\|\||\||>)\s", line)[0])
            arguments = []
            for token in tokens[1:]:
                array = re.fullmatch(r"\$\{(\w+)\[@\]\}", token)
                arguments += arrays.get(array.group(1), []) if array else [token]
            # A shell variable stands in as "1", which every option type (a path, a count, a seed) accepts.
            parsed.append(create_parser().parse_args(["1" if "$" in token else token for token in arguments]))
    return parsed


@pytest.mark.parametrize("name", WORKFLOW_NAMES)
def test_every_valkey_fuzzer_command_in_a_workflow_parses_with_the_cli(name):
    # The workflows that only run pytest or ruff, or build the package or the image, name no valkey-fuzzer command.
    no_cli = ("build-image.yml", "lint.yml", "release.yml", "test.yml", "valkey-experiments-live.yml")
    assert _fuzzer_commands(name) or name in no_cli


def _drawn():
    """The shipped experiments the generator draws, their inputs, and the server config keys of the pools."""

    catalog = valkey_service().catalog
    pools = installed_pools(catalog)
    experiments = opted_in(catalog)
    inputs = {
        f"inputs.{item}"
        for experiment in experiments
        for item in GeneratorBlock.parse(catalog.experiments.resolve(experiment), pools).inputs
    }
    keys = {f"server_config.{key}" for pool in pools.values() for key in pool if key not in ("scope", "default")}
    return set(experiments), inputs | keys


@pytest.mark.parametrize("name", WORKFLOW_NAMES)
def test_every_experiment_and_exclusion_a_workflow_names_is_drawn(name):
    experiments, paths = _drawn()
    for args in _fuzzer_commands(name):
        # A proposed.* experiment is one the workflow writes itself; "1" stands for a shell variable (an input of
        # a reusable workflow).
        for experiment in getattr(args, "experiments", None) or []:
            assert experiment in experiments or experiment in ("all", "1") or experiment.startswith("proposed."), (
                experiment
            )
        for item in getattr(args, "excludes", None) or []:
            if item.path == "experiment":
                assert item.value in experiments, f"{name} excludes {item}, which is not a shipped experiment"
            else:
                assert item.path in paths, f"{name} excludes {item}, which no shipped experiment or pool draws"


@pytest.mark.parametrize("name", WORKFLOW_NAMES)
def test_every_file_a_workflow_names_exists(name):
    scripts = "\n".join(_run_scripts(_workflow(name)))
    # A path that continues another ("$1/tests/modules") is in another checkout.
    for path in re.findall(r"(?<![\w/])(?:tests|examples|zoo|\.github/scripts)/[\w./-]+", scripts):
        assert (ROOT / path).exists(), f"{name} references missing {path}"


@pytest.mark.parametrize("name", GITHUB_FILES)
def test_every_workflow_and_action_pins_its_actions_to_a_commit(name):
    for action in re.findall(r"^\s*(?:-\s+)?uses:\s*(\S.*)$", (ROOT / ".github" / name).read_text(), re.M):
        assert action.startswith("./") or re.fullmatch(r"[\w.-]+/[\w./-]+@[0-9a-f]{40} # v\d+(\.\d+)*", action), action


@pytest.mark.parametrize("name", WORKFLOW_NAMES)
def test_every_composite_action_a_workflow_uses_exists_and_follows_a_checkout_of_the_fuzzer(name):
    # A local action is read from the workspace, so the fuzzer must be checked out there (not under a path) first.
    for job_name, job in _workflow(name)["jobs"].items():
        checked_out = False
        for step in job.get("steps", ()):
            uses = step.get("uses", "")
            if uses.startswith("actions/checkout@") and "path" not in step.get("with", {}):
                checked_out = True
            if uses.startswith("./"):
                assert (ROOT / uses / "action.yml").is_file(), (name, job_name, uses)
                assert checked_out, (name, job_name, step.get("name"))


@pytest.mark.parametrize("name", WORKFLOW_NAMES)
def test_every_workflow_sets_up_python_and_installs_the_project_through_the_one_action(name):
    # lint installs ruff alone and release the packaging tools; module-fuzz installs into the image's Python too, so
    # it keeps a plain install after its own setup-python. Every other workflow takes both from setup-fuzzer.
    own_install = ("lint.yml", "module-fuzz.yml", "release.yml")
    for step in _steps(f"workflows/{name}"):
        if step.get("uses", "").startswith("actions/setup-python@"):
            assert name in own_install, (name, step.get("name"))
        assert "pip install -e" not in step.get("run", ""), (name, step.get("name"))


@pytest.mark.parametrize(("extras", "requirement"), [("", "."), ("dev", ".[dev]")])
def test_the_setup_fuzzer_action_installs_the_project_with_the_extras_asked_for(tmp_path, extras, requirement):
    setup, install = _action("setup-fuzzer")["runs"]["steps"]
    assert setup["uses"].startswith("actions/setup-python@")
    assert setup["with"]["python-version"] == "${{ inputs.python-version }}"
    assert install["env"] == {"EXTRAS": "${{ inputs.extras }}"}
    # The install line, with a python that prints its arguments standing in for the interpreter.
    python = tmp_path / "python"
    python.write_text('#!/bin/sh\necho "$@"\n')
    python.chmod(0o755)
    result = subprocess.run(
        ["bash", "-euo", "pipefail", "-c", install["run"]],
        env={**os.environ, "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}", "EXTRAS": extras},
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.splitlines() == ["-m pip install --upgrade pip", f"-m pip install -e {requirement}"]


def test_the_build_valkey_action_restores_a_keyed_build_and_builds_only_on_a_miss():
    restore, build = _action("build-valkey")["runs"]["steps"]
    assert restore["uses"].startswith("actions/cache@") and restore["if"] == "inputs.cache-key != ''"
    assert restore["with"] == {"path": "${{ inputs.path }}", "key": "${{ inputs.cache-key }}"}
    # A skipped restore has no cache-hit output, so a build without a key always builds.
    assert build["if"] == f"steps.{restore['id']}.outputs.cache-hit != 'true'"
    inputs = {"REF": "${{ inputs.ref }}", "BUILD_DIR": "${{ inputs.path }}", "MAKE_ARGS": "${{ inputs.make-args }}"}
    assert build["env"] == inputs
    assert 'fetch --quiet --depth 1 https://github.com/valkey-io/valkey.git "${REF}"' in build["run"]
    assert "git clone" not in build["run"] and 'make -C "${BUILD_DIR}" -j"$(nproc)" ${MAKE_ARGS}' in build["run"]


# The jobs that write: the one that posts the PR comment the read-only summarize job rendered, the two of a
# release that post the built distributions to a GitHub release and to PyPI (its OIDC token), and the one that
# pushes the image on a tag.
WRITING_JOBS = {
    ("valkey-pr-fuzzer.yml", "comment"): {"issues": "write", "pull-requests": "write"},
    ("release.yml", "github-release"): {"contents": "write"},
    ("release.yml", "pypi"): {"id-token": "write"},
    ("build-image.yml", "publish"): {"packages": "write"},
}
# The image push builds the tag's Dockerfile and pushes it: a maintainer pushed the tag, no PR's code is near, and
# pushing is what the token is for (test_the_image_workflow_pushes_only_on_a_tag_and_builds_without_pushing_otherwise).
RUNS_WITH_A_WRITE_TOKEN = {("build-image.yml", "publish")}


@pytest.mark.parametrize("name", WORKFLOW_NAMES)
def test_every_workflow_holds_a_read_only_token_except_the_job_that_posts_the_comment(name):
    # Without a permissions block a workflow gets the repository's default token, which may be able to write; a
    # write token must never be in a job that checks out or runs anything, since the PR's code ran on the runners
    # whose artifacts the jobs read.
    workflow = _workflow(name)
    assert workflow["permissions"] == {"contents": "read"}, name
    for job_name, job in workflow["jobs"].items():
        raised = {scope: level for scope, level in job.get("permissions", {}).items() if level != "read"}
        assert raised == WRITING_JOBS.get((name, job_name), {}), (name, job_name)
        if raised and (name, job_name) not in RUNS_WITH_A_WRITE_TOKEN:
            assert not any("run" in step or "checkout" in step.get("uses", "") for step in job["steps"]), job_name


@pytest.mark.parametrize("name", WORKFLOW_NAMES)
def test_every_workflow_serializes_its_runs_and_bounds_every_job(name):
    # Without a concurrency group a schedule that outlasts its interval runs beside itself; without a timeout a hung
    # job holds a runner for six hours.
    workflow = _workflow(name)
    if set(workflow["on"]) == {"workflow_call"}:
        # A called workflow has no run of its own: each of its jobs carries the group.
        for job_name, job in workflow["jobs"].items():
            assert job["concurrency"]["group"] and job["concurrency"]["cancel-in-progress"] is False, (name, job_name)
    else:
        assert workflow["concurrency"]["group"] and workflow["concurrency"]["cancel-in-progress"] is False, name
    inputs = workflow["on"].get("workflow_call", {}).get("inputs", {})
    for job_name, job in workflow["jobs"].items():
        timeout = job.get("timeout-minutes")
        # A reusable workflow's caller sets the budget, through a number input that has a default.
        given = re.fullmatch(r"\$\{\{ inputs\.(\w+) \}\}", str(timeout))
        if given:
            rule = inputs[given.group(1)]
            assert rule["type"] == "number" and isinstance(rule["default"], int), (name, job_name)
        else:
            assert isinstance(timeout, int), (name, job_name)


UPSTREAM = "valkey-io/valkey-fuzzer"
OWNING_REPOSITORIES = (UPSTREAM, "sarthakaggarwal97/valkey-fuzzer")
# The repositories each schedule runs in: the four soaks in both, the bug zoo and the nightly model check upstream
# only. A workflow moves between the two lists here and in its job's `if`; a dispatch runs anywhere.
SCHEDULE_OWNERS = {
    "fuzzer-run.yml": OWNING_REPOSITORIES,
    "search-chaos.yml": OWNING_REPOSITORIES,
    "tla-model-check-nightly.yml": (UPSTREAM,),
    "valkey-builds.yml": OWNING_REPOSITORIES,
    "valkey-experiments-live.yml": OWNING_REPOSITORIES,
    "zoo.yml": (UPSTREAM,),
}


def _schedule_guard(name):
    """The one guard of every job of scheduled workflow ``name``."""

    owners = "".join(f" || github.repository == '{repository}'" for repository in SCHEDULE_OWNERS[name])
    return f"github.event_name != 'schedule'{owners}"


def test_the_schedule_owner_table_names_every_scheduled_workflow_and_no_other():
    assert sorted(SCHEDULE_OWNERS) == [name for name in WORKFLOW_NAMES if "schedule" in _workflow(name)["on"]]


@pytest.mark.parametrize("name", sorted(SCHEDULE_OWNERS))
def test_every_scheduled_workflow_runs_its_schedule_only_in_the_repositories_that_own_it(name):
    # A fork that enables Actions would otherwise run every schedule; a dispatch runs anywhere.
    for job_name, job in _workflow(name)["jobs"].items():
        assert job.get("if") == _schedule_guard(name), (name, job_name)


@pytest.mark.parametrize("name", GITHUB_FILES)
def test_every_download_a_workflow_installs_is_checked_against_a_pinned_digest(name):
    # The jar and the .deb are run or installed as root: a release asset that changed must fail the step.
    for script in [step["run"] for step in _steps(name) if "run" in step]:
        if "curl " in script:
            assert "sha256sum -c" in script, (name, script)
        for install in re.findall(r"^\s*sudo dpkg -i (\S+)", script, re.M):
            check = re.search(rf'\$\{{\w+_SHA256\}}  {re.escape(install)}" \| sha256sum -c -', script)
            assert check and check.start() < script.index(f"sudo dpkg -i {install}"), (name, install)


def test_the_pr_fuzzer_posts_its_comment_from_a_job_that_never_meets_the_prs_code():
    workflow = _workflow("valkey-pr-fuzzer.yml")
    summarize, comment = workflow["jobs"]["summarize"], workflow["jobs"]["comment"]
    # summarize renders what the runs wrote with a read-only token and hands over one file.
    assert summarize["permissions"] == {"contents": "read"}
    (upload,) = [step for step in summarize["steps"] if step.get("uses", "").startswith("actions/upload-artifact@")]
    assert upload["with"]["path"] == "pr-comment.md" and upload["with"]["if-no-files-found"] == "error"
    assert upload["if"] == "${{ needs.resolve-inputs.outputs.pr_number != '' }}"
    # The verdict is summarize's own, after the upload, so a FAIL still gets its comment.
    assert summarize["steps"][-1]["run"] == "exit 1" and "overall_failed == 'true'" in summarize["steps"][-1]["if"]
    # comment downloads that file and posts it, after summarize whether it passed or failed, and nothing else.
    assert "summarize" in comment["needs"] and "always()" in comment["if"]
    assert 'contains(fromJSON(\'["success", "failure"]\'), needs.summarize.result)' in comment["if"]
    download, post = comment["steps"]
    assert (
        download["uses"].startswith("actions/download-artifact@") and download["with"]["name"] == upload["with"]["name"]
    )
    assert post["uses"].startswith("actions/github-script@")
    assert post["env"]["COMMENT_FILE"] == f"{download['with']['path']}/pr-comment.md"
    assert "readFileSync(process.env.COMMENT_FILE" in post["with"]["script"]
    # The runs job's result reaches the verdict, so a run's metadata alone cannot make the workflow pass.
    (summary,) = _step_env("valkey-pr-fuzzer.yml", "summarize")
    assert summary["RUNS_RESULT"] == "${{ needs.random-fuzzer.result }}"


@pytest.mark.parametrize("name", GITHUB_FILES)
def test_every_workflow_caches_the_pip_downloads_on_the_file_that_names_them(name):
    for step in _steps(name):
        if step.get("uses", "").startswith("actions/setup-python@"):
            assert step["with"]["cache"] == "pip", (name, step["name"])
            assert step["with"]["cache-dependency-path"] == "pyproject.toml", (name, step["name"])


def test_the_server_tests_build_valkey_only_when_the_pinned_commits_build_is_not_cached():
    steps = {step["name"]: step for step in _workflow("test.yml")["jobs"]["server-tests"]["steps"] if "name" in step}
    pin, build = steps["Read the pinned Valkey commit"], steps["Build Valkey at the pinned commit"]
    assert "zoo/valkey-commit" in pin["run"] and 'echo "sha=' in pin["run"]
    assert build["uses"] == "./.github/actions/build-valkey" and build["with"]["path"] == "/tmp/valkey"
    assert build["with"]["ref"] == f"${{{{ steps.{pin['id']}.outputs.sha }}}}"
    assert "hashFiles('zoo/valkey-commit')" in build["with"]["cache-key"]
    # On PATH whether the build was restored or made.
    assert "if" not in steps["Put Valkey on PATH"] and "/tmp/valkey/src" in steps["Put Valkey on PATH"]["run"]


def test_the_daily_live_workflow_builds_valkey_8_1_only_when_the_branch_tips_build_is_not_cached():
    workflow = _workflow("valkey-experiments-live.yml")
    steps = {step["name"]: step for step in workflow["jobs"]["valkey-experiments-live"]["steps"] if "name" in step}
    # The key is the branch's tip, read before the cache is consulted, so a patch release builds once.
    tip = steps["Resolve the tip of Valkey 8.1"]
    assert "git ls-remote https://github.com/valkey-io/valkey.git refs/heads/8.1" in tip["run"]
    assert 'echo "sha=${sha}" >> "${GITHUB_OUTPUT}"' in tip["run"]
    restore, save = steps["Restore the Valkey 8.1 build"], steps["Save the Valkey 8.1 build"]
    assert restore["uses"].startswith("actions/cache/restore@") and save["uses"].startswith("actions/cache/save@")
    assert restore["with"]["path"] == save["with"]["path"] == "/tmp/valkey-8.1"
    assert f"${{{{ steps.{tip['id']}.outputs.sha }}}}" in restore["with"]["key"]
    assert save["with"]["key"] == f"${{{{ steps.{restore['id']}.outputs.cache-primary-key }}}}"
    # Built only on a miss, at the tip the key names, and saved right after, whatever the experiments do later.
    build = steps["Build Valkey 8.1 at that tip"]
    miss = f"steps.{restore['id']}.outputs.cache-hit != 'true'"
    assert build["if"] == save["if"] == miss
    assert build["env"]["VALKEY_8_1_SHA"] == f"${{{{ steps.{tip['id']}.outputs.sha }}}}"
    assert 'fetch --quiet --depth 1 https://github.com/valkey-io/valkey.git "${VALKEY_8_1_SHA}"' in build["run"]
    assert "git clone" not in build["run"] and "--branch 8.1" not in build["run"]
    # unstable moves daily and is still fetched and built every night, uncached, before the cache steps.
    names = list(steps)
    assert steps["Build Valkey unstable"]["uses"] == "./.github/actions/build-valkey"
    assert steps["Build Valkey unstable"]["with"] == {"ref": "unstable", "path": "/tmp/valkey-unstable"}
    assert (
        names.index("Build Valkey unstable")
        < names.index("Resolve the tip of Valkey 8.1")
        < names.index("Restore the Valkey 8.1 build")
    )
    assert (
        names.index("Restore the Valkey 8.1 build")
        < names.index("Build Valkey 8.1 at that tip")
        < names.index("Save the Valkey 8.1 build")
    )
    # Both live steps still run the old build from the cached tree.
    for script in _run_scripts(workflow):
        if "-m live" in script:
            assert "VALKEY_FUZZER_OLD_BINARY=/tmp/valkey-8.1/src/valkey-server" in script


def test_lint_installs_the_ruff_version_the_dev_extra_pins():
    (install,) = [script.strip() for script in _run_scripts(_workflow("lint.yml")) if "pip install" in script]
    (pinned,) = re.findall(r'"ruff==([\d.]+)"', (ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    # Quoted, or the shell would read a version specifier such as >= as a redirect.
    assert install == f"pip install 'ruff=={pinned}'"


def _live_tests_selected(script):
    """The test functions of test_live_valkey_experiments.py that ``script``'s pytest ``-k`` expression selects."""

    tokens = shlex.split(re.sub(r"\\\n\s*", " ", script))
    expression = Expression.compile(tokens[tokens.index("-k") + 1])
    source = ast.parse((ROOT / "tests" / "test_live_valkey_experiments.py").read_text(encoding="utf-8"))
    names = {node.name for node in source.body if isinstance(node, ast.FunctionDef) and node.name.startswith("test_")}
    return {name for name in names if _k_matches(expression, name)}, names


def _k_matches(expression, name):
    # -k matches a name case-insensitively, by substring.
    return expression.evaluate(lambda word, **_: word.lower() in name.lower())


def test_daily_live_workflow_runs_only_the_root_only_tests_as_root():
    steps = _workflow("valkey-experiments-live.yml")["jobs"]["valkey-experiments-live"]["steps"]
    unprivileged, privileged = [step["run"] for step in steps if "-m live" in step.get("run", "")]
    as_user, every_test = _live_tests_selected(unprivileged)
    as_root, _ = _live_tests_selected(privileged)
    assert as_root == set(ROOT_ONLY_LIVE_TESTS) and as_user == every_test - as_root
    assert "sudo" not in shlex.split(unprivileged) and "sudo" in shlex.split(privileged)
    # Root lets the tmpfs and iptables experiments run instead of skipping, and the step fails if they skip anyway.
    assert "SKIPPED" in privileged


def _load_script(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / ".github" / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CI = _load_script("ci")
PR_7 = {
    "valkey_repository": "valkey-io/valkey",
    "valkey_ref": "refs/pull/7/merge",
    "pr_number": "7",
    "pr_url": "https://github.com/valkey-io/valkey/pull/7",
    "concurrency_key": "valkey-pr-fuzzer-valkey-io/valkey-pr-7",
}


@pytest.mark.parametrize(
    ("inputs", "expected"),
    [
        ({"pr_link": "https://github.com/valkey-io/valkey/pull/7"}, PR_7),
        ({"pr_link": " https://github.com/valkey-io/valkey/pull/7/files?w=1#r1 "}, PR_7),
        # A PR URL pasted into pr_number counts as pr_link; matching overrides are fine.
        ({"pr_number": "https://github.com/valkey-io/valkey/pull/7", "valkey_ref": "refs/pull/7/merge"}, PR_7),
        ({"valkey_ref": "refs/pull/7/merge", "pr_number": "7", "pr_url": "https://elsewhere"}, PR_7),
        ({"valkey_repository": "me/valkey", "valkey_ref": "unstable"},
         {"valkey_repository": "me/valkey", "valkey_ref": "unstable", "pr_number": "", "pr_url": "",
          "concurrency_key": "valkey-pr-fuzzer-me/valkey-unstable"}),
        ({"pr_link": "https://github.com/valkey-io/valkey/pull/7", "valkey_ref": "unstable"}, "Conflicting inputs"),
        ({"pr_link": "https://github.com/valkey-io/valkey/pull/7", "pr_number": "8"}, "Conflicting inputs"),
        ({"pr_link": "https://gitlab.com/valkey-io/valkey/pull/7"}, "Invalid PR link format"),
        ({"pr_number": "seven", "valkey_ref": "unstable"}, "must be a numeric PR number"),
        ({}, "Either pr_link or valkey_ref must be provided"),
    ],
)  # fmt: skip
def test_the_pr_fuzzer_resolves_its_target_from_the_inputs(inputs, expected):
    if isinstance(expected, dict):
        assert CI.resolve(**inputs) == expected
        return
    with pytest.raises(SystemExit, match=expected):
        CI.resolve(**inputs)


@pytest.mark.parametrize(("raw", "expected"), [("10", 10), ("1", 1), ("3.0", 3), ("256", 256)])
def test_the_pr_fuzzer_queues_run_count_runs(raw, expected, monkeypatch, capsys):
    monkeypatch.setenv("RUN_COUNT", raw)
    CI.main("matrix")
    outputs = dict(line.split("=", 1) for line in capsys.readouterr().out.splitlines())
    assert outputs == {"run_count": str(expected), "runs": json.dumps([{"run_id": n} for n in range(1, expected + 1)])}


# GitHub runs at most 256 jobs from one matrix, so a larger run_count would fail only once the matrix expands.
@pytest.mark.parametrize("raw", ["0", "-1", "257", "1.5", "ten", "", "nan", "inf"])
def test_the_pr_fuzzer_refuses_a_run_count_that_is_not_a_whole_number_from_1_to_256(raw):
    with pytest.raises(SystemExit, match="must be a whole number from 1 to 256"):
        CI.run_count(raw)


def test_the_pr_fuzzer_metadata_reads_a_real_run_report(tmp_path):
    batch = tmp_path / "batch"
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
    run = {"id": folder.name, "status": "FAIL", "seed": 31, "summary": "x", "report": f"runs/{folder.name}/report.json"}
    (batch / "summary.json").write_text(json.dumps({"status": "FAIL", "runs": [run]}))

    metadata = CI.metadata(1, str(batch), "success", "failure")

    assert metadata == {
        "run_id": 1, "status": "failure", "stage": "fuzzer", "scenario_id": outcome.plan.scenario_id, "seed": 31,
        "outcome": "FAIL", "duration": 42.5, "failed_components": ["chaos: wave 1 validation failed (replication)"],
        "summary": "x", "valkey_sha": "deadbeef",
    }  # fmt: skip


def test_the_pr_fuzzer_metadata_of_a_run_without_a_report_keeps_the_batch_verdict(tmp_path):
    run = {"id": "s7", "status": "ERROR", "seed": 7, "summary": "WorkerExit: no run report", "report": None}
    (tmp_path / "summary.json").write_text(json.dumps({"status": "ERROR", "runs": [run]}))

    metadata = CI.metadata(2, str(tmp_path), "success", "failure")

    assert {key: metadata[key] for key in ("status", "stage", "outcome", "seed", "summary")} == {
        "status": "failure", "stage": "fuzzer", "outcome": "ERROR", "seed": 7, "summary": "WorkerExit: no run report",
    }  # fmt: skip


def test_the_pr_fuzzer_metadata_of_a_run_whose_setup_failed_names_the_setup_stage(tmp_path):
    metadata = CI.metadata(3, str(tmp_path / "batch"), "failure", "skipped")

    assert (metadata["status"], metadata["stage"], metadata["outcome"]) == ("failure", "setup", None)


def test_every_pr_fuzzer_step_passes_the_environment_its_command_reads(tmp_path, monkeypatch):
    jobs = _workflow("valkey-pr-fuzzer.yml")["jobs"].values()
    steps = [step for job in jobs for step in job["steps"] if "ci.py" in step.get("run", "")]
    values = {"INPUT_VALKEY_REF": "unstable", "RUN_COUNT": "1", "RUN_ID": "1", "RUN_DIR": str(tmp_path / "run-1"),
              "RESULTS_DIR": str(tmp_path), "PREPARE_MATRIX_RESULT": "success", "BUILD_RESULT": "success"}  # fmt: skip
    # What the runner sets for every step.
    runner = {"GITHUB_OUTPUT": str(tmp_path / "output"), "GITHUB_STEP_SUMMARY": str(tmp_path / "summary")}
    monkeypatch.chdir(tmp_path)
    commands = []
    for step in steps:
        (command,) = re.findall(r"ci\.py ([\w-]+)", step["run"])
        with patch.dict(os.environ, {**{name: values.get(name, "") for name in step["env"]}, **runner}, clear=True):
            CI.main(command)
        commands.append(command)
    assert commands == ["resolve", "matrix", "metadata", "summarize"]


def _summary_env(**overrides):
    env = {
        "PREPARE_MATRIX_RESULT": "success", "RUN_COUNT": "2", "BUILD_RESULT": "success", "RUNS_RESULT": "success",
        "RESOLVED_SHA": "abc123",
        "FUZZER_REPOSITORY": "valkey-io/valkey-fuzzer", "FUZZER_REF": "main", "VALKEY_REPOSITORY": "valkey-io/valkey",
        "VALKEY_REF": "refs/pull/7/merge", "PR_NUMBER": "7", "PR_URL": "https://github.com/valkey-io/valkey/pull/7",
        "LABEL_NAME": "run-cluster-fuzzer", "RUN_URL": "https://github.com/valkey-io/valkey-fuzzer/actions/runs/1",
    }  # fmt: skip
    return {**env, **overrides}


def _run_result(run_id, status="success", **fields):
    return {"run_id": run_id, "status": status, "stage": "fuzzer", "seed": run_id, "outcome": "PASS", **fields}


def test_the_pr_fuzzer_summary_passes_when_every_run_passed():
    summary, comment, failed = CI.summarize(_summary_env(PR_NUMBER=""), {1: _run_result(1), 2: _run_result(2)})

    assert not failed and comment is None
    assert "- Overall result: `PASS`" in summary and "- Passed runs: 2/2" in summary
    assert "| 2 | `success` | `2` | `fuzzer` | - | `PASS` | - |" in summary


def test_the_pr_fuzzer_summary_fails_on_a_failed_or_missing_run_and_details_them_in_the_comment():
    failing = _run_result(1, "failure", outcome="FAIL", failed_components=["chaos: lost | 3 keys\nafter wave 1"])

    summary, comment, failed = CI.summarize(_summary_env(RUNS_RESULT="failure"), {1: failing})

    assert failed
    assert "| 1 | `failure` | `1` | `fuzzer` | - | `FAIL` | `chaos: lost / 3 keys after wave 1` |" in summary
    assert "| 2 | missing | - | artifact | - | - | artifact not uploaded |" in summary
    assert comment.startswith("<!-- valkey-cluster-fuzzer-results -->\n") and "- Overall result: `FAIL`" in comment
    assert "| 1 | `failure` | `1` | `fuzzer` | `FAIL` | `chaos: lost / 3 keys after wave 1` |" in comment
    assert (
        "Missing artifacts for runs: 2" in comment
        and "- PR: [#7](https://github.com/valkey-io/valkey/pull/7)" in summary
    )


def test_the_pr_fuzzer_summary_shows_what_a_run_wrote_as_text_and_trusts_the_runs_job_over_it():
    # metadata.json and the build's output were written where the PR's code ran: markdown in them must not render
    # as a link, mention, HTML, or bare URL in the comment, and they must not turn a failed runs job into a PASS.
    forged = _run_result(
        1,
        "[ok](https://evil.example) @maintainer <img src=x>",
        failed_components=["see https://evil.example/`pwned`"],
        seed="#1",
    )
    env = _summary_env(RESOLVED_SHA="abc`def", RUNS_RESULT="failure")

    summary, comment, failed = CI.summarize(env, {1: forged, 2: _run_result(2)})

    assert failed and "- Overall result: `FAIL`" in comment and "- Fuzzer runs: `failure`" in summary
    assert "- Resolved SHA: `abc'def`" in comment
    row = "| 1 | `[ok](https://evil.example) @maintainer <img src=x>` | `#1` | `fuzzer` | `PASS` | "
    assert row + "`see https://evil.example/'pwned'` |" in comment
    # Every value a run wrote is inside a code span, so the comment and the summary hold no bare markdown.
    for text in (summary, comment):
        for line in text.splitlines():
            if line.startswith("| 1 |"):
                assert re.fullmatch(r"\| 1 \| (`[^`]*`|-)( \| (`[^`]*`|-))+ \|", line), line

    # A forged PASS: every run's metadata says success, but the runs job failed.
    _, comment, failed = CI.summarize(env, {1: _run_result(1), 2: _run_result(2)})
    assert failed and "- Overall result: `FAIL`" in comment and "- Passed runs: 2/2" in comment


def test_the_pr_fuzzer_summary_takes_each_runs_id_from_its_artifact_not_from_a_file_it_wrote(tmp_path, monkeypatch):
    # The PR's code can leave any file under its run folder; a metadata.json with another run's ID, or one in a
    # subfolder, must not stand in for that run.
    results = tmp_path / "collected-results"
    for run_id, status in ((1, "failure"), (2, "success")):
        folder = results / f"valkey-pr-fuzzer-run-{run_id}"
        folder.mkdir(parents=True)
        (folder / "metadata.json").write_text(json.dumps(_run_result(run_id, status)))
    (results / "valkey-pr-fuzzer-run-2" / "metadata.json").write_text(json.dumps(_run_result(1, "success")))
    forged = results / "valkey-pr-fuzzer-run-2" / "batch"
    forged.mkdir()
    (forged / "metadata.json").write_text(json.dumps(_run_result(1, "success")))
    env = {**_summary_env(RESULTS_DIR=str(results)), "GITHUB_STEP_SUMMARY": str(tmp_path / "summary")}
    env["GITHUB_OUTPUT"] = str(tmp_path / "output")
    monkeypatch.chdir(tmp_path)

    with patch.dict(os.environ, env, clear=True):
        CI.main("summarize")

    summary = (tmp_path / "summary").read_text(encoding="utf-8")
    assert "| 1 | `failure` |" in summary and "| 2 | `success` |" in summary
    assert (tmp_path / "output").read_text(encoding="utf-8") == "overall_failed=true\n"


def test_the_pr_fuzzer_summary_fails_when_no_runs_were_queued():
    summary, comment, failed = CI.summarize(_summary_env(PREPARE_MATRIX_RESULT="failure", RUN_COUNT=""), {})

    assert failed and "No runs were queued" in summary and "No runs were queued" in comment
    assert "| Run |" not in summary and "<details>" not in comment


def test_the_pr_batch_workflow_holds_no_credentials_and_splices_no_input_into_a_shell():
    workflow = _workflow("fuzzer-pr-batch.yml")
    assert set(workflow["on"]) == {"workflow_dispatch"} and workflow["permissions"] == {"contents": "read"}
    (job,) = workflow["jobs"].values()
    text = (WORKFLOWS / "fuzzer-pr-batch.yml").read_text(encoding="utf-8")
    assert "permissions" not in job and "secrets." not in text and "github.token" not in text
    for step in job["steps"]:
        assert step.get("with", {}).get("persist-credentials", False) is False
        assert "${{ inputs." not in step.get("run", "")


SCENARIO_DOCUMENT = {
    "scenario_id": "s", "seed": 1, "experiment": {"id": "toy.register"}, "deployment": {"mode": "standalone"},
}  # fmt: skip
SCENARIO = yaml.safe_dump(SCENARIO_DOCUMENT)
BUNDLE = {str(path.relative_to(REGISTER)): path.read_text() for path in sorted(REGISTER.rglob("*")) if path.is_file()}
BINARY = yaml.safe_dump({"deployment": {"valkey_binary": "/bin/sh"}})
INSTANCE_BINARIES = yaml.safe_dump({"deployment": {"mode": "standalone", "instance_binaries": {"replica": "/bin/sh"}}})
ARTIFACT = yaml.safe_dump({"artifacts": {"old": {"use": "valkey.artifact.local-binary", "with": {"path": "/bin/sh"}}}})
# A scenario that compares the parent build: what a valkey.action.commands scenario holds.
COMPARES_PARENT = yaml.safe_dump(
    {**SCENARIO_DOCUMENT, "experiment": {"id": "x", "inputs": {"with": {"$ref": "artifacts.parent.binary"}}}}
)
OTHER_ARTIFACT = yaml.safe_dump(
    {**SCENARIO_DOCUMENT, "experiment": {"id": "x", "inputs": {"with": {"$ref": "artifacts.other.binary"}}}}
)
PARENT = "b" * 40
PARENT_BINARY = "/work/valkey-parent/src/valkey-server"
MODULE_DIR, MODULE_PARENT_DIR = "/work/module-src", "/work/module-parent"
# A scenario that compares two builds of a module: what a valkey.action.commands scenario with modules holds.
COMPARES_MODULES = yaml.safe_dump(
    {
        **SCENARIO_DOCUMENT,
        "experiment": {
            "id": "x",
            "inputs": {"module": {"$ref": "artifacts.module.path"}, "old": {"$ref": "artifacts.module_parent.path"}},
        },
    }
)
JSON_MODULE = {"module_repository": "valkey-io/valkey-json", "module_sha": "c" * 40, "module_parent_sha": "d" * 40}
# A proposal's own experiment: the register bundle's, under a proposed. ID.
PROPOSED = BUNDLE["experiments/toy.register.yaml"].replace("id: toy.register\n", "id: proposed.register\n")


def _raw(value):
    return value if isinstance(value, str) else json.dumps(value)


def _check_pr_batch_inputs(tmp_path, scenarios, inputs):
    """Run the PR batch's input step as CI does; its error, or None."""

    env = {
        "INPUT_SCENARIOS": scenarios if isinstance(scenarios, str) else json.dumps(scenarios),
        "INPUT_SPEC_BUNDLES": json.dumps(inputs["bundles"]) if "bundles" in inputs else "",
        "INPUT_EXPERIMENTS": _raw(inputs["experiments"]) if "experiments" in inputs else "",
        "INPUT_VALKEY_REPOSITORY": inputs.get("repository", "valkey-io/valkey"),
        "INPUT_VALKEY_SHA": inputs.get("sha", "a" * 40),
        "INPUT_VALKEY_PARENT_SHA": inputs.get("parent_sha", ""),
        "INPUT_BUILD": inputs.get("build", "default"),
        "INPUT_MODULE_REPOSITORY": inputs.get("module_repository", ""),
        "INPUT_MODULE_SHA": inputs.get("module_sha", ""),
        "INPUT_MODULE_PARENT_SHA": inputs.get("module_parent_sha", ""),
        "INPUT_MODULE_FILE": inputs.get("module_file", ""),
        "INPUT_RANDOM_SEEDS": inputs.get("random_seeds", "0"),
        "INPUT_REQUEST_ID": inputs.get("request_id", ""),
        "INPUT_EXCLUDE_EXPERIMENTS": inputs.get("exclude_experiments", ""),
        "CANDIDATES_DIR": str(tmp_path / "candidates"),
        "SPEC_DIR": str(tmp_path / "spec"),
        "EXPERIMENTS_DIR": str(tmp_path / "experiments"),
        "PARENT_BINARY": PARENT_BINARY,
        "MODULE_DIR": MODULE_DIR,
        "MODULE_PARENT_DIR": MODULE_PARENT_DIR,
    }
    with patch.dict(os.environ, env), contextlib.redirect_stdout(io.StringIO()):
        try:
            CI.main("check-inputs")
        except SystemExit as exc:
            return str(exc)
    return None


def test_the_pr_batch_runs_a_random_baseline_without_scenarios(tmp_path):
    inputs = {"random_seeds": "4", "request_id": "replay-007-122ec7e16"}
    inputs["exclude_experiments"] = "valkey.replication-chaos  valkey.cluster-chaos"
    assert _check_pr_batch_inputs(tmp_path, {}, inputs) is None
    assert list((tmp_path / "candidates").iterdir()) == []


def test_the_pr_batch_writes_the_scenarios_and_bundles_it_is_sent(tmp_path):
    scenarios = {"proposal-1": SCENARIO, "spec.register": SCENARIO}

    assert _check_pr_batch_inputs(tmp_path, scenarios, {"bundles": {"register": BUNDLE}}) is None
    assert sorted(path.name for path in (tmp_path / "candidates").iterdir()) == [
        "proposal-1.yaml",
        "spec.register.yaml",
    ]
    written = tmp_path / "spec" / "register"
    assert {str(path.relative_to(written)) for path in written.rglob("*") if path.is_file()} == set(BUNDLE)


def test_the_pr_batch_builds_a_parent_commit_the_same_way_and_declares_it_to_every_scenario(tmp_path):
    workflow = _workflow("fuzzer-pr-batch.yml")
    inputs = workflow["on"]["workflow_dispatch"]["inputs"]
    assert inputs["valkey_parent_sha"]["required"] is False and inputs["valkey_parent_sha"]["default"] == ""
    assert inputs["build"]["type"] == "choice" and inputs["build"]["default"] == "default"
    assert tuple(inputs["build"]["options"]) == CI.BUILDS
    (job,) = workflow["jobs"].values()
    (parent,) = [step for step in job["steps"] if step.get("with", {}).get("path") == "valkey-parent"]
    assert parent["if"] == "inputs.valkey_parent_sha != ''"
    assert parent["with"]["ref"] == "${{ inputs.valkey_parent_sha }}" and "fetch-depth" in parent["with"]
    (build,) = [step for step in job["steps"] if step.get("name") == "Build Valkey"]
    assert {"INPUT_VALKEY_SHA", "INPUT_VALKEY_PARENT_SHA", "INPUT_BUILD"} <= set(build["env"])
    # Both checkouts get the chosen build; the sanitizer lines are Valkey's own CI's.
    assert "build valkey-src" in build["run"] and "build valkey-parent" in build["run"]
    for option in ("SANITIZER=address", "SANITIZER=undefined LUA_DEBUG=yes", "CC=clang"):
        assert option in build["run"]
    assert build["run"].count("BUILD_ZSTD=yes") == len(CI.BUILDS)
    (batch,) = [step for step in job["steps"] if step.get("id") == "batch"]
    assert batch["env"]["ASAN_OPTIONS"] == "allocator_may_return_null=1:quarantine_size_mb=16"
    (summary,) = _step_env("fuzzer-pr-batch.yml", "summary")
    assert {"VALKEY_PARENT_SHA", "BUILD"} <= set(summary)
    assert job["env"]["PARENT_BINARY"].endswith("/valkey-parent/src/valkey-server")

    # With a parent build, the block is appended to the text as sent, and the result is the scenario plus it.
    scenarios = {"compare": COMPARES_PARENT, "plain": SCENARIO}
    assert _check_pr_batch_inputs(tmp_path, scenarios, {"parent_sha": PARENT, "build": "asan"}) is None
    block = CI.artifacts_block({"parent": ("valkey.artifact.local-binary", PARENT_BINARY)})
    assert block == (
        f'artifacts:\n  parent:\n    use: valkey.artifact.local-binary\n    with:\n      path: "{PARENT_BINARY}"\n'
    )
    for name, text in scenarios.items():
        written = (tmp_path / "candidates" / f"{name}.yaml").read_text(encoding="utf-8")
        assert written == text + block
        assert yaml.safe_load(written) == {
            **yaml.safe_load(text),
            "artifacts": {"parent": {"use": "valkey.artifact.local-binary", "with": {"path": PARENT_BINARY}}},
        }


def test_the_pr_batch_builds_a_module_at_two_commits_and_declares_both_to_every_scenario(tmp_path):
    workflow = _workflow("fuzzer-pr-batch.yml")
    inputs = workflow["on"]["workflow_dispatch"]["inputs"]
    for name in ("module_repository", "module_sha", "module_parent_sha", "module_file"):
        assert inputs[name]["required"] is False and inputs[name]["default"] == "", name
    (job,) = workflow["jobs"].values()
    assert job["env"]["MODULE_DIR"].endswith("/module-src")
    assert job["env"]["MODULE_PARENT_DIR"].endswith("/module-parent")
    checkouts = {step["with"]["path"]: step for step in job["steps"] if "path" in step.get("with", {})}
    assert checkouts["module-src"]["if"] == "inputs.module_repository != ''"
    assert checkouts["module-src"]["with"]["ref"] == "${{ inputs.module_sha }}"
    assert checkouts["module-parent"]["if"] == "inputs.module_parent_sha != ''"
    assert checkouts["module-parent"]["with"]["ref"] == "${{ inputs.module_parent_sha }}"
    for checkout in ("module-src", "module-parent"):
        assert checkouts[checkout]["with"]["repository"] == "${{ inputs.module_repository }}"
    steps = [step.get("name") for step in job["steps"]]
    # The modules are built against the Valkey build, after it.
    assert steps.index("Build the modules") > steps.index("Build Valkey")
    (build,) = [step for step in job["steps"] if step.get("name") == "Build the modules"]
    assert build["if"] == "inputs.module_repository != ''"
    # The workflow's table of module builds is exactly the modules check-inputs accepts.
    table = re.search(r'case "\$\{INPUT_MODULE_REPOSITORY#\*/\}" in(.*?)esac', build["run"], re.S).group(1)
    assert set(re.findall(r"^\s*([\w.-]+)\)", table, re.M)) == set(CI.MODULE_FILES)
    assert 'VALKEY_MODULE_H_PATH="${GITHUB_WORKSPACE}/valkey-src/src/valkeymodule.h" ./build.sh' in table
    assert 'make -C "$1/tests/modules"' in table
    assert "build_module module-src" in build["run"] and "build_module module-parent" in build["run"]
    (summary,) = _step_env("fuzzer-pr-batch.yml", "summary")
    assert {"MODULE_REPOSITORY", "MODULE_SHA", "MODULE_PARENT_SHA"} <= set(summary)

    # One Valkey build, two module builds: no valkey_parent_sha needed.
    scenarios = {"compare": COMPARES_MODULES, "plain": SCENARIO}
    assert _check_pr_batch_inputs(tmp_path, scenarios, JSON_MODULE) is None
    declared = {
        "module": ("valkey.artifact.local-module", f"{MODULE_DIR}/build/src/libjson.so"),
        "module_parent": ("valkey.artifact.local-module", f"{MODULE_PARENT_DIR}/build/src/libjson.so"),
    }
    for name, text in scenarios.items():
        written = (tmp_path / "candidates" / f"{name}.yaml").read_text(encoding="utf-8")
        assert written == text + CI.artifacts_block(declared)
        assert yaml.safe_load(written)["artifacts"]["module"] == {
            "use": "valkey.artifact.local-module",
            "with": {"path": f"{MODULE_DIR}/build/src/libjson.so"},
        }

    # valkey's tests/modules at both commits, with the parent Valkey build too.
    core = {"module_repository": "valkey-io/valkey", "module_sha": "c" * 40, "module_parent_sha": "d" * 40,
            "module_file": "tests/modules/datatype.so", "parent_sha": PARENT}  # fmt: skip
    assert _check_pr_batch_inputs(tmp_path / "core", {"compare": COMPARES_MODULES}, core) is None
    written = yaml.safe_load((tmp_path / "core" / "candidates" / "compare.yaml").read_text(encoding="utf-8"))
    assert list(written["artifacts"]) == ["parent", "module", "module_parent"]
    assert written["artifacts"]["module_parent"]["with"]["path"] == f"{MODULE_PARENT_DIR}/tests/modules/datatype.so"


def test_the_batch_summary_names_the_module_commits(tmp_path):
    (tmp_path / "summary.json").write_text(json.dumps({"status": "PASS", "counts": {}, "runs": [], "signatures": []}))

    summary = CI.batch_summary("Batch", str(tmp_path), module="`o/valkey-json` `ccc`", module_parent="ddd")

    assert "- Module (artifacts.module): `o/valkey-json` `ccc`\n" in summary
    assert "- Module parent commit (artifacts.module_parent): `ddd`\n" in summary


def test_the_pr_batch_writes_the_experiments_it_is_sent_and_the_batch_loads_them(tmp_path):
    workflow = _workflow("fuzzer-pr-batch.yml")
    experiments = workflow["on"]["workflow_dispatch"]["inputs"]["experiments"]
    assert experiments["required"] is False and experiments["default"] == ""
    (job,) = workflow["jobs"].values()
    (batch,) = [step for step in job["steps"] if step.get("id") == "batch"]
    assert '--experiment-dir "${EXPERIMENTS_DIR}"' in batch["run"] and "EXPERIMENTS_DIR" in job["env"]

    inputs = {"experiments": {"register.yaml": PROPOSED}, "bundles": {"register": BUNDLE}}
    assert _check_pr_batch_inputs(tmp_path, {"a": SCENARIO}, inputs) is None
    assert [path.name for path in (tmp_path / "experiments").iterdir()] == ["register.yaml"]
    assert (tmp_path / "experiments" / "register.yaml").read_text(encoding="utf-8") == PROPOSED
    # Without experiments there is no directory, and the batch step passes none.
    assert _check_pr_batch_inputs(tmp_path / "none", {"a": SCENARIO}, {}) is None
    assert not (tmp_path / "none" / "experiments").exists()


def test_the_batch_summary_names_the_parent_commit_and_the_build(tmp_path):
    (tmp_path / "summary.json").write_text(json.dumps({"status": "PASS", "counts": {}, "runs": [], "signatures": []}))

    summary = CI.batch_summary("Batch", str(tmp_path), "`v` `-` `abc`", parent=PARENT, build="asan")

    assert summary.startswith(
        f"## Batch\n\n- Valkey: `v` `-` `abc`\n- Parent commit (artifacts.parent): `{PARENT}`\n- Build: `asan`\n"
    )
    assert "Parent" not in CI.batch_summary("Batch", str(tmp_path)) and "Build" not in CI.batch_summary(
        "B", str(tmp_path)
    )


@pytest.mark.parametrize(
    ("scenarios", "inputs", "message"),
    [
        ("x" * 60001, {}, "the limit is 60,000"),
        ("not json", {}, "not JSON"),
        ({}, {}, "there is nothing to run"),
        ([], {}, "must be a JSON object"),
        ({"a": SCENARIO}, {"random_seeds": "5"}, "random_seeds must be 0 to 4"),
        ({"a": SCENARIO}, {"request_id": "a b"}, "request_id must be"),
        ({"a": SCENARIO}, {"random_seeds": "2", "exclude_experiments": "x;rm"}, "exclude_experiments must be"),
        ({"a": SCENARIO}, {"exclude_experiments": "valkey.replication-chaos"}, "which random_seeds 0 skips"),
        ({"../escape": SCENARIO}, {}, "must be letters"),
        ({"a": "[1, 2]"}, {}, "must be a mapping"),
        ({"a": BINARY}, {}, "sets deployment.valkey_binary; the batch runs valkey_sha's build"),
        ({"a": INSTANCE_BINARIES}, {}, "sets deployment.instance_binaries; the batch runs valkey_sha's build"),
        ({"a": ARTIFACT}, {}, "declares artifacts; the batch declares them"),
        ({"a": ARTIFACT}, {"parent_sha": PARENT}, "declares artifacts; the batch declares them"),
        (
            {"a": COMPARES_PARENT},
            {},
            "references artifacts.parent, which the batch does not declare (no valkey_parent_sha)",
        ),
        ({"a": OTHER_ARTIFACT}, {"parent_sha": PARENT}, "references artifacts.other, which the batch does not declare"),
        ({"a": SCENARIO.rstrip("\n")}, {"parent_sha": PARENT}, "does not add exactly that key"),
        ({"a": json.dumps(SCENARIO_DOCUMENT)}, {"parent_sha": PARENT}, "does not add exactly that key"),
        ({"a": SCENARIO}, {"parent_sha": "B" * 40}, "valkey_parent_sha must be empty or a full"),
        ({"a": SCENARIO}, {"parent_sha": "a" * 40}, "valkey_parent_sha is valkey_sha"),
        ({"a": SCENARIO}, {"build": "valgrind"}, "build must be one of default, asan, ubsan"),
        (
            {"a": COMPARES_MODULES},
            {},
            "references artifacts.module, which the batch does not declare (no module_repository)",
        ),
        (
            {"a": COMPARES_MODULES},
            {**JSON_MODULE, "module_parent_sha": ""},
            "references artifacts.module_parent, which the batch does not declare (no module_parent_sha)",
        ),
        (
            {"a": SCENARIO},
            {"module_sha": "c" * 40},
            "module_sha, module_parent_sha, and module_file need module_repository",
        ),
        ({"a": SCENARIO}, {"module_file": "x.so"}, "need module_repository"),
        (
            {"a": SCENARIO},
            {**JSON_MODULE, "module_repository": "evil; rm"},
            "module_repository must be empty or OWNER/NAME",
        ),
        (
            {"a": SCENARIO},
            {**JSON_MODULE, "module_repository": "o/valkey-search"},
            "is not a module the batch builds; it builds valkey-json, valkey",
        ),
        ({"a": SCENARIO}, {**JSON_MODULE, "module_sha": "main"}, "module_sha must be a full 40-character"),
        ({"a": SCENARIO}, {**JSON_MODULE, "module_parent_sha": "D" * 40}, "module_parent_sha must be empty or a full"),
        ({"a": SCENARIO}, {**JSON_MODULE, "module_parent_sha": "c" * 40}, "module_parent_sha is module_sha"),
        (
            {"a": SCENARIO},
            {**JSON_MODULE, "module_repository": "valkey-io/valkey"},
            "valkey builds many modules: module_file must name one",
        ),
        (
            {"a": SCENARIO},
            {**JSON_MODULE, "module_file": "../escape.so"},
            "module_file must be a relative path to a .so",
        ),
        ({"a": SCENARIO}, {**JSON_MODULE, "module_file": "/abs/x.so"}, "module_file must be a relative path to a .so"),
        ({"a": SCENARIO.rstrip("\n")}, JSON_MODULE, "does not add exactly that key"),
        ({"a": SCENARIO}, {"sha": "main"}, "full 40-character lowercase commit SHA"),
        ({"a": SCENARIO}, {"repository": "evil; rm -rf /"}, "OWNER/NAME"),
        ({"a": SCENARIO}, {"bundles": {"../up": {}}}, "must be lowercase letters"),
        ({"a": SCENARIO}, {"bundles": {"register": {"model.tla": "x"}}}, "has no claims.yaml"),
        ({"a": SCENARIO}, {"bundles": {"register": {**BUNDLE, "../evil.cfg": "x"}}}, "must be claims.yaml"),
        ({"a": SCENARIO}, {"bundles": {"register": {**BUNDLE, "model.tla": "---- MODULE x ----\n===="}}}, "is invalid"),
        ({"a": SCENARIO}, {"experiments": "x" * 20001}, "experiments is 20001 characters; the limit is 20,000"),
        ({"a": SCENARIO}, {"experiments": "not json"}, "experiments is not JSON"),
        ({"a": SCENARIO}, {"experiments": []}, "experiments must be a JSON object"),
        ({"a": SCENARIO}, {"experiments": {"Register.yaml": PROPOSED}}, "must be <name>.yaml"),
        ({"a": SCENARIO}, {"experiments": {"register.yml": PROPOSED}}, "must be <name>.yaml"),
        ({"a": SCENARIO}, {"experiments": {"../up.yaml": PROPOSED}}, "must be <name>.yaml"),
        ({"a": SCENARIO}, {"experiments": {"r.yaml": 3}}, "experiment r.yaml: the value must be YAML text"),
        ({"a": SCENARIO}, {"experiments": {"r.yaml": "[1]"}}, "experiment r.yaml must be a mapping"),
        ({"a": SCENARIO}, {"experiments": {"r.yaml": "id: toy.x\n"}}, "id must start with 'proposed.'"),
        ({"a": SCENARIO}, {"experiments": {"r.yaml": "name: x\n"}}, "id must start with 'proposed.'"),
        ({"a": SCENARIO}, {"experiments": {"r.yaml": "id: proposed.x\nextends: [nope]\n"}}, "do not load"),
        ({"a": SCENARIO}, {"experiments": {"a.yaml": PROPOSED, "b.yaml": PROPOSED}}, "do not load"),
    ],
)
def test_the_pr_batch_refuses_bad_inputs(tmp_path, scenarios, inputs, message):
    error = _check_pr_batch_inputs(tmp_path, scenarios, inputs)

    assert error and message in error, (message, error)


def test_the_unit_tests_run_on_every_python_version_the_package_declares():
    declared = re.findall(r'"Programming Language :: Python :: (3\.\d+)"', (ROOT / "pyproject.toml").read_text())
    assert _workflow("test.yml")["jobs"]["test"]["strategy"]["matrix"]["python-version"] == declared


def test_the_pr_gating_server_tests_build_the_pinned_valkey_commit():
    steps = _workflow("test.yml")["jobs"]["server-tests"]["steps"]
    (build,) = [step for step in steps if step.get("uses") == "./.github/actions/build-valkey"]
    # Unstable HEAD would let a Valkey regression fail an unrelated PR; the daily live workflow follows it instead.
    assert build["with"]["ref"] != "unstable" and "zoo/valkey-commit" in build["with"]["cache-key"]


def test_the_scheduled_soak_runs_a_build_of_valkey_unstable():
    workflow = _workflow("fuzzer-run.yml")
    # Without --valkey-binary the local deployment builds its fixed fallback release, never a new Valkey commit.
    (binary,) = re.findall(r"--valkey-binary (\S+)", "\n".join(_run_scripts(workflow)))
    (job,) = workflow["jobs"].values()
    (build,) = [step for step in job["steps"] if step.get("uses") == "./.github/actions/build-valkey"]
    assert build["with"] == {"ref": "unstable", "path": binary.removesuffix("/src/valkey-server")}


def test_the_bug_zoo_runs_daily_and_on_dispatch_and_fails_a_case_below_its_rate():
    workflow = _workflow("zoo.yml")
    assert "workflow_dispatch" in workflow["on"] and workflow["on"]["schedule"]
    assert workflow["permissions"] == {"contents": "read"}
    assert workflow["concurrency"]["group"]
    (job,) = workflow["jobs"].values()
    # The schedule runs upstream only; a dispatch runs in any repository, so a fork can try it.
    assert job["if"] == _schedule_guard("zoo.yml")
    assert job["timeout-minutes"]
    scripts = "\n".join(_run_scripts(workflow))
    assert "zoo/build.sh" in scripts and "valkey-fuzzer batch --zoo" in scripts
    # Without this the batch would hide a bug no oracle sees.
    assert "--allow-unsupported" not in scripts
    assert any(step.get("uses", "").startswith("actions/upload-artifact@") for step in job["steps"])


def test_the_release_publishes_only_a_pushed_tag_through_trusted_publishing():
    workflow = _workflow("release.yml")
    assert workflow["on"] == {"push": {"tags": ["v*"]}, "workflow_dispatch": None}
    build, release, pypi = (workflow["jobs"][name] for name in ("build", "github-release", "pypi"))
    scripts = "\n".join(step["run"] for step in build["steps"] if "run" in step)
    assert "python -m build\n" in scripts and "twine check --strict dist/*" in scripts
    # A tag that does not name valkey_fuzzer.__version__ releases nothing.
    (check,) = [step for step in build["steps"] if "GITHUB_REF_NAME#v" in step.get("run", "")]
    assert check["if"] == "github.ref_type == 'tag'"
    # A dispatch (a dry run of the build) and anything but a v* tag reach neither the release nor PyPI.
    gate = "github.event_name == 'push' && startsWith(github.ref, 'refs/tags/v')"
    assert release["needs"] == "build" and release["if"] == gate and release["permissions"] == {"contents": "write"}
    # The release is posted through the API from the downloaded distributions, and never creates the tag.
    (post,) = [step for step in release["steps"] if step.get("uses", "").startswith("actions/github-script@")]
    assert "github.rest.repos.createRelease" in post["with"]["script"] and "tag_name: tag" in post["with"]["script"]
    assert (
        "github.rest.repos.uploadReleaseAsset" in post["with"]["script"]
        and post["env"]["TAG"] == "${{ github.ref_name }}"
    )
    assert pypi["needs"] == "github-release" and pypi["if"] == gate
    # Trusted publishing: the OIDC token of the `pypi` environment the publisher registered on PyPI names, no secret.
    assert pypi["permissions"] == {"id-token": "write"} and pypi["environment"]["name"] == "pypi"
    (publish,) = [step for step in pypi["steps"] if step.get("uses", "").startswith("pypa/gh-action-pypi-publish@")]
    assert "with" not in publish and "secrets." not in (WORKFLOWS / "release.yml").read_text()


def test_the_spec_check_runs_on_every_change_to_main():
    # Its experiment gate compiles the Valkey experiments with the core compiler: a path filter narrower than
    # the whole repository would let a change that breaks the translated scenarios pass.
    triggers = _workflow("tla-model-check.yml")["on"]
    assert all("paths" not in (triggers[event] or {}) for event in ("push", "pull_request"))


def test_the_nightly_spec_check_runs_every_bundles_raised_bounds():
    # The bounds a PR check cannot afford live in each bundle's nightly/ configs; without this job nothing runs them.
    workflow = _workflow("tla-model-check-nightly.yml")
    assert "workflow_dispatch" in workflow["on"] and workflow["on"]["schedule"]
    (job,) = workflow["jobs"].values()
    assert job["if"] == _schedule_guard("tla-model-check-nightly.yml")
    scripts = "\n".join(_run_scripts(workflow))
    assert "for nightly in formal/*/nightly; do" in scripts
    (check,) = [args for args in _fuzzer_commands("tla-model-check-nightly.yml") if args.spec_command == "check"]
    assert check.configs == "nightly" and check.strict
    nightly = sorted(path.parent.name for path in (ROOT / "formal").glob("*/nightly/*.cfg"))
    assert nightly, "no bundle has a nightly config; drop the workflow or add one"


# Each job that runs tests backed by TLC, or a batch that checks its runs against the spec bundles they exercise,
# and the pytest and batch steps in it that need TLC.
TLC_JOBS = {
    ("tla-model-check.yml", "tlc"): 1,
    ("tla-model-check-nightly.yml", "tlc"): 0,
    ("test.yml", "server-tests"): 2,
    ("valkey-experiments-live.yml", "valkey-experiments-live"): 1,
    ("fuzzer-pr-batch.yml", "batch"): 1,
}


def test_the_tla_tools_action_pins_the_release_the_spec_bridge_knows():
    # One pin, the action's: the hint the spec commands print without the jar names the same release, and `spec check
    # --strict` (which the model-check workflows pass) knows the pinned jar by its digest. No workflow has one of its own.
    java, download = _action("tla-tools")["runs"]["steps"]
    assert java["uses"].startswith("actions/setup-java@")
    assert download["env"]["TLA_TOOLS_VERSION"] == TLA_TOOLS_VERSION
    assert TLA_TOOLS_RELEASES[download["env"]["TLA_TOOLS_SHA256"]] == TLA_TOOLS_VERSION
    assert "${TLA_TOOLS_SHA256}  ${RUNNER_TEMP}/tla2tools.jar" in download["run"] and "sha256sum -c" in download["run"]
    for name in WORKFLOW_NAMES:
        assert "TLA_TOOLS_" not in (WORKFLOWS / name).read_text(encoding="utf-8"), name


@pytest.mark.parametrize(("name", "job"), sorted(TLC_JOBS))
def test_every_job_with_tlc_backed_tests_has_java_and_the_pinned_tla_tools(name, job):
    # Without them those tests skip, a batch skips the bundles that need TLC, and CI never checks the spec bridge
    # end to end.
    steps = _workflow(name)["jobs"][job]["steps"]
    (tools,) = [index for index, step in enumerate(steps) if step.get("uses") == "./.github/actions/tla-tools"]
    tests = [step for step in steps if "pytest" in step.get("run", "") or "valkey-fuzzer batch" in step.get("run", "")]
    with_jar = [
        step for step in tests if step.get("env", {}).get("TLA2TOOLS_JAR") == "${{ runner.temp }}/tla2tools.jar"
    ]
    assert len(with_jar) == TLC_JOBS[name, job], [step.get("name") for step in tests]
    assert all(steps.index(step) > tools for step in with_jar)


@pytest.mark.parametrize("name", GITHUB_FILES)
def test_every_checkout_leaves_no_credentials_behind(name):
    # Later steps run scenario, fuzzer, and test code that has no use for the token.
    for step in _steps(name):
        if step.get("uses", "").startswith("actions/checkout@"):
            assert step.get("with", {}).get("persist-credentials") is False, (name, step.get("name"))


def _step_env(workflow, command):
    """The environment of each step of ``workflow`` that runs ``ci.py <command>``: its job's and its own."""

    jobs = _workflow(workflow)["jobs"].values()
    return [
        {**job.get("env", {}), **step.get("env", {})}
        for job in jobs
        for step in job["steps"]
        if re.search(rf"ci\.py {command}\b", step.get("run", ""))
    ]


def test_the_batch_workflows_summarize_through_the_ci_script():
    for workflow in ("fuzzer-pr-batch.yml", "fuzzer-run.yml"):
        (env,) = _step_env(workflow, "summary")
        assert {"SUMMARY_TITLE", "BATCH_DIR"} <= set(env), workflow
        assert "python - <<" not in (WORKFLOWS / workflow).read_text(), "no workflow keeps a script of its own"
    (env,) = _step_env("fuzzer-pr-batch.yml", "check-inputs")
    names = {"INPUT_SCENARIOS", "INPUT_SPEC_BUNDLES", "INPUT_VALKEY_REPOSITORY", "INPUT_VALKEY_SHA"}
    names |= {"INPUT_VALKEY_PARENT_SHA", "INPUT_BUILD", "INPUT_RANDOM_SEEDS", "INPUT_REQUEST_ID"}
    names |= {"INPUT_EXCLUDE_EXPERIMENTS", "INPUT_EXPERIMENTS"}
    assert names | {"CANDIDATES_DIR", "SPEC_DIR", "EXPERIMENTS_DIR", "PARENT_BINARY"} <= set(env)


def test_the_pr_batch_drops_the_write_ledgers_of_the_runs_that_passed(tmp_path):
    batch = tmp_path / "batch"
    runs = []
    for run_id, status in (("passed", "PASS"), ("failed", "FAIL")):
        writes = batch / "runs" / run_id / "components" / "writes"
        writes.mkdir(parents=True)
        for name in ("ledger.jsonl", "preload.jsonl", "result.json"):
            (writes / name).write_text("{}")
        (batch / "runs" / run_id / "report.json").write_text("{}")
        runs.append({"id": run_id, "status": status, "summary": "x", "report": f"runs/{run_id}/report.json"})
    runs.append({"id": "lost", "status": "PASS", "summary": "x", "report": None})
    (batch / "summary.json").write_text(json.dumps({"runs": runs}))
    (env,) = _step_env("fuzzer-pr-batch.yml", "drop-ledgers")
    assert "BATCH_DIR" in env

    with patch.dict(os.environ, {"BATCH_DIR": str(batch)}):
        CI.main("drop-ledgers")

    kept = {path.relative_to(batch).as_posix() for path in batch.rglob("*.*")}
    assert "runs/passed/components/writes/ledger.jsonl" not in kept
    assert "runs/passed/components/writes/preload.jsonl" not in kept
    assert "runs/passed/components/writes/result.json" in kept
    assert {"runs/failed/components/writes/ledger.jsonl", "runs/failed/components/writes/preload.jsonl"} <= kept


def test_the_pr_comment_is_found_by_the_marker_the_summary_writes():
    text = (WORKFLOWS / "valkey-pr-fuzzer.yml").read_text()
    assert CI.COMMENT_MARKER not in text and "const marker = body.split('\\n', 1)[0];" in text


def test_the_batch_summary_lists_every_run_with_its_report_and_the_signatures(tmp_path):
    batch = tmp_path / "batch"
    assert "The batch wrote no summary" in CI.batch_summary("Soak", str(batch))
    folder = batch / "runs" / "random-valkey.cluster-chaos-s31"
    folder.mkdir(parents=True)
    results = (ComponentResult("chaos", ComponentStatus.FAIL, "wave 1 | lost keys"),)
    outcome = RunOutcome(
        plan=valkey_service().compile(generate_scenario(31)),
        status=OutcomeStatus.FAIL,
        summary="chaos failed",
        component_results=results,
        valkey_sha="deadbeef",
    )
    write_document(str(folder / "report.json"), valkey_service().run_report(outcome))
    runs = [
        {"id": folder.name, "status": "FAIL", "seed": 31, "summary": "x", "report": f"runs/{folder.name}/report.json"},
        {"id": "s7", "status": "ERROR", "seed": 7, "summary": "WorkerExit: no run report", "report": None},
    ]
    signatures = [{"count": 1, "label": "FAIL | valkey.cluster-chaos | chaos:FAIL"}]
    conformance = {"replication": {"status": "pass", "approved": True, "counts": {"pass": 3, "unexercised": 1}}}
    (batch / "summary.json").write_text(
        json.dumps(
            {
                "status": "FAIL",
                "counts": {"FAIL": 1, "ERROR": 1},
                "runs": runs,
                "signatures": signatures,
                "conformance": conformance,
            }
        )
    )

    summary = CI.batch_summary("Soak", str(batch), "`valkey-io/valkey` `-` `abc`")

    assert summary.startswith("## Soak\n\n- Valkey: `valkey-io/valkey` `-` `abc`\n- Batch status: `FAIL`")
    plan = outcome.plan
    assert (
        f"| {folder.name} | FAIL | {plan.scenario_id} | 31 | deadbeef | `x` | `chaos: wave 1 / lost keys` |" in summary
    )
    assert "| s7 | ERROR | - | 7 | - | `WorkerExit: no run report` | - |" in summary
    assert "- Signature (1 run(s)): FAIL | valkey.cluster-chaos | chaos:FAIL" in summary
    assert "- Spec conform `replication` (approved claims): `pass` (pass 3, unexercised 1), runs by status" in summary


def test_the_search_chaos_workflow_starts_valkey_search_with_its_index_check_on(tmp_path):
    script = ROOT / ".github" / "scripts" / "module_chaos.py"
    subprocess.run([sys.executable, str(script), str(tmp_path)], check=True, capture_output=True)
    runtime = valkey_service(experiment_directories=(str(tmp_path),))
    # The base the workflow writes: the module, and the module's option beside it, which is how one is set.
    (write_base,) = [item for item in _run_scripts(_workflow("search-chaos.yml")) if "printf" in item]
    (printf,) = re.findall(
        r"printf '%s\\n' (.*?) > /tmp/search-chaos/base\.yaml", write_base.replace("\\\n", " "), re.S
    )
    base = yaml.safe_load("\n".join(shlex.split(printf)))
    module = "/tmp/valkey-search/.build-release/libsearch.so"
    assert base == {"deployment": {"server_config": {"loadmodule": module, "search.rdb-validate-on-write": "yes"}}}

    def plan(name):
        experiment = f"proposed.{name}-chaos"
        return generate(runtime.catalog, 3, compile_fn=runtime.compile, experiment=experiment, base=base).plan

    search = plan("search")
    configs = [instance["server_config"] for instance in to_plain_data(search.instances).values()]
    assert len(configs) >= 2
    for config in configs:
        assert (config["loadmodule"], config["search.rdb-validate-on-write"]) == (module, "yes")
    # A save that fails the check fails the run: the log observer forbids its line, from before the load.
    [logs] = [item for item in search.components if item.use == "valkey.observer.log"]
    assert [rule["pattern"] for rule in to_plain_data(logs.config)["forbidden"]] == [
        "Index validation failed for index"
    ]
    started = [item.id for item in search.ordered_components]
    assert started.index("logs") < started.index("load")
    # The other modules' experiments forbid no line.
    for other in ("json", "bloom"):
        assert not any(item.use == "valkey.observer.log" for item in plan(other).components)


# ---------------------------------------------------------------------------
# The image and the module repositories' reusable workflow (docs/module-repos.md)
# ---------------------------------------------------------------------------

EXAMPLE_EXPERIMENTS = ROOT / "examples" / "valkey" / "experiments"
MODULE_CHAOS_BASE = {
    "deployment": {"server_config": {"loadmodule": "/opt/module/libjson.so"}},
    "experiment": {
        "inputs": {
            "module_command": "JSON.SET",
            "load": [{"command": ["JSON.SET", "j:1", "$", '{"a": 1}'], "expect": {"equals": "OK"}}],
            "during": [{"command": ["JSON.SET", "j:1", "$.a", "2"]}],
            "verify": [{"command": ["JSON.GET", "j:1", "$.a"], "expect": {"equals": "[2]"}}],
            "forbidden_logs": [{"pattern": "Index validation failed"}],
        }
    },
}


def test_the_image_workflow_pushes_only_on_a_tag_and_builds_without_pushing_otherwise():
    workflow = _workflow("build-image.yml")
    assert set(workflow["on"]) == {"pull_request", "push", "workflow_dispatch"}
    assert workflow["on"]["push"] == {"tags": ["v*"]} and "Dockerfile" in workflow["on"]["pull_request"]["paths"]
    assert workflow["permissions"] == {"contents": "read"}
    build, publish = workflow["jobs"]["build"], workflow["jobs"]["publish"]
    assert "permissions" not in build and "push" not in "\n".join(_run_scripts({"jobs": {"build": build}}))
    assert publish["if"] == "github.event_name == 'push' && startsWith(github.ref, 'refs/tags/v')"
    assert publish["needs"] == "build" and publish["permissions"] == {"contents": "read", "packages": "write"}
    image, push = [step for step in publish["steps"] if "docker" in step.get("run", "")]
    assert "ghcr.io/${OWNER,,}/valkey-fuzzer" in image["run"] and image["env"]["TAG"] == "${{ github.ref_name }}"
    # The token reaches the push step alone, after the Dockerfile ran without it.
    assert "secrets." not in str(image) and push["env"]["REGISTRY_TOKEN"] == "${{ secrets.GITHUB_TOKEN }}"
    assert "docker push" in push["run"] and "docker build" not in push["run"]
    # The build job proves the image runs an experiment with valkey-server, as its non-root user.
    (smoke,) = [
        step for step in build["steps"] if "docker run" in step.get("run", "") and "aof_durability" in step["run"]
    ]
    assert "--user" not in smoke["run"]


def test_the_module_fuzz_workflow_keeps_its_inputs_and_runs_in_the_image_or_on_the_runner():
    workflow = _workflow("module-fuzz.yml")
    assert set(workflow["on"]) == {"workflow_call"} and workflow["permissions"] == {"contents": "read"}
    inputs = workflow["on"]["workflow_call"]["inputs"]
    assert {name for name, rule in inputs.items() if rule.get("required")} == {"module", "fuzzer_ref"}
    for name in ("build", "module_artifact", "experiment", "experiment_dir", "base", "image", "valkey_version"):
        assert name in inputs and not inputs[name].get("required"), name
    assert inputs["experiment"]["default"] == "example.module-chaos"
    assert inputs["fail_on_error"] == {"description": inputs["fail_on_error"]["description"], "required": False, "type": "boolean", "default": False}  # fmt: skip
    (job,) = workflow["jobs"].values()
    assert job["container"] == "${{ inputs.image }}" and job["timeout-minutes"] == "${{ inputs.timeout_minutes }}"
    assert "${{ inputs.module }}" in job["concurrency"]["group"] and "${{ inputs.experiment }}" in job["concurrency"]["group"]  # fmt: skip
    assert job["defaults"]["run"]["shell"] == "bash"
    text = (WORKFLOWS / "module-fuzz.yml").read_text(encoding="utf-8")
    assert "secrets." not in text and "github.token" not in text
    steps = {step["name"]: step for step in job["steps"]}
    # What the image brings is built on the runner only, and the fuzzer the caller pinned is installed either way.
    for name in ("Set up Python", "Build Valkey"):
        assert steps[name]["if"] == "inputs.image == ''", name
    assert "if" not in steps["Install the fuzzer"] and steps["Install the fuzzer"]["run"].strip().endswith(" .")
    assert steps["Checkout valkey-fuzzer"]["with"]["ref"] == "${{ inputs.fuzzer_ref }}"
    assert steps["Checkout the module repository"]["with"] == {"path": "module", "persist-credentials": False}
    # The caller's build command and the inputs reach the shell through the environment, never spliced in.
    for step in job["steps"]:
        assert "${{ inputs." not in step.get("run", ""), step["name"]
    assert steps["Build the module"]["if"] == "inputs.build != ''"
    # The batch's exit code is kept for the last step, so the summary and the upload run on a FAIL too.
    assert "set +e" in steps["Run the batch"]["run"] and 'echo "code=$?"' in steps["Run the batch"]["run"]
    assert steps["Summarize the batch"]["if"] == "always()" and steps["Upload the batch"]["if"] == "always()"
    assert steps["Upload the batch"]["with"]["name"] == "${{ inputs.artifact_name }}"
    (env,) = _step_env("module-fuzz.yml", "summary")
    assert {"SUMMARY_TITLE", "BATCH_DIR", "MODULE_REPOSITORY", "MODULE_SHA"} <= set(env)
    (env,) = _step_env("module-fuzz.yml", "module-base")
    assert "BASE_OUT" in env and "MODULE_PATH=" in steps["Write the batch's base file"]["run"]


@pytest.mark.parametrize(
    ("code", "fail_on_error", "fails"),
    [("0", "false", False), ("1", "false", True), ("2", "false", False), ("3", "false", False), ("2", "true", True), ("64", "false", True)],
)  # fmt: skip
def test_the_module_fuzz_workflow_fails_the_job_on_a_fail_and_on_an_error_only_when_asked(code, fail_on_error, fails):
    (job,) = _workflow("module-fuzz.yml")["jobs"].values()
    (step,) = [step for step in job["steps"] if step["name"] == "Fail on FAIL"]
    result = subprocess.run(
        ["bash", "-eo", "pipefail", "-c", step["run"]],
        env={**os.environ, "CODE": code, "FAIL_ON_ERROR": fail_on_error},
        capture_output=True,
        text=True,
    )
    assert (result.returncode != 0) is fails, result.stdout


def test_the_module_base_loads_the_built_module_over_the_callers_base(tmp_path, monkeypatch):
    base = tmp_path / "base.yaml"
    base.write_text(
        yaml.safe_dump(
            {
                "deployment": {"server_config": {"search.rdb-validate-on-write": "yes"}},
                "experiment": {"inputs": {"module_command": "FT.CREATE"}},
            }
        )
    )
    out = tmp_path / "out.yaml"
    monkeypatch.chdir(tmp_path)
    CI.module_base("module/libsearch.so", str(base), str(out))
    assert yaml.safe_load(out.read_text()) == {
        "deployment": {
            "server_config": {
                "search.rdb-validate-on-write": "yes",
                "loadmodule": str(tmp_path / "module" / "libsearch.so"),
            }
        },
        "experiment": {"inputs": {"module_command": "FT.CREATE"}},
    }
    # Without a base, only the module.
    CI.module_base("/opt/module/libjson.so", "", str(out))
    assert yaml.safe_load(out.read_text()) == {
        "deployment": {"server_config": {"loadmodule": "/opt/module/libjson.so"}}
    }
    # A base that loads a module itself would load two, or the wrong one.
    base.write_text(yaml.safe_dump({"deployment": {"server_config": {"loadmodule": "/elsewhere.so"}}}))
    with pytest.raises(SystemExit, match=r"sets deployment\.server_config\.loadmodule"):
        CI.module_base("/opt/module/libjson.so", str(base), str(out))
    base.write_text("- not a mapping\n")
    with pytest.raises(SystemExit, match="must be a mapping"):
        CI.module_base("/opt/module/libjson.so", str(base), str(out))


def test_the_module_chaos_example_takes_a_modules_commands_as_inputs_and_draws_the_faults(tmp_path):
    runtime = valkey_service(experiment_directories=(str(EXAMPLE_EXPERIMENTS),))
    plans = [
        generate(runtime.catalog, seed, compile_fn=runtime.compile, experiment="example.module-chaos", base=MODULE_CHAOS_BASE)
        for seed in range(6)
    ]  # fmt: skip
    components = {item.id: item for item in plans[0].plan.components}
    inputs = MODULE_CHAOS_BASE["experiment"]["inputs"]
    assert to_plain_data(components["build"].config["commands"]) == ["JSON.SET"]
    assert to_plain_data(components["build"].config["deployment_config"]) == ["loadmodule"]
    for name in ("load", "during", "verify"):
        # The compiled items carry the sequence's defaults beside what the input gave.
        compiled = to_plain_data(components[name].config["commands"])
        assert [{key: item[key] for key in given} for item, given in zip(compiled, inputs[name])] == inputs[name], name
    assert components["verify"].config["instances"] == "all"
    assert to_plain_data(components["logs"].config["forbidden"]) == inputs["forbidden_logs"]
    for generated in plans:
        for instance in to_plain_data(generated.plan.instances).values():
            assert instance["server_config"]["loadmodule"] == "/opt/module/libjson.so"
        runtime.validate_plan(generated.plan)
    # The faults vary with the seed; the module's commands do not.
    drawn = {generated.scenario["experiment"]["inputs"]["signal"] for generated in plans}
    assert len(drawn) > 1
    assert all(generated.plan.deployment_mode == "standalone" for generated in plans)
    # Without the module's commands there is no scenario to draw.
    with pytest.raises(GeneratorError, match="missing required field 'module_command'"):
        generate(runtime.catalog, 1, compile_fn=runtime.compile, experiment="example.module-chaos")
    # The shape is .github/scripts/module_chaos.py's: the same components, in the same order, with the module's data.
    script = ROOT / ".github" / "scripts" / "module_chaos.py"
    subprocess.run([sys.executable, str(script), str(tmp_path)], check=True, capture_output=True)
    json_doc = yaml.safe_load((tmp_path / "proposed.json-chaos.yaml").read_text())
    example = yaml.safe_load((EXAMPLE_EXPERIMENTS / "module-chaos.yaml").read_text())
    ids = [item["id"] for item in example["components"]]
    assert ids == ["build", "logs", *(item["id"] for item in json_doc["components"] if item["id"] != "build")]
    assert example["required_events"] == json_doc["required_events"]
    assert example["required_evidence"] == json_doc["required_evidence"]
    assert example["metadata"]["generator"] == json_doc["metadata"]["generator"]
