"""CLI commands for listing, replay, differential runs, campaigns, coverage, and minimization.

Plans are compiled by a real runtime with the Valkey packs plugin; only
``run_plan`` is replaced, so no Valkey processes start.
"""

import json
from pathlib import Path

import pytest
import yaml

from tests.valkey_packs_support import valkey_entry_points, valkey_runtime
from valkey_fuzzer import cli, feature_commands
from valkey_fuzzer.extensibility import (
    ComponentResult,
    ComponentStatus,
    Evidence,
    OutcomeStatus,
    RunOutcome,
    RunPlan,
    to_plain_data,
)

EXAMPLES = Path(__file__).resolve().parents[1] / "examples" / "valkey"


class _Runs:
    def __init__(self):
        self.plans = []
        self.status = OutcomeStatus.PASS
        self.evidence = lambda plan: ()


@pytest.fixture
def runs(monkeypatch):
    runtime = valkey_runtime()
    record = _Runs()

    def run_plan(plan, *, deadline=None):
        record.plans.append(plan)
        failing = ()
        if record.status is OutcomeStatus.FAIL:
            failing = (ComponentResult(component_instance_id="data", status=ComponentStatus.FAIL, summary="lost"),)
        return RunOutcome(
            plan=plan,
            status=record.status,
            summary=record.status.value,
            component_results=failing,
            evidence=tuple(record.evidence(plan)),
            started_at=1.0,
            ended_at=2.0,
        )

    monkeypatch.setattr(runtime, "run_plan", run_plan)
    monkeypatch.setattr(feature_commands, "runtime_for", lambda args, artifact_dir=None: runtime)
    monkeypatch.setattr(cli.FuzzerCLI, "_runtime", staticmethod(lambda args: runtime))
    return record


def _args(*argv):
    with valkey_entry_points():
        return cli.create_parser().parse_args(list(argv))


def _run_report(tmp_path, runs, scenario="forkless_save.yaml"):
    output = tmp_path / "report.json"
    args = _args("run", str(EXAMPLES / scenario), "--output", str(output), "--artifact-dir", str(tmp_path / "a"))
    assert cli.FuzzerCLI().run_scenario(args) == 0
    return output


def test_list_packs_and_components_as_json(capsys):
    with valkey_entry_points():
        assert feature_commands.list_command(_args("list", "packs", "--format", "json")) == 0
        packs = json.loads(capsys.readouterr().out)
        assert feature_commands.list_command(_args("list", "components", "--format", "json")) == 0
        components = json.loads(capsys.readouterr().out)
    assert {"valkey.forkless-save", "valkey.rolling-upgrade"} <= {pack["pack_id"] for pack in packs}
    assert len(components) == 24
    assert all(row["implementation"].startswith("valkey_fuzzer_packs.") for row in components)


def test_run_report_carries_the_scenario_and_reproducer(tmp_path, runs):
    report = json.loads(_run_report(tmp_path, runs).read_text())

    assert report["scenario"]["feature"]["use"] == "valkey.forkless-save"
    assert report["reproducer"]["pack"]["pack_id"] == "valkey.forkless-save"
    assert report["reproducer"]["pack"]["digest"]
    assert report["reproducer"]["plugins"][0]["plugin_id"] == "valkey-fuzzer.valkey-packs"
    used = {component["component_id"] for component in report["reproducer"]["components"]}
    assert {"valkey.bgsave", "valkey.liveness"} <= used
    assert RunOutcome.from_data(report["results"][0]).status is OutcomeStatus.PASS


def test_replay_reruns_the_recorded_plan(tmp_path, runs):
    report = _run_report(tmp_path, runs)
    replay = tmp_path / "replay.json"

    code = feature_commands.replay_command(
        _args("replay", str(report), "--output", str(replay), "--artifact-dir", str(tmp_path / "r"))
    )

    assert code == 0
    assert runs.plans[1] == runs.plans[0]
    replayed = json.loads(replay.read_text())
    assert replayed["schema"] == feature_commands.REPORT_SCHEMA
    assert replayed["scenario"]["scenario_id"] == "forkless-save"
    assert list((tmp_path / "r" / "replays").glob("*/plan.json"))


def test_replay_accepts_a_plan_and_overrides_its_binary(tmp_path, runs):
    plan = valkey_runtime().compile_yaml((EXAMPLES / "forkless_save.yaml").read_text())
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(to_plain_data(plan)))

    code = feature_commands.replay_command(
        _args("replay", str(path), "--valkey-binary", "/opt/valkey", "--artifact-dir", str(tmp_path / "r"))
    )

    assert code == 0
    assert runs.plans[0].topology_document["valkey_binary"] == "/opt/valkey"
    assert RunPlan.from_data(to_plain_data(runs.plans[0])).pack_id == plan.pack_id


def test_diff_compares_variant_evidence(tmp_path, runs, capsys):
    def evidence(plan):
        save_type = plan.topology_document["server_config"]["bgsave-default-method"]
        return (
            Evidence("valkey.dataset.digest", "dataset", "oracle.completed", 1.0, {"digest": "same"}),
            Evidence("valkey.rdb.reload_verified", "snapshot", "oracle.completed", 1.0, {"content_digest": "same"}),
            Evidence("valkey.workload.ledger", "workload", "workload.stopped", 1.0, {"replies_digest": "same"}),
            Evidence(
                "valkey.bgsave.result",
                "bgsave",
                "action.completed",
                1.0,
                {"after": {"rdb_last_bgsave_type": save_type}},
            ),
        )

    runs.evidence = evidence
    output = tmp_path / "diff.json"
    code = feature_commands.diff_command(
        _args(
            "diff",
            str(EXAMPLES / "differential_fork_vs_forkless.yaml"),
            "--output",
            str(output),
            "--artifact-dir",
            str(tmp_path / "d"),
        )
    )

    assert code == 0, capsys.readouterr().out
    report = json.loads(output.read_text())
    assert report["status"] == "PASS"
    assert [item["result"] for item in report["comparisons"]] == ["ok"] * 4
    assert {plan.seed for plan in runs.plans} == {41}


def test_diff_fails_when_an_equal_rule_diverges(tmp_path, runs):
    runs.evidence = lambda plan: (
        Evidence(
            "valkey.dataset.digest",
            "dataset",
            "oracle.completed",
            1.0,
            {"digest": plan.topology_document["server_config"]["bgsave-default-method"]},
        ),
    )
    code = feature_commands.diff_command(
        _args("diff", str(EXAMPLES / "differential_fork_vs_forkless.yaml"), "--artifact-dir", str(tmp_path / "d"))
    )
    assert code == feature_commands.EXIT_CODES["FAIL"]


def test_campaign_runs_untried_tuples_and_records_coverage(tmp_path, runs, capsys):
    coverage = tmp_path / "coverage.json"
    campaign = EXAMPLES / "forkless_campaign.yaml"

    dry = feature_commands.campaign_command(_args("campaign", str(campaign), "--dry-run"))
    assert dry == 0
    assert "9 cases, 6 scheduled" in capsys.readouterr().out
    assert not runs.plans

    code = feature_commands.campaign_command(
        _args(
            "campaign",
            str(campaign),
            "--coverage",
            str(coverage),
            "--max-runs",
            "2",
            "--artifact-dir",
            str(tmp_path / "c"),
        )
    )

    assert code == 0
    assert len(runs.plans) == 2
    database = json.loads(coverage.read_text())
    assert len(database["tuples"]) == 2

    assert feature_commands.coverage_command(_args("coverage", str(coverage), "--campaign", str(campaign))) == 0
    text = capsys.readouterr().out
    assert "2/9 tuple(s) covered, 7 untried" in text


def test_campaign_exit_code_reflects_failures(tmp_path, runs):
    runs.status = OutcomeStatus.FAIL
    code = feature_commands.campaign_command(
        _args(
            "campaign",
            str(EXAMPLES / "forkless_campaign.yaml"),
            "--max-runs",
            "1",
            "--artifact-dir",
            str(tmp_path / "c"),
        )
    )
    assert code == feature_commands.EXIT_CODES["FAIL"]


def test_minimize_shrinks_inputs_while_the_failure_persists(tmp_path, runs):
    runs.status = OutcomeStatus.FAIL
    report = _run_report_failing(tmp_path, runs)
    output = tmp_path / "minimized.yaml"

    code = feature_commands.minimize_command(
        _args("minimize", str(report), "--output", str(output), "--max-attempts", "8", "--artifact-dir", str(tmp_path))
    )

    assert code == 0
    minimized = yaml.safe_load(output.read_text())
    original = yaml.safe_load((EXAMPLES / "forkless_save.yaml").read_text())
    assert minimized["feature"]["with"] != original["feature"]["with"]
    assert minimized["feature"]["with"]["stable_keys"] < original["feature"]["with"]["stable_keys"]


def _run_report_failing(tmp_path, runs):
    output = tmp_path / "report.json"
    args = _args("run", str(EXAMPLES / "forkless_save.yaml"), "--output", str(output))
    assert cli.FuzzerCLI().run_scenario(args) == cli.EXIT_FEATURE_FAILURE
    return output


def test_minimize_leaves_passing_runs_alone(tmp_path, runs, capsys):
    report = _run_report(tmp_path, runs)
    assert feature_commands.minimize_command(_args("minimize", str(report))) == 0
    assert "Nothing to minimize" in capsys.readouterr().out
    assert len(runs.plans) == 1


def test_commands_report_bad_inputs_as_harness_errors(tmp_path, capsys):
    missing = str(tmp_path / "missing.yaml")
    assert feature_commands.diff_command(_args("diff", missing)) == feature_commands.EXIT_CODES["ERROR"]
    assert feature_commands.campaign_command(_args("campaign", missing)) == feature_commands.EXIT_CODES["ERROR"]
    assert feature_commands.coverage_command(_args("coverage", missing)) == feature_commands.EXIT_CODES["ERROR"]
    assert "not found" in capsys.readouterr().out


def test_a_case_that_fails_execution_preflight_is_recorded_as_error(tmp_path, runs, monkeypatch, capsys):
    calls = {"count": 0}

    def run_plan(plan, *, deadline=None):
        calls["count"] += 1
        raise ValueError("unknown node_configs selector 'replica'")

    runtime = feature_commands.runtime_for(None)
    monkeypatch.setattr(runtime, "run_plan", run_plan)
    code = feature_commands.campaign_command(
        _args(
            "campaign",
            str(EXAMPLES / "forkless_campaign.yaml"),
            "--max-runs",
            "2",
            "--artifact-dir",
            str(tmp_path / "c"),
        )
    )
    assert calls["count"] == 2, "one failing case must not abort the campaign"
    assert code == feature_commands.EXIT_CODES["ERROR"]
    assert "run failed before execution" in capsys.readouterr().out


def test_a_campaign_that_runs_nothing_is_an_error(tmp_path, runs):
    document = yaml.safe_load((EXAMPLES / "forkless_campaign.yaml").read_text())
    document["matrix"] = {"topology.type": ["no-such-topology"]}
    path = tmp_path / "campaign.yaml"
    path.write_text(yaml.safe_dump(document))
    code = feature_commands.campaign_command(_args("campaign", str(path), "--artifact-dir", str(tmp_path / "c")))
    assert code == feature_commands.EXIT_CODES["ERROR"]
    assert not runs.plans


def test_campaign_ids_must_form_valid_scenario_ids(tmp_path, runs):
    document = yaml.safe_load((EXAMPLES / "forkless_campaign.yaml").read_text())
    document["campaign_id"] = "forkless matrix"
    path = tmp_path / "campaign.yaml"
    path.write_text(yaml.safe_dump(document))
    assert feature_commands.campaign_command(_args("campaign", str(path))) == feature_commands.EXIT_CODES["ERROR"]


def test_rerunning_a_campaign_explores_new_seeds(tmp_path, runs):
    coverage = tmp_path / "coverage.json"
    for _ in range(2):
        feature_commands.campaign_command(
            _args(
                "campaign",
                str(EXAMPLES / "forkless_campaign.yaml"),
                "--coverage",
                str(coverage),
                "--max-runs",
                "9",
                "--artifact-dir",
                str(tmp_path / "c"),
            )
        )
    first, second = runs.plans[:9], runs.plans[9:]
    assert len(second) == 9
    assert {plan.seed for plan in first}.isdisjoint({plan.seed for plan in second})


def test_valkey_binary_override_is_recorded_and_reused_by_minimize(tmp_path, runs):
    runs.status = OutcomeStatus.FAIL
    output = tmp_path / "report.json"
    args = _args("run", str(EXAMPLES / "forkless_save.yaml"), "--output", str(output), "--valkey-binary", "/cand")
    cli.FuzzerCLI().run_scenario(args)
    report = json.loads(output.read_text())
    assert report["scenario"]["topology"]["valkey_binary"] == "/cand"

    feature_commands.minimize_command(_args("minimize", str(output), "--max-attempts", "2"))
    assert {plan.topology_document["valkey_binary"] for plan in runs.plans} == {"/cand"}


def test_yaml_reports_can_be_replayed(tmp_path, runs):
    output = tmp_path / "report.yaml"
    args = _args("run", str(EXAMPLES / "forkless_save.yaml"), "--output", str(output), "--format", "yaml")
    assert cli.FuzzerCLI().run_scenario(args) == 0
    assert feature_commands.replay_command(_args("replay", str(output), "--artifact-dir", str(tmp_path))) == 0
    assert runs.plans[1] == runs.plans[0]


def test_replay_honors_allow_unsupported(tmp_path, runs):
    output = _run_report(tmp_path, runs)
    runs.status = OutcomeStatus.SKIP_UNSUPPORTED
    args = _args("replay", str(output), "--artifact-dir", str(tmp_path))
    assert feature_commands.replay_command(args) == feature_commands.EXIT_CODES["SKIP_UNSUPPORTED"]
    args = _args("replay", str(output), "--allow-unsupported", "--artifact-dir", str(tmp_path))
    assert feature_commands.replay_command(args) == 0
