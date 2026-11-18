"""What a run's report is trusted for: a run whose report cannot be written fails, replay refuses changed
code or a plan its scenario does not compile to, minimization keeps only the same failure, and a differential
comparison never passes on missing evidence."""

import json
import shlex
import shutil
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from tests.support import make_plan, recorded_report
from valkey_fuzzer import provenance, reports
from valkey_fuzzer.cli import main
from valkey_fuzzer.differential import Differential, compare_outcomes, differential_status
from valkey_fuzzer.minimize import failure_signature, same_failure
from valkey_fuzzer.reports import InputError, ReportWriteError, export_scenario, write_plan
from valkey_fuzzer.results import ComponentResult, ComponentStatus, Evidence, OutcomeStatus, RunOutcome
from valkey_fuzzer.service import ReplayDriftError

GENERATED = ["--seed", "9", "--experiment", "valkey.cluster-chaos"]
EXAMPLE = str(Path(__file__).resolve().parents[1] / "examples" / "valkey" / "snapshot_equivalence.yaml")
DIFFERENTIAL = Path(__file__).resolve().parents[1] / "examples" / "valkey" / "differential_baseline_vs_candidate.yaml"


@pytest.mark.usefixtures("report_root")
@pytest.mark.parametrize(
    ("argv", "status", "code"),
    [
        (["run", *GENERATED, "--export-scenario"], OutcomeStatus.FAIL, 1),
        (["run", EXAMPLE, "--output"], OutcomeStatus.PASS, 2),
        # A FAIL stands when only the copy of its report cannot be written, as for run.
        (["replay", "REPORT", "--output"], OutcomeStatus.FAIL, 1),
        (["replay", "REPORT", "--output"], OutcomeStatus.PASS, 2),
        (["diff", str(DIFFERENTIAL), "--output"], OutcomeStatus.FAIL, 1),
    ],
)
def test_a_run_whose_requested_output_cannot_be_written_does_not_succeed(
    tmp_path, canned_runs, capsys, argv, status, code
):
    canned_runs.status = status
    (tmp_path / "file").write_text("")
    if "REPORT" in argv:
        report = str(tmp_path / "recorded.json")
        with patch("sys.argv", ["valkey-fuzzer", "run", EXAMPLE, "--output", report]):
            main()
        argv = [report if item == "REPORT" else item for item in argv]
    with patch("sys.argv", ["valkey-fuzzer", *argv, str(tmp_path / "file" / "out")]):
        assert main() == code
    assert "could not" in capsys.readouterr().err


def _edited(package, relative):
    """Make ``package`` load from a copy of its folder in which ``relative`` is edited."""

    def edit(tmp_path, monkeypatch):
        module = sys.modules[package]
        copy = tmp_path / "edited" / package
        shutil.copytree(Path(module.__file__).parent, copy, ignore=shutil.ignore_patterns("__pycache__"))
        (copy / relative).write_text((copy / relative).read_text() + "\n# changed\n")
        monkeypatch.setattr(module, "__file__", str(copy / "__init__.py"))

    return edit


def _seed(report):
    report["scenario"]["seed"] += 1


@pytest.mark.parametrize(
    ("changed", "edit", "item"),
    [
        # Shared code no component class is defined in: the verdict, and a helper the Valkey components call.
        (_edited("valkey_fuzzer", "verdict.py"), None, "fuzzer code"),
        (_edited("valkey_fuzzer", "components/polling.py"), None, "fuzzer code"),
        (_edited("valkey_fuzzer", "deployment/local/provider.py"), None, "deployment provider for standalone code"),
        (None, _seed, "the report's scenario and plan"),
    ],
)
def test_replay_refuses_a_report_whose_code_or_plan_changed(service, tmp_path, monkeypatch, changed, edit, item):
    recorded = recorded_report(service, tmp_path, edit)
    assert edit or service.prepare_replay(recorded).verified
    if changed is not None:
        changed(tmp_path, monkeypatch)

    with pytest.raises(ReplayDriftError) as raised:
        service.prepare_replay(recorded)
    entries = {entry["item"]: entry for entry in raised.value.drift}
    assert item in entries and entries[item]["recorded"] != entries[item]["replay"]


def test_replay_treats_code_without_a_recorded_digest_as_drift(service, tmp_path):
    def drop_digests(report):
        report["reproducer"]["fuzzer_digest"] = None

    recorded = recorded_report(service, tmp_path, drop_digests)
    with pytest.raises(ReplayDriftError) as raised:
        service.prepare_replay(recorded)
    assert {"item": "fuzzer code", "recorded": None, "replay": raised.value.drift[0]["replay"]} in raised.value.drift
    assert service.prepare_replay(recorded, allow_drift=True).drift


def test_a_digest_missing_on_both_sides_is_drift(service, tmp_path, monkeypatch):
    def drop_digests(report):
        report["reproducer"]["fuzzer_digest"] = None
        report["reproducer"]["experiment"]["digest"] = None

    recorded = recorded_report(service, tmp_path, drop_digests)
    monkeypatch.setattr(provenance, "_fuzzer_digest", lambda: None)
    monkeypatch.setattr(provenance, "_experiment_digest", lambda experiment: None)
    with pytest.raises(ReplayDriftError) as raised:
        service.prepare_replay(recorded)
    items = {entry["item"]: entry for entry in raised.value.drift}
    assert items["fuzzer code"]["recorded"] is None and items["fuzzer code"]["replay"] is None
    assert any(item.endswith(" digest") and "experiment" in item for item in items)


@pytest.mark.parametrize(
    ("key", "value", "item", "drifts"),
    [
        ("python", "2.7.18", "python", True),
        ("platform", "Plan9-1-vax", "platform", True),
        ("python", "{major}.{minor}.99", "python", False),
    ],
)
def test_replay_compares_the_python_minor_version_and_the_platform(service, tmp_path, key, value, item, drifts):
    value = value.format(major=sys.version_info.major, minor=sys.version_info.minor)

    def edit(report):
        report["reproducer"][key] = value

    recorded = recorded_report(service, tmp_path, edit)
    if drifts:
        with pytest.raises(ReplayDriftError) as raised:
            service.prepare_replay(recorded)
        assert [entry["item"] for entry in raised.value.drift] == [item]
    else:
        assert not service.prepare_replay(recorded).drift


def test_a_report_recording_no_python_or_platform_leaves_them_unchecked(service, tmp_path):
    def edit(report):
        report["reproducer"]["python"] = report["reproducer"]["platform"] = None

    prepared = service.prepare_replay(recorded_report(service, tmp_path, edit))
    assert not prepared.drift and {"python: the recorded run has none", "platform: the recorded run has none"} <= set(
        prepared.not_verified
    )


def test_a_bare_plan_is_refused_unless_provenance_sits_beside_it_or_drift_is_allowed(service, tmp_path):
    plan = service.compile(yaml.safe_load(Path(EXAMPLE).read_text()))
    bare = tmp_path / "bare" / "plan.json"
    bare.parent.mkdir()
    bare.write_text(plan.to_json())
    with pytest.raises(InputError, match=r"bare plan.*--allow-drift"):
        service.prepare_replay(str(bare))
    assert not service.prepare_replay(str(bare), allow_drift=True).verified

    # The run's own folder verifies: reproducer.json beside plan.json, even for a run that never finished.
    folder = tmp_path / "run"
    folder.mkdir()
    write_plan(str(folder), plan, provenance.reproducer(service.catalog, plan))
    prepared = service.prepare_replay(str(folder / "plan.json"))
    assert prepared.verified and not prepared.drift
    reproducer = json.loads((folder / "reproducer.json").read_text())
    reproducer["fuzzer_digest"] = "0" * 64
    (folder / "reproducer.json").write_text(json.dumps(reproducer))
    with pytest.raises(ReplayDriftError):
        service.prepare_replay(str(folder / "plan.json"))
    # --allow-drift on a verifiable folder still records the drift it found.
    assert service.prepare_replay(str(folder / "plan.json"), allow_drift=True).drift

    # An older fuzzer's reproducer.json that lacks keys is an input error, not a crash.
    del reproducer["python"]
    (folder / "reproducer.json").write_text(json.dumps(reproducer))
    with pytest.raises(InputError, match="not a complete reproducer"):
        service.prepare_replay(str(folder / "plan.json"), allow_drift=True)


def _failing(*results, diagnostics=None):
    results = tuple(ComponentResult(*result) for result in results)
    return RunOutcome(make_plan(), OutcomeStatus.FAIL, "FAIL", component_results=results, diagnostics=diagnostics or {})


LOST = ("ledger", ComponentStatus.FAIL, "3 acknowledged write(s) lost (budget 0); valkey-4: RDB failed to load")


@pytest.mark.parametrize(
    ("candidate", "same"),
    [
        (_failing(("ledger", ComponentStatus.FAIL, "1 acknowledged write(s) lost (budget 0); valkey-2: RDB failed to load")), True),
        (_failing(("ledger", ComponentStatus.FAIL, "cannot read dataset: ConnectionError")), False),
        (_failing(("ledger", ComponentStatus.ERROR, "component execution failed")), False),
        (_failing(LOST, diagnostics={"missing_required_events": ["bgsave.save-started"]}), False),
    ],
    ids=["other-counts-and-instances", "other-verdict", "fail-became-error", "missing-event"],
)  # fmt: skip
def test_minimization_keeps_only_the_same_failure_of_a_component(candidate, same):
    assert same_failure(failure_signature(_failing(LOST)), failure_signature(candidate)) is same


def test_a_differential_item_without_the_compared_field_leaves_the_comparison_unmade():
    base = {"seed": 5, "experiment": {"id": "valkey.snapshot-equivalence"}, "deployment": {"mode": "standalone"}}
    rule = {"evidence_id": "valkey.dataset.digest", "source": "dataset", "field": "digest"}
    differential = Differential.from_document(
        {"differential_id": "unit", "base": base, "variants": {"a": {}, "b": {}}, "compare": {"equal": [rule]}}
    )

    def digests(*values):
        evidence = tuple(Evidence("valkey.dataset.digest", "dataset", "oracle.completed", 1.0, item) for item in values)
        return RunOutcome(plan=make_plan(), status=OutcomeStatus.PASS, summary="PASS", evidence=evidence)

    # The item without a digest counts: dropped, both variants would compare as "d1".
    outcomes = {"a": digests({"digest": "d1"}, {"shard": 2}), "b": digests({"digest": "d1"})}
    comparisons = compare_outcomes(differential, outcomes)
    assert comparisons[0]["result"] == "missing"
    # Not a pass, and not a finding about the server either: the harness could not compare.
    assert differential_status(differential, outcomes, comparisons)[0] == "ERROR"


def test_a_report_with_a_small_float_replays_and_minimizes_as_written(service, canned_runs, tmp_path):
    # The report JSON holds 5e-05, which a YAML 1.1 loader reads back as the string "5e-05".
    scenario = tmp_path / "cancel.yaml"
    scenario.write_text(
        "scenario_id: cancel\nseed: 11\nexperiment:\n  id: valkey.aof-durability\n"
        "  inputs: {run_after_crash_s: 0.00005}\ndeployment: {mode: standalone, replicas_per_primary: 1}\n"
    )
    canned_runs.status = OutcomeStatus.FAIL
    prepared = service.prepare_run(str(scenario))
    record = service.execute(prepared.plan, scenario=prepared.scenario, report_dir=str(tmp_path / "r"))
    assert '"run_after_crash_s": 5e-05' in Path(record.report_path).read_text()

    assert service.prepare_replay(record.report_path).drift == []
    minimization = service.prepare_minimization(record.report_path)
    assert minimization.scenario["experiment"]["inputs"]["run_after_crash_s"] == 5e-05

    duplicated = tmp_path / "duplicated.json"
    duplicated.write_text('{"outcome": 1, "outcome": 2}')
    with pytest.raises(InputError, match="duplicate key 'outcome'"):
        service.prepare_replay(str(duplicated))


def test_an_exported_scenario_replaces_the_old_file_whole_or_not_at_all(tmp_path, monkeypatch):
    target = tmp_path / "scenario.yaml"
    target.write_text("old: scenario\n", encoding="utf-8")

    def interrupted(source, destination):
        raise OSError("disk full")

    monkeypatch.setattr("valkey_fuzzer.reports.os.replace", interrupted)
    with pytest.raises(ReportWriteError, match="could not write the scenario"):
        export_scenario({"scenario_id": "s", "seed": 1}, str(target))
    assert target.read_text(encoding="utf-8") == "old: scenario\n" and list(tmp_path.iterdir()) == [target]


# The keys of a report.json and of its reproducer before the readable layout; sarthakaggarwal97/valkey-ci-agent
# reads them, so none may go or change its name.
LEGACY_REPORT_KEYS = {
    "generated_at",
    "status",
    "summary",
    "scenario",
    "outcome",
    "coverage",
    "reproducer",
    "provenance",
}
LEGACY_REPRODUCER_KEYS = {
    "python",
    "platform",
    "experiment",
    "fuzzer_digest",
    "components",
    "deployment_provider",
    "plan_digest",
    "valkey_sha",
}


def _failed_report(service, canned_runs, tmp_path):
    canned_runs.status = OutcomeStatus.FAIL
    canned_runs.evidence = lambda plan: ()
    prepared = service.prepare_run(EXAMPLE)
    record = service.execute(prepared.plan, scenario=prepared.scenario, report_dir=str(tmp_path / "r"))
    return record, json.loads((tmp_path / "r" / "report.json").read_text())


def test_report_json_leads_with_the_verdict_and_keeps_every_key_a_tool_reads(service, canned_runs, tmp_path):
    _, report = _failed_report(service, canned_runs, tmp_path)
    assert list(report)[:5] == ["status", "summary", "failed_components", "reproduce", "look_at"]
    assert set(report) >= LEGACY_REPORT_KEYS
    assert set(report["reproducer"]) >= LEGACY_REPRODUCER_KEYS
    assert [row["component_id"] for row in report["failed_components"]] == ["data"]
    assert report["failed_components"][0]["summary"] == "lost"
    assert report["look_at"]["report_folder"] == str(tmp_path / "r")
    # The data a tool reads is unchanged: the full component results stay in the outcome.
    assert [row["component_id"] for row in report["outcome"]["component_results"]] == ["data"]


def test_the_report_records_the_command_and_binary_that_reproduce_it(service, canned_runs, tmp_path):
    prepared = service.prepare_run(EXAMPLE, valkey_binary="/opt/my builds/valkey")
    record = service.execute(prepared.plan, scenario=prepared.scenario, report_dir=str(tmp_path / "r"))
    report = json.loads((tmp_path / "r" / "report.json").read_text())
    assert report["reproduce"] == report["reproducer"]["command"]
    assert shlex.split(report["reproduce"]) == ["valkey-fuzzer", "replay", record.report_path]
    assert report["reproducer"]["valkey_binary"] == "/opt/my builds/valkey"


def test_a_long_value_in_failed_components_is_cut_with_its_length_and_digest_and_the_outcome_keeps_it(
    service, canned_runs, tmp_path
):
    long_value = "k" * 4000

    def run(plan, *, deadline=None, run_dir=None):
        evidence = (Evidence("valkey.data.violation", "data", "oracle", 1.0, long_value),)
        failing = ComponentResult("data", ComponentStatus.FAIL, "s" * 500, evidence=evidence)
        return RunOutcome(plan, OutcomeStatus.FAIL, "FAIL", component_results=(failing,))

    with patch("valkey_fuzzer.runner.Runner.run", staticmethod(run)):
        prepared = service.prepare_run(EXAMPLE)
        service.execute(prepared.plan, scenario=prepared.scenario, report_dir=str(tmp_path / "r"))
    report = json.loads((tmp_path / "r" / "report.json").read_text())
    (row,) = report["failed_components"]
    assert row["summary"].startswith("s" * 120 + "...") and "500 chars" in row["summary"] and "sha256" in row["summary"]
    shown = row["evidence"][0]["value"]
    assert len(shown) < 200 and "sha256" in shown and long_value not in shown
    assert report["outcome"]["component_results"][0]["evidence"][0]["value"] == long_value


def test_shorten_leaves_a_short_value_alone_and_gives_equal_digests_to_equal_values():
    assert reports.shorten("abc") == "abc" and reports.shorten({"a": 1}) == '{"a": 1}'
    assert reports.shorten("z" * 121) == reports.shorten("z" * 121) != reports.shorten("y" * 121)
    assert reports.shorten("z" * 120) == "z" * 120


def test_an_old_report_without_the_readable_keys_still_loads_and_replays(service, canned_runs, tmp_path):
    def drop_new_keys(report):
        for key in ("failed_components", "reproduce", "look_at"):
            del report[key]
        del report["reproducer"]["command"], report["reproducer"]["valkey_binary"]

    path = recorded_report(service, tmp_path, edit=drop_new_keys)
    assert reports.as_run_report(json.loads(Path(path).read_text()), path) is not None
    prepared = service.prepare_replay(path)
    assert prepared.verified
