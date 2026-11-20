"""The findings store: what a batch appends to it, what it keeps across batches, and the ``findings`` command."""

import json
import os
from pathlib import Path

import pytest

from tests.conftest import cli
from valkey_fuzzer import findings
from valkey_fuzzer.outcomes import EXIT_USAGE
from valkey_fuzzer.reports import FORMAT_VERSION, InputError

FIRST, SECOND, THIRD = "2026-11-20T01:00:00Z", "2026-11-21T01:00:00Z", "2026-11-22T01:00:00Z"


def _summary(folder, *groups, seen_at=FIRST, reports=True):
    """A batch summary of ``folder`` with one signature group per ``(key, run ids)``, as summarize() writes it."""

    runs = [run_id for _, run_ids in groups for run_id in run_ids]
    return {
        "folder": str(folder),
        "batch_id": os.path.basename(str(folder)),
        "updated_at": seen_at,
        "runs": [{"id": run_id, "report": f"runs/{run_id}/report.json" if reports else None} for run_id in runs],
        "signatures": [
            {
                "key": key,
                "label": f"FAIL | valkey.x | data:FAIL | lost N keys ({key})",
                "status": "FAIL",
                "experiment": "valkey.x",
                "signature": {"status": "FAIL", "failing_components": ["data"]},
                "count": len(run_ids),
                "runs": list(run_ids),
            }
            for key, run_ids in groups
        ],
    }


def test_a_batch_records_one_entry_per_signature_with_its_runs_and_a_reproduce_command(tmp_path):
    store = tmp_path / "findings.json"
    first = tmp_path / "soak" / "first"
    touched = findings.record(str(store), _summary(first, ("aaa", ["s0", "s1"]), ("bbb", ["s2"])))

    data = json.loads(store.read_text())
    assert data["format_version"] == FORMAT_VERSION and sorted(data["findings"]) == ["aaa", "bbb"] == sorted(touched)
    entry = data["findings"]["aaa"]
    assert entry["status"] == "new" and entry["count"] == 2 and entry["first_seen"] == entry["last_seen"] == FIRST
    assert entry["runs"] == [{"batch": str(first), "id": "s0"}, {"batch": str(first), "id": "s1"}]
    assert entry["reproduce"] == f"valkey-fuzzer replay {first}/runs/s1/report.json", "the latest run's report"
    assert entry["label"].startswith("FAIL | valkey.x") and entry["experiment"] == "valkey.x"
    assert entry["outcome"] == "FAIL" and entry["signature"] == {"status": "FAIL", "failing_components": ["data"]}

    # The same batch recorded again (a resume, a re-summary) adds nothing: a run counts once.
    assert findings.record(str(store), _summary(first, ("aaa", ["s0", "s1"]), ("bbb", ["s2"]))) == {}
    assert json.loads(store.read_text()) == data


def test_a_signature_a_second_batch_sees_is_known_and_a_fixed_one_seen_again_is_new(tmp_path):
    store = str(tmp_path / "findings.json")
    first, second, third = (tmp_path / "soak" / name for name in ("first", "second", "third"))
    findings.record(store, _summary(first, ("aaa", ["s0"]), ("bbb", ["s1"])))
    touched = findings.record(store, _summary(second, ("aaa", ["s0"]), ("ccc", ["s3"]), seen_at=SECOND))

    assert {key: entry["status"] for key, entry in touched.items()} == {"aaa": "known", "ccc": "new"}
    known = touched["aaa"]
    assert known["count"] == 2 and (known["first_seen"], known["last_seen"]) == (FIRST, SECOND)
    assert known["reproduce"] == f"valkey-fuzzer replay {second}/runs/s0/report.json"
    # A finding nobody saw again keeps its status and times.
    data = findings.load(store)
    assert data["findings"]["bbb"]["status"] == "new" and data["findings"]["bbb"]["last_seen"] == FIRST

    # Someone marks aaa fixed by hand; the next batch that hits it makes it new again.
    data["findings"]["aaa"]["status"] = "fixed"
    Path(store).write_text(json.dumps(data))
    touched = findings.record(store, _summary(third, ("aaa", ["s9"]), seen_at=THIRD))
    assert touched["aaa"]["status"] == "new" and touched["aaa"]["count"] == 3 and touched["aaa"]["last_seen"] == THIRD
    # And the listing puts new before known before fixed, the last seen first within a status.
    data = findings.load(store)
    data["findings"]["bbb"]["status"] = "fixed"
    assert [entry["key"] for entry in findings.entries(data)] == ["aaa", "ccc", "bbb"]
    assert [entry["key"] for entry in findings.entries(data, "fixed")] == ["bbb"]


def test_the_reproduce_command_falls_back_to_the_scenario_and_then_stays(tmp_path):
    store = str(tmp_path / "findings.json")
    folder = tmp_path / "batch"
    # A worker that died left a scenario but no report: the scenario runs again. Neither: nothing to say.
    (folder / "runs" / "s0").mkdir(parents=True)
    (folder / "runs" / "s0" / "scenario.yaml").write_text("{}")
    touched = findings.record(store, _summary(folder, ("aaa", ["s0"]), ("bbb", ["s1"]), reports=False))
    assert touched["aaa"]["reproduce"] == f"valkey-fuzzer run {folder}/runs/s0/scenario.yaml"
    assert touched["bbb"]["reproduce"] is None
    # A later run without a report keeps the command that works.
    touched = findings.record(store, _summary(tmp_path / "later", ("aaa", ["s4"]), reports=False, seen_at=SECOND))
    assert touched["aaa"]["reproduce"] == f"valkey-fuzzer run {folder}/runs/s0/scenario.yaml"


def test_find_takes_a_key_or_a_prefix_that_names_one_finding(tmp_path):
    store = str(tmp_path / "findings.json")
    findings.record(store, _summary(tmp_path / "b", ("abc123", ["s0"]), ("abd456", ["s1"])))
    data = findings.load(store)
    assert findings.find(data, "abc123", store)["key"] == "abc123"
    assert findings.find(data, "abd", store)["key"] == "abd456"
    with pytest.raises(InputError, match=r"'ab' names 2 findings in .*: abc123, abd456"):
        findings.find(data, "ab", store)
    with pytest.raises(InputError, match="no finding 'zzz' in"):
        findings.find(data, "zzz", store)


def test_a_store_another_version_wrote_or_edited_wrong_is_refused(tmp_path):
    store = tmp_path / "findings.json"
    store.write_text(json.dumps({"format_version": FORMAT_VERSION + 1, "findings": {}}))
    with pytest.raises(InputError, match=f"has format_version {FORMAT_VERSION + 1}"):
        findings.load(str(store))
    store.write_text(json.dumps({"format_version": FORMAT_VERSION, "runs": []}))
    with pytest.raises(InputError, match="is not a findings store"):
        findings.load(str(store))
    store.write_text(json.dumps({"format_version": FORMAT_VERSION, "findings": {"k": {"status": "done"}}}))
    with pytest.raises(InputError, match="finding k needs a status out of new, known, fixed"):
        findings.load(str(store))
    # A missing store is empty, so the first batch can append to it.
    assert findings.load(str(tmp_path / "none.json")) == {"format_version": FORMAT_VERSION, "findings": {}}


def test_the_findings_command_lists_and_shows_a_store_or_the_one_in_a_batch_folder(tmp_path, capsys, monkeypatch):
    folder = tmp_path / "batch"
    folder.mkdir()
    store = folder / "findings.json"
    findings.record(str(store), _summary(folder, ("aaa111aaa111", ["s0", "s1"]), ("bbb222bbb222", ["s2"])))
    findings.record(str(store), _summary(tmp_path / "other", ("aaa111aaa111", ["s0"]), seen_at=SECOND))

    assert cli("findings", "list", "--store", folder) == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].split() == ["status", "runs", "first", "seen", "last", "seen", "key", "label"]
    assert lines[1].split()[:5] == ["new", "1", "2026-11-20", "2026-11-20", "bbb222bbb222"]
    assert lines[2].split()[:5] == ["known", "3", "2026-11-20", "2026-11-21", "aaa111aaa111"]
    assert cli("findings", "list", "--store", store, "--status", "fixed") == 0
    assert capsys.readouterr().out == f"No fixed findings in {store}\n"

    assert cli("findings", "show", "aaa111", "--store", store) == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["key"] == "aaa111aaa111" and shown["status"] == "known" and len(shown["runs"]) == 3
    assert cli("findings", "show", "zzz", "--store", store) == EXIT_USAGE
    assert "no finding 'zzz'" in capsys.readouterr().err

    # The default store is findings.json in the working directory; a missing one is an input error that says
    # where a batch writes it.
    monkeypatch.chdir(folder)
    assert cli("findings", "list") == 0 and "aaa111aaa111" in capsys.readouterr().out
    monkeypatch.chdir(tmp_path)
    assert cli("findings", "list") == EXIT_USAGE
    assert "no findings store at findings.json: a finished batch writes findings.json" in capsys.readouterr().err
