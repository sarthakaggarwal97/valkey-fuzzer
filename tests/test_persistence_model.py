"""The persistence bundle (``formal/persistence``): its controls, how a run's log lines map onto its steps, and,
with java and ``$TLA2TOOLS_JAR``, the counterexample of a manifest written before the base it names."""

import json
import os
import re
import shutil

import pytest

from valkey_fuzzer.spec.bundle import load_bundle
from valkey_fuzzer.spec.conform import ObservedRun, collect_runs


def test_every_guard_the_model_relies_on_has_a_control_that_breaks_it():
    bundle = load_bundle("persistence")

    assert bundle.claims.status == "draft"
    assert bundle.safe_configs == ("model.cfg",)
    assert {control.name: control.violates for control in bundle.controls} == {
        "manifest_before_base": "ManifestFilesExist",
        "delete_history_first": "ManifestFilesExist",
        "no_flush_before_rewrite": "AofIsPrefix",
        "feed_while_waiting": "AofIsPrefix",
        "ack_before_flush": "AckedRestored",
        "current_incr_to_history": "AckedRestored",
    }
    # Every control is a mutated module: the safe config is Valkey as it is.
    assert all(control.module for control in bundle.controls)
    assert all(not item.conform and item.conform_note for item in bundle.claims.properties)


@pytest.mark.parametrize(
    "line, evidence",
    [
        ("Background append only file rewriting started by pid 4242", "rewrite-started"),
        (
            "Successfully renamed the temporary AOF base file temp-rewriteaof-bg-4242.aof into appendonly.aof.2.base.rdb",
            "base-renamed",
        ),
        ("Background AOF rewrite finished successfully", "rewrite-finished"),
        ("Background AOF rewrite terminated by signal 9", "rewrite-failed"),
        ("Background saving started by pid 4243", "save-started"),
        ("Background saving terminated with success", "save-done"),
    ],
)
def test_each_log_line_shows_one_step(line, evidence):
    mapping = load_bundle("persistence").mapping
    matched = [item.name for item in mapping.evidence.values() if item.log and re.search(item.log, line)]

    assert matched == [evidence]


def test_conform_maps_a_rewrite_and_a_kill_onto_the_steps(tmp_path):
    events = [
        ("persistence.rewrite.started", {"instance_id": "valkey-0"}),
        ("persistence.rewrite.base.renamed", {"instance_id": "valkey-0"}),
        ("persistence.rewrite.finished", {"instance_id": "valkey-0"}),
        ("persistence.save.started", {"instance_id": "valkey-0"}),
        ("kill.injected", {"applied": True, "signal": "SIGKILL", "target": "valkey-0"}),
    ]
    records = [{"kind": "header", "scenario_id": "unit", "seed": 1, "nodes": {}}]
    for index, (name, payload) in enumerate(events):
        records.append({"kind": "event", "t": 1 + index / 10, "sequence": index, "name": name, "payload": payload})
    path = tmp_path / "trace.jsonl"
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n")

    observed = ObservedRun(load_bundle("persistence").mapping, collect_runs([str(path)])[0])
    observed.replay()

    assert not observed.unapplied
    assert dict(observed.actions) == {
        "StartRewrite": 1,
        "RenameBase": 1,
        "DeleteHistory": 1,
        "StartSave": 1,
        "Crash": 1,
    }
    assert observed.states[-1]["crashes"] == 1


def _tla_jar():
    path = os.environ.get("TLA2TOOLS_JAR")
    return path if path and os.path.exists(path) else None


@pytest.mark.slow
@pytest.mark.skipif(not _tla_jar() or not shutil.which("java"), reason="needs java and TLA2TOOLS_JAR")
def test_a_manifest_written_before_its_base_names_a_missing_file():
    from valkey_fuzzer.spec import TlaTools
    from valkey_fuzzer.spec.check import run_control

    bundle = load_bundle("persistence")
    result, _ = run_control(bundle, bundle.control("manifest_before_base"), TlaTools.locate(_tla_jar(), workers="1"))

    assert (result.violation, result.violated) == ("invariant", "ManifestFilesExist")
    assert [state.action for state in result.trace][-2:] == ["StartRewrite", "MutantPersistManifest"]
