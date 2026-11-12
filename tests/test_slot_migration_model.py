"""The slot migration bundle (``formal/slot-migration``): its controls, how a run's log lines map onto its steps, and,
with java and ``$TLA2TOOLS_JAR``, the counterexample of a cancel after the grant."""

import json
import os
import re
import shutil

import pytest

from valkey_fuzzer.spec.bundle import load_bundle
from valkey_fuzzer.spec.conform import ObservedRun, collect_runs

JOB = "{name: " + "a" * 40 + ", operation: %s, %s_node_id: %s, slots: 7629-7629}"
SOURCE_ID = "b" * 40
TARGET_ID = "c" * 40


def test_every_guard_the_model_relies_on_has_a_control_that_breaks_it():
    bundle = load_bundle("slot-migration")

    assert bundle.claims.status == "draft"
    assert bundle.safe_configs == ("model.cfg",)
    assert {control.name: control.violates for control in bundle.controls} == {
        # Valkey as it is: the safe config assumes a cancel after the grant keeps the source paused, and that the
        # target's replica learns the claim first.
        "cancel_unpauses": "OneWriter",
        "replica_misses_claim": "NoAckedWriteLost",
        # The documented window: the source's pause ends before the target's claim reaches it.
        "pause_expires_after_claim": "OneWriter",
        "grant_without_pause": "OneWriter",
        "grant_after_pause_ends": "OneWriter",
        "drop_unpauses_after_grant": "OneWriter",
        "feed_after_fork": "NoAckedWriteLost",
    }
    assert {control.name for control in bundle.controls if control.module is None} == {
        "cancel_unpauses",
        "replica_misses_claim",
        "pause_expires_after_claim",
    }
    # Both properties need each node's view and data, which a run does not observe: conform reports coverage only.
    assert all(not item.conform and item.conform_note for item in bundle.claims.properties)


@pytest.mark.parametrize(
    "line, evidence",
    [
        ("Slot migration initiated through CLUSTER MIGRATESLOTS command: x (user request from 'id=3')", "started"),
        ("New slot import job created: " + JOB % ("import", "source", SOURCE_ID) + ".", "established"),
        ("New slot import job created: " + JOB % ("import", "source", "unknown") + ".", "tracking"),
        ("Slot migration " + JOB % ("export", "target", TARGET_ID) + " connection lost", "link-lost"),
        (
            "Slot migration " + JOB % ("export", "target", TARGET_ID) + " state transition: "
            "failover-paused -> failover-granted",
            "granted",
        ),
        (
            "Slot migration " + JOB % ("import", "source", SOURCE_ID) + " state transition: "
            "waiting-for-paused -> failover-requested",
            "requested",
        ),
    ],
)
def test_each_log_line_shows_one_step(line, evidence):
    mapping = load_bundle("slot-migration").mapping
    matched = [item.name for item in mapping.evidence.values() if item.log and re.search(item.log, line)]

    assert matched == [evidence]


def test_the_target_losing_the_link_is_not_the_source_losing_it():
    mapping = load_bundle("slot-migration").mapping
    line = "Slot migration " + JOB % ("import", "source", SOURCE_ID) + " connection lost"

    assert not re.search(mapping.event("link-lost").log, line)


def test_conform_maps_a_migration_run_onto_the_steps(tmp_path):
    events = [
        ("slot-migration.started", {"instance_id": "valkey-0"}),
        ("slot-migration.import.created", {"instance_id": "valkey-1"}),
        ("slot-migration.tracking.created", {"instance_id": "valkey-2"}),
        ("slot-migration.snapshot.started", {"instance_id": "valkey-0"}),
        ("slot-migration.snapshot.received", {"instance_id": "valkey-1"}),
        ("slot-migration.paused", {"instance_id": "valkey-0"}),
        ("slot-migration.failover.requested", {"instance_id": "valkey-1"}),
        ("slot-migration.failover.granted", {"instance_id": "valkey-0"}),
        ("slot-migration.cancelled", {"instance_id": "valkey-0"}),
        ("slot-migration.claimed", {"instance_id": "valkey-1"}),
        ("slot-migration.owner.learned", {"instance_id": "valkey-0"}),
    ]
    records = [{"kind": "header", "scenario_id": "unit", "seed": 1, "nodes": {}}]
    for index, (name, payload) in enumerate(events):
        records.append({"kind": "event", "t": 1 + index / 10, "sequence": index, "name": name, "payload": payload})
    path = tmp_path / "trace.jsonl"
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n")

    observed = ObservedRun(load_bundle("slot-migration").mapping, collect_runs([str(path)])[0])
    observed.replay()

    assert not observed.unapplied
    assert dict(observed.actions) == {
        "Start": 1,
        "TEstablish": 1,
        "RApply": 1,
        "SFork": 1,
        "TApply": 1,
        "SPause": 1,
        "TPaused": 1,
        "SGrant": 1,
        "Cancel": 1,
        "TClaim": 1,
        "Deliver": 1,
    }
    assert observed.states[-1]["faults"] == 1


def _tla_jar():
    path = os.environ.get("TLA2TOOLS_JAR")
    return path if path and os.path.exists(path) else None


@pytest.mark.slow
@pytest.mark.skipif(not _tla_jar() or not shutil.which("java"), reason="needs java and TLA2TOOLS_JAR")
def test_a_cancel_after_the_grant_leaves_two_writers():
    from valkey_fuzzer.spec import TlaTools
    from valkey_fuzzer.spec.check import run_control

    bundle = load_bundle("slot-migration")
    result, _ = run_control(bundle, bundle.control("cancel_unpauses"), TlaTools.locate(_tla_jar(), workers="1"))

    assert (result.violation, result.violated) == ("invariant", "OneWriter")
    steps = [state.action for state in result.trace]
    # The target claimed the slot when it read FAILOVER-GRANTED; the cancel then unpaused the source.
    assert steps[-3:] == ["SGrant", "TClaim", "Cancel"]


@pytest.mark.slow
@pytest.mark.skipif(not _tla_jar() or not shutil.which("java"), reason="needs java and TLA2TOOLS_JAR")
def test_a_replica_promoted_without_the_claim_deletes_the_imported_slot():
    from valkey_fuzzer.spec import TlaTools
    from valkey_fuzzer.spec.check import run_control

    bundle = load_bundle("slot-migration")
    result, _ = run_control(bundle, bundle.control("replica_misses_claim"), TlaTools.locate(_tla_jar(), workers="1"))

    assert (result.violation, result.violated) == ("invariant", "NoAckedWriteLost")
    steps = [state.action for state in result.trace]
    # The trace issue #140 reproduces on unstable: the source learned the claim and deleted its keys, the target
    # died, its claim to the replica was lost, and the replica, promoted with the source as owner in its view,
    # deleted the keys it had applied.
    assert "TClaim" in steps and "RApply" in steps[steps.index("TClaim") :]
    assert steps[-3:] == ["CrashT", "LoseBus", "Promote"]
    last = result.trace[-1].values
    assert last["view"]["R"]["owner"] == "S" and last["data"]["S"] == () and last["data"]["R"] == ()
