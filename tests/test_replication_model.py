"""The replication bundle (``formal/replication``): its controls, how a run's log lines map onto its steps, and,
with java and ``$TLA2TOOLS_JAR``, the counterexample of fuzzer issue #133."""

import json
import os
import shutil

import pytest

from valkey_fuzzer.components import experiments
from valkey_fuzzer.spec.bundle import load_bundle
from valkey_fuzzer.spec.conform import ObservedRun, collect_runs

ID_A = "a" * 40
ID_B = "b" * 40


def test_every_guard_the_model_relies_on_has_a_control_that_breaks_it():
    bundle = load_bundle("replication")

    assert (bundle.claims.status, bundle.claims.approved_by) == ("approved", "sarthakaggarwal97")
    assert bundle.safe_configs == ("dual-channel.cfg", "model.cfg")
    assert {control.name: control.violates for control in bundle.controls} == {
        # Valkey as it is (fuzzer issue #133): the safe configs assume the fix.
        "keep_old_id_on_load": "OneHistoryPerId",
        "keep_id_without_backlog": "OneHistoryPerId",
        "no_shift_on_promotion": "OneHistoryPerId",
        "save_info_without_primary": "OneHistoryPerId",
        "no_second_offset_check": "ReplicaIsPrefix",
    }
    assert {control.name for control in bundle.controls if control.module is None} == {"keep_old_id_on_load"}
    # Both properties compare data sets, which a run does not observe: conform reports coverage only.
    assert all(not item.conform and item.conform_note for item in bundle.claims.properties)


def _run(tmp_path, events):
    records = [{"kind": "header", "scenario_id": "unit", "seed": 1, "nodes": {}}]
    for index, (name, payload) in enumerate(events):
        records.append({"kind": "event", "t": 1 + index / 10, "sequence": index, "name": name, "payload": payload})
    path = tmp_path / "trace.jsonl"
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n")
    observed = ObservedRun(load_bundle("replication").mapping, collect_runs([str(path)])[0])
    observed.replay()
    return observed


def test_conform_maps_a_failover_run_onto_the_replication_steps(tmp_path):
    # valkey-1 full-syncs over two channels, valkey-2 continues; valkey-0 is killed, valkey-1 promoted, and
    # valkey-2 re-pointed to it continues on the secondary ID.
    observed = _run(
        tmp_path,
        [
            ("replication.dual.started", {"instance_id": "valkey-1"}),
            ("replication.partial.continued", {"instance_id": "valkey-2"}),
            ("stall.injected", {"applied": True, "signal": "SIGSTOP", "target": "valkey-0"}),
            ("kill.injected", {"applied": True, "signal": "SIGKILL", "target": "valkey-0"}),
            ("replication.link.lost", {"instance_id": "valkey-1"}),
            ("replication.promoted", {"instance_id": "valkey-1", "old": ID_A, "offset": 1201, "new": ID_B}),
            ("replication.replicaof", {"instance_id": "valkey-2", "host": "127.0.0.1", "port": 7001}),
            ("replication.partial.continued", {"instance_id": "valkey-2"}),
        ],
    )

    assert not observed.unapplied
    assert observed.members == {"Nodes": ["valkey-0", "valkey-1", "valkey-2"]}
    assert dict(observed.actions) == {
        "DualFork": 1,
        "PartialSync": 2,
        "Crash": 1,
        "Disconnect": 1,
        "Promote": 1,
        "Follow": 1,
    }
    final = observed.states[-1]
    assert final["role"] == {"node_valkey_1": "primary", "node_valkey_2": "replica", "node_valkey_0": "unknown"}
    assert final["link"]["node_valkey_2"] == "up"
    assert (final["crashes"], final["switches"]) == (1, 2)


def test_a_replication_chaos_kill_is_a_crash():
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as folder:
        observed = _run(
            Path(folder),
            [
                ("chaos.instance-killed", {"instance_id": "valkey-0", "signal": "SIGTERM", "success": True}),
                # Known-bad: a target that exited before the kill was not killed.
                ("chaos.instance-killed", {"instance_id": "valkey-2", "signal": "SIGKILL", "success": False}),
            ],
        )
    assert dict(observed.actions) == {"Crash": 1} and observed.members == {"Nodes": ["valkey-0"]}
    assert observed.states[-1]["crashes"] == 1


@pytest.mark.parametrize(
    "experiment_id",
    ["valkey.replication-chaos", "valkey.replication-full-resync", "valkey.replication-partial-resync"],
)
def test_the_replication_experiments_publish_the_models_steps_before_they_act(experiment_id):
    (experiment,) = [item for item in experiments() if item.experiment_id == experiment_id]
    order = [component.id for component in experiment.components]
    (steps,) = [component for component in experiment.components if component.id == "replication-steps"]
    assert (steps.use, steps.config["spec"]) == ("valkey.observer.log-trigger", "replication")
    patterns = load_bundle("replication").mapping.log_patterns()
    assert sorted(rule["publish"] for rule in steps.config["rules"]) == sorted(patterns)
    actions = [component.id for component in experiment.components if component.use.split(".")[1] == "action"]
    assert actions and all(order.index("replication-steps") < order.index(action) for action in actions)


def _tla_jar():
    path = os.environ.get("TLA2TOOLS_JAR")
    return path if path and os.path.exists(path) else None


@pytest.mark.slow
@pytest.mark.skipif(not _tla_jar() or not shutil.which("java"), reason="needs java and TLA2TOOLS_JAR")
def test_a_promotion_before_continue_names_two_histories_with_one_id():
    from valkey_fuzzer.spec import TlaTools
    from valkey_fuzzer.spec.check import run_control

    bundle = load_bundle("replication")
    result, _ = run_control(bundle, bundle.control("keep_old_id_on_load"), TlaTools.locate(_tla_jar(), workers="1"))

    assert (result.violation, result.violated) == ("invariant", "OneHistoryPerId")
    steps = [state.action for state in result.trace]
    # Fuzzer issue #133: the snapshot loads before +CONTINUE, and the replica is promoted on its old ID.
    assert steps[-2:] == ["DualLoadFirst", "Promote"]
