"""The replication bundle (``formal/replication``): its controls, how a run's log lines map onto its steps, and,
with java and ``$TLA2TOOLS_JAR``, the counterexample of fuzzer issue #133."""

import pytest

from tests.conform_support import events, write_trace
from tests.conftest import needs_tlc
from valkey_fuzzer.components import experiments
from valkey_fuzzer.spec.bundle import load_bundle
from valkey_fuzzer.spec.conform import ObservedRun, collect_runs
from valkey_fuzzer.spec.tla import parse_config

ID_A = "a" * 40
ID_B = "b" * 40


def test_every_guard_the_model_relies_on_has_a_control_that_breaks_it():
    bundle = load_bundle("replication")

    # Approved on 2026-11-12 under AdoptOnLoad = TRUE, which Valkey does not do; a draft again since the claims say
    # what Valkey does, and SameOffsetSameData is new.
    assert (bundle.claims.status, bundle.claims.approved_by) == ("draft", None)
    assert bundle.safe_configs == ("dual-channel.cfg", "model.cfg")
    assert {control.name: control.violates for control in bundle.controls} == {
        # Valkey as it is (fuzzer issue #133): the safe configs assume the fix.
        "keep_old_id_on_load": "OneHistoryPerId",
        "keep_id_without_backlog": "OneHistoryPerId",
        "no_shift_on_promotion": "OneHistoryPerId",
        "save_info_without_primary": "OneHistoryPerId",
        "no_second_offset_check": "ReplicaIsPrefix",
        "write_without_offset": "SameOffsetSameData",
    }
    assert {control.name for control in bundle.controls if control.module is None} == {"keep_old_id_on_load"}
    # The two properties over data sets as sequences are model only; the one over sampled IDs, offsets and
    # digests is checked on runs.
    conformed = {item.name: item.conform for item in bundle.claims.properties}
    assert conformed == {"OneHistoryPerId": False, "ReplicaIsPrefix": False, "SameOffsetSameData": True}
    assert all(item.conform or item.conform_note for item in bundle.claims.properties)


def test_the_claims_hold_for_valkey_as_it_is_which_conform_reads_from_the_run():
    # Single channel, and a load before +CONTINUE keeps the old ID (fuzzer issue #133): what Valkey does and what
    # model.cfg checks. A run's channel comes from the trace header's dual-channel-replication-enabled, so a
    # dual-channel run is outside the claim; AdoptOnLoad is pinned to what Valkey does.
    bundle = load_bundle("replication")
    constants = bundle.mapping.constants

    for item in bundle.claims.properties:
        assert item.holds_when == {"DualChannel": "FALSE", "AdoptOnLoad": "FALSE"}, item.name
    assert constants["DualChannel"] == {"config": "dual-channel-replication-enabled", "equals": "yes"}
    assert str(constants["AdoptOnLoad"]["value"]) == "FALSE"
    assert parse_config(bundle.files["model.cfg"]).constants["AdoptOnLoad"] == "FALSE"


NO_ID = "0" * 40


def _info(role, replid, offset, *, link="up", syncing=False, replid2=NO_ID, second_offset=-1, backlog=1):
    """INFO replication as the trace records it: every field as text."""

    fields = {
        "role": role,
        "master_replid": replid,
        "master_replid2": replid2,
        "master_repl_offset": str(offset),
        "second_repl_offset": str(second_offset),
        "repl_backlog_active": "1" if backlog else "0",
        "repl_backlog_first_byte_offset": str(backlog),
    }
    if role == "slave":
        fields.update(master_link_status=link, master_sync_in_progress="1" if syncing else "0")
    return fields


def _sample(node, t, info, digest):
    return {
        "kind": "observation",
        "round": 1,
        "reason": "periodic",
        "node": node,
        "t": t,
        "info": info,
        "digest": digest,
    }


def _trace(tmp_path, records, dual_channel="no"):
    """A trace of ``records`` (``(t, name, payload)`` events, or observation records), with every node's channel."""

    nodes = {f"valkey-{n}": {"config": {"dual-channel-replication-enabled": dual_channel}} for n in range(3)}
    lines = [
        {"kind": "event", "t": record[0], "sequence": index, "name": record[1], "payload": record[2]}
        if isinstance(record, tuple)
        else record
        for index, record in enumerate(records)
    ]
    return write_trace(tmp_path / "trace.jsonl", lines, nodes=nodes)


def _run(tmp_path, pairs):
    observed = ObservedRun(load_bundle("replication").mapping, collect_runs([_trace(tmp_path, events(pairs))])[0])
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


def test_samples_observe_each_nodes_ids_offsets_and_digest(tmp_path):
    # valkey-0 is the primary; valkey-1 streams from it, valkey-2 is in a full sync (its link down) and still
    # reports its old ID over data being replaced, so its sample is left out until it streams.
    records = [
        _sample("valkey-0", 1.0, _info("master", ID_A, 120), "d120"),
        _sample("valkey-1", 1.1, _info("slave", ID_A, 120), "d120"),
        _sample("valkey-2", 1.2, _info("slave", ID_B, 40, link="down", syncing=True), "half"),
        (1.3, "replication.promoted", {"instance_id": "valkey-1", "old": ID_A, "offset": 121, "new": ID_B}),
        _sample("valkey-1", 1.4, _info("master", ID_B, 120, replid2=ID_A, second_offset=121), "d120"),
    ]
    observed = ObservedRun(load_bundle("replication").mapping, collect_runs([_trace(tmp_path, records)])[0])
    observed.replay()

    # The syncing replica's sample matches no evidence, so nothing it reports enters a state.
    assert not observed.unapplied and observed.members == {"Nodes": ["valkey-0", "valkey-1"]}
    final = observed.states[-1]
    assert final["role"] == {"node_valkey_0": "primary", "node_valkey_1": "primary"}
    assert final["replid"] == {"node_valkey_0": ID_A, "node_valkey_1": ID_B}
    assert final["off"] == {"node_valkey_0": 120, "node_valkey_1": 120}
    assert final["data"] == {"node_valkey_0": "d120", "node_valkey_1": "d120"}
    # Forty zeros and -1 are Valkey's "none"; the model's is 0.
    assert final["replid2"] == {"node_valkey_0": 0, "node_valkey_1": ID_A}
    assert final["secondOff"] == {"node_valkey_0": 0, "node_valkey_1": 121}
    # A sample is an observed state, not a step: the steps are the events' alone.
    assert dict(observed.actions) == {"Promote": 1}
    assert [step["why"] for step in observed.steps[1:]] == [
        "sample-primary sample", "sample-replica sample", "Promote", "sample-primary sample"
    ]  # fmt: skip
    # The sample after the promotion agrees with it: no disagreement to report.
    assert observed.sample_mismatches == []
    assert observed.constants()["DualChannel"] == "FALSE"


def test_a_sample_that_disagrees_with_the_events_is_reported_not_overwritten_in_silence(tmp_path):
    # The log says valkey-1 was promoted, and the sample after it still shows a replica: the sample is taken as
    # the observed state, and the disagreement is reported by variable and node.
    records = [
        (1.0, "replication.promoted", {"instance_id": "valkey-1", "old": ID_A, "offset": 121, "new": ID_B}),
        _sample("valkey-1", 1.1, _info("slave", ID_A, 120), "d120"),
        _sample("valkey-1", 1.2, _info("slave", ID_A, 130), "d130"),
    ]
    observed = ObservedRun(load_bundle("replication").mapping, collect_runs([_trace(tmp_path, records)])[0])
    observed.replay()

    assert observed.states[-1]["role"]["node_valkey_1"] == "replica"
    assert observed.sample_mismatches == [
        {"evidence": "sample-replica", "var": "role", "key": "node_valkey_1", "inferred": '"primary"',
         "sampled": '"replica"', "t": 1.1}
    ]  # fmt: skip
    # A sample that supersedes a sample is what samples do: not a disagreement.
    assert len(observed.sample_mismatches) == 1


@pytest.mark.slow
@needs_tlc
def test_conform_checks_same_offset_same_data_on_sampled_runs(tmp_path, tla_tools):
    from valkey_fuzzer.spec.conform import conform_runs

    bundle = load_bundle("replication")
    partial = (1.3, "replication.partial.continued", {"instance_id": "valkey-1"})
    agree = [
        _sample("valkey-0", 1.0, _info("master", ID_A, 120), "d120"),
        _sample("valkey-1", 1.1, _info("slave", ID_A, 100), "d100"),
        partial,
        _sample("valkey-1", 1.4, _info("slave", ID_A, 120), "d120"),
    ]
    # Known-bad: the replica stands at the primary's offset of the primary's ID with other data.
    disagree = [*agree[:3], _sample("valkey-1", 1.4, _info("slave", ID_A, 120), "other")]
    runs = collect_runs(
        [_trace(tmp_path / "agree", agree), _trace(tmp_path / "disagree", disagree), _trace(tmp_path / "dual", disagree, "yes")]
    )  # fmt: skip
    report = conform_runs(bundle, runs, tla_tools)

    holds, violated, outside = report["runs"]
    assert holds["status"] == "pass" and holds["properties"]["SameOffsetSameData"] == {
        "status": "holds",
        "claimed": True,
    }
    assert violated["status"] == "fail" and violated["properties"]["SameOffsetSameData"]["states"] == [5]
    assert violated["properties"]["SameOffsetSameData"]["first"]["evidence"] == "sample-replica"
    # Over two channels the claim is not made: the violation is reported, not held against the run.
    assert outside["status"] == "pass" and outside["properties"]["SameOffsetSameData"]["claimed"] is False
    assert outside["constants"]["DualChannel"] == "TRUE"


@pytest.mark.slow
@needs_tlc
def test_a_promotion_before_continue_names_two_histories_with_one_id(tla_tools):
    from valkey_fuzzer.spec.check import run_control

    bundle = load_bundle("replication")
    result, _ = run_control(bundle, bundle.control("keep_old_id_on_load"), tla_tools)

    assert (result.violation, result.violated) == ("invariant", "OneHistoryPerId")
    steps = [state.action for state in result.trace]
    # Fuzzer issue #133: the snapshot loads before +CONTINUE, and the replica is promoted on its old ID.
    assert steps[-2:] == ["DualLoadFirst", "Promote"]
