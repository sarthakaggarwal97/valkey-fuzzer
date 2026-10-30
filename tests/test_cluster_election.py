"""Event triggers, the log trigger, valkey.oracle.cluster-election-safety, and formal/cluster-election through
`valkey-fuzzer spec conform`."""

import json
import os
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from tests.support import VirtualClock
from tests.valkey_experiments_support import fake_context, resolved
from valkey_fuzzer.contracts import ComponentRole, ComponentSpec, ComponentStatus, Evidence
from valkey_fuzzer.extensions import plain_config
from valkey_fuzzer.spec import collect_runs, load_bundle, load_mapping
from valkey_fuzzer_experiments import cluster, common
from valkey_fuzzer_experiments.cluster import (
    TRACE_EVIDENCE,
    ElectionAction,
    ElectionSafetyOracle,
    parse_slots,
    split_primaries,
)
from valkey_fuzzer_experiments.common import log_line_time
from valkey_fuzzer_experiments.faults import FAULT_INJECTED, CommandFault
from valkey_fuzzer_experiments.observers import LogTriggerObserver

ID = {name: name[-1] * 40 for name in ("valkey-0", "valkey-1", "valkey-2", "valkey-3")}


class FakeHandle:
    """Instances valkey-0 up to ``count``; each client answers ``replies`` and records its commands."""

    def __init__(self, count=2, replies=lambda node_id: {}, log_dir=""):
        self.instances = tuple(
            SimpleNamespace(instance_id=f"valkey-{n}", shard_id=0, port=7000 + n) for n in range(count)
        )
        self.replies, self.log_dir, self.commands = replies, log_dir, {}

    def is_alive(self, node_id):
        return True

    def primaries(self):
        return ["valkey-0"]

    def replicas(self):
        return [node.instance_id for node in self.instances[1:]]

    def log_file(self, node_id):
        return os.path.join(self.log_dir, f"{node_id}.log")

    def new_client(self, node_id, timeout=5.0):
        handle, replies = self, self.replies(node_id)

        class Client:
            def execute_command(self, *args):
                handle.commands.setdefault(node_id, []).append(args)
                reply = replies.get(args, "OK")
                if isinstance(reply, Exception):
                    raise reply
                return reply

            def config_get(self, name):
                return {name: replies.get(("CONFIG", name), "sync")}

            def close(self):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *exc_info):
                pass

        return Client()


def test_a_fault_fires_on_the_nth_matching_event_and_fails_when_that_event_never_came():
    handle = FakeHandle()
    context = fake_context(handle)
    when = {"instance_id": ["valkey-1", "valkey-3"], "epoch": {"min": 4}, "flag": {"exists": False}}
    config = {"when_event": "vote", "occurrence": 2, "when_payload": when, "instance": "event", "command": ["PING"]}
    spec = resolved(ComponentSpec(role=ComponentRole.FAULT, use="valkey.fault.command", id="fault", config=config))
    fault = CommandFault()
    fault.inject(context, spec)
    for instance_id, epoch in (("valkey-1", 4), ("valkey-0", 4), ("valkey-1", 3), ("valkey-1", 5), ("valkey-1", 6)):
        context.event_bus.publish("vote", source="t", payload={"instance_id": instance_id, "epoch": epoch})
    result = fault.recover(context, spec)
    assert result.status is ComponentStatus.PASS and handle.commands == {"valkey-1": [("PING",)]}
    record = next(item.value for item in result.evidence if item.evidence_id == FAULT_INJECTED)
    assert record["occurrence"] == 2 and record["trigger_payload"]["epoch"] == 5

    context = fake_context(FakeHandle())
    fault = CommandFault()
    fault.inject(context, spec)
    context.event_bus.publish("vote", source="t", payload={"instance_id": "valkey-1", "epoch": 4})
    result = fault.recover(context, spec)
    assert result.status is ComponentStatus.FAIL and "matched 1 time(s), occurrence 2 needed" in result.summary


def test_the_log_trigger_publishes_whole_new_lines_with_their_named_groups(tmp_path):
    handle = FakeHandle(log_dir=str(tmp_path))
    Path(handle.log_file("valkey-0")).write_text("4242:M 25 Oct 2026 21:04:54.100 * old line before the start\n")
    Path(handle.log_file("valkey-1")).write_text("")
    rules = [{"pattern": load_mapping("cluster-election").event("granted").log, "publish": "cluster.vote.granted",
              "evidence_id": "t.vote-logged", "max_publishes": 1}]  # fmt: skip
    spec = resolved(
        ComponentSpec(
            role=ComponentRole.OBSERVER,
            use="valkey.observer.log-trigger",
            id="logs",
            config={"rules": rules, "poll_interval_s": 10, "switch_interval_s": 0.0005},
        )
    )
    context = fake_context(handle, artifact_dir=str(tmp_path / "artifacts"))
    observer = LogTriggerObserver()
    before = sys.getswitchinterval()
    observer.start(context, spec)
    assert sys.getswitchinterval() == pytest.approx(0.0005)
    line = f"4242:M 25 Oct 2026 21:04:54.250 * Failover auth granted to {'a' * 40} (127.0.0.1:7001) for epoch 12\n"
    with open(handle.log_file("valkey-0"), "a") as log:
        log.write(line[:30])
    assert observer.poll() == 0, "a partial line is held back"
    with open(handle.log_file("valkey-0"), "a") as log:
        log.write(line[30:] + line)
    assert observer.poll() == 1, "publishes stop at max_publishes"
    event = context.event_bus.committed_history("cluster.vote.granted")[0]
    assert (event.payload["instance_id"], event.payload["candidate"], event.payload["epoch"]) == (
        "valkey-0",
        "a" * 40,
        12,
    )
    assert event.payload["logged_at"] == pytest.approx(log_line_time(line)) and log_line_time("no stamp") is None
    # A log rotated or truncated under the observer is read again from its start.
    Path(handle.log_file("valkey-0")).write_text(line)
    observer.poll()
    result = observer.stop(context, spec)
    assert result.status is ComponentStatus.PASS and result.evidence[0].evidence_id == "t.vote-logged"
    assert result.evidence[0].value["counts"] == {"valkey-0": 3} and sys.getswitchinterval() == pytest.approx(before)


# ---------------------------------------------------------------------------
# valkey.oracle.cluster-election-safety
# ---------------------------------------------------------------------------

_NODES = (
    f"{ID['valkey-0']} 127.0.0.1:7000@17000 master - 0 0 3 connected 0-8191\n"
    f"{ID['valkey-1']} 127.0.0.1:7001@17001 slave {ID['valkey-0']} 0 0 3 connected\n"
    f"{ID['valkey-2']} 127.0.0.1:7002@17002 master - 0 0 4 connected 8192-16383\n"
    f"{ID['valkey-3']} 127.0.0.1:7003@17003 slave {ID['valkey-2']} 0 0 4 connected\n"
)


def _safety_handle(save_behavior, views=None):
    """Two converged shards: valkey-0 owns 0-8191 at epoch 3, valkey-2 owns the rest at epoch 4; ``views`` maps
    a node to the CLUSTER NODES it answers instead (or the error it raises)."""

    def replies(node_id):
        address = f"{ID[node_id]} 127.0.0.1:700{node_id[-1]}@1700{node_id[-1]} "
        nodes = (views or {}).get(node_id, _NODES)
        return {
            ("CLUSTER", "MYID"): ID[node_id],
            ("CLUSTER", "INFO"): "cluster_state:ok\r\ncluster_current_epoch:6\r\n",
            ("CLUSTER", "NODES"): nodes
            if isinstance(nodes, Exception)
            else nodes.replace(address, address + "myself,"),
            ("CONFIG", "cluster-config-save-behavior"): save_behavior,
        }

    return FakeHandle(4, replies)


def _vote(node, candidate, epoch, at=1.0):
    return {"instance_id": node, "candidate": ID[candidate], "epoch": epoch, "logged_at": at}


def _ack(voter, acks, quorum=2, at=9.6):
    return {"instance_id": "valkey-1", "voter": ID[voter], "epoch": 6, "acks": acks, "quorum": quorum, "logged_at": at}


_MAJORITY = [_ack("valkey-2", 1), _ack("valkey-0", 2)]


def _safety(save_behavior="sync", acks=_MAJORITY, disk_vote=None, evidence=(), extra=(), views=None, **config):
    """The oracle's result for an election where valkey-0 and valkey-2 vote for valkey-1 at epoch 6, the votes
    ``acks`` were counted, and valkey-1 won; with ``disk_vote``, valkey-0 was killed with that last vote saved."""

    context = fake_context(_safety_handle(save_behavior, views))
    publish = context.event_bus.publish
    publish("cluster.vote.granted", source="log", payload=_vote("valkey-2", "valkey-1", 6, at=9.0))
    publish("cluster.vote.granted", source="log", payload=_vote("valkey-0", "valkey-1", 6, at=9.5))
    for ack in acks:
        publish("cluster.vote.acked", source="log", payload=ack)
    publish("cluster.election.won", source="log", payload={"instance_id": "valkey-1", "epoch": 6, "logged_at": 9.7})
    for name, payload in extra:
        publish(name, source="log", payload=payload)
    if disk_vote is not None:
        value = {"target": "valkey-0", "applied_at": 10.0, "restart_spawned_at": 11.0, "restart_ready_at": 12.0,
                 "disk_vars_at_kill": {"currentEpoch": 5, "lastVoteEpoch": disk_vote}}  # fmt: skip
        evidence = (*evidence, Evidence(FAULT_INJECTED, "kill", "fault.recovered", 1.0, value))
    spec = resolved(
        ComponentSpec(role=ComponentRole.ORACLE, use="valkey.oracle.cluster-election-safety", id="safety",
                      config={"crash_faults": ["kill"], "converge_timeout_s": 0, **config})
    )  # fmt: skip
    return ElectionSafetyOracle().evaluate(context, spec, evidence)


def test_election_safety_passes_a_majority_win_and_flags_unsaved_votes_outside_sync():
    result = _safety("best-effort", disk_vote=5)
    assert result.status is ComponentStatus.PASS, result.summary
    ids = [item.evidence_id for item in result.evidence]
    assert ids == ["valkey.cluster.election-safety.verified", "valkey.cluster.vote.unsaved-at-crash"]
    record = plain_config(result.evidence[0].value)
    assert record["require_durable_votes"] == "never" and len(record["unsaved_votes_delivered"]) == 1
    voters = sorted([ID["valkey-0"], ID["valkey-2"]])
    assert record["wins"] == [{"winner": "valkey-1", "epoch": 6, "voters": voters, "quorum": 2}]
    assert _safety("sync", disk_vote=6).status is ComponentStatus.PASS
    # An ACK logged after the restarted voter's process started, though before it was ready, may be the new
    # process's vote: it is not one that left the killed process before it was saved.
    late = [_ack("valkey-2", 1), _ack("valkey-0", 2, at=11.5)]
    assert _safety("sync", disk_vote=5, acks=late).status is ComponentStatus.PASS
    # Two primaries of one config epoch claiming overlapping slots, in a view and between two views, are a split.
    primaries = [
        {"cluster_id": cluster_id, "config_epoch": 7, "slots": slots, "flags": ["master"]}
        for cluster_id, slots in (("a", [[0, 100]]), ("b", [[101, 200]]), ("c", [[50, 60]]))
    ]
    views = {
        "valkey-0": {"role": "primary", "slots": [[0, 100]], "primaries": primaries},
        "valkey-2": {"role": "primary", "slots": [[50, 60]], "primaries": primaries[:2]},
    }
    problems = split_primaries(views)
    assert {"view": "valkey-0", "primaries": ["a", "c"]} in problems
    assert {"self_claims": ["valkey-0", "valkey-2"]} in problems
    assert parse_slots(["0-5", "7", "[8->-abc]"]) == [[0, 5], [7, 7]], "a migrating slot is not owned yet"


def _split_trace(tmp_path):
    observations = [
        {"kind": "observation", "round": 3, "t": 1.0, "instance_id": node, "role": "primary", "cluster_id": cluster_id,
         "config_epoch": 6, "slots": slots}
        for node, cluster_id, slots in (("valkey-1", "x", [[0, 10]]), ("valkey-3", "y", [[5, 10]]))
    ]  # fmt: skip
    trace = tmp_path / "trace.jsonl"
    trace.write_text("\n".join(json.dumps(record) for record in [{"kind": "header"}, *observations]))
    return (Evidence(TRACE_EVIDENCE, "trace", "observer.stopped", 1.0, {}, artifacts=(str(trace),)),)


@pytest.mark.parametrize(
    ("case", "status", "violations"),
    [
        # Under sync, a vote the crash lost had left before it was saved.
        ({"disk_vote": 5}, ComponentStatus.FAIL, {"vote_left_before_save"}),
        ({"acks": [_ack("valkey-0", 1)]}, ComponentStatus.FAIL, {"win_without_quorum"}),
        # The count reached quorum only because one primary's vote was counted twice.
        ({"acks": [_ack("valkey-0", 1), _ack("valkey-0", 2)]}, ComponentStatus.FAIL, {"win_without_quorum"}),
        # The quorum is never below a majority of the shards, whatever the log says.
        ({"acks": [_ack("valkey-0", 1, quorum=1)]}, ComponentStatus.FAIL, {"win_without_quorum"}),
        # Wins without any logged vote count cannot pass.
        ({"acks": []}, ComponentStatus.ERROR, None),
        # Every node answered, and valkey-3 sees shard 1 at another config epoch: the views never agreed.
        ({"views": {"valkey-3": _NODES.replace("0 0 4 connected 8192", "0 0 5 connected 8192")}}, ComponentStatus.FAIL,
         "did not converge"),
        # A running node whose view could not be read may hold a split (or be the one that agrees): unjudged.
        ({"views": {"valkey-3": TimeoutError("Timeout reading from socket")}}, ComponentStatus.ERROR,
         "cannot read the cluster views of running node(s) valkey-3"),
        # Two winners and a double vote at epoch 6, and two primaries claiming slots 5-10 at config epoch 6.
        ({"extra": [("cluster.vote.granted", _vote("valkey-2", "valkey-3", 6, at=11.0)),
                    ("cluster.election.won", {"instance_id": "valkey-3", "epoch": 6})],
          "trace": "trace", "require_durable_votes": "never"}, ComponentStatus.FAIL,
         {"vote_once_per_epoch", "one_winner_per_epoch", "same_slots_same_config_epoch", "win_without_quorum"}),
    ],
    ids=["vote-left-before-save", "below-quorum", "voter-counted-twice", "quorum-below-majority", "no-vote-count",
         "views-disagree", "view-unread", "two-winners-and-split-slots"],
)  # fmt: skip
def test_election_safety_does_not_pass_a_win_without_one_durable_majority(tmp_path, case, status, violations):
    evidence = _split_trace(tmp_path) if "trace" in case else ()
    result = _safety(evidence=evidence, **case)
    assert result.status is status, result.summary
    if violations is None:
        assert "Failover auth ACK" in result.summary
    elif isinstance(violations, str):
        assert violations in result.summary
    else:
        assert set(result.diagnostics["violations"]) == violations


# ---------------------------------------------------------------------------
# valkey.action.cluster-election
# ---------------------------------------------------------------------------


class _ElectionShard:
    """Soak s100062: primary valkey-1, and valkey-0, a replica whose sync ends ``sync_s`` from now; like Valkey, it wins
    only if its link was up when the primary stopped (and ``elects``)."""

    def __init__(self, clock, sync_s, elects=True, master_host="127.0.0.1"):
        self.instances = tuple(
            SimpleNamespace(instance_id=f"valkey-{n}", shard_id=0, host="127.0.0.1", port=11120 + n) for n in (0, 1)
        )
        self.clock, self.synced_at = clock, clock.monotonic() + sync_s
        self.elects, self.paused_linked, self.signals = elects, None, []
        # Where the replica's link leads.
        self.master_host = master_host

    def instance(self, node_id):
        return next(item for item in self.instances if item.instance_id == node_id)

    def is_alive(self, node_id):
        return True

    def refresh_roles(self):
        return {}

    def send_signal(self, node_id, signum):
        self.signals.append((node_id, signum))
        if self.paused_linked is None:
            self.paused_linked = self.clock.monotonic() >= self.synced_at

    def new_client(self, node_id, timeout=5.0):
        client = MagicMock()
        client.__enter__.return_value = client
        link = "up" if self.clock.monotonic() >= self.synced_at else "down"
        client.info.return_value = {
            "role": "slave",
            "master_host": self.master_host,
            "master_port": 11121,
            "master_link_status": link,
        }
        return client

    def view(self, handle, node_id, timeout=2.0):
        won = self.elects and bool(self.paused_linked) and node_id == "valkey-0"
        primary = node_id == ("valkey-0" if won else "valkey-1")
        role, slots, epoch = ("primary", [[0, 5460]], 7 if won else 6) if primary else ("replica", [], 6)
        return {"cluster_id": node_id, "current_epoch": 6, "config_epoch": epoch, "role": role, "slots": slots}


@pytest.mark.parametrize(
    ("sync_s", "elects", "master_host", "status", "summary"),
    [
        (0.5, True, "127.0.0.1", "PASS", ""),
        (0.0, False, "127.0.0.1", "FAIL", "no replica of shard 0 won"),
        (60.0, True, "127.0.0.1", "ERROR", "did not link to valkey-1"),
        # A link to the primary's port on another address is not a link to the primary.
        (0.0, True, "127.0.0.9", "ERROR", "did not link to valkey-1"),
    ],
    ids=["waits-for-the-sync", "no-winner-fails", "never-synced-is-no-verdict", "linked-to-another-host"],
)  # fmt: skip
def test_a_paused_primary_election_starts_only_once_a_replica_is_linked(
    monkeypatch, sync_s, elects, master_host, status, summary
):
    clock = VirtualClock().install(monkeypatch, cluster, common)
    shard = _ElectionShard(clock, sync_s, elects, master_host)
    config = {
        "shard": 0,
        "trigger": "pause_primary",
        "pause_s": 1.0,
        "timeout_s": 2,
        "settle_s": 0.1,
        "wait_synced": False,
    }
    spec = resolved(
        ComponentSpec(role=ComponentRole.ACTION, use="valkey.action.cluster-election", id="election", config=config)
    )
    with patch.object(cluster, "_view", shard.view):
        result = ElectionAction().run(clock.drive(fake_context(shard, spec)), spec)
    assert result.status.value == status and summary in result.summary, result.summary
    assert shard.signals == [] if status == "ERROR" else shard.paused_linked is True
    # It waited for the sync, or gave up at its timeout.
    assert clock.elapsed >= (config["timeout_s"] if status == "ERROR" else sync_s)


# ---------------------------------------------------------------------------
# formal/cluster-election through `valkey-fuzzer spec conform`
# ---------------------------------------------------------------------------


def _trace(tmp_path, save_behavior="sync", disk_vote=0, second_winner=False):
    """Two replicas compete for epoch 9; valkey-3 votes for valkey-1 and is killed."""

    nodes = {name: {"cluster_id": ID[name], "config": {"cluster-config-save-behavior": save_behavior}} for name in ID}

    def event(t, name, **payload):
        return {"kind": "event", "t": t, "sequence": int(t * 10), "name": name, "payload": payload}

    def observation(node, role, slots):
        return {"kind": "observation", "t": 0.1, "round": 1, "node": node, "role": role, "slots": slots,
                "current_epoch": 8, "disk": {"currentEpoch": 8, "lastVoteEpoch": 0}}  # fmt: skip

    def granted(t, voter, candidate):
        return event(t, "cluster.vote.granted", instance_id=voter, candidate=ID[candidate], epoch=9)

    def acked(t, voter, candidate):
        return event(t, "cluster.vote.acked", instance_id=candidate, voter=ID[voter], epoch=9)

    killed = {"currentEpoch": 8, "lastVoteEpoch": disk_vote}
    records = [
        {"kind": "header", "scenario_id": "unit", "seed": 1, "nodes": nodes},
        observation("valkey-0", "primary", [[0, 8191]]),
        observation("valkey-3", "primary", [[8192, 16383]]),
        observation("valkey-1", "replica", []),
        event(1.0, "cluster.election.started", instance_id="valkey-1", epoch=9),
        event(1.1, "cluster.election.started", instance_id="valkey-2", epoch=9),
        granted(1.2, "valkey-0", "valkey-1"),
        granted(1.3, "valkey-3", "valkey-1"),
        acked(1.4, "valkey-0", "valkey-1"),
        event(1.5, "fault.injected", applied=True, signal="SIGKILL", target="valkey-3", disk_vars_at_kill=killed),
        acked(1.6, "valkey-3", "valkey-1"),
        event(1.7, "cluster.election.won", instance_id="valkey-1", epoch=9),
        event(2.0, "node.recovered", instance_id="valkey-3"),
    ]
    if second_winner:
        records += [
            granted(3.0, "valkey-3", "valkey-2"),
            acked(3.1, "valkey-3", "valkey-2"),
            event(3.2, "cluster.election.won", instance_id="valkey-2", epoch=9),
        ]
    path = tmp_path / "trace.jsonl"
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n")
    bundle = load_bundle("cluster-election")
    (run,) = collect_runs([str(path)])
    return bundle, run


def _tla_jar():
    path = os.environ.get("TLA2TOOLS_JAR")
    return path if path and os.path.exists(path) else None


@pytest.mark.slow
@pytest.mark.skipif(not _tla_jar() or not shutil.which("java"), reason="needs java and TLA2TOOLS_JAR")
def test_conform_checks_the_election_claims_with_tlc(tmp_path):
    from valkey_fuzzer.spec import TlaTools
    from valkey_fuzzer.spec.conform import check_run

    tools = TlaTools.locate(_tla_jar(), workers="1")

    def conform(**kwargs):
        folder = tmp_path / str(len(list(tmp_path.iterdir())))
        folder.mkdir()
        return check_run(*_trace(folder, **kwargs), tools)

    clean = conform(save_behavior="best-effort")
    assert clean["status"] == "pass", clean
    assert clean["targets"]["VoteLostInCrash"]["reached"], "the vote left before the crash lost it"
    # Under best-effort the model allows a lost vote: outside the claim, not a failure.
    lost = clean["properties"]["NoVoteLostInCrash"]
    assert (lost["status"], lost["claimed"]) == ("violated", False)
    assert lost["outside_claim"] == {"PersistBeforeAck": "FALSE", "FlushBeforeSave": "FALSE"}
    # Known-bad: under sync a delivered vote the crash lost is a claim violation.
    lost = conform(save_behavior="sync", disk_vote=0)
    assert lost["status"] == "fail" and lost["properties"]["NoVoteLostInCrash"]["claimed"]
    assert lost["properties"]["NoVoteLostInCrash"]["first"]["why"] == "ReceiveVote"
    saved = conform(save_behavior="sync", disk_vote=9)
    assert saved["status"] == "pass" and not saved["targets"]["VoteLostInCrash"]["reached"]
    # Known-bad: two winners and a double vote in one epoch under sync.
    double = conform(save_behavior="sync", disk_vote=9, second_winner=True)
    assert double["status"] == "fail"
    violated = {name for name, entry in double["properties"].items() if entry["status"] == "violated"}
    assert violated == {"OneWinnerPerEpoch", "VoteOncePerEpoch"}
