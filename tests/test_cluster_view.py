"""The cluster view the harness decides on: what a majority of the answering nodes agree on (S-09)."""

import random
from unittest.mock import patch

import pytest

from tests.deployment_support import instance
from valkey_fuzzer.deployment.local.cluster_nodes import parse_cluster_nodes
from valkey_fuzzer.deployment.local.failover import failover_shard
from valkey_fuzzer.deployment.local.models import LocalDeploymentError
from valkey_fuzzer.deployment.local.topology import DeploymentConnection
from valkey_fuzzer.deployment.local.validation import snapshot

LOCAL = "valkey_fuzzer.deployment.local."
PORTS = (7000, 7001, 7002)


def view(epochs=(1, 2, 3), primaries=(0, 1, 2)):
    """A CLUSTER NODES of three shards of one primary each; ``primaries`` are the ones it knows as primary."""
    ranges = ("0-5460", "5461-10922", "10923-16383")
    lines = []
    for shard, port in enumerate(PORTS):
        role = "master" if shard in primaries else "slave"
        lines.append(f"n{shard} 127.0.0.1:{port}@{port + 10000} {role} - 0 0 {epochs[shard]} connected {ranges[shard]}")
    return parse_cluster_nodes("\n".join(lines))


def connection():
    infos = [instance(f"valkey-{i}", port=port, slot_start=s, slot_end=e, shard_id=i)
             for i, (port, (s, e)) in enumerate(zip(PORTS, ((0, 5460), (5461, 10922), (10923, 16383))))]  # fmt: skip
    return DeploymentConnection(infos, "d")


def read(views, **kwargs):
    """``get_current_nodes`` as the nodes at PORTS answer ``views`` (None: no answer), with the evidence read."""
    asked = []

    def answer(node, timeout=2.0):
        asked.append(node["port"])
        return views[PORTS.index(node["port"])]

    conn = connection()
    with patch(f"{LOCAL}topology.query_cluster_nodes", side_effect=answer):
        try:
            rows = conn.get_current_nodes(**kwargs)
        except LocalDeploymentError as exc:
            return exc, conn.drain_view_reads(), asked
    return rows, conn.drain_view_reads(), asked


def test_the_view_a_majority_agrees_on_wins_over_a_stale_first_node():
    stale = view(epochs=(1, 2, 3), primaries=(1, 2))  # node 0 still has the primary role of shard 0 as a replica
    fresh = view(epochs=(5, 2, 3))
    rows, evidence, _ = read([stale, fresh, fresh])

    assert {row.node_id: row.role for row in rows}["n0"] == "primary"
    assert evidence["last"]["answered"] == ["127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002"]
    assert evidence["last"]["agreed"] == ["127.0.0.1:7001", "127.0.0.1:7002"]
    assert evidence["last"]["view"] == {"n0": "5", "n1": "2", "n2": "3"}
    assert evidence["disputed"] == [evidence["last"]]


def test_a_first_node_with_a_stale_epoch_does_not_decide_the_view():
    rows, evidence, _ = read([view(epochs=(1, 2, 3)), view(epochs=(7, 2, 3)), view(epochs=(7, 2, 3))])
    assert evidence["last"]["view"]["n0"] == "7"
    assert len(rows) == 3


def test_a_three_way_split_has_no_majority_and_is_an_error_not_a_pick():
    views = [view(epochs=(1, 2, 3)), view(epochs=(2, 2, 3)), view(epochs=(3, 2, 3))]
    error, evidence, _ = read(views)

    assert isinstance(error, LocalDeploymentError)
    assert "no majority view" in str(error)
    assert evidence["last"]["agreed"] == [] and evidence["last"]["view"] is None
    assert len(evidence["last"]["answered"]) == 3


def test_two_answering_nodes_that_disagree_have_no_majority():
    error, _, _ = read([view(epochs=(1, 2, 3)), view(epochs=(2, 2, 3)), None])
    assert isinstance(error, LocalDeploymentError)


def test_two_answering_nodes_that_agree_are_a_majority_when_the_third_is_silent():
    rows, evidence, _ = read([None, view(), view()])
    assert len(rows) == 3
    assert evidence["last"]["answered"] == ["127.0.0.1:7001", "127.0.0.1:7002"]


def test_one_answering_node_is_its_own_majority_and_none_is_no_rows():
    assert len(read([None, None, view()])[0]) == 3
    rows, evidence, _ = read([None, None, None])
    assert rows == [] and evidence["last"]["answered"] == []


def test_replicas_that_answer_alone_still_give_a_view():
    # The primary of shard 1 is gone; the two nodes left agree it is a primary with epoch 2.
    rows, _, _ = read([view(), None, view()])
    assert [row.node_id for row in rows] == ["n0", "n1", "n2"]


def test_a_paused_node_is_not_asked():
    stale = view(epochs=(1, 9, 9))
    rows, evidence, asked = read([view(), stale, view()], skip_ports={7001})
    assert asked == [7000, 7002]
    assert evidence["last"]["skipped"] == ["127.0.0.1:7001"]
    assert rows[1].node_id == "n1"


def test_skipping_the_only_other_nodes_leaves_the_one_that_is_left_as_the_view():
    rows, evidence, asked = read([view(), view(), view()], skip_ports={7000, 7001})
    assert (
        asked == [7002]
        and len(rows) == 3
        and evidence["last"]["skipped"]
        == [
            "127.0.0.1:7000",
            "127.0.0.1:7001",
        ]
    )


def test_the_evidence_counts_every_read_and_is_forgotten_once_drained():
    conn = connection()
    with patch(f"{LOCAL}topology.query_cluster_nodes", return_value=view()):
        conn.get_current_nodes()
        conn.get_current_nodes()
    evidence = conn.drain_view_reads()
    assert evidence["reads"] == 2 and evidence["disputed"] == []
    assert conn.drain_view_reads() == {"reads": 0, "last": None, "disputed": []}


def test_a_snapshot_of_a_split_cluster_records_the_split_as_unread():
    conn = connection()
    views = {7000: view(epochs=(1, 2, 3)), 7001: view(epochs=(2, 2, 3)), 7002: view(epochs=(3, 2, 3))}
    with patch(f"{LOCAL}topology.query_cluster_nodes", side_effect=lambda node, timeout=2.0: views[node["port"]]):
        taken = snapshot.take_snapshot(conn, 1.0)
    assert taken.rows == []
    assert all("no majority view" in why for why in taken.unanswered.values()) and len(taken.unanswered) == 3


def test_a_failover_does_not_pick_a_target_from_a_split_view():
    conn = connection()
    views = {7000: view(epochs=(1, 2, 3)), 7001: view(epochs=(2, 2, 3)), 7002: view(epochs=(3, 2, 3))}
    with (
        patch(f"{LOCAL}topology.query_cluster_nodes", side_effect=lambda node, timeout=2.0: views[node["port"]]),
        pytest.raises(LocalDeploymentError, match="no majority view"),
    ):
        failover_shard(conn, 0, timeout=1.0, rng=random.Random(0))


def test_a_failover_does_not_ask_a_paused_instance():
    conn = connection()
    asked = []

    def answer(node, timeout=2.0):
        asked.append(node["port"])
        return view()

    with (
        patch(f"{LOCAL}topology.query_cluster_nodes", side_effect=answer),
        pytest.raises(LocalDeploymentError, match="has no replica to promote"),
    ):
        failover_shard(conn, 0, timeout=1.0, paused_instances=["valkey-0"], rng=random.Random(0))
    assert 7000 not in asked
