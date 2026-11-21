"""``valkey.observer.trace``: what a round samples from each node is what the bundle's mapping asks for, read in one
transaction, and the mapping's ``sample:`` section is checked when the bundle loads."""

import json
import shutil
from pathlib import Path

import pytest

from tests.deployment_support import cluster_nodes_line, cluster_nodes_text
from tests.fakes import FakeClient, FakeHandle, fake_instances
from tests.support import FORMAL, wait_until
from tests.valkey_experiments_support import fake_context, registration_for, rejects_config, resolved, unit_plan
from valkey_fuzzer.components.trace import TraceObserver
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.results import ComponentStatus
from valkey_fuzzer.spec.bundle import SpecBundleError, load_bundle

REPLICATION = FORMAL / "replication"
INFO = "# Replication\r\nrole:master\r\nmaster_replid:{}\r\nmaster_replid2:{}\r\nmaster_repl_offset:1234567890123456789012345678901234567890\r\n"


class _Pipeline:
    """A MULTI/EXEC pipeline of the fake server: the commands queued, answered together at execute."""

    def __init__(self, client):
        self.client, self.queued = client, []

    def execute_command(self, *args):
        self.queued.append(args)

    def execute(self):
        self.client.transactions.append(list(self.queued))
        return [self.client.execute_command(*args) for args in self.queued]


class _Client(FakeClient):
    def __init__(self, commands, replies):
        super().__init__(commands, replies)
        self.transactions = []
        self.callbacks = {}

    def set_response_callback(self, command, callback):
        self.callbacks[command] = callback

    def pipeline(self, transaction=True):
        assert transaction
        return _Pipeline(self)

    def config_get(self, *names):
        self.commands.append(("CONFIG", "GET", *names))
        return {name: self.replies.get(("CONFIG", name), "no") for name in names}


class _Handle(FakeHandle):
    def __init__(self, replies, count=2):
        super().__init__(fake_instances(count, shard_id=0, port=lambda n: 7000 + n), root="/nonexistent")
        self.replies, self.clients = replies, {}

    def new_client(self, node_id, timeout=5.0):
        client = _Client([], self.replies(node_id))
        self.clients.setdefault(node_id, []).append(client)
        return client


def _observer(handle, tmp_path, **config):
    spec = resolved(
        ComponentSpec(role=ComponentRole.OBSERVER, use="valkey.observer.trace", id="trace",
                      config={"interval_s": 0, "spec": "replication", **config})
    )  # fmt: skip
    context = fake_context(handle, artifact_dir=str(tmp_path / "artifacts"))
    return TraceObserver(), context, spec


def _records(result):
    path = result.evidence[0].value["path"]
    return [json.loads(line) for line in Path(path).read_text().splitlines()]


def test_a_round_samples_what_the_mapping_asks_for_in_one_transaction(tmp_path):
    # The replication bundle asks for INFO replication and DEBUG DIGEST; a replication ID of digits stays text.
    def replies(node_id):
        digest = {"valkey-0": "d0", "valkey-1": "d1"}[node_id]
        return {
            ("INFO", "replication"): INFO.format("1" * 40, "0" * 40),
            ("DEBUG", "DIGEST"): digest,
            ("CONFIG", "dual-channel-replication-enabled"): "no",
        }

    handle = _Handle(replies)
    observer, context, spec = _observer(handle, tmp_path, config_keys=["dual-channel-replication-enabled"])
    assert observer.start(context, spec).status is ComponentStatus.PASS
    context.event_bus.publish("writes.seeded", source="writes", payload={})
    context.event_bus.publish("replication.promoted", source="log", payload={"instance_id": "valkey-1"})
    wait_until(lambda: observer._rounds >= 2)
    result = observer.stop(context, spec)

    assert result.status is ComponentStatus.PASS, result.summary
    header, *records = _records(result)
    # The header carries the files' format version, so spec conform can refuse another fuzzer's trace.
    assert (header["kind"], header["format_version"]) == ("header", 1)
    assert header["samples"] == {
        "info": ["replication"], "digest": True, "digest_keys": [], "cluster": False, "slot_migrations": False,
        "nodes_conf": False, "on": ["replication.*", "*.injected", "*.instance-killed"],
    }  # fmt: skip
    assert header["nodes"]["valkey-0"]["config"] == {"dual-channel-replication-enabled": "no"}
    assert header["spec"]["bundle"] == "replication"
    observations = [record for record in records if record["kind"] == "observation"]
    first = next(record for record in observations if record["node"] == "valkey-1" and record["round"] == 1)
    assert first["digest"] == "d1" and "disk" not in first and "current_epoch" not in first
    assert first["info"]["master_replid"] == "1" * 40 and first["info"]["master_replid2"] == "0" * 40
    assert first["info"]["master_repl_offset"] == "1234567890123456789012345678901234567890"
    # INFO and DEBUG DIGEST went in one MULTI/EXEC, and INFO's reply was kept as text.
    (client,) = [item for item in handle.clients["valkey-1"] if item.transactions]
    assert client.transactions[0] == [("INFO", "replication"), ("DEBUG", "DIGEST")]
    assert "INFO" in client.callbacks
    # The replication event woke a round; the workload's did not (the mapping's `on` globs).
    reasons = sorted({record["reason"] for record in observations})
    assert reasons == ["event", "final", "start"], reasons
    assert sum(record["reason"] == "event" and record["node"] == "valkey-0" for record in observations) == 1


def test_a_sample_that_fails_at_the_start_is_an_error_and_a_later_one_is_recorded(tmp_path):
    def refused(node_id):
        return {("INFO", "replication"): INFO.format("a" * 40, "0" * 40), ("DEBUG", "DIGEST"): Exception("ERR DEBUG")}

    observer, context, spec = _observer(_Handle(refused), tmp_path)
    result = observer.start(context, spec)
    assert result.status is ComponentStatus.ERROR and "DEBUG" in result.summary, result.summary

    # A node that stops answering later is an error record of its own, and the trace goes on.
    handle = _Handle(
        lambda node_id: {("INFO", "replication"): INFO.format("a" * 40, "0" * 40), ("DEBUG", "DIGEST"): "d"}
    )
    observer, context, spec = _observer(handle, tmp_path)
    assert observer.start(context, spec).status is ComponentStatus.PASS
    (sampler,) = [client for client in handle.clients["valkey-1"] if client.transactions]
    sampler.replies[("INFO", "replication")] = ConnectionError("Connection refused")
    result = observer.stop(context, spec)
    final = [record for record in _records(result)[1:] if record.get("reason") == "final"]
    assert "error" in next(record for record in final if record["node"] == "valkey-1")
    assert "digest" in next(record for record in final if record["node"] == "valkey-0")


def test_the_cluster_parts_group_the_slot_migration_jobs_by_kind(tmp_path):
    export = ["name", "e" * 40, "operation", "EXPORT", "slot_ranges", "1-2", "target_node", "t" * 40, "source_node",
              "s" * 40, "state", "replicating"]  # fmt: skip
    tracking = ["name", "r" * 40, "operation", "IMPORT", "slot_ranges", "1-2", "state", "occurring-on-primary"]
    me = "0" * 40

    def replies(node_id):
        return {
            ("CLUSTER", "INFO"): "cluster_state:ok\r\ncluster_current_epoch:3\r\n",
            ("CLUSTER", "NODES"): cluster_nodes_text(
                cluster_nodes_line(me, 7000, "master", epoch=3, slots="0-16383", myself=True)
            ),
            ("CLUSTER", "GETSLOTMIGRATIONS"): [export, tracking],
        }

    handle = _Handle(replies, count=1)
    spec = resolved(
        ComponentSpec(role=ComponentRole.OBSERVER, use="valkey.observer.trace", id="trace",
                      config={"interval_s": 0, "spec": "slot-migration"})
    )  # fmt: skip
    context = fake_context(handle, artifact_dir=str(tmp_path / "artifacts"), deployment_mode="cluster")
    observer = TraceObserver()
    assert observer.start(context, spec).status is ComponentStatus.PASS
    result = observer.stop(context, spec)
    header, *records = _records(result)
    assert header["nodes"]["valkey-0"]["node_id"] == me
    assert header["nodes"]["valkey-0"]["config"] == {"cluster-config-save-behavior": "no", "cluster-node-timeout": "no"}
    sample = next(record for record in records if record["kind"] == "observation")
    assert (sample["current_epoch"], sample["role"]) == (3, "primary")
    assert [job["state"] for job in sample["slot_migrations"]["export"]] == ["replicating"]
    assert sample["slot_migrations"]["import"] == []
    assert [job["state"] for job in sample["slot_migrations"]["tracking"]] == ["occurring-on-primary"]
    # The slot-migration bundle asks for no nodes.conf.
    assert "disk" not in sample
    # The header reads every config key in one CONFIG GET; the view goes in the sample's one MULTI/EXEC.
    header_commands = [c for client in handle.clients["valkey-0"] if not client.transactions for c in client.commands]
    assert header_commands == [
        ("CLUSTER", "INFO"),
        ("CLUSTER", "NODES"),
        ("CONFIG", "GET", "cluster-config-save-behavior", "cluster-node-timeout"),
    ]
    (sampler,) = [client for client in handle.clients["valkey-0"] if client.transactions]
    assert sampler.transactions[0] == [("CLUSTER", "INFO"), ("CLUSTER", "NODES"), ("CLUSTER", "GETSLOTMIGRATIONS")]
    assert sampler.commands == [command for transaction in sampler.transactions for command in transaction]


def test_a_bundle_that_samples_debug_digest_needs_the_debug_command_enabled():
    spec = ComponentSpec(
        role=ComponentRole.OBSERVER, use="valkey.observer.trace", id="trace", config={"spec": "replication"}
    )
    rejects_config(spec.use, spec.config, "enable-debug-command")
    plan = unit_plan((spec,), document={"mode": "standalone", "server_config": {"enable-debug-command": "yes"}})
    assert registration_for("valkey.observer.trace").resolve_config(spec, plan)["spec"] == "replication"


def _edit(path, old, new):
    text = path.read_text()
    assert old in text, (path, old)
    path.write_text(text.replace(old, new))


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        ("sample:\n", "", "no sample section"),
        ("  digest: true\n", "", "does not record .sample.digest"),
        ("      digest: str\n", "      digest: {type: str, path: data.digest}\n", "not a field of a sample record"),
        ("  info: [replication]\n  digest: true\n", "", "must say what the trace observer records"),
    ],
)
def test_a_sample_evidence_reads_only_what_the_sample_section_records(tmp_path, old, new, message):
    bundle = tmp_path / "replication"
    shutil.copytree(REPLICATION, bundle)
    mapping = bundle / "mapping.yaml"
    if old == "sample:\n":
        # Without the section at all.
        text = mapping.read_text()
        mapping.write_text(text[: text.index("sample:\n")] + text[text.index("evidence:\n") :])
    else:
        _edit(mapping, old, new)
    with pytest.raises(SpecBundleError, match=message):
        load_bundle(str(bundle))


def test_a_fields_map_renames_captured_values_and_leaves_the_rest(tmp_path):
    bundle = load_bundle(str(REPLICATION))
    field = bundle.mapping.evidence["sample-replica"].fields["second_offset"]
    assert field.map == {-1: 0}
