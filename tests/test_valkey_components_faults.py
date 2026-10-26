"""Unit tests for addressing, partitions, rolling upgrades, digests, and scoped loss windows."""

import os
import time
from types import SimpleNamespace

import pytest

from valkey_fuzzer.extensibility import (
    ComponentKind,
    ComponentSpec,
    ComponentStatus,
    Evidence,
    LifecycleEventBus,
    RunPlan,
    TopologyProviderIdentity,
)
from valkey_fuzzer_packs import component_registrations, network
from valkey_fuzzer_packs.common import ComponentConfigError, keyspace_digest
from valkey_fuzzer_packs.faults import FAULT_APPLIED
from valkey_fuzzer_packs.oracles import LedgerModel, fault_records, fault_windows

PROVIDER = TopologyProviderIdentity("valkey-fuzzer.local-topology", "1", "v1")


def _registration(component_id):
    return next(item for item in component_registrations() if item.component_id == component_id)


def _plan(components=(), topology="cluster", document=None):
    oracle = ComponentSpec(kind=ComponentKind.ORACLE, component_id="valkey.evidence.assert", instance_id="assert")
    return RunPlan(
        scenario_id="unit",
        pack_id="tests.pack",
        pack_version="1",
        seed=1,
        topology=topology,
        components=(*components, oracle),
        topology_provider=PROVIDER,
        mode="topology_smoke",
        topology_document=document or {"type": topology},
    )


# ---------------------------------------------------------------------------
# partition fault
# ---------------------------------------------------------------------------


def test_partition_requires_per_node_addressing_and_a_trigger():
    spec = ComponentSpec(
        kind=ComponentKind.FAULT,
        component_id="valkey.net.partition",
        instance_id="p",
        config={"when_event": "go", "isolate": "node-0"},
    )
    validator = _registration("valkey.net.partition").validator
    with pytest.raises(ComponentConfigError, match="per_node_loopback"):
        validator(spec, _plan((spec,)))
    validator(spec, _plan((spec,), document={"type": "cluster", "addressing": "per_node_loopback"}))
    both = ComponentSpec(
        kind=ComponentKind.FAULT,
        component_id="valkey.net.partition",
        instance_id="p",
        config={"when_event": "go", "isolate": "node-0", "heal_event": "x", "duration_s": 1},
    )
    with pytest.raises(ComponentConfigError, match="not both"):
        validator(both, _plan((both,), document={"type": "cluster", "addressing": "per_node_loopback"}))


class _Handle:
    def __init__(self, count=3, shared=False):
        self.nodes = tuple(
            SimpleNamespace(node_id=f"node-{index}", host="127.0.0.1" if shared else f"127.0.0.{index + 2}")
            for index in range(count)
        )

    def primaries(self):
        return ["node-0"]

    def replicas(self):
        return [node.node_id for node in self.nodes[1:]]


class _Context:
    def __init__(self, handle):
        self.services = {"topology": {"valkey.topology": handle}}
        self.event_bus = LifecycleEventBus()


def test_partition_installs_symmetric_rules_in_a_private_chain_and_heals(monkeypatch):
    calls = []

    def fake_iptables(*args):
        calls.append(args)
        # "-C INPUT -j chain" succeeds once (the jump exists), then fails.
        if args[0] == "-C":
            present = sum(1 for call in calls if call[0] == "-I") > sum(1 for call in calls if call[0] == "-D")
            return SimpleNamespace(returncode=0 if present else 1, stderr="", stdout="")
        if args[0] == "-S":
            return SimpleNamespace(returncode=1, stderr="No chain", stdout="")
        return SimpleNamespace(returncode=0, stderr="", stdout="")

    monkeypatch.setattr(network, "_iptables", fake_iptables)
    context = _Context(_Handle())
    spec = ComponentSpec(
        kind=ComponentKind.FAULT,
        component_id="valkey.net.partition",
        instance_id="split",
        config={"when_event": "go", "isolate": "node-0", "heal_event": "heal"},
    )
    fault = network.PartitionFault()
    assert fault.arm(context, spec).status is ComponentStatus.PASS
    context.event_bus.publish("go", source="test")
    drops = [call for call in calls if call[0] == "-A"]
    assert len(drops) == 4, "two peers, both directions"
    assert ("-A", fault._chain, "-s", "127.0.0.2", "-d", "127.0.0.3", "-j", "DROP") in drops
    assert ("-A", fault._chain, "-s", "127.0.0.3", "-d", "127.0.0.2", "-j", "DROP") in drops
    assert ("-I", "INPUT", "1", "-j", fault._chain) in calls
    context.event_bus.publish("heal", source="test")
    assert ("-X", fault._chain) in calls
    result = fault.cleanup(context, spec)
    assert result.status is ComponentStatus.PASS
    (evidence,) = result.evidence
    assert evidence.value["targets"] == ("node-0",)
    assert evidence.value["heal_reason"] == "event"
    assert [event.name for event in context.event_bus.committed_history() if event.name.startswith("net.")] == [
        "net.partitioned",
        "net.healed",
    ]


def test_partition_refuses_shared_addresses(monkeypatch):
    monkeypatch.setattr(network, "_iptables", lambda *args: SimpleNamespace(returncode=0, stderr="", stdout=""))
    spec = ComponentSpec(
        kind=ComponentKind.FAULT,
        component_id="valkey.net.partition",
        instance_id="split",
        config={"when_event": "go", "isolate": "node-0"},
    )
    with pytest.raises(ValueError, match="share an address"):
        network.PartitionFault().arm(_Context(_Handle(shared=True)), spec)


def test_host_probe_reports_partition_prerequisites(monkeypatch):
    from valkey_fuzzer_packs.storage import HostCapabilityProbe

    monkeypatch.setattr(network, "iptables_supported", lambda: (False, "iptables is not installed"))
    spec = ComponentSpec(
        kind=ComponentKind.CAPABILITY,
        component_id="valkey.host.capability",
        instance_id="host",
        config={"requires": ["iptables"]},
    )
    result = HostCapabilityProbe().probe(None, spec)
    assert result.status is ComponentStatus.SKIP
    assert "iptables is not installed" in result.summary


# ---------------------------------------------------------------------------
# scoped loss windows
# ---------------------------------------------------------------------------


def _op(op_id, op, key, value=None, reply=None, t_end=None, outcome="ok"):
    entry = {"id": op_id, "op": op, "key": key, "outcome": outcome, "t_start": t_end or op_id, "t_end": t_end or op_id}
    if value is not None:
        entry["value"] = value
    if op == "get":
        entry["reply"] = reply
    return entry


def test_fault_records_extend_windows_to_heal_time_and_carry_targets():
    evidence = [
        Evidence(FAULT_APPLIED, "split", "p", 1.0, {"applied_at": 100.0, "healed_at": 105.0, "targets": ["node-0"]}),
        Evidence(FAULT_APPLIED, "kill", "p", 1.0, {"applied_at": 50.0, "target": "node-3"}),
    ]
    assert fault_records(evidence, 1.0, None, 2.0) == [(99.0, 107.0, ["node-0"]), (49.0, 52.0, ["node-3"])]
    assert fault_windows(evidence, 0.0, ["split"]) == [(100.0, 105.0)]


def test_loss_window_applies_only_to_scoped_keys_and_skips_reads_inside_it():
    ledger = [
        _op(1, "set", "a", "a1", t_end=10),
        _op(2, "set", "a", "a2", t_end=101),
        _op(3, "set", "b", "b1", t_end=10),
        _op(4, "set", "b", "b2", t_end=101),
        _op(5, "get", "a", reply="a1", t_end=102),
    ]
    model = LedgerModel({}, ledger, [(100.0, 110.0)], lossy_key=lambda key: key == "a", window_reads="written")
    assert model.possible["a"] == {"a1", "a2"}
    assert model.possible["b"] == {"b2"}, "writes outside the scope must survive"
    assert model.unchecked_reads == 1 and not model.read_violations
    assert model.loss_permitted_ops == 1


# ---------------------------------------------------------------------------
# rolling upgrade validation, digests
# ---------------------------------------------------------------------------


def test_rolling_upgrade_needs_a_target_binary_and_replicas():
    spec = ComponentSpec(
        kind=ComponentKind.ACTION,
        component_id="valkey.rolling_upgrade",
        instance_id="upgrade",
        config={"to_binary": "/opt/new/valkey-server"},
    )
    validator = _registration("valkey.rolling_upgrade").validator
    validator(spec, _plan((spec,)))
    with pytest.raises(ComponentConfigError, match="missing required config field 'to_binary'"):
        missing = ComponentSpec(kind=ComponentKind.ACTION, component_id="valkey.rolling_upgrade", instance_id="u")
        validator(missing, _plan((missing,)))
    with pytest.raises(ComponentConfigError, match="supports topologies"):
        validator(spec, _plan((spec,), topology="standalone"))


def test_rolling_upgrade_target_may_be_an_unbound_artifact_reference():
    from valkey_fuzzer.environment import parse_environment_reference

    spec = ComponentSpec(
        kind=ComponentKind.ACTION,
        component_id="valkey.rolling_upgrade",
        instance_id="upgrade",
        config={"to_binary": parse_environment_reference("artifacts.candidate.binary")},
    )
    _registration("valkey.rolling_upgrade").validator(spec, _plan((spec,)))
    with pytest.raises(ComponentConfigError, match="cannot be an environment reference"):
        bad = ComponentSpec(
            kind=ComponentKind.ACTION,
            component_id="valkey.rolling_upgrade",
            instance_id="upgrade",
            config={"to_binary": "/x", "settle_s": parse_environment_reference("artifacts.candidate.binary")},
        )
        _registration("valkey.rolling_upgrade").validator(bad, _plan((bad,)))


def test_keyspace_digest_is_order_independent_and_type_aware():
    first = {"a": ["string", "1"], "b": ["set", ["x", "y"]]}
    second = {"b": ["set", ["x", "y"]], "a": ["string", "1"]}
    assert keyspace_digest(first) == keyspace_digest(second)
    assert keyspace_digest(first) != keyspace_digest({"a": ["list", ["1"]], "b": ["set", ["x", "y"]]})


def test_wait_event_action_waits_for_committed_events():
    from valkey_fuzzer_packs.actions import WaitEventAction

    context = SimpleNamespace(event_bus=LifecycleEventBus(), deadline=None)
    spec = ComponentSpec(
        kind=ComponentKind.ACTION,
        component_id="valkey.flow.wait_event",
        instance_id="wait",
        config={"event": "workload.drained", "timeout_s": 0.05},
    )
    assert WaitEventAction().run(context, spec).status is ComponentStatus.FAIL
    context.event_bus.publish("workload.drained", source="w")
    started = time.monotonic()
    result = WaitEventAction().run(context, spec)
    assert result.status is ComponentStatus.PASS
    assert time.monotonic() - started < 0.05


def test_window_reads_default_to_strict_and_written_mode_still_catches_foreign_values():
    ledger = [
        _op(1, "set", "a", "a1", t_end=10),
        _op(2, "set", "a", "a2", t_end=101),
        _op(3, "get", "a", reply="a1", t_end=102),
        _op(4, "set", "a", "a3", t_end=103),
        _op(5, "get", "a", reply="never-written", t_end=104),
    ]
    strict = LedgerModel({}, ledger, [(100.0, 110.0)])
    assert [item["id"] for item in strict.read_violations] == [5]
    assert strict.unchecked_reads == 0
    written = LedgerModel({}, ledger, [(100.0, 110.0)], window_reads="written")
    assert [item["id"] for item in written.read_violations] == [5]
    assert written.unchecked_reads == 2


def test_fault_does_not_fire_after_cleanup_started():
    from valkey_fuzzer_packs.faults import CommandFault

    class Client:
        def __init__(self):
            self.commands = []

        def execute_command(self, *args):
            self.commands.append(args)
            return "OK"

        def close(self):
            pass

    client = Client()
    handle = SimpleNamespace(primaries=lambda: ["node-0"], replicas=lambda: [], new_client=lambda node, timeout: client)
    handle.nodes = (SimpleNamespace(node_id="node-0"),)
    context = _Context(handle)
    spec = ComponentSpec(
        kind=ComponentKind.FAULT,
        component_id="valkey.command_fault",
        instance_id="late",
        config={"when_event": "go", "command": ["PING"]},
    )
    fault = CommandFault()
    fault.arm(context, spec)
    trigger = fault._on_event
    fault.cleanup(context, spec)
    trigger(SimpleNamespace(name="go", sequence=1))
    assert client.commands == []


def test_chain_names_are_unique_per_instance_and_stale_chains_are_removed(monkeypatch):
    assert network.chain_name("isolate-primary-and-replica-a", 7) != network.chain_name(
        "isolate-primary-and-replica-b", 7
    )
    assert len(network.chain_name("x" * 100, 99999)) <= 28
    calls = []
    dead, alive = network.chain_name("old", 999999), network.chain_name("live", os.getpid())

    def fake(*args):
        calls.append(args)
        if args == ("-S",):
            return SimpleNamespace(
                returncode=0, stdout=f"-P INPUT ACCEPT\n-N {dead}\n-N {alive}\n-N DOCKER\n", stderr=""
            )
        return SimpleNamespace(returncode=1 if args[0] == "-C" else 0, stdout="", stderr="")

    monkeypatch.setattr(network, "_iptables", fake)
    monkeypatch.setattr(network, "_pid_alive", lambda pid: pid != 999999)
    assert network.remove_stale_chains() == [dead]
    assert ("-X", alive) not in calls and ("-X", "DOCKER") not in calls
