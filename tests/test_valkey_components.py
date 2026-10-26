"""Unit tests for the Valkey component library (no Valkey processes)."""

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
from valkey_fuzzer_packs import component_registrations
from valkey_fuzzer_packs.capabilities import plan_binaries
from valkey_fuzzer_packs.common import ComponentConfigError, compare, select_nodes
from valkey_fuzzer_packs.faults import FAULT_APPLIED, CommandFault, ProcessSignalFault
from valkey_fuzzer_packs.observers import InfoObserver, LogObserver
from valkey_fuzzer_packs.oracles import EvidenceAssertOracle, LedgerModel, fault_windows, scan_for_crashes

PROVIDER = TopologyProviderIdentity("valkey-fuzzer.local-topology", "1", "v1")


def _plan(components=(), topology="standalone", document=None):
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


def _registration(component_id):
    for registration in component_registrations():
        if registration.component_id == component_id:
            return registration
    raise KeyError(component_id)


def _validate(component_id, kind, config, *, when_event=None, topology="standalone", extra=()):
    if when_event is not None:
        config = {**config, "when_event": when_event}
    spec = ComponentSpec(kind=kind, component_id=component_id, instance_id="subject", config=config)
    _registration(component_id).validator(spec, _plan((spec, *extra), topology=topology))


class FakeNode(SimpleNamespace):
    pass


class FakeHandle:
    def __init__(self, nodes=("node-0", "node-1"), log_dir=None):
        self.nodes = tuple(
            FakeNode(
                node_id=node_id,
                role="primary" if index == 0 else "replica",
                pid=1000 + index,
                log_file=os.path.join(log_dir, f"{node_id}.log") if log_dir else "",
                artifact=None,
            )
            for index, node_id in enumerate(nodes)
        )
        self.topology_type = SimpleNamespace(value="replication")
        self.signals = []
        self.clients = {}

    def primaries(self):
        return [node.node_id for node in self.nodes if node.role == "primary"]

    def replicas(self):
        return [node.node_id for node in self.nodes if node.role == "replica"]

    def node(self, node_id):
        return next(node for node in self.nodes if node.node_id == node_id)

    def log_file(self, node_id):
        return self.node(node_id).log_file

    def pid(self, node_id):
        return self.node(node_id).pid

    def new_client(self, node_id, timeout=5.0):
        client = self.clients.setdefault(node_id, FakeClient())
        return client


class FakeClient:
    def __init__(self, reply="OK", error=None):
        self.reply = reply
        self.error = error
        self.commands = []

    def execute_command(self, *args):
        self.commands.append(args)
        if self.error is not None:
            raise self.error
        return self.reply

    def close(self):
        pass


class FakeContext:
    def __init__(self, handle, plan=None, artifact_dir=""):
        self.services = {"topology": {"valkey.topology": handle}}
        self.event_bus = LifecycleEventBus()
        self.plan = plan or _plan()
        self.artifact_dir = artifact_dir
        self.deadline = None

    def rng(self, instance_id, stream="default"):
        import random

        return random.Random(1)


# ---------------------------------------------------------------------------
# registry / mandatory components
# ---------------------------------------------------------------------------


def test_components_are_namespaced_and_registered_once():
    ids = [registration.component_id for registration in component_registrations()]
    assert len(ids) == len(set(ids)) == 24
    assert all(component_id.startswith("valkey.") for component_id in ids)


# ---------------------------------------------------------------------------
# validators
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("component_id", "kind", "config", "message"),
    [
        ("valkey.kv_workload", ComponentKind.WORKLOAD, {"rate": "fast"}, "must be float"),
        ("valkey.kv_workload", ComponentKind.WORKLOAD, {"bogus": 1}, "unknown config fields"),
        ("valkey.kv_workload", ComponentKind.WORKLOAD, {"mix": {"zap": 1}}, "unknown operations"),
        ("valkey.kv_workload", ComponentKind.WORKLOAD, {"rate": True}, "boolean"),
        ("valkey.bgsave", ComponentKind.ACTION, {"nodes": "everyone"}, "or a node ID"),
        ("valkey.bgsave", ComponentKind.ACTION, {"expect": "maybe"}, "must be one of"),
        ("valkey.command", ComponentKind.ACTION, {"command": []}, "non-empty"),
        ("valkey.command", ComponentKind.ACTION, {"command": ["PING"], "expect": {}}, "predicate"),
        ("valkey.info", ComponentKind.OBSERVER, {"expectations": [{"field": "x", "equals": 1}]}, "evidence_id"),
        (
            "valkey.info",
            ComponentKind.OBSERVER,
            {"expectations": [{"evidence_id": "a.b", "field": "x", "delta_min": 1, "equals": 2}]},
            "cannot combine",
        ),
        ("valkey.log", ComponentKind.OBSERVER, {"required": [{"pattern": "("}]}, "valid regex"),
        ("valkey.wait_for", ComponentKind.ACTION, {"section": "persistence"}, "needs seconds"),
        ("valkey.ledger", ComponentKind.ORACLE, {"workload": "missing"}, "not a component"),
        ("valkey.host.capability", ComponentKind.CAPABILITY, {"requires": ["moon"]}, "requires must list"),
    ],
)
def test_component_validators_reject_bad_config(component_id, kind, config, message):
    with pytest.raises(ComponentConfigError, match=message):
        _validate(component_id, kind, config)


def test_faults_require_a_trigger_event():
    with pytest.raises(ComponentConfigError, match="missing required config field 'when_event'"):
        _validate("valkey.process_signal", ComponentKind.FAULT, {})
    _validate("valkey.process_signal", ComponentKind.FAULT, {}, when_event="bgsave.started")


def test_failover_is_cluster_only():
    with pytest.raises(ComponentConfigError, match="supports topologies"):
        _validate("valkey.failover", ComponentKind.ACTION, {}, topology="standalone")
    _validate("valkey.failover", ComponentKind.ACTION, {}, topology="cluster")


def test_fill_fault_must_fill_a_node_data_root():
    spec = ComponentSpec(
        kind=ComponentKind.FAULT,
        component_id="valkey.fs.fill",
        instance_id="fill",
        config={"path": "/mnt/disk", "when_event": "x.y"},
    )
    with pytest.raises(ComponentConfigError, match="node_data_roots"):
        _registration("valkey.fs.fill").validator(spec, _plan((spec,)))
    document = {"type": "standalone", "node_data_roots": {"primary": "/mnt/disk"}}
    _registration("valkey.fs.fill").validator(spec, _plan((spec,), document=document))


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "rule", "expected"),
    [
        ("forkless", {"equals": "forkless"}, True),
        ("fork", {"equals": "forkless"}, False),
        (3, {"min": 1}, True),
        ("0", {"min": 1}, False),
        (None, {"exists": False}, True),
        ("ok", {"in": ["ok", "err"]}, True),
        ("x", {"min": 0}, False),
    ],
)
def test_compare_predicates(value, rule, expected):
    assert compare(value, rule) is expected


def test_select_nodes_uses_live_roles():
    handle = FakeHandle(("node-0", "node-1", "node-2"))
    assert select_nodes(handle, "all") == ["node-0", "node-1", "node-2"]
    assert select_nodes(handle, "primaries") == ["node-0"]
    assert select_nodes(handle, "replica") == ["node-1"]
    assert select_nodes(handle, ["node-2"]) == ["node-2"]
    with pytest.raises(ValueError):
        select_nodes(handle, ["node-9"])


def test_plan_binaries_include_node_overrides_and_the_default():
    document = {"type": "cluster", "valkey_binary": "/opt/new", "node_binaries": {"replica": "/opt/old"}}
    assert plan_binaries(_plan(document=document, topology="cluster")) == {
        "default": "/opt/new",
        "node_binaries.replica": "/opt/old",
    }
    covered = {"type": "cluster", "node_binaries": {"primary": "/opt/a", "replica": "/opt/b"}}
    assert plan_binaries(_plan(document=covered, topology="cluster")) == {
        "node_binaries.primary": "/opt/a",
        "node_binaries.replica": "/opt/b",
    }
    assert plan_binaries(_plan(document={"type": "standalone", "valkey_binary": "/x"})) == {"default": "/x"}


# ---------------------------------------------------------------------------
# ledger model
# ---------------------------------------------------------------------------


def _op(op_id, op, key, outcome, value=None, reply=None, t_end=None):
    entry = {"id": op_id, "op": op, "key": key, "outcome": outcome, "t_start": op_id, "t_end": t_end or op_id}
    if value is not None:
        entry["value"] = value
    if reply is not None or op == "get":
        entry["reply"] = reply
    return entry


def test_ledger_model_acknowledged_ambiguous_and_rejected():
    model = LedgerModel(
        {"k": "v0"},
        [
            _op(1, "set", "k", "ok", "v1"),
            _op(2, "set", "k", "ambiguous", "v2"),
            _op(3, "set", "k", "error", "v3"),
            _op(4, "set", "j", "ok", "j1"),
            _op(5, "del", "j", "ok"),
        ],
        [],
    )
    assert model.possible["k"] == {"v1", "v2"}
    assert model.possible["j"] == {None}
    assert "v3" in model.ever["k"] and "v3" not in model.possible["k"]


def test_ledger_model_loss_window_makes_acknowledged_writes_optional():
    ledger = [_op(1, "set", "k", "ok", "v1", t_end=10), _op(2, "set", "k", "ok", "v2", t_end=19)]
    strict = LedgerModel({}, ledger, [])
    assert strict.possible["k"] == {"v2"}
    relaxed = LedgerModel({}, ledger, [(18.0, 20.0)])
    assert relaxed.possible["k"] == {"v1", "v2"}
    assert relaxed.loss_permitted_ops == 1


def test_ledger_model_flags_impossible_reads():
    model = LedgerModel(
        {"k": "v0"},
        [_op(1, "set", "k", "ok", "v1"), _op(2, "get", "k", "ok", reply="v0"), _op(3, "get", "k", "ok", reply="v1")],
        [],
    )
    assert [violation["id"] for violation in model.read_violations] == [2]


def test_fault_windows_use_injection_time():
    evidence = [
        Evidence(FAULT_APPLIED, "kill", "fault.stopped", 1.0, {"applied_at": 100.0}),
        Evidence(FAULT_APPLIED, "other", "fault.stopped", 1.0, {"applied_at": 50.0}),
    ]
    assert fault_windows(evidence, 2.0, ["kill"]) == [(98.0, 100.0)]
    assert len(fault_windows(evidence, 2.0, None)) == 2


# ---------------------------------------------------------------------------
# observers
# ---------------------------------------------------------------------------


def _info_observer(expectations, nodes=("node-0", "node-1")):
    observer = InfoObserver()
    observer._config = {"expectations": expectations}
    observer._nodes = list(nodes)
    return observer


def _sample(index, phase, node, **fields):
    return {"index": index, "phase": phase, "node": node, "at": index, "fields": fields}


def test_info_expectations_distinguish_during_and_final():
    samples = [
        _sample(0, "baseline", "node-0", rdb_current_bgsave_type="none", rdb_saves="0"),
        _sample(1, "event", "node-0", rdb_current_bgsave_type="forkless", rdb_saves="1"),
        _sample(2, "final", "node-0", rdb_current_bgsave_type="none", rdb_saves="1"),
    ]
    observer = _info_observer([], nodes=("node-0",))
    during = observer._evaluate({"field": "rdb_current_bgsave_type", "equals": "forkless", "when": "during"}, samples)
    assert during["matches"][0]["sample_index"] == 1
    assert (
        observer._evaluate({"field": "rdb_current_bgsave_type", "equals": "forkless", "when": "final"}, samples) is None
    )
    delta = observer._evaluate({"field": "rdb_saves", "delta_min": 1}, samples)
    assert delta["deltas"]["node-0"]["delta"] == 1


def test_info_expectation_all_requires_every_node():
    samples = [_sample(0, "final", "node-0", x="1"), _sample(1, "final", "node-1", x="0")]
    observer = _info_observer([])
    assert observer._evaluate({"field": "x", "equals": "1", "when": "final", "nodes": "any"}, samples)
    assert observer._evaluate({"field": "x", "equals": "1", "when": "final", "nodes": "all"}, samples) is None


def test_log_observer_only_reads_the_observation_window(tmp_path):
    handle = FakeHandle(("node-0",), log_dir=str(tmp_path))
    log = tmp_path / "node-0.log"
    log.write_text("forkless-save: forkless save complete\n")
    spec = ComponentSpec(
        kind=ComponentKind.OBSERVER,
        component_id="valkey.log",
        instance_id="logs",
        config={
            "required": [{"evidence_id": "t.complete", "pattern": "save complete"}],
            "forbidden": [{"pattern": "BUG REPORT"}],
        },
    )
    context = FakeContext(handle, artifact_dir=str(tmp_path / "artifacts"))
    observer = LogObserver()
    observer.start(context, spec)
    result = observer.stop(context, spec)
    assert result.status is ComponentStatus.PASS
    assert not [item for item in result.evidence if item.evidence_id == "t.complete"]

    observer = LogObserver()
    observer.start(context, spec)
    with log.open("a") as handle_file:
        handle_file.write("forkless-save: forkless save complete\n=== VALKEY BUG REPORT START\n")
    result = observer.stop(context, spec)
    assert result.status is ComponentStatus.FAIL
    ids = {item.evidence_id for item in result.evidence}
    assert ids == {"t.complete", "valkey.log.forbidden_match"}


def test_crash_scan_detects_bug_reports(tmp_path):
    log = tmp_path / "n.log"
    log.write_text("ok\n=== VALKEY BUG REPORT START: Cut & paste\n")
    from valkey_fuzzer_packs.oracles import CRASH_PATTERNS

    assert list(scan_for_crashes({"node-0": str(log)}, CRASH_PATTERNS)) == ["node-0"]
    # A crash written by an earlier run sharing the log file is out of scope.
    assert scan_for_crashes({"node-0": str(log)}, CRASH_PATTERNS, {"node-0": log.stat().st_size}) == {}
    assert scan_for_crashes({"node-0": str(tmp_path / "missing.log")}, CRASH_PATTERNS) == {}


# ---------------------------------------------------------------------------
# faults
# ---------------------------------------------------------------------------


def test_fault_that_never_triggers_fails():
    handle = FakeHandle()
    context = FakeContext(handle)
    spec = ComponentSpec(
        kind=ComponentKind.FAULT,
        component_id="valkey.command_fault",
        instance_id="cancel",
        config={"when_event": "bgsave.started", "command": ["BGSAVE", "CANCEL"]},
    )
    fault = CommandFault()
    assert fault.arm(context, spec).status is ComponentStatus.PASS
    result = fault.cleanup(context, spec)
    assert result.status is ComponentStatus.FAIL
    assert "never triggered" in result.summary


def test_command_fault_fires_once_on_its_event_and_records_placement():
    handle = FakeHandle()
    context = FakeContext(handle)
    spec = ComponentSpec(
        kind=ComponentKind.FAULT,
        component_id="valkey.command_fault",
        instance_id="cancel",
        config={"when_event": "bgsave.started", "command": ["BGSAVE", "CANCEL"], "node": "primaries"},
    )
    fault = CommandFault()
    fault.arm(context, spec)
    trigger = context.event_bus.publish("bgsave.started", source="action")
    context.event_bus.publish("bgsave.started", source="action")
    result = fault.cleanup(context, spec)
    assert result.status is ComponentStatus.PASS
    assert handle.clients["node-0"].commands == [("BGSAVE", "CANCEL")]
    (evidence,) = result.evidence
    assert evidence.evidence_id == FAULT_APPLIED
    assert evidence.event_sequence == trigger.sequence
    assert context.event_bus.committed_history("fault.injected")


def test_command_fault_reports_unexpected_errors_as_not_applied():
    handle = FakeHandle()
    handle.clients["node-0"] = FakeClient(error=RuntimeError("ERR no save"))
    context = FakeContext(handle)
    spec = ComponentSpec(
        kind=ComponentKind.FAULT,
        component_id="valkey.command_fault",
        instance_id="cancel",
        config={"when_event": "go", "command": ["BGSAVE", "CANCEL"]},
    )
    fault = CommandFault()
    fault.arm(context, spec)
    context.event_bus.publish("go", source="test")
    result = fault.cleanup(context, spec)
    assert result.status is ComponentStatus.FAIL
    assert not result.evidence


def test_sigstop_fault_resumes_the_process(monkeypatch):
    handle = FakeHandle()
    sent = []
    handle.send_signal = lambda node_id, signum: sent.append((node_id, signum))
    context = FakeContext(handle)
    spec = ComponentSpec(
        kind=ComponentKind.FAULT,
        component_id="valkey.process_signal",
        instance_id="pause",
        config={"when_event": "go", "signal": "SIGSTOP", "pause_s": 0.0},
    )
    fault = ProcessSignalFault()
    fault.arm(context, spec)
    context.event_bus.publish("go", source="test")
    result = fault.cleanup(context, spec)
    import signal

    assert sent == [("node-0", signal.SIGSTOP), ("node-0", signal.SIGCONT)]
    assert result.status is ComponentStatus.PASS


# ---------------------------------------------------------------------------
# oracles
# ---------------------------------------------------------------------------


def test_evidence_assert_requires_and_forbids():
    spec = ComponentSpec(
        kind=ComponentKind.ORACLE,
        component_id="valkey.evidence.assert",
        instance_id="assert",
        config={"require": [{"evidence_id": "a.b", "min_count": 2}], "forbid": [{"evidence_id": "c.d"}]},
    )
    evidence = [Evidence("a.b", "x", "p", time.time(), 1), Evidence("a.b", "y", "p", time.time(), 2)]
    oracle = EvidenceAssertOracle()
    assert oracle.evaluate(FakeContext(FakeHandle()), spec, evidence).status is ComponentStatus.PASS
    evidence.append(Evidence("c.d", "z", "p", time.time(), 3))
    assert oracle.evaluate(FakeContext(FakeHandle()), spec, evidence).status is ComponentStatus.FAIL
