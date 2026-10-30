"""The Valkey component library without Valkey processes: config checks at compile time, and the observers,
actions, faults, and oracles against fake deployments."""

import os
import signal
import time
from types import SimpleNamespace

import pytest
from valkey.exceptions import ResponseError

from tests.support import VirtualClock
from tests.valkey_experiments_support import fake_context, registration_for, resolved, unit_plan
from valkey_fuzzer.compiler import ScenarioCompileError
from valkey_fuzzer.contracts import ComponentRole, ComponentSpec, ComponentStatus, Evidence
from valkey_fuzzer.extensions import (
    ComponentRegistration,
    ConfigError,
    ConfigField,
    Declarations,
    EnvironmentKind,
    EnvironmentReference,
    EvidenceDeclaration,
    PluginManifest,
    ResourceSpec,
    load_experiment_yaml,
    make_result,
)
from valkey_fuzzer.extensions.testing import OutcomeStatus, run_experiment
from valkey_fuzzer_experiments import actions, common, component_registrations
from valkey_fuzzer_experiments.faults import FAULT_INJECTED, CommandFault, ProcessSignalFault
from valkey_fuzzer_experiments.observers import InfoObserver, LogObserver
from valkey_fuzzer_experiments.oracles import DeploymentStateOracle, EvidenceAssertOracle, LivenessOracle
from valkey_fuzzer_experiments.snapshots import DatasetDigestOracle

_DISK = EnvironmentReference(EnvironmentKind.RESOURCE, "disk", "path")
_TMPFS = {"resources": (ResourceSpec("disk", "valkey.resource.tmpfs", {"size_mb": 32}),)}
_ROOTS = {"mode": "standalone", "instance_data_roots": {"primary": _DISK}}


def _component(component_id, component, **config):
    return ComponentSpec(role=ComponentRole(component_id.split(".")[1]), use=component_id, id=component, config=config)


_BGSAVE = {"extra": (_component("valkey.action.bgsave", "bgsave"),)}
_LEDGER = {"workload": "writes", "loss_window_faults": ["kill"]}
_KILL = {
    "extra": (
        _component("valkey.workload.key-value", "writes"),
        _component("valkey.fault.process-signal", "kill", when_event="x"),
    )
}
_SHARDS = {"mode": "cluster", "document": {"mode": "cluster", "primaries": 3, "replicas_per_primary": 1}}
_FORCED = {"extra": (_component("valkey.action.failover", "failover", force=True),), "mode": "cluster"}


@pytest.mark.parametrize(
    ("component_id", "config", "options", "error"),
    [
        ("valkey.workload.key-value", {"mix": {"zap": 1}}, {}, r"mix contains unknown fields: \['zap'\]"),
        # No operation thread runs, so <instance>.drained would never be published.
        ("valkey.workload.key-value", {"drain_on_stop": True, "max_ops": 10, "rate": 0}, {}, "drain_on_stop requires rate > 0 and mutable_keys > 0"),
        ("valkey.workload.key-value", {"wait_replicas": 2}, {"mode": "cluster"}, "standalone"),
        ("valkey.workload.key-value", {"wait_replicas": 2}, {"document": {"mode": "standalone", "replicas_per_primary": 1}}, "exceeds replicas_per_primary"),
        ("valkey.action.bgsave", {"instances": "everyone"}, {}, "must be all, primaries, primary, replicas, replica, or instance IDs"),
        ("valkey.action.command", {"command": []}, {}, "non-empty"),
        ("valkey.action.command", {"command": ["PING"], "expect": {}}, {}, "predicate"),
        ("valkey.action.restart-instance", {"instance": "primaries,replicas"}, {}, "config field 'instance'"),
        ("valkey.action.failover", {}, {}, "supports deployment modes"),
        # A shard the cluster does not have is a config error, not a finding about Valkey.
        ("valkey.action.failover", {"shard": 3}, _SHARDS, "shard 3 is not below primaries 3"),
        ("valkey.action.cluster-election", {"shard": 3}, _SHARDS, "shard 3 is not below primaries 3"),
        ("valkey.action.poll", {"section": "s", "field": "f", "command": ["PING"], "predicate": {"equals": 1}}, {}, "section and field, or command"),
        ("valkey.observer.info", {"expectations": [{"field": "x", "equals": 1}]}, {}, "missing required field 'evidence_id'"),
        ("valkey.observer.info", {"expectations": [{"evidence_id": "a.b", "field": "x", "equals": 1, "required": "yes"}]}, {}, "required must be a boolean"),
        ("valkey.observer.info", {"expectations": [{"evidence_id": "a.b", "field": "x", "delta_min": 1, "equals": 2}]}, {}, "cannot combine"),
        # A misspelt key next to a predicate is an error, not silently ignored.
        ("valkey.observer.info", {"expectations": [{"evidence_id": "a.b", "field": "x", "equals": 1, "whne": "final"}]}, {}, r"contains unknown fields: \['whne'\]"),
        ("valkey.observer.log", {"expect": [{"evidence_id": "a.b", "pattern": "("}]}, {}, "valid regex"),
        ("valkey.capability.host-probe", {"requires": ["moon"]}, {}, r"requires\[0\] must be one of"),
        ("valkey.capability.build-probe", {"scope": "listed"}, {}, "needs at least one"),
        ("valkey.capability.build-probe", {"scope": "listed", "binaries": ["/old"], "deployment_config": True}, {}, "scope: listed skips"),
        ("valkey.capability.build-probe", {"lacks_configs": [""]}, {}, "must hold config names"),
        ("valkey.capability.build-probe", {"config_set": {"x": "zstd"}}, {}, "must be a list"),
        ("valkey.capability.build-probe", {"config_set": {"x": []}}, {}, "at least one value"),
        ("valkey.capability.build-probe", {"lacks_configs": ["x"], "config_get": {"x": None}}, {}, "both required"),
        ("valkey.oracle.rdb-snapshot", {"bgsave": "bgsave", "instance_reload": True}, _BGSAVE, "needs expect_codec"),
        ("valkey.oracle.rdb-snapshot", {"bgsave": "bgsave", "refused_log": "x"}, _BGSAVE, "only with expect: refused"),
        ("valkey.oracle.rdb-snapshot", {"bgsave": "bgsave", "expect": "refused", "refused_log": "("}, _BGSAVE, "not a valid regex"),
        ("valkey.oracle.rdb-snapshot", {"bgsave": "bgsave"}, {"extra": (_component("valkey.action.bgsave", "bgsave", capture_rdb=False),)}, "capture_rdb: false"),
        ("valkey.oracle.rdb-snapshot", {"bgsave": "pause"}, {"extra": (_component("valkey.action.sleep", "pause", seconds=1),)}, r"'pause' is a valkey\.action\.sleep, not a valkey\.action\.bgsave"),
        ("valkey.oracle.deployment-state", {"stabilization_wait_s": 60, "timeout_s": 60}, {}, "must be less than timeout_s"),
        ("valkey.oracle.ledger", {"workload": "missing"}, {}, "not a component"),
        # appendfsync always cannot lose an acknowledged write to a process fault (appendonly yes is the local
        # deployment's default); everysec may lose two seconds of writes; a forced failover can lose writes.
        ("valkey.oracle.ledger", {**_LEDGER, "loss_window_s": 2}, {**_KILL, "document": {"mode": "standalone", "server_config": {"appendfsync": "always"}}}, "appendfsync always"),
        ("valkey.oracle.ledger", {**_LEDGER, "loss_window_s": 1}, _KILL, "appendfsync everysec"),
        ("valkey.oracle.ledger", {"workload": "failover", "loss_window_faults": []}, _FORCED, "FORCE"),
        # Only a tmpfs resource that holds instance data is ever filled.
        ("valkey.fault.disk-fill", {"path": _DISK, "when_event": "x"}, _TMPFS, "instance_data_roots"),
        ("valkey.fault.disk-fill", {"path": _DISK, "when_event": "x"}, {"document": _ROOTS, "resources": (ResourceSpec("disk", "valkey.resource.tmpdir"),)}, r"of a valkey\.resource\.tmpfs resource"),
        ("valkey.fault.disk-fill", {"path": "/mnt/disk", "when_event": "x"}, {"document": {"mode": "standalone", "instance_data_roots": {"primary": "/mnt/disk"}}}, r"of a valkey\.resource\.tmpfs resource"),
    ],
)  # fmt: skip
def test_a_bad_component_config_fails_to_compile(component_id, config, options, error):
    spec = _component(component_id, "subject", **config)
    plan = unit_plan(
        (spec, *options.get("extra", ())),
        deployment_mode=options.get("mode", "standalone"),
        document=options.get("document"),
        resources=options.get("resources", ()),
    )
    with pytest.raises(ConfigError, match=error):
        registration_for(component_id).resolve_config(spec, plan)


class FakeClient:
    """Replies ``reply`` to every command (or raises ``error``); INFO answers with the next of ``infos``."""

    def __init__(self, error=None, reply="OK", infos=({"role": "master"},)):
        self.error, self.reply, self.infos, self.commands, self.closed = error, reply, list(infos), [], False

    def execute_command(self, *args):
        self.commands.append(args)
        if self.error is not None:
            raise self.error
        return self.reply

    def info(self, section="all"):
        return self.infos.pop(0) if len(self.infos) > 1 else self.infos[0]

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        pass


class FakeHandle:
    """A primary valkey-0 and replicas of one shard; a stopped instance stays down until ``restart``."""

    def __init__(self, instances=("valkey-0", "valkey-1"), log_dir=None):
        self.instances = tuple(
            SimpleNamespace(instance_id=node_id, role="replica" if index else "primary", pid=1000 + index, shard_id=0)
            for index, node_id in enumerate(instances)
        )
        self.mode = SimpleNamespace(value="standalone")
        self.log_dir, self.clients, self.alive, self.restarted = log_dir, {}, dict.fromkeys(instances, True), []

    def primaries(self):
        return [node.instance_id for node in self.instances if node.role == "primary"]

    def replicas(self):
        return [node.instance_id for node in self.instances if node.role == "replica"]

    def roles(self):
        return {node.instance_id: node.role for node in self.instances}

    def instance(self, node_id):
        return next(node for node in self.instances if node.instance_id == node_id)

    def pid(self, node_id):
        return self.instance(node_id).pid

    def log_file(self, node_id):
        return os.path.join(self.log_dir, f"{node_id}.log")

    def process_output_file(self, node_id):
        return os.path.join(self.log_dir, f"{node_id}.stdio.log")

    def new_client(self, node_id, timeout=5.0):
        return self.clients.setdefault(node_id, FakeClient())

    def send_signal(self, node_id, signum):
        self.alive[node_id] = signum in (signal.SIGSTOP, signal.SIGCONT)

    def is_alive(self, node_id):
        return self.alive[node_id]

    def wait_exit(self, node_id, timeout=10):
        return True

    def restart(self, node_id, **options):
        self.alive[node_id] = True
        self.restarted.append(node_id)

    def started_at(self, node_id):
        return 5.0

    def failover(self, shard, force, timeout, rng):
        return True


def _spec(component_id, component, **config):
    return resolved(_component(component_id, component, **config))


@pytest.fixture
def clock(monkeypatch):
    """Time that passes only as the components wait: retries and timeouts take no real time."""

    return VirtualClock().install(monkeypatch, common, actions)


# Observers


def test_the_log_observer_reads_only_its_window_and_reports_forbidden_lines(tmp_path):
    log = tmp_path / "valkey-0.log"
    log.write_text("forkless save complete\n")
    expect = [{"evidence_id": "t.complete", "pattern": "save complete"}]
    spec = _spec("valkey.observer.log", "logs", expect=expect, forbidden=[{"pattern": "BUG REPORT"}])
    context = fake_context(FakeHandle(("valkey-0",), log_dir=str(tmp_path)), artifact_dir=str(tmp_path / "artifacts"))
    observer = LogObserver()
    observer.start(context, spec)
    assert not observer.stop(context, spec).evidence, "a line written before the window is not evidence"

    observer = LogObserver()
    observer.start(context, spec)
    with log.open("a") as handle_file:
        handle_file.write("forkless save complete\n=== VALKEY BUG REPORT START\n")
    result = observer.stop(context, spec)
    # A forbidden line is a finding for valkey.oracle.liveness, not an observer failure.
    assert result.status is ComponentStatus.PASS
    found = {item.evidence_id: item.value for item in result.evidence}
    assert set(found) == {"t.complete", "valkey.log.forbidden-match"}
    assert found["valkey.log.forbidden-match"] == {"BUG REPORT": {"valkey-0": ("=== VALKEY BUG REPORT START",)}}


def test_the_info_observer_judges_each_expectation_over_its_samples(tmp_path):
    handle = FakeHandle()
    base = {"type": "none", "saves": "0", "slave0": {"codec": "lz4", "bytes": 100}, "slave1": {"codec": "lz4", "bytes": 50}}  # fmt: skip
    during = {**base, "type": "forkless", "saves": "1"}
    final = {"saves": "1", "type": "none", "slave0": {"codec": "lz4", "bytes": 400}, "slave1": {"codec": "lz4", "bytes": 400}}  # fmt: skip
    handle.clients = {"valkey-0": FakeClient(infos=(base, during, final)), "valkey-1": FakeClient(infos=(base,))}
    expectations = [
        {"evidence_id": "t.during", "field": "type", "equals": "forkless", "when": "during"},
        {"evidence_id": "t.final", "field": "type", "equals": "forkless", "when": "final"},
        {"evidence_id": "t.delta", "field": "saves", "delta_min": 1},
        {"evidence_id": "t.any", "field": "saves", "equals": "1", "when": "final", "instances": "any"},
        {"evidence_id": "t.all", "field": "saves", "equals": "1", "when": "final", "instances": "all"},
        # A wildcard path matches when every entry it names does; its delta is their sum's.
        {"evidence_id": "t.codec", "field": "slave*.codec", "equals": "lz4", "when": "final"},
        {"evidence_id": "t.bytes", "field": "slave*.bytes", "delta_min": 600},
        {"evidence_id": "t.in", "field": "type", "in": ["fork", "forkless"], "when": "during"},
        {"evidence_id": "t.contains", "field": "type", "contains": "fork"},
        {"evidence_id": "t.absent", "field": "missing", "exists": False},
        {"evidence_id": "t.min", "field": "saves", "min": 1, "when": "final"},
        # A value that is not a number never meets a numeric bound.
        {"evidence_id": "t.number", "field": "type", "max": 5},
    ]  # fmt: skip
    config = {"instances": "all", "sections": ["all"], "on_events": ["go"], "interval_s": 0}
    spec = _spec("valkey.observer.info", "info", expectations=expectations, **config)
    context = fake_context(handle, artifact_dir=str(tmp_path))
    observer = InfoObserver()
    observer.start(context, spec)
    context.event_bus.publish("go", source="test")
    result = observer.stop(context, spec)
    met = {item.evidence_id: item.value for item in result.evidence}
    assert set(met) == {"t.during", "t.delta", "t.any", "t.codec", "t.bytes", "t.in", "t.contains", "t.absent", "t.min"}
    assert result.summary.endswith("unmet: t.final, t.all, t.number")
    assert met["t.bytes"]["deltas"]["valkey-0"] == {"baseline": 150.0, "final": 800.0, "delta": 650.0}


class _FlakyClient(FakeClient):
    """INFO answers with the next of ``infos``, except the calls numbered in ``timeouts`` (from 0), which time out."""

    def __init__(self, infos, timeouts=()):
        super().__init__(infos=infos)
        self.calls, self.timeouts = 0, set(timeouts)

    def info(self, section="all"):
        self.calls += 1
        if self.calls - 1 in self.timeouts:
            raise TimeoutError("Timeout reading from socket")
        return super().info(section)


_ALL = range(100)


@pytest.mark.parametrize(
    ("samples", "expectation", "restart", "outcome"),
    [
        # Seen, and the server was wrong: a finding.
        ([({"aof_enabled": 0},), ()], {"field": "aof_enabled", "equals": 1, "when": "final"}, False, "unmet"),
        ([({"sync_full": 3}, {"sync_full": 4}), ()], {"field": "sync_full", "delta_max": 0}, False, "unmet"),
        ([({"sync_full": 3}, {"sync_full": 3}), ()], {"field": "sync_full", "delta_min": 1}, False, "unmet"),
        # With instances: all, one instance seen not to meet it decides it, whatever the other's samples.
        ([({"aof_enabled": 0},), ()], {"field": "aof_enabled", "equals": 1, "when": "final", "instances": "all"}, False, "unmet"),
        ([({"sync_full": 3}, {"sync_full": 3}), ()], {"field": "sync_full", "delta_max": 0}, False, "met"),
        # Never seen: every sample, or the one the expectation needs, timed out.
        ([({"aof_enabled": 1},), _ALL], {"field": "aof_enabled", "equals": 1, "when": "final"}, False, "unobserved"),
        ([({"aof_enabled": 1},), {1}], {"field": "aof_enabled", "equals": 1, "when": "final"}, False, "unobserved"),
        ([({"rdb_saves": 0}, {"rdb_saves": 1}), {1}], {"field": "rdb_saves", "delta_min": 1}, False, "unobserved"),
        # A counter that reset hides a full resync from delta_max: 3, a restart, then 1.
        ([({"sync_full": 3}, {"sync_full": 1}), ()], {"field": "sync_full", "delta_max": 0}, False, "unobserved"),
        ([({"sync_full": 3}, {"sync_full": 3}), ()], {"field": "sync_full", "delta_max": 0}, True, "unobserved"),
    ],
    ids=["final-value-wrong", "full-resync-counted", "no-save-counted", "all-one-instance-wrong", "delta-within-bound",
         "every-sample-timed-out", "final-sample-timed-out", "final-delta-sample-timed-out", "counter-reset",
         "process-restarted"],
)  # fmt: skip
def test_the_info_observer_tells_an_unmet_expectation_from_one_it_could_not_judge(
    tmp_path, samples, expectation, restart, outcome
):
    handle = FakeHandle()
    infos, timeouts = samples
    # valkey-1 never answers: with instances: any its silence decides nothing once valkey-0 is judged.
    handle.clients = {"valkey-0": _FlakyClient(infos, timeouts), "valkey-1": _FlakyClient(({},), _ALL)}
    instances = "all" if expectation.get("instances") == "all" else "primaries"
    expectations = [{"evidence_id": "t.check", **expectation, "instances": "all", "required": True}]
    spec = _spec("valkey.observer.info", "info", interval_s=0, instances=instances, expectations=expectations)
    context = fake_context(handle, artifact_dir=str(tmp_path))
    observer = InfoObserver()
    observer.start(context, spec)
    if restart:
        handle.instances[0].pid += 100
    result = observer.stop(context, spec)
    assert result.status is ComponentStatus.PASS
    found = {
        "met": bool(result.evidence),
        "unmet": result.diagnostics["unmet_expectations"] == ("t.check",),
        "unobserved": result.unobserved == ("t.check",),
    }
    assert [name for name, holds in found.items() if holds] == [outcome], result.summary


@pytest.mark.parametrize(
    ("timeouts", "precondition", "status", "summary"),
    [
        # The server was seen not saving, in the state the expectation is about (the ping action replied).
        ((), "valkey.command.replies", OutcomeStatus.FAIL, "required evidence was not observed: t.saved from info"),
        # The server was never seen.
        (_ALL, None, OutcomeStatus.ERROR, "required evidence was not established: t.saved from info"),
        # The run never reached that state (no forbidden line was logged): the missing save says nothing.
        ((), "valkey.log.forbidden-match", OutcomeStatus.ERROR, "required evidence was not established: t.saved from info"),
    ],
    ids=["server-seen-not-saving", "server-never-seen", "precondition-never-reached"],
)  # fmt: skip
def test_a_required_expectation_is_a_finding_only_if_the_server_was_seen_in_its_state(
    tmp_path, timeouts, precondition, status, summary
):
    handle = FakeHandle(("valkey-0",), log_dir=str(tmp_path))
    (tmp_path / "valkey-0.log").write_text("")
    handle.clients = {"valkey-0": _FlakyClient(({"rdb_saves": 0},), timeouts)}
    condition = f", precondition: {precondition}" if precondition else ""
    experiment = load_experiment_yaml(
        "id: tests.observe\nmode: experiment\ndeployment: {modes: [standalone]}\ncomponents:\n"
        "  - use: valkey.observer.info\n    id: info\n"
        f"    config: {{interval_s: 0, expectations: [{{evidence_id: t.saved, field: rdb_saves, delta_min: 1, required: true{condition}}}]}}\n"
        "  - {use: valkey.observer.log, id: logs, config: {forbidden: [{pattern: 'BUG REPORT'}]}}\n"
        "  - {use: valkey.action.command, id: ping, config: {instances: all, command: [PING]}}\n"
        "  - {use: valkey.oracle.evidence, id: check, config: {require: [{evidence_id: valkey.command.replies}]}}\n"
    )
    plugin = PluginManifest(
        plugin_id="tests.observe", registrations=component_registrations(), experiments=(experiment,)
    )
    outcome = run_experiment(
        plugin, "tests.observe", services={"valkey.deployment": handle}, artifact_dir=str(tmp_path)
    )
    assert outcome.status is status, outcome.summary
    assert outcome.summary.startswith(summary)


@pytest.mark.parametrize(
    ("config", "status", "unmet", "unobserved"),
    [
        ({"expect": [{"evidence_id": "t.saved", "pattern": "DB saved"}]}, "PASS", (), ("t.saved",)),
        ({"expect": [{"evidence_id": "t.saved", "pattern": "DB saved", "instances": "all"}]}, "PASS", ("t.saved",), ()),
        ({"forbidden": [{"pattern": "BUG REPORT"}]}, "ERROR", (), ()),
    ],
    ids=["any-unread-log-might-hold-it", "all-a-read-log-lacks-it", "forbidden-unchecked"],
)  # fmt: skip
def test_the_log_observer_does_not_judge_a_log_it_could_not_read(tmp_path, config, status, unmet, unobserved):
    (tmp_path / "valkey-0.log").write_text("")
    spec = _spec("valkey.observer.log", "logs", **config)
    context = fake_context(FakeHandle(log_dir=str(tmp_path)), artifact_dir=str(tmp_path / "artifacts"))
    observer = LogObserver()
    observer.start(context, spec)
    # valkey-1's log never appeared, and is unreadable when the observer stops.
    result = observer.stop(context, spec)
    assert result.status.value == status, result.summary
    assert (tuple(result.diagnostics["unmet"]), result.unobserved) == (unmet, unobserved)
    assert "valkey-1" in result.diagnostics["unreadable"]


# Actions


@pytest.mark.parametrize(
    ("component_id", "config", "status", "evidence"),
    [
        ("valkey.action.command", {"command": ["PING"], "instances": "all"}, "PASS", None),
        ("valkey.action.command", {"command": ["PING"], "expect": {"equals": "PONG"}}, "FAIL", None),
        ("valkey.action.poll", {"section": "replication", "field": "role", "predicate": {"equals": "master"}}, "PASS", {"valkey-0": "master", "valkey-1": "master"}),
        ("valkey.action.poll", {"command": ["PING"], "predicate": {"equals": "PONG"}, "timeout_s": 0.2}, "FAIL", None),
        ("valkey.action.restart-instance", {"instance": "valkey-1"}, "PASS", {"instance_id": "valkey-1", "pid": 1001}),
        ("valkey.action.failover", {"target": "valkey-1", "force": True}, "PASS", {"primary_before": "valkey-0", "force": True, "success": True}),
    ],
)  # fmt: skip
def test_actions_report_what_they_did_to_the_deployment(component_id, config, status, evidence):
    handle = FakeHandle()
    spec = _spec(component_id, "act", **config)
    action = registration_for(component_id).factory()
    result = action.run(fake_context(handle, spec), spec)
    assert result.status.value == status, result.summary
    if evidence is not None:
        assert evidence.items() <= dict(result.evidence[0].value).items()
    if component_id.endswith("restart-instance"):
        assert handle.restarted == ["valkey-1"] and handle.alive["valkey-1"]


@pytest.mark.parametrize(
    ("errors", "status", "summary"),
    [
        ({}, "FAIL", "PING not satisfied before timeout"),
        # One instance seen not to answer PONG decides it, whatever the other's reads.
        ({"valkey-1": TimeoutError("Timeout reading from socket")}, "FAIL", "PING not satisfied before timeout"),
        ({"valkey-0": TimeoutError("timed out"), "valkey-1": TimeoutError("timed out")}, "ERROR", "cannot read PING on valkey-0, valkey-1 before timeout"),
    ],
    ids=["seen-not-satisfied", "one-seen-one-unread", "never-read"],
)  # fmt: skip
def test_a_poll_that_could_not_read_is_an_error_and_one_that_saw_the_wrong_value_fails(clock, errors, status, summary):
    handle = FakeHandle()
    handle.clients = {node_id: FakeClient(error=errors.get(node_id)) for node_id in ("valkey-0", "valkey-1")}
    spec = _spec("valkey.action.poll", "poll", command=["PING"], predicate={"equals": "PONG"}, timeout_s=2)
    result = registration_for("valkey.action.poll").factory().run(clock.drive(fake_context(handle, spec)), spec)
    assert (result.status.value, result.summary) == (status, summary) and clock.elapsed >= 2


def test_an_event_that_never_came_is_an_error_of_the_wait_not_a_failure():
    spec = _spec("valkey.action.wait-event", "drained", event="writes.drained", timeout_s=0.05)
    result = registration_for("valkey.action.wait-event").factory().run(fake_context(FakeHandle(), spec), spec)
    assert (result.status.value, result.summary) == ("ERROR", "writes.drained observed 0/1 times before timeout")


class _SaveClient(FakeClient):
    """BGSAVE writes ``rdb`` into ``directory`` (unless it is None) and ends with ``status``."""

    def __init__(self, directory, rdb, status="ok", setup_error=False):
        super().__init__(reply="Background saving started")
        self.directory, self.rdb, self.status, self.setup_error, self.polls = directory, rdb, status, setup_error, 0

    def config_get(self, name):
        if self.setup_error:
            raise RuntimeError("ERR unknown command 'CONFIG'")
        return {name: {"dir": str(self.directory), "dbfilename": "dump.rdb"}[name]}

    def execute_command(self, *args):
        if self.rdb is not None:
            (self.directory / "dump.rdb").write_bytes(self.rdb)
        return super().execute_command(*args)

    def info(self, section="all"):
        if section == "replication":
            return {"role": "master"}
        self.polls += 1
        done = self.polls > 2
        return {"rdb_bgsave_in_progress": "0" if done else "1", "rdb_current_bgsave_type": "none" if done else "forkless", "rdb_last_bgsave_status": self.status, "rdb_last_bgsave_type": "forkless", "rdb_saves": self.polls}  # fmt: skip


class _BusyClient(FakeClient):
    """BGSAVE is refused while a replication RDB child runs for the first ``busy`` sends, then writes the RDB."""

    def __init__(self, directory, busy, error="Background save already in progress"):
        super().__init__(reply="Background saving started")
        self.directory, self.busy, self.error_text, self.saves = directory, busy, error, 0

    def config_get(self, name):
        return {name: {"dir": str(self.directory), "dbfilename": "dump.rdb"}[name]}

    def execute_command(self, *args):
        self.commands.append(args)
        if self.busy > 0:
            self.busy -= 1
            raise ResponseError(self.error_text)
        self.saves += 1
        (self.directory / "dump.rdb").write_bytes(b"REDIS0011")
        return self.reply

    def info(self, section="all"):
        if section == "replication":
            return {"role": "master"}
        return {"rdb_bgsave_in_progress": "0", "rdb_last_bgsave_status": "ok", "rdb_saves": self.saves}


@pytest.mark.parametrize(
    ("busy", "error", "status", "summary"),
    [
        (2, "Background save already in progress", "PASS", "BGSAVE ok on 1 instance(s)"),
        (10_000, "Background save already in progress", "FAIL", "valkey-0: expected ok, got rejected"),
        (1, "ERR unknown command 'BGSAVE'", "FAIL", "valkey-0: expected ok, got rejected"),
    ],
    ids=["busy-then-accepted", "busy-until-the-timeout", "another-error"],
)
def test_bgsave_waits_out_a_save_another_child_is_running(clock, tmp_path, busy, error, status, summary):
    from valkey_fuzzer_experiments.actions import BgsaveAction

    handle = FakeHandle(("valkey-0",))
    handle.clients["valkey-0"] = client = _BusyClient(tmp_path, busy, error)
    spec = _spec("valkey.action.bgsave", "save", instances="all", timeout_s=5)
    result = BgsaveAction().run(clock.drive(fake_context(handle, artifact_dir=str(tmp_path / "artifacts"))), spec)
    assert result.status.value == status, result.summary
    assert result.summary.startswith(summary)
    # Sent again only while Valkey said another save was running.
    assert len(client.commands) > 2 if busy == 10_000 else len(client.commands) == (busy + 1 if status == "PASS" else 1)


@pytest.mark.parametrize(
    ("rdb", "status", "setup_error", "result_status", "summary"),
    [
        (b"VCS\x01\x01\x00\x01" + b"\x00" * 8, "ok", False, "PASS", "BGSAVE ok on 2 instance(s)"),
        (None, "err", False, "FAIL", "expected ok, got err"),
        # A save that wrote nothing new did not complete.
        (None, "ok", False, "FAIL", "expected ok, got not_completed"),
        (None, "ok", True, None, None),
    ],
    ids=["saved", "save-failed", "nothing-written", "setup-fails"],
)
def test_bgsave_follows_each_save_and_records_the_rdb_it_wrote(
    tmp_path, rdb, status, setup_error, result_status, summary
):
    from valkey_fuzzer_experiments.actions import BgsaveAction

    handle = FakeHandle()
    for node in handle.instances:
        (tmp_path / node.instance_id).mkdir()
        failing = setup_error and node.instance_id == "valkey-1"
        handle.clients[node.instance_id] = _SaveClient(tmp_path / node.instance_id, rdb, status, failing)
    spec = _spec("valkey.action.bgsave", "save", instances="all", expect_type="forkless")
    context = fake_context(handle, artifact_dir=str(tmp_path / "artifacts"))
    if setup_error:
        with pytest.raises(RuntimeError, match="unknown command"):
            BgsaveAction().run(context, spec)
    else:
        result = BgsaveAction().run(context, spec)
        assert result.status.value == result_status and summary in result.summary
        records = [dict(item.value) for item in result.evidence]
        assert all(("rdb_artifact" in record) is (rdb is not None) for record in records)
        if rdb is not None:
            assert {record["rdb_format"]["codec"] for record in records} == {"lz4"}
            assert all(os.path.exists(record["rdb_artifact"]) for record in records)
    # Every client is closed, even when a later target's setup failed.
    assert all(client.closed for client in handle.clients.values())


# Faults


@pytest.mark.parametrize(
    ("triggers", "error", "status", "applied"),
    [(0, None, "FAIL", False), (2, None, "PASS", True), (1, RuntimeError("ERR no save"), "FAIL", False)],
    ids=["never-triggered", "fires-once", "error-not-applied"],
)
def test_a_command_fault_fires_once_on_its_event_and_records_only_what_it_applied(triggers, error, status, applied):
    handle = FakeHandle()
    handle.clients["valkey-0"] = FakeClient(error=error)
    context = fake_context(handle)
    spec = _spec("valkey.fault.command", "cancel", when_event="go", command=["BGSAVE", "CANCEL"])
    fault = CommandFault()
    fault.inject(context, spec)
    events = [context.event_bus.publish("go", source="action") for _ in range(triggers)]
    result = fault.recover(context, spec)
    assert result.status.value == status, result.summary
    assert handle.clients["valkey-0"].commands == [("BGSAVE", "CANCEL")] * min(triggers, 1)
    assert [item.event_sequence for item in result.evidence] == ([events[0].sequence] if applied else [])


@pytest.mark.parametrize("restart_mode", ["immediate", "background"])
def test_a_failed_restart_is_retried_and_the_kill_is_still_reported(restart_mode):
    handle = FakeHandle()
    attempts = []

    def restart(node_id, wait_ready=True, ready_timeout=30.0):
        attempts.append(node_id)
        if len(attempts) == 1:
            raise RuntimeError("did not become ready")
        handle.alive[node_id] = True

    handle.restart = restart
    context = fake_context(handle)
    spec = _spec("valkey.fault.process-signal", "kill", when_event="go", signal="SIGKILL", restart_mode=restart_mode)
    fault = ProcessSignalFault()
    fault.inject(context, spec)
    context.event_bus.publish("go", source="test")
    result = fault.recover(context, spec)
    assert attempts == ["valkey-0", "valkey-0"], "recovery retries the restart"
    # The kill outlasted the fault's own restart, so the fault reports it (and the run is ERROR).
    assert result.status is ComponentStatus.ERROR
    assert "process.stopped on valkey-0 failed before a retry succeeded" in result.summary
    record = next(item.value for item in result.evidence if item.evidence_id == FAULT_INJECTED)
    assert "did not become ready" in record["restart_error"] and record["restarted"] is True
    # The restart is stamped when the new process started, not when it became ready.
    assert record["restart_spawned_at"] == 5.0 < record["restart_ready_at"]


@pytest.mark.parametrize(
    ("provisioned", "mounted", "config", "status", "summary", "written"),
    [
        (None, 1 << 30, {}, "ERROR", "not a mount provisioned by a valkey.resource.tmpfs resource", [None]),
        (8192, None, {}, "ERROR", "no longer a tmpfs mount point", [None]),
        (8192, 1 << 30, {}, "ERROR", "more than the 8192 it was provisioned with", [None]),
        # tmp_path is on a large disk: only the ceiling stops the writes.
        (3 * 4096, 3 * 4096, {}, "FAIL", "stopped at max_bytes (12288)", [3 * 4096]),
        (1 << 20, 1 << 20, {"max_bytes": 5000}, "FAIL", "stopped at max_bytes (5000)", [5000]),
    ],
)
def test_disk_fill_writes_only_into_the_runs_tmpfs_and_never_past_its_size(
    tmp_path, monkeypatch, provisioned, mounted, config, status, summary, written
):
    from valkey_fuzzer_experiments import providers
    from valkey_fuzzer_experiments.storage import FillFilesystemFault

    monkeypatch.setattr(providers, "provisioned_tmpfs_bytes", lambda path: provisioned)
    monkeypatch.setattr(providers, "mounted_tmpfs_bytes", lambda path: mounted)
    context = fake_context(FakeHandle())
    spec = _spec("valkey.fault.disk-fill", "fill", when_event="go", path=str(tmp_path), chunk_bytes=4096, **config)
    fault = FillFilesystemFault()
    fault.inject(context, spec)
    ballast, filled = tmp_path / ".ballast-fill", []
    context.event_bus.subscribe(
        "fill.injected", lambda event: filled.append(ballast.stat().st_size if ballast.exists() else None)
    )
    context.event_bus.publish("go", source="test")
    result = fault.recover(context, spec)
    assert result.status.value == status and summary in result.summary
    assert filled == written and not ballast.exists()


def test_a_disk_fill_release_that_fails_leaves_its_event_alone_and_recovery_reports_it(tmp_path, monkeypatch):
    from valkey_fuzzer_experiments import providers, storage

    monkeypatch.setattr(providers, "provisioned_tmpfs_bytes", lambda path: 8192)
    monkeypatch.setattr(providers, "mounted_tmpfs_bytes", lambda path: 8192)
    context = fake_context(FakeHandle())
    spec = _spec(
        "valkey.fault.disk-fill", "fill", when_event="go", path=str(tmp_path), chunk_bytes=4096, release_event="saved"
    )
    fault = storage.FillFilesystemFault()
    fault.inject(context, spec)
    context.event_bus.publish("go", source="test")
    remove = storage.os.remove
    monkeypatch.setattr(storage.os, "remove", lambda path: (_ for _ in ()).throw(OSError("device busy")))
    # bgsave publishes the release event: a failed release must not fail its publish.
    context.event_bus.publish("saved", source="bgsave")
    monkeypatch.setattr(storage.os, "remove", remove)
    result = fault.recover(context, spec)
    assert result.status is ComponentStatus.ERROR and "device busy" in result.summary
    assert not (tmp_path / ".ballast-fill").exists()


# Oracles


def _item(evidence_id, source):
    return Evidence(evidence_id, source, "p", time.time(), 1)


@pytest.mark.parametrize(
    ("evidence", "status"),
    [
        ([_item("a.b", "x"), _item("a.b", "y")], "PASS"),
        ([_item("a.b", "x")], "FAIL"),
        ([_item("a.b", "x"), _item("a.b", "y"), _item("c.d", "z")], "FAIL"),
    ],
    ids=["required-observed", "too-few", "forbidden-observed"],
)
def test_the_evidence_oracle_requires_and_forbids(evidence, status):
    spec = _spec(
        "valkey.oracle.evidence",
        "assert",
        require=[{"evidence_id": "a.b", "min_count": 2}],
        forbid=[{"evidence_id": "c.d"}],
    )
    assert EvidenceAssertOracle().evaluate(fake_context(FakeHandle()), spec, evidence).status.value == status


def _rule(rule):
    return f"{{use: valkey.oracle.evidence, id: check, config: {{{rule}}}}}"


@pytest.mark.parametrize(
    ("component", "message"),
    [
        (_rule("forbid: [{evidence_id: valkey.log.forbiden-match}]"), "'valkey.log.forbiden-match' is not declared by any component"),
        (_rule("require: [{evidence_id: valkey.data.ledger-verifeid}]"), "'valkey.data.ledger-verifeid' is not declared by any component"),
        (_rule("forbid: [{evidence_id: valkey.log.forbidden-match, source: nosuchcomponent}]"), "names source 'nosuchcomponent', which is not a component"),
        (_rule("require: [{evidence_id: valkey.log.forbidden-match, source: ping}]"), "is not declared by component 'ping'"),
        ("{use: valkey.observer.info, id: info, config: {expectations: [{evidence_id: t.saved, field: rdb_saves, delta_min: 1, precondition: t.typo}]}}\n"
         f"  - {_rule('require: [{evidence_id: valkey.command.replies}]')}", r"component 'info' names evidence 't\.typo' is not declared"),
    ],
    ids=["misspelt-forbid", "misspelt-require", "unknown-source", "wrong-source", "misspelt-precondition"],
)  # fmt: skip
def test_evidence_a_config_names_that_no_component_declares_does_not_compile(component, message):
    """A misspelt forbid rule would pass vacuously, and a misspelt require rule or precondition fail the server."""

    experiment = load_experiment_yaml(
        "id: tests.rules\nmode: experiment\ndeployment: {modes: [standalone]}\ncomponents:\n"
        "  - {use: valkey.action.command, id: ping, config: {instances: all, command: [PING]}}\n"
        "  - {use: valkey.observer.log, id: logs, config: {forbidden: [{pattern: 'BUG REPORT'}]}}\n"
        f"  - {component}\n"
        "required_evidence: [{evidence_id: valkey.command.replies, source: ping}]\n"
    )
    plugin = PluginManifest(plugin_id="tests.rules", registrations=component_registrations(), experiments=(experiment,))
    with pytest.raises(ScenarioCompileError, match=message):
        run_experiment(plugin, "tests.rules", services={"valkey.deployment": FakeHandle()})


class _LivenessHandle(FakeHandle):
    def new_client(self, node_id, timeout=5.0):
        client = FakeClient()
        client.ping, client.info = (lambda: True), (lambda section: {"loading": 0})
        return client


_FORBIDDEN = "Bad file format reading the append only file"


@pytest.mark.parametrize(
    ("output", "evidence", "summary"),
    [
        ("", (), None),
        ("==1==ERROR: AddressSanitizer: heap-use-after-free\n", (), "AddressSanitizer"),
        ("", (Evidence("valkey.log.forbidden-match", "aof-log", "observer.stopped", 1.0, {_FORBIDDEN: {"valkey-0": [_FORBIDDEN], "valkey-1": [_FORBIDDEN]}}),), f"valkey-0, valkey-1: forbidden log pattern {_FORBIDDEN!r} matched (seen by aof-log)"),
    ],
    ids=["healthy", "sanitizer-report-on-stderr", "forbidden-log-line"],
)  # fmt: skip
def test_liveness_fails_on_a_sanitizer_report_or_a_forbidden_log_line(tmp_path, output, evidence, summary):
    (tmp_path / "valkey-0.log").write_text("Ready to accept connections\n")
    (tmp_path / "valkey-0.stdio.log").write_text(output)
    handle = _LivenessHandle(("valkey-0",), log_dir=str(tmp_path))
    result = LivenessOracle().evaluate(fake_context(handle), _spec("valkey.oracle.liveness", "liveness"), evidence)
    assert result.status.value == ("PASS" if summary is None else "FAIL"), result.summary
    assert (summary or "") in result.summary


@pytest.mark.parametrize(
    ("record", "status", "summary"),
    [
        ({"overall_success": True, "error_messages": [], "failed_checks": []}, "PASS", "standalone state validated"),
        ({"overall_success": False, "error_messages": ["valkey-1 is not replicating valkey-0"], "failed_checks": ["replication"]}, "FAIL", "valkey-1 is not replicating valkey-0"),
        ({"overall_success": False, "error_messages": [], "failed_checks": ["slot_coverage"]}, "FAIL", "failed checks: ['slot_coverage']"),
        ({"overall_success": False, "inconclusive": True, "error_messages": ["running instance(s) did not answer: 127.0.0.1:7001"], "failed_checks": ["unanswered"]}, "ERROR", "running instance(s) did not answer: 127.0.0.1:7001"),
    ],
    ids=["validated", "a-failed-check", "a-failed-check-without-a-message", "inconclusive"],
)  # fmt: skip
def test_the_deployment_state_oracle_fails_when_the_deployments_validation_fails(record, status, summary):
    handle, asked = FakeHandle(), []
    handle.validate_state = lambda **checks: asked.append(checks) or record
    spec = _spec("valkey.oracle.deployment-state", "state", check_roles=False)
    result = DeploymentStateOracle().evaluate(fake_context(handle), spec, ())
    assert (result.status.value, result.summary) == (status, summary)
    assert asked[0]["check_roles"] is False and asked[0]["check_replication"] is True


class _KeyspaceClient(FakeClient):
    """An empty keyspace whose first ``failures`` SCANs time out."""

    def __init__(self, failures=0):
        super().__init__()
        self.failures = failures

    def scan_iter(self, match=None, count=1000):
        if self.failures > 0:
            self.failures -= 1
            raise TimeoutError("Timeout reading from socket")
        return iter(())


@pytest.mark.parametrize(
    ("roles", "failures", "status", "summary"),
    [
        (("primary", "replica"), 0, "PASS", "dataset digest"),
        (("primary", "replica"), 1, "PASS", "dataset digest"),
        # Every instance answers, and none is a primary: no primary holds the data.
        (("replica", "replica"), 0, "FAIL", "no instance is a primary, so no dataset was read"),
        (("primary", "replica"), 10_000, "ERROR", "cannot read the dataset: valkey-0: TimeoutError"),
        (("primary", None), 0, "ERROR", "cannot read the dataset: unreachable instance(s): valkey-1"),
    ],
    ids=["read", "read-after-a-timeout", "no-primary", "primary-never-read", "role-unreadable"],
)  # fmt: skip
def test_the_digest_oracle_needs_a_primary_and_never_fails_valkey_for_a_read_it_could_not_make(
    clock, tmp_path, roles, failures, status, summary
):
    handle = FakeHandle(("valkey-0", "valkey-1"))
    handle.instances = tuple(
        SimpleNamespace(**{**vars(node), "role": role}) for node, role in zip(handle.instances, roles)
    )
    handle.roles = lambda: {node.instance_id: node.role for node in handle.instances if node.role}
    handle.primaries = lambda: [node_id for node_id, role in handle.roles().items() if role == "primary"]
    handle.clients = {"valkey-0": _KeyspaceClient(failures), "valkey-1": _KeyspaceClient()}
    spec = _spec("valkey.oracle.dataset-digest", "dataset", converge_timeout_s=5)
    result = DatasetDigestOracle().evaluate(clock.drive(fake_context(handle, artifact_dir=str(tmp_path))), spec, ())
    assert result.status.value == status, result.summary
    assert result.summary.startswith(summary)
    # A read that failed was retried; one that never worked, for the whole converge_timeout_s.
    assert (clock.elapsed >= 5) is (status == "ERROR")


@pytest.mark.parametrize(
    ("role", "emitted", "status", "summary"),
    [
        ("action", 2, OutcomeStatus.PASS, ""),
        ("action", 1, OutcomeStatus.FAIL, "required evidence was not observed: tests.path from info"),
        # Only a component that looks at the server (observer, action, oracle) can find it wanting; a workload or
        # fault that records nothing never established what the plan needs.
        ("workload", 0, OutcomeStatus.ERROR, "required evidence was not established: tests.path from info"),
    ],
)  # fmt: skip
def test_missing_required_evidence_fails_a_run_unless_its_source_never_looked(tmp_path, role, emitted, status, summary):
    class Source:
        def run(self, context, spec):
            evidence = tuple(
                Evidence("tests.path", spec.id, "action.completed", time.time(), {}) for _ in range(emitted)
            )
            return make_result(spec, ComponentStatus.PASS, "observed", evidence=evidence)

        def start(self, context, spec):
            return make_result(spec, ComponentStatus.PASS, "started")

        def stop(self, context, spec):
            return make_result(spec, ComponentStatus.PASS, "stopped without its ledger")

    class Check:
        def evaluate(self, context, spec, evidence):
            return make_result(spec, ComponentStatus.PASS, "checked")

    declares = Declarations(evidence={"tests.path": EvidenceDeclaration(ConfigField(dict), many=True)})
    experiment = load_experiment_yaml(
        "id: tests.missing\nmode: experiment\ndeployment: {modes: [standalone]}\n"
        f"components: [{{use: tests.{role}.source, id: info}}, {{use: tests.oracle.check, id: check}}]\n"
        "required_evidence: [{evidence_id: tests.path, source: info, min_count: 2}]\n"
    )
    registrations = (
        ComponentRegistration(f"tests.{role}.source", role, Source, declares=declares),
        ComponentRegistration("tests.oracle.check", "oracle", Check, declares=Declarations()),
    )
    plugin = PluginManifest(plugin_id="tests.missing-evidence", registrations=registrations, experiments=(experiment,))
    outcome = run_experiment(plugin, "tests.missing", artifact_dir=str(tmp_path))
    assert outcome.status is status and outcome.summary.startswith(summary), outcome.summary
    if status is OutcomeStatus.ERROR:
        assert outcome.diagnostics["unestablished_required_evidence"][0]["evidence_id"] == "tests.path"


def test_a_private_server_that_writes_more_than_a_pipe_holds_still_exits(tmp_path):
    from valkey_fuzzer_experiments.server import PrivateServer

    chatty = tmp_path / "chatty-server"
    chatty.write_text("#!/bin/sh\nhead -c 1048576 /dev/zero | tr '\\0' x\necho 'FATAL CONFIG FILE ERROR'\nexit 1\n")
    chatty.chmod(0o755)
    server = PrivateServer(str(chatty), timeout_s=5)
    try:
        output = server.start()
    finally:
        server.stop()
    assert output is not None and output.endswith("FATAL CONFIG FILE ERROR")
