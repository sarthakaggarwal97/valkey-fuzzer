"""The plugin's faults and state-changing actions own each change with an undo obligation until it is undone, and a
recovery that could not undo a change does not pass."""

import os
import signal
import subprocess
from types import SimpleNamespace

import pytest

from tests.valkey_experiments_support import fake_context, registration_for, resolved, unit_plan
from valkey_fuzzer.contracts import ComponentRole, ComponentSpec, ComponentStatus, Evidence
from valkey_fuzzer.extensions import ConfigError
from valkey_fuzzer_experiments import capabilities, network, providers, storage
from valkey_fuzzer_experiments.faults import FAULT_INJECTED, CommandFault, ProcessSignalFault
from valkey_fuzzer_experiments.ledger import fault_records


class Iptables:
    """Records iptables calls; ``fail`` maps (verb, nth call of that verb) to an error text or an exception."""

    def __init__(self, fail=None):
        self.calls, self.fail, self.chains, self.jumps = [], dict(fail or {}), set(), []

    def __call__(self, *args):
        self.calls.append(args)
        verb, ok = args[0], SimpleNamespace(returncode=0, stdout="", stderr="")
        error = self.fail.get((verb, sum(1 for call in self.calls if call[0] == verb)))
        if isinstance(error, BaseException):
            raise error
        if error is not None:
            return SimpleNamespace(returncode=1, stdout="", stderr=error)
        if verb in ("-S", "-C"):
            ok.returncode = 0 if args[-1] in (self.chains if verb == "-S" else self.jumps) else 1
        elif verb in ("-N", "-X"):
            (self.chains.add if verb == "-N" else self.chains.discard)(args[1])
        elif verb in ("-I", "-D"):
            (self.jumps.append if verb == "-I" else self.jumps.remove)(args[-1])
        return ok


class _Handle:
    """valkey-0 (the primary) to valkey-2 on their own addresses; restarts and resumes fail as many times as set."""

    def __init__(self, root=""):
        self.instances = tuple(SimpleNamespace(instance_id=f"valkey-{n}", host=f"127.0.0.{n + 2}") for n in range(3))
        self.root, self.alive, self.signals, self.commands = root, True, [], []
        self.fail_restarts = self.fail_resumes = 0

    def primaries(self):
        return ["valkey-0"]

    def replicas(self):
        return ["valkey-1", "valkey-2"]

    def pid(self, instance_id):
        return 4242

    def is_alive(self, instance_id):
        return self.alive

    def send_signal(self, instance_id, signum):
        if signum == signal.SIGCONT and self.fail_resumes:
            self.fail_resumes -= 1
            raise OSError("resume refused")
        self.signals.append(signum)
        self.alive = self.alive and signum not in (signal.SIGKILL, signal.SIGTERM)

    def wait_exit(self, instance_id, timeout=10.0):
        return True

    def restart(self, instance_id, wait_ready=True, ready_timeout=30.0):
        if self.fail_restarts:
            self.fail_restarts -= 1
            raise RuntimeError("did not become ready")
        self.alive = True

    def started_at(self, instance_id):
        return 0.0

    def data_dir(self, instance_id):
        return os.path.join(self.root, instance_id)

    def new_client(self, instance_id, timeout=5.0):
        return _Client(self.commands)


class _Client:
    def __init__(self, commands):
        self.commands = commands

    def execute_command(self, *args):
        self.commands.append(args)
        return "OK"

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        pass


def _owned(context):
    """(owner, kind, who undid it) of every change the run's obligations recorded."""
    report = context.obligations.report()
    opened = sorted(report["undone"] + report["still_active"], key=lambda item: item["number"])
    return [(item["owner"], item["kind"], item.get("undone_by")) for item in opened]


def _fire(fault, handle, use, component_id="fault", **config):
    context = fake_context(handle)
    spec = resolved(
        ComponentSpec(role=ComponentRole.FAULT, use=use, id=component_id, config={"when_event": "go", **config})
    )
    fault.inject(context, spec)
    context.event_bus.publish("go", source="test")
    return context, spec, fault


@pytest.mark.parametrize(
    ("make", "use", "config", "kind"),
    [
        (ProcessSignalFault, "valkey.fault.process-signal", {"signal": "SIGKILL"}, "process.stopped"),
        (ProcessSignalFault, "valkey.fault.process-signal", {"signal": "SIGSTOP", "pause_s": 0.0}, "process.paused"),
        (CommandFault, "valkey.fault.command",
         {"command": ["CONFIG", "SET", "maxmemory", "1mb"], "undo": ["CONFIG", "SET", "maxmemory", "0"]}, "server.command"),
        (storage.DenyWritesFault, "valkey.fault.deny-writes", {"instances": "primaries"}, "directory.read-only"),
        (network.PartitionFault, "valkey.fault.network-partition", {"isolate": "valkey-0"}, "network.partition"),
    ],
    ids=["kill", "pause", "command", "deny-writes", "partition"],
)  # fmt: skip
def test_a_fault_owns_its_change_until_its_recovery_undoes_it(monkeypatch, tmp_path, make, use, config, kind):
    iptables = Iptables()
    monkeypatch.setattr(network, "_iptables", iptables)
    handle = _Handle(root=str(tmp_path))
    os.makedirs(handle.data_dir("valkey-0"))
    os.chmod(handle.data_dir("valkey-0"), 0o750)
    context, spec, fault = _fire(make(), handle, use, **config)
    assert [item[:2] for item in _owned(context)] == [("fault", kind)]
    if make is network.PartitionFault:
        # Both directions between the isolated instance and each peer, in a private chain removed once.
        drops = [call[1:] for call in iptables.calls if call[0] == "-A"]
        assert len(drops) == 4 and (fault._chain, "-s", "127.0.0.3", "-d", "127.0.0.2", "-j", "DROP") in drops
        fault._heal("event")
        fault._heal("event")
        assert sum(call[0] == "-X" for call in iptables.calls) == 1
    assert fault.recover(context, spec).status is ComponentStatus.PASS
    assert _owned(context) == [("fault", kind, "owner")]
    assert handle.alive and os.stat(handle.data_dir("valkey-0")).st_mode & 0o777 == 0o750 and not iptables.chains
    if "undo" in config:
        assert handle.commands[-1] == tuple(config["undo"])


def _paused_without_resume(monkeypatch, tmp_path):
    handle = _Handle()
    handle.fail_resumes = 2
    return _fire(ProcessSignalFault(), handle, "valkey.fault.process-signal", signal="SIGSTOP", pause_s=0)


def _restart_never_ready(monkeypatch, tmp_path):
    handle = _Handle()
    handle.fail_restarts = 5
    return _fire(
        ProcessSignalFault(), handle, "valkey.fault.process-signal", signal="SIGKILL", restart_mode="immediate"
    )


def _partition(fail, heal=None):
    """A partition whose iptables calls fail as ``fail`` says (``"hung"``: every call times out), healed by ``heal``."""

    def setup(monkeypatch, tmp_path):
        if fail == "hung":

            def hung(command, timeout=None, **kwargs):
                assert timeout and timeout <= 60, "every iptables call is bounded"
                raise subprocess.TimeoutExpired(command, timeout)

            monkeypatch.setattr(subprocess, "run", hung)
        monkeypatch.setattr(network, "_iptables", network._iptables if fail == "hung" else Iptables(fail))
        context, spec, fault = _fire(
            network.PartitionFault(), _Handle(), "valkey.fault.network-partition", isolate="valkey-0", heal_event="heal"
        )
        if heal:
            context.event_bus.publish("heal", source="test")  # a failed heal must not raise into the dispatch
        return context, spec, fault

    return setup


def _cancelled_command(monkeypatch, tmp_path):
    handle = _Handle()
    context = fake_context(handle)
    context.cancel()
    spec = resolved(
        ComponentSpec(
            role=ComponentRole.FAULT,
            use="valkey.fault.command",
            id="fault",
            config={"when_event": "go", "command": ["PING"], "delay_s": 30},
        )
    )
    fault = CommandFault()
    fault.inject(context, spec)
    context.event_bus.publish("go", source="test")
    assert handle.commands == []
    return context, spec, fault


def _cancelled_fill(monkeypatch, tmp_path):
    monkeypatch.setattr(storage, "_tmpfs_limit", lambda path: 8 * 1024 * 1024)
    statvfs, calls = storage.os.statvfs, []
    handle = SimpleNamespace(instances=())
    context = fake_context(handle)

    def cancelling_statvfs(path):
        calls.append(path)
        if len(calls) > 2:
            context.cancel()
        return statvfs(path)

    monkeypatch.setattr(storage.os, "statvfs", cancelling_statvfs)
    spec = resolved(
        ComponentSpec(
            role=ComponentRole.FAULT,
            use="valkey.fault.disk-fill",
            id="fill",
            config={"when_event": "go", "path": str(tmp_path), "chunk_bytes": 1024 * 1024},
        )
    )
    fault = storage.FillFilesystemFault()
    fault.inject(context, spec)
    context.event_bus.publish("go", source="test")
    assert 0 < fault._record["bytes_written"] < fault._record["max_bytes"]
    return context, spec, fault


_BUSY = "Device or resource busy"


@pytest.mark.parametrize(
    ("setup", "status", "summary", "left_open"),
    [
        (_paused_without_resume, "ERROR", "fault recovery failed", 1),
        (_restart_never_ready, "ERROR", "did not become ready", 1),
        (_partition({("-A", 2): OSError("iptables vanished")}), "", "iptables vanished", 0),
        (_partition({("-A", 2): "Bad rule", ("-X", 1): _BUSY}), "", "", 0),
        (_partition({("-I", 1): "no INPUT", ("-X", 1): _BUSY, ("-X", 2): _BUSY}), "ERROR", _BUSY, 1),
        (_partition({("-X", 1): _BUSY}, heal=True), "ERROR", "heal failed", 0),
        (_partition("hung"), "", "timed out", 0),
        (_cancelled_command, "", "cancelled", 0),
        (_cancelled_fill, "", "cancelled", 0),
    ],
    ids=[
        "pause-not-resumed",
        "restart-never-ready",
        "partition-fails-partway",
        "partition-cleanup-retried",
        "partition-chain-stays",
        "partition-heal-fails",
        "iptables-hangs",
        "cancelled-command",
        "cancelled-fill",
    ],
)
def test_a_fault_that_could_not_be_applied_or_undone_does_not_pass(
    monkeypatch, tmp_path, setup, status, summary, left_open
):
    context, spec, fault = setup(monkeypatch, tmp_path)
    result = fault.recover(context, spec)
    assert result.status is not ComponentStatus.PASS and summary in result.summary, result.summary
    assert not status or result.status.value == status
    assert len(context.obligations.still_open()) == left_open
    if left_open:
        # What the fault left open, the runner's sweep undoes once the instance cooperates.
        [left] = context.obligations.still_open()
        assert left.errors
    assert not list(tmp_path.iterdir()), "a cancelled fill removes its ballast"


def test_a_busy_tmpfs_stays_owned_until_it_is_unmounted(monkeypatch, tmp_path):
    mounted, busy = set(), [10]

    def run(command, **kwargs):
        assert kwargs.get("timeout"), "every mount call is bounded"
        if command[0] == "umount" and busy[0]:
            busy[0] -= 1
            if kwargs.get("check"):
                raise subprocess.CalledProcessError(32, command, stderr="target is busy")
            return subprocess.CompletedProcess(command, 32, "", "umount: target is busy")
        (mounted.add if command[0] == "mount" else mounted.discard)(command[-1])
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(providers.os.path, "ismount", lambda path: path in mounted)
    monkeypatch.setattr(providers, "_UMOUNT_RETRY_DELAYS_S", (0, 0))
    monkeypatch.setattr(providers, "mount_parent", lambda: str(tmp_path))
    monkeypatch.setattr(providers.tempfile, "mkdtemp", lambda prefix, dir: str(tmp_path / "tmpfs"))
    (tmp_path / "tmpfs").mkdir()
    provider, spec = (
        providers.TmpfsProvider(),
        SimpleNamespace(resource_id="disk", provider_id=providers.TMPFS_PROVIDER),
    )
    resource = provider.provision(None, spec, {"size_mb": 1, "prefix": "x-"})
    path = resource.outputs["path"]
    with pytest.raises(Exception, match="busy"):
        provider.cleanup(None, spec, resource)
    assert path in mounted and providers.provisioned_tmpfs_bytes(path) == 1024 * 1024
    busy[0] = 0
    provider.cleanup(None, spec, resource)
    assert not mounted and providers.provisioned_tmpfs_bytes(path) is None and not (tmp_path / "tmpfs").exists()


def test_fault_records_span_each_fault_until_its_confirmed_exit_or_heal_and_carry_its_targets():
    evidence = [
        Evidence(FAULT_INJECTED, "split", "p", 1.0, {"applied_at": 100.0, "healed_at": 105.0, "targets": ["valkey-0"]}),
        Evidence(FAULT_INJECTED, "kill", "p", 1.0, {"applied_at": 50.0, "target": "valkey-3"}),
        # The killed process acknowledged writes until 50.4, after the signal at 50.0.
        Evidence(FAULT_INJECTED, "kill2", "p", 1.0, {"applied_at": 50.0, "exited_at": 50.4, "target": "valkey-4"}),
    ]
    assert fault_records(evidence, 1.0, None, 2.0) == [
        (99.0, 107.0, ["valkey-0"]),
        (49.0, 52.0, ["valkey-3"]),
        (49.0, 52.4, ["valkey-4"]),
    ]
    assert [(start, end) for start, end, _ in fault_records(evidence, 0.0, ["split"])] == [(100.0, 105.0)]


def test_an_event_targeted_kill_records_the_cluster_vars_it_left_on_disk_and_needs_a_known_instance(tmp_path):
    handle = _Handle(root=str(tmp_path))
    os.makedirs(handle.data_dir("valkey-2"))
    (tmp_path / "valkey-2" / "nodes.conf").write_text(
        f"{'2' * 40} 127.0.0.1:7002@17002 myself,master - 0 0 3 connected 0-100\nvars currentEpoch 8 lastVoteEpoch 6\n"
    )
    for target, status in (("valkey-2", ComponentStatus.PASS), ("valkey-9", ComponentStatus.FAIL)):
        context = fake_context(handle)
        config = {"when_event": "vote", "instance": "event", "restart_mode": "background", "capture_cluster_vars": True}
        spec = resolved(
            ComponentSpec(role=ComponentRole.FAULT, use="valkey.fault.process-signal", id="f", config=config)
        )
        fault = ProcessSignalFault()
        fault.inject(context, spec)
        context.event_bus.publish("vote", source="t", payload={"instance_id": target, "epoch": 9})
        result = fault.recover(context, spec)
        assert result.status is status, result.summary
        if status is ComponentStatus.PASS:
            record = next(item.value for item in result.evidence if item.evidence_id == FAULT_INJECTED)
            assert record["target"] == "valkey-2" and record["disk_vars_at_kill"] == {
                "currentEpoch": 8,
                "lastVoteEpoch": 6,
            }
            assert record["restarted"] is True
    assert "names no known instance" in result.summary


_LOOPBACK = {"addressing": "per_instance_loopback"}
_CC, _RC = "valkey.action.cluster-chaos", "valkey.action.replication-chaos"
_SHARDS = {"primaries": 3, "replicas_per_primary": 1}  # cluster chaos waves
_PAIR = {"replicas_per_primary": 2}  # replication chaos waves
_FO0 = {"op": "failover", "shard": 0}


@pytest.mark.parametrize(
    ("use", "role", "config", "mode", "document", "message"),
    [
        ("valkey.fault.command", "fault", {"command": ["config", "set", "x", "1"]}, "standalone", {},
         "CONFIG SET needs an undo command"),
        ("valkey.fault.command", "fault", {"command": ["PING"], "occurrence": 0}, "standalone", {}, "occurrence must be at least 1"),
        ("valkey.fault.command", "fault", {"command": ["PING"], "when_payload": {"epoch": {"between": 1}}},
         "standalone", {}, "epoch needs one predicate"),
        ("valkey.fault.command", "fault", {"command": ["PING"], "when_payload": {"instance_id": []}}, "standalone", {},
         "must be a non-empty list"),
        ("valkey.fault.process-signal", "fault", {"capture_cluster_vars": True}, "standalone", {}, "needs a cluster"),
        ("valkey.fault.network-partition", "fault", {"isolate": "valkey-0"}, "cluster", {}, "per_instance_loopback"),
        ("valkey.fault.network-partition", "fault", {"isolate": "valkey-0", "heal_event": "x", "duration_s": 1},
         "cluster", _LOOPBACK, "not both"),
        ("valkey.observer.log-trigger", "observer", {"rules": []}, "standalone", {}, "at least one"),
        ("valkey.observer.log-trigger", "observer", {"rules": [{"pattern": "(", "publish": "x"}]}, "standalone", {}, "not a valid regex"),
        ("valkey.observer.log-trigger", "observer", {"rules": [{"pattern": "a", "publish": "bad name"}]}, "standalone",
         {}, "must be an event name"),
        ("valkey.observer.log-trigger", "observer", {"rules": [{"pattern": "(?P<instance_id>a)", "publish": "x"}]},
         "standalone", {}, "reserved groups"),
        ("valkey.observer.log-trigger", "observer", {"rules": [{"pattern": "a", "publish": "x", "evidence_id": "flat"}]},
         "standalone", {}, "namespaced"),
        (_CC, "action", {"waves": []}, "cluster", _SHARDS, "at least one wave"),
        (_CC, "action", {"waves": [[{"op": "failover", "shard": 3}]]}, "cluster", _SHARDS, "not below primaries 3"),
        (_CC, "action", {"waves": [[_FO0, _FO0]]}, "cluster", _SHARDS, "already fails over in this wave"),
        (_CC, "action", {"waves": [[{"op": "kill", "target": "valkey-6"}]]}, "cluster", _SHARDS, "not a planned instance"),
        (_CC, "action", {"waves": [[{"op": "kill", "target": []}]]}, "cluster", _SHARDS, "names no instance"),
        (_CC, "action", {"waves": [[{"op": "kill", "after": 0}]]}, "cluster", _SHARDS, "earlier operation"),
        (_CC, "action", {"waves": [[_FO0]], "validation": {"replication_config": {"lag": 1}}}, "cluster", _SHARDS,
         "replication_config contains unknown fields"),
        (_CC, "action", {"waves": [[_FO0]]}, "cluster", {"replicas_per_primary": 0}, "replicas_per_primary >= 1"),
        (_RC, "action", {"waves": [[]]}, "standalone", _PAIR, "has no operations"),
        (_RC, "action", {"waves": [[_FO0]]}, "standalone", _PAIR, r"op must be one of \['kill', 'restart'"),
        (_RC, "action", {"waves": [[{"op": "restart"}]]}, "standalone", _PAIR, "needs a kill with a smaller delay_s"),
        (_RC, "action", {"waves": [[{"op": "promote", "target": "primary"}]]}, "standalone", _PAIR, "must be one of"),
        (_RC, "action", {"waves": [[{"op": "repoint", "target": "valkey-1", "to": "valkey-1"}]]}, "standalone", _PAIR,
         "cannot replicate itself"),
        (_RC, "action", {"waves": [[{"op": "repoint", "target": "replica", "to": "replica"}]]}, "standalone",
         {"replicas_per_primary": 1}, "replicas_per_primary >= 2"),
        (_RC, "action", {"waves": [[{"op": "partition", "target": "replica"}]]}, "standalone", _PAIR, "per_instance_loopback"),
        ("valkey.action.rolling-upgrade", "action", {"to_binary": "/x"}, "standalone", {}, "replicas_per_primary >= 1"),
    ],
)  # fmt: skip
def test_bad_component_configs_fail_to_compile(use, role, config, mode, document, message):
    if role == "fault":
        config = {"when_event": "go", **config}
    spec = ComponentSpec(role=ComponentRole(role), use=use, id="subject", config=config)
    plan = unit_plan((spec,), deployment_mode=mode, document={"mode": mode, **document})
    with pytest.raises(ConfigError, match=message):
        registration_for(use).resolve_config(spec, plan)


def test_the_host_probe_skips_a_run_whose_partitions_cannot_work_here(monkeypatch):
    monkeypatch.setattr(capabilities, "iptables_supported", lambda: (False, "iptables is not installed"))
    config = {"requires": ["iptables"]}
    spec = resolved(
        ComponentSpec(role=ComponentRole.CAPABILITY, use="valkey.capability.host-probe", id="h", config=config)
    )
    result = capabilities.HostCapabilityProbe().probe(None, spec)
    assert result.status is ComponentStatus.SKIP and "iptables is not installed" in result.summary
    monkeypatch.setattr(capabilities, "iptables_supported", lambda: (True, ""))
    result = capabilities.HostCapabilityProbe().probe(None, spec)
    assert result.status is ComponentStatus.PASS and result.evidence[0].evidence_id == "valkey.host.supported"
    with pytest.raises(ValueError, match="share an address"):
        handle = _Handle()
        handle.instances = tuple(SimpleNamespace(instance_id=f"valkey-{n}", host="127.0.0.1") for n in range(3))
        _fire(network.PartitionFault(), handle, "valkey.fault.network-partition", isolate="valkey-0")
