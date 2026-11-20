"""The Valkey faults and state-changing actions own each change with an undo obligation until it is undone, and a
recovery that could not undo a change does not pass."""

import os
import signal
import subprocess
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from tests.fakes import FakeClient, FakeHandle, fake_instances
from tests.support import VirtualClock
from tests.valkey_experiments_support import fake_context, registration_for, resolved, unit_plan
from valkey_fuzzer import hoststate
from valkey_fuzzer.components import capabilities, network, processes, providers, storage
from valkey_fuzzer.components.faults import FAULT_INJECTED, HeldFault, ProcessSignalFault
from valkey_fuzzer.components.ledger import fault_records
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.results import ComponentStatus, Evidence
from valkey_fuzzer.schema import ConfigError


class Iptables:
    """Records iptables calls; ``fail`` maps (verb, nth call of that verb) to an error text or an exception. A
    ``restore`` (``iptables-restore``'s stdin) runs its rule lines as calls, so the verbs count across both."""

    def __init__(self, fail=None):
        self.calls, self.fail, self.chains, self.jumps = [], dict(fail or {}), set(), []
        self.restores = []  # every iptables-restore stdin, as given

    def install(self, monkeypatch):
        """Route the network module's iptables and iptables-restore calls here."""

        monkeypatch.setattr(network, "run_iptables", self)
        monkeypatch.setattr(network, "run_iptables_restore", self.restore)
        return self

    def restore(self, text):
        """``iptables-restore -w -n``'s stdin: the filter table's rules between ``*filter`` and ``COMMIT``, run in
        order up to the first that fails, which the stderr names by line as the legacy tool does."""

        self.restores.append(text)
        lines = text.splitlines()
        assert text.endswith("\n") and lines[0] == "*filter" and lines[-1] == "COMMIT", text
        for number, line in enumerate(lines[1:-1], start=2):
            assert line.split()[0] in ("-A", "-I"), line
            result = self(*line.split())
            if result.returncode != 0:
                error = f"{result.stderr}\niptables-restore: line {number} failed"
                return SimpleNamespace(returncode=1, stdout="", stderr=error)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

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


class Tc:
    """Records ``tc`` and ``ip`` calls and keeps the devices, lo's ingress qdisc, and the filters they make.

    ``fail`` maps (``"tc filter add"``, nth call of that verb) to an error text or an exception, as ``Iptables``
    does; ``ingress`` is whether lo already has an ingress qdisc; ``taken`` are prefs another run's filters hold.
    """

    def __init__(self, fail=None, *, ingress=False, taken=()):
        self.calls, self.fail, self.ingress = [], dict(fail or {}), ingress
        self.devices: dict = {}  # name -> its netem arguments, once a qdisc is on it
        self.filters: dict = {pref: [(1, "10.0.0.1", "10.0.0.2", "other0")] for pref in taken}
        self.batches = []  # every tc -batch stdin, as given

    def tc(self, *args):
        return self("tc", *args)

    def tc_batch(self, text):
        """``tc -force -batch -``'s stdin: one tc command per line, each run as a call and every line tried, as
        ``-force`` does; the stderr names each failed line after its own error, as the real tool's does."""

        self.batches.append(text)
        assert text.endswith("\n"), text
        stderr = ""
        for number, line in enumerate(text.splitlines(), start=1):
            result = self.tc(*line.split())
            if result.returncode != 0:
                stderr += f"{result.stderr}\nCommand failed -:{number}\n"
        return self._reply(1 if stderr else 0, stderr=stderr)

    def ip(self, *args):
        return self("ip", *args)

    def __call__(self, tool, *args):
        self.calls.append((tool, *args))
        words = [word for word in args if not word.startswith("-")]
        verb = " ".join((tool, *words[:2]))
        error = self.fail.get((verb, sum(1 for call in self.calls if call[0] == tool and call[1:3] == args[:2])))
        if isinstance(error, BaseException):
            raise error
        if error is not None:
            return self._reply(1, stderr=error)
        return getattr(self, "_" + verb.replace(" ", "_"))(words[2:])

    @staticmethod
    def _reply(returncode=0, stdout="", stderr=""):
        return SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)

    def _ip_link_add(self, words):  # ip link add dev NAME type ifb
        name = words[words.index("dev") + 1]
        if name in self.devices:
            return self._reply(2, stderr="RTNETLINK answers: File exists")
        self.devices[name] = None
        return self._reply()

    def _ip_link_set(self, words):
        return self._reply() if words[1] in self.devices else self._reply(1, stderr=f'Cannot find device "{words[1]}"')

    def _ip_link_del(self, words):
        if words[1] not in self.devices:
            return self._reply(1, stderr=f'Cannot find device "{words[1]}"')
        del self.devices[words[1]]
        return self._reply()

    def _ip_link_show(self, words):  # ip -o link show type ifb
        lines = [f"{n + 3}: {name}: <BROADCAST,NOARP,UP,LOWER_UP> mtu 1500 qdisc netem state UNKNOWN mode DEFAULT"
                 for n, name in enumerate(self.devices)]  # fmt: skip
        return self._reply(stdout="\n".join(lines) + ("\n" if lines else ""))

    def _tc_qdisc_add(self, words):  # tc qdisc add dev NAME root netem ...  |  tc qdisc add dev lo ingress
        name = words[words.index("dev") + 1]
        if name == "lo":
            assert words[-1] == "ingress", words
            if self.ingress:
                return self._reply(2, stderr="Error: Exclusivity flag on, cannot modify.")
            self.ingress = True
            return self._reply()
        assert name in self.devices and words[words.index("root") + 1] == "netem", words
        self.devices[name] = words[words.index("netem") + 1 :]
        return self._reply()

    def _tc_qdisc_del(self, words):  # tc qdisc del dev lo ingress
        assert words[words.index("dev") + 1] == "lo", words
        if not self.ingress:
            return self._reply(2, stderr="Error: Cannot find specified qdisc on specified device.")
        assert not self.filters, "lo's ingress qdisc was deleted with filters on it"
        self.ingress = False
        return self._reply()

    def _tc_qdisc_show(self, words):
        return self._reply(
            stdout="qdisc noqueue 0: root refcnt 2\n"
            + ("qdisc ingress ffff: parent ffff:fff1\n" if self.ingress else "")
        )

    def _tc_filter_add(self, words):
        assert words[:4] == ["dev", "lo", "ingress", "protocol"] and words[4] == "ip", words
        if not self.ingress:
            return self._reply(2, stderr="Error: Parent Qdisc doesn't exists.")
        pref, handle = int(words[words.index("pref") + 1]), int(words[words.index("handle") + 1])
        if any(held == handle for held, *_ in self.filters.get(pref, ())):
            return self._reply(2, stderr="RTNETLINK answers: File exists")
        assert words[words.index("flower") + 1 : words.index("action")] == [
            "ip_proto", "tcp", "src_ip", words[words.index("src_ip") + 1], "dst_ip", words[words.index("dst_ip") + 1]
        ], words  # fmt: skip
        assert words[words.index("action") + 1 :][:4] == ["mirred", "egress", "redirect", "dev"], words
        device = words[-1]
        assert device in self.devices, words
        self.filters.setdefault(pref, []).append(
            (handle, words[words.index("src_ip") + 1], words[words.index("dst_ip") + 1], device)
        )
        return self._reply()

    def _tc_filter_del(self, words):  # tc filter del dev lo ingress protocol ip pref P
        pref = int(words[words.index("pref") + 1])
        if pref not in self.filters:
            return self._reply(2, stderr="Error: Filter with specified priority/protocol not found.")
        del self.filters[pref]
        return self._reply()

    def _tc_filter_show(self, words):
        lines = []
        for pref, filters in sorted(self.filters.items()):
            lines.append(f"filter protocol ip pref {pref} flower chain 0 ")
            for handle, source, destination, device in filters:
                lines += [
                    f"filter protocol ip pref {pref} flower chain 0 handle 0x{handle:x} ",
                    "  eth_type ipv4", "  ip_proto tcp", f"  src_ip {source}", f"  dst_ip {destination}",
                    "  not_in_hw",
                    f"\taction order 1: mirred (Egress Redirect to device {device}) stolen",
                    " \tindex 1 ref 1 bind 1",
                ]  # fmt: skip
        return self._reply(stdout="\n".join(lines) + ("\n" if lines else ""))

    def install(self, monkeypatch, tmp_path):
        """Route the network module's tc and ip calls here, with the lock and the ingress marker in a directory
        beside ``tmp_path`` instead of the host's shared directory."""

        monkeypatch.setattr(network, "run_tc", self.tc)
        monkeypatch.setattr(network, "run_tc_batch", self.tc_batch)
        monkeypatch.setattr(network, "run_ip", self.ip)
        monkeypatch.setattr(hoststate, "default_root", lambda: str(tmp_path.parent / f"{tmp_path.name}-host"))
        if self.ingress:
            # Not the fuzzer's: no marker says it added the qdisc, so no fuzzer process removes it.
            assert not os.path.exists(hoststate._lo_ingress_marker())
        return self

    @property
    def marked(self):
        """Whether the marker says the fuzzer added lo's ingress qdisc."""

        return os.path.exists(hoststate._lo_ingress_marker())

    def flows(self):
        """``(source, destination, device)`` of every filter, in pref and handle order."""

        return [
            (source, destination, device)
            for pref in sorted(self.filters)
            for _, source, destination, device in sorted(self.filters[pref])
        ]


class _Handle(FakeHandle):
    """valkey-0 (the primary) to valkey-2 on their own addresses; restarts and resumes fail as many times as set."""

    def __init__(self, root=""):
        self.instances = fake_instances(3, host=lambda n: f"127.0.0.{n + 2}")
        self.root, self.alive, self.signals, self.commands = root, True, [], []
        self.fail_restarts = self.fail_resumes = 0

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
        return FakeClient(self.commands)


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
        (network.PartitionFault, "valkey.fault.network-partition", {"isolate": "valkey-0"}, "network.partition"),
        (network.DegradeFault, "valkey.fault.network-degrade",
         {"instances": "valkey-0", "loss_pct": 5, "delay_ms": 20, "jitter_ms": 5, "reorder_pct": 10}, "network.degrade"),
    ],
    ids=["kill", "pause", "partition", "degrade"],
)  # fmt: skip
def test_a_fault_owns_its_change_until_its_recovery_undoes_it(monkeypatch, tmp_path, make, use, config, kind):
    iptables = Iptables().install(monkeypatch)
    tc = Tc().install(monkeypatch, tmp_path)
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
        # Another process's leaked chains are the sweep's to find and report, not this fault's to delete.
        assert ("-S",) not in iptables.calls
    if make is network.DegradeFault:
        # Netem on a device of the fault's own, named after this process; one lo ingress filter per direction
        # between the instance and each peer, all at one pref, matching exactly the pair's addresses over TCP.
        [device] = tc.devices
        assert device == hoststate.netem_device_name(0) and len(device) <= 15
        assert tc.devices[device] == ["delay", "20ms", "5ms", "loss", "5%", "reorder", "10%"]
        assert sorted(tc.flows()) == sorted(
            (a, b, device) for a, b in (("127.0.0.2", "127.0.0.3"), ("127.0.0.3", "127.0.0.2"),
                                        ("127.0.0.2", "127.0.0.4"), ("127.0.0.4", "127.0.0.2"))
        )  # fmt: skip
        assert len(tc.filters) == 1 and tc.ingress and fault._record["pref"] in tc.filters
        assert fault._record["netem"] == "delay 20ms 5ms loss 5% reorder 10%" and fault._record["device"] == device
        fault._heal("event")
        fault._heal("event")
        # Removed in order: the filters, the ingress qdisc this fault added (now empty), the device; once.
        removals = [
            call[:3] for call in tc.calls if call[1:3] in (("filter", "del"), ("qdisc", "del"), ("link", "del"))
        ]
        assert removals == [("tc", "filter", "del"), ("tc", "qdisc", "del"), ("ip", "link", "del")]
        assert not tc.filters and not tc.ingress and not tc.devices
        # Another process's leaked devices are the sweep's to find, not this fault's to delete.
        assert all(call[1:3] != ("link", "show") for call in tc.calls)
    assert fault.recover(context, spec).status is ComponentStatus.PASS
    assert _owned(context) == [("fault", kind, "owner")]
    assert handle.alive and os.stat(handle.data_dir("valkey-0")).st_mode & 0o777 == 0o750 and not iptables.chains
    assert not tc.devices and not tc.filters


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
        else:
            Iptables(fail).install(monkeypatch)
        context, spec, fault = _fire(
            network.PartitionFault(), _Handle(), "valkey.fault.network-partition", isolate="valkey-0", heal_event="heal"
        )
        if heal:
            context.event_bus.publish("heal", source="test")  # a failed heal must not raise into the dispatch
        return context, spec, fault

    return setup


def _degrade(fail, heal=None, ingress=False):
    """A degradation whose tc or ip calls fail as ``fail`` says, on a host whose lo has an ``ingress`` qdisc or
    not, healed by ``heal``."""

    def setup(monkeypatch, tmp_path):
        tc = Tc(fail, ingress=ingress).install(monkeypatch, tmp_path)
        context, spec, fault = _fire(
            network.DegradeFault(), _Handle(), "valkey.fault.network-degrade", instances="valkey-0", loss_pct=5,
            heal_event="heal",
        )  # fmt: skip
        if heal:
            context.event_bus.publish("heal", source="test")  # a failed heal must not raise into the dispatch
        fault._tc, fault._shared_ingress = tc, ingress
        return context, spec, fault

    return setup


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
        # The second rule fails: the error names the restore's line (the first is ``*filter``) and the rule.
        (_partition({("-A", 2): "Bad rule"}), "", "iptables-restore -w -n (line 3: -A VF-", 0),
        (_partition({("-A", 2): "Bad rule", ("-X", 1): _BUSY}), "", "", 0),
        (_partition({("-I", 1): "no INPUT", ("-X", 1): _BUSY, ("-X", 2): _BUSY}), "ERROR", _BUSY, 1),
        (_partition({("-X", 1): _BUSY}, heal=True), "ERROR", "heal failed", 0),
        (_partition("hung"), "", "timed out", 0),
        (_cancelled_fill, "", "cancelled", 0),
        # The third filter (the batch's second line, after the claim) fails: the error names the batch's line and
        # its filter; the others, the ingress qdisc, and the device are removed again.
        (
            _degrade({("tc filter add", 3): "RTNETLINK answers: Invalid argument"}),
            "",
            "tc -force -batch - (line 2: filter add dev lo ingress protocol ip pref ",
            0,
        ),
        # The kernel lacks netem: the device is removed again, and the error says what is missing.
        (
            _degrade({("tc qdisc add", 1): "Error: Specified qdisc kind is unknown."}),
            "",
            "lacks a module: ifb, sch_netem, cls_flower, or act_mirred",
            0,
        ),
        (_degrade({("ip link add", 1): "RTNETLINK answers: Operation not permitted"}), "", "not permitted", 0),
        # The device cannot be deleted: its obligation stays open for the runner, with the filters gone.
        (_degrade({("ip link del", 1): "busy", ("ip link del", 2): "busy"}), "ERROR", "busy", 1),
        (_degrade({("tc filter del", 1): "busy"}, heal=True), "ERROR", "heal failed", 0),
        # An ingress qdisc lo already had is never removed, not even when the fault fails.
        (_degrade({("tc filter add", 2): "bad"}, ingress=True), "", "bad", 0),
    ],
    ids=[
        "pause-not-resumed",
        "restart-never-ready",
        "partition-fails-partway",
        "partition-rule-refused",
        "partition-cleanup-retried",
        "partition-chain-stays",
        "partition-heal-fails",
        "iptables-hangs",
        "cancelled-fill",
        "degrade-fails-partway",
        "degrade-no-netem",
        "degrade-no-device",
        "degrade-device-stays",
        "degrade-heal-fails",
        "degrade-fails-on-shared-ingress",
    ],
)
def test_a_fault_that_could_not_be_applied_or_undone_does_not_pass(
    monkeypatch, tmp_path, setup, status, summary, left_open
):
    context, spec, fault = setup(monkeypatch, tmp_path)
    result = fault.recover(context, spec)
    assert result.status is not ComponentStatus.PASS and summary in result.summary, result.summary
    assert not status or result.status.value == status
    still_active = context.obligations.report()["still_active"]
    assert len(still_active) == left_open
    if left_open:
        # What the fault left open, the runner's sweep undoes once the instance cooperates.
        [left] = still_active
        assert left["errors"]
    assert not list(tmp_path.iterdir()), "a cancelled fill removes its ballast"
    tc = getattr(fault, "_tc", None)
    if tc is not None:
        # Nothing of the fault's is left on lo: no filter, and the ingress qdisc only if lo had it before. A device
        # that could not be deleted is the one thing that stays, owned.
        assert not tc.filters and len(tc.devices) == left_open and tc.ingress is fault._shared_ingress


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
        SimpleNamespace(id="disk", provider_id=providers.TMPFS_PROVIDER),
    )
    resource = provider.acquire(None, spec, {"size_mb": 1, "prefix": "x-"})
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
    for target, status in (("valkey-2", ComponentStatus.PASS), ("valkey-9", ComponentStatus.ERROR)):
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


def test_an_interrupt_while_a_fault_fires_leaves_a_record_recovery_reads_at_once():
    # Ctrl-C inside the dispatch left ``_record`` at "firing": recovery waited 600 s, then said "could not be
    # applied: None" (F13).
    handle = _Handle()
    handle.send_signal = Mock(side_effect=KeyboardInterrupt())
    context = fake_context(handle)
    spec = resolved(
        ComponentSpec(
            role=ComponentRole.FAULT, use="valkey.fault.process-signal", id="fault", config={"when_event": "go"}
        )
    )
    fault = ProcessSignalFault()
    fault.inject(context, spec)
    with pytest.raises(KeyboardInterrupt):
        fault._on_event(SimpleNamespace(name="go", sequence=1, payload={}))

    started = time.monotonic()
    result = fault.recover(context, spec)
    assert time.monotonic() - started < 1.0
    assert result.status is ComponentStatus.ERROR
    assert "fault could not be applied: interrupted while firing: KeyboardInterrupt" in result.summary


def test_recovery_waits_for_a_firing_only_until_its_own_deadline(monkeypatch):
    handle = _Handle()
    context, spec, fault = _fire(ProcessSignalFault(), handle, "valkey.fault.process-signal", occurrence=2)
    fault._fire_done.clear()  # a firing that never ends
    context.deadline = time.monotonic() + 0.2
    started = time.monotonic()
    result = fault.recover(context, spec)
    assert 0.2 <= time.monotonic() - started < 2.0
    assert result.status is ComponentStatus.ERROR and "still firing when recovery timed out" in result.summary


@pytest.mark.parametrize("restart_mode", ["immediate", "recover"])
def test_a_kill_whose_target_never_exits_is_a_fault_error_and_announces_no_recovery(monkeypatch, restart_mode):
    clock = VirtualClock().install(monkeypatch, processes)
    handle = _Handle()
    handle.send_signal = lambda instance_id, signum: handle.signals.append(signum)

    def wait_exit(instance_id, timeout=10.0):
        clock.sleep(timeout)
        return False

    handle.wait_exit = wait_exit
    context, spec, fault = _fire(
        ProcessSignalFault(), handle, "valkey.fault.process-signal", signal="SIGKILL", restart_mode=restart_mode
    )
    result = fault.recover(context, spec)
    assert result.status is ComponentStatus.ERROR, result.summary
    assert "valkey-0 did not exit within 30s of SIGKILL" in result.summary
    assert not context.event_bus.committed_history("fault.recovered")


_LOOPBACK = {"addressing": "per_instance_loopback"}
_CC, _RC = "valkey.action.cluster-chaos", "valkey.action.replication-chaos"
_SHARDS = {"primaries": 3, "replicas_per_primary": 1}  # cluster chaos waves
_PAIR = {"replicas_per_primary": 2}  # replication chaos waves
_FO0 = {"op": "failover", "shard": 0}


@pytest.mark.parametrize(
    ("use", "role", "config", "mode", "document", "message"),
    [
        ("valkey.fault.process-signal", "fault", {"signal": "SIGKILL", "occurrence": 0}, "standalone", {}, "occurrence must be at least 1"),
        ("valkey.fault.process-signal", "fault", {"signal": "SIGKILL", "when_payload": {"epoch": {"between": 1}}},
         "standalone", {}, r"when_payload.epoch contains unknown fields: \['between'\]"),
        ("valkey.fault.process-signal", "fault", {"signal": "SIGKILL", "when_payload": {"epoch": {}}},
         "standalone", {}, "epoch needs one predicate"),
        ("valkey.fault.process-signal", "fault", {"signal": "SIGKILL", "when_payload": {"instance_id": []}}, "standalone", {},
         "must be a non-empty list"),
        # Checked as expect and predicate are: at run time these raised in the event dispatch, or matched characters.
        ("valkey.fault.process-signal", "fault", {"signal": "SIGKILL", "when_payload": {"epoch": {"min": "x"}}},
         "standalone", {}, "when_payload.epoch.min"),
        ("valkey.fault.process-signal", "fault", {"signal": "SIGKILL", "when_payload": {"role": {"in": "abc"}}},
         "standalone", {}, "when_payload.role.in"),
        ("valkey.fault.process-signal", "fault", {"capture_cluster_vars": True}, "standalone", {}, "needs a cluster"),
        ("valkey.fault.network-partition", "fault", {"isolate": "valkey-0"}, "cluster", {}, "per_instance_loopback"),
        ("valkey.fault.network-partition", "fault", {"isolate": "valkey-0", "heal_event": "x", "duration_s": 1},
         "cluster", _LOOPBACK, "not both"),
        ("valkey.fault.network-degrade", "fault", {"instances": "valkey-0", "loss_pct": 5}, "cluster", {}, "per_instance_loopback"),
        ("valkey.fault.network-degrade", "fault", {"instances": "valkey-0"}, "cluster", _LOOPBACK, "set at least one of loss_pct"),
        ("valkey.fault.network-degrade", "fault", {"instances": "valkey-0", "reorder_pct": 10}, "cluster", _LOOPBACK,
         "need delay_ms"),
        ("valkey.fault.network-degrade", "fault", {"instances": "valkey-0", "loss_pct": 101}, "cluster", _LOOPBACK, "loss_pct"),
        ("valkey.fault.network-degrade", "fault", {"instances": "valkey-0", "loss_pct": 5, "heal_event": "x", "duration_s": 1},
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
        (_RC, "action", {"waves": [[{"op": "restart"}]]}, "standalone", _PAIR, "needs a kill of the same wave"),
        # The kill waits for the stall, so it may start after the restart.
        (_RC, "action", {"waves": [[{"op": "stall", "duration_s": 5}, {"op": "kill", "after": 0},
                                    {"op": "restart", "delay_s": 1}]]}, "standalone", _PAIR,
         "needs a kill of the same wave that starts before it"),
        (_RC, "action", {"waves": [[{"op": "promote", "target": "primary"}]]}, "standalone", _PAIR, "must be one of"),
        (_RC, "action", {"waves": [[{"op": "repoint", "target": "valkey-1", "to": "valkey-1"}]]}, "standalone", _PAIR,
         "cannot replicate itself"),
        (_RC, "action", {"waves": [[{"op": "repoint", "target": "replica", "to": "replica"}]]}, "standalone",
         {"replicas_per_primary": 1}, "replicas_per_primary >= 2"),
        (_RC, "action", {"waves": [[{"op": "partition", "target": "replica"}]]}, "standalone", _PAIR, "per_instance_loopback"),
        (_RC, "action", {"waves": [[{"op": "degrade", "target": "replica", "loss_pct": 5}]]}, "standalone", _PAIR,
         "a degrade needs deployment.addressing: per_instance_loopback"),
        (_RC, "action", {"waves": [[{"op": "degrade", "target": "replica"}]]}, "standalone", {**_PAIR, **_LOOPBACK},
         r"waves\[0\]\[0\]: set at least one of"),
        (_RC, "action", {"waves": [[{"op": "degrade", "target": "replica", "jitter_ms": 5}]]}, "standalone",
         {**_PAIR, **_LOOPBACK}, "need delay_ms"),
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


@pytest.mark.parametrize(
    "wave",
    [
        [{"op": "kill"}, {"op": "restart", "after": 0}],
        [{"op": "kill", "delay_s": 0.5}, {"op": "stall"}, {"op": "restart", "after": 1, "delay_s": 1}],
    ],
    ids=["after-the-kill", "delayed-past-the-kill"],
)
def test_a_restart_that_starts_after_its_kill_compiles(wave):
    spec = ComponentSpec(role=ComponentRole.ACTION, use=_RC, id="subject", config={"waves": [wave]})
    plan = unit_plan((spec,), deployment_mode="standalone", document={"mode": "standalone", **_PAIR})
    registration_for(_RC).resolve_config(spec, plan)


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


def test_recovery_waits_for_a_background_restart_so_nothing_happens_after_it():
    handle = _Handle()
    restarts = []
    restart = handle.restart
    handle.restart = lambda instance_id, **options: restarts.append(time.monotonic()) or restart(instance_id, **options)
    started = time.monotonic()
    context, spec, fault = _fire(
        ProcessSignalFault(), handle, "valkey.fault.process-signal", restart_mode="background", restart_delay_s=0.2
    )
    result = fault.recover(context, spec)
    assert result.status is ComponentStatus.PASS, result.summary
    # Restarted once, after restart_delay_s, and announced before recovery returned.
    assert len(restarts) == 1 and restarts[0] - started >= 0.2
    assert [event.name for event in context.event_bus.committed_history("fault.recovered")] == ["fault.recovered"]


def test_a_degradation_takes_a_free_pref_and_leaves_lo_as_it_found_it(monkeypatch, tmp_path):
    # lo already has an ingress qdisc, and another run's filters hold the first two prefs this one tries.
    tc = Tc(ingress=True, taken=(10000, 10001)).install(monkeypatch, tmp_path)
    monkeypatch.setattr(network.secrets, "randbelow", lambda n: 0)
    context, spec, fault = _fire(
        network.DegradeFault(), _Handle(), "valkey.fault.network-degrade", instances="valkey-1", peers="valkey-0",
        delay_ms=50, rate_kbit=1000, duration_s=60,
    )  # fmt: skip
    assert fault._record["pref"] == 10002 and sorted(tc.filters) == [10000, 10001, 10002]
    assert [flow[:2] for flow in tc.flows() if flow[2] != "other0"] == [
        ("127.0.0.3", "127.0.0.2"), ("127.0.0.2", "127.0.0.3")
    ]  # fmt: skip
    [device] = tc.devices
    assert tc.devices[device] == ["delay", "50ms", "rate", "1000kbit"]
    assert fault.recover(context, spec).status is ComponentStatus.PASS
    # The other run's filters and the ingress qdisc stay; the fault's pref, filters, and device are gone.
    assert sorted(tc.filters) == [10000, 10001] and tc.ingress and not tc.devices
    assert all(call[1:3] != ("qdisc", "del") for call in tc.calls)


def test_two_degradations_of_one_process_get_their_own_devices(monkeypatch, tmp_path):
    tc = Tc().install(monkeypatch, tmp_path)
    handle = _Handle()
    first = _fire(network.DegradeFault(), handle, "valkey.fault.network-degrade", "a", instances="valkey-1", loss_pct=1)
    second = _fire(
        network.DegradeFault(), handle, "valkey.fault.network-degrade", "b", instances="valkey-2", delay_ms=5
    )
    assert sorted(tc.devices) == [hoststate.netem_device_name(0), hoststate.netem_device_name(1)]
    assert first[2]._record["pref"] != second[2]._record["pref"] and tc.marked
    # The first to recover leaves the ingress qdisc: the second's filters still hang off it.
    assert first[2].recover(*first[:2]).status is ComponentStatus.PASS
    assert tc.ingress and len(tc.filters) == 1 and len(tc.devices) == 1
    assert second[2].recover(*second[:2]).status is ComponentStatus.PASS
    assert not tc.devices and not tc.filters and not tc.ingress and not tc.marked


def _filter_line(pref, handle, source, destination, device):
    return (
        f"filter add dev lo ingress protocol ip pref {pref} handle {handle} flower ip_proto tcp"
        f" src_ip {source} dst_ip {destination} action mirred egress redirect dev {device}"
    )


@pytest.mark.parametrize(
    ("make", "use", "config", "spawned"),
    [
        # The device, up, its netem, lo's ingress qdisc, the claiming filter, the other nine filters in one batch.
        (network.DegradeFault, "valkey.fault.network-degrade", {"instances": "valkey-0", "loss_pct": 5},
         ["ip link add", "ip link set", "tc qdisc add", "tc qdisc add", "tc filter add", "tc -force -batch"]),
        # The chain, then its ten rules and the jump in one restore.
        (network.PartitionFault, "valkey.fault.network-partition", {"isolate": "valkey-0"},
         ["iptables -w -N", "iptables-restore -w -n"]),
    ],
    ids=["degrade", "partition"],
)  # fmt: skip
def test_an_install_against_five_peers_spawns_one_batch_for_its_filters_or_rules(
    monkeypatch, tmp_path, make, use, config, spawned
):
    tc, iptables, spawns = Tc(), Iptables(), []

    def run_bounded(command, **kwargs):
        # Every tc, ip, iptables, and iptables-restore process the install starts, with the batch tools' stdin.
        spawns.append(command)
        tool, *args = command
        if tool == "iptables-restore":
            return iptables.restore(kwargs["input"])
        if tool == "iptables":
            return iptables(*args[1:])  # after -w
        if args[:3] == ["-force", "-batch", "-"]:
            return tc.tc_batch(kwargs["input"])
        return tc(tool, *args)

    monkeypatch.setattr(hoststate, "run_bounded", run_bounded)
    monkeypatch.setattr(hoststate, "default_root", lambda: str(tmp_path / "host"))
    monkeypatch.setattr(network.secrets, "randbelow", lambda n: 0)
    handle = _Handle()
    handle.instances = fake_instances(6, host=lambda n: f"127.0.0.{n + 2}")
    context, spec, fault = _fire(make(), handle, use, **config)
    assert fault._record["applied"], fault._record
    assert [" ".join(command[:3]) for command in spawns] == spawned and len(spawns) == len(spawned)
    peers = [f"127.0.0.{n}" for n in range(3, 8)]
    flows = [flow for peer in peers for flow in (("127.0.0.2", peer), (peer, "127.0.0.2"))]
    if make is network.DegradeFault:
        device = hoststate.netem_device_name(0)
        assert spawns[4][1:] == _filter_line(10000, 1, *flows[0], device).split()
        lines = [_filter_line(10000, handle, source, destination, device) for handle, (source, destination) in
                 enumerate(flows[1:], start=2)]  # fmt: skip
        assert tc.batches == ["".join(f"{line}\n" for line in lines)]
        assert fault._record["rules"] == [f"{source}->{destination}" for source, destination in flows]
    else:
        rules = [f"-A {fault._chain} -s {source} -d {destination} -j DROP" for source, destination in flows]
        assert iptables.restores == [
            "".join(f"{line}\n" for line in ("*filter", *rules, f"-I INPUT 1 -j {fault._chain}", "COMMIT"))
        ]
    assert fault.recover(context, spec).status is ComponentStatus.PASS
    assert not tc.devices and not tc.filters and not iptables.chains


@pytest.mark.parametrize(
    ("addresses", "message"),
    [
        ({"a": "127.0.0.1", "b": "127.0.0.3"}, "harness's own address"),
        ({"a": "::1", "b": "127.0.0.3"}, "not an IPv4 address"),
        ({"a": "127.0.0.2", "b": "127.0.0.2"}, "share an address"),
        ({"a": "10.0.0.2", "b": "127.0.0.3"}, "not a loopback"),
    ],
)
def test_a_degradation_refuses_addresses_its_filters_could_not_scope_before_any_call(
    monkeypatch, tmp_path, addresses, message
):
    tc = Tc().install(monkeypatch, tmp_path)
    degradation = network.Degradation(addresses, ["a"], ["b"], ["loss", "1%"])
    with pytest.raises(network.DegradeError, match=message):
        degradation.create_device()
    assert tc.calls == []


@pytest.mark.parametrize(
    ("config", "arguments"),
    [
        ({"loss_pct": 5}, ["loss", "5%"]),
        ({"delay_ms": 100, "jitter_ms": 10, "reorder_pct": 25}, ["delay", "100ms", "10ms", "reorder", "25%"]),
        ({"delay_ms": 0.5, "duplicate_pct": 1, "rate_kbit": 512}, ["delay", "0.5ms", "duplicate", "1%", "rate", "512kbit"]),
    ],
)  # fmt: skip
def test_netem_arguments_say_only_what_the_config_sets(config, arguments):
    resolved_config = {**{name: field.default for name, field in network.DEGRADE_FIELDS.items()}, **config}
    assert network.netem_arguments(resolved_config) == arguments


def test_the_host_probe_skips_a_run_whose_degradations_cannot_work_here(monkeypatch):
    monkeypatch.setattr(capabilities, "netem_supported", lambda: (False, "tc not installed"))
    spec = resolved(
        ComponentSpec(role=ComponentRole.CAPABILITY, use="valkey.capability.host-probe", id="h", config={"requires": ["netem"]})
    )  # fmt: skip
    result = capabilities.HostCapabilityProbe().probe(None, spec)
    assert result.status is ComponentStatus.SKIP and "tc not installed" in result.summary
    monkeypatch.setattr(capabilities, "netem_supported", lambda: (True, ""))
    assert capabilities.HostCapabilityProbe().probe(None, spec).status is ComponentStatus.PASS


@pytest.mark.parametrize(
    ("which", "cap_eff", "tc_ok", "reason"),
    [
        ({"tc": None, "ip": "/sbin/ip"}, "0000003fffffffff", True, "tc not installed"),
        ({"tc": "/sbin/tc", "ip": None}, "0000003fffffffff", True, "ip not installed"),
        ({"tc": "/sbin/tc", "ip": "/sbin/ip"}, "0000000000000000", True, "requires CAP_NET_ADMIN"),
        ({"tc": "/sbin/tc", "ip": "/sbin/ip"}, "0000000000001000", False, "tc is not usable"),
        ({"tc": "/sbin/tc", "ip": "/sbin/ip"}, "0000000000001000", True, ""),
    ],
    ids=["no-tc", "no-ip", "no-cap-net-admin", "tc-unusable", "supported"],
)
def test_netem_support_needs_tc_ip_and_cap_net_admin_and_never_sudo(
    monkeypatch, tmp_path, which, cap_eff, tc_ok, reason
):
    monkeypatch.setattr(capabilities.providers, "on_linux", lambda: True)
    monkeypatch.setattr(capabilities.shutil, "which", lambda name: which.get(name))
    status = tmp_path / "status"
    status.write_text(f"Name:\tpython\nCapInh:\t0000000000000000\nCapEff:\t{cap_eff}\n")
    real_open = open
    monkeypatch.setattr(
        "builtins.open", lambda path, *a, **k: real_open(status if path == "/proc/self/status" else path, *a, **k)
    )
    calls = []

    def run_tc(*args):
        calls.append(args)
        return SimpleNamespace(returncode=0 if tc_ok else 2, stdout="", stderr="Cannot open netlink socket")

    monkeypatch.setattr(capabilities, "run_tc", run_tc)
    supported, why = capabilities.netem_supported()
    assert supported is (reason == "") and reason in why
    assert all(call[0] != "sudo" for call in calls) and (
        calls == [("qdisc", "show", "dev", "lo")] if cap_eff.endswith("1000") else calls == []
    )


def test_a_partition_timer_that_fires_once_recovery_began_does_nothing(monkeypatch):
    iptables = Iptables().install(monkeypatch)
    context, spec, fault = _fire(
        network.PartitionFault(), _Handle(), "valkey.fault.network-partition", isolate="valkey-0", duration_s=60
    )
    fault._quiesce()
    fault._heal("duration")
    assert context.event_bus.committed_history("fault.healed") == () and iptables.chains, "recovery removes it"
    assert fault.recover(context, spec).status is ComponentStatus.PASS and not iptables.chains


def test_a_held_change_is_undone_once_before_its_release_event_or_else_by_recovery_which_publishes_nothing():
    undone = []  # How many <id>.released events had been published each time the change was undone.

    class Held(HeldFault):
        seconds_field = "held_seconds"

        def _install(self, event):
            bus = self._context.event_bus
            undo = lambda: undone.append(len(bus.committed_history("held.released")))  # noqa: E731
            self._change = self._own("test.held", "valkey-0", undo)
            return {"target": "valkey-0"}

        def _released_payload(self, reason):
            return {"instance_id": "valkey-0", "reason": reason}

        def _release_failed(self, record, error):
            raise AssertionError(error)

    def held(**config):
        context = fake_context(_Handle())
        trigger = {"when_event": "go", "when_payload": {}, "occurrence": 1, "delay_s": 0.0, "release_event": None}
        spec = ComponentSpec(role=ComponentRole.FAULT, use="test.held", id="held", config={**trigger, **config})
        fault = Held()
        fault.inject(context, spec)
        context.event_bus.publish("go", source="test")
        return context, spec, fault

    def record_of(result):
        return next(item.value for item in result.evidence if item.evidence_id == FAULT_INJECTED)

    # Released by its event: the change is undone, then <id>.released says why, once; recovery undoes nothing more.
    context, spec, fault = held(release_event="let-go")
    context.event_bus.publish("let-go", source="test")
    context.event_bus.publish("let-go", source="test")
    (released,) = context.event_bus.committed_history("held.released")
    assert undone == [0] and released.payload == {"instance_id": "valkey-0", "reason": "event"}
    result = fault.recover(context, spec)
    assert result.status is ComponentStatus.PASS and undone == [0]
    assert _owned(context) == [("held", "test.held", "owner")]
    record = record_of(result)
    assert record["release_reason"] == "event" and record["applied_at"] <= record["released_at"]
    assert record["held_seconds"] == round(record["released_at"] - record["applied_at"], 3)
    # Without one: recovery closes the fault to late releases, undoes the change itself, and publishes nothing.
    undone.clear()
    context, spec, fault = held()
    fault._quiesce()
    fault._release("event")
    assert undone == []
    result = fault.recover(context, spec)
    assert result.status is ComponentStatus.PASS and undone == [0]
    assert _owned(context) == [("held", "test.held", "owner")]
    record = record_of(result)
    assert record["release_reason"] == "recover" and record["applied_at"] <= record["released_at"]
    assert context.event_bus.committed_history("held.released") == ()


def test_a_disk_fill_whose_ballast_stayed_does_not_record_it_removed(monkeypatch, tmp_path):
    monkeypatch.setattr(storage, "_tmpfs_limit", lambda path: 8192)
    monkeypatch.setattr(storage.os, "statvfs", lambda path: SimpleNamespace(f_bavail=0, f_frsize=4096))
    context, spec, fault = _fire(
        storage.FillFilesystemFault(), _Handle(), "valkey.fault.disk-fill", path=str(tmp_path), chunk_bytes=4096
    )
    monkeypatch.setattr(storage.os, "remove", lambda path: (_ for _ in ()).throw(OSError("device busy")))
    result = fault.recover(context, spec)
    record = next(item.value for item in result.evidence if item.evidence_id == FAULT_INJECTED)
    assert result.status is ComponentStatus.ERROR and record["ballast_removed"] is False
