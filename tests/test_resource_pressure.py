"""``valkey.fault.resource-pressure`` and the cgroup v2 subtree behind it, on a fake cgroup filesystem.

The fake is a directory tree under ``tmp_path`` shaped like ``/sys/fs/cgroup``: ``cgroup.controllers``,
``cgroup.subtree_control`` and ``cgroup.procs`` files, and a ``/proc`` whose ``self/cgroup`` names this process's
cgroup and whose ``<pid>`` entries belong to this user. Nothing touches the host's cgroups.
"""

import errno
import os
import shutil
from pathlib import Path

import pytest

from tests.fakes import FakeHandle, fake_instances
from tests.valkey_experiments_support import fake_context, registration_for, resolved, unit_plan
from valkey_fuzzer import cgroups, hoststate
from valkey_fuzzer.components import capabilities
from valkey_fuzzer.components.faults import FAULT_INJECTED
from valkey_fuzzer.components.resource_pressure import ResourcePressureFault, cpu_max, limits_for
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.results import ComponentStatus
from valkey_fuzzer.schema import ConfigError

REAL_DELEGATED = cgroups.delegated
OWN = "/user.slice/user-1000.slice/user@1000.service/app.slice/run-u7.scope"


class FakeCgroups:
    """A cgroup v2 tree under ``root``: this process's cgroup ``OWN`` holds ``pids`` (this process and a server),
    with ``controllers`` available to it; ``proc`` is the /proc beside it. :meth:`write`, :meth:`mkdir` and
    :meth:`rmdir` do the kernel's side: a PID written to ``cgroup.procs`` leaves the cgroup it was in, a
    controller written to ``cgroup.subtree_control`` is refused (EBUSY) while the cgroup has processes, a new
    cgroup gets its files, and a cgroup is removed with them."""

    def __init__(self, tmp_path, controllers="cpu memory pids", pids=(), own=OWN):
        self.root, self.proc = tmp_path / "cgroup", tmp_path / "proc"
        (self.proc / "self").mkdir(parents=True)
        (self.proc / "self" / "cgroup").write_text(f"0::{own}\n")
        self.root.mkdir()
        (self.root / "cgroup.controllers").write_text("cpuset cpu io memory pids\n")
        self.own = self.root / own.lstrip("/")
        self.own.mkdir(parents=True, exist_ok=True)
        (self.own / "cgroup.controllers").write_text(controllers + "\n")
        (self.own / "cgroup.subtree_control").write_text("\n")
        (self.own / "cgroup.procs").write_text("".join(f"{pid}\n" for pid in pids))
        for pid in pids:
            (self.proc / str(pid)).mkdir()

    def procs(self, path):
        return cgroups.procs(str(path))

    def cgroup_of(self, pid):
        """The cgroup directory holding ``pid``, searched under this process's cgroup."""

        for directory, _, files in os.walk(self.own):
            if "cgroup.procs" in files and pid in cgroups.procs(directory):
                return Path(directory)
        return None

    def write(self, path, text):
        path = Path(path)
        if path.name == "cgroup.procs":
            pid = int(text)
            source = self.cgroup_of(pid)
            if source is not None:
                remaining = [other for other in self.procs(source) if other != pid]
                (source / "cgroup.procs").write_text("".join(f"{other}\n" for other in remaining))
            with open(path, "a", encoding="utf-8") as handle:
                handle.write(f"{pid}\n")
        elif path.name == "cgroup.subtree_control":
            if self.procs(path.parent):
                raise OSError(errno.EBUSY, "Device or resource busy")
            enabled = set(path.read_text().split())
            for word in text.split():
                (enabled.add if word[0] == "+" else enabled.discard)(word[1:])
            path.write_text(" ".join(sorted(enabled, key=lambda name: text.find(name))))
        else:
            path.write_text(text)

    def mkdir(self, path, exist_ok=False):
        path = Path(path)
        if path.exists() and exist_ok:
            return
        path.mkdir()
        (path / "cgroup.procs").write_text("")
        (path / "cgroup.subtree_control").write_text("\n")
        (path / "cgroup.controllers").write_text((path.parent / "cgroup.subtree_control").read_text())

    def rmdir(self, path):
        if any(child.is_dir() for child in Path(path).iterdir()):
            raise OSError(errno.ENOTEMPTY, "Directory not empty")
        shutil.rmtree(path)


@pytest.fixture
def fake_cgroups(tmp_path, monkeypatch):
    """The fake tree, standing in for the kernel, with the real :func:`cgroups.delegated` reading it (the
    conftest stubs it out)."""

    def make(**options):
        fake = FakeCgroups(tmp_path, **options)
        monkeypatch.setattr(cgroups, "ROOT", str(fake.root))
        monkeypatch.setattr(cgroups, "_write", fake.write)
        monkeypatch.setattr(cgroups, "_mkdir", fake.mkdir)
        monkeypatch.setattr(cgroups, "_rmdir", fake.rmdir)
        monkeypatch.setattr(
            cgroups,
            "delegated",
            lambda controllers, root=str(fake.root), proc=str(fake.proc): REAL_DELEGATED(controllers, root, proc),
        )
        return fake

    return make


class _Handle(FakeHandle):
    def __init__(self, pid=4242):
        self.instances = fake_instances(2)
        self._pid = pid

    def pid(self, instance_id):
        return self._pid


def _fault(handle, **config):
    context = fake_context(handle)
    spec = resolved(
        ComponentSpec(
            role=ComponentRole.FAULT,
            use="valkey.fault.resource-pressure",
            id="starve",
            config={"when_event": "go", **config},
        )
    )
    fault = ResourcePressureFault()
    fault.inject(context, spec)
    return context, spec, fault


def _record(result):
    return next(item.value for item in result.evidence if item.evidence_id == FAULT_INJECTED)


def _write_stats(leaf, throttled=3, memory_max=0):
    (leaf / "cpu.stat").write_text(
        f"usage_usec 1000\nuser_usec 800\nsystem_usec 200\nnr_periods 50\nnr_throttled {throttled}\n"
        f"throttled_usec {throttled * 90000}\n"
    )
    (leaf / "memory.events").write_text(f"low 0\nhigh 0\nmax {memory_max}\noom 0\noom_kill 0\n")
    (leaf / "memory.current").write_text("20971520\n")


def test_cpu_max_is_a_quota_per_period_down_to_the_kernels_minimum_quota():
    assert cpu_max(100) == "100000 100000" and cpu_max(50) == "50000 100000" and cpu_max(1) == "1000 100000"
    # Below 1 ms per 100 ms the period grows to a second, so the quota stays at least the kernel's 1 ms.
    assert cpu_max(0.5) == "5000 1000000" and cpu_max(0.1) == "1000 1000000"
    assert limits_for({"cpu_percent": 2.0, "memory_max_mb": 64}) == {"cpu.max": "2000 100000", "memory.max": "67108864"}
    assert limits_for({"cpu_percent": None, "memory_max_mb": 1}) == {"memory.max": "1048576"}


def test_the_fault_limits_the_instance_in_a_leaf_of_the_runs_cgroup_and_releases_it(fake_cgroups):
    fake = fake_cgroups(pids=(os.getpid(), 4242))
    handle = _Handle(pid=4242)
    context, spec, fault = _fault(handle, instance="primary", cpu_percent=1.0, memory_max_mb=64, release_event="go-on")
    context.event_bus.publish("go", source="test")
    (injected,) = context.event_bus.committed_history("starve.injected")
    assert injected.payload["applied"] and injected.payload["target"] == "valkey-0"
    # Every process of the run's cgroup moved to the harness leaf, so the controllers could be handed down...
    harness = fake.own / cgroups.HARNESS
    assert fake.procs(fake.own) == [] and fake.procs(harness) == [os.getpid()]
    assert (fake.own / "cgroup.subtree_control").read_text() == "cpu memory"
    # ...and the server into a leaf of its own, named for this process, with the limits.
    leaf = fake.cgroup_of(4242)
    assert leaf.parent == fake.own and hoststate.cgroup_owner(leaf.name) == hoststate.current_owner()
    assert (leaf / "cpu.max").read_text() == "1000 100000" and (leaf / "memory.max").read_text() == "67108864"
    _write_stats(leaf, throttled=12, memory_max=2)
    context.event_bus.publish("go-on", source="test")
    assert not leaf.exists() and sorted(fake.procs(harness)) == sorted([os.getpid(), 4242])
    (released,) = context.event_bus.committed_history("starve.released")
    assert released.payload == {"instance_id": "valkey-0", "reason": "event"}
    result = fault.recover(context, spec)
    assert result.status is ComponentStatus.PASS, result.summary
    record = _record(result)
    assert record["cgroup"] == str(leaf) and record["limits"] == {"cpu.max": "1000 100000", "memory.max": "67108864"}
    assert record["stats"]["cpu.stat.nr_throttled"] == 12 and record["stats"]["memory.events.max"] == 2
    assert record["stats"]["memory.current"] == 20971520 and record["release_reason"] == "event"
    assert [item["kind"] for item in context.obligations.report()["undone"]] == ["cgroup.limited"]
    # The harness leaf and the enabled controllers stay: they are the cgroup's, not one fault's.
    assert harness.is_dir() and sorted(path.name for path in fake.own.iterdir() if path.is_dir()) == [cgroups.HARNESS]


def test_recovery_releases_a_limit_no_event_released_and_a_limit_that_never_bit_is_not_applied(fake_cgroups):
    fake = fake_cgroups(pids=(os.getpid(), 4242))
    context, spec, fault = _fault(_Handle(), cpu_percent=0.5)
    context.event_bus.publish("go", source="test")
    leaf = fake.cgroup_of(4242)
    _write_stats(leaf, throttled=0)
    result = fault.recover(context, spec)
    assert result.status is ComponentStatus.ERROR
    assert "cpu.max 5000 1000000 never throttled valkey-0" in result.summary
    assert not leaf.exists() and 4242 in fake.procs(fake.own / cgroups.HARNESS)
    assert not context.event_bus.committed_history("starve.released")


def test_a_memory_limit_that_was_never_reached_is_not_applied(fake_cgroups):
    fake = fake_cgroups(pids=(os.getpid(), 4242))
    context, spec, fault = _fault(_Handle(), memory_max_mb=512)
    context.event_bus.publish("go", source="test")
    leaf = fake.cgroup_of(4242)
    assert (fake.own / "cgroup.subtree_control").read_text() == "memory" and not (leaf / "cpu.max").exists()
    _write_stats(leaf, memory_max=0)
    result = fault.recover(context, spec)
    assert result.status is ComponentStatus.ERROR
    assert "memory.max 536870912 was never reached by valkey-0 (memory.current 20971520 bytes" in result.summary


def test_a_second_fault_reuses_the_harness_leaf_and_the_enabled_controllers(fake_cgroups):
    fake = fake_cgroups(pids=(os.getpid(), 4242, 4243))
    first = _fault(_Handle(pid=4242), cpu_percent=1.0)
    first[0].event_bus.publish("go", source="test")
    second = _fault(_Handle(pid=4243), cpu_percent=2.0)
    second[0].event_bus.publish("go", source="test")
    assert fake.cgroup_of(4242) != fake.cgroup_of(4243) and fake.procs(fake.own / cgroups.HARNESS) == [os.getpid()]
    assert (fake.own / "cgroup.subtree_control").read_text() == "cpu"
    for leaf in (fake.cgroup_of(4242), fake.cgroup_of(4243)):
        _write_stats(leaf)
    for context, spec, fault in (first, second):
        assert fault.recover(context, spec).status is ComponentStatus.PASS
    assert sorted(fake.procs(fake.own / cgroups.HARNESS)) == sorted([os.getpid(), 4242, 4243])


@pytest.mark.parametrize(
    ("options", "reason"),
    [
        ({"own": "/"}, "root cgroup"),
        ({"controllers": "memory pids"}, "lacks the cpu controller"),
        ({"pids": (os.getpid(), 1)}, "processes of other users"),
    ],
)
def test_a_cgroup_that_is_not_the_runs_own_is_refused_with_the_reason(fake_cgroups, monkeypatch, options, reason):
    fake = fake_cgroups(**options)
    if options.get("pids"):
        monkeypatch.setattr(cgroups, "_uid", lambda pid, proc: 0 if pid == 1 else os.geteuid())  # PID 1 is root's.
    supported, why = capabilities.cgroup_v2_supported(("cpu", "memory"))
    assert not supported and reason in why, why
    spec = resolved(
        ComponentSpec(
            role=ComponentRole.FAULT,
            use="valkey.fault.resource-pressure",
            id="f",
            config={"when_event": "x", "cpu_percent": 1},
        )
    )
    plan = unit_plan((spec,), document={"mode": "standalone"})
    with pytest.raises(RuntimeError, match=reason):
        registration_for("valkey.fault.resource-pressure").probe(spec.config, spec, plan)
    # Firing refuses too, and nothing under the cgroup changed.
    context, spec, fault = _fault(_Handle(), cpu_percent=1.0)
    context.event_bus.publish("go", source="test")
    result = fault.recover(context, spec)
    assert (
        result.status is ComponentStatus.ERROR
        and "fault refused to fire" in result.summary
        and reason in result.summary
    )
    assert [path.name for path in fake.own.iterdir() if path.is_dir()] == []


def test_without_cgroup_v2_or_a_writable_cgroup_the_reason_says_so(tmp_path, monkeypatch):
    monkeypatch.setattr(cgroups, "delegated", REAL_DELEGATED)
    directory, reason = cgroups.delegated(("cpu",), root=str(tmp_path / "none"), proc=str(tmp_path / "proc"))
    assert directory is None and "cgroup v2 is not mounted" in reason
    fake = FakeCgroups(tmp_path, pids=(os.getpid(),))
    (fake.proc / "self" / "cgroup").write_text("11:memory:/system.slice\n")
    assert cgroups.delegated(("cpu",), str(fake.root), str(fake.proc))[1] == "this process is in no cgroup v2 hierarchy"
    (fake.proc / "self" / "cgroup").write_text(f"0::{OWN}\n")
    assert cgroups.delegated(("cpu",), str(fake.root), str(fake.proc)) == (str(fake.own), "")
    fake.own.chmod(0o555)
    try:
        assert "not writable by uid" in cgroups.delegated(("cpu",), str(fake.root), str(fake.proc))[1]
    finally:
        fake.own.chmod(0o755)


def test_the_sweep_removes_the_leaves_of_a_fuzzer_that_died_and_leaves_live_ones(fake_cgroups, monkeypatch):
    fake = fake_cgroups(pids=(os.getpid(), 4242))
    cgroups.enter(str(fake.own), ("cpu",))
    dead = hoststate.Owner(999999, "abcdef")
    monkeypatch.setattr(hoststate.Owner, "alive", lambda owner: owner != dead)
    stale = cgroups.limit(str(fake.own), f"vf-{dead.token}-0123abcd", 4242, {"cpu.max": "1000 100000"})
    live = cgroups.limit(str(fake.own), hoststate.cgroup_name("mine"), os.getpid(), {"cpu.max": "1000 100000"})
    (fake.own / "not-ours").mkdir()
    monkeypatch.setattr(cgroups, "own_cgroup", lambda proc="/proc": OWN)
    monkeypatch.setattr(hoststate, "own_processes", lambda: [])
    monkeypatch.setattr(hoststate.platform, "system", lambda: "Darwin")
    report = hoststate.sweep(roots=[], dry_run=True)
    assert [(item.kind, item.target, item.action) for item in report.leftovers] == [("cgroup", stale, "would remove")]
    report = hoststate.sweep(roots=[])
    assert [(item.kind, item.action) for item in report.leftovers] == [("cgroup", "removed")] and report.ok
    assert not os.path.isdir(stale) and os.path.isdir(live) and (fake.own / "not-ours").is_dir()
    assert 4242 in fake.procs(fake.own / cgroups.HARNESS)
    # Nothing to find: the sweep's own kind of leaf of a live owner, and a cgroup v1 host.
    assert hoststate.sweep(roots=[]).leftovers == []
    monkeypatch.setattr(cgroups, "own_cgroup", lambda proc="/proc": None)
    assert hoststate.sweep(roots=[]).leftovers == []


@pytest.mark.parametrize(
    ("config", "message"),
    [
        ({}, "set cpu_percent, memory_max_mb, or both"),
        ({"cpu_percent": 0}, "cpu_percent"),
        ({"memory_max_mb": 0}, "memory_max_mb"),
        ({"cpu_percent": 1, "instance": "nobody"}, "instance"),
    ],
)
def test_bad_resource_pressure_configs_fail_to_compile(config, message):
    spec = ComponentSpec(
        role=ComponentRole.FAULT, use="valkey.fault.resource-pressure", id="f", config={"when_event": "x", **config}
    )
    plan = unit_plan((spec,), document={"mode": "standalone"})
    with pytest.raises(ConfigError, match=message):
        registration_for("valkey.fault.resource-pressure").resolve_config(spec, plan)
