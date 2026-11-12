"""The syscall fault library and ``valkey.fault.syscall``.

The library tests build ``syscall_faults.c`` with the host's C compiler and run small Python programs under it:
Python's ``os.write``, ``os.fsync``, ``os.fdatasync`` and ``os.replace`` are the libc calls the library wraps.
"""

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import textwrap
import time
from types import SimpleNamespace

import pytest

from tests.valkey_experiments_support import fake_context, registration_for, resolved, unit_plan
from valkey_fuzzer.components.faults import FAULT_INJECTED
from valkey_fuzzer.components.syscall_faults import SyscallFault, read_calls, rule_text
from valkey_fuzzer.deployment.local import syscall_faults as library
from valkey_fuzzer.deployment.local.config import deployment_config_from_document
from valkey_fuzzer.deployment.local.models import DeploymentConfig, LocalDeploymentError
from valkey_fuzzer.deployment.local.processes import ProcessManager
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.results import ComponentStatus
from valkey_fuzzer.schema import ConfigError

needs_cc = pytest.mark.skipif(not shutil.which(os.environ.get("CC") or "cc"), reason="needs a C compiler")


@pytest.fixture(scope="module")
def shim(tmp_path_factory):
    if not shutil.which(os.environ.get("CC") or "cc"):
        pytest.skip("needs a C compiler")
    return library.build_library(str(tmp_path_factory.mktemp("lib")))


def _arm(rules, name, text):
    os.makedirs(rules, exist_ok=True)
    with open(os.path.join(rules, f"{name}.tmp"), "w") as out:
        out.write(text + "\n")
    os.replace(os.path.join(rules, f"{name}.tmp"), os.path.join(rules, f"{name}.rule"))


def _run(shim, rules, program, **kwargs):
    env = library.server_environment(shim, str(rules))
    return subprocess.run(
        [sys.executable, "-I", "-c", textwrap.dedent(program)], env=env, capture_output=True, text=True, **kwargs
    )


def test_a_rule_fails_only_its_nth_matching_call_and_logs_each_one_up_to_it(shim, tmp_path):
    rules, target = tmp_path / "rules", tmp_path / "data" / "target.txt"
    target.parent.mkdir()
    _arm(rules, "f1", f"calls=write path={tmp_path}/data/*.txt nth=2 action=fail errno=28")
    program = f"""
        import os
        other = os.open({str(tmp_path / "other.log")!r}, os.O_WRONLY | os.O_CREAT)
        fd = os.open({str(target)!r}, os.O_WRONLY | os.O_CREAT)
        for i in range(3):
            os.write(other, b"x")
            try:
                os.write(fd, b"a")
                print("ok")
            except OSError as exc:
                print("errno", exc.errno)
    """
    run = _run(shim, rules, program)
    assert run.returncode == 0, run.stderr
    assert run.stdout.split("\n")[:3] == ["ok", "errno 28", "ok"]
    assert target.read_text() == "aa"
    calls = read_calls(str(rules / "f1.log"))
    assert [(call["n"], call["call"], call["action"]) for call in calls] == [(1, "write", "pass"), (2, "write", "fail")]
    assert calls[1]["path"] == str(target)
    assert not (rules / "f1.rule").exists() and (rules / "f1.fired").exists()


def test_a_kill_in_a_fork_child_takes_the_server_down_too(shim, tmp_path):
    rules = tmp_path / "rules"
    _arm(rules, "k", f"calls=fsync path={tmp_path}/temp-* nth=1 action=kill")
    program = f"""
        import os, time
        child = os.fork()
        if child == 0:
            fd = os.open({str(tmp_path / "temp-1.rdb")!r}, os.O_WRONLY | os.O_CREAT)
            os.write(fd, b"snapshot")
            os.fsync(fd)
            os._exit(0)
        time.sleep(30)
    """
    started = time.monotonic()
    run = _run(shim, rules, program, timeout=60)
    assert run.returncode == -signal.SIGKILL and time.monotonic() - started < 20
    (call,) = read_calls(str(rules / "k.log"))
    assert call["call"] == "fsync" and call["action"] == "kill"
    assert (tmp_path / "temp-1.rdb").read_bytes() == b"snapshot"


def test_a_rename_matches_either_path_and_a_delay_holds_the_call(shim, tmp_path):
    rules = tmp_path / "rules"
    _arm(rules, "r", f"calls=rename,fdatasync path={tmp_path}/dump.rdb nth=1 action=delay delay_ms=400 when=after")
    program = f"""
        import os, time
        open({str(tmp_path / "temp.rdb")!r}, "w").close()
        started = time.monotonic()
        os.replace({str(tmp_path / "temp.rdb")!r}, {str(tmp_path / "dump.rdb")!r})
        print(round(time.monotonic() - started, 3), os.path.exists({str(tmp_path / "dump.rdb")!r}))
    """
    run = _run(shim, rules, program)
    assert run.returncode == 0, run.stderr
    held, renamed = run.stdout.split()
    assert float(held) >= 0.4 and renamed == "True"
    (call,) = read_calls(str(rules / "r.log"))
    assert call["call"] == "rename" and call["to"] == str(tmp_path / "dump.rdb") and call["when"] == "after"


def test_a_peer_filter_matches_only_sockets_connected_to_its_ports(shim, tmp_path):
    rules = tmp_path / "rules"
    servers = [socket.create_server(("127.0.0.1", 0)) for _ in range(2)]
    ports = [server.getsockname()[1] for server in servers]
    _arm(rules, "p", f"calls=write peer_ports={ports[1]} nth=1 action=fail errno=32")
    program = f"""
        import os, socket
        for port in {ports!r}:
            conn = socket.create_connection(("127.0.0.1", port))
            try:
                os.write(conn.fileno(), b"ping")
                print("sent", port)
            except OSError as exc:
                print("errno", exc.errno)
    """
    run = _run(shim, rules, program)
    for server in servers:
        server.close()
    assert run.returncode == 0, run.stderr
    assert run.stdout.split("\n")[:2] == [f"sent {ports[0]}", "errno 32"]


def test_without_a_rule_directory_or_a_rule_every_call_goes_through(shim, tmp_path):
    program = f"""
        import os
        fd = os.open({str(tmp_path / "f")!r}, os.O_WRONLY | os.O_CREAT)
        os.write(fd, b"abc"); os.fsync(fd); os.fdatasync(fd)
        os.replace({str(tmp_path / "f")!r}, {str(tmp_path / "g")!r})
    """
    env = {key: value for key, value in library.server_environment(shim, "").items() if key != library.RULE_DIR_ENV}
    plain = subprocess.run([sys.executable, "-I", "-c", textwrap.dedent(program)], env=env, capture_output=True)
    assert plain.returncode == 0 and (tmp_path / "g").read_bytes() == b"abc"
    (tmp_path / "rules").mkdir()
    (tmp_path / "g").rename(tmp_path / "f")
    assert _run(shim, tmp_path / "rules", program).returncode == 0


@needs_cc
def test_the_library_builds_once_per_source_and_a_missing_compiler_is_a_deployment_error(tmp_path, monkeypatch):
    first = library.build_library(str(tmp_path))
    mtime = os.stat(first).st_mtime_ns
    assert library.build_library(str(tmp_path)) == first and os.stat(first).st_mtime_ns == mtime
    monkeypatch.setenv("CC", str(tmp_path / "no-such-cc"))
    with pytest.raises(LocalDeploymentError, match="cannot build the syscall fault library"):
        library.build_library(str(tmp_path / "other"))


def test_a_deployment_with_syscall_faults_starts_its_servers_under_the_library(tmp_path, monkeypatch):
    built = []
    monkeypatch.setattr(library, "build_library", lambda directory: built.append(directory) or "/lib/shim.so")
    data_dir = tmp_path / "deployment-x" / "valkey-0" / "data"
    assert ProcessManager(DeploymentConfig(syscall_faults=False))._server_env(str(data_dir)) is None
    env = ProcessManager(DeploymentConfig(syscall_faults=True))._server_env(str(data_dir))
    rules = tmp_path / "deployment-x" / "valkey-0" / "syscall-faults"
    assert env[library.RULE_DIR_ENV] == str(rules) and rules.is_dir()
    assert env["LD_PRELOAD"].endswith("/lib/shim.so") and "verify_asan_link_order=0" in env["ASAN_OPTIONS"]
    assert built == [str(tmp_path / "deployment-x")]
    assert deployment_config_from_document({"mode": "standalone", "syscall_faults": True}).syscall_faults


class _Handle:
    """Two instances; the syscall fault directory is under ``root``; the process dies once the library kills."""

    def __init__(self, root):
        self.root = root
        self.instances = tuple(
            SimpleNamespace(instance_id=f"valkey-{n}", port=7000 + n, bus_port=17000 + n) for n in range(2)
        )
        self.alive, self.restarts = True, 0

    def primaries(self):
        return ["valkey-0"]

    def replicas(self):
        return ["valkey-1"]

    def syscall_fault_dir(self, instance_id):
        path = os.path.join(self.root, instance_id, "syscall-faults")
        os.makedirs(path, exist_ok=True)
        return path

    def data_dir(self, instance_id):
        return os.path.join(self.root, instance_id, "data")

    def pid(self, instance_id):
        return 4242

    def started_at(self, instance_id):
        return 123.0

    def is_alive(self, instance_id):
        return self.alive

    def send_signal(self, instance_id, signum):
        pass

    def wait_exit(self, instance_id, timeout=10.0):
        return not self.alive

    def restart(self, instance_id, wait_ready=True, ready_timeout=30.0):
        self.restarts += 1
        self.alive = True


def _library_acts(handle, instance_id, fault_id, at, action="kill"):
    """What the library does at the nth call: log it, retire the rule, act."""

    rules = handle.syscall_fault_dir(instance_id)
    with open(os.path.join(rules, f"{fault_id}.log"), "a") as log:
        for n, kind in ((1, "pass"), (2, action)):
            line = {"n": n, "call": "write", "path": "/d/nodes.conf.tmp-1", "pid": 9, "t": at, "action": kind}
            log.write(json.dumps(line) + "\n")
    os.replace(os.path.join(rules, f"{fault_id}.rule"), os.path.join(rules, f"{fault_id}.fired"))
    if action == "kill":
        handle.alive = False


def _fault(handle, **config):
    context = fake_context(handle)
    spec = resolved(
        ComponentSpec(
            role=ComponentRole.FAULT,
            use="valkey.fault.syscall",
            id="crash",
            config={"when_event": "go", "calls": ["write"], **config},
        )
    )
    fault = SyscallFault()
    fault.inject(context, spec)
    return context, spec, fault


def _wait(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.01)


def test_a_kill_lands_when_the_library_acts_and_the_fault_restarts_the_instance(tmp_path):
    handle = _Handle(str(tmp_path))
    context, spec, fault = _fault(handle, instance="valkey-1", path="nodes.conf.tmp-*", nth=2)
    context.event_bus.publish("go", source="test")
    rule = tmp_path / "valkey-1" / "syscall-faults" / "crash.rule"
    assert rule.read_text() == (
        f"calls=write nth=2 action=kill when=before path={tmp_path}/valkey-1/data/nodes.conf.tmp-*\n"
    )
    assert not context.event_bus.committed_history("crash.injected"), "armed, not landed"
    _library_acts(handle, "valkey-1", "crash", at=1000.5)
    _wait(lambda: context.event_bus.committed_history("crash.recovered"))
    (injected,) = context.event_bus.committed_history("crash.injected")
    assert injected.payload["applied_at"] == 1000.5 and injected.payload["signal"] == "SIGKILL"
    result = fault.recover(context, spec)
    assert result.status is ComponentStatus.PASS, result.summary
    record = next(item.value for item in result.evidence if item.evidence_id == FAULT_INJECTED)
    assert record["call"] == "write" and record["calls_matched"] == 2 and record["applied_at"] == 1000.5
    assert record["restarted"] and record["restart_spawned_at"] == 123.0 and handle.restarts == 1


def test_a_rule_that_never_acts_is_removed_and_leaves_the_run_untested(tmp_path):
    handle = _Handle(str(tmp_path))
    context, spec, fault = _fault(handle, instance="primary", action="fail", errno="ENOSPC", peer="valkey-1")
    context.event_bus.publish("go", source="test")
    rule = tmp_path / "valkey-0" / "syscall-faults" / "crash.rule"
    assert "peer_ports=7001,17001" in rule.read_text() and "errno=28" in rule.read_text()
    result = fault.recover(context, spec)
    assert result.status is ComponentStatus.ERROR
    assert "the rule never acted on valkey-0: 0 matching call(s), nth 1" in result.summary
    assert not rule.exists() and handle.restarts == 0


def test_a_delay_or_fail_lands_without_a_restart(tmp_path):
    handle = _Handle(str(tmp_path))
    context, spec, fault = _fault(handle, instance="valkey-0", action="delay", delay_ms=5)
    context.event_bus.publish("go", source="test")
    _library_acts(handle, "valkey-0", "crash", at=7.0, action="delay")
    _wait(lambda: context.event_bus.committed_history("crash.injected"))
    result = fault.recover(context, spec)
    assert result.status is ComponentStatus.PASS, result.summary
    assert handle.restarts == 0 and not context.event_bus.committed_history("crash.recovered")


_ON = {"syscall_faults": True}


@pytest.mark.parametrize(
    ("config", "document", "message"),
    [
        ({"calls": ["write"]}, {}, "requires deployment.syscall_faults: true"),
        ({"calls": []}, _ON, "calls must name at least one"),
        ({"calls": ["fsync"], "peer": "valkey-0"}, _ON, "only write"),
        ({"calls": ["write"], "errno": "ENOPE"}, _ON, "not an errno"),
        ({"calls": ["write"], "path": "a b"}, _ON, "whitespace"),
        ({"calls": ["write"], "capture_cluster_vars": True}, _ON, "needs a cluster deployment"),
    ],
)
def test_bad_syscall_fault_configs_fail_to_compile(config, document, message):
    spec = ComponentSpec(
        role=ComponentRole.FAULT, use="valkey.fault.syscall", id="f", config={"when_event": "x", **config}
    )
    plan = unit_plan((spec,), document={"mode": "standalone", **document})
    with pytest.raises(ConfigError, match=message):
        registration_for("valkey.fault.syscall").resolve_config(spec, plan)


def test_a_rule_names_only_the_fields_its_action_uses():
    config = {"calls": ["write", "rename"], "nth": 3, "action": "fail", "when": "before", "delay_ms": 9, "errno": "EIO"}
    assert rule_text(config, None, []) == "calls=write,rename nth=3 action=fail when=before errno=5\n"


class _BenchmarkHandle:
    """valkey-0 and valkey-1, each with a valkey-benchmark beside its binary: a script that records its arguments
    and exits at once the first time, then runs until killed."""

    def __init__(self, root):
        self.root = root
        self.instances = tuple(
            SimpleNamespace(instance_id=f"valkey-{n}", host="127.0.0.1", port=7000 + n, bus_port=17000 + n)
            for n in range(2)
        )
        os.makedirs(os.path.join(root, "bin"))
        script = os.path.join(root, "bin", "valkey-benchmark")
        with open(script, "w") as out:
            out.write(
                f'#!/bin/sh\necho "$@" >> {root}/args\n'
                f"[ -e {root}/ran-$4 ] || {{ touch {root}/ran-$4; exit 1; }}\nexec sleep 60\n"
            )
        os.chmod(script, 0o755)

    def primaries(self):
        return ["valkey-0"]

    def replicas(self):
        return []

    def binary(self, instance_id):
        return os.path.join(self.root, "bin", "valkey-server")


def test_the_benchmark_workload_loads_each_target_with_its_build_and_starts_one_that_exited_again(tmp_path):
    from valkey_fuzzer.components.workloads import BENCHMARK_EVIDENCE, BenchmarkWorkload

    handle = _BenchmarkHandle(str(tmp_path))
    spec = resolved(
        ComponentSpec(
            role=ComponentRole.WORKLOAD,
            use="valkey.workload.benchmark",
            id="busy",
            config={"instances": "all", "clients": 4, "command": ["DEBUG", "SLEEP", "0.001"]},
        )
    )
    context = fake_context(handle)
    workload = BenchmarkWorkload()
    assert workload.start(context, spec).status is ComponentStatus.PASS
    _wait(lambda: (tmp_path / "args").exists() and len((tmp_path / "args").read_text().splitlines()) == 4)
    result = workload.stop(context, spec)
    assert result.status is ComponentStatus.PASS, result.summary
    assert sorted((tmp_path / "args").read_text().splitlines())[0] == (
        "-h 127.0.0.1 -p 7000 -c 4 -P 1 -l -q DEBUG SLEEP 0.001"
    )
    (record,) = [item.value for item in result.evidence if item.evidence_id == BENCHMARK_EVIDENCE]
    assert dict(record["starts"]) == {"valkey-0": 2, "valkey-1": 2} and list(record["targets"]) == [
        "valkey-0",
        "valkey-1",
    ]


def test_a_build_without_valkey_benchmark_leaves_the_load_untested(tmp_path):
    from valkey_fuzzer.components.workloads import BenchmarkWorkload

    handle = _BenchmarkHandle(str(tmp_path))
    os.unlink(tmp_path / "bin" / "valkey-benchmark")
    spec = resolved(ComponentSpec(role=ComponentRole.WORKLOAD, use="valkey.workload.benchmark", id="busy", config={}))
    result = BenchmarkWorkload().start(fake_context(handle), spec)
    assert result.status is ComponentStatus.ERROR and "no valkey-benchmark beside the build" in result.summary
