"""``valkey.fault.clock-skew`` and the libfaketime deployment option behind it.

The fault tests fake the deployment: the handle's ``TIME`` answers from the timestamp file the fault wrote, as a
server under libfaketime would, or does not, as one without it would.
"""

import os
import time
from pathlib import Path

import pytest

from tests.fakes import FakeHandle, fake_instances
from tests.valkey_experiments_support import (
    evidence_values,
    injected_fault,
    registration_for,
    rejects_config,
    resolved,
    unit_plan,
)
from valkey_fuzzer.components import capabilities
from valkey_fuzzer.components.clock_skew import ClockSkewFault, measure_skew, time_spec
from valkey_fuzzer.components.faults import FAULT_INJECTED
from valkey_fuzzer.deployment.local import clock_faults
from valkey_fuzzer.deployment.local.config import deployment_config_from_document
from valkey_fuzzer.deployment.local.models import DeploymentConfig, LocalDeploymentError
from valkey_fuzzer.deployment.local.processes import ProcessManager
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.results import ComponentStatus

REAL_FIND_LIBRARY = clock_faults.find_library


class _Clock:
    """A client whose ``TIME`` is the host's clock plus what the instance's timestamp file says, as libfaketime
    reports it: the offset, and the rate applied to the time since the server started."""

    def __init__(self, path, started, faked=True):
        self.path, self.started, self.faked = path, started, faked

    def execute_command(self, *args):
        assert args == ("TIME",)
        real = time.time()
        if self.faked:
            offset, _, rate = Path(self.path).read_text(encoding="utf-8").strip().partition(" x")
            real = self.started + (real - self.started) * float(rate) + float(offset)
        return [str(int(real)), str(int((real % 1) * 1_000_000))]

    def close(self):
        pass


class _Handle(FakeHandle):
    """valkey-0 (the primary) and valkey-1, each with a timestamp file under ``root``, started 100 s ago."""

    def __init__(self, root, faked=True, clock_faults=True):
        self.instances = fake_instances(2)
        self.root, self.faked, self.clock_faults = str(root), faked, clock_faults
        self.started = time.time() - 100.0

    def clock_fault_file(self, instance_id):
        if not self.clock_faults:
            return None
        path = os.path.join(self.root, instance_id, "faketime.rc")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if not os.path.exists(path):
            clock_faults.write_timestamp(path, clock_faults.REAL_TIME)
        return path

    def pid(self, instance_id):
        return 4242

    def started_at(self, instance_id):
        # The run clock reads as wall time, so a wall-clock start is a run-clock start too.
        return self.started

    def new_client(self, instance_id, timeout=5.0):
        return _Clock(self.clock_fault_file(instance_id), self.started, self.faked)


def _fault(handle, **config):
    return injected_fault(ClockSkewFault, "valkey.fault.clock-skew", handle, component_id="skew", **config)


def _text(path):
    return Path(path).read_text(encoding="utf-8")


def _fault_record(fault):
    return fault._record


def test_the_spec_compensates_a_rate_for_the_time_the_server_has_run():
    assert time_spec(30.0, 1.0, 1000.0) == "+30.000000 x1\n"
    assert time_spec(-30.0, 1.0, 1000.0) == "-30.000000 x1\n"
    # libfaketime scales the time since the start: 100 s at x2 would add 100 s, so the offset takes them off.
    assert time_spec(30.0, 2.0, 100.0) == "-70.000000 x2\n"
    assert time_spec(0.0, 0.5, 100.0) == "+50.000000 x0.5\n"


@pytest.mark.parametrize(("offset", "rate"), [(-30.0, 1.0), (30.0, 1.0), (0.0, 3.0), (-86400.0, 0.5)])
def test_the_fault_skews_the_instances_clock_and_restores_it_on_the_release_event(tmp_path, offset, rate):
    handle = _Handle(tmp_path)
    context, spec, fault = _fault(handle, instance="primary", offset_s=offset, rate=rate, release_event="back")
    context.event_bus.publish("go", source="test")
    path = handle.clock_fault_file("valkey-0")
    written = _text(path)
    assert written.endswith(f" x{rate:g}\n") and abs(measure_skew(handle, "valkey-0") - offset) < 0.5
    (injected,) = context.event_bus.committed_history("skew.injected")
    assert injected.payload["applied"] and injected.payload["target"] == "valkey-0"
    assert _text(handle.clock_fault_file("valkey-1")) == clock_faults.REAL_TIME, "only the target is skewed"
    context.event_bus.publish("back", source="test")
    assert _text(path) == clock_faults.REAL_TIME
    (released,) = context.event_bus.committed_history("skew.released")
    assert released.payload == {"instance_id": "valkey-0", "reason": "event"}
    result = fault.recover(context, spec)
    assert result.status is ComponentStatus.PASS, result.summary
    record = evidence_values(result.evidence, FAULT_INJECTED)[0]
    assert record["spec"] == written.strip() and record["offset_s"] == offset and record["rate"] == rate
    assert abs(record["skew_measured_s"] - offset) < 0.5 and abs(record["skew_at_release_s"] - offset) < 0.5
    assert record["release_reason"] == "event" and record["skew_seconds"] >= 0
    assert [item["kind"] for item in context.obligations.report()["undone"]] == ["clock.skewed"]


def test_recovery_restores_a_clock_no_event_released(tmp_path):
    handle = _Handle(tmp_path)
    context, spec, fault = _fault(handle, instance="valkey-1", offset_s=60.0)
    context.event_bus.publish("go", source="test")
    path = handle.clock_fault_file("valkey-1")
    assert _text(path) == "+60.000000 x1\n"
    result = fault.recover(context, spec)
    assert result.status is ComponentStatus.PASS, result.summary
    assert _text(path) == clock_faults.REAL_TIME
    record = evidence_values(result.evidence, FAULT_INJECTED)[0]
    assert record["release_reason"] == "recover" and not context.event_bus.committed_history("skew.released")


def test_a_skew_the_server_does_not_show_is_not_applied(tmp_path):
    # The server answers TIME with the real time: libfaketime is not loaded.
    handle = _Handle(tmp_path, faked=False)
    context, spec, fault = _fault(handle, offset_s=-30.0)
    context.event_bus.publish("go", source="test")
    (injected,) = context.event_bus.committed_history("skew.injected")
    assert not injected.payload["applied"]
    result = fault.recover(context, spec)
    assert result.status is ComponentStatus.ERROR
    assert "TIME on valkey-0 is " in result.summary and "s from the host's clock, not -30.000 s" in result.summary
    assert "libfaketime loaded" in result.summary and abs(_fault_record(fault)["skew_measured_s"]) < 0.5
    # Restored all the same: the file holds the real time again.
    assert _text(handle.clock_fault_file("valkey-0")) == clock_faults.REAL_TIME


def test_a_deployment_without_clock_faults_cannot_be_skewed(tmp_path):
    handle = _Handle(tmp_path, clock_faults=False)
    context, spec, fault = _fault(handle, offset_s=-30.0)
    context.event_bus.publish("go", source="test")
    result = fault.recover(context, spec)
    assert result.status is ComponentStatus.ERROR and "clock_faults" in result.summary


@pytest.mark.parametrize(
    ("config", "document", "message"),
    [
        ({"offset_s": 10}, {}, "requires deployment.clock_faults: true"),
        ({}, {"clock_faults": True}, "needs an offset_s other than 0 or a rate other than 1"),
        ({"offset_s": 10, "rate": 0}, {"clock_faults": True}, "rate"),
        ({"offset_s": 10, "instance": "nobody"}, {"clock_faults": True}, "instance"),
    ],
)
def test_bad_clock_skew_configs_fail_to_compile(config, document, message):
    config = {"when_event": "x", **config}
    rejects_config("valkey.fault.clock-skew", config, message, document={"mode": "standalone", **document})


def test_the_library_is_the_named_one_or_the_packages_and_its_absence_names_the_package(tmp_path, monkeypatch):
    monkeypatch.setattr(clock_faults, "find_library", REAL_FIND_LIBRARY)
    monkeypatch.delenv(clock_faults.LIBRARY_ENV, raising=False)
    monkeypatch.setattr(clock_faults, "LIBRARY_PATTERNS", (str(tmp_path / "*" / "faketime" / "libfaketime.so.1"),))
    with pytest.raises(LocalDeploymentError, match="install the libfaketime package") as missing:
        clock_faults.find_library()
    assert "VALKEY_FUZZER_LIBFAKETIME" in str(missing.value)
    supported, reason = capabilities.libfaketime_supported()
    assert not supported and "libfaketime" in reason
    library = tmp_path / "x86_64-linux-gnu" / "faketime" / "libfaketime.so.1"
    library.parent.mkdir(parents=True)
    library.write_bytes(b"")
    assert clock_faults.find_library() == str(library) and capabilities.libfaketime_supported() == (True, "")
    monkeypatch.setenv(clock_faults.LIBRARY_ENV, str(tmp_path / "nowhere.so"))
    with pytest.raises(LocalDeploymentError, match="is not a file"):
        clock_faults.find_library()
    monkeypatch.setenv(clock_faults.LIBRARY_ENV, str(library))
    assert clock_faults.find_library() == str(library)
    # The fault's probe is the same check: a run on a host without the library is an ERROR naming it.
    spec = resolved(
        ComponentSpec(
            role=ComponentRole.FAULT, use="valkey.fault.clock-skew", id="f", config={"when_event": "x", "offset_s": 1}
        )
    )
    plan = unit_plan((spec,), document={"mode": "standalone", "clock_faults": True})
    registration_for("valkey.fault.clock-skew").probe(spec.config, spec, plan)
    monkeypatch.setenv(clock_faults.LIBRARY_ENV, str(tmp_path / "nowhere.so"))
    with pytest.raises(LocalDeploymentError, match="is not a file"):
        registration_for("valkey.fault.clock-skew").probe(spec.config, spec, plan)


def test_a_deployment_with_clock_faults_starts_its_servers_under_libfaketime_reading_their_own_file(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(clock_faults, "find_library", lambda: "/lib/faketime/libfaketime.so.1")
    monkeypatch.setenv("LD_PRELOAD", "/lib/first.so")
    data_dir = tmp_path / "deployment-x" / "valkey-0" / "data"
    assert ProcessManager(DeploymentConfig(clock_faults=False))._server_env(str(data_dir)) is None
    env = ProcessManager(DeploymentConfig(clock_faults=True))._server_env(str(data_dir))
    timestamp = tmp_path / "deployment-x" / "valkey-0" / "faketime.rc"
    assert env[clock_faults.TIMESTAMP_FILE_ENV] == str(timestamp) and timestamp.read_text() == clock_faults.REAL_TIME
    assert env["LD_PRELOAD"] == "/lib/first.so /lib/faketime/libfaketime.so.1"
    assert env["FAKETIME_NO_CACHE"] == "1" and env["DONT_FAKE_MONOTONIC"] == "1"
    assert env["ASAN_OPTIONS"].count("verify_asan_link_order=0") == 1
    # A restart keeps the instance's file: a skewed instance restarts skewed, until the fault restores it.
    clock_faults.write_timestamp(str(timestamp), "+5.000000 x1\n")
    ProcessManager(DeploymentConfig(clock_faults=True))._server_env(str(data_dir))
    assert timestamp.read_text() == "+5.000000 x1\n"
    assert deployment_config_from_document({"mode": "standalone", "clock_faults": True}).clock_faults
    # With the syscall fault library too, both are preloaded and the sanitizer option appears once.
    monkeypatch.setattr(
        "valkey_fuzzer.deployment.local.syscall_faults.build_library", lambda directory: "/lib/syscall.so"
    )
    both = ProcessManager(DeploymentConfig(clock_faults=True, syscall_faults=True))._server_env(str(data_dir))
    assert both["LD_PRELOAD"] == "/lib/first.so /lib/syscall.so /lib/faketime/libfaketime.so.1"
    assert both["ASAN_OPTIONS"].count("verify_asan_link_order=0") == 1


def test_the_timestamp_file_is_replaced_whole(tmp_path):
    path = tmp_path / "faketime.rc"
    clock_faults.write_timestamp(str(path), "+1.000000 x1\n")
    clock_faults.write_timestamp(str(path), "+2.000000 x1\n")
    assert path.read_text() == "+2.000000 x1\n" and sorted(tmp_path.iterdir()) == [path]
