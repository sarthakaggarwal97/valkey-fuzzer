"""The redis pack, tested without a real redis-server: an in-memory fake deployment for the components, and a fake
``redis-server`` process (``fake_redis_server.py``) for the provider and the experiment end to end."""

import json
import os
import shutil
import signal
import socket
import sys
import time
from importlib.metadata import entry_points
from pathlib import Path

import pytest
import redis_pack
import valkey
from redis_pack import REGISTRATIONS, register
from redis_pack.deployment import (
    INSTANCE_ID,
    MODE,
    PROVIDER_ID,
    SERVICE,
    RedisDeploymentError,
    RedisDeploymentProvider,
    binary_override,
    deployment_provider_registration,
    normalize_document,
    plan_instances,
    recorded_instances,
    server_command,
)
from redis_pack.fault import FAULT_INJECTED, ProcessSignalFault
from redis_pack.oracles import CrashOracle, LedgerModel, LivenessOracle, crash_cause
from redis_pack.workload import LEDGER_EVIDENCE, operation

from valkey_fuzzer.catalog import Catalog
from valkey_fuzzer.hoststate import OWNER_FILE
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan, ExperimentSpec, RunMode, StartupOverlay
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.results import ComponentStatus, OutcomeStatus, evidence_of
from valkey_fuzzer.testing import FakeContext, resolved_spec, run_experiment

FAKE_SERVER = Path(__file__).resolve().parent / "fake_redis_server.py"
FAST = {"keys": 50, "rate": 500, "run_before_kill_s": 0.3, "run_after_restart_s": 0.2}
BUG_REPORT = (
    "1:M 20 Nov 2026 11:00:00.000 * Ready to accept connections tcp\n"
    "1:M 20 Nov 2026 11:00:01.000 # === REDIS BUG REPORT START: Cut & paste starting from here ===\n"
    "1:M 20 Nov 2026 11:00:01.000 # Redis 7.2.17 crashed by signal: 11, si_code: 0\n"
)


def experiment(experiment_id="redis.kill-restart"):
    return next(spec for spec in redis_pack.experiments() if spec.experiment_id == experiment_id)


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def fake_binary(tmp_path):
    """An executable that runs the fake server with this interpreter, as ``redis_binary`` takes a path."""

    script = tmp_path / "redis-server"
    script.write_text(f'#!/bin/sh\nexec {sys.executable} {FAKE_SERVER} "$@"\n', encoding="utf-8")
    script.chmod(0o755)
    return str(script)


# ---------------------------------------------------------------------------
# An in-memory deployment: the components without a process
# ---------------------------------------------------------------------------


class FakeServer:
    """The dataset, and whether the server is up; ``durable`` keeps the data across a kill, as an AOF does."""

    def __init__(self, durable=True):
        self.durable = durable
        self.up = True
        self.data = {}
        self.loading = False

    def execute(self, name, *args):
        if self.loading:
            raise valkey.BusyLoadingError("LOADING Redis is loading the dataset in memory")
        if name == "SET":
            self.data[args[0]] = args[1]
            return b"OK"
        if name == "GET":
            value = self.data.get(args[0])
            return None if value is None else value.encode()
        if name == "MGET":
            return [None if self.data.get(key) is None else self.data[key].encode() for key in args]
        raise valkey.ResponseError(f"ERR unknown command '{name}'")

    def kill(self):
        self.up = False
        if not self.durable:
            self.data = {}

    def start(self):
        self.up = True


class FakeConnection:
    def __init__(self, server):
        self.server = server
        self.pending = None

    def connect(self):
        if not self.server.up:
            raise valkey.ConnectionError("Error 111 connecting to 127.0.0.1:7000. Connection refused.")

    def send_command(self, *args):
        self.pending = args

    def read_response(self):
        if not self.server.up:
            raise valkey.ConnectionError("Connection closed by server.")
        return self.server.execute(*self.pending)

    def disconnect(self):
        self.pending = None


class FakeClient:
    def __init__(self, server):
        self.server = server

    def ping(self):
        if not self.server.up:
            raise valkey.ConnectionError("Connection refused")
        return True

    def info(self, section=None):
        return {"process_id": 4242, "loading": "1" if self.server.loading else "0"}

    def mget(self, keys):
        return self.server.execute("MGET", *keys)

    def close(self):
        pass


class FakeDeployment:
    """``redis.deployment`` over a :class:`FakeServer`: a kill marks it down, a restart up."""

    def __init__(self, tmp_path, durable=True):
        self.server = FakeServer(durable)
        self.instances = ({"instance_id": INSTANCE_ID, "role": "primary", "host": "127.0.0.1", "port": 7000},)
        self.signals = []
        self.restarts = 0
        self.log = tmp_path / "redis-0.log"
        self.log.write_text("1:M 20 Nov 2026 11:00:00.000 * Ready to accept connections tcp\n", encoding="utf-8")
        self.output = tmp_path / "redis-0.stdio.log"
        self.output.write_text("", encoding="utf-8")

    def connection(self, instance_id, timeout=5.0):
        return FakeConnection(self.server)

    def new_client(self, instance_id, timeout=5.0):
        return FakeClient(self.server)

    def pid(self, instance_id):
        return 4242

    def is_alive(self, instance_id):
        return self.server.up

    def send_signal(self, instance_id, signum):
        if not self.server.up:
            raise ProcessLookupError(f"{instance_id} is not running")
        self.signals.append(signum)
        self.server.kill()

    def wait_exit(self, instance_id, timeout=10.0):
        return True

    def restart(self, instance_id, ready_timeout=30.0):
        self.restarts += 1
        self.server.start()

    def log_file(self, instance_id):
        return str(self.log)

    def process_output_file(self, instance_id):
        return str(self.output)


def fake_run(tmp_path, durable=True, **inputs):
    deployment = FakeDeployment(tmp_path, durable)
    outcome = run_experiment(
        experiment(),
        REGISTRATIONS,
        inputs={**FAST, **inputs},
        services={SERVICE: deployment},
        artifact_dir=str(tmp_path / "runs"),
    )
    return deployment, outcome


def test_the_pack_registers_its_components_its_provider_and_its_experiment_as_the_redis_entry_point():
    catalog = Catalog()
    register(catalog)
    assert set(catalog.components.registered_ids) == {
        "redis.action.sleep",
        "redis.fault.process-signal",
        "redis.oracle.crash",
        "redis.oracle.ledger",
        "redis.oracle.liveness",
        "redis.workload.set-get",
    }
    assert catalog.deployment_providers.registered_ids == ((MODE, PROVIDER_ID),)
    assert catalog.experiments.registered_ids == ("redis.kill-restart",)
    assert experiment().deployment_modes == (MODE,)
    # Installed, the pack is the ``redis`` entry point of the group build_catalog loads.
    (point,) = [point for point in entry_points(group="valkey_fuzzer.packs") if point.name == "redis"]
    assert point.value == "redis_pack:register" and point.load() is register
    # Nothing of the Valkey pack: the pack is held to the core alone. (The core itself still imports the local
    # deployment's constants, valkey_fuzzer.deployment.local.models, from hoststate; see docs/second-target.md.)
    assert not [name for name in sys.modules if name.startswith("valkey_fuzzer.components")]


def test_kill_restart_passes_on_a_durable_server_and_records_the_kill_the_restart_and_the_ledger(tmp_path):
    deployment, outcome = fake_run(tmp_path)
    assert (outcome.status, outcome.summary) == (OutcomeStatus.PASS, "experiment passed")
    assert deployment.signals == [signal.SIGKILL] and deployment.restarts == 1
    results = {result.component_id: result for result in outcome.component_results}
    assert {result.status for result in results.values()} == {ComponentStatus.PASS}
    (injected,) = evidence_of(outcome.all_evidence, FAULT_INJECTED, "kill")
    assert injected.value["applied"] and injected.value["restarted"] and injected.value["signal"] == "SIGKILL"
    assert injected.value["event"] == "run-before-kill.completed"
    (ledger,) = evidence_of(outcome.all_evidence, LEDGER_EVIDENCE, "writes")
    assert ledger.value["operations"] > 20 and ledger.value["acknowledged"] > 10
    entries = [json.loads(line) for line in Path(ledger.value["ledger_path"]).read_text().splitlines()]
    assert [entry["id"] for entry in entries] == list(range(1, len(entries) + 1))
    assert {entry["outcome"] for entry in entries} <= {"ok", "error", "ambiguous"}
    (verified,) = evidence_of(outcome.all_evidence, "redis.ledger.verified", "data")
    assert verified.value["acknowledged"] == ledger.value["acknowledged"]
    names = [event["name"] for event in outcome.diagnostics["events"]]
    assert names.index("run-before-kill.completed") < names.index("kill.injected") < names.index("kill.recovered")


def test_kill_restart_fails_when_the_restarted_server_lost_acknowledged_writes(tmp_path):
    _, outcome = fake_run(tmp_path, durable=False)
    assert outcome.status is OutcomeStatus.FAIL
    (data,) = [result for result in outcome.component_results if result.component_id == "data"]
    assert data.status is ComponentStatus.FAIL and "hold a value the ledger does not allow" in data.summary
    violation = data.diagnostics["violations"][0]
    assert violation["observed"] is None and violation["acceptable"]
    # The server came back and crashed nowhere: only the data is wrong.
    others = {
        result.component_id: result.status for result in outcome.component_results if result.component_id != "data"
    }
    assert set(others.values()) == {ComponentStatus.PASS}


def test_an_operation_is_rejected_when_the_connect_fails_ambiguous_when_the_reply_never_comes_and_rejected_on_loading():
    server = FakeServer()
    connection = FakeConnection(server)
    assert operation(connection, 1, "k", read=False)["outcome"] == "ok"
    read = operation(connection, 2, "k", read=True)
    assert (read["op"], read["outcome"], read["reply"]) == ("get", "ok", "v1") and read["t_end"] >= read["t_start"]
    server.kill()
    down = operation(connection, 3, "k", read=False)
    assert down["outcome"] == "error" and "Connection refused" in down["error"]

    class DiesAfterSend(FakeConnection):
        def send_command(self, *args):
            super().send_command(*args)
            self.server.kill()

    server.start()
    ambiguous = operation(DiesAfterSend(server), 4, "k", read=False)
    assert ambiguous["outcome"] == "ambiguous" and ambiguous["value"] == "v4"
    server.start()
    server.loading = True
    loading = operation(FakeConnection(server), 5, "k", read=False)
    assert loading["outcome"] == "error" and "LOADING" in loading["error"]


def test_the_ledger_model_allows_an_ambiguous_write_until_a_later_acknowledged_one_and_checks_reads():
    def entry(op_id, op, key, outcome, value=None, reply=None):
        item = {"id": op_id, "op": op, "key": key, "outcome": outcome, "t_start": op_id, "t_end": op_id + 0.5}
        if op == "set":
            item["value"] = value
        if reply is not None or (op == "get" and outcome == "ok"):
            item["reply"] = reply
        return item

    model = LedgerModel(
        [
            entry(1, "set", "a", "ok", "v1"),
            entry(2, "set", "a", "ambiguous", "v2"),
            entry(3, "set", "b", "error", "v3"),
            entry(4, "get", "a", "ok", reply="v1"),  # v1 read back: v2 may still land
            entry(5, "set", "c", "ambiguous", "v5"),
            entry(6, "set", "c", "ok", "v6"),  # acknowledged: v5 can no longer be the value
            entry(7, "get", "b", "ok", reply="v3"),  # rejected write read back: a violation
        ]
    )
    assert model.possible == {"a": {"v1", "v2"}, "b": {None}, "c": {"v6"}}
    assert model.read_violations == [{"id": 7, "key": "b", "observed": "v3", "acceptable": [None]}]


def _spec(use, spec_id, **config):
    return ComponentSpec(use=use, id=spec_id, role=ComponentRole(use.split(".")[1]), config=config)


def _registration(use):
    return next(item for item in REGISTRATIONS if item.component_id == use)


def test_the_crash_oracle_names_the_cause_from_the_log_and_judges_a_failed_run_post_mortem(tmp_path):
    deployment = FakeDeployment(tmp_path)
    deployment.log.write_text(BUG_REPORT, encoding="utf-8")
    spec = resolved_spec(_registration("redis.oracle.crash"), _spec("redis.oracle.crash", "crash"))
    context = FakeContext(spec, services={SERVICE: deployment}, deployment_mode=MODE, registrations=REGISTRATIONS)
    result = context.call(CrashOracle().evaluate, spec, ())
    assert (result.status, result.crash) == (ComponentStatus.FAIL, True)
    assert result.summary == f"{INSTANCE_ID}: crash report (crashed by signal: 11)"
    assert result.diagnostics["crashes"] == {INSTANCE_ID: "crashed by signal: 11"}
    assert len(result.diagnostics["crash_reports"][INSTANCE_ID]) == 2
    # The post-mortem reads the files the provider names, with no deployment to ask.
    post = CrashOracle().post_mortem(context, spec, {INSTANCE_ID: (str(deployment.log), str(deployment.output))})
    assert (
        post.status is ComponentStatus.FAIL
        and post.crash
        and post.diagnostics["crashes"] == result.diagnostics["crashes"]
    )
    assert CrashOracle().post_mortem(context, spec, {INSTANCE_ID: (str(deployment.output),)}) is None
    assert crash_cause(["x:1 # ==> server.c:123 'x > 0' is not true"]) == "server.c:123 'x > 0' is not true"
    assert (
        crash_cause(["SUMMARY: AddressSanitizer: heap-use-after-free src/x.c:1 in f"])
        == "AddressSanitizer: heap-use-after-free"
    )
    assert crash_cause(["something else"]) == "something else"


def test_the_liveness_oracle_passes_a_server_that_answers_and_fails_one_that_is_gone_or_loading(tmp_path):
    deployment = FakeDeployment(tmp_path)
    spec = resolved_spec(
        _registration("redis.oracle.liveness"), _spec("redis.oracle.liveness", "liveness", timeout_s=0.3)
    )
    context = FakeContext(spec, services={SERVICE: deployment}, deployment_mode=MODE, registrations=REGISTRATIONS)
    assert context.call(LivenessOracle().evaluate, spec, ()).status is ComponentStatus.PASS
    deployment.server.loading = True
    loading = LivenessOracle().evaluate(context, spec, ())
    assert (loading.status, loading.crash, loading.summary) == (
        ComponentStatus.FAIL,
        False,
        f"{INSTANCE_ID}: still loading",
    )
    deployment.server.kill()
    gone = LivenessOracle().evaluate(context, spec, ())
    assert (gone.status, gone.summary) == (ComponentStatus.FAIL, f"{INSTANCE_ID}: process not running")


def test_a_fault_that_never_fires_is_an_error_and_one_whose_target_is_gone_cannot_be_applied(tmp_path):
    deployment = FakeDeployment(tmp_path)
    registration = _registration("redis.fault.process-signal")
    spec = resolved_spec(registration, _spec("redis.fault.process-signal", "kill", when_event="go.completed"))
    context = FakeContext(spec, services={SERVICE: deployment}, deployment_mode=MODE, registrations=REGISTRATIONS)
    fault = ProcessSignalFault()
    assert context.call(fault.inject, spec).status is ComponentStatus.PASS
    never = context.call(fault.recover, spec)
    assert never.status is ComponentStatus.ERROR and "never triggered" in never.summary and not deployment.signals

    deployment.server.kill()
    fault = ProcessSignalFault()
    context.call(fault.inject, spec)
    context.event_bus.publish("go.completed", source="go", payload={})
    gone = context.call(fault.recover, spec)
    assert gone.status is ComponentStatus.ERROR and "could not be applied" in gone.summary


def test_the_document_fills_its_defaults_rejects_unknown_fields_and_plans_one_instance_with_the_overlays():
    assert normalize_document({"mode": MODE}) == {
        "mode": MODE,
        "base_port": 7000,
        "redis_binary": "redis-server",
        "base_data_dir": "/tmp/valkey-fuzzer",
        "server_config": {},
    }
    with pytest.raises(ValueError, match="replicas_per_primary"):
        normalize_document({"mode": MODE, "replicas_per_primary": 1})
    with pytest.raises(ValueError):
        normalize_document({"mode": "standalone"})
    overlay = StartupOverlay(overlay_id="aof", server_config={"appendonly": "yes", "appendfsync": "always"})
    planned = plan_instances({"mode": MODE, "server_config": {"maxmemory": "1gb", "appendfsync": "no"}}, (overlay,))
    assert planned == {
        INSTANCE_ID: {
            "role": "primary",
            "shard": None,
            "server_config": {"save": "", "appendonly": "yes", "maxmemory": "1gb", "appendfsync": "always"},
        }
    }
    assert binary_override("/opt/redis/src/redis-server") == {"redis_binary": "/opt/redis/src/redis-server"}
    rows = [{"instance_id": INSTANCE_ID, "binary": "/x/redis-server"}]
    assert recorded_instances({"diagnostics": {"deployment": {"instances": rows}}}) == tuple(rows)
    assert recorded_instances({"diagnostics": {}}) == ()
    command = server_command("/x/redis-server", "127.0.0.1", 7000, "/d", "/l.log", {"save": "", "appendonly": "yes"})
    assert command[:7] == ["/x/redis-server", "--port", "7000", "--bind", "127.0.0.1", "--protected-mode", "no"]
    assert command[-4:] == ["--save", "", "--appendonly", "yes"] and "--proc-title-template" in command


def _plan(deployment):
    spec = ExperimentSpec(experiment_id="test.provider", mode=RunMode.SMOKE, deployment_modes=(MODE,), components=())
    return ExecutionPlan.from_experiment(
        spec,
        scenario_id="provider",
        seed=7,
        deployment_mode=MODE,
        deployment_provider=deployment_provider_registration().identity,
        deployment={"mode": MODE, **deployment},
    )


def test_the_provider_runs_a_fake_redis_server_tags_its_directory_restarts_it_and_cleans_up(tmp_path):
    port = free_port()
    document = {"base_port": port, "redis_binary": fake_binary(tmp_path), "base_data_dir": str(tmp_path / "data")}
    provider = RedisDeploymentProvider()
    plan = _plan(document)
    provider.validate(plan)
    context = FakeContext(plan=plan, deployment_mode=MODE)
    deployment = provider.provision(context)
    service = deployment.services[SERVICE]
    try:
        (row,) = deployment.metadata["instances"]
        assert row["port"] == port and row["pid"] == service.pid(INSTANCE_ID) and row["binary_sha256"]
        assert deployment.metadata["redis_version"].startswith("Fake Redis server")
        # The deployment directory carries the sweep's tag, and the server answers as the process started.
        (tagged,) = [path for path in (tmp_path / "data").glob("deployment-*")]
        assert (tagged / OWNER_FILE).exists() and json.loads((tagged / OWNER_FILE).read_text())["pid"] == os.getpid()
        client = service.new_client(INSTANCE_ID)
        assert client.ping() and int(client.info("server")["process_id"]) == service.pid(INSTANCE_ID)
        client.set("k", "v")
        client.close()
        log = Path(service.log_file(INSTANCE_ID))
        assert "Ready to accept connections" in log.read_text() and log.parent == tmp_path / "data" / "logs"
        assert provider.log_files(context) == {INSTANCE_ID: (str(log), service.process_output_file(INSTANCE_ID))}

        # A kill, then a restart on the same address: without persistence the data is gone, as with Redis.
        first = service.pid(INSTANCE_ID)
        service.send_signal(INSTANCE_ID, signal.SIGKILL)
        assert service.wait_exit(INSTANCE_ID, timeout=5) and not service.is_alive(INSTANCE_ID)
        service.restart(INSTANCE_ID, ready_timeout=10)
        assert service.is_alive(INSTANCE_ID) and service.pid(INSTANCE_ID) != first
        client = service.new_client(INSTANCE_ID)
        assert client.get("k") is None
        client.close()
        with pytest.raises(KeyError):
            service.pid("redis-1")
    finally:
        provider.cleanup(context, deployment)
    assert not service.is_alive(INSTANCE_ID) and not list((tmp_path / "data").glob("deployment-*"))
    assert log.exists(), "the log outlives the cleanup for the post-mortem"

    # A port in use, or a binary that is not there, is an error before anything starts.
    with socket.socket() as holder:
        holder.bind(("127.0.0.1", port))
        with pytest.raises(RedisDeploymentError, match="in use"):
            RedisDeploymentProvider().provision(context)
    with pytest.raises(FileNotFoundError, match="redis_binary"):
        RedisDeploymentProvider().provision(
            FakeContext(plan=_plan({**document, "redis_binary": str(tmp_path / "nope")}), deployment_mode=MODE)
        )


def _run_on_fake_process(tmp_path, **server_config):
    document = {
        "mode": MODE,
        "base_port": free_port(),
        "redis_binary": fake_binary(tmp_path),
        "base_data_dir": str(tmp_path / "data"),
        "server_config": server_config,
    }
    return run_experiment(
        experiment(),
        REGISTRATIONS,
        inputs=FAST,
        deployment=document,
        deployment_providers=(deployment_provider_registration(),),
    )


def test_the_experiment_passes_end_to_end_on_the_fake_process(tmp_path):
    outcome = _run_on_fake_process(tmp_path)
    assert (outcome.status, outcome.summary) == (OutcomeStatus.PASS, "experiment passed")
    (injected,) = evidence_of(outcome.all_evidence, FAULT_INJECTED, "kill")
    assert injected.value["exited"] and injected.value["restarted"] and injected.value["pid"]
    (ledger,) = evidence_of(outcome.all_evidence, LEDGER_EVIDENCE, "writes")
    assert ledger.value["acknowledged"] > 10
    assert outcome.diagnostics["deployment"]["instances"][0]["port"] == outcome.plan.deployment["base_port"]
    assert not list((tmp_path / "data").glob("deployment-*"))


def test_a_server_that_dies_with_a_crash_report_fails_the_run_through_the_post_mortem(tmp_path):
    # The fake exits with a crash report after five SETs: the writer sees errors, the fault finds nothing to kill
    # (ERROR), and the crash oracle's post-mortem reads the log the provider names and makes it a FAIL.
    outcome = _run_on_fake_process(tmp_path, **{"fake-exit-after": 5})
    assert outcome.status is OutcomeStatus.FAIL
    (crash,) = [result for result in outcome.component_results if result.component_id == "crash"]
    assert crash.status is ComponentStatus.FAIL and crash.crash
    assert crash.diagnostics["crashes"] == {INSTANCE_ID: "crashed by signal: 11"}


@pytest.mark.slow
def test_live_kill_restart_passes_on_a_real_redis_server(tmp_path):
    binary = os.environ.get("VALKEY_FUZZER_REDIS_BINARY") or shutil.which("redis-server")
    if not binary:
        pytest.skip("no redis-server: set VALKEY_FUZZER_REDIS_BINARY or put redis-server on PATH")
    document = {"mode": MODE, "base_port": free_port(), "redis_binary": binary, "base_data_dir": str(tmp_path / "data")}
    started = time.monotonic()
    outcome = run_experiment(
        experiment(),
        REGISTRATIONS,
        inputs={"run_before_kill_s": 1, "run_after_restart_s": 0.5},
        deployment=document,
        deployment_providers=(deployment_provider_registration(),),
    )
    assert (outcome.status, outcome.summary) == (OutcomeStatus.PASS, "experiment passed"), outcome.component_results
    assert outcome.diagnostics["deployment"]["redis_version"].startswith("Redis server")
    (ledger,) = evidence_of(outcome.all_evidence, LEDGER_EVIDENCE, "writes")
    assert ledger.value["acknowledged"] > 50 and time.monotonic() - started < 60
