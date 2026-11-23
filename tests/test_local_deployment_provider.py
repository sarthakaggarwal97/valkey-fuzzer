"""The local deployment provider and the Valkey deployment service it hands components."""

import os
import random
import signal
import socket
import stat
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import pytest

from tests import support
from tests.deployment_support import (
    MIGRATED_TOPOLOGY,
    client_context,
    instance,
    planned_cluster_instances,
    process_manager,
)
from tests.support import fake_executable
from valkey_fuzzer.deadlines import WaitInterruptedError
from valkey_fuzzer.deployment.local.cluster_nodes import parse_cluster_nodes
from valkey_fuzzer.deployment.local.formation import Formation
from valkey_fuzzer.deployment.local.models import (
    DeploymentConfig,
    DeploymentMode,
    LocalDeployment,
    LocalDeploymentError,
)
from valkey_fuzzer.deployment.local.ports import PortUnavailableError
from valkey_fuzzer.deployment.local.provider import LOCAL_DEPLOYMENT_PROVIDER_ID, LocalDeploymentProvider
from valkey_fuzzer.deployment.local.queries import Replication
from valkey_fuzzer.deployment.local.rerun import busy_planned_ports
from valkey_fuzzer.deployment.local.service import VALKEY_DEPLOYMENT_SERVICE, ValkeyDeploymentService
from valkey_fuzzer.deployment.local.topology import TopologyNode
from valkey_fuzzer.deployment.local.validation.replication import _recorded_primary
from valkey_fuzzer.deployment.local.validation.snapshot import Snapshot
from valkey_fuzzer.plan import DeploymentProviderIdentity, ExecutionPlan
from valkey_fuzzer.protocols import ProvisionedDeployment

PROVIDER = "valkey_fuzzer.deployment.local.provider"
SERVICE = "valkey_fuzzer.deployment.local.service"


def make_plan(**deployment) -> ExecutionPlan:
    """A smoke plan for the local provider with ``deployment`` (standalone unless it says); seed 17."""

    return support.make_plan(
        seed=17,
        deployment_mode=deployment.get("mode", "standalone"),
        deployment_provider=DeploymentProviderIdentity(provider_id=LOCAL_DEPLOYMENT_PROVIDER_ID),
        deployment={"mode": "standalone", **deployment},
    )


def run_context(plan: ExecutionPlan, deadline=None) -> Mock:
    """The runner's context for a provision of ``plan``."""
    return Mock(plan=plan, deadline=deadline)


class FakeLaunch:
    """Stand-ins for the processes and formation of one provision attempt; ``provider()`` uses them."""

    def __init__(self):
        self.instances = [instance()]
        self.processes = Mock(deployment_id="local-test", spawned_instances=self.instances)
        self.processes.spawn_all.return_value = self.instances
        self.processes.resolve_valkey_binary.return_value = "/opt/valkey-server"
        self.formation = Mock()
        self.formation.form.return_value = None

    def _processes(self, config, seed):
        self.config, self.seed = config, seed
        return self.processes

    def _formation(self, context):
        self.formation_context = context
        return self.formation

    def provider(self):
        """A provider that provisions with these stand-ins until the test ends."""
        patch.multiple(PROVIDER, ProcessManager=self._processes, Formation=self._formation).start()
        return LocalDeploymentProvider()


@pytest.fixture(autouse=True)
def _stop_launches():
    yield
    patch.stopall()


@pytest.fixture(autouse=True)
def _no_probes(request):
    """Provisioning uses fake processes, whatever this host runs on the planned ports."""
    if request.function is test_provision_names_every_busy_planned_port_before_starting_anything:
        yield
        return
    with (
        patch(f"{PROVIDER}.detect_valkey_sha", return_value="abc1234"),
        patch(f"{PROVIDER}.ensure_ports_free"),
    ):
        yield


def test_provision_builds_a_service_that_restarts_and_is_cleaned_up():
    launch = FakeLaunch()
    launch.processes.restart_instance.side_effect = [None, RuntimeError("died")]
    provider, context = launch.provider(), run_context(make_plan(server_config={"appendonly": "no"}))

    deployment = provider.provision(context)
    handle = deployment.handle

    assert launch.seed == 17 and launch.processes.spawn_all.call_args.args[0][0].server_config == {"appendonly": "no"}
    assert deployment.services == {VALKEY_DEPLOYMENT_SERVICE: handle} and deployment.valkey_sha == "abc1234"
    assert [node.instance_id for node in handle.instances] == ["valkey-0"] and handle.mode is DeploymentMode.STANDALONE
    handle.restart("valkey-0", wait_ready=False)
    with pytest.raises(LocalDeploymentError, match="failed to restart instance 'valkey-0'"):
        handle.restart("valkey-0")
    with pytest.raises(LocalDeploymentError, match="already owns"):
        provider.provision(context)
    with pytest.raises(LocalDeploymentError, match="not owned"):
        provider.cleanup(context, ProvisionedDeployment())

    provider.cleanup(context, deployment)

    launch.processes.cleanup_deployment.assert_called_once_with(launch.instances)
    with pytest.raises(LocalDeploymentError, match="already been cleaned up"):
        handle.pid("valkey-0")


def test_provisioning_waits_and_the_source_build_end_by_the_runs_deadline(monkeypatch):
    launch = FakeLaunch()
    monkeypatch.setattr(f"{PROVIDER}.time.monotonic", lambda: 1000.0)
    context = run_context(make_plan(), deadline=1060.0)

    launch.provider().provision(context)

    assert launch.processes.resolve_valkey_binary.call_args.kwargs == {"build_timeout": 60.0}
    assert launch.processes.spawn_all.call_args.args[1] is context and launch.formation_context is context


@pytest.mark.parametrize("failure", ["spawn", "formation", "formation's cause"])
def test_a_failed_provision_is_released_at_once_and_a_failed_release_is_retried_without_its_text(
    failure, caplog, capsys
):
    launch = FakeLaunch()
    if failure == "spawn":
        launch.processes.spawn_all.side_effect = RuntimeError("spawn failed")
    elif failure == "formation":
        launch.formation.form.side_effect = RuntimeError("formation failed")
    elif failure == "formation's cause":
        # The error names what failed, and nothing is printed.
        launch.formation = Formation()
        launch.formation.reset_cluster_state = Mock(side_effect=RuntimeError("CLUSTER RESET refused"))
    # The release at once fails, and so does the first rollback.
    launch.processes.cleanup_deployment.side_effect = [RuntimeError("SECRET"), RuntimeError("SECRET"), None]
    plan = make_plan(mode="cluster") if failure == "formation's cause" else make_plan()
    provider, context = launch.provider(), run_context(plan)

    with pytest.raises(Exception, match=r"spawn failed|formation failed|CLUSTER RESET refused"):
        provider.provision(context)
    assert capsys.readouterr().err == ""
    with pytest.raises(LocalDeploymentError, match="failed to roll back") as error:
        provider.rollback(context)
    provider.rollback(context)
    provider.rollback(context)

    assert "SECRET" not in str(error.value) and "SECRET" not in caplog.text
    assert launch.processes.cleanup_deployment.call_args_list == [call(launch.instances)] * 3


def test_a_failed_cleanup_can_be_retried():
    launch = FakeLaunch()
    launch.processes.cleanup_deployment.side_effect = [RuntimeError("cleanup failed"), None]
    provider, context = launch.provider(), run_context(make_plan())
    deployment = provider.provision(context)

    with pytest.raises(LocalDeploymentError, match="failed to clean up"):
        provider.cleanup(context, deployment)
    assert deployment.handle.instances  # still open
    provider.cleanup(context, deployment)
    with pytest.raises(LocalDeploymentError, match="already been cleaned up"):
        deployment.handle.instance("valkey-0")


def test_a_deployment_holds_its_loopback_block_host_wide_until_it_is_cleaned_up(tmp_path, monkeypatch):
    # Two runs on one host never share per-instance addresses (two batches' worker 0, or two plain runs on
    # block 0): a partition's DROP rules match addresses only, so one run's partition would silently cut the
    # other's replication, a wrong FAIL.
    from valkey_fuzzer.deployment.local import ports

    monkeypatch.setattr(ports, "LOCK_DIRECTORY", str(tmp_path / "locks"))
    plan = make_plan(addressing="per_instance_loopback", loopback_block=5)
    other = ports.claim_loopback_block(5)  # Another run, in any process, holds block 5.
    first = FakeLaunch()
    with pytest.raises(PortUnavailableError, match=r"loopback block 5 \(127\.0\.5\.0/24\) is in use by another run"):
        first.provider().provision(run_context(plan))
    first.processes.spawn_all.assert_not_called()
    os.close(other)

    launch = FakeLaunch()
    provider, context = launch.provider(), run_context(plan)
    deployment = provider.provision(context)
    with pytest.raises(PortUnavailableError, match="loopback block 5"):
        ports.claim_loopback_block(5)
    provider.cleanup(context, deployment)
    os.close(ports.claim_loopback_block(5))
    # A run on 127.0.0.1 holds no block.
    shared = FakeLaunch()
    shared.provider().provision(run_context(make_plan()))
    os.close(ports.claim_loopback_block(0))


def test_the_loopback_locks_live_apart_from_the_data_every_user_shares(tmp_path, monkeypatch):
    # A root run once created /tmp/valkey-fuzzer (root-owned, 0755) to hold the locks, and every later non-root run
    # then failed with "Permission denied" on its runs and data under it.
    from valkey_fuzzer.deployment.local import ports
    from valkey_fuzzer.deployment.local.models import DEFAULT_BASE_DATA_DIR

    assert os.path.dirname(ports.LOCK_DIRECTORY) != DEFAULT_BASE_DATA_DIR.rstrip("/")
    assert not ports.LOCK_DIRECTORY.startswith(DEFAULT_BASE_DATA_DIR.rstrip("/") + "/")
    monkeypatch.setattr(ports, "LOCK_DIRECTORY", str(tmp_path / "locks"))
    os.close(ports.claim_loopback_block(3))
    assert stat.S_IMODE(os.stat(tmp_path / "locks").st_mode) == 0o1777


def _two_adjacent_ports():
    """A listener on an ephemeral port P with the port P - 1 bound too, so both are known free of anything else
    on this host: valkey-0 plans its client port on P - 1 and valkey-1 on P."""

    while True:
        listener, below = socket.socket(), socket.socket()
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        try:
            below.bind(("127.0.0.1", port - 1))
        except OSError:
            listener.close()
            below.close()
            continue
        listener.listen(1)
        return listener, below, port


def test_provision_names_every_busy_planned_port_before_starting_anything():
    listener, below, port = _two_adjacent_ports()
    below.close()  # valkey-0's port is free again, valkey-1's is listening
    factory = patch(f"{PROVIDER}.ProcessManager").start()
    try:
        busy = busy_planned_ports({"mode": "standalone", "replicas_per_primary": 1, "base_port": port - 1})
        assert [(item.instance_id, item.kind, item.port) for item in busy] == [("valkey-1", "client", port)]
        with pytest.raises(PortUnavailableError, match=f"valkey-1: client port {port} on 127.0.0.1"):
            LocalDeploymentProvider().provision(run_context(make_plan(replicas_per_primary=1, base_port=port - 1)))
    finally:
        listener.close()
    # A free port in the host's ephemeral range can still be taken by an outgoing connection before Valkey binds it.
    ports = "valkey_fuzzer.deployment.local.ports"
    with (
        patch(f"{ports}.port_unavailable_reason", return_value=None),
        patch(f"{ports}.ephemeral_port_range", return_value=(7001, 60999)),
        pytest.raises(PortUnavailableError, match=r"ephemeral port range 7001-60999.*base_port of at most 6999"),
    ):
        LocalDeploymentProvider().provision(run_context(make_plan(replicas_per_primary=1, base_port=7000)))
    factory.assert_not_called()


def test_the_service_controls_each_instances_process_and_follows_failovers(tmp_path):
    process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    nodes = [instance(pid=process.pid, log_file="/tmp/l0"), instance("valkey-1", "replica", 7001)]
    nodes[0].process = process
    binary = fake_executable(tmp_path / "valkey-server")
    config = DeploymentConfig(primaries=1, replicas_per_primary=1, mode=DeploymentMode.STANDALONE, valkey_binary="/x")
    handle = ValkeyDeploymentService(Mock(), LocalDeployment("d", config, nodes))
    try:
        assert (handle.pid("valkey-0"), handle.data_dir("valkey-0"), handle.binary("valkey-0")) == (
            process.pid,
            "/tmp/valkey-0",
            "/x",
        )
        assert handle.process_output_file("valkey-0") == "/tmp/l0.stdio.log" and handle.valkey_sha is None
        assert handle.set_binary("valkey-0", str(binary)) == handle.binary("valkey-0") == str(binary)
        with pytest.raises(FileNotFoundError):
            handle.set_binary("valkey-0", str(tmp_path / "missing"))
        handle.send_signal("valkey-0", signal.SIGKILL)
        assert handle.wait_exit("valkey-0", timeout=10) and not handle.is_alive("valkey-0")
        with pytest.raises(ProcessLookupError):
            handle.send_signal("valkey-0", signal.SIGTERM)
    finally:
        process.kill()
        process.wait()

    # A fault's SIGKILL takes the server's forked children along, as cleanup's does (F14); other signals go to
    # the server alone.
    with patch(f"{SERVICE}.kill_server") as kill_server:
        handle.send_signal("valkey-1", signal.SIGTERM)
        handle.send_signal("valkey-1", signal.SIGKILL)
    nodes[1].process.send_signal.assert_called_once_with(signal.SIGTERM)
    kill_server.assert_called_once_with(nodes[1].process)

    # Roles follow a failover the service did not make; only a cluster fails over on request.
    handle._live_replication = lambda: {
        "valkey-0": Replication("replica", "127.0.0.1", 7001),
        "valkey-1": Replication("primary"),
    }
    assert handle.refresh_roles() == handle.roles() == {"valkey-0": "replica", "valkey-1": "primary"}
    assert handle.primaries() == ["valkey-1"] and nodes[0].primary_id == "valkey-1"
    with pytest.raises(LocalDeploymentError, match="only available in cluster mode"):
        handle.failover(0, timeout=1.0, rng=random.Random(0))
    # Clients handed to components close with the deployment, and none is handed out after.
    client = handle.new_client("valkey-1")
    # Each command is sent once, so a workload knows whether a timed-out write may have applied.
    assert client.connection_pool.connection_kwargs["retry"]._retries == 0
    client.close = Mock()
    handle.close(lambda instances: None)
    client.close.assert_called_once()
    with pytest.raises(LocalDeploymentError, match="cleaned up"):
        handle.new_client("valkey-1")


def test_a_roles_sweep_asks_every_instance_at_once_so_stalled_ones_cost_it_one_timeout(monkeypatch):
    """Under a held stall fault an instance accepts the connect and its INFO times out (2 s): the sweep used to
    pay that per stalled instance, on every primaries() of the workloads, oracles and wave engine."""

    config = DeploymentConfig(mode=DeploymentMode.STANDALONE, primaries=1, replicas_per_primary=3)
    nodes = [instance(f"valkey-{n}", "primary" if n == 0 else "replica", 7000 + n) for n in range(4)]
    handle = ValkeyDeploymentService(Mock(), LocalDeployment("d", config, nodes))
    threads = set()

    def stalled(host, port, timeout, query):
        threads.add(threading.get_ident())
        time.sleep(0.2)
        # valkey-1's INFO timed out; the others answered, late.
        return None if port == 7001 else Replication("primary" if port == 7000 else "replica")

    monkeypatch.setattr(f"{SERVICE}.query_or_none", stalled)
    started = time.monotonic()
    roles = handle.roles()
    assert time.monotonic() - started < 0.6, "the sweep waited out the stalls one after another"
    assert len(threads) == len(nodes), "every instance was asked on its own thread"
    # The same answers, in planning order.
    assert list(roles.items()) == [("valkey-0", "primary"), ("valkey-2", "replica"), ("valkey-3", "replica")]


def test_a_restart_that_outlived_the_cleanup_wait_stops_the_server_it_started(monkeypatch):
    # Cleanup released the instances while the restart still ran; its new server would outlive the deployment.
    monkeypatch.setattr(f"{SERVICE}.CLOSE_WAIT_S", 0.2)
    config = DeploymentConfig(mode=DeploymentMode.STANDALONE, primaries=1, replicas_per_primary=0)
    processes = Mock()
    handle = ValkeyDeploymentService(processes, LocalDeployment("d", config, [instance()]))
    handle._live_replication = dict
    spawning, released = threading.Event(), threading.Event()
    processes.restart_instance.side_effect = lambda *args, **kwargs: spawning.set() or released.wait(10)
    errors = []

    def restart():
        try:
            handle.restart("valkey-0")
        except LocalDeploymentError as exc:
            errors.append(str(exc))

    worker = threading.Thread(target=restart)
    worker.start()
    spawning.wait(5)
    handle.close(lambda instances: released.set())
    worker.join(10)

    processes.terminate_instance.assert_called_once_with(handle._infos_by_id["valkey-0"])
    assert errors and "cleaned up during the restart" in errors[0]


def test_cleanup_stops_waiting_for_an_operation_that_does_not_return(monkeypatch):
    # A component call the runner abandoned can still hold an operation; cleanup must not wait for it forever.
    monkeypatch.setattr(f"{SERVICE}.CLOSE_WAIT_S", 0.2)
    config = DeploymentConfig(mode=DeploymentMode.STANDALONE, primaries=1, replicas_per_primary=0)
    handle = ValkeyDeploymentService(Mock(), LocalDeployment("d", config, [instance()]))
    released, holding, done = [], threading.Event(), threading.Event()

    def hold():
        with handle._operation():
            holding.set()
            done.wait(10)

    worker = threading.Thread(target=hold)
    worker.start()
    try:
        holding.wait(5)
        started = time.monotonic()
        handle.close(released.append)
        assert time.monotonic() - started < 5 and [i.instance_id for i in released[0]] == ["valkey-0"]
        with pytest.raises(LocalDeploymentError, match="already been cleaned up"):
            handle.instance("valkey-0")
    finally:
        done.set()
        worker.join()


def test_waiting_for_an_instance_to_exit_ends_when_the_call_is_cancelled():
    config = DeploymentConfig(mode=DeploymentMode.STANDALONE, primaries=1, replicas_per_primary=0)
    cancelled = SimpleNamespace(deadline=None, sleep=lambda seconds: False)
    handle = ValkeyDeploymentService(Mock(), LocalDeployment("d", config, [instance()]), cancelled)

    with pytest.raises(WaitInterruptedError, match="valkey-0 to exit"):
        handle.wait_exit("valkey-0", timeout=60)


def test_live_instances_report_the_shard_each_node_serves_now():
    config = DeploymentConfig(mode=DeploymentMode.CLUSTER, primaries=4, replicas_per_primary=2, base_data_dir="/x")
    service = ValkeyDeploymentService(Mock(), LocalDeployment("seed-38", config, planned_cluster_instances()))
    with patch(
        "valkey_fuzzer.deployment.local.topology.query_cluster_nodes",
        return_value=parse_cluster_nodes(MIGRATED_TOPOLOGY),
    ):
        live = {row["instance_id"]: (row["role"], row["shard_id"]) for row in service.live_instances()}

    # valkey-4, planned in shard 1, migrated into shard 0 and owns its slots; valkey-2, planned in shard 0, follows it.
    assert live["valkey-4"] == ("primary", 0) and live["valkey-2"] == ("replica", 0)
    assert live["valkey-3"] == ("primary", 1) and {"valkey-0", "valkey-1", "valkey-9"}.isdisjoint(live)


@pytest.mark.parametrize("replicated", [True, False])
def test_a_killed_old_primary_restarts_as_a_replica_only_of_a_primary_replicate_recorded(
    tmp_path, monkeypatch, replicated
):
    old, new = instance("valkey-0", "primary", 7000), instance("valkey-1", "replica", 7001, primary_id="valkey-0")
    old.process.poll.return_value = -9
    manager = process_manager(tmp_path, replicas_per_primary=1)
    manager._spawned_instances.update({"valkey-0": old, "valkey-1": new})
    handle = ValkeyDeploymentService(manager, LocalDeployment("d1", manager.config, [old, new]))
    monkeypatch.setattr(handle, "_refresh_roles", lambda: {})
    monkeypatch.setattr(manager, "ensure_ports_available", lambda *args, **kwargs: None)
    start = Mock(return_value=Mock(pid=2000, **{"poll.return_value": None}))
    monkeypatch.setattr(manager, "_start_process", start)
    client = Mock()
    with patch(f"{SERVICE}.valkey_client", return_value=client_context(client)):
        if replicated:
            handle.replicate("valkey-1", None)
            handle.replicate("valkey-0", "valkey-1")
        else:
            new.role, new.primary_id = "primary", None  # promoted; the dead primary's record was never changed

    handle.restart("valkey-0", wait_ready=False)

    command = start.call_args.args[0]
    view = [
        TopologyNode(n.instance_id, n.instance_id, n.host, n.port, n.role, n.shard_id, failed=False) for n in (old, new)
    ]
    snapshot = Snapshot(DeploymentMode.STANDALONE, [old, new], view)
    if replicated:
        # Only the running instance is sent a command; the stopped one is only recorded.
        client.execute_command.assert_called_once_with("REPLICAOF", "NO", "ONE")
        assert command[command.index("--replicaof") :][:3] == ["--replicaof", "127.0.0.1", "7001"]
        assert _recorded_primary(snapshot).node_id == "valkey-1"
    else:
        assert "--replicaof" not in command
        with pytest.raises(ValueError, match="exactly one recorded primary, found 2"):
            _recorded_primary(snapshot)
