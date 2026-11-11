"""TLS deployments (``deployment.tls``): their certificates, server arguments, clients, builds without TLS, and real
servers that run every connection over TLS."""

import shutil
import ssl
import subprocess
from unittest.mock import Mock, patch

import pytest
import valkey

from tests.deployment_support import formed_cluster, instance, process_manager
from tests.live_support import BINARY, free_base_port
from valkey_fuzzer.deployment.local.formation import Formation
from valkey_fuzzer.deployment.local.models import (
    DeploymentConfig,
    DeploymentMode,
    LocalDeployment,
    LocalDeploymentError,
)
from valkey_fuzzer.deployment.local.planner import plan_deployment
from valkey_fuzzer.deployment.local.processes import ProcessManager
from valkey_fuzzer.deployment.local.provider import LocalDeploymentProvider
from valkey_fuzzer.deployment.local.queries import query_replication, valkey_client
from valkey_fuzzer.deployment.local.service import ValkeyDeploymentService
from valkey_fuzzer.deployment.local.tls import TlsFiles, client_options, create_tls_files, supports_tls
from valkey_fuzzer.deployment.local.topology import DeploymentConnection
from valkey_fuzzer.protocols import UnsupportedDeploymentError

needs_openssl = pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl is not installed")
FILES = TlsFiles(ca_cert="/t/ca.crt", cert="/t/valkey.crt", key="/t/valkey.key")


def _build(tmp_path, says: str, name: str = "valkey-server") -> str:
    """A valkey-server that prints ``says`` and exits 1, as a build does when TLS replication has no certificate
    (``No tls-cert-file configured!``) or the build has no TLS (``Failed finding TLS support.``)."""

    binary = tmp_path / name
    binary.write_text(f"#!/bin/sh\necho '# {says}'\nexit 1\n")
    binary.chmod(0o755)
    return str(binary)


def _handshake(files: TlsFiles) -> None:
    """A TLS handshake in memory between a server and a client that each present ``files.cert``, the client
    verifying the server as valkey-py does (``ssl.create_default_context``, strict on Python 3.13 and later)."""

    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(files.cert, files.key)
    server_context.load_verify_locations(files.ca_cert)
    server_context.verify_mode = ssl.CERT_REQUIRED
    client_context = ssl.create_default_context(cafile=files.ca_cert)
    client_context.check_hostname = False
    client_context.load_cert_chain(files.cert, files.key)
    to_server, to_client = ssl.MemoryBIO(), ssl.MemoryBIO()
    server = server_context.wrap_bio(to_server, to_client, server_side=True)
    client = client_context.wrap_bio(to_client, to_server)
    pending = [client, server]
    for _ in range(10):
        for side in list(pending):
            try:
                side.do_handshake()
                pending.remove(side)
            except ssl.SSLWantReadError:
                pass
    assert not pending
    assert server.getpeercert()["subject"] == ((("commonName", "valkey-fuzzer"),),)


@needs_openssl
def test_a_deployment_makes_an_authority_and_a_certificate_both_sides_verify(tmp_path):
    files = create_tls_files(str(tmp_path / "tls"))

    for purpose in ("sslserver", "sslclient"):
        verify = ["openssl", "verify", "-purpose", purpose, "-CAfile", files.ca_cert, files.cert]
        assert subprocess.run(verify, capture_output=True, text=True).returncode == 0, purpose
    _handshake(files)


def test_a_tls_instance_serves_its_port_over_tls_only_and_replicates_and_gossips_over_tls(tmp_path):
    made = []
    with patch(
        "valkey_fuzzer.deployment.local.processes.create_tls_files", side_effect=lambda d: made.append(d) or FILES
    ):
        manager = process_manager(tmp_path, mode=DeploymentMode.CLUSTER, tls=True)
        command = manager.build_instance_command(7000, "/d", "/l", {"loglevel": "debug"})
        assert manager.build_instance_command(7001, "/d", "/l") and manager.tls_files() is FILES
    plain = process_manager(tmp_path / "plain").build_instance_command(7000, "/d", "/l")

    def arg(option, of=command):
        return of[of.index(option) + 1]

    # Made once, in the deployment's own directory, which cleanup removes.
    assert made == [str(tmp_path / f"deployment-{manager.deployment_id}" / "tls")]
    assert (arg("--port"), arg("--tls-port"), arg("--tls-replication"), arg("--tls-cluster")) == (
        "0",
        "7000",
        "yes",
        "yes",
    )
    assert (arg("--tls-cert-file"), arg("--tls-key-file"), arg("--tls-ca-cert-file")) == (
        FILES.cert,
        FILES.key,
        FILES.ca_cert,
    )
    assert arg("--port", plain) == "7000" and not [option for option in plain if option.startswith("--tls")]
    assert command[-2:] == ["--loglevel", "debug"]


def test_every_client_of_a_tls_instance_connects_with_the_deployments_certificates():
    assert client_options(None) == {}
    with (
        patch("valkey_fuzzer.deployment.local.queries.valkey.Valkey") as client,
        valkey_client("127.0.0.1", 7000, 1.0, tls=FILES),
    ):
        pass
    options = client.call_args.kwargs
    assert options["ssl"] and options["ssl_check_hostname"] is False
    assert (options["ssl_ca_certs"], options["ssl_certfile"], options["ssl_keyfile"]) == (
        FILES.ca_cert,
        FILES.cert,
        FILES.key,
    )
    # The topology and the deployment-state checks reach each node, from CLUSTER NODES too, with them.
    connection = DeploymentConnection([instance(tls=FILES), instance("valkey-1", "replica", 7001, tls=FILES)], "d")
    assert connection.tls is FILES and all(node["tls"] is FILES for node in connection.startup_nodes)


def test_a_build_without_tls_is_told_apart_without_starting_a_server(tmp_path):
    assert supports_tls(_build(tmp_path, "No tls-cert-file configured!", "with-tls"))
    assert not supports_tls(_build(tmp_path, "Failed finding TLS support.", "without-tls"))


@pytest.mark.parametrize("which", ["deployment", "replica"])
def test_a_tls_deployment_on_a_build_without_tls_is_unsupported_before_anything_starts(tmp_path, which):
    with_tls = _build(tmp_path, "No tls-cert-file configured!", "with-tls")
    without = _build(tmp_path, "Failed finding TLS support.", "without-tls")
    binaries = {"deployment": (without, {}), "replica": (with_tls, {"replica": without})}[which]
    config = DeploymentConfig(
        mode=DeploymentMode.STANDALONE,
        replicas_per_primary=1,
        base_data_dir=str(tmp_path / "data"),
        valkey_binary=binaries[0],
        instance_binaries=binaries[1],
        tls=True,
    )
    provider = LocalDeploymentProvider()
    context = Mock(deadline=None, plan=Mock(seed=1))

    unsupported = pytest.raises(
        UnsupportedDeploymentError, match=rf"needs a build with TLS \(BUILD_TLS=yes\): {without}"
    )
    with patch("valkey_fuzzer.deployment.local.provider.ProcessManager.spawn_all") as spawn, unsupported:
        provider._launch(config, plan_deployment(config), context)
    spawn.assert_not_called()


def test_an_instance_of_a_tls_deployment_refuses_a_build_without_tls_for_its_next_start(tmp_path):
    without = _build(tmp_path, "Failed finding TLS support.")
    config = DeploymentConfig(mode=DeploymentMode.STANDALONE, primaries=1, valkey_binary="/x", tls=True)
    handle = ValkeyDeploymentService(Mock(), LocalDeployment("d", config, [instance(tls=FILES)]))
    with pytest.raises(LocalDeploymentError, match="runs over TLS"):
        handle.set_binary("valkey-0", without)
    assert handle.binary("valkey-0") == "/x"


def _tls_build() -> str:
    if not BINARY:
        pytest.skip("valkey-server is not available (set VALKEY_FUZZER_BINARY or put it on PATH)")
    if not supports_tls(BINARY):
        pytest.skip(f"{BINARY} was built without TLS (BUILD_TLS=yes)")
    return BINARY


def _refuses_plain_text(node) -> bool:
    try:
        with valkey_client(node.host, node.port, timeout=2.0) as client:
            client.ping()
    except (valkey.ConnectionError, valkey.TimeoutError):
        return True
    return False


@pytest.mark.slow
@needs_openssl
def test_a_real_tls_cluster_forms_gossips_replicates_and_restarts_over_tls_only():
    with formed_cluster(3, 1, free_base_port(), valkey_binary=_tls_build(), tls=True) as cluster:
        nodes = cluster.connection.get_current_nodes(include_failed=False)
        assert len(nodes) == 6 and all(node.tls is cluster.processes.tls for node in nodes)
        replica = next(node for node in cluster.nodes if node.role == "replica")
        with valkey_client(replica.host, replica.port, timeout=2.0, tls=replica.tls) as client:
            assert query_replication(client).link_up
        assert all(_refuses_plain_text(node) for node in cluster.nodes)

        cluster.processes.restart_instance(replica, wait_ready=True, ready_timeout=30.0)
        with valkey_client(replica.host, replica.port, timeout=2.0, tls=replica.tls) as client:
            assert query_replication(client).link_up


@pytest.mark.slow
@needs_openssl
def test_a_real_tls_primary_and_replica_sync_over_tls(tmp_path):
    config = DeploymentConfig(
        mode=DeploymentMode.STANDALONE,
        replicas_per_primary=1,
        base_port=free_base_port(),
        base_data_dir=str(tmp_path),
        valkey_binary=_tls_build(),
        tls=True,
    )
    processes = ProcessManager(config)
    formation = Formation()
    nodes = processes.spawn_all(plan_deployment(config))
    try:
        formation.form(nodes, mode=DeploymentMode.STANDALONE)
        primary, replica = nodes
        with valkey_client(primary.host, primary.port, timeout=2.0, tls=primary.tls) as client:
            client.set("key", "value")
            assert client.wait(1, 5000) == 1
        with valkey_client(replica.host, replica.port, timeout=2.0, tls=replica.tls) as client:
            assert client.get("key") == "value" and query_replication(client).link_up
        assert _refuses_plain_text(primary) and _refuses_plain_text(replica)
    finally:
        formation.close_connections()
        processes.cleanup_deployment(nodes)
