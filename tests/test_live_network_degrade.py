"""``valkey.fault.network-degrade``'s tc netem rules against the real kernel: they slow exactly the flows between
the two addresses they name, nothing else on ``lo``, and healing (and the sweep, for a dead owner's device)
leaves ``lo`` as it was found.

Needs Linux, ``tc`` and ``ip`` with ``CAP_NET_ADMIN``, and 127.0.0.x addresses; skipped otherwise (CI's
server-tests job runs as a user without it, the daily live workflow's root step runs it).
"""

import socket
import subprocess
import threading
import time

import pytest

from valkey_fuzzer import hoststate
from valkey_fuzzer.components import capabilities, network
from valkey_fuzzer.hoststate import Owner, sweep

pytestmark = pytest.mark.slow

NETEM = capabilities.netem_supported()
LOOPBACK = capabilities.per_instance_loopback_supported()
if not NETEM[0]:
    pytest.skip(NETEM[1], allow_module_level=True)
if not LOOPBACK[0]:
    pytest.skip(LOOPBACK[1], allow_module_level=True)

A, B, C = "127.0.0.2", "127.0.0.3", "127.0.0.4"
DELAY_MS = 150


def _echo_server(address):
    """A TCP echo server on ``address``; its port and a stop function."""

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((address, 0))
    server.listen(8)
    server.settimeout(0.2)
    stop = threading.Event()

    def serve():
        while not stop.is_set():
            try:
                connection, _ = server.accept()
            except socket.timeout:
                continue
            with connection:
                connection.settimeout(5)
                try:
                    while True:
                        data = connection.recv(64)
                        if not data:
                            break
                        connection.sendall(data)
                except OSError:
                    pass

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()

    def close():
        stop.set()
        thread.join(timeout=5)
        server.close()

    return server.getsockname()[1], close


def _round_trip_ms(source, destination, port):
    """One connect, one echo of a byte, from ``source`` to ``destination:port``, in milliseconds."""

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.settimeout(10)
    client.bind((source, 0))
    started = time.monotonic()
    client.connect((destination, port))
    client.sendall(b"x")
    assert client.recv(1) == b"x"
    client.close()
    return (time.monotonic() - started) * 1000


def _lo_filters():
    return subprocess.run(["tc", "filter", "show", "dev", "lo", "ingress"], capture_output=True, text=True).stdout


def _ifb_devices():
    listing = subprocess.run(["ip", "-o", "link", "show", "type", "ifb"], capture_output=True, text=True).stdout
    return [line.split(":", 2)[1].strip() for line in listing.splitlines() if len(line.split(":", 2)) > 2]


def _lo_has_ingress():
    return "ingress" in subprocess.run(["tc", "qdisc", "show", "dev", "lo"], capture_output=True, text=True).stdout


def test_a_degradation_slows_only_the_flows_between_its_addresses_and_healing_leaves_lo_as_found():
    had_ingress = _lo_has_ingress()
    port_b, close_b = _echo_server(B)
    port_c, close_c = _echo_server(C)
    degradation = network.Degradation({"a": A, "b": B, "c": C}, ["a"], ["b"], ["delay", f"{DELAY_MS}ms"])
    try:
        baseline = _round_trip_ms(A, B, port_b)
        assert baseline < DELAY_MS, f"loopback already takes {baseline:.0f} ms"
        device = degradation.create_device()
        assert device in _ifb_devices() and hoststate.netem_device_owner(device) == hoststate.current_owner()
        rules = degradation.install()
        assert rules == [f"{A}->{B}", f"{B}->{A}"]
        filters = _lo_filters()
        assert filters.count(f"Redirect to device {device}") == 2 and f"pref {degradation.pref} " in filters

        # A connect (SYN, SYN-ACK) and an echo (data, reply) each cross the delayed link twice.
        slowed = min(_round_trip_ms(A, B, port_b) for _ in range(2))
        assert slowed >= 2 * DELAY_MS, f"the degraded flow took {slowed:.0f} ms"
        # The same client address to another instance, and the harness's own address to the degraded one, are not.
        assert _round_trip_ms(A, C, port_c) < DELAY_MS
        assert _round_trip_ms("127.0.0.1", B, port_b) < DELAY_MS

        degradation.remove()
        assert _round_trip_ms(A, B, port_b) < DELAY_MS
        assert device not in _ifb_devices() and f"Redirect to device {device}" not in _lo_filters()
        assert _lo_has_ingress() is had_ingress, "lo's ingress qdisc is as it was found"
        degradation.remove()  # idempotent
    finally:
        if degradation.device in _ifb_devices():
            degradation.remove()
        close_b()
        close_c()


def test_the_sweep_removes_a_dead_owners_device_and_its_filters_and_leaves_a_live_ones():
    had_ingress = _lo_has_ingress()
    dead = Owner(999_999, "0a1b2c")
    assert not dead.alive()
    dead_device = f"vf{dead.pid:06x}{dead.start}0"
    live = network.Degradation({"a": A, "c": C}, ["a"], ["c"], ["loss", "1%"])
    try:
        subprocess.run(["ip", "link", "add", "dev", dead_device, "type", "ifb"], check=True)
        subprocess.run(["ip", "link", "set", "dev", dead_device, "up"], check=True)
        subprocess.run(["tc", "qdisc", "add", "dev", dead_device, "root", "netem", "delay", "10ms"], check=True)
        with hoststate.lo_ingress_lock():
            assert hoststate.ensure_lo_ingress() is None
        subprocess.run(
            ["tc", "filter", "add", "dev", "lo", "ingress", "protocol", "ip", "pref", "60001", "handle", "1", "flower",
             "ip_proto", "tcp", "src_ip", A, "dst_ip", B, "action", "mirred", "egress", "redirect", "dev", dead_device],
            check=True,
        )  # fmt: skip
        live.create_device()
        live.install()

        report = sweep([])
        netem = [(item.target, item.action, item.reason) for item in report.leftovers if item.kind == "netem"]
        assert netem == [(dead_device, "removed", "")], report.to_data()
        filters = _lo_filters()
        assert dead_device not in _ifb_devices() and dead_device not in filters
        assert live.device in _ifb_devices() and filters.count(f"Redirect to device {live.device}") == 2
        assert _lo_has_ingress()

        live.remove()
        assert _lo_has_ingress() is had_ingress and live.device not in _ifb_devices()
    finally:
        subprocess.run(
            ["tc", "filter", "del", "dev", "lo", "ingress", "protocol", "ip", "pref", "60001"], capture_output=True
        )
        subprocess.run(["ip", "link", "del", "dev", dead_device], capture_output=True)
        if live.device in _ifb_devices():
            live.remove()
