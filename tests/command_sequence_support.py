"""Doubles for the tests of the components that drive fake private servers: builds of a fake ``valkey-server``
whose reply to each command a test sets, the servers they start, and a valkey-py connection to one, so arguments go
out as bytes and replies come back as the server sent them."""

import hashlib
import os
import shutil
import subprocess
import tempfile

import libvalkey
from valkey.exceptions import ConnectionError as ValkeyConnectionError
from valkey.exceptions import InvalidResponse, ResponseError
from valkey.exceptions import TimeoutError as ValkeyTimeoutError

from valkey_fuzzer.components import command_sequence

# What a fake build does with a command (see Build).
DIE, TIMEOUT, SILENT, CLOSE, EXIT = "<die>", "<timeout>", "<silent>", "<close>", "<exit>"


class Error(str):
    """An error reply."""


class ByResp(dict):
    """A reply by the protocol of the connection it goes to: ``{2: ..., 3: ...}``."""


class Wire(bytes):
    """A reply's bytes as the server sends them, which the client parses as valkey-py does, with libvalkey."""


class Build:
    """A fake valkey-server build: the options it rejects at start, and its reply to each command, keyed by
    the arguments as sent (strings, or bytes for a binary argument; raw bytes as they are); an unlisted command
    gets ``OK`` (CLUSTER MYID the server's ID, CLUSTER MYSHARDID it reversed, CLUSTER INFO a state ok knowing every server of the build;
    SUBSCRIBE and PUBLISH work, on the connections of one server). A
    reply may be an :class:`Error`, a :class:`Wire`, :data:`DIE` (the server exits without answering), :data:`CLOSE` (it closes the
    connection without answering), :data:`TIMEOUT`,
    :data:`SILENT` (no reply begins), or a function of the :class:`FakeServer` that returns the reply."""

    def __init__(self, replies=None, rejects=()):
        self.replies, self.rejects = dict(replies or {}), set(rejects)
        # Its servers exit at a second start, as from a config file they cannot read.
        self.restart_exits = False
        # Its servers log a fatal RDB-read refusal and exit when started with a dump.rdb, as a build hardened
        # to reject bad data does; not a crash.
        self.rdb_load_exits = False
        self.starts: list[list[str]] = []
        self.sent: list[tuple] = []
        self.connections = 0
        # The connections (numbered from 1, across its servers) it closes in the handshake, as it opens them.
        self.closes_at_connect: set[int] = set()


BUILDS: dict[str, Build] = {}
SERVERS: dict[str, "FakeServer"] = {}


class FakeProcess:
    def __init__(self):
        self.returncode = None

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        if self.returncode is None and timeout is not None:
            raise subprocess.TimeoutExpired("valkey-server", timeout)
        return self.returncode


class FakeServer:
    """Stands in for PrivateServer: starts unless its build rejects an option, and logs a line."""

    def __init__(self, binary, arguments=(), *, timeout_s=15.0, prefix="x-", files=None, config_file=False):
        self.binary, self.arguments, self.build = binary, list(arguments), BUILDS[binary]
        self.files, self.config_file, self.shut_downs = dict(files or {}), config_file, 0
        self.workdir = tempfile.mkdtemp(prefix=prefix)
        # A node of a side, not a probe of whether the build starts.
        self.node = prefix == "vfc-"
        self.id = hashlib.sha1(self.workdir.encode()).hexdigest()
        self.port = int(self.option("port") or 0)
        self.sent: list[tuple] = []
        self.wires: list[FakeWire] = []
        self.socket_path = os.path.join(self.workdir, "s.sock")
        self.log_path = os.path.join(self.workdir, "server.log")
        self.output_path = os.path.join(self.workdir, "output.log")
        self.process, self.client, self.stopped = FakeProcess(), None, False

    def start(self, context=None):
        self.build.starts.append(self.arguments)
        if self.shut_downs and self.build.restart_exits:
            with open(self.log_path, "a") as log:
                log.write("1:M 03 Nov 2026 10:00:02.000 # Fatal error, can't open config file 'valkey.conf'\n")
            self.process.returncode = 1
            return "Fatal error, can't open config file 'valkey.conf'"
        rejected = [name for name in self.arguments[::2] if name[2:] in self.build.rejects]
        if rejected:
            return f"*** FATAL CONFIG FILE ERROR *** Bad directive or wrong number of arguments: {rejected[0]}"
        if self.build.rdb_load_exits and self.files.get("dump.rdb"):
            with open(self.log_path, "a") as log:
                if isinstance(self.build.rdb_load_exits, str):
                    log.write(self.build.rdb_load_exits)
                log.write("1:M 03 Nov 2026 10:00:03.000 # Terminating server after rdb file reading failure.\n")
            self.process.returncode = 1
            return "Terminating server after rdb file reading failure."
        with open(self.log_path, "w") as log:
            log.write(f"1:M 03 Nov 2026 10:00:00.000 * Ready to accept connections ({self.binary})\n")
        with open(self.output_path, "w") as output:
            output.write("")
        SERVERS[self.socket_path] = self
        return None

    def shut_down(self, timeout_s):
        self.shut_downs += 1
        return True

    def stop(self):
        self.stopped = True
        shutil.rmtree(self.workdir, ignore_errors=True)

    def option(self, name):
        names = self.arguments[::2]
        return self.arguments[names.index(f"--{name}") * 2 + 1] if f"--{name}" in names else None

    def die(self):
        with open(self.log_path, "a") as log:
            log.write("1:M 03 Nov 2026 10:00:01.000 # Valkey 9.0.0 crashed by signal: 11, si_code: 0\n")
        self.process.returncode = -11

    def exit_cleanly(self):
        # A commanded SHUTDOWN: the server logs a normal goodbye and exits 0, no crash report.
        with open(self.log_path, "a") as log:
            log.write("1:M 03 Nov 2026 10:00:01.000 * Valkey is now ready to exit, bye bye...\n")
        self.process.returncode = 0


class FakeWire:
    """A valkey-py connection to one fake server: ``send_command``, ``send_packed_command``, ``can_read``,
    ``read_response``, ``disconnect``, and its own ``_sock``, which ``recv`` a :class:`Wire` reply's bytes."""

    def __init__(self, server, resp=2):
        self.server, self.pending, self.resp = server, None, resp
        self._sock = self
        # Its channels, and the messages published to them, waiting to be read.
        self.channels: set[str] = set()
        self.waiting: list = []
        server.build.connections += 1
        server.wires.append(self)

    def send_command(self, *args):
        self.pending = tuple(item if isinstance(item, bytes) else str(item) for item in args)
        self.server.build.sent.append(self.pending)
        self.server.sent.append(self.pending)

    def send_packed_command(self, command, check_health=True):
        self.pending = b"".join(command)
        self.server.build.sent.append(self.pending)
        self.server.sent.append(self.pending)

    def can_read(self, timeout=0):
        if self.pending is None:
            return bool(self.waiting)
        return self.server.build.replies.get(self.pending) != SILENT

    def read_response(self, push_request=False):
        # What was delivered before the command comes first, as on a socket.
        if self.pending is None or self.waiting:
            return self.waiting.pop(0)
        pending, self.pending = self.pending, None
        known = len(nodes_of(self.server.build))
        defaults = {
            ("CLUSTER", "MYID"): self.server.id.encode(),
            ("CLUSTER", "MYSHARDID"): self.server.id[::-1].encode(),
            ("CLUSTER", "INFO"): f"cluster_state:ok\r\ncluster_known_nodes:{known}\r\n".encode(),
        }
        if pending[0] == "SUBSCRIBE":
            self.channels.add(pending[1])
            defaults[pending] = [b"subscribe", pending[1].encode(), len(self.channels)]
        if pending[0] == "UNSUBSCRIBE":
            self.channels.discard(pending[1])
            defaults[pending] = [b"unsubscribe", pending[1].encode(), len(self.channels)]
        if pending[0] == "PUBLISH":
            receivers = [wire for wire in self.server.wires if pending[1] in wire.channels]
            for wire in receivers:
                wire.waiting.append([b"message", pending[1].encode(), pending[2].encode()])
            defaults[pending] = len(receivers)
        reply = self.server.build.replies.get(pending, defaults.get(pending, b"OK"))
        if callable(reply):
            reply = reply(self.server)
        if isinstance(reply, ByResp):
            reply = reply[self.resp]
        if reply == DIE:
            self.server.die()
            raise ValkeyConnectionError("Connection closed by server.")
        if reply == EXIT:
            self.server.exit_cleanly()
            raise ValkeyConnectionError("Connection closed by server.")
        if reply == TIMEOUT:
            raise ValkeyTimeoutError("Timeout reading from socket")
        if reply == CLOSE:
            raise ValkeyConnectionError("Connection closed by server.")
        if isinstance(reply, Error):
            raise ResponseError(str(reply))
        if isinstance(reply, Wire):
            reader = libvalkey.Reader(protocolError=InvalidResponse, replyError=ResponseError)
            reader.feed(reply)
            return reader.gets()
        return reply

    def settimeout(self, timeout):
        pass

    def recv(self, size):
        pending, self.pending = self.pending, None
        return bytes(self.server.build.replies.get(pending, b""))[:size] if pending is not None else b""

    def disconnect(self):
        pass


def fake_open_raw_connection(resp, timeout_s, *, path=None, host=None, port=None):
    server = SERVERS[path] if path is not None else SERVERS[f"{host}:{port}"]
    wire = FakeWire(server, resp)
    if server.build.connections in server.build.closes_at_connect:
        raise ValkeyConnectionError("Connection closed by server.")
    return command_sequence.RawConnection(wire)


def nodes_of(build):
    """The fake servers of ``build`` in the order they started: its nodes."""

    return [server for server in SERVERS.values() if server.build is build and server.node]
