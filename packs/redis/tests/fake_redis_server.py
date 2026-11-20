"""A fake ``redis-server`` for the pack's tests: enough of the command line and of RESP for the provider, the
workload, the fault, and the oracles, in one process a test can kill.

It takes ``--name value`` options like the real one (``--port``, ``--dir``, ``--logfile``, ``--appendonly``,
the rest ignored), logs ``Ready to accept connections`` in Redis's log line format, and answers PING, INFO
(``process_id``, ``loading``), SET, GET, MGET, CLIENT, and QUIT. With ``--appendonly yes`` every SET is written
to ``<dir>/fake-appendonly.json`` first and read back at start, so a kill loses nothing acknowledged; without it
a restart starts empty, as Redis does without persistence. ``--fake-exit-after N`` ends the process with exit
code 1 after N SETs, logging a Redis crash report, for the tests of the crash oracle.
"""

import json
import os
import signal
import socketserver
import sys
import time
from pathlib import Path

OPTIONS = {"port": "6379", "bind": "127.0.0.1", "dir": ".", "logfile": "", "appendonly": "no", "fake-exit-after": ""}


def log(message, level="*"):
    line = f"{os.getpid()}:M {time.strftime('%d %b %Y %H:%M:%S')}.000 {level} {message}\n"
    if OPTIONS["logfile"]:
        with open(OPTIONS["logfile"], "a", encoding="utf-8") as handle:
            handle.write(line)
    else:
        sys.stdout.write(line)
        sys.stdout.flush()


class Store:
    def __init__(self):
        self.data = {}
        self.sets = 0
        self.path = Path(OPTIONS["dir"]) / "fake-appendonly.json" if OPTIONS["appendonly"] == "yes" else None
        if self.path is not None and self.path.exists():
            self.data = json.loads(self.path.read_text(encoding="utf-8"))
            log(f"DB loaded from append only file: {len(self.data)} keys")

    def set(self, key, value):
        self.data[key] = value
        self.sets += 1
        if self.path is not None:
            temporary = self.path.with_suffix(".tmp")
            temporary.write_text(json.dumps(self.data), encoding="utf-8")
            os.replace(temporary, self.path)
            with open(self.path, "rb") as handle:
                os.fsync(handle.fileno())
        limit = OPTIONS["fake-exit-after"]
        if limit and self.sets >= int(limit):
            log("=== REDIS BUG REPORT START: Cut & paste starting from here ===", "#")
            log("Redis 0.0.0 crashed by signal: 11, si_code: 0", "#")
            log("Crashed running the instruction at: 0x0", "#")
            os._exit(1)


STORE = None


def bulk(value):
    if value is None:
        return b"$-1\r\n"
    data = value.encode("utf-8") if isinstance(value, str) else value
    return b"$" + str(len(data)).encode() + b"\r\n" + data + b"\r\n"


class Handler(socketserver.StreamRequestHandler):
    def read_command(self):
        head = self.rfile.readline()
        if not head:
            return None
        if not head.startswith(b"*"):
            return head.strip().split()
        parts = []
        for _ in range(int(head[1:])):
            length = int(self.rfile.readline()[1:])
            parts.append(self.rfile.read(length))
            self.rfile.read(2)
        return parts

    def handle(self):
        while True:
            command = self.read_command()
            if command is None:
                return
            name = command[0].upper()
            args = [part.decode("utf-8") for part in command[1:]]
            if name == b"PING":
                reply = b"+PONG\r\n"
            elif name == b"INFO":
                reply = bulk(
                    f"# Server\r\nprocess_id:{os.getpid()}\r\nredis_version:0.0.0\r\n# Persistence\r\nloading:0\r\n"
                )
            elif name == b"SET":
                STORE.set(args[0], args[1])
                reply = b"+OK\r\n"
            elif name == b"GET":
                reply = bulk(STORE.data.get(args[0]))
            elif name == b"MGET":
                reply = b"*" + str(len(args)).encode() + b"\r\n" + b"".join(bulk(STORE.data.get(key)) for key in args)
            elif name in (b"CLIENT", b"CONFIG"):
                reply = b"+OK\r\n"
            elif name == b"QUIT":
                self.wfile.write(b"+OK\r\n")
                return
            else:
                reply = b"-ERR unknown command '" + name + b"'\r\n"
            self.wfile.write(reply)


def main(argv):
    if argv == ["--version"]:
        print("Fake Redis server v=0.0.0 sha=00000000:0")
        return 0
    for index in range(0, len(argv) - 1, 2):
        OPTIONS[argv[index][2:]] = argv[index + 1]
    global STORE
    STORE = Store()
    socketserver.ThreadingTCPServer.allow_reuse_address = True
    socketserver.ThreadingTCPServer.daemon_threads = True
    server = socketserver.ThreadingTCPServer((OPTIONS["bind"], int(OPTIONS["port"])), Handler)

    def shutdown(signum, frame):
        log("User requested shutdown...", "#")
        os._exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    log(f"Ready to accept connections tcp on port {OPTIONS['port']}")
    server.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
