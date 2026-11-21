"""Test doubles shared by the unit tests: a client that is a context manager and records commands, and a deployment handle that answers
the role and instance questions from its ``instances`` and stands in for their processes."""

import os
import signal
from types import SimpleNamespace


class FakeClient:
    """A client the code under test opens with ``with`` or closes: it enters as itself and closes to nothing.
    ``execute_command`` appends its arguments to ``commands`` and answers ``replies.get(args, "OK")``, raising an
    exception reply. Its ``connection_pool`` hands out and takes back a connected stand-in: a test of a client
    that cannot connect gives its own ``connection_pool``."""

    connection_pool = SimpleNamespace(get_connection=lambda *args, **options: None, release=lambda conn: None)

    def __init__(self, commands=None, replies=None):
        self.commands = [] if commands is None else commands
        self.replies = replies or {}

    def execute_command(self, *args):
        self.commands.append(args)
        reply = self.replies.get(args, "OK")
        if isinstance(reply, Exception):
            raise reply
        return reply

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def close(self):
        pass


def fake_instances(count, role=lambda n: "replica" if n else "primary", **fields):
    """``count`` instances ``valkey-0``... ; ``role(n)`` is the role of the n-th, and each field is a value or a
    function of ``n`` (``port=lambda n: 7000 + n``)."""

    return tuple(
        SimpleNamespace(
            instance_id=f"valkey-{n}",
            role=role(n),
            **{name: value(n) if callable(value) else value for name, value in fields.items()},
        )
        for n in range(count)
    )


class FakeHandle:
    """A deployment handle whose ``instances`` (each with an ``instance_id`` and a ``role``) say who is a primary
    and who a replica, and whose processes are stand-ins: every pid is ``process_id``, every start was at
    ``started``, a data directory is under ``root``. ``alive`` says which instances run: ``send_signal`` records
    ``(instance_id, signum)`` in ``signals`` and a SIGKILL or SIGTERM stops the instance until ``restart``
    (recorded in ``restarted``) starts it again. A subclass overrides ``roles`` when roles change, and
    ``send_signal`` when it stands in for a deployment that raises ``ProcessLookupError`` for a stopped process."""

    process_id = 4242
    started = 0.0

    def __init__(self, instances=(), root=""):
        self.instances = tuple(instances)
        self.alive = dict.fromkeys((node.instance_id for node in self.instances), True)
        self.root, self.signals, self.restarted = root, [], []

    def roles(self):
        return {node.instance_id: node.role for node in self.instances}

    def primaries(self):
        return [node_id for node_id, role in self.roles().items() if role == "primary"]

    def replicas(self):
        return [node_id for node_id, role in self.roles().items() if role == "replica"]

    def instance(self, instance_id):
        return next(node for node in self.instances if node.instance_id == instance_id)

    def pid(self, instance_id):
        return self.process_id

    def is_alive(self, instance_id):
        return self.alive[instance_id]

    def wait_exit(self, instance_id, timeout=10.0):
        return not self.alive[instance_id]

    def send_signal(self, instance_id, signum):
        self.signals.append((instance_id, signum))
        if signum in (signal.SIGKILL, signal.SIGTERM):
            self.alive[instance_id] = False

    def restart(self, instance_id, wait_ready=True, ready_timeout=30.0):
        self.alive[instance_id] = True
        self.restarted.append(instance_id)

    def started_at(self, instance_id):
        return self.started

    def data_dir(self, instance_id):
        return os.path.join(self.root, instance_id)
