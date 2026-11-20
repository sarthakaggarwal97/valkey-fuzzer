"""Test doubles shared by the unit tests: a client that is a context manager and records commands, and a deployment handle that answers
the role and instance questions from its ``instances``."""

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
    and who a replica; a subclass overrides ``roles`` when roles change."""

    instances = ()

    def roles(self):
        return {node.instance_id: node.role for node in self.instances}

    def primaries(self):
        return [node_id for node_id, role in self.roles().items() if role == "primary"]

    def replicas(self):
        return [node_id for node_id, role in self.roles().items() if role == "replica"]

    def instance(self, instance_id):
        return next(node for node in self.instances if node.instance_id == instance_id)
