"""Doubles for the key-value workload's ledger tests: a primary that stores what it is told and answers WAIT, and a
standalone deployment handle over one such client."""

import time
from types import SimpleNamespace


class StoreClient:
    """A primary that stores what it is told and answers WAIT with ``wait_reply``, slowly."""

    def __init__(self, wait_reply=1):
        self.store, self.wait_reply, self.commands, self.waits_sent_at = {}, wait_reply, [], []

    def set(self, key, value):
        self.store[key] = value
        return True

    def delete(self, key):
        return int(self.store.pop(key, None) is not None)

    def get(self, key):
        return self.store.get(key)

    def pipeline(self, transaction=False):
        return SimpleNamespace(set=self.set, execute=lambda: [True] * len(self.store))

    def execute_command(self, *args):
        self.commands.append(args)
        self.waits_sent_at.append(time.time())
        time.sleep(0.01)
        return self.wait_reply

    def close(self):
        pass


def standalone(client, primaries=("valkey-0",)):
    instances = (SimpleNamespace(instance_id="valkey-0", host="127.0.0.1", port=7000),)
    mode = SimpleNamespace(value="standalone")
    return SimpleNamespace(
        mode=mode, instances=instances, primaries=lambda: list(primaries), new_client=lambda *a, **k: client
    )
