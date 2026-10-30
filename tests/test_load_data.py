"""The cluster slot preload writes the same keys into every slot for the same seed."""

import random
from types import SimpleNamespace
from unittest.mock import Mock, patch

from valkey.crc import key_slot

from valkey_fuzzer.deployment.local.models import DeploymentMode
from valkey_fuzzer.deployment.local.service import load_all_slots


def test_the_same_seed_preloads_the_same_key_into_every_slot():
    cluster = SimpleNamespace(mode=DeploymentMode.CLUSTER, startup_nodes=[{"host": "127.0.0.1", "port": 7000}])
    clients = [Mock(), Mock()]
    with patch("valkey_fuzzer.deployment.local.service.ValkeyCluster", side_effect=clients):
        for _ in clients:
            assert load_all_slots(cluster, keys_per_slot=1, rng=random.Random(1234)) is True
    first, second = ([call.args[0] for call in client.set.call_args_list] for client in clients)
    assert first == second
    assert [key_slot(key.encode()) for key in first] == list(range(16384))
    # Outside cluster mode there are no slots to fill.
    assert load_all_slots(SimpleNamespace(mode=DeploymentMode.STANDALONE), keys_per_slot=1, rng=random.Random(0))
