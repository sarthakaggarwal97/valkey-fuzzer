"""The keyspace snapshot: a client that pipelines is read in two round trips per SCAN batch, and gives the same
snapshot, in the same order, as reading each key with ``canonical_value``."""

import json

import pytest

from valkey_fuzzer.components.keyspace import canonical_value, keyspace_digest, keyspace_snapshot

# Every read the snapshot sends: each is one round trip on a client that does not pipeline.
_READS = ("type", "get", "hgetall", "lrange", "smembers", "zrange", "xrange", "dump")


class _Keyspace:
    """A keyspace of mixed types: ``keys`` maps a key to its ``(type, data)`` as the client's read of that type
    returns it; type ``none`` is a key gone after its SCAN, a module type with ``None`` data one gone before its
    DUMP. Every command is a round trip; SCAN pages ``count`` keys per round trip."""

    def __init__(self, keys):
        self.keys = dict(keys)
        self.round_trips = 0

    def scan_iter(self, match=None, count=1000):
        keys = list(self.keys)
        for start in range(0, len(keys), count):
            self.round_trips += 1
            yield from keys[start : start + count]

    def _reply(self, command, key):
        kind, data = self.keys[key]
        return kind if command == "type" else data

    def __getattr__(self, command):
        if command not in _READS:
            raise AttributeError(command)

        def send(key, *args, **kwargs):
            self.round_trips += 1
            return self._reply(command, key)

        return send


class _Pipeline:
    """Queues the client's reads and answers them all on ``execute``: one round trip, of a recorded size."""

    def __init__(self, client):
        self.client, self.queued = client, []

    def __getattr__(self, command):
        if command not in _READS:
            raise AttributeError(command)

        def queue(key, *args, **kwargs):
            self.queued.append((command, key))
            return self

        return queue

    def execute(self):
        if self.queued:
            self.client.round_trips += 1
            self.client.pipeline_sizes.append(len(self.queued))
        return [self.client._reply(command, key) for command, key in self.queued]

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.queued = []


class _PipeliningKeyspace(_Keyspace):
    def __init__(self, keys):
        super().__init__(keys)
        self.pipeline_sizes = []

    def pipeline(self, transaction=True):
        assert transaction is False, "the snapshot's reads need no MULTI/EXEC"
        return _Pipeline(self)


def _mixed_keys(total, encode=lambda value: value):
    """``total`` keys cycling through every type the snapshot reads, with a key gone after its SCAN and a module
    value gone before its DUMP; ``encode`` turns each text into what a non-decoding client returns."""

    e = encode
    keys = {}
    for i in range(total):
        kind = i % 9
        if kind == 0:
            keys[e(f"k:{i}")] = (e("string"), e(f"value{i}"))
        elif kind == 1:
            keys[e(f"k:{i}")] = (e("hash"), {e("b"): e("2"), e("a"): e(f"{i}")})
        elif kind == 2:
            keys[e(f"k:{i}")] = (e("list"), [e("z"), e("y"), e(f"{i}")])
        elif kind == 3:
            keys[e(f"k:{i}")] = (e("set"), {e("m2"), e("m1"), e(f"{i}")})
        elif kind == 4:
            keys[e(f"k:{i}")] = (e("zset"), [(e("one"), 1.0), (e("two"), 2.5)])
        elif kind == 5:
            keys[e(f"k:{i}")] = (e("stream"), [(e("1-0"), {e("f"): e(f"{i}")}), (e("2-0"), {e("g"): e("v")})])
        elif kind == 6:
            keys[e(f"k:{i}")] = (e("test___dt"), b"\x07module" + b"\x0b\x00" + b"\x01" * 8)
        elif kind == 7:
            keys[e(f"k:{i}")] = (e("none"), None)
        else:
            keys[e(f"k:{i}")] = (e("test___dt"), None)
    return keys


def _per_key(client):
    snapshot = {}
    for key in list(client.keys):
        value = canonical_value(client, key)
        if value is not None:
            snapshot[key if isinstance(key, str) else key.decode()] = value
    return snapshot


@pytest.mark.parametrize("encode", [lambda value: value, str.encode], ids=["decoding", "bytes"])
def test_a_pipelining_client_is_read_in_two_round_trips_per_batch_and_gives_the_per_key_snapshot(encode):
    keys = _mixed_keys(2500, encode)
    per_key, pipelined = _Keyspace(keys), _PipeliningKeyspace(keys)

    expected = keyspace_snapshot(per_key, count=1000)
    snapshot = keyspace_snapshot(pipelined, count=1000)

    # Byte-identical: the same canonical values in SCAN order, so ledgers and digests do not change.
    assert json.dumps(snapshot, ensure_ascii=False) == json.dumps(expected, ensure_ascii=False)
    assert list(snapshot) == list(expected) and keyspace_digest(snapshot) == keyspace_digest(expected)
    assert expected == _per_key(_Keyspace(keys))
    assert len(snapshot) == 2500 - 2500 // 9 * 2, "keys gone after SCAN or before DUMP are left out"
    # 3 SCAN pages; then TYPE and one read per key, against one TYPE pipeline and one read pipeline per batch.
    assert per_key.round_trips == 3 + 2 * 2500 - 2500 // 9
    assert pipelined.round_trips == 3 + 2 * 3
    assert max(pipelined.pipeline_sizes) <= 1000


def test_a_pipelining_client_reads_a_module_value_by_its_dump_and_a_binary_hash_exactly():
    vector = b"\x00\x00\x80?\xc3\x28\x00\x00"
    payload = b"\x07value" + b"\x0b\x00" + b"\x01" * 8
    keys = {b"d:1": (b"hash", {b"v": vector, b"tag": b"t1"}), b"m": (b"test___dt", payload)}

    snapshot = keyspace_snapshot(_PipeliningKeyspace(keys))

    assert snapshot == keyspace_snapshot(_Keyspace(keys))
    assert snapshot["d:1"] == ["hash", [["tag", "t1"], ["v", {"$base64": "AACAP8MoAAA="}]]]
    assert snapshot["m"] == ["test___dt", canonical_value(_Keyspace(keys), b"m")[1]]


def test_a_batch_whose_keys_are_all_gone_sends_no_read_pipeline():
    client = _PipeliningKeyspace({f"k:{i}": ("none", None) for i in range(10)})

    assert keyspace_snapshot(client, count=4) == {}
    # 3 SCAN pages and 3 TYPE pipelines; nothing to read.
    assert client.round_trips == 6 and client.pipeline_sizes == [4, 4, 2]


def test_a_client_without_pipelines_is_read_key_by_key():
    keys = _mixed_keys(18)
    client = _Keyspace(keys)

    assert keyspace_snapshot(client, count=1000) == _per_key(_Keyspace(keys))
    assert client.round_trips == 1 + 2 * 18 - 2
