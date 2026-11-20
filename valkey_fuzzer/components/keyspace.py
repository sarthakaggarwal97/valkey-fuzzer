"""An instance's keyspace as canonical data, and its digest."""

import base64
import hashlib
import json
from collections.abc import Iterable, Iterator, Mapping
from itertools import islice
from typing import Any, Optional

# The types canonical_value reads as data; any other is a module's.
BUILTIN_TYPES = frozenset({"string", "hash", "list", "set", "zset", "stream"})
# A DUMP payload ends with the RDB version (2 bytes) and a CRC64 (8 bytes) over the rest, which tell the build
# that wrote it, not the value.
_DUMP_TRAILER_BYTES = 10


def dump_body(payload: bytes) -> bytes:
    """A DUMP payload without its RDB version and CRC64: the value as its type serializes it."""

    return payload[:-_DUMP_TRAILER_BYTES]


def exact(value: Any) -> Any:
    """A reply as data that keeps every byte: bytes as the string they decode to as UTF-8, else
    ``{"$base64": ...}``; a list element by element. Values a decoding client read stay as they are."""

    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return {"$base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, (list, tuple)):
        return [exact(item) for item in value]
    return value


def _ordered(items: Any) -> list:
    return sorted(items, key=lambda item: json.dumps(item, sort_keys=True))


def _key(key: Any) -> str:
    """A key as a snapshot names it: its text, or ``$base64:...`` for a key that is not UTF-8."""

    named = exact(key)
    return named if isinstance(named, str) else f"$base64:{named['$base64']}"


def _read(target: Any, kind: str, key: Any) -> Any:
    """The one read of a key of type ``kind``, sent to ``target``: a client answers it, a pipeline queues it."""

    if kind == "string":
        return target.get(key)
    if kind == "hash":
        return target.hgetall(key)
    if kind == "list":
        return target.lrange(key, 0, -1)
    if kind == "set":
        return target.smembers(key)
    if kind == "zset":
        return target.zrange(key, 0, -1, withscores=True)
    if kind == "stream":
        return target.xrange(key)
    # DUMP is never decoded, so a decoding client returns the payload as bytes.
    return target.dump(key)


def _canonical(kind: str, reply: Any) -> Optional[list]:
    """``[kind, content]`` from the reply to :func:`_read`, or None for a module value gone before its DUMP.

    The content is JSON data as :func:`exact` returns it: a reply's bytes become str or ``{"$base64": ...}``, its
    tuples lists, and a str, int, float or None stays; a zset score is ``repr``ed, a DUMP a hex digest. Nothing walks
    it again.
    """

    if kind == "string":
        content: Any = exact(reply)
    elif kind == "hash":
        content = _ordered([exact(field), exact(value)] for field, value in reply.items())
    elif kind == "list":
        content = exact(reply)
    elif kind == "set":
        content = _ordered(exact(member) for member in reply)
    elif kind == "zset":
        content = [[exact(member), repr(score)] for member, score in reply]
    elif kind == "stream":
        content = [
            [exact(entry_id), _ordered([exact(field), exact(value)] for field, value in fields.items())]
            for entry_id, fields in reply
        ]
    else:
        if reply is None:
            return None
        content = hashlib.sha256(dump_body(reply)).hexdigest()
    return [kind, content]


def canonical_value(client: Any, key: Any) -> Optional[list]:
    """Return ``[type, content]`` with order-insensitive types sorted, or None. ``client`` may decode its replies
    or not: one that does not reads binary values (valkey-search's vectors) exactly.

    A type it does not read, a module's, is ``[type, SHA-256 of its DUMP payload less the RDB version and
    CRC64]``: the module's serialization, which compares across processes only when the module writes a value
    the same way every time (``valkey.oracle.dataset-digest`` probes that).
    """

    kind = exact(client.type(key))
    if kind == "none":
        return None
    return _canonical(kind, _read(client, kind, key))


def _batches(items: Iterable[Any], size: int) -> Iterator[list]:
    iterator = iter(items)
    while batch := list(islice(iterator, size)):
        yield batch


def keyspace_snapshot(client: Any, match: Optional[str] = None, count: int = 1000) -> dict[str, list]:
    """Canonical content of every key on one instance (optionally a SCAN pattern), in SCAN order.

    A client that pipelines reads each batch of up to ``count`` keys in two round trips (every TYPE, then every
    read) instead of two per key; one that cannot (a test's) is read key by key with :func:`canonical_value`.
    Both give the same snapshot.
    """

    snapshot: dict[str, list] = {}
    keys = client.scan_iter(match=match, count=count)
    if not hasattr(client, "pipeline"):
        for key in keys:
            value = canonical_value(client, key)
            if value is not None:
                snapshot[_key(key)] = value
        return snapshot
    for batch in _batches(keys, count):
        with client.pipeline(transaction=False) as pipe:
            for key in batch:
                pipe.type(key)
            kinds = [exact(kind) for kind in pipe.execute()]
        held = [(key, kind) for key, kind in zip(batch, kinds) if kind != "none"]
        with client.pipeline(transaction=False) as pipe:
            for key, kind in held:
                _read(pipe, kind, key)
            replies = pipe.execute()
        for (key, kind), reply in zip(held, replies):
            value = _canonical(kind, reply)
            if value is not None:
                snapshot[_key(key)] = value
    return snapshot


def keyspace_digest(snapshot: Mapping[str, Any]) -> str:
    material = json.dumps(sorted(snapshot.items()), separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(material.encode("utf-8", errors="surrogatepass")).hexdigest()
