"""CLUSTER NODES parsing and hash-slot helpers."""

import logging
import string
from collections.abc import Iterable, Mapping, Sequence
from functools import cache
from itertools import product
from typing import Any, Optional

from valkey.crc import key_slot

logger = logging.getLogger(__name__)

# A cluster's hash slots.
HASH_SLOTS = 16384

# Slot hash tags are tried shortest first, in itertools.product order over these characters.
_SLOT_TAG_ALPHABET = string.digits + string.ascii_lowercase + string.ascii_uppercase


@cache
def slot_hash_tags() -> tuple[str, ...]:
    """For each hash slot, the first tag of one to three characters that hashes to it."""
    tags: dict[int, str] = {}
    for length in (1, 2, 3):
        for chars in product(_SLOT_TAG_ALPHABET, repeat=length):
            tag = "".join(chars)
            tags.setdefault(key_slot(tag.encode()), tag)
    return tuple(tags[slot] for slot in range(HASH_SLOTS))


def parse_cluster_nodes_line(line: str) -> Optional[dict[str, Any]]:
    if not line.strip():
        return None

    parts = line.split()
    if len(parts) < 8:
        return None

    # Parse address (format: host:port@bus_port or host:port)
    address = parts[1].split("@")[0]  # Remove bus port if present
    host, port_str = address.rsplit(":", 1)

    try:
        port = int(port_str)
    except ValueError:
        logger.warning("Invalid port in CLUSTER NODES line: %s", line)
        return None

    # Parse flags - can be comma-separated like "master,fail"
    flags = parts[2]
    flag_list = flags.split(",")

    return {
        "node_id": parts[0],
        "address": address,
        "host": host,
        "port": port,
        "flags": flags,
        "primary_id": parts[3] if parts[3] != "-" else None,
        "config_epoch": parts[6],
        "link_state": parts[7],
        "slots": parts[8:] if len(parts) > 8 else [],
        "is_myself": "myself" in flag_list,
        "is_primary": "master" in flag_list,
        "is_replica": "slave" in flag_list,
        "is_fail": "fail" in flag_list or "fail?" in flag_list,
    }


def parse_cluster_nodes(cluster_nodes_raw: str) -> list[dict[str, Any]]:
    nodes = []
    for line in cluster_nodes_raw.split("\n"):
        node = parse_cluster_nodes_line(line)
        if node:
            nodes.append(node)
    return nodes


def parse_slot_ranges(tokens: list[str]) -> list[tuple[int, int]]:
    """The hash-slot ranges in a node's CLUSTER NODES slot tokens: ``["0-5460", "5462"]`` is
    ``[(0, 5460), (5462, 5462)]``.

    Importing and migrating markers such as "[5461->-<node-id>]" are skipped.
    """
    ranges = []
    for token in tokens:
        if token.startswith("["):
            continue
        start, _, end = token.partition("-")
        try:
            ranges.append((int(start), int(end or start)))
        except ValueError:
            continue
    return ranges


def parse_slots_in_flight(tokens: list[str]) -> set[int]:
    """The slots a node's own CLUSTER NODES line marks as migrating out (``[5461->-<node-id>]``) or importing
    (``[5461-<-<node-id>]``): a key in one legitimately exists on both nodes until the handoff."""
    slots = set()
    for token in tokens:
        if token.startswith("[") and ("->-" in token or "-<-" in token):
            try:
                slots.add(int(token[1:].split("-", 1)[0]))
            except ValueError:
                continue
    return slots


def merge_slot_ranges(ranges: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
    """``ranges`` sorted, with those that overlap or touch joined: ``[(5, 9), (0, 4), (0, 2)]`` is ``[(0, 9)]``."""
    merged: list[tuple[int, int]] = []
    for start, end in sorted(ranges):
        if merged and start <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def shared_slots(claims: Mapping[str, Iterable[tuple[int, int]]]) -> dict[int, set[str]]:
    """Every slot in the ranges of two of ``claims``' primaries (node ID -> slot ranges), and those primaries."""
    ranges = sorted((start, end, node_id) for node_id, owned in claims.items() for start, end in owned)
    shared: dict[int, set[str]] = {}
    for index, (_start, end, node_id) in enumerate(ranges):
        # Sorted by start, so the ranges that can overlap this one follow it until one starts after its end.
        for other_start, other_end, other in ranges[index + 1 :]:
            if other_start > end:
                break
            if other != node_id:
                for slot in range(other_start, min(end, other_end) + 1):
                    shared.setdefault(slot, set()).update((node_id, other))
    return shared


def split_slots(views: Mapping[Any, Sequence[Mapping[str, Any]]]) -> dict[int, set[str]]:
    """The slots two primaries serve at once, and those primaries' node IDs: a split brain.

    ``views`` holds each node's CLUSTER NODES as :func:`parse_cluster_nodes`
    returns it, keyed by anything that names the node. Two primaries split a
    slot when one node's view gives it to both and neither is flagged
    ``fail`` there, or when each claims it in its own view. A primary only
    suspected there (``fail?``) still serves its slots, so it counts.
    """
    split: dict[int, set[str]] = {}
    own_claims: dict[str, list[tuple[int, int]]] = {}
    for view in views.values():
        owners: dict[str, list[tuple[int, int]]] = {}
        for node in view:
            if not node["is_primary"]:
                continue
            ranges = parse_slot_ranges(node["slots"])
            if "fail" not in node["flags"].split(","):
                owners.setdefault(node["node_id"], []).extend(ranges)
            if node["is_myself"]:
                own_claims.setdefault(node["node_id"], []).extend(ranges)
        for slot, nodes in shared_slots(owners).items():
            split.setdefault(slot, set()).update(nodes)
    for slot, nodes in shared_slots(own_claims).items():
        split.setdefault(slot, set()).update(nodes)
    return split


def format_slot_ranges(ranges: Sequence[tuple[int, int]]) -> str:
    """Slot ranges written as CLUSTER NODES writes each, comma-separated ("0-5460, 5462"); the first ten when
    there are more."""
    shown = [str(start) if start == end else f"{start}-{end}" for start, end in ranges]
    if len(shown) > 10:
        return f"{', '.join(shown[:10])}... ({len(shown)} ranges total)"
    return ", ".join(shown)


def group_slots_into_ranges(slots: Iterable[int]) -> str:
    """Slots as ranges (see :func:`format_slot_ranges`): ``[0, 1, 2, 5]`` is ``"0-2, 5"``."""
    return format_slot_ranges(merge_slot_ranges((slot, slot) for slot in slots))
