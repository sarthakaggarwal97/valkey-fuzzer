"""CLUSTER NODES parsing and hash-slot helpers.

It has no dependencies on models.py, to avoid circular imports.
"""

import logging
import string
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
        logger.warning(f"Invalid port in CLUSTER NODES line: {line}")
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
        "ping_sent": parts[4],
        "pong_recv": parts[5],
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


def parse_slot_tokens(tokens: list[str]) -> list[int]:
    """The hash slots in a node's CLUSTER NODES slot tokens ("0-5460", "5461").

    Importing and migrating markers such as "[5461->-<node-id>]" are skipped.
    """
    slots: list[int] = []
    for token in tokens:
        if token.startswith("["):
            continue
        try:
            if "-" in token:
                start, end = token.split("-")
                slots.extend(range(int(start), int(end) + 1))
            else:
                slots.append(int(token))
        except ValueError:
            continue
    return slots


def format_node_address(node: dict) -> str:
    return f"{node['host']}:{node['port']}"


def group_slots_into_ranges(slots: list[int]) -> str:
    if not slots:
        return ""

    sorted_slots = sorted(slots)
    ranges = []
    start = sorted_slots[0]
    end = sorted_slots[0]

    for slot in sorted_slots[1:]:
        if slot == end + 1:
            # Consecutive slot, extend range
            end = slot
        else:
            # Gap found, save current range and start new one
            if start == end:
                ranges.append(str(start))
            else:
                ranges.append(f"{start}-{end}")
            start = slot
            end = slot

    # Add the last range
    if start == end:
        ranges.append(str(start))
    else:
        ranges.append(f"{start}-{end}")

    # Limit output length for very long lists
    if len(ranges) > 10:
        return f"{', '.join(ranges[:10])}... ({len(ranges)} ranges total)"
    else:
        return ", ".join(ranges)
