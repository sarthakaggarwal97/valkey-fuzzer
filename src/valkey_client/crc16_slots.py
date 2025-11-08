"""Utilities for working with CRC16 hash slots.

This module provides helpers to compute Redis/Valkey compatible CRC16
hashes and to generate a complete set of hash tags that cover all
16384 cluster slots.  The previous implementation stored a gigantic
pre-generated table of hash tags.  Generating the table on the fly
keeps the repository lean while still guaranteeing full slot coverage
for workloads that depend on deterministic hash tags.
"""

from __future__ import annotations

from functools import lru_cache
from itertools import product
from typing import Iterable, List

CRC16_POLY = 0x1021
# Alphabet of printable characters used when generating hash tags.  The order
# of characters is important for determinism so we keep it explicitly defined
# instead of relying on ``string.ascii_letters``.
_ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"


def _generate_crc16_table() -> List[int]:
    """Create the lookup table used by the CRC16 implementation."""

    table: List[int] = []
    for byte in range(256):
        crc = byte << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ CRC16_POLY) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
        table.append(crc)
    return table


_CRC16_TABLE = _generate_crc16_table()


def crc16(data: bytes) -> int:
    """Compute the CRC16 checksum used for Redis/Valkey cluster slots."""

    crc = 0
    for byte in data:
        crc = ((crc << 8) & 0xFFFF) ^ _CRC16_TABLE[((crc >> 8) ^ byte) & 0xFF]
    return crc


def hash_slot(tag: str) -> int:
    """Return the cluster slot for the provided hash tag."""

    return crc16(tag.encode("utf-8")) % 16384


def _candidate_tags() -> Iterable[str]:
    """Yield printable candidate hash tags in a deterministic order."""

    for length in range(1, 5):
        for combo in product(_ALPHABET, repeat=length):
            yield "".join(combo)


@lru_cache(maxsize=1)
def generate_crc16_slot_table() -> List[str]:
    """Generate a list of hash tags that cover every cluster slot.

    The resulting list has a length of 16,384 (one entry per slot) where the
    value at index *i* maps to slot *i*.  The computation is cached so callers
    can reuse the generated table without recomputing it.
    """

    slot_table: List[str | None] = [None] * 16384
    remaining = 16384

    for tag in _candidate_tags():
        slot = hash_slot(tag)
        if slot_table[slot] is None:
            slot_table[slot] = tag
            remaining -= 1
            if remaining == 0:
                # The table is complete; mypy insists on a precise type.
                return [t for t in slot_table if t is not None]

    raise RuntimeError("Failed to generate a complete CRC16 slot table")

