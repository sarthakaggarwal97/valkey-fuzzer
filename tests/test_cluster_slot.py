"""Known-vector tests for Valkey Cluster hash-slot calculation."""

import pytest

from valkey_fuzzer.utils.cluster_parser import compute_cluster_slot


@pytest.mark.parametrize(
    ("key", "expected_slot"),
    [
        ("foo", 12182),
        ("fuzzer:test:0", 8823),
        ("{user1000}.following", 3443),
        ("foo{}{bar}", 8363),
        ("{}", 15257),
    ],
)
def test_compute_cluster_slot_matches_valkey_vectors(key, expected_slot):
    assert compute_cluster_slot(key) == expected_slot


def test_compute_cluster_slot_uses_nonempty_hash_tag():
    assert compute_cluster_slot("{user1000}.following") == compute_cluster_slot("{user1000}.followers")
