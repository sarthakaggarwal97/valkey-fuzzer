"""How the variants check compares two replies: in order and by type, except where a variant or a command allows."""

import pytest

from valkey_fuzzer.components.checks import _shape


def same(command, base, mine, protocol):
    return _shape({"reply": base}, command, protocol) == _shape({"reply": mine}, command, protocol)


@pytest.mark.parametrize(
    ("base", "mine"),
    [
        (["a", "b", "c"], ["c", "b", "a"]),
        ([["a", 1], ["b", 2]], [["a", 2], ["b", 1]]),
        ("1", 1),
        ("1.0", 1),
        (1.0, 1),
        (True, 1),
        (False, None),
        ([["a", 1]], ["a", 1]),
        ({"a": 1}, ["a", 1]),
        ([1, None], [1]),
        ([[1, [2]], 3], [[1], [2, 3]]),
        ([1, 1, 2], [1, 2, 2]),
        (2**70, str(2**70)),
        (1.5, "1.5"),
    ],
)
def test_the_encodings_variant_compares_order_structure_and_type(base, mine):
    assert not same("LRANGE", base, mine, protocol=False)
    assert same("LRANGE", base, base, protocol=False)


@pytest.mark.parametrize(
    ("base", "mine"),
    [
        ("1", 1),
        (True, 1),
        (False, None),
        (1.0, 1),
        ({"a": 1, "b": 2}, ["a", 1, "b", 2]),
        ([["a", 1.5], ["b", 2.0]], ["a", "1.5", "b", "2"]),
        ([[1, [2]], 3], [1, 2, 3]),
        ([None, 1], [None, True]),
    ],
)
def test_the_protocol_variant_equates_what_resp2_and_resp3_reply_alike(base, mine):
    assert same("LRANGE", base, mine, protocol=True)


@pytest.mark.parametrize(
    ("base", "mine"),
    [
        (["a", "b", "c"], ["c", "b", "a"]),
        ([["a", 1], ["b", 2]], [["a", 2], ["b", 1]]),
        ({"a": 1, "b": 2}, ["b", 1, "a", 2]),
        ([1, 1, 2], [1, 2, 2]),
    ],
)
def test_the_protocol_variant_still_compares_order_and_pairing(base, mine):
    assert not same("ZRANGE", base, mine, protocol=True)


@pytest.mark.parametrize("protocol", [False, True])
@pytest.mark.parametrize("command", ["SMEMBERS", "SUNION", "SINTER", "SDIFF", "HKEYS", "HVALS", "KEYS"])
def test_a_set_replies_in_any_order(command, protocol):
    assert same(command, ["a", "b", "c"], ["c", "a", "b"], protocol)
    assert not same(command, ["a", "b", "c"], ["a", "b", "d"], protocol)
    assert not same(command, ["a", "a", "b"], ["a", "b", "b"], protocol)


@pytest.mark.parametrize("protocol", [False, True])
def test_a_hash_replies_its_pairs_in_any_order_but_keeps_them_paired(protocol):
    assert same("HGETALL", ["a", "1", "b", "2"], ["b", "2", "a", "1"], protocol)
    assert not same("HGETALL", ["a", "1", "b", "2"], ["a", "2", "b", "1"], protocol)
    assert not same("HGETALL", ["a", "1", "b", "1"], ["a", "1", "b", "2"], protocol)


def test_a_hash_replies_alike_in_resp2_and_resp3_in_the_protocol_variant_only():
    flat, mapped = ["a", "1", "b", "2"], {"b": "2", "a": "1"}
    assert same("HGETALL", flat, mapped, protocol=True)
    assert not same("HGETALL", flat, mapped, protocol=False)


def test_an_unordered_command_that_replies_a_number_compares_it_typed():
    assert same("SINTER", 3, 3, protocol=False) and not same("SINTER", 3, "3", protocol=False)


def test_a_record_without_a_reply_compares_as_itself():
    assert _shape({"error": "x"}) != _shape({"error": "y"}) and _shape(None) == _shape(None)
