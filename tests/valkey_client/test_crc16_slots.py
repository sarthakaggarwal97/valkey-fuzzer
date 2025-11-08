from valkey import crc as valkey_crc

from src.valkey_client.crc16_slots import crc16, generate_crc16_slot_table, hash_slot


def test_crc16_matches_known_values():
    # Sanity check against a couple of known CRC16 values taken from the
    # Redis cluster specification examples.
    assert crc16(b"123456789") == 0x31C3
    assert crc16(b"foo") == 0xAF16


def test_generate_crc16_slot_table_covers_all_slots():
    slot_table = generate_crc16_slot_table()

    assert len(slot_table) == 16384
    # Each index should correspond to the slot computed from the tag.
    for slot, tag in enumerate(slot_table):
        assert hash_slot(tag) == slot


def test_hash_slot_matches_valkey_crc():
    slot = hash_slot("foo")
    expected = valkey_crc.crc_hqx(b"foo", 0) % 16384
    assert slot == expected


def test_slot_table_entries_are_unique():
    slot_table = generate_crc16_slot_table()
    assert len(set(slot_table)) == len(slot_table)
