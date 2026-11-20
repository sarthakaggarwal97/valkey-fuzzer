"""The ``waves`` config check: each operation's own rules are one ``_check_operation`` call."""

from valkey_fuzzer.components import wave_operations
from valkey_fuzzer.components.wave_operations import MIGRATION, MIGRATION_TARGETS, check_waves

_DEPLOYMENT = {"primaries": 3, "replicas_per_primary": 1, "addressing": "per_instance_loopback"}


def test_check_waves_checks_each_operation_once_with_its_wave_context(monkeypatch):
    calls = []
    monkeypatch.setattr(wave_operations, "_check_operation", lambda *args: calls.append(args))
    waves = [
        [{"op": "migrate", "key": "{s}"}, {"op": "kill", "target": "source", "after": 0, "at": "snapshot"}],
        [{"op": "kill", "target": "replica"}],
    ]
    check_waves(waves, MIGRATION, _DEPLOYMENT)
    seen = []
    for kind, operation, where, mode, instances, primaries, replicas, endpoints, document, has_migrate in calls:
        # The deployment's shape goes with every call, and the operation is the resolved one (its defaults in).
        assert (mode, instances, primaries, replicas) == (MIGRATION, 6, 3, 1) and document is _DEPLOYMENT
        assert operation["op"] == kind and "delay_s" in operation
        seen.append((kind, where, endpoints, has_migrate))
    # One call per operation in wave order; a migration operation may name the endpoints in either wave, and
    # whether the wave has a migrate is the wave's.
    assert seen == [
        ("migrate", "waves[0][0]", (), True),
        ("kill", "waves[0][1]", MIGRATION_TARGETS, True),
        ("kill", "waves[1][0]", MIGRATION_TARGETS, False),
    ]
