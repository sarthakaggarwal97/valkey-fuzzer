"""``clock.now``: the run clock reads as wall time and never steps with it."""

import time
from types import SimpleNamespace

import pytest

from valkey_fuzzer import clock


def test_the_run_clock_reads_as_wall_time():
    assert clock.now() == pytest.approx(time.time(), abs=0.1)


def test_the_run_clock_follows_the_monotonic_clock_alone(monkeypatch):
    # A wall clock set back an hour mid-run (NTP, a resumed VM) would put a write before the fault that lost it:
    # a reading never consults it (the namespace has no ``time``), only the monotonic clock.
    monotonic = iter((1000.0, 1000.5))
    monkeypatch.setattr(clock, "time", SimpleNamespace(monotonic=lambda: next(monotonic)))
    first = clock.now()
    assert clock.now() - first == pytest.approx(0.5)
