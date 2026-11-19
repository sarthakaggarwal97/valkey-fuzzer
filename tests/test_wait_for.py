"""``deadlines.wait_for``: a timeout is a result, a cancel or a deadline is an error."""

from types import SimpleNamespace

import pytest

from valkey_fuzzer.components.polling import poll_until
from valkey_fuzzer.deadlines import WaitInterruptedError, wait_for


def _context(*, cancel_after=None, deadline=None):
    calls = []

    def sleep(seconds):
        calls.append(seconds)
        return cancel_after is None or len(calls) < cancel_after

    return SimpleNamespace(deadline=deadline, sleep=sleep, calls=calls)


def test_a_wait_returns_the_value_once_the_check_is_done():
    context, seen = _context(), iter([(False, 1), (True, 2)])
    assert wait_for(lambda: next(seen), timeout_s=5, interval_s=0.1, reason="x", context=context) == (True, 2)


def test_a_wait_that_runs_out_of_time_returns_the_last_value():
    result = wait_for(lambda: (False, "last"), timeout_s=0.0, interval_s=0.1, reason="x", context=_context())
    assert result == (False, "last")


def test_a_cancelled_call_raises_naming_what_it_waited_for():
    context = _context(cancel_after=2)
    with pytest.raises(WaitInterruptedError, match="cancelled while waiting for a replica"):
        wait_for(lambda: (False, None), timeout_s=60, interval_s=0.1, reason="a replica", context=context)


def test_a_deadline_before_the_timeout_raises_but_a_clamped_poll_times_out():
    import time

    context = _context(deadline=time.monotonic() + 0.05)
    with pytest.raises(WaitInterruptedError, match="deadline"):
        wait_for(lambda: (False, None), timeout_s=60, interval_s=1.0, reason="x", context=context)
    assert poll_until(
        lambda: (False, "v"), timeout_s=60, interval_s=0.01, context=_context(deadline=time.monotonic() + 0.05)
    ) == (False, "v")
