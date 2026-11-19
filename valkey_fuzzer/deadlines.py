"""Component-call deadlines: the watchdog that runs a call on its own thread and cancels it at its deadline, and
the process-wide record of cancelled calls that are still running."""

import contextvars
import threading
import time
from collections.abc import Callable, Mapping
from typing import Any, Optional


class CooperativeDeadlineError(RuntimeError):
    """A component crossed a runner-visible deadline."""


class WaitInterruptedError(CooperativeDeadlineError):
    """The run's deadline came, or the runner cancelled the call, before a wait ended: what it waited for was
    never seen, so the call cannot say whether Valkey did it."""


class ComponentTimeoutError(CooperativeDeadlineError):
    """A component call was still running when its deadline passed and was cancelled.

    ``call`` is the call's thread if it was still running after the grace period.
    """

    def __init__(self, message: str, call: Optional[threading.Thread] = None):
        super().__init__(message)
        self.call = call


# How long the runner waits for a cancelled call to return, once right after
# cancelling it and once more after deployment cleanup.
CANCEL_GRACE_S = 5.0


class ComponentCall(threading.Thread):
    """One component call on a daemon thread, so the runner can stop waiting for it."""

    def __init__(self, function: Callable[[], Any], label: str, deadline: float):
        super().__init__(name=f"valkey-fuzzer-{label}", daemon=True)
        self.label = label
        self.deadline = deadline
        self.cancel_event = threading.Event()
        self.cancelled_at: Optional[float] = None
        self.outcome: dict[str, Any] = {}
        self._function = function

    def run(self) -> None:
        current_call.set(self)
        try:
            self.outcome["result"] = self._function()
        except BaseException as exc:
            self.outcome["error"] = exc

    def cancel(self, now: float) -> None:
        self.cancelled_at = now
        self.cancel_event.set()


# The watched component call the code runs for: set on the call's thread, and
# seen by every thread the call starts under a copy of its context
# (``contextvars.copy_context().run``), so those share its deadline and cancellation.
current_call: "contextvars.ContextVar[Optional[ComponentCall]]" = contextvars.ContextVar(
    "valkey_fuzzer_component_call", default=None
)


def cancelled_call() -> Optional[ComponentCall]:
    """The component call this code runs for, if the runner has cancelled it."""

    call = current_call.get()
    return call if call is not None and call.cancel_event.is_set() else None


def call_with_watchdog(
    function: Callable[[], Any],
    *,
    deadline: Optional[float],
    label: str,
    interrupted: Callable[[ComponentCall], None] = lambda call: None,
) -> Any:
    """Call ``function``; if it is still running at ``deadline``, cancel it. With no deadline, it runs here,
    unwatched: the runner always passes one for cleanup, rollback, undo and teardown.

    Components are synchronous and cannot be interrupted, so the call runs on a
    daemon thread, where ``RunContext.deadline`` is ``deadline``. At the
    deadline the runner cancels it: on that thread ``RunContext.cancelled``
    becomes true, ``RunContext.sleep`` returns early, and
    ``RunContext.deadline`` is the moment of cancellation. The runner waits up
    to ``CANCEL_GRACE_S`` for the call to return and then raises
    :class:`ComponentTimeoutError` either way, carrying the thread if it is
    still running so the caller can wait for it again after deployment cleanup.
    An interrupt (Ctrl-C) while waiting cancels the call too, hands it to
    ``interrupted``, and is re-raised at once.
    """

    if deadline is None:
        return function()
    call = ComponentCall(function, label, deadline)
    timeout = deadline - time.monotonic()
    call.start()
    try:
        call.join(max(0.0, timeout))
    except BaseException:
        call.cancel(time.monotonic())
        interrupted(call)
        raise
    if not call.is_alive():
        if "error" in call.outcome:
            raise call.outcome["error"]
        return call.outcome["result"]
    call.cancel(time.monotonic())
    try:
        call.join(CANCEL_GRACE_S)
    except BaseException:
        interrupted(call)
        raise
    if call.is_alive():
        raise ComponentTimeoutError(
            f"{label} did not return within {timeout:.3f}s and is still running after cancellation", call
        )
    raise ComponentTimeoutError(f"{label} did not return within {timeout:.3f}s and was cancelled")


# Cancelled calls that outlived their grace period, with their run's scenario.
# No run starts in this process while one is alive: it may still be acting on
# a deployment, a port, or a file that the next run would use.
_cancelled_calls: list[tuple[str, ComponentCall]] = []
_cancelled_calls_lock = threading.Lock()


def record_abandoned_call(scenario_id: str, call: ComponentCall) -> None:
    """Refuse every later run in this process until ``call``, which ``scenario_id``'s run gave up on, returns."""

    with _cancelled_calls_lock:
        _cancelled_calls.append((scenario_id, call))


def abandoned_calls() -> tuple[Mapping[str, str], ...]:
    """Cancelled calls from any run in this process that are still running."""

    with _cancelled_calls_lock:
        _cancelled_calls[:] = [(scenario, call) for scenario, call in _cancelled_calls if call.is_alive()]
        return tuple({"scenario_id": scenario, "call": call.label} for scenario, call in _cancelled_calls)


def pause(context: Any, seconds: float, reason: str) -> None:
    """Sleep ``seconds`` with ``context.sleep``; without a context (provisioning, a test), a plain sleep.

    Raises :class:`WaitInterruptedError` when the run's deadline comes before the sleep ends or the runner
    cancels the call, so a cancelled wait is never mistaken for one that simply ran out of time.
    """

    if context is None:
        time.sleep(seconds)
        return
    deadline = context.deadline
    # A millisecond of slack: a timeout clamped to the deadline must end as a timeout, not on float rounding.
    if deadline is not None and time.monotonic() + seconds > deadline + 1e-3:
        raise WaitInterruptedError(f"the run's deadline came before the wait for {reason} ended")
    if not context.sleep(seconds):
        raise WaitInterruptedError(f"the run was cancelled while waiting for {reason}")


def wait_for(
    check: Callable[[], tuple[bool, Any]],
    *,
    timeout_s: float,
    interval_s: float,
    reason: str,
    context: Any = None,
) -> tuple[bool, Any]:
    """Call ``check() -> (done, value)`` every ``interval_s`` until done or ``timeout_s`` pass: the last
    ``(done, value)``. This is the one wait of the harness; its watchdog is ``timeout_s``.

    ``check`` runs at least once. Time is measured on the monotonic clock. Running out of ``timeout_s`` is a
    result (``done`` False); a cancelled call, or a run deadline that comes before ``timeout_s``, is not:
    both raise :class:`WaitInterruptedError` naming ``reason`` (see :func:`pause`). A caller that wants the
    deadline to end the wait quietly passes ``timeout_s`` already clamped to it. An exception from ``check``
    propagates.
    """

    end = time.monotonic() + max(timeout_s, 0.0)
    while True:
        done, value = check()
        left = end - time.monotonic()
        if done or left <= 0:
            return done, value
        pause(context, min(max(interval_s, 0.0), left), reason)
