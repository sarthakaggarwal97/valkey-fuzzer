"""The default run deadline: derived from the search profile's budget or the scenario's own component budgets,
printed with its source, and replaced by an explicit --timeout."""

import time

import pytest

from tests.conftest import cli as _cli
from tests.test_runner import WORKLOAD, _plan, _spec
from valkey_fuzzer.service import DEFAULT_TIMEOUT_S, derive_timeout


def test_without_a_profile_or_component_budgets_the_default_applies():
    assert derive_timeout(_plan((_spec(WORKLOAD, "load"),))) == (DEFAULT_TIMEOUT_S, "default")


def test_a_profile_budget_replaces_the_default():
    plan = _plan((_spec(WORKLOAD, "load"),))
    assert derive_timeout(plan, profile_timeout_s=600, profile="pr-quick") == (600, "search profile 'pr-quick' budget")


def test_components_that_declare_more_time_than_the_base_raise_the_default():
    plan = _plan((_spec(WORKLOAD, "a", timeout_s=900), _spec(WORKLOAD, "b", timeout_s=900)))
    seconds, source = derive_timeout(plan)
    assert seconds == 1800 and "sum of the components' timeout_s" in source and f"{DEFAULT_TIMEOUT_S:g}s" in source
    assert derive_timeout(plan, profile_timeout_s=3000)[0] == 3000, "a larger profile budget stands"


@pytest.mark.parametrize(
    ("options", "line"),
    [
        ((), "Timeout: 1200s (default)"),
        (("--profile", "pr-quick"), "Timeout: 600s (search profile 'pr-quick' budget)"),
        (("--timeout", 90), "Timeout: 90s (--timeout)"),
        (("--profile", "pr-quick", "--timeout", 90), "Timeout: 90s (--timeout)"),
    ],
)
def test_run_prints_the_deadline_it_uses_and_where_it_came_from(canned_runs, capsys, monkeypatch, options, line):
    deadlines = []
    run = canned_runs.__call__
    monkeypatch.setattr(
        "valkey_fuzzer.runner.Runner.run",
        lambda self, plan, *, deadline=None, run_dir=None: deadlines.append(deadline) or run(plan, run_dir=run_dir),
    )
    started = time.monotonic()
    assert _cli("run", "--seed", "4", "--experiment", "valkey.aof-durability", *options) == 0
    assert line in capsys.readouterr().out
    expected = float(line.split("Timeout: ")[1].split("s ")[0])
    (deadline,) = deadlines
    assert deadline is not None and expected - 5 <= deadline - started <= expected + 5


def test_a_run_started_without_any_timeout_still_gets_a_deadline(service, canned_runs, monkeypatch):
    deadlines = []
    monkeypatch.setattr(
        "valkey_fuzzer.runner.Runner.run",
        lambda self, plan, *, deadline=None, run_dir=None: deadlines.append(deadline) or canned_runs(plan),
    )
    service.run_plan(_plan((_spec(WORKLOAD, "load"),)))
    assert deadlines[0] is not None
