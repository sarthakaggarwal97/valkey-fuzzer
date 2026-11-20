"""Fixtures the command, service, and CLI tests share."""

import tempfile
from typing import Any

import pytest

from valkey_fuzzer.results import ComponentResult, ComponentStatus, OutcomeStatus, RunOutcome


class CannedRuns:
    """Stands in for ``Runner.run``: records each plan and returns an outcome of ``status`` (a FAIL
    carries a failing ``data`` component) whose ``evidence`` and ``diagnostics`` are functions of the plan;
    with ``error`` set, the run raises it instead. ``statuses`` are consumed first, one per run, before
    ``status`` applies."""

    def __init__(self, catalog: Any):
        self.catalog, self.plans, self.status, self.error = catalog, [], OutcomeStatus.PASS, None
        self.statuses: list[OutcomeStatus] = []
        self.evidence, self.diagnostics = (lambda plan: ()), (lambda plan: {})

    def __call__(self, plan, *, deadline=None, run_dir=None):
        self.plans.append(plan)
        if self.error is not None:
            raise self.error
        status = self.statuses.pop(0) if self.statuses else self.status
        failing = (ComponentResult("data", ComponentStatus.FAIL, "lost"),) if status is OutcomeStatus.FAIL else ()
        return RunOutcome(
            plan,
            status,
            status.value,
            component_results=failing,
            evidence=tuple(self.evidence(plan)),
            diagnostics=self.diagnostics(plan),
            started_at=1.0,
            ended_at=2.0,
        )


@pytest.fixture(autouse=True)
def checker_installed(monkeypatch):
    """The host probe of ``valkey.oracle.history`` passes, so a test that validates or runs a scenario with the
    oracle needs no Go toolchain. The tests of the probe itself (tests/test_history.py) put the real one back; the
    tests of the checker build or find the real porcupine."""

    monkeypatch.setattr("valkey_fuzzer.components.history.probe_host", lambda: None)


@pytest.fixture(scope="session")
def catalog():
    """The Valkey components' catalog, built once: it is frozen, so every test can read the same one."""

    from tests.valkey_experiments_support import shared_catalog

    return shared_catalog()


@pytest.fixture
def canned_runs(monkeypatch, catalog) -> CannedRuns:
    """The Valkey components' catalog, with canned runs; every RunService the commands build uses both."""

    runs = CannedRuns(catalog)
    monkeypatch.setattr("valkey_fuzzer.runner.Runner.run", runs)
    monkeypatch.setattr("valkey_fuzzer.service.build_catalog", lambda **options: catalog)
    # Canned runs start no servers, so a generated scenario keeps its ports whatever this host is using.
    monkeypatch.setattr("valkey_fuzzer.deployment.local.ports.busy_planned_ports", lambda deployment: [])
    return runs


@pytest.fixture
def service(canned_runs, tmp_path):
    """A RunService on the canned catalog and runs, with its report folders under ``tmp_path/reports``."""

    from valkey_fuzzer.service import RunService

    return RunService(artifact_dir=str(tmp_path / "reports"))


@pytest.fixture
def report_root(tmp_path, monkeypatch) -> None:
    """Without --artifact-dir, report folders go under <tmp>/valkey-fuzzer; <tmp> is the test's tmp_path."""

    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))


@pytest.fixture(autouse=True)
def temporary_data_dir(request, tmp_path_factory, monkeypatch) -> None:
    """A slow (server-starting) test's deployments default to a temporary ``base_data_dir``.

    Instance logs stay under ``<base_data_dir>/logs`` after a run, so the
    shared default, /tmp/valkey-fuzzer, would keep every such test's logs on the host.
    """

    if request.node.get_closest_marker("slow") is None:
        return
    from valkey_fuzzer.deployment.local.models import DeploymentConfig

    default = DeploymentConfig.__dataclass_fields__["base_data_dir"].default
    data_dir = str(tmp_path_factory.mktemp("valkey-data"))
    post_init = DeploymentConfig.__post_init__

    def use_temporary_data_dir(config: DeploymentConfig) -> None:
        post_init(config)
        if config.base_data_dir == default:
            config.base_data_dir = data_dir

    monkeypatch.setattr(DeploymentConfig, "__post_init__", use_temporary_data_dir)


# ---------------------------------------------------------------------------
# Strict evidence contract for unit tests that call Valkey components directly
# ---------------------------------------------------------------------------

_LIFECYCLE = {
    "capability": ("probe",), "workload": ("start", "stop"), "observer": ("start", "stop"),
    "fault": ("inject", "recover"), "action": ("run",), "oracle": ("evaluate",),
}  # fmt: skip


@pytest.fixture(autouse=True)
def strict_component_contract(monkeypatch):
    """Every FakeContext gets the Valkey registrations, and every Valkey component a test calls with one goes
    through ``FakeContext.call``, which holds it to its declarations; the fixture yields those contexts."""

    import functools
    import types

    from valkey_fuzzer.components import component_registrations
    from valkey_fuzzer.testing import FakeContext

    registrations = component_registrations()
    contexts = []
    init = FakeContext.__init__

    @functools.wraps(init)
    def with_registrations(context, *args, **kwargs):
        init(context, *args, registrations=kwargs.pop("registrations", registrations), **kwargs)
        contexts.append(context)

    monkeypatch.setattr(FakeContext, "__init__", with_registrations)
    for registration in registrations:
        for method in _LIFECYCLE[registration.role.value]:
            function = getattr(registration.factory, method)

            @functools.wraps(function)
            def checked(component, context, spec, *args, _use=registration.component_id, _function=function):
                if not isinstance(context, FakeContext) or spec.use != _use:
                    return _function(component, context, spec, *args)
                return context.call(types.MethodType(_function, component), spec, *args)

            monkeypatch.setattr(registration.factory, method, checked)
    # A known-bad test clears the violations it expects from these.
    yield contexts
    for context in contexts:
        assert not context.violations, f"evidence contract violated: {context.violations}"
