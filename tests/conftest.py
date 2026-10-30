"""Fixtures the command, service, and CLI tests share."""

import tempfile
from typing import Any

import pytest

from valkey_fuzzer.contracts import ComponentResult, ComponentStatus, OutcomeStatus, RunOutcome


class CannedRuns:
    """Stands in for ``Runner.run``: records each plan and returns an outcome of ``status`` (a FAIL
    carries a failing ``data`` component) whose ``evidence`` and ``diagnostics`` are functions of the plan;
    with ``error`` set, the run raises it instead."""

    def __init__(self, catalog: Any):
        self.catalog, self.plans, self.status, self.error = catalog, [], OutcomeStatus.PASS, None
        self.evidence, self.diagnostics = (lambda plan: ()), (lambda plan: {})

    def __call__(self, plan, *, deadline=None, run_dir=None):
        self.plans.append(plan)
        if self.error is not None:
            raise self.error
        failing = (ComponentResult("data", ComponentStatus.FAIL, "lost"),) if self.status is OutcomeStatus.FAIL else ()
        return RunOutcome(
            plan,
            self.status,
            self.status.value,
            component_results=failing,
            evidence=tuple(self.evidence(plan)),
            diagnostics=self.diagnostics(plan),
            started_at=1.0,
            ended_at=2.0,
        )


@pytest.fixture
def canned_runs(monkeypatch) -> CannedRuns:
    """The Valkey experiments plugin's catalog, with canned runs; every RunService the commands build uses both."""

    from tests.valkey_experiments_support import valkey_service

    catalog = valkey_service().catalog
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

    return RunService(report_root=str(tmp_path / "reports"))


@pytest.fixture
def report_root(tmp_path, monkeypatch) -> None:
    """Without --artifact-dir, report folders go under <tmp>/valkey-fuzzer; <tmp> is the test's tmp_path."""

    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))


_SOURCE_DIGESTS: dict = {}


@pytest.fixture(autouse=True)
def source_digest_once(monkeypatch) -> None:
    """Hash each package's source once per test session, not on every run: the files do not change meanwhile.

    The key is each package's ``__file__``, so a test that loads a package from an edited copy gets that copy's
    digest.
    """

    import sys

    from valkey_fuzzer import service

    source_digest = getattr(service, "source_digest", None)
    if source_digest is None:
        return

    def once(packages):
        packages = sorted(set(packages))
        key = tuple((package, getattr(sys.modules.get(package), "__file__", None)) for package in packages)
        if key not in _SOURCE_DIGESTS:
            _SOURCE_DIGESTS[key] = source_digest(packages)
        return _SOURCE_DIGESTS[key]

    monkeypatch.setattr(service, "source_digest", once)


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
# Strict evidence contract for unit tests that call plugin components directly
# ---------------------------------------------------------------------------

_LIFECYCLE = {
    "capability": ("probe",), "workload": ("start", "stop"), "observer": ("start", "stop"),
    "fault": ("inject", "recover"), "action": ("run",), "oracle": ("evaluate",),
}  # fmt: skip
_LAST_CALLS = {"probe", "stop", "recover", "run", "evaluate"}


class _StrictContract:
    """The runner's evidence contract for the plugin components one FakeContext calls.

    Events those components publish on its bus, the evidence they return, and
    the state changes they own are checked exactly as in a run; a violation
    fails the test, even one the component swallowed.
    """

    def __init__(self, context, components):
        from valkey_fuzzer.evidence import EvidenceContract

        self.context = context
        self.contract = EvidenceContract(components)
        self.specs = {}
        context.event_bus._check = self._check_event
        context.obligations._check = self._check_change

    def _check_event(self, name, source, payload):
        if source in self.specs:
            self.contract.check_event(name, source, payload)

    def _check_change(self, owner, kind):
        if owner in self.specs:
            self.contract.check_change(owner, kind)

    def _bind(self, spec, registration):
        from dataclasses import replace

        from tests.support import make_plan

        if spec.id not in self.specs:
            self.specs[spec.id] = replace(spec, role=registration.role)
            mode = self.context.plan.deployment_mode
            self.contract.bind(make_plan(deployment_mode=mode, components=tuple(self.specs.values())))

    def call(self, registration, method, function, component, spec, args, kwargs):
        self._bind(spec, registration)
        try:
            result = function(component, self.context, spec, *args, **kwargs)
        finally:
            if method in _LAST_CALLS:
                self.contract.finished(spec.id)
        if isinstance(result, ComponentResult):
            committed = {event.sequence for event in self.context.event_bus.committed_history()}
            self.contract.check_evidence(spec, result.evidence, committed)
        self.assert_clean()
        return result

    def assert_clean(self):
        assert not self.contract.violations, f"evidence contract violated: {self.contract.violations}"


@pytest.fixture(autouse=True)
def strict_plugin_contract(monkeypatch):
    """Every plugin component a test calls with a FakeContext is held to its declarations."""

    import functools

    from valkey_fuzzer.catalog import ComponentRegistry
    from valkey_fuzzer.extensions.testing import FakeContext
    from valkey_fuzzer_experiments import component_registrations

    registrations = component_registrations()
    components = ComponentRegistry()
    components.register_many(registrations)
    contracts = []

    def strict(context):
        for item in contracts:
            if item.context is context:
                return item
        contracts.append(_StrictContract(context, components))
        return contracts[-1]

    for registration in registrations:
        for method in _LIFECYCLE[registration.role.value]:
            function = getattr(registration.factory, method)

            @functools.wraps(function)
            def checked(
                component,
                context,
                spec,
                *args,
                _registration=registration,
                _method=method,
                _function=function,
                **kwargs,
            ):
                if not isinstance(context, FakeContext) or spec.use != _registration.component_id:
                    return _function(component, context, spec, *args, **kwargs)
                return strict(context).call(_registration, _method, _function, component, spec, args, kwargs)

            monkeypatch.setattr(registration.factory, method, checked)
    # A known-bad test clears the violations it expects from these.
    yield contracts
    for item in contracts:
        item.assert_clean()
