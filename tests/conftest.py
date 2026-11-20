"""Fixtures the command, service, and CLI tests share."""

import os
import shutil
import tempfile
from typing import Any
from unittest.mock import patch

import pytest

from valkey_fuzzer.results import ComponentResult, ComponentStatus, OutcomeStatus, RunOutcome

TLA_JAR = os.environ.get("TLA2TOOLS_JAR")
# The tests that run TLC: a model check, a counterexample, or a run conformed to a bundle.
needs_tlc = pytest.mark.skipif(
    not (TLA_JAR and os.path.exists(TLA_JAR) and shutil.which("java")), reason="needs java and TLA2TOOLS_JAR"
)


@pytest.fixture
def tla_tools():
    """TLC from ``$TLA2TOOLS_JAR`` on one worker, for a ``needs_tlc`` test."""

    from valkey_fuzzer.spec.tla import TlaTools

    return TlaTools.locate(TLA_JAR, workers="1")


def cli(*argv, via_main=True):
    """Run ``valkey-fuzzer argv`` and return its exit code.

    ``via_main`` runs :func:`valkey_fuzzer.cli.main`, which maps a command's exceptions to exit codes as the
    installed command does; without it the command's handler runs on the parsed arguments and its exceptions
    propagate to the test.
    """

    from valkey_fuzzer.cli import create_parser, main

    argv = [str(item) for item in argv]
    if via_main:
        with patch("sys.argv", ["valkey-fuzzer", *argv]):
            return main()
    args = create_parser().parse_args(argv)
    return args.handler(args)


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
def checker_installed(request, monkeypatch):
    """The host probes of ``valkey.oracle.history``, ``valkey.fault.clock-skew`` and ``valkey.fault.resource-pressure``
    pass, so a test that validates or runs a scenario with them needs no Go toolchain, libfaketime, or delegated
    cgroup. The tests of the probes themselves (tests/test_history.py, tests/test_clock_skew.py,
    tests/test_resource_pressure.py) put the real ones back; the tests of the checker build or find the real
    porcupine. A ``live`` test keeps the real libfaketime and cgroup checks: it starts real servers and must use
    the real library and cgroup subtree, not a stub path nothing can load."""

    monkeypatch.setattr("valkey_fuzzer.components.history.probe_host", lambda: None)
    if request.node.get_closest_marker("live") is None:
        monkeypatch.setattr(
            "valkey_fuzzer.deployment.local.clock_faults.find_library", lambda: "/stub/libfaketime.so.1"
        )
        monkeypatch.setattr("valkey_fuzzer.cgroups.delegated", lambda controllers, **where: ("/stub/cgroup", ""))


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
