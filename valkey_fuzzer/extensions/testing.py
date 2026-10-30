"""Unit-test helpers: run an experiment in-process, or call one component directly.

``run_experiment`` compiles and executes an experiment through the real
compiler and runner with a fake deployment whose services you supply, so a
plugin can test its components, events, evidence, and verdicts without
starting servers::

    outcome = run_experiment(PLUGIN, "example.neutral", inputs={"message": "hi"}, deployment_mode="example-neutral")
    assert outcome.status is OutcomeStatus.PASS

``FakeContext`` is a :class:`~valkey_fuzzer.extensions.RunContext` for calling
a single component's lifecycle methods, and ``resolved_spec`` gives that
component its config with defaults, as the compiler would.
"""

import random
import tempfile
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import Any, Callable, Optional, Union

from ..contracts import (
    ComponentRegistration,
    ComponentSpec,
    DeploymentProviderIdentity,
    DeploymentProviderRegistration,
    ExecutionPlan,
    ExperimentSpec,
    OutcomeStatus,
    PluginManifest,
    ProvisionedDeployment,
    RunMode,
    RunOutcome,
)
from ..events import EventBus
from ..obligations import ObligationLedger
from ..service import RunService

DEPLOYMENT_SERVICES = Union[Mapping[str, Any], Callable[[ExecutionPlan], Mapping[str, Any]]]
FAKE_PROVIDER_ID = "valkey-fuzzer.deployment.fake"


class FakeDeploymentProvider:
    """A deployment provider that starts nothing and publishes the given services.

    Every call is appended to ``calls`` (a new list, or one you share with
    other fakes) as its method name, or as ``<name>.<method>`` given a
    ``name``. ``failures`` maps a method name to the exception it raises.
    """

    def __init__(
        self,
        services: DEPLOYMENT_SERVICES = None,
        *,
        calls: Optional[list[str]] = None,
        name: Optional[str] = None,
        failures: Optional[Mapping[str, BaseException]] = None,
    ):
        self._services = services or {}
        self.calls: list[str] = [] if calls is None else calls
        self._prefix = "" if name is None else f"{name}."
        self._failures = dict(failures or {})

    def _record(self, method: str) -> None:
        self.calls.append(f"{self._prefix}{method}")
        if method in self._failures:
            raise self._failures[method]

    def validate(self, plan: ExecutionPlan) -> None:
        self._record("validate")

    def provision(self, context: Any) -> ProvisionedDeployment:
        self._record("provision")
        services = self._services(context.plan) if callable(self._services) else self._services
        return ProvisionedDeployment(services=services)

    def rollback(self, context: Any) -> None:
        self._record("rollback")

    def cleanup(self, context: Any, deployment: ProvisionedDeployment) -> None:
        self._record("cleanup")


def fake_deployment_providers(
    deployment_modes: Sequence[str], services: DEPLOYMENT_SERVICES = None
) -> dict[str, DeploymentProviderRegistration]:
    """Registrations of :class:`FakeDeploymentProvider` for each deployment mode."""

    return {
        mode: DeploymentProviderRegistration(
            deployment_mode=mode,
            provider_id=FAKE_PROVIDER_ID,
            factory=lambda: FakeDeploymentProvider(services),
        )
        for mode in deployment_modes
    }


def run_experiment(
    plugin: PluginManifest,
    experiment_id: str,
    *,
    inputs: Optional[Mapping[str, Any]] = None,
    deployment_mode: Optional[str] = None,
    deployment: Optional[Mapping[str, Any]] = None,
    services: DEPLOYMENT_SERVICES = None,
    plugins: Sequence[PluginManifest] = (),
    seed: int = 1,
    scenario_id: str = "test",
    artifact_dir: Optional[str] = None,
    deadline: Optional[float] = None,
) -> RunOutcome:
    """Compile and run ``experiment_id`` from ``plugin`` against a fake deployment.

    ``deployment_mode`` defaults to the experiment's first supported mode.
    Modes the plugins do not provide themselves get a
    :class:`FakeDeploymentProvider` whose ``services`` (a mapping, or a
    function of the executable plan) become ``context.services["deployment"]``.
    Installed plugins are not loaded.
    """

    experiment = next((item for item in plugin.experiments if item.experiment_id == experiment_id), None)
    if experiment is None:
        raise KeyError(f"plugin {plugin.plugin_id!r} has no experiment {experiment_id!r}")
    manifests = (plugin, *plugins)
    provided = {entry.deployment_mode for manifest in manifests for entry in manifest.deployment_providers}
    service = RunService(
        discover_plugins=False,
        plugins=manifests,
        deployment_providers=tuple(
            fake_deployment_providers(
                [mode for mode in experiment.deployment_modes if mode not in provided], services
            ).values()
        ),
        artifact_dir=artifact_dir or tempfile.mkdtemp(prefix="valkey-fuzzer-test-"),
    )
    document = dict(deployment or {})
    document.setdefault("mode", deployment_mode or experiment.deployment_modes[0])
    plan = service.compile(
        {
            "scenario_id": scenario_id,
            "seed": seed,
            "experiment": {"id": experiment.experiment_id, "inputs": dict(inputs or {})},
            "deployment": document,
        }
    )
    # Through the runner, so a preflight error raises rather than becoming an ERROR outcome.
    return service.runner.run(plan, deadline=deadline)


def resolved_spec(registration: ComponentRegistration, spec: ComponentSpec) -> ComponentSpec:
    """``spec`` with its config resolved by ``registration``'s schema, as a compiled plan holds it.

    Components read their defaults from ``spec.config``, so a test that calls
    one directly passes it this. The registration's ``check`` does not run.
    """

    if registration.config is None:
        return spec
    return replace(spec, config=registration.config.resolve(spec.config, f"components.{spec.id}.config"))


class FakeContext:
    """A run context for calling one component's lifecycle methods directly.

    :meth:`cancel` does what the runner does when a call outlives its timeout.
    """

    def __init__(
        self,
        *components: ComponentSpec,
        services: Optional[Mapping[str, Any]] = None,
        plan: Optional[ExecutionPlan] = None,
        deployment_mode: str = "standalone",
        deployment: Optional[Mapping[str, Any]] = None,
        seed: int = 1,
        artifact_dir: Optional[str] = None,
        deadline: Optional[float] = None,
    ):
        if plan is None:
            experiment = ExperimentSpec(
                experiment_id="testing.component",
                mode=RunMode.SMOKE,
                deployment_modes=(deployment_mode,),
                components=components,
            )
            plan = ExecutionPlan.from_experiment(
                experiment,
                scenario_id="component-test",
                seed=seed,
                deployment_mode=deployment_mode,
                deployment_provider=DeploymentProviderIdentity(FAKE_PROVIDER_ID),
                deployment={"mode": deployment_mode, **(deployment or {})},
            )
        self.plan = plan
        self.event_bus = EventBus()
        self.services = {"deployment": dict(services or {})}
        self.artifact_dir = artifact_dir or tempfile.mkdtemp(prefix="valkey-fuzzer-test-")
        self.deadline = deadline
        # Obligations opened here are undone only when the test discharges them.
        self.obligations = ObligationLedger()
        self._rngs: dict[tuple[str, str], random.Random] = {}
        self._cancelled = threading.Event()

    def rng(self, component_id: str, stream: str = "default") -> random.Random:
        key = (component_id, stream)
        if key not in self._rngs:
            self._rngs[key] = self.plan.rng(component_id, stream)
        return self._rngs[key]

    @property
    def cancelled(self) -> bool:
        return self._cancelled.is_set()

    def cancel(self) -> None:
        """Cancel the call under test: ``cancelled`` turns true, ``sleep`` returns early, the deadline passes."""

        self.deadline = time.monotonic()
        self._cancelled.set()

    def sleep(self, seconds: float) -> bool:
        return not self._cancelled.wait(max(0.0, seconds))


__all__ = [
    "FAKE_PROVIDER_ID",
    "FakeContext",
    "FakeDeploymentProvider",
    "OutcomeStatus",
    "fake_deployment_providers",
    "resolved_spec",
    "run_experiment",
]
