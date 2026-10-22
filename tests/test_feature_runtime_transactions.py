"""Atomic pinning and replay validation for the complete extension environment."""

import threading
from typing import Any, Callable, Optional

import pytest

from src.environment import (
    ArtifactProviderRegistration,
    ArtifactSpec,
    EnvironmentDependencyCycleError,
    EnvironmentKind,
    EnvironmentReference,
    PolicySpec,
    ResolvedArtifact,
    ResourceProviderRegistration,
    ResourceSpec,
    SupportDecision,
    UndeclaredEnvironmentOutputError,
    UnknownEnvironmentTargetError,
    UnknownProviderError,
)
from src.extensibility import (
    ComponentKind,
    ComponentRegistration,
    ComponentResult,
    ComponentSpec,
    ComponentStatus,
    FeaturePackSpec,
    NodeSelector,
    OutcomeStatus,
    RunMode,
    RunPlan,
    StartupOverlay,
    TopologyProviderRegistration,
    discover_plugins,
)
from src.feature_packs import FeaturePackCatalog
from src.feature_runner import FeatureRunnerError, ProvisionedTopology
from src.feature_runtime import FeatureRuntime, FeatureRuntimeError


class _TopologyProvider:
    def validate(self, plan):
        return None

    def provision(self, context):
        return ProvisionedTopology()

    def rollback(self, context):
        return None

    def cleanup(self, context, topology):
        return None


class _Action:
    def __init__(self, callback: Optional[Callable[[], None]] = None):
        self._callback = callback

    def run(self, context, spec):
        if self._callback is not None:
            self._callback()
        return ComponentResult(
            component_instance_id=spec.instance_id,
            status=ComponentStatus.PASS,
            summary="done",
        )


class _AbortOperation(BaseException):
    pass


def _pack(
    pack_id: str = "example.feature",
    *,
    components: tuple[ComponentSpec, ...] = (
        ComponentSpec(
            kind=ComponentKind.ACTION,
            component_id="example.action",
            instance_id="exercise",
        ),
    ),
) -> FeaturePackSpec:
    return FeaturePackSpec(
        pack_id=pack_id,
        version="1",
        mode=RunMode.TOPOLOGY_SMOKE,
        supported_topologies=("standalone",),
        components=components,
    )


def _artifact_registration(provider_id: str = "example.artifact") -> ArtifactProviderRegistration:
    return ArtifactProviderRegistration(
        provider_id=provider_id,
        outputs=("path",),
        factory=lambda: object(),
    )


def _resource_registration(provider_id: str = "example.resource") -> ResourceProviderRegistration:
    return ResourceProviderRegistration(
        provider_id=provider_id,
        outputs=("endpoint",),
        factory=lambda: object(),
    )


def _topology_registration(
    topology: str = "example-topology",
) -> TopologyProviderRegistration:
    return TopologyProviderRegistration(
        topology=topology,
        provider_id="example.topology-provider",
        implementation_version="1",
        api_version="v1",
        factory=_TopologyProvider,
    )


def _runtime(
    *,
    action_validator=None,
    action_callback: Optional[Callable[[], None]] = None,
) -> FeatureRuntime:
    runtime = FeatureRuntime(
        discover_entry_points=False,
        topology_providers={"standalone": _topology_registration("standalone")},
    )
    runtime.catalog.register(_pack())
    runtime.registries[ComponentKind.ACTION].register(
        ComponentRegistration[Any](
            component_id="example.action",
            kind=ComponentKind.ACTION,
            api_version="v1",
            factory=lambda: _Action(action_callback),
            validator=action_validator,
        )
    )
    runtime.artifact_providers.register(_artifact_registration())
    runtime.resource_providers.register(_resource_registration())
    return runtime


def _scenario() -> dict[str, Any]:
    return {
        "scenario_id": "transaction-test",
        "seed": 7,
        "feature": {
            "use": "example.feature",
            "version": "1",
            "with": {},
        },
        "topology": {"type": "standalone"},
    }


def _plan(runtime: FeatureRuntime) -> RunPlan:
    return RunPlan.from_feature_pack(
        runtime.catalog.resolve("example.feature", "1"),
        scenario_id="transaction-test",
        seed=7,
        topology="standalone",
        topology_provider=runtime.topology_providers.resolve("standalone").identity,
    )


def _assert_same_snapshot(before, after) -> None:
    (
        before_components,
        before_artifacts,
        before_resources,
        before_topologies,
        before_catalog,
    ) = before
    (
        after_components,
        after_artifacts,
        after_resources,
        after_topologies,
        after_catalog,
    ) = after
    assert set(after_components) == set(before_components)
    for kind, registrations in before_components.items():
        assert set(after_components[kind]) == set(registrations)
        assert all(
            after_components[kind][registration_id] is registration
            for registration_id, registration in registrations.items()
        )
    for expected, actual in (
        (before_artifacts, after_artifacts),
        (before_resources, after_resources),
        (before_topologies, after_topologies),
        (before_catalog, after_catalog),
    ):
        assert set(actual) == set(expected)
        assert all(actual[entry_id] is entry for entry_id, entry in expected.items())


def _mutate_complete_environment(
    runtime: FeatureRuntime,
    *,
    bypass_catalog_guard: bool = False,
    mutate_topology: bool = True,
) -> None:
    runtime.registries[ComponentKind.ACTION].register(
        ComponentRegistration[Any](
            component_id="example.injected-action",
            kind=ComponentKind.ACTION,
            api_version="v1",
            factory=_Action,
        )
    )
    runtime.artifact_providers.register(_artifact_registration("example.injected-artifact"))
    runtime.resource_providers.register(_resource_registration("example.injected-resource"))
    if mutate_topology:
        runtime.topology_providers.register(_topology_registration("example-injected-topology"))
    injected_pack = _pack("example.injected-feature", components=())
    if bypass_catalog_guard:
        FeaturePackCatalog.register_many(runtime.catalog, (injected_pack,))
    else:
        runtime.catalog.register(injected_pack)


def test_compile_restores_complete_environment_and_preserves_base_exception_identity():
    sentinel = _AbortOperation()
    runtime: FeatureRuntime

    def mutate_and_abort(spec, plan):
        _mutate_complete_environment(runtime, mutate_topology=False)
        raise sentinel

    runtime = _runtime(action_validator=mutate_and_abort)
    before = runtime._extension_snapshot()

    with pytest.raises(_AbortOperation) as captured:
        runtime.compile(_scenario())

    assert captured.value is sentinel
    _assert_same_snapshot(before, runtime._extension_snapshot())
    assert runtime._extension_pin is None


def test_validate_restores_detected_drift_and_runtime_remains_usable():
    first_call = True
    runtime: FeatureRuntime

    def mutate_once(spec, plan):
        nonlocal first_call
        if first_call:
            first_call = False
            _mutate_complete_environment(runtime, mutate_topology=False)

    runtime = _runtime(action_validator=mutate_once)
    plan = _plan(runtime)
    before = runtime._extension_snapshot()

    with pytest.raises(FeatureRuntimeError, match="changed during the runtime operation"):
        runtime.validate_plan(plan)

    _assert_same_snapshot(before, runtime._extension_snapshot())
    assert runtime._extension_pin is None
    runtime.validate_plan(plan)
    assert runtime._extension_pin is not None


def test_run_restores_complete_environment_and_preserves_base_exception_identity():
    sentinel = _AbortOperation()
    runtime: FeatureRuntime

    def mutate_and_abort():
        _mutate_complete_environment(runtime, mutate_topology=False)
        raise sentinel

    runtime = _runtime(action_callback=mutate_and_abort)
    plan = _plan(runtime)
    before = runtime._extension_snapshot()

    with pytest.raises(_AbortOperation) as captured:
        runtime.run_plan(plan)

    assert captured.value is sentinel
    _assert_same_snapshot(before, runtime._extension_snapshot())
    assert runtime._extension_pin is None


def test_worker_thread_can_read_bound_values_while_runtime_registries_are_pinned(
    tmp_path,
):
    """Bound liveness checks must not reacquire main-thread registry locks."""

    worker_threads = []
    worker_values = []
    worker_errors = []

    class ArtifactProvider:
        def check_support(self, context, spec, resolved_config):
            return SupportDecision.allow()

        def resolve(self, context, spec, resolved_config):
            return ResolvedArtifact(
                artifact_id=spec.artifact_id,
                provider_id=spec.provider_id,
                outputs={"token": "resolved-in-worker"},
                provenance={},
            )

        def rollback(self, context, spec):
            return None

        def cleanup(self, context, spec, artifact):
            return None

    class ThreadedAction:
        def run(self, context, spec):
            def read_bound_value():
                try:
                    worker_values.append(spec.config["token"])
                except BaseException as exc:
                    worker_errors.append(exc)

            worker = threading.Thread(target=read_bound_value, daemon=True)
            worker_threads.append(worker)
            worker.start()
            worker.join(timeout=1)
            if worker.is_alive():
                raise RuntimeError("worker could not read bound configuration")
            if worker_errors:
                raise worker_errors[0]
            return ComponentResult(
                component_instance_id=spec.instance_id,
                status=ComponentStatus.PASS,
                summary="worker read bound configuration",
            )

    runtime = FeatureRuntime(
        discover_entry_points=False,
        topology_providers={"standalone": _topology_registration("standalone")},
        artifact_dir=str(tmp_path),
    )
    runtime.registries[ComponentKind.ACTION].register(
        ComponentRegistration[Any](
            component_id="example.threaded-action",
            kind=ComponentKind.ACTION,
            api_version="v1",
            factory=ThreadedAction,
        )
    )
    runtime.artifact_providers.register(
        ArtifactProviderRegistration(
            provider_id="example.threaded-artifact",
            outputs=("token",),
            factory=ArtifactProvider,
        )
    )
    plan = RunPlan(
        scenario_id="threaded-bound-read",
        pack_id="example.threaded",
        pack_version="1",
        seed=19,
        topology="standalone",
        components=(
            ComponentSpec(
                kind=ComponentKind.ACTION,
                component_id="example.threaded-action",
                instance_id="exercise",
                config={
                    "token": EnvironmentReference(
                        EnvironmentKind.ARTIFACT,
                        "candidate",
                        "token",
                    )
                },
            ),
        ),
        topology_provider=_topology_registration("standalone").identity,
        mode=RunMode.TOPOLOGY_SMOKE,
        artifacts=(
            ArtifactSpec(
                artifact_id="candidate",
                provider_id="example.threaded-artifact",
            ),
        ),
        topology_document={"type": "standalone"},
    )

    outcome = runtime.run_plan(plan)
    for worker in worker_threads:
        worker.join(timeout=1)

    assert outcome.status is OutcomeStatus.PASS
    assert worker_values == ["resolved-in-worker"]
    assert worker_errors == []
    assert all(not worker.is_alive() for worker in worker_threads)


def test_worker_thread_can_read_topology_registry_while_runtime_snapshot_is_pinned():
    worker_values = []
    worker_errors = []
    worker_threads = []
    runtime: FeatureRuntime

    def read_topology_registry():
        try:
            registration = runtime.topology_providers.resolve("standalone")
            snapshot = runtime.topology_providers.snapshot()
            worker_values.append(
                (
                    registration.identity,
                    snapshot["standalone"].identity,
                )
            )
        except BaseException as exc:
            worker_errors.append(exc)

    def action_callback():
        worker = threading.Thread(
            target=read_topology_registry,
            name="topology-registry-reader",
            daemon=True,
        )
        worker_threads.append(worker)
        worker.start()
        worker.join(timeout=1)
        if worker.is_alive():
            raise RuntimeError("worker could not read the topology registry while the runtime snapshot was pinned")
        if worker_errors:
            raise worker_errors[0]

    runtime = _runtime(action_callback=action_callback)
    registration = runtime.topology_providers.resolve("standalone")

    outcome = runtime.run_plan(_plan(runtime))
    for worker in worker_threads:
        worker.join(timeout=1)

    assert outcome.status is OutcomeStatus.PASS
    assert worker_values == [(registration.identity, registration.identity)]
    assert worker_errors == []
    assert all(not worker.is_alive() for worker in worker_threads)


def test_runtime_and_plugin_discovery_lock_order_both_make_progress():
    validator_entered = threading.Event()
    release_validator = threading.Event()
    discovery_lock_attempted = threading.Event()
    runtime_errors = []
    discovery_errors = []
    runtime: FeatureRuntime

    def blocking_validator(spec, plan):
        del spec, plan
        validator_entered.set()
        if not release_validator.wait(timeout=2):
            raise RuntimeError("test did not release the validator")

    runtime = _runtime(action_validator=blocking_validator)
    plan = _plan(runtime)
    first_kind = min(runtime.registries, key=lambda kind: kind.value)
    first_registry = runtime.registries[first_kind]
    original_lock = first_registry._lock

    class ObservedLock:
        def acquire(self, *args, **kwargs):
            if threading.current_thread().name == "plugin-discovery":
                discovery_lock_attempted.set()
            return original_lock.acquire(*args, **kwargs)

        def release(self):
            return original_lock.release()

        def __enter__(self):
            self.acquire()
            return self

        def __exit__(self, exc_type, exc, traceback):
            del exc_type, exc, traceback
            self.release()

    first_registry._lock = ObservedLock()

    def validate_runtime():
        try:
            runtime.validate_plan(plan)
        except BaseException as exc:
            runtime_errors.append(exc)

    def run_discovery():
        try:
            discover_plugins(
                runtime.registries,
                artifact_provider_registry=runtime.artifact_providers,
                resource_provider_registry=runtime.resource_providers,
                topology_provider_registry=runtime.topology_providers,
            )
        except BaseException as exc:
            discovery_errors.append(exc)

    runtime_thread = threading.Thread(
        target=validate_runtime,
        name="runtime-validation",
        daemon=True,
    )
    discovery_thread = threading.Thread(
        target=run_discovery,
        name="plugin-discovery",
        daemon=True,
    )
    runtime_thread.start()
    assert validator_entered.wait(timeout=1)
    discovery_thread.start()
    assert discovery_lock_attempted.wait(timeout=1)

    release_validator.set()
    runtime_thread.join(timeout=2)
    discovery_thread.join(timeout=2)

    assert not runtime_thread.is_alive()
    assert not discovery_thread.is_alive()
    assert runtime_errors == []
    assert discovery_errors == []


def test_entry_drift_is_repaired_rejected_once_and_does_not_poison_runtime():
    runtime = _runtime()
    plan = runtime.compile(_scenario())
    pinned = runtime._extension_snapshot()
    _mutate_complete_environment(runtime, bypass_catalog_guard=True)

    with pytest.raises(FeatureRuntimeError) as captured:
        runtime.validate_plan(plan)

    message = str(captured.value)
    assert "action component registry" in message
    assert "artifact provider registry" in message
    assert "resource provider registry" in message
    assert "topology provider registry" in message
    assert "feature-pack catalog" in message
    assert "changed after the extension environment was pinned" in message
    _assert_same_snapshot(pinned, runtime._extension_snapshot())

    runtime.validate_plan(plan)


def test_replayed_plan_uses_the_pinned_topology_registration_identity():
    runtime = _runtime()
    plan = runtime.compile(_scenario())
    original = runtime.topology_providers.resolve("standalone")
    runtime.topology_providers.unregister("standalone")
    runtime.topology_providers.register(_topology_registration("standalone"))

    with pytest.raises(
        FeatureRuntimeError,
        match=r"topology provider registry.*changed after",
    ):
        runtime.run_plan(plan)

    assert runtime.topology_providers.resolve("standalone") is original
    assert runtime.run_plan(plan).status is OutcomeStatus.PASS


def test_cross_runtime_replay_rejects_different_provider_before_factory_or_environment(
    monkeypatch,
):
    source_runtime = _runtime()
    replayed = RunPlan.from_json(source_runtime.compile(_scenario()).to_json())
    calls = []
    target_registration = TopologyProviderRegistration(
        topology="standalone",
        provider_id="example.different-topology-provider",
        implementation_version="2",
        api_version="v1",
        factory=lambda: calls.append("factory") or _TopologyProvider(),
    )
    target_runtime = FeatureRuntime(
        discover_entry_points=False,
        topology_providers={"standalone": target_registration},
    )
    target_runtime.catalog.register(_pack())
    target_runtime.registries[ComponentKind.ACTION].register(
        ComponentRegistration[Any](
            component_id="example.action",
            kind=ComponentKind.ACTION,
            api_version="v1",
            factory=_Action,
        )
    )
    monkeypatch.setattr(
        target_runtime._environment_manager,
        "acquire",
        lambda *args, **kwargs: pytest.fail("environment acquisition must not run for a provider mismatch"),
    )

    with pytest.raises(FeatureRunnerError, match="does not match"):
        target_runtime.run_plan(replayed)

    assert calls == []
    assert target_runtime._extension_pin is None


def test_public_catalog_mutation_is_rejected_immediately_after_pinning():
    runtime = _runtime()
    runtime.compile(_scenario())
    pinned = runtime._extension_snapshot()

    with pytest.raises(FeatureRuntimeError, match="catalog cannot be mutated after"):
        runtime.catalog.register(_pack("example.late-feature", components=()))

    _assert_same_snapshot(pinned, runtime._extension_snapshot())


def test_loading_feature_pack_directories_is_rejected_after_pinning(tmp_path):
    runtime = _runtime()
    runtime.compile(_scenario())
    pinned = runtime._extension_snapshot()
    (tmp_path / "unused.yaml").write_text("not: [valid", encoding="utf-8")

    with pytest.raises(FeatureRuntimeError, match="cannot be loaded after"):
        runtime.load_feature_pack_directories(tmp_path)

    _assert_same_snapshot(pinned, runtime._extension_snapshot())


def _replayed_plan(case: str) -> RunPlan:
    if case == "unknown-provider":
        artifacts = (ArtifactSpec("candidate", "missing.artifact"),)
        resources = ()
    elif case == "undeclared-output":
        artifacts = (ArtifactSpec("candidate", "example.artifact"),)
        resources = (
            ResourceSpec(
                "cache",
                "example.resource",
                config={
                    "source": EnvironmentReference(
                        EnvironmentKind.ARTIFACT,
                        "candidate",
                        "missing",
                    )
                },
            ),
        )
    else:
        artifacts = (
            ArtifactSpec(
                "candidate",
                "example.artifact",
                depends_on=("resources.cache",),
            ),
        )
        resources = (
            ResourceSpec(
                "cache",
                "example.resource",
                depends_on=("artifacts.candidate",),
            ),
        )
    plan = RunPlan(
        scenario_id="replayed-plan",
        pack_id="example.replayed",
        pack_version="1",
        seed=11,
        topology="standalone",
        components=(),
        topology_provider=_topology_registration("standalone").identity,
        mode=RunMode.TOPOLOGY_SMOKE,
        artifacts=artifacts,
        resources=resources,
        topology_document={"type": "standalone"},
    )
    return RunPlan.from_json(plan.to_json())


@pytest.mark.parametrize("operation", ("validate_plan", "run_plan"))
@pytest.mark.parametrize(
    ("case", "error_type"),
    (
        ("unknown-provider", UnknownProviderError),
        ("undeclared-output", UndeclaredEnvironmentOutputError),
        ("dependency-cycle", EnvironmentDependencyCycleError),
    ),
)
def test_replayed_plans_revalidate_environment_before_runner_or_pinning(
    operation,
    case,
    error_type,
    monkeypatch,
):
    runtime = _runtime()
    plan = _replayed_plan(case)
    monkeypatch.setattr(
        runtime._runner,
        "validate" if operation == "validate_plan" else "run",
        lambda *args, **kwargs: pytest.fail("runner must not receive an invalid replayed environment"),
    )

    with pytest.raises(error_type):
        getattr(runtime, operation)(plan)

    assert runtime._extension_pin is None


def _invalid_consumer_replayed_plan(case: str) -> tuple[RunPlan, str]:
    missing_output = EnvironmentReference(
        EnvironmentKind.ARTIFACT,
        "candidate",
        "missing",
    )
    artifacts = [ArtifactSpec("candidate", "example.artifact")]
    resources = []
    components = []
    policies = []
    startup_overlays = []
    topology_document = {"type": "standalone"}

    if case == "topology":
        topology_document["binary"] = missing_output
        source = "topology"
    elif case == "component":
        components.append(
            ComponentSpec(
                kind=ComponentKind.ACTION,
                component_id="example.action",
                instance_id="exercise",
                config={"binary": missing_output},
            )
        )
        source = "components.exercise.config"
    elif case == "overlay-selector":
        startup_overlays.append(
            StartupOverlay(
                overlay_id="candidate-overlay",
                selector=NodeSelector(labels={"binary": missing_output}),
                server_config={"static": "value"},
            )
        )
        source = "startup_overlays.candidate-overlay.selector.labels"
    elif case == "overlay-config":
        startup_overlays.append(
            StartupOverlay(
                overlay_id="candidate-overlay",
                server_config={"binary": missing_output},
            )
        )
        source = "startup_overlays.candidate-overlay.server_config"
    elif case == "policy":
        policies.append(
            PolicySpec(
                policy_id="guard",
                policy_type="example.policy",
                config={"binary": missing_output},
            )
        )
        source = "policies.guard"
    elif case == "artifact-config":
        artifacts.append(
            ArtifactSpec(
                "consumer",
                "example.artifact",
                config={"binary": missing_output},
            )
        )
        source = "artifacts.consumer"
    elif case == "resource-config":
        resources.append(
            ResourceSpec(
                "cache",
                "example.resource",
                config={"binary": missing_output},
            )
        )
        source = "resources.cache"
    elif case == "artifact-dependency":
        artifacts.append(
            ArtifactSpec(
                "consumer",
                "example.artifact",
                depends_on=("resources.missing",),
            )
        )
        source = "artifacts.consumer"
    elif case == "resource-dependency":
        resources.append(
            ResourceSpec(
                "cache",
                "example.resource",
                depends_on=("artifacts.missing",),
            )
        )
        source = "resources.cache"
    else:  # pragma: no cover - test helper guard
        raise AssertionError(f"unknown invalid consumer case {case!r}")

    plan = RunPlan(
        scenario_id=f"replayed-{case}",
        pack_id="example.replayed",
        pack_version="1",
        seed=11,
        topology="standalone",
        components=tuple(components),
        topology_provider=_topology_registration("standalone").identity,
        mode=RunMode.TOPOLOGY_SMOKE,
        artifacts=tuple(artifacts),
        resources=tuple(resources),
        policies=tuple(policies),
        startup_overlays=tuple(startup_overlays),
        topology_document=topology_document,
    )
    return RunPlan.from_json(plan.to_json()), source


@pytest.mark.parametrize("operation", ("validate_plan", "run_plan"))
@pytest.mark.parametrize(
    ("case", "error_type"),
    (
        ("topology", UndeclaredEnvironmentOutputError),
        ("component", UndeclaredEnvironmentOutputError),
        ("overlay-selector", UndeclaredEnvironmentOutputError),
        ("overlay-config", UndeclaredEnvironmentOutputError),
        ("policy", UndeclaredEnvironmentOutputError),
        ("artifact-config", UndeclaredEnvironmentOutputError),
        ("resource-config", UndeclaredEnvironmentOutputError),
        ("artifact-dependency", UnknownEnvironmentTargetError),
        ("resource-dependency", UnknownEnvironmentTargetError),
    ),
)
def test_replayed_plans_revalidate_every_environment_consumer(
    operation,
    case,
    error_type,
    monkeypatch,
):
    runtime = _runtime()
    plan, source = _invalid_consumer_replayed_plan(case)
    monkeypatch.setattr(
        runtime._runner,
        "validate" if operation == "validate_plan" else "run",
        lambda *args, **kwargs: pytest.fail("runner must not receive an invalid replayed environment"),
    )

    with pytest.raises(error_type) as captured:
        getattr(runtime, operation)(plan)

    assert captured.value.source == source
    assert runtime._extension_pin is None
