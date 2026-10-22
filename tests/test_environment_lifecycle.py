"""Acceptance tests for the generic transactional environment lifecycle."""

import json
import os
import threading
from types import MappingProxyType

import pytest

from valkey_fuzzer.environment import (
    ArtifactProviderRegistration,
    ArtifactProviderRegistry,
    ArtifactSpec,
    ProviderContractError,
    ProviderResultError,
    ProvisionedResource,
    ResolvedArtifact,
    ResourceProviderRegistration,
    ResourceProviderRegistry,
    ResourceSpec,
    SupportDecision,
    parse_environment_reference,
)
from valkey_fuzzer.environment_lifecycle import (
    EnvironmentArtifactPathError,
    EnvironmentCleanupError,
    EnvironmentLifecycleOutcome,
    EnvironmentLifecyclePhase,
    EnvironmentManager,
    EnvironmentRegistryDriftError,
    EnvironmentReportStatus,
    EnvironmentSessionClosedError,
    EnvironmentSessionClosingError,
    EnvironmentSessionState,
    UnsupportedEnvironmentError,
)
from valkey_fuzzer.extensibility import to_plain_data


def _ref(path):
    return parse_environment_reference(path)


class _Behavior:
    def __init__(
        self,
        provider_id,
        *,
        outputs,
        provenance=None,
        services=None,
        support=None,
        acquire_error=None,
        rollback_error=None,
        cleanup_error=None,
        result_factory=None,
        acquire_hook=None,
        cleanup_hook=None,
    ):
        self.provider_id = provider_id
        self.outputs = dict(outputs)
        self.provenance = {} if provenance is None else dict(provenance)
        self.services = {} if services is None else dict(services)
        self.support = SupportDecision.allow() if support is None else support
        self.acquire_error = acquire_error
        self.rollback_error = rollback_error
        self.cleanup_error = cleanup_error
        self.result_factory = result_factory
        self.acquire_hook = acquire_hook
        self.cleanup_hook = cleanup_hook
        self.calls = []
        self.contexts = {}
        self.configs = {}

    def capture(self, method, context, spec, config=None):
        self.calls.append((method, spec.dependency.path))
        self.contexts[method] = context
        if config is not None:
            self.configs[method] = config
        context.publish_event(
            "provider.lifecycle",
            {"method": method, "dependency": spec.dependency.path},
        )


class _ArtifactProvider:
    def __init__(self, behavior):
        self.behavior = behavior

    def check_support(self, context, spec, resolved_config):
        self.behavior.capture("support", context, spec, resolved_config)
        if isinstance(self.behavior.support, BaseException):
            raise self.behavior.support
        return self.behavior.support

    def resolve(self, context, spec, resolved_config):
        self.behavior.capture("acquire", context, spec, resolved_config)
        if self.behavior.acquire_hook is not None:
            self.behavior.acquire_hook()
        if self.behavior.acquire_error is not None:
            raise self.behavior.acquire_error
        if self.behavior.result_factory is not None:
            return self.behavior.result_factory(spec)
        return ResolvedArtifact(
            artifact_id=spec.artifact_id,
            provider_id=self.behavior.provider_id,
            outputs=self.behavior.outputs,
            provenance=self.behavior.provenance,
            services=self.behavior.services,
            handle=object(),
        )

    def rollback(self, context, spec):
        self.behavior.capture("rollback", context, spec)
        if self.behavior.rollback_error is not None:
            raise self.behavior.rollback_error
        return None

    def cleanup(self, context, spec, artifact):
        self.behavior.capture("cleanup", context, spec)
        assert artifact.artifact_id == spec.artifact_id
        if self.behavior.cleanup_hook is not None:
            self.behavior.cleanup_hook()
        if self.behavior.cleanup_error is not None:
            raise self.behavior.cleanup_error
        return None


class _ResourceProvider:
    def __init__(self, behavior):
        self.behavior = behavior

    def check_support(self, context, spec, resolved_config):
        self.behavior.capture("support", context, spec, resolved_config)
        if isinstance(self.behavior.support, BaseException):
            raise self.behavior.support
        return self.behavior.support

    def provision(self, context, spec, resolved_config):
        self.behavior.capture("acquire", context, spec, resolved_config)
        if self.behavior.acquire_hook is not None:
            self.behavior.acquire_hook()
        if self.behavior.acquire_error is not None:
            raise self.behavior.acquire_error
        if self.behavior.result_factory is not None:
            return self.behavior.result_factory(spec)
        return ProvisionedResource(
            resource_id=spec.resource_id,
            provider_id=self.behavior.provider_id,
            outputs=self.behavior.outputs,
            provenance=self.behavior.provenance,
            services=self.behavior.services,
            handle=object(),
        )

    def rollback(self, context, spec):
        self.behavior.capture("rollback", context, spec)
        if self.behavior.rollback_error is not None:
            raise self.behavior.rollback_error
        return None

    def cleanup(self, context, spec, resource):
        self.behavior.capture("cleanup", context, spec)
        assert resource.resource_id == spec.resource_id
        if self.behavior.cleanup_hook is not None:
            self.behavior.cleanup_hook()
        if self.behavior.cleanup_error is not None:
            raise self.behavior.cleanup_error
        return None


def _artifact_registration(behavior):
    return ArtifactProviderRegistration(
        provider_id=behavior.provider_id,
        factory=lambda: _ArtifactProvider(behavior),
        outputs=tuple(behavior.outputs),
        services=tuple(behavior.services),
    )


def _resource_registration(behavior):
    return ResourceProviderRegistration(
        provider_id=behavior.provider_id,
        factory=lambda: _ResourceProvider(behavior),
        outputs=tuple(behavior.outputs),
        services=tuple(behavior.services),
    )


def _manager(tmp_path, artifact_behaviors=(), resource_behaviors=()):
    artifact_registry = ArtifactProviderRegistry()
    resource_registry = ResourceProviderRegistry()
    for behavior in artifact_behaviors:
        artifact_registry.register(_artifact_registration(behavior))
    for behavior in resource_behaviors:
        resource_registry.register(_resource_registration(behavior))
    return (
        EnvironmentManager(
            artifact_registry=artifact_registry,
            resource_registry=resource_registry,
            artifact_root=tmp_path,
        ),
        artifact_registry,
        resource_registry,
    )


def _call_names(*behaviors):
    return [(method, dependency) for behavior in behaviors for method, dependency in behavior.calls]


def test_acquisition_resolves_typed_references_and_scopes_services_explicitly(tmp_path):
    client = object()
    launcher = object()
    store = _Behavior(
        "tests.store",
        outputs={"endpoint": "secret://store"},
        provenance={"implementation": "fake-store"},
        services={"client": client},
    )
    candidate = _Behavior(
        "tests.candidate",
        outputs={"path": "/private/candidate"},
        provenance={"digest": "report-safe"},
        services={"launcher": launcher},
    )
    observer = _Behavior(
        "tests.observer",
        outputs={"token": "secret-observer-token"},
        provenance={"kind": "fake-observer"},
    )
    manager, _, _ = _manager(
        tmp_path,
        artifact_behaviors=(candidate,),
        resource_behaviors=(observer, store),
    )
    candidate_spec = ArtifactSpec(
        "candidate",
        "tests.candidate",
        config={"endpoint": _ref("resources.store.endpoint")},
        depends_on=("resources.store",),
    )
    observer_spec = ResourceSpec(
        "observer",
        "tests.observer",
        config={"binary": _ref("artifacts.candidate.path")},
    )
    store_spec = ResourceSpec("store", "tests.store")

    session = manager.acquire(
        (candidate_spec,),
        (observer_spec, store_spec),
        run_id="run/with spaces",
        seed=42,
        deadline=123.5,
    )

    assert store.calls[:2] == [
        ("support", "resources.store"),
        ("acquire", "resources.store"),
    ]
    assert candidate.calls[:2] == [
        ("support", "artifacts.candidate"),
        ("acquire", "artifacts.candidate"),
    ]
    assert observer.calls[:2] == [
        ("support", "resources.observer"),
        ("acquire", "resources.observer"),
    ]
    assert candidate.configs["support"] == {"endpoint": "secret://store"}
    assert candidate.configs["acquire"] == {"endpoint": "secret://store"}
    assert observer.configs["support"] == {"binary": "/private/candidate"}
    assert isinstance(candidate.configs["acquire"], MappingProxyType)

    candidate_services = candidate.contexts["support"].dependency_services
    assert tuple(candidate_services) == ("resources.store",)
    assert candidate_services["resources.store"]["client"] is client
    # The reference to the artifact inferred an ordering edge, but did not
    # implicitly grant the observer access to the artifact's launcher service.
    assert observer.contexts["support"].dependency_services == {}
    with pytest.raises(TypeError):
        candidate_services["resources.store"]["extra"] = object()

    for behavior in (store, candidate, observer):
        assert behavior.contexts["support"].deadline == 123.5
        assert behavior.contexts["acquire"].deadline == 123.5
        artifact_dir = behavior.contexts["support"].artifact_dir
        assert os.path.isabs(artifact_dir)
        relative_parts = os.path.relpath(
            os.path.realpath(artifact_dir),
            os.path.realpath(tmp_path),
        ).split(os.sep)
        assert relative_parts[0] not in (".", "..")
        assert relative_parts[1] == "providers"
        assert relative_parts[2] not in (".", "..")
        assert not os.path.exists(artifact_dir)

    binding = session.binding
    assert binding.artifacts["candidate"].services["launcher"] is launcher
    assert binding.resources["store"].services["client"] is client
    assert binding.resolve(
        {
            "binary": _ref("artifacts.candidate.path"),
            "endpoint": _ref("resources.store.endpoint"),
        }
    ) == {
        "binary": "/private/candidate",
        "endpoint": "secret://store",
    }

    acquired_report = session.report
    assert acquired_report.status is EnvironmentReportStatus.ACQUIRED
    serialized_report = json.dumps(acquired_report.to_data(), sort_keys=True)
    assert to_plain_data(acquired_report) == acquired_report.to_data()
    assert "/private/candidate" not in serialized_report
    assert "secret://store" not in serialized_report
    assert "secret-observer-token" not in serialized_report
    assert "digest" in serialized_report
    assert len(acquired_report.events) == 6
    assert {(event.dependency, event.phase) for event in acquired_report.events} == {
        ("resources.store", EnvironmentLifecyclePhase.SUPPORT),
        ("resources.store", EnvironmentLifecyclePhase.ACQUIRE),
        ("artifacts.candidate", EnvironmentLifecyclePhase.SUPPORT),
        ("artifacts.candidate", EnvironmentLifecyclePhase.ACQUIRE),
        ("resources.observer", EnvironmentLifecyclePhase.SUPPORT),
        ("resources.observer", EnvironmentLifecyclePhase.ACQUIRE),
    }

    cleaned_report = session.close()

    assert cleaned_report.status is EnvironmentReportStatus.CLEANED
    assert observer.calls[-1] == ("cleanup", "resources.observer")
    assert candidate.calls[-1] == ("cleanup", "artifacts.candidate")
    assert store.calls[-1] == ("cleanup", "resources.store")
    cleanup_records = [
        record.dependency for record in cleaned_report.records if record.phase is EnvironmentLifecyclePhase.CLEANUP
    ]
    assert cleanup_records == [
        "resources.observer",
        "artifacts.candidate",
        "resources.store",
    ]
    assert all(behavior.contexts["cleanup"].deadline is None for behavior in (store, candidate, observer))
    assert session.closed
    with pytest.raises(EnvironmentSessionClosedError) as raised:
        binding.resolve(_ref("artifacts.candidate.path"))
    assert raised.value.report is cleaned_report
    with pytest.raises(TypeError):
        to_plain_data(binding)


def test_artifact_directories_use_safe_collision_resistant_contained_segments(tmp_path):
    behavior = _Behavior(
        "tests/path-provider",
        outputs={"value": "runtime-value"},
    )
    manager, _, _ = _manager(tmp_path, artifact_behaviors=(behavior,))
    spec = ArtifactSpec("candidate", "tests/path-provider")
    run_ids = (
        ".",
        "..",
        "a/b",
        "a b",
        "a-b",
        "%2E%2E",
        "snowman-\N{SNOWMAN}",
    )
    paths = {}

    for run_id in run_ids:
        session = manager.acquire(
            (spec,),
            (),
            run_id=run_id,
            seed=42,
        )
        artifact_dir = behavior.contexts["support"].artifact_dir
        resolved_root = os.path.realpath(tmp_path)
        resolved_dir = os.path.realpath(artifact_dir)
        assert os.path.commonpath((resolved_root, resolved_dir)) == resolved_root
        relative_parts = os.path.relpath(resolved_dir, resolved_root).split(os.sep)
        assert all(part not in ("", ".", "..") for part in relative_parts)
        assert relative_parts[1] == "providers"
        paths[run_id] = artifact_dir
        session.close()

    assert len(set(paths.values())) == len(run_ids)
    repeated = manager.acquire((spec,), (), run_id="..", seed=42)
    assert behavior.contexts["support"].artifact_dir == paths[".."]
    repeated.close()


def test_artifact_directory_rejects_preexisting_symlink_escape(tmp_path):
    behavior = _Behavior("tests.path", outputs={"value": "runtime-value"})
    manager, _, _ = _manager(tmp_path, artifact_behaviors=(behavior,))
    spec = ArtifactSpec("candidate", "tests.path")
    first = manager.acquire((spec,), (), run_id="symlink-run", seed=1)
    artifact_dir = behavior.contexts["support"].artifact_dir
    first.close()
    relative = os.path.relpath(artifact_dir, os.path.realpath(tmp_path)).split(os.sep)
    run_directory = tmp_path / relative[0]
    outside = tmp_path.parent / f"{tmp_path.name}-outside"
    outside.mkdir()
    run_directory.symlink_to(outside, target_is_directory=True)

    try:
        with pytest.raises(EnvironmentArtifactPathError) as raised:
            manager.acquire((spec,), (), run_id="symlink-run", seed=1)
    finally:
        run_directory.unlink()
        outside.rmdir()

    assert raised.value.report.status is EnvironmentReportStatus.FAILED
    assert raised.value.report.records[-1].phase is EnvironmentLifecyclePhase.CONFIGURE


def test_artifact_directory_rejects_internal_symlink_alias_between_run_ids(tmp_path):
    behavior = _Behavior("tests.path", outputs={"value": "runtime-value"})
    manager, _, _ = _manager(tmp_path, artifact_behaviors=(behavior,))
    spec = ArtifactSpec("candidate", "tests.path")
    first = manager.acquire((spec,), (), run_id="first-run", seed=1)
    first_path = behavior.contexts["support"].artifact_dir
    first.close()
    second = manager.acquire((spec,), (), run_id="second-run", seed=1)
    second_path = behavior.contexts["support"].artifact_dir
    second.close()
    resolved_root = os.path.realpath(tmp_path)
    first_run = os.path.relpath(first_path, resolved_root).split(os.sep)[0]
    second_run = os.path.relpath(second_path, resolved_root).split(os.sep)[0]
    first_run_directory = tmp_path / first_run
    second_run_directory = tmp_path / second_run
    first_run_directory.mkdir()
    second_run_directory.symlink_to(first_run_directory, target_is_directory=True)

    try:
        with pytest.raises(EnvironmentArtifactPathError, match="symbolic link") as raised:
            manager.acquire((spec,), (), run_id="second-run", seed=1)
    finally:
        second_run_directory.unlink()
        first_run_directory.rmdir()

    assert raised.value.report.status is EnvironmentReportStatus.FAILED
    assert raised.value.reason == "an existing lifecycle-owned path component is a symbolic link"


def test_artifact_directory_bounds_author_controlled_instance_segment(tmp_path):
    behavior = _Behavior("tests.long-id", outputs={"value": "runtime-value"})
    manager, _, _ = _manager(tmp_path, artifact_behaviors=(behavior,))
    instance_id = "x" * 10_000

    session = manager.acquire(
        (ArtifactSpec(instance_id, "tests.long-id"),),
        (),
        run_id="long-id",
        seed=1,
    )

    artifact_dir = behavior.contexts["support"].artifact_dir
    assert len(os.path.basename(artifact_dir)) <= 73
    assert os.path.commonpath((os.path.realpath(tmp_path), artifact_dir)) == os.path.realpath(tmp_path)
    session.close()


def test_support_denial_unwinds_prior_successes_without_rolling_back_current(tmp_path):
    first = _Behavior("tests.first", outputs={"value": "runtime-first"})
    denied = _Behavior(
        "tests.denied",
        outputs={"value": "unused"},
        support=SupportDecision.deny(
            "not available",
            details={"capability": "generic-test-capability"},
        ),
    )
    manager, _, _ = _manager(tmp_path, artifact_behaviors=(first, denied))

    with pytest.raises(UnsupportedEnvironmentError) as raised:
        manager.acquire(
            (
                ArtifactSpec("first", "tests.first"),
                ArtifactSpec("denied", "tests.denied"),
            ),
            (),
            run_id="unsupported",
            seed=1,
            deadline=20.0,
        )

    error = raised.value
    assert error.dependency.path == "artifacts.denied"
    assert error.decision.reason == "not available"
    assert first.calls[-1] == ("cleanup", "artifacts.first")
    assert first.contexts["cleanup"].deadline is None
    assert denied.calls == [("support", "artifacts.denied")]
    assert "rollback" not in {method for method, _dependency in denied.calls}
    assert error.report is error.environment_report
    assert error.environment_report.status is EnvironmentReportStatus.UNSUPPORTED
    denied_records = [record for record in error.environment_report.records if record.dependency == "artifacts.denied"]
    assert denied_records[-1].outcome is EnvironmentLifecycleOutcome.UNSUPPORTED


def test_support_denial_becomes_cleanup_error_when_unwind_fails(tmp_path):
    cleanup_error = RuntimeError("cleanup secret")
    first = _Behavior(
        "tests.first",
        outputs={"value": "runtime-first"},
        cleanup_error=cleanup_error,
    )
    denied = _Behavior(
        "tests.denied",
        outputs={"value": "unused"},
        support=SupportDecision.deny("not available"),
    )
    manager, _, _ = _manager(tmp_path, artifact_behaviors=(first, denied))

    with pytest.raises(EnvironmentCleanupError) as raised:
        manager.acquire(
            (
                ArtifactSpec("first", "tests.first"),
                ArtifactSpec("denied", "tests.denied"),
            ),
            (),
            run_id="unsupported-cleanup-failure",
            seed=1,
        )

    assert isinstance(raised.value.__cause__, UnsupportedEnvironmentError)
    assert raised.value.failures[0].error is cleanup_error
    assert raised.value.report.status is EnvironmentReportStatus.CLEANUP_FAILED
    assert "cleanup secret" not in json.dumps(raised.value.report.to_data())


def test_registry_drift_during_unsupported_unwind_is_a_failure_not_unsupported(tmp_path):
    first = _Behavior("tests.first", outputs={"value": "runtime-first"})
    denied = _Behavior(
        "tests.denied",
        outputs={"value": "unused"},
        support=SupportDecision.deny("not available"),
    )
    manager, artifact_registry, _ = _manager(
        tmp_path,
        artifact_behaviors=(first, denied),
    )
    first.cleanup_hook = lambda: artifact_registry.unregister("tests.first")

    with pytest.raises(EnvironmentRegistryDriftError) as raised:
        manager.acquire(
            (
                ArtifactSpec("first", "tests.first"),
                ArtifactSpec("denied", "tests.denied"),
            ),
            (),
            run_id="unsupported-registry-drift",
            seed=1,
        )

    assert isinstance(raised.value.__cause__, UnsupportedEnvironmentError)
    assert raised.value.report.status is EnvironmentReportStatus.FAILED
    assert raised.value.environment_registry_drift is raised.value
    assert first.calls[-1] == ("cleanup", "artifacts.first")
    assert not isinstance(raised.value, UnsupportedEnvironmentError)


@pytest.mark.parametrize("error", [RuntimeError("acquire failed"), KeyboardInterrupt()])
def test_acquisition_failure_preserves_primary_and_rolls_back_before_reverse_cleanup(
    tmp_path,
    error,
):
    first = _Behavior("tests.first", outputs={"value": "runtime-first"})
    failing = _Behavior(
        "tests.failing",
        outputs={"value": "unused"},
        acquire_error=error,
    )
    manager, _, _ = _manager(tmp_path, artifact_behaviors=(first, failing))

    with pytest.raises(type(error)) as raised:
        manager.acquire(
            (
                ArtifactSpec("first", "tests.first"),
                ArtifactSpec("failing", "tests.failing"),
            ),
            (),
            run_id="failure",
            seed=2,
            deadline=30,
        )

    assert raised.value is error
    assert failing.calls[-1] == ("rollback", "artifacts.failing")
    assert first.calls[-1] == ("cleanup", "artifacts.first")
    assert failing.contexts["rollback"].deadline is None
    assert first.contexts["cleanup"].deadline is None
    assert raised.value.environment_report.status is EnvironmentReportStatus.FAILED
    teardown = [
        (record.phase, record.dependency)
        for record in raised.value.environment_report.records
        if record.phase in (EnvironmentLifecyclePhase.ROLLBACK, EnvironmentLifecyclePhase.CLEANUP)
    ]
    assert teardown == [
        (EnvironmentLifecyclePhase.ROLLBACK, "artifacts.failing"),
        (EnvironmentLifecyclePhase.CLEANUP, "artifacts.first"),
    ]


def test_exact_result_validation_failure_rolls_back_current_node(tmp_path):
    invalid = _Behavior(
        "tests.invalid",
        outputs={"required": "unused"},
        result_factory=lambda spec: ResolvedArtifact(
            artifact_id=spec.artifact_id,
            provider_id="tests.invalid",
            outputs={"unexpected": "runtime-secret"},
            provenance={},
        ),
    )
    manager, _, _ = _manager(tmp_path, artifact_behaviors=(invalid,))

    with pytest.raises(ProviderResultError, match="missing declared outputs") as raised:
        manager.acquire(
            (ArtifactSpec("invalid", "tests.invalid"),),
            (),
            run_id="invalid-result",
            seed=3,
        )

    assert invalid.calls == [
        ("support", "artifacts.invalid"),
        ("acquire", "artifacts.invalid"),
        ("rollback", "artifacts.invalid"),
    ]
    assert invalid.contexts["rollback"].deadline is None
    assert raised.value.report is raised.value.environment_report
    report_data = raised.value.environment_report.to_data()
    assert "runtime-secret" not in json.dumps(report_data)


def test_cleanup_attempts_every_lease_and_reports_no_exception_messages(tmp_path):
    first = _Behavior(
        "tests.first",
        outputs={"value": "runtime-first"},
        cleanup_error=RuntimeError("first cleanup secret"),
    )
    second = _Behavior(
        "tests.second",
        outputs={"value": "runtime-second"},
        cleanup_error=ValueError("second cleanup secret"),
    )
    manager, _, _ = _manager(tmp_path, artifact_behaviors=(first, second))
    session = manager.acquire(
        (
            ArtifactSpec("first", "tests.first"),
            ArtifactSpec("second", "tests.second"),
        ),
        (),
        run_id="cleanup-failures",
        seed=4,
    )

    with pytest.raises(EnvironmentCleanupError) as raised:
        session.close()

    error = raised.value
    assert second.calls[-1] == ("cleanup", "artifacts.second")
    assert first.calls[-1] == ("cleanup", "artifacts.first")
    assert [failure.dependency for failure in error.failures] == [
        "artifacts.second",
        "artifacts.first",
    ]
    assert error.environment_report.status is EnvironmentReportStatus.CLEANUP_FAILED
    serialized = json.dumps(error.environment_report.to_data())
    assert "first cleanup secret" not in serialized
    assert "second cleanup secret" not in serialized
    assert "RuntimeError" in serialized
    assert "ValueError" in serialized
    assert session.close() is error.environment_report


def test_cleanup_base_exception_propagates_after_remaining_leases_are_attempted(tmp_path):
    interrupt = KeyboardInterrupt()
    first = _Behavior("tests.first", outputs={"value": "runtime-first"})
    second = _Behavior(
        "tests.second",
        outputs={"value": "runtime-second"},
        cleanup_error=interrupt,
    )
    manager, _, _ = _manager(tmp_path, artifact_behaviors=(first, second))
    session = manager.acquire(
        (
            ArtifactSpec("first", "tests.first"),
            ArtifactSpec("second", "tests.second"),
        ),
        (),
        run_id="cleanup-interrupt",
        seed=4,
    )

    with pytest.raises(KeyboardInterrupt) as raised:
        session.close()

    assert raised.value is interrupt
    assert second.calls[-1] == ("cleanup", "artifacts.second")
    assert first.calls[-1] == ("cleanup", "artifacts.first")
    assert interrupt.report.status is EnvironmentReportStatus.CLEANUP_FAILED
    assert session.close() is interrupt.report


def test_reentrant_close_fails_fast_without_invoking_cleanup_twice(tmp_path):
    behavior = _Behavior("tests.reentrant", outputs={"value": "runtime-value"})
    manager, _, _ = _manager(tmp_path, artifact_behaviors=(behavior,))
    session = manager.acquire(
        (ArtifactSpec("reentrant", "tests.reentrant"),),
        (),
        run_id="reentrant-close",
        seed=4,
    )
    observations = {}

    def reenter():
        observations["state"] = session.state
        with pytest.raises(EnvironmentSessionClosingError) as binding_error:
            _ = session.binding
        observations["binding_error"] = binding_error.value
        session.close()

    behavior.cleanup_hook = reenter

    with pytest.raises(EnvironmentCleanupError) as raised:
        session.close()

    assert observations["state"] is EnvironmentSessionState.CLOSING
    assert observations["binding_error"].report.status is EnvironmentReportStatus.FAILED
    assert isinstance(
        raised.value.failures[0].error,
        EnvironmentSessionClosingError,
    )
    assert behavior.calls.count(("cleanup", "artifacts.reentrant")) == 1
    assert session.state is EnvironmentSessionState.CLOSED
    assert session.cleanup_failures == raised.value.failures
    assert session.close() is raised.value.report


def test_concurrent_close_waits_for_exactly_once_cleanup_and_reuses_report(tmp_path):
    cleanup_entered = threading.Event()
    release_cleanup = threading.Event()
    behavior = _Behavior("tests.concurrent", outputs={"value": "runtime-value"})
    manager, _, _ = _manager(tmp_path, artifact_behaviors=(behavior,))
    session = manager.acquire(
        (ArtifactSpec("concurrent", "tests.concurrent"),),
        (),
        run_id="concurrent-close",
        seed=4,
    )

    def block_cleanup():
        cleanup_entered.set()
        assert release_cleanup.wait(timeout=5)

    behavior.cleanup_hook = block_cleanup
    reports = []
    errors = []

    def close_session():
        try:
            reports.append(session.close())
        except BaseException as error:
            errors.append(error)

    first = threading.Thread(target=close_session)
    second = threading.Thread(target=close_session)
    first.start()
    assert cleanup_entered.wait(timeout=5)
    second.start()
    release_cleanup.set()
    first.join(timeout=5)
    second.join(timeout=5)

    assert not first.is_alive()
    assert not second.is_alive()
    assert errors == []
    assert len(reports) == 2
    assert reports[0] is reports[1]
    assert behavior.calls.count(("cleanup", "artifacts.concurrent")) == 1
    assert session.state is EnvironmentSessionState.CLOSED


def test_bound_liveness_check_fails_promptly_from_worker_during_cleanup(tmp_path):
    behavior = _Behavior("tests.cleanup-worker", outputs={"value": "runtime-value"})
    manager, _, _ = _manager(tmp_path, artifact_behaviors=(behavior,))
    session = manager.acquire(
        (ArtifactSpec("candidate", "tests.cleanup-worker"),),
        (),
        run_id="cleanup-worker",
        seed=4,
    )
    binding = session.binding
    worker_finished = threading.Event()
    worker_errors = []
    workers = []

    def check_binding():
        try:
            binding.assert_active()
        except BaseException as error:
            worker_errors.append(error)
        finally:
            worker_finished.set()

    def cleanup_with_worker():
        worker = threading.Thread(target=check_binding, daemon=True)
        workers.append(worker)
        worker.start()
        assert worker_finished.wait(timeout=1)

    behavior.cleanup_hook = cleanup_with_worker

    report = session.close()
    for worker in workers:
        worker.join(timeout=1)

    assert report.status is EnvironmentReportStatus.CLEANED
    assert len(worker_errors) == 1
    assert isinstance(worker_errors[0], EnvironmentSessionClosingError)
    assert all(not worker.is_alive() for worker in workers)


def test_body_base_exception_keeps_identity_when_cleanup_also_fails(tmp_path):
    cleanup_error = RuntimeError("cleanup credential")
    behavior = _Behavior(
        "tests.body",
        outputs={"value": "runtime-value"},
        cleanup_error=cleanup_error,
    )
    manager, _, _ = _manager(tmp_path, artifact_behaviors=(behavior,))
    session = manager.acquire(
        (ArtifactSpec("body", "tests.body"),),
        (),
        run_id="body-error",
        seed=5,
    )
    primary = KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt) as raised, session:
        raise primary

    assert raised.value is primary
    assert behavior.calls[-1] == ("cleanup", "artifacts.body")
    assert primary.environment_cleanup_failures[0].error is cleanup_error
    assert primary.environment_report.status is EnvironmentReportStatus.CLEANUP_FAILED
    assert "cleanup credential" not in json.dumps(primary.environment_report.to_data())


def test_cleanup_base_exception_preempts_ordinary_body_error_and_keeps_diagnostics(
    tmp_path,
):
    primary = RuntimeError("body primary")
    interrupt = KeyboardInterrupt()
    behavior = _Behavior(
        "tests.primary",
        outputs={"value": "runtime-value"},
        cleanup_error=interrupt,
    )
    manager, artifact_registry, _ = _manager(tmp_path, artifact_behaviors=(behavior,))
    session = manager.acquire(
        (ArtifactSpec("primary", "tests.primary"),),
        (),
        run_id="primary-precedence",
        seed=5,
    )
    behavior.cleanup_hook = lambda: artifact_registry.unregister("tests.primary")

    with pytest.raises(KeyboardInterrupt) as raised, session:
        raise primary

    assert raised.value is interrupt
    assert raised.value.__cause__ is primary
    assert primary.environment_cleanup_failures[0].error is interrupt
    assert primary.environment_registry_drift is not None
    assert primary.report.status is EnvironmentReportStatus.CLEANUP_FAILED
    assert interrupt.environment_cleanup_failures[0].error is interrupt
    assert interrupt.environment_registry_drift is primary.environment_registry_drift
    assert interrupt.report is primary.report
    assert session.state is EnvironmentSessionState.CLOSED
    assert session.registry_drift is primary.environment_registry_drift


def test_context_exit_attaches_retained_report_when_session_was_already_closed(tmp_path):
    behavior = _Behavior("tests.preclosed", outputs={"value": "runtime-value"})
    manager, _, _ = _manager(tmp_path, artifact_behaviors=(behavior,))
    session = manager.acquire(
        (ArtifactSpec("preclosed", "tests.preclosed"),),
        (),
        run_id="preclosed",
        seed=5,
    )
    primary = RuntimeError("body failed after explicit close")

    with pytest.raises(RuntimeError) as raised, session:
        retained_report = session.close()
        raise primary

    assert raised.value is primary
    assert primary.report is retained_report
    assert behavior.calls.count(("cleanup", "artifacts.preclosed")) == 1


def test_rollback_failure_is_attached_without_masking_acquisition_error(tmp_path):
    primary = RuntimeError("primary failure")
    rollback = RuntimeError("rollback secret")
    behavior = _Behavior(
        "tests.rollback",
        outputs={"value": "unused"},
        acquire_error=primary,
        rollback_error=rollback,
    )
    manager, _, _ = _manager(tmp_path, artifact_behaviors=(behavior,))

    with pytest.raises(RuntimeError) as raised:
        manager.acquire(
            (ArtifactSpec("rollback", "tests.rollback"),),
            (),
            run_id="rollback-failure",
            seed=6,
        )

    assert raised.value is primary
    assert primary.environment_cleanup_failures[0].error is rollback
    assert primary.environment_cleanup_failures[0].phase is EnvironmentLifecyclePhase.ROLLBACK
    assert primary.environment_report.status is EnvironmentReportStatus.CLEANUP_FAILED
    assert "rollback secret" not in json.dumps(primary.environment_report.to_data())


def test_rollback_base_exception_preempts_ordinary_acquisition_error(tmp_path):
    primary = RuntimeError("acquisition primary")
    interrupt = KeyboardInterrupt()
    behavior = _Behavior(
        "tests.rollback-interrupt",
        outputs={"value": "unused"},
        acquire_error=primary,
        rollback_error=interrupt,
    )
    manager, _, _ = _manager(tmp_path, artifact_behaviors=(behavior,))

    with pytest.raises(KeyboardInterrupt) as raised:
        manager.acquire(
            (ArtifactSpec("rollback-interrupt", "tests.rollback-interrupt"),),
            (),
            run_id="rollback-interrupt",
            seed=6,
        )

    assert raised.value is interrupt
    assert raised.value.__cause__ is primary
    assert primary.environment_cleanup_failures[0].error is interrupt
    assert primary.report.status is EnvironmentReportStatus.CLEANUP_FAILED
    assert interrupt.environment_cleanup_failures[0].error is interrupt
    assert interrupt.report is primary.report


def test_diagnostic_attachment_can_never_mask_a_hostile_primary_exception(tmp_path):
    class HostilePrimaryError(RuntimeError):
        def __setattr__(self, name, value):
            if name in {
                "report",
                "environment_report",
                "environment_cleanup_failures",
                "environment_registry_drift",
            }:
                raise AssertionError("diagnostics rejected")
            super().__setattr__(name, value)

    primary = HostilePrimaryError("primary")
    behavior = _Behavior(
        "tests.hostile-primary",
        outputs={"value": "unused"},
        acquire_error=primary,
    )
    manager, _, _ = _manager(tmp_path, artifact_behaviors=(behavior,))

    with pytest.raises(HostilePrimaryError) as raised:
        manager.acquire(
            (ArtifactSpec("hostile-primary", "tests.hostile-primary"),),
            (),
            run_id="hostile-primary",
            seed=6,
        )

    assert raised.value is primary
    assert behavior.calls[-1] == ("rollback", "artifacts.hostile-primary")


def test_support_contract_failure_does_not_rollback_nonmutating_phase(tmp_path):
    invalid_support = _Behavior(
        "tests.support",
        outputs={"value": "unused"},
        support="yes",
    )
    manager, _, _ = _manager(tmp_path, artifact_behaviors=(invalid_support,))

    with pytest.raises(ProviderContractError, match="must return SupportDecision"):
        manager.acquire(
            (ArtifactSpec("support", "tests.support"),),
            (),
            run_id="support-contract",
            seed=7,
        )

    assert invalid_support.calls == [("support", "artifacts.support")]


def test_resolved_config_validator_cannot_be_bypassed_by_symbolic_reference(tmp_path):
    store = _Behavior(
        "tests.validation-store",
        outputs={"endpoint": "concrete-invalid-endpoint"},
    )
    candidate = _Behavior(
        "tests.validation-candidate",
        outputs={"value": "unused"},
    )
    observed = []
    factory_calls = []

    def validate_static(spec):
        observed.append(("static", spec.config["endpoint"]))

    def validate_resolved(spec, resolved_config):
        observed.append(("resolved", resolved_config["endpoint"]))
        assert spec.config["endpoint"] == _ref("resources.store.endpoint")
        raise ValueError("resolved constraint rejected")

    def candidate_factory():
        factory_calls.append("created")
        return _ArtifactProvider(candidate)

    artifact_registry = ArtifactProviderRegistry()
    resource_registry = ResourceProviderRegistry()
    artifact_registry.register(
        ArtifactProviderRegistration(
            provider_id="tests.validation-candidate",
            factory=candidate_factory,
            outputs=("value",),
            validator=validate_static,
            resolved_config_validator=validate_resolved,
        )
    )
    resource_registry.register(_resource_registration(store))
    manager = EnvironmentManager(
        artifact_registry=artifact_registry,
        resource_registry=resource_registry,
        artifact_root=tmp_path,
    )

    with pytest.raises(ValueError, match="resolved constraint rejected") as raised:
        manager.acquire(
            (
                ArtifactSpec(
                    "candidate",
                    "tests.validation-candidate",
                    config={"endpoint": _ref("resources.store.endpoint")},
                ),
            ),
            (ResourceSpec("store", "tests.validation-store"),),
            run_id="resolved-validation",
            seed=8,
        )

    assert observed == [
        ("static", _ref("resources.store.endpoint")),
        ("resolved", "concrete-invalid-endpoint"),
    ]
    assert factory_calls == []
    assert candidate.calls == []
    assert store.calls[-1] == ("cleanup", "resources.store")
    assert raised.value.report is raised.value.environment_report
    assert raised.value.report.status is EnvironmentReportStatus.FAILED
    assert raised.value.report.records[-1].phase is EnvironmentLifecyclePhase.CLEANUP


def test_registry_entry_drift_after_valid_result_cleans_current_instead_of_rollback(tmp_path):
    behavior = _Behavior(
        "tests.drift",
        outputs={"value": "runtime-value"},
    )
    manager, artifact_registry, _ = _manager(tmp_path, artifact_behaviors=(behavior,))
    behavior.acquire_hook = lambda: artifact_registry.unregister("tests.drift")

    with pytest.raises(EnvironmentRegistryDriftError) as raised:
        manager.acquire(
            (ArtifactSpec("drift", "tests.drift"),),
            (),
            run_id="registry-drift",
            seed=8,
        )

    assert behavior.calls == [
        ("support", "artifacts.drift"),
        ("acquire", "artifacts.drift"),
        ("cleanup", "artifacts.drift"),
    ]
    assert raised.value.environment_report.status is EnvironmentReportStatus.FAILED
    assert raised.value.environment_registry_drift is raised.value
    assert "tests.drift" not in artifact_registry


def test_session_detects_registry_drift_but_still_cleans_with_pinned_provider(tmp_path):
    behavior = _Behavior("tests.session-drift", outputs={"value": "runtime-value"})
    manager, artifact_registry, _ = _manager(tmp_path, artifact_behaviors=(behavior,))
    session = manager.acquire(
        (ArtifactSpec("session-drift", "tests.session-drift"),),
        (),
        run_id="session-drift",
        seed=9,
    )
    artifact_registry.unregister("tests.session-drift")

    with pytest.raises(EnvironmentRegistryDriftError) as binding_error:
        _ = session.binding
    assert binding_error.value.report.status is EnvironmentReportStatus.FAILED
    with pytest.raises(EnvironmentRegistryDriftError) as raised:
        session.close()

    assert behavior.calls[-1] == ("cleanup", "artifacts.session-drift")
    assert behavior.contexts["cleanup"].deadline is None
    assert raised.value.environment_report.status is EnvironmentReportStatus.FAILED
    assert session.closed


def test_provider_event_details_must_be_report_safe_and_trigger_rollback_if_invalid(tmp_path):
    class _UnsafeEventArtifactProvider(_ArtifactProvider):
        def resolve(self, context, spec, resolved_config):
            self.behavior.capture("acquire", context, spec, resolved_config)
            context.publish_event("unsafe", {"service": object()})

    behavior = _Behavior("tests.events", outputs={"value": "unused"})
    artifact_registry = ArtifactProviderRegistry()
    resource_registry = ResourceProviderRegistry()
    artifact_registry.register(
        ArtifactProviderRegistration(
            provider_id=behavior.provider_id,
            factory=lambda: _UnsafeEventArtifactProvider(behavior),
            outputs=("value",),
        )
    )
    manager = EnvironmentManager(
        artifact_registry=artifact_registry,
        resource_registry=resource_registry,
        artifact_root=tmp_path,
    )

    with pytest.raises(TypeError, match="JSON-safe") as raised:
        manager.acquire(
            (ArtifactSpec("events", "tests.events"),),
            (),
            run_id="unsafe-event",
            seed=10,
        )

    assert behavior.calls[-1] == ("rollback", "artifacts.events")
    assert raised.value.environment_report.status is EnvironmentReportStatus.FAILED


def test_empty_environment_and_input_validation_are_deterministic(tmp_path):
    manager, _, _ = _manager(tmp_path)
    session = manager.acquire((), (), run_id="empty", seed=0)

    assert session.binding.artifacts == {}
    assert session.binding.resources == {}
    assert session.close().to_data() == {
        "run_id": "empty",
        "status": "cleaned",
        "records": [],
        "events": [],
    }

    with pytest.raises(TypeError, match="seed"):
        manager.acquire((), (), run_id="empty", seed=True)
    with pytest.raises(ValueError, match="deadline"):
        manager.acquire((), (), run_id="empty", seed=0, deadline=float("inf"))
    with pytest.raises(ValueError, match="absolute"):
        EnvironmentManager(
            artifact_registry=ArtifactProviderRegistry(),
            resource_registry=ResourceProviderRegistry(),
            artifact_root="relative",
        )


def test_artifact_and_resource_bindings_remain_kind_separated(tmp_path):
    artifact = _Behavior("tests.shared", outputs={"value": "artifact-runtime"})
    resource = _Behavior("tests.shared", outputs={"value": "resource-runtime"})
    manager, _, _ = _manager(
        tmp_path,
        artifact_behaviors=(artifact,),
        resource_behaviors=(resource,),
    )
    session = manager.acquire(
        (ArtifactSpec("same-id", "tests.shared"),),
        (ResourceSpec("same-id", "tests.shared"),),
        run_id="kind-separated",
        seed=11,
    )

    assert session.binding.artifacts["same-id"].outputs["value"] == "artifact-runtime"
    assert session.binding.resources["same-id"].outputs["value"] == "resource-runtime"
    assert session.binding.resolve(_ref("artifacts.same-id.value")) == "artifact-runtime"
    assert session.binding.resolve(_ref("resources.same-id.value")) == "resource-runtime"
    assert session.binding.artifacts["same-id"].artifact_id == "same-id"
    assert session.binding.resources["same-id"].resource_id == "same-id"

    session.close()
