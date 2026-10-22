"""Focused tests for the pre-topology environment contracts."""

import json
import threading
from dataclasses import FrozenInstanceError
from types import MappingProxyType

import pytest

from src.environment import (
    ENVIRONMENT_EXTENSION_API_VERSION,
    ENVIRONMENT_REFERENCE_DATA_KEY,
    ArtifactProvider,
    ArtifactProviderRegistration,
    ArtifactProviderRegistry,
    ArtifactSpec,
    DuplicateProviderRegistrationError,
    EnvironmentDependency,
    EnvironmentDependencyCycleError,
    EnvironmentKind,
    EnvironmentProviderContext,
    EnvironmentReference,
    EnvironmentRegistryChangedError,
    IncompatibleProviderRegistrationError,
    InvalidEnvironmentReferenceError,
    PolicySpec,
    ProviderContractError,
    ProviderResultError,
    ProvisionedResource,
    ResolvedArtifact,
    ResourceProvider,
    ResourceProviderRegistration,
    ResourceProviderRegistry,
    ResourceSpec,
    SupportDecision,
    UndeclaredEnvironmentOutputError,
    UnknownEnvironmentTargetError,
    UnknownProviderError,
    UnresolvedEnvironmentReferenceError,
    collect_environment_references,
    environment_references_from_data,
    parse_environment_dependency,
    parse_environment_reference,
    resolve_environment_references,
    stable_environment_order,
    validate_environment_dependencies,
)
from src.extensibility import to_plain_data
from src.extension_contract import EXTENSION_API_VERSION
from src.run_state import canonical_snapshot


def _ref(path: str) -> EnvironmentReference:
    return parse_environment_reference(path)


class _ArtifactProvider:
    def check_support(self, context, spec, resolved_config):
        return SupportDecision.allow()

    def resolve(self, context, spec, resolved_config):
        return ResolvedArtifact(
            artifact_id=spec.artifact_id,
            provider_id=spec.provider_id,
            outputs={"path": "/tmp/server-under-test"},
            provenance={"sha256": "abc"},
        )

    def rollback(self, context, spec):
        return None

    def cleanup(self, context, spec, artifact):
        return None


class _ResourceProvider:
    def check_support(self, context, spec, resolved_config):
        return SupportDecision.allow()

    def provision(self, context, spec, resolved_config):
        return ProvisionedResource(
            resource_id=spec.resource_id,
            provider_id=spec.provider_id,
            outputs={"endpoint": "127.0.0.1:9000"},
            provenance={"implementation": "test"},
        )

    def rollback(self, context, spec):
        return None

    def cleanup(self, context, spec, resource):
        return None


class _EnvironmentContext:
    def __init__(self):
        self._dependency_services = MappingProxyType(
            {
                "resources.store": MappingProxyType(
                    {
                        "client": object(),
                    }
                )
            }
        )
        self.events = []

    @property
    def run_id(self):
        return "run-123"

    @property
    def seed(self):
        return 42

    @property
    def deadline(self):
        return 100.0

    @property
    def artifact_dir(self):
        return "/tmp/run-123/providers/candidate"

    @property
    def dependency_services(self):
        return self._dependency_services

    def publish_event(self, name, details):
        self.events.append((name, details))


def _artifact_registration(
    provider_id="tests.artifact",
    *,
    outputs=("path", "sha256"),
    services=(),
    api_version=ENVIRONMENT_EXTENSION_API_VERSION,
    factory=_ArtifactProvider,
    validator=None,
    resolved_config_validator=None,
):
    return ArtifactProviderRegistration(
        provider_id=provider_id,
        factory=factory,
        outputs=outputs,
        services=services,
        api_version=api_version,
        validator=validator,
        resolved_config_validator=resolved_config_validator,
    )


def _resource_registration(
    provider_id="tests.resource",
    *,
    outputs=("endpoint", "path"),
    services=(),
    api_version=ENVIRONMENT_EXTENSION_API_VERSION,
    factory=_ResourceProvider,
    validator=None,
    resolved_config_validator=None,
):
    return ResourceProviderRegistration(
        provider_id=provider_id,
        factory=factory,
        outputs=outputs,
        services=services,
        api_version=api_version,
        validator=validator,
        resolved_config_validator=resolved_config_validator,
    )


def _registries():
    artifacts = ArtifactProviderRegistry()
    resources = ResourceProviderRegistry()
    artifacts.register(_artifact_registration())
    resources.register(_resource_registration())
    return artifacts, resources


def test_specs_take_deep_immutable_snapshots_and_export_json():
    artifact_config = {
        "build": {"flags": ["instrumented"]},
        "builder": _ref("resources.builder.endpoint"),
    }
    resource_config = {"capacity": [1024]}
    policy_config = {"thresholds": {"errors": 0}}
    artifact = ArtifactSpec(
        artifact_id="candidate",
        provider_id="tests.artifact",
        config=artifact_config,
        depends_on=["resources.builder"],
    )
    resource = ResourceSpec(
        resource_id="support-service",
        provider_id="tests.resource",
        config=resource_config,
        depends_on=["artifacts.candidate"],
    )
    policy = PolicySpec(
        policy_id="acceptance",
        policy_type="tests.threshold",
        config=policy_config,
    )

    artifact_config["build"]["flags"].append("debug")
    resource_config["capacity"].append(2048)
    policy_config["thresholds"]["errors"] = 1

    assert artifact.config["build"]["flags"] == ("instrumented",)
    assert artifact.config["builder"] == _ref("resources.builder.endpoint")
    assert resource.config["capacity"] == (1024,)
    assert policy.config["thresholds"]["errors"] == 0
    assert artifact.depends_on == ("resources.builder",)
    assert json.loads(artifact.to_json()) == artifact.to_data()
    assert artifact.to_data()["config"]["builder"] == {ENVIRONMENT_REFERENCE_DATA_KEY: "resources.builder.endpoint"}
    assert json.loads(resource.to_json()) == resource.to_data()
    assert json.loads(policy.to_json()) == policy.to_data()
    assert ArtifactSpec.from_data(artifact.to_data()) == artifact
    assert ArtifactSpec.from_json(artifact.to_json()) == artifact
    assert ResourceSpec.from_data(resource.to_data()) == resource
    assert ResourceSpec.from_json(resource.to_json()) == resource
    assert PolicySpec.from_data(policy.to_data()) == policy
    assert PolicySpec.from_json(policy.to_json()) == policy
    with pytest.raises(TypeError):
        artifact.config["other"] = True
    with pytest.raises(FrozenInstanceError):
        resource.resource_id = "changed"


def test_spec_deserialization_rejects_malformed_reserved_reference_markers():
    data = ArtifactSpec(
        artifact_id="candidate",
        provider_id="tests.artifact",
    ).to_data()
    data["config"] = {
        "builder": {
            ENVIRONMENT_REFERENCE_DATA_KEY: "resources.builder.endpoint",
            "fallback": "localhost",
        }
    }

    with pytest.raises(
        InvalidEnvironmentReferenceError,
        match=r"serialized reference marker.*only the '\$valkey_fuzzer_ref' key",
    ):
        ArtifactSpec.from_data(data)


@pytest.mark.parametrize(
    ("factory", "error"),
    (
        (
            lambda: ArtifactSpec(
                artifact_id="has.dot",
                provider_id="tests.artifact",
            ),
            "artifact_id",
        ),
        (
            lambda: ResourceSpec(
                resource_id="store",
                provider_id="unqualified",
            ),
            "namespaced",
        ),
        (
            lambda: PolicySpec(
                policy_id="acceptance",
                policy_type="tests.policy",
                config={"bad": object()},
            ),
            "JSON-safe",
        ),
        (
            lambda: ArtifactSpec(
                artifact_id="candidate",
                provider_id="tests.artifact",
                depends_on={"resources.store"},
            ),
            "ordered sequence",
        ),
        (
            lambda: ResourceSpec(
                resource_id="store",
                provider_id="tests.resource",
                depends_on=("resources.store",),
            ),
            "depend on itself",
        ),
        (
            lambda: ResourceSpec(
                resource_id="store",
                provider_id="tests.resource",
                depends_on=("artifacts.candidate", "artifacts.candidate"),
            ),
            "duplicate dependency",
        ),
    ),
)
def test_specs_reject_ambiguous_or_unsafe_values(factory, error):
    with pytest.raises((TypeError, ValueError), match=error):
        factory()


@pytest.mark.parametrize(
    "provider_id",
    (
        ".",
        ":",
        "/",
        ".provider",
        "provider.",
        "namespace..provider",
        "namespace/provider/",
        "namespace:\nprovider",
    ),
)
def test_provider_and_policy_type_ids_require_nonempty_safe_namespaces(provider_id):
    with pytest.raises(ValueError, match="namespaced identifier"):
        ArtifactSpec("candidate", provider_id)
    with pytest.raises(ValueError, match="namespaced identifier"):
        PolicySpec("acceptance", provider_id)


def test_support_decision_has_explicit_supported_and_unsupported_semantics():
    accepted = SupportDecision.allow(details={"platform": ["linux"]})
    rejected = SupportDecision.deny("unsupported platform", details={"platform": "legacy"})

    assert accepted.supported
    assert accepted.reason is None
    assert accepted.details["platform"] == ("linux",)
    assert not rejected.supported
    assert rejected.reason == "unsupported platform"
    assert json.loads(rejected.to_json()) == rejected.to_data()
    with pytest.raises(ValueError, match="cannot include"):
        SupportDecision(supported=True, reason="maybe")
    with pytest.raises(ValueError, match="reason"):
        SupportDecision(supported=False)
    with pytest.raises(TypeError, match="boolean"):
        SupportDecision(supported=1)


def test_provider_results_separate_replayable_data_from_runtime_state():
    handle = object()
    client = object()
    artifact = ResolvedArtifact(
        artifact_id="candidate",
        provider_id="tests.artifact",
        outputs={
            "path": "/opt/server-under-test",
            "capabilities": ["instrumented"],
            "token": "artifact-secret",
        },
        provenance={"sha256": "abc", "build": {"commit": "def"}},
        services={"launcher": client},
        handle=handle,
    )
    resource = ProvisionedResource(
        resource_id="store",
        provider_id="tests.resource",
        outputs={
            "endpoint": "http://127.0.0.1:9000",
            "password": "resource-secret",
        },
        provenance={"image_digest": "sha256:123"},
        services={"client": client},
        handle=handle,
    )

    assert artifact.outputs["capabilities"] == ("instrumented",)
    assert artifact.provenance["build"]["commit"] == "def"
    assert isinstance(artifact.services, MappingProxyType)
    assert artifact.services["launcher"] is client
    assert artifact.handle is handle
    assert "services" not in artifact.to_data()
    assert "handle" not in artifact.to_data()
    assert "services" not in resource.to_data()
    assert "handle" not in resource.to_data()
    assert artifact.to_data()["output_names"] == ["capabilities", "path", "token"]
    assert resource.to_data()["output_names"] == ["endpoint", "password"]
    assert "artifact-secret" not in artifact.to_json()
    assert "resource-secret" not in resource.to_json()
    assert "artifact-secret" not in repr(artifact)
    assert "resource-secret" not in repr(resource)
    assert "object at" not in repr(artifact)
    assert json.loads(artifact.to_json()) == artifact.to_data()
    assert json.loads(resource.to_json()) == resource.to_data()
    nested = {"artifact": artifact, "resource": resource}
    for serialized in (to_plain_data(nested), canonical_snapshot(nested)):
        assert serialized == {
            "artifact": artifact.to_data(),
            "resource": resource.to_data(),
        }

    same_public_record = ResolvedArtifact(
        artifact_id="candidate",
        provider_id="tests.artifact",
        outputs={
            "path": "/different/runtime/path",
            "capabilities": ["different"],
            "token": "different-secret",
        },
        provenance={"sha256": "abc", "build": {"commit": "def"}},
        services={"other": object()},
        handle=object(),
    )
    assert same_public_record == artifact


@pytest.mark.parametrize(
    ("factory", "value", "field"),
    (
        (
            ArtifactSpec.from_data,
            {
                "artifact_id": "candidate",
                "provider_id": "tests.artifact",
                "unexpected": True,
            },
            "artifact spec",
        ),
        (
            ResourceSpec.from_data,
            {"resource_id": "service"},
            "resource spec",
        ),
        (
            PolicySpec.from_data,
            {
                "policy_id": "acceptance",
                "policy_type": "tests.threshold",
                1: "invalid",
            },
            "policy spec",
        ),
    ),
)
def test_serialized_environment_specs_reject_incomplete_or_unknown_fields(factory, value, field):
    with pytest.raises((TypeError, ValueError), match=field):
        factory(value)


def test_environment_model_json_rejects_duplicate_keys_and_non_mapping_roots():
    with pytest.raises(ValueError, match="duplicate object key"):
        ArtifactSpec.from_json('{"artifact_id":"candidate","artifact_id":"other","provider_id":"tests.artifact"}')
    with pytest.raises(TypeError, match="decode to a mapping"):
        ResourceSpec.from_json("[]")


@pytest.mark.parametrize(
    "factory",
    (
        lambda: ResolvedArtifact(
            artifact_id="candidate",
            provider_id="tests.artifact",
            outputs={"has.dot": "bad"},
            provenance={},
        ),
        lambda: ProvisionedResource(
            resource_id="store",
            provider_id="tests.resource",
            outputs={"endpoint": object()},
            provenance={},
        ),
        lambda: ResolvedArtifact(
            artifact_id="candidate",
            provider_id="tests.artifact",
            outputs={},
            provenance={"bad": float("nan")},
        ),
        lambda: ProvisionedResource(
            resource_id="store",
            provider_id="tests.resource",
            outputs={},
            provenance={},
            services=[],
        ),
        lambda: ResolvedArtifact(
            artifact_id="candidate",
            provider_id="tests.artifact",
            outputs={},
            provenance={},
            services={"bad.service": object()},
        ),
    ),
)
def test_provider_results_reject_invalid_export_and_service_shapes(factory):
    with pytest.raises((TypeError, ValueError)):
        factory()


def test_environment_provider_context_exposes_only_runner_independent_runtime_services():
    context = _EnvironmentContext()

    assert isinstance(context, EnvironmentProviderContext)
    assert context.run_id == "run-123"
    assert context.seed == 42
    assert context.deadline == 100.0
    assert context.artifact_dir == "/tmp/run-123/providers/candidate"
    assert tuple(context.dependency_services) == ("resources.store",)
    assert tuple(context.dependency_services["resources.store"]) == ("client",)
    context.publish_event("provider.resolved", {"attempt": 1})
    assert context.events == [("provider.resolved", {"attempt": 1})]
    with pytest.raises(TypeError):
        context.dependency_services["resources.other"] = {}
    with pytest.raises(TypeError):
        context.dependency_services["resources.store"]["other"] = object()


def test_provider_protocols_are_runtime_checkable_and_kept_separate():
    assert isinstance(_ArtifactProvider(), ArtifactProvider)
    assert not isinstance(_ArtifactProvider(), ResourceProvider)
    assert isinstance(_ResourceProvider(), ResourceProvider)
    assert not isinstance(_ResourceProvider(), ArtifactProvider)


def test_registrations_declare_outputs_and_services_strictly():
    with pytest.raises(ValueError, match="duplicate declaration"):
        _artifact_registration(outputs=("path", "path"))
    with pytest.raises(ValueError, match="declared output"):
        _resource_registration(outputs=("bad.output",))
    with pytest.raises(TypeError, match="ordered sequence"):
        _artifact_registration(outputs={"path"})
    with pytest.raises(ValueError, match="duplicate declaration"):
        _resource_registration(services=("client", "client"))
    with pytest.raises(ValueError, match="declared service"):
        _artifact_registration(services=("bad.service",))
    with pytest.raises(TypeError, match="ordered sequence"):
        _resource_registration(services={"client"})
    with pytest.raises(TypeError, match="factory"):
        _resource_registration(factory=object())
    with pytest.raises(TypeError, match="incompatible signature"):
        _artifact_registration(factory=lambda required: required)
    with pytest.raises(TypeError, match=r"validator.*incompatible signature"):
        _resource_registration(validator=lambda spec, extra: None)
    with pytest.raises(TypeError, match=r"resolved config validator.*incompatible signature"):
        _artifact_registration(resolved_config_validator=lambda spec: None)


def test_registrations_reject_deferred_factories_and_validators():
    async def async_factory():
        return _ArtifactProvider()

    async def async_validator(_spec):
        return None

    def generator_validator(_spec):
        yield None

    class AsyncCallableValidator:
        async def __call__(self, _spec):
            return None

    with pytest.raises(TypeError, match="factory must be synchronous"):
        _artifact_registration(factory=async_factory)
    with pytest.raises(TypeError, match="validator must be synchronous"):
        _artifact_registration(validator=async_validator)
    with pytest.raises(TypeError, match="validator must execute eagerly"):
        _resource_registration(validator=generator_validator)
    with pytest.raises(TypeError, match="validator must be synchronous"):
        _resource_registration(validator=AsyncCallableValidator())


def test_resolved_config_validator_is_concrete_and_separate_from_static_validation():
    observed = []

    def validate_static(spec):
        observed.append(("static", spec.config["endpoint"]))

    def validate_resolved(spec, resolved_config):
        observed.append(("resolved", resolved_config["endpoint"]))
        assert spec.config["endpoint"] == _ref("resources.store.endpoint")

    registration = _artifact_registration(
        validator=validate_static,
        resolved_config_validator=validate_resolved,
    )
    spec = ArtifactSpec(
        "candidate",
        "tests.artifact",
        config={"endpoint": _ref("resources.store.endpoint")},
        depends_on=("resources.store",),
    )

    registration.validator(spec)
    registration.validate_resolved_config(
        spec,
        MappingProxyType({"endpoint": "127.0.0.1:9000"}),
    )

    assert observed == [
        ("static", _ref("resources.store.endpoint")),
        ("resolved", "127.0.0.1:9000"),
    ]
    with pytest.raises(ProviderContractError, match="unresolved environment references"):
        registration.validate_resolved_config(spec, spec.config)


@pytest.mark.parametrize(
    ("validator", "message"),
    (
        (lambda _spec, _config: (item for item in ()), "returned a generator"),
        (lambda _spec, _config: "unexpected", "must return None"),
    ),
)
def test_resolved_config_validator_must_execute_eagerly_and_return_none(validator, message):
    registration = _resource_registration(resolved_config_validator=validator)
    spec = ResourceSpec("store", "tests.resource")

    with pytest.raises(ProviderContractError, match=message):
        registration.validate_resolved_config(spec, MappingProxyType({}))


def test_artifact_registration_validates_exact_result_identity_outputs_and_services():
    registration = _artifact_registration(
        outputs=("path", "sha256"),
        services=("inspector", "launcher"),
    )
    spec = ArtifactSpec("candidate", "tests.artifact")
    inspector = object()
    launcher = object()
    valid = ResolvedArtifact(
        artifact_id="candidate",
        provider_id="tests.artifact",
        outputs={"sha256": "abc", "path": "/opt/server-under-test"},
        provenance={"commit": "def"},
        services={"launcher": launcher, "inspector": inspector},
    )

    registration.validate_result(spec, valid)
    assert registration.declared_services == ("inspector", "launcher")
    with pytest.raises(ProviderResultError, match="missing declared outputs"):
        registration.validate_result(
            spec,
            ResolvedArtifact(
                artifact_id="candidate",
                provider_id="tests.artifact",
                outputs={"path": "/opt/server-under-test"},
                provenance={},
                services={"inspector": inspector, "launcher": launcher},
            ),
        )
    with pytest.raises(ProviderResultError, match="undeclared outputs"):
        registration.validate_result(
            spec,
            ResolvedArtifact(
                artifact_id="candidate",
                provider_id="tests.artifact",
                outputs={"path": "/opt/server-under-test", "sha256": "abc", "version": "9"},
                provenance={},
                services={"inspector": inspector, "launcher": launcher},
            ),
        )
    with pytest.raises(ProviderResultError, match="missing declared services"):
        registration.validate_result(
            spec,
            ResolvedArtifact(
                artifact_id="candidate",
                provider_id="tests.artifact",
                outputs={"path": "/opt/server-under-test", "sha256": "abc"},
                provenance={},
                services={"launcher": launcher},
            ),
        )
    with pytest.raises(ProviderResultError, match="undeclared services"):
        registration.validate_result(
            spec,
            ResolvedArtifact(
                artifact_id="candidate",
                provider_id="tests.artifact",
                outputs={"path": "/opt/server-under-test", "sha256": "abc"},
                provenance={},
                services={
                    "inspector": inspector,
                    "launcher": launcher,
                    "metrics": object(),
                },
            ),
        )
    with pytest.raises(ProviderResultError, match="artifact_id"):
        registration.validate_result(
            spec,
            ResolvedArtifact(
                artifact_id="baseline",
                provider_id="tests.artifact",
                outputs={"path": "/opt/server-under-test", "sha256": "abc"},
                provenance={},
                services={"inspector": inspector, "launcher": launcher},
            ),
        )
    with pytest.raises(ProviderResultError, match="expected ResolvedArtifact"):
        registration.validate_result(spec, object())


def test_resource_registration_validates_exact_result_identity_outputs_and_services():
    registration = _resource_registration(
        outputs=("endpoint",),
        services=("client",),
    )
    spec = ResourceSpec("store", "tests.resource")
    client = object()
    valid = ProvisionedResource(
        resource_id="store",
        provider_id="tests.resource",
        outputs={"endpoint": "http://localhost"},
        provenance={"implementation": "test"},
        services={"client": client},
    )

    registration.validate_result(spec, valid)
    assert registration.declared_services == ("client",)
    with pytest.raises(ProviderResultError, match="resource_id"):
        registration.validate_result(
            spec,
            ProvisionedResource(
                resource_id="other",
                provider_id="tests.resource",
                outputs={"endpoint": "http://localhost"},
                provenance={},
                services={"client": client},
            ),
        )
    with pytest.raises(ProviderResultError, match="result names provider"):
        registration.validate_result(
            spec,
            ProvisionedResource(
                resource_id="store",
                provider_id="other.resource",
                outputs={"endpoint": "http://localhost"},
                provenance={},
                services={"client": client},
            ),
        )
    with pytest.raises(ProviderResultError, match="missing declared services"):
        registration.validate_result(
            spec,
            ProvisionedResource(
                resource_id="store",
                provider_id="tests.resource",
                outputs={"endpoint": "http://localhost"},
                provenance={},
            ),
        )


def test_provider_registries_are_typed_deterministic_and_invoke_validators():
    validated = []
    artifact_registry = ArtifactProviderRegistry()
    resource_registry = ResourceProviderRegistry()
    artifact_registry.register(
        _artifact_registration(
            "zeta.artifact",
            validator=lambda spec: validated.append(("artifact", spec.artifact_id)),
        )
    )
    artifact_registry.register(_artifact_registration("alpha.artifact"))
    resource_registry.register(
        _resource_registration(
            validator=lambda spec: validated.append(("resource", spec.resource_id)),
        )
    )

    artifact_spec = ArtifactSpec("candidate", "zeta.artifact")
    resource_spec = ResourceSpec("store", "tests.resource")
    artifact_registry.validate_spec(artifact_spec)
    resource_registry.validate_spec(resource_spec)

    assert artifact_registry.registered_ids == ("alpha.artifact", "zeta.artifact")
    assert tuple(artifact_registry.snapshot()) == ("alpha.artifact", "zeta.artifact")
    assert isinstance(artifact_registry.create("alpha.artifact"), ArtifactProvider)
    assert isinstance(resource_registry.create("tests.resource"), ResourceProvider)
    assert validated == [("artifact", "candidate"), ("resource", "store")]

    removed = artifact_registry.unregister("alpha.artifact")
    assert removed.provider_id == "alpha.artifact"
    with pytest.raises(UnknownProviderError):
        artifact_registry.resolve("alpha.artifact")
    with pytest.raises(TypeError, match="ArtifactSpec"):
        artifact_registry.validate_result(object(), object())
    with pytest.raises(TypeError, match="ResourceSpec"):
        resource_registry.validate_result(object(), object())


def test_provider_registry_batch_registration_is_atomic():
    registry = ArtifactProviderRegistry()
    existing = _artifact_registration("existing.artifact")
    registry.register(existing)

    with pytest.raises(DuplicateProviderRegistrationError):
        registry.register_many(
            (
                _artifact_registration("new.artifact"),
                _artifact_registration("existing.artifact"),
            )
        )

    assert registry.registered_ids == ("existing.artifact",)


def test_provider_registry_enforces_internal_api_and_registration_type():
    artifact_registry = ArtifactProviderRegistry(api_version="internal-current")
    resource_registry = ResourceProviderRegistry()

    with pytest.raises(IncompatibleProviderRegistrationError, match="API"):
        artifact_registry.register(
            _artifact_registration(api_version="internal-old"),
        )
    with pytest.raises(TypeError, match="ArtifactProviderRegistration"):
        artifact_registry.register(_resource_registration())
    with pytest.raises(TypeError, match="ResourceProviderRegistration"):
        resource_registry.register(_artifact_registration())


def test_environment_providers_share_the_internal_extension_contract():
    assert ENVIRONMENT_EXTENSION_API_VERSION == EXTENSION_API_VERSION
    assert _artifact_registration().api_version == EXTENSION_API_VERSION
    assert _resource_registration().api_version == EXTENSION_API_VERSION
    assert ArtifactProviderRegistry().api_version == EXTENSION_API_VERSION
    assert ResourceProviderRegistry().api_version == EXTENSION_API_VERSION


def test_registry_factory_contract_is_checked():
    registry = ArtifactProviderRegistry()
    registry.register(
        _artifact_registration(
            "broken.artifact",
            factory=lambda: object(),
        )
    )

    with pytest.raises(ProviderContractError, match="check_support method must be callable"):
        registry.create("broken.artifact")


def test_registry_rejects_non_callable_and_incompatible_provider_methods():
    class NonCallableCleanup:
        check_support = _ArtifactProvider.check_support
        resolve = _ArtifactProvider.resolve
        rollback = _ArtifactProvider.rollback
        cleanup = 1

    class IncompatibleCleanup(_ArtifactProvider):
        def cleanup(self, context, spec, artifact, required):
            return None

    registry = ArtifactProviderRegistry()
    registry.register(
        _artifact_registration(
            "noncallable.artifact",
            factory=NonCallableCleanup,
        )
    )
    registry.register(
        _artifact_registration(
            "bad-signature.artifact",
            factory=IncompatibleCleanup,
        )
    )

    with pytest.raises(ProviderContractError, match="cleanup method must be callable"):
        registry.create("noncallable.artifact")
    with pytest.raises(ProviderContractError, match="cleanup method has an incompatible signature"):
        registry.create("bad-signature.artifact")


def test_registry_requires_context_on_every_provider_lifecycle_method():
    class LegacyArtifactProvider:
        def check_support(self, spec, resolved_config):
            return SupportDecision.allow()

        def resolve(self, spec, resolved_config):
            return ResolvedArtifact(
                artifact_id=spec.artifact_id,
                provider_id=spec.provider_id,
                outputs={},
                provenance={},
            )

        def rollback(self, spec):
            return None

        def cleanup(self, spec, artifact):
            return None

    registry = ArtifactProviderRegistry()
    registry.register(
        _artifact_registration(
            "legacy.artifact",
            outputs=(),
            factory=LegacyArtifactProvider,
        )
    )

    with pytest.raises(ProviderContractError, match="check_support method has an incompatible signature"):
        registry.create("legacy.artifact")


def test_registry_rejects_async_provider_lifecycle_methods():
    class AsyncCleanup(_ArtifactProvider):
        async def cleanup(self, context, spec, artifact):
            return None

    registry = ArtifactProviderRegistry()
    registry.register(
        _artifact_registration(
            "async-cleanup.artifact",
            factory=AsyncCleanup,
        )
    )

    with pytest.raises(ProviderContractError, match="cleanup method must be synchronous"):
        registry.create("async-cleanup.artifact")


def test_registry_wraps_provider_factory_failures():
    def fail_factory():
        raise TypeError("factory internals failed")

    registry = ResourceProviderRegistry()
    registry.register(
        _resource_registration(
            "broken.resource",
            factory=fail_factory,
        )
    )

    with pytest.raises(ProviderContractError, match="factory failed with TypeError") as error:
        registry.create("broken.resource")
    assert isinstance(error.value.__cause__, TypeError)


def test_registry_concurrent_duplicate_registration_never_partially_mutates():
    registry = ResourceProviderRegistry()
    barrier = threading.Barrier(3)
    results = []

    def register():
        barrier.wait()
        try:
            registry.register(_resource_registration("shared.resource"))
            results.append("registered")
        except DuplicateProviderRegistrationError:
            results.append("duplicate")

    first = threading.Thread(target=register)
    second = threading.Thread(target=register)
    first.start()
    second.start()
    barrier.wait()
    first.join(timeout=5)
    second.join(timeout=5)

    assert not first.is_alive()
    assert not second.is_alive()
    assert sorted(results) == ["duplicate", "registered"]
    assert registry.registered_ids == ("shared.resource",)


def test_registry_snapshot_restore_is_atomic_and_validated():
    registry = ArtifactProviderRegistry()
    registry.register(_artifact_registration("first.artifact"))
    snapshot = registry.snapshot()
    registry.register(_artifact_registration("second.artifact"))

    registry.restore_snapshot(snapshot)

    assert registry.registered_ids == ("first.artifact",)
    with pytest.raises(ValueError, match="keys must match"):
        registry.restore_snapshot({"wrong.artifact": snapshot["first.artifact"]})
    assert registry.registered_ids == ("first.artifact",)


def test_reference_and_dependency_parsers_accept_only_exact_grammars():
    reference = parse_environment_reference("artifacts.candidate.executable")
    dependency = parse_environment_dependency("resources.support-service")

    assert reference == EnvironmentReference(
        EnvironmentKind.ARTIFACT,
        "candidate",
        "executable",
    )
    assert reference.path == "artifacts.candidate.executable"
    assert reference.dependency == EnvironmentDependency(
        EnvironmentKind.ARTIFACT,
        "candidate",
    )
    assert dependency.path == "resources.support-service"

    invalid_references = (
        "artifact.candidate.path",
        "artifacts.candidate",
        "artifacts.candidate.path.extra",
        " artifacts.candidate.path",
        "resources.store.bad.output",
        1,
    )
    for invalid in invalid_references:
        with pytest.raises(InvalidEnvironmentReferenceError):
            parse_environment_reference(invalid)

    invalid_dependencies = (
        "resource.store",
        "resources.store.path",
        "resources.has.dot",
        "resources.store ",
        None,
    )
    for invalid in invalid_dependencies:
        with pytest.raises(InvalidEnvironmentReferenceError):
            parse_environment_dependency(invalid)


def test_reference_collection_is_recursive_stable_and_distinct():
    value = {
        "binary": _ref("artifacts.candidate.path"),
        "nested": [
            {"endpoint": _ref("resources.store.endpoint")},
            _ref("artifacts.candidate.path"),
        ],
        "literal": {"$ref": "resources.literal.endpoint"},
    }

    assert collect_environment_references(value) == (
        EnvironmentReference(EnvironmentKind.ARTIFACT, "candidate", "path"),
        EnvironmentReference(EnvironmentKind.RESOURCE, "store", "endpoint"),
    )


def test_serialized_reference_markers_are_reserved_and_strict():
    with pytest.raises(
        InvalidEnvironmentReferenceError,
        match=r"only the '\$valkey_fuzzer_ref' key",
    ):
        environment_references_from_data(
            {
                ENVIRONMENT_REFERENCE_DATA_KEY: "artifacts.candidate.path",
                "default": "/usr/bin/server-under-test",
            }
        )
    with pytest.raises(InvalidEnvironmentReferenceError, match="must be a string"):
        environment_references_from_data({ENVIRONMENT_REFERENCE_DATA_KEY: 1})

    literal = {"$ref": "artifacts.candidate.path"}
    assert environment_references_from_data(literal) == literal
    assert collect_environment_references(literal) == ()


def test_reference_resolution_uses_only_exported_outputs_and_returns_immutable_data():
    artifact = ResolvedArtifact(
        artifact_id="candidate",
        provider_id="tests.artifact",
        outputs={"path": "/opt/server-under-test", "capabilities": {"modes": ["instrumented"]}},
        provenance={"sha256": "abc"},
        services={"launcher": object()},
    )
    resource = ProvisionedResource(
        resource_id="store",
        provider_id="tests.resource",
        outputs={"endpoint": "http://localhost:9000"},
        provenance={"image": "test"},
        services={"client": object()},
    )
    resolved = resolve_environment_references(
        {
            "executable": _ref("artifacts.candidate.path"),
            "capabilities": _ref("artifacts.candidate.capabilities"),
            "backends": [_ref("resources.store.endpoint")],
        },
        artifacts={"candidate": artifact},
        resources={"store": resource},
    )

    assert resolved["executable"] == "/opt/server-under-test"
    assert resolved["capabilities"]["modes"] == ("instrumented",)
    assert resolved["backends"] == ("http://localhost:9000",)
    assert "launcher" not in resolved
    assert "client" not in resolved
    with pytest.raises(TypeError):
        resolved["new"] = "value"


def test_reference_resolution_has_specific_missing_target_and_output_errors():
    artifact = ResolvedArtifact(
        artifact_id="candidate",
        provider_id="tests.artifact",
        outputs={"path": "/opt/server-under-test"},
        provenance={},
    )

    with pytest.raises(UnresolvedEnvironmentReferenceError, match="has not been provisioned"):
        resolve_environment_references(
            _ref("resources.store.endpoint"),
            artifacts={"candidate": artifact},
            resources={},
        )
    with pytest.raises(UnresolvedEnvironmentReferenceError, match="does not contain"):
        resolve_environment_references(
            _ref("artifacts.candidate.sha256"),
            artifacts={"candidate": artifact},
            resources={},
        )
    with pytest.raises(ValueError, match="mapping key"):
        resolve_environment_references(
            _ref("artifacts.candidate.path"),
            artifacts={
                "candidate": ResolvedArtifact(
                    artifact_id="baseline",
                    provider_id="tests.artifact",
                    outputs={"path": "/opt/server-under-test"},
                    provenance={},
                )
            },
            resources={},
        )


def test_combined_environment_order_infers_cross_kind_reference_dependencies():
    artifact_registry, resource_registry = _registries()
    candidate = ArtifactSpec(
        "candidate",
        "tests.artifact",
        config={"source": _ref("resources.store.path")},
    )
    baseline = ArtifactSpec("baseline", "tests.artifact")
    builder = ResourceSpec(
        "builder",
        "tests.resource",
        depends_on=("artifacts.baseline",),
    )
    store = ResourceSpec(
        "store",
        "tests.resource",
        config={"seed": _ref("resources.builder.endpoint")},
    )

    ordered = stable_environment_order(
        (candidate, baseline),
        (builder, store),
        artifact_registry=artifact_registry,
        resource_registry=resource_registry,
    )

    assert tuple(spec.dependency.path for spec in ordered) == (
        "artifacts.baseline",
        "resources.builder",
        "resources.store",
        "artifacts.candidate",
    )


def test_stable_environment_order_preserves_declaration_order_among_ready_nodes():
    artifact_registry, resource_registry = _registries()
    late = ArtifactSpec(
        "late",
        "tests.artifact",
        depends_on=("artifacts.first",),
    )
    independent = ArtifactSpec("independent", "tests.artifact")
    first = ArtifactSpec("first", "tests.artifact")

    ordered = stable_environment_order(
        (late, independent, first),
        (),
        artifact_registry=artifact_registry,
        resource_registry=resource_registry,
    )

    assert tuple(spec.artifact_id for spec in ordered) == (
        "independent",
        "first",
        "late",
    )


def test_dependency_validation_rejects_unknown_targets():
    artifact_registry, resource_registry = _registries()
    artifact = ArtifactSpec(
        "candidate",
        "tests.artifact",
        depends_on=("resources.missing",),
    )

    with pytest.raises(UnknownEnvironmentTargetError, match=r"resources\.missing") as error:
        validate_environment_dependencies(
            (artifact,),
            (),
            artifact_registry=artifact_registry,
            resource_registry=resource_registry,
        )

    assert error.value.source == "artifacts.candidate"


def test_dependency_validation_rejects_refs_to_undeclared_outputs():
    artifact_registry, resource_registry = _registries()
    resource = ResourceSpec(
        "store",
        "tests.resource",
        config={"binary": _ref("artifacts.candidate.version")},
    )
    artifact = ArtifactSpec("candidate", "tests.artifact")

    with pytest.raises(UndeclaredEnvironmentOutputError, match="version") as error:
        validate_environment_dependencies(
            (artifact,),
            (resource,),
            artifact_registry=artifact_registry,
            resource_registry=resource_registry,
        )

    assert error.value.reference.path == "artifacts.candidate.version"
    assert error.value.declared_outputs == ("path", "sha256")


def test_dependency_validation_rejects_combined_cycles_with_cycle_path():
    artifact_registry, resource_registry = _registries()
    artifact = ArtifactSpec(
        "candidate",
        "tests.artifact",
        config={"endpoint": _ref("resources.store.endpoint")},
    )
    resource = ResourceSpec(
        "store",
        "tests.resource",
        config={"binary": _ref("artifacts.candidate.path")},
    )

    with pytest.raises(EnvironmentDependencyCycleError) as error:
        validate_environment_dependencies(
            (artifact,),
            (resource,),
            artifact_registry=artifact_registry,
            resource_registry=resource_registry,
        )

    assert tuple(item.path for item in error.value.cycle) == (
        "artifacts.candidate",
        "resources.store",
        "artifacts.candidate",
    )


def test_config_reference_to_self_is_reported_as_a_cycle():
    artifact_registry, resource_registry = _registries()
    artifact = ArtifactSpec(
        "candidate",
        "tests.artifact",
        config={"binary": _ref("artifacts.candidate.path")},
    )

    with pytest.raises(EnvironmentDependencyCycleError, match=r"artifacts\.candidate"):
        validate_environment_dependencies(
            (artifact,),
            (),
            artifact_registry=artifact_registry,
            resource_registry=resource_registry,
        )


def test_policy_references_are_validated_without_becoming_acquisition_nodes():
    artifact_registry, resource_registry = _registries()
    artifact = ArtifactSpec("candidate", "tests.artifact")
    resource = ResourceSpec("store", "tests.resource")
    policy = PolicySpec(
        "acceptance",
        "tests.threshold",
        config={
            "artifact_digest": _ref("artifacts.candidate.sha256"),
            "service_endpoint": _ref("resources.store.endpoint"),
        },
    )

    ordered = stable_environment_order(
        (artifact,),
        (resource,),
        policies=(policy,),
        artifact_registry=artifact_registry,
        resource_registry=resource_registry,
    )

    assert ordered == (artifact, resource)

    invalid_policy = PolicySpec(
        "budget",
        "tests.budget",
        config={"service_endpoint": _ref("resources.missing.endpoint")},
    )
    with pytest.raises(UnknownEnvironmentTargetError) as error:
        validate_environment_dependencies(
            (artifact,),
            (resource,),
            policies=(invalid_policy,),
            artifact_registry=artifact_registry,
            resource_registry=resource_registry,
        )
    assert error.value.source == "policies.budget"


def test_dependency_validation_invokes_provider_validators_before_acquisition():
    validated = []
    artifact_registry = ArtifactProviderRegistry()
    resource_registry = ResourceProviderRegistry()
    artifact_registry.register(
        _artifact_registration(
            validator=lambda spec: validated.append(spec.artifact_id),
        )
    )
    resource_registry.register(_resource_registration())
    artifact = ArtifactSpec("candidate", "tests.artifact")

    validate_environment_dependencies(
        (artifact,),
        (),
        artifact_registry=artifact_registry,
        resource_registry=resource_registry,
    )

    assert validated == ["candidate"]


def test_dependency_validation_detects_and_rolls_back_reentrant_registry_mutation():
    artifact_registry = ArtifactProviderRegistry()
    resource_registry = ResourceProviderRegistry()

    def mutate_registry(_spec):
        artifact_registry.unregister("tests.artifact")

    registration = _artifact_registration(validator=mutate_registry)
    artifact_registry.register(registration)

    with pytest.raises(EnvironmentRegistryChangedError, match="artifact provider registry"):
        validate_environment_dependencies(
            (ArtifactSpec("candidate", "tests.artifact"),),
            (),
            artifact_registry=artifact_registry,
            resource_registry=resource_registry,
        )

    assert artifact_registry.snapshot() == {"tests.artifact": registration}


def test_dependency_validation_restores_mutation_without_masking_cancellation():
    artifact_registry = ArtifactProviderRegistry()
    resource_registry = ResourceProviderRegistry()

    def cancel_after_mutation(_spec):
        artifact_registry.unregister("tests.artifact")
        raise KeyboardInterrupt

    registration = _artifact_registration(validator=cancel_after_mutation)
    artifact_registry.register(registration)

    with pytest.raises(KeyboardInterrupt):
        validate_environment_dependencies(
            (ArtifactSpec("candidate", "tests.artifact"),),
            (),
            artifact_registry=artifact_registry,
            resource_registry=resource_registry,
        )

    assert artifact_registry.snapshot() == {"tests.artifact": registration}


@pytest.mark.parametrize(
    ("validator", "message"),
    (
        (lambda _spec: (item for item in ()), "returned a generator"),
        (lambda _spec: "unexpected", "must return None"),
    ),
)
def test_dependency_validation_rejects_deferred_or_nonempty_validator_results(validator, message):
    artifact_registry = ArtifactProviderRegistry()
    resource_registry = ResourceProviderRegistry()
    artifact_registry.register(_artifact_registration(validator=validator))

    with pytest.raises(ProviderContractError, match=message):
        validate_environment_dependencies(
            (ArtifactSpec("candidate", "tests.artifact"),),
            (),
            artifact_registry=artifact_registry,
            resource_registry=resource_registry,
        )


def test_dependency_validation_rejects_disguised_async_validator_result():
    async def deferred():
        return None

    def validator(_spec):
        return deferred()

    artifact_registry = ArtifactProviderRegistry()
    resource_registry = ResourceProviderRegistry()
    artifact_registry.register(_artifact_registration(validator=validator))

    with pytest.raises(ProviderContractError, match="returned an awaitable"):
        validate_environment_dependencies(
            (ArtifactSpec("candidate", "tests.artifact"),),
            (),
            artifact_registry=artifact_registry,
            resource_registry=resource_registry,
        )


def test_dependency_validation_holds_registry_locks_against_concurrent_mutation():
    validation_entered = threading.Event()
    release_validation = threading.Event()
    mutation_completed = threading.Event()
    validation_errors = []

    artifact_registry = ArtifactProviderRegistry()
    resource_registry = ResourceProviderRegistry()

    def blocking_validator(_spec):
        validation_entered.set()
        assert release_validation.wait(timeout=5)

    artifact_registry.register(_artifact_registration(validator=blocking_validator))

    def validate():
        try:
            validate_environment_dependencies(
                (ArtifactSpec("candidate", "tests.artifact"),),
                (),
                artifact_registry=artifact_registry,
                resource_registry=resource_registry,
            )
        except BaseException as exc:
            validation_errors.append(exc)

    def mutate():
        artifact_registry.register(_artifact_registration("later.artifact"))
        mutation_completed.set()

    validation_thread = threading.Thread(target=validate)
    validation_thread.start()
    assert validation_entered.wait(timeout=5)

    mutation_thread = threading.Thread(target=mutate)
    mutation_thread.start()
    assert not mutation_completed.wait(timeout=0.05)
    release_validation.set()

    validation_thread.join(timeout=5)
    mutation_thread.join(timeout=5)
    assert not validation_thread.is_alive()
    assert not mutation_thread.is_alive()
    assert validation_errors == []
    assert mutation_completed.is_set()
    assert artifact_registry.registered_ids == ("later.artifact", "tests.artifact")


def test_dependency_validation_rejects_unknown_providers_and_duplicate_ids():
    artifact_registry, resource_registry = _registries()

    with pytest.raises(UnknownProviderError, match=r"missing\.artifact"):
        validate_environment_dependencies(
            (ArtifactSpec("candidate", "missing.artifact"),),
            (),
            artifact_registry=artifact_registry,
            resource_registry=resource_registry,
        )
    with pytest.raises(ValueError, match="duplicate artifact_id"):
        validate_environment_dependencies(
            (
                ArtifactSpec("candidate", "tests.artifact"),
                ArtifactSpec("candidate", "tests.artifact"),
            ),
            (),
            artifact_registry=artifact_registry,
            resource_registry=resource_registry,
        )
    with pytest.raises(ValueError, match="duplicate policy_id"):
        validate_environment_dependencies(
            (),
            (),
            policies=(
                PolicySpec("acceptance", "tests.threshold"),
                PolicySpec("acceptance", "tests.threshold"),
            ),
            artifact_registry=artifact_registry,
            resource_registry=resource_registry,
        )


def test_dependency_validation_rejects_unordered_spec_collections():
    artifact_registry, resource_registry = _registries()

    with pytest.raises(TypeError, match="ordered sequence"):
        validate_environment_dependencies(
            set(),
            (),
            artifact_registry=artifact_registry,
            resource_registry=resource_registry,
        )
    with pytest.raises(TypeError, match="ordered sequence"):
        validate_environment_dependencies(
            (),
            (item for item in ()),
            artifact_registry=artifact_registry,
            resource_registry=resource_registry,
        )
