"""Compilation boundary tests for declarative pre-topology environments."""

import copy
import threading
from dataclasses import dataclass, field
from typing import Any

import pytest

from src.environment import (
    ENVIRONMENT_REFERENCE_DATA_KEY,
    ArtifactProviderRegistration,
    ArtifactProviderRegistry,
    ArtifactSpec,
    EnvironmentDependencyCycleError,
    EnvironmentReference,
    EnvironmentRegistryChangedError,
    PolicySpec,
    ProvisionedResource,
    ResolvedArtifact,
    ResourceProviderRegistration,
    ResourceProviderRegistry,
    SupportDecision,
    UndeclaredEnvironmentOutputError,
    UnknownEnvironmentTargetError,
    UnknownProviderError,
    parse_environment_reference,
)
from src.extensibility import (
    ComponentKind,
    ComponentRegistration,
    ComponentResult,
    ComponentSpec,
    ComponentStatus,
    OutcomeStatus,
    Registry,
    RunMode,
    RunPlan,
    to_plain_data,
)
from src.feature_packs import (
    FeaturePackCatalog,
    FeaturePackCompiler,
    FeaturePackDocumentError,
    ReferenceInventory,
    ScenarioCompileError,
    UnresolvedReferenceError,
    load_feature_pack_yaml,
    validate_run_plan_environment,
)
from src.feature_runner import FeatureRunner, ProvisionedTopology
from src.feature_runtime import FeatureRuntime, FeatureRuntimeError


def _ref(path: str) -> EnvironmentReference:
    return parse_environment_reference(path)


def _pack():
    return load_feature_pack_yaml(
        """
pack_id: tests.environment
version: "1"
mode: topology_smoke
supported_topologies: [standalone]
input_schema:
  type: object
  properties:
    base_port: {type: integer}
    channel: {type: string}
  required: [base_port, channel]
  additionalProperties: false
components:
  - kind: action
    component_id: tests.action
    instance_id: exercise
    config:
      binary: {$ref: artifacts.candidate.path}
      endpoint: {$ref: resources.store.endpoint}
startup_overlays:
  - overlay_id: runtime-paths
    selector:
      labels:
        zone: {$ref: resources.store.zone}
    server_config:
      dir: {$ref: resources.store.directory}
metadata:
  owner: tests
"""
    )


def _scenario() -> dict[str, Any]:
    return {
        "scenario_id": "environment-plan",
        "seed": 101,
        "feature": {
            "use": "tests.environment",
            "version": "1",
            "with": {
                "base_port": 7400,
                "channel": "nightly",
            },
        },
        "artifacts": {
            "candidate": {
                "use": "tests.artifact",
                "with": {"channel": {"$ref": "inputs.channel"}},
            }
        },
        "resources": {
            "store": {
                "use": "tests.resource",
                "depends_on": ["artifacts.candidate"],
                "with": {
                    "binary": {"$ref": "artifacts.candidate.path"},
                },
            }
        },
        "policies": {
            "retention": {
                "use": "tests.retention",
                "with": {
                    "endpoint": {"$ref": "resources.store.endpoint"},
                },
            }
        },
        "topology": {
            "type": "standalone",
            "base_port": {"$ref": "inputs.base_port"},
            "valkey_binary": {"$ref": "artifacts.candidate.path"},
        },
    }


def _component_registries() -> dict[ComponentKind, Registry[Any]]:
    registry = Registry[Any](ComponentKind.ACTION)
    registry.register(
        ComponentRegistration[Any](
            component_id="tests.action",
            kind=ComponentKind.ACTION,
            api_version="v1",
            factory=lambda: object(),
        )
    )
    return {ComponentKind.ACTION: registry}


def _provider_registries() -> tuple[ArtifactProviderRegistry, ResourceProviderRegistry]:
    class ArtifactProvider:
        def check_support(self, context, spec, resolved_config):
            return SupportDecision.allow()

        def resolve(self, context, spec, resolved_config):
            return ResolvedArtifact(
                artifact_id=spec.artifact_id,
                provider_id=spec.provider_id,
                outputs={"path": "/test/valkey", "digest": "sha256:test"},
                provenance={"kind": "test"},
            )

        def rollback(self, context, spec):
            return None

        def cleanup(self, context, spec, artifact):
            return None

    class ResourceProvider:
        def check_support(self, context, spec, resolved_config):
            return SupportDecision.allow()

        def provision(self, context, spec, resolved_config):
            return ProvisionedResource(
                resource_id=spec.resource_id,
                provider_id=spec.provider_id,
                outputs={
                    "endpoint": "test://store",
                    "zone": "test-zone",
                    "directory": "/test/store",
                },
                provenance={"kind": "test"},
            )

        def rollback(self, context, spec):
            return None

        def cleanup(self, context, spec, resource):
            return None

    artifacts = ArtifactProviderRegistry()
    artifacts.register(
        ArtifactProviderRegistration(
            provider_id="tests.artifact",
            factory=ArtifactProvider,
            outputs=("path", "digest"),
        )
    )
    resources = ResourceProviderRegistry()
    resources.register(
        ResourceProviderRegistration(
            provider_id="tests.resource",
            factory=ResourceProvider,
            outputs=("endpoint", "zone", "directory"),
        )
    )
    return artifacts, resources


def _consumer_preflight_plan() -> RunPlan:
    return RunPlan(
        scenario_id="consumer-preflight",
        pack_id="tests.consumer-preflight",
        pack_version="1",
        seed=1,
        topology="standalone",
        components=(),
        mode=RunMode.TOPOLOGY_SMOKE,
        artifacts=(ArtifactSpec("candidate", "tests.artifact"),),
        topology_document={
            "type": "standalone",
            "valkey_binary": _ref("artifacts.candidate.path"),
        },
    )


def _compiler() -> FeaturePackCompiler:
    catalog = FeaturePackCatalog()
    catalog.register(_pack())
    artifact_registry, resource_registry = _provider_registries()
    return FeaturePackCompiler(
        catalog,
        _component_registries(),
        reference_inventory=ReferenceInventory(labels={"zone": None}),
        artifact_registry=artifact_registry,
        resource_registry=resource_registry,
    )


def test_compiler_preserves_environment_references_and_serializes_declarations():
    plan = _compiler().compile(_scenario())

    assert plan.components[0].config == {
        "binary": _ref("artifacts.candidate.path"),
        "endpoint": _ref("resources.store.endpoint"),
    }
    assert plan.startup_overlays[0].selector.labels["zone"] == _ref("resources.store.zone")
    assert plan.startup_overlays[0].server_config["dir"] == _ref("resources.store.directory")
    assert plan.metadata == {"owner": "tests"}
    assert plan.topology_document["base_port"] == 7400
    assert plan.topology_document["valkey_binary"] == _ref("artifacts.candidate.path")

    assert plan.artifacts[0].artifact_id == "candidate"
    assert plan.artifacts[0].provider_id == "tests.artifact"
    assert plan.artifacts[0].config == {"channel": "nightly"}
    assert plan.resources[0].resource_id == "store"
    assert plan.resources[0].depends_on == ("artifacts.candidate",)
    assert plan.resources[0].config["binary"] == _ref("artifacts.candidate.path")
    assert plan.policies[0].policy_id == "retention"
    assert plan.policies[0].policy_type == "tests.retention"
    assert plan.policies[0].config["endpoint"] == _ref("resources.store.endpoint")

    assert RunPlan.from_json(plan.to_json()) == plan
    serialized = plan.to_data()
    assert serialized["artifacts"][0]["artifact_id"] == "candidate"
    assert serialized["resources"][0]["provider_id"] == "tests.resource"
    assert serialized["policies"][0]["policy_type"] == "tests.retention"
    assert serialized["components"][0]["config"]["binary"] == {
        ENVIRONMENT_REFERENCE_DATA_KEY: "artifacts.candidate.path"
    }
    assert serialized["topology_document"]["valkey_binary"] == {
        ENVIRONMENT_REFERENCE_DATA_KEY: "artifacts.candidate.path"
    }

    malformed = copy.deepcopy(serialized)
    malformed["artifacts"][0]["unexpected"] = True
    with pytest.raises(ValueError, match="unknown fields"):
        RunPlan.from_data(malformed)

    malformed_reference = copy.deepcopy(serialized)
    malformed_reference["components"][0]["config"]["binary"]["fallback"] = "/opt/valkey"
    with pytest.raises(
        ValueError,
        match=r"serialized reference marker.*only the '\$valkey_fuzzer_ref' key",
    ):
        RunPlan.from_data(malformed_reference)

    non_string_reference = copy.deepcopy(serialized)
    non_string_reference["components"][0]["config"]["binary"] = {ENVIRONMENT_REFERENCE_DATA_KEY: 1}
    with pytest.raises(ValueError, match=r"\$ref value must be a string"):
        RunPlan.from_data(non_string_reference)

    unsafe_metadata = copy.deepcopy(serialized)
    unsafe_metadata["metadata"] = {"executable": {ENVIRONMENT_REFERENCE_DATA_KEY: "artifacts.candidate.path"}}
    with pytest.raises(ValueError, match=r"metadata.*must not contain environment references"):
        RunPlan.from_data(unsafe_metadata)

    literal_metadata = copy.deepcopy(serialized)
    literal_metadata["metadata"] = {"executable": {"$ref": "artifacts.candidate.path"}}
    assert RunPlan.from_data(literal_metadata).metadata == literal_metadata["metadata"]


def test_feature_inputs_cannot_defer_schema_validation_to_environment_outputs():
    scenario = _scenario()
    scenario["feature"]["with"]["channel"] = {"$ref": "artifacts.candidate.path"}

    with pytest.raises(
        ScenarioCompileError,
        match=r"feature\.with\.channel cannot contain environment references",
    ):
        _compiler().compile(scenario)


def test_opt_in_environment_reference_inputs_bind_reusable_pack_templates_and_roundtrip():
    pack = load_feature_pack_yaml(
        """
pack_id: tests.environment-inputs
version: "1"
mode: topology_smoke
supported_topologies: [standalone]
input_schema:
  type: object
  properties:
    binary:
      type: environment_reference
      reference_kinds: [artifacts]
    placement:
      type: environment_reference
  required: [binary, placement]
  additionalProperties: false
components:
  - kind: action
    component_id: tests.action
    instance_id: exercise
    config:
      binary: {$ref: inputs.binary}
      placement:
        resolved: {$ref: inputs.placement}
startup_overlays:
  - overlay_id: environment-inputs
    selector:
      labels:
        placement:
          resolved: {$ref: inputs.placement}
    server_config:
      executable: {$ref: inputs.binary}
"""
    )
    catalog = FeaturePackCatalog()
    catalog.register(pack)
    artifact_registry, resource_registry = _provider_registries()
    compiler = FeaturePackCompiler(
        catalog,
        _component_registries(),
        reference_inventory=ReferenceInventory(
            labels={"placement": ({"resolved": "static"},)},
        ),
        artifact_registry=artifact_registry,
        resource_registry=resource_registry,
    )
    scenario = _scenario()
    scenario["feature"] = {
        "use": "tests.environment-inputs",
        "with": {
            "binary": {"$ref": "artifacts.candidate.path"},
            "placement": {"$ref": "resources.store.zone"},
        },
    }
    scenario["artifacts"]["candidate"]["with"]["channel"] = "nightly"
    scenario["topology"]["base_port"] = 7400

    plan = compiler.compile(scenario)

    binary = _ref("artifacts.candidate.path")
    placement = _ref("resources.store.zone")
    assert plan.inputs == {"binary": binary, "placement": placement}
    assert plan.components[0].config == {
        "binary": binary,
        "placement": {"resolved": placement},
    }
    assert plan.startup_overlays[0].selector.labels == {
        "placement": {"resolved": placement},
    }
    assert plan.startup_overlays[0].server_config == {"executable": binary}
    assert plan.to_data()["inputs"] == {
        "binary": {ENVIRONMENT_REFERENCE_DATA_KEY: binary.path},
        "placement": {ENVIRONMENT_REFERENCE_DATA_KEY: placement.path},
    }
    assert RunPlan.from_json(plan.to_json()) == plan


def test_environment_reference_input_kind_constraint_is_enforced():
    pack = load_feature_pack_yaml(
        """
pack_id: tests.artifact-input
version: "1"
mode: topology_smoke
supported_topologies: [standalone]
input_schema:
  type: object
  properties:
    binary:
      type: environment_reference
      reference_kinds: [artifacts]
  required: [binary]
  additionalProperties: false
components:
  - kind: action
    component_id: tests.action
    instance_id: exercise
    config:
      binary: {$ref: inputs.binary}
"""
    )
    catalog = FeaturePackCatalog()
    catalog.register(pack)
    artifact_registry, resource_registry = _provider_registries()
    compiler = FeaturePackCompiler(
        catalog,
        _component_registries(),
        artifact_registry=artifact_registry,
        resource_registry=resource_registry,
    )
    scenario = _scenario()
    scenario["feature"] = {
        "use": "tests.artifact-input",
        "with": {"binary": {"$ref": "resources.store.endpoint"}},
    }
    scenario["artifacts"]["candidate"]["with"]["channel"] = "nightly"
    scenario["topology"]["base_port"] = 7400

    with pytest.raises(
        ScenarioCompileError,
        match=r"input for input 'binary' references kind 'resources'; allowed kinds are 'artifacts'",
    ):
        compiler.compile(scenario)

    scenario["feature"]["with"]["binary"] = {
        "$valkey_fuzzer_literal": {"$ref": "artifacts.candidate.path"},
    }
    with pytest.raises(
        ScenarioCompileError,
        match=r"input for input 'binary' must be an exact environment reference",
    ):
        compiler.compile(scenario)


def test_feature_input_literal_escape_is_opaque_and_preserves_schema_validation():
    pack = load_feature_pack_yaml(
        """
pack_id: tests.literal-input
version: "1"
mode: topology_smoke
supported_topologies: [standalone]
input_schema:
  type: object
  properties:
    payload: {type: object}
  required: [payload]
  additionalProperties: false
components:
  - kind: action
    component_id: tests.action
    instance_id: exercise
    config:
      payload: {$ref: inputs.payload}
"""
    )
    catalog = FeaturePackCatalog()
    catalog.register(pack)
    compiler = FeaturePackCompiler(catalog, _component_registries())
    scenario = {
        "scenario_id": "literal-input",
        "seed": 1,
        "feature": {
            "use": "tests.literal-input",
            "with": {"payload": {"$valkey_fuzzer_literal": {"$ref": "artifacts.candidate.path"}}},
        },
        "topology": {"type": "standalone"},
    }

    plan = compiler.compile(scenario)

    literal = {"$ref": "artifacts.candidate.path"}
    assert plan.inputs["payload"] == literal
    assert plan.components[0].config["payload"] == literal
    assert plan.to_data()["components"][0]["config"]["payload"] == literal
    assert RunPlan.from_json(plan.to_json()) == plan

    unescaped = copy.deepcopy(scenario)
    unescaped["feature"]["with"]["payload"] = literal
    with pytest.raises(ScenarioCompileError, match="cannot contain environment references"):
        compiler.compile(unescaped)

    malformed = copy.deepcopy(scenario)
    malformed["feature"]["with"]["payload"] = {
        "$valkey_fuzzer_literal": literal,
        "extra": True,
    }
    with pytest.raises(
        ScenarioCompileError,
        match=r"literal marker must contain only the '\$valkey_fuzzer_literal' key",
    ):
        compiler.compile(malformed)

    wrong_type = _scenario()
    wrong_type["feature"]["with"]["channel"] = {"$valkey_fuzzer_literal": literal}
    with pytest.raises(
        ScenarioCompileError,
        match=r"input 'channel' must have type 'string'",
    ):
        _compiler().compile(wrong_type)


def test_ordinary_input_rules_reject_nested_environment_references():
    pack = load_feature_pack_yaml(
        """
pack_id: tests.ordinary-object-input
version: "1"
mode: topology_smoke
supported_topologies: [standalone]
input_schema:
  type: object
  properties:
    payload: {type: object}
  required: [payload]
  additionalProperties: false
components:
  - kind: action
    component_id: tests.action
    instance_id: exercise
    config:
      payload: {$ref: inputs.payload}
"""
    )
    catalog = FeaturePackCatalog()
    catalog.register(pack)
    compiler = FeaturePackCompiler(catalog, _component_registries())

    with pytest.raises(
        ScenarioCompileError,
        match=r"feature\.with\.payload cannot contain environment references",
    ):
        compiler.compile(
            {
                "scenario_id": "nested-binding",
                "seed": 1,
                "feature": {
                    "use": "tests.ordinary-object-input",
                    "with": {
                        "payload": {
                            "nested": {"$ref": "artifacts.candidate.path"},
                        }
                    },
                },
                "topology": {"type": "standalone"},
            }
        )


def test_literal_escape_is_opaque_on_every_template_capable_surface():
    pack = load_feature_pack_yaml(
        """
pack_id: tests.literal-surfaces
version: "1"
mode: topology_smoke
supported_topologies: [standalone]
input_schema:
  type: object
  properties:
    payload: {type: object}
  required: [payload]
  additionalProperties: false
components:
  - kind: action
    component_id: tests.action
    instance_id: exercise
    config:
      from_input: {$ref: inputs.payload}
      direct:
        $valkey_fuzzer_literal:
          $ref: "#/components/schemas/Request"
startup_overlays:
  - overlay_id: literal-surfaces
    selector:
      labels:
        schema:
          $valkey_fuzzer_literal:
            $ref: "#/components/schemas/Request"
    server_config:
      schema:
        $valkey_fuzzer_literal:
          $ref: "#/components/schemas/Request"
"""
    )
    catalog = FeaturePackCatalog()
    catalog.register(pack)
    artifact_registry, resource_registry = _provider_registries()
    literal = {"$ref": "#/components/schemas/Request"}
    compiler = FeaturePackCompiler(
        catalog,
        _component_registries(),
        reference_inventory=ReferenceInventory(labels={"schema": (literal,)}),
        artifact_registry=artifact_registry,
        resource_registry=resource_registry,
    )
    scenario = _scenario()
    scenario["feature"] = {
        "use": "tests.literal-surfaces",
        "with": {"payload": {"$valkey_fuzzer_literal": literal}},
    }
    scenario["artifacts"]["candidate"]["with"]["channel"] = "nightly"
    scenario["topology"]["base_port"] = 7400
    scenario["topology"]["schema"] = {"$valkey_fuzzer_literal": literal}
    scenario["artifacts"]["candidate"]["with"]["schema"] = {
        "$valkey_fuzzer_literal": literal,
    }
    scenario["resources"]["store"]["with"]["schema"] = {
        "$valkey_fuzzer_literal": literal,
    }
    scenario["policies"]["retention"]["with"]["schema"] = {
        "$valkey_fuzzer_literal": literal,
    }

    plan = compiler.compile(scenario)

    assert plan.inputs["payload"] == literal
    assert plan.topology_document["schema"] == literal
    assert plan.artifacts[0].config["schema"] == literal
    assert plan.resources[0].config["schema"] == literal
    assert plan.policies[0].config["schema"] == literal
    assert plan.components[0].config["from_input"] == literal
    assert plan.components[0].config["direct"] == literal
    assert plan.startup_overlays[0].selector.labels["schema"] == literal
    assert plan.startup_overlays[0].server_config["schema"] == literal
    assert RunPlan.from_json(plan.to_json()) == plan

    scenario["topology"]["schema"] = {
        "$valkey_fuzzer_literal": literal,
        "description": "ambiguous",
    }
    with pytest.raises(
        UnresolvedReferenceError,
        match=r"topology\.schema literal marker must contain only the '\$valkey_fuzzer_literal' key",
    ):
        compiler.compile(scenario)


def test_feature_pack_metadata_cannot_persist_environment_references():
    with pytest.raises(
        FeaturePackDocumentError,
        match=r"metadata.*must not contain environment references",
    ):
        load_feature_pack_yaml(
            """
pack_id: tests.unsafe-metadata
version: "1"
mode: topology_smoke
supported_topologies: [standalone]
components: []
metadata:
  executable: {$ref: artifacts.candidate.path}
"""
        )


def test_plain_data_skips_nested_runtime_only_dataclass_fields():
    @dataclass(frozen=True)
    class RuntimeBinding:
        name: str
        secret: Any = field(metadata={"serialize": False})

    @dataclass(frozen=True)
    class Record:
        binding: RuntimeBinding
        bindings: tuple[RuntimeBinding, ...]

    value = Record(
        binding=RuntimeBinding("primary", object()),
        bindings=(RuntimeBinding("secondary", object()),),
    )

    assert to_plain_data(value) == {
        "binding": {"name": "primary"},
        "bindings": [{"name": "secondary"}],
    }


def test_scenario_environment_declarations_are_strict_mappings():
    compiler = _compiler()

    scenario = _scenario()
    scenario["artifacts"]["candidate"]["unexpected"] = True
    with pytest.raises(FeaturePackDocumentError, match=r"artifacts\.candidate.*unknown fields"):
        compiler.compile(scenario)

    scenario = _scenario()
    scenario["resources"] = []
    with pytest.raises(FeaturePackDocumentError, match="resources must be a mapping"):
        compiler.compile(scenario)

    scenario = _scenario()
    scenario["policies"]["retention"]["depends_on"] = ["resources.store"]
    with pytest.raises(FeaturePackDocumentError, match=r"policies\.retention.*unknown fields"):
        compiler.compile(scenario)


def test_compiler_rejects_unknown_environment_provider_before_execution():
    scenario = _scenario()
    scenario["artifacts"]["candidate"]["use"] = "tests.missing"

    with pytest.raises(UnknownProviderError, match=r"tests\.missing"):
        _compiler().compile(scenario)


def test_compiler_validates_every_environment_output_consumer():
    scenario = _scenario()
    scenario["topology"]["valkey_binary"] = {"$ref": "artifacts.candidate.undeclared"}

    with pytest.raises(
        UndeclaredEnvironmentOutputError,
        match=r"topology.*undeclared",
    ):
        _compiler().compile(scenario)


def test_whole_plan_preflight_restores_mutation_during_consumer_validation(monkeypatch):
    artifact_registry, resource_registry = _provider_registries()
    registration = artifact_registry.resolve("tests.artifact")
    replacement = ArtifactProviderRegistration(
        provider_id="tests.artifact",
        factory=lambda: object(),
        outputs=("path", "digest"),
    )
    original_resolve = artifact_registry.resolve
    resolve_calls = 0

    def mutate_on_consumer(provider_id):
        nonlocal resolve_calls
        resolved = original_resolve(provider_id)
        resolve_calls += 1
        if resolve_calls == 2:
            artifact_registry.unregister(provider_id)
            artifact_registry.register(replacement)
        return resolved

    monkeypatch.setattr(artifact_registry, "resolve", mutate_on_consumer)

    with pytest.raises(EnvironmentRegistryChangedError, match="artifact provider registry"):
        validate_run_plan_environment(
            _consumer_preflight_plan(),
            artifact_registry=artifact_registry,
            resource_registry=resource_registry,
        )

    assert artifact_registry.snapshot()["tests.artifact"] is registration


def test_whole_plan_preflight_restores_consumer_mutation_without_masking_cancellation(monkeypatch):
    artifact_registry, resource_registry = _provider_registries()
    registration = artifact_registry.resolve("tests.artifact")
    original_resolve = artifact_registry.resolve
    resolve_calls = 0

    def cancel_on_consumer(provider_id):
        nonlocal resolve_calls
        resolved = original_resolve(provider_id)
        resolve_calls += 1
        if resolve_calls == 2:
            artifact_registry.unregister(provider_id)
            raise KeyboardInterrupt
        return resolved

    monkeypatch.setattr(artifact_registry, "resolve", cancel_on_consumer)

    with pytest.raises(KeyboardInterrupt):
        validate_run_plan_environment(
            _consumer_preflight_plan(),
            artifact_registry=artifact_registry,
            resource_registry=resource_registry,
        )

    assert artifact_registry.snapshot()["tests.artifact"] is registration


def test_whole_plan_preflight_holds_both_registry_locks_through_consumer_validation(
    monkeypatch,
):
    consumer_entered = threading.Event()
    release_consumer = threading.Event()
    mutation_completed = threading.Event()
    validation_errors = []
    artifact_registry, resource_registry = _provider_registries()
    original_resolve = artifact_registry.resolve
    resolve_calls = 0

    def block_on_consumer(provider_id):
        nonlocal resolve_calls
        resolved = original_resolve(provider_id)
        resolve_calls += 1
        if resolve_calls == 2:
            consumer_entered.set()
            assert release_consumer.wait(timeout=5)
        return resolved

    monkeypatch.setattr(artifact_registry, "resolve", block_on_consumer)

    def validate():
        try:
            validate_run_plan_environment(
                _consumer_preflight_plan(),
                artifact_registry=artifact_registry,
                resource_registry=resource_registry,
            )
        except BaseException as exc:
            validation_errors.append(exc)

    def mutate_resource_registry():
        resource_registry.register(
            ResourceProviderRegistration(
                provider_id="tests.later-resource",
                factory=lambda: object(),
            )
        )
        mutation_completed.set()

    validation_thread = threading.Thread(target=validate)
    validation_thread.start()
    assert consumer_entered.wait(timeout=5)

    mutation_thread = threading.Thread(target=mutate_resource_registry)
    mutation_thread.start()
    try:
        assert not mutation_completed.wait(timeout=0.05)
    finally:
        release_consumer.set()

    validation_thread.join(timeout=5)
    mutation_thread.join(timeout=5)
    assert not validation_thread.is_alive()
    assert not mutation_thread.is_alive()
    assert validation_errors == []
    assert mutation_completed.is_set()
    assert resource_registry.registered_ids == (
        "tests.later-resource",
        "tests.resource",
    )


def test_only_typed_selector_label_references_defer_value_comparison():
    catalog = FeaturePackCatalog()
    catalog.register(_pack())
    artifact_registry, resource_registry = _provider_registries()
    compiler = FeaturePackCompiler(
        catalog,
        _component_registries(),
        reference_inventory=ReferenceInventory(labels={"zone": ("a",)}),
        artifact_registry=artifact_registry,
        resource_registry=resource_registry,
    )

    assert compiler.compile(_scenario()).startup_overlays[0].selector.labels["zone"] == _ref("resources.store.zone")


def test_component_validators_defer_only_for_typed_component_bindings():
    validations = []
    registry = Registry[Any](ComponentKind.ACTION)
    registry.register(
        ComponentRegistration[Any](
            component_id="tests.action",
            kind=ComponentKind.ACTION,
            api_version="v1",
            factory=lambda: object(),
            validator=lambda spec, plan: validations.append((spec.instance_id, plan.scenario_id)),
        )
    )
    catalog = FeaturePackCatalog()
    catalog.register(_pack())
    artifact_registry, resource_registry = _provider_registries()
    compiler = FeaturePackCompiler(
        catalog,
        {ComponentKind.ACTION: registry},
        reference_inventory=ReferenceInventory(labels={"zone": None}),
        artifact_registry=artifact_registry,
        resource_registry=resource_registry,
    )

    plan = compiler.compile(_scenario())
    FeatureRunner(
        {ComponentKind.ACTION: registry},
        {"standalone": _TopologyProvider()},
    ).validate(plan)

    assert validations == []

    static_pack = load_feature_pack_yaml(
        """
pack_id: tests.static-component
version: "1"
mode: topology_smoke
supported_topologies: [standalone]
components:
  - kind: action
    component_id: tests.action
    instance_id: exercise
    config: {binary: /opt/valkey}
"""
    )
    static_catalog = FeaturePackCatalog()
    static_catalog.register(static_pack)
    static_plan = FeaturePackCompiler(
        static_catalog,
        {ComponentKind.ACTION: registry},
    ).compile(
        {
            "scenario_id": "static-component",
            "seed": 1,
            "feature": {"use": "tests.static-component"},
            "topology": {"type": "standalone"},
        }
    )

    assert validations == [("exercise", "static-component")]
    FeatureRunner(
        {ComponentKind.ACTION: registry},
        {"standalone": _TopologyProvider()},
    ).validate(static_plan)
    assert validations == [
        ("exercise", "static-component"),
        ("exercise", "static-component"),
    ]


def test_compiler_rejects_environment_references_to_undeclared_instances():
    scenario = _scenario()
    scenario["topology"]["valkey_binary"] = {"$ref": "artifacts.missing.path"}

    with pytest.raises(
        UnknownEnvironmentTargetError,
        match=r"topology.*artifacts\.missing",
    ):
        _compiler().compile(scenario)


def test_compiler_validates_cross_kind_environment_dependency_cycles():
    scenario = _scenario()
    scenario["artifacts"]["candidate"]["depends_on"] = ["resources.store"]

    with pytest.raises(
        EnvironmentDependencyCycleError,
        match=r"artifacts\.candidate.*resources\.store.*artifacts\.candidate",
    ):
        _compiler().compile(scenario)


class _TopologyProvider:
    def __init__(self):
        self.validated = False
        self.provisioned = False

    def validate(self, plan):
        self.validated = True
        return None

    def provision(self, context):
        self.provisioned = True
        return ProvisionedTopology()

    def rollback(self, context):
        return None

    def cleanup(self, context, topology):
        return None


def _runtime() -> tuple[FeatureRuntime, _TopologyProvider]:
    class PassingAction:
        def run(self, context, spec):
            return ComponentResult(
                component_instance_id=spec.instance_id,
                status=ComponentStatus.PASS,
                summary="passed",
            )

    topology_provider = _TopologyProvider()
    runtime = FeatureRuntime(
        discover_entry_points=False,
        topology_providers={"standalone": topology_provider},
        reference_inventory=ReferenceInventory(labels={"zone": None}),
    )
    runtime.catalog.register(_pack())
    runtime.registries[ComponentKind.ACTION].register(
        ComponentRegistration[Any](
            component_id="tests.action",
            kind=ComponentKind.ACTION,
            api_version="v1",
            factory=PassingAction,
        )
    )
    artifact_registry, resource_registry = _provider_registries()
    runtime.artifact_providers.register_many(artifact_registry.snapshot().values())
    runtime.resource_providers.register_many(resource_registry.snapshot().values())
    return runtime, topology_provider


def test_runtime_acquires_registered_environment_providers_and_executes():
    runtime, topology_provider = _runtime()
    plan = runtime.compile(_scenario())

    runtime.validate_plan(plan)
    outcome = runtime.run_plan(plan)

    assert outcome.status is OutcomeStatus.PASS
    assert outcome.plan is plan
    assert outcome.diagnostics["environment_report"]["status"] == "cleaned"
    assert topology_provider.validated is True
    assert topology_provider.provisioned is True


def test_validation_uses_typed_references_instead_of_declarations_as_defer_signal():
    topology_provider = _TopologyProvider()
    plan = RunPlan(
        scenario_id="static-artifact",
        pack_id="tests.static-artifact",
        pack_version="1",
        seed=1,
        topology="standalone",
        components=(),
        mode=RunMode.TOPOLOGY_SMOKE,
        artifacts=(_compiler().compile(_scenario()).artifacts[0],),
    )

    FeatureRunner(
        {},
        {"standalone": topology_provider},
    ).validate(plan)

    assert topology_provider.validated is True
    assert topology_provider.provisioned is False


def test_component_only_environment_reference_does_not_defer_topology_validation():
    class _RejectingTopologyProvider(_TopologyProvider):
        def validate(self, plan):
            super().validate(plan)
            raise ValueError("static topology is invalid")

    topology_provider = _RejectingTopologyProvider()
    plan = RunPlan(
        scenario_id="component-binding-static-topology",
        pack_id="tests.component-binding-static-topology",
        pack_version="1",
        seed=1,
        topology="standalone",
        components=(
            ComponentSpec(
                component_id="tests.action",
                kind=ComponentKind.ACTION,
                instance_id="exercise",
                config={"binary": _ref("artifacts.candidate.path")},
            ),
        ),
        mode=RunMode.TOPOLOGY_SMOKE,
        topology_document={"type": "standalone", "invalid": True},
    )

    with pytest.raises(ValueError, match="static topology is invalid"):
        FeatureRunner(
            _component_registries(),
            {"standalone": topology_provider},
        ).validate(plan)

    assert topology_provider.validated is True
    assert topology_provider.provisioned is False


def test_policy_only_plan_is_bound_as_opaque_context_without_core_dispatch():
    topology_provider = _TopologyProvider()
    plan = RunPlan(
        scenario_id="policy-only",
        pack_id="tests.policy-only",
        pack_version="1",
        seed=1,
        topology="standalone",
        components=(),
        mode=RunMode.TOPOLOGY_SMOKE,
        policies=(
            PolicySpec(
                policy_id="retention",
                policy_type="tests.retention",
                config={"keep_runs": 3},
            ),
        ),
    )

    outcome = FeatureRunner(
        {},
        {"standalone": topology_provider},
    ).run(plan)

    assert outcome.status is OutcomeStatus.PASS
    assert topology_provider.validated is True
    assert topology_provider.provisioned is True


def test_runner_returns_error_for_unresolved_reference_without_declaration():
    topology_provider = _TopologyProvider()
    plan = RunPlan(
        scenario_id="unresolved-policy",
        pack_id="tests.unresolved-policy",
        pack_version="1",
        seed=1,
        topology="standalone",
        components=(),
        mode=RunMode.TOPOLOGY_SMOKE,
        policies=(
            PolicySpec(
                policy_id="retention",
                policy_type="tests.retention",
                config={"target": _ref("resources.missing.endpoint")},
            ),
        ),
    )

    outcome = FeatureRunner(
        {},
        {"standalone": topology_provider},
    ).run(plan)

    assert outcome.status is OutcomeStatus.ERROR
    assert topology_provider.validated is False
    assert topology_provider.provisioned is False


def test_raw_dollar_ref_in_run_plan_data_remains_literal_policy_payload():
    topology_provider = _TopologyProvider()
    raw_reference = {"$ref": "resources.missing.endpoint"}
    plan = RunPlan(
        scenario_id="literal-policy-payload",
        pack_id="tests.literal-policy-payload",
        pack_version="1",
        seed=1,
        topology="standalone",
        components=(),
        mode=RunMode.TOPOLOGY_SMOKE,
        policies=(
            PolicySpec(
                policy_id="retention",
                policy_type="tests.retention",
                config={"target": raw_reference},
            ),
        ),
    )

    restored = RunPlan.from_json(plan.to_json())

    assert restored.policies[0].config["target"] == raw_reference
    outcome = FeatureRunner(
        {},
        {"standalone": topology_provider},
    ).run(restored)

    assert outcome.status is OutcomeStatus.PASS
    assert topology_provider.validated is True
    assert topology_provider.provisioned is True


def test_runtime_pins_environment_provider_registries_with_the_plan():
    runtime, _topology_provider = _runtime()
    plan = runtime.compile(_scenario())
    runtime.artifact_providers.register(
        ArtifactProviderRegistration(
            provider_id="tests.additional",
            factory=lambda: object(),
        )
    )

    with pytest.raises(
        FeatureRuntimeError,
        match="artifact provider registry changed",
    ):
        runtime.validate_plan(plan)
