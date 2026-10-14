"""Focused tests for data-only feature packs and the generic v3 compiler."""

import json
from pathlib import Path
from typing import Any

import pytest

from src.extensibility import (
    ComponentKind,
    ComponentRegistration,
    ComponentResult,
    ComponentStatus,
    DuplicateRegistrationError,
    Evidence,
    EvidenceRequirement,
    OutcomeStatus,
    PluginDiscoveryError,
    PluginManifest,
    Registry,
    RunMode,
    RunOutcome,
    RunPlan,
    discover_plugins,
)
from src.feature_packs import (
    ComponentCompatibilityError,
    DuplicateFeaturePackError,
    FeaturePackCatalog,
    FeaturePackCompiler,
    FeaturePackDocumentError,
    IncompatibleFeaturePackError,
    ReferenceInventory,
    ScenarioCompileError,
    UnresolvedReferenceError,
    UnsupportedResourceError,
    compile_v3_scenario,
    load_feature_pack_directory,
    load_feature_pack_yaml,
    load_v3_scenario_yaml,
    parse_feature_pack_document,
)


def _registration(
    component_id: str,
    kind: ComponentKind,
    *,
    api_version: str = "v1",
    factory=None,
    validator=None,
) -> ComponentRegistration[Any]:
    return ComponentRegistration(
        component_id=component_id,
        kind=kind,
        api_version=api_version,
        factory=factory or (lambda: object()),
        validator=validator,
    )


def _manifest(
    *,
    pack_id: str = "example.feature",
    version: str = "1",
    topologies: tuple[str, ...] = ("standalone",),
    component_id: str = "example.action",
    kind: str = "action",
    config: Any = None,
    input_schema: Any = None,
    startup_overlays: Any = None,
    api_version: str = "v1",
) -> dict[str, Any]:
    return {
        "api_version": api_version,
        "pack_id": pack_id,
        "version": version,
        "mode": "topology_smoke",
        "supported_topologies": list(topologies),
        "input_schema": input_schema or {},
        "components": [
            {
                "kind": kind,
                "component_id": component_id,
                "instance_id": "activate",
                "config": config or {},
            }
        ],
        "startup_overlays": startup_overlays or [],
        "required_events": ["action.completed"],
        "metadata": {"owner": "tests"},
    }


def _scenario(
    *,
    pack_id: str = "example.feature",
    version: str = "1",
    topology: str = "standalone",
    inputs: Any = None,
) -> dict[str, Any]:
    return {
        "api_version": "v3",
        "scenario_id": "example-scenario",
        "seed": 123,
        "feature": {
            "use": pack_id,
            "version": version,
            "with": inputs or {},
        },
        "topology": {
            "type": topology,
            "base_port": 7400,
            "server_config": {"appendonly": "yes"},
            "opaque_future_field": {"values": [1, 2]},
        },
    }


def _compiler_for(
    manifest: dict[str, Any],
    *,
    registered_kind: ComponentKind = ComponentKind.ACTION,
    registered_id: str = "example.action",
    reference_inventory: ReferenceInventory = None,
) -> FeaturePackCompiler:
    catalog = FeaturePackCatalog()
    catalog.register(parse_feature_pack_document(manifest))
    registry = Registry[Any](registered_kind)
    registry.register(_registration(registered_id, registered_kind))
    return FeaturePackCompiler(
        catalog,
        {registered_kind: registry},
        reference_inventory=reference_inventory,
    )


def test_catalog_uses_exact_versions_and_batch_registration_is_atomic():
    version_two = parse_feature_pack_document(_manifest(version="2"))
    alpha = parse_feature_pack_document(_manifest(pack_id="alpha.feature"))
    catalog = FeaturePackCatalog()
    catalog.register_many((version_two, alpha))

    assert catalog.registered_keys == (("alpha.feature", "1"), ("example.feature", "2"))
    assert catalog.resolve("example.feature", "2") is version_two

    duplicate = parse_feature_pack_document(_manifest(version="3"))
    with pytest.raises(DuplicateFeaturePackError):
        catalog.register_many((duplicate, duplicate))
    assert catalog.registered_keys == (("alpha.feature", "1"), ("example.feature", "2"))

    incompatible = parse_feature_pack_document(_manifest(pack_id="future.feature", api_version="v2"))
    with pytest.raises(IncompatibleFeaturePackError):
        catalog.register_many((incompatible,))
    assert ("future.feature", "1") not in catalog


def test_yaml_parser_is_data_only_and_rejects_duplicate_keys():
    pack = load_feature_pack_yaml(
        """
api_version: v1
pack_id: example.yaml
version: "1"
mode: topology_smoke
supported_topologies: [standalone]
components:
  - kind: action
    component_id: example.action
    instance_id: activate
    config:
      command: BGSAVE
"""
    )
    assert pack.pack_id == "example.yaml"
    assert pack.components[0].config["command"] == "BGSAVE"

    with pytest.raises(FeaturePackDocumentError, match="duplicate key"):
        load_feature_pack_yaml(
            """
pack_id: example.first
pack_id: example.second
version: "1"
mode: topology_smoke
supported_topologies: [standalone]
components: []
"""
        )


def test_feature_manifest_preserves_mode_and_required_evidence_through_compile():
    manifest = {
        "pack_id": "example.real-feature",
        "version": "1",
        "mode": "feature",
        "supported_topologies": ["standalone"],
        "components": [
            {
                "kind": "observer",
                "component_id": "example.observer",
                "instance_id": "observe",
            },
            {
                "kind": "oracle",
                "component_id": "example.oracle",
                "instance_id": "validate",
                "depends_on": ["observe"],
            },
        ],
        "required_evidence": [
            {
                "evidence_id": "example.path",
                "source": "observe",
                "min_count": 2,
            }
        ],
    }
    pack = parse_feature_pack_document(manifest)
    catalog = FeaturePackCatalog()
    catalog.register(pack)
    observer_registry = Registry[Any](ComponentKind.OBSERVER)
    observer_registry.register(_registration("example.observer", ComponentKind.OBSERVER))
    oracle_registry = Registry[Any](ComponentKind.ORACLE)
    oracle_registry.register(_registration("example.oracle", ComponentKind.ORACLE))

    plan = compile_v3_scenario(
        _scenario(pack_id="example.real-feature"),
        catalog,
        {
            ComponentKind.OBSERVER: observer_registry,
            ComponentKind.ORACLE: oracle_registry,
        },
    )

    assert plan.mode is RunMode.FEATURE
    assert plan.required_evidence == (
        EvidenceRequirement(
            evidence_id="example.path",
            source="observe",
            min_count=2,
        ),
    )
    assert RunPlan.from_json(plan.to_json()) == plan


def test_compiler_invokes_pure_component_validator_and_checks_signatures():
    calls = []

    def validator(spec, plan):
        calls.append((spec.instance_id, plan.scenario_id))

    catalog = FeaturePackCatalog()
    catalog.register(parse_feature_pack_document(_manifest()))
    registry = Registry[Any](ComponentKind.ACTION)
    registry.register(
        _registration(
            "example.action",
            ComponentKind.ACTION,
            validator=validator,
        )
    )

    FeaturePackCompiler(catalog, {ComponentKind.ACTION: registry}).compile(_scenario())
    assert calls == [("activate", "example-scenario")]

    rejecting_registry = Registry[Any](ComponentKind.ACTION)
    rejecting_registry.register(
        _registration(
            "example.action",
            ComponentKind.ACTION,
            validator=lambda _spec, _plan: (_ for _ in ()).throw(ValueError("invalid component config")),
        )
    )
    with pytest.raises(ComponentCompatibilityError, match="invalid component config"):
        FeaturePackCompiler(
            catalog,
            {ComponentKind.ACTION: rejecting_registry},
        ).compile(_scenario())

    bad_validator_registry = Registry[Any](ComponentKind.ACTION)
    bad_validator_registry.register(
        _registration(
            "example.action",
            ComponentKind.ACTION,
            validator=lambda _spec: None,
        )
    )
    with pytest.raises(ComponentCompatibilityError, match="cannot accept"):
        FeaturePackCompiler(
            catalog,
            {ComponentKind.ACTION: bad_validator_registry},
        ).compile(_scenario())

    bad_factory_registry = Registry[Any](ComponentKind.ACTION)
    bad_factory_registry.register(
        _registration(
            "example.action",
            ComponentKind.ACTION,
            factory=lambda _required: object(),
        )
    )
    with pytest.raises(ComponentCompatibilityError, match="without arguments"):
        FeaturePackCompiler(
            catalog,
            {ComponentKind.ACTION: bad_factory_registry},
        ).compile(_scenario())


def test_v3_yaml_loader_is_safe_and_rejects_duplicate_keys():
    scenario = load_v3_scenario_yaml(
        """
api_version: v3
scenario_id: example-yaml
seed: 123
feature:
  use: example.feature
  version: "1"
  with: {}
topology:
  type: standalone
"""
    )
    assert scenario["feature"]["use"] == "example.feature"

    with pytest.raises(FeaturePackDocumentError, match="duplicate key"):
        load_v3_scenario_yaml(
            """
api_version: v3
scenario_id: first
scenario_id: second
seed: 123
feature:
  use: example.feature
  version: "1"
  with: {}
topology:
  type: standalone
"""
        )
    with pytest.raises(FeaturePackDocumentError):
        load_v3_scenario_yaml("!!python/object/apply:os.system ['echo unsafe']")


def test_directory_load_is_sorted_and_does_not_partially_register(tmp_path: Path):
    (tmp_path / "b.yaml").write_text(
        """
pack_id: example.second
version: "1"
mode: topology_smoke
supported_topologies: [standalone]
components: []
""",
        encoding="utf-8",
    )
    (tmp_path / "a.yml").write_text(
        """
pack_id: example.first
version: "1"
mode: topology_smoke
supported_topologies: [standalone]
components: []
""",
        encoding="utf-8",
    )
    catalog = FeaturePackCatalog()

    packs = load_feature_pack_directory(tmp_path, catalog=catalog)
    assert tuple(pack.pack_id for pack in packs) == ("example.first", "example.second")

    broken_dir = tmp_path / "broken"
    broken_dir.mkdir()
    (broken_dir / "a.yaml").write_text(
        """
pack_id: example.valid
version: "1"
mode: topology_smoke
supported_topologies: [standalone]
components: []
""",
        encoding="utf-8",
    )
    (broken_dir / "b.yaml").write_text("pack_id: [", encoding="utf-8")
    empty_catalog = FeaturePackCatalog()
    with pytest.raises(FeaturePackDocumentError, match=r"b\.yaml"):
        load_feature_pack_directory(broken_dir, catalog=empty_catalog)
    assert len(empty_catalog) == 0


def test_directory_manifest_and_v3_yaml_use_the_generic_compiler(tmp_path: Path):
    (tmp_path / "data-only.yaml").write_text(
        """
api_version: v1
pack_id: example.data-only
version: "1"
mode: topology_smoke
supported_topologies: [standalone]
input_schema:
  type: object
  properties:
    command:
      type: string
      enum: [BGSAVE, SAVE]
      default: BGSAVE
  additionalProperties: false
components:
  - kind: action
    component_id: example.action
    instance_id: activate
    config:
      command:
        $ref: inputs.command
""",
        encoding="utf-8",
    )
    catalog = FeaturePackCatalog()
    load_feature_pack_directory(tmp_path, catalog=catalog)
    registry = Registry[Any](ComponentKind.ACTION)
    registry.register(_registration("example.action", ComponentKind.ACTION))
    compiler = FeaturePackCompiler(catalog, {ComponentKind.ACTION: registry})

    plan = compiler.compile_yaml(
        """
api_version: v3
scenario_id: data-only-yaml
seed: 991
feature:
  use: example.data-only
  version: "1"
  with: {}
topology:
  type: standalone
  provider_extension:
    future_dimension: true
"""
    )

    assert plan.pack_id == "example.data-only"
    assert plan.inputs == {"command": "BGSAVE"}
    assert plan.components[0].config == {"command": "BGSAVE"}
    assert plan.topology_document["provider_extension"] == {"future_dimension": True}


def test_compile_resolves_inputs_and_preserves_complete_topology_and_plan_contract():
    manifest = _manifest(
        config={
            "count": {"$ref": "inputs.count"},
            "payload": {"$ref": "inputs.payload"},
            "enabled": {"$ref": "inputs.enabled"},
        },
        input_schema={
            "type": "object",
            "properties": {
                "count": {"type": "integer", "minimum": 1},
                "payload": {"type": "object"},
                "enabled": {"type": "boolean", "default": True},
                "compression": {"type": "string", "default": "lz4"},
            },
            "required": ["count", "payload"],
            "additionalProperties": False,
        },
        startup_overlays=[
            {
                "overlay_id": "candidate-zone",
                "selector": {
                    "roles": ["primary"],
                    "labels": {"zone": {"$ref": "inputs.payload.zone"}},
                },
                "server_config": {
                    "repl-compression": {"$ref": "inputs.compression"},
                },
                "artifact": "candidate",
            }
        ],
    )
    manifest["components"].insert(
        0,
        {
            "kind": "workload",
            "component_id": "example.workload",
            "instance_id": "seed",
            "config": {"count": {"$ref": "inputs.count"}},
        },
    )
    manifest["components"][1]["depends_on"] = ["seed"]
    catalog = FeaturePackCatalog()
    catalog.register(parse_feature_pack_document(manifest))
    action_registry = Registry[Any](ComponentKind.ACTION)
    action_registry.register(_registration("example.action", ComponentKind.ACTION))
    workload_registry = Registry[Any](ComponentKind.WORKLOAD)
    workload_registry.register(_registration("example.workload", ComponentKind.WORKLOAD))
    compiler = FeaturePackCompiler(
        catalog,
        {
            ComponentKind.ACTION: action_registry,
            ComponentKind.WORKLOAD: workload_registry,
        },
        reference_inventory=ReferenceInventory(
            artifacts=("candidate",),
            labels={"zone": ("a", "b")},
        ),
    )
    supplied = {"count": 10, "payload": {"zone": "a", "items": [1, 2]}}
    scenario = _scenario(inputs=supplied)

    plan = compiler.compile(scenario)
    supplied["payload"]["items"].append(3)
    scenario["topology"]["opaque_future_field"]["values"].append(3)

    assert plan.inputs == {
        "count": 10,
        "payload": {"zone": "a", "items": (1, 2)},
        "enabled": True,
        "compression": "lz4",
    }
    assert tuple(component.instance_id for component in plan.components) == ("seed", "activate")
    assert plan.components[1].depends_on == ("seed",)
    assert plan.components[1].config == {
        "count": 10,
        "payload": {"zone": "a", "items": (1, 2)},
        "enabled": True,
    }
    assert plan.startup_overlays[0].server_config["repl-compression"] == "lz4"
    assert plan.startup_overlays[0].selector.labels["zone"] == "a"
    assert plan.required_events == ("action.completed",)
    assert plan.topology_document["opaque_future_field"]["values"] == (1, 2)
    assert plan.scenario_api_version == "v3"

    original_pack = compiler.catalog.resolve("example.feature", "1")
    assert original_pack.components[1].config["count"] == {"$ref": "inputs.count"}
    assert RunPlan.from_json(plan.to_json()) == plan
    assert json.loads(plan.to_json())["topology_document"]["base_port"] == 7400


@pytest.mark.parametrize(
    ("change", "match"),
    [
        (lambda scenario: scenario.update({"unexpected": True}), "unknown fields"),
        (lambda scenario: scenario.update({"api_version": "v2"}), "incompatible"),
        (lambda scenario: scenario.update({"seed": True}), "seed must be an integer"),
        (lambda scenario: scenario.update({"feature": []}), "feature must be a mapping"),
        (
            lambda scenario: scenario["feature"].update({"unexpected": True}),
            "unknown fields",
        ),
        (lambda scenario: scenario.update({"topology": []}), "topology must be a mapping"),
        (lambda scenario: scenario["feature"].update({"with": []}), "feature.with must be a mapping"),
    ],
)
def test_compile_rejects_malformed_v3_envelopes(change, match):
    compiler = _compiler_for(_manifest())
    scenario = _scenario()
    change(scenario)
    with pytest.raises(ScenarioCompileError, match=match):
        compiler.compile(scenario)


def test_compile_rejects_unsupported_topology_and_missing_or_wrong_kind_components():
    compiler = _compiler_for(_manifest())
    with pytest.raises(ScenarioCompileError, match="not supported"):
        compiler.compile(_scenario(topology="cluster"))

    catalog = FeaturePackCatalog()
    catalog.register(parse_feature_pack_document(_manifest()))
    with pytest.raises(ComponentCompatibilityError, match="not registered"):
        FeaturePackCompiler(catalog, {}).compile(_scenario())

    observer_registry = Registry[Any](ComponentKind.OBSERVER)
    observer_registry.register(_registration("example.action", ComponentKind.OBSERVER))
    with pytest.raises(ComponentCompatibilityError, match="registered as 'observer'"):
        FeaturePackCompiler(
            catalog,
            {ComponentKind.OBSERVER: observer_registry},
        ).compile(_scenario())


@pytest.mark.parametrize(
    ("config", "inputs", "match"),
    [
        ({"value": {"$ref": "inputs.missing"}}, {}, "missing input path"),
        ({"value": {"$ref": "inputs.scalar.child"}}, {"scalar": 1}, "cannot traverse"),
        (
            {"value": {"$ref": "inputs.value", "fallback": 1}},
            {"value": 2},
            "sibling fields",
        ),
        ({"value": {"$ref": "environment.secret"}}, {}, "exact path"),
    ],
)
def test_compile_fails_closed_for_invalid_references(config, inputs, match):
    compiler = _compiler_for(_manifest(config=config))
    with pytest.raises(UnresolvedReferenceError, match=match):
        compiler.compile(_scenario(inputs=inputs))


def test_input_values_are_not_recursively_interpreted_as_templates():
    manifest = _manifest(
        config={"payload": {"$ref": "inputs.payload"}},
        input_schema={
            "type": "object",
            "properties": {"payload": {"type": "object"}},
            "required": ["payload"],
        },
    )
    compiler = _compiler_for(manifest)
    user_value = {"$ref": "inputs.secret"}

    plan = compiler.compile(_scenario(inputs={"payload": user_value}))

    assert plan.components[0].config["payload"] == user_value


@pytest.mark.parametrize(
    ("rule", "match"),
    [
        ({"type": "imaginary"}, "unsupported schema type"),
        ({"type": None}, "unsupported schema type"),
        ({"enum": "not-a-sequence"}, "must be a sequence"),
        ({"minimum": True}, "invalid minimum"),
        ({"maximum": "large"}, "invalid maximum"),
        ({"minimum": 10, "maximum": 1}, "minimum greater than"),
        ({"type": "integer", "default": "one"}, "default.*must have type"),
        ({"enum": [1], "default": True}, "default.*allowed values"),
        ({"minimum": 2, "default": 1}, "default.*below its minimum"),
        ({"maximum": 2, "default": 3}, "default.*above its maximum"),
    ],
)
def test_optional_omitted_input_still_validates_its_complete_schema_rule(rule, match):
    manifest = _manifest(
        input_schema={
            "type": "object",
            "properties": {"optional": rule},
            "additionalProperties": False,
        }
    )
    compiler = _compiler_for(manifest)

    with pytest.raises(ScenarioCompileError, match=match):
        compiler.compile(_scenario())


def test_invalid_default_is_rejected_even_when_a_valid_input_overrides_it():
    compiler = _compiler_for(
        _manifest(
            input_schema={
                "type": "object",
                "properties": {
                    "choice": {
                        "type": "integer",
                        "minimum": 1,
                        "default": "invalid",
                    }
                },
            }
        )
    )

    with pytest.raises(ScenarioCompileError, match=r"default.*must have type"):
        compiler.compile(_scenario(inputs={"choice": 2}))


@pytest.mark.parametrize("property_name", ["ambiguous.name", "ambiguous:name", "_leading"])
def test_input_property_names_use_the_same_safe_segment_grammar_as_references(property_name):
    manifest = _manifest(
        input_schema={
            "type": "object",
            "properties": {property_name: {"type": "string"}},
        }
    )
    compiler = _compiler_for(manifest)

    with pytest.raises(ScenarioCompileError, match="unambiguous path segment"):
        compiler.compile(_scenario())


def test_reference_path_rejects_segments_that_schema_property_names_cannot_use():
    compiler = _compiler_for(
        _manifest(
            config={"value": {"$ref": "inputs.nested:ambiguous"}},
            input_schema={
                "type": "object",
                "properties": {"nested": {"type": "object"}},
            },
        )
    )

    with pytest.raises(UnresolvedReferenceError, match="exact path"):
        compiler.compile(_scenario(inputs={"nested": {"ambiguous": "value"}}))


def test_enum_equality_uses_json_types_instead_of_python_bool_integer_coercion():
    integer_enum = _compiler_for(
        _manifest(
            input_schema={
                "type": "object",
                "properties": {"choice": {"enum": [1]}},
            }
        )
    )
    boolean_enum = _compiler_for(
        _manifest(
            input_schema={
                "type": "object",
                "properties": {"choice": {"enum": [True]}},
            }
        )
    )

    with pytest.raises(ScenarioCompileError, match="allowed values"):
        integer_enum.compile(_scenario(inputs={"choice": True}))
    with pytest.raises(ScenarioCompileError, match="allowed values"):
        boolean_enum.compile(_scenario(inputs={"choice": 1}))


def test_compile_rejects_unresolved_artifacts_and_labels():
    manifest = _manifest(
        startup_overlays=[
            {
                "overlay_id": "candidate",
                "selector": {"labels": {"zone": "a"}},
                "artifact": "candidate",
            }
        ]
    )
    compiler = _compiler_for(manifest)
    with pytest.raises(UnsupportedResourceError, match="artifact"):
        compiler.compile(_scenario())

    label_only = _manifest(
        startup_overlays=[
            {
                "overlay_id": "zone",
                "selector": {"labels": {"zone": "a"}},
                "server_config": {"appendonly": "yes"},
            }
        ]
    )
    with pytest.raises(UnsupportedResourceError, match="node label"):
        _compiler_for(label_only).compile(_scenario())


def test_label_allowed_values_use_json_types_instead_of_python_bool_integer_coercion():
    manifest = _manifest(
        startup_overlays=[
            {
                "overlay_id": "zone",
                "selector": {"labels": {"canary": True}},
                "server_config": {"appendonly": "yes"},
            }
        ]
    )
    compiler = _compiler_for(
        manifest,
        reference_inventory=ReferenceInventory(labels={"canary": (1,)}),
    )

    with pytest.raises(UnsupportedResourceError, match="unsupported value"):
        compiler.compile(_scenario())


class _Distribution:
    def __init__(self, name: str):
        self.name = name


class _EntryPoint:
    def __init__(self, name: str, distribution: str, loaded: Any):
        self.name = name
        self.value = f"tests:{name}"
        self.dist = _Distribution(distribution)
        self._loaded = loaded

    def load(self):
        return self._loaded


class _SelectableEntryPoints:
    def __init__(self, entry_points):
        self.entry_points = entry_points

    def select(self, *, group):
        assert group == "valkey_fuzzer.plugins"
        return self.entry_points


def test_external_entry_point_can_add_component_and_pack_then_compile(monkeypatch):
    pack = parse_feature_pack_document(_manifest(pack_id="external.feature", component_id="external.action"))
    plugin = PluginManifest(
        plugin_id="external.plugin",
        api_version="v1",
        registrations=(_registration("external.action", ComponentKind.ACTION),),
        feature_packs=(pack,),
    )
    monkeypatch.setattr(
        "src.extensibility.importlib_metadata.entry_points",
        lambda: _SelectableEntryPoints([_EntryPoint("external.plugin", "external-distribution", plugin)]),
    )
    catalog = FeaturePackCatalog()
    registry = Registry[Any](ComponentKind.ACTION)

    assert discover_plugins(
        {ComponentKind.ACTION: registry},
        feature_pack_catalog=catalog,
    ) == (plugin,)
    plan = compile_v3_scenario(
        _scenario(pack_id="external.feature"),
        catalog,
        {ComponentKind.ACTION: registry},
    )
    assert plan.pack_id == "external.feature"
    assert plan.components[0].component_id == "external.action"


def test_plugin_pack_collision_is_atomic_across_catalog_and_registry(monkeypatch):
    existing = parse_feature_pack_document(_manifest())
    catalog = FeaturePackCatalog()
    catalog.register(existing)
    registry = Registry[Any](ComponentKind.ACTION)
    plugin = PluginManifest(
        plugin_id="collision.plugin",
        api_version="v1",
        registrations=(_registration("new.action", ComponentKind.ACTION),),
        feature_packs=(parse_feature_pack_document(_manifest()),),
    )
    monkeypatch.setattr(
        "src.extensibility.importlib_metadata.entry_points",
        lambda: _SelectableEntryPoints([_EntryPoint("collision.plugin", "distribution", plugin)]),
    )

    with pytest.raises(DuplicateFeaturePackError):
        discover_plugins(
            {ComponentKind.ACTION: registry},
            feature_pack_catalog=catalog,
        )
    assert len(registry) == 0
    assert catalog.registered_keys == (("example.feature", "1"),)


def test_plugin_commit_failure_rolls_back_every_registry_and_catalog(monkeypatch):
    pack = parse_feature_pack_document(_manifest(pack_id="new.feature", component_id="new.action"))
    plugin = PluginManifest(
        plugin_id="rollback.plugin",
        api_version="v1",
        registrations=(_registration("new.action", ComponentKind.ACTION),),
        feature_packs=(pack,),
    )
    monkeypatch.setattr(
        "src.extensibility.importlib_metadata.entry_points",
        lambda: _SelectableEntryPoints([_EntryPoint("rollback.plugin", "distribution", plugin)]),
    )
    catalog = FeaturePackCatalog()
    registry = Registry[Any](ComponentKind.ACTION)

    def fail_commit(_packs):
        raise RuntimeError("catalog storage failed")

    monkeypatch.setattr(catalog, "register_many", fail_commit)
    with pytest.raises(RuntimeError, match="catalog storage failed"):
        discover_plugins(
            {ComponentKind.ACTION: registry},
            feature_pack_catalog=catalog,
        )

    assert len(registry) == 0
    assert len(catalog) == 0


def test_pack_only_plugin_api_mismatch_and_registry_api_mismatch_are_clean(monkeypatch):
    future_pack = parse_feature_pack_document(_manifest(pack_id="future.feature", api_version="v2"))
    future_plugin = PluginManifest(
        plugin_id="future.plugin",
        api_version="v2",
        registrations=(),
        feature_packs=(future_pack,),
    )
    monkeypatch.setattr(
        "src.extensibility.importlib_metadata.entry_points",
        lambda: _SelectableEntryPoints([_EntryPoint("future.plugin", "distribution", future_plugin)]),
    )
    catalog = FeaturePackCatalog()
    with pytest.raises(PluginDiscoveryError, match="plugin API"):
        discover_plugins({}, feature_pack_catalog=catalog)
    assert len(catalog) == 0

    wrong_api_registry = Registry[Any](ComponentKind.ACTION, api_version="v2")
    with pytest.raises(PluginDiscoveryError, match="registry API"):
        discover_plugins({ComponentKind.ACTION: wrong_api_registry})


def test_outcome_canonical_roundtrip_and_component_rng_streams():
    compiler = _compiler_for(_manifest())
    plan = compiler.compile(_scenario())
    evidence = Evidence(
        evidence_id="example.path",
        source="activate",
        phase="action.completed",
        observed_at=2.0,
        value={"counter": 1},
    )
    result = ComponentResult(
        component_instance_id="activate",
        status=ComponentStatus.PASS,
        summary="observed",
        evidence=(evidence,),
    )
    outcome = RunOutcome(
        plan=plan,
        status=OutcomeStatus.PASS,
        summary="complete",
        component_results=(result,),
    )

    serialized = outcome.to_json()
    assert RunOutcome.from_json(serialized) == outcome
    assert serialized == outcome.to_json()
    assert plan.rng("activate").getrandbits(128) == plan.rng("activate").getrandbits(128)
    assert plan.rng("activate", "fault").getrandbits(128) != plan.rng("activate").getrandbits(128)
    with pytest.raises(ValueError, match="unknown component"):
        plan.rng("missing")


def test_plugin_duplicate_component_id_across_kinds_is_rejected():
    with pytest.raises(DuplicateRegistrationError):
        PluginManifest(
            plugin_id="example.plugin",
            api_version="v1",
            registrations=(
                _registration("example.shared", ComponentKind.ACTION),
                _registration("example.shared", ComponentKind.OBSERVER),
            ),
        )
