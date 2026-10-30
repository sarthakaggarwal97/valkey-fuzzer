"""The catalog as data: what ``valkey-fuzzer list ... --format json`` prints.

Every field is derived from the frozen :class:`~valkey_fuzzer.catalog.ExtensionCatalog`:
an experiment's ``inputs:`` and deployment rules, and a component's config,
are rendered from the :class:`~valkey_fuzzer.schema.ConfigSchema` the
compiler checks them with (:meth:`~valkey_fuzzer.schema.ConfigSchema.json_schema`),
so the catalog cannot drift from what a scenario must meet. Rows are sorted
(experiments and generators by ID, components by role and then ID) and their mappings print
with sorted keys; lists that carry an order (components, enums) keep it.
"""

from collections.abc import Mapping
from typing import Any, Optional

from ._data import to_plain_data
from .catalog import ExtensionCatalog
from .contracts import ComponentRegistration, ComponentSpec, ExperimentSpec
from .generator import GeneratorBlock, generator_document, installed_pools
from .schema import ConfigSchema


def implementation_name(factory: Any) -> str:
    """``<module>.<qualname>`` of a component or provider factory, or of a generator hook."""

    return f"{getattr(factory, '__module__', '?')}.{getattr(factory, '__qualname__', '?')}"


def _registered_role(catalog: ExtensionCatalog, use: str) -> Optional[str]:
    registration = catalog.components.get(use)
    return None if registration is None else registration.role.value


def _component(catalog: ExtensionCatalog, component: ComponentSpec) -> dict[str, Any]:
    return {
        "id": component.id,
        "use": component.use,
        "role": _registered_role(catalog, component.use),
        "depends_on": list(component.depends_on),
        "required": component.required,
        "timeout_s": component.timeout_s,
        # As the experiment writes it: {$ref: inputs.<name>} shows which input sets a field.
        "config": to_plain_data(component.config),
    }


def _generator(spec: ExperimentSpec, pools: Mapping[str, Mapping[str, Any]]) -> Optional[dict[str, Any]]:
    """The merged ``metadata.generator`` block and the server config pools it draws from, or None."""

    declaration = generator_document(spec)
    if declaration is None:
        return None
    # Parsing checks the block the way generating does, and resolves its pools.
    block = GeneratorBlock.parse(spec, pools)
    return {"declaration": declaration, "pools": {pool_id: dict(pool) for pool_id, pool in block.pools}}


def describe_experiments(catalog: ExtensionCatalog) -> list[dict[str, Any]]:
    """Every experiment: inputs and deployment rules as JSON Schemas, generator, components, and requirements."""

    pools = installed_pools(catalog)
    rows = []
    for experiment_id, spec in sorted(catalog.experiments.snapshot().items()):
        metadata = to_plain_data(spec.metadata)
        rows.append(
            {
                "id": experiment_id,
                "description": metadata.get("description", ""),
                "mode": spec.mode.value,
                **catalog.origin("experiment", experiment_id),
                "deployment_modes": list(spec.deployment_modes),
                "deployment_requirements": ConfigSchema.from_rules(spec.deployment_requirements).json_schema(),
                "inputs": ConfigSchema.from_document(spec.inputs).json_schema(),
                "generator": _generator(spec, pools),
                "components": [_component(catalog, component) for component in spec.components],
                "startup_overlays": [to_plain_data(overlay) for overlay in spec.startup_overlays],
                "required_events": list(spec.required_events),
                "required_evidence": [to_plain_data(item) for item in spec.required_evidence],
            }
        )
    return rows


def describe_components(catalog: ExtensionCatalog) -> list[dict[str, Any]]:
    """Every component registration: its role, config schema, deployment modes, declarations, and origin.

    ``config`` is None for a registration without a schema, whose config is
    taken as written; ``deployment_modes`` is None when it supports every mode;
    ``declares`` is the events and evidence it emits, with their schemas, and
    the state it changes.
    """

    return [
        {
            "component_id": registration.component_id,
            "role": registration.role.value,
            "implementation": implementation_name(registration.factory),
            "plugin": catalog.origin("component", registration.component_id)["plugin"],
            "config": None if registration.config is None else registration.config.json_schema(),
            "deployment_modes": None if registration.deployment_modes is None else list(registration.deployment_modes),
            "declares": registration.declares.json_schema(),
        }
        for registration in components_by_role(catalog)
    ]


def components_by_role(catalog: ExtensionCatalog) -> list[ComponentRegistration[Any]]:
    """The catalog's component registrations, sorted by role and then ID, as the catalog lists them."""

    return sorted(catalog.components.snapshot().values(), key=lambda item: (item.role.value, item.component_id))


def describe_generators(catalog: ExtensionCatalog) -> list[dict[str, Any]]:
    """Every generator hook, by ID: its implementation and the plugin that registered it."""

    return [
        {
            "generator_id": generator_id,
            "implementation": implementation_name(registration.hook),
            "plugin": catalog.origin("generator", generator_id)["plugin"],
        }
        for generator_id, registration in catalog.generators.snapshot().items()
    ]
