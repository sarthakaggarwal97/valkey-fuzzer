"""Helpers for the Valkey experiments plugin's tests: discovery that finds only the plugin (installed or not), and
what a unit test of one component needs: a plan, its registration, its resolved spec, and a fake run context."""

from types import SimpleNamespace
from unittest.mock import patch

from valkey_fuzzer.compiler import with_planned_instances
from valkey_fuzzer.contracts import ComponentRole, ComponentSpec, DeploymentProviderIdentity, ExecutionPlan
from valkey_fuzzer.deployment.local.provider import local_deployment_provider_registrations
from valkey_fuzzer.extensions.testing import FakeContext, resolved_spec
from valkey_fuzzer.service import RunService
from valkey_fuzzer_experiments import PLUGIN, component_registrations

LOCAL_PROVIDER = DeploymentProviderIdentity("valkey-fuzzer.deployment.local")


class _EntryPoint:
    name = PLUGIN.plugin_id
    value = "valkey_fuzzer_experiments:PLUGIN"
    dist = SimpleNamespace(name="valkey-fuzzer")

    @staticmethod
    def load():
        return PLUGIN


class _EntryPoints:
    @staticmethod
    def select(*, group):
        return (_EntryPoint(),) if group == "valkey_fuzzer.plugins" else ()


def valkey_entry_points():
    """Patch plugin discovery so it finds only the Valkey experiments plugin."""

    return patch("valkey_fuzzer.catalog.importlib_metadata.entry_points", lambda: _EntryPoints())


def valkey_service(**options) -> RunService:
    """A run service with the Valkey experiments plugin, its catalog already built."""

    service = RunService(**options)
    with valkey_entry_points():
        service.catalog  # noqa: B018
    return service


def unit_plan(components=(), deployment_mode="standalone", document=None, resources=(), startup_overlays=()):
    """A plan holding ``components`` and an evidence oracle, to check component configs against.

    Its deployment has the local provider's defaults filled in, and it records the instances that provider
    plans, as a compiled plan does.
    """

    registration = local_deployment_provider_registrations()[deployment_mode]
    oracle = ComponentSpec(role=ComponentRole.ORACLE, use="valkey.oracle.evidence", id="assert")
    plan = ExecutionPlan(
        scenario_id="unit",
        experiment_id="tests.experiment",
        seed=1,
        deployment_mode=deployment_mode,
        components=(*components, oracle),
        deployment_provider=LOCAL_PROVIDER,
        mode="smoke",
        resources=resources,
        startup_overlays=startup_overlays,
        deployment=registration.normalizer(document or {"mode": deployment_mode}),
    )
    return with_planned_instances(plan, registration, error=ValueError)


def registration_for(component_id):
    return next(item for item in component_registrations() if item.component_id == component_id)


def resolved(spec):
    """``spec`` with its config's defaults, as the compiler records it."""

    return resolved_spec(registration_for(spec.use), spec)


def check_config(component_id, role, config, *, deployment_mode="standalone", extra=()):
    """Check ``config`` as the compiler does for a component ``subject``; raises ConfigError."""

    spec = ComponentSpec(role=role, use=component_id, id="subject", config=config)
    registration_for(component_id).resolve_config(spec, unit_plan((spec, *extra), deployment_mode=deployment_mode))


def fake_context(handle, *components, **kwargs):
    """A FakeContext whose ``valkey.deployment`` service is ``handle``; ``components`` may draw from its rng."""

    return FakeContext(*components, services={"valkey.deployment": handle}, **kwargs)
