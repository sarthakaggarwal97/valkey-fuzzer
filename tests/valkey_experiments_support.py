"""Helpers for the Valkey components' tests: a run service with them, a shipped experiment, and what a unit test of
one component needs: a plan, its registration, its resolved spec, and a fake run context."""

from collections.abc import Sequence
from pathlib import Path

import pytest
import yaml

from valkey_fuzzer.catalog import Catalog, ExperimentDirectory, build_catalog
from valkey_fuzzer.compiler import with_planned_instances
from valkey_fuzzer.components import component_registrations
from valkey_fuzzer.deployment.local.provider import local_deployment_provider_registrations
from valkey_fuzzer.plan import ComponentSpec, DeploymentProviderIdentity, ExecutionPlan
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.results import evidence_of
from valkey_fuzzer.schema import ConfigError, plain_config
from valkey_fuzzer.service import RunService
from valkey_fuzzer.testing import FakeContext, resolved_spec

LOCAL_PROVIDER = DeploymentProviderIdentity("valkey-fuzzer.deployment.local")

_CATALOGS: dict[tuple[str, ...], Catalog] = {}


def shared_catalog(experiment_directories: Sequence[ExperimentDirectory] = ()) -> Catalog:
    """The catalog of the Valkey components and ``experiment_directories``, built once per process.

    A catalog is frozen once built, so every test that would build the same one can share it; the ``catalog``
    fixture is the one without directories. A test that patches what the build reads builds its own with
    :func:`build_catalog`.
    """

    key = tuple(str(directory) for directory in experiment_directories)
    if key not in _CATALOGS:
        _CATALOGS[key] = build_catalog(experiment_directories)
    return _CATALOGS[key]


def valkey_service(**options) -> RunService:
    """A run service with the Valkey components, on the shared catalog of its ``experiment_directories``."""

    return RunService(catalog=shared_catalog(options.pop("experiment_directories", ())), **options)


def valkey_experiment(experiment_id):
    """The shipped Valkey experiment ``experiment_id``."""

    return shared_catalog().experiments.resolve(experiment_id)


def experiment_directory(tmp_path, *documents) -> Path:
    """``tmp_path / "experiments"`` holding each experiment of ``documents`` as ``<id>.yaml``."""

    directory = tmp_path / "experiments"
    directory.mkdir(parents=True, exist_ok=True)
    for document in documents:
        text = yaml.safe_dump(document, sort_keys=False)
        (directory / f"{document['id']}.yaml").write_text(text, encoding="utf-8")
    return directory


def experiment_runtime(tmp_path, *documents, **options) -> RunService:
    """A run service with the experiments ``documents``, written under ``tmp_path`` with its artifacts; ``options``
    are the service's."""

    options.setdefault("artifact_dir", str(tmp_path / "artifacts"))
    return valkey_service(experiment_directories=(str(experiment_directory(tmp_path, *documents)),), **options)


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
    return with_planned_instances(plan, registration)


def registration_for(component_id):
    return next(item for item in component_registrations() if item.component_id == component_id)


def resolved(spec):
    """``spec`` with its config's defaults, as the compiler records it."""

    return resolved_spec(registration_for(spec.use), spec)


def spec_for(use, component_id, role, config):
    """The resolved spec of component ``use`` as ``component_id`` in ``role`` with ``config``."""

    spec = ComponentSpec(role=role, use=use, id=component_id, config=config)
    return resolved_spec(registration_for(use), spec)


def rejects_config(use, config, match, *others, **plan_options):
    """Component ``use`` with ``config`` fails to compile with a ConfigError matching ``match``, in a unit plan
    of it, ``others``, and ``plan_options``."""

    spec = ComponentSpec(role=ComponentRole(use.split(".")[1]), use=use, id="subject", config=config)
    with pytest.raises(ConfigError, match=match):
        registration_for(use).resolve_config(spec, unit_plan((spec, *others), **plan_options))


def fake_context(handle, *components, **kwargs):
    """A FakeContext whose ``valkey.deployment`` service is ``handle``; ``components`` may draw from its rng."""

    return FakeContext(*components, services={"valkey.deployment": handle}, **kwargs)


def injected_fault(fault_cls, use, handle, *, component_id, **config):
    """A ``fault_cls`` fault (``use``, ``component_id``) armed on the event ``go`` with ``config``, injected on a
    fake context over ``handle``: ``(context, spec, fault)``, for the test to publish ``go`` and recover."""

    context = fake_context(handle)
    spec = resolved(
        ComponentSpec(role=ComponentRole.FAULT, use=use, id=component_id, config={"when_event": "go", **config})
    )
    fault = fault_cls()
    fault.inject(context, spec)
    return context, spec, fault


def evidence_values(evidence, evidence_id):
    """The plain values of the ``evidence_id`` items of ``evidence``, in order."""

    return [plain_config(item.value) for item in evidence_of(evidence, evidence_id)]


def chaos_schedule(result):
    """A chaos action's ``valkey.chaos.schedule``."""

    [schedule] = evidence_values(result.evidence, "valkey.chaos.schedule")
    return schedule


def chaos_operation(result, index):
    """Operation ``index`` (such as ``"1.0"``) of a chaos action's schedule."""

    return next(record for record in chaos_schedule(result)["operations"] if record["index"] == index)


def validation_result(*failed_checks, critical=False, inconclusive=False):
    """A deployment state validation's result: a pass, or a failure of ``failed_checks``, each with a message."""

    return {
        "overall_success": not failed_checks,
        "inconclusive": inconclusive,
        "critical": critical,
        "failed_checks": list(failed_checks),
        "error_messages": [f"{check} broke" for check in failed_checks],
        "checks": {},
    }
