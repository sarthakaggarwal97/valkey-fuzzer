"""Pre-deployment environments: declarations and references, the provisioner's lifecycle, and how the
compiler and the runner carry them."""

import copy
import json
import os
from typing import Any

import pytest

from tests.support import (
    ProviderBehavior,
    artifact_provider,
    component_registration,
    deployment_registrations,
    make_catalog,
    make_plan,
    resource_provider,
)
from valkey_fuzzer._data import REFERENCE_DATA_KEY, UnknownRegistrationError, references_from_data
from valkey_fuzzer.catalog import ComponentRegistry, ExtensionCatalog, load_experiment_yaml
from valkey_fuzzer.compiler import ScenarioCompiler
from valkey_fuzzer.contracts import (
    ComponentResult,
    ComponentRole,
    ComponentSpec,
    ComponentStatus,
    ExecutionPlan,
    OutcomeStatus,
    PluginManifest,
)
from valkey_fuzzer.environment import (
    ArtifactProviderRegistration,
    ArtifactProviderRegistry,
    ArtifactSpec,
    DependencyCycleError,
    EnvironmentCleanupError,
    EnvironmentLifecycleError,
    EnvironmentProvisioner,
    EnvironmentReferenceError,
    PolicySpec,
    ProviderError,
    ResolvedArtifact,
    ResourceProviderRegistration,
    ResourceProviderRegistry,
    ResourceSpec,
    SupportDecision,
    parse_environment_reference,
    resolve_environment_references,
    stable_environment_order,
)
from valkey_fuzzer.extensions.testing import FakeDeploymentProvider
from valkey_fuzzer.runner import Runner
from valkey_fuzzer.schema import ExperimentDocumentError, ScenarioCompileError
from valkey_fuzzer.service import RunService

ref = parse_environment_reference
ARTIFACT = "tests.artifact.fake"
RESOURCE = "tests.resource.fake"


def _order(artifacts=(), resources=(), policies=(), *, check=None):
    artifact_registry, resource_registry = ArtifactProviderRegistry(), ResourceProviderRegistry()
    artifact_registry.register(ArtifactProviderRegistration(ARTIFACT, object, outputs=("path", "sha256"), check=check))
    resource_registry.register(ResourceProviderRegistration(RESOURCE, object, outputs=("endpoint",)))
    registries = {"artifact_registry": artifact_registry, "resource_registry": resource_registry}
    return stable_environment_order(artifacts, resources, policies=policies, **registries)


def _provisioner(tmp_path, *behaviors):
    """A provisioner whose providers are ``behaviors``, an artifact or a resource provider by the kind their ID names."""

    artifact_registry, resource_registry = ArtifactProviderRegistry(), ResourceProviderRegistry()
    for behavior in behaviors:
        if ".resource." in behavior.provider_id:
            resource_registry.register(resource_provider(behavior))
        else:
            artifact_registry.register(artifact_provider(behavior))
    return EnvironmentProvisioner(
        artifact_registry=artifact_registry, resource_registry=resource_registry, artifact_root=tmp_path
    )


def _phases(report, phase):
    return [record["dependency"] for record in report.records if record["phase"] == phase]


def _artifact(id="c", **options):
    return ArtifactSpec(id, ARTIFACT, **options)


def _validate(outputs, id="c"):
    registration = ArtifactProviderRegistration(ARTIFACT, object, outputs=("path", "sha256"))
    registration.validate_result(_artifact(), ResolvedArtifact(id, ARTIFACT, outputs, {}))


def _cancel(spec, config):
    raise KeyboardInterrupt


@pytest.mark.parametrize(
    ("build", "error", "match"),
    [
        (lambda: _artifact("has.dot"), ValueError, "artifact_id"),
        (lambda: ResourceSpec("store", "unqualified"), ValueError, "namespaced"),
        (lambda: PolicySpec("p", "tests.policy", config={"bad": object()}), TypeError, "JSON/YAML-safe"),
        (lambda: ResourceSpec("s", RESOURCE, depends_on=("resources.s",)), ValueError, "depend on itself"),
        (lambda: _artifact(config={"b": {REFERENCE_DATA_KEY: "artifacts.c.path"}}), EnvironmentReferenceError,
         "reserved"),
        (lambda: references_from_data({REFERENCE_DATA_KEY: "artifacts.c.path", "default": "/x"}),
         EnvironmentReferenceError, r"only the '\$valkey_fuzzer_ref' key"),
        (lambda: ref("artifacts.c"), EnvironmentReferenceError, "invalid environment reference"),
        (lambda: SupportDecision(supported=False), ValueError, "reason"),
        (lambda: _validate({"path": "/p"}), ProviderError, r"missing \['sha256'\]"),
        (lambda: _validate({"path": "/p", "sha256": "a", "v": "9"}), ProviderError, r"extra \['v'\]"),
        (lambda: _validate({"path": "/p", "sha256": "a"}, id="other"), ProviderError, r"result for .*/other"),
        (lambda: resolve_environment_references(ref("artifacts.c.sha256"), artifacts={}, resources={}),
         EnvironmentReferenceError, "it has not been acquired"),
        (lambda: _order((_artifact(depends_on=("resources.missing",)),)), EnvironmentReferenceError,
         r"artifacts\.c references unknown environment target 'resources\.missing'"),
        (lambda: _order((_artifact(),), (ResourceSpec("s", RESOURCE, config={"b": ref("artifacts.c.version")}),)),
         EnvironmentReferenceError, r"undeclared output 'version' on 'artifacts\.c'"),
        (lambda: _order((_artifact(config={"e": ref("resources.s.endpoint")}),),
                        (ResourceSpec("s", RESOURCE, config={"b": ref("artifacts.c.path")}),)),
         DependencyCycleError, r"artifacts\.c.*resources\.s.*artifacts\.c"),
        (lambda: _order((), (), (PolicySpec("p", "tests.p", config={"e": ref("resources.missing.endpoint")}),)),
         EnvironmentReferenceError, r"policies\.p references unknown environment target"),
        (lambda: _order((ArtifactSpec("c", "missing.artifact.fake"),)), UnknownRegistrationError, "missing"),
        (lambda: _order((_artifact(),), check=lambda spec, config: "x"), ProviderError, "check must return None"),
        (lambda: _order((_artifact(),), check=_cancel), KeyboardInterrupt, None),
    ],
)  # fmt: skip
def test_an_invalid_declaration_result_or_environment_is_rejected(build, error, match):
    with pytest.raises(error, match=match):
        build()


# ---------------------------------------------------------------------------
# The provisioner: acquire in dependency order, unwind on failure, release in reverse
# ---------------------------------------------------------------------------


def test_acquisition_resolves_references_in_dependency_order_and_releases_in_reverse(tmp_path):
    store = ProviderBehavior("tests.resource.store", outputs={"endpoint": "secret://store"})
    candidate = ProviderBehavior("tests.artifact.candidate", outputs={"path": "/private/c"}, provenance={"d": 1})
    observer = ProviderBehavior("tests.resource.observer", outputs={"token": "secret-token"})
    session = _provisioner(tmp_path, candidate, observer, store).acquire(
        (ArtifactSpec("candidate", "tests.artifact.candidate", config={"endpoint": ref("resources.store.endpoint")}),),
        (
            ResourceSpec("observer", "tests.resource.observer", config={"binary": [ref("artifacts.candidate.path")]}),
            ResourceSpec("store", "tests.resource.store"),
        ),
        run_id="run/with spaces",
        seed=42,
        deadline=123.5,
    )

    acquired = ["resources.store", "artifacts.candidate", "resources.observer"]
    assert _phases(session.report, "acquire") == acquired
    assert candidate.configs["support"] == candidate.configs["acquire"] == {"endpoint": "secret://store"}
    assert observer.configs["acquire"] == {"binary": ("/private/c",)}
    assert (candidate.contexts["acquire"].seed, candidate.contexts["acquire"].deadline) == (42, 123.5)
    report = json.dumps(session.report.to_data())
    assert '"d": 1' in report and not any(secret in report for secret in ("/private/c", "secret:", "secret-token"))

    assert session.close().status == "cleaned"
    assert _phases(session.report, "cleanup") == acquired[::-1]
    with pytest.raises(EnvironmentLifecycleError, match="closed"):
        session.resolve(ref("artifacts.candidate.path"))


def test_artifact_directories_stay_inside_the_root_and_differ_per_run(tmp_path):
    behavior = ProviderBehavior("tests.artifact.dirs", outputs={"value": "v"})
    root, paths = os.path.realpath(tmp_path), set()
    for run_id, artifact_id in [(".", "c"), ("..", "c"), ("a/b", "c"), ("x", "x" * 10_000)]:
        _provisioner(tmp_path, behavior).acquire(
            (ArtifactSpec(artifact_id, behavior.provider_id),), (), run_id=run_id, seed=1
        ).close()
        directory = os.path.realpath(behavior.contexts["support"].artifact_dir)
        assert os.path.commonpath((root, directory)) == root and len(os.path.basename(directory)) <= 73
        assert ".." not in os.path.relpath(directory, root).split(os.sep)
        paths.add(directory)
    assert len(paths) == 4


@pytest.mark.parametrize(
    ("options", "error", "calls", "status"),
    [
        ({"acquire_error": RuntimeError("acquire secret")}, RuntimeError, ["support", "acquire", "rollback"], "failed"),
        # A failed rollback does not mask the acquisition error, and the report says it.
        ({"acquire_error": RuntimeError("acquire"), "rollback_error": RuntimeError("rollback secret")}, RuntimeError,
         ["support", "acquire", "rollback"], "cleanup_failed"),
        ({"acquire_error": KeyboardInterrupt()}, KeyboardInterrupt, ["support", "acquire", "rollback"], "failed"),
        # Support changes nothing, so a broken support check is not rolled back.
        ({"support": "yes"}, ProviderError, ["support"], "failed"),
    ],
    ids=["raises", "rollback-fails", "interrupted", "invalid-support"],
)  # fmt: skip
def test_a_failed_acquisition_rolls_back_its_node_and_releases_earlier_ones(tmp_path, options, error, calls, status):
    first = ProviderBehavior("tests.artifact.first", outputs={"value": "v"})
    broken = ProviderBehavior("tests.artifact.broken", **options)
    with pytest.raises(error) as raised:
        # Its config needs first's output, so first is acquired before it.
        needs_first = ArtifactSpec("broken", broken.provider_id, config={"input": ref("artifacts.first.value")})
        _provisioner(tmp_path, first, broken).acquire(
            (ArtifactSpec("first", first.provider_id), needs_first), (), run_id="failure", seed=2
        )

    assert [method for method, _ in broken.calls] == calls
    assert first.calls[-1] == ("cleanup", "artifacts.first")
    report = raised.value.environment_report
    assert report.status == status and "secret" not in json.dumps(report.to_data())


@pytest.mark.parametrize(
    "errors", [(RuntimeError("first secret"), ValueError("second secret")), (None, KeyboardInterrupt())]
)
def test_release_attempts_every_lease_and_reports_only_the_error_types(tmp_path, errors):
    first = ProviderBehavior("tests.artifact.first", cleanup_error=errors[0])
    second = ProviderBehavior("tests.artifact.second", cleanup_error=errors[1])
    session = _provisioner(tmp_path, first, second).acquire(
        (ArtifactSpec("first", first.provider_id), ArtifactSpec("second", second.provider_id)), (), run_id="r", seed=4
    )

    with pytest.raises((EnvironmentCleanupError, KeyboardInterrupt)) as raised:
        session.close()

    assert second.calls[-1][0] == first.calls[-1][0] == "cleanup"
    assert session.closed and session.report.status == "cleanup_failed" and session.close() is session.report
    if errors[0] is None:
        assert raised.value is errors[1]
        return
    assert [path for path, _, _ in raised.value.failures] == ["artifacts.second", "artifacts.first"]
    serialized = json.dumps(session.report.to_data())
    assert "secret" not in serialized + str(raised.value) and "RuntimeError" in serialized


# ---------------------------------------------------------------------------
# The runner: acquire before the deployment, release after it, and the verdict
# ---------------------------------------------------------------------------


def _run(tmp_path, plan, *behaviors, deployment=None, action_status=None):
    registry = ComponentRegistry()
    if action_status is not None:

        class Action:
            def run(self, context, spec):
                return ComponentResult(spec.id, action_status, "feature verdict")

        registry.register(component_registration("tests.action.fake", factory=Action))
        plan = make_plan(**{**plan, "components": (ComponentSpec("tests.action.fake", "x", ComponentRole.ACTION),)})
    else:
        plan = make_plan(**plan)
    provisioner = _provisioner(tmp_path, *behaviors)
    catalog = make_catalog(
        registry,
        deployment_registrations(standalone=lambda: deployment or FakeDeploymentProvider()),
        artifacts=provisioner.artifact_registry,
        resources=provisioner.resource_registry,
    )
    return Runner(catalog, artifact_dir=str(tmp_path)).run(plan)


@pytest.mark.parametrize(
    ("needs_output", "cleanup_error", "status", "calls", "report"),
    [
        (True, None, OutcomeStatus.SKIP_UNSUPPORTED,
         ["artifact.support", "artifact.acquire", "resource.support", "artifact.cleanup"], "unsupported"),
        (True, RuntimeError("cleanup-secret"), OutcomeStatus.ERROR,
         ["artifact.support", "artifact.acquire", "resource.support", "artifact.cleanup"], "cleanup_failed"),
        # Support needs no output here, so it is decided before anything is acquired.
        (False, None, OutcomeStatus.SKIP_UNSUPPORTED, ["artifact.support", "resource.support"], "unsupported"),
    ],
)  # fmt: skip
def test_an_unsupported_environment_skips_the_run_only_after_a_clean_unwind(
    tmp_path, needs_output, cleanup_error, status, calls, report
):
    log = []
    resource = ResourceSpec(
        "optional",
        RESOURCE,
        config={"binary": ref("artifacts.c.path")} if needs_output else {},
        depends_on=() if needs_output else ("artifacts.c",),
    )
    outcome = _run(
        tmp_path,
        {"artifacts": (_artifact(),), "resources": (resource,)},
        ProviderBehavior(ARTIFACT, outputs={"path": "/c"}, cleanup_error=cleanup_error, log=log),
        ProviderBehavior(RESOURCE, support=SupportDecision.deny("not available"), log=log),
        deployment=FakeDeploymentProvider(calls=log, name="deployment"),
    )

    # Nothing unsupported is provisioned, nor the deployment.
    assert outcome.status is status and log == ["deployment.validate", *calls]
    assert outcome.diagnostics["environment_report"]["status"] == report
    assert "cleanup-secret" not in outcome.to_json()


@pytest.mark.parametrize(
    ("options", "action", "expected", "report"),
    [
        ({"cleanup_error": RuntimeError("SECRET")}, ComponentStatus.PASS, OutcomeStatus.ERROR, "cleanup_failed"),
        # A trustworthy FAIL stays a FAIL when only the environment's release failed.
        ({"cleanup_error": RuntimeError("SECRET")}, ComponentStatus.FAIL, OutcomeStatus.FAIL, "cleanup_failed"),
        ({"acquire_error": RuntimeError("SECRET")}, ComponentStatus.PASS, OutcomeStatus.ERROR, "failed"),
    ],
    ids=["release-fails-after-pass", "release-fails-after-fail", "acquire-fails"],
)
def test_environment_failures_decide_the_verdict_and_stay_out_of_the_outcome(
    tmp_path, options, action, expected, report
):
    deployment = FakeDeploymentProvider()
    plan = {"artifacts": (_artifact(),)}
    outcome = _run(tmp_path, plan, ProviderBehavior(ARTIFACT, **options), deployment=deployment, action_status=action)

    assert outcome.status is expected
    assert outcome.diagnostics["environment_report"]["status"] == report
    assert "SECRET" not in outcome.to_json() + repr(outcome)
    if "acquire_error" in options:
        assert deployment.calls == ["validate"]


# ---------------------------------------------------------------------------
# The compiler and the runtime: scenario environments become typed references, resolved at run time
# ---------------------------------------------------------------------------

EXPERIMENT = """
id: tests.environment
mode: smoke
deployment:
  modes: [standalone]
inputs:
  type: object
  properties:
    channel: {type: string}
    binary: {type: environment_reference, reference_kinds: [artifacts]}
    payload: {type: object}
  required: [channel]
  additionalProperties: false
components:
  - use: tests.action.fake
    id: exercise
    config:
      endpoint: {$ref: resources.store.endpoint}
      from_input: {$ref: inputs.binary}
"""


def _scenario() -> dict[str, Any]:
    return {
        "scenario_id": "environment-plan",
        "seed": 101,
        "experiment": {
            "id": "tests.environment",
            "inputs": {"channel": "nightly", "binary": {"$ref": "artifacts.candidate.path"}},
        },
        "artifacts": {"candidate": {"use": ARTIFACT, "with": {"channel": {"$ref": "inputs.channel"}}}},
        "resources": {"store": {"use": RESOURCE, "with": {"binary": {"$ref": "artifacts.candidate.path"}}}},
        "deployment": {"mode": "standalone", "valkey_binary": {"$ref": "artifacts.candidate.path"}},
    }


class _Action:
    def run(self, context, spec):
        _Action.seen = dict(spec.config)
        return ComponentResult(spec.id, ComponentStatus.PASS, "passed")


def _manifest():
    return PluginManifest(
        plugin_id="tests.environment",
        registrations=(component_registration("tests.action.fake", factory=_Action),),
        experiments=(load_experiment_yaml(EXPERIMENT),),
        artifact_providers=(artifact_provider(ProviderBehavior(ARTIFACT, outputs={"path": "/test/valkey"})),),
        resource_providers=(resource_provider(ProviderBehavior(RESOURCE, outputs={"endpoint": "test://store"})),),
    )


def _compile(scenario):
    catalog = ExtensionCatalog()
    catalog.register_plugins([_manifest()])
    catalog.deployment_providers.register_many(deployment_registrations(standalone=object).snapshot().values())
    return ScenarioCompiler(catalog).compile(scenario)


def test_the_compiler_keeps_references_typed_and_the_runtime_resolves_them_around_the_deployment():
    plan = _compile(_scenario())
    binary = ref("artifacts.candidate.path")
    assert plan.components[0].config == {"endpoint": ref("resources.store.endpoint"), "from_input": binary}
    assert plan.deployment["valkey_binary"] == binary
    assert plan.artifacts[0] == ArtifactSpec("candidate", ARTIFACT, config={"channel": "nightly"})
    assert ExecutionPlan.from_data(json.loads(plan.to_json())) == plan

    class Deployment(FakeDeploymentProvider):
        def validate(self, plan):
            self.binary = plan.deployment["valkey_binary"]
            super().validate(plan)

    deployment = Deployment()
    runtime = RunService(
        discover_plugins=False,
        deployment_providers=deployment_registrations(standalone=lambda: deployment).snapshot().values(),
        plugins=[_manifest()],
    )
    outcome = runtime.runner.run(runtime.compile(_scenario()))

    assert outcome.status is OutcomeStatus.PASS and deployment.calls == ["validate", "provision", "cleanup"]
    assert deployment.binary == "/test/valkey"
    assert _Action.seen == {"endpoint": "test://store", "from_input": "/test/valkey"}
    assert outcome.diagnostics["environment_report"]["status"] == "cleaned"
    assert "/test/valkey" not in outcome.to_json()


def _set(path, value):
    def edit(scenario):
        *parents, last = path.split(".")
        target = scenario
        for key in parents:
            target = target[key]
        target[last] = value

    return edit


@pytest.mark.parametrize(
    ("edit", "error", "match"),
    [
        (_set("experiment.inputs.channel", {"$ref": "artifacts.candidate.path"}), ScenarioCompileError,
         r"experiment\.inputs\.channel cannot contain environment references"),
        (_set("experiment.inputs.payload", {"nested": {"$ref": "artifacts.candidate.path"}}), ScenarioCompileError,
         r"experiment\.inputs\.payload cannot contain environment references"),
        (_set("experiment.inputs.binary", {"$ref": "resources.store.endpoint"}), ScenarioCompileError,
         r"references kind 'resources'; allowed kinds are \['artifacts'\]"),
        (_set("experiment.inputs.payload", {"$valkey_fuzzer_literal": {}, "extra": True}), ScenarioCompileError,
         r"literal marker must contain only the '\$valkey_fuzzer_literal' key"),
        (_set("artifacts.candidate.unexpected", True), ExperimentDocumentError, r"artifacts\.candidate.*unknown fields"),
        (_set("artifacts.candidate.use", "tests.artifact.missing"), UnknownRegistrationError,
         r"artifact provider 'tests\.artifact\.missing' is not registered"),
        (_set("deployment.valkey_binary", {"$ref": "artifacts.candidate.undeclared"}), EnvironmentReferenceError,
         r"undeclared output 'undeclared' on 'artifacts\.candidate'"),
        (_set("artifacts.candidate.depends_on", ["resources.store"]), DependencyCycleError,
         r"artifacts\.candidate.*resources\.store.*artifacts\.candidate"),
    ],
)  # fmt: skip
def test_the_compiler_rejects_an_invalid_scenario_environment(edit, error, match):
    scenario = copy.deepcopy(_scenario())
    edit(scenario)
    with pytest.raises(error, match=match):
        _compile(scenario)
