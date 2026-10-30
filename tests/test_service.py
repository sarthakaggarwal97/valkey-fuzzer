"""RunService below the CLI: its catalog, report folders, replay provenance, and the run reports it refuses."""

import copy
import json
from dataclasses import replace
from pathlib import Path

import pytest

from tests.support import component_registration, deployment_registrations, install_entry_points, make_plan
from valkey_fuzzer import runner as runner_module
from valkey_fuzzer._data import FrozenRegistryError, RegistrationError, file_sha256
from valkey_fuzzer.catalog import (
    DuplicateExperimentError,
    PluginDiscoveryError,
    UnknownExperimentError,
    load_experiment_yaml,
)
from valkey_fuzzer.contracts import (
    ComponentResult,
    ComponentStatus,
    DeploymentProviderRegistration,
    ExecutionPlan,
    ExperimentSpec,
    OutcomeStatus,
    PluginManifest,
    RunMode,
)
from valkey_fuzzer.deployment.local.rerun import binary_drift
from valkey_fuzzer.environment import ArtifactSpec, parse_environment_reference
from valkey_fuzzer.extensions.testing import FakeDeploymentProvider
from valkey_fuzzer.runner import RunnerError
from valkey_fuzzer.schema import ExperimentDocumentError, ScenarioCompileError
from valkey_fuzzer.service import InputError, ReportWriteError, RunService, new_report_folder

FORKLESS = str(Path(__file__).resolve().parents[1] / "examples" / "valkey" / "forkless_save.yaml")


def test_report_folders_sort_by_start_time_and_never_collide(tmp_path, monkeypatch):
    suffixes = iter(["aaaaaa", "aaaaaa", "bbbbbb", "cccccc"])
    monkeypatch.setattr(runner_module.secrets, "token_hex", lambda size: next(suffixes))
    monkeypatch.setattr(runner_module.time, "time", lambda: 1_700_000_000.25)
    first = new_report_folder(str(tmp_path), "forkless-save")
    # Same millisecond and the same random suffix: the next suffix is tried.
    second = new_report_folder(str(tmp_path), "forkless-save")
    monkeypatch.setattr(runner_module.time, "time", lambda: 1_700_000_000.5)
    third = new_report_folder(str(tmp_path), "a/b c")
    assert [Path(item).name for item in (first, second, third)] == [
        "20231114T221320.250Z-forkless-save-aaaaaa",
        "20231114T221320.250Z-forkless-save-bbbbbb",
        "20231114T221320.500Z-a-b-c-cccccc",
    ]
    (tmp_path / "file").write_text("")
    with pytest.raises(ReportWriteError, match="could not create a report folder under"):
        new_report_folder(str(tmp_path / "file"), "run")


def test_provenance_ignores_unused_plugins_but_a_garbled_record_drifts(service):
    plan = service.prepare_run(FORKLESS).plan
    recorded = service.reproducer(plan)
    recorded["plugins"].append({"plugin_id": "other.plugin", "metadata": {}})
    assert service.provenance_drift(plan, recorded) == ([], [])
    # Known-bad: a malformed list compares as empty, so what it should hold drifts.
    recorded["components"], recorded["plugins"] = "garbled", None
    items = {entry["item"] for entry in service.provenance_drift(plan, recorded)[0]}
    assert {"plugin valkey-fuzzer.valkey-experiments", *(f"component {item.use}" for item in plan.components)} <= items


@pytest.mark.parametrize(
    ("edit", "missing"),
    [
        (lambda report: report.pop("reproducer"), "reproducer"),
        (lambda report: report.pop("scenario"), "scenario"),
        (lambda report: report["reproducer"].pop("components"), "reproducer.components"),
        (lambda report: report.update(reproducer=[]), "reproducer"),
        (lambda report: report.pop("outcome"), "outcome"),
    ],
)
def test_replay_and_minimize_reject_an_incomplete_run_report(service, tmp_path, edit, missing):
    prepared = service.prepare_run(FORKLESS)
    report = copy.deepcopy(service.execute(prepared.plan, scenario=prepared.scenario).report)
    edit(report)
    path = tmp_path / "report.json"
    path.write_text(json.dumps(report))
    for prepare in (service.prepare_replay, service.prepare_minimization):
        with pytest.raises(InputError, match=rf"not a complete run report: it has no {missing}\b"):
            prepare(str(path))


# ---------------------------------------------------------------------------
# The catalog: plugins and experiment directories load into one frozen catalog, which compiles and runs on the
# deployment provider the scenario picks
# ---------------------------------------------------------------------------


def _service(*experiment_directories, **options) -> RunService:
    """A run service whose catalog is built now, as a command's is on first use."""

    service = RunService(experiment_directories=experiment_directories, **options)
    service.catalog  # noqa: B018
    return service


SCENARIO = {
    "scenario_id": "s",
    "seed": 41,
    "experiment": {"id": "example.feature"},
    "deployment": {"mode": "standalone"},
}


def _yaml(experiment_id="example.feature"):
    return f"id: {experiment_id}\nmode: smoke\ndeployment: {{modes: [standalone]}}\ncomponents: [{{use: example.action.fake, id: act}}]\n"


class _Action:
    def run(self, context, spec):
        return ComponentResult(spec.id, ComponentStatus.PASS, "done")


def test_installed_plugins_and_directory_experiments_compile_and_run_on_a_frozen_catalog(tmp_path, monkeypatch):
    calls = []
    plugin = PluginManifest("example.plugin", (component_registration("example.action.fake", factory=_Action),))
    install_entry_points(monkeypatch, plugin)
    (tmp_path / "experiment.yaml").write_text(_yaml(), encoding="utf-8")
    providers = deployment_registrations(standalone=lambda: FakeDeploymentProvider(calls=calls))
    service = _service(tmp_path, deployment_providers=providers.snapshot().values())

    outcome = service.runner.run(service.compile(SCENARIO))
    assert outcome.status is OutcomeStatus.PASS and outcome.component_results[0].component_id == "act"
    assert calls == ["validate", "provision", "cleanup"]
    with pytest.raises(FrozenRegistryError):
        service.catalog.register_plugins((PluginManifest(plugin_id="example.late"),))
    # An experiment of a plugin that was not loaded says why.
    unloaded = {**SCENARIO, "experiment": {"id": "valkey.forkless-save"}}
    with pytest.raises(UnknownExperimentError, match=r"plugins are disabled \(--no-plugins\)"):
        RunService(discover_plugins=False).compile(unloaded)
    install_entry_points(monkeypatch)
    with pytest.raises(UnknownExperimentError, match=r"importable.*pip install -e \."):
        RunService().compile(unloaded)


@pytest.mark.parametrize(
    ("files", "plugin", "error"),
    [
        ({"first/a.yaml": "example.a", "second/a.yaml": "example.a"}, None, DuplicateExperimentError),
        ({"first/a.yaml": "example.feature"}, "experiment", DuplicateExperimentError),
        ({"first/a.yaml": "example.a", "second/broken.yaml": None}, None, ExperimentDocumentError),
        ({"first/a.yaml": "example.a"}, "fragment", RegistrationError),
        ({"first/a.yaml": "example.a"}, "broken", PluginDiscoveryError),
    ],
    ids=["one-id-in-two-directories", "a-directory-and-a-plugin", "a-malformed-document", "a-fragment-twice", "a-plugin-that-fails-to-load"],
)  # fmt: skip
def test_a_catalog_with_a_conflict_or_a_malformed_document_is_never_built(tmp_path, monkeypatch, files, plugin, error):
    for name, experiment_id in files.items():
        (tmp_path / name).parent.mkdir(exist_ok=True)
        (tmp_path / name).write_text(_yaml(experiment_id) if experiment_id else "id: a\nid: b\n", encoding="utf-8")
    fragment = {"example.base": {"required_events": ["act.completed"]}}
    installed = {
        "experiment": [PluginManifest("example.plugin", experiments=(load_experiment_yaml(_yaml()),))],
        "fragment": [PluginManifest("installed.plugin", experiment_fragments=fragment)],
        "broken": [RuntimeError("cannot import dependency")],
    }.get(plugin, [])
    install_entry_points(monkeypatch, *installed)
    explicit = [PluginManifest("explicit.plugin", experiment_fragments=fragment)] if plugin == "fragment" else []

    with pytest.raises(error):
        _service(
            *sorted({path.parent for path in tmp_path.glob("*/*.yaml")}), plugins=explicit, deployment_providers=()
        )


def _deployment(provider_id="example.deployment.fake", factory=object):
    return DeploymentProviderRegistration(deployment_mode="example-mode", provider_id=provider_id, factory=factory)


def test_a_scenario_picks_one_of_several_providers_of_its_mode_and_replay_keeps_that_one():
    calls, local_calls = [], []
    local = _deployment(factory=lambda: FakeDeploymentProvider(calls=local_calls))
    docker = _deployment("example.deployment.docker", lambda: FakeDeploymentProvider(calls=calls))
    experiment = ExperimentSpec("example.choice", mode=RunMode.SMOKE, deployment_modes=("example-mode",), components=())
    manifest = PluginManifest("example.plugin", experiments=(experiment,), deployment_providers=(docker,))
    scenario = {"scenario_id": "choice", "seed": 7, "experiment": {"id": "example.choice"}}
    service = _service(discover_plugins=False, plugins=(manifest,), deployment_providers=(local,))

    with pytest.raises(ScenarioCompileError, match="multiple registered providers"):
        service.compile({**scenario, "deployment": {"mode": "example-mode"}})
    plan = service.compile(
        {**scenario, "deployment": {"mode": "example-mode", "provider": "example.deployment.docker"}}
    )
    assert service.runner.run(ExecutionPlan.from_data(json.loads(plan.to_json()))).status is OutcomeStatus.PASS
    assert (calls, local_calls) == (["validate", "provision", "cleanup"], [])

    manifest = replace(manifest, deployment_providers=())
    without = _service(discover_plugins=False, plugins=(manifest,), deployment_providers=(local,))
    with pytest.raises(RunnerError, match=r"'example\.deployment\.docker' is not registered for 'example-mode'"):
        without.validate_plan(plan)
    without_any = _service(discover_plugins=False, plugins=(manifest,), deployment_providers=())
    with pytest.raises(ScenarioCompileError, match="has no registered deployment provider"):
        without_any.compile({**scenario, "deployment": {"mode": "example-mode"}})


def test_binary_drift_names_a_referenced_binary_it_cannot_read():
    # Before the fix the message showed the reference's serialized marker, not its path.
    plan = make_plan(
        deployment={"mode": "standalone", "valkey_binary": parse_environment_reference("artifacts.build.path")},
        artifacts=(ArtifactSpec("build", "tests.artifact.fake"),),
    )
    recorded = [{"instance_id": "valkey-0", "binary": "/old/valkey-server", "binary_sha256": "ab" * 32}]

    drift, unchecked = binary_drift(plan, recorded, replayed_instances=[{"instance_id": "valkey-0"}])

    assert drift == []
    assert unchecked == ["Valkey binary of valkey-0: the replay's binary (artifacts.build.path) cannot be read"]


def test_replay_compares_each_artifact_by_its_recorded_sha256(service, canned_runs, tmp_path):
    # rolling-upgrade's to_binary names only an artifact; no instance starts on it, so the
    # per-instance comparison never sees it.
    binaries = {}
    for name in ("baseline", "candidate"):
        binaries[name] = tmp_path / f"{name}-server"
        binaries[name].write_text(f"#!/bin/sh\n# {name}\n")
        binaries[name].chmod(0o755)
    scenario = tmp_path / "upgrade.yaml"
    scenario.write_text(
        json.dumps(
            {
                "scenario_id": "rolling-upgrade",
                "seed": 61,
                "artifacts": {
                    name: {"use": "valkey.artifact.local-binary", "with": {"path": str(path)}}
                    for name, path in binaries.items()
                },
                "experiment": {
                    "id": "valkey.rolling-upgrade",
                    "inputs": {"to_binary": {"$ref": "artifacts.candidate.binary"}},
                },
                "deployment": {
                    "mode": "standalone",
                    "replicas_per_primary": 1,
                    "valkey_binary": {"$ref": "artifacts.baseline.binary"},
                },
            }
        )
    )

    def acquired(name, digest):
        result = {"artifact_id": name, "provenance": {} if digest is None else {"sha256": digest}}
        return {
            "dependency": f"artifacts.{name}",
            "phase": "acquire",
            "outcome": "succeeded",
            "details": {"result": result},
        }

    # What the provisioner records: each resolved artifact's provenance, here as the local-binary provider does.
    unrecorded = set()
    canned_runs.diagnostics = lambda plan: {
        "environment_report": {
            "records": [
                acquired(name, None if name in unrecorded else file_sha256(str(path)))
                for name, path in binaries.items()
            ]
        }
    }
    prepared = service.prepare_run(str(scenario))
    record = service.execute(prepared.plan, scenario=prepared.scenario, report_dir=str(tmp_path / "r"))
    binaries["candidate"].write_text("#!/bin/sh\n# rebuilt from another commit\n")

    replay = service.prepare_replay(record.report_path)
    assert replay.drift == []
    late = service.replay(replay).late_drift
    assert [entry["item"] for entry in late] == ["artifacts.candidate sha256"]

    unrecorded.add("candidate")
    record = service.execute(prepared.plan, scenario=prepared.scenario, report_dir=str(tmp_path / "r2"))
    assert (
        "artifacts.candidate: the recorded run has no sha256 for it"
        in service.prepare_replay(record.report_path).not_verified
    )
