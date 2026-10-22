"""Tests for the single-path generic command-line interface."""

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock, patch

import pytest
import yaml

import src.cli as cli_module
from src.cli import (
    EXIT_FEATURE_FAILURE,
    EXIT_HARNESS_ERROR,
    EXIT_SUCCESS,
    EXIT_UNSUPPORTED,
    FuzzerCLI,
    create_parser,
    main,
)
from src.extensibility import (
    EXTENSION_API_VERSION,
    ComponentKind,
    ComponentRegistration,
    ComponentResult,
    ComponentStatus,
    Evidence,
    OutcomeStatus,
    PluginManifest,
    RunMode,
    RunOutcome,
    RunPlan,
    TopologyProviderIdentity,
    TopologyProviderRegistration,
)
from src.feature_runner import ProvisionedTopology


def _plan() -> RunPlan:
    return RunPlan(
        scenario_id="feature-scenario",
        pack_id="tests.feature",
        pack_version="1",
        seed=123,
        topology="standalone",
        components=(),
        topology_provider=TopologyProviderIdentity(
            provider_id="tests.topology-provider",
            implementation_version="1",
            api_version=EXTENSION_API_VERSION,
        ),
        mode=RunMode.TOPOLOGY_SMOKE,
        topology_document={"type": "standalone", "base_port": 7400},
    )


def _outcome(status: OutcomeStatus = OutcomeStatus.PASS) -> RunOutcome:
    return RunOutcome(
        plan=_plan(),
        status=status,
        summary=f"{status.value.lower()} result",
        started_at=10.0,
        ended_at=12.5,
    )


def _run_args(path, **overrides):
    values = {
        "file": str(path),
        "valkey_binary": None,
        "feature_pack_dirs": [],
        "no_plugins": False,
        "artifact_dir": "",
        "timeout": None,
        "allow_unsupported": False,
        "output": None,
        "format": "json",
        "verbose": False,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _validate_args(path, **overrides):
    values = {
        "file": str(path),
        "valkey_binary": None,
        "feature_pack_dirs": [],
        "no_plugins": False,
        "artifact_dir": "",
        "verbose": False,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _scenario_text() -> str:
    return """
scenario_id: feature-scenario
seed: 123
feature:
  use: tests.feature
  version: "1"
  with: {}
topology:
  type: standalone
"""


def test_cli_exposes_no_legacy_engine_or_version_router():
    assert not hasattr(cli_module, "ClusterBusFuzzer")
    assert not hasattr(cli_module, "DSLLoader")
    assert not hasattr(cli_module, "ScenarioGenerator")
    assert not hasattr(cli_module, "ScenarioRoutingError")
    assert not hasattr(FuzzerCLI, "run_random_test")
    assert not hasattr(FuzzerCLI, "run_dsl_test")


@pytest.mark.parametrize(
    ("status", "allow_unsupported", "expected_exit"),
    [
        (OutcomeStatus.PASS, False, EXIT_SUCCESS),
        (OutcomeStatus.FAIL, False, EXIT_FEATURE_FAILURE),
        (OutcomeStatus.ERROR, False, EXIT_HARNESS_ERROR),
        (OutcomeStatus.SKIP_UNSUPPORTED, False, EXIT_UNSUPPORTED),
        (OutcomeStatus.SKIP_UNSUPPORTED, True, EXIT_SUCCESS),
    ],
)
@patch("src.cli.FeatureRuntime")
def test_run_uses_one_generic_compile_and_run_path(
    runtime_class,
    status,
    allow_unsupported,
    expected_exit,
    tmp_path,
):
    scenario = tmp_path / "scenario.yaml"
    source = _scenario_text()
    scenario.write_text(source, encoding="utf-8")
    outcome = _outcome(status)
    runtime = runtime_class.return_value
    runtime.compile_yaml.return_value = outcome.plan
    runtime.run_plan.return_value = outcome

    result = FuzzerCLI().run_scenario(
        _run_args(
            scenario,
            valkey_binary="/tmp/valkey-server",
            feature_pack_dirs=["packs-a", "packs-b"],
            no_plugins=True,
            artifact_dir="/tmp/artifacts",
            allow_unsupported=allow_unsupported,
        )
    )

    assert result == expected_exit
    runtime_class.assert_called_once_with(
        feature_pack_directories=("packs-a", "packs-b"),
        discover_entry_points=False,
        artifact_dir="/tmp/artifacts",
    )
    runtime.compile_yaml.assert_called_once_with(
        source,
        source_name=str(scenario),
        topology_overrides={"valkey_binary": "/tmp/valkey-server"},
    )
    runtime.compile.assert_not_called()
    runtime.run_plan.assert_called_once_with(outcome.plan, deadline=None)


@patch("src.cli.time.monotonic", return_value=100.0)
@patch("src.cli.FeatureRuntime")
def test_run_converts_relative_timeout_to_absolute_deadline(runtime_class, monotonic, tmp_path):
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(_scenario_text(), encoding="utf-8")
    outcome = _outcome()
    runtime = runtime_class.return_value
    runtime.compile_yaml.return_value = outcome.plan
    runtime.run_plan.return_value = outcome

    assert FuzzerCLI().run_scenario(_run_args(scenario, timeout=5.5)) == EXIT_SUCCESS
    runtime.run_plan.assert_called_once_with(outcome.plan, deadline=105.5)
    monotonic.assert_called_once_with()


@patch("src.cli.FeatureRuntime")
def test_run_compile_error_is_a_harness_error_and_never_executes(runtime_class, tmp_path, capsys):
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(_scenario_text(), encoding="utf-8")
    runtime = runtime_class.return_value
    runtime.compile_yaml.side_effect = ValueError("unknown scenario field")

    assert FuzzerCLI().run_scenario(_run_args(scenario)) == EXIT_HARNESS_ERROR
    runtime.run_plan.assert_not_called()
    assert "unknown scenario field" in capsys.readouterr().out


@patch("src.cli.FeatureRuntime")
def test_missing_or_non_file_scenario_fails_before_runtime_construction(runtime_class, tmp_path):
    assert FuzzerCLI().run_scenario(_run_args(tmp_path / "missing.yaml")) == EXIT_HARNESS_ERROR
    assert FuzzerCLI().validate_scenario(_validate_args(tmp_path)) == EXIT_HARNESS_ERROR
    runtime_class.assert_not_called()


def test_duplicate_yaml_is_rejected_by_the_real_strict_loader(tmp_path, capsys):
    scenario = tmp_path / "duplicate.yaml"
    scenario.write_text(
        """
scenario_id: first
scenario_id: second
seed: 1
feature: {use: tests.feature, version: "1", with: {}}
topology: {type: standalone}
""",
        encoding="utf-8",
    )

    result = FuzzerCLI().run_scenario(_run_args(scenario, no_plugins=True))

    assert result == EXIT_HARNESS_ERROR
    assert "duplicate key" in capsys.readouterr().out


def test_api_version_is_an_unknown_field_not_a_routing_switch(tmp_path, capsys):
    scenario = tmp_path / "versioned.yaml"
    scenario.write_text(
        """
api_version: old
scenario_id: versioned
seed: 1
feature: {use: tests.feature}
topology: {type: standalone}
""",
        encoding="utf-8",
    )

    result = FuzzerCLI().run_scenario(_run_args(scenario, no_plugins=True))

    assert result == EXIT_HARNESS_ERROR
    assert "unknown fields: ['api_version']" in capsys.readouterr().out


@patch("src.cli.FeatureRuntime")
def test_validate_compiles_and_preflights_without_execution(runtime_class, tmp_path, capsys):
    scenario = tmp_path / "scenario.yaml"
    source = _scenario_text()
    scenario.write_text(source, encoding="utf-8")
    runtime = runtime_class.return_value
    runtime.compile_yaml.return_value = _plan()

    result = FuzzerCLI().validate_scenario(
        _validate_args(
            scenario,
            valkey_binary="/tmp/valkey-server",
            feature_pack_dirs=["packs"],
            no_plugins=True,
            artifact_dir="/tmp/artifacts",
            verbose=True,
        )
    )

    assert result == EXIT_SUCCESS
    runtime_class.assert_called_once_with(
        feature_pack_directories=("packs",),
        discover_entry_points=False,
        artifact_dir="/tmp/artifacts",
    )
    runtime.compile_yaml.assert_called_once_with(
        source,
        source_name=str(scenario),
        topology_overrides={"valkey_binary": "/tmp/valkey-server"},
    )
    runtime.validate_plan.assert_called_once_with(runtime.compile_yaml.return_value)
    runtime.run_plan.assert_not_called()
    output = capsys.readouterr().out
    assert "Static scenario preflight completed without provisioning" in output
    assert "bound environment values" in output
    assert "tests.feature@1" in output
    assert "Scenario API" not in output
    assert "Extension API" not in output


@patch("src.cli.FeatureRuntime")
def test_validate_error_is_a_harness_error(runtime_class, tmp_path):
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(_scenario_text(), encoding="utf-8")
    runtime_class.return_value.compile_yaml.side_effect = ValueError("invalid scenario")

    assert FuzzerCLI().validate_scenario(_validate_args(scenario)) == EXIT_HARNESS_ERROR
    runtime_class.return_value.validate_plan.assert_not_called()


@pytest.mark.parametrize("output_format", ["json", "yaml"])
def test_report_round_trips_the_complete_outcome_without_dsl_version(output_format, tmp_path):
    outcome = _outcome(OutcomeStatus.SKIP_UNSUPPORTED)
    output = tmp_path / f"result.{output_format}"

    FuzzerCLI()._save_outcomes((outcome,), str(output), output_format)

    with output.open(encoding="utf-8") as report_file:
        report = json.load(report_file) if output_format == "json" else yaml.safe_load(report_file)
    assert "report_api_version" not in report
    assert report["generated_at"]
    assert report["summary"] == {
        "total": 1,
        "passed": 0,
        "failed": 0,
        "errors": 0,
        "skipped_unsupported": 1,
    }
    assert RunOutcome.from_data(report["results"][0]) == outcome


@patch("src.cli.FeatureRuntime")
def test_report_write_failure_is_a_harness_error_and_removes_temporary_file(runtime_class, tmp_path):
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(_scenario_text(), encoding="utf-8")
    outcome = _outcome()
    runtime = runtime_class.return_value
    runtime.compile_yaml.return_value = outcome.plan
    runtime.run_plan.return_value = outcome

    assert FuzzerCLI().run_scenario(_run_args(scenario, output=str(tmp_path))) == EXIT_HARNESS_ERROR
    assert not tuple(tmp_path.parent.glob(f".{tmp_path.name}.*.tmp"))


def test_detailed_reporting_includes_components_evidence_and_diagnostics(capsys):
    component = SimpleNamespace(
        instance_id="activate",
        kind=ComponentKind.ACTION,
        component_id="tests.action",
        depends_on=(),
        required=True,
    )
    plan = Mock(
        scenario_id="scenario",
        pack_id="tests.feature",
        pack_version="1",
        mode=RunMode.FEATURE,
        topology="standalone",
        components=(component,),
        startup_overlays=(),
        required_evidence=(),
    )
    result = ComponentResult(
        component_instance_id="activate",
        status=ComponentStatus.PASS,
        summary="activated",
        started_at=1.0,
        ended_at=2.0,
        evidence=(
            Evidence(
                evidence_id="tests.path-used",
                source="activate",
                phase="activation",
                observed_at=1.5,
                value=True,
            ),
        ),
        diagnostics={"attempt": 1},
    )
    outcome = Mock(
        plan=plan,
        status=OutcomeStatus.PASS,
        summary="passed",
        started_at=1.0,
        ended_at=2.0,
        component_results=(result,),
        all_evidence=result.evidence,
        diagnostics={"events": ("action.completed",)},
    )

    FuzzerCLI()._print_detailed_result(outcome)

    output = capsys.readouterr().out
    assert "[PASS] activate" in output
    assert "Evidence: 1" in output
    assert "attempt: 1" in output
    assert "action.completed" in output


def test_cli_executes_plugin_manifest_provider_and_oracle_end_to_end(tmp_path, monkeypatch):
    calls = []

    class PassingAction:
        def run(self, context, spec):
            calls.append("action.run")
            return ComponentResult(
                component_instance_id=spec.instance_id,
                status=ComponentStatus.PASS,
                summary="feature path activated",
                evidence=(
                    Evidence(
                        evidence_id="tests.path-used",
                        source=spec.instance_id,
                        phase="activation",
                        observed_at=1.0,
                        value={"used": True},
                    ),
                ),
            )

    class EvidenceOracle:
        def evaluate(self, context, spec, evidence):
            calls.append("oracle.evaluate")
            observed = any(item.evidence_id == "tests.path-used" for item in evidence)
            return ComponentResult(
                component_instance_id=spec.instance_id,
                status=ComponentStatus.PASS if observed else ComponentStatus.FAIL,
                summary="required path evidence observed" if observed else "path evidence missing",
            )

    class Provider:
        def validate(self, plan):
            calls.append("provider.validate")

        def provision(self, context):
            calls.append("provider.provision")
            return ProvisionedTopology()

        def rollback(self, context):
            calls.append("provider.rollback")

        def cleanup(self, context, topology):
            calls.append("provider.cleanup")

    plugin = PluginManifest(
        plugin_id="tests.cli-plugin",
        api_version=EXTENSION_API_VERSION,
        registrations=(
            ComponentRegistration[Any](
                component_id="tests.action",
                kind=ComponentKind.ACTION,
                api_version=EXTENSION_API_VERSION,
                factory=PassingAction,
            ),
            ComponentRegistration[Any](
                component_id="tests.oracle",
                kind=ComponentKind.ORACLE,
                api_version=EXTENSION_API_VERSION,
                factory=EvidenceOracle,
            ),
        ),
    )

    class EntryPoint:
        name = "tests.cli-plugin"
        value = "tests.cli:plugin"
        dist = SimpleNamespace(name="test-plugin-distribution")

        @staticmethod
        def load():
            return plugin

    class EntryPoints:
        @staticmethod
        def select(*, group):
            assert group == "valkey_fuzzer.plugins"
            return (EntryPoint(),)

    monkeypatch.setattr("src.extensibility.importlib_metadata.entry_points", EntryPoints)
    monkeypatch.setattr(
        "src.feature_runtime.local_topology_provider_registrations",
        lambda coordinator_factory, *, api_version: {
            "standalone": TopologyProviderRegistration(
                topology="standalone",
                provider_id="tests.cli-topology",
                implementation_version="1",
                api_version=api_version,
                factory=Provider,
            )
        },
    )

    pack_dir = tmp_path / "packs"
    pack_dir.mkdir()
    (pack_dir / "feature.yaml").write_text(
        """
pack_id: tests.cli-feature
version: "1"
mode: feature
supported_topologies: [standalone]
components:
  - kind: action
    component_id: tests.action
    instance_id: activate
  - kind: oracle
    component_id: tests.oracle
    instance_id: verify
    depends_on: [activate]
required_events: [action.completed, oracle.completed]
required_evidence:
  - evidence_id: tests.path-used
    source: activate
""",
        encoding="utf-8",
    )
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(
        """
scenario_id: plugin-feature
seed: 77
feature:
  use: tests.cli-feature
  version: "1"
  with: {}
topology:
  type: standalone
""",
        encoding="utf-8",
    )
    report_path = tmp_path / "result.json"

    result = FuzzerCLI().run_scenario(
        _run_args(
            scenario,
            feature_pack_dirs=[str(pack_dir)],
            output=str(report_path),
        )
    )

    assert result == EXIT_SUCCESS
    assert calls == [
        "provider.validate",
        "provider.provision",
        "action.run",
        "oracle.evaluate",
        "provider.cleanup",
    ]
    report = json.loads(report_path.read_text(encoding="utf-8"))
    outcome = RunOutcome.from_data(report["results"][0])
    assert outcome.status is OutcomeStatus.PASS
    assert outcome.plan.pack_id == "tests.cli-feature"
    assert [item.component_instance_id for item in outcome.component_results] == ["activate", "verify"]
    assert outcome.all_evidence[0].evidence_id == "tests.path-used"


class TestParser:
    def test_public_commands_are_only_run_validate_and_cleanup(self):
        parser = create_parser()
        assert parser.parse_args(["run", "scenario.yaml"]).command == "run"
        assert parser.parse_args(["validate", "scenario.yaml"]).command == "validate"
        assert parser.parse_args(["cleanup-logs", "42", "--force"]).command == "cleanup-logs"
        with pytest.raises(SystemExit):
            parser.parse_args(["cluster", "--random"])

    def test_run_runtime_and_report_options(self):
        args = create_parser().parse_args(
            [
                "run",
                "scenario.yaml",
                "--feature-pack-dir",
                "packs-a",
                "--feature-pack-dir",
                "packs-b",
                "--no-plugins",
                "--valkey-binary",
                "/tmp/valkey-server",
                "--artifact-dir",
                "artifacts",
                "--timeout",
                "12.5",
                "--allow-unsupported",
                "--output",
                "result.yaml",
                "--format",
                "yaml",
                "--verbose",
            ]
        )

        assert args.feature_pack_dirs == ["packs-a", "packs-b"]
        assert args.no_plugins is True
        assert args.valkey_binary == "/tmp/valkey-server"
        assert args.artifact_dir == "artifacts"
        assert args.timeout == 12.5
        assert args.allow_unsupported is True
        assert args.output == "result.yaml"
        assert args.format == "yaml"
        assert args.verbose is True

    def test_validate_accepts_the_same_environment_inputs(self):
        args = create_parser().parse_args(
            [
                "validate",
                "scenario.yaml",
                "--feature-pack-dir",
                "packs",
                "--no-plugins",
                "--valkey-binary",
                "/tmp/valkey-server",
                "--artifact-dir",
                "artifacts",
            ]
        )

        assert args.feature_pack_dirs == ["packs"]
        assert args.no_plugins is True
        assert args.valkey_binary == "/tmp/valkey-server"
        assert args.artifact_dir == "artifacts"

    @pytest.mark.parametrize("value", ["0", "-1", "nan", "inf"])
    def test_timeout_must_be_positive_and_finite(self, value):
        with pytest.raises(SystemExit):
            create_parser().parse_args(["run", "scenario.yaml", "--timeout", value])

    def test_help_describes_the_generic_model_only(self):
        help_text = create_parser().format_help()
        assert "standalone, replication, and cluster" in help_text
        assert "feature scenario" in help_text
        assert "legacy" not in help_text.lower()
        assert "cluster --" not in help_text


class TestMain:
    @patch("src.cli.FuzzerCLI")
    def test_run_dispatch(self, cli_class):
        cli_class.return_value.run_scenario.return_value = EXIT_SUCCESS
        with patch("sys.argv", ["cli", "run", "scenario.yaml"]):
            assert main() == EXIT_SUCCESS
        cli_class.return_value.run_scenario.assert_called_once()

    @patch("src.cli.FuzzerCLI")
    def test_validate_dispatch(self, cli_class):
        cli_class.return_value.validate_scenario.return_value = EXIT_SUCCESS
        with patch("sys.argv", ["cli", "validate", "scenario.yaml"]):
            assert main() == EXIT_SUCCESS
        cli_class.return_value.validate_scenario.assert_called_once()

    @patch("src.cli.cleanup_logs", return_value=EXIT_SUCCESS)
    def test_cleanup_dispatch(self, cleanup):
        with patch("sys.argv", ["cli", "cleanup-logs", "42", "--data-dir", "/tmp/test", "--force"]):
            assert main() == EXIT_SUCCESS
        cleanup.assert_called_once_with(42, "/tmp/test", True)

    def test_no_command_prints_help_and_fails(self):
        with patch("sys.argv", ["cli"]):
            assert main() == EXIT_FEATURE_FAILURE

    @patch("src.cli.FuzzerCLI")
    def test_keyboard_interrupt_maps_to_130(self, cli_class):
        cli_class.return_value.run_scenario.side_effect = KeyboardInterrupt()
        with patch("sys.argv", ["cli", "run", "scenario.yaml"]):
            assert main() == 130

    @patch("src.cli.FuzzerCLI")
    def test_unexpected_dispatch_error_is_a_harness_error(self, cli_class):
        cli_class.return_value.run_scenario.side_effect = RuntimeError("boom")
        with patch("sys.argv", ["cli", "run", "scenario.yaml"]):
            assert main() == EXIT_HARNESS_ERROR
