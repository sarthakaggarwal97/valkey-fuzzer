"""
Tests for CLI functionality
"""

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock, patch

import pytest
import yaml

from src.cli import FuzzerCLI, create_parser, main
from src.extensibility import (
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
)
from src.feature_runner import ProvisionedTopology
from src.models import ChaosResult, ClusterConfig, ExecutionResult, TopologyType


@pytest.fixture
def mock_result():
    """Create a mock ExecutionResult"""
    return ExecutionResult(
        scenario_id="test-scenario-123",
        success=True,
        start_time=1000.0,
        end_time=1015.5,
        operations_executed=3,
        chaos_events=[
            ChaosResult(
                chaos_id="chaos-1",
                chaos_type=Mock(value="process_kill"),
                target_node="node-1",
                success=True,
                start_time=1005.0,
                end_time=1006.0,
            )
        ],
        seed=42,
    )


def feature_outcome(status: OutcomeStatus = OutcomeStatus.PASS) -> RunOutcome:
    plan = RunPlan(
        scenario_id="feature-scenario",
        pack_id="tests.feature",
        pack_version="1",
        api_version="v1",
        seed=123,
        topology="standalone",
        components=(),
        mode=RunMode.TOPOLOGY_SMOKE,
        topology_document={"type": "standalone", "base_port": 7400},
        scenario_api_version="v3",
    )
    return RunOutcome(
        plan=plan,
        status=status,
        summary=f"{status.value.lower()} result",
        started_at=10.0,
        ended_at=12.5,
    )


class TestFuzzerCLI:
    """Test FuzzerCLI class"""

    def test_load_yaml_config(self, tmp_path):
        """Test loading YAML configuration file"""
        config_file = tmp_path / "config.yaml"
        config_data = {"cluster": {"num_shards": 3}, "validation": {"check_slot_coverage": True}}

        with open(config_file, "w") as f:
            yaml.dump(config_data, f)

        cli = FuzzerCLI()
        loaded = cli.load_config_file(str(config_file))

        assert loaded == config_data

    def test_load_json_config(self, tmp_path):
        """Test loading JSON configuration file"""
        config_file = tmp_path / "config.json"
        config_data = {"cluster": {"num_shards": 3}, "validation": {"check_slot_coverage": True}}

        with open(config_file, "w") as f:
            json.dump(config_data, f)

        cli = FuzzerCLI()
        loaded = cli.load_config_file(str(config_file))

        assert loaded == config_data

    def test_load_config_file_not_found(self):
        """Test loading non-existent config file"""
        cli = FuzzerCLI()

        with pytest.raises(FileNotFoundError):
            cli.load_config_file("nonexistent.yaml")

    def test_load_config_unsupported_format(self, tmp_path):
        """Test loading unsupported config format"""
        config_file = tmp_path / "config.txt"
        config_file.write_text("some text")

        cli = FuzzerCLI()

        with pytest.raises(ValueError, match="Unsupported config format"):
            cli.load_config_file(str(config_file))

    @patch("src.cli.ClusterBusFuzzer")
    def test_run_random_test_success(self, mock_fuzzer_class, mock_result, capsys):
        """Test running random test successfully"""
        mock_fuzzer = Mock()
        mock_fuzzer.run_random_test.return_value = mock_result
        mock_fuzzer.last_scenario = Mock()
        mock_fuzzer_class.return_value = mock_fuzzer

        cli = FuzzerCLI()
        args = Mock(seed=42, iterations=1, config=None, valkey_binary=None, output=None, verbose=False, export_dsl=None)

        result = cli.run_random_test(args)

        assert result == 0
        mock_fuzzer.run_random_test.assert_called_once_with(seed=42)

        captured = capsys.readouterr()
        assert "PASSED" in captured.out
        assert "test-scenario-123" in captured.out

    @patch("src.cli.ClusterBusFuzzer")
    def test_run_random_test_with_config(self, mock_fuzzer_class, mock_result, tmp_path):
        """Test running random test with config file"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("cluster:\n  num_shards: 3\n")

        mock_fuzzer = Mock()
        mock_fuzzer.run_random_test.return_value = mock_result
        mock_fuzzer.last_scenario = Mock()
        mock_fuzzer_class.return_value = mock_fuzzer

        cli = FuzzerCLI()
        args = Mock(
            seed=None,
            iterations=1,
            config=str(config_file),
            valkey_binary=None,
            output=None,
            verbose=False,
            export_dsl=None,
        )

        result = cli.run_random_test(args)

        assert result == 0
        assert cli.config == {"cluster": {"num_shards": 3}}

    @patch("src.cli.ClusterBusFuzzer")
    def test_run_random_test_multiple_iterations(self, mock_fuzzer_class, mock_result):
        """Test running multiple test iterations"""
        mock_fuzzer = Mock()
        mock_fuzzer.run_random_test.return_value = mock_result
        mock_fuzzer.last_scenario = Mock()
        mock_fuzzer_class.return_value = mock_fuzzer

        cli = FuzzerCLI()
        args = Mock(seed=42, iterations=3, config=None, valkey_binary=None, output=None, verbose=False, export_dsl=None)

        result = cli.run_random_test(args)

        assert result == 0
        assert mock_fuzzer.run_random_test.call_count == 3

    @patch("src.cli.ClusterBusFuzzer")
    def test_run_random_test_with_output(self, mock_fuzzer_class, mock_result, tmp_path):
        """Test saving test results to file"""
        output_file = tmp_path / "results.json"

        mock_fuzzer = Mock()
        mock_fuzzer.run_random_test.return_value = mock_result
        mock_fuzzer.last_scenario = Mock()
        mock_fuzzer_class.return_value = mock_fuzzer

        cli = FuzzerCLI()
        args = Mock(
            seed=42,
            iterations=1,
            config=None,
            valkey_binary=None,
            output=str(output_file),
            format="json",
            verbose=False,
            export_dsl=None,
        )

        result = cli.run_random_test(args)

        assert result == 0
        assert output_file.exists()

        with open(output_file) as f:
            data = json.load(f)

        assert data["total_tests"] == 1
        assert data["passed"] == 1
        assert len(data["results"]) == 1

    @patch("src.cli.DSLLoader.save_scenario_as_dsl")
    @patch("src.cli.ClusterBusFuzzer")
    def test_run_random_test_with_export_dsl(self, mock_fuzzer_class, mock_save_dsl, mock_result, tmp_path):
        """Test exporting random scenario to DSL file"""
        export_file = tmp_path / "scenario.yaml"

        mock_scenario = Mock()
        mock_fuzzer = Mock()
        mock_fuzzer.run_random_test.return_value = mock_result
        mock_fuzzer.last_scenario = mock_scenario
        mock_fuzzer_class.return_value = mock_fuzzer

        cli = FuzzerCLI()
        args = Mock(
            seed=42,
            iterations=1,
            config=None,
            valkey_binary=None,
            output=None,
            verbose=False,
            export_dsl=str(export_file),
        )

        result = cli.run_random_test(args)

        assert result == 0
        mock_save_dsl.assert_called_once_with(mock_scenario, export_file)

    @patch("src.cli.ClusterBusFuzzer")
    @patch("src.cli.DSLLoader")
    def test_run_dsl_test_success(self, mock_loader, mock_fuzzer_class, mock_result, tmp_path):
        """Test running DSL-based test"""
        dsl_file = tmp_path / "test.yaml"
        dsl_file.write_text("scenario:\n  id: test\n")

        mock_dsl_config = Mock()
        mock_loader.load_from_string.return_value = mock_dsl_config

        mock_fuzzer = Mock()
        mock_fuzzer.run_dsl_test.return_value = mock_result
        mock_fuzzer_class.return_value = mock_fuzzer

        cli = FuzzerCLI()
        args = Mock(file=str(dsl_file), valkey_binary=None, output=None, verbose=False)

        result = cli.run_dsl_test(args)

        assert result == 0
        mock_loader.load_from_string.assert_called_once_with(dsl_file.read_text())
        mock_fuzzer.run_dsl_test.assert_called_once_with(mock_dsl_config)

    @patch("src.cli.ClusterBusFuzzer")
    def test_run_random_test_passes_valkey_binary(self, mock_fuzzer_class, mock_result):
        """Test running random test with explicit Valkey binary override"""
        mock_fuzzer = Mock()
        mock_fuzzer.run_random_test.return_value = mock_result
        mock_fuzzer.last_scenario = Mock()
        mock_fuzzer_class.return_value = mock_fuzzer

        cli = FuzzerCLI()
        args = Mock(
            seed=42,
            iterations=1,
            config=None,
            valkey_binary="/tmp/valkey/src/valkey-server",
            output=None,
            verbose=False,
            export_dsl=None,
        )

        result = cli.run_random_test(args)

        assert result == 0
        mock_fuzzer.run_random_test.assert_called_once_with(seed=42, valkey_binary="/tmp/valkey/src/valkey-server")

    @patch("src.cli.ClusterBusFuzzer")
    @patch("src.cli.DSLLoader")
    def test_run_dsl_test_passes_valkey_binary(self, mock_loader, mock_fuzzer_class, mock_result, tmp_path):
        """Test running DSL test with explicit Valkey binary override"""
        dsl_file = tmp_path / "test.yaml"
        dsl_file.write_text("scenario:\n  id: test\n")

        mock_dsl_config = Mock()
        mock_loader.load_from_string.return_value = mock_dsl_config

        mock_fuzzer = Mock()
        mock_fuzzer.run_dsl_test.return_value = mock_result
        mock_fuzzer_class.return_value = mock_fuzzer

        cli = FuzzerCLI()
        args = Mock(file=str(dsl_file), valkey_binary="/tmp/valkey/src/valkey-server", output=None, verbose=False)

        result = cli.run_dsl_test(args)

        assert result == 0
        mock_fuzzer.run_dsl_test.assert_called_once_with(mock_dsl_config, valkey_binary="/tmp/valkey/src/valkey-server")

    @pytest.mark.parametrize(
        ("status", "allow_unsupported", "expected_exit"),
        [
            (OutcomeStatus.PASS, False, 0),
            (OutcomeStatus.FAIL, False, 1),
            (OutcomeStatus.ERROR, False, 2),
            (OutcomeStatus.SKIP_UNSUPPORTED, False, 3),
            (OutcomeStatus.SKIP_UNSUPPORTED, True, 0),
        ],
    )
    @patch("src.cli.FeatureRuntime")
    @patch("src.cli.ClusterBusFuzzer")
    def test_run_v3_routes_to_generic_runtime_and_maps_exit_status(
        self,
        mock_fuzzer_class,
        mock_runtime_class,
        status,
        allow_unsupported,
        expected_exit,
        tmp_path,
    ):
        dsl_file = tmp_path / "feature.yaml"
        dsl_file.write_text(
            """
api_version: v3
scenario_id: feature-scenario
seed: 123
feature:
  use: tests.feature
  version: "1"
  with: {}
topology:
  type: standalone
"""
        )
        outcome = feature_outcome(status)
        runtime = mock_runtime_class.return_value
        runtime.compile.return_value = outcome.plan
        runtime.run_plan.return_value = outcome
        args = SimpleNamespace(
            file=str(dsl_file),
            valkey_binary="/tmp/valkey-server",
            feature_pack_dirs=["packs-a", "packs-b"],
            no_plugins=True,
            artifact_dir="/tmp/artifacts",
            timeout=None,
            allow_unsupported=allow_unsupported,
            output=None,
            format="json",
            verbose=False,
        )

        result = FuzzerCLI().run_dsl_test(args)

        assert result == expected_exit
        mock_runtime_class.assert_called_once_with(
            feature_pack_directories=("packs-a", "packs-b"),
            discover_entry_points=False,
            artifact_dir="/tmp/artifacts",
        )
        compiled_document = runtime.compile.call_args.args[0]
        assert compiled_document["api_version"] == "v3"
        runtime.compile.assert_called_once_with(
            compiled_document,
            topology_overrides={"valkey_binary": "/tmp/valkey-server"},
        )
        runtime.run_plan.assert_called_once_with(outcome.plan, deadline=None)
        mock_fuzzer_class.assert_not_called()

    @patch("src.cli.time.monotonic", return_value=100.0)
    @patch("src.cli.FeatureRuntime")
    def test_run_v3_converts_relative_timeout_to_absolute_deadline(
        self,
        mock_runtime_class,
        mock_monotonic,
        tmp_path,
    ):
        dsl_file = tmp_path / "feature.yaml"
        dsl_file.write_text(
            "api_version: v3\nscenario_id: feature-scenario\nseed: 1\n"
            "feature: {use: tests.feature, version: '1', with: {}}\n"
            "topology: {type: standalone}\n"
        )
        outcome = feature_outcome()
        runtime = mock_runtime_class.return_value
        runtime.compile.return_value = outcome.plan
        runtime.run_plan.return_value = outcome
        args = SimpleNamespace(
            file=str(dsl_file),
            valkey_binary=None,
            feature_pack_dirs=[],
            no_plugins=False,
            artifact_dir="",
            timeout=5.5,
            allow_unsupported=False,
            output=None,
            format="json",
            verbose=False,
        )

        assert FuzzerCLI().run_dsl_test(args) == 0
        runtime.run_plan.assert_called_once_with(outcome.plan, deadline=105.5)
        mock_monotonic.assert_called_once_with()

    @patch("src.cli.ClusterBusFuzzer")
    def test_v3_cli_executes_discovered_plugin_and_directory_manifest_end_to_end(
        self,
        mock_fuzzer_class,
        tmp_path,
        monkeypatch,
    ):
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
            api_version="v1",
            registrations=(
                ComponentRegistration[Any](
                    component_id="tests.action",
                    kind=ComponentKind.ACTION,
                    api_version="v1",
                    factory=PassingAction,
                ),
                ComponentRegistration[Any](
                    component_id="tests.oracle",
                    kind=ComponentKind.ORACLE,
                    api_version="v1",
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

        monkeypatch.setattr(
            "src.extensibility.importlib_metadata.entry_points",
            EntryPoints,
        )
        monkeypatch.setattr(
            "src.feature_runtime.local_topology_provider_factories",
            lambda coordinator_factory: {"standalone": Provider},
        )

        pack_dir = tmp_path / "packs"
        pack_dir.mkdir()
        (pack_dir / "feature.yaml").write_text(
            """
api_version: v1
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
api_version: v3
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
        args = SimpleNamespace(
            file=str(scenario),
            valkey_binary=None,
            feature_pack_dirs=[str(pack_dir)],
            no_plugins=False,
            artifact_dir="",
            timeout=None,
            allow_unsupported=False,
            output=str(report_path),
            format="json",
            verbose=False,
        )

        assert FuzzerCLI().run_dsl_test(args) == 0
        assert calls == [
            "provider.validate",
            "provider.provision",
            "action.run",
            "oracle.evaluate",
            "provider.cleanup",
        ]
        mock_fuzzer_class.assert_not_called()

        report = json.loads(report_path.read_text(encoding="utf-8"))
        outcome = RunOutcome.from_data(report["results"][0])
        assert outcome.status is OutcomeStatus.PASS
        assert outcome.plan.pack_id == "tests.cli-feature"
        assert [result.component_instance_id for result in outcome.component_results] == [
            "activate",
            "verify",
        ]
        assert outcome.all_evidence[0].evidence_id == "tests.path-used"
        assert outcome.diagnostics["committed_events"]

    @patch("src.cli.FeatureRuntime")
    @patch("src.cli.ClusterBusFuzzer")
    def test_duplicate_v3_yaml_fails_before_runtime_or_legacy_execution(
        self,
        mock_fuzzer_class,
        mock_runtime_class,
        tmp_path,
    ):
        dsl_file = tmp_path / "duplicate.yaml"
        dsl_file.write_text(
            "api_version: v3\napi_version: v3\nscenario_id: duplicate\n"
            "seed: 1\nfeature: {use: tests.feature, version: '1', with: {}}\n"
            "topology: {type: standalone}\n"
        )
        args = SimpleNamespace(file=str(dsl_file), verbose=False)

        assert FuzzerCLI().run_dsl_test(args) == 2
        mock_runtime_class.assert_not_called()
        mock_fuzzer_class.assert_not_called()

    @pytest.mark.parametrize(
        "versions",
        [
            ("v4", "v4"),
            ("v2", "v4"),
            ("v4", "v2"),
        ],
    )
    @patch("src.cli.FeatureRuntime")
    @patch("src.cli.ClusterBusFuzzer")
    def test_duplicate_nonlegacy_api_versions_are_unsafe_routing_errors(
        self,
        mock_fuzzer_class,
        mock_runtime_class,
        versions,
        tmp_path,
    ):
        dsl_file = tmp_path / "duplicate-unknown.yaml"
        dsl_file.write_text(
            f"api_version: {versions[0]}\napi_version: {versions[1]}\n"
            "scenario_id: duplicate\nseed: 1\n"
            "topology: {type: standalone}\noperations: []\n"
        )

        assert FuzzerCLI().run_dsl_test(SimpleNamespace(file=str(dsl_file), verbose=False)) == 2
        mock_runtime_class.assert_not_called()
        mock_fuzzer_class.assert_not_called()

    @pytest.mark.parametrize(
        "api_declaration",
        [
            "api_version: [v3]\n",
            "api_version: {major: 3}\n",
            "api_version: v3\napi_version: [v2]\n",
        ],
    )
    @patch("src.cli.FeatureRuntime")
    @patch("src.cli.ClusterBusFuzzer")
    def test_non_scalar_api_versions_are_unsafe_routing_errors(
        self,
        mock_fuzzer_class,
        mock_runtime_class,
        api_declaration,
        tmp_path,
    ):
        dsl_file = tmp_path / "invalid-api.yaml"
        dsl_file.write_text(
            api_declaration + "scenario_id: invalid\nseed: 1\ntopology: {type: standalone}\noperations: []\n"
        )

        assert FuzzerCLI().run_dsl_test(SimpleNamespace(file=str(dsl_file), verbose=False)) == 2
        mock_runtime_class.assert_not_called()
        mock_fuzzer_class.assert_not_called()

    @patch("src.cli.FeatureRuntime")
    @patch("src.cli.ClusterBusFuzzer")
    @patch("src.cli.DSLLoader")
    def test_legacy_scenario_never_bootstraps_plugins(
        self,
        mock_loader,
        mock_fuzzer_class,
        mock_runtime_class,
        mock_result,
        tmp_path,
    ):
        dsl_file = tmp_path / "legacy.yaml"
        dsl_file.write_text(
            "api_version: v2\nscenario_id: legacy\nseed: 1\ntopology: {type: standalone}\noperations: []\n"
        )
        mock_loader.load_from_string.return_value = Mock()
        mock_fuzzer_class.return_value.run_dsl_test.return_value = mock_result
        args = SimpleNamespace(
            file=str(dsl_file),
            valkey_binary=None,
            output=None,
            verbose=False,
        )

        assert FuzzerCLI().run_dsl_test(args) == 0
        mock_runtime_class.assert_not_called()

    @patch("src.cli.FeatureRuntime")
    @patch("src.cli.ClusterBusFuzzer")
    @patch("src.cli.DSLLoader")
    def test_legacy_yaml_merge_override_preserves_previous_loading_semantics(
        self,
        mock_loader,
        mock_fuzzer_class,
        mock_runtime_class,
        mock_result,
        tmp_path,
    ):
        dsl_file = tmp_path / "legacy-merge.yaml"
        source = """
api_version: v2
scenario_id: legacy-merge
seed: 1
topology:
  <<: &defaults
    type: replication
    replicas_per_shard: 1
  type: standalone
operations: []
"""
        dsl_file.write_text(source)
        mock_loader.load_from_string.return_value = Mock()
        mock_fuzzer_class.return_value.run_dsl_test.return_value = mock_result
        args = SimpleNamespace(
            file=str(dsl_file),
            valkey_binary=None,
            output=None,
            verbose=False,
        )

        assert FuzzerCLI().run_dsl_test(args) == 0
        mock_loader.load_from_string.assert_called_once_with(source)
        mock_runtime_class.assert_not_called()

    @pytest.mark.parametrize(
        "versions",
        [
            ("v2", "v2"),
            ("v1", "v2"),
        ],
    )
    @patch("src.cli.FeatureRuntime")
    @patch("src.cli.ClusterBusFuzzer")
    @patch("src.cli.DSLLoader")
    def test_duplicate_legacy_api_versions_preserve_last_key_wins_loading(
        self,
        mock_loader,
        mock_fuzzer_class,
        mock_runtime_class,
        versions,
        mock_result,
        tmp_path,
    ):
        source = (
            f"api_version: {versions[0]}\n"
            f"api_version: {versions[1]}\n"
            "scenario_id: legacy\nseed: 1\n"
            "topology: {type: standalone}\noperations: []\n"
        )
        dsl_file = tmp_path / "duplicate-legacy-version.yaml"
        dsl_file.write_text(source)
        mock_loader.load_from_string.return_value = Mock()
        mock_fuzzer_class.return_value.run_dsl_test.return_value = mock_result
        args = SimpleNamespace(
            file=str(dsl_file),
            valkey_binary=None,
            output=None,
            verbose=False,
        )

        assert FuzzerCLI().run_dsl_test(args) == 0
        mock_loader.load_from_string.assert_called_once_with(source)
        mock_runtime_class.assert_not_called()

    @patch("src.cli.ClusterBusFuzzer")
    def test_malformed_legacy_run_preserves_failure_exit_code(
        self,
        mock_fuzzer_class,
        tmp_path,
    ):
        dsl_file = tmp_path / "broken-v2.yaml"
        dsl_file.write_text("api_version: v2\ntopology: [\n")

        assert FuzzerCLI().run_dsl_test(SimpleNamespace(file=str(dsl_file), verbose=False)) == 1
        mock_fuzzer_class.assert_not_called()

    @pytest.mark.parametrize(
        ("option", "value"),
        [
            ("feature_pack_dirs", ["packs"]),
            ("no_plugins", True),
            ("artifact_dir", "artifacts"),
            ("timeout", 1.0),
            ("allow_unsupported", True),
        ],
    )
    @patch("src.cli.FeatureRuntime")
    @patch("src.cli.ClusterBusFuzzer")
    def test_legacy_scenario_rejects_v3_only_run_options(
        self,
        mock_fuzzer_class,
        mock_runtime_class,
        option,
        value,
        tmp_path,
    ):
        dsl_file = tmp_path / "legacy.yaml"
        dsl_file.write_text(
            "api_version: v2\nscenario_id: legacy\nseed: 1\ntopology: {type: standalone}\noperations: []\n"
        )
        values = {
            "file": str(dsl_file),
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
        values[option] = value

        assert FuzzerCLI().run_dsl_test(SimpleNamespace(**values)) == 2
        mock_runtime_class.assert_not_called()
        mock_fuzzer_class.assert_not_called()

    @patch("src.cli.ClusterBusFuzzer")
    def test_run_dsl_test_file_not_found(self, mock_fuzzer_class, capsys):
        """Test DSL test with non-existent file"""
        cli = FuzzerCLI()
        args = Mock(file="nonexistent.yaml", output=None, verbose=False)

        result = cli.run_dsl_test(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "not found" in captured.out

    @patch("src.cli.ScenarioGenerator")
    @patch("src.cli.DSLLoader")
    def test_validate_dsl_success(self, mock_loader, mock_generator_class, tmp_path, capsys):
        """Test validating DSL configuration"""
        dsl_file = tmp_path / "test.yaml"
        dsl_file.write_text(
            "scenario_id: test\ncluster:\n  num_shards: 3\n  replicas_per_shard: 1\noperations:\n  - type: failover\n    target_node: node-0\n"
        )

        mock_dsl_config = Mock(config_text="test config")
        mock_loader.load_from_string.return_value = mock_dsl_config

        mock_scenario = Mock(
            scenario_id="test-123",
            seed=42,
            cluster_config=Mock(num_shards=3, replicas_per_shard=1),
            operations=[Mock()],
            chaos_config=Mock(chaos_type=Mock(value="process_kill")),
        )

        mock_generator = Mock()
        mock_generator.parse_dsl_config.return_value = mock_scenario
        mock_generator.validate_scenario.return_value = None
        mock_generator_class.return_value = mock_generator

        cli = FuzzerCLI()
        args = Mock(file=str(dsl_file), verbose=False)

        result = cli.validate_dsl(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "valid" in captured.out.lower()

    @patch("src.cli.ScenarioGenerator")
    @patch("src.cli.DSLLoader")
    def test_validate_dsl_prints_replication_topology(self, mock_loader, mock_generator_class, tmp_path, capsys):
        """Test validation summary reports the topology type and node count."""
        dsl_file = tmp_path / "replication.yaml"
        dsl_file.write_text("api_version: v2\ntopology:\n  type: replication\n")

        mock_loader.load_from_string.return_value = Mock(config_text="test config")
        mock_scenario = Mock(
            scenario_id="replication-test",
            api_version="v2",
            seed=None,
            cluster_config=ClusterConfig(
                num_shards=1,
                replicas_per_shard=2,
                topology_type=TopologyType.REPLICATION,
            ),
            operations=[],
            chaos_config=Mock(chaos_type=Mock(value="process_kill")),
        )
        mock_generator = Mock()
        mock_generator.parse_dsl_config.return_value = mock_scenario
        mock_generator_class.return_value = mock_generator

        result = FuzzerCLI().validate_dsl(Mock(file=str(dsl_file), verbose=False))

        assert result == 0
        output = capsys.readouterr().out
        assert "API Version: v2" in output
        assert "Topology: replication (1 primary, 2 replicas, 3 nodes total)" in output

    @patch("src.cli.ScenarioGenerator")
    @patch("src.cli.DSLLoader")
    def test_validate_dsl_reports_chaos_disabled_without_operations(
        self, mock_loader, mock_generator_class, tmp_path, capsys
    ):
        """Test zero-operation scenarios do not imply that chaos will run."""
        dsl_file = tmp_path / "standalone.yaml"
        dsl_file.write_text("api_version: v2\ntopology:\n  type: standalone\n")
        mock_loader.load_from_string.return_value = Mock(config_text="test config")
        mock_generator = Mock()
        mock_generator.parse_dsl_config.return_value = Mock(
            scenario_id="standalone-test",
            api_version="v2",
            seed=None,
            cluster_config=ClusterConfig(
                num_shards=1,
                replicas_per_shard=0,
                topology_type=TopologyType.STANDALONE,
            ),
            operations=[],
            chaos_config=Mock(chaos_type=Mock(value="process_kill")),
        )
        mock_generator_class.return_value = mock_generator

        result = FuzzerCLI().validate_dsl(Mock(file=str(dsl_file), verbose=False))

        assert result == 0
        assert "Chaos: disabled (no operations)" in capsys.readouterr().out

    @patch("src.cli.ClusterBusFuzzer")
    @patch("src.cli.FeatureRuntime")
    def test_validate_v3_compiles_and_preflights_without_execution(
        self,
        mock_runtime_class,
        mock_fuzzer_class,
        tmp_path,
        capsys,
    ):
        dsl_file = tmp_path / "feature.yaml"
        dsl_file.write_text(
            "api_version: v3\nscenario_id: feature-scenario\nseed: 1\n"
            "feature: {use: tests.feature, version: '1', with: {}}\n"
            "topology: {type: standalone}\n"
        )
        outcome = feature_outcome()
        runtime = mock_runtime_class.return_value
        runtime.compile.return_value = outcome.plan
        args = SimpleNamespace(
            file=str(dsl_file),
            valkey_binary="/tmp/valkey-server",
            feature_pack_dirs=["packs"],
            no_plugins=True,
            verbose=True,
        )

        result = FuzzerCLI().validate_dsl(args)

        assert result == 0
        runtime.compile.assert_called_once()
        assert runtime.compile.call_args.kwargs["topology_overrides"] == {"valkey_binary": "/tmp/valkey-server"}
        runtime.validate_plan.assert_called_once_with(outcome.plan)
        runtime.run_plan.assert_not_called()
        mock_fuzzer_class.assert_not_called()
        output = capsys.readouterr().out
        assert "Structural topology preflight completed without provisioning" in output
        assert "Component factories were not instantiated" in output
        assert "Valkey binary availability was not checked" in output
        assert "tests.feature@1" in output

    @pytest.mark.parametrize(
        ("option", "value"),
        [
            ("feature_pack_dirs", ["packs"]),
            ("no_plugins", True),
            ("valkey_binary", "/tmp/valkey-server"),
        ],
    )
    @patch("src.cli.FeatureRuntime")
    @patch("src.cli.DSLLoader")
    @patch("src.cli.ScenarioGenerator")
    def test_validate_legacy_rejects_v3_only_options(
        self,
        mock_generator_class,
        mock_loader,
        mock_runtime_class,
        option,
        value,
        tmp_path,
    ):
        dsl_file = tmp_path / "legacy.yaml"
        dsl_file.write_text(
            "api_version: v2\nscenario_id: legacy\nseed: 1\ntopology: {type: standalone}\noperations: []\n"
        )
        values = {
            "file": str(dsl_file),
            "valkey_binary": None,
            "feature_pack_dirs": [],
            "no_plugins": False,
            "verbose": False,
        }
        values[option] = value

        assert FuzzerCLI().validate_dsl(SimpleNamespace(**values)) == 2
        mock_runtime_class.assert_not_called()
        mock_loader.load_from_string.assert_not_called()
        mock_generator_class.assert_not_called()

    def test_malformed_legacy_validation_preserves_failure_exit_code(self, tmp_path):
        dsl_file = tmp_path / "broken-v2.yaml"
        dsl_file.write_text("api_version: v2\ntopology: [\n")

        assert FuzzerCLI().validate_dsl(SimpleNamespace(file=str(dsl_file), verbose=False)) == 1

    def test_format_topology_summary_singular_replica(self):
        """Test topology summaries use singular grammar for one replica."""
        config = ClusterConfig(
            num_shards=1,
            replicas_per_shard=1,
            topology_type=TopologyType.REPLICATION,
        )

        assert FuzzerCLI._format_topology_summary(config) == (
            "Topology: replication (1 primary, 1 replica, 2 nodes total)"
        )

    def test_result_to_dict(self, mock_result):
        """Test converting ExecutionResult to dictionary"""
        cli = FuzzerCLI()
        result_dict = cli._result_to_dict(mock_result)

        assert result_dict["scenario_id"] == "test-scenario-123"
        assert result_dict["success"] is True
        assert result_dict["duration"] == 15.5
        assert result_dict["operations_executed"] == 3
        assert result_dict["chaos_events"] == 1
        assert result_dict["seed"] == 42

    @pytest.mark.parametrize("format", ["json", "yaml"])
    def test_generic_report_round_trips_complete_outcome(self, format, tmp_path):
        outcome = feature_outcome(OutcomeStatus.SKIP_UNSUPPORTED)
        output_file = tmp_path / f"result.{format}"

        FuzzerCLI()._save_feature_outcomes((outcome,), str(output_file), format)

        with output_file.open(encoding="utf-8") as report_file:
            report = json.load(report_file) if format == "json" else yaml.safe_load(report_file)
        assert report["report_api_version"] == "v1"
        assert report["summary"] == {
            "total": 1,
            "passed": 0,
            "failed": 0,
            "errors": 0,
            "skipped_unsupported": 1,
        }
        assert RunOutcome.from_data(report["results"][0]) == outcome

    @patch("src.cli.FeatureRuntime")
    def test_v3_report_write_failure_returns_harness_error(
        self,
        mock_runtime_class,
        tmp_path,
    ):
        dsl_file = tmp_path / "feature.yaml"
        dsl_file.write_text(
            "api_version: v3\nscenario_id: feature-scenario\nseed: 1\n"
            "feature: {use: tests.feature, version: '1', with: {}}\n"
            "topology: {type: standalone}\n"
        )
        outcome = feature_outcome()
        runtime = mock_runtime_class.return_value
        runtime.compile.return_value = outcome.plan
        runtime.run_plan.return_value = outcome
        args = SimpleNamespace(
            file=str(dsl_file),
            valkey_binary=None,
            feature_pack_dirs=[],
            no_plugins=False,
            artifact_dir="",
            timeout=None,
            allow_unsupported=False,
            output=str(tmp_path),
            format="json",
            verbose=False,
        )

        assert FuzzerCLI().run_dsl_test(args) == 2
        assert not tuple(tmp_path.parent.glob(f".{tmp_path.name}.*.tmp"))


class TestCLIParser:
    """Test CLI argument parser"""

    def test_parse_commands(self):
        """Test parsing various CLI commands"""
        parser = create_parser()
        assert parser.prog == "valkey-fuzzer"

    def test_parse_random_command(self):
        """Test parsing random command"""
        parser = create_parser()
        args = parser.parse_args(["cluster", "--seed", "42"])

        assert args.command == "cluster"
        assert args.seed == 42

    def test_parse_random_with_iterations(self):
        """Test parsing random command with iterations"""
        parser = create_parser()
        args = parser.parse_args(["cluster", "--random", "--iterations", "5"])

        assert args.command == "cluster"
        assert args.iterations == 5

    def test_parse_random_with_config(self):
        """Test parsing random command with config"""
        parser = create_parser()
        args = parser.parse_args(["cluster", "--random", "--config", "config.yaml"])

        assert args.command == "cluster"
        assert args.config == "config.yaml"

    def test_parse_dsl_command(self):
        """Test parsing DSL command"""
        parser = create_parser()
        args = parser.parse_args(["cluster", "--dsl", "test.yaml"])

        assert args.command == "cluster"
        assert args.dsl == "test.yaml"

    def test_parse_generic_run_command(self):
        """Test parsing the topology-neutral DSL command."""
        parser = create_parser()
        args = parser.parse_args(
            [
                "run",
                "topology.yaml",
                "--valkey-binary",
                "/tmp/valkey-server",
                "--output",
                "results.yaml",
                "--format",
                "yaml",
                "--verbose",
            ]
        )

        assert args.command == "run"
        assert args.file == "topology.yaml"
        assert args.valkey_binary == "/tmp/valkey-server"
        assert args.output == "results.yaml"
        assert args.format == "yaml"
        assert args.verbose is True

    def test_parse_v3_runtime_options(self):
        parser = create_parser()
        args = parser.parse_args(
            [
                "run",
                "feature.yaml",
                "--feature-pack-dir",
                "packs-a",
                "--feature-pack-dir",
                "packs-b",
                "--no-plugins",
                "--artifact-dir",
                "artifacts",
                "--timeout",
                "12.5",
                "--allow-unsupported",
            ]
        )

        assert args.feature_pack_dirs == ["packs-a", "packs-b"]
        assert args.no_plugins is True
        assert args.artifact_dir == "artifacts"
        assert args.timeout == 12.5
        assert args.allow_unsupported is True

    def test_parse_validate_v3_runtime_options(self):
        parser = create_parser()
        args = parser.parse_args(
            [
                "validate",
                "feature.yaml",
                "--feature-pack-dir",
                "packs",
                "--no-plugins",
                "--valkey-binary",
                "/tmp/valkey-server",
            ]
        )

        assert args.feature_pack_dirs == ["packs"]
        assert args.no_plugins is True
        assert args.valkey_binary == "/tmp/valkey-server"

    @pytest.mark.parametrize("value", ["0", "-1", "nan", "inf"])
    def test_parse_rejects_invalid_v3_timeout(self, value):
        parser = create_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["run", "feature.yaml", "--timeout", value])

    def test_help_is_topology_neutral(self):
        """Test top-level help describes every supported topology."""
        help_text = create_parser().format_help()

        assert "Valkey Fuzzer" in help_text
        assert "standalone, replication, and cluster" in help_text
        assert "Cluster Bus Fuzzer" not in help_text

    def test_parse_valkey_binary_option(self):
        """Test parsing explicit Valkey binary option"""
        parser = create_parser()
        args = parser.parse_args(["cluster", "--random", "--valkey-binary", "/tmp/valkey-server"])

        assert args.command == "cluster"
        assert args.valkey_binary == "/tmp/valkey-server"

    def test_parse_output_options(self):
        """Test parsing output options"""
        parser = create_parser()
        args = parser.parse_args(["cluster", "--random", "--output", "results.json", "--format", "yaml"])

        assert args.command == "cluster"
        assert args.output == "results.json"
        assert args.format == "yaml"

    def test_parse_export_dsl_option(self):
        """Test parsing export-dsl option"""
        parser = create_parser()
        args = parser.parse_args(["cluster", "--random", "--export-dsl", "scenario.yaml"])

        assert args.command == "cluster"
        assert args.export_dsl == "scenario.yaml"


class TestCLIMain:
    """Test main CLI entry point"""

    @patch("src.cli.FuzzerCLI")
    def test_main_generic_run_command(self, mock_cli_class):
        """Test main dispatches the generic run command to DSL execution."""
        mock_cli = Mock()
        mock_cli.run_dsl_test.return_value = 0
        mock_cli_class.return_value = mock_cli

        with patch(
            "sys.argv",
            [
                "cli",
                "run",
                "replication.yaml",
                "--valkey-binary",
                "/tmp/valkey-server",
                "--output",
                "results.json",
            ],
        ):
            result = main()

        assert result == 0
        mock_cli.run_dsl_test.assert_called_once()
        run_args = mock_cli.run_dsl_test.call_args.args[0]
        assert run_args.file == "replication.yaml"
        assert run_args.valkey_binary == "/tmp/valkey-server"
        assert run_args.output == "results.json"

    @patch("src.cli.FuzzerCLI")
    def test_main_random_command(self, mock_cli_class):
        """Test main with random command"""
        mock_cli = Mock()
        mock_cli.run_random_test.return_value = 0
        mock_cli_class.return_value = mock_cli

        with patch("sys.argv", ["cli", "cluster", "--random", "--seed", "42"]):
            result = main()

        assert result == 0
        mock_cli.run_random_test.assert_called_once()

    @patch("src.cli.FuzzerCLI")
    def test_main_dsl_command(self, mock_cli_class):
        """Test main with DSL command"""
        mock_cli = Mock()
        mock_cli.run_dsl_test.return_value = 0
        mock_cli_class.return_value = mock_cli

        with patch("sys.argv", ["cli", "cluster", "--dsl", "test.yaml"]):
            result = main()

        assert result == 0
        mock_cli.run_dsl_test.assert_called_once()
        dsl_args = mock_cli.run_dsl_test.call_args.args[0]
        assert dsl_args.valkey_binary is None
        assert dsl_args.legacy_only is True

    @patch("src.cli.FuzzerCLI")
    def test_main_dsl_command_with_valkey_binary(self, mock_cli_class):
        """Test main with DSL command and explicit Valkey binary"""
        mock_cli = Mock()
        mock_cli.run_dsl_test.return_value = 0
        mock_cli_class.return_value = mock_cli

        with patch("sys.argv", ["cli", "cluster", "--dsl", "test.yaml", "--valkey-binary", "/tmp/valkey-server"]):
            result = main()

        assert result == 0
        mock_cli.run_dsl_test.assert_called_once()
        dsl_args = mock_cli.run_dsl_test.call_args.args[0]
        assert dsl_args.valkey_binary == "/tmp/valkey-server"

    @patch("src.cli.FuzzerCLI")
    def test_main_validate_command(self, mock_cli_class):
        """Test main with validate command"""
        mock_cli = Mock()
        mock_cli.validate_dsl.return_value = 0
        mock_cli_class.return_value = mock_cli

        with patch("sys.argv", ["cli", "validate", "test.yaml"]):
            result = main()

        assert result == 0
        mock_cli.validate_dsl.assert_called_once()

    def test_main_no_command(self):
        """Test main with no command"""
        with patch("sys.argv", ["cli"]):
            result = main()

        assert result == 1

    @patch("src.cli.FuzzerCLI")
    def test_main_keyboard_interrupt(self, mock_cli_class):
        """Test main handles keyboard interrupt"""
        mock_cli = Mock()
        mock_cli.run_random_test.side_effect = KeyboardInterrupt()
        mock_cli_class.return_value = mock_cli

        with patch("sys.argv", ["cli", "cluster", "--random"]):
            result = main()

        assert result == 130


def test_print_detailed_with_validation():
    """Test that _print_detailed_result displays detailed validation info"""
    import io
    import sys

    from src.models import ReplicationValidation, StateValidationResult, TopologyMismatch, TopologyValidation

    cli = FuzzerCLI()

    replication = ReplicationValidation(
        success=False,
        all_replicas_synced=False,
        max_lag=10.5,
        lagging_replicas=[],
        disconnected_replicas=["127.0.0.1:7001"],
        error_message="1 replica disconnected",
    )

    topology = TopologyValidation(
        success=False,
        expected_primaries=3,
        actual_primaries=2,
        expected_replicas=3,
        actual_replicas=3,
        topology_mismatches=[
            TopologyMismatch(
                mismatch_type="primary_count", node_id="cluster", expected="3 primaries", actual="2 primaries"
            )
        ],
        error_message="Primary count mismatch",
    )

    validation = StateValidationResult(
        overall_success=False,
        validation_timestamp=1000.0,
        validation_duration=2.5,
        replication=replication,
        cluster_status=None,
        slot_coverage=None,
        topology=topology,
        view_consistency=None,
        data_consistency=None,
        failed_checks=["replication", "topology"],
        error_messages=["1 replica disconnected", "Primary count mismatch"],
    )

    result = ExecutionResult(
        scenario_id="test-123",
        success=False,
        start_time=1000.0,
        end_time=1015.0,
        operations_executed=3,
        chaos_events=[],
        seed=42,
        final_validation=validation,
    )

    captured_output = io.StringIO()
    sys.stdout = captured_output

    try:
        cli._print_detailed_result(result)
        output = captured_output.getvalue()
    finally:
        sys.stdout = sys.__stdout__

    assert "Final Validation Details:" in output
    assert "Replication: FAIL" in output
    assert "Topology: FAIL" in output


def test_result_to_dict_includes_validation():
    """Test that _result_to_dict includes validation data"""
    from src.models import ReplicationValidation, StateValidationResult, TopologyValidation

    cli = FuzzerCLI()

    # Create result with validation
    replication = ReplicationValidation(
        success=False,
        all_replicas_synced=False,
        max_lag=10.5,
        lagging_replicas=[],
        disconnected_replicas=["127.0.0.1:7001"],
        error_message="1 replica disconnected",
    )

    topology = TopologyValidation(
        success=True,
        expected_primaries=3,
        actual_primaries=3,
        expected_replicas=3,
        actual_replicas=3,
        topology_mismatches=[],
        error_message=None,
    )

    validation = StateValidationResult(
        overall_success=False,
        validation_timestamp=1000.0,
        validation_duration=2.5,
        replication=replication,
        cluster_status=None,
        slot_coverage=None,
        topology=topology,
        view_consistency=None,
        data_consistency=None,
        failed_checks=["replication"],
        error_messages=["1 replica disconnected"],
    )

    result = ExecutionResult(
        scenario_id="test-123",
        success=False,
        start_time=1000.0,
        end_time=1015.0,
        operations_executed=3,
        chaos_events=[],
        seed=42,
        final_validation=validation,
    )

    # Convert to dict
    result_dict = cli._result_to_dict(result)

    # Verify final validation data is included
    assert "final_validation" in result_dict
    assert result_dict["final_validation"]["overall_success"] is False
    assert "replication" in result_dict["final_validation"]["failed_checks"]
    assert result_dict["final_validation"]["checks"]["replication"]["success"] is False
    assert result_dict["final_validation"]["checks"]["replication"]["error"] == "1 replica disconnected"
    assert result_dict["final_validation"]["checks"]["replication"]["max_lag"] == 10.5
    assert result_dict["final_validation"]["checks"]["replication"]["disconnected_replicas_count"] == 1

    # Verify topology check is included
    assert result_dict["final_validation"]["checks"]["topology"]["success"] is True
    assert result_dict["final_validation"]["checks"]["topology"]["expected_primaries"] == 3
    assert result_dict["final_validation"]["checks"]["topology"]["actual_primaries"] == 3


def test_validation_serialization_all_checks():
    """Test validation serialization with all check types"""
    from src.models import (
        DataConsistencyValidation,
        SlotConflict,
        SlotCoverageValidation,
        StateValidationResult,
        ViewConsistencyValidation,
        ViewDiscrepancy,
    )

    cli = FuzzerCLI()

    validation = StateValidationResult(
        overall_success=False,
        validation_timestamp=1000.0,
        validation_duration=3.5,
        replication=None,
        cluster_status=None,
        slot_coverage=SlotCoverageValidation(
            success=False,
            total_slots_assigned=16380,
            unassigned_slots=[100, 101],
            conflicting_slots=[SlotConflict(slot=200, conflicting_nodes=["n1", "n2"])],
            slot_distribution={},
            error_message="Slot issues",
        ),
        topology=None,
        view_consistency=ViewConsistencyValidation(
            success=False,
            nodes_checked=6,
            consistent_views=False,
            split_brain_detected=True,
            view_discrepancies=[
                ViewDiscrepancy(
                    discrepancy_type="membership",
                    node_reporting="127.0.0.1:7000",
                    subject_node="127.0.0.1:7001",
                    expected_value="primary",
                    actual_value="failed",
                )
            ],
            consensus_percentage=66.7,
            error_message="Split brain",
        ),
        data_consistency=DataConsistencyValidation(
            success=False,
            test_keys_checked=100,
            missing_keys=["k1"],
            inconsistent_keys=[],
            unreachable_keys=[],
            error_message="Missing keys",
        ),
        failed_checks=["slot_coverage", "view_consistency", "data_consistency"],
        error_messages=[],
    )

    validation_dict = cli._validation_to_dict(validation)

    # Verify all check types serialize correctly
    assert validation_dict["checks"]["slot_coverage"]["unassigned_slots_count"] == 2
    assert validation_dict["checks"]["view_consistency"]["split_brain_detected"] is True
    assert validation_dict["checks"]["data_consistency"]["missing_keys_count"] == 1
