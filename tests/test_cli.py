"""
Tests for CLI functionality
"""
import pytest
import json
import yaml
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from io import StringIO
import sys

from src.cli import FuzzerCLI, create_parser, main
from src.models import ExecutionResult, ChaosResult, ValidationResult
from src.models import ReplicationStatus, ConnectivityStatus, ConsistencyStatus


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
                end_time=1006.0
            )
        ],
        validation_results=[
            ValidationResult(
                slot_coverage=True,
                slot_conflicts=[],
                replica_sync=ReplicationStatus(
                    all_replicas_synced=True,
                    max_lag=0.5,
                    lagging_replicas=[]
                ),
                node_connectivity=ConnectivityStatus(
                    all_nodes_connected=True,
                    disconnected_nodes=[],
                    partition_groups=[]
                ),
                data_consistency=ConsistencyStatus(
                    consistent=True,
                    inconsistent_keys=[],
                    node_data_mismatches={}
                ),
                convergence_time=2.5,
                replication_lag=0.5,
                validation_timestamp=1015.0
            )
        ],
        seed=42
    )


class TestFuzzerCLI:
    """Test FuzzerCLI class"""
    
    def test_load_yaml_config(self, tmp_path):
        """Test loading YAML configuration file"""
        config_file = tmp_path / "config.yaml"
        config_data = {
            'cluster': {'num_shards': 3},
            'validation': {'check_slot_coverage': True}
        }
        
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        cli = FuzzerCLI()
        loaded = cli.load_config_file(str(config_file))
        
        assert loaded == config_data
    
    def test_load_json_config(self, tmp_path):
        """Test loading JSON configuration file"""
        config_file = tmp_path / "config.json"
        config_data = {
            'cluster': {'num_shards': 3},
            'validation': {'check_slot_coverage': True}
        }
        
        with open(config_file, 'w') as f:
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
    
    @patch('src.cli.ClusterBusFuzzer')
    def test_run_random_test_success(self, mock_fuzzer_class, mock_result, capsys):
        """Test running random test successfully"""
        mock_fuzzer = Mock()
        mock_fuzzer.run_random_test.return_value = mock_result
        mock_fuzzer_class.return_value = mock_fuzzer
        
        cli = FuzzerCLI()
        args = Mock(
            seed=42,
            iterations=1,
            config=None,
            output=None,
            verbose=False
        )
        
        result = cli.run_random_test(args)
        
        assert result == 0
        mock_fuzzer.run_random_test.assert_called_once_with(seed=42)
        
        captured = capsys.readouterr()
        assert "PASSED" in captured.out
        assert "test-scenario-123" in captured.out
    
    @patch('src.cli.ClusterBusFuzzer')
    def test_run_random_test_with_config(self, mock_fuzzer_class, mock_result, tmp_path):
        """Test running random test with config file"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("cluster:\n  num_shards: 3\n")
        
        mock_fuzzer = Mock()
        mock_fuzzer.run_random_test.return_value = mock_result
        mock_fuzzer_class.return_value = mock_fuzzer
        
        cli = FuzzerCLI()
        args = Mock(
            seed=None,
            iterations=1,
            config=str(config_file),
            output=None,
            verbose=False
        )
        
        result = cli.run_random_test(args)
        
        assert result == 0
        assert cli.config == {'cluster': {'num_shards': 3}}
    
    @patch('src.cli.ClusterBusFuzzer')
    def test_run_random_test_multiple_iterations(self, mock_fuzzer_class, mock_result):
        """Test running multiple test iterations"""
        mock_fuzzer = Mock()
        mock_fuzzer.run_random_test.return_value = mock_result
        mock_fuzzer_class.return_value = mock_fuzzer
        
        cli = FuzzerCLI()
        args = Mock(
            seed=42,
            iterations=3,
            config=None,
            output=None,
            verbose=False
        )
        
        result = cli.run_random_test(args)
        
        assert result == 0
        assert mock_fuzzer.run_random_test.call_count == 3
    
    @patch('src.cli.ClusterBusFuzzer')
    def test_run_random_test_with_output(self, mock_fuzzer_class, mock_result, tmp_path):
        """Test saving test results to file"""
        output_file = tmp_path / "results.json"
        
        mock_fuzzer = Mock()
        mock_fuzzer.run_random_test.return_value = mock_result
        mock_fuzzer_class.return_value = mock_fuzzer
        
        cli = FuzzerCLI()
        args = Mock(
            seed=42,
            iterations=1,
            config=None,
            output=str(output_file),
            format='json',
            verbose=False
        )
        
        result = cli.run_random_test(args)
        
        assert result == 0
        assert output_file.exists()
        
        with open(output_file) as f:
            data = json.load(f)
        
        assert data['total_tests'] == 1
        assert data['passed'] == 1
        assert len(data['results']) == 1
    
    @patch('src.cli.ClusterBusFuzzer')
    @patch('src.cli.DSLLoader')
    def test_run_dsl_test_success(self, mock_loader, mock_fuzzer_class, mock_result, tmp_path):
        """Test running DSL-based test"""
        dsl_file = tmp_path / "test.yaml"
        dsl_file.write_text("scenario:\n  id: test\n")
        
        mock_dsl_config = Mock()
        mock_loader.load_from_file.return_value = mock_dsl_config
        
        mock_fuzzer = Mock()
        mock_fuzzer.run_dsl_test.return_value = mock_result
        mock_fuzzer_class.return_value = mock_fuzzer
        
        cli = FuzzerCLI()
        args = Mock(
            file=str(dsl_file),
            output=None,
            verbose=False
        )
        
        result = cli.run_dsl_test(args)
        
        assert result == 0
        mock_loader.load_from_file.assert_called_once_with(str(dsl_file))
        mock_fuzzer.run_dsl_test.assert_called_once_with(mock_dsl_config)
    
    @patch('src.cli.ClusterBusFuzzer')
    def test_run_dsl_test_file_not_found(self, mock_fuzzer_class, capsys):
        """Test DSL test with non-existent file"""
        cli = FuzzerCLI()
        args = Mock(
            file="nonexistent.yaml",
            output=None,
            verbose=False
        )
        
        result = cli.run_dsl_test(args)
        
        assert result == 1
        captured = capsys.readouterr()
        assert "not found" in captured.out
    
    @patch('src.cli.DSLLoader')
    @patch('src.cli.ScenarioGenerator')
    def test_validate_dsl_success(self, mock_generator_class, mock_loader, tmp_path, capsys):
        """Test validating DSL configuration"""
        dsl_file = tmp_path / "test.yaml"
        dsl_file.write_text("scenario_id: test\ncluster:\n  num_shards: 3\n  replicas_per_shard: 1\noperations:\n  - type: failover\n    target_node: node-0\n")
        
        mock_dsl_config = Mock(config_text="test config")
        mock_loader.load_from_file.return_value = mock_dsl_config
        
        mock_scenario = Mock(
            scenario_id="test-123",
            seed=42,
            cluster_config=Mock(num_shards=3, replicas_per_shard=1),
            operations=[Mock()],
            chaos_config=Mock(chaos_type=Mock(value="process_kill"))
        )
        
        mock_generator = Mock()
        mock_generator.parse_dsl_config.return_value = mock_scenario
        mock_generator.validate_scenario.return_value = None
        mock_generator_class.return_value = mock_generator
        
        cli = FuzzerCLI()
        args = Mock(
            file=str(dsl_file),
            verbose=False
        )
        
        result = cli.validate_dsl(args)
        
        assert result == 0
        captured = capsys.readouterr()
        assert "valid" in captured.out.lower()
    
    def test_result_to_dict(self, mock_result):
        """Test converting ExecutionResult to dictionary"""
        cli = FuzzerCLI()
        result_dict = cli._result_to_dict(mock_result)
        
        assert result_dict['scenario_id'] == "test-scenario-123"
        assert result_dict['success'] is True
        assert result_dict['duration'] == 15.5
        assert result_dict['operations_executed'] == 3
        assert result_dict['chaos_events'] == 1
        assert result_dict['seed'] == 42


class TestCLIParser:
    """Test CLI argument parser"""
    
    def test_create_parser(self):
        """Test parser creation"""
        parser = create_parser()
        assert parser.prog == 'valkey-fuzzer'
    
    def test_parse_random_command(self):
        """Test parsing random command"""
        parser = create_parser()
        args = parser.parse_args(['random', '--seed', '42'])
        
        assert args.command == 'random'
        assert args.seed == 42
    
    def test_parse_random_with_iterations(self):
        """Test parsing random command with iterations"""
        parser = create_parser()
        args = parser.parse_args(['random', '--iterations', '5'])
        
        assert args.command == 'random'
        assert args.iterations == 5
    
    def test_parse_random_with_config(self):
        """Test parsing random command with config"""
        parser = create_parser()
        args = parser.parse_args(['random', '--config', 'config.yaml'])
        
        assert args.command == 'random'
        assert args.config == 'config.yaml'
    
    def test_parse_dsl_command(self):
        """Test parsing DSL command"""
        parser = create_parser()
        args = parser.parse_args(['dsl', 'test.yaml'])
        
        assert args.command == 'dsl'
        assert args.file == 'test.yaml'
    
    def test_parse_validate_command(self):
        """Test parsing validate command"""
        parser = create_parser()
        args = parser.parse_args(['validate', 'test.yaml'])
        
        assert args.command == 'validate'
        assert args.file == 'test.yaml'
    
    def test_parse_verbose_flag(self):
        """Test parsing verbose flag"""
        parser = create_parser()
        args = parser.parse_args(['random', '--verbose'])
        
        assert args.verbose is True
    
    def test_parse_output_options(self):
        """Test parsing output options"""
        parser = create_parser()
        args = parser.parse_args(['random', '--output', 'results.json', '--format', 'yaml'])
        
        assert args.output == 'results.json'
        assert args.format == 'yaml'


class TestCLIMain:
    """Test main CLI entry point"""
    
    @patch('src.cli.FuzzerCLI')
    def test_main_random_command(self, mock_cli_class):
        """Test main with random command"""
        mock_cli = Mock()
        mock_cli.run_random_test.return_value = 0
        mock_cli_class.return_value = mock_cli
        
        with patch('sys.argv', ['cli', 'random', '--seed', '42']):
            result = main()
        
        assert result == 0
        mock_cli.run_random_test.assert_called_once()
    
    @patch('src.cli.FuzzerCLI')
    def test_main_dsl_command(self, mock_cli_class):
        """Test main with DSL command"""
        mock_cli = Mock()
        mock_cli.run_dsl_test.return_value = 0
        mock_cli_class.return_value = mock_cli
        
        with patch('sys.argv', ['cli', 'dsl', 'test.yaml']):
            result = main()
        
        assert result == 0
        mock_cli.run_dsl_test.assert_called_once()
    
    @patch('src.cli.FuzzerCLI')
    def test_main_validate_command(self, mock_cli_class):
        """Test main with validate command"""
        mock_cli = Mock()
        mock_cli.validate_dsl.return_value = 0
        mock_cli_class.return_value = mock_cli
        
        with patch('sys.argv', ['cli', 'validate', 'test.yaml']):
            result = main()
        
        assert result == 0
        mock_cli.validate_dsl.assert_called_once()
    
    def test_main_no_command(self, capsys):
        """Test main with no command"""
        with patch('sys.argv', ['cli']):
            result = main()
        
        assert result == 1
    
    @patch('src.cli.FuzzerCLI')
    def test_main_keyboard_interrupt(self, mock_cli_class):
        """Test main handles keyboard interrupt"""
        mock_cli = Mock()
        mock_cli.run_random_test.side_effect = KeyboardInterrupt()
        mock_cli_class.return_value = mock_cli
        
        with patch('sys.argv', ['cli', 'random']):
            result = main()
        
        assert result == 130
