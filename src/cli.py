#!/usr/bin/env python3
"""
Command-line interface for the Cluster Bus Fuzzer

Provides commands for running random tests, DSL-based tests, and validating configurations.
"""
import sys
import argparse
import json
import yaml
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime

from .main import ClusterBusFuzzer
from .fuzzer_engine import DSLLoader
from .models import ClusterConfig, ValidationConfig, WorkloadConfig, ExecutionResult, ValidationResult


class FuzzerCLI:
    """Command-line interface for the Cluster Bus Fuzzer"""
    
    def __init__(self):
        self.fuzzer = ClusterBusFuzzer()
        self.config = {}
    
    def load_config_file(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML or JSON file"""
        path = Path(config_path)
        
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(path, 'r') as f:
            if path.suffix in ['.yaml', '.yml']:
                return yaml.safe_load(f)
            elif path.suffix == '.json':
                return json.load(f)
            else:
                raise ValueError(f"Unsupported config format: {path.suffix}")
    
    def run_random_test(self, args) -> int:
        """Execute a randomized test scenario"""
        self._print_header("Random Test Scenario")
        
        # Load config if provided
        if args.config:
            try:
                self.config = self.load_config_file(args.config)
                print(f"Loaded configuration from {args.config}")
            except Exception as e:
                print(f"Failed to load config: {e}")
                return 1
        
        # Display test parameters
        seed = args.seed
        if seed:
            print(f"Seed: {seed} (reproducible)")
        else:
            print("Seed: Random")
        
        if args.iterations:
            print(f"Iterations: {args.iterations}")
        
        print()
        
        # Run test(s)
        results = []
        for i in range(args.iterations):
            if args.iterations > 1:
                print(f"\n--- Iteration {i + 1}/{args.iterations} ---")
            
            try:
                result = self.fuzzer.run_random_test(seed=seed)
                results.append(result)
                
                if args.verbose:
                    self._print_detailed_result(result)
                else:
                    self._print_summary_result(result)
                
                # For multiple iterations with same seed, increment seed
                if seed and args.iterations > 1:
                    seed += 1
                    
            except Exception as e:
                print(f"Test failed with exception: {e}")
                if args.verbose:
                    import traceback
                    traceback.print_exc()
                return 1
        
        # Print aggregate results for multiple iterations
        if args.iterations > 1:
            self._print_aggregate_results(results)
        
        # Save results if output specified
        if args.output:
            self._save_results(results, args.output, args.format)
        
        # Return success if all tests passed
        return 0 if all(r.success for r in results) else 1
    
    def run_dsl_test(self, args) -> int:
        """Execute a DSL-based test scenario"""
        self._print_header(f"DSL Test: {args.file}")
        
        dsl_path = Path(args.file)
        if not dsl_path.exists():
            print(f"[FAIL] DSL file not found: {args.file}")
            return 1
        
        try:
            # Load DSL configuration
            dsl_config = DSLLoader.load_from_file(str(dsl_path))
            print(f"Loaded DSL from {args.file}")
            print()
            
            # Run test
            result = self.fuzzer.run_dsl_test(dsl_config)
            
            if args.verbose:
                self._print_detailed_result(result)
            else:
                self._print_summary_result(result)
            
            # Save results if output specified
            if args.output:
                self._save_results([result], args.output, args.format)
            
            return 0 if result.success else 1
            
        except Exception as e:
            print(f"DSL test failed: {e}")
            if args.verbose:
                import traceback
                traceback.print_exc()
            return 1
    
    def validate_dsl(self, args) -> int:
        """Validate a DSL configuration file"""
        self._print_header(f"Validating DSL: {args.file}")
        
        dsl_path = Path(args.file)
        if not dsl_path.exists():
            print(f"[FAIL] DSL file not found: {args.file}")
            return 1
        
        try:
            # Load DSL
            dsl_config = DSLLoader.load_from_file(str(dsl_path))
            print("DSL file loaded successfully")
            
            # Parse and validate
            from .fuzzer_engine import ScenarioGenerator
            generator = ScenarioGenerator()
            
            scenario = generator.parse_dsl_config(dsl_config.config_text)
            print("DSL parsed successfully")
            
            generator.validate_scenario(scenario)
            print("Scenario validated successfully")
            
            # Print scenario summary
            print("\n" + "=" * 60)
            print("Scenario Summary")
            print("=" * 60)
            print(f"ID: {scenario.scenario_id}")
            print(f"Seed: {scenario.seed}")
            print(f"Cluster: {scenario.cluster_config.num_shards} shards, "
                  f"{scenario.cluster_config.replicas_per_shard} replicas per shard")
            print(f"Operations: {len(scenario.operations)}")
            print(f"Chaos Type: {scenario.chaos_config.chaos_type.value}")
            
            if args.verbose:
                print("\nOperations:")
                for i, op in enumerate(scenario.operations, 1):
                    print(f"  {i}. {op.type.value} on {op.target_node}")
            
            print("\nDSL configuration is valid!")
            return 0
            
        except Exception as e:
            print(f"\nValidation Failed: {e}")
            if args.verbose:
                import traceback
                traceback.print_exc()
            return 1
    
    def _print_header(self, title: str):
        """Print formatted header"""
        print()
        print("=" * 80)
        print(title)
        print("=" * 80)
        print()
    
    def _print_summary_result(self, result: ExecutionResult):
        """Print summary of test result"""
        status = "PASSED" if result.success else "FAILED"
        duration = result.end_time - result.start_time
        
        print(f"\nScenario: {result.scenario_id}")
        print(f"Status: {status}")
        print(f"Duration: {duration:.2f}s")
        print(f"Operations: {result.operations_executed}")
        print(f"Chaos Events: {len(result.chaos_events)}")
        print(f"Validations: {len(result.validation_results)}")
        
        if result.seed:
            print(f"Seed: {result.seed} (use to reproduce)")
        
        if result.error_message:
            print(f"Error: {result.error_message}")
    
    def _print_detailed_result(self, result: ExecutionResult):
        """Print detailed test result"""
        self._print_summary_result(result)
        
        # Print chaos events
        if result.chaos_events:
            print("\nChaos Events:")
            for event in result.chaos_events:
                status = "[PASS]" if event.success else "[FAIL]"
                duration = (event.end_time - event.start_time) if event.end_time else 0
                print(f"  {status} {event.chaos_type.value} on {event.target_node} "
                      f"({duration:.2f}s)")
                if event.error_message:
                    print(f"    Error: {event.error_message}")
        
        # Print validation results
        if result.validation_results:
            print("\nValidation Results:")
            final = result.validation_results[-1]
            print(f"  Slot Coverage: {'[PASS]' if final.slot_coverage else '[FAIL]'}")
            print(f"  Slot Conflicts: {len(final.slot_conflicts)}")
            print(f"  Replicas Synced: {'[PASS]' if final.replica_sync.all_replicas_synced else '[FAIL]'}")
            print(f"  Nodes Connected: {'[PASS]' if final.node_connectivity.all_nodes_connected else '[FAIL]'}")
            print(f"  Data Consistent: {'[PASS]' if final.data_consistency.consistent else '[FAIL]'}")
            print(f"  Convergence Time: {final.convergence_time:.2f}s")
            print(f"  Replication Lag: {final.replication_lag:.2f}s")
    
    def _print_aggregate_results(self, results: list):
        """Print aggregate statistics for multiple test runs"""
        print("\n" + "=" * 80)
        print("Aggregate Results")
        print("=" * 80)
        
        total = len(results)
        passed = sum(1 for r in results if r.success)
        failed = total - passed
        
        avg_duration = sum(r.end_time - r.start_time for r in results) / total
        avg_operations = sum(r.operations_executed for r in results) / total
        avg_chaos = sum(len(r.chaos_events) for r in results) / total
        
        print(f"Total Tests: {total}")
        print(f"Passed: {passed} ({passed/total*100:.1f}%)")
        print(f"Failed: {failed} ({failed/total*100:.1f}%)")
        print(f"Average Duration: {avg_duration:.2f}s")
        print(f"Average Operations: {avg_operations:.1f}")
        print(f"Average Chaos Events: {avg_chaos:.1f}")
    
    def _save_results(self, results: list, output_path: str, format: str):
        """Save test results to file"""
        try:
            output = Path(output_path)
            output.parent.mkdir(parents=True, exist_ok=True)
            
            # Convert results to dict
            data = {
                'timestamp': datetime.now().isoformat(),
                'total_tests': len(results),
                'passed': sum(1 for r in results if r.success),
                'failed': sum(1 for r in results if not r.success),
                'results': [self._result_to_dict(r) for r in results]
            }
            
            with open(output, 'w') as f:
                if format == 'json':
                    json.dump(data, f, indent=2)
                elif format == 'yaml':
                    yaml.dump(data, f, default_flow_style=False)
            
            print(f"\n[PASS] Results saved to {output_path}")
            
        except Exception as e:
            print(f"\n[FAIL] Failed to save results: {e}")
    
    def _result_to_dict(self, result: ExecutionResult) -> Dict[str, Any]:
        """Convert ExecutionResult to dictionary"""
        return {
            'scenario_id': result.scenario_id,
            'success': result.success,
            'duration': result.end_time - result.start_time,
            'operations_executed': result.operations_executed,
            'chaos_events': len(result.chaos_events),
            'validations': len(result.validation_results),
            'seed': result.seed,
            'error_message': result.error_message
        }


def create_parser() -> argparse.ArgumentParser:
    """Create argument parser for CLI"""
    parser = argparse.ArgumentParser(
        prog='valkey-fuzzer',
        description='Valkey Cluster Bus Fuzzer - Test Valkey cluster robustness through chaos engineering',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run a random test
  valkey-fuzzer random
  
  # Run with specific seed for reproducibility
  valkey-fuzzer random --seed 42
  
  # Run multiple iterations
  valkey-fuzzer random --iterations 10
  
  # Run with configuration file
  valkey-fuzzer random --config config.yaml --output results.json
  
  # Run DSL-based test
  valkey-fuzzer dsl examples/simple_failover.yaml
  
  # Validate DSL file
  valkey-fuzzer validate examples/simple_failover.yaml
  
  # Verbose output
  valkey-fuzzer random --seed 42 --verbose
        """
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version='Valkey Fuzzer 0.1.0'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Random test command
    random_parser = subparsers.add_parser(
        'random',
        help='Run a randomized test scenario'
    )
    random_parser.add_argument(
        '--seed',
        type=int,
        help='Seed for reproducibility'
    )
    random_parser.add_argument(
        '--iterations',
        type=int,
        default=1,
        help='Number of test iterations to run (default: 1)'
    )
    random_parser.add_argument(
        '--config',
        type=str,
        help='Path to configuration file (YAML or JSON)'
    )
    random_parser.add_argument(
        '--output',
        type=str,
        help='Path to save test results'
    )
    random_parser.add_argument(
        '--format',
        choices=['json', 'yaml'],
        default='json',
        help='Output format for results (default: json)'
    )
    random_parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    
    # DSL test command
    dsl_parser = subparsers.add_parser(
        'dsl',
        help='Run a DSL-based test scenario'
    )
    dsl_parser.add_argument(
        'file',
        help='Path to DSL YAML file'
    )
    dsl_parser.add_argument(
        '--output',
        type=str,
        help='Path to save test results'
    )
    dsl_parser.add_argument(
        '--format',
        choices=['json', 'yaml'],
        default='json',
        help='Output format for results (default: json)'
    )
    dsl_parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    
    # Validate command
    validate_parser = subparsers.add_parser(
        'validate',
        help='Validate a DSL configuration file'
    )
    validate_parser.add_argument(
        'file',
        help='Path to DSL YAML file'
    )
    validate_parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    
    return parser


def main():
    """Main entry point for CLI"""
    parser = create_parser()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    cli = FuzzerCLI()
    
    try:
        if args.command == 'random':
            return cli.run_random_test(args)
        elif args.command == 'dsl':
            return cli.run_dsl_test(args)
        elif args.command == 'validate':
            return cli.validate_dsl(args)
    except KeyboardInterrupt:
        print("\n\nValkey Fuzzer process was interrupted by user")
        return 130
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        if hasattr(args, 'verbose') and args.verbose:
            import traceback
            traceback.print_exc()
        return 1
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
