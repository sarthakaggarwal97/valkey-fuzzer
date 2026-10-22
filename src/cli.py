#!/usr/bin/env python3
"""Command-line interface for the Valkey Fuzzer."""

import argparse
import json
import logging
import math
import os
import sys
import tempfile
import time
import traceback
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from .extensibility import ComponentStatus, OutcomeStatus, RunOutcome, RunPlan, to_plain_data
from .feature_packs import SCENARIO_API_VERSION, load_v3_scenario_yaml
from .feature_runtime import FeatureRuntime
from .fuzzer_engine import DSLLoader
from .fuzzer_engine.test_case_generator import ScenarioGenerator
from .main import ClusterBusFuzzer
from .models import ExecutionResult, StateValidationResult, TopologyType
from .utils.cluster_utils import cleanup_logs

FEATURE_REPORT_API_VERSION = "v1"
EXIT_SUCCESS = 0
EXIT_FEATURE_FAILURE = 1
EXIT_HARNESS_ERROR = 2
EXIT_UNSUPPORTED = 3


class ScenarioRoutingError(ValueError):
    """A scenario cannot be routed safely to a DSL implementation."""

    def __init__(self, message: str, *, feature_hint: bool = False):
        self.feature_hint = feature_hint
        super().__init__(message)


class FuzzerCLI:
    """Command-line interface for topology-aware Valkey fuzzing."""

    def __init__(self):
        self._fuzzer = None
        self.config = {}

    @property
    def fuzzer(self) -> ClusterBusFuzzer:
        """Construct the legacy engine only when a v1/v2 command needs it."""
        if self._fuzzer is None:
            self._fuzzer = ClusterBusFuzzer()
        return self._fuzzer

    def load_config_file(self, config_path: str) -> dict[str, Any]:
        """Load configuration from YAML or JSON file"""
        path = Path(config_path)

        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(path) as f:
            if path.suffix in [".yaml", ".yml"]:
                return yaml.safe_load(f)
            elif path.suffix == ".json":
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
                print(f"Error: Failed to load config file: {e}")
                print("\nConfig file must be YAML (.yaml, .yml) or JSON (.json)")
                print("Example: valkey-fuzzer cluster --seed 42 --config config.yaml")
                return 1

        seed = args.seed
        if seed:
            print(f"Seed: {seed} (reproducible)")
        else:
            print("Seed: Random")

        # Default number of iterations is 1
        if args.iterations:
            print(f"Iterations: {args.iterations}")

        if args.export_dsl:
            print(f"DSL Export: {args.export_dsl}")

        print()

        # Run test(s)
        results = []
        for i in range(args.iterations):
            if args.iterations > 1:
                print(f"\n--- Iteration {i + 1}/{args.iterations} ---")

            try:
                run_kwargs = {"seed": seed}
                if args.valkey_binary:
                    run_kwargs["valkey_binary"] = args.valkey_binary

                result = self.fuzzer.run_random_test(**run_kwargs)
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
                    traceback.print_exc()
                return 1

        # Print aggregate results for multiple iterations
        if args.iterations > 1:
            self._print_aggregate_results(results)

        # Save results if output specified
        if args.output:
            self._save_results(results, args.output, args.format)

        # Export scenario to DSL if requested
        if args.export_dsl and self.fuzzer.last_scenario:
            try:
                self._export_scenario_to_dsl(self.fuzzer.last_scenario, args.export_dsl)
            except Exception as e:
                print(f"\nError: Failed to export scenario to DSL: {e}")
                return 1

        # Return success if all tests passed
        return 0 if all(r.success for r in results) else 1

    @staticmethod
    def _load_scenario_source(dsl_path: Path) -> tuple[str, str]:
        """Read once and identify the API without imposing v3 rules on legacy YAML."""
        try:
            source = dsl_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ValueError(f"Unable to read DSL file {dsl_path}: {exc}") from exc

        try:
            root = yaml.compose(source, Loader=yaml.SafeLoader)
            document = yaml.safe_load(source)
        except yaml.YAMLError as exc:
            raise ScenarioRoutingError(f"Invalid YAML syntax in {dsl_path}: {exc}") from exc
        if not isinstance(root, yaml.nodes.MappingNode) or not isinstance(document, Mapping):
            raise ScenarioRoutingError("DSL configuration must be a mapping")

        direct_versions = [
            value_node.value if isinstance(value_node, yaml.nodes.ScalarNode) else None
            for key_node, value_node in root.value
            if isinstance(key_node, yaml.nodes.ScalarNode) and key_node.value == "api_version"
        ]
        if len(direct_versions) > 1 and not set(direct_versions).issubset({"v1", "v2"}):
            raise ScenarioRoutingError(
                "top-level api_version is defined more than once",
                feature_hint=any(version not in {"v1", "v2"} for version in direct_versions),
            )

        api_version = document.get("api_version", "v1")
        if api_version is None:
            api_version = "v1"
        if not isinstance(api_version, str):
            raise ScenarioRoutingError(
                "api_version must be a string",
                feature_hint=True,
            )
        if api_version not in {"v1", "v2", SCENARIO_API_VERSION}:
            raise ScenarioRoutingError(
                f"Unsupported api_version: {api_version}",
                feature_hint=True,
            )
        return source, api_version

    @staticmethod
    def _feature_runtime(args) -> FeatureRuntime:
        """Create one isolated extension runtime only for a v3 invocation."""
        feature_pack_directories = getattr(args, "feature_pack_dirs", ())
        if not isinstance(feature_pack_directories, (list, tuple)):
            feature_pack_directories = ()
        artifact_dir = getattr(args, "artifact_dir", "")
        if not isinstance(artifact_dir, str):
            artifact_dir = ""
        return FeatureRuntime(
            feature_pack_directories=tuple(feature_pack_directories),
            discover_entry_points=getattr(args, "no_plugins", False) is not True,
            artifact_dir=artifact_dir,
        )

    @staticmethod
    def _topology_overrides(args) -> Mapping[str, Any]:
        valkey_binary = getattr(args, "valkey_binary", None)
        return {"valkey_binary": valkey_binary} if isinstance(valkey_binary, str) and valkey_binary else {}

    @staticmethod
    def _reject_v3_only_legacy_options(args, *, validation: bool = False) -> None:
        enabled = []
        feature_pack_directories = getattr(args, "feature_pack_dirs", ())
        if isinstance(feature_pack_directories, (list, tuple)) and feature_pack_directories:
            enabled.append("--feature-pack-dir")
        if getattr(args, "no_plugins", False) is True:
            enabled.append("--no-plugins")
        artifact_dir = getattr(args, "artifact_dir", "")
        if isinstance(artifact_dir, str) and artifact_dir:
            enabled.append("--artifact-dir")
        timeout = getattr(args, "timeout", None)
        if isinstance(timeout, (int, float)) and not isinstance(timeout, bool):
            enabled.append("--timeout")
        if getattr(args, "allow_unsupported", False) is True:
            enabled.append("--allow-unsupported")
        valkey_binary = getattr(args, "valkey_binary", None)
        if validation and isinstance(valkey_binary, str) and valkey_binary:
            enabled.append("--valkey-binary")
        if enabled:
            joined = ", ".join(enabled)
            raise ScenarioRoutingError(
                f"{joined} require an api_version: v3 scenario",
                feature_hint=True,
            )

    def run_dsl_test(self, args) -> int:
        """Execute a version-routed DSL scenario."""
        self._print_header(f"Scenario: {args.file}")

        dsl_path = Path(args.file)
        if not dsl_path.exists():
            print(f"Error: DSL file not found: {args.file}")
            print("\nMake sure the file path is correct.")
            print("Example: valkey-fuzzer run examples/simple_failover.yaml")
            return 1

        api_version = None
        try:
            source, api_version = self._load_scenario_source(dsl_path)
            if api_version == SCENARIO_API_VERSION and getattr(args, "legacy_only", False) is not True:
                document = load_v3_scenario_yaml(source, source_name=str(dsl_path))
                return self._run_feature_test(args, document)
            return self._run_legacy_dsl_test(args, source)

        except Exception as e:
            print(f"Error: DSL test failed: {e}")
            print(f"\nTry validating your DSL file first: valkey-fuzzer validate {args.file}")
            if args.verbose:
                traceback.print_exc()
            feature_error = api_version == SCENARIO_API_VERSION or (
                isinstance(e, ScenarioRoutingError) and e.feature_hint
            )
            return EXIT_HARNESS_ERROR if feature_error else EXIT_FEATURE_FAILURE

    def _run_legacy_dsl_test(self, args, source: str) -> int:
        """Run a v1/v2 scenario through the unchanged legacy engine."""
        self._reject_v3_only_legacy_options(args)
        dsl_config = DSLLoader.load_from_string(source)
        print(f"Loaded DSL from {args.file}")
        print()

        run_kwargs = {}
        if getattr(args, "valkey_binary", None):
            run_kwargs["valkey_binary"] = args.valkey_binary

        result = self.fuzzer.run_dsl_test(dsl_config, **run_kwargs)

        if args.verbose:
            self._print_detailed_result(result)
        else:
            self._print_summary_result(result)

        if getattr(args, "output", None):
            self._save_results([result], args.output, args.format)

        return EXIT_SUCCESS if result.success else EXIT_FEATURE_FAILURE

    def _run_feature_test(
        self,
        args,
        document: Mapping[str, Any],
    ) -> int:
        """Compile and execute one v3 feature-pack scenario."""
        runtime = self._feature_runtime(args)
        plan = runtime.compile(
            document,
            topology_overrides=self._topology_overrides(args),
        )
        timeout = getattr(args, "timeout", None)
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool):
            timeout = None
        deadline = time.monotonic() + timeout if timeout is not None else None
        outcome = runtime.run_plan(plan, deadline=deadline)

        print(f"Loaded v3 feature scenario from {args.file}")
        if args.verbose:
            self._print_feature_detailed_result(outcome)
        else:
            self._print_feature_summary_result(outcome)

        if getattr(args, "output", None):
            self._save_feature_outcomes((outcome,), args.output, args.format)

        if outcome.status is OutcomeStatus.PASS:
            return EXIT_SUCCESS
        if outcome.status is OutcomeStatus.FAIL:
            return EXIT_FEATURE_FAILURE
        if outcome.status is OutcomeStatus.SKIP_UNSUPPORTED:
            return EXIT_SUCCESS if getattr(args, "allow_unsupported", False) is True else EXIT_UNSUPPORTED
        return EXIT_HARNESS_ERROR

    def validate_dsl(self, args) -> int:
        """Validate a DSL configuration file"""
        self._print_header(f"Validating DSL: {args.file}")

        dsl_path = Path(args.file)
        if not dsl_path.exists():
            print(f"Error: DSL file not found: {args.file}")
            print("\nMake sure the file path is correct.")
            print("Example: valkey-fuzzer validate examples/simple_failover.yaml")
            return 1

        api_version = None
        try:
            source, api_version = self._load_scenario_source(dsl_path)
            if api_version == SCENARIO_API_VERSION:
                document = load_v3_scenario_yaml(source, source_name=str(dsl_path))
                return self._validate_feature_scenario(args, document)
            return self._validate_legacy_dsl(args, source)

        except Exception as e:
            print(f"\nError: Validation failed: {e}")
            print("\nCheck your DSL file syntax. See examples in the examples/ directory.")
            if args.verbose:
                traceback.print_exc()
            feature_error = api_version == SCENARIO_API_VERSION or (
                isinstance(e, ScenarioRoutingError) and e.feature_hint
            )
            return EXIT_HARNESS_ERROR if feature_error else EXIT_FEATURE_FAILURE

    def _validate_legacy_dsl(self, args, source: str) -> int:
        self._reject_v3_only_legacy_options(args, validation=True)
        dsl_config = DSLLoader.load_from_string(source)
        print("DSL file loaded successfully")

        generator = ScenarioGenerator()
        scenario = generator.parse_dsl_config(dsl_config.config_text)
        print("DSL parsed successfully")

        generator.validate_scenario(scenario)
        print("Scenario validated successfully")

        print("\n" + "=" * 60)
        print("Scenario Summary")
        print("=" * 60)
        print(f"ID: {scenario.scenario_id}")
        print(f"API Version: {scenario.api_version}")
        print(f"Seed: {scenario.seed}")
        print(self._format_topology_summary(scenario.cluster_config))
        print(f"Operations: {len(scenario.operations)}")
        if scenario.operations:
            print(f"Chaos Type: {scenario.chaos_config.chaos_type.value}")
        else:
            print("Chaos: disabled (no operations)")

        if args.verbose:
            print("\nOperations:")
            for i, op in enumerate(scenario.operations, 1):
                print(f"  {i}. {op.type.value} on {op.target_node}")

        print("\nDSL configuration is valid!")
        return EXIT_SUCCESS

    def _validate_feature_scenario(
        self,
        args,
        document: Mapping[str, Any],
    ) -> int:
        runtime = self._feature_runtime(args)
        plan = runtime.compile(
            document,
            topology_overrides=self._topology_overrides(args),
        )
        runtime.validate_plan(plan)

        print("V3 scenario compiled successfully")
        print("Structural topology preflight completed without provisioning")
        print("Component factories were not instantiated; Valkey binary availability was not checked")
        self._print_feature_plan_summary(plan, verbose=args.verbose)
        print("\nV3 feature scenario passed structural validation!")
        return EXIT_SUCCESS

    @staticmethod
    def _format_topology_summary(config) -> str:
        """Return a concise topology summary with an accurate node count."""
        topology_type = getattr(config, "topology_type", TopologyType.CLUSTER)
        topology_name = getattr(topology_type, "value", str(topology_type))
        num_shards = config.num_shards
        replicas_per_shard = config.replicas_per_shard
        total_nodes = num_shards * (1 + replicas_per_shard)

        if topology_name == TopologyType.STANDALONE.value:
            return f"Topology: standalone ({total_nodes} node)"
        if topology_name == TopologyType.REPLICATION.value:
            replica_label = "replica" if replicas_per_shard == 1 else "replicas"
            return f"Topology: replication (1 primary, {replicas_per_shard} {replica_label}, {total_nodes} nodes total)"
        return (
            f"Topology: cluster ({num_shards} shards, {replicas_per_shard} replicas per shard, "
            f"{total_nodes} nodes total)"
        )

    @staticmethod
    def _feature_duration(outcome: RunOutcome) -> float:
        if outcome.started_at is None or outcome.ended_at is None:
            return 0.0
        return max(0.0, outcome.ended_at - outcome.started_at)

    @staticmethod
    def _component_status_counts(outcome: RunOutcome) -> dict[ComponentStatus, int]:
        counts = {status: 0 for status in ComponentStatus}
        for result in outcome.component_results:
            counts[result.status] += 1
        return counts

    def _print_feature_plan_summary(self, plan: RunPlan, *, verbose: bool = False) -> None:
        print("\n" + "=" * 60)
        print("Feature Plan Summary")
        print("=" * 60)
        print(f"ID: {plan.scenario_id}")
        print(f"Feature Pack: {plan.pack_id}@{plan.pack_version}")
        print(f"Scenario API: {plan.scenario_api_version or SCENARIO_API_VERSION}")
        print(f"Extension API: {plan.api_version}")
        print(f"Mode: {plan.mode.value}")
        print(f"Topology: {plan.topology}")
        print(f"Seed: {plan.seed}")
        print(f"Components: {len(plan.components)}")
        print(f"Startup Overlays: {len(plan.startup_overlays)}")
        print(f"Required Evidence: {len(plan.required_evidence)}")

        if verbose and plan.components:
            print("\nComponents:")
            for component in plan.components:
                dependencies = ", ".join(component.depends_on) if component.depends_on else "none"
                print(
                    f"  - {component.instance_id}: {component.kind.value} "
                    f"{component.component_id} (required={component.required}, depends_on={dependencies})"
                )

    def _print_feature_summary_result(self, outcome: RunOutcome) -> None:
        counts = self._component_status_counts(outcome)
        print(f"\nScenario: {outcome.plan.scenario_id}")
        print(f"Feature Pack: {outcome.plan.pack_id}@{outcome.plan.pack_version}")
        print(f"Status: {outcome.status.value}")
        print(f"Mode: {outcome.plan.mode.value}")
        print(f"Topology: {outcome.plan.topology}")
        print(f"Duration: {self._feature_duration(outcome):.2f}s")
        print(
            "Components: "
            f"{counts[ComponentStatus.PASS]} passed, "
            f"{counts[ComponentStatus.FAIL]} failed, "
            f"{counts[ComponentStatus.SKIP]} skipped, "
            f"{counts[ComponentStatus.ERROR]} errors"
        )
        print(f"Evidence: {len(outcome.all_evidence)}")
        print(f"Summary: {outcome.summary}")

    def _print_feature_detailed_result(self, outcome: RunOutcome) -> None:
        self._print_feature_summary_result(outcome)
        results_by_id = {result.component_instance_id: result for result in outcome.component_results}

        print("\nComponent Results:")
        for component in outcome.plan.components:
            result = results_by_id.get(component.instance_id)
            if result is None:
                print(f"  [NOT_RUN] {component.instance_id} ({component.kind.value}: {component.component_id})")
                continue

            duration = 0.0
            if result.started_at is not None and result.ended_at is not None:
                duration = max(0.0, result.ended_at - result.started_at)
            print(
                f"  [{result.status.value}] {component.instance_id} "
                f"({component.kind.value}: {component.component_id}, {duration:.2f}s)"
            )
            print(f"    Summary: {result.summary}")
            print(f"    Evidence: {len(result.evidence)}")
            if result.diagnostics:
                diagnostics = yaml.safe_dump(
                    to_plain_data(result.diagnostics),
                    default_flow_style=False,
                    sort_keys=False,
                ).rstrip()
                print("    Diagnostics:")
                for line in diagnostics.splitlines():
                    print(f"      {line}")

        if outcome.diagnostics:
            diagnostics = yaml.safe_dump(
                to_plain_data(outcome.diagnostics),
                default_flow_style=False,
                sort_keys=False,
            ).rstrip()
            print("\nRun Diagnostics:")
            for line in diagnostics.splitlines():
                print(f"  {line}")

    def _print_header(self, title: str):
        """Print formatted header"""
        print()
        print("=" * 80)
        print(title)
        print("=" * 80)
        print()

    def _print_summary_result(self, result: ExecutionResult):
        """Print summary of test result"""
        time.sleep(0.1)

        status = "PASSED" if result.success else "FAILED"
        duration = result.end_time - result.start_time

        print(f"\nScenario: {result.scenario_id}")
        print(f"Status: {status}")
        print(f"Duration: {duration:.2f}s")
        print(f"Operations: {result.operations_executed}")
        print(f"Chaos Events: {len(result.chaos_events)}")

        if result.seed:
            print(f"Seed: {result.seed} (use to reproduce)")

        # Print final validation summary
        if result.final_validation:
            val = result.final_validation
            val_status = "PASSED" if val.overall_success else "FAILED"
            print(f"\nFinal Validation: {val_status}")

            if not val.overall_success and val.failed_checks:
                print(f"Failed Checks: {', '.join(val.failed_checks)}")

            if val.error_messages:
                print(f"Validation Errors: {len(val.error_messages)}")

        if result.error_message:
            print(f"Error: {result.error_message}")

    def _print_detailed_result(self, result: ExecutionResult):
        """Print detailed test result when --verbose flag is specified"""
        self._print_summary_result(result)

        # Print chaos events
        if result.chaos_events:
            print("\nChaos Events:")
            for event in result.chaos_events:
                status = "[PASS]" if event.success else "[FAIL]"
                duration = (event.end_time - event.start_time) if event.end_time else 0
                print(f"  {status} {event.chaos_type.value} on {event.target_node} ({duration:.2f}s)")
                if event.error_message:
                    print(f"    Error: {event.error_message}")

        # Print detailed validation results
        if result.final_validation:
            print("\nFinal Validation Details:")
            val = result.final_validation

            # Print individual check results
            checks = []
            if val.replication:
                status = "PASS" if val.replication.success else "FAIL"
                checks.append(f"  Replication: {status}")
                if not val.replication.success and val.replication.error_message:
                    checks.append(f"    → {val.replication.error_message}")

            if val.cluster_status:
                status = "PASS" if val.cluster_status.success else "FAIL"
                checks.append(f"  Cluster Status: {status}")
                if not val.cluster_status.success and val.cluster_status.error_message:
                    checks.append(f"    → {val.cluster_status.error_message}")

            if val.slot_coverage:
                status = "PASS" if val.slot_coverage.success else "FAIL"
                checks.append(f"  Slot Coverage: {status}")
                if not val.slot_coverage.success and val.slot_coverage.error_message:
                    checks.append(f"    → {val.slot_coverage.error_message}")

            if val.topology:
                status = "PASS" if val.topology.success else "FAIL"
                checks.append(f"  Topology: {status}")
                if not val.topology.success and val.topology.error_message:
                    checks.append(f"    → {val.topology.error_message}")

            if val.view_consistency:
                status = "PASS" if val.view_consistency.success else "FAIL"
                checks.append(f"  View Consistency: {status}")
                if not val.view_consistency.success and val.view_consistency.error_message:
                    checks.append(f"    → {val.view_consistency.error_message}")

            if val.data_consistency:
                status = "PASS" if val.data_consistency.success else "FAIL"
                checks.append(f"  Data Consistency: {status}")
                if not val.data_consistency.success and val.data_consistency.error_message:
                    checks.append(f"    → {val.data_consistency.error_message}")

            if val.log_validation:
                status = "PASS" if val.log_validation.success else "FAIL"
                checks.append(f"  Log Validation: {status}")
                if not val.log_validation.success:
                    error_count = len(
                        [f for f in val.log_validation.findings if hasattr(f, "severity") and f.severity == "error"]
                    )
                    warning_count = len(
                        [f for f in val.log_validation.findings if hasattr(f, "severity") and f.severity == "warning"]
                    )
                    checks.append(
                        f"    → {error_count} errors, {warning_count} warnings in {val.log_validation.shards_checked} shards"
                    )

            for check in checks:
                print(check)

            # Print all error messages if any
            if val.error_messages:
                print("\n  Validation Error Messages:")
                for msg in val.error_messages:
                    print(f"    → {msg}")

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
        print(f"Passed: {passed} ({passed / total * 100:.1f}%)")
        print(f"Failed: {failed} ({failed / total * 100:.1f}%)")
        print(f"Average Duration: {avg_duration:.2f}s")
        print(f"Average Operations: {avg_operations:.1f}")
        print(f"Average Chaos Events: {avg_chaos:.1f}")

    def _export_scenario_to_dsl(self, scenario, export_path: str):
        """Export scenario to DSL YAML file"""
        export_path = Path(export_path)
        export_path.parent.mkdir(parents=True, exist_ok=True)

        DSLLoader.save_scenario_as_dsl(scenario, export_path)
        print(f"\nScenario exported to DSL: {export_path}")

    @staticmethod
    def _feature_report(outcomes: tuple[RunOutcome, ...]) -> dict[str, Any]:
        statuses = tuple(outcome.status for outcome in outcomes)
        return {
            "report_api_version": FEATURE_REPORT_API_VERSION,
            "generated_at": datetime.now().astimezone().isoformat(),
            "summary": {
                "total": len(outcomes),
                "passed": statuses.count(OutcomeStatus.PASS),
                "failed": statuses.count(OutcomeStatus.FAIL),
                "errors": statuses.count(OutcomeStatus.ERROR),
                "skipped_unsupported": statuses.count(OutcomeStatus.SKIP_UNSUPPORTED),
            },
            "results": [outcome.to_data() for outcome in outcomes],
        }

    def _save_feature_outcomes(
        self,
        outcomes: tuple[RunOutcome, ...],
        output_path: str,
        format: str,
    ) -> None:
        """Atomically save generic outcomes; reporting failure fails the run."""
        if format not in {"json", "yaml"}:
            raise ValueError(f"Unsupported output format: {format}")

        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=output.parent,
                prefix=f".{output.name}.",
                suffix=".tmp",
                delete=False,
            ) as temporary:
                temporary_path = Path(temporary.name)
                report = self._feature_report(outcomes)
                if format == "json":
                    json.dump(report, temporary, indent=2, allow_nan=False)
                    temporary.write("\n")
                else:
                    yaml.safe_dump(
                        report,
                        temporary,
                        default_flow_style=False,
                        sort_keys=False,
                    )
                temporary.flush()
                os.fsync(temporary.fileno())
            os.replace(temporary_path, output)
        except BaseException:
            if temporary_path is not None:
                try:
                    temporary_path.unlink(missing_ok=True)
                except OSError:
                    pass
            raise

        print(f"\nResults saved to {output_path}")

    def _save_results(self, results: list, output_path: str, format: str):
        """Save test results to file"""
        try:
            output = Path(output_path)
            output.parent.mkdir(parents=True, exist_ok=True)

            # Convert results to dict
            data = {
                "timestamp": datetime.now().isoformat(),
                "total_tests": len(results),
                "passed": sum(1 for r in results if r.success),
                "failed": sum(1 for r in results if not r.success),
                "results": [self._result_to_dict(r) for r in results],
            }

            with open(output, "w") as f:
                if format == "json":
                    json.dump(data, f, indent=2)
                elif format == "yaml":
                    yaml.dump(data, f, default_flow_style=False)

            print(f"\nResults saved to {output_path}")

        except Exception as e:
            print(f"\nFailed to save results: {e}")

    def _result_to_dict(self, result: ExecutionResult) -> dict[str, Any]:
        """Convert ExecutionResult to dictionary"""
        data = {
            "scenario_id": result.scenario_id,
            "success": result.success,
            "duration": result.end_time - result.start_time,
            "operations_executed": result.operations_executed,
            "chaos_events": len(result.chaos_events),
            "seed": result.seed,
            "error_message": result.error_message,
            "valkey_sha": result.valkey_sha,
        }

        # Add final validation results if available
        if result.final_validation:
            data["final_validation"] = self._validation_to_dict(result.final_validation)

        # Add per-operation validation results if available
        if result.validation_results:
            data["per_operation_validations"] = [self._validation_to_dict(val) for val in result.validation_results]

        return data

    def _validation_to_dict(self, val: StateValidationResult) -> dict[str, Any]:
        """Convert StateValidationResult to dictionary"""
        validation_data = {
            "overall_success": val.overall_success,
            "failed_checks": val.failed_checks,
            "error_messages": val.error_messages,
            "duration": val.validation_duration,
            "timestamp": val.validation_timestamp,
            "checks": {},
        }

        # Add individual check results
        if val.replication:
            validation_data["checks"]["replication"] = {
                "success": val.replication.success,
                "all_replicas_synced": val.replication.all_replicas_synced,
                "max_lag": val.replication.max_lag,
                "lagging_replicas_count": len(val.replication.lagging_replicas),
                "disconnected_replicas_count": len(val.replication.disconnected_replicas),
                "error": val.replication.error_message,
            }

        if val.cluster_status:
            validation_data["checks"]["cluster_status"] = {
                "success": val.cluster_status.success,
                "cluster_state": val.cluster_status.cluster_state,
                "has_quorum": val.cluster_status.has_quorum,
                "nodes_in_fail_state_count": len(val.cluster_status.nodes_in_fail_state),
                "error": val.cluster_status.error_message,
            }

        if val.slot_coverage:
            validation_data["checks"]["slot_coverage"] = {
                "success": val.slot_coverage.success,
                "total_slots_assigned": val.slot_coverage.total_slots_assigned,
                "unassigned_slots_count": len(val.slot_coverage.unassigned_slots),
                "conflicting_slots_count": len(val.slot_coverage.conflicting_slots),
                "error": val.slot_coverage.error_message,
            }

        if val.topology:
            validation_data["checks"]["topology"] = {
                "success": val.topology.success,
                "expected_primaries": val.topology.expected_primaries,
                "actual_primaries": val.topology.actual_primaries,
                "expected_replicas": val.topology.expected_replicas,
                "actual_replicas": val.topology.actual_replicas,
                "mismatches_count": len(val.topology.topology_mismatches),
                "error": val.topology.error_message,
            }

        if val.view_consistency:
            validation_data["checks"]["view_consistency"] = {
                "success": val.view_consistency.success,
                "nodes_checked": val.view_consistency.nodes_checked,
                "consistent_views": val.view_consistency.consistent_views,
                "split_brain_detected": val.view_consistency.split_brain_detected,
                "consensus_percentage": val.view_consistency.consensus_percentage,
                "discrepancies_count": len(val.view_consistency.view_discrepancies),
                "error": val.view_consistency.error_message,
            }

        if val.data_consistency:
            validation_data["checks"]["data_consistency"] = {
                "success": val.data_consistency.success,
                "test_keys_checked": val.data_consistency.test_keys_checked,
                "missing_keys_count": len(val.data_consistency.missing_keys),
                "inconsistent_keys_count": len(val.data_consistency.inconsistent_keys),
                "unreachable_keys_count": len(val.data_consistency.unreachable_keys),
                "error": val.data_consistency.error_message,
            }

        if val.log_validation:
            validation_data["checks"]["log_validation"] = {
                "success": val.log_validation.success,
                "shards_checked": val.log_validation.shards_checked,
                "findings_count": len(val.log_validation.findings),
            }

        return validation_data


def _positive_seconds(value: str) -> float:
    try:
        seconds = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number greater than zero") from exc
    if not math.isfinite(seconds) or seconds <= 0:
        raise argparse.ArgumentTypeError("must be a finite number greater than zero")
    return seconds


def create_parser() -> argparse.ArgumentParser:
    """Create argument parser for CLI"""
    parser = argparse.ArgumentParser(
        prog="valkey-fuzzer",
        description="Valkey Fuzzer - Test standalone, replication, and cluster topologies",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run a topology scenario from DSL
  valkey-fuzzer run examples/standalone_scenario.yaml

  # Run a v3 feature pack from an explicit manifest directory
  valkey-fuzzer run scenario.yaml --feature-pack-dir ./feature-packs

  # Run a random cluster test
  valkey-fuzzer cluster --random

  # Run with specific seed for reproducibility
  valkey-fuzzer cluster --seed 42

  # Run multiple iterations
  valkey-fuzzer cluster --random --iterations 10

  # Run with configuration file and output results to another file
  valkey-fuzzer cluster --seed 42 --config config.yaml --output results.json

  # Verbose output
  valkey-fuzzer cluster --seed 42 --verbose

  # Run DSL-based test
  valkey-fuzzer cluster --dsl examples/simple_failover.yaml

  # Export scenario to DSL File
  valkey-fuzzer cluster --random --export-dsl dsl_file.yml

  # Validate DSL file
  valkey-fuzzer validate examples/simple_failover.yaml
        """,
    )

    parser.add_argument("--version", action="version", version="Valkey Fuzzer 0.1.0")

    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Generic topology scenario command
    run_parser = subparsers.add_parser("run", help="Run a standalone, replication, or cluster DSL scenario")
    run_parser.add_argument("file", help="Path to DSL YAML file")
    run_parser.add_argument("--valkey-binary", type=str, help="Path to the valkey-server binary to run")
    run_parser.add_argument(
        "--feature-pack-dir",
        dest="feature_pack_dirs",
        action="append",
        default=[],
        metavar="DIR",
        help="Directory containing v3 feature-pack manifests; may be repeated",
    )
    run_parser.add_argument(
        "--no-plugins",
        action="store_true",
        help="Disable trusted installed plugins from the valkey_fuzzer.plugins entry-point group",
    )
    run_parser.add_argument(
        "--artifact-dir",
        default="",
        metavar="DIR",
        help="Directory exposed to v3 components for run artifacts",
    )
    run_parser.add_argument(
        "--timeout",
        type=_positive_seconds,
        metavar="SECONDS",
        help="Cooperative v3 run deadline in seconds",
    )
    run_parser.add_argument(
        "--allow-unsupported",
        action="store_true",
        help="Return success when a required v3 capability reports unsupported",
    )
    run_parser.add_argument("--output", type=str, help="Path to save test results")
    run_parser.add_argument(
        "--format", choices=["json", "yaml"], default="json", help="Output format for results (default: json)"
    )
    run_parser.add_argument("--verbose", action="store_true", help="Enable verbose output")

    # Cluster test command
    cluster_parser = subparsers.add_parser("cluster", help="Run legacy cluster random or DSL tests")
    cluster_parser.add_argument("--random", action="store_true", help="Run with random scenario")
    cluster_parser.add_argument("--seed", type=int, help="Seed for reproducibility")
    cluster_parser.add_argument("--dsl", type=str, metavar="FILE", help="Path to DSL YAML file")
    cluster_parser.add_argument(
        "--iterations", type=int, default=1, help="Number of test iterations to run (default: 1)"
    )
    cluster_parser.add_argument("--config", type=str, help="Path to configuration file (YAML or JSON)")
    cluster_parser.add_argument("--valkey-binary", type=str, help="Path to the valkey-server binary to run")
    cluster_parser.add_argument("--output", type=str, help="Path to save test results")
    cluster_parser.add_argument(
        "--format", choices=["json", "yaml"], default="json", help="Output format for results (default: json)"
    )
    cluster_parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    cluster_parser.add_argument(
        "--export-dsl", type=str, metavar="FILE", help="Export the generated scenario to a DSL YAML file"
    )

    # Validate command
    validate_parser = subparsers.add_parser("validate", help="Validate a DSL configuration file")
    validate_parser.add_argument("file", help="Path to DSL YAML file")
    validate_parser.add_argument("--valkey-binary", type=str, help="Override the v3 topology's Valkey binary")
    validate_parser.add_argument(
        "--feature-pack-dir",
        dest="feature_pack_dirs",
        action="append",
        default=[],
        metavar="DIR",
        help="Directory containing v3 feature-pack manifests; may be repeated",
    )
    validate_parser.add_argument(
        "--no-plugins",
        action="store_true",
        help="Disable trusted installed plugins from the valkey_fuzzer.plugins entry-point group",
    )
    validate_parser.add_argument("--verbose", action="store_true", help="Enable verbose output")

    # Cleanup logs command
    cleanup_parser = subparsers.add_parser("cleanup-logs", help="Clean up log files for a specific seed")
    cleanup_parser.add_argument("seed", type=int, help="Seed value to clean up logs for")
    cleanup_parser.add_argument("--data-dir", type=str, help="Base data directory (default: /tmp/valkey-fuzzer)")
    cleanup_parser.add_argument("--force", action="store_true", help="Skip confirmation prompt")

    return parser


def main():
    """Main entry point for CLI"""

    logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[logging.StreamHandler()])

    parser = create_parser()
    args = parser.parse_args()

    if not args.command:
        print("Error: No command specified\n")
        parser.print_help()
        print("\nCommon commands:")
        print("  valkey-fuzzer run <file.yaml>            # Run a topology scenario")
        print("  valkey-fuzzer cluster --random          # Run random test")
        print("  valkey-fuzzer cluster --seed 42         # Run with specific seed")
        print("  valkey-fuzzer validate <file.yaml>      # Validate DSL file")
        return 1

    cli = FuzzerCLI()

    try:
        if args.command == "run":
            return cli.run_dsl_test(args)
        elif args.command == "cluster":
            # Validate cluster command arguments
            if not args.dsl and args.seed is None and not args.random:
                print("Error: cluster command requires either --dsl, --seed, or --random\n")
                print("Examples:")
                print("  valkey-fuzzer cluster --random")
                print("  valkey-fuzzer cluster --seed 42")
                print("  valkey-fuzzer cluster --dsl test.yaml")
                return 1

            if args.dsl:
                dsl_args = type(
                    "obj",
                    (object,),
                    {
                        "file": args.dsl,
                        "valkey_binary": args.valkey_binary,
                        "output": args.output,
                        "format": args.format,
                        "verbose": args.verbose,
                        "legacy_only": True,
                    },
                )
                return cli.run_dsl_test(dsl_args)
            else:
                return cli.run_random_test(args)
        elif args.command == "validate":
            return cli.validate_dsl(args)
        elif args.command == "cleanup-logs":
            return cleanup_logs(args.seed, args.data_dir or "/tmp/valkey-fuzzer", args.force)
    except KeyboardInterrupt:
        print("\n\nValkey Fuzzer process was interrupted by user")
        return 130
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        if hasattr(args, "verbose") and args.verbose:
            traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
