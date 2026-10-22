#!/usr/bin/env python3
"""Command-line interface for generic, feature-pack-driven Valkey fuzzing."""

import argparse
import json
import logging
import math
import os
import sys
import tempfile
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import yaml

from .extensibility import ComponentStatus, OutcomeStatus, RunOutcome, RunPlan, to_plain_data
from .feature_runtime import FeatureRuntime
from .utils.cluster_utils import cleanup_logs

EXIT_SUCCESS = 0
EXIT_FEATURE_FAILURE = 1
EXIT_HARNESS_ERROR = 2
EXIT_UNSUPPORTED = 3


class FuzzerCLI:
    """Small public facade over the generic compiler, runtime, and runner."""

    @staticmethod
    def _runtime(args: argparse.Namespace) -> FeatureRuntime:
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
    def _topology_overrides(args: argparse.Namespace) -> Optional[dict[str, Any]]:
        valkey_binary = getattr(args, "valkey_binary", None)
        if isinstance(valkey_binary, str) and valkey_binary:
            return {"valkey_binary": valkey_binary}
        return None

    @staticmethod
    def _read_scenario(path: Path) -> str:
        if not path.is_file():
            raise FileNotFoundError(f"scenario file not found: {path}")
        try:
            return path.read_text(encoding="utf-8")
        except OSError as exc:
            raise OSError(f"unable to read scenario file {path}: {exc}") from exc

    def run_scenario(self, args: argparse.Namespace) -> int:
        """Compile and execute one generic scenario."""

        self._print_header(f"Scenario: {args.file}")
        path = Path(args.file)

        try:
            source = self._read_scenario(path)
            runtime = self._runtime(args)
            plan = runtime.compile_yaml(
                source,
                source_name=str(path),
                topology_overrides=self._topology_overrides(args),
            )

            timeout = getattr(args, "timeout", None)
            deadline = time.monotonic() + timeout if timeout is not None else None
            outcome = runtime.run_plan(plan, deadline=deadline)

            print(f"Loaded scenario from {path}")
            if getattr(args, "verbose", False):
                self._print_detailed_result(outcome)
            else:
                self._print_summary_result(outcome)

            output_path = getattr(args, "output", None)
            if output_path:
                self._save_outcomes((outcome,), output_path, getattr(args, "format", "json"))

            return self._exit_code(outcome, allow_unsupported=getattr(args, "allow_unsupported", False))
        except Exception as exc:
            print(f"Error: scenario run failed: {exc}")
            print(f"\nValidate it first with: valkey-fuzzer validate {args.file}")
            if getattr(args, "verbose", False):
                traceback.print_exc()
            return EXIT_HARNESS_ERROR

    def validate_scenario(self, args: argparse.Namespace) -> int:
        """Compile and structurally validate one generic scenario."""

        self._print_header(f"Validating scenario: {args.file}")
        path = Path(args.file)

        try:
            source = self._read_scenario(path)
            runtime = self._runtime(args)
            plan = runtime.compile_yaml(
                source,
                source_name=str(path),
                topology_overrides=self._topology_overrides(args),
            )
            runtime.validate_plan(plan)

            print("Scenario compiled successfully")
            print("Structural topology preflight completed without provisioning")
            print("Component factories were not instantiated; Valkey binary availability was not checked")
            self._print_plan_summary(plan, verbose=getattr(args, "verbose", False))
            print("\nScenario passed structural validation!")
            return EXIT_SUCCESS
        except Exception as exc:
            print(f"Error: scenario validation failed: {exc}")
            if getattr(args, "verbose", False):
                traceback.print_exc()
            return EXIT_HARNESS_ERROR

    @staticmethod
    def _exit_code(outcome: RunOutcome, *, allow_unsupported: bool) -> int:
        if outcome.status is OutcomeStatus.PASS:
            return EXIT_SUCCESS
        if outcome.status is OutcomeStatus.FAIL:
            return EXIT_FEATURE_FAILURE
        if outcome.status is OutcomeStatus.SKIP_UNSUPPORTED:
            return EXIT_SUCCESS if allow_unsupported else EXIT_UNSUPPORTED
        return EXIT_HARNESS_ERROR

    @staticmethod
    def _duration(outcome: RunOutcome) -> float:
        if outcome.started_at is None or outcome.ended_at is None:
            return 0.0
        return max(0.0, outcome.ended_at - outcome.started_at)

    @staticmethod
    def _component_status_counts(outcome: RunOutcome) -> dict[ComponentStatus, int]:
        counts = {status: 0 for status in ComponentStatus}
        for result in outcome.component_results:
            counts[result.status] += 1
        return counts

    def _print_plan_summary(self, plan: RunPlan, *, verbose: bool = False) -> None:
        print("\n" + "=" * 60)
        print("Execution Plan")
        print("=" * 60)
        print(f"ID: {plan.scenario_id}")
        print(f"Feature Pack: {plan.pack_id}@{plan.pack_version}")
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

    def _print_summary_result(self, outcome: RunOutcome) -> None:
        counts = self._component_status_counts(outcome)
        print(f"\nScenario: {outcome.plan.scenario_id}")
        print(f"Feature Pack: {outcome.plan.pack_id}@{outcome.plan.pack_version}")
        print(f"Status: {outcome.status.value}")
        print(f"Mode: {outcome.plan.mode.value}")
        print(f"Topology: {outcome.plan.topology}")
        print(f"Duration: {self._duration(outcome):.2f}s")
        print(
            "Components: "
            f"{counts[ComponentStatus.PASS]} passed, "
            f"{counts[ComponentStatus.FAIL]} failed, "
            f"{counts[ComponentStatus.SKIP]} skipped, "
            f"{counts[ComponentStatus.ERROR]} errors"
        )
        print(f"Evidence: {len(outcome.all_evidence)}")
        print(f"Summary: {outcome.summary}")

    def _print_detailed_result(self, outcome: RunOutcome) -> None:
        self._print_summary_result(outcome)
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

    @staticmethod
    def _print_header(title: str) -> None:
        print()
        print("=" * 80)
        print(title)
        print("=" * 80)
        print()

    @staticmethod
    def _report(outcomes: tuple[RunOutcome, ...]) -> dict[str, Any]:
        statuses = tuple(outcome.status for outcome in outcomes)
        return {
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

    def _save_outcomes(
        self,
        outcomes: tuple[RunOutcome, ...],
        output_path: str,
        output_format: str,
    ) -> None:
        """Atomically save complete generic outcomes."""

        if output_format not in {"json", "yaml"}:
            raise ValueError(f"unsupported output format: {output_format}")

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
                report = self._report(outcomes)
                if output_format == "json":
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


def _positive_seconds(value: str) -> float:
    try:
        seconds = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number greater than zero") from exc
    if not math.isfinite(seconds) or seconds <= 0:
        raise argparse.ArgumentTypeError("must be a finite number greater than zero")
    return seconds


def _add_runtime_options(parser: argparse.ArgumentParser, *, execution: bool) -> None:
    parser.add_argument("--valkey-binary", help="Override the Valkey binary used by the topology")
    parser.add_argument(
        "--feature-pack-dir",
        dest="feature_pack_dirs",
        action="append",
        default=[],
        metavar="DIR",
        help="Directory containing feature-pack manifests; may be repeated",
    )
    parser.add_argument(
        "--no-plugins",
        action="store_true",
        help="Disable trusted plugins from the valkey_fuzzer.plugins entry-point group",
    )
    parser.add_argument(
        "--artifact-dir",
        default="",
        metavar="DIR",
        help="Directory exposed to components for run artifacts",
    )
    if execution:
        parser.add_argument(
            "--timeout",
            type=_positive_seconds,
            metavar="SECONDS",
            help="Cooperative run deadline in seconds",
        )
        parser.add_argument(
            "--allow-unsupported",
            action="store_true",
            help="Return success when a required capability reports unsupported",
        )


def create_parser() -> argparse.ArgumentParser:
    """Create the public command-line parser."""

    parser = argparse.ArgumentParser(
        prog="valkey-fuzzer",
        description="Feature-pack-driven fuzzing for standalone, replication, and cluster Valkey topologies",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  valkey-fuzzer run scenario.yaml --feature-pack-dir ./feature-packs
  valkey-fuzzer run scenario.yaml --timeout 120 --output results.json
  valkey-fuzzer validate scenario.yaml --feature-pack-dir ./feature-packs
  valkey-fuzzer cleanup-logs 12345 --force
        """,
    )
    parser.add_argument("--version", action="version", version="Valkey Fuzzer 0.1.0")
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    run_parser = subparsers.add_parser("run", help="Compile and run a generic feature scenario")
    run_parser.add_argument("file", help="Path to the scenario YAML file")
    _add_runtime_options(run_parser, execution=True)
    run_parser.add_argument("--output", help="Path to save complete run results")
    run_parser.add_argument(
        "--format",
        choices=["json", "yaml"],
        default="json",
        help="Output format for results (default: json)",
    )
    run_parser.add_argument("--verbose", action="store_true", help="Show component-level details")

    validate_parser = subparsers.add_parser("validate", help="Compile and structurally validate a scenario")
    validate_parser.add_argument("file", help="Path to the scenario YAML file")
    _add_runtime_options(validate_parser, execution=False)
    validate_parser.add_argument("--verbose", action="store_true", help="Show the resolved component plan")

    cleanup_parser = subparsers.add_parser("cleanup-logs", help="Clean up log files for a seed")
    cleanup_parser.add_argument("seed", type=int, help="Seed whose logs should be removed")
    cleanup_parser.add_argument("--data-dir", help="Base data directory (default: /tmp/valkey-fuzzer)")
    cleanup_parser.add_argument("--force", action="store_true", help="Skip confirmation")

    return parser


def main() -> int:
    """CLI entry point."""

    logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[logging.StreamHandler()])
    parser = create_parser()
    args = parser.parse_args()

    if not args.command:
        print("Error: No command specified\n")
        parser.print_help()
        return EXIT_FEATURE_FAILURE

    cli = FuzzerCLI()
    try:
        if args.command == "run":
            return cli.run_scenario(args)
        if args.command == "validate":
            return cli.validate_scenario(args)
        if args.command == "cleanup-logs":
            return cleanup_logs(args.seed, args.data_dir or "/tmp/valkey-fuzzer", args.force)
    except KeyboardInterrupt:
        print("\n\nValkey Fuzzer process was interrupted by user")
        return 130
    except Exception as exc:
        print(f"\nUnexpected harness error: {exc}")
        if getattr(args, "verbose", False):
            traceback.print_exc()
        return EXIT_HARNESS_ERROR

    return EXIT_SUCCESS


if __name__ == "__main__":
    sys.exit(main())
