"""``replay``: re-execute a recorded run, recording and warning about drift from its provenance."""

import argparse
import sys
from typing import Any

from ..compiler import Overrides
from ..outcomes import EXIT_FAILURE, EXIT_HARNESS_ERROR, exit_code
from ..reports import ReportWriteError, write_document
from ._common import add_common_options, artifact_paths, describe_drift, service_for


def register(subparsers: Any) -> None:
    replay = subparsers.add_parser("replay", help="Re-execute a recorded run (report.json or plan.json)")
    replay.add_argument("plan", help="Path to a run report (report.json or a --output copy) or a plan.json")
    replay.add_argument("--output", help="Path to save the replay report")
    replay.add_argument("--verbose", action="store_true")
    add_common_options(
        replay, valkey_binary=True, artifact=True, artifact_dir=True, timeout=True, allow_unsupported=True
    )
    replay.set_defaults(handler=replay_command)


def replay_command(args: argparse.Namespace) -> int:
    """Check a recorded run against this environment, then run its plan again."""

    service = service_for(args)
    artifacts = artifact_paths(args)
    prepared = service.prepare_replay(args.plan, overrides=Overrides(binary=args.valkey_binary, artifacts=artifacts))
    if not prepared.verified:
        print(
            f"Warning: {args.plan} records no provenance, so the replay cannot be verified against the original run",
            file=sys.stderr,
        )
    for entry in prepared.drift:
        print(f"Warning: drift from the recorded run: {describe_drift(entry)}", file=sys.stderr)
    for note in prepared.not_verified:
        print(f"Note: not verified: {note}")
    if args.valkey_binary:
        # An intentional override: recorded in the report, not treated as drift.
        print(f"Using --valkey-binary {args.valkey_binary} instead of the recorded default binary")
    for name, path in artifacts.items():
        # Recorded in the report; the binary's digest is still compared after the run.
        print(f"Using --artifact {name}={path} instead of the recorded path")
    plan = prepared.plan
    print(f"Replaying {plan.scenario_id} ({plan.experiment_id}, seed {plan.seed})")

    result = service.replay(prepared, timeout=args.timeout)
    record = result.record
    print(f"Status: {record.outcome.status.value}")
    print(f"Summary: {record.outcome.summary}")
    print(f"Report: {record.report_path}")
    code = exit_code(record.outcome.status, allow_unsupported=args.allow_unsupported)
    for entry in result.late_drift:
        print(f"Warning: drift found by running: {describe_drift(entry)}", file=sys.stderr)
    if args.output:
        try:
            write_document(args.output, record.report)
        except ReportWriteError as exc:
            # A copy that cannot be written is an ERROR, unless the replay already FAILed.
            print(f"Error: {exc}", file=sys.stderr)
            return code if code == EXIT_FAILURE else EXIT_HARNESS_ERROR
    return code
