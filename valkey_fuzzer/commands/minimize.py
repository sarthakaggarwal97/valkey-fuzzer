"""``minimize``: shrink a failing run's scenario while its failure signature holds."""

import argparse
import json
import sys
from typing import Any

import yaml

from ..compiler import Overrides
from ..minimize import signature_key
from ..outcomes import EXIT_HARNESS_ERROR, EXIT_SUCCESS
from ..reports import write_document
from ._common import add_common_options, describe_drift, positive_count, service_for


def register(subparsers: Any) -> None:
    minimize = subparsers.add_parser("minimize", help="Shrink a failing run while preserving the failure")
    minimize.add_argument("report", help="Run report (report.json or a --output copy) of the failing run")
    minimize.add_argument("--output", help="Write the minimized scenario YAML here")
    minimize.add_argument("--max-attempts", type=positive_count, default=30, help="Candidates to run (default 30)")
    minimize.add_argument("--verbose", action="store_true")
    add_common_options(minimize, valkey_binary=True, artifact_dir=True, timeout=True)
    minimize.set_defaults(handler=minimize_command)


def minimize_command(args: argparse.Namespace) -> int:
    """Shrink a failing run's scenario while the same failure persists, and print or write the result."""

    service = service_for(args)
    prepared = service.prepare_minimization(args.report, overrides=Overrides(binary=args.valkey_binary))
    for entry in prepared.drift:
        print(f"Warning: drift from the recorded run: {describe_drift(entry)}", file=sys.stderr)
    if not prepared.needed:
        print(f"Nothing to minimize: the run's status is {prepared.reference['status']}")
        return EXIT_SUCCESS
    # The same key a batch's summary.json and findings.json give this failure.
    key = signature_key(prepared.plan.experiment_id, prepared.reference)
    print(f"Minimizing {prepared.plan.scenario_id}: signature {key} {json.dumps(prepared.reference)}")
    result = service.minimize(prepared, timeout=args.timeout, max_attempts=args.max_attempts, log=print)
    if result.report_dir:
        print(f"Reports: {result.report_dir}")
    if not result.reproduced:
        print("Error: the failure did not reproduce, so nothing was minimized", file=sys.stderr)
        return EXIT_HARNESS_ERROR
    print(f"Accepted reductions: {', '.join(result.accepted) or 'none'}")
    if args.output:
        write_document(args.output, result.scenario, "yaml")
        print(f"Minimized scenario written to {args.output}")
    else:
        print(yaml.safe_dump(result.scenario, sort_keys=False))
    if not result.accepted:
        print("Error: no reduction kept the failure, so the scenario is not smaller", file=sys.stderr)
        return EXIT_HARNESS_ERROR
    if not result.confirmed:
        print("Error: the minimized scenario did not fail the same way when run again", file=sys.stderr)
        return EXIT_HARNESS_ERROR
    return EXIT_SUCCESS
