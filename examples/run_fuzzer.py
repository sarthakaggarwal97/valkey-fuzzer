#!/usr/bin/env python3
"""Compile, validate, and run a scenario with the run service, without going through the CLI."""

import argparse
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from valkey_fuzzer.contracts import OutcomeStatus  # noqa: E402
from valkey_fuzzer.schema import load_yaml_mapping  # noqa: E402
from valkey_fuzzer.service import RunService  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("validate", "run"))
    parser.add_argument("scenario", type=Path)
    parser.add_argument(
        "--experiment-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "experiments",
    )
    parser.add_argument("--valkey-binary")
    parser.add_argument(
        "--enable-plugins",
        action="store_true",
        help="Load trusted installed valkey_fuzzer.plugins entry points",
    )
    args = parser.parse_args()

    service = RunService(experiment_directories=(args.experiment_dir,), discover_plugins=args.enable_plugins)
    scenario = dict(load_yaml_mapping(args.scenario.read_text(encoding="utf-8"), source_name=str(args.scenario)))
    if args.valkey_binary:
        scenario["deployment"] = {**scenario["deployment"], "valkey_binary": args.valkey_binary}
    plan = service.compile(scenario)

    if args.command == "validate":
        service.validate_plan(plan)
        print(plan.to_json())
        return 0

    outcome = service.run_plan(plan)
    print(outcome.to_json())
    return 0 if outcome.status is OutcomeStatus.PASS else 1


if __name__ == "__main__":
    raise SystemExit(main())
