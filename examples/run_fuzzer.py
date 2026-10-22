#!/usr/bin/env python3
"""Use the generic feature runtime without going through the CLI."""

import argparse
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from src.extensibility import OutcomeStatus  # noqa: E402
from src.feature_runtime import FeatureRuntime  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("validate", "run"))
    parser.add_argument("scenario", type=Path)
    parser.add_argument(
        "--feature-pack-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "feature-packs",
    )
    parser.add_argument("--valkey-binary")
    parser.add_argument(
        "--enable-plugins",
        action="store_true",
        help="Load trusted installed valkey_fuzzer.plugins entry points",
    )
    args = parser.parse_args()

    runtime = FeatureRuntime(
        feature_pack_directories=(args.feature_pack_dir,),
        discover_entry_points=args.enable_plugins,
    )
    topology_overrides = {"valkey_binary": args.valkey_binary} if args.valkey_binary else None
    plan = runtime.compile_yaml(
        args.scenario.read_text(encoding="utf-8"),
        source_name=str(args.scenario),
        topology_overrides=topology_overrides,
    )

    if args.command == "validate":
        runtime.validate_plan(plan)
        print(plan.to_json())
        return 0

    outcome = runtime.run_plan(plan)
    print(outcome.to_json())
    return 0 if outcome.status is OutcomeStatus.PASS else 1


if __name__ == "__main__":
    raise SystemExit(main())
