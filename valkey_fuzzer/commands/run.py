"""``run`` and ``cleanup-logs``.

``run`` executes one scenario file, or with ``--random``/``--seed`` one
scenario generated for any experiment that declares ``metadata.generator``
(see :mod:`valkey_fuzzer.generator`), ``valkey.cluster-chaos`` included; the PR
fuzzer workflow runs it. Many runs go through ``batch``. ``cleanup-logs``
removes the local deployment's logs for a seed. The run leaves a report folder
under ``runs/``, and ``--output`` saves a copy of its run report.
"""

import argparse
import glob
import os
import shlex
import sys
from typing import Any, Optional

from ..compiler import Overrides
from ..deployment.local.models import DEFAULT_BASE_DATA_DIR
from ..hoststate import Owner, read_tag
from ..outcomes import EXIT_FAILURE, EXIT_HARNESS_ERROR, EXIT_SUCCESS, EXIT_USAGE, exit_code, print_outcome
from ..reports import SCENARIO_FILE, ReportWriteError, export_scenario, replay_command, write_document
from ..results import OutcomeStatus
from ..service import PreparedRun
from ._common import add_common_options, artifact_paths, exclusion, print_header, service_for


def register(subparsers: Any) -> None:
    run_parser = subparsers.add_parser("run", help="Compile and run a scenario, or generated ones")
    run_parser.add_argument("file", nargs="?", help="Path to the scenario YAML file (omit with --random or --seed)")
    add_common_options(
        run_parser,
        valkey_binary=True,
        artifact=True,
        base_port=True,
        artifact_dir=True,
        timeout=True,
        allow_unsupported=True,
    )
    generated = run_parser.add_argument_group("generated scenarios")
    generated.add_argument("--random", action="store_true", help="Run a scenario generated from a random seed")
    generated.add_argument("--seed", type=int, help="Run the scenario generated from this seed")
    generated.add_argument(
        "--experiment", metavar="ID", help="Experiment to generate for (default: drawn from those that opt in)"
    )
    generated.add_argument(
        "--exclude",
        dest="excludes",
        action="append",
        default=[],
        type=exclusion,
        metavar="PATH=VALUE",
        help="Never generate VALUE at PATH (inputs.<name>, deployment.<field>, server_config.<key>, or "
        "experiment); may be repeated",
    )
    generated.add_argument(
        "--base", metavar="FILE", help="Partial scenario whose fields override the draws (a second build, say)"
    )
    generated.add_argument(
        "--profile", metavar="NAME", help="Search profile that samples the experiment's legal space (pr-quick, ...)"
    )
    generated.add_argument(
        "--export-scenario", metavar="FILE", help="Write the generated scenario to FILE; `run FILE` reruns it"
    )
    run_parser.add_argument("--output", help="Path to save a copy of the run report")
    run_parser.add_argument(
        "--output-format", choices=["json", "yaml"], help="Format of the --output copy (default: json)"
    )
    run_parser.add_argument("--verbose", action="store_true", help="Show component-level details")
    run_parser.set_defaults(handler=run_command)

    cleanup_parser = subparsers.add_parser("cleanup-logs", help="Clean up log files for a seed")
    cleanup_parser.add_argument("seed", type=int, help="Seed whose logs should be removed")
    cleanup_parser.add_argument(
        "--data-dir",
        default=DEFAULT_BASE_DATA_DIR,
        help="Base data directory (default: %(default)s)",
    )
    cleanup_parser.add_argument("--force", action="store_true", help="Skip confirmation")
    cleanup_parser.set_defaults(handler=cleanup_logs_command)


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def run_command(args: argparse.Namespace) -> int:
    """Compile and execute one scenario (``valkey-fuzzer run``), or a generated one with ``--random``/``--seed``."""

    generating = args.random or args.seed is not None
    misplaced = [
        flag
        for flag, value in (
            ("--experiment", args.experiment),
            ("--exclude", args.excludes),
            ("--base", args.base),
            ("--profile", args.profile),
            ("--export-scenario", args.export_scenario),
        )
        if value
    ]
    if args.output_format and not args.output:
        reason = "--output-format needs --output"
    elif generating and args.file:
        reason = "give a scenario file or --random/--seed, not both"
    elif not generating and misplaced:
        reason = f"{', '.join(misplaced)} need --random or --seed"
    elif generating and args.artifacts:
        reason = "--artifact needs a scenario file"
    elif not generating and not args.file:
        reason = "run needs a scenario file"
    else:
        reason = None
    if reason is not None:
        print(f"Error: {reason}\n", file=sys.stderr)
        print("Examples:", file=sys.stderr)
        print("  valkey-fuzzer run scenario.yaml", file=sys.stderr)
        print("  valkey-fuzzer run --random", file=sys.stderr)
        print("  valkey-fuzzer run --seed 42 --experiment valkey.aof-durability", file=sys.stderr)
        return EXIT_USAGE

    service = service_for(args)
    if generating:
        base = service.read_base(args.base) if args.base else None
        prepared = service.prepare_random(
            seed=args.seed,
            experiment=args.experiment,
            excludes=args.excludes,
            base=base,
            profile=args.profile,
            overrides=Overrides(binary=args.valkey_binary, base_port=args.base_port),
        )
        generated = prepared.generated
        print_header(
            f"Generated scenario: {generated['experiment']}, seed {generated['seed']} "
            f"(attempt {generated['attempt']}, cores {_cores(generated['cores'])})"
        )
        for note in prepared.notes:
            print(f"Note: {note}")
    else:
        print_header(f"Scenario: {args.file}")
        # The report records the scenario as run, including a --valkey-binary
        # or --artifact override, so it replays and minimizes on its own.
        prepared = service.prepare_run(
            args.file,
            overrides=Overrides(binary=args.valkey_binary, artifacts=artifact_paths(args), base_port=args.base_port),
        )
    timeout, source = service.resolve_timeout(
        prepared.plan, args.timeout, profile=(prepared.generated or {}).get("profile")
    )
    print(f"Timeout: {timeout:g}s ({source})")
    record = service.execute(prepared.plan, scenario=prepared.scenario, timeout=timeout, generated=prepared.generated)
    print_outcome(record.outcome, verbose=args.verbose)
    print(f"Report: {record.report_path}")
    replay = replay_command(record.report_path)
    if generating:
        print(f"Reproduce: {_reproduce_command(prepared, args)}")
        print(f"  or, without the generator: valkey-fuzzer run {record.report_dir}/{SCENARIO_FILE}")
        print(f"  or, replay this exact run: {replay}")
    else:
        print(f"Reproduce: {replay}")
    if record.outcome.status in (OutcomeStatus.FAIL, OutcomeStatus.ERROR):
        print(
            f"Look at: {record.report_dir} (plan.json, components/), report.json keys failed_components and reproduce"
        )
        print(f"  server logs: {DEFAULT_BASE_DATA_DIR}/logs/ (data directories are deleted after the run)")
    code = exit_code(record.outcome.status, allow_unsupported=args.allow_unsupported)
    try:
        if args.output:
            write_document(args.output, record.report, args.output_format or "json")
            print(f"\nResults saved to {args.output}")
        if args.export_scenario:
            export_scenario(prepared.scenario, args.export_scenario, generated=prepared.generated)
            print(f"\nScenario exported to {args.export_scenario}")
    except ReportWriteError as exc:
        # A file that cannot be written is an ERROR, unless the run already FAILed.
        print(f"\nError: {exc}", file=sys.stderr)
        return code if code == EXIT_FAILURE else EXIT_HARNESS_ERROR
    return code


def _cores(cores: Optional[float]) -> str:
    """The cores a draw had to fit, as the header says them: ``any`` when it had no limit."""

    return "any" if cores is None else f"{cores:g}"


def _reproduce_command(prepared: PreparedRun, args: argparse.Namespace) -> str:
    """A shell command that draws the same scenario: every option a draw depends on, quoted."""

    generated = prepared.generated
    command = ["valkey-fuzzer", "run", "--seed", str(generated["seed"]), "--experiment", generated["experiment"]]
    for item in generated.get("excludes", ()):
        command += ["--exclude", str(item)]
    if generated.get("profile"):
        command += ["--profile", generated["profile"]]
    if args.base:
        command += ["--base", args.base]
    if args.base_port is not None:
        command += ["--base-port", str(args.base_port)]
    # Hooks see the binary (rolling-upgrade names the second build after it), and the catalog decides what exists.
    if args.valkey_binary:
        command += ["--valkey-binary", args.valkey_binary]
    for directory in args.experiment_dirs:
        command += ["--experiment-dir", directory]
    return shlex.join(command)


# ---------------------------------------------------------------------------
# cleanup-logs
# ---------------------------------------------------------------------------


def _live_owner(base_data_dir: str, log_file: str) -> Optional[str]:
    """The owner token if ``log_file``'s deployment is tagged with a live owner, else None.

    The log is ``<seed>-<deployment id>-<instance>.log``; its deployment directory is
    ``<base_data_dir>/deployment-<id>``. shortcut: a deployment under a separate data root is not found, so its
    logs count as dead; upgrade when logs record their deployment directory.
    """

    parts = os.path.basename(log_file).split("-", 2)
    tag = read_tag(os.path.join(base_data_dir, f"deployment-{parts[1]}")) if len(parts) == 3 else None
    owner = Owner.parse(tag["owner"]) if tag else None
    if owner is not None and isinstance(tag.get("pid_ns"), str):
        owner = Owner(owner.pid, owner.start, tag["pid_ns"])
    return owner.token if owner is not None and owner.alive() else None


def cleanup_logs(seed: int, base_data_dir: str, force: bool = False) -> int:
    """Delete ``<seed>-*.log`` under ``base_data_dir/logs``, asking first unless ``force``.

    A cancelled prompt or a file that could not be deleted is a harness error
    (exit 2): a script that cleans up after a seed must not take either for
    done.
    """

    log_files = sorted(glob.glob(os.path.join(base_data_dir, "logs", f"{seed}-*.log")))
    live = {log_file: _live_owner(base_data_dir, log_file) for log_file in log_files}
    for log_file, owner in live.items():
        if owner:
            print(f"Skipped {os.path.basename(log_file)}: its deployment is still running (owner {owner})")
    log_files = [log_file for log_file in log_files if not live[log_file]]
    if not log_files:
        print(f"No log files found for seed {seed}")
        return EXIT_SUCCESS
    for log_file in log_files:
        print(f"  {os.path.basename(log_file)}")
    if not force and input(f"\nDelete these {len(log_files)} log file(s)? [y/N]: ").lower() != "y":
        print("Cancelled; nothing deleted")
        return EXIT_HARNESS_ERROR

    failed = 0
    for log_file in log_files:
        try:
            os.remove(log_file)
        except OSError as exc:
            failed += 1
            print(f"Error: could not delete {log_file}: {exc}", file=sys.stderr)
    print(f"Deleted {len(log_files) - failed} log file(s) for seed {seed}")
    return EXIT_HARNESS_ERROR if failed else EXIT_SUCCESS


def cleanup_logs_command(args: argparse.Namespace) -> int:
    """Remove the local deployment's log files for a seed (``valkey-fuzzer cleanup-logs``)."""

    return cleanup_logs(args.seed, args.data_dir, args.force)
