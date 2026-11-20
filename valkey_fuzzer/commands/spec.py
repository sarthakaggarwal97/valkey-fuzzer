"""``spec``: check a TLA+ bundle, translate its counterexamples into scenarios, and conform real runs to it.

Without java and ``tla2tools.jar`` a command that needs TLC prints :data:`~valkey_fuzzer.spec.tla.TOOLS_HINT` and
exits 2.
"""

import argparse
import json
import os
import sys
from collections.abc import Mapping
from typing import Any, Optional

import yaml

from ..outcomes import EXIT_FAILURE, EXIT_HARNESS_ERROR, EXIT_SUCCESS
from ..reports import write_document
from ..service import CONFORM_TLC_TIMEOUT_S
from ..spec.bundle import SPEC_PATH_VARIABLE, Bundle, SpecBundleError, load_bundle
from ..spec.check import check_bundle, run_control
from ..spec.conform import TRANSITIONS_UNCHECKED, collect_runs, conform_runs, needs_tlc
from ..spec.tla import TOOLS_HINT, TlaError, TlaTools, config_model_values, read_tlc_output
from ..spec.translate import TranslateError, bundle_service, search_model, translate_trace
from ._common import add_common_options, positive_count, positive_seconds

# How long one spec command may run SANY and TLC, unless --timeout says otherwise.
DEFAULT_TIMEOUT_S = 3600.0
# How many scenarios one model search writes at most, unless --max-scenarios says otherwise.
DEFAULT_MAX_SCENARIOS = 50


def register(subparsers: Any) -> None:
    spec = subparsers.add_parser(
        "spec",
        help="Check a TLA+ spec bundle, translate its counterexamples, and conform runs to it",
        description=f"A spec bundle is a directory, or a protocol name looked up in ${SPEC_PATH_VARIABLE} and then "
        "the bundles shipped in formal/; see docs/spec-bridge.md.",
    )
    tools = spec.add_subparsers(dest="spec_command", required=True)

    check = tools.add_parser("check", help="SANY, TLC on the safe configs and negative controls, model coverage")
    add_common_options(check)
    _bundle_options(check)
    check.add_argument("--require-approved", action="store_true", help="Fail unless the claims are approved")
    check.add_argument(
        "--configs",
        metavar="DIR",
        help="Check the safe configs in this subdirectory of the bundle instead of its own; `nightly` holds the "
        "bounds raised past what PR CI checks in time",
    )
    check.set_defaults(handler=check_command)

    translate = tools.add_parser(
        "translate",
        help="Turn TLC counterexamples into fuzzer scenarios",
        description="Translate one counterexample (--control or --trace), or search the model for them (--reach, "
        "--mutate): each distinct counterexample, by its action sequence, becomes a scenario and its variants.",
    )
    add_common_options(translate)
    _bundle_options(translate)
    source = translate.add_mutually_exclusive_group()
    source.add_argument("--control", help="Run TLC on this negative control and translate its counterexample")
    source.add_argument(
        "--trace", help="TLC output, or its -dumpTrace json file, holding a counterexample (needs --config)"
    )
    translate.add_argument("--config", help="The .cfg TLC ran with --trace")
    translate.add_argument(
        "--reach",
        action="append",
        default=[],
        metavar="TARGET",
        help="Find a path of each safe config to this target of claims.yaml and translate it (TLC checks ~TARGET); "
        "may be repeated, 'all' for every target",
    )
    translate.add_argument(
        "--mutate",
        action="store_true",
        help="Run TLC on each safe config with each boolean constant flipped (and each pair, when the single flips "
        "were cheap) and translate every violation found",
    )
    translate.add_argument(
        "--tlc-timeout",
        type=positive_seconds,
        default=CONFORM_TLC_TIMEOUT_S,
        metavar="SECONDS",
        help="How long each TLC run of --reach or --mutate may take; one that runs out is recorded and the search "
        f"goes on (default {CONFORM_TLC_TIMEOUT_S:g}, as a batch's conform_tlc_timeout_s)",
    )
    translate.add_argument(
        "--max-scenarios",
        type=positive_count,
        default=DEFAULT_MAX_SCENARIOS,
        metavar="N",
        help=f"Stop the search once it has written this many scenarios (default {DEFAULT_MAX_SCENARIOS})",
    )
    translate.add_argument("--variants", type=int, default=4, help="Nearby timing variants to add (default 4)")
    translate.add_argument("--seed", type=int, default=1, help="Seed of the first scenario (default 1)")
    translate.add_argument("--out", required=True, help="Directory to write the scenarios and translation.json to")
    translate.set_defaults(handler=translate_command)

    conform = tools.add_parser("conform", help="Check the claims on states observed in real runs")
    _bundle_options(conform)
    conform.add_argument(
        "runs", nargs="+", help="Run report folders, report.json files, directories, or trace.jsonl files"
    )
    conform.add_argument("--group-by", default="", help="Comma-separated scenario inputs to tabulate runs by")
    conform.add_argument(
        "--no-transitions",
        action="store_true",
        help="Check only the properties on each observed state, not that the model's Next relates consecutive ones",
    )
    conform.set_defaults(handler=conform_command)


def _bundle_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("bundle", help=f"Bundle directory, or a protocol name (${SPEC_PATH_VARIABLE}, then formal/)")
    parser.add_argument("--tla2tools", help="Path to tla2tools.jar (default: $TLA2TOOLS_JAR)")
    parser.add_argument("--workers", default="auto", help="TLC workers (default auto)")
    parser.add_argument(
        "--timeout",
        type=positive_seconds,
        default=DEFAULT_TIMEOUT_S,
        metavar="SECONDS",
        help="Stop SANY and TLC (their process group) once the command has run this long; a run cut short "
        f"is an error, exit 2 (default {DEFAULT_TIMEOUT_S:g})",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="A tla2tools.jar that is no release the spec bridge knows (by SHA-256) is an error, not a warning",
    )
    parser.add_argument("--output", help="Write the JSON report to this file")
    parser.add_argument("--verbose", action="store_true")


def _tools(args: argparse.Namespace) -> TlaTools:
    """The tools the command runs: located, probed (a missing, non-TLC, or too-old jar is a :class:`TlaError`), and
    under ``--timeout``; a jar that is no known release is printed as a warning, or raised with ``--strict``."""

    tools = TlaTools.locate(args.tla2tools, workers=args.workers, strict=args.strict).within(args.timeout)
    for warning in tools.warnings:
        print(f"Warning: {warning}", file=sys.stderr)
    return tools


def _relative(bundle: Bundle, path: str) -> str:
    """``path`` relative to the bundle when it is inside it, else its file name: reports never hold absolute paths."""

    relative = os.path.relpath(os.path.abspath(path), bundle.path)
    return relative if not relative.startswith(os.pardir) else os.path.basename(path)


def _write(args: argparse.Namespace, report: dict[str, Any]) -> None:
    if args.output:
        write_document(args.output, report)
        print(f"Report: {args.output}")


def check_command(args: argparse.Namespace) -> int:
    try:
        tools = _tools(args)
    except TlaError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return EXIT_HARNESS_ERROR
    identity = tools.identity()
    release = identity["release"] or "no release the spec bridge knows"
    print(f"Tools: TLC {identity['tlc_version']} from {identity['jar']} ({release}, sha256 {identity['sha256']})")
    report = check_bundle(
        args.bundle,
        tools,
        require_approved=args.require_approved,
        experiment_dirs=args.experiment_dirs,
        configs=args.configs,
        progress=print,
    )
    for name, gate in report["gates"].items():
        print(f"[{gate['status'].upper():<4}] {name}{': ' + gate['error'] if gate.get('error') else ''}")
        for item, entry in (gate.get("configs") or gate.get("controls") or gate.get("modules") or {}).items():
            if isinstance(entry, dict) and "status" in entry:
                detail = entry.get("error") or entry.get("reason") or ""
                if not detail and entry.get("distinct_states") is not None:
                    detail = f"{entry['distinct_states']} distinct states, depth {entry.get('depth')}"
                if not detail and "scenarios" in entry:
                    detail = f"{len(entry['scenarios'])} scenario(s) compile"
                print(f"         {entry['status']:<4} {item} {detail}".rstrip())
        for problem in gate.get("problems") or []:
            print(f"         {problem}")
    coverage = report["gates"].get("model_coverage", {})
    if coverage.get("never_fired"):
        print(f"Actions that never fired in a safe config: {', '.join(coverage['never_fired'])}")
    if report.get("error"):
        print(f"\n{report['error']}")
    claims = f"Claims {report['claims']['status']}; " if "claims" in report else ""
    print(f"\n{claims}spec check {report['status'].upper()} in {report['seconds']}s")
    _write(args, report)
    return {"pass": EXIT_SUCCESS, "fail": EXIT_FAILURE}.get(report["status"], EXIT_HARNESS_ERROR)


def translate_command(args: argparse.Namespace) -> int:
    searching = bool(args.reach or args.mutate)
    if (args.control or args.trace) and searching:
        print("Error: --reach and --mutate search the model on their own; leave out --control and --trace", file=sys.stderr)  # fmt: skip
        return EXIT_HARNESS_ERROR
    if not (args.control or args.trace or searching):
        print("Error: one of --control, --trace, --reach, or --mutate is required", file=sys.stderr)
        return EXIT_HARNESS_ERROR
    bundle = load_bundle(args.bundle)
    try:
        service = bundle_service(bundle, args.experiment_dirs)
    except Exception as exc:
        print(f"Error: the experiments do not load: {exc}", file=sys.stderr)
        return EXIT_HARNESS_ERROR
    if searching:
        return _search(args, bundle, service.compile)
    if args.control:
        control = bundle.control(args.control)
        print(f"TLC: negative control {control.name} (must violate {control.violates})")
        result, run = run_control(bundle, control, _tools(args))
        if run.timed_out:
            print(f"Error: {run.timeout_error}", file=sys.stderr)
            return EXIT_HARNESS_ERROR
        config_text = bundle.files[control.config]
        config = control.config
        name = control.name
    else:
        if not args.config:
            print("Error: --trace needs --config, the .cfg TLC ran with", file=sys.stderr)
            return EXIT_HARNESS_ERROR
        with open(args.trace, encoding="utf-8") as handle:
            text = handle.read()
        with open(args.config, encoding="utf-8") as handle:
            config_text = handle.read()
        if text.lstrip().startswith("{"):
            # TLC's -dumpTrace json file: the trace alone, so the violated property is not known.
            result = read_tlc_output("", json_trace=text, model_values=config_model_values(config_text))
        else:
            result = read_tlc_output(text)
        config = _relative(bundle, args.config)
        name = os.path.splitext(os.path.basename(args.trace))[0]
        name = name[len(bundle.protocol) + 1 :] if name.startswith(f"{bundle.protocol}-") else name
    if not result.trace:
        print("Error: TLC produced no counterexample to translate", file=sys.stderr)
        return EXIT_HARNESS_ERROR
    try:
        translation = translate_trace(
            bundle,
            result.trace,
            config_text,
            name=name,
            violated=result.violated or result.violation,
            config=config,
            variants=args.variants,
            seed=args.seed,
            compile_scenario=service.compile,
        )
    except (TranslateError, SpecBundleError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return EXIT_HARNESS_ERROR
    os.makedirs(args.out, exist_ok=True)
    _write_translation(args.out, bundle, translation)
    write_document(os.path.join(args.out, "translation.json"), translation)
    _write(args, translation)
    return EXIT_SUCCESS


def _write_translation(out: str, bundle: Bundle, translation: Mapping[str, Any], found_by: str = "") -> None:
    """Write one translation's scenarios to ``out`` and print them, the fault alignment, and the unmapped faults."""

    counterexample = translation["counterexample"]
    for scenario in translation["scenarios"]:
        path = os.path.join(out, f"{scenario['scenario_id']}.yaml")
        header = (
            f"# Translated by `valkey-fuzzer spec translate` from the {counterexample['name']} counterexample of the\n"
            f"# {bundle.protocol} bundle ({counterexample['config']}: {counterexample['violated']}, "
            f"{counterexample['length']} states).\n"
        ) + (f"# Found by {found_by}.\n" if found_by else "")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(header + yaml.safe_dump(scenario, sort_keys=False))
        scenario_inputs = json.dumps(scenario["experiment"]["inputs"], sort_keys=True)
        print(f"  {path}: {scenario_inputs}")
    for item in translation["alignment"]:
        between = ", ".join(item["between"]) or "nothing"
        print(
            f"{item['fault']} ({item['component']}) at state {item['fault_state']} follows "
            f"{item['trigger']} #{item['occurrence']} "
            f"({item['trigger_event']}, state {item['trigger_state']}); model steps in between: {between}"
        )
    for action, why in translation["unmapped"].items():
        print(f"{action}: not injected by the scenario; {why}")


def _found_by(source: Mapping[str, Any]) -> str:
    """How the search found a counterexample, for a scenario's header and the console."""

    if source["kind"] == "reach":
        return f"--reach {source['target']} ({source['config']})"
    flips = " and ".join(f"{name} = {value}" for name, value in source["flipped"].items())
    return f"--mutate ({source['config']} with {flips})"


def _search(args: argparse.Namespace, bundle: Bundle, compile_scenario: Any) -> int:
    """``translate --reach``/``--mutate``: search the model, write every scenario, and report the search."""

    try:
        tools = _tools(args)
    except TlaError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return EXIT_HARNESS_ERROR
    reach = args.reach
    if "all" in reach:
        reach = [item.name for item in bundle.claims.targets]
        if not reach and not args.mutate:
            print(f"Error: {bundle.protocol}'s claims.yaml names no targets to reach", file=sys.stderr)
            return EXIT_HARNESS_ERROR
        if not reach:
            print(f"{bundle.protocol}'s claims.yaml names no targets to reach; mutating only")
    try:
        report = search_model(
            bundle,
            tools,
            tlc_timeout_s=args.tlc_timeout,
            max_scenarios=args.max_scenarios,
            reach=reach,
            mutate=args.mutate,
            variants=args.variants,
            seed=args.seed,
            compile_scenario=compile_scenario,
            progress=print,
        )
    except (TranslateError, SpecBundleError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return EXIT_HARNESS_ERROR
    os.makedirs(args.out, exist_ok=True)
    for translation in report["translations"]:
        found_by = _found_by(translation["source"])
        print(f"\n{translation['counterexample']['name']}: found by {found_by}")
        _write_translation(args.out, bundle, translation, found_by)
    entries = [
        (f"{target} in {config}", entry)
        for target, per_config in report["targets"].items()
        for config, entry in per_config.items()
    ] + [
        (f"{config} with {label}", entry)
        for config, per_config in report["mutations"].items()
        for label, entry in per_config.items()
    ]
    print()
    for what, entry in entries:
        detail = entry.get("error") or entry.get("reason") or ""
        set_aside = ", ".join(item["violated"] for item in entry.get("without_fault") or [])
        if entry["status"] == "duplicate":
            detail = f"same action sequence as {entry['same_as']}"
        elif entry.get("scenarios"):
            detail = f"{len(entry['scenarios'])} scenario(s)" + (
                f", after setting aside {set_aside}" if set_aside else ""
            )
        elif entry["status"] == "held":
            detail = "every check held"
        elif entry["status"] == "no-fault" and set_aside:
            detail = f"every violation found takes no mapped fault: {set_aside}"
        print(f"[{entry['status'].upper():<14}] {what}" + (f": {detail}" if detail else ""))
    if report["unreachable"]:
        print(f"Unreachable in every safe config: {', '.join(report['unreachable'])}")
    if report["held"]:
        print(f"Mutations under which every check held: {', '.join(report['held'])}")
    if report["stopped"]:
        print(f"Search stopped: {report['stopped']}")
    print(f"\n{len(report['scenarios'])} scenario(s) from {len(report['translations'])} counterexample(s)")
    write_document(os.path.join(args.out, "translation.json"), report)
    _write(args, report)
    failed = any(entry["status"] == "error" for _, entry in entries)
    return EXIT_HARNESS_ERROR if failed else EXIT_SUCCESS


def conform_command(args: argparse.Namespace) -> int:
    bundle = load_bundle(args.bundle)
    transitions = not args.no_transitions and bundle.claims.transitions_excluded is None
    tools = None
    try:
        tools = _tools(args)
    except TlaError as exc:
        # The transition check is reported as not checked without TLC; the properties cannot be.
        if needs_tlc(bundle):
            print(f"Error: {exc}", file=sys.stderr)
            return EXIT_HARNESS_ERROR
    runs = collect_runs(args.runs)
    if not runs:
        print("Error: no runs found", file=sys.stderr)
        return EXIT_HARNESS_ERROR
    group_by = [key for key in args.group_by.split(",") if key]
    report = conform_runs(bundle, runs, tools, group_by=group_by, transitions=not args.no_transitions)
    for run in report["runs"]:
        checks = [
            f"{name} {entry['status']}" + ("" if entry["claimed"] else " (outside the claim)")
            for name, entry in (run.get("properties") or {}).items()
        ]
        checks.append(_transitions_text(run.get("transitions")))
        reached = [name for name, entry in (run.get("targets") or {}).items() if entry["reached"]]
        mismatches = run.get("sample_mismatches") or []
        print(
            f"[{run['status'].upper():<5}] {run.get('scenario_id') or run['source']}: {run['states']} observed states; "
            + (run.get("error") or ", ".join(item for item in checks if item))
            + (f"; reached {', '.join(reached)}" if reached else "")
            + (f"; {len(mismatches)} sample mismatch(es): {mismatches[0]['var']} ..." if mismatches else "")
        )
    coverage = report["action_coverage"]
    observed = ", ".join(f"{action} {count}/{coverage['runs']}" for action, count in coverage["observed"].items())
    print(f"\nLive action coverage (runs observing each action): {observed}")
    if coverage["unobservable"]:
        print(f"Unobservable: {', '.join(coverage['unobservable'])}")
    for row in report.get("groups", []):
        print("  " + ", ".join(f"{key}={value}" for key, value in row.items()))
    print(f"\nChecked: {report['checks']}. Refinement: {report['refinement']}.")
    if transitions and tools is None:
        print(f"Transitions {TRANSITIONS_UNCHECKED}: {TOOLS_HINT}")
    print(f"spec conform {report['status'].upper()}")
    _write(args, report)
    return {"pass": EXIT_SUCCESS, "fail": EXIT_FAILURE}.get(report["status"], EXIT_HARNESS_ERROR)


def _transitions_text(entry: Optional[Mapping[str, Any]]) -> str:
    """One run's transition result for the console."""

    if not entry:
        return ""
    if entry.get("holds"):
        return f"transitions hold ({entry['checked']} checked)"
    if entry.get("holds") is False:
        violation = entry["first_violation"]
        return (
            f"transition violated at record {violation['index']}: {violation['from']['why']} -> "
            f"{violation['to']['why']} ({violation['event']}) has no model step"
        )
    if entry.get("excluded"):
        return f"transitions excluded: {entry['excluded']}"
    return f"transitions {entry.get('skipped', 'not checked')}"
