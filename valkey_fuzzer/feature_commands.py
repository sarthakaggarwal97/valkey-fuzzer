"""Catalog, replay, differential, campaign, coverage, and minimization commands.

Every command compiles and runs through :class:`FeatureRuntime`; nothing here
knows about individual feature packs, components, or evidence IDs.
"""

import argparse
import copy
import hashlib
import json
import os
import platform
import sys
import tempfile
import time
import traceback
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Callable, Optional

import yaml

from .campaign import Campaign, CoverageDB, coverage_report, coverage_tuple, failure_signature, minimize, run_campaign
from .differential import Differential, run_differential
from .extensibility import ComponentKind, OutcomeStatus, RunOutcome, RunPlan, canonical_json, to_plain_data
from .feature_packs import load_scenario_yaml
from .feature_runtime import FeatureRuntime

REPORT_SCHEMA = "valkey-fuzzer.run-report/v1"
FUZZER_VERSION = "0.1.0"

EXIT_CODES = {
    OutcomeStatus.PASS.value: 0,
    OutcomeStatus.FAIL.value: 1,
    OutcomeStatus.ERROR.value: 2,
    OutcomeStatus.SKIP_UNSUPPORTED.value: 3,
}


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def runtime_for(args: argparse.Namespace, artifact_dir: Optional[str] = None) -> FeatureRuntime:
    directories = getattr(args, "feature_pack_dirs", ()) or ()
    return FeatureRuntime(
        feature_pack_directories=tuple(directories),
        discover_entry_points=getattr(args, "no_plugins", False) is not True,
        artifact_dir=artifact_dir if artifact_dir is not None else (getattr(args, "artifact_dir", "") or ""),
    )


def plain(value: Any) -> Any:
    """Thaw frozen mappings/tuples into JSON-friendly dicts and lists."""

    if isinstance(value, Mapping):
        return {str(key): plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    return value


def write_json(path: str, data: Any) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=target.parent, prefix=f".{target.name}.", suffix=".tmp", delete=False
    ) as temporary:
        json.dump(data, temporary, indent=2, sort_keys=True, default=str)
        temporary.write("\n")
    os.replace(temporary.name, target)


def load_scenario_file(path: str) -> dict[str, Any]:
    source = Path(path)
    if not source.is_file():
        raise FileNotFoundError(f"scenario file not found: {path}")
    return plain(load_scenario_yaml(source.read_text(encoding="utf-8"), source_name=path))


def load_yaml_document(path: str) -> Any:
    source = Path(path)
    if not source.is_file():
        raise FileNotFoundError(f"file not found: {path}")
    return yaml.safe_load(source.read_text(encoding="utf-8"))


def with_binary(scenario: Mapping[str, Any], valkey_binary: Optional[str]) -> dict[str, Any]:
    """Return a copy whose topology uses ``valkey_binary``, as ``run --valkey-binary`` does."""

    updated = copy.deepcopy(dict(scenario))
    if valkey_binary:
        topology = dict(updated.get("topology") or {})
        topology["valkey_binary"] = valkey_binary
        updated["topology"] = topology
    return updated


def reproducer(runtime: FeatureRuntime, plan: RunPlan, outcome: Optional[RunOutcome] = None) -> dict[str, Any]:
    """Report-safe provenance needed to reproduce a run exactly."""

    used = {component.component_id for component in plan.components}
    components = [row for row in describe_components(runtime) if row["component_id"] in used]
    pack_digest = None
    try:
        pack_digest = hashlib.sha256(canonical_json(runtime.catalog.resolve(plan.pack_id, plan.pack_version)).encode())
    except Exception:
        pass
    return {
        "fuzzer_version": FUZZER_VERSION,
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "pack": {
            "pack_id": plan.pack_id,
            "version": plan.pack_version,
            "digest": pack_digest.hexdigest() if pack_digest is not None else None,
        },
        "plugins": [
            {"plugin_id": manifest.plugin_id, "metadata": plain(to_plain_data(manifest.metadata))}
            for manifest in runtime.plugins
        ],
        "components": components,
        "plan_digest": hashlib.sha256(plan.to_json().encode("utf-8")).hexdigest(),
        "valkey_sha": outcome.valkey_sha if outcome is not None else None,
    }


def run_report(
    runtime: FeatureRuntime,
    scenario: Optional[Mapping[str, Any]],
    outcome: RunOutcome,
) -> dict[str, Any]:
    """One self-contained, replayable report for a single run."""

    return {
        "schema": REPORT_SCHEMA,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "status": outcome.status.value,
        "summary": outcome.summary,
        "scenario": plain(scenario) if scenario is not None else None,
        "coverage": plain(coverage_tuple(outcome.plan, outcome)),
        "reproducer": reproducer(runtime, outcome.plan, outcome),
        "outcome": plain(outcome.to_data()),
    }


def execute(
    runtime: FeatureRuntime,
    scenario: Optional[Mapping[str, Any]],
    plan: RunPlan,
    *,
    report_dir: str,
    timeout: Optional[float],
) -> tuple[RunOutcome, dict[str, Any]]:
    """Run one plan and leave ``plan.json`` and ``report.json`` in ``report_dir``."""

    os.makedirs(report_dir, exist_ok=True)
    write_json(os.path.join(report_dir, "plan.json"), plain(plan.to_data()))
    deadline = time.monotonic() + timeout if timeout is not None else None
    try:
        outcome = runtime.run_plan(plan, deadline=deadline)
    except Exception as exc:
        # A plan can compile yet fail execution preflight (for example a
        # topology selector a reduced scenario no longer has). Record it as an
        # ERROR run so campaigns, differentials, and minimization continue.
        outcome = RunOutcome(
            plan=plan,
            status=OutcomeStatus.ERROR,
            summary=f"run failed before execution: {exc}",
            diagnostics={"error": {"type": type(exc).__name__, "message": str(exc)[:2000]}},
        )
    report = run_report(runtime, scenario, outcome)
    write_json(os.path.join(report_dir, "report.json"), report)
    return outcome, report


def describe_components(runtime: FeatureRuntime) -> list[dict[str, str]]:
    rows = []
    for kind in sorted(ComponentKind, key=lambda item: item.value):
        for component_id, registration in sorted(runtime.registries[kind].snapshot().items()):
            factory = registration.factory
            rows.append(
                {
                    "component_id": component_id,
                    "kind": kind.value,
                    "api_version": registration.api_version,
                    "implementation": f"{getattr(factory, '__module__', '?')}.{getattr(factory, '__qualname__', '?')}",
                }
            )
    return rows


def _base_dir(args: argparse.Namespace, kind: str, name: str) -> str:
    root = getattr(args, "artifact_dir", "") or os.path.join(tempfile.gettempdir(), "valkey-fuzzer")
    return os.path.join(root, kind, name)


def _short(value: Any) -> str:
    text = value if isinstance(value, str) else json.dumps(value, sort_keys=True, default=str)
    return text if len(text) <= 20 else f"{text[:16]}..."


def _guard(command: Callable[[argparse.Namespace], int]) -> Callable[[argparse.Namespace], int]:
    def run(args: argparse.Namespace) -> int:
        try:
            return command(args)
        except Exception as exc:
            print(f"Error: {exc}")
            if getattr(args, "verbose", False):
                traceback.print_exc()
            return EXIT_CODES[OutcomeStatus.ERROR.value]

    return run


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


@_guard
def list_command(args: argparse.Namespace) -> int:
    runtime = runtime_for(args)
    if args.what == "packs":
        rows = [
            {
                "pack_id": pack_id,
                "version": version,
                "mode": spec.mode.value,
                "topologies": list(spec.supported_topologies),
                "description": plain(to_plain_data(spec.metadata)).get("description", ""),
            }
            for (pack_id, version), spec in sorted(runtime.catalog.snapshot().items())
        ]
        if args.format == "json":
            print(json.dumps(rows, indent=2))
        else:
            for row in rows:
                print(f"{row['pack_id']}@{row['version']} [{', '.join(row['topologies'])}] {row['description']}")
        return 0
    rows = describe_components(runtime)
    if args.format == "json":
        print(json.dumps(rows, indent=2))
    else:
        for row in rows:
            print(f"{row['kind']:<10} {row['component_id']:<28} {row['implementation']}")
    return 0


def _load_report(path: str) -> Any:
    # JSON is YAML, so this reads --format json and --format yaml reports.
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def _exit_code(status: str, args: argparse.Namespace) -> int:
    if status == OutcomeStatus.SKIP_UNSUPPORTED.value and getattr(args, "allow_unsupported", False):
        return 0
    return EXIT_CODES[status]


def _plan_from_file(path: str) -> tuple[RunPlan, Optional[dict[str, Any]]]:
    data = _load_report(path)
    if isinstance(data, Mapping) and data.get("schema") == REPORT_SCHEMA:
        return RunPlan.from_data(data["outcome"]["plan"]), data.get("scenario")
    if isinstance(data, Mapping) and isinstance(data.get("results"), list):
        results = data["results"]
        if len(results) != 1:
            raise ValueError(f"{path} holds {len(results)} results; replay needs exactly one")
        return RunPlan.from_data(results[0]["plan"]), data.get("scenario")
    return RunPlan.from_data(data), None


@_guard
def replay_command(args: argparse.Namespace) -> int:
    plan, scenario = _plan_from_file(args.plan)
    if args.valkey_binary:
        data = plain(to_plain_data(plan))
        data["topology_document"]["valkey_binary"] = args.valkey_binary
        plan = RunPlan.from_data(data)
    runtime = runtime_for(args)
    print(f"Replaying {plan.scenario_id} ({plan.pack_id}@{plan.pack_version}, seed {plan.seed})")
    report_dir = _base_dir(args, "replays", f"{plan.scenario_id}-{time.strftime('%Y%m%d-%H%M%S')}")
    outcome, report = execute(runtime, scenario, plan, report_dir=report_dir, timeout=args.timeout)
    print(f"Status: {outcome.status.value}")
    print(f"Summary: {outcome.summary}")
    print(f"Report: {os.path.join(report_dir, 'report.json')}")
    if args.output:
        write_json(args.output, report)
    return _exit_code(outcome.status.value, args)


@_guard
def diff_command(args: argparse.Namespace) -> int:
    differential = Differential.from_document(load_yaml_document(args.file))
    if args.seed is not None:
        differential.base["seed"] = args.seed
    # Variants patch the base, so a variant that sets its own binary keeps it.
    differential.base = with_binary(differential.base, args.valkey_binary)
    runtime = runtime_for(args)
    base_dir = _base_dir(args, "differential", differential.differential_id)

    def compile_variant(scenario: dict) -> RunPlan:
        return runtime.compile(scenario)

    def execute_variant(name: str, scenario: dict, plan: RunPlan) -> tuple[RunOutcome, str]:
        run_dir = os.path.join(base_dir, name)
        outcome, _ = execute(runtime, scenario, plan, report_dir=run_dir, timeout=args.timeout)
        return outcome, run_dir

    def on_variant(name: str, outcome: RunOutcome) -> None:
        print(f"[{outcome.status.value:<16}] variant {name}: {outcome.summary}")

    report = run_differential(
        differential,
        compile_fn=compile_variant,
        execute_fn=execute_variant,
        on_variant=on_variant,
    )
    print(f"\nDifferential {report['differential_id']} (seed {report['seed']}): {report['status']}")
    print(f"Summary: {report['summary']}")
    for comparison in report["comparisons"]:
        marker = "ok  " if comparison["result"] == "ok" else "FAIL"
        values = ", ".join(f"{name}={_short(value)}" for name, value in comparison["values"].items())
        print(f"  [{marker}] {comparison['kind']:<6} {comparison['rule']}: {values}")
    write_json(os.path.join(base_dir, "differential.json"), report)
    if args.output:
        write_json(args.output, report)
    return _exit_code(report["status"], args)


@_guard
def campaign_command(args: argparse.Namespace) -> int:
    campaign = Campaign.from_document(load_yaml_document(args.file))
    if args.max_runs is not None:
        campaign.max_runs = args.max_runs
    # Matrix dimensions patch the base, so a dimension over binaries still wins.
    campaign.base = with_binary(campaign.base, args.valkey_binary)
    runtime = runtime_for(args)
    coverage = CoverageDB(args.coverage)
    base_dir = _base_dir(args, "campaigns", campaign.campaign_id)

    def compile_case(scenario: dict) -> RunPlan:
        return runtime.compile(scenario)

    if args.dry_run:
        schedule = campaign.schedule(compile_case, coverage)
        print(f"Campaign {campaign.campaign_id}: {len(campaign.cases)} cases, {len(schedule)} scheduled")
        for case in schedule:
            print(f"  {case.case_id} key={case.key} runs={coverage.runs(case.key)} {json.dumps(case.assignment)}")
        for case in campaign.cases:
            if case.compile_error:
                print(f"  invalid {case.case_id}: {case.compile_error}")
        return 0

    def execute_case(case: Any) -> RunOutcome:
        outcome, _ = execute(
            runtime,
            case.scenario,
            case.plan,
            report_dir=os.path.join(base_dir, f"{case.case_id}-seed{case.seed}"),
            timeout=args.timeout,
        )
        return outcome

    def on_result(case: Any, outcome: RunOutcome, record: Mapping[str, Any]) -> None:
        marker = "new" if record["new_tuple"] else "   "
        print(f"[{outcome.status.value:<16}] {marker} {case.case_id} {json.dumps(case.assignment)}: {outcome.summary}")

    summary = run_campaign(
        campaign,
        compile_fn=compile_case,
        execute_fn=execute_case,
        coverage=coverage,
        on_result=on_result,
    )
    print(
        f"\nCampaign {summary['campaign_id']}: ran {summary['cases_run']} of {summary['cases_generated']} cases "
        f"({summary['cases_invalid']} invalid), {summary['new_tuples']} new tuple(s); statuses {summary['statuses']}"
    )
    write_json(os.path.join(base_dir, "campaign.json"), summary)
    if args.output:
        write_json(args.output, summary)
    statuses = summary["statuses"]
    if statuses.get(OutcomeStatus.FAIL.value):
        return EXIT_CODES[OutcomeStatus.FAIL.value]
    if statuses.get(OutcomeStatus.ERROR.value):
        return EXIT_CODES[OutcomeStatus.ERROR.value]
    if not summary["cases_run"]:
        print("Error: the campaign ran no cases")
        return EXIT_CODES[OutcomeStatus.ERROR.value]
    return 0


@_guard
def coverage_command(args: argparse.Namespace) -> int:
    if not Path(args.database).exists():
        raise FileNotFoundError(f"coverage database not found: {args.database}")
    db = CoverageDB(args.database)
    campaign = None
    compile_case = None
    if args.campaign:
        campaign = Campaign.from_document(load_yaml_document(args.campaign))
        runtime = runtime_for(args)
        compile_case = runtime.compile
    report = coverage_report(db, campaign, compile_case)
    if args.format == "json":
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
        return 0
    summary = report["summary"]
    print(f"Coverage: {summary['tuples']} tuple(s) over {summary['runs']} run(s); statuses {summary['statuses']}")
    print("\nBy feature:")
    for name, feature in report["features"].items():
        print(f"  {name}: {feature['tuples']} tuple(s), {feature['runs']} run(s), {feature['statuses']}")
    for dimension, values in report["dimensions"].items():
        print(f"\n{dimension.capitalize()} (runs):")
        for value, runs in values.items():
            print(f"  {runs:>5}  {value}")
    if report["failing_tuples"]:
        print("\nTuples with failures:")
        for item in report["failing_tuples"]:
            print(f"  {item['key']} {item['feature']} [{item['topology']}] {item['statuses']} inputs={item['inputs']}")
    if "campaign" in report:
        info = report["campaign"]
        print(
            f"\nCampaign {info['campaign_id']}: {info['covered_tuples']}/{info['distinct_tuples']} tuple(s) covered, "
            f"{info['untried_tuples']} untried, {len(info['invalid'])} invalid case(s)"
        )
        for item in info["untried"]:
            print(f"  untried {item['case_id']} {json.dumps(item['assignment'], sort_keys=True)}")
    return 0


@_guard
def minimize_command(args: argparse.Namespace) -> int:
    report = _load_report(args.report)
    scenario = report.get("scenario") if isinstance(report, Mapping) else None
    if not isinstance(scenario, Mapping) or "feature" not in scenario:
        raise ValueError("report does not contain the original scenario; minimize needs a 'run' report")
    if report.get("schema") == REPORT_SCHEMA:
        outcome_data = report["outcome"]
    else:
        outcome_data = report["results"][0]
    reference_outcome = RunOutcome.from_data(outcome_data)
    plan = reference_outcome.plan
    required = [requirement.evidence_id for requirement in plan.required_evidence]
    reference = failure_signature(reference_outcome, required)
    if reference["status"] not in (OutcomeStatus.FAIL.value, OutcomeStatus.ERROR.value):
        print(f"Nothing to minimize: the run's status is {reference['status']}")
        return 0
    runtime = runtime_for(args)
    spec = runtime.catalog.resolve(plan.pack_id, plan.pack_version)
    base_dir = _base_dir(args, "minimize", plan.scenario_id)
    attempts = {"count": 0}
    scenario = with_binary(scenario, args.valkey_binary)

    def compile_candidate(candidate: dict) -> RunPlan:
        return runtime.compile(candidate)

    def execute_candidate(candidate: dict, candidate_plan: RunPlan) -> RunOutcome:
        attempts["count"] += 1
        outcome, _ = execute(
            runtime,
            candidate,
            candidate_plan,
            report_dir=os.path.join(base_dir, f"attempt-{attempts['count']}"),
            timeout=args.timeout,
        )
        return outcome

    print(f"Minimizing {plan.scenario_id}: signature {json.dumps(reference)}")
    result = minimize(
        scenario,
        reference,
        compile_fn=compile_candidate,
        execute_fn=execute_candidate,
        input_schema=plain(to_plain_data(spec.input_schema)),
        required_evidence=required,
        max_attempts=args.max_attempts,
        log=print,
    )
    print(f"Accepted reductions: {', '.join(result['accepted']) or 'none'}")
    text = yaml.safe_dump(plain(result["scenario"]), sort_keys=False)
    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
        print(f"Minimized scenario written to {args.output}")
    else:
        print(text)
    return 0


COMMANDS = {
    "campaign": campaign_command,
    "coverage": coverage_command,
    "diff": diff_command,
    "list": list_command,
    "minimize": minimize_command,
    "replay": replay_command,
}


def add_commands(subparsers: Any, add_runtime_options: Callable[..., None]) -> None:
    """Register the commands on the main parser's subparsers."""

    list_parser = subparsers.add_parser("list", help="List installed feature packs or components")
    list_parser.add_argument("what", choices=["packs", "components"])
    list_parser.add_argument("--format", choices=["text", "json"], default="text")
    add_runtime_options(list_parser, execution=False)

    replay = subparsers.add_parser("replay", help="Re-execute a recorded run (report.json or plan.json)")
    replay.add_argument("plan", help="Path to a run report, a --output results file, or a plan.json")
    replay.add_argument("--output", help="Path to save the replay report")
    replay.add_argument("--verbose", action="store_true")
    add_runtime_options(replay, execution=True)

    diff = subparsers.add_parser("diff", help="Run one seeded scenario under several variants and compare evidence")
    diff.add_argument("file", help="Path to a differential YAML document")
    diff.add_argument("--seed", type=int, help="Override the base scenario seed")
    diff.add_argument("--output", help="Path to save the differential report")
    diff.add_argument("--verbose", action="store_true")
    add_runtime_options(diff, execution=True)

    campaign = subparsers.add_parser("campaign", help="Run a scenario matrix, untried combinations first")
    campaign.add_argument("file", help="Path to a campaign YAML document")
    campaign.add_argument("--coverage", help="Coverage database JSON to read and update")
    campaign.add_argument("--max-runs", type=int, help="Override the campaign's max_runs")
    campaign.add_argument("--dry-run", action="store_true", help="Only print the schedule")
    campaign.add_argument("--output", help="Path to save the campaign summary")
    campaign.add_argument("--verbose", action="store_true")
    add_runtime_options(campaign, execution=True)

    coverage = subparsers.add_parser("coverage", help="Summarize a coverage database and what a campaign has left")
    coverage.add_argument("database", help="Coverage JSON written by 'campaign --coverage'")
    coverage.add_argument("--campaign", help="Campaign YAML whose untried combinations to list")
    coverage.add_argument("--format", choices=["text", "json"], default="text")
    coverage.add_argument("--verbose", action="store_true")
    add_runtime_options(coverage, execution=False)

    minimize_parser = subparsers.add_parser("minimize", help="Shrink a failing run while preserving the failure")
    minimize_parser.add_argument("report", help="Run report (or --output results) of the failing run")
    minimize_parser.add_argument("--output", help="Write the minimized scenario YAML here")
    minimize_parser.add_argument("--max-attempts", type=int, default=30)
    minimize_parser.add_argument("--verbose", action="store_true")
    add_runtime_options(minimize_parser, execution=True)


__all__ = [
    "COMMANDS",
    "EXIT_CODES",
    "REPORT_SCHEMA",
    "add_commands",
    "describe_components",
    "execute",
    "reproducer",
    "run_report",
]
