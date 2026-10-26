"""Campaign generation, semantic coverage tracking, and failure minimization.

* A **campaign** expands a base scenario across a matrix of dotted-path
  dimensions, then schedules uncovered semantic tuples first, boundary values
  next, and seeded randomness within ties.
* **Coverage** counts semantic tuples (feature, topology shape, config mix,
  inputs, workload/action/fault/observer/oracle composition, artifact and
  resource providers, observed evidence) rather than raw run counts, persisted
  as JSON.

Nothing here interprets feature IDs, component IDs, or component config; a
tuple is built only from the plan's generic structure.
* The **minimizer** greedily shrinks a failing scenario (inputs, topology size,
  configuration keys) while preserving the failure signature: the terminal
  status, the failing required components, and the path evidence that was
  observed.
"""

import copy
import hashlib
import itertools
import json
import os
import random
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from .extensibility import ComponentKind, RunOutcome, RunPlan, canonical_json, to_plain_data

COVERAGE_SCHEMA = "valkey-fuzzer.coverage/v1"


class CampaignError(ValueError):
    """A campaign document is malformed."""


# Scenario sections a campaign matrix or differential variant may patch.
_SCENARIO_SECTIONS = ("feature", "topology", "artifacts", "resources", "policies")


# ---------------------------------------------------------------------------
# Coverage
# ---------------------------------------------------------------------------


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:16]


def static_tuple(plan: RunPlan) -> dict[str, Any]:
    """The semantic tuple a plan targets, independent of its result."""

    document = to_plain_data(plan.topology_document)
    components = to_plain_data(plan.components)

    def of_kind(kind: ComponentKind) -> list:
        return sorted({component["component_id"] for component in components if component["kind"] == kind.value})

    config_mix = {
        "server_config": document.get("server_config", {}),
        "node_configs": document.get("node_configs", {}),
        "overlays": [to_plain_data(overlay) for overlay in plan.startup_overlays],
    }
    # Which nodes get their own binary matters; resolved paths do not.
    artifact_mix = {
        "providers": sorted(artifact.provider_id for artifact in plan.artifacts),
        "node_binaries": sorted(document.get("node_binaries") or {}),
    }
    return {
        "feature": f"{plan.pack_id}@{plan.pack_version}",
        "topology": {
            "type": plan.topology,
            "num_shards": document.get("num_shards"),
            "replicas_per_shard": document.get("replicas_per_shard"),
            "addressing": document.get("addressing"),
        },
        "config_mix": _digest(config_mix),
        "inputs": to_plain_data(plan.inputs),
        "artifact_mix": _digest(artifact_mix),
        "resources": sorted(resource.provider_id for resource in plan.resources),
        "workloads": of_kind(ComponentKind.WORKLOAD),
        "actions": of_kind(ComponentKind.ACTION),
        "faults": of_kind(ComponentKind.FAULT),
        "observers": of_kind(ComponentKind.OBSERVER),
        "oracles": of_kind(ComponentKind.ORACLE),
    }


def coverage_key(plan: RunPlan) -> str:
    return _digest(static_tuple(plan))


def coverage_tuple(plan: RunPlan, outcome: Optional[RunOutcome] = None) -> dict[str, Any]:
    data = {"key": coverage_key(plan), "tuple": static_tuple(plan)}
    if outcome is not None:
        data["status"] = outcome.status.value
        data["evidence"] = sorted({item.evidence_id for item in outcome.all_evidence})
    return data


class CoverageDB:
    """Persistent counts of exercised semantic tuples."""

    def __init__(self, path: Optional[str] = None):
        self.path = path
        self.tuples: dict[str, dict[str, Any]] = {}
        if path and os.path.exists(path):
            with open(path, encoding="utf-8") as handle:
                data = json.load(handle)
            if data.get("schema") != COVERAGE_SCHEMA:
                raise CampaignError(f"{path} is not a {COVERAGE_SCHEMA} file")
            self.tuples = dict(data.get("tuples", {}))

    def runs(self, key: str) -> int:
        return int(self.tuples.get(key, {}).get("runs", 0))

    def record(self, plan: RunPlan, outcome: RunOutcome) -> dict[str, Any]:
        entry = coverage_tuple(plan, outcome)
        stored = self.tuples.setdefault(
            entry["key"], {"tuple": entry["tuple"], "runs": 0, "statuses": {}, "evidence": []}
        )
        stored["runs"] += 1
        stored["statuses"][entry["status"]] = stored["statuses"].get(entry["status"], 0) + 1
        stored["evidence"] = sorted(set(stored["evidence"]) | set(entry["evidence"]))
        stored["last_seen"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        return entry

    def summary(self) -> dict[str, Any]:
        statuses: dict[str, int] = {}
        for entry in self.tuples.values():
            for status, count in entry["statuses"].items():
                statuses[status] = statuses.get(status, 0) + count
        dimensions: dict[str, set] = {}
        for entry in self.tuples.values():
            for name in ("feature", "actions", "faults", "oracles"):
                value = entry["tuple"].get(name)
                for item in value if isinstance(value, list) else [value]:
                    dimensions.setdefault(name, set()).add(str(item))
            dimensions.setdefault("topology", set()).add(json.dumps(entry["tuple"]["topology"], sort_keys=True))
        return {
            "tuples": len(self.tuples),
            "runs": sum(entry["runs"] for entry in self.tuples.values()),
            "statuses": statuses,
            "distinct": {name: len(values) for name, values in sorted(dimensions.items())},
        }

    def save(self) -> None:
        if not self.path:
            return
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        tmp = f"{self.path}.tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump({"schema": COVERAGE_SCHEMA, "tuples": self.tuples}, handle, indent=2, sort_keys=True)
        os.replace(tmp, self.path)


def coverage_report(
    db: "CoverageDB",
    campaign: Optional["Campaign"] = None,
    compile_fn: Optional[Callable[[dict], RunPlan]] = None,
) -> dict[str, Any]:
    """Summarize exercised tuples and, given a campaign, what is still untried."""

    features: dict[str, dict[str, Any]] = {}
    dimensions: dict[str, dict[str, int]] = {}
    failing = []
    for key, entry in sorted(db.tuples.items()):
        data = entry["tuple"]
        feature = features.setdefault(data["feature"], {"tuples": 0, "runs": 0, "statuses": {}, "evidence": set()})
        feature["tuples"] += 1
        feature["runs"] += entry["runs"]
        for status, count in entry["statuses"].items():
            feature["statuses"][status] = feature["statuses"].get(status, 0) + count
        feature["evidence"].update(entry.get("evidence", ()))
        topology = data["topology"]
        shape = topology["type"] + "".join(
            f" {name}={topology[name]}"
            for name in ("num_shards", "replicas_per_shard")
            if topology.get(name) is not None
        )
        values = {
            "topology": [shape],
            "faults": data.get("faults") or ["(none)"],
            "actions": data.get("actions") or ["(none)"],
            "oracles": data.get("oracles") or [],
            "resources": data.get("resources") or ["(none)"],
        }
        for name, items in values.items():
            bucket = dimensions.setdefault(name, {})
            for item in items:
                bucket[item] = bucket.get(item, 0) + entry["runs"]
        bad = {status: count for status, count in entry["statuses"].items() if status in ("FAIL", "ERROR")}
        if bad:
            failing.append(
                {
                    "key": key,
                    "feature": data["feature"],
                    "topology": shape,
                    "inputs": data.get("inputs"),
                    "statuses": bad,
                }
            )
    report: dict[str, Any] = {
        "summary": db.summary(),
        "features": {
            name: {**values, "evidence": sorted(values["evidence"])} for name, values in sorted(features.items())
        },
        "dimensions": {name: dict(sorted(bucket.items())) for name, bucket in sorted(dimensions.items())},
        "failing_tuples": failing,
    }
    if campaign is not None and compile_fn is not None:
        campaign.schedule(compile_fn, db)
        untried = [
            {"case_id": case.case_id, "assignment": case.assignment, "key": case.key}
            for case in campaign.cases
            if case.key and db.runs(case.key) == 0
        ]
        distinct_untried = {item["key"] for item in untried}
        report["campaign"] = {
            "campaign_id": campaign.campaign_id,
            "cases": len(campaign.cases),
            "distinct_tuples": len({case.key for case in campaign.cases if case.key}),
            "covered_tuples": len({case.key for case in campaign.cases if case.key and db.runs(case.key) > 0}),
            "untried_tuples": len(distinct_untried),
            "untried": untried,
            "invalid": [
                {"case_id": case.case_id, "assignment": case.assignment, "error": case.compile_error}
                for case in campaign.cases
                if case.compile_error
            ],
        }
    return report


# ---------------------------------------------------------------------------
# Campaign generation
# ---------------------------------------------------------------------------


def set_path(document: dict, dotted: str, value: Any) -> None:
    parts = dotted.split(".")
    current = document
    for part in parts[:-1]:
        nxt = current.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            current[part] = nxt
        current = nxt
    if value is None and parts[-1] in current:
        del current[parts[-1]]
    elif value is not None:
        current[parts[-1]] = value


def get_path(document: Mapping[str, Any], dotted: str) -> Any:
    current: Any = document
    for part in dotted.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current


@dataclass
class CampaignCase:
    case_id: str
    scenario: dict
    assignment: dict
    seed: int
    boundary: int = 0
    plan: Optional[RunPlan] = None
    compile_error: Optional[str] = None
    key: Optional[str] = None


@dataclass
class Campaign:
    campaign_id: str
    seed: int
    base: dict
    matrix: dict
    seeds_per_tuple: int = 1
    max_runs: Optional[int] = None
    budget_s: Optional[float] = None
    stop_on_failure: bool = False
    cases: list = field(default_factory=list)

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> "Campaign":
        allowed = {
            "campaign_id",
            "seed",
            "base",
            "matrix",
            "seeds_per_tuple",
            "max_runs",
            "budget_s",
            "stop_on_failure",
        }
        unknown = sorted(set(document) - allowed)
        if unknown:
            raise CampaignError(f"campaign contains unknown fields: {unknown}")
        if not isinstance(document, Mapping):
            raise CampaignError("a campaign must be a mapping")
        for name in ("campaign_id", "base"):
            if name not in document:
                raise CampaignError(f"campaign is missing {name!r}")
        base = document["base"]
        if not isinstance(base, Mapping) or "feature" not in base or "topology" not in base:
            raise CampaignError("base must be a scenario with feature and topology")
        matrix = document.get("matrix") or {}
        if not isinstance(matrix, Mapping):
            raise CampaignError("matrix must map dotted scenario paths to value lists")
        for path, values in matrix.items():
            if not isinstance(path, str) or not path or path.split(".")[0] not in _SCENARIO_SECTIONS:
                raise CampaignError(f"matrix dimension {path!r} must start with one of {list(_SCENARIO_SECTIONS)}")
            if not isinstance(values, (list, tuple)) or not values:
                raise CampaignError(f"matrix dimension {path!r} needs a non-empty list of values")
        seed = document.get("seed", 0)
        if isinstance(seed, bool) or not isinstance(seed, int):
            raise CampaignError("seed must be an integer")
        seeds_per_tuple = document.get("seeds_per_tuple", 1)
        if isinstance(seeds_per_tuple, bool) or not isinstance(seeds_per_tuple, int) or seeds_per_tuple < 1:
            raise CampaignError("seeds_per_tuple must be a positive integer")
        return cls(
            campaign_id=str(document["campaign_id"]),
            seed=seed,
            base=copy.deepcopy(dict(base)),
            matrix={path: list(values) for path, values in matrix.items()},
            seeds_per_tuple=seeds_per_tuple,
            max_runs=document.get("max_runs"),
            budget_s=document.get("budget_s"),
            stop_on_failure=bool(document.get("stop_on_failure", False)),
        )

    def expand(self) -> list[CampaignCase]:
        rng = random.Random(self.seed)
        dimensions = list(self.matrix.items())
        cases = []
        product = itertools.product(*[list(enumerate(values)) for _, values in dimensions]) if dimensions else [()]
        for combo_index, combo in enumerate(product):
            assignment = {}
            boundary = 0
            for (path, values), (value_index, value) in zip(dimensions, combo):
                assignment[path] = value
                if value_index in (0, len(values) - 1):
                    boundary += 1
            for replica in range(self.seeds_per_tuple):
                scenario = copy.deepcopy(self.base)
                for path, value in assignment.items():
                    set_path(scenario, path, copy.deepcopy(value))
                seed = rng.randrange(1, 2**31)
                scenario["seed"] = seed
                scenario["scenario_id"] = f"{self.campaign_id}-{combo_index}-{replica}"
                cases.append(
                    CampaignCase(
                        case_id=scenario["scenario_id"],
                        scenario=scenario,
                        assignment=assignment,
                        seed=seed,
                        boundary=boundary,
                    )
                )
        self.cases = cases
        return cases

    def schedule(self, compile_fn: Callable[[dict], RunPlan], coverage: CoverageDB) -> list[CampaignCase]:
        """Compile every case and order uncovered, boundary-heavy tuples first."""

        rng = random.Random(self.seed ^ 0x5EED)
        cases = self.cases or self.expand()
        for case in cases:
            try:
                case.plan = compile_fn(case.scenario)
                case.key = coverage_key(case.plan)
            except Exception as exc:
                case.compile_error = f"{type(exc).__name__}: {exc}"
        valid = [case for case in cases if case.plan is not None]
        tiebreak = {case.case_id: rng.random() for case in valid}
        seen_in_campaign: dict[str, int] = {}
        ordered = []
        remaining_cases = list(valid)
        while remaining_cases:
            remaining_cases.sort(
                key=lambda case: (
                    coverage.runs(case.key) + seen_in_campaign.get(case.key, 0),
                    -case.boundary,
                    tiebreak[case.case_id],
                )
            )
            chosen = remaining_cases.pop(0)
            seen_in_campaign[chosen.key] = seen_in_campaign.get(chosen.key, 0) + 1
            ordered.append(chosen)
        if self.max_runs is not None:
            ordered = ordered[: self.max_runs]
        return ordered


def run_campaign(
    campaign: Campaign,
    *,
    compile_fn: Callable[[dict], RunPlan],
    execute_fn: Callable[[CampaignCase], RunOutcome],
    coverage: CoverageDB,
    on_result: Optional[Callable[[CampaignCase, RunOutcome, dict], None]] = None,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    started = clock()
    scheduled = campaign.schedule(compile_fn, coverage)
    results = []
    for case in scheduled:
        if campaign.budget_s is not None and clock() - started >= campaign.budget_s:
            break
        before = coverage.runs(case.key)
        outcome = execute_fn(case)
        entry = coverage.record(case.plan, outcome)
        coverage.save()
        record = {
            "case_id": case.case_id,
            "assignment": case.assignment,
            "seed": case.seed,
            "status": outcome.status.value,
            "summary": outcome.summary,
            "coverage_key": entry["key"],
            "new_tuple": before == 0,
        }
        results.append(record)
        if on_result is not None:
            on_result(case, outcome, record)
        if campaign.stop_on_failure and outcome.status.value in ("FAIL", "ERROR"):
            break
    invalid = [
        {"case_id": case.case_id, "assignment": case.assignment, "error": case.compile_error}
        for case in campaign.cases
        if case.compile_error
    ]
    statuses: dict[str, int] = {}
    for record in results:
        statuses[record["status"]] = statuses.get(record["status"], 0) + 1
    return {
        "campaign_id": campaign.campaign_id,
        "cases_generated": len(campaign.cases),
        "cases_invalid": len(invalid),
        "cases_run": len(results),
        "new_tuples": sum(record["new_tuple"] for record in results),
        "statuses": statuses,
        "results": results,
        "invalid": invalid,
        "coverage": coverage.summary(),
    }


# ---------------------------------------------------------------------------
# Minimization
# ---------------------------------------------------------------------------


def failure_signature(outcome: RunOutcome, required_evidence: Iterable[str] = ()) -> dict[str, Any]:
    """What must stay true for a reduced scenario to count as the same failure."""

    diagnostics = to_plain_data(outcome.diagnostics)
    failing = sorted(
        result.component_instance_id for result in outcome.component_results if result.status.value in ("FAIL", "ERROR")
    )
    missing = sorted(item["evidence_id"] for item in diagnostics.get("missing_required_evidence", ()))
    observed = {item.evidence_id for item in outcome.all_evidence}
    return {
        "status": outcome.status.value,
        "failing_components": failing,
        "missing_evidence": missing,
        "path_evidence": sorted(observed & set(required_evidence)),
    }


def same_failure(reference: Mapping[str, Any], candidate: Mapping[str, Any]) -> bool:
    return (
        candidate["status"] == reference["status"]
        and candidate["failing_components"] == reference["failing_components"]
        and candidate["missing_evidence"] == reference["missing_evidence"]
        and set(reference["path_evidence"]) <= set(candidate["path_evidence"])
    )


def reduction_candidates(scenario: Mapping[str, Any], input_schema: Mapping[str, Any]) -> list[tuple[str, dict]]:
    """Single-step simplifications, most aggressive first."""

    candidates: list[tuple[str, dict]] = []
    topology = scenario.get("topology", {})
    replicas = topology.get("replicas_per_shard")
    minimum_replicas = 1 if topology.get("type") == "replication" else 0
    if isinstance(replicas, int) and replicas > minimum_replicas:
        for value in sorted({minimum_replicas, replicas - 1}):
            if value < replicas:
                reduced = copy.deepcopy(dict(scenario))
                reduced["topology"]["replicas_per_shard"] = value
                candidates.append((f"topology.replicas_per_shard={value}", reduced))
    shards = topology.get("num_shards")
    if topology.get("type") == "cluster" and isinstance(shards, int) and shards > 3:
        reduced = copy.deepcopy(dict(scenario))
        reduced["topology"]["num_shards"] = 3
        candidates.append(("topology.num_shards=3", reduced))
    for section in ("server_config",):
        for key in sorted(topology.get(section) or {}):
            reduced = copy.deepcopy(dict(scenario))
            del reduced["topology"][section][key]
            candidates.append((f"drop topology.{section}.{key}", reduced))
    for selector in sorted(topology.get("node_configs") or {}):
        reduced = copy.deepcopy(dict(scenario))
        del reduced["topology"]["node_configs"][selector]
        candidates.append((f"drop topology.node_configs.{selector}", reduced))
    properties = (input_schema or {}).get("properties", {})
    inputs = (scenario.get("feature") or {}).get("with") or {}
    for name in sorted(inputs):
        value = inputs[name]
        rule = properties.get(name, {})
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            continue
        minimum = rule.get("minimum", 0)
        for proposal in sorted({minimum, value // 2 if isinstance(value, int) else value / 2}):
            if proposal < value and proposal >= minimum:
                reduced = copy.deepcopy(dict(scenario))
                reduced["feature"]["with"][name] = proposal
                candidates.append((f"feature.with.{name}={proposal}", reduced))
    return candidates


def minimize(
    scenario: Mapping[str, Any],
    reference: Mapping[str, Any],
    *,
    compile_fn: Callable[[dict], RunPlan],
    execute_fn: Callable[[dict, RunPlan], RunOutcome],
    input_schema: Mapping[str, Any],
    required_evidence: Sequence[str],
    max_attempts: int = 50,
    log: Optional[Callable[[str], None]] = None,
) -> dict[str, Any]:
    """Greedy one-step-at-a-time reduction preserving ``reference``."""

    current = copy.deepcopy(dict(scenario))
    attempts = []
    accepted = []
    budget = max_attempts
    progress = True
    while progress and budget > 0:
        progress = False
        for description, candidate in reduction_candidates(current, input_schema):
            if budget <= 0:
                break
            try:
                plan = compile_fn(candidate)
            except Exception as exc:
                attempts.append({"step": description, "result": f"invalid: {exc}"})
                continue
            budget -= 1
            outcome = execute_fn(candidate, plan)
            signature = failure_signature(outcome, required_evidence)
            kept = same_failure(reference, signature)
            attempts.append(
                {"step": description, "result": "kept" if kept else "rejected", "status": signature["status"]}
            )
            if log is not None:
                log(f"  {description}: {'kept' if kept else 'rejected'} ({signature['status']})")
            if kept:
                current = candidate
                accepted.append(description)
                progress = True
                break
    return {"scenario": current, "accepted": accepted, "attempts": attempts, "signature": dict(reference)}


__all__ = [
    "COVERAGE_SCHEMA",
    "Campaign",
    "CampaignCase",
    "CampaignError",
    "CoverageDB",
    "canonical_json",
    "coverage_key",
    "coverage_report",
    "coverage_tuple",
    "failure_signature",
    "get_path",
    "minimize",
    "reduction_candidates",
    "run_campaign",
    "same_failure",
    "set_path",
    "static_tuple",
]
