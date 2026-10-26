"""Differential and metamorphic execution of one seeded scenario.

A differential document runs the same base scenario (same seed, so the same
generated workload) under several variants, then compares selected evidence
fields.  ``equal`` rules hold externally visible behaviour constant (dataset
digests, client replies, reloaded snapshots); ``differ`` rules require the
feature-specific evidence to change (e.g. the save type), proving each variant
actually exercised a different path.

    differential_id: fork-vs-forkless
    base: {<scenario>}
    variants:
      fork:
        topology.server_config: {bgsave-default-method: fork, ...}
      forkless:
        topology.server_config: {bgsave-default-method: forkless, ...}
    compare:
      equal:
        - {evidence_id: valkey.dataset.digest, source: dataset, field: digest}
      differ:
        - {evidence_id: valkey.bgsave.result, source: bgsave, field: after.rdb_last_bgsave_type}

Evidence IDs and fields are opaque here; the feature pack defines them.
"""

import copy
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from .campaign import _SCENARIO_SECTIONS, set_path
from .extensibility import Evidence, OutcomeStatus, RunOutcome, RunPlan, to_plain_data

_RULE_FIELDS = {"evidence_id", "source", "field"}
_MISSING = "<missing>"


class DifferentialError(ValueError):
    """A differential document is malformed."""


@dataclass(frozen=True)
class CompareRule:
    evidence_id: str
    source: Optional[str]
    field: Optional[str]

    @classmethod
    def parse(cls, value: Any, where: str) -> "CompareRule":
        if not isinstance(value, Mapping) or not isinstance(value.get("evidence_id"), str):
            raise DifferentialError(f"{where} needs an evidence_id")
        unknown = sorted(set(value) - _RULE_FIELDS)
        if unknown:
            raise DifferentialError(f"{where} has unknown fields {unknown}")
        for name in ("source", "field"):
            if value.get(name) is not None and not isinstance(value[name], str):
                raise DifferentialError(f"{where}.{name} must be a string")
        return cls(value["evidence_id"], value.get("source"), value.get("field"))

    def label(self) -> str:
        source = f"@{self.source}" if self.source else ""
        path = f".{self.field}" if self.field else ""
        return f"{self.evidence_id}{source}{path}"


@dataclass
class Differential:
    differential_id: str
    base: dict
    variants: dict
    equal: list = field(default_factory=list)
    differ: list = field(default_factory=list)
    require_status: str = OutcomeStatus.PASS.value

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> "Differential":
        if not isinstance(document, Mapping):
            raise DifferentialError("a differential must be a mapping")
        allowed = {"differential_id", "base", "variants", "compare"}
        unknown = sorted(set(document) - allowed)
        if unknown:
            raise DifferentialError(f"differential contains unknown fields: {unknown}")
        if not isinstance(document.get("differential_id"), str) or not document["differential_id"]:
            raise DifferentialError("differential_id is required")
        base = document.get("base")
        if not isinstance(base, Mapping) or not {"feature", "topology", "seed"} <= set(base):
            raise DifferentialError("base must be a scenario with seed, feature, and topology")
        variants = document.get("variants")
        if not isinstance(variants, Mapping) or len(variants) < 2:
            raise DifferentialError("variants must map at least two variant names to patches")
        for name, patch in variants.items():
            if not isinstance(name, str) or not name.replace("-", "").replace("_", "").isalnum():
                raise DifferentialError(f"variant name {name!r} must be alphanumeric (with - or _)")
            if not isinstance(patch, Mapping):
                raise DifferentialError(f"variant {name!r} must map dotted paths to values")
            for path in patch:
                if not isinstance(path, str) or path.split(".")[0] not in _SCENARIO_SECTIONS:
                    raise DifferentialError(
                        f"variant {name!r} path {path!r} must start with one of {list(_SCENARIO_SECTIONS)}"
                    )
                if path in ("feature.use", "feature.version"):
                    raise DifferentialError("variants must run the same feature pack and version")
        compare = document.get("compare") or {}
        if not isinstance(compare, Mapping):
            raise DifferentialError("compare must be a mapping")
        unknown = sorted(set(compare) - {"equal", "differ", "require_status"})
        if unknown:
            raise DifferentialError(f"compare contains unknown fields: {unknown}")
        equal = [
            CompareRule.parse(rule, f"compare.equal[{index}]") for index, rule in enumerate(compare.get("equal", []))
        ]
        differ = [
            CompareRule.parse(rule, f"compare.differ[{index}]") for index, rule in enumerate(compare.get("differ", []))
        ]
        if not equal and not differ:
            raise DifferentialError("compare needs at least one equal or differ rule")
        require_status = compare.get("require_status", OutcomeStatus.PASS.value)
        if require_status not in {status.value for status in OutcomeStatus}:
            raise DifferentialError(f"require_status must be one of {[status.value for status in OutcomeStatus]}")
        return cls(
            differential_id=document["differential_id"],
            base=copy.deepcopy(dict(base)),
            variants={name: dict(patch) for name, patch in variants.items()},
            equal=equal,
            differ=differ,
            require_status=require_status,
        )

    def scenario(self, variant: str) -> dict:
        scenario = copy.deepcopy(self.base)
        for path, value in self.variants[variant].items():
            set_path(scenario, path, copy.deepcopy(value))
        base_id = str(scenario.get("scenario_id") or self.differential_id)
        scenario["scenario_id"] = f"{base_id}-{variant}"
        return scenario


def extract(value: Any, dotted: Optional[str]) -> Any:
    if not dotted:
        return value
    current = value
    for part in dotted.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        elif isinstance(current, (list, tuple)) and part.isdigit() and int(part) < len(current):
            current = current[int(part)]
        else:
            return _MISSING
    return current


def observed(rule: CompareRule, evidence: Sequence[Evidence]) -> Any:
    """The rule's comparable value: one value, a sorted list, or ``<missing>``."""

    values = [
        extract(to_plain_data(item.value), rule.field)
        for item in evidence
        if item.evidence_id == rule.evidence_id and (rule.source is None or item.source == rule.source)
    ]
    values = [value for value in values if value != _MISSING]
    if not values:
        return _MISSING
    if len(values) == 1:
        return values[0]
    return sorted(values, key=lambda item: json.dumps(item, sort_keys=True, default=str))


def compare_outcomes(differential: Differential, outcomes: Mapping[str, RunOutcome]) -> list[dict[str, Any]]:
    comparisons = []
    for kind, rules in (("equal", differential.equal), ("differ", differential.differ)):
        for rule in rules:
            values = {name: observed(rule, outcome.all_evidence) for name, outcome in outcomes.items()}
            present = [value for value in values.values() if value != _MISSING]
            distinct = {json.dumps(value, sort_keys=True, default=str) for value in present}
            if len(present) != len(values):
                result = "missing"
            elif kind == "equal":
                result = "ok" if len(distinct) == 1 else "mismatch"
            else:
                result = "ok" if len(distinct) == len(values) else "unexpectedly_equal"
            comparisons.append({"kind": kind, "rule": rule.label(), "result": result, "values": values})
    return comparisons


def differential_status(
    differential: Differential,
    outcomes: Mapping[str, RunOutcome],
    comparisons: Sequence[Mapping[str, Any]],
) -> tuple[str, str]:
    statuses = {name: outcome.status.value for name, outcome in outcomes.items()}
    if "ERROR" in statuses.values():
        failing = [name for name, status in statuses.items() if status == "ERROR"]
        return "ERROR", f"variant(s) errored: {', '.join(failing)}"
    if "SKIP_UNSUPPORTED" in statuses.values() and differential.require_status != "SKIP_UNSUPPORTED":
        skipped = [name for name, status in statuses.items() if status == "SKIP_UNSUPPORTED"]
        return "SKIP_UNSUPPORTED", f"variant(s) unsupported: {', '.join(skipped)}"
    off = [name for name, status in statuses.items() if status != differential.require_status]
    if off:
        return "FAIL", f"variant(s) did not reach {differential.require_status}: " + ", ".join(
            f"{name}={statuses[name]}" for name in off
        )
    broken = [item for item in comparisons if item["result"] != "ok"]
    if broken:
        return "FAIL", "; ".join(f"{item['rule']} {item['result']}" for item in broken)
    return "PASS", f"{len(comparisons)} comparison(s) hold across {len(outcomes)} variants"


def run_differential(
    differential: Differential,
    *,
    compile_fn: Callable[[dict], RunPlan],
    execute_fn: Callable[[str, dict, RunPlan], tuple[RunOutcome, Optional[str]]],
    on_variant: Optional[Callable[[str, RunOutcome], None]] = None,
) -> dict[str, Any]:
    """Compile every variant first (no mutation on a bad variant), then run them in order."""

    plans = {}
    scenarios = {}
    for name in differential.variants:
        scenarios[name] = differential.scenario(name)
        plans[name] = compile_fn(scenarios[name])
    seeds = {plan.seed for plan in plans.values()}
    if len(seeds) != 1:
        raise DifferentialError("variants must share the base seed so they generate the same workload")
    outcomes: dict[str, RunOutcome] = {}
    variants = {}
    for name, plan in plans.items():
        outcome, artifact_dir = execute_fn(name, scenarios[name], plan)
        outcomes[name] = outcome
        variants[name] = {
            "status": outcome.status.value,
            "summary": outcome.summary,
            "artifact_dir": artifact_dir,
            "scenario": scenarios[name],
        }
        if on_variant is not None:
            on_variant(name, outcome)
    comparisons = compare_outcomes(differential, outcomes)
    status, summary = differential_status(differential, outcomes, comparisons)
    return {
        "schema": "valkey-fuzzer.differential-report/v1",
        "differential_id": differential.differential_id,
        "status": status,
        "summary": summary,
        "seed": next(iter(seeds)),
        "variants": variants,
        "comparisons": comparisons,
    }


__all__ = [
    "CompareRule",
    "Differential",
    "DifferentialError",
    "compare_outcomes",
    "differential_status",
    "extract",
    "observed",
    "run_differential",
]
