"""Spec conformance as a batch plug-in: each run's own check against the spec bundles its plan names, and a
conform report per bundle over the batch's runs.

A run checks itself as its report is written (:meth:`~valkey_fuzzer.service.RunService.write_run_report`, under
the batch's ``conform_tlc_timeout_s``): its ``report.json`` carries the results under ``conform`` and the verdict
they give, so the plug-in takes both as they are. A run that reached its verdict and whose plan names a bundle but
whose report has no ``conform`` section (it was stopped during its check) is an error for that bundle, held
against the run if the bundle's claims are approved and only reported for a draft's. The summary writes
``conformance/<bundle>.json`` and a digest of it, claimed violations first.
"""

import os
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Optional

from ..minimize import verdict_clauses
from ..reports import write_document
from ..spec.bundle import SpecBundleError, load_bundle
from ..spec.conform import CHECKED_RUN_STATUSES, CONFORM_TIMEOUT_OPTION, bundle_approved, plan_bundles, summarize
from .hooks import BatchPlugin
from .results import Verdict
from .spec import BatchError, BatchOptions, worst

if TYPE_CHECKING:
    from .pool import BatchRunner

CONFORMANCE_DIRECTORY = "conformance"


def _recorded(report: Optional[Mapping[str, Any]]) -> Optional[dict[str, Any]]:
    """The run's own conform results, from its report's ``conform`` section; None for a report without one (the
    run checked no bundle, or was stopped during its check)."""

    recorded = (report or {}).get("conform")
    return dict(recorded) if isinstance(recorded, Mapping) and recorded else None


def _conformance(report: Optional[Mapping[str, Any]]) -> dict[str, Any]:
    """The run's own conform results (its report's ``conform``); for a run that reached its verdict and whose plan
    names spec bundles but whose report has no such section, an error for each bundle, saying whether its claims
    are approved (the batch holds only those against the run)."""

    recorded = _recorded(report)
    if recorded is not None:
        return recorded
    if report is None or report.get("status") not in CHECKED_RUN_STATUSES:
        return {}
    plan = (report.get("outcome") or {}).get("plan") or {}
    return {
        name: {"status": "error", "error": "report has no conform section", "approved": bundle_approved(name)}
        for name in plan_bundles(plan)
    }


class ConformancePlugin(BatchPlugin):
    """Spec conformance: the run's own check and verdict are taken as they are, a run whose report has no check is
    an ERROR for an approved bundle, and the summary reports each bundle over the runs that exercised it."""

    name = "conformance"

    def task_options(self, options: BatchOptions) -> dict[str, Any]:
        if options.conform_tlc_timeout_s <= 0:
            raise BatchError("batch.conform_tlc_timeout_s must be a number of seconds greater than zero")
        return {CONFORM_TIMEOUT_OPTION: options.conform_tlc_timeout_s}

    def judge(self, job: Mapping[str, Any], run_dir: str, report: Optional[Mapping[str, Any]], verdict: Verdict) -> Any:
        recorded = _recorded(report)
        if recorded is not None:
            # The run judged itself (spec.conform.judge_run): its status, summary and signature, formed from its
            # outcome, already carry an approved bundle's finding as the failed component spec-conform:<bundle>.
            return recorded
        conformance = _conformance(report)
        for name, entry in sorted(conformance.items()):
            # Every entry made here is an error. A draft's is reported, not held against the run; a bundle that
            # does not load is held against the run, since nothing says its claims are a draft.
            if not entry["approved"]:
                continue
            verdict.status = worst([verdict.status, "ERROR"])
            verdict.summary = f"spec conform {name}: {entry['error']}; {verdict.summary}"
            signature = verdict.signature
            if not signature["crashes"]:
                signature["failures"][f"spec-conform:{name}"] = {
                    "status": "ERROR",
                    "verdict": verdict_clauses(entry["error"]),
                }
                signature["failing_components"] = sorted(signature["failures"])
                signature["status"] = verdict.status
        return conformance

    def summarize(
        self, runner: "BatchRunner", results: Mapping[str, Mapping[str, Any]], main: Sequence[Mapping[str, Any]]
    ) -> tuple[Any, list[str]]:
        """For each spec bundle the runs' plans name (zoo runs and serial re-runs left out, as from ``counts``):
        its conform report over the runs checked against it, written to ``conformance/<bundle>.json``, and a
        digest of it, claimed violations first."""

        by_bundle: dict[str, list[dict[str, Any]]] = {}
        for result in main:
            for name, entry in (result.get("conformance") or {}).items():
                by_bundle.setdefault(name, []).append({"run": result["id"], **entry})
        digests = {}
        for name, entries in sorted(by_bundle.items()):
            counts = dict(Counter(entry["status"] for entry in entries))
            skipped = sorted({entry["reason"] for entry in entries if entry["status"] == "skipped"})
            checked = [entry for entry in entries if entry["status"] in ("pass", "fail", "error")]
            digest: dict[str, Any] = {"counts": counts, "skipped": skipped}
            try:
                bundle = load_bundle(name)
            except SpecBundleError as exc:
                digests[name] = {**digest, "status": "error", "error": str(exc)}
                continue
            report = summarize(bundle, checked)
            path = os.path.join(CONFORMANCE_DIRECTORY, f"{name}.json")
            write_document(os.path.join(runner.folder, path), {**report, "counts": counts})
            if checked:
                status = report["status"]
            else:
                status = next(item for item in ("skipped", "unexercised", "not_run") if item in counts)
            digests[name] = {
                **digest,
                "status": status,
                "approved": bundle.claims.status == "approved",
                "report": path,
                "violations": sorted(
                    (
                        {
                            "run": entry["run"],
                            "property": prop,
                            "claimed": item["claimed"],
                            "first": item.get("first"),
                        }
                        for entry in checked
                        for prop, item in sorted((entry.get("properties") or {}).items())
                        if item["status"] == "violated"
                    ),
                    key=lambda item: not item["claimed"],
                ),
                "errors": [
                    {"run": entry["run"], "error": entry["error"]} for entry in checked if entry["status"] == "error"
                ],
                "transition_violations": [
                    {"run": entry["run"], **entry["transitions"]["first_violation"]}
                    for entry in checked
                    if (entry.get("transitions") or {}).get("holds") is False
                ],
                "action_coverage": report["action_coverage"]["observed"],
                "never_observed": report["action_coverage"]["never_observed"],
                "targets_reached": report["targets_reached"],
            }
        return digests, []

    def describe(self, entry: Any) -> list[str]:
        return [line for name, digest in entry.items() for line in _describe(name, digest)]


def _describe(name: str, digest: Mapping[str, Any]) -> list[str]:
    """The command's lines for one bundle's digest: its status and counts, up to five violations and errors, the
    actions never observed, the reasons runs were skipped, and the error of a bundle that did not load."""

    counts = ", ".join(f"{status} {count}" for status, count in sorted(digest["counts"].items()))
    claims = " (approved)" if digest.get("approved") else " (draft)" if "approved" in digest else ""
    lines = [f"  spec conform {name}{claims}: {digest['status']} ({counts})"]
    for item in digest.get("violations", [])[:5]:
        outside = "" if item["claimed"] else " (outside the claim)"
        lines.append(f"    runs/{item['run']}: {item['property']} violated{outside}")
    lines += [f"    runs/{item['run']}: {item['error'][:200]}" for item in digest.get("errors", [])[:5]]
    if digest.get("never_observed"):
        lines.append(f"    never observed: {', '.join(digest['never_observed'])}")
    lines += [f"    skipped: {reason}" for reason in digest["skipped"]]
    if digest.get("error"):
        lines.append(f"    {digest['error']}")
    return lines
