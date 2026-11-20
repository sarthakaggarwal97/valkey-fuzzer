"""Spec conformance as a batch plug-in: each run checked against the spec bundles its plan names, as
``valkey-fuzzer spec conform`` would, and a conform report per bundle over the batch's runs.

A run checks itself as its report is written (:meth:`~valkey_fuzzer.service.RunService.write_run_report`, under
the batch's ``conform_tlc_timeout_s``): its ``report.json`` carries the results under ``conform`` and the verdict
they give, so the plug-in reuses them rather than running TLC twice. For a run that predates that section, the
worker writes ``conform.json`` into its run folder after the run (TLC from ``$TLA2TOOLS_JAR``, bounded the same
way; without TLC a bundle that needs it is skipped, or an error if its claims are approved), and the verdict is
formed here: for a bundle whose claims are approved, a claimed property violated on the run's observed states
fails the run, and a check that could not decide is an ERROR; a draft's findings are only reported. The summary
writes ``conformance/<bundle>.json`` and a digest of it, claimed violations first.
"""

import os
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Optional

from ..minimize import verdict_clauses
from ..reports import write_document
from ..spec.bundle import SpecBundleError, load_bundle
from ..spec.conform import CHECKED_RUN_STATUSES, conform_run, describe, plan_bundles, summarize
from .hooks import BatchPlugin
from .results import Verdict, read_json
from .spec import BatchOptions, worst

if TYPE_CHECKING:
    from .pool import BatchRunner

# What a worker writes in its run folder for a run whose report has no conform section: the run checked against
# the spec bundles its plan names.
CONFORMANCE_FILE = "conform.json"
# The batch option that bounds a worker's TLC run on its run's observed states (BatchOptions).
CONFORM_TIMEOUT_OPTION = "conform_tlc_timeout_s"
CONFORMANCE_DIRECTORY = "conformance"


def _recorded(report: Optional[Mapping[str, Any]]) -> Optional[dict[str, Any]]:
    """The run's own conform results, from its report's ``conform`` section; None for a run that predates it (or
    checked no bundle), whose check and verdict are the plug-in's."""

    recorded = (report or {}).get("conform")
    return dict(recorded) if isinstance(recorded, Mapping) and recorded else None


def _conform(report_dir: str, timeout_s: float) -> None:
    """Print the run's own conform results, or, for a run whose report has none, check it now and write
    ``conform.json``: the run against every spec bundle its plan names, with TLC for at most ``timeout_s``
    (:func:`~valkey_fuzzer.spec.conform.conform_run`)."""

    try:
        report = read_json(os.path.join(report_dir, "report.json"))
    except (OSError, ValueError):
        report = None
    results = _recorded(report)
    if results is None:
        results = conform_run(report_dir, timeout_s)
        if not results:
            return
        write_document(os.path.join(report_dir, CONFORMANCE_FILE), results)
    for name, entry in results.items():
        print(f"spec conform {describe(name, entry)}")


def _conformance(run_dir: str, report: Optional[Mapping[str, Any]]) -> dict[str, Any]:
    """The run's own conform results (its report's ``conform``), else the worker's ``conform.json``; for a run
    that reached its verdict and whose plan names spec bundles but that has neither, an error for each bundle,
    saying whether its claims are approved (the batch holds only those against the run)."""

    recorded = _recorded(report)
    if recorded is not None:
        return recorded
    path = os.path.join(run_dir, CONFORMANCE_FILE)
    if os.path.exists(path):
        try:
            return read_json(path)
        except (OSError, ValueError) as exc:
            problem = f"{CONFORMANCE_FILE} cannot be read: {exc}"
    else:
        problem = f"no {CONFORMANCE_FILE}: the worker ended before spec conform checked the run"
    if report is None or report.get("status") not in CHECKED_RUN_STATUSES:
        return {}
    plan = (report.get("outcome") or {}).get("plan") or {}
    return {name: {"status": "error", "error": problem, "approved": _approved(name)} for name in plan_bundles(plan)}


def _approved(name: str) -> bool:
    """Whether the claims of spec bundle ``name`` are approved. A bundle that does not load counts as approved:
    the plan named it, and nothing says its claims are a draft, so the run is not passed over its check."""

    try:
        return load_bundle(name).claims.status == "approved"
    except SpecBundleError:
        return True


class ConformancePlugin(BatchPlugin):
    """Spec conformance: the run's own check is reused (or the worker checks a run that has none), the verdict
    takes an approved bundle's claimed violation as a FAIL, and the summary reports each bundle over the runs that
    exercised it."""

    name = "conformance"
    kept_when_pruned = (CONFORMANCE_FILE,)

    def after_run_timeout_s(self, options: BatchOptions) -> float:
        return options.conform_tlc_timeout_s

    def after_run(self, report_dir: str, task: Mapping[str, Any]) -> None:
        _conform(report_dir, task[CONFORM_TIMEOUT_OPTION])

    def judge(self, job: Mapping[str, Any], run_dir: str, report: Optional[Mapping[str, Any]], verdict: Verdict) -> Any:
        conformance = _conformance(run_dir, report)
        if _recorded(report) is not None:
            # The run judged itself (spec.conform.judge_run): its status, summary and signature, formed from its
            # outcome, already carry an approved bundle's finding as the failed component spec-conform:<bundle>.
            return conformance
        for name, entry in sorted(conformance.items()):
            # A draft's findings are reported, not held against the run. Every entry says whether its claims are
            # approved: the worker's (conform_folder), and the ones made here for a run whose worker wrote no
            # conform.json (_conformance); a bundle that does not load is held against the run, since nothing
            # says its claims are a draft. An entry that does not say is read as a draft: only an explicit
            # approval decides a run.
            if entry["status"] not in ("fail", "error") or not entry.get("approved", False):
                continue
            if entry["status"] == "fail":
                violated = sorted(
                    prop
                    for prop, item in entry["properties"].items()
                    if item["status"] == "violated" and item["claimed"]
                )
                if (entry.get("transitions") or {}).get("holds") is False:
                    violated.append("transitions")
                clauses, text = [f"{prop} violated" for prop in violated], f"{', '.join(violated)} violated"
            else:
                clauses, text = verdict_clauses(entry["error"]), entry["error"]
            verdict.status = worst([verdict.status, entry["status"].upper()])
            verdict.summary = f"spec conform {name}: {text}; {verdict.summary}"
            signature = verdict.signature
            if not signature["crashes"]:
                signature["failures"][f"spec-conform:{name}"] = {"status": entry["status"].upper(), "verdict": clauses}
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
