"""Spec conformance as a batch plug-in: each run checked against the spec bundles its plan names, as
``valkey-fuzzer spec conform`` would, and a conform report per bundle over the batch's runs.

The worker writes ``conform.json`` into its run folder (TLC from ``$TLA2TOOLS_JAR``; without it a bundle that
needs TLC is skipped, or an error if its claims are approved). For a bundle whose claims are approved, a
claimed property violated on the run's observed states fails the run, and a check that could not decide is an
ERROR; a draft's findings are only reported. The summary writes ``conformance/<bundle>.json`` and a digest of
it, claimed violations first.

:mod:`~valkey_fuzzer.spec.conform` is imported where it is used, never when this module loads: registering the
plug-in leaves the spec bridge's tools unloaded for a batch whose runs name no bundle.
"""

import os
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Optional

from ..minimize import verdict_clauses
from ..reports import write_document
from ..spec.bundle import SpecBundleError, load_bundle
from ..spec.tla import TlaError, TlaTools
from .hooks import BatchPlugin
from .results import Verdict, read_json
from .spec import worst

if TYPE_CHECKING:
    from .pool import BatchRunner

# What a worker writes in its run folder: the run checked against the spec bundles its plan names.
CONFORMANCE_FILE = "conform.json"
# How long a worker's spec conform may run TLC on its run's observed states.
CONFORM_TIMEOUT_S = 300.0
CONFORMANCE_DIRECTORY = "conformance"


def _conform(report_dir: str) -> None:
    """Write ``conform.json``: the run checked against every spec bundle its plan names, with TLC from
    ``$TLA2TOOLS_JAR`` (without it, a bundle that needs TLC is skipped, or an error if its claims are approved)."""

    from ..spec.conform import conform_folder

    try:
        tools, unavailable = TlaTools.locate(None, workers="1").within(CONFORM_TIMEOUT_S), ""
    except TlaError as exc:
        tools, unavailable = None, str(exc)
    results = conform_folder(report_dir, tools, unavailable)
    if not results:
        return
    write_document(os.path.join(report_dir, CONFORMANCE_FILE), results)
    for name, entry in results.items():
        print(
            f"spec conform {name}: {entry['status']}"
            + (f": {entry.get('error') or entry.get('reason')}" if entry["status"] not in ("pass", "fail") else "")
        )


def _conformance(run_dir: str, report: Optional[Mapping[str, Any]]) -> dict[str, Any]:
    """The worker's ``conform.json``; for a run that reached its verdict and whose plan names spec bundles but
    that has none, an error for each bundle, saying whether its claims are approved (the batch holds only those
    against the run)."""

    path = os.path.join(run_dir, CONFORMANCE_FILE)
    if os.path.exists(path):
        try:
            return read_json(path)
        except (OSError, ValueError) as exc:
            problem = f"{CONFORMANCE_FILE} cannot be read: {exc}"
    else:
        problem = f"no {CONFORMANCE_FILE}: the worker ended before spec conform checked the run"
    from ..spec.conform import CHECKED_RUN_STATUSES, plan_bundles

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
    """Spec conformance: the worker checks its run, the verdict takes an approved bundle's claimed violation as a
    FAIL, and the summary reports each bundle over the runs that exercised it."""

    name = "conformance"
    kept_when_pruned = (CONFORMANCE_FILE,)

    def after_run(self, report_dir: str) -> None:
        _conform(report_dir)

    def judge(self, job: Mapping[str, Any], run_dir: str, report: Optional[Mapping[str, Any]], verdict: Verdict) -> Any:
        conformance = _conformance(run_dir, report)
        for name, entry in sorted(conformance.items()):
            # A draft's findings are reported, not held against the run. Every entry says whether its claims are
            # approved: the worker's (conform_folder), and the ones made here for a run whose worker wrote no
            # conform.json (_conformance); a bundle that does not load is held against the run, since nothing
            # says its claims are a draft.
            if entry["status"] not in ("fail", "error") or not entry["approved"]:
                continue
            if entry["status"] == "fail":
                violated = sorted(
                    prop
                    for prop, item in entry["properties"].items()
                    if item["status"] == "violated" and item["claimed"]
                )
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

        from ..spec.conform import summarize

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
                "action_coverage": report["action_coverage"]["observed"],
                "never_observed": report["action_coverage"]["never_observed"],
                "targets_reached": report["targets_reached"],
            }
        return digests, []
