"""The findings store: ``findings.json``, one entry per failure signature, across batches.

A batch appends the failure signatures of its summary to a store when it finishes: its own ``findings.json``
beside ``summary.json``, or the file ``--findings PATH`` names, which several batches (a nightly soak's, say)
share. An entry is one :func:`~valkey_fuzzer.minimize.signature_key`: its label and signature, when it was first
and last seen, the runs that hit it (each by its batch folder and job id, so ``count`` is distinct runs, and a
resumed batch adds nothing twice), the command that reproduces the latest of them, and a ``status``:

- ``new``: no earlier batch in this store had seen it;
- ``known``: a second batch has seen it since (set when that batch records it);
- ``fixed``: set by hand, when the bug is fixed (``valkey-fuzzer findings`` only reads the store); a fixed
  finding a batch sees again is ``new`` again.

The file is rewritten whole, atomically, and carries ``format_version`` :data:`~valkey_fuzzer.reports.FORMAT_VERSION`
(docs/stability.md: a key is never removed or renamed without a bump)::

    {
      "format_version": 1,
      "findings": {
        "<key>": {
          "key": "<key>", "label": "FAIL | valkey.aof-durability | data:FAIL | lost N keys", "status": "new",
          "experiment": "valkey.aof-durability", "outcome": "FAIL", "signature": {...},
          "first_seen": "2026-11-20T01:00:00Z", "last_seen": "2026-11-21T01:00:00Z", "count": 2,
          "runs": [{"batch": "/soak/20261120T0100", "id": "random-valkey.aof-durability-s3"}, ...],
          "reproduce": "valkey-fuzzer replay /soak/20261121T0100/runs/random-valkey.aof-durability-s7/report.json"
        }
      }
    }

``valkey-fuzzer findings list`` and ``show`` read a store (:mod:`valkey_fuzzer.commands.findings`).
"""

import os
import shlex
from collections.abc import Mapping
from typing import Any, Optional

from .reports import FORMAT_VERSION, InputError, format_version_problem, load_document, replay_command, write_document

FINDINGS_FILE = "findings.json"
STATUSES = ("new", "known", "fixed")


def load(path: str) -> dict[str, Any]:
    """The store at ``path``; an empty one when the file does not exist. An ``InputError`` names a file another
    fuzzer version wrote, or one that is not a store."""

    if not os.path.exists(path):
        return {"format_version": FORMAT_VERSION, "findings": {}}
    data = load_document(path)
    problem = format_version_problem(data, path)
    if problem is not None:
        raise InputError(problem)
    if not isinstance(data, Mapping) or not isinstance(data.get("findings"), Mapping):
        raise InputError(f"{path} is not a findings store: it has no 'findings' mapping")
    for key, entry in data["findings"].items():
        # The status is the one field edited by hand (to fixed).
        if not isinstance(entry, Mapping) or entry.get("status") not in STATUSES:
            raise InputError(f"{path}: finding {key} needs a status out of {', '.join(STATUSES)}")
    return {"format_version": FORMAT_VERSION, **data}


def entries(store: Mapping[str, Any], status: Optional[str] = None) -> list[dict[str, Any]]:
    """The store's findings (those of one ``status``, if given): new before known before fixed, then the last
    seen first, then the most runs first."""

    listed = [dict(entry) for entry in store["findings"].values() if status is None or entry["status"] == status]
    listed.sort(key=lambda entry: (entry["last_seen"], entry["count"]), reverse=True)
    listed.sort(key=lambda entry: STATUSES.index(entry["status"]))
    return listed


def find(store: Mapping[str, Any], key: str, path: str) -> dict[str, Any]:
    """The finding whose key is ``key``, or starts with it when that names exactly one; an ``InputError`` otherwise."""

    findings = store["findings"]
    if key in findings:
        return dict(findings[key])
    matching = sorted(candidate for candidate in findings if candidate.startswith(key))
    if len(matching) == 1:
        return dict(findings[matching[0]])
    if matching:
        raise InputError(f"{key!r} names {len(matching)} findings in {path}: {', '.join(matching)}")
    raise InputError(
        f"no finding {key!r} in {path}; `valkey-fuzzer findings list --store {shlex.quote(path)}` lists them"
    )


def _reproduce(summary: Mapping[str, Any], run_ids: list[str]) -> Optional[str]:
    """The command that runs the latest of ``run_ids`` again: a replay of its report, else a run of the scenario it
    drew; None when it left neither."""

    folder = summary["folder"]
    reports = {run["id"]: run.get("report") for run in summary["runs"]}
    for run_id in reversed(run_ids):
        if reports.get(run_id):
            return replay_command(os.path.join(folder, reports[run_id]))
        scenario = os.path.join(folder, "runs", run_id, "scenario.yaml")
        if os.path.exists(scenario):
            return shlex.join(["valkey-fuzzer", "run", scenario])
    return None


def record(path: str, summary: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Append the failure signatures of a batch ``summary`` (its ``signatures`` groups) to the store at ``path``,
    and return the entries that gained a run, by key.

    A run already recorded (same batch folder and job id) counts once, so a resumed or re-summarized batch changes
    nothing. An entry that gains a run is seen now (the summary's ``updated_at``); one the store had as ``new``
    becomes ``known`` when the run is another batch's; a ``fixed`` one becomes ``new``.
    """

    store = load(path)
    findings = store["findings"]
    seen_at = summary["updated_at"]
    touched: dict[str, dict[str, Any]] = {}
    for group in summary["signatures"]:
        entry = findings.get(group["key"])
        if entry is None:
            entry = findings[group["key"]] = {
                "key": group["key"],
                "label": group["label"],
                "status": "new",
                "experiment": group["experiment"],
                "outcome": group["status"],
                "signature": group["signature"],
                "first_seen": seen_at,
                "last_seen": seen_at,
                "count": 0,
                "runs": [],
                "reproduce": None,
            }
            batches_before: set[str] = set()
        else:
            batches_before = {run["batch"] for run in entry["runs"]}
        recorded = {(run["batch"], run["id"]) for run in entry["runs"]}
        added = [run_id for run_id in group["runs"] if (summary["folder"], run_id) not in recorded]
        if not added:
            continue
        entry["runs"].extend({"batch": summary["folder"], "id": run_id} for run_id in added)
        entry["count"] = len(entry["runs"])
        entry["last_seen"] = seen_at
        entry["reproduce"] = _reproduce(summary, added) or entry["reproduce"]
        if entry["status"] == "fixed":
            entry["status"] = "new"
        elif entry["status"] == "new" and batches_before and summary["folder"] not in batches_before:
            entry["status"] = "known"
        touched[group["key"]] = entry
    write_document(path, store)
    return touched
