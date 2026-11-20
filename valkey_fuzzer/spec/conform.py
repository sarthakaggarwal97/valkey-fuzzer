"""``spec conform``: rebuild a model's states from real runs and check the claims on them.

For each run, the mapping turns the run's records (fuzzer events, including
log lines published as events, and state samples) into a sequence of
observed states over the model's variables. TLC then evaluates every claimed
property marked ``conform: true`` and every target on each observed state,
through ``INSTANCE model WITH <var> <- Trace[k].<var>``, so the properties are
the model's own definitions.

An event is a step: it counts for its model action and applies its updates.
A sample is an observed state, not a step: a trace observer read it from the
node (``valkey.observer.trace``, as the mapping's ``sample:`` section asks),
and its updates set the variables to what the node reported. When a sample
sets a variable to another value than the events had it at, the sample is
taken (the node said so) and the disagreement is reported in
``sample_mismatches``, by variable and key, never overwritten in silence.

TLC also checks the transitions (:func:`_transition_section`): from the first
observed state, with the hidden variables at the mapping's ``hidden`` value,
the model must take the action each record shows (or no step, when earlier
steps already account for it) to a state that agrees with what the record
says, with up to ``max_unobserved_steps`` (``claims.yaml``) steps of the
actions the mapping calls unobservable in between; a sample only raises what
it reports. A pass is observed-projection refinement: the run's observed steps
are steps of the model. It is not full refinement: hidden state is the model's
own guess, and a record says nothing about the variables it does not update.

A run's records are its committed events, or a trace a component wrote: a
JSONL file :data:`TRACE_FILE` in the component's artifact folder
(``components/<id>/``) or anywhere under a directory given. The spec bridge
owns this format: the first record is the header, ``{"kind": "header",
"format_version": <:data:`~valkey_fuzzer.reports.FORMAT_VERSION`>,
"nodes": {<node>: {"node_id": <ID>, "config": {...}}}, "scenario_id", "seed",
"experiment_id", "inputs"}``, and every later record is one the mapping's
evidence matches. A ``node_id`` field of the mapping resolves to its node
through ``nodes``, and a constant mapped from a ``config`` reads ``nodes.<node>.config``.

A run whose plan names a bundle (a component's ``spec``) checks itself as its
report is written (:func:`conform_run`, from
:meth:`~valkey_fuzzer.service.RunService.write_run_report`): the results go
into the report's ``conform`` section, by bundle, and :func:`judge_run` turns
an approved bundle's claimed violation into a failed component
:func:`conform_component` of the run, so ``minimize``, ``replay`` and the bug
zoo see a failure only the model finds. A batch reuses that section
(:mod:`~valkey_fuzzer.batch.conform_plugin`).
"""

import copy
import glob
import json
import os
import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from collections.abc import Set as AbstractSet
from dataclasses import dataclass, field, replace
from typing import Any, Optional

from .._data import error_record, jsonable
from ..documents import ABSENT, get_path
from ..reports import format_version_problem
from ..results import ComponentResult, ComponentStatus, OutcomeStatus, RunOutcome
from .bundle import (
    MODEL_FILE,
    TRACE_FILE,
    Bundle,
    Evidence,
    SpecBundleError,
    SpecMapping,
    StateVar,
    field_ref,
    load_bundle,
    where_matches,
)
from .tla import (
    ModelValue,
    TlaError,
    TlaFunction,
    TlaSet,
    TlaTools,
    definition_body,
    definitions,
    print_values,
    render,
    tlc,
)

CHECKS = "observed-state check and action coverage"
REFINEMENT = (
    "not checked: consecutive observed states are not checked against the model's Next (with stuttering and "
    "hidden steps); a pass means every observed state satisfies the checked properties, not that the run "
    "is a behavior of the model"
)
CHECKS_WITH_TRANSITIONS = "observed-state check, transition check, and action coverage"
REFINEMENT_WITH_TRANSITIONS = (
    "observed-projection refinement: from the first observed state, the model takes the action each record shows "
    "(or no step) to a state that agrees with what the record says, with up to max_unobserved_steps unobservable "
    "steps between records; hidden variables follow the model from the mapping's value. Not checked: that hidden "
    "state matched the run, or the variables a record does not update"
)
_TAG = "spec-conform"
# The run statuses of a run that reached its verdict: the only runs conform_folder checks.
CHECKED_RUN_STATUSES = ("PASS", "FAIL")
# The batch option that bounds the TLC run of a run's own check (conform_run); a run outside a batch uses its default.
CONFORM_TIMEOUT_OPTION = "conform_tlc_timeout_s"


class _SkipError(Exception):
    """A record's evidence cannot be applied (a field is missing or names an unknown node)."""


class _OutsideProcessError(_SkipError):
    """An update keys on a node that is not in the process set it ranges over."""


@dataclass
class RunEvidence:
    """What one run left behind: its records in order, the trace header, and the run's identity.

    ``problem`` says why a run folder could not be read; the run is then an
    error rather than left out, so a conformance pass covers every run.
    """

    source: str
    header: dict[str, Any]
    records: list[dict[str, Any]]
    meta: dict[str, Any] = field(default_factory=dict)
    problem: Optional[str] = None


# ---------------------------------------------------------------------------
# Reading runs
# ---------------------------------------------------------------------------


def _jsonl(path: str) -> list[dict[str, Any]]:
    with open(path, encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def _trace_run(path: str, meta: Optional[dict[str, Any]] = None) -> RunEvidence:
    records = _jsonl(path)
    if not records or records[0].get("kind") != "header":
        raise ValueError(f"{path} does not start with a trace header")
    header = records[0]
    problem = format_version_problem(header, path)
    if problem is not None:
        raise ValueError(problem)
    meta = dict(meta or {})
    for key in ("scenario_id", "seed", "experiment_id", "inputs"):
        meta.setdefault(key, header.get(key))
    return RunEvidence(source=path, header=header, records=records[1:], meta=meta)


def _run_report(path: str) -> Optional[dict[str, Any]]:
    """The content of a report.json that is a run report, else None."""

    try:
        with open(path, encoding="utf-8") as handle:
            report = json.load(handle)
    except (OSError, ValueError):
        return None
    if isinstance(report, dict) and isinstance(report.get("outcome"), dict) and "plan" in report["outcome"]:
        return report
    return None


def _report_run(path: str, report: Mapping[str, Any]) -> RunEvidence:
    outcome = report["outcome"]
    plan = outcome["plan"]
    meta = {
        "scenario_id": plan.get("scenario_id"),
        "seed": plan.get("seed"),
        "experiment_id": plan.get("experiment_id"),
        "inputs": plan.get("inputs"),
        "run_status": report.get("status"),
        "report": path,
    }
    problem = format_version_problem(report, path)
    if problem is not None:
        return RunEvidence(source=path, header={}, records=[], meta=meta, problem=problem)
    folder = os.path.dirname(path)
    traces = sorted(glob.glob(os.path.join(folder, "components", "*", TRACE_FILE)))
    if traces:
        try:
            return _trace_run(traces[0], meta)
        except (OSError, ValueError) as exc:
            return RunEvidence(source=path, header={}, records=[], meta=meta, problem=f"{traces[0]}: {exc}")
    recorded = [
        artifact
        for item in outcome.get("evidence") or []
        for artifact in item.get("artifacts") or []
        if str(artifact).endswith(TRACE_FILE)
    ]
    if recorded:
        # The run recorded a trace that is gone (a pruned run keeps only its report, scenario, and conform.json):
        # its events alone would miss the samples the trace carries, so the run is not checked on them.
        problem = f"the trace the run recorded is missing (a pruned run keeps its conform.json): {recorded[0]}"
        return RunEvidence(source=path, header={}, records=[], meta=meta, problem=problem)
    events = sorted(
        (item for item in (outcome.get("diagnostics") or {}).get("events") or [] if item.get("committed")),
        key=lambda item: item.get("sequence", 0),
    )
    records = [{"kind": "event", "t": item.get("timestamp"), **item} for item in events]
    return RunEvidence(source=path, header={"nodes": {}}, records=records, meta=meta)


def _folder_run(folder: str) -> RunEvidence:
    """The run in a run folder: its report, else (a run killed before it wrote one) its trace."""

    report_path = os.path.join(folder, "report.json")
    if os.path.exists(report_path):
        report = _run_report(report_path)
        if report is not None:
            return _report_run(report_path, report)
        problem = f"{report_path}: report.json is not a readable run report"
    else:
        traces = sorted(glob.glob(os.path.join(folder, "components", "*", TRACE_FILE)))
        if not traces:
            problem = f"{folder}: the run left no report.json and no trace"
        else:
            try:
                return _trace_run(traces[0])
            except (OSError, ValueError) as exc:
                problem = f"{traces[0]}: {exc}"
    return RunEvidence(source=folder, header={}, records=[], problem=problem)


def _directory_runs(path: str) -> list[RunEvidence]:
    """Every run folder under ``path`` (one with plan.json or report.json), then traces outside them."""

    folders = sorted(
        {
            os.path.dirname(item)
            for name in ("plan.json", "report.json")
            for item in glob.glob(os.path.join(path, "**", name), recursive=True)
        }
    )
    runs = [_folder_run(folder) for folder in folders]
    for trace in sorted(glob.glob(os.path.join(path, "**", TRACE_FILE), recursive=True)):
        if not any(trace.startswith(folder + os.sep) for folder in folders):
            try:
                runs.append(_trace_run(trace))
            except (OSError, ValueError) as exc:
                runs.append(RunEvidence(source=trace, header={}, records=[], problem=f"{trace}: {exc}"))
    return runs


def collect_runs(paths: Iterable[str]) -> list[RunEvidence]:
    """Runs from report.json files, run folders, directories of them, or trace files.

    In a directory, every run folder counts: one whose report cannot be
    read, or that left neither a report nor a trace, is collected with its
    ``problem``, and a run killed before its report counts by its trace.
    """

    runs = []
    for path in paths:
        if os.path.isdir(path):
            runs.extend(_directory_runs(path))
        elif path.endswith(".jsonl"):
            runs.append(_trace_run(path))
        else:
            report = _run_report(path)
            if report is None:
                raise ValueError(f"{path} is not a run report.json, a run folder, or a trace.jsonl file")
            runs.append(_report_run(path, report))
    return runs


# ---------------------------------------------------------------------------
# Observed states
# ---------------------------------------------------------------------------


def _identity(prefix: str, node: str) -> ModelValue:
    return ModelValue(prefix + "_" + re.sub(r"[^A-Za-z0-9]", "_", node))


class ObservedRun:
    """One run's records replayed through the mapping into observed states."""

    def __init__(self, mapping: SpecMapping, run: RunEvidence):
        self.mapping = mapping
        self.run = run
        nodes = run.header.get("nodes") or {}
        self.by_node_id = {info["node_id"]: node for node, info in nodes.items() if info.get("node_id")}
        self.notes: list[str] = []
        # Mapped records (or their updates) that could not be applied: the states may miss what they show.
        self.unapplied: list[str] = []
        # A sample that set a variable to another value than the events had it at: the sample is taken as the
        # observed state, and the disagreement is reported, by field, rather than overwritten in silence.
        self.sample_mismatches: list[dict[str, Any]] = []
        # Which kind of evidence last set each variable (and key): "event" or "sample"; unset means its init.
        self._set_by: dict[tuple[str, Any], str] = {}
        self.actions: Counter = Counter()
        self.members = self._members()
        self.states: list[dict[str, Any]] = []
        self.steps: list[dict[str, Any]] = []
        # Per record that is a mapped action or changes the state (for the transition check): the step's entry
        # (as in ``steps``, with ``state`` the observed state after it), its ``kind`` (``init``, ``step`` for a
        # mapped action, ``sample`` for a record without one), its ``action``, and what the record says of that
        # state: ``eq`` are paths, ``(var,)`` or ``(var, key)``, that hold the state's value; ``has`` and ``lacks``
        # are ``(path, value)`` pairs the set at the path holds or not; ``lo`` are paths the state's value is a
        # lower bound of (a ``max`` update).
        self.reports: list[dict[str, Any]] = []

    # -- fields and values --------------------------------------------------

    def _fields(self, evidence: Evidence, record: Mapping[str, Any]) -> dict[str, Any]:
        body = evidence.body(record)
        taken = evidence.after.take if evidence.after else ()
        values = {}
        for spec in evidence.fields.values():
            if spec.name in taken:
                # Copied from the record this one follows, once that record is found (replay).
                continue
            value = get_path(body, spec.path)
            if value is ABSENT or value is None:
                if not spec.optional:
                    raise _SkipError(f"{evidence.name}: no {spec.path}")
                value = spec.default
            value = None if value is None else self._typed(evidence, spec.name, spec.type, value)
            values[spec.name] = spec.map.get(value, value) if spec.map else value
        return values

    def _typed(self, evidence: Evidence, name: str, kind: str, value: Any) -> Any:
        try:
            if kind == "int":
                return int(value)
            if kind == "bool":
                # bool("false") is True: take a JSON boolean, or the word true or false.
                if isinstance(value, bool):
                    return value
                if isinstance(value, str) and value.lower() in ("true", "false"):
                    return value.lower() == "true"
                raise ValueError(value)
            if kind == "node_id":
                if value not in self.by_node_id:
                    raise _SkipError(f"{evidence.name}: {name} {value} is no node of the trace header")
                return self.by_node_id[value]
            return str(value)
        except (TypeError, ValueError) as exc:
            raise _SkipError(f"{evidence.name}: {name} = {value!r} is not {kind}") from exc

    def _members(self) -> dict[str, list[str]]:
        members: dict[str, list[str]] = {name: [] for name in self.mapping.processes}
        for record in self.run.records:
            for name, process in self.mapping.processes.items():
                for rule in process.members:
                    evidence = self.mapping.evidence[rule.evidence]
                    if not evidence.matches(record):
                        continue
                    body = evidence.body(record)
                    if not where_matches(body, rule.where):
                        continue
                    if not all(get_path(body, key) not in (ABSENT, None, [], {}, "") for key in rule.require):
                        continue
                    try:
                        node = self._fields(evidence, record)[rule.field]
                    except _SkipError:
                        continue
                    if node not in members[name]:
                        members[name].append(node)
        return {name: sorted(nodes) for name, nodes in members.items()}

    def identities(self, process: str) -> list[ModelValue]:
        prefix = self.mapping.processes[process].prefix
        return [_identity(prefix, node) for node in self.members[process]]

    def _identity_of(self, node: str, process: str) -> ModelValue:
        if node not in self.members[process]:
            raise _OutsideProcessError(f"{node} is not one of {process}")
        return _identity(self.mapping.processes[process].prefix, node)

    def _value(self, spec: Any, fields: Mapping[str, Any]) -> Any:
        if isinstance(spec, str):
            ref = field_ref(spec)
            if not ref:
                return spec
            value = fields[ref[0]]
            return self._identity_of(value, ref[1]) if ref[1] else value
        if isinstance(spec, list):
            return tuple(self._value(item, fields) for item in spec)
        if isinstance(spec, Mapping):
            if set(spec) == {"set"}:
                values = TlaSet()
                for item in spec["set"]:
                    value = self._value(item, fields)
                    if value not in values:
                        values.append(value)
                return values
            return {key: self._value(item, fields) for key, item in spec.items()}
        return spec

    def _keys(self, var: StateVar, spec: Any, fields: Mapping[str, Any]) -> list[Any]:
        if spec is None:
            raise _SkipError(f"{var.name} is a function; the update needs a key")
        ref = field_ref(str(spec))
        if ref and not ref[1]:
            # A node in every process set the variable ranges over.
            node = fields[ref[0]]
            keys = [self._identity_of(node, process) for process in var.over if node in self.members[process]]
            if not keys:
                raise _OutsideProcessError(f"{node} is in none of {list(var.over)}")
            return keys
        return [self._value(spec, fields)]

    # -- replay ---------------------------------------------------------------

    def initial(self) -> dict[str, Any]:
        state: dict[str, Any] = {}
        for var in self.mapping.state.values():
            if var.hidden is not None:
                continue
            if var.over:
                state[var.name] = TlaFunction(
                    (key, self._value(var.init, {})) for process in var.over for key in self.identities(process)
                )
            else:
                state[var.name] = self._value(var.init, {})
        return state

    def replay(self) -> list[dict[str, Any]]:
        state = self.initial()
        self.states = [copy.deepcopy(state)]
        self.steps = [{"state": 1, "why": "initial state"}]
        self.reports = [
            {"state": 1, "why": "initial state", "kind": "init", "action": None, "repeats": False}
            | {key: [] for key in ("eq", "has", "lacks", "lo", "changes", "unchanged")}
        ]
        applied: dict[str, list[Mapping[str, Any]]] = {name: [] for name in self.mapping.evidence}
        # Records waiting for the record they follow: trace order is log time to the millisecond, and two
        # nodes' lines of one millisecond can be read in either order.
        waiting: list[tuple[Evidence, Mapping[str, Any], Mapping[str, Any]]] = []
        for record in self.run.records:
            for evidence in self.mapping.evidence.values():
                if not evidence.matches(record):
                    continue
                try:
                    fields = self._fields(evidence, record)
                except _SkipError as exc:
                    self.unapplied.append(str(exc))
                    continue
                if not self._follows(evidence, fields, applied):
                    waiting.append((evidence, record, fields))
                    continue
                self._apply_record(state, evidence, record, fields, applied)
                ready = True
                while ready:
                    ready = next((item for item in waiting if self._follows(item[0], item[2], applied)), None)
                    if ready:
                        waiting.remove(ready)
                        self._apply_record(state, *ready, applied)
        for evidence, _, fields in waiting:
            after = evidence.after
            wanted = ", ".join(f"{name} {fields[spec[1:]]}" for name, spec in after.match.items())
            self.unapplied.append(f"{evidence.name}: no {after.evidence} record ({wanted}) before or after it")
        return self.states

    def _apply_record(
        self,
        state: dict[str, Any],
        evidence: Evidence,
        record: Mapping[str, Any],
        fields: dict[str, Any],
        applied: dict[str, list[Mapping[str, Any]]],
    ) -> None:
        """Apply a record whose ``after`` (if any) is satisfied, with the fields it takes from that record."""

        if evidence.after is not None and evidence.after.take:
            prior = self._prior(evidence, fields, applied)
            fields = {**fields, **{name: prior[name] for name in evidence.after.take}}
        self._step(state, evidence, record, fields)
        applied[evidence.name].append(fields)

    def _follows(
        self, evidence: Evidence, fields: Mapping[str, Any], applied: Mapping[str, list[Mapping[str, Any]]]
    ) -> bool:
        """Whether the record ``evidence`` follows (its ``after``) has been applied, or it follows none."""

        return evidence.after is None or self._prior(evidence, fields, applied) is not None

    def _prior(
        self, evidence: Evidence, fields: Mapping[str, Any], applied: Mapping[str, list[Mapping[str, Any]]]
    ) -> Optional[Mapping[str, Any]]:
        """The latest applied record of ``evidence.after.evidence`` whose fields match this record's, else None."""

        wanted = {name: fields[spec[1:]] for name, spec in evidence.after.match.items()}
        return next(
            (
                prior
                for prior in reversed(applied[evidence.after.evidence])
                if all(prior[name] == value for name, value in wanted.items())
            ),
            None,
        )

    def _step(
        self, state: dict[str, Any], evidence: Evidence, record: Mapping[str, Any], fields: Mapping[str, Any]
    ) -> None:
        before = copy.deepcopy(state)
        reported: list[tuple] = []
        for update in evidence.updates:
            # Each update stands alone: a node outside the process set one update ranges over still
            # gets the updates whose variables it is in.
            try:
                reported.extend(self._apply(state, update, fields, evidence, record))
            except _OutsideProcessError as exc:
                self._note(f"{evidence.name}: {update.op} {update.var} skipped: {exc}")
            except _SkipError as exc:
                self.unapplied.append(f"{evidence.name}: {update.op} {update.var}: {exc}")
        if evidence.action:
            self.actions[evidence.action] += 1
        step = {
            "state": len(self.states) + (state != before),
            "why": evidence.action or f"{evidence.name} sample",
            "evidence": evidence.name,
            "t": record.get("t"),
            "fields": {key: value for key, value in fields.items()},
        }
        if state != before:
            self.states.append(copy.deepcopy(state))
            self.steps.append(step)
        if state != before or evidence.action:
            # A mapped action that changed nothing observed is still a step the model must allow.
            self.reports.append({**step, **self._report(evidence, reported)})

    @staticmethod
    def _report(evidence: Evidence, reported: Sequence[tuple]) -> dict[str, Any]:
        """What the record says of the state after it, by how its updates said it (see ``reports``). A step's
        ``add`` and ``remove`` say only that the value is in the set, or not (a vote whose ACK is never logged
        stays in the observed ``held``); its ``max`` says nothing a step can check. A sample sets what it reports."""

        report: dict[str, Any] = {
            "kind": "step" if evidence.action else "sample",
            "action": evidence.action,
            "eq": [],
            "has": [],
            "lacks": [],
            "lo": [],
            # The mapping's step rule: the variable the action changes, or leaves unchanged (a denied vote), and
            # whether one record stands for several steps of the action (a line that reports every write at once).
            "changes": [evidence.step["changes"]] if evidence.step.get("changes") else [],
            "unchanged": [evidence.step["unchanged"]] if evidence.step.get("unchanged") else [],
            "repeats": bool(evidence.step.get("repeats")),
        }
        for path, op, value in reported:
            if evidence.action and op == "add":
                relation, item = "has", (path, value)
            elif evidence.action and op == "remove":
                relation, item = "lacks", (path, value)
            elif op == "max":
                if evidence.action:
                    continue
                relation, item = "lo", path
            else:
                relation, item = "eq", path
            if item not in report[relation]:
                report[relation].append(item)
        report["lo"] = [path for path in report["lo"] if path not in report["eq"]]
        return report

    def _note(self, text: str) -> None:
        if text not in self.notes and len(self.notes) < 100:
            self.notes.append(text)

    def _apply(
        self,
        state: dict[str, Any],
        update: Any,
        fields: Mapping[str, Any],
        evidence: Evidence,
        record: Mapping[str, Any],
    ) -> list[tuple]:
        """Apply ``update`` to ``state``; for each path it wrote, ``(var,)`` or ``(var, key)``: the path, the
        update's op, and the value it set, added, or removed."""

        var = self.mapping.state[update.var]
        if var.hidden is not None:
            raise _SkipError(f"{var.name} is hidden; it cannot be updated")
        targets = self._keys(var, update.key, fields) if var.over else [None]
        value = None if update.value is None else self._value(update.value, fields)
        for key in targets:
            current = state[var.name] if key is None else state[var.name][key]
            if update.op == "set":
                new = value
                if evidence.sample and self._set_by.get((var.name, key)) == "event" and current != new:
                    self._mismatch(evidence, record, var.name, key, current, new)
            elif update.op == "max":
                new = max(current, value)
            elif update.op == "incr":
                new = current + 1
            elif update.op == "clear":
                new = self._value(var.init, {})
            elif update.op == "add":
                new = TlaSet(current)
                if value not in new:
                    new.append(value)
            else:  # remove
                new = TlaSet(item for item in current if item != value)
            if key is None:
                state[var.name] = new
            else:
                state[var.name][key] = new
            self._set_by[(var.name, key)] = "sample" if evidence.sample else "event"
        return [((var.name,) if key is None else (var.name, key), update.op, value) for key in targets]

    def _mismatch(
        self, evidence: Evidence, record: Mapping[str, Any], var: str, key: Any, inferred: Any, sampled: Any
    ) -> None:
        if len(self.sample_mismatches) < 100:
            self.sample_mismatches.append(
                {
                    "evidence": evidence.name,
                    "var": var,
                    "key": None if key is None else str(key),
                    "inferred": render(inferred),
                    "sampled": render(sampled),
                    "t": record.get("t"),
                }
            )

    # -- constants ------------------------------------------------------------

    def constants(self) -> dict[str, str]:
        values: dict[str, str] = {}
        for name in self.mapping.processes:
            values[name] = render(TlaSet(self.identities(name)))
        for name, spec in self.mapping.constants.items():
            if "value" in spec:
                values[name] = str(spec["value"])
            elif "config" in spec:
                seen = [
                    info.get("config", {}).get(spec["config"])
                    for info in (self.run.header.get("nodes") or {}).values()
                    if spec["config"] in (info.get("config") or {})
                ]
                if not seen:
                    raise TlaError(f"constant {name} needs {spec['config']!r} from the trace header's node configs")
                values[name] = render(all(item == spec["equals"] for item in seen))
            elif "input" in spec:
                # The experiment sets the configuration from this input; the run records its value.
                inputs = self.run.meta.get("inputs") or {}
                if spec["input"] not in inputs:
                    raise TlaError(f"constant {name} needs input {spec['input']!r}, which the run does not record")
                values[name] = render(inputs[spec["input"]] == spec["equals"])
            elif "field" in spec:
                # The largest value of the field in the run's records of that evidence (the quorum an ACK line
                # logs), at_least when no record carries it.
                numbers = [
                    spec.get("at_least", 0),
                    *self._field_values(spec["field"]["evidence"], spec["field"]["name"]),
                ]
                values[name] = str(max(numbers))
            else:
                numbers = [spec.get("at_least", 0)]
                for state in self.states:
                    for var in spec["max_of"]:
                        value = state[var]
                        numbers.extend(value.values() if isinstance(value, dict) else [value])
                values[name] = str(max(item for item in numbers if isinstance(item, int)))
        return values

    def _field_values(self, evidence_name: str, field_name: str) -> list[int]:
        """The integer values of field ``field_name`` in the run's records of evidence ``evidence_name``."""

        evidence = self.mapping.evidence[evidence_name]
        found = []
        for record in self.run.records:
            if not evidence.matches(record):
                continue
            try:
                value = self._fields(evidence, record).get(field_name)
            except _SkipError:
                continue
            if isinstance(value, int) and not isinstance(value, bool):
                found.append(value)
        return found


# ---------------------------------------------------------------------------
# Checking observed states with TLC
# ---------------------------------------------------------------------------


def trace_module(
    bundle: Bundle,
    observed: ObservedRun,
    constants: Mapping[str, str],
    names: Sequence[str],
    targets: Sequence[str],
    max_unobserved: Optional[int] = None,
) -> tuple[str, str]:
    """``ObservedTrace.tla`` and its config: every observed state, PrintT of where each check fails or holds, and,
    with ``max_unobserved`` (a count of model steps), the transition check (:func:`_transition_section`)."""

    mapping = bundle.mapping
    variables = list(mapping.state)
    states = []
    for state in observed.states:
        fields = [
            f"{var} |-> {mapping.state[var].hidden if mapping.state[var].hidden is not None else render(state[var])}"
            for var in variables
        ]
        states.append("[" + ", ".join(fields) + "]")
    model_values = sorted({str(item) for process in mapping.processes for item in observed.identities(process)})
    substitutions = ",\n    ".join(f"{var} <- Trace[j].{var}" for var in variables)
    prints = [
        f'ASSUME PrintT(<<"{_TAG}", "violated", "{name}", {{j \\in DOMAIN Trace : ~S(j)!{name}}}>>)' for name in names
    ] + [f'ASSUME PrintT(<<"{_TAG}", "reached", "{name}", {{j \\in DOMAIN Trace : S(j)!{name}}}>>)' for name in targets]
    declared = list(constants) + model_values
    if max_unobserved is None:
        declared_variables = "VARIABLE checked"
        behavior = ["Init == checked = TRUE", "Next == UNCHANGED checked"]
        checks = ["INIT Init", "NEXT Next"]
    else:
        declared_variables = f"VARIABLES i, k, {', '.join(variables)}"
        behavior = _transition_section(bundle, observed, variables, max_unobserved)
        checks = ["INIT Init", "NEXT Next", "INVARIANT Incomplete", "CHECK_DEADLOCK FALSE"]
    module = "\n".join(
        [
            "---- MODULE ObservedTrace ----",
            f"\\* States observed in {observed.run.source}, rebuilt through {os.path.basename(mapping.path)}.",
            "EXTENDS Naturals, Sequences, TLC",
            f"CONSTANTS {', '.join(declared)}" if declared else "",
            declared_variables,
            "Trace == <<",
            "    " + ",\n    ".join(states),
            ">>",
            "S(j) == INSTANCE model WITH",
            "    " + substitutions,
            *prints,
            *behavior,
            "====",
            "",
        ]
    )
    cfg = "\n".join(
        ["CONSTANTS"]
        + [f"    {name} = {value}" for name, value in constants.items()]
        + [f"    {name} = {name}" for name in model_values]
        + [*checks, ""]
    )
    return module, cfg


def _path(path: tuple) -> str:
    return "<<" + ", ".join([f'"{path[0]}"', *map(render, path[1:])]) + ">>"


def split_next(model_text: str, keep: AbstractSet[str], drop: AbstractSet[str]) -> str:
    """The body of the model's ``Next`` with the actions in ``keep`` taken from the instance ``M`` and the
    actions in ``drop`` replaced by FALSE: the model's steps restricted to ``keep``. ``Next`` must apply its
    actions directly (``\\E p \\in Primaries : Vote(p) \\/ Crash(p)``), as every shipped model does; an operator
    it names that takes a step itself (one that primes a variable) is a :class:`TlaError`. Other operators
    (``\\E n \\in Nodes`` over a defined set) are taken from ``M`` too."""

    body = definition_body(model_text, "Next")
    if body is None:
        raise TlaError(f"{MODEL_FILE} defines no Next")
    actions = {name: f"M!{name}" for name in keep} | dict.fromkeys(drop, "FALSE")
    defined = definitions(model_text)

    def replace(match: re.Match) -> str:
        name, arguments = match.group(1), match.group(2) or ""
        if name in actions:
            return "FALSE" if actions[name] == "FALSE" else actions[name] + arguments
        if name in defined:
            body = defined[name]
            if "'" in body or "UNCHANGED" in body or any(re.search(rf"\b{action}\b", body) for action in actions):
                raise TlaError(
                    f"Next of {MODEL_FILE} takes steps through {name}, which the mapping neither maps nor calls "
                    "unobservable; the transition check needs Next to apply the model's actions directly"
                )
            return f"M!{name}{arguments}"
        return match.group(0)

    restricted = re.sub(r"\b([A-Za-z_]\w*)(\([^()]*\))?", replace, body)
    # Drop the FALSE disjuncts and the lines left with nothing else; an \E over no kept action stays.
    restricted = re.sub(r"FALSE \\/ ", "", restricted)
    restricted = re.sub(r"(?<=\S) \\/ FALSE\b", "", restricted)
    lines = [
        line
        for line in restricted.splitlines()
        if line.strip() and not re.fullmatch(r"\\/ (\\E [^:]+ : )?FALSE", line.strip())
    ]
    if not any("M!" in line for line in lines):
        return " FALSE"
    # A \/ list keeps its own lines, so its bullets stay aligned.
    return ("\n" if lines[0].startswith(" ") else "") + "\n".join(lines)


def _transition_section(
    bundle: Bundle, observed: ObservedRun, variables: Sequence[str], max_unobserved: int
) -> list[str]:
    """The lines that check the observed transitions: TLC follows the model from the first observed state and,
    for each record that shows a model action, takes that action (or none, when earlier steps already account
    for it) to a state that agrees with what the record says; a sample only raises what it reports. Up to
    ``max_unobserved`` steps of the actions the mapping calls unobservable may come between two records. Each
    furthest record a path reaches is printed; a path that reaches the last one violates ``Incomplete``, so TLC
    stops there."""

    reports = []
    for report in observed.reports:
        fields = [f'kind |-> "{report["kind"]}"', f"at |-> {report['state']}", f'action |-> "{report["action"] or ""}"']
        for name in ("eq", "lo"):
            fields.append(f"{name} |-> {{{', '.join(_path(path) for path in report[name])}}}")
        for name in ("has", "lacks"):
            pairs = ", ".join(f"<<{_path(path)}, {render(value)}>>" for path, value in report[name])
            fields.append(f"{name} |-> {{{pairs}}}")
        for name in ("changes", "unchanged"):
            fields.append(f"{name} |-> {{{', '.join(render(var) for var in report[name])}}}")
        fields.append(f"repeats |-> {render(report['repeats'])}")
        reports.append("[" + ", ".join(fields) + "]")
    mapping = bundle.mapping
    observable, unobservable = set(mapping.actions()), set(mapping.unobservable)
    taken = sorted({report["action"] for report in observed.reports if report["action"]})
    actions = [
        f"Do_{name} ==" + split_next(bundle.model_text, {name}, (observable | unobservable) - {name}) for name in taken
    ]
    cases = "\n                  [] ".join(f'name = "{name}" -> Do_{name}' for name in taken)
    return [
        "\\* Each record that is a mapped action or changes the state: the observed state after it (at), its action,",
        "\\* what it says of that state as paths into it (<<var>> or <<var, key>>: eq holds the observed value, has and",
        "\\* lacks pair a path with a value its set gained or lost, lo is a lower bound a sample raises to), the",
        "\\* variables the mapping says the action changes or leaves unchanged, and whether the record stands for",
        "\\* several steps of its action (repeats).",
        "Steps == <<",
        "    " + ",\n    ".join(reports),
        ">>",
        f"MaxUnobserved == {max_unobserved}",
        "M == INSTANCE model",
        "\\* The model's actions as its Next applies them, each alone, and the ones no record shows.",
        *actions,
        f"Action(name) == CASE {cases}" if cases else "Action(name) == FALSE",
        "Unobservable ==" + split_next(bundle.model_text, unobservable, observable),
        f"Vars == <<{', '.join(variables)}>>",
        "State == [" + ", ".join(f"{var} |-> {var}" for var in variables) + "]",
        "At(s, path) == IF Len(path) = 1 THEN s[path[1]] ELSE s[path[1]][path[2]]",
        "Max(a, b) == IF a > b THEN a ELSE b",
        "Matches(j, s) == /\\ \\A p \\in Steps[j].eq : At(s, p) = At(Trace[Steps[j].at], p)",
        "                 /\\ \\A pv \\in Steps[j].has : pv[2] \\in At(s, pv[1])",
        "                 /\\ \\A pv \\in Steps[j].lacks : pv[2] \\notin At(s, pv[1])",
        "\\* Variable `name`, now `old`, raised to what sample j reports of it.",
        "Raised(j, name, old) ==",
        "    LET paths == {p \\in Steps[j].eq \\cup Steps[j].lo : p[1] = name}",
        "        Seen(p) == At(Trace[Steps[j].at], p)",
        "        New(p, prior) == IF p \\in Steps[j].lo THEN Max(prior, Seen(p)) ELSE Seen(p)",
        "    IN IF paths = {} THEN old",
        "       ELSE IF <<name>> \\in paths THEN New(<<name>>, old)",
        "       ELSE [x \\in DOMAIN old |-> IF <<name, x>> \\in paths THEN New(<<name, x>>, old[x]) ELSE old[x]]",
        "\\* Each furthest record a path reaches, once per TLC worker.",
        f'Reached(j) == IF j > TLCGet(1) THEN TLCSet(1, j) /\\ PrintT(<<"{_TAG}", "matched", j>>) ELSE TRUE',
        "ASSUME TLCSet(1, 1)",
        "Init == /\\ i = 1 /\\ k = 0",
        *[f"        /\\ {var} = Trace[1].{var}" for var in variables],
        'Step == /\\ i < Len(Steps) /\\ Steps[i + 1].kind = "step"',
        "        /\\ i' = i + 1 /\\ k' = 0",
        "        /\\ Action(Steps[i'].action) \\/ UNCHANGED Vars",
        "        /\\ \\A v \\in Steps[i'].changes : State'[v] # State[v]",
        "        /\\ \\A v \\in Steps[i'].unchanged : State'[v] = State[v]",
        "        /\\ Matches(i', State') /\\ Reached(i')",
        'Sample == /\\ i < Len(Steps) /\\ Steps[i + 1].kind = "sample"',
        "          /\\ i' = i + 1 /\\ UNCHANGED k",
        *[f"          /\\ {var}' = Raised(i', \"{var}\", {var})" for var in variables],
        "          /\\ Reached(i')",
        "Unobserved == k < MaxUnobserved /\\ k' = k + 1 /\\ UNCHANGED i /\\ Unobservable",
        "\\* A record that stands for several steps of its action: the action, before the record.",
        "Repeated == i < Len(Steps) /\\ Steps[i + 1].repeats /\\ UNCHANGED <<i, k>> /\\ Action(Steps[i + 1].action)",
        "Next == Step \\/ Sample \\/ Unobserved \\/ Repeated",
        "\\* Violated, so TLC stops, once a path explains every record.",
        "Incomplete == i < Len(Steps)",
    ]


def needs_tlc(bundle: Bundle) -> bool:
    """Whether checking a run's states needs TLC: the bundle has a property to check on observed states, or a
    target. The transition check always needs TLC; without it, it is reported as not checked."""

    return any(item.conform for item in bundle.claims.properties) or bool(bundle.claims.targets)


TRANSITIONS_UNCHECKED = "not checked: the transition check needs TLC (java and tla2tools.jar)"


def check_run(
    bundle: Bundle, run: RunEvidence, tools: Optional[TlaTools], *, transitions: bool = True
) -> dict[str, Any]:
    """Rebuild ``run``'s observed states and check the claims on them, and (``transitions``) that the model's
    Next relates consecutive observed states. ``tools`` may be None only when the bundle does not
    :func:`needs_tlc`; the transition check is then reported as not checked."""

    claims = bundle.claims
    names = [item.name for item in claims.properties if item.conform]
    targets = [item.name for item in claims.targets]
    result: dict[str, Any] = {
        "source": run.source,
        **{key: run.meta.get(key) for key in ("scenario_id", "seed", "inputs", "run_status", "report")},
        "excluded": {item.name: item.conform_note for item in claims.properties if not item.conform},
    }
    if run.problem is not None:
        return {**result, "states": 0, "status": "error", "error": run.problem}
    recorded = (run.header.get("spec") or {}).get("mapping_sha256")
    observed = ObservedRun(bundle.mapping, run)
    observed.replay()
    if recorded and recorded != bundle.mapping.sha256:
        observed.notes.append("the run recorded a different mapping.yaml (sha256 differs): check for mapping drift")
    if observed.sample_mismatches:
        observed.notes.append(
            f"{len(observed.sample_mismatches)} sample(s) disagree with the state the events gave (sample_mismatches)"
        )
    result.update(
        {
            "processes": {
                name: dict(zip(observed.members[name], map(str, observed.identities(name))))
                for name in bundle.mapping.processes
            },
            "states": len(observed.states),
            "actions": dict(sorted(observed.actions.items())),
            "sample_mismatches": observed.sample_mismatches,
            "notes": observed.notes,
        }
    )
    if observed.unapplied:
        result["status"] = "error"
        shown = "; ".join(observed.unapplied[:5]) + ("; ..." if len(observed.unapplied) > 5 else "")
        result["error"] = (
            f"{len(observed.unapplied)} mapped record(s) could not be applied, so the observed states may miss "
            f"what they show: {shown}"
        )
        return result
    if len(observed.states) < 2 or not observed.actions:
        result["status"] = "error"
        result["error"] = "no mapped evidence in this run: nothing to check"
        result["unexercised"] = True
        return result
    printed: dict[tuple[str, str], list[int]] = {}
    # The records (observed.reports) a model path explains: the first, the initial state, always.
    furthest = 1
    checking = transitions and tools is not None and claims.transitions_excluded is None
    try:
        result["constants"] = observed.constants()
        # A bundle whose properties are all excluded and that has no targets gives TLC nothing to evaluate.
        if needs_tlc(bundle) or checking:
            max_unobserved = claims.max_unobserved_steps if checking else None
            module, cfg = trace_module(bundle, observed, result["constants"], names, targets, max_unobserved)
            found, run_tlc = tlc(
                tools,
                "ObservedTrace.tla",
                "ObservedTrace.cfg",
                {MODEL_FILE: bundle.model_text, "ObservedTrace.tla": module, "ObservedTrace.cfg": cfg},
            )
            result["tlc_seconds"] = run_tlc.seconds
            if run_tlc.timed_out:
                result["timed_out"] = True
                raise TlaError(run_tlc.timeout_error)
            if found.errors:
                result["tlc_output"] = run_tlc.tail()
                raise TlaError(f"TLC failed on the observed states: {found.errors[0]}")
            for item in print_values(run_tlc.output, _TAG):
                if item[1] == "matched":
                    furthest = max(furthest, item[2])
                else:
                    printed[(item[1], item[2])] = sorted(item[3])
    except TlaError as exc:
        result["status"] = "error"
        result["error"] = str(exc)
        return result
    missing = [name for name in names if ("violated", name) not in printed] + [
        name for name in targets if ("reached", name) not in printed
    ]
    if missing:
        result["status"] = "error"
        result["error"] = f"TLC did not evaluate {missing}"
        result["tlc_output"] = run_tlc.tail()
        return result
    properties = {}
    for name in names:
        failing = printed[("violated", name)]
        claim = bundle.property(name)
        claimed = claim.holds_under(result["constants"])
        entry: dict[str, Any] = {"status": "violated" if failing else "holds", "claimed": claimed}
        if not claimed:
            entry["outside_claim"] = {key: result["constants"].get(key) for key in claim.holds_when}
        if failing:
            entry["states"] = failing
            entry["first"] = observed.steps[failing[0] - 1]
            entry["previous"] = observed.steps[max(failing[0] - 2, 0)]
        properties[name] = entry
    result["properties"] = properties
    result["targets"] = {}
    for name in targets:
        reached = printed[("reached", name)]
        result["targets"][name] = {"reached": bool(reached), "states": reached}
        if reached:
            result["targets"][name]["first"] = observed.steps[reached[0] - 1]
    if checking:
        # reports[:furthest] have a model explanation; short of the last, no step of the model (after up to
        # max_unobserved_steps unobservable ones) takes the model from record furthest to record furthest + 1.
        holds = furthest >= len(observed.reports)
        result["transitions"] = {
            "checked": len(observed.reports) - 1,
            "max_unobserved_steps": claims.max_unobserved_steps,
            "holds": holds,
            "tlc_states": found.distinct_states,
        }
        if not holds:
            shown = ("state", "why", "evidence", "t", "fields")
            result["transitions"]["first_violation"] = {
                "index": furthest + 1,
                "from": {key: observed.reports[furthest - 1].get(key) for key in shown},
                "to": {key: observed.reports[furthest].get(key) for key in shown},
                "event": observed.reports[furthest]["evidence"],
            }
    elif transitions and claims.transitions_excluded is not None:
        result["transitions"] = {"checked": 0, "holds": None, "excluded": claims.transitions_excluded}
    elif transitions:
        result["transitions"] = {"checked": 0, "holds": None, "skipped": TRANSITIONS_UNCHECKED}
    result["violated"] = violated_claims(result)
    result["first_violated"] = first_violated(result)
    result["status"] = "fail" if result["violated"] else "pass"
    return result


def violated_claims(result: Mapping[str, Any]) -> list[str]:
    """What a :func:`check_run` result holds against the run: each claimed property the observed states violated,
    and ``transitions`` when the transition check found a step the model cannot take. Empty for a pass."""

    violated = sorted(
        name
        for name, entry in (result.get("properties") or {}).items()
        if entry["status"] == "violated" and entry["claimed"]
    )
    if (result.get("transitions") or {}).get("holds") is False:
        violated.append("transitions")
    return violated


def first_violated(result: Mapping[str, Any]) -> Optional[dict[str, Any]]:
    """The first of :func:`violated_claims`, with the observed ``state`` (its index in the trace) that violates it
    (for ``transitions``, the first observed step no model step reaches) and that ``step`` as ``steps`` lists it;
    None for a pass."""

    violated = violated_claims(result)
    if not violated:
        return None
    claim = violated[0]
    if claim == "transitions":
        step = result["transitions"]["first_violation"]["to"]
    else:
        step = result["properties"][claim]["first"]
    return {"claim": claim, "state": step["state"], "step": step}


def conform_runs(
    bundle: Bundle,
    runs: Sequence[RunEvidence],
    tools: Optional[TlaTools],
    *,
    group_by: Sequence[str] = (),
    transitions: bool = True,
) -> dict[str, Any]:
    """Check every run and summarize: per-run results, live action coverage, targets reached, and groups."""

    results = [check_run(bundle, run, tools, transitions=transitions) for run in runs]
    return summarize(bundle, results, group_by=group_by, transitions=transitions)


def summarize(
    bundle: Bundle, results: Sequence[Mapping[str, Any]], *, group_by: Sequence[str] = (), transitions: bool = True
) -> dict[str, Any]:
    """The conformance report of :func:`check_run` results: live action coverage, targets reached, and groups."""

    observed_in: Counter = Counter()
    for result in results:
        for action in result.get("actions", {}):
            observed_in[action] += 1
    mapped = sorted(bundle.mapping.actions())
    targets = {
        item.name: sum(bool(result.get("targets", {}).get(item.name, {}).get("reached")) for result in results)
        for item in bundle.claims.targets
    }
    statuses = [result["status"] for result in results]
    checked = transitions and bundle.claims.transitions_excluded is None
    report: dict[str, Any] = {
        "kind": "spec-conformance",
        "bundle": bundle.protocol,
        "mapping_sha256": bundle.mapping.sha256,
        "checks": CHECKS_WITH_TRANSITIONS if checked else CHECKS,
        "refinement": REFINEMENT_WITH_TRANSITIONS if checked else REFINEMENT,
        "properties_checked": [item.name for item in bundle.claims.properties if item.conform],
        "runs": list(results),
        "action_coverage": {
            "runs": len(results),
            "observed": {action: observed_in.get(action, 0) for action in mapped},
            "never_observed": [action for action in mapped if not observed_in.get(action)],
            "unobservable": bundle.mapping.unobservable,
        },
        "targets_reached": targets,
        "status": "fail" if "fail" in statuses else ("error" if "error" in statuses or not results else "pass"),
    }
    if transitions:
        report["transitions"] = {
            "max_unobserved_steps": bundle.claims.max_unobserved_steps,
            "violated_in": sum(1 for result in results if (result.get("transitions") or {}).get("holds") is False),
            **({"excluded": bundle.claims.transitions_excluded} if bundle.claims.transitions_excluded else {}),
        }
    if group_by:
        report["groups"] = _groups(results, group_by, [item.name for item in bundle.claims.targets])
    return report


def plan_bundles(plan: Mapping[str, Any]) -> list[str]:
    """The spec bundles the components of a plan (as data) name in their ``spec``: the protocols its run exercises."""

    names: list[str] = []
    for component in plan.get("components") or ():
        name = (component.get("config") or {}).get("spec")
        if isinstance(name, str) and name and name not in names:
            names.append(name)
    return names


def conform_folder(
    folder: str, tools: Optional[TlaTools], unavailable: str = "", *, transitions: bool = True
) -> dict[str, dict[str, Any]]:
    """The run in ``folder`` checked against each spec bundle its plan names, by bundle; empty when it names none.

    Each entry is :func:`check_run`'s result and whether the bundle's claims are ``approved``. A run that did
    not reach its verdict (it ended ERROR or SKIP_UNSUPPORTED) is ``not_run``, and one that took no mapped step
    is ``unexercised``: its plan names the bundle, but nothing it did shows the protocol (a replication chaos
    wave that only stalls a replica briefly). Without ``tools``, a bundle that :func:`needs_tlc` cannot be
    checked, with ``unavailable`` as the reason: a draft is ``skipped``, and an approved bundle is an ``error``,
    since its claims decide the run. An approved bundle whose transition check (``transitions``, unless its
    claims exclude it) cannot run without ``tools`` is an ``error`` too; a draft's is reported as not checked.
    """

    path = os.path.join(folder, "report.json")
    report = _run_report(path)
    if report is None:
        return {}
    results: dict[str, dict[str, Any]] = {}
    for name in plan_bundles(report["outcome"]["plan"]):
        try:
            bundle = load_bundle(name)
        except SpecBundleError as exc:
            # Nothing says the claims are a draft, so the batch holds the error against the run.
            results[name] = {"status": "error", "error": str(exc), "approved": True}
            continue
        entry: dict[str, Any] = {"approved": bundle.claims.status == "approved"}
        checks_transitions = transitions and bundle.claims.transitions_excluded is None
        if report.get("status") not in CHECKED_RUN_STATUSES:
            entry.update(status="not_run", reason=f"the run ended {report.get('status')}, before its verdict")
        elif tools is None and entry["approved"] and (needs_tlc(bundle) or checks_transitions):
            entry.update(status="error", error=unavailable or "no java and tla2tools.jar")
        elif tools is None and needs_tlc(bundle):
            entry.update(status="skipped", reason=unavailable or "no java and tla2tools.jar")
        else:
            entry.update(check_run(bundle, _report_run(path, report), tools, transitions=transitions))
            if entry.pop("unexercised", False):
                entry.update(status="unexercised", reason=entry.pop("error"))
        results[name] = entry
    return results


def bundle_approved(name: str) -> bool:
    """Whether the claims of spec bundle ``name`` are approved. A bundle that does not load counts as approved:
    the plan named it, and nothing says its claims are a draft, so the run is not passed over its check."""

    try:
        return load_bundle(name).claims.status == "approved"
    except SpecBundleError:
        return True


def conform_run(folder: str, timeout_s: float) -> dict[str, dict[str, Any]]:
    """The run in ``folder`` (its ``report.json`` written) checked against every spec bundle its plan names
    (:func:`conform_folder`), with TLC from ``$TLA2TOOLS_JAR`` on one worker for at most ``timeout_s``; a check
    it stops is an error that names the option. Without TLC, a bundle that needs it is skipped, or an error if
    its claims are approved. A check that fails outright still gives an entry per bundle, with why."""

    try:
        try:
            tools, unavailable = TlaTools.locate(None, workers="1").within(timeout_s), ""
        except TlaError as exc:
            tools, unavailable = None, str(exc)
        results = conform_folder(folder, tools, unavailable)
    except Exception as exc:
        # The run's report would otherwise say nothing of the bundle, not why its check did not finish.
        error = error_record(exc, "conform")
        problem = f"spec conform did not finish: {error['type']}: {error.get('message', '')}"
        report = _run_report(os.path.join(folder, "report.json")) or {}
        plan = (report.get("outcome") or {}).get("plan") or {}
        results = {
            name: {"status": "error", "error": problem, "approved": bundle_approved(name)}
            for name in plan_bundles(plan)
        }
    for entry in results.values():
        if entry.get("timed_out"):
            entry["error"] = f"{entry['error']} ({CONFORM_TIMEOUT_OPTION} = {timeout_s:g}, a batch option)"
    return results


def conform_component(bundle: str) -> str:
    """The ID of the component a run's verdict names for what spec conform held against it over ``bundle``."""

    return f"spec-conform:{bundle}"


def judge_run(outcome: RunOutcome, results: Mapping[str, Mapping[str, Any]]) -> RunOutcome:
    """``outcome`` with what the conform ``results`` (by bundle, as :func:`conform_run` gives them) hold against
    the run. For an approved bundle whose check found a claimed property violated or a step the model cannot
    take, the run FAILs through a failed component :func:`conform_component`, whose summary names each violated
    claim; for one whose check could not decide, that component is an ERROR, and so is the run unless it FAILed.
    A draft's findings stay in the results, never in the verdict; an entry that does not say its claims are
    approved is read as a draft's."""

    added = []
    status, summary = outcome.status, outcome.summary
    for name, entry in sorted(results.items()):
        if entry["status"] not in ("fail", "error") or not entry.get("approved", False):
            continue
        if entry["status"] == "fail":
            violated = violated_claims(entry)
            text = "; ".join(f"{claim} violated" for claim in violated)
            diagnostics = {"bundle": name, "violated": violated, "first_violated": entry.get("first_violated")}
            status = OutcomeStatus.FAIL
        else:
            text = entry["error"]
            diagnostics = {"bundle": name}
            if status is not OutcomeStatus.FAIL:
                status = OutcomeStatus.ERROR
        component = ComponentResult(
            conform_component(name), ComponentStatus(entry["status"].upper()), text, diagnostics=jsonable(diagnostics)
        )
        added.append(component)
        summary = f"spec conform {name}: {text}; {summary}"
    if not added:
        return outcome
    return replace(outcome, status=status, summary=summary, component_results=outcome.component_results + tuple(added))


def describe(name: str, entry: Mapping[str, Any]) -> str:
    """One line for bundle ``name``'s conform entry: whether its claims are a draft, its status, and the violated
    claims or why it was not checked."""

    claims = "" if entry.get("approved") else " (draft claims)"
    if entry["status"] == "fail":
        detail = f": {', '.join(violated_claims(entry))} violated"
    elif entry["status"] == "pass":
        detail = ""
    else:
        detail = f": {entry.get('error') or entry.get('reason')}"
    return f"{name}{claims}: {entry['status']}{detail}"


def _groups(results: Sequence[Mapping[str, Any]], keys: Sequence[str], targets: Sequence[str]) -> list[dict[str, Any]]:
    groups: dict[tuple, list[Mapping[str, Any]]] = {}
    for result in results:
        inputs = result.get("inputs") or {}
        groups.setdefault(tuple(inputs.get(key) for key in keys), []).append(result)
    rows = []
    for group, members in sorted(groups.items(), key=lambda item: tuple(map(str, item[0]))):
        rows.append(
            {
                **dict(zip(keys, group)),
                "runs": len(members),
                "run_passed": sum(result.get("run_status") == "PASS" for result in members),
                "conform_violations": sum(result["status"] == "fail" for result in members),
                "conform_errors": sum(result["status"] == "error" for result in members),
                **{
                    f"reached {name}": sum(
                        bool(result.get("targets", {}).get(name, {}).get("reached")) for result in members
                    )
                    for name in targets
                },
            }
        )
    return rows


__all__ = [
    "CHECKED_RUN_STATUSES",
    "CHECKS",
    "CHECKS_WITH_TRANSITIONS",
    "CONFORM_TIMEOUT_OPTION",
    "REFINEMENT",
    "REFINEMENT_WITH_TRANSITIONS",
    "TRANSITIONS_UNCHECKED",
    "ObservedRun",
    "RunEvidence",
    "bundle_approved",
    "check_run",
    "collect_runs",
    "conform_component",
    "conform_folder",
    "conform_run",
    "conform_runs",
    "describe",
    "first_violated",
    "judge_run",
    "needs_tlc",
    "plan_bundles",
    "split_next",
    "summarize",
    "trace_module",
    "violated_claims",
]
