"""Observers that collect path evidence: INFO sampling and log scanning.

Observers emit evidence only when a declared expectation is met. An
expectation marked ``required: true`` becomes one of the plan's required
evidence (the compiler derives it), so a missing observation fails the run;
unmarked expectations are diagnostic. An expectation the observer could not
judge (every INFO sample it needed errored, a log could not be read) is
reported ``unobserved`` instead, which makes the run ERROR: the server was
never seen to miss it. Raw samples and log excerpts are always saved as
artifacts.
"""

import os
import re
import sys
import threading
import time
from typing import Any, Optional

from valkey_fuzzer.extensions import (
    ComponentSpec,
    ComponentStatus,
    ConfigError,
    ConfigField,
    ConfigSchema,
    Declarations,
    ExecutionPlan,
    component_artifact_dir,
    make_evidence,
    make_result,
    plain_config,
)

from .common import (
    ANY,
    INTEGER,
    MAPPING,
    NUMBER,
    PREDICATE_FIELDS,
    SPEC_SHA256_FIELD,
    TEXT,
    StoppableThread,
    close_quietly,
    compare,
    component_registration,
    declared,
    deployment,
    log_line_time,
    mapping_of,
    optional,
    pinned_spec_mapping,
    select_instances,
    validate_predicate,
    write_jsonl,
)

# ---------------------------------------------------------------------------
# valkey.observer.info
# ---------------------------------------------------------------------------

# Which instances an expectation or log rule must be met on: any one, or every one observed.
_AGGREGATE = ConfigField(str, default="any", choices=("any", "all"))
# One expectation: a value predicate (``equals``, ``min``, ...) or a delta between the first and last samples.
_EXPECTATION = mapping_of(
    {
        "evidence_id": TEXT,
        "field": TEXT,
        "when": ConfigField(str, default="any", choices=("any", "baseline", "final", "during")),
        "instances": _AGGREGATE,
        "delta_min": optional(float),
        "delta_max": optional(float),
        "required": ConfigField(bool, default=False),
        "precondition": optional(str),
        **PREDICATE_FIELDS,
    }
)
_INFO_FIELDS = {
    "instances": ConfigField((str, list), default="primaries"),
    "sections": ConfigField(list, default=lambda: ["persistence"], items=TEXT),
    "interval_s": ConfigField(float, default=0.05, minimum=0, maximum=3600),
    "on_events": ConfigField(list, default=list, items=TEXT),
    "expectations": ConfigField(list, default=list, items=_EXPECTATION),
}

_EXPECTATION_KEYS = {"evidence_id", "field", "when", "instances", "delta_min", "delta_max", "required", "precondition"}


def field_values(fields: dict[str, Any], path: str) -> list[Any]:
    """The values ``path`` names in one INFO sample.

    A plain field name reads that field. A dotted path reads into the
    ``key=value,...`` mappings INFO reports, such as ``slave0.state``; a ``*``
    in its first segment stands for an index, so ``slave*.repl_compression``
    reads that entry of every replica line. A missing plain field reads as
    ``None``; missing entries of a dotted path are skipped.
    """

    if path in fields or "." not in path:
        return [fields.get(path)]
    head, _, rest = path.partition(".")
    if "*" in head:
        pattern = re.compile("^" + re.escape(head).replace(r"\*", r"\d+") + "$")
        values = [fields[key] for key in sorted(fields) if pattern.match(key)]
    else:
        values = [fields[head]] if head in fields else []
    for segment in rest.split(".") if rest else ():
        values = [value[segment] for value in values if isinstance(value, dict) and segment in value]
    return values


def _namespaced(evidence_id: str, where: str) -> None:
    if "." not in evidence_id:
        raise ConfigError(f"{where}.evidence_id must be namespaced, such as tests.saved; got {evidence_id!r}")


def _regex(pattern: str, where: str) -> Any:
    try:
        return re.compile(pattern)
    except re.error as exc:
        raise ConfigError(f"{where} is not a valid regex: {exc}") from exc


def _validate_info(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    if not config["sections"]:
        raise ConfigError("sections must name at least one INFO section")
    seen = set()
    for index, expectation in enumerate(config["expectations"]):
        where = f"expectations[{index}]"
        _namespaced(expectation["evidence_id"], where)
        if expectation["evidence_id"] in seen:
            raise ConfigError(f"{where}.evidence_id duplicates another expectation")
        seen.add(expectation["evidence_id"])
        is_delta = "delta_min" in expectation or "delta_max" in expectation
        predicate = {key: value for key, value in expectation.items() if key not in _EXPECTATION_KEYS}
        if is_delta:
            if predicate:
                raise ConfigError(f"{where} cannot combine a delta with a value predicate")
        else:
            validate_predicate(predicate, where)


class InfoObserver:
    """Sample INFO at start, periodically, on lifecycle events, and at stop."""

    def __init__(self) -> None:
        self._samples: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._thread: Optional[StoppableThread] = None
        self._subscriptions: list[Any] = []
        self._instances: list[str] = []
        self._config: dict[str, Any] = {}
        self._handle: Any = None
        # The periodic sampler owns separate clients so it never shares a
        # socket with the event-driven sampler.
        self._clients: dict[str, Any] = {}
        self._periodic_clients: dict[str, Any] = {}

    def _sample(self, phase: str, event: Optional[Any] = None, clients: Optional[dict[str, Any]] = None) -> None:
        clients = self._clients if clients is None else clients
        for instance_id in self._instances:
            record: dict[str, Any] = {"phase": phase, "instance_id": instance_id, "at": time.time()}
            if event is not None:
                record["event"] = event.name
                record["event_sequence"] = event.sequence
            fields: dict[str, Any] = {}
            try:
                # Counters restart with the process, so a delta across a restart measures nothing.
                record["pid"] = self._handle.pid(instance_id)
                client = clients.get(instance_id)
                if client is None:
                    client = clients[instance_id] = self._handle.new_client(instance_id, timeout=2.0)
                for section in self._config["sections"]:
                    fields.update(client.info(section))
            except Exception as exc:
                record["error"] = f"{type(exc).__name__}: {exc}"
                close_quietly(clients.pop(instance_id, None))
            record["fields"] = {key: value for key, value in fields.items() if isinstance(key, str)}
            with self._lock:
                record["index"] = len(self._samples)
                self._samples.append(record)

    def start(self, context: Any, spec: ComponentSpec):
        self._config = spec.config
        self._handle = deployment(context)
        self._instances = select_instances(self._handle, self._config["instances"])
        self._sample("baseline")
        for event_name in self._config["on_events"]:
            self._subscriptions.append(
                context.event_bus.subscribe(event_name, lambda event: self._sample("event", event))
            )
        if self._config["interval_s"] > 0:

            def loop(stop_event) -> None:
                while not stop_event.wait(self._config["interval_s"]):
                    self._sample("periodic", clients=self._periodic_clients)

            self._thread = StoppableThread(f"info-{spec.id}", loop)
            self._thread.start()
        return make_result(spec, ComponentStatus.PASS, f"sampling INFO on {', '.join(self._instances)}")

    def stop(self, context: Any, spec: ComponentSpec):
        for subscription in self._subscriptions:
            context.event_bus.unsubscribe(subscription)
        if self._thread is not None:
            self._thread.stop()
            close_quietly(*self._periodic_clients.values())
        self._sample("final")
        close_quietly(*self._clients.values())
        path = os.path.join(component_artifact_dir(context, spec), "info-samples.jsonl")
        with self._lock:
            samples = sorted(self._samples, key=lambda item: item["at"])
        write_jsonl(path, samples)
        evidence = []
        unmet = []
        unobserved: dict[str, dict[str, str]] = {}
        for expectation in self._config["expectations"]:
            matched, blind = self._evaluate(expectation, samples)
            if matched is not None:
                evidence.append(
                    make_evidence(spec, expectation["evidence_id"], "observer.stopped", matched, artifacts=(path,))
                )
            elif blind:
                unobserved[expectation["evidence_id"]] = blind
            else:
                unmet.append(expectation["evidence_id"])
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{len(samples)} samples; {len(evidence)} expectation(s) met"
            + (f"; unmet: {', '.join(unmet)}" if unmet else "")
            + (f"; could not observe: {', '.join(unobserved)}" if unobserved else ""),
            evidence=evidence,
            diagnostics={"unmet_expectations": unmet, "unobserved_expectations": unobserved, "samples_path": path},
            unobserved=tuple(unobserved),
            preconditions=_preconditions(self._config["expectations"]),
        )

    def _evaluate(self, expectation: dict, samples: list[dict]) -> tuple[Optional[dict], dict[str, str]]:
        """The evidence value when ``expectation`` is met; otherwise, why each instance it is unmet on could not
        be judged (empty when every one was seen and did not meet it)."""

        field = expectation["field"]
        when = expectation["when"]
        aggregate = expectation["instances"]
        if "delta_min" in expectation or "delta_max" in expectation:
            return self._evaluate_delta(expectation, samples, aggregate)
        predicate = {key: value for key, value in expectation.items() if key not in _EXPECTATION_KEYS}
        if when == "baseline":
            candidates = [sample for sample in samples if sample["phase"] == "baseline"]
        elif when == "final":
            candidates = [sample for sample in samples if sample["phase"] == "final"]
        elif when == "during":
            candidates = [sample for sample in samples if sample["phase"] in ("periodic", "event")]
        else:
            candidates = samples
        matches: dict[str, dict] = {}
        seen: set[str] = set()
        errors: dict[str, str] = {}
        for sample in candidates:
            if "error" in sample:
                errors.setdefault(sample["instance_id"], sample["error"])
                continue
            seen.add(sample["instance_id"])
            if sample["instance_id"] in matches:
                continue
            values = field_values(sample["fields"], field)
            # A wildcard path matches when every entry it names does.
            value = values[0] if len(values) == 1 else values
            if values and all(compare(item, predicate) for item in values):
                matches[sample["instance_id"]] = {
                    "instance_id": sample["instance_id"],
                    "field": field,
                    "value": value,
                    "phase": sample["phase"],
                    "sample_index": sample["index"],
                    "event": sample.get("event"),
                }
        if matches and (aggregate == "any" or set(matches) == set(self._instances)):
            return {
                "field": field,
                "instances": aggregate,
                "predicate": predicate,
                "matches": list(matches.values()),
            }, {}
        # An unmet instance with no successful sample in the window was never seen.
        blind = {
            instance_id: errors.get(instance_id, f"no {when} sample")
            for instance_id in self._instances
            if instance_id not in matches and instance_id not in seen
        }
        return None, self._unjudged(aggregate, matches, blind)

    def _unjudged(self, aggregate: str, met: Any, blind: dict[str, str]) -> dict[str, str]:
        """``blind`` when it leaves an unmet expectation undecided: with ``all``, one instance seen not to meet it
        decides it; with ``any``, an instance never seen might have met it."""

        if aggregate == "all" and set(self._instances) - set(met) - set(blind):
            return {}
        return blind

    def _evaluate_delta(
        self, expectation: dict, samples: list[dict], aggregate: str
    ) -> tuple[Optional[dict], dict[str, str]]:
        """A delta from each instance's baseline sample to its final sample.

        Counters restart with the process and never decrease within one, so a
        restart between the two samples, or a decrease, leaves the delta
        unmeasured: a counter that reset could hide a full resync from
        ``delta_max`` or invent a shortfall for ``delta_min``.
        """

        field = expectation["field"]
        deltas = {}
        blind: dict[str, str] = {}
        for instance_id in self._instances:
            ends = [
                next(
                    (sample for sample in samples if sample["instance_id"] == instance_id and sample["phase"] == phase),
                    None,
                )
                for phase in ("baseline", "final")
            ]
            problem = next(
                (f"{phase} sample: {sample['error'] if sample else 'not taken'}"
                 for phase, sample in zip(("baseline", "final"), ends) if sample is None or "error" in sample),
                None,
            )  # fmt: skip
            if problem is None and ends[0].get("pid") != ends[1].get("pid"):
                problem = f"the process restarted (pid {ends[0].get('pid')} to {ends[1].get('pid')})"
            if problem is not None:
                blind[instance_id] = problem
                continue
            # A wildcard path's value is the sum over the entries it names.
            totals = []
            for sample in ends:
                values = field_values(sample["fields"], field)
                try:
                    totals.append(sum(float(value) for value in values) if values else None)
                except (TypeError, ValueError):
                    totals.append(None)
            first, last = totals
            if first is None or last is None:
                continue
            delta = last - first
            if delta < 0:
                blind[instance_id] = f"{field} decreased from {first:g} to {last:g}, so it was reset"
                continue
            if "delta_min" in expectation and delta < float(expectation["delta_min"]):
                continue
            if "delta_max" in expectation and delta > float(expectation["delta_max"]):
                continue
            deltas[instance_id] = {"baseline": first, "final": last, "delta": delta}
        if deltas and (aggregate == "any" or set(deltas) == set(self._instances)):
            return {"field": field, "instances": aggregate, "deltas": deltas}, {}
        return None, self._unjudged(aggregate, deltas, blind)


# ---------------------------------------------------------------------------
# valkey.observer.log
# ---------------------------------------------------------------------------

# A forbidden pattern matched; ``valkey.oracle.liveness`` fails the run on it.
FORBIDDEN_LOG_MATCH = "valkey.log.forbidden-match"

# How many matching lines an instance must log for a rule to be met.
_MIN_COUNT = ConfigField(int, default=1, minimum=1)
_LOG_FIELDS = {
    "instances": ConfigField((str, list), default="all"),
    "expect": ConfigField(
        list,
        default=list,
        items=mapping_of(
            {
                "evidence_id": TEXT,
                "pattern": TEXT,
                "min_count": _MIN_COUNT,
                "instances": _AGGREGATE,
                "required": ConfigField(bool, default=False),
                "precondition": optional(str),
            }
        ),
    ),
    "forbidden": ConfigField(list, default=list, items=mapping_of({"pattern": TEXT})),
    "max_excerpt_lines": ConfigField(int, default=200, minimum=1, maximum=100_000),
}


def _validate_log(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    for group in ("expect", "forbidden"):
        for index, rule in enumerate(config[group]):
            _regex(rule["pattern"], f"{group}[{index}].pattern")
            if group == "expect":
                _namespaced(rule["evidence_id"], f"{group}[{index}]")


def _preconditions(rules: list[dict]) -> dict[str, str]:
    """Each rule's ``precondition``: evidence the run must have recorded for the rule's absence to be a finding."""

    return {rule["evidence_id"]: rule["precondition"] for rule in rules if rule.get("precondition")}


def read_log_from(path: str, offset: int = 0) -> list[str]:
    return _read_log(path, offset)[0]


def _read_log(path: str, offset: int = 0) -> tuple[list[str], Optional[str]]:
    """The lines of ``path`` from ``offset``, and why it could not be read (None when it could)."""

    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            handle.seek(offset)
            return handle.read().splitlines(), None
    except OSError as exc:
        return [], f"{type(exc).__name__}: {exc}"


def log_size(path: str) -> int:
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


class LogObserver:
    """Scan instance logs written during the observation window.

    A ``forbidden`` match is a finding about the server, not an observer
    failure, so the observer passes and records it as ``valkey.log.forbidden-match``
    (``{pattern: {instance_id: [lines]}}``) for ``valkey.oracle.liveness`` to judge.
    A log that cannot be read leaves the ``expect`` rules it decides
    ``unobserved``, and with ``forbidden`` patterns the observer returns ERROR:
    it cannot show that the server never logged them.
    """

    def __init__(self) -> None:
        self._offsets: dict[str, tuple[str, int]] = {}
        self._config: dict[str, Any] = {}

    def start(self, context: Any, spec: ComponentSpec):
        self._config = spec.config
        handle = deployment(context)
        for instance_id in select_instances(handle, self._config["instances"]):
            path = handle.log_file(instance_id)
            self._offsets[instance_id] = (path, log_size(path))
        return make_result(spec, ComponentStatus.PASS, f"watching {len(self._offsets)} log file(s)")

    def stop(self, context: Any, spec: ComponentSpec):
        lines_by_instance = {}
        unreadable = {}
        for instance_id, (path, offset) in sorted(self._offsets.items()):
            lines_by_instance[instance_id], error = _read_log(path, offset)
            if error is not None:
                unreadable[instance_id] = error
        directory = component_artifact_dir(context, spec)
        excerpt_paths = []
        for instance_id, lines in lines_by_instance.items():
            excerpt = os.path.join(directory, f"{instance_id}.log")
            with open(excerpt, "w", encoding="utf-8") as handle:
                handle.write("\n".join(lines[-self._config["max_excerpt_lines"] :]))
            excerpt_paths.append(excerpt)

        evidence = []
        unmet = []
        unobserved = []
        for rule in self._config["expect"]:
            pattern = re.compile(rule["pattern"])
            per_instance = {
                instance_id: [line for line in lines if pattern.search(line)]
                for instance_id, lines in lines_by_instance.items()
            }
            satisfied = {
                instance_id: matches
                for instance_id, matches in per_instance.items()
                if len(matches) >= rule["min_count"]
            }
            if not satisfied or (rule["instances"] == "all" and set(satisfied) != set(lines_by_instance)):
                # With ``all``, one readable log without the line decides it; with ``any``, an unread one might
                # have held it.
                seen_unmet = set(lines_by_instance) - set(satisfied) - set(unreadable)
                undecided = unreadable and not (rule["instances"] == "all" and seen_unmet)
                (unobserved if undecided else unmet).append(rule["evidence_id"])
                continue
            evidence.append(
                make_evidence(
                    spec,
                    rule["evidence_id"],
                    "observer.stopped",
                    {
                        "pattern": rule["pattern"],
                        "matches": {instance_id: matches[:5] for instance_id, matches in satisfied.items()},
                        "counts": {instance_id: len(matches) for instance_id, matches in satisfied.items()},
                    },
                    artifacts=excerpt_paths,
                )
            )

        forbidden_hits = {}
        for rule in self._config["forbidden"]:
            pattern = re.compile(rule["pattern"])
            for instance_id, lines in lines_by_instance.items():
                hits = [line for line in lines if pattern.search(line)]
                if hits:
                    forbidden_hits.setdefault(rule["pattern"], {})[instance_id] = hits[:5]
        if forbidden_hits:
            evidence.append(
                make_evidence(spec, FORBIDDEN_LOG_MATCH, "observer.stopped", forbidden_hits, artifacts=excerpt_paths)
            )
        blind = self._config["forbidden"] and unreadable
        return make_result(
            spec,
            ComponentStatus.ERROR if blind else ComponentStatus.PASS,
            (
                f"cannot read the log of {', '.join(sorted(unreadable))}, so the forbidden patterns are unchecked"
                if blind
                else f"scanned {sum(len(lines) for lines in lines_by_instance.values())} log lines"
            )
            + (f"; forbidden patterns matched: {', '.join(sorted(forbidden_hits))}" if forbidden_hits else "")
            + (f"; unmet: {', '.join(unmet)}" if unmet else "")
            + (f"; could not observe: {', '.join(unobserved)}" if unobserved else ""),
            evidence=evidence,
            diagnostics={"forbidden": forbidden_hits, "unmet": unmet, "unreadable": unreadable},
            unobserved=unobserved,
            preconditions=_preconditions(self._config["expect"]),
        )


# ---------------------------------------------------------------------------
# valkey.observer.log-trigger — turn instance log lines into lifecycle events
# ---------------------------------------------------------------------------

_LOG_TRIGGER_FIELDS = {
    "instances": ConfigField((str, list), default="all"),
    # Each line a rule matches publishes ``publish``; ``evidence_id`` is met by ``min_count`` lines on one
    # instance; ``max_publishes`` (null: no limit) caps the events.
    "rules": ConfigField(
        list,
        items=mapping_of(
            {
                "pattern": optional(str),
                "publish": TEXT,
                "evidence_id": optional(str),
                "min_count": _MIN_COUNT,
                "max_publishes": ConfigField(int, allow_none=True, default=None, minimum=1),
                "required": ConfigField(bool, default=False),
            }
        ),
    ),
    # A spec bundle (formal/<protocol>/): a rule without a pattern takes the
    # log pattern its mapping.yaml gives for the rule's publish event.
    "spec": ConfigField(str, allow_none=True, default=None),
    "spec_sha256": SPEC_SHA256_FIELD,
    "poll_interval_s": ConfigField(float, default=0.002, minimum=0.0005, maximum=10),
    # Optional interpreter thread-switch interval while tailing.  Each file
    # read releases the GIL, and under contention reacquiring it can take up
    # to the default 5 ms, which delays faults that must land right after a
    # line.  Process-wide; restored when the observer stops.
    "switch_interval_s": ConfigField(float, allow_none=True, default=None, minimum=0.0001, maximum=0.1),
}

_EVENT_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_RESERVED_GROUPS = {"instance_id", "line", "logged_at", "detected_at", "rule"}


def spec_rules(config: dict) -> tuple[list[dict[str, Any]], Optional[dict[str, str]]]:
    """``config["rules"]`` with every missing pattern taken from the spec mapping, and the mapping's identity."""

    rules = plain_config(config["rules"])
    if all("pattern" in rule for rule in rules):
        return rules, None
    if not config["spec"]:
        raise ConfigError("a rule without a pattern needs spec: the bundle whose mapping.yaml holds its pattern")
    mapping = pinned_spec_mapping(config)
    patterns = mapping.log_patterns()
    for index, rule in enumerate(rules):
        if "pattern" not in rule:
            if rule["publish"] not in patterns:
                raise ConfigError(f"rules[{index}]: {mapping.path} has no log pattern for event {rule['publish']!r}")
            rule["pattern"] = patterns[rule["publish"]]
    return rules, {"bundle": config["spec"], "mapping": mapping.path, "mapping_sha256": mapping.sha256}


def _validate_log_trigger(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    if not config["rules"]:
        raise ConfigError("rules must list at least one {pattern, publish} rule")
    rules, _ = spec_rules(config)
    for index, rule in enumerate(rules):
        where = f"rules[{index}]"
        compiled = _regex(rule["pattern"], f"{where}.pattern")
        if set(compiled.groupindex) & _RESERVED_GROUPS:
            raise ConfigError(f"{where}.pattern must not define the reserved groups {sorted(_RESERVED_GROUPS)}")
        if not _EVENT_NAME.fullmatch(rule["publish"]):
            raise ConfigError(f"{where}.publish must be an event name such as votes.granted, got {rule['publish']!r}")
        if "evidence_id" in rule:
            _namespaced(rule["evidence_id"], where)
        elif rule["required"]:
            raise ConfigError(f"{where}.required needs an evidence_id")


def _group_value(value: Optional[str]) -> Any:
    return int(value) if value is not None and value.isdigit() else value


class LogTriggerObserver:
    """Tail instance logs and publish an event for every line a rule matches.

    The event payload carries the instance, the line, its log timestamp
    (``logged_at``), and the pattern's named groups (all-digit groups become
    integers), so a fault can trigger on, say, the second vote a specific instance
    grants (``occurrence``/``when_payload``) and target that instance
    (``instance: event``).  Logs are appended across restarts; a file that
    shrinks is re-read from the start.  A rule with ``required: true`` becomes
    one of the plan's required evidence.
    """

    def __init__(self) -> None:
        self._config: dict[str, Any] = {}
        self._files: dict[str, str] = {}
        self._offsets: dict[str, int] = {}
        self._partial: dict[str, str] = {}
        self._rules: list[tuple[dict, Any]] = []
        self._counts: list[dict[str, int]] = []
        self._published: list[int] = []
        self._matches: list[dict[str, Any]] = []
        self._thread: Optional[StoppableThread] = None
        self._context: Any = None
        self._spec: Optional[ComponentSpec] = None
        self._errors: list[str] = []
        # Logs a read failed on, by instance: lines may have been missed.
        self._unreadable: dict[str, str] = {}
        self._saved_switch_interval: Optional[float] = None
        self._spec_mapping: Optional[dict[str, str]] = None

    def start(self, context: Any, spec: ComponentSpec):
        self._config = spec.config
        self._context = context
        self._spec = spec
        handle = deployment(context)
        for instance_id in select_instances(handle, self._config["instances"]):
            path = handle.log_file(instance_id)
            self._files[instance_id] = path
            self._offsets[instance_id] = log_size(path)
            self._partial[instance_id] = ""
        rules, self._spec_mapping = spec_rules(self._config)
        self._rules = [(rule, re.compile(rule["pattern"])) for rule in rules]
        self._counts = [{} for _ in self._rules]
        self._published = [0 for _ in self._rules]
        if self._config["switch_interval_s"] is not None:
            self._saved_switch_interval = sys.getswitchinterval()
            sys.setswitchinterval(self._config["switch_interval_s"])

        def loop(stop_event) -> None:
            while not stop_event.wait(self._config["poll_interval_s"]):
                self._safe_poll()

        self._thread = StoppableThread(f"log-trigger-{spec.id}", loop)
        self._thread.start()
        return make_result(spec, ComponentStatus.PASS, f"tailing {len(self._files)} log file(s)")

    def _new_lines(self, instance_id: str) -> list[str]:
        path = self._files[instance_id]
        size = log_size(path)
        offset = self._offsets[instance_id]
        if size < offset:
            offset = 0
            self._partial[instance_id] = ""
        if size == offset:
            return []
        try:
            with open(path, "rb") as handle:
                handle.seek(offset)
                data = handle.read(size - offset)
        except OSError as exc:
            self._unreadable[instance_id] = f"{type(exc).__name__}: {exc}"
            return []
        self._offsets[instance_id] = offset + len(data)
        text = self._partial[instance_id] + data.decode("utf-8", errors="replace")
        lines = text.split("\n")
        self._partial[instance_id] = lines.pop()
        return lines

    def poll(self) -> int:
        """Publish events for lines written since the last poll; return how many.

        Each instance's lines are published as soon as they are read, so a fault
        lands right after its line; across instances, events are ordered within
        one poll interval, and ``logged_at`` carries the log time.
        """

        published = 0
        for instance_id in self._files:
            lines = self._new_lines(instance_id)
            if not lines:
                continue
            detected_at = time.time()
            for line in lines:
                for index, (_, pattern) in enumerate(self._rules):
                    match = pattern.search(line)
                    if match is not None:
                        published += self._publish(instance_id, index, line, match, detected_at)
        return published

    def _publish(self, instance_id: str, index: int, line: str, match: Any, detected_at: float) -> int:
        rule = self._rules[index][0]
        logged_at = log_line_time(line)
        if logged_at is None:
            logged_at = detected_at
        self._counts[index][instance_id] = self._counts[index].get(instance_id, 0) + 1
        record: dict[str, Any] = {"rule": index, "instance_id": instance_id, "logged_at": logged_at, "line": line[:500]}
        self._matches.append(record)
        if rule["max_publishes"] is not None and self._published[index] >= rule["max_publishes"]:
            return 0
        self._published[index] += 1
        payload = {key: _group_value(value) for key, value in match.groupdict().items()}
        payload.update(
            {
                "instance_id": instance_id,
                "line": line[:500],
                "logged_at": logged_at,
                "detected_at": detected_at,
                "rule": index,
            }
        )
        event = self._context.event_bus.publish(rule["publish"], source=self._spec.id, payload=payload)
        record["event"] = rule["publish"]
        record["event_sequence"] = event.sequence
        return 1

    def _safe_poll(self) -> None:
        try:
            self.poll()
        except Exception as exc:  # a failing subscriber must not stop the tailer
            if len(self._errors) < 20:
                self._errors.append(f"{type(exc).__name__}: {exc}")

    def stop(self, context: Any, spec: ComponentSpec):
        if self._thread is not None:
            self._thread.stop()
        if self._saved_switch_interval is not None:
            sys.setswitchinterval(self._saved_switch_interval)
            self._saved_switch_interval = None
        self._safe_poll()
        path = os.path.join(component_artifact_dir(context, spec), "log-matches.jsonl")
        write_jsonl(path, self._matches)
        evidence = []
        unmet = []
        unobserved = []
        for index, (rule, _) in enumerate(self._rules):
            if "evidence_id" not in rule:
                continue
            satisfied = {
                instance: count for instance, count in self._counts[index].items() if count >= rule["min_count"]
            }
            if not satisfied:
                # A failed read, or a poll a subscriber broke off, may have skipped the line.
                (unobserved if self._unreadable or self._errors else unmet).append(rule["evidence_id"])
                continue
            evidence.append(
                make_evidence(
                    spec,
                    rule["evidence_id"],
                    "observer.stopped",
                    {"pattern": rule["pattern"], "event": rule["publish"], "counts": satisfied},
                    artifacts=(path,),
                )
            )
        counts = {rule["publish"]: sum(self._counts[index].values()) for index, (rule, _) in enumerate(self._rules)}
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{len(self._matches)} matching line(s): {counts}"
            + (f"; unmet: {', '.join(unmet)}" if unmet else "")
            + (f"; could not observe: {', '.join(unobserved)}" if unobserved else ""),
            evidence=evidence,
            unobserved=unobserved,
            diagnostics={
                "counts": counts,
                "unmet": unmet,
                "matches_path": path,
                "errors": self._errors,
                "unreadable": self._unreadable,
                "patterns": {rule["publish"]: rule["pattern"] for rule, _ in self._rules},
                "spec": self._spec_mapping,
            },
        )


def _precondition_references(group: str):
    """The preconditions ``config[group]`` names: the compiler rejects one no component of the plan declares."""

    def references(spec: ComponentSpec) -> list[tuple[str, None]]:
        rules = plain_config(spec.config).get(group) or []
        return [(rule["precondition"], None) for rule in rules if isinstance(rule, dict) and rule.get("precondition")]

    return references


def _marked_required(group: str):
    """The evidence IDs of the rules in ``config[group]`` marked ``required: true``."""

    def required_evidence(spec: ComponentSpec) -> list[str]:
        rules = plain_config(spec.config).get(group) or []
        return [
            rule["evidence_id"]
            for rule in rules
            if isinstance(rule, dict) and rule.get("required") is True and "evidence_id" in rule
        ]

    return required_evidence


# A met expectation: a value predicate's matches, or a delta's per-instance deltas.
_INFO_DECLARES = Declarations(
    evidence={
        "{config.expectations[].evidence_id}": declared(
            mapping_of(
                {
                    "field": TEXT,
                    "instances": ConfigField(str, choices=("any", "all")),
                    "predicate": optional(dict),
                    "matches": optional(list),
                    "deltas": optional(dict),
                }
            )
        )
    }
)
_LOG_DECLARES = Declarations(
    evidence={
        "{config.expect[].evidence_id}": declared(mapping_of({"pattern": TEXT, "matches": MAPPING, "counts": MAPPING})),
        # {pattern: {instance_id: [lines]}}
        FORBIDDEN_LOG_MATCH: declared(mapping_of(additional=MAPPING)),
    }
)
# A matched line's event carries the pattern's named groups besides these.
_LINE_EVENT = ConfigSchema(
    {"instance_id": TEXT, "line": TEXT, "logged_at": NUMBER, "detected_at": NUMBER, "rule": INTEGER}, additional=ANY
)
_LOG_TRIGGER_DECLARES = Declarations(
    events={"{config.rules[].publish}": _LINE_EVENT},
    evidence={
        "{config.rules[].evidence_id}": declared(mapping_of({"pattern": TEXT, "event": TEXT, "counts": MAPPING}))
    },
)

REGISTRATIONS = (
    component_registration(
        "valkey.observer.info",
        InfoObserver,
        _INFO_FIELDS,
        check=_validate_info,
        selectors={"instances": ()},
        required_evidence=_marked_required("expectations"),
        references_evidence=_precondition_references("expectations"),
        declares=_INFO_DECLARES,
    ),
    component_registration(
        "valkey.observer.log",
        LogObserver,
        _LOG_FIELDS,
        check=_validate_log,
        selectors={"instances": ()},
        required_evidence=_marked_required("expect"),
        references_evidence=_precondition_references("expect"),
        declares=_LOG_DECLARES,
    ),
    component_registration(
        "valkey.observer.log-trigger",
        LogTriggerObserver,
        _LOG_TRIGGER_FIELDS,
        check=_validate_log_trigger,
        selectors={"instances": ()},
        required_evidence=_marked_required("rules"),
        declares=_LOG_TRIGGER_DECLARES,
    ),
)
