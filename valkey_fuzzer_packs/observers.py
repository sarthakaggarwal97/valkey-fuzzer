"""Observers that collect path evidence: INFO sampling and log scanning.

Observers emit evidence only when a declared expectation is met, so a pack's
``required_evidence`` is the single place that decides whether a missing
observation fails the run.  Raw samples and log excerpts are always saved as
artifacts for diagnosis.
"""

import os
import re
import threading
import time
from typing import Any, Optional

from valkey_fuzzer.extensions import ComponentStatus

from .common import (
    ComponentConfigError,
    ComponentSpec,
    Field,
    RunPlan,
    StoppableThread,
    compare,
    component_artifact_dir,
    config_validator,
    make_evidence,
    make_result,
    resolved_config,
    select_nodes,
    topology,
    validate_node_selector,
    validate_predicate,
    write_jsonl,
)

# ---------------------------------------------------------------------------
# valkey.info
# ---------------------------------------------------------------------------

_INFO_FIELDS = {
    "nodes": Field((str, list), default="primaries"),
    "sections": Field(list, default=lambda: ["persistence"]),
    "interval_s": Field(float, default=0.05, minimum=0, maximum=3600),
    "on_events": Field(list, default=list),
    "expectations": Field(list, default=list),
}

_EXPECTATION_KEYS = {"evidence_id", "field", "when", "nodes", "delta_min", "delta_max"}
_WHEN = ("any", "baseline", "final", "during")


def _validate_info(config: dict, spec: ComponentSpec, plan: RunPlan) -> None:
    validate_node_selector(config["nodes"])
    if not config["sections"] or not all(isinstance(item, str) for item in config["sections"]):
        raise ComponentConfigError("sections must be a non-empty list of INFO section names")
    if not all(isinstance(item, str) for item in config["on_events"]):
        raise ComponentConfigError("on_events must be a list of event names")
    seen = set()
    for index, expectation in enumerate(config["expectations"]):
        where = f"expectations[{index}]"
        if not isinstance(expectation, dict):
            raise ComponentConfigError(f"{where} must be a mapping")
        for key in ("evidence_id", "field"):
            if not isinstance(expectation.get(key), str) or not expectation.get(key):
                raise ComponentConfigError(f"{where}.{key} is required")
        if "." not in expectation["evidence_id"]:
            raise ComponentConfigError(f"{where}.evidence_id must be namespaced")
        if expectation["evidence_id"] in seen:
            raise ComponentConfigError(f"{where}.evidence_id duplicates another expectation")
        seen.add(expectation["evidence_id"])
        if expectation.get("when", "any") not in _WHEN:
            raise ComponentConfigError(f"{where}.when must be one of {list(_WHEN)}")
        if expectation.get("nodes", "any") not in ("any", "all"):
            raise ComponentConfigError(f"{where}.nodes must be 'any' or 'all'")
        is_delta = "delta_min" in expectation or "delta_max" in expectation
        predicate = {key: value for key, value in expectation.items() if key not in _EXPECTATION_KEYS}
        if is_delta:
            if predicate:
                raise ComponentConfigError(f"{where} cannot combine a delta with a value predicate")
        else:
            validate_predicate(predicate, where)


class InfoObserver:
    """Sample INFO at start, periodically, on lifecycle events, and at stop."""

    def __init__(self) -> None:
        self._samples: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._thread: Optional[StoppableThread] = None
        self._subscriptions: list[Any] = []
        self._nodes: list[str] = []
        self._config: dict[str, Any] = {}
        self._handle: Any = None
        self._clients: dict[str, Any] = {}

    def _client(self, node_id: str) -> Any:
        client = self._clients.get(node_id)
        if client is None:
            client = self._handle.new_client(node_id, timeout=2.0)
            self._clients[node_id] = client
        return client

    def _sample(self, phase: str, event: Optional[Any] = None) -> None:
        for node_id in self._nodes:
            record: dict[str, Any] = {"phase": phase, "node": node_id, "at": time.time()}
            if event is not None:
                record["event"] = event.name
                record["event_sequence"] = event.sequence
            fields: dict[str, Any] = {}
            try:
                client = self._client(node_id)
                for section in self._config["sections"]:
                    fields.update(client.info(section))
            except Exception as exc:
                record["error"] = f"{type(exc).__name__}: {exc}"
                stale = self._clients.pop(node_id, None)
                if stale is not None:
                    try:
                        stale.close()
                    except Exception:
                        pass
            record["fields"] = {key: value for key, value in fields.items() if isinstance(key, str)}
            with self._lock:
                record["index"] = len(self._samples)
                self._samples.append(record)

    def start(self, context: Any, spec: ComponentSpec):
        self._config = resolved_config(spec, _INFO_FIELDS)
        self._handle = topology(context)
        self._nodes = select_nodes(self._handle, self._config["nodes"])
        self._sample("baseline")
        for event_name in self._config["on_events"]:
            self._subscriptions.append(
                context.event_bus.subscribe(event_name, lambda event: self._sample("event", event))
            )
        if self._config["interval_s"] > 0:
            # The periodic sampler owns separate clients so it never shares a
            # socket with the event-driven sampler.
            sampler = InfoObserver()
            sampler._config = self._config
            sampler._handle = self._handle
            sampler._nodes = self._nodes
            sampler._samples = self._samples
            sampler._lock = self._lock
            self._periodic = sampler

            def loop(stop_event) -> None:
                while not stop_event.wait(self._config["interval_s"]):
                    sampler._sample("periodic")

            self._thread = StoppableThread(f"info-{spec.instance_id}", loop)
            self._thread.start()
        return make_result(spec, ComponentStatus.PASS, f"sampling INFO on {', '.join(self._nodes)}")

    def stop(self, context: Any, spec: ComponentSpec):
        for subscription in self._subscriptions:
            context.event_bus.unsubscribe(subscription)
        if self._thread is not None:
            self._thread.stop()
            for client in getattr(self, "_periodic", self)._clients.values():
                try:
                    client.close()
                except Exception:
                    pass
        self._sample("final")
        for client in self._clients.values():
            try:
                client.close()
            except Exception:
                pass
        path = os.path.join(component_artifact_dir(context, spec), "info-samples.jsonl")
        with self._lock:
            samples = sorted(self._samples, key=lambda item: item["at"])
        write_jsonl(path, samples)
        evidence = []
        unmet = []
        for expectation in self._config["expectations"]:
            matched = self._evaluate(expectation, samples)
            if matched is None:
                unmet.append(expectation["evidence_id"])
                continue
            evidence.append(
                make_evidence(
                    spec,
                    expectation["evidence_id"],
                    "observer.stopped",
                    matched,
                    artifacts=(path,),
                )
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{len(samples)} samples; {len(evidence)} expectation(s) met"
            + (f"; unmet: {', '.join(unmet)}" if unmet else ""),
            evidence=evidence,
            diagnostics={"unmet_expectations": unmet, "samples_path": path},
        )

    def _evaluate(self, expectation: dict, samples: list[dict]) -> Optional[dict]:
        field = expectation["field"]
        when = expectation.get("when", "any")
        aggregate = expectation.get("nodes", "any")
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
        for sample in candidates:
            if "error" in sample or sample["node"] in matches:
                continue
            value = sample["fields"].get(field)
            if compare(value, predicate):
                matches[sample["node"]] = {
                    "node": sample["node"],
                    "field": field,
                    "value": value,
                    "phase": sample["phase"],
                    "sample_index": sample["index"],
                    "event": sample.get("event"),
                }
        if not matches or (aggregate == "all" and set(matches) != set(self._nodes)):
            return None
        return {"field": field, "nodes": aggregate, "predicate": predicate, "matches": list(matches.values())}

    def _evaluate_delta(self, expectation: dict, samples: list[dict], aggregate: str) -> Optional[dict]:
        field = expectation["field"]
        deltas = {}
        for node_id in self._nodes:
            node_samples = [
                sample
                for sample in samples
                if sample["node"] == node_id and "error" not in sample and field in sample["fields"]
            ]
            if len(node_samples) < 2:
                continue
            try:
                first = float(node_samples[0]["fields"][field])
                last = float(node_samples[-1]["fields"][field])
            except (TypeError, ValueError):
                continue
            delta = last - first
            if "delta_min" in expectation and delta < float(expectation["delta_min"]):
                continue
            if "delta_max" in expectation and delta > float(expectation["delta_max"]):
                continue
            deltas[node_id] = {"baseline": first, "final": last, "delta": delta}
        if not deltas or (aggregate == "all" and set(deltas) != set(self._nodes)):
            return None
        return {"field": field, "nodes": aggregate, "deltas": deltas}


# ---------------------------------------------------------------------------
# valkey.log
# ---------------------------------------------------------------------------

_LOG_FIELDS = {
    "nodes": Field((str, list), default="all"),
    "required": Field(list, default=list),
    "forbidden": Field(list, default=list),
    "max_excerpt_lines": Field(int, default=200, minimum=1, maximum=100_000),
}


def _validate_log(config: dict, spec: ComponentSpec, plan: RunPlan) -> None:
    validate_node_selector(config["nodes"])
    for group in ("required", "forbidden"):
        for index, rule in enumerate(config[group]):
            where = f"{group}[{index}]"
            if not isinstance(rule, dict) or not isinstance(rule.get("pattern"), str):
                raise ComponentConfigError(f"{where} needs a pattern")
            try:
                re.compile(rule["pattern"])
            except re.error as exc:
                raise ComponentConfigError(f"{where}.pattern is not a valid regex: {exc}") from exc
            if group == "required":
                if not isinstance(rule.get("evidence_id"), str) or "." not in rule["evidence_id"]:
                    raise ComponentConfigError(f"{where}.evidence_id must be a namespaced string")
                count = rule.get("min_count", 1)
                if isinstance(count, bool) or not isinstance(count, int) or count < 1:
                    raise ComponentConfigError(f"{where}.min_count must be a positive integer")
                if rule.get("nodes", "any") not in ("any", "all"):
                    raise ComponentConfigError(f"{where}.nodes must be 'any' or 'all'")


def read_log_from(path: str, offset: int) -> list[str]:
    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            handle.seek(offset)
            return handle.read().splitlines()
    except OSError:
        return []


def log_size(path: str) -> int:
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


class LogObserver:
    """Scan node logs written during the observation window."""

    def __init__(self) -> None:
        self._offsets: dict[str, tuple[str, int]] = {}
        self._config: dict[str, Any] = {}

    def start(self, context: Any, spec: ComponentSpec):
        self._config = resolved_config(spec, _LOG_FIELDS)
        handle = topology(context)
        for node_id in select_nodes(handle, self._config["nodes"]):
            path = handle.log_file(node_id)
            self._offsets[node_id] = (path, log_size(path))
        return make_result(spec, ComponentStatus.PASS, f"watching {len(self._offsets)} log file(s)")

    def stop(self, context: Any, spec: ComponentSpec):
        lines_by_node = {
            node_id: read_log_from(path, offset) for node_id, (path, offset) in sorted(self._offsets.items())
        }
        directory = component_artifact_dir(context, spec)
        excerpt_paths = []
        for node_id, lines in lines_by_node.items():
            excerpt = os.path.join(directory, f"{node_id}.log")
            with open(excerpt, "w", encoding="utf-8") as handle:
                handle.write("\n".join(lines[-self._config["max_excerpt_lines"] :]))
            excerpt_paths.append(excerpt)

        evidence = []
        unmet = []
        for rule in self._config["required"]:
            pattern = re.compile(rule["pattern"])
            per_node = {
                node_id: [line for line in lines if pattern.search(line)] for node_id, lines in lines_by_node.items()
            }
            satisfied = {
                node_id: matches for node_id, matches in per_node.items() if len(matches) >= rule.get("min_count", 1)
            }
            if not satisfied or (rule.get("nodes", "any") == "all" and set(satisfied) != set(lines_by_node)):
                unmet.append(rule["evidence_id"])
                continue
            evidence.append(
                make_evidence(
                    spec,
                    rule["evidence_id"],
                    "observer.stopped",
                    {
                        "pattern": rule["pattern"],
                        "matches": {node_id: matches[:5] for node_id, matches in satisfied.items()},
                        "counts": {node_id: len(matches) for node_id, matches in satisfied.items()},
                    },
                    artifacts=excerpt_paths,
                )
            )

        forbidden_hits = {}
        for rule in self._config["forbidden"]:
            pattern = re.compile(rule["pattern"])
            for node_id, lines in lines_by_node.items():
                hits = [line for line in lines if pattern.search(line)]
                if hits:
                    forbidden_hits.setdefault(rule["pattern"], {})[node_id] = hits[:5]
        if forbidden_hits:
            evidence.append(
                make_evidence(
                    spec, "valkey.log.forbidden_match", "observer.stopped", forbidden_hits, artifacts=excerpt_paths
                )
            )
            return make_result(
                spec,
                ComponentStatus.FAIL,
                f"forbidden log patterns matched: {', '.join(sorted(forbidden_hits))}",
                evidence=evidence,
                diagnostics={"forbidden": forbidden_hits, "unmet_required": unmet},
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"scanned {sum(len(lines) for lines in lines_by_node.values())} log lines"
            + (f"; unmet: {', '.join(unmet)}" if unmet else ""),
            evidence=evidence,
            diagnostics={"unmet_required": unmet},
        )


REGISTRATIONS = (
    ("valkey.info", "observer", InfoObserver, config_validator(_INFO_FIELDS, extra=_validate_info)),
    ("valkey.log", "observer", LogObserver, config_validator(_LOG_FIELDS, extra=_validate_log)),
)
