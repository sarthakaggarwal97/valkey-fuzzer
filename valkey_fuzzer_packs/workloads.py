"""Continuous key/value workload with an acknowledged-operation ledger.

One writer thread issues operations sequentially, so every key has a total
order of client-visible operations.  Each ledger entry records what the client
observed: ``ok`` (acknowledged), ``error`` (definite rejection, never
applied), or ``ambiguous`` (timeout/disconnect; may or may not be applied).
Oracles reconstruct the set of acceptable final values from this ledger under
an explicit durability policy instead of guessing.
"""

import hashlib
import json
import os
import random
import string
import threading
import time
from typing import Any, Optional

import valkey

from valkey_fuzzer.extensions import ComponentStatus

from .common import (
    ComponentConfigError,
    ComponentSpec,
    Field,
    RunPlan,
    StoppableThread,
    component_artifact_dir,
    config_validator,
    make_evidence,
    make_result,
    remaining,
    resolved_config,
    topology,
    write_jsonl,
)

LEDGER_EVIDENCE = "valkey.workload.ledger"

_WORKLOAD_FIELDS = {
    "stable_keys": Field(int, default=0, minimum=0, maximum=5_000_000),
    "mutable_keys": Field(int, default=100, minimum=0, maximum=1_000_000),
    "preload_mutable": Field(bool, default=True),
    "rate": Field(float, default=200.0, minimum=0, maximum=100_000),
    "value_size": Field(int, default=64, minimum=1, maximum=1_048_576),
    "compressible": Field(bool, default=False),
    "mix": Field(dict, default=lambda: {"set": 0.7, "del": 0.1, "get": 0.2}),
    "prefix": Field(str, allow_none=True, default=None),
    "op_timeout_s": Field(float, default=2.0, minimum=0.05, maximum=60),
    "preload_batch": Field(int, default=1000, minimum=1, maximum=100_000),
    "max_ops": Field(int, allow_none=True, default=None, minimum=1),
    "drain_on_stop": Field(bool, default=False),
    "drain_timeout_s": Field(float, default=120.0, minimum=0.1, maximum=7200),
}

DRAINED_EVENT = "workload.drained"


def _validate_workload(config: dict, spec: ComponentSpec, plan: RunPlan) -> None:
    mix = config["mix"]
    unknown = sorted(set(mix) - {"set", "del", "get"})
    if unknown:
        raise ComponentConfigError(f"mix contains unknown operations: {unknown}")
    weights = list(mix.values())
    if not weights or any(isinstance(weight, bool) or not isinstance(weight, (int, float)) for weight in weights):
        raise ComponentConfigError("mix weights must be numbers")
    if any(weight < 0 for weight in weights) or sum(weights) <= 0:
        raise ComponentConfigError("mix weights must be non-negative and not all zero")
    if config["drain_on_stop"] and config["max_ops"] is None:
        raise ComponentConfigError("drain_on_stop requires max_ops")
    if config["stable_keys"] == 0 and config["mutable_keys"] == 0:
        raise ComponentConfigError("workload needs stable_keys or mutable_keys")


def key_prefix(spec: ComponentSpec, config: dict) -> str:
    return config["prefix"] or f"vf:{spec.instance_id}"


def make_value(rng: random.Random, size: int, compressible: bool, tag: str) -> str:
    if compressible:
        body = (tag + ":") * (size // (len(tag) + 1) + 1)
        return body[:size]
    alphabet = string.ascii_letters + string.digits
    return (tag + ":" + "".join(rng.choice(alphabet) for _ in range(size)))[: max(size, len(tag) + 1)]


class _Target:
    """Routes commands to the current writable endpoint."""

    def __init__(self, handle: Any, timeout: float):
        self.handle = handle
        self.timeout = timeout
        self.cluster = getattr(getattr(handle, "topology_type", None), "value", "") == "cluster"
        self.client: Any = None
        self.node_id: Optional[str] = None

    def connect(self) -> Any:
        if self.client is not None:
            return self.client
        if self.cluster:
            self.client = self.handle.cluster_client(timeout=self.timeout)
            self.node_id = "cluster"
        else:
            primaries = self.handle.primaries()
            if not primaries:
                raise valkey.exceptions.ConnectionError("no reachable primary")
            self.node_id = primaries[0]
            self.client = self.handle.new_client(self.node_id, timeout=self.timeout)
        return self.client

    def reset(self) -> None:
        if self.client is not None:
            try:
                self.client.close()
            except Exception:
                pass
        self.client = None

    def close(self) -> None:
        self.reset()


def _classify(exc: BaseException) -> str:
    if isinstance(exc, valkey.exceptions.ResponseError):
        # A server error reply (READONLY, MISCONF, OOM, WRONGTYPE, ...) means
        # the command was rejected and not applied.
        return "error"
    return "ambiguous"


class KeyValueWorkload:
    """Preload deterministic data, then mutate it continuously."""

    def __init__(self) -> None:
        self._thread: Optional[StoppableThread] = None
        self._ledger: list[dict[str, Any]] = []
        self._preload: dict[str, str] = {}
        self._counters = {"ok": 0, "error": 0, "ambiguous": 0}
        self._started_at = 0.0
        self._target: Optional[_Target] = None
        self._config: dict[str, Any] = {}
        self._drained = threading.Event()

    def start(self, context: Any, spec: ComponentSpec):
        config = resolved_config(spec, _WORKLOAD_FIELDS)
        self._config = config
        handle = topology(context)
        rng = context.rng(spec.instance_id, "values")
        op_rng = context.rng(spec.instance_id, "operations")
        prefix = key_prefix(spec, config)
        self._target = _Target(handle, config["op_timeout_s"])
        self._started_at = time.time()

        stable = {
            f"{prefix}:stable:{index}": make_value(rng, config["value_size"], config["compressible"], f"s{index}")
            for index in range(config["stable_keys"])
        }
        mutable_keys = [f"{prefix}:m:{index}" for index in range(config["mutable_keys"])]
        mutable_initial = {}
        if config["preload_mutable"]:
            mutable_initial = {
                key: make_value(rng, config["value_size"], config["compressible"], f"p{index}")
                for index, key in enumerate(mutable_keys)
            }
        preload = {**stable, **mutable_initial}
        try:
            self._load(preload, config["preload_batch"])
        except Exception as exc:
            self._target.close()
            return make_result(spec, ComponentStatus.ERROR, f"preload failed: {exc}")
        self._preload = preload
        context.event_bus.publish(
            "workload.seeded",
            source=spec.instance_id,
            payload={"keys": len(preload), "stable_keys": len(stable), "mutable_keys": len(mutable_keys)},
        )

        if config["rate"] > 0 and mutable_keys:
            ops = [name for name in ("set", "del", "get") if config["mix"].get(name, 0) > 0]
            weights = [config["mix"][name] for name in ops]
            interval = 1.0 / config["rate"]
            sequence = {"value": 0}

            def loop(stop_event) -> None:
                next_at = time.monotonic()
                while not stop_event.is_set():
                    if config["max_ops"] is not None and sequence["value"] >= config["max_ops"]:
                        if not self._drained.is_set():
                            self._drained.set()
                            context.event_bus.publish(
                                DRAINED_EVENT,
                                source=spec.instance_id,
                                payload={"operations": sequence["value"]},
                            )
                        stop_event.wait(0.05)
                        continue
                    op = op_rng.choices(ops, weights)[0]
                    key = op_rng.choice(mutable_keys)
                    sequence["value"] += 1
                    op_id = sequence["value"]
                    value = None
                    if op == "set":
                        value = make_value(rng, config["value_size"], config["compressible"], f"w{op_id}")
                    if self._execute(op_id, op, key, value) != "ok":
                        stop_event.wait(0.02)
                    next_at += interval
                    delay = next_at - time.monotonic()
                    if delay > 0:
                        stop_event.wait(delay)
                    else:
                        next_at = time.monotonic()

            self._thread = StoppableThread(f"workload-{spec.instance_id}", loop)
            self._thread.start()

        return make_result(
            spec,
            ComponentStatus.PASS,
            f"seeded {len(preload)} keys; mutating {len(mutable_keys)} keys at {config['rate']}/s",
            diagnostics={"prefix": prefix},
        )

    def _load(self, preload: dict[str, str], batch: int) -> None:
        items = list(preload.items())
        for start in range(0, len(items), batch):
            chunk = items[start : start + batch]
            for attempt in range(3):
                try:
                    client = self._target.connect()
                    pipe = client.pipeline(transaction=False)
                    for key, value in chunk:
                        pipe.set(key, value)
                    replies = pipe.execute()
                    if not all(reply is True or reply == "OK" for reply in replies):
                        raise RuntimeError(f"unexpected preload replies: {replies[:3]}")
                    break
                except (valkey.exceptions.ConnectionError, valkey.exceptions.TimeoutError):
                    self._target.reset()
                    if attempt == 2:
                        raise
                    time.sleep(0.2)

    def _execute(self, op_id: int, op: str, key: str, value: Optional[str]) -> str:
        entry: dict[str, Any] = {"id": op_id, "op": op, "key": key, "t_start": time.time()}
        if value is not None:
            entry["value"] = value
        try:
            client = self._target.connect()
            entry["node"] = self._target.node_id
            if op == "set":
                reply = client.set(key, value)
                entry["reply"] = "OK" if reply is True else reply
            elif op == "del":
                entry["reply"] = client.delete(key)
            else:
                entry["reply"] = client.get(key)
            entry["outcome"] = "ok"
        except Exception as exc:
            entry["outcome"] = _classify(exc)
            entry["error"] = f"{type(exc).__name__}: {exc}"
            # Reconnect after any failure: an error reply such as READONLY means
            # the endpoint stopped being the writable primary (e.g. failover).
            self._target.reset()
        entry["t_end"] = time.time()
        self._counters[entry["outcome"]] += 1
        self._ledger.append(entry)
        return entry["outcome"]

    def stop(self, context: Any, spec: ComponentSpec):
        stopped = True
        thread_error = None
        drained = True
        if self._config.get("drain_on_stop") and self._thread is not None:
            deadline = time.monotonic() + remaining(context, self._config["drain_timeout_s"])
            while not self._drained.is_set() and self._thread.is_alive() and time.monotonic() < deadline:
                self._drained.wait(0.2)
            drained = self._drained.is_set()
        if self._thread is not None:
            stopped = self._thread.stop(timeout=max(30.0, self._config.get("op_timeout_s", 2.0) * 3))
            thread_error = self._thread.error
        if self._target is not None:
            self._target.close()
        directory = component_artifact_dir(context, spec)
        ledger_path = os.path.join(directory, "ledger.jsonl")
        preload_path = os.path.join(directory, "preload.jsonl")
        write_jsonl(ledger_path, self._ledger)
        write_jsonl(preload_path, ({"key": key, "value": value} for key, value in sorted(self._preload.items())))
        summary = {
            "operations": len(self._ledger),
            "acknowledged": self._counters["ok"],
            "rejected": self._counters["error"],
            "ambiguous": self._counters["ambiguous"],
            "preloaded_keys": len(self._preload),
            "stable_keys": self._config.get("stable_keys", 0),
            "prefix": key_prefix(spec, self._config) if self._config else None,
            "started_at": self._started_at,
            "stopped_at": time.time(),
            "ledger_path": ledger_path,
            "preload_path": preload_path,
            "replies_digest": replies_digest(self._ledger),
            "drained": self._drained.is_set(),
        }
        evidence = (
            make_evidence(
                spec,
                LEDGER_EVIDENCE,
                "workload.quiesced",
                summary,
                artifacts=(ledger_path, preload_path),
            ),
        )
        if thread_error is not None:
            return make_result(
                spec, ComponentStatus.ERROR, f"workload thread crashed: {thread_error}", evidence=evidence
            )
        if not stopped:
            return make_result(spec, ComponentStatus.ERROR, "workload thread did not stop", evidence=evidence)
        if not drained:
            return make_result(
                spec,
                ComponentStatus.FAIL,
                f"workload did not complete {self._config['max_ops']} operations within "
                f"{self._config['drain_timeout_s']}s ({len(self._ledger)} done)",
                evidence=evidence,
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{summary['operations']} operations ({summary['acknowledged']} acknowledged, "
            f"{summary['ambiguous']} ambiguous, {summary['rejected']} rejected)",
            evidence=evidence,
            diagnostics={"counters": dict(self._counters)},
        )


def replies_digest(ledger: list[dict[str, Any]]) -> str:
    """Digest of the client-visible operation history (ops, keys, outcomes, replies).

    Identical seeds produce identical operation sequences, so two runs whose
    servers behave equivalently produce the same digest.
    """

    material = [
        [
            entry["id"],
            entry["op"],
            entry["key"],
            entry["outcome"],
            entry.get("reply") if entry["outcome"] == "ok" else None,
        ]
        for entry in ledger
    ]
    return hashlib.sha256(json.dumps(material, separators=(",", ":")).encode("utf-8")).hexdigest()


REGISTRATIONS = (
    (
        "valkey.kv_workload",
        "workload",
        KeyValueWorkload,
        config_validator(_WORKLOAD_FIELDS, extra=_validate_workload),
    ),
)
