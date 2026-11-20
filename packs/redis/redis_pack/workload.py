"""``redis.workload.set-get``: one writer that SETs and GETs a bounded key set and records what the server said.

Every operation is one ledger entry, ``{id, op, key, value?, reply?, outcome, t_start, t_end, error?}``, where
``outcome`` is ``ok`` (the server acknowledged it), ``error`` (the server, or the connect, refused it: it was
not applied), or ``ambiguous`` (sent, no answer: it may have been applied). ``stop`` writes the ledger to
``ledger.jsonl`` in the component's folder and emits ``redis.workload.ledger`` with its summary and that path;
``redis.oracle.ledger`` reads it. Values are unique per operation (``v<id>``), so a value names the write that
put it there.
"""

import json
import os
import threading
from collections import Counter
from typing import Any, Optional

import valkey

from valkey_fuzzer.clock import now
from valkey_fuzzer.declarations import Declarations, EvidenceDeclaration
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole, component_artifact_dir
from valkey_fuzzer.registrations import ComponentRegistration
from valkey_fuzzer.results import ComponentStatus, make_evidence, make_result
from valkey_fuzzer.schema import ConfigField, ConfigSchema

from .deployment import INSTANCE_ID, SERVICE

LEDGER_EVIDENCE = "redis.workload.ledger"

CONFIG = ConfigSchema(
    {
        "instance": ConfigField(str, default=INSTANCE_ID),
        "keys": ConfigField(int, default=200, minimum=1, maximum=1_000_000),
        # Operations per second; the writer pauses between operations to hold it.
        "rate": ConfigField(float, default=200.0, minimum=0.1, maximum=20_000),
        "get_share": ConfigField(float, default=0.2, minimum=0, maximum=1),
        "op_timeout_s": ConfigField(float, default=1.0, minimum=0.01, maximum=60),
        "prefix": ConfigField(str, default="redis-writes:"),
    }
)

LEDGER_SUMMARY = ConfigSchema(
    {
        "operations": ConfigField(int),
        "acknowledged": ConfigField(int),
        "rejected": ConfigField(int),
        "ambiguous": ConfigField(int),
        "keys": ConfigField(int),
        "prefix": ConfigField(str),
        "started_at": ConfigField(float),
        "stopped_at": ConfigField(float),
        "ledger_path": ConfigField(str),
    }
)

DECLARES = Declarations(evidence={LEDGER_EVIDENCE: EvidenceDeclaration(ConfigField(dict, schema=LEDGER_SUMMARY))})


def deployment(context: Any) -> Any:
    """The ``redis.deployment`` service of the run's deployment."""

    return context.services["deployment"][SERVICE]


def operation(connection: Any, op_id: int, key: str, read: bool) -> dict[str, Any]:
    """Run one SET or GET on ``connection`` and return its ledger entry.

    The connect is separate from the send: a connect that fails means the command was never sent (``error``); a
    send or a read that fails after it means the server may have applied it (``ambiguous``), and the connection
    is dropped so the next operation opens a fresh one. A reply that is an error (``LOADING``, ``MISCONF``)
    means the server read the command and refused it (``error``).
    """

    entry: dict[str, Any] = {"id": op_id, "op": "get" if read else "set", "key": key, "t_start": now()}
    if not read:
        entry["value"] = f"v{op_id}"
    try:
        connection.connect()
    except Exception as exc:
        entry.update(outcome="error", error=f"{type(exc).__name__}: {exc}", t_end=now())
        return entry
    try:
        if read:
            connection.send_command("GET", key)
        else:
            connection.send_command("SET", key, entry["value"])
        reply = connection.read_response()
    except (valkey.BusyLoadingError, valkey.ResponseError) as exc:
        entry.update(outcome="error", error=f"{type(exc).__name__}: {exc}")
    except Exception as exc:
        entry.update(outcome="ambiguous", error=f"{type(exc).__name__}: {exc}")
        connection.disconnect()
    else:
        entry["outcome"] = "ok"
        if read:
            entry["reply"] = reply.decode("utf-8", errors="replace") if isinstance(reply, bytes) else reply
    entry["t_end"] = now()
    return entry


class SetGetWorkload:
    """SET and GET keys ``<prefix><n>`` on one connection until stopped, one operation at a time."""

    def __init__(self) -> None:
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._ledger: list[dict[str, Any]] = []
        self._counters: Counter = Counter()
        self._started_at: Optional[float] = None
        self._failure: Optional[str] = None

    def start(self, context: Any, spec: ComponentSpec):
        config = spec.config
        handle = deployment(context)
        connection = handle.connection(config["instance"], timeout=config["op_timeout_s"])
        rng = context.rng(spec.id, "ops")
        self._started_at = now()
        self._thread = threading.Thread(
            target=self._run, args=(connection, config, rng), name=f"workload-{spec.id}", daemon=True
        )
        self._thread.start()
        return make_result(spec, ComponentStatus.PASS, f"writing {config['keys']} keys at {config['rate']:g} ops/s")

    def _run(self, connection: Any, config: Any, rng: Any) -> None:
        interval = 1.0 / config["rate"]
        op_id = 0
        try:
            while not self._stop.is_set():
                op_id += 1
                key = f"{config['prefix']}{rng.randrange(config['keys'])}"
                entry = operation(connection, op_id, key, rng.random() < config["get_share"])
                self._ledger.append(entry)
                self._counters[entry["outcome"]] += 1
                if self._stop.wait(interval):
                    break
        except Exception as exc:
            self._failure = f"{type(exc).__name__}: {exc}"
        finally:
            connection.disconnect()

    def stop(self, context: Any, spec: ComponentSpec):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(30.0, spec.config["op_timeout_s"] * 3))
            if self._thread.is_alive():
                return make_result(spec, ComponentStatus.ERROR, "the writer did not stop; its ledger is not read")
        ledger_path = os.path.join(component_artifact_dir(context, spec), "ledger.jsonl")
        with open(ledger_path, "w", encoding="utf-8") as handle:
            for entry in self._ledger:
                handle.write(json.dumps(entry, sort_keys=True) + "\n")
        summary = {
            "operations": len(self._ledger),
            "acknowledged": self._counters["ok"],
            "rejected": self._counters["error"],
            "ambiguous": self._counters["ambiguous"],
            "keys": spec.config["keys"],
            "prefix": spec.config["prefix"],
            "started_at": self._started_at,
            "stopped_at": now(),
            "ledger_path": ledger_path,
        }
        evidence = (make_evidence(spec, LEDGER_EVIDENCE, "workload.quiesced", summary, artifacts=(ledger_path,)),)
        if self._failure is not None:
            # A workload does not judge the server: a writer that broke leaves the run untested.
            return make_result(spec, ComponentStatus.ERROR, f"the writer failed: {self._failure}", evidence=evidence)
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{summary['operations']} operations ({summary['acknowledged']} acknowledged, "
            f"{summary['ambiguous']} ambiguous, {summary['rejected']} rejected)",
            evidence=evidence,
            diagnostics={"counters": dict(self._counters)},
        )


REGISTRATIONS = (
    ComponentRegistration(
        "redis.workload.set-get", ComponentRole.WORKLOAD, SetGetWorkload, config=CONFIG, declares=DECLARES
    ),
)
