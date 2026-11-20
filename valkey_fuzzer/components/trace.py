"""``valkey.observer.trace``: the run's events together with per-node samples of the server's state, as JSONL
that ``valkey-fuzzer spec conform`` reads.

What a round samples from each node is what the spec bundle's ``mapping.yaml``
asks for in its ``sample:`` section (:class:`~valkey_fuzzer.spec.bundle.SampleSpec`):
INFO sections, ``DEBUG DIGEST`` (and ``DEBUG DIGEST-VALUE`` of named keys), the
node's cluster view, ``CLUSTER GETSLOTMIGRATIONS``, and the ``vars`` line of its
``nodes.conf``. Without a bundle, a cluster deployment is sampled as the
election bundle needs it (the view and ``nodes.conf``), and a standalone one
records events only.
"""

import dataclasses
import fnmatch
import os
import threading
import time
from collections.abc import Mapping
from typing import Any, Optional

import valkey

from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.deployment.local.queries import parse_cluster_info
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.protocols import component_artifact_dir
from valkey_fuzzer.results import ComponentStatus, make_evidence, make_result
from valkey_fuzzer.schema import ConfigError, ConfigField, plain_config
from valkey_fuzzer.spec.bundle import TRACE_FILE, SampleSpec

from .cluster import cluster_view, read_view
from .files import nodes_conf_path, read_cluster_vars, write_jsonl
from .instances import close_quietly, deployment, select_instances
from .polling import StoppableThread
from .registration import SPEC_SHA256_FIELD, component_registration, pinned_spec_mapping
from .schemas import INTEGER, TEXT, declared, mapping_of

TRACE_EVIDENCE = "valkey.trace.recorded"
# What a cluster deployment is sampled for without a bundle: the node's view and its nodes.conf, as the election
# bundle reads them.
CLUSTER_SAMPLE = SampleSpec(
    info=(), digest=False, digest_keys=(), cluster=True, slot_migrations=False, nodes_conf=True, on=()
)
NO_SAMPLE = SampleSpec(
    info=(), digest=False, digest_keys=(), cluster=False, slot_migrations=False, nodes_conf=False, on=()
)


_TRACE_FIELDS = {
    "nodes": ConfigField((str, list), default="all"),
    "interval_s": ConfigField(float, default=0.25, minimum=0, maximum=3600),
    "sample_on_events": ConfigField(bool, default=True),
    # Event-driven rounds start at least this long after the event, so a
    # burst of election events costs one round and the sampler does not
    # compete with a fault that fires inside the event's dispatch.
    "event_delay_s": ConfigField(float, default=0.05, minimum=0, maximum=60),
    "max_rounds": ConfigField(int, default=20_000, minimum=1, maximum=1_000_000),
    "sample_timeout_s": ConfigField(float, default=0.5, minimum=0.05, maximum=30),
    # Server configs recorded per node in the header (CONFIG GET at start).
    "config_keys": ConfigField(
        list, default=lambda: ["cluster-config-save-behavior", "cluster-node-timeout"], items=TEXT
    ),
    # The spec bundle whose mapping reads this trace (`valkey-fuzzer spec conform`): its `sample:` section says
    # what to sample, and the header records the mapping's digest so a later change to it shows.
    "spec": ConfigField(str, allow_none=True, default=None),
    "spec_sha256": SPEC_SHA256_FIELD,
}


def _validate_trace(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    if not all(config["config_keys"]):
        raise ConfigError("config_keys must hold config names; remove the empty one")
    if config["spec"]:
        sample = pinned_spec_mapping(config).sample
        if sample is not None and (sample.digest or sample.digest_keys):
            # DEBUG is refused unless the server allows it; a sample that fails would leave the data claims unchecked.
            refused = [
                instance_id
                for instance_id, instance in plan.instances.items()
                if str((instance.get("server_config") or {}).get("enable-debug-command", "no")) != "yes"
            ]
            if refused:
                raise ConfigError(
                    f"spec {config['spec']!r} samples DEBUG DIGEST, which {refused} refuse: set enable-debug-command "
                    f"yes in the deployment's server_config"
                )


def _jobs(reply: Any) -> dict[str, list[dict[str, Any]]]:
    """CLUSTER GETSLOTMIGRATIONS (RESP3 maps, or RESP2 flat key-value arrays) as the jobs by kind, each in
    creation order: ``export``, ``import`` (one with a source), and ``tracking`` (a replica's, which names no
    source node: Valkey's ``is_tracking_only``)."""

    jobs: dict[str, list[dict[str, Any]]] = {"export": [], "import": [], "tracking": []}
    for item in reply or []:
        job = dict(item) if isinstance(item, Mapping) else dict(zip(item[::2], item[1::2]))
        if job.get("operation") == "EXPORT":
            kind = "export"
        else:
            kind = "import" if "source_node" in job else "tracking"
        jobs[kind].append(job)
    return jobs


def _text(response: Any, **options: Any) -> Any:
    """A reply as the client read it: INFO is kept as text and parsed into strings, not into numbers, so a
    replication ID of digits stays an ID."""

    return response


class TraceObserver:
    """Record every lifecycle event and per-node samples as JSONL.

    Records are ``header`` (node map, configs, what is sampled), ``event``
    (bus events; ``t`` is the log timestamp for log-derived events, else the
    receive time), and ``observation`` (one node's sample: ``round``,
    ``reason``, ``node``, ``t``, and the parts the ``sample:`` section asks
    for; ``error`` when the node did not answer). A node's INFO, digests and
    slot migrations are read in one MULTI/EXEC, so they are of one instant.
    Sampling rounds run periodically and after events, on a background
    thread, so the observer never delays a fault that fires inside an event's
    dispatch.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._records: list[dict[str, Any]] = []
        self._wake = threading.Event()
        self._thread: Optional[StoppableThread] = None
        self._subscription = None
        self._clients: dict[str, Any] = {}
        self._rounds = 0
        self._header: dict[str, Any] = {}
        self._sample = NO_SAMPLE

    def start(self, context: Any, spec: ComponentSpec):
        self._config = spec.config
        self._handle = deployment(context)
        self._nodes = select_instances(self._handle, self._config["nodes"])
        plan = context.plan
        cluster = plan.deployment_mode == "cluster"
        mapping = pinned_spec_mapping(self._config) if self._config["spec"] else None
        wanted = mapping.sample if mapping is not None and mapping.sample is not None else None
        if wanted is None:
            wanted = CLUSTER_SAMPLE if cluster else NO_SAMPLE
        if not cluster:
            # The cluster parts have nothing to read on a standalone deployment.
            wanted = dataclasses.replace(wanted, cluster=False, slot_migrations=False, nodes_conf=False)
        self._sample = wanted
        nodes = {}
        for node in self._handle.instances:
            view = read_view(self._handle, node.instance_id) if cluster else None
            nodes[node.instance_id] = {
                "node_id": view["node_id"] if view else None,
                "shard": node.shard_id,
                "port": node.port,
                "planned_role": node.role,
            }
            nodes[node.instance_id]["config"], error = self._server_config(node.instance_id)
            if error is not None:
                # spec conform then names the config the trace lacks, and this says why.
                nodes[node.instance_id]["config_error"] = error
        self._header = {
            "kind": "header",
            "t": time.time(),
            "scenario_id": plan.scenario_id,
            "seed": plan.seed,
            "experiment_id": plan.experiment_id,
            "inputs": plain_config(plan.inputs),
            "nodes": nodes,
            "samples": dataclasses.asdict(self._sample),
        }
        if mapping is not None:
            self._header["spec"] = {"bundle": self._config["spec"], "mapping_sha256": mapping.sha256}
        self._subscription = context.event_bus.subscribe("*", self._on_event)
        self._sample_round("start")
        failed = [record for record in self._records if record["kind"] == "observation" and "error" in record]
        if failed:
            # Every node is up at the start: a sample that fails now would fail all run long, and the claims that
            # read it would be checked on nothing.
            self._unsubscribe(context)
            close_quietly(*self._clients.values())
            return make_result(
                spec, ComponentStatus.ERROR, f"sampling {failed[0]['node']} failed at the start: {failed[0]['error']}"
            )

        def loop(stop_event) -> None:
            interval = self._config["interval_s"] or None
            while not stop_event.is_set():
                woke = self._wake.wait(interval)
                if stop_event.is_set():
                    return
                self._wake.clear()
                if woke and self._config["event_delay_s"]:
                    if stop_event.wait(self._config["event_delay_s"]):
                        return
                    self._wake.clear()
                self._sample_round("event" if woke else "periodic")

        self._thread = StoppableThread(f"trace-{spec.id}", loop)
        self._thread.start()
        return make_result(spec, ComponentStatus.PASS, f"tracing {len(self._nodes)} node(s)")

    def _server_config(self, instance_id: str) -> tuple[dict[str, Any], Optional[str]]:
        """The ``config_keys`` the instance reported, and why it reported no more (None when it did)."""

        values: dict[str, Any] = {}
        with self._handle.new_client(instance_id, timeout=2.0) as client:
            try:
                for key in self._config["config_keys"]:
                    values.update(client.config_get(key))
            except (valkey.exceptions.ValkeyError, OSError) as exc:
                return values, f"{type(exc).__name__}: {exc}"
        return values, None

    def _on_event(self, event: Any) -> None:
        payload = dict(event.payload)
        logged_at = payload.get("logged_at")
        with self._lock:
            self._records.append(
                {
                    "kind": "event",
                    "t": logged_at if isinstance(logged_at, (int, float)) else time.time(),
                    "received_at": time.time(),
                    "sequence": event.sequence,
                    "name": event.name,
                    "source": event.source,
                    "payload": payload,
                }
            )
        on = self._sample.on
        if self._config["sample_on_events"] and (not on or any(fnmatch.fnmatchcase(event.name, g) for g in on)):
            self._wake.set()

    def _client(self, instance_id: str) -> Any:
        client = self._clients.get(instance_id)
        if client is None:
            client = self._handle.new_client(instance_id, timeout=self._config["sample_timeout_s"])
            if hasattr(client, "set_response_callback"):
                client.set_response_callback("INFO", _text)
            self._clients[instance_id] = client
        return client

    def _sample_node(self, instance_id: str) -> dict[str, Any]:
        """One node's observation: the parts ``sample:`` asks for, or ``error`` when the node did not answer."""

        sample = self._sample
        record: dict[str, Any] = {}
        try:
            client = self._client(instance_id)
            if sample.cluster:
                view = cluster_view(client)
                # The trace records the view; the raw CLUSTER NODES records serve only the split check.
                view.pop("nodes")
                record.update(view)
            commands: list[tuple[str, ...]] = [("INFO", section) for section in sample.info]
            if sample.digest:
                commands.append(("DEBUG", "DIGEST"))
            commands.extend(("DEBUG", "DIGEST-VALUE", key) for key in sample.digest_keys)
            if sample.slot_migrations:
                commands.append(("CLUSTER", "GETSLOTMIGRATIONS"))
            if commands:
                pipe = client.pipeline(transaction=True)
                for command in commands:
                    pipe.execute_command(*command)
                replies = pipe.execute()
                for command, reply in zip(commands, replies):
                    if command[0] == "INFO":
                        record.setdefault("info", {}).update(parse_cluster_info(reply))
                    elif command[1] == "DIGEST":
                        record["digest"] = reply
                    elif command[1] == "DIGEST-VALUE":
                        record.setdefault("digest_values", {})[command[2]] = (
                            reply[0] if isinstance(reply, list) else reply
                        )
                    else:
                        record["slot_migrations"] = _jobs(reply)
        except Exception as exc:
            record["error"] = f"{type(exc).__name__}: {exc}"
            close_quietly(self._clients.pop(instance_id, None))
        record["t"] = time.time()
        if sample.nodes_conf:
            try:
                record["disk"] = read_cluster_vars(nodes_conf_path(self._handle, instance_id))
            except Exception:
                record["disk"] = None
        return record

    def _sample_round(self, reason: str) -> None:
        if self._rounds >= self._config["max_rounds"] or not self._sample.collects():
            return
        self._rounds += 1
        for instance_id in self._nodes:
            record = {"kind": "observation", "round": self._rounds, "reason": reason, "node": instance_id}
            record.update(self._sample_node(instance_id))
            with self._lock:
                self._records.append(record)

    def _unsubscribe(self, context: Any) -> None:
        if self._subscription is not None:
            context.event_bus.unsubscribe(self._subscription)
            self._subscription = None

    def stop(self, context: Any, spec: ComponentSpec):
        self._unsubscribe(context)
        if self._thread is not None:
            # With interval_s 0 the sampler waits only for an event: wake it to see the stop.
            self._thread.stop_event.set()
            self._wake.set()
        problem = self._thread.stop() if self._thread is not None else None
        if problem is not None:
            # The trace is partial, and a sampler still running owns its clients.
            return make_result(spec, ComponentStatus.ERROR, f"tracing {problem}")
        self._sample_round("final")
        close_quietly(*self._clients.values())
        with self._lock:
            records = sorted(self._records, key=lambda item: (item["t"], item.get("sequence", 0)))
        path = os.path.join(component_artifact_dir(context, spec), TRACE_FILE)
        write_jsonl(path, [self._header, *records])
        counts = {
            "events": sum(record["kind"] == "event" for record in records),
            "observations": sum(record["kind"] == "observation" for record in records),
            "rounds": self._rounds,
        }
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"trace: {counts['events']} events, {counts['observations']} observations",
            evidence=(
                make_evidence(spec, TRACE_EVIDENCE, "observer.stopped", {"path": path, **counts}, artifacts=(path,)),
            ),
        )


_TRACE_DECLARES = Declarations(
    evidence={
        TRACE_EVIDENCE: declared(
            mapping_of({"path": TEXT, "events": INTEGER, "observations": INTEGER, "rounds": INTEGER})
        )
    }
)

REGISTRATIONS = (
    component_registration(
        "valkey.observer.trace",
        TraceObserver,
        _TRACE_FIELDS,
        check=_validate_trace,
        selectors={"nodes": ()},
        pins_spec=True,
        declares=_TRACE_DECLARES,
    ),
)
