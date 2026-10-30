"""Cluster failover-election components.

``valkey.action.cluster-election`` forces an election in one shard,
``valkey.observer.cluster-trace`` records the run's events together with every
node's epochs (in memory and in nodes.conf) as JSONL, and
``valkey.oracle.cluster-election-safety`` checks the invariants of the TLA+
model in ``formal/cluster-election/model.tla`` on the live run: one winner per
epoch, one vote per primary per epoch, every winner counted votes from a
majority of the primaries, votes saved before they leave under
``cluster-config-save-behavior sync``, and no split primary.

Election facts come from node log lines published by a
``valkey.observer.log-trigger`` observer (see the
``valkey.cluster-election-durability`` experiment): votes granted (``Failover
auth granted to``), votes counted (``Failover auth ACK from ... (ACKs N,
quorum Q)``, logged since Valkey 9.2), elections started, and elections won
(``configEpoch set to N after successful failover``). The patterns and event
names live in ``formal/cluster-election/mapping.yaml``; the observer and the
oracle read them from there (``spec: cluster-election``).
"""

import functools
import os
import signal
import threading
import time
from collections.abc import Mapping, Sequence
from typing import Any, Optional

from valkey_fuzzer.extensions import (
    ComponentSpec,
    ComponentStatus,
    ConfigError,
    ConfigField,
    ConfigSchema,
    Declarations,
    Evidence,
    ExecutionPlan,
    component_artifact_dir,
    evidence_of,
    make_evidence,
    make_result,
    parse_cluster_info,
    parse_cluster_nodes,
    plain_config,
)

from .common import (
    ANY,
    FLAG,
    INTEGER,
    LIST,
    MAPPING,
    SPEC_SHA256_FIELD,
    TEXT,
    StoppableThread,
    check_shard,
    close_quietly,
    component_registration,
    declared,
    deployment,
    mapping_of,
    nodes_conf_path,
    nullable,
    pause_owned,
    pinned_spec_mapping,
    poll_until,
    read_cluster_vars,
    read_jsonl,
    remaining,
    select_instances,
    stop_owned,
    wait_following,
    write_jsonl,
)
from .faults import FAULT_INJECTED

TRACE_EVIDENCE = "valkey.cluster.trace.recorded"

# ---------------------------------------------------------------------------
# One node's cluster view, from CLUSTER INFO and CLUSTER NODES
# ---------------------------------------------------------------------------


def parse_slots(tokens: Sequence[str]) -> list[list[int]]:
    """``["0-5460", "5462"]`` -> ``[[0, 5460], [5462, 5462]]``; migration markers are skipped."""

    ranges = []
    for token in tokens:
        if token.startswith("["):
            continue
        start, _, end = token.partition("-")
        try:
            ranges.append([int(start), int(end or start)])
        except ValueError:
            continue
    return ranges


def ranges_overlap(left: Sequence[Sequence[int]], right: Sequence[Sequence[int]]) -> bool:
    return any(a <= d and c <= b for a, b in left for c, d in right)


def _epoch(node: Mapping[str, Any]) -> Optional[int]:
    try:
        return int(node["config_epoch"])
    except ValueError:
        return None


def cluster_view(client: Any) -> dict[str, Any]:
    """One node's view: its own epochs and role plus every primary it knows (slots as ranges)."""

    info = parse_cluster_info(client.execute_command("CLUSTER", "INFO"))
    nodes = parse_cluster_nodes(client.execute_command("CLUSTER", "NODES"))
    myself = next((node for node in nodes if node["is_myself"]), None)
    if myself is None:
        raise RuntimeError("CLUSTER NODES has no myself entry")
    return {
        "cluster_id": myself["node_id"],
        "current_epoch": int(info.get("cluster_current_epoch", 0)),
        "config_epoch": _epoch(myself),
        "role": "primary" if myself["is_primary"] else "replica",
        "primary_id": myself["primary_id"],
        "slots": parse_slots(myself["slots"]),
        "state": info.get("cluster_state"),
        "primaries": [
            {
                "cluster_id": node["node_id"],
                "config_epoch": _epoch(node),
                "slots": parse_slots(node["slots"]),
                "flags": node["flags"].split(","),
            }
            for node in nodes
            if node["is_primary"]
        ],
    }


def _view(handle: Any, node_id: str, timeout: float = 2.0) -> Optional[dict[str, Any]]:
    with handle.new_client(node_id, timeout=timeout) as client:
        try:
            return cluster_view(client)
        except Exception:
            return None


# ---------------------------------------------------------------------------
# valkey.action.cluster-election
# ---------------------------------------------------------------------------

_ELECTION_FIELDS = {
    "shard": ConfigField(int, default=0, minimum=0),
    # force_failover: CLUSTER FAILOVER FORCE on every replica of the shard at
    # once, so the replicas compete in the same epoch and the old primary
    # votes; kill_primary / pause_primary: SIGKILL or SIGSTOP the primary and
    # let failure detection start the election.
    "trigger": ConfigField(str, default="force_failover", choices=("force_failover", "kill_primary", "pause_primary")),
    "candidates": ConfigField(str, default="all", choices=("all", "one")),
    "attempts": ConfigField(int, default=3, minimum=1, maximum=20),
    "timeout_s": ConfigField(float, default=30.0, minimum=1, maximum=600),
    "settle_s": ConfigField(float, default=1.0, minimum=0, maximum=60),
    "pause_s": ConfigField(float, default=10.0, minimum=0.1, maximum=600),
    "restart_killed": ConfigField(bool, default=True),
    # Wait until the shard's other nodes replicate from the winner with an
    # up link, so later steps and oracles start from a settled shard.
    "wait_synced": ConfigField(bool, default=True),
}


def _validate_election(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    check_shard(config["shard"], plan)


class ElectionAction:
    """Force one failover election in a shard and wait for a stable winner."""

    def run(self, context: Any, spec: ComponentSpec):
        config = spec.config
        handle = deployment(context)
        members = [node.instance_id for node in handle.instances if node.shard_id == config["shard"]]
        if not members:
            return make_result(spec, ComponentStatus.FAIL, f"shard {config['shard']} has no nodes")
        before = {node_id: _view(handle, node_id) for node_id in members if handle.is_alive(node_id)}
        owners = [node_id for node_id, view in before.items() if view and view["role"] == "primary" and view["slots"]]
        if len(owners) != 1:
            return make_result(
                spec,
                ComponentStatus.FAIL,
                f"shard {config['shard']} has {len(owners)} slot-owning primaries before the election",
                diagnostics={"before": before},
            )
        old_primary = owners[0]
        old_epoch = before[old_primary]["config_epoch"] or 0
        candidates = [node_id for node_id, view in before.items() if view and view["role"] == "replica"]
        if config["candidates"] == "one":
            candidates = candidates[:1]
        if not candidates:
            return make_result(spec, ComponentStatus.FAIL, f"shard {config['shard']} has no replica to promote")
        record: dict[str, Any] = {
            "shard": config["shard"],
            "trigger": config["trigger"],
            "old_primary": old_primary,
            "old_config_epoch": old_epoch,
            "candidates": candidates,
            "before": {node_id: _summary(view) for node_id, view in before.items()},
            "attempts": [],
        }
        context.event_bus.publish(
            f"{spec.id}.election-started",
            source=spec.id,
            payload={"shard": config["shard"], "trigger": config["trigger"], "old_primary": old_primary},
        )
        winner = None
        timeout = config["timeout_s"]
        own = functools.partial(context.obligations.open, spec)
        if config["trigger"] != "force_failover":
            # Failure detection elects only a replica whose data is recent: Valkey refuses one disconnected from
            # its primary for longer than the data-age bound, which a replica restarted moments ago and still in
            # its first sync is. Start from candidates linked to the primary (FORCE skips that check by design).
            synced, record["candidates_synced"] = wait_following(context, handle, candidates, old_primary, timeout)
            if not synced:
                return make_result(
                    spec,
                    ComponentStatus.ERROR,
                    f"the replicas of shard {config['shard']} did not link to {old_primary} within {timeout}s, so "
                    f"no {config['trigger']} election could start from synced replicas: {record['candidates_synced']}",
                    diagnostics=record,
                )
        if config["trigger"] == "force_failover":
            for attempt in range(config["attempts"]):
                replies = _parallel_failover(handle, candidates)
                record["attempts"].append({"attempt": attempt, "replies": replies, "at": time.time()})
                winner = self._wait_winner(context, handle, members, old_primary, old_epoch, config, timeout)
                if winner is not None:
                    break
        elif config["trigger"] == "kill_primary":
            killed = None
            if config["restart_killed"]:
                killed = stop_owned(own, handle, old_primary, signal.SIGKILL, wait_ready=True)
            else:
                # A kill without restart_killed lasts until the deployment is removed.
                handle.send_signal(old_primary, signal.SIGKILL)
            record["killed_at"] = time.time()
            record["old_primary_exited"] = handle.wait_exit(old_primary, timeout=30)
            winner = self._wait_winner(context, handle, members, old_primary, old_epoch, config, timeout)
            if killed is not None:
                try:
                    killed.discharge()
                    record["old_primary_restarted"] = True
                except Exception as exc:
                    record["restart_error"] = f"{type(exc).__name__}: {exc}"
        else:
            paused = pause_owned(own, handle, old_primary)
            record["paused_at"] = time.time()
            try:
                winner = self._wait_winner(
                    context, handle, members, old_primary, old_epoch, config, min(config["pause_s"], timeout)
                )
            finally:
                paused.discharge()
                record["resumed_at"] = time.time()
        if winner is not None and handle.is_alive(old_primary):
            record["old_primary_demoted"] = self._wait_demoted(context, handle, old_primary, config["timeout_s"])
        if winner is not None and config["wait_synced"]:
            record["replicas_synced"] = wait_following(context, handle, members, winner, config["timeout_s"])[1]
        handle.refresh_roles()
        after = {node_id: _view(handle, node_id) for node_id in members if handle.is_alive(node_id)}
        record["after"] = {node_id: _summary(view) for node_id, view in after.items()}
        record["winner"] = winner
        record["winner_config_epoch"] = (after.get(winner) or {}).get("config_epoch") if winner else None
        completed = context.event_bus.publish(
            f"{spec.id}.election-completed",
            source=spec.id,
            payload={
                "shard": config["shard"],
                "old_primary": old_primary,
                "winner": winner,
                "config_epoch": record["winner_config_epoch"],
            },
        )
        evidence = (
            make_evidence(
                spec, "valkey.cluster.election.result", "action.completed", record, event_sequence=completed.sequence
            ),
        )
        if winner is None:
            return make_result(
                spec,
                ComponentStatus.FAIL,
                f"no replica of shard {config['shard']} won an election ({config['trigger']})",
                evidence=evidence,
                diagnostics=record,
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{winner} replaced {old_primary} (configEpoch {old_epoch} -> {record['winner_config_epoch']})",
            evidence=evidence,
        )

    def _wait_winner(
        self,
        context: Any,
        handle: Any,
        members: list[str],
        old_primary: str,
        old_epoch: int,
        config: dict,
        timeout: float,
    ) -> Optional[str]:
        """Return the shard's new primary once it has been the only one for ``settle_s``."""

        deadline = time.monotonic() + remaining(context, timeout)
        stable_since = None
        leader = None
        while time.monotonic() < deadline:
            owners = []
            for node_id in members:
                if node_id == old_primary or not handle.is_alive(node_id):
                    continue
                view = _view(handle, node_id, timeout=0.5)
                if view and view["role"] == "primary" and view["slots"] and (view["config_epoch"] or 0) > old_epoch:
                    owners.append(node_id)
            current = owners[0] if len(owners) == 1 else None
            if current is not None and current == leader:
                if time.monotonic() - stable_since >= config["settle_s"]:
                    return current
            else:
                leader = current
                stable_since = time.monotonic()
            if not context.sleep(0.05):
                return None
        return None

    def _wait_demoted(self, context: Any, handle: Any, node_id: str, timeout: float) -> bool:
        def check() -> tuple[bool, None]:
            view = _view(handle, node_id, timeout=0.5)
            return bool(view and view["role"] == "replica"), None

        return poll_until(check, timeout_s=timeout, interval_s=0.1, context=context)[0]


def _summary(view: Optional[Mapping[str, Any]]) -> Optional[dict[str, Any]]:
    if view is None:
        return None
    return {key: view[key] for key in ("cluster_id", "current_epoch", "config_epoch", "role", "slots")}


def _parallel_failover(handle: Any, candidates: list[str]) -> dict[str, Any]:
    """Send CLUSTER FAILOVER FORCE to every candidate as close together as possible."""

    clients = {node_id: handle.new_client(node_id, timeout=5.0) for node_id in candidates}
    barrier = threading.Barrier(len(candidates))
    replies: dict[str, Any] = {}

    def send(node_id: str) -> None:
        client = clients[node_id]
        try:
            client.ping()
            barrier.wait(timeout=10)
            sent_at = time.time()
            replies[node_id] = {"reply": str(client.execute_command("CLUSTER", "FAILOVER", "FORCE")), "at": sent_at}
        except Exception as exc:
            replies[node_id] = {"error": f"{type(exc).__name__}: {exc}"}
        finally:
            client.close()

    threads = [threading.Thread(target=send, args=(node_id,), daemon=True) for node_id in candidates]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    return replies


# ---------------------------------------------------------------------------
# valkey.observer.cluster-trace
# ---------------------------------------------------------------------------

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
    # The spec bundle whose mapping reads this trace (`valkey-fuzzer spec conform`);
    # the header records the mapping's digest so a later change to it shows.
    "spec": ConfigField(str, allow_none=True, default=None),
    "spec_sha256": SPEC_SHA256_FIELD,
}


def _validate_trace(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    if not all(config["config_keys"]):
        raise ConfigError("config_keys must hold config names; remove the empty one")
    if config["spec"]:
        pinned_spec_mapping(config)


class ClusterTraceObserver:
    """Record every lifecycle event and per-node epoch observations as JSONL.

    Records are ``header`` (node map), ``event`` (bus events; ``t`` is the
    log timestamp for log-derived events, else the receive time), and
    ``observation`` (one node's currentEpoch, configEpoch, role, slots, the
    primaries it knows, and the ``vars`` line of its nodes.conf).  Sampling
    rounds run periodically and after events, on a background thread, so the
    observer never delays a fault that fires inside an event's dispatch.
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

    def start(self, context: Any, spec: ComponentSpec):
        self._config = spec.config
        self._handle = deployment(context)
        self._nodes = select_instances(self._handle, self._config["nodes"])
        nodes = {}
        for node in self._handle.instances:
            view = _view(self._handle, node.instance_id)
            nodes[node.instance_id] = {
                "cluster_id": view["cluster_id"] if view else None,
                "shard": node.shard_id,
                "port": node.port,
                "planned_role": node.role,
                "config": self._server_config(node.instance_id),
            }
        plan = context.plan
        self._header = {
            "kind": "header",
            "t": time.time(),
            "scenario_id": plan.scenario_id,
            "seed": plan.seed,
            "experiment_id": plan.experiment_id,
            "inputs": plain_config(plan.inputs),
            "nodes": nodes,
        }
        if self._config["spec"]:
            mapping = pinned_spec_mapping(self._config)
            self._header["spec"] = {"bundle": self._config["spec"], "mapping_sha256": mapping.sha256}
        self._subscription = context.event_bus.subscribe("*", self._on_event)
        self._sample_round("start")

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

        self._thread = StoppableThread(f"cluster-trace-{spec.id}", loop)
        self._thread.start()
        return make_result(spec, ComponentStatus.PASS, f"tracing {len(self._nodes)} node(s)")

    def _server_config(self, node_id: str) -> dict[str, Any]:
        values = {}
        with self._handle.new_client(node_id, timeout=2.0) as client:
            try:
                for key in self._config["config_keys"]:
                    values.update(client.config_get(key))
            except Exception:
                pass
        return values

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
        if self._config["sample_on_events"]:
            self._wake.set()

    def _client(self, node_id: str) -> Any:
        client = self._clients.get(node_id)
        if client is None:
            client = self._handle.new_client(node_id, timeout=self._config["sample_timeout_s"])
            self._clients[node_id] = client
        return client

    def _sample_round(self, reason: str) -> None:
        if self._rounds >= self._config["max_rounds"]:
            return
        self._rounds += 1
        round_index = self._rounds
        for node_id in self._nodes:
            record: dict[str, Any] = {"kind": "observation", "round": round_index, "reason": reason, "node": node_id}
            try:
                record.update(cluster_view(self._client(node_id)))
            except Exception as exc:
                record["error"] = f"{type(exc).__name__}: {exc}"
                close_quietly(self._clients.pop(node_id, None))
            record["t"] = time.time()
            try:
                record["disk"] = read_cluster_vars(nodes_conf_path(self._handle, node_id))
            except Exception:
                record["disk"] = None
            with self._lock:
                self._records.append(record)

    def stop(self, context: Any, spec: ComponentSpec):
        if self._subscription is not None:
            context.event_bus.unsubscribe(self._subscription)
        if self._thread is not None:
            self._thread.stop()
        self._sample_round("final")
        close_quietly(*self._clients.values())
        with self._lock:
            records = sorted(self._records, key=lambda item: (item["t"], item.get("sequence", 0)))
        path = os.path.join(component_artifact_dir(context, spec), "trace.jsonl")
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


# ---------------------------------------------------------------------------
# valkey.oracle.cluster-election-safety
# ---------------------------------------------------------------------------

_SAFETY_FIELDS = {
    "trace": ConfigField(str, allow_none=True, default=None),
    "crash_faults": ConfigField(list, default=list, items=TEXT),
    # The bundle whose mapping.yaml names the vote, ACK, and win events (its
    # granted, acked, and won evidence) and the fields they carry.
    "spec": ConfigField(str, default="cluster-election"),
    "spec_sha256": SPEC_SHA256_FIELD,
    # auto: votes must be saved before they leave only when every node runs
    # cluster-config-save-behavior sync.
    "require_durable_votes": ConfigField(str, default="auto", choices=("auto", "always", "never")),
    "converge_timeout_s": ConfigField(float, default=60.0, minimum=0, maximum=600),
}


# The mapping evidence the oracle reads, with the payload fields it uses.
_SAFETY_EVIDENCE = {
    "granted": ("instance_id", "candidate", "epoch"),
    "acked": ("instance_id", "voter", "epoch", "acks", "quorum"),
    "won": ("instance_id", "epoch"),
}


def safety_events(config: Mapping[str, Any]) -> dict[str, str]:
    """``{granted|acked|won: event name}`` from the bundle's mapping, which must carry the fields the oracle reads."""

    name = config["spec"]
    mapping = pinned_spec_mapping(config)
    events = {}
    for key, fields in _SAFETY_EVIDENCE.items():
        evidence = mapping.evidence.get(key)
        if evidence is None or not evidence.event:
            raise ConfigError(f"spec {name!r}: mapping.yaml has no {key} event evidence")
        missing = sorted(set(fields) - set(evidence.fields))
        if missing:
            raise ConfigError(f"spec {name!r}: the {key} evidence lacks fields {missing}")
        events[key] = evidence.event
    return events


def _validate_safety(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    safety_events(config)


def votes_by_epoch(votes: Sequence[Mapping[str, Any]]) -> dict[tuple[str, int], set[str]]:
    """``{(voter, epoch): {candidate, ...}}`` from vote-granted payloads."""

    grouped: dict[tuple[str, int], set[str]] = {}
    for vote in votes:
        grouped.setdefault((vote["instance_id"], int(vote["epoch"])), set()).add(vote["candidate"])
    return grouped


def check_vote_once_per_epoch(votes: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """TLA+ ``VoteOncePerEpoch``: no primary votes for two candidates in one epoch."""

    return [
        {"voter": voter, "epoch": epoch, "candidates": sorted(candidates)}
        for (voter, epoch), candidates in sorted(votes_by_epoch(votes).items())
        if len(candidates) > 1
    ]


def check_one_winner_per_epoch(wins: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """TLA+ ``OneWinnerPerEpoch``: one election epoch never promotes two replicas."""

    winners: dict[int, set[str]] = {}
    for win in wins:
        winners.setdefault(int(win["epoch"]), set()).add(win["instance_id"])
    return [{"epoch": epoch, "winners": sorted(nodes)} for epoch, nodes in sorted(winners.items()) if len(nodes) > 1]


def check_wins_reached_quorum(
    wins: Sequence[Mapping[str, Any]], acks: Sequence[Mapping[str, Any]], primaries: int
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """TLA+ ``Win``: every winner counted votes from a majority of the voting primaries.

    Returns ``(checked, violations)``. A win's votes are the ACK lines its
    candidate logged for the win's epoch; the quorum is the larger of the one
    the candidate logged and a majority of ``primaries``. Distinct voters are
    what count, so a vote counted twice does not make up for a missing one.
    """

    checked, violations = [], []
    for win in wins:
        node, epoch = win["instance_id"], int(win["epoch"])
        counted = [ack for ack in acks if ack["instance_id"] == node and int(ack["epoch"]) == epoch]
        voters = sorted({ack["voter"] for ack in counted})
        quorum = max([primaries // 2 + 1, *(int(ack["quorum"]) for ack in counted if ack.get("quorum") is not None)])
        entry = {"winner": node, "epoch": epoch, "voters": voters, "quorum": quorum}
        checked.append(entry)
        if len(voters) < quorum:
            logged = [int(ack["acks"]) for ack in counted if ack.get("acks") is not None]
            violations.append({**entry, "acks": max(logged, default=len(counted))})
    return checked, violations


def unsaved_votes(
    crashes: Sequence[Mapping[str, Any]],
    votes: Sequence[Mapping[str, Any]],
    acks: Sequence[Mapping[str, Any]],
    cluster_ids: Mapping[str, str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Votes a crashed primary granted but had not saved (``diskVote < lastVote``).

    Returns ``(unsaved, delivered)``: every vote granted before the crash whose
    epoch is above the ``lastVoteEpoch`` in nodes.conf at the crash, and the
    subset a candidate counted before the voter's new process was spawned (so
    the counted vote is the pre-crash one).  ``delivered`` is the model's
    ``PersistBeforeAck = FALSE`` behavior: a vote left before it was saved.
    """

    unsaved, delivered = [], []
    for crash in crashes:
        disk = crash.get("disk_vars_at_kill") or {}
        disk_vote = disk.get("lastVoteEpoch")
        if disk_vote is None:
            continue
        voter = crash["target"]
        voter_id = cluster_ids.get(voter)
        # Once the new process is spawned, an ACK may be its vote rather than the killed one's.
        spawned_at = crash.get("restart_spawned_at") or float("inf")
        for vote in votes:
            if vote["instance_id"] != voter or vote.get("logged_at", 0) > crash["applied_at"]:
                continue
            epoch = int(vote["epoch"])
            if epoch <= disk_vote:
                continue
            entry = {
                "voter": voter,
                "candidate": vote["candidate"],
                "epoch": epoch,
                "disk_last_vote_epoch": disk_vote,
                "disk_current_epoch": disk.get("currentEpoch"),
                "vote_logged_at": vote.get("logged_at"),
                "crashed_at": crash["applied_at"],
            }
            unsaved.append(entry)
            if any(
                ack.get("voter") == voter_id
                and int(ack["epoch"]) == epoch
                and cluster_ids.get(ack["instance_id"]) == vote["candidate"]
                and ack.get("logged_at", 0) < spawned_at
                for ack in acks
            ):
                delivered.append(entry)
    return unsaved, delivered


def same_slot_epoch_collisions(primaries: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Distinct primaries that serve overlapping slots with the same configEpoch."""

    found = []
    for index, left in enumerate(primaries):
        for right in primaries[index + 1 :]:
            if left["cluster_id"] == right["cluster_id"] or not left["slots"] or not right["slots"]:
                continue
            if ranges_overlap(left["slots"], right["slots"]) and left["config_epoch"] == right["config_epoch"]:
                found.append(
                    {
                        "config_epoch": left["config_epoch"],
                        "primaries": sorted([left["cluster_id"], right["cluster_id"]]),
                    }
                )
    return found


def split_primaries(views: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Slots claimed by two primaries, either within one node's view or by two nodes' own claims."""

    problems = []
    for node_id, view in sorted(views.items()):
        owners = [entry for entry in view["primaries"] if entry["slots"] and "fail" not in entry["flags"]]
        for index, left in enumerate(owners):
            for right in owners[index + 1 :]:
                if ranges_overlap(left["slots"], right["slots"]):
                    problems.append({"view": node_id, "primaries": sorted([left["cluster_id"], right["cluster_id"]])})
    claims = [(node_id, view) for node_id, view in sorted(views.items()) if view["role"] == "primary" and view["slots"]]
    for index, (left_id, left) in enumerate(claims):
        for right_id, right in claims[index + 1 :]:
            if ranges_overlap(left["slots"], right["slots"]):
                problems.append({"self_claims": sorted([left_id, right_id])})
    return problems


def trace_collisions(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Sampling rounds in which two nodes claimed the same slots with the same configEpoch."""

    rounds: dict[int, list[Mapping[str, Any]]] = {}
    for record in records:
        if record.get("kind") == "observation" and "error" not in record and record.get("role") == "primary":
            rounds.setdefault(record["round"], []).append(record)
    found = []
    for round_index, claims in sorted(rounds.items()):
        for collision in same_slot_epoch_collisions(claims):
            found.append({"round": round_index, "t": claims[0]["t"], **collision})
    return found


def _payloads(context: Any, name: str) -> list[dict[str, Any]]:
    return [dict(event.payload) for event in context.event_bus.committed_history(name)]


class ElectionSafetyOracle:
    """Check the TLA+ model's election invariants on the run's events, the final views, and the trace.

    The model (formal/cluster-election) says what may never happen; this oracle is where a run that did it
    fails: two votes or two winners in one epoch, a win without a quorum, a vote that left before it was
    saved, or two primaries claiming the same slots.
    """

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = spec.config
        handle = deployment(context)
        cluster_ids = {}
        save_behavior = {}
        for node in handle.instances:
            with handle.new_client(node.instance_id, timeout=2.0) as client:
                try:
                    cluster_ids[node.instance_id] = str(client.execute_command("CLUSTER", "MYID"))
                    save_behavior[node.instance_id] = client.config_get("cluster-config-save-behavior").get(
                        "cluster-config-save-behavior"
                    )
                except Exception:
                    continue

        events = safety_events(config)
        votes = _payloads(context, events["granted"])
        acks = _payloads(context, events["acked"])
        wins = _payloads(context, events["won"])
        crashes = [
            item.value
            for fault in config["crash_faults"]
            for item in evidence_of(evidence, FAULT_INJECTED, fault)
            if isinstance(item.value, Mapping)
        ]
        violations: dict[str, Any] = {}
        double_votes = check_vote_once_per_epoch(votes)
        if double_votes:
            violations["vote_once_per_epoch"] = double_votes
        double_wins = check_one_winner_per_epoch(wins)
        if double_wins:
            violations["one_winner_per_epoch"] = double_wins
        unsaved, delivered = unsaved_votes(crashes, votes, acks, cluster_ids)
        durable = config["require_durable_votes"]
        if durable == "auto":
            durable = "always" if save_behavior and set(save_behavior.values()) == {"sync"} else "never"
        if durable == "always" and delivered:
            violations["vote_left_before_save"] = delivered

        views, converged, unanswered = self._converge(context, handle, config["converge_timeout_s"])
        # Valkey's cluster size: the primaries that serve slots, one per shard.
        primaries = max(
            [len({node.shard_id for node in handle.instances})]
            + [len([entry for entry in view["primaries"] if entry["slots"]]) for view in views.values()]
        )
        # Without any ACK line the quorum check cannot run (servers before 9.2 log none).
        checked_wins, short = check_wins_reached_quorum(wins, acks, primaries) if acks else ([], [])
        if short:
            violations["win_without_quorum"] = short
        split = split_primaries(views)
        if split:
            violations["split_primary"] = split
        collisions = [
            {"view": node_id, **collision}
            for node_id, view in sorted(views.items())
            for collision in same_slot_epoch_collisions(view["primaries"])
        ]
        trace_path = None
        if config["trace"]:
            recorded = evidence_of(evidence, TRACE_EVIDENCE, config["trace"])
            if recorded and recorded[-1].artifacts:
                trace_path = recorded[-1].artifacts[0]
                collisions.extend(trace_collisions(read_jsonl(trace_path)))
        if collisions:
            violations["same_slots_same_config_epoch"] = collisions
        owners = {}
        for view in views.values():
            for entry in view["primaries"]:
                if entry["slots"]:
                    owners[entry["cluster_id"]] = entry["config_epoch"]
        duplicate_epochs = sorted({epoch for epoch in owners.values() if list(owners.values()).count(epoch) > 1})

        record = {
            "save_behavior": save_behavior,
            "require_durable_votes": durable,
            "votes": len(votes),
            "acks": len(acks),
            "elections_won": sorted({int(win["epoch"]) for win in wins}),
            "wins": checked_wins,
            "crashes": [
                {
                    key: crash.get(key)
                    for key in (
                        "target",
                        "applied_at",
                        "restart_spawned_at",
                        "restart_ready_at",
                        "disk_vars_at_kill",
                        "trigger_payload",
                    )
                }
                for crash in crashes
            ],
            "unsaved_votes_at_crash": unsaved,
            "unsaved_votes_delivered": delivered,
            "converged": converged,
            "final_epochs": {node_id: _summary(view) for node_id, view in sorted(views.items())},
            "cross_shard_duplicate_config_epochs": duplicate_epochs,
            "trace": trace_path,
        }
        if violations:
            return make_result(
                spec,
                ComponentStatus.FAIL,
                "election safety violated: " + ", ".join(sorted(violations)),
                diagnostics={**record, "violations": violations},
            )
        if not converged and (unanswered or not views):
            # A running node whose view could not be read may hold a split, or be the one that agrees: unjudged.
            return make_result(
                spec,
                ComponentStatus.ERROR,
                "cannot read the cluster views of running node(s) "
                f"{', '.join(unanswered) or 'all'} within {config['converge_timeout_s']:g}s",
                diagnostics={**record, "unanswered": unanswered},
            )
        if not converged:
            return make_result(
                spec,
                ComponentStatus.FAIL,
                "cluster views did not converge on one primary per slot",
                diagnostics=record,
            )
        if wins and not acks:
            # Nothing says the winners had a majority: the check cannot run, so the run cannot pass.
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"cannot check that {len(wins)} election winner(s) reached quorum: no 'Failover auth ACK' line "
                f"was published as {events['acked']!r} (Valkey logs them since 9.2)",
                diagnostics=record,
            )
        evidence_out = [make_evidence(spec, "valkey.cluster.election-safety.verified", "oracle.completed", record)]
        if unsaved:
            # Coverage signal: the run reached the model's dangerous state
            # (a crash with a vote in memory that nodes.conf did not hold).
            evidence_out.append(
                make_evidence(spec, "valkey.cluster.vote.unsaved-at-crash", "oracle.completed", {"votes": unsaved})
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{len(votes)} vote(s), {len(record['elections_won'])} epoch(s) won, "
            f"{len(unsaved)} unsaved at crash ({len(delivered)} delivered); one winner per epoch, "
            "each with a quorum of votes",
            evidence=evidence_out,
        )

    def _converge(self, context: Any, handle: Any, timeout: float) -> tuple[dict[str, dict], bool, list[str]]:
        """Poll until every running node answers and agrees on slot owners and their configEpochs: the last
        views, whether they agreed, and the running nodes that did not answer."""

        def check() -> tuple[bool, tuple[dict[str, dict], list[str]]]:
            running = [node.instance_id for node in handle.instances if handle.is_alive(node.instance_id)]
            read = {node_id: _view(handle, node_id) for node_id in running}
            views = {node_id: view for node_id, view in read.items() if view is not None}
            unanswered = [node_id for node_id, view in read.items() if view is None]
            maps = set()
            for view in views.values():
                owners = tuple(
                    sorted(
                        (tuple(map(tuple, entry["slots"])), entry["cluster_id"], entry["config_epoch"])
                        for entry in view["primaries"]
                        if entry["slots"] and "fail" not in entry["flags"] and "fail?" not in entry["flags"]
                    )
                )
                maps.add(owners)
            agreed = (
                not unanswered
                and len(maps) == 1
                and all(view["state"] == "ok" for view in views.values())
                and not split_primaries(views)
            )
            return agreed, (views, unanswered)

        agreed, (views, unanswered) = poll_until(check, timeout_s=timeout, interval_s=0.2, context=context)
        return views, agreed, unanswered


_ELECTION_DECLARES = Declarations(
    events={
        "{id}.election-started": ConfigSchema({"shard": INTEGER, "trigger": TEXT, "old_primary": TEXT}),
        "{id}.election-completed": ConfigSchema(
            {"shard": INTEGER, "old_primary": TEXT, "winner": nullable(str), "config_epoch": nullable(int)}
        ),
    },
    evidence={
        "valkey.cluster.election.result": declared(
            mapping_of(
                {
                    "shard": INTEGER,
                    "trigger": TEXT,
                    "old_primary": TEXT,
                    "old_config_epoch": INTEGER,
                    "candidates": LIST,
                    "before": MAPPING,
                    "attempts": LIST,
                    "after": MAPPING,
                    "winner": nullable(str),
                    "winner_config_epoch": nullable(int),
                },
                # When the trigger acted, and what became of the old primary.
                additional=ANY,
            )
        )
    },
    changes=("process.stopped", "process.paused"),
)
_TRACE_DECLARES = Declarations(
    evidence={
        TRACE_EVIDENCE: declared(
            mapping_of({"path": TEXT, "events": INTEGER, "observations": INTEGER, "rounds": INTEGER})
        )
    }
)
_SAFETY_DECLARES = Declarations(
    evidence={
        "valkey.cluster.election-safety.verified": declared(
            mapping_of(
                {
                    "save_behavior": MAPPING,
                    "require_durable_votes": TEXT,
                    "votes": INTEGER,
                    "acks": INTEGER,
                    "elections_won": LIST,
                    "wins": LIST,
                    "crashes": LIST,
                    "unsaved_votes_at_crash": LIST,
                    "unsaved_votes_delivered": LIST,
                    "converged": FLAG,
                    "final_epochs": MAPPING,
                    "cross_shard_duplicate_config_epochs": LIST,
                    "trace": nullable(str),
                }
            )
        ),
        "valkey.cluster.vote.unsaved-at-crash": declared(mapping_of({"votes": LIST})),
    }
)

REGISTRATIONS = (
    component_registration(
        "valkey.action.cluster-election",
        ElectionAction,
        _ELECTION_FIELDS,
        check=_validate_election,
        deployment_modes=("cluster",),
        declares=_ELECTION_DECLARES,
    ),
    component_registration(
        "valkey.observer.cluster-trace",
        ClusterTraceObserver,
        _TRACE_FIELDS,
        check=_validate_trace,
        selectors={"nodes": ()},
        deployment_modes=("cluster",),
        declares=_TRACE_DECLARES,
    ),
    component_registration(
        "valkey.oracle.cluster-election-safety",
        ElectionSafetyOracle,
        _SAFETY_FIELDS,
        check=_validate_safety,
        deployment_modes=("cluster",),
        declares=_SAFETY_DECLARES,
    ),
)
