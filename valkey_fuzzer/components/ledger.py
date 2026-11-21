"""The workload ledger model and the ``valkey.oracle.ledger`` durability/consistency oracle.

The ``valkey.workload.key-value`` ledger records what clients observed for
every operation. ``LedgerModel`` turns it into the set of acceptable values per
key under an explicit loss policy, and ``valkey.oracle.ledger`` checks the live
dataset (and every replica) against it.
"""

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any, Optional

from valkey.crc import key_slot

from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.deployment.local.cluster_nodes import parse_cluster_nodes, parse_slot_ranges
from valkey_fuzzer.deployment.local.queries import read_keys
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.results import ComponentStatus, Evidence, evidence_of, make_evidence, make_result
from valkey_fuzzer.schema import ConfigError, ConfigField, plain_config

from .faults import FAULT_INJECTED
from .files import read_jsonl
from .instances import (
    close_quietly,
    deployment,
    instance_at,
    is_cluster,
    stopped_on_purpose,
    unreachable_instances,
)
from .polling import poll_each, read_retrying
from .registration import component_registration
from .schemas import INTEGER, LIST, MAPPING, TEXT, declared, mapping_of, nullable, optional
from .workloads import LEDGER_EVIDENCE, is_stable_key

_ABSENT = None

# With appendfsync everysec, while a background fsync runs Valkey postpones
# writing the AOF buffer for up to two seconds and still replies to clients
# (flushAppendOnlyFile), so a crash can lose writes it acknowledged up to two
# seconds, plus an event-loop pass, before it. A crash window shorter than
# this would fail runs where Valkey is right.
EVERYSEC_LOSS_WINDOW_S = 2.5


class LedgerModel:
    """Acceptable values per key, derived from what clients observed.

    One writer thread issues every operation, so each key has a total order
    of operations. Walking it, ``possible[key]`` is the set of values the key
    may hold at that point:

    * an acknowledged write replaces the set with its value, unless it falls
      in a loss window, where it may still be lost and only adds its value
      (``loss_windows`` are ``(start, end)`` or ``(start, end, instances)``:
      the latter covers only writes those instances acknowledged); the
      preload is acknowledged writes too, at the times and by the instances
      in ``preload_acks``, so a preloaded key acknowledged in a window may be
      absent;
    * an acknowledged write whose ``WAIT`` reply counted ``wait_confirmed``
      replicas or more is never lost, inside a loss window or not;
    * an ambiguous write adds its value (it may have been applied);
    * a rejected write changes nothing;
    * an acknowledged read must return a value in the set, and then narrows
      the set to that value: a later state cannot go back to a value the read
      proved was already overwritten. The narrowing keeps the values of
      ambiguous writes since the last acknowledged write, which may still be
      applied after the read, and is skipped while the key has a write that
      may yet be lost (a loss can revert the key to any earlier possible
      value, and may materialize after the read).

    Writes keep their order, and a read may overtake an ambiguous write:
    the writer closes a connection after any error before it opens the next,
    so the server reads an ambiguous write's command before any later one,
    and a CLIENT PAUSE WRITE queues writes in order while it serves reads.
    So an acknowledged write ends what the earlier ambiguous ones may do,
    and a read does not.
    """

    def __init__(
        self,
        preload: dict[str, str],
        ledger: list[dict[str, Any]],
        loss_windows: Sequence[tuple],
        lossy_key: Optional[Any] = None,
        window_reads: str = "strict",
        history: bool = False,
        wait_confirmed: Optional[int] = None,
        preload_acks: Optional[Mapping[str, tuple]] = None,
    ):
        self.preload = preload
        self.wait_confirmed = wait_confirmed
        self.unchecked_reads = 0
        self.possible: dict[str, set] = {key: {value} for key, value in preload.items()}
        self.ever: dict[str, set] = {key: {value} for key, value in preload.items()}
        self.read_violations: list[dict[str, Any]] = []
        self.loss_permitted_ops = 0
        # Keys with a write since their last acknowledged write that may be lost.
        unsettled: set = set()
        for key, (acknowledged_at, instance_id) in (preload_acks or {}).items():
            if key in preload and _lossy(key, acknowledged_at, instance_id, loss_windows, lossy_key):
                self.possible[key].add(_ABSENT)
                self.ever[key].add(_ABSENT)
                unsettled.add(key)
                self.loss_permitted_ops += 1
        # Writes inside a loss window that WAIT confirmed, so they must survive.
        self.wait_protected_ops = 0
        # With ``history``: per key, ``(op_id, values)`` each time its possible
        # values changed, starting with ``(0, initial values)``; the values
        # hold from that operation until the next entry.
        self.history: Optional[dict[str, list]] = {} if history else None
        # Values of ambiguous writes since the key's last acknowledged write.
        pending: dict[str, set] = {}
        for entry in ledger:
            key = entry["key"]
            if self.history is not None and key not in self.history:
                self.history[key] = [(0, frozenset(self.possible.get(key, {_ABSENT})))]
            self._step(entry, pending, unsettled, loss_windows, lossy_key, window_reads)
            if self.history is not None and self.history[key][-1][1] != self.possible[key]:
                self.history[key].append((entry["id"], frozenset(self.possible[key])))

    def _step(
        self,
        entry: dict[str, Any],
        pending: dict[str, set],
        unsettled: set,
        loss_windows: Sequence[tuple],
        lossy_key: Optional[Any],
        window_reads: str,
    ) -> None:
        key = entry["key"]
        possible = self.possible.setdefault(key, {_ABSENT})
        ever = self.ever.setdefault(key, {_ABSENT} if key not in self.preload else set())
        outcome = entry["outcome"]
        op = entry["op"]
        in_scope = lossy_key is None or lossy_key(key)
        in_window = in_scope and any(window[0] < entry["t_end"] <= window[1] for window in loss_windows)
        lossy = outcome == "ok" and _lossy(key, entry["t_end"], entry.get("instance_id"), loss_windows, lossy_key)
        needed = wait_needed(self.wait_confirmed, entry.get("instance_id"))
        if lossy and needed is not None and entry.get("wait", -1) >= needed:
            lossy = False
            self.wait_protected_ops += 1
        if lossy and op != "get":
            self.loss_permitted_ops += 1
        if op == "get":
            if outcome != "ok":
                return
            reply = entry.get("reply")
            if in_window and window_reads == "written":
                # Under a partition a read may be served by an instance that is
                # about to lose its writes (so no single order of operations
                # explains every reply), so only require a value that was
                # actually written to this key.
                self.unchecked_reads += 1
                if reply not in ever:
                    self.read_violations.append(
                        {"id": entry["id"], "key": key, "observed": reply, "acceptable": "any written value"}
                    )
            elif reply not in possible:
                self.read_violations.append(
                    {
                        "id": entry["id"],
                        "key": key,
                        "observed": reply,
                        "acceptable": sorted_values(possible),
                    }
                )
            elif key not in unsettled:
                self.possible[key] = {reply} | pending.get(key, set())
            return
        new_value = entry.get("value") if op == "set" else _ABSENT
        if op == "set":
            ever.add(new_value)
        else:
            ever.add(_ABSENT)
        if outcome == "error":
            return
        if outcome == "ok" and not lossy:
            self.possible[key] = {new_value}
            pending.pop(key, None)
            unsettled.discard(key)
            return
        possible.add(new_value)
        if outcome != "ok":
            pending.setdefault(key, set()).add(new_value)
        # An ambiguous write may be applied any time after it was sent, so
        # it may be lost if any loss window ends after that.
        if lossy or (in_scope and any(entry["t_start"] < window[1] for window in loss_windows)):
            unsettled.add(key)

    def keys(self) -> list[str]:
        return sorted(self.possible)


def _lossy(
    key: str, acknowledged_at: float, instance_id: Optional[str], loss_windows: Sequence[tuple], lossy_key: Any
) -> bool:
    """Whether a write of ``key`` acknowledged at ``acknowledged_at`` by ``instance_id`` may be lost.

    A window that names instances lets only their acknowledgements be lost; an acknowledgement whose instance
    is unknown may be one of theirs.
    """

    if lossy_key is not None and not lossy_key(key):
        return False
    return any(
        window[0] < acknowledged_at <= window[1]
        and (len(window) < 3 or window[2] is None or instance_id is None or instance_id in window[2])
        for window in loss_windows
    )


def wait_needed(wait_confirmed: Any, instance_id: Optional[str]) -> Optional[int]:
    """The WAIT reply that proves a write ``instance_id`` acknowledged reached every replica of its shard.

    ``wait_confirmed`` is one count for a deployment of one shard, or ``{instance_id: replicas of its shard}``
    for a cluster. An acknowledger nobody could attribute needs the largest count; a shard without replicas
    has no count (WAIT proves nothing there).
    """

    if not isinstance(wait_confirmed, Mapping):
        return wait_confirmed
    if instance_id is None:
        return max(wait_confirmed.values(), default=None)
    return wait_confirmed.get(instance_id)


class LossScopeError(RuntimeError):
    """The loss window's scope was requested and cannot be computed; widening it would hide a lost write."""


def sorted_values(values: set) -> list:
    return sorted(values, key=lambda item: (item is not None, str(item)))[:5]


def load_ledger(evidence: Sequence[Evidence], workload: str) -> tuple[dict, list[dict], dict, dict]:
    """The workload's preload, ledger, preload acknowledgements (key: ``(t_end, instance_id)``), and summary."""

    items = evidence_of(evidence, LEDGER_EVIDENCE, workload)
    if not items:
        raise LookupError(f"no {LEDGER_EVIDENCE} evidence from workload {workload!r}")
    summary = plain_config(items[-1].value)
    entries = read_jsonl(summary["preload_path"])
    preload = {entry["key"]: entry["value"] for entry in entries}
    acks = {entry["key"]: (entry["t_end"], entry.get("instance_id")) for entry in entries if "t_end" in entry}
    ledger = read_jsonl(summary["ledger_path"])
    return preload, ledger, acks, summary


FAILOVER_RESULT = "valkey.failover.result"
# ``{start, end, instance_id?}``: writes acknowledged inside (start, end] may
# be lost; with ``instance_id``, only writes that instance acknowledged.
LOSS_WINDOW = "valkey.data.loss-window"


def fault_records(
    evidence: Sequence[Evidence],
    loss_window_s: float,
    faults: Optional[Sequence[str]],
    after_s: float = 0.0,
    until: Sequence[str] = (),
) -> list[tuple]:
    """Loss windows ``(start, end, targets)``: ``loss_window_s`` before each fault until ``after_s`` past its end.

    A fault's end is its ``healed_at`` time when it records one (partitions),
    otherwise the moment it was applied. A forced failover counts as a fault
    from when it started until it completed. A fault window also runs at least
    until the last evidence of the ``until`` components was recorded, such as
    a poll that saw the isolated primary demoted: Valkey may lose what it
    acknowledged until then, however long after the heal that came.
    """
    settled = max((item.observed_at for item in evidence if item.source in until), default=None)
    records = []
    for item in evidence_of(evidence, FAULT_INJECTED):
        if faults is not None and item.source not in faults:
            continue
        value = plain_config(item.value)
        applied_at = value.get("applied_at")
        if not isinstance(applied_at, (int, float)):
            continue
        # A killed process can still acknowledge writes until it exits, so its
        # window runs to the confirmed exit (or the heal), not the signal.
        ended = max(
            (value.get(key) for key in ("healed_at", "exited_at") if isinstance(value.get(key), (int, float))),
            default=applied_at,
        )
        targets = value.get("targets") or ([value["target"]] if value.get("target") else [])
        end = max(applied_at, ended) + after_s
        if settled is not None and settled > applied_at:
            end = max(end, settled)
        records.append((applied_at - loss_window_s, end, list(targets)))
    # CLUSTER FAILOVER FORCE skips the handshake in which the primary stops
    # writes until its replica caught up, so the old primary's last
    # acknowledged writes may be lost. A graceful failover loses none.
    for item in evidence_of(evidence, FAILOVER_RESULT):
        if faults is not None and item.source not in faults:
            continue
        value = plain_config(item.value)
        started_at, completed_at = value.get("started_at"), value.get("completed_at")
        if not value.get("force") or not all(isinstance(at, (int, float)) for at in (started_at, completed_at)):
            continue
        targets = [value["primary_before"]] if value.get("primary_before") else []
        records.append((started_at - loss_window_s, completed_at + after_s, targets))
    for item in evidence_of(evidence, LOSS_WINDOW):
        if faults is not None and item.source not in faults:
            continue
        value = plain_config(item.value)
        start, end = value.get("start"), value.get("end")
        # A malformed window permits no loss.
        if not all(isinstance(at, (int, float)) for at in (start, end)):
            continue
        targets = [value["instance_id"]] if value.get("instance_id") else []
        records.append((start - loss_window_s, end + after_s, targets))
    return records


def loss_policy(
    config: Mapping[str, Any], evidence: Sequence[Evidence], handle: Any, summary: Mapping[str, Any]
) -> tuple[list, Optional[Any], Optional[list], Optional[int]]:
    """What ``valkey.oracle.ledger`` ``config`` lets Valkey lose: ``(windows, lossy_key, scope_slots, wait_confirmed)``.

    ``windows`` and ``lossy_key`` are :class:`LedgerModel`'s ``loss_windows``
    and ``lossy_key``, ``scope_slots`` the slot ranges ``lossy_key`` covers,
    and ``wait_confirmed`` its ``wait_confirmed``. Raises :class:`LossScopeError` when a requested scope
    cannot be computed.
    """

    lossy_key = None
    scope_slots = None
    if config["acknowledged"] == "may_be_lost":
        windows = [(float("-inf"), float("inf"))]
    else:
        records = fault_records(
            evidence,
            config["loss_window_s"],
            config["loss_window_faults"],
            config["loss_window_after_s"],
            config.get("loss_window_until") or (),
        )
        if config["loss_window_scope"] == "fault_targets":
            # Only writes the faults' targets acknowledged may be lost; a
            # fault that records no target lets no write be lost.
            windows = [(start, end, frozenset(targets)) for start, end, targets in records]
        else:
            windows = [(start, end) for start, end, _ in records]
        if config["loss_window_scope"] in ("fault_target_slots", "fault_targets"):
            scope_slots = _scope_slots(config["loss_window_scope"], handle, records)
            if scope_slots is not None:

                def lossy_key(key: str) -> bool:
                    slot = key_slot(key.encode("utf-8", errors="surrogatepass"))
                    return any(low <= slot <= high for low, high in scope_slots)

    # A write WAIT confirmed on every replica of its shard survives any promotion. WAIT counts the replicas
    # of the shard that served the write, so a cluster needs the count per shard.
    wait_confirmed = None
    if summary.get("wait_replicas"):
        replicas = Counter(instance.shard_id for instance in handle.instances if instance.role == "replica")
        if is_cluster(handle):
            wait_confirmed = {
                instance.instance_id: replicas[instance.shard_id]
                for instance in handle.instances
                if replicas[instance.shard_id]
            }
        else:
            wait_confirmed = sum(replicas.values())
    return windows, lossy_key, scope_slots, wait_confirmed


def _scope_slots(scope: str, handle: Any, records: Sequence[tuple]) -> Optional[list]:
    """The slot ranges the faults' targets own now, for a cluster; None when every key is in scope (standalone,
    or no fault names a target under ``fault_targets``, which then lets no write be lost).

    Raises :class:`LossScopeError` when ``scope`` is requested and the ranges cannot be computed.
    """

    if not is_cluster(handle):
        return None
    targets = [target for _, _, names in records for target in names]
    if scope == "fault_target_slots" and any(not names for _, _, names in records):
        raise LossScopeError("loss_window_scope fault_target_slots: a fault recorded no target to take the slots of")
    if not targets:
        return None
    return live_slot_ranges(handle, targets)


class _Reader:
    """Reads keys from an instance or through cluster routing."""

    def __init__(self, handle: Any, instance_id: Optional[str]):
        self.handle = handle
        self.cluster = is_cluster(handle)
        if instance_id is None:
            self.client = handle.cluster_client(timeout=5.0) if self.cluster else None
            if self.client is None:
                primaries = handle.primaries()
                if not primaries:
                    raise RuntimeError("no live primary to read from")
                self.client = handle.new_client(primaries[0], timeout=5.0)
        else:
            self.client = handle.new_client(instance_id, timeout=5.0)
        # A cluster replica answers for its shard's keys only after READONLY, sent once on this connection.
        self.readonly = self.cluster and instance_id is not None

    def read(self, keys: Sequence[str]) -> dict[str, Optional[str]]:
        return dict(zip(keys, read_keys(self.client, keys, readonly=self.readonly)))

    def close(self) -> None:
        close_quietly(self.client)


# ---------------------------------------------------------------------------
# valkey.oracle.ledger — durability/consistency against the workload ledger
# ---------------------------------------------------------------------------

_LEDGER_FIELDS = {
    "workload": ConfigField(str),
    "acknowledged": ConfigField(str, default="must_survive", choices=("must_survive", "may_be_lost")),
    "loss_window_s": ConfigField(float, default=0.0, minimum=0, maximum=3600),
    # Components whose faults, forced failovers, and loss-window evidence
    # open loss windows; every component by default.
    "loss_window_faults": ConfigField(list, allow_none=True, default=None),
    "loss_window_after_s": ConfigField(float, default=0.0, minimum=0, maximum=3600),
    # Components whose evidence marks when a fault's effect ended (a poll that saw the isolated primary
    # demoted): each fault window runs at least until their last evidence.
    "loss_window_until": optional(list, items=TEXT),
    "loss_window_scope": ConfigField(str, default="all", choices=("all", "fault_target_slots", "fault_targets")),
    "lost_writes_budget": ConfigField(int, default=0, minimum=0),
    "check_reads": ConfigField(bool, default=True),
    "window_reads": ConfigField(str, default="strict", choices=("strict", "written")),
    "check_replicas": ConfigField(bool, default=True),
    "converge_timeout_s": ConfigField(float, default=30.0, minimum=0, maximum=600),
}


def _validate_ledger(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    components = {component.id: component for component in plan.components}
    if config["workload"] not in components:
        raise ConfigError(f"workload {config['workload']!r} is not a component of this plan")
    for fault in config["loss_window_faults"] or ():
        if fault not in components:
            raise ConfigError(f"loss_window_faults entry {fault!r} is not a component of this plan")
    for component_id in config.get("loss_window_until") or ():
        if component_id not in components:
            raise ConfigError(f"loss_window_until entry {component_id!r} is not a component of this plan")
    if config["acknowledged"] == "may_be_lost":
        return
    faults = config["loss_window_faults"]
    # A forced failover can lose acknowledged writes, so it cannot be paired
    # with a policy that forbids that loss.
    for component in plan.components:
        if component.use == "valkey.action.failover" and plain_config(component.config).get("force") is True:
            if faults is not None and component.id not in faults:
                raise ConfigError(
                    f"{component.id!r} uses CLUSTER FAILOVER FORCE, which can lose writes the old primary "
                    f"acknowledged: list it in loss_window_faults"
                )
    # A crash cannot lose a write that appendfsync always fsynced before
    # acknowledging it, as long as no replica can take over the shard.
    if faults is None:
        windowed = [component for component in plan.components if component.role is ComponentRole.FAULT]
    else:
        windowed = [components[fault] for fault in faults]
    replicas = plan.deployment["replicas_per_primary"]
    if not (
        windowed
        and all(component.use == "valkey.fault.process-signal" for component in windowed)
        and (plan.deployment_mode == "standalone" or replicas == 0)
        # Valkey's appendonly defaults to no, so only yes on every instance (set or the provider's default) counts.
        and _uniform_server_setting(plan, "appendonly") == "yes"
    ):
        return
    appendfsync = _uniform_server_setting(plan, "appendfsync")
    if appendfsync == "always" and (config["loss_window_s"] > 0 or config["loss_window_after_s"] > 0):
        raise ConfigError(
            "every instance runs appendfsync always, which fsyncs each write before acknowledging it, and the "
            "loss windows come only from process faults with no replica to take over: loss_window_s and "
            f"loss_window_after_s must be 0 (got {config['loss_window_s']} and {config['loss_window_after_s']})"
        )
    kills = any(plain_config(component.config).get("signal", "SIGKILL") == "SIGKILL" for component in windowed)
    if appendfsync == "everysec" and kills and config["loss_window_s"] < EVERYSEC_LOSS_WINDOW_S:
        raise ConfigError(
            "every instance runs appendfsync everysec, which may write (and so lose) what it acknowledged up to "
            f"two seconds before a SIGKILL: loss_window_s must be at least {EVERYSEC_LOSS_WINDOW_S} "
            f"(got {config['loss_window_s']})"
        )


def _uniform_server_setting(plan: ExecutionPlan, name: str) -> Optional[str]:
    """The effective value of server option ``name`` on every instance, lowercased; ``None`` when unset or not uniform.

    The plan's instances hold each instance's effective config, so a
    deployment provider's default counts the same as a setting written out.
    """

    values = {
        str(instance["server_config"].get(name)).lower() if name in instance["server_config"] else None
        for instance in plan.instances.values()
    }
    return values.pop() if len(values) == 1 else None


class LedgerOracle:
    """Final data equals the ledger's acceptable states; replicas converge.

    Reads are retried within ``converge_timeout_s``. One that keeps failing
    (a timeout, LOADING, CLUSTERDOWN, an unreachable replica) makes the oracle
    ERROR, never FAIL: the harness could not read, which says nothing about
    the data. Values that were read and are wrong still FAIL.
    """

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = spec.config
        handle = deployment(context)
        try:
            preload, ledger, preload_acks, summary = load_ledger(evidence, config["workload"])
        except (LookupError, OSError, ValueError) as exc:
            return make_result(spec, ComponentStatus.ERROR, f"cannot load workload ledger: {exc}")
        try:
            windows, lossy_key, scope_slots, wait_confirmed = loss_policy(config, evidence, handle, summary)
        except LossScopeError as exc:
            return make_result(spec, ComponentStatus.ERROR, f"cannot scope the loss window: {exc}")
        model = LedgerModel(
            preload,
            ledger,
            windows,
            lossy_key,
            config["window_reads"],
            wait_confirmed=wait_confirmed,
            preload_acks=preload_acks,
        )
        keys = model.keys()

        actual, error = read_retrying(
            context, lambda: _read_keys(handle, None, keys), timeout_s=config["converge_timeout_s"]
        )
        if error is not None:
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"cannot read the dataset within {config['converge_timeout_s']:g}s: {error}",
            )

        violations = []
        for key in keys:
            if actual.get(key) not in model.possible[key]:
                violations.append(
                    {
                        "key": key,
                        "observed": actual.get(key),
                        "acceptable": sorted_values(model.possible[key]),
                        "stable": is_stable_key(key),
                    }
                )
        # How many keys actually took advantage of the loss window: their value
        # is acceptable only because acknowledged writes were allowed to vanish.
        window_losses = []
        if windows:
            strict = LedgerModel(preload, ledger, [])
            window_losses = [
                key
                for key in keys
                if actual.get(key) in model.possible[key] and actual.get(key) not in strict.possible[key]
            ]
        lost = [item for item in violations if item["observed"] is None]
        corrupt = [item for item in violations if item["observed"] is not None]
        problems = []
        if corrupt:
            problems.append(f"{len(corrupt)} key(s) hold a value outside the acceptable set")
        if len(lost) > config["lost_writes_budget"]:
            problems.append(f"{len(lost)} acknowledged write(s) lost (budget {config['lost_writes_budget']})")
        if config["check_reads"] and model.read_violations:
            problems.append(f"{len(model.read_violations)} read(s) returned a value never acceptable at that time")

        notes = []
        if summary.get("wait_replicas") and not model.wait_protected_ops:
            notes.append(
                f"WAIT {summary['wait_replicas']} was configured and no write was protected by it: "
                "no write got a WAIT reply covering every replica of its shard"
            )
        replica_report: dict[str, Any] = {}
        unreadable = {}
        uncompared = False
        if config["check_replicas"]:
            stopped = stopped_on_purpose(handle, evidence)
            replica_report = self._check_replicas(context, handle, keys, actual, config["converge_timeout_s"], stopped)
            # Replicas were planned (and none stopped on purpose) but none was compared: no evidence.
            uncompared = not replica_report and any(
                instance.role == "replica" and instance.instance_id not in stopped for instance in handle.instances
            )
            diverged = {instance: info for instance, info in replica_report.items() if info.get("diverged")}
            if diverged:
                problems.append(f"replicas did not converge: {', '.join(sorted(diverged))}")
            unreadable = {instance: info["error"] for instance, info in replica_report.items() if info.get("unread")}

        record = {
            "keys_checked": len(keys),
            "operations": len(ledger),
            "acknowledged": summary.get("acknowledged"),
            "ambiguous": summary.get("ambiguous"),
            "loss_windows": [
                [window[0], window[1], *([sorted(window[2])] if len(window) > 2 else [])]
                for window in windows
                if window[0] != float("-inf")
            ],
            "loss_permitted_ops": model.loss_permitted_ops,
            "wait_confirmed_replicas": max(wait_confirmed.values(), default=None)
            if isinstance(wait_confirmed, Mapping)
            else wait_confirmed,
            "wait_protected_ops": model.wait_protected_ops,
            "loss_scope_slots": scope_slots,
            "notes": notes,
            "unchecked_reads": model.unchecked_reads,
            "window_losses": len(window_losses),
            "window_loss_examples": window_losses[:10],
            "violations": violations[:20],
            "read_violations": model.read_violations[:20],
            "replicas": replica_report,
        }
        if problems:
            return make_result(spec, ComponentStatus.FAIL, "; ".join(problems), diagnostics=record)
        if unreadable:
            return make_result(
                spec,
                ComponentStatus.ERROR,
                "cannot read replica(s) "
                + "; ".join(f"{instance}: {error}" for instance, error in sorted(unreadable.items())),
                diagnostics=record,
            )
        if uncompared:
            return make_result(
                spec,
                ComponentStatus.ERROR,
                "check_replicas is set and replicas are planned, but no replica was compared",
                diagnostics=record,
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{len(keys)} keys match the ledger ({len(ledger)} operations)",
            evidence=(make_evidence(spec, "valkey.data.ledger-verified", "oracle.completed", record),),
        )

    @staticmethod
    def _check_replicas(
        context: Any, handle: Any, keys: list, primary_values: dict, timeout_s: float, stopped: set[str]
    ) -> dict:
        """Each replica's report: ``diverged`` when it was read and its values differ from the primary's, and
        ``unread`` when its last read (or its role) failed, which proves nothing either way. Instances
        ``stopped`` on purpose hold no data to check."""

        report: dict[str, Any] = {}
        replicas = handle.replicas()
        for instance_id in unreachable_instances(handle, stopped):
            report[instance_id] = {"keys": 0, "diverged": False, "unread": True, "mismatched": [], "error": "instance is unreachable"}  # fmt: skip
        cluster = is_cluster(handle)

        def converged_on(instance_id: str) -> tuple[bool, tuple[list, Optional[str]]]:
            try:
                expected = _keys_owned_by_primary_of(handle, instance_id, keys) if cluster else keys
                values = _read_keys(handle, instance_id, expected)
            except Exception as exc:
                return False, (None, f"{type(exc).__name__}: {exc}")
            mismatched = [key for key in expected if values.get(key) != primary_values.get(key)]
            return not mismatched, (mismatched, len(expected))

        polled = poll_each(context, replicas, converged_on, timeout_s=timeout_s, interval_s=0.2)
        for instance_id, (converged, (mismatched, detail)) in polled.items():
            unread = mismatched is None
            report[instance_id] = {
                "keys": 0 if unread else detail,
                "diverged": not converged and not unread,
                "unread": unread,
                "mismatched": [] if unread else mismatched[:10],
                "error": detail if unread else None,
            }
        return report


def _read_keys(handle: Any, instance_id: Optional[str], keys: Sequence[str]) -> dict[str, Optional[str]]:
    """``keys``' values on ``instance_id``, or through cluster routing (a primary in standalone) when None."""

    reader = None
    try:
        reader = _Reader(handle, instance_id)
        return reader.read(keys)
    finally:
        if reader is not None:
            reader.close()


def target_slot_ranges(handle: Any, targets: Sequence[str]) -> Optional[list]:
    """Planned slot ranges of the shards containing ``targets`` (cluster only)."""

    if not is_cluster(handle):
        return None
    shards = {handle.instance(target).shard_id for target in targets if target}
    ranges = []
    for instance in handle.instances:
        if instance.shard_id in shards and instance.slot_start is not None:
            ranges.append((instance.slot_start, instance.slot_end))
    # Faults that record no targets cannot be scoped; fall back to every key
    # rather than an empty scope that would forbid all loss.
    return sorted(set(ranges)) or None


def live_slot_ranges(handle: Any, targets: Sequence[str]) -> list:
    """The slot ranges the shards of ``targets`` own now, read from the first answering instance's CLUSTER NODES.

    A target is found by its address, so a node that was demoted or failed since the plan still names its shard.
    Raises :class:`LossScopeError` when no instance answers, a target is not in CLUSTER NODES, or its shard
    owns no slots.
    """

    nodes, errors = None, []
    for instance in handle.instances:
        try:
            with handle.new_client(instance.instance_id, timeout=5.0) as client:
                nodes = parse_cluster_nodes(client.execute_command("CLUSTER", "NODES"))
        except Exception as exc:
            errors.append(f"{instance.instance_id}: {type(exc).__name__}: {exc}")
            continue
        if nodes:
            break
    if not nodes:
        raise LossScopeError(f"cannot read CLUSTER NODES to find the slots of {sorted(set(targets))}: {errors[:3]}")
    ranges = set()
    for target in sorted(set(targets)):
        row = next((node for node in nodes if instance_at(handle, node["host"], node["port"]) == target), None)
        if row is None:
            raise LossScopeError(f"fault target {target} is not in CLUSTER NODES")
        primary_id = row["node_id"] if row["is_primary"] else row["primary_id"]
        owned = next((parse_slot_ranges(node["slots"]) for node in nodes if node["node_id"] == primary_id), [])
        if not owned:
            raise LossScopeError(f"the shard of fault target {target} owns no slots in CLUSTER NODES")
        ranges.update(owned)
    return sorted(ranges)


def _keys_owned_by_primary_of(handle: Any, replica_id: str, keys: list) -> list:
    """The ``keys`` whose slots ``replica_id``'s CLUSTER NODES gives its primary (itself, if it leads).

    Raises when the node, its primary, or the primary's slots are missing while there are ``keys``: nothing
    would be compared.
    """

    with handle.new_client(replica_id, timeout=5.0) as client:
        nodes = parse_cluster_nodes(client.execute_command("CLUSTER", "NODES"))
    myself = next((node for node in nodes if node["is_myself"]), None)
    primary_id = None if myself is None else myself["node_id"] if myself["is_primary"] else myself["primary_id"]
    ranges = next((parse_slot_ranges(node["slots"]) for node in nodes if node["node_id"] == primary_id), [])
    if keys and not ranges:
        raise LookupError(f"CLUSTER NODES of {replica_id} gives its primary no slots (primary {primary_id})")
    return [
        key
        for key in keys
        if any(low <= key_slot(key.encode("utf-8", errors="surrogatepass")) <= high for low, high in ranges)
    ]


_LEDGER_DECLARES = Declarations(
    evidence={
        "valkey.data.ledger-verified": declared(
            mapping_of(
                {
                    "keys_checked": INTEGER,
                    "operations": INTEGER,
                    "acknowledged": nullable(int),
                    "ambiguous": nullable(int),
                    "loss_windows": LIST,
                    "loss_permitted_ops": INTEGER,
                    "wait_confirmed_replicas": nullable(int),
                    "wait_protected_ops": INTEGER,
                    # Inclusive (low, high) hash slot ranges, or null when every key may be lost.
                    "loss_scope_slots": ConfigField(list, allow_none=True, items=ConfigField(list, items=INTEGER)),
                    "notes": LIST,
                    "unchecked_reads": INTEGER,
                    "window_losses": INTEGER,
                    "window_loss_examples": LIST,
                    "violations": LIST,
                    "read_violations": LIST,
                    "replicas": MAPPING,
                }
            )
        )
    }
)

REGISTRATIONS = (
    component_registration(
        "valkey.oracle.ledger", LedgerOracle, _LEDGER_FIELDS, check=_validate_ledger, declares=_LEDGER_DECLARES
    ),
)
