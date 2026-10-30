"""The wave engine of ``valkey.action.cluster-chaos`` and ``valkey.action.replication-chaos``.

Both actions run ``waves``: each wave is a list of operations that start
together (each after its ``delay_s``, or after another operation of the wave
finished), then a heal, then validation (:meth:`WaveRun.run_waves`). The
building blocks are the :data:`OPERATIONS`, each run by a ``_<name>`` method;
the policies are per mode, in a :class:`WaveRun` subclass
(``chaos.ClusterWaves``, ``replication_chaos.StandaloneWaves``): the live
members targets resolve against, the heal, the validation, and which faults
excuse a failed topology change. Every operation draws from its own random
stream, ``wave-<n>-op-<i>``: its target before the wave, then whatever it
picks while it runs. Every change the engine will undo is an undo obligation
of the run (:meth:`WaveRun.own`), the one record of what is still in place.
See docs/architecture.md, "The wave engine".
"""

import contextvars
import functools
import signal
import threading
import time
import uuid
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from valkey_fuzzer.extensions import (
    STATE_VALIDATION_SCHEMA,
    ComponentStatus,
    ConfigError,
    ConfigField,
    ConfigSchema,
    Declarations,
    chain_name,
    make_evidence,
    make_result,
    plain_config,
)

from .common import (
    ANY,
    FLAG,
    INSTANCE_ID,
    INTEGER,
    LIST,
    MAPPING,
    NUMBER,
    TEXT,
    component_registration,
    declared,
    deployment,
    mapping_of,
    nullable,
    optional,
    pause_owned,
)
from .ledger import LOSS_WINDOW
from .network import create_chain, drop_between, remove_partition

SIGNALS = {"SIGKILL": signal.SIGKILL, "SIGTERM": signal.SIGTERM}
CLUSTER, STANDALONE = "cluster", "standalone"
ROLE_TARGETS = ("primary", "replica", "any")
# How long a kill waits for its target to exit.
EXIT_TIMEOUT_S = 30.0
# The order a heal or the final cleanup undoes changes in: resume stalled processes first (a config restore on
# a stopped one would hang), restart killed instances last.
UNDO_ORDER = {
    "process.paused": 0,
    "network.partition": 1,
    "server.config": 2,
    "server.client-pause": 3,
    "process.stopped": 4,
}

_DURATION = ConfigField(float, default=1.0, minimum=0, maximum=600)
_TARGET = {"target": ConfigField(str, default="replica")}
# Every operation's fields: when it starts, after its delay_s, or after the operation at position
# ``after`` of its wave finished, then its delay_s.
_COMMON = {
    "delay_s": ConfigField(float, default=0.0, minimum=0, maximum=600),
    "after": ConfigField(int, allow_none=True, default=None, minimum=0),
}


@dataclass(frozen=True)
class Operation:
    """What one building block of a wave supports and does."""

    modes: tuple[str, ...]
    # Target kinds it accepts besides an instance ID; () when it takes none.
    targets: tuple[str, ...]
    # Its own config fields, besides the timing every operation has.
    fields: Mapping[str, ConfigField] = field(default_factory=dict)
    # The role its target must have (resolved against the wave's start).
    role: Optional[str] = None
    # Its target's process goes away: kill safety applies.
    removes: bool = False
    # Later picks of the wave avoid its target.
    disrupts: bool = False
    # Held for duration_s, then undone (at the latest by the heal).
    held: bool = False
    # Changes who replicates whom: only a fault that interferes excuses its failure.
    topology: bool = False
    # Durability: it may lose writes the old primary acknowledged, and only then opens a loss window.
    loss_window: bool = False
    # The state it changes on each target (its conflict keys). A held fault claims these from before it
    # reads them until its undo succeeds, and a kill while it signals and waits for the exit: another
    # of the wave on the same state waits, so no undo restores a value an overlapping fault saved or
    # meets a process that is going away, or is skipped if the holder's undo failed. A config change
    # also claims run-state, because its undo needs the process running. ``shard`` is the target's
    # live shard rather than the target: a cluster failover claims its shard until it settled, and a
    # kill the shard its target serves, so a kill is confirmed against the roles a failover of that
    # shard left, never against ones it is changing. Standalone topology changes take the run's
    # topology lock instead, one at a time. A partition claims nothing: its chain is its own.
    mutates: tuple[str, ...] = ()


OPERATIONS = {
    # A killed primary's replacement may lack writes it acknowledged. Its target may also be a list of
    # instance IDs: one of them.
    "kill": Operation(
        (CLUSTER, STANDALONE),
        ROLE_TARGETS,
        {
            "target": ConfigField((str, list), default="replica"),
            "signal": ConfigField(str, default="SIGKILL", choices=tuple(SIGNALS)),
        },
        removes=True,
        disrupts=True,
        loss_window=True,
        mutates=("process", "run-state", "shard"),
    ),
    # A FORCE failover does not wait for the replica to catch up.
    "failover": Operation(
        (CLUSTER,),
        (),
        {
            "shard": ConfigField(int, minimum=0, maximum=15),
            "force": ConfigField(bool, default=False),
            "timeout_s": ConfigField(float, default=30.0, minimum=0.1, maximum=3600),
        },
        topology=True,
        loss_window=True,
        mutates=("shard",),
    ),
    "restart": Operation(
        (STANDALONE,),
        ("killed",),
        {"target": ConfigField(str, default="killed")},
        topology=True,
        mutates=("process", "replication"),
    ),
    # Only a forced promotion loses writes: a graceful one waits for the replica to catch up.
    "promote": Operation(
        (STANDALONE,),
        ("replica",),
        {
            **_TARGET,
            "mode": ConfigField(str, default="graceful", choices=("graceful", "force")),
            "split_s": ConfigField(float, default=0.0, minimum=0, maximum=60),
            "timeout_s": ConfigField(float, default=10.0, minimum=0.1, maximum=600),
        },
        role="replica",
        topology=True,
        loss_window=True,
        mutates=("replication",),
    ),
    "repoint": Operation(
        (STANDALONE,),
        ("replica",),
        {**_TARGET, "to": ConfigField(str, default="replica")},
        role="replica",
        topology=True,
        mutates=("replication",),
    ),
    "partition": Operation((STANDALONE,), ROLE_TARGETS, {**_TARGET, "duration_s": _DURATION}, disrupts=True, held=True),
    "stall": Operation(
        (STANDALONE,),
        ROLE_TARGETS,
        {**_TARGET, "duration_s": _DURATION},
        disrupts=True,
        held=True,
        mutates=("run-state",),
    ),
    "backlog_pressure": Operation(
        (STANDALONE,),
        (),
        {
            "repl_backlog_size": ConfigField(int, default=16384, minimum=1),
            "output_limit": ConfigField(int, default=65536, minimum=1),
            "duration_s": _DURATION,
        },
        held=True,
        mutates=("config:repl-backlog-size", "config:client-output-buffer-limit", "run-state"),
    ),
    "compression": Operation(
        (STANDALONE,),
        (*ROLE_TARGETS, "all"),
        {
            "target": ConfigField(str, default="primary"),
            "value": ConfigField(str, default="lz4", choices=("no", "yes", "lz4", "zstd")),
            "duration_s": _DURATION,
        },
        held=True,
        mutates=("config:repl-compression", "run-state"),
    ),
}


def operations_for(mode: str) -> tuple[str, ...]:
    return tuple(name for name, operation in OPERATIONS.items() if mode in operation.modes)


def operation_field(mode: str) -> ConfigField:
    """One wave operation of ``mode``: a union tagged by ``op``, each kind with its timing and own fields."""

    return ConfigField(
        dict,
        variants=(
            "op",
            {
                kind: ConfigSchema({"op": ConfigField(str, choices=(kind,)), **_COMMON, **OPERATIONS[kind].fields})
                for kind in operations_for(mode)
            },
        ),
    )


def waves_field(mode: str, **options: Any) -> ConfigField:
    """The ``waves`` of a ``mode`` action: lists of :func:`operation_field` items."""

    return ConfigField(list, items=ConfigField(list, items=operation_field(mode)), **options)


def resolve_waves(
    waves: Sequence[Sequence[Mapping[str, Any]]], mode: str, where: str = "waves"
) -> list[list[dict[str, Any]]]:
    """``waves`` checked against :func:`waves_field`, with their defaults: no wave is empty, and ``after``
    names an earlier operation of its wave."""

    if not waves:
        raise ConfigError(f"{where} must list at least one wave")
    for number, wave in enumerate(waves):
        if not wave:
            raise ConfigError(f"{where}[{number}] has no operations")
    resolved = waves_field(mode).resolve(waves, where)
    for number, wave in enumerate(resolved):
        for index, operation in enumerate(wave):
            if operation["after"] is not None and operation["after"] >= index:
                raise ConfigError(f"{where}[{number}][{index}].after must name an earlier operation of the wave")
    return resolved


def check_target(value: str, allowed: Sequence[str], instances: int, where: str) -> None:
    """``value`` is one of ``allowed`` or a planned instance ID (below ``instances``)."""

    if value in allowed:
        return
    if not INSTANCE_ID.fullmatch(value):
        raise ConfigError(f"{where} must be one of {list(allowed)} or an instance ID such as valkey-1, got {value!r}")
    if int(value.split("-")[1]) >= instances:
        raise ConfigError(f"{where}: {value!r} is not a planned instance ID (valkey-0 to valkey-{instances - 1})")


def check_waves(waves: Sequence[Sequence[Mapping[str, Any]]], mode: str, document: Mapping[str, Any]) -> None:
    """``waves`` for a ``mode`` deployment shaped as ``document`` (a plan's deployment, with its defaults) says."""

    waves = resolve_waves(waves, mode)
    replicas = document["replicas_per_primary"]
    primaries = document["primaries"] if mode == CLUSTER else 1
    if replicas < 1:
        raise ConfigError("wave chaos needs replicas_per_primary >= 1")
    instances = primaries * (1 + replicas)
    for number, wave in enumerate(waves):
        failovers: set[int] = set()
        for index, operation in enumerate(wave):
            where = f"waves[{number}][{index}]"
            kind = operation["op"]
            if OPERATIONS[kind].targets:
                targets = operation["target"] if isinstance(operation["target"], list) else [operation["target"]]
                if not targets:
                    raise ConfigError(f"{where}.target names no instance")
                for target in targets:
                    allowed = OPERATIONS[kind].targets if len(targets) == 1 else ()
                    check_target(target, allowed, instances, f"{where}.target")
            if kind == "failover":
                if operation["shard"] >= primaries:
                    raise ConfigError(f"{where}.shard {operation['shard']} is not below primaries {primaries}")
                if operation["shard"] in failovers:
                    raise ConfigError(f"{where}: shard {operation['shard']} already fails over in this wave")
                failovers.add(operation["shard"])
            elif kind == "restart" and not any(
                other["op"] == "kill" and other["delay_s"] < operation["delay_s"] for other in wave
            ):
                raise ConfigError(f"{where}: a restart needs a kill with a smaller delay_s in the same wave")
            elif kind == "repoint":
                check_target(operation["to"], ("primary", "replica"), instances, f"{where}.to")
                if INSTANCE_ID.fullmatch(operation["to"]) and operation["to"] == operation["target"]:
                    raise ConfigError(f"{where}: a replica cannot replicate itself")
                if operation["to"] == "replica" and replicas < 2:
                    raise ConfigError(f"{where}: repointing to another replica needs replicas_per_primary >= 2")
            elif kind == "partition" and document.get("addressing") != "per_instance_loopback":
                raise ConfigError(f"{where}: a partition needs deployment.addressing: per_instance_loopback")


# ---------------------------------------------------------------------------
# Targets and kill safety
# ---------------------------------------------------------------------------


class TargetMissing(str):
    """Why a planned fault has no target at all, as opposed to a target the safety rules refuse."""


@dataclass(frozen=True)
class Member:
    """A live instance: its role (None when it does not answer) and live shard (None: it holds no slots)."""

    instance_id: str
    role: Optional[str]
    shard: Optional[int]


class TargetSelector:
    """Resolves targets against the live members and keeps kills safe.

    A kill never takes a shard's last live member: shards are the live ones
    (a replica belongs to its primary's shard), so a replica that migrated
    counts where it replicates now. With ``quorum`` it also keeps a
    majority of the planned shards' primaries live; a dead primary still
    counts toward the failover majority until a replica replaces it, so the
    majority is of the planned shards, and only slot owners count as live.

    ``begin_wave`` takes the members at the start of a wave and forgets the
    previous wave's picks; ``select`` resolves one operation's target in
    plan order and reserves a kill target, so two kills of a wave cannot
    both take a shard's last member; ``confirm`` checks a kill again right
    before the signal (with a fresh view, when given, because failovers
    change roles in the meantime); ``land`` marks a target down,
    ``release`` forgets a pick whose kill did not happen, and ``restore`` a
    down instance that runs again.
    """

    def __init__(self, members: Sequence[Member], *, quorum: bool):
        shards = {member.shard for member in members}
        self._quorum = len(shards) // 2 + 1 if quorum else None
        self._members = list(members)
        self._reserved: set[str] = set()
        self._down: set[str] = set()
        self._disrupted: set[str] = set()
        self._lock = threading.Lock()

    @property
    def down(self) -> frozenset:
        with self._lock:
            return frozenset(self._down)

    def begin_wave(self, members: Sequence[Member]) -> None:
        with self._lock:
            self._members = list(members)
            self._reserved = set()
            self._disrupted = set()

    def land(self, instance_id: str) -> None:
        with self._lock:
            self._reserved.discard(instance_id)
            self._down.add(instance_id)

    def release(self, instance_id: str) -> None:
        with self._lock:
            self._reserved.discard(instance_id)

    def restore(self, instance_id: str) -> None:
        with self._lock:
            self._down.discard(instance_id)

    def _candidates(self, target: Any) -> list[Member]:
        """Members ``target`` names, not down, in instance ID order: a role, ``any``, or instance IDs."""

        members = sorted(
            (member for member in self._members if member.instance_id not in self._down),
            key=lambda member: member.instance_id,
        )
        if target == "any":
            return [member for member in members if member.role is not None]
        if target in ("primary", "replica"):
            return [member for member in members if member.role == target]
        wanted = {target} if isinstance(target, str) else set(target)
        return [member for member in members if member.instance_id in wanted]

    def _safe(self, candidates: list[Member], stopped: set[str]) -> tuple[list[Member], Optional[str]]:
        alive = [member for member in self._members if member.instance_id not in stopped | self._down]
        shard_safe = [
            candidate
            for candidate in candidates
            if candidate.shard is None
            or any(member.shard == candidate.shard and member.instance_id != candidate.instance_id for member in alive)
        ]
        if not shard_safe:
            return [], "shard-safe"
        if self._quorum is None:
            return shard_safe, None
        voters = {member.instance_id for member in alive if member.role == "primary" and member.shard is not None}
        quorum_safe = [
            candidate
            for candidate in shard_safe
            if candidate.role != "primary" or len(voters - {candidate.instance_id}) >= self._quorum
        ]
        if not quorum_safe:
            return [], "quorum-safe"
        return quorum_safe, None

    @staticmethod
    def _described(target: Any) -> str:
        if target == "any":
            return "instance"
        return target if isinstance(target, str) else " or ".join(target)

    def select(self, kind: str, target: Any, rng: Any) -> tuple[Optional[Member], Optional[str]]:
        """``(member, None)`` for operation ``kind`` on ``target``, or ``(None, reason)``.

        A :class:`TargetMissing` reason means no target at all; any other
        means the safety rules refused every candidate.
        """

        operation = OPERATIONS[kind]
        with self._lock:
            candidates = self._candidates(target)
            if operation.role is not None:
                candidates = [member for member in candidates if member.role == operation.role]
            if not candidates:
                return None, TargetMissing(f"no live {self._described(target)} to {kind}")
            if operation.removes:
                candidates = [member for member in candidates if member.instance_id not in self._reserved]
                if not candidates:
                    return None, TargetMissing(f"every live {self._described(target)} is already killed")
                candidates, reason = self._safe(candidates, self._reserved)
                if not candidates:
                    return None, f"no {reason} target"
                choice = rng.choice(candidates)
                self._reserved.add(choice.instance_id)
            elif operation.role is not None:
                # Prefer a member this wave leaves alone; one it disrupts is
                # still picked, and the operation is then refused and excused.
                choice = rng.choice(
                    [member for member in candidates if member.instance_id not in self._disrupted] or candidates
                )
            else:
                choice = rng.choice(candidates)
            if operation.disrupts:
                self._disrupted.add(choice.instance_id)
            return choice, None

    def select_all(self) -> list[str]:
        """Every instance that is not down, for an operation on ``all``."""

        with self._lock:
            return sorted(member.instance_id for member in self._members if member.instance_id not in self._down)

    def select_other(self, target: str, other_than: str, rng: Any) -> tuple[Optional[str], Optional[str]]:
        """A live ``target`` (a role or an ID) other than ``other_than``, e.g. where a repoint sends a replica."""

        with self._lock:
            candidates = [member for member in self._candidates(target) if member.instance_id != other_than]
        if not candidates:
            return None, TargetMissing(f"no live {target} other than {other_than} to repoint it to")
        return rng.choice(candidates).instance_id, None

    def confirm(self, instance_id: str, members: Optional[Sequence[Member]] = None) -> Optional[str]:
        """``None`` if killing a picked target is still safe, else why not (``members``: a fresh view)."""

        with self._lock:
            if members is not None:
                self._members = list(members)
            if instance_id in self._down:
                return TargetMissing(f"{instance_id} is already down")
            current = next((member for member in self._members if member.instance_id == instance_id), None)
            if current is None:
                # Already out of the live view: killing it takes nothing more away.
                return None
            _, reason = self._safe([current], self._reserved - {instance_id})
            return f"no longer {reason}" if reason else None


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------


class WaveRun:
    """One action's waves: plan, run, heal, validate, and judge each; the rest of the contract is shared.

    A mode's subclass sets ``mode``, ``quorum`` (kills also keep a majority of
    the planned shards' primaries live: cluster mode; a standalone deployment
    is one shard, so one instance always stays up), and ``halt_on_failure``
    (stop after any failed wave, when the next wave's heal builds on this one),
    and implements ``prepare()`` (what the run needs before its test keys),
    ``outcome(records)`` (its own summary text and schedule fields),
    ``members()`` (the live view targets resolve against),
    ``kill_view()`` (a fresh view to confirm a kill against, or None),
    ``before_kill(instance_id)`` (what to record before an instance goes),
    ``role_of(instance_id, members)`` (the role a kill records), ``heal``,
    ``validate``, ``excusing(record, results)`` (the faults that excuse a
    failed topology change), ``wave_summary`` (its own wave fields), and an
    ``_<name>`` method per operation of its mode.
    """

    mode: str
    quorum: bool
    halt_on_failure: bool

    def __init__(self, context: Any, spec: Any, handle: Any, state: Any):
        self.context = context
        self.spec = spec
        self.handle = handle
        self.state = state
        self.ids = [instance.instance_id for instance in handle.instances]
        self.lock = threading.RLock()
        planned = [Member(node.instance_id, node.role, node.shard_id) for node in handle.instances]
        self.selector = TargetSelector(planned, quorum=self.quorum)
        # Every operation's random stream, by record index.
        self.rngs: dict[str, Any] = {}
        # Each record's completion, for operations that start ``after`` another.
        self.done: dict[str, threading.Event] = {}
        # Every kill sent (or tried), in order, and the killed instances that exited, until restarted.
        self.kills: list[dict[str, Any]] = []
        self.killed: dict[str, dict[str, Any]] = {}
        # Every change the run applied, as the undo obligation it opened, with the record of the operation
        # that owns it (None: the wave's own, such as the validation's CLIENT PAUSE). The one record of what
        # is still in place: a change is active until its obligation is discharged.
        self.owned: list[tuple[Any, Optional[dict[str, Any]]]] = []
        # Conflict key -> the record of the held fault that claimed it.
        self.claims: dict[str, dict[str, Any]] = {}
        self.claimed = threading.Condition(self.lock)
        self.failures: list[str] = []
        self.errors: list[str] = []

    @classmethod
    def execute(cls, context: Any, spec: Any) -> Any:
        """Run the action ``spec``: resolve its waves, start the run, run every wave, and finish.

        A validation config the deployment rejects, a host that cannot run
        the waves (:meth:`host_ready`), or a run that cannot start (no test
        keys, no single primary) is an ``ERROR``.
        """

        config = spec.config
        waves = resolve_waves(config["waves"], cls.mode)
        handle = deployment(context)
        reason = cls.host_ready(waves)
        if reason is not None:
            return make_result(spec, ComponentStatus.ERROR, reason)
        try:
            state = handle.state_check(config["validation"], seed=context.plan.seed)
        except ValueError as exc:
            return make_result(spec, ComponentStatus.ERROR, f"invalid validation config: {exc}")
        try:
            run = cls(context, spec, handle, state)
            run.prepare()
            test_keys = state.write_test_keys()
        except Exception as exc:
            return make_result(spec, ComponentStatus.ERROR, f"could not start: {type(exc).__name__}: {exc}")
        records, summaries, halted_after = run.run_waves(waves)
        extra_summary, schedule = run.outcome(records)
        return run.finish(records, summaries, halted_after, len(waves), test_keys, extra_summary, **schedule)

    @staticmethod
    def host_ready(waves: Sequence[list[dict[str, Any]]]) -> Optional[str]:
        """Why this host cannot run ``waves``, and what to do about it; None when it can."""
        return None

    def prepare(self) -> None:
        """What the run needs in place before its test keys are written."""

    def wave_running(self, number: int) -> Any:
        """A context held while a wave's operations run."""
        return nullcontext()

    def kill_view(self) -> Optional[list[Member]]:
        return None

    # -- the loop ----------------------------------------------------------------

    def run_waves(self, waves: Sequence[list[dict[str, Any]]]) -> tuple[list, list, Optional[int]]:
        """Plan, run, heal, and validate every wave until one halts the run: ``(records, summaries, halted_after)``."""

        records: list[dict[str, Any]] = []
        summaries: list[dict[str, Any]] = []
        try:
            for number, wave in enumerate(waves, 1):
                if self._run_wave(number, wave, records, summaries):
                    return records, summaries, number
        except BaseException:
            # A cancelled or crashed run leaves no stalled process, partition, or setting behind; killed
            # instances are left to the runner, whose restarts may outlast the cancellation.
            self.release("cleanup")
            raise
        return records, summaries, None

    def _run_wave(self, number: int, wave: list[dict[str, Any]], records: list, summaries: list) -> bool:
        """Run one wave; whether it halts the run."""

        planned = self.plan(number, wave)
        with self.wave_running(number), ThreadPoolExecutor(max_workers=len(planned)) as pool:
            # Each operation runs in a copy of this call's context, so the runner's cancellation of the call
            # reaches it: its delay_s and duration_s waits end at once.
            futures = [pool.submit(contextvars.copy_context().run, self.run_operation, record) for record in planned]
            results = [future.result() for future in futures]
        records.extend(results)
        summary, validated, halt = self.settle(number, results)
        summaries.append(summary)
        self.context.event_bus.publish(
            f"{self.spec.id}.wave-validated", source=self.spec.id, payload={"wave": number, "success": validated}
        )
        return halt

    def plan(self, number: int, wave: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Each operation's record, its target resolved against the live members before the wave starts.

        Targets are picked in operation order, except that operations that
        need a role (promotions, repoints) pick last, so they can avoid what
        the wave disrupts. A restart picks its target when it runs.
        """

        self.selector.begin_wave(self.members())
        records: list[Any] = [None] * len(wave)
        order = sorted(range(len(wave)), key=lambda index: OPERATIONS[wave[index]["op"]].role is not None)
        for index in order:
            operation = wave[index]
            kind, target = operation["op"], operation.get("target")
            rng = self.rngs[f"{number}.{index}"] = self.context.rng(self.spec.id, f"wave-{number}-op-{index}")
            reason, shard = None, operation.get("shard")
            if kind == "compression" and target == "all":
                target = self.selector.select_all()
            elif kind != "restart" and OPERATIONS[kind].targets:
                member, reason = self.selector.select(kind, target, rng)
                target, shard = (member.instance_id, member.shard) if member else (None, None)
            record: dict[str, Any] = {
                "index": f"{number}.{index}",
                "wave": number,
                "op": kind,
                "shard": shard,
                "config": {key: value for key, value in operation.items() if key != "op"},
                "target": target,
                "success": False,
            }
            if reason is None and kind == "repoint":
                record["to"] = "primary"  # resolved when it runs
                if operation["to"] != "primary":
                    record["to"], reason = self.selector.select_other(operation["to"], target, rng)
            if reason is not None:
                record["missing" if isinstance(reason, TargetMissing) else "skipped"] = str(reason)
            self.done[record["index"]] = threading.Event()
            records[index] = record
        return records

    def run_operation(self, record: dict[str, Any]) -> dict[str, Any]:
        """Run one planned operation when it is due, unless it has no target or was skipped;
        ``finished_at`` is when it returned, unless the operation set it."""

        config = record["config"]
        try:
            if "missing" in record or "skipped" in record:
                return record
            if config["after"] is not None:
                self.done[f"{record['wave']}.{config['after']}"].wait()
            if not self.context.sleep(config["delay_s"]):
                record.update(error="cancelled", injection_error=True)
                return record
            try:
                getattr(self, f"_{record['op']}")(record, config)
            except Exception as exc:
                record["error"] = f"{type(exc).__name__}: {exc}"
            finally:
                # A held fault keeps its claims until its undo succeeds.
                if not (OPERATIONS[record["op"]].held and self.still_owned(record)):
                    self.unclaim(record)
            return record
        finally:
            record.setdefault("finished_at", time.time())
            self.done[record["index"]].set()

    def landed(self, record: dict[str, Any], affected: Sequence[str]) -> None:
        """``record``'s fault is in place and acts on ``affected``."""
        record["applied_at"] = time.time()
        record["affected"] = sorted(set(affected))

    # -- heal, validation, and verdicts ---------------------------------------------

    def settle(self, number: int, results: list[dict[str, Any]]) -> tuple[dict[str, Any], bool, bool]:
        """Heal, validate, and judge one wave: ``(summary, validated, halt)``.

        The heal first undoes every held fault still in place and checks that
        every instance not killed is alive; an undo, heal step, or validation
        that cannot complete is an error, and the wave is not validated.
        """

        heal_errors = self.release("heal")
        heal_failures = [
            f"{instance_id} exited without being killed"
            for instance_id in self.ids
            if instance_id not in self.down() and not self.handle.is_alive(instance_id)
        ]
        if not heal_failures and not heal_errors:
            try:
                self.heal(number)
            except Exception as exc:
                heal_errors.append(f"heal after wave {number} failed: {type(exc).__name__}: {exc}")
        validation = None
        if not heal_failures and not heal_errors:
            try:
                validation = self.validate(number, results)
            except Exception as exc:
                heal_errors.append(f"could not validate: {type(exc).__name__}: {exc}")
        failures, errors = self.judge(number, results, validation)
        failures[:0] = [f"wave {number}: {problem}" for problem in heal_failures]
        errors[:0] = [f"wave {number}: {problem}" for problem in heal_errors]
        self.failures += failures
        self.errors += errors
        summary = {
            "wave": number,
            "operations": [record["index"] for record in results],
            "ran": [record["index"] for record in results if record.get("applied_at")],
            "heal_problems": heal_failures + heal_errors,
            "validation": validation,
            **self.ledger(results),
            **self.wave_summary(number, results),
        }
        validated = bool(validation and validation["overall_success"]) and not heal_failures and not heal_errors
        critical = (
            validation is not None
            and not validation["overall_success"]
            and (validation["critical"] or self.state.config.blocking_on_failure)
        )
        halt = bool(heal_failures or heal_errors or critical or (failures and self.halt_on_failure))
        return summary, validated, halt

    def judge(
        self, number: int, results: list[dict[str, Any]], validation: Optional[Mapping[str, Any]]
    ) -> tuple[list[str], list[str]]:
        """The failures (FAIL) and the problems that leave the wave untested (ERROR) of one wave's operations."""

        failures = []
        errors = []
        if validation is not None and not validation["overall_success"]:
            checks = ", ".join(validation["failed_checks"]) or "unknown"
            detail = "; ".join(validation.get("error_messages") or ())
            # An inconclusive validation never saw the deployment: the wave is untested, not failed.
            (errors if validation.get("inconclusive") else failures).append(
                f"wave {number} validation {'could not judge' if validation.get('inconclusive') else 'failed'} "
                f"({checks})" + (f": {detail}" if detail else "")
            )
        for record in results:
            label = f"wave {number} {record['op']} {record['index']}"
            operation = OPERATIONS[record["op"]]
            if "undo_error" in record:
                # Even when the heal undid it later: the fault was not undone when it should have been.
                then = f"undone by the {record['undone_by']}" if record.get("undone_at") else "still in place"
                errors.append(f"{label} could not be undone ({record['undo_error']}); {then}")
            if record.get("unexpected_exit") or record.get("stuck"):
                failures.append(f"{label}: {record.get('unexpected_exit') or record['stuck']}")
            elif record.get("injection_error") or ((operation.removes or operation.held) and record.get("error")):
                errors.append(f"{label} was not injected: {record['error']}")
            elif "missing" in record:
                errors.append(f"{label} had no target: {record['missing']}")
            elif operation.topology and not record["success"] and "skipped" not in record:
                record["excused_by"] = self.excusing(record, results)
                if not record["excused_by"]:
                    why = record.get("refused") or record.get("error") or "did not complete"
                    failures.append(f"{label} failed without an interfering operation: {why}")
        if not any(record.get("applied_at") for record in results):
            errors.append(f"wave {number} ran no operation")
        return failures, errors

    def finish(
        self,
        records: list[dict[str, Any]],
        summaries: list[dict[str, Any]],
        halted_after: Optional[int],
        planned_waves: int,
        test_keys: Mapping[str, Any],
        extra_summary: str = "",
        **schedule: Any,
    ) -> Any:
        """The action's result after the final cleanup: the schedule, its evidence, and the verdict.

        ``FAIL`` on a failure; ``ERROR`` on a problem that leaves the run
        untested or the deployment unrestored (still-active faults
        included); ``SKIP`` when faults were planned and none landed.
        """

        cleanup_problems = self.release("cleanup", restarts=True)
        still_active = self.ledger(records)["still_active"]
        errors = [*self.errors, *(f"final cleanup: {problem}" for problem in cleanup_problems)]
        if still_active:
            errors.append(f"the changes of operation(s) {still_active} are still in place after the final cleanup")
        landed = [kill for kill in self.kills if kill["success"]]
        still_down = sorted(self.down())
        windows = schedule.pop("loss_windows", [])
        value = {
            "operations": records,
            # The kills whose instance is still down (the others were restarted), and every kill tried.
            "kills": [kill for kill in landed if kill["instance_id"] in still_down],
            "all_kills": self.kills,
            "waves": summaries,
            "halted_after_wave": halted_after,
            "not_run": planned_waves - len(summaries),
            # Operations whose changes are still in place (their obligations are open), and why the final
            # cleanup could not undo them.
            "still_active": still_active,
            "cleanup_problems": cleanup_problems,
            "loss_windows": windows,
            "test_keys": len(test_keys),
            **schedule,
        }
        faults = [record for record in records if OPERATIONS[record["op"]].removes or OPERATIONS[record["op"]].held]
        untested = faults and not any(record.get("applied_at") for record in faults)
        ran = sum(1 for record in records if record.get("applied_at"))
        skipped = [record["index"] for record in records if "skipped" in record]
        if skipped:
            extra_summary += f"; {len(skipped)} planned operation(s) skipped by the safety rules ({skipped})"
        summary = (
            f"{ran} of {len(records)} operation(s) in {len(summaries)} wave(s), {len(landed)} kill(s){extra_summary}"
        )
        return verdict(
            spec=self.spec,
            summary=f"{summary}; every wave validated",
            failures=self.failures,
            errors=errors,
            untested="no planned fault was injected" if untested else None,
            evidence=wave_evidence(self.spec, value, still_down, test_keys, windows),
            diagnostics={"summary": summary},
        )

    # -- building blocks -----------------------------------------------------------

    def _kill(self, record: dict[str, Any], config: dict[str, Any]) -> None:
        """Signal the target unless that is no longer safe, and wait for it to exit.

        It first waits for any held fault on the target to be undone (see
        ``Operation.mutates``), so it never meets a stalled process (one
        takes ``SIGTERM`` only once it runs again) or one a config change
        still needs. A target that already exited was not killed:
        ``unexpected_exit``, which fails the run.
        """

        target = record["target"]
        members = self.claim_kill(record, target)
        if members is False:
            self.selector.release(target)
            return
        reason = self.selector.confirm(target, members)
        if reason is not None:
            self.selector.release(target)
            record["missing" if isinstance(reason, TargetMissing) else "skipped"] = str(reason)
            return
        self.before_kill(target)
        kill: dict[str, Any] = {
            "instance_id": target,
            "role": self.role_of(target, members),
            "shard": record["shard"],
            "signal": config["signal"],
            "operation": record["index"],
            "wave": record["wave"],
            "at": time.time(),
            "success": True,
        }
        with self.lock:
            self.kills.append(kill)
        try:
            self.handle.send_signal(target, SIGNALS[config["signal"]])
        except ProcessLookupError:
            # Never picked twice, so the process exited by itself.
            kill.update(success=False, unexpected_exit=True)
            record["unexpected_exit"] = f"{target} exited before it was killed"
            return
        except Exception as exc:
            kill["success"] = False
            self.selector.release(target)
            record.update(error=f"{type(exc).__name__}: {exc}", injection_error=True)
            return
        undo = self.kill_undo(target)
        if undo is not None:
            self.own(record, "process.stopped", target, undo)
        self.selector.land(target)
        self.landed(record, [target])
        exited = self.handle.wait_exit(target, timeout=EXIT_TIMEOUT_S)
        kill["exited_at"] = time.time() if exited else None
        with self.lock:
            self.killed[target] = kill
        record["success"] = exited
        if not exited:
            record["stuck"] = f"{target} did not exit within {EXIT_TIMEOUT_S:.0f}s of {config['signal']}"
        self.context.event_bus.publish(f"{self.spec.id}.instance-killed", source=self.spec.id, payload=kill)

    def claim_kill(self, record: dict[str, Any], target: str) -> Any:
        """Claim a kill's target and the live shard it serves; the fresh view to confirm the kill against
        (None: the mode has none), or False when the claim was refused.

        The claim waits for a failover of that shard to settle (see
        ``Operation.mutates``); the view is read again once the claim is held,
        after whatever the wave did meanwhile.
        """

        members = self.kill_view()
        shard = next((member.shard for member in members or () if member.instance_id == target), None)
        if not self.claim(record, [target], shards=() if shard is None else (shard,)):
            return False
        return self.kill_view()

    def before_kill(self, instance_id: str) -> None:
        """What the mode records about ``instance_id`` right before it is killed."""

    def kill_undo(self, instance_id: str) -> Optional[Callable[[], None]]:
        """The undo of a kill whose instance the mode restarts (the kill then owns it), or None: it stays down."""
        return None

    def down(self) -> set[str]:
        """Instances killed and not restarted."""
        return set(self.selector.down)

    def _stall(self, record: dict[str, Any], config: dict[str, Any]) -> None:
        target = record["target"]
        if not self.claim(record, [target]):
            return
        with self.lock:
            if target in self.down():
                record["skipped"] = f"{target} was killed in this wave"
                return
            pause_owned(functools.partial(self.own, record), self.handle, target)
        self.hold(record, [target], config["duration_s"])

    def _partition(self, record: dict[str, Any], config: dict[str, Any]) -> None:
        target = record["target"]
        # Unique to this operation, so concurrent runs in one process never share a chain.
        chain = chain_name(f"{self.spec.id}/{record['index']}/{uuid.uuid4().hex}")
        hosts = {instance.instance_id: instance.host for instance in self.handle.instances}
        create_chain(chain)
        self.own(record, "network.partition", chain, lambda: remove_partition(chain))
        try:
            drop_between(chain, hosts, [target], [peer for peer in hosts if peer != target])
        except Exception:
            self.undo(record, "operation")
            raise
        self.hold(record, [target], config["duration_s"])

    def reachable(self, record: dict[str, Any], targets: Sequence[str]) -> list[str]:
        """``targets`` that are neither killed nor stalled; the operation is skipped when none is."""

        with self.lock:
            down, stalled = self.down(), self.held_on("process.paused")
            usable = [target for target in targets if target not in down and target not in stalled]
        if not usable:
            record["skipped"] = f"{', '.join(targets)} killed or stalled in this wave"
        return usable

    def claim(self, record: dict[str, Any], owns: Sequence[str], shards: Sequence[int] = ()) -> bool:
        """Claim the state ``record``'s operation changes on ``owns``, and on ``shards`` if it changes a
        shard (see ``Operation.mutates``).

        Waits while another operation of the wave holds any of it, and claims
        it all at once. False, with ``record["skipped"]``, when the holder's
        undo failed: that state is not back to what this operation would save.
        """

        mutates = OPERATIONS[record["op"]].mutates
        keys = {f"{kind}:{item}" for kind in mutates if kind != "shard" for item in owns}
        keys |= {f"shard:{shard}" for shard in shards if "shard" in mutates}
        with self.claimed:
            while True:
                holders = {holder["index"]: holder for key, holder in self.claims.items() if key in keys}
                if not holders:
                    self.claims.update(dict.fromkeys(keys, record))
                    return True
                stuck = sorted(index for index, holder in holders.items() if "undo_error" in holder)
                if stuck:
                    record["skipped"] = f"{', '.join(sorted(keys))} still changed by {stuck[0]}, whose undo failed"
                    return False
                record["waited_for"] = sorted(set(record.get("waited_for", [])) | set(holders))
                self.claimed.wait(0.5)

    def unclaim(self, record: dict[str, Any]) -> None:
        with self.claimed:
            for key in [key for key, holder in self.claims.items() if holder is record]:
                del self.claims[key]
            self.claimed.notify_all()

    def hold(self, record: dict[str, Any], affected: Sequence[str], duration_s: float) -> None:
        """A held fault is in place: keep it ``duration_s``, then undo it (unless the heal already did).

        The fault owns its changes (and its claims) until the undo succeeds:
        what fails stays owned for the heal and the final cleanup to retry,
        and records ``undo_error``.
        """

        self.landed(record, affected)
        record["success"] = True
        self.context.sleep(duration_s)
        self.undo(record, "operation")

    # -- ownership ---------------------------------------------------------------

    def own(self, record: Optional[dict[str, Any]], kind: str, target: str, undo: Callable[[], None]) -> Any:
        """Open the obligation for a change of ``kind`` to ``target`` just applied, owned by ``record``'s
        operation (None: by the wave), and undone by ``undo``, which must be idempotent."""

        obligation = self.context.obligations.open(self.spec, kind, target, undo)
        with self.lock:
            self.owned.append((obligation, record))
        return obligation

    def still_owned(self, record: Optional[dict[str, Any]]) -> list[Any]:
        """The open obligations ``record``'s operation owns."""

        with self.lock:
            return [obligation for obligation, owner in self.owned if owner is record and obligation.open]

    def held_on(self, kind: str) -> set[str]:
        """The targets of the operations whose change of ``kind`` is still in place (stalled, partitioned)."""

        with self.lock:
            return {
                owner["target"]
                for obligation, owner in self.owned
                if owner is not None and obligation.open and obligation.kind == kind
            }

    def discharge(self, obligation: Any, record: Optional[dict[str, Any]], stage: str) -> Optional[str]:
        """Undo one owned change as ``stage`` (the operation, a restart, the heal, the validation, or the final
        cleanup); why it failed, or None.

        A failed undo leaves the obligation open, and the operation's claims
        held, for the next try; it records the operation's first
        ``undo_error``. Once every change an operation owns is undone, it
        records ``undone_at`` and ``undone_by`` and releases its claims.
        """

        try:
            obligation.discharge()
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            if record is not None:
                with self.claimed:
                    record.setdefault("undo_error", error)
                    self.claimed.notify_all()
            return f"could not undo the {obligation.kind} on {obligation.target}: {error}"
        if record is not None and not self.still_owned(record):
            with self.lock:
                undone_at = max(item.undone_at for item, owner in self.owned if owner is record)
            record.update(undone_at=undone_at, undone_by=stage)
            self.unclaim(record)
        return None

    def undo(self, record: dict[str, Any], stage: str) -> list[str]:
        """Undo every change ``record``'s operation still owns; the problems."""

        problems = [self.discharge(obligation, record, stage) for obligation in self.still_owned(record)]
        return [problem for problem in problems if problem]

    def release(self, stage: str, *, restarts: bool = False) -> list[str]:
        """Undo every change still in place (``stage``: who does, the heal or the final cleanup); with
        ``restarts``, restart the killed instances too (the heal restarts them in its own order).

        Returns the undos that failed; those stay owned, for the next try.
        """

        with self.lock:
            pending = [
                (obligation, record)
                for obligation, record in self.owned
                if obligation.open and (restarts or obligation.kind != "process.stopped")
            ]
        pending.sort(key=lambda item: UNDO_ORDER.get(item[0].kind, len(UNDO_ORDER)))
        problems = [self.discharge(obligation, record, stage) for obligation, record in pending]
        return [problem for problem in problems if problem]

    def ledger(self, results: Sequence[Mapping[str, Any]]) -> dict[str, list]:
        """A wave's operations by what happened to them: ``planned`` (every one), ``applied`` (landed),
        ``healed`` (landed, and every change it owned is undone), ``still_active`` (a change it owns is still
        in place), and ``skipped``.

        Read from the obligations: only a successful undo counts as healed.
        """

        with self.lock:
            active: dict[str, bool] = {}
            for obligation, owner in self.owned:
                if owner is not None:
                    active[owner["index"]] = active.get(owner["index"], False) or obligation.open
        return {
            "planned": [record["index"] for record in results],
            "applied": [record["index"] for record in results if record.get("applied_at")],
            "healed": [
                record["index"]
                for record in results
                if record.get("applied_at") and record["index"] in active and not active[record["index"]]
            ],
            "still_active": [record["index"] for record in results if active.get(record["index"])],
            # Refused by the safety rules (or by an earlier fault's state): this wave did not inject them.
            "skipped": [record["index"] for record in results if "skipped" in record],
        }


# ---------------------------------------------------------------------------
# Evidence and verdicts
# ---------------------------------------------------------------------------

# One kill: the ``<id>.instance-killed`` payload, and an item of the schedule's ``kills`` and ``all_kills``.
KILL_FIELDS = {
    "instance_id": TEXT,
    "role": nullable(str),
    "shard": nullable(int),
    "signal": TEXT,
    "operation": TEXT,
    "wave": INTEGER,
    "at": NUMBER,
    "success": FLAG,
    # Set once the kill landed: None when the process did not exit in time.
    "exited_at": ConfigField(float, required=False, allow_none=True),
    "unexpected_exit": optional(bool),
}
# One operation's record: these fields always, and what happened to it (see docs/components.md).
_OPERATION = mapping_of(
    {
        "index": TEXT,
        "wave": INTEGER,
        "op": ConfigField(str, choices=tuple(OPERATIONS)),
        "shard": nullable(int),
        "config": MAPPING,
        "target": ANY,
        "success": FLAG,
    },
    additional=ANY,
)


def wave_declares(
    wave_fields: Mapping[str, ConfigField],
    schedule_fields: Mapping[str, ConfigField],
    *,
    events: Optional[Mapping[str, ConfigSchema]] = None,
    loss_windows: bool = False,
    changes: tuple[str, ...] = (),
) -> Declarations:
    """A chaos action's declarations: its events, the evidence of :func:`wave_evidence`, and its changes.

    ``wave_fields`` and ``schedule_fields`` are the mode's own fields of each
    wave summary (``wave_summary``) and of the schedule; with
    ``loss_windows`` it emits ``valkey.data.loss-window`` too.
    """

    wave = mapping_of(
        {
            "wave": INTEGER,
            "operations": LIST,
            "ran": LIST,
            "heal_problems": LIST,
            "validation": nullable(dict),
            **{name: LIST for name in ("planned", "applied", "healed", "still_active", "skipped")},
            **wave_fields,
        }
    )
    kills = ConfigField(list, items=mapping_of(KILL_FIELDS))
    schedule = mapping_of(
        {
            "operations": ConfigField(list, items=_OPERATION),
            "kills": kills,
            "all_kills": kills,
            "waves": ConfigField(list, items=wave),
            "halted_after_wave": nullable(int),
            "not_run": INTEGER,
            "still_active": LIST,
            "cleanup_problems": LIST,
            "loss_windows": LIST,
            "test_keys": INTEGER,
            **schedule_fields,
        }
    )
    evidence = {
        "valkey.chaos.schedule": declared(schedule),
        "valkey.instances.expected-down": declared(mapping_of({"instances": ConfigField(list, items=TEXT)})),
        "valkey.chaos.test-keys": declared(mapping_of({"keys": mapping_of(additional=TEXT)})),
    }
    if loss_windows:
        window = mapping_of({"start": NUMBER, "end": NUMBER, "instance_id": TEXT, "reason": TEXT})
        evidence[LOSS_WINDOW] = declared(window, many=True)
    return Declarations(
        events={
            "{id}.instance-killed": ConfigSchema(KILL_FIELDS),
            "{id}.wave-validated": ConfigSchema({"wave": INTEGER, "success": FLAG}),
            **(events or {}),
        },
        evidence=evidence,
        changes=changes,
    )


def wave_evidence(
    spec: Any,
    schedule: Mapping[str, Any],
    expected_down: Sequence[str],
    test_keys: Mapping[str, Any],
    loss_windows: Sequence[Mapping[str, Any]] = (),
) -> tuple:
    """The evidence both actions publish: the schedule, the instances down on purpose, the test keys, and
    one ``valkey.data.loss-window`` per window (``valkey.oracle.ledger`` reads those)."""

    return (
        make_evidence(spec, "valkey.chaos.schedule", "action.completed", schedule),
        make_evidence(spec, "valkey.instances.expected-down", "action.completed", {"instances": list(expected_down)}),
        make_evidence(spec, "valkey.chaos.test-keys", "action.completed", {"keys": test_keys}),
        *(
            make_evidence(
                spec,
                LOSS_WINDOW,
                "action.completed",
                {key: window[key] for key in ("start", "end", "instance_id", "reason")},
            )
            for window in loss_windows
        ),
    )


def verdict(
    spec: Any,
    summary: str,
    *,
    failures: Sequence[str] = (),
    errors: Sequence[str] = (),
    untested: Optional[str] = None,
    evidence: tuple = (),
    diagnostics: Optional[Mapping[str, Any]] = None,
) -> Any:
    """``FAIL`` on any failure, else ``ERROR`` on any problem that leaves the run untested (a fault with no
    target, one that could not be injected), else ``SKIP`` when no planned fault landed (``untested``),
    else ``PASS``."""

    if failures or errors:
        return make_result(
            spec,
            ComponentStatus.FAIL if failures else ComponentStatus.ERROR,
            "; ".join([*failures, *errors]),
            evidence=evidence,
            diagnostics=diagnostics,
        )
    if untested:
        return make_result(spec, ComponentStatus.SKIP, f"{untested}: {summary}", evidence=evidence)
    return make_result(spec, ComponentStatus.PASS, summary, evidence=evidence)


class WaveAction:
    """A chaos action: its ``run_cls`` runs its waves (:meth:`WaveRun.execute`)."""

    run_cls: type

    def run(self, context: Any, spec: Any) -> Any:
        return self.run_cls.execute(context, spec)


def wave_registration(
    component_id: str, action: type, fields: Mapping[str, ConfigField], declares: Declarations
) -> Any:
    """The registration of chaos action ``action``: its mode's ``waves``, checked against the plan's
    deployment when the scenario compiles, the deployment's state ``validation`` config, and ``fields``."""

    mode = action.run_cls.mode
    return component_registration(
        component_id,
        action,
        {
            "waves": waves_field(mode),
            # The deployment's state validation config (``handle.state_check``), checked when the scenario compiles.
            "validation": ConfigField(dict, default=dict, schema=STATE_VALIDATION_SCHEMA),
            **fields,
        },
        check=lambda config, spec, plan: check_waves(config["waves"], mode, plain_config(plan.deployment)),
        deployment_modes=(mode,),
        declares=declares,
    )
