"""The wave operations: what each building block supports and does, and the ``waves`` config of a mode."""

import signal
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Optional

from valkey_fuzzer.schema import ConfigError, ConfigField, ConfigSchema

from .instances import INSTANCE_ID, check_shard
from .network import DEGRADE_FIELDS, check_degrade

SIGNALS = {"SIGKILL": signal.SIGKILL, "SIGTERM": signal.SIGTERM}
CLUSTER, STANDALONE, MIGRATION = "cluster", "standalone", "slot-migration"
ROLE_TARGETS = ("primary", "replica", "any")
# The slot-migration mode's targets besides the roles: the wave's migration endpoints, the primary that owns
# the slot (``source``) and the one importing it (``target``), resolved once the wave's ``migrate`` picked them.
MIGRATION_TARGETS = ("source", "target")
# The phases of a wave's migration an operation of the slot-migration mode may wait for (``at``), in order,
# read from CLUSTER GETSLOTMIGRATIONS on the source (its export job) and the target (its import job): ``started``
# once the source accepted CLUSTER MIGRATESLOTS; ``snapshot`` while the export is snapshotting or streaming
# (states snapshotting, replicating, waiting-to-pause; the import receiving-snapshot, waiting-for-paused);
# ``handoff`` from the source's pause to the target's claim (failover-paused, failover-granted; the import
# failover-requested, failover-granted); ``finished`` once either job ended (success, failed, cancelled, or
# the import's cleaning-up).
PHASES = ("started", "snapshot", "handoff", "finished")
# The operations of the slot-migration mode that act on the wave's migration: they take ``at``, and ``source``
# and ``target`` as targets.
MIGRATION_OPERATIONS = ("kill", "partition", "cancel")
# The deployment mode each wave mode runs on.
DEPLOYMENT_MODES = {CLUSTER: "cluster", STANDALONE: "standalone", MIGRATION: "cluster"}
_DURATION = ConfigField(float, default=1.0, minimum=0, maximum=600)
_TARGET = {"target": ConfigField(str, default="replica")}
# Every operation's fields: when it starts, after its delay_s, or after the operation at position
# ``after`` of its wave finished, then its delay_s.
_COMMON = {
    "delay_s": ConfigField(float, default=0.0, minimum=0, maximum=600),
    "after": ConfigField(int, allow_none=True, default=None, minimum=0),
}
# The fields of MIGRATION_OPERATIONS in the slot-migration mode: the migration phase the operation waits
# for (None: it starts at its delay_s), at most ``phase_timeout_s``; a phase the migration passed or never
# reached before it ended lets the operation go ahead, recorded as missed.
_PHASED = {
    "at": ConfigField(str, allow_none=True, default=None, choices=PHASES),
    "phase_timeout_s": ConfigField(float, default=60.0, minimum=0.1, maximum=3600),
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
        (CLUSTER, STANDALONE, MIGRATION),
        ROLE_TARGETS,
        {
            "target": ConfigField((str, list), default="replica"),
            "signal": ConfigField(str, default="SIGKILL", choices=tuple(SIGNALS)),
        },
        removes=True,
        disrupts=True,
        mutates=("process", "run-state", "shard"),
    ),
    # Slot-migration mode: CLUSTER MIGRATESLOTS of key's slot from the primary that owns it to shard's primary
    # (null: the shard after the owner's), waited for until the export ends; the wave's kills, partitions and
    # cancels may name its endpoints and phases.
    "migrate": Operation(
        (MIGRATION,),
        (),
        {
            "key": ConfigField(str),
            "shard": ConfigField(int, allow_none=True, default=None, minimum=0, maximum=15),
            "timeout_s": ConfigField(float, default=60.0, minimum=1, maximum=3600),
        },
        topology=True,
    ),
    # CLUSTER CANCELSLOTMIGRATIONS on the wave's migration source.
    "cancel": Operation((MIGRATION,), (), {}, disrupts=True),
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
        mutates=("shard",),
    ),
    "restart": Operation(
        (STANDALONE, MIGRATION),
        ("killed",),
        {"target": ConfigField(str, default="killed")},
        topology=True,
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
    ),
    "repoint": Operation(
        (STANDALONE,),
        ("replica",),
        {**_TARGET, "to": ConfigField(str, default="replica")},
        role="replica",
        topology=True,
    ),
    # Drops the traffic between the target and its peers: every other instance (``others``), or, in the
    # slot-migration mode, the wave's migration endpoints or instance IDs.
    "partition": Operation(
        (STANDALONE, MIGRATION),
        ROLE_TARGETS,
        {**_TARGET, "peers": ConfigField((str, list), default="others"), "duration_s": _DURATION},
        disrupts=True,
        held=True,
    ),
    # Loss, delay, reordering, duplication, or a rate limit on the links between the instance and every other.
    "degrade": Operation(
        (STANDALONE,),
        ROLE_TARGETS,
        {**_TARGET, "duration_s": _DURATION, **DEGRADE_FIELDS},
        disrupts=True,
        held=True,
    ),
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
}


def operations_for(mode: str) -> tuple[str, ...]:
    return tuple(name for name, operation in OPERATIONS.items() if mode in operation.modes)


def operation_field(mode: str) -> ConfigField:
    """One wave operation of ``mode``: a union tagged by ``op``, each kind with its timing and own fields."""

    def fields(kind: str) -> dict[str, ConfigField]:
        phased = _PHASED if mode == MIGRATION and kind in MIGRATION_OPERATIONS else {}
        return {"op": ConfigField(str, choices=(kind,)), **_COMMON, **OPERATIONS[kind].fields, **phased}

    return ConfigField(dict, variants=("op", {kind: ConfigSchema(fields(kind)) for kind in operations_for(mode)}))


def waves_field(mode: str) -> ConfigField:
    """The ``waves`` of a ``mode`` action: lists of :func:`operation_field` items."""

    return ConfigField(list, items=ConfigField(list, items=operation_field(mode)))


def resolve_waves(waves: Sequence[Sequence[Mapping[str, Any]]], mode: str) -> list[list[dict[str, Any]]]:
    """``waves`` checked against :func:`waves_field`, with their defaults: no wave is empty, and ``after``
    names an earlier operation of its wave."""

    if not waves:
        raise ConfigError("waves must list at least one wave")
    for number, wave in enumerate(waves):
        if not wave:
            raise ConfigError(f"waves[{number}] has no operations")
    resolved = waves_field(mode).resolve(waves, "waves")
    for number, wave in enumerate(resolved):
        for index, operation in enumerate(wave):
            if operation["after"] is not None and operation["after"] >= index:
                raise ConfigError(f"waves[{number}][{index}].after must name an earlier operation of the wave")
    return resolved


def check_target(value: str, allowed: Sequence[str], instances: int, where: str) -> None:
    """``value`` is one of ``allowed`` or a planned instance ID (below ``instances``)."""

    if value in allowed:
        return
    if not INSTANCE_ID.fullmatch(value):
        raise ConfigError(f"{where} must be one of {list(allowed)} or an instance ID such as valkey-1, got {value!r}")
    if int(value.split("-")[1]) >= instances:
        raise ConfigError(f"{where}: {value!r} is not a planned instance ID (valkey-0 to valkey-{instances - 1})")


def starts_before(wave: Sequence[Mapping[str, Any]], earlier: int, later: int) -> bool:
    """Whether operation ``earlier`` of ``wave`` surely starts before operation ``later``.

    It does when ``later`` waits (through ``after``) for it to finish, or when
    it starts at its own ``delay_s`` and ``later`` cannot start until after
    that: an operation that waits for another starts no sooner than its own
    ``delay_s`` past that one's earliest start.
    """

    start, index = 0.0, later
    while index is not None:
        if index == earlier:
            return True
        start += wave[index]["delay_s"]
        index = wave[index]["after"]
    return wave[earlier]["after"] is None and wave[earlier]["delay_s"] < start


def check_waves(waves: Sequence[Sequence[Mapping[str, Any]]], mode: str, document: Mapping[str, Any]) -> None:
    """``waves`` for a ``mode`` deployment shaped as ``document`` (a plan's deployment, with its defaults) says."""

    waves = resolve_waves(waves, mode)
    replicas = document["replicas_per_primary"]
    primaries = document["primaries"] if mode in (CLUSTER, MIGRATION) else 1
    if replicas < 1:
        raise ConfigError("wave chaos needs replicas_per_primary >= 1")
    instances = primaries * (1 + replicas)
    for number, wave in enumerate(waves):
        failovers: set[int] = set()
        migrate = next((position for position, item in enumerate(wave) if item["op"] == "migrate"), None)
        for index, operation in enumerate(wave):
            where = f"waves[{number}][{index}]"
            kind = operation["op"]
            # The wave's migration endpoints are targets only once its migrate operation started.
            endpoints = MIGRATION_TARGETS if mode == MIGRATION and kind in MIGRATION_OPERATIONS else ()
            if OPERATIONS[kind].targets:
                targets = operation["target"] if isinstance(operation["target"], list) else [operation["target"]]
                if not targets:
                    raise ConfigError(f"{where}.target names no instance")
                for target in targets:
                    allowed = (*OPERATIONS[kind].targets, *endpoints) if len(targets) == 1 else ()
                    check_target(target, allowed, instances, f"{where}.target")
            if kind == "partition" and operation["peers"] != "others":
                if mode != MIGRATION:
                    raise ConfigError(
                        f"{where}.peers: a {mode} partition isolates its target from every other instance"
                    )
                peers = operation["peers"] if isinstance(operation["peers"], list) else [operation["peers"]]
                if not peers:
                    raise ConfigError(f"{where}.peers names no instance")
                for peer in peers:
                    check_target(peer, endpoints if len(peers) == 1 else (), instances, f"{where}.peers")
            if mode == MIGRATION and kind in MIGRATION_OPERATIONS:
                # It waits for the migrate operation to start, so the two may start together.
                needs = kind == "cancel" or operation["at"] is not None
                needs = needs or operation.get("target") in endpoints or operation.get("peers") in endpoints
                if needs and migrate is None:
                    raise ConfigError(f"{where}: {kind} acts on the wave's migration, so the wave needs a migrate")
            if kind == "migrate":
                if operation["shard"] is not None:
                    check_shard(operation["shard"], primaries, f"{where}.shard")
                if index != migrate:
                    raise ConfigError(f"{where}: a wave migrates one slot; put a second migration in its own wave")
            elif kind == "failover":
                check_shard(operation["shard"], primaries, f"{where}.shard")
                if operation["shard"] in failovers:
                    raise ConfigError(f"{where}: shard {operation['shard']} already fails over in this wave")
                failovers.add(operation["shard"])
            elif kind == "restart" and not any(
                other["op"] == "kill" and starts_before(wave, other_index, index)
                for other_index, other in enumerate(wave)
            ):
                raise ConfigError(f"{where}: a restart needs a kill of the same wave that starts before it")
            elif kind == "repoint":
                check_target(operation["to"], ("primary", "replica"), instances, f"{where}.to")
                if INSTANCE_ID.fullmatch(operation["to"]) and operation["to"] == operation["target"]:
                    raise ConfigError(f"{where}: a replica cannot replicate itself")
                if operation["to"] == "replica" and replicas < 2:
                    raise ConfigError(f"{where}: repointing to another replica needs replicas_per_primary >= 2")
            elif kind in ("partition", "degrade") and document.get("addressing") != "per_instance_loopback":
                raise ConfigError(f"{where}: a {kind} needs deployment.addressing: per_instance_loopback")
            if kind == "degrade":
                check_degrade(operation, where)
