"""Slot-migration chaos: atomic slot migrations in waves, each with faults on the migration, healed and validated.

``valkey.action.slot-migration-chaos`` is the slot-migration mode of the wave
engine (:mod:`.waves`). Each wave holds one ``migrate {key, shard, timeout_s}``
(``CLUSTER MIGRATESLOTS`` of ``key``'s slot from the primary that owns it to
shard ``shard``'s primary, waited for until the export ends, as
``valkey.action.migrate-slots`` does) and the operations that disturb it:
``kill {target, signal}`` of the migration's ``source`` or ``target`` (or a
role or instance, as in cluster chaos, under its kill safety), ``partition
{target, peers, duration_s}`` between them (Linux, root, iptables, per-instance
loopback addresses), ``cancel`` (``CLUSTER CANCELSLOTMIGRATIONS`` on the
source) and ``restart {target: killed}``. Each of the first three may wait for
a phase of the wave's migration (``at``: ``started``, ``snapshot``, ``handoff``,
``finished``), which the migrate operation reads from ``CLUSTER
GETSLOTMIGRATIONS`` on both endpoints while it runs; faults that must land on
a log line instead trigger on the ``slot-migration.*`` events of
``valkey.observer.log-trigger`` (``spec: slot-migration``).

Before the first wave the action writes ``keys_per_slot`` keys into every
slot its waves migrate. After each wave it heals (removes the partitions,
restarts the killed instances) and validates: the deployment's state checks
(the cluster converges, every slot has one owner in every view, roles,
replication, the state check's own keys) and its slot keys, every one of
which must still read back from the slot's owner with the value it wrote. A
migration that did not succeed is tolerated only when a kill, partition or
cancel of its wave landed before it ended (:func:`.replication_chaos.excusing_operations`);
missing or changed slot keys fail the wave. Verdicts, evidence and events are
the engine's (``valkey.chaos.schedule``, ``<id>.instance-killed``,
``<id>.wave-validated``); the schedule's operations carry each migration's
``migration`` record (slot, endpoints, final ``state``) and each phased
operation's ``phase`` (the phase it waited for, the one it saw, whether it
missed the phase).
"""

import functools
import threading
import uuid
from collections.abc import Mapping
from typing import Any, Optional

from valkey.cluster import key_slot

from valkey_fuzzer.clock import now
from valkey_fuzzer.deployment.local.queries import close_quietly
from valkey_fuzzer.hoststate import chain_name
from valkey_fuzzer.schema import ConfigField, ConfigSchema, plain_config

from .cluster_chaos import ClusterWaves
from .instances import ClientCache, send_to
from .network import instance_addresses, partition
from .replication_chaos import excusing_operations
from .schemas import INTEGER, LIST, MAPPING, TEXT, nullable
from .slot_migration import ENDED, hash_tag, job_of, migration_endpoints, read_jobs
from .wave_evidence import wave_declares
from .wave_operations import MIGRATION, MIGRATION_TARGETS, PHASES
from .waves import WaveAction, wave_registration

# The export (source) and import (target) job states of each phase (cluster_migrateslots.c,
# slotMigrationJobStateToString); a job that ended is in the last phase.
_PHASE_STATES = {
    "snapshot": {"snapshotting", "replicating", "waiting-to-pause", "receiving-snapshot", "waiting-for-paused"},
    "handoff": {"failover-paused", "failover-granted", "failover-requested"},
    "finished": {*ENDED, "cleaning-up"},
}
# How often the migrate operation reads both endpoints' jobs.
_POLL_INTERVAL_S = 0.01
_KEY_PREFIX = "slot-migration-chaos"
# The slot-migration bundle's events that show a phase (formal/slot-migration/mapping.yaml; an experiment
# publishes them with valkey.observer.log-trigger, spec: slot-migration), each on the endpoint that logged the
# line: an experiment that publishes them sees a phase on the line, a millisecond or two after Valkey logged it,
# rather than at the next job read.
_PHASE_EVENTS = {
    "slot-migration.snapshot.started": "snapshot",
    "slot-migration.snapshot.received": "snapshot",
    "slot-migration.paused": "handoff",
    "slot-migration.failover.requested": "handoff",
    "slot-migration.failover.granted": "handoff",
    "slot-migration.claimed": "finished",
    "slot-migration.cancelled": "finished",
    "slot-migration.finished": "finished",
}


def _state(job: Optional[Mapping[str, Any]]) -> Optional[str]:
    return str(job["state"]).lower() if job and job.get("state") is not None else None


def phase_of(export_state: Optional[str], import_state: Optional[str]) -> str:
    """The furthest phase of :data:`.wave_operations.PHASES` the jobs' states show: ``started`` until either
    job snapshots or streams."""

    states = {state for state in (export_state, import_state) if state}
    for phase in reversed(PHASES[1:]):
        if states & _PHASE_STATES[phase]:
            return phase
    return "started"


class _Migration:
    """One wave's migration: its endpoints once the migrate operation picked them, and the phase it is in, for
    the wave's other operations to wait on."""

    def __init__(self) -> None:
        self.lock = threading.Condition()
        self.endpoints: Optional[dict[str, Any]] = None
        # None until the migrate operation ran: it may have found nothing to migrate.
        self.started: Optional[bool] = None
        self.phase = PHASES[0]
        self.states: dict[str, Optional[str]] = {"export": None, "import": None}

    def set_endpoints(self, endpoints: Optional[dict[str, Any]]) -> None:
        with self.lock:
            self.endpoints = endpoints
            self.started = endpoints is not None
            self.lock.notify_all()

    def advance(self, export_state: Optional[str], import_state: Optional[str]) -> str:
        """Record the jobs' states and the phase they show."""

        with self.lock:
            self.states = {"export": export_state, "import": import_state}
            return self.advance_to(phase_of(export_state, import_state))

    def advance_to(self, phase: str) -> str:
        """The migration reached ``phase``; the phase never goes back (a job list read before another's may
        lag, and a line's event may come after a later read)."""

        with self.lock:
            if PHASES.index(phase) > PHASES.index(self.phase):
                self.phase = phase
                self.lock.notify_all()
            return self.phase

    @property
    def ended(self) -> bool:
        with self.lock:
            return self.phase == PHASES[-1]

    def end(self) -> None:
        self.advance_to(PHASES[-1])

    def wait_started(self, context: Any, timeout_s: float) -> Optional[dict[str, Any]]:
        """The endpoints once the migrate operation picked them; None when it found none, or in time."""

        deadline = now() + timeout_s
        with self.lock:
            while self.started is None and not context.cancelled:
                left = deadline - now()
                if left <= 0:
                    return None
                self.lock.wait(min(left, 0.5))
            return self.endpoints

    def wait_phase(self, phase: str, context: Any, timeout_s: float) -> str:
        """Wait until the migration reached ``phase`` or ended (or ``timeout_s`` passed); the phase it is in."""

        wanted = PHASES.index(phase)
        deadline = now() + timeout_s
        with self.lock:
            while PHASES.index(self.phase) < wanted and not context.cancelled:
                left = deadline - now()
                if left <= 0:
                    break
                self.lock.wait(min(left, 0.5))
            return self.phase


class MigrationWaves(ClusterWaves):
    """The slot-migration mode of the wave engine: a migration per wave, its faults, and the action's heal.

    Members, kill safety and views are cluster chaos's; unlike it, the heal
    restarts the killed instances (the next wave's migration needs its
    endpoints), so no kill stays down on purpose.
    """

    mode = MIGRATION
    restarts_killed = True

    def __init__(self, context: Any, spec: Any, handle: Any, state: Any):
        super().__init__(context, spec, handle, state)
        self.migrations: dict[int, _Migration] = {}
        # Kills about to be signalled, by target: ``before_kill`` stamps the migration's phase on each.
        self.killing: dict[str, dict[str, Any]] = {}
        # The slot keys written before the first wave, by slot: key -> value.
        self.slot_keys: dict[int, dict[str, str]] = {}
        # The slot-routing client the slot keys are written and read through, for the whole run (``prepare`` to
        # ``finish``): it follows MOVED after a migration.
        self.client: Any = None
        self.subscriptions = [context.event_bus.subscribe(name, self._on_phase_event) for name in _PHASE_EVENTS]

    def _on_phase_event(self, event: Any) -> None:
        """A bundle event on one of a running migration's endpoints: the phase it shows is reached."""

        instance_id = event.payload.get("instance_id")
        with self.lock:
            migrations = list(self.migrations.values())
        for migration in migrations:
            endpoints = migration.endpoints
            if endpoints and instance_id in (endpoints["source"], endpoints["target"]) and not migration.ended:
                migration.advance_to(_PHASE_EVENTS[event.name])

    def finish(self, *args: Any, **kwargs: Any) -> Any:
        for subscription in self.subscriptions:
            self.context.event_bus.unsubscribe(subscription)
        close_quietly(self.client)
        self.client = None
        return super().finish(*args, **kwargs)

    def prepare(self) -> None:
        """Write the slot keys: ``keys_per_slot`` keys carrying each migrated key's hash tag, so they are in its
        slot whoever owns it."""

        waves = plain_config(self.spec.config["waves"])
        count = self.spec.config["keys_per_slot"]
        tags = {}
        for operation in (operation for wave in waves for operation in wave if operation["op"] == "migrate"):
            tag, slot = hash_tag(operation["key"]), key_slot(operation["key"].encode())
            if key_slot(f"{{{tag}}}".encode()) != slot:
                raise ValueError(f"{operation['key']!r} has braces but no hash tag, so no key can be put in its slot")
            tags[tag] = slot
        self.client = self.handle.cluster_client(timeout=5.0)
        for tag, slot in sorted(tags.items()):
            keys = {f"{{{tag}}}:{_KEY_PREFIX}:{index}": f"{tag}:{index}" for index in range(count)}
            self.slot_keys[slot] = keys
            if keys:
                self.cluster_command("MSET", *(item for pair in keys.items() for item in pair))

    def cluster_command(self, *args: Any) -> Any:
        """One command through the run's slot-routing client."""

        return self.client.execute_command(*args)

    def outcome(self, records: list[dict[str, Any]]) -> tuple[str, dict[str, Any]]:
        migrations = [record for record in records if record["op"] == "migrate"]
        done = sum(record["success"] for record in migrations)
        return f", {done} of {len(migrations)} migration(s) succeeded", {
            "slots": sorted(self.slot_keys),
            "slot_keys": sum(len(keys) for keys in self.slot_keys.values()),
        }

    def resolves_later(self, kind: str, target: Any) -> bool:
        return target in MIGRATION_TARGETS

    def migration(self, wave: int) -> _Migration:
        with self.lock:
            return self.migrations.setdefault(wave, _Migration())

    # -- Operations ------------------------------------------------------------

    def _migrate(self, record: dict[str, Any], config: dict[str, Any]) -> None:
        migration = self.migration(record["wave"])
        try:
            self._start_and_follow(migration, record, config)
        finally:
            # However it ended, the wave's other operations learn whether the migration started.
            if migration.started is None:
                migration.set_endpoints(None)

    def _start_and_follow(self, migration: _Migration, record: dict[str, Any], config: dict[str, Any]) -> None:
        try:
            endpoints = migration_endpoints(self.handle, config["key"], config["shard"])
        except LookupError as exc:
            record["missing"] = str(exc)
            return
        source, target, slot = endpoints["source"], endpoints["target"], endpoints["slot"]
        record.update(target=target, involved=[source, target], migration=dict(endpoints))
        command = ["CLUSTER", "MIGRATESLOTS", "SLOTSRANGE", slot, slot, "NODE", endpoints["target_node_id"]]
        reply = send_to(self.handle, source, command, timeout_s=config["timeout_s"], context=self.context)
        record["migration"]["reply"] = reply
        if "reply" not in reply:
            record.update(error=f"{source} did not start the migration: {reply.get('error')}", injection_error=True)
            return
        self.landed(record, [source, target])
        migration.set_endpoints(endpoints)
        self.context.event_bus.publish(
            f"{self.spec.id}.migration-started",
            source=self.spec.id,
            payload={"wave": record["wave"], "slot": slot, "source": source, "target": target},
        )
        state = self._follow(migration, endpoints, config["timeout_s"])
        record["migration"].update(state=state, ended_at=now(), states=dict(migration.states))
        record["success"] = state == "success"
        if state is None:
            record["error"] = (
                f"the export of slot {slot} from {source} to {target} did not end within {config['timeout_s']:g}s"
            )
        self.context.event_bus.publish(
            f"{self.spec.id}.migration-completed",
            source=self.spec.id,
            payload={"wave": record["wave"], "slot": slot, "source": source, "target": target, "state": state},
        )

    def _follow(self, migration: _Migration, endpoints: Mapping[str, Any], timeout_s: float) -> Optional[str]:
        """Read both endpoints' jobs until the migration ended: the export's final state, the import's when the
        source is down (its export cannot be read, so the target's import tells how it ended), or None when
        neither ended in ``timeout_s``."""

        source, target, slot = endpoints["source"], endpoints["target"], endpoints["slot"]
        deadline = now() + timeout_s
        # The export job's name, which the target's import job of this migration shares (SYNCSLOTS ESTABLISH
        # NAME): a target that imported the slot before lists that job too, until the new one is created.
        name = None
        # One client per endpoint for the whole follow (:func:`.slot_migration.read_jobs`).
        clients = ClientCache(timeout=2.0)
        try:
            while True:
                down = self.selector.down
                export_job = self._job(clients, source, slot=slot) if source not in down else None
                name = (export_job or {}).get("name") or name
                import_job = self._job(clients, target, name=name) if target not in down and name else None
                export, import_ = _state(export_job), _state(import_job)
                migration.advance(export, import_)
                if export in ENDED:
                    migration.end()
                    return export
                if source in down and (import_ in ENDED or import_ == "cleaning-up" or target in down):
                    migration.end()
                    return import_ if import_ in ENDED else None
                if now() >= deadline or not self.context.sleep(_POLL_INTERVAL_S):
                    migration.end()
                    return None
        finally:
            clients.close()

    def _job(
        self, clients: ClientCache, instance_id: str, *, slot: Optional[int] = None, name: Optional[str] = None
    ) -> Optional[dict]:
        """``instance_id``'s newest export job of ``slot``, or its job named ``name``, read through the client
        ``clients`` holds for it; None when it lists none, or does not answer."""

        jobs, _ = read_jobs(self.handle, clients, instance_id, context=self.context)
        if jobs is None:
            return None
        if name is not None:
            return next((job for job in jobs if job.get("name") == name), None)
        return job_of(jobs, "export", slot)

    def _endpoints(self, record: dict[str, Any], config: dict[str, Any]) -> Optional[dict[str, Any]]:
        """The wave's migration endpoints for an operation that acts on them; None, with the record saying why
        the operation did not run, when the migration did not start."""

        endpoints = self.migration(record["wave"]).wait_started(self.context, config["phase_timeout_s"])
        if endpoints is None:
            record["skipped"] = "the wave's migration did not start"
        return endpoints

    def _await_phase(self, record: dict[str, Any], config: dict[str, Any]) -> None:
        if config["at"] is None:
            return
        seen = self.migration(record["wave"]).wait_phase(config["at"], self.context, config["phase_timeout_s"])
        record["phase"] = {"wanted": config["at"], "seen": seen, "missed": seen != config["at"], "at": now()}

    def _resolve(self, record: dict[str, Any], config: dict[str, Any], name: str) -> Optional[str]:
        """An instance ``name`` names: a migration endpoint, once the wave's migration started, or itself."""

        if name not in MIGRATION_TARGETS:
            return name
        endpoints = self._endpoints(record, config)
        return None if endpoints is None else endpoints[name]

    def _kill(self, record: dict[str, Any], config: dict[str, Any]) -> None:
        endpoint = record["target"] if record["target"] in MIGRATION_TARGETS else None
        if endpoint is not None:
            instance = self._resolve(record, config, record["target"])
            if instance is None:
                return
            member, refusal = self.selector.select("kill", [instance], self.rngs[record["index"]])
            if member is None:
                record[refusal.kind] = refusal.reason
                return
            record.update(target=member.instance_id, shard=member.shard)
        self._await_phase(record, config)
        with self.lock:
            self.killing[record["target"]] = record
        super()._kill(record, config)
        if endpoint is not None and record["success"]:
            # The bundle's mapping reads a killed target as the model's CrashT (formal/slot-migration/mapping.yaml).
            migration = self.migration(record["wave"]).endpoints or {}
            self.context.event_bus.publish(
                f"{self.spec.id}.endpoint-killed",
                source=self.spec.id,
                payload={
                    "wave": record["wave"],
                    "slot": migration.get("slot"),
                    "endpoint": endpoint,
                    "instance_id": record["target"],
                },
            )

    def before_kill(self, instance_id: str) -> None:
        """Right before the signal: the phase the wave's migration is in, which the phase wait may have left."""

        with self.lock:
            record = self.killing.pop(instance_id, None)
        if record is not None and "phase" in record:
            record["phase"]["landed_in"] = self.migration(record["wave"]).phase

    def _cancel(self, record: dict[str, Any], config: dict[str, Any]) -> None:
        endpoints = self._endpoints(record, config)
        if endpoints is None:
            return
        source = endpoints["source"]
        record.update(target=source)
        self._await_phase(record, config)
        if not self.reachable(record, [source]):
            return
        reply = send_to(self.handle, source, ["CLUSTER", "CANCELSLOTMIGRATIONS"], timeout_s=5.0, context=self.context)
        record["reply"] = reply
        if reply.get("unanswered"):
            record.update(
                error=f"{source} did not answer CLUSTER CANCELSLOTMIGRATIONS: {reply['error']}", injection_error=True
            )
            return
        self.landed(record, [source])
        # "No migrations ongoing": the migration ended before the cancel reached it, which the record shows.
        record["success"] = "reply" in reply

    def _partition(self, record: dict[str, Any], config: dict[str, Any]) -> None:
        target = self._resolve(record, config, record["target"])
        if target is None:
            return
        peers_config = config["peers"] if isinstance(config["peers"], list) else [config["peers"]]
        hosts = instance_addresses(self.handle)
        if peers_config == ["others"]:
            peers = [peer for peer in hosts if peer != target]
        else:
            peers = []
            for name in peers_config:
                peer = self._resolve(record, config, name)
                if peer is None:
                    return
                peers.append(peer)
            peers = [peer for peer in peers if peer != target]
        if not peers:
            record["skipped"] = f"{target} has no peer to be cut off from"
            return
        record.update(target=target, peers=peers)
        self._await_phase(record, config)
        # Unique to this operation, so concurrent runs in one process never share a chain.
        chain = chain_name(f"{self.spec.id}/{record['index']}/{uuid.uuid4().hex}")
        partition(functools.partial(self.own, record), chain, hosts, [target], peers)
        self.context.event_bus.publish(
            f"{self.spec.id}.partitioned",
            source=self.spec.id,
            payload={"wave": record["wave"], "isolated": [target], "peers": peers},
        )
        self.hold(record, [target, *peers], config["duration_s"])

    # -- Heal and validation ------------------------------------------------------

    def heal(self, number: int) -> None:
        """Restart every killed instance, then wait for every instance to answer with the cluster state ok."""

        self.restart_down("heal")
        self.handle.refresh_roles()

    def validate(self, number: int, results: list[dict[str, Any]]) -> dict[str, Any]:
        """The deployment's state checks, then the slot keys: every one reads back with the value written."""

        validation = self.state.validate()
        check = self.check_slot_keys()
        validation.setdefault("checks", {})["slot_keys"] = check
        if not check["success"]:
            validation["overall_success"] = False
            validation["failed_checks"] = [*validation.get("failed_checks", []), "slot_keys"]
            validation["error_messages"] = [*validation.get("error_messages", []), check["error"]]
            # Lost keys were lost in this wave: the next wave starts with the slot keys back in place.
            for slot in check["reseeded"]:
                keys = self.slot_keys[slot]
                self.cluster_command("MSET", *(item for pair in keys.items() for item in pair))
        return validation

    def check_slot_keys(self) -> dict[str, Any]:
        """Read every slot key through a slot-routing client: ``success`` when each holds the value written."""

        missing: list[str] = []
        wrong: list[dict[str, Any]] = []
        reseeded: list[int] = []
        checked = 0
        for slot, keys in sorted(self.slot_keys.items()):
            if not keys:
                continue
            names = list(keys)
            values = self.cluster_command("MGET", *names)
            checked += len(names)
            lost = False
            for name, value in zip(names, values):
                if value is None:
                    missing.append(name)
                    lost = True
                elif str(value) != keys[name]:
                    wrong.append({"key": name, "expected": keys[name], "actual": str(value)[:200]})
                    lost = True
            if lost:
                reseeded.append(slot)
        error = None
        if missing or wrong:
            error = (
                f"{len(missing)} of {checked} slot key(s) missing and {len(wrong)} changed in slot(s) {reseeded}"
                + (f"; missing: {missing[:5]}" if missing else "")
                + (f"; changed: {[item['key'] for item in wrong[:5]]}" if wrong else "")
            )
        return {
            "success": error is None,
            "slots": sorted(self.slot_keys),
            "keys_checked": checked,
            "missing": len(missing),
            "wrong": len(wrong),
            "examples": [*({"key": name, "type": "missing"} for name in missing[:5]), *wrong[:5]],
            "reseeded": reseeded,
            "error": error,
        }

    def excusing(self, record: dict[str, Any], results: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return excusing_operations(record, results)

    def wave_summary(self, number: int, results: list[dict[str, Any]]) -> dict[str, Any]:
        migration = next((record.get("migration") for record in results if record["op"] == "migrate"), None)
        return {
            "kills": sum(kill["wave"] == number and kill["success"] for kill in self.kills),
            "migration": migration,
            "views": self.handle.drain_view_reads(),
        }


class SlotMigrationChaosAction(WaveAction):
    """Slot migrations in waves with kills, partitions and cancels on them, healed and validated after every wave."""

    run_cls = MigrationWaves


_ENDPOINTS = {"wave": INTEGER, "slot": INTEGER, "source": TEXT, "target": TEXT}
_CHAOS_DECLARES = wave_declares(
    {"kills": INTEGER, "migration": nullable(dict), "views": MAPPING},
    {"slots": LIST, "slot_keys": INTEGER},
    events={
        "{id}.migration-started": ConfigSchema(_ENDPOINTS),
        "{id}.migration-completed": ConfigSchema({**_ENDPOINTS, "state": nullable(str)}),
        # A kill of the wave's migration source or target: ``endpoint`` says which, ``instance_id`` the node.
        "{id}.endpoint-killed": ConfigSchema(
            {
                "wave": INTEGER,
                "slot": nullable(int),
                "endpoint": ConfigField(str, choices=MIGRATION_TARGETS),
                "instance_id": TEXT,
            }
        ),
        "{id}.partitioned": ConfigSchema({"wave": INTEGER, "isolated": LIST, "peers": LIST}),
    },
    changes=("process.stopped", "network.partition"),
)

REGISTRATIONS = (
    wave_registration(
        "valkey.action.slot-migration-chaos",
        SlotMigrationChaosAction,
        {
            # Keys written into every migrated slot before the first wave, each checked after every wave.
            "keys_per_slot": ConfigField(int, default=20, minimum=0, maximum=1000),
            # How long the heal waits for a restarted instance to be ready.
            "heal_timeout_s": ConfigField(float, default=60.0, minimum=1, maximum=3600),
        },
        _CHAOS_DECLARES,
    ),
)
