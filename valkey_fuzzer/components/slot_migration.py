"""``valkey.action.migrate-slots``: move one hash slot to another primary with ``CLUSTER MIGRATESLOTS`` and wait
for the export to end.

The slot is ``key``'s, so a workload whose keys carry ``key``'s hash tag
(``{s}``, say) writes to it; the source is the primary that owns the slot when
the action runs, and the target the primary of shard ``shard``. The action waits for
the source's export job (``CLUSTER GETSLOTMIGRATIONS``) to reach ``success``,
``failed`` or ``cancelled`` and records which, so an experiment can kill the
target or drop the link meanwhile: the oracles judge the data and the cluster
state, this action only that the migration ran to an end.
``valkey.action.slot-migration-chaos`` (:mod:`.slot_migration_chaos`) runs
migrations in waves with faults on them; it shares this module's endpoint
resolution and job reading.
"""

from collections.abc import Mapping, Sequence
from typing import Any, Optional

import valkey
from valkey.cluster import key_slot

from valkey_fuzzer._data import jsonable
from valkey_fuzzer.clock import now
from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.results import ComponentStatus, make_evidence, make_result
from valkey_fuzzer.schema import ConfigField, ConfigSchema

from .cluster import read_view
from .instances import ClientCache, check_shard, deployment, send_command, send_to, shard_primary
from .polling import poll_until
from .registration import component_registration
from .schemas import ANY, FLAG, INTEGER, NUMBER, TEXT, declared, mapping_of, nullable

_MIGRATE_FIELDS = {
    # The key whose slot moves; a hash tag ({s}) names the slot every key carrying it is in.
    "key": ConfigField(str),
    # The shard whose primary imports the slot; the source is whichever primary owns the slot.
    "shard": ConfigField(int, default=1, minimum=0),
    "timeout_s": ConfigField(float, default=60.0, minimum=1, maximum=3600),
    "poll_interval_s": ConfigField(float, default=0.05, minimum=0.01, maximum=10),
}
# The states a slot migration job ends in (cluster_migrateslots.c); any other is in progress.
ENDED = ("success", "failed", "cancelled")


def jobs_of(reply: Any) -> list[dict[str, Any]]:
    """``CLUSTER GETSLOTMIGRATIONS``'s reply (an array of flat field-value arrays, or of maps) as records, in
    the order Valkey lists them: the newest job first (``cluster_migrateslots.c`` adds a job at the list's head)."""

    jobs = []
    for item in reply or []:
        if isinstance(item, Mapping):
            jobs.append({str(key): value for key, value in item.items()})
        elif isinstance(item, Sequence) and not isinstance(item, str):
            jobs.append({str(key): value for key, value in zip(item[::2], item[1::2])})
    return jobs


def job_of(jobs: Sequence[Mapping[str, Any]], operation: str, slot: int) -> Optional[dict[str, Any]]:
    """The newest ``operation`` (``export`` or ``import``) job of ``slot`` alone among ``jobs``, or None: a node
    that migrated the slot before lists that job too, after the one in progress."""

    matching = [
        job
        for job in jobs
        if str(job.get("operation", "")).lower() == operation and str(job.get("slot_ranges")) == f"{slot}-{slot}"
    ]
    return dict(matching[0]) if matching else None


def read_jobs(
    handle: Any, clients: ClientCache, instance_id: str, *, context: Any
) -> tuple[Optional[list[dict[str, Any]]], Optional[str]]:
    """``instance_id``'s ``CLUSTER GETSLOTMIGRATIONS`` as :func:`jobs_of` records, through the client ``clients``
    holds for it (opened at its first read): ``(jobs, None)``, or ``(None, error)`` when it did not answer.

    A follow reads both endpoints every few milliseconds, so each keeps one
    client for as long as it is followed; the caller closes them
    (``clients.close()``). A client whose connection failed or was lost is
    dropped, so the next read opens a new one: the instance may have been
    killed and restarted meanwhile.
    """

    client = clients.get(handle, instance_id)
    try:
        reply = send_command(client, ["CLUSTER", "GETSLOTMIGRATIONS"], timeout_s=2.0, context=context)
    except (valkey.exceptions.ConnectionError, valkey.exceptions.TimeoutError, OSError) as exc:
        clients.drop(instance_id)
        return None, f"{type(exc).__name__}: {exc}"
    except valkey.exceptions.ResponseError as exc:
        return None, f"{type(exc).__name__}: {exc}"
    return jobs_of(jsonable(reply)), None


def hash_tag(key: str) -> str:
    """The hash tag Valkey takes ``key``'s slot from: the text between its first ``{`` and the ``}`` after it,
    when there is any, else the key itself. ``{tag}`` then names the same slot as ``key``, unless the key has
    braces but no tag (``{}x``): Valkey hashes it whole, and no tag names its slot."""

    start = key.find("{")
    end = key.find("}", start + 1)
    return key[start + 1 : end] if start >= 0 and end > start + 1 else key


def _owner(handle: Any, view: Mapping[str, Any], slot: int) -> tuple[Optional[str], Optional[str]]:
    """The instance that owns ``slot`` in ``view``, and its node ID."""

    owner_id = next(
        (
            primary["node_id"]
            for primary in view["primaries"]
            if any(start <= slot <= end for start, end in primary["slots"])
        ),
        None,
    )
    return next((item.instance_id for item in handle.instances if item.cluster_node_id == owner_id), None), owner_id


def migration_endpoints(handle: Any, key: str, shard: Optional[int]) -> dict[str, Any]:
    """Where ``key``'s slot moves from and to when shard ``shard``'s primary imports it: ``slot``, ``source``
    and ``target`` (instance IDs) and their node IDs. With ``shard`` None the slot moves to the shard after
    its owner's planned one (the last shard's to shard 0), so a slot migrated again and again goes round the
    shards. Raises :class:`LookupError` with the reason when nothing can be migrated: the shard has no primary
    that answers, no instance owns the slot, or the target owns it."""

    slot = key_slot(key.encode())
    roles = handle.roles()
    if shard is None:
        # Any primary's view names the owner; the owner's own view, when it answers, is read for the owner too.
        shards = sorted({item.shard_id for item in handle.instances})
        for candidate in (shard_primary(handle, item, roles) for item in shards):
            view = read_view(handle, candidate) if candidate is not None else None
            if view is not None:
                break
        else:
            raise LookupError(f"no primary answers, so no view names the owner of slot {slot}")
        source, _ = _owner(handle, view, slot)
        if source is None:
            raise LookupError(f"no instance owns slot {slot} (the slot of {key}) in {candidate}'s view")
        planned = handle.instance(source).shard_id
        shard = shards[(shards.index(planned) + 1) % len(shards)]
    target = shard_primary(handle, shard, roles)
    # The target's own view names the slot's owner; the deployment names the instance with that node ID.
    view = read_view(handle, target) if target is not None else None
    if target is None or view is None or view["role"] != "primary":
        raise LookupError(f"shard {shard} has no primary that answers, so none can import")
    source, owner_id = _owner(handle, view, slot)
    if source is None:
        raise LookupError(f"no instance owns slot {slot} (the slot of {key}) in {target}'s view")
    if source == target:
        raise LookupError(f"{target} already owns slot {slot}; pick another key or shard")
    return {
        "key": key,
        "slot": slot,
        "source": source,
        "target": target,
        "source_node_id": owner_id,
        "target_node_id": view["node_id"],
    }


def _validate_migrate(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    check_shard(config["shard"], plan.deployment["primaries"])


class MigrateSlotsAction:
    """Move ``key``'s slot from the primary that owns it to shard ``shard``'s primary, and wait for the export to end."""

    def run(self, context: Any, spec: ComponentSpec):
        config = spec.config
        handle = deployment(context)
        try:
            record: dict[str, Any] = migration_endpoints(handle, config["key"], config["shard"])
        except LookupError as exc:
            # Nothing can be migrated: the run never reached what it tests.
            return make_result(spec, ComponentStatus.ERROR, str(exc))
        key, slot, source, target = record["key"], record["slot"], record["source"], record["target"]
        record["started_at"] = now()
        command = ["CLUSTER", "MIGRATESLOTS", "SLOTSRANGE", slot, slot, "NODE", record["target_node_id"]]
        reply = send_to(handle, source, command, timeout_s=config["timeout_s"], context=context)
        record["reply"] = reply
        if "reply" not in reply:
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"{source} did not start the migration of slot {slot} to {target}: {reply.get('error')}",
                diagnostics=record,
            )
        context.event_bus.publish(
            f"{spec.id}.migration-started",
            source=spec.id,
            payload={"slot": slot, "source": source, "target": target},
        )

        # The source's client, held for the whole wait (one read per poll_interval_s).
        clients = ClientCache(timeout=2.0)

        def ended() -> tuple[bool, Any]:
            jobs, error = read_jobs(handle, clients, source, context=context)
            job = job_of(jobs, "export", slot) if jobs is not None else None
            if job is None:
                return False, error or f"{source} lists no export of slot {slot}"
            return str(job.get("state", "")).lower() in ENDED, job

        try:
            done, last = poll_until(
                ended, timeout_s=config["timeout_s"], interval_s=config["poll_interval_s"], context=context
            )
        finally:
            clients.close()
        record["ended_at"] = now()
        record["job"] = last if isinstance(last, dict) else None
        record["state"] = str(last.get("state", "")).lower() if isinstance(last, dict) else None
        record["message"] = str(last.get("message") or "") if isinstance(last, dict) else None
        context.event_bus.publish(
            f"{spec.id}.migration-completed",
            source=spec.id,
            payload={"slot": slot, "source": source, "target": target, "state": record["state"], "ended": done},
        )
        evidence = (make_evidence(spec, "valkey.cluster.slot-migration.result", "action.completed", record),)
        if not done:
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"the export of slot {slot} from {source} to {target} did not end within {config['timeout_s']:g}s: "
                f"{last if isinstance(last, str) else record['state']}",
                evidence=evidence,
                diagnostics=record,
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"slot {slot} ({key}) migrated from {source} to {target}: {record['state']}"
            + (f", {record['message']}" if record["message"] else ""),
            evidence=evidence,
        )


_MIGRATE_DECLARES = Declarations(
    events={
        "{id}.migration-started": ConfigSchema({"slot": INTEGER, "source": TEXT, "target": TEXT}),
        "{id}.migration-completed": ConfigSchema(
            {"slot": INTEGER, "source": TEXT, "target": TEXT, "state": nullable(str), "ended": FLAG}
        ),
    },
    evidence={
        "valkey.cluster.slot-migration.result": declared(
            mapping_of(
                {
                    "key": TEXT,
                    "slot": INTEGER,
                    "source": TEXT,
                    "target": TEXT,
                    "source_node_id": TEXT,
                    "target_node_id": TEXT,
                    "started_at": NUMBER,
                    "ended_at": NUMBER,
                    "reply": ANY,
                    "job": nullable(dict),
                    "state": nullable(str),
                    "message": nullable(str),
                }
            )
        ),
    },
)

REGISTRATIONS = (
    component_registration(
        "valkey.action.migrate-slots",
        MigrateSlotsAction,
        _MIGRATE_FIELDS,
        check=_validate_migrate,
        deployment_modes=("cluster",),
        declares=_MIGRATE_DECLARES,
    ),
)
