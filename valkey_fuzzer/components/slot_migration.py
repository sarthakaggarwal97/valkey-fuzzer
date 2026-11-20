"""``valkey.action.migrate-slots``: move one hash slot to another primary with ``CLUSTER MIGRATESLOTS`` and wait
for the export to end.

The slot is ``key``'s, so a workload whose keys carry ``key``'s hash tag
(``{s}``, say) writes to it; the source is the primary that owns the slot when
the action runs, and the target the primary of shard ``shard``. The action waits for
the source's export job (``CLUSTER GETSLOTMIGRATIONS``) to reach ``success``,
``failed`` or ``cancelled`` and records which, so an experiment can kill the
target or drop the link meanwhile: the oracles judge the data and the cluster
state, this action only that the migration ran to an end.
"""

import time
from collections.abc import Mapping, Sequence
from typing import Any, Optional

from valkey.cluster import key_slot

from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.results import ComponentStatus, make_evidence, make_result
from valkey_fuzzer.schema import ConfigField, ConfigSchema

from .cluster import read_view
from .instances import check_shard, deployment, send_to
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
_ENDED = ("success", "failed", "cancelled")


def _jobs(reply: Any) -> list[dict[str, Any]]:
    """``CLUSTER GETSLOTMIGRATIONS``'s reply (an array of flat field-value arrays) as records."""

    jobs = []
    for item in reply or []:
        if isinstance(item, Mapping):
            jobs.append({str(key): value for key, value in item.items()})
        elif isinstance(item, Sequence) and not isinstance(item, str):
            jobs.append({str(key): value for key, value in zip(item[::2], item[1::2])})
    return jobs


def _export_of(jobs: Sequence[Mapping[str, Any]], slot: int) -> Optional[dict[str, Any]]:
    """The export job of ``slot`` alone among ``jobs``; the last one when the source ran several."""

    matching = [
        job
        for job in jobs
        if str(job.get("operation", "")).lower() == "export" and str(job.get("slot_ranges")) == f"{slot}-{slot}"
    ]
    return dict(matching[-1]) if matching else None


def _validate_migrate(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    check_shard(config["shard"], plan.deployment["primaries"])


class MigrateSlotsAction:
    """Move ``key``'s slot from the primary that owns it to shard ``shard``'s primary, and wait for the export to end."""

    def run(self, context: Any, spec: ComponentSpec):
        config = spec.config
        handle = deployment(context)
        key = config["key"]
        slot = key_slot(key.encode())
        roles = handle.roles()
        target = next(
            (
                item.instance_id
                for item in handle.instances
                if item.shard_id == config["shard"] and roles.get(item.instance_id) == "primary"
            ),
            None,
        )
        # The target's own view names the slot's owner; the deployment names the instance with that node ID.
        view = read_view(handle, target) if target is not None else None
        if target is None or view is None or view["role"] != "primary":
            # Nothing can be migrated: the run never reached what it tests.
            return make_result(
                spec, ComponentStatus.ERROR, f"shard {config['shard']} has no primary that answers, so none can import"
            )
        owner_id = next(
            (
                primary["node_id"]
                for primary in view["primaries"]
                if any(start <= slot <= end for start, end in primary["slots"])
            ),
            None,
        )
        source = next((item.instance_id for item in handle.instances if item.cluster_node_id == owner_id), None)
        if source is None:
            return make_result(
                spec, ComponentStatus.ERROR, f"no instance owns slot {slot} (the slot of {key}) in {target}'s view"
            )
        if source == target:
            return make_result(
                spec, ComponentStatus.ERROR, f"{target} already owns slot {slot}; pick another key or shard"
            )
        record: dict[str, Any] = {
            "key": key,
            "slot": slot,
            "source": source,
            "target": target,
            "source_node_id": owner_id,
            "target_node_id": view["node_id"],
            "started_at": time.time(),
        }
        command = ["CLUSTER", "MIGRATESLOTS", "SLOTSRANGE", slot, slot, "NODE", view["node_id"]]
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

        def ended() -> tuple[bool, Any]:
            read = send_to(handle, source, ["CLUSTER", "GETSLOTMIGRATIONS"], timeout_s=2.0, context=context)
            job = _export_of(_jobs(read.get("reply")), slot) if "reply" in read else None
            if job is None:
                return False, read.get("error") or f"{source} lists no export of slot {slot}"
            return str(job.get("state", "")).lower() in _ENDED, job

        done, last = poll_until(
            ended, timeout_s=config["timeout_s"], interval_s=config["poll_interval_s"], context=context
        )
        record["ended_at"] = time.time()
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
