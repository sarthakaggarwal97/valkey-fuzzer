"""valkey.action.slot-migration-chaos against a fake cluster whose migrations walk a script: its waves, phases,
kill and cancel of the endpoints, partitions, heal, slot-key validation, config checks, and the experiment."""

import json
import threading
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import valkey
from valkey.cluster import key_slot

from tests.fakes import FakeClient
from tests.valkey_experiments_support import (
    chaos_operation,
    chaos_schedule,
    evidence_values,
    resolved,
    unit_plan,
    validation_result,
    valkey_experiment,
)
from valkey_fuzzer.components import component_registrations, slot_migration, slot_migration_chaos
from valkey_fuzzer.components.slot_migration import hash_tag, job_of, jobs_of, migration_endpoints
from valkey_fuzzer.components.slot_migration_chaos import SlotMigrationChaosAction, phase_of
from valkey_fuzzer.components.wave_operations import MIGRATION, check_waves
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.results import ComponentStatus
from valkey_fuzzer.schema import ConfigError, plain_config
from valkey_fuzzer.testing import FakeContext, run_experiment

PASSED = validation_result()
SLOT = key_slot(b"{s}")  # 3828, in shard 0 of three
# An export's states up to its success, with the import's state beside each (cluster_migrateslots.c); the
# snapshot and the handoff last 30 reads (0.3 s at the action's poll interval), so a fault waiting for them lands
# in them.
SCRIPT = [
    ("connecting", None),
    *[("snapshotting", "receiving-snapshot")] * 30,
    ("replicating", "receiving-snapshot"),
    *[("failover-paused", "waiting-for-paused")] * 30,
    ("failover-granted", "failover-requested"),
    ("success", "success"),
]
HASH_SLOTS = 16384


def _job(slot, operation, state, source, target, name="x" * 40):
    return ["name", name, "operation", operation, "slot_ranges", f"{slot}-{slot}", "target_node", target,
            "source_node", source, "state", state, "message", ""]  # fmt: skip


class _Client(FakeClient):
    """One instance's client: the cluster commands the action sends go to the fake cluster."""

    def __init__(self, cluster, instance_id):
        super().__init__(cluster.commands.setdefault(instance_id, []))
        self.cluster, self.instance_id = cluster, instance_id

    def execute_command(self, *args):
        self.commands.append(args)
        return self.cluster.command(self.instance_id, args)

    def info(self, section=None):
        return {"loading": 0, "master_link_status": "up", "slave_repl_offset": 4242}

    def ping(self):
        return True


class _Migration:
    def __init__(self, slot, source, target, script, name):
        self.slot, self.source, self.target, self.script, self.step = slot, source, target, list(script), 0
        self.states = script[0]
        self.name = name

    @property
    def ended(self):
        return self.states[0] in ("success", "failed", "cancelled")


class FakeMigrationCluster:
    """Three shards of a primary and a replica (valkey-0/1, valkey-2/3, valkey-4/5) on per-instance addresses.

    A ``CLUSTER MIGRATESLOTS`` starts a migration that walks ``script`` one step per ``CLUSTER
    GETSLOTMIGRATIONS`` read on its source; at ``success`` the slot's owner becomes the target's shard. A killed
    primary's replica takes over at once; a migration whose endpoint died fails at the next read, and a cancel
    ends it ``cancelled``. Keys live in ``data``, whoever owns their slot; ``lose_on_success`` names keys the
    target drops when it claims the slot, and ``change_on_success`` the values it then holds instead.
    """

    def __init__(self, script=SCRIPT, results=()):
        self.instances = [
            SimpleNamespace(
                instance_id=f"valkey-{n}", role="primary" if n % 2 == 0 else "replica", shard_id=n // 2,
                cluster_node_id=chr(ord("a") + n) * 40, host=f"127.0.0.{n + 1}", port=7000 + n,
            )
            for n in range(6)
        ]  # fmt: skip
        self.alive = {node.instance_id: True for node in self.instances}
        self.role = {node.instance_id: node.role for node in self.instances}
        self.owner = {shard: shard for shard in range(3)}  # moved slots: slot -> shard, below
        self.moved = {}
        self.script, self.results = script, list(results)
        self.data = {}
        self.lose_on_success, self.change_on_success = set(), {}
        # A directory of <instance>.log files the fake appends Valkey's "Slot migration initiated" line to.
        self.logs = None
        self.migrations = []
        self.commands, self.calls, self.restarts, self.validations, self.stopped = {}, [], [], [], []
        self.mode = SimpleNamespace(value="cluster")
        self.lock = threading.RLock()

    # -- the deployment handle ------------------------------------------------------

    def roles(self):
        return {node_id: role for node_id, role in self.role.items() if self.alive[node_id]}

    def primaries(self):
        return [node_id for node_id, role in self.roles().items() if role == "primary"]

    def replicas(self):
        return [node_id for node_id, role in self.roles().items() if role == "replica"]

    def instance(self, instance_id):
        return next(node for node in self.instances if node.instance_id == instance_id)

    def live_instances(self, skip_instances=()):
        return [
            {"instance_id": node.instance_id, "role": self.role[node.instance_id], "shard_id": node.shard_id}
            for node in self.instances
            if self.alive[node.instance_id]
        ]

    def drain_view_reads(self):
        return {"reads": 0, "last": None, "disputed": []}

    def pid(self, instance_id):
        return 1

    def is_alive(self, instance_id):
        return self.alive[instance_id]

    def wait_exit(self, instance_id, timeout=10.0):
        return not self.alive[instance_id]

    def send_signal(self, instance_id, signum):
        with self.lock:
            if not self.alive[instance_id]:
                raise ProcessLookupError(instance_id)
            self.alive[instance_id] = False
            self.calls.append(("kill", instance_id, signum))
            # A migration job dies with its endpoint's process.
            for migration in self.migrations:
                if instance_id in (migration.source, migration.target) and not migration.ended:
                    migration.states = ("failed", "failed")
            if self.role[instance_id] == "primary":
                # The shard's replica wins the election at once.
                shard = self.instance(instance_id).shard_id
                for node in self.instances:
                    if node.shard_id == shard and node.instance_id != instance_id and self.alive[node.instance_id]:
                        self.role[node.instance_id] = "primary"
                        self.role[instance_id] = "replica"
                        break

    def restart(self, instance_id, wait_ready=True, ready_timeout=30.0):
        with self.lock:
            self.alive[instance_id] = True
            self.restarts.append(instance_id)

    def refresh_roles(self):
        return self.roles()

    def new_client(self, instance_id, timeout=5.0):
        return _Client(self, instance_id)

    def cluster_client(self, timeout=5.0):
        return _Client(self, "cluster")

    def state_check(self, validation=None, *, seed=None):
        validation = dict(validation or {})
        self.config = SimpleNamespace(
            blocking_on_failure=validation.get("blocking_on_failure", False), check_data_consistency=True
        )
        return self

    def write_test_keys(self):
        return {"fuzzer:test:0": "value"}

    def load_test_keys(self, keys):
        pass

    def mark_stopped(self, node_id, role, shard=None, *, left_survivor=False):
        self.stopped.append((node_id, role, shard))

    def validate(self, operations=None, *, retry=True):
        self.validations.append({"operations": operations, "alive": dict(self.alive)})
        return self.results.pop(0) if self.results else dict(PASSED)

    def log_file(self, instance_id):
        return f"{self.logs or '/nonexistent'}/{instance_id}.log"

    def process_output_file(self, instance_id):
        return None

    # -- the cluster --------------------------------------------------------------

    def shard_of(self, slot):
        return self.moved.get(slot, min(slot * 3 // HASH_SLOTS, 2))

    def primary_of(self, shard):
        return next(
            n.instance_id for n in self.instances if n.shard_id == shard and self.role[n.instance_id] == "primary"
        )

    def view(self, instance_id):
        node = self.instance(instance_id)
        return {
            "node_id": node.cluster_node_id,
            "role": self.role[instance_id],
            "primaries": [
                {
                    "node_id": self.instance(self.primary_of(shard)).cluster_node_id,
                    "slots": [[slot, slot] for slot in range(HASH_SLOTS) if self.shard_of(slot) == shard],
                }
                for shard in range(3)
            ],
        }

    def command(self, instance_id, args):
        with self.lock:
            if instance_id != "cluster" and not self.alive[instance_id]:
                raise valkey.exceptions.ConnectionError(f"{instance_id} is down")
            if args[0] == "MSET":
                self.data.update(dict(zip(args[1::2], args[2::2])))
                return "OK"
            if args[0] == "MGET":
                return [self.data.get(key) for key in args[1:]]
            if args[:2] == ("CLUSTER", "MIGRATESLOTS"):
                slot, node_id = int(args[3]), args[6]
                target = next(n.instance_id for n in self.instances if n.cluster_node_id == node_id)
                name = f"{len(self.migrations):040x}"  # the export's name, which the target's import job shares
                self.migrations.insert(0, _Migration(slot, instance_id, target, self.script, name))
                if self.logs is not None:
                    with open(self.log_file(instance_id), "a", encoding="utf-8") as log:
                        log.write(
                            "9300:M 19 Nov 2026 22:42:00.000 * Slot migration initiated through CLUSTER MIGRATESLOTS "
                            f"command: {{name: {'a' * 40}, operation: export, target_node_id: {node_id}, "
                            f"slots: {slot}-{slot}}} (user request from 'id=3')\n"
                        )
                return "OK"
            if args[:2] == ("CLUSTER", "CANCELSLOTMIGRATIONS"):
                running = [m for m in self.migrations if m.source == instance_id and not m.ended]
                if not running:
                    raise valkey.exceptions.ResponseError("No migrations ongoing")
                for migration in running:
                    migration.states = ("cancelled", "cancelled")
                return "OK"
            if args[:2] == ("CLUSTER", "GETSLOTMIGRATIONS"):
                return self._jobs(instance_id)
            if args == ("CLUSTER", "NODES"):
                return ""  # the quiet-end oracle's link check: no links to judge
            raise AssertionError(f"unexpected command {args}")

    def _jobs(self, instance_id):
        jobs = []
        for migration in self.migrations:
            if instance_id not in (migration.source, migration.target):
                continue
            # The target's import job exists once ESTABLISH arrived: two source reads after the command here
            # (one that ended was established, whenever it ended).
            if instance_id == migration.target and migration.step < 2 and not migration.ended:
                continue
            self._advance(migration, instance_id)
            export, import_ = migration.states
            operation = "EXPORT" if instance_id == migration.source else "IMPORT"
            state = export if operation == "EXPORT" else import_
            if state is not None:
                source, target = (self.instance(m).cluster_node_id for m in (migration.source, migration.target))
                jobs.append(_job(migration.slot, operation, state, source, target, migration.name))
        return jobs

    def _advance(self, migration, reader):
        if migration.ended:
            return
        if reader == migration.source and migration.step + 1 < len(migration.script):
            migration.step += 1
            migration.states = migration.script[migration.step]
            if migration.states[0] == "success":
                self.moved[migration.slot] = self.instance(migration.target).shard_id
                for key in self.lose_on_success:
                    self.data.pop(key, None)
                self.data.update(self.change_on_success)


def migrate(**fields):
    return {"op": "migrate", "key": "{s}", **fields}


def kill(target, **fields):
    return {"op": "kill", "target": target, **fields}


def run_chaos(handle, waves, *, seed=7, **config):
    """Run the action (held to its declarations by conftest's strict contract) on ``handle``."""

    spec = resolved(
        ComponentSpec(
            role=ComponentRole.ACTION,
            use="valkey.action.slot-migration-chaos",
            id="chaos",
            config={"waves": waves, "keys_per_slot": 3, **config},
        )
    )
    context = FakeContext(spec, services={"valkey.deployment": handle}, deployment_mode="cluster", seed=seed)
    with patch.object(slot_migration, "read_view", lambda handle, instance_id, timeout=2.0: handle.view(instance_id)):
        return SlotMigrationChaosAction().run(context, spec), context


# ---------------------------------------------------------------------------
# The building blocks
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("export", "import_", "phase"),
    [
        (None, None, "started"),
        ("connecting", None, "started"),
        ("waiting-to-snapshot", "waiting-for-ack", "started"),
        ("snapshotting", "receiving-snapshot", "snapshot"),
        ("replicating", None, "snapshot"),
        ("failover-paused", "waiting-for-paused", "handoff"),
        (None, "failover-requested", "handoff"),
        ("failover-granted", "failover-granted", "handoff"),
        ("success", None, "finished"),
        (None, "cleaning-up", "finished"),
        # A read of one job may lag the other's: the furthest state counts.
        ("replicating", "failover-requested", "handoff"),
    ],
)
def test_the_phase_is_the_furthest_the_jobs_states_show(export, import_, phase):
    assert phase_of(export, import_) == phase


def test_the_newest_job_of_a_slot_is_its_job():
    # Valkey lists the newest first: a node that migrated the slot before lists that job after the current one.
    jobs = jobs_of([_job(3828, "EXPORT", "snapshotting", "a", "b"), _job(3828, "EXPORT", "success", "a", "c")])
    assert job_of(jobs, "export", 3828)["state"] == "snapshotting"
    assert job_of(jobs, "import", 3828) is None and job_of(jobs, "export", 1) is None
    # RESP3 maps are jobs too.
    assert job_of(jobs_of([{"operation": "IMPORT", "slot_ranges": "7-7", "state": "cleaning-up"}]), "import", 7)


@pytest.mark.parametrize(
    ("key", "tag", "same_slot"),
    [("{s}", "s", True), ("user:{42}:name", "42", True), ("plain", "plain", True), ("{}x", "{}x", False),
     ("a}b{c", "a}b{c", False)],
)  # fmt: skip
def test_the_hash_tag_names_the_same_slot_as_its_key_unless_the_key_has_braces_without_a_tag(key, tag, same_slot):
    assert hash_tag(key) == tag
    assert (key_slot(f"{{{tag}}}".encode()) == key_slot(key.encode())) is same_slot


def test_a_key_with_braces_but_no_hash_tag_cannot_be_seeded():
    result, _ = run_chaos(FakeMigrationCluster(), [[{"op": "migrate", "key": "{}x"}]])
    assert result.status is ComponentStatus.ERROR
    assert "could not start: ValueError: '{}x' has braces but no hash tag" in result.summary


def test_without_a_shard_a_slot_goes_round_the_shards():
    handle = FakeMigrationCluster()
    with patch.object(slot_migration, "read_view", lambda handle, instance_id, timeout=2.0: handle.view(instance_id)):
        first = migration_endpoints(handle, "{s}", None)
        assert (first["slot"], first["source"], first["target"]) == (SLOT, "valkey-0", "valkey-2")
        handle.moved[SLOT] = 2
        assert migration_endpoints(handle, "{s}", None)["target"] == "valkey-0"
        # Shard 2's primary died: its replica owns the slot now and is the source.
        handle.send_signal("valkey-4", 9)
        assert migration_endpoints(handle, "{s}", None)["source"] == "valkey-5"
        with pytest.raises(LookupError, match=r"valkey-[45] already owns slot"):
            migration_endpoints(handle, "{s}", 2)


# ---------------------------------------------------------------------------
# The action
# ---------------------------------------------------------------------------


def test_waves_disturb_the_migration_at_its_phases_then_heal_and_validate():
    handle = FakeMigrationCluster()
    waves = [
        [migrate(), kill("target", at="handoff")],
        [migrate(), kill("source", at="snapshot"), {"op": "restart", "target": "killed", "after": 1}],
        [migrate(), {"op": "cancel", "at": "snapshot"}],
        [migrate()],
    ]
    result, context = run_chaos(handle, waves)
    assert result.status is ComponentStatus.PASS, result.summary
    assert "4 wave(s), 2 kill(s), 1 of 4 migration(s) succeeded; every wave validated" in result.summary
    schedule = chaos_schedule(result)
    # The slot keys were written before the first wave and read back after every wave.
    assert sorted(handle.data) == [f"{{s}}:slot-migration-chaos:{n}" for n in range(3)]
    assert schedule["slots"] == [SLOT] and schedule["slot_keys"] == 3
    assert [wave["validation"]["checks"]["slot_keys"]["success"] for wave in schedule["waves"]] == [True] * 4
    # Wave 1: the target, shard 1's primary, killed at the handoff; the migration failed and the kill excuses it.
    first, kill_1 = chaos_operation(result, "1.0"), chaos_operation(result, "1.1")
    assert (first["migration"]["source"], first["migration"]["target"], first["migration"]["state"]) == (
        "valkey-0", "valkey-2", "failed")  # fmt: skip
    assert first["success"] is False and first["excused_by"] == [
        {"operation": "1.1", "op": "kill", "instances": ["valkey-2"]}
    ]
    assert (kill_1["target"], kill_1["shard"], kill_1["success"]) == ("valkey-2", 1, True)
    assert kill_1["phase"]["wanted"] == kill_1["phase"]["seen"] == kill_1["phase"]["landed_in"] == "handoff"
    assert not kill_1["phase"]["missed"]
    # The heal restarted it before the validation, so no instance is down on purpose.
    assert handle.restarts[0] == "valkey-2" and handle.validations[0]["alive"]["valkey-2"] is True
    assert evidence_values(result.evidence, "valkey.instances.expected-down") == [{"instances": []}]
    # Wave 2: the source killed as it snapshots, restarted by the wave's own restart, the migration failed.
    second, kill_2, restart = (chaos_operation(result, index) for index in ("2.0", "2.1", "2.2"))
    assert (kill_2["target"], kill_2["phase"]["seen"], second["migration"]["state"]) == (
        "valkey-0",
        "snapshot",
        "failed",
    )
    assert restart["target"] == "valkey-0" and restart["success"] and handle.restarts == ["valkey-2", "valkey-0"]
    # Wave 3: cancelled mid-way; wave 4: undisturbed, so the slot moved on.
    third, cancel = chaos_operation(result, "3.0"), chaos_operation(result, "3.1")
    assert (
        third["migration"]["state"] == "cancelled"
        and cancel["success"]
        and cancel["target"] == third["migration"]["source"]
    )
    assert third["excused_by"] == [{"operation": "3.1", "op": "cancel", "instances": [cancel["target"]]}]
    fourth = chaos_operation(result, "4.0")
    assert fourth["success"] and fourth["migration"]["state"] == "success"
    assert handle.moved[SLOT] == handle.instance(fourth["migration"]["target"]).shard_id
    assert [wave["migration"]["state"] for wave in schedule["waves"]] == ["failed", "failed", "cancelled", "success"]
    names = [event.name for event in context.event_bus.committed_history()]
    assert names.count("chaos.migration-started") == 4 and names.count("chaos.migration-completed") == 4
    assert names.count("chaos.instance-killed") == 2 and names.count("chaos.wave-validated") == 4
    # Each kill of a migration endpoint says which one: the bundle's mapping reads a killed target as CrashT.
    killed = [plain_config(event.payload) for event in context.event_bus.committed_history("chaos.endpoint-killed")]
    assert killed == [
        {"wave": 1, "slot": SLOT, "endpoint": "target", "instance_id": "valkey-2"},
        {"wave": 2, "slot": SLOT, "endpoint": "source", "instance_id": "valkey-0"},
    ]
    assert not context.violations


def test_conform_reads_a_chaos_runs_target_kill_as_the_models_crash_and_how_each_job_ended(tmp_path):
    from valkey_fuzzer.spec.bundle import load_bundle
    from valkey_fuzzer.spec.conform import ObservedRun, collect_runs

    events = [
        ("slot-migration.started", {"instance_id": "valkey-0"}),
        ("slot-migration.import.created", {"instance_id": "valkey-2"}),
        ("slot-migration.snapshot.started", {"instance_id": "valkey-0"}),
        ("slot-migration.snapshot.received", {"instance_id": "valkey-2"}),
        ("slot-migration.paused", {"instance_id": "valkey-0"}),
        ("slot-migration.failover.requested", {"instance_id": "valkey-2"}),
        ("slot-migration.failover.granted", {"instance_id": "valkey-0"}),
        ("slot-migration.claimed", {"instance_id": "valkey-2"}),
        ("slot-migration.finished", {"instance_id": "valkey-2", "operation": "import", "state": "success"}),
        # The chaos action's kills: the target's is CrashT, the source's no step of the model.
        ("chaos.endpoint-killed", {"wave": 1, "slot": SLOT, "endpoint": "target", "instance_id": "valkey-2"}),
        ("chaos.endpoint-killed", {"wave": 2, "slot": SLOT, "endpoint": "source", "instance_id": "valkey-0"}),
        ("slot-migration.owner.learned", {"instance_id": "valkey-0"}),
        ("slot-migration.finished", {"instance_id": "valkey-0", "operation": "export", "state": "success"}),
    ]
    records = [{"kind": "header", "scenario_id": "unit", "seed": 1, "nodes": {}}]
    for index, (name, payload) in enumerate(events):
        records.append(
            {"kind": "event", "t": 1 + index / 10, "sequence": index, "name": name, "source": "x", "payload": payload}
        )
    path = tmp_path / "trace.jsonl"
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n")

    observed = ObservedRun(load_bundle("slot-migration").mapping, collect_runs([str(path)])[0])
    observed.replay()

    assert not observed.unapplied
    assert dict(observed.actions) == {
        "Start": 1, "TEstablish": 1, "SFork": 1, "TApply": 1, "SPause": 1, "TPaused": 1, "SGrant": 1, "TClaim": 1,
        "CrashT": 1, "Deliver": 1,
    }  # fmt: skip
    assert observed.states[-1]["faults"] == 1


def test_a_migration_that_did_not_succeed_without_a_fault_fails_the_wave():
    script = [*SCRIPT[:3], ("failed", "failed")]
    result, _ = run_chaos(FakeMigrationCluster(script=script), [[migrate()]])
    assert result.status is ComponentStatus.FAIL
    assert "wave 1 migrate 1.0 failed without an interfering operation" in result.summary
    assert chaos_operation(result, "1.0")["migration"]["state"] == "failed"


def test_lost_slot_keys_fail_the_wave_and_are_written_again_for_the_next():
    handle = FakeMigrationCluster()
    handle.lose_on_success.add("{s}:slot-migration-chaos:1")
    result, _ = run_chaos(handle, [[migrate()], [migrate()]])
    assert result.status is ComponentStatus.FAIL
    assert "wave 1 validation failed (slot_keys): 1 of 3 slot key(s) missing and 0 changed" in result.summary
    assert f"in slot(s) [{SLOT}]" in result.summary and "{s}:slot-migration-chaos:1" in result.summary
    checks = [wave["validation"]["checks"]["slot_keys"] for wave in chaos_schedule(result)["waves"]]
    assert (checks[0]["missing"], checks[0]["reseeded"], checks[0]["examples"]) == (
        1, [SLOT], [{"key": "{s}:slot-migration-chaos:1", "type": "missing"}])  # fmt: skip
    # Written again after wave 1, lost again at wave 2's success: each wave is judged on its own.
    assert checks[1]["missing"] == 1 and result.summary.count("validation failed (slot_keys)") == 2
    handle.lose_on_success.clear()
    handle.change_on_success["{s}:slot-migration-chaos:0"] = "changed"
    result, _ = run_chaos(handle, [[migrate()]])
    assert "0 of 3 slot key(s) missing and 1 changed" in result.summary


def test_a_fault_whose_phase_the_migration_passed_goes_ahead_and_records_the_miss():
    # One read and the export succeeded: the handoff was never seen.
    handle = FakeMigrationCluster(script=[("connecting", None), ("success", "success")])
    result, _ = run_chaos(handle, [[migrate(), kill("target", at="handoff")]])
    assert result.status is ComponentStatus.PASS, result.summary
    record = chaos_operation(result, "1.1")
    assert record["phase"] == {"wanted": "handoff", "seen": "finished", "missed": True, "at": record["phase"]["at"],
                               "landed_in": "finished"}  # fmt: skip
    assert record["success"] and ("kill", "valkey-2", 9) in handle.calls
    assert chaos_operation(result, "1.0")["success"]


def test_a_bundle_event_on_an_endpoint_shows_the_phase_ahead_of_the_next_job_read():
    # The snapshot lasts 300 reads (3 s); the source's "Pausing writes" line, published as the bundle's event by
    # the log-trigger observer, says the handoff began, so the kill lands then rather than at the next read.
    handle = FakeMigrationCluster(script=[("connecting", None), *[("snapshotting", "receiving-snapshot")] * 300])
    waves = [[migrate(), kill("target", at="handoff")]]
    spec = resolved(
        ComponentSpec(role=ComponentRole.ACTION, use="valkey.action.slot-migration-chaos", id="chaos",
                      config={"waves": waves, "keys_per_slot": 1})
    )  # fmt: skip
    context = FakeContext(spec, services={"valkey.deployment": handle}, deployment_mode="cluster")

    def paused_line():
        while not any(m.states[0] == "snapshotting" for m in handle.migrations):
            threading.Event().wait(0.01)
        context.event_bus.publish("slot-migration.paused", source="migration-log", payload={"instance_id": "valkey-0"})
        # Another node's line is not this migration's.
        context.event_bus.publish("slot-migration.claimed", source="migration-log", payload={"instance_id": "valkey-4"})

    thread = threading.Thread(target=paused_line)
    thread.start()
    with patch.object(slot_migration, "read_view", lambda handle, instance_id, timeout=2.0: handle.view(instance_id)):
        result = SlotMigrationChaosAction().run(context, spec)
    thread.join()
    assert result.status is ComponentStatus.PASS, result.summary
    record = chaos_operation(result, "1.1")
    assert (record["phase"]["seen"], record["phase"]["landed_in"], record["phase"]["missed"]) == (
        "handoff",
        "handoff",
        False,
    )
    assert chaos_operation(result, "1.0")["migration"]["states"]["export"] == "failed"
    assert record["finished_at"] - record["phase"]["at"] < 2


def test_an_older_import_job_of_the_slot_on_the_target_is_not_this_migrations():
    # Wave 1's cancel leaves a failed import job of the slot on valkey-2; wave 2 migrates to it again, and until
    # its own import job exists the target lists only the old one, which must not end the phases early.
    handle = FakeMigrationCluster()
    waves = [
        [migrate(shard=1), {"op": "cancel", "at": "snapshot"}],
        [migrate(shard=1), kill("target", at="handoff")],
    ]
    result, _ = run_chaos(handle, waves)
    assert result.status is ComponentStatus.PASS, result.summary
    kill_2 = chaos_operation(result, "2.1")
    assert (kill_2["phase"]["seen"], kill_2["phase"]["landed_in"], kill_2["target"]) == (
        "handoff",
        "handoff",
        "valkey-2",
    )
    assert [m.name for m in handle.migrations] == ["0" * 39 + "1", "0" * 40]


def test_operations_on_a_migration_that_did_not_start_are_skipped_and_the_wave_is_untested():
    handle = FakeMigrationCluster()
    handle.moved[SLOT] = 1  # shard 1 owns the slot already
    result, _ = run_chaos(handle, [[migrate(shard=1), kill("target", at="handoff")]])
    assert result.status is ComponentStatus.ERROR
    assert "wave 1 migrate 1.0 had no target: valkey-2 already owns slot" in result.summary
    assert chaos_operation(result, "1.1")["skipped"] == "the wave's migration did not start"
    assert handle.calls == []


def test_a_cancel_after_the_migration_ended_is_sent_and_recorded_as_late():
    handle = FakeMigrationCluster(script=[("connecting", None), ("success", "success")])
    result, _ = run_chaos(handle, [[migrate(), {"op": "cancel", "at": "handoff"}]])
    cancel = chaos_operation(result, "1.1")
    assert cancel["phase"]["missed"] and cancel["success"] is False
    assert cancel["reply"] == {"error": "ResponseError: No migrations ongoing"}
    assert result.status is ComponentStatus.PASS, result.summary


def test_a_wave_validation_failure_is_reported_and_the_next_wave_still_runs():
    handle = FakeMigrationCluster(results=[validation_result("slot_coverage"), PASSED])
    result, _ = run_chaos(handle, [[migrate()], [migrate()]])
    assert result.status is ComponentStatus.FAIL and "wave 1 validation failed (slot_coverage)" in result.summary
    assert len(handle.validations) == 2


def test_a_source_kill_is_refused_when_its_shard_has_no_other_live_member():
    handle = FakeMigrationCluster()
    handle.alive["valkey-1"] = False  # shard 0's replica is gone: the source is the shard's last member
    result, _ = run_chaos(handle, [[migrate(), kill("source")]])
    assert chaos_operation(result, "1.1")["skipped"] == "no shard-safe target"
    assert ("kill", "valkey-0", 9) not in handle.calls


class _Iptables:
    def __init__(self):
        self.calls = []

    def __call__(self, *args):
        self.calls.append(args)
        return SimpleNamespace(returncode=0, stderr="")


def test_a_partition_cuts_the_source_from_the_target_for_its_duration(monkeypatch):
    from valkey_fuzzer.components import network

    iptables = _Iptables()
    monkeypatch.setattr(network, "run_iptables", iptables)
    monkeypatch.setattr(slot_migration_chaos, "iptables_supported", lambda: (True, ""))
    removed = []
    monkeypatch.setattr(slot_migration_chaos, "remove_partition", lambda chain: removed.append(chain))
    handle = FakeMigrationCluster()
    waves = [[migrate(), {"op": "partition", "target": "source", "peers": "target", "at": "snapshot",
                          "duration_s": 0.05}]]  # fmt: skip
    result, context = run_chaos(handle, waves)
    assert result.status is ComponentStatus.PASS, result.summary
    record = chaos_operation(result, "1.1")
    assert (record["target"], record["peers"], record["phase"]["seen"]) == ("valkey-0", ["valkey-2"], "snapshot")
    rules = [call for call in iptables.calls if call[0] == "-A"]
    assert [(call[3], call[5]) for call in rules] == [("127.0.0.1", "127.0.0.3"), ("127.0.0.3", "127.0.0.1")]
    # Held for its duration, then undone by the operation itself (the heal undoes one still in place).
    assert record["undone_by"] == "operation" and len(removed) == 1
    [event] = context.event_bus.committed_history("chaos.partitioned")
    assert plain_config(event.payload) == {"wave": 1, "isolated": ["valkey-0"], "peers": ["valkey-2"]}
    # The migration went on in the fake; a real one fails, and the partition excuses it.
    assert chaos_operation(result, "1.0")["success"]


def test_partitions_need_iptables(monkeypatch):
    monkeypatch.setattr(slot_migration_chaos, "iptables_supported", lambda: (False, "network partitions require root"))
    waves = [[migrate(), {"op": "partition", "target": "source", "peers": "target"}]]
    result, _ = run_chaos(FakeMigrationCluster(), waves)
    assert result.status is ComponentStatus.ERROR
    assert "partition operations need Linux, root, and iptables (network partitions require root)" in result.summary


# ---------------------------------------------------------------------------
# Config checks
# ---------------------------------------------------------------------------

_DEPLOYMENT = {"primaries": 3, "replicas_per_primary": 1, "addressing": "per_instance_loopback"}


@pytest.mark.parametrize(
    ("mode", "waves", "match"),
    [
        (MIGRATION, [[kill("source")]], r"kill acts on the wave's migration, so the wave needs a migrate"),
        (MIGRATION, [[migrate(), {"op": "restart", "target": "killed"}]], r"a restart needs a kill of the same wave"),
        (MIGRATION, [[{"op": "cancel"}]], r"cancel acts on the wave's migration"),
        (MIGRATION, [[migrate(), migrate(shard=2)]], r"a wave migrates one slot"),
        (MIGRATION, [[migrate(shard=3)]], r"shard 3 is not below primaries 3"),
        (MIGRATION, [[migrate(), kill("source", at="claimed")]], r"at must be one of"),
        (
            MIGRATION,
            [[migrate(), {"op": "partition", "target": "source", "peers": "valkey-9"}]],
            r"peers: 'valkey-9' is not a planned instance ID",
        ),
        (MIGRATION, [[migrate(), {"op": "failover", "shard": 0}]], r"op"),
        ("cluster", [[kill("source"), {"op": "failover", "shard": 0}]], r"target must be one of"),
        (
            "standalone",
            [[{"op": "partition", "target": "replica", "peers": "valkey-0"}]],
            r"a standalone partition isolates its target from every other instance",
        ),
        ("standalone", [[kill("replica", at="handoff")]], r"at"),
    ],
)
def test_waves_that_misuse_the_migration_operations_are_rejected(mode, waves, match):
    with pytest.raises(ConfigError, match=match):
        check_waves(waves, mode, _DEPLOYMENT)


def test_the_migration_waves_of_the_experiment_and_the_example_pass_the_checks():
    check_waves([[migrate(), kill("source", after=0, at="snapshot"), {"op": "restart", "target": "killed", "after": 1}],
                 [migrate(shard=2), {"op": "cancel", "at": "handoff"}],
                 [migrate(), {"op": "partition", "target": "source", "peers": "target"}],
                 [migrate(), {"op": "partition", "target": "target", "peers": ["valkey-3"]}],
                 [kill("replica")]], MIGRATION, _DEPLOYMENT)  # fmt: skip
    # The compiled registration rejects a standalone deployment and checks the waves against the plan.
    spec = ComponentSpec(role=ComponentRole.ACTION, use="valkey.action.slot-migration-chaos", id="chaos",
                         config={"waves": [[migrate(), kill("target")]]})  # fmt: skip
    registration = next(r for r in component_registrations() if r.component_id == "valkey.action.slot-migration-chaos")
    assert registration.deployment_modes == ("cluster",)
    plan = unit_plan([spec], "cluster", {"mode": "cluster", "primaries": 3, "replicas_per_primary": 1})
    registration.check(resolved(spec).config, spec, plan)


# ---------------------------------------------------------------------------
# The experiment, through the real compiler and runner
# ---------------------------------------------------------------------------


def test_the_experiment_runs_its_default_waves_and_validates_the_cluster_at_the_end(tmp_path):
    handle = FakeMigrationCluster()
    handle.logs = tmp_path
    for node in handle.instances:
        (tmp_path / f"{node.instance_id}.log").write_text("")
    # Without the trace observer, which samples CLUSTER INFO and CLUSTER NODES the fake does not answer.
    experiment = valkey_experiment("valkey.slot-migration-chaos")
    experiment = replace(
        experiment,
        components=tuple(
            replace(item, depends_on=tuple(d for d in item.depends_on if d != "trace"))
            for item in experiment.components
            if item.id != "trace"
        ),
        required_evidence=tuple(item for item in experiment.required_evidence if item.source != "trace"),
    )
    with patch.object(slot_migration, "read_view", lambda handle, instance_id, timeout=2.0: handle.view(instance_id)):
        outcome = run_experiment(
            experiment,
            component_registrations(),
            deployment={"primaries": 3, "replicas_per_primary": 1},
            services={"valkey.deployment": handle},
        )
    results = {result.component_id: result for result in outcome.component_results}
    assert {name: result.status.value for name, result in results.items()} == {
        "chaos": "PASS", "cluster": "PASS", "liveness": "PASS", "error-counters": "PASS", "quiet-end": "PASS",
        "config-churn": "PASS", "migration-log": "PASS",
    }  # fmt: skip
    assert outcome.status.value == "PASS", outcome.summary
    # The default waves: the target killed at the handoff and the source as it snapshots, both restarted.
    assert sorted(handle.restarts) == ["valkey-0", "valkey-2"]
    assert sorted(handle.data) == sorted(f"{{s}}:slot-migration-chaos:{n}" for n in range(20))
    [schedule] = evidence_values(outcome.all_evidence, "valkey.chaos.schedule")
    assert [wave["migration"]["state"] for wave in schedule["waves"]] == ["failed", "failed", "cancelled", "success"]
    # The source's log line became the slot-migration.started event, the required evidence of the run.
    assert len(evidence_values(outcome.all_evidence, "valkey.slot-migration.started-logged")) == 1
