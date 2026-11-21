"""The slot migration bundle (``formal/slot-migration``): its controls, how a run's log lines map onto its steps, and,
with java and ``$TLA2TOOLS_JAR``, the counterexample of a cancel after the grant."""

import re
from unittest.mock import patch

import pytest

from tests.conform_support import events, write_trace
from tests.conftest import needs_tlc
from tests.fakes import FakeClient, FakeHandle, fake_instances
from tests.valkey_experiments_support import evidence_values, fake_context, resolved, valkey_experiment
from valkey_fuzzer.components import slot_migration
from valkey_fuzzer.components.slot_migration import MigrateSlotsAction
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.results import ComponentStatus
from valkey_fuzzer.spec.bundle import load_bundle
from valkey_fuzzer.spec.conform import ObservedRun, collect_runs

JOB = "{name: " + "a" * 40 + ", operation: %s, %s_node_id: %s, slots: 7629-7629}"
SOURCE_ID = "b" * 40
TARGET_ID = "c" * 40


def test_every_guard_the_model_relies_on_has_a_control_that_breaks_it():
    bundle = load_bundle("slot-migration")

    assert bundle.claims.status == "draft"
    assert bundle.safe_configs == ("model.cfg",)
    assert {control.name: control.violates for control in bundle.controls} == {
        # Valkey as it is: the safe config assumes a cancel after the grant keeps the source paused, and that the
        # target's replica learns the claim first.
        "cancel_unpauses": "OneWriter",
        "replica_misses_claim": "NoAckedWriteLost",
        # The documented window: the source's pause ends before the target's claim reaches it.
        "pause_expires_after_claim": "OneWriter",
        "grant_without_pause": "OneWriter",
        "grant_after_pause_ends": "OneWriter",
        "drop_unpauses_after_grant": "OneWriter",
        "feed_after_fork": "NoAckedWriteLost",
    }
    assert {control.name for control in bundle.controls if control.module is None} == {
        "cancel_unpauses",
        "replica_misses_claim",
        "pause_expires_after_claim",
    }
    # Both properties need each node's view and data, which a run does not observe: conform reports coverage only.
    assert all(not item.conform and item.conform_note for item in bundle.claims.properties)


def test_the_claims_conditions_are_what_conform_pins():
    # The three constants are fixes Valkey does not have (the cfg-only controls are Valkey as it is); mapping.yaml
    # pins them to the claims' values, so a run is checked as claimed, as cluster-election pins FlushBeforeSave.
    bundle = load_bundle("slot-migration")
    pinned = {name: str(spec["value"]) for name, spec in bundle.mapping.constants.items() if "value" in spec}
    claimed = {"CancelKeepsPause": "TRUE", "ExpiryAfterClaim": "FALSE", "ClaimReachesReplica": "TRUE"}

    assert bundle.property("OneWriter").holds_when == {k: claimed[k] for k in ("CancelKeepsPause", "ExpiryAfterClaim")}
    assert bundle.property("NoAckedWriteLost").holds_when == claimed
    assert {name: pinned[name] for name in claimed} == claimed


@pytest.mark.parametrize(
    "line, evidence",
    [
        ("Slot migration initiated through CLUSTER MIGRATESLOTS command: x (user request from 'id=3')", "started"),
        ("New slot import job created: " + JOB % ("import", "source", SOURCE_ID) + ".", "established"),
        ("New slot import job created: " + JOB % ("import", "source", "unknown") + ".", "tracking"),
        ("Slot migration " + JOB % ("export", "target", TARGET_ID) + " connection lost", "link-lost"),
        (
            "Slot migration " + JOB % ("export", "target", TARGET_ID) + " state transition: "
            "failover-paused -> failover-granted",
            "granted",
        ),
        (
            "Slot migration " + JOB % ("import", "source", SOURCE_ID) + " state transition: "
            "waiting-for-paused -> failover-requested",
            "requested",
        ),
    ],
)
def test_each_log_line_shows_one_step(line, evidence):
    mapping = load_bundle("slot-migration").mapping
    matched = [item.name for item in mapping.evidence.values() if item.log and re.search(item.log, line)]

    assert matched == [evidence]


def test_the_target_losing_the_link_is_not_the_source_losing_it():
    mapping = load_bundle("slot-migration").mapping
    line = "Slot migration " + JOB % ("import", "source", SOURCE_ID) + " connection lost"

    assert not re.search(mapping.event("link-lost").log, line)


def test_conform_maps_a_migration_run_onto_the_steps(tmp_path):
    pairs = [
        ("slot-migration.started", {"instance_id": "valkey-0"}),
        ("slot-migration.import.created", {"instance_id": "valkey-1"}),
        ("slot-migration.tracking.created", {"instance_id": "valkey-2"}),
        ("slot-migration.snapshot.started", {"instance_id": "valkey-0"}),
        ("slot-migration.snapshot.received", {"instance_id": "valkey-1"}),
        ("slot-migration.paused", {"instance_id": "valkey-0"}),
        ("slot-migration.failover.requested", {"instance_id": "valkey-1"}),
        ("slot-migration.failover.granted", {"instance_id": "valkey-0"}),
        ("slot-migration.cancelled", {"instance_id": "valkey-0"}),
        ("slot-migration.claimed", {"instance_id": "valkey-1"}),
        ("slot-migration.owner.learned", {"instance_id": "valkey-0"}),
    ]
    path = write_trace(tmp_path / "trace.jsonl", events(pairs))

    observed = ObservedRun(load_bundle("slot-migration").mapping, collect_runs([path])[0])
    observed.replay()

    assert not observed.unapplied
    assert dict(observed.actions) == {
        "Start": 1,
        "TEstablish": 1,
        "RApply": 1,
        "SFork": 1,
        "TApply": 1,
        "SPause": 1,
        "TPaused": 1,
        "SGrant": 1,
        "Cancel": 1,
        "TClaim": 1,
        "Deliver": 1,
    }
    assert observed.states[-1]["faults"] == 1


class _ClusterHandle(FakeHandle):
    """Two shards of a primary and a replica each; each client answers ``replies`` and records its commands."""

    def __init__(self, replies):
        self.instances = fake_instances(
            4, role=lambda n: "primary" if n % 2 == 0 else "replica", shard_id=lambda n: n // 2,
            cluster_node_id=lambda n: chr(ord("a") + n) * 40,
        )  # fmt: skip
        self.replies, self.commands = replies, {}

    def new_client(self, node_id, timeout=5.0):
        return FakeClient(self.commands.setdefault(node_id, []), self.replies.get(node_id, {}))


def _job(state):
    return ["name", "x" * 40, "operation", "EXPORT", "slot_ranges", "3828-3828", "state", state, "message", ""]


def test_migrate_slots_moves_the_tags_slot_from_its_owner_to_the_shards_primary_and_waits_for_the_export():
    # {s} is slot 3828, in shard 0's range; valkey-2 is shard 1's primary and imports it.
    view = {
        "node_id": "c" * 40,
        "role": "primary",
        "primaries": [{"node_id": "a" * 40, "slots": [[0, 8191]]}, {"node_id": "c" * 40, "slots": [[8192, 16383]]}],
    }
    # The source's export is read until it ends.
    reads = iter([[_job("snapshotting")], [_job("failover-granted")], [_job("success")]])

    class Client(FakeClient):
        def execute_command(self, *args):
            if args == ("CLUSTER", "GETSLOTMIGRATIONS"):
                self.commands.append(args)
                return next(reads)
            return super().execute_command(*args)

    handle = _ClusterHandle({})
    handle.new_client = lambda node_id, timeout=5.0: Client(handle.commands.setdefault(node_id, []))
    spec = resolved(
        ComponentSpec(role=ComponentRole.ACTION, use="valkey.action.migrate-slots", id="migrate", config={"key": "{s}"})
    )
    context = fake_context(handle, spec, deployment_mode="cluster")
    with patch.object(slot_migration, "read_view", lambda handle, instance_id, timeout=2.0: view):
        result = MigrateSlotsAction().run(context, spec)

    assert result.status is ComponentStatus.PASS and "valkey-0 to valkey-2: success" in result.summary
    assert handle.commands["valkey-0"][0] == ("CLUSTER", "MIGRATESLOTS", "SLOTSRANGE", "3828", "3828", "NODE", "c" * 40)
    assert handle.commands["valkey-0"].count(("CLUSTER", "GETSLOTMIGRATIONS")) == 3
    (record,) = evidence_values(result.evidence, "valkey.cluster.slot-migration.result")
    assert (record["slot"], record["source"], record["target"], record["state"]) == (
        3828,
        "valkey-0",
        "valkey-2",
        "success",
    )
    names = [event.name for event in context.event_bus.committed_history()]
    assert names == ["migrate.migration-started", "migrate.migration-completed"]


def test_migrate_slots_does_not_migrate_a_slot_the_target_owns():
    # Shard 1's primary owns the slot already: nothing to migrate, so the run never reached what it tests.
    view = {"node_id": "c" * 40, "role": "primary", "primaries": [{"node_id": "c" * 40, "slots": [[0, 16383]]}]}
    handle = _ClusterHandle({})
    spec = resolved(
        ComponentSpec(role=ComponentRole.ACTION, use="valkey.action.migrate-slots", id="migrate", config={"key": "{s}"})
    )
    with patch.object(slot_migration, "read_view", lambda handle, instance_id, timeout=2.0: view):
        result = MigrateSlotsAction().run(fake_context(handle, spec, deployment_mode="cluster"), spec)
    assert result.status is ComponentStatus.ERROR and "valkey-2 already owns slot 3828" in result.summary
    assert handle.commands == {}


def test_the_slot_migration_experiment_publishes_the_models_steps_and_kills_the_target_at_its_claim():
    experiment = valkey_experiment("valkey.slot-migration-spec")
    components = {component.id: component for component in experiment.components}
    log = components["migration-log"]
    assert (log.use, log.config["spec"]) == ("valkey.observer.log-trigger", "slot-migration")
    patterns = load_bundle("slot-migration").mapping.log_patterns()
    assert sorted(rule["publish"] for rule in log.config["rules"]) == sorted(patterns)
    assert components["trace"].config["spec"] == "slot-migration"
    kill = components["kill-target"].config
    assert (kill["when_event"], kill["instance"], kill["signal"]) == ("slot-migration.claimed", "event", "SIGKILL")
    # The scenario section names this experiment and this fault, so translate's scenarios compile against it.
    scenario = load_bundle("slot-migration").mapping.scenario
    assert scenario["experiment"] == experiment.experiment_id
    assert [(fault["action"], fault["after"], fault["component"]) for fault in scenario["faults"]] == [
        ("CrashT", "claimed", "kill-target")
    ]


@pytest.mark.slow
@needs_tlc
def test_a_cancel_after_the_grant_leaves_two_writers(tla_tools):
    from valkey_fuzzer.spec.check import run_control

    bundle = load_bundle("slot-migration")
    result, _ = run_control(bundle, bundle.control("cancel_unpauses"), tla_tools)

    assert (result.violation, result.violated) == ("invariant", "OneWriter")
    steps = [state.action for state in result.trace]
    # The target claimed the slot when it read FAILOVER-GRANTED; the cancel then unpaused the source.
    assert steps[-3:] == ["SGrant", "TClaim", "Cancel"]


@pytest.mark.slow
@needs_tlc
def test_a_replica_promoted_without_the_claim_deletes_the_imported_slot(tla_tools):
    from valkey_fuzzer.spec.check import run_control

    bundle = load_bundle("slot-migration")
    result, _ = run_control(bundle, bundle.control("replica_misses_claim"), tla_tools)

    assert (result.violation, result.violated) == ("invariant", "NoAckedWriteLost")
    steps = [state.action for state in result.trace]
    # The trace issue #140 reproduces on unstable: the source learned the claim and deleted its keys, the target
    # died, its claim to the replica was lost, and the replica, promoted with the source as owner in its view,
    # deleted the keys it had applied.
    assert "TClaim" in steps and "RApply" in steps[steps.index("TClaim") :]
    assert steps[-3:] == ["CrashT", "LoseBus", "Promote"]
    last = result.trace[-1].values
    assert last["view"]["R"]["owner"] == "S" and last["data"]["S"] == () and last["data"]["R"] == ()
