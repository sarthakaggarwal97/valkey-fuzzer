"""Live feature-pack runs against real valkey-server processes.

Skipped unless a valkey-server binary is available (``VALKEY_FUZZER_BINARY`` or
``valkey-server`` on PATH). Forkless tests also need a build with forkless
save; mixed-version and rolling-upgrade tests need ``VALKEY_FUZZER_OLD_BINARY``
(a second, older build). Run only these with ``pytest -m live``.
"""

import copy
import os
import shutil
import subprocess
import time

import pytest
import yaml

from tests.valkey_packs_support import valkey_entry_points, valkey_runtime
from valkey_fuzzer.campaign import Campaign, CoverageDB, run_campaign
from valkey_fuzzer.differential import Differential, run_differential
from valkey_fuzzer.extensibility import OutcomeStatus, to_plain_data
from valkey_fuzzer.feature_commands import run_report
from valkey_fuzzer_packs import PACK_DIRECTORY, network, providers
from valkey_fuzzer_packs.capabilities import probe_binary

pytestmark = [pytest.mark.live, pytest.mark.slow]

BINARY = os.environ.get("VALKEY_FUZZER_BINARY") or shutil.which("valkey-server")
OLD_BINARY = os.environ.get("VALKEY_FUZZER_OLD_BINARY")

if not BINARY:
    pytest.skip("valkey-server is not available", allow_module_level=True)


def _supports_forkless() -> bool:
    status, _, _ = probe_binary(
        BINARY,
        {
            "startup_config": {"forkless-infrastructure-enabled": "yes", "bgsave-default-method": "forkless"},
            "config_get": {"bgsave-default-method": ["forkless"]},
            "info_fields": {"persistence": ["rdb_current_bgsave_type"]},
            "commands": [],
            "min_version": None,
            "timeout_s": 15.0,
        },
    )
    return status.value == "PASS"


FORKLESS = _supports_forkless()
needs_forkless = pytest.mark.skipif(not FORKLESS, reason="valkey-server build lacks forkless save")
needs_old_binary = pytest.mark.skipif(
    not OLD_BINARY, reason="set VALKEY_FUZZER_OLD_BINARY to a second valkey-server build"
)

_PORTS = iter(range(8000, 30000, 40))


@pytest.fixture
def runtime(tmp_path):
    return valkey_runtime(artifact_dir=str(tmp_path / "artifacts"))


def _scenario(pack, topology, inputs=None, seed=7, version="1", **topology_extra):
    return {
        "scenario_id": f"live-{pack.split('.')[-1]}-{topology}",
        "seed": seed,
        "feature": {"use": pack, "version": version, "with": inputs or {}},
        "topology": {"type": topology, "base_port": next(_PORTS), "valkey_binary": BINARY, **topology_extra},
    }


def _builds(scenario, baseline, candidate):
    scenario["artifacts"] = {
        "baseline": {"use": "valkey.local-binary", "with": {"path": baseline}},
        "candidate": {"use": "valkey.local-binary", "with": {"path": candidate}},
    }
    return scenario


def _run(runtime, scenario):
    plan = runtime.compile(scenario)
    outcome = runtime.run_plan(plan, deadline=time.monotonic() + 600)
    return outcome, run_report(runtime, scenario, outcome)


def _explain(outcome):
    lines = [f"{outcome.status.value}: {outcome.summary}"]
    lines += [
        f"  {item.component_instance_id}: {item.status.value} {item.summary}" for item in outcome.component_results
    ]
    return "\n".join(lines)


def _evidence_ids(outcome):
    return {item.evidence_id for item in outcome.all_evidence}


def _evidence(outcome, evidence_id):
    return next(to_plain_data(item.value) for item in outcome.all_evidence if item.evidence_id == evidence_id)


def _variant_runtime(tmp_path, pack_file, version, edit):
    """A runtime with one pack copied into a manifest directory and edited."""

    document = yaml.safe_load((PACK_DIRECTORY / pack_file).read_text(encoding="utf-8"))
    document["version"] = version
    edit(document)
    directory = tmp_path / "packs"
    directory.mkdir(exist_ok=True)
    (directory / pack_file).write_text(yaml.safe_dump(document), encoding="utf-8")
    return valkey_runtime(feature_pack_directories=(str(directory),), artifact_dir=str(tmp_path / "artifacts"))


# ---------------------------------------------------------------------------
# Forkless save
# ---------------------------------------------------------------------------


@needs_forkless
@pytest.mark.parametrize("topology", ["standalone", "replication", "cluster"])
def test_forkless_save_passes_with_path_evidence(runtime, topology):
    outcome, report = _run(runtime, _scenario("valkey.forkless-save", topology))
    assert outcome.status is OutcomeStatus.PASS, _explain(outcome)
    assert {
        "valkey.forkless.in_progress",
        "valkey.forkless.log_completed",
        "valkey.rdb.reload_verified",
    } <= _evidence_ids(outcome)
    assert report["coverage"]["tuple"]["topology"]["type"] == topology
    assert report["reproducer"]["plugins"][0]["plugin_id"] == "valkey-fuzzer.valkey-packs"
    nodes = report["outcome"]["diagnostics"]["topology"]["nodes"]
    assert all(node["binary_sha256"] for node in nodes)


@needs_forkless
def test_configured_but_unexercised_feature_fails(tmp_path):
    """Negative control: same evidence contract, but the save runs with fork."""

    def save_with_fork(document):
        document["startup_overlays"][0]["server_config"]["bgsave-default-method"] = "fork"

    runtime = _variant_runtime(tmp_path, "valkey.forkless-save.yaml", "negative-control", save_with_fork)
    outcome, _ = _run(runtime, _scenario("valkey.forkless-save", "standalone", version="negative-control"))
    assert outcome.status is OutcomeStatus.FAIL, _explain(outcome)
    missing = {item["evidence_id"] for item in to_plain_data(outcome.diagnostics)["missing_required_evidence"]}
    assert {"valkey.forkless.in_progress", "valkey.forkless.log_started"} <= missing


@needs_forkless
@pytest.mark.parametrize("topology", ["standalone", "replication", "cluster"])
def test_forkless_save_cancel_aborts_cleanly(runtime, topology):
    outcome, _ = _run(runtime, _scenario("valkey.forkless-save-cancel", topology))
    assert outcome.status is OutcomeStatus.PASS, _explain(outcome)
    assert "valkey.forkless.log_cancelled" in _evidence_ids(outcome)


@needs_forkless
@pytest.mark.parametrize("topology", ["standalone", "replication", "cluster"])
def test_forkless_save_crash_recovers(runtime, topology):
    outcome, _ = _run(runtime, _scenario("valkey.forkless-save-crash", topology))
    assert outcome.status is OutcomeStatus.PASS, _explain(outcome)
    assert _evidence(outcome, "valkey.fault.applied")["restarted"] is True


# ---------------------------------------------------------------------------
# Durability and storage
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("topology", ["standalone", "replication"])
def test_aof_always_survives_sigkill(runtime, topology):
    outcome, _ = _run(runtime, _scenario("valkey.aof-durability", topology))
    assert outcome.status is OutcomeStatus.PASS, _explain(outcome)


def test_durability_oracle_detects_lost_writes(tmp_path):
    """Negative control: without AOF a crash loses acknowledged writes."""

    def disable_aof(document):
        document["startup_overlays"][0]["server_config"]["appendonly"] = "no"

    runtime = _variant_runtime(tmp_path, "valkey.aof-durability.yaml", "no-aof-control", disable_aof)
    outcome, _ = _run(runtime, _scenario("valkey.aof-durability", "standalone", version="no-aof-control"))
    assert outcome.status is OutcomeStatus.FAIL, _explain(outcome)
    durability = next(item for item in outcome.component_results if item.component_instance_id == "durability")
    assert "lost" in durability.summary


def _disk_full_scenario():
    scenario = _scenario("valkey.bgsave-disk-full", "standalone")
    scenario["resources"] = {"disk": {"use": "valkey.fs.tmpfs", "with": {"size_mb": 32}}}
    scenario["feature"]["with"] = {"disk": {"$ref": "resources.disk.path"}}
    scenario["topology"]["node_data_roots"] = {"primary": {"$ref": "resources.disk.path"}}
    return scenario


@pytest.mark.skipif(not providers.tmpfs_supported()[0], reason=providers.tmpfs_supported()[1] or "no tmpfs")
def test_disk_full_degrades_gracefully_and_recovers(runtime):
    outcome, _ = _run(runtime, _disk_full_scenario())
    assert outcome.status is OutcomeStatus.PASS, _explain(outcome)


def test_disk_full_is_unsupported_without_tmpfs(runtime, monkeypatch):
    monkeypatch.setattr(providers, "tmpfs_supported", lambda: (False, "requires Linux"))
    outcome, _ = _run(runtime, _disk_full_scenario())
    assert outcome.status is OutcomeStatus.SKIP_UNSUPPORTED, _explain(outcome)


# ---------------------------------------------------------------------------
# Replication and builds
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("pack", ["valkey.replication-partial-resync", "valkey.replication-full-resync"])
@pytest.mark.parametrize("topology", ["replication", "cluster"])
def test_replication_resync(runtime, pack, topology):
    outcome, _ = _run(runtime, _scenario(pack, topology))
    assert outcome.status is OutcomeStatus.PASS, _explain(outcome)


@needs_old_binary
def test_mixed_version_cluster_failover(runtime):
    scenario = _builds(_scenario("valkey.mixed-version-cluster", "cluster"), OLD_BINARY, BINARY)
    scenario["topology"]["valkey_binary"] = {"$ref": "artifacts.candidate.binary"}
    scenario["topology"]["node_binaries"] = {"replica": {"$ref": "artifacts.baseline.binary"}}
    outcome, report = _run(runtime, scenario)
    assert outcome.status is OutcomeStatus.PASS, _explain(outcome)
    nodes = report["outcome"]["diagnostics"]["topology"]["nodes"]
    assert len({node["binary_sha256"] for node in nodes}) == 2
    assert _evidence(outcome, "valkey.binary.verified")["mixed"] is True


def test_mixed_version_pack_is_unsupported_with_one_build(runtime):
    outcome, _ = _run(runtime, _scenario("valkey.mixed-version-cluster", "cluster"))
    assert outcome.status is OutcomeStatus.SKIP_UNSUPPORTED, _explain(outcome)


@needs_old_binary
@pytest.mark.parametrize("topology", ["replication", "cluster"])
def test_rolling_upgrade_under_writes(runtime, topology):
    scenario = _builds(
        _scenario("valkey.rolling-upgrade", topology, inputs={"to_binary": {"$ref": "artifacts.candidate.binary"}}),
        OLD_BINARY,
        BINARY,
    )
    scenario["topology"]["valkey_binary"] = {"$ref": "artifacts.baseline.binary"}
    outcome, report = _run(runtime, scenario)
    assert outcome.status is OutcomeStatus.PASS, _explain(outcome)
    steps = _evidence(outcome, "valkey.upgrade.completed")["steps"]
    restarts = [step for step in steps if step["kind"] == "restart"]
    assert len(restarts) == len(report["outcome"]["diagnostics"]["topology"]["nodes"])
    assert all(step["before"]["version"] != step["after"]["version"] for step in restarts)


# ---------------------------------------------------------------------------
# Network partitions
# ---------------------------------------------------------------------------

_PARTITIONS = network.iptables_supported()[0] and network.per_node_loopback_supported()[0]


@pytest.mark.skipif(not _PARTITIONS, reason="needs Linux root with iptables and 127.0.0.x addresses")
def test_cluster_partition_fails_over_heals_and_bounds_loss(runtime):
    scenario = _scenario("valkey.cluster-partition", "cluster", addressing="per_node_loopback")
    outcome, _ = _run(runtime, scenario)
    assert outcome.status is OutcomeStatus.PASS, _explain(outcome)
    assert _evidence(outcome, "valkey.data.ledger_verified")["loss_scope_slots"] == [[0, 5460]]
    rules = subprocess.run(["iptables", "-S"], capture_output=True, text=True).stdout
    assert "VF-" not in rules, "partition chains must be removed"


def test_cluster_partition_is_unsupported_without_prerequisites(runtime, monkeypatch):
    monkeypatch.setattr(network, "iptables_supported", lambda: (False, "network partitions require Linux iptables"))
    outcome, _ = _run(runtime, _scenario("valkey.cluster-partition", "cluster", addressing="per_node_loopback"))
    assert outcome.status is OutcomeStatus.SKIP_UNSUPPORTED, _explain(outcome)


# ---------------------------------------------------------------------------
# Differential runs, campaigns, and data-only manifests
# ---------------------------------------------------------------------------


_DIGESTS = [
    {"evidence_id": "valkey.dataset.digest", "source": "dataset", "field": "digest"},
    {"evidence_id": "valkey.rdb.reload_verified", "source": "snapshot", "field": "content_digest"},
    {"evidence_id": "valkey.workload.ledger", "source": "workload", "field": "replies_digest"},
]


def _differential(runtime, document):
    def execute(name, scenario, plan):
        return runtime.run_plan(plan, deadline=time.monotonic() + 600), None

    return run_differential(Differential.from_document(document), compile_fn=runtime.compile, execute_fn=execute)


def _snapshot_base(topology, **inputs):
    return _scenario("valkey.snapshot-equivalence", topology, inputs={"operations": 1500, **inputs})


@needs_forkless
def test_differential_fork_vs_forkless_is_equivalent(runtime):
    forkless = {"forkless-infrastructure-enabled": "yes"}
    report = _differential(
        runtime,
        {
            "differential_id": "live-fork-forkless",
            "base": _snapshot_base("standalone"),
            "variants": {
                "fork": {"topology.server_config": {**forkless, "bgsave-default-method": "fork"}},
                "forkless": {"topology.server_config": {**forkless, "bgsave-default-method": "forkless"}},
            },
            "compare": {
                "equal": _DIGESTS,
                "differ": [
                    {"evidence_id": "valkey.bgsave.result", "source": "bgsave", "field": "after.rdb_last_bgsave_type"}
                ],
            },
        },
    )
    assert report["status"] == "PASS", report["summary"]


def test_differential_detects_divergent_behaviour(runtime):
    """Negative control: changing the generated data must break equality."""

    report = _differential(
        runtime,
        {
            "differential_id": "live-divergent",
            "base": _snapshot_base("standalone"),
            "variants": {"small": {}, "large": {"feature.with.value_size": 65}},
            "compare": {"equal": _DIGESTS[:1]},
        },
    )
    assert report["status"] == "FAIL"
    assert report["comparisons"][0]["result"] == "mismatch"


@needs_old_binary
def test_differential_baseline_vs_candidate_cluster(runtime):
    base = _builds(_snapshot_base("cluster"), OLD_BINARY, BINARY)
    base["topology"].pop("valkey_binary")
    report = _differential(
        runtime,
        {
            "differential_id": "live-builds",
            "base": base,
            "variants": {
                "baseline": {"topology.valkey_binary": {"$ref": "artifacts.baseline.binary"}},
                "candidate": {"topology.valkey_binary": {"$ref": "artifacts.candidate.binary"}},
            },
            "compare": {"equal": _DIGESTS},
        },
    )
    assert report["status"] == "PASS", report["summary"]


@needs_forkless
def test_live_campaign_records_coverage(runtime, tmp_path):
    base = _scenario("valkey.forkless-save", "standalone", inputs={"stable_keys": 500, "key_save_delay_us": 2000})
    campaign = Campaign.from_document(
        {
            "campaign_id": "live",
            "seed": 1,
            "base": {"feature": base["feature"], "topology": base["topology"]},
            "matrix": {"feature.with.mutation_rate": [0, 500]},
        }
    )

    def execute(case):
        scenario = copy.deepcopy(case.scenario)
        scenario["topology"]["base_port"] = next(_PORTS)
        return runtime.run_plan(runtime.compile(scenario), deadline=time.monotonic() + 300)

    coverage = CoverageDB(str(tmp_path / "coverage.json"))
    summary = run_campaign(campaign, compile_fn=runtime.compile, execute_fn=execute, coverage=coverage)
    assert summary["statuses"] == {"PASS": 2}
    assert summary["coverage"]["tuples"] == 2


RESTART_PACK = """
pack_id: tests.restart-under-writes
version: "1"
supported_topologies: [standalone, replication]
components:
  - kind: workload
    component_id: valkey.kv_workload
    instance_id: writes
    config: {stable_keys: 200, mutable_keys: 50, rate: 200}
  - kind: action
    component_id: valkey.wait_for
    instance_id: warmup
    depends_on: [writes]
    config: {seconds: 0.5}
  - kind: action
    component_id: valkey.restart_node
    instance_id: restart
    depends_on: [warmup]
    config: {node: primary}
  - kind: action
    component_id: valkey.command
    instance_id: dbsize
    depends_on: [restart]
    config: {nodes: primary, command: [DBSIZE], expect: {min: 200}}
  - kind: oracle
    component_id: valkey.ledger
    instance_id: data
    config: {workload: writes}
required_events: [node.restarted]
required_evidence:
  - {evidence_id: valkey.node.restarted, source: restart}
  - {evidence_id: valkey.data.ledger_verified, source: data}
"""


@pytest.mark.parametrize("topology", ["standalone", "replication"])
def test_data_only_manifest_runs_live(tmp_path, topology):
    """A manifest composed only of library components, loaded from a pack directory."""

    packs = tmp_path / "packs"
    packs.mkdir()
    (packs / "restart.yaml").write_text(RESTART_PACK)
    with valkey_entry_points():
        runtime = valkey_runtime(feature_pack_directories=(str(packs),), artifact_dir=str(tmp_path / "a"))
    outcome, _ = _run(runtime, _scenario("tests.restart-under-writes", topology))
    assert outcome.status is OutcomeStatus.PASS, _explain(outcome)
