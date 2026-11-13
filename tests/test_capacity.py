"""Host-sized draws: the cores a run may use, what a deployment needs of them, and draws that fit."""

import json
from pathlib import Path

import pytest

from valkey_fuzzer import capacity
from valkey_fuzzer.deployment.local.provider import IO_THREAD_CORES, MAIN_THREAD_CORES, core_demand
from valkey_fuzzer.generator import Exclusion
from valkey_fuzzer.results import OutcomeStatus, RunOutcome
from valkey_fuzzer.service import RunService

from .valkey_experiments_support import unit_plan, valkey_service

CLUSTER_CHAOS = "valkey.cluster-chaos"
NO_ZSTD = (Exclusion.parse("server_config.rdbcompression=zstd"),)


@pytest.mark.parametrize(
    "files, cores",
    [
        ({}, 24.0),
        ({"v2": "max 100000"}, 24.0),
        ({"v2": "200000 100000"}, 2.0),
        ({"v2": "4800000 100000"}, 24.0),
        ({"quota": "-1", "period": "100000"}, 24.0),
        ({"quota": "150000", "period": "100000"}, 1.5),
        ({"quota": "garbled", "period": "100000"}, 24.0),
    ],
)
def test_a_run_may_use_its_cpusets_cores_or_fewer_when_its_cgroup_has_a_lower_quota(
    tmp_path, monkeypatch, files, cores
):
    monkeypatch.setattr(capacity.os, "sched_getaffinity", lambda pid: set(range(24)))
    for name, constant in (("v2", "CGROUP_V2_CPU_MAX"), ("quota", "CGROUP_V1_QUOTA"), ("period", "CGROUP_V1_PERIOD")):
        path = tmp_path / name
        if name in files:
            path.write_text(files[name] + "\n")
        monkeypatch.setattr(capacity, constant, str(path))
    assert capacity.usable_cores() == cores


def test_a_deployment_needs_a_core_share_for_each_main_thread_and_more_for_each_io_thread():
    document = {
        "mode": "cluster",
        "primaries": 3,
        "replicas_per_primary": 1,
        "instance_configs": {"primary": {"io-threads": "4"}, "replica": {"io-threads": 1}},
    }
    plan = unit_plan(deployment_mode="cluster", document=document)
    assert core_demand(plan) == pytest.approx(6 * MAIN_THREAD_CORES + 3 * 3 * IO_THREAD_CORES)
    # The provider's default (one thread) and a value the server would refuse count as one thread.
    assert core_demand(unit_plan(document={"mode": "standalone"})) == pytest.approx(MAIN_THREAD_CORES)
    garbled = unit_plan(document={"mode": "standalone", "server_config": {"io-threads": "many"}})
    assert core_demand(garbled) == pytest.approx(MAIN_THREAD_CORES)


def _demand(generated):
    return core_demand(generated.plan)


def test_a_draw_whose_deployment_needs_more_cores_than_the_run_may_use_is_drawn_smaller():
    # Known-bad: unbounded, cluster chaos draws clusters that need far more than four cores.
    unbounded = valkey_service()
    seeds = range(40)
    assert max(_demand(unbounded.generate(seed, experiment=CLUSTER_CHAOS, excludes=NO_ZSTD)) for seed in seeds) > 4
    bounded = valkey_service(cores=4)
    redrawn = 0
    for seed in seeds:
        drawn = bounded.generate(seed, experiment=CLUSTER_CHAOS, excludes=NO_ZSTD)
        assert _demand(drawn) <= 4 and drawn.provenance["cores"] == 4 and drawn.provenance["shortfall"] is None
        if drawn.attempt:
            redrawn += 1
        else:
            # A first attempt that fits is the scenario the seed draws on any host.
            assert drawn.scenario == unbounded.generate(seed, experiment=CLUSTER_CHAOS, excludes=NO_ZSTD).scenario
    assert redrawn


def test_a_draw_no_attempt_of_which_fits_is_its_first_attempt_and_its_run_is_skipped_saying_why(tmp_path, monkeypatch):
    service = valkey_service(cores=0.1, artifact_dir=str(tmp_path))
    drawn = service.generate(3, experiment=CLUSTER_CHAOS, excludes=NO_ZSTD)
    assert drawn.attempt == 0
    assert drawn.scenario == valkey_service().generate(3, experiment=CLUSTER_CHAOS, excludes=NO_ZSTD).scenario
    assert drawn.shortfall.endswith("and this run may use 0.1") and drawn.provenance["shortfall"] == drawn.shortfall

    def run_plan(*args, **kwargs):
        raise AssertionError("a draw that does not fit must not run")

    monkeypatch.setattr(RunService, "run_plan", run_plan)
    record = service.execute(drawn.plan, scenario=drawn.scenario, report_dir=str(tmp_path), generated=drawn.provenance)
    assert record.outcome.status is OutcomeStatus.SKIP_UNSUPPORTED
    assert record.outcome.summary == f"no draw fits the cores this run may use: {drawn.shortfall}"
    report = json.loads(Path(tmp_path, "report.json").read_text())
    assert report["status"] == "SKIP_UNSUPPORTED" and report["reproducer"]["generated"]["cores"] == 0.1
    assert Path(tmp_path, "scenario.yaml").exists()


def test_a_scenario_read_from_a_file_runs_as_written_whatever_it_needs(tmp_path, monkeypatch):
    drawn = valkey_service().generate(3, experiment=CLUSTER_CHAOS, excludes=NO_ZSTD)
    service = valkey_service(cores=0.1, artifact_dir=str(tmp_path))
    plan = service.compile(drawn.scenario)
    assert capacity.shortfall(plan, service.catalog, 0.1) is not None
    ran = []

    def run_plan(self, plan, *, timeout=None, run_dir=None):
        ran.append(plan.scenario_id)
        return RunOutcome(plan=plan, status=OutcomeStatus.PASS, summary="experiment passed")

    monkeypatch.setattr(RunService, "run_plan", run_plan)
    record = service.execute(plan, scenario=drawn.scenario, report_dir=str(tmp_path))
    assert ran == [plan.scenario_id] and record.outcome.status is OutcomeStatus.PASS
