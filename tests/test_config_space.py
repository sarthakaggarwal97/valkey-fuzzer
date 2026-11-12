"""The config space: Valkey's config table read from a build's own config.c, what a scenario draws from it, and the
config-churn workload."""

import os
import random
import shutil
import threading
from dataclasses import replace
from pathlib import Path

import pytest
import valkey

from valkey_fuzzer.components import config_space, server_stats
from valkey_fuzzer.components.config_space import (
    CHURN_EVIDENCE,
    EXCLUDED,
    ConfigChurnWorkload,
    ServerConfig,
    build_config_table,
    config_space_pool,
    parse_config_table,
    rule,
    scenario_builds,
)
from valkey_fuzzer.deployment.local import processes
from valkey_fuzzer.generator import draw_rule
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.results import make_evidence

from .live_support import BINARY

FIXTURE = Path(__file__).parent / "fixtures" / "config_space"
INT_MAX, LLONG_MAX = 2**31 - 1, 2**63 - 1


def _table():
    return parse_config_table((FIXTURE / "config.c").read_text(), [(FIXTURE / "server.h").read_text()])


def stand_in_build(directory: Path) -> str:
    """A build tree whose ``src`` holds the fixture's config.c beside a ``valkey-server``; its binary's path."""

    source = directory / "src"
    source.mkdir(parents=True)
    for name in ("config.c", "server.h"):
        shutil.copy(FIXTURE / name, source / name)
    binary = source / "valkey-server"
    binary.write_text("#!/bin/sh\n")
    binary.chmod(0o755)
    return str(binary)


def test_the_table_holds_every_entry_with_its_type_flags_bounds_and_default():
    configs = {config.name: config for config in _table()}
    # An entry inside #ifdef depends on how the build was made; the comma in a comment splits nothing.
    assert list(configs) == [
        "rdbchecksum", "lazyfree-lazy-eviction", "io-threads-always-active", "lua-enable-insecure-api", "dbfilename",
        "maxmemory-policy", "acl-pubsub-default", "shutdown-on-sigterm", "io-threads", "hz", "list-max-listpack-size",
        "unixsocketperm", "commandlog-request-larger-than", "maxmemory", "maxmemory-clients", "repl-backlog-ttl",
        "lfu-log-factor", "watchdog-period", "save",
    ]  # fmt: skip
    assert configs["io-threads"] == ServerConfig(
        "io-threads", "numeric", frozenset({"DEBUG", "IMMUTABLE"}), minimum=1, maximum=256, default=1
    )
    assert configs["rdbchecksum"] == ServerConfig("rdbchecksum", "bool", frozenset({"IMMUTABLE"}), default="yes")
    insecure = configs["lua-enable-insecure-api"]
    assert insecure.alias == "lua-enable-deprecated-api" and insecure.flags == {"HIDDEN", "PROTECTED"}
    policy = configs["maxmemory-policy"]
    assert policy.values == ("volatile-lru", "allkeys-lru", "noeviction") and policy.default == "noeviction"
    # A default given as a number is the value whose constant has it.
    assert configs["acl-pubsub-default"].default == "resetchannels"
    assert configs["shutdown-on-sigterm"].flags == {"MULTI_ARG"}
    assert (configs["hz"].minimum, configs["hz"].maximum, configs["hz"].default) == (0, INT_MAX, 10)
    assert configs["list-max-listpack-size"].minimum == -(2**31)
    assert configs["unixsocketperm"].maximum == 0o777 and configs["unixsocketperm"].number_flags == {"OCTAL"}
    request = configs["commandlog-request-larger-than"]
    assert (request.minimum, request.maximum, request.default) == (-1, LLONG_MAX, 1024 * 1024)
    assert request.number_flags == {"MEMORY", "SIGNED_MEMORY"}
    assert configs["maxmemory"].maximum == 2**64 - 1 and configs["repl-backlog-ttl"].default == 3600
    # A constant this parser cannot evaluate leaves the value unknown.
    assert configs["lfu-log-factor"].default is None and configs["watchdog-period"].maximum is None
    assert (configs["dbfilename"].kind, configs["save"].kind) == ("string", "special")
    assert configs["io-threads-always-active"].mutable and not configs["rdbchecksum"].mutable
    assert not insecure.mutable


def test_the_space_leaves_out_what_it_cannot_draw_and_what_is_excluded():
    space = config_space.config_space([_table()])
    # Excluded (rdbchecksum, acl-pubsub-default, maxmemory, ...), the deployment's own (unixsocketperm), protected,
    # a string, multi-argument, special, or a number without known bounds (watchdog-period) are not in it.
    assert set(space) == {
        "lazyfree-lazy-eviction", "io-threads-always-active", "maxmemory-policy", "io-threads",
        "list-max-listpack-size", "commandlog-request-larger-than", "lfu-log-factor",
    }  # fmt: skip
    assert not set(space) & set(EXCLUDED)
    # A hand-written pool draws hz, with values chosen for it.
    assert "hz" in EXCLUDED
    assert {"rdbchecksum", "acl-pubsub-default", "maxmemory", "maxmemory-clients"} <= set(EXCLUDED)
    # The deployment sets unixsocketperm itself, and gives every instance its own maxmemory.
    assert {"unixsocketperm", "maxmemory", "port", "dir"} <= config_space._deployment_owned()


def test_a_scenario_of_several_builds_draws_what_every_one_of_them_has():
    table = _table()
    older = [
        replace(config, values=("volatile-lru", "noeviction")) if config.name == "maxmemory-policy" else
        replace(config, maximum=500) if config.name == "lfu-log-factor" else config
        for config in table
        if config.name != "io-threads-always-active"
    ]  # fmt: skip
    space = config_space.config_space([table, older])
    assert "io-threads-always-active" not in space
    assert space["maxmemory-policy"].values == ("volatile-lru", "noeviction")
    assert (space["lfu-log-factor"].minimum, space["lfu-log-factor"].maximum) == (0, 500)
    assert config_space.config_space([]) == {}


def test_each_config_draws_values_of_its_type_toward_its_bounds_its_default_and_small_values():
    configs = {config.name: config for config in _table()}
    assert rule(configs["lazyfree-lazy-eviction"]) == ["no", "yes"]
    assert rule(configs["maxmemory-policy"]) == ["volatile-lru", "allkeys-lru", "noeviction"]
    assert rule(configs["hz"]) == {"minimum": 0, "maximum": INT_MAX, "default": 10}
    # A negative percentage is set only as "N%": the range starts at 0. A default the parser could not read is left out.
    assert rule(configs["maxmemory-clients"]) == {"minimum": 0, "maximum": LLONG_MAX, "default": 0}
    assert rule(configs["lfu-log-factor"]) == {"minimum": 0, "maximum": INT_MAX}
    # A bound above LLONG_MAX is one Valkey cannot read unless the config is flagged UNSIGNED_CONFIG.
    assert rule(configs["maxmemory"])["maximum"] == LLONG_MAX
    unsigned = replace(configs["maxmemory"], number_flags=frozenset({"MEMORY", "UNSIGNED"}))
    assert rule(unsigned)["maximum"] == 2**64 - 1
    rng = random.Random(7)
    values = [draw_rule(rule(configs["hz"]), rng, "hz") for _ in range(500)]
    assert all(isinstance(value, int) and 0 <= value <= INT_MAX for value in values)
    assert {0, 1, 10, INT_MAX - 1, INT_MAX} <= set(values)
    # Log-uniform between the edges, so small values are not starved by a range of two billion.
    assert any(1 < value < 1000 and value != 10 for value in values)
    assert any(10**6 < value < INT_MAX - 1 for value in values)


def test_a_build_reads_the_config_c_beside_its_binary(tmp_path):
    binary = stand_in_build(tmp_path / "tree")
    assert build_config_table(binary) == _table()
    # A link to the binary reads the tree it links into.
    os.symlink(binary, tmp_path / "valkey-server")
    assert build_config_table(str(tmp_path / "valkey-server")) == _table()
    # A binary copied out of its tree, and a missing one, have no table.
    installed = tmp_path / "bin" / "valkey-server"
    installed.parent.mkdir()
    shutil.copy(binary, installed)
    assert build_config_table(str(installed)) is None and build_config_table(str(tmp_path / "nope")) is None


def test_a_scenario_names_its_builds_and_the_default_build_when_it_names_none(monkeypatch):
    scenario = {
        "deployment": {
            "valkey_binary": {"$ref": "artifacts.old.binary"},
            "instance_binaries": {"replica": "/b/src/valkey-server"},
        },
        "artifacts": {
            "old": {"use": "valkey.artifact.local-binary", "with": {"path": "/a/src/valkey-server"}},
            "module": {"use": "valkey.artifact.local-module", "with": {"path": "/c/module.so"}},
        },
    }
    assert scenario_builds(scenario) == ["/a/src/valkey-server", "/b/src/valkey-server"]
    monkeypatch.setattr(processes, "installed_valkey_binary", lambda: "/usr/bin/valkey-server")
    assert scenario_builds({"deployment": {}}) == ["/usr/bin/valkey-server"]
    monkeypatch.setattr(processes, "installed_valkey_binary", lambda: None)
    assert scenario_builds({"deployment": {}}) == []


def test_the_pool_hook_offers_the_space_of_the_scenarios_builds_or_nothing_when_one_has_no_table(tmp_path):
    binary = stand_in_build(tmp_path / "tree")
    offered = config_space_pool(random.Random(1), {"deployment": {"valkey_binary": binary}})
    assert offered == {name: rule(config) for name, config in config_space.config_space([_table()]).items()}
    installed = tmp_path / "bin" / "valkey-server"
    installed.parent.mkdir()
    shutil.copy(binary, installed)
    two = {"deployment": {"valkey_binary": binary, "instance_binaries": {"replica": str(installed)}}}
    assert config_space_pool(random.Random(1), two) == {}


@pytest.mark.skipif(not BINARY or build_config_table(BINARY) is None, reason="needs a valkey-server built in its tree")
def test_every_excluded_config_is_one_the_build_under_test_has():
    """Holds :data:`EXCLUDED` to the build: a name it does not have is a typo, or a config Valkey removed."""

    names = {config.name for config in build_config_table(BINARY)}
    assert len(names) > 150
    # Configs of builds made with other options (RDMA, TLS) are in every table; these are newer than some builds.
    assert sorted(set(EXCLUDED) - names) == []


# ---------------------------------------------------------------------------
# The config-churn workload
# ---------------------------------------------------------------------------


class _Instance:
    def __init__(self, instance_id):
        self.instance_id = instance_id


class _Client:
    def __init__(self, deployment, instance_id):
        self.deployment, self.instance_id = deployment, instance_id

    def config_set(self, name, value):
        if self.instance_id in self.deployment.down:
            raise valkey.exceptions.ConnectionError("Connection refused")
        if name in self.deployment.refused:
            raise valkey.exceptions.ResponseError(f"CONFIG SET failed (possibly related to argument '{name}')")
        self.deployment.sets.append((self.instance_id, name, value))
        return True

    def close(self):
        pass


class _Deployment:
    def __init__(self, binary):
        self.instances = [_Instance("valkey-0"), _Instance("valkey-1")]
        self.binaries = {"valkey-0": binary, "valkey-1": binary}
        self.sets, self.refused, self.down = [], set(), set()

    def binary(self, instance_id):
        return self.binaries[instance_id]

    def new_client(self, instance_id, timeout=5.0):
        return _Client(self, instance_id)


class _Plan:
    def __init__(self, instances):
        self.instances = instances


class _Context:
    def __init__(self, handle, planned):
        self.services = {"deployment": {"valkey.deployment": handle}}
        self.plan = _Plan({instance_id: {"server_config": config} for instance_id, config in planned.items()})

    def rng(self, component_id, stream="default"):
        return random.Random(f"{component_id}\0{stream}")


def _spec(**config):
    return ComponentSpec(
        "valkey.workload.config-churn",
        "config-churn",
        ComponentRole.WORKLOAD,
        config={"changes": 0, "interval_s": 0.05, "op_timeout_s": 1.0, **config},
    )


def _churn(context, spec):
    workload = ConfigChurnWorkload()
    assert workload.start(context, spec).status.value == "PASS"
    done = threading.Event()
    for _ in range(200):
        if len(workload._changes) >= spec.config["changes"]:
            break
        done.wait(0.05)
    return workload.stop(context, spec)


def test_the_churn_sets_only_mutable_configs_of_the_space_the_planned_config_leaves_to_the_default(tmp_path):
    handle = _Deployment(stand_in_build(tmp_path / "tree"))
    planned = {"valkey-0": {"hz": "10", "maxmemory-policy": "noeviction"}, "valkey-1": {"lfu-log-factor": "5"}}
    result = _churn(_Context(handle, planned), _spec(changes=40))
    assert result.status.value == "PASS" and len(handle.sets) == 40, result.summary
    space = config_space.config_space([_table()])
    for instance_id, name, value in handle.sets:
        # io-threads is immutable; the planned config's keys are the experiment's or the generator's.
        assert name in space and name != "io-threads" and name not in planned[instance_id]
        config = space[name]
        if config.kind == "numeric":
            assert config.minimum <= value <= config.maximum
        else:
            assert value in rule(config)
    assert {instance_id for instance_id, _, _ in handle.sets} == {"valkey-0", "valkey-1"}
    (evidence,) = result.evidence
    assert evidence.evidence_id == CHURN_EVIDENCE and evidence.value["errors"] == 0
    assert len(evidence.value["changes"]) == 40 and evidence.value["changes"][0]["reply"] == "OK"


def test_the_churn_records_what_valkey_refused_and_what_did_not_answer_without_judging_it(tmp_path):
    handle = _Deployment(stand_in_build(tmp_path / "tree"))
    handle.refused = {"hz", "lfu-log-factor", "lazyfree-lazy-eviction", "commandlog-request-larger-than"}
    handle.down = {"valkey-1"}
    result = _churn(_Context(handle, {"valkey-0": {}, "valkey-1": {}}), _spec(changes=30))
    value = result.evidence[0].value
    assert result.status.value == "PASS", result.summary
    assert value["errors"] and value["unanswered"] and value["errors"] + value["unanswered"] < 30
    assert all(record.get("unanswered") for record in value["changes"] if record["instance"] == "valkey-1")


def test_the_churn_changes_nothing_by_default_and_nothing_on_a_build_without_a_table(tmp_path):
    handle = _Deployment(stand_in_build(tmp_path / "tree"))
    workload = ConfigChurnWorkload()
    result = workload.start(_Context(handle, {}), _spec())
    assert result.status.value == "PASS" and "no config changed" in result.summary
    assert not workload.stop(_Context(handle, {}), _spec()).evidence[0].value["changes"]
    handle.binaries = {"valkey-0": str(tmp_path / "nope"), "valkey-1": str(tmp_path / "nope")}
    result = _churn(_Context(handle, {}), _spec(changes=3))
    value = result.evidence[0].value
    assert not handle.sets and value["errors"] == 0 and all("skipped" in record for record in value["changes"])


def test_a_refused_churn_change_explains_error_replies():
    spec = _spec()
    refused = make_evidence(spec, CHURN_EVIDENCE, "workload.quiesced", {"changes": [], "errors": 1, "unanswered": 0})
    clean = make_evidence(spec, CHURN_EVIDENCE, "workload.quiesced", {"changes": [], "errors": 0, "unanswered": 3})
    assert server_stats._got_errors([refused]) == ["config-churn"]
    assert server_stats._got_errors([clean]) == []
