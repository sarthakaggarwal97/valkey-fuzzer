"""Focused tests for the generic runner's local Valkey topology provider."""

from unittest.mock import Mock, call, patch

import pytest

from src.cluster_orchestrator.orchestrator import ConfigurationManager, PortManager
from src.extensibility import NodeSelector, OutcomeStatus, RunMode, RunPlan, StartupOverlay
from src.feature_runner import FeatureRunner, ProvisionedTopology
from src.fuzzer_engine.cluster_coordinator import ClusterCoordinator
from src.fuzzer_engine.test_case_generator import ScenarioGenerator
from src.local_topology_provider import (
    LOCAL_TOPOLOGY_SERVICE,
    LocalTopologyError,
    LocalTopologyValidationError,
    LocalValkeyTopologyHandle,
    LocalValkeyTopologyProvider,
    apply_startup_overlays,
    local_topology_provider_factories,
)
from src.models import ClusterConfig, ClusterInstance, NodeInfo, NodePlan, TopologyType
from src.utils.topology_config import cluster_config_from_topology_document


def make_plan(
    topology: str = "standalone",
    *,
    topology_document=None,
    overlays=(),
) -> RunPlan:
    return RunPlan(
        scenario_id="local-provider-test",
        pack_id="tests.local-provider",
        pack_version="1",
        api_version="v1",
        seed=17,
        topology=topology,
        components=(),
        startup_overlays=tuple(overlays),
        topology_document=topology_document or {"type": topology},
        scenario_api_version="v3",
        mode=RunMode.TOPOLOGY_SMOKE,
    )


def make_plan_node(
    node_id: str,
    role: str,
    *,
    server_config=None,
    shard_id: int = 0,
) -> NodePlan:
    index = int(node_id.split("-")[-1])
    return NodePlan(
        node_id=node_id,
        role=role,
        shard_id=shard_id,
        port=7000 + index,
        bus_port=17000 + index,
        server_config=server_config or {},
    )


def make_node(
    node_id: str = "node-0",
    role: str = "primary",
    *,
    shard_id: int = 0,
    slot_start=None,
    slot_end=None,
    master_node_id=None,
    server_config=None,
) -> NodeInfo:
    index = int(node_id.split("-")[-1])
    process = Mock(pid=100 + index)
    process.poll.return_value = None
    return NodeInfo(
        node_id=node_id,
        role=role,
        shard_id=shard_id,
        port=7000 + index,
        bus_port=17000 + index,
        pid=process.pid,
        process=process,
        data_dir=f"/tmp/{node_id}",
        log_file=f"/tmp/{node_id}.log",
        slot_start=slot_start,
        slot_end=slot_end,
        master_node_id=master_node_id,
        server_config=server_config or {},
    )


def make_instance(
    topology_type: TopologyType = TopologyType.STANDALONE,
    *,
    nodes=None,
) -> ClusterInstance:
    if nodes is None:
        nodes = [make_node()]
    replicas = sum(node.role == "replica" for node in nodes)
    shards = 3 if topology_type == TopologyType.CLUSTER else 1
    return ClusterInstance(
        cluster_id="local-test",
        config=ClusterConfig(
            num_shards=shards,
            replicas_per_shard=replicas if topology_type != TopologyType.CLUSTER else 0,
            topology_type=topology_type,
        ),
        nodes=nodes,
        is_ready=True,
        valkey_sha="abc1234",
    )


@pytest.mark.parametrize(
    ("document", "expected"),
    [
        ({"type": "standalone"}, (TopologyType.STANDALONE, 1, 0)),
        ({"type": "replication"}, (TopologyType.REPLICATION, 1, 1)),
        (
            {"type": "cluster", "num_shards": 4, "replicas_per_shard": 0},
            (TopologyType.CLUSTER, 4, 0),
        ),
    ],
)
def test_pure_topology_document_parser_preserves_topology_defaults(document, expected):
    config = cluster_config_from_topology_document(document, strict=True)

    assert (config.topology_type, config.num_shards, config.replicas_per_shard) == expected


def test_v2_parser_delegates_without_rejecting_previously_ignored_fields():
    scenario = ScenarioGenerator().parse_dsl_config(
        """
api_version: v2
scenario_id: compatible
topology:
  type: standalone
  future_provider_field: ignored-by-v2
"""
    )

    assert scenario.cluster_config.topology_type is TopologyType.STANDALONE


@pytest.mark.parametrize(
    ("document", "message"),
    [
        (
            {"type": "standalone", "future_provider_field": True},
            "Unsupported local topology fields",
        ),
        (
            {"type": "standalone", "base_data_dir": ""},
            "base_data_dir must be a non-empty string",
        ),
        (
            {"type": "standalone", "valkey_binary": None},
            "valkey_binary must be a string",
        ),
        (
            {"type": "standalone", "enable_cleanup": 1},
            "enable_cleanup must be a boolean",
        ),
    ],
)
def test_strict_topology_parser_rejects_unsupported_or_unsafe_runtime_fields(document, message):
    with pytest.raises(ValueError, match=message):
        cluster_config_from_topology_document(document, strict=True)


def test_topology_parser_takes_a_deep_snapshot_of_repeated_directives():
    document = {
        "type": "standalone",
        "server_config": {"save": ["900 1", "300 10"]},
        "node_configs": {"node-0": {"rename-command": ["FLUSHALL", ""]}},
    }

    config = cluster_config_from_topology_document(document, strict=True)
    document["server_config"]["save"].append("60 1000")
    document["node_configs"]["node-0"]["rename-command"].append("CONFIG")

    assert config.server_config["save"] == ["900 1", "300 10"]
    assert config.node_configs["node-0"]["rename-command"] == ["FLUSHALL", ""]


@pytest.mark.parametrize(
    "document",
    [
        {
            "type": "standalone",
            "server_config": {
                "repl_compression": "none",
                "repl-compression": "lz4",
            },
        },
        {
            "type": "standalone",
            "node_configs": {
                "node-0": {
                    "repl_compression": "none",
                    "repl-compression": "lz4",
                },
            },
        },
    ],
)
def test_strict_topology_parser_rejects_duplicate_aliases_within_one_layer(document):
    with pytest.raises(ValueError, match="both normalize to 'repl-compression'"):
        cluster_config_from_topology_document(document, strict=True)


def test_strict_topology_parser_canonicalizes_each_config_layer_independently():
    config = cluster_config_from_topology_document(
        {
            "type": "standalone",
            "server_config": {"repl_compression": "none"},
            "node_configs": {
                "primary": {"--REPL-COMPRESSION": False},
                "node-0": {"repl-compression": "lz4"},
            },
        },
        strict=True,
    )

    assert config.server_config == {"repl-compression": "none"}
    assert config.node_configs == {
        "primary": {"repl-compression": False},
        "node-0": {"repl-compression": "lz4"},
    }


def test_provider_validate_is_repeatable_and_never_constructs_a_coordinator():
    constructed = []

    def coordinator_factory():
        constructed.append(True)
        raise AssertionError("validate must not construct a coordinator")

    provider = LocalValkeyTopologyProvider(coordinator_factory)
    plan = make_plan(
        "replication",
        topology_document={
            "type": "replication",
            "replicas_per_shard": 2,
            "server_config": {"appendonly": "no"},
        },
        overlays=(
            StartupOverlay(
                overlay_id="replicas",
                selector=NodeSelector(roles=("replica",)),
                server_config={"repl-diskless-load": "swapdb"},
            ),
        ),
    )

    with (
        patch.object(ConfigurationManager, "setup_valkey_from_source") as setup,
        patch.object(ConfigurationManager, "spawn_all_nodes") as spawn,
    ):
        assert provider.validate(plan) is None
        assert provider.validate(plan) is None

    assert constructed == []
    setup.assert_not_called()
    spawn.assert_not_called()


def test_provider_rejects_non_loopback_bind_before_constructing_coordinator():
    coordinator_factory = Mock()
    provider = LocalValkeyTopologyProvider(coordinator_factory)
    plan = make_plan(
        topology_document={
            "type": "standalone",
            "server_config": {"bind": "0.0.0.0"},
        },
    )

    with pytest.raises(LocalTopologyValidationError, match="managed by the orchestrator"):
        provider.validate(plan)

    coordinator_factory.assert_not_called()


def test_apply_overlays_supports_all_role_and_exact_node_selectors_in_order():
    plans = [
        make_plan_node("node-0", "primary"),
        make_plan_node("node-1", "replica"),
        make_plan_node("node-2", "replica"),
    ]
    overlays = (
        StartupOverlay(
            overlay_id="all",
            server_config={"appendonly": False},
        ),
        StartupOverlay(
            overlay_id="replicas",
            selector=NodeSelector(roles=("replica",)),
            server_config={"repl-compression": "lz4"},
        ),
        StartupOverlay(
            overlay_id="selected-replicas",
            selector=NodeSelector(
                roles=("replica",),
                node_ids=("node-1", "node-2"),
            ),
            server_config={"replica-priority": 50},
        ),
        StartupOverlay(
            overlay_id="exact-last",
            selector=NodeSelector(node_ids=("node-2",)),
            server_config={"replica-priority": 25},
        ),
    )

    apply_startup_overlays(plans, overlays)

    assert plans[0].server_config == {"appendonly": False}
    assert plans[1].server_config == {
        "appendonly": False,
        "repl-compression": "lz4",
        "replica-priority": 50,
    }
    assert plans[2].server_config == {
        "appendonly": False,
        "repl-compression": "lz4",
        "replica-priority": 25,
    }


def test_override_normalizes_aliases_and_replaces_complete_repeated_value():
    plan = make_plan_node(
        "node-0",
        "primary",
        server_config={
            "repl_compression": "none",
            "save": ["900 1", "300 10"],
        },
    )

    apply_startup_overlays(
        [plan],
        (
            StartupOverlay(
                overlay_id="override",
                server_config={
                    "--REPL-COMPRESSION": "lz4",
                    "save": ["60 1000"],
                },
            ),
        ),
    )

    assert plan.server_config == {
        "repl-compression": "lz4",
        "save": ["60 1000"],
    }


def test_topology_layers_override_equivalent_option_aliases_canonically():
    config = ClusterConfig(
        num_shards=1,
        replicas_per_shard=0,
        topology_type=TopologyType.STANDALONE,
        server_config={"repl_compression": True, "loglevel": "notice"},
        node_configs={
            "primary": {"--REPL-COMPRESSION": False},
            "node-0": {"repl-compression": "lz4"},
        },
    )

    node_plan = ConfigurationManager(config, PortManager()).plan_topology()[0]

    assert node_plan.server_config == {
        "repl-compression": "lz4",
        "loglevel": "notice",
    }


def test_topology_layer_validation_context_handles_invalid_utf8_bytes():
    config = ClusterConfig(
        num_shards=1,
        replicas_per_shard=0,
        topology_type=TopologyType.STANDALONE,
        server_config={"loglevel": b"\xff"},
    )

    with pytest.raises(ValueError, match="server_config"):
        ConfigurationManager(config, PortManager()).plan_topology()


def test_error_policy_accepts_semantically_identical_values():
    plan = make_plan_node(
        "node-0",
        "primary",
        server_config={"appendonly": False},
    )

    apply_startup_overlays(
        [plan],
        (
            StartupOverlay(
                overlay_id="same",
                server_config={"appendonly": "no"},
                conflict_policy="error",
            ),
        ),
    )

    assert plan.server_config == {"appendonly": False}


def test_error_policy_is_scoped_to_each_selected_node():
    plans = [
        make_plan_node("node-0", "primary"),
        make_plan_node("node-1", "replica"),
    ]

    apply_startup_overlays(
        plans,
        (
            StartupOverlay(
                overlay_id="primary",
                selector=NodeSelector(node_ids=("node-0",)),
                server_config={"loglevel": "notice"},
            ),
            StartupOverlay(
                overlay_id="replica",
                selector=NodeSelector(node_ids=("node-1",)),
                server_config={"loglevel": "debug"},
                conflict_policy="error",
            ),
        ),
    )

    assert plans[0].server_config["loglevel"] == "notice"
    assert plans[1].server_config["loglevel"] == "debug"


@pytest.mark.parametrize(
    ("selector", "message"),
    [
        (NodeSelector(roles=("sentinel",)), "unknown roles"),
        (NodeSelector(node_ids=("node-9",)), "unknown node IDs"),
        (
            NodeSelector(roles=("primary",), node_ids=("node-1",)),
            "does not match any planned node",
        ),
    ],
)
def test_overlay_rejects_unknown_and_disjoint_selectors(selector, message):
    plans = [
        make_plan_node("node-0", "primary"),
        make_plan_node("node-1", "replica"),
    ]

    with pytest.raises(LocalTopologyValidationError, match=message):
        apply_startup_overlays(
            plans,
            (
                StartupOverlay(
                    overlay_id="invalid",
                    selector=selector,
                    server_config={"appendonly": "no"},
                ),
            ),
        )


@pytest.mark.parametrize(
    ("overlay", "message"),
    [
        (
            StartupOverlay(
                overlay_id="labels",
                selector=NodeSelector(labels={"zone": "a"}),
                server_config={"appendonly": "no"},
            ),
            "node labels",
        ),
        (
            StartupOverlay(
                overlay_id="artifact",
                artifact="candidate",
            ),
            "per-node artifacts",
        ),
        (
            StartupOverlay(
                overlay_id="policy",
                server_config={"appendonly": "no"},
                conflict_policy="merge",
            ),
            "unsupported conflict policy",
        ),
    ],
)
def test_local_provider_rejects_unimplemented_overlay_dimensions(overlay, message):
    with pytest.raises(LocalTopologyValidationError, match=message):
        LocalValkeyTopologyProvider().validate(make_plan(overlays=(overlay,)))


def test_overlay_failure_is_atomic_even_after_an_earlier_overlay_would_succeed():
    plans = [
        make_plan_node("node-0", "primary", server_config={"loglevel": "notice"}),
        make_plan_node("node-1", "replica", server_config={"appendonly": True}),
    ]
    originals = [dict(plan.server_config) for plan in plans]

    with pytest.raises(LocalTopologyValidationError, match="conflicts"):
        apply_startup_overlays(
            plans,
            (
                StartupOverlay(
                    overlay_id="first",
                    server_config={"maxmemory": "128mb"},
                ),
                StartupOverlay(
                    overlay_id="late-conflict",
                    selector=NodeSelector(node_ids=("node-1",)),
                    server_config={"appendonly": "no"},
                    conflict_policy="error",
                ),
            ),
        )

    assert [plan.server_config for plan in plans] == originals


def test_managed_option_rejection_is_atomic():
    plan = make_plan_node("node-0", "primary", server_config={"appendonly": "yes"})

    with pytest.raises(ValueError, match="managed by the orchestrator"):
        apply_startup_overlays(
            [plan],
            (
                StartupOverlay(
                    overlay_id="unsafe",
                    server_config={"port": 9000},
                ),
            ),
        )

    assert plan.server_config == {"appendonly": "yes"}


def test_coordinator_configurator_runs_after_plan_and_before_spawn():
    events = []
    manager = Mock()
    manager.setup_valkey_from_source.side_effect = lambda: events.append("binary") or "/opt/valkey-server"
    plan = make_plan_node("node-0", "primary")
    manager.plan_topology.side_effect = lambda: events.append("plan") or [plan]
    manager.spawned_nodes = []
    node = make_node()

    def spawn(plans):
        events.append("spawn")
        assert plans[0].server_config == {"appendonly": "no"}
        return [node]

    manager.spawn_all_nodes.side_effect = spawn
    manager.cluster_id = "local-test"
    cluster_manager = Mock()
    cluster_manager.form_cluster.side_effect = lambda *args, **kwargs: (
        events.append("form") or Mock(cluster_id="local-test")
    )

    def configure(plans):
        events.append("configure")
        plans[0].server_config = {"appendonly": "no"}

    with (
        patch(
            "src.fuzzer_engine.cluster_coordinator.ConfigurationManager",
            return_value=manager,
        ),
        patch("src.fuzzer_engine.cluster_coordinator.detect_valkey_sha", return_value=None),
    ):
        coordinator = ClusterCoordinator()
        coordinator.cluster_manager = cluster_manager
        coordinator.create_topology(
            ClusterConfig(num_shards=1, replicas_per_shard=0),
            configure_node_plans=configure,
        )

    assert events == ["binary", "plan", "configure", "spawn", "form"]


def test_coordinator_rejects_configurator_return_before_spawn():
    manager = Mock()
    manager.setup_valkey_from_source.return_value = "/opt/valkey-server"
    manager.plan_topology.return_value = [make_plan_node("node-0", "primary")]
    manager.spawned_nodes = []

    with (
        patch(
            "src.fuzzer_engine.cluster_coordinator.ConfigurationManager",
            return_value=manager,
        ),
        patch("src.fuzzer_engine.cluster_coordinator.detect_valkey_sha", return_value=None),
    ):
        coordinator = ClusterCoordinator()
        coordinator.cluster_manager = Mock()
        with pytest.raises(TypeError, match="must mutate plans in place"):
            coordinator.create_topology(
                ClusterConfig(num_shards=1, replicas_per_shard=0),
                configure_node_plans=lambda plans: plans,
            )

    manager.spawn_all_nodes.assert_not_called()
    manager.cleanup_cluster.assert_called_once_with([])


def test_provider_provisions_handle_services_clients_restarts_and_exact_cleanup():
    plan = make_plan(
        topology_document={
            "type": "standalone",
            "server_config": {"appendonly": "yes"},
        },
        overlays=(
            StartupOverlay(
                overlay_id="disable-aof",
                selector=NodeSelector(node_ids=("node-0",)),
                server_config={"appendonly": "no"},
            ),
        ),
    )
    instance = make_instance(
        nodes=[make_node(server_config={"appendonly": "no"})],
    )
    borrowed_client = Mock()
    coordinator = Mock()
    coordinator.cluster_manager.get_client.return_value = borrowed_client
    captured = {}

    def create_topology(config, seed, configure_node_plans):
        captured["config"] = config
        captured["seed"] = seed
        instance.config = config
        node_plan = make_plan_node(
            "node-0",
            "primary",
            server_config=config.server_config,
        )
        configure_node_plans([node_plan])
        captured["node_plan"] = node_plan
        return instance

    coordinator.create_topology.side_effect = create_topology
    coordinator.restart_node.return_value = True
    coordinator.destroy_cluster.return_value = True
    provider = LocalValkeyTopologyProvider(lambda: coordinator)
    context = Mock(plan=plan)

    topology = provider.provision(context)
    handle = topology.handle

    assert isinstance(topology, ProvisionedTopology)
    assert isinstance(handle, LocalValkeyTopologyHandle)
    assert topology.services == {LOCAL_TOPOLOGY_SERVICE: handle}
    assert captured["seed"] == 17
    assert captured["node_plan"].server_config == {"appendonly": "no"}
    assert handle.nodes == tuple(instance.nodes)
    assert handle.topology_type is TopologyType.STANDALONE
    assert handle.valkey_sha == "abc1234"
    assert handle.client("node-0") is borrowed_client
    assert handle.restart("node-0", wait_ready=False) is instance.nodes[0]
    coordinator.restart_node.assert_called_once_with(
        "local-test",
        "node-0",
        wait_ready=False,
    )

    provider.cleanup(context, topology)

    coordinator.destroy_cluster.assert_called_once_with("local-test")
    assert handle.is_closed
    with pytest.raises(LocalTopologyError, match="already been cleaned up"):
        handle.client("node-0")


@pytest.mark.parametrize(
    ("topology", "document", "topology_type", "nodes"),
    [
        (
            "standalone",
            {"type": "standalone"},
            TopologyType.STANDALONE,
            [make_node()],
        ),
        (
            "replication",
            {"type": "replication"},
            TopologyType.REPLICATION,
            [
                make_node(),
                make_node("node-1", "replica", master_node_id="node-0"),
            ],
        ),
        (
            "cluster",
            {"type": "cluster", "replicas_per_shard": 0},
            TopologyType.CLUSTER,
            [
                make_node(slot_start=0, slot_end=5460),
                make_node("node-1", shard_id=1, slot_start=5461, slot_end=10921),
                make_node("node-2", shard_id=2, slot_start=10922, slot_end=16383),
            ],
        ),
    ],
)
def test_provider_routes_every_local_topology_through_the_same_coordinator_path(
    topology,
    document,
    topology_type,
    nodes,
):
    coordinator = Mock()
    coordinator.create_topology.return_value = make_instance(
        topology_type,
        nodes=nodes,
    )
    coordinator.destroy_cluster.return_value = True
    provider = LocalValkeyTopologyProvider(lambda: coordinator)
    context = Mock(plan=make_plan(topology, topology_document=document))

    provisioned = provider.provision(context)

    passed_config = coordinator.create_topology.call_args.args[0]
    assert passed_config.topology_type is topology_type
    provider.cleanup(context, provisioned)


def test_provider_rolls_back_if_handle_construction_fails_after_create():
    duplicate = make_node()
    instance = make_instance(nodes=[duplicate, duplicate])
    coordinator = Mock()
    coordinator.create_topology.return_value = instance
    coordinator.destroy_cluster.return_value = True
    provider = LocalValkeyTopologyProvider(lambda: coordinator)
    context = Mock(plan=make_plan())

    with pytest.raises(LocalTopologyError, match="node IDs must be unique"):
        provider.provision(context)

    coordinator.destroy_cluster.assert_not_called()
    provider.rollback(context)
    coordinator.destroy_cluster.assert_called_once_with(instance.cluster_id)
    provider.rollback(context)
    coordinator.destroy_cluster.assert_called_once_with(instance.cluster_id)


@pytest.mark.parametrize(
    "rollback_effect",
    [
        False,
        RuntimeError("rollback exploded"),
    ],
)
def test_provider_reports_rollback_failure_after_post_create_error(rollback_effect):
    duplicate = make_node()
    instance = make_instance(nodes=[duplicate, duplicate])
    coordinator = Mock()
    coordinator.create_topology.return_value = instance
    if isinstance(rollback_effect, BaseException):
        coordinator.destroy_cluster.side_effect = rollback_effect
    else:
        coordinator.destroy_cluster.return_value = rollback_effect
    provider = LocalValkeyTopologyProvider(lambda: coordinator)
    context = Mock(plan=make_plan())

    with pytest.raises(LocalTopologyError, match="node IDs must be unique"):
        provider.provision(context)
    with pytest.raises(LocalTopologyError, match="local topology rollback failed"):
        provider.rollback(context)

    coordinator.destroy_cluster.assert_called_once_with(instance.cluster_id)


def test_provider_preserves_keyboard_interrupt_while_attempting_rollback():
    instance = make_instance()
    coordinator = Mock()
    coordinator.create_topology.return_value = instance
    coordinator.destroy_cluster.side_effect = KeyboardInterrupt("rollback interrupted")
    provider = LocalValkeyTopologyProvider(lambda: coordinator)
    context = Mock(plan=make_plan())

    with (
        patch(
            "src.local_topology_provider.LocalValkeyTopologyHandle",
            side_effect=KeyboardInterrupt("provision interrupted"),
        ),
        pytest.raises(KeyboardInterrupt, match="provision interrupted"),
    ):
        provider.provision(context)

    with pytest.raises(KeyboardInterrupt, match="rollback interrupted"):
        provider.rollback(context)
    coordinator.destroy_cluster.assert_called_once_with(instance.cluster_id)


def test_provider_rejects_wrong_return_type_and_topology_with_rollback():
    wrong_type = Mock(cluster_id="wrong-type")
    mismatched = make_instance(TopologyType.REPLICATION, nodes=[make_node(), make_node("node-1", "replica")])
    coordinator = Mock()
    coordinator.create_topology.side_effect = [wrong_type, mismatched]
    coordinator.destroy_cluster.return_value = True
    provider = LocalValkeyTopologyProvider(lambda: coordinator)
    context = Mock(plan=make_plan())

    with pytest.raises(LocalTopologyError, match="expected ClusterInstance"):
        provider.provision(context)
    provider.rollback(context)
    with pytest.raises(LocalTopologyError, match="does not match the requested plan"):
        provider.provision(context)
    provider.rollback(context)

    assert coordinator.destroy_cluster.call_args_list == [
        call("wrong-type"),
        call(mismatched.cluster_id),
    ]


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda instance: setattr(instance, "cluster_id", ""), "topology ID"),
        (lambda instance: setattr(instance, "is_ready", False), "before it was ready"),
        (lambda instance: setattr(instance, "nodes", []), "without nodes"),
        (lambda instance: setattr(instance.nodes[0], "role", "replica"), "planned role"),
        (lambda instance: setattr(instance.nodes[0], "host", "0.0.0.0"), "loopback host"),
    ],
)
def test_provider_rejects_invalid_provisioned_instance_and_keeps_it_rollbackable(
    mutation,
    message,
):
    instance = make_instance()
    mutation(instance)
    coordinator = Mock()
    coordinator.active_clusters = {}

    def create_topology(*args, **kwargs):
        del args, kwargs
        coordinator.active_clusters[instance.cluster_id] = object()
        return instance

    def destroy_cluster(cluster_id):
        del coordinator.active_clusters[cluster_id]
        return True

    coordinator.create_topology.side_effect = create_topology
    coordinator.destroy_cluster.side_effect = destroy_cluster
    provider = LocalValkeyTopologyProvider(lambda: coordinator)
    context = Mock(plan=make_plan())

    with pytest.raises(LocalTopologyError, match=message):
        provider.provision(context)

    provider.rollback(context)
    coordinator.destroy_cluster.assert_called_once_with(instance.cluster_id)
    assert coordinator.active_clusters == {}


def test_provider_validates_against_pre_create_config_snapshot():
    instance = make_instance()
    coordinator = Mock()
    coordinator.active_clusters = {}

    def create_topology(config, **kwargs):
        del kwargs
        config.base_port = 8000
        instance.config = config
        instance.nodes[0].port = 8000
        instance.nodes[0].bus_port = 18000
        coordinator.active_clusters[instance.cluster_id] = object()
        return instance

    def destroy_cluster(cluster_id):
        del coordinator.active_clusters[cluster_id]
        return True

    coordinator.create_topology.side_effect = create_topology
    coordinator.destroy_cluster.side_effect = destroy_cluster
    provider = LocalValkeyTopologyProvider(lambda: coordinator)
    context = Mock(plan=make_plan())

    with pytest.raises(LocalTopologyError, match="does not match the requested plan"):
        provider.provision(context)

    provider.rollback(context)
    coordinator.destroy_cluster.assert_called_once_with(instance.cluster_id)


def test_provider_rejects_coordinator_that_drops_resolved_overlay_config():
    plan = make_plan(
        overlays=(
            StartupOverlay(
                overlay_id="feature",
                server_config={"appendonly": "no"},
            ),
        ),
    )
    instance = make_instance()
    coordinator = Mock()

    def create_topology(config, **kwargs):
        del kwargs
        instance.config = config
        return instance

    coordinator.create_topology.side_effect = create_topology
    coordinator.destroy_cluster.return_value = True
    provider = LocalValkeyTopologyProvider(lambda: coordinator)
    context = Mock(plan=plan)

    with pytest.raises(LocalTopologyError, match="planned server config"):
        provider.provision(context)

    provider.rollback(context)
    coordinator.destroy_cluster.assert_called_once_with(instance.cluster_id)


def test_active_provider_rejects_second_provision_and_successful_cleanup_allows_reuse():
    coordinator = Mock()
    coordinator.create_topology.return_value = make_instance()
    coordinator.destroy_cluster.return_value = True
    factory = Mock(return_value=coordinator)
    provider = LocalValkeyTopologyProvider(factory)
    context = Mock(plan=make_plan())

    first = provider.provision(context)
    with pytest.raises(LocalTopologyError, match="already owns"):
        provider.provision(context)
    assert factory.call_count == 1
    provider.rollback(context)
    coordinator.destroy_cluster.assert_not_called()

    provider.cleanup(context, first)
    second = provider.provision(context)
    assert factory.call_count == 2
    provider.cleanup(context, second)


def test_provider_rollback_discovers_active_cluster_if_create_raises_before_return():
    coordinator = Mock()
    coordinator.active_clusters = {}

    def create_topology(*args, **kwargs):
        del args, kwargs
        coordinator.active_clusters["partial"] = object()
        raise RuntimeError("formation failed")

    def destroy_cluster(cluster_id):
        del coordinator.active_clusters[cluster_id]
        return True

    coordinator.create_topology.side_effect = create_topology
    coordinator.destroy_cluster.side_effect = destroy_cluster
    provider = LocalValkeyTopologyProvider(lambda: coordinator)
    context = Mock(plan=make_plan())

    with pytest.raises(RuntimeError, match="formation failed"):
        provider.provision(context)

    provider.rollback(context)
    coordinator.destroy_cluster.assert_called_once_with("partial")
    assert coordinator.active_clusters == {}


def test_provider_rollback_preserves_clusters_that_predate_provision():
    existing = object()
    coordinator = Mock()
    coordinator.active_clusters = {"a-existing": existing}

    def create_topology(*args, **kwargs):
        del args, kwargs
        coordinator.active_clusters["b-new"] = object()
        coordinator.active_clusters["c-new"] = object()
        raise RuntimeError("formation failed")

    def destroy_cluster(cluster_id):
        del coordinator.active_clusters[cluster_id]
        return True

    coordinator.create_topology.side_effect = create_topology
    coordinator.destroy_cluster.side_effect = destroy_cluster
    provider = LocalValkeyTopologyProvider(lambda: coordinator)
    context = Mock(plan=make_plan())

    with pytest.raises(RuntimeError, match="formation failed"):
        provider.provision(context)

    provider.rollback(context)
    assert coordinator.destroy_cluster.call_args_list == [
        call("b-new"),
        call("c-new"),
    ]
    assert coordinator.active_clusters == {"a-existing": existing}


def test_provider_rejects_preexisting_return_without_rolling_it_back():
    instance = make_instance()
    coordinator = Mock()
    coordinator.active_clusters = {instance.cluster_id: object()}
    coordinator.create_topology.return_value = instance
    provider = LocalValkeyTopologyProvider(lambda: coordinator)
    context = Mock(plan=make_plan())

    with pytest.raises(LocalTopologyError, match="pre-existing topology"):
        provider.provision(context)

    provider.rollback(context)
    coordinator.destroy_cluster.assert_not_called()
    assert instance.cluster_id in coordinator.active_clusters


def test_provider_rollback_rejects_success_if_owned_cluster_remains_active():
    coordinator = Mock()
    coordinator.active_clusters = {}

    def create_topology(*args, **kwargs):
        del args, kwargs
        coordinator.active_clusters["partial"] = object()
        raise RuntimeError("formation failed")

    coordinator.create_topology.side_effect = create_topology
    coordinator.destroy_cluster.return_value = True
    provider = LocalValkeyTopologyProvider(lambda: coordinator)
    context = Mock(plan=make_plan())

    with pytest.raises(RuntimeError, match="formation failed"):
        provider.provision(context)
    with pytest.raises(LocalTopologyError, match="rollback failed"):
        provider.rollback(context)

    assert "partial" in coordinator.active_clusters


def test_provider_rollback_attempts_all_ids_and_preserves_later_interrupt():
    coordinator = Mock()
    coordinator.active_clusters = {}

    def create_topology(*args, **kwargs):
        del args, kwargs
        coordinator.active_clusters.update(
            {
                "a-new": object(),
                "b-new": object(),
            }
        )
        raise RuntimeError("formation failed")

    coordinator.create_topology.side_effect = create_topology
    coordinator.destroy_cluster.side_effect = [
        RuntimeError("ordinary cleanup failure"),
        KeyboardInterrupt("rollback interrupted"),
    ]
    provider = LocalValkeyTopologyProvider(lambda: coordinator)
    context = Mock(plan=make_plan())

    with pytest.raises(RuntimeError, match="formation failed"):
        provider.provision(context)
    with pytest.raises(KeyboardInterrupt, match="rollback interrupted"):
        provider.rollback(context)

    assert coordinator.destroy_cluster.call_args_list == [
        call("a-new"),
        call("b-new"),
    ]


def test_failed_rollback_can_be_retried_before_next_provision():
    duplicate = make_node()
    coordinator = Mock()
    coordinator.create_topology.return_value = make_instance(nodes=[duplicate, duplicate])
    coordinator.destroy_cluster.side_effect = [False, True]
    provider = LocalValkeyTopologyProvider(lambda: coordinator)
    context = Mock(plan=make_plan())

    with pytest.raises(LocalTopologyError, match="node IDs must be unique"):
        provider.provision(context)
    with pytest.raises(LocalTopologyError, match="rollback failed"):
        provider.rollback(context)
    with pytest.raises(LocalTopologyError, match="partial topology"):
        provider.provision(context)

    provider.rollback(context)
    assert coordinator.destroy_cluster.call_args_list == [
        call("local-test"),
        call("local-test"),
    ]


def test_provider_cleanup_false_is_reported_and_can_be_retried():
    coordinator = Mock()
    coordinator.create_topology.return_value = make_instance()
    coordinator.destroy_cluster.side_effect = [False, True]
    provider = LocalValkeyTopologyProvider(lambda: coordinator)
    context = Mock(plan=make_plan())
    topology = provider.provision(context)

    with pytest.raises(LocalTopologyError, match="failed to clean up"):
        provider.cleanup(context, topology)

    assert not topology.handle.is_closed
    provider.cleanup(context, topology)
    assert topology.handle.is_closed
    assert coordinator.destroy_cluster.call_args_list == [
        call("local-test"),
        call("local-test"),
    ]


def test_provider_cleanup_exception_leaves_handle_open_and_can_be_retried():
    coordinator = Mock()
    coordinator.create_topology.return_value = make_instance()
    coordinator.destroy_cluster.side_effect = [RuntimeError("cleanup failed"), True]
    provider = LocalValkeyTopologyProvider(lambda: coordinator)
    context = Mock(plan=make_plan())
    topology = provider.provision(context)

    with pytest.raises(RuntimeError, match="cleanup failed"):
        provider.cleanup(context, topology)

    assert not topology.handle.is_closed
    provider.cleanup(context, topology)
    assert topology.handle.is_closed


def test_provider_cleanup_rejects_success_if_owned_cluster_remains_active():
    instance = make_instance()
    coordinator = Mock()
    coordinator.active_clusters = {}

    def create_topology(*args, **kwargs):
        del args, kwargs
        coordinator.active_clusters[instance.cluster_id] = object()
        return instance

    coordinator.create_topology.side_effect = create_topology
    coordinator.destroy_cluster.return_value = True
    provider = LocalValkeyTopologyProvider(lambda: coordinator)
    context = Mock(plan=make_plan())
    topology = provider.provision(context)

    with pytest.raises(LocalTopologyError, match="failed to clean up"):
        provider.cleanup(context, topology)

    assert not topology.handle.is_closed


def test_provider_rejects_cleanup_for_a_different_topology():
    coordinator = Mock()
    coordinator.create_topology.return_value = make_instance()
    provider = LocalValkeyTopologyProvider(lambda: coordinator)
    context = Mock(plan=make_plan())
    owned = provider.provision(context)

    with pytest.raises(LocalTopologyError, match="not owned"):
        provider.cleanup(context, ProvisionedTopology())

    coordinator.destroy_cluster.return_value = True
    provider.cleanup(context, owned)


def test_generic_runner_uses_local_provider_and_cleans_up():
    coordinator = Mock()
    coordinator.create_topology.return_value = make_instance()
    coordinator.destroy_cluster.return_value = True
    plan = make_plan()

    outcome = FeatureRunner(
        {},
        {"standalone": lambda: LocalValkeyTopologyProvider(lambda: coordinator)},
    ).run(plan)

    assert outcome.status is OutcomeStatus.PASS
    coordinator.create_topology.assert_called_once()
    coordinator.destroy_cluster.assert_called_once_with("local-test")


def test_generic_runner_rolls_back_local_provider_after_post_create_validation_error():
    duplicate = make_node()
    coordinator = Mock()
    coordinator.create_topology.return_value = make_instance(nodes=[duplicate, duplicate])
    coordinator.destroy_cluster.return_value = True

    outcome = FeatureRunner(
        {},
        {"standalone": lambda: LocalValkeyTopologyProvider(lambda: coordinator)},
    ).run(make_plan())

    assert outcome.status is OutcomeStatus.ERROR
    coordinator.destroy_cluster.assert_called_once_with("local-test")


def test_local_provider_factories_cover_every_existing_topology():
    coordinator_factory = Mock()
    factories = local_topology_provider_factories(coordinator_factory)

    assert set(factories) == {topology_type.value for topology_type in TopologyType}
    providers = [factory() for factory in factories.values()]
    assert all(isinstance(provider, LocalValkeyTopologyProvider) for provider in providers)
    assert len({id(provider) for provider in providers}) == len(providers)


def test_destroy_cluster_attempts_nodes_after_client_close_failure():
    coordinator = ClusterCoordinator()
    coordinator.cluster_manager = Mock()
    coordinator.cluster_manager.close_connections.side_effect = RuntimeError("close failed")
    config_manager = Mock()
    instance = make_instance()
    coordinator.active_clusters[instance.cluster_id] = {
        "instance": instance,
        "config_manager": config_manager,
    }

    assert coordinator.destroy_cluster(instance.cluster_id) is False

    config_manager.cleanup_cluster.assert_called_once_with(instance.nodes)
    assert instance.cluster_id in coordinator.active_clusters


def test_destroy_cluster_retains_active_topology_when_node_cleanup_fails_then_allows_retry():
    coordinator = ClusterCoordinator()
    coordinator.cluster_manager = Mock()
    config_manager = Mock()
    config_manager.cleanup_cluster.side_effect = [RuntimeError("node cleanup failed"), None]
    instance = make_instance()
    coordinator.active_clusters[instance.cluster_id] = {
        "instance": instance,
        "config_manager": config_manager,
    }

    assert coordinator.destroy_cluster(instance.cluster_id) is False
    assert instance.cluster_id in coordinator.active_clusters
    assert coordinator.destroy_cluster(instance.cluster_id) is True
    assert instance.cluster_id not in coordinator.active_clusters
    assert coordinator.cluster_manager.close_connections.call_count == 2
    assert config_manager.cleanup_cluster.call_count == 2


def test_destroy_cluster_attempts_nodes_before_reraising_interrupt():
    coordinator = ClusterCoordinator()
    coordinator.cluster_manager = Mock()
    coordinator.cluster_manager.close_connections.side_effect = KeyboardInterrupt()
    config_manager = Mock()
    instance = make_instance()
    coordinator.active_clusters[instance.cluster_id] = {
        "instance": instance,
        "config_manager": config_manager,
    }

    with pytest.raises(KeyboardInterrupt):
        coordinator.destroy_cluster(instance.cluster_id)

    config_manager.cleanup_cluster.assert_called_once_with(instance.nodes)
    assert instance.cluster_id in coordinator.active_clusters


def test_destroy_cluster_success_removes_active_topology():
    coordinator = ClusterCoordinator()
    coordinator.cluster_manager = Mock()
    config_manager = Mock()
    instance = make_instance()
    coordinator.active_clusters[instance.cluster_id] = {
        "instance": instance,
        "config_manager": config_manager,
    }

    assert coordinator.destroy_cluster(instance.cluster_id) is True
    assert instance.cluster_id not in coordinator.active_clusters
