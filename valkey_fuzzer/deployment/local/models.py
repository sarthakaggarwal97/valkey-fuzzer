"""Data models of the local deployment: its configuration, planned and running instances, and validation results."""

import subprocess
from bisect import bisect_left, bisect_right
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from .cluster_nodes import HASH_SLOTS, parse_slot_tokens
from .queries import Replication, query_cluster_nodes, query_replication, valkey_client


class LocalDeploymentError(RuntimeError):
    """Base class for local deployment lifecycle failures."""


class LocalDeploymentValidationError(ValueError):
    """A local deployment document or startup overlay is invalid."""


DEFAULT_BASE_PORT = 7000
# Each instance's cluster bus port is this far above its client port.
CLUSTER_BUS_PORT_OFFSET = 10000
DEFAULT_BASE_DATA_DIR = "/tmp/valkey-fuzzer"


class DeploymentMode(str, Enum):
    """Valkey's modes: standalone (one primary, optional replicas) or cluster."""

    STANDALONE = "standalone"
    CLUSTER = "cluster"


# The instance counts each mode has when a deployment leaves them out: a single instance, or three
# primaries with a replica each.
DEFAULT_DIMENSIONS = {
    DeploymentMode.STANDALONE: {"primaries": 1, "replicas_per_primary": 0},
    DeploymentMode.CLUSTER: {"primaries": 3, "replicas_per_primary": 1},
}


@dataclass
class DeploymentConfig:
    """Configuration for one local Valkey deployment, as the document describes it (see :mod:`.config`).

    ``primaries`` is the number of shards in cluster mode and one in
    standalone mode; each primary has ``replicas_per_primary`` replicas. Left
    out, they take the mode's :data:`DEFAULT_DIMENSIONS`.
    """

    mode: DeploymentMode = DeploymentMode.CLUSTER
    primaries: Optional[int] = None
    replicas_per_primary: Optional[int] = None
    base_port: int = DEFAULT_BASE_PORT
    base_data_dir: str = DEFAULT_BASE_DATA_DIR
    valkey_binary: str = ""
    enable_cleanup: bool = True
    server_config: dict[str, Any] = field(default_factory=dict)
    instance_configs: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Per-instance overrides keyed by role ("primary"/"replica") or exact instance ID.
    # An exact instance ID wins over a role, which wins over ``valkey_binary``.
    instance_binaries: dict[str, str] = field(default_factory=dict)
    # Roots that hold instance data (RDB, AOF, nodes.conf) instead of
    # ``base_data_dir``, e.g. a size-limited filesystem. Logs stay put.
    instance_data_roots: dict[str, str] = field(default_factory=dict)
    # "shared_loopback": every instance binds 127.0.0.1. "per_instance_loopback":
    # valkey-N binds 127.0.B.(N+2), B being ``loopback_block``, so network faults can target instance pairs.
    addressing: str = "shared_loopback"
    # The /24 of 127.0.0.0/16 per-instance addresses come from: a batch gives each worker its own, so a
    # partition (which matches addresses) in one worker never cuts another's instances.
    loopback_block: int = 0

    def __post_init__(self):
        defaults = DEFAULT_DIMENSIONS[self.mode]
        if self.primaries is None:
            self.primaries = defaults["primaries"]
        if self.replicas_per_primary is None:
            self.replicas_per_primary = defaults["replicas_per_primary"]


@dataclass
class InstancePlan:
    """Plan for one instance before it is spawned."""

    instance_id: str
    role: str
    shard_id: int
    port: int
    bus_port: int
    slot_start: Optional[int] = None
    slot_end: Optional[int] = None
    primary_id: Optional[str] = None
    server_config: dict[str, Any] = field(default_factory=dict)
    valkey_binary: Optional[str] = None
    data_root: Optional[str] = None
    host: str = "127.0.0.1"


@dataclass
class InstanceInfo(InstancePlan):
    """A started instance: its plan, process, and files.

    The record is live and mutable: ``role`` and ``primary_id`` follow
    recorded role changes, and ``process``, ``pid`` and ``started_at`` each restart.
    ``valkey_binary`` is the binary its next (re)start uses; None means the
    deployment default.
    """

    pid: int = 0
    process: Optional[subprocess.Popen] = None
    # Wall-clock time the current process was spawned: from then on it, not its
    # predecessor, may be the one acting, even before it is ready.
    started_at: float = 0.0
    data_dir: str = ""
    log_file: str = ""
    cluster_node_id: Optional[str] = None

    @property
    def is_running(self) -> bool:
        """Whether its current server process is running."""
        return self.process is not None and self.process.poll() is None


@dataclass
class SlotConflict:
    """Represents a slot assignment conflict"""

    slot: int
    conflicting_nodes: list[str]


@dataclass
class LocalDeployment:
    """A formed local deployment and its running instances."""

    deployment_id: str
    config: DeploymentConfig
    instances: list[InstanceInfo]
    valkey_sha: Optional[str] = None  # Git SHA the deployment's valkey-server was built from


@dataclass
class DeploymentConnection:
    """How to reach a formed deployment, and its current view of its instances."""

    instances: list[InstanceInfo]  # the live instance records, shared with the deployment service
    deployment_id: str
    mode: DeploymentMode = DeploymentMode.CLUSTER

    def __post_init__(self):
        self.mode = DeploymentMode(self.mode)
        self.startup_nodes = [{"host": instance.host, "port": instance.port} for instance in self.instances]
        # The plan's slot ranges, fixed now: instance records are updated with
        # live roles later, and a demoted primary must keep naming its shard.
        self._planned_slot_ranges = tuple(
            (planned.shard_id, planned.slot_start, planned.slot_end)
            for planned in self.instances
            if planned.slot_start is not None and planned.slot_end is not None
        )
        self._planned_port_shards = {planned.port: planned.shard_id for planned in self.instances}

    def get_current_nodes(self, include_failed: bool = True, timeout: float = 2.0) -> list[dict]:
        """The current instances, from the deployment's authoritative source, waiting up to ``timeout`` seconds
        for each query."""
        if self.mode != DeploymentMode.CLUSTER:
            return self._get_current_non_cluster_nodes(include_failed, timeout)

        return self._get_current_cluster_nodes(include_failed, timeout)

    def _planned_shard_of(self, slots: list[int], port: int) -> Optional[int]:
        """The planned shard whose slot range holds most of ``slots``; by planned port when the plan has no ranges.

        Only the plan's slot ranges and the live slot ownership decide it, never
        an instance's current role: a planned primary that was demoted still
        names its shard's range.
        """
        if not self._planned_slot_ranges:
            return self._planned_port_shards.get(port)
        slots = sorted(slots)
        votes: dict[int, int] = {}
        for shard_id, start, end in self._planned_slot_ranges:
            votes[shard_id] = votes.get(shard_id, 0) + bisect_right(slots, end) - bisect_left(slots, start)
        # Most slots wins; a tie goes to the lowest shard.
        return min(votes, key=lambda shard: (-votes[shard], shard))

    def _get_current_cluster_nodes(self, include_failed: bool, timeout: float = 2.0) -> list[dict]:
        """The current cluster nodes, from CLUSTER NODES.

        A node's ``shard_id`` comes from the live topology, not the plan: a
        primary belongs to the planned shard whose hash slots it owns, and a
        replica to its primary's shard. Replica migration and failovers move
        instances between shards, so an instance's planned shard can be wrong.
        A primary without slots (e.g. one stopped on purpose whose replica took
        over its slots) and its replicas hold no shard's data: ``shard_id`` None.
        """
        for info in self.startup_nodes:
            try:
                parsed_nodes = query_cluster_nodes(info, timeout=timeout)
                if not parsed_nodes:
                    continue
                slots_of = {node["node_id"]: parse_slot_tokens(node["slots"]) for node in parsed_nodes}
                node_id_to_shard = {
                    node["node_id"]: self._planned_shard_of(slots_of[node["node_id"]], node["port"])
                    for node in parsed_nodes
                    if node["is_primary"] and slots_of[node["node_id"]]
                }
                for node in parsed_nodes:
                    if node["is_replica"] and node["primary_id"]:
                        node_id_to_shard[node["node_id"]] = node_id_to_shard.get(node["primary_id"])

                current_instances = []
                for node in parsed_nodes:
                    is_failed = node["is_fail"] or node["link_state"] == "disconnected"
                    if not include_failed and is_failed:
                        continue

                    current_instances.append(
                        {
                            "node_id": node["node_id"],
                            "host": node["host"],
                            "port": node["port"],
                            "role": "primary" if node["is_primary"] else "replica",
                            "primary_id": node["primary_id"] if node["is_replica"] else None,
                            "shard_id": node_id_to_shard.get(node["node_id"]),
                            "status": "failed" if is_failed else "connected",
                            # Valkey's quorum counts only primaries that own slots.
                            "slot_count": len(slots_of[node["node_id"]]),
                            "slots": slots_of[node["node_id"]],
                        }
                    )

                return current_instances
            except Exception:
                continue
        return []

    @staticmethod
    def _query_replication(instance: InstanceInfo, timeout: float = 2.0) -> Optional[Replication]:
        try:
            with valkey_client(instance.host, instance.port, timeout=timeout) as client:
                return query_replication(client)
        except Exception:
            return None

    def _get_current_non_cluster_nodes(self, include_failed: bool, timeout: float = 2.0) -> list[dict]:
        """Inspect a standalone deployment's primary and replicas through direct clients.

        An instance that does not answer is ``failed`` here whether its process
        runs or not; :func:`~.validation.snapshot.take_snapshot` tells the two
        apart.
        """
        current_instances = []
        for instance in self.instances:
            replication = self._query_replication(instance, timeout) if instance.is_running else None
            is_connected = replication is not None
            if not include_failed and not is_connected:
                continue

            role = (replication and replication.role) or instance.role

            current_instances.append(
                {
                    "node_id": instance.instance_id,
                    "host": instance.host,
                    "port": instance.port,
                    "role": role,
                    "shard_id": instance.shard_id,
                    "status": "connected" if is_connected else "failed",
                    # What the role came from, so a state check need not ask again.
                    "replication": replication,
                }
            )

        return current_instances

    def get_live_nodes(self) -> list[dict]:
        """Only the live (connected) instances."""
        return self.get_current_nodes(include_failed=False)


# The validation configs below are also the YAML form of ``state_validation``
# (``validation.check.STATE_VALIDATION_SCHEMA`` is built from them): a field's
# ``minimum`` metadata bounds it there.
_AT_LEAST_0 = {"minimum": 0}
_TIMEOUT = {"minimum": 0.001}  # seconds each query of the check may wait


@dataclass
class ReplicationValidationConfig:
    """Configuration for replication validation"""

    max_acceptable_lag: float = field(default=10.0, metadata=_AT_LEAST_0)  # seconds
    require_all_replicas_synced: bool = False  # Allow dead replicas after chaos
    check_replication_offset: bool = True
    min_replicas_per_shard: int = field(default=1, metadata=_AT_LEAST_0)  # Live replicas per shard (0 = no check)
    timeout: float = field(default=10.0, metadata=_TIMEOUT)


@dataclass
class ClusterStatusValidationConfig:
    """Configuration for cluster status validation"""

    acceptable_states: list[str] = field(default_factory=lambda: ["ok"])
    allow_degraded: bool = False
    require_quorum: bool = True
    timeout: float = field(default=10.0, metadata=_TIMEOUT)


@dataclass
class SlotCoverageValidationConfig:
    """Configuration for slot coverage validation"""

    require_full_coverage: bool = True
    allow_slot_conflicts: bool = False
    timeout: float = field(default=10.0, metadata=_TIMEOUT)


@dataclass
class ViewConsistencyValidationConfig:
    """Configuration for view consistency validation"""

    require_full_consensus: bool = True
    allow_transient_inconsistency: bool = True
    timeout: float = field(default=15.0, metadata=_TIMEOUT)


@dataclass
class DataConsistencyValidationConfig:
    """Configuration for data consistency validation"""

    check_cross_replica_consistency: bool = True  # Validate data matches across replicas
    num_test_keys: int = field(default=100, metadata={"minimum": 1})  # Number of test keys to write/validate
    key_prefix: str = "fuzzer:test:"  # Prefix for test keys
    timeout: float = field(default=10.0, metadata=_TIMEOUT)


@dataclass
class StateValidationConfig:
    """Configuration for post-operation validation"""

    # Enable/disable individual checks
    check_replication: bool = True
    check_cluster_status: bool = True
    check_slot_coverage: bool = True
    check_roles: bool = True
    check_view_consistency: bool = True
    check_data_consistency: bool = True
    check_logs: bool = True

    # Timing configuration
    stabilization_wait: float = field(default=5.0, metadata=_AT_LEAST_0)  # Wait before validation
    validation_timeout: float = field(default=30.0, metadata=_TIMEOUT)  # Total validation timeout

    # Behavior configuration
    blocking_on_failure: bool = False  # Halt execution on failure
    retry_on_transient_failure: bool = True
    max_retries: int = field(default=3, metadata=_AT_LEAST_0)
    retry_delay: float = field(default=2.0, metadata=_AT_LEAST_0)

    # Sub-component configurations
    replication_config: ReplicationValidationConfig = field(default_factory=ReplicationValidationConfig)
    cluster_status_config: ClusterStatusValidationConfig = field(default_factory=ClusterStatusValidationConfig)
    slot_coverage_config: SlotCoverageValidationConfig = field(default_factory=SlotCoverageValidationConfig)
    view_consistency_config: ViewConsistencyValidationConfig = field(default_factory=ViewConsistencyValidationConfig)
    data_consistency_config: DataConsistencyValidationConfig = field(default_factory=DataConsistencyValidationConfig)


@dataclass
class ReplicaLagInfo:
    """Information about replica lag"""

    replica_node_id: str
    replica_address: str
    primary_node_id: str
    primary_address: str
    lag_seconds: float
    replication_offset_diff: int
    link_status: str


# The validation results' defaults describe a check that measured nothing.


@dataclass
class ReplicationValidation:
    """Replication validation result"""

    success: bool
    all_replicas_synced: bool = False
    max_lag: float = -1.0
    lagging_replicas: list[ReplicaLagInfo] = field(default_factory=list)
    disconnected_replicas: list[str] = field(default_factory=list)
    error_message: Optional[str] = None
    # A shard lost replicas to failures that were not stops: critical, see StateValidationResult.is_critical_failure.
    redundancy_lost: bool = False


@dataclass
class ClusterStatusValidation:
    """Cluster status validation result"""

    success: bool
    cluster_state: str = "unknown"
    nodes_in_fail_state: list[str] = field(default_factory=list)
    has_quorum: bool = False
    degraded_reason: Optional[str] = None
    error_message: Optional[str] = None


@dataclass
class SlotCoverageValidation:
    """Slot coverage validation result"""

    success: bool
    total_slots_assigned: int = 0
    unassigned_slots: list[int] = field(default_factory=lambda: list(range(HASH_SLOTS)))
    conflicting_slots: list[SlotConflict] = field(default_factory=list)
    slot_distribution: dict[str, list[int]] = field(default_factory=dict)  # node_id -> slot ranges
    error_message: Optional[str] = None


@dataclass
class RoleMismatch:
    """Describes a role mismatch"""

    mismatch_type: str  # "primary_count", "replica_count", or "unexpected_failure"
    node_id: str
    expected: str
    actual: str


@dataclass
class RoleValidation:
    """Role validation result"""

    success: bool
    expected_primaries: int = 0
    actual_primaries: int = 0
    expected_replicas: int = 0
    actual_replicas: int = 0
    role_mismatches: list[RoleMismatch] = field(default_factory=list)
    error_message: Optional[str] = None


@dataclass
class ViewDiscrepancy:
    """Describes a view discrepancy between nodes"""

    discrepancy_type: str  # "missing", "extra", "role", "state", "address", "primary", "config_epoch"
    node_reporting: str
    subject_node: str
    expected_value: str
    actual_value: str


@dataclass
class ViewConsistencyValidation:
    """View consistency validation result"""

    success: bool
    nodes_checked: int = 0
    consistent_views: bool = False
    split_brain_detected: bool = False
    view_discrepancies: list[ViewDiscrepancy] = field(default_factory=list)
    consensus_percentage: float = 0.0  # Percentage of nodes with majority view
    error_message: Optional[str] = None


@dataclass
class DataInconsistency:
    """Describes a data inconsistency"""

    key: str
    inconsistency_type: str  # "missing", "value_mismatch", "unreachable"
    expected_value: Optional[str]
    actual_values: dict[str, str]  # node_address -> value


@dataclass
class DataConsistencyValidation:
    """Data consistency validation result"""

    success: bool
    test_keys_checked: int = 0
    missing_keys: list[str] = field(default_factory=list)
    inconsistent_keys: list[DataInconsistency] = field(default_factory=list)
    unreachable_keys: list[str] = field(default_factory=list)
    error_message: Optional[str] = None


@dataclass
class StateValidationResult:
    """Comprehensive validation result"""

    overall_success: bool
    validation_duration: float
    # The attempt could not judge the deployment: a running instance did not answer, the time budget ran out,
    # or a check broke. Retried; one still inconclusive is ERROR, never FAIL.
    inconclusive: bool = False

    # Individual check results; None for a check that did not run
    replication: Optional[ReplicationValidation] = None
    cluster_status: Optional[ClusterStatusValidation] = None
    slot_coverage: Optional[SlotCoverageValidation] = None
    roles: Optional[RoleValidation] = None
    view_consistency: Optional[ViewConsistencyValidation] = None
    data_consistency: Optional[DataConsistencyValidation] = None
    log_validation: Optional[Any] = None  # LogValidationResult from validators.log_validator

    # Failure information
    failed_checks: list[str] = None
    error_messages: list[str] = None

    def __post_init__(self):
        if self.failed_checks is None:
            self.failed_checks = []
        if self.error_messages is None:
            self.error_messages = []

    def is_critical_failure(self) -> bool:
        """Determine if failure is critical and should halt execution.

        Critical failures indicate the cluster is in a broken state where
        continuing operations could cause data loss or corruption. An
        inconclusive attempt judged nothing, so it is never critical.
        """
        if self.inconclusive:
            return False
        # 1. Slot coverage lost - data is unreachable
        if self.slot_coverage and not self.slot_coverage.success:
            return True

        # 2. Split-brain detected - cluster partitioned
        if self.view_consistency and self.view_consistency.split_brain_detected:
            return True

        # 3. Cluster status failures - cluster in unhealthy state
        if self.cluster_status and not self.cluster_status.success:
            if self.cluster_status.has_quorum is False:
                return True

            # Cluster state is 'fail' - cluster is broken
            if self.cluster_status.cluster_state == "fail":
                return True

            # 'unknown' state is only critical if slots are lost
            # (transient 'unknown' after failover is normal)
            if self.cluster_status.cluster_state == "unknown":
                if self.slot_coverage and not self.slot_coverage.success:
                    return True

            # Nodes in fail state - cluster has failed nodes
            if self.cluster_status.nodes_in_fail_state:
                return True

        # 4. Replicas lost to failures that were not stops - redundancy broken
        if self.replication and not self.replication.success and self.replication.redundancy_lost:
            return True

        # 5. Wrong primary count, or unexpected failures (spontaneous node crashes) - roles broken
        if self.roles and not self.roles.success:
            if any(m.mismatch_type in ("primary_count", "unexpected_failure") for m in self.roles.role_mismatches):
                return True

        # 6. Data loss detected
        return bool(self.data_consistency and not self.data_consistency.success and self.data_consistency.missing_keys)


@dataclass
class ExpectedRoles:
    """Expected primaries and replicas after an operation"""

    num_primaries: int
    num_replicas: int
