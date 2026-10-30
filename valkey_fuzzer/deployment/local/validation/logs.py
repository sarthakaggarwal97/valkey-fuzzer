"""Log checks: failure detection and failover lines in the logs of the shards an operation touched."""

import logging
import re
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# How much of each log the checks read: its last lines.
TAIL_LINES = 500

# Logged by a replica that won its election and became the shard's primary. A voter's "Failover auth
# granted" is not one: Valkey logs it on the primary that votes.
_PROMOTION = re.compile(r"Failover election won|configEpoch set to \d+ after successful failover", re.IGNORECASE)
_ELECTION_ERRORS = (re.compile(r"Failover election in progress for epoch 0", re.IGNORECASE),)
# A replica's election can expire or time out under load while another replica wins; the state checks
# judge whether the shard ended with a primary.
_ELECTION_RETRIES = tuple(
    re.compile(pattern, re.IGNORECASE) for pattern in (r"Failover attempt expired", r"Manual failover timed out")
)
_SUB_REPLICA = re.compile(r"I'm a sub-replica! Reconfiguring myself", re.IGNORECASE)


@dataclass
class LogFinding:
    instance_id: str
    shard_id: int
    severity: str
    message: str
    log_line: str


@dataclass
class LogValidationResult:
    success: bool
    findings: list[LogFinding]
    shards_checked: int


@dataclass(frozen=True)
class LogNode:
    """An instance as the log check sees it: its live role and shard (None without slots) and its log."""

    instance_id: str
    address: str
    cluster_node_id: Optional[str]
    role: str
    shard_id: Optional[int]
    log_file: str


def _tail(log_file: str) -> Optional[list[str]]:
    """The last :data:`TAIL_LINES` lines of ``log_file``; None when it cannot be read."""
    try:
        with open(log_file) as handle:
            return handle.readlines()[-TAIL_LINES:]
    except OSError as exc:
        logger.warning(f"Cannot read log file {log_file}: {exc}")
        return None


def _first(lines: Sequence[str], pattern: re.Pattern) -> Optional[str]:
    return next((line.strip() for line in lines if pattern.search(line)), None)


def check_logs(
    nodes: Sequence[LogNode],
    failover_shards: Collection[int],
    stopped: Collection[str],
    shards_with_primary_stopped: Collection[int],
) -> LogValidationResult:
    """Check the logs of the shards that failed over or had nodes stopped (by address, in ``stopped``).

    Each log is read once. Shards are the live ones: a replica that migrated to
    another primary belongs to that primary's shard.
    """
    tails = {node.instance_id: _tail(node.log_file) for node in nodes}
    affected = (
        set(failover_shards)
        | set(shards_with_primary_stopped)
        | {node.shard_id for node in nodes if node.address in stopped and node.shard_id is not None}
    )
    logger.info(f"Validating logs for {len(affected)} affected shards: {sorted(affected)}")

    findings = _failure_detection(nodes, tails, stopped)
    for shard_id in sorted(affected):
        if shard_id in failover_shards or shard_id in shards_with_primary_stopped:
            findings.extend(_failover_findings([node for node in nodes if node.shard_id == shard_id], tails))
    success = not any(finding.severity == "error" for finding in findings)
    return LogValidationResult(success=success, findings=findings, shards_checked=len(affected))


def _failure_detection(
    nodes: Sequence[LogNode], tails: dict[str, Optional[list[str]]], stopped: Collection[str]
) -> list[LogFinding]:
    """Whether the other nodes noticed each stopped node: PFAIL first, then FAIL once a quorum agrees."""
    findings = []
    watchers = [tails[node.instance_id] or [] for node in nodes if node.address not in stopped]
    for node in nodes:
        if node.address not in stopped or not node.cluster_node_id:
            continue
        quorum = re.compile(f"Marking node {node.cluster_node_id}.*as failing.*quorum reached", re.IGNORECASE)
        pfail = re.compile(f"NODE {node.cluster_node_id}.*possibly failing", re.IGNORECASE)
        if any(_first(lines, quorum) for lines in watchers):
            continue
        suspected = any(_first(lines, pfail) for lines in watchers)
        findings.append(
            LogFinding(
                instance_id=node.address,
                shard_id=-1,
                severity="warning" if suspected else "error",
                message=(
                    f"Stopped node {node.address} detected as possibly failing but quorum not yet reached "
                    "(may need more time)"
                    if suspected
                    else f"Cluster did not detect failure of stopped node {node.address} "
                    "(no failure detection messages found)"
                ),
                log_line="",
            )
        )
    return findings


def _failover_findings(nodes: Sequence[LogNode], tails: dict[str, Optional[list[str]]]) -> list[LogFinding]:
    """Whether the shard's failover completed, from its members' logs."""
    findings = []

    def found(node: LogNode, severity: str, message: str, line: str = "") -> None:
        findings.append(LogFinding(node.instance_id, node.shard_id, severity, message, line))

    for node in nodes:
        lines = tails[node.instance_id]
        if lines is None:
            continue
        promoted = _first(lines, _PROMOTION) is not None
        if node.role == "primary" and not promoted:
            found(node, "warning", "Primary has no failover promotion messages in recent logs")
        if not promoted:
            for severity, message, patterns in (
                ("error", "Failover error detected", _ELECTION_ERRORS),
                ("warning", "Failover attempt did not complete", _ELECTION_RETRIES),
            ):
                for pattern in patterns:
                    line = _first(lines, pattern)
                    if line:
                        found(node, severity, message, line)
        line = _first(lines, _SUB_REPLICA)
        if line:
            found(node, "warning", "Replication topology issue detected", line)
    return findings
