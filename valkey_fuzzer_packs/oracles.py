"""Correctness oracles evaluated after the workload and faults have stopped."""

import os
import re
import shutil
import subprocess
import tempfile
import time
from collections.abc import Sequence
from typing import Any, Optional

import valkey
from valkey.crc import key_slot

from valkey_fuzzer.extensions import ComponentStatus, Evidence

from .actions import binary_version
from .common import (
    ComponentConfigError,
    ComponentSpec,
    Field,
    RunPlan,
    component_artifact_dir,
    config_validator,
    evidence_of,
    keyspace_digest,
    keyspace_snapshot,
    make_evidence,
    make_result,
    read_jsonl,
    remaining,
    resolved_config,
    to_plain_data,
    topology,
    write_jsonl,
)
from .faults import FAULT_APPLIED
from .workloads import LEDGER_EVIDENCE

CRASH_PATTERNS = (
    r"=== (VALKEY|REDIS) BUG REPORT START",
    r"ASSERTION FAILED",
    r"Guru Meditation",
    r"crashed by signal",
    r"ERROR: AddressSanitizer",
    r"ERROR: LeakSanitizer",
    r"WARNING: ThreadSanitizer",
    r"runtime error: ",
)

_ABSENT = None


# ---------------------------------------------------------------------------
# valkey.liveness — every Valkey pack includes it
# ---------------------------------------------------------------------------

_LIVENESS_FIELDS = {
    "timeout_s": Field(float, default=30.0, minimum=0.1, maximum=600),
    "extra_crash_patterns": Field(list, default=list),
}


def _validate_liveness(config: dict, spec: ComponentSpec, plan: RunPlan) -> None:
    for pattern in config["extra_crash_patterns"]:
        if not isinstance(pattern, str):
            raise ComponentConfigError("extra_crash_patterns must be strings")
        re.compile(pattern)


def scan_for_crashes(
    paths: dict[str, str],
    patterns: Sequence[str],
    offsets: Optional[dict[str, int]] = None,
) -> dict[str, list[str]]:
    compiled = [re.compile(pattern) for pattern in patterns]
    findings: dict[str, list[str]] = {}
    for node_id, path in paths.items():
        try:
            with open(path, encoding="utf-8", errors="replace") as handle:
                handle.seek((offsets or {}).get(node_id, 0))
                for line in handle:
                    if any(pattern.search(line) for pattern in compiled):
                        findings.setdefault(node_id, []).append(line.rstrip()[:500])
        except OSError:
            continue
    return findings


class LivenessOracle:
    """Every node is alive and responsive, with no crash or sanitizer report."""

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = resolved_config(spec, _LIVENESS_FIELDS)
        handle = topology(context)
        deadline = time.monotonic() + remaining(context, config["timeout_s"])
        status: dict[str, str] = {}
        for node in handle.nodes:
            if not handle.is_alive(node.node_id):
                status[node.node_id] = "process not running"
                continue
            while True:
                client = handle.new_client(node.node_id, timeout=2.0)
                try:
                    client.ping()
                    loading = str(client.info("persistence").get("loading", "0"))
                    if loading in ("0", "False", "false"):
                        status[node.node_id] = "ok"
                        break
                    reason = "still loading"
                except Exception as exc:
                    reason = f"{type(exc).__name__}: {exc}"
                finally:
                    client.close()
                if time.monotonic() >= deadline:
                    status[node.node_id] = reason
                    break
                time.sleep(0.1)
        offset_of = getattr(handle, "log_start_offset", None)
        crashes = scan_for_crashes(
            {node.node_id: handle.log_file(node.node_id) for node in handle.nodes},
            CRASH_PATTERNS + tuple(config["extra_crash_patterns"]),
            {node.node_id: offset_of(node.node_id) for node in handle.nodes} if callable(offset_of) else None,
        )
        problems = [f"{node_id}: {reason}" for node_id, reason in status.items() if reason != "ok"]
        problems += [f"{node_id}: crash report ({lines[0]})" for node_id, lines in crashes.items()]
        record = {"nodes": status, "crash_reports": crashes}
        if problems:
            return make_result(spec, ComponentStatus.FAIL, "; ".join(problems), diagnostics=record)
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{len(status)} node(s) alive and responsive; no crash reports",
            evidence=(make_evidence(spec, "valkey.liveness.verified", "oracle.completed", record),),
        )


# ---------------------------------------------------------------------------
# valkey.topology_state — the topology's state validators
# ---------------------------------------------------------------------------

_TOPOLOGY_FIELDS = {
    "check_replication": Field(bool, default=True),
    "check_cluster_status": Field(bool, default=True),
    "check_slot_coverage": Field(bool, default=True),
    "check_topology": Field(bool, default=True),
    "check_view_consistency": Field(bool, default=True),
    "stabilization_wait_s": Field(float, default=1.0, minimum=0, maximum=600),
    "timeout_s": Field(float, default=60.0, minimum=1, maximum=600),
}


class TopologyStateOracle:
    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = resolved_config(spec, _TOPOLOGY_FIELDS)
        handle = topology(context)
        record = handle.validate_state(
            check_replication=config["check_replication"],
            check_cluster_status=config["check_cluster_status"],
            check_slot_coverage=config["check_slot_coverage"],
            check_topology=config["check_topology"],
            check_view_consistency=config["check_view_consistency"],
            stabilization_wait=config["stabilization_wait_s"],
            timeout=config["timeout_s"],
        )
        if not record["overall_success"]:
            return make_result(
                spec,
                ComponentStatus.FAIL,
                "; ".join(record["error_messages"]) or f"failed checks: {record['failed_checks']}",
                diagnostics=record,
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{getattr(handle.topology_type, 'value', handle.topology_type)} state validated",
            evidence=(make_evidence(spec, "valkey.topology.validated", "oracle.completed", record),),
        )


# ---------------------------------------------------------------------------
# Ledger model shared by the data and snapshot oracles
# ---------------------------------------------------------------------------


class LedgerModel:
    """Acceptable values per key, derived from what clients observed."""

    def __init__(
        self,
        preload: dict[str, str],
        ledger: list[dict[str, Any]],
        loss_windows: Sequence[tuple],
        lossy_key: Optional[Any] = None,
        window_reads: str = "strict",
    ):
        self.preload = preload
        self.unchecked_reads = 0
        self.possible: dict[str, set] = {key: {value} for key, value in preload.items()}
        self.ever: dict[str, set] = {key: {value} for key, value in preload.items()}
        self.read_violations: list[dict[str, Any]] = []
        self.loss_permitted_ops = 0
        for entry in ledger:
            key = entry["key"]
            possible = self.possible.setdefault(key, {_ABSENT})
            ever = self.ever.setdefault(key, {_ABSENT} if key not in preload else set())
            outcome = entry["outcome"]
            op = entry["op"]
            in_window = any(start < entry["t_end"] <= end for start, end in loss_windows) and (
                lossy_key is None or lossy_key(key)
            )
            lossy = outcome == "ok" and in_window
            if lossy and op != "get":
                self.loss_permitted_ops += 1
            if op == "get":
                if outcome != "ok":
                    continue
                reply = entry.get("reply")
                if in_window and window_reads == "written":
                    # Under a partition a read may be served by a node that is
                    # about to lose its writes (no linearizability), so only
                    # require a value that was actually written to this key.
                    self.unchecked_reads += 1
                    if reply not in ever:
                        self.read_violations.append(
                            {"id": entry["id"], "key": key, "observed": reply, "acceptable": "any written value"}
                        )
                elif reply not in possible:
                    self.read_violations.append(
                        {
                            "id": entry["id"],
                            "key": key,
                            "observed": reply,
                            "acceptable": sorted_values(possible),
                        }
                    )
                continue
            new_value = entry.get("value") if op == "set" else _ABSENT
            if op == "set":
                ever.add(new_value)
            else:
                ever.add(_ABSENT)
            if outcome == "error":
                continue
            if outcome == "ok" and not lossy:
                self.possible[key] = {new_value}
            else:
                possible.add(new_value)

    def keys(self) -> list[str]:
        return sorted(self.possible)


def sorted_values(values: set) -> list:
    return sorted(values, key=lambda item: (item is not None, str(item)))[:5]


def load_ledger(evidence: Sequence[Evidence], workload: str) -> tuple[dict, list[dict], dict]:
    items = evidence_of(evidence, LEDGER_EVIDENCE, workload)
    if not items:
        raise LookupError(f"no {LEDGER_EVIDENCE} evidence from workload {workload!r}")
    summary = to_plain_data(items[-1].value)
    preload = {entry["key"]: entry["value"] for entry in read_jsonl(summary["preload_path"])}
    ledger = read_jsonl(summary["ledger_path"])
    return preload, ledger, summary


def fault_windows(
    evidence: Sequence[Evidence],
    loss_window_s: float,
    faults: Optional[Sequence[str]],
    after_s: float = 0.0,
) -> list[tuple]:
    """Loss windows: ``loss_window_s`` before each fault until ``after_s`` past its end.

    A fault's end is its ``healed_at`` time when it records one (partitions),
    otherwise the moment it was applied.
    """

    return [(start, end) for start, end, _ in fault_records(evidence, loss_window_s, faults, after_s)]


def fault_records(
    evidence: Sequence[Evidence],
    loss_window_s: float,
    faults: Optional[Sequence[str]],
    after_s: float = 0.0,
) -> list[tuple]:
    records = []
    for item in evidence_of(evidence, FAULT_APPLIED):
        if faults is not None and item.source not in faults:
            continue
        value = to_plain_data(item.value)
        applied_at = value.get("applied_at")
        if not isinstance(applied_at, (int, float)):
            continue
        ended = value.get("healed_at")
        ended = ended if isinstance(ended, (int, float)) else applied_at
        targets = value.get("targets") or ([value["target"]] if value.get("target") else [])
        records.append((applied_at - loss_window_s, max(applied_at, ended) + after_s, list(targets)))
    return records


class _Reader:
    """Reads keys from a node or through cluster routing."""

    def __init__(self, handle: Any, node_id: Optional[str]):
        self.handle = handle
        self.cluster = getattr(handle.topology_type, "value", "") == "cluster"
        if node_id is None:
            self.client = handle.cluster_client(timeout=5.0) if self.cluster else None
            if self.client is None:
                self.client = handle.new_client(handle.primaries()[0], timeout=5.0)
        else:
            self.client = handle.new_client(node_id, timeout=5.0)
        # READONLY is per-connection, so it is sent inside each pipeline.
        self.readonly = self.cluster and node_id is not None

    def read(self, keys: Sequence[str]) -> dict[str, Optional[str]]:
        # Pipelined GETs rather than MGET: cluster nodes reject cross-slot MGET.
        values = {}
        for start in range(0, len(keys), 500):
            chunk = list(keys[start : start + 500])
            pipe = self.client.pipeline(transaction=False)
            if self.readonly:
                pipe.execute_command("READONLY")
            for key in chunk:
                pipe.get(key)
            replies = pipe.execute()
            if self.readonly:
                replies = replies[1:]
            for key, value in zip(chunk, replies):
                values[key] = value
        return values

    def close(self) -> None:
        try:
            self.client.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# valkey.ledger — durability/consistency against the workload ledger
# ---------------------------------------------------------------------------

_LEDGER_FIELDS = {
    "workload": Field(str),
    "acknowledged": Field(str, default="must_survive", choices=("must_survive", "may_be_lost")),
    "loss_window_s": Field(float, default=0.0, minimum=0, maximum=3600),
    "loss_window_faults": Field(list, allow_none=True, default=None),
    "loss_window_after_s": Field(float, default=0.0, minimum=0, maximum=3600),
    "loss_window_scope": Field(str, default="all", choices=("all", "fault_target_slots")),
    "lost_writes_budget": Field(int, default=0, minimum=0),
    "check_reads": Field(bool, default=True),
    "window_reads": Field(str, default="strict", choices=("strict", "written")),
    "check_replicas": Field(bool, default=True),
    "converge_timeout_s": Field(float, default=30.0, minimum=0, maximum=600),
}


def _validate_ledger(config: dict, spec: ComponentSpec, plan: RunPlan) -> None:
    known = {component.instance_id for component in plan.components}
    if config["workload"] not in known:
        raise ComponentConfigError(f"workload {config['workload']!r} is not a component of this plan")
    for fault in config["loss_window_faults"] or ():
        if fault not in known:
            raise ComponentConfigError(f"loss_window_faults entry {fault!r} is not a component of this plan")


class LedgerOracle:
    """Final data equals the ledger's acceptable states; replicas converge."""

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = resolved_config(spec, _LEDGER_FIELDS)
        handle = topology(context)
        try:
            preload, ledger, summary = load_ledger(evidence, config["workload"])
        except (LookupError, OSError, ValueError) as exc:
            return make_result(spec, ComponentStatus.ERROR, f"cannot load workload ledger: {exc}")
        lossy_key = None
        if config["acknowledged"] == "may_be_lost":
            windows = [(float("-inf"), float("inf"))]
            scope_slots = None
        else:
            records = fault_records(
                evidence, config["loss_window_s"], config["loss_window_faults"], config["loss_window_after_s"]
            )
            windows = [(start, end) for start, end, _ in records]
            scope_slots = None
            if config["loss_window_scope"] == "fault_target_slots":
                scope_slots = _target_slot_ranges(handle, [target for _, _, targets in records for target in targets])
                if scope_slots is not None:

                    def lossy_key(key: str) -> bool:
                        slot = key_slot(key.encode("utf-8", errors="surrogatepass"))
                        return any(low <= slot <= high for low, high in scope_slots)

        model = LedgerModel(preload, ledger, windows, lossy_key, config["window_reads"])
        keys = model.keys()

        reader = _Reader(handle, None)
        try:
            actual = reader.read(keys)
        except Exception as exc:
            return make_result(spec, ComponentStatus.FAIL, f"cannot read dataset: {exc}")
        finally:
            reader.close()

        violations = []
        for key in keys:
            if actual.get(key) not in model.possible[key]:
                violations.append(
                    {
                        "key": key,
                        "observed": actual.get(key),
                        "acceptable": sorted_values(model.possible[key]),
                        "stable": key.split(":")[-2] == "stable",
                    }
                )
        # How many keys actually took advantage of the loss window: their value
        # is acceptable only because acknowledged writes were allowed to vanish.
        window_losses = []
        if windows:
            strict = LedgerModel(preload, ledger, [])
            window_losses = [
                key
                for key in keys
                if actual.get(key) in model.possible[key] and actual.get(key) not in strict.possible[key]
            ]
        lost = [item for item in violations if item["observed"] is None]
        corrupt = [item for item in violations if item["observed"] is not None]
        problems = []
        if corrupt:
            problems.append(f"{len(corrupt)} key(s) hold a value outside the acceptable set")
        if len(lost) > config["lost_writes_budget"]:
            problems.append(f"{len(lost)} acknowledged write(s) lost (budget {config['lost_writes_budget']})")
        if config["check_reads"] and model.read_violations:
            problems.append(f"{len(model.read_violations)} read(s) returned a value never acceptable at that time")

        replica_report: dict[str, Any] = {}
        if config["check_replicas"]:
            replica_report = self._check_replicas(context, handle, keys, actual, config["converge_timeout_s"])
            diverged = {node: info for node, info in replica_report.items() if info.get("diverged")}
            if diverged:
                problems.append(f"replicas did not converge: {', '.join(sorted(diverged))}")

        record = {
            "keys_checked": len(keys),
            "operations": len(ledger),
            "acknowledged": summary.get("acknowledged"),
            "ambiguous": summary.get("ambiguous"),
            "loss_windows": [list(window) for window in windows if window[0] != float("-inf")],
            "loss_permitted_ops": model.loss_permitted_ops,
            "loss_scope_slots": scope_slots,
            "unchecked_reads": model.unchecked_reads,
            "window_losses": len(window_losses),
            "window_loss_examples": window_losses[:10],
            "violations": violations[:20],
            "read_violations": model.read_violations[:20],
            "replicas": replica_report,
        }
        if problems:
            return make_result(spec, ComponentStatus.FAIL, "; ".join(problems), diagnostics=record)
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{len(keys)} keys match the ledger ({len(ledger)} operations)",
            evidence=(make_evidence(spec, "valkey.data.ledger_verified", "oracle.completed", record),),
        )

    @staticmethod
    def _check_replicas(context: Any, handle: Any, keys: list, primary_values: dict, timeout_s: float) -> dict:
        report: dict[str, Any] = {}
        replicas = handle.replicas()
        cluster = getattr(handle.topology_type, "value", "") == "cluster"
        deadline = time.monotonic() + remaining(context, timeout_s)
        for node_id in replicas:
            expected_keys = keys
            if cluster:
                expected_keys = _keys_owned_by_primary_of(handle, node_id, keys)
            while True:
                reader = _Reader(handle, node_id)
                try:
                    values = reader.read(expected_keys)
                    mismatched = [key for key in expected_keys if values.get(key) != primary_values.get(key)]
                    error = None
                except Exception as exc:
                    mismatched = list(expected_keys)
                    error = f"{type(exc).__name__}: {exc}"
                finally:
                    reader.close()
                if not mismatched or time.monotonic() >= deadline:
                    report[node_id] = {
                        "keys": len(expected_keys),
                        "diverged": bool(mismatched),
                        "mismatched": mismatched[:10],
                        "error": error,
                    }
                    break
                time.sleep(0.2)
        return report


def _target_slot_ranges(handle: Any, targets: Sequence[str]) -> Optional[list]:
    """Planned slot ranges of the shards containing ``targets`` (cluster only)."""

    if getattr(handle.topology_type, "value", "") != "cluster":
        return None
    shards = {handle.node(target).shard_id for target in targets if target}
    ranges = []
    for node in handle.nodes:
        if node.shard_id in shards and node.slot_start is not None:
            ranges.append((node.slot_start, node.slot_end))
    return sorted(set(ranges))


def _keys_owned_by_primary_of(handle: Any, replica_id: str, keys: list) -> list:
    client = handle.new_client(replica_id, timeout=5.0)
    try:
        my_id = client.execute_command("CLUSTER", "MYID")
        nodes = client.execute_command("CLUSTER", "SHARDS")
    finally:
        client.close()
    ranges = []
    for shard in nodes:
        shard_map = _pairs(shard)
        members = [_pairs(member) for member in shard_map.get("nodes", [])]
        if any(member.get("id") == my_id for member in members):
            slots = shard_map.get("slots", [])
            ranges = [(int(slots[i]), int(slots[i + 1])) for i in range(0, len(slots), 2)]
            break
    return [
        key
        for key in keys
        if any(low <= key_slot(key.encode("utf-8", errors="surrogatepass")) <= high for low, high in ranges)
    ]


def _pairs(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    return {value[index]: value[index + 1] for index in range(0, len(value) - 1, 2)}


# ---------------------------------------------------------------------------
# valkey.rdb_snapshot — every captured RDB reloads with plausible data
# ---------------------------------------------------------------------------

_RDB_FIELDS = {
    "bgsave": Field(str),
    "workload": Field(str, allow_none=True, default=None),
    "timeout_s": Field(float, default=60.0, minimum=1, maximum=3600),
}


def _validate_rdb(config: dict, spec: ComponentSpec, plan: RunPlan) -> None:
    known = {component.instance_id for component in plan.components}
    for field in ("bgsave", "workload"):
        if config[field] is not None and config[field] not in known:
            raise ComponentConfigError(f"{field} {config[field]!r} is not a component of this plan")


def load_rdb(binary: str, rdb_path: str, timeout_s: float) -> tuple[Optional[dict], str]:
    """Load ``rdb_path`` into a private server and return all string values."""

    workdir = tempfile.mkdtemp(prefix="vfr-", dir="/tmp" if os.path.isdir("/tmp") else None)
    socket_path = os.path.join(workdir, "s.sock")
    log_path = os.path.join(workdir, "load.log")
    shutil.copyfile(rdb_path, os.path.join(workdir, "dump.rdb"))
    process = subprocess.Popen(
        [
            binary,
            "--port",
            "0",
            "--unixsocket",
            socket_path,
            "--dir",
            workdir,
            "--dbfilename",
            "dump.rdb",
            "--appendonly",
            "no",
            "--save",
            "",
            "--logfile",
            log_path,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    client = None
    try:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if process.poll() is not None:
                break
            if os.path.exists(socket_path):
                try:
                    client = valkey.Valkey(unix_socket_path=socket_path, socket_timeout=10, decode_responses=True)
                    if str(client.info("persistence").get("loading")) == "0":
                        break
                except valkey.exceptions.ConnectionError:
                    client = None
                except valkey.exceptions.BusyLoadingError:
                    pass
            time.sleep(0.05)
        if client is None or process.poll() is not None:
            try:
                with open(log_path, encoding="utf-8", errors="replace") as handle:
                    return None, handle.read()[-2000:]
            except OSError:
                return None, "server exited before loading the RDB"
        snapshot = keyspace_snapshot(client)
        values: dict[str, Optional[str]] = {
            key: (content if kind == "string" else f"<{kind}>") for key, (kind, content) in snapshot.items()
        }
        info = client.info("persistence")
        values_meta = {
            "keys_loaded": info.get("rdb_last_load_keys_loaded"),
            "dbsize": len(values),
            "content_digest": keyspace_digest(snapshot),
        }
        return {"values": values, "meta": values_meta, "snapshot": snapshot}, ""
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        shutil.rmtree(workdir, ignore_errors=True)


class RdbSnapshotOracle:
    """Captured RDBs load, keep every stable key, and hold only written values."""

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = resolved_config(spec, _RDB_FIELDS)
        handle = topology(context)
        results = [
            to_plain_data(item.value) for item in evidence_of(evidence, "valkey.bgsave.result", config["bgsave"])
        ]
        snapshots = [result for result in results if result.get("rdb_artifact")]
        if not snapshots:
            return make_result(spec, ComponentStatus.FAIL, f"no RDB snapshot was captured by {config['bgsave']!r}")
        model = None
        if config["workload"] is not None:
            try:
                preload, ledger, _ = load_ledger(evidence, config["workload"])
            except (LookupError, OSError, ValueError) as exc:
                return make_result(spec, ComponentStatus.ERROR, f"cannot load workload ledger: {exc}")
            model = LedgerModel(preload, ledger, [(float("-inf"), float("inf"))])
        loaded: dict[str, Optional[str]] = {}
        combined: dict[str, list] = {}
        problems = []
        per_node = {}
        for snapshot in snapshots:
            node_id = snapshot["node"]
            content, error = load_rdb(handle.binary(node_id), snapshot["rdb_artifact"], config["timeout_s"])
            if content is None:
                problems.append(f"{node_id}: RDB failed to load: {error[-300:]}")
                per_node[node_id] = {"loaded": False, "error": error[-1000:]}
                continue
            per_node[node_id] = {"loaded": True, **content["meta"]}
            loaded.update(content["values"])
            combined.update(content["snapshot"])
        if model is not None and not problems:
            stable = {key: value for key, value in model.preload.items() if ":stable:" in key}
            missing = [key for key in stable if loaded.get(key) != stable[key]]
            if missing:
                problems.append(f"{len(missing)} stable key(s) missing or wrong in the snapshot (e.g. {missing[0]})")
            foreign = [key for key, value in loaded.items() if key in model.ever and value not in model.ever[key]]
            if foreign:
                problems.append(f"{len(foreign)} key(s) hold values never written (e.g. {foreign[0]})")
        record = {"snapshots": per_node, "keys_loaded": len(loaded), "content_digest": keyspace_digest(combined)}
        if problems:
            return make_result(spec, ComponentStatus.FAIL, "; ".join(problems), diagnostics=record)
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{len(per_node)} snapshot(s) reloaded with {len(loaded)} keys",
            evidence=(make_evidence(spec, "valkey.rdb.reload_verified", "oracle.completed", record),),
        )


# ---------------------------------------------------------------------------
# valkey.dataset_digest — canonical digest of the live dataset
# ---------------------------------------------------------------------------

_DIGEST_FIELDS = {
    "match": Field(str, allow_none=True, default=None),
    "include_replicas": Field(bool, default=False),
    "converge_timeout_s": Field(float, default=30.0, minimum=0, maximum=600),
    "max_keys_listed": Field(int, default=100_000, minimum=0),
}


class DatasetDigestOracle:
    """Record a canonical digest of the dataset held by every primary.

    The digest itself never fails a run; it is evidence for differential
    comparison.  The oracle fails only when a primary cannot be read, or when
    ``include_replicas`` is set and a replica's digest differs from its view
    of the primaries' data.
    """

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = resolved_config(spec, _DIGEST_FIELDS)
        handle = topology(context)
        combined: dict[str, list] = {}
        per_node = {}
        problems = []
        cluster = getattr(handle.topology_type, "value", "") == "cluster"
        for node_id in handle.primaries():
            client = handle.new_client(node_id, timeout=10.0)
            try:
                snapshot = keyspace_snapshot(client, config["match"])
            except Exception as exc:
                problems.append(f"{node_id}: {type(exc).__name__}: {exc}")
                continue
            finally:
                client.close()
            per_node[node_id] = {"keys": len(snapshot), "digest": keyspace_digest(snapshot)}
            duplicate = set(snapshot) & set(combined)
            if duplicate and cluster:
                problems.append(f"{len(duplicate)} key(s) are held by more than one primary")
            combined.update(snapshot)
        replica_report = {}
        if config["include_replicas"] and not problems:
            by_address = {(node.host, node.port): node.node_id for node in handle.nodes}
            deadline = time.monotonic() + remaining(context, config["converge_timeout_s"])
            for node_id in handle.replicas():
                while True:
                    error = None
                    primary_id = None
                    snapshot: dict[str, list] = {}
                    client = handle.new_client(node_id, timeout=10.0)
                    try:
                        info = client.info("replication")
                        primary_id = by_address.get((str(info.get("master_host")), int(info.get("master_port") or 0)))
                        if cluster:
                            client.execute_command("READONLY")
                        snapshot = keyspace_snapshot(client, config["match"])
                    except Exception as exc:
                        error = f"{type(exc).__name__}: {exc}"
                    finally:
                        client.close()
                    expected_digest = per_node.get(primary_id, {}).get("digest") if primary_id else None
                    digest = keyspace_digest(snapshot)
                    matched = error is None and expected_digest is not None and digest == expected_digest
                    if matched or time.monotonic() >= deadline:
                        break
                    time.sleep(0.2)
                replica_report[node_id] = {
                    "primary": primary_id,
                    "keys": len(snapshot),
                    "digest": digest,
                    "matches_primary": matched,
                    "error": error,
                }
                if not matched:
                    problems.append(
                        f"replica {node_id} does not hold its primary's ({primary_id}) dataset"
                        + (f": {error}" if error else "")
                    )
        digest = keyspace_digest(combined)
        listing_path = os.path.join(component_artifact_dir(context, spec), "keyspace.jsonl")
        write_jsonl(
            listing_path,
            (
                {"key": key, "type": value[0], "sha256": keyspace_digest({key: value})[:16]}
                for key, value in sorted(combined.items())[: config["max_keys_listed"]]
            ),
        )
        record = {
            "digest": digest,
            "keys": len(combined),
            "primaries": per_node,
            "replicas": replica_report,
            "listing": listing_path,
        }
        if problems:
            return make_result(spec, ComponentStatus.FAIL, "; ".join(problems), diagnostics=record)
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"dataset digest {digest[:12]} over {len(combined)} keys",
            evidence=(
                make_evidence(spec, "valkey.dataset.digest", "oracle.completed", record, artifacts=(listing_path,)),
            ),
        )


# ---------------------------------------------------------------------------
# valkey.evidence.assert — generic evidence predicates
# ---------------------------------------------------------------------------

_ASSERT_FIELDS = {
    "require": Field(list, default=list),
    "forbid": Field(list, default=list),
}


def _validate_assert(config: dict, spec: ComponentSpec, plan: RunPlan) -> None:
    for group in ("require", "forbid"):
        for index, rule in enumerate(config[group]):
            if not isinstance(rule, dict) or not isinstance(rule.get("evidence_id"), str):
                raise ComponentConfigError(f"{group}[{index}] needs an evidence_id")
            unknown = set(rule) - {"evidence_id", "source", "min_count"}
            if unknown:
                raise ComponentConfigError(f"{group}[{index}] has unknown fields {sorted(unknown)}")
    if not config["require"] and not config["forbid"]:
        raise ComponentConfigError("evidence assertion needs require or forbid rules")


class EvidenceAssertOracle:
    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = resolved_config(spec, _ASSERT_FIELDS)
        problems = []
        for rule in config["require"]:
            count = len(evidence_of(evidence, rule["evidence_id"], rule.get("source")))
            if count < rule.get("min_count", 1):
                problems.append(f"missing {rule['evidence_id']} ({count}/{rule.get('min_count', 1)})")
        for rule in config["forbid"]:
            if evidence_of(evidence, rule["evidence_id"], rule.get("source")):
                problems.append(f"forbidden evidence {rule['evidence_id']} was observed")
        if problems:
            return make_result(spec, ComponentStatus.FAIL, "; ".join(problems))
        return make_result(spec, ComponentStatus.PASS, "evidence assertions hold")


# ---------------------------------------------------------------------------
# valkey.binary_check — every node runs the binary it was assigned
# ---------------------------------------------------------------------------

_BINARY_FIELDS = {
    "min_distinct_versions": Field(int, default=1, minimum=1, maximum=64),
    # Every node must run this binary, e.g. ``{$ref: inputs.to_binary}``.
    "expect_binary": Field(str, allow_none=True, default=None, reference=True),
}


def _same_file(left: Optional[str], right: Optional[str]) -> bool:
    return bool(left) and bool(right) and os.path.realpath(left) == os.path.realpath(right)


class BinaryCheckOracle:
    """Each node's running server is the managed process on its assigned binary."""

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = resolved_config(spec, _BINARY_FIELDS)
        handle = topology(context)
        expected_by_binary: dict[str, dict[str, Optional[str]]] = {}
        nodes = {}
        problems = []
        for node in handle.nodes:
            node_id = node.node_id
            client = handle.new_client(node_id, timeout=5.0)
            try:
                info = client.info("server")
            except Exception as exc:
                problems.append(f"{node_id}: cannot read INFO server: {exc}")
                continue
            finally:
                client.close()
            reported = {
                "version": str(info.get("valkey_version") or info.get("redis_version") or ""),
                "git_sha": str(info.get("valkey_git_sha1") or info.get("redis_git_sha1") or "").lower(),
                "process_id": info.get("process_id"),
                "executable": info.get("executable"),
            }
            assigned = handle.binary(node_id)
            if assigned not in expected_by_binary:
                expected_by_binary[assigned] = binary_version(assigned)
            expected = expected_by_binary[assigned]
            nodes[node_id] = {"binary": assigned, "expected": expected, "reported": reported}
            if config["expect_binary"] is not None and not _same_file(assigned, config["expect_binary"]):
                problems.append(f"{node_id}: assigned {assigned}, expected {config['expect_binary']}")
            if reported["executable"] and not _same_file(reported["executable"], assigned):
                problems.append(f"{node_id}: runs {reported['executable']}, assigned {assigned}")
            if expected["version"] and expected["version"] != reported["version"]:
                problems.append(f"{node_id}: runs {reported['version']}, its binary is {expected['version']}")
            expected_sha = (expected["git_sha"] or "").lower()
            if expected_sha and set(expected_sha) != {"0"} and reported["git_sha"]:
                short = min(len(expected_sha), len(reported["git_sha"]))
                if expected_sha[:short] != reported["git_sha"][:short]:
                    problems.append(f"{node_id}: git sha {reported['git_sha']} does not match its binary")
            if str(reported["process_id"]) != str(handle.pid(node_id)):
                problems.append(f"{node_id}: INFO process_id does not match the managed process")
        versions = sorted({(record["reported"]["version"], record["reported"]["git_sha"]) for record in nodes.values()})
        if len(versions) < config["min_distinct_versions"]:
            problems.append(
                f"only {len(versions)} distinct build(s) are running; expected {config['min_distinct_versions']}"
            )
        result = {"nodes": nodes, "distinct_builds": [list(item) for item in versions], "mixed": len(versions) > 1}
        if problems:
            return make_result(spec, ComponentStatus.FAIL, "; ".join(problems), diagnostics=result)
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{len(nodes)} node(s) run their assigned binaries ({len(versions)} distinct build(s))",
            evidence=(make_evidence(spec, "valkey.binary.verified", "oracle.completed", result),),
        )


REGISTRATIONS = (
    ("valkey.binary_check", "oracle", BinaryCheckOracle, config_validator(_BINARY_FIELDS)),
    ("valkey.liveness", "oracle", LivenessOracle, config_validator(_LIVENESS_FIELDS, extra=_validate_liveness)),
    ("valkey.topology_state", "oracle", TopologyStateOracle, config_validator(_TOPOLOGY_FIELDS)),
    ("valkey.ledger", "oracle", LedgerOracle, config_validator(_LEDGER_FIELDS, extra=_validate_ledger)),
    ("valkey.rdb_snapshot", "oracle", RdbSnapshotOracle, config_validator(_RDB_FIELDS, extra=_validate_rdb)),
    ("valkey.dataset_digest", "oracle", DatasetDigestOracle, config_validator(_DIGEST_FIELDS)),
    (
        "valkey.evidence.assert",
        "oracle",
        EvidenceAssertOracle,
        config_validator(_ASSERT_FIELDS, extra=_validate_assert),
    ),
)
