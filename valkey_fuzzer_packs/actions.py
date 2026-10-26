"""Built-in actions: generic commands, BGSAVE, failover, restart, and waits."""

import os
import re
import shutil
import signal
import subprocess
import time
from typing import Any, Optional

from valkey_fuzzer.extensions import ComponentStatus

from .common import (
    ComponentConfigError,
    ComponentSpec,
    Field,
    RunPlan,
    compare,
    component_artifact_dir,
    config_validator,
    jsonable,
    make_evidence,
    make_result,
    remaining,
    resolved_config,
    select_nodes,
    topology,
    validate_node_selector,
    validate_predicate,
)

# ---------------------------------------------------------------------------
# valkey.command
# ---------------------------------------------------------------------------

_COMMAND_FIELDS = {
    "nodes": Field((str, list), default="primaries"),
    "command": Field(list),
    "expect": Field(dict, allow_none=True, default=None),
    "publish": Field(str, allow_none=True, default=None),
    "repeat": Field(int, default=1, minimum=1, maximum=100_000),
    "interval_s": Field(float, default=0.0, minimum=0, maximum=3600),
    "timeout_s": Field(float, default=10.0, minimum=0.1, maximum=3600),
}


def _validate_command(config: dict, spec: ComponentSpec, plan: RunPlan) -> None:
    validate_node_selector(config["nodes"])
    command = config["command"]
    if not command or not all(isinstance(item, (str, int, float)) and not isinstance(item, bool) for item in command):
        raise ComponentConfigError("command must be a non-empty list of strings or numbers")
    if config["expect"] is not None:
        expect = config["expect"]
        if "error" in expect:
            if set(expect) != {"error"} or not isinstance(expect["error"], (bool, str)):
                raise ComponentConfigError("expect.error must be true, false, or an error substring")
        else:
            validate_predicate(expect, "expect")


def run_command_on_nodes(context: Any, spec: ComponentSpec, config: dict) -> tuple[bool, list[dict]]:
    handle = topology(context)
    replies = []
    all_ok = True
    for node_id in select_nodes(handle, config["nodes"]):
        for attempt in range(config["repeat"]):
            record: dict[str, Any] = {"node": node_id, "attempt": attempt, "at": time.time()}
            client = handle.new_client(node_id, timeout=config["timeout_s"])
            try:
                reply = client.execute_command(*[str(item) for item in config["command"]])
                record["reply"] = jsonable(reply)
                error = None
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                record["error"] = error
            finally:
                client.close()
            expect = config["expect"]
            if expect is None:
                record["matched"] = error is None
            elif "error" in expect:
                wanted = expect["error"]
                if wanted is False:
                    record["matched"] = error is None
                elif wanted is True:
                    record["matched"] = error is not None
                else:
                    record["matched"] = error is not None and str(wanted) in error
            else:
                record["matched"] = error is None and compare(record.get("reply"), expect)
            all_ok = all_ok and record["matched"]
            replies.append(record)
            if config["interval_s"] and attempt + 1 < config["repeat"]:
                time.sleep(min(config["interval_s"], remaining(context, config["interval_s"])))
    return all_ok, replies


class CommandAction:
    def run(self, context: Any, spec: ComponentSpec):
        config = resolved_config(spec, _COMMAND_FIELDS)
        all_ok, replies = run_command_on_nodes(context, spec, config)
        if config["publish"]:
            context.event_bus.publish(config["publish"], source=spec.instance_id, payload={"matched": all_ok})
        evidence = (make_evidence(spec, "valkey.command.replies", "action.completed", replies),)
        status = ComponentStatus.PASS if all_ok else ComponentStatus.FAIL
        return make_result(
            spec,
            status,
            f"{' '.join(str(item) for item in config['command'])}: "
            f"{sum(item['matched'] for item in replies)}/{len(replies)} replies matched",
            evidence=evidence,
        )


# ---------------------------------------------------------------------------
# valkey.bgsave
# ---------------------------------------------------------------------------

_BGSAVE_FIELDS = {
    "nodes": Field((str, list), default="primaries"),
    "args": Field(list, default=list),
    "timeout_s": Field(float, default=120.0, minimum=0.5, maximum=7200),
    "poll_interval_s": Field(float, default=0.02, minimum=0.001, maximum=10),
    "expect": Field(str, default="ok", choices=("ok", "err", "not_completed", "any")),
    "expect_type": Field(str, allow_none=True, default=None, choices=("fork", "forkless", None)),
    "capture_rdb": Field(bool, default=True),
    "started_event": Field(str, default="bgsave.started"),
    "completed_event": Field(str, default="bgsave.completed"),
}


def _validate_bgsave(config: dict, spec: ComponentSpec, plan: RunPlan) -> None:
    validate_node_selector(config["nodes"])
    if not all(isinstance(item, str) for item in config["args"]):
        raise ComponentConfigError("args must be a list of strings")


def _snapshot_identity(path: str) -> Optional[tuple[int, int, int]]:
    try:
        stat = os.stat(path)
    except OSError:
        return None
    return (stat.st_ino, stat.st_mtime_ns, stat.st_size)


class _SaveTarget:
    def __init__(self, handle: Any, node_id: str):
        self.handle = handle
        self.node_id = node_id
        self.client = handle.new_client(node_id, timeout=5.0)
        directory = self.client.config_get("dir").get("dir") or handle.data_dir(node_id)
        dbfilename = self.client.config_get("dbfilename").get("dbfilename") or "dump.rdb"
        self.rdb_path = os.path.join(directory, dbfilename)
        persistence = self.client.info("persistence")
        self.before = {
            "rdb_saves": persistence.get("rdb_saves"),
            "rdb_last_bgsave_type": persistence.get("rdb_last_bgsave_type"),
            "rdb_last_bgsave_status": persistence.get("rdb_last_bgsave_status"),
            "snapshot": _snapshot_identity(self.rdb_path),
        }
        self.observed_types: set[str] = set()
        self.reply: Any = None
        self.error: Optional[str] = None
        self.finished = False
        self.unavailable = False
        self.final: dict[str, Any] = {}

    def poll(self) -> None:
        try:
            persistence = self.client.info("persistence")
        except Exception as exc:
            self.unavailable = True
            self.error = self.error or f"{type(exc).__name__}: {exc}"
            self.finished = True
            return
        current = persistence.get("rdb_current_bgsave_type")
        if current not in (None, "none"):
            self.observed_types.add(str(current))
        if str(persistence.get("rdb_bgsave_in_progress")) == "0":
            self.finished = True
            self.final = {
                "rdb_saves": persistence.get("rdb_saves"),
                "rdb_last_bgsave_type": persistence.get("rdb_last_bgsave_type"),
                "rdb_last_bgsave_status": persistence.get("rdb_last_bgsave_status"),
                "rdb_last_bgsave_time_sec": persistence.get("rdb_last_bgsave_time_sec"),
            }

    def status(self) -> str:
        if self.unavailable:
            return "unavailable"
        if not self.finished:
            return "timeout"
        if self.final.get("rdb_last_bgsave_status") == "err":
            return "err"
        if _snapshot_identity(self.rdb_path) not in (None, self.before["snapshot"]):
            return "ok"
        return "not_completed"

    def close(self) -> None:
        try:
            self.client.close()
        except Exception:
            pass


class BgsaveAction:
    """Issue BGSAVE on selected nodes and follow it to completion."""

    def run(self, context: Any, spec: ComponentSpec):
        config = resolved_config(spec, _BGSAVE_FIELDS)
        handle = topology(context)
        nodes = select_nodes(handle, config["nodes"])
        targets = [_SaveTarget(handle, node_id) for node_id in nodes]
        started_at = time.time()
        try:
            for target in targets:
                try:
                    target.reply = jsonable(target.client.execute_command("BGSAVE", *config["args"]))
                except Exception as exc:
                    target.error = f"{type(exc).__name__}: {exc}"
                    target.finished = True
            started = context.event_bus.publish(
                config["started_event"],
                source=spec.instance_id,
                payload={"nodes": nodes, "accepted": [target.node_id for target in targets if target.error is None]},
            )
            deadline = time.monotonic() + remaining(context, config["timeout_s"])
            while time.monotonic() < deadline and not all(target.finished for target in targets):
                for target in targets:
                    if not target.finished:
                        target.poll()
                time.sleep(config["poll_interval_s"])
            results = []
            artifacts = []
            for target in targets:
                status = target.status() if target.reply is not None or target.unavailable else "rejected"
                record = {
                    "node": target.node_id,
                    "reply": target.reply,
                    "status": status,
                    "observed_types": sorted(target.observed_types),
                    "before": jsonable(target.before),
                    "after": jsonable(target.final),
                    "rdb_path": target.rdb_path,
                    "error": target.error,
                }
                if config["capture_rdb"] and status == "ok":
                    destination = os.path.join(component_artifact_dir(context, spec), f"{target.node_id}.rdb")
                    try:
                        shutil.copyfile(target.rdb_path, destination)
                        record["rdb_artifact"] = destination
                        artifacts.append(destination)
                    except OSError as exc:
                        record["capture_error"] = str(exc)
                results.append(record)
            completed = context.event_bus.publish(
                config["completed_event"],
                source=spec.instance_id,
                payload={"statuses": {record["node"]: record["status"] for record in results}},
            )
        finally:
            for target in targets:
                target.close()

        evidence = [
            make_evidence(
                spec,
                "valkey.bgsave.result",
                "action.completed",
                record,
                event_sequence=completed.sequence,
                artifacts=[record["rdb_artifact"]] if "rdb_artifact" in record else (),
            )
            for record in results
        ]
        problems = []
        for record in results:
            if config["expect"] != "any" and record["status"] != config["expect"]:
                problems.append(f"{record['node']}: expected {config['expect']}, got {record['status']}")
            if config["expect_type"] and config["expect"] == "ok":
                last_type = record["after"].get("rdb_last_bgsave_type")
                if last_type != config["expect_type"]:
                    problems.append(f"{record['node']}: last save type {last_type!r}, expected {config['expect_type']}")
        status = ComponentStatus.FAIL if problems else ComponentStatus.PASS
        return make_result(
            spec,
            status,
            "; ".join(problems)
            or f"BGSAVE {config['expect']} on {len(results)} node(s) in {time.time() - started_at:.2f}s",
            evidence=evidence,
            diagnostics={"started_event": started.sequence, "results": results},
        )


# ---------------------------------------------------------------------------
# valkey.failover
# ---------------------------------------------------------------------------

_FAILOVER_FIELDS = {
    "shard": Field(int, allow_none=True, default=None, minimum=0),
    "target": Field(str, allow_none=True, default=None),
    "force": Field(bool, default=False),
    "timeout_s": Field(float, default=60.0, minimum=1, maximum=600),
}


def _validate_failover(config: dict, spec: ComponentSpec, plan: RunPlan) -> None:
    if config["shard"] is not None and config["target"] is not None:
        raise ComponentConfigError("set either shard or target, not both")
    if config["target"] is not None:
        validate_node_selector(config["target"], "target")


class FailoverAction:
    """Fail over a shard's current primary through the topology's failover operation.

    ``target`` names a planned node (its shard is failed over); ``shard``
    names the shard directly.  The default is shard 0.
    """

    def run(self, context: Any, spec: ComponentSpec):
        config = resolved_config(spec, _FAILOVER_FIELDS)
        handle = topology(context)
        shard = config["shard"]
        if config["target"] is not None:
            shard = handle.node(select_nodes(handle, config["target"])[0]).shard_id
        shard = 0 if shard is None else shard
        config["target"] = f"shard-{shard}-primary"
        context.event_bus.publish("failover.started", source=spec.instance_id, payload={"target": config["target"]})
        ok = handle.failover(shard, force=config["force"], timeout=config["timeout_s"])
        completed = context.event_bus.publish(
            "failover.completed", source=spec.instance_id, payload={"target": config["target"], "success": ok}
        )
        evidence = (
            make_evidence(
                spec,
                "valkey.failover.result",
                "action.completed",
                {"target": config["target"], "force": config["force"], "success": ok, "roles": handle.roles()},
                event_sequence=completed.sequence,
            ),
        )
        return make_result(
            spec,
            ComponentStatus.PASS if ok else ComponentStatus.FAIL,
            f"failover of {config['target']} {'completed' if ok else 'failed'}",
            evidence=evidence,
        )


# ---------------------------------------------------------------------------
# valkey.restart_node
# ---------------------------------------------------------------------------

_RESTART_FIELDS = {
    "node": Field(str),
    "wait_ready": Field(bool, default=True),
}


class RestartAction:
    def run(self, context: Any, spec: ComponentSpec):
        config = resolved_config(spec, _RESTART_FIELDS)
        handle = topology(context)
        node_id = select_nodes(handle, config["node"])[0]
        context.event_bus.publish("node.restarting", source=spec.instance_id, payload={"node": node_id})
        try:
            if handle.is_alive(node_id):
                handle.send_signal(node_id, signal.SIGTERM)
                handle.wait_exit(node_id, timeout=30)
            handle.restart(node_id, wait_ready=config["wait_ready"])
        except Exception as exc:
            return make_result(spec, ComponentStatus.FAIL, f"restart of {node_id} failed: {exc}")
        restarted = context.event_bus.publish("node.restarted", source=spec.instance_id, payload={"node": node_id})
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"restarted {node_id}",
            evidence=(
                make_evidence(
                    spec,
                    "valkey.node.restarted",
                    "action.completed",
                    {"node": node_id, "pid": handle.pid(node_id)},
                    event_sequence=restarted.sequence,
                ),
            ),
        )


# ---------------------------------------------------------------------------
# valkey.wait_for
# ---------------------------------------------------------------------------

_WAIT_FIELDS = {
    "nodes": Field((str, list), default="all"),
    "section": Field(str, allow_none=True, default=None),
    "field": Field(str, allow_none=True, default=None),
    "predicate": Field(dict, allow_none=True, default=None),
    "seconds": Field(float, allow_none=True, default=None, minimum=0, maximum=3600),
    "timeout_s": Field(float, default=60.0, minimum=0.1, maximum=7200),
    "poll_interval_s": Field(float, default=0.1, minimum=0.001, maximum=60),
    "publish": Field(str, allow_none=True, default=None),
}


def _validate_wait(config: dict, spec: ComponentSpec, plan: RunPlan) -> None:
    validate_node_selector(config["nodes"])
    if config["seconds"] is None:
        if not (config["section"] and config["field"] and config["predicate"]):
            raise ComponentConfigError("wait_for needs seconds or section, field, and predicate")
        validate_predicate(config["predicate"], "predicate")


class WaitForAction:
    def run(self, context: Any, spec: ComponentSpec):
        config = resolved_config(spec, _WAIT_FIELDS)
        if config["seconds"] is not None:
            time.sleep(remaining(context, config["seconds"]))
            if config["publish"]:
                context.event_bus.publish(config["publish"], source=spec.instance_id)
            return make_result(spec, ComponentStatus.PASS, f"waited {config['seconds']}s")
        handle = topology(context)
        nodes = select_nodes(handle, config["nodes"])
        deadline = time.monotonic() + remaining(context, config["timeout_s"])
        last: dict[str, Any] = {}
        while True:
            satisfied = True
            for node_id in nodes:
                client = handle.new_client(node_id, timeout=2.0)
                try:
                    value = client.info(config["section"]).get(config["field"])
                except Exception as exc:
                    value = None
                    last[node_id] = f"error: {exc}"
                else:
                    last[node_id] = value
                finally:
                    client.close()
                if not compare(value, config["predicate"]):
                    satisfied = False
            if satisfied:
                if config["publish"]:
                    context.event_bus.publish(config["publish"], source=spec.instance_id, payload={"values": last})
                return make_result(
                    spec,
                    ComponentStatus.PASS,
                    f"{config['section']}.{config['field']} satisfied on {len(nodes)} node(s)",
                    evidence=(make_evidence(spec, "valkey.wait.satisfied", "action.completed", last),),
                )
            if time.monotonic() >= deadline:
                return make_result(
                    spec,
                    ComponentStatus.FAIL,
                    f"{config['section']}.{config['field']} not satisfied before timeout",
                    diagnostics={"last_values": last},
                )
            time.sleep(config["poll_interval_s"])


# ---------------------------------------------------------------------------
# valkey.rolling_upgrade
# ---------------------------------------------------------------------------

_UPGRADE_FIELDS = {
    # Usually ``{$ref: artifacts.<id>.binary}`` passed in through a pack input.
    "to_binary": Field(str, reference=True),
    "failover": Field(bool, default=True),
    "settle_s": Field(float, default=1.0, minimum=0, maximum=600),
    "step_timeout_s": Field(float, default=60.0, minimum=1, maximum=3600),
}


def binary_version(binary: str) -> dict[str, Optional[str]]:
    """Return ``{version, git_sha}`` reported by ``binary --version``."""

    try:
        output = subprocess.run([binary, "--version"], capture_output=True, text=True, timeout=10, check=True).stdout
    except (OSError, subprocess.SubprocessError):
        return {"version": None, "git_sha": None}
    version = re.search(r"\bv=(\S+)", output)
    sha = re.search(r"sha=([0-9a-f]{7,40}):", output, re.IGNORECASE)
    return {
        "version": version.group(1) if version else None,
        "git_sha": sha.group(1).lower() if sha else None,
    }


def _server_identity(handle: Any, node_id: str) -> dict[str, Any]:
    client = handle.new_client(node_id, timeout=5.0)
    try:
        info = client.info("server")
    finally:
        client.close()
    return {
        "version": str(info.get("valkey_version") or info.get("redis_version") or ""),
        "git_sha": str(info.get("valkey_git_sha1") or info.get("redis_git_sha1") or ""),
        "process_id": info.get("process_id"),
    }


def _role_of(handle: Any, node_id: str) -> Optional[str]:
    client = handle.new_client(node_id, timeout=2.0)
    try:
        info = client.info("replication")
    except Exception:
        return None
    finally:
        client.close()
    role = str(info.get("role", "")).lower()
    if role in ("master", "primary"):
        return "primary"
    if str(info.get("master_link_status", "")) != "up":
        return "replica-syncing"
    return "replica"


class RollingUpgradeAction:
    """Move every node to ``to_binary`` one at a time, replicas first.

    Per shard: restart each replica on the new build, gracefully fail over to
    an upgraded replica (``CLUSTER FAILOVER`` or ``FAILOVER TO``), then
    restart the former primary on the new build.  Without replicas, or with
    ``failover: false``, the primary restarts in place (expected downtime).
    """

    def run(self, context: Any, spec: ComponentSpec):
        config = resolved_config(spec, _UPGRADE_FIELDS)
        handle = topology(context)
        cluster = getattr(handle.topology_type, "value", "") == "cluster"
        target_record = binary_version(config["to_binary"])
        config["target"] = target_record
        steps: list[dict[str, Any]] = []
        context.event_bus.publish(
            "upgrade.started", source=spec.instance_id, payload={"to_version": target_record["version"]}
        )
        try:
            handle.refresh_roles()
            for shard in sorted({node.shard_id for node in handle.nodes}):
                members = [node.node_id for node in handle.nodes if node.shard_id == shard]
                roles = handle.roles()
                primaries = [node_id for node_id in members if roles.get(node_id) == "primary"]
                if len(primaries) != 1:
                    raise RuntimeError(f"shard {shard} has {len(primaries)} live primaries: {primaries}")
                primary = primaries[0]
                replicas = [node_id for node_id in members if node_id != primary]
                for replica in replicas:
                    self._upgrade(context, spec, handle, replica, config, steps)
                if config["failover"] and replicas:
                    self._failover(context, spec, handle, primary, replicas, cluster, config, steps)
                self._upgrade(context, spec, handle, primary, config, steps)
        except Exception as exc:
            return make_result(
                spec,
                ComponentStatus.FAIL,
                f"rolling upgrade failed: {type(exc).__name__}: {exc}",
                diagnostics={"steps": steps},
            )
        completed = context.event_bus.publish(
            "upgrade.completed", source=spec.instance_id, payload={"to_version": target_record["version"]}
        )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"upgraded {sum(step['kind'] == 'restart' for step in steps)} node(s) to {target_record['version']} "
            f"with {sum(step['kind'] == 'failover' for step in steps)} failover(s)",
            evidence=(
                make_evidence(
                    spec,
                    "valkey.upgrade.completed",
                    "action.completed",
                    {"target": target_record, "steps": steps},
                    event_sequence=completed.sequence,
                ),
            ),
            diagnostics={"steps": steps},
        )

    @staticmethod
    def _upgrade(context: Any, spec: ComponentSpec, handle: Any, node_id: str, config: dict, steps: list) -> None:
        previous = handle.binary(node_id)
        if os.path.realpath(previous) == os.path.realpath(config["to_binary"]):
            steps.append({"kind": "skip", "node": node_id, "reason": "already on target"})
            return
        before = _server_identity(handle, node_id)
        context.event_bus.publish("upgrade.node.started", source=spec.instance_id, payload={"node": node_id})
        role = handle.refresh_roles().get(node_id)
        handle.set_binary(node_id, config["to_binary"])
        handle.send_signal(node_id, signal.SIGTERM)
        if not handle.wait_exit(node_id, timeout=config["step_timeout_s"]):
            raise RuntimeError(f"{node_id} did not exit within {config['step_timeout_s']}s")
        started = time.time()
        handle.restart(node_id, wait_ready=True)
        after = _server_identity(handle, node_id)
        expected_version = config["target"].get("version")
        if expected_version and after["version"] != expected_version:
            raise RuntimeError(f"{node_id} restarted on {after['version']}, expected {expected_version}")
        steps.append(
            {
                "kind": "restart",
                "node": node_id,
                "before": before,
                "after": after,
                "seconds": round(time.time() - started, 3),
            }
        )
        if role == "replica":
            # A replica must be linked and synced before it can take over.
            deadline = time.monotonic() + config["step_timeout_s"]
            while _role_of(handle, node_id) != "replica":
                if time.monotonic() >= deadline:
                    raise RuntimeError(f"{node_id} did not resynchronize within {config['step_timeout_s']}s")
                time.sleep(0.1)
        context.event_bus.publish("upgrade.node.completed", source=spec.instance_id, payload={"node": node_id})
        if config["settle_s"]:
            time.sleep(config["settle_s"])

    @staticmethod
    def _failover(
        context: Any,
        spec: ComponentSpec,
        handle: Any,
        primary: str,
        replicas: list,
        cluster: bool,
        config: dict,
        steps: list,
    ) -> None:
        target = replicas[0]
        started = time.time()
        if cluster:
            client = handle.new_client(target, timeout=10.0)
            try:
                client.execute_command("CLUSTER", "FAILOVER")
            finally:
                client.close()
        else:
            node = handle.node(target)
            client = handle.new_client(primary, timeout=15.0)
            try:
                client.execute_command("FAILOVER", "TO", node.host, node.port, "TIMEOUT", 10000)
            finally:
                client.close()
        deadline = time.monotonic() + config["step_timeout_s"]
        while time.monotonic() < deadline:
            if _role_of(handle, target) == "primary" and _role_of(handle, primary) == "replica":
                break
            time.sleep(0.1)
        else:
            raise RuntimeError(f"failover from {primary} to {target} did not complete")
        if not cluster:
            # Other replicas keep following the demoted primary; point them at
            # the new one so the topology stays a single-level star.
            new_primary = handle.node(target)
            for other in replicas[1:]:
                client = handle.new_client(other, timeout=5.0)
                try:
                    client.execute_command("REPLICAOF", new_primary.host, new_primary.port)
                finally:
                    client.close()
            deadline = time.monotonic() + config["step_timeout_s"]
            while any(_role_of(handle, other) != "replica" for other in replicas[1:]):
                if time.monotonic() >= deadline:
                    raise RuntimeError(f"replicas did not follow the new primary {target} in time")
                time.sleep(0.1)
        handle.refresh_roles()
        steps.append({"kind": "failover", "from": primary, "to": target, "seconds": round(time.time() - started, 3)})
        context.event_bus.publish(
            "upgrade.failover.completed", source=spec.instance_id, payload={"from": primary, "to": target}
        )


# ---------------------------------------------------------------------------
# valkey.flow.wait_event
# ---------------------------------------------------------------------------

_WAIT_EVENT_FIELDS = {
    "event": Field(str),
    "min_count": Field(int, default=1, minimum=1, maximum=1_000_000),
    "timeout_s": Field(float, default=120.0, minimum=0.01, maximum=7200),
    "poll_interval_s": Field(float, default=0.02, minimum=0.001, maximum=10),
    "publish": Field(str, allow_none=True, default=None),
}


class WaitEventAction:
    """Block until a lifecycle event (e.g. ``workload.drained``) is committed."""

    def run(self, context: Any, spec: ComponentSpec):
        config = resolved_config(spec, _WAIT_EVENT_FIELDS)
        deadline = time.monotonic() + remaining(context, config["timeout_s"])
        while True:
            events = context.event_bus.committed_history(config["event"])
            if len(events) >= config["min_count"]:
                if config["publish"]:
                    context.event_bus.publish(config["publish"], source=spec.instance_id)
                return make_result(
                    spec,
                    ComponentStatus.PASS,
                    f"observed {config['event']} x{len(events)}",
                    evidence=(
                        make_evidence(
                            spec,
                            "valkey.flow.event_observed",
                            "action.completed",
                            {"event": config["event"], "count": len(events)},
                            event_sequence=events[config["min_count"] - 1].sequence,
                        ),
                    ),
                )
            if time.monotonic() >= deadline:
                return make_result(
                    spec,
                    ComponentStatus.FAIL,
                    f"{config['event']} observed {len(events)}/{config['min_count']} times before timeout",
                )
            time.sleep(config["poll_interval_s"])


REGISTRATIONS = (
    ("valkey.flow.wait_event", "action", WaitEventAction, config_validator(_WAIT_EVENT_FIELDS)),
    (
        "valkey.rolling_upgrade",
        "action",
        RollingUpgradeAction,
        config_validator(_UPGRADE_FIELDS, topologies=("replication", "cluster")),
    ),
    ("valkey.command", "action", CommandAction, config_validator(_COMMAND_FIELDS, extra=_validate_command)),
    ("valkey.bgsave", "action", BgsaveAction, config_validator(_BGSAVE_FIELDS, extra=_validate_bgsave)),
    (
        "valkey.failover",
        "action",
        FailoverAction,
        config_validator(_FAILOVER_FIELDS, extra=_validate_failover, topologies=("cluster",)),
    ),
    ("valkey.restart_node", "action", RestartAction, config_validator(_RESTART_FIELDS)),
    ("valkey.wait_for", "action", WaitForAction, config_validator(_WAIT_FIELDS, extra=_validate_wait)),
)
