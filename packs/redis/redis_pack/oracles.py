"""The oracles: ``redis.oracle.liveness``, ``redis.oracle.crash``, and ``redis.oracle.ledger``.

Liveness: the server process runs, answers PING, and is not loading. Crash: neither its log nor its process
output holds a crash report (``=== REDIS BUG REPORT START``, a sanitizer's report); it also judges a run that
failed before the oracles ran, from the files the deployment provider names (``post_mortem``). Ledger: every key
the workload touched holds a value its ledger allows, and no acknowledged read returned one it did not.
"""

import json
import re
import time
from collections.abc import Mapping, Sequence
from typing import Any, Optional

from valkey_fuzzer.declarations import Declarations, EvidenceDeclaration
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.registrations import ComponentRegistration
from valkey_fuzzer.results import ComponentStatus, Evidence, evidence_of, make_evidence, make_result
from valkey_fuzzer.schema import ConfigError, ConfigField, ConfigSchema

from .deployment import INSTANCE_ID
from .workload import LEDGER_EVIDENCE, deployment

# What Redis logs when it crashes, and what a sanitizer prints on its stderr.
CRASH_PATTERNS = (
    r"=== REDIS BUG REPORT START",
    r"crashed by signal",
    r"ASSERTION FAILED",
    r"Guru Meditation",
    r"ERROR: (Address|Leak)Sanitizer",
    r"WARNING: ThreadSanitizer",
    r"SUMMARY: \w*Sanitizer:",
)
# What a report says caused it, most specific first.
CRASH_CAUSES = tuple(
    re.compile(pattern)
    for pattern in (
        r"==> (\S+:\d+ '.*' is not true)",
        r"Guru Meditation: (.*?)\s*$",
        r"(\w*Sanitizer: \S+)",
        r"(crashed by signal: \d+)",
    )
)
MAX_VIOLATIONS = 20

LIVENESS_CONFIG = ConfigSchema(
    {
        "instance": ConfigField(str, default=INSTANCE_ID),
        "timeout_s": ConfigField(float, default=30.0, minimum=0.1, maximum=600),
    }
)
LIVENESS_DECLARES = Declarations(
    evidence={
        "redis.liveness.verified": EvidenceDeclaration(
            ConfigField(
                dict, schema=ConfigSchema({"instance": ConfigField(str), "answered_after_s": ConfigField(float)})
            )
        )
    }
)


class LivenessOracle:
    """The server runs and answers within ``timeout_s``.

    A dead or silent server is a FAIL; whether it crashed is ``redis.oracle.crash``'s to say, from its log.
    """

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        handle = deployment(context)
        instance = spec.config["instance"]
        if not handle.is_alive(instance):
            return make_result(spec, ComponentStatus.FAIL, f"{instance}: process not running")
        started = time.monotonic()
        reason = "not polled"
        while time.monotonic() - started < spec.config["timeout_s"] and not context.cancelled:
            client = handle.new_client(instance, timeout=2.0)
            try:
                client.ping()
                loading = str(client.info("persistence").get("loading", "0"))
                if loading in ("0", "False", "false"):
                    record = {"instance": instance, "answered_after_s": time.monotonic() - started}
                    return make_result(
                        spec,
                        ComponentStatus.PASS,
                        f"{instance} alive and responsive",
                        evidence=(make_evidence(spec, "redis.liveness.verified", "oracle.completed", record),),
                    )
                reason = "still loading"
            except Exception as exc:
                reason = f"{type(exc).__name__}: {exc}"
            finally:
                client.close()
            if not context.sleep(0.1):
                break
        return make_result(spec, ComponentStatus.FAIL, f"{instance}: {reason}")


CRASH_CONFIG = ConfigSchema({"extra_patterns": ConfigField(list, default=list, items=ConfigField(str))})
CRASH_DECLARES = Declarations(
    evidence={
        "redis.crash.none": EvidenceDeclaration(
            ConfigField(dict, schema=ConfigSchema({"scanned": ConfigField(list, items=ConfigField(str))}))
        )
    }
)


def _validate_crash(config: Mapping[str, Any], spec: ComponentSpec, plan: Any) -> None:
    for index, pattern in enumerate(config["extra_patterns"]):
        try:
            re.compile(pattern)
        except re.error as exc:
            raise ConfigError(f"extra_patterns[{index}] is not a valid regex: {exc}") from exc


def scan_for_crashes(paths: Sequence[Optional[str]], patterns: Sequence[str]) -> list[str]:
    """The lines of ``paths`` that match ``patterns``, cut to 500 characters."""

    compiled = [re.compile(pattern) for pattern in patterns]
    found: list[str] = []
    for path in paths:
        if not path:
            continue
        try:
            with open(path, encoding="utf-8", errors="replace") as handle:
                found += [line.rstrip()[:500] for line in handle if any(item.search(line) for item in compiled)]
        except OSError:
            continue
    return found


def crash_cause(lines: Sequence[str]) -> str:
    """What the report ``lines`` say caused the crash (:data:`CRASH_CAUSES`), else their first line."""

    for pattern in CRASH_CAUSES:
        for line in lines:
            match = pattern.search(line)
            if match:
                return match.group(1)
    return lines[0].strip()


class CrashOracle:
    """No crash report in the server's log or process output."""

    def _judge(self, spec: ComponentSpec, files: Mapping[str, Sequence[Optional[str]]]):
        patterns = CRASH_PATTERNS + tuple(spec.config["extra_patterns"])
        reports = {instance: scan_for_crashes(paths, patterns) for instance, paths in files.items()}
        reports = {instance: lines for instance, lines in reports.items() if lines}
        if not reports:
            return None
        causes = {instance: crash_cause(lines) for instance, lines in reports.items()}
        return make_result(
            spec,
            ComponentStatus.FAIL,
            "; ".join(f"{instance}: crash report ({cause})" for instance, cause in causes.items()),
            # ``crashes`` is what the failure signature groups by: one crash is one signature.
            diagnostics={"crash_reports": reports, "crashes": causes},
            crash=True,
        )

    def post_mortem(self, context: Any, spec: ComponentSpec, logs: Mapping[str, Sequence[str]]):
        """A run that failed before the oracles ran: judge the files the deployment provider names; None without a
        report, so the run's own error stands."""

        return self._judge(spec, logs)

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        handle = deployment(context)
        files = {
            instance["instance_id"]: (
                handle.log_file(instance["instance_id"]),
                handle.process_output_file(instance["instance_id"]),
            )
            for instance in handle.instances
        }
        failed = self._judge(spec, files)
        if failed is not None:
            return failed
        scanned = [path for paths in files.values() for path in paths if path]
        return make_result(
            spec,
            ComponentStatus.PASS,
            "no crash reports",
            evidence=(make_evidence(spec, "redis.crash.none", "oracle.completed", {"scanned": scanned}),),
        )


LEDGER_CONFIG = ConfigSchema(
    {
        # The ``redis.workload.set-get`` use whose ledger is checked.
        "workload": ConfigField(str),
        "instance": ConfigField(str, default=INSTANCE_ID),
    }
)
LEDGER_DECLARES = Declarations(
    evidence={
        "redis.ledger.verified": EvidenceDeclaration(
            ConfigField(
                dict,
                schema=ConfigSchema(
                    {
                        "keys": ConfigField(int),
                        "operations": ConfigField(int),
                        "acknowledged": ConfigField(int),
                        "ambiguous": ConfigField(int),
                        "rejected": ConfigField(int),
                    }
                ),
            )
        )
    }
)


def _ledger_references(spec: ComponentSpec) -> list[tuple[str, Optional[str]]]:
    return [(LEDGER_EVIDENCE, spec.config["workload"])]


def read_ledger(path: str) -> list[dict[str, Any]]:
    with open(path, encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


class LedgerModel:
    """The values each key may hold after the ledger's operations, in the writer's order.

    An acknowledged write replaces the key's possible values with its value; an ambiguous one adds its value
    (it may have been applied, now or after a later read); a rejected one changes nothing. An acknowledged read
    must return a possible value, and narrows the possible values to it plus the ambiguous writes since the last
    acknowledged one, which may still land. ``None`` stands for an absent key.
    """

    def __init__(self, ledger: Sequence[Mapping[str, Any]]):
        self.possible: dict[str, set] = {}
        self.read_violations: list[dict[str, Any]] = []
        pending: dict[str, set] = {}
        for entry in ledger:
            key = entry["key"]
            possible = self.possible.setdefault(key, {None})
            if entry["outcome"] == "error":
                continue
            if entry["op"] == "set":
                if entry["outcome"] == "ok":
                    self.possible[key] = {entry["value"]}
                    pending[key] = set()
                else:
                    possible.add(entry["value"])
                    pending.setdefault(key, set()).add(entry["value"])
            elif entry["outcome"] == "ok":
                reply = entry.get("reply")
                if reply not in possible:
                    self.read_violations.append(
                        {"id": entry["id"], "key": key, "observed": reply, "acceptable": sorted(possible, key=str)}
                    )
                else:
                    self.possible[key] = {reply} | pending.get(key, set())


class LedgerOracle:
    """The dataset against the workload's ledger."""

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        items = evidence_of(evidence, LEDGER_EVIDENCE, spec.config["workload"])
        if not items:
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"workload {spec.config['workload']!r} recorded no ledger; nothing to check",
            )
        summary = items[-1].value
        ledger = read_ledger(summary["ledger_path"])
        model = LedgerModel(ledger)
        handle = deployment(context)
        client = handle.new_client(spec.config["instance"], timeout=5.0)
        try:
            keys = sorted(model.possible)
            replies = client.mget(keys) if keys else []
        finally:
            client.close()
        violations = []
        for key, reply in zip(keys, replies):
            observed = reply.decode("utf-8", errors="replace") if isinstance(reply, bytes) else reply
            if observed not in model.possible[key]:
                violations.append(
                    {"key": key, "observed": observed, "acceptable": sorted(model.possible[key], key=str)}
                )
        record = {
            "keys": len(keys),
            "operations": summary["operations"],
            "acknowledged": summary["acknowledged"],
            "ambiguous": summary["ambiguous"],
            "rejected": summary["rejected"],
        }
        if violations or model.read_violations:
            return make_result(
                spec,
                ComponentStatus.FAIL,
                f"{len(violations)} key(s) hold a value the ledger does not allow; "
                f"{len(model.read_violations)} acknowledged read(s) returned one",
                diagnostics={
                    **record,
                    "violations": violations[:MAX_VIOLATIONS],
                    "read_violations": model.read_violations[:MAX_VIOLATIONS],
                },
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{len(keys)} key(s) hold values the ledger allows ({record['acknowledged']} acknowledged, "
            f"{record['ambiguous']} ambiguous operations)",
            evidence=(make_evidence(spec, "redis.ledger.verified", "oracle.completed", record),),
        )


REGISTRATIONS = (
    ComponentRegistration(
        "redis.oracle.liveness",
        ComponentRole.ORACLE,
        LivenessOracle,
        config=LIVENESS_CONFIG,
        declares=LIVENESS_DECLARES,
    ),
    ComponentRegistration(
        "redis.oracle.crash",
        ComponentRole.ORACLE,
        CrashOracle,
        config=CRASH_CONFIG,
        check=_validate_crash,
        declares=CRASH_DECLARES,
    ),
    ComponentRegistration(
        "redis.oracle.ledger",
        ComponentRole.ORACLE,
        LedgerOracle,
        config=LEDGER_CONFIG,
        references_evidence=_ledger_references,
        declares=LEDGER_DECLARES,
    ),
)
