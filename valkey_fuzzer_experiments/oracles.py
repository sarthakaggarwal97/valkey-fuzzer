"""Run-level oracles: liveness and crash reports, deployment state, binaries, evidence assertions.

The workload-ledger oracle lives in :mod:`.ledger` and the dataset oracles in
:mod:`.snapshots`.
"""

import os
import re
from collections.abc import Sequence
from typing import Any, Optional

from valkey_fuzzer.extensions import (
    ComponentSpec,
    ComponentStatus,
    ConfigError,
    ConfigField,
    Declarations,
    Evidence,
    ExecutionPlan,
    evidence_of,
    make_evidence,
    make_result,
    plain_config,
)

from .common import (
    ANY,
    FLAG,
    LIST,
    LOG_LINE,
    MAPPING,
    TEXT,
    component_registration,
    declared,
    deployment,
    mapping_of,
    poll_each,
)
from .observers import FORBIDDEN_LOG_MATCH
from .server import binary_version, server_identity

CRASH_PATTERNS = (
    r"=== (VALKEY|REDIS) BUG REPORT START",
    r"ASSERTION FAILED",
    r"==> \S+:\d+ '.*' is not true",
    r"Guru Meditation",
    r"crashed by signal",
    r"ERROR: AddressSanitizer",
    r"ERROR: LeakSanitizer",
    r"WARNING: ThreadSanitizer",
    r"runtime error: ",
)


# ---------------------------------------------------------------------------
# valkey.oracle.liveness — every Valkey experiment includes it
# ---------------------------------------------------------------------------

_LIVENESS_FIELDS = {
    "timeout_s": ConfigField(float, default=30.0, minimum=0.1, maximum=600),
    "extra_crash_patterns": ConfigField(list, default=list, items=TEXT),
}


def _validate_liveness(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    for index, pattern in enumerate(config["extra_crash_patterns"]):
        try:
            re.compile(pattern)
        except re.error as exc:
            raise ConfigError(f"extra_crash_patterns[{index}] is not a valid regex: {exc}") from exc


def scan_for_crashes(paths: dict[str, str], patterns: Sequence[str]) -> dict[str, list[str]]:
    compiled = [re.compile(pattern) for pattern in patterns]
    findings: dict[str, list[str]] = {}
    for instance_id, path in paths.items():
        try:
            with open(path, encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    if any(pattern.search(line) for pattern in compiled):
                        findings.setdefault(instance_id, []).append(line.rstrip()[:500])
        except OSError:
            continue
    return findings


# What a crash report says caused it, most specific first: an assertion's file:line and condition, a panic,
# a sanitizer's finding, then the signal. Within one kind the first in the log wins: a second assertion
# while the bug report is written is not the cause.
_CRASH_CAUSES = tuple(
    re.compile(pattern)
    for pattern in (
        r"==> (\S+:\d+ '.*' is not true)",
        r"Guru Meditation: (.*?)\s*$",
        r"ERROR: ((?:Address|Leak)Sanitizer: \S+)",
        r"WARNING: (ThreadSanitizer: [^(]*?)\s*(?:\(|$)",
        r"(\S+: runtime error: .*?)\s*$",
        r"(crashed by signal: \d+)",
    )
)


def crash_cause(lines: Sequence[str]) -> str:
    """What caused the crash whose report ``lines`` hold (see ``_CRASH_CAUSES``); its first line otherwise."""

    for pattern in _CRASH_CAUSES:
        for line in lines:
            match = pattern.search(line)
            if match:
                return match.group(1)
    match = LOG_LINE.match(lines[0])
    return (match["message"] if match else lines[0]).strip()


# A fatal exit without a crash report: Valkey logs why at warning level ("#") just before exit(1), as in
# "Short read or OOM loading DB. Unrecoverable error, aborting now.", "Failed listening on port N (cluster),
# aborting.", "Fatal error loading the DB, check server logs. Exiting." or "Terminating server after rdb file
# reading failure.". Other warnings that abort something ("Aborting dual channel sync.") carry on.
_FATAL_EXIT = re.compile(
    r"Unrecoverable error|\b[Aa]borting(?: now|\.|$| \S+ startup)|\b[Ee]xiting\b|\bexit now\b|\bwill now exit\b"
    r"|^(?:\*+ )?(?:FATAL|Fatal)\b|^Terminating server"
)
# A new life of the process, or a load that went on after a fatal-looking line ("... assuming connection
# error, resuming operation."): what came before is not why the process is gone.
_CARRIED_ON = re.compile(r"oO0OoO0OoO0Oo \w+ is starting|resuming operation")


def _warning(line: str) -> Optional[str]:
    """The message of a warning-level log line (``#``), with or without the :data:`LOG_LINE` prefix; else None."""

    match = LOG_LINE.match(line)
    if match is not None:
        return match["message"].rstrip() if match["level"] == "#" else None
    return line[2:].rstrip() if line.startswith("# ") else None


def fatal_exit(path: str) -> Optional[str]:
    """The last fatal-looking warning (see ``_FATAL_EXIT``) the process logging to ``path`` wrote before it
    exited; None if it logged none since it last started or carried on."""

    last = None
    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if _CARRIED_ON.search(line):
                    last = None
                    continue
                warning = _warning(line)
                if warning is not None and _FATAL_EXIT.search(warning):
                    last = line.rstrip()[:500]
    except OSError:
        return None
    return last


EXPECTED_DOWN = "valkey.instances.expected-down"


_HEALTHY = ("ok", "stopped on purpose")


class LivenessOracle:
    """Every instance is alive and responsive, with no crash or sanitizer report.

    Instances listed by ``valkey.instances.expected-down`` evidence
    (``{"instances": [...]}`` from any component that stopped them on purpose)
    may be down; their logs are still scanned for crash reports. Each crash
    is reported by its cause (:func:`crash_cause`), in the summary and as
    ``crashes`` in the diagnostics, which the failure signature uses: one
    crash is one signature, whatever failed after it. An instance that died
    unexpectedly without a crash report exited on a fatal error: its cause
    is the last fatal line it logged (:func:`fatal_exit`), if any. A
    ``valkey.log.forbidden-match`` from a ``valkey.observer.log`` observer
    fails the run too, naming the patterns and the instances that logged them.
    """

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = spec.config
        handle = deployment(context)
        expected_down = {
            instance for item in evidence_of(evidence, EXPECTED_DOWN) for instance in item.value.get("instances", ())
        }

        def responsive(instance_id: str) -> tuple[bool, str]:
            if instance_id in expected_down and not handle.is_alive(instance_id):
                return True, "stopped on purpose"
            with handle.new_client(instance_id, timeout=2.0) as client:
                try:
                    client.ping()
                    if str(client.info("persistence").get("loading", "0")) in ("0", "False", "false"):
                        return True, "ok"
                    return False, "still loading"
                except Exception as exc:
                    return False, f"{type(exc).__name__}: {exc}"

        ids = [instance.instance_id for instance in handle.instances]
        polled = poll_each(
            context, [item for item in ids if handle.is_alive(item)], responsive, timeout_s=config["timeout_s"]
        )
        status = {
            item: polled[item][1]
            if item in polled
            else "stopped on purpose"
            if item in expected_down
            else "process not running"
            for item in ids
        }
        patterns = CRASH_PATTERNS + tuple(config["extra_crash_patterns"])
        crashes = scan_for_crashes(
            {instance.instance_id: handle.log_file(instance.instance_id) for instance in handle.instances}, patterns
        )
        # Sanitizers report on stderr, which the deployment captures separately.
        outputs = {
            instance.instance_id: handle.process_output_file(instance.instance_id) for instance in handle.instances
        }
        found = scan_for_crashes({key: path for key, path in outputs.items() if path}, patterns)
        for instance_id, lines in found.items():
            crashes.setdefault(instance_id, []).extend(lines)
        problems = [f"{instance_id}: {reason}" for instance_id, reason in status.items() if reason not in _HEALTHY]
        causes = {instance_id: crash_cause(lines) for instance_id, lines in crashes.items()}
        problems += [f"{instance_id}: crash report ({cause})" for instance_id, cause in causes.items()]
        for instance_id, reason in status.items():
            if reason != "process not running" or instance_id in crashes:
                continue
            line = fatal_exit(handle.log_file(instance_id))
            if line:
                crashes[instance_id] = [line]
                causes[instance_id] = _warning(line)
                problems.append(f"{instance_id}: fatal exit ({causes[instance_id]})")
        forbidden: dict[str, dict[str, Any]] = {}
        for item in evidence_of(evidence, FORBIDDEN_LOG_MATCH):
            forbidden[item.source] = dict(item.value)
            problems += [
                f"{', '.join(sorted(hits))}: forbidden log pattern {pattern!r} matched (seen by {item.source})"
                for pattern, hits in sorted(item.value.items())
            ]
        record = {"instances": status, "crash_reports": crashes, "crashes": causes, "forbidden_log_matches": forbidden}
        if problems:
            return make_result(spec, ComponentStatus.FAIL, "; ".join(problems), diagnostics=record)
        down = sum(reason == "stopped on purpose" for reason in status.values())
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{len(status) - down} instance(s) alive and responsive"
            + (f", {down} stopped on purpose" if down else "")
            + "; no crash reports",
            evidence=(make_evidence(spec, "valkey.liveness.verified", "oracle.completed", record),),
        )


# ---------------------------------------------------------------------------
# valkey.oracle.deployment-state — the deployment's state validators
# ---------------------------------------------------------------------------

_DEPLOYMENT_STATE_FIELDS = {
    "check_replication": ConfigField(bool, default=True),
    "check_cluster_status": ConfigField(bool, default=True),
    "check_slot_coverage": ConfigField(bool, default=True),
    "check_roles": ConfigField(bool, default=True),
    "check_view_consistency": ConfigField(bool, default=True),
    "stabilization_wait_s": ConfigField(float, default=1.0, minimum=0, maximum=600),
    "timeout_s": ConfigField(float, default=60.0, minimum=1, maximum=600),
}


def _validate_deployment_state(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    if config["stabilization_wait_s"] >= config["timeout_s"]:
        raise ConfigError(
            f"stabilization_wait_s ({config['stabilization_wait_s']:g}s) must be less than timeout_s "
            f"({config['timeout_s']:g}s): the wait would use the whole budget and the validation could never finish"
        )


class DeploymentStateOracle:
    """Run the deployment's own state validators at the end of the run: replication, cluster state, slots, views.

    Every experiment that leaves the deployment as it formed it checks that it still is.
    """

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = spec.config
        handle = deployment(context)
        record = handle.validate_state(
            check_replication=config["check_replication"],
            check_cluster_status=config["check_cluster_status"],
            check_slot_coverage=config["check_slot_coverage"],
            check_roles=config["check_roles"],
            check_view_consistency=config["check_view_consistency"],
            stabilization_wait=config["stabilization_wait_s"],
            timeout=config["timeout_s"],
        )
        if not record["overall_success"]:
            # An inconclusive validation never saw the deployment (an instance did not answer, time ran out).
            return make_result(
                spec,
                ComponentStatus.ERROR if record.get("inconclusive") else ComponentStatus.FAIL,
                "; ".join(record["error_messages"]) or f"failed checks: {record['failed_checks']}",
                diagnostics=record,
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{getattr(handle.mode, 'value', handle.mode)} state validated",
            evidence=(make_evidence(spec, "valkey.deployment.validated", "oracle.completed", record),),
        )


# ---------------------------------------------------------------------------
# valkey.oracle.evidence — generic evidence predicates
# ---------------------------------------------------------------------------

# Evidence ``evidence_id`` from ``source`` (any component when null), at least ``min_count`` times.
_EVIDENCE_RULE = mapping_of(
    {
        "evidence_id": TEXT,
        "source": ConfigField(str, allow_none=True, default=None),
        "min_count": ConfigField(int, default=1, minimum=1),
    }
)
_ASSERT_FIELDS = {
    "require": ConfigField(list, default=list, items=_EVIDENCE_RULE),
    "forbid": ConfigField(list, default=list, items=_EVIDENCE_RULE),
}


def _validate_assert(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    if not config["require"] and not config["forbid"]:
        raise ConfigError("evidence assertion needs require or forbid rules")


def _assert_references(spec: ComponentSpec) -> list[tuple[str, Optional[str]]]:
    """Every rule's evidence: the compiler rejects one no component of the plan declares, so a misspelt ``forbid``
    cannot pass vacuously and a misspelt ``require`` cannot fail the server."""

    config = plain_config(spec.config)
    return [(rule["evidence_id"], rule["source"]) for group in ("require", "forbid") for rule in config[group]]


class EvidenceAssertOracle:
    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = spec.config
        problems = []
        for rule in config["require"]:
            count = len(evidence_of(evidence, rule["evidence_id"], rule["source"]))
            if count < rule["min_count"]:
                problems.append(f"missing {rule['evidence_id']} ({count}/{rule['min_count']})")
        for rule in config["forbid"]:
            if evidence_of(evidence, rule["evidence_id"], rule["source"]):
                problems.append(f"forbidden evidence {rule['evidence_id']} was observed")
        if problems:
            return make_result(spec, ComponentStatus.FAIL, "; ".join(problems))
        return make_result(spec, ComponentStatus.PASS, "evidence assertions hold")


# ---------------------------------------------------------------------------
# valkey.oracle.binary-check — every instance runs the binary it was assigned
# ---------------------------------------------------------------------------

_BINARY_FIELDS = {
    "min_distinct_versions": ConfigField(int, default=1, minimum=1, maximum=64),
    # Every instance must run this binary, e.g. ``{$ref: inputs.to_binary}``.
    "expect_binary": ConfigField(str, allow_none=True, default=None, reference=True),
}


def _same_file(left: Optional[str], right: Optional[str]) -> bool:
    return bool(left) and bool(right) and os.path.realpath(left) == os.path.realpath(right)


class BinaryCheckOracle:
    """Each instance's running server is the managed process on its assigned binary."""

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = spec.config
        handle = deployment(context)
        expected_by_binary: dict[str, dict[str, Optional[str]]] = {}
        instances = {}
        problems = []
        for instance in handle.instances:
            instance_id = instance.instance_id
            with handle.new_client(instance_id, timeout=5.0) as client:
                try:
                    info = client.info("server")
                except Exception as exc:
                    problems.append(f"{instance_id}: cannot read INFO server: {exc}")
                    continue
            reported = server_identity(info)
            assigned = handle.binary(instance_id)
            if assigned not in expected_by_binary:
                expected_by_binary[assigned] = binary_version(assigned)
            expected = expected_by_binary[assigned]
            instances[instance_id] = {"binary": assigned, "expected": expected, "reported": reported}
            if config["expect_binary"] is not None and not _same_file(assigned, config["expect_binary"]):
                problems.append(f"{instance_id}: assigned {assigned}, expected {config['expect_binary']}")
            if reported["executable"] and not _same_file(reported["executable"], assigned):
                problems.append(f"{instance_id}: runs {reported['executable']}, assigned {assigned}")
            if expected["version"] and expected["version"] != reported["version"]:
                problems.append(f"{instance_id}: runs {reported['version']}, its binary is {expected['version']}")
            # INFO parsing turns an all-digit SHA into an integer, so compare
            # without leading zeros.
            expected_sha = (expected["git_sha"] or "").lower().lstrip("0")
            reported_sha = reported["git_sha"].lstrip("0")
            if expected_sha and reported_sha:
                short = min(len(expected_sha), len(reported_sha))
                if expected_sha[:short] != reported_sha[:short]:
                    problems.append(f"{instance_id}: git sha {reported['git_sha']} does not match its binary")
            if str(reported["process_id"]) != str(handle.pid(instance_id)):
                problems.append(f"{instance_id}: INFO process_id does not match the managed process")
        versions = sorted(
            {(record["reported"]["version"], record["reported"]["git_sha"]) for record in instances.values()}
        )
        if len(versions) < config["min_distinct_versions"]:
            problems.append(
                f"only {len(versions)} distinct build(s) are running; expected {config['min_distinct_versions']}"
            )
        result = {
            "instances": instances,
            "distinct_builds": [list(item) for item in versions],
            "mixed": len(versions) > 1,
        }
        if problems:
            return make_result(spec, ComponentStatus.FAIL, "; ".join(problems), diagnostics=result)
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{len(instances)} instance(s) run their assigned binaries ({len(versions)} distinct build(s))",
            evidence=(make_evidence(spec, "valkey.binary.verified", "oracle.completed", result),),
        )


_LIVENESS_DECLARES = Declarations(
    evidence={
        "valkey.liveness.verified": declared(
            mapping_of(
                {"instances": MAPPING, "crash_reports": MAPPING, "crashes": MAPPING, "forbidden_log_matches": MAPPING}
            )
        )
    }
)
# The deployment service's report-safe validation summary.
_STATE_DECLARES = Declarations(
    evidence={"valkey.deployment.validated": declared(mapping_of({"overall_success": FLAG}, additional=ANY))}
)
_BINARY_DECLARES = Declarations(
    evidence={
        "valkey.binary.verified": declared(mapping_of({"instances": MAPPING, "distinct_builds": LIST, "mixed": FLAG}))
    }
)

REGISTRATIONS = (
    component_registration(
        "valkey.oracle.liveness",
        LivenessOracle,
        _LIVENESS_FIELDS,
        check=_validate_liveness,
        declares=_LIVENESS_DECLARES,
    ),
    component_registration(
        "valkey.oracle.deployment-state",
        DeploymentStateOracle,
        _DEPLOYMENT_STATE_FIELDS,
        check=_validate_deployment_state,
        declares=_STATE_DECLARES,
    ),
    component_registration("valkey.oracle.binary-check", BinaryCheckOracle, _BINARY_FIELDS, declares=_BINARY_DECLARES),
    component_registration(
        "valkey.oracle.evidence",
        EvidenceAssertOracle,
        _ASSERT_FIELDS,
        check=_validate_assert,
        references_evidence=_assert_references,
        declares=Declarations(),
    ),
)
