"""The spec bridge on the toy register bundle in ``tests/fixtures/spec/register``, whose negative control
acknowledges a write before saving it, so a crash loses it. The slow tests run TLC (java and ``$TLA2TOOLS_JAR``)."""

import argparse
import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from unittest.mock import Mock

import pytest

from tests.valkey_experiments_support import valkey_service
from valkey_fuzzer.cli import create_parser
from valkey_fuzzer.commands import spec as spec_command
from valkey_fuzzer.commands._common import add_common_options
from valkey_fuzzer.spec import (
    SpecBundleError,
    TlaTools,
    TranslateError,
    bundle_service,
    check_bundle,
    collect_runs,
    conform,
    conform_runs,
    load_bundle,
    tla,
    translate_trace,
)
from valkey_fuzzer.spec.bundle import load_spec_mapping
from valkey_fuzzer.spec.check import log_patterns_gate
from valkey_fuzzer.spec.conform import ObservedRun, check_run, conform_folder
from valkey_fuzzer.spec.tla import (
    ModelValue,
    TlaError,
    TlaSet,
    TraceState,
    changed_keys,
    parse_value,
    print_values,
    read_tlc_output,
    render,
)

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "spec"
BUNDLE = FIXTURES / "register"
CONTROL = BUNDLE / "negative-controls" / "ack-before-save.cfg"
COUNTEREXAMPLE = FIXTURES / "register-ack-before-save.out"
ELECTION = Path(__file__).resolve().parents[1] / "formal" / "cluster-election"
ID = {name: name[-1] * 40 for name in ("valkey-0", "valkey-1", "valkey-2", "valkey-3")}
JAR = os.environ.get("TLA2TOOLS_JAR")
needs_tlc = pytest.mark.skipif(
    not (JAR and os.path.exists(JAR) and shutil.which("java")), reason="needs java and TLA2TOOLS_JAR"
)


@pytest.fixture(autouse=True)
def _register_on_the_spec_path(monkeypatch):
    """The register experiment names its bundle (``spec: register``), so the fixtures are on the spec path."""

    monkeypatch.setenv("VALKEY_FUZZER_SPEC_PATH", str(FIXTURES))


def _tools():
    return TlaTools.locate(JAR, workers="1")


def _copy(tmp_path, monkeypatch=None):
    """A copy of the register bundle; with ``monkeypatch``, the copy is the one its protocol name finds."""

    target = tmp_path / "register"
    shutil.copytree(BUNDLE, target)
    if monkeypatch is not None:
        monkeypatch.setenv("VALKEY_FUZZER_SPEC_PATH", str(tmp_path))
    return target


def _edit(path, old, new):
    text = path.read_text()
    assert old in text, (path, old)
    path.write_text(text.replace(old, new))


def _cli(*argv):
    args = create_parser().parse_args(["spec", *map(str, argv)])
    return args.handler(args)


@pytest.mark.parametrize(
    ("text", "rendered"),
    [
        ("TRUE", "TRUE"),
        ('<<"a\\"b", 1, FALSE>>', '<<"a\\"b", 1, FALSE>>'),
        ("[epoch |-> 1, from |-> p1]", "[epoch |-> 1, from |-> p1]"),
        ("(p2 :> {} @@ p1 :> {[epoch |-> 1, type |-> \"vote\"]})", "(p1 :> {[epoch |-> 1, type |-> \"vote\"]} @@ p2 :> {})"),
        ("{<<r1, 1>>, <<r1, 1>>, 0}", "{0, <<r1, 1>>}"),
        ("(<<>>)", "<<>>"),
        ("{1", TlaError),
        ("1 2", TlaError),
        ("({1} :> 2)", TlaError),
    ],
)  # fmt: skip
def test_tla_values_parse_and_render_in_canonical_order(text, rendered):
    if rendered is TlaError:
        with pytest.raises(TlaError):
            parse_value(text)
    else:
        assert render(parse_value(text)) == rendered


def test_tlc_output_and_printed_values_are_read():
    result = read_tlc_output(COUNTEREXAMPLE.read_text(), 12)
    assert result.violated == "AckedDurable" and not result.no_error
    assert [state.action for state in result.trace] == ["Init", "Write", "Ack", "Crash"]
    assert changed_keys(result.trace[1].values, result.trace[2].values) == ["acked"]
    assert changed_keys(parse_value("(a :> 0 @@ b :> 1)"), parse_value("(a :> 0 @@ b :> 2)")) == [ModelValue("b")]
    # TLC wraps a long PrintT value over several lines.
    printed = '<<"spec-conform", "violated", "A", {}>>\n<< "spec-conform",\n   "reached",\n   "B",\n   { 2,\n     3 } >>\nnoise'
    assert print_values(printed, "spec-conform") == [
        ("spec-conform", "violated", "A", TlaSet()),
        ("spec-conform", "reached", "B", TlaSet([2, 3])),
    ]


@pytest.mark.parametrize(
    ("path", "old", "new", "message"),
    [
        ("negative-controls/spare.cfg", None, None, "controls no property names"),
        ("claims.yaml", "name: AckedDurable", "name: Durable", "'Durable' is not defined in model.tla"),
        ("claims.yaml", "negative_controls: [ack-before-save]", "negative_controls: []", "needs a negative control"),
        ("claims.yaml", "[ack-before-save]", "[missing]", "missing.cfg is missing"),
        ("claims.yaml", "status: draft", "status: approved", "approved claims need approved_by"),
        ("claims.yaml", "status: draft", "status: final", "status must be one of"),
        ("claims.yaml", "MaxValue: 3", "Speed: 3", "bounds must map model constants"),
        ("claims.yaml", "    conform: true", "    conform: maybe", "conform must be true or"),
        # conform evaluates a property on each observed state, which a temporal or action property is not.
        ("claims.yaml", "kind: invariant", "kind: property", "conform: true needs an invariant"),
        ("mapping.yaml", "  crashes: {init: 0}\n", "", "'crashes' is missing"),
        ("mapping.yaml", "{set: acked, value: $keys}", "{set: acked, value: $amount}", "$amount, which is not one"),
        ("mapping.yaml", "action: Crash", "action: Reboot", "'Reboot' is not defined"),
        ("mapping.yaml", "{incr: crashes}", "{incr: restarts}", "'restarts' is not a state variable"),
        ("mapping.yaml", "(?P<keys>\\d+)'", "(\\d+)'", "not captured by the log pattern"),
        ("mapping.yaml", "  MaxCrashes: {max_of: [crashes], at_least: 1}\n", "", "'MaxCrashes' is missing"),
        ("mapping.yaml", "  Save: Valkey", "  Crash: Valkey", "both observed and unobservable"),
        ("mapping.yaml", ", component: crash}", "}", "faults[0] is missing required field 'component'"),
        # Leaving target out is any trigger; same-process is the only target to name.
        ("mapping.yaml", ", component: crash}", ", component: crash, target: any}", "target must be one of"),
        (
            "mapping.yaml",
            "    inputs:\n      crash_delay_s",
            "    occurrence_shift: [1]\n    inputs:\n      crash_delay_s",
            "occurrence_shift needs an occurrence_input",
        ),
        # A fault the scenario does not inject names a model action and says why; one it injects is not unmapped.
        (
            "mapping.yaml",
            "  variants:\n    inputs:\n      crash_delay_s",
            "  unmapped: {Reboot: no fault component}\n  variants:\n    inputs:\n      crash_delay_s",
            "unmapped.Reboot must be a model action",
        ),
        (
            "mapping.yaml",
            "  variants:\n    inputs:\n      crash_delay_s",
            "  unmapped: {Crash: no fault component}\n  variants:\n    inputs:\n      crash_delay_s",
            "unmapped.Crash is also a fault",
        ),
        (
            "mapping.yaml",
            "    event: register.loaded\n",
            "    sample: true\n",
            "component names the experiment component",
        ),
        ("mapping.yaml", '{input: appendonly, equals: "yes"}', "{input: appendonly}", "input needs equals"),
        (
            "mapping.yaml",
            "    action: Crash\n",
            "    action: Crash\n    kind: fault\n",
            "crash contains unknown fields",
        ),
        (
            "mapping.yaml",
            "fields: {keys: int}\n    action: Write",
            "fields: {keys: float}\n    action: Write",
            "type must be",
        ),
        (
            "mapping.yaml",
            "    action: Ack\n",
            "    action: Ack\n    after: {evidence: wrote, match: {count: $keys}}\n",
            "wrote has no field 'count'",
        ),
        (
            "mapping.yaml",
            "    action: Ack\n",
            "    action: Ack\n    after: {evidence: wrote, match: {keys: $count}}\n",
            "match.keys must be $field",
        ),
        # A taken field comes from the record this one follows: both must declare it, and it cannot be matched on.
        (
            "mapping.yaml",
            "    action: Ack\n",
            "    action: Ack\n    after: {evidence: wrote, match: {keys: $keys}, take: [count]}\n",
            "take: 'count' must be a field of both wrote and acked",
        ),
        (
            "mapping.yaml",
            "    action: Ack\n",
            "    action: Ack\n    after: {evidence: wrote, match: {keys: $keys}, take: [keys]}\n",
            "match.keys must be $field, one of acked's own fields",
        ),
        ("mapping.yaml", "    action: Ack\n", "    action: Ack\n    logged_by: server\n", "logged_by says who logs"),
        ("model.tla", "MODULE model", "MODULE register", "must declare MODULE model"),
    ],
)
def test_a_malformed_bundle_is_rejected(tmp_path, path, old, new, message):
    bundle = _copy(tmp_path)
    if old is None:
        shutil.copy(CONTROL, bundle / path)
    else:
        _edit(bundle / path, old, new)
    with pytest.raises(SpecBundleError, match=message.replace("$", r"\$").replace("[", r"\[").replace("(", r"\(")):
        load_bundle(str(bundle))


MUTANT = (BUNDLE / "model.tla").read_text().replace("MODULE model", "MODULE ack-before-save")


@pytest.mark.parametrize(
    ("module", "error"),
    [
        # The mutation the control is for: acknowledge without the save.
        (MUTANT.replace("/\\ SyncBeforeAck => disk = mem\n", ""), None),
        ("---- MODULE ack-before-save ----\nEXTENDS model\nBad == Init /\\ [][Next]_vars\n====\n", None),
        # A control that redefines the property it must violate proves nothing about the model's own.
        (MUTANT.replace("AckedDurable == acked <= mem", "AckedDurable == FALSE"), "AckedDurable differs"),
        (MUTANT.replace("AckedDurable == acked <= mem", "Durable == acked <= mem"), "defines no AckedDurable"),
    ],
)
def test_a_mutated_control_must_check_the_models_own_property(tmp_path, module, error):
    bundle = _copy(tmp_path)
    (bundle / "negative-controls" / "ack-before-save.tla").write_text(module)
    if error is None:
        assert load_bundle(str(bundle)).controls[0].module == "negative-controls/ack-before-save.tla"
    else:
        with pytest.raises(SpecBundleError, match=error):
            load_bundle(str(bundle))


def test_a_mapping_broken_after_it_was_read_is_refused_not_served_from_the_cache(tmp_path):
    bundle = _copy(tmp_path)
    first = load_spec_mapping(str(bundle))
    assert load_spec_mapping(str(bundle)) is first
    (bundle / "mapping.yaml").write_text("evidence: [", encoding="utf-8")
    with pytest.raises(SpecBundleError):
        load_spec_mapping(str(bundle))
    (bundle / "mapping.yaml").unlink()
    with pytest.raises(SpecBundleError, match=r"cannot read .*mapping\.yaml"):
        load_spec_mapping(str(bundle))


# ---------------------------------------------------------------------------
# translate
# ---------------------------------------------------------------------------


def _translate(bundle=BUNDLE, **kwargs):
    trace = kwargs.pop("trace", read_tlc_output(COUNTEREXAMPLE.read_text()).trace)
    compile_scenario = bundle_service(load_bundle(str(bundle))).compile
    options = {"name": "ack-before-save", "violated": "AckedDurable", "compile_scenario": compile_scenario, **kwargs}
    return translate_trace(load_bundle(str(bundle)), trace, CONTROL.read_text(), **options)


def test_translate_turns_the_counterexample_into_scenarios_of_the_bundles_experiment(tmp_path):
    translation = _translate(variants=4, config="negative-controls/ack-before-save.cfg")
    (alignment,) = translation["alignment"]
    assert (alignment["fault"], alignment["fault_state"], alignment["trigger"]) == ("Crash", 4, "acked")
    first, nearby = translation["scenarios"]
    assert first["experiment"] == {"id": "toy.register", "inputs": {"appendonly": "no"}}
    assert nearby["experiment"]["inputs"] == {"appendonly": "no", "crash_delay_s": 0.01}
    # Reproducible: the translation names the bundle and its files, never a path on this host.
    assert translation["bundle"] == "register" and "protocol" not in translation
    assert str(BUNDLE) not in json.dumps(translation)
    out = tmp_path / "scenarios"
    assert (
        _cli("translate", "register", "--trace", COUNTEREXAMPLE, "--config", CONTROL, "--out", out, "--variants", 1)
        == 0
    )
    names = ["spec-register-ack-before-save-v1.yaml", "spec-register-ack-before-save.yaml", "translation.json"]
    assert sorted(path.name for path in out.iterdir()) == names
    assert all(str(BUNDLE) not in (out / name).read_text() for name in names)
    # Nothing to translate is a harness error (never exit 1, a FAIL).
    empty = tmp_path / "empty.out"
    empty.write_text("")
    assert _cli("translate", "register", "--trace", empty, "--config", CONTROL, "--out", tmp_path / "none") == 2


def test_translate_aligns_the_election_counterexample_on_the_vote_that_left_before_its_save():
    result = read_tlc_output((FIXTURES / "cluster-election-vote-before-save.out").read_text(), 12)
    translation = translate_trace(
        load_bundle("cluster-election"),
        result.trace,
        (ELECTION / "negative-controls" / "vote-before-save.cfg").read_text(),
        name="vote-before-save",
        violated=result.violated,
        compile_scenario=valkey_service().compile,
    )
    # p1 grants the first vote, its queued save has not landed when the vote leaves, and p1 crashes.
    (alignment,) = translation["alignment"]
    assert (alignment["process"], alignment["occurrence"], alignment["between"]) == (
        "p1",
        1,
        ["SaveConfig", "ReceiveVote"],
    )
    inputs = translation["scenarios"][0]["experiment"]["inputs"]
    assert inputs == {"save_behavior": "best-effort", "election": "force_failover", "vote_occurrence": 1}


def _shipped(protocol, control, variants=4):
    """The checked-in TLC counterexample of a shipped bundle's control, translated against the Valkey experiments."""

    result = read_tlc_output((FIXTURES / f"{protocol}-{control}.out").read_text(), 12)
    bundle = load_bundle(protocol)
    return translate_trace(
        bundle,
        result.trace,
        bundle.files[bundle.control(control).config],
        name=control,
        violated=result.violated,
        variants=variants,
        compile_scenario=valkey_service().compile,
    )


def test_translate_kills_the_slot_migration_target_at_its_claim():
    translation = _shipped("slot-migration", "replica_misses_claim")
    # T crashes after it claimed the slot; the write to T and R applying T's stream must fit in kill_delay_s.
    (alignment,) = translation["alignment"]
    assert (alignment["fault"], alignment["component"], alignment["trigger"]) == ("CrashT", "kill-target", "claimed")
    assert alignment["between"] == ["Write", "RApply", "RApply", "RApply", "RApply"]
    direct, *nearby = translation["scenarios"]
    assert direct["experiment"] == {"id": "valkey.slot-migration-spec", "inputs": {}}
    assert direct["deployment"] == {"mode": "cluster", "primaries": 3, "replicas_per_primary": 1, "base_port": 7760}
    assert [item["experiment"]["inputs"] for item in nearby] == [{"kill_delay_s": 0.2}, {"kill_delay_s": 1}]
    # The faults the scenario leaves to the experiment, each with its reason.
    assert set(translation["unmapped"]) == {"Cancel", "DropLink", "PauseExpires", "LoseBus"}


@pytest.mark.parametrize(
    ("protocol", "control", "experiment", "deployment", "inputs", "unmapped"),
    [
        # No persistence counterexample takes Crash: a kill would lose data after the trace's last step, so the
        # scenario names the crash-point experiment and sweeps the rewrite's calls.
        (
            "persistence",
            "manifest_before_base",
            "valkey.save-crash-points",
            {"mode": "standalone", "syscall_faults": True, "base_port": 7720},
            {"trigger_command": ["BGREWRITEAOF"], "nth": 9},
            {"Crash", "RewriteFails"},
        ),
        # No replication counterexample takes Crash either; its one fault, the link drop, is the experiment's own.
        (
            "replication",
            "keep_old_id_on_load",
            "valkey.replication-full-resync",
            {"mode": "standalone", "replicas_per_primary": 2, "base_port": 7730},
            {},
            {"Crash", "Disconnect", "LoadFails"},
        ),
    ],
)
def test_translate_writes_a_scenario_without_a_fault_for_a_counterexample_that_takes_none(
    protocol, control, experiment, deployment, inputs, unmapped
):
    translation = _shipped(protocol, control)
    assert translation["alignment"] == [] and set(translation["unmapped"]) == unmapped
    direct = translation["scenarios"][0]
    assert direct["experiment"] == {"id": experiment, "inputs": inputs} and direct["deployment"] == deployment
    # The variants sweep what the trace does not say (the crash point, the stall), and each compiles.
    assert len(translation["scenarios"]) > 1
    assert all(item["experiment"]["inputs"] != inputs for item in translation["scenarios"][1:])


def test_translate_refuses_what_the_experiment_cannot_run(tmp_path, monkeypatch):
    trace = read_tlc_output(COUNTEREXAMPLE.read_text()).trace
    with pytest.raises(TranslateError, match="none of the mapped fault actions"):
        _translate(trace=trace[:3])
    with pytest.raises(TranslateError, match="do not compile"):
        _translate(compile_scenario=lambda scenario: int("appendonly is out of range"))
    # Without an occurrence input the fault fires on the first trigger, so a later one cannot be expressed.
    steps = [("Init", 0, 0, 0), ("Write", 1, 0, 0), ("Ack", 1, 1, 0), ("Write", 2, 1, 0), ("Ack", 2, 2, 0),
             ("Crash", 0, 2, 1)]  # fmt: skip
    later = [
        TraceState(index + 1, action, {"mem": mem, "acked": acked, "disk": 0, "crashes": crashes})
        for index, (action, mem, acked, crashes) in enumerate(steps)
    ]
    with pytest.raises(TranslateError, match="follows acked #2, but the mapping sets no occurrence_input"):
        _translate(trace=later)
    # A mapped fault the counterexample does not take would still fire in the scenario, which then
    # does not follow the counterexample.
    bundle = _copy(tmp_path / "unused", monkeypatch)
    _edit(
        bundle / "mapping.yaml",
        "component: crash}\n",
        "component: crash}\n    - {action: Save, after: acked, component: crash}\n",
    )
    with pytest.raises(TranslateError, match="has no Save step, but the experiment's fault component crash would"):
        _translate(bundle)
    # Known-bad: the mapping names a fault component the experiment does not have, or a component that is no fault.
    bundle = _copy(tmp_path, monkeypatch)
    for old, component in (("crash", "kill"), ("kill", "writes")):
        _edit(bundle / "mapping.yaml", f", component: {old}}}", f", component: {component}}}")
        with pytest.raises(TranslateError, match=rf"toy\.register has no fault component {component}"):
            _translate(bundle)


# ---------------------------------------------------------------------------
# conform
# ---------------------------------------------------------------------------


def _run(tmp_path, name, *, appendonly="yes", loaded=1, crash_source="crash", keys=1):
    """A register run as its experiment records it: ``keys`` writes acknowledged at once, a crash, and the reload's
    key count."""

    events = [
        ("writes.seeded", "writes", {"keys": keys}),
        ("crash.injected", crash_source, {"signal": "SIGKILL", "applied": True, "target": "valkey-0"}),
        ("crash.recovered", "crash", {}),
        ("register.loaded", "reload-log", {"keys": loaded, "instance_id": "valkey-0"}),
    ]
    records = [{"kind": "header", "scenario_id": name, "seed": 1, "inputs": {"appendonly": appendonly}, "nodes": {}}]
    records += [
        {"kind": "event", "t": float(t), "name": e, "source": s, "payload": p} for t, (e, s, p) in enumerate(events, 1)
    ]
    path = tmp_path / f"{name}.jsonl"
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n")
    return str(path)


def test_conform_rebuilds_the_observed_states_from_the_mapped_component_only(tmp_path):
    bundle = load_bundle(str(BUNDLE))
    observed = ObservedRun(bundle.mapping, collect_runs([_run(tmp_path, "lost", loaded=0)])[0])
    assert observed.replay()[-2:] == [
        {"mem": 1, "disk": 0, "acked": 1, "crashes": 1},
        {"mem": 0, "disk": 0, "acked": 1, "crashes": 1},
    ]
    assert observed.constants() == {"SyncBeforeAck": "TRUE", "MaxValue": "1", "MaxCrashes": "1"}
    # Known-bad: another component's SIGKILL is not this experiment's crash.
    other = ObservedRun(bundle.mapping, collect_runs([_run(tmp_path, "other", crash_source="another-fault")])[0])
    other.replay()
    assert "Crash" not in other.actions


def test_conform_never_passes_a_run_without_evidence_or_its_constants(tmp_path):
    path = tmp_path / "empty.jsonl"
    path.write_text(json.dumps({"kind": "header", "nodes": {}}) + "\n")
    bundle = load_bundle(str(BUNDLE))
    # No TLC is needed to refuse them.
    result = check_run(bundle, collect_runs([str(path)])[0], TlaTools(jar="unused"))
    assert result["status"] == "error" and "no mapped evidence" in result["error"]
    report = conform_runs(bundle, collect_runs([str(path)]), TlaTools(jar="unused"))
    assert (report["status"], report["bundle"]) == ("error", "register") and "protocol" not in report
    (run,) = collect_runs([_run(tmp_path, "unknown")])
    run.meta["inputs"] = {}
    result = check_run(bundle, run, TlaTools(jar="unused"))
    assert result["status"] == "error" and "needs input 'appendonly'" in result["error"]


def test_conform_errors_on_a_run_with_a_mapped_record_it_could_not_apply(tmp_path):
    # The reload lost the write, but its key count is not an int: without that record every observed state
    # satisfies AckedDurable, so a skipped record must not leave a pass.
    bundle = load_bundle(str(BUNDLE))
    (run,) = collect_runs([_run(tmp_path, "lost", loaded="0 (truncated AOF)")])
    result = check_run(bundle, run, TlaTools(jar="unused"))
    assert result["status"] == "error"
    assert "could not be applied" in result["error"] and "keys = '0 (truncated AOF)' is not int" in result["error"]


@pytest.mark.parametrize(
    ("value", "typed"),
    [(True, True), (False, False), ("false", False), ("TRUE", True), ("no", None), (1, None)],
)
def test_conform_reads_a_bool_field_as_written_not_by_truthiness(tmp_path, value, typed):
    bundle = _copy(tmp_path)
    _edit(bundle / "mapping.yaml", "fields: {keys: int}\n    action: Ack", "fields: {keys: bool}\n    action: Ack")
    observed = ObservedRun(load_bundle(str(bundle)).mapping, collect_runs([_run(tmp_path, "run")])[0])
    evidence = observed.mapping.evidence["acked"]
    record = {"kind": "event", "name": "writes.seeded", "source": "writes", "payload": {"keys": value}}
    if typed is None:
        with pytest.raises(Exception, match="is not bool"):
            observed._fields(evidence, record)
    else:
        assert observed._fields(evidence, record) == {"keys": typed}


def test_conform_reads_every_run_folder_and_reports_the_unreadable_ones(tmp_path):
    def trace(folder):
        (folder / "components" / "trace").mkdir(parents=True)
        shutil.copy(_run(tmp_path, folder.name), folder / "components" / "trace" / "trace.jsonl")
        (folder / "plan.json").write_text("{}")

    runs = tmp_path / "batch" / "runs"
    for name in ("good", "corrupt", "hung", "empty", "truncated"):
        trace(runs / name)
    (runs / "good" / "report.json").write_text(json.dumps({"status": "PASS", "outcome": {"plan": {}}}))
    (runs / "corrupt" / "report.json").write_text('{"status": "FAIL", "outcome": ')
    # hung: killed at its deadline, it left only plan.json and its trace.
    shutil.rmtree(runs / "empty" / "components")
    with open(runs / "truncated" / "components" / "trace" / "trace.jsonl", "a") as handle:
        handle.write('{"kind": "event", "t": ')

    collected = {Path(run.source).relative_to(runs).parts[0]: run for run in collect_runs([str(tmp_path / "batch")])}
    assert sorted(collected) == ["corrupt", "empty", "good", "hung", "truncated"]
    assert (collected["good"].problem, collected["hung"].problem) == (None, None)
    assert collected["hung"].meta["scenario_id"] == "hung"
    assert "report.json is not a readable run report" in collected["corrupt"].problem
    assert "no report.json and no trace" in collected["empty"].problem
    assert "trace.jsonl" in collected["truncated"].problem
    result = check_run(load_bundle(str(BUNDLE)), collected["corrupt"], TlaTools(jar="unused"))
    assert (result["status"], result["error"], result["states"]) == ("error", collected["corrupt"].problem, 0)


def _run_folder(folder, status, specs, events=()):
    """A run folder whose report has ``status``, one component per name in ``specs``, and ``events`` committed."""

    components = [{"use": "valkey.observer.log-trigger", "id": f"c{index}", "config": {"spec": name}}
                  for index, name in enumerate(specs)] + [{"use": "valkey.oracle.liveness", "id": "liveness", "config": {}}]  # fmt: skip
    diagnostics = {"events": [{"committed": True, "sequence": index, "timestamp": float(index), "name": name,
                               "source": "c0", "payload": payload} for index, (name, payload) in enumerate(events)]}  # fmt: skip
    plan = {"scenario_id": folder.name, "seed": 1, "experiment_id": "x", "inputs": {}, "components": components}
    folder.mkdir(parents=True)
    report = {"status": status, "outcome": {"plan": plan, "diagnostics": diagnostics}}
    (folder / "report.json").write_text(json.dumps(report))
    return str(folder)


PERSISTENCE_EVENTS = [
    ("kill.injected", {"signal": "SIGKILL", "applied": True, "target": "valkey-0"}),
    ("persistence.save.done", {"instance_id": "valkey-0"}),
]


def test_conform_folder_checks_a_run_against_each_bundle_its_plan_names(tmp_path):
    run = _run_folder(tmp_path / "run", "FAIL", ["persistence", "register", "persistence"], PERSISTENCE_EVENTS)
    checked = conform_folder(run, None, "tla2tools.jar not found")
    assert list(checked) == ["persistence", "register"]
    # Every persistence property is excluded and it has no target: TLC has nothing to evaluate, so no tools.
    persistence = checked["persistence"]
    assert (persistence["status"], persistence["approved"]) == ("pass", False)
    assert persistence["actions"] == {"Crash": 1, "SaveDone": 1} and persistence["properties"] == {}
    assert checked["register"] == {"approved": False, "status": "skipped", "reason": "tla2tools.jar not found"}
    # Known-bad: a run that never reached its verdict, a bundle that is gone, and a run of no bundle.
    errored = conform_folder(_run_folder(tmp_path / "errored", "ERROR", ["persistence"], PERSISTENCE_EVENTS), None)
    assert errored["persistence"]["status"] == "not_run" and "ended ERROR" in errored["persistence"]["reason"]
    gone = conform_folder(_run_folder(tmp_path / "gone", "PASS", ["no-such-bundle"]), None)
    assert gone["no-such-bundle"]["status"] == "error" and "no-such-bundle" in gone["no-such-bundle"]["error"]
    # A bundle that does not load is held against the run: nothing says its claims are a draft.
    assert gone["no-such-bundle"]["approved"] is True
    assert conform_folder(_run_folder(tmp_path / "none", "PASS", []), None) == {}
    # A run that took no mapped step is unexercised: never a pass, and no error of the check either.
    empty = conform_folder(_run_folder(tmp_path / "empty", "PASS", ["persistence"]), None)
    assert empty["persistence"]["status"] == "unexercised" and "no mapped evidence" in empty["persistence"]["reason"]
    assert "error" not in empty["persistence"]


def test_conform_folder_without_tlc_is_an_error_for_an_approved_bundle_and_a_skip_for_a_draft(tmp_path, monkeypatch):
    run = _run_folder(tmp_path / "run", "PASS", ["register"])
    draft = {"approved": False, "status": "skipped", "reason": "no java"}
    assert conform_folder(run, None, "no java") == {"register": draft}
    # An approved bundle's claims decide the run, so a check that could not run must not let it pass.
    _edit(_copy(tmp_path, monkeypatch) / "claims.yaml", "status: draft", "status: approved\napproved_by: someone")
    approved = {"approved": True, "status": "error", "error": "no java"}
    assert conform_folder(run, None, "no java") == {"register": approved}


def test_the_tools_name_what_is_missing_and_how_to_get_it(monkeypatch, tmp_path):
    monkeypatch.delenv("TLA2TOOLS_JAR", raising=False)
    monkeypatch.setattr(tla.shutil, "which", lambda name: None)
    with pytest.raises(TlaError) as error:
        TlaTools.locate(None)
    assert str(error.value) == f"tla2tools.jar not found and java not found on PATH: {tla.TOOLS_HINT}"
    assert "Java 11 or later" in tla.TOOLS_HINT and f"curl -fsSL -o tla2tools.jar {tla.TLA_TOOLS_URL}" in tla.TOOLS_HINT
    (tmp_path / "tla2tools.jar").write_text("")
    with pytest.raises(TlaError, match=r"^java not found on PATH: the spec bridge needs Java 11"):
        TlaTools.locate(str(tmp_path / "tla2tools.jar"))


def test_conform_needs_no_tools_for_a_bundle_tlc_has_nothing_to_evaluate(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("TLA2TOOLS_JAR", raising=False)
    run = _run_folder(tmp_path / "run", "PASS", ["persistence"], PERSISTENCE_EVENTS)
    assert _cli("conform", "persistence", run) == 0 and "Crash 1/1" in capsys.readouterr().out
    assert _cli("conform", str(BUNDLE), run) == 2 and "tla2tools.jar not found" in capsys.readouterr().err


ELECTION_OBSERVATIONS = [
    {"kind": "observation", "t": 0.1, "round": 1, "node": node, "role": role, "slots": slots, "current_epoch": 8,
     "disk": {"currentEpoch": 8, "lastVoteEpoch": 0}}
    for node, role, slots in (("valkey-0", "primary", [[0, 8191]]), ("valkey-3", "primary", [[8192, 16383]]), ("valkey-1", "replica", []), ("valkey-2", "replica", []))
]  # fmt: skip
ELECTION_EVENTS = [
    ("cluster.election.started", {"instance_id": "valkey-1", "epoch": 9}),
    ("cluster.election.started", {"instance_id": "valkey-2", "epoch": 9}),
    ("cluster.vote.granted", {"instance_id": "valkey-0", "candidate": ID["valkey-1"], "epoch": 9}),
    ("cluster.vote.granted", {"instance_id": "valkey-3", "candidate": ID["valkey-1"], "epoch": 9}),
    ("cluster.vote.acked", {"instance_id": "valkey-1", "voter": ID["valkey-0"], "epoch": 9}),
    ("fault.injected", {"applied": True, "signal": "SIGKILL", "target": "valkey-3", "disk_vars_at_kill": {"currentEpoch": 8, "lastVoteEpoch": 0}}),
    ("cluster.vote.acked", {"instance_id": "valkey-1", "voter": ID["valkey-3"], "epoch": 9}),
    ("cluster.election.won", {"instance_id": "valkey-1"}),
    ("node.recovered", {"instance_id": "valkey-3"}),
]  # fmt: skip


def _election_run(tmp_path, events, times=None):
    """An election run's trace under best-effort: the observations, then ``events`` at ``times`` (or 0.1 s apart)."""

    nodes = {name: {"node_id": ID[name], "config": {"cluster-config-save-behavior": "best-effort"}} for name in ID}
    records = [{"kind": "header", "scenario_id": "unit", "seed": 1, "nodes": nodes}, *ELECTION_OBSERVATIONS]
    for index, (name, payload) in enumerate(events):
        t = times[index] if times else 1 + index / 10
        records.append({"kind": "event", "t": t, "sequence": 10 + index, "name": name, "payload": payload})
    path = tmp_path / "trace.jsonl"
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n")
    observed = ObservedRun(load_bundle("cluster-election").mapping, collect_runs([str(path)])[0])
    observed.replay()
    return observed


def test_conform_maps_an_election_run_onto_the_model(tmp_path):
    # Two replicas compete for epoch 9; valkey-3 votes for valkey-1 and is killed before its vote is saved.
    observed = _election_run(tmp_path, ELECTION_EVENTS)

    assert observed.members == {"Primaries": ["valkey-0", "valkey-3"], "Candidates": ["valkey-1", "valkey-2"]}
    final = observed.states[-1]
    # An ACK of the voter's vote shows the vote left.
    assert {"type": "vote", "from": "prim_valkey_3", "to": "cand_valkey_1", "epoch": 9} in final["msgs"]
    assert render(final["winners"]) == "{<<cand_valkey_1, 9>>}"
    assert sorted(final["granted"]["cand_valkey_1"]) == ["prim_valkey_0", "prim_valkey_3"]
    assert dict(observed.actions) == {"StartElection": 2, "HandleRequest": 2, "ReceiveVote": 2, "Crash": 1, "Win": 1}
    assert observed.constants()["PersistBeforeAck"] == "FALSE"


def test_conform_never_makes_up_the_vars_a_crash_restarts_with(tmp_path):
    # A kill whose nodes.conf vars were not captured says nothing about what the voter restarts with; reading
    # it as nodes.conf holding 0 would reset lastVote and invent a vote lost in the crash.
    events = [
        (name, {key: value for key, value in payload.items() if key != "disk_vars_at_kill"})
        for name, payload in ELECTION_EVENTS
    ]
    observed = _election_run(tmp_path, events)
    assert any("no disk_vars_at_kill.currentEpoch" in item for item in observed.unapplied)
    assert "Crash" not in observed.actions


def _lost_votes(state):
    """The votes in ``state`` that left their voter while its lastVote is below their epoch (VoteLostInCrash)."""

    return [m for m in state["msgs"] if m["type"] == "vote" and state["lastVote"][m["from"]] < m["epoch"]]


def test_conform_applies_a_vote_ack_only_after_the_grant_it_acknowledges(tmp_path):
    # The candidate's ACK and the voter's grant carry the same millisecond, and log-trigger read the candidate's
    # log first: in trace order the ACK comes first. The vote left only after it was granted, so no observed
    # state may show it sent while the voter has not voted (that reads as a vote lost in a crash).
    granted, acked = ELECTION_EVENTS[2], ELECTION_EVENTS[4]
    observed = _election_run(tmp_path, [ELECTION_EVENTS[0], acked, granted], times=[1.0, 1.5, 1.5])
    assert observed.unapplied == []
    assert dict(observed.actions) == {"StartElection": 1, "HandleRequest": 1, "ReceiveVote": 1}
    assert [_lost_votes(state) for state in observed.states] == [[]] * len(observed.states)
    assert render(observed.states[-1]["granted"]["cand_valkey_1"]) == "{prim_valkey_0}"
    # Known-bad: an ACK whose grant was never seen leaves states that miss the vote, so the run is an error.
    observed = _election_run(tmp_path, [ELECTION_EVENTS[0], acked])
    assert "acked: no granted" in " ".join(observed.unapplied) and "ReceiveVote" not in observed.actions


def test_conform_takes_a_wins_epoch_from_the_election_its_winner_started(tmp_path):
    # "Failover election won" names no epoch, and the line that does ("configEpoch set to N after successful
    # failover") is logged only when the winner's configEpoch was below N: the won record takes the epoch of the
    # winner's latest started record (after: ... take: [epoch]).
    won = ("cluster.election.won", {"instance_id": "valkey-1"})
    retried = ("cluster.election.started", {"instance_id": "valkey-1", "epoch": 10})
    observed = _election_run(tmp_path, [*ELECTION_EVENTS[:7], retried, won])
    assert observed.unapplied == [] and observed.actions["Win"] == 1
    assert render(observed.states[-1]["winners"]) == "{<<cand_valkey_1, 10>>}"
    assert observed.steps[-1]["fields"] == {"instance_id": "valkey-1", "epoch": 10}
    # A win line that no started line of the winner precedes has no epoch: a record conform cannot apply, never a
    # win at a made-up epoch. The grants and ACKs of an election that never started are records it cannot apply too.
    observed = _election_run(tmp_path, [*ELECTION_EVENTS[2:7], won])
    assert "won: no started record (instance_id valkey-1) before or after it" in observed.unapplied
    assert "granted: no started record (instance_id valkey-1, epoch 9) before or after it" in observed.unapplied
    assert "Win" not in observed.actions
    # No won line, no Win: the model's state is what the run logged, and a missing Win is no violation.
    observed = _election_run(tmp_path, ELECTION_EVENTS[:7])
    assert "Win" not in observed.actions and render(observed.states[-1]["winners"]) == "{}"
    assert observed.unapplied == []


def test_conform_reports_a_trace_that_is_missing_or_unreadable_instead_of_checking_without_it(tmp_path):
    tools = TlaTools(jar="unused")  # check_run returns before TLC: the run has a problem
    run = _run_folder(tmp_path / "gone", "PASS", ["cluster-election"], ELECTION_EVENTS)
    report = json.loads(Path(run, "report.json").read_text())
    trace = Path(run, "components", "trace", "trace.jsonl")
    report["outcome"]["evidence"] = [{"evidence_id": "valkey.trace.recorded", "artifacts": [str(trace)]}]
    Path(run, "report.json").write_text(json.dumps(report))
    # The run recorded a trace (its samples carry what the events do not) that is not there: not the events alone.
    missing = conform_folder(run, tools)["cluster-election"]
    assert missing["status"] == "error" and missing["error"].endswith(
        f"is missing (a pruned run keeps its conform.json): {trace}"
    )
    # A trace that is there but is not one: the same, with what is wrong, and the batch's worker does not crash.
    trace.parent.mkdir(parents=True)
    trace.write_text('{"kind": "event"}\n')
    broken = conform_folder(run, tools)["cluster-election"]
    assert broken["status"] == "error" and broken["error"] == f"{trace}: {trace} does not start with a trace header"
    assert (missing["approved"], broken["approved"]) == (False, False)


@needs_tlc
@pytest.mark.slow
def test_conform_fails_a_lost_acknowledged_write_only_inside_the_claim(tmp_path):
    names = [
        _run(tmp_path, "saved"),
        _run(tmp_path, "lost", loaded=0),
        _run(tmp_path, "unsynced", appendonly="no", loaded=0),
    ]
    report = conform_runs(load_bundle(str(BUNDLE)), collect_runs(names), _tools(), group_by=["appendonly"])
    saved, lost, unsynced = report["runs"]
    assert saved["status"] == "pass" and saved["properties"]["AckedDurable"] == {"status": "holds", "claimed": True}
    # Known-bad: an acknowledged write lost by a crash, in a run that claims appendfsync always.
    assert lost["status"] == "fail" and lost["properties"]["AckedDurable"]["states"] == [5]
    # The same run without appendonly is outside the claim: reported, not a failure.
    assert unsynced["status"] == "pass" and unsynced["properties"]["AckedDurable"]["claimed"] is False
    assert report["status"] == "fail"
    output = tmp_path / "conform.json"
    base = ("conform", BUNDLE, "--tla2tools", JAR, "--workers", 1, "--output", output)
    assert _cli(*base, names[0]) == 0 and _cli(*base, names[1]) == 1
    # Every run's observed steps are steps of the model: the lost write is a Crash the model allows.
    assert all(run["transitions"]["holds"] for run in report["runs"])
    assert report["transitions"] == {"max_unobserved_steps": 1, "violated_in": 0}


# ---------------------------------------------------------------------------
# conform: transitions
# ---------------------------------------------------------------------------


def _election_trace(tmp_path, events, behavior="best-effort", name="unit"):
    """An election run's trace file: the observations, then ``events`` 0.1 s apart."""

    nodes = {n: {"node_id": ID[n], "config": {"cluster-config-save-behavior": behavior}} for n in ID}
    records = [{"kind": "header", "scenario_id": name, "seed": 1, "nodes": nodes}, *ELECTION_OBSERVATIONS]
    for index, (event, payload) in enumerate(events):
        records.append(
            {"kind": "event", "t": 1 + index / 10, "sequence": 10 + index, "name": event, "payload": payload}
        )
    path = tmp_path / f"{name}.jsonl"
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n")
    return str(path)


# The election without the kill: every vote is saved, both ACKs arrive, the candidate wins.
ELECTION_CORRECT = [event for event in ELECTION_EVENTS if event[0] not in ("fault.injected", "node.recovered")]
DENIED = ("cluster.vote.denied", {"instance_id": "valkey-0", "candidate": ID["valkey-2"]})


def test_claims_take_the_transition_checks_knobs(tmp_path):
    bundle = _copy(tmp_path)
    claims = load_bundle(str(bundle)).claims
    assert (claims.max_unobserved_steps, claims.transitions_excluded) == (1, None)
    _edit(bundle / "claims.yaml", "status: draft\n", "status: draft\nmax_unobserved_steps: 0\n")
    assert load_bundle(str(bundle)).claims.max_unobserved_steps == 0
    _edit(bundle / "claims.yaml", "max_unobserved_steps: 0\n", "transitions: {excluded: no roles in the lines}\n")
    assert load_bundle(str(bundle)).claims.transitions_excluded == "no roles in the lines"
    for index, (field, message) in enumerate(
        (
            ("max_unobserved_steps: -1", "0 or more"),
            ("max_unobserved_steps: true", "must be an integer"),
            ("transitions: {excluded: ' '}", "must say why"),
            ("transitions: {reason: x}", "transitions"),
        )
    ):
        broken = _copy(tmp_path / f"broken{index}")
        _edit(broken / "claims.yaml", "status: draft\n", f"status: draft\n{field}\n")
        with pytest.raises(SpecBundleError, match=message):
            load_bundle(str(broken))


def test_a_record_reports_what_its_updates_say_about_the_state(tmp_path):
    observed = _election_run(tmp_path, [*ELECTION_EVENTS[:5], DENIED])
    by_why = {}
    for report in observed.reports:
        by_why.setdefault(report["why"], report)
    p0, c1 = ModelValue("prim_valkey_0"), ModelValue("cand_valkey_1")
    vote = {"type": "vote", "from": p0, "to": c1, "epoch": 9}
    assert by_why["initial state"]["kind"] == "init" and observed.reports[1]["kind"] == "sample"
    # A sample reports lower bounds (nodes.conf samples lag), which raise the model's values.
    assert observed.reports[1]["lo"] == [("currentEpoch", p0), ("diskEpoch", p0), ("diskVote", p0)]
    assert observed.reports[1]["eq"] == [] and observed.reports[1]["state"] == 2
    # A grant: lastVote is set, the vote is in held, the mapping's step rule says lastVote changes; its max on
    # currentEpoch is a lower bound a step cannot check.
    granted = by_why["HandleRequest"]
    assert (granted["kind"], granted["action"], granted["evidence"]) == ("step", "HandleRequest", "granted")
    assert granted["eq"] == [("lastVote", p0)] and granted["has"] == [(("held", p0), vote)] and granted["lo"] == []
    assert (granted["changes"], granted["unchanged"]) == (["lastVote"], [])
    acked = by_why["ReceiveVote"]
    assert acked["lacks"] == [(("held", p0), vote)]
    assert acked["has"] == [(("msgs",), vote), (("granted", c1), p0)] and acked["eq"] == []
    # A denial changes nothing observed, so it is no state, but it is a step the model must allow (or not take).
    denied = observed.reports[-1]
    assert denied["evidence"] == "denied" and denied["state"] == len(observed.states)
    assert denied["unchanged"] == ["lastVote"] and denied["eq"] == denied["has"] == []
    assert len(observed.reports) == len(observed.states) + 1


def test_split_next_keeps_the_named_actions_of_the_models_next():
    model = load_bundle("cluster-election").model_text
    assert conform.split_next(model, {"Win", "Crash"}, {"StartElection", "LearnEpoch", "ReceiveVote", "HandleRequest", "SaveConfig", "BioWrite", "FlushEarly"}) == (
        "\n    \\/ \\E c \\in Candidates :\n          M!Win(c)\n    \\/ \\E p \\in Primaries :\n          M!Crash(p)"
    )  # fmt: skip
    register = load_bundle(str(BUNDLE)).model_text
    assert conform.split_next(register, {"Save"}, {"Write", "Ack", "Crash"}) == "\n M!Save"
    assert conform.split_next(register, set(), {"Write", "Ack", "Save", "Crash"}) == " FALSE"
    # A defined set in a quantifier comes from the instance too; an operator that takes steps itself does not.
    migration = load_bundle("slot-migration")
    others = (set(migration.mapping.actions()) | set(migration.mapping.unobservable)) - {"Deliver"}
    assert "\\E n \\in M!Nodes : M!Deliver(n)" in conform.split_next(migration.model_text, {"Deliver"}, others)
    grouped = register.replace(
        "Next == Write \\/ Ack \\/ Save \\/ Crash", "Faults == Crash\nNext == Write \\/ Ack \\/ Save \\/ Faults"
    )
    with pytest.raises(TlaError, match="takes steps through Faults"):
        conform.split_next(grouped, {"Write"}, {"Ack", "Save", "Crash"})


def test_the_trace_module_checks_the_transitions_with_the_models_own_actions(tmp_path):
    bundle = load_bundle("cluster-election")
    observed = _election_run(tmp_path, [*ELECTION_CORRECT, DENIED])
    module, cfg = conform.trace_module(bundle, observed, observed.constants(), ["OneWinnerPerEpoch"], [], 2)
    lines = module.splitlines()
    assert lines[4].startswith("VARIABLES i, k, currentEpoch, lastVote, diskEpoch")
    # The hidden variable starts at the mapping's value and follows the model from there.
    assert "        /\\ bio = Trace[1].bio" in lines and "bio |-> [p \\in Primaries |-> <<>>]" in module
    assert "MaxUnobserved == 2" in lines
    # Each record's action, alone, from the model's Next; the stutter rule; and the unobservable steps.
    assert "Do_Win ==\n    \\/ \\E c \\in Candidates :\n          M!Win(c)" in module
    assert "        /\\ Action(Steps[i'].action) \\/ UNCHANGED Vars" in lines
    assert "Unobservable ==\n    \\/ \\E c \\in Candidates :\n          M!LearnEpoch(c)" in module
    assert "Unobserved == k < MaxUnobserved /\\ k' = k + 1 /\\ UNCHANGED i /\\ Unobservable" in lines
    assert (
        "Repeated == i < Len(Steps) /\\ Steps[i + 1].repeats /\\ UNCHANGED <<i, k>> /\\ Action(Steps[i + 1].action)"
        in lines
    )
    # What the records say, as paths; the denial's step rule.
    assert (
        '[kind |-> "step", at |-> 8, action |-> "HandleRequest", eq |-> {<<"lastVote", prim_valkey_0>>}, lo |-> {}, '
        'has |-> {<<<<"held", prim_valkey_0>>, [epoch |-> 9, from |-> prim_valkey_0, to |-> cand_valkey_1, type |-> "vote"]>>}, '
        'lacks |-> {}, changes |-> {"lastVote"}, unchanged |-> {}, repeats |-> FALSE]'
    ) in module
    assert (
        '[kind |-> "step", at |-> 12, action |-> "HandleRequest", eq |-> {}, lo |-> {}, has |-> {}, lacks |-> {}, '
        'changes |-> {}, unchanged |-> {"lastVote"}, repeats |-> FALSE]'
    ) in module
    # The old check is still there, and the config checks the trace is explained to its end.
    assert (
        'ASSUME PrintT(<<"spec-conform", "violated", "OneWinnerPerEpoch", {j \\in DOMAIN Trace : ~S(j)!OneWinnerPerEpoch}>>)'
        in lines
    )
    assert cfg.endswith("INIT Init\nNEXT Next\nINVARIANT Incomplete\nCHECK_DEADLOCK FALSE\n")
    # Without the transition check, the module is the observed-state check alone.
    module, cfg = conform.trace_module(bundle, observed, observed.constants(), ["OneWinnerPerEpoch"], [])
    assert "VARIABLE checked" in module and "Steps ==" not in module and cfg.endswith("INIT Init\nNEXT Next\n")


def _tlc_said(monkeypatch, output):
    """A TLC that prints ``output`` (its invariant checks found nothing) without running."""

    result = tla.read_tlc_output(output, 0)
    run = tla.ToolRun(command=["java", "tlc2.TLC"], exit_code=0, output=output, seconds=0.1)
    monkeypatch.setattr(conform, "tlc", lambda *args, **kwargs: (result, run))


def test_check_run_reports_the_transitions_tlc_explained(tmp_path, monkeypatch):
    bundle = load_bundle(str(BUNDLE))
    run = collect_runs([_run(tmp_path, "saved")])[0]
    tools = TlaTools(jar="unused")
    explained = '<<"spec-conform", "violated", "AckedDurable", {}>>\n' + "".join(
        f'<<"spec-conform", "matched", {j}>>\n' for j in range(2, 6)
    )
    _tlc_said(monkeypatch, explained + "5 states generated, 4 distinct states found, 0 states left on queue.\n")
    result = check_run(bundle, run, tools)
    assert result["status"] == "pass"
    assert result["transitions"] == {"checked": 4, "max_unobserved_steps": 1, "holds": True, "tlc_states": 4}
    # Short of the last record: the first unexplained record, and the step before it.
    _tlc_said(monkeypatch, explained.replace('<<"spec-conform", "matched", 5>>\n', ""))
    result = check_run(bundle, run, tools)
    assert result["status"] == "fail" and result["properties"]["AckedDurable"]["status"] == "holds"
    violation = result["transitions"]["first_violation"]
    assert (violation["index"], violation["event"]) == (5, "loaded")
    assert (violation["from"]["why"], violation["to"]["why"]) == ("Crash", "loaded sample")
    assert set(violation["to"]) == {"state", "why", "evidence", "t", "fields"}
    # Nothing matched: the first observed step is the violation.
    _tlc_said(monkeypatch, '<<"spec-conform", "violated", "AckedDurable", {}>>\n')
    assert check_run(bundle, run, tools)["transitions"]["first_violation"]["index"] == 2
    # Without the check, or without TLC, or excluded by the claims.
    assert "transitions" not in check_run(bundle, run, tools, transitions=False)
    excluded = _copy(tmp_path)
    _edit(excluded / "claims.yaml", "conform: true\n", "conform: {excluded: not on observed states}\n")
    unchecked = check_run(load_bundle(str(excluded)), run, None)
    assert unchecked["transitions"] == {"checked": 0, "holds": None, "skipped": conform.TRANSITIONS_UNCHECKED}
    _edit(excluded / "claims.yaml", "status: draft\n", "status: draft\ntransitions: {excluded: no roles}\n")
    assert check_run(load_bundle(str(excluded)), run, None)["transitions"] == {
        "checked": 0,
        "holds": None,
        "excluded": "no roles",
    }
    # A TLC that fails mid-way is an error, never a violation.
    _tlc_said(monkeypatch, explained + "Error: Attempted to apply function on a key not in its domain.\n")
    result = check_run(bundle, run, tools)
    assert result["status"] == "error" and "TLC failed on the observed states" in result["error"]


def test_conform_folder_without_tlc_errors_an_approved_bundle_whose_transitions_are_unchecked(tmp_path, monkeypatch):
    events = [("replication.link.lost", {"instance_id": "valkey-1"})]
    run = _run_folder(tmp_path / "run", "PASS", ["replication"], events)
    bundle = tmp_path / "replication"
    shutil.copytree(load_bundle("replication").path, bundle)
    monkeypatch.setenv("VALKEY_FUZZER_SPEC_PATH", str(tmp_path))
    # A replication whose properties need no TLC and whose constants need no trace header: with the transition
    # check excluded, checked without tools.
    _edit(bundle / "claims.yaml", "    conform: true\n", "    conform: {excluded: needs two nodes' samples}\n")
    _edit(
        bundle / "mapping.yaml",
        '  DualChannel: {config: dual-channel-replication-enabled, equals: "yes"}',
        '  DualChannel: {value: "FALSE"}',
    )
    assert conform_folder(run, None, "no java")["replication"]["status"] == "pass"
    text = (bundle / "claims.yaml").read_text()
    (bundle / "claims.yaml").write_text(text[: text.index("# conform's transition check")])
    draft = conform_folder(run, None, "no java")["replication"]
    assert draft["status"] == "pass" and draft["transitions"]["skipped"] == conform.TRANSITIONS_UNCHECKED
    _edit(bundle / "claims.yaml", "status: draft", "status: approved\napproved_by: someone")
    assert conform_folder(run, None, "no java")["replication"] == {
        "approved": True,
        "status": "error",
        "error": "no java",
    }
    assert conform_folder(run, None, "no java", transitions=False)["replication"]["status"] == "pass"


@needs_tlc
@pytest.mark.slow
def test_conform_checks_that_the_models_actions_explain_the_observed_steps(tmp_path):
    bundle = load_bundle("cluster-election")
    illegal = [*ELECTION_CORRECT[:4], ELECTION_CORRECT[6], *ELECTION_CORRECT[4:6]]  # the win before any ACK
    paths = {
        "correct": _election_trace(tmp_path, [*ELECTION_CORRECT, DENIED], name="correct"),
        "correct-sync": _election_trace(tmp_path, ELECTION_CORRECT, "sync", name="correct-sync"),
        "lost-best-effort": _election_trace(tmp_path, ELECTION_EVENTS, name="lost-best-effort"),
        "lost-sync": _election_trace(tmp_path, ELECTION_EVENTS, "sync", name="lost-sync"),
        "illegal": _election_trace(tmp_path, illegal, name="illegal"),
    }
    results = {name: check_run(bundle, collect_runs([path])[0], _tools()) for name, path in paths.items()}
    # A correct run, with a denied vote (a step that changes nothing observed) and under either save behavior.
    for name in ("correct", "correct-sync"):
        assert results[name]["status"] == "pass" and results[name]["transitions"]["holds"], name
    assert results["correct"]["transitions"]["checked"] == 12  # 8 steps (one denied) and 4 samples
    # Under best-effort the model lets a vote leave before its save lands: the kill that loses it is a model
    # step, and the lost vote is outside the claim.
    lost = results["lost-best-effort"]
    assert lost["status"] == "pass" and lost["transitions"]["holds"]
    assert lost["properties"]["NoVoteLostInCrash"] == {
        **lost["properties"]["NoVoteLostInCrash"],
        "status": "violated",
        "claimed": False,
    }
    # Known-bad: under sync, the ACK of a vote the crash lost is a step the model forbids (the vote cannot have
    # left before its save), at the record of that ACK.
    violation = results["lost-sync"]["transitions"]["first_violation"]
    assert results["lost-sync"]["status"] == "fail" and results["lost-sync"]["transitions"]["holds"] is False
    assert (violation["from"]["why"], violation["to"]["why"], violation["event"]) == ("Crash", "ReceiveVote", "acked")
    # Known-bad: a win before any vote arrived has no model step, whatever the states say.
    violation = results["illegal"]["transitions"]["first_violation"]
    assert (
        results["illegal"]["status"] == "fail"
        and results["illegal"]["properties"]["OneWinnerPerEpoch"]["status"] == "holds"
    )
    assert (violation["from"]["why"], violation["to"]["why"], violation["event"]) == ("HandleRequest", "Win", "won")
    # The old check alone passes it.
    assert check_run(bundle, collect_runs([paths["illegal"]])[0], _tools(), transitions=False)["status"] == "pass"
    base = ("conform", "cluster-election", "--tla2tools", JAR, "--workers", 1)
    assert _cli(*base, paths["illegal"]) == 1 and _cli(*base, "--no-transitions", paths["illegal"]) == 0


@needs_tlc
@pytest.mark.slow
def test_conform_allows_the_mappings_count_of_unobservable_steps_between_records(tmp_path):
    # Valkey saves before it acknowledges: the Ack needs one unobservable Save between the write and the ACK.
    bundle = _copy(tmp_path)
    saved = collect_runs([_run(tmp_path, "saved")])[0]
    result = check_run(load_bundle(str(bundle)), saved, _tools())
    assert result["transitions"]["holds"] and result["transitions"]["max_unobserved_steps"] == 1
    _edit(bundle / "claims.yaml", "status: draft\n", "status: draft\nmax_unobserved_steps: 0\n")
    result = check_run(load_bundle(str(bundle)), saved, _tools())
    violation = result["transitions"]["first_violation"]
    assert result["status"] == "fail" and (violation["from"]["why"], violation["to"]["why"]) == ("Write", "Ack")
    # The experiment reports every write in one line (step: repeats): 100 keys are 100 Write steps, as the live run
    # logs them, saved or lost.
    for name, kwargs in (
        ("many", {"keys": 100, "loaded": 100}),
        ("many-lost", {"keys": 100, "loaded": 0, "appendonly": "no"}),
    ):
        result = check_run(load_bundle(str(BUNDLE)), collect_runs([_run(tmp_path, name, **kwargs)])[0], _tools())
        assert result["transitions"]["holds"] and result["transitions"]["tlc_states"] > 100, name
    # Known-bad: a line that reports fewer writes than the one before it is no Write step (the register only grows).
    fewer = Path(_run(tmp_path, "fewer", keys=2))
    records = [json.loads(line) for line in fewer.read_text().splitlines()]
    records.insert(2, {**records[1], "t": 1.5, "payload": {"keys": 1}})
    fewer.write_text("\n".join(json.dumps(record) for record in records) + "\n")
    result = check_run(load_bundle(str(BUNDLE)), collect_runs([str(fewer)])[0], _tools())
    assert result["transitions"]["first_violation"]["index"] == 4 and result["status"] == "fail"


# ---------------------------------------------------------------------------
# check
# ---------------------------------------------------------------------------


def test_check_rejects_a_module_outside_the_standard_ones_before_running_sany(tmp_path, monkeypatch):
    bundle = _copy(tmp_path, monkeypatch)
    _edit(bundle / "model.tla", "EXTENDS Naturals", "EXTENDS Naturals, Helpers")
    jar = tmp_path / "tla2tools.jar"
    jar.write_bytes(b"not a jar: these checks fail before any tool runs")
    report = check_bundle(str(bundle), TlaTools(jar=str(jar)))
    parsed = report["gates"]["parsed"]
    assert report["status"] == parsed["status"] == "fail" and set(report["gates"]) == {
        "bundle",
        "log_patterns",
        "parsed",
    }
    assert "a bundle's modules may use only the standard modules" in parsed["modules"]["model.tla"]["error"]
    monkeypatch.setattr(
        TlaTools, "locate", classmethod(lambda cls, jar, **kwargs: TlaTools(jar=str(tmp_path / "tla2tools.jar")))
    )
    assert _cli("check", bundle, "--output", tmp_path / "assurance.json") == 1
    assert json.loads((tmp_path / "assurance.json").read_text())["status"] == "fail"


@pytest.mark.parametrize(
    ("java", "gate"),
    [
        # A corrupt jar: the JVM never loads SANY, which is no verdict on the model.
        ("echo 'Error: Could not find or load main class tla2sany.SANY' >&2; exit 1", "parsed"),
        # SANY runs, but TLC never starts.
        ('case "$*" in *tla2sany*) echo "Parsing file $PWD/model.tla";; *) echo "Killed" >&2; exit 137;; esac', "model_checked"),
        (None, "bundle"),
    ],
)  # fmt: skip
def test_check_is_an_error_not_a_failure_when_its_tools_or_bundle_cannot_be_used(tmp_path, monkeypatch, java, gate):
    fake = tmp_path / "java"
    fake.write_text(f"#!/bin/sh\n{java}\n")
    fake.chmod(0o755)
    (tmp_path / "tla2tools.jar").write_bytes(b"")
    monkeypatch.setattr(TlaTools, "locate", classmethod(lambda cls, jar, **kwargs: TlaTools(jar=jar, java=str(fake))))
    bundle = BUNDLE if java else tmp_path / "missing"
    output = tmp_path / "assurance.json"
    assert _cli("check", bundle, "--tla2tools", tmp_path / "tla2tools.jar", "--output", output) == 2
    report = json.loads(output.read_text())
    assert report["status"] == report["gates"][gate]["status"] == "error", json.dumps(report["gates"])


def test_check_without_tlcs_action_coverage_is_an_error_not_an_unmapped_bundle(tmp_path, monkeypatch):
    # TLC finishes but prints no coverage block: the output says nothing about the mapping.
    fake = tmp_path / "java"
    fake.write_text(
        '#!/bin/sh\ncase "$*" in *tla2sany*) echo "Parsing file $PWD/model.tla";; '
        '*) echo "TLC2 Version 2.19"; echo "Model checking completed. No error has been found.";; esac\n'
    )
    fake.chmod(0o755)
    (tmp_path / "tla2tools.jar").write_bytes(b"")
    report = check_bundle(str(BUNDLE), TlaTools(jar=str(tmp_path / "tla2tools.jar"), java=str(fake)))
    gates = report["gates"]
    assert gates["model_checked"]["status"] == "pass"
    assert gates["model_coverage"]["status"] == gates["mapping_complete"]["status"] == "error", json.dumps(gates)
    assert "no action coverage" in gates["mapping_complete"]["error"]


def test_a_tool_run_has_no_hidden_time_limit_and_the_commands_default_to_one_hour(monkeypatch):
    # A run without a deadline waits for the tool however long it takes; the commands' limit is --timeout.
    process = Mock(pid=1, returncode=0)
    process.communicate.return_value = ("", "")
    monkeypatch.setattr(tla.subprocess, "Popen", Mock(return_value=process))
    tla._run(TlaTools(jar="unused"), ["tlc2.TLC", "model.tla"], {})
    assert process.communicate.call_args.kwargs["timeout"] is None
    for command, extra in (("check", []), ("translate", ["--control", "c", "--out", "o"]), ("conform", ["run"])):
        assert create_parser().parse_args(["spec", command, "register", *extra]).timeout == 3600
    # The shared options set no default for a timeout they did not add, in either order.
    parser = argparse.ArgumentParser()
    spec_command._bundle_options(parser)
    add_common_options(parser)
    assert parser.parse_args(["register"]).timeout == 3600


def test_a_tool_run_past_its_time_is_killed_with_its_process_group(tmp_path, monkeypatch):
    # A stand-in for java that, like TLC, leaves a child process running.
    java = tmp_path / "java"
    java.write_text('#!/bin/sh\n[ "$1" = warm ] && exit 0\nsleep 60 &\necho $! > "$CHILD_PID_FILE"\nwait\n')
    java.chmod(0o755)
    # macOS scans a new executable at its first run, which can take longer than the run's 1s.
    subprocess.run([str(java), "warm"], check=True)
    monkeypatch.setenv("CHILD_PID_FILE", str(tmp_path / "child.pid"))
    tools = TlaTools(jar="unused", java=str(java)).within(1.0)
    run = tla._run(tools, ["tlc2.TLC", "model.tla"], {})
    assert run.timed_out and run.timeout_error.startswith("TLC was stopped after 1.")
    child, deadline = int((tmp_path / "child.pid").read_text()), time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            os.kill(child, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)
    os.kill(child, 9)
    raise AssertionError("TLC's child process outlived the timeout")


@needs_tlc
@pytest.mark.slow
def test_check_passes_the_toy_bundle_only_where_its_protocol_name_finds_it(tmp_path, monkeypatch):
    report = check_bundle(str(BUNDLE), _tools())
    assert report["status"] == "pass", json.dumps(report["gates"], indent=1)
    control = report["gates"]["negative_controls"]["controls"]["ack-before-save"]
    assert control["violated"] == "AckedDurable" and report["gates"]["model_coverage"]["never_fired"] == []
    # Reproducible: the bundle and the jar by name, not by a path on this host.
    # Draft claims are not a pass: the gate is skipped, and the report still passes.
    assert report["gates"]["approved_claim"]["status"] == "skip"
    assert report["bundle"] == "register" and "protocol" not in report and str(BUNDLE) not in json.dumps(report)
    # Known-bad: a copy elsewhere would compile its experiment against the fixture's mapping.
    copy = _copy(tmp_path)
    gate = check_bundle(str(copy), _tools())["gates"]["experiment"]
    assert gate["status"] == "fail" and "does not resolve to this bundle" in gate["error"]
    # A draft claim cannot become a required check.
    assert _cli("check", "register", "--tla2tools", JAR, "--workers", 1, "--require-approved") == 1


@needs_tlc
@pytest.mark.slow
@pytest.mark.parametrize(
    ("path", "old", "new", "gate"),
    [
        # A control that cannot fail means the claim is vacuous as configured.
        ("negative-controls/ack-before-save.cfg", "SyncBeforeAck = FALSE", "SyncBeforeAck = TRUE", "negative_controls"),
        # The safe config no longer meets the property's holds_when, and TLC finds the violation.
        ("model.cfg", "SyncBeforeAck = TRUE", "SyncBeforeAck = FALSE", "model_checked"),
        ("model.cfg", "MaxValue = 3", "MaxValue = 2", "claims_checked"),
        (
            "mapping.yaml",
            "unobservable:\n  Save: Valkey does not log its appendfsync writes; the reload shows what was saved.\n",
            "",
            "mapping_complete",
        ),
        ("model.tla", "AckedDurable == acked <= mem", "AckedDurable == acked <= mem +", "parsed"),
        # The experiment cannot inject the mapped fault, or does not load at all.
        ("mapping.yaml", ", component: crash}", ", component: kill}", "experiment: no fault component kill"),
        (
            "experiments/toy.register.yaml",
            "use: valkey.fault.process-signal",
            "use: valkey.fault.nosuch",
            "experiment: valkey.fault.nosuch",
        ),
        ("experiments/toy.register.yaml", "id: toy.register", "id: toy.other", "experiment: toy.register"),
        # The experiment's log rule takes its pattern from the mapping: a mapping without it does not compile.
        ("mapping.yaml", "    log: 'Done loading RDB", "    lg: 'Done loading RDB", "bundle"),
        ("mapping.yaml", "    log: 'Done loading RDB", "    # log: 'Done loading RDB", "experiment: no log pattern"),
    ],
)
def test_check_fails_a_broken_bundle(tmp_path, monkeypatch, path, old, new, gate):
    bundle = _copy(tmp_path, monkeypatch)
    _edit(bundle / path, old, new)
    report = check_bundle(str(bundle), _tools())
    gate, _, expected = gate.partition(": ")
    assert report["status"] == "fail" and report["gates"][gate]["status"] == "fail", json.dumps(report["gates"][gate])
    assert expected in json.dumps(report["gates"][gate]), json.dumps(report["gates"][gate])
    if gate == "negative_controls":
        assert "vacuous" in report["gates"][gate]["controls"]["ack-before-save"]["error"]
    if gate == "mapping_complete":
        assert report["gates"][gate]["unmapped_actions"] == ["Save"]


SHIPPED_BUNDLES = ["cluster-election", "replication", "slot-migration", "persistence", "register"]


@pytest.mark.parametrize("name", SHIPPED_BUNDLES)
def test_every_mapped_log_pattern_matches_a_line_valkey_logged(name):
    # The oracle, the log-trigger rules that publish the lines, and conform all take their patterns from one
    # mapping.yaml: a pattern Valkey never logs blinds them all at once, so each must match the bundle's sample.log.
    gate = log_patterns_gate(load_bundle(name))
    assert gate["status"] == "pass", gate
    assert gate["matched"] and all(gate["matched"].values()), gate["matched"]


def test_check_fails_a_log_pattern_that_matches_no_line_of_the_sample_log(tmp_path):
    bundle = _copy(tmp_path)
    # Deliberately broken: a wording Valkey does not log. Without this gate the rule would simply never fire.
    _edit(bundle / "mapping.yaml", "keys loaded:", "keys loaded now:")
    gate = log_patterns_gate(load_bundle(str(bundle)))
    assert gate["status"] == "fail" and gate["matched"] == {"loaded": 0}
    assert gate["error"].startswith("1 log pattern(s) match no line of sample.log: loaded;")
    # No lines to match against is no evidence that the patterns are right.
    (bundle / "sample.log").unlink()
    gate = log_patterns_gate(load_bundle(str(bundle)))
    assert gate["status"] == "fail" and "cannot read sample.log" in gate["error"] and "loaded" in gate["error"]
    # A mapping with no log pattern has nothing to check.
    _edit(bundle / "mapping.yaml", "    log: 'Done loading RDB, keys loaded now: (?P<keys>\\d+)'\n", "")
    assert log_patterns_gate(load_bundle(str(bundle))) == {"status": "pass", "note": "mapping.yaml maps no log pattern"}


def test_a_plan_pins_the_mapping_its_spec_components_read(tmp_path, monkeypatch):
    # The log trigger reads its pattern from mapping.yaml, and the plan must say which mapping: otherwise a
    # changed pattern that never matches turns into missing required evidence (a FAIL) with the same plan.
    bundle = _copy(tmp_path, monkeypatch)
    service = bundle_service(load_bundle(str(bundle)))
    scenario = {
        "scenario_id": "pin",
        "seed": 1,
        "experiment": {"id": "toy.register"},
        "deployment": {"mode": "standalone"},
    }
    plan = service.compile(scenario)
    (trigger,) = [component for component in plan.components if component.use == "valkey.observer.log-trigger"]
    assert trigger.config["spec_sha256"] == load_spec_mapping("register").sha256

    _edit(bundle / "mapping.yaml", "keys loaded:", "keys loaded now:")
    assert service.compile(scenario).to_json() != plan.to_json()
    with pytest.raises(Exception, match=r"spec 'register': .*mapping\.yaml has sha256 \w+, but the plan was compiled"):
        service.validate_plan(plan)
