"""The spec bridge on the toy register bundle in ``tests/fixtures/spec/register``, whose negative control
acknowledges a write before saving it, so a crash loses it. The slow tests run TLC (java and ``$TLA2TOOLS_JAR``)."""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import time
from pathlib import Path
from unittest.mock import Mock

import pytest

from tests.conform_support import approved_register, register_service, scenario_file
from tests.conftest import TLA_JAR as JAR
from tests.conftest import cli, needs_tlc
from tests.valkey_experiments_support import valkey_service
from valkey_fuzzer import __version__
from valkey_fuzzer.cli import create_parser
from valkey_fuzzer.commands import spec as spec_command
from valkey_fuzzer.commands._common import add_common_options
from valkey_fuzzer.service import RunService
from valkey_fuzzer.spec import conform, tla, translate
from valkey_fuzzer.spec.bundle import SpecBundleError, derive_constants, load_bundle, load_spec_mapping
from valkey_fuzzer.spec.check import check_bundle, log_patterns_gate
from valkey_fuzzer.spec.conform import ObservedRun, check_run, collect_runs, conform_folder, conform_runs
from valkey_fuzzer.spec.tla import (
    ModelValue,
    TlaError,
    TlaSet,
    TlaTools,
    TraceState,
    changed_keys,
    parse_value,
    print_values,
    read_tlc_output,
    render,
)
from valkey_fuzzer.spec.translate import TranslateError, bundle_service, translate_trace

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "spec"
BUNDLE = FIXTURES / "register"
CONTROL = BUNDLE / "negative-controls" / "ack-before-save.cfg"
COUNTEREXAMPLE = FIXTURES / "register-ack-before-save.out"
ELECTION = Path(__file__).resolve().parents[1] / "formal" / "cluster-election"
ID = {name: name[-1] * 40 for name in ("valkey-0", "valkey-1", "valkey-2", "valkey-3")}


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
    """``valkey-fuzzer spec argv`` through the command's handler, so a failure shows its traceback."""

    return cli("spec", *argv, via_main=False)


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


ELECTION_2026 = FIXTURES / "cluster-election-vote-before-save-2026"  # one TLC 2026 run: .out and .trace.json


def test_a_json_trace_reads_as_the_text_trace_of_the_same_run_does():
    # The register's JSON dump (TLC 2026) is the trace the 2.19 text fixture holds.
    json_trace = (FIXTURES / "register-ack-before-save.trace.json").read_text()
    assert tla.read_json_trace(json_trace) == read_tlc_output(COUNTEREXAMPLE.read_text()).trace
    result = read_tlc_output(COUNTEREXAMPLE.read_text(), 12, json_trace=json_trace)
    assert result.trace_format == "json" and [state.action for state in result.trace] == [
        "Init",
        "Write",
        "Ack",
        "Crash",
    ]
    # A 2026 TLC labels a state with the action's arguments; the text reader keeps the name alone.
    text = read_tlc_output((ELECTION_2026.with_suffix(".out")).read_text(), 12)
    assert text.violated == "OneWinnerPerEpoch" and text.trace_format == "text"
    assert [state.action for state in text.trace[:4]] == ["Init", "StartElection", "StartElection", "HandleRequest"]
    # JSON writes model values as strings, functions and records as objects, sets and sequences as arrays: the
    # config's model values come back as such, an object keyed by them as a function, and arrays as tuples, in
    # TLC's fixed order, so both readers agree on every state.
    config = (ELECTION / "negative-controls" / "vote-before-save.cfg").read_text()
    names = tla.config_model_values(config)
    assert names == {"p1", "p2", "p3", "r1", "r2"}
    parsed = tla.read_json_trace(ELECTION_2026.with_suffix(".trace.json").read_text(), names)
    assert [(state.index, state.action) for state in parsed] == [(state.index, state.action) for state in text.trace]
    for ours, theirs in zip(parsed, text.trace):
        for name in ("lastVote", "currentEpoch", "diskVote", "electionEpoch", "crashes"):
            assert ours.values[name] == theirs.values[name], (ours.index, name)
        assert sorted(map(render, ours.values["msgs"])) == sorted(map(render, theirs.values["msgs"]))
    last = parsed[-1].values
    assert isinstance(last["lastVote"], tla.TlaFunction) and list(last["lastVote"]) == [
        ModelValue(f"p{i}") for i in (1, 2, 3)
    ]
    assert last["msgs"][0] == {"type": "request", "from": ModelValue("r1"), "epoch": 1}
    assert last["bio"][ModelValue("p1")] == ((1, 1),) and last["winners"] == (
        (ModelValue("r1"), 1),
        (ModelValue("r2"), 1),
    )
    assert render(last["lastVote"]) == "(p1 :> 1 @@ p2 :> 1 @@ p3 :> 1)"
    # A dump that is not a trace is an error of the result, and the trace comes from the text.
    for broken in ("not json", '{"counterexample": {"state": [[1, 2]]}}', "[]"):
        result = read_tlc_output(COUNTEREXAMPLE.read_text(), 12, json_trace=broken)
        assert result.trace_format == "text" and len(result.trace) == 4 and not result.no_error
        assert result.errors[0].startswith("cannot read TLC's JSON trace") and "text output instead" in result.errors[0]
    with pytest.raises(TlaError, match="cannot read TLC's JSON trace"):
        tla.read_json_trace("{}")


def test_translate_reads_a_json_counterexample_as_it_reads_the_text(tmp_path):
    # The same counterexample, from the JSON dump and from the text of the run that wrote it.
    config = (ELECTION / "negative-controls" / "vote-before-save.cfg").read_text()
    bundle = load_bundle("cluster-election")
    compile_scenario = valkey_service().compile
    traces = {
        "text": read_tlc_output(ELECTION_2026.with_suffix(".out").read_text(), 12).trace,
        "json": tla.read_json_trace(
            ELECTION_2026.with_suffix(".trace.json").read_text(), tla.config_model_values(config)
        ),
    }
    translations = {
        name: translate_trace(bundle, trace, config, name="vote-before-save", violated="OneWinnerPerEpoch",
                              compile_scenario=compile_scenario)
        for name, trace in traces.items()
    }  # fmt: skip
    assert translations["json"]["alignment"] == translations["text"]["alignment"]
    assert translations["json"]["scenarios"] == translations["text"]["scenarios"]
    assert translations["json"]["alignment"][0]["process"] == "p1"
    # The command takes the dump too (the violated property is then not known).
    out = tmp_path / "scenarios"
    trace, cfg = ELECTION_2026.with_suffix(".trace.json"), ELECTION / "negative-controls" / "vote-before-save.cfg"
    assert _cli("translate", "cluster-election", "--trace", trace, "--config", cfg, "--out", out, "--variants", 1) == 0
    translation = json.loads((out / "translation.json").read_text())
    assert translation["counterexample"]["length"] == 18 and translation["counterexample"]["violated"] is None


def _fake_java(tmp_path, script):
    """A stand-in for java: a shell script, and an empty jar next to it."""

    java = tmp_path / "java"
    java.write_text(f"#!/bin/sh\n{script}\n")
    java.chmod(0o755)
    jar = tmp_path / "tla2tools.jar"
    jar.write_bytes(b"not a jar")
    return java, jar


HELP_2_19 = "TLC - provides model checking and simulation of TLA+ specifications - Version 2.19 of 08 August 2024"
HELP_2026 = "TLC [-h] [-dumpTrace format file] SPEC\n\tTLC - provides ... - Version 2026.11.19.180144"


def test_the_tools_are_probed_for_tlcs_version_and_whether_it_dumps_json_traces(tmp_path, monkeypatch):
    java, jar = _fake_java(tmp_path, f'case "$*" in *-h*) echo "{HELP_2_19}";; esac')
    monkeypatch.setattr(tla.shutil, "which", lambda name: str(java))
    tools = TlaTools.locate(str(jar))
    assert (tools.tlc_version, tools.json_trace) == ("2.19", False)
    # A jar that is no release the bridge knows runs with a warning, or not at all with --strict.
    digest = tools.sha256()
    (warning,) = tools.warnings
    assert warning.startswith(f"tla2tools.jar (sha256 {digest}) is not a tla2tools.jar release the spec bridge knows")
    assert "v1.7.4" in warning and "v1.8.0" in warning
    assert tools.identity() == {
        "jar": "tla2tools.jar",
        "sha256": digest,
        "release": None,
        "tlc_version": "2.19",
        "json_trace": False,
    }
    with pytest.raises(TlaError, match=r"is not a tla2tools.jar release the spec bridge knows .*\(--strict\)$"):
        TlaTools.locate(str(jar), strict=True)
    monkeypatch.setitem(tla.TLA_TOOLS_RELEASES, digest, "v9.9.9")
    tools = TlaTools.locate(str(jar), strict=True)
    assert tools.warnings == () and tools.identity()["release"] == "v9.9.9"
    # A 2026 TLC takes -dumpTrace; its version is a date, later than 2.19 as numbers. (The probe is cached on the
    # jar, which has not changed: a new java at the same path needs the cache cleared.)
    java.write_text(f'#!/bin/sh\nprintf "%s\\n" "{HELP_2026}"\n')
    tla._probe.cache_clear()
    tools = TlaTools.locate(str(jar))
    assert (tools.tlc_version, tools.json_trace) == ("2026.11.19.180144", True)
    assert tla.version_key("2026.11.19.180144") > tla.version_key("2.19") > tla.version_key("2.18")
    # Too old, or no TLC at all (a corrupt jar): an error that names the jar and how to get one.
    java.write_text('#!/bin/sh\necho "TLC - Version 2.18 of 01 January 2023"\n')
    tla._probe.cache_clear()
    with pytest.raises(
        TlaError,
        match=rf"^TLC 2.18 in {re.escape(str(jar))} is older than 2.19, the oldest the spec bridge runs; the spec bridge needs Java 11",
    ):
        TlaTools.locate(str(jar))
    java.write_text('#!/bin/sh\necho "Error: Could not find or load main class tlc2.TLC" >&2\nexit 1\n')
    tla._probe.cache_clear()
    with pytest.raises(
        TlaError,
        match=r"does not run TLC \(tlc2.TLC -h printed no version: Error: Could not find or load main class tlc2.TLC\)",
    ):
        TlaTools.locate(str(jar))
    # Tools made without the probe (as tests do) report no version; check records TLC's banner instead.
    assert TlaTools(jar=str(jar)).identity()["tlc_version"] is None


def test_the_probe_runs_once_per_jar_and_again_when_the_jar_changes(tmp_path, monkeypatch):
    # Every java start is a line in calls; the probe is the only one these tools make.
    calls = tmp_path / "calls"
    java, jar = _fake_java(tmp_path, f'echo "$*" >> "{calls}"\necho "{HELP_2_19}"')
    monkeypatch.setattr(tla.shutil, "which", lambda name: str(java))
    tools = TlaTools.locate(str(jar))
    again = TlaTools.locate(str(jar), workers="1", strict=False).within(5.0)
    assert len(calls.read_text().splitlines()) == 1
    # The digest the probe took is the one identity reports, on the probed tools and on their copies.
    digest = hashlib.sha256(b"not a jar").hexdigest()
    assert tools.digest == again.digest == digest and again.identity()["sha256"] == digest
    assert again.tlc_version == "2.19" and again.timeout_s == 5.0
    # A changed jar (its size or modification time) is probed and hashed again.
    jar.write_bytes(b"not a jar, rebuilt")
    assert TlaTools.locate(str(jar)).digest == hashlib.sha256(b"not a jar, rebuilt").hexdigest()
    assert len(calls.read_text().splitlines()) == 2
    # A probe that fails is not kept: the next locate tries the jar again.
    java.write_text(f'#!/bin/sh\necho "$*" >> "{calls}"\necho "no TLC here" >&2\nexit 1\n')
    jar.write_bytes(b"not a jar, broken")
    for _ in range(2):
        with pytest.raises(TlaError, match="does not run TLC"):
            TlaTools.locate(str(jar))
    assert len(calls.read_text().splitlines()) == 4


def test_tlc_dumps_the_trace_as_json_when_it_can(tmp_path, monkeypatch):
    # A TLC that writes trace.json next to the spec (as -dumpTrace json does) and prints the text trace too.
    json_trace = (FIXTURES / "register-ack-before-save.trace.json").read_text().replace("\n", " ")
    text = COUNTEREXAMPLE.read_text().replace("Ack line", "Ack(1) line")  # a 2026 label, as its text would show
    (tmp_path / "out.txt").write_text(text)
    script = (
        f"case \"$*\" in *-dumpTrace*) printf %s '{json_trace}' > trace.json;; esac\n"
        f'cat "{tmp_path / "out.txt"}"\nexit 12'
    )
    java, jar = _fake_java(tmp_path, script)
    files = {"model.tla": "", "ack-before-save.cfg": CONTROL.read_text()}
    tools = TlaTools(jar=str(jar), java=str(java), json_trace=True)
    result, run = tla.tlc(tools, "model.tla", "ack-before-save.cfg", files)
    assert run.command[-4:] == ["-dumpTrace", "json", "trace.json", "model.tla"] and "trace.json" in run.files
    assert result.trace_format == "json" and result.violated == "AckedDurable" and not result.errors
    assert [state.action for state in result.trace] == ["Init", "Write", "Ack", "Crash"]
    # A TLC that cannot: no -dumpTrace, and the trace from the text.
    result, run = tla.tlc(TlaTools(jar=str(jar), java=str(java)), "model.tla", "ack-before-save.cfg", files)
    assert "-dumpTrace" not in run.command and run.files == {} and result.trace_format == "text"
    assert [state.action for state in result.trace] == ["Init", "Write", "Ack", "Crash"]


def test_check_reports_the_tools_and_strict_refuses_an_unknown_jar(tmp_path, monkeypatch, capsys):
    java, jar = _fake_java(tmp_path, f'case "$*" in *-h*) echo "{HELP_2_19}";; *) echo "Killed" >&2; exit 137;; esac')
    monkeypatch.setattr(tla.shutil, "which", lambda name: str(java))
    assert _cli("check", BUNDLE, "--tla2tools", jar, "--strict") == 2
    assert "Error: tla2tools.jar (sha256 " in capsys.readouterr().err
    # Without --strict the check runs (here, to the SANY that never starts), and says what it ran with.
    output = tmp_path / "assurance.json"
    assert _cli("check", BUNDLE, "--tla2tools", jar, "--output", output) == 2
    captured = capsys.readouterr()
    assert captured.err.startswith("Warning: tla2tools.jar (sha256 ")
    assert captured.out.startswith("Tools: TLC 2.19 from tla2tools.jar (no release the spec bridge knows, sha256 ")
    report = json.loads(output.read_text())
    assert report["tools"]["tlc_version"] == "2.19" and report["tools"]["release"] is None
    assert report["tools"]["warnings"][0].startswith("tla2tools.jar (sha256 ")


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
# translate: searching the model
# ---------------------------------------------------------------------------


def _searchable(tmp_path, monkeypatch):
    """A register copy the search has more to find in: a target (a crash after an acknowledged write), a second
    boolean constant the model does not read (SaveOnAck), and a second safe config that sets it FALSE."""

    bundle = _copy(tmp_path, monkeypatch)
    _edit(
        bundle / "model.tla",
        "    SyncBeforeAck,  \\*",
        "    SaveOnAck,  \\* unread: a second boolean to flip\n    SyncBeforeAck,  \\*",
    )
    _edit(
        bundle / "model.tla",
        "AckedDurable == acked <= mem\n",
        "AckedDurable == acked <= mem\n\nAckedThenCrashed == acked > 0 /\\ crashes > 0\n",
    )
    _edit(
        bundle / "claims.yaml",
        "assumptions:",
        "targets:\n  - {name: AckedThenCrashed, meaning: A crash after an acknowledged write.}\nassumptions:",
    )
    _edit(bundle / "mapping.yaml", "constants:\n", 'constants:\n  SaveOnAck: {value: "TRUE"}\n')
    for config in ("model.cfg", "negative-controls/ack-before-save.cfg"):
        _edit(bundle / config, "    MaxValue = 3\n", "    SaveOnAck = TRUE\n    MaxValue = 3\n")
    (bundle / "other.cfg").write_text(
        (bundle / "model.cfg").read_text().replace("SaveOnAck = TRUE", "SaveOnAck = FALSE")
    )
    return bundle


def _tlc_output(violated, steps):
    """TLC's text output for an invariant violation whose trace is ``steps`` (``(action, {var: value})`` pairs)."""

    lines = [
        "TLC2 Version 2.19",
        f"Error: Invariant {violated} is violated.",
        "Error: The behavior up to this point is:",
    ]
    for index, (action, values) in enumerate(steps, 1):
        label = "Initial predicate" if action == "Init" else f"{action} line 1, col 1 to line 1, col 9 of module model"
        lines += [f"State {index}: <{label}>", *(f"/\\ {name} = {value}" for name, value in values.items()), ""]
    lines.append(f"{3 * len(steps)} states generated, {2 * len(steps)} distinct states found, 0 states left on queue.")
    return "\n".join(lines) + "\n"


def _states(*steps):
    return [
        (action, {"mem": mem, "acked": acked, "disk": disk, "crashes": crashes})
        for action, mem, acked, disk, crashes in steps
    ]


REACHED = _tlc_output(
    "NotAckedThenCrashed",
    _states(
        ("Init", 0, 0, 0, 0), ("Write", 1, 0, 0, 0), ("Save", 1, 0, 1, 0), ("Ack", 1, 1, 1, 0), ("Crash", 1, 1, 1, 1)
    ),
)
LOST = _tlc_output(
    "AckedDurable", _states(("Init", 0, 0, 0, 0), ("Write", 1, 0, 0, 0), ("Ack", 1, 1, 0, 0), ("Crash", 0, 1, 0, 1))
)
HELD = (
    "TLC2 Version 2.19\nModel checking completed. No error has been found.\n"
    "40 states generated, 20 distinct states found, 0 states left on queue.\n"
    "The depth of the complete state graph search is 5.\nFinished in 01s at (2026-01-01 00:00:00)\n"
)
TLC_FAILED = (
    "TLC2 Version 2.19\nError: Parsing or semantic analysis failed.\nFinished in 00s at (2026-01-01 00:00:00)\n"
)


class _FakeTlc:
    """A TLC that answers from the config it is given, without running: a reach run reaches the target, a config
    with ``SyncBeforeAck = FALSE`` violates AckedDurable, any other holds (or ``default``). ``answers`` override
    that by a text of the config, the first that matches (``Name_VALUE`` for ``Name = VALUE``; ``"timeout"`` for a
    run that runs out of time, else TLC output); every call is recorded."""

    def __init__(self, monkeypatch, default=None, **answers):
        self.default = default
        self.answers = {key.replace("_", " = "): value for key, value in answers.items()}
        self.calls = []
        monkeypatch.setattr(translate, "tlc", self)

    def __call__(self, tools, module, config, files):
        text = files[config]
        self.calls.append((tools, module, text))
        output = next((answer for key, answer in self.answers.items() if key in text), self.default)
        if output is None:
            output = REACHED if module == "reach.tla" else LOST if "SyncBeforeAck = FALSE" in text else HELD
        run = tla.ToolRun(command=["java", "tlc2.TLC"], exit_code=0, output=output, seconds=0.5)
        if output == "timeout":
            run = tla.ToolRun(
                command=["java", "tlc2.TLC"], exit_code=-1, output="", seconds=tools.timeout_s, timed_out=True
            )
        result = tla.read_tlc_output(run.output, run.exit_code)
        if run.timed_out:
            result.errors.append(run.timeout_error)
        return result, run


def _search(bundle, **kwargs):
    options = {
        "tlc_timeout_s": 300.0,
        "max_scenarios": 50,
        "compile_scenario": bundle_service(bundle).compile,
        **kwargs,
    }
    return translate.search_model(bundle, TlaTools(jar="unused"), **options)


def test_mutations_flip_each_boolean_constant_alone_and_in_pairs():
    constants = {"A": "TRUE", "N": "3", "B": "FALSE", "S": "{p1, p2}"}
    assert translate.mutations(constants) == [{"A": "FALSE"}, {"B": "TRUE"}]
    assert translate.mutations(constants, pairs=True) == [{"A": "FALSE", "B": "TRUE"}]
    config = "\\* A is on\nCONSTANTS\n    A = TRUE\n    N = 3\n    B = FALSE\nINVARIANT Safe\n"
    assert (
        translate.flipped_config(config, {"A": "FALSE", "B": "TRUE"})
        == "\nCONSTANTS\n    A = FALSE\n    N = 3\n    B = TRUE\nINVARIANT Safe\n"
    )
    with pytest.raises(TranslateError, match="N is assigned a boolean 0 times"):
        translate.flipped_config(config, {"N": "TRUE"})
    # A reach run extends the model with the invariant that the target is not reached, checked by the safe config.
    files = translate.reach_files(load_bundle(str(BUNDLE)), "model.cfg", "AckedDurable")
    assert files["reach.tla"] == "---- MODULE reach ----\nEXTENDS model\nNotAckedDurable == ~AckedDurable\n====\n"
    assert files["reach.cfg"].endswith("CHECK_DEADLOCK FALSE\nINVARIANT NotAckedDurable\n") and "model.tla" in files


def test_reach_translates_the_path_to_each_target_and_reports_the_unreachable_ones(tmp_path, monkeypatch):
    bundle = load_bundle(str(_searchable(tmp_path, monkeypatch)))
    fake = _FakeTlc(monkeypatch)
    report = _search(bundle, reach=["AckedThenCrashed"])
    assert [(module, "INVARIANT NotAckedThenCrashed" in text) for _, module, text in fake.calls] == [
        ("reach.tla", True)
    ] * 2
    reached, again = report["targets"]["AckedThenCrashed"].values()
    assert (reached["status"], reached["actions"]) == ("reached", ["Init", "Write", "Save", "Ack", "Crash"])
    assert reached["scenarios"] == [
        "spec-register-model-reach-AckedThenCrashed",
        "spec-register-model-reach-AckedThenCrashed-v1",
    ]
    # The second safe config finds the same path: translated once.
    assert (again["status"], again["same_as"]) == ("duplicate", "model-reach-AckedThenCrashed")
    (translation,) = report["translations"]
    assert translation["source"] == {"kind": "reach", "target": "AckedThenCrashed", "config": "model.cfg"}
    assert translation["scenarios"][0]["experiment"]["inputs"] == {"appendonly": "yes"}  # the safe config's constants
    assert (report["unreachable"], report["held"], report["stopped"]) == ([], [], None)
    assert report["scenarios"] == reached["scenarios"] and report["kind"] == "spec-search"
    # Known-bad: a target no safe config reaches, and a target the claims do not name.
    _FakeTlc(monkeypatch, SaveOnAck_TRUE=HELD, SaveOnAck_FALSE=HELD)
    report = _search(bundle, reach=["AckedThenCrashed"])
    assert {entry["status"] for entry in report["targets"]["AckedThenCrashed"].values()} == {"unreachable"}
    assert report["unreachable"] == ["AckedThenCrashed"] and report["translations"] == []
    with pytest.raises(TranslateError, match="names no target Lost; its targets are AckedThenCrashed"):
        _search(bundle, reach=["Lost"])


def test_mutate_flips_the_safe_configs_booleans_and_translates_each_distinct_violation(tmp_path, monkeypatch):
    bundle = load_bundle(str(_searchable(tmp_path, monkeypatch)))
    fake = _FakeTlc(monkeypatch)
    report = _search(bundle, mutate=True)
    model, other = report["mutations"]["model.cfg"], report["mutations"]["other.cfg"]
    assert list(model) == ["SyncBeforeAck=FALSE", "SaveOnAck=FALSE", "SyncBeforeAck=FALSE,SaveOnAck=FALSE"]
    # Flipping SaveOnAck alone gives the other safe config's constants, which TLC already found no error in.
    assert model["SaveOnAck=FALSE"] == {
        "status": "skipped",
        "flipped": {"SaveOnAck": "FALSE"},
        "reason": "these are other.cfg's constants",
    }
    violated = model["SyncBeforeAck=FALSE"]
    assert (violated["status"], violated["violated"], violated["flipped"]) == (
        "violated",
        "AckedDurable",
        {"SyncBeforeAck": "FALSE"},
    )
    assert violated["scenarios"] == [
        "spec-register-model-SyncBeforeAck-FALSE",
        "spec-register-model-SyncBeforeAck-FALSE-v1",
    ]
    # The pair was cheap (the singles took 0.5 s each) and finds the same trace, as does the other config's flip.
    pair = model["SyncBeforeAck=FALSE,SaveOnAck=FALSE"]
    assert (pair["status"], pair["same_as"], pair["actions"]) == (
        "duplicate",
        "model-SyncBeforeAck-FALSE",
        ["Init", "Write", "Ack", "Crash"],
    )
    assert other["SyncBeforeAck=FALSE"]["status"] == "duplicate" and other["SaveOnAck=TRUE"]["status"] == "skipped"
    # TLC ran the model itself on each config with the flips applied, in the order of the flips.
    flipped = [("SyncBeforeAck = FALSE" in text, "SaveOnAck = FALSE" in text) for _, _, text in fake.calls]
    assert flipped == [(True, False), (True, True), (True, True), (True, False)]
    assert {module for _, module, _ in fake.calls} == {"model.tla"}
    (translation,) = report["translations"]
    assert translation["source"] == {"kind": "mutation", "config": "model.cfg", "flipped": {"SyncBeforeAck": "FALSE"}}
    assert translation["counterexample"]["constants"]["SyncBeforeAck"] == "FALSE"
    assert translation["scenarios"][0]["experiment"]["inputs"] == {"appendonly": "no"}
    assert report["held"] == [] and report["scenarios"] == violated["scenarios"]
    # A mutation under which every check holds is reported as such, and is no counterexample.
    _FakeTlc(monkeypatch, SyncBeforeAck_FALSE=HELD)
    report = _search(bundle, mutate=True)
    assert report["held"] == [
        "model.cfg: SyncBeforeAck=FALSE",
        "model.cfg: SyncBeforeAck=FALSE,SaveOnAck=FALSE",
        "other.cfg: SyncBeforeAck=FALSE",
        "other.cfg: SyncBeforeAck=FALSE,SaveOnAck=TRUE",
    ]
    assert report["translations"] == [] and report["scenarios"] == []


def test_mutate_sets_aside_a_violation_that_takes_no_fault_and_looks_again(tmp_path, monkeypatch):
    # Under best-effort saves TLC first finds a vote sent before its save (VotesSavedBeforeSent, four states, no
    # crash): the experiment has nothing to inject. Without that property the same mutation finds the lost vote.
    bundle = load_bundle("cluster-election")
    shallow = _tlc_output("VotesSavedBeforeSent", [(a, {"msgs": "{}"}) for a in ("Init", "StartElection", "HandleRequest", "ReceiveVote")])  # fmt: skip
    deep = (FIXTURES / "cluster-election-vote-before-save.out").read_text()
    fake = _FakeTlc(monkeypatch, default=deep, VotesSavedBeforeSent=shallow)
    report = translate.search_model(
        bundle, TlaTools(jar="unused"), tlc_timeout_s=300.0, max_scenarios=50, mutate=True, compile_scenario=valkey_service().compile
    )  # fmt: skip
    first = report["mutations"]["model.cfg"]["PersistBeforeAck=FALSE"]
    assert (first["status"], first["violated"], first["seconds"]) == ("violated", "OneWinnerPerEpoch", 1.0)
    assert first["without_fault"] == [{"violated": "VotesSavedBeforeSent", "actions": ["Init", "StartElection", "HandleRequest", "ReceiveVote"]}]  # fmt: skip
    assert first["scenarios"][0] == "spec-cluster-election-model-PersistBeforeAck-FALSE"
    # The second run checked the config without the property set aside, and with the others.
    checks = [tla.parse_config(text).checked for _, _, text in fake.calls[:2]]
    assert checks[0] == ("TypeOK", "OneWinnerPerEpoch", "VoteOncePerEpoch", "VotesSavedBeforeSent", "NoVoteLostInCrash")
    assert checks[1] == ("TypeOK", "OneWinnerPerEpoch", "VoteOncePerEpoch", "NoVoteLostInCrash")
    # The other flips find the same deep trace: duplicates, each after the same set-aside.
    others = [entry for label, entry in report["mutations"]["model.cfg"].items() if label != "PersistBeforeAck=FALSE"]
    assert [(entry["status"], entry["same_as"]) for entry in others] == [
        ("duplicate", "model-PersistBeforeAck-FALSE")
    ] * 2
    assert len(fake.calls) == 6 and len(report["scenarios"]) == 5
    # When no claimed property is left to check, the mutation is no-fault without another TLC run.
    register = load_bundle(str(_searchable(tmp_path, monkeypatch)))
    no_crash = _tlc_output("AckedDurable", _states(("Init", 0, 0, 0, 0), ("Write", 1, 0, 0, 0), ("Ack", 1, 1, 0, 0)))
    fake = _FakeTlc(monkeypatch, SyncBeforeAck_FALSE=no_crash)
    report = _search(register, mutate=True)
    entry = report["mutations"]["model.cfg"]["SyncBeforeAck=FALSE"]
    assert entry["status"] == "no-fault" and [item["violated"] for item in entry["without_fault"]] == ["AckedDurable"]
    assert "violated" not in entry and "scenarios" not in entry
    # One TLC run per flip that ran (two configs, a single and a pair each): nothing was left to look for.
    assert len(fake.calls) == 4 and [e["status"] for e in report["mutations"]["other.cfg"].values()] == ["no-fault", "skipped", "no-fault"]  # fmt: skip
    assert report["held"] == [] and report["translations"] == []


def test_the_search_translates_one_counterexample_per_action_sequence(tmp_path, monkeypatch):
    # The reach path and the mutation's trace differ (Save before the Ack, or not), so both are kept; a second
    # mutation with the mutation's trace is a duplicate of it, whatever its constants.
    bundle = load_bundle(str(_searchable(tmp_path, monkeypatch)))
    _FakeTlc(monkeypatch, SaveOnAck_FALSE=LOST)
    report = _search(bundle, reach=["AckedThenCrashed"], mutate=True)
    assert [item["counterexample"]["actions"] for item in report["translations"]] == [
        ["Init", "Write", "Save", "Ack", "Crash"],
        ["Init", "Write", "Ack", "Crash"],
    ]
    assert [item["counterexample"]["name"] for item in report["translations"]] == [
        "model-reach-AckedThenCrashed",
        "model-SyncBeforeAck-FALSE",
    ]
    statuses = {label: entry["status"] for label, entry in report["mutations"]["model.cfg"].items()}
    assert statuses == {
        "SaveOnAck=FALSE": "skipped",
        "SyncBeforeAck=FALSE": "violated",
        "SyncBeforeAck=FALSE,SaveOnAck=FALSE": "duplicate",
    }
    # other.cfg already sets SaveOnAck = FALSE, so TLC answers its SyncBeforeAck flip with the same lost write.
    assert report["mutations"]["other.cfg"]["SyncBeforeAck=FALSE"]["same_as"] == "model-SyncBeforeAck-FALSE"
    # Seeds never repeat across the search's scenarios.
    seeds = [scenario["seed"] for item in report["translations"] for scenario in item["scenarios"]]
    assert seeds == [1, 2, 3, 4]


def test_each_tlc_run_of_the_search_has_its_own_time_budget(tmp_path, monkeypatch):
    bundle = load_bundle(str(_searchable(tmp_path, monkeypatch)))
    fake = _FakeTlc(monkeypatch, SyncBeforeAck_FALSE="timeout")
    before = time.monotonic()
    report = _search(bundle, mutate=True, tlc_timeout_s=7.5)
    # Every run got the per-run budget, as a deadline from when it started.
    assert all(
        tools.timeout_s == 7.5 and before + 7.5 <= tools.deadline <= time.monotonic() + 7.5
        for tools, _, _ in fake.calls
    )
    model = report["mutations"]["model.cfg"]
    timed_out = model["SyncBeforeAck=FALSE"]
    assert (
        timed_out["status"] == "timed_out"
        and timed_out["error"] == "TLC was stopped after 7.5s: the time limit ran out before it finished"
    )
    # The search went on to the other config, but the pairs did not run: the single flips were not cheap.
    assert report["mutations"]["other.cfg"]["SyncBeforeAck=FALSE"]["status"] == "timed_out"
    pair = model["SyncBeforeAck=FALSE,SaveOnAck=FALSE"]
    assert (
        pair["status"] == "not-run"
        and pair["reason"] == "the single flips took 7.5s together, more than one run's 7.5s"
    )
    assert report["tlc_timeout_s"] == 7.5 and report["translations"] == [] and report["stopped"] is None
    # The command's own limit bounds each run too, and once it has passed no run starts.
    fake = _FakeTlc(monkeypatch)
    tools = TlaTools(jar="unused").within(2.0)
    report = translate.search_model(bundle, tools, tlc_timeout_s=300.0, max_scenarios=50, reach=["AckedThenCrashed"])
    assert all(run_tools.timeout_s <= 2.0 for run_tools, _, _ in fake.calls) and report["translations"]
    report = translate.search_model(
        bundle, TlaTools(jar="unused").within(0.0), tlc_timeout_s=300.0, max_scenarios=50, mutate=True
    )
    assert report["stopped"] == "the command's time limit ran out"
    assert {entry["status"] for entry in report["mutations"]["model.cfg"].values()} == {"skipped", "not-run"}


def test_the_search_stops_at_its_scenario_cap(tmp_path, monkeypatch):
    bundle = load_bundle(str(_searchable(tmp_path, monkeypatch)))
    _FakeTlc(monkeypatch)
    report = _search(bundle, reach=["AckedThenCrashed"], mutate=True, max_scenarios=3)
    # The reach path takes two scenarios; the mutation has room for one, its direct translation; then nothing runs.
    assert [len(item["scenarios"]) for item in report["translations"]] == [2, 1]
    assert report["scenarios"] == [
        "spec-register-model-reach-AckedThenCrashed",
        "spec-register-model-reach-AckedThenCrashed-v1",
        "spec-register-model-SyncBeforeAck-FALSE",
    ]
    assert report["stopped"] == "the scenario cap (3) was reached"
    pair = report["mutations"]["model.cfg"]["SyncBeforeAck=FALSE,SaveOnAck=FALSE"]
    assert pair == {
        "status": "not-run",
        "reason": "the scenario cap (3) was reached",
        "flipped": {"SyncBeforeAck": "FALSE", "SaveOnAck": "FALSE"},
    }
    assert all(entry["status"] in ("skipped", "not-run") for entry in report["mutations"]["other.cfg"].values())
    # A counterexample the mapping cannot express is reported, and the search goes on.
    broken = _tlc_output("NotAckedThenCrashed", _states(("Init", 0, 0, 0, 0), ("Crash", 0, 0, 0, 1)))
    _FakeTlc(monkeypatch, SaveOnAck_TRUE=broken)
    report = _search(bundle, reach=["AckedThenCrashed"], mutate=True)
    entry = report["targets"]["AckedThenCrashed"]["model.cfg"]
    assert entry["status"] == "untranslatable" and "Crash at state 2 follows no acked step" in entry["error"]
    assert report["targets"]["AckedThenCrashed"]["other.cfg"]["status"] == "reached"
    assert len(report["translations"]) == 2


def test_translate_searches_the_model_from_the_command_line(tmp_path, monkeypatch, capsys):
    bundle = _searchable(tmp_path, monkeypatch)
    monkeypatch.setattr(TlaTools, "locate", classmethod(lambda cls, jar, **kwargs: TlaTools(jar="unused")))
    _FakeTlc(monkeypatch)
    out = tmp_path / "scenarios"
    assert (
        _cli(
            "translate", "register", "--reach", "all", "--mutate", "--out", out, "--variants", "1", "--tlc-timeout", "9"
        )
        == 0
    )
    printed = capsys.readouterr().out
    assert (
        "TLC: model.cfg reaching AckedThenCrashed" in printed and "TLC: model.cfg with SyncBeforeAck = FALSE" in printed
    )
    assert "[REACHED       ] AckedThenCrashed in model.cfg: 2 scenario(s)" in printed
    assert (
        "[DUPLICATE     ] AckedThenCrashed in other.cfg: same action sequence as model-reach-AckedThenCrashed"
        in printed
    )
    assert "[SKIPPED       ] model.cfg with SaveOnAck=FALSE: these are other.cfg's constants" in printed
    assert "4 scenario(s) from 2 counterexample(s)" in printed
    names = sorted(path.name for path in out.iterdir())
    assert names == [
        "spec-register-model-SyncBeforeAck-FALSE-v1.yaml",
        "spec-register-model-SyncBeforeAck-FALSE.yaml",
        "spec-register-model-reach-AckedThenCrashed-v1.yaml",
        "spec-register-model-reach-AckedThenCrashed.yaml",
        "translation.json",
    ]
    header = (out / "spec-register-model-SyncBeforeAck-FALSE.yaml").read_text().splitlines()[:3]
    assert header[1:] == [
        "# register bundle (model.cfg: AckedDurable, 4 states).",
        "# Found by --mutate (model.cfg with SyncBeforeAck = FALSE).",
    ]
    report = json.loads((out / "translation.json").read_text())
    assert (report["kind"], report["tlc_timeout_s"], report["max_scenarios"]) == ("spec-search", 9.0, 50)
    assert len(report["translations"]) == 2 and str(bundle) not in json.dumps(report)
    # Known-bad: a search with a control, no source at all, a bundle without targets, and a TLC that fails.
    assert _cli("translate", "register", "--control", "ack-before-save", "--mutate", "--out", out) == 2
    assert "leave out --control" in capsys.readouterr().err
    assert _cli("translate", "register", "--out", out) == 2
    assert "one of --control, --trace, --reach, or --mutate" in capsys.readouterr().err
    assert _cli("translate", str(BUNDLE), "--reach", "all", "--out", out) == 2
    assert "names no targets to reach" in capsys.readouterr().err
    _FakeTlc(monkeypatch, SyncBeforeAck_FALSE=TLC_FAILED)
    assert _cli("translate", "register", "--mutate", "--out", out) == 2
    assert (
        "[ERROR         ] model.cfg with SyncBeforeAck=FALSE: Error: Parsing or semantic analysis failed."
        in capsys.readouterr().out
    )
    # Translating one control is as it was: one translation, no search report.
    _FakeTlc(monkeypatch)
    out = tmp_path / "control"
    assert (
        _cli("translate", "register", "--trace", COUNTEREXAMPLE, "--config", CONTROL, "--out", out, "--variants", 1)
        == 0
    )
    assert json.loads((out / "translation.json").read_text())["kind"] == "spec-translation"


@needs_tlc
@pytest.mark.slow
def test_translate_searches_the_register_with_tlc(tmp_path, monkeypatch):
    bundle = load_bundle(str(_searchable(tmp_path, monkeypatch)))
    report = translate.search_model(
        bundle,
        _tools(),
        tlc_timeout_s=120.0,
        max_scenarios=50,
        reach=["AckedThenCrashed"],
        mutate=True,
        compile_scenario=bundle_service(bundle).compile,
    )
    reached = report["targets"]["AckedThenCrashed"]["model.cfg"]
    assert (reached["status"], reached["actions"]) == ("reached", ["Init", "Write", "Save", "Ack", "Crash"]), reached
    assert report["targets"]["AckedThenCrashed"]["other.cfg"]["status"] == "duplicate"
    lost = report["mutations"]["model.cfg"]["SyncBeforeAck=FALSE"]
    assert (lost["status"], lost["violated"], lost["actions"]) == (
        "violated",
        "AckedDurable",
        ["Init", "Write", "Ack", "Crash"],
    ), lost
    assert report["mutations"]["model.cfg"]["SyncBeforeAck=FALSE,SaveOnAck=FALSE"]["status"] == "duplicate"
    assert report["mutations"]["other.cfg"]["SyncBeforeAck=FALSE"]["status"] == "duplicate"
    assert [item["scenarios"][0]["experiment"]["inputs"] for item in report["translations"]] == [
        {"appendonly": "yes"},
        {"appendonly": "no"},
    ]
    assert len(report["scenarios"]) == 4 and report["unreachable"] == [] and report["held"] == []
    # The election's target needs best-effort saves, which no safe config has: unreachable, in a few seconds.
    election = translate.search_model(
        load_bundle("cluster-election"), _tools(), tlc_timeout_s=120.0, max_scenarios=50, reach=["VoteLostInCrash"]
    )
    assert election["unreachable"] == ["VoteLostInCrash"] and election["translations"] == []


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
    for name in ("good", "corrupt", "hung", "empty", "truncated", "newer-report", "newer-trace"):
        trace(runs / name)
    (runs / "good" / "report.json").write_text(json.dumps({"status": "PASS", "outcome": {"plan": {}}}))
    (runs / "corrupt" / "report.json").write_text('{"status": "FAIL", "outcome": ')
    # hung: killed at its deadline, it left only plan.json and its trace.
    shutil.rmtree(runs / "empty" / "components")
    with open(runs / "truncated" / "components" / "trace" / "trace.jsonl", "a") as handle:
        handle.write('{"kind": "event", "t": ')
    # Another fuzzer's format: a report, and a trace whose header says so.
    newer_report = runs / "newer-report" / "report.json"
    newer_report.write_text(json.dumps({"format_version": 2, "status": "PASS", "outcome": {"plan": {}}}))
    newer_trace = runs / "newer-trace" / "components" / "trace" / "trace.jsonl"
    header, *rest = newer_trace.read_text().splitlines()
    newer_trace.write_text("\n".join([json.dumps({**json.loads(header), "format_version": 2}), *rest]) + "\n")

    collected = {Path(run.source).relative_to(runs).parts[0]: run for run in collect_runs([str(tmp_path / "batch")])}
    assert sorted(collected) == ["corrupt", "empty", "good", "hung", "newer-report", "newer-trace", "truncated"]
    assert (collected["good"].problem, collected["hung"].problem) == (None, None)
    assert collected["hung"].meta["scenario_id"] == "hung"
    assert "report.json is not a readable run report" in collected["corrupt"].problem
    assert "no report.json and no trace" in collected["empty"].problem
    assert "trace.jsonl" in collected["truncated"].problem
    version = f"has format_version 2, which an older or newer valkey-fuzzer wrote; this one ({__version__}) reads"
    assert collected["newer-report"].problem == f"{newer_report} {version} format_version 1"
    assert collected["newer-trace"].problem == f"{newer_trace}: {newer_trace} {version} format_version 1"
    result = check_run(load_bundle(str(BUNDLE)), collected["corrupt"], TlaTools(jar="unused"))
    assert (result["status"], result["error"], result["states"]) == ("error", collected["corrupt"].problem, 0)
    # A trace named on its own is refused outright.
    with pytest.raises(ValueError, match="has format_version 2"):
        collect_runs([str(newer_trace)])


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


def test_a_config_constant_is_unknown_while_a_nodes_setting_is_and_no_known_one_differs(tmp_path):
    # One derivation serves spec conform (the trace header's configs) and the election oracle (live CONFIG GET):
    # a node whose setting is missing may be the one that differs, so TRUE needs every node's; FALSE needs one.
    mapping = load_bundle("cluster-election").mapping
    sync = {"cluster-config-save-behavior": "sync"}
    values, unknown = derive_constants(mapping, {"valkey-0": sync, "valkey-1": sync}, {})
    assert values == {"PersistBeforeAck": "TRUE", "FlushBeforeSave": "FALSE"}
    # Without observed states or the run's records, the bounds are unknown, never at_least.
    assert set(unknown) == {"MaxEpoch", "MaxCrashes", "Quorum"}
    best_effort = {"cluster-config-save-behavior": "best-effort"}
    values, unknown = derive_constants(mapping, {"valkey-0": sync, "valkey-1": {}}, {})
    assert "PersistBeforeAck" not in values
    assert unknown["PersistBeforeAck"] == "needs 'cluster-config-save-behavior', and the setting of valkey-1 is unknown"
    assert derive_constants(mapping, {"valkey-0": best_effort, "valkey-1": {}}, {})[0]["PersistBeforeAck"] == "FALSE"
    assert "the setting of every node is unknown" in derive_constants(mapping, {}, {})[1]["PersistBeforeAck"]
    # With the states and the records' field values, the bounds are the largest value seen, at least at_least.
    epochs = {"currentEpoch": {"a": 3}, "lastVote": {"a": 0}, "diskEpoch": {"a": 1}, "electionEpoch": {"a": 0}}
    states = [{**epochs, "crashes": 0}]
    values, unknown = derive_constants(mapping, {}, {}, states=states, field_values={"Quorum": [2, 3]})
    assert (values["MaxEpoch"], values["MaxCrashes"], values["Quorum"]) == ("3", "1", "3")
    assert set(unknown) == {"PersistBeforeAck"}
    # An input the run does not record is unknown too.
    register = load_bundle(str(BUNDLE)).mapping
    _, unknown = derive_constants(register, {}, {})
    assert unknown["SyncBeforeAck"] == "needs input 'appendonly', which the run does not record"
    assert derive_constants(register, {}, {"appendonly": "yes"})[0]["SyncBeforeAck"] == "TRUE"
    # spec conform, which judges the run under the constants, stops on an unknown one and names the node.
    nodes = {n: {"node_id": ID[n], "config": {} if n == "valkey-1" else sync} for n in ID}
    records = [{"kind": "header", "scenario_id": "unit", "seed": 1, "nodes": nodes}, *ELECTION_OBSERVATIONS]
    path = tmp_path / "trace.jsonl"
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n")
    observed = ObservedRun(mapping, collect_runs([str(path)])[0])
    observed.replay()
    with pytest.raises(TlaError, match=r"constant PersistBeforeAck needs .* the setting of valkey-1 is unknown"):
        observed.constants()


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


def _with_quorum(events, quorum):
    """``events`` with every ACK logging ``quorum``, as Valkey 9.2+ does (``(ACKs N, quorum Q)``)."""

    return [
        (name, {**payload, "quorum": quorum} if name == "cluster.vote.acked" else payload) for name, payload in events
    ]


def test_conform_takes_the_quorum_from_the_ack_lines(tmp_path):
    # Valkey's quorum is of the slot-owning primaries, as the candidate's ACK line logs it: the constant is the
    # largest any ACK of the run logs, and 1 (at_least) in a run whose candidate logged none.
    assert _election_run(tmp_path, _with_quorum(ELECTION_EVENTS, 2)).constants()["Quorum"] == "2"
    mixed = [*_with_quorum(ELECTION_EVENTS[:5], 3), *_with_quorum(ELECTION_EVENTS[5:], 2)]
    assert _election_run(tmp_path, mixed).constants()["Quorum"] == "3"
    assert _election_run(tmp_path, ELECTION_EVENTS).constants()["Quorum"] == "1"


@pytest.mark.parametrize(
    ("spec", "message"),
    [
        ("{field: {evidence: loaded, name: keys}, at_least: 1}", None),
        ("{field: {evidence: nosuch, name: keys}}", "MaxCrashes.field must name an evidence entry and one of its fields"),
        ("{field: {evidence: loaded, name: lines}}", "MaxCrashes.field must name an evidence entry and one of its fields"),
        ("{field: {evidence: loaded}}", "field is missing required field 'name'"),
        ("{field: {evidence: loaded, name: keys}, max_of: [crashes]}", "needs exactly one of value, config, input, max_of, field"),
    ],
)  # fmt: skip
def test_a_mapping_constant_from_a_records_field_names_an_int_field_of_one_evidence(tmp_path, spec, message):
    bundle = _copy(tmp_path)
    _edit(bundle / "mapping.yaml", "MaxCrashes: {max_of: [crashes], at_least: 1}", f"MaxCrashes: {spec}")
    if message is None:
        assert load_spec_mapping(str(bundle)).constants["MaxCrashes"]["field"] == {"evidence": "loaded", "name": "keys"}
        return
    with pytest.raises(SpecBundleError, match=re.escape(message)):
        load_spec_mapping(str(bundle))
    _edit(
        bundle / "mapping.yaml",
        "fields: {keys: int}\n    updates:\n      - {set: mem",
        "fields: {keys: str}\n    updates:\n      - {set: mem",
    )
    _edit(bundle / "mapping.yaml", f"MaxCrashes: {spec}", "MaxCrashes: {field: {evidence: loaded, name: keys}}")
    with pytest.raises(SpecBundleError, match=r"MaxCrashes\.field must name an int field"):
        load_spec_mapping(str(bundle))


# Three shards: a second election after the first promotion, as the shipped experiments run it. The primary of
# shard 0 (valkey-0) fails and valkey-1 wins epoch 7 with the votes of valkey-2 and valkey-4; then valkey-2 fails
# and valkey-3 wins epoch 8 with the votes of valkey-4 and valkey-1, now a primary, so four nodes ever voted
# while Valkey's quorum is 2 of the 3 slot-owning primaries.
TWO_ELECTIONS_IDS = {f"valkey-{index}": str(index) * 40 for index in range(6)}
TWO_ELECTIONS_OBSERVATIONS = [
    {"kind": "observation", "t": 0.1, "round": 1, "node": f"valkey-{index}", "role": "primary" if index % 2 == 0 else "replica",
     "slots": [[index // 2 * 5000, index // 2 * 5000 + 4999]] if index % 2 == 0 else [], "current_epoch": 6,
     "disk": {"currentEpoch": 6, "lastVoteEpoch": 0}}
    for index in range(6)
]  # fmt: skip
TWO_ELECTIONS = [
    ("cluster.election.started", {"instance_id": "valkey-1", "epoch": 7}),
    ("cluster.vote.granted", {"instance_id": "valkey-2", "candidate": TWO_ELECTIONS_IDS["valkey-1"], "epoch": 7}),
    ("cluster.vote.granted", {"instance_id": "valkey-4", "candidate": TWO_ELECTIONS_IDS["valkey-1"], "epoch": 7}),
    ("cluster.vote.acked", {"instance_id": "valkey-1", "voter": TWO_ELECTIONS_IDS["valkey-2"], "epoch": 7, "acks": 1}),
    ("cluster.vote.acked", {"instance_id": "valkey-1", "voter": TWO_ELECTIONS_IDS["valkey-4"], "epoch": 7, "acks": 2}),
    ("cluster.election.won", {"instance_id": "valkey-1"}),
    ("cluster.election.started", {"instance_id": "valkey-3", "epoch": 8}),
    ("cluster.vote.granted", {"instance_id": "valkey-4", "candidate": TWO_ELECTIONS_IDS["valkey-3"], "epoch": 8}),
    ("cluster.vote.granted", {"instance_id": "valkey-1", "candidate": TWO_ELECTIONS_IDS["valkey-3"], "epoch": 8}),
    ("cluster.vote.acked", {"instance_id": "valkey-3", "voter": TWO_ELECTIONS_IDS["valkey-4"], "epoch": 8, "acks": 1}),
    ("cluster.vote.acked", {"instance_id": "valkey-3", "voter": TWO_ELECTIONS_IDS["valkey-1"], "epoch": 8, "acks": 2}),
    ("cluster.election.won", {"instance_id": "valkey-3"}),
]  # fmt: skip


def _two_elections_trace(tmp_path, quorum, name):
    nodes = {
        n: {"node_id": TWO_ELECTIONS_IDS[n], "config": {"cluster-config-save-behavior": "sync"}}
        for n in TWO_ELECTIONS_IDS
    }
    records = [{"kind": "header", "scenario_id": name, "seed": 1, "nodes": nodes}, *TWO_ELECTIONS_OBSERVATIONS]
    for index, (event, payload) in enumerate(_with_quorum(TWO_ELECTIONS, quorum)):
        records.append(
            {"kind": "event", "t": 1 + index / 10, "sequence": 10 + index, "name": event, "payload": payload}
        )
    path = tmp_path / f"{name}.jsonl"
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n")
    return str(path)


@needs_tlc
@pytest.mark.slow
def test_conform_holds_a_second_election_won_with_valkeys_quorum(tmp_path):
    # A replica promoted by the first election votes in the second, so the mapping's Primaries has more nodes
    # than vote in one election; the model's Win needs Quorum votes, the ACK line's count, not a majority of them.
    bundle = load_bundle("cluster-election")
    result = check_run(bundle, collect_runs([_two_elections_trace(tmp_path, 2, "two")])[0], _tools())
    assert sorted(result["processes"]["Primaries"]) == ["valkey-0", "valkey-1", "valkey-2", "valkey-4"]
    assert result["constants"]["Quorum"] == "2" and result["actions"]["Win"] == 2
    assert result["status"] == "pass" and result["transitions"]["holds"], json.dumps(result, indent=1)
    # Known-bad: had Valkey needed three votes, the second win with two has no model step, at its won line.
    result = check_run(bundle, collect_runs([_two_elections_trace(tmp_path, 3, "three")])[0], _tools())
    violation = result["transitions"]["first_violation"]
    assert result["status"] == "fail" and result["constants"]["Quorum"] == "3"
    assert (violation["from"]["why"], violation["to"]["why"], violation["event"]) == ("ReceiveVote", "Win", "won")


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


@needs_tlc
@pytest.mark.slow
def test_check_runs_the_configs_of_a_subdirectory_that_raise_a_bound(tmp_path, monkeypatch):
    # nightly/ holds the bounds PR CI cannot afford: --configs nightly checks them instead of the bundle's own.
    bundle = _copy(tmp_path, monkeypatch)
    nightly = bundle / "nightly"
    nightly.mkdir()
    (nightly / "model.cfg").write_text((bundle / "model.cfg").read_text().replace("MaxValue = 3", "MaxValue = 4"))
    output = tmp_path / "assurance.json"
    assert _cli("check", bundle, "--tla2tools", JAR, "--workers", 1, "--configs", "nightly", "--output", output) == 0
    report = json.loads(output.read_text())
    assert report["status"] == "pass" and report["configs"] == "nightly", json.dumps(report["gates"], indent=1)
    assert list(report["gates"]["model_checked"]["configs"]) == ["nightly/model.cfg"]
    assert report["gates"]["model_checked"]["configs"]["nightly/model.cfg"]["distinct_states"] > 40  # model.cfg: 40
    # The claims' integer bounds may only be raised there; a lowered or changed one is what PR CI checks, not this.
    (nightly / "model.cfg").write_text((bundle / "model.cfg").read_text().replace("MaxValue = 3", "MaxValue = 2"))
    report = check_bundle(str(bundle), _tools(), configs="nightly")
    assert report["status"] == "fail" and report["gates"]["claims_checked"]["problems"] == [
        "bound MaxValue = 3, but nightly/model.cfg sets '2'; a config outside the bundle's own may only raise an "
        "integer bound"
    ]
    # The bundle's own configs are still held to the exact bound, and a bundle without the directory has none.
    _edit(bundle / "model.cfg", "MaxValue = 3", "MaxValue = 4")
    problems = check_bundle(str(bundle), _tools())["gates"]["claims_checked"]["problems"]
    assert problems == ["bound MaxValue = 3, but model.cfg sets '4'"]
    report = check_bundle(str(bundle), _tools(), configs="weekly")
    assert report["status"] == "fail" and report["gates"]["bundle"]["error"].endswith(
        "has no .cfg: a bundle without a weekly/ directory has only its own configs"
    )


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


@needs_tlc
@pytest.mark.slow
def test_a_run_checks_itself_against_its_bundle_with_tlc(tmp_path, monkeypatch):
    # The same check as `spec conform register RUN`, run as the report is written: TLC on the run's trace, under the
    # service's conform_tlc_timeout_s, its result in the report and, for the approved claims, in the verdict.
    bundle = approved_register(tmp_path, monkeypatch)
    service = register_service(tmp_path, monkeypatch, bundle, lost=True)
    prepared = service.prepare_run(scenario_file(tmp_path))
    record = service.execute(prepared.plan, scenario=prepared.scenario, report_dir=str(tmp_path / "run"))
    entry = record.report["conform"]["register"]
    assert record.outcome.status.value == "FAIL" and entry["status"] == "fail", json.dumps(entry)
    assert entry["violated"] == ["AckedDurable"] and entry["first_violated"]["state"] == 5
    assert entry["transitions"]["holds"] is True and entry["tlc_seconds"] > 0
    assert [item.component_id for item in record.outcome.component_results] == ["spec-conform:register"]
    # A check TLC cannot finish in the time the service allows is an error that names the option, so the run is.
    service = RunService(experiment_directories=[str(bundle / "experiments")], artifact_dir=str(tmp_path / "reports"),
                         conform_tlc_timeout_s=0.001)  # fmt: skip
    record = service.execute(prepared.plan, scenario=prepared.scenario, report_dir=str(tmp_path / "slow"))
    entry = record.report["conform"]["register"]
    assert record.outcome.status.value == "ERROR" and entry["status"] == "error" and entry["timed_out"]
    assert entry["error"].endswith("(conform_tlc_timeout_s = 0.001, a batch option)")
