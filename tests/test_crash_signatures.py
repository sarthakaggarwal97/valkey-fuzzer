"""One server crash is one failure signature: the liveness oracle names the crash cause, and batches and the
minimizer group runs by it whatever experiment hit it and whatever failed after it."""

import time
from pathlib import Path
from types import SimpleNamespace

import pytest
import valkey
import yaml

from valkey_fuzzer.batch import signature_key, signature_label
from valkey_fuzzer.components import actions, capabilities
from valkey_fuzzer.components.oracles import LivenessOracle
from valkey_fuzzer.minimize import failure_signature, same_failure
from valkey_fuzzer.plan import ComponentSpec, DeploymentProviderIdentity, EvidenceRequirement, ExecutionPlan
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.results import ComponentResult, ComponentStatus, Evidence, OutcomeStatus, RunOutcome
from valkey_fuzzer.testing import FakeContext

from .live_support import BINARY, explain, free_base_port
from .valkey_experiments_support import fake_context, resolved, valkey_service

CRASHES = Path(__file__).resolve().parent / "fixtures" / "crashes"
ASSERTION = "bgiteration.c:2699 'wasAdded' is not true"
SHORT_READ = "Short read or OOM loading DB. Unrecoverable error, aborting now."
DUAL_CHANNEL = "dual-channel-short-read-valkey-1.log"


class _Client:
    def __enter__(self):
        return SimpleNamespace(ping=lambda: True, info=lambda section: {"loading": 0})

    def __exit__(self, *exc_info):
        pass


def _liveness(node, log, alive=False, evidence=()):
    spec = resolved(ComponentSpec(role=ComponentRole.ORACLE, use="valkey.oracle.liveness", id="liveness"))
    # One instance, ``node``, whose process is gone (unless ``alive``), whose log is ``log``, and that answers.
    handle = SimpleNamespace(
        instances=(SimpleNamespace(instance_id=node),),
        is_alive=lambda _: alive,
        log_file=lambda _: str(CRASHES / log),
        new_client=lambda _, timeout=5.0: _Client(),
        process_output_file=lambda _: None,
    )
    return LivenessOracle().evaluate(fake_context(handle), spec, evidence)


_DOWN = Evidence("valkey.instances.expected-down", "chaos", "action.completed", 1.0, {"instances": ["valkey-1"]})
PREFIX = "4242:S 29 Oct 2026 06:07:59.786"
STARTING = f"{PREFIX} * oO0OoO0OoO0Oo Valkey is starting oO0OoO0OoO0Oo"


@pytest.mark.parametrize(
    ("node", "log", "alive", "evidence", "cause", "problem"),
    [
        ("valkey-0", "save-assertion-s100016-valkey-0.log", False, (), ASSERTION, "crash report"),
        # A recursive assertion during the bug report is not the cause.
        ("valkey-2", "save-assertion-s200014-valkey-2.log", False, (), ASSERTION, "crash report"),
        # A signal names the function it hit in, as in valkey-io/valkey#3645's proposed test, so two
        # segfaults elsewhere are two failures.
        (
            "valkey-0",
            "module-pause-segfault-valkey-0.log",
            False,
            (),
            "crashed by signal: 11 in VM_FreeCallReply",
            "crash report",
        ),
        # No assertion, panic or signal: Valkey's fatal log line before it exits.
        ("valkey-1", DUAL_CHANNEL, False, (), SHORT_READ, "fatal exit"),
        # Still running, or stopped on purpose: the last fatal-looking line is not why it is down.
        ("valkey-1", DUAL_CHANNEL, True, (), None, None),
        ("valkey-1", DUAL_CHANNEL, False, (_DOWN,), None, None),
    ],
    ids=["assertion", "recursive-assertion", "segfault", "fatal-exit", "alive", "stopped-on-purpose"],
)
def test_the_liveness_oracle_names_what_crashed_an_instance(node, log, alive, evidence, cause, problem):
    result = _liveness(node, log, alive, evidence)
    assert result.diagnostics.get("crashes", {}).get(node) == cause
    assert (
        (result.status is ComponentStatus.PASS) if cause is None else f"{node}: {problem} ({cause})" in result.summary
    )


@pytest.mark.parametrize(
    ("lines", "cause"),
    [
        # Loading an RDB from disk: the read error, the RDB check, then the exit.
        ([SHORT_READ, "Internal error in RDB reading offset 0", "Terminating server after rdb file reading failure."],
         "Terminating server after rdb file reading failure."),
        # A diskless load that carried on after the short read, or a later life, did not die of it.
        ([SHORT_READ, "Failure loading rdb format from socket, assuming connection error, resuming operation."], None),
        ([SHORT_READ, STARTING, "* Ready to accept connections tcp"], None),
        # A notice that mentions an exit is not a fatal exit.
        (["* Saving the final RDB snapshot before exiting."], None),
        ([STARTING, "Failed listening on port 21003 (cluster), aborting."], "Failed listening on port 21003 (cluster), aborting."),
    ],
    ids=["last-fatal-line", "resumed", "restarted", "notice", "after-restart"],
)  # fmt: skip
def test_a_fatal_exit_cause_is_the_last_fatal_line_of_the_last_life(tmp_path, lines, cause):
    path = tmp_path / "valkey-1.log"
    # A line is a warning ("#") unless it is a notice ("*") or already has its prefix.
    logged = (
        line if line.startswith(PREFIX) else f"{PREFIX} {line}" if line.startswith("*") else f"{PREFIX} # {line}"
        for line in lines
    )
    path.write_text("".join(f"{line}\n" for line in logged))
    assert _liveness("valkey-1", str(path)).diagnostics.get("crashes", {}).get("valkey-1") == cause


def _asan_report(function):
    return (
        "==16108==ERROR: AddressSanitizer: heap-use-after-free on address 0x7a36431e8790 at pc 0x603d49ee9311\n"
        "READ of size 4 at 0x7a36431e8790 thread T0\n"
        f"    #0 0x603d49ee9310 in {function} /src/file.c:511:15\n"
        f"SUMMARY: AddressSanitizer: heap-use-after-free /src/file.c:511:15 in {function}\n"
    )


def test_two_sanitizer_findings_in_different_functions_are_two_causes(tmp_path):
    # "AddressSanitizer: heap-use-after-free" alone made valkey-search's FT.HYBRID use-after-free and the
    # script debugger's one signature, so the walk's second bug landed on the first's issue (fuzzer #61).
    causes = {}
    for node, function in (("valkey-0", "connBlock"), ("valkey-1", "FindRecord")):
        path = tmp_path / f"{node}.log"
        path.write_text(_asan_report(function))
        causes[function] = _liveness(node, str(path)).diagnostics["crashes"][node]
    assert causes["connBlock"] == "AddressSanitizer: heap-use-after-free in connBlock"
    assert causes["FindRecord"] == "AddressSanitizer: heap-use-after-free in FindRecord"


def _signature(experiment, liveness, failing=None):
    """The failure signature of a run of ``experiment`` with this ``liveness`` result and ``failing`` components."""

    plan = ExecutionPlan(
        scenario_id="s", experiment_id=experiment, seed=1, deployment_mode="standalone",
        deployment_provider=DeploymentProviderIdentity("valkey-fuzzer.deployment.local"),
        components=(ComponentSpec(role=ComponentRole.ORACLE, use="valkey.oracle.liveness", id="liveness"),),
        required_evidence=(EvidenceRequirement("valkey.rdb.reload-verified", source="liveness"),),
        deployment={"mode": "standalone"},
    )  # fmt: skip
    failed = (
        ComponentResult(component_id=name, status=ComponentStatus.FAIL, summary=text)
        for name, text in (failing or {}).items()
    )
    outcome = RunOutcome(plan=plan, status=OutcomeStatus.FAIL, summary="FAIL", component_results=(liveness, *failed))
    return experiment, failure_signature(outcome, ["valkey.rdb.reload-verified"])


def test_one_crash_is_one_signature_whatever_experiment_hit_it_and_whatever_failed_after_it():
    # The soak split this crash into six signatures, by experiment and by what else failed after it.
    crashed = [
        _signature("valkey.snapshot-equivalence", _liveness("valkey-0", "save-assertion-s100016-valkey-0.log"),
                   {"bgsave": "valkey-0: expected ok, got unavailable", "data": "cannot read dataset: Error 111"}),
        _signature("valkey.aof-durability", _liveness("valkey-2", "save-assertion-s200014-valkey-2.log"),
                   {"data": "replicas did not converge: valkey-2"}),
        _signature("valkey.replication-chaos", _liveness("valkey-0", "save-assertion-s300008-valkey-0.log")),
    ]  # fmt: skip
    assert len({signature_key(*item) for item in crashed}) == 1
    assert "bgiteration.c:N 'wasAdded' is not true" in signature_label(*crashed[0])
    assert same_failure(crashed[0][1], crashed[1][1]), "the minimizer keeps a reduction that crashes the same way"
    fatal = [_signature(experiment, _liveness(node, DUAL_CHANNEL)) for experiment, node in
             (("valkey.repl-dual-channel", "valkey-1"), ("valkey.replication-full-resync", "valkey-2"))]  # fmt: skip
    assert len({signature_key(*item) for item in fatal}) == 1
    assert signature_label(*fatal[0]) == f"FAIL | crash | {SHORT_READ}"

    # A re-run that failed without a crash, or crashed elsewhere, is another failure.
    experiment, reference = crashed[2]
    plain = _signature(
        experiment,
        ComponentResult(component_id="snapshot", status=ComponentStatus.FAIL, summary="the RDB is plain"),
    )[1]
    elsewhere = _signature(
        experiment,
        ComponentResult(
            component_id="liveness",
            status=ComponentStatus.FAIL,
            summary="valkey-0: crash report",
            diagnostics={
                "crashes": {"valkey-0": "object.c:414 'objectGetEncoding(o) == OBJ_ENCODING_EMBSTR' is not true"}
            },
        ),
    )[1]
    assert len({signature_key(experiment, item) for item in (reference, plain, elsewhere)}) == 3
    assert not same_failure(reference, plain) and not same_failure(plain, reference)


# UBSan's report of fuzzer issue #148 at startup (a build with -fno-sanitize-recover exits on it), on stderr.
OVERFLOW = (
    "cluster_legacy.c:1136:20: runtime error: signed integer overflow: 9223372036854775807 * 1000 cannot be "
    "represented in type 'long long int'"
)
OVERFLOW_CAUSE = (
    "cluster_legacy.c:N:N: runtime error: signed integer overflow: N * N cannot be represented in type 'long long int'"
)
# A server that cannot start, as Valkey logs it before exit(1): an error of the host, not of the server.
PORT_IN_USE = "# Failed listening on port 40000 (tcp), aborting."


def _fake_server(path, real_start, *, real=None):
    """A valkey-server at ``path``: a real start (one with ``--logfile``) runs the shell ``real_start``; a config
    probe (``--port -1``) and ``--version`` go to the ``real`` binary, or are answered as Valkey answers them."""

    if real:
        other = f'exec {real} "$@"'
    else:
        other = r'''case " $* " in *" --port -1 "*) echo ">>> 'port \"-1\"'"; exit 1;; esac; echo "v=9.1.0 sha=0:0"'''
    path.write_text(f'#!/bin/sh\ncase " $* " in *" --logfile "*) {real_start};; esac\n{other}\n')
    path.chmod(0o755)
    return str(path)


_RESTART = {"use": "valkey.action.restart-instance", "id": "restart", "config": {"instance": "valkey-0"}}


def _run_crash_experiment(tmp_path, binary, first=_RESTART):
    """A run of an experiment of the component ``first`` (an action that restarts valkey-0) and then the liveness
    oracle, on a standalone deployment of ``binary``."""

    directory = tmp_path / "experiments"
    directory.mkdir(parents=True)
    experiment = {
        "id": "tests.crash-before-ready",
        "mode": "smoke",
        "deployment": {"modes": ["standalone"]},
        "components": [first, {"use": "valkey.oracle.liveness", "id": "liveness"}],
    }
    (directory / "tests.crash-before-ready.yaml").write_text(yaml.safe_dump(experiment))
    runtime = valkey_service(experiment_directories=(str(directory),), artifact_dir=str(tmp_path / "artifacts"))
    scenario = {
        "scenario_id": "crash-before-ready",
        "seed": 7,
        "experiment": {"id": "tests.crash-before-ready", "inputs": {}},
        "deployment": {
            "mode": "standalone",
            "base_port": free_base_port(),
            "valkey_binary": binary,
            "base_data_dir": str(tmp_path / "data"),
        },
    }
    return runtime.runner.run(runtime.compile(scenario), deadline=time.monotonic() + 120)


@pytest.mark.parametrize(
    ("real_start", "crashed"),
    [
        (f'echo "{OVERFLOW}" >&2; exit 1', True),
        # The log file is the argument after --logfile.
        (f'while [ "$1" != --logfile ]; do shift; done; echo "{PORT_IN_USE}" >> "$2"; exit 1', False),
    ],
    ids=["sanitizer-report", "port-in-use"],
)
def test_a_server_that_crashes_while_the_deployment_is_provisioned_fails_the_run_with_its_crash(
    tmp_path, real_start, crashed
):
    # The provision raised "Instance valkey-0 failed to start": the run was an ERROR, every crash at startup one
    # signature with every port in use. Now the liveness oracle's post-mortem finds the crash report.
    outcome = _run_crash_experiment(tmp_path, _fake_server(tmp_path / "valkey-server", real_start))

    assert outcome.diagnostics["error"]["phase"] == "deployment.provision"
    signature = failure_signature(outcome)
    if crashed:
        assert outcome.status is OutcomeStatus.FAIL, explain(outcome)
        (liveness,) = (item for item in outcome.component_results if item.component_id == "liveness")
        assert liveness.crash and liveness.summary == f"valkey-0: crash report ({OVERFLOW})"
        assert signature["crashes"] == [OVERFLOW_CAUSE]
    else:
        assert outcome.status is OutcomeStatus.ERROR, explain(outcome)
        assert not signature["crashes"] and signature["error_type"].endswith(".LocalDeploymentError")


@pytest.mark.slow
@pytest.mark.skipif(not BINARY, reason="needs valkey-server (VALKEY_FUZZER_BINARY or on PATH)")
def test_a_server_that_crashes_when_an_action_restarts_it_fails_the_run_with_its_crash(tmp_path):
    # rolling-upgrade and restart-instance returned ERROR ("failed to restart instance"), and the run never
    # reached its oracles. The server starts for real the first time and reports a crash the second.
    started = tmp_path / "started"
    real_start = f'if [ -e {started} ]; then echo "{OVERFLOW}" >&2; exit 1; fi; touch {started}'
    outcome = _run_crash_experiment(tmp_path, _fake_server(tmp_path / "valkey-server", real_start, real=BINARY))

    assert outcome.status is OutcomeStatus.FAIL, explain(outcome)
    assert outcome.diagnostics["error"]["phase"] == "action.run"
    assert failure_signature(outcome)["crashes"] == [OVERFLOW_CAUSE]


# What a probe server's start leaves: a rejected startup option, as Valkey reports it before exit(1), and a
# startup warning that names an errno ("Invalid argument") before the server crashed.
REJECTED = (
    ">>> 'nonsense yes'\nBad directive or wrong number of arguments\n*** FATAL CONFIG FILE ERROR (Version 9.1.0) ***"
)
EINVAL = "# Failed to set the CPU affinity of the IO threads: Invalid argument"
SOCKET_IN_USE = "# Failed opening Unix socket: bind: Address already in use"


@pytest.mark.parametrize(
    ("real_start", "status", "summary"),
    [
        (f'echo "{OVERFLOW}" >&2; exit 1', "FAIL", f"probe server crashed: {OVERFLOW}"),
        # A crash report stands over a line an unsupported option would print: this one was a SKIP.
        (f'echo "{EINVAL}"; echo "{OVERFLOW}" >&2; exit 1', "FAIL", f"probe server crashed: {OVERFLOW}"),
        (f'printf "{REJECTED}\\n"; exit 1', "SKIP", "build rejects startup configuration"),
        (f'echo "{SOCKET_IN_USE}"; exit 1', "ERROR", "probe server exited"),
        # A server that reported a crash and then hung (a crash handler stuck), and one that only hung.
        (f'echo "{OVERFLOW}" >&2; exec sleep 5', "FAIL", f"probe server crashed: {OVERFLOW}"),
        ("exec sleep 5", "ERROR", "probe server failed: private server did not become ready"),
    ],
    ids=["sanitizer-report", "after-an-errno-warning", "rejected-option", "socket-in-use", "hung-after-a-report",
         "hung"],
)  # fmt: skip
def test_a_probe_server_that_crashes_as_it_starts_is_a_crash_and_one_that_cannot_start_is_not(
    tmp_path, real_start, status, summary
):
    # "probe server exited: ..." was an ERROR whatever the server said, and the crash report was dropped.
    binary = _fake_server(tmp_path / "valkey-server", real_start)
    spec = resolved(ComponentSpec(role=ComponentRole.CAPABILITY, use="valkey.capability.build-probe", id="build"))

    probed, summary_text, facts = capabilities.starts_with(FakeContext(), spec, binary, {"io-threads": 4}, 1)

    assert (probed.value, summary_text[: len(summary)]) == (status, summary)
    assert facts.get("crash") == (OVERFLOW if status == "FAIL" else None)


@pytest.mark.parametrize("crashed", [True, False], ids=["crash-report", "no-report"])
def test_a_probe_server_that_crashes_on_a_probe_command_is_a_crash(monkeypatch, tmp_path, crashed):
    # The probe server started; the probe's INFO broke the connection. With a crash report in the server's output
    # it is the crash; without one the broken connection stays the error it was.
    output = tmp_path / "output.log"

    class Server:
        def __init__(self, binary, arguments, **_):
            self.workdir, self.log_path, self.output_path = str(tmp_path), str(tmp_path / "server.log"), str(output)
            self.process = SimpleNamespace(wait=lambda timeout: 1)
            self.client = SimpleNamespace(info=self.crash)

        def crash(self, section):
            output.write_text(f"{OVERFLOW}\n" if crashed else "")
            raise valkey.exceptions.ConnectionError("Connection closed by server.")

        def start(self):
            return None

        def stop(self):
            pass

    monkeypatch.setattr(capabilities, "PrivateServer", Server)
    binary = _fake_server(tmp_path / "valkey-server", "exit 1")
    spec = resolved(ComponentSpec(role=ComponentRole.CAPABILITY, use="valkey.capability.build-probe", id="build"))

    if not crashed:
        with pytest.raises(valkey.exceptions.ConnectionError):
            capabilities.probe_binary(FakeContext(), spec, binary, spec.config)
        return
    status, summary, facts = capabilities.probe_binary(FakeContext(), spec, binary, spec.config)
    assert (status, summary, facts["crash"]) == (ComponentStatus.FAIL, f"probe server crashed: {OVERFLOW}", OVERFLOW)


_PROBE = {"use": "valkey.capability.build-probe", "id": "build"}


@pytest.mark.parametrize(
    ("real_start", "status"),
    [
        (f'echo "{OVERFLOW}" >&2; exit 1', OutcomeStatus.FAIL),
        (f'printf "{REJECTED}\\n"; exit 1', OutcomeStatus.SKIP_UNSUPPORTED),
        (f'echo "{SOCKET_IN_USE}"; exit 1', OutcomeStatus.ERROR),
    ],
    ids=["sanitizer-report", "rejected-option", "socket-in-use"],
)
def test_a_build_that_crashes_when_the_capability_probe_starts_it_fails_the_run_with_the_liveness_signature(
    tmp_path, real_start, status
):
    # The capability gate's crash was "required capability 'build' failed; ... no trustworthy verdict": an ERROR.
    binary = _fake_server(tmp_path / "valkey-server", real_start)
    outcome = _run_crash_experiment(tmp_path / "probe", binary, _PROBE)

    assert outcome.status is status, explain(outcome)
    (probe,) = (item for item in outcome.component_results if item.component_id == "build")
    assert probe.crash is (status is OutcomeStatus.FAIL)
    if status is not OutcomeStatus.FAIL:
        return
    assert "error" not in outcome.diagnostics, "the run stopped for the finding; nothing failed"
    assert [item.component_id for item in outcome.component_results] == ["build"], "never provisioned"
    assert probe.summary == f"default: probe server crashed: {OVERFLOW}"
    # The same crash in a deployment instance (the liveness post-mortem of a provision it crashed): one signature.
    provisioned = _run_crash_experiment(tmp_path / "provision", binary)
    assert (
        failure_signature(outcome)
        == failure_signature(provisioned)
        != failure_signature(
            _run_crash_experiment(
                tmp_path / "other",
                _fake_server(tmp_path / "other-server", 'echo "x.c:1:1: runtime error: y" >&2; exit 1'),
                _PROBE,
            )
        )
    )
    assert signature_key("tests.probe", failure_signature(outcome)) == signature_key(
        "tests.provision", failure_signature(provisioned)
    )


@pytest.mark.parametrize("crashed", [True, False], ids=["sanitizer-report", "socket-in-use"])
def test_a_compared_build_that_crashes_when_the_commands_action_starts_it_is_a_crash(tmp_path, crashed):
    # The walk's prefetch-zero-to-positive-startup on a UBSan build: the parent's start check exited on fuzzer
    # #150, and the action said "the parent build ... does not start with {...}: the probe could not run".
    real_start = f'echo "{OVERFLOW}" >&2; exit 1' if crashed else f'echo "{SOCKET_IN_USE}"; exit 1'
    parent = _fake_server(tmp_path / "parent-server", real_start)
    spec = resolved(
        ComponentSpec(
            role=ComponentRole.ACTION,
            use="valkey.action.commands",
            id="prefetch",
            config={
                "compare_with": parent,
                "server_config": {"prefetch-batch-max-size": 0},
                "commands": [{"command": ["PING"]}],
            },
        )
    )
    context = FakeContext(spec, deployment={"valkey_binary": _fake_server(tmp_path / "candidate", "exit 1")})

    result = actions.CommandsAction().run(context, spec)

    started = f"the parent build ({parent})"
    if not crashed:
        assert result.status is ComponentStatus.ERROR and not result.crash
        assert result.summary.startswith(started) and result.summary.endswith(": the probe could not run")
        return
    assert result.status is ComponentStatus.FAIL and result.crash, result.summary
    assert result.summary.startswith(f"{started} crashed when started with ")
    assert result.summary.endswith(f": probe server crashed: {OVERFLOW}")
    assert result.diagnostics["crashes"] == {"parent": OVERFLOW}


@pytest.mark.slow
@pytest.mark.skipif(not BINARY, reason="needs valkey-server (VALKEY_FUZZER_BINARY or on PATH)")
def test_a_run_whose_commands_action_finds_its_compared_build_crashing_fails_with_the_crash(tmp_path):
    # The deployment's instances run the real build; the compared build crashes on a private server's start
    # (one on a Unix socket), before any command.
    parent = tmp_path / "parent-server"
    parent.write_text(
        f'#!/bin/sh\ncase " $* " in *" --unixsocket "*) echo "{OVERFLOW}" >&2; exit 1;; esac\nexec {BINARY} "$@"\n'
    )
    parent.chmod(0o755)
    action = {"use": "valkey.action.commands", "id": "prefetch", "config": {"compare_with": str(parent), "commands": [
        {"command": ["SET", "k", "v"], "expect": {"equals": "OK"}}
    ]}}  # fmt: skip

    outcome = _run_crash_experiment(tmp_path, BINARY, action)

    assert outcome.status is OutcomeStatus.FAIL, explain(outcome)
    results = {item.component_id: item for item in outcome.component_results}
    assert results["prefetch"].crash and results["liveness"].status is ComponentStatus.PASS
    assert failure_signature(outcome)["crashes"] == [OVERFLOW_CAUSE]
