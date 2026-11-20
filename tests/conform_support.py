"""What the tests of a run's own spec conform check share: an approved copy of the register bundle
(``tests/fixtures/spec/register``), canned runs of its experiment that leave a register trace in their run folder,
and a TLC that reads the trace module it would check instead of running, so no java is needed."""

import json
import re
import shutil
from pathlib import Path

import yaml

from valkey_fuzzer.results import OutcomeStatus, RunOutcome
from valkey_fuzzer.runner import Runner
from valkey_fuzzer.service import RunService
from valkey_fuzzer.spec import conform, tla
from valkey_fuzzer.spec.bundle import TRACE_FILE

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "spec"
REGISTER = FIXTURES / "register"
# A register scenario under the claim: appendonly yes, so an acknowledged write lost in the crash violates AckedDurable.
SCENARIO = {
    "scenario_id": "register-lost",
    "seed": 1,
    "experiment": {"id": "toy.register", "inputs": {"appendonly": "yes", "writes": 4}},
    "deployment": {"mode": "standalone", "base_port": 7940},
}


def approved_register(tmp_path, monkeypatch, *, approved=True, shrinkable=False) -> Path:
    """A copy of the register bundle, the one ``spec: register`` finds; with ``approved``, its claims are approved,
    so a violation decides a run; with ``shrinkable``, its experiment's ``writes`` input is shrinkable, so
    ``minimize`` has a reduction to try."""

    target = tmp_path / "spec" / "register"
    shutil.copytree(REGISTER, target)
    if approved:
        claims = target / "claims.yaml"
        claims.write_text(claims.read_text().replace("status: draft", "status: approved\napproved_by: tests"))
    if shrinkable:
        experiment = target / "experiments" / "toy.register.yaml"
        text = experiment.read_text()
        assert "maximum: 10000, default: 100}" in text
        experiment.write_text(
            text.replace("maximum: 10000, default: 100}", "maximum: 10000, default: 100, shrinkable: true}")
        )
    monkeypatch.setenv("VALKEY_FUZZER_SPEC_PATH", str(tmp_path / "spec"))
    return target


def scenario_file(tmp_path, **changes) -> str:
    path = tmp_path / "register.yaml"
    path.write_text(yaml.safe_dump({**SCENARIO, **changes}, sort_keys=False))
    return str(path)


def write_register_trace(run_dir, plan, *, lost: bool) -> None:
    """The trace observer's ``trace.jsonl`` of a register run of ``plan``: its ``writes`` keys acknowledged at once,
    the crash, and the reload's key count: all of them, or (``lost``) none, an acknowledged write lost under
    appendonly yes, which AckedDurable forbids."""

    keys = plan.inputs["writes"]
    events = [
        ("writes.seeded", "writes", {"keys": keys}),
        ("crash.injected", "crash", {"signal": "SIGKILL", "applied": True, "target": "valkey-0"}),
        ("crash.recovered", "crash", {}),
        ("register.loaded", "reload-log", {"keys": 0 if lost else keys, "instance_id": "valkey-0"}),
    ]
    header = {"kind": "header", "scenario_id": plan.scenario_id, "seed": plan.seed, "experiment_id": plan.experiment_id,
              "inputs": dict(plan.inputs), "nodes": {}}  # fmt: skip
    records = [header] + [
        {"kind": "event", "t": float(t), "name": e, "source": s, "payload": p} for t, (e, s, p) in enumerate(events, 1)
    ]
    path = Path(run_dir, "components", "trace", TRACE_FILE)
    path.parent.mkdir(parents=True)
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n")


def register_service(tmp_path, monkeypatch, bundle: Path, *, lost) -> RunService:
    """A run service with the register bundle's experiment whose runs are canned: each passes on its own and leaves
    the trace of :func:`write_register_trace`, the acknowledged write ``lost`` (a bool, or a function of the plan)."""

    def run(self, plan, *, deadline=None, run_dir=None):
        write_register_trace(run_dir, plan, lost=lost(plan) if callable(lost) else lost)
        return RunOutcome(plan, OutcomeStatus.PASS, "experiment passed", started_at=1.0, ended_at=2.0)

    monkeypatch.setattr(Runner, "run", run)
    # Canned runs start no servers, so the scenario keeps its port whatever this host is using.
    monkeypatch.setattr("valkey_fuzzer.deployment.local.ports.busy_planned_ports", lambda deployment: [])
    return RunService(experiment_directories=[str(bundle / "experiments")], artifact_dir=str(tmp_path / "reports"))


def stub_tlc(monkeypatch) -> None:
    """TLC replaced by a reading of the trace module it would check: AckedDurable is violated in every observed
    state whose ``acked`` exceeds ``mem``, and the model explains every step; the tools are found without java."""

    def tlc(tools, module, config, files):
        text = files[module]
        states = re.findall(r"^\s*\[mem \|-> (\d+), disk \|-> \d+, acked \|-> (\d+), crashes \|-> \d+\]", text, re.M)
        violated = [index for index, (mem, acked) in enumerate(states, 1) if int(acked) > int(mem)]
        steps = len(re.findall(r'^\s*\[kind \|-> "', text, re.M))
        output = (
            f'<<"spec-conform", "violated", "AckedDurable", {{{", ".join(map(str, violated))}}}>>\n'
            + "".join(f'<<"spec-conform", "matched", {j}>>\n' for j in range(2, steps + 1))
            + f"{steps} states generated, {steps} distinct states found, 0 states left on queue.\n"
        )
        run = tla.ToolRun(command=["java", "tlc2.TLC"], exit_code=0, output=output, seconds=0.1)
        return tla.read_tlc_output(output, 0), run

    monkeypatch.setattr(conform, "tlc", tlc)
    monkeypatch.setattr(tla.TlaTools, "locate", lambda jar, workers="auto": tla.TlaTools(jar="unused"))
