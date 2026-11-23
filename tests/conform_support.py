"""What the tests of a run's own spec conform check share: a copy of the register bundle
(``tests/fixtures/spec/register``), canned runs of its experiment that leave a register trace in their run folder,
the trace files the model tests replay through a mapping, and a TLC that reads the trace module it would check
instead of running, so no java is needed."""

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
# The node IDs of a four-node election trace, and its first round: two primaries and two replicas at epoch 8.
ID = {name: name[-1] * 40 for name in ("valkey-0", "valkey-1", "valkey-2", "valkey-3")}
ELECTION_OBSERVATIONS = [
    {"kind": "observation", "t": 0.1, "round": 1, "node": node, "role": role, "slots": slots, "current_epoch": 8,
     "disk": {"currentEpoch": 8, "lastVoteEpoch": 0}}
    for node, role, slots in (("valkey-0", "primary", [[0, 8191]]), ("valkey-3", "primary", [[8192, 16383]]), ("valkey-1", "replica", []), ("valkey-2", "replica", []))
]  # fmt: skip


def register_copy(tmp_path, monkeypatch=None, *, approved=False, shrinkable=False) -> Path:
    """A copy of the register bundle; with ``monkeypatch``, the one ``spec: register`` finds. With ``approved``, its
    claims are approved, so a violation decides a run; with ``shrinkable``, its experiment's ``writes`` input is
    shrinkable, so ``minimize`` has a reduction to try."""

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
    if monkeypatch is not None:
        monkeypatch.setenv("VALKEY_FUZZER_SPEC_PATH", str(tmp_path / "spec"))
    return target


def edit(path, old, new) -> None:
    """``old``, which the file at ``path`` must contain, replaced by ``new``."""

    text = path.read_text()
    assert old in text, (path, old)
    path.write_text(text.replace(old, new))


def scenario_file(tmp_path, **changes) -> str:
    path = tmp_path / "register.yaml"
    path.write_text(yaml.safe_dump({**SCENARIO, **changes}, sort_keys=False))
    return str(path)


def write_trace(path, records, *, nodes=None, scenario_id="unit", seed=1, **fields) -> str:
    """A trace file at ``path``, its directories made: the header (``scenario_id``, ``seed``, ``fields`` and
    ``nodes``), then ``records``, one JSON object per line; the path as a string, as ``collect_runs`` takes it."""

    header = {"kind": "header", "scenario_id": scenario_id, "seed": seed, **fields, "nodes": nodes or {}}
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(record) for record in [header, *records]) + "\n")
    return str(path)


def events(pairs, *, first=0, times=None, **fields):
    """Event records of ``(name, payload)`` pairs, 0.1 s apart from t=1 (or at ``times``) and numbered from
    ``first``, each with ``fields``: the trace the model tests replay."""

    return [
        {"kind": "event", "t": times[index] if times else 1 + index / 10, "sequence": first + index, "name": name,
         **fields, "payload": payload}
        for index, (name, payload) in enumerate(pairs)
    ]  # fmt: skip


def write_register_trace(path, *, keys=1, loaded=1, crash_source="crash", **header) -> str:
    """The trace observer's ``trace.jsonl`` of a register run, at ``path``: ``keys`` writes acknowledged at once, a
    crash (``crash_source``'s), and the reload's key count ``loaded``; ``header`` is the header's other fields
    (the run's ``inputs``, say)."""

    records = [
        ("writes.seeded", "writes", {"keys": keys}),
        ("crash.injected", crash_source, {"signal": "SIGKILL", "applied": True, "target": "valkey-0"}),
        ("crash.recovered", "crash", {}),
        ("register.loaded", "reload-log", {"keys": loaded, "instance_id": "valkey-0"}),
    ]
    records = [
        {"kind": "event", "t": float(t), "name": e, "source": s, "payload": p} for t, (e, s, p) in enumerate(records, 1)
    ]
    return write_trace(path, records, **header)


def register_service(tmp_path, monkeypatch, bundle: Path, *, lost) -> RunService:
    """A run service with the register bundle's experiment whose runs are canned: each passes on its own and leaves
    the trace of :func:`write_register_trace`: its ``writes`` keys all reloaded, or (``lost``, a bool or a function
    of the plan) none, an acknowledged write lost under appendonly yes, which AckedDurable forbids."""

    def run(self, plan, *, deadline=None, run_dir=None):
        keys = plan.inputs["writes"]
        write_register_trace(
            Path(run_dir, "components", "trace", TRACE_FILE),
            keys=keys,
            loaded=0 if (lost(plan) if callable(lost) else lost) else keys,
            scenario_id=plan.scenario_id,
            seed=plan.seed,
            experiment_id=plan.experiment_id,
            inputs=dict(plan.inputs),
        )
        return RunOutcome(plan, OutcomeStatus.PASS, "experiment passed", started_at=1.0, ended_at=2.0)

    monkeypatch.setattr(Runner, "run", run)
    # Canned runs start no servers, so the scenario keeps its port whatever this host is using.
    monkeypatch.setattr("valkey_fuzzer.deployment.local.rerun.busy_planned_ports", lambda deployment: [])
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
