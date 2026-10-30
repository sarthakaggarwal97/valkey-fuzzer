"""The evidence contract: components are held to the events and evidence they declare.

A component that breaks a rule makes the run ERROR, names itself, and what it
emitted reaches no subscriber or oracle.
"""

import threading
import time

import pytest

from tests.valkey_experiments_support import fake_context, resolved
from valkey_fuzzer.compiler import ScenarioCompileError
from valkey_fuzzer.contracts import (
    ComponentRegistration,
    ComponentRole,
    ComponentSpec,
    ComponentStatus,
    Declarations,
    Evidence,
    EvidenceDeclaration,
    EvidenceRequirement,
    ExperimentSpec,
    OutcomeStatus,
    PluginManifest,
    RunMode,
    make_evidence,
    make_result,
)
from valkey_fuzzer.evidence import ContractViolationError
from valkey_fuzzer.extensions.testing import run_experiment
from valkey_fuzzer.schema import ConfigField, ConfigSchema
from valkey_fuzzer_experiments import actions

COUNT = EvidenceDeclaration(ConfigField(dict, schema=ConfigSchema({"n": ConfigField(int)})))
SEEN: dict[str, list] = {"evidence": [], "events": [], "late": []}


def _late_ping(bus):
    time.sleep(0.2)
    try:
        bus.publish("emit.ping", source="emit", payload={"n": 1})
    except Exception as exc:
        SEEN["events"].append(f"rejected: {type(exc).__name__}")


class Emitter:
    """Does one thing its ``mode`` names, right or wrong."""

    def run(self, context, spec):
        mode, bus, evidence = spec.config["mode"], context.event_bus, []
        if mode in ("good", "twice", "uncommitted"):
            sequence = bus.publish("emit.ping", source=spec.id, payload={"n": 1}).sequence
            sequence += 1000 if mode == "uncommitted" else 0
            evidence.append(make_evidence(spec, "tests.count", "action.completed", {"n": 1}, event_sequence=sequence))
        if mode == "twice":
            evidence.append(make_evidence(spec, "tests.count", "action.completed", {"n": 2}))
        elif mode == "foreign-evidence":
            evidence.append(Evidence("tests.count", "intruder", "action.completed", time.monotonic(), {"n": 1}))
        elif mode == "undeclared-evidence":
            evidence.append(make_evidence(spec, "tests.other", "action.completed", {"n": 1}))
        elif mode == "malformed-evidence":
            evidence.append(make_evidence(spec, "tests.count", "action.completed", {"n": "one"}))
        elif mode == "late-event":
            SEEN["late"].append(threading.Thread(target=_late_ping, args=(bus,)))
            SEEN["late"][-1].start()
        else:
            name, source, payload = {
                "undeclared-event": ("emit.other", spec.id, {"n": 1}),
                "malformed-event": ("emit.ping", spec.id, {"n": "one"}),
                "lifecycle-event": ("verify.completed", spec.id, {}),
                "foreign-source": ("intruder.ping", "intruder", {"n": 1}),
            }.get(mode, (None, None, None))
            if name:
                bus.publish(name, source=source, payload=payload)
        return make_result(spec, ComponentStatus.PASS, mode, evidence=evidence)


class Watcher:
    def start(self, context, spec):
        context.event_bus.subscribe("*", lambda event: SEEN["events"].append(event.name))
        return make_result(spec, ComponentStatus.PASS, "watching")

    def stop(self, context, spec):
        for thread in SEEN["late"]:
            thread.join()
        return make_result(spec, ComponentStatus.PASS, "stopped")


class Checker:
    def evaluate(self, context, spec, evidence):
        SEEN["evidence"].extend(evidence)
        return make_result(spec, ComponentStatus.PASS, "checked")


def _registration(component_id, factory, declares=None, **kwargs):
    role = ComponentRole(component_id.split(".")[1])
    return ComponentRegistration(component_id, role, factory, declares=declares or Declarations(), **kwargs)


REGISTRATIONS = (
    _registration(
        "tests.action.emit",
        Emitter,
        Declarations(events={"{id}.ping": ConfigSchema({"n": ConfigField(int)})}, evidence={"tests.count": COUNT}),
        config=ConfigSchema({"mode": ConfigField(str)}),
    ),
    _registration("tests.observer.watch", Watcher),
    _registration("tests.oracle.check", Checker),
)


COUNTED = (EvidenceRequirement("tests.count", "emit"),)


def _run(mode="good", required_evidence=COUNTED, required_events=()):
    for items in SEEN.values():
        items.clear()
    experiment = ExperimentSpec(
        experiment_id="tests.contract",
        mode=RunMode.EXPERIMENT if required_evidence else RunMode.SMOKE,
        deployment_modes=("fake",),
        components=(
            ComponentSpec(use="tests.observer.watch", id="watch"),
            ComponentSpec(use="tests.action.emit", id="emit", config={"mode": mode}),
            ComponentSpec(use="tests.oracle.check", id="verify", depends_on=("emit",)),
        ),
        required_events=required_events,
        required_evidence=required_evidence,
    )
    plugin = PluginManifest(plugin_id="tests.contract", registrations=REGISTRATIONS, experiments=(experiment,))
    return run_experiment(plugin, "tests.contract")


def test_declared_events_and_evidence_pass():
    outcome = _run("good", required_events=("emit.ping", "watch.stopped", "deployment.ready"))

    assert outcome.status is OutcomeStatus.PASS, outcome.summary
    assert [item.evidence_id for item in SEEN["evidence"]] == ["tests.count"]
    assert "emit.ping" in SEEN["events"]


@pytest.mark.parametrize(
    ("mode", "component", "kind", "name", "message"),
    [
        ("foreign-evidence", "emit", "evidence", "tests.count", "emitted evidence owned by 'intruder'"),
        ("undeclared-evidence", "emit", "evidence", "tests.other", "emitted undeclared evidence 'tests.other'"),
        ("malformed-evidence", "emit", "evidence", "tests.count", "emitted malformed 'tests.count'"),
        ("twice", "emit", "evidence", "tests.count", "emitted 'tests.count' 2 times, but declares it as a single value"),
        ("uncommitted", "emit", "evidence", "tests.count", "referencing uncommitted event sequence"),
        ("undeclared-event", "emit", "event", "emit.other", "published undeclared event 'emit.other'"),
        ("malformed-event", "emit", "event", "emit.ping", "published a malformed 'emit.ping'"),
        ("lifecycle-event", "emit", "event", "verify.completed", "which only the runner publishes"),
        ("foreign-source", "intruder", "event", "intruder.ping", "is not a component of the plan"),
        ("late-event", "emit", "event", "emit.ping", "after its last call returned"),
    ],
)  # fmt: skip
def test_a_contract_violation_is_an_error_naming_the_component(mode, component, kind, name, message):
    outcome = _run(mode, required_evidence=())

    assert outcome.status is OutcomeStatus.ERROR
    assert outcome.summary.startswith(f"evidence contract violated: component '{component}'")
    (violation,) = outcome.diagnostics["contract_violations"]
    assert (violation["component"], violation["kind"], violation["name"]) == (component, kind, name)
    assert message in violation["message"]
    # Nothing emitted against the declarations reached the oracle or a subscriber.
    assert not any(item.source == component for item in [*SEEN["evidence"], *outcome.all_evidence])
    assert name not in SEEN["events"]


@pytest.mark.parametrize(
    ("requirements", "message"),
    [
        ({"required_evidence": (EvidenceRequirement("tests.missing", "emit"),)}, "not declared by component 'emit'"),
        ({"required_evidence": (EvidenceRequirement("tests.missing"),)}, "not declared by any component"),
        ({"required_evidence": (EvidenceRequirement("tests.count", "emit", min_count=2),)}, "as a single value"),
        ({"required_events": ("emit.never",)}, "required event 'emit.never' is not published"),
    ],
)  # fmt: skip
def test_requirements_nothing_in_the_plan_declares_fail_to_compile(requirements, message):
    with pytest.raises(ScenarioCompileError, match=message):
        _run("good", **requirements)


@pytest.mark.parametrize(
    ("declare", "message"),
    [
        (lambda: ComponentRegistration("tests.action.emit", ComponentRole.ACTION, Emitter), "must declare the events"),
        (lambda: Declarations(events={"{id}.completed": ConfigSchema()}), "published by the runner"),
        (lambda: Declarations(events={"{name}.x": ConfigSchema()}), "placeholder other than"),
        (lambda: Declarations(evidence={"unqualified": COUNT}), "namespaced identifier"),
        (lambda: Declarations(evidence={"tests.x": ConfigField(dict)}), "must be a EvidenceDeclaration"),
    ],
)  # fmt: skip
def test_a_missing_or_malformed_declaration_is_rejected_at_registration(declare, message):
    with pytest.raises((TypeError, ValueError), match=message):
        declare()


# conftest.py's strict_plugin_contract holds plugin components a unit test calls to their declarations.
def test_strict_mode_fails_a_component_that_breaks_its_declarations(monkeypatch, strict_plugin_contract):
    # Known-bad: replies without the declared fields, as a drifted command action would emit them.
    replies = [{"matched": True, "reply": "OK"}]
    monkeypatch.setattr(actions, "run_command_on_instances", lambda context, spec, config: (True, replies))
    spec = ComponentSpec("valkey.action.command", "subject", ComponentRole.ACTION, {"command": ["PING"]})

    with pytest.raises(ContractViolationError, match=r"emitted malformed 'valkey\.command\.replies'"):
        actions.CommandAction().run(fake_context(object()), resolved(spec))
    (checked,) = strict_plugin_contract
    assert [item["name"] for item in checked.contract.violations] == ["valkey.command.replies"]
    checked.contract.violations.clear()
