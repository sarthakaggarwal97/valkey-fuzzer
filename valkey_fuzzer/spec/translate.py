"""``spec translate``: a TLC counterexample to ordinary fuzzer scenarios, through the mapping.

The mapping's ``scenario`` section says which experiment runs the
counterexample, how model constants become deployment fields and inputs, and
which model actions are faults, each injected by a named fault component of
the experiment. For each fault step of the trace the translator finds the
mapped evidence the fault follows (``after``): the last occurrence before the
fault, by the same process when ``target: same-process``, and sets the
experiment input ``occurrence_input`` to its occurrence number among all of
that evidence's steps (without ``occurrence_input`` the fault fires on the
first occurrence, so the trigger must be the first). Model steps between the
trigger and the fault are reported: the real fault fires when the trigger
event is seen, so those steps must happen in that gap or not at all, which is
what the nearby timing variants explore (occurrence shifts and the inputs in
``variants.inputs``).

The experiment is the bundle's own (``experiments/``, see
:func:`bundle_service`) or an installed one. Every scenario is compiled by the
fuzzer's compiler before it is written, and each mapped fault must be a fault
component of the compiled plan.

:func:`search_model` finds counterexamples itself instead of taking one: a
path of each safe config to a target of ``claims.yaml`` (TLC checks
``~Target`` as an invariant, so its counterexample is a shortest path that
reaches it), and the violations of the safe configs with their boolean
constants flipped, one at a time and in pairs when the single flips were
cheap. Traces with the same action sequence are translated once. Each TLC
run has a time budget, the search a cap on scenarios, and the report says
which targets were unreachable and which mutations held.
"""

import itertools
import os
import posixpath
import re
import time
from collections.abc import Mapping, Sequence
from typing import Any, Callable, Optional

from ..protocols import ComponentRole
from ..schema import plain_config
from ..service import RunService
from .bundle import CLAIMS_FILE, MODEL_FILE, Bundle, Evidence, SpecBundleError, tla_text
from .tla import (
    TlaSet,
    TlaTools,
    TlcResult,
    TraceState,
    changed_keys,
    normalize,
    parse_config,
    parse_value,
    render,
    strip_comments,
    symbols,
    tlc,
)

Compile = Callable[[Mapping[str, Any]], Any]
Progress = Callable[[str], None]

# The module and config of a reach run: the model extended with the invariant that the target is not reached.
REACH_MODULE = "reach.tla"
REACH_CONFIG = "reach.cfg"
# The config of a mutation run: a safe config with some boolean constants flipped.
MUTANT_CONFIG = "mutant.cfg"
_BOOLEANS = ("TRUE", "FALSE")


class TranslateError(ValueError):
    """The counterexample cannot be expressed as a scenario of the mapped experiment."""


class NoFaultStepError(TranslateError):
    """The counterexample has none of the mapped fault actions: there is nothing to inject."""


def bundle_service(bundle: Bundle, experiment_dirs: Sequence[str] = ()) -> RunService:
    """A run service with the Valkey experiments, ``experiment_dirs``, and the bundle's own ``experiments/``."""

    directories = list(experiment_dirs)
    own = bundle.experiment_directory
    if own is not None and all(os.path.realpath(item) != os.path.realpath(own) for item in directories):
        directories.append(own)
    service = RunService(experiment_directories=directories)
    service.catalog  # noqa: B018 - loads the experiments now, so one that does not load fails here
    return service


def _changed(before: Mapping[str, Any], after: Mapping[str, Any], var: str) -> bool:
    return before.get(var) != after.get(var)


def _process(before: Mapping[str, Any], after: Mapping[str, Any], variables: Sequence[str]) -> Optional[str]:
    """The argument of the first of ``variables`` (functions) that changed in this step."""

    for var in variables:
        keys = changed_keys(before.get(var), after.get(var))
        if len(keys) == 1:
            return render(keys[0])
    return None


def evidence_steps(bundle: Bundle, trace: Sequence[TraceState], evidence: Evidence) -> list[dict[str, Any]]:
    """The trace steps that would show ``evidence`` in a real run, with the process each acts on."""

    steps = []
    for before, after in zip(trace, trace[1:]):
        if after.action != evidence.action:
            continue
        step = evidence.step
        if "changes" in step and not _changed(before.values, after.values, step["changes"]):
            continue
        if "unchanged" in step and _changed(before.values, after.values, step["unchanged"]):
            continue
        steps.append(
            {
                "state": after.index,
                "action": after.action,
                "process": _process(before.values, after.values, step.get("process", [])),
            }
        )
    return steps


def _resolve(spec: Any, constants: Mapping[str, str], where: str) -> Any:
    """A scenario value: a literal, ``{count: Const}``, or ``{constant: Const, map: {...}}``."""

    if not isinstance(spec, Mapping):
        return spec
    name = spec.get("count", spec.get("constant"))
    if name not in constants:
        raise TranslateError(f"{where} needs constant {name}, which the config does not set")
    text = constants[name]
    if "count" in spec:
        value = parse_value(text)
        if not isinstance(value, TlaSet):
            raise TranslateError(f"{where}: {name} = {text} is not a set")
        return len(value)
    choices = {tla_text(key): item for key, item in (spec.get("map") or {}).items()}
    if choices:
        if text not in choices:
            raise TranslateError(f"{where}: no mapping for {name} = {text}")
        return choices[text]
    return parse_value(text)


def translate_trace(
    bundle: Bundle,
    trace: Sequence[TraceState],
    config_text: str,
    *,
    name: str,
    violated: Optional[str],
    config: Optional[str] = None,
    variants: int = 4,
    seed: int = 1,
    compile_scenario: Optional[Compile] = None,
) -> dict[str, Any]:
    """Scenarios for one counterexample: the direct translation first, then up to ``variants`` neighbors.

    ``config`` names the TLC config the trace came from (bundle-relative when
    it is in the bundle); it is recorded, not read.
    """

    mapping = bundle.mapping
    scenario_spec = mapping.scenario
    if not scenario_spec:
        raise TranslateError(f"{mapping.path} has no scenario section")
    if len(trace) < 2:
        raise TranslateError("the counterexample has no steps")
    constants = parse_config(config_text).constants
    deployment = {
        key: _resolve(value, constants, f"scenario.deployment.{key}")
        for key, value in scenario_spec["deployment"].items()
    }
    inputs = {
        key: _resolve(value, constants, f"scenario.inputs.{key}")
        for key, value in (scenario_spec.get("inputs") or {}).items()
    }

    alignment = []
    occurrence_inputs = []
    absent = []
    for fault in scenario_spec.get("faults") or []:
        trigger = mapping.event(fault["after"])
        triggers = evidence_steps(bundle, trace, trigger)
        fault_steps = [
            {"state": after.index, "process": _process(before.values, after.values, fault.get("process", []))}
            for before, after in zip(trace, trace[1:])
            if after.action == fault["action"]
        ]
        if len(fault_steps) > 1:
            raise TranslateError(
                f"the counterexample has {len(fault_steps)} {fault['action']} steps; the mapping expresses one"
            )
        if not fault_steps:
            absent.append(fault)
            continue
        step = fault_steps[0]
        earlier = [item for item in triggers if item["state"] < step["state"]]
        if fault.get("target") == "same-process":
            if step["process"] is None:
                raise TranslateError(f"cannot tell which process {fault['action']} at state {step['state']} acts on")
            earlier = [item for item in earlier if item["process"] == step["process"]]
        if not earlier:
            raise TranslateError(f"{fault['action']} at state {step['state']} follows no {fault['after']} step")
        chosen = earlier[-1]
        occurrence = triggers.index(chosen) + 1
        if "occurrence_input" in fault:
            inputs[fault["occurrence_input"]] = occurrence
            occurrence_inputs.append(fault["occurrence_input"])
        elif occurrence != 1:
            raise TranslateError(
                f"{fault['action']} at state {step['state']} follows {fault['after']} #{occurrence}, but the "
                "mapping sets no occurrence_input, so the fault fires on the first"
            )
        alignment.append(
            {
                "fault": fault["action"],
                "component": fault["component"],
                "fault_state": step["state"],
                "process": step["process"],
                "trigger": fault["after"],
                "trigger_event": trigger.event,
                "trigger_state": chosen["state"],
                "occurrence": occurrence,
                "occurrence_input": fault.get("occurrence_input"),
                # The real fault fires when the trigger is seen: these steps must fit in that gap.
                "between": [trace[index - 1].action for index in range(chosen["state"] + 1, step["state"])],
            }
        )
    if not alignment and scenario_spec.get("faults"):
        raise NoFaultStepError("the counterexample has none of the mapped fault actions")
    if absent:
        # The scenario cannot leave a fault component out, and one left as it is fires with its defaults.
        raise TranslateError(
            "; ".join(
                f"the counterexample has no {fault['action']} step, but the experiment's fault component "
                f"{fault['component']} would still fire, so the scenario would not follow the counterexample"
                for fault in absent
            )
        )

    def scenario(index: int, values: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "scenario_id": _scenario_id(bundle.protocol, name, index),
            "seed": seed + index,
            "experiment": {"id": scenario_spec["experiment"], "inputs": dict(values)},
            "deployment": dict(deployment),
        }

    errors: list[str] = []
    seen: list[Any] = []

    def accept(candidate: dict[str, Any]) -> bool:
        """Compile ``candidate``; keep it unless it runs the same inputs as one already kept."""

        resolved: Any = candidate["experiment"]["inputs"]
        if compile_scenario is not None:
            try:
                plan = compile_scenario(candidate)
            except Exception as exc:
                errors.append(f"{candidate['scenario_id']}: {exc}")
                return False
            faults = {spec.id for spec in plan.components if spec.role is ComponentRole.FAULT}
            missing = sorted({item["component"] for item in alignment} - faults)
            if missing:
                errors.append(
                    f"{candidate['scenario_id']}: {plan.experiment_id} has no fault component {', '.join(missing)}"
                )
                return False
            resolved = plain_config(plan.inputs)
        if resolved in seen:
            return False
        seen.append(resolved)
        return True

    scenarios = [scenario(0, inputs)]
    accept(scenarios[0])
    variant_spec = scenario_spec.get("variants") or {}
    shifts = [0, *variant_spec.get("occurrence_shift", [])]
    timing = variant_spec.get("inputs") or {}
    combos = [dict(zip(timing, values)) for values in itertools.product(*timing.values())] or [{}]
    for shift, combo in itertools.product(shifts, combos):
        if len(scenarios) > variants:
            break
        changed = {**inputs, **combo}
        for key in occurrence_inputs:
            changed[key] = inputs[key] + shift
        if any(changed[key] < 1 for key in occurrence_inputs):
            continue
        candidate = scenario(len(scenarios), changed)
        if accept(candidate):
            scenarios.append(candidate)
    if errors:
        raise TranslateError("translated scenarios do not compile: " + "; ".join(errors))
    return {
        "kind": "spec-translation",
        "bundle": bundle.protocol,
        "mapping_sha256": mapping.sha256,
        "counterexample": {
            "name": name,
            "config": config,
            "violated": violated,
            "length": len(trace),
            "actions": [state.action for state in trace],
            "constants": constants,
        },
        "alignment": alignment,
        # The model's fault actions the scenario leaves to the experiment's own schedule, with the reason.
        "unmapped": dict(scenario_spec.get("unmapped") or {}),
        "scenarios": scenarios,
    }


def _scenario_id(protocol: str, name: str, index: int) -> str:
    base = re.sub(r"[^A-Za-z0-9_.:-]", "-", f"spec-{protocol}-{name}")
    return base if index == 0 else f"{base}-v{index}"


# ---------------------------------------------------------------------------
# Searching the model
# ---------------------------------------------------------------------------


def reach_files(bundle: Bundle, config: str, target: str) -> dict[str, str]:
    """The files of a TLC run that finds a path of the safe config ``config`` to ``target``.

    The module extends the model with ``Not<target> == ~<target>`` (a name
    :func:`search_model` checks the model leaves free), which the config
    checks along with its own invariants (a safe config's never fire), so
    the counterexample TLC reports is a shortest path to the target.
    """

    invariant = f"Not{target}"
    return {
        MODEL_FILE: bundle.model_text,
        REACH_MODULE: f"---- MODULE reach ----\nEXTENDS model\n{invariant} == ~{target}\n====\n",
        REACH_CONFIG: bundle.files[config].rstrip("\n") + f"\nINVARIANT {invariant}\n",
    }


def flipped_config(config_text: str, flips: Mapping[str, str]) -> str:
    """``config_text`` with each boolean constant of ``flips`` set to its value (comments removed)."""

    body = strip_comments(config_text)
    for name, value in flips.items():
        body, count = re.subn(rf"(\b{name}\s*=\s*)(?:TRUE|FALSE)\b", rf"\g<1>{value}", body)
        if count != 1:
            raise TranslateError(f"{name} is assigned a boolean {count} times in the config; the search flips one")
    return body


def without_check(config_text: str, name: str) -> str:
    """``config_text`` with the property ``name`` removed from what TLC checks (its INVARIANT and PROPERTY lists).

    shortcut: the name is removed wherever it stands as a word, so a model value or constant named like a
    property would go too; split the config into its sections if a bundle ever does that.
    """

    return re.sub(rf"(?<!\w){re.escape(name)}(?!\w)", "", config_text)


def mutations(constants: Mapping[str, str], *, pairs: bool = False) -> list[dict[str, str]]:
    """Each boolean constant of ``constants`` (TLA+ text) flipped on its own, or with ``pairs`` each pair of them
    flipped together: the constants to set, in the config's order."""

    booleans = {name: "FALSE" if value == "TRUE" else "TRUE" for name, value in constants.items() if value in _BOOLEANS}
    if pairs:
        return [{a: booleans[a], b: booleans[b]} for a, b in itertools.combinations(booleans, 2)]
    return [{name: value} for name, value in booleans.items()]


def _label(flips: Mapping[str, str]) -> str:
    return ",".join(f"{name}={value}" for name, value in flips.items())


class _Search:
    """One search's state: the traces kept so far (by action sequence), the scenarios produced, and why it stopped."""

    def __init__(
        self,
        bundle: Bundle,
        tools: TlaTools,
        *,
        tlc_timeout_s: float,
        max_scenarios: int,
        variants: int,
        seed: int,
        compile_scenario: Optional[Compile],
        say: Progress,
    ):
        self.bundle = bundle
        self.tools = tools
        self.tlc_timeout_s = tlc_timeout_s
        self.max_scenarios = max_scenarios
        self.variants = variants
        self.seed = seed
        self.compile_scenario = compile_scenario
        self.say = say
        self.seen: dict[tuple[str, ...], str] = {}
        self.translations: list[dict[str, Any]] = []
        self.scenarios: list[str] = []
        self.stopped: Optional[str] = None

    def budget(self) -> Optional[float]:
        """Seconds the next TLC run may take: the per-run limit, within the command's own; None when none is left."""

        remaining = None if self.tools.deadline is None else self.tools.deadline - time.monotonic()
        seconds = self.tlc_timeout_s if remaining is None else min(self.tlc_timeout_s, remaining)
        return seconds if seconds > 0 else None

    def check(
        self, label: str, module: str, config: str, files: Mapping[str, str]
    ) -> tuple[Optional[TlcResult], dict[str, Any]]:
        """One TLC run of the search and its entry; the result is None when the run gave no verdict (the entry says why)."""

        if len(self.scenarios) >= self.max_scenarios:
            self.stopped = f"the scenario cap ({self.max_scenarios}) was reached"
        budget = self.budget()
        if budget is None and self.stopped is None:
            self.stopped = "the command's time limit ran out"
        if self.stopped is not None:
            return None, {"status": "not-run", "reason": self.stopped}
        self.say(f"TLC: {label}")
        result, run = tlc(self.tools.within(budget), module, config, files)
        # The caller sets the status; it is first here so the report reads status, then the run's figures.
        entry: dict[str, Any] = {"status": None}
        entry.update(seconds=run.seconds, distinct_states=result.distinct_states, depth=result.depth)
        if run.timed_out:
            entry.update(status="timed_out", error=run.timeout_error)
        elif run.harness_error:
            entry.update(status="error", error=run.harness_error)
        elif result.errors:
            entry.update(status="error", error="; ".join(result.errors), output=run.tail())
        return (None if entry["status"] else result), entry

    def keep(
        self,
        entry: dict[str, Any],
        result: TlcResult,
        *,
        name: str,
        config: str,
        config_text: str,
        source: Mapping[str, Any],
        status: str,
    ) -> None:
        """Translate the counterexample unless one with the same action sequence came before; record it in ``entry``."""

        actions = tuple(state.action for state in result.trace)
        entry["actions"] = list(actions)
        if actions in self.seen:
            entry.update(status="duplicate", same_as=self.seen[actions])
            return
        self.seen[actions] = name
        room = self.max_scenarios - len(self.scenarios)
        try:
            translation = translate_trace(
                self.bundle,
                result.trace,
                config_text,
                name=name,
                violated=result.violated or result.violation,
                config=config,
                variants=min(self.variants, room - 1),
                seed=self.seed + len(self.scenarios),
                compile_scenario=self.compile_scenario,
            )
        except NoFaultStepError as exc:
            # The experiment has nothing to inject for a counterexample that takes no mapped fault.
            entry.update(status="no-fault", error=str(exc))
            return
        except (TranslateError, SpecBundleError) as exc:
            entry.update(status="untranslatable", error=str(exc))
            return
        translation["source"] = dict(source)
        self.translations.append(translation)
        entry["status"] = status
        entry["scenarios"] = [item["scenario_id"] for item in translation["scenarios"]]
        self.scenarios.extend(entry["scenarios"])


def search_model(
    bundle: Bundle,
    tools: TlaTools,
    *,
    tlc_timeout_s: float,
    max_scenarios: int,
    reach: Sequence[str] = (),
    mutate: bool = False,
    variants: int = 4,
    seed: int = 1,
    compile_scenario: Optional[Compile] = None,
    progress: Optional[Progress] = None,
) -> dict[str, Any]:
    """Search the model for counterexamples and translate each distinct one (by action sequence) into scenarios.

    ``reach`` names targets of ``claims.yaml``: for each, TLC looks in every
    safe config for a path that reaches it. With ``mutate``, every safe
    config runs with each boolean constant flipped, and with each pair
    flipped when the single flips all finished and together took no longer
    than one run's budget; a violation is a counterexample. Each TLC run may
    take ``tlc_timeout_s`` (within the time limit of ``tools``); a run that
    times out is recorded and the search goes on. The search stops once
    ``max_scenarios`` scenarios are written. The report lists each target's
    and mutation's outcome, the targets ``unreachable`` in every safe config,
    the mutations that ``held``, the ``translations``, and the ``scenarios``.
    """

    say = progress or (lambda message: None)
    targets = [item.name for item in bundle.claims.targets]
    unknown = [name for name in reach if name not in targets]
    if unknown:
        raise TranslateError(
            f"{CLAIMS_FILE} names no target {', '.join(unknown)}; its targets are {', '.join(targets) or 'none'}"
        )
    search = _Search(
        bundle,
        tools,
        tlc_timeout_s=tlc_timeout_s,
        max_scenarios=max_scenarios,
        variants=variants,
        seed=seed,
        compile_scenario=compile_scenario,
        say=say,
    )
    report: dict[str, Any] = {
        "kind": "spec-search",
        "bundle": bundle.protocol,
        "mapping_sha256": bundle.mapping.sha256,
        "tlc_timeout_s": tlc_timeout_s,
        "max_scenarios": max_scenarios,
        "targets": {},
        "mutations": {},
    }
    definitions = symbols(bundle.model_text).definitions if reach else frozenset()
    for target in reach:
        if f"Not{target}" in definitions:
            raise TranslateError(f"{MODEL_FILE} defines Not{target}, the name the search needs for ~{target}")
        report["targets"][target] = per_config = {}
        for config in bundle.safe_configs:
            files = reach_files(bundle, config, target)
            result, entry = search.check(f"{config} reaching {target}", REACH_MODULE, REACH_CONFIG, files)
            per_config[config] = entry
            if result is None:
                continue
            if result.violation is None:
                entry["status"] = "unreachable"
            elif result.violated != f"Not{target}":
                entry.update(
                    status="error",
                    error=f"TLC reported {result.violation} {result.violated or ''} violated".rstrip()
                    + f" before any path reached {target}: the safe config is not safe",
                )
            else:
                search.keep(
                    entry,
                    result,
                    name=f"{posixpath.splitext(config)[0]}-reach-{target}",
                    config=config,
                    config_text=bundle.files[config],
                    source={"kind": "reach", "target": target, "config": config},
                    status="reached",
                )
    if mutate:
        # Each safe config's constants (normalized: TRUE and FALSE stay as they are), parsed once for the flips
        # and for telling a mutant that is another safe config.
        safe = {name: _constants(bundle.files[name]) for name in bundle.safe_configs}
        for config in bundle.safe_configs:
            entries: dict[str, Any] = {}
            report["mutations"][config] = entries
            _mutate(search, config, safe, mutations(safe[config]), entries)
            # Pairs when the single flips were cheap: every run finished, and together they fit one run's budget.
            seconds = sum(entry.get("seconds", 0.0) for entry in entries.values())
            cheap = all(entry["status"] != "timed_out" for entry in entries.values()) and seconds <= tlc_timeout_s
            pairs = mutations(safe[config], pairs=True)
            if cheap:
                _mutate(search, config, safe, pairs, entries)
            else:
                reason = f"the single flips took {seconds:.1f}s together, more than one run's {tlc_timeout_s:g}s"
                for flip in pairs:
                    entries[_label(flip)] = {"status": "not-run", "flipped": dict(flip), "reason": reason}
    report["unreachable"] = [
        target
        for target, per_config in report["targets"].items()
        if per_config and all(entry["status"] == "unreachable" for entry in per_config.values())
    ]
    report["held"] = [
        f"{config}: {label}"
        for config, entries in report["mutations"].items()
        for label, entry in entries.items()
        if entry["status"] == "held"
    ]
    report["stopped"] = search.stopped
    report["translations"] = search.translations
    report["scenarios"] = search.scenarios
    return report


def _constants(config_text: str) -> dict[str, str]:
    return {name: normalize(value) for name, value in parse_config(config_text).constants.items()}


def _mutate(
    search: _Search,
    config: str,
    safe: Mapping[str, Mapping[str, str]],
    flips: Sequence[Mapping[str, str]],
    entries: dict[str, Any],
) -> None:
    """Run TLC on ``config`` with each of ``flips`` applied and record each run under its label.

    TLC reports the shallowest violation, which may take none of the mapped
    fault actions (a vote sent before its save needs no crash): the
    experiment has nothing to inject for it. Such a violation is set aside
    (``without_fault``) and the mutation runs again without that property,
    until a violation with a fault, or none, is found.
    """

    faults = {fault["action"] for fault in (search.bundle.mapping.scenario or {}).get("faults") or []}
    claimed = {item.name for item in search.bundle.claims.properties}
    for flip in flips:
        label = _label(flip)
        mutated = {**safe[config], **flip}
        same = next((name for name, values in safe.items() if values == mutated), None)
        if same is not None:
            # Another safe config already has these constants: TLC found no error there, so nothing to translate.
            entries[label] = {"status": "skipped", "flipped": dict(flip), "reason": f"these are {same}'s constants"}
            continue
        text = flipped_config(search.bundle.files[config], flip)
        with_flips = " and ".join(f"{name} = {value}" for name, value in flip.items())
        without_fault: list[dict[str, Any]] = []
        spent = 0.0
        while True:
            files = {MODEL_FILE: search.bundle.model_text, MUTANT_CONFIG: text}
            set_aside = f" without {', '.join(item['violated'] for item in without_fault)}" if without_fault else ""
            result, entry = search.check(f"{config} with {with_flips}{set_aside}", MODEL_FILE, MUTANT_CONFIG, files)
            entry["flipped"] = dict(flip)
            if without_fault:
                entry["without_fault"] = without_fault
                if "seconds" in entry:
                    entry["seconds"] = round(entry["seconds"] + spent, 2)
            entries[label] = entry
            if result is None:
                break
            if result.violation is None:
                entry["status"] = "no-fault" if without_fault else "held"
                break
            violated = result.violated or result.violation
            if faults and not any(state.action in faults for state in result.trace):
                without_fault.append({"violated": violated, "actions": [state.action for state in result.trace]})
                entry["without_fault"] = without_fault
                text = without_check(text, violated) if result.violated else text
                # Look again while a claimed property is still checked (TypeOK alone is no claim to violate).
                if not result.violated or not claimed & set(parse_config(text).checked):
                    entry["status"] = "no-fault"
                    break
                spent += entry["seconds"]
                continue
            entry["violated"] = violated
            search.keep(
                entry,
                result,
                name=f"{posixpath.splitext(config)[0]}-{'-'.join(f'{n}-{v}' for n, v in flip.items())}",
                config=config,
                config_text=text,
                source={"kind": "mutation", "config": config, "flipped": dict(flip)},
                status="violated",
            )
            break


__all__ = [
    "NoFaultStepError",
    "TranslateError",
    "bundle_service",
    "evidence_steps",
    "flipped_config",
    "mutations",
    "reach_files",
    "search_model",
    "translate_trace",
]
