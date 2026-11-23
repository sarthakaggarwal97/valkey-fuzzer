"""Spec bundles: ``formal/<protocol>/`` with its claims, model, configs, mapping, and negative controls.

::

    <protocol>/
      claims.yaml          properties, scope, assumptions, bounds, exclusions, approval
      model.tla            MODULE model
      *.cfg                expected-safe TLC configurations
      mapping.yaml         model actions and variables <-> implementation evidence
      negative-controls/   <name>.cfg (+ optional <name>.tla): must violate a named property
      experiments/         optional data-only experiments the mapping's scenario can name

A reference to a bundle is a path to its directory, or its protocol name
(no ``/``), which :func:`find_bundle` looks up in the directories of
``$VALKEY_FUZZER_SPEC_PATH`` and then in the bundles shipped with
valkey-fuzzer (``formal/``, which the wheel installs as
``valkey_fuzzer/formal``), never in the current directory.

:func:`load_bundle` reads and checks the YAML files against the model text;
it does not run any TLA+ tool (see :mod:`valkey_fuzzer.spec.check`).
"""

import copy
import fnmatch
import functools
import os
import re
from collections.abc import Mapping, Sequence
from collections.abc import Set as AbstractSet
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Optional, Union

from .._data import file_stamp, sha256_text
from ..documents import ABSENT, get_path
from ..schema import ConfigError, ConfigField, ConfigSchema, load_yaml_mapping
from . import tla

SPEC_PATH_VARIABLE = "VALKEY_FUZZER_SPEC_PATH"
CLAIMS_FILE = "claims.yaml"
MODEL_FILE = "model.tla"
MAPPING_FILE = "mapping.yaml"
CONTROLS_DIRECTORY = "negative-controls"
EXPERIMENTS_DIRECTORY = "experiments"
# Lines Valkey logged, at least one per mapped log pattern: spec check's log_patterns gate matches each
# pattern against them, so a wrong pattern cannot silently blind every reader of the mapping.
SAMPLE_LOG_FILE = "sample.log"
# The JSONL a trace component writes in its artifact folder and spec conform reads (:mod:`.conform`).
TRACE_FILE = "trace.jsonl"
# Who logged a mapped line: any process, or the server only (a fork child's lines, role C, are skipped).
LOGGED_BY = ("any", "server")

# node reads as str; the name tells a reader the field is a node of the trace header.
FIELD_TYPES = ("int", "str", "bool", "node", "node_id")
UPDATE_OPS = ("set", "max", "add", "remove", "clear", "incr")
CLAIM_STATUSES = ("draft", "approved")
# claims.yaml ``max_unobserved_steps`` when absent: one model step no line shows may come between two observed
# states (an unobservable save before the vote it lets leave).
DEFAULT_MAX_UNOBSERVED_STEPS = 1


class SpecBundleError(ValueError):
    """A spec bundle is missing a file or one of its files is malformed."""


def tla_text(value: Any) -> str:
    """A YAML scalar as the TLA+ text it stands for (``true`` -> ``TRUE``)."""

    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value)


def shipped_bundles() -> Path:
    """The bundles shipped with valkey-fuzzer: ``formal/`` of a checkout, or of an installed wheel."""

    package = Path(__file__).resolve().parents[1]
    installed = package / "formal"
    return installed if installed.is_dir() else package.parent / "formal"


def bundle_directories() -> list[Path]:
    """Where protocol names are looked up, in order: ``$VALKEY_FUZZER_SPEC_PATH``, then the shipped bundles."""

    listed = [Path(item) for item in os.environ.get(SPEC_PATH_VARIABLE, "").split(os.pathsep) if item]
    return [*listed, shipped_bundles()]


def find_bundle(reference: str) -> Path:
    """The bundle directory ``reference`` names: a bundle directory, or a protocol name (see the module docs).

    A protocol name never means a directory of the current one, so what a
    scenario's ``spec: <protocol>`` reads does not depend on where it runs;
    write ``./<name>`` for that directory.
    """

    path = Path(reference)
    if reference and not any(separator in reference for separator in ("/", os.sep)) and reference not in (".", ".."):
        directories = bundle_directories()
        for directory in directories:
            if (directory / reference / CLAIMS_FILE).is_file():
                return (directory / reference).resolve()
        looked = ", ".join(str(directory) for directory in directories)
        raise SpecBundleError(
            f"no spec bundle named {reference!r} in {looked} (${SPEC_PATH_VARIABLE} adds directories; "
            f"write ./{reference} for a directory here)"
        )
    if (path / CLAIMS_FILE).is_file():
        return path.resolve()
    raise SpecBundleError(f"no spec bundle at {reference!r} (looked for {path / CLAIMS_FILE})")


# ---------------------------------------------------------------------------
# claims.yaml
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Property:
    name: str
    kind: str  # invariant | property
    statement: str
    holds_when: dict[str, str]
    negative_controls: tuple[str, ...]
    conform: bool
    conform_note: Optional[str]

    def holds_under(self, constants: Mapping[str, str]) -> bool:
        """Whether ``constants`` (TLA+ value text) meet ``holds_when``: the property is claimed for them."""

        return all(
            tla.normalize(constants.get(key, "")) == tla.normalize(value) for key, value in self.holds_when.items()
        )


@dataclass(frozen=True)
class Target:
    name: str


@dataclass(frozen=True)
class Claims:
    protocol: str
    summary: str
    status: str
    approved_by: Optional[str]
    scope: tuple[dict[str, Any], ...]
    properties: tuple[Property, ...]
    targets: tuple[Target, ...]
    assumptions: tuple[str, ...]
    exclusions: tuple[str, ...]
    bounds: dict[str, str]
    # How many model steps no line shows conform allows between two observed states (its transition check).
    max_unobserved_steps: int = DEFAULT_MAX_UNOBSERVED_STEPS
    # Why the transition check cannot apply to this bundle's mapping (``transitions: {excluded: <why>}``), or None.
    transitions_excluded: Optional[str] = None


def _field(rule: Any) -> ConfigField:
    return rule if isinstance(rule, ConfigField) else ConfigField(rule)


def _optional(rule: Any) -> ConfigField:
    field = copy.copy(_field(rule))
    field.required, field.allow_none = False, True
    return field


def _record(required: Mapping[str, Any], optional: Mapping[str, Any] = MappingProxyType({})) -> ConfigField:
    """A mapping with exactly these fields, each a type or a :class:`ConfigField`; an optional one may be null."""

    fields = {name: _field(rule) for name, rule in required.items()}
    fields.update((name, _optional(rule)) for name, rule in optional.items())
    return ConfigField(dict, schema=ConfigSchema(fields))


def _named(rule: Any) -> ConfigField:
    """A mapping from names to values ``rule`` checks."""

    return ConfigField(dict, schema=ConfigSchema(additional=_field(rule)))


def _named_only(names: Sequence[str]) -> ConfigSchema:
    """A mapping of mappings whose keys must be among ``names`` (the model's variables or constants)."""

    return ConfigSchema({name: ConfigField(dict, required=False) for name in names})


def _list(rule: Any) -> ConfigField:
    return ConfigField(list, items=_field(rule))


_TEXTS = _list(str)
_CLAIMS = _record(
    {
        "protocol": str,
        "summary": str,
        "status": ConfigField(str, choices=CLAIM_STATUSES),
        "scope": _list(_record({"file": str, "functions": _TEXTS})),
        "properties": _list(
            _record(
                {
                    "name": str,
                    "kind": ConfigField(str, choices=("invariant", "property")),
                    "statement": str,
                    "negative_controls": _TEXTS,
                    "conform": object,  # true, or {excluded: <why>}
                },
                {"holds_when": dict},
            )
        ),
        "assumptions": _TEXTS,
        "exclusions": _TEXTS,
        "bounds": dict,
    },
    {
        "approved_by": str,
        "targets": _list(_record({"name": str}, {"meaning": str})),
        "max_unobserved_steps": int,
        "transitions": _record({"excluded": str}),
    },
)
_CONFORM_EXCLUDED = _record({"excluded": str})


def _resolve(schema: Union[ConfigSchema, ConfigField], value: Any, where: str) -> Any:
    """``value`` checked against ``schema``, as plain data; a mismatch is a :class:`SpecBundleError`."""

    try:
        return schema.resolve(value, where)
    except ConfigError as exc:
        raise SpecBundleError(str(exc)) from None


def _texts(values: Sequence[str], where: str) -> tuple[str, ...]:
    """A list of strings that must not be blank."""

    if not all(item.strip() for item in values):
        raise SpecBundleError(f"{where} must be a list of non-empty strings")
    return tuple(values)


def parse_claims(document: Mapping[str, Any], model: tla.ModelSymbols) -> Claims:
    where = CLAIMS_FILE
    document = _resolve(_CLAIMS, document, where)
    status = document["status"]
    approved_by = document.get("approved_by")
    if status == "approved" and not (approved_by and approved_by.strip()):
        raise SpecBundleError(f"{where}: approved claims need approved_by (the maintainer who approved them)")
    if status == "draft" and approved_by is not None:
        raise SpecBundleError(f"{where}: a draft has no approved_by")
    defined = model.definitions
    constants = set(model.constants)
    scope = document["scope"]
    if not scope:
        raise SpecBundleError(f"{where}.scope must list the implementation files and functions the claims cover")
    for index, item in enumerate(scope):
        _texts(item["functions"], f"{where}.scope[{index}].functions")
    properties = []
    if not document["properties"]:
        raise SpecBundleError(f"{where}.properties must list at least one property")
    for index, item in enumerate(document["properties"]):
        at = f"{where}.properties[{index}]"
        if item["name"] not in defined:
            raise SpecBundleError(f"{at}: {item['name']!r} is not defined in {MODEL_FILE}")
        holds_when = item.get("holds_when") or {}
        if not set(holds_when) <= constants:
            raise SpecBundleError(f"{at}.holds_when must map model constants to values")
        controls = _texts(item["negative_controls"], f"{at}.negative_controls")
        if not controls:
            raise SpecBundleError(f"{at}: every property needs a negative control that must violate it")
        conform = item["conform"]
        note = None
        if isinstance(conform, Mapping):
            note = _resolve(_CONFORM_EXCLUDED, conform, f"{at}.conform")["excluded"]
            conform = False
        elif conform is not True:
            raise SpecBundleError(f"{at}.conform must be true or {{excluded: <why>}}")
        elif item["kind"] != "invariant":
            raise SpecBundleError(
                f"{at}: conform: true needs an invariant; conform checks each observed state, not a behavior"
            )
        properties.append(
            Property(
                name=item["name"],
                kind=item["kind"],
                statement=item["statement"],
                holds_when={key: tla_text(value) for key, value in holds_when.items()},
                negative_controls=controls,
                conform=conform,
                conform_note=note,
            )
        )
    targets = []
    for index, item in enumerate(document.get("targets") or []):
        if item["name"] not in defined:
            raise SpecBundleError(f"{where}.targets[{index}]: {item['name']!r} is not defined in {MODEL_FILE}")
        targets.append(Target(name=item["name"]))
    bounds = document["bounds"]
    if not set(bounds) <= constants:
        raise SpecBundleError(f"{where}.bounds must map model constants to the values the safe configs use")
    unobserved = document.get("max_unobserved_steps")
    if unobserved is None:
        unobserved = DEFAULT_MAX_UNOBSERVED_STEPS
    elif unobserved < 0:
        raise SpecBundleError(f"{where}.max_unobserved_steps must be a count of model steps, 0 or more")
    transitions = document.get("transitions") or {}
    if "excluded" in transitions and not transitions["excluded"].strip():
        raise SpecBundleError(f"{where}.transitions.excluded must say why the transition check does not apply")
    return Claims(
        protocol=document["protocol"],
        summary=document["summary"],
        status=status,
        approved_by=approved_by,
        scope=tuple(scope),
        properties=tuple(properties),
        targets=tuple(targets),
        assumptions=_texts(document["assumptions"], f"{where}.assumptions"),
        exclusions=_texts(document["exclusions"], f"{where}.exclusions"),
        bounds={key: tla_text(value) for key, value in bounds.items()},
        max_unobserved_steps=unobserved,
        transitions_excluded=transitions.get("excluded"),
    )


# ---------------------------------------------------------------------------
# mapping.yaml
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FieldSpec:
    name: str
    path: str
    type: str
    default: Any = ABSENT
    # Captured values replaced by the model's names (``master`` -> ``primary``); a value not listed stays.
    map: Mapping[Any, Any] = field(default_factory=dict)

    @property
    def optional(self) -> bool:
        return self.default is not ABSENT


@dataclass(frozen=True)
class Update:
    op: str
    var: str
    key: Any = None
    value: Any = None


@dataclass(frozen=True)
class After:
    """The record an evidence record follows: one of ``evidence`` whose fields equal ``match`` (``$field`` refs).

    ``take`` names the fields this record copies from the latest such record, for a line that does not carry them.
    """

    evidence: str
    match: dict[str, str]
    take: tuple[str, ...] = ()


@dataclass(frozen=True)
class Evidence:
    """One kind of implementation evidence: a fuzzer event (optionally from a log line) or a state sample."""

    name: str
    event: Optional[str]
    sample: bool
    log: Optional[str]
    where: dict[str, Any]
    fields: dict[str, FieldSpec]
    action: Optional[str]
    step: dict[str, Any]
    updates: tuple[Update, ...]
    component: Optional[str] = None  # the experiment component that publishes the event
    after: Optional[After] = None
    logged_by: str = "any"  # who logs the line: any process, or the server only (not a fork child)

    def body(self, record: Mapping[str, Any]) -> Mapping[str, Any]:
        """Where this evidence's fields are in a record: a sample itself, or an event's payload."""

        return record if self.sample else (record.get("payload") or {})

    def matches(self, record: Mapping[str, Any]) -> bool:
        """Whether a trace record (``kind`` event or observation) is this evidence."""

        if self.sample:
            if record.get("kind") != "observation" or "error" in record:
                return False
        else:
            if record.get("kind") != "event" or not fnmatch.fnmatchcase(str(record.get("name")), self.event or ""):
                return False
            if self.component is not None and record.get("source") != self.component:
                return False
        return where_matches(self.body(record), self.where)


@dataclass(frozen=True)
class MemberRule:
    evidence: str
    field: str
    where: dict[str, Any]
    require: tuple[str, ...]


@dataclass(frozen=True)
class ProcessSet:
    name: str
    prefix: str
    members: tuple[MemberRule, ...]


@dataclass(frozen=True)
class StateVar:
    name: str
    over: tuple[str, ...]
    init: Any
    hidden: Optional[str]


@dataclass(frozen=True)
class SampleSpec:
    """``sample:``: what a trace observer (``valkey.observer.trace``) reads from every node in each sampling round,
    as the ``sample: true`` evidence finds it in an observation record. A bundle declares what it needs and nothing
    more; the observer skips the cluster parts on a standalone deployment."""

    info: tuple[str, ...]  # INFO sections, their fields as text under ``info``
    digest: bool  # DEBUG DIGEST, under ``digest``
    digest_keys: tuple[str, ...]  # DEBUG DIGEST-VALUE of each key, under ``digest_values.<key>``
    cluster: bool  # CLUSTER INFO and CLUSTER NODES: the node's view (``current_epoch``, ``role``, ``slots``, ...)
    slot_migrations: bool  # CLUSTER GETSLOTMIGRATIONS under ``slot_migrations``, the jobs by kind
    nodes_conf: bool  # the ``vars`` line of nodes.conf under ``disk``, read from the file, also when the node is down
    on: tuple[str, ...]  # event name globs after which a round runs; empty: after every event

    def collects(self) -> bool:
        return bool(
            self.info or self.digest or self.digest_keys or self.cluster or self.slot_migrations or self.nodes_conf
        )


# The top segment of a sample field's path, and the part of ``sample:`` that records it (None: every record).
_CLUSTER_VIEW_FIELDS = ("node_id", "current_epoch", "config_epoch", "role", "primary_id", "slots", "state", "primaries")
_SAMPLE_PATHS = {
    **dict.fromkeys(("node", "round", "reason", "t"), None),
    **dict.fromkeys(_CLUSTER_VIEW_FIELDS, "cluster"),
    "info": "info",
    "digest": "digest",
    "digest_values": "digest_keys",
    "slot_migrations": "slot_migrations",
    "disk": "nodes_conf",
}


@dataclass(frozen=True)
class SpecMapping:
    """``mapping.yaml``: the single place a model's implementation evidence is defined."""

    path: str
    sha256: str
    evidence: dict[str, Evidence]
    unobservable: dict[str, str]
    processes: dict[str, ProcessSet]
    state: dict[str, StateVar]
    constants: dict[str, dict[str, Any]]
    scenario: Optional[dict[str, Any]]
    sample: Optional[SampleSpec] = None

    def log_patterns(self) -> dict[str, str]:
        """``{event name: log pattern}`` for every evidence published from a log line."""

        return {item.event: item.log for item in self.evidence.values() if item.log and item.event}

    def event(self, name: str) -> Evidence:
        """The evidence entry ``name``; raise :class:`SpecBundleError` if the mapping has none."""

        if name not in self.evidence:
            raise SpecBundleError(f"{self.path} has no evidence named {name!r}")
        return self.evidence[name]

    def actions(self) -> dict[str, list[str]]:
        """``{model action: [evidence names]}`` for every action the mapping says is observable."""

        observed: dict[str, list[str]] = {}
        for item in self.evidence.values():
            if item.action:
                observed.setdefault(item.action, []).append(item.name)
        return observed


def where_matches(body: Mapping[str, Any], where: Mapping[str, Any]) -> bool:
    """Whether every dotted path of ``where`` holds its value in ``body``."""

    return all(get_path(body, key) == value for key, value in where.items())


_FIELD_REF = re.compile(r"^\$([A-Za-z_][\w.]*)(?:@([A-Za-z_]\w*))?$")


def field_ref(spec: Any) -> Optional[tuple[str, Optional[str]]]:
    """``(field, process set or None)`` when ``spec`` is ``$field`` or ``$field@ProcessSet``, else None."""

    match = _FIELD_REF.match(spec) if isinstance(spec, str) else None
    return (match.group(1), match.group(2)) if match else None


def field_refs(spec: Any) -> list[tuple[str, Optional[str]]]:
    """Every ``$field`` or ``$field@ProcessSet`` reference in a value spec."""

    if isinstance(spec, str):
        ref = field_ref(spec)
        return [ref] if ref else []
    if isinstance(spec, Mapping):
        return [ref for item in spec.values() for ref in field_refs(item)]
    if isinstance(spec, list):
        return [ref for item in spec for ref in field_refs(item)]
    return []


def derive_constants(
    mapping: SpecMapping,
    configs: Mapping[str, Mapping[str, Any]],
    inputs: Mapping[str, Any],
    *,
    states: Sequence[Mapping[str, Any]] = (),
    field_values: Mapping[str, Sequence[int]] = MappingProxyType({}),
) -> tuple[dict[str, str], dict[str, str]]:
    """The model constants ``mapping.constants`` derives from a run, as TLA+ text, and the ones it cannot:
    ``({name: value}, {name: why it is unknown})``.

    ``configs`` is every node's settings, ``{node: {config name: value}}``; ``inputs`` the scenario's; ``states``
    the observed states (``max_of``); ``field_values`` the int values a ``field`` constant's field took in the
    run's records, by constant name. A ``value`` is its text. A ``config`` that ``equals`` a value is FALSE once
    a node's setting differs, TRUE when every node's setting is known and equals, and unknown when a node's
    setting is missing and none differs: the node nobody could read may be the one that differs. An ``input``
    the run does not record, a ``max_of`` without states, and a ``field`` without its values are unknown.
    """

    values: dict[str, str] = {}
    unknown: dict[str, str] = {}
    for name, spec in mapping.constants.items():
        if "value" in spec:
            values[name] = str(spec["value"])
        elif "config" in spec:
            seen = {node: settings.get(spec["config"]) for node, settings in configs.items()}
            if any(value is not None and value != spec["equals"] for value in seen.values()):
                values[name] = tla.render(False)
            elif seen and all(value is not None for value in seen.values()):
                values[name] = tla.render(True)
            else:
                missing = sorted(node for node, value in seen.items() if value is None) or ["every node"]
                unknown[name] = f"needs {spec['config']!r}, and the setting of {', '.join(missing)} is unknown"
        elif "input" in spec:
            if spec["input"] not in inputs:
                unknown[name] = f"needs input {spec['input']!r}, which the run does not record"
            else:
                values[name] = tla.render(inputs[spec["input"]] == spec["equals"])
        elif "field" in spec:
            # The largest value of the field in the run's records of that evidence (the quorum an ACK line logs),
            # at_least when no record carries it.
            if name not in field_values:
                unknown[name] = "needs the run's records"
            else:
                values[name] = str(max([spec.get("at_least", 0), *field_values[name]]))
        elif not states:
            unknown[name] = "needs the observed states"
        else:
            numbers = [spec.get("at_least", 0)]
            for state in states:
                for var in spec["max_of"]:
                    value = state[var]
                    numbers.extend(value.values() if isinstance(value, dict) else [value])
            values[name] = str(max(item for item in numbers if isinstance(item, int)))
    return values, unknown


def claimed_under(prop: Optional[Property], constants: Mapping[str, str]) -> Optional[bool]:
    """Whether ``prop`` is claimed under ``constants`` (its ``holds_when``); None when one it needs is unknown,
    and True for a property the claims do not list (nothing conditions it)."""

    if prop is None:
        return True
    if any(key not in constants for key in prop.holds_when):
        return None
    return prop.holds_under(constants)


_FIELD_SPEC = _record({"type": ConfigField(str, choices=FIELD_TYPES)}, {"path": str, "default": object, "map": dict})
_UPDATE = _record({}, {**dict.fromkeys(UPDATE_OPS, str), "key": object, "value": object})
_EVIDENCE = _record(
    {"fields": _named((str, dict))},
    {
        "event": str,
        "sample": bool,
        "log": str,
        "where": dict,
        "action": str,
        "step": _record({}, {"changes": str, "unchanged": str, "process": _TEXTS, "repeats": bool}),
        "updates": _list(dict),
        "component": str,
        "after": _record({"evidence": str, "match": _named(str)}, {"take": _TEXTS}),
        "logged_by": ConfigField(str, choices=LOGGED_BY),
    },
)
_PROCESS = _record(
    {"prefix": str, "members": _list(_record({"evidence": str, "field": str}, {"where": dict, "require": _TEXTS}))}
)
_HIDDEN_STATE = _record({"hidden": object, "why": object})
_VISIBLE_STATE = _record({"init": object}, {"over": _TEXTS})
# A constant read from a field of the run's records of one evidence: {field: {evidence: acked, name: quorum}}.
_FROM_FIELD = _record({"evidence": str, "name": str})
_CONSTANT = _record(
    {},
    {
        "value": object,
        "config": str,
        "input": str,
        "equals": object,
        "max_of": _TEXTS,
        "field": _FROM_FIELD,
        "at_least": int,
        "why": str,
    },
)
# A scenario value drawn from a model constant: {constant: C, map: {...}} or {count: C}.
_FROM_CONSTANT = _record({}, {"constant": str, "map": dict, "count": str})
_FAULT = _record(
    {"action": str, "after": str, "component": str},
    {"occurrence_input": str, "target": ConfigField(str, choices=("same-process",)), "process": _TEXTS},
)
_SCENARIO = _record(
    {"experiment": str, "deployment": dict},
    {
        "inputs": dict,
        "faults": _list(_FAULT),
        # Model fault actions the scenario does not inject, each with the reason (no fault component does it, or no
        # counterexample takes it): {action: reason}.
        "unmapped": _named(str),
        "variants": _record({}, {"occurrence_shift": _list(int), "inputs": _named(list)}),
    },
)
_SAMPLE = _record(
    {},
    {
        "info": _TEXTS,
        "digest": bool,
        "digest_keys": _TEXTS,
        "cluster": bool,
        "slot_migrations": bool,
        "nodes_conf": bool,
        "on": _TEXTS,
    },
)
_MAPPING = _record(
    {"evidence": _named(_EVIDENCE), "state": _named(dict), "constants": _named(_CONSTANT)},
    {"unobservable": _named(str), "processes": _named(_PROCESS), "scenario": _SCENARIO, "sample": _SAMPLE},
)


def _parse_fields(value: Mapping[str, Any], where: str) -> dict[str, FieldSpec]:
    fields = {}
    for name, spec in value.items():
        spec = _resolve(_FIELD_SPEC, {"type": spec} if isinstance(spec, str) else spec, f"{where}.{name}")
        fields[name] = FieldSpec(
            name,
            spec.get("path", name),
            spec["type"],
            spec.get("default", ABSENT),
            MappingProxyType(dict(spec.get("map") or {})),
        )
    return fields


def _parse_sample(document: Mapping[str, Any], where: str) -> SampleSpec:
    sample = SampleSpec(
        info=tuple(_texts(document.get("info") or [], f"{where}.info")),
        digest=bool(document.get("digest")),
        digest_keys=tuple(_texts(document.get("digest_keys") or [], f"{where}.digest_keys")),
        cluster=bool(document.get("cluster")),
        slot_migrations=bool(document.get("slot_migrations")),
        nodes_conf=bool(document.get("nodes_conf")),
        on=tuple(_texts(document.get("on") or [], f"{where}.on")),
    )
    if not sample.collects():
        raise SpecBundleError(f"{where} must say what the trace observer records (info, digest, cluster, ...)")
    return sample


def _parse_update(value: Any, where: str, state_names: set[str]) -> Update:
    value = _resolve(_UPDATE, value, where)
    ops = [op for op in UPDATE_OPS if op in value]
    if len(ops) != 1:
        raise SpecBundleError(f"{where} needs exactly one of {list(UPDATE_OPS)}")
    op = ops[0]
    var = value[op]
    if var not in state_names:
        raise SpecBundleError(f"{where}: {var!r} is not a state variable of the mapping")
    if op in ("set", "max", "add", "remove") and "value" not in value:
        raise SpecBundleError(f"{where}: {op} needs a value")
    return Update(op=op, var=var, key=value.get("key"), value=value.get("value"))


def parse_mapping(document: Mapping[str, Any], model: tla.ModelSymbols, *, path: str, sha256: str) -> SpecMapping:
    where = MAPPING_FILE
    document = _resolve(_MAPPING, document, where)
    variables = model.variables
    constants = model.constants
    defined = model.definitions

    state: dict[str, StateVar] = {}
    raw_state = _resolve(_named_only(variables), document["state"], f"{where}.state")
    for name in variables:
        if name not in raw_state:
            raise SpecBundleError(f"{where}.state must map every model variable; {name!r} is missing")
        spec = raw_state[name]
        if "hidden" in spec:
            spec = _resolve(_HIDDEN_STATE, spec, f"{where}.state.{name}")
            state[name] = StateVar(name, (), None, str(spec["hidden"]))
            continue
        spec = _resolve(_VISIBLE_STATE, spec, f"{where}.state.{name}")
        over = _texts(spec.get("over") or [], f"{where}.state.{name}.over")
        state[name] = StateVar(name, over, spec["init"], None)

    processes: dict[str, ProcessSet] = {}
    for name, spec in (document.get("processes") or {}).items():
        at = f"{where}.processes.{name}"
        if name not in constants:
            raise SpecBundleError(f"{at}: {name!r} is not a model constant")
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9]+", spec["prefix"]):
            # TLC reads a model value named <letter>_... as typed, so prefixes need two characters.
            raise SpecBundleError(f"{at}.prefix must be letters and digits, at least two")
        members = tuple(
            MemberRule(
                evidence=rule["evidence"],
                field=rule["field"],
                where=rule.get("where") or {},
                require=tuple(rule.get("require") or ()),
            )
            for rule in spec["members"] or []
        )
        if not members:
            raise SpecBundleError(f"{at}.members must say which nodes belong to {name}")
        processes[name] = ProcessSet(name=name, prefix=spec["prefix"], members=members)
    for var in state.values():
        unknown = set(var.over) - set(processes)
        if unknown:
            raise SpecBundleError(f"{where}.state.{var.name}.over names unknown process sets {sorted(unknown)}")

    evidence: dict[str, Evidence] = {}
    sample = _parse_sample(document["sample"], f"{where}.sample") if document.get("sample") is not None else None
    for name, spec in document["evidence"].items():
        at = f"{where}.evidence.{name}"
        if ("event" in spec) == bool(spec.get("sample")):
            raise SpecBundleError(f"{at} needs exactly one of event (a fuzzer event name or glob) or sample: true")
        if "component" in spec and spec.get("sample"):
            raise SpecBundleError(f"{at}.component names the experiment component that publishes the event")
        log = spec.get("log")
        if log is not None:
            if not spec.get("event") or any(char in spec["event"] for char in "*?["):
                raise SpecBundleError(f"{at}.log needs a plain event name to publish the matched line as")
            try:
                groups = set(re.compile(log).groupindex)
            except re.error as exc:
                raise SpecBundleError(f"{at}.log is not a valid regex: {exc}") from exc
        elif "logged_by" in spec:
            raise SpecBundleError(f"{at}.logged_by says who logs the line, so it needs log")
        fields = _parse_fields(spec["fields"], f"{at}.fields")
        after = spec.get("after")
        taken = set(_texts(after.get("take") or [], f"{at}.after.take")) if after else set()
        if log is not None:
            # A taken field comes from the record this one follows, not from the line.
            missing = [
                item.name
                for item in fields.values()
                if item.name not in taken and item.path.split(".")[0] not in groups | {"instance_id"}
            ]
            if missing:
                raise SpecBundleError(f"{at}.fields {missing} are not captured by the log pattern")
        if spec.get("sample"):
            _check_sample_fields(at, fields, spec.get("where") or {}, sample)
        action = spec.get("action")
        if action is not None and action not in defined:
            raise SpecBundleError(f"{at}.action: {action!r} is not defined in {MODEL_FILE}")
        step = spec.get("step") or {}
        for key in ("changes", "unchanged"):
            if key in step and step[key] not in variables:
                raise SpecBundleError(f"{at}.step.{key}: {step[key]!r} is not a model variable")
        if "process" in step:
            step["process"] = list(_texts(step["process"], f"{at}.step.process"))
        updates = tuple(
            _parse_update(item, f"{at}.updates[{index}]", set(state))
            for index, item in enumerate(spec.get("updates") or [])
        )
        for update in updates:
            for ref, process in field_refs([update.key, update.value]):
                if ref not in fields:
                    raise SpecBundleError(f"{at}.updates use ${ref}, which is not one of its fields")
                if process is not None and process not in processes:
                    raise SpecBundleError(f"{at}.updates use @{process}, which is not a process set")
        evidence[name] = Evidence(
            name=name,
            event=spec.get("event"),
            sample=bool(spec.get("sample")),
            log=log,
            where=spec.get("where") or {},
            fields=fields,
            action=action,
            step=step,
            updates=updates,
            component=spec.get("component"),
            after=After(after["evidence"], after["match"], tuple(sorted(taken))) if after else None,
            logged_by=spec.get("logged_by", "any"),
        )
    for item in evidence.values():
        if item.after is None:
            continue
        at = f"{where}.evidence.{item.name}.after"
        if item.after.evidence not in evidence:
            raise SpecBundleError(f"{at}.evidence: {item.after.evidence!r} is not evidence of the mapping")
        if not item.after.match:
            raise SpecBundleError(f"{at}.match must say which {item.after.evidence} record this one follows")
        for name, spec in item.after.match.items():
            if name not in evidence[item.after.evidence].fields:
                raise SpecBundleError(f"{at}.match: {item.after.evidence} has no field {name!r}")
            ref = field_ref(spec)
            if not ref or ref[1] is not None or ref[0] not in item.fields or ref[0] in item.after.take:
                raise SpecBundleError(f"{at}.match.{name} must be $field, one of {item.name}'s own fields")
        for name in item.after.take:
            if name not in evidence[item.after.evidence].fields or name not in item.fields:
                raise SpecBundleError(
                    f"{at}.take: {name!r} must be a field of both {item.after.evidence} and {item.name}"
                )
    for process in processes.values():
        for rule in process.members:
            if rule.evidence not in evidence:
                raise SpecBundleError(f"{where}.processes.{process.name} uses unknown evidence {rule.evidence!r}")
            item = evidence[rule.evidence]
            if rule.field not in item.fields or (item.after is not None and rule.field in item.after.take):
                raise SpecBundleError(f"{where}.processes.{process.name}: {rule.evidence} has no field {rule.field!r}")

    unobservable = document.get("unobservable") or {}
    for action, why in unobservable.items():
        if action not in defined or not why.strip():
            raise SpecBundleError(
                f"{where}.unobservable.{action} must be a model action with the reason it is not observed"
            )
    both = set(unobservable) & {item.action for item in evidence.values() if item.action}
    if both:
        raise SpecBundleError(f"{where}: {sorted(both)} are both observed and unobservable")

    constant_specs: dict[str, dict[str, Any]] = {}
    raw_constants = _resolve(_named_only(constants), document["constants"], f"{where}.constants")
    for name in constants:
        if name in processes:
            continue
        if name not in raw_constants:
            raise SpecBundleError(
                f"{where}.constants must give every model constant that is not a process set; {name!r} is missing"
            )
        spec = raw_constants[name]
        forms = [key for key in ("value", "config", "input", "max_of", "field") if key in spec]
        if len(forms) != 1:
            raise SpecBundleError(f"{where}.constants.{name} needs exactly one of value, config, input, max_of, field")
        for form in ("config", "input"):
            if form in spec and "equals" not in spec:
                raise SpecBundleError(f"{where}.constants.{name}.{form} needs equals")
        if "max_of" in spec and not set(_texts(spec["max_of"], f"{where}.constants.{name}.max_of")) <= set(state):
            raise SpecBundleError(f"{where}.constants.{name}.max_of must name state variables")
        if "field" in spec:
            source = evidence.get(spec["field"]["evidence"])
            if source is None or spec["field"]["name"] not in source.fields:
                raise SpecBundleError(
                    f"{where}.constants.{name}.field must name an evidence entry and one of its fields"
                )
            if source.fields[spec["field"]["name"]].type != "int":
                raise SpecBundleError(f"{where}.constants.{name}.field must name an int field")
        constant_specs[name] = spec

    scenario = document.get("scenario")
    if scenario is not None:
        _check_scenario(scenario, f"{where}.scenario", evidence, constants, defined)
    return SpecMapping(
        path=path,
        sha256=sha256,
        evidence=evidence,
        unobservable=unobservable,
        processes=processes,
        state=state,
        constants=constant_specs,
        scenario=scenario,
        sample=sample,
    )


def _check_sample_fields(
    at: str, fields: Mapping[str, FieldSpec], where: Mapping[str, Any], sample: Optional[SampleSpec]
) -> None:
    """A sample evidence reads (its fields and its ``where`` filter) only what the mapping's ``sample:`` section has
    the trace observer record."""

    if sample is None:
        raise SpecBundleError(f"{at} is a sample, but the mapping has no sample section saying what to record")
    for path in [item.path for item in fields.values()] + list(where):
        top = path.split(".")[0]
        if top not in _SAMPLE_PATHS:
            raise SpecBundleError(f"{at}: {path!r} is not a field of a sample record ({sorted(_SAMPLE_PATHS)})")
        part = _SAMPLE_PATHS[top]
        if part is not None and not getattr(sample, part):
            raise SpecBundleError(f"{at} reads {path!r}, which the sample section does not record (sample.{part})")


def _check_scenario(
    scenario: Mapping[str, Any],
    where: str,
    evidence: Mapping[str, Evidence],
    constants: Sequence[str],
    defined: AbstractSet[str],
) -> None:
    if "mode" not in scenario["deployment"]:
        raise SpecBundleError(f"{where}.deployment is missing required field 'mode'")
    for name, spec in list((scenario.get("inputs") or {}).items()) + list(scenario["deployment"].items()):
        if isinstance(spec, Mapping):
            spec = _resolve(_FROM_CONSTANT, spec, f"{where}.{name}")
            ref = spec.get("constant", spec.get("count"))
            if ref not in constants:
                raise SpecBundleError(f"{where}.{name} refers to {ref!r}, which is not a model constant")
    for index, fault in enumerate(scenario.get("faults") or []):
        at = f"{where}.faults[{index}]"
        if not fault["component"]:
            raise SpecBundleError(f"{at}.component must name the experiment's fault component that injects it")
        if fault["action"] not in defined:
            raise SpecBundleError(f"{at}.action: {fault['action']!r} is not defined in {MODEL_FILE}")
        if fault["after"] not in evidence or not evidence[fault["after"]].action:
            raise SpecBundleError(f"{at}.after must name evidence of a model action")
        if fault.get("target") == "same-process" and not (
            fault.get("process") and evidence[fault["after"]].step.get("process")
        ):
            raise SpecBundleError(
                f"{at}: same-process needs process (model variables) here and step.process on {fault['after']}"
            )
    variants = scenario.get("variants") or {}
    if variants.get("occurrence_shift") and not all(
        "occurrence_input" in fault for fault in scenario.get("faults") or []
    ):
        raise SpecBundleError(f"{where}.variants.occurrence_shift needs an occurrence_input on every fault")
    if any(shift == 0 for shift in variants.get("occurrence_shift") or []):
        raise SpecBundleError(f"{where}.variants.occurrence_shift must list non-zero integers")
    for name, values in (variants.get("inputs") or {}).items():
        if not values:
            raise SpecBundleError(f"{where}.variants.inputs.{name} must list values")
    injected = {fault["action"] for fault in scenario.get("faults") or []}
    for action, why in (scenario.get("unmapped") or {}).items():
        if action not in defined or not why.strip():
            raise SpecBundleError(
                f"{where}.unmapped.{action} must be a model action with the reason it is not injected"
            )
        if action in injected:
            raise SpecBundleError(f"{where}.unmapped.{action} is also a fault of the scenario")


# ---------------------------------------------------------------------------
# The bundle
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class NegativeControl:
    name: str
    config: str
    module: Optional[str]  # a mutated module, or None for model.tla
    violates: str


@dataclass(frozen=True)
class Bundle:
    path: str
    claims: Claims
    mapping: SpecMapping
    model_text: str
    safe_configs: tuple[str, ...]
    controls: tuple[NegativeControl, ...]
    files: dict[str, str] = field(default_factory=dict)

    @property
    def protocol(self) -> str:
        return self.claims.protocol

    @property
    def experiment_directory(self) -> Optional[str]:
        """The bundle's own ``experiments/`` directory, when it has one."""

        path = Path(self.path) / EXPERIMENTS_DIRECTORY
        return str(path) if path.is_dir() else None

    def property(self, name: str) -> Property:
        return next(item for item in self.claims.properties if item.name == name)

    def control(self, name: str) -> NegativeControl:
        for control in self.controls:
            if control.name == name:
                return control
        raise SpecBundleError(f"{self.path} has no negative control {name!r}")


def _read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        raise SpecBundleError(f"cannot read {path}: {exc}") from exc


def _stamps(path: Path, *names: str) -> tuple[tuple[int, int], ...]:
    """The modification time and size of each of the bundle files ``names``: a cache key that changes when they do."""

    try:
        return tuple(file_stamp(path / name) for name in names)
    except OSError as exc:
        raise SpecBundleError(f"cannot read {exc.filename}: {exc}") from exc


def load_spec_mapping(reference: str) -> SpecMapping:
    """The mapping of the bundle ``reference`` names (see :func:`find_bundle`).

    Every compile of an experiment that names a bundle checks its mapping,
    once per component, so a parsed mapping is reused while neither file
    changes (same size and modification time); treat it as read-only.
    """

    path = find_bundle(reference)
    return _cached_mapping(path, _stamps(path, MODEL_FILE, MAPPING_FILE))


@functools.lru_cache(maxsize=64)
def _cached_mapping(path: Path, stamps: tuple[tuple[int, int], ...]) -> SpecMapping:
    return _mapping(path, _read(path / MODEL_FILE))


def load_spec_claims(reference: str) -> Claims:
    """The claims of the bundle ``reference`` names, reused while claims.yaml and model.tla do not change, as
    :func:`load_spec_mapping` reuses the mapping; treat them as read-only."""

    path = find_bundle(reference)
    return _cached_claims(path, _stamps(path, MODEL_FILE, CLAIMS_FILE))


@functools.lru_cache(maxsize=64)
def _cached_claims(path: Path, stamps: tuple[tuple[int, int], ...]) -> Claims:
    return parse_claims(_yaml(path / CLAIMS_FILE)[1], tla.symbols(_read(path / MODEL_FILE)))


def _yaml(path: Path) -> tuple[str, Mapping[str, Any]]:
    text = _read(path)
    try:
        return text, load_yaml_mapping(text, source_name=str(path))
    except ValueError as exc:
        raise SpecBundleError(str(exc)) from exc


def _mapping(path: Path, model_text: str) -> SpecMapping:
    text, document = _yaml(path / MAPPING_FILE)
    return parse_mapping(
        document,
        tla.symbols(model_text),
        path=str(path / MAPPING_FILE),
        sha256=sha256_text(text),
    )


def _check_mutant(path: str, text: str, model_text: str, violates: str) -> None:
    """A mutated control module must check model.tla's own ``violates``, not one of its own making.

    Otherwise a control could define the property as ``FALSE``, "find" the
    violation, and pass while the model's property is vacuous. So the
    property, and every model operator it uses, must be the model's
    definition word for word, or come from ``EXTENDS model``.
    """

    model = tla.definitions(model_text)
    own = tla.definitions(text)
    if violates not in own and "model" not in tla.module_references(text):
        raise SpecBundleError(f"{path} defines no {violates}: keep model.tla's definition, or EXTENDS model")
    used, todo = set(), [violates]
    while todo:
        name = todo.pop()
        if name in used or name not in model:
            continue
        used.add(name)
        todo.extend(re.findall(r"[A-Za-z_]\w*", model[name].split("==", 1)[1]))
    for name in sorted(used):
        if name in own and own[name] != model[name]:
            raise SpecBundleError(
                f"{path}: {name} differs from its definition in model.tla, so the control would not check the "
                f"model's {violates}; mutate the actions, and keep {violates} and what it uses as they are"
            )


def load_bundle(reference: str) -> Bundle:
    """Read and check the bundle ``reference`` names; raise :class:`SpecBundleError` on the first problem."""

    path = find_bundle(reference)
    model_text = _read(path / MODEL_FILE)
    if tla.module_name(model_text) != "model":
        raise SpecBundleError(f"{path / MODEL_FILE} must declare MODULE model")
    claims = load_spec_claims(str(path))
    mapping = load_spec_mapping(str(path))
    safe = tuple(sorted(item.name for item in path.glob("*.cfg")))
    if not safe:
        raise SpecBundleError(f"{path} has no expected-safe .cfg")
    controls_dir = path / CONTROLS_DIRECTORY
    available = {item.stem for item in controls_dir.glob("*.cfg")} if controls_dir.is_dir() else set()
    controls = []
    referenced = set()
    for item in claims.properties:
        for name in item.negative_controls:
            if name not in available:
                raise SpecBundleError(
                    f"{CLAIMS_FILE}: {item.name} names negative control {name!r}, but {CONTROLS_DIRECTORY}/{name}.cfg is missing"
                )
            if name in referenced:
                raise SpecBundleError(
                    f"{CLAIMS_FILE}: negative control {name!r} is named by two properties; a control violates one"
                )
            referenced.add(name)
            module = controls_dir / f"{name}.tla"
            controls.append(
                NegativeControl(
                    name=name,
                    config=f"{CONTROLS_DIRECTORY}/{name}.cfg",
                    module=f"{CONTROLS_DIRECTORY}/{name}.tla" if module.is_file() else None,
                    violates=item.name,
                )
            )
    unused = available - referenced
    if unused:
        raise SpecBundleError(f"{CONTROLS_DIRECTORY}/ has controls no property names: {sorted(unused)}")
    files = {MODEL_FILE: model_text}
    for name in safe:
        files[name] = _read(path / name)
    for control in controls:
        files[control.config] = _read(path / control.config)
        if control.module:
            files[control.module] = _read(path / control.module)
            _check_mutant(control.module, files[control.module], model_text, control.violates)
    return Bundle(
        path=str(path),
        claims=claims,
        mapping=mapping,
        model_text=model_text,
        safe_configs=safe,
        controls=tuple(controls),
        files=files,
    )
