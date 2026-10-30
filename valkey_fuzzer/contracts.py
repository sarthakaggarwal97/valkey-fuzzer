"""Extension contracts: experiments, execution plans, results, components, providers, and plugins.

These are the neutral models the compiler and runner share. What an
experiment tests lives entirely in registered components and providers;
nothing here knows an individual experiment, evidence ID, or policy type. The
helpers at the end build component results and evidence and locate run
artifact directories.
"""

import hashlib
import os
import random
import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import MISSING, dataclass, field, fields
from enum import Enum, IntEnum
from types import MappingProxyType
from typing import Any, Generic, Optional, Protocol, TypeVar, runtime_checkable

from ._data import (
    as_tuple,
    canonical_json,
    freeze,
    freeze_mapping,
    jsonable,
    references_from_data,
    require_name,
    require_number,
    require_qualified_id,
    require_text,
    require_type,
    stable_order,
    strict_fields,
    to_plain_data,
    unique_tuple,
)
from .environment import (
    ArtifactProviderRegistration,
    ArtifactSpec,
    PolicySpec,
    ResourceProviderRegistration,
    ResourceSpec,
)
from .events import EventBus
from .obligations import ObligationLedger
from .schema import ConfigError, ConfigField, ConfigSchema, plain_config

PLUGIN_ENTRY_POINT_GROUP = "valkey_fuzzer.plugins"


class ComponentRole(str, Enum):
    """The role a component is registered for, which decides when the runner calls it."""

    CAPABILITY = "capability"
    WORKLOAD = "workload"
    ACTION = "action"
    FAULT = "fault"
    OBSERVER = "observer"
    ORACLE = "oracle"


class ComponentStatus(str, Enum):
    """Terminal result of one component invocation."""

    PASS = "PASS"
    FAIL = "FAIL"
    SKIP = "SKIP"
    ERROR = "ERROR"


class OutcomeStatus(str, Enum):
    """Terminal, trustworthy classification for a complete run."""

    PASS = "PASS"
    FAIL = "FAIL"
    SKIP_UNSUPPORTED = "SKIP_UNSUPPORTED"
    ERROR = "ERROR"


class RunMode(str, Enum):
    """``experiment`` runs make coverage claims; ``smoke`` runs only exercise the deployment lifecycle."""

    EXPERIMENT = "experiment"
    SMOKE = "smoke"


def _check_times(started_at: Any, ended_at: Any) -> None:
    for value, field_name in ((started_at, "started_at"), (ended_at, "ended_at")):
        if value is not None:
            require_number(value, field_name)
    if started_at is not None and ended_at is not None and ended_at < started_at:
        raise ValueError("ended_at cannot be earlier than started_at")


def _from_data(cls: type, value: Any, field_name: str, **convert: Callable[[Any], Any]) -> Any:
    """Build dataclass ``cls`` from its canonical data form, rejecting unknown fields."""

    init_fields = [item for item in fields(cls) if item.init]
    required = [item.name for item in init_fields if item.default is MISSING and item.default_factory is MISSING]
    mapping = strict_fields(value, field_name, allowed=[item.name for item in init_fields], required=required)
    kwargs = dict(mapping)
    for name, converter in convert.items():
        if name in kwargs:
            kwargs[name] = converter(kwargs[name])
    return cls(**kwargs)


def _many(cls: type, field_name: str, **convert: Callable[[Any], Any]) -> Callable[[Any], tuple[Any, ...]]:
    return lambda items: tuple(_from_data(cls, item, field_name, **convert) for item in as_tuple(items, field_name))


# ---------------------------------------------------------------------------
# Experiments and execution plans
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ComponentSpec:
    """One configured use of a registered component in an experiment.

    ``use`` names the registration and ``id`` this use of it, unique within
    the experiment, as in experiment YAML. ``role`` comes from the
    registration: an experiment may leave it out, the compiler fills it in,
    and every component of an execution plan has one.
    """

    use: str
    id: str
    role: Optional[ComponentRole] = None
    config: Mapping[str, Any] = field(default_factory=dict)
    required: bool = True
    depends_on: tuple[str, ...] = ()
    timeout_s: Optional[float] = None

    def __post_init__(self) -> None:
        if self.role is not None:
            object.__setattr__(self, "role", ComponentRole(self.role))
        require_qualified_id(self.use, "use")
        require_name(self.id, "id")
        object.__setattr__(self, "config", freeze_mapping(self.config, "component config", references=True))
        depends_on = unique_tuple(self.depends_on, f"component {self.id!r} depends_on")
        if self.id in depends_on:
            raise ValueError(f"component {self.id!r} cannot depend on itself")
        object.__setattr__(self, "depends_on", depends_on)
        if self.timeout_s is not None:
            require_number(self.timeout_s, "timeout_s", positive=True)
        require_type(self.required, bool, "required")


# The selector words, as (roles, first): "primary" and "replica" pick one instance.
_SELECTOR_WORDS = {
    "all": ((), False),
    "primaries": (("primary",), False),
    "primary": (("primary",), True),
    "replicas": (("replica",), False),
    "replica": (("replica",), True),
}


@dataclass(frozen=True)
class InstanceSelector:
    """Instances of a deployment, by role, by instance ID, or both.

    Empty criteria select every instance. Values within a field are
    alternatives; non-empty fields combine with AND. ``first`` keeps only the
    first selected instance. :meth:`parse` reads the forms scenarios and
    component configs use, and :meth:`resolve` selects from a deployment.
    """

    roles: tuple[str, ...] = ()
    instance_ids: tuple[str, ...] = ()
    first: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "roles", unique_tuple(self.roles, "selector roles"))
        object.__setattr__(self, "instance_ids", unique_tuple(self.instance_ids, "selector instance_ids"))
        require_type(self.first, bool, "selector first")

    @classmethod
    def parse(cls, value: Any) -> "InstanceSelector":
        """``all``, ``primaries``, ``primary``, ``replicas``, ``replica``, an instance ID,
        a non-empty list of instance IDs, or ``{roles, instance_ids, first}``."""

        if isinstance(value, InstanceSelector):
            return value
        if isinstance(value, str):
            if value in _SELECTOR_WORDS:
                roles, first = _SELECTOR_WORDS[value]
                return cls(roles=roles, first=first)
            return cls(instance_ids=(value,))
        if isinstance(value, Mapping):
            return cls(**strict_fields(value, "instance selector", allowed=("roles", "instance_ids", "first")))
        if isinstance(value, (list, tuple)) and value and all(isinstance(item, str) for item in value):
            return cls(instance_ids=tuple(value))
        raise ValueError(
            f"an instance selector is one of {list(_SELECTOR_WORDS)}, an instance ID, a non-empty list of "
            f"instance IDs, or a mapping of roles and instance_ids; got {value!r}"
        )

    def resolve(self, instance_ids: Sequence[str], roles: Mapping[str, str]) -> tuple[str, ...]:
        """The selected instances among ``instance_ids``; ``roles`` maps an instance ID to its role.

        Instances come in ``instance_ids`` order, except that IDs selected by
        ID alone come in the order the selector lists them. An unknown
        instance ID is an error; a role no instance has selects nothing.
        """

        known = set(instance_ids)
        unknown = [item for item in self.instance_ids if item not in known]
        if unknown:
            raise ValueError(f"unknown instance IDs {unknown}")
        if self.instance_ids and not self.roles:
            selected = self.instance_ids
        else:
            selected = tuple(
                item
                for item in instance_ids
                if (not self.roles or roles.get(item) in self.roles)
                and (not self.instance_ids or item in self.instance_ids)
            )
        return selected[:1] if self.first else selected


@dataclass(frozen=True)
class StartupOverlay:
    """Pre-start server configuration for selected instances.

    ``conflict_policy`` is ``override`` (replace earlier values) or ``error``
    (reject a different value already set by the deployment or an earlier overlay).
    """

    overlay_id: str
    selector: InstanceSelector = field(default_factory=InstanceSelector)
    server_config: Mapping[str, Any] = field(default_factory=dict)
    conflict_policy: str = "override"

    def __post_init__(self) -> None:
        require_name(self.overlay_id, "overlay_id")
        require_type(self.selector, InstanceSelector, "selector")
        server_config = freeze_mapping(self.server_config, "startup overlay server_config", references=True)
        if not server_config:
            raise ValueError("a startup overlay must provide server_config")
        object.__setattr__(self, "server_config", server_config)
        require_name(self.conflict_policy, "conflict_policy")


@dataclass(frozen=True)
class EvidenceRequirement:
    """Minimum trusted evidence an experiment verdict needs."""

    evidence_id: str
    source: Optional[str] = None
    min_count: int = 1

    def __post_init__(self) -> None:
        require_qualified_id(self.evidence_id, "evidence_id")
        if self.source is not None:
            require_name(self.source, "source")
        if require_number(self.min_count, "min_count", integer=True) < 1:
            raise ValueError("min_count must be an integer greater than or equal to one")


def stable_topological_order(components: Sequence[Any]) -> tuple[Any, ...]:
    """Dependency order that preserves declaration order whenever possible."""

    entries = tuple(components)
    known = {component.id for component in entries}
    if len(known) != len(entries):
        raise ValueError("component IDs must be unique")
    for component in entries:
        unknown = [dependency for dependency in component.depends_on if dependency not in known]
        if unknown:
            raise ValueError(f"component {component.id!r} has unknown dependencies: {unknown}")
    by_id = {component.id: component for component in entries}
    order = stable_order(tuple(by_id), {component.id: component.depends_on for component in entries}, "component")
    return tuple(by_id[component_id] for component_id in order)


def _validate_composition(spec: Any) -> tuple[ComponentSpec, ...]:
    """Checks shared by an experiment and the plan compiled from it; returns its components in dependency order."""

    object.__setattr__(spec, "mode", RunMode(spec.mode))
    components = as_tuple(spec.components, "components")
    for component in components:
        require_type(component, ComponentSpec, "component")
    ordered = stable_topological_order(components)
    object.__setattr__(spec, "components", components)
    component_ids = {component.id for component in components}

    overlays = as_tuple(spec.startup_overlays, "startup_overlays")
    for overlay in overlays:
        require_type(overlay, StartupOverlay, "startup overlay")
    unique_tuple([overlay.overlay_id for overlay in overlays], "startup overlay_id")
    object.__setattr__(spec, "startup_overlays", overlays)
    object.__setattr__(spec, "required_events", unique_tuple(spec.required_events, "required_events"))

    requirements = as_tuple(spec.required_evidence, "required_evidence")
    keys = set()
    for requirement in requirements:
        require_type(requirement, EvidenceRequirement, "evidence requirement")
        key = (requirement.evidence_id, requirement.source)
        if key in keys:
            raise ValueError(
                f"duplicate evidence requirement for {requirement.evidence_id!r} from {requirement.source!r}"
            )
        keys.add(key)
        if requirement.source is not None and requirement.source not in component_ids:
            raise ValueError(
                f"evidence requirement {requirement.evidence_id!r} references unknown source component "
                f"{requirement.source!r}"
            )
    object.__setattr__(spec, "required_evidence", requirements)
    object.__setattr__(spec, "metadata", freeze_mapping(spec.metadata, "metadata"))
    return ordered


def _check_stages(components: Sequence[ComponentSpec]) -> None:
    """Reject a component that depends on one the runner calls at a later stage, which it could never wait for."""

    roles = {component.id: component.role for component in components}
    for component in components:
        stage = ROLES[component.role].stage
        for dependency_id in component.depends_on:
            if ROLES[roles[dependency_id]].stage <= stage:
                continue
            if stage is Stage.PROBE:
                raise ValueError(
                    f"capability {component.id!r} cannot depend on post-provision component {dependency_id!r}"
                )
            raise ValueError(f"component {component.id!r} cannot depend on post-execution oracle {dependency_id!r}")


@dataclass(frozen=True)
class ExperimentSpec:
    """Declarative description of an experiment.

    ``deployment_modes`` are the deployment modes it supports, and ``inputs``
    is the schema of the inputs a scenario passes it.
    ``deployment_requirements`` maps deployment fields to rules (``type``,
    ``enum``, ``minimum``, ``maximum``, as in ``inputs``) that the deployment
    document, with its provider's defaults filled in, must satisfy; for
    example ``{"replicas_per_primary": {"minimum": 1}}``. In ``experiment`` mode
    the compiled plan must have a required oracle and require some evidence:
    listed in ``required_evidence`` or derived from components whose
    registration declares the evidence their configuration marks as required.
    """

    experiment_id: str
    deployment_modes: tuple[str, ...]
    components: tuple[ComponentSpec, ...]
    mode: RunMode = RunMode.EXPERIMENT
    inputs: Mapping[str, Any] = field(default_factory=dict)
    deployment_requirements: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    startup_overlays: tuple[StartupOverlay, ...] = ()
    required_events: tuple[str, ...] = ()
    required_evidence: tuple[EvidenceRequirement, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_qualified_id(self.experiment_id, "experiment_id")
        modes = unique_tuple(self.deployment_modes, "deployment_modes")
        if not modes:
            raise ValueError("deployment_modes must not be empty")
        object.__setattr__(self, "deployment_modes", modes)
        requirements = freeze_mapping(self.deployment_requirements, "deployment_requirements")
        for name, rule in requirements.items():
            require_name(name, "deployment requirement field")
            if name in ("mode", "modes", "provider") or not isinstance(rule, Mapping):
                raise ValueError(f"deployment requirement {name!r} must name a deployment field and map to a rule")
        object.__setattr__(self, "deployment_requirements", requirements)
        object.__setattr__(self, "inputs", freeze_mapping(self.inputs, "inputs"))
        _validate_composition(self)


@dataclass(frozen=True)
class DeploymentProviderIdentity:
    """Replay lock for one deployment-provider implementation.

    ``implementation_digest``, when the provider declares one, locks the plan
    to that build of it; the runner refuses a plan whose identity differs.
    """

    provider_id: str
    implementation_digest: Optional[str] = None

    def __post_init__(self) -> None:
        require_qualified_id(self.provider_id, "provider_id")
        if self.implementation_digest is not None:
            require_text(self.implementation_digest, "implementation_digest")


def derive_component_seed(seed: int, component_id: str, stream: str = "default") -> int:
    """A stable seed that does not depend on Python's process-randomized hash."""

    require_number(seed, "seed", integer=True)
    require_name(component_id, "component_id")
    require_name(stream, "stream")
    material = f"{seed}\0{component_id}\0{stream}".encode()
    return int.from_bytes(hashlib.sha256(material).digest()[:16], "big")


@dataclass(frozen=True)
class ExecutionPlan:
    """A fully compiled, replayable run.

    The compiled plan keeps typed environment references; the runner executes
    a copy in which every reference is replaced by the acquired output. Only
    the compiled plan is ever reported or persisted.

    ``instances`` maps each instance the deployment provider plans to its
    ``role``, ``shard``, and effective ``server_config``: the provider's
    defaults under the deployment's own config and the startup overlays.
    Compile-time checks read it rather than the deployment document, so a
    default counts the same as a setting written out; the runner refuses a
    plan whose provider no longer plans it that way.
    """

    scenario_id: str
    experiment_id: str
    seed: int
    deployment_mode: str
    components: tuple[ComponentSpec, ...]
    deployment_provider: DeploymentProviderIdentity
    mode: RunMode = RunMode.EXPERIMENT
    inputs: Mapping[str, Any] = field(default_factory=dict)
    artifacts: tuple[ArtifactSpec, ...] = ()
    resources: tuple[ResourceSpec, ...] = ()
    policies: tuple[PolicySpec, ...] = ()
    startup_overlays: tuple[StartupOverlay, ...] = ()
    required_events: tuple[str, ...] = ()
    required_evidence: tuple[EvidenceRequirement, ...] = ()
    deployment: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)
    instances: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_name(self.scenario_id, "scenario_id")
        require_qualified_id(self.experiment_id, "experiment_id")
        require_number(self.seed, "seed", integer=True)
        require_name(self.deployment_mode, "deployment_mode")
        require_type(self.deployment_provider, DeploymentProviderIdentity, "deployment_provider")
        ordered = _validate_composition(self)
        unassigned = [component.id for component in self.components if component.role is None]
        if unassigned:
            raise ValueError(f"execution plan components need a role: {unassigned}")
        _check_stages(self.components)
        # Not a field: never serialized or compared, and rebuilt whenever the plan is.
        object.__setattr__(self, "_ordered_components", ordered)
        if self.mode is RunMode.EXPERIMENT and not any(
            component.role is ComponentRole.ORACLE and component.required for component in self.components
        ):
            raise ValueError("experiment mode requires at least one required oracle")
        if self.mode is RunMode.EXPERIMENT and not self.required_evidence:
            raise ValueError("experiment mode requires at least one required_evidence requirement")
        object.__setattr__(self, "inputs", freeze_mapping(self.inputs, "inputs", references=True))
        for name, spec_type, id_field in (
            ("artifacts", ArtifactSpec, "artifact_id"),
            ("resources", ResourceSpec, "resource_id"),
            ("policies", PolicySpec, "policy_id"),
        ):
            specs = as_tuple(getattr(self, name), name)
            for spec in specs:
                require_type(spec, spec_type, f"{name} entry")
            unique_tuple([getattr(spec, id_field) for spec in specs], f"{name} {id_field}")
            object.__setattr__(self, name, specs)
        if not isinstance(self.deployment, Mapping):
            raise TypeError("deployment must be a mapping")
        document = freeze_mapping(self.deployment or {"mode": self.deployment_mode}, "deployment", references=True)
        if document.get("mode") != self.deployment_mode:
            raise ValueError("deployment.mode must match deployment_mode")
        object.__setattr__(self, "deployment", document)
        instances = freeze_mapping(self.instances, "instances", references=True)
        for instance_id, instance in instances.items():
            require_name(instance_id, "instance ID")
            if not isinstance(instance, Mapping) or not isinstance(instance.get("server_config"), Mapping):
                raise ValueError(f"instances.{instance_id} must be a mapping with a server_config mapping")
        object.__setattr__(self, "instances", instances)

    @classmethod
    def from_experiment(
        cls,
        spec: ExperimentSpec,
        *,
        scenario_id: str,
        seed: int,
        deployment_mode: str,
        deployment_provider: DeploymentProviderIdentity,
        inputs: Optional[Mapping[str, Any]] = None,
        deployment: Optional[Mapping[str, Any]] = None,
        artifacts: Sequence[ArtifactSpec] = (),
        resources: Sequence[ResourceSpec] = (),
        policies: Sequence[PolicySpec] = (),
    ) -> "ExecutionPlan":
        require_type(spec, ExperimentSpec, "spec")
        return cls(
            scenario_id=scenario_id,
            experiment_id=spec.experiment_id,
            seed=seed,
            deployment_mode=deployment_mode,
            components=spec.components,
            deployment_provider=deployment_provider,
            mode=spec.mode,
            inputs={} if inputs is None else inputs,
            artifacts=tuple(artifacts),
            resources=tuple(resources),
            policies=tuple(policies),
            startup_overlays=spec.startup_overlays,
            required_events=spec.required_events,
            required_evidence=spec.required_evidence,
            deployment={"mode": deployment_mode} if deployment is None else deployment,
            metadata=spec.metadata,
        )

    @property
    def ordered_components(self) -> tuple[ComponentSpec, ...]:
        """The components in the order the runner starts them: after their dependencies, else as declared."""

        return self._ordered_components

    def rng(self, component_id: str, stream: str = "default") -> random.Random:
        """A new generator at the start of one component stream.

        Components should draw from ``RunContext.rng``, which keeps one
        continuing generator per stream for the whole run.
        """

        if component_id not in {component.id for component in self.components}:
            raise ValueError(f"unknown component id: {component_id!r}")
        return random.Random(derive_component_seed(self.seed, component_id, stream))

    def to_data(self) -> Mapping[str, Any]:
        return to_plain_data(self)

    def to_json(self) -> str:
        return canonical_json(self)

    @classmethod
    def from_data(cls, value: Mapping[str, Any]) -> "ExecutionPlan":
        """Rebuild a validated plan from its canonical data form."""

        def selector(item: Any) -> InstanceSelector:
            return _from_data(InstanceSelector, item, "instance selector")

        return _from_data(
            cls,
            references_from_data(value),
            "execution plan",
            components=_many(ComponentSpec, "component"),
            deployment_provider=lambda item: _from_data(
                DeploymentProviderIdentity, item, "deployment provider identity"
            ),
            artifacts=_many(ArtifactSpec, "artifact spec"),
            resources=_many(ResourceSpec, "resource spec"),
            policies=_many(PolicySpec, "policy spec"),
            startup_overlays=_many(StartupOverlay, "startup overlay", selector=selector),
            required_evidence=_many(EvidenceRequirement, "evidence requirement"),
        )


# ---------------------------------------------------------------------------
# Results and outcomes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Evidence:
    """Immutable path evidence or diagnostic observation."""

    evidence_id: str
    source: str
    phase: str
    observed_at: float
    value: Any
    event_sequence: Optional[int] = None
    artifacts: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_qualified_id(self.evidence_id, "evidence_id")
        require_name(self.source, "source")
        require_name(self.phase, "phase")
        require_number(self.observed_at, "observed_at")
        if self.event_sequence is not None and require_number(self.event_sequence, "event_sequence", integer=True) < 1:
            raise ValueError("event_sequence must be a positive integer")
        artifacts = as_tuple(self.artifacts, "artifacts")
        for artifact in artifacts:
            require_text(artifact, "artifact")
        object.__setattr__(self, "artifacts", artifacts)
        object.__setattr__(self, "value", freeze(self.value, "evidence value"))
        object.__setattr__(self, "metadata", freeze_mapping(self.metadata, "evidence metadata"))


@dataclass(frozen=True)
class ComponentResult:
    """Terminal result emitted by one component of a plan.

    ``unobserved`` names the evidence IDs the component could not look for
    (the server did not answer, a log could not be read): their absence is no
    finding about the system under test, so a run missing them as required
    evidence is ``ERROR``, not ``FAIL``. ``preconditions`` maps an evidence ID
    to the evidence that must have been recorded for its absence to be a
    finding: without it the run never reached the state the evidence is about.
    """

    component_id: str
    status: ComponentStatus
    summary: str
    evidence: tuple[Evidence, ...] = ()
    diagnostics: Mapping[str, Any] = field(default_factory=dict)
    started_at: Optional[float] = None
    ended_at: Optional[float] = None
    unobserved: tuple[str, ...] = ()
    preconditions: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_name(self.component_id, "component_id")
        object.__setattr__(self, "status", ComponentStatus(self.status))
        require_type(self.summary, str, "summary")
        evidence = as_tuple(self.evidence, "evidence")
        for item in evidence:
            require_type(item, Evidence, "evidence entry")
        object.__setattr__(self, "evidence", evidence)
        object.__setattr__(self, "diagnostics", freeze_mapping(self.diagnostics, "diagnostics"))
        _check_times(self.started_at, self.ended_at)
        unobserved = as_tuple(self.unobserved, "unobserved")
        for evidence_id in unobserved:
            require_qualified_id(evidence_id, "unobserved evidence_id")
        object.__setattr__(self, "unobserved", unobserved)
        preconditions = freeze_mapping(self.preconditions, "preconditions")
        for evidence_id, precondition in preconditions.items():
            require_qualified_id(evidence_id, "precondition evidence_id")
            require_qualified_id(precondition, "precondition")
        object.__setattr__(self, "preconditions", preconditions)


@dataclass(frozen=True)
class RunOutcome:
    """Terminal classification and result bundle for a complete run."""

    plan: ExecutionPlan
    status: OutcomeStatus
    summary: str
    component_results: tuple[ComponentResult, ...] = ()
    evidence: tuple[Evidence, ...] = ()
    diagnostics: Mapping[str, Any] = field(default_factory=dict)
    started_at: Optional[float] = None
    ended_at: Optional[float] = None
    # Git SHA of the valkey-server under test, when the deployment provider knows it.
    valkey_sha: Optional[str] = None

    def __post_init__(self) -> None:
        require_type(self.plan, ExecutionPlan, "plan")
        object.__setattr__(self, "status", OutcomeStatus(self.status))
        require_type(self.summary, str, "summary")
        if self.valkey_sha is not None:
            require_type(self.valkey_sha, str, "valkey_sha")
        for name, item_type in (("component_results", ComponentResult), ("evidence", Evidence)):
            items = as_tuple(getattr(self, name), name)
            for item in items:
                require_type(item, item_type, f"{name} entry")
            object.__setattr__(self, name, items)
        object.__setattr__(self, "diagnostics", freeze_mapping(self.diagnostics, "diagnostics"))
        _check_times(self.started_at, self.ended_at)

    @property
    def all_evidence(self) -> tuple[Evidence, ...]:
        return self.evidence + tuple(item for result in self.component_results for item in result.evidence)

    def to_data(self) -> Mapping[str, Any]:
        return to_plain_data(self)

    def to_json(self) -> str:
        return canonical_json(self)

    @classmethod
    def from_data(cls, value: Mapping[str, Any]) -> "RunOutcome":
        """Rebuild a validated outcome from its canonical data form."""

        evidence = _many(Evidence, "evidence")
        return _from_data(
            cls,
            value,
            "run outcome",
            plan=ExecutionPlan.from_data,
            component_results=_many(ComponentResult, "component result", evidence=evidence),
            evidence=evidence,
        )


# ---------------------------------------------------------------------------
# Runtime contracts
# ---------------------------------------------------------------------------


@runtime_checkable
class RunContext(Protocol):
    """Runner-owned services visible to components and deployment providers.

    ``plan`` is the executable plan, with environment references resolved.
    ``services["deployment"]`` holds the provisioned deployment's services.
    ``artifact_dir`` is this run's own folder for the files components write
    (its report folder, when the run has one); see :func:`component_artifact_dir`.
    ``deadline`` is an absolute monotonic time or ``None``: in a component
    call, the call's own deadline (the earlier of the run's and the
    component's ``timeout_s``; in ``stop`` and ``recover``, which ignore the
    run's, the ``timeout_s`` window alone).

    A component call that outlives its timeout is cancelled: on the thread the
    runner called it on, ``cancelled`` becomes true, ``sleep(seconds)``
    returns ``False`` early (``True`` after a full sleep), and ``deadline``
    has passed. Its result is discarded, so it should return promptly.

    ``obligations`` is the run's :class:`~valkey_fuzzer.obligations.ObligationLedger`:
    a component that changes host or server state opens an obligation when it
    applies the change and discharges it when it undoes it.
    """

    @property
    def plan(self) -> ExecutionPlan: ...

    @property
    def event_bus(self) -> EventBus: ...

    def rng(self, component_id: str, stream: str = "default") -> random.Random: ...

    @property
    def services(self) -> Mapping[str, Any]: ...

    @property
    def artifact_dir(self) -> str: ...

    @property
    def deadline(self) -> Optional[float]: ...

    @property
    def cancelled(self) -> bool: ...

    def sleep(self, seconds: float) -> bool: ...

    @property
    def obligations(self) -> ObligationLedger: ...


@dataclass(frozen=True, eq=False)
class ProvisionedDeployment:
    """Runtime handle and services returned by a deployment provider."""

    handle: Any = field(default=None, repr=False, metadata={"serialize": False})
    services: Mapping[str, Any] = field(default_factory=dict, repr=False, metadata={"serialize": False})
    # Report-safe identity of the server build, recorded on the outcome.
    valkey_sha: Optional[str] = None
    # Report-safe provenance (instance layout, binaries, processes) recorded in the
    # outcome diagnostics so a reproducer shows what was actually tested.
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.valkey_sha is not None:
            require_type(self.valkey_sha, str, "valkey_sha")
        object.__setattr__(self, "metadata", freeze_mapping(self.metadata, "deployment metadata"))
        if not isinstance(self.services, Mapping) or any(not isinstance(key, str) for key in self.services):
            raise TypeError("deployment services must be a mapping with string keys")
        object.__setattr__(self, "services", MappingProxyType(dict(self.services)))


@runtime_checkable
class DeploymentProvider(Protocol):
    """Deployment lifecycle implemented by provider plugins.

    ``validate`` is pure. If ``provision`` fails the runner calls ``rollback``;
    after a successful provision it calls ``cleanup`` once with the returned
    deployment.
    """

    def validate(self, plan: ExecutionPlan) -> None: ...

    def provision(self, context: RunContext) -> ProvisionedDeployment: ...

    def rollback(self, context: RunContext) -> None: ...

    def cleanup(self, context: RunContext, deployment: ProvisionedDeployment) -> None: ...


@runtime_checkable
class CapabilityProbe(Protocol):
    def probe(self, context: RunContext, spec: ComponentSpec) -> ComponentResult: ...


@runtime_checkable
class Workload(Protocol):
    def start(self, context: RunContext, spec: ComponentSpec) -> ComponentResult: ...

    def stop(self, context: RunContext, spec: ComponentSpec) -> ComponentResult: ...


@runtime_checkable
class Action(Protocol):
    def run(self, context: RunContext, spec: ComponentSpec) -> ComponentResult: ...


@runtime_checkable
class Fault(Protocol):
    """``inject`` installs the fault's trigger, and the fault is injected when it
    fires; ``recover`` undoes it at teardown and reports whether it was injected."""

    def inject(self, context: RunContext, spec: ComponentSpec) -> ComponentResult: ...

    def recover(self, context: RunContext, spec: ComponentSpec) -> ComponentResult: ...


@runtime_checkable
class Observer(Protocol):
    def start(self, context: RunContext, spec: ComponentSpec) -> ComponentResult: ...

    def stop(self, context: RunContext, spec: ComponentSpec) -> ComponentResult: ...


@runtime_checkable
class Oracle(Protocol):
    def evaluate(self, context: RunContext, spec: ComponentSpec, evidence: Sequence[Evidence]) -> ComponentResult: ...


DeploymentProviderFactory = Callable[[], DeploymentProvider]


class Stage(IntEnum):
    """When the runner calls a role: a component may depend only on components of its stage or an earlier one."""

    # Capabilities are probed before the deployment exists.
    PROBE = 0
    # Observers, workloads, faults, and actions start on the deployment and are torn down before the oracles.
    START = 1
    # Oracles judge the run once everything is torn down.
    EVALUATE = 2


@dataclass(frozen=True)
class RoleLifecycle:
    """How the runner drives one role: the protocol its components implement, the method it calls, at which
    stage, the method that tears it down (if it keeps running), whether its FAIL judges the system under
    test rather than leaving the run without a verdict, and whether it ``witnesses``: looks at the system under
    test, so that required evidence it did not record (and did not report :attr:`ComponentResult.unobserved`)
    is a finding. A workload, fault, or capability that records none never established what the plan needs."""

    protocol: type
    call: str
    stage: Stage
    teardown: Optional[str] = None
    judges: bool = False
    witnesses: bool = False


ROLES: Mapping[ComponentRole, RoleLifecycle] = MappingProxyType(
    {
        ComponentRole.CAPABILITY: RoleLifecycle(CapabilityProbe, "probe", Stage.PROBE),
        ComponentRole.OBSERVER: RoleLifecycle(Observer, "start", Stage.START, teardown="stop", witnesses=True),
        ComponentRole.WORKLOAD: RoleLifecycle(Workload, "start", Stage.START, teardown="stop"),
        ComponentRole.FAULT: RoleLifecycle(Fault, "inject", Stage.START, teardown="recover"),
        ComponentRole.ACTION: RoleLifecycle(Action, "run", Stage.START, judges=True, witnesses=True),
        ComponentRole.ORACLE: RoleLifecycle(Oracle, "evaluate", Stage.EVALUATE, judges=True, witnesses=True),
    }
)

# ---------------------------------------------------------------------------
# Registrations and plugin manifests
# ---------------------------------------------------------------------------

T = TypeVar("T")

# Events the runner publishes for every component use: before and after its
# call, and around the teardown of the roles that have one. No component may
# publish them, or declare them.
LIFECYCLE_EVENTS = ("started", "completed", "failed")
TEARDOWN_EVENTS = ("stopping", "stopped", "stop-failed")

# Events the runner publishes for the run as a whole.
RUN_STARTED = "run.started"
RUN_COMPLETED = "run.completed"
DEPLOYMENT_PROVISIONING = "deployment.provisioning"
DEPLOYMENT_READY = "deployment.ready"
VALIDATION_COMPLETED = "validation.completed"
# Around a rollback after a failed provision, and around the cleanup of a provisioned deployment.
DEPLOYMENT_RELEASE_EVENTS = MappingProxyType(
    {
        "rollback": ("deployment.rollback.started", "deployment.rollback.completed"),
        "cleanup": ("deployment.cleanup.started", "deployment.cleanup.completed"),
    }
)
RUNNER_EVENTS = frozenset(
    {
        RUN_STARTED,
        RUN_COMPLETED,
        DEPLOYMENT_PROVISIONING,
        DEPLOYMENT_READY,
        VALIDATION_COMPLETED,
        *(name for events in DEPLOYMENT_RELEASE_EVENTS.values() for name in events),
    }
)

# A placeholder in a declared event name or evidence ID: ``{id}`` is the
# component use's ``id``; ``{config.<path>}`` is every string the path names
# in its resolved config, where ``<segment>[]`` steps into each list item.
_PLACEHOLDER = re.compile(r"\{(id|config(?:\.[A-Za-z0-9_-]+(?:\[\])?)+)\}")


def _expand(template: str, spec: ComponentSpec) -> tuple[str, ...]:
    """The names ``template`` declares for ``spec``; none when a config path names nothing."""

    names = [template]
    for match in _PLACEHOLDER.finditer(template):
        token = match.group(0)
        if match.group(1) == "id":
            values = [spec.id]
        else:
            values = [plain_config(spec.config)]
            for segment in match.group(1).split(".")[1:]:
                key = segment[:-2] if segment.endswith("[]") else segment
                values = [value[key] for value in values if isinstance(value, Mapping) and key in value]
                if segment.endswith("[]"):
                    values = [item for value in values if isinstance(value, (list, tuple)) for item in value]
            values = [value for value in values if isinstance(value, str)]
        names = [name.replace(token, value, 1) for name in names for value in values]
    return tuple(dict.fromkeys(names))


def _check_template(template: Any, what: str, check: Callable[[Any, str], Any]) -> None:
    """``template`` with every placeholder filled in must pass ``check``."""

    if not isinstance(template, str):
        raise TypeError(f"declared {what} names must be strings")
    leftover = _PLACEHOLDER.sub("x", template)
    if "{" in leftover or "}" in leftover:
        raise ValueError(f"declared {what} {template!r} has a placeholder other than {{id}} or {{config.<path>}}")
    check(_PLACEHOLDER.sub("x.x", template), f"declared {what}")


@dataclass(frozen=True)
class EvidenceDeclaration:
    """The schema of one evidence ID's value; ``many`` allows more than one item per component use.

    Without ``many`` the evidence is a single-valued quantity: a component
    use that emits it twice in one run has contradicted itself.
    """

    value: ConfigField
    many: bool = False

    def __post_init__(self) -> None:
        require_type(self.value, ConfigField, "evidence value schema")
        require_type(self.many, bool, "many")


@dataclass(frozen=True)
class Declarations:
    """What a component publishes, emits, and changes; the runner holds it to them.

    ``events`` maps each event name the component publishes (besides the
    runner's lifecycle events) to the schema of its payload; ``evidence``
    maps each evidence ID it emits to the schema of its value; ``changes``
    names the kinds of host or server state it changes, each owned by an
    undo obligation while it lasts. Names may hold placeholders (``{id}``,
    ``{config.<path>}``).
    """

    events: Mapping[str, ConfigSchema] = field(default_factory=dict)
    evidence: Mapping[str, EvidenceDeclaration] = field(default_factory=dict)
    changes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name, schema in self.events.items():
            _check_template(name, "event", require_name)
            require_type(schema, ConfigSchema, f"payload schema of event {name!r}")
            if name.startswith("{id}.") and name[len("{id}.") :] in (*LIFECYCLE_EVENTS, *TEARDOWN_EVENTS):
                raise ValueError(f"event {name!r} is published by the runner")
        for evidence_id, declaration in self.evidence.items():
            _check_template(evidence_id, "evidence", require_qualified_id)
            require_type(declaration, EvidenceDeclaration, f"declaration of evidence {evidence_id!r}")
        object.__setattr__(self, "events", MappingProxyType(dict(self.events)))
        object.__setattr__(self, "evidence", MappingProxyType(dict(self.evidence)))
        changes = unique_tuple(self.changes, "changes")
        for kind in changes:
            require_name(kind, "change kind")
        object.__setattr__(self, "changes", changes)

    def events_for(self, spec: ComponentSpec) -> dict[str, ConfigSchema]:
        """The events ``spec`` may publish, by name, with their payload schemas."""

        return {name: schema for template, schema in self.events.items() for name in _expand(template, spec)}

    def evidence_for(self, spec: ComponentSpec) -> dict[str, EvidenceDeclaration]:
        """The evidence ``spec`` may emit, by ID, with its declaration."""

        return {name: item for template, item in self.evidence.items() for name in _expand(template, spec)}

    def json_schema(self) -> dict[str, Any]:
        """The declarations as the catalog shows them: payload and value schemas, as written."""

        return {
            "events": {name: schema.json_schema() for name, schema in sorted(self.events.items())},
            "evidence": {
                name: {"value": item.value.json_schema(), "many": item.many}
                for name, item in sorted(self.evidence.items())
            },
            "changes": list(self.changes),
        }


@dataclass(frozen=True)
class ComponentRegistration(Generic[T]):
    """A component factory plus the schema of its config.

    Factories only build in-memory state; external work belongs in lifecycle
    methods. :meth:`resolve_config` checks a component's config, and the
    compiler records its result, defaults included, in the plan: ``config``
    checks the fields and fills in defaults (without it the config is taken
    as written), ``deployment_modes`` limits the deployment modes the
    component supports, and ``check(config, spec, plan)`` makes the
    cross-field checks that need the resolved config or the plan. ``check``
    must be pure: it runs at compile time, when reference fields may still
    hold unresolved environment references, and again on the executable plan.
    """

    component_id: str
    role: ComponentRole
    factory: Callable[[], T]
    config: Optional[ConfigSchema] = None
    check: Optional[Callable[[Mapping[str, Any], ComponentSpec, ExecutionPlan], None]] = None
    deployment_modes: Optional[tuple[str, ...]] = None
    # Evidence IDs a component's config marks as required; the compiler adds
    # them to the plan's required_evidence with that component as their source.
    required_evidence: Optional[Callable[[ComponentSpec], Sequence[str]]] = None
    # The ``(evidence_id, source)`` pairs a component's config names (source None: any component); each must be
    # declared by its source, or by some component, as required evidence must, so a typo is a compile error.
    references_evidence: Optional[Callable[[ComponentSpec], Sequence[tuple[str, Optional[str]]]]] = None
    # What it publishes, emits, and changes; required (``Declarations()`` for nothing).
    declares: Optional[Declarations] = None

    def __post_init__(self) -> None:
        require_qualified_id(self.component_id, "component_id")
        object.__setattr__(self, "role", ComponentRole(self.role))
        if self.declares is None:
            raise ValueError(
                f"component {self.component_id!r} must declare the events it publishes, the evidence it "
                "emits, and the state it changes (declares=Declarations(...); Declarations() for none)"
            )
        require_type(self.declares, Declarations, "declares")
        if self.config is not None:
            require_type(self.config, ConfigSchema, "component config")
        if self.deployment_modes is not None:
            object.__setattr__(self, "deployment_modes", unique_tuple(self.deployment_modes, "deployment_modes"))
        for name in ("factory", "check", "required_evidence", "references_evidence"):
            value = getattr(self, name)
            if (value is not None or name == "factory") and not callable(value):
                raise TypeError(f"component {name} must be callable")

    def resolve_config(self, spec: ComponentSpec, plan: ExecutionPlan) -> dict[str, Any]:
        """``spec``'s config as plain data, checked, with its defaults filled in.

        Raises :class:`~valkey_fuzzer.schema.ConfigError` naming the field, or
        whatever ``check`` raises.
        """

        location = f"components.{spec.id}.config"
        config = plain_config(spec.config) if self.config is None else self.config.resolve(spec.config, location)
        if self.deployment_modes is not None and plan.deployment_mode not in self.deployment_modes:
            raise ConfigError(
                f"{self.component_id} supports deployment modes {sorted(self.deployment_modes)}, "
                f"not {plan.deployment_mode!r}"
            )
        if self.check is not None and self.check(config, spec, plan) is not None:
            raise TypeError(f"check for component {self.component_id!r} must return None")
        return config


@dataclass(frozen=True)
class GeneratorRegistration:
    """A generator hook, named ``<namespace>.generator.<name>``.

    ``hook(rng, scenario)`` draws part of a random scenario: an input hook
    returns the input's value, a scenario hook a partial scenario (see
    :mod:`valkey_fuzzer.generator`). Hooks run while a scenario is drawn,
    never in a plan.
    """

    generator_id: str
    hook: Callable[[random.Random, Mapping[str, Any]], Any]

    def __post_init__(self) -> None:
        require_qualified_id(self.generator_id, "generator_id")
        if not callable(self.hook):
            raise TypeError(f"generator {self.generator_id!r} hook must be callable")


@dataclass(frozen=True)
class DeploymentProviderRegistration:
    """A fresh-provider factory implementing one deployment mode.

    ``deployment_mode`` is the mode experiments list in ``deployment_modes``;
    ``provider_id`` names this implementation of it. Providers own mutable
    lifecycle state, so the runner builds a new one for every validation or
    execution attempt. ``normalizer``, when given, is a pure function from a
    scenario's deployment document to the same document with the provider's
    defaults filled in; it raises ``ValueError`` for a document the provider
    cannot run. The compiler records its result in the plan and checks the
    experiment's ``deployment_requirements`` against it. ``plan_instances``,
    when given, is a pure function from the normalized document and the
    plan's startup overlays to the instances the provider will start, each
    ``{role, shard, server_config}`` with its effective server config: the
    provider's own defaults under the document's config and the overlays.
    The compiler records it as the plan's ``instances``.
    """

    deployment_mode: str
    provider_id: str
    factory: Callable[[], DeploymentProvider]
    implementation_digest: Optional[str] = None
    normalizer: Optional[Callable[[Mapping[str, Any]], Mapping[str, Any]]] = None
    plan_instances: Optional[Callable[[Mapping[str, Any], Sequence[StartupOverlay]], Mapping[str, Any]]] = None

    def __post_init__(self) -> None:
        require_name(self.deployment_mode, "deployment_mode")
        self.identity  # noqa: B018 - validates the identity fields
        if not callable(self.factory):
            raise TypeError("deployment provider factory must be callable")
        for name in ("normalizer", "plan_instances"):
            if getattr(self, name) is not None and not callable(getattr(self, name)):
                raise TypeError(f"deployment provider {name} must be callable")

    @property
    def identity(self) -> DeploymentProviderIdentity:
        return DeploymentProviderIdentity(
            provider_id=self.provider_id, implementation_digest=self.implementation_digest
        )


@dataclass(frozen=True)
class PluginManifest:
    """What a ``valkey_fuzzer.plugins`` entry point returns.

    Its entries are checked, for their type and against each other and
    everything else installed, when the catalog registers the plugin.
    """

    plugin_id: str
    registrations: tuple[ComponentRegistration[Any], ...] = ()
    experiments: tuple[ExperimentSpec, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)
    artifact_providers: tuple[ArtifactProviderRegistration, ...] = ()
    resource_providers: tuple[ResourceProviderRegistration, ...] = ()
    deployment_providers: tuple[DeploymentProviderRegistration, ...] = ()
    # Named fragments that data-only experiments can ``extends``.
    experiment_fragments: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    # Generator hooks for random scenarios; not components.
    generators: tuple[GeneratorRegistration, ...] = ()

    def __post_init__(self) -> None:
        require_qualified_id(self.plugin_id, "plugin_id")
        fragments = freeze_mapping(self.experiment_fragments, "experiment_fragments")
        for name, fragment in fragments.items():
            require_qualified_id(name, "experiment fragment name")
            if not isinstance(fragment, Mapping):
                raise TypeError(f"experiment fragment {name!r} must be a mapping")
        object.__setattr__(self, "experiment_fragments", fragments)
        for name in (
            "registrations",
            "generators",
            "experiments",
            "artifact_providers",
            "resource_providers",
            "deployment_providers",
        ):
            object.__setattr__(self, name, as_tuple(getattr(self, name), name))
        object.__setattr__(self, "metadata", freeze_mapping(self.metadata, "plugin metadata"))


# ---------------------------------------------------------------------------
# Component helpers
# ---------------------------------------------------------------------------


def make_evidence(
    spec: ComponentSpec,
    evidence_id: str,
    phase: str,
    value: Any,
    *,
    event_sequence: Optional[int] = None,
    artifacts: Sequence[str] = (),
    metadata: Optional[Mapping[str, Any]] = None,
) -> Evidence:
    """Evidence owned by component ``spec``, observed now; ``value`` is made JSON-safe."""

    return Evidence(
        evidence_id=evidence_id,
        source=spec.id,
        phase=phase,
        observed_at=time.time(),
        value=jsonable(value),
        event_sequence=event_sequence,
        artifacts=tuple(artifacts),
        metadata=jsonable(metadata or {}),
    )


def make_result(
    spec: ComponentSpec,
    status: ComponentStatus,
    summary: str,
    *,
    evidence: Sequence[Evidence] = (),
    diagnostics: Optional[Mapping[str, Any]] = None,
    unobserved: Sequence[str] = (),
    preconditions: Optional[Mapping[str, str]] = None,
) -> ComponentResult:
    """A result for component ``spec``; ``diagnostics`` are made JSON-safe, ``unobserved`` names the evidence
    it could not look for, and ``preconditions`` what its evidence needs (:class:`ComponentResult`)."""

    return ComponentResult(
        component_id=spec.id,
        status=status,
        summary=summary,
        evidence=tuple(evidence),
        diagnostics=jsonable(diagnostics or {}),
        unobserved=tuple(unobserved),
        preconditions=dict(preconditions or {}),
    )


def evidence_of(evidence: Sequence[Evidence], evidence_id: str, source: Optional[str] = None) -> list[Evidence]:
    """The items of ``evidence`` with ``evidence_id`` (and ``source``, if given)."""

    return [item for item in evidence if item.evidence_id == evidence_id and source in (None, item.source)]


def run_artifact_dir(context: RunContext) -> str:
    """This run's own folder, ``context.artifact_dir`` (created on demand)."""

    directory = context.artifact_dir
    os.makedirs(directory, exist_ok=True)
    return directory


def component_artifact_dir(context: RunContext, spec: ComponentSpec) -> str:
    """This run's directory for one component (created on demand)."""

    path = os.path.join(run_artifact_dir(context), "components", spec.id)
    os.makedirs(path, exist_ok=True)
    return path
