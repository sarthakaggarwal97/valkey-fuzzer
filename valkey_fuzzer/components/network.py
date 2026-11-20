"""Network faults between deployment instances (Linux, per-instance loopback addresses).

``valkey.fault.network-partition`` isolates selected instances from the rest of
the deployment: it drops every packet between each isolated instance's address
and each other instance's address.  Client traffic from the harness (127.0.0.1)
is unaffected, so clients can keep talking to both sides — the classic
split-brain setup.

Rules live in a dedicated iptables chain jumped to from INPUT, so healing is
atomic and recovery never touches unrelated rules.  The chain is owned by an
undo obligation from the moment it exists: installing is all-or-nothing (a
failed step removes the chain again), a heal discharges it early, and a chain
that could not be removed stays open for recovery and then the runner.
Every iptables call is bounded (``run_iptables``), so a trigger never blocks
its event's dispatch on a held xtables lock. The chain's name carries this
process as its owner (``chain_name``), so ``valkey-fuzzer sweep`` finds and
removes a chain a killed run left (a batch sweeps each worker after its run and
reports it as that run's leak).  The deployment must use
``addressing: per_instance_loopback``; the ``valkey.capability.host-probe``
probe with ``requires: [linux, root, iptables, per_instance_loopback]`` turns
hosts that cannot do this into ``SKIP_UNSUPPORTED`` before anything starts.

``valkey.fault.network-degrade`` degrades the links between selected instances
and their peers instead of cutting them: packet loss, delay with jitter,
reordering, duplication, and a rate limit, with ``tc`` netem
(:class:`Degradation`). Nothing is queued on ``lo`` itself: the netem qdisc is
the root of an ``ifb`` device of the fault's own, named after this process
(``netem_device_name``), and flower filters on ``lo``'s ingress redirect to it
only the TCP packets whose source and destination are both the exact
addresses of an instance pair (``src_ip A dst_ip B``, each direction its own
filter); every other loopback flow, the harness's clients on 127.0.0.1 among
them, never matches a filter and is never queued. Each fault takes one
``pref`` of its own for its filters, claimed with an explicit handle so two
runs never share one, and removes them by that ``pref``. The ingress qdisc the
filters hang off is added only when ``lo`` has none, marked as the fuzzer's,
and removed by whichever fuzzer process removes the last filter, under a lock
every fuzzer process takes (``lo_ingress_lock``, ``ensure_lo_ingress``,
``release_lo_ingress``); one an operator added stays. The device carries this
process as its owner, so the sweep removes a killed run's device with the
filters that redirect to it. It needs ``tc`` and ``ip`` and ``CAP_NET_ADMIN``
(``requires: [linux, netem, per_instance_loopback]``); the fuzzer never asks
for ``sudo``.
"""

import ipaddress
import secrets
import uuid
from collections.abc import Mapping, Sequence
from typing import Any, Optional

from valkey_fuzzer.hoststate import (
    chain_name,
    ensure_lo_ingress,
    lo_ingress_lock,
    netem_device_name,
    release_lo_ingress,
    remove_chain,
    remove_lo_filters,
    remove_netem_device,
    run_ip,
    run_iptables,
    run_tc,
)
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.schema import ConfigError, ConfigField, ConfigSchema, plain_config

from .faults import HeldFault, fault_declares
from .instances import deployment, select_instances
from .registration import component_registration
from .schemas import LIST, MAPPING, TEXT, optional

# The harness's own address: its clients come from it, so a rule on it would catch them.
_HARNESS_ADDRESS = ipaddress.ip_address("127.0.0.1")
# How many prefs a degradation tries before it gives up finding a free one.
_PREF_TRIES = 64


class PartitionError(Exception):
    """An iptables step of a partition failed."""


class DegradeError(Exception):
    """A tc or ip step of a degradation failed."""


def create_chain(chain: str) -> None:
    """Create the partition's own chain; whoever partitions owns it (``network.partition``) from then on."""

    result = run_iptables("-N", chain)
    if result.returncode != 0:
        raise PartitionError(f"cannot create chain: {result.stderr.strip()}")


def _refuse_unsafe_addresses(
    addresses: Mapping[str, str], isolated: Sequence[str], peers: Sequence[str], error: type, verb: str
) -> None:
    """Raise ``error`` unless every address is loopback and distinct, and no pair is one address.

    A rule between 127.0.0.1 and itself, which a provider that returns one address for every instance would
    produce, drops all loopback traffic on the host, the operator's included.
    """

    parsed = {}
    for name, address in addresses.items():
        try:
            parsed[name] = ipaddress.ip_address(address)
        except ValueError:
            raise error(f"refusing to {verb}: {name!r} has address {address!r}, not an IP address") from None
        if not parsed[name].is_loopback:
            raise error(f"refusing to {verb}: {name!r} has address {address}, not a loopback address")
    if len(set(parsed.values())) != len(parsed):
        raise error(f"refusing to {verb}: instances share an address: {dict(addresses)}")
    for one in isolated:
        for peer in peers:
            if parsed[one] == parsed[peer]:
                raise error(f"refusing to {verb}: {one!r} and {peer!r} are the same address")


def drop_between(chain: str, addresses: Mapping[str, str], isolated: Sequence[str], peers: Sequence[str]) -> list[str]:
    """Add a DROP rule each way between every isolated instance and every peer to ``chain``, then jump to it
    from INPUT; the rules (``source->destination``), or :class:`PartitionError` at the first failed step.

    Refuses, before any iptables call, addresses that are not all distinct loopback addresses."""

    _refuse_unsafe_addresses(addresses, isolated, peers, PartitionError, "partition")
    rules = []
    for one in isolated:
        for peer in peers:
            a, b = addresses[one], addresses[peer]
            for source, destination in ((a, b), (b, a)):
                rule = ("-A", chain, "-s", source, "-d", destination, "-j", "DROP")
                added = run_iptables(*rule)
                if added.returncode != 0:
                    raise PartitionError(f"iptables {' '.join(rule)}: {added.stderr.strip()}")
                rules.append(f"{source}->{destination}")
    jump = run_iptables("-I", "INPUT", "1", "-j", chain)
    if jump.returncode != 0:
        raise PartitionError(f"cannot install jump: {jump.stderr.strip()}")
    return rules


def remove_partition(chain: str) -> None:
    """The undo of a partition: remove its chain and the jump to it (idempotent); raise while it remains."""

    errors = remove_chain(chain, run_iptables)
    if errors:
        raise RuntimeError(f"iptables chain {chain} remains: {errors}")


# ---------------------------------------------------------------------------
# Degradations: tc netem on a device of the fault's own
# ---------------------------------------------------------------------------

# What a degradation does to the matched packets; a fault and the ``degrade`` wave operation take the same.
DEGRADE_FIELDS = {
    "loss_pct": ConfigField(float, default=0.0, minimum=0, maximum=100),
    "delay_ms": ConfigField(float, default=0.0, minimum=0, maximum=60000),
    # Added to and subtracted from delay_ms at random (netem's second delay argument).
    "jitter_ms": ConfigField(float, default=0.0, minimum=0, maximum=60000),
    # Sent at once instead of delayed, so they overtake the delayed ones: needs delay_ms.
    "reorder_pct": ConfigField(float, default=0.0, minimum=0, maximum=100),
    "duplicate_pct": ConfigField(float, default=0.0, minimum=0, maximum=100),
    "rate_kbit": ConfigField(int, allow_none=True, default=None, minimum=8, maximum=100_000_000),
}
_EFFECTS = ("loss_pct", "delay_ms", "jitter_ms", "reorder_pct", "duplicate_pct", "rate_kbit")


def check_degrade(config: Mapping[str, Any], where: str = "") -> None:
    """Raise :class:`ConfigError` unless ``config`` degrades something netem can do."""

    prefix = f"{where}: " if where else ""
    if not any(config[name] for name in _EFFECTS):
        raise ConfigError(f"{prefix}set at least one of {', '.join(_EFFECTS)}")
    if (config["reorder_pct"] or config["jitter_ms"]) and not config["delay_ms"]:
        raise ConfigError(f"{prefix}reorder_pct and jitter_ms need delay_ms above 0")


def netem_arguments(config: Mapping[str, Any]) -> list[str]:
    """The ``tc qdisc ... netem`` arguments for ``config`` (:data:`DEGRADE_FIELDS`)."""

    arguments = []
    if config["delay_ms"]:
        arguments += ["delay", f"{config['delay_ms']:g}ms"]
        if config["jitter_ms"]:
            arguments.append(f"{config['jitter_ms']:g}ms")
    if config["loss_pct"]:
        arguments += ["loss", f"{config['loss_pct']:g}%"]
    if config["reorder_pct"]:
        arguments += ["reorder", f"{config['reorder_pct']:g}%"]
    if config["duplicate_pct"]:
        arguments += ["duplicate", f"{config['duplicate_pct']:g}%"]
    if config["rate_kbit"]:
        arguments += ["rate", f"{config['rate_kbit']}kbit"]
    return arguments


def _refuse_unsafe_links(addresses: Mapping[str, str], instances: Sequence[str], peers: Sequence[str]) -> None:
    """Raise :class:`DegradeError` unless every address is a distinct IPv4 loopback address other than the
    harness's own 127.0.0.1, and no pair is one address: the filters match exactly these addresses, so nothing
    else on the host can match them."""

    _refuse_unsafe_addresses(addresses, instances, peers, DegradeError, "degrade")
    for name, address in addresses.items():
        parsed = ipaddress.ip_address(address)
        if parsed.version != 4:
            raise DegradeError(f"refusing to degrade: {name!r} has address {address}, not an IPv4 address")
        if parsed == _HARNESS_ADDRESS:
            raise DegradeError(f"refusing to degrade: {name!r} has the harness's own address {address}")


def _failed(step: str, result: Any) -> DegradeError:
    error = result.stderr.strip() or f"exit status {result.returncode}"
    # "Specified qdisc kind is unknown", "Unknown device type", "TC classifier not found", "Operation not
    # supported", "No such file": the module behind the step is not in this kernel.
    if any(phrase in error.lower() for phrase in ("unknown", "not found", "not supported", "no such file")):
        error += " (the kernel lacks a module: ifb, sch_netem, cls_flower, or act_mirred)"
    return DegradeError(f"{step}: {error}")


class Degradation:
    """One degradation: a netem device of this process's own and the ``lo`` ingress filters that redirect to it
    the TCP packets between each of ``instances`` and each of ``peers``, both ways.

    :meth:`create_device` makes the device, which carries this process as its owner; whoever degrades owns it
    (``network.degrade``) from then on and undoes it with :meth:`remove`. :meth:`install` then puts netem
    ``arguments`` on the device and redirects the pairs to it; it raises :class:`DegradeError` at the first
    failed step, and refuses unsafe addresses before any call.
    """

    def __init__(
        self, addresses: Mapping[str, str], instances: Sequence[str], peers: Sequence[str], arguments: Sequence[str]
    ):
        self.addresses = dict(addresses)
        self.instances = list(instances)
        self.peers = list(peers)
        self.arguments = list(arguments)
        self.device: Optional[str] = None
        self.pref: Optional[int] = None
        self.rules: list[str] = []

    def create_device(self) -> str:
        """Make the fault's ``ifb`` device under the first free name of this process's; its name."""

        _refuse_unsafe_links(self.addresses, self.instances, self.peers)
        for suffix in range(36):
            name = netem_device_name(suffix)
            added = run_ip("link", "add", "dev", name, "type", "ifb")
            if added.returncode == 0:
                self.device = name
                return name
            if "File exists" not in added.stderr:
                raise _failed(f"ip link add {name} type ifb", added)
        raise DegradeError("every netem device name of this process is taken (36 degradations at once)")

    def install(self) -> list[str]:
        """Netem on the device, the ingress qdisc on ``lo`` if it has none, and one redirect filter per direction
        of each pair, all at one free ``pref``; the rules (``source->destination``)."""

        assert self.device is not None, "create_device first"
        up = ("link", "set", "dev", self.device, "up")
        result = run_ip(*up)
        if result.returncode != 0:
            raise _failed(f"ip {' '.join(up)}", result)
        qdisc = ("qdisc", "add", "dev", self.device, "root", "netem", *self.arguments)
        result = run_tc(*qdisc)
        if result.returncode != 0:
            raise _failed(f"tc {' '.join(qdisc)}", result)
        flows = [
            (source, destination)
            for one in self.instances
            for peer in self.peers
            if one != peer
            for source, destination in (
                (self.addresses[one], self.addresses[peer]),
                (self.addresses[peer], self.addresses[one]),
            )
        ]
        with lo_ingress_lock():
            error = ensure_lo_ingress(run_tc)
            if error is not None:
                raise DegradeError(f"tc qdisc add dev lo ingress: {error}")
            # The first flow's filter claims the pref: an explicit handle makes a taken pref a ``File exists``.
            start = 10000 + secrets.randbelow(50000)
            for pref in range(start, start + _PREF_TRIES):
                if self._redirect(pref, 1, *flows[0], claim=True):
                    self.pref = pref
                    break
            else:
                raise DegradeError(f"no free filter pref among {start}..{start + _PREF_TRIES - 1} on lo")
            self.rules = [f"{flows[0][0]}->{flows[0][1]}"]
            for handle, (source, destination) in enumerate(flows[1:], start=2):
                self._redirect(self.pref, handle, source, destination, claim=False)
                self.rules.append(f"{source}->{destination}")
        return list(self.rules)

    def _redirect(self, pref: int, handle: int, source: str, destination: str, *, claim: bool) -> bool:
        """Add the filter redirecting TCP from ``source`` to ``destination`` to the device; False when ``claim``
        and the ``pref`` is taken, else True, or :class:`DegradeError`."""

        step = (
            "filter", "add", "dev", "lo", "ingress", "protocol", "ip", "pref", str(pref), "handle", str(handle),
            "flower", "ip_proto", "tcp", "src_ip", source, "dst_ip", destination,
            "action", "mirred", "egress", "redirect", "dev", self.device,
        )  # fmt: skip
        result = run_tc(*step)
        if result.returncode == 0:
            return True
        if claim and "File exists" in result.stderr:
            return False
        raise _failed(f"tc {' '.join(step)}", result)

    def remove(self) -> None:
        """The undo: the filters, then ``lo``'s ingress qdisc once the fuzzer added it and no filter is left on
        it, then the device (idempotent); raise while any remains."""

        errors = []
        with lo_ingress_lock():
            if self.pref is not None:
                errors += remove_lo_filters([self.pref], run_tc)
            if not errors:
                errors += release_lo_ingress(run_tc)
        if self.device is not None and not errors:
            # Only once no filter redirects to it: a filter whose device is gone drops what it matches.
            errors += remove_netem_device(self.device, run_ip)
        if errors:
            raise RuntimeError(f"netem device {self.device} remains: {errors}")


# ---------------------------------------------------------------------------
# The faults
# ---------------------------------------------------------------------------

_HEAL_FIELDS = {
    "heal_event": ConfigField(str, allow_none=True, default=None),
    "duration_s": ConfigField(float, allow_none=True, default=None, minimum=0.01, maximum=3600),
}

_PARTITION_FIELDS = {
    "isolate": ConfigField((str, list)),
    "peers": ConfigField((str, list), default="others"),
    **_HEAL_FIELDS,
}

_DEGRADE_FIELDS = {
    "instances": ConfigField((str, list)),
    "peers": ConfigField((str, list), default="others"),
    **DEGRADE_FIELDS,
    **_HEAL_FIELDS,
}


def _check_heal(config: dict, use: str, plan: ExecutionPlan) -> None:
    document = plain_config(plan.deployment)
    if document.get("addressing") != "per_instance_loopback":
        raise ConfigError(f"{use} requires deployment.addressing: per_instance_loopback")
    if config["heal_event"] is not None and config["duration_s"] is not None:
        raise ConfigError("set heal_event or duration_s, not both")


def _validate_partition(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    _check_heal(config, "valkey.fault.network-partition", plan)


def _validate_degrade(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    _check_heal(config, "valkey.fault.network-degrade", plan)
    check_degrade(config)


def _resolve_pairs(context: Any, config: Mapping[str, Any], field: str) -> tuple[list[str], list[str], dict]:
    """The selected instances, their peers, and every instance's address, or ``ValueError``."""

    handle = deployment(context)
    selected = select_instances(handle, config[field])
    if config["peers"] == "others":
        peers = [instance.instance_id for instance in handle.instances if instance.instance_id not in selected]
    else:
        peers = [item for item in select_instances(handle, config["peers"]) if item not in selected]
    if not selected or not peers:
        raise ValueError("a network fault needs at least one selected instance and one peer")
    addresses = {instance.instance_id: instance.host for instance in handle.instances}
    if len(set(addresses.values())) != len(addresses):
        raise ValueError("instances share an address; use deployment.addressing: per_instance_loopback")
    return selected, peers, addresses


class _HealingFault(HeldFault):
    """A link fault installed at a lifecycle event and healed on ``heal_event``, after ``duration_s``, or at
    recovery: a :class:`HeldFault` whose installation is all-or-nothing (a failed step removes what it made) and
    which announces the changed links with an event of its own; subclasses install the change and name their
    events and record fields."""

    release_field = "heal_event"
    released_event = "healed"
    released_at_field = "healed_at"
    reason_field = "heal_reason"
    # The exception an installation step raises, and the event an installation publishes.
    step_error: type = Exception
    applied_event = ""

    def _applied_payload(self) -> dict[str, Any]:
        raise NotImplementedError

    def _fire(self, context: Any, spec: ComponentSpec, event: Any) -> dict[str, Any]:
        try:
            record = self._install(event)
        except self.step_error as exc:
            record = {"applied": False, "error": str(exc)}
            error = self._remove()
            if error:
                record["cleanup_errors"] = [error]
            return record
        except BaseException:
            self._remove()
            raise
        self._installed = True
        context.event_bus.publish(f"{spec.id}.{self.applied_event}", source=spec.id, payload=self._applied_payload())
        self._release_later()
        return {"applied": True, **record}

    def _remove(self) -> Optional[str]:
        """Discharge the change's obligation if it was opened; the error, if the change remains."""

        if self._change is None:
            return None
        try:
            self._change.discharge()
        except Exception as exc:
            return f"{type(exc).__name__}: {exc}"
        return None

    def _heal(self, reason: str) -> None:
        """Release the change: the link faults' word for it."""

        self._release(reason)

    def _release_failed(self, record: dict[str, Any], error: str) -> str:
        record["heal_errors"] = [error]
        return f"{self.applied_event} heal failed, so it lasted until recovery: {[error]}"


class PartitionFault(_HealingFault):
    """Cut the selected instances off from their peers at a lifecycle event, until ``heal_event``,
    ``duration_s``, or recovery.

    Clients on 127.0.0.1 still reach both sides, so writes can land on a primary that is about to lose them.
    """

    step_error = PartitionError
    applied_event = "partitioned"
    seconds_field = "partition_seconds"

    def _resolve(self, context: Any, spec: ComponentSpec) -> None:
        self._isolated, self._peers, self._addresses = _resolve_pairs(context, self._config, "isolate")
        # Unique to this fault instance, so concurrent runs in one process never share a chain.
        self._chain = chain_name(f"{spec.id}/{uuid.uuid4().hex}")

    def _install(self, event: Any) -> dict[str, Any]:
        """Create the chain, add every DROP rule, and jump to it from INPUT; raise on the first failure."""

        create_chain(self._chain)
        self._change = self._own("network.partition", self._chain, lambda: remove_partition(self._chain))
        rules = drop_between(self._chain, self._addresses, self._isolated, self._peers)
        return {
            "targets": self._isolated,
            "isolated": self._isolated,
            "peers": self._peers,
            "addresses": self._addresses,
            "chain": self._chain,
            "rules": rules,
        }

    def _applied_payload(self) -> dict[str, Any]:
        return {"isolated": self._isolated, "peers": self._peers}

    def _released_payload(self, reason: str) -> dict[str, Any]:
        return {"reason": reason, "isolated": self._isolated}


class DegradeFault(_HealingFault):
    """Degrade the links between the selected instances and their peers at a lifecycle event (loss, delay with
    jitter, reordering, duplication, a rate limit), until ``heal_event``, ``duration_s``, or recovery.

    Only packets between the instances' exact addresses are touched; the harness's clients on 127.0.0.1 are not.
    """

    step_error = DegradeError
    applied_event = "degraded"
    seconds_field = "degrade_seconds"

    def _resolve(self, context: Any, spec: ComponentSpec) -> None:
        self._instances, self._peers, self._addresses = _resolve_pairs(context, self._config, "instances")
        self._netem = netem_arguments(self._config)
        self._degradation = Degradation(self._addresses, self._instances, self._peers, self._netem)

    def _install(self, event: Any) -> dict[str, Any]:
        """Create the device, then netem on it and the redirect filters; raise on the first failure."""

        device = self._degradation.create_device()
        self._change = self._own("network.degrade", device, self._degradation.remove)
        rules = self._degradation.install()
        return {
            "targets": self._instances,
            "instances": self._instances,
            "peers": self._peers,
            "addresses": self._addresses,
            "device": device,
            "pref": self._degradation.pref,
            "netem": " ".join(self._netem),
            "rules": rules,
        }

    def _applied_payload(self) -> dict[str, Any]:
        return {"instances": self._instances, "peers": self._peers, "netem": " ".join(self._netem)}

    def _released_payload(self, reason: str) -> dict[str, Any]:
        return {"reason": reason, "instances": self._instances}


_HEAL_RECORD = {
    "healed_at": optional(float),
    "heal_reason": optional(str, allow_none=True),
    "heal_errors": optional(list),
    "cleanup_errors": optional(list),
}

_PARTITION_DECLARES = fault_declares(
    {
        "targets": LIST,
        "isolated": LIST,
        "peers": LIST,
        "addresses": MAPPING,
        "chain": TEXT,
        "rules": LIST,
        "partition_seconds": optional(float),
        **_HEAL_RECORD,
    },
    events={
        "{id}.partitioned": ConfigSchema({"isolated": LIST, "peers": LIST}),
        "{id}.healed": ConfigSchema({"reason": TEXT, "isolated": LIST}),
    },
    changes=("network.partition",),
)

_DEGRADE_DECLARES = fault_declares(
    {
        "targets": LIST,
        "instances": LIST,
        "peers": LIST,
        "addresses": MAPPING,
        "device": TEXT,
        "pref": ConfigField(int),
        "netem": TEXT,
        "rules": LIST,
        "degrade_seconds": optional(float),
        **_HEAL_RECORD,
    },
    events={
        "{id}.degraded": ConfigSchema({"instances": LIST, "peers": LIST, "netem": TEXT}),
        "{id}.healed": ConfigSchema({"reason": TEXT, "instances": LIST}),
    },
    changes=("network.degrade",),
)

REGISTRATIONS = (
    component_registration(
        "valkey.fault.network-partition",
        PartitionFault,
        _PARTITION_FIELDS,
        check=_validate_partition,
        selectors={"isolate": (), "peers": ("others",)},
        triggered=True,
        declares=_PARTITION_DECLARES,
    ),
    component_registration(
        "valkey.fault.network-degrade",
        DegradeFault,
        _DEGRADE_FIELDS,
        check=_validate_degrade,
        selectors={"instances": (), "peers": ("others",)},
        triggered=True,
        declares=_DEGRADE_DECLARES,
    ),
)
