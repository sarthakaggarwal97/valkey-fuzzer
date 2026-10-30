"""Shared helpers for the Valkey component library.

Components only talk to the generic run context from
:mod:`valkey_fuzzer.extensions`.  Deployment access goes through the
``valkey.deployment`` service published by the local deployment provider; nothing
here imports framework internals.
"""

import functools
import hashlib
import json
import os
import re
import signal
import threading
import time
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Callable, Optional

import valkey

from valkey_fuzzer.extensions import (
    ComponentRegistration,
    ComponentRole,
    ComponentSpec,
    ConfigError,
    ConfigField,
    ConfigSchema,
    Declarations,
    EvidenceDeclaration,
    ExecutionPlan,
    InstanceSelector,
    SpecBundleError,
    jsonable,
    load_spec_mapping,
    parse_replication,
)

DEPLOYMENT_SERVICE = "valkey.deployment"

Check = Callable[[Mapping[str, Any], ComponentSpec, ExecutionPlan], None]
# Opens the undo obligation of a change just applied: ``own(kind, target, undo)``.
Own = Callable[[str, str, Callable[[], None]], Any]

# The local deployment names its instances valkey-0, valkey-1, ...
INSTANCE_ID = re.compile(r"valkey-\d+")

# Event-triggered components name their trigger in their own config; the
# framework has no scheduling language.  ``occurrence`` fires on the nth
# matching event, ``when_payload`` filters events by payload fields, and
# ``delay_s`` waits inside the trigger's dispatch before firing.
TRIGGER_FIELDS = {
    "when_event": ConfigField(str),
    "occurrence": ConfigField(int, default=1, minimum=1, maximum=1_000_000),
    "when_payload": ConfigField(dict, default=dict),
    "delay_s": ConfigField(float, default=0.0, minimum=0, maximum=600),
}


# Value schemas for declared events and evidence (see :func:`mapping_of`).
TEXT = ConfigField(str)
INTEGER = ConfigField(int)
NUMBER = ConfigField(float)
FLAG = ConfigField(bool)
MAPPING = ConfigField(dict)
LIST = ConfigField(list)
ANY = ConfigField(object)


def optional(types: Any, **kwargs: Any) -> ConfigField:
    """A field a record or payload may leave out."""

    return ConfigField(types, required=False, **kwargs)


def nullable(types: Any) -> ConfigField:
    """A required field that may be null."""

    return ConfigField(types, allow_none=True)


def mapping_of(
    fields: Optional[Mapping[str, ConfigField]] = None, *, additional: Any = False, **kwargs: Any
) -> ConfigField:
    """A mapping value with ``fields``; ``additional`` is :class:`ConfigSchema`'s, for keys that vary."""

    return ConfigField(dict, schema=ConfigSchema(fields or {}, additional=additional), **kwargs)


def declared(value: ConfigField, *, many: bool = False) -> EvidenceDeclaration:
    return EvidenceDeclaration(value, many=many)


def component_registration(
    component_id: str,
    factory: Callable[[], Any],
    fields: Mapping[str, ConfigField],
    *,
    check: Optional[Check] = None,
    selectors: Mapping[str, Sequence[str]] = {},
    deployment_modes: Optional[Iterable[str]] = None,
    required_evidence: Optional[Callable[[ComponentSpec], Any]] = None,
    references_evidence: Optional[Callable[[ComponentSpec], Any]] = None,
    triggered: bool = False,
    declares: Declarations,
) -> ComponentRegistration:
    """The registration of one component whose config has ``fields``.

    Its role is the second segment of ``component_id`` (``valkey.<role>.<name>``).
    ``selectors`` names the fields that select instances, each with the
    values it also accepts (``instance: event``); they are checked when the
    scenario compiles, before ``check``, so no component repeats it.
    ``triggered`` components take the trigger fields too: they name their
    trigger in their own config (``when_event``), optionally the nth matching
    occurrence, a payload filter, which is checked before ``check`` runs, and a
    delay; the framework has no scheduling language. ``declares`` names the
    events, evidence, and state changes the runner holds the component to.
    """

    if selectors:
        check = functools.partial(_check_selectors, dict(selectors), check)
    if triggered:
        fields = {**TRIGGER_FIELDS, **fields}
        check = functools.partial(_check_trigger, check)
    return ComponentRegistration(
        component_id=component_id,
        role=ComponentRole(component_id.split(".")[1]),
        factory=factory,
        config=(SpecSchema if "spec_sha256" in fields else ConfigSchema)(fields),
        check=check,
        deployment_modes=None if deployment_modes is None else tuple(deployment_modes),
        required_evidence=required_evidence,
        references_evidence=references_evidence,
        declares=declares,
    )


def _check_selectors(
    selectors: Mapping[str, Sequence[str]],
    check: Optional[Check],
    config: Mapping[str, Any],
    spec: ComponentSpec,
    plan: ExecutionPlan,
) -> None:
    """Every selector field that is set names local instances (or one of its other values), then ``check``."""

    for name, accepted in selectors.items():
        if config[name] is not None and config[name] not in accepted:
            validate_instance_selector(config[name], name)
    if check is not None:
        check(config, spec, plan)


def _check_trigger(check: Optional[Check], config: Mapping[str, Any], spec: ComponentSpec, plan: ExecutionPlan) -> None:
    """A triggered component's ``when_payload`` filter, then its own ``check``."""

    validate_payload_filter(config["when_payload"])
    if check is not None:
        check(config, spec, plan)


def validate_payload_filter(rules: Mapping[str, Any], where: str = "when_payload") -> None:
    for key, rule in rules.items():
        if not isinstance(key, str) or not key:
            raise ConfigError(f"{where} keys must be payload field names")
        if isinstance(rule, Mapping):
            validate_predicate(rule, f"{where}.{key}")
        elif isinstance(rule, (list, tuple)):
            if not rule or not all(isinstance(item, (str, int, float, bool)) for item in rule):
                raise ConfigError(f"{where}.{key} must be a non-empty list of scalars")
        elif not isinstance(rule, (str, int, float, bool)):
            raise ConfigError(f"{where}.{key} must be a scalar, a list of scalars, or a predicate")


def payload_matches(payload: Mapping[str, Any], rules: Mapping[str, Any]) -> bool:
    """Match an event payload: scalars compare as strings, lists are any-of, mappings are predicates."""

    for key, rule in rules.items():
        actual = payload.get(key)
        if isinstance(rule, Mapping):
            if not compare(actual, rule):
                return False
        elif actual is None:
            return False
        elif isinstance(rule, (list, tuple)):
            if str(actual) not in {str(item) for item in rule}:
                return False
        elif str(actual) != str(rule):
            return False
    return True


def validate_command(command: Any, name: str = "command") -> None:
    if not command or not all(isinstance(item, (str, int, float)) and not isinstance(item, bool) for item in command):
        raise ConfigError(f"{name} must be a non-empty list of strings or numbers")


def server_arguments(config: Mapping[str, Any]) -> list[str]:
    """Render ``{option: value}`` as valkey-server command-line arguments."""

    arguments: list[str] = []
    for option, raw in config.items():
        for value in raw if isinstance(raw, (list, tuple)) else (raw,):
            if isinstance(value, bool):
                value = "yes" if value else "no"
            arguments.extend((f"--{option}", str(value)))
    return arguments


def is_cluster(handle: Any) -> bool:
    """Whether the deployment behind ``handle`` runs in cluster mode."""

    return getattr(getattr(handle, "mode", None), "value", None) == "cluster"


def deployment(context: Any) -> Any:
    try:
        return context.services["deployment"][DEPLOYMENT_SERVICE]
    except KeyError as exc:
        raise RuntimeError(f"run context does not provide the {DEPLOYMENT_SERVICE!r} deployment service") from exc


def validate_instance_selector(value: Any, name: str = "instances") -> None:
    """Accept what :class:`InstanceSelector` parses, naming only local instance IDs (``valkey-N``)."""

    try:
        selector = InstanceSelector.parse(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"config field {name!r}: {exc}") from None
    invalid = [item for item in selector.instance_ids if not INSTANCE_ID.fullmatch(item)]
    if invalid:
        raise ConfigError(
            f"config field {name!r} must be all, primaries, primary, replicas, replica, or instance IDs such as "
            f"valkey-0; got {invalid}"
        )


def check_shard(shard: int, plan: ExecutionPlan, where: str = "shard") -> None:
    """``shard`` names one of the plan's cluster shards: Valkey cannot fail over one it does not have."""

    primaries = plan.deployment["primaries"]
    if shard >= primaries:
        raise ConfigError(f"{where} {shard} is not below primaries {primaries}; pick a shard from 0 to {primaries - 1}")


def select_instances(handle: Any, value: Any) -> list[str]:
    """Resolve an instance selector against the live deployment: live roles, planned order."""

    selector = InstanceSelector.parse(value)
    roles: dict[str, str] = {}
    if "primary" in selector.roles:
        roles.update(dict.fromkeys(handle.primaries(), "primary"))
    if "replica" in selector.roles:
        roles.update(dict.fromkeys(handle.replicas(), "replica"))
    return list(selector.resolve([instance.instance_id for instance in handle.instances], roles))


def instance_at(handle: Any, host: Any, port: Any) -> Optional[str]:
    """The planned instance that listens at ``host:port``; None for any other address."""

    return next(
        (
            instance.instance_id
            for instance in handle.instances
            if (str(instance.host), str(instance.port)) == (str(host), str(port))
        ),
        None,
    )


def replication_info(handle: Any, instance_id: str, timeout: float) -> Optional[dict[str, Any]]:
    """``INFO replication`` of ``instance_id``; None when it does not answer within ``timeout``."""

    client = None
    try:
        client = handle.new_client(instance_id, timeout=timeout)
        return client.info("replication")
    except Exception:
        return None
    finally:
        close_quietly(client)


def is_primary(info: Optional[Mapping[str, Any]]) -> bool:
    """Whether ``info``, an instance's INFO replication (None: unreachable), reports a primary."""

    return bool(info) and parse_replication(info).role == "primary"


def follows(info: Optional[Mapping[str, Any]], primary: Any) -> bool:
    """Whether ``info``, an instance's INFO replication, is a replica of instance ``primary`` with its link up.

    Host and port both count: per-instance loopback addresses may share ports.
    """

    if not info:
        return False
    link = parse_replication(info)
    return (
        link.role == "replica"
        and link.link_up
        and (link.primary_host, link.primary_port) == (str(primary.host), int(primary.port))
    )


def replica_links(info: Mapping[str, Any], handle: Any) -> list[tuple[Mapping[str, Any], Optional[str]]]:
    """A primary's replica entries (INFO replication's ``slaveN``), each with the instance at its address."""

    return [
        (info[key], instance_at(handle, info[key].get("ip"), info[key].get("port")))
        for key in sorted(info)
        if re.fullmatch(r"slave\d+", key) and isinstance(info[key], Mapping)
    ]


def wait_following(
    context: Any, handle: Any, ids: Sequence[str], primary: str, timeout_s: float
) -> tuple[bool, dict[str, bool]]:
    """Wait until every live instance of ``ids`` but ``primary`` :func:`follows` it: ``(all do, by instance)``."""

    target = handle.instance(primary)

    def check() -> tuple[bool, dict[str, bool]]:
        status = {
            item: follows(replication_info(handle, item, 0.5), target)
            for item in ids
            if item != primary and handle.is_alive(item)
        }
        return all(status.values()), status

    return poll_until(check, timeout_s=timeout_s, interval_s=0.1, context=context)


def graceful_failover(
    context: Any, handle: Any, primary: str, target: str, timeout_s: float
) -> tuple[bool, Optional[str]]:
    """Hand standalone ``primary``'s role to its replica ``target``: ``FAILOVER TO`` with a ``timeout_s`` limit.

    Valkey stops writes until ``target`` caught up, so nothing acknowledged is
    lost. The roles get ``timeout_s`` plus 2 seconds to swap; then ``FAILOVER
    ABORT`` stops a failover still in progress, so ``primary`` takes writes
    again. Returns whether ``target`` took over, and the error ``FAILOVER TO``
    raised, if any (a timeout may follow a failover that started, so the
    roles decide).

    ``target`` took over when it leads and ``primary`` does not, or when
    ``primary`` follows it with its failover state cleared, which Valkey does
    only once ``target`` accepted the handover: a ``target`` that died right
    after it (killed by a concurrent fault) was still promoted.
    """

    address = handle.instance(target)

    def swapped() -> bool:
        old = replication_info(handle, primary, 1.0)
        if old and not is_primary(old):
            link = parse_replication(old)
            state = old.get("master_failover_state", b"")
            handed_over = (state.decode() if isinstance(state, bytes) else str(state)) == "no-failover"
            if handed_over and (link.primary_host, link.primary_port) == (str(address.host), int(address.port)):
                return True
        return is_primary(replication_info(handle, target, 1.0)) and not is_primary(old)

    def send(*command: Any) -> None:
        client = handle.new_client(primary, timeout=5.0)
        try:
            client.execute_command(*command)
        finally:
            close_quietly(client)

    error = None
    try:
        send("FAILOVER", "TO", address.host, address.port, "TIMEOUT", max(1, int(timeout_s * 1000)))
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
    if poll_until(lambda: (swapped(), None), timeout_s=timeout_s + 2.0, context=context)[0]:
        return True, error
    try:
        send("FAILOVER", "ABORT")
    except Exception:
        pass
    return swapped(), error


def restart_once(handle: Any, instance_id: str, **restart: Any) -> Callable[[], None]:
    """The undo of a stopped instance: restart it once with ``restart``'s arguments.

    A restart also stops a process that has not exited yet; once it succeeded,
    the undo does nothing more.
    """

    restarted = []

    def undo() -> None:
        if not restarted:
            handle.restart(instance_id, **restart)
            restarted.append(instance_id)

    return undo


def resume_if_alive(handle: Any, instance_id: str) -> None:
    """The undo of a paused instance: SIGCONT it, unless its process is gone."""

    if handle.is_alive(instance_id):
        handle.send_signal(instance_id, signal.SIGCONT)


def stop_owned(own: Own, handle: Any, instance_id: str, signum: int = signal.SIGTERM, **restart: Any) -> Any:
    """Send ``instance_id`` ``signum``; the stop is owned (``process.stopped``) until it restarts once with
    ``restart``'s arguments, the returned obligation's discharge."""

    handle.send_signal(instance_id, signum)
    return own("process.stopped", instance_id, restart_once(handle, instance_id, **restart))


# How often a stop is signalled again while the instance has not exited.
RESIGNAL_INTERVAL_S = 1.0


def wait_exit_signalling(handle: Any, instance_id: str, timeout_s: float, signum: int = signal.SIGTERM) -> bool:
    """Wait up to ``timeout_s`` for ``instance_id`` to exit, sending ``signum`` again every second until it does.

    Valkey drops a SIGTERM it cannot act on yet and never acts on it later:
    while it writes its initial AOF ("Writing initial AOF, can't exit", as a
    replica does right after a full sync) or when its shutdown save fails.
    A repeated SIGTERM is harmless once a shutdown is under way.
    """

    deadline = time.monotonic() + timeout_s
    while not handle.wait_exit(instance_id, timeout=max(0.0, min(RESIGNAL_INTERVAL_S, deadline - time.monotonic()))):
        if time.monotonic() >= deadline:
            return False
        try:
            handle.send_signal(instance_id, signum)
        except ProcessLookupError:
            pass  # It exited meanwhile: the next wait reaps it.
    return True


def pause_owned(own: Own, handle: Any, instance_id: str) -> Any:
    """SIGSTOP ``instance_id``; the pause is owned (``process.paused``) until it is resumed, unless it is gone."""

    handle.send_signal(instance_id, signal.SIGSTOP)
    return own("process.paused", instance_id, lambda: resume_if_alive(handle, instance_id))


def unreachable_instances(handle: Any) -> list[str]:
    """Instances whose live replication role cannot be read."""

    roles = handle.roles()
    return [instance.instance_id for instance in handle.instances if instance.instance_id not in roles]


def close_quietly(*clients: Any) -> None:
    """Close every client that is not None; a connection that is already gone is not an error."""

    for client in clients:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass


def remaining(context: Any, fallback: float) -> float:
    """Seconds left before the cooperative deadline, bounded by ``fallback``."""

    deadline = context.deadline
    if deadline is None:
        return fallback
    return max(0.0, min(fallback, deadline - time.monotonic()))


def poll_until(
    check: Callable[[], tuple[bool, Any]],
    *,
    timeout_s: float,
    interval_s: float = 0.1,
    context: Any,
) -> tuple[bool, Any]:
    """Call ``check() -> (done, value)`` until done or ``timeout_s`` (bounded by the run deadline).

    Returns the last ``(done, value)``; ``check`` always runs at least once.
    It waits with ``context.sleep``, so a cancelled call returns at once.
    """

    deadline = time.monotonic() + remaining(context, timeout_s)
    while True:
        done, value = check()
        if done or time.monotonic() >= deadline or not context.sleep(interval_s):
            return done, value


def read_retrying(
    context: Any, read: Callable[[], Any], *, timeout_s: float, interval_s: float = 0.2
) -> tuple[Any, Optional[str]]:
    """``read()`` until it returns, within ``timeout_s`` (bounded by the run deadline): ``(value, None)``, or
    ``(None, error)`` with the last error when it never did.

    A client error (a timeout, LOADING, CLUSTERDOWN while the cluster converges, a host out of local ports)
    says the harness could not read, not what the server holds: an oracle retries it, and returns ERROR if it
    persists. ``valkey.oracle.liveness`` judges a server that is down or does not answer.
    """

    def attempt() -> tuple[bool, tuple[Any, Optional[str]]]:
        try:
            return True, (read(), None)
        except Exception as exc:
            return False, (None, f"{type(exc).__name__}: {exc}")

    return poll_until(attempt, timeout_s=timeout_s, interval_s=interval_s, context=context)[1]


def poll_each(
    context: Any,
    ids: Sequence[str],
    check: Callable[[str], tuple[bool, Any]],
    *,
    timeout_s: float,
    interval_s: float = 0.1,
) -> dict[str, tuple[bool, Any]]:
    """:func:`poll_until` ``check(id)`` for each of ``ids`` in turn, all within one ``timeout_s``: ``{id: (done, value)}``.

    The budget is shared, so many instances that each fail slowly cannot
    stretch a check past ``timeout_s`` (or the run deadline); each id is still
    checked at least once.
    """

    deadline = time.monotonic() + remaining(context, timeout_s)
    return {
        item: poll_until(
            lambda item=item: check(item),
            timeout_s=max(0.0, deadline - time.monotonic()),
            interval_s=interval_s,
            context=context,
        )
        for item in ids
    }


# How the client reports a connection it could not open ("Error 49 connecting to 127.0.0.1:21120. Can't
# assign requested address.", "Timeout connecting to server"): nothing was sent.
_NOT_CONNECTED = re.compile(r"\bconnecting to\b")


def send_command(client: Any, command: Sequence[Any], *, timeout_s: float, context: Any) -> Any:
    """Send ``command`` once and return the reply.

    Until the connection is open nothing has reached the server, so a connection
    that could not be opened (a host out of local ports under load, a server not
    accepting yet) is retried until ``timeout_s``, bounded by the run deadline;
    then its error is raised. Any other error, a lost connection included, is
    raised at once, and the command is never sent twice.
    """

    deadline = time.monotonic() + remaining(context, timeout_s)
    while True:
        try:
            return client.execute_command(*[str(item) for item in command])
        except (valkey.exceptions.ConnectionError, valkey.exceptions.TimeoutError) as exc:
            if not _NOT_CONNECTED.search(str(exc)) or time.monotonic() >= deadline or not context.sleep(0.1):
                raise


def send_to(handle: Any, instance_id: str, command: Sequence[Any], *, timeout_s: float, context: Any) -> dict[str, Any]:
    """:func:`send_command` on a new client of ``instance_id``: ``{"reply": <as JSON data>}`` or ``{"error": ...}``."""

    with handle.new_client(instance_id, timeout=timeout_s) as client:
        try:
            return {"reply": jsonable(send_command(client, command, timeout_s=timeout_s, context=context))}
        except Exception as exc:
            return {"error": f"{type(exc).__name__}: {exc}"}


def compare(actual: Any, rule: Mapping[str, Any]) -> bool:
    """Evaluate one scalar predicate against an observed value."""

    if "exists" in rule:
        return (actual is not None) is bool(rule["exists"])
    if actual is None:
        return False
    if "equals" in rule:
        return str(actual) == str(rule["equals"])
    if "not_equals" in rule:
        return str(actual) != str(rule["not_equals"])
    if "in" in rule:
        return str(actual) in {str(item) for item in rule["in"]}
    if "contains" in rule:
        return str(rule["contains"]) in str(actual)
    numeric = _as_number(actual)
    if numeric is None:
        return False
    if "min" in rule and numeric < float(rule["min"]):
        return False
    if "max" in rule and numeric > float(rule["max"]):
        return False
    return "min" in rule or "max" in rule


# A scalar predicate's fields (:func:`compare`): one of exists, equals, not_equals, in, and contains, or min,
# max, or both (:func:`validate_predicate`).
_SCALAR = (str, int, float, bool)
PREDICATE_FIELDS = {
    "exists": optional(bool),
    "equals": optional(_SCALAR),
    "not_equals": optional(_SCALAR),
    "in": optional(list, items=ConfigField(_SCALAR)),
    "contains": optional(_SCALAR),
    "min": optional(float),
    "max": optional(float),
}
PREDICATE_KEYS = tuple(PREDICATE_FIELDS)


def validate_predicate(rule: Mapping[str, Any], where: str) -> None:
    present = [key for key in PREDICATE_KEYS if key in rule]
    if not present:
        raise ConfigError(f"{where} needs one predicate of {list(PREDICATE_KEYS)}")
    if len([key for key in present if key not in ("min", "max")]) > 1:
        raise ConfigError(f"{where} may combine only min and max")


def _as_number(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


class StoppableThread:
    """A daemon worker that is always joined by its owner."""

    def __init__(self, name: str, target: Callable[[threading.Event], None]):
        self.stop_event = threading.Event()
        self.error: Optional[BaseException] = None

        def run() -> None:
            try:
                target(self.stop_event)
            except BaseException as exc:  # pragma: no cover - surfaced by join()
                self.error = exc

        self._thread = threading.Thread(target=run, name=name, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def stop(self, timeout: float = 30.0) -> bool:
        self.stop_event.set()
        self._thread.join(timeout)
        return not self._thread.is_alive()


def read_jsonl(path: str) -> list[dict[str, Any]]:
    entries = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                entries.append(json.loads(line))
    return entries


def write_jsonl(path: str, entries: Iterable[Mapping[str, Any]]) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        for entry in entries:
            handle.write(json.dumps(jsonable(entry), sort_keys=True))
            handle.write("\n")
    os.replace(tmp, path)


def canonical_value(client: Any, key: str) -> Optional[list]:
    """Return ``[type, content]`` with order-insensitive types sorted, or None."""

    kind = client.type(key)
    if isinstance(kind, bytes):
        kind = kind.decode()
    if kind == "none":
        return None
    if kind == "string":
        content: Any = client.get(key)
    elif kind == "hash":
        content = sorted(client.hgetall(key).items())
    elif kind == "list":
        content = client.lrange(key, 0, -1)
    elif kind == "set":
        content = sorted(client.smembers(key))
    elif kind == "zset":
        content = [[member, repr(score)] for member, score in client.zrange(key, 0, -1, withscores=True)]
    elif kind == "stream":
        content = [[entry_id, sorted(fields.items())] for entry_id, fields in client.xrange(key)]
    else:
        content = f"<{kind}>"
    return [kind, jsonable(content)]


def keyspace_snapshot(client: Any, match: Optional[str] = None, count: int = 1000) -> dict[str, list]:
    """Canonical content of every key on one instance (optionally a SCAN pattern)."""

    snapshot: dict[str, list] = {}
    for key in client.scan_iter(match=match, count=count):
        if isinstance(key, bytes):
            key = key.decode("utf-8", errors="surrogateescape")
        value = canonical_value(client, key)
        if value is not None:
            snapshot[key] = value
    return snapshot


def keyspace_digest(snapshot: Mapping[str, Any]) -> str:
    material = json.dumps(sorted(snapshot.items()), separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(material.encode("utf-8", errors="surrogatepass")).hexdigest()


def spec_mapping(name: str) -> Any:
    """The ``mapping.yaml`` of spec bundle ``name``; a bundle that cannot be read is a config error."""

    try:
        return load_spec_mapping(name)
    except SpecBundleError as exc:
        raise ConfigError(f"spec {name!r}: {exc}") from exc


# The digest of the mapping.yaml that ``spec`` names, filled in when the scenario compiles (SpecSchema).
SPEC_SHA256_FIELD = ConfigField(str, allow_none=True, default=None)


class SpecSchema(ConfigSchema):
    """The config of a component that reads its ``spec`` bundle's mapping.yaml when it runs.

    Resolving fills in ``spec_sha256``, so the plan, and its digest, name
    the mapping it was compiled against: a pattern or event name the
    mapping changes then changes the plan, and replay sees it.
    """

    def resolve(self, config: Mapping[str, Any], location: str) -> dict[str, Any]:
        resolved = super().resolve(config, location)
        if resolved["spec"] and resolved["spec_sha256"] is None:
            resolved["spec_sha256"] = spec_mapping(resolved["spec"]).sha256
        return resolved


def pinned_spec_mapping(config: Mapping[str, Any]) -> Any:
    """The mapping ``config["spec"]`` names, which must be the one the plan was compiled against."""

    mapping = spec_mapping(config["spec"])
    pinned = config["spec_sha256"]
    if pinned is not None and mapping.sha256 != pinned:
        raise ConfigError(
            f"spec {config['spec']!r}: {mapping.path} has sha256 {mapping.sha256}, but the plan was compiled "
            f"against {pinned}; compile the scenario again, or restore that mapping"
        )
    return mapping


def nodes_conf_path(handle: Any, node_id: str) -> str:
    return os.path.join(handle.data_dir(node_id), "nodes.conf")


def read_cluster_vars(path: str) -> Optional[dict[str, int]]:
    """Return the ``vars`` line of a nodes.conf (currentEpoch, lastVoteEpoch), or None."""

    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if line.startswith("vars "):
                    tokens = line.split()[1:]
                    values = {}
                    for name, value in zip(tokens[::2], tokens[1::2]):
                        try:
                            values[name] = int(value)
                        except ValueError:
                            continue
                    return values
    except OSError:
        return None
    return None


# A Valkey log line in the legacy format: ``pid:role dd Mon yyyy hh:mm:ss.mmm level message``, where the level is
# ``.`` (debug), ``-`` (verbose), ``*`` (notice), or ``#`` (warning).
LOG_LINE = re.compile(
    r"^\d+:[A-Za-z] (?P<time>\d{1,2} \w{3} \d{4} \d{2}:\d{2}:\d{2})\.(?P<ms>\d{3}) (?P<level>[#*.-]) (?P<message>.*)$"
)


def log_line_time(line: str) -> Optional[float]:
    """Wall-clock time of a legacy-format Valkey log line (:data:`LOG_LINE`)."""

    match = LOG_LINE.match(line)
    if match is None:
        return None
    try:
        whole = time.mktime(time.strptime(match["time"], "%d %b %Y %H:%M:%S"))
    except ValueError:
        return None
    return whole + int(match["ms"]) / 1000.0
