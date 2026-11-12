"""The server config a Valkey build has, read from its own config table, and the share of it a scenario draws.

Every standard config of Valkey is one entry of ``static_configs[]`` in ``src/config.c``: a ``create<Type>Config``
call with its name, alias, flags (``IMMUTABLE_CONFIG``, ``PROTECTED_CONFIG``, ``DEBUG_CONFIG``, ``HIDDEN_CONFIG``,
...), and, by type, its enum table, its bounds and its default. :func:`parse_config_table` reads them, and
:func:`build_config_table` reads them for a built binary from the ``config.c`` beside it (``<tree>/src``, where
``make`` leaves ``valkey-server``), so a config a change adds is drawn as soon as the build has it.

The default pool ``valkey.pool.config-space`` (``valkey.random``) draws from it through the generator hook
``valkey.generator.config-space`` (:func:`config_space_pool`): the pool's ``share`` of the space joins a scenario,
each config chosen by its name alone, so a config the build adds changes no other config's draws. A boolean draws
yes or no, an enum one of its values, a number a value of its range shaped toward the bounds, 0 and 1, and the
default (see :mod:`valkey_fuzzer.generator`). Strings and the special configs (``save``,
``client-output-buffer-limit``, ...) are not drawn: their values have no type to draw them from. The hand-written
pools come first, so a config one of them draws keeps the values chosen for it.

A scenario that runs several builds draws from what all of them have (:func:`config_space`); one whose build has
no ``config.c`` beside it draws nothing from the table.

The space leaves out the options the local deployment sets itself (ports, addresses, paths, ``daemonize``, the
provider's defaults), and :data:`EXCLUDED`, each with its reason: configs the harness relies on, configs that change
what the oracles judge in a way Valkey declares (a refused write, an evicted key, a disconnected client, a lost write
within a declared window), and configs a hand-written pool or an experiment draws with values it chose.
"""

import ast
import functools
import logging
import operator
import os
import re
import shutil
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any, Optional

import valkey

from valkey_fuzzer._data import to_plain_data
from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.deployment.local import processes
from valkey_fuzzer.deployment.local.server_config import (
    CLUSTER_DEFAULT_SERVER_CONFIG,
    DEFAULT_SERVER_CONFIG,
    MANAGED_SERVER_OPTIONS,
)
from valkey_fuzzer.generator import draw_rule
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.results import ComponentStatus, make_evidence, make_result
from valkey_fuzzer.schema import ConfigField

from .instances import close_quietly, deployment
from .polling import StoppableThread
from .registration import component_registration
from .schemas import INTEGER, LIST, declared, mapping_of

logger = logging.getLogger(__name__)

# Why each config is never drawn from the table.
_HARNESS = "the deployment or the harness sets or relies on it"
_PROCESS = "it changes the process or the host, not the server's execution"
_CLIENTS = "it disconnects or refuses the harness's own clients"
_REFUSES = "it refuses commands the workloads expect to succeed"
_REPLIES = "it changes replies the oracles compare in a way Valkey declares"
_DURABILITY = "it changes what a crash or a restart may lose, which the ledger's loss windows encode"
_TOPOLOGY = "it changes who is primary or which replicas sync, which the experiments decide"
_POOL = "a hand-written pool or an experiment draws it with values chosen for it"
EXCLUDED: Mapping[str, str] = {
    # The harness's process, addressing and access, besides the options the deployment sets itself (see
    # _deployment_owned).
    "cluster-announce-client-ipv4": _HARNESS,
    "cluster-announce-client-ipv6": _HARNESS,
    "cluster-announce-client-port": _HARNESS,
    "cluster-announce-client-tls-port": _HARNESS,
    "dbfilename": _HARNESS,
    "appenddirname": _HARNESS,
    "tls-cluster": _HARNESS,
    "tls-replication": _HARNESS,
    "unixsocketgroup": _HARNESS,
    "rdma-port": _HARNESS,
    "rdma-bind": _HARNESS,
    "rdma-rx-size": _HARNESS,
    "rdma-completion-vector": _HARNESS,
    "requirepass": _HARNESS,
    "primaryauth": _HARNESS,
    "primaryuser": _HARNESS,
    "aclfile": _HARNESS,
    "enable-debug-command": _HARNESS,
    "enable-module-command": _HARNESS,
    "enable-protected-configs": _HARNESS,
    "loglevel": "the log observers match notice-level lines",
    "log-format": "the log observers match Valkey's legacy log lines",
    "log-timestamp-format": "the log observers match Valkey's legacy log lines",
    "crash-log-enabled": "a crash report is how a run tells a crash from a kill",
    "save": "a background save it schedules collides with an experiment's own BGSAVE",
    "rdb-key-save-delay": "a test knob that sleeps for each key saved: a drawn delay stalls a save past the run",
    "key-load-delay": "a test knob that sleeps for each key loaded: a drawn delay stalls a load past the run",
    "watchdog-period": "a debugging knob that logs a stack trace at every event-loop pass longer than it, which a "
    "short one makes most of them",
    # The process and the host.
    "always-show-logo": _PROCESS,
    "set-proc-title": _PROCESS,
    "supervised": _PROCESS,
    "syslog-enabled": _PROCESS,
    "syslog-ident": _PROCESS,
    "syslog-facility": _PROCESS,
    "oom-score-adj": _PROCESS,
    "oom-score-adj-values": _PROCESS,
    "disable-thp": _PROCESS,
    "server-cpulist": _PROCESS,
    "bio-cpulist": _PROCESS,
    "aof-rewrite-cpulist": _PROCESS,
    "bgsave-cpulist": _PROCESS,
    "mptcp": "it needs the host kernel's MPTCP, without which the server refuses to start",
    "repl-mptcp": "it needs the host kernel's MPTCP, without which the server refuses to start",
    "socket-mark-id": "it marks the server's sockets for the host's firewall, which the network faults program",
    "databases": "every database costs memory at the start: a count up to INT_MAX exhausts the host",
    "cluster-databases": "every database costs memory at the start: a count up to INT_MAX exhausts the host",
    "shutdown-on-sigint": "the harness stops servers with signals and expects the default shutdown",
    "shutdown-on-sigterm": "the harness stops servers with signals and expects the default shutdown",
    "shutdown-timeout": "the harness stops servers with signals and expects the default shutdown",
    # The harness's clients.
    "timeout": _CLIENTS,
    "maxclients": _CLIENTS,
    "maxmemory-clients": _CLIENTS,
    "client-query-buffer-limit": _CLIENTS,
    "client-output-buffer-limit": _CLIENTS,
    "proto-max-bulk-len": _CLIENTS,
    "tcp-keepalive": _CLIENTS,
    "tcp-backlog": _CLIENTS,
    "busy-reply-threshold": _CLIENTS,
    # Refused commands.
    "maxmemory": "the provider's cap, which no experiment reaches; item 01c draws one that is reached",
    "maxmemory-scripts": _REFUSES,
    "min-replicas-to-write": _REFUSES,
    "min-replicas-max-lag": _REFUSES,
    "replica-read-only": _REFUSES,
    "replica-serve-stale-data": _REFUSES,
    "stop-writes-on-bgsave-error": _REFUSES,
    "acl-pubsub-default": _REFUSES,
    "cluster-require-full-coverage": _REFUSES,
    "cluster-allow-reads-when-down": _REFUSES,
    "cluster-allow-pubsubshard-when-down": _REFUSES,
    "lua-enable-insecure-api": _REFUSES,
    # Declared changes of replies.
    "notify-keyspace-events": _REPLIES,
    "locale-collate": _REPLIES,
    "extended-redis-compatibility": _REPLIES,
    "availability-zone": _REPLIES,
    "cluster-preferred-endpoint-type": _REPLIES,
    "import-mode": _REPLIES,
    # Durability.
    "appendonly": _DURABILITY,
    "appendfsync": _DURABILITY,
    "aof-load-truncated": _DURABILITY,
    "rdbchecksum": _DURABILITY,
    "aof-use-rdb-preamble": _POOL,
    # Topology.
    "replica-priority": _TOPOLOGY,
    "cluster-replica-no-failover": _TOPOLOGY,
    "cluster-allow-replica-migration": _TOPOLOGY,
    "cluster-migration-barrier": _TOPOLOGY,
    "cluster-replica-validity-factor": _TOPOLOGY,
    "cluster-node-timeout": _TOPOLOGY,
    "repl-timeout": _TOPOLOGY,
    "repl-ping-replica-period": _TOPOLOGY,
    "cluster-ping-interval": _TOPOLOGY,
    "cluster-manual-failover-timeout": "a short one makes the manual failovers the experiments ask for time out",
    "cluster-link-sendbuf-limit": "a small one frees every cluster bus link that has output pending",
    "repl-backlog-size": "a small backlog turns the partial resyncs the replication experiments expect into full ones",
    "repl-backlog-ttl": "a short one turns the partial resyncs the replication experiments expect into full ones",
    # Hand-written pools (valkey.random) and experiments.
    "repl-diskless-sync": _POOL,
    "repl-diskless-load": _POOL,
    "dual-channel-replication-enabled": _POOL,
    "repl-diskless-sync-delay": _POOL,
    # The codecs and forkless saves left the base (00c-base-only).
    "rdbcompression": "a build without zstd refuses zstd at the start; the base leaves the codecs out",
    "repl-compression": "a build without zstd refuses zstd at the start; the base leaves the codecs out",
    "forkless-policy": "prefer-forkless needs forkless-infrastructure-enabled at the start; the base leaves forkless "
    "saves out (and #5 fails every one under lz4)",
}

# The C limits a bound may name, as a 64-bit Linux build has them.
_LIMITS = {
    "INT_MIN": -(2**31),
    "INT_MAX": 2**31 - 1,
    "UINT_MAX": 2**32 - 1,
    "LONG_MIN": -(2**63),
    "LONG_MAX": 2**63 - 1,
    "ULONG_MAX": 2**64 - 1,
    "LLONG_MIN": -(2**63),
    "LLONG_MAX": 2**63 - 1,
    "ULLONG_MAX": 2**64 - 1,
    "SIZE_MAX": 2**64 - 1,
    "SSIZE_MAX": 2**63 - 1,
}
# The kind of each create<Type>Config.
_NUMBERS = ("Int", "UInt", "Long", "ULong", "LongLong", "ULongLong", "SizeT", "SSizeT", "TimeT", "OffT")
_KINDS = {
    "Bool": "bool",
    "Enum": "enum",
    "String": "string",
    "SDS": "string",
    "Special": "special",
    **dict.fromkeys(_NUMBERS, "numeric"),
}


@dataclass(frozen=True)
class ServerConfig:
    """One entry of a build's config table.

    ``kind`` is ``bool``, ``enum``, ``numeric``, ``string`` or ``special``; ``flags`` the config's flags without
    their ``_CONFIG`` suffix (``IMMUTABLE``, ``PROTECTED``, ``DEBUG``, ``HIDDEN``, ``MULTI_ARG``, ...);
    ``number_flags`` a number's (``MEMORY``, ``PERCENT``, ``OCTAL``, ...). ``values`` lists an enum's values;
    ``minimum``, ``maximum`` and ``default`` are a number's bounds and default, or None where the table names a
    constant this parser cannot evaluate. ``default`` is a boolean's or an enum's default as ``CONFIG GET`` shows
    it.
    """

    name: str
    kind: str
    flags: frozenset = field(default_factory=frozenset)
    alias: Optional[str] = None
    values: tuple[str, ...] = ()
    minimum: Optional[int] = None
    maximum: Optional[int] = None
    default: Any = None
    number_flags: frozenset = field(default_factory=frozenset)

    @property
    def mutable(self) -> bool:
        """Whether ``CONFIG SET`` may change it on a running server the harness deployed."""

        return not self.flags & {"IMMUTABLE", "PROTECTED"}


# ---------------------------------------------------------------------------
# Parsing config.c
# ---------------------------------------------------------------------------


def _strip_comments(text: str) -> str:
    """``text`` without its C comments, keeping string literals (a config name holds no comment marker)."""

    return re.sub(
        r'"(?:\\.|[^"\\])*"|/\*.*?\*/|//[^\n]*', lambda m: m.group(0) if m.group(0)[0] == '"' else " ", text, flags=re.S
    )


def _arguments(text: str, start: int) -> tuple[list[str], int]:
    """The top-level, comma-separated arguments of the call whose ``(`` is at ``start``, and the index after ``)``."""

    depth, current, arguments, index = 0, [], [], start
    in_string = False
    while index < len(text):
        char = text[index]
        if in_string:
            current.append(char)
            if char == "\\":
                current.append(text[index + 1])
                index += 1
            elif char == '"':
                in_string = False
        elif char == '"':
            in_string = True
            current.append(char)
        elif char in "([{":
            depth += 1
            if depth > 1:
                current.append(char)
        elif char in ")]}":
            depth -= 1
            if depth == 0:
                arguments.append("".join(current).strip())
                return arguments, index + 1
            current.append(char)
        elif char == "," and depth == 1:
            arguments.append("".join(current).strip())
            current = []
        else:
            current.append(char)
        index += 1
    raise ValueError("unbalanced parentheses in the config table")


def _defines(texts: Iterable[str]) -> dict[str, str]:
    """Every object-like ``#define NAME value`` of ``texts`` whose value is one line."""

    found: dict[str, str] = {}
    for text in texts:
        for match in re.finditer(r"^[ \t]*#[ \t]*define[ \t]+([A-Za-z_]\w*)[ \t]+([^\n\\]+)$", text, re.M):
            found.setdefault(match.group(1), match.group(2).strip())
    return found


_OPERATORS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.LShift: operator.lshift,
    ast.RShift: operator.rshift,
    ast.BitOr: operator.or_,
    ast.BitAnd: operator.and_,
}
_CAST = re.compile(r"\(\s*(?:unsigned\s+|signed\s+)?(?:long\s+long|long|int|size_t|ssize_t|time_t|off_t|char)\s*\)")


def _evaluate(expression: str, defines: Mapping[str, str], depth: int = 0) -> Optional[int]:
    """The integer value of a C constant expression, or None when it names something that is not a constant."""

    if depth > 8:
        return None
    text = _CAST.sub(" ", expression)
    text = re.sub(r"\b(0[xX][0-9a-fA-F]+|\d+)[uUlL]*\b", r"\1", text).replace("/", "//")
    text = re.sub(r"\b0([0-7]+)\b", r"0o\1", text)
    try:
        tree = ast.parse(text.strip(), mode="eval")
    except SyntaxError:
        return None

    def value(node: ast.AST) -> Optional[int]:
        if isinstance(node, ast.Constant) and isinstance(node.value, int) and not isinstance(node.value, bool):
            return node.value
        if isinstance(node, ast.Name):
            if node.id in _LIMITS:
                return _LIMITS[node.id]
            if node.id in defines:
                return _evaluate(defines[node.id], defines, depth + 1)
            return None
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
            operand = value(node.operand)
            return None if operand is None else (-operand if isinstance(node.op, ast.USub) else operand)
        if isinstance(node, ast.BinOp) and type(node.op) in _OPERATORS:
            left, right = value(node.left), value(node.right)
            if left is None or right is None:
                return None
            try:
                return _OPERATORS[type(node.op)](left, right)
            except (ZeroDivisionError, ValueError):
                return None
        return None

    return value(tree.body)


def _string(argument: str) -> Optional[str]:
    match = re.fullmatch(r'"((?:\\.|[^"\\])*)"', argument.strip())
    return match.group(1) if match else None


def _flags(argument: str, suffix: str) -> frozenset:
    return frozenset(
        token[: -len(suffix)]
        for token in re.findall(r"[A-Z_]+", argument)
        if token.endswith(suffix) and token != suffix
    ) - {"INTEGER", "MODIFIABLE"}


def _enum_tables(text: str) -> dict[str, tuple[tuple[str, str], ...]]:
    """Every ``configEnum <name>[] = {{"value", CONSTANT}, ..., {NULL, 0}}``: its values and their constants."""

    tables = {}
    for match in re.finditer(r"configEnum\s+(\w+)\s*\[\]\s*=\s*\{", text):
        body, _ = _arguments(text, match.end() - 1)
        entries = []
        for entry in body:
            pair = re.fullmatch(r'\{\s*"([^"]*)"\s*,\s*([^}]+?)\s*\}', entry.strip())
            if pair:
                entries.append((pair.group(1), pair.group(2)))
        tables[match.group(1)] = tuple(entries)
    return tables


def parse_config_table(config_c: str, headers: Sequence[str] = ()) -> tuple[ServerConfig, ...]:
    """The standard configs of ``static_configs[]`` in the text of a ``config.c``, in table order.

    ``headers`` are the texts of the build's other sources, for the constants a bound or default names. An entry
    inside an ``#if`` block is left out: whether the build has it depends on how it was built.
    """

    text = _strip_comments(config_c)
    start = re.search(r"standardConfig\s+static_configs\s*\[\]\s*=\s*\{", text)
    if start is None:
        raise ValueError("config.c has no static_configs[] table")
    defines = _defines([text, *(_strip_comments(header) for header in headers)])
    enums = _enum_tables(text)
    opening = start.end() - 1
    body = text[opening : _arguments(text, opening)[1]]
    conditional = _conditional_spans(body)
    configs = []
    for match in re.finditer(r"\bcreate(\w+)Config\s*\(", body):
        kind = _KINDS.get(match.group(1))
        if kind is None or any(low <= match.start() < high for low, high in conditional):
            continue
        arguments, _ = _arguments(body, match.end() - 1)
        name, alias = _string(arguments[0]), _string(arguments[1])
        if name is None:
            continue
        config = ServerConfig(name=name, kind=kind, flags=_flags(arguments[2], "_CONFIG"), alias=alias)
        if kind == "bool":
            default = _evaluate(arguments[4], defines)
            config = replace(config, default=None if default is None else ("yes" if default else "no"))
        elif kind == "enum":
            entries = enums.get(arguments[3], ())
            constant = arguments[5].strip()
            default = next((value for value, named in entries if named == constant), None)
            if default is None:
                number = _evaluate(constant, defines)
                default = next((value for value, named in entries if _evaluate(named, defines) == number), None)
            config = replace(config, values=tuple(value for value, _ in entries), default=default)
        elif kind == "numeric":
            config = replace(
                config,
                minimum=_evaluate(arguments[3], defines),
                maximum=_evaluate(arguments[4], defines),
                default=_evaluate(arguments[6], defines),
                number_flags=_flags(arguments[7], "_CONFIG"),
            )
        configs.append(config)
    return tuple(configs)


def _conditional_spans(body: str) -> list[tuple[int, int]]:
    """The spans of ``body`` inside a preprocessor conditional (``#if``/``#ifdef`` to its ``#endif``)."""

    spans, opened = [], []
    for match in re.finditer(r"^[ \t]*#[ \t]*(if|ifdef|ifndef|endif)\b", body, re.M):
        if match.group(1) == "endif":
            if opened:
                start = opened.pop()
                if not opened:
                    spans.append((start, match.end()))
        else:
            opened.append(match.start())
    return spans


# ---------------------------------------------------------------------------
# A build's table
# ---------------------------------------------------------------------------


def config_source(binary: str) -> Optional[str]:
    """The ``config.c`` beside ``binary`` (a binary ``make`` left in ``<tree>/src``), or None."""

    path = shutil.which(binary) or binary
    if not os.path.isfile(path):
        return None
    source = os.path.join(os.path.dirname(os.path.realpath(path)), "config.c")
    return source if os.path.isfile(source) else None


def build_config_table(binary: str) -> Optional[tuple[ServerConfig, ...]]:
    """The config table of ``binary``'s build, read from the ``config.c`` beside it; None when there is none."""

    source = config_source(binary)
    if source is None:
        return None
    return _read_table(source, os.stat(source).st_mtime_ns)


@functools.cache
def _read_table(source: str, mtime_ns: int) -> Optional[tuple[ServerConfig, ...]]:
    directory = os.path.dirname(source)
    headers = []
    for name in sorted(os.listdir(directory)):
        if name.endswith(".h"):
            with open(os.path.join(directory, name), encoding="utf-8", errors="replace") as handle:
                headers.append(handle.read())
    with open(source, encoding="utf-8", errors="replace") as handle:
        text = handle.read()
    try:
        return parse_config_table(text, headers)
    except ValueError as exc:
        logger.warning("cannot read the config table of %s: %s", source, exc)
        return None


# ---------------------------------------------------------------------------
# The space a scenario draws from
# ---------------------------------------------------------------------------


def _deployment_owned() -> frozenset:
    """The options the local deployment sets itself (:data:`MANAGED_SERVER_OPTIONS`) or gives every instance a
    value of its own (the provider's defaults), which the space leaves to it."""

    return frozenset(MANAGED_SERVER_OPTIONS) | set(DEFAULT_SERVER_CONFIG) | set(CLUSTER_DEFAULT_SERVER_CONFIG)


def drawable(config: ServerConfig) -> bool:
    """Whether the table draws ``config``: a boolean, an enum, or a number with known bounds, that is neither
    excluded (:data:`EXCLUDED`), the deployment's own, protected, nor a config of several arguments."""

    owned = _deployment_owned()
    if config.name in EXCLUDED or config.name in owned or config.alias in owned:
        return False
    if "MULTI_ARG" in config.flags or "PROTECTED" in config.flags:
        return False
    if config.kind in ("bool", "enum"):
        return bool(config.values) or config.kind == "bool"
    return config.kind == "numeric" and config.minimum is not None and config.maximum is not None


def config_space(tables: Sequence[Sequence[ServerConfig]]) -> dict[str, ServerConfig]:
    """The drawable configs every table of ``tables`` has, by name: an enum with the values all of them have, a
    number with the range all of them accept."""

    space: Optional[dict[str, ServerConfig]] = None
    for table in tables:
        configs = {config.name: config for config in table if drawable(config)}
        if space is None:
            space = configs
            continue
        merged = {}
        for name, config in space.items():
            other = configs.get(name)
            if other is None or other.kind != config.kind:
                continue
            if config.kind == "enum":
                values = tuple(value for value in config.values if value in other.values)
                if not values:
                    continue
                config = replace(config, values=values)
            elif config.kind == "numeric":
                low, high = max(config.minimum, other.minimum), min(config.maximum, other.maximum)
                if low > high:
                    continue
                config = replace(config, minimum=low, maximum=high)
            merged[name] = replace(config, flags=config.flags | other.flags)
        space = merged
    return space or {}


def rule(config: ServerConfig) -> Any:
    """How the generator draws ``config`` (a pool's value list, or a range with its default)."""

    if config.kind == "bool":
        return ["no", "yes"]
    if config.kind == "enum":
        return list(config.values)
    low, high = config.minimum, config.maximum
    if "PERCENT" in config.number_flags and low < 0:
        # A negative value is a percentage, which only the "N%" form sets: the range starts at 0.
        low = 0
    if "UNSIGNED" not in config.number_flags:
        # Valkey reads the value as a long long unless the config is flagged UNSIGNED_CONFIG, whatever its
        # declared bound (cluster-blacklist-ttl declares ULONG_MAX and refuses 2^63).
        high = min(high, _LIMITS["LLONG_MAX"])
    document = {"minimum": low, "maximum": high}
    if config.default is not None and low <= config.default <= high:
        document["default"] = config.default
    return document


def scenario_builds(scenario: Mapping[str, Any]) -> list[str]:
    """The Valkey binaries ``scenario`` names: its deployment's, its instances', and its local-binary artifacts'.

    A deployment that names none runs the provider's default build (``valkey-server`` on PATH, or the one it
    built), which is returned instead.
    """

    deployment = scenario.get("deployment") or {}
    named = [deployment.get("valkey_binary"), *(deployment.get("instance_binaries") or {}).values()]
    for artifact in (scenario.get("artifacts") or {}).values():
        if isinstance(artifact, Mapping) and artifact.get("use") == "valkey.artifact.local-binary":
            named.append((artifact.get("with") or {}).get("path"))
    builds = sorted({path for path in named if isinstance(path, str) and path})
    if not deployment.get("valkey_binary"):
        default = processes.installed_valkey_binary()
        if default:
            builds.append(default)
    return builds


def scenario_space(scenario: Mapping[str, Any]) -> dict[str, ServerConfig]:
    """The config space of every build ``scenario`` runs; empty when one of them has no table."""

    tables = []
    for binary in scenario_builds(scenario):
        table = build_config_table(binary)
        if table is None:
            logger.info("%s has no config.c beside it: the scenario draws no config from a config table", binary)
            return {}
        tables.append(table)
    return config_space(tables)


def config_space_pool(rng: Any, scenario: Mapping[str, Any]) -> dict[str, Any]:
    """``valkey.generator.config-space``: every config of the scenario's space, with its rule."""

    return {name: rule(config) for name, config in sorted(scenario_space(scenario).items())}


# ---------------------------------------------------------------------------
# The config-churn workload
# ---------------------------------------------------------------------------

CHURN_EVIDENCE = "valkey.workload.config-churn"


class ConfigChurnWorkload:
    """``CONFIG SET`` of the config space on running instances, ``changes`` times, one every ``interval_s``.

    Each change picks an instance, a config of its build's space (:func:`config_space`) that can change at run
    time (not ``IMMUTABLE`` or ``PROTECTED``) and that the instance's planned config leaves to Valkey's default,
    so it never undoes what the experiment, a pool, or the table draw set; and a value drawn as the generator draws
    one. It runs beside the other workloads and the faults until teardown. A change lasts until the deployment is
    removed, and a restarted instance starts again with its planned config. What Valkey answers is recorded, not
    judged: an error reply (a value a validator refuses, a config refused while loading) is a refusal Valkey
    declares, a connection that fails meets an instance a fault stopped. The oracles judge what the changes do.
    """

    def __init__(self) -> None:
        self._thread: Any = None
        self._changes: list[dict[str, Any]] = []

    def start(self, context: Any, spec: ComponentSpec):
        config = spec.config
        if config["changes"] == 0:
            return make_result(spec, ComponentStatus.PASS, "changes 0: no config changed")
        handle = deployment(context)
        planned = {
            instance_id: set(instance.get("server_config") or {})
            for instance_id, instance in to_plain_data(context.plan.instances).items()
        }
        rng = context.rng(spec.id, "changes")

        def space(instance_id: str) -> list[ServerConfig]:
            table = build_config_table(handle.binary(instance_id))
            configs = config_space([table]) if table else {}
            return [
                item
                for name, item in sorted(configs.items())
                if item.mutable and name not in planned.get(instance_id, ())
            ]

        def change() -> None:
            instance_id = rng.choice(sorted(instance.instance_id for instance in handle.instances))
            candidates = space(instance_id)
            record: dict[str, Any] = {"at": time.time(), "instance": instance_id}
            if not candidates:
                self._changes.append({**record, "skipped": "its build has no config table, or none left to change"})
                return
            item = rng.choice(candidates)
            value = draw_rule(rule(item), rng, item.name)
            record.update(config=item.name, value=str(value))
            client = None
            try:
                client = handle.new_client(instance_id, timeout=config["op_timeout_s"])
                client.config_set(item.name, value)
                record["reply"] = "OK"
            except valkey.exceptions.ResponseError as exc:
                record["error"] = str(exc)
            except (valkey.exceptions.ValkeyError, OSError) as exc:
                record["error"] = f"{type(exc).__name__}: {exc}"
                record["unanswered"] = True
            finally:
                close_quietly(client)
            self._changes.append(record)

        def loop(stop_event: Any) -> None:
            while len(self._changes) < config["changes"] and not stop_event.wait(config["interval_s"]):
                change()

        self._thread = StoppableThread(f"workload-{spec.id}", loop)
        self._thread.start()
        return make_result(
            spec, ComponentStatus.PASS, f"changing {config['changes']} configs, one every {config['interval_s']:g}s"
        )

    def stop(self, context: Any, spec: ComponentSpec):
        problem = None
        if self._thread is not None:
            problem = self._thread.stop(timeout=max(30.0, spec.config["op_timeout_s"] * 3))
            if self._thread.is_alive():
                return make_result(spec, ComponentStatus.ERROR, f"workload {problem}")
        errors = sum(1 for record in self._changes if "error" in record and not record.get("unanswered"))
        unanswered = sum(1 for record in self._changes if record.get("unanswered"))
        summary = {"changes": self._changes, "errors": errors, "unanswered": unanswered}
        evidence = (make_evidence(spec, CHURN_EVIDENCE, "workload.quiesced", summary),)
        text = f"{len(self._changes)} config changes: {errors} refused, {unanswered} unanswered"
        if problem is not None:
            return make_result(spec, ComponentStatus.ERROR, f"workload {problem}", evidence=evidence)
        return make_result(spec, ComponentStatus.PASS, text, evidence=evidence)


_CHURN_FIELDS = {
    # 0 changes nothing (an experiment's input turns the workload on).
    "changes": ConfigField(int, default=0, minimum=0, maximum=1000),
    "interval_s": ConfigField(float, default=1.0, minimum=0.05, maximum=600),
    "op_timeout_s": ConfigField(float, default=2.0, minimum=0.05, maximum=60),
}
_CHURN_DECLARES = Declarations(
    evidence={
        CHURN_EVIDENCE: declared(mapping_of({"changes": LIST, "errors": INTEGER, "unanswered": INTEGER})),
    },
)

REGISTRATIONS = (
    component_registration(
        "valkey.workload.config-churn", ConfigChurnWorkload, _CHURN_FIELDS, declares=_CHURN_DECLARES
    ),
)
