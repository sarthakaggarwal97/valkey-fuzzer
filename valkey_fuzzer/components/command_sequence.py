"""``valkey.action.commands``: a command sequence on the deployment's instances or, with ``compare_with``,
on private servers of two builds whose replies are compared; and the machinery the sequence runs on.

:class:`RawConnection` sends every argument as bytes and keeps every reply as the server sent it; :class:`Side`
is one build's private servers (a :class:`_Node` each), started, named by their placeholders, and stopped with
their logs kept; :class:`_Instance` is a deployment instance the sequence runs on; :func:`run_sequence` runs the
commands on either, and the helpers after :class:`CommandsAction` judge the candidate's replies against their
``expect`` and the parent's. ``checks.py`` runs the checks of a compared sequence on sides of its own, and
``tools.py`` and ``command_table.py`` start their servers and connections through the same machinery.
"""

import base64
import functools
import hashlib
import json
import math
import os
import posixpath
import random
import re
import shutil
import socket
import subprocess
import threading
from collections.abc import Iterator, Mapping, Sequence
from typing import Any, ClassVar, Optional

import valkey
from valkey._parsers.base import SERVER_CLOSED_CONNECTION_ERROR

from valkey_fuzzer._data import canonical_json
from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.deployment.local.ports import ephemeral_port_range
from valkey_fuzzer.deployment.local.server_config import MANAGED_SERVER_OPTIONS, canonicalize_server_config
from valkey_fuzzer.hoststate import OWNER_FILE
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.protocols import component_artifact_dir
from valkey_fuzzer.results import ComponentStatus, make_evidence, make_result
from valkey_fuzzer.schema import BASE64_KEY, ConfigError, ConfigField, plain_config

from .actions import _matches
from .capabilities import Probed, resolve_binary, starts_with
from .instances import deployment, select_instances
from .oracles import CRASH_PATTERNS, crash_cause, fatal_exit, scan_for_crashes
from .polling import poll_until
from .predicates import PREDICATE_FIELDS, validate_predicate
from .registration import component_registration
from .schemas import FLAG, INTEGER, TEXT, declared, mapping_of, optional
from .server import PrivateServer, info_fields, server_arguments

_COMPARE_MODES = ("equal", "unordered", "unordered-pairs", "json", "skip")
_MAX_NODES = 6

# The predicates of valkey.action.command; ``equals`` also takes a list or a mapping, compared as JSON data, or
# null, a null reply.
_SEQUENCE_EXPECT = {
    **PREDICATE_FIELDS,
    "equals": optional((str, int, float, bool, list, dict), allow_none=True),
    "error": optional((bool, str)),
}
_SEQUENCE_COMMAND = mapping_of(
    {
        # A command; or raw: bytes sent as they are (a string as UTF-8, or !!binary), for the protocol parser; or
        # read: the replies waiting on the connection (Pub/Sub messages, pushes); or restart: stop the node
        # gracefully and start it again from its directory and config file.
        "command": ConfigField(list, allow_none=True, default=None),
        "raw": ConfigField((str, dict), allow_none=True, default=None),
        "read": ConfigField(bool, default=False),
        "restart": ConfigField(bool, default=False),
        # The node it goes to, by index (see nodes), and the connection to it, opened on first use; main is the one
        # every command takes unless it names another.
        "node": ConfigField(int, default=0, minimum=0, maximum=_MAX_NODES - 1),
        "connection": ConfigField(str, default="main"),
        "expect": mapping_of(_SEQUENCE_EXPECT, allow_none=True, default=None),
        # Send the command again until its reply meets expect, for this long at most: for asynchronous work the
        # sequence started (a slot migration, a replica's sync). Its last reply is the one recorded.
        "wait_s": ConfigField(float, allow_none=True, default=None, minimum=0.1, maximum=3600),
        # The change under test means this reply to differ from the parent's.
        "expect_difference": ConfigField(bool, default=False),
        "compare": ConfigField(str, default="equal", choices=_COMPARE_MODES),
    }
)
_COMMANDS_FIELDS = {
    "instances": ConfigField((str, list), default="primaries"),
    # A parent build, usually {$ref: artifacts.parent.binary}: the sequence then runs on two private servers,
    # one of the deployment's binary and one of this, never on the deployment's instances.
    "compare_with": ConfigField(str, allow_none=True, default=None, reference=True),
    # Module builds both private servers load (--loadmodule each), usually artifact references; the parent
    # loads modules_parent instead when it is given. modules_parent alone, without compare_with, compares two
    # builds of a module on one Valkey build: both servers run the deployment's binary.
    "modules": ConfigField(list, default=list, reference=True, items=ConfigField(str, reference=True)),
    "modules_parent": ConfigField(
        list, allow_none=True, default=None, reference=True, items=ConfigField(str, reference=True)
    ),
    # Every private server runs in cluster mode: node 0 serves every slot, and the other nodes join it as empty
    # primaries.
    "cluster": ConfigField(bool, default=False),
    # Each side runs on this many private servers; a command names its node.
    "nodes": ConfigField(int, default=1, minimum=1, maximum=_MAX_NODES),
    # Both private servers start with it; an option the parent rejects is dropped on both sides.
    "server_config": ConfigField(dict, default=dict),
    # Written into every private server's directory before it starts, by path relative to it: a string as UTF-8,
    # or !!binary. server_config may name them (aclfile: users.acl).
    "files": ConfigField(dict, default=dict),
    "resp": ConfigField(int, default=2, choices=(2, 3)),
    "timeout_s": ConfigField(float, default=10.0, minimum=0.1, maximum=3600),
    "commands": ConfigField(list, items=_SEQUENCE_COMMAND),
    # False: replies are recorded, never judged (no expect, comparison, or reached change decides the verdict), so
    # only a crash, a protocol error, a command with no reply, and the checks do; for a sequence replayed only for
    # its checks.
    "judge_replies": ConfigField(bool, default=True),
    # Checks that run the sequence again on fresh candidate servers (see checks.py); each is off unless set.
    "checks": mapping_of(
        {
            "variants": ConfigField(bool, default=False),
            "replica": ConfigField(bool, default=False),
            "acl": ConfigField(bool, default=False),
            "concurrency": ConfigField(int, default=0, minimum=0, maximum=16),
            "compat": ConfigField(bool, default=False),
            "faults": ConfigField(bool, default=False),
        },
        allow_none=True,
        default=None,
    ),
}

# Refused in every sequence: they end or restart the server the sequence talks to, or hold its connection.
_REFUSED = {"SHUTDOWN": "it stops the server", "MONITOR": "it holds the connection"}
# Refused on the main connection, which they would hold; allowed on a named one. Each channel of these and of the
# unsubscribing commands is confirmed by a reply of its own, so they take one each.
SUBSCRIBE = ("SUBSCRIBE", "PSUBSCRIBE", "SSUBSCRIBE")
UNSUBSCRIBE = ("UNSUBSCRIBE", "PUNSUBSCRIBE", "SUNSUBSCRIBE")
# What a subscriber is delivered, by its first element and length: a message answers no command.
_DELIVERIES = {b"message": 3, b"smessage": 3, b"pmessage": 4}
_REFUSED_DEBUG = ("RESTART", "CRASH-AND-RECOVER", "RELOAD")
_REFUSED_MODULE = ("LOAD", "LOADEX", "UNLOAD")
# Refused on the deployment's instances, whose topology the deployment owns; allowed on the private servers.
_REFUSED_ON_INSTANCES = ("REPLICAOF", "SLAVEOF", "CLUSTER", "SYNC", "PSYNC", "REPLCONF")
# Commands a refusal names with their subcommand.
_CONTAINER_COMMANDS = ("DEBUG", "MODULE", "CONFIG", "CLUSTER")
# What both private servers run with unless server_config sets it: a crafted RESTORE payload that crashes the
# server is a real bug, and DEBUG is allowed (a DEBUG SEGFAULT crashes the server it is sent to, which is the
# test). An option the parent rejects is dropped on both sides, these included.
PRIVATE_SERVER_DEFAULTS = {"sanitize-dump-payload": "yes", "enable-debug-command": "yes"}
# How long a private server may take to start (a sanitizer build takes seconds, and many times that with dozens
# starting at once), and how long a lost connection waits for its process to exit before the loss counts as
# anything but a crash.
_START_TIMEOUT_S = 60.0
# How many times a private server starts, on fresh ports each time, while another process takes its port first.
_START_ATTEMPTS = 3
_EXIT_WAIT_S = 5.0
# How often a command with wait_s is sent again.
_WAIT_INTERVAL_S = 0.1
# How long raw bytes wait for a reply before they are recorded as answered by none.
_NO_REPLY_S = 1.0
# How much of a reply the client could not parse is recorded as its bytes.
_UNPARSED_BYTES = 4096
# How long a restart waits for the server to stop (it waits for lagging replicas up to shutdown-timeout, 10s).
_STOP_TIMEOUT_S = 30.0
# The files of a private server's directory the harness writes itself.
_HARNESS_FILES = ("valkey.conf", "server.log", "output.log", "s.sock", OWNER_FILE)
# A node of the side a command runs on, in an argument: <node.1.id>, <node.1.host>, <node.1.port>,
# <node.1.cluster-port>, <node.1.shard-id> (its shard's ID as the cluster formed). Angle brackets, not braces: YAML reads {node.1.id} as a mapping, and Valkey braces as a
# hash tag.
_PLACEHOLDER = re.compile(r"<node\.(\d+)\.([a-z-]+)>")
_NODE_FIELDS = ("id", "host", "port", "cluster-port", "shard-id")
_CLUSTER_FIELDS = ("id", "cluster-port", "shard-id")


def is_binary(item: Any) -> bool:
    """Whether ``item`` is a binary argument, ``{"$base64": ...}``, as YAML ``!!binary`` loads."""

    return isinstance(item, Mapping) and set(item) == {BASE64_KEY} and isinstance(item[BASE64_KEY], str)


def validate_sequence_command(command: Any, name: str) -> list[str]:
    """``command`` is a non-empty list of strings, numbers, and binary arguments; its words, upper-cased (a
    binary argument is an empty word)."""

    if not command or not all(
        is_binary(item) or (isinstance(item, (str, int, float)) and not isinstance(item, bool)) for item in command
    ):
        raise ConfigError(f"{name} must be a non-empty list of strings, numbers, or !!binary values")
    return ["" if is_binary(item) else str(item).upper() for item in command]


def _refusal(words: list[str], timeout_s: float, on_instances: bool) -> Optional[str]:
    """Why a command with these ``words`` is refused, or None."""

    name, rest = words[0], words[1:]
    if name in _REFUSED:
        return _REFUSED[name]
    if name == "DEBUG" and rest:
        if rest[0] in _REFUSED_DEBUG:
            return "it ends or restarts the server"
        if rest[0] == "SLEEP" and len(rest) > 1:
            try:
                if float(rest[1]) > timeout_s:
                    return f"it sleeps longer than timeout_s ({timeout_s:g}s)"
            except ValueError:
                pass
    if name == "MODULE" and rest and rest[0] in _REFUSED_MODULE:
        return "it loads or unloads a module"
    if name == "CONFIG" and rest and rest[0] == "SET":
        managed = [option for option in rest[1::2] if option.lower() in MANAGED_SERVER_OPTIONS]
        if managed:
            return f"{managed[0].lower()} is an option the harness manages"
    if on_instances and name in _REFUSED_ON_INSTANCES:
        return "it changes the deployment's topology; a sequence with compare_with may send it"
    return None


def _raw_requests(raw: Any) -> list[list[str]]:
    """The whole requests in raw bytes, inline or RESP, each as its upper-cased words; what follows the last
    whole one (a request cut short on purpose, bytes that are no request) is left out."""

    text = (_argument(raw) if is_binary(raw) else raw.encode("utf-8", "surrogateescape")).decode("latin-1")
    lines, requests = text.split("\r\n")[:-1], []
    while lines:
        line = lines.pop(0)
        if not line.startswith("*"):
            words = line.split()
        else:
            try:
                count = int(line[1:])
            except ValueError:
                break
            if len(lines) < 2 * count or not all(lines[2 * index].startswith("$") for index in range(count)):
                break
            words, lines = lines[1 : 2 * count : 2], lines[2 * count :]
        if words:
            requests.append([word.upper() for word in words])
    return requests


def _validate_placeholders(arguments: Sequence[Any], where: str, config: Mapping[str, Any], compared: bool) -> None:
    for argument in arguments:
        for match in _PLACEHOLDER.finditer(argument if isinstance(argument, str) else ""):
            text, node, field = match.group(0), int(match.group(1)), match.group(2)
            if not compared:
                raise ConfigError(f"{where}: {text} needs compare_with or modules_parent: it names a private server")
            if field not in _NODE_FIELDS:
                raise ConfigError(f"{where}: {text} names no field of a node; the fields are {', '.join(_NODE_FIELDS)}")
            if node >= config["nodes"]:
                raise ConfigError(f"{where}: {text} names node {node}, but each side runs on {config['nodes']} node(s)")
            if field in _CLUSTER_FIELDS and not config["cluster"]:
                raise ConfigError(f"{where}: {text} needs cluster")


def _two_servers(config: Mapping[str, Any]) -> bool:
    """Whether the sequence runs on two private servers: with a parent build, or a parent build of the modules."""

    return config["compare_with"] is not None or config["modules_parent"] is not None


def _validate_commands(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    compared = _two_servers(config)
    if config["cluster"] and not compared:
        raise ConfigError(
            "cluster needs compare_with or modules_parent: on the deployment's instances the deployment decides the mode"
        )
    if config["nodes"] > 1 and not compared:
        raise ConfigError("nodes needs compare_with or modules_parent: the deployment decides its instances")
    if config["files"] and not compared:
        raise ConfigError("files needs compare_with or modules_parent: they are written for the private servers")
    for name, content in config["files"].items():
        path = posixpath.normpath(name)
        if path != name or path.startswith("/") or path == ".." or path.startswith("../") or path in _HARNESS_FILES:
            raise ConfigError(
                f"files: {name!r} must be a plain relative path inside the server's directory, "
                f"none of {', '.join(_HARNESS_FILES)}"
            )
        if not isinstance(content, str) and not is_binary(content):
            raise ConfigError(f"files: {name!r} must be a string or a !!binary value")
    if config["modules"] and not compared:
        raise ConfigError(
            "modules needs compare_with or modules_parent: the deployment's instances load modules through "
            "deployment.server_config.loadmodule"
        )
    if config["modules_parent"] is not None and not config["modules"]:
        raise ConfigError("modules_parent needs modules: it replaces the candidate's modules on the parent")
    if config["checks"] is not None:
        if not compared:
            raise ConfigError("checks needs compare_with or modules_parent: they run on private servers")
        if config["checks"]["compat"] and config["compare_with"] is None:
            raise ConfigError("checks.compat needs compare_with: it loads the parent build's data into the candidate")
    try:
        server_config = canonicalize_server_config(config["server_config"])
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"server_config: {exc}") from exc
    if "loadmodule" in server_config:
        raise ConfigError("server_config.loadmodule: list the modules in modules (and modules_parent)")
    if not config["commands"]:
        raise ConfigError("commands must list at least one command")
    for index, item in enumerate(config["commands"]):
        _validate_sequence_item(item, f"commands[{index}]", config, compared)


def _validate_sequence_item(item: Mapping[str, Any], where: str, config: Mapping[str, Any], compared: bool) -> None:
    """One command of a sequence (``where`` names it in the sequence): what it is (one of command, raw, read, and
    restart), its connection and node, what it may send, and its expectations; ``compared`` says the sequence runs
    on private servers, where a restart, a placeholder, and expect_difference are allowed."""

    if [item["command"] is not None, item["raw"] is not None, item["read"], item["restart"]].count(True) != 1:
        raise ConfigError(f"{where} needs one of command, raw, read, and restart")
    if item["restart"]:
        if not compared:
            raise ConfigError(f"{where}: restart needs compare_with or modules_parent")
        if item["connection"] != "main":
            raise ConfigError(f"{where}: restart restarts a node, every connection to it; it names none")
    if not item["connection"]:
        raise ConfigError(f"{where}.connection must name a connection")
    if item["wait_s"] is not None and item["command"] is None:
        raise ConfigError(f"{where}: wait_s sends a command again; raw bytes, a read, and a restart happen once")
    if item["raw"] is not None:
        if not item["raw"] or not (isinstance(item["raw"], str) or is_binary(item["raw"])):
            raise ConfigError(f"{where}.raw must be a non-empty string or a !!binary value")
        # Raw bytes are refused what a command is: the walk's test of valkey#1948 sent SHUTDOWN raw, and the
        # server's clean exit was filed as a crash (fuzzer#122).
        for words in _raw_requests(item["raw"]):
            refused = _refusal(words, config["timeout_s"], not compared)
            if refused is not None:
                named = " ".join(words[:2]) if words[0] in _CONTAINER_COMMANDS and len(words) > 1 else words[0]
                raise ConfigError(f"{where}: raw {named} is refused: {refused}")
    elif item["command"] is not None:
        words = validate_sequence_command(item["command"], f"{where}.command")
        refused = _refusal(words, config["timeout_s"], not compared)
        if refused is not None:
            named = " ".join(words[:2]) if words[0] in _CONTAINER_COMMANDS and len(words) > 1 else words[0]
            raise ConfigError(f"{where}: {named} is refused: {refused}")
        if words[0] in SUBSCRIBE and item["connection"] == "main":
            raise ConfigError(
                f"{where}: {words[0]} is refused on the main connection, which it would hold; "
                "send it on a connection of its own (connection: <name>)"
            )
        if words[0] in (*SUBSCRIBE, *UNSUBSCRIBE) and len(words) != 2:
            raise ConfigError(f"{where}: {words[0]} takes one channel: each is confirmed by a reply of its own")
    if item["node"] >= config["nodes"]:
        raise ConfigError(f"{where}.node is {item['node']}, but each side runs on {config['nodes']} node(s)")
    _validate_placeholders(item["command"] or [item["raw"]], where, config, compared)
    if item["expect"] is not None:
        if "error" in item["expect"]:
            if set(item["expect"]) != {"error"}:
                raise ConfigError(f"{where}.expect.error cannot be combined with a reply predicate")
        else:
            validate_predicate(item["expect"], f"{where}.expect")
    if item["wait_s"] is not None and item["expect"] is None:
        raise ConfigError(f"{where}: wait_s needs an expect to wait for")
    if item["expect_difference"]:
        if not compared:
            raise ConfigError(f"{where}: expect_difference needs compare_with or modules_parent")
        if item["expect"] is None:
            raise ConfigError(f"{where}: a command marked expect_difference needs an expect")


def _argument(item: Any) -> Any:
    return base64.b64decode(item[BASE64_KEY]) if is_binary(item) else item


def _packed(arguments: Sequence[Any]) -> bytes:
    """``arguments`` as one RESP request, each as valkey-py's encoder writes it (a float by repr)."""

    parts = [
        item if isinstance(item, bytes) else (repr(item) if isinstance(item, float) else str(item)).encode()
        for item in arguments
    ]
    return b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n%s\r\n" % (len(part), part) for part in parts)


def text(value: bytes) -> Any:
    """A bulk string as the evidence records it: the string when it is valid UTF-8, else ``{"$base64": ...}``."""

    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return {BASE64_KEY: base64.b64encode(value).decode("ascii")}


def raw_reply(value: Any) -> Any:
    """A reply as the connection parsed it, as JSON data: bytes through :func:`text`, a RESP3 set sorted, an
    error inside an array (a transaction's) as ``{"$error": ...}``, and nothing reshaped."""

    if isinstance(value, bytes):
        return text(value)
    if isinstance(value, (list, tuple)):
        return [raw_reply(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted((raw_reply(item) for item in value), key=canonical_json)
    if isinstance(value, Mapping):
        keys = ((text(key) if isinstance(key, bytes) else key) for key in value)
        return {
            (f"{BASE64_KEY}:{key[BASE64_KEY]}" if isinstance(key, dict) else str(key)): raw_reply(item)
            for key, item in zip(keys, value.values())
        }
    if isinstance(value, valkey.exceptions.ResponseError):
        return {"$error": str(value)}
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if value is None or isinstance(value, (bool, int, str)):
        return value
    return str(value)


class RawConnection:
    """One connection for a whole sequence: arguments go as bytes, replies come back as the server sent them
    (``decode_responses=False``, no response callbacks), and the protocol is the ``resp`` the sequence asks."""

    def __init__(self, connection: Any):
        self.connection = connection
        # The database a SELECT on it chose, for a reply read again unparsed (see _unparsed).
        self.db: Any = None
        # Once it subscribes, messages come between replies: those a command's reply came after wait for a read.
        self.subscriber = False
        self.delivered: list[Any] = []

    def send(self, command: Sequence[Any]) -> dict[str, Any]:
        """``{"reply": ...}``, the server's error as ``{"error": ...}``, a reply the client could not parse as
        ``{"protocol_error": ...}``, or a lost connection or a reply that timed out as ``{"error": ...,
        "unanswered": True}``."""

        if str(command[0]).upper() in SUBSCRIBE:
            self.subscriber = True
        record = self._exchange(lambda: self._write(command), past_deliveries=self.subscriber)
        if str(command[0]).upper() == "SELECT" and record.get("reply") == "OK":
            self.db = command[1]
        return record

    def _write(self, command: Sequence[Any]) -> None:
        """Send ``command`` with every argument as the sequence wrote it. valkey-py splits a first argument at
        whitespace (``"CONFIG GET"`` into two, ``""`` into none, which the server never answers), so one it would
        split goes packed here."""

        arguments = [_argument(item) for item in command]
        first = arguments[0] if arguments else b""
        if isinstance(first, (str, bytes)) and first.split() != [first]:
            self.connection.send_packed_command([_packed(arguments)])
        else:
            self.connection.send_command(*arguments)

    def send_raw(self, data: bytes) -> dict[str, Any]:
        """Send ``data`` as it is and read one reply, as :meth:`send`; ``{"no_reply": True}`` when none began
        within a second (the bytes are not a whole request, or the server holds them). A further reply stays on
        the connection, for the next command to read."""

        return self._exchange(lambda: self.connection.send_packed_command([data]), wait_s=_NO_REPLY_S)

    def read_waiting(self) -> dict[str, Any]:
        """The messages a command's reply came after, then every reply waiting on the connection, and each that
        begins within a second of the last (Pub/Sub messages, RESP3 pushes): ``{"reply": [...]}``, an error among
        them as ``{"$error": ...}``; or a lost connection as :meth:`send` records it."""

        replies, self.delivered = self.delivered, []
        try:
            while self.connection.can_read(timeout=_NO_REPLY_S):
                try:
                    replies.append(raw_reply(self.connection.read_response(push_request=True)))
                except valkey.exceptions.ResponseError as exc:
                    replies.append(raw_reply(exc))
        except (valkey.exceptions.ConnectionError, valkey.exceptions.TimeoutError, OSError) as exc:
            return _lost(exc)
        return {"reply": replies}

    def _exchange(self, write: Any, wait_s: Optional[float] = None, past_deliveries: bool = False) -> dict[str, Any]:
        try:
            write()
            if wait_s is not None and not self.connection.can_read(timeout=wait_s):
                return {"no_reply": True}
            while True:
                # A RESP3 push (a SUBSCRIBE's confirmation) is a reply too, not one for valkey-py to handle.
                reply = self.connection.read_response(push_request=True)
                if not (past_deliveries and _delivery(reply)):
                    return {"reply": raw_reply(reply)}
                self.delivered.append(raw_reply(reply))
        except valkey.exceptions.ResponseError as exc:
            return {"error": str(exc)}
        except valkey.exceptions.InvalidResponse as exc:
            return {"protocol_error": str(exc)}
        except (valkey.exceptions.ConnectionError, valkey.exceptions.TimeoutError, OSError) as exc:
            return _lost(exc)

    def unparsed(self, request: Any) -> bytes:
        """Send ``request`` (a command, or bytes as they are) and return the bytes that come back as they are: up
        to 4 KB, until none come for a second or the server closes the connection."""

        if isinstance(request, bytes):
            self.connection.send_packed_command([request])
        else:
            self._write(request)
        sock = self.connection._sock
        sock.settimeout(_NO_REPLY_S)
        received = b""
        try:
            while len(received) < _UNPARSED_BYTES:
                chunk = sock.recv(_UNPARSED_BYTES - len(received))
                if not chunk:
                    break
                received += chunk
        except OSError:
            pass
        return received

    def close(self) -> None:
        try:
            self.connection.disconnect()
        except Exception:
            pass


def _delivery(reply: Any) -> bool:
    """Whether ``reply`` is a message delivered to a subscriber (see ``_DELIVERIES``)."""

    return (
        isinstance(reply, list)
        and bool(reply)
        and isinstance(reply[0], bytes)
        and len(reply) == _DELIVERIES.get(reply[0], 0)
    )


def _lost(exc: BaseException) -> dict[str, Any]:
    """A command that got no reply: the reply timed out, or the connection was lost, ``closed`` when the server
    closed it (see :func:`run_sequence`)."""

    record: dict[str, Any] = {"error": f"{type(exc).__name__}: {exc}", "unanswered": True}
    if isinstance(exc, valkey.exceptions.ConnectionError):
        record["closed"] = True
    return record


class _ErrorsAsSent(valkey.connection.DefaultParser):
    """valkey-py's parser, with every error reply a ResponseError of the server's own text: valkey-py strips the
    code of the errors it maps to exception classes (NOPERM, READONLY, ...), and raises WRONGPASS, NOAUTH,
    LOADING and "ERR max number of clients reached" as connection errors, which a sequence takes for a lost
    connection."""

    EXCEPTION_CLASSES: ClassVar[dict[str, Any]] = {}


def open_raw_connection(
    resp: int, timeout_s: float, *, path: Optional[str] = None, host: Any = None, port: Any = None
) -> RawConnection:
    """A :class:`RawConnection` to the unix socket ``path``, or to ``host:port``, connected."""

    options = {
        "socket_timeout": timeout_s,
        "socket_connect_timeout": timeout_s,
        "protocol": resp,
        "parser_class": _ErrorsAsSent,
    }
    if path is not None:
        connection = valkey.connection.UnixDomainSocketConnection(path=path, **options)
    else:
        connection = valkey.connection.Connection(host=str(host), port=int(port), **options)
    connection.connect()
    return RawConnection(connection)


def run_sequence(target: Any, commands: Sequence[Mapping[str, Any]], context: Any) -> list[dict]:
    """Every command in order on its node's connection of ``target`` (a :class:`Side` or an
    :class:`_Instance`), each with its index; it stops at the first that got no reply, since the connection's
    state is unknown after it, and when the call is cancelled. A connection the server closed is its answer,
    ``{"closed": True}``, while its node lives on (the connection opens anew on its next use), and a connection
    lost to a crash when not."""

    replies: list[dict[str, Any]] = []
    for index, item in enumerate(commands):
        if context.cancelled:
            break
        record = {"index": index, **_run_step(target, item, context)}
        if record.pop("closed", False) and target.survives(item["node"]):
            target.forget(item["node"], item["connection"])
            record = {"index": index, "closed": True}
        replies.append(record)
        if record.get("unanswered"):
            break
    return replies


def _run_step(target: Any, item: Mapping[str, Any], context: Any) -> dict[str, Any]:
    """One command of a sequence on ``target``: its record, with ``wait_s`` the last of its attempts."""

    if item["restart"]:
        return target.restart(item["node"], context)
    try:
        connection = target.connection(item["node"], item["connection"])
    except (valkey.exceptions.ValkeyError, OSError) as exc:
        record = {"error": f"cannot connect: {type(exc).__name__}: {exc}", "unanswered": True}
        # A server that accepts a connection and closes it in the handshake (an unauthenticated client over its
        # output buffer limit) answers as one that closes it after a command.
        if isinstance(exc, valkey.exceptions.ConnectionError) and str(exc) == SERVER_CLOSED_CONNECTION_ERROR:
            record["closed"] = True
        return record
    if item["read"]:
        return target.mask(connection.read_waiting())
    if item["raw"] is not None:
        raw = item["raw"]
        request = _argument(raw) if is_binary(raw) else target.substitute(raw).encode()
        record = target.mask(connection.send_raw(request))
    elif item["wait_s"] is None:
        request = [target.substitute(argument) for argument in item["command"]]
        record = target.mask(connection.send(request))
    else:
        request = [target.substitute(argument) for argument in item["command"]]

        def attempt() -> tuple[bool, dict[str, Any]]:
            record = target.mask(connection.send(request))
            done = bool(record.get("unanswered")) or "protocol_error" in record
            return done or _sequence_matches(item["expect"], record, item["compare"]), record

        record = poll_until(attempt, timeout_s=item["wait_s"], interval_s=_WAIT_INTERVAL_S, context=context)[1]
    if "protocol_error" in record:
        received = target.mask(_unparsed(target, item, connection.db, request).decode("latin-1"))
        record["raw"] = received.encode("unicode_escape").decode("ascii")
    return record


def _unparsed(target: Any, item: Mapping[str, Any], db: Any, request: Any) -> bytes:
    """The bytes the server replies to ``request`` on a fresh connection to the node of ``item`` (the same RESP,
    and ``db`` SELECTed), for a reply the client could not parse. The connection the reply broke is closed
    first, and the fresh one after, so the next command opens one anew."""

    node, name = item["node"], item["connection"]
    target.forget(node, name)
    try:
        connection = target.connection(node, name)
    except (valkey.exceptions.ValkeyError, OSError):
        return b""
    try:
        if db is not None:
            connection.send(["SELECT", db])
        return connection.unparsed(request)
    except (valkey.exceptions.ValkeyError, OSError):
        return b""
    finally:
        target.forget(node, name)


def _json_value(item: Any) -> Any:
    """A bulk string of JSON text as the canonical JSON of the value it holds (``{"$json": ...}``), else ``item``.

    Canonical text, not the parsed data, so ``true`` and ``1`` stay different (Python's ``True == 1``); a number
    with a fraction or an exponent compares by its value (``1.0`` as ``1``), and one too large for a float, or
    NaN, stays its text.
    """

    if not isinstance(item, str):
        return item

    def number(text: str) -> Any:
        value = float(text)
        if not math.isfinite(value):
            return text
        return int(value) if value.is_integer() and abs(value) < 2**53 else value

    try:
        return {"$json": canonical_json(json.loads(item, parse_float=number, parse_constant=str))}
    except ValueError:
        return item


def _compared(record: Mapping[str, Any], mode: str) -> Any:
    """What a reply record compares as under ``mode``: the reply, its items sorted (``unordered``), its
    field-value pairs sorted (``unordered-pairs``; a RESP3 map compares as a map), or the JSON a bulk string (or
    each item of an array) holds (``json``, so two JSON texts of the same value compare equal); an error as
    itself."""

    if "reply" not in record:
        return {key: value for key, value in record.items() if key != "index"}
    reply = record["reply"]
    if mode == "json":
        reply = [_json_value(item) for item in reply] if isinstance(reply, list) else _json_value(reply)
    elif isinstance(reply, list):
        if mode == "unordered":
            reply = sorted(reply, key=canonical_json)
        elif mode == "unordered-pairs" and len(reply) % 2 == 0:
            reply = sorted((reply[index : index + 2] for index in range(0, len(reply), 2)), key=canonical_json)
    return {"reply": reply}


def _json_expected(expected: Any, reply: Any) -> Any:
    """``expected`` as :func:`_compared` makes ``reply`` under ``json``: where the reply holds JSON text, the
    expectation may be that text or the value it holds."""

    def one(value: Any, item: Any) -> Any:
        if isinstance(item, str) and not isinstance(value, str):
            return _json_value(json.dumps(value))
        return _json_value(value)

    if isinstance(reply, list) and isinstance(expected, list) and len(reply) == len(expected):
        return [one(value, item) for value, item in zip(expected, reply)]
    return one(expected, reply)


def _sequence_matches(expect: Optional[Mapping[str, Any]], record: Mapping[str, Any], compare: str) -> bool:
    expect = None if expect is None else plain_config(expect)
    if expect is not None and "equals" in expect and expect["equals"] is None:
        return record.get("error") is None and record.get("reply") is None
    if compare == "json" and expect is not None and "equals" in expect:
        return "reply" in record and canonical_json(_compared(record, "json")["reply"]) == canonical_json(
            _json_expected(expect["equals"], record["reply"])
        )
    if expect is not None and isinstance(expect.get("equals"), (list, dict)):
        return "reply" in record and canonical_json(record["reply"]) == canonical_json(expect["equals"])
    return _matches(expect, record.get("reply"), record.get("error"))


def _side(replies: Sequence[Mapping[str, Any]], commands: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """One server's evidence: its replies and the SHA-256 of the canonical JSON of the compared ones."""

    compared = [
        _compared(record, commands[record["index"]]["compare"])
        for record in replies
        if commands[record["index"]]["compare"] != "skip"
    ]
    return {"replies": list(replies), "digest": hashlib.sha256(canonical_json(compared).encode()).hexdigest()}


def _command_text(command: Sequence[Any]) -> str:
    return " ".join("<binary>" if is_binary(item) else str(item) for item in command)


def step_text(item: Mapping[str, Any]) -> str:
    """A command of a sequence as a summary names it."""

    if item["read"] or item["restart"]:
        text = "read" if item["read"] else "restart"
    elif item["raw"] is not None:
        text = "raw <binary>" if is_binary(item["raw"]) else f"raw {json.dumps(item['raw'])}"
    else:
        text = _command_text(item["command"])
    if item["connection"] != "main":
        text += f" on connection {item['connection']}"
    return text + (f" on node {item['node']}" if item["node"] else "")


class _Instance:
    """A deployment instance a sequence runs on: its connections by name, opened on first use, and no
    placeholders."""

    def __init__(self, connect: Any):
        self.connect = connect
        self.connections = {"main": connect()}

    def connection(self, node: int, name: str) -> RawConnection:
        if name not in self.connections:
            self.connections[name] = self.connect()
        return self.connections[name]

    def survives(self, node: int) -> bool:
        """Never known here: an instance's liveness is the liveness oracle's to judge, so a connection it closed
        stays a command without a reply."""

        return False

    def forget(self, node: int, name: str) -> None:
        # A connection closed as it opened was never kept.
        connection = self.connections.pop(name, None)
        if connection is not None:
            connection.close()

    def close(self) -> None:
        for connection in self.connections.values():
            connection.close()

    def substitute(self, argument: Any) -> Any:
        return argument

    def mask(self, value: Any) -> Any:
        return value


class _Node:
    """One private server of a side: its address options, the server, its connections by name, its kept logs."""

    def __init__(self, name: str):
        # The prefix of its kept logs: the side's label, with the node's index when the side has several.
        self.name = name
        self.options: dict[str, Any] = {}
        self.server: Any = None
        self.owned: Any = None
        self.connections: dict[str, RawConnection] = {}
        # Its cluster node ID and shard ID (as the cluster formed), with cluster.
        self.id: Optional[str] = None
        self.shard_id: Optional[str] = None
        self.logs: list[str] = []


class Side:
    """One build's side of a compared sequence: its private servers (nodes), started, owned until stopped, their
    logs kept, and the placeholders that name them."""

    def __init__(self, label: str, binary: str, nodes: int, resp: int, timeout_s: float):
        self.label, self.binary = label, binary
        # The protocol and client timeout of every connection.
        self.resp, self.timeout_s = resp, timeout_s
        # The server options of this side's nodes (cluster mode, modules), beside the shared server_config.
        self.options: dict[str, Any] = {}
        self.nodes = [_Node(label if nodes == 1 else f"{label}.node{index}") for index in range(nodes)]
        self.replies: list[dict[str, Any]] = []
        # The exit status of the first node found exited, and that node.
        self.exited: Optional[int] = None
        self.exited_node: Optional[int] = None
        self.logs: list[str] = []
        # {placeholder: value}, and {node ID or port: placeholder} for the replies.
        self.values: dict[str, str] = {}
        self.names: dict[str, str] = {}
        self.pattern: Optional[re.Pattern] = None

    def start(
        self, context: Any, spec: ComponentSpec, server_config: Mapping[str, Any], files: Mapping[str, bytes]
    ) -> str:
        """Start every node from a config file of ``server_config`` and its options, ``files`` in its directory, and
        open its main connection; what went wrong, or an empty string."""

        for index, node in enumerate(self.nodes):
            for attempt in range(_START_ATTEMPTS):
                arguments = server_arguments({**server_config, **self.options, **node.options})
                node.server = PrivateServer(
                    self.binary, arguments, timeout_s=_START_TIMEOUT_S, prefix="vfc-", files=files, config_file=True
                )
                node.owned = context.obligations.open(
                    spec, "process.private-server", node.server.workdir, node.server.stop
                )
                try:
                    exit_output = node.server.start(context)
                except (OSError, TimeoutError) as exc:
                    return f"the {node.name} server did not start: {exc}"
                if (
                    exit_output is None
                    or attempt + 1 == _START_ATTEMPTS
                    or "Failed listening on port" not in exit_output
                ):
                    break
                # Another process took a port between its free check and Valkey's bind (a batch runs dozens of
                # workers at once): the node starts again on fresh ports.
                node.owned.discharge()
                fresh = iter(
                    free_ports(1 + ("cluster-port" in node.options), port_block(context), context.rng(spec.id, "ports"))
                )
                node.options["port"] = next(fresh)
                if "cluster-port" in node.options:
                    node.options["cluster-port"] = next(fresh)
            if exit_output is not None:
                return f"the {node.name} server exited at startup: {exit_output[-500:]}"
            try:
                self.connection(index, "main")
            except (valkey.exceptions.ValkeyError, OSError) as exc:
                return f"cannot connect to the {node.name} server with RESP{self.resp}: {type(exc).__name__}: {exc}"
        return ""

    def form_cluster(self, context: Any, timeout_s: float) -> Optional[str]:
        """Give node 0 every slot, have every other node meet it, and wait until every node knows them all with
        its state ok; why that did not happen, or None. Each node's ID is recorded."""

        first = self.nodes[0]
        added = first.connections["main"].send(["CLUSTER", "ADDSLOTSRANGE", "0", "16383"])
        if "reply" not in added:
            return f"the {first.name} server: CLUSTER ADDSLOTSRANGE failed: {added.get('error')}"
        for node in self.nodes:
            myid = node.connections["main"].send(["CLUSTER", "MYID"])
            if "reply" not in myid:
                return f"the {node.name} server: CLUSTER MYID failed: {myid.get('error')}"
            node.id = str(myid["reply"])
            # Random like the node ID, so two clusters' replies differ by it alone (fuzzer#89: CLUSTER SHARDS).
            # A build without CLUSTER MYSHARDID has none to name.
            shard = node.connections["main"].send(["CLUSTER", "MYSHARDID"])
            node.shard_id = str(shard["reply"]) if "reply" in shard else None
        for node in self.nodes[1:]:
            met = node.connections["main"].send(
                ["CLUSTER", "MEET", "127.0.0.1", first.options["port"], first.options["cluster-port"]]
            )
            if "reply" not in met:
                return f"the {node.name} server: CLUSTER MEET failed: {met.get('error')}"
        for node in self.nodes:

            def formed(node: _Node = node) -> tuple[bool, Any]:
                info = node.connections["main"].send(["CLUSTER", "INFO"])
                fields = info_fields(info.get("reply", ""))
                ready = fields.get("cluster_state") == "ok" and fields.get("cluster_known_nodes") == str(
                    len(self.nodes)
                )
                return ready, info

            done, last = poll_until(formed, timeout_s=timeout_s, interval_s=0.1, context=context)
            if not done:
                return (
                    f"the {node.name} server: cluster_state is not ok with {len(self.nodes)} known node(s) "
                    f"within {timeout_s:g}s: {last}"
                )
        return None

    def restart(self, index: int, context: Any) -> dict[str, Any]:
        """Stop node ``index`` gracefully and start it again from its directory and config file: ``{"reply": "OK"}``
        once it answers again, or why not as a command that got no reply. A cluster node answers once its
        cluster_state is ok again, as it was when the cluster formed: until then it refuses keys with CLUSTERDOWN.
        One whose state stays otherwise answers with that state, an error both builds compare on."""

        node = self.nodes[index]
        for connection in node.connections.values():
            connection.close()
        node.connections.clear()
        if not node.server.shut_down(_STOP_TIMEOUT_S):
            return {"error": f"the server did not stop within {_STOP_TIMEOUT_S:g}s", "unanswered": True}
        try:
            exit_output = node.server.start(context)
        except (OSError, TimeoutError) as exc:
            return {"error": f"the server did not start again: {exc}", "unanswered": True}
        if exit_output is not None:
            return {"error": f"the server exited at startup: {exit_output[-500:]}", "unanswered": True}
        if "cluster-port" in node.options:

            def settled() -> tuple[bool, str]:
                info = self.connection(index, "main").send(["CLUSTER", "INFO"])
                fields = info_fields(info.get("reply", ""))
                return fields.get("cluster_state") == "ok", fields.get("cluster_state") or str(info)

            done, state = poll_until(settled, timeout_s=self.timeout_s, interval_s=0.1, context=context)
            if not done:
                return {"error": f"cluster_state is not ok within {self.timeout_s:g}s of the restart: {state}"}
        return {"reply": "OK"}

    def name_nodes(self) -> None:
        """The placeholders of its nodes, both ways: each to its value, and each node ID and port in a reply back to
        its placeholder (the host is the same on every side)."""

        for index, node in enumerate(self.nodes):
            fields = {
                "id": node.id,
                "shard-id": node.shard_id,
                "host": "127.0.0.1",
                **{key: node.options.get(key) for key in ("port", "cluster-port")},
            }
            for field, value in fields.items():
                if value is None:
                    continue
                self.values[f"<node.{index}.{field}>"] = str(value)
                if field != "host":
                    self.names[str(value)] = f"<node.{index}.{field}>"
        if self.names:
            alternatives = "|".join(re.escape(value) for value in sorted(self.names, key=len, reverse=True))
            self.pattern = re.compile(f"(?<![0-9A-Za-z])(?:{alternatives})(?![0-9A-Za-z])")

    def connection(self, node: int, name: str) -> RawConnection:
        connections = self.nodes[node].connections
        if name not in connections:
            connections[name] = open_raw_connection(self.resp, self.timeout_s, path=self.nodes[node].server.socket_path)
        return connections[name]

    def survives(self, index: int) -> bool:
        """Whether node ``index`` still runs ``_EXIT_WAIT_S`` after a connection to it closed."""

        try:
            self.nodes[index].server.process.wait(timeout=_EXIT_WAIT_S)
        except subprocess.TimeoutExpired:
            return True
        return False

    def forget(self, index: int, name: str) -> None:
        # A connection closed as it opened was never kept.
        connection = self.nodes[index].connections.pop(name, None)
        if connection is not None:
            connection.close()

    def substitute(self, argument: Any) -> Any:
        """``argument`` with every placeholder in it replaced by this side's value."""

        if not isinstance(argument, str):
            return argument
        return _PLACEHOLDER.sub(lambda match: self.values[match.group(0)], argument)

    def mask(self, value: Any) -> Any:
        """``value`` (a record, or a part of one) with this side's node IDs and ports as their placeholders, so two
        sides that behave the same compare equal; a port is masked as an integer too, as CLUSTER SLOTS replies it."""

        if self.pattern is None or is_binary(value):
            return value
        if isinstance(value, str):
            return self.pattern.sub(lambda match: self.names[match.group(0)], value)
        if isinstance(value, int) and not isinstance(value, bool):
            return self.names.get(str(value), value)
        if isinstance(value, list):
            return [self.mask(item) for item in value]
        if isinstance(value, dict):
            return {self.mask(key): self.mask(item) for key, item in value.items()}
        return value

    def finish(self, context: Any, spec: ComponentSpec, commands: Sequence[Mapping[str, Any]]) -> None:
        """Note whether a node exited (a lost connection waits for its node), keep every node's logs, stop them."""

        for node in self.nodes:
            for connection in node.connections.values():
                connection.close()
        if self.replies and self.replies[-1].get("unanswered"):
            lost = self.nodes[commands[self.replies[-1]["index"]]["node"]].server
            if lost is not None and lost.process is not None:
                try:
                    lost.process.wait(timeout=_EXIT_WAIT_S)
                except subprocess.TimeoutExpired:
                    pass
        directory = component_artifact_dir(context, spec)
        for index, node in enumerate(self.nodes):
            if node.server is not None and node.server.process is not None:
                status = node.server.process.poll()
                if status is not None and self.exited is None:
                    self.exited, self.exited_node = status, index
                for name, path in (("server.log", node.server.log_path), ("output.log", node.server.output_path)):
                    destination = os.path.join(directory, f"{node.name}.{name}")
                    try:
                        shutil.copyfile(path, destination)
                        node.logs.append(destination)
                    except OSError:
                        pass
            self.logs.extend(node.logs)
            if node.owned is not None:
                node.owned.discharge()

    def crash_cause(self) -> str:
        """Why its node exited, from that node's kept logs: the crash report's cause, else its fatal line, else
        its status."""

        logs = self.nodes[self.exited_node or 0].logs
        reports = scan_for_crashes({path: path for path in logs}, CRASH_PATTERNS)
        if reports:
            return crash_cause(next(iter(reports.values())))
        fatal = next((line for line in map(fatal_exit, logs) if line), None)
        return fatal or f"exited with status {self.exited}"

    def crashed(self) -> bool:
        """Whether its node's exit is a crash (a sanitizer report, assertion or signal), not a controlled fatal
        exit it logged on purpose (as a build hardened to reject bad data does on a load it refuses)."""

        logs = self.nodes[self.exited_node or 0].logs
        return bool(scan_for_crashes({path: path for path in logs}, CRASH_PATTERNS))


# The fewest ports above the host's ephemeral range for private servers to take theirs there.
_PRIVATE_SPAN_MIN = 1000


@functools.lru_cache(maxsize=1)
def _private_port_span() -> Optional[tuple[int, int]]:
    """Ports above the host's ephemeral range, where no outgoing connection takes one between the free-port check
    and Valkey's bind (the server would abort, "Failed listening on port"); None where there are too few, or the
    range cannot be read. Below it, a batch's workers own the ports of their deployments and cluster buses."""

    span = ephemeral_port_range()
    if span is None or 65535 - span[1] < _PRIVATE_SPAN_MIN:
        return None
    return span[1] + 1, 65535


# Ports of the private span (see _private_port_span) each batch worker takes its private servers' ports from, by its
# deployment's loopback_block: no other worker's server takes one, even while its node restarts.
_PRIVATE_SLICE = 64
# Every port this process handed out (a run is one process): never handed out twice, so none goes to another server
# of the run while its own node restarts.
_GIVEN: set[int] = set()
_GIVEN_LOCK = threading.Lock()


def port_block(context: Any) -> Optional[int]:
    """The run's deployment loopback_block, which a batch gives each worker its own of; None outside a batch."""

    block = plain_config(context.plan.deployment).get("loopback_block") if context is not None else None
    return block if isinstance(block, int) and block > 0 else None


def free_ports(count: int, block: Optional[int] = None, rng: Optional[random.Random] = None) -> list[int]:
    """``count`` distinct TCP ports free on the loopback address right now, none handed out before by this process:
    above the ephemeral range where it leaves room (see :func:`_private_port_span`), in worker ``block``'s own slice
    of it while that has room, else any the kernel hands out. The draw outside the slice is ``rng``'s (the run's
    ``ports`` stream, so a replay draws the same ports) or, without one, the process-wide generator's."""

    span = _private_port_span()
    if span is not None and block is not None:
        low = span[0] + (block - 1) * _PRIVATE_SLICE
        own = range(low, min(low + _PRIVATE_SLICE, span[1] + 1))
    else:
        own = range(0)
    draw = random.randint if rng is None else rng.randint
    ports: list[int] = []
    with _GIVEN_LOCK:
        for port in own:
            if len(ports) == count:
                break
            if port not in _GIVEN and _bindable(port):
                ports.append(port)
        while len(ports) < count:
            port = _bindable(0 if span is None else draw(*span))
            if port and port not in _GIVEN and port not in ports:
                ports.append(port)
        _GIVEN.update(ports)
    return ports


def _bindable(port: int) -> int:
    """``port`` (or, for 0, the one the kernel picks) if it binds on the loopback address now, else 0."""

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            return 0
        return sock.getsockname()[1]


def new_side(label: str, binary: str, config: Mapping[str, Any], modules: Sequence[str], ports: Iterator[int]) -> Side:
    """A side of ``config``'s nodes, not started: every node listens on a loopback port of its own (and a cluster
    bus port) from ``ports``, so nodes can meet and replicate; the sequence talks to it over its unix socket."""

    side = Side(label, binary, config["nodes"], config["resp"], config["timeout_s"])
    for node in side.nodes:
        node.options = {"bind": "127.0.0.1", "port": next(ports)}
        if config["cluster"]:
            node.options["cluster-port"] = next(ports)
    if config["cluster"]:
        side.options["cluster-enabled"] = "yes"
    if modules:
        side.options["loadmodule"] = list(modules)
    return side


class CommandsAction:
    """Send a command sequence, and with ``compare_with`` compare the replies of two builds.

    With ``compare_with`` the sequence runs on two sides of ``nodes`` private servers each, which the action
    starts from an empty dataset with the same ``server_config`` (``sanitize-dump-payload yes`` unless set, and
    ``cluster-enabled yes`` with ``cluster``), one side of the deployment's binary (the candidate) and one of
    ``compare_with`` (the parent); the deployment's instances are never touched. An option the parent rejects
    is dropped on both sides and recorded. Only the candidate is judged: a reply that violates its ``expect``,
    a reply the client cannot parse, or a candidate node that exits, is FAIL. A difference between the builds is
    never a FAIL by itself: a marked command whose replies are equal leaves the run ERROR ``unreached-change``, an
    unmarked command whose replies differ ERROR ``unexplained-difference``; a parent that dies on a marked command
    reached the change. Without ``compare_with`` the sequence runs on each selected instance, and ``expect`` alone
    judges it.
    """

    def run(self, context: Any, spec: ComponentSpec):
        config = spec.config
        if not _two_servers(config):
            return self._run_on_instances(context, spec, config)
        return self._run_compared(context, spec, config)

    def _run_on_instances(self, context: Any, spec: ComponentSpec, config: Mapping[str, Any]):
        handle = deployment(context)
        evidence, problems, unanswered, broken = [], [], [], []
        for instance_id in select_instances(handle, config["instances"]):
            instance = handle.instance(instance_id)
            try:
                target = _Instance(
                    functools.partial(
                        open_raw_connection, config["resp"], config["timeout_s"], host=instance.host, port=instance.port
                    )
                )
            except (valkey.exceptions.ValkeyError, OSError) as exc:
                return make_result(spec, ComponentStatus.ERROR, f"cannot connect to {instance_id}: {exc}")
            try:
                replies = run_sequence(target, config["commands"], context)
            finally:
                target.close()
            if config["judge_replies"]:
                problems.extend(f"{instance_id}: {text}" for text in _violations(config["commands"], replies))
            unanswered.extend(f"{instance_id}: {text}" for text in _unanswered(config["commands"], replies))
            broken.extend(
                {"instance_id": instance_id, **item} for item in _protocol_errors(config["commands"], replies)
            )
            record = _record(config, _side(replies, config["commands"]), None, None, [], [], [])
            record["instance_id"] = instance_id
            evidence.append(make_evidence(spec, "valkey.command.sequence", "action.completed", record))
        if broken:
            return make_result(
                spec,
                ComponentStatus.FAIL,
                "; ".join(f"{item['instance_id']}: {_protocol_error_text(item)}" for item in broken),
                evidence=evidence,
                diagnostics={"reason": "protocol-error", "commands": broken},
            )
        status = ComponentStatus.FAIL if problems else ComponentStatus.ERROR if unanswered else ComponentStatus.PASS
        summary = (
            "; ".join([*problems, *unanswered]) or f"{len(config['commands'])} commands on {len(evidence)} instance(s)"
        )
        return make_result(spec, status, summary, evidence=evidence)

    def _run_compared(self, context: Any, spec: ComponentSpec, config: Mapping[str, Any]):
        commands = config["commands"]
        try:
            candidate_binary = resolve_binary(plain_config(context.plan.deployment).get("valkey_binary") or "")
            # Without compare_with, both servers run the deployment's binary and differ by their modules.
            parent_binary = (
                candidate_binary if config["compare_with"] is None else resolve_binary(config["compare_with"])
            )
        except OSError as exc:
            return make_result(spec, ComponentStatus.ERROR, str(exc))
        server_config = {**PRIVATE_SERVER_DEFAULTS, **canonicalize_server_config(config["server_config"])}
        modules = {
            "candidate": list(config["modules"]),
            "parent": list(config["modules"] if config["modules_parent"] is None else config["modules_parent"]),
        }
        ports = iter(
            free_ports(
                2 * config["nodes"] * (2 if config["cluster"] else 1),
                port_block(context),
                context.rng(spec.id, "ports"),
            )
        )
        candidate = new_side("candidate", candidate_binary, config, modules["candidate"], ports)
        parent = new_side("parent", parent_binary, config, modules["parent"], ports)

        files = {
            name: _argument(content) if is_binary(content) else content.encode()
            for name, content in config["files"].items()
        }

        def starts(server: Side, options: Mapping[str, Any]) -> Probed:
            probed = {**options, **server.options, **server.nodes[0].options}
            return starts_with(context, spec, server.binary, probed, _START_TIMEOUT_S, files)

        # Configuration both builds accept: an option the parent rejects is dropped on both sides.
        dropped: list[str] = []
        if starts(parent, server_config)[0] is ComponentStatus.SKIP:
            dropped = [
                name
                for name, value in server_config.items()
                if starts(parent, {name: value})[0] is ComponentStatus.SKIP
            ]
            server_config = {name: value for name, value in server_config.items() if name not in dropped}
        for server in (parent, candidate):
            status, summary, facts = starts(server, server_config)
            build = f"the {server.label} build ({server.binary})"
            loaded = f" and modules {modules[server.label]}" if modules[server.label] else ""
            if status is ComponentStatus.FAIL:
                # The probe's server crashed: a build that crashes with the configuration it is started with is a
                # finding, whichever side it is.
                return make_result(
                    spec,
                    ComponentStatus.FAIL,
                    f"{build} crashed when started with {server_config}{loaded}: {summary}",
                    diagnostics={"crashes": {server.label: facts["crash"]}},
                    crash=True,
                )
            if status is not ComponentStatus.PASS:
                return make_result(
                    spec,
                    ComponentStatus.ERROR,
                    f"{build} does not start with {server_config}{loaded}"
                    + ("" if status is ComponentStatus.SKIP else ": the probe could not run")
                    + (f": {summary}" if modules[server.label] else ""),
                )

        try:
            for server in (candidate, parent):
                failed = server.start(context, spec, server_config, files)
                if not failed and config["cluster"]:
                    failed = server.form_cluster(context, config["timeout_s"]) or ""
                if failed:
                    return make_result(spec, ComponentStatus.ERROR, failed)
                server.name_nodes()
            for server in (candidate, parent):
                server.replies = run_sequence(server, commands, context)
        finally:
            for server in (candidate, parent):
                server.finish(context, spec, commands)

        reached, unexplained, unreached, uncompared, after_death = compare(commands, candidate, parent)
        # Replies that differ between the two builds with no change to explain it vary between runs: the variants
        # check leaves them alone, whether or not the replies are judged.
        differing = list(unexplained)
        if not config["judge_replies"]:
            unexplained, unreached = [], []
        record = _record(
            config,
            _side(candidate.replies, commands),
            _side(parent.replies, commands),
            modules,
            dropped,
            reached,
            unexplained,
        )
        record["uncompared"] = len(after_death)
        for server in (candidate, parent):
            if server.exited is not None:
                record[server.label]["exited"] = server.exited
                record[server.label]["exited_node"] = server.exited_node
        record["violating"] = _violating(commands, candidate.replies)
        checks: dict[str, dict[str, Any]] = {}
        check_logs: list[str] = []
        if config["checks"] is not None:
            # checks.py builds on this module, so it is imported where it is used.
            from .checks import run_checks

            checks, check_logs = run_checks(
                context, spec, config, candidate, parent_binary, server_config, files, differing
            )
            record["checks"] = checks
        failed = next((outcome for outcome in checks.values() if outcome["outcome"] == "fail"), None)
        evidence = (
            make_evidence(
                spec,
                "valkey.command.sequence",
                "action.completed",
                record,
                artifacts=[*candidate.logs, *parent.logs, *check_logs],
            ),
        )
        violations = _violations(commands, candidate.replies) if config["judge_replies"] else []
        # A violated expect on which the parent replied the same is about behaviour the change did not touch:
        # nearly always the test's own expectation, not the change. Recorded, never a FAIL.
        disputed = [
            index
            for index in _violating(commands, candidate.replies)
            if config["judge_replies"] and _same_on_parent(commands, index, candidate, parent)
        ]
        violations = [
            text for index, text in zip(_violating(commands, candidate.replies), violations) if index not in disputed
        ]
        if candidate.exited is not None:
            cause = candidate.crash_cause()
            at = candidate.replies[-1]["index"] if candidate.replies else None
            where = "before the first command" if at is None else f"on command {at} ({step_text(commands[at])})"
            who = "the candidate" if config["nodes"] == 1 else f"the candidate's node {candidate.exited_node}"
            # A crash report or a fatal-exit line it logged makes this a crash; a clean exit with status 0 and
            # neither (a commanded SHUTDOWN) is still a FAIL but not a crash, so the walk does not file it as one
            # (fuzzer#127: a SHUTDOWN-then-SET experiment exited every build with status 0).
            clean_exit = candidate.exited == 0 and cause == f"exited with status {candidate.exited}"
            return make_result(
                spec,
                ComponentStatus.FAIL,
                f"{who} exited {where}: {cause}",
                evidence=evidence,
                diagnostics={"crashes": {"candidate": cause}},
                crash=not clean_exit,
            )
        # A reply the client cannot parse is a protocol bug, a FAIL even where the parent gives the same one.
        broken = _protocol_errors(commands, candidate.replies)
        if broken:
            return make_result(
                spec,
                ComponentStatus.FAIL,
                "; ".join(map(_protocol_error_text, broken)),
                evidence=evidence,
                diagnostics={"reason": "protocol-error", "commands": broken},
            )
        if violations:
            return make_result(spec, ComponentStatus.FAIL, "; ".join(violations), evidence=evidence)
        if failed is not None:
            return make_result(
                spec,
                ComponentStatus.FAIL,
                failed["summary"],
                evidence=evidence,
                diagnostics=failed["diagnostics"],
                crash=failed["crash"],
            )
        unanswered = _unanswered(commands, candidate.replies) + [
            f"the parent did not answer {text}" for text in uncompared
        ]
        if unanswered:
            return make_result(spec, ComponentStatus.ERROR, "; ".join(unanswered), evidence=evidence)
        # Commands after the one the parent died on were not compared. One marked expect_difference is a change
        # that was never examined: ERROR, not a PASS on half the sequence.
        unexamined = [index for index in after_death if commands[index]["expect_difference"]]
        if unexamined:
            return make_result(
                spec,
                ComponentStatus.ERROR,
                "the parent died before marked commands ran on it, so they were not compared: "
                + ", ".join(f"{index} ({step_text(commands[index])})" for index in unexamined),
                evidence=evidence,
                diagnostics={"reason": "uncompared-change", "commands": unexamined},
            )
        if disputed:
            commands_disputed = [
                {
                    "index": index,
                    "command": step_text(commands[index]),
                    "expect": plain_config(commands[index]["expect"]),
                    "candidate": reply_at(candidate.replies, index),
                    "parent": reply_at(parent.replies, index),
                }
                for index in disputed
            ]
            return make_result(
                spec,
                ComponentStatus.ERROR,
                "expectations both builds violate alike, so they are not about the change: "
                + ", ".join(f"{item['index']} ({item['command']})" for item in commands_disputed),
                evidence=evidence,
                # pre_existing: whatever is wrong here is in the parent too, so the batch can file it once.
                diagnostics={"reason": "expect-disputed", "pre_existing": True, "commands": commands_disputed},
            )
        if unexplained:
            differences = [
                {
                    "index": index,
                    "command": step_text(commands[index]),
                    "candidate": reply_at(candidate.replies, index),
                    "parent": reply_at(parent.replies, index),
                }
                for index in unexplained
            ]
            return make_result(
                spec,
                ComponentStatus.ERROR,
                "unmarked commands reply differently on the two builds: "
                + ", ".join(f"{item['index']} ({item['command']})" for item in differences),
                evidence=evidence,
                diagnostics={"reason": "unexplained-difference", "commands": differences},
            )
        if unreached:
            return make_result(
                spec,
                ComponentStatus.ERROR,
                "marked commands reply the same on both builds, so the sequence did not reach the change: "
                + ", ".join(f"{index} ({step_text(commands[index])})" for index in unreached),
                evidence=evidence,
                diagnostics={"reason": "unreached-change"},
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{len(commands)} commands on both builds, {len(reached)} marked command(s) reached the change"
            + (f", dropped {', '.join(dropped)}" if dropped else "")
            + (
                f", {len(after_death)} command(s) after the parent died not compared: "
                + ", ".join(f"{index} ({step_text(commands[index])})" for index in after_death)
                if after_death
                else ""
            )
            + "".join(f", {name} {outcome['outcome']}" for name, outcome in checks.items()),
            evidence=evidence,
        )


def _record(
    config: Mapping[str, Any],
    candidate: dict[str, Any],
    parent: Optional[dict[str, Any]],
    modules: Optional[Mapping[str, list[str]]],
    dropped: Sequence[str],
    reached: Sequence[int],
    unexplained: Sequence[int],
) -> dict[str, Any]:
    return {
        "candidate": candidate,
        "parent": parent,
        "modules": None if modules is None else dict(modules),
        "cluster": config["cluster"],
        "nodes": config["nodes"],
        "dropped_config": list(dropped),
        "reached": list(reached),
        "unexplained": list(unexplained),
    }


def without_index(record: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in record.items() if key != "index"}


def reply_at(replies: Sequence[Mapping[str, Any]], index: int) -> Optional[dict[str, Any]]:
    return next((without_index(record) for record in replies if record["index"] == index), None)


def _violations(commands: Sequence[Mapping[str, Any]], replies: Sequence[Mapping[str, Any]]) -> list[str]:
    """The answered replies that violate their command's ``expect``, described."""

    return [
        f"command {record['index']} ({step_text(commands[record['index']])}) replied "
        f"{canonical_json(without_index(record))}, not {canonical_json(plain_config(commands[record['index']]['expect']))}"
        for record in replies
        if not record.get("unanswered")
        and not _sequence_matches(commands[record["index"]]["expect"], record, commands[record["index"]]["compare"])
    ]


def _violating(commands: Sequence[Mapping[str, Any]], replies: Sequence[Mapping[str, Any]]) -> list[int]:
    """The indexes of the answered replies that violate their command's ``expect``, in the order of
    :func:`_violations`."""

    return [
        record["index"]
        for record in replies
        if not record.get("unanswered")
        and not _sequence_matches(commands[record["index"]]["expect"], record, commands[record["index"]]["compare"])
    ]


def _same_on_parent(commands: Sequence[Mapping[str, Any]], index: int, candidate: Any, parent: Any) -> bool:
    """Whether the parent answered command ``index`` and its reply compares equal to the candidate's."""

    mine, theirs = reply_at(candidate.replies, index), reply_at(parent.replies, index)
    if mine is None or theirs is None or theirs.get("unanswered"):
        return False
    mode = commands[index]["compare"]
    return canonical_json(_compared({**mine, "index": index}, mode)) == canonical_json(
        _compared({**theirs, "index": index}, mode)
    )


def _replication_links(commands: Sequence[Mapping[str, Any]]) -> set[int]:
    """The commands sent on a connection from its SYNC or PSYNC on: it is a replica's link since, and what it
    reads is the replication stream, not replies (the SYNC's own, the newlines that keep it alive while the RDB
    waits to start)."""

    links, found = set(), set()
    for index, item in enumerate(commands):
        if item["command"] is not None:
            words = [str(word) for word in item["command"][:1]]
        elif isinstance(item["raw"], str):
            words = item["raw"].split()[:1]
        else:
            continue
        key = (item["node"], item["connection"])
        if key in links:
            found.add(index)
        elif words and words[0].upper() in ("SYNC", "PSYNC"):
            links.add(key)
            found.add(index)
    return found


def _protocol_errors(commands: Sequence[Mapping[str, Any]], replies: Sequence[Mapping[str, Any]]) -> list[dict]:
    """The commands whose reply the client could not parse: each with its index, the client's message, and the
    reply's bytes. A replica's link reads the replication stream, which is no reply."""

    links = _replication_links(commands)
    return [
        {
            "index": record["index"],
            "command": step_text(commands[record["index"]]),
            "protocol_error": record["protocol_error"],
            "raw": record["raw"],
        }
        for record in replies
        if "protocol_error" in record and record["index"] not in links
    ]


def _protocol_error_text(item: Mapping[str, Any]) -> str:
    return f"command {item['index']} ({item['command']}) got a reply the client cannot parse: {item['protocol_error']}"


def _unanswered(commands: Sequence[Mapping[str, Any]], replies: Sequence[Mapping[str, Any]]) -> list[str]:
    return [
        f"command {record['index']} ({step_text(commands[record['index']])}) got no reply: {record['error']}"
        for record in replies
        if record.get("unanswered")
    ]


def compare(commands, candidate: Side, parent: Side) -> tuple[list[int], list[int], list[int], list[str], list[int]]:
    """Judge the parent's replies against the candidate's: the marked commands that reached the change, the
    unmarked ones that differ, the marked ones that do not, the commands the parent did not answer
    without dying (described), and the commands after the one it died on, which it never ran."""

    reached: list[int] = []
    unexplained: list[int] = []
    unreached: list[int] = []
    uncompared: list[str] = []
    after_death: list[int] = []
    # The command the parent died on: its last record (the one that got no reply, or the last it answered).
    died_at = parent.replies[-1]["index"] if parent.exited is not None and parent.replies else None
    for record in candidate.replies:
        index = record["index"]
        item = commands[index]
        if record.get("unanswered") or item["compare"] == "skip":
            continue
        other = reply_at(parent.replies, index)
        if other is None or other.get("unanswered"):
            if index == died_at:
                (reached if item["expect_difference"] else unexplained).append(index)
            elif parent.exited is None and other is not None:
                uncompared.append(f"command {index} ({step_text(item)}): {other['error']}")
            elif died_at is not None and index > died_at:
                after_death.append(index)
            continue
        # As canonical JSON, not with ==, under which a RESP3 boolean true equals the integer 1.
        mine, theirs = _compared(record, item["compare"]), _compared({**other, "index": index}, item["compare"])
        if canonical_json(mine) == canonical_json(theirs):
            if item["expect_difference"]:
                unreached.append(index)
        else:
            (reached if item["expect_difference"] else unexplained).append(index)
    return reached, unexplained, unreached, uncompared, after_death


# One reply of a sequence: the reply as the server sent it, or the error it answered; no timestamps, so two
# runs' replies compare equal.
_SEQUENCE_REPLY = mapping_of(
    {
        "index": INTEGER,
        "reply": optional(object),
        "error": optional(str),
        "unanswered": optional(bool),
        # Raw bytes no reply began to answer.
        "no_reply": optional(bool),
        # The server closed the connection without a reply, its node living on.
        "closed": optional(bool),
        # A reply the client could not parse: its message, and the reply's bytes, escaped (the first 4 KB).
        "protocol_error": optional(str),
        "raw": optional(str),
    }
)
# One side of a sequence: its replies, the digest of the compared ones, and the exit status of a node that exited,
# and which node.
_SEQUENCE_SIDE = mapping_of(
    {
        "replies": ConfigField(list, items=_SEQUENCE_REPLY),
        "digest": TEXT,
        "exited": optional(int),
        "exited_node": optional(int),
    }
)
_COMMANDS_DECLARES = Declarations(
    evidence={
        "valkey.command.sequence": declared(
            mapping_of(
                {
                    "candidate": _SEQUENCE_SIDE,
                    # The parent's side with compare_with; null on a deployment instance.
                    "parent": ConfigField(dict, allow_none=True, schema=_SEQUENCE_SIDE.schema),
                    # The modules each private server loaded; null on a deployment instance.
                    "modules": ConfigField(
                        dict,
                        allow_none=True,
                        schema=mapping_of(
                            {"candidate": ConfigField(list, items=TEXT), "parent": ConfigField(list, items=TEXT)}
                        ).schema,
                    ),
                    "cluster": FLAG,
                    "nodes": INTEGER,
                    "dropped_config": ConfigField(list, items=TEXT),
                    # Marked commands whose replies differ, and unmarked ones whose replies differ, by index.
                    "reached": ConfigField(list, items=INTEGER),
                    "unexplained": ConfigField(list, items=INTEGER),
                    # With compare_with: how many commands ran only on the build under test, the parent having died.
                    "uncompared": optional(int),
                    # With compare_with: the commands whose reply on the build under test violates their expect.
                    "violating": optional(list),
                    # Without compare_with: the instance the sequence ran on.
                    "instance_id": optional(str),
                    # With checks: each enabled check's outcome, by name (see checks.py).
                    "checks": optional(dict),
                }
            ),
            many=True,
        )
    },
    changes=("process.private-server",),
)

REGISTRATIONS = (
    component_registration(
        "valkey.action.commands",
        CommandsAction,
        _COMMANDS_FIELDS,
        check=_validate_commands,
        selectors={"instances": ()},
        declares=_COMMANDS_DECLARES,
    ),
)

__all__ = [
    "PRIVATE_SERVER_DEFAULTS",
    "REGISTRATIONS",
    "SUBSCRIBE",
    "UNSUBSCRIBE",
    "CommandsAction",
    "RawConnection",
    "Side",
    "compare",
    "free_ports",
    "is_binary",
    "new_side",
    "open_raw_connection",
    "port_block",
    "raw_reply",
    "reply_at",
    "run_sequence",
    "step_text",
    "text",
    "validate_sequence_command",
    "without_index",
]
