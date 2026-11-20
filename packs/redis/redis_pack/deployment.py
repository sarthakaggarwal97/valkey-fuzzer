"""The ``redis`` deployment: one ``redis-server`` process on this host, started by this pack's provider.

The provider knows nothing of Valkey's deployment: it resolves ``redis_binary``, makes a data directory and a
log file under ``base_data_dir``, tags the deployment directory for the fuzzer's host-state sweep
(:mod:`valkey_fuzzer.hoststate`), spawns the server, and waits for it to answer as the process it started.
The service it publishes under ``context.services["deployment"]["redis.deployment"]`` is what this pack's
components use: a raw connection, the process's PID and liveness, a signal, a restart, and the log files.

The deployment document::

    deployment:
      mode: redis
      base_port: 7000               # the server's port
      redis_binary: redis-server    # a path, or a command name on PATH
      base_data_dir: /tmp/valkey-fuzzer
      server_config: {appendonly: "yes"}   # redis-server options, each a --name value argument
"""

import logging
import os
import shutil
import socket
import subprocess
import time
import uuid
from collections.abc import Mapping, Sequence
from typing import Any, Optional

import valkey

from valkey_fuzzer._data import file_sha256, to_plain_data
from valkey_fuzzer.clock import now
from valkey_fuzzer.hoststate import server_tag_arguments, shared_directory, tag_directory
from valkey_fuzzer.plan import ExecutionPlan, StartupOverlay
from valkey_fuzzer.protocols import ProvisionedDeployment, RunContext
from valkey_fuzzer.registrations import DeploymentProviderRegistration
from valkey_fuzzer.schema import ConfigField, ConfigSchema

logger = logging.getLogger(__name__)

MODE = "redis"
PROVIDER_ID = "redis.deployment.local"
SERVICE = "redis.deployment"
INSTANCE_ID = "redis-0"
# The root the fuzzer's host-state sweep scans (valkey_fuzzer.hoststate.default_roots), shared by every user's runs.
DEFAULT_BASE_DATA_DIR = "/tmp/valkey-fuzzer"
# How long a start or a restart may take before the server answers.
READY_TIMEOUT_S = 30.0
# How long a stopped server may take to exit after SIGTERM before it is killed.
STOP_TIMEOUT_S = 5.0
# What the server runs with unless the document or a startup overlay says otherwise: no RDB snapshots, so a
# restart loads the append-only file alone when an experiment turns it on, and nothing when it does not.
DEFAULT_SERVER_CONFIG: Mapping[str, Any] = {"save": "", "appendonly": "no"}

DOCUMENT = ConfigSchema(
    {
        "mode": ConfigField(str, choices=(MODE,)),
        "base_port": ConfigField(int, default=7000, minimum=1024, maximum=65535),
        "redis_binary": ConfigField(str, default="redis-server"),
        "base_data_dir": ConfigField(str, default=DEFAULT_BASE_DATA_DIR),
        "server_config": ConfigField(dict, default=dict),
    }
)


class RedisDeploymentError(RuntimeError):
    """The deployment could not be started, restarted, or cleaned up."""


def normalize_document(document: Mapping[str, Any]) -> dict[str, Any]:
    """The deployment document with this provider's defaults filled in; ``ValueError`` for one it cannot run."""

    return DOCUMENT.resolve(document, "deployment")


def effective_server_config(document: Mapping[str, Any], overlays: Sequence[StartupOverlay]) -> dict[str, Any]:
    """The options the server starts with: the defaults under the document's ``server_config`` and the overlays."""

    config = dict(DEFAULT_SERVER_CONFIG)
    config.update(to_plain_data(document.get("server_config") or {}))
    for overlay in overlays:
        config.update(to_plain_data(overlay.server_config))
    return config


def plan_instances(document: Mapping[str, Any], overlays: Sequence[StartupOverlay]) -> dict[str, dict[str, Any]]:
    """The one instance the provider starts, with its effective server config (pure)."""

    normalized = normalize_document(document)
    return {
        INSTANCE_ID: {"role": "primary", "shard": None, "server_config": effective_server_config(normalized, overlays)}
    }


def resolve_binary(configured: str) -> str:
    """An absolute executable path for a configured path or command name."""

    if os.path.sep in configured or configured.startswith("."):
        if not os.path.isfile(configured):
            raise FileNotFoundError(f"redis_binary {configured!r} does not exist")
        if not os.access(configured, os.X_OK):
            raise PermissionError(f"redis_binary {configured!r} is not executable")
        return os.path.abspath(configured)
    found = shutil.which(configured)
    if found is None:
        raise FileNotFoundError(f"redis_binary {configured!r} is not on PATH")
    return found


def server_version(binary: str) -> Optional[str]:
    """The first line of ``redis-server --version``, or None when it cannot be run."""

    try:
        output = subprocess.run([binary, "--version"], capture_output=True, text=True, timeout=5, check=True).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    return output.strip().splitlines()[0] if output.strip() else None


def port_is_free(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            probe.bind((host, port))
        except OSError:
            return False
    return True


def process_output_path(log_file: str) -> str:
    """The file the server's stdout and stderr go to, beside its log."""

    root, _ = os.path.splitext(log_file)
    return f"{root}.stdio.log"


def server_command(binary: str, host: str, port: int, data_dir: str, log_file: str, config: Mapping[str, Any]) -> list:
    """The ``redis-server`` command line: the address, the files, the sweep's tag, then ``config`` as options."""

    command = [
        binary,
        "--port",
        str(port),
        "--bind",
        host,
        "--protected-mode",
        "no",
        "--dir",
        data_dir,
        "--logfile",
        log_file,
        *server_tag_arguments(),
    ]
    for name, value in config.items():
        command += [f"--{name}", "" if value is None else str(value)]
    return command


def _tail(path: str, lines: int = 10) -> str:
    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            kept = [line.strip() for line in handle.read().splitlines() if line.strip()][-lines:]
    except OSError:
        return ""
    return f" (last lines of {path}: " + " | ".join(kept) + ")" if kept else ""


class RedisServer:
    """One ``redis-server`` process: its files, its command line, and its process."""

    def __init__(self, binary: str, host: str, port: int, data_dir: str, log_file: str, config: Mapping[str, Any]):
        self.binary = binary
        self.host = host
        self.port = port
        self.data_dir = data_dir
        self.log_file = log_file
        self.output_file = process_output_path(log_file)
        self.config = dict(config)
        self.process: Optional[subprocess.Popen] = None
        self.started_at: Optional[float] = None

    @property
    def pid(self) -> Optional[int]:
        return None if self.process is None else self.process.pid

    def is_alive(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def start(self) -> None:
        command = server_command(self.binary, self.host, self.port, self.data_dir, self.log_file, self.config)
        logger.debug("Starting %s", " ".join(command))
        # Appended to a file, never left in a pipe that could fill and block the server.
        with open(self.output_file, "ab") as output:
            self.process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT)
        self.started_at = now()

    def answers(self) -> bool:
        """True once the server on the port is the process this object started and it is not loading."""

        client = valkey.Valkey(host=self.host, port=self.port, socket_timeout=2.0, socket_connect_timeout=2.0)
        try:
            info = client.info("server")
            if int(info.get("process_id", -1)) != self.pid:
                raise RedisDeploymentError(
                    f"port {self.port} is served by pid {info.get('process_id')}, not by the process this deployment "
                    f"started (pid {self.pid}); refusing to use a foreign server"
                )
            return str(client.info("persistence").get("loading", "0")) in ("0", "False", "false")
        except (valkey.ConnectionError, valkey.TimeoutError, valkey.BusyLoadingError):
            return False
        finally:
            client.close()

    def wait_ready(self, timeout: float, context: Optional[RunContext] = None) -> None:
        """Wait until the server answers; a process that exits first, or does not answer in ``timeout``, raises."""

        deadline = time.monotonic() + timeout
        while True:
            exit_code = self.process.poll() if self.process is not None else None
            if exit_code is not None:
                raise RedisDeploymentError(
                    f"{INSTANCE_ID} exited with code {exit_code} before answering{_tail(self.output_file)}"
                    f"{_tail(self.log_file)}"
                )
            if self.answers():
                return
            if time.monotonic() >= deadline or (context is not None and context.cancelled):
                raise RedisDeploymentError(f"{INSTANCE_ID} did not answer within {timeout:g}s{_tail(self.log_file)}")
            time.sleep(0.05)

    def send_signal(self, signum: int) -> None:
        if self.process is None or self.process.poll() is not None:
            raise ProcessLookupError(f"{INSTANCE_ID} is not running")
        self.process.send_signal(signum)

    def wait_exit(self, timeout: float) -> bool:
        if self.process is None:
            return True
        try:
            self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return False
        return True

    def stop(self) -> None:
        """SIGTERM, then SIGKILL after :data:`STOP_TIMEOUT_S`; a no-op for a server that is gone."""

        if self.process is None or self.process.poll() is not None:
            return
        self.process.terminate()
        if not self.wait_exit(STOP_TIMEOUT_S):
            self.process.kill()
            self.process.wait()


class RedisDeploymentService:
    """What this pack's components see under ``context.services["deployment"]["redis.deployment"]``."""

    def __init__(self, server: RedisServer, deployment_id: str, context: RunContext):
        self._server = server
        self._context = context
        self.deployment_id = deployment_id
        self.mode = MODE
        self.instances = ({"instance_id": INSTANCE_ID, "role": "primary", "host": server.host, "port": server.port},)

    def _check(self, instance_id: str) -> RedisServer:
        if instance_id != INSTANCE_ID:
            raise KeyError(f"unknown instance {instance_id!r}; this deployment has {INSTANCE_ID!r}")
        return self._server

    def connection(self, instance_id: str, timeout: float = 5.0) -> valkey.connection.Connection:
        """A raw, unconnected RESP connection: ``connect()``, ``send_command(*args)``, ``read_response()``."""

        server = self._check(instance_id)
        return valkey.connection.Connection(
            host=server.host, port=server.port, socket_timeout=timeout, socket_connect_timeout=timeout
        )

    def new_client(self, instance_id: str, timeout: float = 5.0) -> valkey.Valkey:
        server = self._check(instance_id)
        return valkey.Valkey(host=server.host, port=server.port, socket_timeout=timeout, socket_connect_timeout=timeout)

    def pid(self, instance_id: str) -> Optional[int]:
        return self._check(instance_id).pid

    def is_alive(self, instance_id: str) -> bool:
        return self._check(instance_id).is_alive()

    def send_signal(self, instance_id: str, signum: int) -> None:
        self._check(instance_id).send_signal(signum)

    def wait_exit(self, instance_id: str, timeout: float = 10.0) -> bool:
        return self._check(instance_id).wait_exit(timeout)

    def restart(self, instance_id: str, ready_timeout: float = READY_TIMEOUT_S) -> None:
        """Start the server again on its address, files, and options, and wait until it answers."""

        server = self._check(instance_id)
        server.stop()
        server.start()
        server.wait_ready(ready_timeout, self._context)

    def log_file(self, instance_id: str) -> str:
        return self._check(instance_id).log_file

    def process_output_file(self, instance_id: str) -> str:
        return self._check(instance_id).output_file

    def data_dir(self, instance_id: str) -> str:
        return self._check(instance_id).data_dir


class RedisDeploymentProvider:
    """Start one ``redis-server`` for a run and remove it afterwards."""

    def __init__(self) -> None:
        self._server: Optional[RedisServer] = None
        self._deployment_dir: Optional[str] = None
        self._deployment: Optional[ProvisionedDeployment] = None

    def validate(self, plan: ExecutionPlan) -> None:
        """A pure preflight: the document resolves, and selects this provider's mode."""

        document = normalize_document(to_plain_data(plan.deployment))
        if plan.deployment_mode != document["mode"]:
            raise ValueError(
                f"deployment document selects {document['mode']!r}, but the plan selects {plan.deployment_mode!r}"
            )

    def provision(self, context: RunContext) -> ProvisionedDeployment:
        if self._server is not None:
            raise RedisDeploymentError("this provider already owns a deployment")
        plan = context.plan
        document = normalize_document(to_plain_data(plan.deployment))
        binary = resolve_binary(document["redis_binary"])
        host, port = "127.0.0.1", document["base_port"]
        if not port_is_free(host, port):
            raise RedisDeploymentError(f"cannot provision the redis deployment: port {port} is in use")
        deployment_id = str(uuid.uuid4())[:8]
        base_data_dir = os.path.abspath(document["base_data_dir"])
        log_dir = os.path.join(base_data_dir, "logs")
        if os.path.normpath(document["base_data_dir"]) == DEFAULT_BASE_DATA_DIR:
            shared_directory(base_data_dir)
            shared_directory(log_dir)
        os.makedirs(log_dir, exist_ok=True)
        deployment_dir = os.path.join(base_data_dir, f"deployment-{deployment_id}")
        os.makedirs(deployment_dir, exist_ok=True)
        # Tagged while still empty, so a kill in between leaves no untagged tree for the sweep to miss.
        tag_directory(deployment_dir, "deployment", f"redis deployment {deployment_id} seed {plan.seed}")
        self._deployment_dir = deployment_dir
        data_dir = os.path.join(deployment_dir, INSTANCE_ID, "data")
        os.makedirs(data_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"{plan.seed}-{deployment_id}-{INSTANCE_ID}.log")
        config = effective_server_config(document, plan.startup_overlays)
        server = RedisServer(binary, host, port, data_dir, log_file, config)
        self._server = server
        version = server_version(binary)
        logger.info("Starting redis-server on port %s (%s)", port, version or binary)
        server.start()
        server.wait_ready(READY_TIMEOUT_S, context)
        service = RedisDeploymentService(server, deployment_id, context)
        metadata = {
            "deployment_id": deployment_id,
            "mode": MODE,
            "redis_version": version,
            "instances": [
                {
                    "instance_id": INSTANCE_ID,
                    "role": "primary",
                    "host": host,
                    "port": port,
                    "pid": server.pid,
                    "binary": binary,
                    "binary_sha256": file_sha256(binary),
                }
            ],
        }
        self._deployment = ProvisionedDeployment(handle=service, services={SERVICE: service}, metadata=metadata)
        return self._deployment

    def log_files(self, context: RunContext) -> dict[str, tuple[str, ...]]:
        """The log and the process output of the server, which outlive the cleanup: the post-mortem reads them."""

        del context
        if self._server is None:
            return {}
        return {INSTANCE_ID: (self._server.log_file, self._server.output_file)}

    def _release(self) -> None:
        if self._server is not None:
            self._server.stop()
        if self._deployment_dir is not None:
            shutil.rmtree(self._deployment_dir, ignore_errors=True)
            self._deployment_dir = None
        self._deployment = None

    def rollback(self, context: RunContext) -> None:
        del context
        self._release()

    def cleanup(self, context: RunContext, deployment: ProvisionedDeployment) -> None:
        del context
        if deployment is not self._deployment:
            raise RedisDeploymentError("cleanup received a deployment not owned by this provider")
        self._release()


def binary_override(path: str) -> dict[str, str]:
    """The deployment fields that run the server ``path`` (``--valkey-binary``, named for the first target)."""

    return {"redis_binary": path}


def recorded_instances(outcome: Mapping[str, Any]) -> Sequence[Mapping[str, Any]]:
    """The per-instance provenance :meth:`RedisDeploymentProvider.provision` left in a recorded outcome."""

    deployment = (outcome.get("diagnostics") or {}).get("deployment") or {}
    return tuple(row for row in deployment.get("instances") or () if isinstance(row, Mapping))


def binary_drift(
    plan: ExecutionPlan,
    recorded: Sequence[Mapping[str, Any]],
    *,
    override: Optional[str] = None,
    replayed: Optional[Sequence[Mapping[str, Any]]] = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """How the server a replay of ``plan`` runs differs from each ``recorded`` instance's, by SHA-256.

    Before the run the configured binary (``override`` first) is resolved and hashed; given the ``replayed``
    instances, the replay's own deployment metadata is used.
    """

    drift: list[dict[str, Any]] = []
    unchecked: list[str] = []
    for instance in recorded:
        instance_id, recorded_digest = instance["instance_id"], instance.get("binary_sha256")
        if not recorded_digest:
            unchecked.append(f"redis binary of {instance_id}: the recorded run has no digest for it")
            continue
        if replayed is not None:
            row = next((item for item in replayed if item.get("instance_id") == instance_id), {})
            path, digest = row.get("binary"), row.get("binary_sha256")
        else:
            setting = override or to_plain_data(plan.deployment).get("redis_binary") or "redis-server"
            try:
                path = resolve_binary(setting)
            except (FileNotFoundError, PermissionError):
                path = None
            digest = file_sha256(path) if path else None
        if not digest:
            unchecked.append(f"redis binary of {instance_id}: the replay's binary cannot be read")
        elif digest != recorded_digest:
            drift.append(
                {
                    "item": f"redis binary of {instance_id}",
                    "recorded": f"{instance.get('binary')} (sha256 {recorded_digest})",
                    "replay": f"{path} (sha256 {digest})",
                }
            )
    return drift, unchecked


def deployment_provider_registration() -> DeploymentProviderRegistration:
    return DeploymentProviderRegistration(
        deployment_mode=MODE,
        provider_id=PROVIDER_ID,
        factory=RedisDeploymentProvider,
        normalizer=normalize_document,
        plan_instances=plan_instances,
        binary_override=binary_override,
        recorded_instances=recorded_instances,
        binary_drift=binary_drift,
        host_fields=("base_port", "base_data_dir"),
    )


__all__ = [
    "DEFAULT_SERVER_CONFIG",
    "INSTANCE_ID",
    "MODE",
    "PROVIDER_ID",
    "SERVICE",
    "RedisDeploymentProvider",
    "RedisDeploymentService",
    "RedisServer",
    "deployment_provider_registration",
    "effective_server_config",
    "normalize_document",
    "plan_instances",
    "server_command",
]
