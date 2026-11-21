"""A batch's options and sources, as a batch spec or the command line gives them."""

import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Optional

from ..deployment.local.config import CLUSTER_BUS_PORT_OFFSET, MAX_TCP_PORT
from ..deployment.local.ports import ephemeral_overlap
from ..reports import InputError, format_version_problem, load_document, read_yaml_mapping
from ..schema import ConfigError, ConfigField, ConfigSchema
from ..service import CONFORM_TLC_TIMEOUT_S
from . import hooks

# Client ports per worker: a cluster of 16 primaries with 5 replicas each is 96 instances.
PORT_BLOCK = 120
DEFAULT_PORT_BASE = 11000

STATUS_ORDER = ("FAIL", "ERROR", "SKIP_UNSUPPORTED", "PASS")


class BatchError(ValueError):
    """A batch spec, source, or folder that cannot be used."""


def worst(statuses: Sequence[str]) -> str:
    """The worst of ``statuses`` (FAIL, then ERROR, then SKIP_UNSUPPORTED); PASS for none."""

    return next((status for status in STATUS_ORDER if status in statuses), "PASS")


# The options a batch spec may set, also checked on BatchOptions; the dataclass holds their defaults.
_OPTION_FIELDS = {
    "batch_id": ConfigField(str, required=False),
    "profile": ConfigField(str, required=False, allow_none=True),
    "workers": ConfigField(int, minimum=1, required=False),
    "timeout_s": ConfigField(float, required=False, allow_none=True),
    "port_base": ConfigField(int, minimum=1024, maximum=MAX_TCP_PORT, required=False),
    "valkey_binary": ConfigField(str, required=False, allow_none=True),
    "prune_pass": ConfigField(bool, required=False),
    "serial_reruns": ConfigField(int, minimum=0, required=False),
    "conform_tlc_timeout_s": ConfigField(float, required=False),
    "findings": ConfigField(str, required=False, allow_none=True),
}
_SPEC = ConfigSchema({**_OPTION_FIELDS, "sources": ConfigField(list, required=False, allow_none=True)})


@dataclass
class BatchOptions:
    """How a batch runs; ``timeout_s`` None means the profile's budget, or :data:`DEFAULT_TIMEOUT_S`.

    ``conform_tlc_timeout_s`` bounds the TLC run of each run's ``spec conform`` check on its observed states
    (:meth:`~valkey_fuzzer.service.RunService.write_run_report`, :mod:`.conform_plugin`); a run it stops is a
    conform ``error`` that names this option. ``findings`` is the findings store the batch appends its failure
    signatures to (:mod:`valkey_fuzzer.findings`); None means ``findings.json`` in the batch folder.
    """

    batch_id: str = "batch"
    profile: Optional[str] = None
    workers: int = 1
    timeout_s: Optional[float] = None
    port_base: int = DEFAULT_PORT_BASE
    valkey_binary: Optional[str] = None
    prune_pass: bool = False
    serial_reruns: int = 0
    conform_tlc_timeout_s: float = CONFORM_TLC_TIMEOUT_S
    findings: Optional[str] = None
    experiment_directories: tuple[str, ...] = ()

    def check(self) -> None:
        try:
            _SPEC.resolve({name: getattr(self, name) for name in _OPTION_FIELDS}, "batch")
        except ConfigError as exc:
            raise BatchError(str(exc)) from None
        if not self.batch_id.strip():
            raise BatchError("batch.batch_id must name the batch")
        if self.timeout_s is not None and self.timeout_s <= 0:
            raise BatchError("batch.timeout_s must be a number of seconds greater than zero")
        # Each plug-in checks the options it carries into the workers' tasks (conform_tlc_timeout_s is spec conform's).
        hooks.task_options(self)
        # Bus ports sit CLUSTER_BUS_PORT_OFFSET above the client ports: neither may reach another worker's block.
        span = self.workers * PORT_BLOCK
        if span > CLUSTER_BUS_PORT_OFFSET or self.port_base + span - 1 + CLUSTER_BUS_PORT_OFFSET > MAX_TCP_PORT:
            raise BatchError(
                f"{self.workers} workers need ports {self.port_base}..{self.port_base + span - 1} and their "
                f"cluster bus ports {CLUSTER_BUS_PORT_OFFSET} higher: use fewer workers or a lower port_base"
            )
        overlap = ephemeral_overlap(self.port_base, span, cluster=True, field="port_base")
        if overlap:
            raise BatchError(f"the workers' {overlap}, or use fewer workers")

    def ports(self, slot: int) -> tuple[int, int]:
        """The first and last client port of worker ``slot``."""

        low = self.port_base + slot * PORT_BLOCK
        return low, low + PORT_BLOCK - 1

    @staticmethod
    def loopback_block(slot: int) -> int:
        """The deployment ``loopback_block`` of worker ``slot``: block 0 stays for runs outside a batch."""

        return slot + 1


def absolute(path: str, base_dir: str, where: str) -> str:
    """``path`` made absolute from ``base_dir``; ``where`` names it in the error for an empty one."""

    if not path:
        raise BatchError(f"{where} must be a path")
    return os.path.abspath(os.path.join(base_dir, os.path.expanduser(path)))


def normalize_source(source: Any, base_dir: str) -> dict[str, Any]:
    """One source, checked, with its defaults and its paths absolute (relative ones are read from ``base_dir``)."""

    if not isinstance(source, Mapping) or len(source) != 1 or next(iter(source)) not in hooks.SOURCES:
        raise BatchError(f"each source is a mapping with one key out of {list(hooks.SOURCES)}, got {source!r}")
    kind, value = next(iter(source.items()))
    where = f"source {kind}"
    definition = hooks.SOURCES[kind]
    shorthand = definition.shorthand(value) if definition.shorthand is not None else None
    if shorthand is not None:
        value = shorthand
    try:
        settings = definition.schema.resolve(value, where)
    except ConfigError as exc:
        raise BatchError(str(exc)) from None
    return {kind: definition.normalize(settings, base_dir, where)}


def read_batch_file(path: str) -> Any:
    """A batch folder's JSON file; a ``BatchError`` names one that is missing, unreadable, or another fuzzer version's."""

    try:
        document = load_document(path)
    except InputError as exc:
        raise BatchError(f"not a batch folder: {exc}") from None
    problem = format_version_problem(document, path)
    if problem is not None:
        raise BatchError(problem)
    return document


def load_spec(path: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """A batch spec file's options (``batch_id``, ``profile``, ...) and its normalized sources."""

    try:
        document = _SPEC.resolve(read_yaml_mapping(path), "batch spec")
    except (InputError, ConfigError) as exc:
        raise BatchError(str(exc)) from None
    base_dir = os.path.dirname(os.path.abspath(path))
    sources = document.pop("sources", None) or []
    for name in ("valkey_binary", "findings"):
        if document.get(name):
            document[name] = absolute(document[name], base_dir, name)
    return document, [normalize_source(source, base_dir) for source in sources]
