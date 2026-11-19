"""Jobs: what a batch's sources expand to, in order."""

import copy
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Optional

from ..generator import Profile, find_profile
from ..provenance import catalog_digest
from ..service import RunService
from . import hooks
from .spec import BatchError, BatchOptions

MAX_JOBS = 100_000


def _recorded() -> dict[str, Any]:
    return {plugin.name: None for plugin in hooks.PLUGINS if plugin.source is not None}


@dataclass
class Expansion:
    """What a batch's sources expand to: the jobs, matrix cases that do not compile, and what each plug-in's
    source recorded (the zoo cases), by plug-in name."""

    jobs: list[dict[str, Any]] = field(default_factory=list)
    invalid: list[dict[str, Any]] = field(default_factory=list)
    recorded: dict[str, Any] = field(default_factory=_recorded)
    # The profile the batch draws under, and its run deadline, for a batch that sets none.
    profile: Optional[Profile] = field(default=None, repr=False)
    profile_timeout_s: Optional[float] = None
    # provenance.catalog_digest when the batch has random jobs, which draw their scenarios when they run.
    catalog_digest: Optional[str] = None
    _ids: set[str] = field(default_factory=set, repr=False)

    def add(self, job: dict[str, Any]) -> None:
        """Append ``job``, its ID suffixed ``-2``, ``-3``, ... if taken; refuse one past :data:`MAX_JOBS`."""

        if len(self.jobs) >= MAX_JOBS:
            raise BatchError(f"the batch has more than {MAX_JOBS} jobs")
        base, index = job["id"], 2
        while job["id"] in self._ids:
            job["id"] = f"{base}-{index}"
            index += 1
        self._ids.add(job["id"])
        self.jobs.append(job)


def expand(sources: Sequence[Mapping[str, Any]], service: RunService, options: BatchOptions) -> Expansion:
    """Every job of ``sources``, in order."""

    expansion = Expansion()
    expansion.profile = None if options.profile is None else find_profile(service.catalog, options.profile)
    expansion.profile_timeout_s = None if expansion.profile is None else expansion.profile.timeout_s
    for index, source in enumerate(sources):
        kind, value = next(iter(source.items()))
        hooks.SOURCES[kind].expand(value, index, service, options, expansion)
    if any("generate" in job for job in expansion.jobs):
        expansion.catalog_digest = catalog_digest(service.catalog)
    return expansion


def job_definition(job: Mapping[str, Any]) -> dict[str, Any]:
    """A job without its batch-specific fields: what runs again the same way."""

    return {key: copy.deepcopy(job[key]) for key in ("id", "label", "generate", "scenario") if key in job}


def ids_per_signature(
    results: Iterable[Mapping[str, Any]], keep: Callable[[Mapping[str, Any]], bool], count: Optional[int]
) -> list[str]:
    """The IDs of up to ``count`` (all, for None) kept results of each failure signature, by signature then ID."""

    groups: dict[str, list[str]] = {}
    for result in results:
        if keep(result):
            groups.setdefault(result["signature_key"], []).append(result["id"])
    return [job_id for key in sorted(groups) for job_id in sorted(groups[key])[:count]]
