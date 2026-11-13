"""The cores a run may use, and whether a drawn deployment fits them.

A deployment's validators read every instance within a window (30 s in
cluster chaos). On a host whose cores the deployment's threads oversubscribe
they miss it, and the run is an ERROR that says nothing about Valkey; in a
batch, such a run also slows the runs beside it. So a deployment provider says
how many cores a plan's deployment needs
(:attr:`~valkey_fuzzer.registrations.DeploymentProviderRegistration.core_demand`),
and a random run may use only so many: the cores this process may use
(:func:`usable_cores`), or, in a batch, its worker's share of the batch's. A
draw that needs more is drawn again (:func:`~valkey_fuzzer.generator.generate`);
when no attempt fits, the run is ``SKIP_UNSUPPORTED``, saying why. A scenario
read from a file runs as written: whoever wrote it chose its size.
"""

import os
from typing import Optional

from .catalog import Catalog
from .compiler import deployment_registration
from .plan import ExecutionPlan

# The cgroup files that hold a CPU quota: v2 ``cpu.max`` ("<quota> <period>" or "max <period>"), and v1's pair.
CGROUP_V2_CPU_MAX = "/sys/fs/cgroup/cpu.max"
CGROUP_V1_QUOTA = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us"
CGROUP_V1_PERIOD = "/sys/fs/cgroup/cpu/cpu.cfs_period_us"


def usable_cores() -> float:
    """The cores this process may run on: its CPU affinity (its cpuset), less when its cgroup's CPU quota is less.

    The quota is read where a container sees its own cgroup (``/sys/fs/cgroup``); a quota on a cgroup further up
    is not seen.
    """

    cores = float(len(os.sched_getaffinity(0)))
    quota = _cgroup_quota()
    return cores if quota is None else min(cores, quota)


def _read(path: str) -> Optional[str]:
    try:
        with open(path, encoding="ascii") as handle:
            return handle.read().strip()
    except (OSError, ValueError):
        return None


def _cgroup_quota() -> Optional[float]:
    """The CPU quota of this process's cgroup, in cores; None when it has none or none can be read."""

    text = _read(CGROUP_V2_CPU_MAX)
    if text is not None:
        quota, _, period = text.partition(" ")
    else:
        quota, period = _read(CGROUP_V1_QUOTA), _read(CGROUP_V1_PERIOD)
    try:
        quota_us, period_us = int(quota), int(period)
    except (TypeError, ValueError):
        # "max", -1, or no cgroup files: no quota.
        return None
    return quota_us / period_us if quota_us > 0 and period_us > 0 else None


def shortfall(plan: ExecutionPlan, catalog: Catalog, cores: Optional[float]) -> Optional[str]:
    """Why ``plan``'s deployment cannot run in ``cores`` cores, or None when it fits (always, for ``cores`` None
    or a provider that says nothing of its demand)."""

    if cores is None:
        return None
    demand = deployment_registration(plan, catalog).core_demand
    if demand is None:
        return None
    needed = demand(plan)
    if needed <= cores:
        return None
    return (
        f"its {len(plan.instances)} instance(s) need {needed:.1f} cores to be validated in time, "
        f"and this run may use {cores:.1f}"
    )
