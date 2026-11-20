"""The batch runner: the one way to run many scenarios.

A batch expands its **sources** into jobs, one run each:

- ``random``: seeds ``first_seed .. first_seed + seeds - 1`` for each listed
  experiment (or ``all`` that declare ``metadata.generator``), drawn under the
  batch's search profile;
- ``matrix``: a base scenario over every combination of dotted-path values;
- ``scenarios``: scenario files, or directories of them, run as written;
- ``zoo``: the bug zoo's cases, each a clean control and broken runs on its builds;
- ``failures``: the failed runs of an earlier batch, a sample per failure signature, each
  as it ran (a random job as the scenario it drew).

Every job runs in its own worker process (``python -m valkey_fuzzer.batch
TASK``) on one of ``workers`` slots. Slot ``k`` owns the client ports
``port_base + k * PORT_BLOCK`` up to the next block (and their cluster bus
ports), the per-instance loopback addresses ``127.0.<k+1>.0/24``, and its own
data directory, ``TMPDIR``, and provider directories, which are wiped after
each job, so no two concurrent runs share ports, addresses, logs, or
directories (a partition matches addresses, so a shared address would let one
worker's partition cut another's instances). After each run the host :func:`~valkey_fuzzer.hoststate.sweep`
removes whatever that worker left behind (servers, chains, mounts, data
directories); a leftover is a finding that makes the job an ERROR, and a slot
whose sweep cannot remove one runs no more jobs (the batch stops once none is left).
Before its first job a batch removes the servers, chains, and mounts of every
fuzzer process that is gone, and refuses to start if one stays.

A random job's deployment is drawn to fit its worker's share of the batch's
cores: the cores the process that created the batch may use (its CPU affinity
and cgroup quota, recorded as ``cores`` in ``batch.json``) over ``workers``
(:mod:`~valkey_fuzzer.capacity`). So a large run neither misses its own
validation window nor slows the runs beside it.

The batch folder::

    batch.json          options, sources, the zoo cases, and the cores, as resolved (written once)
    jobs.json           every job, in order
    workers.json        the jobs running now and their worker processes (for resume)
    results/<id>.json   one finished job: status, failure signature, leaks, zoo observation
    runs/<id>/          the job's run folder: plan.json, report.json, scenario.yaml,
                        components/, logs/ (instance logs), console.log, and conform.json
                        for a run from before report.json's conform section
    conformance/<b>.json  the spec conform report of spec bundle <b> over the runs that exercised it
    summary.json        counts, failure signatures, serial re-runs, zoo verdicts, conformance, leaks, exit code
    findings.json       the failure signatures across batches (:mod:`valkey_fuzzer.findings`), unless the
                        ``findings`` option names a store elsewhere
    batch.log           one line per finished job

A batch resumes where it stopped: finished jobs keep their results, a job that
was running is swept and run again (its slot retired if the sweep could not
remove what it left), and a worker that outlived a killed batch is stopped
first. Non-PASS runs are grouped by the minimizer's failure signature
(:func:`~valkey_fuzzer.minimize.failure_signature`, named by its
:func:`~valkey_fuzzer.minimize.signature_key`); with ``serial_reruns`` a
sample of each group runs again one at a time, which separates failures that
need load from real ones. When the batch finishes, each group is appended to
the findings store (``findings.json`` beside ``summary.json``, or the one the
``findings`` option names, which several batches share), one entry per key.

Each run whose plan names a spec bundle (a component's ``spec``) checks
itself against it as its report is written, as ``valkey-fuzzer spec conform``
would (:func:`~valkey_fuzzer.spec.conform.conform_run`; TLC from
``$TLA2TOOLS_JAR``, bounded by the batch's ``conform_tlc_timeout_s``), and
the batch reuses the report's ``conform`` section; a run from before that
section is checked by its worker after the run. For a bundle whose claims
are approved, a claimed property violated on the run's observed states fails
the run, and a check that could not decide is an ERROR; a draft's findings
are only reported. The batch's status is the worst of its runs, its
leaks (ERROR), its zoo cases (FAIL when one does not hold), and its matrix
combinations that do not compile (ERROR).

The package::

    spec.py             the options and the sources a batch spec names; ``normalize_source``, ``load_spec``
    sources.py          the built-in sources: random, matrix, scenarios, failures
    jobs.py             ``Expansion`` and ``expand``: the sources' jobs, in order
    worker.py           ``run_worker``: one job in its own process (``python -m valkey_fuzzer.batch``)
    results.py          ``job_result``: a finished job's status and failure signature
    pool.py             ``BatchRunner``: the worker slots, sweeps, deadlines, resume
    summary.py          ``summary.json``
    hooks.py            the plug-in points: ``BatchSource`` and ``BatchPlugin``; the installed plug-ins
    zoo_plugin.py       the bug zoo as a plug-in
    conform_plugin.py   spec conformance as a plug-in

The plug-ins are the ``valkey_fuzzer.batch_plugins`` entry points, one per pack (``pyproject.toml``,
``docs/extending.md``); the fuzzer's own, ``valkey``, is :func:`shipped_plugins`.
"""

from ..minimize import signature_key, signature_label
from . import hooks, sources  # noqa: F401  (registers the built-in sources)
from .conform_plugin import ConformancePlugin
from .hooks import BatchPlugin, BatchSource
from .jobs import MAX_JOBS, Expansion, expand
from .pool import HOST_LOCK, WORKER_COMMAND, BatchRunner
from .results import Verdict, job_result
from .sources import failure_samples
from .spec import (
    DEFAULT_PORT_BASE,
    PORT_BLOCK,
    STATUS_ORDER,
    BatchError,
    BatchOptions,
    load_spec,
    normalize_source,
    worst,
)
from .worker import main, run_worker
from .zoo_plugin import ZooPlugin


def shipped_plugins() -> list[BatchPlugin]:
    """The ``valkey`` entry point of the ``valkey_fuzzer.batch_plugins`` group: the bug zoo, then spec conformance."""

    return [ZooPlugin(), ConformancePlugin()]


# The entry point above resolves to this module while it loads: shipped_plugins is bound before this line runs.
hooks.register_installed()

__all__ = [
    "DEFAULT_PORT_BASE",
    "HOST_LOCK",
    "MAX_JOBS",
    "PORT_BLOCK",
    "STATUS_ORDER",
    "WORKER_COMMAND",
    "BatchError",
    "BatchOptions",
    "BatchPlugin",
    "BatchRunner",
    "BatchSource",
    "Expansion",
    "Verdict",
    "expand",
    "failure_samples",
    "job_result",
    "load_spec",
    "main",
    "normalize_source",
    "run_worker",
    "shipped_plugins",
    "signature_key",
    "signature_label",
    "worst",
]
