"""The worker: one job in its own process, ``python -m valkey_fuzzer.batch TASK``."""

import os
import sys
from collections.abc import Sequence
from typing import Optional

from .._data import error_record
from ..compiler import Overrides, with_valkey_binary
from ..documents import merge
from ..generator import Exclusion
from ..hoststate import interrupt_on_sigterm
from ..outcomes import EXIT_HARNESS_ERROR, EXIT_INTERRUPTED, exit_code, print_outcome
from ..plan import ExecutionPlan
from ..reports import write_document
from ..service import PreparedRun, RunService
from . import hooks
from .conform_plugin import CONFORM_TIMEOUT_OPTION
from .results import ERROR_FILE, read_json
from .spec import PORT_BLOCK, BatchError


def _check_port_block(plan: ExecutionPlan) -> None:
    count = len(plan.instances)
    if count > PORT_BLOCK:
        raise BatchError(f"the scenario plans {count} instances; a batch worker's port block holds {PORT_BLOCK}")


def run_worker(task_path: str) -> int:
    """Run the job in ``task_path`` in its report folder; the exit code is the run's (2 when it cannot start)."""

    task = read_json(task_path)
    job, report_dir = task["job"], task["report_dir"]
    isolation = {
        "deployment": {
            "base_port": task["base_port"],
            "base_data_dir": task["data_dir"],
            "loopback_block": task["loopback_block"],
        }
    }
    service = RunService(
        experiment_directories=task["experiment_directories"],
        artifact_dir=task["artifact_dir"],
        cores=task["cores"],
        conform_tlc_timeout_s=task[CONFORM_TIMEOUT_OPTION],
    )
    try:
        if "generate" in job:
            draw = job["generate"]
            prepared = service.prepare_random(
                seed=draw["seed"],
                experiment=draw["experiment"],
                excludes=[Exclusion.parse(item) for item in draw["excludes"]],
                base=merge(draw["base"], isolation),
                profile=draw["profile"],
                free_ports=False,
                overrides=Overrides(binary=task["valkey_binary"], keep_own_binary=True),
            )
        else:
            scenario = merge(
                with_valkey_binary(service.catalog, job["scenario"], task["valkey_binary"], override=False), isolation
            )
            prepared = PreparedRun(scenario=scenario, plan=service.compile(scenario))
        _check_port_block(prepared.plan)
    except Exception as exc:
        error = error_record(exc, "prepare")
        write_document(os.path.join(report_dir, ERROR_FILE), error)
        print(f"Error: the job could not start: {error['type']}: {error.get('message', '')}")
        return EXIT_HARNESS_ERROR
    print(f"Job {job['id']}: {job.get('label', '')}")
    record = service.execute(
        prepared.plan,
        scenario=prepared.scenario,
        timeout=task["timeout_s"],
        report_dir=report_dir,
        generated=prepared.generated,
    )
    print_outcome(record.outcome, verbose=True)
    for plugin in hooks.PLUGINS:
        plugin.after_run(report_dir, task)
    return exit_code(record.outcome.status)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """``python -m valkey_fuzzer.batch TASK``: the worker process of one job."""

    arguments = sys.argv[1:] if argv is None else list(argv)
    if len(arguments) != 1:
        print("usage: python -m valkey_fuzzer.batch TASK (a batch worker; run batches with valkey-fuzzer batch)")
        return EXIT_HARNESS_ERROR
    try:
        # The batch stops a worker with SIGINT, then SIGTERM: either way its run cleans up first.
        with interrupt_on_sigterm():
            return run_worker(arguments[0])
    except KeyboardInterrupt:
        print("\nThe batch worker was interrupted")
        return EXIT_INTERRUPTED
