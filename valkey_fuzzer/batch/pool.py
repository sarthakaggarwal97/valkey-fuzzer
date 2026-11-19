"""The pool: a batch folder's jobs on its worker slots, with the host sweeps, the deadline escalation, and resume."""

import fcntl
import os
import shutil
import signal
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from ..capacity import usable_cores
from ..deadlines import wait_for
from ..deployment.local.ports import LOCK_DIRECTORY
from ..hoststate import Owner, open_shared_lock, owner_of, shared_directory, sweep
from ..provenance import catalog_digest
from ..reports import REPORT_FILE, SCENARIO_FILE, write_document
from ..service import DEFAULT_TIMEOUT_S, RunService
from . import hooks
from .jobs import Expansion, ids_per_signature, job_definition
from .results import job_result, read_json, results_of
from .spec import BatchError, BatchOptions
from .summary import summarize

# A run's own deadline is cooperative; the batch stops its worker this much later
# (or a quarter of the deadline later, if that is longer) with SIGINT, then
# SIGTERM, then SIGKILL, STOP_GRACE_S apart. Stopping a batch skips the wait.
DEADLINE_GRACE_S = 60.0
STOP_GRACE_S = (60.0, 30.0)
POLL_S = 0.2
# Held while a batch runs: every batch's workers use loopback blocks 1, 2, ..., so two at once would collide.
HOST_LOCK = os.path.join(LOCK_DIRECTORY, "batch")

WORKER_COMMAND = (sys.executable, "-m", "valkey_fuzzer.batch")
CONSOLE_FILE = "console.log"


@dataclass
class _Running:
    job: dict[str, Any]
    slot: int
    process: Any
    owner: str
    started: float
    deadline: float
    load_start: float
    signals_sent: int = 0
    last_signal: float = 0.0
    timed_out: bool = False


def _copy_experiments(folder: str, directories: Sequence[str]) -> None:
    """Copy the experiment documents of ``directories`` into ``<folder>/experiments``.

    The batch folder then holds what its runs loaded with ``--experiment-dir``,
    so a run replays from the folder alone (``replay --experiment-dir experiments``).
    """

    target = os.path.join(folder, "experiments")
    for directory in directories:
        for name in sorted(os.listdir(directory)):
            source = os.path.join(directory, name)
            if not os.path.isfile(source) or not name.lower().endswith((".yaml", ".yml")):
                continue
            if os.path.exists(os.path.join(target, name)):
                raise BatchError(f"two experiment directories hold {name}; the batch folder keeps one experiments/")
            os.makedirs(target, exist_ok=True)
            shutil.copy2(source, os.path.join(target, name))


def _prune(run_dir: str) -> None:
    """Remove everything of a passing run's folder but its report, scenario, and what the plug-ins keep."""

    kept = (REPORT_FILE, SCENARIO_FILE, *hooks.kept_when_pruned())
    for entry in os.listdir(run_dir):
        if entry not in kept:
            target = os.path.join(run_dir, entry)
            shutil.rmtree(target) if os.path.isdir(target) else os.remove(target)


class BatchRunner:
    """Runs a batch folder's jobs on its workers; see :mod:`valkey_fuzzer.batch`."""

    def __init__(self, folder: str):
        self.folder = os.path.abspath(folder)
        spec = read_json(os.path.join(self.folder, "batch.json"))
        self.options = BatchOptions(**spec["options"])
        self.spec = spec
        self.jobs: list[dict[str, Any]] = read_json(os.path.join(self.folder, "jobs.json"))
        self.stop_requested = False
        self._running: dict[int, _Running] = {}
        # Slots whose sweep could not remove a leftover: its ports or addresses may still be taken.
        self._retired: set[int] = set()

    # -- creating --------------------------------------------------------------

    @classmethod
    def create(
        cls,
        folder: str,
        options: BatchOptions,
        sources: Sequence[Mapping[str, Any]],
        expansion: Expansion,
    ) -> "BatchRunner":
        """Write a new batch folder for ``expansion`` and return its runner."""

        if not expansion.jobs:
            raise BatchError("the batch has no jobs")
        if os.path.exists(os.path.join(folder, "batch.json")):
            raise BatchError(f"{folder} already holds a batch; resume it instead")
        os.makedirs(folder, exist_ok=True)
        _copy_experiments(folder, options.experiment_directories)
        spec = {
            "options": asdict(options),
            "sources": list(sources),
            "invalid": expansion.invalid,
            **expansion.recorded,
            "profile_timeout_s": expansion.profile_timeout_s,
            "catalog_digest": expansion.catalog_digest,
            "cores": usable_cores(),
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        # jobs.json first: a folder with batch.json is complete.
        write_document(os.path.join(folder, "jobs.json"), expansion.jobs)
        write_document(os.path.join(folder, "batch.json"), spec)
        return cls(folder)

    def check_draws(self) -> None:
        """Refuse to resume random jobs that the installed code would draw differently from the batch's seeds."""

        recorded = self.spec["catalog_digest"]
        if recorded is None:
            return
        service = RunService(experiment_directories=self.options.experiment_directories)
        if catalog_digest(service.catalog) != recorded:
            raise BatchError(
                "the installed fuzzer or experiments changed since this batch was created, so its "
                "random jobs would draw other scenarios than their seeds drew; start a new batch"
            )

    # -- paths -----------------------------------------------------------------

    def _path(self, *parts: str) -> str:
        return os.path.join(self.folder, *parts)

    def _work(self, slot: int) -> str:
        return self._path("work", f"w{slot}")

    def log(self, line: str) -> None:
        print(line)
        with open(self._path("batch.log"), "a", encoding="utf-8") as handle:
            handle.write(f"{time.strftime('%Y-%m-%dT%H:%M:%S')} {line}\n")

    @property
    def timeout_s(self) -> float:
        return float(self.options.timeout_s or self.spec["profile_timeout_s"] or DEFAULT_TIMEOUT_S)

    @property
    def run_cores(self) -> float:
        """The cores each run may use: its worker's share of the batch's."""

        return self.spec["cores"] / self.options.workers

    # -- the run ---------------------------------------------------------------

    def run(self) -> dict[str, Any]:
        """Run every unfinished job, then the serial re-runs; return the summary (also written to summary.json)."""

        with open(self._path("batch.lock"), "w") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                raise BatchError(f"another process is running the batch in {self.folder}") from None
            shared_directory(os.path.dirname(HOST_LOCK))
            host = open_shared_lock(HOST_LOCK)
            try:
                try:
                    fcntl.flock(host, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError:
                    raise BatchError(
                        "another batch is running on this host: their workers would share loopback blocks (and "
                        "the default ports), so every job of this one would be an ERROR; run them one after the other"
                    ) from None
                return self._run_locked()
            finally:
                os.close(host)

    def _run_locked(self) -> dict[str, Any]:
        previous = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
        for sig in previous:
            signal.signal(sig, self._request_stop)
        try:
            self._recover()
            self._sweep_host()
            self.log(
                f"each run may use {self.run_cores:.1f} of the batch's {self.spec['cores']:g} cores "
                f"({self.options.workers} worker(s)); random draws fit their deployments to it"
            )
            done = results_of(self.folder)
            self._run_pool([job for job in self.jobs if job["id"] not in done], self.options.workers)
            if not self.stop_requested and self.options.serial_reruns:
                done = results_of(self.folder)
                reruns = [job for job in self.serial_jobs() if job["id"] not in done]
                if reruns:
                    self.log(f"serial re-runs: {len(reruns)} job(s), one at a time")
                self._run_pool(reruns, 1)
            # Nothing is left running: the next run has no workers to recover. A retired slot's work directory
            # stays for whoever removes what is stuck in it.
            Path(self._path("workers.json")).unlink(missing_ok=True)
            if not self._retired:
                shutil.rmtree(self._path("work"), ignore_errors=True)
            summary = self.summary()
            write_document(self._path("summary.json"), summary)
            return summary
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)

    def _request_stop(self, signum: int, _frame: Any) -> None:
        if not self.stop_requested:
            print(f"\nStopping: {signal.Signals(signum).name}; interrupting the running jobs (resume to finish)")
        else:
            # Another signal: the user will not wait out the grace periods.
            print(f"\n{signal.Signals(signum).name} again: sending the running jobs the next signal now")
            for item in self._running.values():
                item.last_signal = float("-inf")
        self.stop_requested = True

    def serial_jobs(self) -> list[dict[str, Any]]:
        """``serial_reruns`` jobs of each FAIL or ERROR signature of the parallel runs, to run again one at a time.

        Load can explain those; an unsupported run is unsupported however many runs share the host.
        """

        by_id = {job["id"]: job for job in self.jobs}
        picked = ids_per_signature(
            results_of(self.folder).values(),
            lambda result: result["id"] in by_id
            and not hooks.plugin_job(by_id[result["id"]])
            and result["status"] in ("FAIL", "ERROR"),
            self.options.serial_reruns,
        )
        return [
            {**job_definition(by_id[job_id]), "id": f"serial-{job_id}", "source": "serial", "origin": {"job": job_id}}
            for job_id in picked
        ]

    def summary(self) -> dict[str, Any]:
        """Counts, failure signatures with their serial re-runs, the plug-ins' entries, leaks, and the batch's status."""

        return summarize(self)

    # -- recovery and sweeps ---------------------------------------------------

    def _recover(self) -> None:
        """Stop workers a killed batch left running, sweep what they left, and requeue their jobs."""

        path = self._path("workers.json")
        if not os.path.exists(path):
            return
        for entry in read_json(path).values():
            owner = Owner.parse(entry["owner"])
            if owner is not None and owner.alive():
                self.log(f"stopping worker {entry['pid']} of {entry['job']}, left running by an interrupted batch")
                _signal_worker(entry["pid"], signal.SIGKILL)
                wait_for(
                    lambda owner=owner: (not owner.alive(), None), timeout_s=30, interval_s=POLL_S, reason="the worker"
                )
            leftovers = self._sweep(entry["slot"], entry["owner"])
            if leftovers:
                self.log(f"interrupted job {entry['job']}: {_swept(leftovers)}")
            self._retire_if_stuck(entry["slot"], entry["job"], leftovers)
            # A job whose result was written before the batch died has finished: its report stays.
            if not os.path.exists(self._path("results", f"{entry['job']}.json")):
                shutil.rmtree(self._path("runs", entry["job"]), ignore_errors=True)
        os.remove(path)

    def _sweep_host(self) -> None:
        """Remove the servers, chains, and mounts of every fuzzer process that is gone, before the first job.

        A batch killed outright leaves its workers' chains, which match the addresses of every later batch's
        slots: its jobs would see a partition nobody configured. Directories stay, as a dead run's evidence.
        """

        report = sweep([])
        for item in report.unchecked:
            self.log(f"not checked before the first job: {item}")
        found = [asdict(item) for item in report.leftovers]
        stuck = [leak for leak in found if leak["action"] == "failed"]
        if stuck:
            listed = "; ".join(f"{leak['kind']} {leak['target']}: {leak['reason']}" for leak in stuck)
            raise BatchError(
                f"could not remove {len(stuck)} leftover(s) of dead fuzzer processes ({listed[:300]}), which may "
                "take the workers' ports or addresses: sweep the host, then run the batch again"
            )
        if found:
            listed = ", ".join(f"{leak['kind']} {leak['target']}" for leak in found)
            self.log(f"removed {len(found)} leftover(s) of dead fuzzer processes: {listed[:300]}")

    def _retire_if_stuck(self, slot: int, job_id: str, leaks: Sequence[Mapping[str, Any]]) -> None:
        stuck = [leak for leak in leaks if leak["action"] == "failed"]
        if stuck:
            # The next job on this slot would find its ports or addresses taken, and its result would be wrong.
            self._retired.add(slot)
            self.log(
                f"worker slot {slot} runs no more jobs: {job_id} left {len(stuck)} leftover(s) the sweep "
                f"could not remove ({'; '.join(leak['reason'] for leak in stuck)[:200]})"
            )

    def _sweep(self, slot: int, owner: str) -> list[dict[str, Any]]:
        work = self._work(slot)
        # /tmp too: a private server's directory is there whatever TMPDIR is, so its unix socket path stays
        # short. Only the worker's own leftovers are removed.
        roots = [os.path.join(work, "data"), os.path.join(work, "tmp"), "/tmp"]
        report = sweep(roots, owners={owner})
        return [asdict(item) for item in report.leftovers]

    # -- the pool ----------------------------------------------------------------

    def _save_workers(self) -> None:
        write_document(
            self._path("workers.json"),
            {
                str(slot): {"slot": slot, "job": item.job["id"], "pid": item.process.pid, "owner": item.owner}
                for slot, item in self._running.items()
            },
        )

    def _run_pool(self, todo: list[dict[str, Any]], concurrency: int) -> None:
        """Run ``todo``, ``concurrency`` jobs at a time."""

        queue = list(todo)
        while True:
            if not ((queue and not self.stop_requested) or self._running):
                break
            usable = [slot for slot in range(self.options.workers) if slot not in self._retired]
            if queue and not usable and not self._running:
                self.log(f"every worker slot is retired; {len(queue)} job(s) not run: sweep the host, then resume")
                return
            free = [slot for slot in usable if slot not in self._running][: concurrency - len(self._running)]
            while queue and free and not self.stop_requested:
                self._start(queue.pop(0), free.pop(0))
            for slot, item in list(self._running.items()):
                code = item.process.poll()
                if code is None:
                    self._escalate(item)
                    continue
                del self._running[slot]
                self._finish(item, code)
                self._save_workers()
            time.sleep(POLL_S)

    def _start(self, job: dict[str, Any], slot: int) -> None:
        work = self._work(slot)
        shutil.rmtree(work, ignore_errors=True)
        for name in ("data", "tmp", "providers"):
            os.makedirs(os.path.join(work, name))
        run_dir = self._path("runs", job["id"])
        shutil.rmtree(run_dir, ignore_errors=True)
        os.makedirs(run_dir)
        low, _ = self.options.ports(slot)
        task = {
            "job": job,
            "report_dir": run_dir,
            "base_port": low,
            "loopback_block": self.options.loopback_block(slot),
            "data_dir": os.path.join(work, "data"),
            "artifact_dir": os.path.join(work, "providers"),
            "timeout_s": self.timeout_s,
            "cores": self.run_cores,
            "valkey_binary": self.options.valkey_binary,
            "experiment_directories": list(self.options.experiment_directories),
        }
        env = {**os.environ, "TMPDIR": os.path.join(work, "tmp"), "PYTHONUNBUFFERED": "1"}
        task_path = os.path.join(work, "task.json")
        write_document(task_path, task)
        with open(os.path.join(run_dir, CONSOLE_FILE), "w", encoding="utf-8") as console:
            process = subprocess.Popen(
                [*WORKER_COMMAND, task_path],
                stdout=console,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                env=env,
                start_new_session=True,
            )
        now = time.monotonic()
        self._running[slot] = _Running(
            job=job,
            slot=slot,
            process=process,
            owner=owner_of(process.pid).token,
            started=now,
            deadline=now + self.timeout_s + max(DEADLINE_GRACE_S, 0.25 * self.timeout_s),
            load_start=os.getloadavg()[0],
        )
        self._save_workers()

    def _escalate(self, item: _Running) -> None:
        now = time.monotonic()
        if not self.stop_requested and now < item.deadline:
            return
        if now >= item.deadline and not self.stop_requested:
            item.timed_out = True
        waits = (0.0, *STOP_GRACE_S)
        if item.signals_sent < len(waits) and now - item.last_signal >= waits[item.signals_sent]:
            sig = (signal.SIGINT, signal.SIGTERM, signal.SIGKILL)[item.signals_sent]
            if item.signals_sent == 0 and item.timed_out:
                self.log(f"{item.job['id']}: past its deadline; interrupting it")
            _signal_worker(item.process.pid, sig)
            item.signals_sent += 1
            item.last_signal = now

    def _finish(self, item: _Running, code: int) -> None:
        job, slot = item.job, item.slot
        run_dir = self._path("runs", job["id"])
        leaks = self._sweep(slot, item.owner)
        # After the sweep, which reports tagged servers as leaks: whatever untagged is left in the group goes too.
        _signal_worker(item.process.pid, signal.SIGKILL)
        logs = os.path.join(self._work(slot), "data", "logs")
        if os.path.isdir(logs):
            shutil.copytree(logs, os.path.join(run_dir, "logs"), dirs_exist_ok=True)
        self._retire_if_stuck(slot, job["id"], leaks)
        if slot not in self._retired:
            shutil.rmtree(self._work(slot), ignore_errors=True)
        if item.signals_sent and not item.timed_out:
            # Interrupted by the user: no result, so a resume runs it again.
            if leaks:
                self.log(f"{job['id']}: interrupted; {_swept(leaks)}")
            shutil.rmtree(run_dir, ignore_errors=True)
            return
        low, high = self.options.ports(slot)
        extra = {
            "worker": slot,
            # The worker's host-state tag: what the sweep looked for after the run.
            "owner": item.owner,
            "ports": [low, high],
            "loopback_block": self.options.loopback_block(slot),
            "duration_s": round(time.monotonic() - item.started, 1),
            "load": [round(item.load_start, 2), round(os.getloadavg()[0], 2)],
        }
        if job.get("origin"):
            extra["origin"] = job["origin"]
        result = job_result(job, run_dir, exit_code=code, timed_out=item.timed_out, leaks=leaks, extra=extra)
        write_document(self._path("results", f"{job['id']}.json"), result)
        if self.options.prune_pass and result["status"] == "PASS":
            _prune(run_dir)
        leaked = f" LEAKED {len(leaks)}" if leaks else ""
        self.log(
            f"[{result['status']:<16}] {job['id']} {extra['duration_s']}s w{slot}{leaked}: {result['summary'][:200]}"
        )


def _swept(leaks: Sequence[Mapping[str, Any]]) -> str:
    """How a sweep went: how many leftovers it removed, and how many it could not, and why."""

    failed = [leak["reason"] for leak in leaks if leak["action"] == "failed"]
    text = f"removed {sum(leak['action'] == 'removed' for leak in leaks)} leftover(s)"
    return text + (f", could not remove {len(failed)} ({'; '.join(failed)[:200]})" if failed else "")


def _signal_worker(pid: int, sig: int) -> None:
    """Send ``sig`` to a worker: SIGINT and SIGTERM to it alone, SIGKILL to its whole process group.

    The servers a run starts are in its worker's process group. The worker's run stops them; a signal of their
    own would have them shut down (saving an RDB, say) while the run tears them down.
    """

    try:
        if sig == signal.SIGKILL:
            os.killpg(pid, sig)
        else:
            os.kill(pid, sig)
    except (ProcessLookupError, PermissionError):
        pass
