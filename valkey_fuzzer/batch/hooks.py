"""The batch's plug-in points: the kinds of source a batch accepts, and the steps around a run other than running it.

A :class:`BatchSource` is one kind of entry in a batch's ``sources``: its schema, how its settings are normalized
(defaults filled in, paths made absolute), and how they expand to jobs. The built-in kinds (``random``, ``matrix``,
``scenarios``, ``failures``) are registered by :mod:`.sources`; a plug-in may bring its own (the bug zoo does).

A :class:`BatchPlugin` takes part at fixed points of a batch and nowhere else: in the worker after the run
(:meth:`~BatchPlugin.after_run`), when a finished job's verdict is formed (:meth:`~BatchPlugin.judge`), when its
result is recorded (:meth:`~BatchPlugin.record`), and in the summary (:meth:`~BatchPlugin.summarize`). A plug-in
with a source also records what that source expanded to, in ``batch.json`` under its name. A job such a source
adds carries the plug-in's name as a key (the plug-in's own record of the job) and is the plug-in's to judge: the
batch neither counts it, groups it by failure signature, nor runs it again. The batch loop knows no plug-in by name;
:mod:`valkey_fuzzer.batch` registers the bug zoo and spec conformance.
"""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

from ..schema import ConfigSchema

if TYPE_CHECKING:
    from ..service import RunService
    from .jobs import Expansion
    from .pool import BatchRunner
    from .results import Verdict
    from .spec import BatchOptions


@dataclass(frozen=True)
class BatchSource:
    """One kind of batch source.

    ``normalize(settings, base_dir, where)`` returns the source's value as a batch spec records it, from its
    resolved settings; ``expand(value, index, service, options, expansion)`` adds that value's jobs to
    ``expansion``; ``shorthand(value)`` gives the mapping a one-value form stands for, None for no shorthand.
    """

    kind: str
    schema: ConfigSchema
    normalize: Callable[[dict[str, Any], str, str], Any]
    expand: Callable[[Any, int, "RunService", "BatchOptions", "Expansion"], None]
    shorthand: Optional[Callable[[Any], Optional[dict[str, Any]]]] = None


class BatchPlugin:
    """A step of the batch other than running its jobs; override what the step needs.

    ``name`` keys the plug-in's record in ``batch.json`` (a plug-in with a ``source``), its field of a job's
    result, and its entry of the summary. ``kept_when_pruned`` names the files of a run folder it writes that a
    pruned PASS run keeps.
    """

    name = ""
    source: Optional[BatchSource] = None
    kept_when_pruned: tuple[str, ...] = ()

    def after_run(self, report_dir: str, task: Mapping[str, Any]) -> None:
        """In the worker, after its run, in the run's report folder; ``task`` is the worker's task (the job and
        the batch options the run ran under)."""

    def judge(
        self, job: Mapping[str, Any], run_dir: str, report: Optional[Mapping[str, Any]], verdict: "Verdict"
    ) -> Any:
        """Amend a finished job's ``verdict`` before its leaks count; the result's field under ``name``, or None.

        ``report`` is the run's ``report.json`` (None when the run left none).
        """

        return None

    def record(
        self, job: Mapping[str, Any], run_dir: str, report: Optional[Mapping[str, Any]], result: Mapping[str, Any]
    ) -> Any:
        """The result's field under ``name``, from the finished job's ``result`` (its status final), or None."""

        return None

    def summarize(
        self, runner: "BatchRunner", results: Mapping[str, Mapping[str, Any]], main: Sequence[Mapping[str, Any]]
    ) -> tuple[Any, list[str]]:
        """The summary's entry under ``name``, and the statuses it adds to the batch's.

        ``results`` is every finished job's result by ID; ``main`` the results of the jobs the batch counts.
        """

        return None, []


SOURCES: dict[str, BatchSource] = {}
PLUGINS: list[BatchPlugin] = []


def register_source(source: BatchSource) -> None:
    if source.kind in SOURCES:
        raise ValueError(f"a batch source {source.kind!r} is already registered")
    SOURCES[source.kind] = source


def register(plugin: BatchPlugin) -> None:
    """Register ``plugin`` (and its source, if any); plug-ins take part in registration order."""

    if not plugin.name:
        raise ValueError(f"{type(plugin).__name__} needs a name")
    if any(plugin.name == registered.name for registered in PLUGINS):
        raise ValueError(f"a batch plug-in {plugin.name!r} is already registered")
    if plugin.source is not None:
        register_source(plugin.source)
    PLUGINS.append(plugin)


def plugin_job(job: Mapping[str, Any]) -> bool:
    """Whether ``job`` is a plug-in's to judge (it carries a plug-in's name)."""

    return any(plugin.name in job for plugin in PLUGINS)


def kept_when_pruned() -> tuple[str, ...]:
    """Every file the plug-ins keep in a pruned PASS run."""

    return tuple(name for plugin in PLUGINS for name in plugin.kept_when_pruned)
