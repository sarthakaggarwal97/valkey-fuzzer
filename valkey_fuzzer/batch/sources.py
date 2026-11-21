"""The built-in sources: ``random``, ``matrix``, ``scenarios``, and ``failures``."""

import copy
import itertools
import json
import os
import random
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Optional

from .._data import exception_type
from ..documents import SCENARIO_SECTIONS, set_path
from ..generator import Exclusion, GeneratorError, opted_in
from ..reports import SCENARIO_FILE, read_yaml_mapping
from ..schema import ConfigField, ConfigSchema
from ..service import RunService
from .hooks import BatchSource, plugin_job, register_source
from .jobs import Expansion, ids_per_signature, job_definition
from .results import results_of
from .spec import STATUS_ORDER, BatchError, BatchOptions, absolute, read_batch_file

# -- random -------------------------------------------------------------------


def _normalize_random(settings: dict[str, Any], base_dir: str, where: str) -> dict[str, Any]:
    experiments = settings["experiments"]
    if experiments != "all" and (
        not isinstance(experiments, list) or not experiments or not all(isinstance(e, str) for e in experiments)
    ):
        raise BatchError(f"{where}.experiments must be 'all' or a list of experiment IDs")
    try:
        settings["excludes"] = [str(Exclusion.parse(item)) for item in settings["excludes"] or []]
    except GeneratorError as exc:
        raise BatchError(f"{where}.excludes: {exc}") from None
    settings["base"] = settings["base"] or {}
    # A fresh first seed unless one is given; jobs.json records every seed.
    if settings.get("first_seed") is None:
        settings["first_seed"] = random.randrange(0, 2**31)
    settings.setdefault("seeds", None)
    return settings


def _random_jobs(
    value: Mapping[str, Any], index: int, service: RunService, options: BatchOptions, expansion: Expansion
) -> None:
    profile = expansion.profile
    excludes = [Exclusion.parse(item) for item in value["excludes"]]
    experiments = value["experiments"]
    if experiments == "all":
        experiments = opted_in(service.catalog, excludes)
    seeds = value["seeds"] or (profile.seeds if profile is not None and profile.seeds else 1)
    for experiment in experiments:
        service.catalog.experiments.resolve(experiment)
        for seed in range(value["first_seed"], value["first_seed"] + seeds):
            generate = {
                "seed": seed,
                "experiment": experiment,
                "excludes": value["excludes"],
                "base": value["base"],
                "profile": options.profile,
            }
            expansion.add(
                {
                    "id": f"random-{experiment}-s{seed}",
                    "source": "random",
                    "label": f"{experiment} seed {seed}",
                    "generate": generate,
                },
            )


register_source(
    BatchSource(
        "random",
        ConfigSchema(
            {
                "experiments": ConfigField((str, list)),
                "seeds": ConfigField(int, minimum=1, required=False, allow_none=True),
                "first_seed": ConfigField(int, minimum=0, required=False, allow_none=True),
                "excludes": ConfigField(list, items=ConfigField(str), default=list, allow_none=True),
                "base": ConfigField(dict, default=dict, allow_none=True),
            }
        ),
        _normalize_random,
        _random_jobs,
    )
)


# -- matrix -------------------------------------------------------------------


def _normalize_matrix(settings: dict[str, Any], base_dir: str, where: str) -> dict[str, Any]:
    if "experiment" not in settings["base"] or "deployment" not in settings["base"]:
        raise BatchError(f"{where}.base must be a scenario with experiment and deployment")
    for path, values in settings["dimensions"].items():
        if path.split(".")[0] not in SCENARIO_SECTIONS:
            raise BatchError(f"{where}.dimensions path {path!r} must start with one of {list(SCENARIO_SECTIONS)}")
        if not values:
            raise BatchError(f"{where}.dimensions.{path} needs a non-empty list of values")
    return settings


def _matrix_jobs(
    source: Mapping[str, Any], index: int, service: RunService, options: BatchOptions, expansion: Expansion
) -> None:
    base, dimensions = source["base"], list(source["dimensions"].items())
    name = base.get("scenario_id") or base["experiment"].get("id") or f"matrix{index}"
    rng = random.Random(source["seed"])
    combos = itertools.product(*[list(values) for _, values in dimensions]) if dimensions else [()]
    for combo_index, combo in enumerate(combos):
        assignment = {path: value for (path, _), value in zip(dimensions, combo)}
        for replica in range(source["seeds_per_case"]):
            scenario = copy.deepcopy(base)
            for path, value in assignment.items():
                set_path(scenario, path, copy.deepcopy(value))
            scenario["seed"] = rng.randrange(1, 2**31)
            scenario["scenario_id"] = f"{name}-{combo_index}-{replica}"
            try:
                service.compile(scenario)
            except ValueError as exc:
                # A combination the experiment does not support is reported, not run: every compile or config
                # error is a ValueError (:func:`~valkey_fuzzer.generator.generate`); anything else is a bug.
                expansion.invalid.append(
                    {
                        "case": scenario["scenario_id"],
                        "assignment": assignment,
                        "error": f"{exception_type(exc)}: {exc}",
                    }
                )
                continue
            label = json.dumps(assignment, sort_keys=True, default=str)
            expansion.add(
                {"id": f"matrix-{scenario['scenario_id']}", "source": "matrix", "label": label, "scenario": scenario},
            )


register_source(
    BatchSource(
        "matrix",
        ConfigSchema(
            {
                "base": ConfigField(dict),
                "dimensions": ConfigField(dict, default=dict, schema=ConfigSchema(additional=ConfigField(list))),
                "seeds_per_case": ConfigField(int, minimum=1, default=1),
                "seed": ConfigField(int, minimum=0, default=0),
            }
        ),
        _normalize_matrix,
        _matrix_jobs,
    )
)


# -- scenarios ----------------------------------------------------------------


def _scenario_paths(value: Any) -> Optional[dict[str, Any]]:
    """``scenarios: PATH`` or ``scenarios: [PATH, ...]`` for ``scenarios: {paths: [...]}``."""

    if isinstance(value, str):
        return {"paths": [value]}
    if isinstance(value, list):
        return {"paths": value}
    return None


def _normalize_scenarios(settings: dict[str, Any], base_dir: str, where: str) -> list[str]:
    if not settings["paths"]:
        raise BatchError(f"{where} must list scenario files or directories")
    return [absolute(path, base_dir, where) for path in settings["paths"]]


def _scenario_files(path: str) -> list[str]:
    if os.path.isdir(path):
        return sorted(str(item) for item in Path(path).iterdir() if item.suffix in (".yaml", ".yml"))
    if not os.path.isfile(path):
        raise BatchError(f"scenario file or directory not found: {path}")
    return [path]


def _scenario_jobs(
    value: Sequence[str], index: int, service: RunService, options: BatchOptions, expansion: Expansion
) -> None:
    for path in (item for entry in value for item in _scenario_files(entry)):
        try:
            scenario = read_yaml_mapping(path)
        except Exception as exc:
            raise BatchError(f"cannot read the scenario {path}: {exc}") from None
        expansion.add(
            {
                "id": f"scenario-{Path(path).stem}",
                "source": "scenarios",
                "label": path,
                "scenario": json.loads(json.dumps(scenario, default=str)),
            },
        )


register_source(
    BatchSource(
        "scenarios",
        ConfigSchema({"paths": ConfigField(list, items=ConfigField(str))}),
        _normalize_scenarios,
        _scenario_jobs,
        shorthand=_scenario_paths,
    )
)


# -- failures -----------------------------------------------------------------


def _normalize_failures(settings: dict[str, Any], base_dir: str, where: str) -> dict[str, Any]:
    settings["batch"] = absolute(settings["batch"], base_dir, f"{where}.batch")
    return settings


def failure_samples(
    folder: str, *, statuses: Sequence[str] = ("FAIL", "ERROR"), per_signature: Optional[int] = None
) -> list[dict[str, Any]]:
    """The jobs of the batch in ``folder`` whose results have one of ``statuses``, up to ``per_signature`` each.

    A random job comes back as the scenario it drew (its run's ``scenario.yaml``): its seed would draw another
    scenario once the code changed, and a re-run after a fix must run what failed. One that never drew a
    scenario draws again. A plug-in's runs (the zoo's) are left out: their failures are expected.
    """

    jobs = {job["id"]: job for job in read_batch_file(os.path.join(folder, "jobs.json"))}
    picked = ids_per_signature(
        results_of(folder).values(),
        lambda result: result["status"] in statuses and result["id"] in jobs and not plugin_job(jobs[result["id"]]),
        per_signature,
    )
    samples = []
    for job_id in sorted(picked):
        job = job_definition(jobs[job_id])
        drawn = os.path.join(folder, "runs", job_id, SCENARIO_FILE)
        if "generate" in job and os.path.exists(drawn):
            del job["generate"]
            job["scenario"] = read_yaml_mapping(drawn)
        samples.append(job)
    return samples


def _failure_jobs(
    value: Mapping[str, Any], index: int, service: RunService, options: BatchOptions, expansion: Expansion
) -> None:
    for job in failure_samples(value["batch"], statuses=value["statuses"], per_signature=value["per_signature"]):
        expansion.add(
            {
                **job,
                "id": f"rerun-{job['id']}",
                "source": "failures",
                "origin": {"batch": value["batch"], "job": job["id"]},
            },
        )


register_source(
    BatchSource(
        "failures",
        ConfigSchema(
            {
                "batch": ConfigField(str),
                "statuses": ConfigField(
                    list, items=ConfigField(str, choices=STATUS_ORDER[:-1]), default=lambda: ["FAIL", "ERROR"]
                ),
                "per_signature": ConfigField(int, minimum=1, default=None, allow_none=True),
            }
        ),
        _normalize_failures,
        _failure_jobs,
        shorthand=lambda value: {"batch": value} if isinstance(value, str) else None,
    )
)
