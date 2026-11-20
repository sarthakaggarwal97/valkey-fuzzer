"""What a stranger hits first: example flags, error hints, seeds that depend on cores, and a smoke verdict
(``--base-port``, "did you mean", ``validate --strict``, ``replay`` of a scenario)."""

import json
import re
import shlex
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from tests.conftest import cli as _cli
from tests.test_batch import _batch, _random, stub, workers  # noqa: F401  (the batch tests' fixtures)
from tests.valkey_experiments_support import shared_catalog, valkey_service
from valkey_fuzzer.batch import normalize_source
from valkey_fuzzer.cli import create_parser
from valkey_fuzzer.deployment.local.config import deployment_config_from_document
from valkey_fuzzer.deployment.local.planner import plan_deployment
from valkey_fuzzer.fields import did_you_mean, strict_fields
from valkey_fuzzer.outcomes import EXIT_USAGE

ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = ROOT / "examples"
EXPERIMENTS = EXAMPLES / "experiments"

pytestmark = pytest.mark.usefixtures("report_root")


def _ports(plan):
    """Every client and bus port the plan's deployment starts."""

    instances = plan_deployment(deployment_config_from_document(plan.deployment))
    return sorted({port for item in instances for port in (item.port, item.bus_port)})


# ---------------------------------------------------------------------------
# U-04 --base-port, the examples' flags
# ---------------------------------------------------------------------------


def test_base_port_moves_every_port_of_a_scenario_file_and_of_a_generated_run(canned_runs):
    cluster = str(EXAMPLES / "valkey" / "cluster_chaos.yaml")
    assert _cli("run", cluster) == 0
    assert _cli("run", cluster, "--base-port", 9100) == 0
    assert _cli("run", "--seed", 3, "--experiment", "valkey.cluster-chaos") == 0
    assert _cli("run", "--seed", 3, "--experiment", "valkey.cluster-chaos", "--base-port", 9100) == 0
    kept, moved, drawn, drawn_moved = canned_runs.plans
    assert moved.deployment["base_port"] == drawn_moved.deployment["base_port"] == 9100
    count = len(moved.instances)
    assert _ports(moved) == [*range(9100, 9100 + count), *range(19100, 19100 + count)]
    assert _ports(drawn_moved) == _ports(moved) or len(drawn_moved.instances) != count
    assert _ports(kept) != _ports(moved)
    # The same seed still draws the same scenario, whatever its port.
    drawn_ports = {key: value for key, value in drawn.deployment.items() if key != "base_port"}
    assert drawn_ports == {key: value for key, value in drawn_moved.deployment.items() if key != "base_port"}


def test_the_reproduce_line_keeps_the_base_port(canned_runs, capsys):
    assert _cli("run", "--seed", 5, "--experiment", "valkey.aof-durability", "--base-port", 9200) == 0
    (line,) = [line for line in capsys.readouterr().out.splitlines() if line.startswith("Reproduce: ")]
    assert shlex.split(line[len("Reproduce: ") :])[-2:] == ["--base-port", "9200"]


@pytest.mark.parametrize("value", ["0", "65536", "many"])
def test_base_port_must_be_a_port(canned_runs, capsys, value):
    with pytest.raises(SystemExit) as raised:
        _cli("run", EXAMPLES / "valkey" / "aof_durability.yaml", "--base-port", value)
    assert raised.value.code == EXIT_USAGE and "port number from 1 to 65535" in capsys.readouterr().err


def test_batch_takes_base_port_as_its_port_base():
    args = create_parser().parse_args(["batch", "--random", "all", "--base-port", "12000"])
    assert args.port_base == 12000


def _headers(path):
    """The ``valkey-fuzzer`` command an example's leading comment documents, as its arguments (None if none): an
    indented comment line that starts with it, and the lines a trailing backslash continues it over."""

    command = None
    for line in path.read_text().splitlines():
        if not line.startswith("#"):
            break
        text = line[1:].strip()
        if command is None and re.match(r"^#\s{2,}valkey-fuzzer ", line):
            command = text
        elif command is not None and command.endswith("\\"):
            command += " " + text
        if command is not None and not command.endswith("\\"):
            break
    return shlex.split(command.replace("\\", " "))[1:] if command else None


SINGLE_SCENARIOS = [
    path
    for path in sorted([*EXAMPLES.glob("*_scenario.yaml"), *(EXAMPLES / "valkey").glob("*.yaml")])
    if "sources" not in path.read_text()
]


@pytest.mark.parametrize("path", SINGLE_SCENARIOS, ids=lambda path: str(path.relative_to(EXAMPLES)))
def test_every_example_validates_with_the_flags_its_header_documents(path):
    # A header's relative paths are the repository's.
    documented = _headers(path) or []
    directories = [ROOT / documented[index + 1] for index, item in enumerate(documented) if item == "--experiment-dir"]
    service = valkey_service(experiment_directories=[str(item) for item in directories])
    scenario = yaml.safe_load(path.read_text())
    service.validate_plan(service.compile(scenario))
    needs = scenario["experiment"]["id"].startswith("example.")
    assert bool(directories) == needs, f"{path.name} {'needs' if needs else 'does not need'} --experiment-dir"


def test_every_documented_example_command_parses():
    parser = create_parser()
    for path in [*EXAMPLES.glob("*.yaml"), *(EXAMPLES / "valkey").glob("*.yaml")]:
        argv = _headers(path)
        if argv and len(argv) > 1 and (ROOT / argv[1]).exists():
            assert parser.parse_args(argv).command == argv[0]


# ---------------------------------------------------------------------------
# U-06 hints and wrong-file errors
# ---------------------------------------------------------------------------


def test_did_you_mean_offers_at_most_three_close_names():
    assert did_you_mean("zzzz", ["alpha", "beta"]) == ""
    assert did_you_mean("cmds", ["commands", "cmd", 7]) == "did you mean 'cmd', 'commands'?"
    names = [f"name-{item}" for item in "abcde"]
    assert did_you_mean("name-x", names).count("'") == 6


def test_an_unknown_field_names_the_close_field_and_lists_the_valid_ones():
    with pytest.raises(ValueError) as raised:
        strict_fields({"cmds": 1}, "action", allowed=["commands", "timeout_s"])
    message = str(raised.value)
    assert "contains unknown fields: ['cmds']" in message
    assert "'cmds': did you mean 'commands'?" in message
    assert "valid fields: ['commands', 'timeout_s']" in message


def test_a_mistyped_component_or_experiment_names_the_closest_ones(tmp_path):
    service = valkey_service()
    scenario = yaml.safe_load((EXAMPLES / "valkey" / "aof_durability.yaml").read_text())

    with pytest.raises(ValueError, match=r"not registered; did you mean 'valkey.aof-durability',"):
        service.compile({**scenario, "experiment": {**scenario["experiment"], "id": "valkey.aof-durabilty"}})

    experiment = {
        "id": "trial.typo",
        "deployment": {"modes": ["standalone"]},
        "components": [{"use": "valkey.action.command-seq", "id": "go"}],
    }
    directory = tmp_path / "experiments"
    directory.mkdir()
    (directory / "trial.yaml").write_text(yaml.safe_dump(experiment))
    typo = valkey_service(experiment_directories=[str(directory)])
    with pytest.raises(ValueError, match=r"'valkey.action.command-seq'.*not registered; did you mean '"):
        typo.compile(
            {"scenario_id": "t", "seed": 1, "experiment": {"id": "trial.typo"}, "deployment": {"mode": "standalone"}}
        )


def test_replaying_a_scenario_says_to_use_run(tmp_path, canned_runs, capsys):
    scenario = EXAMPLES / "valkey" / "aof_durability.yaml"
    assert _cli("replay", scenario) == EXIT_USAGE
    error = capsys.readouterr().err
    assert "is a scenario, not a run report or plan" in error and f"valkey-fuzzer run {scenario}" in error
    assert not canned_runs.plans


def _strict_setup(tmp_path, *, required):
    """A scenario of a copy of the shipped replica-kill experiment, with or without its own required evidence."""

    directory = tmp_path / "experiments"
    directory.mkdir(exist_ok=True)
    experiment = yaml.safe_load((EXAMPLES / "valkey" / "experiments" / "replica-killed-under-writes.yaml").read_text())
    experiment["id"] = "trial.replica-kill"
    if not required:
        del experiment["required_events"], experiment["required_evidence"]
    (directory / "trial.yaml").write_text(yaml.safe_dump(experiment))
    scenario = tmp_path / "trial.yaml"
    scenario.write_text(
        yaml.safe_dump(
            {
                "scenario_id": "trial",
                "seed": 1,
                "experiment": {"id": "trial.replica-kill"},
                "deployment": {"mode": "standalone", "replicas_per_primary": 1},
            }
        )
    )
    return directory, scenario


def test_validate_strict_warns_when_only_the_base_requires_evidence(tmp_path, capsys):
    directory, scenario = _strict_setup(tmp_path, required=False)
    assert _cli("validate", scenario, "--experiment-dir", directory) == 0
    assert "Warning" not in capsys.readouterr().err

    assert _cli("validate", scenario, "--experiment-dir", directory, "--strict") == 0
    warning = capsys.readouterr().err
    assert "Warning: no component of the experiment's own is required evidence" in warning
    assert "'kill-replica'" in warning

    assert _cli("validate", scenario, "--experiment-dir", directory, "--strict", "--format", "json") == 0
    (row,) = json.loads(capsys.readouterr().out)["files"]
    assert len(row["warnings"]) == 1


def test_validate_strict_is_quiet_when_an_own_component_is_required_evidence(tmp_path, capsys):
    directory, scenario = _strict_setup(tmp_path, required=True)
    assert _cli("validate", scenario, "--experiment-dir", directory, "--strict") == 0
    assert "Warning" not in capsys.readouterr().err


def test_validate_strict_is_quiet_for_a_shipped_experiment(capsys):
    assert _cli("validate", EXAMPLES / "valkey" / "aof_durability.yaml", "--strict") == 0
    assert "Warning" not in capsys.readouterr().err


# ---------------------------------------------------------------------------
# U-08 the seed depends on the cores
# ---------------------------------------------------------------------------


def test_a_seed_draws_another_scenario_at_other_cores_and_the_run_header_says_so(canned_runs, capsys):
    # Pins docs/contracts.md (Seeds): a seed is stable per (profile, cores), not across hosts.
    anywhere = valkey_service().generate(0, experiment="valkey.cluster-chaos")
    squeezed = valkey_service(cores=2).generate(0, experiment="valkey.cluster-chaos")
    assert anywhere.attempt == 0 and squeezed.attempt > 0 and anywhere.scenario != squeezed.scenario
    assert valkey_service(cores=2).generate(0, experiment="valkey.cluster-chaos").scenario == squeezed.scenario
    assert (anywhere.provenance["cores"], squeezed.provenance["cores"]) == (None, 2)

    with patch("valkey_fuzzer.commands._common.usable_cores", return_value=2.0):
        assert _cli("run", "--seed", 0, "--experiment", "valkey.cluster-chaos") == 0
    header = capsys.readouterr().out
    assert f"seed 0 (attempt {squeezed.attempt}, cores 2)" in header


# ---------------------------------------------------------------------------
# U-10 smoke is not an experiment's PASS
# ---------------------------------------------------------------------------


@pytest.fixture
def smoke_runs(canned_runs, monkeypatch):
    """Canned runs on a catalog that also holds the shipped smoke experiment."""

    catalog = shared_catalog([str(EXPERIMENTS)])
    canned_runs.catalog = catalog
    monkeypatch.setattr("valkey_fuzzer.service.build_catalog", lambda **options: catalog)
    return canned_runs


def test_a_smoke_run_prints_its_mode_beside_its_status(smoke_runs, capsys):
    scenario = EXAMPLES / "standalone_scenario.yaml"
    assert _cli("run", scenario, "--experiment-dir", EXPERIMENTS) == 0
    out = capsys.readouterr().out
    assert "Status: PASS (mode: smoke; no coverage claim)" in out

    assert _cli("run", EXAMPLES / "valkey" / "aof_durability.yaml") == 0
    out = capsys.readouterr().out
    assert "Status: PASS\n" in out and "mode: smoke" not in out


def test_a_batch_counts_smoke_runs_apart_and_prints_them(tmp_path, workers, stub, capsys):  # noqa: F811
    sources = [normalize_source({"scenarios": [str(EXAMPLES / "standalone_scenario.yaml")]}, "/")]
    runner = _batch(tmp_path, sources, experiment_directories=(str(EXPERIMENTS),))
    summary = runner.run()

    assert summary["counts"] == {} and summary["smoke"] == {"PASS": 1}
    assert summary["runs"][0]["mode"] == "smoke" and summary["cores"] > 0
    from valkey_fuzzer.commands.batch import _print_summary

    _print_summary(summary)
    out = capsys.readouterr().out
    assert "Smoke runs (lifecycle only, no coverage claim): {'PASS': 1}" in out
    assert "Cores per run:" in out


def test_a_batch_summary_names_the_seed_and_attempt_of_a_failure(tmp_path, workers, stub, capsys):  # noqa: F811
    stub.behave(**{"0": "fail:data"})
    runner = _batch(tmp_path, [_random(1)])
    summary = runner.run()
    (run,) = summary["runs"]
    assert run["seed"] == 0 and run["attempt"] is not None and run["mode"] == "experiment"
    from valkey_fuzzer.commands.batch import _print_summary

    _print_summary(summary)
    assert f"(seed 0, attempt {run['attempt']})" in capsys.readouterr().out
