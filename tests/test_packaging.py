"""Spec bundles ship in the package (``formal/`` is mapped to ``valkey_fuzzer/formal``) and are found by name; the
version has one source; the sdist carries the checkout and the wheel the data the runtime reads."""

import os
import re
import shutil
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

import pytest
from setuptools import find_packages

from valkey_fuzzer import __version__
from valkey_fuzzer.cli import create_parser
from valkey_fuzzer.spec.bundle import SpecBundleError, find_bundle

ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = (ROOT / "pyproject.toml").read_text(encoding="utf-8")


def test_pyproject_lists_every_package_and_the_bundles():
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    block = re.search(r"^\[tool\.setuptools\]\npackages = \[(.*?)\]", text, flags=re.MULTILINE | re.DOTALL)
    listed = sorted(re.findall(r'"([^"]+)"', block.group(1)))
    assert listed == sorted([*find_packages(where=str(ROOT), include=["valkey_fuzzer*"]), "valkey_fuzzer.formal"])


def test_the_version_has_one_source_and_the_cli_prints_it(capsys):
    # A release is `v<__version__>` (docs/releasing.md), so a hand-edited pyproject version would ship a wheel the
    # tag does not name; setuptools reads the attribute instead.
    assert re.search(r'^dynamic = \["version"\]$', PYPROJECT, re.M)
    assert re.search(r'^version = \{attr = "valkey_fuzzer\.__version__"\}$', PYPROJECT, re.M)
    assert not re.search(r'^version = "', PYPROJECT, re.M)
    assert re.fullmatch(r"\d+\.\d+\.\d+", __version__)
    with pytest.raises(SystemExit, match="0"):
        create_parser().parse_args(["--version"])
    assert capsys.readouterr().out == f"Valkey Fuzzer {__version__}\n"


def test_the_license_is_an_spdx_expression():
    # PEP 639: the `license = {text = ...}` table is deprecated and setuptools refuses it from 2027-02-18; the
    # string needs setuptools 77, which the build-system pins and the dev extra installs (the tests build without
    # isolation).
    assert re.search(r'^license = "BSD-3-Clause"$', PYPROJECT, re.M)
    assert re.search(r'^license-files = \["LICENSE"\]$', PYPROJECT, re.M)
    assert "license = {" not in PYPROJECT
    assert re.search(r'^requires = \["setuptools>=77"\]$', PYPROJECT, re.M)
    assert re.search(r'^    "setuptools>=77",$', PYPROJECT, re.M)


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    """The sdist and the wheel of this checkout, as `python -m build` makes them (without isolation: the backend
    is the installed setuptools, which the dev extra pins at the build-system's version)."""

    out = tmp_path_factory.mktemp("dist")
    command = [sys.executable, "-m", "build", "--no-isolation", "--outdir", str(out)]
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert "deprecated" not in result.stdout + result.stderr, result.stdout + result.stderr
    (sdist,) = out.glob("*.tar.gz")
    (wheel,) = out.glob("*.whl")
    return sdist, wheel


def _tracked(*paths):
    """The files git tracks under ``paths``, less the .gitignore files (the sdist ships none)."""

    try:
        listed = subprocess.run(
            ["git", "ls-files", "-z", "--", *paths], cwd=ROOT, capture_output=True, text=True, check=True
        ).stdout
    except (OSError, subprocess.CalledProcessError):
        pytest.skip("not a git checkout: the sdist's expected files are the tracked ones")
    return {path for path in listed.split("\0") if path and not path.endswith(".gitignore")}


def test_the_sdist_carries_the_checkout(built):
    sdist, _ = built
    assert sdist.name == f"valkey_fuzzer-{__version__}.tar.gz"
    with tarfile.open(sdist) as archive:
        names = {member.name.partition("/")[2] for member in archive.getmembers() if member.isfile()}
        metadata = archive.extractfile(f"valkey_fuzzer-{__version__}/PKG-INFO").read().decode()
    # The runtime, the tests and what they read (.github for tests/test_ci_workflows.py), the porcupine checker's
    # sources, the bug zoo, the examples and the docs: `pytest`, `zoo/build.sh` and `go -C porcupine build` work
    # from an unpacked sdist as from a clone.
    roots = ("valkey_fuzzer", "formal", "tests", ".github", "porcupine", "zoo", "examples", "docs")
    files = ("CHANGELOG.md", "LICENSE", "README.md", "pyproject.toml", "MANIFEST.in")
    missing = _tracked(*roots, *files) - names
    assert not missing, sorted(missing)
    # And nothing untracked beyond the metadata setuptools writes: not the __pycache__ a test run leaves under
    # tests/, which `graft tests` would otherwise pick up.
    generated = {"PKG-INFO", "setup.cfg"} | {name for name in names if name.startswith("valkey_fuzzer.egg-info/")}
    extra = names - _tracked() - generated
    assert not extra, sorted(extra)
    assert f"\nVersion: {__version__}\n" in metadata
    assert "\nLicense-Expression: BSD-3-Clause\n" in metadata and "\nLicense-File: LICENSE\n" in metadata


def test_the_wheel_ships_the_data_the_runtime_reads(built):
    _, wheel = built
    assert wheel.name == f"valkey_fuzzer-{__version__}-py3-none-any.whl"
    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
        metadata = archive.read(f"valkey_fuzzer-{__version__}.dist-info/METADATA").decode()
    # The experiments, fragments and reply schemas the catalog loads, the syscall fault library's source, and
    # every spec bundle file (formal/ mapped to valkey_fuzzer/formal): a shipped experiment that names a bundle
    # must find it in any install.
    package = ROOT / "valkey_fuzzer"
    data = {
        path.relative_to(ROOT).as_posix()
        for path in package.rglob("*")
        if path.is_file() and path.suffix != ".py" and "__pycache__" not in path.parts
    }
    bundles = {f"valkey_fuzzer/{path}" for path in _tracked("formal")}
    assert data and bundles
    missing = (data | bundles) - names
    assert not missing, sorted(missing)
    # And nothing of the checkout beyond the package: tests, docs, zoo and examples are the sdist's.
    outside = {
        name for name in names if not name.startswith(("valkey_fuzzer/", f"valkey_fuzzer-{__version__}.dist-info/"))
    }
    assert not outside, sorted(outside)
    assert "\nLicense-Expression: BSD-3-Clause\n" in metadata


@pytest.mark.slow  # pip fetches the dependencies: 5 s with a warm cache, longer without.
def test_the_installed_wheel_runs_in_a_clean_venv(built, tmp_path, capsys):
    # Installed from the wheel with its dependencies, away from the checkout: the console script, the version, the
    # catalog (every shipped experiment, as this checkout lists them) and a spec bundle found inside the install.
    _, wheel = built
    venv = tmp_path / "venv"
    subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True, capture_output=True)
    pip = [str(venv / "bin" / "python"), "-m", "pip", "--quiet", "--disable-pip-version-check"]
    subprocess.run([*pip, "install", str(wheel)], cwd=tmp_path, check=True, capture_output=True, text=True)
    args = create_parser().parse_args(["list", "experiments"])
    assert args.handler(args) == 0
    expected = capsys.readouterr().out

    def run(*command):
        result = subprocess.run(command, cwd=tmp_path, capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
        return result.stdout

    script = str(venv / "bin" / "valkey-fuzzer")
    assert run(script, "--version") == f"Valkey Fuzzer {__version__}\n"
    assert run(script, "list", "experiments") == expected
    located = run(
        str(venv / "bin" / "python"),
        "-c",
        "import valkey_fuzzer\nfrom valkey_fuzzer.spec.bundle import find_bundle\n"
        "print(valkey_fuzzer.__file__)\nprint(find_bundle('cluster-election'))",
    )
    module, bundle = (Path(line) for line in located.splitlines())
    assert module.is_relative_to(venv) and module.parts[-3:] == ("site-packages", "valkey_fuzzer", "__init__.py")
    assert bundle.is_relative_to(venv) and (bundle / "model.tla").is_file()


def test_the_data_files_of_the_components_ship_with_them():
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    shipped = re.search(r'^"valkey_fuzzer\.components" = \[(.*)\]$', text, flags=re.MULTILINE).group(1)
    patterns = re.findall(r'"([^"]+)"', shipped)
    components = ROOT / "valkey_fuzzer" / "components"
    data = [path for path in components.rglob("*") if path.is_file() and path.suffix in (".yaml", ".json")]
    assert len(data) > 20
    for path in data:
        assert any(path.match(pattern) for pattern in patterns), path


def test_the_syscall_fault_library_source_ships_with_the_deployment():
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    shipped = re.search(r'^"valkey_fuzzer\.deployment\.local" = \[(.*)\]$', text, flags=re.MULTILINE).group(1)
    sources = [path.name for path in (ROOT / "valkey_fuzzer" / "deployment" / "local").glob("*.c")]
    assert sources == ["syscall_faults.c"] and re.findall(r'"([^"]+)"', shipped) == sources


def test_bundles_are_found_by_directory_or_by_protocol_name_on_the_spec_path(tmp_path, monkeypatch):
    fixtures = ROOT / "tests" / "fixtures" / "spec"
    monkeypatch.delenv("VALKEY_FUZZER_SPEC_PATH", raising=False)
    assert find_bundle(str(fixtures / "register")) == fixtures / "register"
    assert find_bundle("cluster-election") == ROOT / "formal" / "cluster-election"
    with pytest.raises(SpecBundleError, match=r"no spec bundle named 'register'"):
        find_bundle("register")
    with pytest.raises(SpecBundleError, match="no spec bundle at"):
        find_bundle(str(tmp_path / "missing"))
    # The search path comes before the shipped bundles.
    shutil.copytree(fixtures / "register", tmp_path / "cluster-election")
    monkeypatch.setenv("VALKEY_FUZZER_SPEC_PATH", f"{tmp_path}{os.pathsep}{fixtures}")
    assert find_bundle("register") == fixtures / "register"
    assert find_bundle("cluster-election") == tmp_path / "cluster-election"
    # A protocol name is never a directory of the current one; a path is.
    monkeypatch.delenv("VALKEY_FUZZER_SPEC_PATH")
    monkeypatch.chdir(tmp_path)
    assert find_bundle("cluster-election") == ROOT / "formal" / "cluster-election"
    assert find_bundle("./cluster-election") == tmp_path / "cluster-election"


def test_a_base_install_needs_java_and_the_jar_only_when_a_spec_tool_runs(tmp_path):
    # With no java on PATH and no jar, the CLI builds, the batch package loads, and a scenario naming a bundle
    # compiles (the spec bridge imports plainly; only SANY and TLC need the tools). A spec command then exits 2
    # and says what is missing and how to get the jar.
    loaded = """
import sys
from valkey_fuzzer import batch
from valkey_fuzzer.cli import create_parser
from valkey_fuzzer.spec.tla import TOOLS_HINT
import yaml
from tests.valkey_experiments_support import valkey_service
parser = create_parser()
with open("examples/valkey/cluster_election_durability.yaml") as handle:
    plan = valkey_service().compile(yaml.safe_load(handle))
assert any(component.config.get("spec") == "cluster-election" for component in plan.components)
args = parser.parse_args(["spec", "check", "cluster-election"])
assert args.handler(args) == 2
print(TOOLS_HINT)
"""
    environment = {key: value for key, value in os.environ.items() if key != "TLA2TOOLS_JAR"}
    environment["PATH"] = str(tmp_path)  # a directory without java
    result = subprocess.run(
        [sys.executable, "-c", loaded], cwd=ROOT, env=environment, capture_output=True, text=True, check=True
    )
    hint = result.stdout.strip()
    assert result.stderr.strip() == f"Error: tla2tools.jar not found and java not found on PATH: {hint}"
