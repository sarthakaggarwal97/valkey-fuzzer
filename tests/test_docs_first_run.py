"""The first-run docs match the tool: every ``valkey-fuzzer`` command in the README and the getting-started page
parses with the real parser, both pages state the Go prerequisite, and the experiments they say need
``porcupine`` are the ones that extend the ``valkey.history`` fragment.
"""

import re
import shlex
from pathlib import Path

import pytest
import yaml

from valkey_fuzzer.cli import create_parser

ROOT = Path(__file__).resolve().parents[1]
PAGES = [ROOT / "README.md", ROOT / "docs" / "getting-started.md", ROOT / "docs" / "reading-a-fail.md"]
EXPERIMENTS = ROOT / "valkey_fuzzer" / "components" / "experiments"


def _commands(page: Path) -> list[str]:
    """The ``valkey-fuzzer`` commands of the page's ``bash`` blocks, continuation lines joined."""

    commands = []
    for block in re.findall(r"```bash\n(.*?)```", page.read_text(encoding="utf-8"), flags=re.S):
        for line in block.replace("\\\n", " ").splitlines():
            if line.startswith("valkey-fuzzer "):
                commands.append(line)
    return commands


def _documented_commands() -> list[tuple[str, str]]:
    return [(page.name, command) for page in PAGES for command in _commands(page)]


def test_the_pages_have_commands_to_check():
    assert len(_documented_commands()) >= 15


@pytest.mark.parametrize(("page", "command"), _documented_commands())
def test_a_documented_command_parses(page, command):
    words = shlex.split(command.split(" #", 1)[0])[1:]
    if "BUILDS" in words or "RUN" in words:
        pytest.skip("a placeholder")
    try:
        create_parser().parse_args(words)
    except SystemExit as exit:  # --version and --help exit 0 after printing; a bad command exits 2
        assert exit.code == 0, command


@pytest.mark.parametrize("page", [ROOT / "README.md", ROOT / "docs" / "getting-started.md"])
def test_the_page_states_go_and_the_build_command(page):
    text = page.read_text(encoding="utf-8")

    assert "Go 1.21" in text
    assert 'go -C porcupine build -o "$HOME/.local/bin/porcupine"' in text
    assert "GOPROXY=direct GOSUMDB=off" in text


def test_the_experiments_the_pages_say_need_porcupine_are_the_ones_that_do():
    needing = set()
    for path in EXPERIMENTS.glob("*.yaml"):
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        if "valkey.history" in (document.get("extends") or []):
            needing.add(f"valkey.{path.stem.removeprefix('valkey.')}")
    assert len(needing) == 6
    for page in (ROOT / "README.md", ROOT / "docs" / "getting-started.md"):
        text = page.read_text(encoding="utf-8")
        assert all(f"`{name}`" in text for name in needing), (page.name, needing)


def test_the_experiments_the_pages_say_never_build_valkey_are_the_ones_that_probe():
    probing = {
        f"valkey.{path.stem.removeprefix('valkey.')}"
        for path in EXPERIMENTS.glob("*.yaml")
        if "valkey.capability.build-probe" in path.read_text(encoding="utf-8")
    }
    assert len(probing) == 4
    for page in (ROOT / "README.md", ROOT / "docs" / "getting-started.md"):
        text = page.read_text(encoding="utf-8")
        assert all(f"`{name}`" in text for name in probing), (page.name, probing)
