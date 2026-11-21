"""The first-run docs match the tool: every ``valkey-fuzzer`` command in the README and the getting-started page
parses with the real parser, and the experiments the pages say never build Valkey are the ones that probe for it.
"""

import re
import shlex
from pathlib import Path

import pytest

from tests.support import ROOT
from valkey_fuzzer.cli import create_parser
from valkey_fuzzer.components import experiments

PAGES = [ROOT / "README.md", ROOT / "docs" / "getting-started.md", ROOT / "docs" / "reading-a-fail.md"]


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


def test_the_experiments_the_pages_say_never_build_valkey_are_the_ones_that_probe():
    # The resolved experiments: the election experiments' probe comes from the valkey.election-steps fragment.
    probing = {
        experiment.experiment_id
        for experiment in experiments()
        if any(component.use == "valkey.capability.build-probe" for component in experiment.components)
    }
    assert len(probing) == 4
    for page in (ROOT / "README.md", ROOT / "docs" / "getting-started.md"):
        text = page.read_text(encoding="utf-8")
        assert all(f"`{name}`" in text for name in probing), (page.name, probing)
