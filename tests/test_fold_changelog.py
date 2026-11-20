"""scripts/fold_changelog.py moves every changelog.d fragment under one release heading and deletes it."""

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "fold_changelog.py"
README = Path(__file__).resolve().parents[1] / "changelog.d" / "README.md"


def _fold(tmp_path, *args):
    command = [sys.executable, str(SCRIPT), *args, "--changelog", "CHANGELOG.md", "--fragments", "changelog.d"]
    return subprocess.run(command, cwd=tmp_path, capture_output=True, text=True)


def _fragments(tmp_path, **files):
    folder = tmp_path / "changelog.d"
    folder.mkdir()
    (folder / "README.md").write_text(README.read_text())
    for name, text in files.items():
        (folder / f"{name}.md").write_text(text)
    return folder


def test_the_fragments_go_under_one_release_heading_by_kind_and_are_deleted(tmp_path):
    (tmp_path / "CHANGELOG.md").write_text("# Changelog\n\n## Unreleased\n\n- older\n")
    folder = _fragments(
        tmp_path,
        **{
            "b-fix": "### Fixed\n\n- a crash\n\n### Added\n- `--flag`\n",
            "a-change": "- bullets without a heading are Changed\n  and may wrap\n",
            "c-titled": "### Changed: the titled entry\n\n- keeps its own heading, after the bare kind's\n",
            "d-titled-too": "### Changed: the titled entry\n- and two fragments with one title share it\n",
        },
    )
    done = _fold(tmp_path, "0.3.0", "--date", "2026-11-20")
    assert done.returncode == 0, done.stderr
    assert done.stdout == "folded 4 fragment(s) into CHANGELOG.md as 0.3.0 - 2026-11-20\n"
    assert (tmp_path / "CHANGELOG.md").read_text() == (
        "# Changelog\n"
        "\n"
        "## 0.3.0 - 2026-11-20\n"
        "\n"
        "### Added\n"
        "\n"
        "- `--flag`\n"
        "\n"
        "### Changed\n"
        "\n"
        "- bullets without a heading are Changed\n"
        "  and may wrap\n"
        "\n"
        "### Changed: the titled entry\n"
        "\n"
        "- keeps its own heading, after the bare kind's\n"
        "- and two fragments with one title share it\n"
        "\n"
        "### Fixed\n"
        "\n"
        "- a crash\n"
        "\n"
        "## Unreleased\n"
        "\n"
        "- older\n"
    )
    assert sorted(path.name for path in folder.iterdir()) == ["README.md"]


def test_an_empty_changelog_or_one_without_a_title_gets_the_section_at_the_top(tmp_path):
    (tmp_path / "CHANGELOG.md").write_text("")
    _fragments(tmp_path, one="- first\n")
    assert _fold(tmp_path, "1.0.0", "--date", "2027-01-01").returncode == 0
    assert (tmp_path / "CHANGELOG.md").read_text() == "## 1.0.0 - 2027-01-01\n\n### Changed\n\n- first\n"
    (tmp_path / "again").mkdir()
    _fragments(tmp_path / "again", two="- second\n")
    (tmp_path / "again" / "CHANGELOG.md").write_text("## 1.0.0 - 2027-01-01\n\n- first\n")
    assert _fold(tmp_path / "again", "1.1.0", "--date", "2027-02-01").returncode == 0
    assert (tmp_path / "again" / "CHANGELOG.md").read_text() == (
        "## 1.1.0 - 2027-02-01\n\n### Changed\n\n- second\n\n## 1.0.0 - 2027-01-01\n\n- first\n"
    )


@pytest.mark.parametrize(
    ("text", "message"),
    [
        (
            "### Deprecated: x\n- x\n",
            "a heading is one of Added, Changed, Removed, Fixed, with an optional ': <title>', not 'Deprecated: x'",
        ),
        ("### Added\n\n", "has no bullets"),
    ],
)
def test_a_fragment_with_an_unknown_heading_or_no_bullets_is_refused_and_nothing_changes(tmp_path, text, message):
    (tmp_path / "CHANGELOG.md").write_text("# Changelog\n")
    folder = _fragments(tmp_path, bad=text, good="- fine\n")
    failed = _fold(tmp_path, "0.3.0")
    assert failed.returncode == 1 and message in failed.stderr and "bad.md" in failed.stderr
    assert (tmp_path / "CHANGELOG.md").read_text() == "# Changelog\n"
    assert sorted(path.name for path in folder.iterdir()) == ["README.md", "bad.md", "good.md"]


def test_nothing_to_fold_is_an_error_that_leaves_the_changelog_alone(tmp_path):
    (tmp_path / "CHANGELOG.md").write_text("# Changelog\n")
    _fragments(tmp_path)
    failed = _fold(tmp_path, "0.3.0")
    assert failed.returncode == 1 and failed.stderr == "no fragments in changelog.d: nothing to fold\n"
    assert (tmp_path / "CHANGELOG.md").read_text() == "# Changelog\n"


def test_the_repositorys_fragments_fold_as_they_are(tmp_path):
    # Every fragment in flight has the shape the fold expects, so a release never fails on one.
    (tmp_path / "CHANGELOG.md").write_text("# Changelog\n")
    shutil.copytree(README.parent, tmp_path / "changelog.d")
    done = _fold(tmp_path, "0.0.0")
    if sorted(path.name for path in README.parent.iterdir()) == ["README.md"]:
        assert done.returncode == 1, done.stderr
    else:
        assert done.returncode == 0, done.stderr
