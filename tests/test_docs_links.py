"""Every relative link in the Markdown docs resolves: the file exists, and a ``#fragment`` names a heading of it.

Anchors follow GitHub's rule: the heading text lowercased, punctuation but hyphens and spaces dropped, spaces to
hyphens, and ``-1``, ``-2``, ... on a repeated heading. Links inside fenced code blocks are not links.
"""

import re
from pathlib import Path

import pytest

from tests.support import ROOT

MARKDOWN = sorted(
    path
    for path in ROOT.rglob("*.md")
    if ".venv" not in path.parts
    and "node_modules" not in path.parts
    and "vendor" not in path.parts  # third-party docs vendored with a Go module link to files `go mod vendor` drops
    and not path.parts[len(ROOT.parts)].startswith(".")
)
_LINK = re.compile(r"(?<!!)\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
_HEADING = re.compile(r"^#{1,6}\s+(.*?)\s*#*\s*$")
_FENCE = re.compile(r"^\s*(```|~~~)")


def _anchor(heading: str) -> str:
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", heading)  # a link in a heading: its text
    text = re.sub(r"[`*]", "", text).strip().lower()
    text = re.sub(r"[^\w\- ]", "", text)
    return text.replace(" ", "-")


def _prose(text: str):
    """``(line number, line)`` of every line outside fenced code blocks."""

    fence = None
    for number, line in enumerate(text.splitlines(), start=1):
        match = _FENCE.match(line)
        if match:
            if fence is None:
                fence = match.group(1)
            elif match.group(1) == fence:
                fence = None
            continue
        if fence is None:
            yield number, line


def anchors(path: Path) -> set:
    seen = {}
    found = set()
    for _, line in _prose(path.read_text(encoding="utf-8")):
        match = _HEADING.match(line)
        if not match:
            continue
        base = _anchor(match.group(1))
        count = seen.get(base, 0)
        seen[base] = count + 1
        found.add(base if count == 0 else f"{base}-{count}")
    return found


def links(path: Path):
    for number, line in _prose(path.read_text(encoding="utf-8")):
        for match in _LINK.finditer(line):
            yield number, match.group(1)


def _name(path: Path) -> str:
    return str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path)


def broken_links(path: Path) -> list:
    problems = []
    for number, target in links(path):
        if target.startswith(("http://", "https://", "mailto:")):
            continue
        file_part, _, fragment = target.partition("#")
        resolved = path if not file_part else (path.parent / file_part).resolve()
        if not resolved.exists():
            problems.append(f"{_name(path)}:{number}: {target}: no such file")
            continue
        if fragment and resolved.suffix == ".md" and fragment not in anchors(resolved):
            problems.append(f"{_name(path)}:{number}: {target}: no heading for #{fragment}")
    return problems


@pytest.mark.parametrize("path", MARKDOWN, ids=lambda path: str(path.relative_to(ROOT)))
def test_every_relative_link_resolves(path):
    assert broken_links(path) == []


def test_the_anchor_rule_matches_github(tmp_path):
    page = tmp_path / "page.md"
    page.write_text(
        "# Title\n## `valkey.action.sleep`\n## 5. What to attach\n## Same\n## Same\n### [Faults](faults.md)\n"
        "```\n## not a heading\n```\n",
        encoding="utf-8",
    )

    assert anchors(page) == {"title", "valkeyactionsleep", "5-what-to-attach", "same", "same-1", "faults"}


def test_a_broken_link_is_named(tmp_path):
    page = tmp_path / "page.md"
    page.write_text("# A\n\n[gone](missing.md) and [no anchor](#nothing)\n\n```\n[code](also-missing.md)\n```\n")

    assert broken_links(page) == [
        f"{page}:3: missing.md: no such file",
        f"{page}:3: #nothing: no heading for #nothing",
    ]
