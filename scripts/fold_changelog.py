"""Fold the changelog fragments into CHANGELOG.md for a release.

    python scripts/fold_changelog.py 0.3.0

Every ``changelog.d/*.md`` but ``README.md`` is a fragment: markdown bullets, under an optional ``### Added``,
``### Changed``, ``### Removed``, or ``### Fixed`` heading, which may carry a title (``### Changed: CI trust
hardening``, as CHANGELOG.md's entries do); bullets under no heading are ``Changed``. The fragments go under a new
``## <version> - <date>`` heading right after the changelog's title: one subsection per heading, the bare kinds in
the order above, each kind's titled headings after it, and are deleted. Only the standard library.
"""

import argparse
import datetime
import sys
from pathlib import Path

KINDS = ("Added", "Changed", "Removed", "Fixed")


def _order(heading: str) -> tuple[int, str]:
    """A bare kind first, then its titled headings by title."""

    kind, _, title = heading.partition(":")
    return KINDS.index(kind.strip()), title.strip()


def read_fragment(path: Path) -> dict[str, list[str]]:
    """The bullets of one fragment, by heading (``<kind>`` or ``<kind>: <title>``); text before any is Changed."""

    bullets: dict[str, list[str]] = {}
    heading = "Changed"
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if line.startswith("### "):
            heading = line[len("### ") :].strip()
            if heading.partition(":")[0].strip() not in KINDS:
                raise SystemExit(
                    f"{path}:{number}: a heading is one of {', '.join(KINDS)}, with an optional ': <title>', "
                    f"not {heading!r}"
                )
        elif line.strip():
            bullets.setdefault(heading, []).append(line.rstrip())
    if not bullets:
        raise SystemExit(f"{path} has no bullets")
    return bullets


def release_section(version: str, date: str, fragments: list[Path]) -> str:
    """The ``## <version> - <date>`` section holding every fragment's bullets, by heading."""

    bullets: dict[str, list[str]] = {}
    for path in fragments:
        for heading, lines in read_fragment(path).items():
            bullets.setdefault(heading, []).extend(lines)
    parts = [f"## {version} - {date}"]
    for heading in sorted(bullets, key=_order):
        parts.append(f"### {heading}\n\n" + "\n".join(bullets[heading]))
    return "\n\n".join(parts) + "\n"


def fold(changelog: Path, section: str) -> None:
    """Put ``section`` right after the changelog's ``# `` title, or at the top of a changelog without one."""

    lines = changelog.read_text(encoding="utf-8").splitlines(keepends=True)
    at = next((index + 1 for index, line in enumerate(lines) if line.startswith("# ")), 0)
    # One blank line between the title and the section, and one after it.
    while at < len(lines) and not lines[at].strip():
        at += 1
    head = lines[:at]
    if head and head[-1].strip():
        head.append("\n")
    changelog.write_text("".join(head) + section + ("\n" if lines[at:] else "") + "".join(lines[at:]), encoding="utf-8")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Fold changelog.d/*.md into CHANGELOG.md under a release heading.")
    parser.add_argument("version", help="The release, as in '## <version> - <date>'")
    parser.add_argument("--date", default=datetime.date.today().isoformat(), help="The release date (default: today)")
    parser.add_argument("--changelog", type=Path, default=Path("CHANGELOG.md"))
    parser.add_argument("--fragments", type=Path, default=Path("changelog.d"))
    args = parser.parse_args(argv)
    fragments = sorted(path for path in args.fragments.glob("*.md") if path.name != "README.md")
    if not fragments:
        print(f"no fragments in {args.fragments}: nothing to fold", file=sys.stderr)
        return 1
    fold(args.changelog, release_section(args.version, args.date, fragments))
    for path in fragments:
        path.unlink()
    print(f"folded {len(fragments)} fragment(s) into {args.changelog} as {args.version} - {args.date}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
