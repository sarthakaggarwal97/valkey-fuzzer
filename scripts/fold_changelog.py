"""Fold the changelog fragments into CHANGELOG.md for a release.

    python scripts/fold_changelog.py 0.3.0

Every ``changelog.d/*.md`` but ``README.md`` is a fragment: markdown bullets, under an optional ``### Added``,
``### Changed``, ``### Removed``, or ``### Fixed`` heading (bullets under none are ``Changed``). The fragments go
under a new ``## <version> - <date>`` heading right after the changelog's title, one subsection per kind in the
order above, and are deleted. Only the standard library.
"""

import argparse
import datetime
import sys
from pathlib import Path

KINDS = ("Added", "Changed", "Removed", "Fixed")


def read_fragment(path: Path) -> dict[str, list[str]]:
    """The bullets of one fragment, by kind; a ``### <kind>`` heading starts a kind, and text before any is Changed."""

    bullets: dict[str, list[str]] = {kind: [] for kind in KINDS}
    kind = "Changed"
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if line.startswith("### "):
            kind = line[len("### ") :].strip()
            if kind not in KINDS:
                raise SystemExit(f"{path}:{number}: a heading is one of {', '.join(KINDS)}, not {kind!r}")
        elif line.strip():
            bullets[kind].append(line.rstrip())
    if not any(bullets.values()):
        raise SystemExit(f"{path} has no bullets")
    return bullets


def release_section(version: str, date: str, fragments: list[Path]) -> str:
    """The ``## <version> - <date>`` section holding every fragment's bullets, by kind."""

    bullets: dict[str, list[str]] = {kind: [] for kind in KINDS}
    for path in fragments:
        for kind, lines in read_fragment(path).items():
            bullets[kind].extend(lines)
    parts = [f"## {version} - {date}"]
    for kind in KINDS:
        if bullets[kind]:
            parts.append(f"### {kind}\n\n" + "\n".join(bullets[kind]))
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
