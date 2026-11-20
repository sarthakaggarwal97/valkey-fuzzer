# Changelog fragments

Every pull request adds one file here instead of editing `CHANGELOG.md`, so
two branches never conflict on the same lines.

- Name it after your branch, as a slug: `fix/r4-t1-versioned-formats` becomes
  `t1-versioned-formats.md` (keep what identifies the change; drop the
  prefix).
- Write markdown bullets: what changed, for whom, and why, in the voice of the
  existing changelog. One bullet per behaviour.
- Group bullets under `### Added`, `### Changed`, `### Removed`, or
  `### Fixed` when more than one kind applies; a file with bullets alone
  counts as `### Changed`.

At release time, `python scripts/fold_changelog.py <version>` moves every
fragment under a new `## <version> - <date>` heading at the top of
`CHANGELOG.md` and deletes the fragments. This file stays.
