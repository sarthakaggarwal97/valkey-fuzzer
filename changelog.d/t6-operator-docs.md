### Added: operator and threat-model docs; the component reference is one page per role (PR-T6, P-09)

- `docs/operations.md` (what a host needs and what a run says without it, running a soak, where results go,
  disk, ports, cores, and memory, stop and resume, the sweep, budgets and the default timeout rule, logs),
  `docs/triage.md` (from a batch's failure signatures to a filed bug: load, replay, minimize, the signature,
  fuzzer bug or Valkey bug, what to attach, the zoo), and `docs/threat-model.md` (every host change and its
  mechanism, the scoping guards of #176, what runs as root, what a hostile experiment file can do and what refuses
  it, CI trust, residual risks). Every command in them was run on this branch.
- `docs/components.md` (2,374 lines) is `docs/components/`: `README.md` holds the common config forms, an index
  of every component and provider grouped by role, and the declarations table; `capabilities.md`,
  `workloads.md`, `actions.md`, `faults.md`, `observers.md`, `oracles.md`, and `providers.md` hold the sections
  unchanged (`valkey.workload.benchmark` moved from Actions to Workloads, where its role is). Every link to the
  old page points at the new one.
- `scripts/gen_component_docs.py` writes the index and declarations tables from the installed catalog (the
  one-line descriptions stay hand-written) and `--check` fails when a component lacks a row or a section, or a
  page documents one the catalog lacks; `tests/test_docs_declarations.py` runs it.
- `docs/README.md` indexes every page with one line each, and the README points at it; `tests/test_docs_links.py`
  checks that every relative link and `#anchor` in the Markdown files resolves.
