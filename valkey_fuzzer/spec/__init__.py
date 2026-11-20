"""The spec bridge: TLA+ models checked, turned into fuzzer scenarios, and compared with real runs.

A bundle ``<protocol>/`` holds the claims, the model, its configs, the
mapping to implementation evidence, negative controls, and optionally the
data-only experiments its counterexamples run in
(:mod:`~valkey_fuzzer.spec.bundle`). ``valkey-fuzzer spec`` runs the three
tools over it:

- :mod:`~valkey_fuzzer.spec.check`: SANY, TLC on the safe configs and the
  negative controls, model coverage, and the assurance report;
- :mod:`~valkey_fuzzer.spec.translate`: a counterexample to scenarios;
- :mod:`~valkey_fuzzer.spec.conform`: observed-state checks and action
  coverage of real runs (not refinement).

Each name is imported from its module. Importing any of them needs no Java:
the tools need java and ``tla2tools.jar`` only to run SANY or TLC, and
without them a ``spec`` command, or a run's conform check, prints
:data:`~valkey_fuzzer.spec.tla.TOOLS_HINT`.
"""
