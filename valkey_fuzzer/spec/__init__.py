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

The names below are imported on first use. The components read a bundle's
mapping through :mod:`~valkey_fuzzer.spec.bundle`, so every run imports this
package; the three tools, which need java and ``tla2tools.jar`` to do
anything, are loaded only by the ``spec`` commands and a batch's conform step.
"""

import importlib
from typing import Any

_MODULE_OF = {
    "Bundle": "bundle",
    "SpecBundleError": "bundle",
    "SpecMapping": "bundle",
    "find_bundle": "bundle",
    "load_bundle": "bundle",
    "load_spec_mapping": "bundle",
    "check_bundle": "check",
    "collect_runs": "conform",
    "conform_runs": "conform",
    "TlaError": "tla",
    "TlaTools": "tla",
    "TranslateError": "translate",
    "bundle_service": "translate",
    "translate_trace": "translate",
}

__all__ = sorted(_MODULE_OF)


def __getattr__(name: str) -> Any:
    module = _MODULE_OF.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return getattr(importlib.import_module(f".{module}", __name__), name)
