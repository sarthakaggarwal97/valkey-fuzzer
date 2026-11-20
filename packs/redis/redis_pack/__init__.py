"""The ``redis`` pack: Redis OSS as a second target of valkey-fuzzer, through the ``valkey_fuzzer.packs`` entry point.

It ships a deployment provider for one ``redis-server`` (mode ``redis``), a SET/GET workload with a ledger, a
process-signal fault, the liveness, crash, and ledger oracles, a sleep action, and the experiment
``redis.kill-restart``. It imports the fuzzer's core alone, never ``valkey_fuzzer.components`` or
``valkey_fuzzer.deployment``: what it needs from the fuzzer is what any pack gets.
"""

from pathlib import Path

from valkey_fuzzer.catalog import Catalog, load_experiment_directory

from . import actions, fault, oracles, workload
from .deployment import deployment_provider_registration

__version__ = "0.1.0"

EXPERIMENT_DIRECTORY = Path(__file__).resolve().parent / "experiments"
# The component modules, in registration order.
_MODULES = (workload, actions, fault, oracles)
REGISTRATIONS = tuple(registration for module in _MODULES for registration in module.REGISTRATIONS)


def experiments(fragments=None):
    """Every ``experiments/<id>.yaml``, as :class:`~valkey_fuzzer.plan.ExperimentSpec`."""

    return tuple(load_experiment_directory(EXPERIMENT_DIRECTORY, fragments=fragments).values())


def register(catalog: Catalog) -> None:
    """Register the pack's components, its deployment provider, and its experiments, as one transaction."""

    catalog.register(
        components=REGISTRATIONS,
        deployment_providers=(deployment_provider_registration(),),
        experiments=experiments(catalog.fragments),
    )


__all__ = ["EXPERIMENT_DIRECTORY", "REGISTRATIONS", "__version__", "experiments", "register"]
