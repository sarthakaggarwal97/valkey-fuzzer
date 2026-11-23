"""``fetch-valkey``: build a Valkey ref once into the cache and print its ``valkey-server``.

The build goes to ``<cache>/valkey/<commit>/`` (:func:`~valkey_fuzzer.deployment.local.processes.fetch_valkey`),
where a deployment without a configured binary and no ``valkey-server`` on PATH finds the pinned commit's build
instead of building it on its first run. Only the binary's path goes to stdout, so a script can capture it.
"""

import argparse
from typing import Any

from ..deployment.local.models import VALKEY_SOURCE_REF, VALKEY_SOURCE_SHA
from ..outcomes import EXIT_SUCCESS


def register(subparsers: Any) -> None:
    parser = subparsers.add_parser(
        "fetch-valkey", help="Build a Valkey tag, branch, or commit once into the cache and print its valkey-server"
    )
    parser.add_argument(
        "ref",
        nargs="?",
        default=VALKEY_SOURCE_SHA,
        help=f"A tag, branch, or full commit hash of valkey-io/valkey (default: the pinned {VALKEY_SOURCE_REF} "
        "commit a deployment without a binary builds)",
    )
    parser.set_defaults(handler=fetch_valkey_command)


def fetch_valkey_command(args: argparse.Namespace) -> int:
    """Fetch and build the ref unless the cache has it, and print the binary's path."""

    # Imported here, so building the parser does not load the processes module and the valkey client it brings.
    from ..deployment.local.processes import fetch_valkey

    _, binary = fetch_valkey(args.ref)
    print(binary)
    return EXIT_SUCCESS
