"""Command handlers for ``valkey-fuzzer``.

Each module registers its subcommands on the main parser, with the handler as
the ``handler`` default. A handler takes the parsed namespace, calls
:class:`~valkey_fuzzer.service.RunService`, prints, and returns an exit code;
compiling, executing, and writing reports happen in the service, and an error
a handler raises is reported once, by :func:`valkey_fuzzer.cli.main`.
"""

from typing import Any

from . import batch, diff, listing, minimize, replay, run, sweep, validate

COMMAND_MODULES = (run, batch, validate, listing, replay, diff, minimize, sweep)


def register_commands(subparsers: Any) -> None:
    """Add every command's subparser to the main parser's ``subparsers``."""

    for module in COMMAND_MODULES:
        module.register(subparsers)


__all__ = ["COMMAND_MODULES", "register_commands"]
