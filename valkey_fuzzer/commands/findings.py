"""``findings``: read a findings store (see :mod:`valkey_fuzzer.findings`).

``list`` prints one line per finding, new ones first; ``show KEY`` prints one finding, with its signature and
runs, as JSON. The store is ``findings.json`` in the working directory, or ``--store PATH`` (a batch folder
names the store beside its ``summary.json``).
"""

import argparse
import json
import os
from typing import Any

from .. import findings
from ..outcomes import EXIT_SUCCESS
from ..reports import InputError


def register(subparsers: Any) -> None:
    parser = subparsers.add_parser(
        "findings", help="List or show the failure signatures batches recorded in a findings store"
    )
    commands = parser.add_subparsers(dest="findings_command", required=True)
    listing = commands.add_parser("list", help="One line per finding: status, runs, first and last seen, key, label")
    listing.add_argument("--status", choices=findings.STATUSES, help="Only findings with this status")
    show = commands.add_parser("show", help="One finding as JSON: its signature, runs, and reproduce command")
    show.add_argument("key", help="The finding's key (as list prints it), or a prefix that names one finding")
    for command in (listing, show):
        command.add_argument(
            "--store",
            default=findings.FINDINGS_FILE,
            metavar="PATH",
            help=f"The findings store, or the batch folder holding one (default: ./{findings.FINDINGS_FILE})",
        )
    parser.set_defaults(handler=findings_command)


def store_path(path: str) -> str:
    """``path`` itself, or the store in it when it is a batch folder (or any directory)."""

    return os.path.join(path, findings.FINDINGS_FILE) if os.path.isdir(path) else path


def findings_command(args: argparse.Namespace) -> int:
    path = store_path(args.store)
    if not os.path.exists(path):
        raise InputError(
            f"no findings store at {path}: a finished batch writes {findings.FINDINGS_FILE} beside its summary.json, "
            "and `batch --findings PATH` writes to PATH instead"
        )
    store = findings.load(path)
    if args.findings_command == "show":
        print(json.dumps(findings.find(store, args.key, path), indent=2))
        return EXIT_SUCCESS
    listed = findings.entries(store, args.status)
    if not listed:
        what = f"{args.status} findings" if args.status else "findings"
        print(f"No {what} in {path}")
        return EXIT_SUCCESS
    print(f"{'status':<6} {'runs':>5} {'first seen':<10} {'last seen':<10} {'key':<12} label")
    for entry in listed:
        print(
            f"{entry['status']:<6} {entry['count']:>5} {entry['first_seen'][:10]:<10} {entry['last_seen'][:10]:<10} "
            f"{entry['key']:<12} {entry['label']}"
        )
    return EXIT_SUCCESS
