"""The fuzzer's cache: ``$XDG_CACHE_HOME/valkey-fuzzer``, by default ``~/.cache/valkey-fuzzer``.

What a host fetches or builds once and reuses lives here: the Valkey builds under ``valkey/<commit>/``
(:func:`valkey_fuzzer.deployment.local.processes.fetch_valkey`). It is the user's own: nothing in it is shared
between users, and it is safe to delete.
"""

import os


def cache_dir(*parts: str) -> str:
    """``$XDG_CACHE_HOME/valkey-fuzzer/<parts>``, by default ``~/.cache/valkey-fuzzer/<parts>``."""

    return os.path.join(os.environ.get("XDG_CACHE_HOME") or os.path.expanduser("~/.cache"), "valkey-fuzzer", *parts)
