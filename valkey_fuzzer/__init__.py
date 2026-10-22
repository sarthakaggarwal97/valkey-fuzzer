"""Valkey fuzzer package.

Extension packages should import supported contracts from
:mod:`valkey_fuzzer.extensions`; other modules are framework internals.
"""

from .extension_contract import EXTENSION_API_VERSION

__all__ = ["EXTENSION_API_VERSION"]
