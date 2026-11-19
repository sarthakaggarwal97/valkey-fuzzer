"""``python -m valkey_fuzzer.batch TASK``: the worker process of one job."""

import sys

from .worker import main

sys.exit(main())
