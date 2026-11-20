"""``redis.action.sleep``: wait a fixed time, so a fault can trigger on ``<id>.completed`` after the workload ran.

The fuzzer's core ships no component of any role, so a pack brings even the sleep its experiments pace
themselves with (the Valkey pack has ``valkey.action.sleep``).
"""

import time
from typing import Any

from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.registrations import ComponentRegistration
from valkey_fuzzer.results import ComponentStatus, make_result
from valkey_fuzzer.schema import ConfigField, ConfigSchema

CONFIG = ConfigSchema({"seconds": ConfigField(float, minimum=0, maximum=7200)})


class SleepAction:
    def run(self, context: Any, spec: ComponentSpec):
        seconds = spec.config["seconds"]
        if context.deadline is not None:
            seconds = max(0.0, min(seconds, context.deadline - time.monotonic()))
        context.sleep(seconds)
        return make_result(spec, ComponentStatus.PASS, f"waited {spec.config['seconds']}s")


REGISTRATIONS = (
    ComponentRegistration(
        "redis.action.sleep", ComponentRole.ACTION, SleepAction, config=CONFIG, declares=Declarations()
    ),
)
