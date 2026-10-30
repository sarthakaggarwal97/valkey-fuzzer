# Neutral valkey-fuzzer plugin

This package is the smallest complete external plugin. It adds, without
changing `valkey-fuzzer`:

- a no-op deployment provider (`example.deployment.neutral`) for its own
  deployment mode, `example-neutral`;
- a configured action (`example.action.neutral`);
- an oracle (`example.oracle.neutral`);
- an experiment (`example.neutral`) with a required event and required
  evidence.

The experiment copies its `message` input into the config of both
components. The action publishes an event and records evidence that contains
the message; the oracle compares that evidence with its own config. A `PASS`
therefore shows that plugin discovery, config resolution, deployment
provisioning, component execution, evidence collection, and the verdict all
worked.

## Requirements

`valkey-fuzzer` 0.2.0 or later, which provides the `valkey_fuzzer.extensions`
API this plugin imports, on Python 3.9 or later. Install it first, for example
from a checkout:

```console
python -m pip install -e /path/to/valkey-fuzzer
```

## Build and install

Build the wheel and install it into the same environment as `valkey-fuzzer`:

```console
python -m pip wheel --no-deps --wheel-dir dist .
python -m pip install dist/valkey_fuzzer_neutral_plugin-0.1.0-py3-none-any.whl
```

`valkey-fuzzer` loads every installed plugin unless a command runs with
`--no-plugins`. Check that `example.neutral` is listed:

```console
valkey-fuzzer list experiments
```

## Run

```console
valkey-fuzzer validate scenario.yaml
valkey-fuzzer run scenario.yaml --output report.json --format json
```

`scenario.yaml` selects the `example.neutral` experiment and the
`example-neutral` deployment mode, which only this plugin provides.

## Test in-process

Unit-test the plugin without installing it or starting servers:

```python
from valkey_fuzzer.extensions.testing import run_experiment
from valkey_fuzzer_neutral_plugin import PLUGIN

outcome = run_experiment(PLUGIN, "example.neutral", inputs={"message": "hello"})
assert outcome.status.value == "PASS"
```

## Start a real plugin

Copy this directory, rename the distribution, the entry point, and every
`example.*` identifier, and replace the component implementations. Component
IDs must have the form `<namespace>.<role>.<name>`. Import only from
`valkey_fuzzer.extensions`; other modules are framework internals.

Every registration must declare what its component publishes, emits, and
changes (`declares=`), or it is rejected: the action here declares its event
and evidence with their schemas (`ACTION_DECLARES`), and the oracle, which
emits nothing, `Declarations()`. The runner makes a run that publishes or
emits anything undeclared or malformed an `ERROR`. A component that changes
host or server state it will undo also declares the kind of change and owns
it with an undo obligation; see
[Own every state change](../../../docs/extending.md#own-every-state-change).

What the plugin must provide is the entry point in `pyproject.toml`, which
resolves to a `PluginManifest` (here `PLUGIN`) whose `plugin_id` equals the
entry point name. `valkey-fuzzer` checks the registrations in it when it
starts, and nothing else. See the
[extension guide](../../../docs/extending.md) for the full contract.
