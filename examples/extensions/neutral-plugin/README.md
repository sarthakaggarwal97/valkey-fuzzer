# Neutral valkey-fuzzer extension

This package is the smallest complete external extension example. It adds,
without changing `valkey-fuzzer` core:

- a no-op topology provider;
- a configured action;
- an oracle;
- a feature-mode feature pack with required event and evidence contracts.

The feature pack copies `feature.with.message` into both component
configurations. The action publishes an event and records evidence containing
that message. The oracle independently compares the evidence with its resolved
configuration. A passing run therefore proves that plugin discovery,
configuration resolution, topology provisioning, component execution, evidence
collection, and verdict calculation all occurred.

Build and install it into the same environment as `valkey-fuzzer`:

```console
python -m pip wheel --no-deps --wheel-dir dist .
python -m pip install dist/valkey_fuzzer_neutral_plugin-0.1.0-py3-none-any.whl
```

Then use the existing CLI:

```console
valkey-fuzzer validate scenario.yaml
valkey-fuzzer run scenario.yaml --output report.json --format json
```

To start a real extension, copy this directory and replace the `example.*`
identifiers and component implementations. Extension code should import only
the supported `valkey_fuzzer.extensions` authoring facade.
