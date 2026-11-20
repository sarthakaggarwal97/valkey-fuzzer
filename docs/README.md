# Documentation

Every page, one line each. Start with the [README](../README.md), then the
first group.

## Start here

| Page | For | One line |
| --- | --- | --- |
| [Getting started](getting-started.md) | first-time users | install, choose a binary, build `porcupine`, first runs, the output and report files, troubleshooting |
| [Reading a FAIL](reading-a-fail.md) | anyone with a failed run | where the verdict, logs, and replay command are, and what to attach to a Valkey issue |
| [Concepts and glossary](concepts.md) | everyone | scenarios, execution plans, component roles, events, evidence, and the terms the docs use |

## Operate

| Page | For | One line |
| --- | --- | --- |
| [Operations](operations.md) | whoever runs soaks | what the host needs, running a soak, where results go, disk and ports, stopping and resuming, budgets and timeouts, logs |
| [Triage](triage.md) | whoever reads a soak's failures | from a signature to a filed bug: load, replay, minimize, fuzzer bug or Valkey bug, what to attach, the zoo |
| [Threat model](threat-model.md) | operators, security reviewers | what the fuzzer does to the host, the scoping guards, root, hostile experiment files, CI trust, residual risks |
| [Cluster chaos and CI](cluster-chaos-and-ci.md) | cluster chaos users, CI maintainers | running cluster chaos, kill safety, the GitHub workflows, reproducing a CI failure |
| [Bug zoo](bug-zoo.md) | experiment authors, soak maintainers | deliberately broken Valkey builds the experiments must catch, and how to add one |
| [Fuzzing a module from its own repository](module-repos.md) | module maintainers | the reusable workflow a module repository calls, the module's commands as a base file, the Docker image, what a run uploads, reading a FAIL |

## Reference

| Page | For | One line |
| --- | --- | --- |
| [What is stable](stability.md) | tool and component authors | the surfaces a release keeps (commands, exit codes, YAML, report keys, component ids, `format_version`) and the ones it does not |
| [Base contracts](contracts.md) | everyone | the rules every run is held to: verdicts, evidence, fault ownership, effective config, profiles, deadlines, host state, replay |
| [Scenarios and experiments](scenarios-and-experiments.md) | scenario and experiment authors | the file formats, the local deployment, the Valkey experiments, random scenarios and search profiles |
| [Valkey components](components/README.md) | experiment authors | the index of every component and provider, with one page per role: [capabilities](components/capabilities.md), [workloads](components/workloads.md), [actions](components/actions.md), [faults](components/faults.md), [observers](components/observers.md), [oracles](components/oracles.md), [providers](components/providers.md) |
| [Command-line reference](cli.md) | users and scripts | every command and option, exit codes, report folders and formats |

## Extend

| Page | For | One line |
| --- | --- | --- |
| [Adding tests](extending.md) | experiment and component authors | writing experiments, components, and providers, registering them, testing |
| [Architecture](architecture.md) | framework contributors | code layout, the compiler and runner, the local deployment provider, the wave engine |
| [Releasing](releasing.md) | maintainers | the version, the release steps, what the sdist and the wheel hold, the release workflow, the one-time PyPI setup, the porcupine checker binaries |
| [Spec bridge](spec-bridge.md) | model and experiment authors | TLA+ bundles: `spec check`, `spec translate`, `spec conform`, the assurance report |
| [Cluster failover election](../formal/cluster-election/README.md) | Valkey cluster maintainers | the TLA+ model of the failover vote and its live counterpart |
| [Atomic slot migration](../formal/slot-migration/README.md) | Valkey cluster maintainers | the TLA+ model of atomic slot migration |
| [Primary-replica replication](../formal/replication/README.md) | Valkey maintainers | the TLA+ model of replication, PSYNC included |
| [Persistence ordering](../formal/persistence/README.md) | Valkey maintainers | the TLA+ model of the order a server writes, rewrites, and restarts from its multi-part AOF and RDB files |
| [Changelog](../CHANGELOG.md) | everyone | what changed in each release; `changelog.d/` holds the unreleased fragments |
