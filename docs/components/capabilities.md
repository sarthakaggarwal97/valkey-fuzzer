# Capabilities

Part of the [Valkey component reference](README.md): the
[index](README.md#index) lists every component, [Common config
forms](README.md#common-config-forms) the selectors, predicates, and trigger
fields the tables below use.

Capabilities run before the deployment starts. A required capability that
returns `SKIP` makes the run `SKIP_UNSUPPORTED`, which a probe does only for a
feature the experiment names (in the probe's config); any other startup or
configuration error is `ERROR`, so a typo never skips a run.

## `valkey.capability.build-probe`

Starts each distinct binary the run will use (the deployment's default
binary, every `instance_binaries` entry, and `binaries`) as a private server
on a Unix socket with `startup_config`, then checks its version, config
values, INFO fields, and commands. It returns `SKIP` when a binary lacks
one of them, and `ERROR` when a binary cannot be found, the probe server
fails, a binary knows a config it must lack (`lacks_configs`: it is not the
build the experiment needs), or the binaries cover fewer than
`min_distinct_builds` builds (a second build is an input the operator gives,
not a feature the host lacks). It never builds Valkey from source. With
`deployment_config` it also starts each deployment binary with the
configuration the deployment gives the instances that run it.
`deployment_config` names the settings the build may lack: when the build
rejects the configuration but starts without those settings (and the
probe's own `startup_config`), such as `dual-channel-replication-enabled yes`
on a build from before dual-channel replication, the run is `SKIP_UNSUPPORTED`; any other rejected
setting makes it `ERROR`. Those starts ask whether the build accepts the
configuration and, once started, implements `commands`: a module the
deployment loads (`deployment.server_config.loadmodule`) brings its commands
only there, so with `deployment_config` the commands are checked on those
starts instead of the probe's own, and a deployment that does not load the
module is `SKIP_UNSUPPORTED` (a module that fails to load is `ERROR`). The
`config_get` values and INFO fields are checked on the probe's own
`startup_config`, never on the deployment's. A scenario
that configures a feature away therefore runs and lacks the experiment's
required path evidence, which makes it `ERROR`; it is never hidden as unsupported.
A probe server that crashes, when it starts or on a command the probe sends
(its log or output holds a crash or sanitizer report, read as
`valkey.oracle.liveness` reads an instance's), makes the probe `FAIL` marked
as a crash, whatever else it could not probe: `crashes` in the diagnostics
names each probe's cause, so the run `FAIL`s with the same failure signature
the liveness oracle gives that crash in a deployment instance.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `startup_config` | mapping | `{}` | Server config the probe server starts with; a directive the build rejects means `SKIP`. |
| `deployment_config` | list | `[]` | Settings the build may lack, such as `[dual-channel-replication-enabled]`. Non-empty: also start each binary with each distinct config the deployment gives its instances (`server_config`, `instance_configs`, and startup overlays), on top of `startup_config`; `SKIP` when the build refuses it but starts without these settings, `ERROR` when it refuses something else. |
| `scope` | `all` or `listed` | `all` | `listed` probes only `binaries` (which it then requires), not the deployment's. |
| `config_get` | mapping | `{}` | Config name to `null` (must exist) or a list of accepted values. A renamed config is read by the name the build knows ([Effective server config](../contracts.md#effective-server-config)). |
| `info_fields` | mapping | `{}` | INFO section to the field names it must report. |
| `lacks_configs` | list | `[]` | Configs the build must not know (`CONFIG GET` returns nothing), such as `dual-channel-replication-enabled` for a build from before it; a build that knows one is `ERROR`. |
| `config_set` | mapping | `{}` | Config name to the values `CONFIG SET` must accept at runtime. |
| `commands` | list | `[]` | Command names the build must implement; with `deployment_config` (`[loadmodule]` for a module's commands, such as `JSON.GET`), checked on the deployment config's starts. |
| `min_version` | string or null | `null` | Lowest accepted version, such as `"8.1.0"`. |
| `min_distinct_builds` | integer, 1 to 64 | 1 | Distinct (version, git SHA) builds the binaries must cover; fewer is `ERROR`. |
| `binaries` | list of strings or references | `[]` | Further binaries to probe, such as a rolling-upgrade target. |
| `timeout_s` | number, 1 to 120 | 60 | How long the probe server may take to start. |

Evidence: `valkey.capability.supported` (per-binary results), on `PASS`.

## `valkey.capability.host-probe`

Returns `SKIP` when the host lacks a requirement.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `requires` | list | | One or more of `linux`, `root`, `not_root`, `tmpfs` (Linux, root, `mount` and `umount`), `iptables` (Linux, root, a working `iptables`), `netem` (Linux, `tc` and `ip` with `CAP_NET_ADMIN`), `libfaketime` (the library `valkey.fault.clock-skew` preloads, or `VALKEY_FUZZER_LIBFAKETIME`), `cgroup_v2` (a delegated cgroup v2 of the run's own with the `cpu` and `memory` controllers, for `valkey.fault.resource-pressure`), and `per_instance_loopback` (the host can bind `127.0.0.2`). |

Evidence: `valkey.host.supported`.
