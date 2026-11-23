# Providers

Part of the [Valkey component reference](README.md): the
[index](README.md#index) lists every component, [Common config
forms](README.md#common-config-forms) the selectors, predicates, and trigger
fields the tables below use.

Scenarios declare artifacts and resources with these providers; see
[Artifacts and resources](../scenarios-and-experiments.md#artifacts-and-resources).

## `valkey.artifact.local-binary`

Resolves a `valkey-server` on this host, records its SHA-256, version, and git
SHA, and optionally verifies them.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `path` | string or reference | | A path or a command name on `PATH`. |
| `expected_version` | string or null | `null` | The version `--version` must report. |
| `expected_sha` | string or null | `null` | A prefix of the git SHA or of the file's SHA-256, at least 7 hex characters (as git abbreviates). |

Output: `binary`, the absolute path.

## `valkey.artifact.local-module`

A module build on this host, such as valkey-json's `libjson.so` or one of
Valkey's `tests/modules`, for a deployment's `server_config.loadmodule` or the
`modules` of `valkey.action.commands`. Its SHA-256 is the provenance a replay
compares, so a replay that loads another build of the module reports drift,
as for a binary.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `path` | string or reference | | The module file; a relative path is taken from the current directory. |

Output: `path`, the absolute path. A bare path only, without module
arguments: Valkey reads a module's arguments from the words after its path on
one `loadmodule` directive, and a server config value is one argument (a list
repeats the directive), so arguments could not reach the module.

## `valkey.resource.tmpdir`

A private directory, removed after the run.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `prefix` | string | `valkey-fuzzer-` | |
| `parent` | string or null | `null` | The directory to create it in; the temporary directory by default. |
| `keep` | boolean | `false` | Keep it after the run. |

Output: `path`.

## `valkey.resource.tmpfs`

A size-limited tmpfs mount, unmounted after the run. Linux and root only;
elsewhere the provider declines and the run is `SKIP_UNSUPPORTED`. It is the
only filesystem `valkey.fault.disk-fill` fills. A busy unmount is retried,
and the mount stays owned until it is confirmed gone, so a later cleanup or
rollback can still release it. `mount` and `umount` time out after 60
seconds.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `size_mb` | integer, 1 to 65,536 | 64 | |
| `prefix` | string | `valkey-fuzzer-tmpfs-` | |

Output: `path`, the mount point.
