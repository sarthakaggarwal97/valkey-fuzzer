### Added

- `valkey-fuzzer-fetch-porcupine` fetches the prebuilt `porcupine` checker for the host (Linux or macOS, x86_64 or arm64) from the pinned `porcupine-v1.3.1` release over HTTPS, verifies its SHA-256 against `valkey_fuzzer/components/porcupine_sha256sums.txt`, and installs it under `~/.cache/valkey-fuzzer/bin`; `valkey.oracle.history` uses it when `PATH` has no `porcupine`. Go is no longer needed for the common path.
- The `Porcupine checker binaries` workflow cross-builds the checker with `CGO_ENABLED=0 go build -trimpath -buildvcs=false`, fails unless the build reproduces the pinned sums, uploads the binaries and `SHA256SUMS` as an artifact, and on a `porcupine-v*` tag publishes them as release assets.
- `THIRD_PARTY.md` carries the MIT notice of Porcupine; `porcupine/vendor/` holds the module, so `go -C porcupine build` needs no network.

### Changed

- The error for a missing checker names both the fetch command and the `go -C porcupine build` command.
