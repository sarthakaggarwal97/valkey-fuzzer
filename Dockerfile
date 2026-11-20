# The fuzzer, a pinned Valkey build, the porcupine checker, and the tools the faults need, in one image, so a CI
# job or a laptop runs an experiment without building anything (docs/module-repos.md).
#
#   docker build -t valkey-fuzzer .                      # Valkey 9.1.3, the version the fuzzer builds itself
#   docker build --build-arg VALKEY_VERSION=unstable .   # any tag, branch or commit of valkey-io/valkey
#   docker run --rm -v "$PWD:/work" valkey-fuzzer run /work/scenario.yaml
#
# The default user is not root: process signals, the clock skew (libfaketime), the syscall faults (cc) and the
# permission faults run as it. The network partition (iptables) and the network degradation (tc netem) need
# root and CAP_NET_ADMIN (`docker run --user root --cap-add NET_ADMIN ...`), and a size-limited tmpfs needs
# root and CAP_SYS_ADMIN, since they change the container's own network and mounts. The resource-pressure fault
# (cgroup v2) is not supported in a container. The fuzzer never asks for sudo; a fault it cannot run makes the
# run SKIP_UNSUPPORTED or ERROR, never FAIL (docs/contracts.md).
#
# .github/workflows/build-image.yml builds this on every pull request that touches it, and pushes it to
# ghcr.io/<owner>/valkey-fuzzer:<tag> on a tag.

ARG PYTHON_IMAGE=python:3.12-slim-bookworm

# --- Valkey at a pinned version: the server, its tools, and the config.c the fuzzer reads the config table from.
FROM debian:bookworm-slim AS valkey
ARG VALKEY_VERSION=9.1.3
ARG VALKEY_REPOSITORY=https://github.com/valkey-io/valkey.git
RUN apt-get update -q && apt-get install -y -q --no-install-recommends build-essential git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
# A branch or tag clones shallow; a commit SHA is fetched into an empty repository.
RUN git init -q /src/valkey \
    && git -C /src/valkey fetch -q --depth 1 "${VALKEY_REPOSITORY}" "${VALKEY_VERSION}" \
    && git -C /src/valkey checkout -q FETCH_HEAD \
    && make -C /src/valkey -j"$(nproc)" \
    && mkdir -p /opt/valkey/src \
    && cp /src/valkey/src/valkey-server /src/valkey/src/valkey-cli /src/valkey/src/valkey-benchmark \
          /src/valkey/src/config.c /opt/valkey/src/ \
    # valkey-check-rdb and valkey-check-aof are valkey-server by another name (make copies it; 19 MB each).
    && ln -s valkey-server /opt/valkey/src/valkey-check-rdb && ln -s valkey-server /opt/valkey/src/valkey-check-aof \
    && git -C /src/valkey rev-parse HEAD > /opt/valkey/COMMIT

# --- The porcupine linearizability checker, which valkey.oracle.history runs from PATH.
FROM golang:1.23-bookworm AS porcupine
# A host whose network rewrites TLS (a corporate proxy) builds with --build-arg GOPROXY=direct GOSUMDB=off.
ARG GOPROXY
ARG GOSUMDB
COPY porcupine /src/porcupine
RUN CGO_ENABLED=0 go -C /src/porcupine build -trimpath -o /out/porcupine

# --- The fuzzer, with what its faults call: iptables (partitions), tc and ip (degradations), libfaketime
# (clock skew), a C compiler (the syscall fault library is built at run time), mount (tmpfs), ps (sweep).
FROM ${PYTHON_IMAGE}
ARG SOURCE_URL=https://github.com/valkey-io/valkey-fuzzer
LABEL org.opencontainers.image.source="${SOURCE_URL}" \
      org.opencontainers.image.description="Valkey fuzzer with a pinned Valkey build, porcupine, iptables, tc and libfaketime" \
      org.opencontainers.image.licenses="BSD-3-Clause"
RUN apt-get update -q && apt-get install -y -q --no-install-recommends \
        iptables iproute2 libfaketime gcc libc6-dev procps ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=valkey /opt/valkey /opt/valkey
COPY --from=porcupine /out/porcupine /usr/local/bin/porcupine
# On PATH through a link, so the fuzzer finds the config.c beside the real binary.
RUN for tool in valkey-server valkey-cli valkey-benchmark valkey-check-rdb valkey-check-aof; do \
        ln -s "/opt/valkey/src/${tool}" "/usr/local/bin/${tool}"; done
COPY pyproject.toml README.md LICENSE /opt/valkey-fuzzer/
COPY valkey_fuzzer /opt/valkey-fuzzer/valkey_fuzzer
COPY formal /opt/valkey-fuzzer/formal
COPY examples /opt/valkey-fuzzer/examples
RUN python -m pip install --no-cache-dir --root-user-action ignore /opt/valkey-fuzzer \
    && rm -rf /opt/valkey-fuzzer/valkey_fuzzer /opt/valkey-fuzzer/formal /opt/valkey-fuzzer/pyproject.toml \
           /opt/valkey-fuzzer/build /opt/valkey-fuzzer/valkey_fuzzer.egg-info \
    && useradd --create-home --uid 1000 fuzzer \
    && mkdir -p /work && chown fuzzer:fuzzer /work
USER 1000:1000
WORKDIR /work
# Report folders and the server data directories go under /tmp, as on a host.
ENTRYPOINT ["valkey-fuzzer"]
CMD ["--help"]
