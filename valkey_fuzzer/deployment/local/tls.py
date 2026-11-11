"""TLS for a deployment (``deployment.tls``): its certificates, and how its servers and clients use them.

A TLS deployment runs every connection over TLS: a server listens on its client port with TLS only (``port 0``,
``tls-port``), replicates over TLS (``tls-replication``) and runs its cluster bus over TLS (``tls-cluster``), and
every client of the fuzzer connects with TLS. The plain-text path is closed, so a client that does not use TLS
fails at once instead of passing over the path under test. A deployment makes its own certificate authority and one
certificate it signs, which every server presents to clients and peers and every client presents to the servers
(``tls-auth-clients`` is on by default).
"""

import functools
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from typing import Any, Optional

# The extensions of the two certificates. Spelled out, so neither the openssl configuration of the host nor the
# version of openssl (1.0.2 to 3.x) changes them, and a strict verifier (Python 3.13's) takes them.
_OPENSSL_CONFIG = """\
[ req ]
distinguished_name = subject
[ subject ]
[ ca ]
basicConstraints = critical, CA:TRUE
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
[ leaf ]
basicConstraints = critical, CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid
subjectAltName = IP:127.0.0.1, DNS:localhost
"""
_DAYS = "3650"
# What a build without TLS prints when a TLS setting asks for it (initListeners in server.c).
_NO_TLS = "Failed finding TLS support"


@dataclass(frozen=True)
class TlsFiles:
    """A deployment's CA certificate, and the certificate and key every server and client presents."""

    ca_cert: str
    cert: str
    key: str

    def server_arguments(self, port: int) -> list[str]:
        """valkey-server arguments that serve ``port`` over TLS only, and replicate and gossip over TLS."""

        return [
            "--port",
            "0",
            "--tls-port",
            str(port),
            "--tls-cert-file",
            self.cert,
            "--tls-key-file",
            self.key,
            "--tls-ca-cert-file",
            self.ca_cert,
            "--tls-replication",
            "yes",
            "--tls-cluster",
            "yes",
        ]

    def client_options(self) -> dict[str, Any]:
        """valkey-py connection options. The certificate names 127.0.0.1 only, and a deployment with per-instance
        addressing binds other loopback addresses, so the host name is not checked; Valkey checks none either."""

        return {
            "ssl": True,
            "ssl_ca_certs": self.ca_cert,
            "ssl_certfile": self.cert,
            "ssl_keyfile": self.key,
            "ssl_check_hostname": False,
        }


def client_options(tls: Optional[TlsFiles]) -> dict[str, Any]:
    """valkey-py connection options for a server of a deployment with ``tls`` (None: plain text)."""

    return tls.client_options() if tls is not None else {}


def create_tls_files(directory: str) -> TlsFiles:
    """A new certificate authority and a certificate it signs, in ``directory``; raises ``RuntimeError`` when
    openssl is missing or fails."""

    os.makedirs(directory, exist_ok=True)
    ca_cert, ca_key, cert, key, request, config = (
        os.path.join(directory, name)
        for name in ("ca.crt", "ca.key", "valkey.crt", "valkey.key", "valkey.csr", "openssl.cnf")
    )
    with open(config, "w") as handle:
        handle.write(_OPENSSL_CONFIG)
    new_key = ["-new", "-nodes", "-newkey", "rsa:2048", "-sha256", "-config", config]
    days = ["-days", _DAYS]
    authority = ["-extensions", "ca", "-subj", "/CN=valkey-fuzzer CA", "-keyout", ca_key, "-out", ca_cert]
    signed_by_it = ["-CA", ca_cert, "-CAkey", ca_key, "-set_serial", "1", "-extfile", config, "-extensions", "leaf"]
    for command in (
        ["req", "-x509", *new_key, *days, *authority],
        ["req", *new_key, "-subj", "/CN=valkey-fuzzer", "-keyout", key, "-out", request],
        ["x509", "-req", "-sha256", *days, "-in", request, "-out", cert, *signed_by_it],
    ):
        try:
            result = subprocess.run(["openssl", *command], capture_output=True, text=True, timeout=60)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise RuntimeError(f"cannot make the deployment's TLS certificates: openssl {command[0]}: {exc}") from None
        if result.returncode != 0:
            raise RuntimeError(
                f"cannot make the deployment's TLS certificates: openssl {command[0]} exited {result.returncode}: "
                f"{result.stderr.strip()[-500:]}"
            )
    return TlsFiles(ca_cert=ca_cert, cert=cert, key=key)


def supports_tls(binary: str) -> bool:
    """Whether valkey-server ``binary`` was built with TLS (``BUILD_TLS=yes``), without starting a server."""

    path = shutil.which(binary) or binary
    stat = os.stat(path)
    return _NO_TLS not in _tls_probe_output(os.path.realpath(path), stat.st_mtime_ns, stat.st_size)


@functools.cache
def _tls_probe_output(path: str, mtime_ns: int, size: int) -> str:
    """What ``path`` prints when TLS replication is asked for without a certificate: a build with TLS exits for
    want of the certificate, one without for want of TLS, both before they listen or load data."""

    with tempfile.TemporaryDirectory(prefix="valkey-tls-probe-") as directory:
        command = [
            path,
            "--port",
            "0",
            "--tls-replication",
            "yes",
            "--save",
            "",
            "--appendonly",
            "no",
            "--dir",
            directory,
        ]
        return subprocess.run(
            command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=30, cwd=directory
        ).stdout
