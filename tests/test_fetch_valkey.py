"""``fetch-valkey``: the command on faked git and make, and one real build of the pinned commit (slow)."""

import os
import shutil
import subprocess
from unittest.mock import patch

import pytest

from tests.conftest import cli
from valkey_fuzzer.deployment.local import processes
from valkey_fuzzer.deployment.local.processes import VALKEY_SOURCE_SHA
from valkey_fuzzer.outcomes import EXIT_HARNESS_ERROR

PROCESSES = "valkey_fuzzer.deployment.local.processes"


def _fake_build(commands):
    def run(command, cwd=None, **options):
        commands.append(command)
        if "rev-parse" in command:
            return subprocess.CompletedProcess(command, 0, f"{VALKEY_SOURCE_SHA}\n", "")
        if command[0] == "make":
            os.makedirs(os.path.join(cwd, "src"))
            open(os.path.join(cwd, "src", "valkey-server"), "w").close()
        return subprocess.CompletedProcess(command, 0, "", "")

    return run


def test_fetch_valkey_builds_into_the_cache_once_and_prints_only_the_binary(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    binary = tmp_path / "cache" / "valkey-fuzzer" / "valkey" / VALKEY_SOURCE_SHA / "src" / "valkey-server"
    commands = []
    with patch(f"{PROCESSES}.subprocess.run", side_effect=_fake_build(commands)), patch(f"{PROCESSES}.fcntl.flock"):
        # Without a ref, the pinned commit: what a deployment without a binary builds on its first run.
        assert cli("fetch-valkey") == 0
        assert capsys.readouterr().out == f"{binary}\n" and binary.is_file()
        assert [command[0] for command in commands] == ["git", "git", "git", "git", "make"]
        # Idempotent: the second call runs nothing and prints the same path.
        assert cli("fetch-valkey", VALKEY_SOURCE_SHA) == 0
        assert capsys.readouterr().out == f"{binary}\n" and len(commands) == 5
    # The deployment's fallback finds what the command built.
    with patch(f"{PROCESSES}.shutil.which", return_value=None):
        assert processes.installed_valkey_binary() == str(binary)


def test_a_failed_fetch_is_a_harness_error_that_names_the_build_log(tmp_path, monkeypatch, capsys, caplog):
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))

    def run(command, **options):
        raise subprocess.CalledProcessError(128, command)

    with patch(f"{PROCESSES}.subprocess.run", side_effect=run), patch(f"{PROCESSES}.fcntl.flock"):
        assert cli("fetch-valkey", VALKEY_SOURCE_SHA) == EXIT_HARNESS_ERROR
    assert capsys.readouterr().out == ""
    log = tmp_path / "cache" / "valkey-fuzzer" / "valkey" / f"{VALKEY_SOURCE_SHA}.build.log"
    assert f"The Valkey build failed; its output is in {log}" in caplog.text
    assert not (log.parent / VALKEY_SOURCE_SHA).exists()


@pytest.mark.slow
def test_fetch_valkey_builds_the_pinned_commit_for_real_and_the_build_carries_its_commit(tmp_path):
    for tool in ("git", "make", os.environ.get("CC") or "cc"):
        if not shutil.which(tool):
            pytest.skip(f"{tool} is not installed")
    sha, binary = processes.fetch_valkey(VALKEY_SOURCE_SHA, cache=str(tmp_path))

    assert sha == VALKEY_SOURCE_SHA and binary == str(tmp_path / sha / "src" / "valkey-server")
    assert os.access(binary, os.X_OK) and os.path.isfile(os.path.join(tmp_path, sha, "src", "config.c"))
    # The provenance pin: `valkey-server --version` names the commit, as the report's reproducer.valkey_sha records.
    assert VALKEY_SOURCE_SHA.startswith(processes.detect_valkey_sha(binary))
    assert f"valkey commit {sha[:12]} is checked out" in (tmp_path / f"{sha}.build.log").read_text()
    with patch(f"{PROCESSES}.subprocess.run", side_effect=AssertionError("built again")):
        assert processes.fetch_valkey(VALKEY_SOURCE_SHA, cache=str(tmp_path)) == (sha, binary)
