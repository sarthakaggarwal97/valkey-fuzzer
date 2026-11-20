"""Getting the ``porcupine`` checker without Go: the fetch of the pinned release binary, its SHA-256 check, and the
lookup that prefers PATH and falls back to the cache. No test touches the network: the download is a fake."""

import hashlib
import io
import os
import re
from pathlib import Path

import pytest
import yaml

from valkey_fuzzer.components import porcupine
from valkey_fuzzer.components.porcupine import (
    BUILD_COMMAND,
    FETCH_COMMAND,
    RELEASE_TAG,
    RELEASE_URL,
    SUMS,
    asset_name,
    cache_dir,
    fetch_checker,
    find_checker,
    main,
    pinned_sha256,
)
from valkey_fuzzer.outcomes import EXIT_HARNESS_ERROR, EXIT_SUCCESS

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "porcupine.yml"
BINARY = b"#!/bin/sh\necho porcupine\n"
ASSET = asset_name("Linux", "x86_64")


class _Response(io.BytesIO):
    """What ``urlopen`` returns, as far as the fetch reads it: a stream with the URL it ended at."""

    def __init__(self, body, url):
        super().__init__(body)
        self.url = url

    def geturl(self):
        return self.url


@pytest.fixture
def served(monkeypatch, tmp_path):
    """A fake download: ``served.body`` is what every URL returns; ``served.urls`` the URLs asked for. The host is
    Linux/x86_64, its cache is ``tmp_path/cache``, and the pinned sum is that of ``BINARY``."""

    state = type("Served", (), {"body": BINARY, "urls": [], "final_url": None})()

    def urlopen(url):
        state.urls.append(url)
        return _Response(state.body, state.final_url or url)

    monkeypatch.setattr(porcupine, "urlopen", urlopen)
    monkeypatch.setattr(porcupine.platform, "system", lambda: "Linux")
    monkeypatch.setattr(porcupine.platform, "machine", lambda: "x86_64")
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    sums = tmp_path / "sums.txt"
    sums.write_text(f"{hashlib.sha256(BINARY).hexdigest()}  {ASSET}\n{'0' * 64}  porcupine-darwin-arm64\n")
    monkeypatch.setattr(porcupine, "SUMS", sums)
    return state


# ---------------------------------------------------------------------------
# The fetch
# ---------------------------------------------------------------------------


def test_the_fetch_installs_the_verified_binary_as_an_executable_in_the_directory(served, tmp_path):
    path = fetch_checker(str(tmp_path / "bin"))

    assert path == str(tmp_path / "bin" / "porcupine") and Path(path).read_bytes() == BINARY
    assert os.access(path, os.X_OK)
    assert served.urls == [f"{RELEASE_URL}/{ASSET}"] and RELEASE_URL.startswith("https://github.com/")
    assert os.listdir(tmp_path / "bin") == ["porcupine"], "no temporary file is left beside it"


def test_a_tampered_download_is_rejected_and_nothing_is_installed(served, tmp_path):
    served.body = BINARY + b"echo evil\n"

    with pytest.raises(ValueError, match="not the pinned") as error:
        fetch_checker(str(tmp_path / "bin"))

    assert hashlib.sha256(served.body).hexdigest() in str(error.value) and BUILD_COMMAND in str(error.value)
    assert os.listdir(tmp_path / "bin") == [], "neither the binary nor its temporary file"


def test_a_truncated_download_is_rejected_too(served, tmp_path):
    served.body = BINARY[:-1]
    with pytest.raises(ValueError, match="corrupt or tampered"):
        fetch_checker(str(tmp_path / "bin"))
    assert os.listdir(tmp_path / "bin") == []


def test_a_release_url_that_is_not_https_is_refused_before_any_download(served, monkeypatch, tmp_path):
    monkeypatch.setattr(porcupine, "RELEASE_URL", RELEASE_URL.replace("https://", "http://"))
    with pytest.raises(ValueError, match="refusing to fetch porcupine over http"):
        fetch_checker(str(tmp_path / "bin"))
    assert served.urls == [] and not (tmp_path / "bin" / "porcupine").exists()


def test_a_redirect_away_from_https_is_refused(served, tmp_path):
    served.final_url = "http://mirror.example/porcupine"
    with pytest.raises(ValueError, match=r"refusing to fetch porcupine from where .* redirects"):
        fetch_checker(str(tmp_path / "bin"))
    assert os.listdir(tmp_path / "bin") == []


def test_a_failed_download_names_the_url_and_leaves_no_temporary_file(served, monkeypatch, tmp_path):
    def failing(url):
        raise OSError("HTTP Error 404: Not Found")

    monkeypatch.setattr(porcupine, "urlopen", failing)
    message = f"cannot download {RELEASE_URL}/{ASSET}: HTTP Error 404: Not Found; build"
    with pytest.raises(OSError, match=re.escape(message)):
        fetch_checker(str(tmp_path / "bin"))
    assert os.listdir(tmp_path / "bin") == []


def test_a_platform_without_a_release_binary_is_told_to_build(served, monkeypatch):
    with pytest.raises(
        ValueError,
        match=re.escape(f"no release binary of porcupine for FreeBSD/amd64: build it with `{BUILD_COMMAND}`"),
    ):
        asset_name("FreeBSD", "amd64")
    with pytest.raises(ValueError, match="Linux/riscv64"):
        asset_name("Linux", "riscv64")
    # The fetch of such a host downloads nothing.
    monkeypatch.setattr(porcupine.platform, "machine", lambda: "riscv64")
    with pytest.raises(ValueError, match="no release binary"):
        fetch_checker(cache_dir())
    assert served.urls == []


@pytest.mark.parametrize(
    ("system", "machine", "asset"),
    [
        ("Linux", "x86_64", "porcupine-linux-amd64"),
        ("Linux", "aarch64", "porcupine-linux-arm64"),
        ("Darwin", "x86_64", "porcupine-darwin-amd64"),
        ("Darwin", "arm64", "porcupine-darwin-arm64"),
    ],
)
def test_the_asset_name_is_the_go_platform_name(system, machine, asset):
    assert asset_name(system, machine) == asset


def test_the_pinned_sum_is_read_from_the_sums_file(served):
    assert pinned_sha256(ASSET) == hashlib.sha256(BINARY).hexdigest()
    with pytest.raises(LookupError, match="pins no sum for porcupine-linux-arm64"):
        pinned_sha256("porcupine-linux-arm64")


# ---------------------------------------------------------------------------
# The lookup and the command
# ---------------------------------------------------------------------------


def test_the_checker_on_path_wins_over_the_cached_one_which_wins_over_nothing(served, monkeypatch, tmp_path):
    monkeypatch.setenv("PATH", str(tmp_path / "empty-path"))
    (tmp_path / "empty-path").mkdir()
    with pytest.raises(FileNotFoundError) as error:
        find_checker()
    message = str(error.value)
    assert message.startswith(f"porcupine is not on PATH or in {cache_dir()}: ")
    assert f"fetch the {RELEASE_TAG} release binary ({ASSET}, SHA-256 checked) with `{FETCH_COMMAND}`" in message
    assert f"or build it from a valkey-fuzzer checkout with `{BUILD_COMMAND}`" in message
    assert "Go >= 1.21" in message, "go is not on the empty PATH either"

    cached = fetch_checker(cache_dir())
    assert find_checker() == cached

    on_path = tmp_path / "path" / "porcupine"
    on_path.parent.mkdir()
    on_path.write_bytes(BINARY)
    on_path.chmod(0o755)
    monkeypatch.setenv("PATH", str(on_path.parent))
    assert find_checker() == str(on_path)


def test_a_host_without_a_release_binary_is_told_so_and_to_build(served, monkeypatch, tmp_path):
    monkeypatch.setenv("PATH", str(tmp_path))
    monkeypatch.setattr(porcupine.platform, "system", lambda: "FreeBSD")
    with pytest.raises(
        FileNotFoundError, match=re.escape("no release binary exists for this platform (FreeBSD/x86_64), or build it")
    ):
        find_checker()


def test_the_command_fetches_into_the_cache_and_says_where(served, capsys):
    assert main([]) == EXIT_SUCCESS
    installed = os.path.join(cache_dir(), "porcupine")
    assert capsys.readouterr().out == f"Installed {installed} ({ASSET} of {RELEASE_TAG}, SHA-256 verified)\n"
    assert Path(installed).read_bytes() == BINARY


def test_the_command_fails_with_the_reason_on_a_bad_sum(served, capsys):
    served.body = b"tampered"
    assert main([]) == EXIT_HARNESS_ERROR
    assert re.fullmatch(
        r"Error: the SHA-256 of https://\S+ is \w+, not the pinned \w+: .*\n", capsys.readouterr().err, re.S
    )
    assert not os.path.exists(os.path.join(cache_dir(), "porcupine"))


def test_the_console_script_is_the_fetch_command_and_answers_help():
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert f'{FETCH_COMMAND} = "valkey_fuzzer.components.porcupine:main"' in text
    with pytest.raises(SystemExit) as exit:
        main(["--help"])
    assert exit.value.code == 0


def test_the_cache_directory_follows_xdg_cache_home_then_home(monkeypatch):
    monkeypatch.setenv("XDG_CACHE_HOME", "/xdg")
    assert cache_dir() == "/xdg/valkey-fuzzer/bin"
    monkeypatch.delenv("XDG_CACHE_HOME")
    monkeypatch.setenv("HOME", "/home/someone")
    assert cache_dir() == "/home/someone/.cache/valkey-fuzzer/bin"


# ---------------------------------------------------------------------------
# The pinned sums and the workflow agree
# ---------------------------------------------------------------------------


def _workflow_targets():
    document = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    (script,) = [step["run"] for step in document["jobs"]["build"]["steps"] if "Cross-build" in step.get("name", "")]
    return re.search(r"for target in (.*?); do", script).group(1).split(), document


def test_the_sums_file_pins_one_sum_per_target_the_workflow_builds():
    lines = SUMS.read_text(encoding="utf-8").splitlines()
    pinned = {}
    for line in lines:
        digest, separator, name = line.partition("  ")
        assert separator and re.fullmatch(r"[0-9a-f]{64}", digest), line
        pinned[name] = digest
    targets, _ = _workflow_targets()
    assert sorted(pinned) == sorted(f"porcupine-{target.replace('/', '-')}" for target in targets)
    assert {asset_name(system, machine) for system in ("Linux", "Darwin") for machine in ("x86_64", "arm64")} <= set(
        pinned
    )
    assert len(set(pinned.values())) == len(pinned), "every platform's binary differs"


def test_the_workflow_builds_reproducibly_checks_the_pinned_sums_and_publishes_only_on_a_tag():
    targets, document = _workflow_targets()
    assert targets == ["linux/amd64", "linux/arm64", "darwin/amd64", "darwin/arm64"]
    build = document["jobs"]["build"]
    scripts = "\n".join(step["run"] for step in build["steps"] if "run" in step)
    assert "CGO_ENABLED=0" in scripts and "-trimpath -buildvcs=false" in scripts
    assert "sha256sum porcupine-* > SHA256SUMS" in scripts
    assert f"diff {SUMS.relative_to(ROOT)} dist/SHA256SUMS" in scripts
    (go,) = [step for step in build["steps"] if step.get("uses", "").startswith("actions/setup-go@")]
    assert re.fullmatch(r"1\.\d+\.\d+", str(go["with"]["go-version"])), "an exact toolchain, or the sums drift"
    publish = document["jobs"]["publish"]
    assert publish["if"] == "github.event_name == 'push' && startsWith(github.ref, 'refs/tags/porcupine-v')"
    assert publish["needs"] == "build" and publish["permissions"] == {"contents": "write"}
    assert document["permissions"] == {"contents": "read"}
    triggers = document[True]  # PyYAML reads the bare `on` key as True.
    assert triggers["push"] == {"tags": ["porcupine-v*"]} and "workflow_dispatch" in triggers
    assert RELEASE_TAG.startswith("porcupine-v")
    # The fetch downloads from the release the publish job creates, by the asset names the build job writes.
    assert f"https://github.com/sarthakaggarwal97/valkey-fuzzer/releases/download/{RELEASE_TAG}" == RELEASE_URL
    (script,) = [step for step in publish["steps"] if step.get("uses", "").startswith("actions/github-script@")]
    assert "tag_name: tag" in script["with"]["script"] and "uploadReleaseAsset" in script["with"]["script"]
    assert (
        script["env"]["TAG"] == "${{ github.ref_name }}" and "readdirSync(process.env.DIST)" in script["with"]["script"]
    )


def test_the_vendored_module_is_the_one_go_mod_requires_and_carries_its_license():
    go_mod = (ROOT / "porcupine" / "go.mod").read_text(encoding="utf-8")
    (version,) = re.findall(r"require github\.com/anishathalye/porcupine (v\S+)", go_mod)
    modules = (ROOT / "porcupine" / "vendor" / "modules.txt").read_text(encoding="utf-8")
    assert modules.startswith(f"# github.com/anishathalye/porcupine {version}\n## explicit")
    assert (ROOT / "porcupine" / "vendor" / "github.com" / "anishathalye" / "porcupine" / "LICENSE.md").exists()
    third_party = (ROOT / "THIRD_PARTY.md").read_text(encoding="utf-8")
    assert "The MIT License (MIT)" in third_party and f"version {version.removeprefix('v')}" in third_party
    assert "THIRD_PARTY.md" in (ROOT / "README.md").read_text(encoding="utf-8")
