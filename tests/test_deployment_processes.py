"""The local deployment's servers: their commands, spawning, restarts, ports, formation, output, and cleanup."""

import logging
import os
import re
import signal
import socket
import subprocess
import sys
import time
from contextlib import contextmanager, nullcontext
from pathlib import Path
from unittest.mock import Mock, call, patch

import pytest
import valkey

from tests.deployment_support import (
    PROCESSES,
    FakeDeployment,
    client_context,
    cluster_nodes_line,
    cluster_nodes_text,
    fake_client,
    formed_cluster,
    instance,
    process_manager,
)
from tests.support import VirtualClock, fake_executable, wait_until
from valkey_fuzzer import deadlines
from valkey_fuzzer.commands.run import cleanup_logs
from valkey_fuzzer.components.capabilities import resolve_binary
from valkey_fuzzer.components.server import PrivateServer
from valkey_fuzzer.deadlines import WaitInterruptedError
from valkey_fuzzer.deployment.local import cluster_nodes, processes
from valkey_fuzzer.deployment.local.formation import SETTLE_TIMEOUT_S, Formation
from valkey_fuzzer.deployment.local.models import DeploymentMode, LocalDeploymentError
from valkey_fuzzer.deployment.local.planner import plan_deployment
from valkey_fuzzer.deployment.local.ports import PortUnavailableError
from valkey_fuzzer.deployment.local.processes import VALKEY_SOURCE_SHA, InstanceIdentityError, installed_valkey_binary
from valkey_fuzzer.deployment.local.queries import parse_info_text, valkey_client
from valkey_fuzzer.deployment.local.server_config import DEFAULT_MAXMEMORY, arguments_for_build


@pytest.fixture
def clock(monkeypatch):
    """Time that passes only as the code under test waits: retries and timeouts take no real time."""

    return VirtualClock().install(monkeypatch, deadlines)


def _running(pid):
    return Mock(pid=pid, **{"poll.return_value": None})


def _replica_info(port=7000, link="up", pid=999):
    return dict(process_id=pid, role="slave", master_host="127.0.0.1", master_port=port, master_link_status=link)


@pytest.mark.parametrize("mode", list(DeploymentMode))
def test_the_instance_command_carries_the_mode_the_config_and_the_address(mode):
    manager = process_manager(mode=mode, valkey_binary="/default")

    command = manager.build_instance_command(7000, "/d", "/l", {"loglevel": "debug", "replica-read-only": False})
    isolated = manager.build_instance_command(7000, "/d", "/l", {"maxmemory": "2gb"}, "/node", "127.0.0.3")

    def arg(option):
        return command[command.index(option) + 1]

    assert command[0] == "/default" and isolated[0] == "/node"
    assert arg("--cluster-enabled") == ("yes" if mode is DeploymentMode.CLUSTER else "no")
    assert command[-4:] == ["--loglevel", "debug", "--replica-read-only", "no"]
    # A memory cap but no hidden eviction policy; the scenario's comes last, and Valkey applies the last.
    assert arg("--maxmemory") == DEFAULT_MAXMEMORY and "--maxmemory-policy" not in command
    assert isolated[-2:] == ["--maxmemory", "2gb"] and "--bind-source-addr" not in command
    assert isolated[isolated.index("--bind-source-addr") + 1] == "127.0.0.3"


def test_a_build_starts_without_an_unobservable_config_it_lacks_and_with_another_value_for_one_it_refuses(tmp_path):
    # A build that lacks io-threads-always-active and has no jemalloc defrag hints, answering the config probe as
    # Valkey does: it names the first directive it rejects, else the bad port after the probed one.
    binary = fake_executable(
        tmp_path / "valkey-server",
        r"""case "$1 $2" in
  "--io-threads-always-active "*) echo ">>> 'io-threads-always-active \"$2\"'"; echo "Bad directive";;
  "--activedefrag yes") echo ">>> 'activedefrag \"yes\"'"; echo "-DISABLED Active defragmentation";;
  *) echo ">>> 'port \"-1\"'";;
esac
exit 1""",
    )
    config = {"io-threads-always-active": "yes", "activedefrag": "yes", "io-threads": "4", "lfu-log-factor": "20"}
    command = process_manager(valkey_binary=str(binary)).build_instance_command(7000, "/d", "/l", config)
    # A config that is not unobservable stays as written, so a build that refuses it does not start.
    assert command[-6:] == ["--activedefrag", "no", "--io-threads", "4", "--lfu-log-factor", "20"]
    assert "--io-threads-always-active" not in command
    # A build that takes the value starts with it.
    taken = ["--activedefrag", "no", "--prefetch-batch-max-size", "0"]
    assert arguments_for_build(str(binary), taken) == taken


def test_a_build_older_than_the_debug_assertions_starts_without_them(tmp_path):
    # Valkey 7.2 answers the config probe for enable-debug-assert (8.0's) with "Bad directive".
    binary = fake_executable(
        tmp_path / "valkey-server",
        r"""case "$1" in
  --enable-debug-assert) echo ">>> 'enable-debug-assert \"$2\"'"; echo "Bad directive";;
  *) echo ">>> 'port \"-1\"'";;
esac
exit 1""",
    )
    arguments = ["--crash-memcheck-enabled", "no", "--enable-debug-assert", "yes"]
    assert arguments_for_build(str(binary), arguments) == ["--crash-memcheck-enabled", "no"]


def test_a_build_older_than_dual_channel_replication_starts_without_it(tmp_path):
    # valkey.pool.replication draws dual-channel-replication-enabled (8.0's): a build from before it answered
    # the config file with FATAL CONFIG FILE ERROR and the test was skipped whole (fuzzer-walk, valkey #490).
    binary = fake_executable(
        tmp_path / "valkey-server",
        r"""case "$1" in
  --dual-channel-replication-enabled) echo ">>> 'dual-channel-replication-enabled \"$2\"'"; echo "Bad directive";;
  *) echo ">>> 'port \"-1\"'";;
esac
exit 1""",
    )
    arguments = ["--repl-diskless-sync", "yes", "--dual-channel-replication-enabled", "yes"]
    assert arguments_for_build(str(binary), arguments) == ["--repl-diskless-sync", "yes"]


@pytest.mark.parametrize(
    ("failure", "error"),
    [
        ("second-spawn", OSError),
        ("foreign-server", InstanceIdentityError),  # another server answers on the port of the one just started
        ("interrupted-waiting", KeyboardInterrupt),
        ("interrupted-untracked", KeyboardInterrupt),  # after the process started, before the manager tracks it
    ],
)
def test_a_failed_spawn_stops_the_started_servers_and_removes_their_data(tmp_path, failure, error):
    manager = process_manager(tmp_path, replicas_per_primary=1)
    first = _running(101)
    answers = {
        "foreign-server": {"return_value": client_context(fake_client({"process_id": 4242}))},
        "interrupted-waiting": {"side_effect": KeyboardInterrupt()},
    }
    untracked = failure == "interrupted-untracked"

    with (
        patch("valkey_fuzzer.deployment.local.ports.port_unavailable_reason", return_value=None),
        patch(
            f"{PROCESSES}.subprocess.Popen",
            side_effect=[first, error("x") if failure == "second-spawn" else _running(102)],
        ),
        patch(f"{PROCESSES}.valkey_client", **answers.get(failure, {"return_value": client_context(fake_client())})),
        patch(f"{PROCESSES}.InstanceInfo", side_effect=KeyboardInterrupt()) if untracked else nullcontext(),
        pytest.raises(error),
    ):
        manager.spawn_all(plan_deployment(manager.config))

    first.terminate.assert_called_once()
    assert manager.spawned_instances == []
    assert not (tmp_path / f"deployment-{manager.deployment_id}").exists()


def test_a_server_that_exits_before_its_first_ping_is_reported_with_its_exit_code_and_last_lines(tmp_path):
    manager = process_manager(tmp_path)
    script = "import sys\nfor n in range(8): print(f'line {n}', file=sys.stderr)\nsys.exit(3)"
    started = manager._start_process

    def start(cmd, log_file, env=None):
        return started([sys.executable, "-c", script], log_file, env)

    with (
        patch("valkey_fuzzer.deployment.local.ports.port_unavailable_reason", return_value=None),
        patch.object(manager, "_start_process", side_effect=start),
        patch.object(manager, "_answers", return_value=False),
        pytest.raises(LocalDeploymentError) as raised,
    ):
        manager.spawn_all(plan_deployment(manager.config))

    message = str(raised.value)
    assert re.search(r"valkey-0 exited with code 3 after \d+\.\d+s", message) and "within" not in message
    # The last five lines, not the first three.
    assert "line 7" in message and "line 3" in message and "line 2" not in message


def test_binaries_must_exist(tmp_path):
    binary = fake_executable(tmp_path / "valkey-server")
    replica_binary = {"replica": str(tmp_path / "missing")}
    manager = process_manager(
        tmp_path, valkey_binary=str(binary), replicas_per_primary=1, instance_binaries=replica_binary
    )

    assert manager.resolve_valkey_binary() == str(binary)
    with pytest.raises(FileNotFoundError, match=r"instance_binaries for valkey-1"):
        manager.resolve_instance_binaries(plan_deployment(manager.config))
    with pytest.raises(FileNotFoundError, match="missing-valkey-server"):
        process_manager(tmp_path, valkey_binary=str(tmp_path / "missing-valkey-server")).resolve_valkey_binary()


def test_components_find_valkey_server_and_read_slot_ranges_as_the_deployment_does(tmp_path, monkeypatch):
    binary = tmp_path / "valkey-server"
    binary.write_text("#!/bin/sh\n")
    with pytest.raises(PermissionError, match="probe binary"):
        processes.resolve_binary_path(str(binary), "probe binary")
    binary.chmod(0o755)
    assert processes.resolve_binary_path(str(binary)) == str(binary)
    # The cached build of the pinned commit is the one a deployment without a binary falls back to, under
    # $XDG_CACHE_HOME (else ~/.cache), and only once it is built.
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    built = tmp_path / "cache" / "valkey-fuzzer" / "valkey" / VALKEY_SOURCE_SHA / "src" / "valkey-server"
    assert processes.cached_valkey_binary(VALKEY_SOURCE_SHA) == str(built)
    with patch(f"{PROCESSES}.shutil.which", return_value=None):
        assert installed_valkey_binary() is None
        with pytest.raises(FileNotFoundError, match="not on PATH"):
            resolve_binary("")
        built.parent.mkdir(parents=True)
        built.write_text("#!/bin/sh\n")
        assert installed_valkey_binary() == str(built), "the source build the deployment falls back to"
        assert resolve_binary("") == str(built), "a capability probe of the default binary finds the same build"
    assert cluster_nodes.parse_slot_ranges(["0-5", "7", "[8->-abc]"]) == [(0, 5), (7, 7)]
    assert cluster_nodes.parse_slots_in_flight(["0-5", "[8->-abc]", "[9-<-def]", "[x->-abc]"]) == {8, 9}


def test_the_one_info_parser_reads_a_value_with_a_space_whole():
    text = "# Server\r\nexecutable:/opt/valkey server/valkey-server\r\nrole:master\r\n\r\n"
    fields = {"executable": "/opt/valkey server/valkey-server", "role": "master"}
    assert parse_info_text(text) == fields
    assert parse_info_text(text.encode()) == fields and parse_info_text("") == {}
    assert parse_info_text({"cluster_state": "ok", "cluster_known_nodes": 6}) == {
        "cluster_state": "ok",
        "cluster_known_nodes": "6",
    }


def test_slot_ranges_merge_overlap_and_format_as_cluster_nodes_shows_them():
    assert cluster_nodes.merge_slot_ranges([(5, 9), (0, 4), (0, 2), (11, 11)]) == [(0, 9), (11, 11)]
    # Two primaries' ranges overlap on 5-9 and 20; a's own ranges overlapping each other are no split.
    shared = cluster_nodes.shared_slots({"a": [(0, 9), (3, 20)], "b": [(5, 9), (20, 25)], "c": [(30, 40)]})
    assert shared == {**{slot: {"a", "b"} for slot in range(5, 10)}, 20: {"a", "b"}}
    assert cluster_nodes.format_slot_ranges([(0, 5460), (5462, 5462)]) == "0-5460, 5462"
    assert cluster_nodes.group_slots_into_ranges([5462, 0, 1, 2]) == "0-2, 5462"
    assert cluster_nodes.group_slots_into_ranges([]) == ""
    assert (
        cluster_nodes.group_slots_into_ranges(range(0, 24, 2))
        == "0, 2, 4, 6, 8, 10, 12, 14, 16, 18... (12 ranges total)"
    )


def _fake_build(commands, timeouts, outputs, resolved=None, listing=""):
    """A ``subprocess.run`` that records the git and make commands of a build, and fakes their results: ``git
    ls-remote`` answers ``listing``, ``rev-parse`` answers ``resolved``, and ``make`` leaves ``src/valkey-server``
    in its directory."""

    def run(command, cwd=None, check=False, timeout=None, stdout=None, stderr=None, capture_output=False, text=False):
        commands.append((command, cwd))
        if "ls-remote" in command:
            return subprocess.CompletedProcess(command, 0, listing, "")
        if "rev-parse" in command:
            return subprocess.CompletedProcess(command, 0, f"{resolved}\n", "")
        timeouts.append(timeout)
        outputs.append((stdout.name, stderr))
        if command[0] == "make":
            binary = os.path.join(cwd, "src", "valkey-server")
            os.makedirs(os.path.dirname(binary))
            open(binary, "w").close()

    return run


def test_the_source_build_is_pinned_parallel_and_built_aside_under_a_lock(tmp_path, caplog):
    # Concurrent batch workers without a binary must not race on one tree, or build another commit than the pin.
    commands, timeouts, outputs = [], [], []
    cache = tmp_path / "cache"
    manager = process_manager(tmp_path, valkey_binary="")
    with (
        patch(f"{PROCESSES}.shutil.which", return_value=None),
        patch(f"{PROCESSES}.subprocess.run", side_effect=_fake_build(commands, timeouts, outputs, VALKEY_SOURCE_SHA)),
        patch(f"{PROCESSES}.fcntl.flock") as flock,
        caplog.at_level(logging.INFO),
    ):
        binary = manager.resolve_valkey_binary(build_timeout=600.0, cache=str(cache))

    (init, _), (fetch, _), (checkout, _), (resolve, _), (make, make_dir) = commands
    work = init[2]
    # The pinned commit itself is fetched into an empty repository: a moved tag changes nothing.
    assert init == ["git", "-C", work, "init", "--quiet"] and work.startswith(str(cache / VALKEY_SOURCE_SHA))
    assert fetch == [
        "git",
        "-C",
        work,
        "fetch",
        "--quiet",
        "--depth",
        "1",
        processes.VALKEY_REPOSITORY,
        VALKEY_SOURCE_SHA,
    ]
    assert checkout == ["git", "-C", work, "checkout", "--quiet", "--detach", "FETCH_HEAD"]
    assert resolve == ["git", "-C", work, "rev-parse", "HEAD"]
    assert make[0] == "make" and make[1].startswith("-j")
    assert make_dir == work != str(cache / VALKEY_SOURCE_SHA), "built aside, then renamed into place"
    assert binary == str(cache / VALKEY_SOURCE_SHA / "src" / "valkey-server") and os.path.isfile(binary)
    assert flock.call_count == 2  # taken, then released
    # A hung fetch or make cannot hold the run past its deadline.
    assert all(0 < timeout <= 600.0 for timeout in timeouts) and len(timeouts) == 4
    # The git and make output goes to one file beside the build, not the terminal, and one line says where.
    build_log = str(cache / f"{VALKEY_SOURCE_SHA}.build.log")
    assert outputs == [(build_log, subprocess.STDOUT)] * 4 and os.path.isfile(build_log)
    assert f"valkey commit {VALKEY_SOURCE_SHA[:12]} is checked out" in Path(build_log).read_text()
    assert [record.getMessage() for record in caplog.records if "Valkey" in record.getMessage()] == [
        f"Building Valkey commit {VALKEY_SOURCE_SHA[:12]} into {cache / VALKEY_SOURCE_SHA} (about a minute; once, "
        f"then reused); output: {build_log}"
    ]

    # Built once: the next resolution runs nothing and says it uses the cache.
    caplog.clear()
    with (
        patch(f"{PROCESSES}.shutil.which", return_value=None),
        patch(f"{PROCESSES}.subprocess.run", side_effect=AssertionError("built again")),
        caplog.at_level(logging.INFO),
    ):
        assert manager.resolve_valkey_binary(cache=str(cache)) == binary
    assert [record.getMessage() for record in caplog.records] == [
        f"Using the cached Valkey build of commit {VALKEY_SOURCE_SHA[:12]}: {binary}"
    ]


def test_a_tag_or_branch_is_resolved_to_its_commit_before_the_cache_is_looked_in(tmp_path):
    commit, tag_object = "a" * 40, "b" * 40
    listing = f"{tag_object}\trefs/tags/9.9.9\n{commit}\trefs/tags/9.9.9^{{}}\n"
    commands, timeouts, outputs = [], [], []
    with (
        patch(f"{PROCESSES}.subprocess.run", side_effect=_fake_build(commands, timeouts, outputs, commit, listing)),
        patch(f"{PROCESSES}.fcntl.flock"),
    ):
        assert processes.fetch_valkey("9.9.9", cache=str(tmp_path)) == (
            commit,
            str(tmp_path / commit / "src" / "valkey-server"),
        )
        # An annotated tag resolves to the commit it tags, not the tag object; the second fetch finds the cache.
        assert commands[0][0] == [
            "git",
            "ls-remote",
            processes.VALKEY_REPOSITORY,
            "refs/tags/9.9.9",
            "refs/tags/9.9.9^{}",
            "refs/heads/9.9.9",
        ]
        assert commands[2][0][-1] == commit
        assert processes.fetch_valkey("9.9.9", cache=str(tmp_path))[0] == commit and len(commands) == 7
        # A full commit hash is fetched as it is; a short one, or a name the repository lacks, is refused.
        assert processes.resolve_valkey_ref("c" * 40) == "c" * 40 and len(commands) == 7
        with pytest.raises(LocalDeploymentError, match="has no tag or branch 'nope'"):
            processes.resolve_valkey_ref("nope")
        with pytest.raises(LocalDeploymentError, match="has no tag or branch 'abc1234'"):
            processes.resolve_valkey_ref("abc1234")
    # A branch of the same name as a tag loses to the tag.
    both = f"{commit}\trefs/heads/x\n{tag_object}\trefs/tags/x\n"
    with patch(f"{PROCESSES}.subprocess.run", side_effect=_fake_build([], [], [], listing=both)):
        assert processes.resolve_valkey_ref("x") == tag_object


def test_a_fetch_that_checks_out_another_commit_than_asked_is_refused(tmp_path):
    def run(command, cwd=None, **options):
        if "rev-parse" in command:
            return subprocess.CompletedProcess(command, 0, "f" * 40 + "\n", "")
        return None

    with (
        patch(f"{PROCESSES}.subprocess.run", side_effect=run),
        patch(f"{PROCESSES}.fcntl.flock"),
        pytest.raises(
            LocalDeploymentError, match=rf"the fetch of Valkey commit {VALKEY_SOURCE_SHA} checked out {'f' * 40}"
        ),
    ):
        processes.fetch_valkey(VALKEY_SOURCE_SHA, cache=str(tmp_path / "cache"))
    # Nothing half-built is left where a later build would find it: only the log and the lock remain.
    assert not (tmp_path / "cache" / VALKEY_SOURCE_SHA).exists()
    assert sorted(item.name for item in (tmp_path / "cache").iterdir()) == [".lock", f"{VALKEY_SOURCE_SHA}.build.log"]


def test_spawn_and_formation_waits_end_at_the_runs_deadline(tmp_path, clock):
    cancelled = Mock(deadline=None, **{"sleep.return_value": False})
    manager = process_manager(tmp_path)
    with (
        patch.object(manager, "_start_process", return_value=_running(4242)),
        patch.object(manager, "_answers", side_effect=valkey.ConnectionError("not yet")),
        pytest.raises(WaitInterruptedError, match="cancelled"),
    ):
        manager.spawn_all(plan_deployment(manager.config), cancelled)
    # Formation does not mistake the interruption for a formation failure.
    formation = Formation(cancelled)
    with (
        patch.object(formation, "_standalone_problem", return_value="not yet"),
        pytest.raises(WaitInterruptedError, match="cancelled"),
    ):
        formation.form([instance()], mode=DeploymentMode.STANDALONE)


@pytest.mark.parametrize(
    ("answers", "sleeps"),
    [
        ([True], []),
        ([valkey.ConnectionError("not yet"), valkey.ConnectionError("not yet"), True], [0.05, 0.05]),
    ],
    ids=["ready-at-the-first-check", "ready-at-the-third"],
)
def test_a_spawned_or_restarted_server_is_ready_the_moment_it_answers_and_is_asked_every_50_ms(
    tmp_path, answers, sleeps
):
    """A server binds a few milliseconds after Popen: the first check misses it, and the poll interval, not the
    server, used to set how long a provision or a restart took (0.5 s each)."""

    context = Mock(deadline=None, **{"sleep.return_value": True})
    manager = process_manager(tmp_path)
    with (
        patch.object(manager, "_start_process", return_value=_running(4242)),
        patch.object(manager, "_answers", side_effect=list(answers)),
    ):
        manager.spawn_all(plan_deployment(manager.config), context)
    assert [item.args[0] for item in context.sleep.call_args_list] == sleeps

    manager, replica = _pair(tmp_path)
    context.sleep.reset_mock()
    with (
        patch(f"{PROCESSES}.subprocess.Popen", return_value=_running(999)),
        patch.object(manager, "ensure_ports_available"),
        patch.object(manager, "_ready_after_restart", side_effect=[answer is True for answer in answers]),
    ):
        manager.restart_instance(replica, wait_ready=True, context=context)
    assert [item.args[0] for item in context.sleep.call_args_list] == sleeps


def test_a_private_servers_start_wait_ends_with_its_call(clock):
    # A cancelled run waited out the private server's own timeout (F30, 15 s here): its wait is the harness's.
    never_answers = Mock(**{"poll.return_value": None})
    server = PrivateServer("/usr/bin/valkey-server", timeout_s=15.0)
    try:
        with patch("valkey_fuzzer.components.server.subprocess.Popen", return_value=never_answers):
            with pytest.raises(WaitInterruptedError, match="cancelled while waiting for the private server"):
                server.start(Mock(deadline=None, **{"sleep.return_value": False}))
            # The run's deadline ends the wait as the server's own timeout would.
            with pytest.raises(TimeoutError, match=r"did not become ready within 15\.0s"):
                server.start(Mock(deadline=time.monotonic(), **{"sleep.return_value": True}))
    finally:
        server.stop()


def test_a_server_whose_termination_failed_keeps_its_data_and_a_retry_stops_it(tmp_path):
    manager = process_manager(tmp_path, mode=DeploymentMode.CLUSTER, primaries=3)
    stuck, other = instance("valkey-0"), instance("valkey-1", port=7001)
    stuck.process.terminate.side_effect = [PermissionError("not permitted"), None]
    manager._spawned_instances = {"valkey-0": stuck, "valkey-1": other}
    data_dir = tmp_path / f"deployment-{manager.deployment_id}"
    data_dir.mkdir()

    with pytest.raises(RuntimeError, match="completed with failures"):
        manager.cleanup_deployment(manager.spawned_instances)

    # Every server was attempted; the one that may still run stays owned, with its data.
    other.process.terminate.assert_called_once()
    assert manager.spawned_instances == [stuck] and data_dir.exists()
    manager.cleanup_deployment(manager.spawned_instances)
    assert manager.spawned_instances == [] and not data_dir.exists()


def test_a_kept_deployment_whose_tag_cannot_be_removed_is_reported_like_any_cleanup_failure(tmp_path):
    manager = process_manager(tmp_path, enable_cleanup=False)
    server = instance()
    manager._spawned_instances = {"valkey-0": server}
    with patch(f"{PROCESSES}.untag_directory", side_effect=PermissionError("read-only")):
        # A failed spawn's cleanup must not replace the spawn's own error with its own.
        manager._cleanup_failed_spawn([server])
        server.process.terminate.assert_called_once()
        with pytest.raises(RuntimeError, match="completed with failures: untag"):
            manager.cleanup_deployment([server])


# A server that refuses SIGTERM, as a replica writing its first AOF does, and has forked a child.
_FORKING_SERVER = """
import os, signal, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
child = os.fork()
if child == 0:
    time.sleep(60)
    os._exit(0)
print(child, flush=True)
time.sleep(60)
"""


def _exited(pid):
    # A zombie has exited too, whoever is left to reap it.
    status = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    return not status or status.startswith("Z")


def test_a_server_killed_at_cleanup_takes_the_child_it_forked_along():
    # Issue #34: a SIGKILLed server's AOF rewrite child kept running past the run.
    server = subprocess.Popen([sys.executable, "-c", _FORKING_SERVER], stdout=subprocess.PIPE, text=True)
    child = int(server.stdout.readline())
    try:
        processes.kill_server(server)
        assert server.wait(timeout=10) == -signal.SIGKILL
        assert wait_until(lambda: _exited(child), 10)
    finally:
        server.kill()
        server.wait()
        server.stdout.close()
        try:
            os.kill(child, signal.SIGKILL)
        except ProcessLookupError:
            pass


@contextmanager
def _shell_with_a_child():
    # A shell whose one child is a sleep: echo and wait are builtins.
    shell = subprocess.Popen(["sh", "-c", "sleep 60 & echo $!; wait"], stdout=subprocess.PIPE, text=True)
    child = int(shell.stdout.readline())
    try:
        yield shell, child
    finally:
        for pid in (child, shell.pid):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        shell.wait()
        shell.stdout.close()


@pytest.mark.skipif(
    not os.path.exists(f"/proc/{os.getpid()}/task/{os.getpid()}/children"), reason="needs /proc children"
)
def test_a_servers_children_are_read_from_proc_without_a_process_listing():
    with _shell_with_a_child() as (shell, child), patch(f"{PROCESSES}.run_bounded") as ps:
        assert processes._children(shell.pid) == [child]
    ps.assert_not_called()


def test_a_servers_children_come_from_a_process_listing_where_proc_has_none():
    with (
        _shell_with_a_child() as (shell, child),
        patch(f"{PROCESSES}._proc_children", side_effect=FileNotFoundError),
    ):
        assert processes._children(shell.pid) == [child]


def test_a_server_that_refuses_sigterm_is_killed_with_its_children():
    server = Mock(**{"wait.side_effect": [subprocess.TimeoutExpired("valkey-server", 5), -9]})
    with patch(f"{PROCESSES}.kill_server") as kill_server:
        processes._stop_process(server)
    kill_server.assert_called_once_with(server)
    server.kill.assert_not_called()


def test_an_interrupt_while_a_server_stops_kills_it_and_its_children():
    # The interrupt moves on to the next instance: a server that saw only the SIGTERM would keep running.
    server = Mock(**{"wait.side_effect": KeyboardInterrupt(), "poll.return_value": None})
    with patch(f"{PROCESSES}.kill_server") as kill_server, pytest.raises(KeyboardInterrupt):
        processes._stop_process(server)
    kill_server.assert_called_once_with(server)


def test_a_relative_base_data_dir_gives_absolute_paths(tmp_path, monkeypatch):
    # Each server runs in its own data directory: a relative log path made it refuse to start.
    monkeypatch.chdir(tmp_path)
    data_dir, log_file = process_manager(base_data_dir="runs/data", seed=7).create_instance_directories("valkey-0")
    assert os.path.isabs(data_dir) and os.path.isabs(log_file)
    assert log_file.startswith(str(tmp_path / "runs" / "data" / "logs"))


def test_two_deployments_with_the_same_seed_write_separate_logs_that_cleanup_finds(tmp_path):
    # Concurrent runs of one seed must not interleave their instance logs: log checks would read the other's lines.
    paths = [process_manager(tmp_path, seed=7).create_instance_directories("valkey-0")[1] for _ in range(2)]
    assert paths[0] != paths[1]
    for path in paths:
        open(path, "w").close()
    # While this process owns the deployments, cleanup keeps their logs; untagged, they are finished ones.
    assert cleanup_logs(7, str(tmp_path), force=True) == 0 and all(os.path.exists(path) for path in paths)
    for directory in tmp_path.glob("deployment-*"):
        (directory / ".valkey-fuzzer-owner").unlink()
    assert cleanup_logs(7, str(tmp_path), force=True) == 0
    assert not any(os.path.exists(path) for path in paths)


def test_process_output_goes_to_one_file_per_instance_that_a_startup_failure_reports(tmp_path):
    manager = process_manager(tmp_path)
    log_file = str(tmp_path / "1-valkey-0.log")
    for marker in ("first", "second"):
        process = manager._start_process(
            [sys.executable, "-c", f"import sys; print({marker!r}, file=sys.stderr)"], log_file
        )
        assert process.stdout is None and process.stderr is None
        process.wait(timeout=10)

    assert (tmp_path / "1-valkey-0.stdio.log").read_text().split() == ["first", "second"]
    assert "second" in manager._process_output_tail(log_file)


@contextmanager
def _held_port():
    """A port another server holds; the OS picks a free one with room for a cluster's bus ports above it."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    while listener.getsockname()[1] > 65000:
        listener.close()
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    try:
        yield listener.getsockname()[1]
    finally:
        listener.close()


@pytest.mark.parametrize(("mode", "kind", "offset"), [("standalone", "client", 0), ("cluster", "cluster bus", 10000)])
def test_a_port_another_server_holds_stops_a_spawn_or_restart_before_anything_starts(tmp_path, mode, kind, offset):
    with _held_port() as port, patch(f"{PROCESSES}.subprocess.Popen") as popen:
        primaries = 3 if mode == "cluster" else 1
        manager = process_manager(tmp_path, mode=DeploymentMode(mode), primaries=primaries, base_port=port - offset)
        with pytest.raises(PortUnavailableError, match=f"{kind} port {port}"):
            manager.spawn_all(plan_deployment(manager.config))
        # The port was taken while the instance was down.
        node = instance(port=port - offset, pid=100)
        node.process.poll.return_value = 0
        with pytest.raises(PortUnavailableError):
            manager.restart_instance(node, wait_ready=False)

    popen.assert_not_called()


def _pair(tmp_path):
    manager = process_manager(tmp_path, replicas_per_primary=1)
    primary, replica = instance("valkey-0"), instance("valkey-1", "replica", 7001, primary_id="valkey-0")
    for node in (primary, replica):
        node.log_file = str(tmp_path / f"{node.instance_id}.log")
    manager._spawned_instances = {"valkey-0": primary, "valkey-1": replica}
    return manager, replica


@pytest.mark.parametrize(
    ("answer", "error"),
    [
        (_replica_info(), None),
        (_replica_info(port=7999), "failed to become ready"),  # it replicates another primary
        (_replica_info(pid=4242), "served by pid 4242"),  # another server answers on its port
    ],
)
def test_a_replica_restarts_after_its_old_server_stopped_and_must_rejoin_its_primary(tmp_path, answer, error):
    manager, replica = _pair(tmp_path)
    old, new = replica.process, _running(999)
    events = Mock()
    events.attach_mock(old.terminate, "terminate_old")
    events.attach_mock(Mock(return_value=new), "popen")

    with (
        patch(f"{PROCESSES}.subprocess.Popen", events.popen),
        patch(f"{PROCESSES}.valkey_client", return_value=client_context(fake_client(answer))),
        patch.object(manager, "ensure_ports_available"),
        pytest.raises(Exception, match=error) if error else nullcontext(),
    ):
        manager.restart_instance(replica, wait_ready=True, ready_timeout=0.3)

    # Two servers never compete for one port and data directory.
    assert [name for name, *_ in events.mock_calls][:2] == ["terminate_old", "popen"]
    command = events.popen.call_args.args[0]
    assert command[command.index("--replicaof") :][:3] == ["--replicaof", "127.0.0.1", "7000"]
    assert new.terminate.called is bool(error)


_OK = "cluster_state:ok\r\ncluster_known_nodes:2\r\n"
_CL = {"cluster_enabled": 1}


def _view(flags, primary="-"):
    """CLUSTER NODES as a node sees it: abc (the restarted node) with ``flags``, and its peer def."""
    return cluster_nodes_text(
        cluster_nodes_line("abc", 7000, flags, primary),
        cluster_nodes_line("def", 7001, "master", epoch=2, slots="0-16383"),
    )


@pytest.mark.parametrize(
    ("role", "info", "replies", "peer", "primary_alive", "ready"),
    [
        # A primary waits for its running replicas.
        ("primary", {"role": "master", "connected_slaves": 1}, None, None, True, True),
        ("primary", {"role": "slave"}, None, None, True, False),
        # A replica waits for its link, unless the primary it follows was killed: then the link cannot come up.
        ("replica", _replica_info(pid=7001, link="down"), None, None, True, False),
        ("replica", _replica_info(pid=7001, link="down"), None, None, False, True),
        # A cluster node is ready once it and a peer see it healthy and agree on its role.
        ("cluster", _CL, [_OK, "abc", _view("myself,master")], _view("master"), True, True),
        ("cluster", _CL, [_OK, "abc", _view("myself,master,fail")], _view("master"), True, False),
        ("cluster", _CL, ["cluster_state:fail\r\n"], None, True, False),
        ("cluster", _CL, [_OK, "other-id"], None, True, InstanceIdentityError),
        # The old primary reads itself as primary from nodes.conf until gossip brings the new epoch (F15).
        ("cluster", _CL, [_OK, "abc", _view("myself,master")], _view("slave", "def"), True, False),
        ("cluster", _CL, [_OK, "abc", _view("myself,master")], _view("master,fail?"), True, False),
        ("cluster", _CL, [_OK, "abc", _view("myself,master")], None, True, False),
        ("cluster", {**_CL, "master_link_status": "up"}, [_OK, "abc", _view("myself,slave", "def")], _view("slave", "def"), True, True),
        ("cluster", {**_CL, "master_link_status": "down"}, [_OK, "abc", _view("myself,slave", "def")], _view("slave", "def"), True, False),
    ],
)  # fmt: skip
def test_a_restarted_server_is_ready_once_it_rejoins_in_its_role(
    tmp_path, role, info, replies, peer, primary_alive, ready
):
    manager, replica = _pair(tmp_path)
    manager._spawned_instances["valkey-0"].process.poll.return_value = None if primary_alive else -9
    node = replica if role == "replica" else instance(cluster_node_id="abc")
    if role == "cluster":
        manager.config.mode = DeploymentMode.CLUSTER
        manager._spawned_instances = {"valkey-0": node, "valkey-1": instance("valkey-1", port=7001)}
    client = fake_client({"process_id": node.pid, **info}, replies)
    rows = None if peer is None else cluster_nodes.parse_cluster_nodes(peer)

    with patch(f"{PROCESSES}.query_cluster_nodes", return_value=rows) as asked:
        if ready is InstanceIdentityError:
            with pytest.raises(InstanceIdentityError, match="expected abc"):
                manager._restart_is_ready(node, client, manager.config.mode)
        else:
            assert manager._restart_is_ready(node, client, manager.config.mode) is ready
    # Only a running peer is asked, never the restarted node itself.
    assert all(call.args[0]["port"] == 7001 for call in asked.call_args_list)


@pytest.mark.parametrize("failure", [RuntimeError("formation failed"), KeyboardInterrupt()])
def test_a_failed_formation_closes_its_cached_clients(failure):
    formation = Formation()
    client = formation.connections["valkey-0"] = Mock()

    with (
        patch.object(formation, "reset_cluster_state", side_effect=failure),
        pytest.raises(LocalDeploymentError if isinstance(failure, Exception) else KeyboardInterrupt),
    ):
        formation.form([instance()])

    client.close.assert_called_once()
    assert formation.connections == {}


def test_formation_points_each_replica_at_the_primary_and_resets_only_its_own_servers():
    primary, replica = instance("valkey-0"), instance("valkey-1", "replica", 7001, primary_id="valkey-0")
    clients = {
        "valkey-0": fake_client({"role": "master", "connected_slaves": 1}),
        "valkey-1": fake_client(_replica_info()),
    }
    formation = Formation()
    formation.get_client = lambda node: clients[node.instance_id]

    formation.form([primary, replica], mode=DeploymentMode.STANDALONE)
    assert clients["valkey-1"].execute_command.call_args_list == [call("REPLICAOF", "127.0.0.1", 7000)]
    clients["valkey-1"].info.return_value = _replica_info(port=7999)
    assert "replicates 127.0.0.1:7999" in formation._primary_replica_problem(primary, [replica])
    # A primary that does not report its replicas is not taken to have them all.
    clients["valkey-1"].info.return_value = _replica_info()
    clients["valkey-0"].info.return_value = {"role": "master"}
    assert formation._primary_replica_problem(primary, [replica]) == "valkey-0 reports 0 of 1 replicas connected"

    # CLUSTER RESET HARD wipes a node: never send it to a server another process runs, or after ours exited.
    clients = {"valkey-0": fake_client({"process_id": 7000}), "valkey-1": fake_client({"process_id": 999})}
    with pytest.raises(InstanceIdentityError, match="valkey-1"):
        formation.reset_cluster_state([primary, replica])
    assert clients["valkey-0"].execute_command.call_args_list == [call("CLUSTER", "RESET", "HARD")]
    clients["valkey-1"].execute_command.assert_not_called()
    primary.process.poll.return_value = 0
    with pytest.raises(InstanceIdentityError, match="has exited"):
        formation.reset_cluster_state([primary])


def test_cluster_reset_waits_until_each_node_knows_only_itself_and_no_slot(clock):
    # The reset is done when CLUSTER RESET HARD returns: the poll replaces a fixed 1 s pause (F22).
    nodes = [instance(f"valkey-{n}", port=7000 + n) for n in range(2)]

    def client(port, infos):
        def execute(*args):
            return "OK" if args[:2] == ("CLUSTER", "RESET") else infos.pop(0)

        return Mock(execute_command=Mock(side_effect=execute), info=Mock(return_value={"process_id": port}))

    stale, reset = (
        "cluster_known_nodes:2\r\ncluster_slots_assigned:16384\r\n",
        "cluster_known_nodes:1\r\ncluster_slots_assigned:0\r\n",
    )
    clients = {7000: client(7000, [stale, reset]), 7001: client(7001, [reset])}
    formation = Formation()
    formation.get_client = lambda node: clients[node.port]
    formation.reset_cluster_state(nodes)
    assert clock.elapsed == pytest.approx(0.1), "done on the second round"
    assert all(call("CLUSTER", "RESET", "HARD") in clients[port].execute_command.call_args_list for port in clients)

    clients = {7000: client(7000, [stale] * 200), 7001: client(7001, [reset] * 200)}
    with pytest.raises(LocalDeploymentError, match="did not reset within 10s"):
        formation.reset_cluster_state(nodes)


def test_a_cluster_is_formed_only_once_every_node_sees_each_replica_of_its_primary():
    """A Valkey 7.2 replica's link was up before the cluster bus told the other nodes its role, so a failover
    right after provisioning read a view in which shard 0's primary had no replica to promote."""

    nodes = [
        instance("valkey-0", cluster_node_id="n0"),
        instance("valkey-1", "replica", 7001, primary_id="valkey-0", cluster_node_id="n1"),
    ]
    cluster = FakeDeployment({7000: ("n0", None, "0-16383"), 7001: ("n1", 7000, "")})
    formation = Formation()
    formation.get_client = lambda node: cluster.client(port=node.port)
    assert formation._replica_view_problem(nodes) is None

    seen_by_replica = cluster.cluster_nodes(7001)
    cluster.nodes[7001] = ("n1", None, "")  # valkey-0's view: valkey-1 still a primary
    stale = cluster.cluster_nodes(7000)
    cluster.cluster_nodes = lambda me: stale if me == 7000 else seen_by_replica
    assert formation._replica_view_problem(nodes) == "valkey-0 does not yet see valkey-1 as a replica of valkey-0"


def test_slot_verification_outlasts_a_failing_slot_window_a_loaded_host_stretches(clock):
    """valkey #3438 under a soak's load: every slot was assigned, yet the first node kept reporting slots
    failing past the old 20 s wait (cluster-node-timeout 1000 marks a slow peer down), so provisioning failed
    a cluster that was healthy seconds later."""

    primary = instance("valkey-0", slot_start=0, slot_end=cluster_nodes.HASH_SLOTS - 1)
    failing = "cluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_slots_fail:5460\r\n"
    healthy = "cluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_slots_fail:0\r\n"

    def client(cleared_at):
        def replies(*args):
            if args[:2] == ("CLUSTER", "ADDSLOTS"):
                return "OK"
            if args[:2] == ("CLUSTER", "MYID"):
                return "n0"
            return healthy if clock.elapsed >= cleared_at else failing

        return Mock(execute_command=Mock(side_effect=replies))

    formation = Formation()
    clearing = client(cleared_at=30.0)
    formation.get_client = lambda node: clearing
    assert formation.assign_and_verify_slots([primary]) == {"valkey-0": "n0"}
    assert 30.0 <= clock.elapsed <= 60.0, "waited out the failing-slot window instead of erroring at 20 s"

    # Slots that never clear still fail provisioning, with the counts seen.
    clock.elapsed = 0.0
    stuck = client(cleared_at=float("inf"))
    formation.get_client = lambda node: stuck
    with pytest.raises(LocalDeploymentError, match=r"did not propagate within 60s: 16384/16384 .* 5460 failed"):
        formation.assign_and_verify_slots([primary])


def test_the_health_wait_outlasts_a_fail_state_window_a_loaded_host_stretches(clock):
    """valkey #3438 under a soak's load: slots were assigned and the replicas synced, yet one node kept
    reporting cluster_state fail past the old 30 s health wait (cluster-node-timeout 1000 marks a slow peer
    down), so provisioning failed a cluster that was healthy seconds later."""

    last_slot = cluster_nodes.HASH_SLOTS - 1
    nodes = [
        instance("valkey-0", slot_start=0, slot_end=last_slot, cluster_node_id="n0"),
        instance("valkey-1", "replica", 7001, primary_id="valkey-0", cluster_node_id="n1"),
    ]
    rows = FakeDeployment({7000: ("n0", None, f"0-{last_slot}"), 7001: ("n1", 7000, "")})

    def client(me, cleared_at):
        def execute(*args):
            if args[:2] == ("CLUSTER", "NODES"):
                return rows.cluster_nodes(me)
            state = "ok" if clock.elapsed >= cleared_at else "fail"
            return (
                f"cluster_state:{state}\r\ncluster_slots_assigned:{cluster_nodes.HASH_SLOTS}\r\n"
                "cluster_slots_fail:0\r\n"
            )

        info = {"role": "master", "connected_slaves": 1} if me == 7000 else _replica_info()
        return Mock(execute_command=Mock(side_effect=execute), info=Mock(return_value=info))

    def formed(cleared_at):
        clock.elapsed = 0.0
        formation = Formation()
        clients = {node.port: client(node.port, cleared_at) for node in nodes}
        formation.get_client = lambda node: clients[node.port]
        formation.reset_cluster_state = Mock()
        formation.cluster_meet = Mock()
        formation.assign_and_verify_slots = Mock(return_value={"valkey-0": "n0"})
        formation.setup_and_sync_replication = Mock()
        formation.form(nodes)

    formed(cleared_at=45.0)  # past the old 30 s wait, within SETTLE_TIMEOUT_S
    assert 45.0 <= clock.elapsed <= SETTLE_TIMEOUT_S, "waited out the fail-state window instead of erroring at 30 s"

    # A state that never clears still fails provisioning, with the problem seen.
    with pytest.raises(LocalDeploymentError, match=r"not healthy within 60s: cluster_state is fail"):
        formed(cleared_at=float("inf"))


def test_cluster_meet_outlasts_a_convergence_window_a_loaded_host_stretches(clock):
    """valkey @3eb8314be6af under a soak's load: 6 nodes had not all learned of each other within the old 30 s
    meet wait (the cluster bus gossips slowly on a loaded host), so provisioning failed a cluster that
    converged seconds later."""

    nodes = [instance(f"valkey-{n}", port=7000 + n, cluster_node_id=f"n{n}") for n in range(2)]

    def client(cleared_at):
        def execute(*args):
            if args[:2] == ("CLUSTER", "MEET"):
                return "OK"
            known = len(nodes) if clock.elapsed >= cleared_at else 1
            return f"cluster_state:ok\r\ncluster_known_nodes:{known}\r\n"

        return Mock(execute_command=Mock(side_effect=execute))

    def met(cleared_at):
        clock.elapsed = 0.0
        formation = Formation()
        converging = client(cleared_at)
        formation.get_client = lambda node: converging
        formation.cluster_meet(nodes)

    met(cleared_at=45.0)  # past the old 30 s wait, within SETTLE_TIMEOUT_S
    assert 45.0 <= clock.elapsed <= SETTLE_TIMEOUT_S, "waited out the convergence window instead of erroring at 30 s"
    met(cleared_at=0.0)
    assert clock.elapsed == 0.0, "no fixed pause per MEET: the convergence poll is the wait (F22)"

    # Nodes that never learn of each other still fail provisioning.
    with pytest.raises(LocalDeploymentError, match=r"did not all learn of each other within 60.00s"):
        met(cleared_at=float("inf"))


def test_each_replica_link_seen_up_counts_although_another_replicas_info_times_out(clock):
    """84 instances at load 400: every link was up 6 s in, yet one slow INFO failed each round of the old
    all-replicas poll, so provisioning never passed."""

    replicas = [instance(f"valkey-{n}", "replica", 7000 + n, primary_id="valkey-0") for n in (1, 2)]
    down = {**_replica_info(), "master_link_status": "down"}
    clients = {"valkey-1": Mock(), "valkey-2": Mock()}
    # valkey-1 is up at once, then its INFO times out; valkey-2 comes up on the second round.
    clients["valkey-1"].info.side_effect = [_replica_info(), *[TimeoutError("Timeout reading from socket")] * 10]
    clients["valkey-2"].info.side_effect = [down, _replica_info(), _replica_info()]
    formation = Formation()
    formation.get_client = lambda node: clients[node.instance_id]
    formation.setup_and_sync_replication(replicas, {"valkey-0": "n0"}, timeout=5)
    assert clock.elapsed == pytest.approx(0.1), "done on the second round, 0.1 s after the first"

    # A link never seen up still fails provisioning, naming the replica.
    clients["valkey-1"].info.side_effect = [_replica_info()]
    clients["valkey-2"].info.side_effect = None
    clients["valkey-2"].info.return_value = down
    with pytest.raises(LocalDeploymentError, match=r"did not come up within 1.00s \(valkey-2\)"):
        formation.setup_and_sync_replication(replicas, {"valkey-0": "n0"}, timeout=1)
    # A replica whose primary has no cluster ID cannot be configured: it fails at once, not 60 s later.
    started = clock.elapsed
    with pytest.raises(LocalDeploymentError, match="valkey-1's primary 'valkey-0' has no cluster ID"):
        formation.setup_and_sync_replication(replicas, {}, timeout=60)
    assert clock.elapsed == started


@pytest.mark.slow
def test_a_killed_primary_of_a_real_cluster_restarts_as_a_new_process_the_cluster_takes_back():
    with formed_cluster(3, 1, 7700) as cluster:
        primary = next(node for node in cluster.nodes if node.shard_id == 0 and node.role == "primary")
        old_pid = primary.pid
        primary.process.kill()
        primary.process.wait()
        time.sleep(3)  # let the cluster see it down

        cluster.processes.restart_instance(primary, wait_ready=True, ready_timeout=30.0)

        assert primary.pid != old_pid and primary.process.poll() is None
        with valkey_client(primary.host, primary.port, timeout=2.0) as client:
            assert int(client.info("server")["process_id"]) == primary.pid


def test_planning_a_deployment_is_logged_at_debug_not_info(tmp_path, caplog):
    manager = process_manager(tmp_path)
    with caplog.at_level(logging.INFO, logger="valkey_fuzzer.deployment.local.planner"):
        plan_deployment(manager.config)
    assert not [record for record in caplog.records if "Planning" in record.getMessage()]
    with caplog.at_level(logging.DEBUG, logger="valkey_fuzzer.deployment.local.planner"):
        plan_deployment(manager.config)
    assert [record for record in caplog.records if "Planning" in record.getMessage()]


def test_a_failed_source_build_names_the_file_holding_its_output(tmp_path, caplog):
    def run(command, **options):
        raise subprocess.CalledProcessError(2, command)

    with (
        patch(f"{PROCESSES}.subprocess.run", side_effect=run),
        patch(f"{PROCESSES}.fcntl.flock"),
        caplog.at_level(logging.ERROR),
        pytest.raises(subprocess.CalledProcessError),
    ):
        processes.fetch_valkey(VALKEY_SOURCE_SHA, cache=str(tmp_path / "cache"))
    assert str(tmp_path / "cache" / f"{VALKEY_SOURCE_SHA}.build.log") in caplog.text
