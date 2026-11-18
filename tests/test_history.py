"""``valkey.oracle.history``: the workload's history as Porcupine sees it under the ledger's loss policy, and the
oracle's verdicts on hand-built histories (with the porcupine checker built from ``porcupine/``)."""

import os
import shutil
import subprocess
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest
import valkey

from tests.valkey_experiments_support import fake_context, resolved
from valkey_fuzzer.components.files import write_jsonl
from valkey_fuzzer.components.history import HistoryOracle, register_history
from valkey_fuzzer.components.ledger import LOSS_WINDOW, LedgerModel
from valkey_fuzzer.components.workloads import LEDGER_EVIDENCE, KeyValueWorkload, _Target
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.results import Evidence

ROOT = Path(__file__).resolve().parents[1]
KEY = "vf:writes:m:0"


def _op(op_id, op, outcome="ok", value=None, reply=None, t_end=None, instance_id="valkey-0", key=KEY):
    t_end = float(op_id) if t_end is None else t_end
    entry = {"id": op_id, "op": op, "key": key, "outcome": outcome, "process": op_id, "t_start": t_end - 0.5}
    entry.update({"t_end": t_end, "instance_id": instance_id})
    if value is not None:
        entry["value"] = value
    if op == "get":
        entry["reply"] = reply
    return entry


def _final(value, key=KEY):
    return {key: value}, 100.0, 101.0


# The history Porcupine checks


def test_each_operation_records_its_connection_and_a_failure_retires_it():
    replies = iter([valkey.exceptions.TimeoutError("late"), True, True])

    def write(key, value):
        reply = next(replies)
        if isinstance(reply, Exception):
            raise reply
        return reply

    client = SimpleNamespace(set=write, close=lambda: None)
    mode = SimpleNamespace(value="standalone")
    handle = SimpleNamespace(mode=mode, primaries=lambda: ["valkey-0"], new_client=lambda *args, **kwargs: client)
    workload = KeyValueWorkload()
    workload._target = _Target(handle, 1.0)
    for op_id in (1, 2, 3):
        workload._execute(op_id, "set", KEY, f"v{op_id}")

    assert [(entry["outcome"], entry["process"]) for entry in workload._ledger] == [
        ("ambiguous", 1),
        ("ok", 2),
        ("ok", 2),
    ]


def test_the_history_holds_every_operation_with_its_invocation_completion_and_connection():
    ledger = [
        _op(1, "set", value="v1"),
        _op(2, "get", reply="v1"),
        _op(3, "set", "ambiguous", "v2"),
        _op(4, "set", "error", "v3"),
        _op(5, "del"),
    ]

    history, unchecked = register_history({KEY: "v0", "vf:writes:stable:0": "s"}, ledger, _final(None), started_at=0.0)

    assert history == [
        {"process": 0, "key": KEY, "f": "write", "value": "v0", "call": 0, "return": 0},
        {"process": 1, "key": KEY, "f": "write", "value": "v1", "call": 500_000_000, "return": 1_000_000_000},
        # The ambiguous SET of v2, which no read returned, and the rejected SET are left out.
        {"process": 5, "key": KEY, "f": "write", "value": None, "call": 4_500_000_000, "return": 5_000_000_000},
        {"process": 2, "key": KEY, "f": "read", "value": "v1", "call": 1_500_000_000, "return": 2_000_000_000},
        # The final read, on a connection after every other.
        {"process": 6, "key": KEY, "f": "read", "value": None, "call": 100_000_000_000, "return": 101_000_000_000},
    ]
    assert unchecked == 0


@pytest.mark.parametrize(
    ("ledger", "options", "indeterminate", "unchecked"),
    [
        # A write the policy lets be lost is indeterminate, and a read of its value is not checked.
        pytest.param([_op(1, "set", value="v1", t_end=11.0), _op(2, "get", reply="v1", t_end=12.0)], {"loss_windows": [(10.0, 12.0)]}, [True], 1, id="lossy-write"),
        pytest.param([_op(1, "set", value="v1", t_end=11.0), _op(2, "get", reply="v1", t_end=12.0)], {}, [False], 0, id="no-window"),
        # Only the windows' instances' acknowledgements may be lost.
        pytest.param([_op(1, "set", value="v1", t_end=11.0, instance_id="valkey-1")], {"loss_windows": [(10.0, 12.0, frozenset({"valkey-0"}))]}, [False], 0, id="other-instance"),
        # A write WAIT confirmed on every replica is never lost.
        pytest.param([{**_op(1, "set", value="v1", t_end=11.0), "wait": 2}], {"loss_windows": [(10.0, 12.0)], "wait_confirmed": 2}, [False], 0, id="wait-confirmed"),
        # An ambiguous write sent before a window ends may be lost too, so a read of its value is not checked.
        pytest.param([_op(1, "set", "ambiguous", "v1", t_end=5.0), _op(2, "get", reply="v1", t_end=6.0)], {"loss_windows": [(10.0, 12.0)]}, [True], 1, id="ambiguous-before-window"),
        pytest.param([_op(1, "set", "ambiguous", "v1", t_end=5.0), _op(2, "get", reply="v1", t_end=6.0)], {}, [True], 0, id="ambiguous-without-window"),
        # window_reads: written leaves reads inside a window unchecked.
        pytest.param([_op(1, "get", reply="v0", t_end=11.0)], {"loss_windows": [(10.0, 12.0)], "window_reads": "written"}, [], 1, id="read-in-window"),
        pytest.param([_op(1, "get", reply="v0", t_end=11.0)], {"loss_windows": [(10.0, 12.0)]}, [], 0, id="strict-reads"),
    ],
)  # fmt: skip
def test_the_history_follows_the_ledger_loss_policy(ledger, options, indeterminate, unchecked):
    # The final read returns v1, so an indeterminate write of it is kept (see the next test).
    history, skipped = register_history({}, ledger, _final("v1"), started_at=0.0, **options)

    assert [op["return"] is None for op in history if op["f"] == "write"] == indeterminate
    assert skipped == unchecked


def test_an_indeterminate_write_no_read_returned_is_left_out():
    # A replication-chaos run's 352 such writes on one key kept Porcupine from finishing in 300 s. Taking one never
    # to have applied changes no verdict, as no read saw it.
    ledger = [
        _op(1, "set", "ambiguous", "unread"),
        _op(2, "set", "ambiguous", "read"),
        _op(3, "get", reply="read"),
        _op(4, "del", "ambiguous"),
        _op(5, "set", value="acknowledged"),
    ]

    history, _ = register_history({}, ledger, _final("acknowledged"), started_at=0.0)

    # The DEL too: no read found the key absent.
    assert [(op["value"], op["return"] is None) for op in history if op["f"] == "write"] == [
        ("read", True),
        ("acknowledged", False),
    ]
    # A read that found it absent keeps the DEL.
    history, _ = register_history({}, [*ledger, _op(6, "get")], _final("acknowledged"), started_at=0.0)

    assert [(op["value"], op["return"] is None) for op in history if op["f"] == "write"] == [
        ("read", True),
        (None, True),
        ("acknowledged", False),
    ]


# valkey.oracle.history with the porcupine checker


@pytest.fixture(scope="session")
def porcupine_dir(tmp_path_factory):
    """A directory holding the porcupine checker: the one on PATH, or one built from ``porcupine/``."""

    found = shutil.which("porcupine")
    if found is not None:
        return os.path.dirname(found)
    if shutil.which("go") is None:
        pytest.skip("neither porcupine nor go is on PATH: build it with `go -C porcupine build -o <dir on PATH>`")
    directory = tmp_path_factory.mktemp("porcupine")
    subprocess.run(["go", "build", "-o", str(directory / "porcupine"), "."], cwd=ROOT / "porcupine", check=True)
    return str(directory)


class _Pipe:
    def __init__(self, values):
        self.values, self.replies = values, []

    def get(self, key):
        self.replies.append(self.values.get(key))

    def execute(self):
        return self.replies


class _Standalone:
    """One primary, valkey-0, holding ``values``."""

    mode = SimpleNamespace(value="standalone")
    instances = (SimpleNamespace(instance_id="valkey-0", role="primary"),)

    def __init__(self, values):
        self.values = values

    def primaries(self):
        return ["valkey-0"]

    def new_client(self, instance_id, timeout=5.0):
        return SimpleNamespace(pipeline=lambda transaction=False: _Pipe(self.values), close=lambda: None)


def _window():
    return Evidence(LOSS_WINDOW, "chaos", "action.completed", 1.0, {"start": 10.0, "end": 12.0})


_CLEAN = [_op(1, "set", value="v1"), _op(2, "get", reply="v1"), _op(3, "set", value="v2"), _op(4, "get", reply="v2")]
# Each ambiguous write may or may not have been applied.
_AMBIGUOUS = [_op(1, "set", value="v1"), _op(2, "set", "ambiguous", "v2"), _op(3, "set", "ambiguous", "v3")]
# v2, acknowledged inside the 10-12 s window, may be lost after a read saw it.
_SEEN_THEN_LOST = [
    _op(1, "set", value="v1"),
    _op(2, "set", value="v2", t_end=11.0),
    _op(3, "get", reply="v2", t_end=12.0),
]


@pytest.mark.parametrize(
    ("ledger", "final", "evidence", "status"),
    [
        pytest.param(_CLEAN, "v2", [], "PASS", id="clean"),
        pytest.param(_CLEAN, "v1", [], "FAIL", id="lost-acknowledged-write"),
        pytest.param([*_CLEAN, _op(5, "get", reply="v1")], "v2", [], "FAIL", id="stale-read"),
        pytest.param(_AMBIGUOUS, "v1", [], "PASS", id="ambiguous-writes-not-applied"),
        pytest.param(_AMBIGUOUS, "v3", [], "PASS", id="ambiguous-writes-applied"),
        pytest.param([*_AMBIGUOUS, _op(4, "get", reply="v3")], "v1", [], "FAIL", id="applied-ambiguous-write-undone"),
        pytest.param(_SEEN_THEN_LOST, "v1", [_window()], "PASS", id="loss-the-policy-allows"),
        pytest.param(_SEEN_THEN_LOST, "v1", [], "FAIL", id="loss-outside-the-policy"),
    ],
)  # fmt: skip
def test_the_history_oracle_fails_only_on_a_history_that_is_not_linearizable(
    tmp_path, monkeypatch, porcupine_dir, ledger, final, evidence, status
):
    monkeypatch.setenv("PATH", porcupine_dir + os.pathsep + os.environ["PATH"])
    write_jsonl(tmp_path / "preload.jsonl", [{"key": KEY, "value": "v0", "t_end": 0.5, "instance_id": "valkey-0"}])
    write_jsonl(tmp_path / "ledger.jsonl", ledger)
    summary = {"preload_path": str(tmp_path / "preload.jsonl"), "ledger_path": str(tmp_path / "ledger.jsonl")}
    items = [Evidence(LEDGER_EVIDENCE, "writes", "workload.quiesced", 1.0, {**summary, "started_at": 0.0}), *evidence]
    data = resolved(
        ComponentSpec(role=ComponentRole.ORACLE, use="valkey.oracle.ledger", id="data", config={"workload": "writes"})
    )
    spec = resolved(
        ComponentSpec(role=ComponentRole.ORACLE, use="valkey.oracle.history", id="history", config={"ledger": "data"})
    )
    context = fake_context(_Standalone({KEY: final}), data, spec, artifact_dir=str(tmp_path))

    result = HistoryOracle().evaluate(context, spec, items)

    assert result.status.value == status, result.summary
    if status == "FAIL":
        assert list(result.diagnostics["illegal_keys"]) == [KEY]
        assert os.path.getsize(result.diagnostics["visualization_path"]) > 0


# Several clients at once


def _at(op_id, op, process, t_start, t_end, value=None, reply=None, key=KEY):
    entry = {"id": op_id, "op": op, "key": key, "outcome": "ok", "process": process, "t_start": t_start}
    entry.update({"t_end": t_end, "instance_id": "valkey-0"})
    if value is not None:
        entry["value"] = value
    if op == "get":
        entry["reply"] = reply
    return entry


class _StoreClient:
    """A primary: a dict behind a lock, so several threads may use it at once."""

    def __init__(self):
        self.store, self.lock = {}, threading.Lock()

    def set(self, key, value):
        with self.lock:
            self.store[key] = value
        return True

    def delete(self, key):
        with self.lock:
            return int(self.store.pop(key, None) is not None)

    def get(self, key):
        with self.lock:
            return self.store.get(key)

    def pipeline(self, transaction=False):
        return SimpleNamespace(set=self.set, execute=lambda: [True] * len(self.store))

    def close(self):
        pass


def _run_workload(tmp_path, clients, **config):
    config = {"mutable_keys": 3, "stable_keys": 1, "rate": 2000, "max_ops": 200, "drain_on_stop": True, **config}
    spec = resolved(
        ComponentSpec(
            role=ComponentRole.WORKLOAD,
            use="valkey.workload.key-value",
            id="writes",
            config={**config, "clients": clients},
        )
    )
    client = _StoreClient()
    mode = SimpleNamespace(value="standalone")
    instances = (SimpleNamespace(instance_id="valkey-0", host="127.0.0.1", port=7000),)
    handle = SimpleNamespace(
        mode=mode, instances=instances, primaries=lambda: ["valkey-0"], new_client=lambda *a, **k: client
    )
    context = fake_context(handle, spec, artifact_dir=str(tmp_path))
    workload = KeyValueWorkload()
    assert workload.start(context, spec).status.value == "PASS"
    result = workload.stop(context, spec)
    assert result.status.value == "PASS", result.summary
    return workload, result


def test_every_client_of_the_workload_has_its_own_connection_and_the_operations_add_up(tmp_path):
    workload, result = _run_workload(tmp_path, 4)

    ledger = workload._ledger
    summary = next(item.value for item in result.evidence)
    assert summary["clients"] == 4 and summary["drained"] and summary["operations"] == len(ledger) == 200
    # Unique operation ids, issued at most once each, and one connection number per client (never shared).
    assert sorted(entry["id"] for entry in ledger) == list(range(1, 201))
    assert sorted({entry["process"] for entry in ledger}) == [1, 2, 3, 4]
    per_client = {process: [e for e in ledger if e["process"] == process] for process in (1, 2, 3, 4)}
    assert all(per_client.values())
    # A client issues one operation at a time: its operations never overlap each other.
    for entries in per_client.values():
        entries.sort(key=lambda entry: entry["t_start"])
        assert all(a["t_end"] <= b["t_start"] for a, b in zip(entries, entries[1:]))


def test_a_workload_of_one_client_is_the_sequential_workload(tmp_path):
    workload, result = _run_workload(tmp_path, 1)

    assert {entry["process"] for entry in workload._ledger} == {1}
    assert [entry["id"] for entry in workload._ledger] == list(range(1, 201))
    assert next(item.value for item in result.evidence)["clients"] == 1


def test_the_ledger_model_of_a_concurrent_workload_accepts_either_order_of_overlapping_writes():
    ledger = [
        _at(1, "set", 1, 1.0, 4.0, "v1"),
        _at(2, "set", 2, 2.0, 3.0, "v2"),
        # A read that began before the second write ended: the oracle on the history judges it, not this model.
        _at(3, "get", 3, 2.5, 3.5, reply="neither"),
    ]

    sequential = LedgerModel({}, ledger, [])
    concurrent = LedgerModel({}, ledger, [], concurrent=True)

    assert concurrent.possible[KEY] == {"v1", "v2"} and concurrent.unchecked_reads == 1
    assert not concurrent.read_violations and sequential.read_violations
    # A write that began after both finished is the only value left.
    later = LedgerModel({}, [*ledger, _at(4, "set", 1, 5.0, 6.0, "v3")], [], concurrent=True)
    assert later.possible[KEY] == {"v3"}
    # Without overlap the concurrent model is the sequential one.
    serial = [_at(1, "set", 1, 1.0, 2.0, "v1"), _at(2, "set", 2, 3.0, 4.0, "v2")]
    assert LedgerModel({}, serial, [], concurrent=True).possible == LedgerModel({}, serial, []).possible


def _history_verdict(tmp_path, monkeypatch, porcupine_dir, ledger, final, clients, **config):
    monkeypatch.setenv("PATH", porcupine_dir + os.pathsep + os.environ["PATH"])
    write_jsonl(tmp_path / "preload.jsonl", [{"key": KEY, "value": "v0", "t_end": 0.5, "instance_id": "valkey-0"}])
    write_jsonl(tmp_path / "ledger.jsonl", ledger)
    summary = {"preload_path": str(tmp_path / "preload.jsonl"), "ledger_path": str(tmp_path / "ledger.jsonl")}
    items = [Evidence(LEDGER_EVIDENCE, "writes", "workload.quiesced", 1.0, {**summary, "started_at": 0.0, **clients})]
    data = resolved(
        ComponentSpec(role=ComponentRole.ORACLE, use="valkey.oracle.ledger", id="data", config={"workload": "writes"})
    )
    spec = resolved(
        ComponentSpec(
            role=ComponentRole.ORACLE, use="valkey.oracle.history", id="history", config={"ledger": "data", **config}
        )
    )
    context = fake_context(_Standalone({KEY: final}), data, spec, artifact_dir=str(tmp_path))
    return HistoryOracle().evaluate(context, spec, items)


# Two writes that overlap; two reads after both ended see them in turn. Only an order where v1 is applied last
# (the first read) and then v2 (the second) explains it, and no order does: the sequential workload cannot make
# overlapping writes, so cannot make this history at all.
_OVERLAPPING_WRITES = [_at(1, "set", 1, 1.0, 4.0, "v1"), _at(2, "set", 2, 2.0, 3.0, "v2")]


@pytest.mark.parametrize(
    ("reads", "status"),
    [
        pytest.param(["v2", "v2"], "PASS", id="v1-applied-first"),
        pytest.param(["v1", "v1"], "PASS", id="v2-applied-first"),
        pytest.param(["v1", "v2"], "FAIL", id="the-winner-changes"),
    ],
)
def test_the_history_oracle_judges_the_overlapping_operations_of_several_clients(
    tmp_path, monkeypatch, porcupine_dir, reads, status
):
    ledger = [
        *_OVERLAPPING_WRITES,
        _at(3, "get", 3, 5.0, 6.0, reply=reads[0]),
        _at(4, "get", 4, 7.0, 8.0, reply=reads[1]),
    ]

    result = _history_verdict(tmp_path, monkeypatch, porcupine_dir, ledger, reads[1], {"clients": 4})

    assert result.status.value == status, result.summary
    if status == "PASS":
        assert "4-client history" in result.summary
        assert result.evidence[0].value["clients"] == 4
    else:
        assert "4-client history" in result.summary and list(result.diagnostics["illegal_keys"]) == [KEY]


def test_a_single_client_history_says_so(tmp_path, monkeypatch, porcupine_dir):
    ledger = [_at(1, "set", 1, 1.0, 2.0, "v1"), _at(2, "get", 1, 3.0, 4.0, reply="v1")]

    for clients in ({"clients": 1}, {}):
        result = _history_verdict(tmp_path, monkeypatch, porcupine_dir, ledger, "v1", clients)
        assert result.status.value == "PASS" and "single-client history" in result.summary


def test_the_history_oracle_is_an_error_when_the_workload_ran_fewer_clients_than_required(tmp_path, monkeypatch):
    # The checker is never run: the oracle refuses before it reads anything.
    ledger = [_at(1, "set", 1, 1.0, 2.0, "v1")]

    result = _history_verdict(tmp_path, monkeypatch, str(tmp_path), ledger, "v1", {"clients": 1}, min_clients=2)

    assert result.status.value == "ERROR" and "fewer than min_clients 2" in result.summary
