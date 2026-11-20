"""``valkey.oracle.cluster-election-safety`` checks the invariants of the TLA+
model in ``formal/cluster-election/model.tla`` on the live run: one winner per
epoch, one vote per primary per epoch, every winner counted votes from a
majority of the primaries, votes saved before they leave under
``cluster-config-save-behavior sync``, and no split primary.

Election facts come from node log lines published by a
``valkey.observer.log-trigger`` observer (see the
``valkey.cluster-election-durability`` experiment): votes granted (``Failover
auth granted to``), votes counted (``Failover auth ACK from ... (ACKs N,
quorum Q)``), elections started (``Starting a
failover election for epoch N``), and elections won (``Failover election
won``, whose epoch is the one its winner last started). The patterns and
event names live in ``formal/cluster-election/mapping.yaml``; the observer
and the oracle read them from there (``spec: cluster-election``).

Minimum Valkey version: 9.2, the first to log each counted vote with its
count and quorum. On an older build no ACK line exists, so every run that
elected is ERROR (the quorum check cannot run), never a false PASS or FAIL;
the shipped experiments pin ``min_version: "9.2.0"`` in their build probe.

The model's ``VoteOncePerEpoch`` and ``OneWinnerPerEpoch`` are claimed
(``claims.yaml``, ``holds_when``) only under the constants ``spec conform``
derives from the run, ``PersistBeforeAck`` from ``cluster-config-save-behavior
sync`` among them. The oracle derives the same constants from the nodes'
settings and the plan's inputs, so a double vote or two winners under
``best-effort`` is reported as outside the claim, as ``spec conform`` reports
it, not as a failure; the checks the model does not claim (a win's quorum,
a split primary) always fail the run.
"""

from collections import deque
from collections.abc import Mapping, Sequence
from typing import Any, Optional

from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.deployment.local.cluster_nodes import split_slots
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.results import ComponentStatus, Evidence, evidence_of, make_evidence, make_result
from valkey_fuzzer.schema import ConfigError, ConfigField, plain_config
from valkey_fuzzer.spec.bundle import Property, SpecBundleError, load_spec_claims
from valkey_fuzzer.spec.tla import render

from .cluster import ranges_overlap, read_view, view_summary
from .faults import FAULT_INJECTED
from .files import read_jsonl
from .instances import deployment
from .polling import poll_each, poll_until
from .registration import SPEC_SHA256_FIELD, component_registration, pinned_spec_mapping
from .schemas import FLAG, INTEGER, LIST, MAPPING, TEXT, declared, mapping_of, nullable
from .trace import TRACE_EVIDENCE

_SAFETY_FIELDS = {
    "trace": ConfigField(str, allow_none=True, default=None),
    "crash_faults": ConfigField(list, default=list, items=TEXT),
    # The bundle whose mapping.yaml names the vote, ACK, and win events (its
    # granted, acked, and won evidence) and the fields they carry.
    "spec": ConfigField(str, default="cluster-election"),
    "spec_sha256": SPEC_SHA256_FIELD,
    # auto: votes must be saved before they leave only when every node runs
    # cluster-config-save-behavior sync.
    "require_durable_votes": ConfigField(str, default="auto", choices=("auto", "always", "never")),
    "converge_timeout_s": ConfigField(float, default=60.0, minimum=0, maximum=600),
}


_SAVE_BEHAVIOR = "cluster-config-save-behavior"


# The mapping evidence the oracle reads, with the payload fields it uses.
_SAFETY_EVIDENCE = {
    "started": ("instance_id", "epoch"),
    "granted": ("instance_id", "candidate", "epoch"),
    "acked": ("instance_id", "voter", "epoch", "acks", "quorum"),
    # The won line names no epoch: a win is of the election its winner last started.
    "won": ("instance_id",),
}

# The oracle's checks of properties the claims condition (holds_when): a finding where the claim does not hold
# under the run's constants is reported, not held against the run, as spec conform reports it.
_CLAIMED_CHECKS = {"vote_once_per_epoch": "VoteOncePerEpoch", "one_winner_per_epoch": "OneWinnerPerEpoch"}


def safety_events(config: Mapping[str, Any]) -> dict[str, str]:
    """``{started|granted|acked|won: event name}`` from the bundle's mapping, which must carry the fields the oracle reads."""

    name = config["spec"]
    mapping = pinned_spec_mapping(config)
    events = {}
    for key, fields in _SAFETY_EVIDENCE.items():
        evidence = mapping.evidence.get(key)
        if evidence is None or not evidence.event:
            raise ConfigError(f"spec {name!r}: mapping.yaml has no {key} event evidence")
        missing = sorted(set(fields) - set(evidence.fields))
        if missing:
            raise ConfigError(f"spec {name!r}: the {key} evidence lacks fields {missing}")
        events[key] = evidence.event
    return events


def _claims(config: Mapping[str, Any]) -> Any:
    """The claims of the bundle ``config["spec"]`` names (its mapping is pinned by :func:`pinned_spec_mapping`)."""

    try:
        return load_spec_claims(config["spec"])
    except SpecBundleError as exc:
        raise ConfigError(f"spec {config['spec']!r}: {exc}") from exc


def _validate_safety(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    safety_events(config)
    _claims(config)


def win_epochs(
    wins: Sequence[Mapping[str, Any]], starts: Sequence[Mapping[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Each win with the epoch of the election its winner last started at or before it, and the wins no
    started line of the winner precedes (the won line names no epoch; Valkey wins the election it started)."""

    placed, unplaced = [], []
    for win in wins:
        logged = win.get("logged_at", float("inf"))
        before = [
            start
            for start in starts
            if start["instance_id"] == win["instance_id"] and start.get("logged_at", 0) <= logged
        ]
        if before:
            placed.append({**win, "epoch": int(max(before, key=lambda start: start.get("logged_at", 0))["epoch"])})
        else:
            unplaced.append(dict(win))
    return placed, unplaced


def claim_constants(
    mapping: Any, configs: Mapping[str, Mapping[str, Any]], unread: Sequence[str], inputs: Mapping[str, Any]
) -> dict[str, str]:
    """The model constants as ``spec conform`` derives them from a run, as TLA+ text: a pinned ``value``; a
    ``config`` every node must equal (FALSE once one known setting differs, TRUE when every node's is read
    and equals, else unknown and left out); a scenario ``input``. ``max_of`` constants are left out."""

    values: dict[str, str] = {}
    for name, spec in mapping.constants.items():
        if "value" in spec:
            values[name] = str(spec["value"])
        elif "config" in spec:
            seen = [node[spec["config"]] for node in configs.values() if spec["config"] in node]
            if any(item != spec["equals"] for item in seen):
                values[name] = render(False)
            elif seen and not unread:
                values[name] = render(True)
        elif "input" in spec and spec["input"] in inputs:
            values[name] = render(inputs[spec["input"]] == spec["equals"])
    return values


def claimed_under(prop: Optional[Property], constants: Mapping[str, str]) -> Optional[bool]:
    """Whether ``prop`` is claimed under ``constants`` (its ``holds_when``); None when one it needs is unknown,
    and True for a property the claims do not list (nothing conditions it)."""

    if prop is None:
        return True
    if any(key not in constants for key in prop.holds_when):
        return None
    return prop.holds_under(constants)


def votes_by_epoch(votes: Sequence[Mapping[str, Any]]) -> dict[tuple[str, int], set[str]]:
    """``{(voter, epoch): {candidate, ...}}`` from vote-granted payloads."""

    grouped: dict[tuple[str, int], set[str]] = {}
    for vote in votes:
        grouped.setdefault((vote["instance_id"], int(vote["epoch"])), set()).add(vote["candidate"])
    return grouped


def check_vote_once_per_epoch(votes: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """TLA+ ``VoteOncePerEpoch``: no primary votes for two candidates in one epoch."""

    return [
        {"voter": voter, "epoch": epoch, "candidates": sorted(candidates)}
        for (voter, epoch), candidates in sorted(votes_by_epoch(votes).items())
        if len(candidates) > 1
    ]


def check_one_winner_per_epoch(wins: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """TLA+ ``OneWinnerPerEpoch``: one election epoch never promotes two replicas."""

    winners: dict[int, set[str]] = {}
    for win in wins:
        winners.setdefault(int(win["epoch"]), set()).add(win["instance_id"])
    return [{"epoch": epoch, "winners": sorted(nodes)} for epoch, nodes in sorted(winners.items()) if len(nodes) > 1]


def check_wins_reached_quorum(
    wins: Sequence[Mapping[str, Any]], acks: Sequence[Mapping[str, Any]], primaries: int
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """TLA+ ``Win``: every winner counted votes from a majority of the voting primaries.

    Returns ``(checked, violations)``. A win's votes are the ACK lines its
    candidate logged for the win's epoch; the quorum is the larger of the one
    the candidate logged and a majority of ``primaries``. Distinct voters are
    what count, so a vote counted twice does not make up for a missing one.
    """

    checked, violations = [], []
    for win in wins:
        node, epoch = win["instance_id"], int(win["epoch"])
        counted = [ack for ack in acks if ack["instance_id"] == node and int(ack["epoch"]) == epoch]
        voters = sorted({ack["voter"] for ack in counted})
        quorum = max([primaries // 2 + 1, *(int(ack["quorum"]) for ack in counted if ack.get("quorum") is not None)])
        entry = {"winner": node, "epoch": epoch, "voters": voters, "quorum": quorum}
        checked.append(entry)
        if len(voters) < quorum:
            logged = [int(ack["acks"]) for ack in counted if ack.get("acks") is not None]
            violations.append({**entry, "acks": max(logged, default=len(counted))})
    return checked, violations


def unsaved_votes(
    crashes: Sequence[Mapping[str, Any]],
    votes: Sequence[Mapping[str, Any]],
    acks: Sequence[Mapping[str, Any]],
    node_ids: Mapping[str, str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Votes a crashed primary granted but had not saved (``diskVote < lastVote``).

    Returns ``(unsaved, delivered)``: every vote granted before the crash whose
    epoch is above the ``lastVoteEpoch`` in nodes.conf at the crash, and the
    subset a candidate counted before the voter's new process was spawned (so
    the counted vote is the pre-crash one).  ``delivered`` is the model's
    ``PersistBeforeAck = FALSE`` behavior: a vote left before it was saved.
    A crash without the vars is skipped: the caller cannot judge it.
    """

    unsaved, delivered = [], []
    for crash in crashes:
        disk = crash.get("disk_vars_at_kill") or {}
        disk_vote = disk.get("lastVoteEpoch")
        if disk_vote is None:
            continue
        voter = crash["target"]
        voter_id = node_ids.get(voter)
        # Once the new process is spawned, an ACK may be its vote rather than the killed one's.
        spawned_at = crash.get("restart_spawned_at") or float("inf")
        for vote in votes:
            if vote["instance_id"] != voter or vote.get("logged_at", 0) > crash["applied_at"]:
                continue
            epoch = int(vote["epoch"])
            if epoch <= disk_vote:
                continue
            entry = {
                "voter": voter,
                "candidate": vote["candidate"],
                "epoch": epoch,
                "disk_last_vote_epoch": disk_vote,
                "disk_current_epoch": disk.get("currentEpoch"),
                "vote_logged_at": vote.get("logged_at"),
                "crashed_at": crash["applied_at"],
            }
            unsaved.append(entry)
            if any(
                ack.get("voter") == voter_id
                and int(ack["epoch"]) == epoch
                and node_ids.get(ack["instance_id"]) == vote["candidate"]
                and ack.get("logged_at", 0) < spawned_at
                for ack in acks
            ):
                delivered.append(entry)
    return unsaved, delivered


def same_slot_epoch_collisions(primaries: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Distinct primaries that serve overlapping slots with the same configEpoch."""

    found = []
    for index, left in enumerate(primaries):
        for right in primaries[index + 1 :]:
            if left["node_id"] == right["node_id"] or not left["slots"] or not right["slots"]:
                continue
            if ranges_overlap(left["slots"], right["slots"]) and left["config_epoch"] == right["config_epoch"]:
                found.append(
                    {
                        "config_epoch": left["config_epoch"],
                        "primaries": sorted([left["node_id"], right["node_id"]]),
                    }
                )
    return found


def split_primaries(views: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    """The slots two primaries serve at once, by :func:`~valkey_fuzzer.deployment.local.cluster_nodes.split_slots` over every view's
    CLUSTER NODES: one entry per set of primaries, with their node IDs and those slots as ranges."""

    slots_of: dict[tuple[str, ...], list[int]] = {}
    split = split_slots({instance_id: view["nodes"] for instance_id, view in views.items()})
    for slot, primaries in sorted(split.items()):
        slots_of.setdefault(tuple(sorted(primaries)), []).append(slot)
    found = []
    for primaries, slots in sorted(slots_of.items()):
        ranges: list[list[int]] = []
        for slot in slots:
            if ranges and ranges[-1][1] == slot - 1:
                ranges[-1][1] = slot
            else:
                ranges.append([slot, slot])
        found.append({"primaries": list(primaries), "slots": ranges})
    return found


def trace_collisions(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Sampling rounds in which two nodes claimed the same slots with the same configEpoch."""

    rounds: dict[int, list[Mapping[str, Any]]] = {}
    for record in records:
        if record.get("kind") == "observation" and "error" not in record and record.get("role") == "primary":
            rounds.setdefault(record["round"], []).append(record)
    found = []
    for round_index, claims in sorted(rounds.items()):
        for collision in same_slot_epoch_collisions(claims):
            found.append({"round": round_index, "t": claims[0]["t"], **collision})
    return found


def _payloads(context: Any, name: str) -> list[dict[str, Any]]:
    return [dict(event.payload) for event in context.event_bus.committed_history(name)]


class ElectionSafetyOracle:
    """Check the TLA+ model's election invariants on the run's events, the final views, and the trace.

    The model (formal/cluster-election) says what may never happen; this oracle is where a run that did it
    fails: two votes or two winners in one epoch, a win without a quorum, a vote that left before it was
    saved, or two primaries claiming the same slots.
    """

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = spec.config
        handle = deployment(context)
        # Recorded when the cluster formed and checked on every restart, so a node that is down has one too.
        node_ids = {node.instance_id: node.cluster_node_id for node in handle.instances}
        events = safety_events(config)
        mapping = pinned_spec_mapping(config)
        claims = _claims(config)
        votes = _payloads(context, events["granted"])
        acks = _payloads(context, events["acked"])
        wins, unplaced_wins = win_epochs(_payloads(context, events["won"]), _payloads(context, events["started"]))
        crashes = [
            item.value
            for fault in config["crash_faults"]
            for item in evidence_of(evidence, FAULT_INJECTED, fault)
            if isinstance(item.value, Mapping)
        ]
        unknown_ids = sorted(instance_id for instance_id, node_id in node_ids.items() if not node_id)
        if crashes and unknown_ids:
            # Without them a counted vote of the crashed voter cannot be matched to the vote it granted.
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"cannot check the votes of crashed nodes: the node IDs of {', '.join(unknown_ids)} are unknown",
            )
        unsaved, delivered = unsaved_votes(crashes, votes, acks, node_ids)

        views, converged, unanswered, changing = self._converge(context, handle, config["converge_timeout_s"])
        config_keys = sorted(
            {_SAVE_BEHAVIOR} | {item["config"] for item in mapping.constants.values() if "config" in item}
        )
        configs, unread_save = self._configs(context, handle, config["converge_timeout_s"], config_keys)
        save_behavior = {instance_id: values.get(_SAVE_BEHAVIOR) for instance_id, values in configs.items()}
        # The constants spec conform derives for this run decide which claimed properties hold here.
        constants = claim_constants(mapping, configs, unread_save, plain_config(context.plan.inputs))
        violations: dict[str, Any] = {}
        # Findings of a claimed property where the claim does not hold under the constants: reported, not failed.
        outside_claim: dict[str, Any] = {}
        # Findings whose claim cannot be decided: a constant they need is unknown.
        unjudged: dict[str, Any] = {}
        for check, found in (
            ("vote_once_per_epoch", check_vote_once_per_epoch(votes)),
            ("one_winner_per_epoch", check_one_winner_per_epoch(wins)),
        ):
            if not found:
                continue
            prop = next((item for item in claims.properties if item.name == _CLAIMED_CHECKS[check]), None)
            claimed = claimed_under(prop, constants)
            bucket = unjudged if claimed is None else violations if claimed else outside_claim
            bucket[check] = found
        durable = config["require_durable_votes"]
        if durable == "auto":
            if any(value != "sync" for value in save_behavior.values()):
                durable = "never"
            else:
                durable = "unknown" if unread_save else "always"
        if durable == "always" and delivered:
            violations["vote_left_before_save"] = delivered
        # Valkey's cluster size: the primaries that serve slots, one per shard.
        primaries = max(
            [len({node.shard_id for node in handle.instances})]
            + [len([entry for entry in view["primaries"] if entry["slots"]]) for view in views.values()]
        )
        # Without any ACK line the quorum check cannot run (servers before 9.2 log none).
        checked_wins, short = check_wins_reached_quorum(wins, acks, primaries) if acks else ([], [])
        if short:
            violations["win_without_quorum"] = short
        split = split_primaries(views)
        if split:
            violations["split_primary"] = split
        collisions = [
            {"view": instance_id, **collision}
            for instance_id, view in sorted(views.items())
            for collision in same_slot_epoch_collisions(view["primaries"])
        ]
        trace_path = None
        # Why the trace's collision check could not run; None when it ran or none was asked for.
        unread_trace = None
        if config["trace"]:
            recorded = evidence_of(evidence, TRACE_EVIDENCE, config["trace"])
            if not (recorded and recorded[-1].artifacts):
                unread_trace = f"{config['trace']!r} recorded no trace"
            else:
                trace_path = recorded[-1].artifacts[0]
                try:
                    collisions.extend(trace_collisions(read_jsonl(trace_path)))
                except (OSError, ValueError) as exc:
                    unread_trace = f"the trace {trace_path} could not be read: {type(exc).__name__}: {exc}"
        if collisions:
            violations["same_slots_same_config_epoch"] = collisions
        owners = {}
        for view in views.values():
            for entry in view["primaries"]:
                if entry["slots"]:
                    owners[entry["node_id"]] = entry["config_epoch"]
        duplicate_epochs = sorted({epoch for epoch in owners.values() if list(owners.values()).count(epoch) > 1})

        record = {
            "save_behavior": save_behavior,
            "require_durable_votes": durable,
            "claim_constants": constants,
            "outside_claim": outside_claim,
            "votes": len(votes),
            "acks": len(acks),
            "elections_won": sorted({int(win["epoch"]) for win in wins}),
            "wins": checked_wins,
            "crashes": [
                {
                    key: crash.get(key)
                    for key in (
                        "target",
                        "applied_at",
                        "restart_spawned_at",
                        "restart_ready_at",
                        "disk_vars_at_kill",
                        "trigger_payload",
                    )
                }
                for crash in crashes
            ],
            "unsaved_votes_at_crash": unsaved,
            "unsaved_votes_delivered": delivered,
            "converged": converged,
            "final_epochs": {instance_id: view_summary(view) for instance_id, view in sorted(views.items())},
            "cross_shard_duplicate_config_epochs": duplicate_epochs,
            "trace": trace_path,
        }
        if violations:
            return make_result(
                spec,
                ComponentStatus.FAIL,
                "election safety violated: " + ", ".join(sorted(violations)),
                diagnostics={**record, "violations": violations},
            )
        if unread_trace is not None:
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"cannot check the trace for primaries of the same slots in one configEpoch: {unread_trace}",
                diagnostics=record,
            )
        if not converged and (unanswered or not views):
            # A running node whose view could not be read may hold a split, or be the one that agrees: unjudged.
            return make_result(
                spec,
                ComponentStatus.ERROR,
                "cannot read the cluster views of running node(s) "
                f"{', '.join(unanswered) or 'all'} within {config['converge_timeout_s']:g}s",
                diagnostics={**record, "unanswered": unanswered},
            )
        if not converged and changing:
            # Convergence is bounded by converge_timeout_s, a liveness bound: views still settling at the deadline
            # were not read in a final state, so nothing says they disagree.
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"cluster views were still changing at the converge_timeout_s deadline ({config['converge_timeout_s']:g}s)",
                diagnostics=record,
            )
        if not converged:
            return make_result(
                spec,
                ComponentStatus.FAIL,
                "cluster views did not converge on one primary per slot",
                diagnostics=record,
            )
        blind = sorted(
            {
                crash["target"]
                for crash in crashes
                if (crash.get("disk_vars_at_kill") or {}).get("lastVoteEpoch") is None
            }
        )
        if durable in ("always", "unknown") and blind:
            # Without the vars the node restarted with, a vote it lost to the crash cannot be told from one it saved.
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"cannot check that the votes of crashed node(s) {', '.join(blind)} were saved: no nodes.conf "
                "vars were read at the kill",
                diagnostics=record,
            )
        if durable == "unknown" and delivered:
            # Every node that answered saves before it votes, so these votes may be violations.
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"cannot tell whether {len(delivered)} vote(s) that left before they were saved break "
                f"cluster-config-save-behavior sync: the setting of {', '.join(unread_save)} is unknown",
                diagnostics={**record, "unread_save_behavior": unread_save},
            )
        if unjudged:
            # The claim holds under constants a setting nobody could read decides: the finding may be a violation.
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"cannot tell whether {', '.join(sorted(unjudged))} break the claim: the setting of "
                f"{', '.join(unread_save)} is unknown",
                diagnostics={**record, "unjudged": unjudged, "unread_save_behavior": unread_save},
            )
        if unplaced_wins:
            # Without the started line of the winner, the win's epoch is unknown: neither check above saw it.
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"cannot tell the epoch of {len(unplaced_wins)} election win(s): no 'Starting a failover election' "
                f"line of the winner was published as {events['started']!r} before them",
                diagnostics={**record, "unplaced_wins": unplaced_wins},
            )
        if wins and not acks:
            # Nothing says the winners had a majority: the check cannot run, so the run cannot pass.
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"cannot check that {len(wins)} election winner(s) reached quorum: no 'Failover auth ACK' line "
                f"was published as {events['acked']!r}; the oracle needs Valkey 9.2 or later, which logs each "
                "counted vote with its count and quorum",
                diagnostics=record,
            )
        evidence_out = [make_evidence(spec, "valkey.cluster.election-safety.verified", "oracle.completed", record)]
        if unsaved:
            # Coverage signal: the run reached the model's dangerous state
            # (a crash with a vote in memory that nodes.conf did not hold).
            evidence_out.append(
                make_evidence(spec, "valkey.cluster.vote.unsaved-at-crash", "oracle.completed", {"votes": unsaved})
            )
        claimed = (
            "; outside the claim (not claimed under "
            + ", ".join(f"{key} = {value}" for key, value in sorted(constants.items()))
            + f"): {', '.join(sorted(outside_claim))}"
            if outside_claim
            else "; one winner per epoch, each with a quorum of votes"
        )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{len(votes)} vote(s), {len(record['elections_won'])} epoch(s) won, "
            f"{len(unsaved)} unsaved at crash ({len(delivered)} delivered){claimed}",
            evidence=evidence_out,
        )

    @staticmethod
    def _configs(
        context: Any, handle: Any, timeout: float, keys: Sequence[str]
    ) -> tuple[dict[str, dict[str, Any]], list[str]]:
        """Every running node's settings ``keys`` (``CONFIG GET``), and the nodes whose settings are unknown:
        down, or it did not answer within ``timeout``."""

        def read(instance_id: str) -> tuple[bool, Any]:
            with handle.new_client(instance_id, timeout=2.0) as client:
                try:
                    values: dict[str, Any] = {}
                    for key in keys:
                        values.update(client.config_get(key))
                    return True, values
                except Exception:
                    return False, None

        ids = [node.instance_id for node in handle.instances]
        polled = poll_each(
            context, [instance_id for instance_id in ids if handle.is_alive(instance_id)], read, timeout_s=timeout
        )
        known = {instance_id: value for instance_id, (answered, value) in polled.items() if answered}
        return known, [instance_id for instance_id in ids if instance_id not in known]

    def _converge(self, context: Any, handle: Any, timeout: float) -> tuple[dict[str, dict], bool, list[str], bool]:
        """Poll until every running node answers and agrees on slot owners and their configEpochs: the last
        views, whether they agreed, the running nodes that did not answer, and whether the views were still
        changing between the last two polls (a liveness bound reached, not a disagreement)."""

        # The last two polls' owner maps and cluster states, to tell a view still settling from one that disagrees.
        pictures: deque = deque(maxlen=2)

        def check() -> tuple[bool, tuple[dict[str, dict], list[str]]]:
            running = [node.instance_id for node in handle.instances if handle.is_alive(node.instance_id)]
            read = {instance_id: read_view(handle, instance_id) for instance_id in running}
            views = {instance_id: view for instance_id, view in read.items() if view is not None}
            unanswered = [instance_id for instance_id, view in read.items() if view is None]
            maps = set()
            for view in views.values():
                owners = tuple(
                    sorted(
                        (tuple(map(tuple, entry["slots"])), entry["node_id"], entry["config_epoch"])
                        for entry in view["primaries"]
                        if entry["slots"] and "fail" not in entry["flags"] and "fail?" not in entry["flags"]
                    )
                )
                maps.add(owners)
            pictures.append((frozenset(maps), tuple(sorted((name, view["state"]) for name, view in views.items()))))
            agreed = (
                not unanswered
                and len(maps) == 1
                and all(view["state"] == "ok" for view in views.values())
                and not split_primaries(views)
            )
            return agreed, (views, unanswered)

        agreed, (views, unanswered) = poll_until(check, timeout_s=timeout, interval_s=0.2, context=context)
        changing = len(pictures) == 2 and pictures[0] != pictures[1]
        return views, agreed, unanswered, changing


_SAFETY_DECLARES = Declarations(
    evidence={
        "valkey.cluster.election-safety.verified": declared(
            mapping_of(
                {
                    "save_behavior": MAPPING,
                    "require_durable_votes": TEXT,
                    "claim_constants": MAPPING,
                    "outside_claim": MAPPING,
                    "votes": INTEGER,
                    "acks": INTEGER,
                    "elections_won": LIST,
                    "wins": LIST,
                    "crashes": LIST,
                    "unsaved_votes_at_crash": LIST,
                    "unsaved_votes_delivered": LIST,
                    "converged": FLAG,
                    "final_epochs": MAPPING,
                    "cross_shard_duplicate_config_epochs": LIST,
                    "trace": nullable(str),
                }
            )
        ),
        "valkey.cluster.vote.unsaved-at-crash": declared(mapping_of({"votes": LIST})),
    }
)

REGISTRATIONS = (
    component_registration(
        "valkey.oracle.cluster-election-safety",
        ElectionSafetyOracle,
        _SAFETY_FIELDS,
        check=_validate_safety,
        pins_spec=True,
        deployment_modes=("cluster",),
        declares=_SAFETY_DECLARES,
    ),
)
