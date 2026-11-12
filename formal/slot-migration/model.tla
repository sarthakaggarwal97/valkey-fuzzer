---------------------------------- MODULE model ----------------------------------
(***************************************************************************)
(* Atomic slot migration in Valkey (src/cluster_migrateslots.c, CLUSTER    *)
(* MIGRATESLOTS) of one slot from a source primary S to a target primary   *)
(* T, which has one replica R.                                             *)
(*                                                                         *)
(* S connects to T (CLUSTER SYNCSLOTS ESTABLISH; T hides the slot's keys   *)
(* while it imports them), forks a snapshot of the slot, and streams the   *)
(* slot's writes made after the fork behind it on the same connection.     *)
(* When T has the snapshot it asks S to pause (REQUEST-PAUSE); S pauses    *)
(* client writes and sends PAUSED behind the last write; T asks for the    *)
(* failover (REQUEST-FAILOVER), S grants it (FAILOVER-GRANTED), and T      *)
(* claims the slot under a new config epoch without consensus              *)
(* (performSlotImportJobFailover), shows the keys, and broadcasts the      *)
(* claim on the cluster bus. S learns the claim, deletes its keys in the   *)
(* slot (a dirty slot), and unpauses. T's replica R receives the import    *)
(* through T's replication stream (ESTABLISH, the keys, SYNCSLOTS FINISH)  *)
(* and the claim through the bus.                                          *)
(*                                                                         *)
(* Faults: the migration connection drops, T crashes (R is then promoted), *)
(* a bus message from a dead node is lost, an operator cancels the export  *)
(* (CLUSTER CANCELSLOTMIGRATIONS), and S's pause ends at its timeout.      *)
(*                                                                         *)
(* Three constants choose between Valkey as it is and a fix:               *)
(*   - CancelKeepsPause: a cancel after S granted the failover keeps S     *)
(*     paused until it learns the slot's owner. Valkey unpauses            *)
(*     (finishSlotMigrationJob), while T may still claim the slot.         *)
(*   - ExpiryAfterClaim: S's pause may end while T has claimed or may      *)
(*     still claim. Valkey allows it and logs "Write loss risk!"; FALSE    *)
(*     assumes the claim reaches S before the pause ends.                  *)
(*   - ClaimReachesReplica: R learns T's claim before T acts on it (as     *)
(*     legacy CLUSTER SETSLOT does by replicating to the replicas first).  *)
(*     Valkey sends it to R only on the bus.                               *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    MaxWrites,           \* client writes to the slot, in all
    MaxFaults,           \* link drops, cancels, pause timeouts, crashes, lost bus messages
    CancelKeepsPause,
    ExpiryAfterClaim,
    ClaimReachesReplica

S == "S"
T == "T"
R == "R"
Nodes == {S, T, R}

VARIABLES
    role,    \* node -> "primary" | "replica" | "dead"
    view,    \* node -> [owner, epoch]: who owns the slot, and under which config epoch, as the node sees it
    epoch,   \* currentEpoch: the highest epoch taken
    data,    \* node -> the slot's writes the node holds, in order
    hidden,  \* node -> its keys in the slot are being imported (kvstoreSetIsImporting)
    exp,     \* S's export job
    imp,     \* T's import job
    track,   \* R's tracking job (SLOT_IMPORT_OCCURRING_ON_PRIMARY)
    toT,     \* the migration connection, S to T: messages in flight
    toS,     \* the migration connection, T to S
    repl,    \* T's replication stream to R
    bus,     \* cluster bus messages in flight: [to, owner, epoch]
    paused,  \* S pauses client writes (PAUSE_DURING_SLOT_MIGRATION)
    acked,   \* the writes acknowledged to clients
    writes,
    faults

vars == <<role, view, epoch, data, hidden, exp, imp, track, toT, toS, repl, bus,
          paused, acked, writes, faults>>

Live(n) == role[n] # "dead"
\* The export is in progress on S (isSlotMigrationJobInProgress).
Exporting == exp \in {"establish", "stream", "paused", "granted"}
\* The import is in progress on T.
Importing == imp \in {"receive", "waitPaused", "requested"}
\* S feeds the slot's writes to T (clusterFeedSlotExportJobs: from the fork on).
Feeding == exp \in {"stream", "paused", "granted"}
GrantInFlight == \E i \in 1..Len(toT) : toT[i].k = "granted"
\* T has claimed the slot, or will when the grant arrives.
TMayClaim == imp = "success" \/ (role[T] = "primary" /\ imp = "requested" /\ GrantInFlight)
\* T streams to R while R is its replica.
ToReplica(seq) == IF role[R] = "replica" THEN repl \o seq ELSE repl

\* A node takes client writes to the slot: a primary that owns it and is not paused.
Accepts(n) == role[n] = "primary" /\ view[n].owner = n /\ ~(n = S /\ paused)

Holds(n, w) == \E i \in 1..Len(data[n]) : data[n][i] = w

\* w is in a stream of messages: fed, or in a snapshot.
InStream(seq, w) ==
    \E i \in 1..Len(seq) :
        \/ seq[i].k = "write" /\ seq[i].w = w
        \/ seq[i].k = "snapshot" /\ \E j \in 1..Len(seq[i].d) : seq[i].d[j] = w

Init ==
    /\ role = [n \in Nodes |-> IF n = R THEN "replica" ELSE "primary"]
    /\ view = [n \in Nodes |-> [owner |-> S, epoch |-> 1]]
    /\ epoch = 1
    /\ data = [n \in Nodes |-> <<>>]
    /\ hidden = [n \in Nodes |-> FALSE]
    /\ exp = "none"
    /\ imp = "none"
    /\ track = "none"
    /\ toT = <<>>
    /\ toS = <<>>
    /\ repl = <<>>
    /\ bus = {}
    /\ paused = FALSE
    /\ acked = {}
    /\ writes = 0
    /\ faults = 0

-----------------------------------------------------------------------------
(* Clients *)

\* A client writes to the node that takes writes to the slot. S feeds the
\* write to T during the export; T streams its own writes to R.
Write(n) ==
    LET w == writes + 1 IN
    /\ Accepts(n)
    /\ writes < MaxWrites
    /\ data' = [data EXCEPT ![n] = Append(@, w)]
    /\ acked' = acked \cup {w}
    /\ writes' = w
    /\ toT' = IF n = S /\ Feeding THEN Append(toT, [k |-> "write", w |-> w]) ELSE toT
    /\ repl' = IF n = T THEN ToReplica(<<[k |-> "write", w |-> w]>>) ELSE repl
    /\ UNCHANGED <<role, view, epoch, hidden, exp, imp, track, toS, bus, paused, faults>>

-----------------------------------------------------------------------------
(* The migration *)

\* CLUSTER MIGRATESLOTS on S: connect to T and send ESTABLISH.
Start ==
    /\ exp = "none"
    /\ Accepts(S)
    /\ exp' = "establish"
    /\ toT' = <<[k |-> "establish"]>>
    /\ UNCHANGED <<role, view, epoch, data, hidden, imp, track, toS, repl, bus, paused,
                   acked, writes, faults>>

\* T accepts ESTABLISH (clusterCommandSyncSlotsEstablish): it hides the slot's
\* keys, propagates ESTABLISH to R, and replies +OK.
TEstablish ==
    /\ toT # <<>> /\ Head(toT).k = "establish"
    /\ role[T] = "primary"
    /\ imp = "none"
    /\ view[T].owner = S
    /\ toT' = Tail(toT)
    /\ imp' = "receive"
    /\ hidden' = [hidden EXCEPT ![T] = TRUE]
    /\ repl' = ToReplica(<<[k |-> "establish"]>>)
    /\ toS' = Append(toS, "ok")
    /\ UNCHANGED <<role, view, epoch, data, exp, track, bus, paused, acked, writes, faults>>

\* S reads +OK and forks the snapshot (slotExportJobBeginSnapshotToTargetSocket).
\* The snapshot and SNAPSHOT-EOF go first; the writes fed from now on follow.
SFork ==
    /\ toS # <<>> /\ Head(toS) = "ok"
    /\ exp = "establish"
    /\ toS' = Tail(toS)
    /\ exp' = "stream"
    /\ toT' = Append(toT, [k |-> "snapshot", d |-> data[S]])
    /\ UNCHANGED <<role, view, epoch, data, hidden, imp, track, repl, bus, paused, acked,
                   writes, faults>>

\* T applies the snapshot or a fed write, hidden, and streams it to R. After
\* SNAPSHOT-EOF it sends REQUEST-PAUSE.
TApply ==
    LET m == Head(toT) IN
    /\ toT # <<>> /\ m.k \in {"snapshot", "write"}
    /\ role[T] = "primary"
    /\ toT' = Tail(toT)
    /\ data' = [data EXCEPT ![T] = IF m.k = "snapshot" THEN m.d ELSE Append(@, m.w)]
    /\ repl' = ToReplica(<<m>>)
    /\ IF m.k = "snapshot"
         THEN /\ imp' = "waitPaused"
              /\ toS' = Append(toS, "reqPause")
         ELSE UNCHANGED <<imp, toS>>
    /\ UNCHANGED <<role, view, epoch, hidden, exp, track, bus, paused, acked, writes, faults>>

\* S pauses client writes and sends PAUSED behind the last fed write
\* (slotExportTryDoPause).
SPause ==
    /\ toS # <<>> /\ Head(toS) = "reqPause"
    /\ exp = "stream"
    /\ toS' = Tail(toS)
    /\ paused' = TRUE
    /\ exp' = "paused"
    /\ toT' = Append(toT, [k |-> "paused"])
    /\ UNCHANGED <<role, view, epoch, data, hidden, imp, track, repl, bus, acked, writes, faults>>

\* T has every write: it sends REQUEST-FAILOVER.
TPaused ==
    /\ toT # <<>> /\ Head(toT).k = "paused"
    /\ role[T] = "primary"
    /\ imp = "waitPaused"
    /\ toT' = Tail(toT)
    /\ imp' = "requested"
    /\ toS' = Append(toS, "reqFailover")
    /\ UNCHANGED <<role, view, epoch, data, hidden, exp, track, repl, bus, paused, acked,
                   writes, faults>>

\* S, still paused, grants the failover (clusterCommandSyncSlotsRequestFailover).
\* A pause that ended first failed the export (PauseExpires).
SGrant ==
    /\ toS # <<>> /\ Head(toS) = "reqFailover"
    /\ exp = "paused"
    /\ toS' = Tail(toS)
    /\ exp' = "granted"
    /\ toT' = Append(toT, [k |-> "granted"])
    /\ UNCHANGED <<role, view, epoch, data, hidden, imp, track, repl, bus, paused, acked,
                   writes, faults>>

\* T reads FAILOVER-GRANTED and, in the same event-loop pass, claims the slot
\* under a new epoch, shows the keys, propagates FINISH to R, and broadcasts
\* the claim (performSlotImportJobFailover, finishSlotMigrationJob).
TClaim ==
    LET e == epoch + 1
        claim == [owner |-> T, epoch |-> e]
    IN
    /\ toT # <<>> /\ Head(toT).k = "granted"
    /\ role[T] = "primary"
    /\ imp = "requested"
    /\ toT' = Tail(toT)
    /\ epoch' = e
    /\ view' = [view EXCEPT ![T] = claim,
                            ![R] = IF ClaimReachesReplica /\ role[R] = "replica" THEN claim ELSE @]
    /\ hidden' = [hidden EXCEPT ![T] = FALSE]
    /\ imp' = "success"
    /\ repl' = ToReplica(<<[k |-> "finish", ok |-> TRUE]>>)
    /\ bus' = bus \cup {[to |-> n, owner |-> T, epoch |-> e] :
                          n \in IF ClaimReachesReplica THEN {S} ELSE {S, R}}
    /\ UNCHANGED <<role, data, exp, track, toS, paused, acked, writes, faults>>

\* T reads the close of a connection S ended; an import in progress fails.
TClose ==
    /\ toT # <<>> /\ Head(toT).k = "close"
    /\ toT' = Tail(toT)
    /\ imp' = IF Importing THEN "cleanup" ELSE imp
    /\ UNCHANGED <<role, view, epoch, data, hidden, exp, track, toS, repl, bus, paused, acked,
                   writes, faults>>

\* A failed import is cleaned up in beforeSleep: T deletes the keys of a slot it
\* does not own (delKeysNotOwnedByMyself), shows the slot, and propagates the
\* deletes and FINISH to R.
TCleanup ==
    /\ imp = "cleanup"
    /\ role[T] = "primary"
    /\ data' = [data EXCEPT ![T] = IF view[T].owner = T THEN @ ELSE <<>>]
    /\ hidden' = [hidden EXCEPT ![T] = FALSE]
    /\ imp' = "failed"
    /\ repl' = ToReplica(<<[k |-> "del"], [k |-> "finish", ok |-> FALSE]>>)
    /\ UNCHANGED <<role, view, epoch, exp, track, toT, toS, bus, paused, acked, writes, faults>>

\* R applies T's replication stream: ESTABLISH starts a tracking job that hides
\* the slot, then the keys, the deletes, and FINISH, which shows the slot.
RApply ==
    LET m == Head(repl) IN
    /\ repl # <<>>
    /\ role[R] = "replica"
    /\ repl' = Tail(repl)
    /\ data' = [data EXCEPT ![R] = CASE m.k = "snapshot" -> m.d
                                     [] m.k = "write" -> Append(@, m.w)
                                     [] m.k = "del" -> <<>>
                                     [] OTHER -> @]
    /\ hidden' = [hidden EXCEPT ![R] = CASE m.k = "establish" -> TRUE
                                         [] m.k = "finish" -> FALSE
                                         [] OTHER -> @]
    /\ track' = CASE m.k = "establish" -> "tracking"
                  [] m.k = "finish" -> IF m.ok THEN "success" ELSE "failed"
                  [] OTHER -> track
    /\ UNCHANGED <<role, view, epoch, exp, imp, toT, toS, bus, paused, acked, writes, faults>>

\* Node n reads a bus message. A newer claim updates its view
\* (clusterUpdateSlotsConfigWith): a primary that loses the slot deletes its keys
\* in it; S's export then ends and S unpauses (clusterUpdateSlotExportsOnOwnershipChange).
Deliver(n) ==
    \E m \in {x \in bus : x.to = n} :
    LET newer == Live(n) /\ m.epoch > view[n].epoch
        loses == role[n] = "primary" /\ view[n].owner = n /\ m.owner # n
        ends == n = S /\ (Exporting \/ paused)
    IN
    /\ bus' = bus \ {m}
    /\ view' = IF newer THEN [view EXCEPT ![n] = [owner |-> m.owner, epoch |-> m.epoch]] ELSE view
    /\ data' = IF newer /\ loses THEN [data EXCEPT ![n] = <<>>] ELSE data
    /\ exp' = IF newer /\ ends /\ Exporting THEN (IF m.owner = T THEN "success" ELSE "failed") ELSE exp
    /\ paused' = IF newer /\ ends THEN FALSE ELSE paused
    /\ UNCHANGED <<role, epoch, hidden, imp, track, toT, toS, repl, acked, writes, faults>>

-----------------------------------------------------------------------------
(* Faults and operators *)

\* The migration connection drops: what was in flight is lost. S fails the
\* export and unpauses, unless it granted the failover: then T may have
\* claimed, and S stays paused (clusterHandleSlotMigrationClientClose). T fails
\* an import in progress.
DropLink ==
    LET fails == exp \in {"establish", "stream", "paused"} IN
    /\ Exporting \/ Importing
    /\ faults < MaxFaults
    /\ toT' = <<>>
    /\ toS' = <<>>
    /\ exp' = IF fails THEN "failed" ELSE exp
    /\ paused' = IF fails THEN FALSE ELSE paused
    /\ imp' = IF Importing THEN "cleanup" ELSE imp
    /\ faults' = faults + 1
    /\ UNCHANGED <<role, view, epoch, data, hidden, track, repl, bus, acked, writes>>

\* CLUSTER CANCELSLOTMIGRATIONS on S: the export ends and S closes the
\* connection, after what it already sent. Valkey unpauses
\* (finishSlotMigrationJob), even after the grant.
Cancel ==
    /\ Exporting
    /\ faults < MaxFaults
    /\ exp' = "cancelled"
    /\ paused' = IF exp = "granted" /\ CancelKeepsPause THEN paused ELSE FALSE
    /\ toT' = Append(toT, [k |-> "close"])
    /\ toS' = <<>>
    /\ faults' = faults + 1
    /\ UNCHANGED <<role, view, epoch, data, hidden, imp, track, repl, bus, acked, writes>>

\* S's pause reaches its end (cluster-manual-failover-timeout, renewed by the
\* grant): S fails the export, closes the connection, and takes writes again.
\* Unless ExpiryAfterClaim, only while T can no longer claim the slot.
PauseExpires ==
    /\ paused
    /\ ExpiryAfterClaim \/ ~TMayClaim
    /\ faults < MaxFaults
    /\ paused' = FALSE
    /\ exp' = IF Exporting THEN "failed" ELSE exp
    /\ toT' = IF Exporting THEN Append(toT, [k |-> "close"]) ELSE toT
    /\ toS' = IF Exporting THEN <<>> ELSE toS
    /\ faults' = faults + 1
    /\ UNCHANGED <<role, view, epoch, data, hidden, imp, track, repl, bus, acked, writes>>

\* T crashes. S sees the connection close (as in DropLink); R still receives
\* everything T wrote to it, and T's bus messages already sent still arrive.
CrashT ==
    LET fails == exp \in {"establish", "stream", "paused"} IN
    /\ role[T] = "primary"
    /\ faults < MaxFaults
    /\ role' = [role EXCEPT ![T] = "dead"]
    /\ toT' = <<>>
    /\ toS' = <<>>
    /\ exp' = IF fails THEN "failed" ELSE exp
    /\ paused' = IF fails THEN FALSE ELSE paused
    /\ faults' = faults + 1
    /\ UNCHANGED <<view, epoch, data, hidden, imp, track, repl, bus, acked, writes>>

\* A bus message to n whose sender died never arrives: the sender's later pings
\* would have repeated it.
LoseBus(n) ==
    \E m \in {x \in bus : x.to = n} :
    /\ role[m.owner] = "dead"
    /\ faults < MaxFaults
    /\ bus' = bus \ {m}
    /\ faults' = faults + 1
    /\ UNCHANGED <<role, view, epoch, data, hidden, exp, imp, track, toT, toS, repl, paused,
                   acked, writes>>

\* T is marked FAIL and R, having applied T's stream and read every bus message
\* that reaches it (node timeout outlasts delivery), wins the election
\* (clusterFailoverReplaceYourPrimary): it claims the slots T owns in R's view
\* under a new epoch. A tracking job fails (clusterCleanSlotImportsOnPromotion),
\* and the keys of a slot R does not own are deleted (delKeysNotOwnedByMyself,
\* verifyClusterConfigWithData).
Promote ==
    LET e == epoch + 1
        owns == view[R].owner = T
    IN
    /\ role[R] = "replica"
    /\ role[T] = "dead"
    /\ repl = <<>>
    /\ \A m \in bus : m.to # R
    /\ role' = [role EXCEPT ![R] = "primary"]
    /\ epoch' = e
    /\ view' = IF owns THEN [view EXCEPT ![R] = [owner |-> R, epoch |-> e]] ELSE view
    /\ bus' = IF owns THEN bus \cup {[to |-> S, owner |-> R, epoch |-> e]} ELSE bus
    /\ data' = IF owns THEN data ELSE [data EXCEPT ![R] = <<>>]
    /\ hidden' = [hidden EXCEPT ![R] = FALSE]
    /\ track' = IF track = "tracking" THEN "failed" ELSE track
    /\ UNCHANGED <<exp, imp, toT, toS, repl, paused, acked, writes, faults>>

\* Nothing happens. Once the bounds are used up, some states (S paused for a
\* claim that was lost) enable no other action; this keeps TLC from reporting
\* them as deadlocks.
Idle == UNCHANGED vars

-----------------------------------------------------------------------------
Next ==
    \/ \E n \in Nodes : Write(n)
    \/ Start \/ TEstablish \/ SFork \/ TApply \/ SPause \/ TPaused \/ SGrant \/ TClaim
    \/ TClose \/ TCleanup \/ RApply
    \/ \E n \in Nodes : Deliver(n) \/ LoseBus(n)
    \/ DropLink \/ Cancel \/ PauseExpires \/ CrashT \/ Promote
    \/ Idle

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety properties *)

\* At most one node takes writes to the slot: the source stops before the
\* target starts.
OneWriter == \A a, b \in Nodes : Accepts(a) /\ Accepts(b) => a = b

\* Every acknowledged write is held by a live node, or is on its way to R in
\* T's replication stream.
NoAckedWriteLost ==
    \A w \in acked :
        \/ \E n \in Nodes : Live(n) /\ Holds(n, w)
        \/ role[R] = "replica" /\ InStream(repl, w)

TypeOK ==
    /\ role \in [Nodes -> {"primary", "replica", "dead"}]
    /\ \A n \in Nodes : view[n].owner \in Nodes /\ view[n].epoch \in 1..epoch
    /\ hidden \in [Nodes -> BOOLEAN]
    /\ exp \in {"none", "establish", "stream", "paused", "granted", "success", "failed", "cancelled"}
    /\ imp \in {"none", "receive", "waitPaused", "requested", "cleanup", "success", "failed"}
    /\ track \in {"none", "tracking", "success", "failed"}
    /\ paused \in BOOLEAN
    /\ acked \subseteq 1..MaxWrites
    /\ writes \in 0..MaxWrites
    /\ faults \in 0..MaxFaults
=============================================================================
