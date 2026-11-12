---------------------------------- MODULE model ----------------------------------
(***************************************************************************)
(* Primary-replica replication in Valkey (src/replication.c): PSYNC with   *)
(* two replication IDs, full sync over one channel or two (dual-channel    *)
(* replication), the backlog, promotion, REPLICAOF, and restarts from an   *)
(* RDB file.                                                               *)
(*                                                                         *)
(* A node's data set is the sequence of writes it applied. Its offset is   *)
(* primary_repl_offset, which a write advances only on a node with a       *)
(* backlog (feedReplicationBuffer returns early without one), so a node's  *)
(* data and its offset can drift apart: Gap(n). A replication ID with an   *)
(* offset names a data set. Valkey keeps that naming unique by a few       *)
(* rules, each an action guard here:                                       *)
(*   - PSYNC continues only on the primary's ID, or on its secondary ID up *)
(*     to second_replid_offset, and only from an offset in the backlog     *)
(*     (primaryTryPartialResynchronization);                               *)
(*   - a promotion moves the ID to the secondary ID and takes a new one    *)
(*     (shiftReplicationId); freeing a backlog, or creating one from       *)
(*     scratch for a full sync, takes a new ID (replicationCron,           *)
(*     syncCommand);                                                       *)
(*   - an RDB file carries the ID and offset only when its node has a      *)
(*     backlog (a primary) or a primary, live or cached (a replica):       *)
(*     rdbPopulateSaveInfo; a restart restores them (loadDataFromDisk).    *)
(*                                                                         *)
(* Dual-channel full sync (DualChannel): the primary forks a snapshot at   *)
(* an offset ($ENDOFF), the replica asks on its main channel to continue   *)
(* from that offset, buffers the stream, and loads the snapshot. The two   *)
(* finish in either order. When the load finishes first, Valkey keeps the  *)
(* replica's old ID and offset, with a new backlog after that offset,      *)
(* until +CONTINUE arrives (replicaAfterLoadPrimaryRDB; fuzzer issue       *)
(* #133). AdoptOnLoad = TRUE models the fix that adopts the snapshot's ID  *)
(* and offset when the load ends.                                          *)
(*                                                                         *)
(* Faults: links drop at any time, nodes crash and restart from their RDB  *)
(* file, a load can fail, the backlog drops old entries, and an operator   *)
(* promotes and re-points nodes in any order.                              *)
(*                                                                         *)
(* Not modeled: chained replicas (a replica of a replica), writes to a     *)
(* writable replica, AOF (a restart from it restores no replication ID: a  *)
(* save without one), expiry, and the bytes of the stream: one write is    *)
(* one offset.                                                             *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    Nodes,        \* the servers
    MaxWrites,    \* client writes, in all
    MaxIds,       \* replication IDs ever made (each node boots with one)
    MaxCrashes,   \* process crashes
    MaxSwitches,  \* promotions and REPLICAOF changes
    DualChannel,  \* dual-channel-replication-enabled on every node
    AdoptOnLoad   \* TRUE: a dual-channel replica adopts the snapshot's ID and
                  \* offset when the load ends; FALSE: Valkey keeps its old ones
                  \* until +CONTINUE

First == CHOOSE n \in Nodes : TRUE  \* the node that starts as the primary
None == 0  \* no replication ID: clearReplicationId2 writes forty '0's

\* A replica's link: "down"; "up" (streaming); "rdb" (dual-channel: the
\* snapshot is on its way, the main channel has no answer yet);
\* "rdbContinued" (+CONTINUE came before the load ended); "loaded" (the load
\* ended before +CONTINUE). A primary's link is "none".
Linked == {"up", "rdb", "rdbContinued", "loaded"}

VARIABLES
    role,       \* node -> "primary" | "replica"
    primaryOf,  \* node -> the node it replicates (a replica's)
    link,       \* node -> its link to primaryOf
    cached,     \* node -> it holds a cached primary to PSYNC with
    data,       \* node -> the writes its data set applied, in order
    off,        \* node -> primary_repl_offset
    replid,     \* node -> its replication ID
    replid2,    \* node -> its secondary ID (None: cleared)
    secondOff,  \* node -> second_replid_offset
    backlog,    \* node -> the first offset its backlog holds (0: no backlog)
    rdb,        \* node -> its RDB file [data, id, off] (id None: no replication info)
    snap,       \* node -> the dual-channel snapshot it receives [data, id, off]
    nextId,     \* the last replication ID made
    writes,     \* writes made
    crashes,
    switches

vars == <<role, primaryOf, link, cached, data, off, replid, replid2, secondOff,
          backlog, rdb, snap, nextId, writes, crashes, switches>>

NoFile == [data |-> <<>>, id |-> None, off |-> 0]
Min(a, b) == IF a < b THEN a ELSE b

\* How far n's data set runs ahead of its offset.
Gap(n) == Len(data[n]) - off[n]

\* The write at replication offset o of n's history (0 when n has none there).
EntryAt(n, o) == IF o + Gap(n) \in 1..Len(data[n]) THEN data[n][o + Gap(n)] ELSE 0

\* The replicas attached to p: streaming, or in a dual-channel sync with it.
Attached(p) == {x \in Nodes : role[x] = "replica" /\ primaryOf[x] = p /\ link[x] \in Linked}

\* The links and cached primaries after the replicas in S lose their primary:
\* a streaming replica caches it (replicationCachePrimary); a dual-channel sync
\* is aborted, and a replica that already loaded its snapshot keeps it.
DropLinks(S) == [x \in Nodes |-> IF x \in S THEN "down" ELSE link[x]]
CacheOnDrop(S) == [x \in Nodes |-> IF x \in S /\ link[x] = "up" THEN TRUE ELSE cached[x]]
\* A dual-channel snapshot is kept only while its sync runs.
ClearSnaps(S) == [x \in Nodes |-> IF x \in S THEN NoFile ELSE snap[x]]

\* primaryTryPartialResynchronization: can p continue a replica that asks for
\* ID id from offset o?
CanContinue(p, id, o) ==
    /\ \/ id = replid[p]
       \/ id # None /\ id = replid2[p] /\ o <= secondOff[p]
    /\ backlog[p] # 0
    /\ backlog[p] <= o
    /\ o <= off[p] + 1

\* A replica with a cached primary asks for its ID from the offset after its own.
CanPartial(r, p) == cached[r] /\ CanContinue(p, replid[r], off[r] + 1)

\* A replica ready to sync with its primary.
CanSync(r) ==
    /\ role[r] = "replica"
    /\ link[r] = "down"
    /\ role[primaryOf[r]] = "primary"

\* A full sync makes a primary without a backlog create one under a new ID
\* (syncCommand: changeReplicationId, clearReplicationId2, createReplicationBacklog).
NeedsId(p) == backlog[p] = 0
SyncId(p) == IF NeedsId(p) THEN nextId + 1 ELSE replid[p]

\* Which IDs the RDB file of n records (rdbPopulateSaveInfo).
HasInfo(n) ==
    \/ role[n] = "primary" /\ backlog[n] # 0
    \/ role[n] = "replica" /\ (link[n] = "up" \/ cached[n])

Init ==
    /\ role = [n \in Nodes |-> IF n = First THEN "primary" ELSE "replica"]
    /\ primaryOf = [n \in Nodes |-> First]
    /\ link = [n \in Nodes |-> IF n = First THEN "none" ELSE "down"]
    /\ cached = [n \in Nodes |-> FALSE]
    /\ data = [n \in Nodes |-> <<>>]
    /\ off = [n \in Nodes |-> 0]
    \* Each node boots with an ID of its own.
    /\ replid = CHOOSE f \in [Nodes -> 1..Cardinality(Nodes)] :
                    \A a, b \in Nodes : f[a] = f[b] => a = b
    /\ replid2 = [n \in Nodes |-> None]
    /\ secondOff = [n \in Nodes |-> 0]
    /\ backlog = [n \in Nodes |-> 0]
    /\ rdb = [n \in Nodes |-> NoFile]
    /\ snap = [n \in Nodes |-> NoFile]
    /\ nextId = Cardinality(Nodes)
    /\ writes = 0
    /\ crashes = 0
    /\ switches = 0

-----------------------------------------------------------------------------
(* Clients, the stream, and the backlog *)

\* A client writes to a primary; the offset moves only with a backlog.
Write(p) ==
    /\ role[p] = "primary"
    /\ writes < MaxWrites
    /\ data' = [data EXCEPT ![p] = Append(@, writes + 1)]
    /\ off' = [off EXCEPT ![p] = IF backlog[p] # 0 THEN @ + 1 ELSE @]
    /\ writes' = writes + 1
    /\ UNCHANGED <<role, primaryOf, link, cached, replid, replid2, secondOff, backlog,
                   rdb, snap, nextId, crashes, switches>>

\* A streaming replica applies its primary's next entry, by offset.
Stream(r) ==
    LET p == primaryOf[r] IN
    /\ link[r] = "up"
    /\ off[r] < off[p]
    /\ data' = [data EXCEPT ![r] = Append(@, EntryAt(p, off[r] + 1))]
    /\ off' = [off EXCEPT ![r] = @ + 1]
    /\ UNCHANGED <<role, primaryOf, link, cached, replid, replid2, secondOff, backlog,
                   rdb, snap, nextId, writes, crashes, switches>>

\* A primary's backlog drops its oldest entry (repl-backlog-size). A replica's
\* backlog trims too; the model leaves that out, as it only makes a promoted
\* replica refuse more partial resyncs.
Trim(n) ==
    /\ role[n] = "primary"
    /\ backlog[n] # 0
    /\ backlog[n] <= off[n]
    /\ backlog' = [backlog EXCEPT ![n] = @ + 1]
    /\ UNCHANGED <<role, primaryOf, link, cached, data, off, replid, replid2, secondOff,
                   rdb, snap, nextId, writes, crashes, switches>>

\* replicationCron: a primary with no replica for repl-backlog-ttl frees its
\* backlog and takes a new ID, since its offset stops counting its writes.
FreeBacklog(p) ==
    /\ role[p] = "primary"
    /\ backlog[p] # 0
    /\ Attached(p) = {}
    /\ nextId < MaxIds
    /\ replid' = [replid EXCEPT ![p] = nextId + 1]
    /\ replid2' = [replid2 EXCEPT ![p] = None]
    /\ secondOff' = [secondOff EXCEPT ![p] = 0]
    /\ backlog' = [backlog EXCEPT ![p] = 0]
    /\ nextId' = nextId + 1
    /\ UNCHANGED <<role, primaryOf, link, cached, data, off, rdb, snap, writes, crashes, switches>>

-----------------------------------------------------------------------------
(* Synchronization *)

\* PSYNC accepted (+CONTINUE). A new primary ID moves the replica's ID to its
\* secondary ID (replicaProcessPsyncReply); a replica restarted from its RDB
\* file creates its backlog now.
PartialSync(r) ==
    LET p == primaryOf[r] IN
    /\ CanSync(r)
    /\ CanPartial(r, p)
    /\ link' = [link EXCEPT ![r] = "up"]
    /\ cached' = [cached EXCEPT ![r] = FALSE]
    /\ IF replid[p] # replid[r]
         THEN /\ replid2' = [replid2 EXCEPT ![r] = replid[r]]
              /\ secondOff' = [secondOff EXCEPT ![r] = off[r] + 1]
              /\ replid' = [replid EXCEPT ![r] = replid[p]]
         ELSE UNCHANGED <<replid, replid2, secondOff>>
    /\ backlog' = [backlog EXCEPT ![r] = IF @ = 0 THEN off[r] + 1 ELSE @]
    /\ UNCHANGED <<role, primaryOf, data, off, rdb, snap, nextId, writes, crashes, switches>>

\* Single-channel full sync (+FULLRESYNC), from the fork to the end of the
\* load, in one step: the replica takes the snapshot with the primary's ID and
\* offset, drops its cached primary and backlog (replicationAttachToNewPrimary),
\* and creates a backlog after the offset (replicaAfterLoadPrimaryRDB).
FullSync(r) ==
    LET p == primaryOf[r]
        id == SyncId(p)
    IN
    /\ ~DualChannel
    /\ CanSync(r)
    /\ ~CanPartial(r, p)
    /\ NeedsId(p) => nextId < MaxIds
    /\ data' = [data EXCEPT ![r] = data[p]]
    /\ off' = [off EXCEPT ![r] = off[p]]
    /\ replid' = [replid EXCEPT ![p] = id, ![r] = id]
    /\ replid2' = [replid2 EXCEPT ![p] = IF NeedsId(p) THEN None ELSE @, ![r] = None]
    /\ secondOff' = [secondOff EXCEPT ![p] = IF NeedsId(p) THEN 0 ELSE @, ![r] = 0]
    /\ backlog' = [backlog EXCEPT ![p] = IF NeedsId(p) THEN off[p] + 1 ELSE @, ![r] = off[p] + 1]
    /\ nextId' = IF NeedsId(p) THEN nextId + 1 ELSE nextId
    /\ link' = [link EXCEPT ![r] = "up"]
    /\ cached' = [cached EXCEPT ![r] = FALSE]
    /\ UNCHANGED <<role, primaryOf, rdb, snap, writes, crashes, switches>>

\* Dual-channel: PSYNC was refused (+DUALCHANNELSYNC); the RDB channel's sync
\* forks the snapshot and sends its offset and ID ($ENDOFF).
DualFork(r) ==
    LET p == primaryOf[r]
        id == SyncId(p)
    IN
    /\ DualChannel
    /\ CanSync(r)
    /\ ~CanPartial(r, p)
    /\ NeedsId(p) => nextId < MaxIds
    /\ snap' = [snap EXCEPT ![r] = [data |-> data[p], id |-> id, off |-> off[p]]]
    /\ replid' = [replid EXCEPT ![p] = id]
    /\ replid2' = [replid2 EXCEPT ![p] = IF NeedsId(p) THEN None ELSE @]
    /\ secondOff' = [secondOff EXCEPT ![p] = IF NeedsId(p) THEN 0 ELSE @]
    /\ backlog' = [backlog EXCEPT ![p] = IF NeedsId(p) THEN off[p] + 1 ELSE @]
    /\ nextId' = IF NeedsId(p) THEN nextId + 1 ELSE nextId
    /\ link' = [link EXCEPT ![r] = "rdb"]
    /\ UNCHANGED <<role, primaryOf, cached, data, off, rdb, writes, crashes, switches>>

\* The main channel asks to continue from the snapshot's offset, before the
\* load ends. Refused, the sync is aborted and the replica keeps its data.
DualContinueEarly(r) ==
    LET p == primaryOf[r] IN
    /\ link[r] = "rdb"
    /\ IF CanContinue(p, snap[r].id, snap[r].off + 1)
         THEN /\ link' = [link EXCEPT ![r] = "rdbContinued"]
              /\ UNCHANGED snap
         ELSE /\ link' = [link EXCEPT ![r] = "down"]
              /\ snap' = ClearSnaps({r})
    /\ UNCHANGED <<role, primaryOf, cached, data, off, replid, replid2, secondOff, backlog,
                   rdb, nextId, writes, crashes, switches>>

\* The load ends after +CONTINUE (dualChannelSyncSuccess): the replica takes
\* the snapshot, its ID and offset, and streams the buffered entries.
DualLoadContinued(r) ==
    /\ link[r] = "rdbContinued"
    /\ data' = [data EXCEPT ![r] = snap[r].data]
    /\ off' = [off EXCEPT ![r] = snap[r].off]
    /\ replid' = [replid EXCEPT ![r] = snap[r].id]
    /\ replid2' = [replid2 EXCEPT ![r] = None]
    /\ secondOff' = [secondOff EXCEPT ![r] = 0]
    /\ backlog' = [backlog EXCEPT ![r] = snap[r].off + 1]
    /\ cached' = [cached EXCEPT ![r] = FALSE]
    /\ link' = [link EXCEPT ![r] = "up"]
    /\ snap' = ClearSnaps({r})
    /\ UNCHANGED <<role, primaryOf, rdb, nextId, writes, crashes, switches>>

\* The load ends before +CONTINUE (replicaAfterLoadPrimaryRDB with the RDB
\* channel's state at RDB_LOADED): the cached primary and the old backlog are
\* gone, replid2 is cleared, and a backlog is created. Valkey keeps the old ID
\* and offset until +CONTINUE, so that backlog starts after the old offset.
DualLoadFirst(r) ==
    /\ link[r] = "rdb"
    /\ data' = [data EXCEPT ![r] = snap[r].data]
    /\ replid2' = [replid2 EXCEPT ![r] = None]
    /\ secondOff' = [secondOff EXCEPT ![r] = 0]
    /\ cached' = [cached EXCEPT ![r] = FALSE]
    /\ IF AdoptOnLoad
         THEN /\ off' = [off EXCEPT ![r] = snap[r].off]
              /\ replid' = [replid EXCEPT ![r] = snap[r].id]
              /\ backlog' = [backlog EXCEPT ![r] = snap[r].off + 1]
         ELSE /\ backlog' = [backlog EXCEPT ![r] = off[r] + 1]
              /\ UNCHANGED <<off, replid>>
    /\ link' = [link EXCEPT ![r] = "loaded"]
    /\ UNCHANGED <<role, primaryOf, rdb, snap, nextId, writes, crashes, switches>>

\* +CONTINUE after the load (dualChannelSyncHandlePsync): the replica takes the
\* snapshot's ID and offset. Refused, the sync is aborted with the snapshot loaded.
DualContinueLate(r) ==
    LET p == primaryOf[r] IN
    /\ link[r] = "loaded"
    /\ IF CanContinue(p, snap[r].id, snap[r].off + 1)
         THEN /\ off' = [off EXCEPT ![r] = snap[r].off]
              /\ replid' = [replid EXCEPT ![r] = snap[r].id]
              /\ backlog' = [backlog EXCEPT ![r] = snap[r].off + 1]
              /\ link' = [link EXCEPT ![r] = "up"]
         ELSE /\ link' = [link EXCEPT ![r] = "down"]
              /\ UNCHANGED <<off, replid, backlog>>
    /\ snap' = ClearSnaps({r})
    /\ UNCHANGED <<role, primaryOf, cached, data, replid2, secondOff, rdb,
                   nextId, writes, crashes, switches>>

\* A full sync's load fails after the old data was flushed (repl-diskless-load
\* disabled or on-empty-db): the half-loaded data is discarded, with the cached
\* primary and the backlog. (On a disk load Valkey exits instead: fuzzer issue #12.)
LoadFails(r) ==
    /\ \/ link[r] \in {"rdb", "rdbContinued"}
       \/ ~DualChannel /\ CanSync(r) /\ ~CanPartial(r, primaryOf[r])
    /\ data' = [data EXCEPT ![r] = <<>>]
    /\ cached' = [cached EXCEPT ![r] = FALSE]
    /\ backlog' = [backlog EXCEPT ![r] = 0]
    /\ link' = [link EXCEPT ![r] = "down"]
    /\ snap' = ClearSnaps({r})
    /\ UNCHANGED <<role, primaryOf, off, replid, replid2, secondOff, rdb,
                   nextId, writes, crashes, switches>>

\* The link drops (a timeout, CLIENT KILL, an output buffer limit).
Disconnect(r) ==
    /\ role[r] = "replica"
    /\ link[r] \in Linked
    /\ link' = DropLinks({r})
    /\ cached' = CacheOnDrop({r})
    /\ snap' = ClearSnaps({r})
    /\ UNCHANGED <<role, primaryOf, data, off, replid, replid2, secondOff, backlog,
                   rdb, nextId, writes, crashes, switches>>

-----------------------------------------------------------------------------
(* Operators *)

\* REPLICAOF NO ONE (replicationUnsetPrimary): the ID becomes the secondary ID,
\* valid up to the offset after this one, under a new ID (shiftReplicationId).
\* A sync in progress is aborted.
Promote(r) ==
    /\ role[r] = "replica"
    /\ switches < MaxSwitches
    /\ nextId < MaxIds
    /\ role' = [role EXCEPT ![r] = "primary"]
    /\ link' = [link EXCEPT ![r] = "none"]
    /\ cached' = [cached EXCEPT ![r] = FALSE]
    /\ replid2' = [replid2 EXCEPT ![r] = replid[r]]
    /\ secondOff' = [secondOff EXCEPT ![r] = off[r] + 1]
    /\ replid' = [replid EXCEPT ![r] = nextId + 1]
    /\ nextId' = nextId + 1
    /\ snap' = ClearSnaps({r})
    /\ switches' = switches + 1
    /\ UNCHANGED <<primaryOf, data, off, backlog, rdb, writes, crashes>>

\* REPLICAOF q (replicationSetPrimary). A primary turned replica caches itself
\* as its primary (replicationCachePrimaryUsingMyself) and its replicas lose
\* their link; a replica caches its current primary.
Follow(n, q) ==
    /\ n # q
    /\ role[q] = "primary"
    /\ role[n] = "replica" => primaryOf[n] # q
    /\ switches < MaxSwitches
    /\ LET dropped == IF role[n] = "primary" THEN Attached(n) ELSE {n} IN
       /\ link' = [DropLinks(dropped) EXCEPT ![n] = "down"]
       /\ cached' = [CacheOnDrop(dropped) EXCEPT ![n] = IF role[n] = "primary" THEN TRUE ELSE @]
       /\ snap' = ClearSnaps(dropped \cup {n})
    /\ role' = [role EXCEPT ![n] = "replica"]
    /\ primaryOf' = [primaryOf EXCEPT ![n] = q]
    /\ switches' = switches + 1
    /\ UNCHANGED <<data, off, replid, replid2, secondOff, backlog, rdb, nextId, writes, crashes>>

-----------------------------------------------------------------------------
(* Persistence *)

\* SAVE or BGSAVE: the data set, with the ID and offset when rdbPopulateSaveInfo
\* gives them; only while a crash can still load the file.
Save(n) ==
    /\ crashes < MaxCrashes
    /\ rdb' = [rdb EXCEPT ![n] = [data |-> data[n], id |-> IF HasInfo(n) THEN replid[n] ELSE None,
                                  off |-> off[n]]]
    /\ rdb' # rdb
    /\ UNCHANGED <<role, primaryOf, link, cached, data, off, replid, replid2, secondOff, backlog,
                   snap, nextId, writes, crashes, switches>>

\* The process crashes and restarts in its role, from its RDB file
\* (loadDataFromDisk), under a new boot ID. A primary keeps the file's ID as its
\* secondary ID, with a backlog after the file's offset; a replica takes the
\* file's ID and offset and caches itself as its primary. A file without them
\* restarts the offset at 0. Its replicas lose their link.
Crash(n) ==
    LET file == rdb[n]
        info == file.id # None
        isPrimary == role[n] = "primary"
        lost == IF isPrimary THEN Attached(n) ELSE {}
    IN
    /\ crashes < MaxCrashes
    /\ nextId < MaxIds
    /\ data' = [data EXCEPT ![n] = file.data]
    /\ off' = [off EXCEPT ![n] = IF info THEN file.off ELSE 0]
    /\ replid' = [replid EXCEPT ![n] = IF info /\ ~isPrimary THEN file.id ELSE nextId + 1]
    /\ replid2' = [replid2 EXCEPT ![n] = IF info /\ isPrimary THEN file.id ELSE None]
    /\ secondOff' = [secondOff EXCEPT ![n] = IF info /\ isPrimary THEN file.off + 1 ELSE 0]
    /\ backlog' = [backlog EXCEPT ![n] = IF info /\ isPrimary THEN file.off + 1 ELSE 0]
    /\ link' = [DropLinks(lost) EXCEPT ![n] = IF isPrimary THEN "none" ELSE "down"]
    /\ cached' = [CacheOnDrop(lost) EXCEPT ![n] = info /\ ~isPrimary]
    /\ snap' = ClearSnaps(lost \cup {n})
    /\ nextId' = nextId + 1
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<role, primaryOf, rdb, writes, switches>>

\* Nothing happens. Once the bounds are used up, some states (every node a
\* primary, say) enable no other action; this keeps TLC from reporting them as
\* deadlocks.
Idle == UNCHANGED vars

-----------------------------------------------------------------------------
Next ==
    \/ \E n \in Nodes :
          \/ Write(n) \/ Stream(n) \/ Trim(n) \/ FreeBacklog(n)
          \/ PartialSync(n) \/ FullSync(n) \/ DualFork(n) \/ DualContinueEarly(n)
          \/ DualLoadContinued(n) \/ DualLoadFirst(n) \/ DualContinueLate(n)
          \/ LoadFails(n) \/ Disconnect(n) \/ Promote(n) \/ Save(n) \/ Crash(n)
    \/ \E n, q \in Nodes : Follow(n, q)
    \/ Idle

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety properties *)

\* What a node or an RDB file would act on: a primary's ID with a backlog, a
\* replica's ID while it streams or holds a cached primary, a secondary ID with
\* a backlog to serve it, and an RDB file's ID. Each names a data set up to an
\* offset.
Claims ==
    {[id |-> replid[n], upto |-> off[n], data |-> data[n], gap |-> Gap(n)] :
        n \in {m \in Nodes : \/ role[m] = "primary" /\ backlog[m] # 0
                             \/ role[m] = "replica" /\ (link[m] = "up" \/ cached[m])}}
    \cup {[id |-> replid2[n], upto |-> secondOff[n] - 1, data |-> data[n], gap |-> Gap(n)] :
            n \in {m \in Nodes : replid2[m] # None /\ backlog[m] # 0}}
    \cup {[id |-> rdb[n].id, upto |-> rdb[n].off, data |-> rdb[n].data,
           gap |-> Len(rdb[n].data) - rdb[n].off] :
            n \in {m \in Nodes : rdb[m].id # None}}

\* The data set a claim names at offset k.
Named(c, k) == SubSeq(c.data, 1, k + c.gap)

\* What every partial resync relies on: a replication ID names one history.
\* Two claims on one ID agree on the data set at every offset both reach.
OneHistoryPerId ==
    \A c1, c2 \in Claims :
        c1.id = c2.id =>
            /\ c1.gap = c2.gap
            /\ Named(c1, Min(c1.upto, c2.upto)) = Named(c2, Min(c1.upto, c2.upto))

\* What an operator sees: a streaming replica holds a prefix of its primary's
\* history, at the same offsets.
ReplicaIsPrefix ==
    \A r \in Nodes :
        (role[r] = "replica" /\ link[r] = "up") =>
            LET p == primaryOf[r] IN
            /\ Gap(r) = Gap(p)
            /\ off[r] <= off[p]
            /\ data[r] = SubSeq(data[p], 1, Len(data[r]))

TypeOK ==
    /\ role \in [Nodes -> {"primary", "replica"}]
    /\ primaryOf \in [Nodes -> Nodes]
    /\ link \in [Nodes -> Linked \cup {"down", "none"}]
    /\ cached \in [Nodes -> BOOLEAN]
    /\ replid \in [Nodes -> 1..MaxIds]
    /\ replid2 \in [Nodes -> 0..MaxIds]
    /\ writes \in 0..MaxWrites
    /\ crashes \in 0..MaxCrashes
    /\ switches \in 0..MaxSwitches
=============================================================================
