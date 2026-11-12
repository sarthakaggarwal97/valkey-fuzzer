----------------------- MODULE save_info_without_primary -----------------------
(* Mutation of model.tla: an RDB file always records the ID and offset,   *)
(* also on a node whose offset no longer names its data set: a primary    *)
(* without a backlog (its writes do not move the offset), or a replica    *)
(* with no primary, live or cached (a failed load discarded its data).    *)
EXTENDS model

MutantSave(n) ==
    /\ crashes < MaxCrashes
    /\ rdb' = [rdb EXCEPT ![n] = [data |-> data[n], id |-> replid[n], off |-> off[n]]]
    /\ rdb' # rdb
    /\ UNCHANGED <<role, primaryOf, link, cached, data, off, replid, replid2, secondOff, backlog,
                   snap, nextId, writes, crashes, switches>>

MutantNext ==
    \/ \E n \in Nodes :
          \/ Write(n) \/ Stream(n) \/ Trim(n) \/ FreeBacklog(n)
          \/ PartialSync(n) \/ FullSync(n) \/ DualFork(n) \/ DualContinueEarly(n)
          \/ DualLoadContinued(n) \/ DualLoadFirst(n) \/ DualContinueLate(n)
          \/ LoadFails(n) \/ Disconnect(n) \/ Promote(n) \/ MutantSave(n) \/ Crash(n)
    \/ \E n, q \in Nodes : Follow(n, q)
    \/ Idle

MutantSpec == Init /\ [][MutantNext]_vars
=============================================================================
