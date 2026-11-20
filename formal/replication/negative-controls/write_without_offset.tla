------------------------- MODULE write_without_offset -------------------------
(* Mutation of model.tla: a write does not advance the primary's offset. The  *)
(* primary and a replica streaming from it then stand at one offset of one ID *)
(* with different data sets, which is what a sample of both would show.       *)
EXTENDS model

MutantWrite(p) ==
    /\ role[p] = "primary"
    /\ writes < MaxWrites
    /\ data' = [data EXCEPT ![p] = Append(@, writes + 1)]
    /\ writes' = writes + 1
    /\ UNCHANGED <<role, primaryOf, link, cached, off, replid, replid2, secondOff, backlog,
                   rdb, snap, nextId, crashes, switches>>

MutantNext ==
    \/ \E n \in Nodes :
          \/ MutantWrite(n) \/ Stream(n) \/ Trim(n) \/ FreeBacklog(n)
          \/ PartialSync(n) \/ FullSync(n) \/ DualFork(n) \/ DualContinueEarly(n)
          \/ DualLoadContinued(n) \/ DualLoadFirst(n) \/ DualContinueLate(n)
          \/ LoadFails(n) \/ Disconnect(n) \/ Promote(n) \/ Save(n) \/ Crash(n)
    \/ \E n, q \in Nodes : Follow(n, q)
    \/ Idle

MutantSpec == Init /\ [][MutantNext]_vars
=============================================================================
