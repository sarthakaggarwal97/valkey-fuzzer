------------------------- MODULE no_shift_on_promotion -------------------------
(* Mutation of model.tla: a promoted replica keeps its ID instead of moving *)
(* it to its secondary ID under a new one, so two primaries write different *)
(* histories under one ID.                                                  *)
EXTENDS model

MutantPromote(r) ==
    /\ role[r] = "replica"
    /\ switches < MaxSwitches
    /\ role' = [role EXCEPT ![r] = "primary"]
    /\ link' = [link EXCEPT ![r] = "none"]
    /\ cached' = [cached EXCEPT ![r] = FALSE]
    /\ switches' = switches + 1
    /\ UNCHANGED <<primaryOf, data, off, replid, replid2, secondOff, backlog, rdb, snap,
                   nextId, writes, crashes>>

MutantNext ==
    \/ \E n \in Nodes :
          \/ Write(n) \/ Stream(n) \/ Trim(n) \/ FreeBacklog(n)
          \/ PartialSync(n) \/ FullSync(n) \/ DualFork(n) \/ DualContinueEarly(n)
          \/ DualLoadContinued(n) \/ DualLoadFirst(n) \/ DualContinueLate(n)
          \/ LoadFails(n) \/ Disconnect(n) \/ MutantPromote(n) \/ Save(n) \/ Crash(n)
    \/ \E n, q \in Nodes : Follow(n, q)
    \/ Idle

MutantSpec == Init /\ [][MutantNext]_vars
=============================================================================
