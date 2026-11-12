------------------------ MODULE no_second_offset_check ------------------------
(* Mutation of model.tla: PSYNC on the primary's secondary ID ignores        *)
(* second_replid_offset, so a replica that ran ahead of a promoted replica   *)
(* on the old primary's history continues on the new primary's.            *)
EXTENDS model

AnyOffsetContinue(p, id, o) ==
    /\ \/ id = replid[p]
       \/ id # None /\ id = replid2[p]
    /\ backlog[p] # 0
    /\ backlog[p] <= o
    /\ o <= off[p] + 1

MutantPartialSync(r) ==
    LET p == primaryOf[r] IN
    /\ CanSync(r)
    /\ cached[r] /\ AnyOffsetContinue(p, replid[r], off[r] + 1)
    /\ link' = [link EXCEPT ![r] = "up"]
    /\ cached' = [cached EXCEPT ![r] = FALSE]
    /\ IF replid[p] # replid[r]
         THEN /\ replid2' = [replid2 EXCEPT ![r] = replid[r]]
              /\ secondOff' = [secondOff EXCEPT ![r] = off[r] + 1]
              /\ replid' = [replid EXCEPT ![r] = replid[p]]
         ELSE UNCHANGED <<replid, replid2, secondOff>>
    /\ backlog' = [backlog EXCEPT ![r] = IF @ = 0 THEN off[r] + 1 ELSE @]
    /\ UNCHANGED <<role, primaryOf, data, off, rdb, snap, nextId, writes, crashes, switches>>

MutantNext ==
    \/ \E n \in Nodes :
          \/ Write(n) \/ Stream(n) \/ Trim(n) \/ FreeBacklog(n)
          \/ MutantPartialSync(n) \/ FullSync(n) \/ DualFork(n) \/ DualContinueEarly(n)
          \/ DualLoadContinued(n) \/ DualLoadFirst(n) \/ DualContinueLate(n)
          \/ LoadFails(n) \/ Disconnect(n) \/ Promote(n) \/ Save(n) \/ Crash(n)
    \/ \E n, q \in Nodes : Follow(n, q)
    \/ Idle

MutantSpec == Init /\ [][MutantNext]_vars
=============================================================================
