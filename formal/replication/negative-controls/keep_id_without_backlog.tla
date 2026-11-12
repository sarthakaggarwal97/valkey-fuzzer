------------------------ MODULE keep_id_without_backlog ------------------------
(* Mutation of model.tla: a primary frees its backlog and keeps its ID. Its *)
(* offset then stops counting its writes, and once it follows a replica     *)
(* promoted from that ID it continues as if it had none of them.            *)
EXTENDS model

MutantFreeBacklog(p) ==
    /\ role[p] = "primary"
    /\ backlog[p] # 0
    /\ Attached(p) = {}
    /\ backlog' = [backlog EXCEPT ![p] = 0]
    /\ UNCHANGED <<role, primaryOf, link, cached, data, off, replid, replid2, secondOff,
                   rdb, snap, nextId, writes, crashes, switches>>

MutantNext ==
    \/ \E n \in Nodes :
          \/ Write(n) \/ Stream(n) \/ Trim(n) \/ MutantFreeBacklog(n)
          \/ PartialSync(n) \/ FullSync(n) \/ DualFork(n) \/ DualContinueEarly(n)
          \/ DualLoadContinued(n) \/ DualLoadFirst(n) \/ DualContinueLate(n)
          \/ LoadFails(n) \/ Disconnect(n) \/ Promote(n) \/ Save(n) \/ Crash(n)
    \/ \E n, q \in Nodes : Follow(n, q)
    \/ Idle

MutantSpec == Init /\ [][MutantNext]_vars
=============================================================================
