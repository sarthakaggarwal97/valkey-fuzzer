------------------------- MODULE grant_after_pause_ends -------------------------
(* Mutation of model.tla: the source grants the failover after its pause    *)
(* ended (clusterCommandSyncSlotsRequestFailover checks it and denies).     *)
EXTENDS model

\* The pause ends, but the export stays where it was.
MutantPauseExpires ==
    /\ paused
    /\ ExpiryAfterClaim \/ ~TMayClaim
    /\ faults < MaxFaults
    /\ paused' = FALSE
    /\ faults' = faults + 1
    /\ UNCHANGED <<role, view, epoch, data, hidden, exp, imp, track, toT, toS, repl, bus, acked, writes>>

MutantNext ==
    \/ \E n \in Nodes : Write(n)
    \/ Start \/ TEstablish \/ SFork \/ TApply \/ SPause \/ TPaused \/ SGrant \/ TClaim
    \/ TClose \/ TCleanup \/ RApply
    \/ \E n \in Nodes : Deliver(n) \/ LoseBus(n)
    \/ DropLink \/ Cancel \/ MutantPauseExpires \/ CrashT \/ Promote
    \/ Idle

MutantSpec == Init /\ [][MutantNext]_vars
=============================================================================
