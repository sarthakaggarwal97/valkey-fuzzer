-------------------------- MODULE grant_without_pause --------------------------
(* Mutation of model.tla: the source sends PAUSED without pausing client     *)
(* writes, so it still takes writes when the target claims the slot.        *)
EXTENDS model

MutantSPause ==
    /\ toS # <<>> /\ Head(toS) = "reqPause"
    /\ exp = "stream"
    /\ toS' = Tail(toS)
    /\ exp' = "paused"
    /\ toT' = Append(toT, [k |-> "paused"])
    /\ UNCHANGED <<role, view, epoch, data, hidden, imp, track, repl, bus, paused, acked, writes, faults>>

MutantNext ==
    \/ \E n \in Nodes : Write(n)
    \/ Start \/ TEstablish \/ SFork \/ TApply \/ MutantSPause \/ TPaused \/ SGrant \/ TClaim
    \/ TClose \/ TCleanup \/ RApply
    \/ \E n \in Nodes : Deliver(n) \/ LoseBus(n)
    \/ DropLink \/ Cancel \/ PauseExpires \/ CrashT \/ Promote
    \/ Idle

MutantSpec == Init /\ [][MutantNext]_vars
=============================================================================
