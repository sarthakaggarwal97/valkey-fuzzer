---------------------------- MODULE feed_after_fork ----------------------------
(* Mutation of model.tla: the source feeds the slot's writes to the target  *)
(* only once it pauses, so a write made after the fork, before the pause,   *)
(* reaches neither the snapshot nor the stream.                             *)
EXTENDS model

MutantWrite(n) ==
    LET w == writes + 1 IN
    /\ Accepts(n)
    /\ writes < MaxWrites
    /\ data' = [data EXCEPT ![n] = Append(@, w)]
    /\ acked' = acked \cup {w}
    /\ writes' = w
    /\ toT' = IF n = S /\ exp \in {"paused", "granted"} THEN Append(toT, [k |-> "write", w |-> w]) ELSE toT
    /\ repl' = IF n = T THEN ToReplica(<<[k |-> "write", w |-> w]>>) ELSE repl
    /\ UNCHANGED <<role, view, epoch, hidden, exp, imp, track, toS, bus, paused, faults>>

MutantNext ==
    \/ \E n \in Nodes : MutantWrite(n)
    \/ Start \/ TEstablish \/ SFork \/ TApply \/ SPause \/ TPaused \/ SGrant \/ TClaim
    \/ TClose \/ TCleanup \/ RApply
    \/ \E n \in Nodes : Deliver(n) \/ LoseBus(n)
    \/ DropLink \/ Cancel \/ PauseExpires \/ CrashT \/ Promote
    \/ Idle

MutantSpec == Init /\ [][MutantNext]_vars
=============================================================================
