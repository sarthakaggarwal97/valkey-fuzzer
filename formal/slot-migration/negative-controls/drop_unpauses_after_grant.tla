----------------------- MODULE drop_unpauses_after_grant -----------------------
(* Mutation of model.tla: when the connection drops after the source        *)
(* granted the failover, the source fails the export and unpauses, though   *)
(* the target may have claimed the slot (clusterHandleSlotMigrationClientClose *)
(* keeps it paused).                                                        *)
EXTENDS model

MutantDropLink ==
    LET fails == exp \in {"establish", "stream", "paused", "granted"} IN
    /\ Exporting \/ Importing
    /\ faults < MaxFaults
    /\ toT' = <<>>
    /\ toS' = <<>>
    /\ exp' = IF fails THEN "failed" ELSE exp
    /\ paused' = IF fails THEN FALSE ELSE paused
    /\ imp' = IF Importing THEN "cleanup" ELSE imp
    /\ faults' = faults + 1
    /\ UNCHANGED <<role, view, epoch, data, hidden, track, repl, bus, acked, writes>>

MutantNext ==
    \/ \E n \in Nodes : Write(n)
    \/ Start \/ TEstablish \/ SFork \/ TApply \/ SPause \/ TPaused \/ SGrant \/ TClaim
    \/ TClose \/ TCleanup \/ RApply
    \/ \E n \in Nodes : Deliver(n) \/ LoseBus(n)
    \/ MutantDropLink \/ Cancel \/ PauseExpires \/ CrashT \/ Promote
    \/ Idle

MutantSpec == Init /\ [][MutantNext]_vars
=============================================================================
