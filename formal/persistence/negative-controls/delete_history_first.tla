-------------------- MODULE delete_history_first --------------------
(* Mutation of model.tla: the files the new manifest will not name are     *)
(* deleted before it is written, so a kill between them leaves the old     *)
(* manifest naming deleted files.                                          *)
EXTENDS model

MutantDeleteFirst ==
    /\ done = "renamed"
    /\ present' = present \cap {newBase, cur}
    /\ done' = "deleted"
    /\ UNCHANGED <<data, buf, state, content, man, cur, child, snap, newBase, rdb, next, ackedOn,
                   writes, crashes, switches, rewrites>>

MutantPersistManifest ==
    /\ done = "deleted"
    /\ man' = [base |-> newBase, incrs |-> <<cur>>]
    /\ state' = "on"
    /\ done' = "none"
    /\ child' = "none"
    /\ newBase' = 0
    /\ snap' = <<>>
    /\ UNCHANGED <<data, buf, present, content, cur, rdb, next, ackedOn, writes, crashes,
                   switches, rewrites>>

MutantNext ==
    \/ Write \/ Flush
    \/ StartRewrite \/ RenameBase \/ MutantDeleteFirst \/ MutantPersistManifest \/ RewriteFails
    \/ StopAof \/ StartAof \/ StartSave \/ SaveDone \/ Crash
    \/ Idle

MutantSpec == Init /\ [][MutantNext]_vars
=============================================================================
