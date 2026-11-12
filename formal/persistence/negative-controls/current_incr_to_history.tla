-------------------- MODULE current_incr_to_history --------------------
(* Mutation of model.tla: when the rewrite is done, the incremental file    *)
(* the server appends to becomes history too (markRewrittenIncrAofAsHistory *)
(* keeps it), so the manifest names only the base and the writes after the *)
(* fork are not loaded.                                                    *)
EXTENDS model

MutantPersistManifest ==
    /\ done = "renamed"
    /\ man' = [base |-> newBase, incrs |-> <<>>]
    /\ state' = "on"
    /\ done' = "persisted"
    /\ UNCHANGED <<data, buf, present, content, cur, child, snap, newBase, rdb, next, ackedOn,
                   writes, crashes, switches, rewrites>>

MutantNext ==
    \/ Write \/ Flush
    \/ StartRewrite \/ RenameBase \/ MutantPersistManifest \/ DeleteHistory \/ RewriteFails
    \/ StopAof \/ StartAof \/ StartSave \/ SaveDone \/ Crash
    \/ Idle

MutantSpec == Init /\ [][MutantNext]_vars
=============================================================================
