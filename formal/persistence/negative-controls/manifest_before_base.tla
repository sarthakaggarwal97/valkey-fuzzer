-------------------- MODULE manifest_before_base --------------------
(* Mutation of model.tla: the manifest naming the new base is written      *)
(* before the base is renamed into place, so a kill between them leaves a  *)
(* manifest that names a missing file.                                     *)
EXTENDS model

MutantPersistManifest ==
    /\ child = "aof" /\ done = "none"
    /\ next < MaxFiles
    /\ man' = [base |-> next + 1, incrs |-> <<cur>>]
    /\ newBase' = next + 1
    /\ next' = next + 1
    /\ state' = "on"
    /\ done' = "persisted"
    /\ UNCHANGED <<data, buf, present, content, cur, child, snap, rdb, ackedOn, writes, crashes,
                   switches, rewrites>>

\* The rename, after the manifest.
MutantRenameBase ==
    /\ done = "persisted" /\ newBase \notin present
    /\ present' = present \cup {newBase}
    /\ content' = [content EXCEPT ![newBase] = snap]
    /\ UNCHANGED <<data, buf, state, man, cur, child, snap, done, newBase, rdb, next, ackedOn,
                   writes, crashes, switches, rewrites>>

MutantDeleteHistory == newBase \in present /\ DeleteHistory

MutantNext ==
    \/ Write \/ Flush
    \/ StartRewrite \/ MutantRenameBase \/ MutantPersistManifest \/ MutantDeleteHistory \/ RewriteFails
    \/ StopAof \/ StartAof \/ StartSave \/ SaveDone \/ Crash
    \/ Idle

MutantSpec == Init /\ [][MutantNext]_vars
=============================================================================
