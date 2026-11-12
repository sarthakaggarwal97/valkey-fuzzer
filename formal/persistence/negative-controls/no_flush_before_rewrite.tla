-------------------- MODULE no_flush_before_rewrite --------------------
(* Mutation of model.tla: a rewrite forks without first writing aof_buf    *)
(* out, so the writes in it reach both the base's snapshot and the new     *)
(* incremental file.                                                       *)
EXTENDS model

MutantStartRewrite ==
    /\ state # "off"
    /\ child = "none"
    /\ rewrites < MaxRewrites
    /\ next < MaxFiles
    /\ present' = present \cup {next + 1}
    /\ man' = IF state = "on" THEN [man EXCEPT !.incrs = Append(@, next + 1)] ELSE man
    /\ cur' = next + 1
    /\ next' = next + 1
    /\ child' = "aof"
    /\ snap' = data
    /\ rewrites' = rewrites + 1
    /\ UNCHANGED <<data, buf, state, content, done, newBase, rdb, ackedOn, writes, crashes, switches>>

MutantNext ==
    \/ Write \/ Flush
    \/ MutantStartRewrite \/ RenameBase \/ PersistManifest \/ DeleteHistory \/ RewriteFails
    \/ StopAof \/ StartAof \/ StartSave \/ SaveDone \/ Crash
    \/ Idle

MutantSpec == Init /\ [][MutantNext]_vars
=============================================================================
