-------------------- MODULE ack_before_flush --------------------
(* Mutation of model.tla: a write is replied to before aof_buf is written   *)
(* to the file, so a kill in between loses a write the client saw.         *)
EXTENDS model

MutantWrite ==
    /\ done = "none"
    /\ writes < MaxWrites
    /\ data' = Append(data, writes + 1)
    /\ buf' = IF Feeding THEN Append(buf, writes + 1) ELSE buf
    /\ ackedOn' = IF state = "on" THEN ackedOn \cup {writes + 1} ELSE ackedOn
    /\ writes' = writes + 1
    /\ UNCHANGED <<state, present, content, man, cur, child, snap, done, newBase, rdb, next,
                   crashes, switches, rewrites>>

MutantNext ==
    \/ MutantWrite \/ Flush
    \/ StartRewrite \/ RenameBase \/ PersistManifest \/ DeleteHistory \/ RewriteFails
    \/ StopAof \/ StartAof \/ StartSave \/ SaveDone \/ Crash
    \/ Idle

MutantSpec == Init /\ [][MutantNext]_vars
=============================================================================
