-------------------- MODULE feed_while_waiting --------------------
(* Mutation of model.tla: while appendonly yes waits for a rewrite that has *)
(* not started, writes are still queued in aof_buf, and reach both the     *)
(* rewrite's snapshot and its temporary incremental file.                  *)
EXTENDS model

MutantWrite ==
    /\ done = "none"
    /\ writes < MaxWrites
    /\ data' = Append(data, writes + 1)
    /\ buf' = IF state # "off" THEN Append(buf, writes + 1) ELSE buf
    /\ writes' = writes + 1
    /\ UNCHANGED <<state, present, content, man, cur, child, snap, done, newBase, rdb, next,
                   ackedOn, crashes, switches, rewrites>>

\* With no file open, the queued writes wait in aof_buf.
MutantStartRewrite ==
    /\ state # "off"
    /\ child = "none"
    /\ rewrites < MaxRewrites
    /\ next < MaxFiles
    /\ IF cur # 0 THEN Flushed ELSE UNCHANGED <<content, ackedOn, buf>>
    /\ present' = present \cup {next + 1}
    /\ man' = IF state = "on" THEN [man EXCEPT !.incrs = Append(@, next + 1)] ELSE man
    /\ cur' = next + 1
    /\ next' = next + 1
    /\ child' = "aof"
    /\ snap' = data
    /\ rewrites' = rewrites + 1
    /\ UNCHANGED <<data, state, done, newBase, rdb, writes, crashes, switches>>

MutantNext ==
    \/ MutantWrite \/ Flush
    \/ MutantStartRewrite \/ RenameBase \/ PersistManifest \/ DeleteHistory \/ RewriteFails
    \/ StopAof \/ StartAof \/ StartSave \/ SaveDone \/ Crash
    \/ Idle

MutantSpec == Init /\ [][MutantNext]_vars
=============================================================================
