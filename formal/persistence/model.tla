---------------------------------- MODULE model ----------------------------------
(***************************************************************************)
(* Persistence ordering in one Valkey server (src/aof.c, src/rdb.c,        *)
(* loadDataFromDisk): the multi-part AOF (a manifest naming a base file    *)
(* and incremental files), its background rewrite, turning the AOF on and  *)
(* off at runtime (CONFIG SET appendonly), RDB saves, and a restart from   *)
(* the AOF or the RDB file after the process is killed.                    *)
(*                                                                         *)
(* A kill keeps what the process wrote to its files (the page cache), so   *)
(* fsync is not modeled; what matters is the order of the writes, renames, *)
(* manifest updates and deletions. Each step below is one event-loop pass  *)
(* or one system call that a kill can land between:                        *)
(*   - a write is replied to only after beforeSleep wrote aof_buf to the   *)
(*     current incremental file (flushAppendOnlyFile);                     *)
(*   - a rewrite first writes aof_buf out, opens a new incremental file    *)
(*     and names it in the manifest, then forks the base's snapshot        *)
(*     (rewriteAppendOnlyFileBackground, openNewIncrAofForAppend);         *)
(*   - when the child is done, the base is renamed into place, the         *)
(*     manifest is rewritten to name it with the current incremental file  *)
(*     only, and the files it no longer names are deleted                  *)
(*     (backgroundRewriteDoneHandler, aofDelHistoryFiles);                 *)
(*   - CONFIG SET appendonly yes waits for a rewrite (AOF_WAIT_REWRITE),   *)
(*     whose writes go to a temporary incremental file that the manifest   *)
(*     names only when the rewrite is done.                                *)
(* A restart loads the files the manifest names when appendonly is yes,    *)
(* the RDB file otherwise, and with appendonly yes opens the last          *)
(* incremental file to append to (aofOpenIfNeededOnServerStart).           *)
(*                                                                         *)
(* A kill while CONFIG SET appendonly yes waits for its rewrite restarts   *)
(* from the AOF the manifest names, which an earlier period left or none,  *)
(* and ignores the RDB file: the writes replied to meanwhile are not       *)
(* promised (WAITAOF counts none of them), and the documentation says to   *)
(* wait for the rewrite.                                                   *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    MaxWrites,    \* client writes, in all
    MaxFiles,     \* AOF files ever created
    MaxCrashes,   \* kills, each followed by a restart
    MaxSwitches,  \* CONFIG SET appendonly changes
    MaxRewrites   \* AOF rewrites started

VARIABLES
    data,     \* the data set in memory: the writes applied, in order
    buf,      \* aof_buf: writes applied and not yet written to the incremental file
    state,    \* aof_state: "off" | "wait" (AOF_WAIT_REWRITE) | "on"
    present,  \* the AOF files that exist
    content,  \* file -> the writes it holds
    man,      \* the manifest on disk: [base: file or 0, incrs: sequence of files]
    cur,      \* the file the server appends to (0: none)
    child,    \* the fork child: "none" | "aof" | "rdb"
    snap,     \* the child's snapshot of the data set
    done,     \* the rewrite's completion steps taken: "none" | "renamed" | "persisted"
    newBase,  \* the base file the finishing rewrite renamed into place
    rdb,      \* the RDB file's data set
    next,     \* the last AOF file created
    ackedOn,  \* writes replied to while the AOF was on, since it was last turned on
    writes,
    crashes,
    switches,
    rewrites

vars == <<data, buf, state, present, content, man, cur, child, snap, done, newBase, rdb, next,
          ackedOn, writes, crashes, switches, rewrites>>

Files == 1..MaxFiles
Range(s) == {s[i] : i \in 1..Len(s)}
IsPrefix(a, b) == Len(a) <= Len(b) /\ a = SubSeq(b, 1, Len(a))
Last(s) == s[Len(s)]

\* The files the manifest names.
Named(m) == (IF m.base = 0 THEN {} ELSE {m.base}) \cup Range(m.incrs)

RECURSIVE Cat(_)
Cat(fs) == IF fs = <<>> THEN <<>> ELSE content[Head(fs)] \o Cat(Tail(fs))

\* What loadAppendOnlyFiles loads: the base, then each incremental file in
\* order; nothing when the manifest names no file (AOF_NOT_EXIST). A missing
\* file makes the load fail; <<0>> stands for that, never a prefix of the data.
LoadAof ==
    IF ~(Named(man) \subseteq present) THEN <<0>>
    ELSE (IF man.base = 0 THEN <<>> ELSE content[man.base]) \o Cat(man.incrs)

\* What a restart would load now (loadDataFromDisk).
Restore == IF state # "off" THEN LoadAof ELSE rdb

\* The writes fed to aof_buf: with the AOF on, or waiting for its rewrite while
\* the rewrite child runs (feedAppendOnlyFile).
Feeding == state = "on" \/ (state = "wait" /\ child = "aof")

\* beforeSleep's write of aof_buf to the current file, and the replies it frees.
Flushed ==
    /\ content' = IF cur # 0 /\ buf # <<>> THEN [content EXCEPT ![cur] = @ \o buf] ELSE content
    /\ ackedOn' = IF state = "on" THEN ackedOn \cup Range(buf) ELSE ackedOn
    /\ buf' = <<>>

Init ==
    /\ data = <<>>
    /\ buf = <<>>
    /\ content = [f \in Files |-> <<>>]
    \* The server booted empty, with appendonly yes (a base and an incremental
    \* file: aofOpenIfNeededOnServerStart) or no (no AOF files).
    /\ \/ /\ state = "on"
          /\ present = {1, 2}
          /\ man = [base |-> 1, incrs |-> <<2>>]
          /\ cur = 2
          /\ next = 2
       \/ /\ state = "off"
          /\ present = {}
          /\ man = [base |-> 0, incrs |-> <<>>]
          /\ cur = 0
          /\ next = 0
    /\ child = "none"
    /\ snap = <<>>
    /\ done = "none"
    /\ newBase = 0
    /\ rdb = <<>>
    /\ ackedOn = {}
    /\ writes = 0
    /\ crashes = 0
    /\ switches = 0
    /\ rewrites = 0

-----------------------------------------------------------------------------
(* Clients and the event loop *)

\* A client write is applied and, when the AOF is fed, queued in aof_buf; its
\* reply waits for the next Flush.
Write ==
    /\ done = "none"
    /\ writes < MaxWrites
    /\ data' = Append(data, writes + 1)
    /\ buf' = IF Feeding THEN Append(buf, writes + 1) ELSE buf
    /\ writes' = writes + 1
    /\ UNCHANGED <<state, present, content, man, cur, child, snap, done, newBase, rdb, next,
                   ackedOn, crashes, switches, rewrites>>

\* beforeSleep: flushAppendOnlyFile writes aof_buf, then the replies go out.
Flush ==
    /\ done = "none"
    /\ buf # <<>>
    /\ Flushed
    /\ UNCHANGED <<data, state, present, man, cur, child, snap, done, newBase, rdb, next,
                   writes, crashes, switches, rewrites>>

-----------------------------------------------------------------------------
(* The AOF rewrite *)

\* BGREWRITEAOF, the automatic rewrite, or the one CONFIG SET appendonly yes
\* waits for: write aof_buf out (flushAppendOnlyFile(1)), open a new
\* incremental file (with the AOF on, name it in the manifest; while waiting, a
\* temporary one the manifest does not name), and fork the snapshot.
StartRewrite ==
    /\ state # "off"
    /\ child = "none"
    /\ rewrites < MaxRewrites
    /\ next < MaxFiles
    /\ Flushed
    /\ present' = present \cup {next + 1}
    /\ man' = IF state = "on" THEN [man EXCEPT !.incrs = Append(@, next + 1)] ELSE man
    /\ cur' = next + 1
    /\ next' = next + 1
    /\ child' = "aof"
    /\ snap' = data
    /\ rewrites' = rewrites + 1
    /\ UNCHANGED <<data, state, done, newBase, rdb, writes, crashes, switches>>

\* The child is done: the parent renames its temporary file to a new base.
RenameBase ==
    /\ child = "aof" /\ done = "none"
    /\ next < MaxFiles
    /\ present' = present \cup {next + 1}
    /\ content' = [content EXCEPT ![next + 1] = snap]
    /\ newBase' = next + 1
    /\ next' = next + 1
    /\ done' = "renamed"
    /\ UNCHANGED <<data, buf, state, man, cur, child, snap, rdb, ackedOn, writes, crashes,
                   switches, rewrites>>

\* The manifest is rewritten (temporary file, fsync, rename): the new base,
\* and the incremental file the server appends to; the files before it become
\* history (markRewrittenIncrAofAsHistory). A waiting AOF is now on.
PersistManifest ==
    /\ done = "renamed"
    /\ man' = [base |-> newBase, incrs |-> <<cur>>]
    /\ state' = "on"
    /\ done' = "persisted"
    /\ UNCHANGED <<data, buf, present, content, cur, child, snap, newBase, rdb, next, ackedOn,
                   writes, crashes, switches, rewrites>>

\* The history files are deleted (aofDelHistoryFiles), and the rewrite ends.
DeleteHistory ==
    /\ done = "persisted"
    /\ present' = present \cap (Named(man) \cup {cur})
    /\ child' = "none"
    /\ done' = "none"
    /\ newBase' = 0
    /\ snap' = <<>>
    /\ UNCHANGED <<data, buf, state, content, man, cur, rdb, next, ackedOn, writes, crashes,
                   switches, rewrites>>

\* The child fails (a write error, or killed). With the AOF waiting, the
\* temporary incremental file and aof_buf are dropped and another rewrite is
\* scheduled; with it on, the new incremental file stays in the manifest.
RewriteFails ==
    /\ child = "aof" /\ done = "none"
    /\ child' = "none"
    /\ snap' = <<>>
    /\ present' = IF state = "wait" THEN present \ {cur} ELSE present
    /\ buf' = IF state = "wait" THEN <<>> ELSE buf
    /\ UNCHANGED <<data, state, content, man, cur, done, newBase, rdb, next, ackedOn, writes,
                   crashes, switches, rewrites>>

-----------------------------------------------------------------------------
(* Operators and RDB *)

\* CONFIG SET appendonly no (stopAppendOnly): write aof_buf out, close the file,
\* kill a rewrite child. The manifest and its files stay. The AOF's promise ends.
StopAof ==
    /\ state # "off"
    /\ done = "none"
    /\ child # "rdb"
    /\ switches < MaxSwitches
    /\ content' = IF cur # 0 /\ buf # <<>> THEN [content EXCEPT ![cur] = @ \o buf] ELSE content
    /\ buf' = <<>>
    /\ state' = "off"
    /\ cur' = 0
    /\ child' = "none"
    /\ snap' = <<>>
    /\ ackedOn' = {}
    /\ switches' = switches + 1
    /\ UNCHANGED <<data, present, man, done, newBase, rdb, next, writes, crashes, rewrites>>

\* CONFIG SET appendonly yes (startAppendOnly): wait for a rewrite.
StartAof ==
    /\ state = "off"
    /\ done = "none"
    /\ switches < MaxSwitches
    /\ state' = "wait"
    /\ switches' = switches + 1
    /\ UNCHANGED <<data, buf, present, content, man, cur, child, snap, done, newBase, rdb, next,
                   ackedOn, writes, crashes, rewrites>>

\* BGSAVE forks a snapshot.
StartSave ==
    /\ child = "none"
    /\ child' = "rdb"
    /\ snap' = data
    /\ UNCHANGED <<data, buf, state, present, content, man, cur, done, newBase, rdb, next,
                   ackedOn, writes, crashes, switches, rewrites>>

\* The save is done: the temporary file, written and fsynced, is renamed to the
\* RDB file (rdbSave).
SaveDone ==
    /\ child = "rdb"
    /\ rdb' = snap
    /\ child' = "none"
    /\ snap' = <<>>
    /\ UNCHANGED <<data, buf, state, present, content, man, cur, done, newBase, next, ackedOn,
                   writes, crashes, switches, rewrites>>

\* The process is killed and restarts with the appendonly it had. aof_buf and
\* the child are lost; the files stay. With the AOF, it loads the files the
\* manifest names and opens the last incremental file, or, with none, writes a
\* base of the loaded data and a new incremental file (aofOpenIfNeededOnServerStart).
Crash ==
    LET aof == state # "off"
        loaded == Restore
        fresh == man.base = 0 /\ man.incrs = <<>>
        noIncr == man.incrs = <<>>
        need == IF ~aof THEN 0 ELSE IF fresh THEN 2 ELSE IF noIncr THEN 1 ELSE 0
    IN
    /\ crashes < MaxCrashes
    /\ ~aof \/ Named(man) \subseteq present
    /\ next + need <= MaxFiles
    /\ data' = loaded
    /\ buf' = <<>>
    /\ state' = IF aof THEN "on" ELSE "off"
    /\ child' = "none"
    /\ snap' = <<>>
    /\ done' = "none"
    /\ newBase' = 0
    /\ content' = IF aof /\ fresh THEN [content EXCEPT ![next + 1] = loaded] ELSE content
    /\ present' = present \cup {f \in Files : next < f /\ f <= next + need}
    /\ man' = CASE ~aof -> man
                [] fresh -> [base |-> next + 1, incrs |-> <<next + 2>>]
                [] noIncr -> [man EXCEPT !.incrs = <<next + 1>>]
                [] OTHER -> man
    /\ cur' = CASE ~aof -> 0
                [] fresh -> next + 2
                [] noIncr -> next + 1
                [] OTHER -> Last(man.incrs)
    /\ next' = next + need
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<rdb, ackedOn, writes, switches, rewrites>>

\* Nothing happens. Once the bounds are used up, some states enable no other
\* action; this keeps TLC from reporting them as deadlocks.
Idle == UNCHANGED vars

-----------------------------------------------------------------------------
Next ==
    \/ Write \/ Flush
    \/ StartRewrite \/ RenameBase \/ PersistManifest \/ DeleteHistory \/ RewriteFails
    \/ StopAof \/ StartAof \/ StartSave \/ SaveDone \/ Crash
    \/ Idle

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety properties *)

\* A restart with the AOF can load it: every file the manifest names exists.
ManifestFilesExist == state # "off" => Named(man) \subseteq present

\* With the AOF on, the files the manifest names hold a prefix of the data
\* set: no write twice, none out of order, none missing before one that is
\* there. (An RDB file is a snapshot of an earlier data set by construction;
\* after a kill it may hold writes the AOF never got, as their replies never
\* left.)
AofIsPrefix == state = "on" => IsPrefix(LoadAof, data)

\* While appendonly is yes, a kill loses no write replied to while the AOF was
\* on, since it was last turned on. (Writes replied to while appendonly yes
\* waits for its rewrite are not promised.)
AckedRestored == state # "off" => ackedOn \subseteq Range(Restore)

TypeOK ==
    /\ state \in {"off", "wait", "on"}
    /\ present \subseteq Files
    /\ cur \in 0..MaxFiles
    /\ child \in {"none", "aof", "rdb"}
    /\ done \in {"none", "renamed", "persisted"}
    /\ next \in 0..MaxFiles
    /\ ackedOn \subseteq 1..MaxWrites
    /\ writes \in 0..MaxWrites
    /\ crashes \in 0..MaxCrashes
=============================================================================
