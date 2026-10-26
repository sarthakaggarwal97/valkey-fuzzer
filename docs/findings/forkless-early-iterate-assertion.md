# Forkless save: `bgiteration.c:2689 'wasAdded' is not true`

Found by `valkey.forkless-save-crash` (live test
`test_forkless_save_crash_recovers[standalone]`), whose `valkey.liveness` oracle reported a `VALKEY BUG REPORT` in the node log.

- **Build:** valkey `unstable` at `cf5efa70` (`valkey-server v=255.255.255`)
- **Configuration:** `forkless-infrastructure-enabled yes`,
  `bgsave-default-method forkless`, `rdb-key-save-delay 500`, `appendonly yes`
- **Trigger:** `SET` and `DEL` on overlapping keys while a forkless `BGSAVE`
  is in progress. A crash-and-restart before the save is not required.
- **Effect:** the server aborts on an assertion during `SET`.

```
=== ASSERTION FAILED ===
==> bgiteration.c:2689 'wasAdded' is not true
bgIteration_updateDbEntryPtr <- setKey <- setGenericCommand <- setCommand <- call
```

## Reproduction

`forkless_del_during_save_repro.py` runs the trigger outside the fuzzer:

| Traffic during the save | Trials hitting the assertion |
| --- | --- |
| SET + DEL (15% DEL), fresh server | 2/25, 4/40 |
| SET + DEL, first save after SIGKILL + AOF reload | 1/25 |
| SET only | 0/40 |

It also reproduces on a `v=9.2.0` build at `6a6841cf` (the same
`serverAssert(wasAdded)` at `bgiteration.c:2689`): 3 of 30 fresh SET + DEL
trials.

Under the fuzzer, `valkey.forkless-save-crash` hit it in 2 of 22 standalone
runs. The plain `valkey.forkless-save` pack (fixed-size values, 10% DEL) did
not hit it in 15 runs.

```bash
python3 docs/findings/forkless_del_during_save_repro.py \
  /path/to/valkey-server 40 fresh del
```

## Likely mechanism (unconfirmed)

`bgIteration_updateDbEntryPtr()` moves an entry from `old` to `new` in the
iterator's `early_iterate_entries` set and asserts that `new` was not already
present. `DEL` adds the deleted entry to that set (`bgIteration_keyDelete`).
The set is pruned later, and line 2609 says the entry "might not be here".
If a deleted entry's pointer stays in the set after the entry is freed, a
later allocation at the same address makes the `hashtableAdd(new)` fail. That
fits a stale-pointer (ABA) pattern: it needs `DEL`, and it appears only after
some allocator reuse.
