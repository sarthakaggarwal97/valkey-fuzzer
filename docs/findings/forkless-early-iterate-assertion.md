# Forkless save: `bgiteration.c:2689 'wasAdded' is not true`

Found by `valkey.forkless-save-crash` (live test
`test_forkless_save_crash_recovers[standalone]`), whose `valkey.liveness` oracle reported a `VALKEY BUG REPORT` in the node log.

## Status upstream

- Known class of bug: item 3 of
  [valkey-io/valkey#4604](https://github.com/valkey-io/valkey/issues/4604)
  (dangling pointers in `early_iterate_entries`), reported for `RENAME`,
  `PEXPIRE`, and collection-emptying commands.
- Fix in review: [valkey-io/valkey#4996](https://github.com/valkey-io/valkey/pull/4996)
  removes the entry in `bgIteration_keyDelete()`.
- **That fix does not cover this trigger.** It still asserts at `50751f43`;
  see [Remaining gap after #4996](#remaining-gap-after-4996) and the comment
  draft in [`forkless-pr-4996-comment-draft.md`](forkless-pr-4996-comment-draft.md).

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

## Remaining gap after #4996

`bgIteration_keyDelete()` skips completed and terminated iterators before it
removes the entry from `early_iterate_entries`. A completed iterator stays in
`allIterators` until `bgIteratorRelease()` while the background thread drains
its queue, and `bgIteration_updateDbEntryPtr()` still walks it. A `DEL` in
that window leaves a dangling pointer; a later `SET` that reallocates an
early-iterated key onto the freed address makes `hashtableAdd(new)` fail.

| Build | Trials with the assertion |
| --- | --- |
| `8b9019c9` (#4996 base) | 2/30 |
| `50751f43` (#4996 head) | 2/120 |
| `50751f43` + also delete for completed/terminated iterators | 0/90 |

Logging added at that `continue` on `50751f43` showed a deleted entry left in
a completed iterator's set in 4 of 20 trials, and `updateDbEntryPtr()`
updating a completed iterator's set in 13 of 20. The crash itself also needs
the allocator to reuse the address, which is why it is rarer.
