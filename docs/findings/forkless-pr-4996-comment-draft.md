# Draft: comment for valkey-io/valkey#4996

Post on [valkey-io/valkey#4996](https://github.com/valkey-io/valkey/pull/4996)
("bgiteration: remove from early-iterate on key-delete"), which fixes item 3 of
[valkey-io/valkey#4604](https://github.com/valkey-io/valkey/issues/4604).

---

This fixes the RENAME path, but the same assertion still fires at `50751f43` with plain SET/DEL traffic.

`bgIteration_keyDelete()` skips completed and terminated iterators before the new `hashtableDelete()`:

```c
if (it->completed || it->terminated || !it->keyset_iter->isKeyInScope(it->keyset_iter, key)) continue;
```

A completed iterator stays in `allIterators` until `bgIteratorRelease()` (while the background thread drains its queue), and `bgIteration_updateDbEntryPtr()` still walks it. A DEL in that window leaves the deleted entry in that iterator's `early_iterate_entries`. A later SET that reallocates an early-iterated key onto the freed address collides:

```
# ==> bgiteration.c:2699 'wasAdded' is not true
0 bgIteration_updateDbEntryPtr  1 dbSetValue  2 setKey  3 setGenericCommand  4 setCommand  5 call
```

**Repro:** load 3,000 + 200 keys with `forkless-infrastructure-enabled yes`, `bgsave-default-method forkless`, `rdb-key-save-delay 500`, and `appendonly yes`. Run `BGSAVE`, then send about 250 ops/s to the 200 keys for 4 s: 85% `SET` of 1–128-byte values, 15% `DEL`. Script: [`forkless_del_during_save_repro.py`](https://github.com/sarthakaggarwal97/valkey-fuzzer/blob/generic-feature-fuzzer-complete/docs/findings/forkless_del_during_save_repro.py) (`python3 forkless_del_during_save_repro.py <valkey-server> 30 fresh del`).

| Build | Trials with the assertion |
| --- | --- |
| `8b9019c9` (PR base) | 2/30 |
| `50751f43` (this PR) | 2/120 |
| `50751f43` + also delete for completed/terminated iterators | 0/90 |

I also logged at the `continue` above whenever the entry was still in a completed iterator's set. On this PR, a DEL left a dangling entry in a completed iterator in 4 of 20 trials. `updateDbEntryPtr()` updated a completed iterator's set in 13 of 20.

`iteratorHasPassedKey()` returns early for completed or terminated iterators. After that, the only thing that touches their `early_iterate_entries` is `updateDbEntryPtr()`. So any of these closes the gap:

- also delete for completed/terminated iterators in `keyDelete()` (what I tested, below);
- skip completed/terminated iterators in `updateDbEntryPtr()`; or
- empty the set when the iterator completes or terminates.

```diff
-        if (it->completed || it->terminated || !it->keyset_iter->isKeyInScope(it->keyset_iter, key)) continue;
+        if (it->completed || it->terminated) {
+            /* A completed iterator stays in allIterators until it is released, and
+             * bgIteration_updateDbEntryPtr() still maintains its early_iterate_entries. */
+            hashtableDelete(it->early_iterate_entries, de);
+            continue;
+        }
+        if (!it->keyset_iter->isKeyInScope(it->keyset_iter, key)) continue;
```

Found by the `valkey.forkless-save-crash` pack in [valkey-fuzzer](https://github.com/sarthakaggarwal97/valkey-fuzzer/tree/generic-feature-fuzzer-complete).
