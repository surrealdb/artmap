# Changelog

## Unreleased

Removes now reclaim inner nodes. In 0.6, a node emptied by removes stayed in the tree until `clear()` or drop, so a map used as a queue or a registry, whose keys keep changing, grew without bound. There are no API changes.

### Changed behaviour
- Removes compact the tree (`docs/SAFETY.md` §13):
  - a node left empty is unlinked;
  - a node left with a single leaf is replaced by that leaf;
  - a node left with a single inner child is merged into that child, when their prefixes fit in one node.
- If a compaction leaves the parent empty or holding a single leaf, the parent is compacted too, and so on up. A chain of `Node4`s under a long shared prefix collapses with its subtree.
- `ArtMap` frees unlinked nodes through EBR. A queue with 1,000 live keys holds 49 KB after 2M inserts and removes, down from 16.7 MB; a registry emptied by removes goes back to 0 bytes (5.8 MB in 0.6).
- `ArenaArtMap` compacts too, so scans do not walk dead nodes. Arena memory is still freed only on drop: unlinked nodes stay in the arena, and re-inserting under a prefix that was compacted away allocates its nodes again.
- `validate_invariants` also checks that no inner node is empty or holds a single leaf.
- The versioned maps are unchanged. They never unlink a key, so their nodes are never emptied.

### Performance
Single-threaded, against 0.6.0, on an Apple M2 Max (`benches/safety_bench.rs`):
- A remove that leaves two or more entries in its node locks only that node, as before, and costs the same: removing and re-inserting keys in a dense map, and queue push and pop (insert one key, remove the oldest, 1,000 apart), are unchanged within noise.
- Scanning the 1,000 live keys of a queue that has seen 1M pushes is 7× faster (45 µs → 6.3 µs), because the scan no longer walks empty nodes.
- The worst case is toggling keys that sit in pairs under their own `Node4`: every remove collapses the node, and every re-insert allocates a new one. It is about 40% slower (133 ns → 185 ns per remove and re-insert).

### Verification
- loom models of compaction against lookups, inserts into the same node, the arena inserter's cached-node insert, a merge against an insert into the child, prefix-chain follow-ups and `clear`. Two new mutants must fail: a node unlinked without its own latch, and a merge that does not bump the merged node's version.
- Unit tests of every compaction shape, and node accounting: every node is freed or retired exactly once under random churn.
- Concurrent tests that churn keys around sentinels in every compaction shape, for `ArtMap` and `ArenaArtMap`, run under Miri and the sanitizers with the rest of those suites.
- `tests/retention.rs` measures live memory under queue, prefix-chain and registry churn with a counting allocator. It fails on 0.6.

### Known limitations
- Nodes are not shrunk to a smaller layout as they lose children.
- `VersionedArtMap` never unlinks a deleted key's leaf, even after `prune_key`.

## 0.6.0 (2026-09-29)

0.6 rebuilds all four maps on one shared ART core. The goal is a safe API that is sound, with memory safety checked by Miri, loom and the sanitizers. `docs/SAFETY.md` states the invariants that the code relies on.

### Security

`artmap` ≤ 0.5 has use-after-free and data-race bugs that are reachable from safe code:
- `ArtMap::get` returned a handle that outlived its epoch guard.
- `insert` and `remove` moved values out of published leaves.
- The `Node256` lock-free insert path raced with writers.
- Iterators could yield freed keys.
- `insert_with_guard` accepted guards from any collector.

These cannot be fixed without the API changes below. Upgrade to 0.6.

0.6 also moves to `byteslice` 0.2. In `byteslice` 0.1.0, cloning or slicing a view from `ByteSlice::with_borrowed` could outlive the borrowed buffer, or write to and free memory the caller owns.

### Breaking changes

**Every map**
- All four maps and their inserters are now **invariant** in `K` and `V`. Code that relied on covariance no longer compiles, for example shrinking `ArenaArtMap<&'static str, _>` to `ArenaArtMap<&'a str, _>`. `cargo semver-checks` does not flag this.
- The internals are sealed. `artmap::tree`, `artmap::node`, `artmap::latch`, `artmap::simd`, `artmap::versioned::tree` and the `arena::{node, tree, versioned_tree}` modules are private, and `artmap::Tree` is no longer exported.
- `validate_invariants` takes `&mut self`, so it cannot race a writer. Call it through `Arc::get_mut` after joining your threads.
- The MSRV is Rust 1.86.
- `AsBytes` is implemented for `byteslice` 0.2's `ByteSlice`, not 0.1's. `ByteSlice` keys need `byteslice = "0.2"`.

**`ArtMap`**

| 0.5 | 0.6 |
|---|---|
| `insert(k, v) -> Option<V>` | `insert(k, v) -> Option<EntryRef<'_, K, V>>`: the displaced entry, zero-copy |
| `remove(k) -> Option<V>` | `remove(k) -> Option<EntryRef<'_, K, V>>` |
| `remove_by_slice(k) -> Option<V>` | `remove_by_slice(k) -> Option<EntryRef<'_, K, V>>` |
| `insert_with_guard(k, v, &crossbeam_epoch::Guard)` | `insert_with_guard(k, v, &artmap::Guard<'_>)`, with the guard from `map.pin()` |
| `keys()` yields `&K`; `values()` yields `&V` | `keys()` yields `KeyRef` and `values()` yields `ValueRef`. Both dereference to the key or value and keep the iterator's guard alive. `keys_with_guard` and `values_with_guard` yield bare references |

For the old `insert` behaviour when `V: Clone`, use `insert_cloned(k, v) -> Option<V>`.

**Handles**
- `EntryRef<'a, K, V>` borrows the map and owns (or shares) an epoch guard. It is `!Send`, and it cannot outlive the map.
- Iterator items share one guard per iterator.

**`VersionedArtMap`**
- `VersionedEntryRef` is no longer `Copy`. It is a snapshot of one version and never a tombstone. `is_superseded()` reports whether a newer write replaced it.
- `delete(key, version)` records a tombstone at `version`, and older snapshots are unchanged.
- `remove(key)` is deprecated. It now publishes a tombstone with the newest version's number and returns the replaced version.
- `get_all_versions` returns `Vec<(u64, Option<V>)>`, where `None` is a tombstone.
- `insert_with_guard` and `get_version_le_with_guard` take `&artmap::Guard<'_>`.
- `prune_key` returns the number of versions unlinked, and never unlinks the key itself.

**Arena maps**
- Inserters are bound to their map. Use `map.inserter()`, which returns `ArenaInserter<'_, K, V>` or `ArenaVersionedInserter<'_, K, V>`.
- Removed: `insert_with_inserter` on both maps, and `ArenaInserter::{new, update, matches, reset}`, `Default` and `Copy`/`Clone`.
- `ArenaVersionedEntryRef::versions(&self)` yields `VersionRef { version, value: Option<&V> }`, with no arena parameter. `leaf_ptr()` is removed.
- `ArenaArtMap::insert` and `remove` return `Option<ArenaEntryRef<'_, K, V>>`.
- `ArenaArtMap::debug_lookup` is removed.
- `Arena::{alloc, alloc_global, get_pointer, get_pointer_mut, offset_of}` are private.
- `Arena::new` clamps the capacity to `[64, MAX_ARENA_SIZE]`, and allocates 64-byte-aligned, zeroed memory.
- `ArenaVersionedArtMap` gains `delete(key, version)`; its `remove` is deprecated, as for the heap map.

### Added
- `artmap::Guard<'m>` and `pin()` on `ArtMap` and `VersionedArtMap`.
- `ArtMap`:
  - `get_with_guard`, `range_with_guard`, `iter_with_guard`, `keys_with_guard` and `values_with_guard`;
  - `get_value` and `with_value`, which copy out or borrow in a closure without keeping a handle;
  - `scan(range, |k, v| ...)`, which visits a range under one pin with no per-entry handle.
- `EntryRef`:
  - `to_owned` and `value_cloned`;
  - `is_removed`;
  - `remove()`, which removes the entry only if it is still the live one.
- Arena maps:
  - `try_insert`, which returns `Err(ArenaFull { key, value })` instead of panicking when the arena is full;
  - `max_insert_bytes(key_len)`, a `const fn` giving the worst-case arena bytes of one insert.
- `ArenaVersionedArtMap`:
  - `first_entry`, `last_entry`, `find_successor` and `find_predecessor`, all tombstone-aware;
  - `scan`.

### Changed behaviour
- Replacing a value is out-of-place. Existing handles keep reading the old value, and report `is_removed()`.
- Point operations are linearizable. `clear()` is linearizable too, with an exact `len()`.
- `len()` is exact when no operation is in flight, and never negative. It is striped by thread, so while several threads write it is approximate.
- Iterators yield every key that is present for the whole scan exactly once, in order, in both directions and for every bound kind.
- User code (`AsBytes`, `Clone`, `Drop`, closures) never runs under a latch. A panic in user code leaves the map usable.
- Dropping a map is synchronous and iterative. Deep keys no longer overflow the stack.
- Arena maps run `K` and `V` destructors on drop, including for replaced and removed entries. Teardown is $O(1)$ only when neither needs `Drop`.
- The `Node256` lock-free insert path is removed. All inserts take the node latch.

### Performance
Compared with 0.5.0 on an AMD Threadripper 9970X (runs on either side of a reboot, so differences within about 10% are noise):
- 8-thread writes are 35–50% faster, and mixed 4R+4W workloads 17–28% faster. The live-key count is striped by thread, so writers no longer contend on one cache line.
- Point gets are within about 10%.
- 100-item range scans are level for the arena maps (−10% to 0%), 18% slower for `ArtMap` and 32% slower for `VersionedArtMap`. `scan(range, callback)` avoids the per-entry handle.
- Single-threaded inserts are within about 15%. Arena inserts now pay their page faults as they touch memory, because creating an arena no longer zeroes it up front: `with_capacity(256 MiB)` takes microseconds instead of tens of milliseconds.
- Memory per key is unchanged for `ArtMap`, `ArenaArtMap` and `ArenaVersionedArtMap`, and 14% lower for `VersionedArtMap`: version flags live in the low bits of the chain link, and the chain latch is one byte.
- Overwrites are 45–55% slower (measured on aarch64), because each allocates a new leaf and retires the old one through EBR.

See `benches/comparison_bench.rs`, `benches/safety_bench.rs` and `benches/memory_bench.rs`.

### Known limitations
- Inner nodes emptied by removes are not reclaimed until `clear()` or drop. Delete-side compaction is planned after 0.6.
