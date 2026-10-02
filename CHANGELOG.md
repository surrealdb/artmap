# Changelog

## 0.8.0 (2026-10-02)

Removes now reclaim the inner nodes they empty, `shrink_to_fit` fits the rest on demand, and the versioned maps can remove, prune and clear keys. Before this release, a node emptied by removes stayed in the tree until `clear()` or drop, and a deleted versioned key kept its leaf and a tombstone for good, so a map used as a queue or a registry, whose keys keep changing, grew without bound.

### Added
- `ArtMap::shrink_to_fit`, which fits every node to its entries: a node left with one leaf gives way to it, one left with a single child node is merged into it when their prefixes fit in one node, and every other node is shrunk to the smallest layout that holds its entries. It runs in O(n), under one pin, alongside readers and writers. A registry of 200,000 keys thinned to 2,000 holds 735 KB after its removes, and 94 KB after `shrink_to_fit`. `ArtSet` and `VersionedArtMap` have it too.
- `VersionedArtMap` and `ArenaVersionedArtMap`:
  - `remove_key(key)`, which removes a key and every version of it, for every snapshot;
  - `remove_version(key, version)`, which removes one version (a value or a tombstone), for example to roll back a write, and the key with its last version;
  - `clear()`.
- `ArenaVersionedArtMap::prune_key` and `prune_all`, as on `VersionedArtMap`.
- `VersionedArtMap::validate_invariants`.

### Changed behaviour
- A remove that takes the last entry of a node unlinks the node (`docs/SAFETY.md` §13). If that leaves the parent empty, the parent goes too, and so on up, so a chain of `Node4`s under a long shared prefix goes with the last key below it. A node that keeps an entry is left as it is, until `shrink_to_fit`.
- `ArtMap` frees unlinked nodes through EBR. A queue with 1,000 live keys holds 49 KB after 2M inserts and removes, down from 16.7 MB.
- `ArenaArtMap` unlinks emptied nodes too, so scans do not walk dead nodes. Arena memory is still freed only on drop, and the arena maps have no `shrink_to_fit`: its copies would only add to the arena.
- `validate_invariants` also checks that no inner node is empty.
- `prune_key` and `prune_all` unlink a key whose newest version at or below the watermark is its newest version and a tombstone (a `delete`, or a value `is_tombstone` accepts): no snapshot at or above the watermark can see it. They used to keep the key, with a built-in tombstone. Their counts include the versions of unlinked keys.
- A versioned queue that prunes behind itself stays bounded: 1,000 live keys hold 85 KB after 300,000 inserts and deletes, down from 24 MB.
- The watermark contract covers writes: after a prune at `min_version`, a write at or below `min_version` is unsupported, and one to a key the prune unlinked starts the key afresh.
- A versioned entry handle's `is_superseded()` is also `true` once its key is removed, pruned away or cleared.

### Fixed
- `ArenaVersionedArtMap::validate_invariants` panicked on any map with a deleted key: it compared `len()`, which counts live keys, with the number of leaves.

### Performance
Against 0.6.0 on an AMD Threadripper 9970X, two interleaved rounds per tree (`benches/safety_bench.rs`, `benches/comparison_bench.rs`):
- A remove that keeps an entry in its node locks only that node, as before. Removing and re-inserting keys costs the same as before, in a dense map and in the worst case for unlinking, keys in pairs under their own `Node4` (78 ns per remove and re-insert either way).
- Scanning the 1,000 live keys of a queue that has seen 1M pushes is 7× faster (24 µs → 3.6 µs), because the scan no longer walks empty nodes. Queue push and pop is unchanged.
- Point reads, inserts, overwrites, range scans, versioned updates and the README's concurrent benchmarks are unchanged within noise.
- `shrink_to_fit` takes 81 µs on a map of 100,000 random keys thinned to 1,000.
- The versioned insert path gains one branch, on whether the chain lock is dead; versioned updates measure within 2% of before (Apple M2 Max, one codegen unit).

### Verification
- loom models of removes that empty a node against lookups, inserts into that node, the arena inserter's cached-node insert, a second remove, prefix-chain follow-ups and `clear`; and of `shrink_to_fit` shrinking, collapsing and merging against lookups, inserts into the replaced node or child, and removes. Three new mutants must fail: a node unlinked without its own latch, a node fitted without its own latch, and a merge without the child's latch.
- Unit tests of unlinking and of every `shrink_to_fit` shape, including a cascade in one pass and fitted-tree checks; node accounting: every node is freed or retired exactly once under random churn with `shrink_to_fit` mixed in.
- Concurrent tests that churn keys around sentinels in every shape while a thread runs `shrink_to_fit`, for `ArtMap`, and with an arena inserter for `ArenaArtMap`; run under Miri and the sanitizers with the rest of those suites. The `ArtMap` model test runs `shrink_to_fit` among its operations.
- Versioned leaves: loom models of a prune, `remove_key`, `remove_version` and `clear` against an insert of the same key, with a mutant that unlinks without killing the chain lock; one suite of remove, prune and clear semantics for both versioned maps (`tests/versioned_reclaim.rs`); drop accounting for both; exact per-thread oracles under concurrency; and the versioned model tests.
- `tests/retention.rs` measures live memory under queue, prefix-chain, registry, thinned-registry and versioned-queue churn with a counting allocator. It fails on 0.6.

### Known limitations
- Without `shrink_to_fit`, a node keeps its layout as it loses children, until it is empty.

## 0.7.0 (2026-10-01)

### Added

- `ArtSet<K>`: an ordered, concurrent set, a thin wrapper over `ArtMap<K, ()>`. `insert` is insert-if-absent and returns a `bool`, as do `remove` and `contains`. Iterators, `range` and `scan` yield keys.

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
