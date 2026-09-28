<h1 align="center">artmap</h1>

<p align="center">A concurrent, in-memory Adaptive Radix Tree (ART) data structure for Rust.</p>

<br>

<p align="center">
    <a href="https://github.com/surrealdb/artmap"><img src="https://img.shields.io/badge/status-beta-ff00bb.svg?style=flat-square"></a>
    &nbsp;
    <a href="https://docs.rs/artmap/"><img src="https://img.shields.io/docsrs/artmap?style=flat-square"></a>
    &nbsp;
    <a href="https://crates.io/crates/artmap"><img src="https://img.shields.io/crates/v/artmap?style=flat-square"></a>
    &nbsp;
    <a href="https://github.com/surrealdb/artmap"><img src="https://img.shields.io/badge/license-Apache_License_2.0-00bfff.svg?style=flat-square"></a>
</p>

`artmap` is a concurrent, ordered, in-memory map backed by an **Adaptive Radix Tree (ART)**. It uses **optimistic lock coupling (OLC)**: readers never block and never write to shared memory, and writers latch only the nodes they change. Memory is reclaimed with **epoch-based reclamation (EBR)**, or, for the arena variants, all at once when the map is dropped.

There are four map types:
- **`artmap::ArtMap`**: a key-value map. Removed and replaced entries are reclaimed through `crossbeam-epoch`.
- **`artmap::VersionedArtMap`**: a map with a chain of 64-bit MVCC versions per key, snapshot reads (`get_version_le`, `get_latest`), tombstones (`delete`) and pruning (`prune_key`, `prune_all`).
- **`artmap::ArenaArtMap`**: a key-value map allocated from a fixed-size bump arena with 32-bit offsets. Inserts make no heap allocations, and nothing is freed until the map is dropped.
- **`artmap::ArenaVersionedArtMap`**: the versioned map on an arena, designed for LSM memtables.

### Variant Selection Matrix

| Memory Model | Unversioned (General Purpose) | Versioned / MVCC (Storage Engines) |
| :--- | :--- | :--- |
| **EBR / Dynamic Heap**<br><sup>(reclaimed via `crossbeam-epoch`)</sup> | **`artmap::ArtMap<K, V>`**<br>• Optimistic lock coupling (OLC)<br>• Out-of-place replacement: handles stay valid<br>• Dynamic heap growth and node resizing | **`artmap::VersionedArtMap<K, V>`**<br>• Snapshot reads (`get_version_le`)<br>• Per-key chain latch for version writes<br>• Tombstones and pruning |
| **Arena / 32-Bit Offsets**<br><sup>(fixed-size arena)</sup> | **`artmap::ArenaArtMap<K, V>`**<br>• Compact nodes with 32-bit child offsets<br>• 0 heap allocations per insert<br>• `try_insert` and `max_insert_bytes` for capacity planning | **`artmap::ArenaVersionedArtMap<K, V>`**<br>• 32-bit offset version chains<br>• Sequential inserter cache (`map.inserter()`)<br>• Memtable-oriented: no pruning, freed on drop |

## Performance

> **These numbers were measured on 0.5.0** and have not yet been re-measured for 0.6. The 0.6 rewrite fixes soundness bugs, and some operations are now slower: overwrites allocate a new leaf rather than updating one in place, and the lock-free `Node256` insert path is gone. Compared with 0.5.0 on an Apple M-series machine, 0.6 is 4–16% slower for a per-operation `get` (depending on the benchmark), 9–21% slower for 100-item range scans, and 45–55% slower for overwrites. Inserts of new keys and lookups under long shared prefixes are faster. See `benches/safety_bench.rs`.

Benchmarked on bare metal (**AMD Ryzen Threadripper 9970X 32-Core / 64-Thread Processor @ 5.48 GHz, 128 GB DDR5 RAM**, Linux 6.8):

| Data Structure | Read<br><sup>(with&nbsp;standard&nbsp;key)</sup> | Read<br><sup>(with&nbsp;slice&nbsp;key)</sup> | Insert<br><sup>(sequential&nbsp;entries)</sup> | Insert<br><sup>(random&nbsp;entries)</sup> | Range&nbsp;scans<br><sup>(100&nbsp;items)</sup> |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **`artmap::ArtMap`** | **16.5&nbsp;ns**<br><sup>(60.6M/s)</sup> | **20.7&nbsp;ns**<br><sup>(48.2M/s)</sup> | **28.1&nbsp;ns**<br><sup>(35.6M/s)</sup> | **44.4&nbsp;ns**<br><sup>(22.5M/s)</sup> | **618&nbsp;ns**<br><sup>(161.7M/s)</sup> |
| **`artmap::VersionedArtMap`** | **18.0&nbsp;ns**<br><sup>(55.4M/s)</sup> | **22.9&nbsp;ns**<br><sup>(43.5M/s)</sup> | **38.4&nbsp;ns**<br><sup>(26.0M/s)</sup> | **63.3&nbsp;ns**<br><sup>(15.8M/s)</sup> | **658&nbsp;ns**<br><sup>(152.1M/s)</sup> |
| **`artmap::ArenaArtMap`** | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**14.2&nbsp;ns**<br><sup>(70.3M/s)</sup> | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**13.9&nbsp;ns**<br><sup>(72.1M/s)</sup> | **30.5&nbsp;ns**<br><sup>(32.8M/s)</sup> | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**37.7&nbsp;ns**<br><sup>(26.5M/s)</sup> | **602&nbsp;ns**<br><sup>(166.0M/s)</sup> |
| **`artmap::ArenaVersionedArtMap`** | **15.3&nbsp;ns**<br><sup>(65.5M/s)</sup> | **15.3&nbsp;ns**<br><sup>(65.5M/s)</sup> | **42.2&nbsp;ns**<br><sup>(23.7M/s)</sup> | **50.2&nbsp;ns**<br><sup>(19.9M/s)</sup> | **610&nbsp;ns**<br><sup>(163.9M/s)</sup> |
| `arenaskiplist::SkipList` | 171.9&nbsp;ns<br><sup>(5.8M/s)</sup> | 171.9&nbsp;ns<br><sup>(5.8M/s)</sup> | 39.4&nbsp;ns<br><sup>(25.4M/s)</sup> | 156.4&nbsp;ns<br><sup>(6.4M/s)</sup> | 793&nbsp;ns<br><sup>(126.2M/s)</sup> |
| `concread::bptree::BPTree` | 46.2&nbsp;ns<br><sup>(21.6M/s)</sup> | — | 26.5&nbsp;ns<br><sup>(37.7M/s)</sup> | 66.6&nbsp;ns<br><sup>(15.0M/s)</sup> | 371&nbsp;ns<br><sup>(269.5M/s)</sup> |
| `crossbeam_skiplist::SkipMap` | 144.7&nbsp;ns<br><sup>(6.9M/s)</sup> | — | 75.3&nbsp;ns<br><sup>(13.3M/s)</sup> | 176.5&nbsp;ns<br><sup>(5.7M/s)</sup> | 2.19&nbsp;µs<br><sup>(45.7M/s)</sup> |
| `imbl::OrdMap` | 41.5&nbsp;ns<br><sup>(24.1M/s)</sup> | — | 52.8&nbsp;ns<br><sup>(19.0M/s)</sup> | 74.2&nbsp;ns<br><sup>(13.5M/s)</sup> | 341&nbsp;ns<br><sup>(293M/s)</sup> |
| `scc::TreeIndex` | 51.6&nbsp;ns<br><sup>(19.4M/s)</sup> | — | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**23.1&nbsp;ns**<br><sup>(43.3M/s)</sup> | 93.7&nbsp;ns<br><sup>(10.7M/s)</sup> | 273&nbsp;ns<br><sup>(366M/s)</sup> |
| `std::collections::BTreeMap` | 58.6&nbsp;ns<br><sup>(17.1M/s)</sup> | — | 31.5&nbsp;ns<br><sup>(31.7M/s)</sup> | 64.7&nbsp;ns<br><sup>(15.5M/s)</sup> | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**183&nbsp;ns**<br><sup>(546M/s)</sup> |
| `vart::Tree` | 28.9&nbsp;ns<br><sup>(34.6M/s)</sup> | 27.3&nbsp;ns<br><sup>(36.6M/s)</sup> | 83.2&nbsp;ns<br><sup>(12.0M/s)</sup> | 114.6&nbsp;ns<br><sup>(8.7M/s)</sup> | 819&nbsp;ns<br><sup>(122.1M/s)</sup> |
| `dashmap::DashMap`* | 18.7&nbsp;ns<br><sup>(53.5M/s)</sup> | — | 20.9&nbsp;ns<br><sup>(47.8M/s)</sup> | 19.3&nbsp;ns<br><sup>(51.8M/s)</sup> | N/A |
| `papaya::HashMap`* | 18.8&nbsp;ns<br><sup>(53.2M/s)</sup> | — | 30.4&nbsp;ns<br><sup>(32.9M/s)</sup> | 32.8&nbsp;ns<br><sup>(30.5M/s)</sup> | N/A |
| `scc::HashIndex`* | 20.2&nbsp;ns<br><sup>(49.5M/s)</sup> | — | 18.7&nbsp;ns<br><sup>(53.5M/s)</sup> | 19.8&nbsp;ns<br><sup>(50.5M/s)</sup> | N/A |
| `std::collections::HashMap`* | 13.1&nbsp;ns<br><sup>(76.1M/s)</sup> | — | 18.7&nbsp;ns<br><sup>(53.5M/s)</sup> | 21.1&nbsp;ns<br><sup>(47.4M/s)</sup> | N/A |

<sup>* `dashmap::DashMap`, `papaya::HashMap`, `scc::HashIndex`, and `std::collections::HashMap` are marked with `*` as unordered $O(1)$ reference baselines and do not support range queries, sorted scans, or ordered traversals. The rocket icon denotes the fastest implementation among ordered, concurrent range-scannable maps. Sequential insert times for `ArenaArtMap`, `ArenaVersionedArtMap`, and `arenaskiplist::SkipList` utilize their sequential inserter caches (`ArenaInserter` and `Inserter`).</sup>

### Multi-Threaded Concurrent Performance

When running multi-threaded workloads with concurrent writers, non-concurrent data structures (`BTreeMap`, `HashMap`, `imbl::OrdMap`) require synchronization via `parking_lot::RwLock`. Under write contention, exclusive lock acquisition serializes all threads, causing severe lock convoying and throughput collapse.

Benchmarked on bare metal (**AMD Ryzen Threadripper 9970X 32-Core / 64-Thread Processor @ 5.48 GHz, 128 GB DDR5 RAM**, Linux 6.8):

| Data Structure | Concurrent&nbsp;Writes<br><sup>(8&nbsp;Threads,&nbsp;100k&nbsp;Ops)</sup> | Mixed&nbsp;Workload<br><sup>(4R&nbsp;+&nbsp;4W,&nbsp;100k&nbsp;Ops)</sup> | Concurrency&nbsp;Model |
| :--- | ---: | ---: | :--- |
| **`artmap::ArtMap`** | **3.85&nbsp;ms**<br><sup>(25.9M/s)</sup> | **2.82&nbsp;ms**<br><sup>(35.4M/s)</sup> | Non-Blocking Reads + OLC Node Latching |
| **`artmap::VersionedArtMap`** | **4.18&nbsp;ms**<br><sup>(23.9M/s)</sup> | **3.33&nbsp;ms**<br><sup>(30.0M/s)</sup> | Non-Blocking Reads + OLC + Per-Key Chain Latch |
| **`artmap::ArenaArtMap`** | **3.34&nbsp;ms**<br><sup>(29.9M/s)</sup> | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**2.50&nbsp;ms**<br><sup>(39.9M/s)</sup> | OLC Node Latching + 32-Bit Offsets + Atomic Bump Allocation |
| **`artmap::ArenaVersionedArtMap`** | **3.34&nbsp;ms**<br><sup>(29.9M/s)</sup> | **2.90&nbsp;ms**<br><sup>(34.4M/s)</sup> | OLC Node Latching + 32-Bit Offsets + Per-Key Chain Latch |
| `arenaskiplist::SkipList` | 40.50&nbsp;ms<br><sup>(2.47M/s)</sup> | 28.28&nbsp;ms<br><sup>(3.54M/s)</sup> | Lock-Free Atomic CAS (Contiguous Arena) |
| `concread::bptree::BPTree` | 8.63&nbsp;ms<br><sup>(11.6M/s)</sup> | 6.23&nbsp;ms<br><sup>(16.1M/s)</sup> | Lock-Free Reads + Single-Writer CoW (MVCC) |
| `crossbeam_skiplist::SkipMap` | 10.31&nbsp;ms<br><sup>(9.70M/s)</sup> | 9.69&nbsp;ms<br><sup>(10.3M/s)</sup> | Lock-Free Atomic CAS |
| `parking_lot::RwLock<BTreeMap>` | 68.19&nbsp;ms<br><sup>(1.47M/s)</sup> | 38.05&nbsp;ms<br><sup>(2.63M/s)</sup> | Coarse Exclusive Lock |
| `parking_lot::RwLock<imbl::OrdMap>` | 76.39&nbsp;ms<br><sup>(1.31M/s)</sup> | 54.98&nbsp;ms<br><sup>(1.82M/s)</sup> | Coarse Exclusive Lock |
| `parking_lot::RwLock<vart::Tree>` | 74.12&nbsp;ms<br><sup>(1.35M/s)</sup> | 41.25&nbsp;ms<br><sup>(2.42M/s)</sup> | Coarse Exclusive Lock (Persistent CoW) |
| `scc::TreeIndex` | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**1.96&nbsp;ms**<br><sup>(51.0M/s)</sup> | **3.67&nbsp;ms**<br><sup>(27.2M/s)</sup> | Lock-Free Reads + Node Latching (B-Link) |
| `dashmap::DashMap`* | 3.67&nbsp;ms<br><sup>(27.2M/s)</sup> | 5.10&nbsp;ms<br><sup>(19.6M/s)</sup> | Fine-Grained Sharded RwLock |
| `papaya::HashMap`* | 4.26&nbsp;ms<br><sup>(23.5M/s)</sup> | 6.12&nbsp;ms<br><sup>(16.3M/s)</sup> | Lock-Free Reads + Fine-Grained Latching (EBR) |
| `parking_lot::RwLock<HashMap>`* | 76.64&nbsp;ms<br><sup>(1.30M/s)</sup> | 43.17&nbsp;ms<br><sup>(2.32M/s)</sup> | Coarse Exclusive Lock |
| `scc::HashIndex`* | 2.40&nbsp;ms<br><sup>(41.7M/s)</sup> | 2.85&nbsp;ms<br><sup>(35.1M/s)</sup> | Lock-Free Reads + Bucket Latching |

### Memory Footprint & Allocation Overhead

Benchmarked with 100,000 keys (64-bit integer keys and 64-bit values), measuring idle data structure size in memory when full, peak heap memory during continuous insertion, allocation frequency, and whole-structure teardown cost:

| Data Structure | Idle&nbsp;Memory<br><sup>(100k&nbsp;items)</sup> | Peak&nbsp;Memory<br><sup>(during&nbsp;ingest)</sup> | Allocations<br><sup>(per&nbsp;insert)</sup> | Teardown&nbsp;/&nbsp;Reset<br><sup>(deallocation&nbsp;cost)</sup> |
| :--- | ---: | ---: | ---: | :--- |
| **`artmap::ArtMap`** | **5.21&nbsp;MB**<br><sup>(52.1 B/item)</sup> | **5.21&nbsp;MB** | **1.0** | $O(N)$ epoch-deferred reclamation |
| **`artmap::VersionedArtMap`** | **6.73&nbsp;MB**<br><sup>(67.3 B/item)</sup> | **6.73&nbsp;MB** | **1.0** | $O(N)$ epoch-deferred reclamation |
| **`artmap::ArenaArtMap`** | **4.71&nbsp;MB**<br><sup>(47.1 B/item)</sup> | **8.00&nbsp;MB** | **0** | **$O(1)$** when `K` and `V` need no `Drop` |
| **`artmap::ArenaVersionedArtMap`** | **5.52&nbsp;MB**<br><sup>(55.2 B/item)</sup> | **10.00&nbsp;MB** | **0** | **$O(1)$** when `K` and `V` need no `Drop` |
| `arenaskiplist::SkipList` | 9.42&nbsp;MB<br><sup>(94.2 B/item)</sup> | 16.00&nbsp;MB | **0** | **$O(1)$ zero-cost reset** |
| `concread::bptree::BPTree` | 6.37&nbsp;MB<br><sup>(63.7 B/item)</sup> | 6.87&nbsp;MB | ~7.5 | $O(N)$ CoW heap drop |
| `crossbeam_skiplist::SkipMap` | 3.82&nbsp;MB<br><sup>(38.2 B/item)</sup> | 3.82&nbsp;MB | ~1.0 | $O(N)$ epoch-deferred reclamation |
| `imbl::OrdMap` | 2.69&nbsp;MB<br><sup>(26.9 B/item)</sup> | 2.69&nbsp;MB | ~0.14 | $O(N)$ recursive heap drop |
| `scc::TreeIndex` | 3.74&nbsp;MB<br><sup>(37.4 B/item)</sup> | 3.75&nbsp;MB | ~0.08 | $O(N)$ epoch-deferred reclamation |
| `std::collections::BTreeMap` | 2.58&nbsp;MB<br><sup>(25.8 B/item)</sup> | 2.58&nbsp;MB | ~0.17 | $O(N)$ recursive heap drop |
| `vart::Tree` | 26.30&nbsp;MB<br><sup>(263.0 B/item)</sup> | 26.30&nbsp;MB | 1.0 | $O(N)$ recursive Arc drop |
| `dashmap::DashMap`* | 2.14&nbsp;MB<br><sup>(21.4 B/item)</sup> | 2.15&nbsp;MB | ~0 | $O(N)$ heap drop |
| `papaya::HashMap`* | 3.80&nbsp;MB<br><sup>(38.0 B/item)</sup> | 3.80&nbsp;MB | ~1.0 | $O(N)$ epoch-deferred reclamation |
| `scc::HashIndex`* | 2.28&nbsp;MB<br><sup>(22.8 B/item)</sup> | 3.42&nbsp;MB | ~0 | $O(N)$ epoch-deferred reclamation |
| `std::collections::HashMap`* | 2.13&nbsp;MB<br><sup>(21.3 B/item)</sup> | 3.19&nbsp;MB | ~0 | $O(N)$ heap drop |

<sup>* For sequential keys (e.g. monotonically increasing timestamps or auto-incrementing IDs), radix prefix compression reduces `ArenaArtMap`'s net size to **2.98 MB** (29.8 B/item) and `ArenaVersionedArtMap` to **3.79 MB** (37.9 B/item).</sup>

- **Point reads**: radix path resolution in `ArenaArtMap` completed random point lookups in **13.9 ns**, against **171.9 ns** for `arenaskiplist` and **144.7 ns** for `crossbeam-skiplist::SkipMap`.
- **Range scans**: bitmapped child navigation on `Node48` and `Node256` scanned 100 contiguous items in **602 ns**, against **793 ns** for `arenaskiplist` and **2.19 µs** for `crossbeam-skiplist::SkipMap`.
- **Sequential inserts**: `map.inserter()` caches the last insertion point, so ordered or localised keys skip most of the descent.
- **No per-insert heap allocations** in the arena maps, and an $O(1)$ drop when `K` and `V` need no `Drop`.

## Features

- **Adaptive Radix Tree**: inner nodes resize between four layouts (`Node4`, `Node16`, `Node48` and `Node256`) for cache locality. Prefix compression collapses single-child paths.
- **Optimistic lock coupling**: readers validate per-node version counters and retry on conflict. They never block and never write to shared memory.
- **MVCC**: per-key chains of 64-bit versions with snapshot reads, tombstones and pruning.
- **SIMD `Node16` search**: SSE2 on x86_64 and NEON on aarch64.
- **Slice lookups**: query with `&[u8]` or `&str` without building a key (`get_by_slice`, `contains_key_slice`).
- **Double-ended iterators**: forward and reverse scans over any range (`map.range(a..b).rev()`).
- **Thread safety**: every map is `Send + Sync` when `K` and `V` are `Send + Sync`. Compile-time assertions check this, and compile-fail tests check that handles and iterators are `!Send`.

## Quick Start

Add `artmap` to your `Cargo.toml`:

```toml
[dependencies]
artmap = "0.6"
```

```rust
use artmap::ArtMap;
use std::sync::Arc;
use std::thread;

let map = Arc::new(ArtMap::<String, i32>::new());

// Concurrent writers.
let handles: Vec<_> = (0..4)
    .map(|t| {
        let map = Arc::clone(&map);
        thread::spawn(move || {
            for i in 0..1000 {
                map.insert(format!("users:{:04}", t * 1000 + i), i);
            }
        })
    })
    .collect();
for h in handles {
    h.join().unwrap();
}
assert_eq!(map.len(), 4000);

// A point lookup returns a handle that dereferences to the value.
assert_eq!(map.get("users:0500").as_deref(), Some(&500));

// Or copy the value out.
assert_eq!(map.get_value("users:0500"), Some(500));

// Lookup by raw bytes, without building a `String`.
assert!(map.contains_key_slice(b"users:0500"));

// Range scans, in key order.
let mut n = 0;
for entry in map.range("users:0100".."users:0200") {
    assert!(entry.key().as_str() >= "users:0100");
    n += 1;
}
assert_eq!(n, 100);
```

## Core Operations

### Handles and guards

`get`, `insert`, `remove` and the iterators return handles (`EntryRef`) that borrow the map and keep an epoch guard alive, so the entry stays valid even if another thread removes or replaces it:

```rust
use artmap::ArtMap;

let map = ArtMap::<String, String>::new();
map.insert("k".to_string(), "v1".to_string());

let old = map.get("k").unwrap();
// `insert` returns the displaced entry, if there was one.
let displaced = map.insert("k".to_string(), "v2".to_string()).unwrap();
assert_eq!(displaced.value(), "v1");

// The old handle still reads its own snapshot, and knows it was replaced.
assert_eq!(old.value(), "v1");
assert!(old.is_removed());
assert_eq!(map.get("k").unwrap().value(), "v2");
```

To amortise pinning over many operations, pin once and use the `*_with_guard` methods:

```rust
use artmap::ArtMap;

// Big-endian bytes keep integer keys in numeric order.
let map = ArtMap::<[u8; 8], u64>::new();
for i in 0..100u64 {
    map.insert(i.to_be_bytes(), i * 2);
}

let guard = map.pin();
let mut sum = 0;
for i in 0..100u64 {
    if let Some(v) = map.get_with_guard(&i.to_be_bytes(), &guard) {
        sum += *v;
    }
}
assert_eq!(sum, 9900);
```

**Holding a handle, iterator or guard stalls memory reclamation process-wide**, for every user of the default `crossbeam-epoch` collector, so do not hold one across I/O. Handles are `!Send`. Across an `.await`, use `get_value`, `with_value`, `EntryRef::to_owned` or `EntryRef::value_cloned`.

### Concurrent reads and writes

```rust
use artmap::ArtMap;
use std::sync::Arc;

let map = Arc::new(ArtMap::<String, i32>::new());
map.insert("key:1".to_string(), 100);

let reader_map = Arc::clone(&map);
let writer_map = Arc::clone(&map);

// Readers never block.
let reader = std::thread::spawn(move || reader_map.get_value("key:1"));
// Writers latch only the nodes they change.
let writer = std::thread::spawn(move || {
    writer_map.insert("key:2".to_string(), 200);
});

assert_eq!(reader.join().unwrap(), Some(100));
writer.join().unwrap();
assert_eq!(map.len(), 2);
```

### Bidirectional range scans

```rust
use artmap::ArtMap;

let map = ArtMap::<String, u32>::new();
for i in 0..200u32 {
    map.insert(format!("prefix:{i:03}"), i);
}

let forward: Vec<u32> = map.range("prefix:001".."prefix:100").map(|e| *e).collect();
assert_eq!(forward.len(), 99);
assert_eq!(forward.first(), Some(&1));

let reverse: Vec<u32> = map.range("prefix:001".."prefix:100").rev().map(|e| *e).collect();
assert_eq!(reverse.first(), Some(&99));
```

Iterators guarantee that every key present for the whole scan is yielded exactly once, in order. Keys inserted or removed during the scan may or may not appear.

### Versioned maps

```rust
use artmap::VersionedArtMap;

let map = VersionedArtMap::<String, u64>::new();
map.insert("account:1001".to_string(), 1, 500);
map.insert("account:1001".to_string(), 2, 750);

// Snapshot reads.
assert_eq!(map.get_version_le("account:1001", 1), Some((1, 500)));
assert_eq!(map.get_version_le("account:1001", 2), Some((2, 750)));

// A tombstone at version 3 hides the key from newer snapshots only.
map.delete("account:1001".to_string(), 3);
assert_eq!(map.get("account:1001"), None);
assert_eq!(map.get_version_le("account:1001", 2), Some((2, 750)));

// Drop versions that no snapshot at or above 3 can see.
map.prune_key("account:1001", 3, |_| false);
assert_eq!(map.version_count("account:1001"), 1);
```

### Arena maps

The arena maps allocate every node, leaf and version from one fixed-size arena:

```rust
use artmap::{ArenaArtMap, ArenaVersionedArtMap};

let map = ArenaArtMap::<String, i32>::with_capacity(16 * 1024 * 1024);
map.insert("account:1001".to_string(), 500);
assert_eq!(map.get("account:1001"), Some(500));

// `try_insert` returns the key and value instead of panicking when full.
let tiny = ArenaArtMap::<String, i32>::with_capacity(64);
let full = tiny.try_insert("k".to_string(), 1).unwrap_err();
assert_eq!((full.key.as_str(), full.value), ("k", 1));

// The worst-case arena bytes one insert can consume, for capacity planning.
assert!(ArenaArtMap::<String, i32>::max_insert_bytes(16) > 0);

let vmap = ArenaVersionedArtMap::<String, i32>::with_capacity(16 * 1024 * 1024);
vmap.insert("account:1001".to_string(), 1, 500);
vmap.insert("account:1001".to_string(), 2, 750);
assert_eq!(vmap.get_version_le("account:1001", 1), Some((1, 500)));

for entry in vmap.range("account:1000".."account:2000") {
    assert_eq!((entry.version(), *entry.value()), (2, 750));
    // Every version of the key, newest first.
    let all: Vec<_> = entry.versions().map(|v| (v.version, v.value.copied())).collect();
    assert_eq!(all, vec![(2, Some(750)), (1, Some(500))]);
}

// Ordered bulk loads go faster through an inserter.
let mut ins = map.inserter();
for i in 0..1000 {
    ins.insert(format!("seq:{i:05}"), i);
}
assert_eq!(map.len(), 1001);
```

The arena's capacity is fixed. Updates and removes do not free arena memory: replaced leaves and versions stay in the arena until the map is dropped. When `K` and `V` need `Drop`, dropping the map runs their destructors; otherwise it is $O(1)$. `Arena::reset` needs exclusive access, so it can only run once no map is using the arena.

## Memory retention

Removed and replaced **entries** are reclaimed: once no guard can still see them in the EBR maps, or when the map is dropped in the arena maps. **Inner nodes are not yet reclaimed on delete.** A node emptied by removes stays in the tree until `clear()` or drop, so a workload that keeps deleting and inserting keys under ever-new prefixes grows. Delete-side compaction is planned for a release after 0.6.

## Verification

- **Miri**, under Tree Borrows: the EBR suites, and the arena suites leak-checked with strict provenance and symbolic alignment checks, on x86_64 and aarch64.
- **loom** models of the real latch and node protocol, with mutants that must fail: a missing writer fence, a missing validation fence, and missing lock coupling.
- **ThreadSanitizer and AddressSanitizer** over the race and stress tests.
- **Model tests** of every map and iterator against `BTreeMap`. Property tests over arbitrary byte keys, and a linearizability checker for point operations and `clear`.
- **Fault injection**: panicking `AsBytes`, `Clone` and closures, and re-entrant user code, at every call site.
- **Compile-fail tests** for handle lifetimes, variance, auto traits and the sealed internals.
- **Seeded sequential oracle fuzzing** (`tests/sim.rs` and `tests/versioned_simulation_tests.rs`). Each run is reproducible with `ARTMAP_SIM_SEED`. This is not deterministic simulation of concurrent schedules: concurrency is covered by loom, the sanitizers and the stress tests.

```bash
ARTMAP_SIM_SEED=20202 cargo test --test sim -- --nocapture
```

## Benchmarks

```bash
# Comparison against other ordered and unordered maps
cargo bench --bench comparison_bench

# Allocation and memory footprint
cargo bench --bench alloc_comparison

# Safety-plan benchmarks: guards, overwrites, churn, scans, versioned updates
cargo bench --bench safety_bench
cargo bench --bench memory_bench

# Run on remote dedicated hardware (AMD Threadripper)
./scripts/bench-remote.sh --all
```

## License

This project is licensed under the [Apache License, Version 2.0](LICENSE).
