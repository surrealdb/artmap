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

There is also **`artmap::ArtSet`**, an ordered set of keys. It is a thin wrapper over `ArtMap<K, ()>` with an insert-if-absent `insert` and `bool` results, and costs no more memory than the keys.

### Variant Selection Matrix

| Memory Model | Unversioned (General Purpose) | Versioned / MVCC (Storage Engines) |
| :--- | :--- | :--- |
| **EBR / Dynamic Heap**<br><sup>(reclaimed via `crossbeam-epoch`)</sup> | **`artmap::ArtMap<K, V>`**<br>• Optimistic lock coupling (OLC)<br>• Out-of-place replacement: handles stay valid<br>• Dynamic heap growth and node resizing | **`artmap::VersionedArtMap<K, V>`**<br>• Snapshot reads (`get_version_le`)<br>• Per-key chain latch for version writes<br>• Tombstones and pruning |
| **Arena / 32-Bit Offsets**<br><sup>(fixed-size arena)</sup> | **`artmap::ArenaArtMap<K, V>`**<br>• Compact nodes with 32-bit child offsets<br>• 0 heap allocations per insert<br>• `try_insert` and `max_insert_bytes` for capacity planning | **`artmap::ArenaVersionedArtMap<K, V>`**<br>• 32-bit offset version chains<br>• Sequential inserter cache (`map.inserter()`)<br>• Memtable-oriented: no pruning, freed on drop |

## Performance

Benchmarked on bare metal (**AMD Ryzen Threadripper 9970X 32-Core / 64-Thread Processor @ 5.48 GHz, 128 GB DDR5 RAM**, Linux 6.8):

| Data Structure | Read<br><sup>(with&nbsp;standard&nbsp;key)</sup> | Read<br><sup>(with&nbsp;slice&nbsp;key)</sup> | Insert<br><sup>(sequential&nbsp;entries)</sup> | Insert<br><sup>(random&nbsp;entries)</sup> | Range&nbsp;scans<br><sup>(100&nbsp;items)</sup> |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **`artmap::ArtMap`** | **24.8&nbsp;ns**<br><sup>(40.4M/s)</sup> | **25.1&nbsp;ns**<br><sup>(39.9M/s)</sup> | **32.5&nbsp;ns**<br><sup>(30.7M/s)</sup> | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/artmap/main/img/rocket.png" alt="🚀">&nbsp;**38.7&nbsp;ns**<br><sup>(25.9M/s)</sup> | **718&nbsp;ns**<br><sup>(139M/s)</sup> |
| **`artmap::VersionedArtMap`** | **21.9&nbsp;ns**<br><sup>(45.7M/s)</sup> | **21.6&nbsp;ns**<br><sup>(46.2M/s)</sup> | **48.2&nbsp;ns**<br><sup>(20.7M/s)</sup> | **69.4&nbsp;ns**<br><sup>(14.4M/s)</sup> | **807&nbsp;ns**<br><sup>(124M/s)</sup> |
| **`artmap::ArenaArtMap`** | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/artmap/main/img/rocket.png" alt="🚀">&nbsp;**13.4&nbsp;ns**<br><sup>(74.5M/s)</sup> | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/artmap/main/img/rocket.png" alt="🚀">&nbsp;**13.4&nbsp;ns**<br><sup>(74.8M/s)</sup> | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/artmap/main/img/rocket.png" alt="🚀">&nbsp;**27.1&nbsp;ns**<br><sup>(36.9M/s)</sup> | **39.8&nbsp;ns**<br><sup>(25.1M/s)</sup> | **574&nbsp;ns**<br><sup>(174M/s)</sup> |
| **`artmap::ArenaVersionedArtMap`** | **14.7&nbsp;ns**<br><sup>(68.2M/s)</sup> | **15.7&nbsp;ns**<br><sup>(63.6M/s)</sup> | **41.3&nbsp;ns**<br><sup>(24.2M/s)</sup> | **51.6&nbsp;ns**<br><sup>(19.4M/s)</sup> | **616&nbsp;ns**<br><sup>(162M/s)</sup> |
| `arenaskiplist::SkipList` | 181&nbsp;ns<br><sup>(5.5M/s)</sup> | 181&nbsp;ns<br><sup>(5.5M/s)</sup> | 39.5&nbsp;ns<br><sup>(25.3M/s)</sup> | 151&nbsp;ns<br><sup>(6.6M/s)</sup> | 809&nbsp;ns<br><sup>(124M/s)</sup> |
| `concread::bptree::BPTree` | 64.2&nbsp;ns<br><sup>(15.6M/s)</sup> | — | 44.9&nbsp;ns<br><sup>(22.3M/s)</sup> | 128&nbsp;ns<br><sup>(7.8M/s)</sup> | 434&nbsp;ns<br><sup>(231M/s)</sup> |
| `crossbeam_skiplist::SkipMap` | 160&nbsp;ns<br><sup>(6.2M/s)</sup> | — | 77.8&nbsp;ns<br><sup>(12.9M/s)</sup> | 174&nbsp;ns<br><sup>(5.8M/s)</sup> | 2.39&nbsp;µs<br><sup>(41.9M/s)</sup> |
| `imbl::OrdMap` | 41.8&nbsp;ns<br><sup>(23.9M/s)</sup> | — | 52.6&nbsp;ns<br><sup>(19.0M/s)</sup> | 74.6&nbsp;ns<br><sup>(13.4M/s)</sup> | 343&nbsp;ns<br><sup>(291M/s)</sup> |
| `std::collections::BTreeMap` | 62.4&nbsp;ns<br><sup>(16.0M/s)</sup> | — | 31.1&nbsp;ns<br><sup>(32.1M/s)</sup> | 65.7&nbsp;ns<br><sup>(15.2M/s)</sup> | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/artmap/main/img/rocket.png" alt="🚀">&nbsp;**235&nbsp;ns**<br><sup>(426M/s)</sup> |
| `vart::Tree` | 22.0&nbsp;ns<br><sup>(45.5M/s)</sup> | 20.6&nbsp;ns<br><sup>(48.6M/s)</sup> | 67.7&nbsp;ns<br><sup>(14.8M/s)</sup> | 69.6&nbsp;ns<br><sup>(14.4M/s)</sup> | 2.68&nbsp;µs<br><sup>(37.3M/s)</sup> |
| `dashmap::DashMap`* | 17.3&nbsp;ns<br><sup>(58.0M/s)</sup> | — | 23.6&nbsp;ns<br><sup>(42.4M/s)</sup> | 29.1&nbsp;ns<br><sup>(34.4M/s)</sup> | N/A |
| `papaya::HashMap`* | 21.7&nbsp;ns<br><sup>(46.1M/s)</sup> | — | 52.2&nbsp;ns<br><sup>(19.1M/s)</sup> | 56.9&nbsp;ns<br><sup>(17.6M/s)</sup> | N/A |
| `scc::HashIndex`* | 17.0&nbsp;ns<br><sup>(58.7M/s)</sup> | — | 27.9&nbsp;ns<br><sup>(35.9M/s)</sup> | 31.1&nbsp;ns<br><sup>(32.1M/s)</sup> | N/A |
| `std::collections::HashMap`* | 13.3&nbsp;ns<br><sup>(74.9M/s)</sup> | — | 18.8&nbsp;ns<br><sup>(53.3M/s)</sup> | 21.1&nbsp;ns<br><sup>(47.3M/s)</sup> | N/A |

<sup>* `dashmap::DashMap`, `papaya::HashMap`, `scc::HashIndex`, and `std::collections::HashMap` are marked with `*` as unordered $O(1)$ reference baselines and do not support range queries, sorted scans, or ordered traversals. The rocket icon marks the fastest ordered (range-scannable) map in each column. The sequential insert time for `arenaskiplist::SkipList` uses its sequential inserter cache (`Inserter`).</sup>

### Multi-Threaded Concurrent Performance

When running multi-threaded workloads with concurrent writers, non-concurrent data structures (`BTreeMap`, `HashMap`, `imbl::OrdMap`) require synchronization via `parking_lot::RwLock`. Under write contention, exclusive lock acquisition serializes all threads, causing severe lock convoying and throughput collapse.

Benchmarked on bare metal (**AMD Ryzen Threadripper 9970X 32-Core / 64-Thread Processor @ 5.48 GHz, 128 GB DDR5 RAM**, Linux 6.8):

| Data Structure | Concurrent&nbsp;Writes<br><sup>(8&nbsp;Threads,&nbsp;100k&nbsp;Ops)</sup> | Mixed&nbsp;Workload<br><sup>(4R&nbsp;+&nbsp;4W,&nbsp;100k&nbsp;Ops)</sup> | Concurrency&nbsp;Model |
| :--- | ---: | ---: | :--- |
| **`artmap::ArtMap`** | **2.00&nbsp;ms**<br><sup>(50.1M/s)</sup> | **2.07&nbsp;ms**<br><sup>(48.2M/s)</sup> | Non-Blocking Reads + OLC Node Latching |
| **`artmap::VersionedArtMap`** | **2.34&nbsp;ms**<br><sup>(42.8M/s)</sup> | **2.32&nbsp;ms**<br><sup>(43.1M/s)</sup> | Non-Blocking Reads + OLC + Per-Key Chain Latch |
| **`artmap::ArenaArtMap`** | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/artmap/main/img/rocket.png" alt="🚀">&nbsp;**1.76&nbsp;ms**<br><sup>(56.8M/s)</sup> | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/artmap/main/img/rocket.png" alt="🚀">&nbsp;**2.02&nbsp;ms**<br><sup>(49.6M/s)</sup> | OLC Node Latching + 32-Bit Offsets + Atomic Bump Allocation |
| **`artmap::ArenaVersionedArtMap`** | **2.23&nbsp;ms**<br><sup>(44.9M/s)</sup> | **2.23&nbsp;ms**<br><sup>(44.9M/s)</sup> | OLC Node Latching + 32-Bit Offsets + Per-Key Chain Latch |
| `arenaskiplist::SkipList` | 40.80&nbsp;ms<br><sup>(2.5M/s)</sup> | 28.81&nbsp;ms<br><sup>(3.5M/s)</sup> | Lock-Free Atomic CAS (Contiguous Arena) |
| `concread::bptree::BPTree` | 8.86&nbsp;ms<br><sup>(11.3M/s)</sup> | 4.59&nbsp;ms<br><sup>(21.8M/s)</sup> | Lock-Free Reads + Single-Writer CoW (MVCC) |
| `crossbeam_skiplist::SkipMap` | 10.10&nbsp;ms<br><sup>(9.9M/s)</sup> | 9.49&nbsp;ms<br><sup>(10.5M/s)</sup> | Lock-Free Atomic CAS |
| `parking_lot::RwLock<BTreeMap>` | 66.53&nbsp;ms<br><sup>(1.5M/s)</sup> | 39.01&nbsp;ms<br><sup>(2.6M/s)</sup> | Coarse Exclusive Lock |
| `parking_lot::RwLock<imbl::OrdMap>` | 78.35&nbsp;ms<br><sup>(1.3M/s)</sup> | 57.88&nbsp;ms<br><sup>(1.7M/s)</sup> | Coarse Exclusive Lock |
| `parking_lot::RwLock<vart::Tree>` | 77.81&nbsp;ms<br><sup>(1.3M/s)</sup> | 51.67&nbsp;ms<br><sup>(1.9M/s)</sup> | Coarse Exclusive Lock (Persistent CoW) |
| `dashmap::DashMap`* | 3.37&nbsp;ms<br><sup>(29.6M/s)</sup> | 3.60&nbsp;ms<br><sup>(27.7M/s)</sup> | Fine-Grained Sharded RwLock |
| `papaya::HashMap`* | 7.69&nbsp;ms<br><sup>(13.0M/s)</sup> | 8.12&nbsp;ms<br><sup>(12.3M/s)</sup> | Lock-Free Reads + Fine-Grained Latching (EBR) |
| `parking_lot::RwLock<HashMap>`* | 76.98&nbsp;ms<br><sup>(1.3M/s)</sup> | 44.66&nbsp;ms<br><sup>(2.2M/s)</sup> | Coarse Exclusive Lock |
| `scc::HashIndex`* | 3.00&nbsp;ms<br><sup>(33.3M/s)</sup> | 3.14&nbsp;ms<br><sup>(31.8M/s)</sup> | Lock-Free Reads + Bucket Latching |

### Memory Footprint & Allocation Overhead

Benchmarked with 100,000 keys (64-bit integer keys and 64-bit values), measuring idle data structure size in memory when full, peak heap memory during continuous insertion, allocation frequency, and whole-structure teardown cost:

| Data Structure | Idle&nbsp;Memory<br><sup>(100k&nbsp;items)</sup> | Peak&nbsp;Memory<br><sup>(during&nbsp;ingest)</sup> | Allocations<br><sup>(per&nbsp;insert)</sup> | Teardown&nbsp;/&nbsp;Reset<br><sup>(deallocation&nbsp;cost)</sup> |
| :--- | ---: | ---: | ---: | :--- |
| **`artmap::ArtMap`** | **5.46&nbsp;MB**<br><sup>(54.6 B/item)</sup> | **5.47&nbsp;MB** | **1.32** | $O(N)$ epoch-deferred reclamation |
| **`artmap::VersionedArtMap`** | **10.25&nbsp;MB**<br><sup>(102.5 B/item)</sup> | **10.25&nbsp;MB** | **1.32** | $O(N)$ epoch-deferred reclamation |
| **`artmap::ArenaArtMap`** | **4.92&nbsp;MB**<br><sup>(49.2 B/item)</sup> | **4.92&nbsp;MB** | **0** | **$O(1)$** when `K` and `V` need no `Drop` |
| **`artmap::ArenaVersionedArtMap`** | **6.52&nbsp;MB**<br><sup>(65.2 B/item)</sup> | **6.52&nbsp;MB** | **0** | **$O(1)$** when `K` and `V` need no `Drop` |
| `arenaskiplist::SkipList` | 5.77&nbsp;MB<br><sup>(57.7 B/item)</sup> | 5.77&nbsp;MB | **0** | **$O(1)$ zero-cost reset** |
| `concread::bptree::BPTree` | 6.69&nbsp;MB<br><sup>(66.9 B/item)</sup> | 7.22&nbsp;MB | 0.52 | $O(N)$ CoW heap drop |
| `crossbeam_skiplist::SkipMap` | 4.00&nbsp;MB<br><sup>(40.0 B/item)</sup> | 4.00&nbsp;MB | 1.00 | $O(N)$ epoch-deferred reclamation |
| `imbl::OrdMap` | 2.81&nbsp;MB<br><sup>(28.1 B/item)</sup> | 2.81&nbsp;MB | 0.10 | $O(N)$ recursive heap drop |
| `std::collections::BTreeMap` | 2.71&nbsp;MB<br><sup>(27.1 B/item)</sup> | 2.71&nbsp;MB | 0.13 | $O(N)$ recursive heap drop |
| `vart::Tree` | 27.55&nbsp;MB<br><sup>(275.5 B/item)</sup> | 27.55&nbsp;MB | 1.31 | $O(N)$ recursive Arc drop |
| `dashmap::DashMap`* | 2.23&nbsp;MB<br><sup>(22.3 B/item)</sup> | 2.24&nbsp;MB | 0.02 | $O(N)$ heap drop |
| `papaya::HashMap`* | 3.96&nbsp;MB<br><sup>(39.6 B/item)</sup> | 3.96&nbsp;MB | 1.00 | $O(N)$ epoch-deferred reclamation |
| `scc::HashIndex`* | 2.40&nbsp;MB<br><sup>(24.0 B/item)</sup> | 3.59&nbsp;MB | 0.01 | $O(N)$ epoch-deferred reclamation |
| `std::collections::HashMap`* | 2.23&nbsp;MB<br><sup>(22.3 B/item)</sup> | 3.34&nbsp;MB | 0 | $O(N)$ heap drop |

<sup>* For sequential keys (e.g. monotonically increasing timestamps or auto-incrementing IDs), radix prefix compression reduces `ArenaArtMap`'s net size to **3.11 MB** (31.1 B/item) and `ArenaVersionedArtMap` to **4.71 MB** (47.1 B/item).</sup>

- **Point reads**: radix path resolution in `ArenaArtMap` completes random point lookups in **13.4 ns**, against **181 ns** for `arenaskiplist` and **160 ns** for `crossbeam-skiplist::SkipMap`.
- **Concurrent writes**: 8 threads insert 100,000 keys into `ArenaArtMap` in **1.76 ms** (56.8M ops/s), against **3.00 ms** for `scc::HashIndex`, **3.37 ms** for `dashmap` and **10.1 ms** for `crossbeam-skiplist::SkipMap`.
- **Range scans**: bitmapped child navigation on `Node48` and `Node256` scans 100 contiguous items in **574 ns** in `ArenaArtMap`, against **809 ns** for `arenaskiplist` and **2.39 µs** for `crossbeam-skiplist::SkipMap`. `scan(range, callback)` skips the per-entry handle.
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

### Sets

`ArtSet<K>` is an ordered set. `insert` adds a key only if it is absent, and `insert`, `remove` and `contains` return a `bool`:

```rust
use artmap::ArtSet;

let set = ArtSet::<String>::new();
assert!(set.insert("user:2".to_string()));
assert!(set.insert("user:1".to_string()));
// A key that is already present is left alone.
assert!(!set.insert("user:1".to_string()));

assert!(set.contains("user:1"));
assert!(set.remove("user:2"));
assert!(!set.remove("user:2"));

// Keys come back in order, and ranges work as they do for the maps.
set.insert("user:3".to_string());
let keys: Vec<String> = set.range("user:1"..).map(|k| k.clone()).collect();
assert_eq!(keys, ["user:1", "user:3"]);
```

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

This project is licensed under the [Apache License, Version 2.0](https://github.com/surrealdb/artmap/blob/main/LICENSE).
