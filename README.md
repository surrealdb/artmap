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

`artmap` is a high-performance, concurrent in-memory associative map backed by an **Adaptive Radix Tree (ART)**, featuring **Optimistic Lock Coupling (OLC)**, **Epoch-Based Memory Reclamation (EBR)**, and arena-backed **32-bit offset variants** with $O(1)$ zero-cost reset.

It provides four concurrent map variants:
- **`artmap::ArtMap`**: An EBR-backed concurrent map with fine-grained per-node latching and deferred memory reclamation via `crossbeam-epoch`.
- **`artmap::VersionedArtMap`**: An EBR-backed concurrent map with built-in 64-bit MVCC version chains and lock-free snapshot reads (`get_version_le`, `get_latest`), designed for transactional memory engines.
- **`artmap::ArenaArtMap`**: An ultra-compact arena-backed concurrent map using 32-bit offsets and 24-byte unversioned leaves, featuring **0 per-insert heap allocations** and **$O(1)$ zero-cost arena reset/teardown**.
- **`artmap::ArenaVersionedArtMap`**: An arena-backed concurrent map using 32-bit offsets with built-in **64-bit MVCC versioning** for transactional database memtables and LSM-tree engines.

### Variant Selection Matrix

| Memory Model | Unversioned (General Purpose) | Versioned / MVCC (Storage Engines) |
| :--- | :--- | :--- |
| **EBR / Dynamic Heap**<br><sup>(reclaimed via `crossbeam-epoch`)</sup> | **`artmap::ArtMap<K, V>`**<br>• Fine-grained optimistic lock coupling (OLC)<br>• Compact 24 B leaves<br>• Dynamic heap growth & node resizing | **`artmap::VersionedArtMap<K, V>`**<br>• Lock-free snapshot reads (`get_version_le`)<br>• Atomic version prepend chains<br>• **SurrealMX**: replaces `RwLock<Versions>` |
| **Arena / 32-Bit Offsets**<br><sup>(contiguous arena, 0-alloc)</sup> | **`artmap::ArenaArtMap<K, V>`**<br>• Ultra-compact 24 B leaves, 32-bit child offsets<br>• 0 heap allocations per insert<br>• $O(1)$ zero-cost arena reset & teardown | **`artmap::ArenaVersionedArtMap<K, V>`**<br>• Compact 32-bit offset version chains<br>• 0-alloc sequential inserter cache<br>• **SurrealKV**: Zero-alloc memtable |

## Performance

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
| **`artmap::VersionedArtMap`** | **4.18&nbsp;ms**<br><sup>(23.9M/s)</sup> | **3.33&nbsp;ms**<br><sup>(30.0M/s)</sup> | Non-Blocking Reads + OLC + Atomic Version Prepend |
| **`artmap::ArenaArtMap`** | **3.34&nbsp;ms**<br><sup>(29.9M/s)</sup> | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**2.50&nbsp;ms**<br><sup>(39.9M/s)</sup> | Lock-Free CAS + 32-Bit Offsets + Direct Atomic Bump |
| **`artmap::ArenaVersionedArtMap`** | **3.34&nbsp;ms**<br><sup>(29.9M/s)</sup> | **2.90&nbsp;ms**<br><sup>(34.4M/s)</sup> | Lock-Free CAS + 32-Bit Offsets + MVCC Prepend |
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
| **`artmap::ArenaArtMap`** | **4.71&nbsp;MB**<br><sup>(47.1 B/item)</sup> | **8.00&nbsp;MB** | **0** | **$O(1)$ zero-cost reset** (`arena.reset()`) |
| **`artmap::ArenaVersionedArtMap`** | **5.52&nbsp;MB**<br><sup>(55.2 B/item)</sup> | **10.00&nbsp;MB** | **0** | **$O(1)$ zero-cost reset** (`arena.reset()`) |
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

- **High Concurrent Write Scaling**: Through lock-free atomic `Node256` CAS and direct atomic bump allocation, `ArenaArtMap` executes 100,000 multi-threaded writes in **3.34 ms** (29.9M ops/sec), outperforming `crossbeam-skiplist::SkipMap` by **3.09×** (10.31 ms) and `arenaskiplist` by **12.1×** (40.50 ms), while mixed read/write workloads achieve **2.50 ms** (39.9M ops/sec).
- **12.5× Faster Point Reads**: Radix-based path resolution in `ArenaArtMap` completes random point lookups in **13.9 ns** (72.1M ops/sec), compared to **174.3 ns** for `arenaskiplist` and **144.7 ns** for `crossbeam-skiplist::SkipMap`.
- **3.63× Faster Range Scans via Bitmapped Traversal**: Bitmapped child acceleration (`TZCNT` / 1-cycle bit-scans on `Node48` and `Node256`) and zero-copy cursors scan 100 contiguous items in **602 ns** (166.0M items/sec), compared to **791 ns** for `arenaskiplist` and **2.19 µs** for `crossbeam-skiplist::SkipMap`.
- **Sequential Inserter Speedup**: When inserting ordered or localized keys, `ArenaInserter` achieves **25.2 ns/item** (39.7M/sec) with zero tree descent.
- **Zero Heap Allocations & Instant Teardown**: `ArenaArtMap` guarantees **0 per-insert heap allocations** and instant **$O(1)$ arena teardown/recycling** via `arena.reset()`.
- **Epoch-Based Memory Safety**: Replaced or unlinked nodes are retired safely via `crossbeam-epoch` without reference-counting overhead on read traversal.

## Features

- **Adaptive Radix Tree Architecture**: Dynamically resizes inner nodes across 4 compact layouts (`Node4` $\leftrightarrow$ `Node16` $\leftrightarrow$ `Node48` $\leftrightarrow$ `Node256`) to maximize CPU L1/L2 cache locality.
- **Arena-Backed Radix Tree (`ArenaArtMap`)**: Uses 32-bit offsets instead of 64-bit pointers, reducing inner node footprint by ~40% and enabling $O(1)$ zero-cost whole-arena teardown and instant recycling via `arena.reset()`.
- **Multi-Version Concurrency Control (MVCC)**: Built-in 64-bit monotonic sequence numbers with atomic version prepend chains (`insert_versioned`, `get_version_le`) for lock-free snapshot reads in LSM engines.
- **SIMD-Accelerated Lookups**: Vectorized child key comparisons on `Node16` using SSE2 on x86_64 and NEON on ARM64.
- **Prefix Compression**: Collapses single-child paths into shared byte prefixes, dramatically reducing memory usage for structured database keys.
- **Optimistic Lock Coupling (OLC / ROWEX)**: Readers validate version counters optimistically, operating with zero locks and zero atomic writes.
- **Zero-Allocation Slice Queries**: Query entries directly with raw byte slices (`&[u8]`) or string slices (`&str`) without allocating wrapper objects.
- **Bidirectional Range Iterators**: Full `DoubleEndedIterator` support for ordered forward and reverse scans (`map.range(A..B)` and `map.range(A..B).rev()`).
- **Thread Safety Guaranteed**: Compile-time static assertions ensure `ArtMap`, `VersionedArtMap`, `ArenaArtMap`, and `ArenaVersionedArtMap` implement `Send + Sync`.
- **Deterministic Simulation Tested (DST)**: Validated continuously by a seeded PRNG fuzzer against an in-memory `BTreeMap` reference oracle with comprehensive structural invariant checking.

## Quick Start

Add `artmap` to your `Cargo.toml`:

```toml
[dependencies]
artmap = "0.5"
```

```rust
use artmap::ArtMap;
use std::sync::Arc;
use std::thread;

fn main() {
    let map = Arc::new(ArtMap::<String, i32>::new());

    // Spawn concurrent writers
    let handles: Vec<_> = (0..4).map(|t| {
        let map = Arc::clone(&map);
        thread::spawn(move || {
            for i in 0..1000 {
                map.insert(format!("users:{:04}", t * 1000 + i), i);
            }
        })
    }).collect();

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(map.len(), 4000);

    // Non-blocking point lookup
    assert_eq!(map.get(&"users:0500".to_string()), Some(500));

    // Zero-allocation lookup by raw byte slice
    assert!(map.contains_key_slice(b"users:0500"));

    // Range scanning
    let start = "users:0100".to_string();
    let end = "users:0200".to_string();
    for (key, val) in map.range(&start..&end) {
        println!("{}: {}", key, val);
    }
}
```

## Core Operations

### Zero-Allocation Slice Lookups

Query the map using raw byte slices without allocating key objects:

```rust
use artmap::ArtMap;

let map = ArtMap::<String, i32>::new();
map.insert("tenant:100:profile".to_string(), 42);

// Check existence and retrieve without creating a String
if map.contains_key_slice(b"tenant:100:profile") {
    let value = map.get_by_slice(b"tenant:100:profile");
    assert_eq!(value, Some(42));
}
```

### Concurrent Write & Read Access

Readers and writers execute concurrently without blocking one another:

```rust
use artmap::ArtMap;
use std::sync::Arc;

let map = Arc::new(ArtMap::<String, i32>::new());
map.insert("key:1".into(), 100);

let reader_map = Arc::clone(&map);
let writer_map = Arc::clone(&map);

// Readers proceed optimistically without locks
let reader = std::thread::spawn(move || {
    reader_map.get(&"key:1".to_string())
});

// Writers lock only the affected node
let writer = std::thread::spawn(move || {
    writer_map.insert("key:2".into(), 200);
});

assert_eq!(reader.join().unwrap(), Some(100));
writer.join().unwrap();
```

### Bidirectional Range Scanning

Iterators traverse keys in sorted lexicographical order:

```rust
let start = "prefix:001".to_string();
let end = "prefix:100".to_string();

// Forward iteration
for (k, v) in map.range(&start..&end) {
    // ...
}

// Reverse iteration
for (k, v) in map.range(&start..&end).rev() {
    // ...
}
```

### Arena-Backed ART (`ArenaArtMap` & `ArenaVersionedArtMap`)

For transactional storage engines, database memtables, or workloads requiring instant $O(1)$ teardown without per-node garbage collection:

```rust
use artmap::arena::{ArenaArtMap, ArenaVersionedArtMap};

// Compact unversioned map: 0 heap allocations, 24-byte leaves
let map = ArenaArtMap::<String, i32>::with_capacity(16 * 1024 * 1024);
map.insert("account:1001".to_string(), 500);
assert_eq!(map.get("account:1001"), Some(500));

// Multi-version (MVCC) map: built-in 64-bit sequence numbers & snapshot reads
let vmap = ArenaVersionedArtMap::<String, i32>::with_capacity(16 * 1024 * 1024);
vmap.insert_versioned("account:1001".to_string(), 1, 500);
vmap.insert_versioned("account:1001".to_string(), 2, 750);

// Point read with version <= 1 returns 500
assert_eq!(vmap.get_version_le("account:1001", 1), Some((1, 500)));

// Point read with version <= 2 returns 750
assert_eq!(vmap.get_version_le("account:1001", 2), Some((2, 750)));

// Bidirectional range scan with entry metadata
for entry in vmap.range("account:1000".."account:2000") {
    println!("{}: {} (v{})", entry.key(), entry.value(), entry.version());
}
```

## Deterministic Simulation Testing (DST)

`artmap` includes a deterministic simulation testing harness inspired by FoundationDB, TigerBeetle, and `vart`:

- **Seeded PRNG**: Every simulation run is parameterized by a 64-bit seed (`ARTMAP_SIM_SEED=<seed>`) to reproduce any failure down to the byte.
- **Reference Oracle**: Runs state transitions in lockstep against a canonical `BTreeMap` reference oracle.
- **Continuous Invariant Checking**: Validates prefix compression, node capacity boundaries, child bitmaps, and latch states after every simulated step via `map.validate_invariants()`.

To run the simulation suite:

```bash
# Run with a random seed
cargo test --test sim -- --nocapture

# Run with an exact reproducible seed
ARTMAP_SIM_SEED=20202 cargo test --test sim -- --nocapture
```

## Benchmarks

Benchmarks compare the four `artmap` data structures (`ArtMap`, `VersionedArtMap`, `ArenaArtMap`, and `ArenaVersionedArtMap`) against `arenaskiplist::SkipList`, `crossbeam-skiplist::SkipMap`, `std::collections::BTreeMap`, `imbl::OrdMap`, and `std::collections::HashMap`:

```bash
# Run comparison benchmarks locally
cargo bench --bench comparison_bench

# Run allocation and memory benchmarks locally
cargo bench --bench alloc_comparison

# Run on remote dedicated hardware (AMD Threadripper)
./scripts/bench-remote.sh --all
```

## License

This project is licensed under the [Apache License, Version 2.0](LICENSE).
