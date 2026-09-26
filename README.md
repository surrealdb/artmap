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

`artmap` is a high-performance, concurrent in-memory associative map backed by an **Adaptive Radix Tree (ART)**, featuring **Optimistic Lock Coupling (OLC)**, **Epoch-Based Memory Reclamation (EBR)**, and an arena-backed **32-bit offset variant (`ArenaArtMap`)** with $O(1)$ teardown.

It provides two concurrent map variants:
- **`artmap::ArtMap`**: An EBR-backed concurrent map with fine-grained per-node latching and deferred memory reclamation via `crossbeam-epoch`.
- **`artmap::ArenaArtMap`**: An arena-backed concurrent map using compact **32-bit offsets** instead of 64-bit raw pointers, featuring **0 per-insert heap allocations**, **$O(1)$ zero-cost arena reset/teardown**, and built-in **64-bit MVCC versioning** (`insert_versioned`, `get_version_le`) for transactional database memtables and LSM-tree engines.

## Performance

Benchmarked on bare metal (**AMD Ryzen Threadripper 9970X 32-Core / 64-Thread Processor @ 5.48 GHz, 128 GB DDR5 RAM**, Linux 6.8):

| Data Structure | Read<br><sup>(standard&nbsp;key)</sup> | Read<br><sup>(slice&nbsp;key)</sup> | Insert<br><sup>(sequential)</sup> | Insert<br><sup>(random)</sup> | Range&nbsp;scans<br><sup>(100&nbsp;items)</sup> | Allocations<br><sup>(per&nbsp;insert)</sup> |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| **`artmap::ArenaArtMap`**<br><sup>&nbsp;(with Inserter)</sup> | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**14.5&nbsp;ns**<br><sup>(69.0M/s)</sup> | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**14.8&nbsp;ns**<br><sup>(67.7M/s)</sup> | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**25.3&nbsp;ns**<br><sup>(39.5M/s)</sup> | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**33.9&nbsp;ns**<br><sup>(29.5M/s)</sup> | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**570&nbsp;ns**<br><sup>(175.5M/s)</sup> | **0** |
| **`artmap::ArenaArtMap`** | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**14.5&nbsp;ns**<br><sup>(69.0M/s)</sup> | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**14.8&nbsp;ns**<br><sup>(67.7M/s)</sup> | **27.2&nbsp;ns**<br><sup>(36.8M/s)</sup> | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**33.9&nbsp;ns**<br><sup>(29.5M/s)</sup> | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**570&nbsp;ns**<br><sup>(175.5M/s)</sup> | **0** |
| **`artmap::ArtMap`** | **15.0&nbsp;ns**<br><sup>(66.6M/s)</sup> | **19.6&nbsp;ns**<br><sup>(50.8M/s)</sup> | **26.1&nbsp;ns**<br><sup>(38.2M/s)</sup> | **52.5&nbsp;ns**<br><sup>(19.0M/s)</sup> | **657&nbsp;ns**<br><sup>(152.3M/s)</sup> | **1.0** |
| `arenaskiplist::SkipList`<br><sup>&nbsp;(with Inserter)</sup> | 171.9&nbsp;ns<br><sup>(5.8M/s)</sup> | 171.9&nbsp;ns<br><sup>(5.8M/s)</sup> | 39.4&nbsp;ns<br><sup>(25.4M/s)</sup> | 156.4&nbsp;ns<br><sup>(6.4M/s)</sup> | 793&nbsp;ns<br><sup>(126.2M/s)</sup> | **0** |
| `arenaskiplist::SkipList` | 171.9&nbsp;ns<br><sup>(5.8M/s)</sup> | 171.9&nbsp;ns<br><sup>(5.8M/s)</sup> | 106.8&nbsp;ns<br><sup>(9.4M/s)</sup> | 156.4&nbsp;ns<br><sup>(6.4M/s)</sup> | 793&nbsp;ns<br><sup>(126.2M/s)</sup> | **0** |
| `crossbeam_skiplist::SkipMap` | 144.7&nbsp;ns<br><sup>(6.9M/s)</sup> | — | 75.3&nbsp;ns<br><sup>(13.3M/s)</sup> | 176.5&nbsp;ns<br><sup>(5.7M/s)</sup> | 2.19&nbsp;µs<br><sup>(45.7M/s)</sup> | ~1.0 |
| `imbl::OrdMap` | 41.5&nbsp;ns<br><sup>(24.1M/s)</sup> | — | 52.8&nbsp;ns<br><sup>(19.0M/s)</sup> | 74.2&nbsp;ns<br><sup>(13.5M/s)</sup> | 341&nbsp;ns<br><sup>(293M/s)</sup> | ~0.14 |
| `std::collections::BTreeMap` | 58.6&nbsp;ns<br><sup>(17.1M/s)</sup> | — | 31.5&nbsp;ns<br><sup>(31.7M/s)</sup> | 64.7&nbsp;ns<br><sup>(15.5M/s)</sup> | **183&nbsp;ns**<br><sup>(546M/s)</sup> | ~0.17 |
| `std::collections::HashMap`* | 13.1&nbsp;ns<br><sup>(76.1M/s)</sup> | — | 18.7&nbsp;ns<br><sup>(53.5M/s)</sup> | 21.1&nbsp;ns<br><sup>(47.4M/s)</sup> | N/A | ~0 |

<sup>* `std::collections::HashMap` is included as an unordered $O(1)$ reference baseline and does not support range queries, sorted scans, or concurrent multi-writer scaling. The rocket icon denotes the fastest implementation among ordered, concurrent range-scannable maps.</sup>

### Multi-Threaded Concurrent Performance

When running multi-threaded workloads with concurrent writers, non-concurrent data structures (`BTreeMap`, `HashMap`, `imbl::OrdMap`) require synchronization via `parking_lot::RwLock`. Under write contention, exclusive lock acquisition serializes all threads, causing severe lock convoying and throughput collapse.

Benchmarked on bare metal (**AMD Ryzen Threadripper 9970X 32-Core / 64-Thread Processor @ 5.48 GHz, 128 GB DDR5 RAM**, Linux 6.8):

| Data Structure | Concurrent&nbsp;Writes<br><sup>(8&nbsp;Threads,&nbsp;100k&nbsp;Ops)</sup> | Mixed&nbsp;Workload<br><sup>(4R&nbsp;+&nbsp;4W,&nbsp;100k&nbsp;Ops)</sup> | Concurrency&nbsp;Model |
| :--- | ---: | ---: | :--- |
| **`artmap::ArenaArtMap`** | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**5.02&nbsp;ms**<br><sup>(19.9M/s)</sup> | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**4.29&nbsp;ms**<br><sup>(23.3M/s)</sup> | Lock-Free CAS + 32-Bit Offsets + Thread-Local Bump |
| **`artmap::ArtMap`** | **5.93&nbsp;ms**<br><sup>(16.9M/s)</sup> | **4.93&nbsp;ms**<br><sup>(20.3M/s)</sup> | Non-Blocking Reads + OLC Node Latching |
| `crossbeam_skiplist::SkipMap` | 10.31&nbsp;ms<br><sup>(9.70M/s)</sup> | 9.69&nbsp;ms<br><sup>(10.3M/s)</sup> | Lock-Free Atomic CAS |
| `arenaskiplist::SkipList` | 40.50&nbsp;ms<br><sup>(2.47M/s)</sup> | 28.28&nbsp;ms<br><sup>(3.54M/s)</sup> | Lock-Free Atomic CAS (Contiguous Arena) |
| `parking_lot::RwLock<BTreeMap>` | 68.19&nbsp;ms<br><sup>(1.47M/s)</sup> | 38.05&nbsp;ms<br><sup>(2.63M/s)</sup> | Coarse Exclusive Lock |
| `parking_lot::RwLock<HashMap>`* | 76.64&nbsp;ms<br><sup>(1.30M/s)</sup> | 43.17&nbsp;ms<br><sup>(2.32M/s)</sup> | Coarse Exclusive Lock |
| `parking_lot::RwLock<imbl::OrdMap>` | 76.39&nbsp;ms<br><sup>(1.31M/s)</sup> | 54.98&nbsp;ms<br><sup>(1.82M/s)</sup> | Coarse Exclusive Lock |

- **Concurrent Write Leadership**: Through lock-free atomic `Node256` CAS and thread-local 64 KB chunked bump allocation, `ArenaArtMap` executes 100,000 multi-threaded writes in **5.02 ms** (19.9M ops/sec), outperforming `crossbeam-skiplist::SkipMap` by **2.05×** (10.31 ms) and `arenaskiplist` by **8.1×** (40.50 ms).
- **11.9× Faster Point Reads**: Radix-based path resolution in `ArenaArtMap` completes random point lookups in **14.5 ns** (69.0M ops/sec), compared to **171.9 ns** for `arenaskiplist` and **144.7 ns** for `crossbeam-skiplist::SkipMap`.
- **3.84× Faster Range Scans via Bitmapped Traversal**: Bitmapped child acceleration (`TZCNT` / 1-cycle bit-scans on `Node48` and `Node256`) and zero-copy cursors scan 100 contiguous items in **570 ns** (175.5M items/sec), compared to **793 ns** for `arenaskiplist` and **2.19 µs** for `crossbeam-skiplist::SkipMap`.
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
- **Thread Safety Guaranteed**: Compile-time static assertions ensure both `ArtMap<K, V>` and `ArenaArtMap<K, V>` implement `Send + Sync`.
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

### Arena-Backed ART (`ArenaArtMap`)

For transactional storage engines, database memtables, or workloads requiring instant $O(1)$ teardown without per-node garbage collection:

```rust
use artmap::arena::ArenaArtMap;

// Allocate with a 16 MB pre-allocated arena buffer
let map = ArenaArtMap::<String, i32>::with_capacity(16 * 1024 * 1024);

// Inserts allocate zero heap memory outside the arena buffer
map.insert("account:1001".to_string(), 500);

// Multi-version concurrency control (MVCC) support
map.insert_versioned("account:1001".to_string(), 1, 500);
map.insert_versioned("account:1001".to_string(), 2, 750);

// Point read with version <= 1 returns 500
assert_eq!(map.get_version_le("account:1001", 1), Some((1, 500)));

// Point read with version <= 2 returns 750
assert_eq!(map.get_version_le("account:1001", 2), Some((2, 750)));

// Bidirectional range scan with entry metadata
for entry in map.range("account:1000".."account:2000") {
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

Benchmarks compare `artmap` and `artmap::ArenaArtMap` against `arenaskiplist::SkipList`, `crossbeam-skiplist::SkipMap`, `std::collections::BTreeMap`, `imbl::OrdMap`, and `std::collections::HashMap`:

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
