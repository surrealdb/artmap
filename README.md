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

| Data Structure | Point&nbsp;Read (Random&nbsp;Hit) | Point&nbsp;Insert | Range&nbsp;Scan (100&nbsp;items) | Allocations /&nbsp;Insert | Drop /&nbsp;Reset |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **`artmap::ArenaArtMap`**<br><sup>&nbsp;(Slice Lookup)</sup> | **14.8&nbsp;ns**<br><sup>(67.5M/s)</sup> | — | — | **0&nbsp;allocs** | **$O(1)$** |
| **`artmap::ArenaArtMap`**<br><sup>&nbsp;(Standard Key)</sup> | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**14.7&nbsp;ns**<br><sup>(68.1M/s)</sup> | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**22.6&nbsp;ns**<br><sup>(44.1M/s)</sup> | **586&nbsp;ns**<br><sup>(170.5M/s)</sup> | **0&nbsp;allocs** | **$O(1)$** |
| **`artmap::ArtMap`**<br><sup>&nbsp;(Slice Lookup)</sup> | **19.3&nbsp;ns**<br><sup>(51.9M/s)</sup> | — | — | **0&nbsp;allocs** | $O(N)$ epoch |
| **`artmap::ArtMap`**<br><sup>&nbsp;(Standard Key)</sup> | **15.0&nbsp;ns**<br><sup>(66.5M/s)</sup> | **27.1&nbsp;ns**<br><sup>(36.9M/s)</sup> | **578&nbsp;ns**<br><sup>(172.8M/s)</sup> | **1.0&nbsp;allocs** | $O(N)$ epoch |
| `arenaskiplist::SkipList` | 177.1&nbsp;ns<br><sup>(5.6M/s)</sup> | 107.1&nbsp;ns<br><sup>(9.3M/s)</sup> | 802&nbsp;ns<br><sup>(124.6M/s)</sup> | **0&nbsp;allocs** | **$O(1)$** |
| `crossbeam_skiplist::SkipMap` | 141.5&nbsp;ns<br><sup>(7.1M/s)</sup> | 79.1&nbsp;ns<br><sup>(12.6M/s)</sup> | 2.20&nbsp;µs<br><sup>(45.5M/s)</sup> | ~1.0&nbsp;allocs | $O(N)$ epoch |
| `imbl::OrdMap` | 40.8&nbsp;ns<br><sup>(24.5M/s)</sup> | 51.2&nbsp;ns<br><sup>(19.5M/s)</sup> | 336&nbsp;ns<br><sup>(298M/s)</sup> | ~0.14&nbsp;allocs | $O(N)$ heap |
| `std::collections::BTreeMap` | 58.2&nbsp;ns<br><sup>(17.2M/s)</sup> | 31.6&nbsp;ns<br><sup>(31.6M/s)</sup> | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**188&nbsp;ns**<br><sup>(530M/s)</sup> | ~0.17&nbsp;allocs | $O(N)$ heap |
| `std::collections::HashMap`* | 13.2&nbsp;ns<br><sup>(75.5M/s)</sup> | 18.6&nbsp;ns<br><sup>(53.6M/s)</sup> | N/A | ~0&nbsp;allocs | $O(N)$ heap |

<sup>* `std::collections::HashMap` is included as an unordered $O(1)$ reference baseline and does not support range queries, sorted scans, or concurrent multi-writer scaling. The rocket icon denotes the fastest implementation among ordered, concurrent range-scannable maps.</sup>

### Multi-Threaded Concurrent Performance

When running multi-threaded workloads with concurrent writers, non-concurrent data structures (`BTreeMap`, `HashMap`, `imbl::OrdMap`) require synchronization via `parking_lot::RwLock`. Under write contention, exclusive lock acquisition serializes all threads, causing severe lock convoying and throughput collapse.

Benchmarked on bare metal (**AMD Ryzen Threadripper 9970X 32-Core / 64-Thread Processor @ 5.48 GHz, 128 GB DDR5 RAM**, Linux 6.8):

| Data Structure | Concurrent&nbsp;Writes<br><sup>(8&nbsp;Threads,&nbsp;100k&nbsp;Ops)</sup> | Mixed&nbsp;Workload<br><sup>(4R&nbsp;+&nbsp;4W,&nbsp;100k&nbsp;Ops)</sup> | Concurrency&nbsp;Model |
| :--- | ---: | ---: | :--- |
| **`artmap::ArtMap`** | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**5.96&nbsp;ms**<br><sup>(16.8M/s)</sup> | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**4.69&nbsp;ms**<br><sup>(21.3M/s)</sup> | Non-Blocking Reads + OLC Writes |
| **`artmap::ArenaArtMap`** | **16.96&nbsp;ms**<br><sup>(5.89M/s)</sup> | **8.90&nbsp;ms**<br><sup>(11.2M/s)</sup> | 32-Bit Offsets + Non-Blocking Reads + OLC Writes |
| `crossbeam_skiplist::SkipMap` | 10.30&nbsp;ms<br><sup>(9.71M/s)</sup> | 9.58&nbsp;ms<br><sup>(10.4M/s)</sup> | Lock-Free Atomic CAS |
| `arenaskiplist::SkipList` | 40.10&nbsp;ms<br><sup>(2.49M/s)</sup> | 28.07&nbsp;ms<br><sup>(3.56M/s)</sup> | Lock-Free Atomic CAS (Contiguous Arena) |
| `parking_lot::RwLock<BTreeMap>` | 73.25&nbsp;ms<br><sup>(1.36M/s)</sup> | 36.88&nbsp;ms<br><sup>(2.71M/s)</sup> | Coarse Exclusive Lock |
| `parking_lot::RwLock<HashMap>`* | 77.15&nbsp;ms<br><sup>(1.30M/s)</sup> | 44.58&nbsp;ms<br><sup>(2.24M/s)</sup> | Coarse Exclusive Lock |
| `parking_lot::RwLock<imbl::OrdMap>` | 84.15&nbsp;ms<br><sup>(1.19M/s)</sup> | 56.24&nbsp;ms<br><sup>(1.78M/s)</sup> | Coarse Exclusive Lock |

- **Arena-Backed Zero-Allocation Dominance**: `ArenaArtMap` delivers **14.7 ns point reads** and **22.6 ns inserts** with **0 heap allocations per insert** and instant **$O(1)$ arena teardown/reset**, outperforming `arenaskiplist` by **12× on reads** (14.7 ns vs. 177.1 ns), **4.7× on inserts** (22.6 ns vs. 107.1 ns), **27% on range scans** (586 ns vs. 802 ns), and **3.15× on concurrent mixed workloads** (8.90 ms vs. 28.07 ms).
- **Outperforming SkipMap on Writes & Mixed Loads**: `artmap` is **1.73× faster on concurrent writes** (5.96 ms vs. 10.30 ms) and **2.04× faster on mixed read/write workloads** (4.69 ms vs. 9.58 ms) by eliminating parent-node contention and leveraging fine-grained optimistic lock coupling.
- **9.6× to 12× Faster Point Reads**: Radix-based navigation resolves random point lookups in **14.7 ns – 15.0 ns** (66–68M ops/sec), compared to **141.5 ns** for `crossbeam-skiplist::SkipMap` and **58.2 ns** for standard `BTreeMap`.
- **3.75× Faster Range Scans**: Contiguous 100-item scans complete in **578–586 ns** (170–173M items/sec) with zero heap allocations during iteration via cached cursor-stack descent, compared to **2.20 µs** for `crossbeam-skiplist::SkipMap`.
- **Coarse Lock Bottleneck**: Non-concurrent collections (`RwLock<BTreeMap>`, `RwLock<HashMap>`, `RwLock<imbl::OrdMap>`) run **4.1× to 12× slower** on mixed workloads and up to **14× slower on concurrent writes** because exclusive write acquisitions serialize all threads.
- **True Multi-Writer Scaling**: Writers acquire fine-grained node locks only at the specific leaf or inner node being modified, allowing concurrent updates across disjoint prefixes to proceed in parallel.
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
