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

`artmap` is a high-performance, concurrent in-memory associative map backed by an **Adaptive Radix Tree (ART)**, featuring **Optimistic Lock Coupling (OLC)** and **Epoch-Based Memory Reclamation (EBR)**.

It combines the $O(k)$ key-length lookup time and prefix compression of adaptive radix trees with the multi-core write scalability of fine-grained node latching and non-blocking optimistic readers. It is designed as a drop-in replacement for concurrent skip lists (`crossbeam-skiplist::SkipMap`) and concurrent B-trees in high-throughput transactional database engines and in-memory stores.

## Performance

Benchmarked on bare metal (**AMD Ryzen Threadripper 9970X 32-Core / 64-Thread Processor @ 5.48 GHz, 128 GB DDR5 RAM**, Linux 6.8):

| Data Structure | Point&nbsp;Read (Random&nbsp;Hit) | Point&nbsp;Insert | Range&nbsp;Scan (100&nbsp;items) | Allocations /&nbsp;Insert |
| :--- | ---: | ---: | ---: | ---: |
| **`artmap::ArtMap`**<br><sup>&nbsp;(Slice Lookup)</sup> | **19.4&nbsp;ns**<br><sup>(51.5M/s)</sup> | — | — | **0&nbsp;allocs** |
| **`artmap::ArtMap`**<br><sup>&nbsp;(Standard Key)</sup> | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**15.0&nbsp;ns**<br><sup>(66.5M/s)</sup> | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**30.2&nbsp;ns**<br><sup>(33.1M/s)</sup> | **1.56&nbsp;µs**<br><sup>(63.8M/s)</sup> | **1.0&nbsp;allocs** |
| `crossbeam_skiplist::SkipMap` | 143.8&nbsp;ns<br><sup>(7.0M/s)</sup> | 98.0&nbsp;ns<br><sup>(10.2M/s)</sup> | 2.17&nbsp;µs<br><sup>(46.0M/s)</sup> | ~1.0&nbsp;allocs |
| `imbl::OrdMap` | 40.4&nbsp;ns<br><sup>(24.8M/s)</sup> | 71.9&nbsp;ns<br><sup>(13.9M/s)</sup> | 326&nbsp;ns<br><sup>(307M/s)</sup> | ~0.14&nbsp;allocs |
| `std::collections::BTreeMap` | 58.6&nbsp;ns<br><sup>(17.1M/s)</sup> | 37.1&nbsp;ns<br><sup>(27.0M/s)</sup> | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**189&nbsp;ns**<br><sup>(529M/s)</sup> | ~0.16&nbsp;allocs |
| `std::collections::HashMap`* | 13.1&nbsp;ns<br><sup>(76.1M/s)</sup> | 29.0&nbsp;ns<br><sup>(34.5M/s)</sup> | N/A | ~0&nbsp;allocs |

<sup>* `std::collections::HashMap` is included as an unordered $O(1)$ reference baseline and does not support range queries, sorted scans, or concurrent multi-writer scaling. The rocket icon denotes the fastest implementation among ordered, concurrent range-scannable maps.</sup>

### Multi-Threaded Concurrent Performance

When running multi-threaded workloads with concurrent writers, non-concurrent data structures (`BTreeMap`, `HashMap`, `imbl::OrdMap`) require synchronization via `parking_lot::RwLock`. Under write contention, exclusive lock acquisition serializes all threads, causing severe lock convoying and throughput collapse.

Benchmarked on bare metal (**AMD Ryzen Threadripper 9970X 32-Core / 64-Thread Processor @ 5.48 GHz, 128 GB DDR5 RAM**, Linux 6.8):

| Data Structure | Concurrent&nbsp;Writes<br><sup>(8&nbsp;Threads,&nbsp;100k&nbsp;Ops)</sup> | Mixed&nbsp;Workload<br><sup>(4R&nbsp;+&nbsp;4W,&nbsp;100k&nbsp;Ops)</sup> | Concurrency&nbsp;Model |
| :--- | ---: | ---: | :--- |
| **`artmap::ArtMap`** | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**5.73&nbsp;ms**<br><sup>(17.4M/s)</sup> | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**5.25&nbsp;ms**<br><sup>(19.1M/s)</sup> | Non-Blocking Reads + OLC Writes |
| `crossbeam_skiplist::SkipMap` | 10.12&nbsp;ms<br><sup>(9.88M/s)</sup> | 9.65&nbsp;ms<br><sup>(10.4M/s)</sup> | Lock-Free Atomic CAS |
| `parking_lot::RwLock<BTreeMap>` | 71.0&nbsp;ms<br><sup>(1.41M/s)</sup> | 38.0&nbsp;ms<br><sup>(2.63M/s)</sup> | Coarse Exclusive Lock |
| `parking_lot::RwLock<HashMap>`* | 90.2&nbsp;ms<br><sup>(1.11M/s)</sup> | 45.6&nbsp;ms<br><sup>(2.19M/s)</sup> | Coarse Exclusive Lock |
| `parking_lot::RwLock<imbl::OrdMap>` | 81.0&nbsp;ms<br><sup>(1.24M/s)</sup> | 57.1&nbsp;ms<br><sup>(1.75M/s)</sup> | Coarse Exclusive Lock |

- **Outperforming SkipMap on Writes & Mixed Loads**: `artmap` is **1.77× faster on concurrent writes** (5.73 ms vs. 10.12 ms) and **1.84× faster on mixed read/write workloads** (5.25 ms vs. 9.65 ms) by eliminating parent-node contention and leveraging lock-free atomic CAS for wide nodes.
- **9.6× Faster Point Reads**: `artmap` resolves random point lookups in **15.0 ns** (66.5M ops/sec), compared to **143.8 ns** for `crossbeam-skiplist::SkipMap` and **58.6 ns** for standard `BTreeMap`.
- **1.39× Faster Concurrent Range Scans**: `artmap` traverses 100 contiguous items in **1.56 µs** (63.8M items/sec) with zero heap allocations during iteration, outperforming `crossbeam-skiplist::SkipMap` (2.17 µs).
- **Coarse Lock Bottleneck**: Non-concurrent collections (`RwLock<BTreeMap>`, `RwLock<HashMap>`, `RwLock<imbl::OrdMap>`) run **7.2× to 10.9× slower** on mixed workloads and up to **15.7× slower on concurrent writes** because exclusive write acquisitions serialize all threads.
- **True Multi-Writer Scaling**: Writers in `artmap` acquire fine-grained node locks only at the specific leaf or inner node being modified, allowing concurrent updates across disjoint prefixes to proceed in parallel.
- **Epoch-Based Memory Safety**: Replaced or unlinked nodes are retired safely via `crossbeam-epoch` without reference-counting overhead on read traversal.

## Features

- **Adaptive Radix Tree Architecture**: Dynamically resizes inner nodes across 4 compact layouts (`Node4` $\leftrightarrow$ `Node16` $\leftrightarrow$ `Node48` $\leftrightarrow$ `Node256`) to maximize CPU L1/L2 cache locality.
- **SIMD-Accelerated Lookups**: Vectorized child key comparisons on `Node16` using SSE2 on x86_64 and NEON on ARM64.
- **Prefix Compression**: Collapses single-child paths into shared byte prefixes, dramatically reducing memory usage for structured database keys.
- **Optimistic Lock Coupling (OLC / ROWEX)**: Readers validate version counters optimistically, operating with zero locks and zero atomic writes.
- **Zero-Allocation Slice Queries**: Query entries directly with raw byte slices (`&[u8]`) or string slices (`&str`) without allocating wrapper objects.
- **Bidirectional Range Iterators**: Full `DoubleEndedIterator` support for ordered forward and reverse scans (`map.range(A..B)` and `map.range(A..B).rev()`).
- **Thread Safety Guaranteed**: Compile-time static assertions ensure `ArtMap<K, V>` implements `Send + Sync` when `K: Send + Sync` and `V: Send + Sync`.
- **Deterministic Simulation Tested (DST)**: Validated continuously by a seeded PRNG fuzzer against an in-memory `BTreeMap` reference oracle with comprehensive structural invariant checking.

## Quick Start

Add `artmap` to your `Cargo.toml`:

```toml
[dependencies]
artmap = "0.1"
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

Benchmarks compare `artmap` against `crossbeam-skiplist::SkipMap`, `std::collections::BTreeMap`, `imbl::OrdMap`, and `std::collections::HashMap`:

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
