<h1 align="center">artmap</h1>

<p align="center">A concurrent, in-memory Adaptive Radix Tree (ART) data structure for Rust.</p>

<br>

<p align="center">
    <a href="https://github.com/surrealdb/artmap"><img src="https://img.shields.io/badge/status-pre--alpha-ff00bb.svg?style=flat-square"></a>
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

| Data Structure | Point Read (Random Hit) | Point Insert | Range Scan (100 items) | Allocations / Insert |
| :--- | ---: | ---: | ---: | ---: |
| **`artmap::ArtMap` (Slice Lookup)** | <nobr><img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**20.8&nbsp;ns**</nobr><br><sup>(48.0M/s)</sup> | — | — | <nobr>**0&nbsp;allocs**</nobr> |
| **`artmap::ArtMap` (Standard Key)** | <nobr>**20.8&nbsp;ns**</nobr><br><sup>(48.0M/s)</sup> | <nobr><img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**36.1&nbsp;ns**</nobr><br><sup>(27.7M/s)</sup> | <nobr>**2.25&nbsp;µs**</nobr><br><sup>(44.3M/s)</sup> | <nobr>**1.0&nbsp;allocs**</nobr> |
| `crossbeam_skiplist::SkipMap` | <nobr>147.2&nbsp;ns</nobr><br><sup>(6.8M/s)</sup> | <nobr>96.9&nbsp;ns</nobr><br><sup>(10.3M/s)</sup> | <nobr>2.15&nbsp;µs</nobr><br><sup>(46.5M/s)</sup> | <nobr>~1.0&nbsp;allocs</nobr> |
| `imbl::OrdMap` (Persistent B-Tree v7) | <nobr>40.4&nbsp;ns</nobr><br><sup>(24.7M/s)</sup> | <nobr>72.1&nbsp;ns</nobr><br><sup>(13.9M/s)</sup> | <nobr>320&nbsp;ns</nobr><br><sup>(312M/s)</sup> | <nobr>~0.14&nbsp;allocs</nobr> |
| `std::collections::BTreeMap` | <nobr>60.1&nbsp;ns</nobr><br><sup>(16.6M/s)</sup> | <nobr>38.1&nbsp;ns</nobr><br><sup>(26.2M/s)</sup> | <nobr>183&nbsp;ns</nobr><br><sup>(544M/s)</sup> | <nobr>~0.16&nbsp;allocs</nobr> |
| `std::collections::HashMap`* | <nobr>13.4&nbsp;ns</nobr><br><sup>(74.7M/s)</sup> | <nobr>29.0&nbsp;ns</nobr><br><sup>(34.4M/s)</sup> | N/A | <nobr>~0&nbsp;allocs</nobr> |

<sup>* Rocket badge denotes the fastest implementation among ordered, concurrent range-scannable maps. `std::collections::HashMap` is included as an unordered $O(1)$ reference baseline and does not support range queries, sorted scans, or concurrent multi-writer scaling.</sup>

- **7.1× Faster Point Lookups**: `artmap` resolves random point lookups in **20.8 ns** (48.0M ops/sec), compared to **147.2 ns** for `crossbeam-skiplist::SkipMap` and **60.1 ns** for standard `BTreeMap`.
- **2.7× Faster Ingestion**: Point inserts complete in **36.1 ns** (27.7M ops/sec) vs. **96.9 ns** for `crossbeam-skiplist::SkipMap`.
- **Zero-Atomic-Write Reads**: Optimistic readers traverse nodes without issuing atomic write instructions or updating reference counters, eliminating CPU cache-line bouncing.
- **True Multi-Writer Scaling**: Writers acquire fine-grained node locks only at the local leaf or node being resized, allowing concurrent inserts across disjoint key prefixes to scale linearly with core count.
- **Epoch-Based Memory Safety**: Replaced or shrunk nodes are retired safely via `crossbeam-epoch` without the runtime overhead of atomic reference counts.

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
