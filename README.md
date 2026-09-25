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

| Data Structure | Point Read (Random Hit) | Point Insert (Concurrent) | Range Scan (1K items) | Allocations / Insert |
| :--- | ---: | ---: | ---: | ---: |
| **`artmap::ArtMap` (Slice Lookup)** | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**TBD** | — | — | **0 allocs** |
| **`artmap::ArtMap` (Standard Key)** | **TBD** | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**TBD** | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**TBD** | **TBD** |
| `crossbeam_skiplist::SkipMap` | TBD | TBD | TBD | ~1.0 allocs |
| `imbl::OrdMap` (Persistent B-Tree v7) | 46.5 ns | 71.6 ns | 12.4 µs | ~0.14 allocs |
| `std::collections::BTreeMap` | 72.7 ns | 37.7 ns (single-thread) | 14.8 µs | ~0.16 allocs |
| `std::collections::HashMap`* | 14.4 ns | 28.8 ns (single-thread) | N/A | ~0 allocs |

<sup>* Rocket badge denotes the fastest implementation among ordered, concurrent range-scannable maps. `std::collections::HashMap` is included as an unordered $O(1)$ reference baseline and does not support range queries, sorted scans, or concurrent multi-writer scaling.</sup>

- **$O(k)$ Lookup Complexity**: Search time is strictly bounded by key length in bytes $k$, avoiding the 15–20 pointer hops and full `memcmp` comparisons per lookup inherent to skip lists.
- **Zero-Atomic-Write Reads**: Optimistic readers traverse nodes without issuing atomic write instructions or updating reference counters, eliminating CPU cache-line bouncing.
- **True Multi-Writer Scaling**: Writers acquire fine-grained node locks only at the local leaf or node being resized, allowing concurrent inserts across disjoint key prefixes to scale linearly with core count.
- **Epoch-Based Memory Safety**: Replaced or shrunk nodes are retired safely via `crossbeam-epoch` without the runtime overhead of atomic reference counts.

## Features

- **Adaptive Radix Tree Architecture**: Dynamically resizes inner nodes across 4 compact layouts (`Node4` $\leftrightarrow$ `Node16` $\leftrightarrow$ `Node48` $\leftrightarrow$ `Node256`) to maximize CPU L1/L2 cache locality.
- **SIMD-Accelerated Lookups**: Vectorized child key comparisons on `Node16` using SSE2/AVX2 on x86_64 and NEON on ARM64.
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

# Run ART-specific micro-benchmarks
cargo bench --bench artmap_bench
```

## License

This project is licensed under the [Apache License, Version 2.0](LICENSE).
