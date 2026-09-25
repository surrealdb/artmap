# artmap: Concurrent Adaptive Radix Tree for Rust

`artmap` is a high-performance, concurrent in-memory associative map backed by an **Adaptive Radix Tree (ART)**, featuring **Optimistic Lock Coupling (OLC)** and epoch-based memory reclamation.

[![Status](https://img.shields.io/badge/status-pre--alpha-ff00bb.svg?style=flat-square)](#)
[![License](https://img.shields.io/badge/license-Apache_License_2.0-00bfff.svg?style=flat-square)](LICENSE)

---

## Overview

Traditional concurrent map implementations (such as SkipLists or B-Trees) incur significant comparison and pointer-hopping overheads on large string and byte-slice keys:
- SkipLists require $O(\log N)$ steps, performing full `memcmp` comparisons at every level.
- B-Trees suffer from cache-line overhead and branch mispredictions when comparing variable-length keys.

`artmap` implements an **Adaptive Radix Tree** designed for multi-core scalability:
- **$O(k)$ Lookup Time**: Key lookup complexity depends only on the key length $k$ in bytes, not on the number of keys $N$ in the tree.
- **Adaptive Node Layouts**: Nodes dynamically adapt between four distinct sizes (`Node4`, `Node16`, `Node48`, and `Node256`) to maximize CPU cache locality and space efficiency.
- **Prefix Compression**: Common key prefixes are compressed into compact byte vectors, dramatically reducing memory consumption for structured database keys.
- **Optimistic Lock Coupling (OLC)**: Readers traverse the tree optimistically without taking locks or issuing atomic write instructions, eliminating cache-line bouncing.
- **Epoch-Based Reclamation (EBR)**: Replaced and deleted nodes are retired safely via `crossbeam-epoch` without the overhead of reference counting on traversal.

---

## Features

- **Multi-Writer Scalability**: Parallel writes into disjoint key prefixes proceed concurrently with fine-grained node locks.
- **Non-Blocking Readers**: Readers validate version counters optimistically, operating with zero atomic writes.
- **Range Scans**: In-order forward and backward range iteration.
- **Thread Safety**: `Send + Sync` when key and value types are thread-safe.

---

## Quick Start

```rust
use artmap::ArtMap;

fn main() {
    let map = ArtMap::new();

    // Map operations will be available here
}
```

---

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
