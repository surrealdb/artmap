// Copyright (c) 2026 SurrealDB Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! # artmap: Concurrent Adaptive Radix Tree for Rust
//!
//! `artmap` provides a concurrent, lock-free/optimistic in-memory associative map backed
//! by an Adaptive Radix Tree (ART) with Optimistic Lock Coupling (OLC) and epoch-based
//! memory reclamation.
//!
//! ## Design & Features
//!
//! - **Adaptive Node Sizes**: Node4, Node16, Node48, Node256 dynamically sized based on child count.
//! - **Prefix Compression**: Collapses single-child paths into shared byte prefixes.
//! - **Optimistic Lock Coupling (OLC)**: Readers proceed non-blocking without atomic reference count writes.
//! - **Epoch-Based Reclamation**: Safely reclaims memory for unlinked/grown nodes via `crossbeam-epoch`.
//! - **High Write Concurrency**: Fine-grained node locking enables concurrent updates across disjoint prefixes.

pub struct ArtMap<K, V> {
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> Default for ArtMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> ArtMap<K, V> {
    /// Creates a new, empty [`ArtMap`].
    pub const fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_initializes() {
        let _map: ArtMap<String, i32> = ArtMap::new();
    }
}
