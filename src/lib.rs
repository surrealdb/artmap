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
//! `artmap` provides a concurrent, in-memory associative map backed by an
//! **Adaptive Radix Tree (ART)** with **Optimistic Lock Coupling (OLC)** and
//! **Epoch-Based Memory Reclamation (EBR)**.
//!
//! ## Design & Features
//!
//! - **$O(k)$ Lookup Complexity**: Search time depends strictly on the key length in bytes $k$.
//! - **Adaptive Node Sizes**: Dynamically scales inner node layouts (`Node4` $\leftrightarrow$ `Node16` $\leftrightarrow$ `Node48` $\leftrightarrow$ `Node256`).
//! - **SIMD-Accelerated Lookups**: Vectorized key comparisons in `Node16` via SSE2 on x86_64 and NEON on ARM64.
//! - **Prefix Compression**: Collapses non-branching paths into compact inline byte prefixes.
//! - **Optimistic Lock Coupling (OLC)**: Readers proceed non-blocking without taking locks or issuing atomic writes.
//! - **Epoch-Based Memory Reclamation**: Memory for unlinked or resized nodes is safely reclaimed via `crossbeam-epoch`.
//! - **Multi-Writer Scalability**: Fine-grained node locking permits parallel inserts across disjoint prefixes.

pub mod arena;
pub mod entry;
pub mod iter;
pub mod key;
pub mod latch;
pub mod node;
pub mod simd;
pub mod tree;

use std::borrow::Borrow;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::Ordering;

pub use arena::{Arena, ArenaArtMap};
pub use entry::EntryRef;
pub use iter::{Iter, Keys, Range, Values};
pub use key::AsBytes;
pub use tree::Tree;

/// A concurrent associative map backed by an Adaptive Radix Tree.
pub struct ArtMap<K, V> {
    tree: Tree<K, V>,
}

impl<K, V> Default for ArtMap<K, V> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> ArtMap<K, V> {
    /// Creates a new, empty [`ArtMap`].
    #[inline]
    pub const fn new() -> Self {
        Self { tree: Tree::new() }
    }

    /// Returns the number of entries in the map.
    #[inline]
    pub fn len(&self) -> usize {
        self.tree.len()
    }

    /// Returns `true` if the map contains no entries.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }
}

impl<K: AsBytes + Send + 'static, V: Send + 'static> ArtMap<K, V> {
    /// Returns an entry reference corresponding to the key, if present.
    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<EntryRef<'_, K, V>>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        let guard = &crossbeam_epoch::pin();
        let leaf_ptr = self.tree.get_leaf(key.as_bytes(), guard)?;
        let leaf = unsafe { &*leaf_ptr };
        if leaf.removed.load(Ordering::Acquire) {
            return None;
        }
        Some(EntryRef {
            leaf_ptr,
            tree: &self.tree,
        })
    }

    /// Returns an entry reference corresponding to a raw byte slice key, if present.
    #[inline]
    pub fn get_by_slice(&self, key: &[u8]) -> Option<EntryRef<'_, K, V>> {
        let guard = &crossbeam_epoch::pin();
        let leaf_ptr = self.tree.get_leaf(key, guard)?;
        let leaf = unsafe { &*leaf_ptr };
        if leaf.removed.load(Ordering::Acquire) {
            return None;
        }
        Some(EntryRef {
            leaf_ptr,
            tree: &self.tree,
        })
    }

    /// Returns a copy of the value corresponding to the key, if present.
    #[inline]
    pub fn get_value<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
        V: Clone,
    {
        let guard = &crossbeam_epoch::pin();
        self.tree.get(key.as_bytes(), guard).cloned()
    }

    /// Accesses the value corresponding to the key via a closure without cloning.
    #[inline]
    pub fn with_value<Q, R, F>(&self, key: &Q, f: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
        F: FnOnce(&V) -> R,
    {
        let guard = &crossbeam_epoch::pin();
        self.tree.get(key.as_bytes(), guard).map(f)
    }

    /// Returns `true` if the map contains an entry for the specified key.
    #[inline]
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        let guard = &crossbeam_epoch::pin();
        self.tree.get(key.as_bytes(), guard).is_some()
    }

    /// Returns `true` if the map contains an entry for the raw byte slice key.
    #[inline]
    pub fn contains_key_slice(&self, key: &[u8]) -> bool {
        let guard = &crossbeam_epoch::pin();
        self.tree.get(key, guard).is_some()
    }

    /// Inserts a key-value pair into the map, returning the previous value if present.
    #[inline]
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let guard = &crossbeam_epoch::pin();
        self.tree.insert(key, value, guard)
    }

    /// Inserts a key-value pair if the key is not present, returning an [`EntryRef`].
    #[inline]
    pub fn get_or_insert_with<F>(&self, key: K, f: F) -> EntryRef<'_, K, V>
    where
        F: FnOnce() -> V,
    {
        let guard = crossbeam_epoch::pin();
        let leaf_ptr = self.tree.get_or_insert_with(key, f, &guard);

        EntryRef {
            leaf_ptr,
            tree: &self.tree,
        }
    }

    /// Removes an entry by key, returning the removed value if found.
    #[inline]
    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        let guard = &crossbeam_epoch::pin();
        self.tree.remove(key.as_bytes(), guard)
    }

    /// Removes an entry by raw byte slice key, returning the removed value if found.
    #[inline]
    pub fn remove_by_slice(&self, key: &[u8]) -> Option<V> {
        let guard = &crossbeam_epoch::pin();
        self.tree.remove(key, guard)
    }

    /// Returns an iterator over a sub-range of entries.
    pub fn range<R, Q>(&self, range: R) -> Range<'_, K, V>
    where
        R: RangeBounds<Q>,
        Q: AsBytes + ?Sized,
    {
        let start = match range.start_bound() {
            Bound::Included(b) => Bound::Included(b.as_bytes().to_vec()),
            Bound::Excluded(b) => Bound::Excluded(b.as_bytes().to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end = match range.end_bound() {
            Bound::Included(b) => Bound::Included(b.as_bytes().to_vec()),
            Bound::Excluded(b) => Bound::Excluded(b.as_bytes().to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };

        Range::new(&self.tree, start, end)
    }

    /// Returns an iterator visiting all key-value pairs in lexicographical key order.
    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter::new(&self.tree)
    }

    /// Returns an iterator visiting all keys in lexicographical order.
    pub fn keys(&self) -> Keys<'_, K, V> {
        Keys::new(self.iter())
    }

    /// Returns an iterator visiting all values in lexicographical key order.
    pub fn values(&self) -> Values<'_, K, V> {
        Values::new(self.iter())
    }

    /// Removes all key-value pairs from the map.
    #[inline]
    pub fn clear(&self) {
        self.tree.clear();
    }

    /// Validates all structural invariants of the tree.
    #[inline]
    pub fn validate_invariants(&self) {
        self.tree.validate_invariants();
    }
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> IntoIterator for &'a ArtMap<K, V> {
    type Item = EntryRef<'a, K, V>;
    type IntoIter = Iter<'a, K, V>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_artmap_basic_flow() {
        let map = ArtMap::<String, i32>::new();
        assert!(map.is_empty());

        assert_eq!(map.insert("users:100".to_string(), 1), None);
        assert_eq!(map.insert("users:200".to_string(), 2), None);
        assert_eq!(map.insert("users:300".to_string(), 3), None);
        assert_eq!(map.len(), 3);

        assert_eq!(map.get("users:100").as_deref(), Some(&1));
        assert_eq!(map.get_by_slice(b"users:200").as_deref(), Some(&2));
        assert!(map.contains_key("users:300"));
        assert!(map.contains_key_slice(b"users:100"));
        assert!(!map.contains_key("users:400"));

        // with_value closure
        let doubled = map.with_value("users:100", |v| *v * 2);
        assert_eq!(doubled, Some(2));

        // Update
        assert_eq!(map.insert("users:100".to_string(), 10), Some(1));
        assert_eq!(map.get("users:100").as_deref(), Some(&10));

        // Remove
        assert_eq!(map.remove("users:200"), Some(2));
        assert!(map.get("users:200").is_none());
        assert_eq!(map.len(), 2);

        map.validate_invariants();
    }

    #[test]
    fn test_artmap_range_scan() {
        let map = ArtMap::<String, i32>::new();
        map.insert("k:1".to_string(), 1);
        map.insert("k:2".to_string(), 2);
        map.insert("k:3".to_string(), 3);
        map.insert("k:4".to_string(), 4);
        map.insert("k:5".to_string(), 5);

        let items: Vec<_> = map
            .range("k:2".."k:5")
            .map(|e| (e.key().as_str(), *e.value()))
            .collect();
        assert_eq!(items, vec![("k:2", 2), ("k:3", 3), ("k:4", 4)]);

        let rev_items: Vec<_> = map
            .range("k:2".."k:5")
            .rev()
            .map(|e| (e.key().as_str(), *e.value()))
            .collect();
        assert_eq!(rev_items, vec![("k:4", 4), ("k:3", 3), ("k:2", 2)]);
    }

    #[test]
    fn test_artmap_get_or_insert_with() {
        let map = ArtMap::<String, i32>::new();
        {
            let entry = map.get_or_insert_with("key".to_string(), || 42);
            assert_eq!(*entry, 42);
            assert_eq!(entry.key(), "key");
            assert_eq!(entry.value(), &42);
            assert!(!entry.is_removed());
            assert!(entry.remove());
            assert!(entry.is_removed());
        }
        assert!(map.get("key").is_none());
    }
}
