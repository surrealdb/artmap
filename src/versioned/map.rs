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

//! # Concurrent Multi-Version Adaptive Radix Tree
//!
//! Provides [`VersionedArtMap`], an epoch-based concurrent Adaptive Radix Tree (ART)
//! supporting 64-bit MVCC version chains and lock-free snapshot reads.

use std::borrow::Borrow;
use std::ops::{Bound, RangeBounds};

use crate::key::AsBytes;
use crate::versioned::entry::VersionedEntryRef;
use crate::versioned::iter::Range;
use crate::versioned::tree::VersionedTree;

/// A concurrent associative map with built-in 64-bit MVCC versioning.
///
/// Features lock-free optimistic snapshot reads, epoch-based memory reclamation via `crossbeam-epoch`,
/// and atomic version prepend chains in leaf nodes.
pub struct VersionedArtMap<K: AsBytes + Send + 'static, V: Send + Clone + 'static> {
    tree: VersionedTree<K, V>,
}

impl<K: AsBytes + Send + 'static, V: Send + Clone + 'static> Default for VersionedArtMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: AsBytes + Send + 'static, V: Send + Clone + 'static> VersionedArtMap<K, V> {
    /// Creates a new empty `VersionedArtMap`.
    pub fn new() -> Self {
        Self {
            tree: VersionedTree::new(),
        }
    }

    /// Returns the number of distinct keys stored in the map.
    #[inline]
    pub fn len(&self) -> usize {
        self.tree.len()
    }

    /// Returns `true` if the map contains no entries.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }

    /// Returns the newest committed value corresponding to the key, if present and not deleted.
    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
        V: Clone,
    {
        self.get_latest(key).map(|(_, v)| v)
    }

    /// Point lookup on raw byte slice returning the newest committed value, if present and not deleted.
    #[inline]
    pub fn get_slice(&self, key_bytes: &[u8]) -> Option<V>
    where
        V: Clone,
    {
        let guard = crossbeam_epoch::pin();
        self.tree
            .get_latest(key_bytes, &guard)
            .map(|(_, v)| v.clone())
    }

    /// Looks up the newest committed version and value for `key`.
    #[inline]
    pub fn get_latest<Q>(&self, key: &Q) -> Option<(u64, V)>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
        V: Clone,
    {
        let guard = crossbeam_epoch::pin();
        self.tree
            .get_latest(key, &guard)
            .map(|(ver, v)| (ver, v.clone()))
    }

    /// Looks up the newest version of `key` whose version is less than or equal to `max_version`.
    ///
    /// Essential for lock-free MVCC snapshot isolation.
    #[inline]
    pub fn get_version_le<Q>(&self, key: &Q, max_version: u64) -> Option<(u64, V)>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
        V: Clone,
    {
        let guard = crossbeam_epoch::pin();
        self.tree
            .get_version_le(key, max_version, &guard)
            .map(|(ver, v)| (ver, v.clone()))
    }

    /// Checks if the key is present in the map with an active (non-deleted) head version.
    #[inline]
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
        V: Clone,
    {
        self.get(key).is_some()
    }

    /// Inserts a versioned key-value pair into the map.
    #[inline]
    pub fn insert(&self, key: K, version: u64, value: V) -> bool {
        let guard = crossbeam_epoch::pin();
        self.tree.insert(key, version, value, &guard)
    }

    /// Inserts a versioned key-value pair into the map (alias for `insert`).
    #[inline]
    pub fn insert_versioned(&self, key: K, version: u64, value: V) -> bool {
        self.insert(key, version, value)
    }

    /// Marks the latest version of a key as removed, returning the removed value if present.
    #[inline]
    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
        V: Clone,
    {
        let guard = crossbeam_epoch::pin();
        self.tree.remove(key, &guard)
    }

    /// Returns the number of versions stored for `key`.
    #[inline]
    pub fn version_count<Q>(&self, key: &Q) -> usize
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        let guard = crossbeam_epoch::pin();
        self.tree.version_count(key, &guard)
    }

    /// Prunes stale versions older than `min_version` from the version chain of `key`.
    ///
    /// If the key has become a dead tombstone at or below `min_version` with no newer versions,
    /// the key is unlinked from the tree.
    #[inline]
    pub fn prune_key<Q, F>(&self, key: &Q, min_version: u64, is_tombstone: F) -> usize
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
        F: Fn(&V) -> bool,
    {
        let guard = crossbeam_epoch::pin();
        self.tree.prune_key(key, min_version, is_tombstone, &guard)
    }

    /// Prunes stale versions older than `min_version` across all keys in the map.
    pub fn prune_all<F>(&self, min_version: u64, is_tombstone: F) -> usize
    where
        F: Fn(&V) -> bool + Copy,
    {
        let guard = crossbeam_epoch::pin();
        let mut total = 0;
        for entry in self.iter() {
            total += self
                .tree
                .prune_key(entry.key(), min_version, is_tombstone, &guard);
        }
        total
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

    /// Returns an iterator visiting all entries in ascending key order.
    pub fn iter(&self) -> Range<'_, K, V> {
        self.range::<std::ops::RangeFull, [u8]>(..)
    }

    /// Scans entries in the given key range, invoking `callback` for each entry with its key, value, and version.
    ///
    /// If `callback` returns `false`, scanning terminates early.
    pub fn scan<R, Q, F>(&self, range: R, mut callback: F)
    where
        R: RangeBounds<Q>,
        Q: AsBytes + ?Sized,
        F: FnMut(&K, &V, u64) -> bool,
    {
        for entry in self.range(range) {
            if !callback(entry.key(), entry.value(), entry.version()) {
                break;
            }
        }
    }
}

impl<'a, K: AsBytes + Send + 'static, V: Send + Clone + 'static> IntoIterator
    for &'a VersionedArtMap<K, V>
{
    type Item = VersionedEntryRef<'a, K, V>;
    type IntoIter = Range<'a, K, V>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
