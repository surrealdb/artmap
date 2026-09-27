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

use std::borrow::Borrow;
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::arena::map::ArenaInserter;
use crate::arena::versioned_iter::{ArenaVersionedEntryRef, ArenaVersionedRange};
use crate::arena::versioned_tree::ArenaVersionedTree;
use crate::arena::Arena;
use crate::key::AsBytes;

/// A concurrent associative map with 64-bit MVCC versioning, backed by an arena-allocated Adaptive Radix Tree.
///
/// Uses 32-bit offsets for child pointers instead of 64-bit pointers, reducing inner node
/// memory consumption by ~40% and enabling $O(1)$ whole-arena teardown when dropped or reset.
/// Supports atomic version prepend chains in leaves for LSM memtables and snapshot-isolated workloads.
pub struct ArenaVersionedArtMap<K: AsBytes + Clone, V: Clone> {
    tree: ArenaVersionedTree<K, V>,
}

impl<K: AsBytes + Clone, V: Clone> ArenaVersionedArtMap<K, V> {
    /// Creates a new `ArenaVersionedArtMap` backed by the specified [`Arena`].
    #[inline]
    pub fn new(arena: Arc<Arena>) -> Self {
        Self {
            tree: ArenaVersionedTree::new(arena),
        }
    }

    /// Creates an `ArenaVersionedArtMap` with a dedicated new arena of the given capacity.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self::new(Arena::with_capacity(capacity))
    }

    /// Returns a reference to the underlying [`Arena`].
    #[inline]
    pub fn arena(&self) -> &Arc<Arena> {
        self.tree.arena()
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

    /// Returns the newest committed value corresponding to the key, if present and not deleted.
    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        self.get_latest(key).map(|(_, v)| v)
    }

    /// Point lookup on raw byte slice returning the newest committed value, if present and not deleted.
    #[inline]
    pub fn get_slice(&self, key_bytes: &[u8]) -> Option<V> {
        self.tree.get_latest(key_bytes).map(|(_, v)| v)
    }

    /// Looks up the newest committed version and value for `key`.
    #[inline]
    pub fn get_latest<Q>(&self, key: &Q) -> Option<(u64, V)>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        self.tree.get_latest(key.as_bytes())
    }

    /// Looks up the newest version of `key` whose version is less than or equal to `max_version`.
    ///
    /// Essential for MVCC snapshot reads in LSM engines.
    #[inline]
    pub fn get_version_le<Q>(&self, key: &Q, max_version: u64) -> Option<(u64, V)>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        self.tree.get_version_le(key.as_bytes(), max_version)
    }

    /// Checks if the key is present in the map with a non-deleted head version.
    #[inline]
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        self.get(key).is_some()
    }

    /// Inserts a versioned key-value pair into the map.
    #[inline]
    pub fn insert(&self, key: K, version: u64, value: V) -> bool {
        self.tree.insert(key, version, value)
    }

    /// Inserts a versioned key-value pair into the map (alias for `insert`).
    #[inline]
    pub fn insert_versioned(&self, key: K, version: u64, value: V) -> bool {
        self.tree.insert(key, version, value)
    }

    /// Inserts a versioned key-value pair using an [`ArenaInserter`] cache to accelerate sequential or localized writes.
    #[inline]
    pub fn insert_with_inserter(
        &self,
        key: K,
        version: u64,
        value: V,
        inserter: &mut ArenaInserter,
    ) -> bool {
        self.tree
            .insert_with_inserter(key, version, value, inserter)
    }

    /// Marks the latest version of a key as removed, returning the removed value if present.
    #[inline]
    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        let leaf_ptr = self.tree.get_leaf(key.as_bytes())?;
        let leaf = unsafe { &*leaf_ptr };
        let head_off = leaf.versions_offset.load(Ordering::Acquire) & !1;
        if head_off == 0 {
            return None;
        }
        let head = unsafe {
            &*(self.tree.arena.get_pointer(head_off)
                as *const crate::arena::node::ArenaVersionNode<V>)
        };
        if head.removed.swap(true, Ordering::AcqRel) {
            None
        } else {
            self.tree.len.fetch_sub(1, Ordering::Relaxed);
            Some(head.value.clone())
        }
    }

    /// Returns an iterator over a sub-range of entries.
    pub fn range<R, Q>(&self, range: R) -> ArenaVersionedRange<'_, K, V>
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

        ArenaVersionedRange::new(&self.tree, start, end)
    }

    /// Returns an iterator visiting all entries in ascending key order.
    pub fn iter(&self) -> ArenaVersionedRange<'_, K, V> {
        self.range::<std::ops::RangeFull, [u8]>(..)
    }

    /// Finds the entry matching `search_key` or its successor in lexicographical key order.
    #[inline]
    pub fn find_successor(
        &self,
        search_key: &[u8],
        include_equal: bool,
    ) -> Option<ArenaVersionedEntryRef<'_, K, V>> {
        self.tree
            .find_successor(search_key, include_equal)
            .map(|leaf_ptr| ArenaVersionedEntryRef {
                leaf_ptr,
                arena: &self.tree.arena,
                _marker: PhantomData,
            })
    }

    /// Finds the entry matching `search_key` or its predecessor in lexicographical key order.
    #[inline]
    pub fn find_predecessor(
        &self,
        search_key: &[u8],
        include_equal: bool,
    ) -> Option<ArenaVersionedEntryRef<'_, K, V>> {
        self.tree
            .find_predecessor(search_key, include_equal)
            .map(|leaf_ptr| ArenaVersionedEntryRef {
                leaf_ptr,
                arena: &self.tree.arena,
                _marker: PhantomData,
            })
    }

    /// Finds the first entry in lexicographical key order.
    #[inline]
    pub fn first_entry(&self) -> Option<ArenaVersionedEntryRef<'_, K, V>> {
        self.find_successor(&[], true)
    }

    /// Finds the last entry in lexicographical key order.
    #[inline]
    pub fn last_entry(&self) -> Option<ArenaVersionedEntryRef<'_, K, V>> {
        self.tree
            .last_leaf()
            .map(|leaf_ptr| ArenaVersionedEntryRef {
                leaf_ptr,
                arena: &self.tree.arena,
                _marker: PhantomData,
            })
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

impl<'a, K: AsBytes + Clone, V: Clone> IntoIterator for &'a ArenaVersionedArtMap<K, V> {
    type Item = ArenaVersionedEntryRef<'a, K, V>;
    type IntoIter = ArenaVersionedRange<'a, K, V>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
