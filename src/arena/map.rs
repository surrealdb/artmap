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
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::arena::iter::Range;
use crate::arena::tree::ArenaTree;
use crate::arena::Arena;
use crate::key::AsBytes;

/// A concurrent associative map backed by an arena-allocated Adaptive Radix Tree.
///
/// Uses 32-bit offsets for child pointers instead of 64-bit pointers, reducing inner node
/// memory consumption by ~40% and enabling $O(1)$ whole-arena teardown when dropped.
pub struct ArenaArtMap<K: AsBytes + Clone, V: Clone> {
    tree: ArenaTree<K, V>,
}

impl<K: AsBytes + Clone, V: Clone> ArenaArtMap<K, V> {
    /// Creates a new `ArenaArtMap` backed by the specified [`Arena`].
    #[inline]
    pub fn new(arena: Arc<Arena>) -> Self {
        Self {
            tree: ArenaTree::new(arena),
        }
    }

    /// Creates an `ArenaArtMap` with a dedicated new arena of the given capacity.
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

    pub fn debug_lookup(&self, key_bytes: &[u8]) {
        self.tree.debug_lookup(key_bytes);
    }

    /// Returns a reference to the value corresponding to the key, if present.
    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        let leaf_ptr = self.tree.get_leaf(key.as_bytes())?;
        let leaf = unsafe { &*leaf_ptr };
        if leaf.removed.load(Ordering::Acquire) {
            return None;
        }
        Some(leaf.value.clone())
    }

    /// Point lookup on raw byte slice.
    #[inline]
    pub fn get_slice(&self, key_bytes: &[u8]) -> Option<V> {
        let leaf_ptr = self.tree.get_leaf(key_bytes)?;
        let leaf = unsafe { &*leaf_ptr };
        if leaf.removed.load(Ordering::Acquire) {
            return None;
        }
        Some(leaf.value.clone())
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

    /// Checks if the key is present in the map.
    #[inline]
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        self.get(key).is_some()
    }

    /// Inserts a key-value pair into the map. Returns the old value if replaced.
    #[inline]
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        self.tree.insert(key, value)
    }

    /// Inserts a versioned key-value pair into the map.
    #[inline]
    pub fn insert_versioned(&self, key: K, version: u64, value: V) -> bool {
        self.tree.insert_versioned(key, version, value)
    }

    /// Removes a key from the map, returning the removed value if present.
    #[inline]
    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        let leaf_ptr = self.tree.get_leaf(key.as_bytes())?;
        let leaf = unsafe { &*leaf_ptr };
        if leaf.removed.swap(true, Ordering::AcqRel) {
            None
        } else {
            self.tree.len.fetch_sub(1, Ordering::Relaxed);
            Some(leaf.value.clone())
        }
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
}

impl<'a, K: AsBytes + Clone, V: Clone> IntoIterator for &'a ArenaArtMap<K, V> {
    type Item = crate::arena::iter::ArenaEntryRef<'a, K, V>;
    type IntoIter = Range<'a, K, V>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
