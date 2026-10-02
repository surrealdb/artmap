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

//! # The arena-backed multi-version map

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::borrow::Borrow;
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use crate::arena::map::pad;
use crate::arena::node::{VersionNode, VersionedLeaf};
use crate::arena::tree::InserterCache;
use crate::arena::versioned_iter::{ArenaVersionedEntryRef, ArenaVersionedRange};
use crate::arena::versioned_tree::{chain, find_le, ArenaVersionedTree};
use crate::arena::{Arena, ArenaFull};
use crate::key::AsBytes;
use crate::raw::cursor::{owned_bound, Cursor, KeyBuf};
use crate::raw::node::{Node16, Node256, Node4, Node48, MAX_PREFIX_LEN};
use crate::sync::atomic::AtomicU32;

/// A concurrent ordered map with 64-bit MVCC versions per key, allocated from
/// an [`Arena`] (for LSM memtables and snapshot-isolated workloads).
///
/// Semantics match [`VersionedArtMap`](crate::VersionedArtMap): `len()` counts
/// keys whose newest version is live; `delete` records a tombstone; reads at a
/// snapshot see the newest version at or below it; scans are latest-view;
/// `prune_key` and `prune_all` follow the same watermark contract, and
/// `remove_key` and `remove_version` change history for every snapshot.
/// Pruned and removed versions and keys stop being visible, but their bytes
/// stay in the arena, and their values are dropped, when the map is dropped.
/// See the [module docs](crate::arena) for ownership and capacity.
pub struct ArenaVersionedArtMap<K, V> {
    tree: ArenaVersionedTree<K, V>,
}

impl<K, V> ArenaVersionedArtMap<K, V> {
    /// Creates a map in `arena`.
    #[inline]
    pub fn new(arena: Arc<Arena>) -> Self {
        Self {
            tree: ArenaVersionedTree::new(arena),
        }
    }

    /// Creates a map in a new arena of `capacity` bytes.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self::new(Arena::with_capacity(capacity))
    }

    /// The map's arena.
    #[inline]
    pub fn arena(&self) -> &Arc<Arena> {
        self.tree.arena()
    }

    /// The number of keys whose newest version is live.
    #[inline]
    pub fn len(&self) -> usize {
        self.tree.len()
    }

    /// `true` if no key has a live newest version.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// An upper bound on the arena bytes one insert of a key of `key_len`
    /// bytes can consume: a version node, a leaf, the worst prefix chain, a
    /// split node, one node of each larger size, and alignment padding. A new
    /// version of an existing key costs only the version node.
    pub const fn max_insert_bytes(key_len: usize) -> usize {
        let version = pad(size_of::<VersionNode<V>>(), align_of::<VersionNode<V>>());
        let leaf = pad(
            size_of::<VersionedLeaf<K, V>>(),
            align_of::<VersionedLeaf<K, V>>(),
        );
        let node4s = key_len / (MAX_PREFIX_LEN + 1) + 2;
        version
            + leaf
            + node4s * pad(size_of::<Node4<AtomicU32>>(), 8)
            + pad(size_of::<Node16<AtomicU32>>(), 8)
            + pad(size_of::<Node48<AtomicU32>>(), 8)
            + pad(size_of::<Node256<AtomicU32>>(), 8)
    }
}

impl<K: AsBytes, V> ArenaVersionedArtMap<K, V> {
    /// The newest live value of `key`.
    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
        V: Clone,
    {
        self.get_latest(key).map(|(_, v)| v)
    }

    /// The newest live value of a raw byte key.
    #[inline]
    pub fn get_slice(&self, key: &[u8]) -> Option<V>
    where
        V: Clone,
    {
        let e = self.entry_for(key)?;
        Some(e.value().clone())
    }

    /// Alias for [`get_slice`](Self::get_slice).
    #[inline]
    pub fn get_by_slice(&self, key: &[u8]) -> Option<V>
    where
        V: Clone,
    {
        self.get_slice(key)
    }

    /// The newest version and value of `key`, if live.
    #[inline]
    pub fn get_latest<Q>(&self, key: &Q) -> Option<(u64, V)>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
        V: Clone,
    {
        let e = self.entry_for(key.as_bytes())?;
        Some((e.version(), e.value().clone()))
    }

    /// A handle on the newest live version of `key`, without cloning.
    #[inline]
    pub fn get_entry<Q>(&self, key: &Q) -> Option<ArenaVersionedEntryRef<'_, K, V>>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        self.entry_for(key.as_bytes())
    }

    fn entry_for(&self, key: &[u8]) -> Option<ArenaVersionedEntryRef<'_, K, V>> {
        let leaf = self.tree.raw.get(key)?;
        // SAFETY: a leaf of this map; its head is a node of this arena.
        unsafe { ArenaVersionedEntryRef::new(leaf, leaf.as_ref().head(), self.tree.arena()) }
    }

    /// The newest version of `key` that is `<= max_version`, unless it is a
    /// tombstone.
    #[inline]
    pub fn get_version_le<Q>(&self, key: &Q, max_version: u64) -> Option<(u64, V)>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
        V: Clone,
    {
        let leaf = self.tree.raw.get(key.as_bytes())?;
        // SAFETY: a leaf of this map, valid for `&self`.
        let head = unsafe { leaf.as_ref() }.head();
        let n = find_le::<V>(self.tree.arena(), head, max_version)?;
        n.value().cloned().map(|v| (n.version, v))
    }

    /// Every version of `key`, newest first; `None` values are tombstones.
    pub fn get_all_versions<Q>(&self, key: &Q) -> Vec<(u64, Option<V>)>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
        V: Clone,
    {
        let Some(leaf) = self.tree.raw.get(key.as_bytes()) else {
            return Vec::new();
        };
        // SAFETY: a leaf of this map, valid for `&self`.
        let head = unsafe { leaf.as_ref() }.head();
        chain::<V>(self.tree.arena(), head)
            .map(|n| (n.version, n.value().cloned()))
            .collect()
    }

    /// The number of versions of `key`, including tombstones.
    pub fn version_count<Q>(&self, key: &Q) -> usize
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        let Some(leaf) = self.tree.raw.get(key.as_bytes()) else {
            return 0;
        };
        // SAFETY: a leaf of this map, valid for `&self`.
        let head = unsafe { leaf.as_ref() }.head();
        chain::<V>(self.tree.arena(), head).count()
    }

    /// `true` if the newest version of `key` is live.
    #[inline]
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        self.entry_for(key.as_bytes()).is_some()
    }

    /// Adds `version` of `key`, replacing (out of place) a version with the
    /// same number. Always returns `true`.
    ///
    /// # Panics
    /// If the insert does not fit in the arena (after every latch is released,
    /// with the map unchanged). See [`try_insert`](Self::try_insert).
    #[inline]
    pub fn insert(&self, key: K, version: u64, value: V) -> bool {
        match self.try_insert(key, version, value) {
            Ok(()) => true,
            Err(full) => panic!("{full}"),
        }
    }

    /// Alias for [`insert`](Self::insert).
    #[inline]
    pub fn insert_versioned(&self, key: K, version: u64, value: V) -> bool {
        self.insert(key, version, value)
    }

    /// As [`insert`](Self::insert), returning the key and value if the insert
    /// does not fit.
    #[inline]
    pub fn try_insert(&self, key: K, version: u64, value: V) -> Result<(), ArenaFull<K, V>> {
        match self.tree.insert(key, version, Some(value), None) {
            Ok(_) => Ok(()),
            Err((key, value)) => Err(ArenaFull {
                key,
                value: value.expect("the value comes back"),
            }),
        }
    }

    /// Records a tombstone at `version` for `key`, creating the key if needed
    /// (so it shadows older data elsewhere). Older snapshots are unchanged.
    /// Returns `true` if the key's newest version was live and is now deleted.
    ///
    /// # Panics
    /// If the tombstone does not fit in the arena.
    pub fn delete(&self, key: K, version: u64) -> bool {
        match self.tree.insert(key, version, None, None) {
            Ok(delta) => delta < 0,
            Err(_) => panic!("the arena is full"),
        }
    }

    /// Deletes the newest version of `key` in place of its value: a tombstone
    /// with the same version number, so snapshots at or after it see the key
    /// as absent. Returns the replaced version.
    ///
    /// # Panics
    /// If the tombstone does not fit in the arena.
    #[deprecated(note = "use `delete(key, version)`, which does not rewrite history")]
    pub fn remove<Q>(&self, key: &Q) -> Option<ArenaVersionedEntryRef<'_, K, V>>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        let (leaf, old) = self
            .tree
            .remove_head(key.as_bytes())
            .unwrap_or_else(|()| panic!("the arena is full"))?;
        // SAFETY: the replaced version stays valid for the map's life.
        unsafe { ArenaVersionedEntryRef::new(leaf, old, self.tree.arena()) }
    }

    /// Unlinks every version of `key` older than its newest version
    /// `<= min_version`, and the key itself if that version is its newest one
    /// and a tombstone (a `delete`, or a value that `is_tombstone` accepts).
    /// Returns the number of versions unlinked. As
    /// [`VersionedArtMap::prune_key`](crate::VersionedArtMap::prune_key),
    /// except that nothing is freed before the map is dropped.
    ///
    /// `is_tombstone` runs without any latch held.
    ///
    /// ```
    /// let map = artmap::ArenaVersionedArtMap::<String, u32>::with_capacity(1 << 16);
    /// for v in 1..=4 {
    ///     map.insert("k".to_string(), v, v as u32);
    /// }
    /// assert_eq!(map.prune_key("k", 3, |_| false), 2);
    /// assert_eq!(map.get_all_versions("k"), vec![(4, Some(4)), (3, Some(3))]);
    /// map.delete("k".to_string(), 5);
    /// assert_eq!(map.prune_key("k", 5, |_| false), 3);
    /// assert_eq!(map.version_count("k"), 0);
    /// ```
    pub fn prune_key<Q, F>(&self, key: &Q, min_version: u64, is_tombstone: F) -> usize
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
        F: Fn(&V) -> bool,
    {
        match self.tree.raw.get(key.as_bytes()) {
            Some(leaf) => self.tree.prune_leaf(leaf, min_version, &is_tombstone),
            None => 0,
        }
    }

    /// [`prune_key`](Self::prune_key) for every key, including deleted ones.
    pub fn prune_all<F>(&self, min_version: u64, is_tombstone: F) -> usize
    where
        F: Fn(&V) -> bool,
    {
        let mut cursor = Cursor::new(Bound::Unbounded, Bound::Unbounded);
        let mut total = 0;
        while let Some(leaf) = cursor.next(&self.tree.raw) {
            total += self.tree.prune_leaf(leaf, min_version, &is_tombstone);
        }
        total
    }

    /// Removes `key` and every version of it, for every snapshot, unlike
    /// [`delete`](Self::delete). Returns `true` if the key had any version.
    pub fn remove_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        self.tree.remove_key(key.as_bytes())
    }

    /// Removes exactly `version` of `key`, a value or a tombstone, for every
    /// snapshot. The key goes with its last version. Returns `true` if that
    /// version existed.
    ///
    /// ```
    /// let map = artmap::ArenaVersionedArtMap::<String, u32>::with_capacity(1 << 16);
    /// map.insert("k".to_string(), 1, 10);
    /// map.insert("k".to_string(), 2, 20);
    /// assert!(map.remove_version("k", 2));
    /// assert_eq!(map.get("k"), Some(10));
    /// assert!(map.remove_version("k", 1));
    /// assert!(!map.contains_key("k"));
    /// ```
    pub fn remove_version<Q>(&self, key: &Q, version: u64) -> bool
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        self.tree.remove_version(key.as_bytes(), version)
    }

    /// Removes every key and version, linearizably (at the swap of the root).
    /// Their bytes stay in the arena until the map is dropped.
    pub fn clear(&self) {
        self.tree.clear();
    }

    /// An inserter for this map, caching the last insertion point.
    #[inline]
    pub fn inserter(&self) -> ArenaVersionedInserter<'_, K, V> {
        ArenaVersionedInserter {
            tree: &self.tree,
            cache: InserterCache::new(),
            _not_send: PhantomData,
        }
    }

    /// A latest-view iterator over `range`.
    pub fn range<R, Q>(&self, range: R) -> ArenaVersionedRange<'_, K, V>
    where
        R: RangeBounds<Q>,
        Q: AsBytes + ?Sized,
    {
        ArenaVersionedRange::new(
            &self.tree,
            owned_bound(range.start_bound()),
            owned_bound(range.end_bound()),
        )
    }

    /// A latest-view iterator over every key.
    pub fn iter(&self) -> ArenaVersionedRange<'_, K, V> {
        ArenaVersionedRange::new(&self.tree, Bound::Unbounded, Bound::Unbounded)
    }

    /// The first live entry at or after (`include_equal`) or strictly after
    /// `search_key`.
    #[inline]
    pub fn find_successor(
        &self,
        search_key: &[u8],
        include_equal: bool,
    ) -> Option<ArenaVersionedEntryRef<'_, K, V>> {
        let start = if include_equal {
            Bound::Included(KeyBuf::new(search_key))
        } else {
            Bound::Excluded(KeyBuf::new(search_key))
        };
        ArenaVersionedRange::new(&self.tree, start, Bound::Unbounded).next()
    }

    /// The last live entry at or before (`include_equal`) or strictly before
    /// `search_key`.
    #[inline]
    pub fn find_predecessor(
        &self,
        search_key: &[u8],
        include_equal: bool,
    ) -> Option<ArenaVersionedEntryRef<'_, K, V>> {
        let end = if include_equal {
            Bound::Included(KeyBuf::new(search_key))
        } else {
            Bound::Excluded(KeyBuf::new(search_key))
        };
        ArenaVersionedRange::new(&self.tree, Bound::Unbounded, end).next_back()
    }

    /// The first live entry.
    #[inline]
    pub fn first_entry(&self) -> Option<ArenaVersionedEntryRef<'_, K, V>> {
        self.iter().next()
    }

    /// The last live entry.
    #[inline]
    pub fn last_entry(&self) -> Option<ArenaVersionedEntryRef<'_, K, V>> {
        self.iter().next_back()
    }

    /// Calls `callback(key, value, version)` for the newest live version of
    /// each key in `range`, until it returns `false`.
    pub fn scan<R, Q, F>(&self, range: R, mut callback: F)
    where
        R: RangeBounds<Q>,
        Q: AsBytes + ?Sized,
        F: FnMut(&K, &V, u64) -> bool,
    {
        for e in self.range(range) {
            if !callback(e.key(), e.value(), e.version()) {
                break;
            }
        }
    }

    /// Checks the structural invariants, and that `len()` equals the number
    /// of reachable keys whose newest version is live. Panics on a violation.
    pub fn validate_invariants(&mut self) {
        self.tree.validate();
    }
}

impl<'a, K: AsBytes, V> IntoIterator for &'a ArenaVersionedArtMap<K, V> {
    type Item = ArenaVersionedEntryRef<'a, K, V>;
    type IntoIter = ArenaVersionedRange<'a, K, V>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// A map-bound inserter for an [`ArenaVersionedArtMap`], created by
/// [`ArenaVersionedArtMap::inserter`]. Neither `Send` nor `Sync`.
pub struct ArenaVersionedInserter<'m, K, V> {
    tree: &'m ArenaVersionedTree<K, V>,
    cache: InserterCache,
    _not_send: PhantomData<*const ()>,
}

impl<K: AsBytes, V> ArenaVersionedInserter<'_, K, V> {
    /// As [`ArenaVersionedArtMap::insert`].
    ///
    /// # Panics
    /// If the insert does not fit in the arena.
    #[inline]
    pub fn insert(&mut self, key: K, version: u64, value: V) -> bool {
        match self.try_insert(key, version, value) {
            Ok(()) => true,
            Err(full) => panic!("{full}"),
        }
    }

    /// As [`ArenaVersionedArtMap::try_insert`].
    #[inline]
    pub fn try_insert(&mut self, key: K, version: u64, value: V) -> Result<(), ArenaFull<K, V>> {
        match self
            .tree
            .insert(key, version, Some(value), Some(&mut self.cache))
        {
            Ok(_) => Ok(()),
            Err((key, value)) => Err(ArenaFull {
                key,
                value: value.expect("the value comes back"),
            }),
        }
    }
}
