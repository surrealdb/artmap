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

//! # The arena-backed map

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::borrow::Borrow;
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use crate::arena::iter::{ArenaEntryRef, Range};
use crate::arena::node::Leaf;
use crate::arena::tree::{ArenaTree, InserterCache};
use crate::arena::{Arena, ArenaFull};
use crate::key::AsBytes;
use crate::raw::cursor::owned_bound;
use crate::raw::node::{Node16, Node256, Node4, Node48, MAX_PREFIX_LEN};
use crate::sync::atomic::AtomicU32;

/// A concurrent ordered map allocated from an [`Arena`].
///
/// Nodes use 32-bit offsets, so inner nodes are small. Nothing is freed
/// while the map is alive: removed and replaced entries stay readable through
/// their handles, and every update consumes arena capacity. See the
/// [module docs](crate::arena) for the ownership and capacity contracts.
pub struct ArenaArtMap<K, V> {
    tree: ArenaTree<K, V>,
}

impl<K, V> ArenaArtMap<K, V> {
    /// Creates a map in `arena`.
    #[inline]
    pub fn new(arena: Arc<Arena>) -> Self {
        Self {
            tree: ArenaTree::new(arena),
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

    /// The number of entries.
    #[inline]
    pub fn len(&self) -> usize {
        self.tree.len()
    }

    /// `true` if the map has no entries.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// An upper bound on the arena bytes one insert of a key of `key_len`
    /// bytes can consume, whatever the tree's shape and however many times the
    /// insert retries: a leaf, the worst prefix chain for the key, a split
    /// node, one node of each larger size, and alignment padding.
    pub const fn max_insert_bytes(key_len: usize) -> usize {
        let leaf = pad(size_of::<Leaf<K, V>>(), align_of::<Leaf<K, V>>());
        let node4s = key_len / (MAX_PREFIX_LEN + 1) + 2;
        leaf + node4s * pad(size_of::<Node4<AtomicU32>>(), 8)
            + pad(size_of::<Node16<AtomicU32>>(), 8)
            + pad(size_of::<Node48<AtomicU32>>(), 8)
            + pad(size_of::<Node256<AtomicU32>>(), 8)
    }
}

/// An allocation of `size` bytes with `align` alignment, worst case.
pub(crate) const fn pad(size: usize, align: usize) -> usize {
    size + align - 1
}

impl<K: AsBytes, V> ArenaArtMap<K, V> {
    /// A clone of the value for `key`.
    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
        V: Clone,
    {
        self.get_entry(key).map(|e| e.value().clone())
    }

    /// A clone of the value for a raw byte key.
    #[inline]
    pub fn get_slice(&self, key: &[u8]) -> Option<V>
    where
        V: Clone,
    {
        let leaf = self.tree.raw.get(key)?;
        // SAFETY: a leaf of this map's tree, valid for `&self`.
        Some(unsafe { ArenaEntryRef::new(leaf) }.value().clone())
    }

    /// A handle on the entry for `key`, without cloning.
    #[inline]
    pub fn get_entry<Q>(&self, key: &Q) -> Option<ArenaEntryRef<'_, K, V>>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        let leaf = self.tree.raw.get(key.as_bytes())?;
        // SAFETY: a leaf of this map's tree, valid for `&self`.
        Some(unsafe { ArenaEntryRef::new(leaf) })
    }

    /// `true` if the map has an entry for `key`.
    #[inline]
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        self.tree.raw.get(key.as_bytes()).is_some()
    }

    /// Inserts or replaces, returning the displaced entry.
    ///
    /// # Panics
    /// If the insert does not fit in the arena. The panic happens after every
    /// latch is released and the map is unchanged. Use
    /// [`try_insert`](Self::try_insert) to handle a full arena.
    #[inline]
    pub fn insert(&self, key: K, value: V) -> Option<ArenaEntryRef<'_, K, V>> {
        match self.try_insert(key, value) {
            Ok(old) => old,
            Err(full) => panic!("{full}"),
        }
    }

    /// Inserts or replaces, returning the displaced entry, or the key and
    /// value back if the insert does not fit.
    #[inline]
    pub fn try_insert(
        &self,
        key: K,
        value: V,
    ) -> Result<Option<ArenaEntryRef<'_, K, V>>, ArenaFull<K, V>> {
        let old = self.tree.insert(key, value, None)?;
        // SAFETY: the displaced leaf stays valid for the map's life.
        Ok(old.map(|l| unsafe { ArenaEntryRef::new(l) }))
    }

    /// Removes the entry for `key`, returning it. The entry stays readable
    /// through the handle for the map borrow.
    #[inline]
    pub fn remove<Q>(&self, key: &Q) -> Option<ArenaEntryRef<'_, K, V>>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        let leaf = self.tree.remove(key.as_bytes())?;
        // SAFETY: the removed leaf stays valid for the map's life.
        Some(unsafe { ArenaEntryRef::new(leaf) })
    }

    /// An inserter for this map, which caches the last insertion point so
    /// that sequential or clustered keys skip the descent from the root.
    #[inline]
    pub fn inserter(&self) -> ArenaInserter<'_, K, V> {
        ArenaInserter {
            tree: &self.tree,
            cache: InserterCache::new(),
            _not_send: PhantomData,
        }
    }

    /// An iterator over the entries in `range`, in key order.
    pub fn range<R, Q>(&self, range: R) -> Range<'_, K, V>
    where
        R: RangeBounds<Q>,
        Q: AsBytes + ?Sized,
    {
        Range::new(
            &self.tree,
            owned_bound(range.start_bound()),
            owned_bound(range.end_bound()),
        )
    }

    /// An iterator over every entry, in key order.
    pub fn iter(&self) -> Range<'_, K, V> {
        Range::new(&self.tree, Bound::Unbounded, Bound::Unbounded)
    }

    /// Calls `callback(key, value)` for each entry in `range`, until it
    /// returns `false`.
    pub fn scan<R, Q, F>(&self, range: R, mut callback: F)
    where
        R: RangeBounds<Q>,
        Q: AsBytes + ?Sized,
        F: FnMut(&K, &V) -> bool,
    {
        for e in self.range(range) {
            if !callback(e.key(), e.value()) {
                break;
            }
        }
    }

    /// Checks the structural invariants; panics on a violation.
    pub fn validate_invariants(&mut self) {
        self.tree.raw.validate();
    }
}

impl<'a, K: AsBytes, V> IntoIterator for &'a ArenaArtMap<K, V> {
    type Item = ArenaEntryRef<'a, K, V>;
    type IntoIter = Range<'a, K, V>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// A map-bound inserter for an [`ArenaArtMap`], created by
/// [`ArenaArtMap::inserter`].
///
/// It caches the node the last insert landed in, with the version its own
/// unlock produced, and inserts there directly when the next key falls under
/// it and the node is unchanged; otherwise it takes the normal path. Neither
/// `Send` nor `Sync`.
pub struct ArenaInserter<'m, K, V> {
    tree: &'m ArenaTree<K, V>,
    cache: InserterCache,
    /// `&ArenaTree` alone would make the inserter `Send`/`Sync`.
    _not_send: PhantomData<*const ()>,
}

impl<'m, K: AsBytes, V> ArenaInserter<'m, K, V> {
    /// As [`ArenaArtMap::insert`].
    ///
    /// # Panics
    /// If the insert does not fit in the arena.
    #[inline]
    pub fn insert(&mut self, key: K, value: V) -> Option<ArenaEntryRef<'m, K, V>> {
        match self.try_insert(key, value) {
            Ok(old) => old,
            Err(full) => panic!("{full}"),
        }
    }

    /// As [`ArenaArtMap::try_insert`].
    #[inline]
    pub fn try_insert(
        &mut self,
        key: K,
        value: V,
    ) -> Result<Option<ArenaEntryRef<'m, K, V>>, ArenaFull<K, V>> {
        let old = self.tree.insert(key, value, Some(&mut self.cache))?;
        // SAFETY: the displaced leaf stays valid for the map's life.
        Ok(old.map(|l| unsafe { ArenaEntryRef::new(l) }))
    }
}
