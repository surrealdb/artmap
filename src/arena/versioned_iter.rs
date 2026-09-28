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

//! # Entries and iterators of an [`ArenaVersionedArtMap`](crate::ArenaVersionedArtMap)

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::ops::{Bound, Deref};
use std::ptr::NonNull;

use crate::arena::node::{VersionNode, VersionedLeaf};
use crate::arena::versioned_tree::{chain, find_le, ArenaVersionedTree, Storage};
use crate::arena::Arena;
use crate::key::AsBytes;
use crate::raw::cursor::{Cursor, KeyBuf};

/// One version of a key: `value` is `None` for a tombstone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionRef<'a, V> {
    /// The version number.
    pub version: u64,
    /// The value, or `None` if this version is a tombstone.
    pub value: Option<&'a V>,
}

/// A snapshot of one live version of one key of an
/// [`ArenaVersionedArtMap`](crate::ArenaVersionedArtMap).
///
/// It captures a single version node when created, so `value` and `version`
/// always belong together. Valid for the whole map borrow. Neither `Send` nor
/// `Sync`.
pub struct ArenaVersionedEntryRef<'a, K, V> {
    leaf: NonNull<VersionedLeaf<K, V>>,
    /// Never a tombstone.
    node: NonNull<VersionNode<V>>,
    arena: &'a Arena,
}

impl<K, V> Clone for ArenaVersionedEntryRef<'_, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K, V> Copy for ArenaVersionedEntryRef<'_, K, V> {}

impl<'a, K: 'a, V: 'a> ArenaVersionedEntryRef<'a, K, V> {
    /// A handle on the version at `node`, or `None` if it is a tombstone.
    ///
    /// # Safety
    /// `leaf` and `node` belong to the tree whose arena is `arena`, borrowed
    /// for `'a`.
    pub(crate) unsafe fn new(
        leaf: NonNull<VersionedLeaf<K, V>>,
        node: u32,
        arena: &'a Arena,
    ) -> Option<Self> {
        // SAFETY: per the contract, a version node of this arena.
        let n = unsafe { arena.ptr::<VersionNode<V>>(node) };
        // SAFETY: as above.
        if unsafe { n.as_ref() }.is_tombstone() {
            return None;
        }
        Some(Self {
            leaf,
            node: n,
            arena,
        })
    }

    #[inline]
    fn leaf(&self) -> &'a VersionedLeaf<K, V> {
        // SAFETY: arena leaves are valid and their keys unchanged for `'a`.
        unsafe { self.leaf.as_ref() }
    }

    #[inline]
    fn node(&self) -> &'a VersionNode<V> {
        // SAFETY: arena version nodes are valid and immutable for `'a` (Inv 1).
        unsafe { self.node.as_ref() }
    }

    /// The entry's key.
    #[inline]
    pub fn key(&self) -> &'a K {
        &self.leaf().key
    }

    /// The captured version's value.
    #[inline]
    pub fn value(&self) -> &'a V {
        self.node()
            .value()
            .expect("an ArenaVersionedEntryRef never captures a tombstone")
    }

    /// The captured version.
    #[inline]
    pub fn version(&self) -> u64 {
        self.node().version
    }

    /// Always `false`: handles never capture tombstones.
    #[inline]
    pub fn is_removed(&self) -> bool {
        false
    }

    /// `true` once the captured version was unlinked (a same-version insert
    /// replaced it, or `remove` deleted it).
    #[inline]
    pub fn is_superseded(&self) -> bool {
        self.node().is_superseded()
    }

    /// The captured version and every older one, newest first, resolved
    /// through the map's own arena. `value` is `None` for tombstones.
    pub fn versions(&self) -> impl Iterator<Item = VersionRef<'a, V>> + 'a
    where
        V: 'a,
    {
        let arena = self.arena;
        chain::<V>(arena, arena.offset_of(self.node)).map(|n| VersionRef {
            version: n.version,
            value: n.value(),
        })
    }

    /// A fresh walk of the key's current chain for the newest version
    /// `<= max_version`.
    pub fn get_version_le(&self, max_version: u64) -> Option<(u64, &'a V)> {
        let n = find_le::<V>(self.arena, self.leaf().head(), max_version)?;
        n.value().map(|v| (n.version, v))
    }

    /// A fresh read of the key's newest live version.
    pub fn latest(&self) -> Option<Self> {
        // SAFETY: the leaf's current head belongs to the same arena.
        unsafe { Self::new(self.leaf, self.leaf().head(), self.arena) }
    }
}

impl<K, V> Deref for ArenaVersionedEntryRef<'_, K, V> {
    type Target = V;
    #[inline]
    fn deref(&self) -> &V {
        self.value()
    }
}

impl<K: std::fmt::Debug, V: std::fmt::Debug> std::fmt::Debug for ArenaVersionedEntryRef<'_, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArenaVersionedEntryRef")
            .field("key", self.key())
            .field("version", &self.version())
            .field("value", self.value())
            .finish()
    }
}

impl<K: PartialEq, V: PartialEq> PartialEq for ArenaVersionedEntryRef<'_, K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
            && self.version() == other.version()
            && self.value() == other.value()
    }
}

impl<K: Eq, V: Eq> Eq for ArenaVersionedEntryRef<'_, K, V> {}

/// A latest-view iterator over an [`ArenaVersionedArtMap`](crate::ArenaVersionedArtMap):
/// keys whose newest version is a tombstone are skipped.
///
/// Every key present for the whole scan is yielded exactly once, in key order
/// (descending with `rev`). A key inserted or removed during the scan may or
/// may not appear.
pub struct ArenaVersionedRange<'a, K, V> {
    tree: &'a ArenaVersionedTree<K, V>,
    cursor: Cursor<Storage<K, V>>,
}

impl<'a, K, V> ArenaVersionedRange<'a, K, V> {
    pub(crate) fn new(
        tree: &'a ArenaVersionedTree<K, V>,
        start: Bound<KeyBuf>,
        end: Bound<KeyBuf>,
    ) -> Self {
        Self {
            tree,
            cursor: Cursor::new(start, end),
        }
    }
}

impl<'a, K: AsBytes, V> ArenaVersionedRange<'a, K, V> {
    #[inline]
    fn entry(
        &self,
        leaf: NonNull<VersionedLeaf<K, V>>,
    ) -> Option<ArenaVersionedEntryRef<'a, K, V>> {
        // SAFETY: arena leaves are valid for the map borrow; the head is loaded once.
        let head = unsafe { leaf.as_ref() }.head();
        // SAFETY: a leaf and head of this tree, borrowed for `'a`.
        unsafe { ArenaVersionedEntryRef::new(leaf, head, self.tree.arena()) }
    }
}

impl<'a, K: AsBytes, V> Iterator for ArenaVersionedRange<'a, K, V> {
    type Item = ArenaVersionedEntryRef<'a, K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let leaf = self.cursor.next(&self.tree.raw)?;
            if let Some(e) = self.entry(leaf) {
                return Some(e);
            }
        }
    }
}

impl<K: AsBytes, V> DoubleEndedIterator for ArenaVersionedRange<'_, K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            let leaf = self.cursor.next_back(&self.tree.raw)?;
            if let Some(e) = self.entry(leaf) {
                return Some(e);
            }
        }
    }
}

impl<K: AsBytes, V> std::iter::FusedIterator for ArenaVersionedRange<'_, K, V> {}
