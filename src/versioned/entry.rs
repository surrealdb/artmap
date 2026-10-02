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

//! # Versioned entry handles (§11.6)

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::ops::Deref;
use std::ptr::NonNull;

use crate::guard::GuardHandle;
use crate::versioned::node::{VersionNode, VersionedLeaf};
use crate::versioned::tree::{find_le, VersionedTree};

/// A snapshot of one version of one key of a
/// [`VersionedArtMap`](crate::VersionedArtMap).
///
/// The handle captures a single version node when it is created, so
/// [`value`](Self::value) and [`version`](Self::version) always belong
/// together, whatever writers do meanwhile. Fresh reads are explicit:
/// [`latest`](Self::latest) and [`get_version_le`](Self::get_version_le) walk
/// the current chain.
///
/// Bounded by the map borrow and an epoch guard; neither `Send` nor `Sync`.
pub struct VersionedEntryRef<'a, K, V> {
    pub(crate) leaf: NonNull<VersionedLeaf<K, V>>,
    /// Never a tombstone.
    pub(crate) node: NonNull<VersionNode<V>>,
    pub(crate) tree: &'a VersionedTree<K, V>,
    pub(crate) guard: GuardHandle<'a>,
}

impl<'a, K, V> VersionedEntryRef<'a, K, V> {
    /// A handle on `node`, or `None` if it is a tombstone.
    #[inline]
    pub(crate) fn new(
        leaf: NonNull<VersionedLeaf<K, V>>,
        node: &VersionNode<V>,
        tree: &'a VersionedTree<K, V>,
        guard: GuardHandle<'a>,
    ) -> Option<Self> {
        if node.is_tombstone() {
            return None;
        }
        Some(Self {
            leaf,
            node: NonNull::from(node),
            tree,
            guard,
        })
    }

    #[inline]
    fn leaf(&self) -> &VersionedLeaf<K, V> {
        // SAFETY: Inv 2: the guard and the map borrow keep the leaf alive.
        unsafe { self.leaf.as_ref() }
    }

    #[inline]
    fn node(&self) -> &VersionNode<V> {
        // SAFETY: Inv 1 and 2: the captured node is immutable and protected by
        // the guard (heap nodes) or by the leaf (inline slots).
        unsafe { self.node.as_ref() }
    }

    /// The entry's key.
    #[inline]
    pub fn key(&self) -> &K {
        &self.leaf().key
    }

    /// The captured version's value.
    #[inline]
    pub fn value(&self) -> &V {
        self.node()
            .value()
            .expect("a VersionedEntryRef never captures a tombstone")
    }

    /// The captured version.
    #[inline]
    pub fn version(&self) -> u64 {
        self.node().version
    }

    /// `true` if the captured version was deleted when captured. Always
    /// `false`: handles never capture tombstones.
    #[inline]
    pub fn is_removed(&self) -> bool {
        false
    }

    /// `true` once the captured version has been unlinked: a same-version
    /// insert replaced it, `remove` or `remove_version` removed it, a prune
    /// unlinked it, or its whole key was removed, pruned or cleared.
    #[inline]
    pub fn is_superseded(&self) -> bool {
        self.node().is_superseded() || self.leaf().chain_latch.is_dead()
    }

    /// A fresh read of the key's newest live version, sharing this handle's
    /// protection (no new pin).
    pub fn latest(&self) -> Option<VersionedEntryRef<'_, K, V>> {
        let head = self.leaf().head();
        VersionedEntryRef::new(self.leaf, head, self.tree, self.borrowed())
    }

    /// A fresh walk of the key's chain for the newest version `<= max_version`.
    pub fn get_version_le(&self, max_version: u64) -> Option<(u64, &V)> {
        let n = find_le(self.leaf().head(), max_version)?;
        n.value().map(|v| (n.version, v))
    }

    #[inline]
    fn borrowed(&self) -> GuardHandle<'_> {
        GuardHandle::Borrowed(self.guard.guard())
    }
}

impl<K, V> Clone for VersionedEntryRef<'_, K, V> {
    /// Shares the protection (see [`EntryRef::clone`](crate::EntryRef)).
    fn clone(&self) -> Self {
        Self {
            leaf: self.leaf,
            node: self.node,
            tree: self.tree,
            guard: self.guard.duplicate(),
        }
    }
}

impl<K, V> Deref for VersionedEntryRef<'_, K, V> {
    type Target = V;

    #[inline]
    fn deref(&self) -> &V {
        self.value()
    }
}

impl<K: std::fmt::Debug, V: std::fmt::Debug> std::fmt::Debug for VersionedEntryRef<'_, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VersionedEntryRef")
            .field("key", self.key())
            .field("version", &self.version())
            .field("value", self.value())
            .finish()
    }
}

impl<K: PartialEq, V: PartialEq> PartialEq for VersionedEntryRef<'_, K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
            && self.version() == other.version()
            && self.value() == other.value()
    }
}
