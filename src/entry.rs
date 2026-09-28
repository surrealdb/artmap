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

//! # Entry handles (§8.2)

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::ops::Deref;
use std::ptr::NonNull;

use crate::guard::GuardHandle;
use crate::key::AsBytes;
use crate::node::Leaf;
use crate::tree::Tree;

/// A consistent snapshot of one entry of an [`ArtMap`](crate::ArtMap).
///
/// The handle is bounded by a borrow of the map and by an epoch guard, which
/// it owns, shares with its iterator, or borrows from the caller. While it is
/// alive the entry's key and value stay valid and unchanged, even if the entry
/// is removed or replaced in the map meanwhile; [`is_removed`](Self::is_removed)
/// reports whether that happened.
///
/// Holding a handle delays memory reclamation process-wide (see the crate
/// docs). `EntryRef` is neither `Send` nor `Sync`.
pub struct EntryRef<'a, K, V> {
    pub(crate) leaf: NonNull<Leaf<K, V>>,
    pub(crate) tree: &'a Tree<K, V>,
    pub(crate) guard: GuardHandle<'a>,
}

impl<'a, K, V> EntryRef<'a, K, V> {
    #[inline]
    pub(crate) fn new(
        leaf: NonNull<Leaf<K, V>>,
        tree: &'a Tree<K, V>,
        guard: GuardHandle<'a>,
    ) -> Self {
        Self { leaf, tree, guard }
    }

    #[inline]
    fn leaf(&self) -> &Leaf<K, V> {
        // SAFETY: Inv 1 (the leaf is immutable once published) and Inv 2 (the
        // guard and the map borrow keep it alive for `&self`).
        unsafe { self.leaf.as_ref() }
    }

    /// The entry's key.
    #[inline]
    pub fn key(&self) -> &K {
        &self.leaf().key
    }

    /// The entry's value.
    #[inline]
    pub fn value(&self) -> &V {
        &self.leaf().value
    }

    /// `true` once this entry is no longer the live entry for its key: it was
    /// removed or replaced. The handle stays valid either way.
    #[inline]
    pub fn is_removed(&self) -> bool {
        self.leaf().is_removed()
    }

    /// Clones the key and value out of the handle, releasing nothing. Useful
    /// before an `.await` (handles make futures `!Send`).
    #[inline]
    pub fn to_owned(&self) -> (K, V)
    where
        K: Clone,
        V: Clone,
    {
        (self.key().clone(), self.value().clone())
    }

    /// Clones the value out of the handle.
    #[inline]
    pub fn value_cloned(&self) -> V
    where
        V: Clone,
    {
        self.value().clone()
    }
}

impl<K: AsBytes + Send + 'static, V: Send + 'static> EntryRef<'_, K, V> {
    /// Removes this exact entry if it is still the live entry for its key.
    /// Returns `false` if it was already removed or replaced.
    #[inline]
    pub fn remove(&self) -> bool {
        self.tree.remove_leaf(self.leaf, self.guard.guard())
    }
}

impl<K, V> Clone for EntryRef<'_, K, V> {
    /// Shares the protection: a nested pin on the same participant, the
    /// iterator's shared guard, or the caller's borrowed guard.
    ///
    /// # Panics
    /// Cloning an owned handle panics during thread-local destruction, where a
    /// nested pin cannot be proven to protect the entry.
    #[inline]
    fn clone(&self) -> Self {
        Self {
            leaf: self.leaf,
            tree: self.tree,
            guard: self.guard.duplicate(),
        }
    }
}

impl<K, V> Deref for EntryRef<'_, K, V> {
    type Target = V;

    #[inline]
    fn deref(&self) -> &V {
        self.value()
    }
}

impl<K: std::fmt::Debug, V: std::fmt::Debug> std::fmt::Debug for EntryRef<'_, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntryRef")
            .field("key", self.key())
            .field("value", self.value())
            .field("is_removed", &self.is_removed())
            .finish()
    }
}

impl<K: PartialEq, V: PartialEq> PartialEq for EntryRef<'_, K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key() && self.value() == other.value()
    }
}

impl<K: Eq, V: Eq> Eq for EntryRef<'_, K, V> {}
