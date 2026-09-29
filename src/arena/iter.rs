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

//! # Entries and iterators of an [`ArenaArtMap`](crate::ArenaArtMap)

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::marker::PhantomData;
use std::ops::{Bound, Deref};
use std::ptr::NonNull;

use crate::arena::node::Leaf;
use crate::arena::tree::{ArenaTree, Storage};
use crate::key::AsBytes;
use crate::raw::cursor::{Cursor, KeyBuf};

/// An entry of an [`ArenaArtMap`](crate::ArenaArtMap), valid and unchanged for
/// the whole map borrow (arena memory is never reused while the map is alive).
///
/// Neither `Send` nor `Sync`.
pub struct ArenaEntryRef<'a, K, V> {
    leaf: NonNull<Leaf<K, V>>,
    _map: PhantomData<&'a ArenaTree<K, V>>,
}

impl<K, V> Clone for ArenaEntryRef<'_, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K, V> Copy for ArenaEntryRef<'_, K, V> {}

impl<'a, K, V> ArenaEntryRef<'a, K, V> {
    /// # Safety
    /// `leaf` is a leaf of the tree borrowed for `'a`.
    #[inline]
    pub(crate) unsafe fn new(leaf: NonNull<Leaf<K, V>>) -> Self {
        Self {
            leaf,
            _map: PhantomData,
        }
    }

    #[inline]
    fn leaf(self) -> &'a Leaf<K, V> {
        // SAFETY: Inv 1 and Inv 2: arena leaves are immutable once published
        // and live, unchanged, for the whole map borrow `'a`.
        unsafe { self.leaf.as_ref() }
    }

    /// The entry's key, valid for the map borrow.
    #[inline]
    pub fn key(&self) -> &'a K {
        &self.leaf().key
    }

    /// The entry's value, valid for the map borrow.
    #[inline]
    pub fn value(&self) -> &'a V {
        &self.leaf().value
    }

    /// `true` once the entry was removed or replaced in the map.
    #[inline]
    pub fn is_removed(&self) -> bool {
        self.leaf().is_removed()
    }
}

impl<K, V> Deref for ArenaEntryRef<'_, K, V> {
    type Target = V;
    #[inline]
    fn deref(&self) -> &V {
        self.value()
    }
}

impl<K: std::fmt::Debug, V: std::fmt::Debug> std::fmt::Debug for ArenaEntryRef<'_, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArenaEntryRef")
            .field("key", self.key())
            .field("value", self.value())
            .field("is_removed", &self.is_removed())
            .finish()
    }
}

impl<K: PartialEq, V: PartialEq> PartialEq for ArenaEntryRef<'_, K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key() && self.value() == other.value()
    }
}

impl<K: Eq, V: Eq> Eq for ArenaEntryRef<'_, K, V> {}

/// An iterator over a range of an [`ArenaArtMap`](crate::ArenaArtMap).
///
/// Every key present for the whole scan is yielded exactly once, in key order
/// (descending with `rev`). A key inserted or removed during the scan may or
/// may not appear.
pub struct Range<'a, K, V> {
    tree: &'a ArenaTree<K, V>,
    cursor: Cursor<Storage<K, V>>,
}

impl<'a, K, V> Range<'a, K, V> {
    pub(crate) fn new(tree: &'a ArenaTree<K, V>, start: Bound<KeyBuf>, end: Bound<KeyBuf>) -> Self {
        Self {
            tree,
            cursor: Cursor::new(start, end),
        }
    }
}

impl<'a, K: AsBytes, V> Iterator for Range<'a, K, V> {
    type Item = ArenaEntryRef<'a, K, V>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let leaf = self.cursor.next(&self.tree.raw)?;
        // SAFETY: a leaf of the tree borrowed for `'a`.
        Some(unsafe { ArenaEntryRef::new(leaf) })
    }
}

impl<K: AsBytes, V> DoubleEndedIterator for Range<'_, K, V> {
    #[inline(always)]
    fn next_back(&mut self) -> Option<Self::Item> {
        let leaf = self.cursor.next_back(&self.tree.raw)?;
        // SAFETY: a leaf of the tree borrowed for `'a`.
        Some(unsafe { ArenaEntryRef::new(leaf) })
    }
}

impl<K: AsBytes, V> std::iter::FusedIterator for Range<'_, K, V> {}
