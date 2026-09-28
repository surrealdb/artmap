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

//! # Iterators over an [`ArtMap`](crate::ArtMap) (§8.5)
//!
//! All iterators are double-ended and validated (§10.3): every key present
//! for the whole scan is yielded exactly once, in order. A key inserted or
//! removed during the scan may or may not appear. Items are consistent
//! snapshots of one entry.
//!
//! Owned iterators (`iter`, `range`, `keys`, `values`) hold one pin, shared
//! with every item they yield. The `*_with_guard` iterators borrow the
//! caller's [`Guard`](crate::Guard) instead and allocate nothing.

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::ops::{Bound, Deref};
use std::rc::Rc;

use crate::entry::EntryRef;
use crate::guard::{pin_tagged, GuardHandle};
use crate::key::AsBytes;
use crate::raw::cursor::{Cursor, KeyBuf};
use crate::tree::{Storage, Tree};

/// An iterator over a range of entries of an [`ArtMap`](crate::ArtMap), in
/// ascending key order (or descending, with `rev`).
pub struct Range<'a, K, V> {
    tree: &'a Tree<K, V>,
    guard: GuardHandle<'a>,
    cursor: Cursor<Storage<K, V>>,
}

impl<'a, K, V> Range<'a, K, V> {
    /// An owned iterator: one pin shared by the iterator and its items.
    pub(crate) fn owned(tree: &'a Tree<K, V>, start: Bound<KeyBuf>, end: Bound<KeyBuf>) -> Self {
        let (g, id) = pin_tagged();
        Self {
            tree,
            guard: GuardHandle::Shared(Rc::new(g), id),
            cursor: Cursor::new(start, end),
        }
    }

    /// A borrowed iterator: items borrow the caller's guard.
    pub(crate) fn borrowed(
        tree: &'a Tree<K, V>,
        guard: &'a crossbeam_epoch::Guard,
        start: Bound<KeyBuf>,
        end: Bound<KeyBuf>,
    ) -> Self {
        Self {
            tree,
            guard: GuardHandle::Borrowed(guard),
            cursor: Cursor::new(start, end),
        }
    }
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> Iterator for Range<'a, K, V> {
    type Item = EntryRef<'a, K, V>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let leaf = self.cursor.next(&self.tree.raw)?;
        Some(EntryRef::new(leaf, self.tree, self.guard.duplicate()))
    }
}

impl<K: AsBytes + Send + 'static, V: Send + 'static> DoubleEndedIterator for Range<'_, K, V> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        let leaf = self.cursor.next_back(&self.tree.raw)?;
        Some(EntryRef::new(leaf, self.tree, self.guard.duplicate()))
    }
}

impl<K: AsBytes + Send + 'static, V: Send + 'static> std::iter::FusedIterator for Range<'_, K, V> {}

/// An iterator over all entries of an [`ArtMap`](crate::ArtMap).
pub type Iter<'a, K, V> = Range<'a, K, V>;

/// A key yielded by [`Keys`]: a handle that dereferences to the key.
pub struct KeyRef<'a, K, V>(EntryRef<'a, K, V>);

impl<'a, K, V> KeyRef<'a, K, V> {
    /// The underlying entry.
    #[inline]
    pub fn entry(&self) -> &EntryRef<'a, K, V> {
        &self.0
    }
}

impl<K, V> Deref for KeyRef<'_, K, V> {
    type Target = K;
    #[inline]
    fn deref(&self) -> &K {
        self.0.key()
    }
}

impl<K: std::fmt::Debug, V> std::fmt::Debug for KeyRef<'_, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.key().fmt(f)
    }
}

/// A value yielded by [`Values`]: a handle that dereferences to the value.
pub struct ValueRef<'a, K, V>(EntryRef<'a, K, V>);

impl<'a, K, V> ValueRef<'a, K, V> {
    /// The underlying entry.
    #[inline]
    pub fn entry(&self) -> &EntryRef<'a, K, V> {
        &self.0
    }
}

impl<K, V> Deref for ValueRef<'_, K, V> {
    type Target = V;
    #[inline]
    fn deref(&self) -> &V {
        self.0.value()
    }
}

impl<K, V: std::fmt::Debug> std::fmt::Debug for ValueRef<'_, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.value().fmt(f)
    }
}

/// An iterator over the keys of an [`ArtMap`](crate::ArtMap).
pub struct Keys<'a, K, V>(pub(crate) Range<'a, K, V>);

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> Iterator for Keys<'a, K, V> {
    type Item = KeyRef<'a, K, V>;
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(KeyRef)
    }
}

impl<K: AsBytes + Send + 'static, V: Send + 'static> DoubleEndedIterator for Keys<'_, K, V> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(KeyRef)
    }
}

/// An iterator over the values of an [`ArtMap`](crate::ArtMap).
pub struct Values<'a, K, V>(pub(crate) Range<'a, K, V>);

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> Iterator for Values<'a, K, V> {
    type Item = ValueRef<'a, K, V>;
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(ValueRef)
    }
}

impl<K: AsBytes + Send + 'static, V: Send + 'static> DoubleEndedIterator for Values<'_, K, V> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(ValueRef)
    }
}

/// Keys borrowed through a caller's [`Guard`](crate::Guard): bare `&'a K`,
/// bounded by both the map borrow and the guard borrow.
pub struct GuardKeys<'a, K, V>(pub(crate) Range<'a, K, V>);

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> Iterator for GuardKeys<'a, K, V> {
    type Item = &'a K;
    #[inline]
    fn next(&mut self) -> Option<&'a K> {
        let leaf = self.0.cursor.next(&self.0.tree.raw)?;
        // SAFETY: the caller's guard (borrowed for `'a`) and the map borrow
        // (`'a`) keep the leaf alive and unchanged for `'a` (Inv 1, Inv 2).
        Some(unsafe { &(*leaf.as_ptr()).key })
    }
}

/// Values borrowed through a caller's [`Guard`](crate::Guard): bare `&'a V`.
pub struct GuardValues<'a, K, V>(pub(crate) Range<'a, K, V>);

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> Iterator for GuardValues<'a, K, V> {
    type Item = &'a V;
    #[inline]
    fn next(&mut self) -> Option<&'a V> {
        let leaf = self.0.cursor.next(&self.0.tree.raw)?;
        // SAFETY: as for `GuardKeys`.
        Some(unsafe { &(*leaf.as_ptr()).value })
    }
}
