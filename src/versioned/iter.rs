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

//! # Latest-view iterator over a [`VersionedArtMap`](crate::VersionedArtMap)

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::ops::Bound;
use std::ptr::NonNull;
use std::rc::Rc;

use crate::guard::{pin_tagged, GuardHandle};
use crate::key::AsBytes;
use crate::raw::cursor::{Cursor, KeyBuf};
use crate::versioned::entry::VersionedEntryRef;
use crate::versioned::tree::{Storage, VersionedTree};

/// An iterator over the latest live version of each key in a range.
///
/// A key whose newest version is a tombstone is skipped. Each item captures
/// the head it saw, once. Filtering this iterator with `get_version_le` is not
/// a snapshot scan.
///
/// Every key present for the whole scan is yielded exactly once, in key order
/// (descending with `rev`). A key inserted or removed during the scan may or
/// may not appear.
pub struct Range<'a, K, V> {
    tree: &'a VersionedTree<K, V>,
    guard: GuardHandle<'a>,
    cursor: Cursor<Storage<K, V>>,
}

impl<'a, K, V> Range<'a, K, V> {
    pub(crate) fn owned(
        tree: &'a VersionedTree<K, V>,
        start: Bound<KeyBuf>,
        end: Bound<KeyBuf>,
    ) -> Self {
        let (g, id) = pin_tagged();
        Self {
            tree,
            guard: GuardHandle::Shared(Rc::new(g), id),
            cursor: Cursor::new(start, end),
        }
    }

    pub(crate) fn borrowed(
        tree: &'a VersionedTree<K, V>,
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
    type Item = VersionedEntryRef<'a, K, V>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let leaf = self.cursor.next(&self.tree.raw)?;
            // SAFETY: protected by the iterator's guard. The head is loaded once.
            let head = unsafe { leaf.as_ref() }.head();
            // Checked before the guard is shared, and the item is built in
            // place: building it through `VersionedEntryRef::new` spilled the
            // guard handle to the stack and reloaded it wider, a
            // store-forwarding stall on every item.
            if !head.is_tombstone() {
                return Some(VersionedEntryRef {
                    leaf,
                    node: NonNull::from(head),
                    tree: self.tree,
                    guard: self.guard.duplicate(),
                });
            }
        }
    }
}

impl<K: AsBytes + Send + 'static, V: Send + 'static> DoubleEndedIterator for Range<'_, K, V> {
    #[inline(always)]
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            let leaf = self.cursor.next_back(&self.tree.raw)?;
            // SAFETY: as for `next`.
            let head = unsafe { leaf.as_ref() }.head();
            // Checked before the guard is shared, and the item is built in
            // place: building it through `VersionedEntryRef::new` spilled the
            // guard handle to the stack and reloaded it wider, a
            // store-forwarding stall on every item.
            if !head.is_tombstone() {
                return Some(VersionedEntryRef {
                    leaf,
                    node: NonNull::from(head),
                    tree: self.tree,
                    guard: self.guard.duplicate(),
                });
            }
        }
    }
}

impl<K: AsBytes + Send + 'static, V: Send + 'static> std::iter::FusedIterator for Range<'_, K, V> {}
