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

//! # Entry Reference
//!
//! Provides [`EntryRef`], an ergonomic reference to an entry in the map
//! matching `crossbeam-skiplist::map::Entry` conventions.

use std::ops::Deref;
use std::sync::atomic::Ordering;

use crate::key::AsBytes;
use crate::node::Leaf;
use crate::tree::Tree;

/// A reference to an entry in an [`ArtMap`](crate::ArtMap).
pub struct EntryRef<'a, K: AsBytes + Send + 'static, V: Send + 'static> {
    pub(crate) leaf_ptr: *mut Leaf<K, V>,
    pub(crate) tree: &'a Tree<K, V>,
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> Clone for EntryRef<'a, K, V> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            leaf_ptr: self.leaf_ptr,
            tree: self.tree,
        }
    }
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> EntryRef<'a, K, V> {
    /// Returns a reference to the entry's key.
    #[inline]
    pub fn key(&self) -> &'a K {
        unsafe { &(*self.leaf_ptr).key }
    }

    /// Returns a reference to the entry's value.
    #[inline]
    pub fn value(&self) -> &'a V {
        unsafe { &(*self.leaf_ptr).value }
    }

    /// Checks if this entry has been removed from the map.
    #[inline]
    pub fn is_removed(&self) -> bool {
        unsafe { (*self.leaf_ptr).removed.load(Ordering::Acquire) }
    }

    /// Removes this entry from the map.
    #[inline]
    pub fn remove(&self) -> bool {
        let guard = &crossbeam_epoch::pin();
        self.tree.remove_leaf(self.leaf_ptr, guard)
    }
}

impl<'a, K: AsBytes + Send + 'static + std::fmt::Debug, V: Send + 'static + std::fmt::Debug>
    std::fmt::Debug for EntryRef<'a, K, V>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntryRef")
            .field("key", self.key())
            .field("value", self.value())
            .field("is_removed", &self.is_removed())
            .finish()
    }
}

impl<'a, K: AsBytes + Send + 'static + PartialEq, V: Send + 'static + PartialEq> PartialEq
    for EntryRef<'a, K, V>
{
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key() && self.value() == other.value()
    }
}

impl<'a, K: AsBytes + Send + 'static + Eq, V: Send + 'static + Eq> Eq for EntryRef<'a, K, V> {}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> Deref for EntryRef<'a, K, V> {
    type Target = V;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.value()
    }
}
