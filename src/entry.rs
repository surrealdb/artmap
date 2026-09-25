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

use crate::key::AsBytes;
use crate::tree::Tree;

/// A reference to an entry in an [`ArtMap`](crate::ArtMap).
pub struct EntryRef<'a, K: AsBytes + Send + 'static, V: Send + 'static> {
    pub(crate) key_ptr: *const K,
    pub(crate) val_ptr: *const V,
    pub(crate) tree: &'a Tree<K, V>,
    pub(crate) is_removed: bool,
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> EntryRef<'a, K, V> {
    /// Returns a reference to the entry's key.
    #[inline]
    pub fn key(&self) -> &'a K {
        unsafe { &*self.key_ptr }
    }

    /// Returns a reference to the entry's value.
    #[inline]
    pub fn value(&self) -> &'a V {
        unsafe { &*self.val_ptr }
    }

    /// Checks if this entry has been removed from the map.
    #[inline]
    pub fn is_removed(&self) -> bool {
        self.is_removed
    }

    /// Removes this entry from the map.
    pub fn remove(&mut self) -> bool {
        if self.is_removed {
            return false;
        }
        let key = self.key();
        let guard = &crossbeam_epoch::pin();
        let removed = self.tree.remove(key, guard).is_some();
        if removed {
            self.is_removed = true;
        }
        removed
    }
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> Deref for EntryRef<'a, K, V> {
    type Target = V;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.value()
    }
}
