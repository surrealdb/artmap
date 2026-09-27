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

//! # Versioned Entry Reference
//!
//! Provides [`VersionedEntryRef`], an ergonomic reference to a versioned entry in a [`VersionedArtMap`](crate::versioned::VersionedArtMap).

use std::ops::Deref;
use std::sync::atomic::Ordering;

use crate::key::AsBytes;
use crate::node::VersionedLeaf;

/// A reference to a versioned entry in a [`VersionedArtMap`](crate::versioned::VersionedArtMap).
pub struct VersionedEntryRef<'a, K: AsBytes + Send + 'static, V: Send + 'static> {
    pub(crate) leaf_ptr: *mut VersionedLeaf<K, V>,
    pub(crate) _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> Clone for VersionedEntryRef<'a, K, V> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> Copy for VersionedEntryRef<'a, K, V> {}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> VersionedEntryRef<'a, K, V> {
    /// Returns a reference to the entry's key.
    #[inline]
    pub fn key(&self) -> &'a K {
        unsafe { &(*self.leaf_ptr).key }
    }

    /// Returns a reference to the head (latest) version's value.
    #[inline]
    pub fn value(&self) -> &'a V {
        unsafe {
            let head = (*self.leaf_ptr).versions.load(Ordering::Acquire);
            debug_assert!(!head.is_null());
            &(*head).value
        }
    }

    /// Returns the head entry's monotonic version number.
    #[inline]
    pub fn version(&self) -> u64 {
        unsafe {
            let head = (*self.leaf_ptr).versions.load(Ordering::Acquire);
            debug_assert!(!head.is_null());
            (*head).version
        }
    }

    /// Checks if this head entry has been marked removed.
    #[inline]
    pub fn is_removed(&self) -> bool {
        unsafe {
            let head = (*self.leaf_ptr).versions.load(Ordering::Acquire);
            if head.is_null() {
                true
            } else {
                (*head).removed.load(Ordering::Acquire)
            }
        }
    }

    /// Returns the newest version and value for this key that is $\le$ `max_version`.
    pub fn get_version_le(&self, max_version: u64) -> Option<(u64, &'a V)> {
        let mut cur = unsafe { (*self.leaf_ptr).versions.load(Ordering::Acquire) };
        while !cur.is_null() {
            let node = unsafe { &*cur };
            if node.version <= max_version {
                if node.removed.load(Ordering::Acquire) {
                    return None;
                }
                return Some((node.version, &*node.value));
            }
            cur = node.next_version.load(Ordering::Acquire);
        }
        None
    }
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> Deref for VersionedEntryRef<'a, K, V> {
    type Target = V;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.value()
    }
}

impl<'a, K: AsBytes + Send + 'static + std::fmt::Debug, V: Send + 'static + std::fmt::Debug>
    std::fmt::Debug for VersionedEntryRef<'a, K, V>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VersionedEntryRef")
            .field("key", self.key())
            .field("version", &self.version())
            .field("value", self.value())
            .field("is_removed", &self.is_removed())
            .finish()
    }
}

impl<'a, K: AsBytes + Send + 'static + PartialEq, V: Send + 'static + PartialEq> PartialEq
    for VersionedEntryRef<'a, K, V>
{
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
            && self.value() == other.value()
            && self.version() == other.version()
    }
}
