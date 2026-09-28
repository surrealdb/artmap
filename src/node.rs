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

//! # Heap leaf type (§6.5)

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use crate::key::AsBytes;
use crate::raw::LeafNode;
use crate::sync::atomic::{AtomicBool, Ordering};

/// A leaf of an [`ArtMap`](crate::ArtMap): one key and its value.
///
/// Immutable once published (Inv 1): replacing a value allocates a new leaf.
/// The leaf is dropped as a whole, by its single EBR destructor or by the
/// tree's synchronous drop. `align(8)` keeps the tag bit free.
#[repr(C, align(8))]
pub(crate) struct Leaf<K, V> {
    /// Set by the thread that unlinks or supersedes this leaf, inside the
    /// critical section. Never cleared.
    pub(crate) removed: AtomicBool,
    pub(crate) key: K,
    pub(crate) value: V,
}

impl<K, V> Leaf<K, V> {
    #[inline]
    pub(crate) fn new(key: K, value: V) -> Self {
        Self {
            removed: AtomicBool::new(false),
            key,
            value,
        }
    }

    #[inline]
    pub(crate) fn is_removed(&self) -> bool {
        self.removed.load(Ordering::Acquire)
    }
}

impl<K: AsBytes, V> LeafNode for Leaf<K, V> {
    #[inline(always)]
    fn key_bytes(&self) -> &[u8] {
        self.key.as_bytes()
    }

    #[inline]
    fn mark_removed(&self) {
        self.removed.store(true, Ordering::Release);
    }
}
