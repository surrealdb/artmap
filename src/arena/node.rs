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

//! # Arena leaf and version types (§12.2)
//!
//! All immutable once published (Inv 1), except for the `removed` and
//! `superseded` flags, the version-chain links (written by the chain-latch
//! holder), and the retired-list links (written before retirement).

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr::NonNull;

use crate::arena::storage::ArenaLeaf;
use crate::arena::Arena;
use crate::key::AsBytes;
use crate::latch::{ChainGuard, ChainLock};
use crate::raw::LeafNode;
use crate::sync::atomic::{AtomicBool, AtomicU32, Ordering};

/// A leaf of an [`ArenaArtMap`](crate::ArenaArtMap).
#[repr(C, align(8))]
pub(crate) struct Leaf<K, V> {
    /// Set by the thread that unlinks or supersedes the leaf.
    pub(crate) removed: AtomicBool,
    next_retired: AtomicU32,
    pub(crate) key: K,
    pub(crate) value: V,
}

impl<K, V> Leaf<K, V> {
    pub(crate) fn new(key: K, value: V) -> Self {
        Self {
            removed: AtomicBool::new(false),
            next_retired: AtomicU32::new(0),
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

// SAFETY: the link is only used by the retired list, and `drop_in_arena`
// drops the key and value once.
unsafe impl<K, V> ArenaLeaf for Leaf<K, V> {
    const NEEDS_DROP: bool = std::mem::needs_drop::<K>() || std::mem::needs_drop::<V>();

    #[inline]
    fn next_retired(&self) -> Option<&AtomicU32> {
        Some(&self.next_retired)
    }

    unsafe fn drop_in_arena(this: NonNull<Self>, _arena: &Arena) {
        // SAFETY: exclusive access, called once; the bytes stay in the arena.
        unsafe { this.drop_in_place() }
    }
}

/// Flags kept in the low bits of `VersionNode::next` (arena offsets of
/// version nodes are 8-aligned). Set by the chain-latch holder when the node
/// is unlinked.
const SUPERSEDED: u32 = 0b01;
/// This version is a deletion and has no value. Immutable.
const TOMBSTONE: u32 = 0b10;
const FLAGS: u32 = SUPERSEDED | TOMBSTONE;

/// One version of a key in an [`ArenaVersionedArtMap`](crate::ArenaVersionedArtMap).
#[repr(C, align(8))]
pub(crate) struct VersionNode<V> {
    pub(crate) version: u64,
    /// Offset of the next older version, or 0, tagged with this node's flags
    /// in its low bits. Written by the chain-latch holder (`Release`); loaded
    /// with `Acquire`.
    next: AtomicU32,
    /// Link of the retired-versions list.
    pub(crate) next_retired: AtomicU32,
    /// Initialised if and only if the node is not a tombstone. Immutable
    /// after publication. A flag instead of `Option<V>` saves the
    /// discriminant's word for values without a niche.
    value: MaybeUninit<V>,
}

impl<V> VersionNode<V> {
    pub(crate) fn new(version: u64, value: Option<V>) -> Self {
        let (value, flags) = match value {
            Some(v) => (MaybeUninit::new(v), 0),
            None => (MaybeUninit::uninit(), TOMBSTONE),
        };
        Self {
            version,
            next: AtomicU32::new(flags),
            next_retired: AtomicU32::new(0),
            value,
        }
    }

    #[inline]
    pub(crate) fn is_tombstone(&self) -> bool {
        // Immutable: no ordering needed.
        self.next.load(Ordering::Relaxed) & TOMBSTONE != 0
    }

    /// The value, or `None` for a tombstone.
    #[inline]
    pub(crate) fn value(&self) -> Option<&V> {
        // SAFETY: `value` is initialised whenever the node is not a tombstone,
        // and neither changes after publication (Inv 1).
        (!self.is_tombstone()).then(|| unsafe { self.value.assume_init_ref() })
    }

    /// Consumes a node that was never published, returning its value.
    pub(crate) fn into_value(self) -> Option<V> {
        let this = std::mem::ManuallyDrop::new(self);
        // SAFETY: initialised when not a tombstone; `this` is never dropped,
        // so the value is moved out exactly once.
        (!this.is_tombstone()).then(|| unsafe { std::ptr::read(this.value.as_ptr()) })
    }

    #[inline]
    pub(crate) fn next(&self) -> u32 {
        self.next.load(Ordering::Acquire) & !FLAGS
    }

    /// Stores `next`, keeping this node's flags. Only one thread writes `next`
    /// at a time (the chain-latch holder, or the builder).
    #[inline]
    fn store_next(&self, next: u32, order: Ordering) {
        debug_assert_eq!(next & FLAGS, 0, "version nodes are 8-aligned");
        let flags = self.next.load(Ordering::Relaxed) & FLAGS;
        self.next.store(next | flags, order);
    }

    /// Builder: the node is not yet published.
    #[inline]
    pub(crate) fn init_next(&self, next: u32) {
        self.store_next(next, Ordering::Relaxed);
    }

    /// W3: caller holds the owning leaf's chain latch (`_w`).
    #[inline]
    pub(crate) fn set_next(&self, _w: &ChainGuard<'_>, next: u32) {
        self.store_next(next, Ordering::Release);
    }

    #[inline]
    pub(crate) fn is_superseded(&self) -> bool {
        self.next.load(Ordering::Acquire) & SUPERSEDED != 0
    }

    /// Caller holds the chain latch and has unlinked the node.
    #[inline]
    pub(crate) fn mark_superseded(&self, _w: &ChainGuard<'_>) {
        let cur = self.next.load(Ordering::Relaxed);
        self.next.store(cur | SUPERSEDED, Ordering::Release);
    }
}

impl<V> Drop for VersionNode<V> {
    fn drop(&mut self) {
        if !self.is_tombstone() {
            // SAFETY: initialised (not a tombstone), dropped exactly once here.
            unsafe { self.value.assume_init_drop() };
        }
    }
}

/// A key and its version chain, newest first.
#[repr(C, align(8))]
pub(crate) struct VersionedLeaf<K, V> {
    /// Offset of the newest version. Never 0 once published.
    head: AtomicU32,
    /// Serialises every chain writer; terminal in the lock order (Inv 7).
    pub(crate) chain_latch: ChainLock,
    pub(crate) key: K,
    _values: PhantomData<V>,
}

impl<K, V> VersionedLeaf<K, V> {
    pub(crate) fn new(key: K, head: u32) -> Self {
        Self {
            head: AtomicU32::new(head),
            chain_latch: ChainLock::new(),
            key,
            _values: PhantomData,
        }
    }

    #[inline]
    pub(crate) fn head(&self) -> u32 {
        self.head.load(Ordering::Acquire)
    }

    /// W3: caller holds the chain latch.
    #[inline]
    pub(crate) fn set_head(&self, w: &ChainGuard<'_>, head: u32) {
        debug_assert!(w.holds(&self.chain_latch));
        self.head.store(head, Ordering::Release);
    }

    /// Detaches the version chain of a never-published leaf, so that dropping
    /// the husk drops only its key.
    ///
    /// # Safety
    /// The leaf was never published and is exclusively owned by the caller.
    #[inline]
    pub(crate) unsafe fn detach_head_unpublished(&self) {
        self.head.store(0, Ordering::Relaxed);
    }
}

impl<K: AsBytes, V> LeafNode for VersionedLeaf<K, V> {
    #[inline(always)]
    fn key_bytes(&self) -> &[u8] {
        self.key.as_bytes()
    }

    /// Versioned leaves are never unlinked; tombstones express deletion.
    #[inline]
    fn mark_removed(&self) {}
}

// SAFETY: versioned leaves are never unlinked (inserts use
// `Mode::InsertIfAbsent`, and there is no remove, prune or clear), so they need
// no retired-list link; `drop_in_arena` drops the key and every version of the
// live chain once. Versions unlinked while the map was alive are on the tree's
// own retired-versions list instead.
unsafe impl<K, V> ArenaLeaf for VersionedLeaf<K, V> {
    const NEEDS_DROP: bool = std::mem::needs_drop::<K>() || std::mem::needs_drop::<V>();

    #[inline]
    fn next_retired(&self) -> Option<&AtomicU32> {
        None
    }

    unsafe fn drop_in_arena(this: NonNull<Self>, arena: &Arena) {
        // SAFETY: exclusive access, called once.
        let mut off = unsafe { this.as_ref() }.head.load(Ordering::Relaxed);
        while off != 0 {
            // SAFETY: a version node of this arena in the live chain.
            let n = unsafe { arena.ptr::<VersionNode<V>>(off) };
            // SAFETY: as above.
            let next = unsafe { n.as_ref() }.next();
            // SAFETY: each live version is dropped exactly once, here.
            unsafe { n.drop_in_place() };
            off = next;
        }
        // SAFETY: the key, once.
        unsafe { std::ptr::addr_of_mut!((*this.as_ptr()).key).drop_in_place() };
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::VersionNode;

    /// Flags in the low bits of `next`, and a tombstone flag instead of
    /// `Option<V>`: 24 bytes for a `u64` value.
    #[test]
    fn version_nodes_stay_compact() {
        assert_eq!(std::mem::size_of::<VersionNode<u64>>(), 24);
        // A one-byte chain lock and no retired-list link.
        assert_eq!(
            std::mem::size_of::<super::VersionedLeaf<[u8; 8], u64>>(),
            16
        );
    }

    #[test]
    fn into_value_moves_the_value_out_once() {
        use std::rc::Rc;
        let v = Rc::new(1);
        let n = VersionNode::new(1, Some(Rc::clone(&v)));
        assert_eq!(n.value().map(|v| **v), Some(1));
        let out = n.into_value().unwrap();
        assert_eq!(Rc::strong_count(&v), 2, "moved, not cloned or dropped");
        drop(out);
        assert_eq!(Rc::strong_count(&v), 1);
        assert!(VersionNode::<Rc<u8>>::new(2, None).into_value().is_none());
    }
}
