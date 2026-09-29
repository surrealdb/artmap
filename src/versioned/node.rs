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

//! # Versioned leaf and version node (§11.2–11.3)

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr::{self, NonNull};

use crate::key::AsBytes;
use crate::latch::{ChainGuard, ChainLock};
use crate::raw::LeafNode;
use crate::sync::atomic::{AtomicPtr, AtomicU8, Ordering};

/// Flags kept in the low bits of `VersionNode::next` (nodes are 8-aligned).
/// Set by the chain-latch holder when the node is unlinked (superseded or
/// pruned). Distinct from tombstone state; never a retirement guard.
const SUPERSEDED: usize = 0b001;
/// This version is a deletion and has no value. Immutable.
const TOMBSTONE: usize = 0b010;
/// A leaf-owned inline slot, never retired independently (D5). Immutable.
const INLINE: usize = 0b100;
const FLAGS: usize = SUPERSEDED | TOMBSTONE | INLINE;

/// One version of a key. Immutable after publication (Inv 1), except for the
/// `next` link and the superseded flag, which only the chain-latch holder
/// writes.
#[repr(C, align(8))]
pub(crate) struct VersionNode<V> {
    pub(crate) version: u64,
    /// The next older version, tagged with this node's flags in its three
    /// low bits. Packing the flags here, and the tombstone flag instead of an
    /// `Option<V>` discriminant, keeps a node at `16 + size_of::<V>()` bytes.
    next: AtomicPtr<VersionNode<V>>,
    /// Initialised if and only if the node is not a tombstone. Immutable
    /// after publication.
    value: MaybeUninit<V>,
}

impl<V> VersionNode<V> {
    fn new(version: u64, value: Option<V>, inline: bool) -> Self {
        let (value, tombstone) = match value {
            Some(v) => (MaybeUninit::new(v), false),
            None => (MaybeUninit::uninit(), true),
        };
        let flags = if tombstone { TOMBSTONE } else { 0 } | if inline { INLINE } else { 0 };
        Self {
            version,
            next: AtomicPtr::new(ptr::without_provenance_mut(flags)),
            value,
        }
    }

    /// The immutable flags (tombstone, inline) need no ordering.
    #[inline]
    fn flags(&self) -> usize {
        self.next.load(Ordering::Relaxed).addr() & FLAGS
    }

    #[inline]
    pub(crate) fn is_tombstone(&self) -> bool {
        self.flags() & TOMBSTONE != 0
    }

    #[inline]
    pub(crate) fn is_inline(&self) -> bool {
        self.flags() & INLINE != 0
    }

    /// The value, or `None` for a tombstone.
    #[inline]
    pub(crate) fn value(&self) -> Option<&V> {
        // SAFETY: `value` is initialised whenever the node is not a tombstone,
        // and neither changes after publication (Inv 1).
        (!self.is_tombstone()).then(|| unsafe { self.value.assume_init_ref() })
    }

    #[inline]
    pub(crate) fn next(&self) -> *mut VersionNode<V> {
        self.next.load(Ordering::Acquire).map_addr(|a| a & !FLAGS)
    }

    #[inline]
    pub(crate) fn is_superseded(&self) -> bool {
        self.next.load(Ordering::Acquire).addr() & SUPERSEDED != 0
    }

    /// Stores `next`, keeping this node's flags. Only one thread writes `next`
    /// at a time (the chain-latch holder, or the builder), so the read and the
    /// store need no read-modify-write.
    #[inline]
    fn store_next(&self, next: *mut VersionNode<V>, order: Ordering) {
        debug_assert_eq!(next.addr() & FLAGS, 0, "version nodes are 8-aligned");
        let flags = self.next.load(Ordering::Relaxed).addr() & FLAGS;
        self.next.store(next.map_addr(|a| a | flags), order);
    }

    /// W3: caller holds the owning leaf's chain latch (`_w`).
    #[inline]
    pub(crate) fn set_next(&self, _w: &ChainGuard<'_>, next: *mut VersionNode<V>) {
        self.store_next(next, Ordering::Release);
    }

    /// Builder: the node is not yet published.
    #[inline]
    pub(crate) fn init_next(&self, next: *mut VersionNode<V>) {
        self.store_next(next, Ordering::Relaxed);
    }

    /// Caller holds the owning leaf's chain latch and has unlinked the node.
    #[inline]
    pub(crate) fn mark_superseded(&self, _w: &ChainGuard<'_>) {
        let cur = self.next.load(Ordering::Relaxed);
        self.next
            .store(cur.map_addr(|a| a | SUPERSEDED), Ordering::Release);
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

const SLOT0: u8 = 0b01;
const SLOT1: u8 = 0b10;

/// A key and its version chain, newest first.
#[repr(C, align(8))]
pub(crate) struct VersionedLeaf<K, V> {
    // Readers touch `key`, `head` and (usually) `slot0`, so they come first;
    // the writer-only bytes share the last word.
    pub(crate) key: K,
    head: AtomicPtr<VersionNode<V>>,
    slot0: UnsafeCell<MaybeUninit<VersionNode<V>>>,
    slot1: UnsafeCell<MaybeUninit<VersionNode<V>>>,
    /// Serialises every writer of `head` and of any `next` in this chain.
    /// Terminal in the lock order (Inv 7).
    pub(crate) chain_latch: ChainLock,
    /// Which inline slots are initialised. Set by the chain-latch holder (or
    /// the creator) before publication; never cleared while shared.
    slots_init: AtomicU8,
}

// SAFETY: the leaf owns `K` and its versions' `V`s; the inline slots are
// written only before publication or by the chain-latch holder, and read
// through shared references afterwards.
unsafe impl<K: Send, V: Send> Send for VersionedLeaf<K, V> {}
// SAFETY: shared access hands out `&K`/`&V` (needs `Sync`) and chain writers on
// any thread drop superseded values through EBR (needs `Send`).
unsafe impl<K: Sync + Send, V: Sync + Send> Sync for VersionedLeaf<K, V> {}

impl<K, V> VersionedLeaf<K, V> {
    /// A new leaf whose only version lives in `slot0`.
    pub(crate) fn new_boxed(key: K, version: u64, value: Option<V>) -> NonNull<Self> {
        let leaf = Box::into_raw(Box::new(Self {
            key,
            head: AtomicPtr::new(ptr::null_mut()),
            slot0: UnsafeCell::new(MaybeUninit::new(VersionNode::new(version, value, true))),
            slot1: UnsafeCell::new(MaybeUninit::uninit()),
            chain_latch: ChainLock::new(),
            slots_init: AtomicU8::new(SLOT0),
        }));
        // Every pointer into the leaf, including the self-referential head,
        // is derived from the one raw pointer, so none invalidates another.
        // SAFETY: `leaf` is a fresh, live Box allocation.
        unsafe {
            let head = UnsafeCell::raw_get(ptr::addr_of!((*leaf).slot0)).cast::<VersionNode<V>>();
            (*leaf).head.store(head, Ordering::Relaxed);
            NonNull::new_unchecked(leaf)
        }
    }

    /// The newest version (Acquire). Never null.
    #[inline]
    pub(crate) fn head(&self) -> &VersionNode<V> {
        // SAFETY: `head` always points at a live node of this chain: an inline
        // slot owned by the leaf, or a heap node that is retired only after
        // being unlinked (protected by the caller's guard or map borrow).
        unsafe { &*self.head.load(Ordering::Acquire) }
    }

    /// W3: caller holds the chain latch.
    #[inline]
    pub(crate) fn set_head(&self, w: &ChainGuard<'_>, node: *mut VersionNode<V>) {
        debug_assert!(w.holds(&self.chain_latch));
        self.head.store(node, Ordering::Release);
    }

    /// The raw head pointer (for identity re-checks under the latch).
    #[inline]
    pub(crate) fn head_ptr(&self) -> *mut VersionNode<V> {
        self.head.load(Ordering::Acquire)
    }

    /// Allocates an unpublished version node: the free inline slot if any,
    /// otherwise a heap node. Caller holds the chain latch.
    ///
    /// Never runs user code: `value` is moved, not dropped, on every path.
    pub(crate) fn alloc_version(
        &self,
        w: &ChainGuard<'_>,
        version: u64,
        value: Option<V>,
    ) -> *mut VersionNode<V> {
        debug_assert!(w.holds(&self.chain_latch));
        if self.slots_init.load(Ordering::Relaxed) & SLOT1 == 0 {
            let p = self.slot1.get().cast::<VersionNode<V>>();
            // SAFETY: slot1 is uninitialised and unreachable by readers until
            // it is linked; the chain latch excludes other writers.
            unsafe { p.write(VersionNode::new(version, value, true)) };
            self.slots_init.store(
                self.slots_init.load(Ordering::Relaxed) | SLOT1,
                Ordering::Relaxed,
            );
            p
        } else {
            let mut b = Box::<VersionNode<V>>::new_uninit();
            b.write(VersionNode::new(version, value, false));
            // SAFETY: initialised just above.
            Box::into_raw(unsafe { b.assume_init() })
        }
    }

    /// Moves the only version's value out of a never-published leaf, so that
    /// the husk can be freed without dropping it.
    ///
    /// # Safety
    /// The leaf was created by `new_boxed`, was never published, and is
    /// exclusively owned by the caller.
    pub(crate) unsafe fn take_first(this: NonNull<Self>) -> (u64, Option<V>) {
        // SAFETY: per the contract, slot0 is initialised and exclusively ours.
        unsafe {
            let leaf = this.as_ptr();
            let slot0 = (*leaf).slot0.get().cast::<VersionNode<V>>();
            let version = (*slot0).version;
            // Moved out: clearing `slots_init` stops the leaf dropping slot0.
            let value = if (*slot0).is_tombstone() {
                None
            } else {
                Some(ptr::read((*slot0).value.as_ptr()))
            };
            (*leaf).slots_init.store(0, Ordering::Relaxed);
            (version, value)
        }
    }
}

impl<K, V> Drop for VersionedLeaf<K, V> {
    fn drop(&mut self) {
        // The live chain's heap nodes belong to the leaf; detached heap nodes
        // were retired through EBR. Inline slots are dropped via `slots_init`.
        let slot0 = self.slot0.get().cast::<VersionNode<V>>();
        let slot1 = self.slot1.get().cast::<VersionNode<V>>();
        let mut cur = self.head.load(Ordering::Relaxed);
        while !cur.is_null() {
            // A pointer into our own inline slots was derived when the leaf was
            // built; re-derive it from `&mut self` so no stale tag is used
            // while `self` is uniquely borrowed (Tree Borrows).
            let at = if cur.addr() == slot0.addr() {
                slot0
            } else if cur.addr() == slot1.addr() {
                slot1
            } else {
                cur
            };
            // SAFETY: exclusive access; `at` is a live node of this chain.
            let (next, inline) = unsafe { ((*at).next(), (*at).is_inline()) };
            if !inline {
                // SAFETY: a heap node owned by the live chain, freed once.
                drop(unsafe { Box::from_raw(cur) });
            }
            cur = next;
        }
        let init = self.slots_init.load(Ordering::Relaxed);
        if init & SLOT0 != 0 {
            // SAFETY: initialised, exclusively owned, dropped once.
            unsafe { self.slot0.get_mut().assume_init_drop() };
        }
        if init & SLOT1 != 0 {
            // SAFETY: as above.
            unsafe { self.slot1.get_mut().assume_init_drop() };
        }
    }
}

impl<K: AsBytes, V> LeafNode for VersionedLeaf<K, V> {
    #[inline(always)]
    fn key_bytes(&self) -> &[u8] {
        self.key.as_bytes()
    }

    /// Versioned leaves are never unlinked before Phase 8, so they carry no
    /// `removed` flag; tombstones express deletion.
    #[inline]
    fn mark_removed(&self) {}
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    struct D(Arc<AtomicUsize>);
    impl Drop for D {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Rewrite of `test_versioned_leaf_inline_slots_drop_safety`: slot0,
    /// slot1 and a heap node, linked, are each dropped exactly once by the
    /// leaf's own destructor.
    #[test]
    fn inline_slots_and_heap_nodes_drop_once() {
        let n = Arc::new(AtomicUsize::new(0));
        let leaf = VersionedLeaf::<String, D>::new_boxed("k".into(), 10, Some(D(Arc::clone(&n))));
        {
            // SAFETY: exclusively owned, never published.
            let l = unsafe { leaf.as_ref() };
            let w = l.chain_latch.lock();
            let v20 = l.alloc_version(&w, 20, Some(D(Arc::clone(&n))));
            // SAFETY: freshly allocated.
            assert!(unsafe { (*v20).is_inline() }, "second version takes slot1");
            let v30 = l.alloc_version(&w, 30, Some(D(Arc::clone(&n))));
            // SAFETY: freshly allocated.
            let spilled = !unsafe { (*v30).is_inline() };
            assert!(spilled, "third version spills to the heap");
            // SAFETY: both unpublished until `set_head`.
            unsafe {
                (*v20).init_next(l.head_ptr());
                (*v30).init_next(v20);
            }
            l.set_head(&w, v30);
            drop(w);
            assert_eq!(l.head().version, 30);
        }
        assert_eq!(n.load(Ordering::SeqCst), 0);
        // SAFETY: a Box allocation, exclusively owned.
        drop(unsafe { Box::from_raw(leaf.as_ptr()) });
        assert_eq!(
            n.load(Ordering::SeqCst),
            3,
            "every value dropped exactly once"
        );
    }

    /// Rewrite of `test_version_node_value_taken_guard`: moving the first
    /// version out of an unpublished leaf leaves nothing for the leaf to drop.
    #[test]
    fn take_first_moves_the_value_out() {
        let n = Arc::new(AtomicUsize::new(0));
        let leaf = VersionedLeaf::<String, D>::new_boxed("k".into(), 7, Some(D(Arc::clone(&n))));
        // SAFETY: never published, exclusively owned.
        let (v, value) = unsafe { VersionedLeaf::take_first(leaf) };
        assert_eq!(v, 7);
        // SAFETY: a Box allocation, exclusively owned.
        drop(unsafe { Box::from_raw(leaf.as_ptr()) });
        assert_eq!(
            n.load(Ordering::SeqCst),
            0,
            "the husk does not drop the moved value"
        );
        drop(value);
        assert_eq!(n.load(Ordering::SeqCst), 1);
    }

    /// Flags in the low bits of `next`, and a tombstone flag instead of
    /// `Option<V>`: 24 bytes for a `u64` value (the leaf holds two inline
    /// nodes).
    #[test]
    #[cfg(target_pointer_width = "64")]
    fn version_nodes_stay_compact() {
        assert_eq!(std::mem::size_of::<VersionNode<u64>>(), 24);
        assert_eq!(std::mem::size_of::<VersionedLeaf<[u8; 8], u64>>(), 72);
    }

    /// The flags survive every `next` update and never leak into the link.
    #[test]
    fn flags_survive_next_updates() {
        let leaf = VersionedLeaf::<u8, u8>::new_boxed(1, 1, None);
        // SAFETY: exclusively owned, never published.
        let l = unsafe { leaf.as_ref() };
        let head = l.head();
        assert!(head.is_tombstone() && head.is_inline() && !head.is_superseded());
        assert!(head.next().is_null());
        let w = l.chain_latch.lock();
        let v2 = l.alloc_version(&w, 2, Some(7));
        // SAFETY: freshly allocated, unpublished.
        let n = unsafe { &*v2 };
        assert!(!n.is_tombstone() && n.is_inline());
        n.init_next(l.head_ptr());
        assert_eq!(n.next(), l.head_ptr(), "the link is untagged");
        head.mark_superseded(&w);
        assert!(head.is_superseded() && head.is_tombstone() && head.is_inline());
        assert!(head.next().is_null());
        n.set_next(&w, std::ptr::null_mut());
        assert!(!n.is_tombstone() && n.is_inline() && !n.is_superseded());
        assert_eq!(n.value(), Some(&7));
        drop(w);
        // SAFETY: a Box allocation, exclusively owned.
        drop(unsafe { Box::from_raw(leaf.as_ptr()) });
    }

    #[test]
    fn tombstones_are_values_of_none() {
        let leaf = VersionedLeaf::<u8, u8>::new_boxed(1, 1, None);
        // SAFETY: exclusively owned.
        let l = unsafe { leaf.as_ref() };
        assert!(l.head().is_tombstone());
        assert!(!l.head().is_superseded());
        // SAFETY: a Box allocation, exclusively owned.
        drop(unsafe { Box::from_raw(leaf.as_ptr()) });
    }
}
