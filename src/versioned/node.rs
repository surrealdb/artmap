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
use crate::latch::{HybridLatch, WriteGuard};
use crate::raw::LeafNode;
use crate::sync::atomic::{AtomicBool, AtomicPtr, AtomicU8, Ordering};

/// One version of a key. Immutable after publication (Inv 1), except for the
/// `next` link and the `superseded` flag, which only the chain-latch holder
/// writes.
#[repr(C, align(8))]
pub(crate) struct VersionNode<V> {
    pub(crate) version: u64,
    /// `None` is a tombstone.
    pub(crate) value: Option<V>,
    next: AtomicPtr<VersionNode<V>>,
    /// Set by the chain-latch holder when the node is unlinked (superseded or
    /// pruned). Distinct from tombstone state; never a retirement guard.
    superseded: AtomicBool,
    /// A leaf-owned inline slot, never retired independently (D5).
    pub(crate) inline: bool,
}

impl<V> VersionNode<V> {
    fn new(version: u64, value: Option<V>, inline: bool) -> Self {
        Self {
            version,
            value,
            next: AtomicPtr::new(ptr::null_mut()),
            superseded: AtomicBool::new(false),
            inline,
        }
    }

    #[inline]
    pub(crate) fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }

    #[inline]
    pub(crate) fn next(&self) -> *mut VersionNode<V> {
        self.next.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn is_superseded(&self) -> bool {
        self.superseded.load(Ordering::Acquire)
    }

    /// W3: caller holds the owning leaf's chain latch (`_w`).
    #[inline]
    pub(crate) fn set_next(&self, _w: &WriteGuard<'_>, next: *mut VersionNode<V>) {
        self.next.store(next, Ordering::Release);
    }

    /// Builder: the node is not yet published.
    #[inline]
    pub(crate) fn init_next(&self, next: *mut VersionNode<V>) {
        self.next.store(next, Ordering::Relaxed);
    }

    /// Caller holds the owning leaf's chain latch and has unlinked the node.
    #[inline]
    pub(crate) fn mark_superseded(&self, _w: &WriteGuard<'_>) {
        self.superseded.store(true, Ordering::Release);
    }
}

const SLOT0: u8 = 0b01;
const SLOT1: u8 = 0b10;

/// A key and its version chain, newest first.
#[repr(C, align(8))]
pub(crate) struct VersionedLeaf<K, V> {
    pub(crate) key: K,
    /// Serialises every writer of `head` and of any `next` in this chain.
    /// Terminal in the lock order (Inv 7).
    pub(crate) chain_latch: HybridLatch,
    head: AtomicPtr<VersionNode<V>>,
    slot0: UnsafeCell<MaybeUninit<VersionNode<V>>>,
    slot1: UnsafeCell<MaybeUninit<VersionNode<V>>>,
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
            chain_latch: HybridLatch::new(),
            head: AtomicPtr::new(ptr::null_mut()),
            slot0: UnsafeCell::new(MaybeUninit::new(VersionNode::new(version, value, true))),
            slot1: UnsafeCell::new(MaybeUninit::uninit()),
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
    pub(crate) fn set_head(&self, w: &WriteGuard<'_>, node: *mut VersionNode<V>) {
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
        w: &WriteGuard<'_>,
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
            let value = ptr::read(&(*slot0).value);
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
            let (next, inline) = unsafe { ((*at).next.load(Ordering::Relaxed), (*at).inline) };
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
            let w = l.chain_latch.lock().unwrap();
            let v20 = l.alloc_version(&w, 20, Some(D(Arc::clone(&n))));
            // SAFETY: freshly allocated.
            assert!(unsafe { (*v20).inline }, "second version takes slot1");
            let v30 = l.alloc_version(&w, 30, Some(D(Arc::clone(&n))));
            // SAFETY: freshly allocated.
            let spilled = !unsafe { (*v30).inline };
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
