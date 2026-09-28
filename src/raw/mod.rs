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

//! # The shared ART core
//!
//! One implementation of the adaptive radix tree, its latch protocol, its
//! writer and reader algorithms and its cursor, shared by all four maps. It is
//! generic over a [`Storage`], which decides how nodes and leaves are
//! allocated, addressed and reclaimed:
//!
//! - heap storage: boxed nodes, tagged pointers, EBR retirement;
//! - arena storage: bump-allocated nodes, tagged `u32` offsets, a retired list
//!   drained only when the map is dropped.
//!
//! The invariants of the safety plan (§4) are stated once, here, instead of
//! drifting between four copies.

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

pub(crate) mod cursor;
pub(crate) mod heap;
pub(crate) mod node;
pub(crate) mod read;
pub(crate) mod slot;
pub(crate) mod walk;
pub(crate) mod write;

use std::cell::Cell;
use std::ptr::NonNull;

use crate::latch::{CachePadded, HybridLatch, WriteGuard};
use crate::sync::atomic::{AtomicIsize, Ordering};

use node::{Node16, Node256, Node4, Node48, NodeHeader, NodeType};
use slot::AtomicSlot;
#[cfg(loom)]
use slot::Slot;

/// The slot value type of a storage.
pub(crate) type Raw<S> = <<S as Layout>::Atomic as AtomicSlot>::Raw;
/// A pointer to an inner node of a storage.
pub(crate) type NodePtr<S> = NonNull<NodeHeader<<S as Layout>::Atomic>>;

/// A leaf type stored in the tree.
pub(crate) trait LeafNode {
    /// The key bytes. Calls user code (`AsBytes`), so it is never called while
    /// a latch is held (Inv 6) and its result is never trusted by unsafe code
    /// (Inv 10).
    fn key_bytes(&self) -> &[u8];

    /// Sets the leaf's `removed` flag (Release). Called only by the thread that
    /// unlinks or supersedes the leaf, inside the critical section (Inv 1).
    fn mark_removed(&self);
}

/// How a tree allocates, addresses and frees its nodes and leaves.
///
/// This layer has no bounds on the leaf type, so that a tree can always be
/// dropped.
///
/// # Safety
/// Implementations must return, from `node`/`leaf`, pointers with provenance
/// over the whole allocation that `alloc_node` (or the map's leaf allocator)
/// created for that slot value.
pub(crate) unsafe trait Layout {
    type Atomic: AtomicSlot;
    type Leaf;
    /// Allocation failure. Uninhabited for the heap.
    type Full;

    /// # Safety
    /// `raw` is a non-null, non-leaf slot value of this tree whose node is
    /// still protected.
    unsafe fn node(&self, raw: Raw<Self>) -> NodePtr<Self>;

    /// # Safety
    /// `raw` is a non-null leaf slot value of this tree whose leaf is still
    /// protected.
    unsafe fn leaf(&self, raw: Raw<Self>) -> NonNull<Self::Leaf>;

    fn node_raw(&self, n: NodePtr<Self>) -> Raw<Self>;
    fn leaf_raw(&self, l: NonNull<Self::Leaf>) -> Raw<Self>;

    /// Allocates a fresh, unpublished node of type `ty`.
    fn alloc_node(&self, ty: NodeType) -> Result<NodePtr<Self>, Self::Full>;

    /// Frees a node that was never published, or that the caller owns
    /// exclusively (tree drop).
    ///
    /// # Safety
    /// As stated; the node is not freed twice.
    unsafe fn free_node(&self, n: NodePtr<Self>);

    /// Drops a leaf the caller owns exclusively: a never-published leaf, or a
    /// live leaf during tree drop.
    ///
    /// # Safety
    /// As stated; the leaf is not freed twice.
    unsafe fn free_leaf(&self, l: NonNull<Self::Leaf>);
}

/// Reclamation of unlinked nodes and leaves.
///
/// # Safety
/// Implementations must never reuse memory that a reader protected by `Guard`
/// (heap) or by the map borrow (arena) may still reach.
pub(crate) unsafe trait Storage: Layout<Leaf: LeafNode> {
    /// Reclamation context passed to write operations: a pinned
    /// default-collector guard for the heap, `()` for the arena.
    type Guard;

    /// Retires a node that is unlinked and marked obsolete.
    ///
    /// # Safety
    /// `n` is unreachable for every reader that starts after this call, and is
    /// retired exactly once.
    unsafe fn retire_node(&self, n: NodePtr<Self>, guard: &Self::Guard);

    /// Retires a leaf that is unlinked and marked removed.
    ///
    /// # Safety
    /// As for `retire_node`.
    unsafe fn retire_leaf(&self, l: NonNull<Self::Leaf>, guard: &Self::Guard);
}

/// The concurrent tree. Invariance, ownership and auto traits come from the
/// storage type (Inv 9).
pub(crate) struct RawTree<S: Layout> {
    root: S::Atomic,
    root_latch: HybridLatch,
    /// Inv 12: signed, so a transient negative (a bug) is clamped in `len()`.
    len: CachePadded<AtomicIsize>,
    pub(crate) storage: S,
}

impl<S: Layout> RawTree<S> {
    #[cfg(not(loom))]
    pub(crate) const fn new_in(storage: S) -> Self
    where
        S::Atomic: slot::ConstNull,
    {
        Self {
            root: <S::Atomic as slot::ConstNull>::NULL,
            root_latch: HybridLatch::new(),
            len: CachePadded(AtomicIsize::new(0)),
            storage,
        }
    }

    #[cfg(loom)]
    pub(crate) fn new_in(storage: S) -> Self {
        Self {
            root: S::Atomic::new(Raw::<S>::NULL),
            root_latch: HybridLatch::new(),
            len: CachePadded(AtomicIsize::new(0)),
            storage,
        }
    }

    /// Exact at quiescence; clamped at zero.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed).max(0) as usize
    }

    /// The raw signed counter, for regression tests (the clamp would hide bugs).
    #[cfg(test)]
    pub(crate) fn raw_len(&self) -> isize {
        self.len.load(Ordering::Relaxed)
    }

    /// Inv 8: every load of `root` is `Acquire`.
    #[inline]
    pub(crate) fn root(&self) -> Raw<S> {
        self.root.load(Ordering::Acquire)
    }

    /// W3: caller holds `root_latch`.
    #[inline]
    fn set_root(&self, w: &WriteGuard<'_>, v: Raw<S>) {
        debug_assert!(w.holds(&self.root_latch));
        self.root.store(v, Ordering::Release);
    }

    #[inline]
    pub(crate) fn len_add(&self, n: isize) {
        // Skipped for 0: the counter is a shared cache line.
        if n != 0 {
            self.len.fetch_add(n, Ordering::Relaxed);
        }
    }

    /// # Safety
    /// As for [`Storage::node`].
    #[inline(always)]
    pub(crate) unsafe fn node_ref(&self, raw: Raw<S>) -> &NodeHeader<S::Atomic> {
        // SAFETY: per the caller's contract the node is live and protected for
        // at least as long as `&self` is used by the caller's optimistic section.
        unsafe { self.storage.node(raw).as_ref() }
    }

    /// # Safety
    /// As for [`Storage::leaf`].
    #[inline(always)]
    pub(crate) unsafe fn leaf_ref(&self, raw: Raw<S>) -> &S::Leaf {
        // SAFETY: as for `node_ref`.
        unsafe { self.storage.leaf(raw).as_ref() }
    }
}

/// Owns a never-published leaf (§9.3).
///
/// The installing code calls [`disarm`](Self::disarm) in its commit phase,
/// immediately before the publishing store. If the install unwinds in its
/// prepare phase, or reports an existing leaf, dropping the owner frees the
/// leaf: after the callee's frame is gone, so no protector covers the key
/// bytes derived from it, and outside any latch, so `K::drop`/`V::drop` run
/// outside latches (Inv 6).
pub(crate) struct Unpublished<'s, S: Layout> {
    leaf: Cell<Option<NonNull<S::Leaf>>>,
    storage: &'s S,
}

impl<'s, S: Layout> Unpublished<'s, S> {
    /// # Safety
    /// `leaf` is a fully initialised leaf allocated for `storage`, owned by the
    /// caller and unreachable by any other thread.
    pub(crate) unsafe fn new(storage: &'s S, leaf: NonNull<S::Leaf>) -> Self {
        Self {
            leaf: Cell::new(Some(leaf)),
            storage,
        }
    }

    #[inline]
    pub(crate) fn ptr(&self) -> NonNull<S::Leaf> {
        self.leaf.get().expect("unpublished leaf is armed")
    }

    /// Hands ownership to the tree (or back to the caller).
    #[inline]
    pub(crate) fn disarm(&self) {
        self.leaf.set(None);
    }
}

impl<S: Layout> Drop for Unpublished<'_, S> {
    fn drop(&mut self) {
        if let Some(l) = self.leaf.take() {
            // SAFETY: never published (disarm precedes publication) and
            // exclusively owned by `self`.
            unsafe { self.storage.free_leaf(l) }
        }
    }
}

/// Unpublished inner nodes prepared by a write operation. Retries reuse them
/// instead of allocating again (§12.6); whatever is unused when the operation
/// ends is freed (heap) or abandoned (arena).
pub(crate) struct Prepared<S: Layout> {
    inline: [Option<(NodePtr<S>, bool)>; 4],
    spill: Vec<(NodePtr<S>, bool)>,
}

impl<S: Layout> Prepared<S> {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            inline: [None; 4],
            spill: Vec::new(),
        }
    }

    fn entries(&mut self) -> impl Iterator<Item = &mut (NodePtr<S>, bool)> {
        self.inline
            .iter_mut()
            .flatten()
            .chain(self.spill.iter_mut())
    }

    /// A freshly initialised, unpublished node of type `ty`.
    pub(crate) fn node(&mut self, s: &S, ty: NodeType) -> Result<NodePtr<S>, S::Full> {
        for (n, used) in self.entries() {
            // SAFETY: pool entries are live, unpublished allocations.
            if !*used && unsafe { n.as_ref() }.node_type == ty {
                *used = true;
                // SAFETY: unpublished and exclusively owned; re-initialisation
                // rewrites every field (§12.6).
                unsafe { reinit::<S>(*n) };
                return Ok(*n);
            }
        }
        let n = s.alloc_node(ty)?;
        let entry = Some((n, true));
        if let Some(slot) = self.inline.iter_mut().find(|e| e.is_none()) {
            *slot = entry;
        } else {
            self.spill.push((n, true));
        }
        Ok(n)
    }

    /// A retry: every node handed out becomes available again.
    #[inline]
    pub(crate) fn reset(&mut self) {
        for (_, used) in self.entries() {
            *used = false;
        }
    }

    /// The nodes handed out in this attempt were published; forget them.
    #[inline]
    pub(crate) fn commit(&mut self) {
        for e in self.inline.iter_mut() {
            if matches!(e, Some((_, true))) {
                *e = None;
            }
        }
        self.spill.retain(|(_, used)| !*used);
    }

    /// Frees every node still owned by the pool.
    pub(crate) fn release(&mut self, s: &S) {
        for e in self.inline.iter_mut() {
            if let Some((n, _)) = e.take() {
                // SAFETY: never published (published nodes were removed by `commit`).
                unsafe { s.free_node(n) };
            }
        }
        for (n, _) in self.spill.drain(..) {
            // SAFETY: as above.
            unsafe { s.free_node(n) };
        }
    }
}

/// # Safety
/// `n` is an unpublished node allocation of its current `node_type`.
unsafe fn reinit<S: Layout>(n: NodePtr<S>) {
    // SAFETY: the allocation is exclusively owned and sized for its type; the
    // nodes have no destructors, so overwriting without dropping is fine.
    unsafe {
        match n.as_ref().node_type {
            NodeType::Node4 => n.cast::<Node4<S::Atomic>>().write(Node4::new()),
            NodeType::Node16 => n.cast::<Node16<S::Atomic>>().write(Node16::new()),
            NodeType::Node48 => n.cast::<Node48<S::Atomic>>().write(Node48::new()),
            NodeType::Node256 => n.cast::<Node256<S::Atomic>>().write(Node256::new()),
        }
    }
}

/// How an insert treats an existing key.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub(crate) enum Mode {
    /// Replace the existing leaf out of place (`ArtMap::insert`).
    Replace,
    /// Leave the existing leaf and report it (`get_or_insert_with`, new
    /// versioned leaves).
    InsertIfAbsent,
}

/// The result of an insert.
pub(crate) enum Outcome<L> {
    /// The new leaf (this one) was published.
    Inserted(NonNull<L>),
    /// The new leaf replaced this one, which is unlinked, marked removed and
    /// already retired.
    Replaced(NonNull<L>),
    /// `InsertIfAbsent` found this live leaf; the new leaf was not published.
    Existing(NonNull<L>),
}

/// Moves `v` into a new heap allocation and returns its raw pointer.
///
/// Uses `Box::into_raw`, not `Box::leak`, so the pointer carries the box's own
/// provenance rather than that of a `&mut` reborrow (Tree Borrows).
#[inline]
pub(crate) fn boxed<T>(v: T) -> NonNull<T> {
    // SAFETY: `Box::into_raw` never returns null.
    unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(v))) }
}

/// Longest common prefix of two byte strings.
#[inline]
pub(crate) fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}
