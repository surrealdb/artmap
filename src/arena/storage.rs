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

//! # Arena storage: bump-allocated nodes, `u32` offsets, a retired list

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::alloc::Layout as AllocLayout;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::arena::Arena;
use crate::raw::heap::Invariant;
use crate::raw::node::{Node16, Node256, Node4, Node48, NodeHeader, NodeType};
use crate::raw::slot::TaggedOffset;
use crate::raw::{Layout, LeafNode, Storage};
use crate::sync::atomic::{AtomicU32, Ordering};

/// Allocation failed: the arena is full.
#[derive(Copy, Clone, Debug)]
pub(crate) struct Full;

/// A leaf type allocated in an arena. Implemented for every `K`/`V`, so the
/// storage (and the maps) can always be dropped.
///
/// # Safety
/// The retired-list link is used only by the retired list, and holds what
/// `set_next_retired` last stored. `drop_in_arena` drops the leaf's contents
/// exactly once, resolving offsets only through `arena`.
pub(crate) unsafe trait ArenaLeaf {
    /// Whether dropping the leaf runs any destructor; if not, teardown skips
    /// the walk (O(1) drop).
    const NEEDS_DROP: bool;

    /// Links an unlinked leaf to the next retired leaf. Called only by the
    /// thread that retires the leaf (a versioned leaf's chain is dead by
    /// then).
    fn set_next_retired(&self, next: u32);

    /// The link that `set_next_retired` stored.
    fn next_retired(&self) -> u32;

    /// Drops the leaf's keys and values in place (for versioned leaves, its
    /// live chain as well). The bytes stay in the arena.
    ///
    /// # Safety
    /// Exclusive access; called once per leaf.
    unsafe fn drop_in_arena(this: NonNull<Self>, arena: &Arena);
}

/// Arena storage for leaves of type `L`.
pub(crate) struct ArenaStorage<L: ArenaLeaf> {
    pub(crate) arena: Arc<Arena>,
    /// Head of the retired list: every leaf unlinked while the map is alive.
    retired: AtomicU32,
    _marker: Invariant<L>,
}

// SAFETY: moving the storage moves ownership of its leaves, which are dropped
// on the dropping thread; shared access hands out `&L`.
unsafe impl<L: ArenaLeaf + Send + Sync> Send for ArenaStorage<L> {}
// SAFETY: shared access hands out `&L` to many threads, and values inserted on
// one thread are dropped by whichever thread drops the map.
unsafe impl<L: ArenaLeaf + Send + Sync> Sync for ArenaStorage<L> {}

impl<L: ArenaLeaf> ArenaStorage<L> {
    pub(crate) fn new(arena: Arc<Arena>) -> Self {
        const {
            assert!(std::mem::align_of::<Node256<AtomicU32>>() <= crate::arena::ARENA_ALIGN);
        }
        Self {
            arena,
            retired: AtomicU32::new(0),
            _marker: std::marker::PhantomData,
        }
    }

    /// Allocates and writes `value`. `None` if the arena is full.
    pub(crate) fn alloc_value<T>(&self, value: T) -> Result<NonNull<T>, (Full, T)> {
        const {
            assert!(std::mem::align_of::<T>() <= crate::arena::ARENA_ALIGN);
        }
        match self.arena.alloc(AllocLayout::new::<T>()) {
            Some(off) => {
                // SAFETY: freshly allocated for a `T`, exclusively ours.
                let p = unsafe { self.arena.ptr::<T>(off) };
                // SAFETY: as above; a raw-place write (Inv 3).
                unsafe { p.write(value) };
                Ok(p)
            }
            None => Err((Full, value)),
        }
    }
}

fn place<N>(arena: &Arena, node: N) -> Result<NonNull<NodeHeader<AtomicU32>>, Full> {
    let off = arena.alloc(AllocLayout::new::<N>()).ok_or(Full)?;
    // SAFETY: freshly allocated for an `N`, exclusively ours.
    let p = unsafe { arena.ptr::<N>(off) };
    // SAFETY: as above; a raw-place write of the whole node (Inv 3).
    unsafe { p.write(node) };
    Ok(p.cast())
}

// SAFETY: nodes and leaves live in the map's own arena and are addressed by
// offsets re-based on its base pointer (Inv 4); arena memory is never reused
// while the map exists.
unsafe impl<L: ArenaLeaf> Layout for ArenaStorage<L> {
    type Atomic = AtomicU32;
    type Leaf = L;
    type Full = Full;

    #[inline(always)]
    unsafe fn node(&self, raw: TaggedOffset) -> NonNull<NodeHeader<AtomicU32>> {
        // SAFETY: a node offset allocated from this arena.
        unsafe { self.arena.ptr(raw.offset()) }
    }

    #[inline(always)]
    unsafe fn leaf(&self, raw: TaggedOffset) -> NonNull<L> {
        // SAFETY: a leaf offset allocated from this arena.
        unsafe { self.arena.ptr(raw.offset()) }
    }

    #[inline(always)]
    fn node_raw(&self, n: NonNull<NodeHeader<AtomicU32>>) -> TaggedOffset {
        TaggedOffset::from_inner(self.arena.offset_of(n))
    }

    #[inline(always)]
    fn leaf_raw(&self, l: NonNull<L>) -> TaggedOffset {
        TaggedOffset::from_leaf(self.arena.offset_of(l))
    }

    fn alloc_node(&self, ty: NodeType) -> Result<NonNull<NodeHeader<AtomicU32>>, Full> {
        match ty {
            NodeType::Node4 => place(&self.arena, Node4::<AtomicU32>::new()),
            NodeType::Node16 => place(&self.arena, Node16::<AtomicU32>::new()),
            NodeType::Node48 => place(&self.arena, Node48::<AtomicU32>::new()),
            NodeType::Node256 => place(&self.arena, Node256::<AtomicU32>::new()),
        }
    }

    /// Inner nodes own no keys or values; their bytes are abandoned.
    unsafe fn free_node(&self, _n: NonNull<NodeHeader<AtomicU32>>) {}

    unsafe fn free_leaf(&self, l: NonNull<L>) {
        // SAFETY: exclusively owned, per the caller's contract.
        unsafe { L::drop_in_arena(l, &self.arena) }
    }
}

// SAFETY: retirement never reuses memory; retired leaves are dropped only in
// `Drop`, with exclusive access.
unsafe impl<L: ArenaLeaf + LeafNode> Storage for ArenaStorage<L> {
    type Guard = ();

    /// Replaced inner nodes own nothing: abandoned in place.
    unsafe fn retire_node(&self, _n: NonNull<NodeHeader<AtomicU32>>, _g: &()) {}

    /// Pushes an unlinked leaf onto the retired list. Never allocates, never
    /// fails, and happens after unlock.
    unsafe fn retire_leaf(&self, l: NonNull<L>, _g: &()) {
        let off = self.arena.offset_of(l);
        // SAFETY: the leaf lives in this arena for the map's life.
        let leaf = unsafe { l.as_ref() };
        let mut head = self.retired.load(Ordering::Relaxed);
        loop {
            leaf.set_next_retired(head);
            match self.retired.compare_exchange_weak(
                head,
                off,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(h) => head = h,
            }
        }
    }
}

impl<L: ArenaLeaf> Drop for ArenaStorage<L> {
    fn drop(&mut self) {
        // The tree drops its live leaves first; only retired ones remain.
        if !L::NEEDS_DROP {
            return;
        }
        let mut off = self.retired.load(Ordering::Relaxed);
        while off != 0 {
            // SAFETY: every retired offset is a leaf of this arena, pushed
            // once; `&mut self` gives exclusive access.
            let leaf = unsafe { self.arena.ptr::<L>(off) };
            // SAFETY: as above.
            let next = unsafe { leaf.as_ref() }.next_retired();
            // SAFETY: each retired leaf is dropped exactly once, here.
            unsafe { L::drop_in_arena(leaf, &self.arena) };
            off = next;
        }
    }
}
