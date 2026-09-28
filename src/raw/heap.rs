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

//! # Heap storage: boxed nodes, tagged pointers, EBR retirement

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::convert::Infallible;
use std::marker::PhantomData;
use std::ptr::NonNull;

use crate::guard::{retire, Retired};
use crate::raw::node::{Node16, Node256, Node4, Node48, NodeHeader, NodeType};
use crate::raw::slot::TaggedPtr;
use crate::raw::{Layout, LeafNode, Storage};
use crate::sync::atomic::AtomicPtr;

/// Heap storage for leaves of type `L`.
///
/// The marker owns `L` (drop check) and makes the tree invariant in `L`
/// (Inv 9): insertion through `&self` must not be reachable after covariant
/// weakening.
pub(crate) struct HeapStorage<L> {
    _marker: Invariant<L>,
}

/// Owns `L` and is invariant in it; a named alias keeps `type_complexity` quiet.
pub(crate) type Invariant<L> = PhantomData<(L, fn(L) -> L)>;

impl<L> HeapStorage<L> {
    pub(crate) const fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

// SAFETY: moving the tree moves ownership of its leaves, which EBR may drop
// on another thread, and shared access hands out `&L`: both need `L: Send + Sync`.
unsafe impl<L: Send + Sync> Send for HeapStorage<L> {}
// SAFETY: shared access hands out `&L` to many threads (needs `Sync`), and
// removed or replaced leaves are dropped by EBR on arbitrary threads (needs `Send`).
unsafe impl<L: Send + Sync> Sync for HeapStorage<L> {}

type A = AtomicPtr<u8>;

fn boxed<N>(n: N) -> NonNull<NodeHeader<A>> {
    // The header is at offset 0 of the node, so the cast keeps provenance over it all.
    crate::raw::boxed(n).cast()
}

// SAFETY: nodes and leaves are `Box` allocations addressed by tagged pointers
// with full provenance.
unsafe impl<L> Layout for HeapStorage<L> {
    type Atomic = A;
    type Leaf = L;
    type Full = Infallible;

    #[inline(always)]
    unsafe fn node(&self, raw: TaggedPtr) -> NonNull<NodeHeader<A>> {
        // SAFETY: the caller passes a non-null inner-node pointer.
        unsafe { NonNull::new_unchecked(raw.as_inner_ptr()) }
    }

    #[inline(always)]
    unsafe fn leaf(&self, raw: TaggedPtr) -> NonNull<L> {
        // SAFETY: the caller passes a non-null leaf pointer.
        unsafe { NonNull::new_unchecked(raw.as_leaf_ptr()) }
    }

    #[inline(always)]
    fn node_raw(&self, n: NonNull<NodeHeader<A>>) -> TaggedPtr {
        TaggedPtr::from_inner(n.as_ptr())
    }

    #[inline(always)]
    fn leaf_raw(&self, l: NonNull<L>) -> TaggedPtr {
        TaggedPtr::from_leaf(l.as_ptr())
    }

    fn alloc_node(&self, ty: NodeType) -> Result<NonNull<NodeHeader<A>>, Infallible> {
        Ok(match ty {
            NodeType::Node4 => boxed(Node4::<A>::new()),
            NodeType::Node16 => boxed(Node16::<A>::new()),
            NodeType::Node48 => boxed(Node48::<A>::new()),
            NodeType::Node256 => boxed(Node256::<A>::new()),
        })
    }

    unsafe fn free_node(&self, n: NonNull<NodeHeader<A>>) {
        // SAFETY: `n` is a Box allocation of its `node_type`, owned by the caller.
        unsafe {
            match n.as_ref().node_type {
                NodeType::Node4 => drop(Box::from_raw(n.cast::<Node4<A>>().as_ptr())),
                NodeType::Node16 => drop(Box::from_raw(n.cast::<Node16<A>>().as_ptr())),
                NodeType::Node48 => drop(Box::from_raw(n.cast::<Node48<A>>().as_ptr())),
                NodeType::Node256 => drop(Box::from_raw(n.cast::<Node256<A>>().as_ptr())),
            }
        }
    }

    unsafe fn free_leaf(&self, l: NonNull<L>) {
        // SAFETY: a Box allocation exclusively owned by the caller.
        drop(unsafe { Box::from_raw(l.as_ptr()) });
    }
}

// SAFETY: memory is freed only through EBR after unlinking, or with exclusive
// access, so no protected reader can reach freed memory.
unsafe impl<L: LeafNode + Send + 'static> Storage for HeapStorage<L> {
    type Guard = crossbeam_epoch::Guard;

    unsafe fn retire_node(&self, n: NonNull<NodeHeader<A>>, guard: &crossbeam_epoch::Guard) {
        // SAFETY: the caller guarantees `n` is unlinked, obsolete and retired
        // once; it is a Box allocation of its `node_type`. Inner nodes have no
        // destructor that follows child pointers.
        unsafe {
            match n.as_ref().node_type {
                NodeType::Node4 => retire(guard, Retired::from_non_null(n.cast::<Node4<A>>())),
                NodeType::Node16 => retire(guard, Retired::from_non_null(n.cast::<Node16<A>>())),
                NodeType::Node48 => retire(guard, Retired::from_non_null(n.cast::<Node48<A>>())),
                NodeType::Node256 => retire(guard, Retired::from_non_null(n.cast::<Node256<A>>())),
            }
        }
    }

    unsafe fn retire_leaf(&self, l: NonNull<L>, guard: &crossbeam_epoch::Guard) {
        // SAFETY: the caller guarantees `l` is unlinked, marked and retired once;
        // leaves are Box allocations.
        retire(guard, unsafe { Retired::from_non_null(l) });
    }
}
