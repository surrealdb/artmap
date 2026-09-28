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

//! # Child-slot representations
//!
//! A child slot holds either nothing, a leaf, or an inner node, distinguished
//! by a tag bit. The heap maps store strict-provenance tagged pointers (§6.1,
//! Inv 4: tag bits are manipulated with `map_addr`, never through integers);
//! the arena maps store tagged `u32` offsets into their arena.

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use crate::sync::atomic::{AtomicPtr, AtomicU32, Ordering};

/// A copyable child-slot value.
pub(crate) trait Slot: Copy + Eq + std::fmt::Debug {
    const NULL: Self;
    fn is_null(self) -> bool;
    fn is_leaf(self) -> bool;
}

/// The atomic cell holding a [`Slot`].
pub(crate) trait AtomicSlot: Send + Sync {
    type Raw: Slot;
    fn new(v: Self::Raw) -> Self;
    fn load(&self, order: Ordering) -> Self::Raw;
    fn store(&self, v: Self::Raw, order: Ordering);
    fn swap(&self, v: Self::Raw, order: Ordering) -> Self::Raw;
}

/// A constant null cell, so that `ArtMap::new` stays a `const fn` (not
/// available under loom, whose atomics have no `const fn new`).
#[cfg(not(loom))]
pub(crate) trait ConstNull {
    const NULL: Self;
}

#[cfg(not(loom))]
impl ConstNull for AtomicPtr<u8> {
    const NULL: Self = AtomicPtr::new(std::ptr::null_mut());
}

#[cfg(not(loom))]
impl ConstNull for AtomicU32 {
    const NULL: Self = AtomicU32::new(0);
}

pub(crate) const TAG_LEAF: usize = 0b01;

/// A tagged pointer to a leaf (tag bit set) or an inner node.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
#[repr(transparent)]
pub(crate) struct TaggedPtr {
    ptr: *mut u8,
}

impl TaggedPtr {
    #[inline(always)]
    pub(crate) fn from_leaf<T>(ptr: *mut T) -> Self {
        debug_assert_eq!(ptr.addr() & TAG_LEAF, 0, "leaf pointer must be aligned");
        Self {
            ptr: ptr.cast::<u8>().map_addr(|a| a | TAG_LEAF),
        }
    }

    #[inline(always)]
    pub(crate) fn from_inner<T>(ptr: *mut T) -> Self {
        debug_assert_eq!(ptr.addr() & TAG_LEAF, 0, "node pointer must be aligned");
        Self {
            ptr: ptr.cast::<u8>(),
        }
    }

    #[inline(always)]
    pub(crate) fn as_leaf_ptr<T>(self) -> *mut T {
        debug_assert!(self.is_leaf());
        self.ptr.map_addr(|a| a & !TAG_LEAF).cast::<T>()
    }

    #[inline(always)]
    pub(crate) fn as_inner_ptr<T>(self) -> *mut T {
        debug_assert!(!self.is_leaf());
        self.ptr.cast::<T>()
    }

    #[inline(always)]
    pub(crate) fn as_raw(self) -> *mut u8 {
        self.ptr
    }

    #[inline(always)]
    pub(crate) fn from_raw(ptr: *mut u8) -> Self {
        Self { ptr }
    }
}

impl Slot for TaggedPtr {
    const NULL: Self = Self {
        ptr: std::ptr::null_mut(),
    };

    #[inline(always)]
    fn is_null(self) -> bool {
        self.ptr.is_null()
    }

    #[inline(always)]
    fn is_leaf(self) -> bool {
        self.ptr.addr() & TAG_LEAF != 0
    }
}

impl AtomicSlot for AtomicPtr<u8> {
    type Raw = TaggedPtr;

    #[inline(always)]
    fn new(v: TaggedPtr) -> Self {
        AtomicPtr::new(v.as_raw())
    }

    #[inline(always)]
    fn load(&self, order: Ordering) -> TaggedPtr {
        TaggedPtr::from_raw(AtomicPtr::load(self, order))
    }

    #[inline(always)]
    fn store(&self, v: TaggedPtr, order: Ordering) {
        AtomicPtr::store(self, v.as_raw(), order)
    }

    #[inline(always)]
    fn swap(&self, v: TaggedPtr, order: Ordering) -> TaggedPtr {
        TaggedPtr::from_raw(AtomicPtr::swap(self, v.as_raw(), order))
    }
}

// SAFETY: `TaggedPtr` is a plain address with provenance; sending it between
// threads does not access the pointee, whose sharing is governed by the
// owning tree's own `Send`/`Sync` bounds.
unsafe impl Send for TaggedPtr {}
// SAFETY: as above; `&TaggedPtr` only exposes copies of the address.
unsafe impl Sync for TaggedPtr {}

pub(crate) const TAG_LEAF_OFFSET: u32 = 0b01;

/// A tagged `u32` offset into an arena: 0 is null, bit 0 marks a leaf.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
#[repr(transparent)]
pub(crate) struct TaggedOffset(pub(crate) u32);

#[allow(dead_code)] // used by the arena storage
impl TaggedOffset {
    #[inline(always)]
    pub(crate) fn from_leaf(offset: u32) -> Self {
        debug_assert_eq!(offset & TAG_LEAF_OFFSET, 0, "offset must be aligned");
        debug_assert_ne!(offset, 0);
        Self(offset | TAG_LEAF_OFFSET)
    }

    #[inline(always)]
    pub(crate) fn from_inner(offset: u32) -> Self {
        debug_assert_eq!(offset & TAG_LEAF_OFFSET, 0, "offset must be aligned");
        debug_assert_ne!(offset, 0);
        Self(offset)
    }

    #[inline(always)]
    pub(crate) fn offset(self) -> u32 {
        self.0 & !TAG_LEAF_OFFSET
    }
}

impl Slot for TaggedOffset {
    const NULL: Self = Self(0);

    #[inline(always)]
    fn is_null(self) -> bool {
        self.0 == 0
    }

    #[inline(always)]
    fn is_leaf(self) -> bool {
        self.0 & TAG_LEAF_OFFSET != 0
    }
}

impl AtomicSlot for AtomicU32 {
    type Raw = TaggedOffset;

    #[inline(always)]
    fn new(v: TaggedOffset) -> Self {
        AtomicU32::new(v.0)
    }

    #[inline(always)]
    fn load(&self, order: Ordering) -> TaggedOffset {
        TaggedOffset(AtomicU32::load(self, order))
    }

    #[inline(always)]
    fn store(&self, v: TaggedOffset, order: Ordering) {
        AtomicU32::store(self, v.0, order)
    }

    #[inline(always)]
    fn swap(&self, v: TaggedOffset, order: Ordering) -> TaggedOffset {
        TaggedOffset(AtomicU32::swap(self, v.0, order))
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn tagged_ptr_round_trips_with_provenance() {
        let b = Box::into_raw(Box::new(0xABCD_u64));
        let t = TaggedPtr::from_leaf(b);
        assert!(t.is_leaf());
        assert!(!t.is_null());
        // SAFETY: the untagged pointer is `b`, a live Box allocation.
        assert_eq!(unsafe { *t.as_leaf_ptr::<u64>() }, 0xABCD);
        let i = TaggedPtr::from_inner(b);
        assert!(!i.is_leaf());
        assert_eq!(i.as_inner_ptr::<u64>(), b);
        // SAFETY: `b` came from Box::into_raw and is freed once.
        drop(unsafe { Box::from_raw(b) });
        assert!(TaggedPtr::NULL.is_null());
    }

    #[test]
    fn tagged_offset() {
        let l = TaggedOffset::from_leaf(64);
        assert!(l.is_leaf());
        assert_eq!(l.offset(), 64);
        let n = TaggedOffset::from_inner(128);
        assert!(!n.is_leaf());
        assert_eq!(n.offset(), 128);
        assert!(TaggedOffset::NULL.is_null());
    }
}
