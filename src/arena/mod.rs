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

//! # Arena-Backed Concurrent Adaptive Radix Tree
//!
//! Provides [`ArenaArtMap`], an arena-backed concurrent Adaptive Radix Tree (ART)
//! that uses 32-bit offsets for child pointers instead of 64-bit raw pointers.
//!
//! ## Design & Features
//!
//! - **32-Bit Offsets**: Shrinks inner node memory footprint by ~40% and doubles CPU L1/L2
//!   cache efficiency.
//! - **$O(1)$ Zero-Cost Teardown**: The entire tree is discarded or recycled in $O(1)$ by
//!   dropping or resetting the underlying arena buffer.
//! - **Optimistic Lock Coupling (OLC)**: Readers traverse child offsets non-blocking with zero
//!   locks, and without epoch-based GC registration overhead.
//! - **Multi-Version Support**: Built-in 64-bit versioning (`insert_versioned`, `get_version_le`)
//!   supporting atomic version prepend chains in leaves.

pub mod iter;
pub mod map;
pub mod node;
pub mod tree;

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub use iter::{ArenaEntryRef, Range};
pub use map::ArenaArtMap;

/// Maximum arena size (`u32::MAX` to fit in 32-bit offsets).
pub const MAX_ARENA_SIZE: usize = u32::MAX as usize;

/// Node allocation alignment in the arena (8 bytes).
pub const NODE_ALIGNMENT: u32 = 8;

/// A lock-free contiguous byte arena allocator for [`ArenaArtMap`].
///
/// Memory is pre-allocated upon creation and allocated sequentially via atomic bump allocation.
/// When dropped, the entire memory block is reclaimed in $O(1)$.
pub struct Arena {
    n: AtomicU64,
    _pad: [u8; 56],
    buf: Box<[UnsafeCell<u8>]>,
}

// SAFETY: `Arena` buffer memory is self-contained.
unsafe impl Send for Arena {}

// SAFETY: All mutation is coordinated through the atomic `n` cursor in `alloc`.
// Distinct allocations reserve disjoint byte ranges that never overlap.
unsafe impl Sync for Arena {}

impl Arena {
    /// Creates a new arena with the specified byte capacity.
    #[cfg_attr(target_pointer_width = "32", allow(clippy::unnecessary_min_or_max))]
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.min(MAX_ARENA_SIZE);
        let buf: Box<[u8]> = vec![0u8; capacity].into_boxed_slice();
        // SAFETY: `UnsafeCell<u8>` is `#[repr(transparent)]` over `u8`.
        let buf: Box<[UnsafeCell<u8>]> =
            unsafe { Box::from_raw(Box::into_raw(buf) as *mut [UnsafeCell<u8>]) };

        Self {
            n: AtomicU64::new(NODE_ALIGNMENT as u64),
            _pad: [0u8; 56],
            buf,
        }
    }

    /// Creates an `Arc<Arena>` with the specified byte capacity.
    pub fn with_capacity(capacity: usize) -> Arc<Self> {
        Arc::new(Self::new(capacity))
    }

    /// Returns the number of bytes allocated in the arena so far.
    pub fn size(&self) -> usize {
        let s = self.n.load(Ordering::Relaxed);
        if s > self.buf.len() as u64 {
            self.buf.len()
        } else {
            s as usize
        }
    }

    /// Returns the total capacity in bytes of this arena.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    /// Returns the remaining available bytes in this arena.
    #[inline]
    pub fn remaining(&self) -> usize {
        self.capacity().saturating_sub(self.size())
    }

    /// Returns `true` if no user allocations have been made yet.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.size() <= NODE_ALIGNMENT as usize
    }

    /// Resets the allocation offset to allow reusing the allocated memory buffer in $O(1)$.
    pub fn reset(&mut self) {
        self.n.store(NODE_ALIGNMENT as u64, Ordering::Relaxed);
    }

    /// Returns the offset of a pointer allocated in this arena.
    #[inline(always)]
    pub fn offset_of(&self, ptr: *const u8) -> u32 {
        ((ptr as usize) - (self.buf.as_ptr() as usize)) as u32
    }

    /// Atomically reserves `size` bytes with the specified `alignment`.
    ///
    /// Returns the allocated 32-bit byte offset, or `None` if the arena is full.
    pub fn alloc(&self, size: u32, alignment: u32, overflow: u32) -> Option<u32> {
        debug_assert!(alignment.is_power_of_two());

        let padded = size as u64 + alignment as u64 - 1;
        let new_size = self.n.fetch_add(padded, Ordering::Relaxed) + padded;
        if new_size + overflow as u64 > self.buf.len() as u64 {
            return None;
        }

        let offset = (new_size as u32 - size) & !(alignment - 1);
        debug_assert_eq!(offset % alignment, 0);
        Some(offset)
    }

    /// Returns a raw pointer to the data at the specified 32-bit offset.
    #[inline(always)]
    pub fn get_pointer(&self, offset: u32) -> *const u8 {
        if offset == 0 {
            return std::ptr::null();
        }
        debug_assert!((offset as usize) < self.buf.len());
        // SAFETY: `offset` was verified within bounds during `alloc`.
        unsafe { (self.buf.as_ptr() as *const u8).add(offset as usize) }
    }

    /// Returns a raw mutable pointer to the data at the specified 32-bit offset.
    #[inline(always)]
    pub fn get_pointer_mut(&self, offset: u32) -> *mut u8 {
        if offset == 0 {
            return std::ptr::null_mut();
        }
        debug_assert!((offset as usize) < self.buf.len());
        // SAFETY: `offset` was verified within bounds during `alloc`.
        unsafe { (self.buf.as_ptr() as *mut u8).add(offset as usize) }
    }
}
