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

// The arena modules still expose their pre-0.6 internals; they are rebuilt
// on the shared core and sealed by the arena rewrite.
#![allow(private_interfaces)]

pub mod iter;
pub mod map;
pub mod node;
pub mod tree;
pub mod versioned_iter;
pub mod versioned_map;
pub mod versioned_tree;

use std::cell::{Cell, UnsafeCell};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

pub use iter::{ArenaEntryRef, Range};
pub use map::{ArenaArtMap, ArenaInserter};
pub use versioned_iter::{ArenaVersionedEntryRef, ArenaVersionedRange};
pub use versioned_map::ArenaVersionedArtMap;

/// Maximum arena size (`u32::MAX` to fit in 32-bit offsets).
pub const MAX_ARENA_SIZE: usize = u32::MAX as usize;

/// Node allocation alignment in the arena (8 bytes).
pub const NODE_ALIGNMENT: u32 = 8;

/// Size of thread-local allocation buffer chunks (64 KB).
const TLAB_CHUNK_SIZE: u32 = 64 * 1024;

#[derive(Clone, Copy, Default)]
struct TlabSlot {
    arena_id: usize,
    gen: u32,
    current: u32,
    limit: u32,
}

thread_local! {
    static TLAB: Cell<[TlabSlot; 4]> = const {
        Cell::new([TlabSlot {
            arena_id: 0,
            gen: 0,
            current: 0,
            limit: 0,
        }; 4])
    };
}

static NEXT_ARENA_ID: AtomicUsize = AtomicUsize::new(1);

/// A lock-free contiguous byte arena allocator for [`ArenaArtMap`].
///
/// Memory is pre-allocated upon creation and allocated sequentially via atomic bump allocation
/// accelerated by thread-local allocation buffers (TLAB).
/// When dropped, the entire memory block is reclaimed in $O(1)$.
pub struct Arena {
    id: usize,
    n: AtomicU64,
    gen: AtomicU32,
    _pad: [u8; 44],
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
            id: NEXT_ARENA_ID.fetch_add(1, Ordering::Relaxed),
            n: AtomicU64::new(NODE_ALIGNMENT as u64),
            gen: AtomicU32::new(0),
            _pad: [0u8; 44],
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
        self.gen.fetch_add(1, Ordering::Relaxed);
        self.n.store(NODE_ALIGNMENT as u64, Ordering::Relaxed);
    }

    /// Returns the offset of a pointer allocated in this arena.
    #[inline(always)]
    pub fn offset_of(&self, ptr: *const u8) -> u32 {
        ((ptr as usize) - (self.buf.as_ptr() as usize)) as u32
    }

    /// Reserves `size` bytes directly from the global atomic bump cursor.
    pub fn alloc_global(&self, size: u32, alignment: u32, overflow: u32) -> Option<u32> {
        debug_assert!(alignment.is_power_of_two());

        let align_mask = alignment as u64 - 1;
        let padded = (size as u64 + align_mask) & !align_mask;
        let prev_size = self.n.fetch_add(padded, Ordering::Relaxed);
        let new_size = prev_size + padded;
        if new_size + overflow as u64 > self.buf.len() as u64 {
            return None;
        }

        let offset = prev_size as u32;
        debug_assert_eq!(offset % alignment, 0);
        Some(offset)
    }

    /// Allocates `size` bytes with the specified `alignment`.
    #[inline]
    pub fn alloc(&self, size: u32, alignment: u32, overflow: u32) -> Option<u32> {
        debug_assert!(alignment.is_power_of_two());
        let align_mask = alignment - 1;
        let padded = (size + align_mask) & !align_mask;

        // Try thread-local allocation buffer (TLAB) for small allocations
        if padded <= TLAB_CHUNK_SIZE / 4 {
            let arena_id = self.id;
            let current_gen = self.gen.load(Ordering::Relaxed);
            let res = TLAB.with(|cell| {
                let mut slots = cell.get();
                for slot in slots.iter_mut() {
                    if slot.arena_id == arena_id && slot.gen == current_gen {
                        let cur_aligned = (slot.current + align_mask) & !align_mask;
                        let next = cur_aligned + padded;
                        if next <= slot.limit {
                            slot.current = next;
                            cell.set(slots);
                            return Some(cur_aligned);
                        }
                        break;
                    }
                }

                // Refill TLAB from global atomic cursor
                let chunk_size = TLAB_CHUNK_SIZE;
                if let Some(block_start) = self.alloc_global(chunk_size, alignment, overflow) {
                    let cur_aligned = (block_start + align_mask) & !align_mask;
                    let next = cur_aligned + padded;
                    let mut target_idx = 0;
                    for (i, slot) in slots.iter().enumerate() {
                        if slot.arena_id == arena_id || slot.current >= slot.limit {
                            target_idx = i;
                            break;
                        }
                    }
                    slots[target_idx] = TlabSlot {
                        arena_id,
                        gen: current_gen,
                        current: next,
                        limit: block_start + chunk_size,
                    };
                    cell.set(slots);
                    Some(cur_aligned)
                } else {
                    None
                }
            });

            if let Some(offset) = res {
                return Some(offset);
            }
        }

        self.alloc_global(size, alignment, overflow)
    }

    /// Returns a raw pointer to the data at the specified 32-bit offset.
    #[inline(always)]
    pub fn get_pointer(&self, offset: u32) -> *const u8 {
        debug_assert_ne!(offset, 0);
        debug_assert!((offset as usize) < self.buf.len());
        // SAFETY: `offset` was verified within bounds during `alloc`.
        unsafe { (self.buf.as_ptr() as *const u8).add(offset as usize) }
    }

    /// Returns a raw mutable pointer to the data at the specified 32-bit offset.
    #[inline(always)]
    pub fn get_pointer_mut(&self, offset: u32) -> *mut u8 {
        debug_assert_ne!(offset, 0);
        debug_assert!((offset as usize) < self.buf.len());
        // SAFETY: `offset` was verified within bounds during `alloc`.
        unsafe { (self.buf.as_ptr() as *mut u8).add(offset as usize) }
    }
}
