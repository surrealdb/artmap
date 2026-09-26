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
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

pub use iter::{ArenaEntryRef, Range};
pub use map::{ArenaArtMap, ArenaInserter};

/// Maximum arena size (`u32::MAX` to fit in 32-bit offsets).
pub const MAX_ARENA_SIZE: usize = u32::MAX as usize;

/// Node allocation alignment in the arena (8 bytes).
pub const NODE_ALIGNMENT: u32 = 8;

const CHUNK_SIZE: u32 = 64 * 1024;
const MAX_LOCAL_ALLOC_SIZE: u32 = 520;

#[derive(Default, Clone, Copy)]
struct LocalChunk {
    arena_id: usize,
    arena_epoch: u32,
    current: u32,
    limit: u32,
}

thread_local! {
    static TLS_CHUNK: std::cell::Cell<LocalChunk> = const {
        std::cell::Cell::new(LocalChunk {
            arena_id: 0,
            arena_epoch: 0,
            current: 0,
            limit: 0,
        })
    };
}

/// A lock-free contiguous byte arena allocator for [`ArenaArtMap`].
///
/// Memory is pre-allocated upon creation and allocated sequentially via atomic bump allocation.
/// When dropped, the entire memory block is reclaimed in $O(1)$.
pub struct Arena {
    n: AtomicU64,
    epoch: AtomicU32,
    _pad: [u8; 52],
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
            epoch: AtomicU32::new(1),
            _pad: [0u8; 52],
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
        self.epoch.fetch_add(1, Ordering::Relaxed);
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

        let padded = size as u64 + alignment as u64 - 1;
        let new_size = self.n.fetch_add(padded, Ordering::Relaxed) + padded;
        if new_size + overflow as u64 > self.buf.len() as u64 {
            return None;
        }

        let offset = (new_size as u32 - size) & !(alignment - 1);
        debug_assert_eq!(offset % alignment, 0);
        Some(offset)
    }

    /// Allocates `size` bytes with the specified `alignment`.
    ///
    /// For allocations up to 520 bytes (leaves, Node4, Node16, Node48), uses a thread-local
    /// 64 KB chunk to eliminate atomic cache-line contention across threads.
    pub fn alloc(&self, size: u32, alignment: u32, overflow: u32) -> Option<u32> {
        debug_assert!(alignment.is_power_of_two());
        let arena_id = self.buf.as_ptr() as usize;
        let current_epoch = self.epoch.load(Ordering::Relaxed);

        if size <= MAX_LOCAL_ALLOC_SIZE && alignment <= NODE_ALIGNMENT {
            let local_res = TLS_CHUNK.with(|cell| {
                let mut chunk = cell.get();
                if chunk.arena_id == arena_id && chunk.arena_epoch == current_epoch {
                    let aligned = (chunk.current + alignment - 1) & !(alignment - 1);
                    if aligned + size <= chunk.limit {
                        chunk.current = aligned + size;
                        cell.set(chunk);
                        return Some(aligned);
                    }
                }
                None
            });

            if let Some(off) = local_res {
                return Some(off);
            }

            // Chunk exhausted or epoch changed: reserve a new 64KB chunk from the global arena
            if let Some(chunk_start) = self.alloc_global(CHUNK_SIZE, NODE_ALIGNMENT, overflow) {
                let aligned = (chunk_start + alignment - 1) & !(alignment - 1);
                let chunk_limit = chunk_start + CHUNK_SIZE;
                TLS_CHUNK.with(|cell| {
                    cell.set(LocalChunk {
                        arena_id,
                        arena_epoch: current_epoch,
                        current: aligned + size,
                        limit: chunk_limit,
                    });
                });
                return Some(aligned);
            }
        }

        // Fall back to direct global allocation (for Node256 or when remaining memory < 64KB)
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
