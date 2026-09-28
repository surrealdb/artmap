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

//! # Arena-backed adaptive radix trees
//!
//! [`ArenaArtMap`] and [`ArenaVersionedArtMap`] allocate their nodes, leaves
//! and versions from a bump [`Arena`], addressed by 32-bit offsets, which keeps
//! inner nodes small (a `Node4` is 64 bytes).
//!
//! - **Reclamation.** Nothing is freed while the map is alive: removed or
//!   replaced entries stay readable for the whole map borrow, and every update
//!   permanently consumes arena capacity. Dropping the map drops every key and
//!   value exactly once (the live tree, the version chains, and every entry
//!   unlinked while the map was alive). Teardown is O(1) only for types without
//!   drop glue; otherwise it walks the tree.
//! - **Capacity.** An insert that does not fit fails cleanly: `try_insert`
//!   returns the key and value, and `insert` panics outside any latch.
//!   `max_insert_bytes` bounds what one insert can consume.
//! - **Stability.** Every `&K`/`&V` obtained from an arena map stays valid and
//!   unchanged for the whole map borrow, whatever concurrent writes happen.

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

mod iter;
mod map;
mod node;
mod storage;
mod tree;
mod versioned_iter;
mod versioned_map;
mod versioned_tree;

use std::alloc::Layout;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub use iter::{ArenaEntryRef, Range};
pub use map::{ArenaArtMap, ArenaInserter};
pub use versioned_iter::{ArenaVersionedEntryRef, ArenaVersionedRange, VersionRef};
pub use versioned_map::{ArenaVersionedArtMap, ArenaVersionedInserter};

/// Every arena allocation is aligned to at most this; the buffer is aligned to it.
pub(crate) const ARENA_ALIGN: usize = 64;

/// The largest arena: offsets are 32-bit.
pub const MAX_ARENA_SIZE: usize = (u32::MAX as usize) & !(ARENA_ALIGN - 1);

/// A contiguous, pre-allocated memory region for the arena maps.
///
/// Allocation is a lock-free bump of an atomic cursor. Memory is released
/// only when the arena is dropped, or reused only after [`reset`](Self::reset),
/// which needs exclusive access (so no map can still be using the arena).
pub struct Arena {
    /// Fresh from a global counter on `new` and on every `reset`.
    id: u64,
    /// Offsets `[0, ARENA_ALIGN)` are the null region.
    cursor: AtomicU64,
    base: NonNull<u8>,
    /// `ARENA_ALIGN <= capacity <= MAX_ARENA_SIZE`, a multiple of `ARENA_ALIGN`.
    capacity: usize,
}

// SAFETY: `Arena` owns its buffer. The atomic cursor hands out each byte range
// at most once; each range is accessed only through raw pointers derived from
// `base` (never a `&`/`&mut` to the whole buffer), and the fields inside a range
// that are shared are atomics (Inv 3, Inv 5).
unsafe impl Send for Arena {}
// SAFETY: as above; `&Arena` exposes only `alloc` (an atomic CAS) and the
// `unsafe` pointer accessors.
unsafe impl Sync for Arena {}

fn next_arena_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

impl Arena {
    /// Creates an arena of `capacity` bytes, clamped to
    /// `[64, MAX_ARENA_SIZE]` and rounded down to a multiple of 64.
    pub fn new(capacity: usize) -> Self {
        // A zero-size layout is UB for `alloc_zeroed`. `[0, ARENA_ALIGN)` is the
        // null region, so clamping up only changes `capacity()` for requests
        // below `ARENA_ALIGN`; no allocation that fitted before fails now.
        let capacity = capacity.clamp(ARENA_ALIGN, MAX_ARENA_SIZE) & !(ARENA_ALIGN - 1);
        let layout = Layout::from_size_align(capacity, ARENA_ALIGN).expect("arena layout");
        // SAFETY: `layout.size() >= ARENA_ALIGN > 0`.
        let base = NonNull::new(unsafe { std::alloc::alloc_zeroed(layout) })
            .unwrap_or_else(|| std::alloc::handle_alloc_error(layout));
        Self {
            id: next_arena_id(),
            cursor: AtomicU64::new(ARENA_ALIGN as u64),
            base,
            capacity,
        }
    }

    /// Creates an `Arc<Arena>` of `capacity` bytes.
    pub fn with_capacity(capacity: usize) -> Arc<Self> {
        Arc::new(Self::new(capacity))
    }

    /// Bytes handed out so far, including the 64-byte null region.
    pub fn size(&self) -> usize {
        (self.cursor.load(Ordering::Relaxed) as usize).min(self.capacity)
    }

    /// Usable capacity after clamping.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Bytes still available.
    pub fn remaining(&self) -> usize {
        self.capacity - self.size()
    }

    /// `true` if nothing has been allocated since creation or the last reset.
    pub fn is_empty(&self) -> bool {
        self.cursor.load(Ordering::Relaxed) <= ARENA_ALIGN as u64
    }

    /// Makes the whole arena available again, in O(1). Requires exclusive
    /// access, so no map can still hold it. The arena gets a fresh identity.
    pub fn reset(&mut self) {
        // Every node, leaf and version is fully written before use, so the
        // old bytes need no clearing.
        self.id = next_arena_id();
        *self.cursor.get_mut() = ARENA_ALIGN as u64;
    }

    /// The arena's identity: fresh on creation and on every reset.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// Reserves `layout.size()` bytes aligned to `layout.align()`.
    ///
    /// Uncontended, this is a check-then-bump CAS on the global cursor, which
    /// never advances on failure. A thread whose CAS loses a race switches to a
    /// thread-local chunk for this arena (the TLAB, §12.1), so concurrent
    /// writers stop contending on one cache line. Chunks are small
    /// (`min(16 KiB, remaining / 32)`), carved with checked `u64` arithmetic,
    /// and keyed on the arena's identity, which is fresh after every reset; a
    /// failed refill falls back to an exact-size global allocation.
    pub(crate) fn alloc(&self, layout: Layout) -> Option<u32> {
        debug_assert!(layout.align() <= ARENA_ALIGN);
        let align = layout.align() as u64;
        let size = (layout.size() as u64).max(1);
        if let Some(off) = tlab::take(self.id, align, size) {
            return Some(off);
        }
        let mut cur = self.cursor.load(Ordering::Relaxed);
        let mut contended = false;
        loop {
            let start = cur.checked_add(align - 1)? & !(align - 1);
            let end = start.checked_add(size)?;
            if end > self.capacity as u64 {
                return None;
            }
            if contended {
                if let Some(off) = self.refill(align, size, cur) {
                    return Some(off);
                }
            }
            match self
                .cursor
                .compare_exchange_weak(cur, end, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return Some(start as u32),
                Err(actual) => {
                    // Only a real conflict counts: `compare_exchange_weak` may
                    // also fail spuriously, with the cursor unchanged.
                    contended |= actual != cur;
                    cur = actual;
                }
            }
        }
    }

    /// Claims a chunk for this thread and carves the request from it.
    fn refill(&self, align: u64, size: u64, mut cur: u64) -> Option<u32> {
        const MAX_CHUNK: u64 = 16 * 1024;
        loop {
            let remaining = (self.capacity as u64).saturating_sub(cur);
            let chunk = MAX_CHUNK.min(remaining / 32);
            let start = cur.checked_add(ARENA_ALIGN as u64 - 1)? & !(ARENA_ALIGN as u64 - 1);
            let end = start.checked_add(chunk)?;
            // Too small to be worth it, or does not fit: the caller falls back
            // to an exact-size allocation.
            if chunk < size + align || end > self.capacity as u64 {
                return None;
            }
            match self
                .cursor
                .compare_exchange_weak(cur, end, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => {
                    tlab::install(self.id, start, end);
                    return tlab::take(self.id, align, size);
                }
                Err(actual) => cur = actual,
            }
        }
    }

    /// A pointer to the `T` at `offset`.
    ///
    /// # Safety
    /// `offset` was returned by `self.alloc` for a layout that fits `T` (and
    /// the arena was not reset since).
    #[inline(always)]
    pub(crate) unsafe fn ptr<T>(&self, offset: u32) -> NonNull<T> {
        debug_assert!(offset as usize >= ARENA_ALIGN && (offset as usize) < self.capacity);
        // SAFETY: per the contract the offset is inside the buffer; provenance
        // flows from `base` (Inv 4).
        unsafe { self.base.add(offset as usize).cast::<T>() }
    }

    /// The offset of a pointer into this arena.
    #[inline(always)]
    pub(crate) fn offset_of<T>(&self, ptr: NonNull<T>) -> u32 {
        let off = ptr.addr().get() - self.base.addr().get();
        debug_assert!(off < self.capacity);
        off as u32
    }
}

/// Per-thread allocation buffers (§12.1). A slot is `(arena id, cursor,
/// end)`; ids are never reused, so a slot for a dropped or reset arena is
/// simply never matched again.
mod tlab {
    use std::cell::Cell;

    const SLOTS: usize = 4;

    #[derive(Copy, Clone)]
    struct Slot {
        arena: u64,
        cur: u64,
        end: u64,
    }

    const EMPTY: Slot = Slot {
        arena: 0,
        cur: 0,
        end: 0,
    };

    std::thread_local! {
        static SLOTS_TLS: Cell<[Slot; SLOTS]> = const { Cell::new([EMPTY; SLOTS]) };
    }

    /// Carves `size` bytes aligned to `align` from this thread's chunk for
    /// `arena`, if it has one with room.
    pub(super) fn take(arena: u64, align: u64, size: u64) -> Option<u32> {
        SLOTS_TLS
            .try_with(|c| {
                let mut slots = c.get();
                let s = slots.iter_mut().find(|s| s.arena == arena)?;
                let start = s.cur.checked_add(align - 1)? & !(align - 1);
                let end = start.checked_add(size)?;
                if end > s.end {
                    return None;
                }
                s.cur = end;
                c.set(slots);
                u32::try_from(start).ok()
            })
            .ok()
            .flatten()
    }

    /// Makes `[start, end)` this thread's chunk for `arena`, replacing its old
    /// chunk (whose remainder is abandoned) or the least useful slot.
    pub(super) fn install(arena: u64, start: u64, end: u64) {
        // Outside a live thread (TLS teardown) the chunk is simply not kept.
        SLOTS_TLS
            .try_with(|c| {
                let mut slots = c.get();
                let i = slots
                    .iter()
                    .position(|s| s.arena == arena)
                    .or_else(|| slots.iter().position(|s| s.arena == 0))
                    .unwrap_or_else(|| {
                        // Evict the slot with the least room left.
                        (0..SLOTS)
                            .min_by_key(|&i| slots[i].end - slots[i].cur)
                            .unwrap_or(0)
                    });
                slots[i] = Slot {
                    arena,
                    cur: start,
                    end,
                };
                c.set(slots);
            })
            .unwrap_or(());
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        // SAFETY: allocated in `new` with exactly this (non-zero) layout.
        unsafe {
            std::alloc::dealloc(
                self.base.as_ptr(),
                Layout::from_size_align_unchecked(self.capacity, ARENA_ALIGN),
            )
        }
    }
}

impl std::fmt::Debug for Arena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Arena")
            .field("size", &self.size())
            .field("capacity", &self.capacity)
            .finish()
    }
}

/// An insert did not fit in the arena. Holds the key and value that were not
/// inserted; nothing was published and no latch is held.
pub struct ArenaFull<K, V> {
    /// The key that was not inserted.
    pub key: K,
    /// The value that was not inserted.
    pub value: V,
}

impl<K, V> std::fmt::Debug for ArenaFull<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ArenaFull")
    }
}

impl<K, V> std::fmt::Display for ArenaFull<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the arena is full")
    }
}

impl<K, V> std::error::Error for ArenaFull<K, V> {}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn tiny_and_zero_arenas_are_sound() {
        for cap in [0, 1, 63, 64, 65, 4096] {
            let a = Arena::new(cap);
            assert!(a.capacity() >= ARENA_ALIGN);
            assert_eq!(a.capacity() % ARENA_ALIGN, 0);
            // The null region leaves nothing for a 64-byte arena.
            let r = a.alloc(Layout::new::<u64>());
            assert_eq!(r.is_some(), a.capacity() > ARENA_ALIGN);
        }
    }

    #[test]
    fn allocations_are_aligned_bounded_and_never_advance_on_failure() {
        let a = Arena::new(4096);
        let o1 = a.alloc(Layout::new::<u8>()).unwrap();
        let o2 = a.alloc(Layout::new::<u128>()).unwrap();
        assert_eq!(o2 % 16, 0);
        assert!(o2 > o1);
        let o3 = a.alloc(Layout::from_size_align(8, 64).unwrap()).unwrap();
        assert_eq!(o3 % 64, 0);
        let before = a.size();
        assert!(a
            .alloc(Layout::from_size_align(1 << 20, 8).unwrap())
            .is_none());
        assert_eq!(a.size(), before, "a failed allocation does not advance");
        // Fill exactly.
        while a.alloc(Layout::new::<u64>()).is_some() {}
        assert!(a.remaining() < 8);
        // Addresses round-trip through offsets with provenance.
        let a2 = Arena::new(4096);
        let off = a2.alloc(Layout::new::<u64>()).unwrap();
        // SAFETY: freshly allocated for a u64.
        let p = unsafe { a2.ptr::<u64>(off) };
        assert_eq!(a2.offset_of(p), off);
        assert_eq!(p.as_ptr().addr() % 8, 0);
    }

    #[test]
    fn concurrent_allocations_never_overlap() {
        let a = Arc::new(Arena::new(8 << 20));
        let threads = if cfg!(miri) { 2 } else { 8 };
        let per = if cfg!(miri) { 50 } else { 10_000 };
        let hs: Vec<_> = (0..threads)
            .map(|_| {
                let a = Arc::clone(&a);
                std::thread::spawn(move || {
                    (0..per)
                        .filter_map(|_| a.alloc(Layout::from_size_align(24, 8).unwrap()))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let mut all: Vec<u32> = hs.into_iter().flat_map(|h| h.join().unwrap()).collect();
        all.sort_unstable();
        assert!(
            all.windows(2).all(|w| w[1] - w[0] >= 24),
            "overlapping allocations"
        );
    }

    #[test]
    fn reset_gives_a_fresh_identity() {
        let mut a = Arena::new(1024);
        let id = a.id();
        a.alloc(Layout::new::<u64>()).unwrap();
        a.reset();
        assert!(a.is_empty());
        assert_ne!(a.id(), id);
    }
}
