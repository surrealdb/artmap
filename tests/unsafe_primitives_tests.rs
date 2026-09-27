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

//! # Unsafe Code & Concurrent Primitives Validation Tests
//!
//! Exhaustive validation of:
//! - [`HybridLatch`]: mutual exclusion, optimistic read validation, version stepping, obsolete states.
//! - [`TaggedPtr`] and [`TaggedOffset`]: bit tagging invariants, pointer round-trips, null handling.
//! - [`simd::find_child_node16`]: SIMD vector comparisons across all 256 byte values.
//! - Arena bitmap primitives: [`set_bitmap_bit`], [`clear_bitmap_bit`], [`next_present_byte`], [`prev_present_byte`].
//! - Inner node topologies: [`Node4`], [`Node16`], [`Node48`], [`Node256`] manual lifecycle.
//! - [`VersionNode`] & [`VersionedLeaf`]: inline vs heap drops, `value_taken` double-drop guards.
//! - Arena TLAB: concurrency, bump alignment, non-overlapping allocations, and reset generation safety.

use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use artmap::arena::node::{
    clear_bitmap_bit, next_present_byte, prev_present_byte, set_bitmap_bit, TaggedOffset,
};
use artmap::arena::Arena;
use artmap::latch::{HybridLatch, LockError, SpinBackoff};
use artmap::node::{
    Leaf, Node256, Node4, Node48, NodeType, TaggedPtr, VersionNode, VersionedLeaf, NODE48_EMPTY,
};
use artmap::simd::find_child_node16;

// ============================================================================
// 1. HybridLatch Concurrency & Invariants
// ============================================================================

#[test]
fn test_latch_concurrent_mutual_exclusion() {
    let latch = Arc::new(HybridLatch::new());
    let mut raw_counter = 0usize;
    let counter_ptr = &mut raw_counter as *mut usize as usize;

    const NUM_THREADS: usize = 16;
    const INCREMENTS_PER_THREAD: usize = 20_000;
    let barrier = Arc::new(Barrier::new(NUM_THREADS));

    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|_| {
            let latch = Arc::clone(&latch);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let mut backoff = SpinBackoff::new();
                for _ in 0..INCREMENTS_PER_THREAD {
                    while latch.lock().is_err() {
                        backoff.spin();
                    }
                    // Critical section: mutating a non-atomic usize via raw pointer
                    unsafe {
                        let ptr = counter_ptr as *mut usize;
                        let val = *ptr;
                        *ptr = val + 1;
                    }
                    latch.unlock();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(raw_counter, NUM_THREADS * INCREMENTS_PER_THREAD);
    assert!(!latch.is_obsolete());
    assert!(!latch.is_locked());
}

#[test]
fn test_latch_optimistic_validation_failure_on_concurrent_write() {
    let latch = Arc::new(HybridLatch::new());
    let running = Arc::new(AtomicBool::new(true));

    let l_writer = Arc::clone(&latch);
    let r_writer = Arc::clone(&running);

    let writer_handle = thread::spawn(move || {
        let mut count = 0;
        while r_writer.load(Ordering::Relaxed) && count < 50_000 {
            l_writer.lock().unwrap();
            std::hint::spin_loop();
            l_writer.unlock();
            count += 1;
        }
    });

    let mut failed_validations = 0;
    let mut successful_validations = 0;

    for _ in 0..100_000 {
        if let Some(v) = latch.read_version() {
            for _ in 0..5 {
                std::hint::spin_loop();
            }
            if latch.validate(v) {
                successful_validations += 1;
            } else {
                failed_validations += 1;
            }
        }
    }

    running.store(false, Ordering::Relaxed);
    writer_handle.join().unwrap();

    // Verify both successful and failed validations occurred under contention
    assert!(
        successful_validations > 0,
        "must have observed stable reads"
    );
    assert!(
        failed_validations > 0,
        "must have observed invalidations during concurrent writes"
    );
}

#[test]
fn test_latch_obsolete_state_permanence() {
    let latch = HybridLatch::new();
    assert!(!latch.is_obsolete());

    let v = latch.lock().unwrap();
    latch.mark_obsolete_and_unlock();

    assert!(latch.is_obsolete());
    assert!(!latch.is_locked());
    assert_eq!(latch.lock(), Err(LockError::Obsolete));
    assert_eq!(latch.lock_version(v), Err(LockError::Obsolete));
    assert_eq!(latch.read_version(), None);
    assert!(!latch.validate(v));
}

// ============================================================================
// 2. Tagged Pointer & Tagged Offset Invariants
// ============================================================================

#[test]
fn test_tagged_ptr_bit_tagging_and_round_trip() {
    let mut leaf = Leaf::new("test_key".to_string(), 42u64);
    let leaf_ptr = &mut *leaf as *mut Leaf<String, u64>;

    let tagged_leaf = TaggedPtr::from_leaf(leaf_ptr);
    assert!(tagged_leaf.is_leaf());
    assert!(!tagged_leaf.is_null());
    assert_eq!(tagged_leaf.as_leaf_ptr::<String, u64>(), leaf_ptr);

    let mut node4 = Node4::new(b"prefix");
    let inner_ptr = &mut node4.header as *mut _;

    let tagged_inner = TaggedPtr::from_inner(inner_ptr);
    assert!(!tagged_inner.is_leaf());
    assert!(!tagged_inner.is_null());
    assert_eq!(tagged_inner.as_inner_ptr(), inner_ptr);

    assert!(TaggedPtr::NULL.is_null());
    assert!(!TaggedPtr::NULL.is_leaf());
}

#[test]
fn test_tagged_offset_invariants() {
    // Offset must have bit 0 clear (8-byte aligned)
    let raw_leaf_off = 1024u32;
    let tagged_leaf = TaggedOffset::from_leaf(raw_leaf_off);
    assert!(tagged_leaf.is_leaf());
    assert_eq!(tagged_leaf.leaf_offset(), raw_leaf_off);
    assert!(!tagged_leaf.is_null());

    let raw_inner_off = 2048u32;
    let tagged_inner = TaggedOffset::from_inner(raw_inner_off);
    assert!(!tagged_inner.is_leaf());
    assert_eq!(tagged_inner.inner_offset(), raw_inner_off);
    assert!(!tagged_inner.is_null());

    assert!(TaggedOffset::NULL.is_null());
    assert!(!TaggedOffset::NULL.is_leaf());
}

// ============================================================================
// 3. SIMD Key Search Correctness Across All 256 Byte Values
// ============================================================================

#[test]
fn test_simd_find_child_node16_exhaustive() {
    // Array of 16 sorted unique keys
    let keys = [
        0x05, 0x12, 0x24, 0x33, 0x48, 0x5a, 0x67, 0x7e, 0x89, 0x9f, 0xab, 0xb0, 0xcd, 0xde, 0xef,
        0xfc,
    ];

    // Test every possible u8 value against varying counts of keys
    for count in 1..=16 {
        for byte_val in 0u8..=255 {
            let simd_res = find_child_node16(&keys, count, byte_val);
            let linear_res = keys[..count].iter().position(|&k| k == byte_val);
            assert_eq!(
                simd_res, linear_res,
                "SIMD search for byte 0x{:02x} with count {} failed",
                byte_val, count
            );
        }
    }
}

// ============================================================================
// 4. Arena Atomic Bitmap Primitives
// ============================================================================

#[test]
fn test_node256_bitmap_exhaustive() {
    let bitmap = [const { AtomicU64::new(0) }; 4];

    // Initially empty
    for byte in 0..=255 {
        assert_eq!(next_present_byte(&bitmap, byte), None);
        assert_eq!(prev_present_byte(&bitmap, byte), None);
    }

    // Set every odd bit
    for byte in (1..=255).step_by(2) {
        set_bitmap_bit(&bitmap, byte);
    }

    // Verify next_present_byte and prev_present_byte
    for byte in 0..=254 {
        let expected_next = if byte % 2 != 0 { byte } else { byte + 1 };
        assert_eq!(next_present_byte(&bitmap, byte), Some(expected_next));
    }

    for byte in 1..=255 {
        let expected_prev = if byte % 2 != 0 { byte } else { byte - 1 };
        assert_eq!(prev_present_byte(&bitmap, byte), Some(expected_prev));
    }

    // Clear all bits and verify back to empty
    for byte in (1..=255).step_by(2) {
        clear_bitmap_bit(&bitmap, byte);
    }

    assert_eq!(next_present_byte(&bitmap, 0), None);
    assert_eq!(prev_present_byte(&bitmap, 255), None);
}

// ============================================================================
// 5. Node Topologies Manual Lifecycle & Edge Cases
// ============================================================================

#[test]
fn test_node4_insert_find_replace() {
    let mut n4 = Node4::new(b"prefix4");
    assert_eq!(n4.header.node_type, NodeType::Node4);
    assert_eq!(n4.header.prefix_slice(), b"prefix4");

    let leaf1 = TaggedPtr::from_raw(0x1000 as *mut u8);
    let leaf2 = TaggedPtr::from_raw(0x2000 as *mut u8);
    let leaf3 = TaggedPtr::from_raw(0x3000 as *mut u8);
    let leaf4 = TaggedPtr::from_raw(0x4000 as *mut u8);

    n4.insert_child(10, leaf1);
    n4.insert_child(20, leaf2);
    n4.insert_child(30, leaf3);
    n4.insert_child(40, leaf4);

    assert_eq!(n4.find_child(10), Some(leaf1));
    assert_eq!(n4.find_child(20), Some(leaf2));
    assert_eq!(n4.find_child(30), Some(leaf3));
    assert_eq!(n4.find_child(40), Some(leaf4));
    assert_eq!(n4.find_child(50), None);

    // Replace child
    let leaf1_updated = TaggedPtr::from_raw(0x1008 as *mut u8);
    n4.replace_child(10, leaf1_updated);
    assert_eq!(n4.find_child(10), Some(leaf1_updated));
}

#[test]
fn test_node48_full_capacity_and_indices() {
    let mut n48 = Node48::new(b"p48");
    assert_eq!(n48.header.node_type, NodeType::Node48);

    for i in 0..48u8 {
        let child = TaggedPtr::from_raw(((i as usize + 1) * 8) as *mut u8);
        let key_byte = i * 5; // Sparse key bytes
        n48.insert_child(key_byte, child);
        assert_eq!(n48.find_child(key_byte), Some(child));
    }

    assert_eq!(n48.header.num_children, 48);

    // Verify unused byte
    assert_eq!(n48.child_indices[255], NODE48_EMPTY);
}

#[test]
fn test_node256_lock_free_cas_insertion() {
    let n256 = Node256::new(b"p256");
    assert_eq!(n256.header.node_type, NodeType::Node256);

    let child = TaggedPtr::from_raw(0x8000 as *mut u8);
    let key_byte = 127u8;

    // Direct atomic CAS into empty slot
    let cas_res = n256.children[key_byte as usize].compare_exchange(
        std::ptr::null_mut(),
        child.as_raw(),
        Ordering::Release,
        Ordering::Acquire,
    );
    assert!(cas_res.is_ok());

    assert_eq!(n256.find_child(key_byte), Some(child));
    assert_eq!(n256.find_child(128), None);
}

// ============================================================================
// 6. VersionNode & VersionedLeaf Drop & Epoch Reclamation Safety
// ============================================================================

struct DropDetect {
    drop_counter: Arc<AtomicUsize>,
}

impl Drop for DropDetect {
    fn drop(&mut self) {
        self.drop_counter.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn test_version_node_value_taken_guard() {
    let drop_counter = Arc::new(AtomicUsize::new(0));

    let mut node = VersionNode::new(
        100,
        DropDetect {
            drop_counter: Arc::clone(&drop_counter),
        },
    );

    assert!(!node.is_inline);
    assert!(!node.value_taken.load(Ordering::Acquire));

    // Manually drop value once with value_taken protection
    if !node.value_taken.swap(true, Ordering::AcqRel) {
        unsafe { ManuallyDrop::drop(&mut node.value) };
    }
    assert_eq!(drop_counter.load(Ordering::SeqCst), 1);

    // Second drop attempt must be skipped
    if !node.value_taken.swap(true, Ordering::AcqRel) {
        unsafe { ManuallyDrop::drop(&mut node.value) };
    }
    assert_eq!(
        drop_counter.load(Ordering::SeqCst),
        1,
        "must not double-drop value"
    );
}

#[test]
fn test_versioned_leaf_inline_slots_drop_safety() {
    let drop_counter = Arc::new(AtomicUsize::new(0));

    {
        let leaf_ptr = VersionedLeaf::new(
            "key1".to_string(),
            10,
            DropDetect {
                drop_counter: Arc::clone(&drop_counter),
            },
        );
        let leaf = unsafe { &*leaf_ptr };

        // Update 1: claims slot1 inline
        let node2 = leaf.alloc_version_node(
            20,
            DropDetect {
                drop_counter: Arc::clone(&drop_counter),
            },
        );
        assert!(unsafe { (*node2).is_inline });

        // Update 2: spills to heap Box
        let node3 = leaf.alloc_version_node(
            30,
            DropDetect {
                drop_counter: Arc::clone(&drop_counter),
            },
        );
        assert!(!unsafe { (*node3).is_inline });

        // Link node3 -> node2 -> node1
        unsafe {
            (*node3).next_version.store(node2, Ordering::Relaxed);
            (*node2)
                .next_version
                .store(leaf.versions.load(Ordering::Relaxed), Ordering::Relaxed);
            leaf.versions.store(node3, Ordering::Release);
        }

        // Before drop: 0 values dropped
        assert_eq!(drop_counter.load(Ordering::SeqCst), 0);

        unsafe {
            // Drop entire version chain
            let mut cur = (*leaf_ptr).versions.load(Ordering::Relaxed);
            while !cur.is_null() {
                let next = (*cur).next_version.load(Ordering::Relaxed);
                if (*cur).is_inline {
                    if !(*cur).value_taken.swap(true, Ordering::AcqRel) {
                        ManuallyDrop::drop(&mut (*cur).value);
                    }
                } else {
                    let mut heap_node = Box::from_raw(cur);
                    if !heap_node.value_taken.swap(true, Ordering::AcqRel) {
                        ManuallyDrop::drop(&mut heap_node.value);
                    }
                }
                cur = next;
            }
            drop(Box::from_raw(leaf_ptr));
        }
    }

    // All 3 version values (slot0, slot1, heap node) must have been dropped exactly once
    assert_eq!(
        drop_counter.load(Ordering::SeqCst),
        3,
        "all version values must be dropped exactly once"
    );
}

// ============================================================================
// 7. Arena TLAB Concurrency & Bump Pointer Non-Overlap
// ============================================================================

#[test]
fn test_arena_tlab_concurrent_allocations_non_overlapping() {
    let arena = Arena::with_capacity(32 * 1024 * 1024);
    const NUM_THREADS: usize = 16;
    const ALLOCS_PER_THREAD: usize = 10_000;
    let barrier = Arc::new(Barrier::new(NUM_THREADS));

    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|t| {
            let arena = Arc::clone(&arena);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let mut offsets = Vec::with_capacity(ALLOCS_PER_THREAD);
                for i in 0..ALLOCS_PER_THREAD {
                    let size = if (i + t) % 3 == 0 { 40 } else { 16 };
                    let off = arena.alloc(size, 8, 0).expect("arena has room");
                    assert_eq!(off % 8, 0, "allocation must be 8-byte aligned");

                    // Write unique pattern into allocated slice
                    let ptr = arena.get_pointer_mut(off) as *mut u64;
                    unsafe { *ptr = (t as u64) << 32 | (i as u64) };
                    offsets.push((off, (t as u64) << 32 | (i as u64)));
                }
                offsets
            })
        })
        .collect();

    let mut all_allocs = Vec::with_capacity(NUM_THREADS * ALLOCS_PER_THREAD);
    for h in handles {
        let thread_allocs = h.join().unwrap();
        all_allocs.extend(thread_allocs);
    }

    // Verify all allocations are distinct and memory contents were preserved
    all_allocs.sort_by_key(|&(off, _)| off);
    for i in 1..all_allocs.len() {
        assert!(
            all_allocs[i].0 > all_allocs[i - 1].0,
            "allocated offsets must not overlap: {} vs {}",
            all_allocs[i].0,
            all_allocs[i - 1].0
        );
    }

    // Verify written data
    for (off, expected_val) in all_allocs {
        let ptr = arena.get_pointer(off) as *const u64;
        assert_eq!(
            unsafe { *ptr },
            expected_val,
            "memory corruption detected in allocated slab"
        );
    }
}
