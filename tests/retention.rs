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

//! Memory retention under delete churn (§13, [core-write-5]): removes
//! reclaim the inner nodes they empty, so live memory follows the live keys.
//!
//! Measured with a counting global allocator, so this is its own test binary,
//! and the scenarios run in one test so that nothing else allocates while
//! they measure. Not run under Miri: it needs hundreds of thousands of
//! operations to tell retention from noise.

#![cfg(not(miri))]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use artmap::ArtMap;

struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);

// SAFETY: forwards to the system allocator and only adds a relaxed counter.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller upholds `GlobalAlloc::alloc`'s contract.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from `System.alloc` with this `layout`.
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn live() -> usize {
    LIVE.load(Ordering::Relaxed)
}

/// Pins repeatedly, so the epoch advances and deferred destructors run.
fn settle() {
    let m = ArtMap::<Vec<u8>, u8>::new();
    for _ in 0..4096 {
        drop(m.pin());
        crossbeam_epoch::pin().flush();
    }
}

/// Live bytes held by `map` beyond `base`, after reclamation settles.
fn held(base: usize) -> usize {
    settle();
    live().saturating_sub(base)
}

/// A queue: append at one end, remove a window later. In 0.6 every
/// emptied node stayed, about 8 bytes per operation (4 MB here).
fn sliding_window(prefix: &[u8]) -> usize {
    settle();
    let base = live();
    let map = ArtMap::<Vec<u8>, u64>::new();
    const WINDOW: u64 = 1_000;
    for i in 0..500_000u64 {
        let mut k = prefix.to_vec();
        k.extend_from_slice(&i.to_be_bytes());
        map.insert(k, i);
        if i >= WINDOW {
            let mut old = prefix.to_vec();
            old.extend_from_slice(&(i - WINDOW).to_be_bytes());
            assert!(map.remove(&old).is_some());
        }
    }
    assert_eq!(map.len(), WINDOW as usize);
    held(base)
}

/// A registry: random keys registered, then all deregistered. In 0.6
/// the emptied tree kept every inner node it ever had.
fn registry_to_empty() -> usize {
    settle();
    let base = live();
    let map = ArtMap::<[u8; 16], u64>::new();
    let key = |i: u64| {
        let h = i.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(17) ^ i;
        let mut k = [0u8; 16];
        k[..8].copy_from_slice(&h.to_be_bytes());
        k[8..].copy_from_slice(&h.wrapping_mul(31).to_le_bytes());
        k
    };
    for round in 0..4u64 {
        for i in 0..50_000u64 {
            map.insert(key(round << 32 | i), i);
        }
        for i in (0..50_000u64).rev() {
            assert!(map.remove(&key(round << 32 | i)).is_some());
        }
    }
    assert!(map.is_empty());
    held(base)
}

/// A registry thinned to one key in a hundred: removes leave the large nodes
/// that still hold an entry, and `shrink_to_fit` fits them. Returns the held
/// bytes before and after fitting, and with no map at all for the live keys.
fn thinned_registry() -> (usize, usize) {
    settle();
    let base = live();
    let map = ArtMap::<[u8; 8], u64>::new();
    let key = |i: u64| (i.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (i >> 7)).to_be_bytes();
    for i in 0..200_000u64 {
        map.insert(key(i), i);
    }
    for i in (0..200_000u64).filter(|i| i % 100 != 0) {
        assert!(map.remove(&key(i)).is_some());
    }
    assert_eq!(map.len(), 2_000);
    let thinned = held(base);
    map.shrink_to_fit();
    (thinned, held(base))
}

#[test]
fn delete_churn_does_not_retain_inner_nodes() {
    let queue = sliding_window(b"");
    // A 40-byte shared prefix: the keys sit under a chain of Node4s.
    let chained = sliding_window(&[b'q'; 40]);
    let registry = registry_to_empty();
    let (thinned, fitted) = thinned_registry();
    eprintln!(
        "held bytes: queue {queue}, chained queue {chained}, emptied registry {registry}, \
         thinned registry {thinned} then {fitted} after shrink_to_fit"
    );
    // 1,000 live keys need about 50 KB. The bounds leave room for the
    // destructors still deferred in a thread's epoch bag.
    assert!(queue < 1 << 20, "queue holds {queue} bytes for 1,000 keys");
    assert!(chained < 1 << 20, "chained queue holds {chained} bytes");
    assert!(
        registry < 256 << 10,
        "an emptied registry holds {registry} bytes"
    );
    // 2,000 live keys; fitting releases the nodes sized for 200,000.
    assert!(
        fitted * 2 < thinned,
        "shrink_to_fit kept {fitted} of {thinned} bytes"
    );
    assert!(fitted < 512 << 10, "a fitted registry holds {fitted} bytes");
}
