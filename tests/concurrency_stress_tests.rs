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

//! # Concurrency Stress & Deadlock Prevention Tests
//!
//! Stress-tests high-contention scenarios prone to race conditions, deadlocks, and hangs:
//! - Concurrent root expansion (empty -> leaf -> Node4 -> Node16 -> Node48 -> Node256).
//! - Extreme write contention on a single hot key in [`VersionedArtMap`].
//! - Continuous concurrent updates, snapshot reads, and background watermark pruning.
//! - Deep prefix compression and chain splitting across 16-byte boundary boundaries.
//! - Bidirectional range scans running concurrently with active tree mutations.
//! - High-concurrency operations on [`ArenaArtMap`] and [`ArenaVersionedArtMap`].

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use artmap::arena::{Arena, ArenaArtMap, ArenaInserter, ArenaVersionedArtMap};
use artmap::versioned::VersionedArtMap;
use artmap::ArtMap;

// ============================================================================
// 1. Concurrent Root Growth & Prefix Splits Stress Test
// ============================================================================

#[test]
fn test_stress_concurrent_root_growth_and_splits() {
    let mut map = Arc::new(ArtMap::<[u8; 8], u64>::new());
    const NUM_THREADS: usize = 24;
    const KEYS_PER_THREAD: usize = 1_000;
    let barrier = Arc::new(Barrier::new(NUM_THREADS));

    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|t| {
            let map = Arc::clone(&map);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for i in 0..KEYS_PER_THREAD {
                    // Unique keys structured to trigger multi-level branching and root expansion
                    let key = if i % 2 == 0 {
                        ((t as u64) << 48 | (i as u64)).to_be_bytes()
                    } else {
                        ((t as u64) << 48 | 0x8000_0000 | (i as u64)).to_be_bytes()
                    };
                    map.insert(key, (t * KEYS_PER_THREAD + i) as u64);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(map.len(), NUM_THREADS * KEYS_PER_THREAD);
    Arc::get_mut(&mut map).unwrap().validate_invariants();
}

// ============================================================================
// 2. Hot-Key Version Contention in VersionedArtMap
// ============================================================================

#[test]
fn test_stress_hot_key_version_contention() {
    let map = Arc::new(VersionedArtMap::<String, usize>::new());
    const NUM_THREADS: usize = 24;
    const UPDATES_PER_THREAD: usize = 500;
    let barrier = Arc::new(Barrier::new(NUM_THREADS));

    // Hammer the EXACT same key simultaneously from 24 threads
    let hot_key = "global:singleton:counter".to_string();

    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|t| {
            let map = Arc::clone(&map);
            let barrier = Arc::clone(&barrier);
            let k = hot_key.clone();
            thread::spawn(move || {
                barrier.wait();
                for u in 1..=UPDATES_PER_THREAD {
                    let version = (u * NUM_THREADS + t) as u64;
                    let val = t * 10_000 + u;
                    map.insert(k.clone(), version, val);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(map.len(), 1);

    // Verify all 12,000 versions were captured in strictly descending order
    let versions = map.get_all_versions(&hot_key);
    assert_eq!(versions.len(), NUM_THREADS * UPDATES_PER_THREAD);

    for i in 1..versions.len() {
        assert!(
            versions[i - 1].0 > versions[i].0,
            "version chain corrupted: {} not greater than {}",
            versions[i - 1].0,
            versions[i].0
        );
    }
}

// ============================================================================
// 3. Concurrent Writers, Readers, and Watermark Pruning
// ============================================================================

#[test]
fn test_stress_concurrent_writes_reads_and_pruning() {
    let map = Arc::new(VersionedArtMap::<String, Option<usize>>::new());
    let running = Arc::new(AtomicBool::new(true));

    const NUM_WRITERS: usize = 8;
    const NUM_READERS: usize = 8;
    const TOTAL_THREADS: usize = NUM_WRITERS + NUM_READERS + 1; // +1 pruner
    let barrier = Arc::new(Barrier::new(TOTAL_THREADS));

    const NUM_KEYS: usize = 100;

    // Pre-populate keys
    for k in 0..NUM_KEYS {
        map.insert(format!("account:{k:03}"), 1, Some(k));
    }

    let mut handles = Vec::new();

    // Writers: constantly advance versions and occasionally insert tombstones
    for w_id in 0..NUM_WRITERS {
        let map = Arc::clone(&map);
        let running = Arc::clone(&running);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut v = 10u64 + w_id as u64;
            while running.load(Ordering::Relaxed) {
                let k = (v as usize) % NUM_KEYS;
                let val = if v.is_multiple_of(10) {
                    None
                } else {
                    Some(v as usize)
                };
                map.insert(format!("account:{k:03}"), v, val);
                v += NUM_WRITERS as u64;
                if v > 100_000 {
                    break;
                }
            }
        }));
    }

    // Readers: constantly perform point snapshot lookups
    for r_id in 0..NUM_READERS {
        let map = Arc::clone(&map);
        let running = Arc::clone(&running);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut counter = 0;
            while running.load(Ordering::Relaxed) && counter < 50_000 {
                let k = (counter + r_id) % NUM_KEYS;
                let key_str = format!("account:{k:03}");
                let _ = map.get_version_le(&key_str, 50_000);
                let _ = map.get_latest(&key_str);
                counter += 1;
            }
        }));
    }

    // Pruner: sweeps older versions with an advancing watermark
    {
        let map = Arc::clone(&map);
        let running = Arc::clone(&running);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut watermark = 10u64;
            while running.load(Ordering::Relaxed) && watermark < 80_000 {
                for k in 0..NUM_KEYS {
                    let key_str = format!("account:{k:03}");
                    map.prune_key(&key_str, watermark, |v| v.is_none());
                }
                watermark += 1_000;
                thread::sleep(Duration::from_millis(2));
            }
        }));
    }

    // Let the workload run
    thread::sleep(Duration::from_millis(50));
    running.store(false, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }
}

// ============================================================================
// 4. Deep Prefix Compression & Boundary Splitting (0, 1, 15, 16, 17, 32 bytes)
// ============================================================================

#[test]
fn test_stress_deep_prefix_compression_branches() {
    let mut map = Arc::new(ArtMap::<Vec<u8>, usize>::new());

    // Generate keys with prefixes of exactly 0, 1, 15, 16 (MAX_PREFIX_LEN), 17, 32 bytes
    let prefix_lengths = [0, 1, 15, 16, 17, 32, 64];
    const KEYS_PER_PREFIX: usize = 100;
    const NUM_THREADS: usize = 8;
    let barrier = Arc::new(Barrier::new(NUM_THREADS));

    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|t| {
            let map = Arc::clone(&map);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for &plen in &prefix_lengths {
                    for i in 0..KEYS_PER_PREFIX {
                        let mut key = vec![b'P'; plen];
                        let id = ((t * KEYS_PER_PREFIX + i) as u32).to_be_bytes();
                        key.extend_from_slice(&id);
                        map.insert(key, t * KEYS_PER_PREFIX + i);
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(
        map.len(),
        prefix_lengths.len() * KEYS_PER_PREFIX * NUM_THREADS
    );
    Arc::get_mut(&mut map).unwrap().validate_invariants();
}

// ============================================================================
// 5. Bidirectional Range Scans Concurrent with Active Mutations
// ============================================================================

#[test]
fn test_stress_concurrent_range_scans_during_mutations() {
    let map = Arc::new(ArtMap::<String, usize>::new());
    let running = Arc::new(AtomicBool::new(true));

    const NUM_WRITERS: usize = 4;
    const NUM_SCANNERS: usize = 4;
    let barrier = Arc::new(Barrier::new(NUM_WRITERS + NUM_SCANNERS));

    // Pre-populate keys
    for i in 0..1_000 {
        map.insert(format!("key:{i:04}"), i);
    }

    let mut handles = Vec::new();

    // Writers: constantly inserting, updating, and removing keys
    for w in 0..NUM_WRITERS {
        let map = Arc::clone(&map);
        let running = Arc::clone(&running);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut i = 0;
            while running.load(Ordering::Relaxed) && i < 20_000 {
                let k = format!("key:{:04}", (w * 10_000 + i) % 2_000);
                if i % 3 == 0 {
                    map.remove(&k);
                } else {
                    map.insert(k, i);
                }
                i += 1;
            }
        }));
    }

    // Scanners: forward and reverse iterators
    for _ in 0..NUM_SCANNERS {
        let map = Arc::clone(&map);
        let running = Arc::clone(&running);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut iters = 0;
            while running.load(Ordering::Relaxed) && iters < 500 {
                // Forward range scan
                let mut prev_key: Option<String> = None;
                for entry in map.range("key:0100".."key:0900") {
                    if let Some(ref p) = prev_key {
                        if entry.key() <= p {
                            panic!(
                                "range scan not ascending: current={:?} <= prev={:?}",
                                entry.key(),
                                p
                            );
                        }
                    }
                    prev_key = Some(entry.key().clone());
                }

                // Reverse range scan
                let mut next_key: Option<String> = None;
                for entry in map.range("key:0100".."key:0900").rev() {
                    if let Some(ref n) = next_key {
                        assert!(
                            entry.key() < n,
                            "reverse range scan must remain strictly descending"
                        );
                    }
                    next_key = Some(entry.key().clone());
                }

                iters += 1;
            }
        }));
    }

    thread::sleep(Duration::from_millis(50));
    running.store(false, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }
}

// ============================================================================
// 6. ArenaArtMap & ArenaVersionedArtMap High-Concurrency Stress
// ============================================================================

#[test]
fn test_stress_arena_artmap_concurrent_growth() {
    let arena = Arena::with_capacity(64 * 1024 * 1024);
    let map = Arc::new(ArenaArtMap::<[u8; 8], u64>::new(arena));

    const NUM_THREADS: usize = 16;
    const KEYS_PER_THREAD: usize = 2_500;
    let barrier = Arc::new(Barrier::new(NUM_THREADS));

    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|t| {
            let map = Arc::clone(&map);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let start = (t * KEYS_PER_THREAD) as u64;
                for i in 0..KEYS_PER_THREAD as u64 {
                    let k = (start + i).to_be_bytes();
                    map.insert(k, start + i);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(map.len(), NUM_THREADS * KEYS_PER_THREAD);

    // Verify all keys can be queried correctly
    for t in 0..NUM_THREADS {
        let start = (t * KEYS_PER_THREAD) as u64;
        for i in 0..KEYS_PER_THREAD as u64 {
            let k = (start + i).to_be_bytes();
            assert_eq!(map.get(&k), Some(start + i));
        }
    }
}

#[test]
fn test_stress_arena_versioned_hot_key_updates() {
    let arena = Arena::with_capacity(32 * 1024 * 1024);
    let map = Arc::new(ArenaVersionedArtMap::<String, usize>::new(arena));

    const NUM_THREADS: usize = 16;
    const UPDATES_PER_THREAD: usize = 500;
    let barrier = Arc::new(Barrier::new(NUM_THREADS));
    let hot_key = "hot:item:stock".to_string();

    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|t| {
            let map = Arc::clone(&map);
            let barrier = Arc::clone(&barrier);
            let k = hot_key.clone();
            thread::spawn(move || {
                barrier.wait();
                for u in 1..=UPDATES_PER_THREAD {
                    let version = (u * NUM_THREADS + t) as u64;
                    map.insert(k.clone(), version, t * 1000 + u);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(map.len(), 1);
    let versions = map.get_all_versions(&hot_key);
    assert_eq!(versions.len(), NUM_THREADS * UPDATES_PER_THREAD);

    for i in 1..versions.len() {
        assert!(
            versions[i - 1].0 > versions[i].0,
            "arena versions must be in strictly descending order"
        );
    }
}

// ============================================================================
// 7. Inserter Cache Concurrency & Cache Invalidation Resilience
// ============================================================================

#[test]
fn test_stress_arena_inserter_concurrent_cache_resilience() {
    let arena = Arena::with_capacity(64 * 1024 * 1024);
    let map = Arc::new(ArenaArtMap::<[u8; 8], u64>::new(arena));
    const NUM_INSERTERS: usize = 8;
    const KEYS_PER_INSERTER: usize = 2_000;
    let barrier = Arc::new(Barrier::new(NUM_INSERTERS));

    let handles: Vec<_> = (0..NUM_INSERTERS)
        .map(|t| {
            let map = Arc::clone(&map);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let mut ins = ArenaInserter::new();
                let start = (t * KEYS_PER_INSERTER) as u64;
                for i in 0..KEYS_PER_INSERTER as u64 {
                    let k = (start + i).to_be_bytes();
                    map.insert_with_inserter(k, start + i, &mut ins);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(map.len(), NUM_INSERTERS * KEYS_PER_INSERTER);
    for t in 0..NUM_INSERTERS {
        let start = (t * KEYS_PER_INSERTER) as u64;
        for i in 0..KEYS_PER_INSERTER as u64 {
            let k = (start + i).to_be_bytes();
            assert_eq!(map.get(&k), Some(start + i));
        }
    }
}
