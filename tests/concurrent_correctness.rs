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

//! Real concurrent correctness tests (§16.5): no global lock, persistent
//! threads, and invariants that must hold in every interleaving.
//!
//! Scale with `ARTMAP_STRESS_MS` (default 300 ms per test; tiny under Miri).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use artmap::{ArenaVersionedArtMap, ArtMap, VersionedArtMap};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

fn budget() -> Duration {
    if cfg!(miri) {
        return Duration::from_millis(1);
    }
    let ms = std::env::var("ARTMAP_STRESS_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300);
    Duration::from_millis(ms)
}

fn threads() -> usize {
    if cfg!(miri) {
        2
    } else {
        4
    }
}

/// Runs `f(thread, stop)` on `n` persistent threads until `budget()` elapses.
fn run<F>(n: usize, f: F)
where
    F: Fn(usize, &AtomicBool) + Send + Sync + 'static,
{
    let f = Arc::new(f);
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(n + 1));
    let handles: Vec<_> = (0..n)
        .map(|t| {
            let (f, stop, barrier) = (Arc::clone(&f), Arc::clone(&stop), Arc::clone(&barrier));
            std::thread::spawn(move || {
                barrier.wait();
                f(t, &stop);
            })
        })
        .collect();
    barrier.wait();
    let start = Instant::now();
    while start.elapsed() < budget() {
        std::thread::sleep(Duration::from_millis(1));
    }
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
}

fn key(i: u64) -> [u8; 8] {
    i.to_be_bytes()
}

#[test]
fn overwrite_never_hides_the_key() {
    let map = Arc::new(ArtMap::<[u8; 8], u64>::new());
    for i in 0..64 {
        map.insert(key(i), i);
    }
    let m = Arc::clone(&map);
    run(threads(), move |t, stop| {
        let mut i = 0u64;
        while !stop.load(Ordering::Relaxed) {
            i += 1;
            if t == 0 {
                // One writer overwrites every key, over and over.
                let old = m.insert(key(i % 64), i);
                assert!(old.is_some(), "an overwrite always displaces an entry");
            } else {
                let k = key(i % 64);
                assert!(m.get(&k).is_some(), "get never misses an overwritten key");
                assert!(m.contains_key(&k));
                assert_eq!(m.range(k..=k).count(), 1, "a point range sees the key");
            }
        }
    });
    assert_eq!(map.len(), 64);
}

#[test]
fn sentinels_survive_churn_in_scans() {
    // Sentinels at even keys are never removed; odd keys churn, so nodes grow,
    // shift and split around them all the time.
    const N: u64 = 512;
    let map = Arc::new(ArtMap::<[u8; 8], u64>::new());
    for i in (0..N).step_by(2) {
        map.insert(key(i), i);
    }
    let m = Arc::clone(&map);
    run(threads(), move |t, stop| {
        let mut rng = StdRng::seed_from_u64(t as u64);
        while !stop.load(Ordering::Relaxed) {
            if t % 2 == 0 {
                let k = rng.gen_range(0..N) | 1;
                if rng.gen() {
                    m.insert(key(k), k);
                } else {
                    m.remove(&key(k));
                }
            } else {
                let fwd: Vec<u64> = m
                    .iter()
                    .map(|e| u64::from_be_bytes(*e.key()))
                    .filter(|k| k % 2 == 0)
                    .collect();
                assert_eq!(fwd.len() as u64, N / 2, "every sentinel exactly once");
                assert!(fwd.windows(2).all(|w| w[0] < w[1]), "in order");
                let rev: Vec<u64> = m
                    .iter()
                    .rev()
                    .map(|e| u64::from_be_bytes(*e.key()))
                    .filter(|k| k % 2 == 0)
                    .collect();
                assert_eq!(
                    rev.len() as u64,
                    N / 2,
                    "every sentinel exactly once, reversed"
                );
                let lo = rng.gen_range(0..N / 2) * 2;
                let got = m
                    .range(key(lo)..key(lo + 64))
                    .filter(|e| u64::from_be_bytes(*e.key()) % 2 == 0)
                    .count();
                assert_eq!(
                    got as u64,
                    32.min((N - lo) / 2),
                    "range sees every sentinel"
                );
            }
        }
    });
}

#[test]
fn partitioned_oracle() {
    // Each thread owns the keys `k % threads == t` and checks every result
    // against its own model, while the others churn neighbouring keys.
    let n = threads();
    let map = Arc::new(ArtMap::<Vec<u8>, u64>::new());
    let m = Arc::clone(&map);
    let totals = Arc::new(AtomicUsize::new(0));
    let tot = Arc::clone(&totals);
    run(n, move |t, stop| {
        let mut rng = StdRng::seed_from_u64(100 + t as u64);
        let mut model = BTreeMap::new();
        while !stop.load(Ordering::Relaxed) {
            // Shared prefixes, so ownership partitions interleave in nodes.
            let id = rng.gen_range(0..200u64) * n as u64 + t as u64;
            let k = format!("p:{}:{id}", id % 7).into_bytes();
            match rng.gen_range(0..3) {
                0 => {
                    let v = rng.gen();
                    assert_eq!(m.insert(k.clone(), v).map(|e| *e), model.insert(k, v));
                }
                1 => assert_eq!(m.remove(&k).map(|e| *e), model.remove(&k)),
                _ => assert_eq!(m.get(&k).map(|e| *e), model.get(&k).copied()),
            }
        }
        tot.fetch_add(model.len(), Ordering::Relaxed);
    });
    let mut map = Arc::try_unwrap(map).ok().unwrap();
    assert_eq!(map.len(), totals.load(Ordering::Relaxed));
    map.validate_invariants();
}

#[test]
fn fresh_inserts_and_removes_are_exact_during_prefix_splits() {
    // Thread 0 owns key k1 and alternates insert/remove on it: every insert
    // must return None and every remove Some. The others keep splitting the
    // prefixes around k1.
    let map = Arc::new(ArtMap::<Vec<u8>, u64>::new());
    let m = Arc::clone(&map);
    run(threads(), move |t, stop| {
        let mut rng = StdRng::seed_from_u64(t as u64);
        let k1 = b"abcdefgh:0".to_vec();
        let mut i = 0u64;
        while !stop.load(Ordering::Relaxed) {
            i += 1;
            if t == 0 {
                assert!(
                    m.insert(k1.clone(), i).is_none(),
                    "fresh insert returned Some"
                );
                assert!(m.get(&k1).is_some(), "own key missing");
                assert!(m.remove(&k1).is_some(), "remove of own key returned None");
            } else {
                let cut = rng.gen_range(1..9);
                let mut k = b"abcdefgh".to_vec();
                k.truncate(cut);
                k.push(rng.gen_range(b'x'..=b'z'));
                k.push(t as u8);
                if rng.gen() {
                    m.insert(k, i);
                } else {
                    m.remove(&k);
                }
            }
        }
    });
    let mut map = Arc::try_unwrap(map).ok().unwrap();
    assert_eq!(map.len(), map.iter().count());
    map.validate_invariants();
}

#[test]
fn clear_against_writers_keeps_len_exact() {
    let map = Arc::new(ArtMap::<[u8; 8], u64>::new());
    let m = Arc::clone(&map);
    run(threads(), move |t, stop| {
        let mut rng = StdRng::seed_from_u64(t as u64);
        while !stop.load(Ordering::Relaxed) {
            if t == 0 {
                m.clear();
                assert!(m.len() < 1 << 40, "len never wraps");
            } else {
                let k = rng.gen_range(0..2048u64);
                if rng.gen_ratio(2, 3) {
                    m.insert(key(k), k);
                } else {
                    m.remove(&key(k));
                }
            }
        }
    });
    let mut map = Arc::try_unwrap(map).ok().unwrap();
    assert_eq!(map.len(), map.iter().count(), "len is exact at quiescence");
    map.validate_invariants();
}

#[test]
fn node256_inserters_and_updater() {
    // A pre-grown Node256 at the root; inserters add fresh first bytes under
    // it while an updater overwrites existing ones.
    for _round in 0..if cfg!(miri) { 1 } else { 20 } {
        let map = Arc::new(ArtMap::<Vec<u8>, u64>::new());
        for b in 0..64u8 {
            map.insert(vec![b], 0);
        }
        let n = threads();
        let barrier = Arc::new(Barrier::new(n));
        let handles: Vec<_> = (0..n)
            .map(|t| {
                let (m, barrier) = (Arc::clone(&map), Arc::clone(&barrier));
                std::thread::spawn(move || {
                    barrier.wait();
                    if t == 0 {
                        for i in 0..512u64 {
                            m.insert(vec![(i % 64) as u8], i);
                        }
                    } else {
                        for b in (64..=255u8).filter(|b| (*b as usize) % (n - 1) == t - 1) {
                            assert!(m.insert(vec![b], 1).is_none(), "fresh key returned Some");
                            // Second level under the same first byte.
                            assert!(m.insert(vec![b, 1], 1).is_none());
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let mut map = Arc::try_unwrap(map).ok().unwrap();
        assert_eq!(map.len(), 256 + 192);
        for b in 0..=255u8 {
            assert!(map.get(&vec![b]).is_some(), "key {b} lost");
        }
        map.validate_invariants();
    }
}

#[test]
fn shared_first_byte_node256_inserts() {
    // 8-way contention on one second-level node that grows to a Node256.
    let map = Arc::new(ArtMap::<[u8; 2], u64>::new());
    let n = if cfg!(miri) { 2 } else { 8 };
    let barrier = Arc::new(Barrier::new(n));
    let handles: Vec<_> = (0..n)
        .map(|t| {
            let (m, barrier) = (Arc::clone(&map), Arc::clone(&barrier));
            std::thread::spawn(move || {
                barrier.wait();
                let per = if cfg!(miri) { 4 } else { 64 };
                for b0 in 0..per as u8 {
                    for b1 in (t..256).step_by(n) {
                        assert!(m.insert([b0, b1 as u8], 1).is_none());
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let mut map = Arc::try_unwrap(map).ok().unwrap();
    let per = if cfg!(miri) { 4 } else { 64 };
    assert_eq!(map.len(), per * 256);
    assert_eq!(map.iter().count(), per * 256);
    map.validate_invariants();
}

#[test]
fn handle_outlives_concurrent_remove() {
    let map = Arc::new(ArtMap::<String, String>::new());
    map.insert("k".into(), "a value that owns heap memory".into());
    let e = map.get("k").unwrap();
    let m = Arc::clone(&map);
    std::thread::spawn(move || {
        assert!(m.remove("k").is_some());
        for _ in 0..if cfg!(miri) { 64 } else { 4096 } {
            // Drive the epoch: the removed leaf must survive while `e` pins.
            let _ = m.get("other");
        }
    })
    .join()
    .unwrap();
    assert!(e.is_removed());
    assert_eq!(e.value(), "a value that owns heap memory");
    assert!(!e.remove());
}

#[test]
fn versioned_concurrent_versions_of_one_key() {
    // Every thread inserts distinct versions of the same keys, out of order;
    // one thread also re-writes some versions (same-version replace).
    let map = Arc::new(VersionedArtMap::<[u8; 8], u64>::new());
    let n = threads() as u64;
    let per = if cfg!(miri) { 8 } else { 400 };
    let barrier = Arc::new(Barrier::new(n as usize));
    let handles: Vec<_> = (0..n)
        .map(|t| {
            let (m, barrier) = (Arc::clone(&map), Arc::clone(&barrier));
            std::thread::spawn(move || {
                barrier.wait();
                let mut rng = StdRng::seed_from_u64(t);
                let mut versions: Vec<u64> = (0..per).map(|i| i * n + t + 1).collect();
                // Shuffle to exercise out-of-order inserts.
                for i in (1..versions.len()).rev() {
                    versions.swap(i, rng.gen_range(0..=i));
                }
                for v in versions {
                    for k in 0..4 {
                        m.insert(key(k), v, v);
                        if v % 5 == 0 {
                            m.insert(key(k), v, v); // same-version replace
                        }
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    for k in 0..4 {
        let all = map.get_all_versions(&key(k));
        assert_eq!(all.len() as u64, per * n, "no version lost or duplicated");
        assert!(
            all.windows(2).all(|w| w[0].0 > w[1].0),
            "newest first, strictly"
        );
        assert!(all.iter().all(|(v, x)| *x == Some(*v)));
    }
    assert_eq!(map.len(), 4);
}

#[test]
fn versioned_prune_against_inserts_and_prunes() {
    let map = Arc::new(VersionedArtMap::<[u8; 8], u64>::new());
    let next = Arc::new(AtomicU64::new(1));
    let m = Arc::clone(&map);
    let nx = Arc::clone(&next);
    run(threads(), move |t, stop| {
        let mut rng = StdRng::seed_from_u64(t as u64);
        while !stop.load(Ordering::Relaxed) {
            let k = key(rng.gen_range(0..8));
            match t % 3 {
                0 => {
                    let v = nx.fetch_add(1, Ordering::Relaxed);
                    m.insert(k, v, v);
                }
                1 => {
                    let v = nx.fetch_add(1, Ordering::Relaxed);
                    m.delete(k, v);
                }
                _ => {
                    let min = nx
                        .load(Ordering::Relaxed)
                        .saturating_sub(rng.gen_range(0..50));
                    m.prune_key(&k, min, |_| false);
                    if let Some((v, x)) = m.get_version_le(&k, u64::MAX) {
                        assert_eq!(v, x);
                    }
                }
            }
        }
    });
    // Consistency after the storm: chains are sorted and len matches heads.
    let mut live = 0;
    for k in 0..8 {
        let all = map.get_all_versions(&key(k));
        assert!(all.windows(2).all(|w| w[0].0 > w[1].0));
        if all.first().is_some_and(|(_, x)| x.is_some()) {
            live += 1;
        }
    }
    assert_eq!(map.len(), live);
}

#[test]
fn versioned_len_under_remove_and_reinsert() {
    let map = Arc::new(VersionedArtMap::<[u8; 8], u64>::new());
    let next = Arc::new(AtomicU64::new(1));
    let (m, nx) = (Arc::clone(&map), Arc::clone(&next));
    run(threads(), move |t, stop| {
        let mut rng = StdRng::seed_from_u64(t as u64);
        while !stop.load(Ordering::Relaxed) {
            let k = key(rng.gen_range(0..4));
            let v = nx.fetch_add(1, Ordering::Relaxed);
            if rng.gen() {
                m.insert(k, v, v);
            } else {
                #[allow(deprecated)]
                let _ = m.remove(&k);
            }
            assert!(m.len() <= 4, "len never exceeds the key space");
        }
    });
    let live = (0..4).filter(|k| map.get(&key(*k)).is_some()).count();
    assert_eq!(map.len(), live);
}

#[test]
fn arena_versioned_concurrent_versions_of_one_key() {
    // As `versioned_concurrent_versions_of_one_key`, on the arena chain.
    let map = Arc::new(ArenaVersionedArtMap::<[u8; 8], u64>::with_capacity(
        64 << 20,
    ));
    let n = threads() as u64;
    let per = if cfg!(miri) { 8 } else { 400 };
    let barrier = Arc::new(Barrier::new(n as usize));
    let handles: Vec<_> = (0..n)
        .map(|t| {
            let (m, barrier) = (Arc::clone(&map), Arc::clone(&barrier));
            std::thread::spawn(move || {
                barrier.wait();
                let mut rng = StdRng::seed_from_u64(t);
                let mut versions: Vec<u64> = (0..per).map(|i| i * n + t + 1).collect();
                for i in (1..versions.len()).rev() {
                    versions.swap(i, rng.gen_range(0..=i));
                }
                for v in versions {
                    for k in 0..4 {
                        m.insert(key(k), v, v);
                        if v % 5 == 0 {
                            m.insert(key(k), v, v); // same-version replace
                        }
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    for k in 0..4 {
        let all = map.get_all_versions(&key(k));
        assert_eq!(all.len() as u64, per * n, "no version lost or duplicated");
        assert!(
            all.windows(2).all(|w| w[0].0 > w[1].0),
            "newest first, strictly"
        );
        assert!(all.iter().all(|(v, x)| *x == Some(*v)));
    }
    assert_eq!(map.len(), 4);
}

#[test]
fn arena_versioned_len_under_delete_and_reinsert() {
    // Inserts and deletes of newer versions race on a few keys: `len` never
    // exceeds the key space and equals the live-head count at quiescence.
    const CAP: u64 = 400_000;
    let map = Arc::new(ArenaVersionedArtMap::<[u8; 8], u64>::with_capacity(
        128 << 20,
    ));
    let next = Arc::new(AtomicU64::new(1));
    let (m, nx) = (Arc::clone(&map), Arc::clone(&next));
    run(threads(), move |t, stop| {
        let mut rng = StdRng::seed_from_u64(t as u64);
        while !stop.load(Ordering::Relaxed) {
            let v = nx.fetch_add(1, Ordering::Relaxed);
            if v > CAP {
                break;
            }
            let k = key(rng.gen_range(0..4));
            if rng.gen() {
                m.insert(k, v, v);
            } else {
                m.delete(k, v);
            }
            assert!(m.len() <= 4, "len never exceeds the key space");
        }
    });
    let mut live = 0;
    for k in 0..4 {
        let all = map.get_all_versions(&key(k));
        assert!(
            all.windows(2).all(|w| w[0].0 > w[1].0),
            "chains stay sorted"
        );
        if all.first().is_some_and(|(_, x)| x.is_some()) {
            live += 1;
        }
    }
    assert_eq!(map.len(), live);
    assert_eq!(map.iter().count(), live);
}
