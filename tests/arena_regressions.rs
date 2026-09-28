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

//! Regression tests for the arena maps (§17.0, §17.6, §17.7). These never
//! pin an epoch, so Miri runs them leak-checked, with strict provenance.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

use artmap::arena::{Arena, ArenaArtMap, ArenaVersionedArtMap};
use artmap::AsBytes;

fn n(full: usize, miri: usize) -> usize {
    if cfg!(miri) {
        miri
    } else {
        full
    }
}

/// A key or value that counts its drops.
#[derive(Clone)]
struct Counted {
    bytes: Vec<u8>,
    drops: Arc<AtomicUsize>,
}

impl Counted {
    fn new(bytes: &[u8], drops: &Arc<AtomicUsize>) -> Self {
        Self {
            bytes: bytes.to_vec(),
            drops: Arc::clone(drops),
        }
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

impl AsBytes for Counted {
    fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl std::borrow::Borrow<[u8]> for Counted {
    fn borrow(&self) -> &[u8] {
        &self.bytes
    }
}

impl std::fmt::Debug for Counted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.bytes.fmt(f)
    }
}

#[test]
fn values_are_aligned() {
    #[repr(align(64))]
    #[derive(Clone, Copy, PartialEq, Debug)]
    struct Big(u8);
    let m = ArenaArtMap::<Vec<u8>, u128>::with_capacity(1 << 20);
    let m64 = ArenaArtMap::<Vec<u8>, Big>::with_capacity(1 << 20);
    for i in 0..n(200, 20) as u8 {
        m.insert(vec![i, 1], i as u128);
        m64.insert(vec![i, 2], Big(i));
    }
    for e in m.iter() {
        assert_eq!(
            std::ptr::from_ref(e.value()).addr() % 16,
            0,
            "misaligned u128"
        );
    }
    for e in m64.iter() {
        assert_eq!(
            std::ptr::from_ref(e.value()).addr() % 64,
            0,
            "misaligned align(64)"
        );
    }
}

#[test]
fn zero_and_tiny_arenas_are_sound() {
    for cap in [0, 1, 64, 100] {
        let m = ArenaArtMap::<Vec<u8>, u64>::with_capacity(cap);
        let r = m.try_insert(vec![1, 2, 3], 7);
        let full = r.expect_err("nothing fits in a tiny arena");
        assert_eq!(full.key, vec![1, 2, 3]);
        assert_eq!(full.value, 7);
        assert!(m.is_empty());
        assert!(m.get(&[1u8, 2, 3][..]).is_none());
    }
    let a = Arena::new(0);
    assert!(a.capacity() >= 64);
}

#[test]
fn small_arena_fills_up_cleanly() {
    let m = ArenaArtMap::<[u8; 8], u64>::with_capacity(4096);
    let mut last = m.arena().remaining();
    let mut inserted = 0u64;
    loop {
        match m.try_insert(inserted.to_be_bytes(), inserted) {
            Ok(_) => inserted += 1,
            Err(full) => {
                assert_eq!(full.value, inserted);
                break;
            }
        }
        let now = m.arena().remaining();
        assert!(now <= last, "remaining() is monotonic");
        last = now;
    }
    assert!(inserted > 0, "a 4 KiB arena holds some entries");
    assert_eq!(m.len() as u64, inserted);
    for i in 0..inserted {
        assert_eq!(m.get(&i.to_be_bytes()), Some(i));
    }
    // Still usable for reads and replaces that need no allocation: none left.
    assert!(m.try_insert(0u64.to_be_bytes(), 1).is_err());
}

#[test]
fn full_arena_at_root_split_releases_every_latch() {
    // One leaf at the root; the second insert needs a Node4 and a leaf.
    for cap in 128..512 {
        let m = Arc::new(ArenaVersionedArtMap::<Vec<u8>, u64>::with_capacity(cap));
        let _ = m.try_insert(vec![1], 1, 1);
        let _ = m.try_insert(vec![2], 1, 2);
        // Another thread must still be able to take every latch.
        let m2 = Arc::clone(&m);
        std::thread::spawn(move || {
            let _ = m2.try_insert(vec![1], 2, 3);
            let _ = m2.try_insert(vec![3], 1, 4);
        })
        .join()
        .unwrap();
        let _ = m.get(&vec![1]);
    }
}

#[test]
fn single_insert_stays_within_max_insert_bytes() {
    let m = ArenaArtMap::<Vec<u8>, u64>::with_capacity(64 << 20);
    let mut rng = 1u64;
    let mut next = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    for i in 0..n(4000, 200) {
        let len = (next() % 90) as usize;
        let mut k = vec![b'p'; len];
        // Force splits and growth: vary the tail and the shared prefix length.
        if len > 0 {
            k[len - 1] = next() as u8;
        }
        if len > 3 {
            k[(next() as usize) % len] = next() as u8;
        }
        let before = m.arena().size();
        m.insert(k.clone(), i as u64);
        let used = m.arena().size() - before;
        assert!(
            used <= ArenaArtMap::<Vec<u8>, u64>::max_insert_bytes(k.len()),
            "insert of a {}-byte key used {used} bytes",
            k.len()
        );
    }
    let vm = ArenaVersionedArtMap::<Vec<u8>, u64>::with_capacity(64 << 20);
    for i in 0..n(2000, 100) {
        let k = format!("key:{:05}:{}", i % 97, "x".repeat(i % 40)).into_bytes();
        let before = vm.arena().size();
        vm.insert(k.clone(), i as u64, i as u64);
        let used = vm.arena().size() - before;
        assert!(used <= ArenaVersionedArtMap::<Vec<u8>, u64>::max_insert_bytes(k.len()));
    }
}

#[test]
fn inserters_of_two_maps_sharing_an_arena() {
    // Previously an inserter was a bare offset usable on any map: type
    // confusion. Each inserter now borrows its own map.
    let arena = Arena::with_capacity(8 << 20);
    let strings = ArenaArtMap::<String, String>::new(Arc::clone(&arena));
    let numbers = ArenaArtMap::<Vec<u8>, [u64; 3]>::new(arena);
    let mut a = strings.inserter();
    let mut b = numbers.inserter();
    for i in 0..n(2000, 100) {
        a.insert(format!("s:{i:05}"), format!("v{i}"));
        b.insert(format!("s:{i:05}").into_bytes(), [i as u64; 3]);
    }
    for i in 0..n(2000, 100) {
        assert_eq!(strings.get(&format!("s:{i:05}")), Some(format!("v{i}")));
        assert_eq!(
            numbers.get(format!("s:{i:05}").as_bytes()),
            Some([i as u64; 3])
        );
    }
}

#[test]
fn insert_after_remove_is_a_plain_insert() {
    let m = ArenaArtMap::<Vec<u8>, u64>::with_capacity(1 << 20);
    assert!(m.insert(b"k".to_vec(), 1).is_none());
    assert_eq!(m.remove(&b"k"[..]).map(|e| *e), Some(1));
    assert!(
        m.remove(&b"k"[..]).is_none(),
        "a second remove finds nothing"
    );
    assert_eq!(m.len(), 0);
    assert!(
        m.insert(b"k".to_vec(), 2).is_none(),
        "no stale value after remove"
    );
    assert_eq!(m.len(), 1);
    assert_eq!(m.get(&b"k"[..]), Some(2));
}

#[test]
fn concurrent_remove_and_insert_of_one_key() {
    let m = Arc::new(ArenaArtMap::<Vec<u8>, u64>::with_capacity(64 << 20));
    let threads = n(4, 2);
    let rounds = n(4000, 20);
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let barrier = Arc::new(Barrier::new(threads));
    let hs: Vec<_> = (0..threads)
        .map(|t| {
            let (m, seen, barrier) = (Arc::clone(&m), Arc::clone(&seen), Arc::clone(&barrier));
            std::thread::spawn(move || {
                barrier.wait();
                let mut mine = Vec::new();
                for r in 0..rounds as u64 {
                    let v = (t as u64) << 32 | r;
                    if let Some(old) = m.insert(b"hot".to_vec(), v) {
                        mine.push(*old);
                    }
                    if let Some(old) = m.remove(&b"hot"[..]) {
                        mine.push(*old);
                    }
                }
                seen.lock().unwrap().extend(mine);
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
    let mut seen = Arc::try_unwrap(seen).unwrap().into_inner().unwrap();
    let before = seen.len();
    seen.sort_unstable();
    seen.dedup();
    assert_eq!(seen.len(), before, "a value was displaced or removed twice");
    assert_eq!(m.len(), m.iter().count());
}

#[test]
fn every_key_and_value_is_dropped_exactly_once() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut created = 0usize;
    {
        let m = ArenaArtMap::<Counted, Counted>::with_capacity(1 << 20);
        for i in 0..n(300, 30) {
            let k = format!("k{:03}", i % 50);
            // Replaces retire the displaced entry (a retired leaf).
            let _ = m.insert(
                Counted::new(k.as_bytes(), &drops),
                Counted::new(b"v", &drops),
            );
            created += 2;
            if i % 7 == 0 {
                let _ = m.remove(k.as_bytes());
            }
        }
        // Inserts that do not fit give their key and value back.
        let tiny = ArenaArtMap::<Counted, Counted>::with_capacity(64);
        let full = tiny
            .try_insert(Counted::new(b"x", &drops), Counted::new(b"y", &drops))
            .unwrap_err();
        created += 2;
        drop(full);
    }
    assert_eq!(
        drops.load(Ordering::Relaxed),
        created,
        "leaked or double-dropped"
    );
}

#[test]
fn versioned_every_key_and_value_is_dropped_exactly_once() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut created = 0usize;
    {
        let m = ArenaVersionedArtMap::<Counted, Counted>::with_capacity(1 << 20);
        for i in 0..n(300, 30) {
            let k = format!("k{:03}", i % 20);
            // Same-version replaces (every third) retire a version node;
            // existing-key inserts discard the new key.
            let version = (i / 3) as u64;
            m.insert(
                Counted::new(k.as_bytes(), &drops),
                version,
                Counted::new(b"v", &drops),
            );
            created += 2;
            if i % 11 == 0 {
                m.delete(Counted::new(k.as_bytes(), &drops), version + 1);
                created += 1;
            }
        }
        let tiny = ArenaVersionedArtMap::<Counted, Counted>::with_capacity(64);
        let full = tiny
            .try_insert(Counted::new(b"x", &drops), 1, Counted::new(b"y", &drops))
            .unwrap_err();
        created += 2;
        drop(full);
    }
    assert_eq!(
        drops.load(Ordering::Relaxed),
        created,
        "leaked or double-dropped"
    );
}

#[test]
fn versioned_inserter_places_every_key() {
    let shapes: Vec<Vec<String>> = vec![
        (1..=4).map(|i| format!("aaa{i}")).collect(),
        (0..n(1000, 80)).map(|i| format!("seq:{i:05}")).collect(),
        vec![
            "aaa1".into(),
            "zzz9".into(),
            "bq".into(),
            "aaa2".into(),
            "b".into(),
            "".into(),
        ],
        (0..n(300, 40)).map(|i| "k".repeat(i % 20)).collect(),
        (0..=255u8)
            .map(|b| String::from_utf8_lossy(&[b'n', b]).into_owned())
            .collect(),
    ];
    for keys in shapes {
        let m = ArenaVersionedArtMap::<Vec<u8>, u64>::with_capacity(16 << 20);
        let mut ins = m.inserter();
        let mut model = BTreeMap::new();
        for (i, k) in keys.iter().enumerate() {
            ins.insert(k.clone().into_bytes(), 1, i as u64);
            model.insert(k.clone().into_bytes(), i as u64);
        }
        for (k, v) in &model {
            assert_eq!(m.get_latest(k), Some((1, *v)), "key {k:?} misplaced");
        }
        assert_eq!(m.len(), model.len());
        let got: Vec<_> = m.iter().map(|e| e.key().clone()).collect();
        assert!(got.windows(2).all(|w| w[0] < w[1]), "iteration is sorted");
        assert_eq!(got.len(), model.len());
    }
}

#[test]
fn references_stay_valid_under_concurrent_writes() {
    let m = Arc::new(ArenaVersionedArtMap::<Vec<u8>, Vec<u8>>::with_capacity(
        64 << 20,
    ));
    for i in 0..64u8 {
        m.insert(vec![i], 1, vec![i; 32]);
    }
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (m, stop) = (Arc::clone(&m), Arc::clone(&stop));
        std::thread::spawn(move || {
            let mut v = 2u64;
            while !stop.load(Ordering::Relaxed) && v < n(3000, 30) as u64 {
                for i in 0..64u8 {
                    m.insert(vec![i], v, vec![i ^ 0xFF; 32]);
                    // Same-version replace of version 1.
                    m.insert(vec![i], 1, vec![i; 32]);
                }
                v += 1;
            }
        })
    };
    // Hold references from a range scan and from `versions()`.
    let held: Vec<(&Vec<u8>, &Vec<u8>)> = m
        .range::<_, [u8]>(..)
        .map(|e| (e.key(), e.value()))
        .collect();
    let versions: Vec<_> = m
        .range::<_, [u8]>(..)
        .flat_map(|e| e.versions().filter_map(|v| v.value).collect::<Vec<_>>())
        .collect();
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    for (k, v) in held {
        assert_eq!(v.len(), 32);
        assert!(v.iter().all(|b| *b == k[0] || *b == k[0] ^ 0xFF));
    }
    for v in versions {
        assert_eq!(v.len(), 32);
    }
}

#[test]
fn iteration_is_monotonic_when_siblings_are_inserted() {
    let m = ArenaArtMap::<Vec<u8>, u64>::with_capacity(1 << 20);
    for b in *b"bdf" {
        m.insert(vec![b'x', b], 0);
    }
    let mut got = Vec::new();
    for e in m.iter() {
        got.push(e.key().clone());
        // A smaller sibling in the same Node4, inserted mid-scan.
        m.insert(vec![b'x', b'a'], 1);
        m.insert(vec![b'x', b'c'], 1);
    }
    assert!(
        got.windows(2).all(|w| w[0] < w[1]),
        "no duplicate or out-of-order key: {got:?}"
    );
    for k in [b"xb", b"xd", b"xf"] {
        assert!(
            got.contains(&k.to_vec()),
            "a key present throughout was skipped"
        );
    }
}

#[test]
fn concurrent_scans_are_monotonic() {
    let m = Arc::new(ArenaArtMap::<[u8; 8], u64>::with_capacity(64 << 20));
    for i in (0..n(2000, 40) as u64).step_by(2) {
        m.insert(i.to_be_bytes(), i);
    }
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (m, stop) = (Arc::clone(&m), Arc::clone(&stop));
        std::thread::spawn(move || {
            let mut i = 1u64;
            while !stop.load(Ordering::Relaxed) && i < n(2000, 40) as u64 {
                m.insert(i.to_be_bytes(), i);
                i += 2;
            }
        })
    };
    for _ in 0..n(20, 2) {
        let keys: Vec<u64> = m.iter().map(|e| u64::from_be_bytes(*e.key())).collect();
        assert!(keys.windows(2).all(|w| w[0] < w[1]));
        let evens = keys.iter().filter(|k| *k % 2 == 0).count();
        assert_eq!(
            evens,
            n(2000, 40) / 2,
            "every key present throughout is yielded"
        );
    }
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
}

/// Runs `f` on another thread and fails if it does not finish in time: a
/// latch left locked by an `ArenaFull` path would hang it.
fn within_timeout(f: impl FnOnce() + Send + 'static) {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        f();
        let _ = tx.send(());
    });
    rx.recv_timeout(std::time::Duration::from_secs(if cfg!(miri) {
        600
    } else {
        20
    }))
    .expect("an ArenaFull path left a latch locked");
}

#[test]
fn arena_full_at_every_allocation_site_releases_every_latch() {
    // Sweep the capacity so that the arena runs out at each allocation site
    // in turn: leaves, prefix-split Node4s, prefix chains, and every grow.
    let keys: Vec<Vec<u8>> = (0..40u8)
        .flat_map(|i| {
            [
                vec![b'a', i],                                   // grows a to Node16/48
                vec![b'p'; 20].into_iter().chain([i]).collect(), // prefix chains
                vec![b'z', i, i, i],                             // prefix splits
            ]
        })
        .collect();
    let step = if cfg!(miri) { 997 } else { 13 };
    for cap in (256..16_384).step_by(step) {
        let m = Arc::new(ArenaArtMap::<Vec<u8>, u64>::with_capacity(cap));
        let vm = Arc::new(ArenaVersionedArtMap::<Vec<u8>, u64>::with_capacity(cap));
        let mut stored = Vec::new();
        for (i, k) in keys.iter().enumerate() {
            if m.try_insert(k.clone(), i as u64).is_ok() {
                stored.push((k.clone(), i as u64));
            }
            let _ = vm.try_insert(k.clone(), 1, i as u64);
        }
        let (m2, vm2) = (Arc::clone(&m), Arc::clone(&vm));
        within_timeout(move || {
            // Every latch is free: reads, removes and further inserts proceed.
            let _ = m2.try_insert(b"after".to_vec(), 0);
            let _ = vm2.try_insert(b"after".to_vec(), 1, 0);
            let _ = m2.remove(&b"after"[..]);
            let _ = vm2.get(&b"after"[..]);
        });
        for (k, v) in &stored {
            assert_eq!(m.get(k), Some(*v), "cap {cap}: a committed insert was lost");
        }
        assert_eq!(m.len(), m.iter().count(), "cap {cap}");
        assert_eq!(vm.len(), vm.iter().count(), "cap {cap}");
    }
}

#[test]
fn contended_inserts_stay_within_max_insert_bytes() {
    // Threads race to insert keys under the same parents, so upgrades fail
    // and inserts retry. Retries reuse their prepared allocations, so the
    // bytes consumed stay within the per-insert bound (plus one TLAB chunk
    // of slack per thread).
    const THREADS: usize = 4;
    const TLAB_SLACK: usize = 16 << 10;
    let per = n(4000, 60);
    let m = Arc::new(ArenaArtMap::<Vec<u8>, u64>::with_capacity(64 << 20));
    let vm = Arc::new(ArenaVersionedArtMap::<Vec<u8>, u64>::with_capacity(
        64 << 20,
    ));
    let barrier = Arc::new(Barrier::new(THREADS));
    let key = |t: usize, i: usize| format!("shared:{:03}:{t}", i % 300).into_bytes();
    let key_len = key(0, 0).len();
    let (before, vbefore) = (m.arena().size(), vm.arena().size());
    let hs: Vec<_> = (0..THREADS)
        .map(|t| {
            let (m, vm, barrier) = (Arc::clone(&m), Arc::clone(&vm), Arc::clone(&barrier));
            std::thread::spawn(move || {
                barrier.wait();
                for i in 0..per {
                    m.insert(key(t, i), i as u64);
                    vm.insert(key(t, i), i as u64, i as u64);
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
    let inserts = THREADS * per;
    let used = m.arena().size() - before;
    let bound =
        inserts * ArenaArtMap::<Vec<u8>, u64>::max_insert_bytes(key_len) + THREADS * TLAB_SLACK;
    assert!(
        used <= bound,
        "{used} bytes for {inserts} inserts (bound {bound})"
    );
    let vused = vm.arena().size() - vbefore;
    let vbound = inserts * ArenaVersionedArtMap::<Vec<u8>, u64>::max_insert_bytes(key_len)
        + THREADS * TLAB_SLACK;
    assert!(
        vused <= vbound,
        "{vused} bytes for {inserts} inserts (bound {vbound})"
    );
}
