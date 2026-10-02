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

//! Regression tests for the heap maps, one per audit finding (§17.0). Each
//! reproduced undefined behaviour, a wrong result or a hang before the fix.

use std::any::Any;
use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};

use artmap::{ArtMap, AsBytes, VersionedArtMap};

fn n(full: usize, miri: usize) -> usize {
    if cfg!(miri) {
        miri
    } else {
        full
    }
}

/// Drives epoch advancement so that deferred destructors run.
fn flush_epochs() {
    let m = ArtMap::<Vec<u8>, u8>::new();
    for _ in 0..n(4096, 256) {
        m.pin().repin();
    }
}

#[test]
fn values_need_not_be_clone() {
    // [public-api-9] No `V: Clone` bound on the core API.
    let m = ArtMap::<String, Box<dyn Any + Send>>::new();
    assert!(m.insert("a".into(), Box::new(1u32)).is_none());
    let old = m.insert("a".into(), Box::new("two")).expect("replaced");
    assert_eq!(old.downcast_ref::<u32>(), Some(&1));
    let removed = m.remove("a").expect("removed");
    assert_eq!(removed.downcast_ref::<&str>(), Some(&"two"));
}

#[test]
fn handle_semantics_under_replace() {
    // [public-api-11]
    let m = ArtMap::<String, String>::new();
    m.insert("k".into(), "v1".into());
    let e = m.get("k").unwrap();
    m.insert("k".into(), "v2".into());
    assert!(e.is_removed());
    assert!(
        !e.remove(),
        "a replaced entry cannot be removed through its handle"
    );
    assert_eq!(*e, "v1");
    assert_eq!(*m.get("k").unwrap(), "v2");
    assert_eq!(m.len(), 1);
}

#[test]
fn keys_survive_a_remove_and_flush() {
    // [olc-memory-model-6, public-api-5] Keys yielded bare `&'a K` whose
    // guard dropped with the iterator: a UAF after remove + flush.
    let m = ArtMap::<String, String>::new();
    for i in 0..20 {
        m.insert(format!("key-{i:02}"), format!("value-{i}"));
    }
    let keys: Vec<_> = m.keys().collect();
    let values: Vec<_> = m.values().collect();
    for i in 0..20 {
        m.remove(&format!("key-{i:02}"));
    }
    flush_epochs();
    for (i, (k, v)) in keys.iter().zip(&values).enumerate() {
        assert_eq!(**k, format!("key-{i:02}"));
        assert_eq!(**v, format!("value-{i}"));
    }
}

#[test]
fn removing_during_iteration_skips_nothing() {
    // [olc-memory-model-4] Node4/16 frames stored array indices.
    let m = ArtMap::<String, u32>::new();
    for k in ["a", "b", "c", "d"] {
        m.insert(k.into(), 0);
    }
    let mut it = m.iter();
    assert_eq!(it.next().unwrap().key(), "a");
    m.remove("a");
    assert_eq!(
        it.next().unwrap().key(),
        "b",
        "removing 'a' made the scan skip 'b'"
    );
    drop(it);

    let seen: Vec<String> = m
        .iter()
        .map(|e| {
            assert!(e.remove());
            e.key().clone()
        })
        .collect();
    assert_eq!(seen, ["b", "c", "d"]);
    assert!(m.is_empty());
}

#[test]
fn an_emptied_inner_node_does_not_end_iteration() {
    // [core-write-5] push_and_descend_left returned None at an empty node.
    let m = ArtMap::<String, u32>::new();
    for k in ["aa", "ab", "b"] {
        m.insert(k.into(), 0);
    }
    m.remove("aa");
    m.remove("ab");
    let keys: Vec<_> = m.iter().map(|e| e.key().clone()).collect();
    assert_eq!(keys, ["b"]);
    let keys: Vec<_> = m.iter().rev().map(|e| e.key().clone()).collect();
    assert_eq!(keys, ["b"]);
}

#[test]
fn unbounded_reverse_sees_long_ff_keys_and_zero_start() {
    // [public-api-8, robustness-11, olc-memory-model-10]
    let m = ArtMap::<Vec<u8>, u32>::new();
    let long = vec![0xFF; 65];
    let mut ff00 = vec![0xFF; 64];
    ff00.push(0);
    for k in [vec![0u8], vec![0, 1], long.clone(), ff00.clone(), vec![5]] {
        m.insert(k, 0);
    }
    let rev: Vec<_> = m.iter().rev().map(|e| e.key().clone()).collect();
    assert_eq!(rev[0], long);
    assert_eq!(rev[1], ff00);
    // `range([0]..)` used to yield nothing.
    let from_zero = m
        .range::<_, [u8]>((
            std::ops::Bound::Included(&[0u8][..]),
            std::ops::Bound::Unbounded,
        ))
        .count();
    assert_eq!(from_zero, 5);
}

#[test]
fn deep_keys_do_not_overflow_the_stack() {
    // [robustness-13, core-write-12] Recursion proportional to key length
    // aborted around 250 KB of shared prefix on a 2 MiB thread.
    let prefix = if cfg!(miri) { 2_000 } else { 1 << 20 };
    std::thread::Builder::new()
        .stack_size(2 << 20)
        .spawn(move || {
            let mut m = ArtMap::<Vec<u8>, u32>::new();
            let mut a = vec![7u8; prefix];
            a.push(1);
            let mut b = vec![7u8; prefix];
            b.push(2);
            m.insert(a.clone(), 1);
            m.insert(b.clone(), 2);
            // A chain deepened step by step.
            for j in 1..n(20, 4) {
                let mut k = vec![7u8; j * prefix / 20];
                k.push(1);
                m.insert(k, 3);
            }
            assert_eq!(m.get(&a[..]).as_deref(), Some(&1));
            m.validate_invariants();
            assert!(m.iter().count() >= 2);
            assert!(m.iter().rev().count() >= 2);
            assert!(m
                .range::<_, [u8]>((
                    std::ops::Bound::Unbounded,
                    std::ops::Bound::Excluded(&b[..])
                ))
                .next_back()
                .is_some());
            m.clear();
            assert!(m.is_empty());
            m.insert(a, 1);
            drop(m);
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn deep_chains_are_unlinked_with_their_last_key() {
    // [core-write-5] Two keys sharing 1 MiB build a chain of ~60k Node4s.
    // Removing the last key under it unlinks the whole chain, iteratively
    // (Inv 13) and in one pass up it: a re-descent per link would take
    // minutes. `shrink_to_fit` collapses it the same way while a key remains.
    let prefix = if cfg!(miri) { 2_000 } else { 1 << 20 };
    std::thread::Builder::new()
        .stack_size(2 << 20)
        .spawn(move || {
            let mut m = ArtMap::<Vec<u8>, u32>::new();
            let key = |last: u8| {
                let mut k = vec![7u8; prefix];
                k.push(last);
                k
            };
            let mid = vec![7u8; prefix / 2];
            m.insert(key(1), 1);
            m.insert(key(2), 2);
            m.insert(mid.clone(), 3);
            assert_eq!(m.remove(&key(1)[..]).as_deref(), Some(&1));
            assert_eq!(m.remove(&mid[..]).as_deref(), Some(&3));
            m.validate_invariants();
            let start = std::time::Instant::now();
            assert_eq!(m.remove(&key(2)[..]).as_deref(), Some(&2));
            if !cfg!(miri) {
                assert!(
                    start.elapsed() < std::time::Duration::from_secs(10),
                    "unlinking a chain took {:?}",
                    start.elapsed()
                );
            }
            assert!(m.is_empty());
            assert!(m.iter().next().is_none());
            m.validate_invariants();
            // With one key left, fitting collapses the chain into it.
            m.insert(key(1), 1);
            m.insert(key(2), 2);
            assert_eq!(m.remove(&key(1)[..]).as_deref(), Some(&1));
            let start = std::time::Instant::now();
            m.shrink_to_fit();
            if !cfg!(miri) {
                assert!(
                    start.elapsed() < std::time::Duration::from_secs(10),
                    "fitting a chain took {:?}",
                    start.elapsed()
                );
            }
            assert_eq!(m.get(&key(2)[..]).as_deref(), Some(&2));
            m.validate_invariants();
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn a_registry_emptied_by_removes_is_compact() {
    // [core-write-5] Removes left every emptied inner node in the tree.
    let mut m = ArtMap::<Vec<u8>, u64>::new();
    let keys: Vec<Vec<u8>> = (0..n(5_000, 200) as u64)
        .map(|i| {
            let h = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            format!("registry:{}:{h:x}", h % 13).into_bytes()
        })
        .collect();
    for (i, k) in keys.iter().enumerate() {
        m.insert(k.clone(), i as u64);
    }
    // Remove every other key, then the rest in reverse.
    for k in keys.iter().step_by(2) {
        assert!(m.remove(&k[..]).is_some());
    }
    m.validate_invariants();
    m.shrink_to_fit();
    m.validate_invariants();
    for k in keys.iter().skip(1).step_by(2).rev() {
        assert!(m.remove(&k[..]).is_some());
    }
    assert!(m.is_empty());
    assert!(m.iter().next().is_none());
    m.validate_invariants();
    for k in &keys {
        m.insert(k.clone(), 0);
    }
    assert_eq!(m.len(), keys.len());
    m.validate_invariants();
}

#[test]
fn inserting_existing_keys_frees_the_discarded_leaf_safely() {
    // [core-write-7, robustness-9] The new leaf was freed while key bytes
    // derived from it were still a protected argument (Miri UAF).
    let m = ArtMap::<Vec<u8>, u32>::new();
    m.insert(b"vec-key".to_vec(), 1);
    m.insert(b"vec-key".to_vec(), 2);
    let a = ArtMap::<[u8; 8], u32>::new();
    a.insert(*b"arraykey", 1);
    a.insert(*b"arraykey", 2);
    assert_eq!(*a.get(b"arraykey").unwrap(), 2);
    let e = m.get_or_insert_with(b"vec-key".to_vec(), || 9);
    assert_eq!(*e, 2);
}

#[test]
fn inserting_existing_set_keys_frees_the_discarded_leaf_safely() {
    // As above, through `ArtSet::insert`, which looks the key up first and then
    // installs it with insert-if-absent.
    let s = artmap::ArtSet::<Vec<u8>>::new();
    assert!(s.insert(b"vec-key".to_vec()));
    assert!(!s.insert(b"vec-key".to_vec()));
    let a = artmap::ArtSet::<[u8; 8]>::new();
    assert!(a.insert(*b"arraykey"));
    assert!(!a.insert(*b"arraykey"));
    assert_eq!(a.len(), 1);
}

thread_local! {
    static CALLS: Cell<usize> = const { Cell::new(0) };
}

/// Returns different bytes on its first call (Inv 10).
struct Liar(Vec<u8>);

impl AsBytes for Liar {
    fn as_bytes(&self) -> &[u8] {
        let n = CALLS.with(|c| {
            let n = c.get();
            c.set(n + 1);
            n
        });
        if n == 0 {
            b"something else entirely"
        } else {
            &self.0
        }
    }
}

#[test]
fn a_lying_as_bytes_causes_no_ub() {
    // [core-write-7, v2:consistency#29] Wrong results are allowed; UB is not.
    let m = ArtMap::<Liar, u32>::new();
    m.insert(Liar(b"k".to_vec()), 1);
    CALLS.with(|c| c.set(0));
    let _ = m.get_or_insert_with(Liar(b"k".to_vec()), || 2);
    let _ = m.iter().count();
}

#[test]
fn a_lying_as_bytes_causes_no_ub_in_an_artset() {
    // `ArtSet::insert` calls `as_bytes` for the lookup and again for the
    // install; the install derives the bytes once, from the leaf (Inv 10).
    let s = artmap::ArtSet::<Liar>::new();
    s.insert(Liar(b"k".to_vec()));
    CALLS.with(|c| c.set(0));
    let _ = s.insert(Liar(b"k".to_vec()));
    let _ = s.iter().count();
}

#[test]
fn two_threads_insert_the_same_new_versioned_key() {
    // [robustness-9] The losing VersionedLeaf was freed under a protector.
    for _ in 0..n(200, 4) {
        let m = Arc::new(VersionedArtMap::<Vec<u8>, u64>::new());
        let b = Arc::new(Barrier::new(2));
        let hs: Vec<_> = (0..2)
            .map(|t| {
                let (m, b) = (Arc::clone(&m), Arc::clone(&b));
                std::thread::spawn(move || {
                    b.wait();
                    m.insert(b"shared-new-key".to_vec(), t + 1, t);
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        assert_eq!(m.version_count(&b"shared-new-key"[..]), 2);
        assert_eq!(m.len(), 1);
    }
}

#[test]
fn versioned_delete_keeps_older_snapshots() {
    // [gap-heap-versioned-parity-4] remove() erased history retroactively.
    let m = VersionedArtMap::<Vec<u8>, u64>::new();
    m.insert(b"k".to_vec(), 5, 50);
    assert_eq!(m.get_version_le(&b"k"[..], 7), Some((5, 50)));
    assert!(m.delete(b"k".to_vec(), 8));
    assert_eq!(
        m.get_version_le(&b"k"[..], 7),
        Some((5, 50)),
        "the snapshot at 7 is unchanged"
    );
    assert_eq!(m.get_version_le(&b"k"[..], 9), None);
    assert_eq!(m.get(&b"k"[..]), None);
    assert_eq!(m.len(), 0);
    assert_eq!(
        m.get_all_versions(&b"k"[..]),
        vec![(8, None), (5, Some(50))]
    );
    // Sequential len accounting across remove / reinsert / remove.
    m.insert(b"k".to_vec(), 9, 90);
    assert_eq!(m.len(), 1);
    #[allow(deprecated)]
    let removed = m.remove(&b"k"[..]).map(|e| *e);
    assert_eq!(removed, Some(90));
    assert_eq!(m.len(), 0);
    #[allow(deprecated)]
    let again = m.remove(&b"k"[..]);
    assert!(again.is_none(), "a second remove finds nothing");
    assert_eq!(m.len(), 0, "len never underflows");
}

#[test]
fn versioned_entries_are_snapshots() {
    // [gap-heap-versioned-parity-6, versioned-5] Entries re-loaded the head
    // on every accessor, so value and version could mismatch.
    let m = Arc::new(VersionedArtMap::<Vec<u8>, u64>::new());
    for i in 0..16u8 {
        m.insert(vec![i], 1, 10);
    }
    let next = Arc::new(AtomicU64::new(2));
    let (m2, nx) = (Arc::clone(&m), Arc::clone(&next));
    let writer = std::thread::spawn(move || {
        for _ in 0..n(20_000, 50) {
            let v = nx.fetch_add(1, Ordering::Relaxed);
            for i in 0..16u8 {
                m2.insert(vec![i], v, v * 10);
            }
        }
    });
    for _ in 0..n(200, 5) {
        for e in m.iter() {
            assert_eq!(*e.value(), e.version() * 10, "value and version mismatch");
        }
    }
    writer.join().unwrap();
}

#[test]
fn concurrent_prunes_with_different_watermarks() {
    // [v2:coverage-gaps#4] Two overlapping prunes double-retired nodes once
    // the removed flag stopped guarding retirement.
    for _ in 0..n(50, 2) {
        let m = Arc::new(VersionedArtMap::<Vec<u8>, u64>::new());
        for v in 1..=n(200, 10) as u64 {
            m.insert(b"k".to_vec(), v, v);
        }
        let b = Arc::new(Barrier::new(2));
        let hs: Vec<_> = [n(50, 3) as u64, n(150, 7) as u64]
            .into_iter()
            .map(|min| {
                let (m, b) = (Arc::clone(&m), Arc::clone(&b));
                std::thread::spawn(move || {
                    b.wait();
                    m.prune_key(&b"k"[..], min, |_| false)
                })
            })
            .collect();
        let pruned: usize = hs.into_iter().map(|h| h.join().unwrap()).sum();
        let left = m.version_count(&b"k"[..]);
        assert_eq!(
            pruned + left,
            n(200, 10),
            "every version counted exactly once"
        );
    }
}

#[test]
fn prune_all_includes_deleted_keys() {
    // [gap-heap-versioned-parity-5] prune_all iterated with Range, which
    // skips deleted keys, so their chains were never pruned.
    let mut m = VersionedArtMap::<Vec<u8>, u64>::new();
    for v in 1..=5 {
        m.insert(b"gone".to_vec(), v, v);
    }
    m.delete(b"gone".to_vec(), 6);
    // No snapshot at or above 6 sees the key: it goes, with all six versions.
    assert_eq!(m.prune_all(6, |_| false), 6);
    assert_eq!(m.version_count(&b"gone"[..]), 0);
    m.validate_invariants();
}

#[test]
fn cloning_a_handle_in_a_thread_local_destructor_never_uafs() {
    // [v2:api-lifetimes#0] During TLS teardown a "nested" pin may land on a
    // fresh Local that does not protect the entry. Cloning must panic or
    // share the guard, never extend protection unsoundly.
    static MAP: ArtMap<Vec<u8>, String> = ArtMap::new();
    struct CloneOnDrop;
    impl Drop for CloneOnDrop {
        fn drop(&mut self) {
            let r = std::panic::catch_unwind(|| {
                if let Some(e) = MAP.get(&b"k"[..]) {
                    let c = e.clone();
                    assert_eq!(*c, "value");
                }
                let n = MAP.iter().count();
                assert!(n <= 1);
            });
            // Either outcome is sound; what matters is no UAF (ASan/Miri).
            let _ = r;
        }
    }
    thread_local! {
        static GUARD: CloneOnDrop = const { CloneOnDrop };
    }
    MAP.insert(b"k".to_vec(), "value".into());
    let remover = std::thread::spawn(|| {
        for _ in 0..n(200, 10) {
            MAP.insert(b"k".to_vec(), "value".into());
            let _ = MAP.remove(&b"k"[..]);
        }
    });
    std::thread::spawn(|| GUARD.with(|_| ())).join().unwrap();
    remover.join().unwrap();
}

#[test]
fn pruning_a_user_tombstone_in_the_only_inline_slot() {
    // The key's only version lives in slot0 and is a user tombstone: prune
    // unlinks the whole leaf, and the leaf's own drop releases the slot
    // (§11.4, §13).
    let map = VersionedArtMap::<Vec<u8>, u64>::new();
    map.insert(b"k".to_vec(), 1, 0);
    assert_eq!(map.len(), 1);
    assert_eq!(map.prune_key(&b"k"[..], u64::MAX, |v| *v == 0), 1);
    assert_eq!(map.len(), 0);
    assert!(map.get(&b"k"[..]).is_none());
    assert!(map.get_all_versions(&b"k"[..]).is_empty());
    // The key comes back with a newer version.
    map.insert(b"k".to_vec(), 2, 5);
    assert_eq!(map.get(&b"k"[..]), Some(5));
    assert_eq!(map.len(), 1);
    drop(map);
    flush_epochs();
}
