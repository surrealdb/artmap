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

//! Exact drop accounting for the heap maps: every key and value is dropped
//! exactly once. These tests wait for deferred destructors, which cannot run
//! while any thread in the process holds an epoch pin, so they live in their
//! own test binary, away from tests that hold guards for long.

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use artmap::{ArtMap, AsBytes, VersionedArtMap};

fn n(full: usize, miri: usize) -> usize {
    if cfg!(miri) {
        miri
    } else {
        full
    }
}

/// Waits (bounded) until `drops` reaches `want`, advancing the epoch. Garbage
/// sits in a thread's local bag until the bag fills or the thread exits, so
/// callers retire from a worker thread.
fn await_drops(drops: &AtomicUsize, want: usize) -> usize {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while drops.load(Ordering::Relaxed) < want && std::time::Instant::now() < deadline {
        crossbeam_epoch::pin().flush();
    }
    drops.load(Ordering::Relaxed)
}

/// A value that counts its drops.
struct Counted(Arc<AtomicUsize>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn prune_retires_every_detached_version_exactly_once() {
    // [gap-heap-versioned-parity-3, versioned-4] Nodes flagged by remove()
    // leaked once detached, and prune over-counted.
    let drops = Arc::new(AtomicUsize::new(0));
    let d = Arc::clone(&drops);
    std::thread::spawn(move || {
        let m = VersionedArtMap::<Vec<u8>, Counted>::new();
        m.insert(b"k".to_vec(), 1, Counted(Arc::clone(&d)));
        #[allow(deprecated)]
        let _ = m.remove(&b"k"[..]);
        for v in 2..10 {
            m.insert(b"k".to_vec(), v, Counted(Arc::clone(&d)));
        }
        let pruned = m.prune_key(&b"k"[..], 9, |_| false);
        assert_eq!(pruned, 8, "every older version is detached");
        assert_eq!(m.version_count(&b"k"[..]), 1);
        // A user tombstone at the head becomes a built-in tombstone.
        assert_eq!(m.prune_key(&b"k"[..], 9, |_| true), 1);
        assert_eq!(m.len(), 0);
    })
    .join()
    .unwrap();
    // Nine values were created; every one is dropped exactly once.
    assert_eq!(await_drops(&drops, 9), 9, "leaked or double-dropped");
}

#[test]
fn every_heap_key_and_value_is_dropped_exactly_once() {
    // Phase 3 exit: all heap ArtMap K/V are dropped after map drop plus flush.
    #[derive(Clone)]
    struct K(Vec<u8>, Arc<AtomicUsize>);
    impl Drop for K {
        fn drop(&mut self) {
            self.1.fetch_add(1, Ordering::Relaxed);
        }
    }
    impl AsBytes for K {
        fn as_bytes(&self) -> &[u8] {
            &self.0
        }
    }
    impl std::borrow::Borrow<[u8]> for K {
        fn borrow(&self) -> &[u8] {
            &self.0
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let d = Arc::clone(&drops);
    let created = std::thread::spawn(move || {
        let mut created = 0;
        let m = ArtMap::<K, Counted>::new();
        for i in 0..n(2000, 60) {
            let k = format!("k{:03}", i % 97).into_bytes();
            let _ = m.insert(K(k.clone(), Arc::clone(&d)), Counted(Arc::clone(&d)));
            created += 2;
            if i % 5 == 0 {
                let _ = m.remove(&k[..]);
            }
            if i % 7 == 0 {
                // An existing key discards the key passed in; an absent one
                // also builds a value.
                let built = Cell::new(false);
                let _ = m.get_or_insert_with(K(k, Arc::clone(&d)), || {
                    built.set(true);
                    Counted(Arc::clone(&d))
                });
                created += 1 + usize::from(built.get());
            }
        }
        created
    })
    .join()
    .unwrap();
    assert_eq!(
        await_drops(&drops, created),
        created,
        "leaked or double-dropped"
    );
}

#[test]
fn versioned_removes_prunes_and_clears_drop_everything_once() {
    // §13: an unlinked versioned leaf is retired whole, with every version
    // still in its chain; versions unlinked from a live chain are retired one
    // by one. Nothing leaks and nothing drops twice.
    #[derive(Clone)]
    struct K(Vec<u8>, Arc<AtomicUsize>);
    impl Drop for K {
        fn drop(&mut self) {
            self.1.fetch_add(1, Ordering::Relaxed);
        }
    }
    impl AsBytes for K {
        fn as_bytes(&self) -> &[u8] {
            &self.0
        }
    }
    impl std::borrow::Borrow<[u8]> for K {
        fn borrow(&self) -> &[u8] {
            &self.0
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let d = Arc::clone(&drops);
    let created = std::thread::spawn(move || {
        let mut created = 0;
        let m = VersionedArtMap::<K, Counted>::new();
        for i in 0..n(1500, 60) as u64 {
            let k = format!("k{:02}", i % 23).into_bytes();
            m.insert(K(k.clone(), Arc::clone(&d)), i, Counted(Arc::clone(&d)));
            created += 2;
            match i % 9 {
                0 => {
                    m.delete(K(k.clone(), Arc::clone(&d)), i + 1);
                    created += 1;
                }
                1 => {
                    m.remove_key(&k[..]);
                }
                2 => {
                    m.remove_version(&k[..], i);
                }
                3 => {
                    m.prune_key(&k[..], i, |_| false);
                }
                4 if i % 4 == 0 => {
                    m.prune_all(i.saturating_sub(5), |_| i % 8 == 0);
                }
                5 if i % 50 == 5 => m.clear(),
                _ => {}
            }
        }
        created
    })
    .join()
    .unwrap();
    assert_eq!(
        await_drops(&drops, created),
        created,
        "leaked or double-dropped"
    );
}
