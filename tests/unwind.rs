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

//! Fault injection (§16.5): panicking and re-entrant user code at every call
//! site. After each fault the map must stay usable (no latch left locked,
//! checked by a sibling insert under a watchdog), consistent
//! (`validate_invariants`, `len` equals the reachable count), and free of
//! double drops.

use std::cell::Cell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use artmap::{ArenaArtMap, ArenaVersionedArtMap, ArtMap, AsBytes, VersionedArtMap};

thread_local! {
    /// Panic on the n-th `as_bytes` call on this thread (0 = never).
    static PANIC_AT: Cell<usize> = const { Cell::new(0) };
}

fn arm(n: usize) {
    PANIC_AT.with(|p| p.set(n));
}

fn disarm() {
    PANIC_AT.with(|p| p.set(0));
}

/// A key whose `as_bytes` panics when armed.
#[derive(Clone, PartialEq, Eq, Debug)]
struct PanicKey(Vec<u8>);

impl AsBytes for PanicKey {
    fn as_bytes(&self) -> &[u8] {
        PANIC_AT.with(|p| {
            let n = p.get();
            if n == 1 {
                p.set(0);
                panic!("injected as_bytes panic");
            } else if n > 1 {
                p.set(n - 1);
            }
        });
        &self.0
    }
}

impl std::borrow::Borrow<[u8]> for PanicKey {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

/// Runs `f` on another thread and fails the test if it does not finish:
/// a latch left locked by an unwind would hang it.
fn within_timeout<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(Duration::from_secs(if cfg!(miri) { 600 } else { 20 }))
        .expect("operation hung: a latch was left locked")
}

fn key(s: &str) -> PanicKey {
    PanicKey(s.as_bytes().to_vec())
}

/// Tree shapes that route an insert through each writer path.
fn shapes() -> Vec<(&'static str, Vec<&'static str>, &'static str)> {
    vec![
        ("root leaf replace", vec!["a"], "a"),
        ("root leaf split", vec!["abc"], "abd"),
        ("prefix split", vec!["prefix:1", "prefix:2"], "pre"),
        ("exact leaf", vec!["ab1", "ab2"], "ab"),
        ("fast path", vec!["k1", "k2"], "k3"),
        ("leaf split", vec!["k1", "k2"], "k1x"),
        ("grow 4->16", vec!["g0", "g1", "g2", "g3"], "g4"),
        (
            "grow 16->48",
            vec![
                "g0", "g1", "g2", "g3", "g4", "g5", "g6", "g7", "g8", "g9", "ga", "gb", "gc", "gd",
                "ge", "gf",
            ],
            "gg",
        ),
        ("replace child", vec!["k1", "k2"], "k1"),
    ]
}

#[test]
fn panicking_as_bytes_on_every_artmap_path() {
    for (name, keys, probe) in shapes() {
        // Panic at each of the first few `as_bytes` calls of the insert.
        for nth in 1..=4 {
            let map = Arc::new(ArtMap::<PanicKey, u64>::new());
            for (i, k) in keys.iter().enumerate() {
                map.insert(key(k), i as u64);
            }
            arm(nth);
            let r = catch_unwind(AssertUnwindSafe(|| {
                map.insert(key(probe), 99);
            }));
            disarm();
            let panicked = r.is_err();
            let m = Arc::clone(&map);
            within_timeout(move || {
                // A sibling insert and a remove through the same nodes.
                let _ = m.insert(key(&format!("{probe}~")), 1);
                let _ = m.remove(&b"~never~"[..]);
            });
            let mut map = Arc::try_unwrap(map).ok().unwrap();
            map.validate_invariants();
            assert_eq!(map.len(), map.iter().count(), "{name}: len mismatch");
            if !panicked {
                assert!(map.get(probe.as_bytes()).is_some(), "{name}: insert lost");
            }
        }
    }
}

#[test]
fn panicking_as_bytes_in_remove_and_iteration() {
    let map = Arc::new(ArtMap::<PanicKey, u64>::new());
    for i in 0..40 {
        map.insert(key(&format!("r{i:02}")), i);
    }
    for nth in 1..=3 {
        arm(nth);
        let _ = catch_unwind(AssertUnwindSafe(|| map.remove(&b"r07"[..])));
        disarm();
        arm(nth);
        let _ = catch_unwind(AssertUnwindSafe(|| map.iter().count()));
        disarm();
        arm(nth);
        let _ = catch_unwind(AssertUnwindSafe(|| {
            map.range::<_, [u8]>((
                std::ops::Bound::Included(&b"r10"[..]),
                std::ops::Bound::Unbounded,
            ))
            .rev()
            .count()
        }));
        disarm();
    }
    let m = Arc::clone(&map);
    within_timeout(move || {
        m.insert(key("r99"), 1);
    });
    let mut map = Arc::try_unwrap(map).ok().unwrap();
    map.validate_invariants();
}

#[test]
fn panicking_closure_in_get_or_insert_with() {
    let map = ArtMap::<Vec<u8>, u64>::new();
    map.insert(b"a".to_vec(), 1);
    let r = catch_unwind(AssertUnwindSafe(|| {
        map.get_or_insert_with(b"b".to_vec(), || panic!("injected"));
    }));
    assert!(r.is_err());
    assert!(map.get(&b"b"[..]).is_none());
    assert_eq!(*map.get_or_insert_with(b"b".to_vec(), || 2), 2);
    assert_eq!(map.len(), 2);
}

/// A value whose `clone` panics.
struct PanicClone(u64);

impl Clone for PanicClone {
    fn clone(&self) -> Self {
        panic!("injected clone panic")
    }
}

#[test]
fn panicking_clone_runs_outside_latches() {
    let map = Arc::new(ArtMap::<Vec<u8>, PanicClone>::new());
    map.insert(b"k".to_vec(), PanicClone(1));
    let r = catch_unwind(AssertUnwindSafe(|| {
        map.insert_cloned(b"k".to_vec(), PanicClone(2))
    }));
    assert!(r.is_err());
    let m = Arc::clone(&map);
    within_timeout(move || m.insert(b"k2".to_vec(), PanicClone(3)).is_none());
    assert_eq!(
        map.get(&b"k"[..]).map(|e| e.0),
        Some(2),
        "the insert committed before the clone"
    );
    assert!(catch_unwind(AssertUnwindSafe(|| map.get_value(&b"k"[..]))).is_err());
}

static REENTRANT: ArtMap<Vec<u8>, ReValue> = ArtMap::new();
static REENTRANT_DROPS: AtomicUsize = AtomicUsize::new(0);

/// A value whose deferred `Drop` re-enters the map it was removed from.
struct ReValue(u64);

impl Drop for ReValue {
    fn drop(&mut self) {
        REENTRANT_DROPS.fetch_add(1, Ordering::Relaxed);
        // Reads and writes from inside a deferred destructor.
        let _ = REENTRANT.get(&b"stable"[..]);
        if self.0 % 3 == 0 {
            // Leak-free: a key that is never replaced again.
            let _ = REENTRANT
                .get_or_insert_with(format!("from-drop-{}", self.0).into_bytes(), || ReValue(1));
        }
    }
}

#[test]
fn reentrant_deferred_drop() {
    REENTRANT.insert(b"stable".to_vec(), ReValue(1));
    let n = if cfg!(miri) { 40 } else { 2000 };
    for i in 0..n {
        // Each overwrite retires the displaced value; enough pins run the
        // collector, which runs these destructors inside map operations.
        REENTRANT.insert(b"hot".to_vec(), ReValue(i));
        let _ = REENTRANT.get(&b"hot"[..]);
    }
    within_timeout(|| REENTRANT.insert(b"after".to_vec(), ReValue(2)).is_none());
    assert!(REENTRANT.get(&b"hot"[..]).is_some());
}

static REENTRANT_KEYS: ArtMap<ReKey, u64> = ArtMap::new();

/// A key whose `as_bytes` reads the map it is being inserted into.
#[derive(Clone)]
struct ReKey(Vec<u8>);

thread_local! {
    static IN_AS_BYTES: Cell<bool> = const { Cell::new(false) };
}

impl AsBytes for ReKey {
    fn as_bytes(&self) -> &[u8] {
        // Would deadlock if called while a latch of this map is held. One
        // level only: the lookup itself calls `as_bytes` on stored keys.
        if !IN_AS_BYTES.with(|f| f.replace(true)) {
            let _ = REENTRANT_KEYS.contains_key_slice(b"other");
            IN_AS_BYTES.with(|f| f.set(false));
        }
        &self.0
    }
}

#[test]
fn reentrant_as_bytes_never_deadlocks() {
    within_timeout(|| {
        for i in 0..if cfg!(miri) { 20 } else { 500 } {
            REENTRANT_KEYS.insert(ReKey(format!("k{:03}", i % 70).into_bytes()), i);
            REENTRANT_KEYS.remove_by_slice(format!("k{:03}", (i * 7) % 70).as_bytes());
        }
    });
}

#[test]
fn versioned_panicking_as_bytes_and_prune_closure() {
    let map = Arc::new(VersionedArtMap::<PanicKey, u64>::new());
    for i in 0..20u64 {
        map.insert(key(&format!("v{:02}", i % 5)), i, i);
    }
    for nth in 1..=3 {
        arm(nth);
        let _ = catch_unwind(AssertUnwindSafe(|| {
            map.insert(key("v01"), 100 + nth as u64, 0)
        }));
        disarm();
        arm(nth);
        let _ = catch_unwind(AssertUnwindSafe(|| map.insert(key("vnew"), 1, 0)));
        disarm();
    }
    // A panicking `is_tombstone` runs without any latch.
    let r = catch_unwind(AssertUnwindSafe(|| {
        map.prune_key(&b"v02"[..], u64::MAX, |_| {
            panic!("injected is_tombstone panic")
        })
    }));
    assert!(r.is_err());
    // A re-entrant `is_tombstone` that writes the same key.
    let m = Arc::clone(&map);
    within_timeout(move || {
        m.prune_key(&b"v03"[..], u64::MAX, |_| {
            m.insert(key("v03"), 1_000, 1);
            false
        });
        m.insert(key("v02"), 2_000, 2);
    });
    let live = (0..5)
        .filter(|i| map.get(format!("v{i:02}").as_bytes()).is_some())
        .count();
    assert!(live >= 5);
}

#[test]
fn arena_panicking_as_bytes_on_every_path() {
    for (name, keys, probe) in shapes() {
        for nth in 1..=4 {
            let map = Arc::new(ArenaArtMap::<PanicKey, u64>::with_capacity(1 << 20));
            for (i, k) in keys.iter().enumerate() {
                map.insert(key(k), i as u64);
            }
            arm(nth);
            let _ = catch_unwind(AssertUnwindSafe(|| {
                map.insert(key(probe), 99);
            }));
            disarm();
            let m = Arc::clone(&map);
            within_timeout(move || {
                let _ = m.insert(key(&format!("{probe}~")), 1);
            });
            let mut map = Arc::try_unwrap(map).ok().unwrap();
            map.validate_invariants();
            assert_eq!(map.len(), map.iter().count(), "{name}");
        }
    }
}

#[test]
fn arena_versioned_panicking_as_bytes() {
    let map = Arc::new(ArenaVersionedArtMap::<PanicKey, u64>::with_capacity(
        1 << 20,
    ));
    for i in 0..20u64 {
        map.insert(key(&format!("v{:02}", i % 5)), i, i);
    }
    for nth in 1..=3 {
        arm(nth);
        let _ = catch_unwind(AssertUnwindSafe(|| map.insert(key("v01"), 50, 0)));
        disarm();
        arm(nth);
        let _ = catch_unwind(AssertUnwindSafe(|| map.insert(key("vx"), 1, 0)));
        disarm();
    }
    let m = Arc::clone(&map);
    within_timeout(move || m.insert(key("v04"), 60, 1));
    let mut map = Arc::try_unwrap(map).ok().unwrap();
    map.validate_invariants();
}

static VERSIONED_RE: VersionedArtMap<Vec<u8>, VReValue> = VersionedArtMap::new();

/// A versioned value whose deferred `Drop` re-enters the map, and the key,
/// it was replaced or pruned in. Values written from a destructor carry
/// `u64::MAX` and do not re-enter again.
struct VReValue(u64);

impl Drop for VReValue {
    fn drop(&mut self) {
        if self.0 == u64::MAX {
            return;
        }
        let _ = VERSIONED_RE.version_count(&b"hot"[..]);
        if self.0 % 4 == 0 {
            VERSIONED_RE.insert(b"hot".to_vec(), 1_000_000 + self.0, VReValue(u64::MAX));
            VERSIONED_RE.delete(format!("gone-{}", self.0).into_bytes(), 1);
        }
    }
}

#[test]
fn versioned_reentrant_deferred_drop() {
    // Same-version replaces, deletes and prunes each detach versions whose
    // destructors run later, inside other map operations, and write back.
    let n = if cfg!(miri) { 24 } else { 1500 };
    for i in 0..n {
        VERSIONED_RE.insert(b"hot".to_vec(), i, VReValue(i));
        VERSIONED_RE.insert(b"hot".to_vec(), i, VReValue(i + 1)); // same version
        if i % 5 == 0 {
            VERSIONED_RE.delete(b"hot".to_vec(), i + 1);
        }
        if i % 7 == 0 {
            VERSIONED_RE.prune_key(&b"hot"[..], i, |_| false);
        }
    }
    within_timeout(|| {
        VERSIONED_RE.insert(b"after".to_vec(), 1, VReValue(u64::MAX));
        VERSIONED_RE.get_entry(&b"after"[..]).is_some()
    });
    assert!(VERSIONED_RE.version_count(&b"hot"[..]) > 0);
    let e = VERSIONED_RE.get_entry(&b"hot"[..]);
    assert!(e.is_none_or(|e| e.version() > 0));
}
