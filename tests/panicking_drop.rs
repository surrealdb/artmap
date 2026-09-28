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

//! Fault injection: a value whose `Drop` panics, run by the epoch collector
//! after a same-version replace, a delete and a prune (§16.5). `Drop` must not
//! panic (crate docs); if it does, the map must stay usable and memory-safe.
//!
//! The collector abandons the rest of a bag of deferred frees when one of them
//! panics, so this test leaks by design and is not run under LeakSanitizer.
//! It has its own binary so that no other test's thread collects the garbage
//! (where the panic is not armed).

use std::cell::Cell;
use std::panic::catch_unwind;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use artmap::VersionedArtMap;

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

thread_local! {
    /// Arms `PanicDrop` on this thread only: deferred destructors can run on
    /// other tests' threads, which must not panic.
    static DROP_PANICS: Cell<bool> = const { Cell::new(false) };
}

/// A value whose `Drop` panics when armed on the dropping thread.
struct PanicDrop(#[allow(dead_code)] u64);

impl Drop for PanicDrop {
    fn drop(&mut self) {
        if DROP_PANICS.with(|p| p.replace(false)) && !std::thread::panicking() {
            panic!("injected drop panic");
        }
    }
}

#[test]
fn versioned_panicking_drop_after_replace_delete_and_prune() {
    // `Drop` must not panic (crate docs), but if it does, the map stays
    // usable: values are dropped after the chain latch is released.
    let map = Arc::new(VersionedArtMap::<Vec<u8>, PanicDrop>::new());
    let mut fired = 0;
    for round in 0..if cfg!(miri) { 3 } else { 50 } {
        let v = round * 10;
        map.insert(b"k".to_vec(), v + 1, PanicDrop(1));
        map.insert(b"k".to_vec(), v + 1, PanicDrop(2)); // same-version replace
        map.delete(b"k".to_vec(), v + 2);
        map.insert(b"k".to_vec(), v + 3, PanicDrop(3));
        map.prune_key(&b"k"[..], v + 3, |_| false);
        // Run the deferred destructors here, with a panic armed.
        for _ in 0..if cfg!(miri) { 8 } else { 256 } {
            DROP_PANICS.with(|p| p.set(true));
            fired += usize::from(catch_unwind(|| crossbeam_epoch::pin().flush()).is_err());
        }
        DROP_PANICS.with(|p| p.set(false));
        let m = Arc::clone(&map);
        within_timeout(move || {
            m.insert(b"sibling".to_vec(), 1, PanicDrop(0));
            m.insert(b"k".to_vec(), v + 4, PanicDrop(4));
            assert_eq!(m.get_entry(&b"k"[..]).map(|e| e.version()), Some(v + 4));
        });
        assert_eq!(map.len(), 2);
    }
    assert!(fired > 0 || cfg!(miri), "no injected drop panic fired");
}
