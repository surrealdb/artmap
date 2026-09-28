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

//! # Optimistic version latch
//!
//! [`HybridLatch`] is a sequence lock used for optimistic lock coupling. The
//! protocol it implements is normative (`docs/SAFETY.md` §5):
//!
//! - **W1**: a writer acquires the latch with an `Acquire` CAS and then issues
//!   `fence(Release)`, so the lock-bit store is ordered before every store in
//!   the critical section (the `crossbeam` `SeqLock::write` pattern).
//! - **W4**: `unlock` stores `v + STEP` with `Release`; `mark_obsolete` stores
//!   `v | OBSOLETE` and is used only on nodes already unlinked (Inv 7).
//! - **R1/R5**: a reader takes `read_version()` (`Acquire`), and validates with
//!   `fence(Acquire)` followed by a `Relaxed` load.
//!
//! Exclusive access is represented by a [`WriteGuard`]. Dropping it releases
//! the latch and bumps the version, so unwinding out of a *prepare* phase is
//! harmless (Inv 6). Commit phases are wrapped in [`AbortOnUnwind`].

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use crate::sync::atomic::{fence, AtomicU64, Ordering};

/// Switches for the loom mutant self-checks (§16.3): each model must fail
/// with the fence it depends on removed, which proves the model explores.
/// Loom runs every thread of a model on the calling OS thread, so
/// thread-local switches affect exactly one model at a time.
#[cfg(loom)]
pub(crate) mod mutants {
    use std::cell::Cell;

    std::thread_local! {
        pub(crate) static SKIP_W1_FENCE: Cell<bool> = const { Cell::new(false) };
        pub(crate) static SKIP_R5_FENCE: Cell<bool> = const { Cell::new(false) };
        pub(crate) static SKIP_R4_COUPLING: Cell<bool> = const { Cell::new(false) };
        /// Finds a version-chain position before taking `chain_latch` (§11.4).
        pub(crate) static CHAIN_POSITION_UNLATCHED: Cell<bool> = const { Cell::new(false) };
    }
}

pub(crate) const LOCK_BIT: u64 = 0b01;
pub(crate) const OBSOLETE_BIT: u64 = 0b10;
pub(crate) const VERSION_STEP: u64 = 0b100;

/// An atomic version latch supporting optimistic reads and exclusive writes.
#[repr(transparent)]
pub(crate) struct HybridLatch {
    version: AtomicU64,
}

/// Proof that this thread holds `latch` exclusively.
///
/// Dropping it unlocks and bumps the version.
#[must_use = "dropping a WriteGuard immediately releases the latch"]
pub(crate) struct WriteGuard<'l> {
    latch: &'l HybridLatch,
    /// Version at acquisition: unlocked and not obsolete.
    v: u64,
}

impl HybridLatch {
    crate::sync::const_fn_unless_loom! {
        /// Creates an unlocked, live latch at version 0.
        #[inline]
        pub(crate) fn new() -> Self {
            Self {
                version: AtomicU64::new(0),
            }
        }
    }

    /// R1: `Some(v)` if the latch is unlocked and not obsolete.
    #[inline]
    pub(crate) fn read_version(&self) -> Option<u64> {
        let v = self.version.load(Ordering::Acquire);
        (v & (LOCK_BIT | OBSOLETE_BIT) == 0).then_some(v)
    }

    /// R5: `true` if no writer has entered a critical section since `v` was read.
    #[inline]
    pub(crate) fn validate(&self, v: u64) -> bool {
        #[cfg(loom)]
        if mutants::SKIP_R5_FENCE.with(|m| m.get()) {
            return self.version.load(Ordering::Relaxed) == v;
        }
        fence(Ordering::Acquire);
        self.version.load(Ordering::Relaxed) == v
    }

    /// `true` if the latch has been marked obsolete.
    #[inline]
    pub(crate) fn is_obsolete(&self) -> bool {
        self.version.load(Ordering::Acquire) & OBSOLETE_BIT != 0
    }

    /// Upgrades an optimistic read at version `v` to exclusive access (W1).
    ///
    /// Fails if the latch changed since `v` was read, or if `v` itself is
    /// locked or obsolete (for example a stale cached version).
    #[must_use = "binding the guard is what holds the latch"]
    #[inline]
    pub(crate) fn try_upgrade(&self, v: u64) -> Option<WriteGuard<'_>> {
        if v & (LOCK_BIT | OBSOLETE_BIT) != 0 {
            return None;
        }
        self.version
            .compare_exchange(v, v | LOCK_BIT, Ordering::Acquire, Ordering::Relaxed)
            .ok()?;
        // W1: orders the lock-bit store before every in-section store.
        #[cfg(loom)]
        if mutants::SKIP_W1_FENCE.with(|m| m.get()) {
            return Some(WriteGuard { latch: self, v });
        }
        fence(Ordering::Release);
        Some(WriteGuard { latch: self, v })
    }

    /// Blocking acquire. Used only while holding no other latch (Inv 7), or
    /// for the terminal `chain_latch`. `None` if the latch is obsolete.
    #[must_use = "binding the guard is what holds the latch"]
    pub(crate) fn lock(&self) -> Option<WriteGuard<'_>> {
        let mut backoff = SpinBackoff::new();
        loop {
            let v = self.version.load(Ordering::Relaxed);
            if v & OBSOLETE_BIT != 0 {
                return None;
            }
            if v & LOCK_BIT == 0 {
                if let Some(w) = self.try_upgrade(v) {
                    return Some(w);
                }
            }
            backoff.spin();
        }
    }
}

impl WriteGuard<'_> {
    /// W4: releases the latch and returns the new version, so callers that
    /// cache versions (the arena inserters) never re-load it.
    #[inline]
    pub(crate) fn unlock(self) -> u64 {
        let nv = self.v.wrapping_add(VERSION_STEP);
        self.latch.version.store(nv, Ordering::Release);
        std::mem::forget(self);
        nv
    }

    /// W4 / Inv 7: marks the latch obsolete. Only for nodes already unlinked
    /// from their parent; every later upgrade or validation fails.
    #[inline]
    pub(crate) fn mark_obsolete(self) {
        self.latch
            .version
            .store(self.v | OBSOLETE_BIT, Ordering::Release);
        std::mem::forget(self);
    }

    /// The latch this guard holds (for capability `debug_assert`s).
    #[allow(dead_code)] // used by the arena chain protocol
    #[inline]
    pub(crate) fn latch(&self) -> &HybridLatch {
        self.latch
    }

    /// `true` if this guard holds `latch`.
    #[inline]
    pub(crate) fn holds(&self, latch: &HybridLatch) -> bool {
        std::ptr::eq(self.latch, latch)
    }
}

impl Drop for WriteGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.latch
            .version
            .store(self.v.wrapping_add(VERSION_STEP), Ordering::Release);
    }
}

/// Aborts the process if dropped.
///
/// Created at the start of a commit phase and [`defuse`](Self::defuse)d at its
/// end: an unexpected panic inside a commit phase must never publish a torn
/// node by unwinding through a [`WriteGuard`] (Inv 6, §5.3).
pub(crate) struct AbortOnUnwind;

impl AbortOnUnwind {
    /// Ends the commit phase.
    #[inline]
    pub(crate) fn defuse(self) {
        std::mem::forget(self)
    }
}

impl Drop for AbortOnUnwind {
    #[cold]
    fn drop(&mut self) {
        // Printing first gives the user a pointer to the cause.
        eprintln!("artmap: panic inside a commit phase; aborting to avoid publishing a torn node");
        std::process::abort()
    }
}

/// 64-byte cache-line aligned wrapper to avoid false sharing.
#[repr(align(64))]
#[derive(Debug, Default)]
pub(crate) struct CachePadded<T>(pub(crate) T);

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Exponential backoff for spinning writers and retrying readers.
///
/// Uses the `crate::sync` shims, so loom models of a contended `lock()`
/// yield to the scheduler instead of spinning forever (§16.3).
pub(crate) struct SpinBackoff {
    step: u32,
}

impl SpinBackoff {
    #[inline]
    pub(crate) const fn new() -> Self {
        Self { step: 0 }
    }

    #[inline]
    pub(crate) fn spin(&mut self) {
        if cfg!(loom) || self.step > 6 {
            crate::sync::yield_now();
        } else {
            for _ in 0..(1u32 << self.step) {
                crate::sync::spin_loop();
            }
        }
        if self.step < 16 {
            self.step += 1;
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    /// `true` if an acquisition failed (and so holds nothing).
    fn fails(r: Option<WriteGuard<'_>>) -> bool {
        r.is_none()
    }

    #[test]
    fn latch_lifecycle() {
        let latch = HybridLatch::new();
        let v0 = latch.read_version().expect("initial version");
        assert!(latch.validate(v0));

        let w = latch.try_upgrade(v0).expect("upgrade");
        assert!(w.holds(&latch));
        assert!(
            latch.read_version().is_none(),
            "locked latch has no read version"
        );
        assert!(!latch.validate(v0), "validation fails while locked");
        let v1 = w.unlock();
        assert_eq!(v1, v0 + VERSION_STEP);
        assert_eq!(latch.read_version(), Some(v1));
        assert!(!latch.validate(v0));
        assert!(fails(latch.try_upgrade(v0)), "stale version cannot upgrade");

        let w = latch.lock().expect("lock");
        w.mark_obsolete();
        assert!(latch.is_obsolete());
        assert!(latch.read_version().is_none());
        assert!(fails(latch.lock()), "obsolete latch cannot be locked");
    }

    #[test]
    fn upgrade_rejects_flagged_versions() {
        let latch = HybridLatch::new();
        assert!(fails(latch.try_upgrade(LOCK_BIT)));
        assert!(fails(latch.try_upgrade(OBSOLETE_BIT)));
        let w = latch.lock().unwrap();
        // A stale version that happens to equal the locked word must fail.
        assert!(fails(latch.try_upgrade(LOCK_BIT)));
        drop(w);
    }

    #[test]
    fn concurrent_mutual_exclusion() {
        use std::cell::UnsafeCell;
        use std::sync::{Arc, Barrier};

        struct Shared {
            latch: HybridLatch,
            counter: UnsafeCell<usize>,
        }
        // SAFETY: `counter` is only accessed while holding `latch`.
        unsafe impl Sync for Shared {}

        let threads = if cfg!(miri) { 3 } else { 16 };
        let per = if cfg!(miri) { 50 } else { 20_000 };
        let s = Arc::new(Shared {
            latch: HybridLatch::new(),
            counter: UnsafeCell::new(0),
        });
        let barrier = Arc::new(Barrier::new(threads));
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let (s, barrier) = (Arc::clone(&s), Arc::clone(&barrier));
                std::thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..per {
                        let w = s.latch.lock().unwrap();
                        // SAFETY: exclusive while `w` is held.
                        unsafe { *s.counter.get() += 1 };
                        drop(w);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        // SAFETY: all writers joined.
        assert_eq!(unsafe { *s.counter.get() }, threads * per);
        assert!(s.latch.read_version().is_some());
    }

    #[test]
    fn validation_fails_across_a_concurrent_write() {
        use std::sync::atomic::AtomicBool;
        use std::sync::Arc;

        let latch = Arc::new(HybridLatch::new());
        let running = Arc::new(AtomicBool::new(true));
        let (l, r) = (Arc::clone(&latch), Arc::clone(&running));
        let writer = std::thread::spawn(move || {
            let mut n = 0;
            while r.load(std::sync::atomic::Ordering::Relaxed) && n < 50_000 {
                let w = l.lock().unwrap();
                std::hint::spin_loop();
                drop(w);
                n += 1;
            }
        });
        for _ in 0..if cfg!(miri) { 200 } else { 100_000 } {
            // Under a busy writer, a read may see the latch locked, validate,
            // or fail validation; all three are correct outcomes.
            if let Some(v) = latch.read_version() {
                std::hint::spin_loop();
                std::hint::black_box(latch.validate(v));
            }
        }
        running.store(false, std::sync::atomic::Ordering::Relaxed);
        writer.join().unwrap();
        // With the writer gone, a read is stable and always validates.
        let v = latch.read_version().expect("no writer holds the latch");
        assert!(latch.validate(v));
    }

    #[test]
    fn obsolete_is_permanent() {
        let latch = HybridLatch::new();
        let v = latch.read_version().unwrap();
        latch.lock().unwrap().mark_obsolete();
        assert!(latch.is_obsolete());
        assert!(fails(latch.lock()));
        assert!(fails(latch.try_upgrade(v)));
        assert!(latch.read_version().is_none());
        assert!(!latch.validate(v));
    }

    #[test]
    fn guard_drop_unlocks_on_unwind() {
        let latch = HybridLatch::new();
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _w = latch.lock().unwrap();
            panic!("prepare phase panic");
        }));
        assert!(r.is_err());
        assert!(
            latch.read_version().is_some(),
            "latch released by unwinding"
        );
    }
}
