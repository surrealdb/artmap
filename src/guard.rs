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

//! # Epoch guards and retirement
//!
//! All protection and all retirement in the EBR maps use the process-wide
//! default `crossbeam-epoch` collector (Inv 11). Every artmap pin goes through
//! an artmap-owned per-thread participant, so that a *nested* pin can prove it
//! lands on the same `Local` as an existing guard (§8.2).

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::marker::PhantomData;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

/// An epoch guard for an [`ArtMap`](crate::ArtMap) or
/// [`VersionedArtMap`](crate::VersionedArtMap).
///
/// Obtainable only from a map's `pin()`, so it always wraps a real pin on the
/// default collector. While it is alive, nothing that was reachable from any
/// map when it was pinned is freed; references obtained through it are bounded
/// by both the guard and the map borrow.
///
/// Holding a guard delays reclamation process-wide, for every user of the
/// default collector. Do not hold one across I/O or long computations.
///
/// `Guard` is neither `Send` nor `Sync`.
pub struct Guard<'m> {
    pub(crate) inner: crossbeam_epoch::Guard,
    _map: PhantomData<&'m ()>,
}

impl Guard<'_> {
    pub(crate) fn new() -> Self {
        Self {
            inner: pin(),
            _map: PhantomData,
        }
    }

    /// Unpins and re-pins, letting the global epoch advance.
    ///
    /// Borrowck forbids this while any entry or iterator borrows the guard.
    /// Like `crossbeam_epoch::Guard::repin`, it only advances the epoch when
    /// this is the thread's only live guard.
    #[inline]
    pub fn repin(&mut self) {
        self.inner.repin();
    }
}

impl std::fmt::Debug for Guard<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Guard").finish_non_exhaustive()
    }
}

/// artmap's own participant on the default collector.
///
/// `crossbeam_epoch::is_pinned()` cannot prove that a nested pin protects an
/// existing guard's objects: on targets where std uses OS-key TLS, crossbeam's
/// handle can be re-initialised after its destructor ran, so a "nested" pin may
/// land on an unrelated `Local`, which does not protect objects retired before
/// it. Tagging every pin with the participant ID closes that gap.
struct Participant {
    handle: crossbeam_epoch::LocalHandle,
    id: u64,
}

fn next_participant_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

thread_local! {
    static PARTICIPANT: Participant = Participant {
        handle: crossbeam_epoch::default_collector().register(),
        id: next_participant_id(),
    };
}

/// Every outermost artmap pin (`map.pin()`, `get`, `iter`, …).
#[inline]
pub(crate) fn pin_tagged() -> (crossbeam_epoch::Guard, u64) {
    PARTICIPANT
        .try_with(|p| (p.handle.pin(), p.id))
        // TLS teardown: a fresh `Local`, pinned now. It protects everything it
        // reaches from now on, but can never be extended (id 0).
        .unwrap_or_else(|_| (crossbeam_epoch::default_collector().register().pin(), 0))
}

/// An internal pin for operations that do not return a handle.
#[inline]
pub(crate) fn pin() -> crossbeam_epoch::Guard {
    pin_tagged().0
}

/// A nested pin on the same `Local` as a live guard taken by [`pin_tagged`]
/// with `id`.
///
/// # Panics
/// Panics if that cannot be proven, which happens only during thread-local
/// destruction.
#[inline]
pub(crate) fn nested_pin(id: u64) -> crossbeam_epoch::Guard {
    PARTICIPANT
        .try_with(|p| (id != 0 && p.id == id).then(|| p.handle.pin()))
        .ok()
        .flatten()
        .expect("artmap: cannot extend an epoch guard during thread-local destruction")
}

/// How a handle keeps its epoch protection (§8.2).
pub(crate) enum GuardHandle<'a> {
    /// `get`, `get_or_insert_with`, D1 returns: a pin owned by the handle,
    /// tagged with the participant it was taken on.
    Owned(crossbeam_epoch::Guard, u64),
    /// Items of an owned iterator: one pin shared by the iterator and its items.
    Shared(Rc<crossbeam_epoch::Guard>, u64),
    /// `*_with_guard` APIs: the caller's [`Guard`].
    Borrowed(&'a crossbeam_epoch::Guard),
}

impl GuardHandle<'_> {
    /// A fresh owned pin.
    #[inline]
    pub(crate) fn owned() -> Self {
        let (g, id) = pin_tagged();
        GuardHandle::Owned(g, id)
    }

    #[inline]
    pub(crate) fn guard(&self) -> &crossbeam_epoch::Guard {
        match self {
            GuardHandle::Owned(g, _) => g,
            GuardHandle::Shared(g, _) => g,
            GuardHandle::Borrowed(g) => g,
        }
    }

    /// Duplicates the protection: a nested pin, a shared `Rc`, or a copy of
    /// the borrow. Never an unverified nested pin (§8.2).
    #[inline(always)]
    pub(crate) fn duplicate(&self) -> Self {
        match self {
            GuardHandle::Owned(_, id) => GuardHandle::Owned(nested_pin(*id), *id),
            GuardHandle::Shared(rc, id) => GuardHandle::Shared(Rc::clone(rc), *id),
            GuardHandle::Borrowed(g) => GuardHandle::Borrowed(g),
        }
    }
}

/// An allocation that has been unlinked and is exclusively owned by the
/// retiring thread. Consumed by exactly one deferred destructor (§6.2).
pub(crate) struct Retired<T>(NonNull<T>);

// SAFETY: a `Retired<T>` is only created for an unlinked, exclusively owned
// allocation (Inv 1). Dropping `T` on another thread requires `T: Send`.
unsafe impl<T: Send> Send for Retired<T> {}

impl<T> Retired<T> {
    /// # Safety
    /// `ptr` came from `Box::into_raw` or `Box::leak`, is unreachable from the
    /// tree for any thread pinned after this call, and is retired exactly once.
    /// Never an interior pointer (for example an inline version slot).
    #[inline]
    pub(crate) unsafe fn from_non_null(ptr: NonNull<T>) -> Self {
        Self(ptr)
    }

    /// # Safety
    /// Only called from the single deferred destructor.
    #[inline]
    unsafe fn into_box(self) -> Box<T> {
        // SAFETY: per `from_non_null`, a live `Box` allocation owned by `self`.
        unsafe { Box::from_raw(self.0.as_ptr()) }
    }
}

/// Retires `r` through `guard`, which must be pinned on the default collector
/// (every guard artmap creates is). The destructor runs after the grace
/// period, on an arbitrary thread.
#[inline]
pub(crate) fn retire<T: Send + 'static>(guard: &crossbeam_epoch::Guard, r: Retired<T>) {
    // Capture `r` whole via the method call; never write `r.0` in the closure.
    // SAFETY: `r` is unlinked and owned by this closure alone (Retired
    // contract); crossbeam runs it only after the grace period has elapsed.
    guard.defer(move || unsafe { drop(r.into_box()) });
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn nested_pin_matches_participant() {
        let (g, id) = pin_tagged();
        assert_ne!(id, 0);
        let n = nested_pin(id);
        drop(g);
        drop(n);
    }

    #[test]
    #[should_panic(expected = "thread-local destruction")]
    fn nested_pin_rejects_unknown_participant() {
        drop(nested_pin(0));
    }

    #[test]
    fn nested_pin_rejects_other_thread_participant() {
        let (_g, id) = pin_tagged();
        let other =
            std::thread::spawn(move || std::panic::catch_unwind(|| nested_pin(id)).is_err())
                .join()
                .unwrap();
        assert!(other, "a participant id from another thread is rejected");
    }

    #[test]
    fn retire_runs_destructor_once() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc;
        struct D(Arc<AtomicUsize>);
        impl Drop for D {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let n = Arc::new(AtomicUsize::new(0));
        let p = crate::raw::boxed(D(Arc::clone(&n)));
        {
            let g = pin();
            // SAFETY: a leaked Box, never reachable from anything, retired once.
            retire(&g, unsafe { Retired::from_non_null(p) });
        }
        // Other tests pin concurrently, so the epoch may lag: keep flushing.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while n.load(Ordering::Relaxed) == 0 && std::time::Instant::now() < deadline {
            pin().flush();
        }
        assert_eq!(n.load(Ordering::Relaxed), 1);
    }
}
