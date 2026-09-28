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

//! # The heap tree behind [`VersionedArtMap`](crate::VersionedArtMap)
//!
//! The ART structure is the shared core; this module adds the version-chain
//! protocol (§11.4):
//!
//! - every chain mutation holds the leaf's `chain_latch`, which is terminal in
//!   the lock order, and no ART latch is involved (siblings update in parallel);
//! - the writer re-loads `head` and finds its position under the latch; hints
//!   read before the latch are never used to link;
//! - same-version replacement is out of place: a new node is linked and the
//!   old one marked `superseded`, then retired (heap) or left to the leaf
//!   (inline slot) after unlocking;
//! - `len` changes by the liveness of the head before and after, decided under
//!   the latch;
//! - readers are lock-free: `head` and `next` are loaded with `Acquire`.

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::ptr::NonNull;

use crate::guard::{retire, Retired};
use crate::key::AsBytes;
use crate::latch::AbortOnUnwind;
use crate::raw::heap::HeapStorage;
use crate::raw::{Mode, Outcome, RawTree, Unpublished};
use crate::versioned::node::{VersionNode, VersionedLeaf};

pub(crate) type Storage<K, V> = HeapStorage<VersionedLeaf<K, V>>;

/// A leaf and the head version that `remove_head` replaced.
pub(crate) type RemovedHead<K, V> = (NonNull<VersionedLeaf<K, V>>, NonNull<VersionNode<V>>);

/// The concurrent tree of a `VersionedArtMap`.
pub(crate) struct VersionedTree<K, V> {
    pub(crate) raw: RawTree<Storage<K, V>>,
}

impl<K, V> VersionedTree<K, V> {
    crate::sync::const_fn_unless_loom! {
        pub(crate) fn new() -> Self {
            Self {
                raw: RawTree::new_in(HeapStorage::new()),
            }
        }
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.raw.len()
    }
}

impl<K, V> Drop for VersionedTree<K, V> {
    fn drop(&mut self) {
        // SAFETY: exclusive access in `drop` (Inv 2). Each leaf's drop frees
        // its live chain; detached heap nodes belong to EBR.
        unsafe { self.raw.destroy() }
    }
}

/// The newest node with `version <= max` in the chain starting at `head`.
#[inline]
pub(crate) fn find_le<V>(head: &VersionNode<V>, max: u64) -> Option<&VersionNode<V>> {
    let mut cur: *const VersionNode<V> = head;
    while !cur.is_null() {
        // SAFETY: chain nodes reachable from a protected leaf are protected
        // (EBR for heap nodes, the leaf for inline slots).
        let n = unsafe { &*cur };
        if n.version <= max {
            return Some(n);
        }
        cur = n.next();
    }
    None
}

/// Iterates a chain, newest first.
pub(crate) fn chain<V>(head: &VersionNode<V>) -> impl Iterator<Item = &VersionNode<V>> {
    let mut cur: *const VersionNode<V> = head;
    std::iter::from_fn(move || {
        if cur.is_null() {
            return None;
        }
        // SAFETY: as for `find_le`.
        let n = unsafe { &*cur };
        cur = n.next();
        Some(n)
    })
}

impl<K: AsBytes + Send + 'static, V: Send + 'static> VersionedTree<K, V> {
    /// Adds `version` of `key` (a tombstone if `value` is `None`). The caller
    /// holds `guard` (Inv 6). Returns the change in the number of live keys.
    pub(crate) fn insert(
        &self,
        key: K,
        version: u64,
        value: Option<V>,
        guard: &crossbeam_epoch::Guard,
    ) -> isize {
        // Fast path: the key exists. The probe on the caller's key is a hint
        // (Inv 10); the chain protocol does not depend on it.
        if let Some(leaf) = self.raw.get(key.as_bytes()) {
            drop(key);
            return self.apply(leaf, version, value, guard);
        }
        let count = isize::from(value.is_some());
        let leaf = VersionedLeaf::new_boxed(key, version, value);
        // SAFETY: a fresh, fully initialised Box allocation owned by this call.
        let owner = unsafe { Unpublished::new(&self.raw.storage, leaf) };
        // SAFETY: `owner` keeps the leaf alive for the call (§9.3).
        let key_bytes = unsafe { leaf.as_ref() }.key.as_bytes();
        let outcome = match self
            .raw
            .insert(&owner, key_bytes, Mode::InsertIfAbsent, count, guard)
        {
            Ok(o) => o,
            Err(never) => match never {},
        };
        match outcome {
            Outcome::Inserted(_) => count,
            Outcome::Existing(existing) => {
                // Lost the race to another inserter: move the version out of
                // the unpublished leaf and apply it to the live one. `owner`
                // then frees the husk (and its key) outside every latch.
                // SAFETY: never published, exclusively owned through `owner`.
                let (version, value) = unsafe { VersionedLeaf::take_first(owner.ptr()) };
                self.apply(existing, version, value, guard)
            }
            Outcome::Replaced(_) => unreachable!("InsertIfAbsent never replaces"),
        }
    }

    /// Applies one version to a published leaf under its chain latch.
    /// Returns the change in the number of live keys.
    fn apply(
        &self,
        leaf: NonNull<VersionedLeaf<K, V>>,
        version: u64,
        value: Option<V>,
        guard: &crossbeam_epoch::Guard,
    ) -> isize {
        // SAFETY: a published leaf, protected by `guard`; versioned leaves are
        // never unlinked before Phase 8.
        let leaf = unsafe { leaf.as_ref() };
        // `value` is declared before the guard, so an unwind drops the latch
        // guard first and runs `V::drop` outside the latch.
        let value = value;
        let Some(w) = leaf.chain_latch.lock() else {
            unreachable!("chain latches are only obsoleted by Phase 8 unlinks")
        };
        // Positions are found under the latch.
        let head = leaf.head_ptr();
        // SAFETY: `head` is live (see `VersionedLeaf::head`).
        let old_live = !unsafe { &*head }.is_tombstone();
        // Every node reachable from `head` under the latch is live and not
        // superseded, and protected by `guard`.
        let at = |p: *mut VersionNode<V>| -> Option<&VersionNode<V>> {
            // SAFETY: as stated above; null means the end of the chain.
            unsafe { p.as_ref() }
        };
        let mut prev: *mut VersionNode<V> = std::ptr::null_mut();
        let mut cur = head;
        while let Some(n) = at(cur).filter(|n| n.version > version) {
            prev = cur;
            cur = n.next();
        }
        let node = leaf.alloc_version(&w, version, value);
        let replaced = at(cur).is_some_and(|n| n.version == version);
        // SAFETY: `node` is unpublished; `cur` (if non-null) is live.
        unsafe {
            (*node).init_next(if replaced { (*cur).next() } else { cur });
        }
        let bomb = AbortOnUnwind;
        if prev.is_null() {
            leaf.set_head(&w, node);
        } else {
            // SAFETY: `prev` is a live node of this chain.
            unsafe { (*prev).set_next(&w, node) };
        }
        if replaced {
            // SAFETY: `cur` is live and now unlinked by us.
            unsafe { (*cur).mark_superseded(&w) };
        }
        let new_head = leaf.head_ptr();
        // SAFETY: live.
        let new_live = !unsafe { &*new_head }.is_tombstone();
        let delta = isize::from(new_live) - isize::from(old_live);
        self.raw.len_add(delta);
        bomb.defuse();
        drop(w);
        if replaced {
            // SAFETY: unlinked under the chain latch, exactly once.
            unsafe { retire_version(cur, guard) };
        }
        delta
    }

    /// The deprecated unversioned remove (§11.5): publishes a tombstone with
    /// the head's version in place of a live head. Returns the old head.
    pub(crate) fn remove_head(
        &self,
        key: &[u8],
        guard: &crossbeam_epoch::Guard,
    ) -> Option<RemovedHead<K, V>> {
        let leaf_ptr = self.raw.get(key)?;
        // SAFETY: protected by `guard`.
        let leaf = unsafe { leaf_ptr.as_ref() };
        let Some(w) = leaf.chain_latch.lock() else {
            unreachable!("chain latches are only obsoleted by Phase 8 unlinks")
        };
        let head = leaf.head_ptr();
        // SAFETY: live.
        let h = unsafe { &*head };
        if h.is_tombstone() {
            return None;
        }
        let tomb = leaf.alloc_version(&w, h.version, None);
        // SAFETY: unpublished.
        unsafe { (*tomb).init_next(h.next()) };
        let bomb = AbortOnUnwind;
        leaf.set_head(&w, tomb);
        h.mark_superseded(&w);
        self.raw.len_add(-1);
        bomb.defuse();
        drop(w);
        // SAFETY: unlinked under the chain latch, exactly once.
        unsafe { retire_version(head, guard) };
        // SAFETY: non-null.
        Some((leaf_ptr, unsafe { NonNull::new_unchecked(head) }))
    }

    /// Prunes versions older than the newest one `<= min_version` (§11.4).
    /// Returns the number of versions unlinked from the chain.
    pub(crate) fn prune_leaf<F: Fn(&V) -> bool>(
        &self,
        leaf: NonNull<VersionedLeaf<K, V>>,
        min_version: u64,
        is_tombstone: &F,
        guard: &crossbeam_epoch::Guard,
    ) -> usize {
        // SAFETY: protected by `guard`.
        let leaf = unsafe { leaf.as_ref() };
        loop {
            // 1. Without any latch, find T and evaluate the user closure on it.
            let head = leaf.head_ptr();
            // SAFETY: live.
            let Some(t) = find_le(unsafe { &*head }, min_version) else {
                return 0;
            };
            let t_ptr = std::ptr::from_ref(t).cast_mut();
            let is_head = t_ptr == head;
            let dead = is_head && t.value.as_ref().is_none_or(is_tombstone);

            // 2. Re-check under the latch.
            let Some(w) = leaf.chain_latch.lock() else {
                unreachable!("chain latches are only obsoleted by Phase 8 unlinks")
            };
            let head_now = leaf.head_ptr();
            // SAFETY: live.
            let t_now = find_le(unsafe { &*head_now }, min_version)
                .map(|n| std::ptr::from_ref(n).cast_mut());
            if t_now != Some(t_ptr) || (is_head && head_now != head) || t.is_superseded() {
                drop(w);
                continue;
            }

            // 3. Replace a user tombstone head by a built-in one.
            let bomb = AbortOnUnwind;
            let mut unlinked: Vec<*mut VersionNode<V>> = Vec::new();
            let kept: &VersionNode<V> = if dead && !t.is_tombstone() {
                let b = leaf.alloc_version(&w, t.version, None);
                // A fresh node: `next` is already null.
                leaf.set_head(&w, b);
                t.mark_superseded(&w);
                self.raw.len_add(-1);
                unlinked.push(t_ptr);
                // SAFETY: just linked; live.
                unsafe { &*b }
            } else {
                t
            };

            // 4. Detach everything older than the kept node.
            let mut cur = t.next();
            if std::ptr::eq(kept, t) {
                kept.set_next(&w, std::ptr::null_mut());
            }
            while !cur.is_null() {
                // SAFETY: a detached node, immutable now (unreachable from head).
                let n = unsafe { &*cur };
                n.mark_superseded(&w);
                unlinked.push(cur);
                cur = n.next();
            }
            bomb.defuse();
            drop(w);

            // 5. Retire every detached heap node, exactly once.
            let count = unlinked.len();
            for n in unlinked {
                // SAFETY: unlinked under the chain latch by this call only.
                unsafe { retire_version(n, guard) };
            }
            return count;
        }
    }
}

/// Retires a heap version node; inline nodes are left to their leaf (D5).
///
/// # Safety
/// `n` is unlinked from its chain, marked superseded, and passed here once.
unsafe fn retire_version<V: Send + 'static>(
    n: *mut VersionNode<V>,
    guard: &crossbeam_epoch::Guard,
) {
    // SAFETY: `n` is live until retired.
    if unsafe { (*n).inline } {
        return;
    }
    // SAFETY: a heap node from `Box::into_raw`, unlinked, retired once.
    retire(guard, unsafe {
        Retired::from_non_null(NonNull::new_unchecked(n))
    });
}
