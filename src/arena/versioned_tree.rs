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

//! # The arena tree behind [`ArenaVersionedArtMap`](crate::ArenaVersionedArtMap)
//!
//! The version-chain protocol of §11.4 and §12.7, with arena offsets:
//! every chain writer holds the leaf's `chain_latch` (terminal, Inv 7) and
//! finds its position under it; version nodes are allocated *before* the
//! latch is taken (no arena allocation under it); same-version replacement is
//! out of place; `len` follows the head's liveness; unlinked version nodes go
//! on a retired list that `Drop` drains. Leaves are unlinked as in the heap
//! tree (§13): only once their chain latch is killed, and then onto the
//! storage's retired list, linked through the dead latch.

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::cell::Cell;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::arena::node::{VersionNode, VersionedLeaf};
use crate::arena::storage::{ArenaLeaf, ArenaStorage};
use crate::arena::tree::InserterCache;
use crate::arena::Arena;
use crate::key::AsBytes;
use crate::latch::{AbortOnUnwind, SpinBackoff};
use crate::raw::{Mode, Outcome, RawTree, Unpublished};
use crate::sync::atomic::{AtomicU32, Ordering};
use crate::versioned::tree::Unlinked;

pub(crate) type Storage<K, V> = ArenaStorage<VersionedLeaf<K, V>>;

/// The leaf and the offset of the head version `remove_head` replaced, or
/// `Err(())` if the tombstone did not fit.
pub(crate) type RemovedHead<K, V> = Result<Option<(NonNull<VersionedLeaf<K, V>>, u32)>, ()>;

/// The concurrent tree of an `ArenaVersionedArtMap`.
pub(crate) struct ArenaVersionedTree<K, V> {
    pub(crate) raw: RawTree<Storage<K, V>>,
    /// Version nodes unlinked from a live chain while the map is alive
    /// (same-version replaces, deprecated removes, prunes and
    /// `remove_version`), dropped in `Drop`. Unlinked leaves, with their whole
    /// chains, are on the storage's retired list.
    retired_versions: AtomicU32,
}

/// The newest node with `version <= max`, walking from `head`.
pub(crate) fn find_le<'a, V: 'a>(
    arena: &'a Arena,
    head: u32,
    max: u64,
) -> Option<&'a VersionNode<V>> {
    let mut off = head;
    while off != 0 {
        // SAFETY: chain offsets are version nodes of this arena, valid for the
        // map's life (the caller holds the map borrow).
        let n = unsafe { arena.ptr::<VersionNode<V>>(off).as_ref() };
        if n.version <= max {
            return Some(n);
        }
        off = n.next();
    }
    None
}

/// As [`find_le`], returning the node's offset.
fn find_le_off<V>(arena: &Arena, head: u32, max: u64) -> Option<u32> {
    let mut off = head;
    while off != 0 {
        // SAFETY: as for `find_le`.
        let n = unsafe { arena.ptr::<VersionNode<V>>(off).as_ref() };
        if n.version <= max {
            return Some(off);
        }
        off = n.next();
    }
    None
}

/// The chain starting at `head`, newest first.
pub(crate) fn chain<'a, V: 'a>(
    arena: &'a Arena,
    head: u32,
) -> impl Iterator<Item = &'a VersionNode<V>> + 'a {
    let mut off = head;
    std::iter::from_fn(move || {
        if off == 0 {
            return None;
        }
        // SAFETY: as for `find_le`.
        let n = unsafe { arena.ptr::<VersionNode<V>>(off).as_ref() };
        off = n.next();
        Some(n)
    })
}

impl<K, V> ArenaVersionedTree<K, V> {
    pub(crate) fn new(arena: Arc<Arena>) -> Self {
        Self {
            raw: RawTree::new_in(ArenaStorage::new(arena)),
            retired_versions: AtomicU32::new(0),
        }
    }

    #[inline]
    pub(crate) fn arena(&self) -> &Arc<Arena> {
        &self.raw.storage.arena
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.raw.len()
    }

    /// Pushes an unlinked version node onto the retired list (after unlock).
    fn retire_version(&self, off: u32) {
        // SAFETY: a version node of this arena, unlinked by the caller once.
        let n = unsafe { self.arena().ptr::<VersionNode<V>>(off).as_ref() };
        let mut head = self.retired_versions.load(Ordering::Relaxed);
        loop {
            n.next_retired.store(head, Ordering::Relaxed);
            match self.retired_versions.compare_exchange_weak(
                head,
                off,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(h) => head = h,
            }
        }
    }

    /// Allocates an unpublished version node, or hands the value back.
    fn alloc_version(&self, version: u64, value: Option<V>) -> Result<u32, Option<V>> {
        match self
            .raw
            .storage
            .alloc_value(VersionNode::new(version, value))
        {
            Ok(p) => Ok(self.arena().offset_of(p)),
            Err((_, n)) => Err(n.into_value()),
        }
    }
}

impl<K, V> Drop for ArenaVersionedTree<K, V> {
    fn drop(&mut self) {
        if !VersionedLeaf::<K, V>::NEEDS_DROP {
            return;
        }
        // SAFETY: exclusive access in `drop` (Inv 2): the live leaves and their
        // chains first, then the retired versions.
        unsafe { self.raw.destroy() };
        let mut off = self.retired_versions.load(Ordering::Relaxed);
        while off != 0 {
            // SAFETY: each retired offset is a version node pushed once.
            let n = unsafe { self.arena().ptr::<VersionNode<V>>(off) };
            // SAFETY: as above.
            let next = unsafe { n.as_ref() }.next_retired.load(Ordering::Relaxed);
            // SAFETY: dropped exactly once, here.
            unsafe { n.drop_in_place() };
            off = next;
        }
    }
}

impl<K: AsBytes, V> ArenaVersionedTree<K, V> {
    /// The version node at `off`.
    #[inline]
    fn node(&self, off: u32) -> &VersionNode<V> {
        // SAFETY: chain offsets are version nodes of this arena, valid for the
        // map's life.
        unsafe { self.arena().ptr::<VersionNode<V>>(off).as_ref() }
    }

    /// Adds `version` of `key` (a tombstone if `value` is `None`). Returns the
    /// change in the number of live keys, or the key and value if the arena is
    /// full (nothing published, no latch held).
    pub(crate) fn insert(
        &self,
        key: K,
        version: u64,
        value: Option<V>,
        cache: Option<&mut InserterCache>,
    ) -> Result<isize, (K, Option<V>)> {
        let mut backoff = SpinBackoff::new();
        // The version node is allocated before any latch, once, and reused if
        // a dead chain sends this insert round again.
        let mut value = Some(value);
        let mut node = 0u32;
        let alloc = |value: &mut Option<Option<V>>| -> Result<u32, Option<V>> {
            self.alloc_version(version, value.take().expect("allocated once"))
        };
        // Fast path: the key exists. The probe on the caller's key is a hint.
        while let Some(leaf) = self.raw.get(key.as_bytes()) {
            if node == 0 {
                match alloc(&mut value) {
                    Ok(n) => node = n,
                    Err(v) => return Err((key, v)),
                }
            }
            match self.apply(leaf, node) {
                Ok(delta) => return Ok(delta),
                // Dead: the leaf is being unlinked (§13). Look again.
                Err(()) => backoff.spin(),
            }
        }
        if node == 0 {
            match alloc(&mut value) {
                Ok(n) => node = n,
                Err(v) => return Err((key, v)),
            }
        }
        let count = isize::from(!self.node(node).is_tombstone());
        let leaf = match self.raw.storage.alloc_value(VersionedLeaf::new(key, node)) {
            Ok(l) => l,
            Err((_, l)) => {
                // SAFETY: the node was never published: take its value back.
                let n = unsafe { self.arena().ptr::<VersionNode<V>>(node).read() };
                return Err((l.key, n.into_value()));
            }
        };
        // SAFETY: freshly written and exclusively ours.
        let owner = unsafe { Unpublished::new(&self.raw.storage, leaf) };
        // SAFETY: `owner` keeps the leaf alive for the call (§9.3).
        let key_bytes = unsafe { leaf.as_ref() }.key.as_bytes();
        let mut cache = cache;
        if let Some(c) = cache.as_deref_mut() {
            if let Some(r) = c.try_fast(&self.raw, &owner, key_bytes, count) {
                return Ok(match r {
                    Outcome::Inserted(_) => count,
                    _ => unreachable!("the fast path only inserts"),
                });
            }
        }
        loop {
            let mut hint = None;
            match self.raw.insert_hinted(
                &owner,
                key_bytes,
                Mode::InsertIfAbsent,
                count,
                &(),
                &mut hint,
            ) {
                Ok(Outcome::Inserted(_)) => {
                    if let Some(c) = cache {
                        c.remember(hint, key_bytes);
                    }
                    return Ok(count);
                }
                Ok(Outcome::Existing(existing)) => match self.apply(existing, node) {
                    Ok(delta) => {
                        // Lost the race to a live leaf, which now holds our
                        // version node: detach it from the husk, so `owner`
                        // drops only the husk's key.
                        // SAFETY: never published and exclusively ours.
                        unsafe { leaf.as_ref().detach_head_unpublished() };
                        return Ok(delta);
                    }
                    // It is being unlinked: ours takes its place once it is gone.
                    Err(()) => backoff.spin(),
                },
                Ok(Outcome::Replaced(_)) => unreachable!("InsertIfAbsent never replaces"),
                Err(_) => {
                    owner.disarm();
                    // SAFETY: neither the leaf nor its version was published;
                    // take the key and value back out and abandon their bytes.
                    let (l, n) =
                        unsafe { (leaf.read(), self.arena().ptr::<VersionNode<V>>(node).read()) };
                    return Err((l.key, n.into_value()));
                }
            }
        }
    }

    /// Links the unpublished version node `node` into a published leaf.
    /// `Err` if the leaf's chain is dead (§13): nothing was written.
    fn apply(&self, leaf: NonNull<VersionedLeaf<K, V>>, node: u32) -> Result<isize, ()> {
        let arena = &**self.arena();
        // SAFETY: arena leaves are valid for the map's life.
        let leaf = unsafe { leaf.as_ref() };
        let at = |off: u32| -> Option<&VersionNode<V>> {
            // SAFETY: chain offsets are version nodes of this arena.
            (off != 0).then(|| unsafe { arena.ptr::<VersionNode<V>>(off).as_ref() })
        };
        // SAFETY: allocated by the caller, unpublished.
        let new = unsafe { arena.ptr::<VersionNode<V>>(node).as_ref() };
        let version = new.version;
        let lock = || leaf.chain_latch.lock().ok_or(());
        #[cfg(loom)]
        let unlatched = crate::latch::mutants::CHAIN_POSITION_UNLATCHED.with(|m| m.get());
        #[cfg(not(loom))]
        let unlatched = false;
        let early = if unlatched { None } else { Some(lock()?) };
        // Positions are found under the latch.
        let head = leaf.head();
        let old_live = at(head).is_some_and(|h| !h.is_tombstone());
        let mut prev = 0u32;
        let mut cur = head;
        while let Some(n) = at(cur).filter(|n| n.version > version) {
            prev = cur;
            cur = n.next();
        }
        let w = match early {
            Some(w) => w,
            None => lock()?,
        };
        let replaced = at(cur).is_some_and(|n| n.version == version);
        new.init_next(match (replaced, at(cur)) {
            (true, Some(c)) => c.next(),
            _ => cur,
        });
        let bomb = AbortOnUnwind;
        match at(prev) {
            None => leaf.set_head(&w, node),
            Some(p) => p.set_next(&w, node),
        }
        if replaced {
            if let Some(c) = at(cur) {
                c.mark_superseded(&w);
            }
        }
        let new_live = at(leaf.head()).is_some_and(|h| !h.is_tombstone());
        let delta = isize::from(new_live) - isize::from(old_live);
        self.raw.len_add(delta);
        bomb.defuse();
        drop(w);
        if replaced {
            self.retire_version(cur);
        }
        Ok(delta)
    }

    /// The deprecated unversioned remove (§11.5): a same-version tombstone in
    /// place of a live head. Returns the leaf and the replaced head.
    pub(crate) fn remove_head(&self, key: &[u8]) -> RemovedHead<K, V> {
        if self.raw.get(key).is_none() {
            return Ok(None);
        }
        // Allocated before the latch; its version is set before publication.
        let tomb = self.alloc_version(0, None).map_err(|_| ())?;
        let arena = &**self.arena();
        let mut backoff = SpinBackoff::new();
        loop {
            let Some(leaf_ptr) = self.raw.get(key) else {
                // The unused tombstone is abandoned in place; it owns nothing.
                return Ok(None);
            };
            // SAFETY: arena leaves are valid for the map's life.
            let leaf = unsafe { leaf_ptr.as_ref() };
            let Some(w) = leaf.chain_latch.lock() else {
                // Being unlinked: look again.
                backoff.spin();
                continue;
            };
            let head = leaf.head();
            // SAFETY: a version node of this arena.
            let h = unsafe { arena.ptr::<VersionNode<V>>(head).as_ref() };
            if h.is_tombstone() {
                return Ok(None);
            }
            // SAFETY: unpublished; exclusively ours until `set_head`.
            unsafe {
                let t = arena.ptr::<VersionNode<V>>(tomb).as_ptr();
                (*t).version = h.version;
                (*t).init_next(h.next());
            }
            let bomb = AbortOnUnwind;
            leaf.set_head(&w, tomb);
            h.mark_superseded(&w);
            self.raw.len_add(-1);
            bomb.defuse();
            drop(w);
            self.retire_version(head);
            return Ok(Some((leaf_ptr, head)));
        }
    }

    /// Unlinks `leaf` and its whole chain from the tree if `check` accepts it
    /// under the chain latch, killing the latch first; see the heap tree's
    /// `unlink_leaf` (§13). Allocates nothing: the leaf and its versions stay
    /// in the arena, on the retired list.
    fn unlink_leaf(
        &self,
        leaf: NonNull<VersionedLeaf<K, V>>,
        check: impl Fn(&VersionedLeaf<K, V>) -> bool,
    ) -> Unlinked {
        let arena = &**self.arena();
        // User code, outside every latch; the bytes come from the leaf itself
        // (Inv 10).
        // SAFETY: arena leaves are valid for the map's life.
        let key = unsafe { leaf.as_ref() }.key.as_bytes();
        // Set by `confirm`, which runs at most once: `Some(n)` if it accepted
        // a chain of `n` versions, `None` if it refused.
        let seen: Cell<Option<Option<usize>>> = Cell::new(None);
        let removed = self.raw.remove_confirmed(
            key,
            |c| c == leaf,
            |c| {
                seen.set(Some(None));
                // SAFETY: `c` is `leaf`, valid for the map's life.
                let l = unsafe { c.as_ref() };
                let w = l.chain_latch.lock()?;
                if !check(l) {
                    return None;
                }
                seen.set(Some(Some(chain::<V>(arena, l.head()).count())));
                let live = !self.node(l.head()).is_tombstone();
                w.kill();
                Some(isize::from(live))
            },
            &(),
        );
        match (removed, seen.get()) {
            (Some(_), Some(Some(n))) => Unlinked::Done(n),
            (Some(_), _) => unreachable!("confirm accepted without counting"),
            (None, Some(_)) => Unlinked::Refused,
            // SAFETY: arena leaves are valid for the map's life.
            (None, None) if unsafe { leaf.as_ref() }.chain_latch.is_dead() => Unlinked::Refused,
            (None, None) => Unlinked::Missing,
        }
    }

    /// Prunes the chain of `leaf` at `min_version`, as the heap tree's
    /// `prune_leaf` does: a dead key is unlinked whole, otherwise every
    /// version older than the newest one `<= min_version` is unlinked. The
    /// unlinked versions go on the retired list. Returns how many there were.
    pub(crate) fn prune_leaf<F: Fn(&V) -> bool>(
        &self,
        leaf_ptr: NonNull<VersionedLeaf<K, V>>,
        min_version: u64,
        is_tombstone: &F,
    ) -> usize {
        let arena = &**self.arena();
        // SAFETY: arena leaves are valid for the map's life.
        let leaf = unsafe { leaf_ptr.as_ref() };
        let find = |head: u32| find_le_off::<V>(arena, head, min_version);
        for _attempt in 0..64 {
            // 1. Without any latch, find T and evaluate the user closure on it.
            let head = leaf.head();
            let Some(t_off) = find(head) else {
                return 0;
            };
            let t = self.node(t_off);
            if t_off == head && t.value().is_none_or(is_tombstone) {
                // 2. A dead key: unlink it, if T is still its head.
                match self.unlink_leaf(leaf_ptr, |l| l.head() == t_off) {
                    Unlinked::Done(n) => return n,
                    Unlinked::Refused if leaf.chain_latch.is_dead() => return 0,
                    Unlinked::Refused => continue,
                    Unlinked::Missing => {}
                }
            }

            // 3. Re-check under the latch.
            let Some(w) = leaf.chain_latch.lock() else {
                return 0;
            };
            if find(leaf.head()) != Some(t_off) || t.is_superseded() {
                drop(w);
                continue;
            }

            // 4. Detach everything older than T. The detached nodes are
            // immutable from here on, so their links can be walked again
            // after unlocking.
            let bomb = AbortOnUnwind;
            let first = t.next();
            t.set_next(&w, 0);
            let mut count = 0;
            let mut cur = first;
            while cur != 0 {
                let n = self.node(cur);
                n.mark_superseded(&w);
                count += 1;
                cur = n.next();
            }
            bomb.defuse();
            drop(w);

            // 5. Retire every detached node, exactly once.
            let mut cur = first;
            while cur != 0 {
                let next = self.node(cur).next();
                self.retire_version(cur);
                cur = next;
            }
            return count;
        }
        0
    }

    /// Unlinks `key` and every version of it (§13). Returns `false` if the
    /// key had no version.
    pub(crate) fn remove_key(&self, key: &[u8]) -> bool {
        let mut backoff = SpinBackoff::new();
        loop {
            let Some(leaf) = self.raw.get(key) else {
                return false;
            };
            match self.unlink_leaf(leaf, |_| true) {
                Unlinked::Done(_) => return true,
                Unlinked::Missing => return false,
                Unlinked::Refused => backoff.spin(),
            }
        }
    }

    /// Unlinks exactly `version` of `key`; the key goes with its only
    /// version. Returns `false` if there was no such version.
    pub(crate) fn remove_version(&self, key: &[u8], version: u64) -> bool {
        let only = |l: &VersionedLeaf<K, V>| {
            let h = self.node(l.head());
            h.version == version && h.next() == 0
        };
        let mut backoff = SpinBackoff::new();
        loop {
            let Some(leaf_ptr) = self.raw.get(key) else {
                return false;
            };
            // SAFETY: arena leaves are valid for the map's life.
            let leaf = unsafe { leaf_ptr.as_ref() };
            if only(leaf) {
                match self.unlink_leaf(leaf_ptr, only) {
                    Unlinked::Done(_) => return true,
                    Unlinked::Missing => return false,
                    Unlinked::Refused if leaf.chain_latch.is_dead() => {
                        backoff.spin();
                        continue;
                    }
                    Unlinked::Refused => {}
                }
            }
            let Some(w) = leaf.chain_latch.lock() else {
                backoff.spin();
                continue;
            };
            // Positions are found under the latch.
            let head = leaf.head();
            let mut prev = 0u32;
            let mut cur = head;
            while cur != 0 && self.node(cur).version > version {
                prev = cur;
                cur = self.node(cur).next();
            }
            if cur == 0 || self.node(cur).version != version {
                return false;
            }
            let n = self.node(cur);
            if prev == 0 && n.next() == 0 {
                // It became the only version meanwhile.
                drop(w);
                continue;
            }
            let old_live = !self.node(head).is_tombstone();
            let bomb = AbortOnUnwind;
            if prev == 0 {
                leaf.set_head(&w, n.next());
            } else {
                self.node(prev).set_next(&w, n.next());
            }
            n.mark_superseded(&w);
            let new_live = !self.node(leaf.head()).is_tombstone();
            self.raw
                .len_add(isize::from(new_live) - isize::from(old_live));
            bomb.defuse();
            drop(w);
            self.retire_version(cur);
            return true;
        }
    }

    /// Removes every key (§9.8), killing each detached leaf's chain before
    /// counting its head. The leaves and their versions stay in the arena.
    pub(crate) fn clear(&self) {
        self.raw.clear_counting(&(), |l| {
            // SAFETY: arena leaves are valid for the map's life.
            let l = unsafe { l.as_ref() };
            let Some(w) = l.chain_latch.lock() else {
                debug_assert!(false, "only its node's latch holder kills a chain");
                return 0;
            };
            let live = !self.node(l.head()).is_tombstone();
            w.kill();
            isize::from(live)
        });
    }

    /// Checks the tree's invariants at quiescence; `len` counts the keys
    /// whose newest version is live.
    pub(crate) fn validate(&mut self) {
        let arena = Arc::clone(self.arena());
        self.raw.validate_counting(|l| {
            // SAFETY: exclusive access; a reachable leaf of this arena.
            let head = unsafe { l.as_ref() }.head();
            // SAFETY: as above.
            let h = unsafe { arena.ptr::<VersionNode<V>>(head).as_ref() };
            usize::from(!h.is_tombstone())
        });
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::arena::tree::FAST_PATH_HITS;

    fn hits() -> usize {
        FAST_PATH_HITS.with(|n| n.get())
    }

    /// §12.7: the versioned inserter's fast path hits for sequential new keys
    /// and every key is placed where a descent finds it.
    #[test]
    fn versioned_inserter_fast_path_hits_for_sequential_keys() {
        let t = ArenaVersionedTree::<Vec<u8>, u64>::new(Arena::with_capacity(8 << 20));
        let mut cache = InserterCache::new();
        let before = hits();
        for i in 0..200u8 {
            t.insert(vec![b'k', i], 1, Some(i as u64), Some(&mut cache))
                .ok()
                .unwrap();
        }
        let hit = hits() - before;
        assert!(
            hit >= 190,
            "only {hit} of 200 sequential inserts used the fast path"
        );
        for i in 0..200u8 {
            assert!(t.raw.get(&[b'k', i]).is_some());
        }
    }
}
