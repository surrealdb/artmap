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
//! on a retired list that `Drop` drains.

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::ptr::NonNull;
use std::sync::Arc;

use crate::arena::node::{VersionNode, VersionedLeaf};
use crate::arena::storage::{ArenaLeaf, ArenaStorage};
use crate::arena::tree::InserterCache;
use crate::arena::Arena;
use crate::key::AsBytes;
use crate::latch::AbortOnUnwind;
use crate::raw::{Mode, Outcome, RawTree, Unpublished};
use crate::sync::atomic::{AtomicU32, Ordering};

pub(crate) type Storage<K, V> = ArenaStorage<VersionedLeaf<K, V>>;

/// The leaf and the offset of the head version `remove_head` replaced, or
/// `Err(())` if the tombstone did not fit.
pub(crate) type RemovedHead<K, V> = Result<Option<(NonNull<VersionedLeaf<K, V>>, u32)>, ()>;

/// The concurrent tree of an `ArenaVersionedArtMap`.
pub(crate) struct ArenaVersionedTree<K, V> {
    pub(crate) raw: RawTree<Storage<K, V>>,
    /// Version nodes unlinked while the map is alive (same-version replaces,
    /// deprecated removes), dropped in `Drop`.
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
            Err((_, n)) => Err(n.value),
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
        // Fast path: the key exists. The probe on the caller's key is a hint.
        if let Some(leaf) = self.raw.get(key.as_bytes()) {
            return match self.alloc_version(version, value) {
                Ok(node) => Ok(self.apply(leaf, node)),
                Err(v) => Err((key, v)),
            };
        }
        let count = isize::from(value.is_some());
        let node = match self.alloc_version(version, value) {
            Ok(node) => node,
            Err(v) => return Err((key, v)),
        };
        let leaf = match self.raw.storage.alloc_value(VersionedLeaf::new(key, node)) {
            Ok(l) => l,
            Err((_, l)) => {
                // SAFETY: the node was never published: take its value back.
                let n = unsafe { self.arena().ptr::<VersionNode<V>>(node).read() };
                return Err((l.key, n.value));
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
                Ok(count)
            }
            Ok(Outcome::Existing(existing)) => {
                // Lost the race: move our (unpublished) version node to the
                // live leaf; `owner` then drops only the husk's key.
                // SAFETY: never published and exclusively ours.
                unsafe { leaf.as_ref().detach_head_unpublished() };
                Ok(self.apply(existing, node))
            }
            Ok(Outcome::Replaced(_)) => unreachable!("InsertIfAbsent never replaces"),
            Err(_) => {
                owner.disarm();
                // SAFETY: neither the leaf nor its version was published; take
                // the key and value back out and abandon their bytes.
                let (l, n) =
                    unsafe { (leaf.read(), self.arena().ptr::<VersionNode<V>>(node).read()) };
                Err((l.key, n.value))
            }
        }
    }

    /// Links the unpublished version node `node` into a published leaf.
    fn apply(&self, leaf: NonNull<VersionedLeaf<K, V>>, node: u32) -> isize {
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
        let Some(w) = leaf.chain_latch.lock() else {
            unreachable!("arena chain latches are never obsoleted")
        };
        // Positions are found under the latch.
        let head = leaf.head();
        let old_live = at(head).is_some_and(|h| !h.is_tombstone());
        let mut prev = 0u32;
        let mut cur = head;
        while let Some(n) = at(cur).filter(|n| n.version > version) {
            prev = cur;
            cur = n.next();
        }
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
        delta
    }

    /// The deprecated unversioned remove (§11.5): a same-version tombstone in
    /// place of a live head. Returns the leaf and the replaced head.
    pub(crate) fn remove_head(&self, key: &[u8]) -> RemovedHead<K, V> {
        let Some(leaf_ptr) = self.raw.get(key) else {
            return Ok(None);
        };
        // Allocated before the latch; its version is set before publication.
        let tomb = self.alloc_version(0, None).map_err(|_| ())?;
        let arena = &**self.arena();
        // SAFETY: arena leaves are valid for the map's life.
        let leaf = unsafe { leaf_ptr.as_ref() };
        let Some(w) = leaf.chain_latch.lock() else {
            unreachable!("arena chain latches are never obsoleted")
        };
        let head = leaf.head();
        // SAFETY: a version node of this arena.
        let h = unsafe { arena.ptr::<VersionNode<V>>(head).as_ref() };
        if h.is_tombstone() {
            // The unused tombstone is abandoned in place; it owns nothing.
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
        Ok(Some((leaf_ptr, head)))
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
