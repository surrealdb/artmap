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

//! # The arena tree behind [`ArenaArtMap`](crate::ArenaArtMap)

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::ptr::NonNull;
use std::sync::Arc;

use crate::arena::node::Leaf;
use crate::arena::storage::{ArenaLeaf, ArenaStorage};
use crate::arena::{Arena, ArenaFull};
use crate::key::AsBytes;
use crate::raw::slot::TaggedOffset;
use crate::raw::write::Hint;
use crate::raw::{Mode, Outcome, RawTree, Unpublished};

pub(crate) type Storage<K, V> = ArenaStorage<Leaf<K, V>>;

/// The displaced leaf of an insert, or the key and value back if it did not fit.
pub(crate) type InsertResult<K, V> = Result<Option<NonNull<Leaf<K, V>>>, ArenaFull<K, V>>;

/// The cached insertion point of a map-bound inserter (§12.4).
#[derive(Clone)]
pub(crate) struct InserterCache {
    hint: Option<Hint<TaggedOffset>>,
    /// The key bytes before the cached node's children (its absolute path).
    path: [u8; 32],
}

impl InserterCache {
    pub(crate) const fn new() -> Self {
        Self {
            hint: None,
            path: [0; 32],
        }
    }

    /// `matches()`: the key lies under the cached node.
    #[inline]
    fn matching(&self, key: &[u8]) -> Option<Hint<TaggedOffset>> {
        let h = self.hint?;
        (h.depth <= self.path.len()
            && key.len() > h.depth
            && key[..h.depth] == self.path[..h.depth])
            .then_some(h)
    }

    /// Caches where the last full insert landed.
    #[inline]
    pub(crate) fn remember(&mut self, hint: Option<Hint<TaggedOffset>>, key: &[u8]) {
        match hint {
            Some(h) if h.depth <= self.path.len() && key.len() >= h.depth => {
                self.path[..h.depth].copy_from_slice(&key[..h.depth]);
                self.hint = Some(h);
            }
            _ => self.hint = None,
        }
    }

    /// The fast path: insert under the cached node if `key` falls under it
    /// and the node is unchanged since this inserter's own unlock. `None`
    /// means "take the normal path".
    pub(crate) fn try_fast<L: ArenaLeaf + crate::raw::LeafNode>(
        &mut self,
        raw: &RawTree<ArenaStorage<L>>,
        owner: &Unpublished<'_, ArenaStorage<L>>,
        key: &[u8],
        count: isize,
    ) -> Option<Outcome<L>> {
        let mut h = self.matching(key)?;
        // SAFETY: arena nodes are never freed while the tree is alive.
        let r = unsafe { raw.insert_at_hint(&mut h, owner, key, count) };
        #[cfg(test)]
        FAST_PATH_HITS.with(|n| n.set(n.get() + usize::from(r.is_some())));
        self.hint = Some(h);
        r
    }
}

#[cfg(test)]
std::thread_local! {
    /// Fast-path hits on this thread, for the hit-rate test (§12.4).
    pub(crate) static FAST_PATH_HITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The concurrent tree of an `ArenaArtMap`.
pub(crate) struct ArenaTree<K, V> {
    pub(crate) raw: RawTree<Storage<K, V>>,
}

impl<K, V> ArenaTree<K, V> {
    pub(crate) fn new(arena: Arc<Arena>) -> Self {
        Self {
            raw: RawTree::new_in(ArenaStorage::new(arena)),
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
}

impl<K, V> Drop for ArenaTree<K, V> {
    fn drop(&mut self) {
        if Leaf::<K, V>::NEEDS_DROP {
            // SAFETY: `&mut self` in `drop`: no borrow of the map exists, so
            // nothing can reach the leaves (Inv 2). Retired leaves are dropped
            // by the storage afterwards.
            unsafe { self.raw.destroy() }
        }
    }
}

impl<K: AsBytes, V> ArenaTree<K, V> {
    /// Inserts or replaces. On `ArenaFull` nothing is published, no latch is
    /// held, and the key and value are returned. Returns the replaced leaf,
    /// which stays readable for the map's life.
    pub(crate) fn insert(
        &self,
        key: K,
        value: V,
        cache: Option<&mut InserterCache>,
    ) -> InsertResult<K, V> {
        let leaf = match self.raw.storage.alloc_value(Leaf::new(key, value)) {
            Ok(l) => l,
            Err((_, l)) => {
                return Err(ArenaFull {
                    key: l.key,
                    value: l.value,
                })
            }
        };
        // SAFETY: freshly written and exclusively ours.
        let owner = unsafe { Unpublished::new(&self.raw.storage, leaf) };
        // SAFETY: `owner` keeps the leaf alive for the call; the key bytes are
        // derived once, from the leaf's final location (Inv 10).
        let key_bytes = unsafe { leaf.as_ref() }.key.as_bytes();
        let mut cache = cache;
        if let Some(c) = cache.as_deref_mut() {
            if c.try_fast(&self.raw, &owner, key_bytes, 1).is_some() {
                return Ok(None);
            }
        }
        let mut hint = None;
        match self
            .raw
            .insert_hinted(&owner, key_bytes, Mode::Replace, 1, &(), &mut hint)
        {
            Ok(Outcome::Inserted(_)) => {
                if let Some(c) = cache {
                    c.remember(hint, key_bytes);
                }
                Ok(None)
            }
            Ok(Outcome::Replaced(old)) => Ok(Some(old)),
            Ok(Outcome::Existing(_)) => unreachable!("Mode::Replace always publishes"),
            Err(_) => {
                // Nothing was published: take the key and value back out and
                // abandon the leaf's bytes.
                owner.disarm();
                // SAFETY: never published, exclusively ours; read once.
                let l = unsafe { leaf.read() };
                Err(ArenaFull {
                    key: l.key,
                    value: l.value,
                })
            }
        }
    }

    /// Removes the leaf for `key`: physically unlinked under the latch, then
    /// put on the retired list (it stays readable for the map's life).
    pub(crate) fn remove(&self, key: &[u8]) -> Option<NonNull<Leaf<K, V>>> {
        self.raw.remove(
            key,
            // SAFETY: arena leaves stay valid for the map's life.
            |l| unsafe { l.as_ref() }.key.as_bytes() == key,
            &(),
        )
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    fn hits() -> usize {
        FAST_PATH_HITS.with(|n| n.get())
    }

    #[test]
    fn inserter_fast_path_hits_for_sequential_keys() {
        let t = ArenaTree::<Vec<u8>, u64>::new(Arena::with_capacity(8 << 20));
        let mut cache = InserterCache::new();
        let before = hits();
        // 200 keys under one parent: the parent grows 4 -> 16 -> 48 -> 256,
        // and every other insert lands in the cached node.
        for i in 0..200u8 {
            t.insert(vec![b'k', i], i as u64, Some(&mut cache))
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

    #[test]
    fn inserter_fast_path_hits_for_interleaved_runs() {
        let t = ArenaTree::<Vec<u8>, u64>::new(Arena::with_capacity(8 << 20));
        let mut cache = InserterCache::new();
        // Pre-grow two parents so they do not grow during the runs.
        for p in *b"ab" {
            for i in 0..=255u8 {
                t.insert(vec![p, i, 0], 0, None).ok().unwrap();
            }
        }
        let before = hits();
        // Runs of 16 keys alternating between the two parents' children.
        for run in 0..16u8 {
            let p = if run % 2 == 0 { b'a' } else { b'b' };
            for i in 0..16u8 {
                t.insert(vec![p, run, 1 + i], 1, Some(&mut cache))
                    .ok()
                    .unwrap();
            }
        }
        let hit = hits() - before;
        assert!(
            hit >= 16 * 12,
            "only {hit} of 256 run inserts used the fast path"
        );
    }

    #[test]
    fn stale_cache_fails_safely() {
        let t = ArenaTree::<Vec<u8>, u64>::new(Arena::with_capacity(8 << 20));
        let mut cache = InserterCache::new();
        t.insert(b"ka".to_vec(), 1, Some(&mut cache)).ok().unwrap();
        t.insert(b"kb".to_vec(), 2, Some(&mut cache)).ok().unwrap();
        // Another writer changes the cached node (and splits its prefix).
        t.insert(b"kc".to_vec(), 3, None).ok().unwrap();
        t.insert(b"x".to_vec(), 4, None).ok().unwrap();
        t.insert(b"kd".to_vec(), 5, Some(&mut cache)).ok().unwrap();
        for k in [&b"ka"[..], b"kb", b"kc", b"kd", b"x"] {
            assert!(t.raw.get(k).is_some(), "{k:?} lost");
        }
        assert_eq!(t.len(), 5);
    }
}
