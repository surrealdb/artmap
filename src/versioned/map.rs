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

//! # Concurrent multi-version adaptive radix tree

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::borrow::Borrow;
use std::ops::{Bound, RangeBounds};

use crate::guard::{pin, Guard, GuardHandle};
use crate::key::AsBytes;
use crate::raw::cursor::{owned_bound, Cursor};
use crate::versioned::entry::VersionedEntryRef;
use crate::versioned::iter::Range;
use crate::versioned::tree::{chain, find_le, VersionedTree};

/// A concurrent ordered map with 64-bit MVCC versions per key.
///
/// Each key has a chain of versions, newest first; a version may be a
/// tombstone. Snapshot reads (`get_version_le`) take no latches. Writers of one
/// key serialise on a per-key latch; writers of different keys run in parallel.
///
/// ## Semantics
///
/// - `len()` is the number of keys whose newest version is live (not a
///   tombstone). It is exact when no operation is in flight.
/// - `get_latest`/`get` return `None` when the newest version is a tombstone;
///   `get_version_le` returns `None` when the selected version is one.
///   Older snapshots are unaffected by later deletes.
/// - `version_count` and `get_all_versions` include tombstones.
/// - Scans (`iter`, `range`, `scan`) are latest-view: keys whose newest version
///   is a tombstone are skipped.
/// - After `prune_key(k, min, ..)` or `prune_all(min, ..)`, `get_version_le(k, v)`
///   is exact for `v >= min` and unsupported for `v < min` (the watermark
///   contract).
/// - Deleted keys keep their leaf and one tombstone until pruned; leaves are
///   never unlinked in this release.
pub struct VersionedArtMap<K, V> {
    tree: VersionedTree<K, V>,
}

impl<K, V> Default for VersionedArtMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> VersionedArtMap<K, V> {
    crate::sync::const_fn_unless_loom! {
        /// Creates an empty map.
        pub fn new() -> Self {
            Self {
                tree: VersionedTree::new(),
            }
        }
    }

    /// Creates an empty map. The capacity is a hint and currently unused.
    pub fn with_capacity(_capacity: usize) -> Self {
        Self::new()
    }

    /// The number of keys whose newest version is live.
    #[inline]
    pub fn len(&self) -> usize {
        self.tree.len()
    }

    /// `true` if no key has a live newest version.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<K: AsBytes + Send + 'static, V: Send + 'static> VersionedArtMap<K, V> {
    /// Pins the epoch, for the `*_with_guard` methods.
    #[inline]
    pub fn pin(&self) -> Guard<'_> {
        Guard::new()
    }

    /// The newest live value of `key`.
    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
        V: Clone,
    {
        self.get_latest(key).map(|(_, v)| v)
    }

    /// The newest live value of a raw byte key.
    #[inline]
    pub fn get_by_slice(&self, key: &[u8]) -> Option<V>
    where
        V: Clone,
    {
        self.get_slice(key)
    }

    /// The newest live value of a raw byte key.
    #[inline]
    pub fn get_slice(&self, key: &[u8]) -> Option<V>
    where
        V: Clone,
    {
        let _g = pin();
        let leaf = self.tree.raw.get(key)?;
        // SAFETY: protected by `_g`.
        let head = unsafe { leaf.as_ref() }.head();
        head.value.clone()
    }

    /// The newest version and value of `key`, if live.
    #[inline]
    pub fn get_latest<Q>(&self, key: &Q) -> Option<(u64, V)>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
        V: Clone,
    {
        let _g = pin();
        let leaf = self.tree.raw.get(key.as_bytes())?;
        // SAFETY: protected by `_g`.
        let head = unsafe { leaf.as_ref() }.head();
        head.value.clone().map(|v| (head.version, v))
    }

    /// A handle on the newest live version of `key`, without cloning.
    pub fn get_entry<Q>(&self, key: &Q) -> Option<VersionedEntryRef<'_, K, V>>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        let guard = GuardHandle::owned();
        let leaf = self.tree.raw.get(key.as_bytes())?;
        // SAFETY: protected by `guard`, which moves into the handle.
        let head = unsafe { leaf.as_ref() }.head();
        VersionedEntryRef::new(leaf, head, &self.tree, guard)
    }

    /// The newest version of `key` that is `<= max_version`, unless it is a
    /// tombstone. The basis of MVCC snapshot reads.
    #[inline]
    pub fn get_version_le<Q>(&self, key: &Q, max_version: u64) -> Option<(u64, V)>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
        V: Clone,
    {
        let g = &pin();
        self.version_le(key.as_bytes(), max_version, g)
    }

    /// As [`get_version_le`](Self::get_version_le), with a pre-pinned guard.
    #[inline]
    pub fn get_version_le_with_guard<Q>(
        &self,
        key: &Q,
        max_version: u64,
        guard: &Guard<'_>,
    ) -> Option<(u64, V)>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
        V: Clone,
    {
        self.version_le(key.as_bytes(), max_version, &guard.inner)
    }

    fn version_le(&self, key: &[u8], max: u64, _g: &crossbeam_epoch::Guard) -> Option<(u64, V)>
    where
        V: Clone,
    {
        let leaf = self.tree.raw.get(key)?;
        // SAFETY: protected by `_g`.
        let n = find_le(unsafe { leaf.as_ref() }.head(), max)?;
        n.value.clone().map(|v| (n.version, v))
    }

    /// `true` if the newest version of `key` is live.
    #[inline]
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        let _g = pin();
        self.tree
            .raw
            .get(key.as_bytes())
            // SAFETY: protected by `_g`.
            .is_some_and(|l| !unsafe { l.as_ref() }.head().is_tombstone())
    }

    /// Adds `version` of `key`. An existing version with the same number is
    /// replaced (out of place). Always returns `true`.
    #[inline]
    pub fn insert(&self, key: K, version: u64, value: V) -> bool {
        let g = &pin();
        self.tree.insert(key, version, Some(value), g);
        true
    }

    /// As [`insert`](Self::insert), with a pre-pinned guard.
    #[inline]
    pub fn insert_with_guard(&self, key: K, version: u64, value: V, guard: &Guard<'_>) -> bool {
        self.tree.insert(key, version, Some(value), &guard.inner);
        true
    }

    /// Alias for [`insert`](Self::insert).
    #[inline]
    pub fn insert_versioned(&self, key: K, version: u64, value: V) -> bool {
        self.insert(key, version, value)
    }

    /// Records a tombstone at `version` for `key`, creating the key if needed.
    /// Older snapshots are unchanged. Returns `true` if the key's newest
    /// version was live before and is now deleted.
    pub fn delete(&self, key: K, version: u64) -> bool {
        let g = &pin();
        self.tree.insert(key, version, None, g) < 0
    }

    /// Deletes the newest version of `key` in place of its value: publishes a
    /// tombstone with the same version number, so snapshots at or after that
    /// version see the key as absent. Returns the replaced version.
    #[deprecated(note = "use `delete(key, version)`, which does not rewrite history")]
    pub fn remove<Q>(&self, key: &Q) -> Option<VersionedEntryRef<'_, K, V>>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        let guard = GuardHandle::owned();
        let (leaf, old) = self.tree.remove_head(key.as_bytes(), guard.guard())?;
        // SAFETY: `old` is retired through `guard`, which moves into the handle.
        VersionedEntryRef::new(leaf, unsafe { old.as_ref() }, &self.tree, guard)
    }

    /// The number of versions of `key`, including tombstones.
    pub fn version_count<Q>(&self, key: &Q) -> usize
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        let _g = pin();
        match self.tree.raw.get(key.as_bytes()) {
            // SAFETY: protected by `_g`.
            Some(l) => chain(unsafe { l.as_ref() }.head()).count(),
            None => 0,
        }
    }

    /// Every version of `key`, newest first; `None` values are tombstones.
    pub fn get_all_versions<Q>(&self, key: &Q) -> Vec<(u64, Option<V>)>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
        V: Clone,
    {
        let _g = pin();
        match self.tree.raw.get(key.as_bytes()) {
            // SAFETY: protected by `_g`.
            Some(l) => chain(unsafe { l.as_ref() }.head())
                .map(|n| (n.version, n.value.clone()))
                .collect(),
            None => Vec::new(),
        }
    }

    /// Unlinks every version of `key` older than its newest version
    /// `<= min_version`. If that version is the newest one and `is_tombstone`
    /// says its value is a tombstone, it is replaced by a built-in tombstone.
    /// Returns the number of versions unlinked. The key itself stays in the
    /// map (with a tombstone) in this release.
    ///
    /// `is_tombstone` runs without any latch held.
    pub fn prune_key<Q, F>(&self, key: &Q, min_version: u64, is_tombstone: F) -> usize
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
        F: Fn(&V) -> bool,
    {
        let g = &pin();
        match self.tree.raw.get(key.as_bytes()) {
            Some(leaf) => self.tree.prune_leaf(leaf, min_version, &is_tombstone, g),
            None => 0,
        }
    }

    /// [`prune_key`](Self::prune_key) for every key, including deleted ones.
    /// Repins periodically so a long prune does not stall reclamation.
    pub fn prune_all<F>(&self, min_version: u64, is_tombstone: F) -> usize
    where
        F: Fn(&V) -> bool,
    {
        let mut g = pin();
        let mut cursor = Cursor::new(Bound::Unbounded, Bound::Unbounded);
        let mut total = 0;
        let mut n = 0usize;
        while let Some(leaf) = cursor.next(&self.tree.raw) {
            total += self.tree.prune_leaf(leaf, min_version, &is_tombstone, &g);
            n += 1;
            if n % 1024 == 0 {
                // Frames hold node pointers protected by `g`: drop them first.
                cursor.invalidate(&self.tree.raw);
                g.repin();
            }
        }
        total
    }

    /// An iterator over the latest live version of each key in `range`.
    pub fn range<R, Q>(&self, range: R) -> Range<'_, K, V>
    where
        R: RangeBounds<Q>,
        Q: AsBytes + ?Sized,
    {
        Range::owned(
            &self.tree,
            owned_bound(range.start_bound()),
            owned_bound(range.end_bound()),
        )
    }

    /// As [`range`](Self::range), borrowing the caller's guard.
    pub fn range_with_guard<'a, R, Q>(&'a self, range: R, guard: &'a Guard<'_>) -> Range<'a, K, V>
    where
        R: RangeBounds<Q>,
        Q: AsBytes + ?Sized,
    {
        Range::borrowed(
            &self.tree,
            &guard.inner,
            owned_bound(range.start_bound()),
            owned_bound(range.end_bound()),
        )
    }

    /// An iterator over the latest live version of every key.
    pub fn iter(&self) -> Range<'_, K, V> {
        Range::owned(&self.tree, Bound::Unbounded, Bound::Unbounded)
    }

    /// As [`iter`](Self::iter), borrowing the caller's guard.
    pub fn iter_with_guard<'a>(&'a self, guard: &'a Guard<'_>) -> Range<'a, K, V> {
        Range::borrowed(&self.tree, &guard.inner, Bound::Unbounded, Bound::Unbounded)
    }

    /// Calls `callback(key, value, version)` for the latest live version of
    /// each key in `range`, until it returns `false`.
    pub fn scan<R, Q, F>(&self, range: R, mut callback: F)
    where
        R: RangeBounds<Q>,
        Q: AsBytes + ?Sized,
        F: FnMut(&K, &V, u64) -> bool,
    {
        for e in self.range(range) {
            if !callback(e.key(), e.value(), e.version()) {
                break;
            }
        }
    }
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> IntoIterator
    for &'a VersionedArtMap<K, V>
{
    type Item = VersionedEntryRef<'a, K, V>;
    type IntoIter = Range<'a, K, V>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
