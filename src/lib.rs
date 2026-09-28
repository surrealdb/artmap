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

//! # artmap: concurrent adaptive radix trees
//!
//! Concurrent, ordered, in-memory maps backed by an **Adaptive Radix Tree**
//! (ART) with **optimistic lock coupling** (OLC):
//!
//! - [`ArtMap`]: a key-value map with epoch-based reclamation (EBR).
//! - [`VersionedArtMap`]: a map with a chain of 64-bit MVCC versions per key.
//! - [`ArenaArtMap`] and [`ArenaVersionedArtMap`]: the same, allocated from a
//!   bump [`Arena`] and reclaimed only when the map is dropped.
//!
//! Lookups cost $O(k)$ in the key length. Readers never write to shared tree
//! memory and never block; writers take fine-grained per-node latches, so
//! writes to disjoint parts of the tree proceed in parallel.
//!
//! ## Epoch guards
//!
//! The EBR maps protect memory with the process-wide default
//! `crossbeam-epoch` collector. Handles returned by the maps
//! ([`EntryRef`], [`VersionedEntryRef`], iterators) keep an epoch guard alive,
//! and [`Guard`]s from `map.pin()` can be passed to the `*_with_guard`
//! methods to amortise pinning.
//!
//! - **Holding a guard stalls reclamation process-wide**, for every user of the
//!   default collector. That includes a handle, an iterator, or a [`Guard`]. Do
//!   not hold one across I/O or long computations.
//! - **Leaking a handle** with `mem::forget` pins the current thread for the
//!   rest of its life. This is memory-safe, but reclamation stops for every
//!   user of the default collector.
//! - **Async code.** Handles are `!Send`. Across an `.await`, use `get_value`
//!   or `with_value`, or copy out with [`EntryRef::to_owned`] or
//!   [`EntryRef::value_cloned`].
//! - **Deferred destructors** of removed or replaced keys and values run on an
//!   arbitrary thread, at an arbitrary later time, and may never run (for
//!   example at process exit). `Drop` of keys and values must not panic.
//! - Dropping a map is synchronous: it frees every live entry immediately.
//!
//! ## Keys
//!
//! Keys are ordered by the bytes returned by [`AsBytes`]. `AsBytes` is a safe
//! trait: implementations should be deterministic, agree with `Borrow`, and
//! not panic, but artmap never relies on that for memory safety. A
//! misbehaving implementation may cause wrong results or panics, never
//! undefined behaviour. `as_bytes` may be called more than once per
//! operation, concurrently, and on keys that were already removed.
//!
//! ## Consistency
//!
//! Point operations are linearizable. Iterators guarantee that every key
//! present for the whole scan is yielded exactly once, in order; keys inserted
//! or removed during the scan may or may not appear. `len()` is exact when no
//! operation is in flight; while several threads write, it is approximate.
//!
//! ## Public surface
//!
//! No safe API resolves an offset or pointer against an arena other than the
//! map's own, accepts a raw pointer or handle, or returns a reference that
//! outlives the map borrow (or, for the EBR maps, its guard).

#![cfg_attr(artmap_provenance_lints, feature(strict_provenance_lints))]
// Renamed to `implicit_provenance_casts` on newer nightlies; the old names forward.
#![cfg_attr(artmap_provenance_lints, allow(renamed_and_removed_lints))]
#![cfg_attr(
    artmap_provenance_lints,
    deny(fuzzy_provenance_casts, lossy_provenance_casts)
)]
#![deny(let_underscore_drop, clippy::let_underscore_must_use)]

pub mod arena;
#[cfg(doctest)]
pub mod compile_fail_tests;
/// The README's examples, run as doctests.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
pub struct ReadmeDoctests;
mod entry;
mod guard;
#[cfg(artmap_hooks)]
#[doc(hidden)]
pub mod hooks;
#[cfg(not(artmap_hooks))]
mod hooks;
mod iter;
mod key;
mod latch;
#[cfg(all(test, loom))]
mod loom_tests;
mod node;
mod raw;
mod simd;
mod sync;
mod tree;
pub mod versioned;

use std::borrow::Borrow;
use std::ops::{Bound, RangeBounds};

pub use arena::{Arena, ArenaArtMap, ArenaInserter, ArenaVersionedArtMap};
pub use entry::EntryRef;
pub use guard::Guard;
pub use iter::{GuardKeys, GuardValues, Iter, KeyRef, Keys, Range, ValueRef, Values};
pub use key::AsBytes;
pub use versioned::{VersionedArtMap, VersionedEntryRef};

use guard::{pin, GuardHandle};
use raw::cursor::owned_bound;
use raw::{Mode, Outcome};
use tree::Tree;

/// Positive `Send`/`Sync` assertions (§8.9).
const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ArtMap<String, u64>>();
    assert_send_sync::<VersionedArtMap<String, u64>>();
    assert_send_sync::<ArenaArtMap<String, u64>>();
    assert_send_sync::<ArenaVersionedArtMap<String, u64>>();
    assert_send_sync::<Arena>();
};

/// A concurrent ordered map backed by an adaptive radix tree.
///
/// Reads take no latches (they validate optimistically and retry) and are
/// linearizable; writes take per-node latches. Values are never mutated in
/// place: an overwrite publishes a new entry and returns the displaced one,
/// which stays readable through its handle.
pub struct ArtMap<K, V> {
    tree: Tree<K, V>,
}

impl<K, V> Default for ArtMap<K, V> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> ArtMap<K, V> {
    sync::const_fn_unless_loom! {
        /// Creates an empty map.
        #[inline]
        pub fn new() -> Self {
            Self { tree: Tree::new() }
        }
    }

    /// Creates an empty map. The capacity is a hint and currently unused.
    #[inline]
    pub fn with_capacity(_capacity: usize) -> Self {
        Self::new()
    }

    /// The number of entries. Exact when no operation is in flight. While
    /// writes on several threads (or a `clear()`) are in flight it is
    /// approximate: the count is striped by thread so that writers do not
    /// contend on one cache line.
    #[inline]
    pub fn len(&self) -> usize {
        self.tree.len()
    }

    /// `true` if the map has no entries.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<K: AsBytes + Send + 'static, V: Send + 'static> ArtMap<K, V> {
    /// Pins the epoch, for the `*_with_guard` methods. See the crate docs for
    /// what holding a guard implies.
    ///
    /// ```
    /// let map = artmap::ArtMap::<[u8; 8], u64>::new();
    /// map.insert(7u64.to_be_bytes(), 49);
    /// let guard = map.pin();
    /// assert_eq!(map.get_with_guard(&7u64.to_be_bytes(), &guard).as_deref(), Some(&49));
    /// ```
    #[inline]
    pub fn pin(&self) -> Guard<'_> {
        Guard::new()
    }

    /// A handle on the entry for `key`.
    ///
    /// ```
    /// let map = artmap::ArtMap::<String, u32>::new();
    /// map.insert("a".to_string(), 1);
    /// let e = map.get("a").unwrap();
    /// assert_eq!((e.key().as_str(), *e.value()), ("a", 1));
    /// assert!(map.get("b").is_none());
    /// ```
    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<EntryRef<'_, K, V>>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        self.get_by_slice(key.as_bytes())
    }

    /// A handle on the entry for a raw byte key.
    #[inline]
    pub fn get_by_slice(&self, key: &[u8]) -> Option<EntryRef<'_, K, V>> {
        let guard = GuardHandle::owned();
        let leaf = self.tree.raw.get(key)?;
        Some(EntryRef::new(leaf, &self.tree, guard))
    }

    /// A handle on the entry for `key`, borrowing the caller's guard instead of
    /// pinning.
    #[inline]
    pub fn get_with_guard<'a, Q>(
        &'a self,
        key: &Q,
        guard: &'a Guard<'_>,
    ) -> Option<EntryRef<'a, K, V>>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        let leaf = self.tree.raw.get(key.as_bytes())?;
        Some(EntryRef::new(
            leaf,
            &self.tree,
            GuardHandle::Borrowed(&guard.inner),
        ))
    }

    /// A clone of the value for `key`.
    #[inline]
    pub fn get_value<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
        V: Clone,
    {
        self.with_value(key, V::clone)
    }

    /// Calls `f` with the value for `key`, without cloning.
    #[inline]
    pub fn with_value<Q, R, F>(&self, key: &Q, f: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
        F: FnOnce(&V) -> R,
    {
        let _g = pin();
        let leaf = self.tree.raw.get(key.as_bytes())?;
        // SAFETY: protected by `_g` for the duration of `f`; `R` cannot borrow
        // from `&V` (it is chosen by the caller before the borrow exists).
        Some(f(&unsafe { leaf.as_ref() }.value))
    }

    /// `true` if the map has an entry for `key`.
    #[inline]
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        self.contains_key_slice(key.as_bytes())
    }

    /// `true` if the map has an entry for a raw byte key.
    #[inline]
    pub fn contains_key_slice(&self, key: &[u8]) -> bool {
        let _g = pin();
        self.tree.raw.get(key).is_some()
    }

    /// Inserts or replaces. Returns the displaced entry (already unlinked and
    /// marked removed), or `None` if the key was absent.
    ///
    /// Dropping the returned handle immediately costs one unpin.
    ///
    /// ```
    /// let map = artmap::ArtMap::<String, u32>::new();
    /// assert!(map.insert("a".to_string(), 1).is_none());
    /// let old = map.insert("a".to_string(), 2).unwrap();
    /// assert_eq!(*old, 1);
    /// assert!(old.is_removed());
    /// assert_eq!(map.get_value("a"), Some(2));
    /// ```
    #[inline]
    pub fn insert(&self, key: K, value: V) -> Option<EntryRef<'_, K, V>> {
        let guard = GuardHandle::owned();
        self.insert_in(key, value, guard)
    }

    /// As [`insert`](Self::insert), borrowing the caller's guard.
    #[inline]
    pub fn insert_with_guard<'a>(
        &'a self,
        key: K,
        value: V,
        guard: &'a Guard<'_>,
    ) -> Option<EntryRef<'a, K, V>> {
        self.insert_in(key, value, GuardHandle::Borrowed(&guard.inner))
    }

    /// As [`insert`](Self::insert), returning a clone of the displaced value
    /// (cloned after every latch is released).
    #[inline]
    pub fn insert_cloned(&self, key: K, value: V) -> Option<V>
    where
        V: Clone,
    {
        self.insert(key, value).map(|old| old.value().clone())
    }

    #[inline]
    fn insert_in<'a>(
        &'a self,
        key: K,
        value: V,
        guard: GuardHandle<'a>,
    ) -> Option<EntryRef<'a, K, V>> {
        match self.tree.insert(key, value, Mode::Replace, guard.guard()) {
            Outcome::Inserted(_) => None,
            Outcome::Replaced(old) => Some(EntryRef::new(old, &self.tree, guard)),
            Outcome::Existing(_) => unreachable!("Mode::Replace always publishes"),
        }
    }

    /// Returns the entry for `key`, inserting `f()` first if it is absent.
    ///
    /// `f` runs without any latch held, and only if the key looked absent.
    ///
    /// ```
    /// let map = artmap::ArtMap::<String, u32>::new();
    /// assert_eq!(*map.get_or_insert_with("a".to_string(), || 1), 1);
    /// assert_eq!(*map.get_or_insert_with("a".to_string(), || 2), 1);
    /// ```
    pub fn get_or_insert_with<F>(&self, key: K, f: F) -> EntryRef<'_, K, V>
    where
        F: FnOnce() -> V,
    {
        let guard = GuardHandle::owned();
        // A hint only (Inv 10): the install below decides.
        if let Some(leaf) = self.tree.raw.get(key.as_bytes()) {
            return EntryRef::new(leaf, &self.tree, guard);
        }
        let value = f();
        match self
            .tree
            .insert(key, value, Mode::InsertIfAbsent, guard.guard())
        {
            Outcome::Inserted(leaf) => EntryRef::new(leaf, &self.tree, guard),
            Outcome::Existing(existing) => EntryRef::new(existing, &self.tree, guard),
            Outcome::Replaced(_) => unreachable!("InsertIfAbsent never replaces"),
        }
    }

    /// Removes the entry for `key`. Returns the removed entry (unlinked and
    /// marked removed).
    ///
    /// ```
    /// let map = artmap::ArtMap::<String, u32>::new();
    /// map.insert("a".to_string(), 1);
    /// assert_eq!(map.remove("a").as_deref(), Some(&1));
    /// assert!(map.remove("a").is_none());
    /// assert!(map.is_empty());
    /// ```
    #[inline]
    pub fn remove<Q>(&self, key: &Q) -> Option<EntryRef<'_, K, V>>
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        self.remove_by_slice(key.as_bytes())
    }

    /// Removes the entry for a raw byte key.
    #[inline]
    pub fn remove_by_slice(&self, key: &[u8]) -> Option<EntryRef<'_, K, V>> {
        let guard = GuardHandle::owned();
        let leaf = self.tree.remove(key, guard.guard())?;
        Some(EntryRef::new(leaf, &self.tree, guard))
    }

    /// An iterator over the entries in `range`, in key order.
    ///
    /// ```
    /// let map = artmap::ArtMap::<String, u32>::new();
    /// for (i, k) in ["a", "b", "c", "d"].into_iter().enumerate() {
    ///     map.insert(k.to_string(), i as u32);
    /// }
    /// let keys: Vec<String> = map.range("b".."d").map(|e| e.key().clone()).collect();
    /// assert_eq!(keys, ["b", "c"]);
    /// let back: Vec<u32> = map.range("b"..).rev().map(|e| *e).collect();
    /// assert_eq!(back, [3, 2, 1]);
    /// ```
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

    /// An iterator over every entry, in key order.
    pub fn iter(&self) -> Iter<'_, K, V> {
        Range::owned(&self.tree, Bound::Unbounded, Bound::Unbounded)
    }

    /// As [`iter`](Self::iter), borrowing the caller's guard.
    pub fn iter_with_guard<'a>(&'a self, guard: &'a Guard<'_>) -> Iter<'a, K, V> {
        Range::borrowed(&self.tree, &guard.inner, Bound::Unbounded, Bound::Unbounded)
    }

    /// An iterator over every key, in order.
    pub fn keys(&self) -> Keys<'_, K, V> {
        Keys(self.iter())
    }

    /// An iterator over every value, in key order.
    pub fn values(&self) -> Values<'_, K, V> {
        Values(self.iter())
    }

    /// Keys as bare references, bounded by the map and the caller's guard.
    pub fn keys_with_guard<'a>(&'a self, guard: &'a Guard<'_>) -> GuardKeys<'a, K, V> {
        GuardKeys(self.iter_with_guard(guard))
    }

    /// Values as bare references, bounded by the map and the caller's guard.
    pub fn values_with_guard<'a>(&'a self, guard: &'a Guard<'_>) -> GuardValues<'a, K, V> {
        GuardValues(self.iter_with_guard(guard))
    }

    /// Removes every entry. Linearizable with respect to point operations:
    /// the linearization point is the swap of the root. Runs in O(n) on the
    /// calling thread, holding at most one node latch at a time.
    pub fn clear(&self) {
        let g = pin();
        self.tree.raw.clear(&g);
    }

    /// Checks the tree's structural invariants and that `len()` equals the
    /// number of reachable entries. Panics on a violation. Requires exclusive
    /// access, so it never races with writers.
    pub fn validate_invariants(&mut self) {
        self.tree.raw.validate();
    }
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> IntoIterator for &'a ArtMap<K, V> {
    type Item = EntryRef<'a, K, V>;
    type IntoIter = Iter<'a, K, V>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn basic_flow() {
        let mut map = ArtMap::<String, i32>::new();
        assert!(map.is_empty());
        assert!(map.insert("users:100".into(), 1).is_none());
        assert!(map.insert("users:200".into(), 2).is_none());
        assert!(map.insert("users:300".into(), 3).is_none());
        assert_eq!(map.len(), 3);
        assert_eq!(map.get("users:100").as_deref(), Some(&1));
        assert_eq!(map.get_by_slice(b"users:200").as_deref(), Some(&2));
        assert!(map.contains_key("users:300"));
        assert!(!map.contains_key("users:400"));
        assert_eq!(map.with_value("users:100", |v| *v * 2), Some(2));
        let old = map.insert("users:100".into(), 10).expect("replaced");
        assert_eq!(*old, 1);
        assert!(old.is_removed());
        drop(old);
        assert_eq!(map.get("users:100").as_deref(), Some(&10));
        assert_eq!(map.remove("users:200").as_deref(), Some(&2));
        assert!(map.get("users:200").is_none());
        assert_eq!(map.len(), 2);
        map.validate_invariants();
    }

    #[test]
    fn range_scan() {
        let map = ArtMap::<String, i32>::new();
        for i in 1..=5 {
            map.insert(format!("k:{i}"), i);
        }
        let items: Vec<_> = map.range("k:2".."k:5").map(|e| *e.value()).collect();
        assert_eq!(items, vec![2, 3, 4]);
        let rev: Vec<_> = map.range("k:2".."k:5").rev().map(|e| *e.value()).collect();
        assert_eq!(rev, vec![4, 3, 2]);
    }

    #[test]
    fn get_or_insert_with() {
        let map = ArtMap::<String, i32>::new();
        {
            let e = map.get_or_insert_with("key".into(), || 42);
            assert_eq!(*e, 42);
            assert_eq!(e.key(), "key");
            assert!(!e.is_removed());
            assert!(e.remove());
            assert!(e.is_removed());
            assert!(!e.remove());
        }
        assert!(map.get("key").is_none());
        let e = map.get_or_insert_with("k2".into(), || 1);
        let e2 = map.get_or_insert_with("k2".into(), || panic!("must not run"));
        assert_eq!(*e, 1);
        assert_eq!(*e2, 1);
    }

    #[test]
    fn clear_is_exact() {
        let mut map = ArtMap::<[u8; 8], u64>::new();
        for i in 0..1000u64 {
            map.insert(i.to_be_bytes(), i);
        }
        map.clear();
        assert_eq!(map.len(), 0);
        assert!(map.iter().next().is_none());
        map.insert(1u64.to_be_bytes(), 1);
        assert_eq!(map.len(), 1);
        map.validate_invariants();
    }
}
