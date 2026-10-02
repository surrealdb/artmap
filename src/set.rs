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

//! # The ordered set
//!
//! [`ArtSet`] is [`ArtMap<K, ()>`](crate::ArtMap) with a set-shaped API. It
//! adds no unsafe code and no new tree machinery.

use std::borrow::Borrow;
use std::ops::RangeBounds;

use crate::{ArtMap, AsBytes, Keys};

/// A concurrent ordered set backed by an adaptive radix tree.
///
/// A thin wrapper over [`ArtMap<K, ()>`](ArtMap): the unit value is zero-sized,
/// so a set costs no more memory than the map's keys. It differs from the map
/// in two ways:
///
/// - [`insert`](Self::insert) is insert-if-absent and returns a `bool`. A key
///   that is already present is left alone, where `ArtMap::insert` would
///   publish a new leaf and retire the old one.
/// - Point operations return `bool` instead of handles, so they never hand out
///   an epoch guard. Iterators yield [`KeyRef`](crate::KeyRef)s, which keep
///   one alive.
///
/// Keys are ordered by their [`AsBytes`] bytes. Everything in the crate docs
/// about epoch guards, consistency and `Drop` applies here too.
///
/// ```
/// use artmap::ArtSet;
///
/// let set = ArtSet::<String>::new();
/// assert!(set.insert("b".to_string()));
/// assert!(set.insert("a".to_string()));
/// assert!(!set.insert("a".to_string()));
///
/// assert!(set.contains("a"));
/// let keys: Vec<String> = set.iter().map(|k| k.clone()).collect();
/// assert_eq!(keys, ["a", "b"]);
///
/// assert!(set.remove("a"));
/// assert!(!set.remove("a"));
/// assert_eq!(set.len(), 1);
/// ```
pub struct ArtSet<K> {
    map: ArtMap<K, ()>,
}

impl<K> Default for ArtSet<K> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<K> ArtSet<K> {
    crate::sync::const_fn_unless_loom! {
        /// Creates an empty set.
        #[inline]
        pub fn new() -> Self {
            Self { map: ArtMap::new() }
        }
    }

    /// Creates an empty set. The capacity is a hint and currently unused.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            map: ArtMap::with_capacity(capacity),
        }
    }

    /// The number of keys. Exact when no operation is in flight, approximate
    /// while several threads write. See [`ArtMap::len`].
    #[inline]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// `true` if the set has no keys.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl<K: AsBytes + Send + 'static> ArtSet<K> {
    /// `true` if the set contains `key`.
    ///
    /// ```
    /// let set = artmap::ArtSet::<String>::new();
    /// set.insert("a".to_string());
    /// assert!(set.contains("a"));
    /// assert!(!set.contains("b"));
    /// ```
    #[inline]
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        self.map.contains_key(key)
    }

    /// `true` if the set contains a raw byte key.
    #[inline]
    pub fn contains_slice(&self, key: &[u8]) -> bool {
        self.map.contains_key_slice(key)
    }

    /// Adds `key` to the set. Returns `true` if it was absent, `false` if the
    /// set already contained it, in which case the set is unchanged and `key`
    /// is dropped.
    ///
    /// ```
    /// let set = artmap::ArtSet::<String>::new();
    /// assert!(set.insert("a".to_string()));
    /// assert!(!set.insert("a".to_string()));
    /// assert_eq!(set.len(), 1);
    /// ```
    #[inline]
    pub fn insert(&self, key: K) -> bool {
        self.map.insert_if_absent(key, ())
    }

    /// Removes `key` from the set. Returns `true` if it was present.
    ///
    /// ```
    /// let set = artmap::ArtSet::<String>::new();
    /// set.insert("a".to_string());
    /// assert!(set.remove("a"));
    /// assert!(!set.remove("a"));
    /// assert!(set.is_empty());
    /// ```
    #[inline]
    pub fn remove<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: AsBytes + ?Sized,
    {
        self.map.remove(key).is_some()
    }

    /// Removes a raw byte key from the set. Returns `true` if it was present.
    #[inline]
    pub fn remove_by_slice(&self, key: &[u8]) -> bool {
        self.map.remove_by_slice(key).is_some()
    }

    /// An iterator over the keys in `range`, in key order.
    ///
    /// ```
    /// let set = artmap::ArtSet::<String>::new();
    /// for k in ["a", "b", "c", "d"] {
    ///     set.insert(k.to_string());
    /// }
    /// let keys: Vec<String> = set.range("b".."d").map(|k| k.clone()).collect();
    /// assert_eq!(keys, ["b", "c"]);
    /// let back: Vec<String> = set.range("b"..).rev().map(|k| k.clone()).collect();
    /// assert_eq!(back, ["d", "c", "b"]);
    /// ```
    #[inline]
    pub fn range<R, Q>(&self, range: R) -> Keys<'_, K, ()>
    where
        R: RangeBounds<Q>,
        Q: AsBytes + ?Sized,
    {
        Keys(self.map.range(range))
    }

    /// Calls `callback(key)` for each key in `range`, in key order, until it
    /// returns `false`.
    ///
    /// One pin covers the whole scan and no per-key handle is created, so this
    /// is the cheapest way to visit a range.
    ///
    /// ```
    /// let set = artmap::ArtSet::<String>::new();
    /// for k in ["a", "b", "c", "d"] {
    ///     set.insert(k.to_string());
    /// }
    /// let mut seen = Vec::new();
    /// set.scan("b".."d", |k| {
    ///     seen.push(k.clone());
    ///     true
    /// });
    /// assert_eq!(seen, ["b", "c"]);
    /// ```
    #[inline]
    pub fn scan<R, Q, F>(&self, range: R, mut callback: F)
    where
        R: RangeBounds<Q>,
        Q: AsBytes + ?Sized,
        F: FnMut(&K) -> bool,
    {
        self.map.scan(range, |k, _| callback(k));
    }

    /// An iterator over every key, in order.
    #[inline]
    pub fn iter(&self) -> Keys<'_, K, ()> {
        self.map.keys()
    }

    /// Removes every key. Linearizable with respect to point operations. See
    /// [`ArtMap::clear`].
    #[inline]
    pub fn clear(&self) {
        self.map.clear();
    }

    /// Fits the tree to its keys, releasing the memory that removes leave in
    /// nodes too large for what they still hold. See
    /// [`ArtMap::shrink_to_fit`].
    ///
    /// ```
    /// let set = artmap::ArtSet::<[u8; 2]>::new();
    /// for i in 0..=255u8 {
    ///     set.insert([0, i]);
    /// }
    /// for i in 1..=255u8 {
    ///     set.remove(&[0, i]);
    /// }
    /// set.shrink_to_fit();
    /// assert!(set.contains(&[0, 0]));
    /// assert_eq!(set.len(), 1);
    /// ```
    #[inline]
    pub fn shrink_to_fit(&self) {
        self.map.shrink_to_fit();
    }

    /// Checks the tree's structural invariants and that `len()` equals the
    /// number of reachable keys. Panics on a violation. Requires exclusive
    /// access, so it never races with writers.
    pub fn validate_invariants(&mut self) {
        self.map.validate_invariants();
    }
}

impl<'a, K: AsBytes + Send + 'static> IntoIterator for &'a ArtSet<K> {
    type Item = crate::KeyRef<'a, K, ()>;
    type IntoIter = Keys<'a, K, ()>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;

    #[test]
    fn basic_flow() {
        let mut set = ArtSet::<String>::new();
        assert!(set.is_empty());
        assert!(set.insert("users:100".into()));
        assert!(set.insert("users:200".into()));
        assert!(set.insert("users:300".into()));
        assert!(!set.insert("users:200".into()));
        assert_eq!(set.len(), 3);
        assert!(set.contains("users:100"));
        assert!(set.contains_slice(b"users:200"));
        assert!(!set.contains("users:400"));
        assert!(set.remove("users:200"));
        assert!(!set.remove("users:200"));
        assert!(set.remove_by_slice(b"users:300"));
        assert!(!set.remove_by_slice(b"users:300"));
        assert!(!set.contains("users:200"));
        assert_eq!(set.len(), 1);
        set.validate_invariants();
    }

    #[test]
    fn iteration_is_ordered_and_double_ended() {
        let set = ArtSet::<Vec<u8>>::new();
        for k in [&b"c"[..], b"a", b"", b"b", b"ab"] {
            set.insert(k.to_vec());
        }
        let fwd: Vec<Vec<u8>> = set.iter().map(|k| k.clone()).collect();
        assert_eq!(fwd, [&b""[..], b"a", b"ab", b"b", b"c"]);
        let rev: Vec<Vec<u8>> = (&set).into_iter().rev().map(|k| k.clone()).collect();
        assert_eq!(rev, [&b"c"[..], b"b", b"ab", b"a", b""]);
        let mid: Vec<Vec<u8>> = set
            .range(b"a".to_vec()..=b"b".to_vec())
            .map(|k| k.clone())
            .collect();
        assert_eq!(mid, [&b"a"[..], b"ab", b"b"]);
    }

    #[test]
    fn scan_stops_when_the_callback_says_so() {
        let set = ArtSet::<[u8; 8]>::new();
        for i in 0..100u64 {
            set.insert(i.to_be_bytes());
        }
        let mut seen = 0;
        set.scan::<_, [u8], _>(.., |_| {
            seen += 1;
            seen < 10
        });
        assert_eq!(seen, 10);
    }

    #[test]
    fn clear_empties_the_set() {
        let mut set = ArtSet::<[u8; 8]>::new();
        for i in 0..1000u64 {
            set.insert(i.to_be_bytes());
        }
        set.clear();
        assert!(set.is_empty());
        assert!(set.iter().next().is_none());
        assert!(set.insert(1u64.to_be_bytes()));
        assert_eq!(set.len(), 1);
        set.validate_invariants();
    }

    /// A key rejected by `insert` is dropped at once, and the key already in
    /// the set is the one that stays.
    #[test]
    fn rejected_insert_drops_the_new_key_and_keeps_the_old() {
        struct K(Vec<u8>, usize, Arc<AtomicUsize>);
        impl Drop for K {
            fn drop(&mut self) {
                self.2.fetch_add(1, Ordering::SeqCst);
            }
        }
        impl AsBytes for K {
            fn as_bytes(&self) -> &[u8] {
                &self.0
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let set = ArtSet::<K>::new();
        assert!(set.insert(K(b"k".to_vec(), 1, Arc::clone(&drops))));
        assert!(!set.insert(K(b"k".to_vec(), 2, Arc::clone(&drops))));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(set.iter().map(|k| k.1).collect::<Vec<_>>(), [1]);
        drop(set);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    /// Racing inserts of one key: exactly one thread wins.
    #[test]
    fn concurrent_inserts_of_one_key_have_one_winner() {
        let threads = if cfg!(miri) { 2 } else { 8 };
        let rounds = if cfg!(miri) { 10 } else { 500 };
        let set = Arc::new(ArtSet::<[u8; 8]>::new());
        let wins = Arc::new(AtomicUsize::new(0));
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let (set, wins) = (Arc::clone(&set), Arc::clone(&wins));
                std::thread::spawn(move || {
                    for i in 0..rounds as u64 {
                        if set.insert(i.to_be_bytes()) {
                            wins.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(wins.load(Ordering::SeqCst), rounds);
        assert_eq!(set.len(), rounds);
    }

    /// Mirrors a `BTreeSet` under a mixed workload.
    #[test]
    fn matches_btreeset() {
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};

        let mut rng = StdRng::seed_from_u64(7);
        let mut set = ArtSet::<Vec<u8>>::new();
        let mut model = BTreeSet::<Vec<u8>>::new();
        for _ in 0..if cfg!(miri) { 200 } else { 5000 } {
            let len = rng.gen_range(0..4);
            let k: Vec<u8> = (0..len).map(|_| rng.gen_range(0..4u8)).collect();
            match rng.gen_range(0..4) {
                0 | 1 => assert_eq!(set.insert(k.clone()), model.insert(k)),
                2 => assert_eq!(set.remove(&k), model.remove(&k)),
                _ => assert_eq!(set.contains(&k), model.contains(&k)),
            }
        }
        assert_eq!(set.len(), model.len());
        let got: Vec<Vec<u8>> = set.iter().map(|k| k.clone()).collect();
        assert!(got.into_iter().eq(model.iter().cloned()));
        set.validate_invariants();
    }
}
