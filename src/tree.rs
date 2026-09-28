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

//! # The heap tree behind [`ArtMap`](crate::ArtMap)

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::ptr::NonNull;

use crate::key::AsBytes;
use crate::node::Leaf;
use crate::raw::heap::HeapStorage;
use crate::raw::{Mode, Outcome, RawTree, Unpublished};

pub(crate) type Storage<K, V> = HeapStorage<Leaf<K, V>>;

/// The concurrent tree of an `ArtMap`.
///
/// Invariant in `K` and `V`, owns them, and is `Send`/`Sync` only for
/// `K, V: Send + Sync` (via [`HeapStorage`]; §6.6, §8.9).
pub(crate) struct Tree<K, V> {
    pub(crate) raw: RawTree<Storage<K, V>>,
}

impl<K, V> Tree<K, V> {
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

impl<K, V> Drop for Tree<K, V> {
    fn drop(&mut self) {
        // SAFETY: `&mut self` in `drop`: no borrow of the map, and therefore no
        // handle or iterator, exists (Inv 2). Deferred destructors of retired
        // leaves own their leaves independently.
        unsafe { self.raw.destroy() }
    }
}

impl<K: AsBytes + Send + 'static, V: Send + 'static> Tree<K, V> {
    /// Inserts `(key, value)`. The caller holds `guard`, a pin on the default
    /// collector taken before this call (Inv 6).
    pub(crate) fn insert(
        &self,
        key: K,
        value: V,
        mode: Mode,
        guard: &crossbeam_epoch::Guard,
    ) -> Outcome<Leaf<K, V>> {
        let leaf = crate::raw::boxed(Leaf::new(key, value));
        // SAFETY: a fresh, fully initialised Box allocation owned by this call.
        let owner = unsafe { Unpublished::new(&self.raw.storage, leaf) };
        // SAFETY: `owner` keeps the leaf alive until it is published (after
        // which `guard` protects it) or until `owner` drops, after the call.
        // The key bytes are derived once, from the leaf's final location (Inv 10).
        let key_bytes = unsafe { leaf.as_ref() }.key.as_bytes();
        match self.raw.insert(&owner, key_bytes, mode, 1, guard) {
            Ok(o) => o,
            Err(never) => match never {},
        }
        // `owner` drops here: a discarded leaf is freed after every latch is
        // released and after the last use of `key_bytes`.
    }

    /// Removes the leaf for `key`; see [`RawTree::remove`].
    #[inline]
    pub(crate) fn remove(
        &self,
        key: &[u8],
        guard: &crossbeam_epoch::Guard,
    ) -> Option<NonNull<Leaf<K, V>>> {
        self.raw.remove(
            key,
            // SAFETY: candidate leaves are protected by `guard`.
            |l| unsafe { l.as_ref() }.key.as_bytes() == key,
            guard,
        )
    }

    /// Removes exactly `leaf` if it is still linked. Writes nothing when it is
    /// not found.
    pub(crate) fn remove_leaf(
        &self,
        leaf: NonNull<Leaf<K, V>>,
        guard: &crossbeam_epoch::Guard,
    ) -> bool {
        // SAFETY: the caller's handle keeps `leaf` protected.
        let l = unsafe { leaf.as_ref() };
        if l.is_removed() {
            return false;
        }
        let key = l.key.as_bytes();
        self.raw.remove(key, |c| c == leaf, guard).is_some()
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::raw::LeafNode;

    fn get<'g>(t: &Tree<String, i32>, k: &str, _g: &'g crossbeam_epoch::Guard) -> Option<&'g i32> {
        // SAFETY: the leaf is protected by `_g` for `'g`.
        t.raw
            .get(k.as_bytes())
            .map(|l| unsafe { &(*l.as_ptr()).value })
    }

    #[test]
    fn basic_crud() {
        let mut t = Tree::<String, i32>::new();
        let g = &crate::guard::pin();
        for (k, v) in [("apple", 1), ("banana", 2), ("cherry", 3)] {
            assert!(matches!(
                t.insert(k.into(), v, Mode::Replace, g),
                Outcome::Inserted(_)
            ));
        }
        assert_eq!(t.len(), 3);
        assert_eq!(get(&t, "apple", g), Some(&1));
        assert_eq!(get(&t, "durian", g), None);
        let Outcome::Replaced(old) = t.insert("apple".into(), 400, Mode::Replace, g) else {
            panic!("expected a replace")
        };
        // SAFETY: protected by `g`.
        let old = unsafe { old.as_ref() };
        assert_eq!(old.value, 1);
        assert!(old.is_removed());
        assert_eq!(get(&t, "apple", g), Some(&400));
        let removed = t.remove(b"banana", g).expect("present");
        // SAFETY: protected by `g`.
        assert_eq!(unsafe { removed.as_ref() }.key_bytes(), b"banana");
        assert_eq!(get(&t, "banana", g), None);
        assert_eq!(t.len(), 2);
        t.raw.validate();
    }

    #[test]
    fn growth_and_prefixes() {
        let mut t = Tree::<String, usize>::new();
        let g = &crate::guard::pin();
        for i in 0..300 {
            t.insert(format!("prefix:{i:03}"), i, Mode::Replace, g);
        }
        for i in 0..300 {
            let k = format!("prefix:{i:03}");
            // SAFETY: protected by `g`.
            let v = t.raw.get(k.as_bytes()).map(|l| unsafe { l.as_ref() }.value);
            assert_eq!(v, Some(i));
        }
        assert_eq!(t.len(), 300);
        t.raw.validate();
    }

    /// §16.5 transient `len` (a): an empty map oscillating one key never
    /// shows a negative raw counter (Inv 12).
    #[test]
    fn transient_len_never_negative() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        let t = Arc::new(Tree::<Vec<u8>, u64>::new());
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let (t, stop) = (Arc::clone(&t), Arc::clone(&stop));
            std::thread::spawn(move || {
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) && i < if cfg!(miri) { 20 } else { 200_000 } {
                    let g = crate::guard::pin();
                    t.insert(vec![7], i, Mode::Replace, &g);
                    t.remove(&[7], &g);
                    i += 1;
                }
            })
        };
        let polls = if cfg!(miri) { 50 } else { 200_000 };
        for _ in 0..polls {
            assert!(t.raw.raw_len() >= 0, "len went negative");
            assert!(t.raw.raw_len() <= 1);
        }
        stop.store(true, Ordering::Relaxed);
        writer.join().unwrap();
        assert_eq!(t.raw.raw_len(), 0);
    }

    /// §16.5 transient `len` (b): permanent keys under a Node4 while a third
    /// key oscillates; `len() >= 2` on every poll.
    #[test]
    fn transient_len_keeps_permanent_keys() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        let t = Arc::new(Tree::<Vec<u8>, u64>::new());
        {
            let g = crate::guard::pin();
            t.insert(vec![0, 1], 1, Mode::Replace, &g);
            t.insert(vec![0, 3], 3, Mode::Replace, &g);
        }
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let (t, stop) = (Arc::clone(&t), Arc::clone(&stop));
            std::thread::spawn(move || {
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) && i < if cfg!(miri) { 20 } else { 200_000 } {
                    let g = crate::guard::pin();
                    t.insert(vec![0, 2], i, Mode::Replace, &g);
                    t.remove(&[0, 2], &g);
                    i += 1;
                }
            })
        };
        for _ in 0..if cfg!(miri) { 50 } else { 200_000 } {
            let n = t.raw.raw_len();
            assert!((2..=3).contains(&n), "len {n} outside [2, 3]");
        }
        stop.store(true, Ordering::Relaxed);
        writer.join().unwrap();
    }

    #[test]
    fn long_shared_prefixes_build_chains() {
        let mut t = Tree::<Vec<u8>, usize>::new();
        let g = &crate::guard::pin();
        let mk = |i: usize| {
            let mut k = vec![7u8; 100];
            k.extend_from_slice(&i.to_be_bytes());
            k
        };
        for i in 0..50 {
            t.insert(mk(i), i, Mode::Replace, g);
        }
        // Keys that are prefixes of other keys.
        t.insert(vec![7u8; 100], 1000, Mode::Replace, g);
        t.insert(vec![7u8; 40], 1001, Mode::Replace, g);
        t.insert(vec![], 1002, Mode::Replace, g);
        for i in 0..50 {
            assert!(t.raw.get(&mk(i)).is_some());
        }
        assert!(t.raw.get(&[7u8; 100]).is_some());
        assert!(t.raw.get(&[7u8; 40]).is_some());
        assert!(t.raw.get(&[]).is_some());
        assert!(t.raw.get(&[7u8; 41]).is_none());
        assert_eq!(t.len(), 53);
        t.raw.validate();
    }
}
