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

//! # Whole-tree walks: drop, `clear` and validation
//!
//! All iterative, with an explicit worklist (Inv 13).

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use crate::latch::AbortOnUnwind;
use crate::raw::node::{Taken, MAX_PREFIX_LEN};
use crate::raw::slot::{AtomicSlot, Slot};
use crate::raw::{Layout, LeafNode, Raw, RawTree, Storage};

impl<S: Layout> RawTree<S> {
    /// Frees every node and leaf. Called from the owner's `Drop`, which has
    /// exclusive access (no borrow of the map exists, Inv 2).
    ///
    /// # Safety
    /// The caller has exclusive access and the tree is not used afterwards.
    pub(crate) unsafe fn destroy(&mut self) {
        let root = self.root();
        self.root
            .store(Raw::<S>::NULL, crate::sync::atomic::Ordering::Relaxed);
        let mut work = vec![root];
        while let Some(raw) = work.pop() {
            if raw.is_null() {
                continue;
            }
            if raw.is_leaf() {
                // SAFETY: exclusively owned live leaf, freed once.
                unsafe { self.storage.free_leaf(self.storage.leaf(raw)) };
                continue;
            }
            // SAFETY: exclusively owned live node.
            let node = unsafe { self.node_ref(raw) };
            work.push(node.exact_leaf());
            node.for_each_child(|_, c| work.push(c));
            // SAFETY: exclusively owned; its children were collected above.
            unsafe { self.storage.free_node(self.storage.node(raw)) };
        }
    }
}

impl<S: Storage> RawTree<S> {
    /// `clear()` per §9.8: swap the root under `root_latch`, then walk the
    /// detached tree holding exactly one node latch at a time, marking each
    /// node obsolete, retiring nodes and leaves individually, and subtracting
    /// the count of leaves found. The swap is the linearization point.
    ///
    /// The caller has pinned before calling (Inv 6).
    pub(crate) fn clear(&self, guard: &S::Guard) {
        self.clear_counting(guard, |_| 1);
    }

    /// As [`clear`](Self::clear), where `count` returns the `len` each
    /// detached leaf accounts for (for a versioned leaf, after killing its
    /// chain, whether its newest version is live). It runs under the leaf's
    /// node latch, or after the root swap for a root leaf, with the same
    /// restrictions as `remove_confirmed`'s `confirm`.
    pub(crate) fn clear_counting(
        &self,
        guard: &S::Guard,
        count: impl Fn(std::ptr::NonNull<S::Leaf>) -> isize,
    ) {
        let old = {
            let Some(w) = self.root_latch.lock() else {
                unreachable!("root latch is never obsolete")
            };
            let old = self
                .root
                .swap(Raw::<S>::NULL, crate::sync::atomic::Ordering::AcqRel);
            drop(w);
            old
        };
        if old.is_null() {
            return;
        }
        // The walk allocates (worklist, deferred bags); an unwind mid-walk would
        // leave detached nodes un-obsoleted and `len` wrong.
        let bomb = AbortOnUnwind;
        let mut total = 0isize;
        let mut work = vec![old];
        while let Some(raw) = work.pop() {
            if raw.is_leaf() {
                // A detached root leaf, published before the swap.
                // SAFETY: protected leaf, now unreachable from the root.
                let leaf = unsafe { self.storage.leaf(raw) };
                total += count(leaf);
                // SAFETY: as above.
                unsafe { leaf.as_ref() }.mark_removed();
                // SAFETY: unreachable for new readers, retired once.
                unsafe { self.storage.retire_leaf(leaf, guard) };
                continue;
            }
            // SAFETY: protected node, detached from the root.
            let node = unsafe { self.node_ref(raw) };
            // A detached node's parent was locked and obsoleted before it was
            // pushed, so no concurrent grow can obsolete it.
            let Some(w) = node.latch.lock() else {
                debug_assert!(false, "a detached node cannot be obsolete");
                continue;
            };
            let mut leaves = Vec::new();
            let exact = node.exact_leaf();
            if !exact.is_null() {
                leaves.push(exact);
            }
            node.for_each_child(|_, c| {
                if c.is_leaf() {
                    leaves.push(c);
                } else {
                    work.push(c);
                }
            });
            for &l in &leaves {
                // SAFETY: protected leaf held by a node whose latch we hold.
                total += count(unsafe { self.storage.leaf(l) });
                // SAFETY: as above.
                unsafe { self.leaf_ref(l) }.mark_removed();
            }
            w.mark_obsolete();
            for l in leaves {
                // SAFETY: its only home is an obsolete, unlinked node.
                unsafe { self.storage.retire_leaf(self.storage.leaf(l), guard) };
            }
            // SAFETY: unlinked and obsolete, retired once.
            unsafe { self.storage.retire_node(self.storage.node(raw), guard) };
        }
        self.len_add(-total);
        bomb.defuse();
    }

    /// Checks the structural invariants of a quiescent tree and returns the
    /// number of reachable leaves. Panics on a violation.
    ///
    /// Includes delete-side unlinking (§13): no inner node is empty. A node
    /// with a single entry is allowed: removes leave them, and prefix chains
    /// are built that way.
    pub(crate) fn validate(&mut self) -> usize {
        self.validate_counting(|_| 1)
    }

    /// As [`validate`](Self::validate), where `live` is the `len` each
    /// reachable leaf accounts for (for a versioned leaf, whether its newest
    /// version is live). Returns the number of reachable leaves.
    pub(crate) fn validate_counting(
        &mut self,
        live: impl Fn(std::ptr::NonNull<S::Leaf>) -> usize,
    ) -> usize {
        let mut live_total = 0usize;
        let root = self.root();
        let mut count = 0usize;
        // (node, path length before this node's prefix)
        let mut path: Vec<u8> = Vec::new();
        let mut work: Vec<(Raw<S>, usize, Option<u8>)> = vec![(root, 0, None)];
        while let Some((raw, depth, byte)) = work.pop() {
            path.truncate(depth);
            if let Some(b) = byte {
                path.push(b);
            }
            if raw.is_null() {
                continue;
            }
            if raw.is_leaf() {
                // SAFETY: exclusive access (`&mut self`); live leaf.
                let l = unsafe { self.leaf_ref(raw) };
                assert!(!l.is_removed(), "a reachable leaf is marked removed");
                let k = l.key_bytes();
                assert!(
                    k.starts_with(&path),
                    "leaf key {k:?} does not start with its path {path:?}"
                );
                count += 1;
                // SAFETY: as above.
                live_total += live(unsafe { self.storage.leaf(raw) });
                continue;
            }
            // SAFETY: exclusive access; live node.
            let node = unsafe { self.node_ref(raw) };
            assert!(!node.latch.is_obsolete(), "a linked node is obsolete");
            assert!(
                node.latch.read_version().is_some(),
                "a node is locked at quiescence"
            );
            // The unclamped length: `load_prefix` would hide an overlong one.
            assert!(
                node.raw_prefix_len() <= MAX_PREFIX_LEN,
                "prefix_len exceeds MAX_PREFIX_LEN"
            );
            assert!(
                !node.emptied_by(Taken::Nothing),
                "an inner node is empty (an unlink missed it)"
            );
            let prefix = node.load_prefix();
            path.extend_from_slice(prefix.as_slice());
            let exact = node.exact_leaf();
            if !exact.is_null() {
                assert!(exact.is_leaf(), "exact_leaf holds an inner node");
                // SAFETY: exclusive access; live leaf.
                let l = unsafe { self.leaf_ref(exact) };
                assert!(!l.is_removed(), "a reachable leaf is marked removed");
                let k = l.key_bytes();
                assert_eq!(k, path.as_slice(), "exact leaf key must equal its path");
                count += 1;
                // SAFETY: as above.
                live_total += live(unsafe { self.storage.leaf(exact) });
            }
            let n = node.num_children();
            let mut seen = 0usize;
            let mut last: Option<u8> = None;
            let here = path.len();
            let mut children = Vec::new();
            node.for_each_child(|b, c| {
                assert!(last.is_none_or(|l| l < b), "children must be sorted");
                last = Some(b);
                seen += 1;
                children.push((c, here, Some(b)));
            });
            assert_eq!(seen, n, "num_children must match the live children");
            // Reverse, so the worklist pops children in key order.
            work.extend(children.into_iter().rev());
        }
        assert_eq!(
            live_total,
            self.len(),
            "len must equal the reachable live count"
        );
        count
    }
}
