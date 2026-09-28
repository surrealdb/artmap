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
use crate::raw::node::MAX_PREFIX_LEN;
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
        let mut count = 0isize;
        let mut work = vec![old];
        while let Some(raw) = work.pop() {
            if raw.is_leaf() {
                // A detached root leaf, published before the swap.
                // SAFETY: protected leaf, now unreachable from the root.
                let leaf = unsafe { self.storage.leaf(raw) };
                // SAFETY: as above.
                unsafe { leaf.as_ref() }.mark_removed();
                count += 1;
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
                unsafe { self.leaf_ref(l) }.mark_removed();
            }
            w.mark_obsolete();
            count += leaves.len() as isize;
            for l in leaves {
                // SAFETY: its only home is an obsolete, unlinked node.
                unsafe { self.storage.retire_leaf(self.storage.leaf(l), guard) };
            }
            // SAFETY: unlinked and obsolete, retired once.
            unsafe { self.storage.retire_node(self.storage.node(raw), guard) };
        }
        self.len_add(-count);
        bomb.defuse();
    }

    /// Checks the structural invariants of a quiescent tree and returns the
    /// number of reachable leaves. Panics on a violation.
    pub(crate) fn validate(&mut self) -> usize {
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
                let k = unsafe { self.leaf_ref(raw) }.key_bytes();
                assert!(
                    k.starts_with(&path),
                    "leaf key {k:?} does not start with its path {path:?}"
                );
                count += 1;
                continue;
            }
            // SAFETY: exclusive access; live node.
            let node = unsafe { self.node_ref(raw) };
            assert!(!node.latch.is_obsolete(), "a linked node is obsolete");
            assert!(
                node.latch.read_version().is_some(),
                "a node is locked at quiescence"
            );
            let prefix = node.load_prefix();
            assert!(prefix.len <= MAX_PREFIX_LEN);
            path.extend_from_slice(prefix.as_slice());
            let exact = node.exact_leaf();
            if !exact.is_null() {
                assert!(exact.is_leaf(), "exact_leaf holds an inner node");
                // SAFETY: exclusive access; live leaf.
                let k = unsafe { self.leaf_ref(exact) }.key_bytes();
                assert_eq!(k, path.as_slice(), "exact leaf key must equal its path");
                count += 1;
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
        assert_eq!(count, self.len(), "len must equal the reachable leaf count");
        count
    }
}
