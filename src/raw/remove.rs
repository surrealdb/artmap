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

//! # Removes, and unlinking the nodes they empty (§9.4, §13)
//!
//! A remove unlinks one leaf from its node `N`. If that leaves `N` empty, the
//! same critical section unlinks `N` from its parent `P` too; otherwise the
//! remove locks only `N`, as before. A node left with a single entry stays as
//! it is: collapsing it would make every remove and re-insert of a key beside
//! one other key free a node and build a new one.
//! [`shrink_to_fit`](RawTree::shrink_to_fit) fits such nodes on demand.
//!
//! **Protocol** (Inv 7). `P` (or `root_latch`) is taken with a blocking
//! `lock()` while holding nothing, then re-checked to point at `N`; `N` with
//! `try_upgrade(v_N)`, restarting on failure. That is the grow and split
//! order, so it cannot deadlock. The commit removes `N` from `P` and marks `N`
//! obsolete before `P` is released. `N`'s slots are left as they are: every
//! reader or writer still inside `N` fails its next validation or upgrade, and
//! `N` is retired once `P` is released.
//!
//! Unlinking allocates nothing, so a remove never fails, and the arena maps
//! unlink empty nodes too (their bytes stay in the arena, as a replaced
//! leaf's do).
//!
//! **Follow-up** (§13 3a). If unlinking `N` leaves `P` empty, the remover
//! releases every latch and unlinks `P` as a new top-down operation, then
//! `P`'s parent if that leaves it empty, and so on up, holding at most two
//! latches at a time. A prefix chain of single-child `Node4`s goes with the
//! last key below it.
//!
//! An empty node is a state that readers, writers and cursors already handle,
//! so no reader depends on this for correctness. At quiescence,
//! [`validate`](RawTree::validate) checks that it completed.

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::ptr::NonNull;

use crate::latch::{AbortOnUnwind, SpinBackoff, WriteGuard};
use crate::raw::node::{NodeHeader, Taken};
use crate::raw::slot::{AtomicSlot, Slot};
use crate::raw::write::{Parent, ParentGuard};
use crate::raw::{LeafNode, Raw, RawTree, Storage};

/// The result of an emptying remove's latched section.
enum Unlink {
    /// A failed acquisition. Nothing was written; retry from the root.
    Retry,
    /// Done. `true` if the commit left the parent empty, so it needs a
    /// follow-up.
    Done(bool),
}

/// Takes the removed leaf out of a node that stays linked. Caller holds the
/// node's latch.
fn take_out<A: AtomicSlot>(node: &NodeHeader<A>, w: &WriteGuard<'_>, taken: Taken) {
    match taken {
        Taken::Exact => node.set_exact_leaf(w, A::Raw::NULL),
        Taken::Child(byte) => node.remove_child(w, byte),
        Taken::Nothing => {}
    }
}

impl<S: Storage> RawTree<S> {
    /// Removes the leaf for `key`, if `matches` accepts it, and unlinks the
    /// nodes that leaves empty (§13). Returns the leaf unlinked, marked
    /// removed and already retired (§9.4).
    ///
    /// `matches` runs before any latch: it compares keys (user code) for
    /// `remove`, or pointer identity for `remove_leaf`.
    pub(crate) fn remove(
        &self,
        key: &[u8],
        matches: impl Fn(NonNull<S::Leaf>) -> bool,
        guard: &S::Guard,
    ) -> Option<NonNull<S::Leaf>> {
        let mut backoff = SpinBackoff::new();
        'retry: loop {
            let root = self.root();
            if root.is_null() {
                return None;
            }
            if root.is_leaf() {
                let leaf = self.storage_leaf(root);
                if !matches(leaf) {
                    return None;
                }
                let Some(w) = self.root_latch.lock() else {
                    unreachable!("root latch is never obsolete")
                };
                if self.root() != root {
                    drop(w);
                    continue 'retry;
                }
                let bomb = AbortOnUnwind;
                self.set_root(&w, Raw::<S>::NULL);
                // SAFETY: protected leaf read from `root`.
                unsafe { leaf.as_ref() }.mark_removed();
                self.len_add(-1);
                bomb.defuse();
                drop(w);
                // SAFETY: unlinked under `root_latch` and marked.
                unsafe { self.storage.retire_leaf(leaf, guard) };
                return Some(leaf);
            }

            let mut parent = Parent::Root;
            let mut node_raw = root;
            let mut depth = 0usize;
            loop {
                // SAFETY: protected inner node read from the tree.
                let node = unsafe { self.node_ref(node_raw) };
                let Some(v) = node.latch.read_version() else {
                    backoff.spin();
                    continue 'retry;
                };
                if !self.still_parent(parent, node_raw) {
                    continue 'retry;
                }
                let prefix = node.load_prefix();
                let rest = key.get(depth..).unwrap_or(&[]);
                if !prefix.is_prefix_of(rest) {
                    if !node.latch.validate(v) {
                        continue 'retry;
                    }
                    return None;
                }
                let node_depth = depth + prefix.len;
                let (slot, taken) = if node_depth == key.len() {
                    (node.exact_leaf(), Taken::Exact)
                } else {
                    let byte = key[node_depth];
                    let child = node.find_child(byte);
                    if let Some(c) = child.filter(|c| !c.is_leaf()) {
                        // Coupled at the next level (R4).
                        parent = Parent::Node {
                            raw: node_raw,
                            version: v,
                            byte,
                        };
                        node_raw = c;
                        depth = node_depth + 1;
                        continue;
                    }
                    (child.unwrap_or(Raw::<S>::NULL), Taken::Child(byte))
                };
                let empties = node.emptied_by(taken);
                if !node.latch.validate(v) {
                    continue 'retry;
                }
                if slot.is_null() {
                    return None;
                }
                let leaf = self.storage_leaf(slot);
                if !matches(leaf) {
                    return None;
                }
                if empties {
                    match self.remove_unlinking(parent, node_raw, v, taken, leaf, guard) {
                        Unlink::Retry => {
                            backoff.spin();
                            continue 'retry;
                        }
                        Unlink::Done(follow_up) => {
                            if follow_up {
                                self.unlink_emptied(key, guard);
                            }
                            return Some(leaf);
                        }
                    }
                }
                // `N` keeps an entry: lock only `N`. Its absolute path cannot
                // change without bumping its version (Inv 7).
                let Some(nw) = node.latch.try_upgrade(v) else {
                    backoff.spin();
                    continue 'retry;
                };
                let bomb = AbortOnUnwind;
                take_out(node, &nw, taken);
                // SAFETY: protected leaf read from a validated slot.
                unsafe { leaf.as_ref() }.mark_removed();
                self.len_add(-1);
                bomb.defuse();
                drop(nw);
                // SAFETY: unlinked under the node latch and marked.
                unsafe { self.storage.retire_leaf(leaf, guard) };
                return Some(leaf);
            }
        }
    }

    /// The latched part of a remove that takes `N`'s (`node_raw`, read at
    /// version `v`) last entry: takes `P`, then `N`, and unlinks `N` with the
    /// leaf, per the module docs.
    fn remove_unlinking(
        &self,
        parent: Parent<Raw<S>>,
        node_raw: Raw<S>,
        v: u64,
        taken: Taken,
        leaf: NonNull<S::Leaf>,
        guard: &S::Guard,
    ) -> Unlink {
        // SAFETY: protected inner node read from the tree in this operation.
        let node = unsafe { self.node_ref(node_raw) };
        crate::hooks::pause(crate::hooks::Point::WriterBeforeUpgrade);
        let Some(pg) = self.lock_parent(parent, node_raw) else {
            return Unlink::Retry;
        };
        #[cfg(loom)]
        if crate::latch::mutants::UNLINK_WITHOUT_NODE_LATCH.with(|m| m.get()) {
            // The mutant: unlink `N` holding only `P`, so a writer that locks
            // only `N` can still write into it.
            if !node.latch.validate(v) {
                return Unlink::Retry;
            }
            self.unlink_in_parent(&pg);
            // SAFETY: protected leaf read from a validated slot.
            unsafe { leaf.as_ref() }.mark_removed();
            self.len_add(-1);
            return Unlink::Done(false);
        }
        let Some(nw) = node.latch.try_upgrade(v) else {
            drop(pg);
            return Unlink::Retry;
        };
        // Read at `v`, which the upgrade just confirmed: exact.
        debug_assert!(node.emptied_by(taken));
        let bomb = AbortOnUnwind;
        // The store that unlinks `N`, and with it the removed leaf.
        self.unlink_in_parent(&pg);
        // SAFETY: protected leaf read from a validated slot.
        unsafe { leaf.as_ref() }.mark_removed();
        self.len_add(-1);
        // Inv 7: unlinked, so obsolete, before the parent is released.
        nw.mark_obsolete();
        bomb.defuse();
        let follow_up = match &pg {
            ParentGuard::Node(p, ..) => p.emptied_by(Taken::Nothing),
            ParentGuard::Root(_) => false,
        };
        drop(pg);
        // SAFETY: unlinked under the parent latch and marked.
        unsafe { self.storage.retire_leaf(leaf, guard) };
        // SAFETY: unlinked under the parent latch and obsolete; retired once.
        unsafe { self.storage.retire_node(self.node_ptr(node_raw), guard) };
        Unlink::Done(follow_up)
    }

    /// The follow-up (§13 3a): while the last inner node on `key`'s path is
    /// empty, unlinks it from its parent, then moves up to the parent.
    ///
    /// A new top-down operation: it holds nothing on entry, descends from the
    /// root, and restarts on any failed acquisition. Each step up takes the
    /// parent by `lock()` and a pointer re-check, and the node by
    /// `try_upgrade` of the version its own previous step unlocked it at, so
    /// a chain is unlinked without a re-descent per link. Every step unlinks a
    /// node, so the operation ends.
    fn unlink_emptied(&self, key: &[u8], guard: &S::Guard) {
        let mut backoff = SpinBackoff::new();
        // The ancestors of the node at hand: (node, version, byte to its child).
        let mut path: Vec<(Raw<S>, u64, u8)> = Vec::new();
        let parent_of = |path: &[(Raw<S>, u64, u8)]| match path.last() {
            None => Parent::Root,
            Some(&(raw, version, byte)) => Parent::Node { raw, version, byte },
        };
        'retry: loop {
            path.clear();
            let root = self.root();
            if root.is_null() || root.is_leaf() {
                return;
            }
            // Descend to the last inner node on `key`'s path.
            let mut node_raw = root;
            let mut depth = 0usize;
            let (mut x_raw, mut xv) = loop {
                // SAFETY: protected inner node read from the tree.
                let node = unsafe { self.node_ref(node_raw) };
                let Some(v) = node.latch.read_version() else {
                    backoff.spin();
                    continue 'retry;
                };
                if !self.still_parent(parent_of(&path), node_raw) {
                    continue 'retry;
                }
                let prefix = node.load_prefix();
                let rest = key.get(depth..).unwrap_or(&[]);
                let node_depth = depth + prefix.len;
                let next = match key.get(node_depth) {
                    Some(&byte) if prefix.is_prefix_of(rest) => node
                        .find_child(byte)
                        .filter(|c| !c.is_leaf())
                        .map(|c| (byte, c)),
                    _ => None,
                };
                if let Some((byte, c)) = next {
                    path.push((node_raw, v, byte));
                    node_raw = c;
                    depth = node_depth + 1;
                    continue;
                }
                let empty = node.emptied_by(Taken::Nothing);
                if !node.latch.validate(v) {
                    continue 'retry;
                }
                if !empty {
                    return;
                }
                break (node_raw, v);
            };
            // Unlink upwards while the node at hand is empty.
            loop {
                // SAFETY: protected inner node read from the tree in this operation.
                let x = unsafe { self.node_ref(x_raw) };
                let Some(pg) = self.lock_parent(parent_of(&path), x_raw) else {
                    backoff.spin();
                    continue 'retry;
                };
                let Some(xw) = x.latch.try_upgrade(xv) else {
                    drop(pg);
                    backoff.spin();
                    continue 'retry;
                };
                debug_assert!(x.emptied_by(Taken::Nothing));
                let bomb = AbortOnUnwind;
                self.unlink_in_parent(&pg);
                xw.mark_obsolete();
                bomb.defuse();
                let above_empty = match &pg {
                    ParentGuard::Node(p, ..) => p.emptied_by(Taken::Nothing),
                    ParentGuard::Root(_) => false,
                };
                let unlocked = pg.unlock();
                // SAFETY: unlinked under the parent latch and obsolete; retired once.
                unsafe { self.storage.retire_node(self.node_ptr(x_raw), guard) };
                if !above_empty {
                    return;
                }
                let (Some(pv), Some((p_raw, _, _))) = (unlocked, path.pop()) else {
                    return;
                };
                x_raw = p_raw;
                xv = pv;
            }
        }
    }
}

#[cfg(all(test, not(loom)))]
pub(crate) mod tests {
    use crate::raw::heap::{NODE_COUNTS, RETIRED_NODES};
    use crate::raw::node::{NodeHeader, Rest, Taken};
    use crate::raw::slot::Slot;
    use crate::raw::{Layout, Mode};
    use crate::tree::{Storage, Tree};

    pub(crate) type T = Tree<Vec<u8>, u64>;
    pub(crate) type Header = NodeHeader<<Storage<Vec<u8>, u64> as Layout>::Atomic>;

    pub(crate) fn tree(keys: &[&[u8]]) -> T {
        let t = T::new();
        let g = &crate::guard::pin();
        for k in keys {
            t.insert(k.to_vec(), 0, Mode::Replace, g);
        }
        t
    }

    pub(crate) fn remove(t: &T, k: &[u8]) -> bool {
        t.remove(k, &crate::guard::pin()).is_some()
    }

    pub(crate) fn retired() -> usize {
        RETIRED_NODES.with(|c| c.get())
    }

    pub(crate) fn root_node(t: &T) -> &Header {
        let r = t.raw.root();
        assert!(!r.is_null() && !r.is_leaf(), "the root is an inner node");
        // SAFETY: the test's tree is quiescent and alive for the borrow.
        unsafe { t.raw.node_ref(r) }
    }

    pub(crate) fn get(t: &T, k: &[u8]) -> bool {
        let _g = crate::guard::pin();
        t.raw.get(k).is_some()
    }

    /// Inner nodes on the path of children at byte `b` from the root, down
    /// to the first leaf.
    pub(crate) fn chain_len(t: &T) -> usize {
        let mut n = 0;
        let mut raw = t.raw.root();
        while !raw.is_null() && !raw.is_leaf() {
            n += 1;
            // SAFETY: quiescent tree, alive for the borrow.
            let node = unsafe { t.raw.node_ref(raw) };
            raw = node.next_child(0).map_or(node.exact_leaf(), |(_, c)| c);
        }
        n
    }

    /// A 100-byte shared prefix, then `last`.
    pub(crate) fn long_key(last: u8) -> Vec<u8> {
        let mut k = vec![7u8; 100];
        k.push(last);
        k
    }

    #[test]
    fn rest_and_emptied_by_see_what_a_node_keeps() {
        let t = tree(&[b"ka", b"kb", b"k"]);
        let n = root_node(&t);
        assert_eq!(n.rest(Taken::Nothing), Rest::Many);
        assert!(matches!(n.rest(Taken::Exact), Rest::Many));
        assert!(!n.emptied_by(Taken::Exact) && !n.emptied_by(Taken::Child(b'a')));
        drop(t);
        let t = tree(&[b"ka", b"k"]);
        let n = root_node(&t);
        assert!(matches!(n.rest(Taken::Exact), Rest::Leaf(l) if l.is_leaf()));
        assert!(matches!(n.rest(Taken::Child(b'a')), Rest::Leaf(l) if l == n.exact_leaf()));
        assert!(!n.emptied_by(Taken::Exact));
        drop(t);
        let t = tree(&[b"ka1", b"ka2", b"kb"]);
        let n = root_node(&t);
        assert!(matches!(n.rest(Taken::Child(b'b')), Rest::Inner(b'a', c) if !c.is_leaf()));
        assert!(!n.emptied_by(Taken::Nothing));
    }

    #[test]
    fn a_node_left_with_one_entry_stays() {
        let mut t = tree(&[b"a1", b"a2", b"b"]);
        let before = retired();
        assert!(remove(&t, b"a1"));
        assert_eq!(retired(), before, "nothing is unlinked");
        let child = root_node(&t).find_child(b'a').unwrap();
        assert!(!child.is_leaf(), "the 'a' node keeps a2");
        assert!(get(&t, b"a2") && get(&t, b"b") && !get(&t, b"a1"));
        assert_eq!(t.raw.validate(), 2);
    }

    #[test]
    fn a_node_emptied_by_a_remove_is_unlinked() {
        let mut t = tree(&[b"a1", b"a2", b"b"]);
        assert!(remove(&t, b"a1"));
        let before = retired();
        assert!(remove(&t, b"a2"));
        assert_eq!(retired() - before, 1, "the 'a' node is retired");
        assert!(root_node(&t).find_child(b'a').is_none());
        assert_eq!(t.raw.validate(), 1);
        // Exact leaves count as entries.
        let mut t = tree(&[b"k", b"ka", b"x"]);
        assert!(remove(&t, b"ka"));
        assert!(get(&t, b"k"));
        assert!(remove(&t, b"k"));
        assert!(root_node(&t).find_child(b'k').is_none());
        assert_eq!(t.raw.validate(), 1);
    }

    #[test]
    fn the_root_node_goes_with_its_last_key() {
        let mut t = tree(&[b"ka", b"kb"]);
        assert!(remove(&t, b"ka"));
        assert!(!t.raw.root().is_leaf(), "the root node keeps kb");
        let before = retired();
        assert!(remove(&t, b"kb"));
        assert!(t.raw.root().is_null());
        assert_eq!(retired() - before, 1);
        assert_eq!(t.raw.validate(), 0);
    }

    #[test]
    fn a_prefix_chain_goes_with_its_last_key() {
        // 100 shared bytes: a chain of single-child Node4s above the fork.
        let mut t = tree(&[&long_key(1), &long_key(2)]);
        let chain = chain_len(&t);
        assert!(chain >= 6, "a chain of {chain} nodes");
        let before = retired();
        assert!(remove(&t, &long_key(1)));
        assert_eq!(retired(), before, "the fork keeps a key");
        assert!(remove(&t, &long_key(2)));
        assert_eq!(retired() - before, chain, "the whole chain is retired");
        assert!(t.raw.root().is_null());
        assert_eq!(t.raw.validate(), 0);
    }

    #[test]
    fn an_emptied_chain_is_unlinked_up_to_the_first_fork() {
        // A fork above a 40-byte chain: the chain goes, the fork stays.
        let key = |mid: u8, last: u8| {
            let mut k = vec![mid];
            k.extend_from_slice(&[9u8; 40]);
            k.push(last);
            k
        };
        let mut t = tree(&[&key(1, 1), &key(1, 2), &key(2, 0)]);
        assert!(remove(&t, &key(1, 1)));
        assert!(remove(&t, &key(1, 2)));
        let root = root_node(&t);
        assert!(root.find_child(1).is_none());
        assert!(root.find_child(2).unwrap().is_leaf());
        assert_eq!(t.raw.validate(), 1);
    }

    /// Every node is allocated once and then either freed unpublished, freed
    /// by drop, or retired: never leaked and never retired twice.
    #[test]
    fn every_node_is_accounted_for_under_churn() {
        use rand::{Rng, SeedableRng};
        let counts = || NODE_COUNTS.with(|c| (c.allocated.get(), c.freed.get(), c.retired.get()));
        let (a0, f0, r0) = counts();
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        let n = if cfg!(miri) { 300 } else { 20_000 };
        {
            let mut t = T::new();
            let mut live = std::collections::BTreeSet::new();
            let g = &crate::guard::pin();
            for i in 0..n {
                let len = rng.gen_range(0..6);
                let k: Vec<u8> = (0..len).map(|_| rng.gen_range(0..4u8)).collect();
                if rng.gen_bool(0.5) {
                    t.insert(k.clone(), i, Mode::Replace, g);
                    live.insert(k);
                } else {
                    assert_eq!(t.remove(&k, g).is_some(), live.remove(&k));
                }
                if i % 97 == 0 {
                    t.raw.shrink_to_fit(g);
                }
            }
            assert_eq!(t.raw.validate(), live.len());
            for k in &live {
                assert!(t.remove(k, g).is_some());
            }
            assert!(t.raw.root().is_null(), "an emptied tree has no nodes");
            assert_eq!(t.raw.validate(), 0);
        }
        let (a, f, r) = counts();
        assert!(a > a0);
        assert_eq!(
            a - a0,
            (f - f0) + (r - r0),
            "a node leaked or was retired twice"
        );
    }

    /// A sliding window keeps the tree at the window's size.
    #[test]
    fn a_sliding_window_stays_small() {
        let counts = || NODE_COUNTS.with(|c| c.allocated.get() - c.freed.get() - c.retired.get());
        let window = 64u64;
        let ops: u64 = if cfg!(miri) { 600 } else { 100_000 };
        let t = T::new();
        let base = counts();
        let mut peak = 0;
        let g = &crate::guard::pin();
        for i in 0..ops {
            t.insert(i.to_be_bytes().to_vec(), i, Mode::Replace, g);
            if i >= window {
                assert!(t.remove(&(i - window).to_be_bytes(), g).is_some());
            }
            peak = peak.max(counts() - base);
        }
        assert!(peak <= 8, "{peak} live nodes for a window of {window} keys");
    }
}
