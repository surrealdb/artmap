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

//! # Removes and delete-side compaction (§9.4, §13)
//!
//! A remove unlinks one leaf from its node `N`. If that would leave `N` with
//! at most one entry, the same critical section also compacts `N`:
//!
//! - nothing left: `N` is unlinked from its parent `P`;
//! - one leaf left: that leaf takes `N`'s place in `P`;
//! - one inner child `C` left: `N` is merged into `C`, which takes `N`'s place
//!   in `P` with the prefix `N.prefix + byte + C.prefix`. The merge happens
//!   only if that prefix fits in one node (`MAX_PREFIX_LEN`); otherwise, and
//!   when `C` is contended, it is declined and `N` keeps its single child.
//!
//! Otherwise the remove locks only `N`, as before.
//!
//! **Protocol** (Inv 7). `P` (or `root_latch`) is taken with a blocking
//! `lock()` while holding nothing, then re-checked to point at `N`; `N` with
//! `try_upgrade(v_N)`, restarting on failure; `C` with a `try_upgrade` of a
//! version read while holding `N`, declining the merge on failure. That is the
//! grow and split order, so it cannot deadlock. The commit publishes in `P`,
//! rewrites `C`'s prefix under `C`'s latch (its unlock bumps its version), and
//! marks `N` obsolete, all before `P` is released. `N`'s slots are left as
//! they are: every reader or writer still inside `N` fails its next
//! validation or upgrade, and `N` is retired once `P` is released.
//!
//! Compaction allocates nothing, so a remove never fails, and the arena maps
//! compact too (the unlinked node's bytes stay in the arena, as a replaced
//! leaf's do).
//!
//! **Follow-up** (§13 3a). If the commit leaves `P` empty or holding a single
//! leaf, the remover releases every latch and compacts `P` as a new top-down
//! operation, then `P`'s parent, and so on up, holding at most two latches at
//! a time. A prefix chain of single-child `Node4`s collapses with its subtree.
//! Follow-ups never merge: a node left with a single *inner* child is a chain
//! link or a declined merge, and stays.
//!
//! Every intermediate state (an empty node, a node holding one leaf, a leaf in
//! a shallower slot than it needs) is one that readers, writers and cursors
//! already handle, so no reader depends on compaction for correctness. At
//! quiescence, [`validate`](RawTree::validate) checks that it completed.

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::ptr::NonNull;

use crate::latch::{AbortOnUnwind, SpinBackoff, WriteGuard};
use crate::raw::node::{NodeHeader, PrefixSnapshot, Rest, Taken, MAX_PREFIX_LEN};
use crate::raw::slot::{AtomicSlot, Slot};
use crate::raw::write::{Parent, ParentGuard};
use crate::raw::{LeafNode, Raw, RawTree, Storage};

/// The result of a collapsing remove's latched section.
enum Collapse {
    /// A failed acquisition. Nothing was written; retry from the root.
    Retry,
    /// Done. `true` if the commit left the parent empty or holding a single
    /// leaf, so it needs a follow-up.
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

/// The prefix of `N` merged into its child `C` at `byte`, if it fits.
fn merged_prefix(
    n: &PrefixSnapshot,
    byte: u8,
    c: &PrefixSnapshot,
) -> Option<([u8; MAX_PREFIX_LEN], usize)> {
    let len = n.len + 1 + c.len;
    if len > MAX_PREFIX_LEN {
        return None;
    }
    let mut bytes = [0u8; MAX_PREFIX_LEN];
    bytes[..n.len].copy_from_slice(n.as_slice());
    bytes[n.len] = byte;
    bytes[n.len + 1..len].copy_from_slice(c.as_slice());
    Some((bytes, len))
}

/// `true` if a node holding `rest` should be compacted by a follow-up.
#[inline]
fn needs_follow_up<R>(rest: &Rest<R>) -> bool {
    matches!(rest, Rest::Empty | Rest::Leaf(_))
}

impl<S: Storage> RawTree<S> {
    /// Removes the leaf for `key`, if `matches` accepts it, and compacts the
    /// nodes it leaves empty or holding a single entry (§13). Returns the leaf
    /// unlinked, marked removed and already retired (§9.4).
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
                let left = node.rest(taken);
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
                let collapse = match left {
                    Rest::Many => false,
                    Rest::Empty | Rest::Leaf(_) => true,
                    // A hint (the merge re-checks under `C`'s latch): a
                    // prefix chain link never fits, and locks only `N`.
                    Rest::Inner(_, c) => {
                        // SAFETY: a protected inner node read from a
                        // validated slot.
                        let c = unsafe { self.node_ref(c) };
                        prefix.len + 1 + c.prefix_len() <= MAX_PREFIX_LEN
                    }
                };
                if collapse {
                    match self.remove_collapsing(parent, node_raw, v, taken, leaf, left, guard) {
                        Collapse::Retry => {
                            backoff.spin();
                            continue 'retry;
                        }
                        Collapse::Done(follow_up) => {
                            if follow_up {
                                self.compact(key, guard);
                            }
                            return Some(leaf);
                        }
                    }
                }
                // `N` keeps two or more entries (or an unmergeable child): lock
                // only `N`. Its absolute path cannot change without bumping its
                // version (Inv 7).
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

    /// The latched part of a remove that leaves `N` (`node_raw`, read at
    /// version `v`) holding only `left`. Takes `P`, then `N`, then `C`, per
    /// the module docs.
    #[allow(clippy::too_many_arguments)]
    fn remove_collapsing(
        &self,
        parent: Parent<Raw<S>>,
        node_raw: Raw<S>,
        v: u64,
        taken: Taken,
        leaf: NonNull<S::Leaf>,
        left: Rest<Raw<S>>,
        guard: &S::Guard,
    ) -> Collapse {
        // SAFETY: protected inner node read from the tree in this operation.
        let node = unsafe { self.node_ref(node_raw) };
        crate::hooks::pause(crate::hooks::Point::WriterBeforeUpgrade);
        let Some(pg) = self.lock_parent(parent, node_raw) else {
            return Collapse::Retry;
        };
        #[cfg(loom)]
        if crate::latch::mutants::COLLAPSE_WITHOUT_NODE_LATCH.with(|m| m.get())
            && !matches!(left, Rest::Inner(..))
        {
            // The mutant: unlink `N` holding only `P`, so a writer that locks
            // only `N` can still write into it.
            if !node.latch.validate(v) {
                return Collapse::Retry;
            }
            match left {
                Rest::Empty => self.unlink_in_parent(&pg),
                Rest::Leaf(l) => self.publish_in_parent(&pg, l),
                _ => unreachable!(),
            }
            // SAFETY: protected leaf read from a validated slot.
            unsafe { leaf.as_ref() }.mark_removed();
            self.len_add(-1);
            return Collapse::Done(false);
        }
        let Some(nw) = node.latch.try_upgrade(v) else {
            drop(pg);
            return Collapse::Retry;
        };
        // `left` was read at `v`, which the upgrade just confirmed: it is exact.
        debug_assert_eq!(node.rest(taken), left);
        let merge = match left {
            Rest::Inner(byte, c_raw) => {
                // SAFETY: a child of a node whose latch we hold.
                let c = unsafe { self.node_ref(c_raw) };
                // Below a held latch only by a version-checked upgrade
                // (Inv 7); on failure the merge is declined, not retried.
                c.latch
                    .read_version()
                    .and_then(|cv| c.latch.try_upgrade(cv))
                    .and_then(|cw| {
                        // Exact now: `N` and `C` are both latched.
                        let (bytes, len) =
                            merged_prefix(&node.load_prefix(), byte, &c.load_prefix())?;
                        Some((c, cw, bytes, len))
                    })
            }
            _ => None,
        };
        if matches!(left, Rest::Inner(..)) && merge.is_none() {
            // Declined: take the leaf out and keep `N` with its single child.
            let bomb = AbortOnUnwind;
            take_out(node, &nw, taken);
            // SAFETY: protected leaf read from a validated slot.
            unsafe { leaf.as_ref() }.mark_removed();
            self.len_add(-1);
            bomb.defuse();
            drop(nw);
            drop(pg);
            // SAFETY: unlinked under the node latch and marked.
            unsafe { self.storage.retire_leaf(leaf, guard) };
            return Collapse::Done(false);
        }

        let bomb = AbortOnUnwind;
        // The store that unlinks `N`, and with it the removed leaf.
        match left {
            Rest::Empty => self.unlink_in_parent(&pg),
            Rest::Leaf(l) => self.publish_in_parent(&pg, l),
            Rest::Inner(_, c_raw) => {
                let Some((c, cw, bytes, len)) = merge else {
                    unreachable!("a declined merge returned above")
                };
                // Inv 7: `C`'s depth changes under its own latch, and its
                // version is bumped before `P` is released.
                c.store_prefix(&cw, &bytes[..len]);
                self.publish_in_parent(&pg, c_raw);
                #[cfg(loom)]
                if crate::latch::mutants::MERGE_WITHOUT_CHILD_BUMP.with(|m| m.get()) {
                    cw.unlock_unbumped();
                } else {
                    drop(cw);
                }
                #[cfg(not(loom))]
                drop(cw);
            }
            Rest::Many => unreachable!("only a remove leaving N one entry collapses"),
        }
        // SAFETY: protected leaf read from a validated slot.
        unsafe { leaf.as_ref() }.mark_removed();
        self.len_add(-1);
        // Inv 7: unlinked, so obsolete, before the parent is released.
        nw.mark_obsolete();
        bomb.defuse();
        let follow_up = match &pg {
            ParentGuard::Node(p, ..) => needs_follow_up(&p.rest(Taken::Nothing)),
            ParentGuard::Root(_) => false,
        };
        drop(pg);
        // SAFETY: unlinked under the parent latch and marked.
        unsafe { self.storage.retire_leaf(leaf, guard) };
        // SAFETY: unlinked under the parent latch and obsolete; retired once.
        unsafe { self.storage.retire_node(self.node_ptr(node_raw), guard) };
        Collapse::Done(follow_up)
    }

    /// The follow-up (§13 3a): while the last inner node on `key`'s path is
    /// empty or holds a single leaf, unlinks it (or replaces it with that
    /// leaf) in its parent, then moves up to the parent.
    ///
    /// A new top-down operation: it holds nothing on entry, descends from the
    /// root, and restarts on any failed acquisition. Each step up takes the
    /// parent by `lock()` and a pointer re-check, and the node by
    /// `try_upgrade` of the version its own previous step unlocked it at, so
    /// a chain collapses without a re-descent per link. Every step unlinks a
    /// node, so the operation ends.
    pub(super) fn compact(&self, key: &[u8], guard: &S::Guard) {
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
            let (mut x_raw, mut xv, mut left) = loop {
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
                let left = node.rest(Taken::Nothing);
                if !node.latch.validate(v) {
                    continue 'retry;
                }
                break (node_raw, v, left);
            };
            // Compact upwards while the node at hand needs it.
            while needs_follow_up(&left) {
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
                debug_assert_eq!(x.rest(Taken::Nothing), left);
                let bomb = AbortOnUnwind;
                match left {
                    Rest::Empty => self.unlink_in_parent(&pg),
                    Rest::Leaf(l) => self.publish_in_parent(&pg, l),
                    _ => unreachable!("only empty and single-leaf nodes are followed up"),
                }
                xw.mark_obsolete();
                bomb.defuse();
                let above = match &pg {
                    ParentGuard::Node(p, ..) => p.rest(Taken::Nothing),
                    ParentGuard::Root(_) => Rest::Many,
                };
                let unlocked = pg.unlock();
                // SAFETY: unlinked under the parent latch and obsolete; retired once.
                unsafe { self.storage.retire_node(self.node_ptr(x_raw), guard) };
                let (Some(pv), Some((p_raw, _, _))) = (unlocked, path.pop()) else {
                    return;
                };
                x_raw = p_raw;
                xv = pv;
                left = above;
            }
            return;
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use crate::raw::heap::{NODE_COUNTS, RETIRED_NODES};
    use crate::raw::node::{NodeHeader, Rest, Taken};
    use crate::raw::slot::Slot;
    use crate::raw::{Layout, Mode};
    use crate::tree::{Storage, Tree};

    type T = Tree<Vec<u8>, u64>;

    fn tree(keys: &[&[u8]]) -> T {
        let t = T::new();
        let g = &crate::guard::pin();
        for k in keys {
            t.insert(k.to_vec(), 0, Mode::Replace, g);
        }
        t
    }

    fn remove(t: &T, k: &[u8]) -> bool {
        t.remove(k, &crate::guard::pin()).is_some()
    }

    fn retired() -> usize {
        RETIRED_NODES.with(|c| c.get())
    }

    fn root_node(t: &T) -> &NodeHeader<<Storage<Vec<u8>, u64> as Layout>::Atomic> {
        let r = t.raw.root();
        assert!(!r.is_null() && !r.is_leaf(), "the root is an inner node");
        // SAFETY: the test's tree is quiescent and alive for the borrow.
        unsafe { t.raw.node_ref(r) }
    }

    fn get(t: &T, k: &[u8]) -> bool {
        let _g = crate::guard::pin();
        t.raw.get(k).is_some()
    }

    #[test]
    fn rest_finds_what_a_node_keeps() {
        let t = tree(&[b"ka", b"kb", b"k"]);
        let n = root_node(&t);
        assert_eq!(n.rest(Taken::Nothing), Rest::Many);
        assert!(matches!(n.rest(Taken::Exact), Rest::Many));
        assert!(matches!(n.rest(Taken::Child(b'a')), Rest::Many));
        drop(t);
        let t = tree(&[b"ka", b"k"]);
        let n = root_node(&t);
        assert!(matches!(n.rest(Taken::Exact), Rest::Leaf(l) if l.is_leaf()));
        assert!(matches!(n.rest(Taken::Child(b'a')), Rest::Leaf(l) if l == n.exact_leaf()));
        drop(t);
        let t = tree(&[b"ka1", b"ka2", b"kb"]);
        let n = root_node(&t);
        assert!(matches!(n.rest(Taken::Child(b'b')), Rest::Inner(b'a', c) if !c.is_leaf()));
    }

    #[test]
    fn a_node_left_with_one_leaf_is_replaced_by_it() {
        let mut t = tree(&[b"a1", b"a2", b"b"]);
        let before = retired();
        assert!(remove(&t, b"a1"));
        assert_eq!(retired() - before, 1, "the 'a' node is retired");
        let child = root_node(&t).find_child(b'a').unwrap();
        assert!(child.is_leaf(), "a2 moved up into the root");
        assert!(get(&t, b"a2") && get(&t, b"b") && !get(&t, b"a1"));
        assert_eq!(t.raw.validate(), 2);
    }

    #[test]
    fn the_root_collapses_to_its_last_leaf_then_to_null() {
        let mut t = tree(&[b"ka", b"kb"]);
        assert!(remove(&t, b"ka"));
        assert!(t.raw.root().is_leaf(), "the root is the last leaf");
        assert_eq!(t.raw.validate(), 1);
        assert!(remove(&t, b"kb"));
        assert!(t.raw.root().is_null());
        assert_eq!(t.raw.validate(), 0);
    }

    #[test]
    fn exact_leaves_collapse_like_children() {
        // The exact leaf is the survivor, then the removed entry.
        let mut t = tree(&[b"k", b"ka"]);
        assert!(remove(&t, b"ka"));
        assert!(t.raw.root().is_leaf());
        assert!(get(&t, b"k"));
        let mut t2 = tree(&[b"k", b"ka"]);
        assert!(remove(&t2, b"k"));
        assert!(t2.raw.root().is_leaf());
        assert!(get(&t2, b"ka"));
        assert_eq!(t.raw.validate() + t2.raw.validate(), 2);
    }

    #[test]
    fn a_node_left_with_one_inner_child_is_merged_into_it() {
        // Root "a" {x: "ax", b: C "c" {1, 2}}.
        let mut t = tree(&[b"ax", b"abc1", b"abc2"]);
        assert_eq!(root_node(&t).load_prefix().as_slice(), b"a");
        let old_root = t.raw.root();
        let before = retired();
        assert!(remove(&t, b"ax"));
        assert_eq!(
            retired() - before,
            1,
            "only the merged-away node is retired"
        );
        assert_ne!(t.raw.root(), old_root);
        let root = root_node(&t);
        assert_eq!(root.load_prefix().as_slice(), b"abc", "prefixes are joined");
        assert!(get(&t, b"abc1") && get(&t, b"abc2") && !get(&t, b"ax"));
        // Inserting under the merged prefix splits it again.
        t.insert(b"ab".to_vec(), 1, Mode::Replace, &crate::guard::pin());
        t.insert(b"az".to_vec(), 1, Mode::Replace, &crate::guard::pin());
        assert!(get(&t, b"ab") && get(&t, b"az") && get(&t, b"abc1"));
        assert_eq!(t.raw.validate(), 4);
    }

    #[test]
    fn a_merge_whose_prefix_does_not_fit_is_declined() {
        // N's prefix is 10 bytes and C's is 10: 21 > MAX_PREFIX_LEN.
        let n = b"nnnnnnnnnn";
        let c = b"cccccccccc";
        let k = |tail: &[u8]| [&n[..], tail].concat();
        let deep = |last: u8| [&n[..], b"b", &c[..], &[last]].concat();
        let mut t = tree(&[&k(b"x"), &deep(1), &deep(2)]);
        let root = t.raw.root();
        let before = retired();
        assert!(remove(&t, &k(b"x")));
        assert_eq!(retired(), before, "nothing is unlinked");
        assert_eq!(t.raw.root(), root);
        assert!(matches!(
            root_node(&t).rest(Taken::Nothing),
            Rest::Inner(b'b', _)
        ));
        assert!(get(&t, &deep(1)) && get(&t, &deep(2)));
        assert_eq!(t.raw.validate(), 2);
    }

    #[test]
    fn a_prefix_chain_collapses_with_its_subtree() {
        // 100 shared bytes: a chain of single-child Node4s above the fork.
        let key = |last: u8| {
            let mut k = vec![7u8; 100];
            k.push(last);
            k
        };
        let mut t = tree(&[&key(1), &key(2)]);
        let chain = {
            let mut n = 0;
            let mut raw = t.raw.root();
            while !raw.is_leaf() {
                n += 1;
                // SAFETY: quiescent tree, alive for the borrow.
                let node = unsafe { t.raw.node_ref(raw) };
                raw = node.next_child(0).unwrap().1;
            }
            n
        };
        assert!(chain >= 6, "a chain of {chain} nodes");
        let before = retired();
        assert!(remove(&t, &key(1)));
        assert_eq!(retired() - before, chain, "the whole chain is retired");
        assert!(t.raw.root().is_leaf(), "the survivor is the root");
        assert_eq!(t.raw.validate(), 1);
    }

    #[test]
    fn a_collapse_inside_a_chain_follows_up_to_the_first_fork() {
        // A fork above a 40-byte chain: the chain goes, the fork stays.
        let key = |mid: u8, last: u8| {
            let mut k = vec![mid];
            k.extend_from_slice(&[9u8; 40]);
            k.push(last);
            k
        };
        let mut t = tree(&[&key(1, 1), &key(1, 2), &key(2, 0)]);
        assert!(remove(&t, &key(1, 1)));
        let root = root_node(&t);
        assert!(root.find_child(1).unwrap().is_leaf());
        assert!(root.find_child(2).unwrap().is_leaf());
        assert_eq!(t.raw.validate(), 2);
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
