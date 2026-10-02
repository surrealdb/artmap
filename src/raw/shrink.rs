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

//! # Fitting the tree to its entries (§13)
//!
//! Removes unlink only the nodes they empty, so a node can be left with a
//! single entry, or in a layout sized for far more entries than it holds: a
//! `Node256` that lost all but one child keeps its 2 KiB.
//! [`shrink_to_fit`](RawTree::shrink_to_fit) walks the tree once, children
//! before parents, and fits each node `N`:
//!
//! - no entry: `N` is unlinked;
//! - one leaf (a child or the exact leaf): the leaf takes `N`'s place;
//! - one inner child `C`, when `N.prefix + byte + C.prefix` fits in
//!   `MAX_PREFIX_LEN`: a copy of `C` with that prefix takes `N`'s place;
//! - otherwise, when a smaller layout holds `N`'s children: a copy of `N` in
//!   the smallest one takes its place.
//!
//! Children come first, so a node whose last children collapse into leaves
//! collapses in the same pass.
//!
//! **Protocol** (Inv 7), as growth: a replacement is allocated outside every
//! latch; then the parent `P` (or `root_latch`) is taken by `lock()` while
//! holding nothing and re-checked, `N` by `try_upgrade`, and for a merge `C`
//! by `try_upgrade` too. Under the latches the copy is exact. The commit
//! publishes in `P` and marks `N` (and `C`) obsolete before `P` is released,
//! and they are retired afterwards. Nothing published is rewritten in place:
//! a reader or writer inside a replaced node fails its next validation or
//! upgrade and restarts.
//!
//! The walk is best effort under concurrent writes: a node whose latches it
//! cannot take after a few tries, or whose parent no longer points at it, is
//! left as it is. It allocates, so only the heap maps offer it: in an arena
//! the smaller copies would only add to what the arena holds.

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use crate::latch::{AbortOnUnwind, SpinBackoff, WriteGuard};
use crate::raw::node::{NodeHeader, NodeType, PrefixSnapshot, Rest, Taken, MAX_PREFIX_LEN};
use crate::raw::slot::Slot;
use crate::raw::write::Parent;
use crate::raw::{Prepared, Raw, RawTree, Storage};

/// How often a node's latches are tried before it is left as it is.
const ATTEMPTS: usize = 8;

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

/// One node of the walk.
#[derive(Copy, Clone)]
struct Frame<R> {
    parent: Parent<R>,
    node: R,
    /// Its children are already on the stack: fit it when popped.
    expanded: bool,
}

/// What fitting a node does, decided from an optimistic read and confirmed by
/// the upgrade at that read's version.
#[derive(Copy, Clone)]
enum Plan<R> {
    Unlink,
    Leaf(R),
    Merge { byte: u8, child: R, ty: NodeType },
    Shrink(NodeType),
}

impl<S: Storage> RawTree<S> {
    /// Fits every node to its entries; see the module docs. The caller has
    /// pinned before calling (Inv 6), and the walk runs under that one pin.
    pub(crate) fn shrink_to_fit(&self, guard: &S::Guard) {
        let root = self.root();
        if root.is_null() || root.is_leaf() {
            return;
        }
        let mut prep = Prepared::<S>::new();
        let mut stack = vec![Frame {
            parent: Parent::Root,
            node: root,
            expanded: false,
        }];
        while let Some(f) = stack.pop() {
            if f.expanded {
                if self.fit(f.parent, f.node, &mut prep, guard).is_err() {
                    break;
                }
                continue;
            }
            // SAFETY: an inner node read from the tree during this pin.
            let node = unsafe { self.node_ref(f.node) };
            let Some((v, children)) = inner_children(node) else {
                // Replaced or kept busy: its subtree is left as it is.
                continue;
            };
            stack.push(Frame {
                expanded: true,
                ..f
            });
            for (byte, c) in children.into_iter().rev() {
                stack.push(Frame {
                    parent: Parent::Node {
                        raw: f.node,
                        version: v,
                        byte,
                    },
                    node: c,
                    expanded: false,
                });
            }
        }
        prep.release(&self.storage);
    }

    /// Fits one node, per the module docs. `Err` if allocation failed.
    fn fit(
        &self,
        parent: Parent<Raw<S>>,
        n_raw: Raw<S>,
        prep: &mut Prepared<S>,
        guard: &S::Guard,
    ) -> Result<(), S::Full> {
        // SAFETY: an inner node read from the tree during this pin.
        let node = unsafe { self.node_ref(n_raw) };
        let mut backoff = SpinBackoff::new();
        for _ in 0..ATTEMPTS {
            prep.reset();
            let Some(v) = node.latch.read_version() else {
                if node.latch.is_obsolete() {
                    return Ok(());
                }
                backoff.spin();
                continue;
            };
            let plan = match node.rest(Taken::Nothing) {
                Rest::Empty => Some(Plan::Unlink),
                Rest::Leaf(l) => Some(Plan::Leaf(l)),
                Rest::Inner(byte, child) => {
                    // SAFETY: a non-null child read from `N` (R3); a hint,
                    // confirmed under the latches.
                    let c = unsafe { self.node_ref(child) };
                    if node.prefix_len() + 1 + c.prefix_len() <= MAX_PREFIX_LEN {
                        Some(Plan::Merge {
                            byte,
                            child,
                            ty: NodeType::fitting(c.num_children()),
                        })
                    } else {
                        // A prefix chain link, or a merge that would not fit.
                        (node.node_type > NodeType::Node4).then_some(Plan::Shrink(NodeType::Node4))
                    }
                }
                Rest::Many => {
                    let ty = NodeType::fitting(node.num_children());
                    (ty < node.node_type).then_some(Plan::Shrink(ty))
                }
            };
            if !node.latch.validate(v) {
                backoff.spin();
                continue;
            }
            let Some(plan) = plan else {
                return Ok(());
            };
            // Prepare, outside every latch: the replacement node.
            let fresh = match plan {
                Plan::Merge { ty, .. } | Plan::Shrink(ty) => Some(prep.node(&self.storage, ty)?),
                Plan::Unlink | Plan::Leaf(_) => None,
            };
            crate::hooks::pause(crate::hooks::Point::WriterBeforeUpgrade);
            let Some(pg) = self.lock_parent(parent, n_raw) else {
                // `N` moved or is gone: leave it.
                return Ok(());
            };
            #[cfg(loom)]
            let unlatched = matches!(plan, Plan::Shrink(_))
                && crate::latch::mutants::FIT_WITHOUT_NODE_LATCH.with(|m| m.get());
            #[cfg(not(loom))]
            let unlatched = false;
            let nw = if unlatched {
                // The mutant: copy `N` holding only `P`.
                None
            } else {
                match node.latch.try_upgrade(v) {
                    Some(w) => Some(w),
                    None => {
                        drop(pg);
                        backoff.spin();
                        continue;
                    }
                }
            };
            // `N` is unchanged since `v`: the plan is exact.
            let mut cw: Option<WriteGuard<'_>> = None;
            let replacement = match plan {
                Plan::Unlink => None,
                Plan::Leaf(l) => Some(l),
                Plan::Shrink(_) => {
                    let f = fresh.expect("allocated above");
                    // SAFETY: an unpublished node from the pool, exclusively
                    // ours; `N`'s latch makes the copy exact, and its type was
                    // chosen for `N`'s children at `v`.
                    node.copy_into(unsafe { &mut *f.as_ptr() });
                    Some(self.storage.node_raw(f))
                }
                Plan::Merge { byte, child, ty } => {
                    // SAFETY: a child of a node whose latch we hold.
                    let c = unsafe { self.node_ref(child) };
                    #[cfg(loom)]
                    let unlatched_child =
                        crate::latch::mutants::MERGE_WITHOUT_CHILD_LATCH.with(|m| m.get());
                    #[cfg(not(loom))]
                    let unlatched_child = false;
                    if !unlatched_child {
                        // Below held latches only by a version-checked upgrade
                        // (Inv 7).
                        match c
                            .latch
                            .read_version()
                            .and_then(|cv| c.latch.try_upgrade(cv))
                        {
                            Some(w) => cw = Some(w),
                            None => {
                                drop(nw);
                                drop(pg);
                                backoff.spin();
                                continue;
                            }
                        }
                    }
                    // Exact now that `N` and `C` are latched.
                    let merged = merged_prefix(&node.load_prefix(), byte, &c.load_prefix());
                    let (Some((bytes, len)), true) = (merged, c.num_children() <= ty.capacity())
                    else {
                        // `C` changed since the plan: plan again.
                        drop(cw);
                        drop(nw);
                        drop(pg);
                        backoff.spin();
                        continue;
                    };
                    let f = fresh.expect("allocated above");
                    // SAFETY: an unpublished node from the pool, exclusively
                    // ours; `C`'s latch makes the copy exact.
                    let dst = unsafe { &mut *f.as_ptr() };
                    c.copy_into(dst);
                    dst.init_prefix(&bytes[..len]);
                    Some(self.storage.node_raw(f))
                }
            };
            let bomb = AbortOnUnwind;
            match replacement {
                None => self.unlink_in_parent(&pg),
                Some(r) => self.publish_in_parent(&pg, r),
            }
            // Inv 7: replaced, so obsolete, before the parent is released.
            if let Some(cw) = cw {
                cw.mark_obsolete();
            }
            if let Some(nw) = nw {
                nw.mark_obsolete();
            }
            bomb.defuse();
            drop(pg);
            prep.commit();
            if let Plan::Merge { child, .. } = plan {
                // SAFETY: replaced under the parent latch and obsolete; retired once.
                unsafe { self.storage.retire_node(self.node_ptr(child), guard) };
            }
            // SAFETY: as above.
            unsafe { self.storage.retire_node(self.node_ptr(n_raw), guard) };
            return Ok(());
        }
        Ok(())
    }
}

/// A node's inner children, as `(byte, child)`.
type Children<R> = Vec<(u8, R)>;

/// The inner children of `node`, read optimistically and validated, with the
/// version they were read at. `None` if the node is obsolete or stays busy.
fn inner_children<A: crate::raw::slot::AtomicSlot>(
    node: &NodeHeader<A>,
) -> Option<(u64, Children<A::Raw>)> {
    let mut backoff = SpinBackoff::new();
    for _ in 0..ATTEMPTS {
        let Some(v) = node.latch.read_version() else {
            if node.latch.is_obsolete() {
                return None;
            }
            backoff.spin();
            continue;
        };
        let mut children = Vec::new();
        node.for_each_child(|b, c| {
            if !c.is_leaf() {
                children.push((b, c));
            }
        });
        if node.latch.validate(v) {
            return Some((v, children));
        }
        backoff.spin();
    }
    None
}

#[cfg(all(test, not(loom)))]
mod tests {
    use crate::raw::heap::NODE_COUNTS;
    use crate::raw::node::{NodeType, Rest, Taken, MAX_PREFIX_LEN};
    use crate::raw::remove::tests::{
        chain_len, get, long_key, remove, retired, root_node, tree, Header, T,
    };
    use crate::raw::slot::Slot;
    use crate::raw::Mode;

    fn shrink(t: &T) {
        t.raw.shrink_to_fit(&crate::guard::pin());
    }

    /// Asserts that a quiescent tree is fitted: no node holds a single leaf
    /// or a mergeable single child, and every node has the smallest layout
    /// for its children.
    fn assert_fitted(t: &T) {
        let mut work = vec![t.raw.root()];
        while let Some(raw) = work.pop() {
            if raw.is_null() || raw.is_leaf() {
                continue;
            }
            // SAFETY: quiescent tree, alive for the borrow.
            let n: &Header = unsafe { t.raw.node_ref(raw) };
            match n.rest(Taken::Nothing) {
                Rest::Empty | Rest::Leaf(_) => panic!("a node with one leaf or none"),
                Rest::Inner(_, c) => {
                    // SAFETY: as above.
                    let c = unsafe { t.raw.node_ref(c) };
                    assert!(
                        n.prefix_len() + 1 + c.prefix_len() > MAX_PREFIX_LEN,
                        "a mergeable single child"
                    );
                    assert_eq!(n.node_type, NodeType::Node4);
                }
                Rest::Many => assert_eq!(n.node_type, NodeType::fitting(n.num_children())),
            }
            n.for_each_child(|_, c| work.push(c));
        }
    }

    #[test]
    fn every_layout_shrinks_to_fit() {
        // One node with 256 children: [0, i].
        let mut t = T::new();
        let g = &crate::guard::pin();
        for i in 0..=255u8 {
            t.insert(vec![0, i], i as u64, Mode::Replace, g);
        }
        assert_eq!(root_node(&t).node_type, NodeType::Node256);
        let mut left = 256usize;
        for (keep, ty) in [
            (40, NodeType::Node48),
            (10, NodeType::Node16),
            (3, NodeType::Node4),
        ] {
            for i in keep..left {
                assert!(t.remove(&[0, i as u8], g).is_some());
            }
            left = keep;
            let before = retired();
            shrink(&t);
            assert_eq!(retired() - before, 1, "the old node is retired");
            assert_eq!(root_node(&t).node_type, ty, "{keep} children");
            for i in 0..keep {
                assert!(get(&t, &[0, i as u8]));
            }
            assert_eq!(t.raw.validate(), keep);
            assert_fitted(&t);
        }
        // Already fitted: nothing changes.
        let before = retired();
        shrink(&t);
        assert_eq!(retired(), before);
        // Down to one key: the node gives way to its leaf.
        assert!(t.remove(&[0, 1], g).is_some() && t.remove(&[0, 2], g).is_some());
        shrink(&t);
        assert!(t.raw.root().is_leaf());
        assert!(get(&t, &[0, 0]));
        assert_eq!(t.raw.validate(), 1);
    }

    #[test]
    fn a_node_with_one_leaf_gives_way_to_it() {
        let mut t = tree(&[b"a1", b"a2", b"b"]);
        assert!(remove(&t, b"a2"));
        shrink(&t);
        assert!(root_node(&t).find_child(b'a').unwrap().is_leaf());
        assert!(get(&t, b"a1") && get(&t, b"b"));
        // Exact leaves too.
        let mut t2 = tree(&[b"k", b"ka", b"x"]);
        assert!(remove(&t2, b"ka"));
        shrink(&t2);
        assert!(root_node(&t2).find_child(b'k').unwrap().is_leaf());
        assert!(get(&t2, b"k"));
        assert_eq!(t.raw.validate() + t2.raw.validate(), 4);
        assert_fitted(&t);
        assert_fitted(&t2);
    }

    #[test]
    fn a_node_with_one_child_node_merges_into_it() {
        // Root "a" {x, b: C "c" {1, 2}}: without "ax", the root merges into C.
        let mut t = tree(&[b"ax", b"abc1", b"abc2"]);
        assert!(remove(&t, b"ax"));
        assert_eq!(root_node(&t).load_prefix().as_slice(), b"a");
        let before = retired();
        shrink(&t);
        assert_eq!(retired() - before, 2, "the root and C are retired");
        assert_eq!(root_node(&t).load_prefix().as_slice(), b"abc");
        assert!(get(&t, b"abc1") && get(&t, b"abc2") && !get(&t, b"ax"));
        assert_fitted(&t);
        // Inserting under the merged prefix splits it again.
        let g = &crate::guard::pin();
        for k in [&b"ab"[..], b"az", b"abc3"] {
            t.insert(k.to_vec(), 1, Mode::Replace, g);
        }
        assert!(get(&t, b"ab") && get(&t, b"az") && get(&t, b"abc3") && get(&t, b"abc1"));
        assert_eq!(t.raw.validate(), 5);
    }

    #[test]
    fn single_entries_collapse_in_one_pass() {
        // Root {x: X {a: A {1}, …}, y}: once A holds one leaf and X holds
        // only A, both give way to the leaf, children first.
        let mut t = tree(&[b"xa1", b"xa2", b"xb", b"y"]);
        assert!(remove(&t, b"xa2") && remove(&t, b"xb"));
        let before = retired();
        shrink(&t);
        assert_eq!(retired() - before, 2);
        assert!(root_node(&t).find_child(b'x').unwrap().is_leaf());
        assert!(get(&t, b"xa1") && get(&t, b"y"));
        assert_eq!(t.raw.validate(), 2);
        assert_fitted(&t);
    }

    #[test]
    fn chain_links_that_cannot_merge_stay() {
        // Each link holds 16 prefix bytes: a merge never fits.
        let t = tree(&[&long_key(1), &long_key(2), &long_key(3)]);
        let chain = chain_len(&t);
        let before = retired();
        shrink(&t);
        assert_eq!(retired(), before, "nothing to fit");
        assert_eq!(chain_len(&t), chain);
        assert_fitted(&t);
        // Down to one key, the whole chain gives way to it.
        assert!(remove(&t, &long_key(1)) && remove(&t, &long_key(2)));
        shrink(&t);
        assert!(t.raw.root().is_leaf());
        assert_eq!(retired() - before, chain);
    }

    #[test]
    fn an_empty_or_single_leaf_tree_is_left_alone() {
        let t = T::new();
        shrink(&t);
        assert!(t.raw.root().is_null());
        let t = tree(&[b"k"]);
        shrink(&t);
        assert!(t.raw.root().is_leaf());
    }

    #[test]
    fn a_thinned_tree_returns_its_nodes() {
        use rand::{Rng, SeedableRng};
        let live_nodes =
            || NODE_COUNTS.with(|c| c.allocated.get() - c.freed.get() - c.retired.get());
        let mut rng = rand::rngs::StdRng::seed_from_u64(11);
        let n = if cfg!(miri) { 400 } else { 50_000 };
        let base = live_nodes();
        let mut t = T::new();
        let g = &crate::guard::pin();
        let keys: Vec<Vec<u8>> = (0..n).map(|_| rng.gen::<[u8; 8]>().to_vec()).collect();
        for k in &keys {
            t.insert(k.clone(), 0, Mode::Replace, g);
        }
        // Keep one key in a hundred.
        for k in keys.iter().filter(|_| !rng.gen_ratio(1, 100)) {
            t.remove(k, g);
        }
        let thinned = live_nodes() - base;
        shrink(&t);
        let fitted = live_nodes() - base;
        assert!(
            fitted < thinned,
            "{fitted} nodes after fitting, {thinned} before"
        );
        assert_fitted(&t);
        let live = t.raw.validate();
        assert!(fitted < live, "{fitted} nodes for {live} keys");
    }
}
