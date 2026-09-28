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

//! # Writer algorithms (§9)
//!
//! Every write follows the same structure (§9.1):
//!
//! 1. the caller pins before any latch (Inv 6);
//! 2. optimistic descent with coupling (R1–R6), comparing existing leaf keys
//!    before any latch (user code never runs under a latch);
//! 3. prepare: allocate unpublished nodes from the [`Prepared`] pool;
//! 4. acquire: the parent (or `root_latch`) with a blocking `lock()` while
//!    holding nothing, followed by a pointer re-check; the target node with a
//!    version-checked `try_upgrade`;
//! 5. commit, inside [`AbortOnUnwind`]: `len` accounting before the
//!    publishing store (Inv 12), `Release` stores, unlock or mark obsolete;
//! 6. after every latch is released: retire displaced objects.
//!
//! A failed upgrade or re-check retries from the root. There is no lock-free
//! insert path (Inv 8, §9.9): every publication happens under the owning latch.

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::ptr::NonNull;

use crate::latch::{AbortOnUnwind, SpinBackoff, WriteGuard};
use crate::raw::node::{NodeHeader, NodeType, MAX_PREFIX_LEN};
use crate::raw::slot::Slot;
use crate::raw::{
    common_prefix, LeafNode, Mode, NodePtr, Outcome, Prepared, Raw, RawTree, Storage, Unpublished,
};

/// Where the parent of the node being modified keeps it.
#[derive(Copy, Clone)]
enum Parent<R> {
    Root,
    Node { raw: R, version: u64, byte: u8 },
}

/// A held parent latch, re-checked to still point at the child.
enum ParentGuard<'t, A> {
    Root(WriteGuard<'t>),
    Node(&'t NodeHeader<A>, WriteGuard<'t>, u8),
}

impl<S: Storage> RawTree<S> {
    /// Acquires the latch that owns the pointer to `child` (Inv 7): blocking,
    /// while holding nothing, followed by a pointer re-check.
    fn lock_parent(
        &self,
        parent: Parent<Raw<S>>,
        child: Raw<S>,
    ) -> Option<ParentGuard<'_, S::Atomic>> {
        match parent {
            Parent::Root => {
                let w = self.root_latch.lock()?;
                (self.root() == child).then_some(ParentGuard::Root(w))
            }
            Parent::Node { raw, byte, .. } => {
                // SAFETY: `raw` was read from this tree during the current
                // pinned (or map-borrowed) operation, so the node is protected.
                let p = unsafe { self.node_ref(raw) };
                let w = p.latch.lock()?;
                (p.find_child(byte) == Some(child)).then_some(ParentGuard::Node(p, w, byte))
            }
        }
    }

    /// Publishes `new` in place of the child the parent guard protects.
    fn publish_in_parent(&self, pg: &ParentGuard<'_, S::Atomic>, new: Raw<S>) {
        match pg {
            ParentGuard::Root(w) => self.set_root(w, new),
            ParentGuard::Node(p, w, byte) => p.replace_child(w, *byte, new),
        }
    }

    /// Inserts the leaf owned by `owner`, whose key bytes (derived once, from
    /// the leaf itself, Inv 10) are `key`.
    ///
    /// `count` is the `len` delta of publishing the leaf (0 for a versioned
    /// leaf whose only version is a tombstone). On `Inserted` or `Replaced` the
    /// leaf is published and `owner` is disarmed.
    pub(crate) fn insert(
        &self,
        owner: &Unpublished<'_, S>,
        key: &[u8],
        mode: Mode,
        count: isize,
        guard: &S::Guard,
    ) -> Result<Outcome<S::Leaf>, S::Full> {
        let mut prep = Prepared::<S>::new();
        let r = self.insert_with(owner, key, mode, count, guard, &mut prep);
        prep.release(&self.storage);
        r
    }

    fn insert_with(
        &self,
        owner: &Unpublished<'_, S>,
        key: &[u8],
        mode: Mode,
        count: isize,
        guard: &S::Guard,
        prep: &mut Prepared<S>,
    ) -> Result<Outcome<S::Leaf>, S::Full> {
        let new_ptr = owner.ptr();
        let new_leaf = self.storage.leaf_raw(new_ptr);
        let mut backoff = SpinBackoff::new();
        'retry: loop {
            prep.reset();
            let root = self.root();

            // Empty tree: publish the first leaf under `root_latch` (D14).
            if root.is_null() {
                let Some(w) = self.root_latch.lock() else {
                    unreachable!("root latch is never obsolete")
                };
                if !self.root().is_null() {
                    drop(w);
                    continue 'retry;
                }
                let bomb = AbortOnUnwind;
                self.len_add(count);
                owner.disarm();
                self.set_root(&w, new_leaf);
                bomb.defuse();
                drop(w);
                return Ok(Outcome::Inserted(new_ptr));
            }

            // Root is a single leaf.
            if root.is_leaf() {
                // SAFETY: protected leaf read from `root`.
                let existing = unsafe { self.leaf_ref(root) };
                let ekey = existing.key_bytes();
                if ekey == key {
                    let old = self.storage_leaf(root);
                    if mode == Mode::InsertIfAbsent {
                        return Ok(Outcome::Existing(old));
                    }
                    let Some(w) = self.root_latch.lock() else {
                        unreachable!("root latch is never obsolete")
                    };
                    if self.root() != root {
                        drop(w);
                        continue 'retry;
                    }
                    let bomb = AbortOnUnwind;
                    owner.disarm();
                    self.set_root(&w, new_leaf);
                    existing.mark_removed();
                    bomb.defuse();
                    drop(w);
                    // SAFETY: unlinked under `root_latch` and marked; this is
                    // the only thread that unlinked it.
                    unsafe { self.storage.retire_leaf(old, guard) };
                    return Ok(Outcome::Replaced(old));
                }
                let common = common_prefix(ekey, key);
                let chain =
                    self.build_chain(prep, &key[..common], (ekey, root), (key, new_leaf), common)?;
                let Some(w) = self.root_latch.lock() else {
                    unreachable!("root latch is never obsolete")
                };
                if self.root() != root {
                    drop(w);
                    backoff.spin();
                    continue 'retry;
                }
                let bomb = AbortOnUnwind;
                self.len_add(count);
                owner.disarm();
                self.set_root(&w, chain);
                bomb.defuse();
                drop(w);
                prep.commit();
                return Ok(Outcome::Inserted(new_ptr));
            }

            // Root is an inner node: optimistic descent with coupling.
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
                crate::hooks::pause(crate::hooks::Point::WriterAfterVersion);
                // R4: couple the child to its parent.
                if !self.still_parent(parent, node_raw) {
                    continue 'retry;
                }
                let prefix = node.load_prefix();
                let rest = key.get(depth..).unwrap_or(&[]);
                let matched = if prefix.is_prefix_of(rest) {
                    prefix.len
                } else {
                    common_prefix(prefix.as_slice(), rest)
                };

                if matched < prefix.len {
                    // Prefix split (§9.6).
                    if !node.latch.validate(v) {
                        continue 'retry;
                    }
                    let split_raw = {
                        let split = prep.node(&self.storage, NodeType::Node4)?;
                        // SAFETY: unpublished node from the pool, exclusively ours.
                        let s = unsafe { &mut *split.as_ptr() };
                        s.init_prefix(&prefix.as_slice()[..matched]);
                        s.push_child_unpublished(prefix.bytes[matched], node_raw);
                        if depth + matched == key.len() {
                            s.init_exact_leaf(new_leaf);
                        } else {
                            s.push_child_unpublished(key[depth + matched], new_leaf);
                        }
                        self.storage.node_raw(split)
                    };
                    crate::hooks::pause(crate::hooks::Point::WriterBeforeUpgrade);
                    let Some(pg) = self.lock_parent(parent, node_raw) else {
                        backoff.spin();
                        continue 'retry;
                    };
                    let Some(nw) = node.latch.try_upgrade(v) else {
                        drop(pg);
                        backoff.spin();
                        continue 'retry;
                    };
                    let bomb = AbortOnUnwind;
                    // Inv 7: this node's path changes, under its own latch.
                    node.store_prefix(&nw, &prefix.as_slice()[matched + 1..]);
                    self.len_add(count);
                    owner.disarm();
                    self.publish_in_parent(&pg, split_raw);
                    bomb.defuse();
                    drop(nw);
                    drop(pg);
                    prep.commit();
                    return Ok(Outcome::Inserted(new_ptr));
                }

                let node_depth = depth + prefix.len;
                if node_depth == key.len() {
                    // The exact-leaf home.
                    let exact = node.exact_leaf();
                    if !node.latch.validate(v) {
                        continue 'retry;
                    }
                    if !exact.is_null() {
                        let old = self.storage_leaf(exact);
                        if mode == Mode::InsertIfAbsent {
                            return Ok(Outcome::Existing(old));
                        }
                        let Some(nw) = node.latch.try_upgrade(v) else {
                            backoff.spin();
                            continue 'retry;
                        };
                        let bomb = AbortOnUnwind;
                        owner.disarm();
                        node.set_exact_leaf(&nw, new_leaf);
                        // SAFETY: protected leaf read from the tree.
                        unsafe { self.leaf_ref(exact) }.mark_removed();
                        bomb.defuse();
                        drop(nw);
                        // SAFETY: unlinked under the node latch and marked.
                        unsafe { self.storage.retire_leaf(old, guard) };
                        return Ok(Outcome::Replaced(old));
                    }
                    let Some(nw) = node.latch.try_upgrade(v) else {
                        backoff.spin();
                        continue 'retry;
                    };
                    let bomb = AbortOnUnwind;
                    self.len_add(count);
                    owner.disarm();
                    node.set_exact_leaf(&nw, new_leaf);
                    bomb.defuse();
                    drop(nw);
                    return Ok(Outcome::Inserted(new_ptr));
                }

                let byte = key[node_depth];
                let child = node.find_child(byte);
                if let Some(c) = child.filter(|c| !c.is_leaf()) {
                    // Descend without validating: the next level's coupling
                    // check validates this node (R3, R4).
                    parent = Parent::Node {
                        raw: node_raw,
                        version: v,
                        byte,
                    };
                    node_raw = c;
                    depth = node_depth + 1;
                    continue;
                }
                let full = node.is_full();
                if !node.latch.validate(v) {
                    continue 'retry;
                }
                match child {
                    None if full => {
                        // Grow (§9.5): build the larger node, then publish it in
                        // the parent and obsolete this one.
                        let grown_ty = node
                            .node_type
                            .grown()
                            .expect("a full node is never a Node256");
                        let grown = prep.node(&self.storage, grown_ty)?;
                        let Some(pg) = self.lock_parent(parent, node_raw) else {
                            backoff.spin();
                            continue 'retry;
                        };
                        let Some(nw) = node.latch.try_upgrade(v) else {
                            drop(pg);
                            backoff.spin();
                            continue 'retry;
                        };
                        // Prepare, still: the copy only writes unpublished memory.
                        // SAFETY: unpublished node from the pool, exclusively ours.
                        let g = unsafe { &mut *grown.as_ptr() };
                        node.copy_into(g);
                        g.push_child_unpublished(byte, new_leaf);
                        let grown_raw = self.storage.node_raw(grown);
                        let bomb = AbortOnUnwind;
                        self.len_add(count);
                        owner.disarm();
                        self.publish_in_parent(&pg, grown_raw);
                        nw.mark_obsolete();
                        bomb.defuse();
                        drop(pg);
                        prep.commit();
                        let old = self.node_ptr(node_raw);
                        // SAFETY: unlinked under the parent latch and obsolete.
                        unsafe { self.storage.retire_node(old, guard) };
                        return Ok(Outcome::Inserted(new_ptr));
                    }
                    None => {
                        // Room in the node: lock only this node. Its absolute
                        // path cannot change without bumping its version (Inv 7).
                        crate::hooks::pause(crate::hooks::Point::WriterBeforeUpgrade);
                        let Some(nw) = node.latch.try_upgrade(v) else {
                            backoff.spin();
                            continue 'retry;
                        };
                        let bomb = AbortOnUnwind;
                        self.len_add(count);
                        owner.disarm();
                        node.insert_child(&nw, byte, new_leaf);
                        bomb.defuse();
                        drop(nw);
                        return Ok(Outcome::Inserted(new_ptr));
                    }
                    Some(c) if c.is_leaf() => {
                        // SAFETY: protected leaf read from a validated slot.
                        let existing = unsafe { self.leaf_ref(c) };
                        let ekey = existing.key_bytes();
                        if ekey == key {
                            let old = self.storage_leaf(c);
                            if mode == Mode::InsertIfAbsent {
                                return Ok(Outcome::Existing(old));
                            }
                            let Some(nw) = node.latch.try_upgrade(v) else {
                                backoff.spin();
                                continue 'retry;
                            };
                            let bomb = AbortOnUnwind;
                            owner.disarm();
                            node.replace_child(&nw, byte, new_leaf);
                            existing.mark_removed();
                            bomb.defuse();
                            drop(nw);
                            // SAFETY: unlinked under the node latch and marked.
                            unsafe { self.storage.retire_leaf(old, guard) };
                            return Ok(Outcome::Replaced(old));
                        }
                        // Leaf split: both keys continue below `byte`.
                        let from = node_depth + 1;
                        let es = ekey.get(from..).unwrap_or(&[]);
                        let ns = &key[from..];
                        let common = common_prefix(es, ns);
                        let chain =
                            self.build_chain(prep, &ns[..common], (es, c), (ns, new_leaf), common)?;
                        let Some(nw) = node.latch.try_upgrade(v) else {
                            backoff.spin();
                            continue 'retry;
                        };
                        let bomb = AbortOnUnwind;
                        self.len_add(count);
                        owner.disarm();
                        node.replace_child(&nw, byte, chain);
                        bomb.defuse();
                        drop(nw);
                        prep.commit();
                        return Ok(Outcome::Inserted(new_ptr));
                    }
                    Some(_) => unreachable!("inner children are descended above"),
                }
            }
        }
    }

    /// R4 coupling check after reading a child's version: the parent is
    /// unchanged, or (at the root) `root` still points at the child.
    #[inline]
    fn still_parent(&self, parent: Parent<Raw<S>>, child: Raw<S>) -> bool {
        match parent {
            Parent::Root => self.root() == child,
            Parent::Node { raw, version, .. } => {
                // SAFETY: protected node read from the tree in this operation.
                unsafe { self.node_ref(raw) }.latch.validate(version)
            }
        }
    }

    #[inline]
    fn storage_leaf(&self, raw: Raw<S>) -> NonNull<S::Leaf> {
        // SAFETY: `raw` is a protected leaf slot value of this tree.
        unsafe { self.storage.leaf(raw) }
    }

    #[inline]
    fn node_ptr(&self, raw: Raw<S>) -> NodePtr<S> {
        // SAFETY: `raw` is a protected inner-node slot value of this tree.
        unsafe { self.storage.node(raw) }
    }

    /// Builds, bottom-up and without recursion (Inv 13), the unpublished chain
    /// of `Node4`s that holds two leaves diverging after `prefix`.
    ///
    /// `a` and `b` are `(key suffix, leaf)` where each suffix starts at the
    /// chain's first byte; `common` is `prefix.len()`.
    fn build_chain(
        &self,
        prep: &mut Prepared<S>,
        prefix: &[u8],
        a: (&[u8], Raw<S>),
        b: (&[u8], Raw<S>),
        common: usize,
    ) -> Result<Raw<S>, S::Full> {
        debug_assert_eq!(prefix.len(), common);
        // Segments of 16 prefix bytes plus one branch byte above the bottom node.
        let mut segments = Vec::new();
        let mut rest = prefix;
        while rest.len() > MAX_PREFIX_LEN {
            segments.push((&rest[..MAX_PREFIX_LEN], rest[MAX_PREFIX_LEN]));
            rest = &rest[MAX_PREFIX_LEN + 1..];
        }
        let bottom = prep.node(&self.storage, NodeType::Node4)?;
        {
            // SAFETY: unpublished node from the pool, exclusively ours.
            let n = unsafe { &mut *bottom.as_ptr() };
            n.init_prefix(rest);
            for (suffix, leaf) in [a, b] {
                match suffix.get(common) {
                    None => n.init_exact_leaf(leaf),
                    Some(&byte) => n.push_child_unpublished(byte, leaf),
                }
            }
        }
        let mut below = self.storage.node_raw(bottom);
        for (seg_prefix, byte) in segments.into_iter().rev() {
            let n = prep.node(&self.storage, NodeType::Node4)?;
            // SAFETY: unpublished node from the pool, exclusively ours.
            let r = unsafe { &mut *n.as_ptr() };
            r.init_prefix(seg_prefix);
            r.push_child_unpublished(byte, below);
            below = self.storage.node_raw(n);
        }
        Ok(below)
    }

    /// Removes the leaf for `key`, if `matches` accepts it. Returns it
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
                if node_depth == key.len() {
                    let exact = node.exact_leaf();
                    if !node.latch.validate(v) {
                        continue 'retry;
                    }
                    if exact.is_null() {
                        return None;
                    }
                    let leaf = self.storage_leaf(exact);
                    if !matches(leaf) {
                        return None;
                    }
                    let Some(nw) = node.latch.try_upgrade(v) else {
                        backoff.spin();
                        continue 'retry;
                    };
                    let bomb = AbortOnUnwind;
                    node.set_exact_leaf(&nw, Raw::<S>::NULL);
                    // SAFETY: protected leaf read from a validated slot.
                    unsafe { leaf.as_ref() }.mark_removed();
                    self.len_add(-1);
                    bomb.defuse();
                    drop(nw);
                    // SAFETY: unlinked under the node latch and marked.
                    unsafe { self.storage.retire_leaf(leaf, guard) };
                    return Some(leaf);
                }
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
                if !node.latch.validate(v) {
                    continue 'retry;
                }
                // A leaf child (inner children were descended above).
                let leaf = self.storage_leaf(child?);
                if !matches(leaf) {
                    return None;
                }
                let Some(nw) = node.latch.try_upgrade(v) else {
                    backoff.spin();
                    continue 'retry;
                };
                let bomb = AbortOnUnwind;
                node.remove_child(&nw, byte);
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
}
