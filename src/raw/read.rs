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

//! # Optimistic point lookup (§10.1)
//!
//! ```text
//! node = root.load(Acquire); parent = None
//! loop:
//!   if node is leaf:  compare key; return          (reached through a validated slot)
//!   v = node.read_version() else restart
//!   parent: validate(parent.v) else restart        (R4 coupling)
//!   root:   root.load(Acquire) == node else restart
//!   p = node.load_prefix()                          (R2 snapshot)
//!   child = find_child(node, byte)                  (None if null, R6)
//!   validate(v) else restart
//!   None => return None                             (validated absence)
//! ```
//!
//! Readers never store to shared tree memory.

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::ptr::NonNull;

use crate::latch::SpinBackoff;
use crate::raw::slot::Slot;
use crate::raw::{LeafNode, RawTree, Storage};

impl<S: Storage> RawTree<S> {
    /// The leaf for `key`, if present. The caller must keep the result
    /// protected (a pinned guard for the heap, the map borrow for the arena).
    pub(crate) fn get(&self, key: &[u8]) -> Option<NonNull<S::Leaf>> {
        let mut backoff = SpinBackoff::new();
        'retry: loop {
            let mut cur = self.root();
            crate::hooks::pause(crate::hooks::Point::ReaderAfterRoot);
            if cur.is_null() {
                return None;
            }
            if cur.is_leaf() {
                // Loaded from `root` (Acquire).
                // SAFETY: protected leaf of this tree.
                let leaf = unsafe { self.storage.leaf(cur) };
                // SAFETY: as above.
                let k = unsafe { leaf.as_ref() }.key_bytes();
                return (k == key).then_some(leaf);
            }
            // (node, version) of the parent, for R4 coupling.
            let mut parent: Option<(&crate::raw::node::NodeHeader<S::Atomic>, u64)> = None;
            let mut depth = 0usize;
            loop {
                // SAFETY: protected inner node of this tree. Following a
                // non-null child before validating its parent is allowed (R3):
                // EBR (or the map borrow) keeps it alive and loads cannot tear.
                let node = unsafe { self.node_ref(cur) };
                let Some(v) = node.latch.read_version() else {
                    backoff.spin();
                    continue 'retry;
                };
                crate::hooks::pause(crate::hooks::Point::ReaderAfterChildVersion);
                #[cfg(loom)]
                let coupled =
                    parent.filter(|_| !crate::latch::mutants::SKIP_R4_COUPLING.with(|m| m.get()));
                #[cfg(not(loom))]
                let coupled = parent;
                // R4: the parent is validated once, after the child's version
                // is read, which both validates the parent's own reads and
                // couples the child to it.
                let still_linked = match coupled {
                    Some((p, pv)) => p.latch.validate(pv),
                    None if parent.is_none() => self.root() == cur,
                    None => true,
                };
                if !still_linked {
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
                let child = if node_depth == key.len() {
                    node.exact_leaf()
                } else {
                    node.find_child(key[node_depth])
                        .unwrap_or(crate::raw::Raw::<S>::NULL)
                };
                if child.is_null() || child.is_leaf() {
                    // A conclusion: validate this node first (R3).
                    if !node.latch.validate(v) {
                        continue 'retry;
                    }
                    if child.is_null() {
                        return None;
                    }
                    // SAFETY: a protected leaf reached through a validated slot.
                    let leaf = unsafe { self.storage.leaf(child) };
                    // SAFETY: as above.
                    let k = unsafe { leaf.as_ref() }.key_bytes();
                    return (k == key).then_some(leaf);
                }
                parent = Some((node, v));
                cur = child;
                depth = node_depth + 1;
            }
        }
    }
}
