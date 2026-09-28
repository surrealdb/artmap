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

//! # The validated iterator cursor (§10.3–10.4)
//!
//! One implementation for all four maps.
//!
//! - **Frames** are `(node, version, position)`. Positions are key bytes, not
//!   array indices, so a shift inside a `Node4`/`Node16` never skips a child.
//! - **Validation.** Every step validates the frame's version after reading
//!   from the node; descending into a child validates the parent (R4). On any
//!   failure the stack is discarded and the cursor **re-seeks** from an owned
//!   copy of the last key it yielded, with an exclusive bound.
//! - **Resume keys.** Each end remembers the last leaf it yielded. The leaf
//!   stays protected for the cursor's lifetime (the iterator's guard, or the
//!   map borrow), so its key is read back only when a re-seek or a crossing
//!   check needs it. Before the caller repins, [`Cursor::invalidate`] copies
//!   the key into an owned buffer and forgets the leaf.
//! - **Bounds.** An unbounded back end is a rightmost descent, never a
//!   sentinel key; there is no special case for `[0]`; both bounds are checked
//!   after every step; the two ends stop when they meet (`>=`/`<=`).
//! - **No recursion** (Inv 13): the stack is explicit.
//!
//! Guarantee (§3.3): every key present for the whole scan is yielded exactly
//! once, in order. A key inserted or removed during the scan may or may not
//! appear. A leaf reached through a validated step is yielded even if it is
//! replaced or removed later; the cursor never reads `removed` to decide.

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use std::ops::Bound;
use std::ptr::NonNull;

use crate::latch::SpinBackoff;
use crate::raw::slot::Slot;
use crate::raw::{common_prefix, Layout, LeafNode, Raw, RawTree, Storage};

/// An owned key, inline up to 32 bytes.
#[derive(Clone)]
pub(crate) enum KeyBuf {
    Inline([u8; 32], u8),
    Heap(Box<[u8]>),
}

impl KeyBuf {
    #[inline]
    pub(crate) fn new(bytes: &[u8]) -> Self {
        if bytes.len() <= 32 {
            let mut b = [0u8; 32];
            b[..bytes.len()].copy_from_slice(bytes);
            KeyBuf::Inline(b, bytes.len() as u8)
        } else {
            KeyBuf::Heap(bytes.into())
        }
    }

    #[inline]
    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            KeyBuf::Inline(b, len) => &b[..*len as usize],
            KeyBuf::Heap(b) => b,
        }
    }
}

/// Converts a user bound into an owned byte bound (one `as_bytes()` call).
pub(crate) fn owned_bound<Q: crate::AsBytes + ?Sized>(b: Bound<&Q>) -> Bound<KeyBuf> {
    match b {
        Bound::Included(k) => Bound::Included(KeyBuf::new(k.as_bytes())),
        Bound::Excluded(k) => Bound::Excluded(KeyBuf::new(k.as_bytes())),
        Bound::Unbounded => Bound::Unbounded,
    }
}

#[derive(Copy, Clone)]
struct Frame<R> {
    node: R,
    version: u64,
    /// Forward: children with key byte `>= next` remain. Backward: children
    /// with key byte `<= next` remain; `-1` means none.
    next: i16,
    /// The exact leaf has not been visited yet. Forward cursors visit it
    /// first, backward cursors last.
    exact: bool,
}

const INLINE_FRAMES: usize = 12;

/// A stack of frames, inline for the common depths.
struct Stack<R: Copy> {
    inline: [Option<Frame<R>>; INLINE_FRAMES],
    len: usize,
    spill: Vec<Frame<R>>,
}

impl<R: Copy> Stack<R> {
    fn new() -> Self {
        Self {
            inline: [None; INLINE_FRAMES],
            len: 0,
            spill: Vec::new(),
        }
    }

    #[inline]
    fn clear(&mut self) {
        self.len = 0;
        self.spill.clear();
    }

    #[inline]
    fn push(&mut self, f: Frame<R>) {
        if self.len < INLINE_FRAMES {
            self.inline[self.len] = Some(f);
        } else {
            self.spill.push(f);
        }
        self.len += 1;
    }

    #[inline]
    fn pop(&mut self) {
        if self.len > INLINE_FRAMES {
            self.spill.pop();
        }
        self.len -= 1;
    }

    #[inline]
    fn top(&mut self) -> Option<&mut Frame<R>> {
        match self.len {
            0 => None,
            n if n <= INLINE_FRAMES => self.inline[n - 1].as_mut(),
            _ => self.spill.last_mut(),
        }
    }
}

enum Step<R> {
    Leaf(R),
    End,
    Invalid,
}

/// Leaves validated together, served one by one (amortises the R5 fence).
const BATCH: usize = 16;

struct Side<R: Slot> {
    stack: Stack<R>,
    /// A leaf the seek found at the resume position (the root leaf).
    pending: Option<R>,
    /// Consecutive leaf children of the top frame, validated by one fence.
    batch: [R; BATCH],
    batch_len: u8,
    batch_pos: u8,
    last: Option<Resume<R>>,
    positioned: bool,
}

/// Where an end of the cursor resumes from.
enum Resume<R> {
    /// The last yielded leaf, still protected.
    Leaf(R),
    /// An owned copy of its key (after `invalidate`).
    Key(KeyBuf),
}

impl<R: Slot> Resume<R> {
    /// The resume key. `tree` protects the leaf for the borrow.
    #[inline]
    fn key<'t, S: Storage<Atomic: crate::raw::slot::AtomicSlot<Raw = R>>>(
        &'t self,
        tree: &'t RawTree<S>,
    ) -> &'t [u8] {
        match self {
            // SAFETY: a leaf this cursor yielded, still protected by the
            // cursor's guard or the map borrow.
            Resume::Leaf(l) => unsafe { tree.leaf_ref(*l) }.key_bytes(),
            Resume::Key(k) => k.as_slice(),
        }
    }
}

impl<R: Slot> Side<R> {
    fn new() -> Self {
        Self {
            stack: Stack::new(),
            pending: None,
            batch: [R::NULL; BATCH],
            batch_len: 0,
            batch_pos: 0,
            last: None,
            positioned: false,
        }
    }

    #[inline]
    fn reset(&mut self) {
        self.stack.clear();
        self.pending = None;
        self.batch_len = 0;
        self.batch_pos = 0;
    }

    #[inline]
    fn remember(&mut self, leaf: R) {
        self.last = Some(Resume::Leaf(leaf));
    }
}

/// A double-ended, validated cursor over a key range.
pub(crate) struct Cursor<S: Layout> {
    start: Bound<KeyBuf>,
    end: Bound<KeyBuf>,
    front: Side<Raw<S>>,
    back: Side<Raw<S>>,
    done: bool,
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum Dir {
    Forward,
    Backward,
}

/// Lexicographic comparison of byte keys, inline for short keys (8-byte
/// big-endian chunks); long keys use the library comparison.
#[inline(always)]
pub(crate) fn cmp_keys(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    let n = a.len().min(b.len());
    if n > 64 {
        return a.cmp(b);
    }
    let mut i = 0;
    while i + 8 <= n {
        let (Some(x), Some(y)) = (a[i..].first_chunk::<8>(), b[i..].first_chunk::<8>()) else {
            break;
        };
        let (x, y) = (u64::from_be_bytes(*x), u64::from_be_bytes(*y));
        if x != y {
            return x.cmp(&y);
        }
        i += 8;
    }
    while i < n {
        if a[i] != b[i] {
            return a[i].cmp(&b[i]);
        }
        i += 1;
    }
    a.len().cmp(&b.len())
}

#[inline(always)]
fn above_start(k: &[u8], start: &Bound<KeyBuf>) -> bool {
    match start {
        Bound::Included(s) => cmp_keys(k, s.as_slice()).is_ge(),
        Bound::Excluded(s) => cmp_keys(k, s.as_slice()).is_gt(),
        Bound::Unbounded => true,
    }
}

#[inline(always)]
fn below_end(k: &[u8], end: &Bound<KeyBuf>) -> bool {
    match end {
        Bound::Included(e) => cmp_keys(k, e.as_slice()).is_le(),
        Bound::Excluded(e) => cmp_keys(k, e.as_slice()).is_lt(),
        Bound::Unbounded => true,
    }
}

impl<S: Layout> Cursor<S> {
    pub(crate) fn new(start: Bound<KeyBuf>, end: Bound<KeyBuf>) -> Self {
        Self {
            start,
            end,
            front: Side::new(),
            back: Side::new(),
            done: false,
        }
    }
}

impl<S: Storage> Cursor<S> {
    /// Forgets every frame and every leaf pointer, keeping owned copies of the
    /// resume keys, so the next step re-seeks. Required before the caller
    /// repins its guard (§8.5).
    pub(crate) fn invalidate(&mut self, tree: &RawTree<S>) {
        for side in [&mut self.front, &mut self.back] {
            side.positioned = false;
            side.reset();
            if let Some(r) = &side.last {
                let owned = KeyBuf::new(r.key(tree));
                side.last = Some(Resume::Key(owned));
            }
        }
    }
}

impl<S: Storage> Cursor<S> {
    /// The next leaf in ascending order. The caller keeps `tree` protected.
    pub(crate) fn next(&mut self, tree: &RawTree<S>) -> Option<NonNull<S::Leaf>> {
        self.advance(tree, Dir::Forward)
    }

    /// The next leaf in descending order.
    pub(crate) fn next_back(&mut self, tree: &RawTree<S>) -> Option<NonNull<S::Leaf>> {
        self.advance(tree, Dir::Backward)
    }

    fn advance(&mut self, tree: &RawTree<S>, dir: Dir) -> Option<NonNull<S::Leaf>> {
        loop {
            if self.done {
                return None;
            }
            let (side, other) = match dir {
                Dir::Forward => (&mut self.front, &self.back),
                Dir::Backward => (&mut self.back, &self.front),
            };
            if !side.positioned {
                let bound = match (&side.last, dir) {
                    (Some(r), _) => Bound::Excluded(KeyBuf::new(r.key(tree))),
                    (None, Dir::Forward) => self.start.clone(),
                    (None, Dir::Backward) => self.end.clone(),
                };
                seek(tree, side, &bound, dir);
                side.positioned = true;
            }
            let leaf_raw = match side.pending.take() {
                Some(l) => l,
                None => match step(tree, side, dir) {
                    Step::Leaf(l) => l,
                    Step::End => {
                        self.done = true;
                        return None;
                    }
                    Step::Invalid => {
                        side.positioned = false;
                        continue;
                    }
                },
            };
            // SAFETY: a protected leaf reached through a validated slot.
            let leaf = unsafe { tree.storage.leaf(leaf_raw) };
            // SAFETY: as above.
            let k = unsafe { leaf.as_ref() }.key_bytes();
            // Structure guarantees keys past the last yielded one and inside
            // the start bound; only the far bound and the other end are checked.
            // (An inconsistent `AsBytes` can then only cause wrong results.)
            let (in_range, crossed) = match dir {
                Dir::Forward => (
                    below_end(k, &self.end),
                    other
                        .last
                        .as_ref()
                        .is_some_and(|b| cmp_keys(k, b.key(tree)).is_ge()),
                ),
                Dir::Backward => (
                    above_start(k, &self.start),
                    other
                        .last
                        .as_ref()
                        .is_some_and(|f| cmp_keys(k, f.key(tree)).is_le()),
                ),
            };
            if !in_range || crossed {
                self.done = true;
                return None;
            }
            side.remember(leaf_raw);
            return Some(leaf);
        }
    }
}

/// Positions `side` so that its next step yields the first key (in `dir`)
/// that satisfies `bound`.
fn seek<S: Storage>(tree: &RawTree<S>, side: &mut Side<Raw<S>>, bound: &Bound<KeyBuf>, dir: Dir) {
    let mut backoff = SpinBackoff::new();
    'retry: loop {
        side.reset();
        let root = tree.root();
        if root.is_null() {
            return;
        }
        if root.is_leaf() {
            // SAFETY: a protected leaf loaded from `root` (Acquire).
            let k = unsafe { tree.leaf_ref(root) }.key_bytes();
            let take = match (bound, dir) {
                (Bound::Unbounded, _) => true,
                (Bound::Included(b), Dir::Forward) => cmp_keys(k, b.as_slice()).is_ge(),
                (Bound::Excluded(b), Dir::Forward) => cmp_keys(k, b.as_slice()).is_gt(),
                (Bound::Included(b), Dir::Backward) => cmp_keys(k, b.as_slice()).is_le(),
                (Bound::Excluded(b), Dir::Backward) => cmp_keys(k, b.as_slice()).is_lt(),
            };
            if take {
                side.pending = Some(root);
            }
            return;
        }
        let (key, inclusive) = match bound {
            Bound::Unbounded => {
                // SAFETY: protected inner node read from `root`.
                let node = unsafe { tree.node_ref(root) };
                let Some(v) = node.latch.read_version() else {
                    backoff.spin();
                    continue 'retry;
                };
                if tree.root() != root {
                    continue 'retry;
                }
                side.stack.push(edge_frame(root, v, dir));
                return;
            }
            Bound::Included(k) => (k.as_slice(), true),
            Bound::Excluded(k) => (k.as_slice(), false),
        };
        let mut node_raw = root;
        let mut depth = 0usize;
        loop {
            // SAFETY: protected inner node of this tree.
            let node = unsafe { tree.node_ref(node_raw) };
            let Some(v) = node.latch.read_version() else {
                backoff.spin();
                continue 'retry;
            };
            crate::hooks::pause(crate::hooks::Point::CursorSeek);
            // R4: couple to the frame above, or re-check the root.
            let coupled = match side.stack.top() {
                Some(p) => {
                    let pv = p.version;
                    // SAFETY: protected inner node of this tree.
                    unsafe { tree.node_ref(p.node) }.latch.validate(pv)
                }
                None => tree.root() == node_raw,
            };
            if !coupled {
                continue 'retry;
            }
            let prefix = node.load_prefix();
            let rest = key.get(depth..).unwrap_or(&[]);
            let m = common_prefix(prefix.as_slice(), rest);
            if m < prefix.len {
                // The key diverges inside this node's prefix, so the whole
                // subtree lies on one side of it.
                let subtree_after_key = m == rest.len() || rest[m] < prefix.bytes[m];
                if !node.latch.validate(v) {
                    continue 'retry;
                }
                if subtree_after_key == (dir == Dir::Forward) {
                    side.stack.push(edge_frame(node_raw, v, dir));
                }
                return;
            }
            let node_depth = depth + prefix.len;
            if node_depth == key.len() {
                // The exact leaf equals the key; every child is greater.
                if !node.latch.validate(v) {
                    continue 'retry;
                }
                side.stack.push(match dir {
                    Dir::Forward => Frame {
                        node: node_raw,
                        version: v,
                        next: 0,
                        exact: inclusive,
                    },
                    Dir::Backward => Frame {
                        node: node_raw,
                        version: v,
                        next: -1,
                        exact: inclusive,
                    },
                });
                return;
            }
            let byte = key[node_depth];
            let child = node.find_child(byte);
            if !node.latch.validate(v) {
                continue 'retry;
            }
            side.stack.push(match dir {
                Dir::Forward => Frame {
                    node: node_raw,
                    version: v,
                    next: byte as i16 + 1,
                    exact: false,
                },
                Dir::Backward => Frame {
                    node: node_raw,
                    version: v,
                    next: byte as i16 - 1,
                    exact: true,
                },
            });
            match child {
                None => return,
                Some(c) if c.is_leaf() => {
                    // SAFETY: protected leaf reached through a validated slot.
                    let k = unsafe { tree.leaf_ref(c) }.key_bytes();
                    let take = match dir {
                        Dir::Forward => k > key || (inclusive && k == key),
                        Dir::Backward => k < key || (inclusive && k == key),
                    };
                    if take {
                        if let Some(top) = side.stack.top() {
                            top.next = byte as i16;
                        }
                    }
                    return;
                }
                Some(c) => {
                    node_raw = c;
                    depth = node_depth + 1;
                }
            }
        }
    }
}

/// A frame positioned before the first (forward) or after the last
/// (backward) entry of a node.
fn edge_frame<R>(node: R, version: u64, dir: Dir) -> Frame<R> {
    Frame {
        node,
        version,
        next: match dir {
            Dir::Forward => 0,
            Dir::Backward => 255,
        },
        exact: true,
    }
}

/// One validated step of the cursor.
fn step<S: Storage>(tree: &RawTree<S>, side: &mut Side<Raw<S>>, dir: Dir) -> Step<Raw<S>> {
    if side.batch_pos < side.batch_len {
        let l = side.batch[side.batch_pos as usize];
        side.batch_pos += 1;
        return Step::Leaf(l);
    }
    let (stack, batch) = (&mut side.stack, &mut side.batch);
    loop {
        let Some(top) = stack.top() else {
            return Step::End;
        };
        // SAFETY: frames only hold protected inner nodes of this tree.
        let node = unsafe { tree.node_ref(top.node) };
        let exact_now = match dir {
            Dir::Forward => top.exact,
            Dir::Backward => top.exact && top.next < 0,
        };
        if exact_now {
            top.exact = false;
            let ex = node.exact_leaf();
            if !node.latch.validate(top.version) {
                return Step::Invalid;
            }
            if !ex.is_null() {
                return Step::Leaf(ex);
            }
            continue;
        }
        // Gather consecutive leaf children, stopping before an inner child.
        let mut n = 0usize;
        let mut inner = None;
        let mut next = top.next;
        loop {
            let found = match dir {
                Dir::Forward if next <= 255 => node.next_child(next as u16),
                Dir::Backward if next >= 0 => node.prev_child(next as u8),
                _ => None,
            };
            let Some((byte, child)) = found else {
                break;
            };
            let after = match dir {
                Dir::Forward => byte as i16 + 1,
                Dir::Backward => byte as i16 - 1,
            };
            if child.is_leaf() {
                batch[n] = child;
                n += 1;
                next = after;
                if n == BATCH {
                    break;
                }
            } else {
                if n == 0 {
                    inner = Some(child);
                    next = after;
                }
                break;
            }
        }
        if let Some(child) = inner {
            // Descend. One validation of this node, after reading the child's
            // version, both validates the read of the slot and couples the
            // child to it (R3, R4).
            let parent_version = top.version;
            top.next = next;
            // SAFETY: a protected inner node (following a non-null slot before
            // validating is allowed, R3).
            let cn = unsafe { tree.node_ref(child) };
            let Some(cv) = cn.latch.read_version() else {
                return Step::Invalid;
            };
            if !node.latch.validate(parent_version) {
                return Step::Invalid;
            }
            stack.push(edge_frame(child, cv, dir));
            continue;
        }
        // Leaves found, or the node is exhausted: both are conclusions.
        if !node.latch.validate(top.version) {
            return Step::Invalid;
        }
        top.next = next;
        if n > 0 {
            side.batch_len = n as u8;
            side.batch_pos = 1;
            return Step::Leaf(side.batch[0]);
        }
        match dir {
            Dir::Forward => stack.pop(),
            Dir::Backward if top.exact => top.next = -1,
            Dir::Backward => stack.pop(),
        }
    }
}
