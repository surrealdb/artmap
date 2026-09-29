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

//! # Inner node layouts (§6.3–6.4)
//!
//! Every field an optimistic reader reads is atomic and is written at the width
//! it is read (W5). Readers snapshot each field at most once per optimistic
//! section and clamp what they derive from it (R2); a null child slot is always
//! "absent" (R6). Mutators of published nodes take `&self` plus the node's
//! [`WriteGuard`] (Inv 3); unpublished nodes are built with `&mut self`.
//!
//! The layouts are generic over the child-slot representation: tagged
//! pointers for the heap maps, tagged `u32` offsets for the arena maps.

#![deny(unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]

use crate::latch::{HybridLatch, WriteGuard};
use crate::raw::slot::{AtomicSlot, Slot};
use crate::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, AtomicU8, Ordering};

/// Maximum prefix bytes stored in a node. Longer shared prefixes become
/// chains of `Node4`s (`create_prefix_chain`), so `prefix_len` never exceeds it.
pub(crate) const MAX_PREFIX_LEN: usize = 16;
/// `Node48::child_indices` value for an absent byte. Any value `>= 48` is
/// treated as absent (R2).
pub(crate) const NODE48_EMPTY: u8 = 48;

/// Discriminated node type; immutable after construction.
#[repr(u8)]
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub(crate) enum NodeType {
    Node4 = 0,
    Node16 = 1,
    Node48 = 2,
    Node256 = 3,
}

impl NodeType {
    #[inline]
    pub(crate) fn capacity(self) -> usize {
        match self {
            NodeType::Node4 => 4,
            NodeType::Node16 => 16,
            NodeType::Node48 => 48,
            NodeType::Node256 => 256,
        }
    }

    #[inline]
    pub(crate) fn grown(self) -> Option<NodeType> {
        match self {
            NodeType::Node4 => Some(NodeType::Node16),
            NodeType::Node16 => Some(NodeType::Node48),
            NodeType::Node48 => Some(NodeType::Node256),
            NodeType::Node256 => None,
        }
    }
}

/// Common header at offset 0 of every inner node.
#[repr(C)]
pub(crate) struct NodeHeader<A> {
    pub(crate) latch: HybridLatch,
    /// Immutable after construction; published by the Release store of the
    /// node pointer (W3), so a plain field is fine.
    pub(crate) node_type: NodeType,
    num_children: AtomicU16,
    /// Always `<= MAX_PREFIX_LEN`.
    prefix_len: AtomicU16,
    /// `MAX_PREFIX_LEN` bytes packed little-endian into two words (W5).
    prefix: [AtomicU64; 2],
    exact_leaf: A,
}

/// The entry of a node that a remove takes out, for [`NodeHeader::rest`].
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub(crate) enum Taken {
    /// The exact leaf.
    Exact,
    /// The child at this byte.
    Child(u8),
    /// Nothing: count every entry.
    Nothing,
}

/// What a node holds besides the [`Taken`] entry (§13).
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub(crate) enum Rest<R> {
    /// Nothing: the node can be unlinked.
    Empty,
    /// One leaf (a child or the exact leaf), which can take the node's place.
    Leaf(R),
    /// One inner child, at this byte, which the node can be merged into.
    Inner(u8, R),
    /// Two or more entries: the node stays.
    Many,
}

/// A reader's snapshot of a node prefix (R2).
#[derive(Clone, Copy)]
pub(crate) struct PrefixSnapshot {
    /// Already clamped to `MAX_PREFIX_LEN`.
    pub(crate) len: usize,
    pub(crate) bytes: [u8; MAX_PREFIX_LEN],
}

impl PrefixSnapshot {
    #[inline]
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len]
    }

    /// `rest.starts_with(self.as_slice())`, without a `memcmp` call: this is
    /// on every level of every lookup.
    #[inline(always)]
    pub(crate) fn is_prefix_of(&self, rest: &[u8]) -> bool {
        let n = self.len;
        if n == 0 {
            return true;
        }
        if rest.len() < n {
            return false;
        }
        if let Some(head) = rest.first_chunk::<MAX_PREFIX_LEN>() {
            let diff = u128::from_le_bytes(*head) ^ u128::from_le_bytes(self.bytes);
            let mask = if n == MAX_PREFIX_LEN {
                !0u128
            } else {
                (1u128 << (n * 8)) - 1
            };
            return diff & mask == 0;
        }
        let mut i = 0;
        while i < n {
            if self.bytes[i] != rest[i] {
                return false;
            }
            i += 1;
        }
        true
    }
}

#[inline(always)]
fn pack16(bytes: [u8; 16]) -> [u64; 2] {
    let w = u128::from_le_bytes(bytes);
    [w as u64, (w >> 64) as u64]
}

#[inline(always)]
fn unpack16(words: [u64; 2]) -> [u8; 16] {
    (((words[1] as u128) << 64) | words[0] as u128).to_le_bytes()
}

impl<A: AtomicSlot> NodeHeader<A> {
    fn new(node_type: NodeType) -> Self {
        Self {
            latch: HybridLatch::new(),
            node_type,
            num_children: AtomicU16::new(0),
            prefix_len: AtomicU16::new(0),
            prefix: [AtomicU64::new(0), AtomicU64::new(0)],
            exact_leaf: A::new(A::Raw::NULL),
        }
    }

    /// R2: one load per field, clamped.
    #[inline]
    pub(crate) fn load_prefix(&self) -> PrefixSnapshot {
        let len = (self.prefix_len.load(Ordering::Relaxed) as usize).min(MAX_PREFIX_LEN);
        if len == 0 {
            return PrefixSnapshot {
                len: 0,
                bytes: [0; MAX_PREFIX_LEN],
            };
        }
        let bytes = unpack16([
            self.prefix[0].load(Ordering::Relaxed),
            self.prefix[1].load(Ordering::Relaxed),
        ]);
        PrefixSnapshot { len, bytes }
    }

    /// Snapshot of the prefix length, clamped (R2). A hint when read without
    /// this node's latch or version.
    #[inline]
    pub(crate) fn prefix_len(&self) -> usize {
        (self.prefix_len.load(Ordering::Relaxed) as usize).min(MAX_PREFIX_LEN)
    }

    /// The stored prefix length, unclamped, for `validate`.
    #[inline]
    pub(crate) fn raw_prefix_len(&self) -> usize {
        self.prefix_len.load(Ordering::Relaxed) as usize
    }

    /// Snapshot of `num_children`, clamped to the node's capacity (R2).
    #[inline]
    pub(crate) fn num_children(&self) -> usize {
        self.num_children_max(self.node_type.capacity())
    }

    /// Snapshot of `num_children`, clamped to `cap` (a constant at typed call
    /// sites, so no dispatch on the node type).
    #[inline(always)]
    fn num_children_max(&self, cap: usize) -> usize {
        (self.num_children.load(Ordering::Relaxed) as usize).min(cap)
    }

    #[inline]
    pub(crate) fn is_full(&self) -> bool {
        self.num_children() >= self.node_type.capacity()
    }

    /// Acquire load of `exact_leaf` (W3).
    #[inline]
    pub(crate) fn exact_leaf(&self) -> A::Raw {
        self.exact_leaf.load(Ordering::Acquire)
    }

    /// W2: caller holds this node's latch.
    #[inline]
    pub(crate) fn store_prefix(&self, w: &WriteGuard<'_>, prefix: &[u8]) {
        debug_assert!(w.holds(&self.latch));
        self.write_prefix(prefix);
    }

    /// W3: caller holds this node's latch.
    #[inline]
    pub(crate) fn set_exact_leaf(&self, w: &WriteGuard<'_>, leaf: A::Raw) {
        debug_assert!(w.holds(&self.latch));
        self.exact_leaf.store(leaf, Ordering::Release);
    }

    /// Builder: only for nodes no other thread can reach (Inv 3).
    #[inline]
    pub(crate) fn init_prefix(&mut self, prefix: &[u8]) {
        self.write_prefix(prefix);
    }

    /// Builder: only for nodes no other thread can reach (Inv 3).
    #[inline]
    pub(crate) fn init_exact_leaf(&mut self, leaf: A::Raw) {
        self.exact_leaf.store(leaf, Ordering::Relaxed);
    }

    #[inline]
    fn write_prefix(&self, prefix: &[u8]) {
        debug_assert!(prefix.len() <= MAX_PREFIX_LEN);
        let len = prefix.len().min(MAX_PREFIX_LEN);
        let mut bytes = [0u8; MAX_PREFIX_LEN];
        bytes[..len].copy_from_slice(&prefix[..len]);
        let [lo, hi] = pack16(bytes);
        self.prefix[0].store(lo, Ordering::Relaxed);
        self.prefix[1].store(hi, Ordering::Relaxed);
        self.prefix_len.store(len as u16, Ordering::Relaxed);
    }

    #[inline]
    fn set_num_children(&self, n: usize) {
        self.num_children.store(n as u16, Ordering::Relaxed);
    }
}

/// Up to 4 children, keys sorted.
#[repr(C)]
pub(crate) struct Node4<A> {
    pub(crate) header: NodeHeader<A>,
    keys: AtomicU32,
    children: [A; 4],
}

/// Up to 16 children, keys sorted, searched with SIMD over a local snapshot.
#[repr(C)]
pub(crate) struct Node16<A> {
    pub(crate) header: NodeHeader<A>,
    keys: [AtomicU64; 2],
    children: [A; 16],
}

/// Up to 48 children, indexed through a 256-entry byte map.
#[repr(C)]
pub(crate) struct Node48<A> {
    pub(crate) header: NodeHeader<A>,
    child_indices: [AtomicU8; 256],
    child_bitmap: [AtomicU64; 4],
    children: [A; 48],
}

/// Up to 256 children, directly indexed by byte.
#[repr(C)]
pub(crate) struct Node256<A> {
    pub(crate) header: NodeHeader<A>,
    child_bitmap: [AtomicU64; 4],
    children: [A; 256],
}

fn null_slots<A: AtomicSlot, const N: usize>() -> [A; N] {
    std::array::from_fn(|_| A::new(A::Raw::NULL))
}

impl<A: AtomicSlot> Node4<A> {
    pub(crate) fn new() -> Self {
        Self {
            header: NodeHeader::new(NodeType::Node4),
            keys: AtomicU32::new(0),
            children: null_slots(),
        }
    }
}

impl<A: AtomicSlot> Node16<A> {
    pub(crate) fn new() -> Self {
        Self {
            header: NodeHeader::new(NodeType::Node16),
            keys: [AtomicU64::new(0), AtomicU64::new(0)],
            children: null_slots(),
        }
    }

    #[inline]
    fn load_keys(&self) -> [u8; 16] {
        unpack16([
            self.keys[0].load(Ordering::Relaxed),
            self.keys[1].load(Ordering::Relaxed),
        ])
    }

    #[inline]
    fn store_keys(&self, keys: [u8; 16]) {
        let [lo, hi] = pack16(keys);
        self.keys[0].store(lo, Ordering::Relaxed);
        self.keys[1].store(hi, Ordering::Relaxed);
    }
}

impl<A: AtomicSlot> Node48<A> {
    pub(crate) fn new() -> Self {
        Self {
            header: NodeHeader::new(NodeType::Node48),
            child_indices: std::array::from_fn(|_| AtomicU8::new(NODE48_EMPTY)),
            child_bitmap: std::array::from_fn(|_| AtomicU64::new(0)),
            children: null_slots(),
        }
    }
}

impl<A: AtomicSlot> Node256<A> {
    pub(crate) fn new() -> Self {
        Self {
            header: NodeHeader::new(NodeType::Node256),
            child_bitmap: std::array::from_fn(|_| AtomicU64::new(0)),
            children: null_slots(),
        }
    }
}

#[inline(always)]
fn bitmap_set(bitmap: &[AtomicU64; 4], byte: u8) {
    let w = &bitmap[(byte / 64) as usize];
    // Under the node latch: a plain read-modify-write at the reader's width (W5).
    w.store(
        w.load(Ordering::Relaxed) | (1u64 << (byte % 64)),
        Ordering::Relaxed,
    );
}

#[inline(always)]
fn bitmap_clear(bitmap: &[AtomicU64; 4], byte: u8) {
    let w = &bitmap[(byte / 64) as usize];
    w.store(
        w.load(Ordering::Relaxed) & !(1u64 << (byte % 64)),
        Ordering::Relaxed,
    );
}

/// First set byte `>= from` in a bitmap snapshot.
#[inline(always)]
fn bitmap_next(bitmap: &[AtomicU64; 4], from: u16) -> Option<u8> {
    if from > 255 {
        return None;
    }
    let mut word_idx = (from / 64) as usize;
    let mut word = bitmap[word_idx].load(Ordering::Relaxed) & (!0u64 << (from % 64));
    loop {
        if word != 0 {
            return Some((word_idx * 64 + word.trailing_zeros() as usize) as u8);
        }
        word_idx += 1;
        if word_idx == 4 {
            return None;
        }
        word = bitmap[word_idx].load(Ordering::Relaxed);
    }
}

/// Last set byte `<= to` in a bitmap snapshot.
#[inline(always)]
fn bitmap_prev(bitmap: &[AtomicU64; 4], to: u8) -> Option<u8> {
    let mut word_idx = (to / 64) as usize;
    let bit = to % 64;
    let mask = if bit == 63 {
        !0u64
    } else {
        (1u64 << (bit + 1)) - 1
    };
    let mut word = bitmap[word_idx].load(Ordering::Relaxed) & mask;
    loop {
        if word != 0 {
            return Some((word_idx * 64 + 63 - word.leading_zeros() as usize) as u8);
        }
        if word_idx == 0 {
            return None;
        }
        word_idx -= 1;
        word = bitmap[word_idx].load(Ordering::Relaxed);
    }
}

/// Typed views of a node header. `NodeRef` is a shared reference to a
/// published (or unpublished) inner node of a known type.
pub(crate) enum NodeRef<'n, A> {
    N4(&'n Node4<A>),
    N16(&'n Node16<A>),
    N48(&'n Node48<A>),
    N256(&'n Node256<A>),
}

impl<A: AtomicSlot> NodeHeader<A> {
    /// Dispatches on the (immutable) node type.
    #[inline(always)]
    pub(crate) fn typed(&self) -> NodeRef<'_, A> {
        let p = std::ptr::from_ref(self);
        // SAFETY: every `NodeHeader` lives at offset 0 of a `#[repr(C)]` node
        // whose type is `node_type`, and the reference came from a pointer to
        // that whole node, so the cast keeps its provenance over the node.
        unsafe {
            match self.node_type {
                NodeType::Node4 => NodeRef::N4(&*p.cast::<Node4<A>>()),
                NodeType::Node16 => NodeRef::N16(&*p.cast::<Node16<A>>()),
                NodeType::Node48 => NodeRef::N48(&*p.cast::<Node48<A>>()),
                NodeType::Node256 => NodeRef::N256(&*p.cast::<Node256<A>>()),
            }
        }
    }

    /// R6: the child for `byte`, or `None` if absent or null.
    #[inline]
    pub(crate) fn find_child(&self, byte: u8) -> Option<A::Raw> {
        let c = match self.typed() {
            NodeRef::N4(n) => {
                let count = n.header.num_children_max(4);
                let keys = n.keys.load(Ordering::Relaxed).to_le_bytes();
                let i = keys[..count].iter().position(|&k| k == byte)?;
                n.children[i].load(Ordering::Acquire)
            }
            NodeRef::N16(n) => {
                let count = n.header.num_children_max(16);
                let keys = n.load_keys();
                let i = crate::simd::find_child_node16(&keys, count, byte)?;
                n.children[i].load(Ordering::Acquire)
            }
            NodeRef::N48(n) => {
                let slot = n.child_indices[byte as usize].load(Ordering::Relaxed);
                if slot >= NODE48_EMPTY {
                    return None;
                }
                n.children[slot as usize].load(Ordering::Acquire)
            }
            NodeRef::N256(n) => n.children[byte as usize].load(Ordering::Acquire),
        };
        (!c.is_null()).then_some(c)
    }

    /// The first child with key byte `>= from` (0..=256), as `(byte, child)`.
    /// Null slots are skipped (R6).
    #[inline]
    pub(crate) fn next_child(&self, from: u16) -> Option<(u8, A::Raw)> {
        if from > 255 {
            return None;
        }
        match self.typed() {
            NodeRef::N4(n) => {
                let count = n.header.num_children();
                let keys = n.keys.load(Ordering::Relaxed).to_le_bytes();
                for (i, &k) in keys[..count].iter().enumerate() {
                    if k as u16 >= from {
                        let c = n.children[i].load(Ordering::Acquire);
                        if !c.is_null() {
                            return Some((k, c));
                        }
                    }
                }
                None
            }
            NodeRef::N16(n) => {
                let count = n.header.num_children();
                let keys = n.load_keys();
                for (i, &k) in keys[..count].iter().enumerate() {
                    if k as u16 >= from {
                        let c = n.children[i].load(Ordering::Acquire);
                        if !c.is_null() {
                            return Some((k, c));
                        }
                    }
                }
                None
            }
            NodeRef::N48(n) => {
                let mut from = from;
                while let Some(b) = bitmap_next(&n.child_bitmap, from) {
                    let slot = n.child_indices[b as usize].load(Ordering::Relaxed);
                    if slot < NODE48_EMPTY {
                        let c = n.children[slot as usize].load(Ordering::Acquire);
                        if !c.is_null() {
                            return Some((b, c));
                        }
                    }
                    from = b as u16 + 1;
                }
                None
            }
            NodeRef::N256(n) => {
                let mut from = from;
                while let Some(b) = bitmap_next(&n.child_bitmap, from) {
                    let c = n.children[b as usize].load(Ordering::Acquire);
                    if !c.is_null() {
                        return Some((b, c));
                    }
                    from = b as u16 + 1;
                }
                None
            }
        }
    }

    /// The last child with key byte `<= to`, as `(byte, child)`.
    #[inline]
    pub(crate) fn prev_child(&self, to: u8) -> Option<(u8, A::Raw)> {
        match self.typed() {
            NodeRef::N4(n) => {
                let count = n.header.num_children();
                let keys = n.keys.load(Ordering::Relaxed).to_le_bytes();
                for i in (0..count).rev() {
                    if keys[i] <= to {
                        let c = n.children[i].load(Ordering::Acquire);
                        if !c.is_null() {
                            return Some((keys[i], c));
                        }
                    }
                }
                None
            }
            NodeRef::N16(n) => {
                let count = n.header.num_children();
                let keys = n.load_keys();
                for i in (0..count).rev() {
                    if keys[i] <= to {
                        let c = n.children[i].load(Ordering::Acquire);
                        if !c.is_null() {
                            return Some((keys[i], c));
                        }
                    }
                }
                None
            }
            NodeRef::N48(n) => {
                let mut to = to;
                while let Some(b) = bitmap_prev(&n.child_bitmap, to) {
                    let slot = n.child_indices[b as usize].load(Ordering::Relaxed);
                    if slot < NODE48_EMPTY {
                        let c = n.children[slot as usize].load(Ordering::Acquire);
                        if !c.is_null() {
                            return Some((b, c));
                        }
                    }
                    if b == 0 {
                        return None;
                    }
                    to = b - 1;
                }
                None
            }
            NodeRef::N256(n) => {
                let mut to = to;
                while let Some(b) = bitmap_prev(&n.child_bitmap, to) {
                    let c = n.children[b as usize].load(Ordering::Acquire);
                    if !c.is_null() {
                        return Some((b, c));
                    }
                    if b == 0 {
                        return None;
                    }
                    to = b - 1;
                }
                None
            }
        }
    }

    /// Calls `f(byte, child)` for every non-null child, in key order. For
    /// quiescent walks (drop, validation) and walks under this node's latch.
    pub(crate) fn for_each_child(&self, mut f: impl FnMut(u8, A::Raw)) {
        let mut from = 0u16;
        while let Some((b, c)) = self.next_child(from) {
            f(b, c);
            from = b as u16 + 1;
        }
    }

    /// What this node holds besides `taken` (§13).
    ///
    /// Without the latch the answer is optimistic, and the caller validates
    /// the node's version before acting on it (R3); under the latch it is
    /// exact. `num_children` settles the common case, two or more entries
    /// left, without scanning.
    pub(crate) fn rest(&self, taken: Taken) -> Rest<A::Raw> {
        let exact = self.exact_leaf();
        let has_exact = !exact.is_null() && taken != Taken::Exact;
        let taken_child = usize::from(matches!(taken, Taken::Child(_)));
        let left = (self.num_children() + usize::from(has_exact)).saturating_sub(taken_child);
        if left >= 2 {
            return Rest::Many;
        }
        let mut rest = if has_exact {
            Rest::Leaf(exact)
        } else {
            Rest::Empty
        };
        let mut from = 0u16;
        while let Some((b, c)) = self.next_child(from) {
            from = b as u16 + 1;
            if taken == Taken::Child(b) {
                continue;
            }
            if rest != Rest::Empty {
                return Rest::Many;
            }
            rest = if c.is_leaf() {
                Rest::Leaf(c)
            } else {
                Rest::Inner(b, c)
            };
        }
        rest
    }

    /// W2/W3: inserts a child for an absent `byte` into a non-full node.
    /// Caller holds this node's latch.
    pub(crate) fn insert_child(&self, w: &WriteGuard<'_>, byte: u8, child: A::Raw) {
        debug_assert!(w.holds(&self.latch));
        self.insert_child_raw(byte, child);
    }

    /// Builder variant of [`insert_child`](Self::insert_child) for unpublished nodes.
    pub(crate) fn push_child_unpublished(&mut self, byte: u8, child: A::Raw) {
        self.insert_child_raw(byte, child);
    }

    fn insert_child_raw(&self, byte: u8, child: A::Raw) {
        debug_assert!(!child.is_null());
        match self.typed() {
            NodeRef::N4(n) => {
                let count = n.header.num_children();
                assert!(count < 4, "Node4 insert into a full node");
                let mut keys = n.keys.load(Ordering::Relaxed).to_le_bytes();
                let pos = keys[..count].partition_point(|&k| k < byte);
                for i in (pos..count).rev() {
                    keys[i + 1] = keys[i];
                    let c = n.children[i].load(Ordering::Relaxed);
                    n.children[i + 1].store(c, Ordering::Release);
                }
                keys[pos] = byte;
                n.keys.store(u32::from_le_bytes(keys), Ordering::Relaxed);
                n.children[pos].store(child, Ordering::Release);
                n.header.set_num_children(count + 1);
            }
            NodeRef::N16(n) => {
                let count = n.header.num_children();
                assert!(count < 16, "Node16 insert into a full node");
                let mut keys = n.load_keys();
                let pos = keys[..count].partition_point(|&k| k < byte);
                for i in (pos..count).rev() {
                    keys[i + 1] = keys[i];
                    let c = n.children[i].load(Ordering::Relaxed);
                    n.children[i + 1].store(c, Ordering::Release);
                }
                keys[pos] = byte;
                n.store_keys(keys);
                n.children[pos].store(child, Ordering::Release);
                n.header.set_num_children(count + 1);
            }
            NodeRef::N48(n) => {
                let count = n.header.num_children();
                assert!(count < 48, "Node48 insert into a full node");
                let slot = (0..48)
                    .find(|&i| n.children[i].load(Ordering::Relaxed).is_null())
                    .expect("Node48 with fewer than 48 children has a free slot");
                n.children[slot].store(child, Ordering::Release);
                n.child_indices[byte as usize].store(slot as u8, Ordering::Relaxed);
                bitmap_set(&n.child_bitmap, byte);
                n.header.set_num_children(count + 1);
            }
            NodeRef::N256(n) => {
                debug_assert!(n.children[byte as usize].load(Ordering::Relaxed).is_null());
                let count = n.header.num_children();
                n.children[byte as usize].store(child, Ordering::Release);
                bitmap_set(&n.child_bitmap, byte);
                n.header.set_num_children(count + 1);
            }
        }
    }

    /// W3: replaces the child at `byte`, which must be present. Caller holds
    /// this node's latch.
    pub(crate) fn replace_child(&self, w: &WriteGuard<'_>, byte: u8, child: A::Raw) {
        debug_assert!(w.holds(&self.latch));
        debug_assert!(!child.is_null());
        match self.typed() {
            NodeRef::N4(n) => {
                let count = n.header.num_children();
                let keys = n.keys.load(Ordering::Relaxed).to_le_bytes();
                let i = keys[..count]
                    .iter()
                    .position(|&k| k == byte)
                    .expect("replace_child: byte present");
                n.children[i].store(child, Ordering::Release);
            }
            NodeRef::N16(n) => {
                let count = n.header.num_children();
                let keys = n.load_keys();
                let i = keys[..count]
                    .iter()
                    .position(|&k| k == byte)
                    .expect("replace_child: byte present");
                n.children[i].store(child, Ordering::Release);
            }
            NodeRef::N48(n) => {
                let slot = n.child_indices[byte as usize].load(Ordering::Relaxed);
                assert!(slot < NODE48_EMPTY, "replace_child: byte present");
                n.children[slot as usize].store(child, Ordering::Release);
            }
            NodeRef::N256(n) => {
                n.children[byte as usize].store(child, Ordering::Release);
            }
        }
    }

    /// W2/W3: removes the child at `byte`. Caller holds this node's latch.
    pub(crate) fn remove_child(&self, w: &WriteGuard<'_>, byte: u8) {
        debug_assert!(w.holds(&self.latch));
        let null = A::Raw::NULL;
        match self.typed() {
            NodeRef::N4(n) => {
                let count = n.header.num_children();
                let mut keys = n.keys.load(Ordering::Relaxed).to_le_bytes();
                let Some(pos) = keys[..count].iter().position(|&k| k == byte) else {
                    return;
                };
                for i in pos..count - 1 {
                    keys[i] = keys[i + 1];
                    let c = n.children[i + 1].load(Ordering::Relaxed);
                    n.children[i].store(c, Ordering::Release);
                }
                keys[count - 1] = 0;
                n.children[count - 1].store(null, Ordering::Release);
                n.keys.store(u32::from_le_bytes(keys), Ordering::Relaxed);
                n.header.set_num_children(count - 1);
            }
            NodeRef::N16(n) => {
                let count = n.header.num_children();
                let mut keys = n.load_keys();
                let Some(pos) = keys[..count].iter().position(|&k| k == byte) else {
                    return;
                };
                for i in pos..count - 1 {
                    keys[i] = keys[i + 1];
                    let c = n.children[i + 1].load(Ordering::Relaxed);
                    n.children[i].store(c, Ordering::Release);
                }
                keys[count - 1] = 0;
                n.children[count - 1].store(null, Ordering::Release);
                n.store_keys(keys);
                n.header.set_num_children(count - 1);
            }
            NodeRef::N48(n) => {
                let slot = n.child_indices[byte as usize].load(Ordering::Relaxed);
                if slot >= NODE48_EMPTY {
                    return;
                }
                n.child_indices[byte as usize].store(NODE48_EMPTY, Ordering::Relaxed);
                bitmap_clear(&n.child_bitmap, byte);
                n.children[slot as usize].store(null, Ordering::Release);
                n.header.set_num_children(n.header.num_children() - 1);
            }
            NodeRef::N256(n) => {
                if n.children[byte as usize].load(Ordering::Relaxed).is_null() {
                    return;
                }
                n.children[byte as usize].store(null, Ordering::Release);
                bitmap_clear(&n.child_bitmap, byte);
                n.header.set_num_children(n.header.num_children() - 1);
            }
        }
    }

    /// Copies this node's prefix, exact leaf and children into `dst`, an
    /// unpublished node of a larger type. Caller holds this node's latch, so
    /// the copy is exact.
    pub(crate) fn copy_into(&self, dst: &mut NodeHeader<A>) {
        dst.init_prefix(self.load_prefix().as_slice());
        dst.init_exact_leaf(self.exact_leaf.load(Ordering::Relaxed));
        self.for_each_child(|b, c| dst.push_child_unpublished(b, c));
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::raw::slot::{TaggedOffset, TAG_LEAF_OFFSET};
    use crate::sync::atomic::AtomicU32 as Slot32;

    fn leaf(n: u32) -> TaggedOffset {
        TaggedOffset::from_leaf(n * 8 + 64)
    }

    #[test]
    fn sizes_match_the_documented_layouts() {
        use std::mem::size_of;
        assert_eq!(size_of::<NodeHeader<Slot32>>(), 40);
        assert_eq!(size_of::<Node4<Slot32>>(), 64);
        assert_eq!(size_of::<Node16<Slot32>>(), 120);
        assert_eq!(size_of::<Node48<Slot32>>(), 520);
        assert_eq!(size_of::<Node256<Slot32>>(), 1096);
        let _ = TAG_LEAF_OFFSET;
    }

    #[test]
    fn node4_insert_find_remove() {
        let mut n = Node4::<Slot32>::new();
        n.header.push_child_unpublished(b'b', leaf(2));
        n.header.push_child_unpublished(b'a', leaf(1));
        n.header.push_child_unpublished(b'c', leaf(3));
        let h = &n.header;
        assert_eq!(h.num_children(), 3);
        assert_eq!(h.find_child(b'a'), Some(leaf(1)));
        assert_eq!(h.find_child(b'b'), Some(leaf(2)));
        assert_eq!(h.find_child(b'd'), None);
        assert_eq!(h.next_child(0), Some((b'a', leaf(1))));
        assert_eq!(h.next_child(b'b' as u16 + 1), Some((b'c', leaf(3))));
        assert_eq!(h.prev_child(b'b'), Some((b'b', leaf(2))));
        let w = h.latch.lock().unwrap();
        h.remove_child(&w, b'b');
        drop(w);
        assert_eq!(h.num_children(), 2);
        assert_eq!(h.find_child(b'b'), None);
        assert_eq!(h.next_child(b'a' as u16 + 1), Some((b'c', leaf(3))));
    }

    #[test]
    fn every_node_type_round_trips() {
        fn check(h: &NodeHeader<Slot32>, bytes: &[u8]) {
            for (i, &b) in bytes.iter().enumerate() {
                assert_eq!(h.find_child(b), Some(leaf(i as u32)), "byte {b}");
            }
            let mut seen = Vec::new();
            h.for_each_child(|b, _| seen.push(b));
            let mut sorted = bytes.to_vec();
            sorted.sort_unstable();
            assert_eq!(seen, sorted);
            let last = *sorted.last().unwrap();
            assert_eq!(h.prev_child(255).map(|x| x.0), Some(last));
        }
        let bytes: Vec<u8> = (0..=255u8).rev().collect();
        let mut n16 = Node16::<Slot32>::new();
        for (i, &b) in bytes[..16].iter().enumerate() {
            n16.header.push_child_unpublished(b, leaf(i as u32));
        }
        check(&n16.header, &bytes[..16]);
        let mut n48 = Node48::<Slot32>::new();
        for (i, &b) in bytes[..48].iter().enumerate() {
            n48.header.push_child_unpublished(b, leaf(i as u32));
        }
        check(&n48.header, &bytes[..48]);
        let mut n256 = Node256::<Slot32>::new();
        for (i, &b) in bytes.iter().enumerate() {
            n256.header.push_child_unpublished(b, leaf(i as u32));
        }
        check(&n256.header, &bytes);
    }

    #[test]
    fn bitmap_navigation_is_exact() {
        let bitmap: [AtomicU64; 4] = std::array::from_fn(|_| AtomicU64::new(0));
        for b in 0..=255u8 {
            assert_eq!(bitmap_next(&bitmap, b as u16), None);
            assert_eq!(bitmap_prev(&bitmap, b), None);
        }
        for b in (1..=255u8).step_by(2) {
            bitmap_set(&bitmap, b);
        }
        for b in 0..=254u8 {
            let want = if b % 2 == 1 { b } else { b + 1 };
            assert_eq!(bitmap_next(&bitmap, b as u16), Some(want));
        }
        assert_eq!(bitmap_next(&bitmap, 256), None);
        for b in 1..=255u8 {
            let want = if b % 2 == 1 { b } else { b - 1 };
            assert_eq!(bitmap_prev(&bitmap, b), Some(want));
        }
        assert_eq!(bitmap_prev(&bitmap, 0), None);
        for b in (1..=255u8).step_by(2) {
            bitmap_clear(&bitmap, b);
        }
        assert_eq!(bitmap_next(&bitmap, 0), None);
        assert_eq!(bitmap_prev(&bitmap, 255), None);
    }

    #[test]
    fn node48_full_capacity_and_reuse() {
        let mut n = Node48::<Slot32>::new();
        for b in 0..48u8 {
            n.header.push_child_unpublished(b * 5, leaf(b as u32));
        }
        assert!(n.header.is_full());
        let h = &n.header;
        let w = h.latch.lock().unwrap();
        h.remove_child(&w, 10);
        h.insert_child(&w, 251, leaf(99));
        h.replace_child(&w, 0, leaf(98));
        drop(w);
        assert_eq!(h.num_children(), 48);
        assert_eq!(h.find_child(10), None);
        assert_eq!(h.find_child(251), Some(leaf(99)));
        assert_eq!(h.find_child(0), Some(leaf(98)));
    }

    #[test]
    fn prefix_round_trips() {
        let mut n = Node4::<Slot32>::new();
        n.header.init_prefix(b"0123456789abcdef");
        assert_eq!(n.header.load_prefix().as_slice(), b"0123456789abcdef");
        let w = n.header.latch.lock().unwrap();
        n.header.store_prefix(&w, b"xyz");
        drop(w);
        assert_eq!(n.header.load_prefix().as_slice(), b"xyz");
    }
}
