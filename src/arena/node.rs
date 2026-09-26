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

use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, Ordering};

use crate::latch::HybridLatch;
use crate::node::{NodeType, MAX_PREFIX_LEN, NODE48_EMPTY};
use crate::simd::find_child_node16;

/// Tag bit distinguishing a Leaf offset from an Inner Node offset.
pub const TAG_LEAF: u32 = 0b01;

/// A 32-bit tagged arena offset pointing to either a Leaf or an Inner Node.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
#[repr(transparent)]
pub struct TaggedOffset(pub u32);

impl TaggedOffset {
    pub const NULL: Self = Self(0);

    #[inline(always)]
    pub fn from_leaf(offset: u32) -> Self {
        debug_assert_eq!(offset & TAG_LEAF, 0, "offset must be 8-byte aligned");
        Self(offset | TAG_LEAF)
    }

    #[inline(always)]
    pub fn from_inner(offset: u32) -> Self {
        debug_assert_eq!(offset & TAG_LEAF, 0, "offset must be 8-byte aligned");
        Self(offset)
    }

    #[inline(always)]
    pub fn is_null(self) -> bool {
        self.0 == 0
    }

    #[inline(always)]
    pub fn is_leaf(self) -> bool {
        (self.0 & TAG_LEAF) != 0
    }

    #[inline(always)]
    pub fn leaf_offset(self) -> u32 {
        debug_assert!(self.is_leaf());
        self.0 & !TAG_LEAF
    }

    #[inline(always)]
    pub fn inner_offset(self) -> u32 {
        debug_assert!(!self.is_leaf());
        self.0
    }

    #[inline(always)]
    pub fn raw(self) -> u32 {
        self.0
    }
}

/// A leaf node allocated within the arena.
#[repr(C, align(8))]
pub struct Leaf<K, V> {
    pub removed: AtomicBool,
    pub _pad: [u8; 3],
    /// 32-bit offset to an older version of this key, or 0 if none.
    pub next_version_offset: AtomicU32,
    /// 64-bit monotonic sequence number or timestamp.
    pub version: u64,
    pub key: K,
    pub value: V,
}

impl<K, V> Leaf<K, V> {
    #[inline]
    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn init(ptr: *mut Self, key: K, version: u64, value: V) {
        unsafe {
            std::ptr::write(
                ptr,
                Self {
                    removed: AtomicBool::new(false),
                    _pad: [0; 3],
                    next_version_offset: AtomicU32::new(0),
                    version,
                    key,
                    value,
                },
            );
        }
    }
}

/// Common header for all arena inner nodes, occupying 40 bytes.
#[repr(C, align(8))]
pub struct NodeHeader {
    pub latch: HybridLatch,
    pub node_type: NodeType,
    pub num_children: AtomicU16,
    pub prefix_len: u16,
    pub prefix: [u8; MAX_PREFIX_LEN],
    pub _pad: u8,
    /// 32-bit TaggedOffset pointing to an exact leaf matching this prefix.
    pub exact_leaf: AtomicU32,
}

const _: () = assert!(std::mem::size_of::<NodeHeader>() == 40);

impl NodeHeader {
    #[inline]
    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn init(ptr: *mut Self, node_type: NodeType, prefix: &[u8]) {
        let mut p = [0u8; MAX_PREFIX_LEN];
        let p_len = prefix.len().min(MAX_PREFIX_LEN);
        p[..p_len].copy_from_slice(&prefix[..p_len]);

        unsafe {
            std::ptr::write(
                ptr,
                Self {
                    latch: HybridLatch::new(),
                    node_type,
                    num_children: AtomicU16::new(0),
                    prefix_len: prefix.len() as u16,
                    prefix: p,
                    _pad: 0,
                    exact_leaf: AtomicU32::new(0),
                },
            );
        }
    }

    #[inline(always)]
    pub fn num_children(&self) -> u16 {
        self.num_children.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn inc_num_children(&self) -> u16 {
        self.num_children.fetch_add(1, Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn dec_num_children(&self) -> u16 {
        self.num_children.fetch_sub(1, Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn prefix_slice(&self) -> &[u8] {
        let len = (self.prefix_len as usize).min(MAX_PREFIX_LEN);
        &self.prefix[..len]
    }

    #[inline(always)]
    pub fn match_prefix(&self, key: &[u8], depth: usize) -> (usize, bool) {
        if self.prefix_len == 0 {
            return (0, true);
        }
        let remaining = if depth < key.len() {
            &key[depth..]
        } else {
            &[]
        };
        let prefix = self.prefix_slice();
        let max_cmp = prefix.len().min(remaining.len());
        let mut matched = 0;
        while matched < max_cmp && prefix[matched] == remaining[matched] {
            matched += 1;
        }
        let complete = matched == self.prefix_len as usize;
        (matched, complete)
    }
}

/// Node with up to 4 children (fits in a single 64-byte cache line).
#[repr(C, align(8))]
pub struct Node4 {
    pub header: NodeHeader,
    pub keys: [u8; 4],
    pub children: [AtomicU32; 4],
    pub _pad: [u8; 4],
}

const _: () = assert!(std::mem::size_of::<Node4>() == 64);

impl Node4 {
    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn init(ptr: *mut Self, prefix: &[u8]) {
        NodeHeader::init(&mut (*ptr).header, NodeType::Node4, prefix);
        (*ptr).keys = [0; 4];
        (*ptr).children = [const { AtomicU32::new(0) }; 4];
    }

    pub fn insert_child(&mut self, key: u8, child: TaggedOffset) {
        let count = self.header.num_children() as usize;
        debug_assert!(count < 4);
        let pos = self.keys[..count].partition_point(|&k| k < key);
        for i in (pos..count).rev() {
            self.keys[i + 1] = self.keys[i];
            let val = self.children[i].load(Ordering::Relaxed);
            self.children[i + 1].store(val, Ordering::Relaxed);
        }
        self.keys[pos] = key;
        self.children[pos].store(child.raw(), Ordering::Release);
        self.header.inc_num_children();
    }
}

/// Node with up to 16 children (120 bytes, children fit in 64 bytes).
#[repr(C, align(8))]
pub struct Node16 {
    pub header: NodeHeader,
    pub keys: [u8; 16],
    pub children: [AtomicU32; 16],
}

impl Node16 {
    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn init(ptr: *mut Self, prefix: &[u8]) {
        NodeHeader::init(&mut (*ptr).header, NodeType::Node16, prefix);
        (*ptr).keys = [0; 16];
        (*ptr).children = [const { AtomicU32::new(0) }; 16];
    }

    pub fn insert_child(&mut self, key: u8, child: TaggedOffset) {
        let count = self.header.num_children() as usize;
        debug_assert!(count < 16);
        let pos = self.keys[..count].partition_point(|&k| k < key);
        for i in (pos..count).rev() {
            self.keys[i + 1] = self.keys[i];
            let val = self.children[i].load(Ordering::Relaxed);
            self.children[i + 1].store(val, Ordering::Relaxed);
        }
        self.keys[pos] = key;
        self.children[pos].store(child.raw(), Ordering::Release);
        self.header.inc_num_children();
    }
}

/// Node with up to 48 children (520 bytes, with 256-bit presence bitmap).
#[repr(C, align(8))]
pub struct Node48 {
    pub header: NodeHeader,
    pub child_indices: [u8; 256],
    pub child_bitmap: [AtomicU64; 4],
    pub children: [AtomicU32; 48],
}

const _: () = assert!(std::mem::size_of::<Node48>() == 520);

impl Node48 {
    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn init(ptr: *mut Self, prefix: &[u8]) {
        NodeHeader::init(&mut (*ptr).header, NodeType::Node48, prefix);
        (*ptr).child_indices = [NODE48_EMPTY; 256];
        (*ptr).child_bitmap = [const { AtomicU64::new(0) }; 4];
        (*ptr).children = [const { AtomicU32::new(0) }; 48];
    }

    pub fn insert_child(&mut self, key: u8, child: TaggedOffset) {
        let count = self.header.num_children() as usize;
        debug_assert!(count < 48);
        let slot = (0..48)
            .find(|&i| self.children[i].load(Ordering::Relaxed) == 0)
            .expect("Node48 has room");
        self.children[slot].store(child.raw(), Ordering::Release);
        self.child_indices[key as usize] = slot as u8;
        set_bitmap_bit(&self.child_bitmap, key);
        self.header.inc_num_children();
    }
}

/// Node with up to 256 children (1,096 bytes, with 256-bit presence bitmap).
#[repr(C, align(8))]
pub struct Node256 {
    pub header: NodeHeader,
    pub child_bitmap: [AtomicU64; 4],
    pub children: [AtomicU32; 256],
}

const _: () = assert!(std::mem::size_of::<Node256>() == 1096);

impl Node256 {
    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn init(ptr: *mut Self, prefix: &[u8]) {
        NodeHeader::init(&mut (*ptr).header, NodeType::Node256, prefix);
        (*ptr).child_bitmap = [const { AtomicU64::new(0) }; 4];
        (*ptr).children = [const { AtomicU32::new(0) }; 256];
    }

    pub fn insert_child(&mut self, key: u8, child: TaggedOffset) {
        self.children[key as usize].store(child.raw(), Ordering::Release);
        set_bitmap_bit(&self.child_bitmap, key);
        self.header.inc_num_children();
    }
}

#[inline(always)]
pub fn set_bitmap_bit(bitmap: &[AtomicU64; 4], byte: u8) {
    let word = (byte / 64) as usize;
    let bit = byte % 64;
    bitmap[word].fetch_or(1u64 << bit, Ordering::Release);
}

#[inline(always)]
pub fn clear_bitmap_bit(bitmap: &[AtomicU64; 4], byte: u8) {
    let word = (byte / 64) as usize;
    let bit = byte % 64;
    bitmap[word].fetch_and(!(1u64 << bit), Ordering::Release);
}

#[inline(always)]
#[allow(clippy::needless_range_loop)]
pub fn next_present_byte(bitmap: &[AtomicU64; 4], min_byte: u8) -> Option<u8> {
    let start_word = (min_byte / 64) as usize;
    let start_bit = min_byte % 64;

    for word_idx in start_word..4 {
        let mut word = bitmap[word_idx].load(Ordering::Acquire);
        if word_idx == start_word {
            word &= !0u64 << start_bit;
        }
        if word != 0 {
            let bit = word.trailing_zeros();
            return Some((word_idx * 64 + bit as usize) as u8);
        }
    }
    None
}

#[inline(always)]
pub fn prev_present_byte(bitmap: &[AtomicU64; 4], max_byte: u8) -> Option<u8> {
    let end_word = (max_byte / 64) as usize;
    let end_bit = max_byte % 64;

    for word_idx in (0..=end_word).rev() {
        let mut word = bitmap[word_idx].load(Ordering::Acquire);
        if word_idx == end_word && end_bit < 63 {
            word &= (1u64 << (end_bit + 1)) - 1;
        }
        if word != 0 {
            let bit = 63 - word.leading_zeros();
            return Some((word_idx * 64 + bit as usize) as u8);
        }
    }
    None
}

#[allow(clippy::missing_safety_doc)]
pub unsafe fn find_child(header: *mut NodeHeader, byte: u8) -> Option<TaggedOffset> {
    let n_type = (*header).node_type;
    match n_type {
        NodeType::Node4 => {
            let n = &*(header as *const Node4);
            let count = n.header.num_children() as usize;
            for i in 0..count {
                if n.keys[i] == byte {
                    let raw = n.children[i].load(Ordering::Acquire);
                    return if raw == 0 {
                        None
                    } else {
                        Some(TaggedOffset(raw))
                    };
                }
            }
            None
        }
        NodeType::Node16 => {
            let n = &*(header as *const Node16);
            let count = n.header.num_children() as usize;
            if let Some(idx) = find_child_node16(&n.keys, count, byte) {
                let raw = n.children[idx].load(Ordering::Acquire);
                if raw == 0 {
                    None
                } else {
                    Some(TaggedOffset(raw))
                }
            } else {
                None
            }
        }
        NodeType::Node48 => {
            let n = &*(header as *const Node48);
            let slot = n.child_indices[byte as usize];
            if slot == NODE48_EMPTY {
                None
            } else {
                let raw = n.children[slot as usize].load(Ordering::Acquire);
                if raw == 0 {
                    None
                } else {
                    Some(TaggedOffset(raw))
                }
            }
        }
        NodeType::Node256 => {
            let n = &*(header as *const Node256);
            let raw = n.children[byte as usize].load(Ordering::Acquire);
            if raw == 0 {
                None
            } else {
                Some(TaggedOffset(raw))
            }
        }
    }
}
