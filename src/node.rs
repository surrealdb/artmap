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

//! # Adaptive Node Topologies
//!
//! Implements `Node4`, `Node16`, `Node48`, and `Node256` layouts with prefix compression.

use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};

use crate::latch::HybridLatch;
use crate::simd::find_child_node16;

pub const MAX_PREFIX_LEN: usize = 16;
pub const TAG_LEAF: usize = 0b01;
pub const NODE48_EMPTY: u8 = 48;

/// Discriminated node type indicator.
#[repr(u8)]
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum NodeType {
    Node4 = 0,
    Node16 = 1,
    Node48 = 2,
    Node256 = 3,
}

/// A leaf node holding the stored key-value pair.
#[repr(C, align(8))]
pub struct Leaf<K, V> {
    pub key: K,
    pub value: V,
}

impl<K, V> Leaf<K, V> {
    #[inline]
    pub fn new(key: K, value: V) -> Box<Self> {
        Box::new(Self { key, value })
    }
}

/// Tagged pointer wrapper distinguishing inner nodes from leaves.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct TaggedPtr {
    raw: usize,
}

impl TaggedPtr {
    pub const NULL: Self = Self { raw: 0 };

    #[inline]
    pub fn from_leaf<K, V>(ptr: *mut Leaf<K, V>) -> Self {
        let raw = ptr as usize;
        debug_assert_eq!(raw & TAG_LEAF, 0, "leaf pointer must be aligned");
        Self {
            raw: raw | TAG_LEAF,
        }
    }

    #[inline]
    pub fn from_inner(ptr: *mut NodeHeader) -> Self {
        let raw = ptr as usize;
        debug_assert_eq!(raw & TAG_LEAF, 0, "inner pointer must be aligned");
        Self { raw }
    }

    #[inline]
    pub fn from_raw(raw: *mut u8) -> Self {
        Self { raw: raw as usize }
    }

    #[inline]
    pub fn as_raw(self) -> *mut u8 {
        self.raw as *mut u8
    }

    #[inline]
    pub fn is_null(self) -> bool {
        self.raw == 0
    }

    #[inline]
    pub fn is_leaf(self) -> bool {
        (self.raw & TAG_LEAF) != 0
    }

    #[inline]
    pub fn as_leaf_ptr<K, V>(self) -> *mut Leaf<K, V> {
        debug_assert!(self.is_leaf());
        (self.raw & !TAG_LEAF) as *mut Leaf<K, V>
    }

    #[inline]
    pub fn as_inner_ptr(self) -> *mut NodeHeader {
        debug_assert!(!self.is_leaf());
        self.raw as *mut NodeHeader
    }
}

/// Common header present at offset 0 of every inner node.
#[repr(C)]
pub struct NodeHeader {
    pub latch: HybridLatch,
    pub node_type: NodeType,
    pub num_children: u8,
    pub prefix_len: u16,
    pub prefix: [u8; MAX_PREFIX_LEN],
    pub exact_leaf: AtomicPtr<u8>,
}

impl NodeHeader {
    #[inline]
    pub fn load_exact_leaf<K, V>(&self, order: Ordering) -> Option<*mut Leaf<K, V>> {
        let raw = self.exact_leaf.load(order);
        if raw.is_null() {
            None
        } else {
            Some(TaggedPtr::from_raw(raw).as_leaf_ptr::<K, V>())
        }
    }

    #[inline]
    pub fn new(node_type: NodeType, prefix: &[u8]) -> Self {
        let mut p = [0u8; MAX_PREFIX_LEN];
        let p_len = prefix.len().min(MAX_PREFIX_LEN);
        p[..p_len].copy_from_slice(&prefix[..p_len]);

        Self {
            latch: HybridLatch::new(),
            node_type,
            num_children: 0,
            prefix_len: prefix.len() as u16,
            prefix: p,
            exact_leaf: AtomicPtr::new(ptr::null_mut()),
        }
    }

    #[inline]
    pub fn prefix_slice(&self) -> &[u8] {
        let len = (self.prefix_len as usize).min(MAX_PREFIX_LEN);
        &self.prefix[..len]
    }

    /// Checks how many bytes of `self.prefix` match `key[depth..]`.
    /// Returns `(matched_bytes, is_complete_prefix_match)`.
    #[inline]
    pub fn match_prefix(&self, key: &[u8], depth: usize) -> (usize, bool) {
        if self.prefix_len == 0 {
            return (0, true);
        }
        let remaining_key = if depth < key.len() {
            &key[depth..]
        } else {
            &[]
        };
        let prefix = self.prefix_slice();
        let max_cmp = prefix.len().min(remaining_key.len());
        let mut matched = 0;
        while matched < max_cmp && prefix[matched] == remaining_key[matched] {
            matched += 1;
        }
        let complete = matched == self.prefix_len as usize;
        (matched, complete)
    }
}

/// Node with up to 4 children stored in sorted order.
#[repr(C)]
pub struct Node4 {
    pub header: NodeHeader,
    pub keys: [u8; 4],
    pub children: [AtomicPtr<u8>; 4],
}

impl Node4 {
    pub fn new(prefix: &[u8]) -> Box<Self> {
        Box::new(Self {
            header: NodeHeader::new(NodeType::Node4, prefix),
            keys: [0; 4],
            children: [const { AtomicPtr::new(ptr::null_mut()) }; 4],
        })
    }

    #[inline]
    pub fn find_child(&self, needle: u8) -> Option<TaggedPtr> {
        let count = self.header.num_children as usize;
        if count > 0 && self.keys[0] == needle {
            return Some(TaggedPtr::from_raw(
                self.children[0].load(Ordering::Acquire),
            ));
        }
        if count > 1 && self.keys[1] == needle {
            return Some(TaggedPtr::from_raw(
                self.children[1].load(Ordering::Acquire),
            ));
        }
        if count > 2 && self.keys[2] == needle {
            return Some(TaggedPtr::from_raw(
                self.children[2].load(Ordering::Acquire),
            ));
        }
        if count > 3 && self.keys[3] == needle {
            return Some(TaggedPtr::from_raw(
                self.children[3].load(Ordering::Acquire),
            ));
        }
        None
    }

    pub fn insert_child(&mut self, key: u8, child: TaggedPtr) {
        let count = self.header.num_children as usize;
        debug_assert!(count < 4);

        let pos = self.keys[..count].partition_point(|&k| k < key);
        for i in (pos..count).rev() {
            self.keys[i + 1] = self.keys[i];
            let val = self.children[i].load(Ordering::Relaxed);
            self.children[i + 1].store(val, Ordering::Relaxed);
        }
        self.keys[pos] = key;
        self.children[pos].store(child.as_raw(), Ordering::Release);
        self.header.num_children += 1;
    }

    pub fn remove_child(&mut self, key: u8) -> Option<TaggedPtr> {
        let count = self.header.num_children as usize;
        let pos = self.keys[..count].iter().position(|&k| k == key)?;
        let old = TaggedPtr::from_raw(self.children[pos].load(Ordering::Relaxed));
        for i in pos..(count - 1) {
            self.keys[i] = self.keys[i + 1];
            let val = self.children[i + 1].load(Ordering::Relaxed);
            self.children[i].store(val, Ordering::Relaxed);
        }
        self.children[count - 1].store(ptr::null_mut(), Ordering::Relaxed);
        self.header.num_children -= 1;
        Some(old)
    }
}

/// Node with up to 16 children searched using SIMD.
#[repr(C)]
pub struct Node16 {
    pub header: NodeHeader,
    pub keys: [u8; 16],
    pub children: [AtomicPtr<u8>; 16],
}

impl Node16 {
    pub fn new(prefix: &[u8]) -> Box<Self> {
        Box::new(Self {
            header: NodeHeader::new(NodeType::Node16, prefix),
            keys: [0; 16],
            children: [const { AtomicPtr::new(ptr::null_mut()) }; 16],
        })
    }

    #[inline]
    pub fn find_child(&self, needle: u8) -> Option<TaggedPtr> {
        let idx = find_child_node16(&self.keys, self.header.num_children as usize, needle)?;
        let child = self.children[idx].load(Ordering::Acquire);
        Some(TaggedPtr::from_raw(child))
    }

    pub fn insert_child(&mut self, key: u8, child: TaggedPtr) {
        let count = self.header.num_children as usize;
        debug_assert!(count < 16);

        let pos = self.keys[..count].partition_point(|&k| k < key);
        for i in (pos..count).rev() {
            self.keys[i + 1] = self.keys[i];
            let val = self.children[i].load(Ordering::Relaxed);
            self.children[i + 1].store(val, Ordering::Relaxed);
        }
        self.keys[pos] = key;
        self.children[pos].store(child.as_raw(), Ordering::Release);
        self.header.num_children += 1;
    }

    pub fn remove_child(&mut self, key: u8) -> Option<TaggedPtr> {
        let count = self.header.num_children as usize;
        let pos = self.keys[..count].iter().position(|&k| k == key)?;
        let old = TaggedPtr::from_raw(self.children[pos].load(Ordering::Relaxed));
        for i in pos..(count - 1) {
            self.keys[i] = self.keys[i + 1];
            let val = self.children[i + 1].load(Ordering::Relaxed);
            self.children[i].store(val, Ordering::Relaxed);
        }
        self.children[count - 1].store(ptr::null_mut(), Ordering::Relaxed);
        self.header.num_children -= 1;
        Some(old)
    }
}

/// Node with up to 48 children indexed by a 256-byte mapping array.
#[repr(C)]
pub struct Node48 {
    pub header: NodeHeader,
    pub child_indices: [u8; 256],
    pub children: [AtomicPtr<u8>; 48],
}

impl Node48 {
    pub fn new(prefix: &[u8]) -> Box<Self> {
        Box::new(Self {
            header: NodeHeader::new(NodeType::Node48, prefix),
            child_indices: [NODE48_EMPTY; 256],
            children: [const { AtomicPtr::new(ptr::null_mut()) }; 48],
        })
    }

    #[inline]
    pub fn find_child(&self, needle: u8) -> Option<TaggedPtr> {
        let slot = self.child_indices[needle as usize];
        if slot == NODE48_EMPTY {
            None
        } else {
            let child = self.children[slot as usize].load(Ordering::Acquire);
            Some(TaggedPtr::from_raw(child))
        }
    }

    pub fn insert_child(&mut self, key: u8, child: TaggedPtr) {
        let count = self.header.num_children as usize;
        debug_assert!(count < 48);

        let slot = (0..48)
            .find(|&i| self.children[i].load(Ordering::Relaxed).is_null())
            .expect("Node48 has room");

        self.children[slot].store(child.as_raw(), Ordering::Release);
        self.child_indices[key as usize] = slot as u8;
        self.header.num_children += 1;
    }

    pub fn remove_child(&mut self, key: u8) -> Option<TaggedPtr> {
        let slot = self.child_indices[key as usize];
        if slot == NODE48_EMPTY {
            return None;
        }
        self.child_indices[key as usize] = NODE48_EMPTY;
        let old = TaggedPtr::from_raw(self.children[slot as usize].load(Ordering::Relaxed));
        self.children[slot as usize].store(ptr::null_mut(), Ordering::Relaxed);
        self.header.num_children -= 1;
        Some(old)
    }
}

/// Node with up to 256 children directly indexed by byte.
#[repr(C)]
pub struct Node256 {
    pub header: NodeHeader,
    pub children: [AtomicPtr<u8>; 256],
}

impl Node256 {
    pub fn new(prefix: &[u8]) -> Box<Self> {
        Box::new(Self {
            header: NodeHeader::new(NodeType::Node256, prefix),
            children: [const { AtomicPtr::new(ptr::null_mut()) }; 256],
        })
    }

    #[inline]
    pub fn find_child(&self, needle: u8) -> Option<TaggedPtr> {
        let child = self.children[needle as usize].load(Ordering::Acquire);
        if child.is_null() {
            None
        } else {
            Some(TaggedPtr::from_raw(child))
        }
    }

    pub fn insert_child(&mut self, key: u8, child: TaggedPtr) {
        debug_assert!(self.children[key as usize]
            .load(Ordering::Relaxed)
            .is_null());
        self.children[key as usize].store(child.as_raw(), Ordering::Release);
        self.header.num_children += 1;
    }

    pub fn remove_child(&mut self, key: u8) -> Option<TaggedPtr> {
        let old = self.children[key as usize].load(Ordering::Relaxed);
        if old.is_null() {
            None
        } else {
            self.children[key as usize].store(ptr::null_mut(), Ordering::Relaxed);
            self.header.num_children -= 1;
            Some(TaggedPtr::from_raw(old))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tagged_ptr() {
        let leaf = Leaf::new(10u32, 20u32);
        let leaf_ptr = Box::into_raw(leaf);
        let tagged = TaggedPtr::from_leaf(leaf_ptr);
        assert!(tagged.is_leaf());
        assert_eq!(unsafe { &*tagged.as_leaf_ptr::<u32, u32>() }.key, 10);
        assert_eq!(unsafe { &*tagged.as_leaf_ptr::<u32, u32>() }.value, 20);

        unsafe { drop(Box::from_raw(leaf_ptr)) };

        let n4 = Node4::new(b"test");
        let n4_ptr = Box::into_raw(n4);
        let tagged_inner = TaggedPtr::from_inner(unsafe { &mut (*n4_ptr).header });
        assert!(!tagged_inner.is_leaf());
        assert_eq!(
            unsafe { (*tagged_inner.as_inner_ptr()).prefix_slice() },
            b"test"
        );

        unsafe { drop(Box::from_raw(n4_ptr)) };
    }

    #[test]
    fn test_node4_lifecycle() {
        let mut n4 = Node4::new(b"");
        assert_eq!(n4.header.num_children, 0);

        let dummy_leaf1 = TaggedPtr::from_raw(std::ptr::dangling_mut::<u8>());
        let dummy_leaf2 = TaggedPtr::from_raw(std::ptr::dangling_mut::<u8>());
        let dummy_leaf3 = TaggedPtr::from_raw(std::ptr::dangling_mut::<u8>());

        n4.insert_child(b'b', dummy_leaf2);
        n4.insert_child(b'a', dummy_leaf1);
        n4.insert_child(b'c', dummy_leaf3);

        assert_eq!(n4.header.num_children, 3);
        assert_eq!(&n4.keys[..3], b"abc");
        assert_eq!(n4.find_child(b'a'), Some(dummy_leaf1));
        assert_eq!(n4.find_child(b'b'), Some(dummy_leaf2));
        assert_eq!(n4.find_child(b'c'), Some(dummy_leaf3));
        assert_eq!(n4.find_child(b'd'), None);

        assert_eq!(n4.remove_child(b'b'), Some(dummy_leaf2));
        assert_eq!(n4.header.num_children, 2);
        assert_eq!(&n4.keys[..2], b"ac");
        assert_eq!(n4.find_child(b'b'), None);
    }

    #[test]
    fn test_node48_lifecycle() {
        let mut n48 = Node48::new(b"");
        let dummy = TaggedPtr::from_raw(std::ptr::dangling_mut::<u8>());
        n48.insert_child(b'z', dummy);
        assert_eq!(n48.find_child(b'z'), Some(dummy));
        assert_eq!(n48.find_child(b'a'), None);
        assert_eq!(n48.remove_child(b'z'), Some(dummy));
        assert_eq!(n48.find_child(b'z'), None);
    }
}
