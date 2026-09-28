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

//! # Multi-Version Range Iterators
//!
//! Provides bidirectional range scanning over MVCC versioned entries in a [`VersionedArtMap`](crate::versioned::VersionedArtMap).

use crossbeam_epoch::Guard;
use std::sync::atomic::Ordering;

use crate::iter::{child_pos_for_byte, next_child_in_node, BoundKey};
use crate::key::AsBytes;
use crate::node::{
    next_present_byte, prev_present_byte, Node16, Node256, Node4, Node48, NodeHeader, NodeType,
    TaggedPtr, VersionedLeaf, NODE48_EMPTY,
};
use crate::versioned::entry::VersionedEntryRef;
use crate::versioned::tree::VersionedTree;

const MAX_STACK_DEPTH: usize = 16;

#[derive(Clone, Copy)]
struct CursorFrame {
    node: *mut NodeHeader,
    current_pos: usize,
}

impl CursorFrame {
    const NULL: Self = Self {
        node: std::ptr::null_mut(),
        current_pos: 0,
    };
}

/// An iterator over a range of versioned entries in a [`VersionedArtMap`](crate::versioned::VersionedArtMap).
pub struct Range<'a, K: AsBytes + Send + 'static, V: Send + Clone + 'static> {
    tree: &'a VersionedTree<K, V>,
    _guard: Guard,
    start_bound: BoundKey,
    end_bound: BoundKey,
    stack: [CursorFrame; MAX_STACK_DEPTH],
    stack_len: usize,
    stack_overflow: Vec<CursorFrame>,
    last_leaf_front: Option<*mut VersionedLeaf<K, V>>,
    last_leaf_back: Option<*mut VersionedLeaf<K, V>>,
    exhausted: bool,
}

impl<'a, K: AsBytes + Send + 'static, V: Send + Clone + 'static> Range<'a, K, V> {
    pub(crate) fn new(
        tree: &'a VersionedTree<K, V>,
        start_bound: BoundKey,
        end_bound: BoundKey,
    ) -> Self {
        let guard = crossbeam_epoch::pin();
        Self {
            tree,
            _guard: guard,
            start_bound,
            end_bound,
            stack: [CursorFrame::NULL; MAX_STACK_DEPTH],
            stack_len: 0,
            stack_overflow: Vec::new(),
            last_leaf_front: None,
            last_leaf_back: None,
            exhausted: false,
        }
    }

    #[inline(always)]
    fn stack_push(&mut self, frame: CursorFrame) {
        if self.stack_len < MAX_STACK_DEPTH && self.stack_overflow.is_empty() {
            self.stack[self.stack_len] = frame;
            self.stack_len += 1;
        } else {
            self.stack_overflow.push(frame);
        }
    }

    #[inline(always)]
    fn stack_last_mut(&mut self) -> Option<&mut CursorFrame> {
        if let Some(f) = self.stack_overflow.last_mut() {
            Some(f)
        } else if self.stack_len > 0 {
            Some(&mut self.stack[self.stack_len - 1])
        } else {
            None
        }
    }

    #[inline(always)]
    fn stack_pop(&mut self) -> Option<CursorFrame> {
        if let Some(f) = self.stack_overflow.pop() {
            Some(f)
        } else if self.stack_len > 0 {
            self.stack_len -= 1;
            Some(self.stack[self.stack_len])
        } else {
            None
        }
    }

    #[inline(always)]
    fn stack_clear(&mut self) {
        self.stack_len = 0;
        self.stack_overflow.clear();
    }

    #[inline(always)]
    fn set_cursor_front(&mut self, ptr: *mut VersionedLeaf<K, V>) {
        self.last_leaf_front = Some(ptr);
    }

    #[inline(always)]
    fn cursor_front(&self) -> Option<&[u8]> {
        self.last_leaf_front.map(|p| unsafe { (*p).key.as_bytes() })
    }

    #[inline(always)]
    fn set_cursor_back(&mut self, ptr: *mut VersionedLeaf<K, V>) {
        self.last_leaf_back = Some(ptr);
    }

    #[inline(always)]
    fn cursor_back(&self) -> Option<&[u8]> {
        self.last_leaf_back.map(|p| unsafe { (*p).key.as_bytes() })
    }

    #[inline]
    fn push_and_descend_left(&mut self, mut ptr: TaggedPtr) -> Option<*mut VersionedLeaf<K, V>> {
        while !ptr.is_null() {
            if ptr.is_leaf() {
                return Some(ptr.as_versioned_leaf_ptr());
            }
            let header = ptr.as_inner_ptr();
            let exact = unsafe { (*header).load_exact_versioned_leaf::<K, V>(Ordering::Acquire) };
            if let Some(leaf_ptr) = exact {
                self.stack_push(CursorFrame {
                    node: header,
                    current_pos: usize::MAX,
                });
                return Some(leaf_ptr);
            }

            match unsafe { next_child_in_node(header, usize::MAX) } {
                Some((pos, child)) => {
                    self.stack_push(CursorFrame {
                        node: header,
                        current_pos: pos,
                    });
                    ptr = child;
                }
                None => return None,
            }
        }
        None
    }

    fn seek_to_key(&mut self, target_key: &[u8]) {
        self.stack_clear();
        let root_ptr = TaggedPtr::from_raw(self.tree.raw_root().as_raw());
        if root_ptr.is_null() || root_ptr.is_leaf() {
            return;
        }

        let mut current = root_ptr;
        let mut depth = 0;

        while !current.is_null() && !current.is_leaf() {
            let header = current.as_inner_ptr();
            let (_matched, is_full) = unsafe { (*header).match_prefix(target_key, depth) };
            if !is_full {
                return;
            }

            depth += unsafe { (*header).prefix_len as usize };

            if depth == target_key.len() {
                self.stack_push(CursorFrame {
                    node: header,
                    current_pos: usize::MAX,
                });
                return;
            }

            let next_byte = target_key[depth];
            match unsafe { child_pos_for_byte(header, next_byte) } {
                Some((pos, child)) => {
                    self.stack_push(CursorFrame {
                        node: header,
                        current_pos: pos,
                    });
                    if child.is_leaf() {
                        return;
                    }
                    current = child;
                    depth += 1;
                }
                None => return,
            }
        }
    }

    #[inline]
    fn advance_forward(&mut self) -> Option<*mut VersionedLeaf<K, V>> {
        while let Some(frame) = self.stack_last_mut() {
            let header = frame.node;
            match unsafe { next_child_in_node(header, frame.current_pos) } {
                Some((next_pos, child)) => {
                    frame.current_pos = next_pos;
                    return self.push_and_descend_left(child);
                }
                None => {
                    self.stack_pop();
                }
            }
        }
        None
    }
}

impl<'a, K: AsBytes + Send + 'static, V: Send + Clone + 'static> Iterator for Range<'a, K, V> {
    type Item = VersionedEntryRef<'a, K, V>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }

        loop {
            let mut leaf_ptr = if self.last_leaf_front.is_none() {
                let (search_key, include_equal) = match &self.start_bound {
                    BoundKey::IncludedSmall(b, len) => (&b[..*len as usize], true),
                    BoundKey::ExcludedSmall(b, len) => (&b[..*len as usize], false),
                    BoundKey::IncludedHeap(b) => (b.as_ref(), true),
                    BoundKey::ExcludedHeap(b) => (b.as_ref(), false),
                    BoundKey::Unbounded => (&[][..], true),
                };

                let root_ptr = TaggedPtr::from_raw(self.tree.raw_root().as_raw());
                if root_ptr.is_null() {
                    self.exhausted = true;
                    return None;
                }

                if root_ptr.is_leaf() {
                    let leaf = unsafe { &*root_ptr.as_versioned_leaf_ptr::<K, V>() };
                    let k = leaf.key.as_bytes();
                    let cmp = k.cmp(search_key);
                    if (include_equal && cmp >= std::cmp::Ordering::Equal)
                        || (!include_equal && cmp == std::cmp::Ordering::Greater)
                    {
                        root_ptr.as_versioned_leaf_ptr::<K, V>()
                    } else {
                        self.exhausted = true;
                        return None;
                    }
                } else if (search_key.is_empty() || (search_key == [0u8] && include_equal))
                    && include_equal
                {
                    match self.push_and_descend_left(root_ptr) {
                        Some(ptr) => ptr,
                        None => {
                            self.exhausted = true;
                            return None;
                        }
                    }
                } else {
                    let ptr = match self.tree.find_successor(search_key, include_equal) {
                        Some(p) => p,
                        None => {
                            self.exhausted = true;
                            return None;
                        }
                    };
                    let leaf = unsafe { &*ptr };
                    self.seek_to_key(leaf.key.as_bytes());
                    ptr
                }
            } else {
                match self.advance_forward() {
                    Some(ptr) => ptr,
                    None => {
                        let prev_k = self.cursor_front().unwrap();
                        let ptr = match self.tree.find_successor(prev_k, false) {
                            Some(p) => p,
                            None => {
                                self.exhausted = true;
                                return None;
                            }
                        };
                        let leaf = unsafe { &*ptr };
                        self.seek_to_key(leaf.key.as_bytes());
                        ptr
                    }
                }
            };

            let mut leaf = unsafe { &*leaf_ptr };
            let mut k_bytes = leaf.key.as_bytes();

            if let Some(prev_k) = self.cursor_front() {
                if k_bytes <= prev_k {
                    match self.tree.find_successor(prev_k, false) {
                        Some(p) => {
                            leaf_ptr = p;
                            leaf = unsafe { &*leaf_ptr };
                            k_bytes = leaf.key.as_bytes();
                            self.seek_to_key(k_bytes);
                        }
                        None => {
                            self.exhausted = true;
                            return None;
                        }
                    }
                }
            }

            // Check upper range bound
            match &self.end_bound {
                BoundKey::IncludedSmall(b, len) => {
                    if k_bytes > &b[..*len as usize] {
                        self.exhausted = true;
                        return None;
                    }
                }
                BoundKey::ExcludedSmall(b, len) => {
                    if k_bytes >= &b[..*len as usize] {
                        self.exhausted = true;
                        return None;
                    }
                }
                BoundKey::IncludedHeap(b) => {
                    if k_bytes > b.as_ref() {
                        self.exhausted = true;
                        return None;
                    }
                }
                BoundKey::ExcludedHeap(b) => {
                    if k_bytes >= b.as_ref() {
                        self.exhausted = true;
                        return None;
                    }
                }
                BoundKey::Unbounded => {}
            }

            // Check overlap with backward cursor
            if let Some(back_k) = self.cursor_back() {
                if k_bytes >= back_k {
                    self.exhausted = true;
                    return None;
                }
            }

            self.set_cursor_front(leaf_ptr);

            if !leaf.is_removed() {
                return Some(VersionedEntryRef {
                    leaf_ptr,
                    _marker: std::marker::PhantomData,
                });
            }
        }
    }
}

impl<'a, K: AsBytes + Send + 'static, V: Send + Clone + 'static> DoubleEndedIterator
    for Range<'a, K, V>
{
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }

        loop {
            let (search_key, include_equal) = if let Some(back_k) = self.cursor_back() {
                (back_k, false)
            } else {
                match &self.end_bound {
                    BoundKey::IncludedSmall(b, len) => (&b[..*len as usize], true),
                    BoundKey::ExcludedSmall(b, len) => (&b[..*len as usize], false),
                    BoundKey::IncludedHeap(b) => (b.as_ref(), true),
                    BoundKey::ExcludedHeap(b) => (b.as_ref(), false),
                    BoundKey::Unbounded => (&[0xFF; 64][..], true),
                }
            };

            let leaf_ptr = self.tree.find_predecessor(search_key, include_equal)?;
            let leaf = unsafe { &*leaf_ptr };
            let k_bytes = leaf.key.as_bytes();

            // Check lower range bound
            match &self.start_bound {
                BoundKey::IncludedSmall(b, len) => {
                    if k_bytes < &b[..*len as usize] {
                        self.exhausted = true;
                        return None;
                    }
                }
                BoundKey::ExcludedSmall(b, len) => {
                    if k_bytes <= &b[..*len as usize] {
                        self.exhausted = true;
                        return None;
                    }
                }
                BoundKey::IncludedHeap(b) => {
                    if k_bytes < b.as_ref() {
                        self.exhausted = true;
                        return None;
                    }
                }
                BoundKey::ExcludedHeap(b) => {
                    if k_bytes <= b.as_ref() {
                        self.exhausted = true;
                        return None;
                    }
                }
                BoundKey::Unbounded => {}
            }

            // Check overlap with forward cursor
            if let Some(front_k) = self.cursor_front() {
                if k_bytes <= front_k {
                    self.exhausted = true;
                    return None;
                }
            }

            self.set_cursor_back(leaf_ptr);

            if !leaf.is_removed() {
                return Some(VersionedEntryRef {
                    leaf_ptr,
                    _marker: std::marker::PhantomData,
                });
            }
        }
    }
}

pub(crate) unsafe fn first_leaf_in_subtree<K, V>(
    ptr: TaggedPtr,
) -> Option<*mut VersionedLeaf<K, V>> {
    if ptr.is_null() {
        return None;
    }
    if ptr.is_leaf() {
        return Some(ptr.as_versioned_leaf_ptr());
    }
    let header = &*ptr.as_inner_ptr();
    if let Some(leaf) = header.load_exact_versioned_leaf::<K, V>(Ordering::Acquire) {
        return Some(leaf);
    }

    match header.node_type {
        NodeType::Node4 => {
            let n = &*(ptr.as_inner_ptr() as *const Node4);
            for i in 0..n.header.num_children as usize {
                let child = TaggedPtr::from_raw(n.children[i].load(Ordering::Acquire));
                if !child.is_null() {
                    if let Some(leaf) = first_leaf_in_subtree(child) {
                        return Some(leaf);
                    }
                }
            }
            None
        }
        NodeType::Node16 => {
            let n = &*(ptr.as_inner_ptr() as *const Node16);
            for i in 0..n.header.num_children as usize {
                let child = TaggedPtr::from_raw(n.children[i].load(Ordering::Acquire));
                if !child.is_null() {
                    if let Some(leaf) = first_leaf_in_subtree(child) {
                        return Some(leaf);
                    }
                }
            }
            None
        }
        NodeType::Node48 => {
            let n = &*(ptr.as_inner_ptr() as *const Node48);
            let mut next_byte = 0u8;
            while let Some(byte) = next_present_byte(&n.child_bitmap, next_byte) {
                let slot = n.child_indices[byte as usize];
                if slot != NODE48_EMPTY {
                    let child =
                        TaggedPtr::from_raw(n.children[slot as usize].load(Ordering::Acquire));
                    if !child.is_null() {
                        if let Some(leaf) = first_leaf_in_subtree(child) {
                            return Some(leaf);
                        }
                    }
                }
                if byte == 255 {
                    break;
                }
                next_byte = byte + 1;
            }
            None
        }
        NodeType::Node256 => {
            let n = &*(ptr.as_inner_ptr() as *const Node256);
            let mut next_byte = 0u8;
            while let Some(byte) = next_present_byte(&n.child_bitmap, next_byte) {
                let child = TaggedPtr::from_raw(n.children[byte as usize].load(Ordering::Acquire));
                if !child.is_null() {
                    if let Some(leaf) = first_leaf_in_subtree(child) {
                        return Some(leaf);
                    }
                }
                if byte == 255 {
                    break;
                }
                next_byte = byte + 1;
            }
            None
        }
    }
}

pub(crate) unsafe fn last_leaf_in_subtree<K, V>(
    ptr: TaggedPtr,
) -> Option<*mut VersionedLeaf<K, V>> {
    if ptr.is_null() {
        return None;
    }
    if ptr.is_leaf() {
        return Some(ptr.as_versioned_leaf_ptr());
    }
    let header = &*ptr.as_inner_ptr();

    match header.node_type {
        NodeType::Node4 => {
            let n = &*(ptr.as_inner_ptr() as *const Node4);
            for i in (0..n.header.num_children as usize).rev() {
                let child = TaggedPtr::from_raw(n.children[i].load(Ordering::Acquire));
                if !child.is_null() {
                    if let Some(leaf) = last_leaf_in_subtree(child) {
                        return Some(leaf);
                    }
                }
            }
            header.load_exact_versioned_leaf::<K, V>(Ordering::Acquire)
        }
        NodeType::Node16 => {
            let n = &*(ptr.as_inner_ptr() as *const Node16);
            for i in (0..n.header.num_children as usize).rev() {
                let child = TaggedPtr::from_raw(n.children[i].load(Ordering::Acquire));
                if !child.is_null() {
                    if let Some(leaf) = last_leaf_in_subtree(child) {
                        return Some(leaf);
                    }
                }
            }
            header.load_exact_versioned_leaf::<K, V>(Ordering::Acquire)
        }
        NodeType::Node48 => {
            let n = &*(ptr.as_inner_ptr() as *const Node48);
            let mut max_b = 255u8;
            while let Some(byte) = prev_present_byte(&n.child_bitmap, max_b) {
                let slot = n.child_indices[byte as usize];
                if slot != NODE48_EMPTY {
                    let child =
                        TaggedPtr::from_raw(n.children[slot as usize].load(Ordering::Acquire));
                    if !child.is_null() {
                        if let Some(leaf) = last_leaf_in_subtree(child) {
                            return Some(leaf);
                        }
                    }
                }
                if byte == 0 {
                    break;
                }
                max_b = byte - 1;
            }
            header.load_exact_versioned_leaf::<K, V>(Ordering::Acquire)
        }
        NodeType::Node256 => {
            let n = &*(ptr.as_inner_ptr() as *const Node256);
            let mut max_b = 255u8;
            while let Some(byte) = prev_present_byte(&n.child_bitmap, max_b) {
                let child = TaggedPtr::from_raw(n.children[byte as usize].load(Ordering::Acquire));
                if !child.is_null() {
                    if let Some(leaf) = last_leaf_in_subtree(child) {
                        return Some(leaf);
                    }
                }
                if byte == 0 {
                    break;
                }
                max_b = byte - 1;
            }
            header.load_exact_versioned_leaf::<K, V>(Ordering::Acquire)
        }
    }
}
