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

//! # Range Iterators
//!
//! Provides bidirectional range scanning for [`ArtMap`](crate::ArtMap).

use crossbeam_epoch::Guard;
use std::ops::Bound;
use std::sync::atomic::Ordering;

use crate::entry::EntryRef;
use crate::key::AsBytes;
use crate::node::{
    Leaf, Node16, Node256, Node4, Node48, NodeHeader, NodeType, TaggedPtr, NODE48_EMPTY,
};
use crate::tree::Tree;

const MAX_STACK_DEPTH: usize = 16;
const INLINE_KEY_BUF: usize = 64;

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

/// An iterator over a range of entries in an [`ArtMap`](crate::ArtMap).
pub struct Range<'a, K: AsBytes + Send + 'static, V: Send + 'static> {
    tree: &'a Tree<K, V>,
    _guard: Guard,
    start_bound: Bound<Vec<u8>>,
    end_bound: Bound<Vec<u8>>,
    stack: [CursorFrame; MAX_STACK_DEPTH],
    stack_len: usize,
    stack_overflow: Vec<CursorFrame>,
    cursor_front_buf: [u8; INLINE_KEY_BUF],
    cursor_front_len: usize,
    cursor_front_overflow: Vec<u8>,
    cursor_back_buf: [u8; INLINE_KEY_BUF],
    cursor_back_len: usize,
    cursor_back_overflow: Vec<u8>,
    has_front: bool,
    has_back: bool,
    exhausted: bool,
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> Range<'a, K, V> {
    pub(crate) fn new(
        tree: &'a Tree<K, V>,
        start_bound: Bound<Vec<u8>>,
        end_bound: Bound<Vec<u8>>,
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
            cursor_front_buf: [0u8; INLINE_KEY_BUF],
            cursor_front_len: 0,
            cursor_front_overflow: Vec::new(),
            cursor_back_buf: [0u8; INLINE_KEY_BUF],
            cursor_back_len: 0,
            cursor_back_overflow: Vec::new(),
            has_front: false,
            has_back: false,
            exhausted: false,
        }
    }

    #[inline]
    fn stack_push(&mut self, frame: CursorFrame) {
        if self.stack_len < MAX_STACK_DEPTH && self.stack_overflow.is_empty() {
            self.stack[self.stack_len] = frame;
            self.stack_len += 1;
        } else {
            self.stack_overflow.push(frame);
        }
    }

    #[inline]
    fn stack_last_mut(&mut self) -> Option<&mut CursorFrame> {
        if let Some(f) = self.stack_overflow.last_mut() {
            Some(f)
        } else if self.stack_len > 0 {
            Some(&mut self.stack[self.stack_len - 1])
        } else {
            None
        }
    }

    #[inline]
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

    #[inline]
    fn stack_clear(&mut self) {
        self.stack_len = 0;
        self.stack_overflow.clear();
    }

    #[inline]
    fn set_cursor_front(&mut self, bytes: &[u8]) {
        self.has_front = true;
        if bytes.len() <= INLINE_KEY_BUF {
            self.cursor_front_buf[..bytes.len()].copy_from_slice(bytes);
            self.cursor_front_len = bytes.len();
            self.cursor_front_overflow.clear();
        } else {
            self.cursor_front_len = 0;
            self.cursor_front_overflow.clear();
            self.cursor_front_overflow.extend_from_slice(bytes);
        }
    }

    #[inline]
    fn cursor_front(&self) -> &[u8] {
        if self.cursor_front_len > 0 {
            &self.cursor_front_buf[..self.cursor_front_len]
        } else {
            &self.cursor_front_overflow
        }
    }

    #[inline]
    fn set_cursor_back(&mut self, bytes: &[u8]) {
        self.has_back = true;
        if bytes.len() <= INLINE_KEY_BUF {
            self.cursor_back_buf[..bytes.len()].copy_from_slice(bytes);
            self.cursor_back_len = bytes.len();
            self.cursor_back_overflow.clear();
        } else {
            self.cursor_back_len = 0;
            self.cursor_back_overflow.clear();
            self.cursor_back_overflow.extend_from_slice(bytes);
        }
    }

    #[inline]
    fn cursor_back(&self) -> &[u8] {
        if self.cursor_back_len > 0 {
            &self.cursor_back_buf[..self.cursor_back_len]
        } else {
            &self.cursor_back_overflow
        }
    }

    fn push_and_descend_left(&mut self, mut ptr: TaggedPtr) -> Option<*mut Leaf<K, V>> {
        while !ptr.is_null() {
            if ptr.is_leaf() {
                return Some(ptr.as_leaf_ptr());
            }
            let header = ptr.as_inner_ptr();
            let exact = unsafe { (*header).load_exact_leaf::<K, V>(Ordering::Acquire) };
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

    fn advance_forward(&mut self) -> Option<*mut Leaf<K, V>> {
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

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> Iterator for Range<'a, K, V> {
    type Item = EntryRef<'a, K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }

        loop {
            let leaf_ptr = if !self.has_front {
                let (search_key, include_equal) = match &self.start_bound {
                    Bound::Included(k) => (k.as_slice(), true),
                    Bound::Excluded(k) => (k.as_slice(), false),
                    Bound::Unbounded => (&[][..], true),
                };

                let root_ptr = TaggedPtr::from_raw(self.tree.raw_root().as_raw());
                if root_ptr.is_null() {
                    self.exhausted = true;
                    return None;
                }

                if root_ptr.is_leaf() {
                    let leaf = unsafe { &*root_ptr.as_leaf_ptr::<K, V>() };
                    let k = leaf.key.as_bytes();
                    let cmp = k.cmp(search_key);
                    if (include_equal && cmp >= std::cmp::Ordering::Equal)
                        || (!include_equal && cmp == std::cmp::Ordering::Greater)
                    {
                        let ptr = root_ptr.as_leaf_ptr::<K, V>();
                        self.set_cursor_front(k);
                        let entry = EntryRef {
                            leaf_ptr: ptr,
                            tree: self.tree,
                        };
                        if !entry.is_removed() {
                            return Some(entry);
                        } else {
                            self.exhausted = true;
                            return None;
                        }
                    } else {
                        self.exhausted = true;
                        return None;
                    }
                }

                if (search_key.is_empty() || (search_key == [0u8] && include_equal))
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
                        let ptr = self.tree.find_successor(self.cursor_front(), false)?;
                        let leaf = unsafe { &*ptr };
                        self.seek_to_key(leaf.key.as_bytes());
                        ptr
                    }
                }
            };

            let leaf = unsafe { &*leaf_ptr };
            let k_bytes = leaf.key.as_bytes();

            // Check upper range bound
            match &self.end_bound {
                Bound::Included(end) if k_bytes > end.as_slice() => {
                    self.exhausted = true;
                    return None;
                }
                Bound::Excluded(end) if k_bytes >= end.as_slice() => {
                    self.exhausted = true;
                    return None;
                }
                _ => {}
            }

            // Check overlap with backward cursor
            if self.has_back && k_bytes > self.cursor_back() {
                self.exhausted = true;
                return None;
            }

            self.set_cursor_front(k_bytes);

            let entry = EntryRef {
                leaf_ptr,
                tree: self.tree,
            };
            if !entry.is_removed() {
                return Some(entry);
            }
        }
    }
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> DoubleEndedIterator for Range<'a, K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }

        loop {
            let (search_key, include_equal) = if self.has_back {
                (self.cursor_back(), false)
            } else {
                match &self.end_bound {
                    Bound::Included(k) => (k.as_slice(), true),
                    Bound::Excluded(k) => (k.as_slice(), false),
                    Bound::Unbounded => (&[0xFF; 64][..], true),
                }
            };

            let leaf_ptr = self.tree.find_predecessor(search_key, include_equal)?;
            let leaf = unsafe { &*leaf_ptr };
            let k_bytes = leaf.key.as_bytes();

            // Check lower range bound
            match &self.start_bound {
                Bound::Included(start) if k_bytes < start.as_slice() => {
                    self.exhausted = true;
                    return None;
                }
                Bound::Excluded(start) if k_bytes <= start.as_slice() => {
                    self.exhausted = true;
                    return None;
                }
                _ => {}
            }

            // Check overlap with forward cursor
            if self.has_front && k_bytes < self.cursor_front() {
                self.exhausted = true;
                return None;
            }

            self.set_cursor_back(k_bytes);

            let entry = EntryRef {
                leaf_ptr,
                tree: self.tree,
            };
            if !entry.is_removed() {
                return Some(entry);
            }
        }
    }
}

/// An iterator over all entries in an [`ArtMap`](crate::ArtMap).
pub struct Iter<'a, K: AsBytes + Send + 'static, V: Send + 'static> {
    inner: Range<'a, K, V>,
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> Iter<'a, K, V> {
    pub(crate) fn new(tree: &'a Tree<K, V>) -> Self {
        Self {
            inner: Range::new(tree, Bound::Unbounded, Bound::Unbounded),
        }
    }
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> Iterator for Iter<'a, K, V> {
    type Item = EntryRef<'a, K, V>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> DoubleEndedIterator for Iter<'a, K, V> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back()
    }
}

/// An iterator over keys in an [`ArtMap`](crate::ArtMap).
pub struct Keys<'a, K: AsBytes + Send + 'static, V: Send + 'static> {
    inner: Iter<'a, K, V>,
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> Keys<'a, K, V> {
    pub(crate) fn new(inner: Iter<'a, K, V>) -> Self {
        Self { inner }
    }
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> Iterator for Keys<'a, K, V> {
    type Item = &'a K;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|e| e.key())
    }
}

/// An iterator over values in an [`ArtMap`](crate::ArtMap).
pub struct Values<'a, K: AsBytes + Send + 'static, V: Send + 'static> {
    inner: Iter<'a, K, V>,
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> Values<'a, K, V> {
    pub(crate) fn new(inner: Iter<'a, K, V>) -> Self {
        Self { inner }
    }
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> Iterator for Values<'a, K, V> {
    type Item = &'a V;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|e| e.value())
    }
}

unsafe fn next_child_in_node(
    header: *mut NodeHeader,
    current_pos: usize,
) -> Option<(usize, TaggedPtr)> {
    let h = &*header;
    match h.node_type {
        NodeType::Node4 => {
            let n = &*(header as *const Node4);
            let count = n.header.num_children as usize;
            let next_idx = if current_pos == usize::MAX {
                0
            } else {
                current_pos + 1
            };
            if next_idx < count {
                let child = TaggedPtr::from_raw(n.children[next_idx].load(Ordering::Acquire));
                if !child.is_null() {
                    return Some((next_idx, child));
                }
            }
            None
        }
        NodeType::Node16 => {
            let n = &*(header as *const Node16);
            let count = n.header.num_children as usize;
            let next_idx = if current_pos == usize::MAX {
                0
            } else {
                current_pos + 1
            };
            if next_idx < count {
                let child = TaggedPtr::from_raw(n.children[next_idx].load(Ordering::Acquire));
                if !child.is_null() {
                    return Some((next_idx, child));
                }
            }
            None
        }
        NodeType::Node48 => {
            let n = &*(header as *const Node48);
            let next_byte = if current_pos == usize::MAX {
                0
            } else {
                current_pos + 1
            };
            for byte in next_byte..=255 {
                let slot = n.child_indices[byte];
                if slot != NODE48_EMPTY {
                    let child =
                        TaggedPtr::from_raw(n.children[slot as usize].load(Ordering::Acquire));
                    if !child.is_null() {
                        return Some((byte, child));
                    }
                }
            }
            None
        }
        NodeType::Node256 => {
            let n = &*(header as *const Node256);
            let next_byte = if current_pos == usize::MAX {
                0
            } else {
                current_pos + 1
            };
            for byte in next_byte..=255 {
                let child = TaggedPtr::from_raw(n.children[byte].load(Ordering::Acquire));
                if !child.is_null() {
                    return Some((byte, child));
                }
            }
            None
        }
    }
}

unsafe fn child_pos_for_byte(header: *mut NodeHeader, needle: u8) -> Option<(usize, TaggedPtr)> {
    let h = &*header;
    match h.node_type {
        NodeType::Node4 => {
            let n = &*(header as *const Node4);
            let count = n.header.num_children as usize;
            for i in 0..count {
                if n.keys[i] == needle {
                    let child = TaggedPtr::from_raw(n.children[i].load(Ordering::Acquire));
                    if !child.is_null() {
                        return Some((i, child));
                    }
                }
            }
            None
        }
        NodeType::Node16 => {
            let n = &*(header as *const Node16);
            let count = n.header.num_children as usize;
            for i in 0..count {
                if n.keys[i] == needle {
                    let child = TaggedPtr::from_raw(n.children[i].load(Ordering::Acquire));
                    if !child.is_null() {
                        return Some((i, child));
                    }
                }
            }
            None
        }
        NodeType::Node48 => {
            let n = &*(header as *const Node48);
            let slot = n.child_indices[needle as usize];
            if slot != NODE48_EMPTY {
                let child = TaggedPtr::from_raw(n.children[slot as usize].load(Ordering::Acquire));
                if !child.is_null() {
                    return Some((needle as usize, child));
                }
            }
            None
        }
        NodeType::Node256 => {
            let n = &*(header as *const Node256);
            let child = TaggedPtr::from_raw(n.children[needle as usize].load(Ordering::Acquire));
            if !child.is_null() {
                Some((needle as usize, child))
            } else {
                None
            }
        }
    }
}

pub(crate) unsafe fn first_leaf_in_subtree<K, V>(ptr: TaggedPtr) -> Option<*mut Leaf<K, V>> {
    if ptr.is_null() {
        return None;
    }
    if ptr.is_leaf() {
        return Some(ptr.as_leaf_ptr());
    }
    let header = &*ptr.as_inner_ptr();
    if let Some(leaf) = header.load_exact_leaf::<K, V>(Ordering::Acquire) {
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
            for byte in 0..=255u8 {
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
            }
            None
        }
        NodeType::Node256 => {
            let n = &*(ptr.as_inner_ptr() as *const Node256);
            for byte in 0..=255u8 {
                let child = TaggedPtr::from_raw(n.children[byte as usize].load(Ordering::Acquire));
                if !child.is_null() {
                    if let Some(leaf) = first_leaf_in_subtree(child) {
                        return Some(leaf);
                    }
                }
            }
            None
        }
    }
}

pub(crate) unsafe fn last_leaf_in_subtree<K, V>(ptr: TaggedPtr) -> Option<*mut Leaf<K, V>> {
    if ptr.is_null() {
        return None;
    }
    if ptr.is_leaf() {
        return Some(ptr.as_leaf_ptr());
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
            header.load_exact_leaf::<K, V>(Ordering::Acquire)
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
            header.load_exact_leaf::<K, V>(Ordering::Acquire)
        }
        NodeType::Node48 => {
            let n = &*(ptr.as_inner_ptr() as *const Node48);
            for byte in (0..=255u8).rev() {
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
            }
            header.load_exact_leaf::<K, V>(Ordering::Acquire)
        }
        NodeType::Node256 => {
            let n = &*(ptr.as_inner_ptr() as *const Node256);
            for byte in (0..=255u8).rev() {
                let child = TaggedPtr::from_raw(n.children[byte as usize].load(Ordering::Acquire));
                if !child.is_null() {
                    if let Some(leaf) = last_leaf_in_subtree(child) {
                        return Some(leaf);
                    }
                }
            }
            header.load_exact_leaf::<K, V>(Ordering::Acquire)
        }
    }
}
