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
use crate::node::{Leaf, Node16, Node256, Node4, Node48, NodeType, TaggedPtr, NODE48_EMPTY};
use crate::tree::Tree;

const INLINE_KEY_BUF: usize = 64;

/// An iterator over a range of entries in an [`ArtMap`](crate::ArtMap).
pub struct Range<'a, K: AsBytes + Send + 'static, V: Send + 'static> {
    tree: &'a Tree<K, V>,
    _guard: Guard,
    start_bound: Bound<Vec<u8>>,
    end_bound: Bound<Vec<u8>>,
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

                match self.tree.find_successor(search_key, include_equal) {
                    Some(p) => p,
                    None => {
                        self.exhausted = true;
                        return None;
                    }
                }
            } else {
                match self.tree.find_successor(self.cursor_front(), false) {
                    Some(p) => p,
                    None => {
                        self.exhausted = true;
                        return None;
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
            if self.has_back && k_bytes >= self.cursor_back() {
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
            if self.has_front && k_bytes <= self.cursor_front() {
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
