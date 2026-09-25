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

use crate::key::AsBytes;
use crate::node::{Leaf, TaggedPtr};
use crate::tree::Tree;

/// An iterator over a range of entries in an [`ArtMap`](crate::ArtMap).
pub struct Range<'a, K: AsBytes + Send + 'static, V: Send + 'static> {
    tree: &'a Tree<K, V>,
    _guard: Guard,
    start_bound: Bound<Vec<u8>>,
    end_bound: Bound<Vec<u8>>,
    cursor_front: Vec<u8>,
    cursor_back: Vec<u8>,
    has_front: bool,
    has_back: bool,
    exhausted: bool,
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> Range<'a, K, V> {
    pub(crate) fn new(
        tree: &'a Tree<K, V>,
        guard: Guard,
        start_bound: Bound<Vec<u8>>,
        end_bound: Bound<Vec<u8>>,
    ) -> Self {
        Self {
            tree,
            _guard: guard,
            start_bound,
            end_bound,
            cursor_front: Vec::with_capacity(32),
            cursor_back: Vec::with_capacity(32),
            has_front: false,
            has_back: false,
            exhausted: false,
        }
    }
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> Iterator for Range<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }

        let (search_key, include_equal) = if self.has_front {
            (self.cursor_front.as_slice(), false)
        } else {
            match &self.start_bound {
                Bound::Included(k) => (k.as_slice(), true),
                Bound::Excluded(k) => (k.as_slice(), false),
                Bound::Unbounded => (&[][..], true),
            }
        };

        let leaf_ptr = self.tree.find_successor(search_key, include_equal)?;
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
        if self.has_back && k_bytes > self.cursor_back.as_slice() {
            self.exhausted = true;
            return None;
        }

        self.cursor_front.clear();
        self.cursor_front.extend_from_slice(k_bytes);
        self.has_front = true;
        Some((&leaf.key, &leaf.value))
    }
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> DoubleEndedIterator for Range<'a, K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }

        let (search_key, include_equal) = if self.has_back {
            (self.cursor_back.as_slice(), false)
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
        if self.has_front && k_bytes < self.cursor_front.as_slice() {
            self.exhausted = true;
            return None;
        }

        self.cursor_back.clear();
        self.cursor_back.extend_from_slice(k_bytes);
        self.has_back = true;
        Some((&leaf.key, &leaf.value))
    }
}

/// An iterator over all entries in an [`ArtMap`](crate::ArtMap).
pub struct Iter<'a, K: AsBytes + Send + 'static, V: Send + 'static> {
    inner: Range<'a, K, V>,
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> Iter<'a, K, V> {
    pub(crate) fn new(tree: &'a Tree<K, V>, guard: Guard) -> Self {
        Self {
            inner: Range::new(tree, guard, Bound::Unbounded, Bound::Unbounded),
        }
    }
}

impl<'a, K: AsBytes + Send + 'static, V: Send + 'static> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

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
        self.inner.next().map(|(k, _)| k)
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
        self.inner.next().map(|(_, v)| v)
    }
}

pub(crate) unsafe fn first_leaf_in_subtree<K, V>(ptr: TaggedPtr) -> Option<*mut Leaf<K, V>> {
    if ptr.is_leaf() {
        return Some(ptr.as_leaf_ptr());
    }
    let header = &*ptr.as_inner_ptr();
    if let Some(leaf) = header.load_exact_leaf::<K, V>(std::sync::atomic::Ordering::Acquire) {
        return Some(leaf);
    }

    match header.node_type {
        crate::node::NodeType::Node4 => {
            let n = &*(ptr.as_inner_ptr() as *const crate::node::Node4);
            for i in 0..n.header.num_children as usize {
                let child =
                    TaggedPtr::from_raw(n.children[i].load(std::sync::atomic::Ordering::Acquire));
                if let Some(leaf) = first_leaf_in_subtree(child) {
                    return Some(leaf);
                }
            }
            None
        }
        crate::node::NodeType::Node16 => {
            let n = &*(ptr.as_inner_ptr() as *const crate::node::Node16);
            for i in 0..n.header.num_children as usize {
                let child =
                    TaggedPtr::from_raw(n.children[i].load(std::sync::atomic::Ordering::Acquire));
                if let Some(leaf) = first_leaf_in_subtree(child) {
                    return Some(leaf);
                }
            }
            None
        }
        crate::node::NodeType::Node48 => {
            let n = &*(ptr.as_inner_ptr() as *const crate::node::Node48);
            for byte in 0..=255u8 {
                let slot = n.child_indices[byte as usize];
                if slot != crate::node::NODE48_EMPTY {
                    let child = TaggedPtr::from_raw(
                        n.children[slot as usize].load(std::sync::atomic::Ordering::Acquire),
                    );
                    if let Some(leaf) = first_leaf_in_subtree(child) {
                        return Some(leaf);
                    }
                }
            }
            None
        }
        crate::node::NodeType::Node256 => {
            let n = &*(ptr.as_inner_ptr() as *const crate::node::Node256);
            for byte in 0..=255u8 {
                let child = TaggedPtr::from_raw(
                    n.children[byte as usize].load(std::sync::atomic::Ordering::Acquire),
                );
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
    if ptr.is_leaf() {
        return Some(ptr.as_leaf_ptr());
    }
    let header = &*ptr.as_inner_ptr();

    match header.node_type {
        crate::node::NodeType::Node4 => {
            let n = &*(ptr.as_inner_ptr() as *const crate::node::Node4);
            for i in (0..n.header.num_children as usize).rev() {
                let child =
                    TaggedPtr::from_raw(n.children[i].load(std::sync::atomic::Ordering::Acquire));
                if let Some(leaf) = last_leaf_in_subtree(child) {
                    return Some(leaf);
                }
            }
            header.load_exact_leaf::<K, V>(std::sync::atomic::Ordering::Acquire)
        }
        crate::node::NodeType::Node16 => {
            let n = &*(ptr.as_inner_ptr() as *const crate::node::Node16);
            for i in (0..n.header.num_children as usize).rev() {
                let child =
                    TaggedPtr::from_raw(n.children[i].load(std::sync::atomic::Ordering::Acquire));
                if let Some(leaf) = last_leaf_in_subtree(child) {
                    return Some(leaf);
                }
            }
            header.load_exact_leaf::<K, V>(std::sync::atomic::Ordering::Acquire)
        }
        crate::node::NodeType::Node48 => {
            let n = &*(ptr.as_inner_ptr() as *const crate::node::Node48);
            for byte in (0..=255u8).rev() {
                let slot = n.child_indices[byte as usize];
                if slot != crate::node::NODE48_EMPTY {
                    let child = TaggedPtr::from_raw(
                        n.children[slot as usize].load(std::sync::atomic::Ordering::Acquire),
                    );
                    if let Some(leaf) = last_leaf_in_subtree(child) {
                        return Some(leaf);
                    }
                }
            }
            header.load_exact_leaf::<K, V>(std::sync::atomic::Ordering::Acquire)
        }
        crate::node::NodeType::Node256 => {
            let n = &*(ptr.as_inner_ptr() as *const crate::node::Node256);
            for byte in (0..=255u8).rev() {
                let child = TaggedPtr::from_raw(
                    n.children[byte as usize].load(std::sync::atomic::Ordering::Acquire),
                );
                if !child.is_null() {
                    if let Some(leaf) = last_leaf_in_subtree(child) {
                        return Some(leaf);
                    }
                }
            }
            header.load_exact_leaf::<K, V>(std::sync::atomic::Ordering::Acquire)
        }
    }
}
