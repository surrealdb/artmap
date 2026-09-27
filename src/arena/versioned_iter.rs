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

//! # Versioned Range Iterators for ArenaVersionedArtMap
//!
//! Provides zero-allocation bidirectional range scanning for [`ArenaVersionedArtMap`](crate::arena::ArenaVersionedArtMap).

use std::marker::PhantomData;
use std::ops::{Bound, Deref};
use std::sync::atomic::Ordering;

use crate::arena::node::{
    next_present_byte, prev_present_byte, ArenaVersionNode, Node16, Node256, Node4, Node48,
    NodeHeader, TaggedOffset, VersionedLeaf,
};
use crate::arena::versioned_tree::ArenaVersionedTree;
use crate::key::AsBytes;
use crate::node::{NodeType, NODE48_EMPTY};
use crate::simd::find_child_node16;

const MAX_STACK_DEPTH: usize = 16;

#[derive(Clone, Copy)]
struct CursorFrame {
    node_offset: u32,
    current_pos: usize,
}

impl CursorFrame {
    const NULL: Self = Self {
        node_offset: 0,
        current_pos: 0,
    };
}

/// An ergonomic reference to a versioned entry in an [`ArenaVersionedArtMap`](crate::arena::ArenaVersionedArtMap).
#[derive(Clone, Copy)]
pub struct ArenaVersionedEntryRef<'a, K: AsBytes + Clone, V: Clone> {
    pub(crate) leaf_ptr: *const VersionedLeaf<K, V>,
    pub(crate) arena: &'a crate::arena::Arena,
    pub(crate) _marker: PhantomData<&'a ()>,
}

impl<'a, K: AsBytes + Clone, V: Clone> ArenaVersionedEntryRef<'a, K, V> {
    /// Returns a reference to the entry's key.
    #[inline]
    pub fn key(&self) -> &'a K {
        unsafe { &(*self.leaf_ptr).key }
    }

    /// Returns a reference to the entry's value.
    #[inline]
    pub fn value(&self) -> &'a V {
        unsafe {
            let head_off = (*self.leaf_ptr).versions_offset.load(Ordering::Acquire) & !1;
            let node = self.arena.get_pointer(head_off) as *const ArenaVersionNode<V>;
            &(*node).value
        }
    }

    /// Returns the entry's monotonic MVCC version number.
    #[inline]
    pub fn version(&self) -> u64 {
        unsafe {
            let head_off = (*self.leaf_ptr).versions_offset.load(Ordering::Acquire) & !1;
            let node = self.arena.get_pointer(head_off) as *const ArenaVersionNode<V>;
            (*node).version
        }
    }

    /// Checks if this entry has been removed from the map.
    #[inline]
    pub fn is_removed(&self) -> bool {
        unsafe {
            let head_off = (*self.leaf_ptr).versions_offset.load(Ordering::Acquire) & !1;
            if head_off == 0 {
                true
            } else {
                let node = self.arena.get_pointer(head_off) as *const ArenaVersionNode<V>;
                (*node).removed.load(Ordering::Acquire)
            }
        }
    }

    /// Returns the raw pointer to the underlying [`VersionedLeaf`].
    #[inline]
    pub fn leaf_ptr(&self) -> *const VersionedLeaf<K, V> {
        self.leaf_ptr
    }

    /// Returns an iterator over all versions of this key in descending version order.
    pub fn versions(&self, arena: &'a crate::arena::Arena) -> impl Iterator<Item = (u64, &'a V)>
    where
        V: 'a,
    {
        let head_off = unsafe { (*self.leaf_ptr).versions_offset.load(Ordering::Acquire) & !1 };
        let mut cur_off = head_off;
        std::iter::from_fn(move || {
            if cur_off == 0 {
                None
            } else {
                let node = unsafe { &*(arena.get_pointer(cur_off) as *const ArenaVersionNode<V>) };
                let item = (node.version, &node.value);
                cur_off = node.next_version_offset.load(Ordering::Acquire) & !1;
                Some(item)
            }
        })
    }
}

impl<'a, K: AsBytes + Clone + std::fmt::Debug, V: Clone + std::fmt::Debug> std::fmt::Debug
    for ArenaVersionedEntryRef<'a, K, V>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArenaVersionedEntryRef")
            .field("key", self.key())
            .field("version", &self.version())
            .field("value", self.value())
            .field("is_removed", &self.is_removed())
            .finish()
    }
}

impl<'a, K: AsBytes + Clone + PartialEq, V: Clone + PartialEq> PartialEq
    for ArenaVersionedEntryRef<'a, K, V>
{
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
            && self.value() == other.value()
            && self.version() == other.version()
    }
}

impl<'a, K: AsBytes + Clone + Eq, V: Clone + Eq> Eq for ArenaVersionedEntryRef<'a, K, V> {}

impl<'a, K: AsBytes + Clone, V: Clone> Deref for ArenaVersionedEntryRef<'a, K, V> {
    type Target = V;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.value()
    }
}

/// An iterator over a range of entries in an [`ArenaVersionedArtMap`](crate::arena::ArenaVersionedArtMap).
pub struct ArenaVersionedRange<'a, K: AsBytes + Clone, V: Clone> {
    tree: &'a ArenaVersionedTree<K, V>,
    start_bound: Bound<Vec<u8>>,
    end_bound: Bound<Vec<u8>>,
    stack: [CursorFrame; MAX_STACK_DEPTH],
    stack_len: usize,
    stack_overflow: Vec<CursorFrame>,
    last_leaf_front: Option<*const VersionedLeaf<K, V>>,
    last_leaf_back: Option<*const VersionedLeaf<K, V>>,
    exhausted: bool,
}

impl<'a, K: AsBytes + Clone, V: Clone> ArenaVersionedRange<'a, K, V> {
    pub(crate) fn new(
        tree: &'a ArenaVersionedTree<K, V>,
        start_bound: Bound<Vec<u8>>,
        end_bound: Bound<Vec<u8>>,
    ) -> Self {
        Self {
            tree,
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

    #[inline(always)]
    fn cursor_front(&self) -> Option<&[u8]> {
        self.last_leaf_front
            .map(|ptr| unsafe { (*ptr).key.as_bytes() })
    }

    #[inline(always)]
    fn cursor_back(&self) -> Option<&[u8]> {
        self.last_leaf_back
            .map(|ptr| unsafe { (*ptr).key.as_bytes() })
    }

    #[inline(always)]
    fn set_cursor_front(&mut self, ptr: *const VersionedLeaf<K, V>) {
        self.last_leaf_front = Some(ptr);
    }

    #[inline(always)]
    fn set_cursor_back(&mut self, ptr: *const VersionedLeaf<K, V>) {
        self.last_leaf_back = Some(ptr);
    }

    fn push_and_descend_left(
        &mut self,
        mut offset: TaggedOffset,
    ) -> Option<*const VersionedLeaf<K, V>> {
        while !offset.is_null() {
            if offset.is_leaf() {
                return Some(
                    self.tree.arena.get_pointer(offset.leaf_offset()) as *const VersionedLeaf<K, V>
                );
            }
            let node_offset = offset.inner_offset();
            let header_ptr = self.tree.arena.get_pointer(node_offset) as *const NodeHeader;
            let exact = unsafe { (*header_ptr).exact_leaf.load(Ordering::Acquire) };
            if exact != 0 {
                self.stack_push(CursorFrame {
                    node_offset,
                    current_pos: usize::MAX,
                });
                return Some(
                    self.tree
                        .arena
                        .get_pointer(TaggedOffset(exact).leaf_offset())
                        as *const VersionedLeaf<K, V>,
                );
            }

            match unsafe { next_child_in_node(self.tree, node_offset, usize::MAX) } {
                Some((pos, child)) => {
                    self.stack_push(CursorFrame {
                        node_offset,
                        current_pos: pos,
                    });
                    offset = child;
                }
                None => return None,
            }
        }
        None
    }

    fn seek_to_key(&mut self, target_key: &[u8]) {
        self.stack_clear();
        let root_offset = self.tree.raw_root();
        if root_offset.is_null() || root_offset.is_leaf() {
            return;
        }

        let mut current = root_offset;
        let mut depth = 0;

        while !current.is_null() && !current.is_leaf() {
            let node_offset = current.inner_offset();
            let header_ptr = self.tree.arena.get_pointer(node_offset) as *const NodeHeader;
            let (_matched, is_full) = unsafe { (*header_ptr).match_prefix(target_key, depth) };
            if !is_full {
                return;
            }

            depth += unsafe { (*header_ptr).prefix_len as usize };

            if depth == target_key.len() {
                self.stack_push(CursorFrame {
                    node_offset,
                    current_pos: usize::MAX,
                });
                return;
            }

            let next_byte = target_key[depth];
            match unsafe { child_pos_for_byte(self.tree, node_offset, next_byte) } {
                Some((pos, child)) => {
                    self.stack_push(CursorFrame {
                        node_offset,
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

    fn advance_forward(&mut self) -> Option<*const VersionedLeaf<K, V>> {
        while let Some(frame) = self.stack_last_mut() {
            let node_offset = frame.node_offset;
            let current_pos = frame.current_pos;
            match unsafe { next_child_in_node(self.tree, node_offset, current_pos) } {
                Some((next_pos, child)) => {
                    self.stack_last_mut().unwrap().current_pos = next_pos;
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

impl<'a, K: AsBytes + Clone, V: Clone> Iterator for ArenaVersionedRange<'a, K, V> {
    type Item = ArenaVersionedEntryRef<'a, K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }

        loop {
            let leaf_ptr = if self.last_leaf_front.is_none() {
                let (search_key, include_equal) = match &self.start_bound {
                    Bound::Included(k) => (k.as_slice(), true),
                    Bound::Excluded(k) => (k.as_slice(), false),
                    Bound::Unbounded => (&[][..], true),
                };

                let root_offset = self.tree.raw_root();
                if root_offset.is_null() {
                    self.exhausted = true;
                    return None;
                }

                if root_offset.is_leaf() {
                    let leaf = unsafe {
                        &*(self.tree.arena.get_pointer(root_offset.leaf_offset())
                            as *const VersionedLeaf<K, V>)
                    };
                    let k = leaf.key.as_bytes();
                    let cmp = k.cmp(search_key);
                    if (include_equal && cmp >= std::cmp::Ordering::Equal)
                        || (!include_equal && cmp == std::cmp::Ordering::Greater)
                    {
                        leaf as *const VersionedLeaf<K, V>
                    } else {
                        self.exhausted = true;
                        return None;
                    }
                } else if (search_key.is_empty() || (search_key == [0u8] && include_equal))
                    && include_equal
                {
                    match self.push_and_descend_left(root_offset) {
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
                        let ptr = self
                            .tree
                            .find_successor(self.cursor_front().unwrap(), false)?;
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
            if let Some(back_key) = self.cursor_back() {
                if k_bytes > back_key {
                    self.exhausted = true;
                    return None;
                }
            }

            self.set_cursor_front(leaf_ptr);

            let entry = ArenaVersionedEntryRef {
                leaf_ptr,
                arena: &self.tree.arena,
                _marker: PhantomData,
            };
            if !entry.is_removed() {
                return Some(entry);
            }
        }
    }
}

impl<'a, K: AsBytes + Clone, V: Clone> DoubleEndedIterator for ArenaVersionedRange<'a, K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }

        loop {
            let (search_key, include_equal) = if let Some(back_key) = self.cursor_back() {
                (back_key, false)
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
            if let Some(front_key) = self.cursor_front() {
                if k_bytes < front_key {
                    self.exhausted = true;
                    return None;
                }
            }

            self.set_cursor_back(leaf_ptr);

            let entry = ArenaVersionedEntryRef {
                leaf_ptr,
                arena: &self.tree.arena,
                _marker: PhantomData,
            };
            if !entry.is_removed() {
                return Some(entry);
            }
        }
    }
}

unsafe fn next_child_in_node<K: AsBytes + Clone, V: Clone>(
    tree: &ArenaVersionedTree<K, V>,
    node_offset: u32,
    current_pos: usize,
) -> Option<(usize, TaggedOffset)> {
    let header_ptr = tree.arena.get_pointer(node_offset) as *const NodeHeader;
    let h = &*header_ptr;
    match h.node_type {
        NodeType::Node4 => {
            let n = &*(header_ptr as *const Node4);
            let count = n.header.num_children() as usize;
            let next_idx = if current_pos == usize::MAX {
                0
            } else {
                current_pos + 1
            };
            if next_idx < count {
                let raw = n.children[next_idx].load(Ordering::Acquire);
                if raw != 0 {
                    return Some((next_idx, TaggedOffset(raw)));
                }
            }
            None
        }
        NodeType::Node16 => {
            let n = &*(header_ptr as *const Node16);
            let count = n.header.num_children() as usize;
            let next_idx = if current_pos == usize::MAX {
                0
            } else {
                current_pos + 1
            };
            if next_idx < count {
                let raw = n.children[next_idx].load(Ordering::Acquire);
                if raw != 0 {
                    return Some((next_idx, TaggedOffset(raw)));
                }
            }
            None
        }
        NodeType::Node48 => {
            let n = &*(header_ptr as *const Node48);
            let mut next_byte = if current_pos == usize::MAX {
                0
            } else if current_pos < 255 {
                (current_pos + 1) as u8
            } else {
                return None;
            };
            while let Some(byte) = next_present_byte(&n.child_bitmap, next_byte) {
                let slot = n.child_indices[byte as usize];
                if slot != NODE48_EMPTY {
                    let raw = n.children[slot as usize].load(Ordering::Acquire);
                    if raw != 0 {
                        return Some((byte as usize, TaggedOffset(raw)));
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
            let n = &*(header_ptr as *const Node256);
            let mut next_byte = if current_pos == usize::MAX {
                0
            } else if current_pos < 255 {
                (current_pos + 1) as u8
            } else {
                return None;
            };
            while let Some(byte) = next_present_byte(&n.child_bitmap, next_byte) {
                let raw = n.children[byte as usize].load(Ordering::Acquire);
                if raw != 0 {
                    return Some((byte as usize, TaggedOffset(raw)));
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

unsafe fn prev_child_in_node<K: AsBytes + Clone, V: Clone>(
    tree: &ArenaVersionedTree<K, V>,
    node_offset: u32,
    current_pos: usize,
) -> Option<(usize, TaggedOffset)> {
    let header_ptr = tree.arena.get_pointer(node_offset) as *const NodeHeader;
    let h = &*header_ptr;
    match h.node_type {
        NodeType::Node4 => {
            let n = &*(header_ptr as *const Node4);
            let count = n.header.num_children() as usize;
            let start = if current_pos == usize::MAX {
                count.saturating_sub(1)
            } else if current_pos > 0 {
                (current_pos - 1).min(count.saturating_sub(1))
            } else {
                return None;
            };
            for i in (0..=start).rev() {
                let raw = n.children[i].load(Ordering::Acquire);
                if raw != 0 {
                    return Some((i, TaggedOffset(raw)));
                }
            }
            None
        }
        NodeType::Node16 => {
            let n = &*(header_ptr as *const Node16);
            let count = n.header.num_children() as usize;
            let start = if current_pos == usize::MAX {
                count.saturating_sub(1)
            } else if current_pos > 0 {
                (current_pos - 1).min(count.saturating_sub(1))
            } else {
                return None;
            };
            for i in (0..=start).rev() {
                let raw = n.children[i].load(Ordering::Acquire);
                if raw != 0 {
                    return Some((i, TaggedOffset(raw)));
                }
            }
            None
        }
        NodeType::Node48 => {
            let n = &*(header_ptr as *const Node48);
            let mut max_byte = if current_pos == usize::MAX {
                255
            } else if current_pos > 0 {
                (current_pos - 1) as u8
            } else {
                return None;
            };
            while let Some(byte) = prev_present_byte(&n.child_bitmap, max_byte) {
                let slot = n.child_indices[byte as usize];
                if slot != NODE48_EMPTY {
                    let raw = n.children[slot as usize].load(Ordering::Acquire);
                    if raw != 0 {
                        return Some((byte as usize, TaggedOffset(raw)));
                    }
                }
                if byte == 0 {
                    break;
                }
                max_byte = byte - 1;
            }
            None
        }
        NodeType::Node256 => {
            let n = &*(header_ptr as *const Node256);
            let mut max_byte = if current_pos == usize::MAX {
                255
            } else if current_pos > 0 {
                (current_pos - 1) as u8
            } else {
                return None;
            };
            while let Some(byte) = prev_present_byte(&n.child_bitmap, max_byte) {
                let raw = n.children[byte as usize].load(Ordering::Acquire);
                if raw != 0 {
                    return Some((byte as usize, TaggedOffset(raw)));
                }
                if byte == 0 {
                    break;
                }
                max_byte = byte - 1;
            }
            None
        }
    }
}

unsafe fn child_pos_for_byte<K: AsBytes + Clone, V: Clone>(
    tree: &ArenaVersionedTree<K, V>,
    node_offset: u32,
    needle: u8,
) -> Option<(usize, TaggedOffset)> {
    let header_ptr = tree.arena.get_pointer(node_offset) as *const NodeHeader;
    let h = &*header_ptr;
    match h.node_type {
        NodeType::Node4 => {
            let n = &*(header_ptr as *const Node4);
            let count = n.header.num_children() as usize;
            for i in 0..count {
                if n.keys[i] == needle {
                    let raw = n.children[i].load(Ordering::Acquire);
                    if raw != 0 {
                        return Some((i, TaggedOffset(raw)));
                    }
                }
            }
            None
        }
        NodeType::Node16 => {
            let n = &*(header_ptr as *const Node16);
            let count = n.header.num_children() as usize;
            if let Some(idx) = find_child_node16(&n.keys, count, needle) {
                let raw = n.children[idx].load(Ordering::Acquire);
                if raw != 0 {
                    return Some((idx, TaggedOffset(raw)));
                }
            }
            None
        }
        NodeType::Node48 => {
            let n = &*(header_ptr as *const Node48);
            let slot = n.child_indices[needle as usize];
            if slot != NODE48_EMPTY {
                let raw = n.children[slot as usize].load(Ordering::Acquire);
                if raw != 0 {
                    return Some((needle as usize, TaggedOffset(raw)));
                }
            }
            None
        }
        NodeType::Node256 => {
            let n = &*(header_ptr as *const Node256);
            let raw = n.children[needle as usize].load(Ordering::Acquire);
            if raw != 0 {
                return Some((needle as usize, TaggedOffset(raw)));
            }
            None
        }
    }
}

pub(crate) unsafe fn first_leaf_in_subtree<K: AsBytes + Clone, V: Clone>(
    tree: &ArenaVersionedTree<K, V>,
    mut offset: TaggedOffset,
) -> Option<*const VersionedLeaf<K, V>> {
    while !offset.is_null() {
        if offset.is_leaf() {
            let leaf_ptr =
                tree.arena.get_pointer(offset.leaf_offset()) as *const VersionedLeaf<K, V>;
            let leaf = &*leaf_ptr;
            if !leaf.is_removed(&tree.arena) {
                return Some(leaf_ptr);
            }
            return None;
        }
        let header_ptr = tree.arena.get_pointer(offset.inner_offset()) as *const NodeHeader;
        let header = &*header_ptr;
        let exact = header.exact_leaf.load(Ordering::Acquire);
        if exact != 0 {
            let leaf_ptr = tree.arena.get_pointer(TaggedOffset(exact).leaf_offset())
                as *const VersionedLeaf<K, V>;
            let leaf = &*leaf_ptr;
            if !leaf.is_removed(&tree.arena) {
                return Some(leaf_ptr);
            }
        }
        match next_child_in_node(tree, offset.inner_offset(), usize::MAX) {
            Some((_, child)) => offset = child,
            None => return None,
        }
    }
    None
}

pub(crate) unsafe fn last_leaf_in_subtree<K: AsBytes + Clone, V: Clone>(
    tree: &ArenaVersionedTree<K, V>,
    mut offset: TaggedOffset,
) -> Option<*const VersionedLeaf<K, V>> {
    while !offset.is_null() {
        if offset.is_leaf() {
            let leaf_ptr =
                tree.arena.get_pointer(offset.leaf_offset()) as *const VersionedLeaf<K, V>;
            let leaf = &*leaf_ptr;
            if !leaf.is_removed(&tree.arena) {
                return Some(leaf_ptr);
            }
            return None;
        }
        let header_ptr = tree.arena.get_pointer(offset.inner_offset()) as *const NodeHeader;
        let header = &*header_ptr;
        match prev_child_in_node(tree, offset.inner_offset(), usize::MAX) {
            Some((_, child)) => offset = child,
            None => {
                let exact = header.exact_leaf.load(Ordering::Acquire);
                if exact != 0 {
                    let leaf_ptr = tree.arena.get_pointer(TaggedOffset(exact).leaf_offset())
                        as *const VersionedLeaf<K, V>;
                    let leaf = &*leaf_ptr;
                    if !leaf.is_removed(&tree.arena) {
                        return Some(leaf_ptr);
                    }
                }
                return None;
            }
        }
    }
    None
}
