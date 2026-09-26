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

use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::arena::node::{
    find_child, Leaf, Node16, Node256, Node4, Node48, NodeHeader, TaggedOffset,
};
use crate::arena::Arena;
use crate::key::AsBytes;
use crate::latch::HybridLatch;
use crate::node::{NodeType, MAX_PREFIX_LEN, NODE48_EMPTY};
use crate::simd::find_child_node16;

/// An Adaptive Radix Tree whose nodes and leaves are allocated within a shared [`Arena`].
pub struct ArenaTree<K: AsBytes + Clone, V: Clone> {
    pub(crate) root: AtomicU32,
    pub(crate) root_latch: HybridLatch,
    pub(crate) len: AtomicUsize,
    pub(crate) arena: Arc<Arena>,
    _marker: std::marker::PhantomData<(K, V)>,
}

unsafe impl<K: AsBytes + Clone + Send + Sync, V: Clone + Send + Sync> Send for ArenaTree<K, V> {}
unsafe impl<K: AsBytes + Clone + Send + Sync, V: Clone + Send + Sync> Sync for ArenaTree<K, V> {}

impl<K: AsBytes + Clone, V: Clone> ArenaTree<K, V> {
    pub fn new(arena: Arc<Arena>) -> Self {
        Self {
            root: AtomicU32::new(0),
            root_latch: HybridLatch::new(),
            len: AtomicUsize::new(0),
            arena,
            _marker: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn debug_lookup(&self, key_bytes: &[u8]) {
        let root_raw = self.root.load(Ordering::Acquire);
        println!("ROOT: raw={:08x}", root_raw);
        let mut current = TaggedOffset(root_raw);
        let mut depth = 0;
        while !current.is_null() {
            if current.is_leaf() {
                let leaf_ptr = self.arena.get_pointer(current.leaf_offset()) as *const Leaf<K, V>;
                let leaf = unsafe { &*leaf_ptr };
                println!(
                    "LEAF at depth {}: key={:02x?}, expected={:02x?}",
                    depth,
                    leaf.key.as_bytes(),
                    key_bytes
                );
                return;
            }
            let header_ptr = self.arena.get_pointer(current.inner_offset()) as *const NodeHeader;
            let header = unsafe { &*header_ptr };
            let (matched, complete) = header.match_prefix(key_bytes, depth);
            println!("INNER NODE {:?} at depth {}: prefix={:02x?}, prefix_len={}, matched={}, complete={}, num_children={}",
    				header.node_type, depth, header.prefix_slice(), header.prefix_len, matched, complete, header.num_children);
            if !complete {
                println!(
                    "FAILED PREFIX MATCH: remaining key={:02x?}, node_prefix={:02x?}",
                    &key_bytes[depth..],
                    header.prefix_slice()
                );
                return;
            }
            depth += header.prefix_len as usize;
            if depth >= key_bytes.len() {
                let exact = header.exact_leaf.load(Ordering::Relaxed);
                println!("EXACT LEAF on inner node: raw={:08x}", exact);
                if exact != 0 {
                    let leaf_ptr = self.arena.get_pointer(TaggedOffset(exact).leaf_offset())
                        as *const Leaf<K, V>;
                    let leaf = unsafe { &*leaf_ptr };
                    println!("EXACT LEAF key={:02x?}", leaf.key.as_bytes());
                }
                return;
            }
            let next_byte = key_bytes[depth];
            let child = unsafe { find_child(header_ptr as *mut _, next_byte) };
            println!(
                "CHILD for byte {:02x} at depth {}: {:?}",
                next_byte, depth, child
            );
            match child {
                Some(c) => current = c,
                None => {
                    println!(
                        "CHILD ABSENT for byte {:02x} in {:?}!",
                        next_byte, header.node_type
                    );
                    match header.node_type {
                        NodeType::Node4 => {
                            let n = unsafe { &*(header_ptr as *const Node4) };
                            println!(
                                "Node4 keys: {:?}",
                                &n.keys[..n.header.num_children as usize]
                            );
                        }
                        NodeType::Node16 => {
                            let n = unsafe { &*(header_ptr as *const Node16) };
                            println!(
                                "Node16 keys: {:?}",
                                &n.keys[..n.header.num_children as usize]
                            );
                        }
                        NodeType::Node48 => {
                            let n = unsafe { &*(header_ptr as *const Node48) };
                            println!("Node48 slot: {}", n.child_indices[next_byte as usize]);
                        }
                        NodeType::Node256 => {}
                    }
                    return;
                }
            }
            depth += 1;
        }
    }

    #[inline]
    pub fn arena(&self) -> &Arc<Arena> {
        &self.arena
    }

    #[inline(always)]
    pub fn raw_root(&self) -> TaggedOffset {
        TaggedOffset(self.root.load(Ordering::Acquire))
    }

    pub(crate) fn find_successor(
        &self,
        search_key: &[u8],
        include_equal: bool,
    ) -> Option<*const Leaf<K, V>> {
        'retry: loop {
            let root_raw = self.root.load(Ordering::Acquire);
            if root_raw == 0 {
                return None;
            }
            let root_offset = TaggedOffset(root_raw);

            match unsafe { self.find_successor_in_node(root_offset, search_key, 0, include_equal) }
            {
                Ok(leaf) => return leaf,
                Err(()) => {
                    std::hint::spin_loop();
                    continue 'retry;
                }
            }
        }
    }

    unsafe fn find_successor_in_node(
        &self,
        offset: TaggedOffset,
        search_key: &[u8],
        depth: usize,
        include_equal: bool,
    ) -> Result<Option<*const Leaf<K, V>>, ()> {
        if offset.is_leaf() {
            let leaf_ptr = self.arena.get_pointer(offset.leaf_offset()) as *const Leaf<K, V>;
            let leaf = &*leaf_ptr;
            let k = leaf.key.as_bytes();
            let cmp = k.cmp(search_key);
            if (include_equal && cmp >= std::cmp::Ordering::Equal)
                || (!include_equal && cmp == std::cmp::Ordering::Greater)
            {
                return Ok(Some(leaf_ptr));
            } else {
                return Ok(None);
            }
        }

        let header_ptr = self.arena.get_pointer(offset.inner_offset()) as *const NodeHeader;
        let header = &*header_ptr;
        let v = match header.latch.read_version() {
            Some(v) => v,
            None => return Err(()),
        };

        let remaining_key = if depth < search_key.len() {
            &search_key[depth..]
        } else {
            &[]
        };
        let prefix = header.prefix_slice();

        if prefix > remaining_key {
            if !header.latch.validate(v) {
                return Err(());
            }
            return Ok(crate::arena::iter::first_leaf_in_subtree(self, offset));
        } else if prefix < remaining_key && !remaining_key.starts_with(prefix) {
            if !header.latch.validate(v) {
                return Err(());
            }
            return Ok(None);
        }

        let new_depth = depth + header.prefix_len as usize;

        if new_depth == search_key.len() {
            if include_equal {
                let exact_raw = header.exact_leaf.load(Ordering::Acquire);
                if exact_raw != 0 {
                    if !header.latch.validate(v) {
                        return Err(());
                    }
                    let leaf_ptr = self
                        .arena
                        .get_pointer(TaggedOffset(exact_raw).leaf_offset())
                        as *const Leaf<K, V>;
                    return Ok(Some(leaf_ptr));
                }
            }

            if let Some(leaf) = self.find_first_child_leaf(header_ptr, 0) {
                if !header.latch.validate(v) {
                    return Err(());
                }
                return Ok(Some(leaf));
            }

            if !header.latch.validate(v) {
                return Err(());
            }
            return Ok(None);
        }

        let next_byte = search_key[new_depth];

        if let Some(child) = find_child(header_ptr as *mut _, next_byte) {
            let res =
                self.find_successor_in_node(child, search_key, new_depth + 1, include_equal)?;
            if res.is_some() {
                if !header.latch.validate(v) {
                    return Err(());
                }
                return Ok(res);
            }
        }

        if (next_byte as usize) < 255 {
            if let Some(leaf) = self.find_first_child_leaf(header_ptr, next_byte + 1) {
                if !header.latch.validate(v) {
                    return Err(());
                }
                return Ok(Some(leaf));
            }
        }

        if !header.latch.validate(v) {
            return Err(());
        }
        Ok(None)
    }

    unsafe fn find_first_child_leaf(
        &self,
        header: *const NodeHeader,
        min_byte: u8,
    ) -> Option<*const Leaf<K, V>> {
        let n_type = (*header).node_type;
        match n_type {
            NodeType::Node4 => {
                let n = &*(header as *const Node4);
                let count = n.header.num_children as usize;
                for i in 0..count {
                    if n.keys[i] >= min_byte {
                        let raw = n.children[i].load(Ordering::Acquire);
                        if raw != 0 {
                            if let Some(leaf) =
                                crate::arena::iter::first_leaf_in_subtree(self, TaggedOffset(raw))
                            {
                                return Some(leaf);
                            }
                        }
                    }
                }
                None
            }
            NodeType::Node16 => {
                let n = &*(header as *const Node16);
                let count = n.header.num_children as usize;
                for i in 0..count {
                    if n.keys[i] >= min_byte {
                        let raw = n.children[i].load(Ordering::Acquire);
                        if raw != 0 {
                            if let Some(leaf) =
                                crate::arena::iter::first_leaf_in_subtree(self, TaggedOffset(raw))
                            {
                                return Some(leaf);
                            }
                        }
                    }
                }
                None
            }
            NodeType::Node48 => {
                let n = &*(header as *const Node48);
                for byte in min_byte..=255u8 {
                    let slot = n.child_indices[byte as usize];
                    if slot != NODE48_EMPTY {
                        let raw = n.children[slot as usize].load(Ordering::Acquire);
                        if raw != 0 {
                            if let Some(leaf) =
                                crate::arena::iter::first_leaf_in_subtree(self, TaggedOffset(raw))
                            {
                                return Some(leaf);
                            }
                        }
                    }
                }
                None
            }
            NodeType::Node256 => {
                let n = &*(header as *const Node256);
                for byte in min_byte..=255u8 {
                    let raw = n.children[byte as usize].load(Ordering::Acquire);
                    if raw != 0 {
                        if let Some(leaf) =
                            crate::arena::iter::first_leaf_in_subtree(self, TaggedOffset(raw))
                        {
                            return Some(leaf);
                        }
                    }
                }
                None
            }
        }
    }

    pub(crate) fn find_predecessor(
        &self,
        search_key: &[u8],
        include_equal: bool,
    ) -> Option<*const Leaf<K, V>> {
        'retry: loop {
            let root_raw = self.root.load(Ordering::Acquire);
            if root_raw == 0 {
                return None;
            }
            let root_offset = TaggedOffset(root_raw);

            match unsafe {
                self.find_predecessor_in_node(root_offset, search_key, 0, include_equal)
            } {
                Ok(leaf) => return leaf,
                Err(()) => {
                    std::hint::spin_loop();
                    continue 'retry;
                }
            }
        }
    }

    unsafe fn find_predecessor_in_node(
        &self,
        offset: TaggedOffset,
        search_key: &[u8],
        depth: usize,
        include_equal: bool,
    ) -> Result<Option<*const Leaf<K, V>>, ()> {
        if offset.is_leaf() {
            let leaf_ptr = self.arena.get_pointer(offset.leaf_offset()) as *const Leaf<K, V>;
            let leaf = &*leaf_ptr;
            let k = leaf.key.as_bytes();
            let cmp = k.cmp(search_key);
            if (include_equal && cmp <= std::cmp::Ordering::Equal)
                || (!include_equal && cmp == std::cmp::Ordering::Less)
            {
                return Ok(Some(leaf_ptr));
            } else {
                return Ok(None);
            }
        }

        let header_ptr = self.arena.get_pointer(offset.inner_offset()) as *const NodeHeader;
        let header = &*header_ptr;
        let v = match header.latch.read_version() {
            Some(v) => v,
            None => return Err(()),
        };

        let remaining_key = if depth < search_key.len() {
            &search_key[depth..]
        } else {
            &[]
        };
        let prefix = header.prefix_slice();

        if prefix < remaining_key && !remaining_key.starts_with(prefix) {
            if !header.latch.validate(v) {
                return Err(());
            }
            return Ok(crate::arena::iter::last_leaf_in_subtree(self, offset));
        } else if prefix > remaining_key {
            if !header.latch.validate(v) {
                return Err(());
            }
            return Ok(None);
        }

        let new_depth = depth + header.prefix_len as usize;

        if new_depth >= search_key.len() {
            if include_equal {
                let exact_raw = header.exact_leaf.load(Ordering::Acquire);
                if exact_raw != 0 {
                    if !header.latch.validate(v) {
                        return Err(());
                    }
                    let leaf_ptr = self
                        .arena
                        .get_pointer(TaggedOffset(exact_raw).leaf_offset())
                        as *const Leaf<K, V>;
                    return Ok(Some(leaf_ptr));
                }
            }
            if !header.latch.validate(v) {
                return Err(());
            }
            return Ok(None);
        }

        let next_byte = search_key[new_depth];

        if let Some(child) = find_child(header_ptr as *mut _, next_byte) {
            let res =
                self.find_predecessor_in_node(child, search_key, new_depth + 1, include_equal)?;
            if res.is_some() {
                if !header.latch.validate(v) {
                    return Err(());
                }
                return Ok(res);
            }
        }

        if next_byte > 0 {
            if let Some(leaf) = self.find_last_child_leaf(header_ptr, next_byte - 1) {
                if !header.latch.validate(v) {
                    return Err(());
                }
                return Ok(Some(leaf));
            }
        }

        let exact_raw = header.exact_leaf.load(Ordering::Acquire);
        if exact_raw != 0 {
            if !header.latch.validate(v) {
                return Err(());
            }
            let leaf_ptr = self
                .arena
                .get_pointer(TaggedOffset(exact_raw).leaf_offset())
                as *const Leaf<K, V>;
            return Ok(Some(leaf_ptr));
        }

        if !header.latch.validate(v) {
            return Err(());
        }
        Ok(None)
    }

    unsafe fn find_last_child_leaf(
        &self,
        header: *const NodeHeader,
        max_byte: u8,
    ) -> Option<*const Leaf<K, V>> {
        let n_type = (*header).node_type;
        match n_type {
            NodeType::Node4 => {
                let n = &*(header as *const Node4);
                for i in (0..n.header.num_children as usize).rev() {
                    if n.keys[i] <= max_byte {
                        let raw = n.children[i].load(Ordering::Acquire);
                        if raw != 0 {
                            if let Some(leaf) =
                                crate::arena::iter::last_leaf_in_subtree(self, TaggedOffset(raw))
                            {
                                return Some(leaf);
                            }
                        }
                    }
                }
                None
            }
            NodeType::Node16 => {
                let n = &*(header as *const Node16);
                for i in (0..n.header.num_children as usize).rev() {
                    if n.keys[i] <= max_byte {
                        let raw = n.children[i].load(Ordering::Acquire);
                        if raw != 0 {
                            if let Some(leaf) =
                                crate::arena::iter::last_leaf_in_subtree(self, TaggedOffset(raw))
                            {
                                return Some(leaf);
                            }
                        }
                    }
                }
                None
            }
            NodeType::Node48 => {
                let n = &*(header as *const Node48);
                for byte in (0..=max_byte).rev() {
                    let slot = n.child_indices[byte as usize];
                    if slot != NODE48_EMPTY {
                        let raw = n.children[slot as usize].load(Ordering::Acquire);
                        if raw != 0 {
                            if let Some(leaf) =
                                crate::arena::iter::last_leaf_in_subtree(self, TaggedOffset(raw))
                            {
                                return Some(leaf);
                            }
                        }
                    }
                }
                None
            }
            NodeType::Node256 => {
                let n = &*(header as *const Node256);
                for byte in (0..=max_byte).rev() {
                    let raw = n.children[byte as usize].load(Ordering::Acquire);
                    if raw != 0 {
                        if let Some(leaf) =
                            crate::arena::iter::last_leaf_in_subtree(self, TaggedOffset(raw))
                        {
                            return Some(leaf);
                        }
                    }
                }
                None
            }
        }
    }

    /// Optimistic non-blocking point lookup returning a raw leaf pointer.
    pub fn get_leaf(&self, key_bytes: &[u8]) -> Option<*const Leaf<K, V>> {
        'retry: loop {
            let root_raw = self.root.load(Ordering::Acquire);
            if root_raw == 0 {
                return None;
            }
            let mut current = TaggedOffset(root_raw);
            let mut depth = 0;
            let mut parent_latch: Option<(&HybridLatch, u64)> = None;

            while !current.is_null() {
                if current.is_leaf() {
                    if let Some((latch, v)) = parent_latch {
                        if !latch.validate(v) {
                            continue 'retry;
                        }
                    }
                    let leaf_ptr =
                        self.arena.get_pointer(current.leaf_offset()) as *const Leaf<K, V>;
                    let leaf = unsafe { &*leaf_ptr };
                    if leaf.key.as_bytes() == key_bytes {
                        return Some(leaf_ptr);
                    } else {
                        return None;
                    }
                }

                let header_ptr =
                    self.arena.get_pointer_mut(current.inner_offset()) as *mut NodeHeader;
                let header = unsafe { &*header_ptr };

                let v = match header.latch.read_version() {
                    Some(ver) => ver,
                    None => {
                        std::hint::spin_loop();
                        continue 'retry;
                    }
                };

                let (_matched, is_full) = header.match_prefix(key_bytes, depth);
                if !is_full {
                    if !header.latch.validate(v) {
                        continue 'retry;
                    }
                    return None;
                }

                depth += header.prefix_len as usize;

                if depth == key_bytes.len() {
                    let exact_raw = header.exact_leaf.load(Ordering::Acquire);
                    if !header.latch.validate(v) {
                        continue 'retry;
                    }
                    if exact_raw == 0 {
                        return None;
                    }
                    let exact_leaf_ptr = self
                        .arena
                        .get_pointer(TaggedOffset(exact_raw).leaf_offset())
                        as *const Leaf<K, V>;
                    let leaf = unsafe { &*exact_leaf_ptr };
                    if leaf.key.as_bytes() == key_bytes {
                        return Some(exact_leaf_ptr);
                    } else {
                        return None;
                    }
                }

                let next_byte = key_bytes[depth];
                let next_child = unsafe { find_child(header_ptr, next_byte) };

                if !header.latch.validate(v) {
                    continue 'retry;
                }

                let child = next_child?;
                parent_latch = Some((&header.latch, v));
                current = child;
                depth += 1;
            }

            return None;
        }
    }

    pub fn get_version_le(&self, key_bytes: &[u8], max_version: u64) -> Option<(u64, V)> {
        let leaf_ptr = self.get_leaf(key_bytes)?;
        let mut cur_leaf = leaf_ptr;

        while !cur_leaf.is_null() {
            let leaf = unsafe { &*cur_leaf };
            if leaf.version <= max_version {
                if leaf.removed.load(Ordering::Acquire) {
                    return None;
                }
                return Some((leaf.version, leaf.value.clone()));
            }
            let next_off = leaf.next_version_offset.load(Ordering::Acquire);
            if next_off == 0 {
                break;
            }
            cur_leaf = self.arena.get_pointer(next_off) as *const Leaf<K, V>;
        }

        None
    }

    /// Inserts a key-value pair, returning the previous value if replaced.
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        self.insert_internal(key, 0, value, true)
    }

    /// Inserts a versioned key-value pair.
    pub fn insert_versioned(&self, key: K, version: u64, value: V) -> bool {
        self.insert_internal(key, version, value, false);
        true
    }

    fn alloc_leaf(&self, key: K, version: u64, value: V) -> Option<u32> {
        let size = std::mem::size_of::<Leaf<K, V>>() as u32;
        let off = self.arena.alloc(size, 8, 0)?;
        let ptr = self.arena.get_pointer_mut(off) as *mut Leaf<K, V>;
        // SAFETY: `ptr` is allocated by the arena with sufficient alignment and size.
        unsafe { Leaf::init(ptr, key, version, value) };
        Some(off)
    }

    fn alloc_node4(&self, prefix: &[u8]) -> Option<u32> {
        let size = std::mem::size_of::<Node4>() as u32;
        let off = self.arena.alloc(size, 8, 0)?;
        let ptr = self.arena.get_pointer_mut(off) as *mut Node4;
        // SAFETY: `ptr` is allocated by the arena with sufficient alignment and size.
        unsafe { Node4::init(ptr, prefix) };
        Some(off)
    }

    fn alloc_node16(&self, prefix: &[u8]) -> Option<u32> {
        let size = std::mem::size_of::<Node16>() as u32;
        let off = self.arena.alloc(size, 8, 0)?;
        let ptr = self.arena.get_pointer_mut(off) as *mut Node16;
        // SAFETY: `ptr` is allocated by the arena with sufficient alignment and size.
        unsafe { Node16::init(ptr, prefix) };
        Some(off)
    }

    fn alloc_node48(&self, prefix: &[u8]) -> Option<u32> {
        let size = std::mem::size_of::<Node48>() as u32;
        let off = self.arena.alloc(size, 8, 0)?;
        let ptr = self.arena.get_pointer_mut(off) as *mut Node48;
        // SAFETY: `ptr` is allocated by the arena with sufficient alignment and size.
        unsafe { Node48::init(ptr, prefix) };
        Some(off)
    }

    fn alloc_node256(&self, prefix: &[u8]) -> Option<u32> {
        let size = std::mem::size_of::<Node256>() as u32;
        let off = self.arena.alloc(size, 8, 0)?;
        let ptr = self.arena.get_pointer_mut(off) as *mut Node256;
        // SAFETY: `ptr` is allocated by the arena with sufficient alignment and size.
        unsafe { Node256::init(ptr, prefix) };
        Some(off)
    }

    fn insert_internal(
        &self,
        key: K,
        version: u64,
        value: V,
        replace_if_present: bool,
    ) -> Option<V> {
        let leaf_off = self.alloc_leaf(key, version, value).expect("arena full");
        let new_leaf_ptr = self.arena.get_pointer_mut(leaf_off) as *mut Leaf<K, V>;
        let tagged_new_leaf = TaggedOffset::from_leaf(leaf_off);
        let key_bytes = unsafe { (*new_leaf_ptr).key.as_bytes() };

        'retry: loop {
            let root_raw = self.root.load(Ordering::Acquire);

            // Case 0: Empty tree
            if root_raw == 0 {
                let _ = self.root_latch.lock();
                if self.root.load(Ordering::Relaxed) == 0 {
                    self.root.store(tagged_new_leaf.raw(), Ordering::Release);
                    self.len.fetch_add(1, Ordering::Relaxed);
                    self.root_latch.unlock();
                    return None;
                }
                self.root_latch.unlock();
                continue 'retry;
            }

            let root_offset = TaggedOffset(root_raw);

            // Case 1: Root is a single leaf
            if root_offset.is_leaf() {
                let _ = self.root_latch.lock();
                let cur_root = TaggedOffset(self.root.load(Ordering::Relaxed));
                if !cur_root.is_leaf() {
                    self.root_latch.unlock();
                    continue 'retry;
                }

                let existing_leaf_ptr =
                    self.arena.get_pointer_mut(cur_root.leaf_offset()) as *mut Leaf<K, V>;
                let existing_leaf = unsafe { &mut *existing_leaf_ptr };

                if existing_leaf.key.as_bytes() == key_bytes {
                    if replace_if_present {
                        let old = std::mem::replace(&mut existing_leaf.value, unsafe {
                            (*new_leaf_ptr).value.clone()
                        });
                        existing_leaf.removed.store(false, Ordering::Release);
                        self.root_latch.unlock();
                        return Some(old);
                    } else {
                        // Versioned insert: prepend new leaf to version chain
                        unsafe {
                            (*new_leaf_ptr)
                                .next_version_offset
                                .store(cur_root.leaf_offset(), Ordering::Relaxed);
                        }
                        self.root.store(tagged_new_leaf.raw(), Ordering::Release);
                        self.root_latch.unlock();
                        return None;
                    }
                }

                let existing_key = existing_leaf.key.as_bytes();
                let common_len = longest_common_prefix(existing_key, key_bytes);

                let exact1 = existing_key.len() == common_len;
                let byte1 = if !exact1 { existing_key[common_len] } else { 0 };

                let exact2 = key_bytes.len() == common_len;
                let byte2 = if !exact2 { key_bytes[common_len] } else { 0 };

                let new_root = self.create_prefix_chain(
                    &key_bytes[..common_len],
                    exact1,
                    byte1,
                    cur_root,
                    exact2,
                    byte2,
                    tagged_new_leaf,
                );
                self.root.store(new_root.raw(), Ordering::Release);
                self.len.fetch_add(1, Ordering::Relaxed);
                self.root_latch.unlock();
                return None;
            }

            // Case 2: Root is an InnerNode
            let mut parent: Option<*mut NodeHeader> = None;
            let mut parent_byte = 0u8;
            let mut current = root_offset;
            let mut depth = 0;

            'traverse: loop {
                let header_ptr =
                    self.arena.get_pointer_mut(current.inner_offset()) as *mut NodeHeader;
                let header = unsafe { &mut *header_ptr };

                let v_header = match header.latch.read_version() {
                    Some(v) => v,
                    None => {
                        std::hint::spin_loop();
                        continue 'retry;
                    }
                };

                let (matched, is_full) = header.match_prefix(key_bytes, depth);

                // Prefix mismatch -> prefix split
                if !is_full {
                    let parent_ok = match parent {
                        Some(p) => unsafe { (*p).latch.lock().is_ok() },
                        None => self.root_latch.lock().is_ok(),
                    };
                    if !parent_ok {
                        continue 'retry;
                    }

                    if header.latch.lock_version(v_header).is_err() {
                        match parent {
                            Some(p) => unsafe { (*p).latch.unlock() },
                            None => self.root_latch.unlock(),
                        }
                        continue 'retry;
                    }

                    let parent_valid = match parent {
                        Some(p) => unsafe { find_child(p, parent_byte) == Some(current) },
                        None => self.root.load(Ordering::Acquire) == current.raw(),
                    };
                    if !parent_valid {
                        header.latch.unlock();
                        match parent {
                            Some(p) => unsafe { (*p).latch.unlock() },
                            None => self.root_latch.unlock(),
                        }
                        continue 'retry;
                    }

                    let cur_prefix_len = (header.prefix_len as usize).min(MAX_PREFIX_LEN);
                    let mut cur_prefix_buf = [0u8; MAX_PREFIX_LEN];
                    cur_prefix_buf[..cur_prefix_len].copy_from_slice(header.prefix_slice());
                    let cur_prefix = &cur_prefix_buf[..cur_prefix_len];

                    let mismatch_char_existing = cur_prefix[matched];

                    let split_node_off = self
                        .alloc_node4(&cur_prefix[..matched])
                        .expect("arena full");
                    let split_node = self.arena.get_pointer_mut(split_node_off) as *mut Node4;

                    let remaining_prefix = &cur_prefix[(matched + 1)..];
                    let mut new_p = [0u8; MAX_PREFIX_LEN];
                    let new_p_len = remaining_prefix.len().min(MAX_PREFIX_LEN);
                    new_p[..new_p_len].copy_from_slice(&remaining_prefix[..new_p_len]);
                    header.prefix = new_p;
                    header.prefix_len = remaining_prefix.len() as u16;

                    unsafe {
                        (*split_node).insert_child(
                            mismatch_char_existing,
                            TaggedOffset::from_inner(current.inner_offset()),
                        );

                        if depth + matched == key_bytes.len() {
                            (*split_node)
                                .header
                                .exact_leaf
                                .store(tagged_new_leaf.raw(), Ordering::Relaxed);
                        } else {
                            let new_char = key_bytes[depth + matched];
                            (*split_node).insert_child(new_char, tagged_new_leaf);
                        }

                        let tagged_split = TaggedOffset::from_inner(split_node_off);

                        match parent {
                            Some(p) => self.replace_child(p, parent_byte, tagged_split),
                            None => self.root.store(tagged_split.raw(), Ordering::Release),
                        }

                        self.len.fetch_add(1, Ordering::Relaxed);
                        header.latch.unlock();
                        match parent {
                            Some(p) => (*p).latch.unlock(),
                            None => self.root_latch.unlock(),
                        }
                    }
                    return None;
                }

                depth += header.prefix_len as usize;

                // Exact key match at this inner node
                if depth == key_bytes.len() {
                    if header.latch.lock_version(v_header).is_err() {
                        continue 'retry;
                    }

                    let parent_valid = match parent {
                        Some(p) => unsafe {
                            !(*p).latch.is_obsolete() && find_child(p, parent_byte) == Some(current)
                        },
                        None => self.root.load(Ordering::Acquire) == current.raw(),
                    };
                    if !parent_valid {
                        header.latch.unlock();
                        continue 'retry;
                    }

                    let exact_raw = header.exact_leaf.load(Ordering::Acquire);
                    if exact_raw != 0 {
                        let existing_leaf_ptr = self
                            .arena
                            .get_pointer_mut(TaggedOffset(exact_raw).leaf_offset())
                            as *mut Leaf<K, V>;
                        let existing_leaf = unsafe { &mut *existing_leaf_ptr };

                        if replace_if_present {
                            let old = std::mem::replace(&mut existing_leaf.value, unsafe {
                                (*new_leaf_ptr).value.clone()
                            });
                            existing_leaf.removed.store(false, Ordering::Release);
                            header.latch.unlock();
                            return Some(old);
                        } else {
                            unsafe {
                                (*new_leaf_ptr).next_version_offset.store(
                                    TaggedOffset(exact_raw).leaf_offset(),
                                    Ordering::Relaxed,
                                );
                                header
                                    .exact_leaf
                                    .store(tagged_new_leaf.raw(), Ordering::Release);
                                header.latch.unlock();
                            }
                            return None;
                        }
                    } else {
                        header
                            .exact_leaf
                            .store(tagged_new_leaf.raw(), Ordering::Release);
                        self.len.fetch_add(1, Ordering::Relaxed);
                        header.latch.unlock();
                        return None;
                    }
                }

                let next_byte = key_bytes[depth];
                let next_child = unsafe { find_child(header_ptr, next_byte) };

                match next_child {
                    None => {
                        if is_node_full(header) {
                            // Node needs to grow: lock parent and header
                            let parent_ok = match parent {
                                Some(p) => unsafe { (*p).latch.lock().is_ok() },
                                None => self.root_latch.lock().is_ok(),
                            };
                            if !parent_ok {
                                continue 'retry;
                            }

                            if header.latch.lock_version(v_header).is_err() {
                                match parent {
                                    Some(p) => unsafe { (*p).latch.unlock() },
                                    None => self.root_latch.unlock(),
                                }
                                continue 'retry;
                            }

                            let parent_valid = match parent {
                                Some(p) => unsafe { find_child(p, parent_byte) == Some(current) },
                                None => self.root.load(Ordering::Acquire) == current.raw(),
                            };
                            if !parent_valid {
                                header.latch.unlock();
                                match parent {
                                    Some(p) => unsafe { (*p).latch.unlock() },
                                    None => self.root_latch.unlock(),
                                }
                                continue 'retry;
                            }

                            if unsafe { find_child(header_ptr, next_byte) }.is_some() {
                                header.latch.unlock();
                                match parent {
                                    Some(p) => unsafe { (*p).latch.unlock() },
                                    None => self.root_latch.unlock(),
                                }
                                continue 'retry;
                            }

                            let new_node_off = unsafe { self.grow_node(header_ptr) };
                            let new_node_ptr =
                                self.arena.get_pointer_mut(new_node_off) as *mut NodeHeader;
                            unsafe {
                                self.insert_child_into_node(
                                    new_node_ptr,
                                    next_byte,
                                    tagged_new_leaf,
                                );
                            }
                            let tagged_new_node = TaggedOffset::from_inner(new_node_off);

                            match parent {
                                Some(p) => unsafe {
                                    self.replace_child(p, parent_byte, tagged_new_node)
                                },
                                None => self.root.store(tagged_new_node.raw(), Ordering::Release),
                            }

                            header.latch.mark_obsolete_and_unlock();
                            match parent {
                                Some(p) => unsafe { (*p).latch.unlock() },
                                None => self.root_latch.unlock(),
                            }
                            self.len.fetch_add(1, Ordering::Relaxed);
                            return None;
                        } else {
                            // Node has room: lock header only
                            if header.latch.lock_version(v_header).is_err() {
                                continue 'retry;
                            }

                            let parent_valid = match parent {
                                Some(p) => unsafe {
                                    !(*p).latch.is_obsolete()
                                        && find_child(p, parent_byte) == Some(current)
                                },
                                None => self.root.load(Ordering::Acquire) == current.raw(),
                            };
                            if !parent_valid {
                                header.latch.unlock();
                                continue 'retry;
                            }

                            if is_node_full(header)
                                || unsafe { find_child(header_ptr, next_byte) }.is_some()
                            {
                                header.latch.unlock();
                                continue 'retry;
                            }

                            unsafe {
                                self.insert_child_into_node(header_ptr, next_byte, tagged_new_leaf);
                            }
                            header.latch.unlock();
                            self.len.fetch_add(1, Ordering::Relaxed);
                            return None;
                        }
                    }
                    Some(child) => {
                        if child.is_leaf() {
                            if header.latch.lock_version(v_header).is_err() {
                                continue 'retry;
                            }

                            let parent_valid = match parent {
                                Some(p) => unsafe {
                                    !(*p).latch.is_obsolete()
                                        && find_child(p, parent_byte) == Some(current)
                                },
                                None => self.root.load(Ordering::Acquire) == current.raw(),
                            };
                            if !parent_valid {
                                header.latch.unlock();
                                continue 'retry;
                            }
                            if unsafe { find_child(header_ptr, next_byte) } != Some(child) {
                                header.latch.unlock();
                                continue 'retry;
                            }

                            let existing_leaf_ptr =
                                self.arena.get_pointer_mut(child.leaf_offset()) as *mut Leaf<K, V>;
                            let existing_leaf = unsafe { &mut *existing_leaf_ptr };

                            if existing_leaf.key.as_bytes() == key_bytes {
                                if replace_if_present {
                                    let old = std::mem::replace(&mut existing_leaf.value, unsafe {
                                        (*new_leaf_ptr).value.clone()
                                    });
                                    existing_leaf.removed.store(false, Ordering::Release);
                                    header.latch.unlock();
                                    return Some(old);
                                } else {
                                    unsafe {
                                        (*new_leaf_ptr)
                                            .next_version_offset
                                            .store(child.leaf_offset(), Ordering::Relaxed);
                                        self.replace_child(header_ptr, next_byte, tagged_new_leaf);
                                        header.latch.unlock();
                                    }
                                    return None;
                                }
                            }

                            let existing_key = existing_leaf.key.as_bytes();
                            let suffix_existing = &existing_key[(depth + 1)..];
                            let suffix_new = &key_bytes[(depth + 1)..];
                            let common_len = longest_common_prefix(suffix_existing, suffix_new);

                            let exact1 = suffix_existing.len() == common_len;
                            let byte1 = if !exact1 {
                                suffix_existing[common_len]
                            } else {
                                0
                            };

                            let exact2 = suffix_new.len() == common_len;
                            let byte2 = if !exact2 { suffix_new[common_len] } else { 0 };

                            let new_inner = self.create_prefix_chain(
                                &suffix_new[..common_len],
                                exact1,
                                byte1,
                                child,
                                exact2,
                                byte2,
                                tagged_new_leaf,
                            );

                            unsafe { self.replace_child(header_ptr, next_byte, new_inner) };
                            self.len.fetch_add(1, Ordering::Relaxed);
                            header.latch.unlock();
                            return None;
                        } else {
                            if !header.latch.validate(v_header) {
                                continue 'retry;
                            }
                            parent = Some(header_ptr);
                            parent_byte = next_byte;
                            current = child;
                            depth += 1;
                            continue 'traverse;
                        }
                    }
                }
            }
        }
    }

    unsafe fn replace_child(&self, header: *mut NodeHeader, byte: u8, new_child: TaggedOffset) {
        match (*header).node_type {
            NodeType::Node4 => {
                let n = &mut *(header as *mut Node4);
                for i in 0..n.header.num_children as usize {
                    if n.keys[i] == byte {
                        n.children[i].store(new_child.raw(), Ordering::Release);
                        return;
                    }
                }
            }
            NodeType::Node16 => {
                let n = &mut *(header as *mut Node16);
                if let Some(idx) = find_child_node16(&n.keys, n.header.num_children as usize, byte)
                {
                    n.children[idx].store(new_child.raw(), Ordering::Release);
                }
            }
            NodeType::Node48 => {
                let n = &mut *(header as *mut Node48);
                let slot = n.child_indices[byte as usize];
                if slot != NODE48_EMPTY {
                    n.children[slot as usize].store(new_child.raw(), Ordering::Release);
                }
            }
            NodeType::Node256 => {
                let n = &mut *(header as *mut Node256);
                n.children[byte as usize].store(new_child.raw(), Ordering::Release);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn create_prefix_chain(
        &self,
        prefix: &[u8],
        exact1: bool,
        byte1: u8,
        child1: TaggedOffset,
        exact2: bool,
        byte2: u8,
        child2: TaggedOffset,
    ) -> TaggedOffset {
        if prefix.len() <= MAX_PREFIX_LEN {
            let n4_off = self.alloc_node4(prefix).expect("arena full");
            let n4 = self.arena.get_pointer_mut(n4_off) as *mut Node4;
            unsafe {
                if exact1 {
                    (*n4)
                        .header
                        .exact_leaf
                        .store(child1.raw(), Ordering::Relaxed);
                } else {
                    (*n4).insert_child(byte1, child1);
                }
                if exact2 {
                    (*n4)
                        .header
                        .exact_leaf
                        .store(child2.raw(), Ordering::Relaxed);
                } else {
                    (*n4).insert_child(byte2, child2);
                }
            }
            TaggedOffset::from_inner(n4_off)
        } else {
            let head_prefix = &prefix[..MAX_PREFIX_LEN];
            let rest_prefix = &prefix[MAX_PREFIX_LEN + 1..];
            let connector_byte = prefix[MAX_PREFIX_LEN];

            let child_chain =
                self.create_prefix_chain(rest_prefix, exact1, byte1, child1, exact2, byte2, child2);

            let n4_off = self.alloc_node4(head_prefix).expect("arena full");
            let n4 = self.arena.get_pointer_mut(n4_off) as *mut Node4;
            unsafe {
                (*n4).insert_child(connector_byte, child_chain);
            }
            TaggedOffset::from_inner(n4_off)
        }
    }

    #[inline]
    unsafe fn insert_child_into_node(
        &self,
        header: *mut NodeHeader,
        byte: u8,
        child: TaggedOffset,
    ) {
        match (*header).node_type {
            NodeType::Node4 => (*(header as *mut Node4)).insert_child(byte, child),
            NodeType::Node16 => (*(header as *mut Node16)).insert_child(byte, child),
            NodeType::Node48 => (*(header as *mut Node48)).insert_child(byte, child),
            NodeType::Node256 => (*(header as *mut Node256)).insert_child(byte, child),
        }
    }

    unsafe fn grow_node(&self, header: *mut NodeHeader) -> u32 {
        match (*header).node_type {
            NodeType::Node4 => {
                let old = &*(header as *const Node4);
                let n16_off = self
                    .alloc_node16(old.header.prefix_slice())
                    .expect("arena full");
                let n16 = self.arena.get_pointer_mut(n16_off) as *mut Node16;
                (*n16).header.exact_leaf.store(
                    old.header.exact_leaf.load(Ordering::Relaxed),
                    Ordering::Relaxed,
                );
                for i in 0..old.header.num_children as usize {
                    let child = TaggedOffset(old.children[i].load(Ordering::Relaxed));
                    (*n16).insert_child(old.keys[i], child);
                }
                n16_off
            }
            NodeType::Node16 => {
                let old = &*(header as *const Node16);
                let n48_off = self
                    .alloc_node48(old.header.prefix_slice())
                    .expect("arena full");
                let n48 = self.arena.get_pointer_mut(n48_off) as *mut Node48;
                (*n48).header.exact_leaf.store(
                    old.header.exact_leaf.load(Ordering::Relaxed),
                    Ordering::Relaxed,
                );
                for i in 0..old.header.num_children as usize {
                    let child = TaggedOffset(old.children[i].load(Ordering::Relaxed));
                    (*n48).insert_child(old.keys[i], child);
                }
                n48_off
            }
            NodeType::Node48 => {
                let old = &*(header as *const Node48);
                let n256_off = self
                    .alloc_node256(old.header.prefix_slice())
                    .expect("arena full");
                let n256 = self.arena.get_pointer_mut(n256_off) as *mut Node256;
                (*n256).header.exact_leaf.store(
                    old.header.exact_leaf.load(Ordering::Relaxed),
                    Ordering::Relaxed,
                );
                for byte in 0..=255u8 {
                    let slot = old.child_indices[byte as usize];
                    if slot != NODE48_EMPTY {
                        let child =
                            TaggedOffset(old.children[slot as usize].load(Ordering::Relaxed));
                        (*n256).insert_child(byte, child);
                    }
                }
                n256_off
            }
            NodeType::Node256 => panic!("cannot grow Node256"),
        }
    }
}

#[inline(always)]
fn is_node_full(header: &NodeHeader) -> bool {
    match header.node_type {
        NodeType::Node4 => header.num_children >= 4,
        NodeType::Node16 => header.num_children >= 16,
        NodeType::Node48 => header.num_children >= 48,
        NodeType::Node256 => false,
    }
}

#[inline(always)]
fn longest_common_prefix(a: &[u8], b: &[u8]) -> usize {
    let len = a.len().min(b.len());
    let mut matched = 0;
    while matched < len && a[matched] == b[matched] {
        matched += 1;
    }
    matched
}
