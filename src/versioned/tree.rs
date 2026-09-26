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

//! # Multi-Version Adaptive Radix Tree (EBR)
//!
//! Implements lock-free snapshot reads and versioned prepend chains with `crossbeam-epoch`.

use crossbeam_epoch::Guard;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use crate::key::AsBytes;
use crate::latch::HybridLatch;
use crate::node::{
    Node16, Node256, Node4, Node48, NodeHeader, NodeType, TaggedPtr, VersionedLeaf, MAX_PREFIX_LEN,
    NODE48_EMPTY,
};

/// Internal multi-version concurrent tree structure.
pub struct VersionedTree<K: AsBytes + Send + 'static, V: Send + Clone + 'static> {
    root: AtomicPtr<u8>,
    root_latch: HybridLatch,
    len: AtomicUsize,
    _marker: PhantomData<(K, V)>,
}

unsafe impl<K: AsBytes + Send + Sync + 'static, V: Send + Sync + Clone + 'static> Send
    for VersionedTree<K, V>
{
}
unsafe impl<K: AsBytes + Send + Sync + 'static, V: Send + Sync + Clone + 'static> Sync
    for VersionedTree<K, V>
{
}

impl<K: AsBytes + Send + 'static, V: Send + Clone + 'static> Default for VersionedTree<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: AsBytes + Send + 'static, V: Send + Clone + 'static> Drop for VersionedTree<K, V> {
    fn drop(&mut self) {
        let root = TaggedPtr::from_raw(self.root.load(Ordering::Relaxed));
        if !root.is_null() {
            unsafe {
                drop_subtree::<K, V>(root);
            }
        }
    }
}

unsafe fn drop_subtree<K: AsBytes + Send + 'static, V: Send + Clone + 'static>(ptr: TaggedPtr) {
    if ptr.is_leaf() {
        let mut cur = ptr.as_versioned_leaf_ptr::<K, V>();
        while !cur.is_null() {
            let mut leaf = Box::from_raw(cur);
            let next = leaf.next_version.load(Ordering::Relaxed);
            if !leaf.value_taken.load(Ordering::Acquire) {
                ManuallyDrop::drop(&mut leaf.value);
            }
            cur = next;
        }
    } else {
        let header = &*ptr.as_inner_ptr();
        if let Some(leaf_ptr) = header.load_exact_versioned_leaf::<K, V>(Ordering::Relaxed) {
            let mut cur = leaf_ptr;
            while !cur.is_null() {
                let mut leaf = Box::from_raw(cur);
                let next = leaf.next_version.load(Ordering::Relaxed);
                if !leaf.value_taken.load(Ordering::Acquire) {
                    ManuallyDrop::drop(&mut leaf.value);
                }
                cur = next;
            }
        }
        match header.node_type {
            NodeType::Node4 => {
                let n = &*(ptr.as_inner_ptr() as *const Node4);
                for i in 0..n.header.num_children as usize {
                    let child = TaggedPtr::from_raw(n.children[i].load(Ordering::Relaxed));
                    if !child.is_null() {
                        drop_subtree::<K, V>(child);
                    }
                }
                drop(Box::from_raw(ptr.as_inner_ptr() as *mut Node4));
            }
            NodeType::Node16 => {
                let n = &*(ptr.as_inner_ptr() as *const Node16);
                for i in 0..n.header.num_children as usize {
                    let child = TaggedPtr::from_raw(n.children[i].load(Ordering::Relaxed));
                    if !child.is_null() {
                        drop_subtree::<K, V>(child);
                    }
                }
                drop(Box::from_raw(ptr.as_inner_ptr() as *mut Node16));
            }
            NodeType::Node48 => {
                let n = &*(ptr.as_inner_ptr() as *const Node48);
                for i in 0..48 {
                    let child = TaggedPtr::from_raw(n.children[i].load(Ordering::Relaxed));
                    if !child.is_null() {
                        drop_subtree::<K, V>(child);
                    }
                }
                drop(Box::from_raw(ptr.as_inner_ptr() as *mut Node48));
            }
            NodeType::Node256 => {
                let n = &*(ptr.as_inner_ptr() as *const Node256);
                for i in 0..256 {
                    let child = TaggedPtr::from_raw(n.children[i].load(Ordering::Relaxed));
                    if !child.is_null() {
                        drop_subtree::<K, V>(child);
                    }
                }
                drop(Box::from_raw(ptr.as_inner_ptr() as *mut Node256));
            }
        }
    }
}

impl<K: AsBytes + Send + 'static, V: Send + Clone + 'static> VersionedTree<K, V> {
    pub const fn new() -> Self {
        Self {
            root: AtomicPtr::new(ptr::null_mut()),
            root_latch: HybridLatch::new(),
            len: AtomicUsize::new(0),
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn raw_root(&self) -> TaggedPtr {
        TaggedPtr::from_raw(self.root.load(Ordering::Acquire))
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Optimistic non-blocking lookup returning the head version leaf pointer.
    pub fn get_leaf<Q>(&self, key: &Q, _guard: &Guard) -> Option<*mut VersionedLeaf<K, V>>
    where
        Q: AsBytes + ?Sized,
    {
        let key_bytes = key.as_bytes();
        'retry: loop {
            let root_ptr = TaggedPtr::from_raw(self.root.load(Ordering::Acquire));
            if root_ptr.is_null() {
                return None;
            }

            if root_ptr.is_leaf() {
                let leaf_ptr = root_ptr.as_versioned_leaf_ptr::<K, V>();
                let leaf = unsafe { &*leaf_ptr };
                if leaf.key.as_bytes() == key_bytes {
                    return Some(leaf_ptr);
                } else {
                    return None;
                }
            }

            let mut current = root_ptr;
            let mut depth = 0;
            let mut parent_latch: Option<(&HybridLatch, u64)> = None;

            loop {
                if current.is_leaf() {
                    let leaf_ptr = current.as_versioned_leaf_ptr::<K, V>();
                    let leaf = unsafe { &*leaf_ptr };
                    if let Some((latch, v)) = parent_latch {
                        if !latch.validate(v) {
                            continue 'retry;
                        }
                    }
                    if leaf.key.as_bytes() == key_bytes {
                        return Some(leaf_ptr);
                    } else {
                        return None;
                    }
                }

                let header = unsafe { &*current.as_inner_ptr() };
                let v = match header.latch.read_version() {
                    Some(v) => v,
                    None => continue 'retry,
                };

                if let Some((latch, pv)) = parent_latch {
                    if !latch.validate(pv) {
                        continue 'retry;
                    }
                }

                let (matched, complete) = header.match_prefix(key_bytes, depth);
                if !complete {
                    if !header.latch.validate(v) {
                        continue 'retry;
                    }
                    return None;
                }
                depth += matched;

                if depth == key_bytes.len() {
                    let exact = header.load_exact_versioned_leaf::<K, V>(Ordering::Acquire);
                    if !header.latch.validate(v) {
                        continue 'retry;
                    }
                    let leaf_ptr = exact?;
                    let leaf = unsafe { &*leaf_ptr };
                    if leaf.key.as_bytes() == key_bytes {
                        return Some(leaf_ptr);
                    } else {
                        return None;
                    }
                }

                let next_byte = key_bytes[depth];
                let next_child = unsafe { find_child(header, next_byte) };

                if !header.latch.validate(v) {
                    continue 'retry;
                }

                let child = next_child?;
                parent_latch = Some((&header.latch, v));
                current = child;
                depth += 1;
            }
        }
    }

    /// Looks up the newest committed version and value.
    pub fn get_latest<'g, Q>(&self, key: &Q, guard: &'g Guard) -> Option<(u64, &'g V)>
    where
        Q: AsBytes + ?Sized,
    {
        let leaf_ptr = self.get_leaf(key, guard)?;
        let leaf = unsafe { &*leaf_ptr };
        if leaf.removed.load(Ordering::Acquire) {
            return None;
        }
        Some((leaf.version, &*leaf.value))
    }

    /// Looks up the newest version that is $\le$ `max_version`.
    pub fn get_version_le<'g, Q>(
        &self,
        key: &Q,
        max_version: u64,
        guard: &'g Guard,
    ) -> Option<(u64, &'g V)>
    where
        Q: AsBytes + ?Sized,
    {
        let leaf_ptr = self.get_leaf(key, guard)?;
        let mut cur = leaf_ptr;
        while !cur.is_null() {
            let leaf = unsafe { &*cur };
            if leaf.version <= max_version {
                if leaf.removed.load(Ordering::Acquire) {
                    return None;
                }
                return Some((leaf.version, &*leaf.value));
            }
            cur = leaf.next_version.load(Ordering::Acquire);
        }
        None
    }

    /// Inserts a versioned key-value pair, prepending to the version chain if the key already exists.
    pub fn insert(&self, key: K, version: u64, value: V, guard: &Guard) -> bool {
        let new_leaf_box = VersionedLeaf::new(key, version, value);
        let new_leaf_ptr = Box::into_raw(new_leaf_box);
        let tagged_new_leaf = TaggedPtr::from_versioned_leaf(new_leaf_ptr);
        let key_bytes = unsafe { (*new_leaf_ptr).key.as_bytes() };

        'retry: loop {
            let root_ptr = TaggedPtr::from_raw(self.root.load(Ordering::Acquire));

            // Case 0: Empty tree
            if root_ptr.is_null() {
                let _ = self.root_latch.lock();
                if self.root.load(Ordering::Relaxed).is_null() {
                    self.root.store(tagged_new_leaf.as_raw(), Ordering::Release);
                    self.len.fetch_add(1, Ordering::Relaxed);
                    self.root_latch.unlock();
                    return true;
                }
                self.root_latch.unlock();
                continue 'retry;
            }

            // Case 1: Root is a single leaf
            if root_ptr.is_leaf() {
                let _ = self.root_latch.lock();
                let cur_root = TaggedPtr::from_raw(self.root.load(Ordering::Relaxed));
                if !cur_root.is_leaf() {
                    self.root_latch.unlock();
                    continue 'retry;
                }

                let existing_leaf_ptr = cur_root.as_versioned_leaf_ptr::<K, V>();
                let existing_leaf = unsafe { &*existing_leaf_ptr };

                if existing_leaf.key.as_bytes() == key_bytes {
                    // Prepend new leaf to version chain
                    unsafe {
                        (*new_leaf_ptr)
                            .next_version
                            .store(existing_leaf_ptr, Ordering::Relaxed);
                    }
                    self.root.store(tagged_new_leaf.as_raw(), Ordering::Release);
                    self.root_latch.unlock();
                    return true;
                }

                let existing_key = existing_leaf.key.as_bytes();
                let common_len = longest_common_prefix(existing_key, key_bytes);

                let exact1 = existing_key.len() == common_len;
                let byte1 = if !exact1 { existing_key[common_len] } else { 0 };

                let exact2 = key_bytes.len() == common_len;
                let byte2 = if !exact2 { key_bytes[common_len] } else { 0 };

                let new_inner = self.create_prefix_chain(
                    &key_bytes[..common_len],
                    exact1,
                    byte1,
                    cur_root,
                    exact2,
                    byte2,
                    tagged_new_leaf,
                );

                self.root.store(new_inner.as_raw(), Ordering::Release);
                self.len.fetch_add(1, Ordering::Relaxed);
                self.root_latch.unlock();
                return true;
            }

            // Case 2: Root is an inner node
            let mut parent: Option<*mut NodeHeader> = None;
            let mut parent_byte: u8 = 0;
            let mut current = root_ptr;
            let mut depth = 0;

            'traverse: loop {
                let header = unsafe { &*current.as_inner_ptr() };
                let v_header = match header.latch.read_version() {
                    Some(v) => v,
                    None => continue 'retry,
                };

                let (matched, complete) = header.match_prefix(key_bytes, depth);
                if !complete {
                    let parent_ok = match parent {
                        Some(p) => unsafe { (*p).latch.lock().is_ok() },
                        None => self.root_latch.lock().is_ok(),
                    };
                    if !parent_ok {
                        continue 'retry;
                    }

                    if !header.latch.lock().is_ok() {
                        match parent {
                            Some(p) => unsafe { (*p).latch.unlock() },
                            None => self.root_latch.unlock(),
                        }
                        continue 'retry;
                    }

                    let prefix = header.prefix_slice();
                    let byte_existing = prefix[matched];

                    let exact_new = depth + matched == key_bytes.len();
                    let byte_new = if !exact_new {
                        key_bytes[depth + matched]
                    } else {
                        0
                    };

                    let new_n4 = Node4::new(&prefix[..matched]);
                    let n4_ptr = Box::into_raw(new_n4);

                    let remaining_existing = &prefix[(matched + 1)..];
                    let old_inner = current.as_inner_ptr();
                    unsafe {
                        (*old_inner).set_prefix(remaining_existing);
                    }

                    unsafe {
                        (*n4_ptr).insert_child(byte_existing, current);
                        if exact_new {
                            (*n4_ptr)
                                .header
                                .exact_leaf
                                .store(tagged_new_leaf.as_raw(), Ordering::Relaxed);
                        } else {
                            let new_chain = self.create_single_leaf_chain(
                                &key_bytes[(depth + matched + 1)..],
                                tagged_new_leaf,
                            );
                            (*n4_ptr).insert_child(byte_new, new_chain);
                        }
                    }

                    let new_inner = TaggedPtr::from_inner(n4_ptr as *mut NodeHeader);

                    match parent {
                        Some(p) => unsafe { self.replace_child(p, parent_byte, new_inner) },
                        None => self.root.store(new_inner.as_raw(), Ordering::Release),
                    }

                    self.len.fetch_add(1, Ordering::Relaxed);
                    header.latch.unlock();
                    match parent {
                        Some(p) => unsafe { (*p).latch.unlock() },
                        None => self.root_latch.unlock(),
                    }
                    return true;
                }

                depth += matched;

                if depth == key_bytes.len() {
                    if !header.latch.lock().is_ok() {
                        continue 'retry;
                    }

                    let exact_raw = header.exact_leaf.load(Ordering::Acquire);
                    if !exact_raw.is_null() {
                        let existing_leaf_ptr =
                            TaggedPtr::from_raw(exact_raw).as_versioned_leaf_ptr::<K, V>();
                        unsafe {
                            (*new_leaf_ptr)
                                .next_version
                                .store(existing_leaf_ptr, Ordering::Relaxed);
                        }
                        header
                            .exact_leaf
                            .store(tagged_new_leaf.as_raw(), Ordering::Release);
                        header.latch.unlock();
                        return true;
                    } else {
                        header
                            .exact_leaf
                            .store(tagged_new_leaf.as_raw(), Ordering::Release);
                        self.len.fetch_add(1, Ordering::Relaxed);
                        header.latch.unlock();
                        return true;
                    }
                }

                let next_byte = key_bytes[depth];
                let next_child = unsafe { find_child(header, next_byte) };

                match next_child {
                    None => {
                        if is_node_full(header) {
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
                                Some(p) => unsafe { find_child(&*p, parent_byte) == Some(current) },
                                None => self.root.load(Ordering::Acquire) == current.as_raw(),
                            };
                            if !parent_valid {
                                header.latch.unlock();
                                match parent {
                                    Some(p) => unsafe { (*p).latch.unlock() },
                                    None => self.root_latch.unlock(),
                                }
                                continue 'retry;
                            }

                            let new_node = self.grow_node(current.as_inner_ptr(), guard);
                            let new_child = self.create_single_leaf_chain(
                                &key_bytes[(depth + 1)..],
                                tagged_new_leaf,
                            );
                            unsafe {
                                self.insert_child_into_node(new_node, next_byte, new_child);
                            }

                            let new_tagged = TaggedPtr::from_inner(new_node);
                            match parent {
                                Some(p) => unsafe {
                                    self.replace_child(p, parent_byte, new_tagged)
                                },
                                None => self.root.store(new_tagged.as_raw(), Ordering::Release),
                            }

                            header.latch.mark_obsolete_and_unlock();
                            match parent {
                                Some(p) => unsafe { (*p).latch.unlock() },
                                None => self.root_latch.unlock(),
                            }
                        } else {
                            if header.latch.lock_version(v_header).is_err() {
                                continue 'retry;
                            }

                            if is_node_full(header)
                                || unsafe { find_child(header, next_byte) }.is_some()
                            {
                                header.latch.unlock();
                                continue 'retry;
                            }

                            let new_child = self.create_single_leaf_chain(
                                &key_bytes[(depth + 1)..],
                                tagged_new_leaf,
                            );
                            unsafe {
                                self.insert_child_into_node(
                                    current.as_inner_ptr(),
                                    next_byte,
                                    new_child,
                                );
                            }
                            header.latch.unlock();
                        }

                        self.len.fetch_add(1, Ordering::Relaxed);
                        return true;
                    }
                    Some(child) => {
                        if child.is_leaf() {
                            if header.latch.lock_version(v_header).is_err() {
                                continue 'retry;
                            }
                            if unsafe { find_child(header, next_byte) } != Some(child) {
                                header.latch.unlock();
                                continue 'retry;
                            }

                            let existing_leaf_ptr = child.as_versioned_leaf_ptr::<K, V>();
                            let existing_leaf = unsafe { &*existing_leaf_ptr };

                            if existing_leaf.key.as_bytes() == key_bytes {
                                unsafe {
                                    (*new_leaf_ptr)
                                        .next_version
                                        .store(existing_leaf_ptr, Ordering::Relaxed);
                                    self.replace_child(
                                        current.as_inner_ptr(),
                                        next_byte,
                                        tagged_new_leaf,
                                    );
                                    header.latch.unlock();
                                }
                                return true;
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

                            unsafe {
                                self.replace_child(current.as_inner_ptr(), next_byte, new_inner)
                            };
                            self.len.fetch_add(1, Ordering::Relaxed);
                            header.latch.unlock();
                            return true;
                        } else {
                            if !header.latch.validate(v_header) {
                                continue 'retry;
                            }
                            parent = Some(current.as_inner_ptr());
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

    /// Marks the key as removed, returning true if found and marked.
    pub fn remove<Q>(&self, key: &Q, guard: &Guard) -> Option<V>
    where
        Q: AsBytes + ?Sized,
    {
        let leaf_ptr = self.get_leaf(key, guard)?;
        let leaf = unsafe { &*leaf_ptr };
        if leaf.removed.swap(true, Ordering::AcqRel) {
            None
        } else {
            self.len.fetch_sub(1, Ordering::Relaxed);
            Some((*leaf.value).clone())
        }
    }

    fn create_single_leaf_chain(&self, suffix: &[u8], leaf: TaggedPtr) -> TaggedPtr {
        if suffix.is_empty() {
            return leaf;
        }
        let take = suffix.len().min(MAX_PREFIX_LEN);
        let prefix = &suffix[..take];
        let remaining = &suffix[take..];

        let mut n4 = Node4::new(prefix);
        if remaining.is_empty() {
            n4.header.exact_leaf.store(leaf.as_raw(), Ordering::Relaxed);
        } else {
            let child_chain = self.create_single_leaf_chain(&remaining[1..], leaf);
            n4.insert_child(remaining[0], child_chain);
        }
        TaggedPtr::from_inner(Box::into_raw(n4) as *mut NodeHeader)
    }

    #[allow(clippy::too_many_arguments)]
    fn create_prefix_chain(
        &self,
        common_prefix: &[u8],
        exact1: bool,
        byte1: u8,
        child1: TaggedPtr,
        exact2: bool,
        byte2: u8,
        child2: TaggedPtr,
    ) -> TaggedPtr {
        if common_prefix.len() <= MAX_PREFIX_LEN {
            let mut n4 = Node4::new(common_prefix);
            if exact1 {
                n4.header
                    .exact_leaf
                    .store(child1.as_raw(), Ordering::Relaxed);
            } else {
                n4.insert_child(byte1, child1);
            }
            if exact2 {
                n4.header
                    .exact_leaf
                    .store(child2.as_raw(), Ordering::Relaxed);
            } else {
                n4.insert_child(byte2, child2);
            }
            TaggedPtr::from_inner(Box::into_raw(n4) as *mut NodeHeader)
        } else {
            let prefix = &common_prefix[..MAX_PREFIX_LEN];
            let remaining = &common_prefix[MAX_PREFIX_LEN..];

            let mut n4 = Node4::new(prefix);
            let next_byte = remaining[0];
            let child = self.create_prefix_chain(
                &remaining[1..],
                exact1,
                byte1,
                child1,
                exact2,
                byte2,
                child2,
            );
            n4.insert_child(next_byte, child);
            TaggedPtr::from_inner(Box::into_raw(n4) as *mut NodeHeader)
        }
    }

    unsafe fn replace_child(&self, header: *mut NodeHeader, byte: u8, new_child: TaggedPtr) {
        match (*header).node_type {
            NodeType::Node4 => {
                let n = &mut *(header as *mut Node4);
                for i in 0..n.header.num_children as usize {
                    if n.keys[i] == byte {
                        n.children[i].store(new_child.as_raw(), Ordering::Release);
                        return;
                    }
                }
            }
            NodeType::Node16 => {
                let n = &mut *(header as *mut Node16);
                for i in 0..n.header.num_children as usize {
                    if n.keys[i] == byte {
                        n.children[i].store(new_child.as_raw(), Ordering::Release);
                        return;
                    }
                }
            }
            NodeType::Node48 => {
                let n = &mut *(header as *mut Node48);
                let idx = n.child_indices[byte as usize];
                if idx < 48 {
                    n.children[idx as usize].store(new_child.as_raw(), Ordering::Release);
                }
            }
            NodeType::Node256 => {
                let n = &mut *(header as *mut Node256);
                n.children[byte as usize].store(new_child.as_raw(), Ordering::Release);
            }
        }
    }

    unsafe fn insert_child_into_node(&self, header: *mut NodeHeader, byte: u8, child: TaggedPtr) {
        match (*header).node_type {
            NodeType::Node4 => {
                let n = &mut *(header as *mut Node4);
                n.insert_child(byte, child);
            }
            NodeType::Node16 => {
                let n = &mut *(header as *mut Node16);
                n.insert_child(byte, child);
            }
            NodeType::Node48 => {
                let n = &mut *(header as *mut Node48);
                n.insert_child(byte, child);
            }
            NodeType::Node256 => {
                let n = &mut *(header as *mut Node256);
                n.insert_child(byte, child);
            }
        }
    }

    pub(crate) fn find_successor(
        &self,
        search_key: &[u8],
        include_equal: bool,
    ) -> Option<*mut VersionedLeaf<K, V>> {
        'retry: loop {
            let root_ptr = TaggedPtr::from_raw(self.root.load(Ordering::Acquire));
            if root_ptr.is_null() {
                return None;
            }

            match unsafe { self.find_successor_in_node(root_ptr, search_key, 0, include_equal) } {
                Ok(leaf) => return leaf,
                Err(()) => {
                    std::hint::spin_loop();
                    continue 'retry;
                }
            }
        }
    }

    pub(crate) fn find_predecessor(
        &self,
        search_key: &[u8],
        include_equal: bool,
    ) -> Option<*mut VersionedLeaf<K, V>> {
        'retry: loop {
            let root_ptr = TaggedPtr::from_raw(self.root.load(Ordering::Acquire));
            if root_ptr.is_null() {
                return None;
            }

            match unsafe { self.find_predecessor_in_node(root_ptr, search_key, 0, include_equal) } {
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
        ptr: TaggedPtr,
        search_key: &[u8],
        depth: usize,
        include_equal: bool,
    ) -> Result<Option<*mut VersionedLeaf<K, V>>, ()> {
        if ptr.is_leaf() {
            let leaf = &*ptr.as_versioned_leaf_ptr::<K, V>();
            let k = leaf.key.as_bytes();
            let cmp = k.cmp(search_key);
            if (include_equal && cmp >= std::cmp::Ordering::Equal)
                || (!include_equal && cmp == std::cmp::Ordering::Greater)
            {
                return Ok(Some(ptr.as_versioned_leaf_ptr()));
            } else {
                return Ok(None);
            }
        }

        let header = &*ptr.as_inner_ptr();
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
            return Ok(crate::versioned::iter::first_leaf_in_subtree(ptr));
        } else if prefix < remaining_key && !remaining_key.starts_with(prefix) {
            if !header.latch.validate(v) {
                return Err(());
            }
            return Ok(None);
        }

        let new_depth = depth + header.prefix_len as usize;

        if new_depth == search_key.len() {
            if include_equal {
                if let Some(leaf_ptr) = header.load_exact_versioned_leaf::<K, V>(Ordering::Acquire)
                {
                    if !header.latch.validate(v) {
                        return Err(());
                    }
                    return Ok(Some(leaf_ptr));
                }
            }

            if let Some(leaf) = find_first_child_leaf(header, 0) {
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

        if let Some(child) = find_child(header, next_byte) {
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
            if let Some(leaf) = find_first_child_leaf(header, next_byte + 1) {
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

    unsafe fn find_predecessor_in_node(
        &self,
        ptr: TaggedPtr,
        search_key: &[u8],
        depth: usize,
        include_equal: bool,
    ) -> Result<Option<*mut VersionedLeaf<K, V>>, ()> {
        if ptr.is_leaf() {
            let leaf = &*ptr.as_versioned_leaf_ptr::<K, V>();
            let k = leaf.key.as_bytes();
            let cmp = k.cmp(search_key);
            if (include_equal && cmp <= std::cmp::Ordering::Equal)
                || (!include_equal && cmp == std::cmp::Ordering::Less)
            {
                return Ok(Some(ptr.as_versioned_leaf_ptr()));
            } else {
                return Ok(None);
            }
        }

        let header = &*ptr.as_inner_ptr();
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
            return Ok(crate::versioned::iter::last_leaf_in_subtree(ptr));
        } else if prefix > remaining_key {
            if !header.latch.validate(v) {
                return Err(());
            }
            return Ok(None);
        }

        let new_depth = depth + header.prefix_len as usize;

        if new_depth >= search_key.len() {
            if include_equal {
                if let Some(leaf_ptr) = header.load_exact_versioned_leaf::<K, V>(Ordering::Acquire)
                {
                    if !header.latch.validate(v) {
                        return Err(());
                    }
                    return Ok(Some(leaf_ptr));
                }
            }
            if !header.latch.validate(v) {
                return Err(());
            }
            return Ok(None);
        }

        let next_byte = search_key[new_depth];

        if let Some(child) = find_child(header, next_byte) {
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
            if let Some(leaf) = find_last_child_leaf(header, next_byte - 1) {
                if !header.latch.validate(v) {
                    return Err(());
                }
                return Ok(Some(leaf));
            }
        }

        if let Some(leaf_ptr) = header.load_exact_versioned_leaf::<K, V>(Ordering::Acquire) {
            if !header.latch.validate(v) {
                return Err(());
            }
            return Ok(Some(leaf_ptr));
        }

        if !header.latch.validate(v) {
            return Err(());
        }
        Ok(None)
    }

    fn grow_node(&self, header: *mut NodeHeader, guard: &Guard) -> *mut NodeHeader {
        let node_type = unsafe { (*header).node_type };
        match node_type {
            NodeType::Node4 => {
                let n4 = unsafe { &*(header as *const Node4) };
                let mut n16 = Node16::new(n4.header.prefix_slice());
                n16.header.exact_leaf.store(
                    n4.header.exact_leaf.load(Ordering::Relaxed),
                    Ordering::Relaxed,
                );
                for i in 0..n4.header.num_children as usize {
                    let child = TaggedPtr::from_raw(n4.children[i].load(Ordering::Relaxed));
                    n16.insert_child(n4.keys[i], child);
                }
                let ptr = Box::into_raw(n16);
                let old_raw = header as usize;
                guard.defer(move || drop(unsafe { Box::from_raw(old_raw as *mut Node4) }));
                unsafe { &mut (*ptr).header }
            }
            NodeType::Node16 => {
                let n16 = unsafe { &*(header as *const Node16) };
                let mut n48 = Node48::new(n16.header.prefix_slice());
                n48.header.exact_leaf.store(
                    n16.header.exact_leaf.load(Ordering::Relaxed),
                    Ordering::Relaxed,
                );
                for i in 0..n16.header.num_children as usize {
                    let child = TaggedPtr::from_raw(n16.children[i].load(Ordering::Relaxed));
                    n48.insert_child(n16.keys[i], child);
                }
                let ptr = Box::into_raw(n48);
                let old_raw = header as usize;
                guard.defer(move || drop(unsafe { Box::from_raw(old_raw as *mut Node16) }));
                unsafe { &mut (*ptr).header }
            }
            NodeType::Node48 => {
                let n48 = unsafe { &*(header as *const Node48) };
                let mut n256 = Node256::new(n48.header.prefix_slice());
                n256.header.exact_leaf.store(
                    n48.header.exact_leaf.load(Ordering::Relaxed),
                    Ordering::Relaxed,
                );
                for byte in 0..=255u8 {
                    let idx = n48.child_indices[byte as usize];
                    if idx != NODE48_EMPTY {
                        let child =
                            TaggedPtr::from_raw(n48.children[idx as usize].load(Ordering::Relaxed));
                        n256.insert_child(byte, child);
                    }
                }
                let ptr = Box::into_raw(n256);
                let old_raw = header as usize;
                guard.defer(move || drop(unsafe { Box::from_raw(old_raw as *mut Node48) }));
                unsafe { &mut (*ptr).header }
            }
            NodeType::Node256 => unreachable!("Node256 is maximum node type"),
        }
    }
}

impl NodeHeader {
    fn set_prefix(&mut self, new_prefix: &[u8]) {
        let p_len = new_prefix.len().min(MAX_PREFIX_LEN);
        self.prefix[..p_len].copy_from_slice(&new_prefix[..p_len]);
        self.prefix_len = new_prefix.len() as u16;
    }
}

unsafe fn find_first_child_leaf<K, V>(
    header: &NodeHeader,
    min_byte: u8,
) -> Option<*mut VersionedLeaf<K, V>> {
    match header.node_type {
        NodeType::Node4 => {
            let n = &*(header as *const NodeHeader as *const Node4);
            for i in 0..n.header.num_children as usize {
                if n.keys[i] >= min_byte {
                    let child = TaggedPtr::from_raw(n.children[i].load(Ordering::Acquire));
                    if let Some(leaf) = crate::versioned::iter::first_leaf_in_subtree(child) {
                        return Some(leaf);
                    }
                }
            }
            None
        }
        NodeType::Node16 => {
            let n = &*(header as *const NodeHeader as *const Node16);
            for i in 0..n.header.num_children as usize {
                if n.keys[i] >= min_byte {
                    let child = TaggedPtr::from_raw(n.children[i].load(Ordering::Acquire));
                    if let Some(leaf) = crate::versioned::iter::first_leaf_in_subtree(child) {
                        return Some(leaf);
                    }
                }
            }
            None
        }
        NodeType::Node48 => {
            let n = &*(header as *const NodeHeader as *const Node48);
            for byte in min_byte..=255u8 {
                let slot = n.child_indices[byte as usize];
                if slot != NODE48_EMPTY {
                    let child =
                        TaggedPtr::from_raw(n.children[slot as usize].load(Ordering::Acquire));
                    if let Some(leaf) = crate::versioned::iter::first_leaf_in_subtree(child) {
                        return Some(leaf);
                    }
                }
            }
            None
        }
        NodeType::Node256 => {
            let n = &*(header as *const NodeHeader as *const Node256);
            for byte in min_byte..=255u8 {
                let child = TaggedPtr::from_raw(n.children[byte as usize].load(Ordering::Acquire));
                if !child.is_null() {
                    if let Some(leaf) = crate::versioned::iter::first_leaf_in_subtree(child) {
                        return Some(leaf);
                    }
                }
            }
            None
        }
    }
}

unsafe fn find_last_child_leaf<K, V>(
    header: &NodeHeader,
    max_byte: u8,
) -> Option<*mut VersionedLeaf<K, V>> {
    match header.node_type {
        NodeType::Node4 => {
            let n = &*(header as *const NodeHeader as *const Node4);
            for i in (0..n.header.num_children as usize).rev() {
                if n.keys[i] <= max_byte {
                    let child = TaggedPtr::from_raw(n.children[i].load(Ordering::Acquire));
                    if let Some(leaf) = crate::versioned::iter::last_leaf_in_subtree(child) {
                        return Some(leaf);
                    }
                }
            }
            None
        }
        NodeType::Node16 => {
            let n = &*(header as *const NodeHeader as *const Node16);
            for i in (0..n.header.num_children as usize).rev() {
                if n.keys[i] <= max_byte {
                    let child = TaggedPtr::from_raw(n.children[i].load(Ordering::Acquire));
                    if let Some(leaf) = crate::versioned::iter::last_leaf_in_subtree(child) {
                        return Some(leaf);
                    }
                }
            }
            None
        }
        NodeType::Node48 => {
            let n = &*(header as *const NodeHeader as *const Node48);
            for byte in (0..=max_byte).rev() {
                let slot = n.child_indices[byte as usize];
                if slot != NODE48_EMPTY {
                    let child =
                        TaggedPtr::from_raw(n.children[slot as usize].load(Ordering::Acquire));
                    if let Some(leaf) = crate::versioned::iter::last_leaf_in_subtree(child) {
                        return Some(leaf);
                    }
                }
            }
            None
        }
        NodeType::Node256 => {
            let n = &*(header as *const NodeHeader as *const Node256);
            for byte in (0..=max_byte).rev() {
                let child = TaggedPtr::from_raw(n.children[byte as usize].load(Ordering::Acquire));
                if !child.is_null() {
                    if let Some(leaf) = crate::versioned::iter::last_leaf_in_subtree(child) {
                        return Some(leaf);
                    }
                }
            }
            None
        }
    }
}

unsafe fn find_child(header: &NodeHeader, byte: u8) -> Option<TaggedPtr> {
    match header.node_type {
        NodeType::Node4 => (*(header as *const NodeHeader as *const Node4)).find_child(byte),
        NodeType::Node16 => (*(header as *const NodeHeader as *const Node16)).find_child(byte),
        NodeType::Node48 => (*(header as *const NodeHeader as *const Node48)).find_child(byte),
        NodeType::Node256 => (*(header as *const NodeHeader as *const Node256)).find_child(byte),
    }
}

#[inline]
fn is_node_full(header: &NodeHeader) -> bool {
    match header.node_type {
        NodeType::Node4 => header.num_children >= 4,
        NodeType::Node16 => header.num_children >= 16,
        NodeType::Node48 => header.num_children >= 48,
        NodeType::Node256 => false,
    }
}

#[inline]
fn longest_common_prefix(a: &[u8], b: &[u8]) -> usize {
    let max_len = a.len().min(b.len());
    let mut i = 0;
    while i < max_len && a[i] == b[i] {
        i += 1;
    }
    i
}
