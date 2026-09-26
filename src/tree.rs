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

//! # Core Concurrent Adaptive Radix Tree
//!
//! Implements lock-free optimistic search and fine-grained lock-coupling writes.

use crossbeam_epoch::Guard;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use crate::key::AsBytes;
use crate::latch::HybridLatch;
use crate::node::{
    Leaf, Node16, Node256, Node4, Node48, NodeHeader, NodeType, TaggedPtr, MAX_PREFIX_LEN,
    NODE48_EMPTY,
};

/// Internal concurrent tree structure.
pub struct Tree<K, V> {
    root: AtomicPtr<u8>,
    root_latch: HybridLatch,
    len: AtomicUsize,
    _marker: PhantomData<(K, V)>,
}

unsafe impl<K: Send + Sync, V: Send + Sync> Send for Tree<K, V> {}
unsafe impl<K: Send + Sync, V: Send + Sync> Sync for Tree<K, V> {}

impl<K, V> Default for Tree<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Drop for Tree<K, V> {
    fn drop(&mut self) {
        let root = TaggedPtr::from_raw(self.root.load(Ordering::Relaxed));
        if !root.is_null() {
            unsafe {
                drop_subtree::<K, V>(root);
            }
        }
    }
}

unsafe fn drop_subtree<K, V>(ptr: TaggedPtr) {
    if ptr.is_leaf() {
        let mut leaf = Box::from_raw(ptr.as_leaf_ptr::<K, V>());
        if !leaf.value_taken.load(Ordering::Acquire) {
            ManuallyDrop::drop(&mut leaf.value);
        }
    } else {
        let header = &*ptr.as_inner_ptr();
        if let Some(leaf_ptr) = header.load_exact_leaf::<K, V>(Ordering::Relaxed) {
            let mut leaf = Box::from_raw(leaf_ptr);
            if !leaf.value_taken.load(Ordering::Acquire) {
                ManuallyDrop::drop(&mut leaf.value);
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

impl<K, V> Tree<K, V> {
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
}

impl<K: AsBytes + Send + 'static, V: Send + 'static> Tree<K, V> {
    /// Optimistic non-blocking lookup returning a pointer to the leaf.
    pub fn get_leaf<Q>(&self, key: &Q, _guard: &Guard) -> Option<*mut Leaf<K, V>>
    where
        Q: AsBytes + ?Sized,
    {
        let key_bytes = key.as_bytes();

        'retry: loop {
            let mut current = TaggedPtr::from_raw(self.root.load(Ordering::Acquire));
            if current.is_null() {
                return None;
            }

            let mut depth = 0;
            let mut parent_latch: Option<(&HybridLatch, u64)> = None;

            while !current.is_null() {
                if current.is_leaf() {
                    if let Some((latch, v)) = parent_latch {
                        if !latch.validate(v) {
                            continue 'retry;
                        }
                    }
                    let leaf_ptr = current.as_leaf_ptr::<K, V>();
                    let leaf = unsafe { &*leaf_ptr };
                    if leaf.key.as_bytes() == key_bytes {
                        return Some(leaf_ptr);
                    } else {
                        return None;
                    }
                }

                let header = unsafe { &*current.as_inner_ptr() };
                let v = match header.latch.read_version() {
                    Some(v) => v,
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
                    let exact = header.load_exact_leaf::<K, V>(Ordering::Acquire);
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

            return None;
        }
    }

    /// Optimistic non-blocking lookup.
    pub fn get<'g, Q>(&self, key: &Q, guard: &'g Guard) -> Option<&'g V>
    where
        Q: AsBytes + ?Sized,
    {
        let leaf_ptr = self.get_leaf(key, guard)?;
        let leaf = unsafe { &*leaf_ptr };
        if leaf.removed.load(Ordering::Acquire) {
            return None;
        }
        Some(&leaf.value)
    }

    /// Inserts or updates a key-value pair.
    pub fn insert(&self, key: K, value: V, guard: &Guard) -> Option<V> {
        self.insert_or_modify(key, value, true, guard)
            .map(|(old, _)| old)
            .unwrap_or(None)
    }

    /// Inserts a key-value pair if absent, or returns a pointer to the existing leaf.
    pub fn get_or_insert_with<F>(&self, key: K, f: F, guard: &Guard) -> *mut Leaf<K, V>
    where
        F: FnOnce() -> V,
    {
        if let Some(leaf_ptr) = self.get_leaf(&key, guard) {
            let leaf = unsafe { &*leaf_ptr };
            if !leaf.removed.load(Ordering::Acquire) {
                return leaf_ptr;
            }
        }

        let value = f();
        let (_, leaf_ptr) = self
            .insert_or_modify(key, value, false, guard)
            .expect("insert_or_modify must return leaf pointer");

        leaf_ptr
    }

    fn insert_or_modify(
        &self,
        key: K,
        value: V,
        replace_if_present: bool,
        guard: &Guard,
    ) -> Result<(Option<V>, *mut Leaf<K, V>), ()> {
        let new_leaf_box = Leaf::new(key, value);
        let new_leaf_ptr = Box::into_raw(new_leaf_box);
        let tagged_new_leaf = TaggedPtr::from_leaf(new_leaf_ptr);
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
                    return Ok((None, new_leaf_ptr));
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

                let existing_leaf = unsafe { &mut *cur_root.as_leaf_ptr::<K, V>() };
                if existing_leaf.key.as_bytes() == key_bytes {
                    let mut new_leaf = unsafe { Box::from_raw(new_leaf_ptr) };
                    if replace_if_present {
                        let old_val = unsafe { ManuallyDrop::take(&mut existing_leaf.value) };
                        existing_leaf.value =
                            ManuallyDrop::new(unsafe { ManuallyDrop::take(&mut new_leaf.value) });
                        self.root_latch.unlock();
                        return Ok((Some(old_val), cur_root.as_leaf_ptr::<K, V>()));
                    } else {
                        unsafe { ManuallyDrop::drop(&mut new_leaf.value) };
                        self.root_latch.unlock();
                        return Ok((None, cur_root.as_leaf_ptr::<K, V>()));
                    }
                }

                let existing_key = existing_leaf.key.as_bytes();
                let common_len = longest_common_prefix(existing_key, key_bytes);

                let exact1 = existing_key.len() == common_len;
                let byte1 = if !exact1 { existing_key[common_len] } else { 0 };

                let exact2 = key_bytes.len() == common_len;
                let byte2 = if !exact2 { key_bytes[common_len] } else { 0 };

                let new_root = create_prefix_chain(
                    &key_bytes[..common_len],
                    exact1,
                    byte1,
                    cur_root,
                    exact2,
                    byte2,
                    tagged_new_leaf,
                );
                self.root.store(new_root.as_raw(), Ordering::Release);
                self.len.fetch_add(1, Ordering::Relaxed);
                self.root_latch.unlock();
                return Ok((None, new_leaf_ptr));
            }

            // Case 2: Root is an InnerNode
            let mut parent: Option<*mut NodeHeader> = None;
            let mut parent_byte = 0u8;
            let mut current = root_ptr;
            let mut depth = 0;

            'traverse: loop {
                let header = unsafe { &mut *current.as_inner_ptr() };
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

                    let cur_prefix_len = (header.prefix_len as usize).min(MAX_PREFIX_LEN);
                    let mut cur_prefix_buf = [0u8; MAX_PREFIX_LEN];
                    cur_prefix_buf[..cur_prefix_len].copy_from_slice(header.prefix_slice());
                    let cur_prefix = &cur_prefix_buf[..cur_prefix_len];

                    let mismatch_char_existing = cur_prefix[matched];

                    let mut split_node = Node4::new(&cur_prefix[..matched]);

                    let remaining_prefix = &cur_prefix[(matched + 1)..];
                    let mut new_p = [0u8; MAX_PREFIX_LEN];
                    let new_p_len = remaining_prefix.len().min(MAX_PREFIX_LEN);
                    new_p[..new_p_len].copy_from_slice(&remaining_prefix[..new_p_len]);
                    header.prefix = new_p;
                    header.prefix_len = remaining_prefix.len() as u16;

                    split_node.insert_child(mismatch_char_existing, TaggedPtr::from_inner(header));

                    if depth + matched == key_bytes.len() {
                        split_node
                            .header
                            .exact_leaf
                            .store(tagged_new_leaf.as_raw(), Ordering::Relaxed);
                    } else {
                        let new_char = key_bytes[depth + matched];
                        split_node.insert_child(new_char, tagged_new_leaf);
                    }

                    let split_ptr = Box::into_raw(split_node);
                    let tagged_split =
                        TaggedPtr::from_inner(&mut unsafe { &mut *split_ptr }.header);

                    match parent {
                        Some(p) => unsafe { replace_child(p, parent_byte, tagged_split) },
                        None => self.root.store(tagged_split.as_raw(), Ordering::Release),
                    }

                    self.len.fetch_add(1, Ordering::Relaxed);
                    header.latch.unlock();
                    match parent {
                        Some(p) => unsafe { (*p).latch.unlock() },
                        None => self.root_latch.unlock(),
                    }
                    return Ok((None, new_leaf_ptr));
                }

                depth += header.prefix_len as usize;

                // Exact key match at this inner node
                if depth == key_bytes.len() {
                    if header.latch.lock_version(v_header).is_err() {
                        continue 'retry;
                    }

                    if let Some(leaf_ptr) = header.load_exact_leaf::<K, V>(Ordering::Acquire) {
                        let existing_leaf = unsafe { &mut *leaf_ptr };
                        let mut new_leaf = unsafe { Box::from_raw(new_leaf_ptr) };
                        if replace_if_present {
                            let old_val = unsafe { ManuallyDrop::take(&mut existing_leaf.value) };
                            existing_leaf.value = ManuallyDrop::new(unsafe {
                                ManuallyDrop::take(&mut new_leaf.value)
                            });
                            header.latch.unlock();
                            return Ok((Some(old_val), leaf_ptr));
                        } else {
                            unsafe { ManuallyDrop::drop(&mut new_leaf.value) };
                            header.latch.unlock();
                            return Ok((None, leaf_ptr));
                        }
                    } else {
                        header
                            .exact_leaf
                            .store(tagged_new_leaf.as_raw(), Ordering::Release);
                        self.len.fetch_add(1, Ordering::Relaxed);
                        header.latch.unlock();
                        return Ok((None, new_leaf_ptr));
                    }
                }

                let next_byte = key_bytes[depth];
                let next_child = unsafe { find_child(header, next_byte) };

                match next_child {
                    None => {
                        // Node256 lock-free atomic insertion fast-path
                        if header.node_type == NodeType::Node256 {
                            let n256 = unsafe { &mut *(header as *mut NodeHeader as *mut Node256) };
                            if n256.children[next_byte as usize]
                                .compare_exchange(
                                    ptr::null_mut(),
                                    tagged_new_leaf.as_raw(),
                                    Ordering::Release,
                                    Ordering::Acquire,
                                )
                                .is_ok()
                            {
                                if header.latch.validate(v_header) {
                                    n256.header.num_children += 1;
                                    self.len.fetch_add(1, Ordering::Relaxed);
                                    return Ok((None, new_leaf_ptr));
                                } else {
                                    n256.children[next_byte as usize]
                                        .store(ptr::null_mut(), Ordering::Release);
                                    continue 'retry;
                                }
                            }
                            continue 'retry;
                        }

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

                            if unsafe { find_child(header, next_byte) }.is_some() {
                                header.latch.unlock();
                                match parent {
                                    Some(p) => unsafe { (*p).latch.unlock() },
                                    None => self.root_latch.unlock(),
                                }
                                continue 'retry;
                            }

                            let new_node = unsafe { grow_node(header, guard) };
                            unsafe {
                                insert_child_into_node(new_node, next_byte, tagged_new_leaf);
                            }
                            let tagged_new_node = TaggedPtr::from_inner(new_node);

                            match parent {
                                Some(p) => unsafe {
                                    replace_child(p, parent_byte, tagged_new_node)
                                },
                                None => {
                                    self.root.store(tagged_new_node.as_raw(), Ordering::Release)
                                }
                            }

                            header.latch.mark_obsolete_and_unlock();
                            match parent {
                                Some(p) => unsafe { (*p).latch.unlock() },
                                None => self.root_latch.unlock(),
                            }
                        } else {
                            // Fast path: node has room. Lock header only (no parent or root latch)
                            if header.latch.lock_version(v_header).is_err() {
                                continue 'retry;
                            }

                            if is_node_full(header)
                                || unsafe { find_child(header, next_byte) }.is_some()
                            {
                                header.latch.unlock();
                                continue 'retry;
                            }

                            unsafe {
                                insert_child_into_node(header, next_byte, tagged_new_leaf);
                            }
                            header.latch.unlock();
                        }

                        self.len.fetch_add(1, Ordering::Relaxed);
                        return Ok((None, new_leaf_ptr));
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

                            let existing_leaf = unsafe { &mut *child.as_leaf_ptr::<K, V>() };
                            if existing_leaf.key.as_bytes() == key_bytes {
                                let mut new_leaf = unsafe { Box::from_raw(new_leaf_ptr) };
                                if replace_if_present {
                                    let old_val =
                                        unsafe { ManuallyDrop::take(&mut existing_leaf.value) };
                                    existing_leaf.value = ManuallyDrop::new(unsafe {
                                        ManuallyDrop::take(&mut new_leaf.value)
                                    });
                                    header.latch.unlock();
                                    return Ok((Some(old_val), child.as_leaf_ptr::<K, V>()));
                                } else {
                                    unsafe { ManuallyDrop::drop(&mut new_leaf.value) };
                                    header.latch.unlock();
                                    return Ok((None, child.as_leaf_ptr::<K, V>()));
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

                            let new_inner = create_prefix_chain(
                                &suffix_new[..common_len],
                                exact1,
                                byte1,
                                child,
                                exact2,
                                byte2,
                                tagged_new_leaf,
                            );

                            unsafe { replace_child(header, next_byte, new_inner) };
                            self.len.fetch_add(1, Ordering::Relaxed);
                            header.latch.unlock();
                            return Ok((None, new_leaf_ptr));
                        } else {
                            parent = Some(header);
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

    /// Removes a key from the tree, returning the removed value if found.
    pub fn remove<Q>(&self, key: &Q, guard: &Guard) -> Option<V>
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
                let _ = self.root_latch.lock();
                let cur_root = TaggedPtr::from_raw(self.root.load(Ordering::Relaxed));
                if !cur_root.is_leaf() {
                    self.root_latch.unlock();
                    continue 'retry;
                }
                let leaf_ptr = cur_root.as_leaf_ptr::<K, V>();
                let leaf = unsafe { &*leaf_ptr };
                if leaf.key.as_bytes() == key_bytes {
                    self.root.store(ptr::null_mut(), Ordering::Release);
                    self.len.fetch_sub(1, Ordering::Relaxed);
                    self.root_latch.unlock();
                    leaf.removed.store(true, Ordering::Release);
                    leaf.value_taken.store(true, Ordering::Release);
                    let val = unsafe { ManuallyDrop::take(&mut (*leaf_ptr).value) };
                    let raw_leaf = leaf_ptr as usize;
                    guard
                        .defer(move || unsafe { drop(Box::from_raw(raw_leaf as *mut Leaf<K, V>)) });
                    return Some(val);
                }
                self.root_latch.unlock();
                return None;
            }

            let mut current = root_ptr;
            let mut depth = 0;

            'traverse: loop {
                let header = unsafe { &mut *current.as_inner_ptr() };
                let (_matched, is_full) = header.match_prefix(key_bytes, depth);
                if !is_full {
                    return None;
                }

                depth += header.prefix_len as usize;

                if depth == key_bytes.len() {
                    if header.latch.lock().is_err() {
                        continue 'retry;
                    }

                    if let Some(leaf_ptr) = header.load_exact_leaf::<K, V>(Ordering::Acquire) {
                        let leaf = unsafe { &*leaf_ptr };
                        if leaf.key.as_bytes() == key_bytes {
                            header.exact_leaf.store(ptr::null_mut(), Ordering::Release);
                            self.len.fetch_sub(1, Ordering::Relaxed);
                            header.latch.unlock();
                            leaf.removed.store(true, Ordering::Release);
                            leaf.value_taken.store(true, Ordering::Release);
                            let val = unsafe { ManuallyDrop::take(&mut (*leaf_ptr).value) };
                            let raw_leaf = leaf_ptr as usize;
                            guard.defer(move || unsafe {
                                drop(Box::from_raw(raw_leaf as *mut Leaf<K, V>))
                            });
                            return Some(val);
                        }
                    }
                    header.latch.unlock();
                    return None;
                }

                let next_byte = key_bytes[depth];
                let child = unsafe { find_child(header, next_byte) }?;

                if child.is_leaf() {
                    if header.latch.lock().is_err() {
                        continue 'retry;
                    }
                    if unsafe { find_child(header, next_byte) } != Some(child) {
                        header.latch.unlock();
                        continue 'retry;
                    }

                    let leaf_ptr = child.as_leaf_ptr::<K, V>();
                    let leaf = unsafe { &*leaf_ptr };
                    if leaf.key.as_bytes() == key_bytes {
                        unsafe { remove_child_from_node(header, next_byte) };
                        self.len.fetch_sub(1, Ordering::Relaxed);
                        header.latch.unlock();
                        leaf.removed.store(true, Ordering::Release);
                        leaf.value_taken.store(true, Ordering::Release);
                        let val = unsafe { ManuallyDrop::take(&mut (*leaf_ptr).value) };
                        let raw_leaf = leaf_ptr as usize;
                        guard.defer(move || unsafe {
                            drop(Box::from_raw(raw_leaf as *mut Leaf<K, V>))
                        });
                        return Some(val);
                    }
                    header.latch.unlock();
                    return None;
                } else {
                    current = child;
                    depth += 1;
                    continue 'traverse;
                }
            }
        }
    }

    /// Removes a specific leaf from the tree, verifying identity by pointer equality.
    pub(crate) fn remove_leaf(&self, leaf_ptr: *mut Leaf<K, V>, guard: &Guard) -> bool {
        if leaf_ptr.is_null() {
            return false;
        }
        let leaf = unsafe { &*leaf_ptr };
        if leaf.removed.load(Ordering::Acquire) {
            return false;
        }

        let key_bytes = leaf.key.as_bytes();

        'retry: loop {
            let root_ptr = TaggedPtr::from_raw(self.root.load(Ordering::Acquire));
            if root_ptr.is_null() {
                leaf.removed.store(true, Ordering::Release);
                return false;
            }

            if root_ptr.is_leaf() {
                let _ = self.root_latch.lock();
                let cur_root = TaggedPtr::from_raw(self.root.load(Ordering::Relaxed));
                if !cur_root.is_leaf() {
                    self.root_latch.unlock();
                    continue 'retry;
                }
                if cur_root.as_leaf_ptr::<K, V>() != leaf_ptr {
                    self.root_latch.unlock();
                    leaf.removed.store(true, Ordering::Release);
                    return false;
                }
                self.root.store(ptr::null_mut(), Ordering::Release);
                self.len.fetch_sub(1, Ordering::Relaxed);
                self.root_latch.unlock();
                leaf.removed.store(true, Ordering::Release);
                let raw = leaf_ptr as usize;
                guard.defer(move || unsafe {
                    let mut leaf = Box::from_raw(raw as *mut Leaf<K, V>);
                    if !leaf.value_taken.load(Ordering::Acquire) {
                        ManuallyDrop::drop(&mut leaf.value);
                    }
                });
                return true;
            }

            let mut current = root_ptr;
            let mut depth = 0;

            'traverse: loop {
                let header = unsafe { &mut *current.as_inner_ptr() };
                let (_matched, is_full) = header.match_prefix(key_bytes, depth);
                if !is_full {
                    leaf.removed.store(true, Ordering::Release);
                    return false;
                }

                depth += header.prefix_len as usize;

                if depth == key_bytes.len() {
                    if header.latch.lock().is_err() {
                        continue 'retry;
                    }

                    if let Some(cur_leaf_ptr) = header.load_exact_leaf::<K, V>(Ordering::Acquire) {
                        if cur_leaf_ptr == leaf_ptr {
                            header.exact_leaf.store(ptr::null_mut(), Ordering::Release);
                            self.len.fetch_sub(1, Ordering::Relaxed);
                            header.latch.unlock();
                            leaf.removed.store(true, Ordering::Release);
                            let raw = leaf_ptr as usize;
                            guard.defer(move || unsafe {
                                let mut leaf = Box::from_raw(raw as *mut Leaf<K, V>);
                                if !leaf.value_taken.load(Ordering::Acquire) {
                                    ManuallyDrop::drop(&mut leaf.value);
                                }
                            });
                            return true;
                        }
                    }
                    header.latch.unlock();
                    leaf.removed.store(true, Ordering::Release);
                    return false;
                }

                let next_byte = key_bytes[depth];
                let child = match unsafe { find_child(header, next_byte) } {
                    Some(c) => c,
                    None => {
                        leaf.removed.store(true, Ordering::Release);
                        return false;
                    }
                };

                if child.is_leaf() {
                    if header.latch.lock().is_err() {
                        continue 'retry;
                    }
                    if unsafe { find_child(header, next_byte) } != Some(child) {
                        header.latch.unlock();
                        continue 'retry;
                    }

                    if child.as_leaf_ptr::<K, V>() == leaf_ptr {
                        unsafe { remove_child_from_node(header, next_byte) };
                        self.len.fetch_sub(1, Ordering::Relaxed);
                        header.latch.unlock();
                        leaf.removed.store(true, Ordering::Release);
                        let raw = leaf_ptr as usize;
                        guard.defer(move || unsafe {
                            let mut leaf = Box::from_raw(raw as *mut Leaf<K, V>);
                            if !leaf.value_taken.load(Ordering::Acquire) {
                                ManuallyDrop::drop(&mut leaf.value);
                            }
                        });
                        return true;
                    }
                    header.latch.unlock();
                    leaf.removed.store(true, Ordering::Release);
                    return false;
                } else {
                    current = child;
                    depth += 1;
                    continue 'traverse;
                }
            }
        }
    }

    /// Clears all entries from the tree.
    pub fn clear(&self) {
        let _ = self.root_latch.lock();
        let old_root = TaggedPtr::from_raw(self.root.swap(ptr::null_mut(), Ordering::SeqCst));
        self.len.store(0, Ordering::Release);
        self.root_latch.unlock();

        if !old_root.is_null() {
            let guard = &crossbeam_epoch::pin();
            let raw_root = old_root.as_raw() as usize;
            guard.defer(move || unsafe {
                drop_subtree::<K, V>(TaggedPtr::from_raw(raw_root as *mut u8));
            });
        }
    }

    /// Validates structural invariants across the entire tree.
    pub fn validate_invariants(&self) {
        let root = TaggedPtr::from_raw(self.root.load(Ordering::SeqCst));
        if root.is_null() {
            assert_eq!(self.len(), 0);
            return;
        }

        let count = unsafe { validate_node_invariants::<K, V>(root, &[]) };
        assert_eq!(
            count,
            self.len(),
            "tree len must equal reachable leaf count"
        );
    }

    pub(crate) fn find_successor(
        &self,
        search_key: &[u8],
        include_equal: bool,
    ) -> Option<*mut Leaf<K, V>> {
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
    ) -> Option<*mut Leaf<K, V>> {
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
    ) -> Result<Option<*mut Leaf<K, V>>, ()> {
        if ptr.is_leaf() {
            let leaf = &*ptr.as_leaf_ptr::<K, V>();
            let k = leaf.key.as_bytes();
            let cmp = k.cmp(search_key);
            if (include_equal && cmp >= std::cmp::Ordering::Equal)
                || (!include_equal && cmp == std::cmp::Ordering::Greater)
            {
                return Ok(Some(ptr.as_leaf_ptr()));
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
            return Ok(crate::iter::first_leaf_in_subtree(ptr));
        } else if prefix < remaining_key && !remaining_key.starts_with(prefix) {
            if !header.latch.validate(v) {
                return Err(());
            }
            return Ok(None);
        }

        let new_depth = depth + header.prefix_len as usize;

        if new_depth == search_key.len() {
            if include_equal {
                if let Some(leaf_ptr) = header.load_exact_leaf::<K, V>(Ordering::Acquire) {
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
    ) -> Result<Option<*mut Leaf<K, V>>, ()> {
        if ptr.is_leaf() {
            let leaf = &*ptr.as_leaf_ptr::<K, V>();
            let k = leaf.key.as_bytes();
            let cmp = k.cmp(search_key);
            if (include_equal && cmp <= std::cmp::Ordering::Equal)
                || (!include_equal && cmp == std::cmp::Ordering::Less)
            {
                return Ok(Some(ptr.as_leaf_ptr()));
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
            return Ok(crate::iter::last_leaf_in_subtree(ptr));
        } else if prefix > remaining_key {
            if !header.latch.validate(v) {
                return Err(());
            }
            return Ok(None);
        }

        let new_depth = depth + header.prefix_len as usize;

        if new_depth >= search_key.len() {
            if include_equal {
                if let Some(leaf_ptr) = header.load_exact_leaf::<K, V>(Ordering::Acquire) {
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

        if let Some(leaf_ptr) = header.load_exact_leaf::<K, V>(Ordering::Acquire) {
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
}

fn create_prefix_chain(
    prefix: &[u8],
    exact1: bool,
    byte1: u8,
    child1: TaggedPtr,
    exact2: bool,
    byte2: u8,
    child2: TaggedPtr,
) -> TaggedPtr {
    if prefix.len() <= MAX_PREFIX_LEN {
        let mut n4 = Node4::new(prefix);
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
        let ptr = Box::into_raw(n4);
        TaggedPtr::from_inner(&mut unsafe { &mut *ptr }.header)
    } else {
        let mut n4 = Node4::new(&prefix[..MAX_PREFIX_LEN]);
        let next_child = create_prefix_chain(
            &prefix[(MAX_PREFIX_LEN + 1)..],
            exact1,
            byte1,
            child1,
            exact2,
            byte2,
            child2,
        );
        n4.insert_child(prefix[MAX_PREFIX_LEN], next_child);
        let ptr = Box::into_raw(n4);
        TaggedPtr::from_inner(&mut unsafe { &mut *ptr }.header)
    }
}

unsafe fn validate_node_invariants<K: AsBytes, V>(ptr: TaggedPtr, current_prefix: &[u8]) -> usize {
    if ptr.is_leaf() {
        let leaf = &*ptr.as_leaf_ptr::<K, V>();
        let k = leaf.key.as_bytes();
        assert!(
            k.starts_with(current_prefix),
            "leaf key must start with accumulated prefix"
        );
        return 1;
    }

    let header = &*ptr.as_inner_ptr();
    assert!(
        !header.latch.is_obsolete(),
        "live node must not be obsolete"
    );

    let mut prefix_buf = current_prefix.to_vec();
    prefix_buf.extend_from_slice(header.prefix_slice());

    let mut count = 0;
    if let Some(leaf_ptr) = header.load_exact_leaf::<K, V>(Ordering::SeqCst) {
        let leaf = &*leaf_ptr;
        assert_eq!(leaf.key.as_bytes(), prefix_buf.as_slice());
        count += 1;
    }

    match header.node_type {
        NodeType::Node4 => {
            let n = &*(ptr.as_inner_ptr() as *const Node4);
            assert!(n.header.num_children <= 4);
            for i in 0..n.header.num_children as usize {
                if i > 0 {
                    assert!(
                        n.keys[i - 1] < n.keys[i],
                        "Node4 keys must be strictly sorted"
                    );
                }
                let child = TaggedPtr::from_raw(n.children[i].load(Ordering::SeqCst));
                assert!(!child.is_null());
                let mut child_p = prefix_buf.clone();
                child_p.push(n.keys[i]);
                count += validate_node_invariants::<K, V>(child, &child_p);
            }
        }
        NodeType::Node16 => {
            let n = &*(ptr.as_inner_ptr() as *const Node16);
            assert!(n.header.num_children <= 16);
            for i in 0..n.header.num_children as usize {
                if i > 0 {
                    assert!(
                        n.keys[i - 1] < n.keys[i],
                        "Node16 keys must be strictly sorted"
                    );
                }
                let child = TaggedPtr::from_raw(n.children[i].load(Ordering::SeqCst));
                assert!(!child.is_null());
                let mut child_p = prefix_buf.clone();
                child_p.push(n.keys[i]);
                count += validate_node_invariants::<K, V>(child, &child_p);
            }
        }
        NodeType::Node48 => {
            let n = &*(ptr.as_inner_ptr() as *const Node48);
            assert!(n.header.num_children <= 48);
            for byte in 0..=255u8 {
                let slot = n.child_indices[byte as usize];
                if slot != NODE48_EMPTY {
                    let child =
                        TaggedPtr::from_raw(n.children[slot as usize].load(Ordering::SeqCst));
                    assert!(!child.is_null());
                    let mut child_p = prefix_buf.clone();
                    child_p.push(byte);
                    count += validate_node_invariants::<K, V>(child, &child_p);
                }
            }
        }
        NodeType::Node256 => {
            let n = &*(ptr.as_inner_ptr() as *const Node256);
            for byte in 0..=255u8 {
                let child = TaggedPtr::from_raw(n.children[byte as usize].load(Ordering::SeqCst));
                if !child.is_null() {
                    let mut child_p = prefix_buf.clone();
                    child_p.push(byte);
                    count += validate_node_invariants::<K, V>(child, &child_p);
                }
            }
        }
    }

    count
}

#[inline]
fn longest_common_prefix(a: &[u8], b: &[u8]) -> usize {
    let len = a.len().min(b.len());
    let mut i = 0;
    while i < len && a[i] == b[i] {
        i += 1;
    }
    i
}

#[inline]
unsafe fn find_child(header: &NodeHeader, byte: u8) -> Option<TaggedPtr> {
    match header.node_type {
        NodeType::Node4 => (*(header as *const NodeHeader as *const Node4)).find_child(byte),
        NodeType::Node16 => (*(header as *const NodeHeader as *const Node16)).find_child(byte),
        NodeType::Node48 => (*(header as *const NodeHeader as *const Node48)).find_child(byte),
        NodeType::Node256 => (*(header as *const NodeHeader as *const Node256)).find_child(byte),
    }
}

#[inline]
unsafe fn insert_child_into_node(header: *mut NodeHeader, byte: u8, child: TaggedPtr) {
    match (*header).node_type {
        NodeType::Node4 => (*(header as *mut Node4)).insert_child(byte, child),
        NodeType::Node16 => (*(header as *mut Node16)).insert_child(byte, child),
        NodeType::Node48 => (*(header as *mut Node48)).insert_child(byte, child),
        NodeType::Node256 => (*(header as *mut Node256)).insert_child(byte, child),
    }
}

#[inline]
unsafe fn remove_child_from_node(header: *mut NodeHeader, byte: u8) -> Option<TaggedPtr> {
    match (*header).node_type {
        NodeType::Node4 => (*(header as *mut Node4)).remove_child(byte),
        NodeType::Node16 => (*(header as *mut Node16)).remove_child(byte),
        NodeType::Node48 => (*(header as *mut Node48)).remove_child(byte),
        NodeType::Node256 => (*(header as *mut Node256)).remove_child(byte),
    }
}

#[inline]
unsafe fn replace_child(header: *mut NodeHeader, byte: u8, new_child: TaggedPtr) {
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
            let slot = n.child_indices[byte as usize];
            if slot != NODE48_EMPTY {
                n.children[slot as usize].store(new_child.as_raw(), Ordering::Release);
            }
        }
        NodeType::Node256 => {
            let n = &mut *(header as *mut Node256);
            n.children[byte as usize].store(new_child.as_raw(), Ordering::Release);
        }
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

unsafe fn grow_node(header: *mut NodeHeader, guard: &Guard) -> *mut NodeHeader {
    match (*header).node_type {
        NodeType::Node4 => {
            let old = &*(header as *const Node4);
            let mut n16 = Node16::new(old.header.prefix_slice());
            n16.header.exact_leaf.store(
                old.header.exact_leaf.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            for i in 0..old.header.num_children as usize {
                let child = TaggedPtr::from_raw(old.children[i].load(Ordering::Relaxed));
                n16.insert_child(old.keys[i], child);
            }
            let ptr = Box::into_raw(n16);
            let old_raw = header as usize;
            guard.defer(move || drop(Box::from_raw(old_raw as *mut Node4)));
            &mut (*ptr).header
        }
        NodeType::Node16 => {
            let old = &*(header as *const Node16);
            let mut n48 = Node48::new(old.header.prefix_slice());
            n48.header.exact_leaf.store(
                old.header.exact_leaf.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            for i in 0..old.header.num_children as usize {
                let child = TaggedPtr::from_raw(old.children[i].load(Ordering::Relaxed));
                n48.insert_child(old.keys[i], child);
            }
            let ptr = Box::into_raw(n48);
            let old_raw = header as usize;
            guard.defer(move || drop(Box::from_raw(old_raw as *mut Node16)));
            &mut (*ptr).header
        }
        NodeType::Node48 => {
            let old = &*(header as *const Node48);
            let mut n256 = Node256::new(old.header.prefix_slice());
            n256.header.exact_leaf.store(
                old.header.exact_leaf.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            for byte in 0..=255u8 {
                let slot = old.child_indices[byte as usize];
                if slot != NODE48_EMPTY {
                    let child =
                        TaggedPtr::from_raw(old.children[slot as usize].load(Ordering::Relaxed));
                    n256.insert_child(byte, child);
                }
            }
            let ptr = Box::into_raw(n256);
            let old_raw = header as usize;
            guard.defer(move || drop(Box::from_raw(old_raw as *mut Node48)));
            &mut (*ptr).header
        }
        NodeType::Node256 => unreachable!("Node256 cannot grow"),
    }
}

unsafe fn find_first_child_leaf<K, V>(
    header: &NodeHeader,
    min_byte: u8,
) -> Option<*mut Leaf<K, V>> {
    match header.node_type {
        NodeType::Node4 => {
            let n = &*(header as *const NodeHeader as *const Node4);
            for i in 0..n.header.num_children as usize {
                if n.keys[i] >= min_byte {
                    let child = TaggedPtr::from_raw(n.children[i].load(Ordering::Acquire));
                    if let Some(leaf) = crate::iter::first_leaf_in_subtree(child) {
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
                    if let Some(leaf) = crate::iter::first_leaf_in_subtree(child) {
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
                    if let Some(leaf) = crate::iter::first_leaf_in_subtree(child) {
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
                    if let Some(leaf) = crate::iter::first_leaf_in_subtree(child) {
                        return Some(leaf);
                    }
                }
            }
            None
        }
    }
}

unsafe fn find_last_child_leaf<K, V>(header: &NodeHeader, max_byte: u8) -> Option<*mut Leaf<K, V>> {
    match header.node_type {
        NodeType::Node4 => {
            let n = &*(header as *const NodeHeader as *const Node4);
            for i in (0..n.header.num_children as usize).rev() {
                if n.keys[i] <= max_byte {
                    let child = TaggedPtr::from_raw(n.children[i].load(Ordering::Acquire));
                    if let Some(leaf) = crate::iter::last_leaf_in_subtree(child) {
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
                    if let Some(leaf) = crate::iter::last_leaf_in_subtree(child) {
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
                    if let Some(leaf) = crate::iter::last_leaf_in_subtree(child) {
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
                    if let Some(leaf) = crate::iter::last_leaf_in_subtree(child) {
                        return Some(leaf);
                    }
                }
            }
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tree_basic_crud() {
        let tree = Tree::<String, i32>::new();
        let guard = &crossbeam_epoch::pin();

        assert_eq!(tree.len(), 0);
        assert!(tree.is_empty());

        assert_eq!(tree.insert("apple".to_string(), 1, guard), None);
        assert_eq!(tree.insert("banana".to_string(), 2, guard), None);
        assert_eq!(tree.insert("cherry".to_string(), 3, guard), None);
        assert_eq!(tree.len(), 3);

        assert_eq!(tree.get("apple", guard), Some(&1));
        assert_eq!(tree.get("banana", guard), Some(&2));
        assert_eq!(tree.get("cherry", guard), Some(&3));
        assert_eq!(tree.get("durian", guard), None);

        // Update
        assert_eq!(tree.insert("apple".to_string(), 400, guard), Some(1));
        assert_eq!(tree.get("apple", guard), Some(&400));
        assert_eq!(tree.len(), 3);

        // Remove
        assert_eq!(tree.remove("banana", guard), Some(2));
        assert_eq!(tree.get("banana", guard), None);
        assert_eq!(tree.len(), 2);

        tree.validate_invariants();
    }

    #[test]
    fn test_tree_prefix_branching_and_growth() {
        let tree = Tree::<String, usize>::new();
        let guard = &crossbeam_epoch::pin();

        for i in 0..60 {
            let k = format!("prefix:{:02}", i);
            tree.insert(k, i, guard);
        }

        assert_eq!(tree.len(), 60);

        for i in 0..60 {
            let k = format!("prefix:{:02}", i);
            assert_eq!(tree.get(&k, guard), Some(&i));
        }

        tree.validate_invariants();
    }

    #[test]
    fn test_tree_concurrent_inserts() {
        use std::sync::Arc;

        let tree = Arc::new(Tree::<String, usize>::new());
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let tree = Arc::clone(&tree);
                std::thread::spawn(move || {
                    let guard = &crossbeam_epoch::pin();
                    for i in 0..500 {
                        let k = format!("worker:{:02}:key:{:04}", t, i);
                        tree.insert(k, t * 40000 + i, guard);
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }

        assert_eq!(tree.len(), 4000);

        let guard = &crossbeam_epoch::pin();
        for t in 0..8 {
            for i in 0..500 {
                let k = format!("worker:{:02}:key:{:04}", t, i);
                assert_eq!(tree.get(&k, guard), Some(&(t * 40000 + i)));
            }
        }

        tree.validate_invariants();
    }
}
