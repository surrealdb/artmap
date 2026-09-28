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

//! Loom models of the real latch, node and tree code (§16.3).
//!
//! Run with `RUSTFLAGS="--cfg loom" cargo test --lib --release loom_tests::`.
//! The tree models use a leak-only storage, so no epoch reclamation is
//! involved. Each fence model has a mutant twin that must fail.

#![allow(clippy::undocumented_unsafe_blocks)]

use std::convert::Infallible;
use std::marker::PhantomData;
use std::ptr::NonNull;

use loom::sync::Arc;
use loom::thread;

use crate::latch::{mutants, HybridLatch};
use crate::raw::node::{Node16, Node256, Node4, Node48, NodeHeader, NodeType};
use crate::raw::slot::TaggedPtr;
use crate::raw::{boxed, Layout, LeafNode, Mode, Outcome, RawTree, Storage, Unpublished};
use crate::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};

fn model(f: impl Fn() + Sync + Send + 'static) {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(3);
    b.check(f);
}

/// Runs both actors on spawned threads and joins them.
///
/// Every actor is spawned, never run on the model's main thread: with loom
/// 0.7.2, a main thread that only loads while a spawned thread loads and then
/// CASes the same word explores a single execution.
fn par(a: impl FnOnce() + Send + 'static, b: impl FnOnce() + Send + 'static) {
    let ta = thread::spawn(a);
    let tb = thread::spawn(b);
    ta.join().unwrap();
    tb.join().unwrap();
}

/// Runs `f` as a model with a mutant switched on, and asserts that loom finds
/// a failing interleaving.
fn assert_mutant_fails(flag: &'static std::thread::LocalKey<std::cell::Cell<bool>>, f: fn()) {
    flag.with(|m| m.set(true));
    let r = std::panic::catch_unwind(|| model(f));
    flag.with(|m| m.set(false));
    assert!(
        r.is_err(),
        "the mutant must be caught: the model does not explore"
    );
}

// ---------------------------------------------------------------------------
// Latch: the torn two-field read (W1, R5).

struct TwoFields {
    latch: HybridLatch,
    a: AtomicU64,
    b: AtomicU64,
}

fn torn_read_model() {
    let s = Arc::new(TwoFields {
        latch: HybridLatch::new(),
        a: AtomicU64::new(0),
        b: AtomicU64::new(0),
    });
    let (w, r) = (Arc::clone(&s), s);
    par(
        move || {
            let g = w.latch.lock().unwrap();
            w.a.store(1, Ordering::Relaxed);
            w.b.store(1, Ordering::Relaxed);
            drop(g);
        },
        move || {
            if let Some(v) = r.latch.read_version() {
                let a = r.a.load(Ordering::Relaxed);
                let b = r.b.load(Ordering::Relaxed);
                if r.latch.validate(v) {
                    assert_eq!(a, b, "a validated read saw a torn pair");
                }
            }
        },
    );
}

#[test]
fn latch_torn_read() {
    model(torn_read_model);
}

#[test]
fn latch_torn_read_mutant_without_w1_fence() {
    assert_mutant_fails(&mutants::SKIP_W1_FENCE, torn_read_model);
}

#[test]
fn latch_torn_read_mutant_without_r5_fence() {
    assert_mutant_fails(&mutants::SKIP_R5_FENCE, torn_read_model);
}

// ---------------------------------------------------------------------------
// Latch: mutual exclusion and the obsolete race.

#[test]
fn latch_two_writers() {
    model(|| {
        let s = Arc::new((HybridLatch::new(), AtomicU64::new(0)));
        let bump = |s: Arc<(HybridLatch, AtomicU64)>| {
            move || {
                let g = s.0.lock().unwrap();
                let v = s.1.load(Ordering::Relaxed);
                s.1.store(v + 1, Ordering::Relaxed);
                drop(g);
            }
        };
        par(bump(Arc::clone(&s)), bump(Arc::clone(&s)));
        assert_eq!(s.1.load(Ordering::Relaxed), 2);
    });
}

#[test]
fn latch_obsolete_race() {
    model(|| {
        let l = Arc::new(HybridLatch::new());
        let won = Arc::new(AtomicBool::new(false));
        let (l1, l2, w2) = (Arc::clone(&l), Arc::clone(&l), Arc::clone(&won));
        par(
            move || {
                if let Some(g) = l1.lock() {
                    g.mark_obsolete();
                }
            },
            move || {
                // Either it wins before the obsoletion, or it sees the latch
                // obsolete; it never deadlocks.
                if let Some(g) = l2.lock() {
                    w2.store(true, Ordering::Relaxed);
                    drop(g);
                }
            },
        );
        assert!(l.is_obsolete());
    });
}

// ---------------------------------------------------------------------------
// Node4 shifts against an optimistic reader (W3, R6).

type A = AtomicPtr<u8>;

fn dummy(n: usize) -> TaggedPtr {
    // Real, leaked allocations: the pointers are only compared.
    TaggedPtr::from_leaf(boxed(n as u64).as_ptr())
}

fn node4_shift_model(remove: bool) {
    let n = Arc::new({
        let mut n = Node4::<A>::new();
        n.header.push_child_unpublished(b'b', dummy(2));
        n.header.push_child_unpublished(b'c', dummy(3));
        if remove {
            n.header.push_child_unpublished(b'a', dummy(1));
        }
        n
    });
    let c = n.header.find_child(b'c').unwrap();
    let (w, r) = (Arc::clone(&n), n);
    par(
        move || {
            let g = w.header.latch.lock().unwrap();
            if remove {
                w.header.remove_child(&g, b'a');
            } else {
                w.header.insert_child(&g, b'a', dummy(1));
            }
            drop(g);
        },
        move || {
            if let Some(v) = r.header.latch.read_version() {
                let found = r.header.find_child(b'c');
                if r.header.latch.validate(v) {
                    // 'c' is present throughout: a validated read never
                    // misses it and never sees a null or foreign slot.
                    assert_eq!(found, Some(c));
                }
            }
        },
    );
}

#[test]
fn node4_insert_shift_against_reader() {
    model(|| node4_shift_model(false));
}

#[test]
fn node4_remove_shift_against_reader() {
    model(|| node4_shift_model(true));
}

// ---------------------------------------------------------------------------
// Whole-tree models over a leak-only storage.

struct TLeaf {
    removed: AtomicBool,
    key: Vec<u8>,
}

impl LeafNode for TLeaf {
    fn key_bytes(&self) -> &[u8] {
        &self.key
    }
    fn mark_removed(&self) {
        self.removed.store(true, Ordering::Release);
    }
}

struct Leak<L>(PhantomData<L>);

unsafe impl<L> Layout for Leak<L> {
    type Atomic = A;
    type Leaf = L;
    type Full = Infallible;
    unsafe fn node(&self, raw: TaggedPtr) -> NonNull<NodeHeader<A>> {
        unsafe { NonNull::new_unchecked(raw.as_inner_ptr()) }
    }
    unsafe fn leaf(&self, raw: TaggedPtr) -> NonNull<L> {
        unsafe { NonNull::new_unchecked(raw.as_leaf_ptr()) }
    }
    fn node_raw(&self, n: NonNull<NodeHeader<A>>) -> TaggedPtr {
        TaggedPtr::from_inner(n.as_ptr())
    }
    fn leaf_raw(&self, l: NonNull<L>) -> TaggedPtr {
        TaggedPtr::from_leaf(l.as_ptr())
    }
    fn alloc_node(&self, ty: NodeType) -> Result<NonNull<NodeHeader<A>>, Infallible> {
        Ok(match ty {
            NodeType::Node4 => boxed(Node4::<A>::new()).cast(),
            NodeType::Node16 => boxed(Node16::<A>::new()).cast(),
            NodeType::Node48 => boxed(Node48::<A>::new()).cast(),
            NodeType::Node256 => boxed(Node256::<A>::new()).cast(),
        })
    }
    unsafe fn free_node(&self, _n: NonNull<NodeHeader<A>>) {}
    unsafe fn free_leaf(&self, _l: NonNull<L>) {}
}

unsafe impl<L: LeafNode> Storage for Leak<L> {
    type Guard = ();
    unsafe fn retire_node(&self, _n: NonNull<NodeHeader<A>>, _g: &()) {}
    unsafe fn retire_leaf(&self, _l: NonNull<L>, _g: &()) {}
}

type T = RawTree<Leak<TLeaf>>;

fn tree(keys: &[&[u8]]) -> Arc<T> {
    let t = RawTree::new_in(Leak(PhantomData));
    for k in keys {
        insert(&t, k);
    }
    Arc::new(t)
}

fn insert(t: &T, k: &[u8]) -> bool {
    let leaf = boxed(TLeaf {
        removed: AtomicBool::new(false),
        key: k.to_vec(),
    });
    let owner = unsafe { Unpublished::new(&t.storage, leaf) };
    let key = unsafe { leaf.as_ref() }.key.clone();
    matches!(
        t.insert(&owner, &key, Mode::InsertIfAbsent, 1, &()),
        Ok(Outcome::Inserted(_))
    )
}

fn remove(t: &T, k: &[u8]) -> bool {
    t.remove(k, |l| unsafe { l.as_ref() }.key == k, &())
        .is_some()
}

fn quiescent_check(t: Arc<T>) -> usize {
    let mut t = Arc::try_unwrap(t).ok().expect("threads joined");
    t.validate()
}

#[test]
fn prefix_split_against_get() {
    // Root node with prefix "abc"; inserting "abZ" splits it in place while a
    // reader looks up a key below it (R4 coupling, root re-check).
    model(|| {
        let t = tree(&[b"abcX", b"abcY"]);
        let (w, r) = (Arc::clone(&t), Arc::clone(&t));
        par(
            move || assert!(insert(&w, b"abZ")),
            move || {
                assert!(
                    r.get(b"abcX").is_some(),
                    "false negative during a prefix split"
                )
            },
        );
        assert_eq!(quiescent_check(t), 3);
    });
}

fn inner_prefix_split_model() {
    let t = tree(&[b"a1", b"a2", b"bxyzP", b"bxyzQ"]);
    let (w, r) = (Arc::clone(&t), Arc::clone(&t));
    par(
        move || assert!(insert(&w, b"bxQ")),
        move || {
            assert!(
                r.get(b"bxyzP").is_some(),
                "false negative below an inner split"
            )
        },
    );
    assert_eq!(quiescent_check(t), 5);
}

#[test]
fn inner_prefix_split_against_get() {
    model(inner_prefix_split_model);
}

#[test]
fn inner_prefix_split_mutant_without_coupling() {
    // Without R4 the reader arrives at the split node with a stale depth.
    assert_mutant_fails(&mutants::SKIP_R4_COUPLING, inner_prefix_split_model);
}

#[test]
fn grow_against_get() {
    model(|| {
        let t = tree(&[b"a", b"b", b"c", b"d"]);
        let (w, r) = (Arc::clone(&t), Arc::clone(&t));
        par(
            move || assert!(insert(&w, b"e")),
            move || assert!(r.get(b"c").is_some(), "false negative during growth"),
        );
        assert_eq!(quiescent_check(t), 5);
    });
}

#[test]
fn insert_against_remove_same_node() {
    model(|| {
        let t = tree(&[b"ka", b"kb"]);
        let (w, r) = (Arc::clone(&t), Arc::clone(&t));
        par(
            move || assert!(insert(&w, b"kc")),
            move || assert!(remove(&r, b"ka")),
        );
        assert_eq!(quiescent_check(t), 2);
    });
}

#[test]
fn clear_against_inserter_keeps_len_exact() {
    // §9.8: the mutant `len.store(0)` would fail this model.
    model(|| {
        let t = tree(&[b"ka", b"kb"]);
        let (w, c) = (Arc::clone(&t), Arc::clone(&t));
        par(
            move || {
                insert(&w, b"kc");
            },
            move || c.clear(&()),
        );
        // `validate` asserts that len equals the reachable count.
        assert!(quiescent_check(t) <= 1);
    });
}

#[test]
fn clear_against_remover_never_underflows() {
    model(|| {
        let t = tree(&[b"ka", b"kb"]);
        let (w, c) = (Arc::clone(&t), Arc::clone(&t));
        par(
            move || {
                remove(&w, b"ka");
                assert!(w.raw_len() >= 0, "len went negative");
            },
            move || {
                c.clear(&());
                assert!(c.raw_len() >= 0, "len went negative");
            },
        );
        assert_eq!(quiescent_check(t), 0);
    });
}

// ---------------------------------------------------------------------------
// Version chains (§11.4, §12.7), on the arena map: the heap chain runs the
// same protocol, with EBR in place of the arena's retired list.

type VMap = crate::arena::ArenaVersionedArtMap<Vec<u8>, u64>;

/// A map holding `k` at version 2 (in the leaf's inline slot).
fn vmap() -> Arc<VMap> {
    let m = VMap::with_capacity(1 << 16);
    m.insert(b"k".to_vec(), 2, 20);
    Arc::new(m)
}

fn two_prepends_model() {
    model(|| {
        let m = vmap();
        let (a, b) = (Arc::clone(&m), Arc::clone(&m));
        par(
            move || {
                a.insert(b"k".to_vec(), 3, 30);
            },
            move || {
                b.insert(b"k".to_vec(), 4, 40);
            },
        );
        assert_eq!(
            m.get_all_versions(&b"k"[..]),
            vec![(4, Some(40)), (3, Some(30)), (2, Some(20))],
            "no version is lost"
        );
        assert_eq!(m.len(), 1);
    });
}

#[test]
fn chain_two_prepends() {
    two_prepends_model();
}

#[test]
fn chain_mutant_with_unlatched_positions() {
    assert_mutant_fails(
        &crate::latch::mutants::CHAIN_POSITION_UNLATCHED,
        two_prepends_model,
    );
}

#[test]
fn chain_prepend_against_out_of_order_insert() {
    model(|| {
        let m = vmap();
        let (a, b) = (Arc::clone(&m), Arc::clone(&m));
        par(
            move || {
                a.insert(b"k".to_vec(), 3, 30);
            },
            move || {
                b.insert(b"k".to_vec(), 1, 10);
            },
        );
        assert_eq!(
            m.get_all_versions(&b"k"[..]),
            vec![(3, Some(30)), (2, Some(20)), (1, Some(10))]
        );
        assert_eq!(m.len(), 1);
    });
}

#[test]
fn chain_same_version_replace_against_delete() {
    model(|| {
        let m = vmap();
        let (a, b) = (Arc::clone(&m), Arc::clone(&m));
        par(
            move || {
                a.insert(b"k".to_vec(), 2, 21);
            },
            move || {
                b.delete(b"k".to_vec(), 3);
            },
        );
        assert_eq!(
            m.get_all_versions(&b"k"[..]),
            vec![(3, None), (2, Some(21))]
        );
        assert_eq!(m.len(), 0, "the tombstone head makes the key absent");
    });
}

// Prune (§11.4) exists only on the heap map. Its epoch guards come from
// `crossbeam-epoch`, which loom does not instrument; the chain itself is
// loom-visible, and nothing is freed while the model's threads are pinned.

type HMap = crate::VersionedArtMap<Vec<u8>, u64>;

#[test]
fn chain_prune_tombstone_replacement_against_older_insert() {
    model(|| {
        // The head is a user tombstone (value 0) at version 2.
        let m = Arc::new(HMap::new());
        m.insert(b"k".to_vec(), 2, 0);
        let (a, b) = (Arc::clone(&m), Arc::clone(&m));
        par(
            move || {
                a.prune_key(&b"k"[..], u64::MAX, |v| *v == 0);
            },
            move || {
                b.insert(b"k".to_vec(), 1, 10);
            },
        );
        let all = m.get_all_versions(&b"k"[..]);
        assert_eq!(all[0], (2, None), "the head became a built-in tombstone");
        assert!(all.windows(2).all(|w| w[0].0 > w[1].0), "sorted: {all:?}");
        assert!(all.len() <= 2);
        assert_eq!(m.len(), 0);
    });
}

#[test]
fn chain_prune_detach_against_same_version_head_replace() {
    model(|| {
        let m = Arc::new(HMap::new());
        m.insert(b"k".to_vec(), 1, 10);
        m.insert(b"k".to_vec(), 2, 20);
        let (a, b) = (Arc::clone(&m), Arc::clone(&m));
        par(
            move || {
                a.prune_key(&b"k"[..], 2, |_| false);
            },
            move || {
                b.insert(b"k".to_vec(), 2, 21);
            },
        );
        let all = m.get_all_versions(&b"k"[..]);
        assert_eq!(all, vec![(2, Some(21))], "the replace wins; v1 is pruned");
        assert_eq!(m.len(), 1);
    });
}
