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

fn new_leaf(k: &[u8]) -> NonNull<TLeaf> {
    boxed(TLeaf {
        removed: AtomicBool::new(false),
        key: k.to_vec(),
    })
}

fn insert(t: &T, k: &[u8]) -> bool {
    let leaf = new_leaf(k);
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
// Removes that empty a node (§13).

/// A tree of `keys`, then `removed` taken out again before the model runs.
fn tree_less(keys: &[&[u8]], removed: &[&[u8]]) -> Arc<T> {
    let t = tree(keys);
    for k in removed {
        assert!(remove(&t, k));
    }
    t
}

#[test]
fn unlink_against_get() {
    // Root {a: N {1}, b, c}: removing "a1" unlinks N, shifting the root's
    // keys under a reader looking up "c".
    model(|| {
        let t = tree_less(&[b"a1", b"a2", b"b", b"c"], &[b"a2"]);
        let (w, r) = (Arc::clone(&t), Arc::clone(&t));
        par(
            move || assert!(remove(&w, b"a1")),
            move || assert!(r.get(b"c").is_some(), "false negative during an unlink"),
        );
        assert_eq!(quiescent_check(t), 2);
    });
}

fn unlink_against_insert_model() {
    // Root "k" {a}: removing "ka" empties the root node while "kc" is
    // inserted into it, locking only that node.
    let t = tree_less(&[b"ka", b"kb"], &[b"kb"]);
    let (w, r) = (Arc::clone(&t), Arc::clone(&t));
    par(
        move || assert!(insert(&w, b"kc")),
        move || assert!(remove(&r, b"ka")),
    );
    assert!(t.get(b"kc").is_some(), "the insert was lost");
    assert_eq!(quiescent_check(t), 1);
}

#[test]
fn unlink_against_insert_into_node() {
    model(unlink_against_insert_model);
}

#[test]
fn unlink_mutant_without_node_latch() {
    assert_mutant_fails(
        &mutants::UNLINK_WITHOUT_NODE_LATCH,
        unlink_against_insert_model,
    );
}

#[test]
fn unlink_against_cached_insert() {
    // An inserter-style cached node (§12.4): the leaf split that built
    // N {a, b} under the root cached N. Two removes empty N while the cached
    // insert of "kc" upgrades N directly, without a descent. The leak-only
    // storage never frees N, as the arena does not.
    model(|| {
        let t = RawTree::new_in(Leak(PhantomData));
        insert(&t, b"ka");
        insert(&t, b"x");
        let mut hint = None;
        let kb = new_leaf(b"kb");
        let owner = unsafe { Unpublished::new(&t.storage, kb) };
        let key = unsafe { kb.as_ref() }.key.clone();
        assert!(matches!(
            t.insert_hinted(&owner, &key, Mode::InsertIfAbsent, 1, &(), &mut hint),
            Ok(Outcome::Inserted(_))
        ));
        drop(owner);
        let hint = hint.expect("the leaf split caches N");
        let t = Arc::new(t);
        let (w, r) = (Arc::clone(&t), Arc::clone(&t));
        par(
            move || {
                let mut hint = hint;
                let kc = new_leaf(b"kc");
                let owner = unsafe { Unpublished::new(&w.storage, kc) };
                let key = unsafe { kc.as_ref() }.key.clone();
                let fast = unsafe { w.insert_at_hint(&mut hint, &owner, &key, 1) };
                if fast.is_none() {
                    assert!(matches!(
                        w.insert(&owner, &key, Mode::InsertIfAbsent, 1, &()),
                        Ok(Outcome::Inserted(_))
                    ));
                }
            },
            move || {
                assert!(remove(&r, b"ka"));
                assert!(remove(&r, b"kb"));
            },
        );
        assert!(t.get(b"kc").is_some(), "the cached insert was lost");
        assert_eq!(quiescent_check(t), 2);
    });
}

#[test]
fn two_removes_empty_one_node() {
    // Root {k: N {a, b}, x}: whichever remove runs second unlinks N.
    model(|| {
        let t = tree(&[b"ka", b"kb", b"x"]);
        let (a, b) = (Arc::clone(&t), Arc::clone(&t));
        par(
            move || assert!(remove(&a, b"ka")),
            move || assert!(remove(&b, b"kb")),
        );
        assert!(t.get(b"x").is_some());
        assert_eq!(quiescent_check(t), 1);
    });
}

/// A 17-byte shared prefix: a chain link (16 prefix bytes and one child)
/// above the fork `{1, 2}`, under a root that also holds "x".
fn chain_key(last: u8) -> Vec<u8> {
    let mut k = b"0123456789abcdefg".to_vec();
    k.push(last);
    k
}

#[test]
fn chain_unlink_follow_up_against_get() {
    // Removing the chain's last key empties the fork, and the follow-up
    // unlinks the emptied link above it.
    model(|| {
        let t = tree_less(&[&chain_key(1), &chain_key(2), b"x"], &[&chain_key(2)]);
        let (w, r) = (Arc::clone(&t), Arc::clone(&t));
        par(
            move || assert!(remove(&w, &chain_key(1))),
            move || assert!(r.get(b"x").is_some(), "false negative during a follow-up"),
        );
        assert_eq!(quiescent_check(t), 1);
    });
}

#[test]
fn chain_unlink_follow_up_against_insert() {
    model(|| {
        let t = tree_less(&[&chain_key(1), &chain_key(2), b"x"], &[&chain_key(2)]);
        let (w, r) = (Arc::clone(&t), Arc::clone(&t));
        par(
            move || assert!(remove(&w, &chain_key(1))),
            move || assert!(insert(&r, &chain_key(3))),
        );
        assert!(t.get(&chain_key(3)).is_some(), "the insert was lost");
        assert_eq!(quiescent_check(t), 2);
    });
}

#[test]
fn clear_against_unlink_never_underflows() {
    model(|| {
        let t = tree_less(&[b"ka", b"kb"], &[b"kb"]);
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
// `shrink_to_fit` (§13): replacements are built out of place and published
// under the parent's latch, with the replaced nodes latched and obsoleted.

fn fit(t: &T) {
    t.shrink_to_fit(&());
}

fn root_type(t: &T) -> NodeType {
    unsafe { t.node_ref(t.root()) }.node_type
}

/// Root "k" as a Node16 left with `{0, 1}`.
fn node16_pair() -> Arc<T> {
    tree_less(&[b"k0", b"k1", b"k2", b"k3", b"k4"], &[b"k2", b"k3", b"k4"])
}

#[test]
fn fit_shrink_against_get() {
    model(|| {
        let t = node16_pair();
        assert_eq!(root_type(&t), NodeType::Node16);
        let (w, r) = (Arc::clone(&t), Arc::clone(&t));
        par(
            move || fit(&w),
            move || assert!(r.get(b"k1").is_some(), "false negative during a shrink"),
        );
        assert_eq!(
            root_type(&t),
            NodeType::Node4,
            "an uncontended shrink happens"
        );
        assert_eq!(quiescent_check(t), 2);
    });
}

fn fit_shrink_against_insert_model() {
    // The insert locks only the Node16 (it has room) while it is copied.
    let t = node16_pair();
    let (w, r) = (Arc::clone(&t), Arc::clone(&t));
    par(move || fit(&w), move || assert!(insert(&r, b"k5")));
    assert!(t.get(b"k5").is_some(), "the insert was lost");
    assert_eq!(quiescent_check(t), 3);
}

#[test]
fn fit_shrink_against_insert_into_node() {
    model(fit_shrink_against_insert_model);
}

#[test]
fn fit_mutant_without_node_latch() {
    assert_mutant_fails(
        &mutants::FIT_WITHOUT_NODE_LATCH,
        fit_shrink_against_insert_model,
    );
}

#[test]
fn fit_shrink_against_remove() {
    model(|| {
        let t = node16_pair();
        let (w, r) = (Arc::clone(&t), Arc::clone(&t));
        par(move || fit(&w), move || assert!(remove(&r, b"k0")));
        assert!(t.get(b"k1").is_some());
        assert_eq!(quiescent_check(t), 1);
    });
}

#[test]
fn fit_collapse_against_get() {
    // Root {a: N {1}, b}: N gives way to its leaf under a reader.
    model(|| {
        let t = tree_less(&[b"a1", b"a2", b"b"], &[b"a2"]);
        let (w, r) = (Arc::clone(&t), Arc::clone(&t));
        par(
            move || fit(&w),
            move || assert!(r.get(b"a1").is_some(), "false negative during a collapse"),
        );
        assert_eq!(quiescent_check(t), 2);
    });
}

/// Root "a" with the single child C "c" {1, 2}.
fn single_child() -> Arc<T> {
    tree_less(&[b"ax", b"abc1", b"abc2"], &[b"ax"])
}

#[test]
fn fit_merge_against_get() {
    // The root merges into a copy of C with the prefix "abc".
    model(|| {
        let t = single_child();
        let (w, r) = (Arc::clone(&t), Arc::clone(&t));
        par(
            move || fit(&w),
            move || assert!(r.get(b"abc1").is_some(), "false negative during a merge"),
        );
        assert_eq!(quiescent_check(t), 2);
    });
}

fn fit_merge_against_insert_model() {
    // The insert locks only C (it has room) while C is copied.
    let t = single_child();
    let (w, r) = (Arc::clone(&t), Arc::clone(&t));
    par(move || fit(&w), move || assert!(insert(&r, b"abc3")));
    assert!(t.get(b"abc3").is_some(), "the insert was lost");
    assert_eq!(quiescent_check(t), 3);
}

#[test]
fn fit_merge_against_insert_into_child() {
    model(fit_merge_against_insert_model);
}

#[test]
fn fit_mutant_merge_without_child_latch() {
    assert_mutant_fails(
        &mutants::MERGE_WITHOUT_CHILD_LATCH,
        fit_merge_against_insert_model,
    );
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
