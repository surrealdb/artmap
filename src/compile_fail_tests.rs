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

//! Compile-time soundness checks (§16.5), run as doctests.
//!
//! Every `compile_fail` block has a compiling twin, so that a block cannot
//! pass because of an unrelated error. Error codes are checked on nightly.
//!
//! ## Lifetimes: handles never outlive the map or their guard
//!
//! An `EntryRef` cannot outlive its map:
//!
//! ```compile_fail,E0597
//! let e = {
//!     let m = artmap::ArtMap::<Vec<u8>, u64>::new();
//!     m.insert(vec![1], 1);
//!     m.get(&[1u8][..]).unwrap()
//! };
//! drop(e);
//! ```
//!
//! ```
//! let m = artmap::ArtMap::<Vec<u8>, u64>::new();
//! m.insert(vec![1], 1);
//! let e = m.get(&[1u8][..]).unwrap();
//! assert_eq!(*e, 1);
//! ```
//!
//! A handle cannot outlive a borrowed guard:
//!
//! ```compile_fail,E0597
//! let m = artmap::ArtMap::<Vec<u8>, u64>::new();
//! m.insert(vec![1], 1);
//! let e = {
//!     let g = m.pin();
//!     m.get_with_guard(&[1u8][..], &g)
//! };
//! drop(e);
//! ```
//!
//! ```
//! let m = artmap::ArtMap::<Vec<u8>, u64>::new();
//! m.insert(vec![1], 1);
//! let g = m.pin();
//! let e = m.get_with_guard(&[1u8][..], &g);
//! assert_eq!(e.as_deref(), Some(&1));
//! ```
//!
//! A `get_with_guard` result cannot outlive the map, even if the guard does:
//!
//! ```compile_fail,E0597
//! let other = artmap::ArtMap::<Vec<u8>, u64>::new();
//! let g = other.pin();
//! let e = {
//!     let m = artmap::ArtMap::<Vec<u8>, u64>::new();
//!     m.insert(vec![1], 1);
//!     m.get_with_guard(&[1u8][..], &g)
//! };
//! drop(e);
//! ```
//!
//! A displaced entry returned by `insert` (or `remove`) cannot outlive the map:
//!
//! ```compile_fail,E0597
//! let old = {
//!     let m = artmap::ArtMap::<Vec<u8>, u64>::new();
//!     m.insert(vec![1], 1);
//!     m.insert(vec![1], 2)
//! };
//! drop(old);
//! ```
//!
//! ```
//! let m = artmap::ArtMap::<Vec<u8>, u64>::new();
//! m.insert(vec![1], 1);
//! let old = m.insert(vec![1], 2);
//! assert_eq!(old.as_deref(), Some(&1));
//! ```
//!
//! A `&V` cannot outlive its `EntryRef`:
//!
//! ```compile_fail,E0597
//! let m = artmap::ArtMap::<Vec<u8>, u64>::new();
//! m.insert(vec![1], 1);
//! let v = {
//!     let e = m.get(&[1u8][..]).unwrap();
//!     e.value()
//! };
//! assert_eq!(*v, 1);
//! ```
//!
//! ```
//! let m = artmap::ArtMap::<Vec<u8>, u64>::new();
//! m.insert(vec![1], 1);
//! let v = {
//!     let e = m.get(&[1u8][..]).unwrap();
//!     *e.value()
//! };
//! assert_eq!(v, 1);
//! ```
//!
//! `with_value` cannot leak the reference:
//!
//! ```compile_fail
//! let m = artmap::ArtMap::<Vec<u8>, u64>::new();
//! let r = m.with_value(&[1u8][..], |v| v);
//! drop(r);
//! ```
//!
//! ```
//! let m = artmap::ArtMap::<Vec<u8>, u64>::new();
//! m.insert(vec![1], 1);
//! assert_eq!(m.with_value(&[1u8][..], |v| *v), Some(1));
//! ```
//!
//! Iterator items, keys and values cannot outlive the map:
//!
//! ```compile_fail,E0597
//! let items: Vec<_> = {
//!     let m = artmap::ArtMap::<Vec<u8>, u64>::new();
//!     m.insert(vec![1], 1);
//!     m.iter().collect()
//! };
//! drop(items);
//! ```
//!
//! ```compile_fail,E0597
//! let keys: Vec<_> = {
//!     let m = artmap::ArtMap::<Vec<u8>, u64>::new();
//!     m.insert(vec![1], 1);
//!     m.keys().collect()
//! };
//! drop(keys);
//! ```
//!
//! ```compile_fail,E0597
//! let values: Vec<_> = {
//!     let m = artmap::ArtMap::<Vec<u8>, u64>::new();
//!     m.insert(vec![1], 1);
//!     m.values().collect()
//! };
//! drop(values);
//! ```
//!
//! ```
//! let m = artmap::ArtMap::<Vec<u8>, u64>::new();
//! m.insert(vec![1], 1);
//! let keys: Vec<_> = m.keys().collect();
//! let values: Vec<_> = m.values().collect();
//! m.remove(&[1u8][..]);
//! // Still valid: each item keeps the iterator's guard alive.
//! assert_eq!(**keys[0], [1u8]);
//! assert_eq!(*values[0], 1);
//! ```
//!
//! A `VersionedEntryRef` cannot outlive its map:
//!
//! ```compile_fail,E0597
//! let e = {
//!     let m = artmap::VersionedArtMap::<Vec<u8>, u64>::new();
//!     m.insert(vec![1], 1, 1);
//!     m.iter().next().unwrap()
//! };
//! drop(e);
//! ```
//!
//! A versioned `&V` cannot outlive the item it was borrowed from, so a
//! same-version replace cannot free it underneath the caller:
//!
//! ```compile_fail,E0716
//! let m = artmap::VersionedArtMap::<Vec<u8>, u64>::new();
//! m.insert(vec![1], 1, 1);
//! let v = m.iter().next().unwrap().value();
//! m.insert(vec![1], 2, 1);
//! assert_eq!(*v, 1);
//! ```
//!
//! ```
//! let m = artmap::VersionedArtMap::<Vec<u8>, u64>::new();
//! m.insert(vec![1], 1, 1);
//! let e = m.iter().next().unwrap();
//! m.insert(vec![1], 2, 1);
//! assert_eq!(*e.value(), 1);
//! ```
//!
//! `validate_invariants` needs exclusive access, so it cannot race a writer:
//!
//! ```compile_fail,E0596
//! let m = std::sync::Arc::new(artmap::ArtMap::<Vec<u8>, u64>::new());
//! m.validate_invariants();
//! ```
//!
//! ```
//! let mut m = std::sync::Arc::new(artmap::ArtMap::<Vec<u8>, u64>::new());
//! std::sync::Arc::get_mut(&mut m).unwrap().validate_invariants();
//! ```
//!
//! An arena entry cannot outlive its map:
//!
//! ```compile_fail,E0597
//! let e = {
//!     let m = artmap::ArenaArtMap::<Vec<u8>, u64>::with_capacity(1 << 16);
//!     m.insert(vec![1], 1);
//!     m.get_entry(&[1u8][..]).unwrap()
//! };
//! drop(e);
//! ```
//!
//! ```compile_fail,E0597
//! let versions: Vec<_> = {
//!     let m = artmap::ArenaVersionedArtMap::<Vec<u8>, u64>::with_capacity(1 << 16);
//!     m.insert(vec![1], 1, 1);
//!     let e = m.get_entry(&[1u8][..]).unwrap();
//!     e.versions().collect()
//! };
//! drop(versions);
//! ```
//!
//! ```
//! let m = artmap::ArenaVersionedArtMap::<Vec<u8>, u64>::with_capacity(1 << 16);
//! m.insert(vec![1], 1, 1);
//! m.insert(vec![1], 2, 2);
//! let e = m.get_entry(&[1u8][..]).unwrap();
//! let v: Vec<_> = e.versions().map(|v| (v.version, v.value.copied())).collect();
//! assert_eq!(v, vec![(2, Some(2)), (1, Some(1))]);
//! ```
//!
//! A guard cannot be repinned while something borrows it:
//!
//! ```compile_fail,E0502
//! let m = artmap::ArtMap::<Vec<u8>, u64>::new();
//! m.insert(vec![1], 1);
//! let mut g = m.pin();
//! let e = m.get_with_guard(&[1u8][..], &g);
//! g.repin();
//! drop(e);
//! ```
//!
//! ```
//! let m = artmap::ArtMap::<Vec<u8>, u64>::new();
//! let mut g = m.pin();
//! drop(m.get_with_guard(&[1u8][..], &g));
//! g.repin();
//! ```
//!
//! A raw `crossbeam_epoch` guard is not accepted (Inv 11):
//!
//! ```compile_fail,E0308
//! let m = artmap::ArtMap::<Vec<u8>, u64>::new();
//! let g = crossbeam_epoch::pin();
//! let _ = m.get_with_guard(&[1u8][..], &g);
//! ```
//!
//! ## Variance: every map is invariant in `K` and `V` (Inv 9)
//!
//! A covariant map would let a `for<'x> fn(&'x u8)` value be weakened to
//! `fn(&'static u8)` through a shared reference and then called on a
//! short-lived borrow.
//!
//! ```compile_fail,E0308
//! fn weaken<'a>(m: &'a artmap::ArtMap<Vec<u8>, for<'x> fn(&'x u8)>) -> &'a artmap::ArtMap<Vec<u8>, fn(&'static u8)> {
//!     m
//! }
//! ```
//!
//! ```compile_fail,E0308
//! fn weaken<'a>(m: &'a artmap::VersionedArtMap<Vec<u8>, for<'x> fn(&'x u8)>) -> &'a artmap::VersionedArtMap<Vec<u8>, fn(&'static u8)> {
//!     m
//! }
//! ```
//!
//! ```compile_fail,E0308
//! fn weaken<'a>(m: &'a artmap::ArenaArtMap<Vec<u8>, for<'x> fn(&'x u8)>) -> &'a artmap::ArenaArtMap<Vec<u8>, fn(&'static u8)> {
//!     m
//! }
//! ```
//!
//! ```compile_fail,E0308
//! fn weaken<'a>(m: &'a artmap::ArenaVersionedArtMap<Vec<u8>, for<'x> fn(&'x u8)>) -> &'a artmap::ArenaVersionedArtMap<Vec<u8>, fn(&'static u8)> {
//!     m
//! }
//! ```
//!
//! ```compile_fail
//! fn shrink<'a>(m: &'a artmap::ArenaArtMap<&'static str, u64>) -> &'a artmap::ArenaArtMap<&'a str, u64> {
//!     m
//! }
//! ```
//!
//! ```compile_fail
//! fn shrink<'a>(m: &'a artmap::ArenaVersionedArtMap<&'static str, u64>) -> &'a artmap::ArenaVersionedArtMap<&'a str, u64> {
//!     m
//! }
//! ```
//!
//! Positive control: identity is fine.
//!
//! ```
//! fn same<'a>(m: &'a artmap::ArtMap<Vec<u8>, fn(&'static u8)>) -> &'a artmap::ArtMap<Vec<u8>, fn(&'static u8)> {
//!     m
//! }
//! ```
//!
//! ## Auto traits: handles, guards, iterators and inserters are `!Send`
//!
//! ```compile_fail,E0277
//! fn send<T: Send>(_: T) {}
//! let m = artmap::ArtMap::<Vec<u8>, u64>::new();
//! send(m.get(&[1u8][..]));
//! ```
//!
//! ```compile_fail,E0277
//! fn send<T: Send>(_: T) {}
//! let m = artmap::ArtMap::<Vec<u8>, u64>::new();
//! send(m.pin());
//! ```
//!
//! ```compile_fail,E0277
//! fn send<T: Send>(_: T) {}
//! let m = artmap::ArtMap::<Vec<u8>, u64>::new();
//! send(m.iter());
//! ```
//!
//! ```compile_fail,E0277
//! fn send<T: Send>(_: T) {}
//! let m = artmap::VersionedArtMap::<Vec<u8>, u64>::new();
//! send(m.iter());
//! ```
//!
//! ```compile_fail,E0277
//! fn send<T: Send>(_: T) {}
//! let m = artmap::ArenaArtMap::<Vec<u8>, u64>::with_capacity(1 << 16);
//! send(m.inserter());
//! ```
//!
//! ```compile_fail,E0277
//! fn send<T: Send>(_: T) {}
//! let m = artmap::ArenaArtMap::<Vec<u8>, u64>::with_capacity(1 << 16);
//! send(m.get_entry(&[1u8][..]));
//! ```
//!
//! ```
//! fn send<T: Send>(_: T) {}
//! let m = artmap::ArtMap::<Vec<u8>, u64>::new();
//! m.insert(vec![1], 7);
//! send(m.get_value(&[1u8][..]));
//! send(m);
//! ```
//!
//! Values and keys that are not `Send` cannot be inserted into the EBR maps,
//! whose destructors may run on other threads:
//!
//! ```compile_fail,E0599
//! let m = artmap::ArtMap::<Vec<u8>, std::rc::Rc<u8>>::new();
//! m.insert(vec![1], std::rc::Rc::new(1));
//! ```
//!
//! ```compile_fail,E0599
//! let m = artmap::VersionedArtMap::<Vec<u8>, std::rc::Rc<u8>>::new();
//! m.insert(vec![1], 1, std::rc::Rc::new(1));
//! ```
//!
//! ## Sealed internals are not nameable (§8.1)
//!
//! ```compile_fail,E0603
//! use artmap::tree::Tree;
//! ```
//!
//! ```compile_fail,E0603
//! use artmap::node::Leaf;
//! ```
//!
//! ```compile_fail,E0603
//! use artmap::versioned::tree::VersionedTree;
//! ```
//!
//! ```compile_fail,E0603
//! use artmap::arena::node::Leaf;
//! ```
//!
//! ```compile_fail,E0624
//! let a = artmap::Arena::new(1024);
//! let _ = a.alloc(std::alloc::Layout::new::<u64>());
//! ```
//!
//! ```compile_fail,E0599
//! let a = artmap::Arena::new(1024);
//! let _ = a.get_pointer(1 << 20);
//! ```
//!
//! ```compile_fail,E0599
//! let m = artmap::ArenaArtMap::<Vec<u8>, u64>::with_capacity(1 << 16);
//! m.debug_lookup(b"k");
//! ```
//!
//! ```compile_fail,E0599
//! let i = artmap::ArenaInserter::<Vec<u8>, u64>::new();
//! ```
