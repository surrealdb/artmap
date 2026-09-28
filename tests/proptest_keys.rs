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

//! Property tests over arbitrary byte keys (§16.5): shrinking finds the
//! smallest operation sequence that diverges from a `BTreeMap`.

use std::collections::BTreeMap;
use std::ops::Bound;

use artmap::{ArenaArtMap, ArtMap};
use proptest::prelude::*;

#[derive(Clone, Debug)]
enum Op {
    Insert(Vec<u8>, u32),
    Remove(Vec<u8>),
    Get(Vec<u8>),
    Range(Bound<Vec<u8>>, Bound<Vec<u8>>, bool),
}

fn key() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        // Short keys over a small alphabet collide and share prefixes.
        prop::collection::vec(prop::sample::select(vec![0u8, 1, 0x7F, 0xFF]), 0..6),
        // Arbitrary bytes, up to past the 16-byte stored prefix and 64-byte
        // cursor buffers.
        prop::collection::vec(any::<u8>(), 0..80),
    ]
}

fn bound() -> impl Strategy<Value = Bound<Vec<u8>>> {
    prop_oneof![
        Just(Bound::Unbounded),
        key().prop_map(Bound::Included),
        key().prop_map(Bound::Excluded),
    ]
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (key(), any::<u32>()).prop_map(|(k, v)| Op::Insert(k, v)),
        2 => key().prop_map(Op::Remove),
        2 => key().prop_map(Op::Get),
        1 => (bound(), bound(), any::<bool>()).prop_map(|(s, e, r)| Op::Range(s, e, r)),
    ]
}

/// `BTreeMap::range` panics on these; the maps yield nothing.
fn empty_range(s: &Bound<Vec<u8>>, e: &Bound<Vec<u8>>) -> bool {
    match (s, e) {
        (Bound::Included(a), Bound::Included(b)) => a > b,
        (Bound::Included(a) | Bound::Excluded(a), Bound::Excluded(b))
        | (Bound::Excluded(a), Bound::Included(b)) => a >= b,
        _ => false,
    }
}

fn expected(
    model: &BTreeMap<Vec<u8>, u32>,
    s: &Bound<Vec<u8>>,
    e: &Bound<Vec<u8>>,
    rev: bool,
) -> Vec<(Vec<u8>, u32)> {
    if empty_range(s, e) {
        return Vec::new();
    }
    let it = model
        .range::<Vec<u8>, _>((s.clone(), e.clone()))
        .map(|(k, v)| (k.clone(), *v));
    if rev {
        it.rev().collect()
    } else {
        it.collect()
    }
}

fn config() -> ProptestConfig {
    ProptestConfig {
        cases: if cfg!(miri) { 4 } else { 256 },
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn artmap_matches_btreemap(ops in prop::collection::vec(op(), 1..200)) {
        let map = ArtMap::<Vec<u8>, u32>::new();
        let mut model = BTreeMap::new();
        for op in ops {
            match op {
                Op::Insert(k, v) => {
                    prop_assert_eq!(map.insert(k.clone(), v).map(|e| *e), model.insert(k, v));
                }
                Op::Remove(k) => {
                    prop_assert_eq!(map.remove(&k).map(|e| *e), model.remove(&k));
                }
                Op::Get(k) => {
                    prop_assert_eq!(map.get(&k).map(|e| *e), model.get(&k).copied());
                }
                Op::Range(s, e, rev) => {
                    let want = expected(&model, &s, &e, rev);
                    let range = map.range((s, e));
                    let got: Vec<_> = if rev {
                        range.rev().map(|e| (e.key().clone(), *e)).collect()
                    } else {
                        range.map(|e| (e.key().clone(), *e)).collect()
                    };
                    prop_assert_eq!(got, want);
                }
            }
            prop_assert_eq!(map.len(), model.len());
        }
    }

    #[test]
    fn arena_matches_btreemap(ops in prop::collection::vec(op(), 1..200)) {
        let map = ArenaArtMap::<Vec<u8>, u32>::with_capacity(1 << 22);
        let mut model = BTreeMap::new();
        for op in ops {
            match op {
                Op::Insert(k, v) => {
                    prop_assert_eq!(map.insert(k.clone(), v).map(|e| *e), model.insert(k, v));
                }
                Op::Remove(k) => {
                    prop_assert_eq!(map.remove(&k).map(|e| *e), model.remove(&k));
                }
                Op::Get(k) => {
                    prop_assert_eq!(map.get(&k), model.get(&k).copied());
                }
                Op::Range(s, e, rev) => {
                    let want = expected(&model, &s, &e, rev);
                    let range = map.range((s, e));
                    let got: Vec<_> = if rev {
                        range.rev().map(|e| (e.key().clone(), *e.value())).collect()
                    } else {
                        range.map(|e| (e.key().clone(), *e.value())).collect()
                    };
                    prop_assert_eq!(got, want);
                }
            }
            prop_assert_eq!(map.len(), model.len());
        }
    }
}
