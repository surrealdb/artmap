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

//! Model tests of every map and iterator against `BTreeMap` (§16.5): point
//! operations, forward, reverse and alternating iteration, every bound kind,
//! and adversarial byte keys (empty keys, keys that are prefixes of others,
//! long 0xFF runs, all 256 bytes at one level).

use std::collections::BTreeMap;
use std::ops::Bound;

use artmap::{ArenaArtMap, ArenaVersionedArtMap, ArtMap, VersionedArtMap};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

fn ops() -> usize {
    if cfg!(miri) {
        200
    } else {
        4000
    }
}

fn seeds() -> u64 {
    if cfg!(miri) {
        2
    } else {
        24
    }
}

/// Keys biased towards the edge cases.
fn gen_key(rng: &mut StdRng) -> Vec<u8> {
    match rng.gen_range(0..10) {
        0 => Vec::new(),
        1 => vec![0u8; rng.gen_range(1..4)],
        2 => vec![0xFFu8; rng.gen_range(60..70)],
        3 => {
            let mut k = vec![0xFFu8; 64];
            k.push(rng.gen_range(0..2));
            k
        }
        4 => vec![rng.gen::<u8>()],
        5 => {
            // Long shared prefixes: chains of Node4s.
            let mut k = vec![b'p'; rng.gen_range(15..40)];
            k.push(rng.gen_range(0..4));
            k
        }
        _ => {
            let len = rng.gen_range(1..6);
            (0..len).map(|_| rng.gen_range(0..4u8) * 60).collect()
        }
    }
}

fn gen_bound(rng: &mut StdRng) -> Bound<Vec<u8>> {
    match rng.gen_range(0..3) {
        0 => Bound::Unbounded,
        1 => Bound::Included(gen_key(rng)),
        _ => Bound::Excluded(gen_key(rng)),
    }
}

fn valid_range(s: &Bound<Vec<u8>>, e: &Bound<Vec<u8>>) -> bool {
    match (s, e) {
        (Bound::Included(a), Bound::Included(b)) => a <= b,
        (Bound::Included(a), Bound::Excluded(b)) | (Bound::Excluded(a), Bound::Included(b)) => {
            a <= b
        }
        (Bound::Excluded(a), Bound::Excluded(b)) => a < b,
        _ => true,
    }
}

fn as_ref_bound(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match b {
        Bound::Included(k) => Bound::Included(k.as_slice()),
        Bound::Excluded(k) => Bound::Excluded(k.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Collects `(key, value)` from any double-ended iterator in a random
/// alternation of `next` and `next_back`, returning the items in key order.
fn alternate<I, T>(
    mut it: I,
    rng: &mut StdRng,
    f: impl Fn(T) -> (Vec<u8>, u64),
) -> Vec<(Vec<u8>, u64)>
where
    I: DoubleEndedIterator<Item = T>,
{
    let mut front = Vec::new();
    let mut back = Vec::new();
    loop {
        let item = if rng.gen() {
            it.next().map(|x| (true, x))
        } else {
            it.next_back().map(|x| (false, x))
        };
        match item {
            Some((true, x)) => front.push(f(x)),
            Some((false, x)) => back.push(f(x)),
            None => break,
        }
    }
    back.reverse();
    front.extend(back);
    front
}

#[test]
fn artmap_matches_btreemap() {
    for seed in 0..seeds() {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut map = ArtMap::<Vec<u8>, u64>::new();
        let mut model = BTreeMap::<Vec<u8>, u64>::new();
        for i in 0..ops() as u64 {
            let k = gen_key(&mut rng);
            match rng.gen_range(0..10) {
                0..=3 => {
                    let old = map.insert(k.clone(), i).map(|e| *e.value());
                    assert_eq!(old, model.insert(k, i), "insert, seed {seed}");
                }
                4..=5 => {
                    let old = map.remove(&k).map(|e| *e.value());
                    assert_eq!(old, model.remove(&k), "remove, seed {seed}");
                }
                6 => {
                    let e = map.get_or_insert_with(k.clone(), || i);
                    let m = *model.entry(k).or_insert(i);
                    assert_eq!(*e, m);
                }
                7 => {
                    assert_eq!(
                        map.get(&k).map(|e| *e),
                        model.get(&k).copied(),
                        "get, seed {seed}"
                    );
                }
                _ => {
                    let (s, e) = (gen_bound(&mut rng), gen_bound(&mut rng));
                    if !valid_range(&s, &e) {
                        continue;
                    }
                    let want: Vec<_> = model
                        .range::<[u8], _>((as_ref_bound(&s), as_ref_bound(&e)))
                        .map(|(k, v)| (k.clone(), *v))
                        .collect();
                    let r = || map.range::<_, [u8]>((as_ref_bound(&s), as_ref_bound(&e)));
                    let fwd: Vec<_> = r().map(|e| (e.key().clone(), *e.value())).collect();
                    assert_eq!(fwd, want, "range fwd {s:?}..{e:?}, seed {seed}");
                    let mut rev: Vec<_> =
                        r().rev().map(|e| (e.key().clone(), *e.value())).collect();
                    rev.reverse();
                    assert_eq!(rev, want, "range rev {s:?}..{e:?}, seed {seed}");
                    let alt = alternate(r(), &mut rng, |e| (e.key().clone(), *e.value()));
                    assert_eq!(alt, want, "range alternating, seed {seed}");
                }
            }
            assert_eq!(map.len(), model.len());
        }
        let all: Vec<_> = map.iter().map(|e| (e.key().clone(), *e.value())).collect();
        let want: Vec<_> = model.iter().map(|(k, v)| (k.clone(), *v)).collect();
        assert_eq!(all, want);
        map.validate_invariants();
        map.clear();
        assert_eq!(map.len(), 0);
        assert!(map.iter().next().is_none());
        map.validate_invariants();
    }
}

#[test]
fn artmap_all_bytes_at_one_level() {
    let mut map = ArtMap::<Vec<u8>, u64>::new();
    let mut model = BTreeMap::new();
    for round in 0..3u8 {
        for b in (0..=255u8).rev() {
            let k = vec![round, b];
            map.insert(k.clone(), b as u64);
            model.insert(k, b as u64);
        }
    }
    for b in (0..=255u8).step_by(3) {
        map.remove(&vec![1, b]);
        model.remove(&vec![1, b]);
    }
    let got: Vec<_> = map.iter().map(|e| e.key().clone()).collect();
    let want: Vec<_> = model.keys().cloned().collect();
    assert_eq!(got, want);
    let got: Vec<_> = map.iter().rev().map(|e| e.key().clone()).collect();
    let want: Vec<_> = model.keys().rev().cloned().collect();
    assert_eq!(got, want);
    map.validate_invariants();
}

#[test]
fn versioned_matches_model() {
    for seed in 0..seeds() {
        let mut rng = StdRng::seed_from_u64(seed);
        let map = VersionedArtMap::<Vec<u8>, u64>::new();
        // key -> version -> Option<value>
        let mut model = BTreeMap::<Vec<u8>, BTreeMap<u64, Option<u64>>>::new();
        for i in 0..ops() as u64 {
            let k = gen_key(&mut rng);
            let ver = rng.gen_range(1..20u64);
            match rng.gen_range(0..10) {
                0..=4 => {
                    map.insert(k.clone(), ver, i);
                    model.entry(k).or_default().insert(ver, Some(i));
                }
                5 => {
                    map.delete(k.clone(), ver);
                    model.entry(k).or_default().insert(ver, None);
                }
                6 => {
                    let snap = rng.gen_range(0..22u64);
                    let want = model
                        .get(&k)
                        .and_then(|c| c.range(..=snap).next_back())
                        .and_then(|(v, x)| x.map(|x| (*v, x)));
                    assert_eq!(
                        map.get_version_le(&k, snap),
                        want,
                        "get_version_le, seed {seed}"
                    );
                }
                7 => {
                    let want: Vec<_> = model
                        .get(&k)
                        .map(|c| c.iter().rev().map(|(v, x)| (*v, *x)).collect())
                        .unwrap_or_default();
                    assert_eq!(map.get_all_versions(&k), want);
                    assert_eq!(map.version_count(&k), want.len());
                }
                _ => {
                    let (s, e) = (gen_bound(&mut rng), gen_bound(&mut rng));
                    if !valid_range(&s, &e) {
                        continue;
                    }
                    let want: Vec<_> = model
                        .range::<[u8], _>((as_ref_bound(&s), as_ref_bound(&e)))
                        .filter_map(|(k, c)| {
                            let (v, x) = c.iter().next_back()?;
                            x.map(|x| (k.clone(), *v, x))
                        })
                        .collect();
                    let r = || map.range::<_, [u8]>((as_ref_bound(&s), as_ref_bound(&e)));
                    let fwd: Vec<_> = r()
                        .map(|e| (e.key().clone(), e.version(), *e.value()))
                        .collect();
                    assert_eq!(fwd, want, "versioned range, seed {seed}");
                    let mut rev: Vec<_> = r()
                        .rev()
                        .map(|e| (e.key().clone(), e.version(), *e.value()))
                        .collect();
                    rev.reverse();
                    assert_eq!(rev, want, "versioned range rev, seed {seed}");
                }
            }
            let live = model
                .values()
                .filter(|c| c.values().next_back().is_some_and(|x| x.is_some()))
                .count();
            assert_eq!(map.len(), live, "versioned len, seed {seed}");
        }
        // Prune everything below the median version and check the watermark contract.
        let min = 10;
        map.prune_all(min, |_| false);
        for (k, c) in &model {
            for snap in min..22 {
                let want = c
                    .range(..=snap)
                    .next_back()
                    .and_then(|(v, x)| x.map(|x| (*v, x)));
                assert_eq!(
                    map.get_version_le(k, snap),
                    want,
                    "after prune, seed {seed}"
                );
            }
        }
    }
}

#[test]
fn arena_matches_btreemap() {
    for seed in 0..seeds() {
        let mut rng = StdRng::seed_from_u64(seed);
        let map = ArenaArtMap::<Vec<u8>, u64>::with_capacity(64 << 20);
        let mut model = BTreeMap::<Vec<u8>, u64>::new();
        for i in 0..ops() as u64 {
            let k = gen_key(&mut rng);
            match rng.gen_range(0..10) {
                0..=4 => {
                    map.insert(k.clone(), i);
                    model.insert(k, i);
                }
                5..=6 => {
                    map.remove(&k);
                    model.remove(&k);
                }
                7 => assert_eq!(map.get(&k), model.get(&k).copied()),
                _ => {
                    let (s, e) = (gen_bound(&mut rng), gen_bound(&mut rng));
                    if !valid_range(&s, &e) {
                        continue;
                    }
                    let want: Vec<_> = model
                        .range::<[u8], _>((as_ref_bound(&s), as_ref_bound(&e)))
                        .map(|(k, v)| (k.clone(), *v))
                        .collect();
                    let r = || map.range::<_, [u8]>((as_ref_bound(&s), as_ref_bound(&e)));
                    let fwd: Vec<_> = r().map(|e| (e.key().clone(), *e.value())).collect();
                    assert_eq!(fwd, want, "arena range, seed {seed}");
                    let mut rev: Vec<_> =
                        r().rev().map(|e| (e.key().clone(), *e.value())).collect();
                    rev.reverse();
                    assert_eq!(rev, want, "arena range rev, seed {seed}");
                    let alt = alternate(r(), &mut rng, |e| (e.key().clone(), *e.value()));
                    assert_eq!(alt, want, "arena alternating, seed {seed}");
                }
            }
            assert_eq!(map.len(), model.len(), "arena len, seed {seed}");
        }
    }
}

#[test]
fn arena_versioned_matches_model() {
    for seed in 0..seeds() {
        let mut rng = StdRng::seed_from_u64(seed);
        let map = ArenaVersionedArtMap::<Vec<u8>, u64>::with_capacity(64 << 20);
        let mut model = BTreeMap::<Vec<u8>, BTreeMap<u64, Option<u64>>>::new();
        for i in 0..ops() as u64 {
            let k = gen_key(&mut rng);
            let ver = rng.gen_range(1..20u64);
            match rng.gen_range(0..10) {
                0..=4 => {
                    map.insert(k.clone(), ver, i);
                    model.entry(k).or_default().insert(ver, Some(i));
                }
                5 => {
                    map.delete(k.clone(), ver);
                    model.entry(k).or_default().insert(ver, None);
                }
                6 => {
                    let snap = rng.gen_range(0..22u64);
                    let want = model
                        .get(&k)
                        .and_then(|c| c.range(..=snap).next_back())
                        .and_then(|(v, x)| x.map(|x| (*v, x)));
                    assert_eq!(map.get_version_le(&k, snap), want);
                }
                7 => {
                    let want: Vec<_> = model
                        .get(&k)
                        .map(|c| c.iter().rev().map(|(v, x)| (*v, *x)).collect())
                        .unwrap_or_default();
                    assert_eq!(map.get_all_versions(&k), want);
                }
                _ => {
                    let (s, e) = (gen_bound(&mut rng), gen_bound(&mut rng));
                    if !valid_range(&s, &e) {
                        continue;
                    }
                    let want: Vec<_> = model
                        .range::<[u8], _>((as_ref_bound(&s), as_ref_bound(&e)))
                        .filter_map(|(k, c)| {
                            let (v, x) = c.iter().next_back()?;
                            x.map(|x| (k.clone(), *v, x))
                        })
                        .collect();
                    let r = || map.range::<_, [u8]>((as_ref_bound(&s), as_ref_bound(&e)));
                    let fwd: Vec<_> = r()
                        .map(|e| (e.key().clone(), e.version(), *e.value()))
                        .collect();
                    assert_eq!(fwd, want, "arena versioned range, seed {seed}");
                    let mut rev: Vec<_> = r()
                        .rev()
                        .map(|e| (e.key().clone(), e.version(), *e.value()))
                        .collect();
                    rev.reverse();
                    assert_eq!(rev, want);
                    // first/last/successor/predecessor are latest-view.
                    assert_eq!(map.first_entry().map(|e| e.key().clone()), {
                        model
                            .iter()
                            .find(|(_, c)| c.values().next_back().is_some_and(|x| x.is_some()))
                            .map(|(k, _)| k.clone())
                    });
                    assert_eq!(map.last_entry().map(|e| e.key().clone()), {
                        model
                            .iter()
                            .rev()
                            .find(|(_, c)| c.values().next_back().is_some_and(|x| x.is_some()))
                            .map(|(k, _)| k.clone())
                    });
                }
            }
            let live = model
                .values()
                .filter(|c| c.values().next_back().is_some_and(|x| x.is_some()))
                .count();
            assert_eq!(map.len(), live, "arena versioned len, seed {seed}");
        }
    }
}
