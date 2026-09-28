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

use arenaskiplist::{Arena as SkiplistArena, SkipList};
use artmap::arena::{ArenaArtMap, ArenaVersionedArtMap};
use artmap::versioned::VersionedArtMap;
use artmap::ArtMap;
use concread::bptree::BptreeMap;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use crossbeam_skiplist::SkipMap;
use dashmap::DashMap;
use papaya::HashMap as PapayaMap;
use parking_lot::RwLock;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use scc::{Guard as SccGuard, HashIndex, TreeIndex};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};
use vart::art::Tree as VartTree;
use vart::FixedSizeKey;

const SAMPLE_SIZE: usize = 50_000;

fn seeded_rng(seed: u64) -> StdRng {
    StdRng::seed_from_u64(seed)
}

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");
    const BATCH: u64 = 50_000;
    group.throughput(Throughput::Elements(BATCH));

    // ArtMap
    group.bench_function("artmap", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = ArtMap::<[u8; 8], u64>::new();
                let start = Instant::now();
                for key in 0..BATCH {
                    let k = key.to_be_bytes();
                    let _ = map.insert(k, key);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // VersionedArtMap
    group.bench_function("versioned_artmap", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = VersionedArtMap::<[u8; 8], u64>::new();
                let start = Instant::now();
                for key in 0..BATCH {
                    let k = key.to_be_bytes();
                    let _ = map.insert(k, 1, key);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // ArenaArtMap
    group.bench_function("arena_artmap", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = ArenaArtMap::<[u8; 8], u64>::with_capacity(64 * 1024 * 1024);
                let start = Instant::now();
                for key in 0..BATCH {
                    let k = key.to_be_bytes();
                    let _ = map.insert(k, key);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // ArenaArtMap with Inserter
    group.bench_function("arena_artmap_with_inserter", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = ArenaArtMap::<[u8; 8], u64>::with_capacity(64 * 1024 * 1024);
                let mut ins = map.inserter();
                let start = Instant::now();
                for key in 0..BATCH {
                    let k = key.to_be_bytes();
                    let _ = ins.insert(k, key);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // ArenaVersionedArtMap
    group.bench_function("arena_versioned_artmap", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = ArenaVersionedArtMap::<[u8; 8], u64>::with_capacity(64 * 1024 * 1024);
                let start = Instant::now();
                for key in 0..BATCH {
                    let k = key.to_be_bytes();
                    let _ = map.insert(k, 1, key);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // ArenaVersionedArtMap with Inserter
    group.bench_function("arena_versioned_artmap_with_inserter", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = ArenaVersionedArtMap::<[u8; 8], u64>::with_capacity(64 * 1024 * 1024);
                let mut ins = map.inserter();
                let start = Instant::now();
                for key in 0..BATCH {
                    let k = key.to_be_bytes();
                    let _ = ins.insert(k, 1, key);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // arenaskiplist
    group.bench_function("arenaskiplist", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let arena = SkiplistArena::with_capacity(32 * 1024 * 1024);
                let list = SkipList::new(arena);
                let start = Instant::now();
                for key in 0..BATCH {
                    let k = key.to_be_bytes();
                    let val = key.to_be_bytes();
                    let _ = list.insert(&k, &val);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // arenaskiplist with Inserter
    group.bench_function("arenaskiplist_with_inserter", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let arena = SkiplistArena::with_capacity(32 * 1024 * 1024);
                let list = SkipList::new(arena);
                let mut ins = arenaskiplist::Inserter::new();
                let start = Instant::now();
                for key in 0..BATCH {
                    let k = key.to_be_bytes();
                    let val = key.to_be_bytes();
                    let _ = list.insert_with_inserter(&k, &val, &mut ins);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // crossbeam-skiplist SkipMap
    group.bench_function("crossbeam_skipmap", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = SkipMap::<[u8; 8], u64>::new();
                let start = Instant::now();
                for key in 0..BATCH {
                    let k = key.to_be_bytes();
                    let _ = map.insert(k, key);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // BTreeMap
    group.bench_function("btreemap", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let mut btree = BTreeMap::new();
                let start = Instant::now();
                for key in 0..BATCH {
                    btree.insert(key, key);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // HashMap
    group.bench_function("hashmap", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let mut hmap = HashMap::new();
                let start = Instant::now();
                for key in 0..BATCH {
                    hmap.insert(key, key);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // imbl::OrdMap
    group.bench_function("imbl_ordmap", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let mut imbl_map = imbl::OrdMap::new();
                let start = Instant::now();
                for key in 0..BATCH {
                    imbl_map.insert(key, key);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // DashMap
    group.bench_function("dashmap", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = DashMap::new();
                let start = Instant::now();
                for key in 0..BATCH {
                    map.insert(key, key);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // Papaya
    group.bench_function("papaya", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = PapayaMap::new();
                let pin = map.pin();
                let start = Instant::now();
                for key in 0..BATCH {
                    pin.insert(key, key);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // scc::TreeIndex
    group.bench_function("scc_tree_index", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = TreeIndex::new();
                let start = Instant::now();
                for key in 0..BATCH {
                    let _ = map.insert_sync(key, key);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // scc::HashIndex
    group.bench_function("scc_hash_index", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = HashIndex::new();
                let start = Instant::now();
                for key in 0..BATCH {
                    let _ = map.insert_sync(key, key);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // concread::bptree
    group.bench_function("concread_bptree", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = BptreeMap::new();
                let mut w = map.write();
                let start = Instant::now();
                for key in 0..BATCH {
                    w.insert(key, key);
                }
                w.commit();
                total += start.elapsed();
            }
            total
        })
    });

    // vart::Tree
    group.bench_function("vart", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let mut map = VartTree::<FixedSizeKey<16>, u64>::new();
                let start = Instant::now();
                for key in 0..BATCH {
                    let k: FixedSizeKey<16> = key.into();
                    let _ = map.insert_unchecked(&k, key, 1, 0);
                }
                total += start.elapsed();
            }
            total
        })
    });

    group.finish();
}

fn bench_random_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_insert");
    const BATCH: u64 = 50_000;
    group.throughput(Throughput::Elements(BATCH));

    let mut rng = seeded_rng(0x12345678);
    let random_keys: Vec<[u8; 8]> = (0..BATCH).map(|_| rng.gen::<u64>().to_be_bytes()).collect();

    // ArtMap
    group.bench_function("artmap", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = ArtMap::<[u8; 8], u64>::new();
                let start = Instant::now();
                for (i, &k) in random_keys.iter().enumerate() {
                    let _ = map.insert(k, i as u64);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // VersionedArtMap
    group.bench_function("versioned_artmap", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = VersionedArtMap::<[u8; 8], u64>::new();
                let start = Instant::now();
                for (i, &k) in random_keys.iter().enumerate() {
                    let _ = map.insert(k, 1, i as u64);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // ArenaArtMap
    group.bench_function("arena_artmap", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = ArenaArtMap::<[u8; 8], u64>::with_capacity(64 * 1024 * 1024);
                let start = Instant::now();
                for (i, &k) in random_keys.iter().enumerate() {
                    let _ = map.insert(k, i as u64);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // ArenaVersionedArtMap
    group.bench_function("arena_versioned_artmap", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = ArenaVersionedArtMap::<[u8; 8], u64>::with_capacity(64 * 1024 * 1024);
                let start = Instant::now();
                for (i, &k) in random_keys.iter().enumerate() {
                    let _ = map.insert(k, 1, i as u64);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // arenaskiplist
    group.bench_function("arenaskiplist", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let arena = SkiplistArena::with_capacity(32 * 1024 * 1024);
                let list = SkipList::new(arena);
                let start = Instant::now();
                for (i, &k) in random_keys.iter().enumerate() {
                    let val = (i as u64).to_be_bytes();
                    let _ = list.insert(&k, &val);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // crossbeam-skiplist SkipMap
    group.bench_function("crossbeam_skipmap", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = SkipMap::<[u8; 8], u64>::new();
                let start = Instant::now();
                for (i, &k) in random_keys.iter().enumerate() {
                    let _ = map.insert(k, i as u64);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // BTreeMap
    group.bench_function("btreemap", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let mut btree = BTreeMap::new();
                let start = Instant::now();
                for (i, &k) in random_keys.iter().enumerate() {
                    btree.insert(k, i as u64);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // HashMap
    group.bench_function("hashmap", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let mut hmap = HashMap::new();
                let start = Instant::now();
                for (i, &k) in random_keys.iter().enumerate() {
                    hmap.insert(k, i as u64);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // imbl::OrdMap
    group.bench_function("imbl_ordmap", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let mut imbl_map = imbl::OrdMap::new();
                let start = Instant::now();
                for (i, &k) in random_keys.iter().enumerate() {
                    imbl_map.insert(k, i as u64);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // DashMap
    group.bench_function("dashmap", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = DashMap::new();
                let start = Instant::now();
                for (i, &k) in random_keys.iter().enumerate() {
                    map.insert(k, i as u64);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // Papaya
    group.bench_function("papaya", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = PapayaMap::new();
                let pin = map.pin();
                let start = Instant::now();
                for (i, &k) in random_keys.iter().enumerate() {
                    pin.insert(k, i as u64);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // scc::TreeIndex
    group.bench_function("scc_tree_index", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = TreeIndex::new();
                let start = Instant::now();
                for (i, &k) in random_keys.iter().enumerate() {
                    let _ = map.insert_sync(k, i as u64);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // scc::HashIndex
    group.bench_function("scc_hash_index", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = HashIndex::new();
                let start = Instant::now();
                for (i, &k) in random_keys.iter().enumerate() {
                    let _ = map.insert_sync(k, i as u64);
                }
                total += start.elapsed();
            }
            total
        })
    });

    // concread::bptree
    group.bench_function("concread_bptree", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = BptreeMap::new();
                let mut w = map.write();
                let start = Instant::now();
                for (i, &k) in random_keys.iter().enumerate() {
                    w.insert(k, i as u64);
                }
                w.commit();
                total += start.elapsed();
            }
            total
        })
    });

    // vart::Tree
    group.bench_function("vart", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let mut map = VartTree::<FixedSizeKey<16>, u64>::new();
                let start = Instant::now();
                for (i, &k) in random_keys.iter().enumerate() {
                    let key: FixedSizeKey<16> = u64::from_be_bytes(k).into();
                    let _ = map.insert_unchecked(&key, i as u64, 1, 0);
                }
                total += start.elapsed();
            }
            total
        })
    });

    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_get");
    group.throughput(Throughput::Elements(1));

    let art_map = ArtMap::<[u8; 8], u64>::new();
    let versioned_map = VersionedArtMap::<[u8; 8], u64>::new();
    let arena_map = ArenaArtMap::<[u8; 8], u64>::with_capacity(64 * 1024 * 1024);
    let arena_versioned_map = ArenaVersionedArtMap::<[u8; 8], u64>::with_capacity(64 * 1024 * 1024);
    let sl_arena = SkiplistArena::with_capacity(64 * 1024 * 1024);
    let skiplist = SkipList::new(sl_arena);
    let skip_map = SkipMap::<[u8; 8], u64>::new();
    let mut btree = BTreeMap::new();
    let mut hmap = HashMap::new();
    let mut imbl_map = imbl::OrdMap::new();
    let dmap = DashMap::<u64, u64>::new();
    let pmap = PapayaMap::<u64, u64>::new();
    let scc_tree = TreeIndex::<u64, u64>::new();
    let scc_hash = HashIndex::<u64, u64>::new();
    let concread_tree = BptreeMap::<u64, u64>::new();
    let mut vart_tree = VartTree::<FixedSizeKey<16>, u64>::new();

    {
        let pin = pmap.pin();
        let mut w = concread_tree.write();
        for i in 0..SAMPLE_SIZE as u64 {
            let k = i.to_be_bytes();
            art_map.insert(k, i);
            versioned_map.insert(k, 1, i);
            arena_map.insert(k, i);
            arena_versioned_map.insert(k, 1, i);
            let _ = skiplist.insert(&k, &k);
            skip_map.insert(k, i);
            btree.insert(i, i);
            hmap.insert(i, i);
            imbl_map.insert(i, i);
            dmap.insert(i, i);
            pin.insert(i, i);
            let _ = scc_tree.insert_sync(i, i);
            let _ = scc_hash.insert_sync(i, i);
            w.insert(i, i);
            let k_vart: FixedSizeKey<16> = i.into();
            let _ = vart_tree.insert_unchecked(&k_vart, i, 1, 0);
        }
        w.commit();
    }
    let concread_reader = concread_tree.read();

    group.bench_function("artmap", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let key = rng.gen_range(0..SAMPLE_SIZE as u64);
            let k = key.to_be_bytes();
            black_box(art_map.get(&k))
        })
    });

    group.bench_function("artmap_slice", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let key = rng.gen_range(0..SAMPLE_SIZE as u64);
            let bytes = key.to_be_bytes();
            black_box(art_map.get_by_slice(&bytes))
        })
    });

    group.bench_function("versioned_artmap", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let key = rng.gen_range(0..SAMPLE_SIZE as u64);
            let k = key.to_be_bytes();
            black_box(versioned_map.get(&k))
        })
    });

    group.bench_function("versioned_artmap_slice", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let key = rng.gen_range(0..SAMPLE_SIZE as u64);
            let bytes = key.to_be_bytes();
            black_box(versioned_map.get_by_slice(&bytes))
        })
    });

    group.bench_function("arena_artmap", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let key = rng.gen_range(0..SAMPLE_SIZE as u64);
            let k = key.to_be_bytes();
            black_box(arena_map.get(&k))
        })
    });

    group.bench_function("arena_artmap_slice", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let key = rng.gen_range(0..SAMPLE_SIZE as u64);
            let bytes = key.to_be_bytes();
            black_box(arena_map.get_slice(&bytes))
        })
    });

    group.bench_function("arena_versioned_artmap", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let key = rng.gen_range(0..SAMPLE_SIZE as u64);
            let k = key.to_be_bytes();
            black_box(arena_versioned_map.get(&k))
        })
    });

    group.bench_function("arena_versioned_artmap_slice", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let key = rng.gen_range(0..SAMPLE_SIZE as u64);
            let bytes = key.to_be_bytes();
            black_box(arena_versioned_map.get_slice(&bytes))
        })
    });

    group.bench_function("arenaskiplist", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let key = rng.gen_range(0..SAMPLE_SIZE as u64);
            let k = key.to_be_bytes();
            black_box(skiplist.get_value(&k))
        })
    });

    group.bench_function("crossbeam_skipmap", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let key = rng.gen_range(0..SAMPLE_SIZE as u64);
            let k = key.to_be_bytes();
            black_box(skip_map.get(&k))
        })
    });

    group.bench_function("btreemap", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let key = rng.gen_range(0..SAMPLE_SIZE as u64);
            black_box(btree.get(&key))
        })
    });

    group.bench_function("hashmap", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let key = rng.gen_range(0..SAMPLE_SIZE as u64);
            black_box(hmap.get(&key))
        })
    });

    group.bench_function("imbl_ordmap", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let key = rng.gen_range(0..SAMPLE_SIZE as u64);
            black_box(imbl_map.get(&key))
        })
    });

    group.bench_function("dashmap", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let key = rng.gen_range(0..SAMPLE_SIZE as u64);
            black_box(dmap.get(&key))
        })
    });

    group.bench_function("papaya", |b| {
        let mut rng = seeded_rng(0x12345678);
        let pin = pmap.pin();
        b.iter(|| {
            let key = rng.gen_range(0..SAMPLE_SIZE as u64);
            black_box(pin.get(&key))
        })
    });

    group.bench_function("scc_tree_index", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let key = rng.gen_range(0..SAMPLE_SIZE as u64);
            black_box(scc_tree.peek_with(&key, |_, v| *v))
        })
    });

    group.bench_function("scc_hash_index", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let key = rng.gen_range(0..SAMPLE_SIZE as u64);
            black_box(scc_hash.peek_with(&key, |_, v| *v))
        })
    });

    group.bench_function("concread_bptree", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let key = rng.gen_range(0..SAMPLE_SIZE as u64);
            black_box(concread_reader.get(&key))
        })
    });

    group.bench_function("vart", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let key = rng.gen_range(0..SAMPLE_SIZE as u64);
            let k: FixedSizeKey<16> = key.into();
            black_box(vart_tree.get(&k, 0))
        })
    });

    group.bench_function("vart_slice", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let key = rng.gen_range(0..SAMPLE_SIZE as u64);
            let bytes = key.to_be_bytes();
            black_box(vart_tree.get_by_slice(&bytes, 0))
        })
    });

    group.finish();
}

fn bench_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_scan_100");
    group.throughput(Throughput::Elements(100));

    let art_map = ArtMap::<[u8; 8], u64>::new();
    let versioned_map = VersionedArtMap::<[u8; 8], u64>::new();
    let arena_map = ArenaArtMap::<[u8; 8], u64>::with_capacity(64 * 1024 * 1024);
    let arena_versioned_map = ArenaVersionedArtMap::<[u8; 8], u64>::with_capacity(64 * 1024 * 1024);
    let sl_arena = SkiplistArena::with_capacity(64 * 1024 * 1024);
    let skiplist = SkipList::new(sl_arena);
    let skip_map = SkipMap::<[u8; 8], u64>::new();
    let mut btree = BTreeMap::new();
    let mut imbl_map = imbl::OrdMap::new();
    let scc_tree = TreeIndex::<u64, u64>::new();
    let concread_tree = BptreeMap::<u64, u64>::new();
    let mut vart_tree = VartTree::<FixedSizeKey<16>, u64>::new();

    {
        let mut w = concread_tree.write();
        for i in 0..SAMPLE_SIZE as u64 {
            let k = i.to_be_bytes();
            art_map.insert(k, i);
            versioned_map.insert(k, 1, i);
            arena_map.insert(k, i);
            arena_versioned_map.insert(k, 1, i);
            let _ = skiplist.insert(&k, &k);
            skip_map.insert(k, i);
            btree.insert(i, i);
            imbl_map.insert(i, i);
            let _ = scc_tree.insert_sync(i, i);
            w.insert(i, i);
            let k_vart: FixedSizeKey<16> = i.into();
            let _ = vart_tree.insert_unchecked(&k_vart, i, 1, 0);
        }
        w.commit();
    }
    let concread_reader = concread_tree.read();

    group.bench_function("artmap", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let start = rng.gen_range(0..(SAMPLE_SIZE - 200) as u64);
            let start_k = start.to_be_bytes();
            let end_k = (start + 100).to_be_bytes();
            let count = art_map.range(start_k..end_k).count();
            black_box(count)
        })
    });

    group.bench_function("versioned_artmap", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let start = rng.gen_range(0..(SAMPLE_SIZE - 200) as u64);
            let start_k = start.to_be_bytes();
            let end_k = (start + 100).to_be_bytes();
            let count = versioned_map.range(start_k..end_k).count();
            black_box(count)
        })
    });

    group.bench_function("arena_artmap", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let start = rng.gen_range(0..(SAMPLE_SIZE - 200) as u64);
            let start_k = start.to_be_bytes();
            let end_k = (start + 100).to_be_bytes();
            let count = arena_map.range(start_k..end_k).count();
            black_box(count)
        })
    });

    group.bench_function("arena_versioned_artmap", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let start = rng.gen_range(0..(SAMPLE_SIZE - 200) as u64);
            let start_k = start.to_be_bytes();
            let end_k = (start + 100).to_be_bytes();
            let count = arena_versioned_map.range(start_k..end_k).count();
            black_box(count)
        })
    });

    group.bench_function("arenaskiplist", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let start = rng.gen_range(0..(SAMPLE_SIZE - 200) as u64);
            let start_k = start.to_be_bytes();
            let end_k = (start + 100).to_be_bytes();
            let count = skiplist.range(&start_k[..]..&end_k[..]).count();
            black_box(count)
        })
    });

    group.bench_function("crossbeam_skipmap", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let start = rng.gen_range(0..(SAMPLE_SIZE - 200) as u64);
            let start_k = start.to_be_bytes();
            let end_k = (start + 100).to_be_bytes();
            let count = skip_map.range(start_k..end_k).count();
            black_box(count)
        })
    });

    group.bench_function("btreemap", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let start = rng.gen_range(0..(SAMPLE_SIZE - 200) as u64);
            let count = btree.range(start..start + 100).count();
            black_box(count)
        })
    });

    group.bench_function("imbl_ordmap", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let start = rng.gen_range(0..(SAMPLE_SIZE - 200) as u64);
            let count = imbl_map.range(start..start + 100).count();
            black_box(count)
        })
    });

    group.bench_function("scc_tree_index", |b| {
        let mut rng = seeded_rng(0x12345678);
        let guard = SccGuard::new();
        b.iter(|| {
            let start = rng.gen_range(0..(SAMPLE_SIZE - 200) as u64);
            let count = scc_tree.range(start..start + 100, &guard).count();
            black_box(count)
        })
    });

    group.bench_function("concread_bptree", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let start = rng.gen_range(0..(SAMPLE_SIZE - 200) as u64);
            let count = concread_reader.range(start..start + 100).count();
            black_box(count)
        })
    });

    group.bench_function("vart", |b| {
        let mut rng = seeded_rng(0x12345678);
        b.iter(|| {
            let start = rng.gen_range(0..(SAMPLE_SIZE - 200) as u64);
            let start_k: FixedSizeKey<16> = start.into();
            let end_k: FixedSizeKey<16> = (start + 100).into();
            let count = vart_tree.range(&start_k..&end_k).count();
            black_box(count)
        })
    });

    group.finish();
}

fn bench_concurrent_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_writes_8t");
    group.sample_size(20);
    const TOTAL_OPS: u64 = 100_000;
    const NUM_THREADS: usize = 8;
    const PER_THREAD: u64 = TOTAL_OPS / NUM_THREADS as u64;
    group.throughput(Throughput::Elements(TOTAL_OPS));

    // ArtMap
    group.bench_function("artmap", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(ArtMap::<[u8; 8], u64>::new());
                let barrier = Arc::new(Barrier::new(NUM_THREADS + 1));
                let handles: Vec<_> = (0..NUM_THREADS)
                    .map(|t| {
                        let map = Arc::clone(&map);
                        let barrier = Arc::clone(&barrier);
                        std::thread::spawn(move || {
                            barrier.wait();
                            let start = t as u64 * PER_THREAD;
                            for i in 0..PER_THREAD {
                                let key = (start + i).to_be_bytes();
                                let _ = map.insert(key, start + i);
                            }
                        })
                    })
                    .collect();

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // VersionedArtMap
    group.bench_function("versioned_artmap", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(VersionedArtMap::<[u8; 8], u64>::new());
                let barrier = Arc::new(Barrier::new(NUM_THREADS + 1));
                let handles: Vec<_> = (0..NUM_THREADS)
                    .map(|t| {
                        let map = Arc::clone(&map);
                        let barrier = Arc::clone(&barrier);
                        std::thread::spawn(move || {
                            barrier.wait();
                            let start = t as u64 * PER_THREAD;
                            for i in 0..PER_THREAD {
                                let key = (start + i).to_be_bytes();
                                let _ = map.insert(key, 1, start + i);
                            }
                        })
                    })
                    .collect();

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // ArenaArtMap
    group.bench_function("arena_artmap", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(ArenaArtMap::<[u8; 8], u64>::with_capacity(64 * 1024 * 1024));
                let barrier = Arc::new(Barrier::new(NUM_THREADS + 1));
                let handles: Vec<_> = (0..NUM_THREADS)
                    .map(|t| {
                        let map = Arc::clone(&map);
                        let barrier = Arc::clone(&barrier);
                        std::thread::spawn(move || {
                            barrier.wait();
                            let start = t as u64 * PER_THREAD;
                            for i in 0..PER_THREAD {
                                let key = (start + i).to_be_bytes();
                                let _ = map.insert(key, start + i);
                            }
                        })
                    })
                    .collect();

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // ArenaVersionedArtMap
    group.bench_function("arena_versioned_artmap", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(ArenaVersionedArtMap::<[u8; 8], u64>::with_capacity(
                    64 * 1024 * 1024,
                ));
                let barrier = Arc::new(Barrier::new(NUM_THREADS + 1));
                let handles: Vec<_> = (0..NUM_THREADS)
                    .map(|t| {
                        let map = Arc::clone(&map);
                        let barrier = Arc::clone(&barrier);
                        std::thread::spawn(move || {
                            barrier.wait();
                            let start = t as u64 * PER_THREAD;
                            for i in 0..PER_THREAD {
                                let key = (start + i).to_be_bytes();
                                let _ = map.insert(key, 1, start + i);
                            }
                        })
                    })
                    .collect();

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // arenaskiplist
    group.bench_function("arenaskiplist", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let arena = SkiplistArena::with_capacity(64 * 1024 * 1024);
                let list = Arc::new(SkipList::new(arena));
                let barrier = Arc::new(Barrier::new(NUM_THREADS + 1));
                let handles: Vec<_> = (0..NUM_THREADS)
                    .map(|t| {
                        let list = Arc::clone(&list);
                        let barrier = Arc::clone(&barrier);
                        std::thread::spawn(move || {
                            barrier.wait();
                            let start = t as u64 * PER_THREAD;
                            for i in 0..PER_THREAD {
                                let key = (start + i).to_be_bytes();
                                let val = (start + i).to_be_bytes();
                                let _ = list.insert(&key, &val);
                            }
                        })
                    })
                    .collect();

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // crossbeam-skiplist SkipMap
    group.bench_function("crossbeam_skipmap", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(SkipMap::<[u8; 8], u64>::new());
                let barrier = Arc::new(Barrier::new(NUM_THREADS + 1));
                let handles: Vec<_> = (0..NUM_THREADS)
                    .map(|t| {
                        let map = Arc::clone(&map);
                        let barrier = Arc::clone(&barrier);
                        std::thread::spawn(move || {
                            barrier.wait();
                            let start = t as u64 * PER_THREAD;
                            for i in 0..PER_THREAD {
                                let key = (start + i).to_be_bytes();
                                let _ = map.insert(key, start + i);
                            }
                        })
                    })
                    .collect();

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // RwLock<BTreeMap>
    group.bench_function("rwlock_btreemap", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(RwLock::new(BTreeMap::<[u8; 8], u64>::new()));
                let barrier = Arc::new(Barrier::new(NUM_THREADS + 1));
                let handles: Vec<_> = (0..NUM_THREADS)
                    .map(|t| {
                        let map = Arc::clone(&map);
                        let barrier = Arc::clone(&barrier);
                        std::thread::spawn(move || {
                            barrier.wait();
                            let start = t as u64 * PER_THREAD;
                            for i in 0..PER_THREAD {
                                let key = (start + i).to_be_bytes();
                                map.write().insert(key, start + i);
                            }
                        })
                    })
                    .collect();

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // RwLock<HashMap>
    group.bench_function("rwlock_hashmap", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(RwLock::new(HashMap::<[u8; 8], u64>::new()));
                let barrier = Arc::new(Barrier::new(NUM_THREADS + 1));
                let handles: Vec<_> = (0..NUM_THREADS)
                    .map(|t| {
                        let map = Arc::clone(&map);
                        let barrier = Arc::clone(&barrier);
                        std::thread::spawn(move || {
                            barrier.wait();
                            let start = t as u64 * PER_THREAD;
                            for i in 0..PER_THREAD {
                                let key = (start + i).to_be_bytes();
                                map.write().insert(key, start + i);
                            }
                        })
                    })
                    .collect();

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // RwLock<imbl::OrdMap>
    group.bench_function("rwlock_imbl_ordmap", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(RwLock::new(imbl::OrdMap::<[u8; 8], u64>::new()));
                let barrier = Arc::new(Barrier::new(NUM_THREADS + 1));
                let handles: Vec<_> = (0..NUM_THREADS)
                    .map(|t| {
                        let map = Arc::clone(&map);
                        let barrier = Arc::clone(&barrier);
                        std::thread::spawn(move || {
                            barrier.wait();
                            let start = t as u64 * PER_THREAD;
                            for i in 0..PER_THREAD {
                                let key = (start + i).to_be_bytes();
                                map.write().insert(key, start + i);
                            }
                        })
                    })
                    .collect();

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // DashMap
    group.bench_function("dashmap", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(DashMap::<[u8; 8], u64>::new());
                let barrier = Arc::new(Barrier::new(NUM_THREADS + 1));
                let handles: Vec<_> = (0..NUM_THREADS)
                    .map(|t| {
                        let map = Arc::clone(&map);
                        let barrier = Arc::clone(&barrier);
                        std::thread::spawn(move || {
                            barrier.wait();
                            let start = t as u64 * PER_THREAD;
                            for i in 0..PER_THREAD {
                                let key = (start + i).to_be_bytes();
                                map.insert(key, start + i);
                            }
                        })
                    })
                    .collect();

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // Papaya
    group.bench_function("papaya", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(PapayaMap::<[u8; 8], u64>::new());
                let barrier = Arc::new(Barrier::new(NUM_THREADS + 1));
                let handles: Vec<_> = (0..NUM_THREADS)
                    .map(|t| {
                        let map = Arc::clone(&map);
                        let barrier = Arc::clone(&barrier);
                        std::thread::spawn(move || {
                            barrier.wait();
                            let pin = map.pin();
                            let start = t as u64 * PER_THREAD;
                            for i in 0..PER_THREAD {
                                let key = (start + i).to_be_bytes();
                                pin.insert(key, start + i);
                            }
                        })
                    })
                    .collect();

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // scc::TreeIndex
    group.bench_function("scc_tree_index", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(TreeIndex::<[u8; 8], u64>::new());
                let barrier = Arc::new(Barrier::new(NUM_THREADS + 1));
                let handles: Vec<_> = (0..NUM_THREADS)
                    .map(|t| {
                        let map = Arc::clone(&map);
                        let barrier = Arc::clone(&barrier);
                        std::thread::spawn(move || {
                            barrier.wait();
                            let start = t as u64 * PER_THREAD;
                            for i in 0..PER_THREAD {
                                let key = (start + i).to_be_bytes();
                                let _ = map.insert_sync(key, start + i);
                            }
                        })
                    })
                    .collect();

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // scc::HashIndex
    group.bench_function("scc_hash_index", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(HashIndex::<[u8; 8], u64>::new());
                let barrier = Arc::new(Barrier::new(NUM_THREADS + 1));
                let handles: Vec<_> = (0..NUM_THREADS)
                    .map(|t| {
                        let map = Arc::clone(&map);
                        let barrier = Arc::clone(&barrier);
                        std::thread::spawn(move || {
                            barrier.wait();
                            let start = t as u64 * PER_THREAD;
                            for i in 0..PER_THREAD {
                                let key = (start + i).to_be_bytes();
                                let _ = map.insert_sync(key, start + i);
                            }
                        })
                    })
                    .collect();

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // concread::bptree
    group.bench_function("concread_bptree", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(BptreeMap::<[u8; 8], u64>::new());
                let barrier = Arc::new(Barrier::new(NUM_THREADS + 1));
                let handles: Vec<_> = (0..NUM_THREADS)
                    .map(|t| {
                        let map = Arc::clone(&map);
                        let barrier = Arc::clone(&barrier);
                        std::thread::spawn(move || {
                            barrier.wait();
                            let start = t as u64 * PER_THREAD;
                            let mut w = map.write();
                            for i in 0..PER_THREAD {
                                let key = (start + i).to_be_bytes();
                                w.insert(key, start + i);
                            }
                            w.commit();
                        })
                    })
                    .collect();

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // RwLock<vart::Tree>
    group.bench_function("rwlock_vart", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(RwLock::new(VartTree::<FixedSizeKey<16>, u64>::new()));
                let barrier = Arc::new(Barrier::new(NUM_THREADS + 1));
                let handles: Vec<_> = (0..NUM_THREADS)
                    .map(|t| {
                        let map = Arc::clone(&map);
                        let barrier = Arc::clone(&barrier);
                        std::thread::spawn(move || {
                            barrier.wait();
                            let start = t as u64 * PER_THREAD;
                            for i in 0..PER_THREAD {
                                let key: FixedSizeKey<16> = (start + i).into();
                                let _ = map.write().insert_unchecked(&key, start + i, 1, 0);
                            }
                        })
                    })
                    .collect();

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    group.finish();
}

fn bench_concurrent_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_mixed_4r_4w");
    group.sample_size(20);
    const PRE_POPULATE: u64 = 100_000;
    const OPS_PER_THREAD: u64 = 12_500;
    const NUM_READERS: usize = 4;
    const NUM_WRITERS: usize = 4;
    const TOTAL_OPS: u64 = (NUM_READERS + NUM_WRITERS) as u64 * OPS_PER_THREAD;
    group.throughput(Throughput::Elements(TOTAL_OPS));

    // ArtMap
    group.bench_function("artmap", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(ArtMap::<[u8; 8], u64>::new());
                for i in 0..PRE_POPULATE {
                    map.insert(i.to_be_bytes(), i);
                }
                let barrier = Arc::new(Barrier::new(NUM_READERS + NUM_WRITERS + 1));

                let mut handles = Vec::new();

                // Writers
                for t in 0..NUM_WRITERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let start = PRE_POPULATE + (t as u64 * OPS_PER_THREAD);
                        for i in 0..OPS_PER_THREAD {
                            let key = (start + i).to_be_bytes();
                            let _ = map.insert(key, start + i);
                        }
                    }));
                }

                // Readers
                for _ in 0..NUM_READERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let mut rng = seeded_rng(0x12345678);
                        for _ in 0..OPS_PER_THREAD {
                            let key = rng.gen_range(0..PRE_POPULATE).to_be_bytes();
                            black_box(map.get_by_slice(&key));
                        }
                    }));
                }

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // VersionedArtMap
    group.bench_function("versioned_artmap", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(VersionedArtMap::<[u8; 8], u64>::new());
                for i in 0..PRE_POPULATE {
                    map.insert(i.to_be_bytes(), 1, i);
                }
                let barrier = Arc::new(Barrier::new(NUM_READERS + NUM_WRITERS + 1));

                let mut handles = Vec::new();

                // Writers
                for t in 0..NUM_WRITERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let start = PRE_POPULATE + (t as u64 * OPS_PER_THREAD);
                        for i in 0..OPS_PER_THREAD {
                            let key = (start + i).to_be_bytes();
                            let _ = map.insert(key, 2, start + i);
                        }
                    }));
                }

                // Readers
                for _ in 0..NUM_READERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let mut rng = seeded_rng(0x12345678);
                        for _ in 0..OPS_PER_THREAD {
                            let key = rng.gen_range(0..PRE_POPULATE).to_be_bytes();
                            black_box(map.get_by_slice(&key));
                        }
                    }));
                }

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // ArenaArtMap
    group.bench_function("arena_artmap", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(ArenaArtMap::<[u8; 8], u64>::with_capacity(64 * 1024 * 1024));
                for i in 0..PRE_POPULATE {
                    map.insert(i.to_be_bytes(), i);
                }
                let barrier = Arc::new(Barrier::new(NUM_READERS + NUM_WRITERS + 1));

                let mut handles = Vec::new();

                // Writers
                for t in 0..NUM_WRITERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let start = PRE_POPULATE + (t as u64 * OPS_PER_THREAD);
                        for i in 0..OPS_PER_THREAD {
                            let key = (start + i).to_be_bytes();
                            let _ = map.insert(key, start + i);
                        }
                    }));
                }

                // Readers
                for _ in 0..NUM_READERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let mut rng = seeded_rng(0x12345678);
                        for _ in 0..OPS_PER_THREAD {
                            let key = rng.gen_range(0..PRE_POPULATE).to_be_bytes();
                            black_box(map.get_slice(&key));
                        }
                    }));
                }

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // ArenaVersionedArtMap
    group.bench_function("arena_versioned_artmap", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(ArenaVersionedArtMap::<[u8; 8], u64>::with_capacity(
                    64 * 1024 * 1024,
                ));
                for i in 0..PRE_POPULATE {
                    map.insert(i.to_be_bytes(), 1, i);
                }
                let barrier = Arc::new(Barrier::new(NUM_READERS + NUM_WRITERS + 1));

                let mut handles = Vec::new();

                // Writers
                for t in 0..NUM_WRITERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let start = PRE_POPULATE + (t as u64 * OPS_PER_THREAD);
                        for i in 0..OPS_PER_THREAD {
                            let key = (start + i).to_be_bytes();
                            let _ = map.insert(key, 2, start + i);
                        }
                    }));
                }

                // Readers
                for _ in 0..NUM_READERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let mut rng = seeded_rng(0x12345678);
                        for _ in 0..OPS_PER_THREAD {
                            let key = rng.gen_range(0..PRE_POPULATE).to_be_bytes();
                            black_box(map.get_slice(&key));
                        }
                    }));
                }

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // arenaskiplist
    group.bench_function("arenaskiplist", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let arena = SkiplistArena::with_capacity(64 * 1024 * 1024);
                let list = Arc::new(SkipList::new(arena));
                for i in 0..PRE_POPULATE {
                    let k = i.to_be_bytes();
                    list.insert(&k, &k).unwrap();
                }
                let barrier = Arc::new(Barrier::new(NUM_READERS + NUM_WRITERS + 1));

                let mut handles = Vec::new();

                // Writers
                for t in 0..NUM_WRITERS {
                    let list = Arc::clone(&list);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let start = PRE_POPULATE + (t as u64 * OPS_PER_THREAD);
                        for i in 0..OPS_PER_THREAD {
                            let key = (start + i).to_be_bytes();
                            let val = (start + i).to_be_bytes();
                            let _ = list.insert(&key, &val);
                        }
                    }));
                }

                // Readers
                for _ in 0..NUM_READERS {
                    let list = Arc::clone(&list);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let mut rng = seeded_rng(0x12345678);
                        for _ in 0..OPS_PER_THREAD {
                            let key = rng.gen_range(0..PRE_POPULATE).to_be_bytes();
                            black_box(list.get_value(&key));
                        }
                    }));
                }

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // crossbeam-skiplist SkipMap
    group.bench_function("crossbeam_skipmap", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(SkipMap::<[u8; 8], u64>::new());
                for i in 0..PRE_POPULATE {
                    map.insert(i.to_be_bytes(), i);
                }
                let barrier = Arc::new(Barrier::new(NUM_READERS + NUM_WRITERS + 1));

                let mut handles = Vec::new();

                for t in 0..NUM_WRITERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let start = PRE_POPULATE + (t as u64 * OPS_PER_THREAD);
                        for i in 0..OPS_PER_THREAD {
                            let key = (start + i).to_be_bytes();
                            let _ = map.insert(key, start + i);
                        }
                    }));
                }

                for _ in 0..NUM_READERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let mut rng = seeded_rng(0x12345678);
                        for _ in 0..OPS_PER_THREAD {
                            let key = rng.gen_range(0..PRE_POPULATE).to_be_bytes();
                            black_box(map.get(&key));
                        }
                    }));
                }

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // RwLock<BTreeMap>
    group.bench_function("rwlock_btreemap", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(RwLock::new(BTreeMap::<[u8; 8], u64>::new()));
                {
                    let mut w = map.write();
                    for i in 0..PRE_POPULATE {
                        w.insert(i.to_be_bytes(), i);
                    }
                }
                let barrier = Arc::new(Barrier::new(NUM_READERS + NUM_WRITERS + 1));

                let mut handles = Vec::new();

                for t in 0..NUM_WRITERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let start = PRE_POPULATE + (t as u64 * OPS_PER_THREAD);
                        for i in 0..OPS_PER_THREAD {
                            let key = (start + i).to_be_bytes();
                            map.write().insert(key, start + i);
                        }
                    }));
                }

                for _ in 0..NUM_READERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let mut rng = seeded_rng(0x12345678);
                        for _ in 0..OPS_PER_THREAD {
                            let key = rng.gen_range(0..PRE_POPULATE).to_be_bytes();
                            black_box(map.read().get(&key).copied());
                        }
                    }));
                }

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // RwLock<HashMap>
    group.bench_function("rwlock_hashmap", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(RwLock::new(HashMap::<[u8; 8], u64>::new()));
                {
                    let mut w = map.write();
                    for i in 0..PRE_POPULATE {
                        w.insert(i.to_be_bytes(), i);
                    }
                }
                let barrier = Arc::new(Barrier::new(NUM_READERS + NUM_WRITERS + 1));

                let mut handles = Vec::new();

                for t in 0..NUM_WRITERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let start = PRE_POPULATE + (t as u64 * OPS_PER_THREAD);
                        for i in 0..OPS_PER_THREAD {
                            let key = (start + i).to_be_bytes();
                            map.write().insert(key, start + i);
                        }
                    }));
                }

                for _ in 0..NUM_READERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let mut rng = seeded_rng(0x12345678);
                        for _ in 0..OPS_PER_THREAD {
                            let key = rng.gen_range(0..PRE_POPULATE).to_be_bytes();
                            black_box(map.read().get(&key).copied());
                        }
                    }));
                }

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // RwLock<imbl::OrdMap>
    group.bench_function("rwlock_imbl_ordmap", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(RwLock::new(imbl::OrdMap::<[u8; 8], u64>::new()));
                {
                    let mut w = map.write();
                    for i in 0..PRE_POPULATE {
                        w.insert(i.to_be_bytes(), i);
                    }
                }
                let barrier = Arc::new(Barrier::new(NUM_READERS + NUM_WRITERS + 1));

                let mut handles = Vec::new();

                for t in 0..NUM_WRITERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let start = PRE_POPULATE + (t as u64 * OPS_PER_THREAD);
                        for i in 0..OPS_PER_THREAD {
                            let key = (start + i).to_be_bytes();
                            map.write().insert(key, start + i);
                        }
                    }));
                }

                for _ in 0..NUM_READERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let mut rng = seeded_rng(0x12345678);
                        for _ in 0..OPS_PER_THREAD {
                            let key = rng.gen_range(0..PRE_POPULATE).to_be_bytes();
                            black_box(map.read().get(&key).copied());
                        }
                    }));
                }

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // DashMap
    group.bench_function("dashmap", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(DashMap::<[u8; 8], u64>::new());
                for i in 0..PRE_POPULATE {
                    map.insert(i.to_be_bytes(), i);
                }
                let barrier = Arc::new(Barrier::new(NUM_READERS + NUM_WRITERS + 1));

                let mut handles = Vec::new();

                for t in 0..NUM_WRITERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let start = PRE_POPULATE + (t as u64 * OPS_PER_THREAD);
                        for i in 0..OPS_PER_THREAD {
                            let key = (start + i).to_be_bytes();
                            map.insert(key, start + i);
                        }
                    }));
                }

                for _ in 0..NUM_READERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let mut rng = seeded_rng(0x12345678);
                        for _ in 0..OPS_PER_THREAD {
                            let key = rng.gen_range(0..PRE_POPULATE).to_be_bytes();
                            black_box(map.get(&key));
                        }
                    }));
                }

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // Papaya
    group.bench_function("papaya", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(PapayaMap::<[u8; 8], u64>::new());
                {
                    let pin = map.pin();
                    for i in 0..PRE_POPULATE {
                        pin.insert(i.to_be_bytes(), i);
                    }
                }
                let barrier = Arc::new(Barrier::new(NUM_READERS + NUM_WRITERS + 1));

                let mut handles = Vec::new();

                for t in 0..NUM_WRITERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let pin = map.pin();
                        let start = PRE_POPULATE + (t as u64 * OPS_PER_THREAD);
                        for i in 0..OPS_PER_THREAD {
                            let key = (start + i).to_be_bytes();
                            pin.insert(key, start + i);
                        }
                    }));
                }

                for _ in 0..NUM_READERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let pin = map.pin();
                        let mut rng = seeded_rng(0x12345678);
                        for _ in 0..OPS_PER_THREAD {
                            let key = rng.gen_range(0..PRE_POPULATE).to_be_bytes();
                            black_box(pin.get(&key));
                        }
                    }));
                }

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // scc::TreeIndex
    group.bench_function("scc_tree_index", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(TreeIndex::<[u8; 8], u64>::new());
                for i in 0..PRE_POPULATE {
                    let _ = map.insert_sync(i.to_be_bytes(), i);
                }
                let barrier = Arc::new(Barrier::new(NUM_READERS + NUM_WRITERS + 1));

                let mut handles = Vec::new();

                for t in 0..NUM_WRITERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let start = PRE_POPULATE + (t as u64 * OPS_PER_THREAD);
                        for i in 0..OPS_PER_THREAD {
                            let key = (start + i).to_be_bytes();
                            let _ = map.insert_sync(key, start + i);
                        }
                    }));
                }

                for _ in 0..NUM_READERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let mut rng = seeded_rng(0x12345678);
                        for _ in 0..OPS_PER_THREAD {
                            let key = rng.gen_range(0..PRE_POPULATE).to_be_bytes();
                            black_box(map.peek_with(&key, |_, v| *v));
                        }
                    }));
                }

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // scc::HashIndex
    group.bench_function("scc_hash_index", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(HashIndex::<[u8; 8], u64>::new());
                for i in 0..PRE_POPULATE {
                    let _ = map.insert_sync(i.to_be_bytes(), i);
                }
                let barrier = Arc::new(Barrier::new(NUM_READERS + NUM_WRITERS + 1));

                let mut handles = Vec::new();

                for t in 0..NUM_WRITERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let start = PRE_POPULATE + (t as u64 * OPS_PER_THREAD);
                        for i in 0..OPS_PER_THREAD {
                            let key = (start + i).to_be_bytes();
                            let _ = map.insert_sync(key, start + i);
                        }
                    }));
                }

                for _ in 0..NUM_READERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let mut rng = seeded_rng(0x12345678);
                        for _ in 0..OPS_PER_THREAD {
                            let key = rng.gen_range(0..PRE_POPULATE).to_be_bytes();
                            black_box(map.peek_with(&key, |_, v| *v));
                        }
                    }));
                }

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // concread::bptree
    group.bench_function("concread_bptree", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(BptreeMap::<[u8; 8], u64>::new());
                {
                    let mut w = map.write();
                    for i in 0..PRE_POPULATE {
                        w.insert(i.to_be_bytes(), i);
                    }
                    w.commit();
                }
                let barrier = Arc::new(Barrier::new(NUM_READERS + NUM_WRITERS + 1));

                let mut handles = Vec::new();

                for t in 0..NUM_WRITERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let start = PRE_POPULATE + (t as u64 * OPS_PER_THREAD);
                        let mut w = map.write();
                        for i in 0..OPS_PER_THREAD {
                            let key = (start + i).to_be_bytes();
                            w.insert(key, start + i);
                        }
                        w.commit();
                    }));
                }

                for _ in 0..NUM_READERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let r = map.read();
                        let mut rng = seeded_rng(0x12345678);
                        for _ in 0..OPS_PER_THREAD {
                            let key = rng.gen_range(0..PRE_POPULATE).to_be_bytes();
                            black_box(r.get(&key));
                        }
                    }));
                }

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    // RwLock<vart::Tree>
    group.bench_function("rwlock_vart", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(RwLock::new(VartTree::<FixedSizeKey<16>, u64>::new()));
                {
                    let mut w = map.write();
                    for i in 0..PRE_POPULATE {
                        let k: FixedSizeKey<16> = i.into();
                        let _ = w.insert_unchecked(&k, i, 1, 0);
                    }
                }
                let barrier = Arc::new(Barrier::new(NUM_READERS + NUM_WRITERS + 1));

                let mut handles = Vec::new();

                for t in 0..NUM_WRITERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let start = PRE_POPULATE + (t as u64 * OPS_PER_THREAD);
                        for i in 0..OPS_PER_THREAD {
                            let key: FixedSizeKey<16> = (start + i).into();
                            let _ = map.write().insert_unchecked(&key, start + i, 1, 0);
                        }
                    }));
                }

                for _ in 0..NUM_READERS {
                    let map = Arc::clone(&map);
                    let barrier = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        let mut rng = seeded_rng(0x12345678);
                        for _ in 0..OPS_PER_THREAD {
                            let key = rng.gen_range(0..PRE_POPULATE);
                            let k: FixedSizeKey<16> = key.into();
                            black_box(map.read().get(&k, 0));
                        }
                    }));
                }

                let start_time = Instant::now();
                barrier.wait();
                for h in handles {
                    h.join().unwrap();
                }
                total_duration += start_time.elapsed();
            }
            total_duration
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_insert,
    bench_random_insert,
    bench_get,
    bench_scan,
    bench_concurrent_writes,
    bench_concurrent_mixed
);
criterion_main!(benches);
