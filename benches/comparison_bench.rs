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
use artmap::arena::ArenaArtMap;
use artmap::ArtMap;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use crossbeam_skiplist::SkipMap;
use parking_lot::RwLock;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

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

    // ArenaArtMap
    group.bench_function("arena_artmap", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = ArenaArtMap::<[u8; 8], u64>::with_capacity(32 * 1024 * 1024);
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
                let map = ArenaArtMap::<[u8; 8], u64>::with_capacity(32 * 1024 * 1024);
                let mut ins = artmap::arena::ArenaInserter::new();
                let start = Instant::now();
                for key in 0..BATCH {
                    let k = key.to_be_bytes();
                    let _ = map.insert_with_inserter(k, key, &mut ins);
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

    // ArenaArtMap
    group.bench_function("arena_artmap", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = ArenaArtMap::<[u8; 8], u64>::with_capacity(32 * 1024 * 1024);
                let start = Instant::now();
                for (i, &k) in random_keys.iter().enumerate() {
                    let _ = map.insert(k, i as u64);
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

    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_get");
    group.throughput(Throughput::Elements(1));

    let art_map = ArtMap::<[u8; 8], u64>::new();
    let arena_map = ArenaArtMap::<[u8; 8], u64>::with_capacity(32 * 1024 * 1024);
    let sl_arena = SkiplistArena::with_capacity(32 * 1024 * 1024);
    let skiplist = SkipList::new(sl_arena);
    let skip_map = SkipMap::<[u8; 8], u64>::new();
    let mut btree = BTreeMap::new();
    let mut hmap = HashMap::new();
    let mut imbl_map = imbl::OrdMap::new();

    for i in 0..SAMPLE_SIZE as u64 {
        let k = i.to_be_bytes();
        art_map.insert(k, i);
        arena_map.insert(k, i);
        let _ = skiplist.insert(&k, &k);
        skip_map.insert(k, i);
        btree.insert(i, i);
        hmap.insert(i, i);
        imbl_map.insert(i, i);
    }

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

    group.finish();
}

fn bench_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_scan_100");
    group.throughput(Throughput::Elements(100));

    let art_map = ArtMap::<[u8; 8], u64>::new();
    let arena_map = ArenaArtMap::<[u8; 8], u64>::with_capacity(32 * 1024 * 1024);
    let sl_arena = SkiplistArena::with_capacity(32 * 1024 * 1024);
    let skiplist = SkipList::new(sl_arena);
    let skip_map = SkipMap::<[u8; 8], u64>::new();
    let mut btree = BTreeMap::new();
    let mut imbl_map = imbl::OrdMap::new();

    for i in 0..SAMPLE_SIZE as u64 {
        let k = i.to_be_bytes();
        art_map.insert(k, i);
        arena_map.insert(k, i);
        let _ = skiplist.insert(&k, &k);
        skip_map.insert(k, i);
        btree.insert(i, i);
        imbl_map.insert(i, i);
    }

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
