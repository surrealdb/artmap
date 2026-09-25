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

use artmap::ArtMap;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use crossbeam_skiplist::SkipMap;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, HashMap};

const SAMPLE_SIZE: usize = 50_000;

fn seeded_rng(seed: u64) -> StdRng {
    StdRng::seed_from_u64(seed)
}

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");
    group.throughput(Throughput::Elements(1));

    // ArtMap
    group.bench_function("artmap", |b| {
        let map = ArtMap::<[u8; 8], u64>::new();
        let mut key = 0u64;
        b.iter(|| {
            let k = key.to_be_bytes();
            let _ = map.insert(k, key);
            key += 1;
        })
    });

    // crossbeam-skiplist SkipMap
    group.bench_function("crossbeam_skipmap", |b| {
        let map = SkipMap::<[u8; 8], u64>::new();
        let mut key = 0u64;
        b.iter(|| {
            let k = key.to_be_bytes();
            let _ = map.insert(k, key);
            key += 1;
        })
    });

    // BTreeMap
    group.bench_function("btreemap", |b| {
        let mut btree = BTreeMap::new();
        let mut key = 0u64;
        b.iter(|| {
            btree.insert(key, key);
            key += 1;
        })
    });

    // HashMap
    group.bench_function("hashmap", |b| {
        let mut hmap = HashMap::new();
        let mut key = 0u64;
        b.iter(|| {
            hmap.insert(key, key);
            key += 1;
        })
    });

    // imbl::OrdMap
    group.bench_function("imbl_ordmap", |b| {
        let mut imbl_map = imbl::OrdMap::new();
        let mut key = 0u64;
        b.iter(|| {
            imbl_map.insert(key, key);
            key += 1;
        })
    });

    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_get");
    group.throughput(Throughput::Elements(1));

    let art_map = ArtMap::<[u8; 8], u64>::new();
    let skip_map = SkipMap::<[u8; 8], u64>::new();
    let mut btree = BTreeMap::new();
    let mut hmap = HashMap::new();
    let mut imbl_map = imbl::OrdMap::new();

    for i in 0..SAMPLE_SIZE as u64 {
        let k = i.to_be_bytes();
        art_map.insert(k, i);
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
    let skip_map = SkipMap::<[u8; 8], u64>::new();
    let mut btree = BTreeMap::new();
    let mut imbl_map = imbl::OrdMap::new();

    for i in 0..SAMPLE_SIZE as u64 {
        let k = i.to_be_bytes();
        art_map.insert(k, i);
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

criterion_group!(benches, bench_insert, bench_get, bench_scan);
criterion_main!(benches);
