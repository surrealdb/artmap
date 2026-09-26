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

use artmap::arena::ArenaArtMap;
use artmap::ArtMap;
use crossbeam_skiplist::SkipMap;
use std::collections::{BTreeMap, HashMap};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

const COUNTS: &[usize] = &[50_000];

#[divan::bench(args = COUNTS)]
fn alloc_arena_artmap_insert(bencher: divan::Bencher<'_, '_>, count: usize) {
    let map = ArenaArtMap::<[u8; 8], usize>::with_capacity(32 * 1024 * 1024);
    let mut key = 0usize;

    bencher.counter(count).bench_local(|| {
        let k = (key as u64).to_be_bytes();
        let _ = map.insert(k, key);
        key += 1;
    });
}

#[divan::bench(args = COUNTS)]
fn alloc_arenaskiplist_insert(bencher: divan::Bencher<'_, '_>, count: usize) {
    let arena = arenaskiplist::Arena::with_capacity(32 * 1024 * 1024);
    let list = arenaskiplist::SkipList::new(arena);
    let mut key = 0usize;

    bencher.counter(count).bench_local(|| {
        let k = (key as u64).to_be_bytes();
        let val = key.to_be_bytes();
        let _ = list.insert(&k, &val);
        key += 1;
    });
}

#[divan::bench(args = COUNTS)]
fn alloc_artmap_insert(bencher: divan::Bencher<'_, '_>, count: usize) {
    let map = ArtMap::<[u8; 8], usize>::new();
    let mut key = 0usize;

    bencher.counter(count).bench_local(|| {
        let k = (key as u64).to_be_bytes();
        let _ = map.insert(k, key);
        key += 1;
    });
}

#[divan::bench(args = COUNTS)]
fn alloc_crossbeam_skipmap_insert(bencher: divan::Bencher<'_, '_>, count: usize) {
    let map = SkipMap::<[u8; 8], usize>::new();
    let mut key = 0usize;

    bencher.counter(count).bench_local(|| {
        let k = (key as u64).to_be_bytes();
        let _ = map.insert(k, key);
        key += 1;
    });
}

#[divan::bench(args = COUNTS)]
fn alloc_btreemap_insert(bencher: divan::Bencher<'_, '_>, count: usize) {
    let mut map = BTreeMap::new();
    let mut key = 0usize;

    bencher.counter(count).bench_local(|| {
        map.insert(key, key);
        key += 1;
    });
}

#[divan::bench(args = COUNTS)]
fn alloc_hashmap_insert(bencher: divan::Bencher<'_, '_>, count: usize) {
    let mut map = HashMap::new();
    let mut key = 0usize;

    bencher.counter(count).bench_local(|| {
        map.insert(key, key);
        key += 1;
    });
}

#[divan::bench(args = COUNTS)]
fn alloc_imbl_ordmap_insert(bencher: divan::Bencher<'_, '_>, count: usize) {
    let mut map = imbl::OrdMap::new();
    let mut key = 0usize;

    bencher.counter(count).bench_local(|| {
        map.insert(key, key);
        key += 1;
    });
}
