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

//! Benchmarks that track the read, overwrite and contention budgets of the
//! safety plan (§16.7). They only use API that is stable across the refactor,
//! so the same file measures the baseline and every later phase.

use std::hint::black_box;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use artmap::{ArenaArtMap, ArtMap, VersionedArtMap};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const N: u64 = 10_000;
const THREADS: usize = 8;

fn seeded(seed: u64) -> StdRng {
    StdRng::seed_from_u64(seed)
}

fn filled_map() -> ArtMap<[u8; 8], u64> {
    let map = ArtMap::new();
    for i in 0..N {
        let _ = map.insert(i.to_be_bytes(), i);
    }
    map
}

fn random_keys(count: usize, seed: u64) -> Vec<[u8; 8]> {
    let mut rng = seeded(seed);
    (0..count)
        .map(|_| rng.gen_range(0..N).to_be_bytes())
        .collect()
}

/// Runs `per_thread(thread, iters)` on `THREADS` threads that are spawned
/// before the clock starts, and returns the wall time of the slowest thread.
fn run_threads<F>(iters: u64, per_thread: F) -> Duration
where
    F: Fn(usize, u64) + Send + Sync + 'static,
{
    let per_thread = Arc::new(per_thread);
    let barrier = Arc::new(Barrier::new(THREADS + 1));
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let barrier = Arc::clone(&barrier);
            let f = Arc::clone(&per_thread);
            std::thread::spawn(move || {
                barrier.wait();
                f(t, iters);
            })
        })
        .collect();
    barrier.wait();
    let start = Instant::now();
    for h in handles {
        h.join().unwrap();
    }
    start.elapsed()
}

fn bench_overwrite(c: &mut Criterion) {
    let mut group = c.benchmark_group("overwrite");
    group.throughput(Throughput::Elements(1));
    group.bench_function("1t", |b| {
        let map = filled_map();
        let keys = random_keys(4096, 1);
        let mut i = 0usize;
        b.iter(|| {
            let k = keys[i & 4095];
            i += 1;
            black_box(map.insert(k, i as u64).is_some());
        });
    });
    group.bench_function("8t", |b| {
        let map = Arc::new(filled_map());
        b.iter_custom(|iters| {
            let map = Arc::clone(&map);
            run_threads(iters, move |t, iters| {
                let keys = random_keys(4096, 10 + t as u64);
                for i in 0..iters as usize {
                    black_box(map.insert(keys[i & 4095], i as u64).is_some());
                }
            })
        });
    });
    group.finish();
}

fn bench_churn(c: &mut Criterion) {
    let mut group = c.benchmark_group("remove_reinsert_churn");
    group.throughput(Throughput::Elements(1));
    group.bench_function("1t", |b| {
        let map = filled_map();
        let keys = random_keys(4096, 2);
        let mut i = 0usize;
        b.iter(|| {
            let k = keys[i & 4095];
            i += 1;
            black_box(map.remove(&k).is_some());
            black_box(map.insert(k, i as u64).is_some());
        });
    });
    group.finish();
}

fn bench_gets(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_get");
    group.throughput(Throughput::Elements(1));

    group.bench_function("random_get", |b| {
        let map = filled_map();
        let keys = random_keys(4096, 3);
        let mut i = 0usize;
        b.iter(|| {
            let k = &keys[i & 4095];
            i += 1;
            black_box(map.get(k).is_some());
        });
    });

    group.bench_function("get_with_guard", |b| {
        let map = filled_map();
        let keys = random_keys(4096, 3);
        let guard = map.pin();
        let mut i = 0usize;
        b.iter(|| {
            let k = &keys[i & 4095];
            i += 1;
            black_box(map.get_with_guard(k, &guard).is_some());
        });
    });

    group.bench_function("with_value", |b| {
        let map = filled_map();
        let keys = random_keys(4096, 3);
        let mut i = 0usize;
        b.iter(|| {
            let k = &keys[i & 4095];
            i += 1;
            black_box(map.with_value(k, |v| *v));
        });
    });

    // Node16-heavy: three-byte keys with 10 distinct bytes at every level, so
    // every inner node is a Node16 holding 10 children.
    group.bench_function("node16_get", |b| {
        let map = ArtMap::<[u8; 3], u64>::new();
        for a in 0..10u8 {
            for bb in 0..10u8 {
                for cc in 0..10u8 {
                    let _ = map.insert([a * 7, bb * 13, cc * 17], 1);
                }
            }
        }
        let mut rng = seeded(4);
        let keys: Vec<[u8; 3]> = (0..4096)
            .map(|_| {
                [
                    rng.gen_range(0..10u8) * 7,
                    rng.gen_range(0..10u8) * 13,
                    rng.gen_range(0..10u8) * 17,
                ]
            })
            .collect();
        let mut i = 0usize;
        b.iter(|| {
            let k = &keys[i & 4095];
            i += 1;
            black_box(map.get(k).is_some());
        });
    });

    // Keys sharing a 64-byte prefix: a chain of prefix-compressed Node4s.
    group.bench_function("long_prefix_get", |b| {
        let map = ArtMap::<Vec<u8>, u64>::new();
        let mk = |i: u64| {
            let mut k = vec![b'p'; 64];
            k.extend_from_slice(&i.to_be_bytes());
            k
        };
        for i in 0..N {
            let _ = map.insert(mk(i), i);
        }
        let mut rng = seeded(5);
        let keys: Vec<Vec<u8>> = (0..4096).map(|_| mk(rng.gen_range(0..N))).collect();
        let mut i = 0usize;
        b.iter(|| {
            let k = keys[i & 4095].as_slice();
            i += 1;
            black_box(map.get_by_slice(k).is_some());
        });
    });

    // Eight maps accessed round-robin from one thread.
    group.bench_function("round_robin_8_maps", |b| {
        let maps: Vec<_> = (0..8).map(|_| filled_map()).collect();
        let keys = random_keys(4096, 6);
        let mut i = 0usize;
        b.iter(|| {
            let k = &keys[i & 4095];
            let m = &maps[i & 7];
            i += 1;
            black_box(m.get(k).is_some());
        });
    });
    group.finish();

    let mut group = c.benchmark_group("concurrent_reads_persistent");
    group.throughput(Throughput::Elements(THREADS as u64));
    group.bench_function("8t", |b| {
        let map = Arc::new(filled_map());
        b.iter_custom(|iters| {
            let map = Arc::clone(&map);
            run_threads(iters, move |t, iters| {
                let keys = random_keys(4096, 20 + t as u64);
                for i in 0..iters as usize {
                    black_box(map.get(&keys[i & 4095]).is_some());
                }
            })
        });
    });
    group.finish();
}

fn bench_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_scan");
    group.throughput(Throughput::Elements(100));
    group.bench_function("100", |b| {
        let map = filled_map();
        let mut rng = seeded(7);
        let starts: Vec<u64> = (0..4096).map(|_| rng.gen_range(0..N - 100)).collect();
        let mut i = 0usize;
        b.iter(|| {
            let s = starts[i & 4095];
            i += 1;
            let lo = s.to_be_bytes();
            let hi = (s + 100).to_be_bytes();
            let mut n = 0u64;
            for e in map.range(lo..hi) {
                n += *e.value();
            }
            black_box(n);
        });
    });
    group.bench_function("100_scan", |b| {
        // The callback scan: one pin, no per-entry handles.
        let map = filled_map();
        let mut rng = seeded(7);
        let starts: Vec<u64> = (0..4096).map(|_| rng.gen_range(0..N - 100)).collect();
        let mut i = 0usize;
        b.iter(|| {
            let s = starts[i & 4095];
            i += 1;
            let mut n = 0u64;
            map.scan(s.to_be_bytes()..(s + 100).to_be_bytes(), |_, v| {
                n += *v;
                true
            });
            black_box(n);
        });
    });
    group.bench_function("full_iter_10k", |b| {
        let map = filled_map();
        b.iter(|| {
            let mut n = 0u64;
            for e in map.iter() {
                n += *e.value();
            }
            black_box(n);
        });
    });
    group.finish();
}

fn bench_hot_node256(c: &mut Criterion) {
    let mut group = c.benchmark_group("hot_node256_insert");
    group.throughput(Throughput::Elements(65_536));
    group.sample_size(10);
    group.bench_function("8t", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let map = Arc::new(ArtMap::<[u8; 2], u64>::new());
                let m = Arc::clone(&map);
                total += run_threads(1, move |t, _| {
                    // Every thread fills the same second-level node at the same
                    // time, so the inserts contend on one Node256.
                    for b0 in 0..=255u8 {
                        for b1 in (t as u16..256).step_by(THREADS) {
                            let _ = m.insert([b0, b1 as u8], 1);
                        }
                    }
                });
                assert_eq!(map.len(), 65_536);
            }
            total
        });
    });
    group.finish();
}

fn bench_versioned(c: &mut Criterion) {
    let mut group = c.benchmark_group("versioned_update");
    group.throughput(Throughput::Elements(N));
    group.sample_size(20);
    // Second version of every key: lands in the leaf's inline slot.
    group.bench_function("inline", |b| {
        b.iter_batched(
            || {
                let map = VersionedArtMap::<[u8; 8], u64>::new();
                for i in 0..N {
                    map.insert(i.to_be_bytes(), 1, i);
                }
                map
            },
            |map| {
                for i in 0..N {
                    map.insert(i.to_be_bytes(), 2, i);
                }
                map
            },
            BatchSize::LargeInput,
        );
    });
    // Versions 4.. of every key: heap-allocated version nodes.
    group.bench_function("spilled", |b| {
        b.iter_batched(
            || {
                let map = VersionedArtMap::<[u8; 8], u64>::new();
                for v in 1..=3 {
                    for i in 0..N {
                        map.insert(i.to_be_bytes(), v, i);
                    }
                }
                map
            },
            |map| {
                for i in 0..N {
                    map.insert(i.to_be_bytes(), 4, i);
                }
                map
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();

    let mut group = c.benchmark_group("versioned_update_8t");
    group.throughput(Throughput::Elements(1));
    group.bench_function("spilled", |b| {
        let map = Arc::new(VersionedArtMap::<[u8; 8], u64>::new());
        for i in 0..N {
            map.insert(i.to_be_bytes(), 1, i);
        }
        let version = Arc::new(std::sync::atomic::AtomicU64::new(2));
        b.iter_custom(|iters| {
            let map = Arc::clone(&map);
            let version = Arc::clone(&version);
            run_threads(iters, move |t, iters| {
                let keys = random_keys(4096, 30 + t as u64);
                for i in 0..iters as usize {
                    let v = version.fetch_add(1, Ordering::Relaxed);
                    black_box(map.insert(keys[i & 4095], v, i as u64));
                }
            })
        });
    });
    group.finish();
}

fn bench_arena_create(c: &mut Criterion) {
    // Creating an arena must not cost O(capacity): memtables create one per
    // flush. Pages are zeroed lazily by the OS.
    let mut group = c.benchmark_group("arena_create");
    for mib in [64usize, 256] {
        group.bench_function(format!("{mib}_mib"), |b| {
            b.iter(|| black_box(ArenaArtMap::<[u8; 8], u64>::with_capacity(mib << 20)))
        });
    }
    group.finish();
}

fn config() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2))
        .sample_size(30)
}

criterion_group! {
    name = benches;
    config = config();
    targets = bench_gets, bench_overwrite, bench_churn, bench_scan, bench_hot_node256, bench_versioned,
        bench_arena_create
}
criterion_main!(benches);
