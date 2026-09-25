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
use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;

fn bench_concurrent_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_reads");

    for num_threads in [1, 2, 4, 8] {
        group.bench_function(format!("threads_{}", num_threads), |b| {
            let map = Arc::new(ArtMap::<[u8; 8], u64>::new());
            for i in 0..10_000u64 {
                map.insert(i.to_be_bytes(), i);
            }

            b.iter(|| {
                let handles: Vec<_> = (0..num_threads)
                    .map(|_| {
                        let map = Arc::clone(&map);
                        std::thread::spawn(move || {
                            for i in 0..1000u64 {
                                let _ = map.get_by_slice(&i.to_be_bytes());
                            }
                        })
                    })
                    .collect();

                for h in handles {
                    h.join().unwrap();
                }
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_concurrent_reads);
criterion_main!(benches);
