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

//! # Versioned Deterministic Simulation Test (DST)
//!
//! Validates [`VersionedArtMap`] and [`ArenaVersionedArtMap`] under randomized concurrent
//! schedules against a canonical `BTreeMap<String, Vec<(u64, u64)>>` reference oracle.
//!
//! Run with:
//! ```bash
//! cargo test --test versioned_simulation_tests -- --nocapture
//! ARTMAP_SIM_SEED=12345678 cargo test --test versioned_simulation_tests -- --nocapture
//! ```

use std::collections::BTreeMap;
use std::env;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use artmap::arena::{Arena, ArenaVersionedArtMap};
use artmap::versioned::VersionedArtMap;
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};

fn get_seed() -> u64 {
    if let Ok(seed_str) = env::var("ARTMAP_SIM_SEED") {
        seed_str
            .parse::<u64>()
            .expect("ARTMAP_SIM_SEED must be a valid 64-bit integer")
    } else if cfg!(miri) {
        // Miri's isolation forbids reading the wall clock.
        0x5EED_A27A
    } else {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards");
        now.as_secs() ^ (now.subsec_nanos() as u64)
    }
}

fn generate_key(rng: &mut StdRng) -> String {
    let prefix = match rng.gen_range(0..4) {
        0 => "account:",
        1 => "orders:",
        2 => "inventory:",
        _ => "temp:",
    };
    let id: u32 = rng.gen_range(0..200);
    format!("{}{:04}", prefix, id)
}

#[derive(Default)]
struct VersionOracle {
    // Key -> sorted list of (version, value), ordered descending by version
    data: BTreeMap<String, Vec<(u64, u64)>>,
}

impl VersionOracle {
    fn insert(&mut self, key: String, version: u64, value: u64) {
        let chain = self.data.entry(key).or_default();
        if let Some(pos) = chain.iter().position(|&(v, _)| v == version) {
            chain[pos] = (version, value);
        } else {
            let pos = chain
                .iter()
                .position(|&(v, _)| v < version)
                .unwrap_or(chain.len());
            chain.insert(pos, (version, value));
        }
    }

    fn get_version_le(&self, key: &str, max_version: u64) -> Option<(u64, u64)> {
        let chain = self.data.get(key)?;
        for &(v, val) in chain {
            if v <= max_version {
                return Some((v, val));
            }
        }
        None
    }

    fn get_latest(&self, key: &str) -> Option<(u64, u64)> {
        let chain = self.data.get(key)?;
        chain.first().copied()
    }

    fn get_all_versions(&self, key: &str) -> Vec<(u64, u64)> {
        self.data.get(key).cloned().unwrap_or_default()
    }

    fn version_count(&self, key: &str) -> usize {
        self.data.get(key).map(|c| c.len()).unwrap_or(0)
    }

    fn prune_key(&mut self, key: &str, min_version: u64) -> usize {
        let Some(chain) = self.data.get_mut(key) else {
            return 0;
        };

        // Find the visible version at min_version
        let mut visible_idx = None;
        for (i, &(v, _)) in chain.iter().enumerate() {
            if v <= min_version {
                visible_idx = Some(i);
                break;
            }
        }

        if let Some(idx) = visible_idx {
            // Keep versions up to and including visible_idx; prune everything strictly older
            let older_count = chain.len() - (idx + 1);
            chain.truncate(idx + 1);
            older_count
        } else {
            0
        }
    }
}

#[test]
fn test_versioned_artmap_deterministic_simulation() {
    let seed = get_seed();
    println!("=== Running VersionedArtMap Deterministic Simulation Test (seed: {seed}) ===");

    let mut rng = StdRng::seed_from_u64(seed);
    let map = Arc::new(VersionedArtMap::<String, u64>::new());
    let oracle = Arc::new(Mutex::new(VersionOracle::default()));

    const NUM_WORKERS: usize = if cfg!(miri) { 2 } else { 4 };
    const OPS_PER_WORKER: usize = if cfg!(miri) { 50 } else { 1_500 };

    let handles: Vec<_> = (0..NUM_WORKERS)
        .map(|worker_id| {
            let map = Arc::clone(&map);
            let oracle = Arc::clone(&oracle);
            let worker_seed = rng.next_u64() ^ (worker_id as u64);

            thread::spawn(move || {
                let mut local_rng = StdRng::seed_from_u64(worker_seed);

                for _ in 0..OPS_PER_WORKER {
                    let op = local_rng.gen_range(0..100);
                    let key = generate_key(&mut local_rng);

                    if op < 45 {
                        // 45% Versioned Insert
                        let version = local_rng.gen_range(1..10_000);
                        let value = local_rng.next_u64();

                        let mut o = oracle.lock().unwrap();
                        o.insert(key.clone(), version, value);
                        let _ = map.insert(key, version, value);
                    } else if op < 70 {
                        // 25% Snapshot point lookup
                        let max_v = local_rng.gen_range(1..12_000);
                        let o = oracle.lock().unwrap();
                        let oracle_res = o.get_version_le(&key, max_v);
                        let map_res = map.get_version_le(&key, max_v);
                        assert_eq!(
                            map_res, oracle_res,
                            "get_version_le must match oracle for key {key} at version {max_v}"
                        );
                    } else if op < 85 {
                        // 15% Get latest and verify chain invariants
                        let o = oracle.lock().unwrap();
                        let oracle_latest = o.get_latest(&key);
                        let map_latest = map.get_latest(&key);
                        assert_eq!(map_latest, oracle_latest);

                        let oracle_count = o.version_count(&key);
                        let map_count = map.version_count(&key);
                        assert_eq!(map_count, oracle_count);

                        let map_all = map.get_all_versions(&key);
                        let oracle_all: Vec<_> = o
                            .get_all_versions(&key)
                            .into_iter()
                            .map(|(v, x)| (v, Some(x)))
                            .collect();
                        assert_eq!(map_all, oracle_all);
                    } else {
                        // 15% Prune older versions
                        let watermark = local_rng.gen_range(1..8_000);
                        let mut o = oracle.lock().unwrap();
                        let oracle_pruned = o.prune_key(&key, watermark);
                        let map_pruned = map.prune_key(&key, watermark, |_| false);
                        assert_eq!(
                            map_pruned, oracle_pruned,
                            "prune count must match oracle for key {key} at watermark {watermark}"
                        );
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_arena_versioned_artmap_deterministic_simulation() {
    let seed = get_seed();
    println!("=== Running ArenaVersionedArtMap Deterministic Simulation Test (seed: {seed}) ===");

    let mut rng = StdRng::seed_from_u64(seed);
    let arena = Arena::with_capacity(64 * 1024 * 1024);
    let map = Arc::new(ArenaVersionedArtMap::<String, u64>::new(arena));
    let oracle = Arc::new(Mutex::new(VersionOracle::default()));

    const NUM_WORKERS: usize = if cfg!(miri) { 2 } else { 4 };
    const OPS_PER_WORKER: usize = if cfg!(miri) { 50 } else { 1_500 };

    let handles: Vec<_> = (0..NUM_WORKERS)
        .map(|worker_id| {
            let map = Arc::clone(&map);
            let oracle = Arc::clone(&oracle);
            let worker_seed = rng.next_u64() ^ (worker_id as u64);

            thread::spawn(move || {
                let mut local_rng = StdRng::seed_from_u64(worker_seed);

                for _ in 0..OPS_PER_WORKER {
                    let op = local_rng.gen_range(0..100);
                    let key = generate_key(&mut local_rng);

                    if op < 50 {
                        // 50% Insert
                        let version = local_rng.gen_range(1..10_000);
                        let value = local_rng.next_u64();

                        let mut o = oracle.lock().unwrap();
                        o.insert(key.clone(), version, value);
                        let _ = map.insert(key, version, value);
                    } else if op < 80 {
                        // 30% Get version <= max_v
                        let max_v = local_rng.gen_range(1..12_000);
                        let o = oracle.lock().unwrap();
                        let oracle_res = o.get_version_le(&key, max_v);
                        let map_res = map.get_version_le(&key, max_v);
                        assert_eq!(
                            map_res, oracle_res,
                            "arena get_version_le must match oracle for key {key}"
                        );
                    } else {
                        // 20% Latest & All versions
                        let o = oracle.lock().unwrap();
                        let oracle_latest = o.get_latest(&key);
                        let map_latest = map.get_latest(&key);
                        assert_eq!(map_latest, oracle_latest);

                        let oracle_all: Vec<_> = o
                            .get_all_versions(&key)
                            .into_iter()
                            .map(|(v, x)| (v, Some(x)))
                            .collect();
                        let map_all = map.get_all_versions(&key);
                        assert_eq!(map_all, oracle_all);
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}
