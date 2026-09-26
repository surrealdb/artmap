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

//! # Deterministic Simulation Test (DST)
//!
//! Validates `ArtMap` under randomized concurrent schedules against a canonical
//! `std::collections::BTreeMap` reference oracle.
//!
//! Run with:
//! ```bash
//! cargo test --test sim -- --nocapture
//! ARTMAP_SIM_SEED=12345678 cargo test --test sim -- --nocapture
//! ```

use artmap::ArtMap;
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};
use std::collections::BTreeMap;
use std::env;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

fn get_seed() -> u64 {
    if let Ok(seed_str) = env::var("ARTMAP_SIM_SEED") {
        seed_str
            .parse::<u64>()
            .expect("ARTMAP_SIM_SEED must be a valid 64-bit integer")
    } else {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards");
        now.as_secs() ^ (now.subsec_nanos() as u64)
    }
}

fn generate_key(rng: &mut StdRng) -> String {
    let prefix_choice = rng.gen_range(0..5);
    let prefix = match prefix_choice {
        0 => "users:account:",
        1 => "users:profile:",
        2 => "orders:item:",
        3 => "temp:",
        _ => "",
    };

    let key_id: u32 = rng.gen_range(0..1000);
    format!("{}{:06}", prefix, key_id)
}

#[test]
fn test_deterministic_simulation() {
    let seed = get_seed();
    println!(
        "=== Running ArtMap Deterministic Simulation Test with seed: {} ===",
        seed
    );

    let mut rng = StdRng::seed_from_u64(seed);
    let map = Arc::new(ArtMap::<String, u64>::new());
    let oracle = Arc::new(Mutex::new(BTreeMap::<String, u64>::new()));

    const NUM_WORKERS: usize = 4;
    const OPS_PER_WORKER: usize = 1000;

    // Worker threads running concurrent randomized operations
    let handles: Vec<_> = (0..NUM_WORKERS)
        .map(|worker_id| {
            let map = Arc::clone(&map);
            let oracle = Arc::clone(&oracle);
            let worker_seed = rng.next_u64() ^ (worker_id as u64);

            std::thread::spawn(move || {
                let mut local_rng = StdRng::seed_from_u64(worker_seed);

                for _ in 0..OPS_PER_WORKER {
                    let op = local_rng.gen_range(0..100);
                    let key = generate_key(&mut local_rng);

                    if op < 45 {
                        // 45% Insert / Update
                        let val = local_rng.next_u64();
                        let mut o = oracle.lock().unwrap();
                        let oracle_prev = o.insert(key.clone(), val);
                        let map_prev = map.insert(key, val);
                        assert_eq!(
                            map_prev, oracle_prev,
                            "insert previous values must match oracle"
                        );
                    } else if op < 75 {
                        // 30% Get & Verification
                        let o = oracle.lock().unwrap();
                        let oracle_val = o.get(&key).copied();
                        let map_val = map.get(&key).map(|e| *e.value());
                        assert_eq!(map_val, oracle_val, "point get must match oracle");
                        assert_eq!(map.contains_key(&key), oracle_val.is_some());
                        assert_eq!(map.contains_key_slice(key.as_bytes()), oracle_val.is_some());
                    } else if op < 90 {
                        // 15% Remove
                        let mut o = oracle.lock().unwrap();
                        let oracle_removed = o.remove(&key);
                        let map_removed = map.remove(&key);
                        assert_eq!(
                            map_removed, oracle_removed,
                            "remove result must match oracle"
                        );
                    } else {
                        // 10% Get or insert default
                        let default_val = local_rng.next_u64();
                        let mut o = oracle.lock().unwrap();
                        let expected_val = *o.entry(key.clone()).or_insert(default_val);
                        let entry = map.get_or_insert_with(key, || default_val);
                        assert_eq!(
                            *entry, expected_val,
                            "get_or_insert_with must match oracle entry"
                        );
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("simulation worker thread panicked");
    }

    // Post-simulation validation against oracle
    let o = oracle.lock().unwrap();
    println!(
        "Simulation completed. Validating {} keys against oracle...",
        o.len()
    );
    assert_eq!(
        map.len(),
        o.len(),
        "final map length must match oracle length"
    );

    for (k, v) in o.iter() {
        assert_eq!(
            map.get(k).map(|e| *e.value()),
            Some(*v),
            "key {} must be present with matching value",
            k
        );
    }

    // Invariant check
    map.validate_invariants();

    // Range query validation against oracle
    let start_key = "users:account:000100";
    let end_key = "users:profile:000500";
    let oracle_range: Vec<_> = o
        .range(start_key.to_string()..end_key.to_string())
        .map(|(k, v)| (k.clone(), *v))
        .collect();
    let map_range: Vec<_> = map
        .range(start_key..end_key)
        .map(|e| (e.key().clone(), *e.value()))
        .collect();
    assert_eq!(
        map_range, oracle_range,
        "forward range scan must match oracle range"
    );

    let oracle_rev_range: Vec<_> = o
        .range(start_key.to_string()..end_key.to_string())
        .rev()
        .map(|(k, v)| (k.clone(), *v))
        .collect();
    let map_rev_range: Vec<_> = map
        .range(start_key..end_key)
        .rev()
        .map(|e| (e.key().clone(), *e.value()))
        .collect();
    assert_eq!(
        map_rev_range, oracle_rev_range,
        "reverse range scan must match oracle range"
    );

    println!(
        "All simulation validations passed successfully with seed: {}",
        seed
    );
}
