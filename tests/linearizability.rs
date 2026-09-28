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

//! A Wing–Gong linearizability checker (§16.5): short concurrent histories of
//! point operations and `clear`, recorded with invocation and response
//! timestamps, must have a sequential order that respects real time and is
//! valid for a `BTreeMap`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};

use artmap::{ArenaArtMap, ArtMap};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

#[derive(Clone, Copy, Debug)]
enum Op {
    Insert(u8, u64),
    Remove(u8),
    Get(u8),
    Clear,
}

#[derive(Clone, Copy, Debug)]
struct Event {
    op: Op,
    /// The value returned: the displaced or read value, if any.
    ret: Option<u64>,
    call: u64,
    ret_at: u64,
}

/// Applies `op` to the model, returning what a correct map would return.
fn apply(model: &mut BTreeMap<u8, u64>, op: Op) -> Option<u64> {
    match op {
        Op::Insert(k, v) => model.insert(k, v),
        Op::Remove(k) => model.remove(&k),
        Op::Get(k) => model.get(&k).copied(),
        Op::Clear => {
            model.clear();
            None
        }
    }
}

/// Searches for a linearization of `events` (Wing & Gong), with memoisation
/// of visited (done-set, model) states.
fn linearizable(events: &[Event]) -> bool {
    fn go(
        events: &[Event],
        done: u64,
        model: &mut BTreeMap<u8, u64>,
        seen: &mut std::collections::HashSet<(u64, Vec<(u8, u64)>)>,
    ) -> bool {
        if done.count_ones() as usize == events.len() {
            return true;
        }
        if !seen.insert((done, model.iter().map(|(k, v)| (*k, *v)).collect())) {
            return false;
        }
        // A pending event may go next if no other pending event responded
        // before it was invoked (real-time order).
        let min_ret = events
            .iter()
            .enumerate()
            .filter(|(i, _)| done & (1 << i) == 0)
            .map(|(_, e)| e.ret_at)
            .min()
            .unwrap_or(u64::MAX);
        for (i, e) in events.iter().enumerate() {
            if done & (1 << i) != 0 || e.call > min_ret {
                continue;
            }
            let mut next = model.clone();
            if apply(&mut next, e.op) == e.ret && go(events, done | (1 << i), &mut next, seen) {
                return true;
            }
        }
        false
    }
    go(events, 0, &mut BTreeMap::new(), &mut Default::default())
}

trait Target: Send + Sync + 'static {
    fn run(&self, op: Op) -> Option<u64>;
}

impl Target for ArtMap<[u8; 1], u64> {
    fn run(&self, op: Op) -> Option<u64> {
        match op {
            Op::Insert(k, v) => self.insert([k], v).map(|e| *e),
            Op::Remove(k) => self.remove(&[k]).map(|e| *e),
            Op::Get(k) => self.get(&[k]).map(|e| *e),
            Op::Clear => {
                self.clear();
                None
            }
        }
    }
}

impl Target for ArenaArtMap<[u8; 1], u64> {
    fn run(&self, op: Op) -> Option<u64> {
        match op {
            Op::Insert(k, v) => self.insert([k], v).map(|e| *e),
            Op::Remove(k) => self.remove(&[k]).map(|e| *e),
            Op::Get(k) => self.get(&[k]),
            Op::Clear => unreachable!("the arena map has no clear"),
        }
    }
}

fn check<T: Target>(make: impl Fn() -> T, with_clear: bool) {
    let rounds = if cfg!(miri) { 3 } else { 300 };
    let threads = 3;
    let per = if cfg!(miri) { 3 } else { 5 };
    for round in 0..rounds {
        let map = Arc::new(make());
        let clock = Arc::new(AtomicU64::new(0));
        let log = Arc::new(Mutex::new(Vec::new()));
        let barrier = Arc::new(Barrier::new(threads));
        let hs: Vec<_> = (0..threads)
            .map(|t| {
                let (map, clock, log, barrier) =
                    (Arc::clone(&map), Arc::clone(&clock), Arc::clone(&log), Arc::clone(&barrier));
                std::thread::spawn(move || {
                    let mut rng = StdRng::seed_from_u64(round * 31 + t as u64);
                    let ops: Vec<Op> = (0..per)
                        .map(|i| {
                            // Two keys, which share a Node4 once both exist.
                            let k = rng.gen_range(0..2u8);
                            match rng.gen_range(0..10) {
                                0..=3 => Op::Insert(k, (t * 100 + i) as u64 + 1),
                                4..=5 => Op::Remove(k),
                                9 if with_clear => Op::Clear,
                                _ => Op::Get(k),
                            }
                        })
                        .collect();
                    barrier.wait();
                    let mut mine = Vec::new();
                    for op in ops {
                        let call = clock.fetch_add(1, Ordering::SeqCst);
                        let ret = map.run(op);
                        let ret_at = clock.fetch_add(1, Ordering::SeqCst);
                        mine.push(Event { op, ret, call, ret_at });
                    }
                    log.lock().unwrap().extend(mine);
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        let events = log.lock().unwrap().clone();
        assert!(linearizable(&events), "not linearizable (round {round}): {events:#?}");
    }
}

#[test]
fn artmap_point_operations_and_clear_are_linearizable() {
    check(ArtMap::<[u8; 1], u64>::new, true);
}

#[test]
fn arena_point_operations_are_linearizable() {
    check(|| ArenaArtMap::<[u8; 1], u64>::with_capacity(1 << 20), false);
}

#[test]
fn the_checker_rejects_a_non_linearizable_history() {
    // A get that returns a value no one ever wrote.
    let events = [
        Event { op: Op::Insert(0, 1), ret: None, call: 0, ret_at: 1 },
        Event { op: Op::Get(0), ret: Some(7), call: 2, ret_at: 3 },
    ];
    assert!(!linearizable(&events));
    // A stale read after a completed overwrite.
    let events = [
        Event { op: Op::Insert(0, 1), ret: None, call: 0, ret_at: 1 },
        Event { op: Op::Insert(0, 2), ret: Some(1), call: 2, ret_at: 3 },
        Event { op: Op::Get(0), ret: Some(1), call: 4, ret_at: 5 },
    ];
    assert!(!linearizable(&events));
}
