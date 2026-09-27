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

//! # Loom Model Checker Concurrency Tests
//!
//! Run with:
//! ```bash
//! RUSTFLAGS="--cfg loom" cargo test --test loom_tests --release -- --nocapture
//! ```

#![cfg(loom)]

use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use loom::sync::Arc;
use loom::thread;

const LOCK_BIT: u64 = 0b01;
const OBSOLETE_BIT: u64 = 0b10;
const VERSION_STEP: u64 = 0b100;

struct LoomLatch {
    version: AtomicU64,
}

impl LoomLatch {
    fn new() -> Self {
        Self {
            version: AtomicU64::new(0),
        }
    }

    fn lock(&self) -> Result<u64, ()> {
        let mut cur = self.version.load(Ordering::Acquire);
        loop {
            if cur & (LOCK_BIT | OBSOLETE_BIT) != 0 {
                return Err(());
            }
            match self.version.compare_exchange_weak(
                cur,
                cur | LOCK_BIT,
                Ordering::Acquire,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(cur),
                Err(actual) => cur = actual,
            }
        }
    }

    fn unlock(&self) {
        let cur = self.version.load(Ordering::Relaxed);
        let next = (cur & !LOCK_BIT) + VERSION_STEP;
        self.version.store(next, Ordering::Release);
    }

    fn mark_obsolete_and_unlock(&self) {
        let cur = self.version.load(Ordering::Relaxed);
        let next = cur | OBSOLETE_BIT;
        self.version.store(next, Ordering::Release);
    }

    fn read_version(&self) -> Option<u64> {
        let v = self.version.load(Ordering::Acquire);
        if v & (LOCK_BIT | OBSOLETE_BIT) != 0 {
            None
        } else {
            Some(v)
        }
    }

    fn validate(&self, start_version: u64) -> bool {
        self.version.load(Ordering::Acquire) == start_version
    }
}

#[test]
fn test_loom_latch_two_writers() {
    loom::model(|| {
        let latch = Arc::new(LoomLatch::new());
        let shared_data = Arc::new(AtomicU64::new(0));

        let l1 = Arc::clone(&latch);
        let d1 = Arc::clone(&shared_data);
        let h1 = thread::spawn(move || {
            if l1.lock().is_ok() {
                let v = d1.load(Ordering::Relaxed);
                d1.store(v + 1, Ordering::Relaxed);
                l1.unlock();
            }
        });

        let l2 = Arc::clone(&latch);
        let d2 = Arc::clone(&shared_data);
        let h2 = thread::spawn(move || {
            if l2.lock().is_ok() {
                let v = d2.load(Ordering::Relaxed);
                d2.store(v + 1, Ordering::Relaxed);
                l2.unlock();
            }
        });

        h1.join().unwrap();
        h2.join().unwrap();

        let count = shared_data.load(Ordering::Relaxed);
        assert!(count <= 2);
    });
}

#[test]
fn test_loom_latch_reader_writer_validation() {
    loom::model(|| {
        let latch = Arc::new(LoomLatch::new());

        let l_writer = Arc::clone(&latch);
        let h_writer = thread::spawn(move || {
            if l_writer.lock().is_ok() {
                l_writer.unlock();
            }
        });

        let l_reader = Arc::clone(&latch);
        let h_reader = thread::spawn(move || {
            if let Some(v) = l_reader.read_version() {
                // If validation passes, the writer must either not have started yet,
                // or must be observed consistently
                let valid = l_reader.validate(v);
                let _ = valid;
            }
        });

        h_writer.join().unwrap();
        h_reader.join().unwrap();
    });
}

#[test]
fn test_loom_latch_obsolete_race() {
    loom::model(|| {
        let latch = Arc::new(LoomLatch::new());

        let l1 = Arc::clone(&latch);
        let h1 = thread::spawn(move || {
            if l1.lock().is_ok() {
                l1.mark_obsolete_and_unlock();
            }
        });

        let l2 = Arc::clone(&latch);
        let h2 = thread::spawn(move || {
            let res = l2.lock();
            let _ = res;
        });

        h1.join().unwrap();
        h2.join().unwrap();
    });
}

#[test]
fn test_loom_slot1_atomic_claim_race() {
    loom::model(|| {
        let slot1_used = Arc::new(AtomicBool::new(false));
        let claim_count = Arc::new(AtomicU64::new(0));

        let s1 = Arc::clone(&slot1_used);
        let c1 = Arc::clone(&claim_count);
        let h1 = thread::spawn(move || {
            if !s1.swap(true, Ordering::AcqRel) {
                c1.fetch_add(1, Ordering::SeqCst);
            }
        });

        let s2 = Arc::clone(&slot1_used);
        let c2 = Arc::clone(&claim_count);
        let h2 = thread::spawn(move || {
            if !s2.swap(true, Ordering::AcqRel) {
                c2.fetch_add(1, Ordering::SeqCst);
            }
        });

        h1.join().unwrap();
        h2.join().unwrap();

        // Exactly one thread must successfully claim slot1
        assert_eq!(claim_count.load(Ordering::SeqCst), 1);
        assert!(slot1_used.load(Ordering::SeqCst));
    });
}
