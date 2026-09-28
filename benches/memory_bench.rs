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

//! Live-memory measurements for the safety plan (§16.7): retention under
//! sliding-window churn, and memory held while a long scan pins the epoch.
//! Run with `cargo bench --bench memory_bench`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use artmap::ArtMap;

struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

// SAFETY: forwards to the system allocator and only adds relaxed counters.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            let now = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn live() -> usize {
    LIVE.load(Ordering::Relaxed)
}

/// Drives epoch advancement so deferred destructors run.
fn flush() {
    for _ in 0..1024 {
        crossbeam_epoch::pin().flush();
    }
}

fn sliding_window() {
    let base = live();
    let map = ArtMap::<[u8; 8], u64>::new();
    const WINDOW: u64 = 1_000;
    const OPS: u64 = 4_000_000;
    for i in 0..OPS / 2 {
        let _ = map.insert(i.to_be_bytes(), i);
        if i >= WINDOW {
            let _ = map.remove(&(i - WINDOW).to_be_bytes());
        }
    }
    flush();
    println!(
        "sliding_window live_bytes={} len={}",
        live().saturating_sub(base),
        map.len()
    );
}

fn long_scan_with_churn() {
    let base = live();
    let map = Arc::new(ArtMap::<[u8; 8], u64>::new());
    for i in 0..100_000u64 {
        let _ = map.insert(i.to_be_bytes(), i);
    }
    let stop = Arc::new(AtomicBool::new(false));
    let churn = {
        let map = Arc::clone(&map);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut i = 100_000u64;
            while !stop.load(Ordering::Relaxed) {
                let _ = map.insert(i.to_be_bytes(), i);
                let _ = map.remove(&(i - 100_000).to_be_bytes());
                i += 1;
            }
        })
    };
    let mut peak = 0usize;
    // A slow scan holds the epoch pinned for the whole iteration.
    let mut n = 0u64;
    // Bounded to the initial keys, so the scan does not chase the churn.
    for (i, e) in map.range(..100_000u64.to_be_bytes()).enumerate() {
        n += *e.value();
        if i % 1000 == 0 {
            std::thread::sleep(Duration::from_micros(200));
            peak = peak.max(live().saturating_sub(base));
        }
    }
    stop.store(true, Ordering::Relaxed);
    churn.join().unwrap();
    flush();
    println!(
        "long_scan_churn peak_live_bytes={} after_flush={} (checksum {})",
        peak,
        live().saturating_sub(base),
        n
    );
}

fn main() {
    // `cargo bench` passes `--bench`; ignore arguments.
    sliding_window();
    long_scan_with_churn();
    println!("peak_process_bytes={}", PEAK.load(Ordering::Relaxed));
}
