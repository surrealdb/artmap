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

//! A memtable-style use of `ArenaVersionedArtMap`, the shape an LSM storage
//! engine depends on: versioned writes and deletes with admission control,
//! snapshot reads, range scans over every version, and size accounting.
//! CI runs it as the downstream canary.

use artmap::arena::{Arena, ArenaVersionedArtMap};

type Memtable = ArenaVersionedArtMap<Vec<u8>, Vec<u8>>;

/// Writes one entry if the arena still has room for it, as a memtable does
/// before deciding to rotate.
fn put(m: &Memtable, key: &[u8], version: u64, value: &[u8]) -> bool {
    if m.arena().remaining() < Memtable::max_insert_bytes(key.len()) {
        return false;
    }
    m.try_insert(key.to_vec(), version, value.to_vec()).is_ok()
}

fn main() {
    let memtable = Memtable::new(Arena::with_capacity(4 << 20));
    let mut version = 0u64;
    let mut written = 0usize;
    'fill: for round in 0..3u64 {
        for i in 0..10_000u64 {
            version += 1;
            let key = format!("user:{i:06}");
            if !put(&memtable, key.as_bytes(), version, &[round as u8; 64]) {
                break 'fill;
            }
            written += 1;
            if i % 97 == 0 {
                memtable.delete(key.into_bytes(), version + 1);
                version += 1;
            }
        }
    }
    println!(
        "wrote {written} entries: {} live keys, {} of {} arena bytes used",
        memtable.len(),
        memtable.arena().size(),
        memtable.arena().capacity(),
    );

    // Snapshot read, then a scan over every version of a key range.
    let snapshot = version / 2;
    let _ = memtable.get_version_le(&b"user:000001"[..], snapshot);
    let mut versions = 0usize;
    use std::ops::Bound::{Excluded, Included};
    for entry in
        memtable.range::<_, [u8]>((Included(&b"user:000100"[..]), Excluded(&b"user:000200"[..])))
    {
        for v in entry.versions() {
            assert!(v.version <= version);
            if let Some(value) = v.value {
                assert_eq!(value.len(), 64);
            }
            versions += 1;
        }
    }
    println!("scanned {versions} versions");

    // Once full, a write is refused cleanly and the caller gets its data back.
    loop {
        version += 1;
        match memtable.try_insert(b"overflow".to_vec(), version, vec![0; 4096]) {
            Ok(()) => continue,
            Err(full) => {
                assert_eq!(full.value.len(), 4096);
                break;
            }
        }
    }
    println!("arena full at {} bytes; rotating", memtable.arena().size());
}
