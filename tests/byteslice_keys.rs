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

//! `byteslice::ByteSlice` keys: inline and heap-backed keys, lookups by raw
//! bytes and through `ByteSlice::with_borrowed` views, and range scans.

use artmap::{ArenaArtMap, ArtMap, VersionedArtMap};
use byteslice::ByteSlice;

/// Short keys are stored inline in the `ByteSlice`; long ones on the heap.
fn keys() -> Vec<ByteSlice> {
    let mut keys: Vec<ByteSlice> = (0..32u8)
        .map(|i| ByteSlice::from(format!("k{i:02}").as_str()))
        .collect();
    keys.extend(
        (0..32u8)
            .map(|i| ByteSlice::from(format!("a-long-key-that-lives-on-the-heap-{i:02}").as_str())),
    );
    keys
}

#[test]
fn artmap_with_byteslice_keys() {
    let map = ArtMap::<ByteSlice, u64>::new();
    for (i, k) in keys().into_iter().enumerate() {
        assert!(map.insert(k, i as u64).is_none());
    }
    for (i, k) in keys().iter().enumerate() {
        // By `ByteSlice`, by raw bytes, and through a borrowed view.
        assert_eq!(map.get_value(k), Some(i as u64));
        assert_eq!(map.get_value(k.as_slice()), Some(i as u64));
        let probe = k.as_slice().to_vec();
        let found = ByteSlice::with_borrowed(&probe, |view| map.get_value(view));
        assert_eq!(found, Some(i as u64));
    }
    assert!(map.get_value(&ByteSlice::from("absent")).is_none());

    // Keys come back in byte order.
    let mut sorted = keys();
    sorted.sort();
    let scanned: Vec<ByteSlice> = map.iter().map(|e| e.key().clone()).collect();
    assert_eq!(scanned, sorted);
    let start = ByteSlice::from("k10");
    let end = ByteSlice::from("k20");
    assert_eq!(map.range::<_, ByteSlice>(&start..&end).count(), 10);
}

#[test]
fn versioned_and_arena_maps_with_byteslice_keys() {
    let versioned = VersionedArtMap::<ByteSlice, u64>::new();
    let arena = ArenaArtMap::<ByteSlice, u64>::with_capacity(1 << 20);
    for (i, k) in keys().into_iter().enumerate() {
        versioned.insert(k.clone(), 1, i as u64);
        versioned.insert(k.clone(), 2, i as u64 + 100);
        assert!(arena.insert(k, i as u64).is_none());
    }
    for (i, k) in keys().iter().enumerate() {
        assert_eq!(versioned.get_version_le(k, 1), Some((1, i as u64)));
        assert_eq!(versioned.get(k.as_slice()), Some(i as u64 + 100));
        assert_eq!(arena.get(k), Some(i as u64));
        let probe = k.as_slice().to_vec();
        let found = ByteSlice::with_borrowed(&probe, |view| arena.get(view));
        assert_eq!(found, Some(i as u64));
    }
    assert_eq!(versioned.len(), keys().len());
    assert_eq!(arena.len(), keys().len());
}
