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

//! Removing, pruning and clearing the versioned maps (§11, §13): the same
//! suite for `VersionedArtMap` and `ArenaVersionedArtMap`.

use artmap::{ArenaVersionedArtMap, VersionedArtMap};

macro_rules! reclaim_suite {
    ($name:ident, $map:ty, $new:expr) => {
        mod $name {
            use super::*;

            type M = $map;

            fn map() -> M {
                $new
            }

            fn versions(m: &M, k: &str) -> Vec<(u64, Option<u64>)> {
                m.get_all_versions(k)
            }

            #[test]
            fn remove_key_removes_every_version_for_every_snapshot() {
                let mut m = map();
                m.insert("k".to_string(), 1, 10);
                m.insert("k".to_string(), 2, 20);
                m.delete("k".to_string(), 3);
                m.insert("other".to_string(), 1, 1);
                assert!(m.remove_key("k"));
                for v in 0..5 {
                    assert_eq!(m.get_version_le("k", v), None, "snapshot {v}");
                }
                assert_eq!(m.version_count("k"), 0);
                assert!(!m.contains_key("k"));
                assert!(!m.remove_key("k"), "a second remove finds nothing");
                assert!(!m.remove_key("absent"));
                assert_eq!(m.len(), 1);
                m.validate_invariants();
                // The key can come back.
                m.insert("k".to_string(), 7, 70);
                assert_eq!(versions(&m, "k"), [(7, Some(70))]);
                assert_eq!(m.len(), 2);
                m.validate_invariants();
            }

            #[test]
            fn remove_key_counts_only_a_live_head() {
                let mut m = map();
                m.insert("live".to_string(), 1, 1);
                m.insert("dead".to_string(), 1, 1);
                m.delete("dead".to_string(), 2);
                assert_eq!(m.len(), 1);
                assert!(m.remove_key("dead"));
                assert_eq!(m.len(), 1, "a deleted key was not counted");
                assert!(m.remove_key("live"));
                assert!(m.is_empty());
                m.validate_invariants();
            }

            #[test]
            fn remove_version_at_every_position() {
                let mut m = map();
                for v in 1..=4 {
                    m.insert("k".to_string(), v, v * 10);
                }
                assert!(m.remove_version("k", 3), "the middle");
                assert_eq!(
                    versions(&m, "k"),
                    [(4, Some(40)), (2, Some(20)), (1, Some(10))]
                );
                assert!(m.remove_version("k", 1), "the oldest");
                assert_eq!(versions(&m, "k"), [(4, Some(40)), (2, Some(20))]);
                assert!(m.remove_version("k", 4), "the newest");
                assert_eq!(m.get("k"), Some(20));
                assert!(!m.remove_version("k", 9), "absent version");
                assert!(!m.remove_version("k", 3), "already removed");
                assert!(!m.remove_version("absent", 1), "absent key");
                assert!(m.remove_version("k", 2), "the only version");
                assert_eq!(m.version_count("k"), 0);
                assert!(m.is_empty());
                m.validate_invariants();
            }

            #[test]
            fn remove_version_follows_the_head_liveness() {
                let mut m = map();
                m.insert("k".to_string(), 1, 10);
                m.delete("k".to_string(), 2);
                assert_eq!(m.len(), 0);
                // Removing the tombstone exposes version 1 again.
                assert!(m.remove_version("k", 2));
                assert_eq!(m.len(), 1);
                assert_eq!(m.get("k"), Some(10));
                // A tombstone under a live head does not count.
                m.delete("k".to_string(), 3);
                m.insert("k".to_string(), 4, 40);
                assert_eq!(m.len(), 1);
                assert!(m.remove_version("k", 3));
                assert_eq!(m.len(), 1);
                // A live head over a tombstone: removing it deletes the key.
                m.delete("k".to_string(), 2);
                assert!(m.remove_version("k", 4));
                assert!(m.remove_version("k", 1));
                assert_eq!(versions(&m, "k"), [(2, None)]);
                assert_eq!(m.len(), 0);
                m.validate_invariants();
            }

            #[test]
            fn prune_unlinks_keys_no_snapshot_can_see() {
                let mut m = map();
                // Deleted below the watermark: goes, with all its versions.
                m.insert("gone".to_string(), 1, 1);
                m.delete("gone".to_string(), 2);
                // Deleted above it: older snapshots still see version 3.
                m.insert("later".to_string(), 3, 3);
                m.delete("later".to_string(), 9);
                // Live: keeps the version visible at the watermark.
                m.insert("live".to_string(), 1, 1);
                m.insert("live".to_string(), 4, 4);
                assert_eq!(m.prune_all(5, |_| false), 2 + 1);
                assert_eq!(m.version_count("gone"), 0);
                assert_eq!(versions(&m, "later"), [(9, None), (3, Some(3))]);
                assert_eq!(versions(&m, "live"), [(4, Some(4))]);
                assert_eq!(m.len(), 1);
                m.validate_invariants();
                // At a later watermark, "later" goes too.
                assert_eq!(m.prune_key("later", 9, |_| false), 2);
                assert_eq!(m.version_count("later"), 0);
                let keys: Vec<String> = m.iter().map(|e| e.key().clone()).collect();
                assert_eq!(keys, ["live"]);
                m.validate_invariants();
            }

            #[test]
            fn prune_unlinks_user_tombstones() {
                let mut m = map();
                // 0 is the caller's own tombstone value.
                m.insert("k".to_string(), 1, 5);
                m.insert("k".to_string(), 2, 0);
                assert_eq!(m.len(), 1, "a user tombstone is a value to the map");
                assert_eq!(m.prune_key("k", 1, |v| *v == 0), 0, "not the newest");
                assert_eq!(m.prune_key("k", 2, |v| *v == 0), 2);
                assert_eq!(m.len(), 0);
                assert_eq!(m.version_count("k"), 0);
                m.validate_invariants();
            }

            #[test]
            fn handles_know_their_key_was_removed() {
                let m = map();
                m.insert("a".to_string(), 1, 1);
                m.insert("b".to_string(), 1, 2);
                m.insert("c".to_string(), 1, 3);
                m.insert("c".to_string(), 2, 4);
                let (a, b, c) = (
                    m.get_entry("a").unwrap(),
                    m.get_entry("b").unwrap(),
                    m.get_entry("c").unwrap(),
                );
                assert!(!a.is_superseded() && !b.is_superseded() && !c.is_superseded());
                assert!(m.remove_key("a"));
                m.delete("b".to_string(), 2);
                assert_eq!(m.prune_key("b", 2, |_| false), 2);
                assert!(m.remove_version("c", 2));
                assert!(a.is_superseded(), "removed with its key");
                assert!(b.is_superseded(), "pruned with its key");
                assert!(c.is_superseded(), "removed by version");
                // The handles still read what they captured.
                assert_eq!((*a.value(), *b.value(), *c.value()), (1, 2, 4));
                let c1 = m.get_entry("c").unwrap();
                assert_eq!(*c1.value(), 3);
                m.clear();
                assert!(c1.is_superseded(), "cleared");
            }

            #[test]
            fn clear_removes_every_key_and_version() {
                let mut m = map();
                for i in 0..200u64 {
                    let k = format!("key:{:03}", i);
                    m.insert(k.clone(), 1, i);
                    m.insert(k.clone(), 2, i + 1);
                    if i % 3 == 0 {
                        m.delete(k, 3);
                    }
                }
                m.clear();
                assert_eq!(m.len(), 0);
                assert!(m.iter().next().is_none());
                assert_eq!(m.version_count("key:001"), 0);
                assert_eq!(m.get_version_le("key:001", 1), None);
                m.validate_invariants();
                m.insert("key:001".to_string(), 5, 5);
                assert_eq!(versions(&m, "key:001"), [(5, Some(5))]);
                assert_eq!(m.len(), 1);
                m.validate_invariants();
                m.clear();
                m.clear();
                assert!(m.is_empty());
            }

            #[test]
            fn a_write_below_the_watermark_recreates_a_pruned_key() {
                // The documented edge of the watermark contract: once pruning
                // removes a key, a write below the watermark starts it afresh.
                let m = map();
                m.insert("k".to_string(), 1, 1);
                m.delete("k".to_string(), 5);
                assert_eq!(m.prune_key("k", 5, |_| false), 2);
                m.insert("k".to_string(), 3, 3);
                assert_eq!(m.get("k"), Some(3));
                assert_eq!(versions(&m, "k"), [(3, Some(3))]);
            }

            #[test]
            fn removing_and_pruning_keep_the_tree_compact() {
                // `validate_invariants` rejects empty nodes, so the leaves
                // these unlink leave no empty node behind (§13).
                let mut m = map();
                let n = if cfg!(miri) { 60 } else { 2_000 };
                for i in 0..n {
                    let k = format!("{:08}", i * 7919 % 100_003);
                    m.insert(k.clone(), 1, i);
                    match i % 3 {
                        0 => assert!(m.remove_key(k.as_str())),
                        1 => assert!(m.remove_version(k.as_str(), 1)),
                        _ => {
                            m.delete(k, 2);
                        }
                    }
                }
                m.validate_invariants();
                assert_eq!(m.len(), 0);
                m.prune_all(2, |_| false);
                assert!(m.iter().next().is_none());
                m.validate_invariants();
                assert_eq!(m.prune_all(u64::MAX, |_| false), 0, "nothing is left");
            }
        }
    };
}

reclaim_suite!(heap, VersionedArtMap<String, u64>, VersionedArtMap::new());
reclaim_suite!(
    arena,
    ArenaVersionedArtMap<String, u64>,
    ArenaVersionedArtMap::with_capacity(4 << 20)
);

#[test]
fn shrink_to_fit_keeps_every_version() {
    // Thin a versioned map to one key in fifty, fit it, and read every
    // remaining version back (§13). The arena map does not offer it.
    let mut m = VersionedArtMap::<String, u64>::new();
    let n = if cfg!(miri) { 200 } else { 20_000 };
    let key = |i: u64| format!("{:016x}", i.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    for i in 0..n {
        m.insert(key(i), 1, i);
        m.insert(key(i), 2, i + 1);
    }
    for i in (0..n).filter(|i| i % 50 != 0) {
        assert!(m.remove_key(key(i).as_str()));
    }
    m.shrink_to_fit();
    m.validate_invariants();
    assert_eq!(m.len(), n.div_ceil(50) as usize);
    for i in (0..n).step_by(50) {
        assert_eq!(
            m.get_all_versions(key(i).as_str()),
            [(2, Some(i + 1)), (1, Some(i))]
        );
    }
    m.insert(key(1), 3, 3);
    assert_eq!(m.get(key(1).as_str()), Some(3));
    m.validate_invariants();
}
