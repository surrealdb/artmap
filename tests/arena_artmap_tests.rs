use artmap::arena::{Arena, ArenaArtMap, ArenaVersionedArtMap};
use std::sync::{Arc, Barrier};
use std::thread;

/// Loop sizes, scaled down under Miri.
const SEQ: usize = if cfg!(miri) { 100 } else { 5000 };
const BIG: usize = if cfg!(miri) { 200 } else { 10_000 };

#[test]
fn test_arena_artmap_basic_crud() {
    let map = ArenaArtMap::<String, u64>::with_capacity(16 * 1024 * 1024);

    assert_eq!(map.len(), 0);
    assert!(map.is_empty());

    assert_eq!(map.insert("apple".to_string(), 100), None);
    assert_eq!(map.insert("banana".to_string(), 200), None);
    assert_eq!(map.insert("cherry".to_string(), 300), None);
    assert_eq!(map.len(), 3);

    assert_eq!(map.get("apple"), Some(100));
    assert_eq!(map.get("banana"), Some(200));
    assert_eq!(map.get("cherry"), Some(300));
    assert_eq!(map.get("durian"), None);

    assert!(map.contains_key("apple"));
    assert!(!map.contains_key("durian"));

    // Update existing key
    assert_eq!(map.insert("apple".to_string(), 999).map(|e| *e), Some(100));
    assert_eq!(map.get("apple"), Some(999));
    assert_eq!(map.len(), 3);

    // Remove
    assert_eq!(map.remove("banana").map(|e| *e), Some(200));
    assert_eq!(map.get("banana"), None);
    assert_eq!(map.len(), 2);
}

#[test]
fn test_arena_artmap_node_growth() {
    // 16MB arena
    let map = ArenaArtMap::<String, usize>::with_capacity(16 * 1024 * 1024);

    // Insert 200 entries sharing the same prefix "prefix:items:" to trigger
    // Node4 -> Node16 -> Node48 -> Node256 growth
    for i in 0..200 {
        let k = format!("prefix:items:{i:03}");
        assert_eq!(map.insert(k, i), None);
    }

    assert_eq!(map.len(), 200);

    // Verify all 200 items exist and match
    for i in 0..200 {
        let k = format!("prefix:items:{i:03}");
        assert_eq!(map.get(&k), Some(i), "key {k} must match {i}");
    }
}

#[test]
fn test_arena_versioned_artmap_snapshot_reads() {
    let map = ArenaVersionedArtMap::<String, String>::with_capacity(16 * 1024 * 1024);

    // Insert multiple versions of the same key
    map.insert_versioned("account:1".to_string(), 100, "balance: 50".to_string());
    map.insert_versioned("account:1".to_string(), 200, "balance: 80".to_string());
    map.insert_versioned("account:1".to_string(), 300, "balance: 120".to_string());

    // Snapshot read at max_version = 350 returns newest version (300)
    let snap350 = map.get_version_le("account:1", 350).unwrap();
    assert_eq!(snap350.0, 300);
    assert_eq!(snap350.1, "balance: 120");

    // Snapshot read at max_version = 250 returns version 200
    let snap250 = map.get_version_le("account:1", 250).unwrap();
    assert_eq!(snap250.0, 200);
    assert_eq!(snap250.1, "balance: 80");

    // Snapshot read at max_version = 150 returns version 100
    let snap150 = map.get_version_le("account:1", 150).unwrap();
    assert_eq!(snap150.0, 100);
    assert_eq!(snap150.1, "balance: 50");

    // Snapshot read at max_version = 50 returns None
    assert!(map.get_version_le("account:1", 50).is_none());
}

#[test]
fn test_arena_versioned_artmap_out_of_order_versions() {
    let map = ArenaVersionedArtMap::<String, u64>::with_capacity(16 * 1024 * 1024);

    // Insert versions out of order: 10, then 30, then 5, then 20
    map.insert_versioned("k".to_string(), 10, 100);
    map.insert_versioned("k".to_string(), 30, 300);
    map.insert_versioned("k".to_string(), 5, 50);
    map.insert_versioned("k".to_string(), 20, 200);

    // Should find highest version <= 35 -> 30
    let v = map.get_version_le("k", 35).unwrap();
    assert_eq!(v.0, 30);
    assert_eq!(v.1, 300);

    // <= 25 -> 20
    let v = map.get_version_le("k", 25).unwrap();
    assert_eq!(v.0, 20);
    assert_eq!(v.1, 200);

    // <= 15 -> 10
    let v = map.get_version_le("k", 15).unwrap();
    assert_eq!(v.0, 10);
    assert_eq!(v.1, 100);

    // <= 8 -> 5
    let v = map.get_version_le("k", 8).unwrap();
    assert_eq!(v.0, 5);
    assert_eq!(v.1, 50);

    // <= 4 -> None
    assert!(map.get_version_le("k", 4).is_none());
}

#[test]
fn test_arena_versioned_artmap_successor_predecessor() {
    let map = ArenaVersionedArtMap::<String, u64>::with_capacity(16 * 1024 * 1024);

    map.insert_versioned("apple".to_string(), 1, 10);
    map.insert_versioned("cherry".to_string(), 1, 20);
    map.insert_versioned("date".to_string(), 1, 30);

    assert_eq!(map.first_entry().unwrap().key(), "apple");
    assert_eq!(map.last_entry().unwrap().key(), "date");

    // Successor
    assert_eq!(
        map.find_successor(b"banana", false).unwrap().key(),
        "cherry"
    );
    assert_eq!(map.find_successor(b"cherry", true).unwrap().key(), "cherry");
    assert_eq!(map.find_successor(b"cherry", false).unwrap().key(), "date");
    assert!(map.find_successor(b"date", false).is_none());

    // Predecessor
    assert_eq!(
        map.find_predecessor(b"banana", false).unwrap().key(),
        "apple"
    );
    assert_eq!(
        map.find_predecessor(b"cherry", true).unwrap().key(),
        "cherry"
    );
    assert_eq!(
        map.find_predecessor(b"cherry", false).unwrap().key(),
        "apple"
    );
    assert!(map.find_predecessor(b"apple", false).is_none());
}

#[test]
fn test_arena_artmap_concurrent_writes() {
    let arena = Arena::with_capacity(32 * 1024 * 1024);
    let map = Arc::new(ArenaArtMap::<[u8; 8], u64>::new(arena));

    const NUM_THREADS: usize = if cfg!(miri) { 2 } else { 8 };
    const PER_THREAD: usize = if cfg!(miri) { 50 } else { 5000 };
    let barrier = Arc::new(Barrier::new(NUM_THREADS));

    let mut handles = Vec::new();
    for t in 0..NUM_THREADS {
        let map = Arc::clone(&map);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let start = t as u64 * PER_THREAD as u64;
            for i in 0..PER_THREAD as u64 {
                let k = (start + i).to_be_bytes();
                map.insert(k, start + i);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(map.len(), NUM_THREADS * PER_THREAD);

    // Verify all entries are present
    for t in 0..NUM_THREADS {
        let start = t as u64 * PER_THREAD as u64;
        for i in 0..PER_THREAD as u64 {
            let k = (start + i).to_be_bytes();
            assert_eq!(
                map.get(&k),
                Some(start + i),
                "key {} ({k:02x?}) missing",
                start + i
            );
        }
    }
}

#[test]
fn test_arena_artmap_range_scan() {
    let map = ArenaArtMap::<String, usize>::with_capacity(16 * 1024 * 1024);

    for i in 0..1000 {
        map.insert(format!("item:{i:04}"), i);
    }
    assert_eq!(map.len(), 1000);

    // Full iteration in ascending order
    let items: Vec<(String, usize)> = map.iter().map(|e| (e.key().clone(), *e)).collect();
    assert_eq!(items.len(), 1000);
    for (idx, (k, v)) in items.iter().enumerate() {
        assert_eq!(k, &format!("item:{idx:04}"));
        assert_eq!(*v, idx);
    }

    // Sub-range: [100, 200)
    let sub: Vec<usize> = map.range("item:0100".."item:0200").map(|e| *e).collect();
    assert_eq!(sub.len(), 100);
    assert_eq!(sub.first(), Some(&100));
    assert_eq!(sub.last(), Some(&199));

    // Reverse range: [100, 110)
    let rev_sub: Vec<usize> = map
        .range("item:0100".."item:0110")
        .rev()
        .map(|e| *e)
        .collect();
    assert_eq!(
        rev_sub,
        vec![109, 108, 107, 106, 105, 104, 103, 102, 101, 100]
    );
}

#[test]
fn test_arena_artmap_reset() {
    let mut arena = Arena::new(4 * 1024 * 1024);
    assert!(arena.is_empty());

    {
        let arena_arc = Arc::new(arena);
        let map = ArenaArtMap::<String, usize>::new(Arc::clone(&arena_arc));

        for i in 0..1000 {
            map.insert(format!("key:{i:04}"), i);
        }
        assert_eq!(map.len(), 1000);
        assert!(map.arena().size() > 0);
        drop(map);

        // Drop map to reclaim unique Arc ownership
        arena =
            Arc::try_unwrap(arena_arc).unwrap_or_else(|_| panic!("exclusive arena Arc ownership"));
    }

    // Reset arena in O(1)
    arena.reset();
    assert!(arena.is_empty());

    // Re-populate new map on recycled arena
    let map2 = ArenaArtMap::<String, usize>::new(Arc::new(arena));
    for i in 0..1000 {
        map2.insert(format!("new_key:{i:04}"), i * 10);
    }
    assert_eq!(map2.len(), 1000);
    assert_eq!(map2.get("new_key:0500"), Some(5000));
}

#[test]
fn test_arena_artmap_inserter() {
    let map = ArenaArtMap::<String, usize>::with_capacity(16 * 1024 * 1024);
    let mut inserter = map.inserter();

    // Insert 5000 sequential items with inserter
    for i in 0..SEQ {
        inserter.insert(format!("seq:{i:05}"), i);
    }
    assert_eq!(map.len(), SEQ);

    for i in 0..SEQ {
        assert_eq!(map.get(&format!("seq:{i:05}")), Some(i));
    }

    // Interleaved inserts with a fresh inserter
    let mut inserter = map.inserter();
    for i in SEQ..SEQ + SEQ / 5 {
        inserter.insert(format!("interleaved:{i:05}"), i * 2);
    }
    assert_eq!(map.len(), SEQ + SEQ / 5);
}

#[test]
fn test_arena_artmap_scan_api() {
    let map = ArenaArtMap::<String, usize>::with_capacity(16 * 1024 * 1024);

    for i in 0..500 {
        map.insert(format!("user:{i:04}"), i);
    }

    let mut scanned = Vec::new();
    map.scan("user:0100".."user:0200", |k, v| {
        scanned.push((k.clone(), *v));
        true
    });
    assert_eq!(scanned.len(), 100);
    assert_eq!(scanned[0], ("user:0100".to_string(), 100));
    assert_eq!(scanned[99], ("user:0199".to_string(), 199));

    // Early termination
    let mut count = 0;
    map.scan("user:0000".., |_k, _v| {
        count += 1;
        count < 25
    });
    assert_eq!(count, 25);
}

#[test]
fn test_arena_versioned_artmap_scan_api() {
    let map = ArenaVersionedArtMap::<String, usize>::with_capacity(16 * 1024 * 1024);

    for i in 0..500 {
        map.insert_versioned(format!("user:{i:04}"), i as u64, i);
    }

    let mut scanned = Vec::new();
    map.scan("user:0100".."user:0200", |k, v, ver| {
        scanned.push((k.clone(), *v, ver));
        true
    });
    assert_eq!(scanned.len(), 100);
    assert_eq!(scanned[0], ("user:0100".to_string(), 100, 100));
    assert_eq!(scanned[99], ("user:0199".to_string(), 199, 199));
}

#[test]
fn test_arena_versioned_artmap_range_and_crud() {
    let map = ArenaVersionedArtMap::<String, u64>::with_capacity(16 * 1024 * 1024);

    assert!(map.is_empty());
    assert_eq!(map.len(), 0);

    map.insert_versioned("k1".to_string(), 1, 10);
    map.insert_versioned("k2".to_string(), 1, 20);
    map.insert_versioned("k3".to_string(), 1, 30);
    assert_eq!(map.len(), 3);

    // Overwrite k2 with version 2
    map.insert_versioned("k2".to_string(), 2, 25);

    assert_eq!(map.get("k2"), Some(25));
    assert_eq!(map.get_latest("k2"), Some((2, 25)));
    assert_eq!(map.get_version_le("k2", 1), Some((1, 20)));
    assert_eq!(map.get_version_le("k2", 2), Some((2, 25)));
    assert_eq!(map.get_version_le("k2", 3), Some((2, 25)));

    // Iterate
    let items: Vec<(String, u64, u64)> = map
        .iter()
        .map(|e| (e.key().clone(), *e.value(), e.version()))
        .collect();
    assert_eq!(items.len(), 3);
    assert_eq!(items[0], ("k1".to_string(), 10, 1));
    assert_eq!(items[1], ("k2".to_string(), 25, 2));
    assert_eq!(items[2], ("k3".to_string(), 30, 1));

    // Remove
    #[allow(deprecated)]
    let removed = map.remove("k2").map(|e| *e);
    assert_eq!(removed, Some(25));
    assert_eq!(map.get("k2"), None);
    assert_eq!(map.get_latest("k2"), None);
    assert_eq!(map.len(), 2);
}

#[test]
fn test_arena_versioned_artmap_10k_iteration() {
    let map = ArenaVersionedArtMap::<String, usize>::with_capacity(32 * 1024 * 1024);
    for i in 0..BIG {
        let k = format!("key_{i:08}");
        assert!(map.insert_versioned(k, 1, i));
    }
    assert_eq!(map.len(), BIG);
    let count = map.iter().count();
    assert_eq!(count, BIG);
}
