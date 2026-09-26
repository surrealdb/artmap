use artmap::arena::{Arena, ArenaArtMap, ArenaInserter};
use std::sync::{Arc, Barrier};
use std::thread;

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
    assert_eq!(map.insert("apple".to_string(), 999), Some(100));
    assert_eq!(map.get("apple"), Some(999));
    assert_eq!(map.len(), 3);

    // Remove
    assert_eq!(map.remove("banana"), Some(200));
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
fn test_arena_artmap_versioned_snapshot_reads() {
    let map = ArenaArtMap::<String, String>::with_capacity(16 * 1024 * 1024);

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
fn test_arena_artmap_concurrent_writes() {
    let arena = Arena::with_capacity(32 * 1024 * 1024);
    let map = Arc::new(ArenaArtMap::<[u8; 8], u64>::new(arena));

    const NUM_THREADS: usize = 8;
    const PER_THREAD: usize = 5000;
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
            if map.get(&k) != Some(start + i) {
                println!("MISSING KEY: {} (hex: {:02x?})", start + i, k);
                map.debug_lookup(&k);
                panic!("Key missing!");
            }
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
        arena = Arc::try_unwrap(arena_arc)
            .ok()
            .expect("exclusive arena Arc ownership");
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
    let mut inserter = ArenaInserter::new();

    // Insert 5000 sequential items with inserter
    for i in 0..5000 {
        map.insert_with_inserter(format!("seq:{i:05}"), i, &mut inserter);
    }
    assert_eq!(map.len(), 5000);

    for i in 0..5000 {
        assert_eq!(map.get(&format!("seq:{i:05}")), Some(i));
    }

    // Interleaved inserts with inserter
    inserter.reset();
    for i in 5000..6000 {
        map.insert_with_inserter(format!("interleaved:{i:05}"), i * 2, &mut inserter);
    }
    assert_eq!(map.len(), 6000);
}

#[test]
fn test_arena_artmap_scan_api() {
    let map = ArenaArtMap::<String, usize>::with_capacity(16 * 1024 * 1024);

    for i in 0..500 {
        map.insert(format!("user:{i:04}"), i);
    }

    let mut scanned = Vec::new();
    map.scan("user:0100".."user:0200", |k, v, _ver| {
        scanned.push((k.clone(), *v));
        true
    });
    assert_eq!(scanned.len(), 100);
    assert_eq!(scanned[0], ("user:0100".to_string(), 100));
    assert_eq!(scanned[99], ("user:0199".to_string(), 199));

    // Early termination
    let mut count = 0;
    map.scan("user:0000".., |_k, _v, _ver| {
        count += 1;
        count < 25
    });
    assert_eq!(count, 25);
}
