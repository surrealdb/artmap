use artmap::versioned::VersionedArtMap;
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
fn test_versioned_artmap_basic_crud() {
    let map = VersionedArtMap::<String, String>::new();

    assert_eq!(map.len(), 0);
    assert!(map.is_empty());

    assert!(map.insert("account:1".to_string(), 100, "bal: 50".to_string()));
    assert!(map.insert("account:1".to_string(), 200, "bal: 80".to_string()));
    assert!(map.insert("account:1".to_string(), 300, "bal: 120".to_string()));
    assert_eq!(map.len(), 1);

    // Latest
    assert_eq!(map.get("account:1"), Some("bal: 120".to_string()));
    assert_eq!(
        map.get_latest("account:1"),
        Some((300, "bal: 120".to_string()))
    );

    // Snapshot reads
    assert_eq!(
        map.get_version_le("account:1", 350),
        Some((300, "bal: 120".to_string()))
    );
    assert_eq!(
        map.get_version_le("account:1", 250),
        Some((200, "bal: 80".to_string()))
    );
    assert_eq!(
        map.get_version_le("account:1", 150),
        Some((100, "bal: 50".to_string()))
    );
    assert_eq!(map.get_version_le("account:1", 50), None);

    assert!(map.contains_key("account:1"));
    assert!(!map.contains_key("account:2"));

    // Remove
    assert_eq!(map.remove("account:1"), Some("bal: 120".to_string()));
    assert_eq!(map.get("account:1"), None);
    assert_eq!(map.len(), 0);
}

#[test]
fn test_versioned_artmap_node_growth() {
    let map = VersionedArtMap::<String, usize>::new();

    for i in 0..200 {
        let k = format!("prefix:items:{i:03}");
        assert!(map.insert(k, 1, i));
    }

    assert_eq!(map.len(), 200);

    for i in 0..200 {
        let k = format!("prefix:items:{i:03}");
        assert_eq!(map.get(&k), Some(i));
        assert_eq!(map.get_version_le(&k, 1), Some((1, i)));
    }
}

#[test]
fn test_versioned_artmap_range_and_scan() {
    let map = VersionedArtMap::<String, u64>::new();

    for i in 0..100 {
        let k = format!("user:{i:04}");
        map.insert(k.clone(), 1, i as u64);
        // Add second version to even items
        if i % 2 == 0 {
            map.insert(k, 2, (i * 10) as u64);
        }
    }

    assert_eq!(map.len(), 100);

    // Scan with callback
    let mut scanned = Vec::new();
    map.scan("user:0010".."user:0020", |k, v, ver| {
        scanned.push((k.clone(), *v, ver));
        true
    });

    assert_eq!(scanned.len(), 10);
    assert_eq!(scanned[0], ("user:0010".to_string(), 100, 2));
    assert_eq!(scanned[1], ("user:0011".to_string(), 11, 1));

    // Bidirectional range
    let forward: Vec<String> = map
        .range("user:0010".."user:0015")
        .map(|e| e.key().clone())
        .collect();
    assert_eq!(
        forward,
        vec![
            "user:0010".to_string(),
            "user:0011".to_string(),
            "user:0012".to_string(),
            "user:0013".to_string(),
            "user:0014".to_string(),
        ]
    );

    let backward: Vec<String> = map
        .range("user:0010".."user:0015")
        .rev()
        .map(|e| e.key().clone())
        .collect();
    assert_eq!(
        backward,
        vec![
            "user:0014".to_string(),
            "user:0013".to_string(),
            "user:0012".to_string(),
            "user:0011".to_string(),
            "user:0010".to_string(),
        ]
    );
}

#[test]
fn test_versioned_artmap_concurrent_writes() {
    let map = Arc::new(VersionedArtMap::<String, usize>::new());
    let barrier = Arc::new(Barrier::new(8));

    let mut handles = Vec::new();
    for thread_id in 0..8 {
        let map = Arc::clone(&map);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            for i in 0..500 {
                let k = format!("thread:{thread_id}:item:{i:04}");
                map.insert(k, 1, i);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(map.len(), 4000);

    for thread_id in 0..8 {
        for i in 0..500 {
            let k = format!("thread:{thread_id}:item:{i:04}");
            assert_eq!(map.get(&k), Some(i));
            assert_eq!(map.get_version_le(&k, 1), Some((1, i)));
        }
    }
}
