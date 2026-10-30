//! Phase 3: the file stores write each batch through one batch log.
//!
//! A batch must leave exactly the files one-event-at-a-time dispatch leaves,
//! for all three file stores, including deletes, permanent tombstones,
//! resurrections, facet merges and cascades, and graph detach-deletes inside
//! one batch. A rolled-back batch leaves nothing behind, a failed event inside
//! a batch leaves nothing of itself, a clean shutdown leaves no batch log, and
//! a second writer on the same store is refused.
//!
//! Crash recovery is tested from outside the process, in
//! `crates/cli/tests/file_store_crash.rs`.

use kulyrax_core::adapter::document::file::FileDocumentAdapter;
use kulyrax_core::adapter::document::mapping::DocumentMapping;
use kulyrax_core::adapter::document::runtime::DocumentRuntimeBuilder;
use kulyrax_core::adapter::graph::mapping::GraphMapping;
use kulyrax_core::adapter::graph::runtime::GraphRuntimeBuilder;
use kulyrax_core::adapter::graph::store::GraphStore;
use kulyrax_core::adapter::keyvalue::mapping::KvMapping;
use kulyrax_core::adapter::keyvalue::runtime::KvRuntimeBuilder;
use kulyrax_core::adapter::keyvalue::store::FileKvStore;
use kulyrax_core::adapter::{AdapterOutcome, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

use std::collections::{BTreeMap, HashMap};
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::tempdir;

fn ev(sequence: u64, event_type: &str, id: &str, field: Option<(&str, &str)>) -> Event {
    let mut payload = serde_json::json!({ "id": id });
    if let Some((k, v)) = field {
        payload[k] = serde_json::Value::String(v.to_string());
    }
    Event {
        event_id: format!("{event_type}-{id}-{sequence}"),
        event_type: event_type.to_string(),
        payload: payload.to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence,
    }
}

/// Several entities, each taken through most of the guard rules, with
/// redeliveries, so that one batch holds many writes to the same files.
fn stream() -> Vec<Event> {
    let name = |n| Some(("name", n));
    let email = |e| Some(("email", e));
    let other = |o| Some(("other", o));
    vec![
        ev(1, "UserCreated", "u1", name("Ada")),
        ev(2, "UserEmailChanged", "u1", email("ada@x")),
        ev(3, "UserCreated", "u1", name("Ada L.")),
        ev(4, "UserCreated", "u2", name("Bob")),
        ev(5, "UserFollowed", "u1", other("u2")),
        ev(6, "UserFollowed", "u2", other("u1")),
        ev(7, "UserDeleted", "u1", None),
        // A facet and an edge on a deleted entity: absent.
        ev(8, "UserEmailChanged", "u1", email("gone@x")),
        ev(9, "UserFollowed", "u2", other("u1")),
        // Resurrection, then a stale redelivery of the first create.
        ev(10, "UserCreated", "u1", name("Ada again")),
        ev(1, "UserCreated", "u1", name("Ada")),
        ev(11, "UserEmailChanged", "u1", email("ada@new")),
        // u3: purged for good; a later create stays absent.
        ev(12, "UserCreated", "u3", name("Cy")),
        ev(13, "UserPurged", "u3", None),
        ev(14, "UserCreated", "u3", name("Cy 2")),
        // u4: delete before create.
        ev(16, "UserDeleted", "u4", None),
        ev(15, "UserCreated", "u4", name("Di")),
        ev(17, "UserCreated", "u5", name("Ed")),
    ]
}

fn yaml<T: serde::de::DeserializeOwned>(s: &str) -> T {
    serde_yaml::from_str(s).unwrap()
}

fn document_store(root: &Path) -> Box<dyn StorageAdapter> {
    let mut m: HashMap<String, DocumentMapping> = HashMap::new();
    m.insert("UserCreated".into(), yaml("event: UserCreated\ncollection: users\nversion: 1\nid: payload.id\nfacet: profile\ndocument:\n  name: payload.name\n"));
    m.insert("UserDeleted".into(), yaml("event: UserDeleted\ncollection: users\nversion: 1\nid: payload.id\noperation: delete\ndocument: {}\n"));
    m.insert("UserPurged".into(), yaml("event: UserPurged\ncollection: users\nversion: 1\nid: payload.id\noperation: delete\npermanent: true\ndocument: {}\n"));
    m.insert("UserEmailChanged".into(), yaml("event: UserEmailChanged\ncollection: users\nversion: 1\nid: payload.id\nfacet: contact\ndocument:\n  email: payload.email\n"));
    // Fails every time: `payload.missing` does not resolve to an id.
    m.insert("UserBroken".into(), yaml("event: UserBroken\ncollection: users\nversion: 1\nid: payload.missing\ndocument:\n  id: payload.id\n"));
    Box::new(FileDocumentAdapter::new(
        "doc".into(),
        root.to_path_buf(),
        10,
        DocumentRuntimeBuilder::new(m),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::IgnoreUnmatched,
    ))
}

fn kv_store(root: &Path) -> Box<dyn StorageAdapter> {
    let mut m: HashMap<String, KvMapping> = HashMap::new();
    m.insert("UserCreated".into(), yaml("event: UserCreated\nnamespace: users\nversion: 1\nkey: payload.id\nfacet: profile\nvalue:\n  name: payload.name\n"));
    m.insert("UserDeleted".into(), yaml("event: UserDeleted\nnamespace: users\nversion: 1\nkey: payload.id\noperation: delete\nvalue: {}\n"));
    m.insert("UserPurged".into(), yaml("event: UserPurged\nnamespace: users\nversion: 1\nkey: payload.id\noperation: delete\npermanent: true\nvalue: {}\n"));
    m.insert("UserEmailChanged".into(), yaml("event: UserEmailChanged\nnamespace: users\nversion: 1\nkey: payload.id\nfacet: contact\nvalue:\n  email: payload.email\n"));
    Box::new(FileKvStore::new(
        "kv".into(),
        root.to_path_buf(),
        10,
        KvRuntimeBuilder::new(m),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::IgnoreUnmatched,
    ))
}

fn graph_store(root: &Path) -> Box<dyn StorageAdapter> {
    let mut m: HashMap<String, GraphMapping> = HashMap::new();
    m.insert("UserCreated".into(), yaml("kind: node\nevent: UserCreated\nlabel: User\nversion: 1\nkey: payload.id\nfacet: profile\nproperties:\n  name: payload.name\n"));
    m.insert("UserDeleted".into(), yaml("kind: node\nevent: UserDeleted\nlabel: User\nversion: 1\nkey: payload.id\noperation: delete\n"));
    m.insert("UserPurged".into(), yaml("kind: node\nevent: UserPurged\nlabel: User\nversion: 1\nkey: payload.id\noperation: delete\npermanent: true\n"));
    m.insert("UserEmailChanged".into(), yaml("kind: node\nevent: UserEmailChanged\nlabel: User\nversion: 1\nkey: payload.id\nfacet: contact\nproperties:\n  email: payload.email\n"));
    m.insert("UserFollowed".into(), yaml("kind: edge\nevent: UserFollowed\nedge_type: FOLLOWS\nversion: 1\nfrom: payload.id\nto: payload.other\non_existing: replace\nproperties:\n  to: payload.other\n"));
    Box::new(GraphStore::new(
        "graph".into(),
        root.to_path_buf(),
        10,
        GraphRuntimeBuilder::new(m),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::IgnoreUnmatched,
    ))
}

type Build = fn(&Path) -> Box<dyn StorageAdapter>;
const STORES: [(&str, Build); 3] = [
    ("document", document_store),
    ("key-value", kv_store),
    ("graph", graph_store),
];

/// Every file under `root` and its content, except the lock and the batch
/// logs.
fn tree(root: &Path) -> BTreeMap<String, String> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, String>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            let rel = path
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            if rel == ".conduit/lock" || rel.starts_with(".conduit/wal") {
                continue;
            }
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                out.insert(rel, std::fs::read_to_string(&path).unwrap());
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}

fn wal_logs(root: &Path) -> Vec<PathBuf> {
    match std::fs::read_dir(root.join(".conduit/wal")) {
        Ok(entries) => entries.map(|e| e.unwrap().path()).collect(),
        Err(_) => Vec::new(),
    }
}

fn outcomes_one_at_a_time(adapter: &dyn StorageAdapter, events: &[Event]) -> Vec<String> {
    events
        .iter()
        .map(|e| format!("{:?}", adapter.handle(e).outcome))
        .collect()
}

fn outcomes_batched(adapter: &dyn StorageAdapter, events: &[Event], size: usize) -> Vec<String> {
    let mut out = Vec::new();
    for chunk in events.chunks(size) {
        let refs: Vec<&Event> = chunk.iter().collect();
        adapter.begin_batch(&refs).unwrap();
        for e in chunk {
            out.push(format!("{:?}", adapter.handle(e).outcome));
        }
        adapter.commit_batch().unwrap();
    }
    out
}

#[test]
fn a_batch_leaves_the_files_one_at_a_time_dispatch_leaves() {
    for (name, build) in STORES {
        for size in [1, 4, 100] {
            let (a, b) = (tempdir().unwrap(), tempdir().unwrap());
            let expected = {
                let adapter = build(a.path());
                adapter.prepare().unwrap();
                outcomes_one_at_a_time(adapter.as_ref(), &stream())
            };
            let got = {
                let adapter = build(b.path());
                adapter.prepare().unwrap();
                outcomes_batched(adapter.as_ref(), &stream(), size)
            };
            assert_eq!(got, expected, "{name}, batch {size}: outcomes");
            assert_eq!(
                tree(b.path()),
                tree(a.path()),
                "{name}, batch {size}: files"
            );
            assert!(!tree(a.path()).is_empty());
        }
    }
}

#[test]
fn a_rolled_back_batch_leaves_nothing() {
    for (name, build) in STORES {
        let dir = tempdir().unwrap();
        let adapter = build(dir.path());
        adapter.prepare().unwrap();
        let events = stream();
        let refs: Vec<&Event> = events.iter().collect();
        adapter.begin_batch(&refs).unwrap();
        for e in &events {
            adapter.handle(e);
        }
        adapter.rollback_batch();
        drop(adapter);
        assert!(
            tree(dir.path()).is_empty(),
            "{name}: {:?}",
            tree(dir.path())
        );
        assert!(wal_logs(dir.path()).is_empty(), "{name}");
    }
}

#[test]
fn a_failed_event_in_a_batch_leaves_nothing_of_itself() {
    let (a, b) = (tempdir().unwrap(), tempdir().unwrap());
    let good = vec![ev(1, "UserCreated", "u1", Some(("name", "Ada")))];
    {
        let adapter = document_store(a.path());
        outcomes_one_at_a_time(adapter.as_ref(), &good);
    }
    {
        let adapter = document_store(b.path());
        adapter.prepare().unwrap();
        let broken = ev(2, "UserBroken", "u1", None);
        adapter.begin_batch(&[&good[0], &broken]).unwrap();
        adapter.handle(&good[0]);
        let r = adapter.handle(&broken);
        assert!(
            matches!(r.outcome, AdapterOutcome::Failed(_)),
            "{:?}",
            r.outcome
        );
        adapter.commit_batch().unwrap();
    }
    assert_eq!(tree(b.path()), tree(a.path()));
}

#[test]
fn a_clean_shutdown_syncs_and_deletes_every_batch_log() {
    for (name, build) in STORES {
        let dir = tempdir().unwrap();
        {
            let adapter = build(dir.path());
            adapter.prepare().unwrap();
            outcomes_batched(adapter.as_ref(), &stream(), 3);
            assert!(
                !wal_logs(dir.path()).is_empty(),
                "{name}: logs are kept until a sync point"
            );
        }
        assert!(
            wal_logs(dir.path()).is_empty(),
            "{name}: {:?}",
            wal_logs(dir.path())
        );
        // Nothing left half-written either.
        assert!(
            tree(dir.path()).keys().all(|k| !k.contains(".tmp")),
            "{name}"
        );
    }
}

#[test]
fn a_reopened_store_sees_what_the_last_one_wrote() {
    // The guard cache starts complete on an empty store; a second adapter on
    // a store that is no longer empty must read the guards from disk.
    let dir = tempdir().unwrap();
    let events = stream();
    let (first, rest) = events.split_at(7);
    let expected = {
        let reference = tempdir().unwrap();
        let adapter = document_store(reference.path());
        outcomes_one_at_a_time(adapter.as_ref(), &events);
        drop(adapter);
        tree(reference.path())
    };
    {
        let adapter = document_store(dir.path());
        outcomes_batched(adapter.as_ref(), first, 100);
    }
    {
        let adapter = document_store(dir.path());
        adapter.prepare().unwrap();
        outcomes_batched(adapter.as_ref(), rest, 100);
    }
    assert_eq!(tree(dir.path()), expected);
}

#[test]
fn two_adapters_in_one_process_share_a_store_root() {
    // `kulyrax reload` prepares the new adapters while the old ones are alive.
    let dir = tempdir().unwrap();
    let old = document_store(dir.path());
    old.prepare().unwrap();
    old.handle(&ev(1, "UserCreated", "u1", Some(("name", "Ada"))));
    let new = document_store(dir.path());
    new.prepare().unwrap();
    drop(old);
    let r = new.handle(&ev(1, "UserCreated", "u1", Some(("name", "Ada"))));
    assert!(
        matches!(r.outcome, AdapterOutcome::Skipped(_)),
        "the new adapter sees the old one's guard: {:?}",
        r.outcome
    );
}

#[test]
fn a_second_writer_is_refused_at_prepare() {
    // Another process holding the lock, simulated with an OS lock on the
    // same file through a handle of our own (OS file locks conflict between
    // handles, in one process or two).
    let dir = tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join(".conduit")).unwrap();
    let lock_path = dir.path().join(".conduit/lock");
    let held = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .unwrap();
    held.try_lock().unwrap();

    for (name, build) in STORES {
        let adapter = build(dir.path());
        let err = adapter.prepare().expect_err("the store is locked");
        let msg = err.to_string();
        assert!(
            msg.contains("in use by another process") && msg.contains("lock"),
            "{name}: {msg}"
        );
        // An event fails rather than writing past the lock.
        let r = adapter.handle(&ev(1, "UserCreated", "u1", Some(("name", "Ada"))));
        assert!(matches!(r.outcome, AdapterOutcome::Failed(_)), "{name}");
    }
    drop(held);
    let adapter = document_store(dir.path());
    adapter
        .prepare()
        .expect("free once the other writer is gone");
}
