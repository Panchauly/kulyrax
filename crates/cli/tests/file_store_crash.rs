//! Phase 3: the file stores survive a crash at any point.
//!
//! Each test runs the real `kulyrax` binary over a stream that writes to all
//! three file stores (document, key-value, graph), crashes it, runs it again
//! to completion, and compares every projected file with a run that never
//! crashed: nothing missing, nothing doubled, every guard identical.
//!
//! Crashes are injected two ways: at named points in the commit path
//! (`KULYRAX_CRASH_AT`, compiled in by the `crash-points` feature this crate's
//! dev-dependencies enable), and by killing the process at random moments
//! (`Child::kill`: SIGKILL on Unix, TerminateProcess on Windows).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

const ENTITIES: usize = 120;
const EVENTS: usize = 1_200;
const PER_FILE: usize = 60;
/// Small batches (12 per run), so a crash can land between batches and the
/// logs of several batches are on disk at once. Every batch pays an fsync per
/// store plus one for the checkpoint, so smaller batches make the suite slow.
const MAX_BATCH: &str = "100";
const STORES: [&str; 3] = ["store/doc", "store/kv", "store/graph"];

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_kulyrax")
}

fn write(path: &Path, content: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, content).unwrap();
}

/// A deterministic stream: creates, renames (hot keys), facets, deletes,
/// resurrections and edges, in files of `PER_FILE` events.
fn events() -> Vec<String> {
    let mut out = Vec::with_capacity(EVENTS);
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    for seq in 1..=EVENTS as u64 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        // Half the events go to a tenth of the entities: hot keys.
        let id = if x.is_multiple_of(2) {
            x as usize / 2 % (ENTITIES / 10)
        } else {
            x as usize / 2 % ENTITIES
        };
        let other = (id * 7 + 3) % ENTITIES;
        let (event_type, payload) = match (x >> 32) % 10 {
            0..=3 => (
                "UserCreated",
                serde_json::json!({ "id": format!("u{id}"), "name": format!("User {id} at {seq}") }),
            ),
            4 | 5 => (
                "UserEmailChanged",
                serde_json::json!({ "id": format!("u{id}"), "email": format!("u{id}-{seq}@x") }),
            ),
            6 | 7 => (
                "UserFollowed",
                serde_json::json!({ "id": format!("u{id}"), "other": format!("u{other}") }),
            ),
            _ => ("UserDeleted", serde_json::json!({ "id": format!("u{id}") })),
        };
        out.push(
            serde_json::json!({
                "event_id": format!("e{seq}"),
                "event_type": event_type,
                "payload": payload.to_string(),
                "metadata": {},
                "version": 1,
                "sequence": seq,
            })
            .to_string(),
        );
    }
    out
}

/// A project directory with config, routing, mappings and the event files.
fn project() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path();
    write(
        &p.join("kulyrax.yaml"),
        "version: 1
routing:
  file: routing.yaml
adapters:
  - type: file
    id: doc
    priority: 10
    config:
      root: ./store/doc
    capabilities: [write, upsert, delete]
  - type: keyvalue
    id: kv
    priority: 20
    config:
      root: ./store/kv
    capabilities: [write, upsert, delete]
  - type: graph
    id: graph
    priority: 30
    config:
      root: ./store/graph
    capabilities: [write, upsert, delete]
sources:
  - type: directory
    id: events
    path: ./events
    state_dir: ./state
",
    );
    write(
        &p.join("routing.yaml"),
        "UserCreated: [doc, kv, graph]
UserEmailChanged: [doc, kv, graph]
UserDeleted: [doc, kv, graph]
UserFollowed: [graph]
",
    );
    let m = p.join("mappings");
    std::fs::create_dir_all(m.join("sql")).unwrap();
    for (dir, store_key, body) in [
        ("document", "collection: users\nid: payload.id", "document"),
        ("keyvalue", "namespace: users\nkey: payload.id", "value"),
    ] {
        write(
            &m.join(dir).join("created.yaml"),
            &format!(
                "event: UserCreated\n{store_key}\nversion: 1\nfacet: profile\n{body}:\n  name: payload.name\n"
            ),
        );
        write(
            &m.join(dir).join("email.yaml"),
            &format!(
                "event: UserEmailChanged\n{store_key}\nversion: 1\nfacet: contact\n{body}:\n  email: payload.email\n"
            ),
        );
        write(
            &m.join(dir).join("deleted.yaml"),
            &format!(
                "event: UserDeleted\n{store_key}\nversion: 1\noperation: delete\n{body}: {{}}\n"
            ),
        );
    }
    let g = m.join("graph");
    write(
        &g.join("created.yaml"),
        "kind: node\nevent: UserCreated\nlabel: User\nversion: 1\nkey: payload.id\nfacet: profile\nproperties:\n  name: payload.name\n",
    );
    write(
        &g.join("email.yaml"),
        "kind: node\nevent: UserEmailChanged\nlabel: User\nversion: 1\nkey: payload.id\nfacet: contact\nproperties:\n  email: payload.email\n",
    );
    write(
        &g.join("deleted.yaml"),
        "kind: node\nevent: UserDeleted\nlabel: User\nversion: 1\nkey: payload.id\noperation: delete\n",
    );
    write(
        &g.join("followed.yaml"),
        "kind: edge\nevent: UserFollowed\nedge_type: FOLLOWS\nversion: 1\nfrom: payload.id\nto: payload.other\non_existing: replace\nproperties:\n  to: payload.other\n",
    );

    for (i, chunk) in events().chunks(PER_FILE).enumerate() {
        write(
            &p.join("events").join(format!("{:06}.ndjson", i + 1)),
            &(chunk.join("\n") + "\n"),
        );
    }
    dir
}

fn run_command(project: &Path) -> Command {
    let mut cmd = Command::new(bin());
    cmd.current_dir(project)
        .args(["run", "--config", "kulyrax.yaml", "--mappings", "mappings"])
        .args(["--once", "--max-batch", MAX_BATCH])
        .env_remove("KULYRAX_CRASH_AT")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd
}

fn run_to_completion(project: &Path) -> Output {
    let out = run_command(project).output().unwrap();
    assert!(
        out.status.success(),
        "run failed: {}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

/// Every projected file in the three stores, except the lock and the batch
/// logs.
fn stores(project: &Path) -> BTreeMap<String, String> {
    fn walk(base: &Path, dir: &Path, out: &mut BTreeMap<String, String>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            let rel = path
                .strip_prefix(base)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            if rel.ends_with(".conduit/lock") || rel.contains(".conduit/wal") {
                continue;
            }
            if path.is_dir() {
                walk(base, &path, out);
            } else {
                out.insert(rel, std::fs::read_to_string(&path).unwrap());
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(project, &project.join("store"), &mut out);
    out
}

fn logs(project: &Path, store: &str) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(project.join(store).join(".conduit/wal"))
        .map(|d| d.map(|e| e.unwrap().path()).collect())
        .unwrap_or_default();
    v.sort();
    v
}

fn all_logs(project: &Path) -> Vec<PathBuf> {
    STORES.iter().flat_map(|s| logs(project, s)).collect()
}

/// The files and run time of a run that never crashed.
fn reference() -> &'static (BTreeMap<String, String>, Duration) {
    static REFERENCE: OnceLock<(BTreeMap<String, String>, Duration)> = OnceLock::new();
    REFERENCE.get_or_init(|| {
        let p = project();
        let start = Instant::now();
        let out = run_to_completion(p.path());
        let took = start.elapsed();
        println!(
            "reference run: {took:?}\n{}",
            String::from_utf8_lossy(&out.stdout)
        );
        let files = stores(p.path());
        assert!(files.len() > ENTITIES, "the stream projects something");
        assert!(
            all_logs(p.path()).is_empty(),
            "a clean exit leaves no batch log"
        );
        (files, took)
    })
}

fn assert_matches_reference(project: &Path, what: &str) {
    let got = stores(project);
    let (expected, _) = reference();
    let missing: Vec<_> = expected.keys().filter(|k| !got.contains_key(*k)).collect();
    let extra: Vec<_> = got.keys().filter(|k| !expected.contains_key(*k)).collect();
    let differ: Vec<_> = expected
        .iter()
        .filter(|(k, v)| got.get(*k).is_some_and(|g| g != *v))
        .map(|(k, _)| k)
        .collect();
    assert!(
        missing.is_empty() && extra.is_empty() && differ.is_empty(),
        "{what}: missing {missing:?}, extra {extra:?}, different {differ:?}"
    );
    assert!(
        all_logs(project).is_empty(),
        "{what}: logs left after a clean exit"
    );
}

/// Crash at `point` the `n`-th time it is reached, then recover.
fn crash_at(point: &str, n: u32) -> tempfile::TempDir {
    let p = project();
    let out = run_command(p.path())
        .env("KULYRAX_CRASH_AT", format!("{point}:{n}"))
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success() && stderr.contains(&format!("crash point {point} reached")),
        "{point}:{n} did not crash the run (is the crash-points feature on?): {stderr}"
    );
    p
}

#[test]
fn crash_after_the_log_is_durable() {
    let p = crash_at("after-log", 7);
    assert!(
        !all_logs(p.path()).is_empty(),
        "the crashed batch's log is on disk"
    );
    run_to_completion(p.path());
    assert_matches_reference(p.path(), "after-log");
}

#[test]
fn crash_halfway_through_applying_a_batch() {
    let p = crash_at("mid-apply", 7);
    assert!(!all_logs(p.path()).is_empty());
    run_to_completion(p.path());
    assert_matches_reference(p.path(), "mid-apply");
}

#[test]
fn crash_after_applying_before_the_checkpoint() {
    let p = crash_at("before-checkpoint", 7);
    run_to_completion(p.path());
    assert_matches_reference(p.path(), "before-checkpoint");
}

#[test]
fn crash_before_a_sync_point() {
    let p = crash_at("before-sync", 1);
    assert!(
        !all_logs(p.path()).is_empty(),
        "unsynced logs are still on disk"
    );
    run_to_completion(p.path());
    assert_matches_reference(p.path(), "before-sync");
}

#[test]
fn crash_halfway_through_deleting_a_sync_points_logs() {
    let p = crash_at("mid-sync", 1);
    run_to_completion(p.path());
    assert_matches_reference(p.path(), "mid-sync");
}

#[test]
fn unsynced_files_lost_with_the_page_cache_are_restored() {
    // A power loss can lose files written since the last sync point, which
    // a process crash does not. Simulate it: crash before a sync point, then
    // truncate every file the unsynced logs touched.
    let p = crash_at("before-sync", 1);
    let mut wiped = 0;
    for (path, _) in stores(p.path()) {
        let full = p.path().join(&path);
        std::fs::write(&full, "").unwrap();
        wiped += 1;
    }
    assert!(wiped > 0);
    // Every file was truncated, synced or not: only the ones the logs cover
    // can come back, so check those the logs cover are restored rather than
    // the whole tree. Simpler and stricter: compare after re-running, which
    // must leave exactly the reference only if the logs covered every file
    // written since the store was created. That holds here because this
    // crash happens before the first sync point.
    run_to_completion(p.path());
    assert_matches_reference(p.path(), "wiped unsynced files");
}

#[test]
fn a_torn_newest_log_is_discarded_and_its_events_replayed() {
    let p = crash_at("after-log", 7);
    let newest = all_logs(p.path())
        .into_iter()
        .max_by_key(|l| std::fs::metadata(l).unwrap().modified().unwrap())
        .unwrap();
    // Which store's newest log it is doesn't matter; tear the newest log of
    // every store whose last batch is the crashed one.
    let len = std::fs::metadata(&newest).unwrap().len();
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(&newest)
        .unwrap();
    f.set_len(len / 2).unwrap();
    drop(f);
    run_to_completion(p.path());
    assert_matches_reference(p.path(), "torn log");
}

#[test]
fn a_corrupt_log_followed_by_a_valid_one_stops_the_run() {
    let p = crash_at("after-log", 9);
    let (store, oldest) = STORES
        .iter()
        .find_map(|s| {
            let l = logs(p.path(), s);
            (l.len() >= 2).then(|| (*s, l[0].clone()))
        })
        .expect("a store with two unsynced logs");
    let mut bytes = std::fs::read(&oldest).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    std::fs::write(&oldest, bytes).unwrap();

    let out = run_command(p.path()).output().unwrap();
    let all = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!out.status.success(), "{all}");
    assert!(
        all.contains("is corrupt and newer batch logs exist"),
        "{all}"
    );
    assert!(oldest.exists(), "{store}: the logs are left for inspection");
}

#[test]
fn a_second_process_on_the_same_store_fails_fast() {
    let p = project();
    let mut first = Command::new(bin())
        .current_dir(p.path())
        .args(["run", "--config", "kulyrax.yaml", "--mappings", "mappings"])
        .args(["--poll-interval-ms", "50"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    // Wait until the first process holds the lock and has caught up.
    let checkpoint = p.path().join("state/events.json");
    let deadline = Instant::now() + Duration::from_secs(60);
    while !checkpoint.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(checkpoint.exists(), "the first process started");

    let start = Instant::now();
    let out = run_command(p.path()).output().unwrap();
    let took = start.elapsed();
    first.kill().unwrap();
    first.wait().unwrap();

    let all = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!out.status.success(), "{all}");
    assert!(all.contains("in use by another process"), "{all}");
    assert!(took < Duration::from_secs(10), "fails fast, took {took:?}");
}

/// xorshift64, seeded from the clock unless `KULYRAX_CRASH_SEED` is set; the
/// seed is printed so a failing run can be repeated.
fn rng() -> impl FnMut() -> u64 {
    let seed = std::env::var("KULYRAX_CRASH_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
                | 1
        });
    println!("KULYRAX_CRASH_SEED={seed}");
    let mut x = seed;
    move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    }
}

#[test]
fn killed_at_random_twenty_times() {
    // How long a run takes here and now: the reference may have been timed
    // while other tests competed for the disk. Shrinks whenever a run
    // finishes before its kill, so the kills keep landing mid-run.
    let mut full_run = reference().1;
    let mut next = rng();
    let mut killed_mid_run = 0;
    for i in 0..20 {
        let p = project();
        // One or two kills, the second often during recovery.
        let kills = 1 + next() % 2;
        for k in 0..kills {
            let delay = Duration::from_micros(next() % (full_run.as_micros() as u64).max(1));
            let started = Instant::now();
            let mut child = run_command(p.path()).spawn().unwrap();
            let mut finished_after = None;
            while started.elapsed() < delay {
                if child.try_wait().unwrap().is_some() {
                    finished_after = Some(started.elapsed());
                    break;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            child.kill().ok();
            child.wait().unwrap();
            match finished_after {
                Some(t) => full_run = t,
                None => killed_mid_run += 1,
            }
            println!(
                "iteration {i}, kill {k}: after {delay:?}, finished first: {finished_after:?}"
            );
        }
        run_to_completion(p.path());
        assert_matches_reference(p.path(), &format!("iteration {i}"));
    }
    assert!(
        killed_mid_run >= 15,
        "most kills should land mid-run: {killed_mid_run}"
    );
}
