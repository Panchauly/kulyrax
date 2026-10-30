---
status: accepted
---

# File stores: a per-batch write-ahead log, applied without fsync, synced in the background

Task: [2026-10-30-file-store-durability](../tasks/2026-10-30-file-store-durability.md)

## Context

The three file-backed adapters (document, key-value, graph) write each event straight
to its per-key data and guard files, with no fsync anywhere. The source checkpoint is
written right after each batch, also without fsync. After a power loss or an OS crash
the checkpoint can be durable while the projections it covers are not: those events
are never projected again. After a process crash mid-event, the data file can hold a
new value while the guard still holds the old sequence.

Fsyncing every file per event is not affordable. Measured on this laptop (`fsbench`
microbenchmarks in the session scratchpad, files of 200 bytes, Defender exclusion
**not** active, see the task):

- Creating a file costs 250–800 µs and scales to about 4 threads (~370 µs per file
  at 4–16 threads).
- One fsync that has dirty data to flush costs **~45 ms**, a device cache flush,
  whether the file is new or overwritten. A 1 MB write plus fsync costs 30–80 ms.
- Fsyncing 4,000 freshly written files costs 1,216 µs per file on one thread and
  584 µs on 8. **Two seconds later**, once the OS has written most of them back on
  its own, it costs 42 µs per file on 8 threads.

An event touches two files (data + guard). Per-file fsync on the commit path would
cost milliseconds per event, against a budget of 500 µs (2,000 events/s).

Constraints: `decide()` and the backends' `exec::project` stay unchanged; one writer
per store directory; the checkpoint is never durable ahead of the projections it
covers; no tokio in `kulyrax-core`.

## Options considered

1. **Per-batch write-ahead log (the owner's design).** Decide every event in memory,
   collapse the batch to the final content of each file, write it as one batch log
   with one fsync, apply it to the per-key files without fsync, and fsync the touched
   files later, at a sync point that also deletes the logs it covers. Recovery
   replays the logs left over. Cost: one fsync per batch on the commit path, plus one
   fsync per distinct touched file per sync interval, off that path.
2. **Fsync each touched file at every batch commit, no log.** Simpler, but a crash
   mid-apply leaves a batch half applied with no record of the rest. Data and guard
   files are separate, so this needs ordering rules between them. It also pays
   every per-file fsync on the commit path: rejected on the numbers above.
3. **Move the file stores onto SQLite or one append-only file per store.** Durable
   and fast, but it changes the on-disk layout users read (one JSON file per entity).
   That is a product change, not a durability fix: out of scope.

## Evidence

- Verified (source): every file-backend write goes through three operations: read a
  file, write a whole file, remove a file (`adapter/{document/file.rs,
  keyvalue/store.rs, graph/store.rs}`). The graph store also lists the label
  directories under `.conduit/guard/nodes`. A log of "final content per path" can
  therefore express any batch for all three stores, and replaying it is idempotent.
- Verified (source): the runner commits the source position only after
  `dispatch_batch` returns `Committed`, i.e. after every adapter's `commit_batch`
  returned (`source/runner.rs`). If `commit_batch` returns only after the log is
  fsynced, the checkpoint cannot be durable ahead of the log.
- Verified (source): `kulyrax reload` builds and prepares the new adapters while the
  old ones are still alive, in the same process, and drops the old ones when it swaps
  them in (`source/runner.rs`, `Reloader`). A per-adapter OS lock would make every
  reload of a file store fail.
- Verified (measured): one 2 MB write plus fsync takes 34 ms. One log per 5,000-event
  batch is affordable on the commit path.
- Assumed: NTFS commits metadata changes (create, rename) through its journal in
  order. So when the checkpoint's rename is durable, the earlier batch-log rename is
  too. Windows has no portable directory fsync, so this is the argument for the
  Windows path. It has not been tested with a power cut.

## Decision

Option 1, as follows.

**Batch.** `begin_batch` opens an in-memory overlay. `handle` runs the unchanged
`exec::project` against a per-event scratch overlay that reads through to the batch
overlay, then a guard cache, then disk. On success the scratch merges into the batch;
on failure it is discarded, so a failed event leaves nothing behind. The overlay is
keyed by relative path, so a key written five times in a batch is logged and applied
once (collapse to final state). `handle` without an open batch (the one-event-at-a-time
fallback, `run --event`) is a batch of one.

**Commit** (`commit_batch`):
1. Serialize the overlay to `.conduit/wal/<seq>.log.tmp`: magic, body length,
   CRC-32 of the body, then one entry per path (write with full content, or remove).
   Fsync it, rename it to `<seq>.log`, then fsync `.conduit/wal` (Unix; a no-op on
   Windows, see Evidence).
2. Apply each entry to its file without fsync: whole-file writes, removes. More than
   64 entries are applied on up to 8 threads.
3. Return. The runner then writes the checkpoint, now `temp → fsync → rename → fsync
   dir` (`source/checkpoint.rs`).

**Sync point**, every 32 batches or 1 s, whichever comes first, checked after each
commit. The touched paths since the last sync point (one entry per path, however often
it was written) and the logs written since then go to a background thread. It first
waits until the job is 2 s old (`SYNC_DELAY`): by then the OS has written most of the
files back, and their fsyncs are cheap (Context). Then it fsyncs each file that still
exists, then each touched directory (Unix only), then deletes the logs oldest first.
On Windows a file's flush skips the device cache flush
(`NtFlushBuffersFileEx` with `FLUSH_FLAGS_NO_SYNC`), and one `FlushFileBuffers`
on the job's newest log then flushes the device cache for all of them, before any
log is deleted. Under load the 2 s delay was not enough: the OS had not written the
files back yet, so every per-file `FlushFileBuffers` was a device cache flush. The
sync point then ran at ~2 ms per file and throttled the whole store (task,
Verification). The owner chose this fix over keeping per-file fsync (2026-10-30). It
is the one `unsafe` block in `kulyrax-core`: an FFI call to `ntdll`, Windows only.
It assumes NTFS writes the file's metadata to the device on a `NO_SYNC` flush and
sends a device cache flush on every `FlushFileBuffers`, even for a clean file
(measured: 48 ms). Up to 4 jobs wait in the queue. A commit that finds the queue full blocks until it
can queue its own (back-pressure, so the logs on disk stay bounded). A store that is
dropped cleanly skips the wait, runs a last sync and waits for it.

**Recovery**, in `prepare`, after taking the lock and before anything is ingested:
delete leftover `*.log.tmp` files, validate every `*.log` (length and checksum), apply
them in sequence order, fsync what they touched, delete them. An invalid log that is
the newest is a torn write from a crash before its fsync completed. Its batch never
reached `commit_batch`'s return, so the checkpoint never moved past it: the log is
deleted and its events are re-read from the source. An invalid log with a valid log
after it cannot come from a crash (logs become visible only after their fsync), so it
is corruption: `prepare` fails and leaves the logs in place.

**Ordering invariants**, which the tests check:
- A log is durable before the batch is applied and before `commit_batch` returns, so
  before the checkpoint is written.
- A log is deleted only after every file it touched is fsynced, and logs are deleted
  oldest first. An old log left behind while a newer one is gone would replay stale
  content over newer state.
- A file rewritten by a later batch while a sync is running is in that later batch's
  log, which this sync does not delete.

**Single writer.** `prepare` takes an exclusive OS lock (`File::try_lock`) on
`.conduit/lock` in the store root. A second process fails at `prepare` with an error
naming the root. Within one process, every adapter on the same root shares one handle
(lock, log sequence, unsynced logs, sync thread). A reloaded adapter therefore joins
its predecessor instead of failing, and any sync covers every log on that root, in
order. `File::try_lock` is Rust 1.89, so the MSRV moves from 1.88 to 1.89.

**Guard cache.** Files under `.conduit/` (guards, graph incident indexes) are cached in
memory, as they are after each apply, including "absent". The single-writer lock is
what makes this safe. If `.conduit/` holds nothing but the log directory and the lock
at `prepare` (an empty store), a cache miss means absent and costs no disk read. Past
1,000,000 entries the cache is cleared and every miss goes back to disk.

**Failures.** If applying a durable log fails part-way (an I/O error such as a full
disk), or a background sync fails, the store stops. It deletes no further log, and
every later call fails with that error until a restart, whose recovery replays the
logs still on disk. Repairing in place was rejected. A sync point already queued
could fsync the half-applied files and delete their log before the repair ran,
losing the batch. Stopping has no such window. The events that fail meanwhile
follow the usual retry and dead-letter path, as any persistent write failure did
before.

**Without `prepare`.** Code that embeds a file adapter and calls `handle` directly
(the existing tests, and embedders, which file stores always allowed) gets the same
lock and recovery on first use, before its first write. `run`, `ingest`, `replay`
and `run --event` call `prepare` explicitly, so a lock conflict stops them before
anything is ingested.

**Crash test hook.** With the `crash-points` Cargo feature, `KULYRAX_CRASH_AT=<point>:<n>`
aborts the process the n-th time it reaches `after-log`, `mid-apply`,
`before-checkpoint`, `before-sync` or `mid-sync`. The feature is enabled only through
`kulyrax-cli`'s dev-dependencies, so release builds carry no abort path.

## Trade-offs accepted

- Each touched file is written twice, once in the log and once in place.
- Two processes can no longer write to one file store at the same time. That was
  never safe, and is now refused.
- MSRV 1.88 → 1.89.
- Windows directory durability rests on NTFS journal ordering (assumed, not tested by
  a power cut). Linux gets explicit directory fsyncs.
- Recovery time after a crash grows with the logs retained: about 3 s of batches
  (1 s to start a sync point plus its 2 s delay), more while the sync thread is
  behind, bounded by the 4-job queue.
- Each batch commit pays one fsync per file adapter (its log), plus one for the
  checkpoint. At ~45 ms per fsync on this laptop, that is a fixed ~90 ms per batch
  for one adapter: negligible at 5,000 events per batch, but it is the floor for lag.
- A fresh-store rebuild still writes and fsyncs every file once: the dedup only helps
  hot keys. This laptop's file-creation cost bounds file-store throughput regardless
  of this design (task, Verification).

- An `unsafe` FFI block in `kulyrax-core` (Windows only) for the deferred device
  flush.

## Revisit if

- Measurements show the per-file fsyncs at sync points dominate (unique-key rebuilds).
  On Linux, `syncfs` on the store's filesystem would replace them with one call.
- A user needs a store directory shared by several processes.
- A power-cut test on Windows loses a renamed log while a later checkpoint survives.
