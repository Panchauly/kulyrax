# File stores: durability model

The file-backed adapters (`type: file` documents, `type: keyvalue`, `type: graph`)
keep one JSON file per entity, plus guard sidecars under `.conduit/`. Since v0.11 they
are **crash-safe**. After a process crash, an OS crash or a power loss, a restart
leaves exactly the files a run without the crash would have left: no projection
missing, none applied twice, every guard consistent with its data file.

This page explains how, what it costs, and what it assumes. Design and measurements:
[decision record](ledger/decisions/2026-10-30-file-store-batch-log.md).

## The guarantee

Kulyrax commits a source position (the checkpoint) only after every adapter has
committed the batch. For the file stores, "committed" means: the batch's writes are
on disk in a **batch log** that has been fsynced. Each checkpoint is itself written
to a temp file, fsynced, renamed into place, and its directory fsynced. So the
checkpoint is never durable ahead of the projections it covers.

On restart, the store replays every batch log still on disk before it reads a single
event. The source then resumes from its checkpoint. Any event that was already
projected is skipped by its guard (`Skipped(StaleSequence)`), exactly as for a
redelivery.

## What happens to a batch

1. **Decide in memory.** Each event goes through `decide()` as before, against the
   guards: from the batch itself, then an in-memory cache, then disk. Its writes are
   kept in memory, one entry per file with its final content. A key written five
   times in a batch is written once.
2. **Log.** The whole batch, data files and guard files, is written to
   `.conduit/wal/<n>.log` (via a temp file and a rename), with a length and a CRC-32
   checksum. **One fsync per batch.**
3. **Apply.** The batch is written to the per-key files, **without fsync**, on up to
   8 threads.
4. **Checkpoint.** The runner advances the source position (fsynced, as above).
5. **Sync point**, in the background, every 32 batches or 1 s. Once the batches it
   covers are 2 s old, the store fsyncs each file they touched, once however often
   it was written. Then it fsyncs their directories (Linux and macOS) and deletes
   the logs, oldest first. Waiting 2 s lets the OS write most files back on its own,
   which makes those fsyncs cheap. The commit path never waits for it unless 4 sync
   jobs are already queued.

A clean shutdown runs a final sync point and leaves no log behind.

## Recovery

At start-up (`run`, `ingest`, `replay`, `run --event`), each file store:

- deletes any `*.log.tmp` (a log whose batch never committed);
- checks every `*.log` against its length and checksum. **A torn newest log** comes
  from a crash during its write, before the batch committed. It is deleted, and its
  events are read again from the source. **A corrupt log followed by a valid one**
  cannot come from a crash: the store refuses to start and leaves the logs for
  inspection;
- writes every valid log's files again, oldest first. Each entry is a file's full
  content (or its removal), so replaying a log that was already applied changes
  nothing;
- fsyncs what it wrote and deletes the logs.

## One writer per store

Each store takes an exclusive OS lock on `.conduit/lock` when it starts. A second
Kulyrax process pointed at the same store directory fails at start-up:

```
file store ./store is in use by another process (lock ./store/.conduit/lock is held);
only one Kulyrax process may write to a file store
```

The lock is released when the process exits, however it exits. Inside one process,
adapters on the same directory share it, which is what lets `kulyrax reload` swap a
file adapter in place. The in-memory guard cache relies on this: no one else
changes the files underneath it.

Two different file adapters (say a document store and a key-value store) can share a
root directory, as before: their files never overlap.

## Platform notes

- **Linux, macOS:** directories are fsynced after a log rename, after a checkpoint
  rename, and at each sync point, so new and removed file names are durable too.
- **Windows:** a directory cannot be fsynced with the standard library, so that step
  is a no-op. Kulyrax relies on NTFS journaling its metadata changes (creates,
  renames) in order: if a checkpoint's rename survived, so did the earlier batch
  log's rename. This is the documented behaviour of NTFS's log but has not been
  verified with a power cut.

## What it costs

- One fsync per file adapter per batch (its log) plus one for the checkpoint, on the
  commit path. That is a fixed cost per batch, so it favours large batches: at
  the default of 5,000 events, it is under 20 µs per event even where one fsync
  takes 45 ms (measured on a Windows laptop). With very small batches it dominates.
- Every touched file is written twice: once in the log, once in place.
- Up to about 3 s of batches stay in logs, longer while the sync thread is behind.
  This is the most a crash ever has to replay.
- Memory: the open batch, plus one cached guard per entity seen (about 100 bytes plus
  the path), cleared past 1,000,000 entries.

## What it does not cover

- A disk that loses data it acknowledged as flushed, or a file corrupted after it was
  synced. Recovery detects a corrupt batch log, but nothing checks the per-key files.
- Hand edits to the store while Kulyrax runs. The guard cache will not see them.
- Files written by Kulyrax before v0.11: they were never fsynced. A store that was
  running at the moment of an OS crash under v0.10 may hold torn files. Rebuild it
  from the log if in doubt.
