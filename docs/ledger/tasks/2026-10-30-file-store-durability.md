---
status: blocked
---

# Phase 3: crash-safe, batched file stores

## Objective

Make the three file-backed stores (document, key-value, graph) crash-safe and
batched, per the owner's Phase 3 brief (2026-10-30, deadline 1 Nov): a per-batch
write-ahead log, a single writer per store, crash tests, benchmarks, docs and release
notes for v0.11.0. Design:
[2026-10-30-file-store-batch-log](../decisions/2026-10-30-file-store-batch-log.md).
Cutoff: if the crash tests aren't green by the end of 31 Oct, ship the file stores
labelled "development use, not crash-safe" instead.

Step 0 (done 2026-10-30): Postgres lag at 5,000/s passed (p99 877 ms,
[2026-10-28-batch-transactions](2026-10-28-batch-transactions.md)), so v0.10.0 was
merged to `main` as PR #11 (`8b25195`, all 10 checks green, owner-approved), and
`phase-3` was branched from it. `v0.10.0` is not tagged yet: a tag publishes release
binaries.

## Acceptance criteria

- [x] Per batch: `decide()` per event in memory, guards cached in memory for the
  store, each key collapsed to its final state.
- [x] The whole batch (data + guards) is written to one batch log, fsynced once, and
  renamed into place. The directory is fsynced, a no-op on Windows (documented).
- [x] The batch is applied to the per-key files without fsync.
- [x] The checkpoint is written temp → fsync → rename (+ directory fsync).
- [x] Logs are kept until a sync point (32 batches or 1 s). The sync point fsyncs
  each touched file once, then deletes the logs it covers, oldest first.
- [x] Recovery at start-up replays every log not covered by a sync point, before
  ingesting. Replay is idempotent (full-state writes).
- [x] Invariant: the checkpoint is never durable ahead of the batch log (the log is
  fsynced inside `commit_batch`, and the runner writes the checkpoint after it).
- [x] Single writer: an exclusive OS lock on `.conduit/lock` at `prepare()`. A second
  process fails fast with a clear error (tested in process and across processes).
- [x] Fault injection after the log write, mid-apply, before the checkpoint, before
  the sync point (plus mid-sync). Each restart equals the reference run: no
  duplicates, no missing keys.
- [x] Kill loop: child process killed at random points, 20 iterations, verified
  each time.
- [x] Torn/partial batch log detected (length + checksum) and discarded. Its events
  replay from the checkpoint.
- [ ] Benchmarks: 100k per file store, Defender excluded, batch 5,000, target
  ≥ 2,000 events/s on this laptop, per-stage numbers. Lag reruns at 1,000/s.
  **Run, target not met:** 849–944 events/s; lag at 1,000/s grows without bound
  (Verification). The 5,000/s Linux VM check is the owner's, later.
- [x] Docs: file-store durability model (`docs/file-store-durability.md`).
- [x] Release notes for v0.11.0 (`RELEASES/v0.11.0.md`, with the measured numbers).
- [x] Gates green before every push.

## Constraints and assumptions

- `decide()`, `dispatch.rs` and the dependency graph are unchanged, with no
  storage-kind special case. The three backends' `exec::project*` are unchanged; only
  their file drivers now go through `adapter/file_store.rs`.
- No tokio in core: the sync point runs on one `std::thread` per store root.
- Zero-panic rule: the crash hook calls `std::process::abort()`, but only behind the
  `crash-points` feature, which only `kulyrax-cli`'s dev-dependencies enable.
- MSRV 1.88 → 1.89 for `File::try_lock` (decision record).
- Owner's brief says "Defender excluded" for benchmarks. **The exclusion for
  `examples\benchmark\work` was removed on 2026-10-28 at 21:22** (Defender
  operational log, event 5007). The owner added it back on 2026-10-30 at 18:05,
  and the final benchmarks ran with it.

## Decisions

- [2026-10-30-file-store-batch-log](../decisions/2026-10-30-file-store-batch-log.md):
  the batch log design, sync delay, per-root shared handle, stop-on-failure, lazy
  prepare.

## Verification

Hardware as in the baselines (i5-10210U, 32 GB, LITEON NVMe, Windows 11, on AC
power).

**File-system costs** (`fsbench` microbenchmarks in the session scratchpad, 200-byte
files, Defender exclusion not active):

| Operation | Cost |
|---|---|
| Create a file, 1 / 4 / 16 threads | 802 / 375 / 357 µs per file (excluded dir); 538 / 254 / 247 (scratchpad) |
| One fsync with dirty data (new or overwritten file) | ~45 ms |
| 1 MB write + fsync | 30–80 ms |
| Fsync 4,000 freshly written files, 1 / 8 / 32 threads | 1,216 / 584 / 1,847 µs per file |
| The same, 2 s after writing, 1 / 8 threads | 252 / 42 µs per file |

These set the sync delay (2 s) and explain the crash suite's run times.

**Tests** (all observed):

| Check | Command | Result |
|---|---|---|
| Existing core suite on the new write path | `cargo test -p kulyrax-core` | 85 binaries, 270 passed, 0 failed (before the new tests) |
| Batch = one-at-a-time, all three stores, batch 1 / 4 / 100; rollback; failed event; clean shutdown; reopen; in-process sharing; lock | `cargo test -p kulyrax-core --test file_store_batch` | 7 passed |
| The equivalence test can fail | Mutation: `child_dirs` ignores the open batch | graph, batch 100: an edge to a node tombstoned earlier in the batch was created (`Created` vs `Skipped(EntityAbsent)`); reverted |
| Crash suite: after-log, mid-apply, before-checkpoint, before-sync, mid-sync, unsynced files wiped, torn newest log, corrupt middle log, second process, kill loop | `cargo test -p kulyrax-cli --test file_store_crash` | 10 passed in 177 s; kill loop seed `1793341161776353201`, 29 kills, 26 mid-run, every iteration equal to the reference |
| First run of the crash suite (batch 25) | same | 9 passed; the kill loop **failed its own check**: kill delays were drawn from a reference timed under contention, so almost every kill landed after the run had finished (every recovery still matched). Fixed: the loop learns the run time as it goes |
| fmt, clippy `-D warnings` | `cargo fmt --check`; `cargo clippy --workspace --all-targets -- -D warnings` | clean |
| Throughput, Defender **on** (exclusion missing), 100k events, batch 5,000, `e6f6202` | `bash run.sh file-{document,keyvalue,graph} 100000 1000` | document **60**, key-value **232**, graph **234** events/s (v0.10, Defender on: 159 / 218 / 143). All 100,000 entities verified each time. **Fail** against ≥ 2,000 |
| Where the time goes (key-value; the others are alike) | stage table, `results/file-*-phase3-defender-on.txt` | `apply` 20.5 s per 5,000-event batch; `sync` 26.9 s per job, in the background at the same time; `log` 131 ms and `checkpoint` 86 ms per batch. Under sustained load the OS does not write the files back within the 2 s delay, so each per-file fsync is a device cache flush (~2 ms per file). Those flushes stall `apply`, and the sync queue's back-pressure throttles commits to the per-file fsync rate |
| Cheaper per-file flush on Windows | `fsbench`, 4,000 dirty files, 8 threads | `sync_all` 315 µs/file; `NtFlushBuffersFileEx(FLUSH_FLAGS_NO_SYNC)` 32 µs/file, then one device flush 48 ms |
| Key-value throughput with the Windows `NO_SYNC` sync point (owner's choice), Defender on | `bash run.sh file-keyvalue 100000 1000` | **277** events/s, 100,000 verified. `sync` 845 ms per job (was 26.9 s), `log` 34 ms, `checkpoint` 59 ms per batch; `apply` now dominates at 17.7 s per 5,000-event batch (1.77 ms per file on 8 threads) |
| Why `apply` doesn't scale: Defender | `fsbench`, 5,000 files in `examples/benchmark/work`, 1 / 8 threads | no extension 624 / 393 µs per file; `.json` 734 / **713**; `.done` 899 / 611. Defender's real-time scan serializes `.json` creation, so a store with Defender scanning its directory is bounded at a few hundred events/s whatever the design |
| Defender exclusion restored by the owner | Defender operational log, event 5007 | `…\Exclusions\Paths\C:\Projects\kulyrax\examples\benchmark\work` added 2026-10-30 18:05:55 |
| **Throughput, exclusion on** (the brief's setup), 100k events, batch 5,000, `b4673af` | `bash run.sh file-{document,keyvalue,graph} 100000 1000` → `results/file-*-phase3-excluded.txt` | document **944**, key-value **859**, graph **849** events/s (v0.10, excluded: 393 / 430 / 360). All 100,000 entities verified each time. **Fail** against ≥ 2,000 |
| Where the time goes, exclusion on | same files, stage table | per 5,000-event batch: `apply` 5.0–5.7 s (~95%; 10,000 files, ~500 µs per file on 8 threads), `log` 27–54 ms, `checkpoint` 50–113 ms; `sync` 250–380 ms per job in the background. The durability overhead is ~3% of the batch; file creation is the bound |
| **Lag at 1,000/s**, exclusion on, 180 s | `bash lag.sh file-{document,keyvalue,graph} 1000` → `results/file-*-lag-1000-phase3.txt` | every store falls behind: p50 30.0 / 29.8 / 29.8 s, p99 66.8 / 60.1 / 61.8 s, max 68.6 / 61.8 / 63.6 s (document / key-value / graph); all 180,000 events verified. **Fail** |
| Whole workspace, before the first push | `cargo test --workspace --no-fail-fast` | 115 binaries, 373 passed, 0 failed (includes the crash suite) |

## Risks and open questions

- **Throughput misses 2,000 events/s on this laptop** (849–944 with the exclusion,
  2.0–2.4× v0.10), and lag at 1,000/s fails. The durability path costs ~3% of a
  batch; the rest is creating two files per event. Reaching the target here would
  need a different on-disk layout (fewer files per event), which is a product
  change outside this phase (decision record, option 3). **Owner's call:** accept
  the measured numbers for v0.11, or hold the release for the Linux VM check.
- With Defender scanning the store directory, throughput drops to 230–280
  events/s (Defender serializes `.json` creation). The release notes say so.
- The v0.10 Defender-on result files `results/file-{document,keyvalue,graph}.txt`
  were overwritten by this phase's runs (`results/` is gitignored, local only). Their
  headline numbers survive in
  [2026-10-26-benchmark-baselines](2026-10-26-benchmark-baselines.md); the
  per-stage detail is lost.
- **The 2,000 events/s target may not be reachable on this laptop.** Every event
  creates two files (data + guard). At ~370 µs per file created with 4–16 threads
  (Defender not excluded), that alone caps a store at about 1,300 events/s. This
  is a file-system cost, independent of the durability design.
- Windows directory durability rests on NTFS journal ordering (assumed, not tested
  with a power cut).
- The kill loop and fault points test process crashes. A power loss is simulated
  only by wiping the unsynced files, not by losing the page cache for real.

## Outcome

Blocked on the owner: every criterion is met except the throughput target
(≥ 2,000 events/s, and lag at 1,000/s), which this laptop's file-creation cost
rules out for the current on-disk layout. Crash safety is complete and tested. The
owner decides whether v0.11 ships with the measured numbers.
