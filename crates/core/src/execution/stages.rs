//! Wall-clock time spent in each stage of the source run loop, for finding
//! where projection time goes (`examples/benchmark`).
//!
//! The accumulator is thread-local rather than threaded through the call
//! chain: the stages it times sit behind `StorageAdapter::handle(&self,
//! &Event)` and the backend traits, which have no room for a timing argument.
//! Dispatch is synchronous and every backend call runs on the run-loop thread,
//! so one thread sees one run's stages. A run loop on another thread, such as
//! a parallel test, keeps its own totals.

use std::cell::RefCell;
use std::time::{Duration, Instant};

use serde::Serialize;

/// One timed stage. A call can count towards more than one: `Dispatch`
/// contains `Adapter` and `Batch`, which contain `Read`, `Write` and `Commit`.
/// `Schema` runs in `StorageAdapter::prepare`, before any dispatch.
#[derive(Debug, Clone, Copy)]
pub enum Stage {
    /// `EventSource::poll`: reading the source and parsing its events.
    SourceRead,
    /// Dispatching a whole batch to every routed adapter.
    Dispatch,
    /// One `StorageAdapter::handle` call.
    Adapter,
    /// Opening or ending one adapter's batch transaction:
    /// `StorageAdapter::begin_batch` (which loads the batch's guard lanes,
    /// timed as `Read`), `commit_batch` (`Commit`) or `rollback_batch`.
    Batch,
    /// Store set-up in `StorageAdapter::prepare`: the SQL guard table, the
    /// MongoDB guard index, the Neo4j guard constraint.
    Schema,
    /// Reads that feed `decide()` (guard lanes), plus the existing value a
    /// named key-value facet merges into.
    Read,
    /// The target write and the guard write (for document, key-value and
    /// graph backends these are one call).
    Write,
    /// The transaction commit: SQL, plus the MongoDB and Neo4j adapters, which
    /// time their own. The other backends commit inside `Write`.
    Commit,
    /// `EventSource::commit`: advancing the source checkpoint.
    Checkpoint,
    /// File stores, in `commit_batch`: writing the batch log and its fsync.
    Log,
    /// File stores, in `commit_batch`: writing the batch to the per-key
    /// files, without fsync.
    Apply,
    /// File stores: sync points, which fsync the touched files and delete
    /// the batch logs. They run on a background thread, beside dispatch, not
    /// inside it; a `commit_batch` that has to wait for one counts the wait
    /// here too.
    Sync,
}

/// Call count and total time for one stage.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct StageTotal {
    pub calls: u64,
    pub total_us: u64,
}

impl StageTotal {
    fn add(&mut self, elapsed: Duration) {
        self.calls += 1;
        self.total_us += elapsed.as_micros() as u64;
    }
}

/// Totals for every [`Stage`] over one run.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct StageTimings {
    pub source_read: StageTotal,
    pub dispatch: StageTotal,
    pub adapter: StageTotal,
    pub batch: StageTotal,
    pub schema: StageTotal,
    pub read: StageTotal,
    pub write: StageTotal,
    pub commit: StageTotal,
    pub checkpoint: StageTotal,
    pub log: StageTotal,
    pub apply: StageTotal,
    pub sync: StageTotal,
}

impl StageTimings {
    fn slot(&mut self, stage: Stage) -> &mut StageTotal {
        match stage {
            Stage::SourceRead => &mut self.source_read,
            Stage::Dispatch => &mut self.dispatch,
            Stage::Adapter => &mut self.adapter,
            Stage::Batch => &mut self.batch,
            Stage::Schema => &mut self.schema,
            Stage::Read => &mut self.read,
            Stage::Write => &mut self.write,
            Stage::Commit => &mut self.commit,
            Stage::Checkpoint => &mut self.checkpoint,
            Stage::Log => &mut self.log,
            Stage::Apply => &mut self.apply,
            Stage::Sync => &mut self.sync,
        }
    }
}

thread_local! {
    static TIMINGS: RefCell<StageTimings> = RefCell::new(StageTimings::default());
}

/// Run `f`, adding its wall-clock time to `stage` on this thread.
pub fn timed<T>(stage: Stage, f: impl FnOnce() -> T) -> T {
    let start = Instant::now();
    let out = f();
    let elapsed = start.elapsed();
    TIMINGS.with(|t| t.borrow_mut().slot(stage).add(elapsed));
    out
}

/// Add `calls` calls taking `elapsed` in total to `stage`, for work timed
/// elsewhere (the file stores' background sync thread).
pub fn record(stage: Stage, calls: u64, elapsed: Duration) {
    TIMINGS.with(|t| {
        let mut t = t.borrow_mut();
        let slot = t.slot(stage);
        slot.calls += calls;
        slot.total_us += elapsed.as_micros() as u64;
    });
}

/// Return this thread's totals and reset them to zero.
pub fn take() -> StageTimings {
    TIMINGS.with(|t| std::mem::take(&mut *t.borrow_mut()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timed_counts_calls_and_take_resets() {
        take();
        let v = timed(Stage::Read, || 7);
        timed(Stage::Read, || ());
        timed(Stage::Commit, || ());
        assert_eq!(v, 7);

        let t = take();
        assert_eq!(t.read.calls, 2);
        assert_eq!(t.commit.calls, 1);
        assert_eq!(t.write.calls, 0);
        assert_eq!(take(), StageTimings::default());
    }
}
