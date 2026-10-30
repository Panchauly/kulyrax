use std::path::PathBuf;
use std::sync::Arc;

use serde_json::Value;

use crate::{
    adapter::file_store::{FileStore, FileTxn},
    adapter::keyvalue::adapter::KvError,
    adapter::keyvalue::exec::{self, KvBackend, KvGuard, KvOutcome, KvPlan},
    adapter::keyvalue::runtime::KvRuntimeBuilder,
    adapter::{AdapterError, AdapterResult, SkipReason, StorageAdapter},
    event::Event,
    routing::StorageKind,
    runtime::config::MigrationPolicy,
    upcast::UpcasterRegistry,
};

/// File-backed key-value store (Phase 14). Since Phase 22.1 the write path
/// lives in [`exec::project`]; this file is the file-sidecar driver —
/// Since Phase 3 (v0.11) every write goes through a [`FileStore`]: batched,
/// logged, and crash-safe.
pub struct FileKvStore {
    id: String,
    priority: u32,
    store: FileStore,
    builder: KvRuntimeBuilder,
    upcasters: Arc<UpcasterRegistry>,
    migration_policy: MigrationPolicy,
}

impl FileKvStore {
    pub fn new(
        id: String,
        root: PathBuf,
        priority: u32,
        builder: KvRuntimeBuilder,
        upcasters: Arc<UpcasterRegistry>,
        migration_policy: MigrationPolicy,
    ) -> Self {
        Self {
            id,
            store: FileStore::new(root),
            priority,
            builder,
            upcasters,
            migration_policy,
        }
    }

    fn failure(&self, error: AdapterError) -> AdapterResult {
        AdapterResult::failure(self.id.clone(), StorageKind::KeyValue, error)
    }
}

impl StorageAdapter for FileKvStore {
    fn kind(&self) -> StorageKind {
        StorageKind::KeyValue
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn priority(&self) -> u32 {
        self.priority
    }

    fn prepare(&self) -> Result<(), AdapterError> {
        self.store.prepare()
    }

    fn begin_batch(&self, _events: &[&Event]) -> Result<(), AdapterError> {
        self.store.begin()
    }

    fn commit_batch(&self) -> Result<(), AdapterError> {
        self.store.commit()
    }

    fn rollback_batch(&self) {
        self.store.rollback()
    }

    fn handle(&self, event: &Event) -> AdapterResult {
        let projection = match self.builder.build(event, &self.upcasters) {
            Ok(p) => p,
            Err(KvError::UnsupportedVersion {
                from_version,
                to_version,
                reason,
                ..
            }) => {
                return match self.migration_policy {
                    MigrationPolicy::Strict => AdapterResult::unsupported_version(
                        self.id.clone(),
                        StorageKind::KeyValue,
                        reason,
                        from_version,
                        to_version,
                    ),
                    MigrationPolicy::IgnoreUnmatched => AdapterResult::skipped_versioned(
                        self.id.clone(),
                        StorageKind::KeyValue,
                        SkipReason::UnsupportedVersion,
                        from_version,
                        to_version,
                    ),
                };
            }
            Err(e) => return self.failure(AdapterError::WriteFailed(e.to_string())),
        };

        let source_version = projection.source_version;
        let projected_version = projection.projected_version;
        let plan = KvPlan {
            namespace: projection.namespace,
            entity_key: projection.entity_key,
            value: projection.value,
            on_existing: projection.on_existing,
            operation: projection.operation,
            permanent: projection.permanent,
            facet: projection.facet,
        };

        let result = self
            .store
            .with_txn(|txn| exec::project(&mut FileKvBackend { txn }, &plan, event));
        match result {
            Ok(Ok(KvOutcome::Created)) => AdapterResult::created_versioned(
                self.id.clone(),
                StorageKind::KeyValue,
                source_version,
                projected_version,
            ),
            Ok(Ok(KvOutcome::Updated)) => AdapterResult::updated_versioned(
                self.id.clone(),
                StorageKind::KeyValue,
                source_version,
                projected_version,
            ),
            Ok(Ok(KvOutcome::Deleted)) => AdapterResult::deleted_versioned(
                self.id.clone(),
                StorageKind::KeyValue,
                source_version,
                projected_version,
            ),
            Ok(Ok(KvOutcome::Skipped(reason))) => AdapterResult::skipped_versioned(
                self.id.clone(),
                StorageKind::KeyValue,
                reason,
                source_version,
                projected_version,
            ),
            Ok(Err(e)) => self.failure(AdapterError::WriteFailed(e.to_string())),
            Err(e) => self.failure(e),
        }
    }
}

// ---------------------------------------------------------------------------
// FileKvBackend — the driver
// ---------------------------------------------------------------------------

struct FileKvBackend<'a, 't> {
    txn: &'a mut FileTxn<'t>,
}

/// Value path, keyed by `(namespace, key)` (Phase 14.2).
fn value_path(namespace: &str, key: &str) -> String {
    format!("{namespace}/{key}.json")
}

/// Idempotency guard path, keyed by `(namespace, key)`.
fn guard_path(namespace: &str, key: &str) -> String {
    format!(".conduit/{namespace}/{key}.guard.json")
}

fn write_err(e: std::io::Error) -> KvError {
    KvError::WriteFailed(e.to_string())
}

impl FileKvBackend<'_, '_> {
    fn write_guard(&mut self, namespace: &str, key: &str, state: &KvGuard) -> Result<(), KvError> {
        let content =
            serde_json::to_string(state).map_err(|e| KvError::WriteFailed(e.to_string()))?;
        self.txn.write(&guard_path(namespace, key), content);
        Ok(())
    }
}

impl KvBackend for FileKvBackend<'_, '_> {
    /// `Ok(None)` means this key has never been projected here; a corrupt
    /// sidecar is a write failure, not treated as absent (silently forgetting
    /// `last_sequence`/`deleted` would let a stale event through, or
    /// un-delete a tombstoned key).
    fn read_guard(&mut self, namespace: &str, key: &str) -> Result<Option<KvGuard>, KvError> {
        match self
            .txn
            .read(&guard_path(namespace, key))
            .map_err(write_err)?
        {
            Some(content) => {
                let parsed: KvGuard = serde_json::from_str(&content)
                    .map_err(|e| KvError::WriteFailed(format!("corrupt guard sidecar: {e}")))?;
                Ok(Some(parsed))
            }
            None => Ok(None),
        }
    }

    fn read_value(&mut self, namespace: &str, key: &str) -> Result<Option<Value>, KvError> {
        Ok(self
            .txn
            .read(&value_path(namespace, key))
            .map_err(write_err)?
            .map(|s| serde_json::from_str(&s).unwrap_or(Value::Object(Default::default()))))
    }

    fn write(
        &mut self,
        namespace: &str,
        key: &str,
        value: Option<&Value>,
        guard: &KvGuard,
    ) -> Result<(), KvError> {
        if let Some(value) = value {
            self.txn.write(
                &value_path(namespace, key),
                serde_json::to_string_pretty(value).unwrap_or_default(),
            );
        }
        self.write_guard(namespace, key, guard)
    }

    fn delete(&mut self, namespace: &str, key: &str, guard: &KvGuard) -> Result<(), KvError> {
        self.txn.remove(&value_path(namespace, key));
        self.write_guard(namespace, key, guard)
    }
}
