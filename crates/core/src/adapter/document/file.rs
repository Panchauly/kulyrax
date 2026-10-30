use std::path::PathBuf;
use std::sync::Arc;

use serde_json::Value;

use crate::{
    adapter::document::adapter::DocumentError,
    adapter::document::exec::{
        self, DocumentBackend, DocumentOutcome, DocumentPlan, ProjectionGuard,
    },
    adapter::document::runtime::DocumentRuntimeBuilder,
    adapter::file_store::{FileStore, FileTxn},
    adapter::{AdapterError, AdapterResult, SkipReason, StorageAdapter},
    event::Event,
    routing::StorageKind,
    runtime::config::MigrationPolicy,
    upcast::UpcasterRegistry,
};

/// File-based document adapter (Phase 4: idempotent; Phase 12: sequence-gated
/// upsert; Phase 13: sequence-gated delete & tombstones). Since Phase 23.1
/// the write path lives in [`exec::project`]; this file is the file-sidecar
/// driver. Since Phase 3 (v0.11) every write goes through a [`FileStore`]:
/// batched, logged, and crash-safe.
pub struct FileDocumentAdapter {
    id: String,
    priority: u32,
    store: FileStore,
    builder: DocumentRuntimeBuilder,
    upcasters: Arc<UpcasterRegistry>,
    migration_policy: MigrationPolicy,
}

impl FileDocumentAdapter {
    pub fn new(
        id: String,
        root: PathBuf,
        priority: u32,
        builder: DocumentRuntimeBuilder,
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
        AdapterResult::failure(self.id.clone(), StorageKind::Document, error)
    }
}

impl StorageAdapter for FileDocumentAdapter {
    fn kind(&self) -> StorageKind {
        StorageKind::Document
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
        // Build document projection (upcasting the payload to the mapping's target
        // version first). Entity identity is resolved as part of the projection
        // (Phase 11.1), so the idempotency guard below is checked by entity —
        // it can't be computed any earlier than this.
        let projection = match self.builder.build(event, &self.upcasters) {
            Ok(p) => p,
            Err(DocumentError::UnsupportedVersion {
                from_version,
                to_version,
                reason,
                ..
            }) => {
                return match self.migration_policy {
                    MigrationPolicy::Strict => AdapterResult::unsupported_version(
                        self.id.clone(),
                        StorageKind::Document,
                        reason,
                        from_version,
                        to_version,
                    ),
                    MigrationPolicy::IgnoreUnmatched => AdapterResult::skipped_versioned(
                        self.id.clone(),
                        StorageKind::Document,
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
        let plan = DocumentPlan {
            collection: projection.collection,
            entity_id: projection.entity_id,
            document: projection.document,
            on_existing: projection.on_existing,
            operation: projection.operation,
            permanent: projection.permanent,
            facet: projection.facet,
        };

        let result = self
            .store
            .with_txn(|txn| exec::project(&mut FileDocumentBackend { txn }, &plan, event));
        match result {
            Ok(Ok(DocumentOutcome::Created)) => AdapterResult::created_versioned(
                self.id.clone(),
                StorageKind::Document,
                source_version,
                projected_version,
            ),
            Ok(Ok(DocumentOutcome::Updated)) => AdapterResult::updated_versioned(
                self.id.clone(),
                StorageKind::Document,
                source_version,
                projected_version,
            ),
            Ok(Ok(DocumentOutcome::Deleted)) => AdapterResult::deleted_versioned(
                self.id.clone(),
                StorageKind::Document,
                source_version,
                projected_version,
            ),
            Ok(Ok(DocumentOutcome::Skipped(reason))) => AdapterResult::skipped_versioned(
                self.id.clone(),
                StorageKind::Document,
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
// FileDocumentBackend — the driver
// ---------------------------------------------------------------------------

struct FileDocumentBackend<'a, 't> {
    txn: &'a mut FileTxn<'t>,
}

/// Entity output path, keyed by `(collection, entity_id)` (Phase 13.3) — the
/// mapping's stable target identity, not `event.event_type`. A delete mapping
/// has its own event type (`OrderCancelled` vs `OrderCreated`) but the same
/// `collection`, so it points at the same file its create mapping wrote.
fn output_path(collection: &str, entity_id: &str) -> String {
    format!("{collection}/{entity_id}.json")
}

/// Idempotency guard path, keyed by `(collection, entity_id)` (Phase 11.3,
/// re-keyed by collection in Phase 13.3) — not `event_id` or `event_type`, so
/// every event type touching one entity shares one guard.
fn guard_path(collection: &str, entity_id: &str) -> String {
    format!(".conduit/entities/{collection}/{entity_id}.done")
}

fn write_err(e: std::io::Error) -> DocumentError {
    DocumentError::WriteFailed(e.to_string())
}

impl FileDocumentBackend<'_, '_> {
    fn write_guard(
        &mut self,
        collection: &str,
        entity_id: &str,
        state: &ProjectionGuard,
    ) -> Result<(), DocumentError> {
        let content =
            serde_json::to_string(state).map_err(|e| DocumentError::WriteFailed(e.to_string()))?;
        self.txn.write(&guard_path(collection, entity_id), content);
        Ok(())
    }
}

impl DocumentBackend for FileDocumentBackend<'_, '_> {
    /// `Ok(None)` means no entity has been projected here yet; a corrupt
    /// sidecar is a write failure, not treated as absent (silently
    /// forgetting `last_sequence`/`deleted` would let a stale event through,
    /// or un-delete a tombstoned entity).
    fn read_guard(
        &mut self,
        collection: &str,
        entity_id: &str,
    ) -> Result<Option<ProjectionGuard>, DocumentError> {
        match self
            .txn
            .read(&guard_path(collection, entity_id))
            .map_err(write_err)?
        {
            Some(content) => {
                let parsed: ProjectionGuard = serde_json::from_str(&content).map_err(|e| {
                    DocumentError::WriteFailed(format!("corrupt guard sidecar: {e}"))
                })?;
                Ok(Some(parsed))
            }
            None => Ok(None),
        }
    }

    fn write(
        &mut self,
        collection: &str,
        entity_id: &str,
        document: Option<&Value>,
        facet: &str,
        guard: &ProjectionGuard,
    ) -> Result<(), DocumentError> {
        if let Some(document) = document {
            let out_path = output_path(collection, entity_id);
            if !facet.is_empty() {
                // Phase 15.4: shallow-merge the facet's own top-level keys
                // into the existing document; every other key is preserved.
                let facet_obj = document.as_object().ok_or_else(|| {
                    DocumentError::BuildFailed("facet mapping must resolve to a JSON object".into())
                })?;
                let mut existing = match self.txn.read(&out_path).map_err(write_err)? {
                    Some(s) => serde_json::from_str::<Value>(&s)
                        .unwrap_or_else(|_| Value::Object(Default::default())),
                    None => Value::Object(Default::default()),
                };
                let existing_obj = existing.as_object_mut().ok_or_else(|| {
                    DocumentError::BuildFailed("existing document is not a JSON object".into())
                })?;
                for (k, v) in facet_obj {
                    existing_obj.insert(k.clone(), v.clone());
                }
                self.txn.write(
                    &out_path,
                    serde_json::to_string_pretty(&existing).unwrap_or_default(),
                );
            } else {
                self.txn.write(
                    &out_path,
                    serde_json::to_string_pretty(document).unwrap_or_default(),
                );
            }
        }
        self.write_guard(collection, entity_id, guard)
    }

    fn delete(
        &mut self,
        collection: &str,
        entity_id: &str,
        guard: &ProjectionGuard,
    ) -> Result<(), DocumentError> {
        self.txn.remove(&output_path(collection, entity_id));
        self.write_guard(collection, entity_id, guard)
    }
}
