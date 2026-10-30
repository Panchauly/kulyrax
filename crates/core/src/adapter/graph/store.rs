use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    adapter::file_store::{FileStore, FileTxn},
    adapter::graph::adapter::GraphError,
    adapter::graph::exec::{self, EdgePlan, GraphBackend, GraphGuard, GraphOutcome, NodePlan},
    adapter::graph::mapping::{ResolvedRecord, canonical_edge_key, encode_edge_key},
    adapter::graph::runtime::GraphRuntimeBuilder,
    adapter::{AdapterError, AdapterResult, SkipReason, StorageAdapter},
    event::Event,
    routing::StorageKind,
    runtime::config::MigrationPolicy,
    upcast::UpcasterRegistry,
};

/// One incident edge recorded against a node (Phase 16.2). Carries both
/// endpoints so the node-delete detach cascade can drop the edge from the
/// *other* endpoint's index too.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct IncidentRef {
    edge_type: String,
    key: String,
    from: String,
    to: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct IncidentIndex {
    #[serde(default)]
    edges: Vec<IncidentRef>,
}

/// File-backed directed property graph (Phase 16) — the fourth `StorageAdapter`.
/// Nodes and edges are *separately keyed records*, each an independent Phase
/// 12/13 sequence-gated state machine driven by the same `decide()` as every
/// prior adapter. Since Phase 24.1 the write path lives in
/// `exec::project_node`/`project_edge`; this file is the file-sidecar driver
/// — behaviour is byte-identical to the pre-24.1 inlined version. Since
/// Phase 3 (v0.11) every write goes through a [`FileStore`]: batched, logged,
/// and crash-safe.
pub struct GraphStore {
    id: String,
    priority: u32,
    store: FileStore,
    builder: GraphRuntimeBuilder,
    upcasters: Arc<UpcasterRegistry>,
    migration_policy: MigrationPolicy,
}

impl GraphStore {
    pub fn new(
        id: String,
        root: PathBuf,
        priority: u32,
        builder: GraphRuntimeBuilder,
        upcasters: Arc<UpcasterRegistry>,
        migration_policy: MigrationPolicy,
    ) -> Self {
        Self {
            id,
            priority,
            store: FileStore::new(root),
            builder,
            upcasters,
            migration_policy,
        }
    }

    fn failure(&self, error: AdapterError) -> AdapterResult {
        AdapterResult::failure(self.id.clone(), StorageKind::Graph, error)
    }

    fn outcome(
        &self,
        outcome: GraphOutcome,
        source_version: u32,
        projected_version: u32,
    ) -> AdapterResult {
        match outcome {
            GraphOutcome::Created => AdapterResult::created_versioned(
                self.id.clone(),
                StorageKind::Graph,
                source_version,
                projected_version,
            ),
            GraphOutcome::Updated => AdapterResult::updated_versioned(
                self.id.clone(),
                StorageKind::Graph,
                source_version,
                projected_version,
            ),
            GraphOutcome::Deleted => AdapterResult::deleted_versioned(
                self.id.clone(),
                StorageKind::Graph,
                source_version,
                projected_version,
            ),
            GraphOutcome::Skipped(reason) => AdapterResult::skipped_versioned(
                self.id.clone(),
                StorageKind::Graph,
                reason,
                source_version,
                projected_version,
            ),
        }
    }
}

impl StorageAdapter for GraphStore {
    fn kind(&self) -> StorageKind {
        StorageKind::Graph
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
            Err(GraphError::UnsupportedVersion {
                from_version,
                to_version,
                reason,
                ..
            }) => {
                return match self.migration_policy {
                    MigrationPolicy::Strict => AdapterResult::unsupported_version(
                        self.id.clone(),
                        StorageKind::Graph,
                        reason,
                        from_version,
                        to_version,
                    ),
                    MigrationPolicy::IgnoreUnmatched => AdapterResult::skipped_versioned(
                        self.id.clone(),
                        StorageKind::Graph,
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
        let result = self.store.with_txn(|txn| {
            let mut backend = FileGraphBackend { txn };
            match &projection.record {
                ResolvedRecord::Node {
                    label,
                    node_id,
                    properties,
                } => {
                    let plan = NodePlan {
                        label: label.clone(),
                        node_id: node_id.clone(),
                        properties: properties.clone(),
                        on_existing: projection.on_existing,
                        operation: projection.operation,
                        permanent: projection.permanent,
                        facet: projection.facet.clone(),
                    };
                    exec::project_node(&mut backend, &plan, event)
                }
                ResolvedRecord::Edge {
                    edge_type,
                    from,
                    to,
                    discriminator,
                    properties,
                } => {
                    let key = canonical_edge_key(edge_type, from, to, discriminator.as_deref());
                    let plan = EdgePlan {
                        edge_type: edge_type.clone(),
                        from: from.clone(),
                        to: to.clone(),
                        key,
                        properties: properties.clone(),
                        on_existing: projection.on_existing,
                        operation: projection.operation,
                        permanent: projection.permanent,
                    };
                    exec::project_edge(&mut backend, &plan, event)
                }
            }
        });

        match result {
            Ok(Ok(o)) => self.outcome(o, source_version, projected_version),
            Ok(Err(e)) => self.failure(AdapterError::WriteFailed(e.to_string())),
            Err(e) => self.failure(e),
        }
    }
}

// ---------------------------------------------------------------------------
// FileGraphBackend — the driver
// ---------------------------------------------------------------------------

struct FileGraphBackend<'a, 't> {
    txn: &'a mut FileTxn<'t>,
}

/// Where node guards live; one directory per label.
const NODE_GUARD_DIR: &str = ".conduit/guard/nodes";

fn node_value_path(label: &str, node_id: &str) -> String {
    format!("nodes/{label}/{node_id}.json")
}

fn node_guard_path(label: &str, node_id: &str) -> String {
    format!("{NODE_GUARD_DIR}/{label}/{node_id}.json")
}

fn edge_value_path(edge_type: &str, key: &str) -> String {
    format!("edges/{edge_type}/{}.json", encode_edge_key(key))
}

fn edge_guard_path(edge_type: &str, key: &str) -> String {
    format!(
        ".conduit/guard/edges/{edge_type}/{}.json",
        encode_edge_key(key)
    )
}

fn incident_path(node_id: &str) -> String {
    format!(".conduit/incident/{node_id}.json")
}

fn ge_err(e: std::io::Error) -> GraphError {
    GraphError::WriteFailed(e.to_string())
}

fn invalid(e: serde_json::Error) -> GraphError {
    ge_err(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

impl FileGraphBackend<'_, '_> {
    fn read_guard(&mut self, path: &str) -> Result<Option<GraphGuard>, GraphError> {
        match self.txn.read(path).map_err(ge_err)? {
            Some(content) => serde_json::from_str(&content).map(Some).map_err(invalid),
            None => Ok(None),
        }
    }

    fn read_incident(&mut self, node_id: &str) -> Result<IncidentIndex, GraphError> {
        match self.txn.read(&incident_path(node_id)).map_err(ge_err)? {
            Some(content) => serde_json::from_str(&content).map_err(invalid),
            None => Ok(IncidentIndex::default()),
        }
    }

    fn commit_json<T: Serialize>(&mut self, path: &str, value: &T) -> Result<(), GraphError> {
        let content = serde_json::to_string_pretty(value).map_err(invalid)?;
        self.txn.write(path, content);
        Ok(())
    }

    fn add_incident(&mut self, node_id: &str, r: &IncidentRef) -> Result<(), GraphError> {
        let mut idx = self.read_incident(node_id)?;
        if !idx
            .edges
            .iter()
            .any(|e| e.edge_type == r.edge_type && e.key == r.key)
        {
            idx.edges.push(r.clone());
            self.commit_json(&incident_path(node_id), &idx)?;
        }
        Ok(())
    }

    fn drop_incident(
        &mut self,
        node_id: &str,
        edge_type: &str,
        key: &str,
    ) -> Result<(), GraphError> {
        let mut idx = self.read_incident(node_id)?;
        let before = idx.edges.len();
        idx.edges
            .retain(|e| !(e.edge_type == edge_type && e.key == key));
        if idx.edges.len() != before {
            if idx.edges.is_empty() {
                self.txn.remove(&incident_path(node_id));
            } else {
                self.commit_json(&incident_path(node_id), &idx)?;
            }
        }
        Ok(())
    }
}

impl GraphBackend for FileGraphBackend<'_, '_> {
    fn read_node_guard(
        &mut self,
        label: &str,
        node_id: &str,
    ) -> Result<Option<GraphGuard>, GraphError> {
        self.read_guard(&node_guard_path(label, node_id))
    }

    fn read_edge_guard(
        &mut self,
        edge_type: &str,
        key: &str,
    ) -> Result<Option<GraphGuard>, GraphError> {
        self.read_guard(&edge_guard_path(edge_type, key))
    }

    /// Checks every label a node with this id could have been projected
    /// under: each directory under `.conduit/guard/nodes/`, on disk or still
    /// pending in the batch. Small in practice.
    fn node_is_tombstoned(&mut self, node_id: &str) -> Result<bool, GraphError> {
        for label in self.txn.child_dirs(NODE_GUARD_DIR).map_err(ge_err)? {
            if let Some(g) = self.read_guard(&node_guard_path(&label, node_id))?
                && g.deleted
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn write_node(
        &mut self,
        label: &str,
        node_id: &str,
        properties: Option<&Value>,
        facet: &str,
        guard: &GraphGuard,
    ) -> Result<(), GraphError> {
        if let Some(properties) = properties {
            let value_path = node_value_path(label, node_id);
            if !facet.is_empty() {
                let facet_obj = properties.as_object().ok_or_else(|| {
                    GraphError::BuildFailed(
                        "node facet mapping must resolve to a JSON object".into(),
                    )
                })?;
                let mut existing = match self.txn.read(&value_path).map_err(ge_err)? {
                    Some(s) => serde_json::from_str::<Value>(&s)
                        .unwrap_or_else(|_| Value::Object(Default::default())),
                    None => Value::Object(Default::default()),
                };
                let obj = existing.as_object_mut().ok_or_else(|| {
                    GraphError::BuildFailed("existing node document is not a JSON object".into())
                })?;
                for (k, v) in facet_obj {
                    obj.insert(k.clone(), v.clone());
                }
                self.commit_json(&value_path, &existing)?;
            } else {
                self.commit_json(&value_path, properties)?;
            }
        }
        self.commit_json(&node_guard_path(label, node_id), guard)
    }

    fn write_edge(
        &mut self,
        edge_type: &str,
        from: &str,
        to: &str,
        key: &str,
        properties: Option<&Value>,
        guard: &GraphGuard,
    ) -> Result<(), GraphError> {
        if let Some(properties) = properties {
            self.commit_json(&edge_value_path(edge_type, key), properties)?;
            let r = IncidentRef {
                edge_type: edge_type.to_string(),
                key: key.to_string(),
                from: from.to_string(),
                to: to.to_string(),
            };
            for endpoint in dedup_pair(from, to) {
                self.add_incident(endpoint, &r)?;
            }
        }
        self.commit_json(&edge_guard_path(edge_type, key), guard)
    }

    fn delete_node_cascade(
        &mut self,
        label: &str,
        node_id: &str,
        guard: &GraphGuard,
    ) -> Result<(), GraphError> {
        self.txn.remove(&node_value_path(label, node_id));
        let incident = self.read_incident(node_id)?;
        for r in &incident.edges {
            self.txn.remove(&edge_value_path(&r.edge_type, &r.key));
            let egp = edge_guard_path(&r.edge_type, &r.key);
            let mut eg = self.read_guard(&egp)?.unwrap_or_default();
            eg.last_sequence = guard.last_sequence;
            eg.last_event_id = guard.last_event_id.clone();
            eg.deleted = true;
            eg.permanent = guard.permanent;
            self.commit_json(&egp, &eg)?;
            for other in [&r.from, &r.to] {
                if other.as_str() != node_id {
                    self.drop_incident(other, &r.edge_type, &r.key)?;
                }
            }
        }
        self.txn.remove(&incident_path(node_id));
        self.commit_json(&node_guard_path(label, node_id), guard)
    }

    fn delete_edge(
        &mut self,
        edge_type: &str,
        from: &str,
        to: &str,
        key: &str,
        guard: &GraphGuard,
    ) -> Result<(), GraphError> {
        self.txn.remove(&edge_value_path(edge_type, key));
        for endpoint in dedup_pair(from, to) {
            self.drop_incident(endpoint, edge_type, key)?;
        }
        self.commit_json(&edge_guard_path(edge_type, key), guard)
    }
}

fn dedup_pair<'a>(a: &'a str, b: &'a str) -> Vec<&'a str> {
    if a == b { vec![a] } else { vec![a, b] }
}
