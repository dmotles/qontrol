use petgraph::graph::DiGraph;
use serde::{Deserialize, Serialize};

// ─── API Response Types ───────────────────────────────────────────────────────

/// Wrapper for paginated portal endpoints that return `{ "entries": [...] }`.
#[derive(Debug, Clone, Deserialize)]
#[serde(bound(deserialize = "T: serde::de::DeserializeOwned"))]
pub struct PortalList<T> {
    #[serde(default)]
    pub entries: Vec<T>,
}

/// Host address used in portal hub/spoke responses.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HostAddress {
    pub address: String,
    pub port: u16,
}

/// A hub portal relationship.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PortalHub {
    pub id: u64,
    #[serde(rename = "type")]
    pub portal_type: String,
    pub state: String,
    pub status: String,
    #[serde(default)]
    pub spoke_hosts: Vec<HostAddress>,
    #[serde(default)]
    pub spoke_cluster_uuid: Option<String>,
    #[serde(default)]
    pub spoke_cluster_name: Option<String>,
    #[serde(default)]
    pub pending_roots: Vec<String>,
    #[serde(default)]
    pub authorized_roots: Vec<String>,
}

/// Root mapping for spoke portals.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SpokeRoot {
    pub local_root: String,
    pub remote_root: String,
    #[serde(default)]
    pub authorized: bool,
}

/// A spoke portal relationship.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PortalSpoke {
    pub id: u64,
    #[serde(rename = "type")]
    pub portal_type: String,
    pub state: String,
    pub status: String,
    #[serde(default)]
    pub hub_hosts: Vec<HostAddress>,
    #[serde(default)]
    pub hub_id: Option<u64>,
    #[serde(default)]
    pub hub_cluster_uuid: Option<String>,
    #[serde(default)]
    pub roots: Vec<SpokeRoot>,
}

/// A source replication relationship.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReplicationSource {
    pub id: String,
    #[serde(default)]
    pub target_address: Option<String>,
    #[serde(default)]
    pub target_port: Option<u16>,
    #[serde(default)]
    pub source_root_id: Option<String>,
    #[serde(default)]
    pub source_root_read_only: Option<bool>,
    #[serde(default)]
    pub map_local_ids_to_nfs_ids: Option<bool>,
    #[serde(default)]
    pub replication_enabled: bool,
    #[serde(default)]
    pub replication_mode: Option<String>,
}

/// Job duration from the API (nanoseconds as string).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JobDuration {
    #[serde(default)]
    pub nanoseconds: Option<String>,
}

/// Detailed replication job progress.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReplicationJobStatus {
    #[serde(default)]
    pub percent_complete: Option<String>,
    #[serde(default)]
    pub estimated_seconds_remaining: Option<String>,
    #[serde(default)]
    pub bytes_transferred: Option<String>,
    #[serde(default)]
    pub bytes_unchanged: Option<String>,
    #[serde(default)]
    pub bytes_remaining: Option<String>,
    #[serde(default)]
    pub bytes_deleted: Option<String>,
    #[serde(default)]
    pub bytes_total: Option<String>,
    #[serde(default)]
    pub files_transferred: Option<String>,
    #[serde(default)]
    pub files_unchanged: Option<String>,
    #[serde(default)]
    pub files_remaining: Option<String>,
    #[serde(default)]
    pub files_deleted: Option<String>,
    #[serde(default)]
    pub files_total: Option<String>,
    #[serde(default)]
    pub throughput_overall: Option<String>,
    #[serde(default)]
    pub throughput_current: Option<String>,
}

/// Status for a source replication relationship.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReplicationSourceStatus {
    pub id: String,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub source_cluster_name: Option<String>,
    #[serde(default)]
    pub source_cluster_uuid: Option<String>,
    #[serde(default)]
    pub source_root_path: Option<String>,
    #[serde(default)]
    pub target_cluster_name: Option<String>,
    #[serde(default)]
    pub target_cluster_uuid: Option<String>,
    #[serde(default)]
    pub target_root_path: Option<String>,
    #[serde(default)]
    pub target_address: Option<String>,
    #[serde(default)]
    pub replication_mode: Option<String>,
    #[serde(default)]
    pub replication_enabled: bool,
    #[serde(default)]
    pub job_state: Option<String>,
    #[serde(default)]
    pub recovery_point: Option<String>,
    #[serde(default)]
    pub error_from_last_job: Option<String>,
    #[serde(default)]
    pub duration_of_last_job: Option<JobDuration>,
    #[serde(default)]
    pub replication_job_status: Option<ReplicationJobStatus>,
}

/// Status for a target replication relationship.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReplicationTargetStatus {
    pub id: String,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub source_cluster_name: Option<String>,
    #[serde(default)]
    pub source_cluster_uuid: Option<String>,
    #[serde(default)]
    pub source_root_path: Option<String>,
    #[serde(default)]
    pub source_address: Option<String>,
    #[serde(default)]
    pub source_port: Option<u16>,
    #[serde(default)]
    pub target_cluster_name: Option<String>,
    #[serde(default)]
    pub target_cluster_uuid: Option<String>,
    #[serde(default)]
    pub target_root_path: Option<String>,
    #[serde(default)]
    pub target_root_read_only: Option<bool>,
    #[serde(default)]
    pub replication_enabled: bool,
    #[serde(default)]
    pub job_state: Option<String>,
    #[serde(default)]
    pub recovery_point: Option<String>,
    #[serde(default)]
    pub error_from_last_job: Option<String>,
    #[serde(default)]
    pub duration_of_last_job: Option<JobDuration>,
    #[serde(default)]
    pub replication_job_status: Option<ReplicationJobStatus>,
}

/// An object replication relationship (S3).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ObjectRelationship {
    pub id: String,
    #[serde(default)]
    pub direction: Option<String>,
    #[serde(default)]
    pub local_directory_id: Option<String>,
    #[serde(default)]
    pub object_store_address: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub bucket: Option<String>,
    #[serde(default)]
    pub bucket_style: Option<String>,
    #[serde(default)]
    pub object_folder: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub access_key_id: Option<String>,
}

/// Status for an object replication relationship.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ObjectRelationshipStatus {
    pub id: String,
    #[serde(default)]
    pub direction: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub object_store_address: Option<String>,
    #[serde(default)]
    pub bucket: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub object_folder: Option<String>,
    #[serde(default)]
    pub local_directory_id: Option<String>,
}

// ─── Graph Model Types ────────────────────────────────────────────────────────

/// A node in the CDF relationship graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CdfNode {
    /// A cluster with a known profile.
    ProfiledCluster {
        name: String,
        uuid: String,
        address: String,
    },
    /// A cluster discovered via replication but not in our profiles.
    UnknownCluster {
        address: String,
        uuid: Option<String>,
    },
    /// An S3 bucket target.
    S3Bucket {
        address: String,
        bucket: String,
        region: Option<String>,
    },
}

/// An edge in the CDF relationship graph.
#[derive(Debug, Clone)]
pub enum CdfEdge {
    Portal {
        hub_id: u64,
        spoke_id: u64,
        portal_type: String,
        state: String,
        status: String,
        roots: Vec<String>,
    },
    Replication {
        source_path: Option<String>,
        target_path: Option<String>,
        mode: Option<String>,
        enabled: bool,
        state: Option<String>,
        job_state: Option<String>,
        recovery_point: Option<String>,
        error_from_last_job: Option<String>,
        replication_job_status: Option<ReplicationJobStatus>,
    },
    ObjectReplication {
        direction: Option<String>,
        bucket: Option<String>,
        folder: Option<String>,
        state: Option<String>,
    },
}

impl CdfEdge {
    /// Returns true if this edge represents a problematic relationship:
    /// disabled, errored, or in an unhealthy state.
    pub fn is_problem(&self) -> bool {
        match self {
            CdfEdge::Portal { state, status, .. } => {
                // Healthy portals are ACCEPTED + ACTIVE
                state != "ACCEPTED" || status != "ACTIVE"
            }
            CdfEdge::Replication {
                enabled,
                error_from_last_job,
                state,
                ..
            } => {
                !enabled
                    || error_from_last_job.is_some()
                    || state
                        .as_deref()
                        .map_or(true, |s| s != "ESTABLISHED")
            }
            CdfEdge::ObjectReplication { state, .. } => {
                state.as_deref().map_or(true, |s| s != "ACTIVE")
            }
        }
    }
}

/// Directed graph of inter-cluster data fabric relationships.
pub type CdfGraph = DiGraph<CdfNode, CdfEdge>;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_portal_hub_deserialize() {
        let data = json!({
            "entries": [{
                "id": 1,
                "type": "PORTAL_READ_WRITE",
                "state": "ACCEPTED",
                "status": "ACTIVE",
                "spoke_hosts": [{"address": "10.0.0.1", "port": 3712}],
                "spoke_cluster_uuid": "abc-123",
                "spoke_cluster_name": "spoke-cluster",
                "pending_roots": [],
                "authorized_roots": ["/data"]
            }]
        });
        let list: PortalList<PortalHub> = serde_json::from_value(data).unwrap();
        assert_eq!(list.entries.len(), 1);
        assert_eq!(list.entries[0].id, 1);
        assert_eq!(list.entries[0].portal_type, "PORTAL_READ_WRITE");
        assert_eq!(list.entries[0].state, "ACCEPTED");
        assert_eq!(list.entries[0].status, "ACTIVE");
        assert_eq!(list.entries[0].spoke_hosts[0].address, "10.0.0.1");
        assert_eq!(list.entries[0].spoke_cluster_uuid.as_deref(), Some("abc-123"));
        assert_eq!(list.entries[0].authorized_roots, vec!["/data"]);
    }

    #[test]
    fn test_portal_hub_empty_entries() {
        let data = json!({"entries": []});
        let list: PortalList<PortalHub> = serde_json::from_value(data).unwrap();
        assert!(list.entries.is_empty());
    }

    #[test]
    fn test_portal_hub_missing_optional_fields() {
        let data = json!({
            "entries": [{
                "id": 2,
                "type": "PORTAL_READ_ONLY",
                "state": "PENDING",
                "status": "INACTIVE"
            }]
        });
        let list: PortalList<PortalHub> = serde_json::from_value(data).unwrap();
        assert_eq!(list.entries[0].spoke_hosts.len(), 0);
        assert!(list.entries[0].spoke_cluster_uuid.is_none());
        assert!(list.entries[0].spoke_cluster_name.is_none());
    }

    #[test]
    fn test_portal_spoke_deserialize() {
        let data = json!({
            "entries": [{
                "id": 5,
                "type": "PORTAL_READ_WRITE",
                "state": "ACCEPTED",
                "status": "ACTIVE",
                "hub_hosts": [{"address": "10.0.0.2", "port": 3712}],
                "hub_id": 1,
                "hub_cluster_uuid": "def-456",
                "roots": [{"local_root": "/local", "remote_root": "/remote", "authorized": true}]
            }]
        });
        let list: PortalList<PortalSpoke> = serde_json::from_value(data).unwrap();
        assert_eq!(list.entries[0].id, 5);
        assert_eq!(list.entries[0].hub_id, Some(1));
        assert_eq!(list.entries[0].roots.len(), 1);
        assert!(list.entries[0].roots[0].authorized);
    }

    #[test]
    fn test_replication_source_deserialize() {
        let data = json!([{
            "id": "075d8b86-8e28-40f2-921d-8a1b6585475a",
            "target_address": "10.0.1.1",
            "target_port": 3712,
            "source_root_id": "2",
            "replication_enabled": true,
            "replication_mode": "REPLICATION_CONTINUOUS"
        }]);
        let sources: Vec<ReplicationSource> = serde_json::from_value(data).unwrap();
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].id, "075d8b86-8e28-40f2-921d-8a1b6585475a");
        assert!(sources[0].replication_enabled);
        assert_eq!(sources[0].replication_mode.as_deref(), Some("REPLICATION_CONTINUOUS"));
    }

    #[test]
    fn test_replication_source_empty_array() {
        let data = json!([]);
        let sources: Vec<ReplicationSource> = serde_json::from_value(data).unwrap();
        assert!(sources.is_empty());
    }

    #[test]
    fn test_replication_source_status_deserialize() {
        let data = json!([{
            "id": "075d8b86-8e28-40f2-921d-8a1b6585475a",
            "state": "ESTABLISHED",
            "source_cluster_name": "cluster-a",
            "source_cluster_uuid": "uuid-a",
            "source_root_path": "/src",
            "target_cluster_name": "cluster-b",
            "target_cluster_uuid": "uuid-b",
            "target_root_path": "/dst",
            "target_address": "10.0.1.1",
            "replication_mode": "REPLICATION_CONTINUOUS",
            "replication_enabled": true,
            "job_state": "REPLICATION_RUNNING",
            "recovery_point": "2026-02-23T12:00:00Z",
            "duration_of_last_job": {"nanoseconds": "1000000000"},
            "replication_job_status": {
                "percent_complete": "75.5",
                "bytes_transferred": "1024000",
                "bytes_total": "2048000"
            }
        }]);
        let statuses: Vec<ReplicationSourceStatus> = serde_json::from_value(data).unwrap();
        assert_eq!(statuses[0].state.as_deref(), Some("ESTABLISHED"));
        assert_eq!(statuses[0].job_state.as_deref(), Some("REPLICATION_RUNNING"));
        let job = statuses[0].replication_job_status.as_ref().unwrap();
        assert_eq!(job.percent_complete.as_deref(), Some("75.5"));
    }

    #[test]
    fn test_replication_target_status_deserialize() {
        let data = json!([{
            "id": "1255815d-9cf1-4887-9388-d4d2653b8475",
            "state": "ESTABLISHED",
            "source_cluster_name": "cluster-a",
            "source_address": "10.0.0.1",
            "source_port": 3712,
            "target_cluster_name": "cluster-b",
            "replication_enabled": true,
            "job_state": "REPLICATION_NOT_RUNNING"
        }]);
        let statuses: Vec<ReplicationTargetStatus> = serde_json::from_value(data).unwrap();
        assert_eq!(statuses[0].id, "1255815d-9cf1-4887-9388-d4d2653b8475");
        assert_eq!(statuses[0].source_address.as_deref(), Some("10.0.0.1"));
        assert_eq!(statuses[0].source_port, Some(3712));
    }

    #[test]
    fn test_object_relationship_deserialize() {
        let data = json!([{
            "id": "173f0649-10da-422e-8725-712eaeee5334",
            "direction": "COPY_TO_OBJECT",
            "local_directory_id": "100",
            "object_store_address": "s3.amazonaws.com",
            "port": 443,
            "bucket": "my-backup-bucket",
            "bucket_style": "BUCKET_STYLE_VIRTUAL_HOSTED",
            "object_folder": "backups/",
            "region": "us-east-1",
            "access_key_id": "AKIA..."
        }]);
        let rels: Vec<ObjectRelationship> = serde_json::from_value(data).unwrap();
        assert_eq!(rels[0].direction.as_deref(), Some("COPY_TO_OBJECT"));
        assert_eq!(rels[0].bucket.as_deref(), Some("my-backup-bucket"));
        assert_eq!(rels[0].region.as_deref(), Some("us-east-1"));
    }

    #[test]
    fn test_object_relationship_status_deserialize() {
        let data = json!([{
            "id": "173f0649-10da-422e-8725-712eaeee5334",
            "direction": "COPY_TO_OBJECT",
            "state": "ACTIVE",
            "object_store_address": "s3.amazonaws.com",
            "bucket": "my-bucket",
            "region": "us-east-1"
        }]);
        let statuses: Vec<ObjectRelationshipStatus> = serde_json::from_value(data).unwrap();
        assert_eq!(statuses[0].state.as_deref(), Some("ACTIVE"));
    }

    #[test]
    fn test_object_relationship_missing_optional() {
        let data = json!([{
            "id": "6c6cc96c-e764-456f-adf2-e96de2ddd097",
            "direction": "COPY_FROM_OBJECT"
        }]);
        let rels: Vec<ObjectRelationship> = serde_json::from_value(data).unwrap();
        assert!(rels[0].bucket.is_none());
        assert!(rels[0].region.is_none());
    }

    #[test]
    fn test_unknown_enum_values_passthrough() {
        // API may return new enum values — string fields accept anything
        let data = json!({
            "entries": [{
                "id": 99,
                "type": "PORTAL_NEW_TYPE_V3",
                "state": "FUTURE_STATE",
                "status": "QUANTUM_ENTANGLED"
            }]
        });
        let list: PortalList<PortalHub> = serde_json::from_value(data).unwrap();
        assert_eq!(list.entries[0].portal_type, "PORTAL_NEW_TYPE_V3");
        assert_eq!(list.entries[0].state, "FUTURE_STATE");
    }

    #[test]
    fn test_cdf_node_variants() {
        let profiled = CdfNode::ProfiledCluster {
            name: "test".into(),
            uuid: "uuid-1".into(),
            address: "10.0.0.1".into(),
        };
        let unknown = CdfNode::UnknownCluster {
            address: "10.0.0.2".into(),
            uuid: Some("uuid-2".into()),
        };
        let s3 = CdfNode::S3Bucket {
            address: "s3.amazonaws.com".into(),
            bucket: "my-bucket".into(),
            region: Some("us-east-1".into()),
        };
        // Verify equality works
        assert_eq!(profiled.clone(), profiled);
        assert_ne!(profiled, unknown);
        assert_ne!(unknown, s3);
    }

    #[test]
    fn test_cdf_graph_construction() {
        let mut graph = CdfGraph::new();
        let n1 = graph.add_node(CdfNode::ProfiledCluster {
            name: "cluster-a".into(),
            uuid: "uuid-a".into(),
            address: "10.0.0.1".into(),
        });
        let n2 = graph.add_node(CdfNode::ProfiledCluster {
            name: "cluster-b".into(),
            uuid: "uuid-b".into(),
            address: "10.0.1.1".into(),
        });
        let n3 = graph.add_node(CdfNode::S3Bucket {
            address: "s3.amazonaws.com".into(),
            bucket: "backup".into(),
            region: Some("us-west-2".into()),
        });

        graph.add_edge(
            n1,
            n2,
            CdfEdge::Portal {
                hub_id: 1,
                spoke_id: 5,
                portal_type: "PORTAL_READ_WRITE".into(),
                state: "ACCEPTED".into(),
                status: "ACTIVE".into(),
                roots: vec!["/data".into()],
            },
        );
        graph.add_edge(
            n1,
            n2,
            CdfEdge::Replication {
                source_path: Some("/src".into()),
                target_path: Some("/dst".into()),
                mode: Some("REPLICATION_CONTINUOUS".into()),
                enabled: true,
                state: Some("ESTABLISHED".into()),
                job_state: Some("REPLICATION_RUNNING".into()),
                recovery_point: Some("2026-02-23T12:00:00Z".into()),
                error_from_last_job: None,
                replication_job_status: None,
            },
        );
        graph.add_edge(
            n1,
            n3,
            CdfEdge::ObjectReplication {
                direction: Some("COPY_TO_OBJECT".into()),
                bucket: Some("backup".into()),
                folder: Some("daily/".into()),
                state: Some("ACTIVE".into()),
            },
        );

        assert_eq!(graph.node_count(), 3);
        assert_eq!(graph.edge_count(), 3);
    }
}
