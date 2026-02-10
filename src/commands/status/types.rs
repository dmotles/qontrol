use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Top-level environment status aggregating all clusters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentStatus {
    pub aggregates: Aggregates,
    pub alerts: Vec<Alert>,
    pub clusters: Vec<ClusterStatus>,
}

/// Aggregate metrics across all clusters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Aggregates {
    pub cluster_count: usize,
    pub reachable_count: usize,
    pub total_nodes: usize,
    pub online_nodes: usize,
    pub capacity: CapacityStatus,
    pub files: FileStats,
}

/// Severity levels for alerts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AlertSeverity {
    Critical,
    Warning,
    Info,
}

/// An alert raised during status collection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub severity: AlertSeverity,
    pub cluster: String,
    pub message: String,
    pub category: String,
}

/// Status of a single cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStatus {
    pub profile: String,
    pub name: String,
    pub uuid: String,
    pub version: String,
    #[serde(rename = "type")]
    pub cluster_type: ClusterType,
    pub reachable: bool,
    pub stale: bool,
    pub latency_ms: u64,
    pub nodes: NodeStatus,
    pub capacity: CapacityStatus,
    pub activity: ActivityStatus,
    pub files: FileStats,
    pub health: HealthStatus,
}

/// Node counts for a cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatus {
    pub total: usize,
    pub online: usize,
    #[serde(default)]
    pub details: Vec<NodeNetworkInfo>,
}

/// Per-node network details: connections, NIC throughput, link speed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeNetworkInfo {
    pub node_id: u64,
    pub connections: u32,
    #[serde(default)]
    pub connection_breakdown: HashMap<String, u32>,
    /// Current NIC throughput in bits per second (None if unavailable)
    pub nic_throughput_bps: Option<u64>,
    /// Link speed in bits per second (None for cloud clusters)
    pub nic_link_speed_bps: Option<u64>,
    /// NIC utilization percentage (None for cloud clusters)
    pub nic_utilization_pct: Option<f64>,
}

/// Capacity metrics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CapacityStatus {
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub free_bytes: u64,
    pub snapshot_bytes: u64,
    pub used_pct: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub projection: Option<CapacityProjection>,
}

/// Capacity projection from linear regression on historical usage data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityProjection {
    pub days_until_full: Option<u64>,
    pub growth_rate_bytes_per_day: f64,
    pub confidence: ProjectionConfidence,
}

/// Confidence level for capacity projections.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ProjectionConfidence {
    High,
    Low,
}

/// Activity metrics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ActivityStatus {
    pub iops_read: f64,
    pub iops_write: f64,
    pub throughput_read: f64,
    pub throughput_write: f64,
    pub connections: usize,
    pub is_idle: bool,
}

/// File system statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FileStats {
    pub total_files: u64,
    pub total_directories: u64,
    pub total_snapshots: u64,
    pub snapshot_bytes: u64,
}

/// Overall health status of a cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub status: HealthLevel,
    pub issues: Vec<String>,
    #[serde(default)]
    pub disks_unhealthy: usize,
    #[serde(default)]
    pub psus_unhealthy: usize,
    #[serde(default)]
    pub data_at_risk: bool,
    #[serde(default)]
    pub remaining_node_failures: Option<u64>,
    #[serde(default)]
    pub remaining_drive_failures: Option<u64>,
    #[serde(default)]
    pub protection_type: Option<String>,
}

/// Details of an unhealthy disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnhealthyDisk {
    pub node_id: u64,
    pub bay: String,
    pub disk_type: String,
    pub state: String,
}

/// Details of an unhealthy PSU.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnhealthyPsu {
    pub node_id: u64,
    pub location: String,
    pub name: String,
    pub state: String,
}

/// Health level enumeration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum HealthLevel {
    Healthy,
    Degraded,
    Critical,
}

/// Cluster platform type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ClusterType {
    OnPrem(Vec<String>),
    CnqAws,
    AnqAzure,
}

impl std::fmt::Display for ClusterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterType::OnPrem(models) => {
                if models.is_empty() {
                    write!(f, "On-Prem")
                } else {
                    write!(f, "On-Prem ({})", models.join(", "))
                }
            }
            ClusterType::CnqAws => write!(f, "CNQ-AWS"),
            ClusterType::AnqAzure => write!(f, "ANQ-Azure"),
        }
    }
}

/// Result of collecting data from a single cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "result")]
pub enum ClusterResult {
    Success {
        data: Box<ClusterStatus>,
        latency_ms: u64,
    },
    Unreachable {
        profile: String,
        error: String,
    },
}

/// Cached cluster data with a timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedClusterData {
    pub profile: String,
    pub data: ClusterStatus,
    pub cached_at: String,
}

/// The full cache file structure: profile → cached data.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StatusCache {
    pub clusters: HashMap<String, CachedClusterData>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_type_serde_roundtrip() {
        let types = vec![
            ClusterType::CnqAws,
            ClusterType::AnqAzure,
            ClusterType::OnPrem(vec!["Q0626".to_string()]),
        ];
        for ct in &types {
            let json = serde_json::to_string(ct).unwrap();
            let back: ClusterType = serde_json::from_str(&json).unwrap();
            assert_eq!(*ct, back);
        }
    }

    #[test]
    fn test_cluster_type_display() {
        assert_eq!(ClusterType::CnqAws.to_string(), "CNQ-AWS");
        assert_eq!(ClusterType::AnqAzure.to_string(), "ANQ-Azure");
        assert_eq!(
            ClusterType::OnPrem(vec!["Q0626".to_string()]).to_string(),
            "On-Prem (Q0626)"
        );
        assert_eq!(ClusterType::OnPrem(vec![]).to_string(), "On-Prem");
    }

    #[test]
    fn test_cluster_status_serde_roundtrip() {
        let status = ClusterStatus {
            profile: "test".to_string(),
            name: "my-cluster".to_string(),
            uuid: "abc-123".to_string(),
            version: "7.7.2".to_string(),
            cluster_type: ClusterType::AnqAzure,
            reachable: true,
            stale: false,
            latency_ms: 42,
            nodes: NodeStatus {
                total: 4,
                online: 4,
                details: vec![],
            },
            capacity: CapacityStatus {
                total_bytes: 1_000_000,
                used_bytes: 500_000,
                free_bytes: 500_000,
                snapshot_bytes: 0,
                used_pct: 50.0,
                projection: None,
            },
            activity: ActivityStatus::default(),
            files: FileStats::default(),
            health: HealthStatus {
                status: HealthLevel::Healthy,
                issues: vec![],
                disks_unhealthy: 0,
                psus_unhealthy: 0,
                data_at_risk: false,
                remaining_node_failures: Some(1),
                remaining_drive_failures: Some(2),
                protection_type: Some("PROTECTION_SYSTEM_TYPE_EC".to_string()),
            },
        };

        let json = serde_json::to_string_pretty(&status).unwrap();
        let back: ClusterStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, "my-cluster");
        assert_eq!(back.cluster_type, ClusterType::AnqAzure);
        assert_eq!(back.nodes.total, 4);
        assert_eq!(back.health.disks_unhealthy, 0);
        assert_eq!(back.health.remaining_node_failures, Some(1));
    }

    #[test]
    fn test_node_network_info_serde_roundtrip() {
        let info = NodeNetworkInfo {
            node_id: 1,
            connections: 42,
            connection_breakdown: {
                let mut m = HashMap::new();
                m.insert("NFS".to_string(), 30);
                m.insert("REST".to_string(), 12);
                m
            },
            nic_throughput_bps: Some(12_400_000_000),
            nic_link_speed_bps: Some(200_000_000_000),
            nic_utilization_pct: Some(6.2),
        };

        let json = serde_json::to_string(&info).unwrap();
        let back: NodeNetworkInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(back.node_id, 1);
        assert_eq!(back.connections, 42);
        assert_eq!(back.connection_breakdown.get("NFS"), Some(&30));
        assert_eq!(back.nic_throughput_bps, Some(12_400_000_000));
        assert_eq!(back.nic_link_speed_bps, Some(200_000_000_000));
    }

    #[test]
    fn test_node_network_info_cloud_no_link_speed() {
        let info = NodeNetworkInfo {
            node_id: 1,
            connections: 5,
            connection_breakdown: HashMap::new(),
            nic_throughput_bps: Some(1_000_000),
            nic_link_speed_bps: None,
            nic_utilization_pct: None,
        };

        let json = serde_json::to_string(&info).unwrap();
        let back: NodeNetworkInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(back.nic_link_speed_bps, None);
        assert_eq!(back.nic_utilization_pct, None);
    }

    #[test]
    fn test_environment_status_serde_roundtrip() {
        let env_status = EnvironmentStatus {
            aggregates: Aggregates {
                cluster_count: 1,
                reachable_count: 1,
                total_nodes: 4,
                online_nodes: 4,
                capacity: CapacityStatus::default(),
                files: FileStats::default(),
            },
            alerts: vec![Alert {
                severity: AlertSeverity::Warning,
                cluster: "test".to_string(),
                message: "node offline".to_string(),
                category: "nodes".to_string(),
            }],
            clusters: vec![],
        };

        let json = serde_json::to_string(&env_status).unwrap();
        let back: EnvironmentStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back.aggregates.cluster_count, 1);
        assert_eq!(back.alerts.len(), 1);
        assert_eq!(back.alerts[0].severity, AlertSeverity::Warning);
    }

    #[test]
    fn test_cluster_result_serde_roundtrip() {
        let success = ClusterResult::Success {
            data: Box::new(ClusterStatus {
                profile: "p".to_string(),
                name: "c".to_string(),
                uuid: "u".to_string(),
                version: "v".to_string(),
                cluster_type: ClusterType::CnqAws,
                reachable: true,
                stale: false,
                latency_ms: 10,
                nodes: NodeStatus {
                    total: 1,
                    online: 1,
                    details: vec![],
                },
                capacity: CapacityStatus::default(),
                activity: ActivityStatus::default(),
                files: FileStats::default(),
                health: HealthStatus {
                    status: HealthLevel::Healthy,
                    issues: vec![],
                    disks_unhealthy: 0,
                    psus_unhealthy: 0,
                    data_at_risk: false,
                    remaining_node_failures: None,
                    remaining_drive_failures: None,
                    protection_type: None,
                },
            }),
            latency_ms: 10,
        };
        let json = serde_json::to_string(&success).unwrap();
        let back: ClusterResult = serde_json::from_str(&json).unwrap();
        match back {
            ClusterResult::Success { latency_ms, .. } => assert_eq!(latency_ms, 10),
            _ => panic!("expected Success"),
        }

        let unreachable = ClusterResult::Unreachable {
            profile: "bad".to_string(),
            error: "connection refused".to_string(),
        };
        let json = serde_json::to_string(&unreachable).unwrap();
        let back: ClusterResult = serde_json::from_str(&json).unwrap();
        match back {
            ClusterResult::Unreachable { profile, error } => {
                assert_eq!(profile, "bad");
                assert_eq!(error, "connection refused");
            }
            _ => panic!("expected Unreachable"),
        }
    }
}
