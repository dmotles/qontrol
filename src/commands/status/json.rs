use std::collections::HashMap;

use chrono::Utc;
use serde::Serialize;

use super::types::*;

/// Top-level JSON output matching the design spec Section 8 schema.
#[derive(Debug, Serialize)]
pub struct JsonOutput {
    pub timestamp: String,
    pub aggregates: JsonAggregates,
    pub alerts: Vec<JsonAlert>,
    pub clusters: Vec<JsonCluster>,
}

/// Flattened aggregate metrics across all clusters (spec Section 8).
#[derive(Debug, Serialize)]
pub struct JsonAggregates {
    pub cluster_count: usize,
    pub healthy_count: usize,
    pub unreachable_count: usize,
    pub total_nodes: usize,
    pub online_nodes: usize,
    pub offline_nodes: usize,
    pub total_capacity_bytes: u64,
    pub used_capacity_bytes: u64,
    pub free_capacity_bytes: u64,
    pub snapshot_bytes: u64,
    pub total_files: u64,
    pub total_directories: u64,
    pub total_snapshots: u64,
    pub latency_min_ms: Option<u64>,
    pub latency_max_ms: Option<u64>,
}

/// Alert in JSON output.
#[derive(Debug, Serialize)]
pub struct JsonAlert {
    pub severity: String,
    pub cluster: String,
    pub message: String,
    pub category: String,
}

/// Per-cluster status in JSON output.
#[derive(Debug, Serialize)]
pub struct JsonCluster {
    pub profile: String,
    pub cluster_name: String,
    pub cluster_uuid: String,
    pub version: String,
    pub cluster_type: String,
    pub hardware_skus: Vec<String>,
    pub reachable: bool,
    pub stale: bool,
    pub latency_ms: u64,
    pub nodes: JsonNodes,
    pub capacity: JsonCapacity,
    pub activity: JsonActivity,
    pub files: JsonFiles,
    pub health: JsonHealth,
}

/// Node summary with per-node details.
#[derive(Debug, Serialize)]
pub struct JsonNodes {
    pub total: usize,
    pub online: usize,
    pub offline: usize,
    pub details: Vec<JsonNodeDetail>,
}

/// Per-node network details.
#[derive(Debug, Serialize)]
pub struct JsonNodeDetail {
    pub node_id: u64,
    pub connections: u32,
    pub connection_breakdown: HashMap<String, u32>,
    pub nic_throughput_bps: Option<u64>,
    pub nic_link_speed_bps: Option<u64>,
    pub nic_utilization_pct: Option<f64>,
}

/// Capacity metrics with optional projection.
#[derive(Debug, Serialize)]
pub struct JsonCapacity {
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub free_bytes: u64,
    pub snapshot_bytes: u64,
    pub used_pct: f64,
    pub projection: Option<JsonProjection>,
}

/// Capacity projection from linear regression.
#[derive(Debug, Serialize)]
pub struct JsonProjection {
    pub growth_rate_bytes_per_day: f64,
    pub days_to_full: Option<u64>,
    pub confidence: String,
}

/// Activity metrics (IOPS and throughput).
#[derive(Debug, Serialize)]
pub struct JsonActivity {
    pub read_iops: f64,
    pub write_iops: f64,
    pub read_throughput_bps: f64,
    pub write_throughput_bps: f64,
}

/// File/directory/snapshot counts.
#[derive(Debug, Serialize)]
pub struct JsonFiles {
    pub total_files: u64,
    pub total_directories: u64,
    pub total_snapshots: u64,
}

/// Health status indicators.
#[derive(Debug, Serialize)]
pub struct JsonHealth {
    pub disks_unhealthy: usize,
    pub data_at_risk: bool,
    pub remaining_node_failures: Option<u64>,
    pub remaining_drive_failures: Option<u64>,
    pub protection_type: Option<String>,
}

impl JsonOutput {
    /// Convert internal `EnvironmentStatus` to spec-compliant JSON output.
    pub fn from_status(status: &EnvironmentStatus) -> Self {
        let latencies: Vec<u64> = status
            .clusters
            .iter()
            .filter(|c| c.reachable)
            .map(|c| c.latency_ms)
            .collect();

        JsonOutput {
            timestamp: Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            aggregates: JsonAggregates {
                cluster_count: status.aggregates.cluster_count,
                healthy_count: status.aggregates.reachable_count,
                unreachable_count: status.aggregates.cluster_count
                    - status.aggregates.reachable_count,
                total_nodes: status.aggregates.total_nodes,
                online_nodes: status.aggregates.online_nodes,
                offline_nodes: status.aggregates.total_nodes - status.aggregates.online_nodes,
                total_capacity_bytes: status.aggregates.capacity.total_bytes,
                used_capacity_bytes: status.aggregates.capacity.used_bytes,
                free_capacity_bytes: status.aggregates.capacity.free_bytes,
                snapshot_bytes: status.aggregates.capacity.snapshot_bytes,
                total_files: status.aggregates.files.total_files,
                total_directories: status.aggregates.files.total_directories,
                total_snapshots: status.aggregates.files.total_snapshots,
                latency_min_ms: latencies.iter().copied().min(),
                latency_max_ms: latencies.iter().copied().max(),
            },
            alerts: status
                .alerts
                .iter()
                .map(|a| JsonAlert {
                    severity: match a.severity {
                        AlertSeverity::Critical => "critical".to_string(),
                        AlertSeverity::Warning => "warning".to_string(),
                        AlertSeverity::Info => "info".to_string(),
                    },
                    cluster: a.cluster.clone(),
                    message: a.message.clone(),
                    category: a.category.clone(),
                })
                .collect(),
            clusters: status.clusters.iter().map(convert_cluster).collect(),
        }
    }
}

fn convert_cluster(c: &ClusterStatus) -> JsonCluster {
    let (cluster_type, hardware_skus) = match &c.cluster_type {
        ClusterType::OnPrem(models) => ("on-prem".to_string(), models.clone()),
        ClusterType::CnqAws => ("cnq-aws".to_string(), vec![]),
        ClusterType::AnqAzure => ("anq-azure".to_string(), vec![]),
    };

    JsonCluster {
        profile: c.profile.clone(),
        cluster_name: c.name.clone(),
        cluster_uuid: c.uuid.clone(),
        version: c.version.clone(),
        cluster_type,
        hardware_skus,
        reachable: c.reachable,
        stale: c.stale,
        latency_ms: c.latency_ms,
        nodes: JsonNodes {
            total: c.nodes.total,
            online: c.nodes.online,
            offline: c.nodes.total.saturating_sub(c.nodes.online),
            details: c
                .nodes
                .details
                .iter()
                .map(|n| JsonNodeDetail {
                    node_id: n.node_id,
                    connections: n.connections,
                    connection_breakdown: n.connection_breakdown.clone(),
                    nic_throughput_bps: n.nic_throughput_bps,
                    nic_link_speed_bps: n.nic_link_speed_bps,
                    nic_utilization_pct: n.nic_utilization_pct,
                })
                .collect(),
        },
        capacity: JsonCapacity {
            total_bytes: c.capacity.total_bytes,
            used_bytes: c.capacity.used_bytes,
            free_bytes: c.capacity.free_bytes,
            snapshot_bytes: c.capacity.snapshot_bytes,
            used_pct: c.capacity.used_pct,
            projection: c.capacity.projection.as_ref().map(|p| JsonProjection {
                growth_rate_bytes_per_day: p.growth_rate_bytes_per_day,
                days_to_full: p.days_until_full,
                confidence: match p.confidence {
                    ProjectionConfidence::High => "high".to_string(),
                    ProjectionConfidence::Low => "low".to_string(),
                },
            }),
        },
        activity: JsonActivity {
            read_iops: c.activity.iops_read,
            write_iops: c.activity.iops_write,
            read_throughput_bps: c.activity.throughput_read,
            write_throughput_bps: c.activity.throughput_write,
        },
        files: JsonFiles {
            total_files: c.files.total_files,
            total_directories: c.files.total_directories,
            total_snapshots: c.files.total_snapshots,
        },
        health: JsonHealth {
            disks_unhealthy: c.health.disks_unhealthy,
            data_at_risk: c.health.data_at_risk,
            remaining_node_failures: c.health.remaining_node_failures,
            remaining_drive_failures: c.health.remaining_drive_failures,
            protection_type: c.health.protection_type.clone(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal EnvironmentStatus for testing.
    fn make_test_status() -> EnvironmentStatus {
        EnvironmentStatus {
            aggregates: Aggregates {
                cluster_count: 2,
                reachable_count: 1,
                total_nodes: 8,
                online_nodes: 7,
                capacity: CapacityStatus {
                    total_bytes: 2_170_000_000_000_000,
                    used_bytes: 1_860_000_000_000_000,
                    free_bytes: 310_000_000_000_000,
                    snapshot_bytes: 7_700_000_000_000,
                    used_pct: 85.7,
                    projection: None,
                },
                files: FileStats {
                    total_files: 698_412_061,
                    total_directories: 48_231_004,
                    total_snapshots: 12_847,
                    snapshot_bytes: 7_700_000_000_000,
                },
            },
            alerts: vec![
                Alert {
                    severity: AlertSeverity::Critical,
                    cluster: "iss-sg".to_string(),
                    message: "node 4: OFFLINE".to_string(),
                    category: "node_offline".to_string(),
                },
                Alert {
                    severity: AlertSeverity::Warning,
                    cluster: "music".to_string(),
                    message: "1 disk unhealthy".to_string(),
                    category: "disk_unhealthy".to_string(),
                },
            ],
            clusters: vec![
                ClusterStatus {
                    profile: "gravytrain".to_string(),
                    name: "gravytrain-sg".to_string(),
                    uuid: "f83b970e-1234-5678".to_string(),
                    version: "Qumulo Core 7.8.0".to_string(),
                    cluster_type: ClusterType::OnPrem(vec![
                        "C192T".to_string(),
                        "QCT_D52T".to_string(),
                    ]),
                    reachable: true,
                    stale: false,
                    latency_ms: 42,
                    nodes: NodeStatus {
                        total: 5,
                        online: 5,
                        offline_nodes: vec![],
                        details: vec![NodeNetworkInfo {
                            node_id: 1,
                            connections: 14,
                            connection_breakdown: {
                                let mut m = HashMap::new();
                                m.insert("NFS".to_string(), 8);
                                m.insert("SMB".to_string(), 4);
                                m.insert("REST".to_string(), 2);
                                m
                            },
                            nic_throughput_bps: Some(12_400_000_000),
                            nic_link_speed_bps: Some(200_000_000_000),
                            nic_utilization_pct: Some(6.2),
                            nic_bytes_total: None,
                        }],
                    },
                    capacity: CapacityStatus {
                        total_bytes: 605_000_000_000_000,
                        used_bytes: 594_000_000_000_000,
                        free_bytes: 11_000_000_000_000,
                        snapshot_bytes: 6_700_000_000_000,
                        used_pct: 98.2,
                        projection: Some(CapacityProjection {
                            days_until_full: Some(62),
                            growth_rate_bytes_per_day: 1_200_000_000_000.0,
                            confidence: ProjectionConfidence::High,
                        }),
                    },
                    activity: ActivityStatus {
                        iops_read: 140.0,
                        iops_write: 122.0,
                        throughput_read: 57_800_000.0,
                        throughput_write: 1_600_000.0,
                        connections: 20,
                        is_idle: false,
                    },
                    files: FileStats {
                        total_files: 501_204_881,
                        total_directories: 32_401_221,
                        total_snapshots: 8_201,
                        snapshot_bytes: 6_700_000_000_000,
                    },
                    health: HealthStatus {
                        status: HealthLevel::Healthy,
                        issues: vec![],
                        disks_unhealthy: 0,
                        psus_unhealthy: 0,
                        data_at_risk: false,
                        remaining_node_failures: Some(1),
                        remaining_drive_failures: Some(2),
                        protection_type: Some("PROTECTION_SYSTEM_TYPE_EC".to_string()),
                        unhealthy_disk_details: vec![],
                        unhealthy_psu_details: vec![],
                    },
                },
                ClusterStatus {
                    profile: "aws-grav".to_string(),
                    name: "aws-gravytrain".to_string(),
                    uuid: "abc-def-123".to_string(),
                    version: "Qumulo Core 7.8.0".to_string(),
                    cluster_type: ClusterType::CnqAws,
                    reachable: false,
                    stale: true,
                    latency_ms: 0,
                    nodes: NodeStatus {
                        total: 3,
                        online: 2,
                        offline_nodes: vec![],
                        details: vec![NodeNetworkInfo {
                            node_id: 1,
                            connections: 5,
                            connection_breakdown: HashMap::new(),
                            nic_throughput_bps: Some(1_000_000),
                            nic_link_speed_bps: None,
                            nic_utilization_pct: None,
                            nic_bytes_total: None,
                        }],
                    },
                    capacity: CapacityStatus {
                        total_bytes: 454_700_000_000_000,
                        used_bytes: 77_300_000_000_000,
                        free_bytes: 377_400_000_000_000,
                        snapshot_bytes: 0,
                        used_pct: 17.0,
                        projection: None,
                    },
                    activity: ActivityStatus::default(),
                    files: FileStats {
                        total_files: 35_679,
                        total_directories: 1_452,
                        total_snapshots: 0,
                        snapshot_bytes: 0,
                    },
                    health: HealthStatus {
                        status: HealthLevel::Healthy,
                        issues: vec![],
                        disks_unhealthy: 0,
                        psus_unhealthy: 0,
                        data_at_risk: false,
                        remaining_node_failures: None,
                        remaining_drive_failures: None,
                        protection_type: None,
                        unhealthy_disk_details: vec![],
                        unhealthy_psu_details: vec![],
                    },
                },
            ],
        }
    }

    #[test]
    fn test_json_output_has_timestamp() {
        let status = make_test_status();
        let json_output = JsonOutput::from_status(&status);
        assert!(!json_output.timestamp.is_empty());
        // Timestamp should be valid RFC3339
        chrono::DateTime::parse_from_rfc3339(&json_output.timestamp)
            .expect("timestamp should be valid RFC3339");
    }

    #[test]
    fn test_json_output_valid_json() {
        let status = make_test_status();
        let json_output = JsonOutput::from_status(&status);
        let json_str =
            serde_json::to_string_pretty(&json_output).expect("should serialize to JSON");
        // Verify it parses back as valid JSON
        let _: serde_json::Value =
            serde_json::from_str(&json_str).expect("should parse back as valid JSON");
    }

    #[test]
    fn test_json_aggregates_flat_structure() {
        let status = make_test_status();
        let json_output = JsonOutput::from_status(&status);
        let json_str = serde_json::to_string(&json_output).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        let agg = &val["aggregates"];
        // All spec fields present as top-level keys (not nested)
        assert_eq!(agg["cluster_count"], 2);
        assert_eq!(agg["healthy_count"], 1);
        assert_eq!(agg["unreachable_count"], 1);
        assert_eq!(agg["total_nodes"], 8);
        assert_eq!(agg["online_nodes"], 7);
        assert_eq!(agg["offline_nodes"], 1);
        assert_eq!(agg["total_capacity_bytes"], 2_170_000_000_000_000u64);
        assert_eq!(agg["used_capacity_bytes"], 1_860_000_000_000_000u64);
        assert_eq!(agg["free_capacity_bytes"], 310_000_000_000_000u64);
        assert_eq!(agg["snapshot_bytes"], 7_700_000_000_000u64);
        assert_eq!(agg["total_files"], 698_412_061u64);
        assert_eq!(agg["total_directories"], 48_231_004u64);
        assert_eq!(agg["total_snapshots"], 12_847u64);

        // Should NOT have nested capacity/files objects
        assert!(agg.get("capacity").is_none());
        assert!(agg.get("files").is_none());
    }

    #[test]
    fn test_json_aggregates_latency_from_reachable_clusters() {
        let status = make_test_status();
        let json_output = JsonOutput::from_status(&status);
        let json_str = serde_json::to_string(&json_output).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        let agg = &val["aggregates"];
        // Only the gravytrain cluster is reachable (latency 42ms)
        assert_eq!(agg["latency_min_ms"], 42);
        assert_eq!(agg["latency_max_ms"], 42);
    }

    #[test]
    fn test_json_aggregates_latency_none_when_no_reachable() {
        let mut status = make_test_status();
        for c in &mut status.clusters {
            c.reachable = false;
        }
        let json_output = JsonOutput::from_status(&status);
        let json_str = serde_json::to_string(&json_output).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert!(val["aggregates"]["latency_min_ms"].is_null());
        assert!(val["aggregates"]["latency_max_ms"].is_null());
    }

    #[test]
    fn test_json_cluster_type_strings() {
        let status = make_test_status();
        let json_output = JsonOutput::from_status(&status);
        let json_str = serde_json::to_string(&json_output).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        let clusters = val["clusters"].as_array().unwrap();
        // First cluster: on-prem
        assert_eq!(clusters[0]["cluster_type"], "on-prem");
        assert_eq!(
            clusters[0]["hardware_skus"],
            serde_json::json!(["C192T", "QCT_D52T"])
        );
        // Second cluster: CNQ AWS
        assert_eq!(clusters[1]["cluster_type"], "cnq-aws");
        assert_eq!(clusters[1]["hardware_skus"], serde_json::json!([]));
    }

    #[test]
    fn test_json_cluster_type_anq_azure() {
        let mut status = make_test_status();
        status.clusters[0].cluster_type = ClusterType::AnqAzure;
        let json_output = JsonOutput::from_status(&status);
        let json_str = serde_json::to_string(&json_output).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(val["clusters"][0]["cluster_type"], "anq-azure");
        assert_eq!(val["clusters"][0]["hardware_skus"], serde_json::json!([]));
    }

    #[test]
    fn test_json_cluster_field_names_match_spec() {
        let status = make_test_status();
        let json_output = JsonOutput::from_status(&status);
        let json_str = serde_json::to_string(&json_output).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        let cluster = &val["clusters"][0];
        // Spec field names (not internal names)
        assert_eq!(cluster["cluster_name"], "gravytrain-sg");
        assert_eq!(cluster["cluster_uuid"], "f83b970e-1234-5678");
        assert_eq!(cluster["cluster_type"], "on-prem");
        assert_eq!(cluster["profile"], "gravytrain");
        assert_eq!(cluster["version"], "Qumulo Core 7.8.0");

        // Should NOT have internal field names
        assert!(cluster.get("name").is_none());
        assert!(cluster.get("uuid").is_none());
        assert!(cluster.get("type").is_none());
    }

    #[test]
    fn test_json_nodes_has_offline_count() {
        let status = make_test_status();
        let json_output = JsonOutput::from_status(&status);
        let json_str = serde_json::to_string(&json_output).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        let nodes = &val["clusters"][0]["nodes"];
        assert_eq!(nodes["total"], 5);
        assert_eq!(nodes["online"], 5);
        assert_eq!(nodes["offline"], 0);

        // Second cluster has 1 offline node
        let nodes2 = &val["clusters"][1]["nodes"];
        assert_eq!(nodes2["total"], 3);
        assert_eq!(nodes2["online"], 2);
        assert_eq!(nodes2["offline"], 1);
    }

    #[test]
    fn test_json_activity_field_names() {
        let status = make_test_status();
        let json_output = JsonOutput::from_status(&status);
        let json_str = serde_json::to_string(&json_output).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        let activity = &val["clusters"][0]["activity"];
        // Spec field names
        assert_eq!(activity["read_iops"].as_f64().unwrap(), 140.0);
        assert_eq!(activity["write_iops"].as_f64().unwrap(), 122.0);
        assert_eq!(
            activity["read_throughput_bps"].as_f64().unwrap(),
            57_800_000.0
        );
        assert_eq!(
            activity["write_throughput_bps"].as_f64().unwrap(),
            1_600_000.0
        );

        // Should NOT have internal field names or non-spec fields
        assert!(activity.get("iops_read").is_none());
        assert!(activity.get("iops_write").is_none());
        assert!(activity.get("throughput_read").is_none());
        assert!(activity.get("throughput_write").is_none());
        assert!(activity.get("connections").is_none());
        assert!(activity.get("is_idle").is_none());
    }

    #[test]
    fn test_json_projection_field_names() {
        let status = make_test_status();
        let json_output = JsonOutput::from_status(&status);
        let json_str = serde_json::to_string(&json_output).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        let projection = &val["clusters"][0]["capacity"]["projection"];
        // Spec uses days_to_full, not days_until_full
        assert_eq!(projection["days_to_full"], 62);
        assert_eq!(
            projection["growth_rate_bytes_per_day"].as_f64().unwrap(),
            1_200_000_000_000.0
        );
        assert_eq!(projection["confidence"], "high");

        // Should NOT have internal field name
        assert!(projection.get("days_until_full").is_none());
    }

    #[test]
    fn test_json_projection_null_when_absent() {
        let status = make_test_status();
        let json_output = JsonOutput::from_status(&status);
        let json_str = serde_json::to_string(&json_output).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        // Second cluster has no projection
        let projection = &val["clusters"][1]["capacity"]["projection"];
        assert!(
            projection.is_null(),
            "absent projection should serialize as null, not be omitted"
        );
    }

    #[test]
    fn test_json_large_numbers_as_numbers() {
        let status = make_test_status();
        let json_output = JsonOutput::from_status(&status);
        let json_str = serde_json::to_string(&json_output).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        // Large byte values should be numbers, not strings
        let agg = &val["aggregates"];
        assert!(agg["total_capacity_bytes"].is_u64());
        assert_eq!(
            agg["total_capacity_bytes"].as_u64().unwrap(),
            2_170_000_000_000_000
        );

        let cap = &val["clusters"][0]["capacity"];
        assert!(cap["total_bytes"].is_u64());
        assert_eq!(cap["total_bytes"].as_u64().unwrap(), 605_000_000_000_000);
        assert!(cap["used_bytes"].is_u64());
        assert!(cap["snapshot_bytes"].is_u64());

        // NIC throughput
        let detail = &val["clusters"][0]["nodes"]["details"][0];
        assert!(detail["nic_throughput_bps"].is_u64());
        assert_eq!(
            detail["nic_throughput_bps"].as_u64().unwrap(),
            12_400_000_000
        );
        assert!(detail["nic_link_speed_bps"].is_u64());
        assert_eq!(
            detail["nic_link_speed_bps"].as_u64().unwrap(),
            200_000_000_000
        );
    }

    #[test]
    fn test_json_optional_fields_null_not_omitted() {
        let status = make_test_status();
        let json_output = JsonOutput::from_status(&status);
        let json_str = serde_json::to_string(&json_output).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        // Cloud cluster: nic_link_speed_bps and nic_utilization_pct should be null
        let cloud_node = &val["clusters"][1]["nodes"]["details"][0];
        assert!(
            cloud_node.get("nic_link_speed_bps").is_some(),
            "field should be present"
        );
        assert!(
            cloud_node["nic_link_speed_bps"].is_null(),
            "should be null for cloud"
        );
        assert!(
            cloud_node.get("nic_utilization_pct").is_some(),
            "field should be present"
        );
        assert!(
            cloud_node["nic_utilization_pct"].is_null(),
            "should be null for cloud"
        );

        // Cloud cluster: health optional fields should be null
        let cloud_health = &val["clusters"][1]["health"];
        assert!(cloud_health.get("remaining_node_failures").is_some());
        assert!(cloud_health["remaining_node_failures"].is_null());
        assert!(cloud_health.get("remaining_drive_failures").is_some());
        assert!(cloud_health["remaining_drive_failures"].is_null());
        assert!(cloud_health.get("protection_type").is_some());
        assert!(cloud_health["protection_type"].is_null());
    }

    #[test]
    fn test_json_alert_severity_strings() {
        let status = make_test_status();
        let json_output = JsonOutput::from_status(&status);
        let json_str = serde_json::to_string(&json_output).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        let alerts = val["alerts"].as_array().unwrap();
        assert_eq!(alerts[0]["severity"], "critical");
        assert_eq!(alerts[0]["category"], "node_offline");
        assert_eq!(alerts[1]["severity"], "warning");
        assert_eq!(alerts[1]["category"], "disk_unhealthy");
    }

    #[test]
    fn test_json_files_no_snapshot_bytes() {
        let status = make_test_status();
        let json_output = JsonOutput::from_status(&status);
        let json_str = serde_json::to_string(&json_output).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        let files = &val["clusters"][0]["files"];
        assert_eq!(files["total_files"], 501_204_881u64);
        assert_eq!(files["total_directories"], 32_401_221u64);
        assert_eq!(files["total_snapshots"], 8_201u64);
        // snapshot_bytes lives in capacity, not files
        assert!(files.get("snapshot_bytes").is_none());
    }

    #[test]
    fn test_json_health_no_status_or_issues() {
        let status = make_test_status();
        let json_output = JsonOutput::from_status(&status);
        let json_str = serde_json::to_string(&json_output).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        let health = &val["clusters"][0]["health"];
        // Spec Section 8 health does not include status or issues
        assert!(health.get("status").is_none());
        assert!(health.get("issues").is_none());
        // Spec fields are present
        assert_eq!(health["disks_unhealthy"], 0);
        assert_eq!(health["data_at_risk"], false);
        assert_eq!(health["remaining_node_failures"], 1);
        assert_eq!(health["remaining_drive_failures"], 2);
        assert_eq!(health["protection_type"], "PROTECTION_SYSTEM_TYPE_EC");
    }

    #[test]
    fn test_json_roundtrip_parse() {
        let status = make_test_status();
        let json_output = JsonOutput::from_status(&status);
        let json_str = serde_json::to_string_pretty(&json_output).unwrap();

        // Should be parseable as generic JSON value (pipe to jq)
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        // Top-level structure
        assert!(val.get("timestamp").is_some());
        assert!(val.get("aggregates").is_some());
        assert!(val.get("alerts").is_some());
        assert!(val.get("clusters").is_some());

        assert_eq!(val["clusters"].as_array().unwrap().len(), 2);
        assert_eq!(val["alerts"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_json_empty_status() {
        let status = EnvironmentStatus {
            aggregates: Aggregates {
                cluster_count: 0,
                reachable_count: 0,
                total_nodes: 0,
                online_nodes: 0,
                capacity: CapacityStatus::default(),
                files: FileStats::default(),
            },
            alerts: vec![],
            clusters: vec![],
        };

        let json_output = JsonOutput::from_status(&status);
        let json_str = serde_json::to_string_pretty(&json_output).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(val["aggregates"]["cluster_count"], 0);
        assert_eq!(val["aggregates"]["healthy_count"], 0);
        assert_eq!(val["aggregates"]["unreachable_count"], 0);
        assert!(val["aggregates"]["latency_min_ms"].is_null());
        assert!(val["aggregates"]["latency_max_ms"].is_null());
        assert_eq!(val["clusters"].as_array().unwrap().len(), 0);
        assert_eq!(val["alerts"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn test_json_node_detail_connection_breakdown() {
        let status = make_test_status();
        let json_output = JsonOutput::from_status(&status);
        let json_str = serde_json::to_string(&json_output).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        let detail = &val["clusters"][0]["nodes"]["details"][0];
        assert_eq!(detail["node_id"], 1);
        assert_eq!(detail["connections"], 14);
        let breakdown = detail["connection_breakdown"].as_object().unwrap();
        assert_eq!(breakdown["NFS"], 8);
        assert_eq!(breakdown["SMB"], 4);
        assert_eq!(breakdown["REST"], 2);
    }

    #[test]
    fn test_json_projection_confidence_low() {
        let mut status = make_test_status();
        if let Some(ref mut proj) = status.clusters[0].capacity.projection {
            proj.confidence = ProjectionConfidence::Low;
        }
        let json_output = JsonOutput::from_status(&status);
        let json_str = serde_json::to_string(&json_output).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(
            val["clusters"][0]["capacity"]["projection"]["confidence"],
            "low"
        );
    }
}
