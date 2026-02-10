use super::capacity;
use super::types::*;

/// Generate all alerts from collected cluster data and connectivity failures.
///
/// Takes the successfully collected clusters and any pre-built connectivity alerts
/// (for unreachable clusters). Scans each cluster's health, node status, capacity
/// projection, and protection data to produce a prioritized, sorted alert list.
///
/// Returns alerts sorted by severity: Critical first, then Warning, then Info.
pub fn generate_alerts(clusters: &[ClusterStatus], connectivity_alerts: Vec<Alert>) -> Vec<Alert> {
    let mut alerts = connectivity_alerts;

    for cluster in clusters {
        generate_cluster_alerts(cluster, &mut alerts);
    }

    sort_alerts(&mut alerts);
    alerts
}

/// Generate alerts for a single cluster's collected data.
fn generate_cluster_alerts(cluster: &ClusterStatus, alerts: &mut Vec<Alert>) {
    check_node_offline(cluster, alerts);
    check_data_at_risk(cluster, alerts);
    check_disk_health(cluster, alerts);
    check_psu_health(cluster, alerts);
    check_protection_degraded(cluster, alerts);
    check_capacity_projection(cluster, alerts);
}

/// Offline nodes: one alert per offline node with the node ID.
fn check_node_offline(cluster: &ClusterStatus, alerts: &mut Vec<Alert>) {
    if cluster.nodes.online >= cluster.nodes.total {
        return;
    }

    if cluster.nodes.offline_nodes.is_empty() {
        // Fallback: we know nodes are offline but don't have IDs
        let offline = cluster.nodes.total - cluster.nodes.online;
        alerts.push(Alert {
            severity: AlertSeverity::Critical,
            cluster: cluster.name.clone(),
            message: format!("{} node(s) offline", offline),
            category: "node_offline".to_string(),
        });
    } else {
        for &node_id in &cluster.nodes.offline_nodes {
            alerts.push(Alert {
                severity: AlertSeverity::Critical,
                cluster: cluster.name.clone(),
                message: format!("node {}: OFFLINE", node_id),
                category: "node_offline".to_string(),
            });
        }
    }
}

/// Data at risk from restriper: critical alert.
fn check_data_at_risk(cluster: &ClusterStatus, alerts: &mut Vec<Alert>) {
    if cluster.health.data_at_risk {
        alerts.push(Alert {
            severity: AlertSeverity::Critical,
            cluster: cluster.name.clone(),
            message: "DATA AT RISK \u{2014} restriper active".to_string(),
            category: "data_at_risk".to_string(),
        });
    }
}

/// Unhealthy disks: warning with detail (node, bay, type).
fn check_disk_health(cluster: &ClusterStatus, alerts: &mut Vec<Alert>) {
    if cluster.health.disks_unhealthy == 0 {
        return;
    }

    if cluster.health.unhealthy_disk_details.is_empty() {
        // Fallback: count only
        alerts.push(Alert {
            severity: AlertSeverity::Warning,
            cluster: cluster.name.clone(),
            message: format!("{} disk(s) unhealthy", cluster.health.disks_unhealthy),
            category: "disk_unhealthy".to_string(),
        });
    } else {
        // Detailed: one alert per unhealthy disk
        let count = cluster.health.disks_unhealthy;
        let details: Vec<String> = cluster
            .health
            .unhealthy_disk_details
            .iter()
            .map(|d| format!("node {}, bay {}, {}", d.node_id, d.bay, d.disk_type))
            .collect();
        alerts.push(Alert {
            severity: AlertSeverity::Warning,
            cluster: cluster.name.clone(),
            message: format!("{} disk(s) unhealthy ({})", count, details.join("; ")),
            category: "disk_unhealthy".to_string(),
        });
    }
}

/// Unhealthy PSUs: warning with detail (node, location).
fn check_psu_health(cluster: &ClusterStatus, alerts: &mut Vec<Alert>) {
    if cluster.health.psus_unhealthy == 0 {
        return;
    }

    if cluster.health.unhealthy_psu_details.is_empty() {
        alerts.push(Alert {
            severity: AlertSeverity::Warning,
            cluster: cluster.name.clone(),
            message: format!("{} PSU(s) unhealthy", cluster.health.psus_unhealthy),
            category: "psu_unhealthy".to_string(),
        });
    } else {
        for psu in &cluster.health.unhealthy_psu_details {
            alerts.push(Alert {
                severity: AlertSeverity::Warning,
                cluster: cluster.name.clone(),
                message: format!("PSU issue (node {}, {})", psu.node_id, psu.location),
                category: "psu_unhealthy".to_string(),
            });
        }
    }
}

/// Protection degraded: 0 remaining node or drive failures.
fn check_protection_degraded(cluster: &ClusterStatus, alerts: &mut Vec<Alert>) {
    if let Some(remaining) = cluster.health.remaining_node_failures {
        if remaining == 0 {
            alerts.push(Alert {
                severity: AlertSeverity::Warning,
                cluster: cluster.name.clone(),
                message: "fault tolerance degraded (0 node failures remaining)".to_string(),
                category: "protection_degraded".to_string(),
            });
        }
    }
    if let Some(remaining) = cluster.health.remaining_drive_failures {
        if remaining == 0 {
            alerts.push(Alert {
                severity: AlertSeverity::Warning,
                cluster: cluster.name.clone(),
                message: "fault tolerance degraded (0 drive failures remaining)".to_string(),
                category: "protection_degraded".to_string(),
            });
        }
    }
}

/// Capacity projection: warn if days to full is within threshold.
fn check_capacity_projection(cluster: &ClusterStatus, alerts: &mut Vec<Alert>) {
    if let Some(ref projection) = cluster.capacity.projection {
        if capacity::should_warn(projection, &cluster.cluster_type) {
            alerts.push(Alert {
                severity: AlertSeverity::Warning,
                cluster: cluster.name.clone(),
                message: capacity::format_warning(projection, &cluster.cluster_type),
                category: "capacity_projection".to_string(),
            });
        }
    }
}

/// Sort alerts by severity: Critical (0) > Warning (1) > Info (2).
fn sort_alerts(alerts: &mut Vec<Alert>) {
    alerts.sort_by_key(|a| match a.severity {
        AlertSeverity::Critical => 0,
        AlertSeverity::Warning => 1,
        AlertSeverity::Info => 2,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to build a minimal healthy ClusterStatus for testing.
    fn make_cluster(name: &str) -> ClusterStatus {
        ClusterStatus {
            profile: name.to_string(),
            name: name.to_string(),
            uuid: "test-uuid".to_string(),
            version: "7.8.0".to_string(),
            cluster_type: ClusterType::OnPrem(vec!["C192T".to_string()]),
            reachable: true,
            stale: false,
            latency_ms: 42,
            nodes: NodeStatus {
                total: 5,
                online: 5,
                offline_nodes: vec![],
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
                remaining_node_failures: Some(1),
                remaining_drive_failures: Some(2),
                protection_type: Some("PROTECTION_SYSTEM_TYPE_EC".to_string()),
                unhealthy_disk_details: vec![],
                unhealthy_psu_details: vec![],
            },
        }
    }

    // ── Healthy cluster: no alerts ──────────────────────────────────

    #[test]
    fn test_healthy_cluster_no_alerts() {
        let cluster = make_cluster("healthy");
        let alerts = generate_alerts(&[cluster], vec![]);
        assert!(alerts.is_empty(), "healthy cluster should produce no alerts");
    }

    // ── Node offline alerts ─────────────────────────────────────────

    #[test]
    fn test_node_offline_with_ids() {
        let mut cluster = make_cluster("iss-sg");
        cluster.nodes.total = 6;
        cluster.nodes.online = 5;
        cluster.nodes.offline_nodes = vec![4];

        let alerts = generate_alerts(&[cluster], vec![]);
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].severity, AlertSeverity::Critical);
        assert_eq!(alerts[0].category, "node_offline");
        assert_eq!(alerts[0].cluster, "iss-sg");
        assert_eq!(alerts[0].message, "node 4: OFFLINE");
    }

    #[test]
    fn test_multiple_nodes_offline() {
        let mut cluster = make_cluster("iss-sg");
        cluster.nodes.total = 6;
        cluster.nodes.online = 4;
        cluster.nodes.offline_nodes = vec![3, 5];

        let alerts = generate_alerts(&[cluster], vec![]);
        let node_alerts: Vec<_> = alerts
            .iter()
            .filter(|a| a.category == "node_offline")
            .collect();
        assert_eq!(node_alerts.len(), 2);
        assert_eq!(node_alerts[0].message, "node 3: OFFLINE");
        assert_eq!(node_alerts[1].message, "node 5: OFFLINE");
    }

    #[test]
    fn test_node_offline_fallback_no_ids() {
        let mut cluster = make_cluster("test");
        cluster.nodes.total = 4;
        cluster.nodes.online = 2;
        cluster.nodes.offline_nodes = vec![]; // IDs not available

        let alerts = generate_alerts(&[cluster], vec![]);
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].severity, AlertSeverity::Critical);
        assert!(alerts[0].message.contains("2 node(s) offline"));
    }

    // ── Data at risk alerts ─────────────────────────────────────────

    #[test]
    fn test_data_at_risk_alert() {
        let mut cluster = make_cluster("critical-cluster");
        cluster.health.data_at_risk = true;
        cluster.health.status = HealthLevel::Critical;

        let alerts = generate_alerts(&[cluster], vec![]);
        let risk_alert = alerts.iter().find(|a| a.category == "data_at_risk");
        assert!(risk_alert.is_some());
        assert_eq!(risk_alert.unwrap().severity, AlertSeverity::Critical);
        assert!(risk_alert.unwrap().message.contains("DATA AT RISK"));
    }

    #[test]
    fn test_no_data_at_risk_no_alert() {
        let mut cluster = make_cluster("safe");
        cluster.health.data_at_risk = false;

        let alerts = generate_alerts(&[cluster], vec![]);
        assert!(
            !alerts.iter().any(|a| a.category == "data_at_risk"),
            "should not have data_at_risk alert when data is safe"
        );
    }

    // ── Disk health alerts ──────────────────────────────────────────

    #[test]
    fn test_disk_unhealthy_with_details() {
        let mut cluster = make_cluster("music");
        cluster.health.disks_unhealthy = 1;
        cluster.health.unhealthy_disk_details = vec![UnhealthyDisk {
            node_id: 3,
            bay: "12".to_string(),
            disk_type: "HDD".to_string(),
            state: "unhealthy".to_string(),
        }];

        let alerts = generate_alerts(&[cluster], vec![]);
        let disk_alert = alerts.iter().find(|a| a.category == "disk_unhealthy");
        assert!(disk_alert.is_some());
        assert_eq!(disk_alert.unwrap().severity, AlertSeverity::Warning);
        assert!(disk_alert.unwrap().message.contains("1 disk"));
        assert!(disk_alert.unwrap().message.contains("node 3, bay 12, HDD"));
    }

    #[test]
    fn test_multiple_disks_unhealthy() {
        let mut cluster = make_cluster("test");
        cluster.health.disks_unhealthy = 2;
        cluster.health.unhealthy_disk_details = vec![
            UnhealthyDisk {
                node_id: 1,
                bay: "3".to_string(),
                disk_type: "SSD".to_string(),
                state: "unhealthy".to_string(),
            },
            UnhealthyDisk {
                node_id: 2,
                bay: "7".to_string(),
                disk_type: "HDD".to_string(),
                state: "missing".to_string(),
            },
        ];

        let alerts = generate_alerts(&[cluster], vec![]);
        let disk_alert = alerts.iter().find(|a| a.category == "disk_unhealthy");
        assert!(disk_alert.is_some());
        let msg = &disk_alert.unwrap().message;
        assert!(msg.contains("2 disk(s)"));
        assert!(msg.contains("node 1, bay 3, SSD"));
        assert!(msg.contains("node 2, bay 7, HDD"));
    }

    #[test]
    fn test_disk_unhealthy_fallback_no_details() {
        let mut cluster = make_cluster("test");
        cluster.health.disks_unhealthy = 3;
        // No details available

        let alerts = generate_alerts(&[cluster], vec![]);
        let disk_alert = alerts.iter().find(|a| a.category == "disk_unhealthy");
        assert!(disk_alert.is_some());
        assert!(disk_alert.unwrap().message.contains("3 disk(s) unhealthy"));
    }

    #[test]
    fn test_no_unhealthy_disks_no_alert() {
        let cluster = make_cluster("healthy");
        let alerts = generate_alerts(&[cluster], vec![]);
        assert!(!alerts.iter().any(|a| a.category == "disk_unhealthy"));
    }

    // ── PSU health alerts ───────────────────────────────────────────

    #[test]
    fn test_psu_unhealthy_with_details() {
        let mut cluster = make_cluster("test");
        cluster.health.psus_unhealthy = 1;
        cluster.health.unhealthy_psu_details = vec![UnhealthyPsu {
            node_id: 2,
            location: "right".to_string(),
            name: "PSU1".to_string(),
            state: "FAILED".to_string(),
        }];

        let alerts = generate_alerts(&[cluster], vec![]);
        let psu_alert = alerts.iter().find(|a| a.category == "psu_unhealthy");
        assert!(psu_alert.is_some());
        assert_eq!(psu_alert.unwrap().severity, AlertSeverity::Warning);
        assert!(psu_alert.unwrap().message.contains("node 2"));
        assert!(psu_alert.unwrap().message.contains("right"));
    }

    #[test]
    fn test_multiple_psus_unhealthy() {
        let mut cluster = make_cluster("test");
        cluster.health.psus_unhealthy = 2;
        cluster.health.unhealthy_psu_details = vec![
            UnhealthyPsu {
                node_id: 1,
                location: "left".to_string(),
                name: "PSU2".to_string(),
                state: "DEGRADED".to_string(),
            },
            UnhealthyPsu {
                node_id: 3,
                location: "right".to_string(),
                name: "PSU1".to_string(),
                state: "FAILED".to_string(),
            },
        ];

        let alerts = generate_alerts(&[cluster], vec![]);
        let psu_alerts: Vec<_> = alerts
            .iter()
            .filter(|a| a.category == "psu_unhealthy")
            .collect();
        assert_eq!(psu_alerts.len(), 2);
        assert!(psu_alerts[0].message.contains("node 1"));
        assert!(psu_alerts[1].message.contains("node 3"));
    }

    #[test]
    fn test_psu_unhealthy_fallback_no_details() {
        let mut cluster = make_cluster("test");
        cluster.health.psus_unhealthy = 1;
        // No details

        let alerts = generate_alerts(&[cluster], vec![]);
        let psu_alert = alerts.iter().find(|a| a.category == "psu_unhealthy");
        assert!(psu_alert.is_some());
        assert!(psu_alert.unwrap().message.contains("1 PSU(s) unhealthy"));
    }

    #[test]
    fn test_no_unhealthy_psus_no_alert() {
        let cluster = make_cluster("healthy");
        let alerts = generate_alerts(&[cluster], vec![]);
        assert!(!alerts.iter().any(|a| a.category == "psu_unhealthy"));
    }

    // ── Protection degraded alerts ──────────────────────────────────

    #[test]
    fn test_protection_degraded_node_failures() {
        let mut cluster = make_cluster("test");
        cluster.health.remaining_node_failures = Some(0);

        let alerts = generate_alerts(&[cluster], vec![]);
        let prot_alert = alerts
            .iter()
            .find(|a| a.category == "protection_degraded" && a.message.contains("node"));
        assert!(prot_alert.is_some());
        assert_eq!(prot_alert.unwrap().severity, AlertSeverity::Warning);
    }

    #[test]
    fn test_protection_degraded_drive_failures() {
        let mut cluster = make_cluster("test");
        cluster.health.remaining_drive_failures = Some(0);

        let alerts = generate_alerts(&[cluster], vec![]);
        let prot_alert = alerts
            .iter()
            .find(|a| a.category == "protection_degraded" && a.message.contains("drive"));
        assert!(prot_alert.is_some());
        assert_eq!(prot_alert.unwrap().severity, AlertSeverity::Warning);
    }

    #[test]
    fn test_protection_healthy_no_alert() {
        let mut cluster = make_cluster("test");
        cluster.health.remaining_node_failures = Some(1);
        cluster.health.remaining_drive_failures = Some(2);

        let alerts = generate_alerts(&[cluster], vec![]);
        assert!(!alerts
            .iter()
            .any(|a| a.category == "protection_degraded"));
    }

    #[test]
    fn test_protection_none_no_alert() {
        let mut cluster = make_cluster("test");
        cluster.health.remaining_node_failures = None;
        cluster.health.remaining_drive_failures = None;

        let alerts = generate_alerts(&[cluster], vec![]);
        assert!(!alerts
            .iter()
            .any(|a| a.category == "protection_degraded"));
    }

    // ── Capacity projection alerts ──────────────────────────────────

    #[test]
    fn test_capacity_projection_onprem_warn() {
        let mut cluster = make_cluster("gravytrain-sg");
        cluster.capacity.projection = Some(CapacityProjection {
            days_until_full: Some(62),
            growth_rate_bytes_per_day: 1_200_000_000_000.0,
            confidence: ProjectionConfidence::High,
        });

        let alerts = generate_alerts(&[cluster], vec![]);
        let cap_alert = alerts
            .iter()
            .find(|a| a.category == "capacity_projection");
        assert!(cap_alert.is_some());
        assert_eq!(cap_alert.unwrap().severity, AlertSeverity::Warning);
    }

    #[test]
    fn test_capacity_projection_onprem_no_warn() {
        let mut cluster = make_cluster("gravytrain-sg");
        cluster.capacity.projection = Some(CapacityProjection {
            days_until_full: Some(200),
            growth_rate_bytes_per_day: 100_000_000_000.0,
            confidence: ProjectionConfidence::High,
        });

        let alerts = generate_alerts(&[cluster], vec![]);
        assert!(!alerts
            .iter()
            .any(|a| a.category == "capacity_projection"));
    }

    #[test]
    fn test_capacity_projection_cloud_warn() {
        let mut cluster = make_cluster("aws-grav");
        cluster.cluster_type = ClusterType::CnqAws;
        cluster.capacity.projection = Some(CapacityProjection {
            days_until_full: Some(5),
            growth_rate_bytes_per_day: 500_000_000_000.0,
            confidence: ProjectionConfidence::High,
        });

        let alerts = generate_alerts(&[cluster], vec![]);
        let cap_alert = alerts
            .iter()
            .find(|a| a.category == "capacity_projection");
        assert!(cap_alert.is_some());
    }

    #[test]
    fn test_capacity_projection_cloud_no_warn() {
        let mut cluster = make_cluster("aws-grav");
        cluster.cluster_type = ClusterType::CnqAws;
        cluster.capacity.projection = Some(CapacityProjection {
            days_until_full: Some(30),
            growth_rate_bytes_per_day: 100_000_000_000.0,
            confidence: ProjectionConfidence::High,
        });

        let alerts = generate_alerts(&[cluster], vec![]);
        assert!(!alerts
            .iter()
            .any(|a| a.category == "capacity_projection"));
    }

    #[test]
    fn test_no_projection_no_alert() {
        let cluster = make_cluster("test");
        let alerts = generate_alerts(&[cluster], vec![]);
        assert!(!alerts
            .iter()
            .any(|a| a.category == "capacity_projection"));
    }

    // ── Connectivity alerts pass-through ────────────────────────────

    #[test]
    fn test_connectivity_alerts_preserved() {
        let connectivity = vec![Alert {
            severity: AlertSeverity::Critical,
            cluster: "az-dev".to_string(),
            message: "unreachable and no cache: connection refused".to_string(),
            category: "connectivity".to_string(),
        }];

        let alerts = generate_alerts(&[], connectivity);
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].category, "connectivity");
        assert_eq!(alerts[0].severity, AlertSeverity::Critical);
    }

    // ── Alert sorting ───────────────────────────────────────────────

    #[test]
    fn test_alerts_sorted_critical_first() {
        let mut cluster = make_cluster("iss-sg");
        cluster.nodes.total = 6;
        cluster.nodes.online = 5;
        cluster.nodes.offline_nodes = vec![4];
        cluster.health.disks_unhealthy = 1;
        cluster.health.unhealthy_disk_details = vec![UnhealthyDisk {
            node_id: 3,
            bay: "12".to_string(),
            disk_type: "HDD".to_string(),
            state: "unhealthy".to_string(),
        }];

        let alerts = generate_alerts(&[cluster], vec![]);

        // Critical alerts come before warning alerts
        let mut saw_warning = false;
        for alert in &alerts {
            if alert.severity == AlertSeverity::Warning {
                saw_warning = true;
            }
            if alert.severity == AlertSeverity::Critical {
                assert!(
                    !saw_warning,
                    "critical alert '{}' should come before warning alerts",
                    alert.message
                );
            }
        }
    }

    #[test]
    fn test_mixed_severity_sorting() {
        let connectivity = vec![
            Alert {
                severity: AlertSeverity::Warning,
                cluster: "warn-cluster".to_string(),
                message: "using cached data".to_string(),
                category: "connectivity".to_string(),
            },
            Alert {
                severity: AlertSeverity::Critical,
                cluster: "crit-cluster".to_string(),
                message: "unreachable".to_string(),
                category: "connectivity".to_string(),
            },
        ];

        let mut cluster = make_cluster("test");
        cluster.health.data_at_risk = true;

        let alerts = generate_alerts(&[cluster], connectivity);

        // All critical alerts should be first
        let first_warning_idx = alerts
            .iter()
            .position(|a| a.severity == AlertSeverity::Warning);
        let last_critical_idx = alerts
            .iter()
            .rposition(|a| a.severity == AlertSeverity::Critical);

        if let (Some(warn_idx), Some(crit_idx)) = (first_warning_idx, last_critical_idx) {
            assert!(
                crit_idx < warn_idx,
                "all critical alerts should come before warning alerts"
            );
        }
    }

    // ── Multi-cluster alert generation ──────────────────────────────

    #[test]
    fn test_alerts_from_multiple_clusters() {
        let mut cluster1 = make_cluster("iss-sg");
        cluster1.nodes.total = 6;
        cluster1.nodes.online = 5;
        cluster1.nodes.offline_nodes = vec![4];

        let mut cluster2 = make_cluster("music");
        cluster2.health.disks_unhealthy = 1;
        cluster2.health.unhealthy_disk_details = vec![UnhealthyDisk {
            node_id: 3,
            bay: "12".to_string(),
            disk_type: "HDD".to_string(),
            state: "unhealthy".to_string(),
        }];

        let alerts = generate_alerts(&[cluster1, cluster2], vec![]);
        assert_eq!(alerts.len(), 2);

        // Node offline (critical) from iss-sg should come first
        assert_eq!(alerts[0].cluster, "iss-sg");
        assert_eq!(alerts[0].severity, AlertSeverity::Critical);
        // Disk unhealthy (warning) from music should come second
        assert_eq!(alerts[1].cluster, "music");
        assert_eq!(alerts[1].severity, AlertSeverity::Warning);
    }

    // ── Combined conditions ─────────────────────────────────────────

    #[test]
    fn test_cluster_with_all_issues() {
        let mut cluster = make_cluster("bad-cluster");
        // Offline node
        cluster.nodes.total = 6;
        cluster.nodes.online = 5;
        cluster.nodes.offline_nodes = vec![4];
        // Data at risk
        cluster.health.data_at_risk = true;
        // Unhealthy disk
        cluster.health.disks_unhealthy = 1;
        cluster.health.unhealthy_disk_details = vec![UnhealthyDisk {
            node_id: 3,
            bay: "12".to_string(),
            disk_type: "HDD".to_string(),
            state: "unhealthy".to_string(),
        }];
        // Unhealthy PSU
        cluster.health.psus_unhealthy = 1;
        cluster.health.unhealthy_psu_details = vec![UnhealthyPsu {
            node_id: 1,
            location: "right".to_string(),
            name: "PSU1".to_string(),
            state: "FAILED".to_string(),
        }];
        // Protection degraded
        cluster.health.remaining_node_failures = Some(0);
        // Capacity warning
        cluster.capacity.projection = Some(CapacityProjection {
            days_until_full: Some(30),
            growth_rate_bytes_per_day: 2_000_000_000_000.0,
            confidence: ProjectionConfidence::High,
        });

        let alerts = generate_alerts(&[cluster], vec![]);

        // Should have alerts for each condition
        let categories: Vec<&str> = alerts.iter().map(|a| a.category.as_str()).collect();
        assert!(categories.contains(&"node_offline"));
        assert!(categories.contains(&"data_at_risk"));
        assert!(categories.contains(&"disk_unhealthy"));
        assert!(categories.contains(&"psu_unhealthy"));
        assert!(categories.contains(&"protection_degraded"));
        assert!(categories.contains(&"capacity_projection"));

        // Critical alerts should be first
        let first_warning = alerts
            .iter()
            .position(|a| a.severity == AlertSeverity::Warning)
            .unwrap();
        let last_critical = alerts
            .iter()
            .rposition(|a| a.severity == AlertSeverity::Critical)
            .unwrap();
        assert!(last_critical < first_warning);
    }

    // ── Edge cases ──────────────────────────────────────────────────

    #[test]
    fn test_empty_clusters_only_connectivity() {
        let connectivity = vec![Alert {
            severity: AlertSeverity::Critical,
            cluster: "broken".to_string(),
            message: "unreachable".to_string(),
            category: "connectivity".to_string(),
        }];

        let alerts = generate_alerts(&[], connectivity);
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].cluster, "broken");
    }

    #[test]
    fn test_empty_everything_no_alerts() {
        let alerts = generate_alerts(&[], vec![]);
        assert!(alerts.is_empty());
    }

    #[test]
    fn test_stale_cluster_still_generates_alerts() {
        let mut cluster = make_cluster("stale-cluster");
        cluster.stale = true;
        cluster.reachable = false;
        cluster.health.disks_unhealthy = 1;
        cluster.health.unhealthy_disk_details = vec![UnhealthyDisk {
            node_id: 2,
            bay: "5".to_string(),
            disk_type: "SSD".to_string(),
            state: "unhealthy".to_string(),
        }];

        let alerts = generate_alerts(&[cluster], vec![]);
        assert!(alerts.iter().any(|a| a.category == "disk_unhealthy"));
    }
}
