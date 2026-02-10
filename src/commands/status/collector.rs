use std::time::Instant;

use anyhow::Result;
use serde_json::Value;

use crate::client::QumuloClient;
use crate::config::{Config, ProfileEntry};

use super::cache;
use super::detection::detect_cluster_type;
use super::types::*;

/// Collect status from all configured clusters (or a filtered subset) in parallel.
pub fn collect_all(
    config: &Config,
    profile_filters: &[String],
    timeout_secs: u64,
    no_cache: bool,
) -> Result<EnvironmentStatus> {
    // Determine which profiles to query
    let profiles: Vec<(String, ProfileEntry)> = if profile_filters.is_empty() {
        config
            .profiles
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    } else {
        profile_filters
            .iter()
            .filter_map(|name| {
                config
                    .profiles
                    .get(name)
                    .map(|entry| (name.clone(), entry.clone()))
            })
            .collect()
    };

    if profiles.is_empty() {
        anyhow::bail!("no matching profiles found — add profiles with `qontrol profile add`");
    }

    // Spawn one thread per cluster for parallel collection
    let results: Vec<ClusterResult> = std::thread::scope(|s| {
        let handles: Vec<_> = profiles
            .iter()
            .map(|(name, entry)| {
                let name = name.clone();
                let entry = entry.clone();
                s.spawn(move || collect_cluster(&name, &entry, timeout_secs))
            })
            .collect();

        handles
            .into_iter()
            .map(|h| {
                h.join().unwrap_or_else(|_| ClusterResult::Unreachable {
                    profile: "unknown".to_string(),
                    error: "thread panicked".to_string(),
                })
            })
            .collect()
    });

    // Process results: successes go into clusters, failures try cache fallback
    let mut clusters = Vec::new();
    let mut alerts = Vec::new();

    for result in results {
        match result {
            ClusterResult::Success { data, .. } => {
                let mut data = *data;
                // Write to cache on success
                if !no_cache {
                    if let Err(e) = cache::write_cache(&data.profile, &data) {
                        tracing::warn!(profile = %data.profile, error = %e, "failed to write cache");
                    }
                }
                data.stale = false;
                clusters.push(data);
            }
            ClusterResult::Unreachable { profile, error } => {
                tracing::warn!(%profile, %error, "cluster unreachable");
                // Try cache fallback
                if !no_cache {
                    if let Some(cached) = cache::read_cache(&profile) {
                        tracing::info!(%profile, cached_at = %cached.cached_at, "using cached data");
                        let mut data = cached.data;
                        data.stale = true;
                        data.reachable = false;
                        alerts.push(Alert {
                            severity: AlertSeverity::Warning,
                            cluster: profile.clone(),
                            message: format!(
                                "unreachable, using cached data from {}",
                                cached.cached_at
                            ),
                            category: "connectivity".to_string(),
                        });
                        clusters.push(data);
                    } else {
                        alerts.push(Alert {
                            severity: AlertSeverity::Critical,
                            cluster: profile.clone(),
                            message: format!("unreachable and no cache: {}", error),
                            category: "connectivity".to_string(),
                        });
                    }
                } else {
                    alerts.push(Alert {
                        severity: AlertSeverity::Critical,
                        cluster: profile.clone(),
                        message: format!("unreachable: {}", error),
                        category: "connectivity".to_string(),
                    });
                }
            }
        }
    }

    // Build aggregates
    let aggregates = build_aggregates(&clusters);

    // Add health alerts from per-cluster data
    for cluster in &clusters {
        if cluster.nodes.online < cluster.nodes.total {
            let offline = cluster.nodes.total - cluster.nodes.online;
            alerts.push(Alert {
                severity: AlertSeverity::Critical,
                cluster: cluster.name.clone(),
                message: format!("{} node(s) offline", offline),
                category: "node_offline".to_string(),
            });
        }
        if cluster.health.disks_unhealthy > 0 {
            alerts.push(Alert {
                severity: AlertSeverity::Warning,
                cluster: cluster.name.clone(),
                message: format!("{} disk(s) unhealthy", cluster.health.disks_unhealthy),
                category: "disk_unhealthy".to_string(),
            });
        }
        if cluster.health.psus_unhealthy > 0 {
            alerts.push(Alert {
                severity: AlertSeverity::Warning,
                cluster: cluster.name.clone(),
                message: format!("{} PSU(s) unhealthy", cluster.health.psus_unhealthy),
                category: "psu_unhealthy".to_string(),
            });
        }
        if cluster.health.data_at_risk {
            alerts.push(Alert {
                severity: AlertSeverity::Critical,
                cluster: cluster.name.clone(),
                message: "DATA AT RISK — restriper active".to_string(),
                category: "data_at_risk".to_string(),
            });
        }
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

    Ok(EnvironmentStatus {
        aggregates,
        alerts,
        clusters,
    })
}

/// Collect status from a single cluster. Returns a ClusterResult.
fn collect_cluster(profile: &str, entry: &ProfileEntry, timeout_secs: u64) -> ClusterResult {
    let start = Instant::now();

    let client = match QumuloClient::new(entry, timeout_secs) {
        Ok(c) => c,
        Err(e) => {
            return ClusterResult::Unreachable {
                profile: profile.to_string(),
                error: format!("failed to create client: {}", e),
            };
        }
    };

    // Fetch basic cluster data
    let settings = match client.get_cluster_settings() {
        Ok(v) => v,
        Err(e) => {
            return ClusterResult::Unreachable {
                profile: profile.to_string(),
                error: format!("{}", e),
            };
        }
    };

    let version = match client.get_version() {
        Ok(v) => v,
        Err(e) => {
            return ClusterResult::Unreachable {
                profile: profile.to_string(),
                error: format!("{}", e),
            };
        }
    };

    let nodes_data = match client.get_cluster_nodes() {
        Ok(v) => v,
        Err(e) => {
            return ClusterResult::Unreachable {
                profile: profile.to_string(),
                error: format!("{}", e),
            };
        }
    };

    let latency_ms = start.elapsed().as_millis() as u64;

    // Parse node data
    let nodes_array = nodes_data.as_array().map(|a| a.as_slice()).unwrap_or(&[]);
    let total_nodes = nodes_array.len();
    let online_nodes = nodes_array
        .iter()
        .filter(|n| {
            n["node_status"]
                .as_str()
                .map(|s| s.eq_ignore_ascii_case("online"))
                .unwrap_or(false)
        })
        .count();

    let cluster_type = detect_cluster_type(nodes_array);

    let cluster_name = settings["cluster_name"]
        .as_str()
        .unwrap_or("unknown")
        .to_string();
    let cluster_uuid = settings["cluster_uuid"].as_str().unwrap_or("").to_string();
    let version_str = version["revision_id"]
        .as_str()
        .unwrap_or("unknown")
        .to_string();

    // Fetch optional data — don't fail if these are unavailable
    let capacity = fetch_capacity(&client);
    let activity = fetch_activity(&client);

    // Fetch health data — each individually wrapped for error isolation
    let (unhealthy_disks, disk_details) = fetch_disk_health(&client);
    let (unhealthy_psus, psu_details) = fetch_psu_health(&client);
    let (remaining_node_failures, remaining_drive_failures, protection_type) =
        fetch_protection_status(&client);
    let data_at_risk = fetch_restriper_status(&client);

    // Build health status
    let mut issues = Vec::new();
    if online_nodes < total_nodes {
        issues.push(format!(
            "{} of {} nodes offline",
            total_nodes - online_nodes,
            total_nodes
        ));
    }
    if capacity.used_pct >= 90.0 {
        issues.push(format!("capacity at {:.0}%", capacity.used_pct));
    }
    if unhealthy_disks > 0 {
        for d in &disk_details {
            issues.push(format!(
                "disk unhealthy: node {}, bay {}, {}",
                d.node_id, d.bay, d.disk_type
            ));
        }
    }
    if unhealthy_psus > 0 {
        for p in &psu_details {
            issues.push(format!(
                "PSU issue: node {}, {} ({})",
                p.node_id, p.location, p.state
            ));
        }
    }
    if data_at_risk {
        issues.push("DATA AT RISK — restriper active".to_string());
    }
    if let Some(remaining) = remaining_node_failures {
        if remaining == 0 {
            issues.push("fault tolerance degraded (0 node failures remaining)".to_string());
        }
    }
    if let Some(remaining) = remaining_drive_failures {
        if remaining == 0 {
            issues.push("fault tolerance degraded (0 drive failures remaining)".to_string());
        }
    }

    let health_level = if data_at_risk || (online_nodes == 0 && total_nodes > 0) {
        HealthLevel::Critical
    } else if !issues.is_empty() {
        HealthLevel::Degraded
    } else {
        HealthLevel::Healthy
    };

    let data = ClusterStatus {
        profile: profile.to_string(),
        name: cluster_name,
        uuid: cluster_uuid,
        version: version_str,
        cluster_type,
        reachable: true,
        stale: false,
        latency_ms,
        nodes: NodeStatus {
            total: total_nodes,
            online: online_nodes,
        },
        capacity,
        activity,
        files: FileStats::default(),
        health: HealthStatus {
            status: health_level,
            issues,
            disks_unhealthy: unhealthy_disks,
            psus_unhealthy: unhealthy_psus,
            data_at_risk,
            remaining_node_failures,
            remaining_drive_failures,
            protection_type,
        },
    };

    ClusterResult::Success {
        data: Box::new(data),
        latency_ms,
    }
}

fn fetch_capacity(client: &QumuloClient) -> CapacityStatus {
    match client.get_file_system() {
        Ok(fs) => {
            let total = parse_byte_value(&fs["total_size_bytes"]);
            let free = parse_byte_value(&fs["free_size_bytes"]);
            let snapshot = parse_byte_value(&fs["snapshot_size_bytes"]);
            let used = total.saturating_sub(free);
            let pct = if total > 0 {
                used as f64 / total as f64 * 100.0
            } else {
                0.0
            };
            CapacityStatus {
                total_bytes: total,
                used_bytes: used,
                free_bytes: free,
                snapshot_bytes: snapshot,
                used_pct: pct,
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to fetch filesystem data");
            CapacityStatus::default()
        }
    }
}

fn fetch_activity(client: &QumuloClient) -> ActivityStatus {
    match client.get_activity_current() {
        Ok(activity) => {
            let entries = activity["entries"].as_array();
            match entries {
                Some(entries) if !entries.is_empty() => aggregate_activity(entries),
                _ => ActivityStatus::default(),
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to fetch activity data");
            ActivityStatus::default()
        }
    }
}

fn aggregate_activity(entries: &[Value]) -> ActivityStatus {
    let mut iops_read = 0.0_f64;
    let mut iops_write = 0.0_f64;
    let mut tp_read = 0.0_f64;
    let mut tp_write = 0.0_f64;
    let mut ips = std::collections::HashSet::new();

    for entry in entries {
        let rate = entry["rate"].as_f64().unwrap_or(0.0);
        let kind = entry["type"].as_str().unwrap_or("");
        if let Some(ip) = entry["ip"].as_str() {
            ips.insert(ip.to_string());
        }
        match kind {
            "file-iops-read" | "metadata-iops-read" => iops_read += rate,
            "file-iops-write" | "metadata-iops-write" => iops_write += rate,
            "file-throughput-read" => tp_read += rate,
            "file-throughput-write" => tp_write += rate,
            _ => {}
        }
    }

    ActivityStatus {
        iops_read,
        iops_write,
        throughput_read: tp_read,
        throughput_write: tp_write,
        connections: ips.len(),
    }
}

/// Fetch disk health from /v1/cluster/slots/.
/// Returns (unhealthy_count, details).
fn fetch_disk_health(client: &QumuloClient) -> (usize, Vec<UnhealthyDisk>) {
    match client.get_cluster_slots() {
        Ok(slots) => parse_disk_health(&slots),
        Err(e) => {
            tracing::warn!(error = %e, "failed to fetch disk health");
            (0, Vec::new())
        }
    }
}

/// Fetch PSU health from /v1/cluster/nodes/chassis/.
/// Cloud clusters return empty psu_statuses arrays — handled gracefully.
/// Returns (unhealthy_count, details).
fn fetch_psu_health(client: &QumuloClient) -> (usize, Vec<UnhealthyPsu>) {
    match client.get_cluster_chassis() {
        Ok(chassis) => parse_psu_health(&chassis),
        Err(e) => {
            tracing::warn!(error = %e, "failed to fetch PSU health");
            (0, Vec::new())
        }
    }
}

/// Fetch protection status from /v1/cluster/protection/status.
/// Returns (remaining_node_failures, remaining_drive_failures, protection_system_type).
fn fetch_protection_status(client: &QumuloClient) -> (Option<u64>, Option<u64>, Option<String>) {
    match client.get_cluster_protection_status() {
        Ok(prot) => parse_protection_status(&prot),
        Err(e) => {
            tracing::warn!(error = %e, "failed to fetch protection status");
            (None, None, None)
        }
    }
}

/// Fetch restriper status from /v1/cluster/restriper/status.
/// Returns true if data_at_risk is true.
fn fetch_restriper_status(client: &QumuloClient) -> bool {
    match client.get_cluster_restriper_status() {
        Ok(restriper) => parse_restriper_status(&restriper),
        Err(e) => {
            tracing::warn!(error = %e, "failed to fetch restriper status");
            false
        }
    }
}

fn parse_byte_value(val: &Value) -> u64 {
    match val {
        Value::String(s) => s.parse::<u64>().unwrap_or(0),
        Value::Number(n) => n.as_u64().unwrap_or(0),
        _ => 0,
    }
}

fn build_aggregates(clusters: &[ClusterStatus]) -> Aggregates {
    let reachable_count = clusters.iter().filter(|c| c.reachable).count();
    let total_nodes: usize = clusters.iter().map(|c| c.nodes.total).sum();
    let online_nodes: usize = clusters.iter().map(|c| c.nodes.online).sum();

    let mut cap = CapacityStatus::default();
    for c in clusters {
        cap.total_bytes += c.capacity.total_bytes;
        cap.used_bytes += c.capacity.used_bytes;
        cap.free_bytes += c.capacity.free_bytes;
        cap.snapshot_bytes += c.capacity.snapshot_bytes;
    }
    if cap.total_bytes > 0 {
        cap.used_pct = cap.used_bytes as f64 / cap.total_bytes as f64 * 100.0;
    }

    let mut files = FileStats::default();
    for c in clusters {
        files.total_files += c.files.total_files;
        files.total_directories += c.files.total_directories;
    }

    Aggregates {
        cluster_count: clusters.len(),
        reachable_count,
        total_nodes,
        online_nodes,
        capacity: cap,
        files,
    }
}

/// Parse disk health from a slots JSON array.
fn parse_disk_health(slots: &Value) -> (usize, Vec<UnhealthyDisk>) {
    let mut unhealthy = Vec::new();
    if let Some(arr) = slots.as_array() {
        for slot in arr {
            let state = slot["state"].as_str().unwrap_or("unknown");
            if !state.eq_ignore_ascii_case("healthy") {
                unhealthy.push(UnhealthyDisk {
                    node_id: slot["node_id"].as_u64().unwrap_or(0),
                    bay: slot["drive_bay"].as_str().unwrap_or("").to_string(),
                    disk_type: slot["disk_type"].as_str().unwrap_or("unknown").to_string(),
                    state: state.to_string(),
                });
            }
        }
    }
    let count = unhealthy.len();
    (count, unhealthy)
}

/// Parse PSU health from a chassis JSON array.
fn parse_psu_health(chassis: &Value) -> (usize, Vec<UnhealthyPsu>) {
    let mut unhealthy = Vec::new();
    if let Some(nodes) = chassis.as_array() {
        for node in nodes {
            let node_id = node["id"].as_u64().unwrap_or(0);
            if let Some(psus) = node["psu_statuses"].as_array() {
                for psu in psus {
                    let state = psu["state"].as_str().unwrap_or("unknown");
                    if !state.eq_ignore_ascii_case("GOOD") {
                        unhealthy.push(UnhealthyPsu {
                            node_id,
                            location: psu["location"].as_str().unwrap_or("unknown").to_string(),
                            name: psu["name"].as_str().unwrap_or("unknown").to_string(),
                            state: state.to_string(),
                        });
                    }
                }
            }
        }
    }
    let count = unhealthy.len();
    (count, unhealthy)
}

/// Parse protection status from a protection JSON object.
fn parse_protection_status(prot: &Value) -> (Option<u64>, Option<u64>, Option<String>) {
    let remaining_node = prot["remaining_node_failures"].as_u64();
    let remaining_drive = prot["remaining_drive_failures"].as_u64();
    let prot_type = prot["protection_system_type"]
        .as_str()
        .map(|s| s.to_string());
    (remaining_node, remaining_drive, prot_type)
}

/// Parse restriper status from a restriper JSON object.
fn parse_restriper_status(restriper: &Value) -> bool {
    restriper["data_at_risk"].as_bool().unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_disk_health_all_healthy() {
        let slots = json!([
            {"id": "1.1", "node_id": 1, "drive_bay": "1", "disk_type": "HDD", "state": "healthy"},
            {"id": "1.2", "node_id": 1, "drive_bay": "2", "disk_type": "SSD", "state": "healthy"}
        ]);
        let (count, details) = parse_disk_health(&slots);
        assert_eq!(count, 0);
        assert!(details.is_empty());
    }

    #[test]
    fn test_parse_disk_health_with_unhealthy() {
        let slots = json!([
            {"id": "1.1", "node_id": 1, "drive_bay": "1", "disk_type": "HDD", "state": "healthy"},
            {"id": "1.2", "node_id": 1, "drive_bay": "2", "disk_type": "HDD", "state": "unhealthy"},
            {"id": "2.1", "node_id": 2, "drive_bay": "1", "disk_type": "SSD", "state": "missing"}
        ]);
        let (count, details) = parse_disk_health(&slots);
        assert_eq!(count, 2);
        assert_eq!(details[0].node_id, 1);
        assert_eq!(details[0].bay, "2");
        assert_eq!(details[0].disk_type, "HDD");
        assert_eq!(details[0].state, "unhealthy");
        assert_eq!(details[1].node_id, 2);
        assert_eq!(details[1].state, "missing");
    }

    #[test]
    fn test_parse_disk_health_empty_array() {
        let slots = json!([]);
        let (count, details) = parse_disk_health(&slots);
        assert_eq!(count, 0);
        assert!(details.is_empty());
    }

    #[test]
    fn test_parse_psu_health_all_good() {
        let chassis = json!([
            {
                "id": 1,
                "psu_statuses": [
                    {"location": "left", "name": "PSU1", "state": "GOOD"},
                    {"location": "right", "name": "PSU2", "state": "GOOD"}
                ]
            }
        ]);
        let (count, details) = parse_psu_health(&chassis);
        assert_eq!(count, 0);
        assert!(details.is_empty());
    }

    #[test]
    fn test_parse_psu_health_with_failed() {
        let chassis = json!([
            {
                "id": 1,
                "psu_statuses": [
                    {"location": "left", "name": "PSU1", "state": "GOOD"},
                    {"location": "right", "name": "PSU2", "state": "FAILED"}
                ]
            },
            {
                "id": 2,
                "psu_statuses": [
                    {"location": "left", "name": "PSU1", "state": "DEGRADED"}
                ]
            }
        ]);
        let (count, details) = parse_psu_health(&chassis);
        assert_eq!(count, 2);
        assert_eq!(details[0].node_id, 1);
        assert_eq!(details[0].location, "right");
        assert_eq!(details[0].state, "FAILED");
        assert_eq!(details[1].node_id, 2);
        assert_eq!(details[1].state, "DEGRADED");
    }

    #[test]
    fn test_parse_psu_health_cloud_empty_arrays() {
        let chassis = json!([
            {"id": 1, "psu_statuses": []},
            {"id": 2, "psu_statuses": []},
            {"id": 3, "psu_statuses": []}
        ]);
        let (count, details) = parse_psu_health(&chassis);
        assert_eq!(count, 0);
        assert!(details.is_empty());
    }

    #[test]
    fn test_parse_protection_status_healthy() {
        let prot = json!({
            "protection_system_type": "PROTECTION_SYSTEM_TYPE_EC",
            "remaining_node_failures": 1,
            "remaining_drive_failures": 2
        });
        let (node, drive, ptype) = parse_protection_status(&prot);
        assert_eq!(node, Some(1));
        assert_eq!(drive, Some(2));
        assert_eq!(ptype, Some("PROTECTION_SYSTEM_TYPE_EC".to_string()));
    }

    #[test]
    fn test_parse_protection_status_degraded() {
        let prot = json!({
            "protection_system_type": "PROTECTION_SYSTEM_TYPE_EC",
            "remaining_node_failures": 0,
            "remaining_drive_failures": 0
        });
        let (node, drive, _) = parse_protection_status(&prot);
        assert_eq!(node, Some(0));
        assert_eq!(drive, Some(0));
    }

    #[test]
    fn test_parse_protection_status_object_backed() {
        let prot = json!({
            "protection_system_type": "PROTECTION_SYSTEM_TYPE_OBJECT_BACKED",
            "remaining_node_failures": 1,
            "remaining_drive_failures": 1
        });
        let (_, _, ptype) = parse_protection_status(&prot);
        assert_eq!(
            ptype,
            Some("PROTECTION_SYSTEM_TYPE_OBJECT_BACKED".to_string())
        );
    }

    #[test]
    fn test_parse_restriper_status_not_running() {
        let restriper = json!({
            "data_at_risk": false,
            "status": "NOT_RUNNING"
        });
        assert!(!parse_restriper_status(&restriper));
    }

    #[test]
    fn test_parse_restriper_status_data_at_risk() {
        let restriper = json!({
            "data_at_risk": true,
            "status": "RUNNING",
            "percent_complete": 20
        });
        assert!(parse_restriper_status(&restriper));
    }

    #[test]
    fn test_parse_restriper_status_missing_field() {
        let restriper = json!({"status": "NOT_RUNNING"});
        assert!(!parse_restriper_status(&restriper));
    }
}
