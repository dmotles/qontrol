use std::time::Instant;

use anyhow::Result;
use serde_json::Value;

use crate::client::QumuloClient;
use crate::config::{Config, ProfileEntry};

use super::cache;
use super::capacity;
use super::detection::detect_cluster_type;
use super::types::*;

/// Per-node NIC stats: (throughput_bps, link_speed_bps, utilization_pct)
type NicStatsMap = std::collections::HashMap<u64, (Option<u64>, Option<u64>, Option<f64>)>;

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

    // Add capacity projection alerts
    for cluster in &clusters {
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
    let mut capacity = fetch_capacity(&client);
    let activity = fetch_activity(&client);
    let node_details = fetch_node_network_details(&client, &cluster_type);

    // Fetch capacity history and compute projection
    capacity.projection = fetch_capacity_projection(
        &client,
        capacity.used_bytes,
        capacity.total_bytes,
        &cluster_type,
    );

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
            details: node_details,
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

fn fetch_capacity_projection(
    client: &QumuloClient,
    current_used: u64,
    total_capacity: u64,
    _cluster_type: &ClusterType,
) -> Option<CapacityProjection> {
    if total_capacity == 0 {
        return None;
    }
    // Fetch 30 days of history
    let now = chrono::Utc::now().timestamp();
    let thirty_days_ago = now - 30 * 86400;
    match client.get_capacity_history(thirty_days_ago) {
        Ok(history) => capacity::compute_projection(&history, current_used, total_capacity),
        Err(e) => {
            tracing::warn!(error = %e, "failed to fetch capacity history");
            None
        }
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
                projection: None,
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

/// Collect per-node network details: connections + NIC stats.
fn fetch_node_network_details(
    client: &QumuloClient,
    cluster_type: &ClusterType,
) -> Vec<NodeNetworkInfo> {
    let connections_by_node = fetch_connections_per_node(client);
    let nic_stats_by_node = fetch_nic_stats_per_node(client, cluster_type);

    // Merge connection data and NIC data by node_id
    let mut node_ids: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
    for id in connections_by_node.keys() {
        node_ids.insert(*id);
    }
    for id in nic_stats_by_node.keys() {
        node_ids.insert(*id);
    }

    node_ids
        .into_iter()
        .map(|node_id| {
            let (connections, breakdown) = connections_by_node
                .get(&node_id)
                .cloned()
                .unwrap_or_default();

            let (throughput, link_speed, utilization) = nic_stats_by_node
                .get(&node_id)
                .cloned()
                .unwrap_or((None, None, None));

            NodeNetworkInfo {
                node_id,
                connections,
                connection_breakdown: breakdown,
                nic_throughput_bps: throughput,
                nic_link_speed_bps: link_speed,
                nic_utilization_pct: utilization,
            }
        })
        .collect()
}

/// Parse connections response: array of {id, connections: [{type, ...}]}
/// Returns map of node_id → (total_connections, breakdown_by_protocol)
fn fetch_connections_per_node(
    client: &QumuloClient,
) -> std::collections::HashMap<u64, (u32, std::collections::HashMap<String, u32>)> {
    let mut result = std::collections::HashMap::new();

    let data = match client.get_network_connections() {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "failed to fetch network connections");
            return result;
        }
    };

    let nodes = match data.as_array() {
        Some(a) => a,
        None => return result,
    };

    for node in nodes {
        let node_id = match node["id"].as_u64() {
            Some(id) => id,
            None => continue,
        };

        let conns = match node["connections"].as_array() {
            Some(c) => c,
            None => {
                result.insert(node_id, (0, std::collections::HashMap::new()));
                continue;
            }
        };

        let total = conns.len() as u32;
        let mut breakdown: std::collections::HashMap<String, u32> =
            std::collections::HashMap::new();

        for conn in conns {
            if let Some(conn_type) = conn["type"].as_str() {
                let protocol = normalize_connection_type(conn_type);
                *breakdown.entry(protocol).or_insert(0) += 1;
            }
        }

        result.insert(node_id, (total, breakdown));
    }

    result
}

/// Strip "CONNECTION_TYPE_" prefix for cleaner display.
fn normalize_connection_type(raw: &str) -> String {
    raw.strip_prefix("CONNECTION_TYPE_")
        .unwrap_or(raw)
        .to_string()
}

/// Fetch NIC stats for each node. For on-prem, also extracts link speed and computes utilization.
/// Uses the bond0 device (primary frontend/backend interface).
/// Returns map of node_id → (throughput_bps, link_speed_bps, utilization_pct)
fn fetch_nic_stats_per_node(client: &QumuloClient, cluster_type: &ClusterType) -> NicStatsMap {
    let mut result = std::collections::HashMap::new();
    let is_cloud = matches!(cluster_type, ClusterType::CnqAws | ClusterType::AnqAzure);

    // First call to get baseline byte counters
    let data1 = match client.get_network_status() {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "failed to fetch network status");
            return result;
        }
    };

    // Sleep 1 second then make second call for throughput delta
    std::thread::sleep(std::time::Duration::from_secs(1));

    let data2 = match client.get_network_status() {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "failed to fetch network status (2nd call)");
            // Fall back to first call data only (no throughput)
            parse_nic_data_single(&data1, is_cloud, &mut result);
            return result;
        }
    };

    // Parse both calls and compute throughput delta
    parse_nic_data_delta(&data1, &data2, is_cloud, &mut result);

    result
}

/// Parse NIC data from a single call (no throughput available).
fn parse_nic_data_single(data: &Value, is_cloud: bool, result: &mut NicStatsMap) {
    let nodes = match data.as_array() {
        Some(a) => a,
        None => return,
    };

    for node in nodes {
        let node_id = match node["node_id"].as_u64() {
            Some(id) => id,
            None => continue,
        };

        let (link_speed_bps, _) = extract_bond0_stats(node);
        let link_speed = if is_cloud { None } else { link_speed_bps };

        result.insert(node_id, (None, link_speed, None));
    }
}

/// Parse NIC data from two calls and compute throughput delta.
fn parse_nic_data_delta(data1: &Value, data2: &Value, is_cloud: bool, result: &mut NicStatsMap) {
    let nodes1 = match data1.as_array() {
        Some(a) => a,
        None => return,
    };
    let nodes2 = match data2.as_array() {
        Some(a) => a,
        None => return,
    };

    // Index second call by node_id
    let mut second_by_id: std::collections::HashMap<u64, &Value> = std::collections::HashMap::new();
    for node in nodes2 {
        if let Some(id) = node["node_id"].as_u64() {
            second_by_id.insert(id, node);
        }
    }

    for node1 in nodes1 {
        let node_id = match node1["node_id"].as_u64() {
            Some(id) => id,
            None => continue,
        };

        let (link_speed_bps, total_bytes_1) = extract_bond0_stats(node1);
        let link_speed = if is_cloud { None } else { link_speed_bps };

        let throughput = if let Some(node2) = second_by_id.get(&node_id) {
            let (_, total_bytes_2) = extract_bond0_stats(node2);
            // throughput = delta bytes * 8 (convert to bits) / 1 second
            let delta = total_bytes_2.saturating_sub(total_bytes_1);
            Some(delta * 8)
        } else {
            None
        };

        let utilization = match (throughput, link_speed) {
            (Some(tp), Some(ls)) if ls > 0 => Some(tp as f64 / ls as f64 * 100.0),
            _ => None,
        };

        result.insert(node_id, (throughput, link_speed, utilization));
    }
}

/// Extract bond0 (or first frontend device) stats from a node entry.
/// Returns (link_speed_bps, total_bytes_sent_plus_received).
fn extract_bond0_stats(node: &Value) -> (Option<u64>, u64) {
    let devices = match node["devices"].as_array() {
        Some(d) => d,
        None => return (None, 0),
    };

    // Find the primary frontend device (bond0, or first device with FRONTEND use)
    let device = devices.iter().find(|d| {
        let name = d["name"].as_str().unwrap_or("");
        let use_for = d
            .get("network_details")
            .and_then(|nd| nd["use_for"].as_str())
            .unwrap_or("");
        name == "bond0" || use_for == "FRONTEND_AND_BACKEND" || use_for == "FRONTEND"
    });

    let device = match device {
        Some(d) => d,
        None => return (None, 0),
    };

    let bytes_sent = parse_byte_value(&device["bytes_sent"]);
    let bytes_received = parse_byte_value(&device["bytes_received"]);
    let total_bytes = bytes_sent + bytes_received;

    // Speed is in Mbps as a string, convert to bps
    let link_speed_bps = device["speed"]
        .as_str()
        .and_then(|s| s.parse::<u64>().ok())
        .map(|mbps| mbps * 1_000_000);

    (link_speed_bps, total_bytes)
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

    #[test]
    fn test_normalize_connection_type() {
        assert_eq!(normalize_connection_type("CONNECTION_TYPE_NFS"), "NFS");
        assert_eq!(normalize_connection_type("CONNECTION_TYPE_SMB"), "SMB");
        assert_eq!(normalize_connection_type("CONNECTION_TYPE_REST"), "REST");
        assert_eq!(normalize_connection_type("CONNECTION_TYPE_S3"), "S3");
        assert_eq!(normalize_connection_type("CONNECTION_TYPE_FTP"), "FTP");
        assert_eq!(normalize_connection_type("UNKNOWN"), "UNKNOWN");
    }

    #[test]
    fn test_parse_connections_multiple_nodes() {
        let data = json!([
            {
                "id": 1,
                "connections": [
                    {"type": "CONNECTION_TYPE_NFS", "network_address": "10.0.0.1", "tenant_id": 1},
                    {"type": "CONNECTION_TYPE_NFS", "network_address": "10.0.0.2", "tenant_id": 1},
                    {"type": "CONNECTION_TYPE_REST", "network_address": "127.0.0.1", "tenant_id": 1}
                ]
            },
            {
                "id": 2,
                "connections": [
                    {"type": "CONNECTION_TYPE_SMB", "network_address": "10.0.0.3", "tenant_id": 1}
                ]
            },
            {
                "id": 3,
                "connections": []
            }
        ]);

        // Manually parse the connections (same logic as fetch_connections_per_node)
        let nodes = data.as_array().unwrap();
        let mut result = std::collections::HashMap::new();

        for node in nodes {
            let node_id = node["id"].as_u64().unwrap();
            let conns = node["connections"].as_array().unwrap();
            let total = conns.len() as u32;
            let mut breakdown: std::collections::HashMap<String, u32> =
                std::collections::HashMap::new();
            for conn in conns {
                if let Some(conn_type) = conn["type"].as_str() {
                    let protocol = normalize_connection_type(conn_type);
                    *breakdown.entry(protocol).or_insert(0) += 1;
                }
            }
            result.insert(node_id, (total, breakdown));
        }

        // Node 1: 3 total (2 NFS, 1 REST)
        let (count, breakdown) = result.get(&1).unwrap();
        assert_eq!(*count, 3);
        assert_eq!(breakdown.get("NFS"), Some(&2));
        assert_eq!(breakdown.get("REST"), Some(&1));

        // Node 2: 1 total (1 SMB)
        let (count, breakdown) = result.get(&2).unwrap();
        assert_eq!(*count, 1);
        assert_eq!(breakdown.get("SMB"), Some(&1));

        // Node 3: 0 connections
        let (count, _) = result.get(&3).unwrap();
        assert_eq!(*count, 0);
    }

    #[test]
    fn test_extract_bond0_stats_on_prem() {
        let node = json!({
            "node_id": 1,
            "devices": [{
                "name": "bond0",
                "bytes_sent": "100000",
                "bytes_received": "200000",
                "speed": "200000",
                "interface_status": "UP",
                "network_details": {"use_for": "FRONTEND_AND_BACKEND"}
            }]
        });

        let (link_speed, total_bytes) = extract_bond0_stats(&node);
        assert_eq!(link_speed, Some(200_000_000_000)); // 200000 Mbps = 200 Gbps
        assert_eq!(total_bytes, 300_000); // 100000 + 200000
    }

    #[test]
    fn test_extract_bond0_stats_cloud_no_speed() {
        let node = json!({
            "node_id": 1,
            "devices": [{
                "name": "bond0",
                "bytes_sent": "50000",
                "bytes_received": "75000",
                "speed": null,
                "interface_status": "UP",
                "network_details": {"use_for": "FRONTEND_AND_BACKEND"}
            }]
        });

        let (link_speed, total_bytes) = extract_bond0_stats(&node);
        assert_eq!(link_speed, None);
        assert_eq!(total_bytes, 125_000);
    }

    #[test]
    fn test_extract_bond0_stats_mixed_devices() {
        // Cloud clusters have multiple devices; only bond0 should be used
        let node = json!({
            "node_id": 1,
            "devices": [
                {
                    "name": "bond0",
                    "bytes_sent": "1000",
                    "bytes_received": "2000",
                    "speed": null,
                    "interface_status": "UP",
                    "network_details": {"use_for": "FRONTEND_AND_BACKEND"}
                },
                {
                    "name": "ens5",
                    "bytes_sent": "999",
                    "bytes_received": "999",
                    "speed": null,
                    "interface_status": "UP",
                    "network_details": {"upper_interface_name": "bond0", "use_for": "UNDERLYING"}
                },
                {
                    "name": "lo",
                    "bytes_sent": "500",
                    "bytes_received": "500",
                    "speed": null,
                    "interface_status": "UNKNOWN",
                    "network_details": {"use_for": "NONE"}
                }
            ]
        });

        let (_, total_bytes) = extract_bond0_stats(&node);
        assert_eq!(total_bytes, 3000); // bond0: 1000 + 2000
    }

    #[test]
    fn test_parse_nic_data_delta_on_prem() {
        let data1 = json!([{
            "node_id": 1,
            "devices": [{
                "name": "bond0",
                "bytes_sent": "1000000",
                "bytes_received": "2000000",
                "speed": "100000",
                "interface_status": "UP",
                "network_details": {"use_for": "FRONTEND_AND_BACKEND"}
            }]
        }]);

        let data2 = json!([{
            "node_id": 1,
            "devices": [{
                "name": "bond0",
                "bytes_sent": "1125000",
                "bytes_received": "2125000",
                "speed": "100000",
                "interface_status": "UP",
                "network_details": {"use_for": "FRONTEND_AND_BACKEND"}
            }]
        }]);

        let mut result = std::collections::HashMap::new();
        parse_nic_data_delta(&data1, &data2, false, &mut result);

        let (throughput, link_speed, utilization) = result.get(&1).unwrap();
        // Delta: (1125000+2125000) - (1000000+2000000) = 250000 bytes
        // Throughput: 250000 * 8 = 2_000_000 bps
        assert_eq!(*throughput, Some(2_000_000));
        assert_eq!(*link_speed, Some(100_000_000_000)); // 100 Gbps
                                                        // Utilization: 2_000_000 / 100_000_000_000 * 100 = 0.002%
        assert!(utilization.unwrap() < 0.01);
    }

    #[test]
    fn test_parse_nic_data_delta_cloud() {
        let data1 = json!([{
            "node_id": 1,
            "devices": [{
                "name": "bond0",
                "bytes_sent": "1000000",
                "bytes_received": "2000000",
                "speed": null,
                "interface_status": "UP",
                "network_details": {"use_for": "FRONTEND_AND_BACKEND"}
            }]
        }]);

        let data2 = json!([{
            "node_id": 1,
            "devices": [{
                "name": "bond0",
                "bytes_sent": "1100000",
                "bytes_received": "2100000",
                "speed": null,
                "interface_status": "UP",
                "network_details": {"use_for": "FRONTEND_AND_BACKEND"}
            }]
        }]);

        let mut result = std::collections::HashMap::new();
        parse_nic_data_delta(&data1, &data2, true, &mut result);

        let (throughput, link_speed, utilization) = result.get(&1).unwrap();
        assert_eq!(*throughput, Some(1_600_000)); // (200000 delta) * 8
        assert_eq!(*link_speed, None); // cloud = no link speed
        assert_eq!(*utilization, None); // cloud = no utilization
    }

    #[test]
    fn test_parse_nic_data_single_on_prem() {
        let data = json!([{
            "node_id": 1,
            "devices": [{
                "name": "bond0",
                "bytes_sent": "50000",
                "bytes_received": "75000",
                "speed": "200000",
                "interface_status": "UP",
                "network_details": {"use_for": "FRONTEND_AND_BACKEND"}
            }]
        }]);

        let mut result = std::collections::HashMap::new();
        parse_nic_data_single(&data, false, &mut result);

        let (throughput, link_speed, utilization) = result.get(&1).unwrap();
        assert_eq!(*throughput, None); // single call = no throughput
        assert_eq!(*link_speed, Some(200_000_000_000));
        assert_eq!(*utilization, None); // no throughput = no utilization
    }

    #[test]
    fn test_extract_bond0_stats_no_devices() {
        let node = json!({"node_id": 1, "devices": []});
        let (link_speed, total_bytes) = extract_bond0_stats(&node);
        assert_eq!(link_speed, None);
        assert_eq!(total_bytes, 0);
    }

    #[test]
    fn test_extract_bond0_stats_missing_devices() {
        let node = json!({"node_id": 1});
        let (link_speed, total_bytes) = extract_bond0_stats(&node);
        assert_eq!(link_speed, None);
        assert_eq!(total_bytes, 0);
    }

    #[test]
    fn test_utilization_calculation() {
        // 12.4 Gbps throughput on 200 Gbps link = 6.2%
        let throughput_bps: u64 = 12_400_000_000;
        let link_speed_bps: u64 = 200_000_000_000;
        let utilization = throughput_bps as f64 / link_speed_bps as f64 * 100.0;
        assert!((utilization - 6.2).abs() < 0.01);
    }

    #[test]
    fn test_mixed_link_speeds_across_nodes() {
        let data1 = json!([
            {
                "node_id": 1,
                "devices": [{"name": "bond0", "bytes_sent": "1000", "bytes_received": "1000", "speed": "200000", "interface_status": "UP", "network_details": {"use_for": "FRONTEND_AND_BACKEND"}}]
            },
            {
                "node_id": 2,
                "devices": [{"name": "bond0", "bytes_sent": "1000", "bytes_received": "1000", "speed": "100000", "interface_status": "UP", "network_details": {"use_for": "FRONTEND_AND_BACKEND"}}]
            }
        ]);

        let data2 = json!([
            {
                "node_id": 1,
                "devices": [{"name": "bond0", "bytes_sent": "2000", "bytes_received": "2000", "speed": "200000", "interface_status": "UP", "network_details": {"use_for": "FRONTEND_AND_BACKEND"}}]
            },
            {
                "node_id": 2,
                "devices": [{"name": "bond0", "bytes_sent": "2000", "bytes_received": "2000", "speed": "100000", "interface_status": "UP", "network_details": {"use_for": "FRONTEND_AND_BACKEND"}}]
            }
        ]);

        let mut result = std::collections::HashMap::new();
        parse_nic_data_delta(&data1, &data2, false, &mut result);

        let (_, link1, _) = result.get(&1).unwrap();
        let (_, link2, _) = result.get(&2).unwrap();
        assert_eq!(*link1, Some(200_000_000_000)); // 200 Gbps
        assert_eq!(*link2, Some(100_000_000_000)); // 100 Gbps
    }
}
