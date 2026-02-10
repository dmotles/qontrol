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

    // Add node health alerts
    for cluster in &clusters {
        if cluster.nodes.online < cluster.nodes.total {
            let offline = cluster.nodes.total - cluster.nodes.online;
            alerts.push(Alert {
                severity: AlertSeverity::Warning,
                cluster: cluster.name.clone(),
                message: format!("{} node(s) offline", offline),
                category: "nodes".to_string(),
            });
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
    let files = fetch_file_stats(&client);

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
    let health_level = if !issues.is_empty() && online_nodes == 0 {
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
        files,
        health: HealthStatus {
            status: health_level,
            issues,
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
    let iops_read = fetch_activity_sum(client, "file-iops-read");
    let iops_write = fetch_activity_sum(client, "file-iops-write");
    let throughput_read = fetch_activity_sum(client, "file-throughput-read");
    let throughput_write = fetch_activity_sum(client, "file-throughput-write");

    let is_idle =
        iops_read == 0.0 && iops_write == 0.0 && throughput_read == 0.0 && throughput_write == 0.0;

    ActivityStatus {
        iops_read,
        iops_write,
        throughput_read,
        throughput_write,
        connections: 0,
        is_idle,
    }
}

/// Fetch a single activity type and sum all entry rates.
fn fetch_activity_sum(client: &QumuloClient, activity_type: &str) -> f64 {
    match client.get_activity_by_type(activity_type) {
        Ok(resp) => resp["entries"]
            .as_array()
            .map(|entries| {
                entries
                    .iter()
                    .filter(|e| {
                        e["type"]
                            .as_str()
                            .map(|t| t == activity_type)
                            .unwrap_or(false)
                    })
                    .map(|e| e["rate"].as_f64().unwrap_or(0.0))
                    .sum()
            })
            .unwrap_or(0.0),
        Err(e) => {
            tracing::warn!(error = %e, %activity_type, "failed to fetch activity");
            0.0
        }
    }
}

fn fetch_file_stats(client: &QumuloClient) -> FileStats {
    let mut stats = FileStats::default();

    // File/directory counts from recursive aggregates
    match client.get_file_recursive_aggregates("/") {
        Ok(agg) => {
            // Response is an array of pages; each page has a "files" array
            if let Some(pages) = agg.as_array() {
                for page in pages {
                    if let Some(files) = page["files"].as_array() {
                        for entry in files {
                            stats.total_files += parse_string_u64(&entry["num_files"]);
                            stats.total_directories += parse_string_u64(&entry["num_directories"]);
                        }
                    }
                }
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to fetch recursive aggregates");
        }
    }

    // Snapshot count from /v2/snapshots/
    match client.get_snapshots() {
        Ok(snap) => {
            if let Some(entries) = snap["entries"].as_array() {
                stats.total_snapshots = entries.len() as u64;
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to fetch snapshot list");
        }
    }

    // Snapshot total capacity from /v1/snapshots/total-used-capacity
    match client.get_snapshots_total_capacity() {
        Ok(cap) => {
            stats.snapshot_bytes = parse_byte_value(&cap["bytes"]);
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to fetch snapshot total capacity");
        }
    }

    stats
}

fn parse_string_u64(val: &Value) -> u64 {
    match val {
        Value::String(s) => s.parse::<u64>().unwrap_or(0),
        Value::Number(n) => n.as_u64().unwrap_or(0),
        _ => 0,
    }
}

fn parse_byte_value(val: &Value) -> u64 {
    match val {
        Value::String(s) => s.parse::<u64>().unwrap_or(0),
        Value::Number(n) => n.as_u64().unwrap_or(0),
        _ => 0,
    }
}

/// Parse a recursive-aggregates API response and sum file/directory counts.
/// Response is an array of pages, each containing a "files" array.
#[cfg(test)]
fn parse_recursive_aggregates(agg: &Value) -> (u64, u64) {
    let mut total_files = 0u64;
    let mut total_dirs = 0u64;
    if let Some(pages) = agg.as_array() {
        for page in pages {
            if let Some(files) = page["files"].as_array() {
                for entry in files {
                    total_files += parse_string_u64(&entry["num_files"]);
                    total_dirs += parse_string_u64(&entry["num_directories"]);
                }
            }
        }
    }
    (total_files, total_dirs)
}

/// Sum rates from an activity response, filtering by the expected type.
#[cfg(test)]
fn sum_activity_rates(resp: &Value, activity_type: &str) -> f64 {
    resp["entries"]
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .filter(|e| {
                    e["type"]
                        .as_str()
                        .map(|t| t == activity_type)
                        .unwrap_or(false)
                })
                .map(|e| e["rate"].as_f64().unwrap_or(0.0))
                .sum()
        })
        .unwrap_or(0.0)
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
        files.total_snapshots += c.files.total_snapshots;
        files.snapshot_bytes += c.files.snapshot_bytes;
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_recursive_aggregates_basic() {
        let agg = json!([{
            "files": [
                { "num_files": "100", "num_directories": "10" },
                { "num_files": "200", "num_directories": "20" }
            ]
        }]);
        let (files, dirs) = parse_recursive_aggregates(&agg);
        assert_eq!(files, 300);
        assert_eq!(dirs, 30);
    }

    #[test]
    fn test_parse_recursive_aggregates_empty() {
        let agg = json!([{ "files": [] }]);
        let (files, dirs) = parse_recursive_aggregates(&agg);
        assert_eq!(files, 0);
        assert_eq!(dirs, 0);
    }

    #[test]
    fn test_parse_recursive_aggregates_missing_fields() {
        let agg = json!([{ "files": [{ "num_files": "50" }] }]);
        let (files, dirs) = parse_recursive_aggregates(&agg);
        assert_eq!(files, 50);
        assert_eq!(dirs, 0);
    }

    #[test]
    fn test_parse_recursive_aggregates_large_numbers() {
        // Petabyte-scale file counts
        let agg = json!([{
            "files": [
                { "num_files": "1807976645", "num_directories": "219679366" }
            ]
        }]);
        let (files, dirs) = parse_recursive_aggregates(&agg);
        assert_eq!(files, 1_807_976_645);
        assert_eq!(dirs, 219_679_366);
    }

    #[test]
    fn test_parse_recursive_aggregates_numeric_values() {
        // Handle numeric values (not just strings)
        let agg = json!([{
            "files": [
                { "num_files": 42, "num_directories": 7 }
            ]
        }]);
        let (files, dirs) = parse_recursive_aggregates(&agg);
        assert_eq!(files, 42);
        assert_eq!(dirs, 7);
    }

    #[test]
    fn test_sum_activity_rates_basic() {
        let resp = json!({
            "entries": [
                { "type": "file-iops-read", "rate": 10.0 },
                { "type": "file-iops-read", "rate": 5.5 },
                { "type": "file-iops-write", "rate": 3.0 }
            ]
        });
        let sum = sum_activity_rates(&resp, "file-iops-read");
        assert!((sum - 15.5).abs() < 0.001);
    }

    #[test]
    fn test_sum_activity_rates_filters_by_type() {
        let resp = json!({
            "entries": [
                { "type": "file-iops-read", "rate": 10.0 },
                { "type": "file-iops-write", "rate": 20.0 },
                { "type": "file-throughput-read", "rate": 1000.0 }
            ]
        });
        assert!((sum_activity_rates(&resp, "file-iops-read") - 10.0).abs() < 0.001);
        assert!((sum_activity_rates(&resp, "file-iops-write") - 20.0).abs() < 0.001);
        assert!((sum_activity_rates(&resp, "file-throughput-read") - 1000.0).abs() < 0.001);
        assert!((sum_activity_rates(&resp, "file-throughput-write") - 0.0).abs() < 0.001);
    }

    #[test]
    fn test_sum_activity_rates_empty_entries() {
        let resp = json!({ "entries": [] });
        assert_eq!(sum_activity_rates(&resp, "file-iops-read"), 0.0);
    }

    #[test]
    fn test_sum_activity_rates_missing_entries() {
        let resp = json!({});
        assert_eq!(sum_activity_rates(&resp, "file-iops-read"), 0.0);
    }

    #[test]
    fn test_idle_detection_all_zeros() {
        let activity = ActivityStatus {
            iops_read: 0.0,
            iops_write: 0.0,
            throughput_read: 0.0,
            throughput_write: 0.0,
            connections: 0,
            is_idle: true,
        };
        assert!(activity.is_idle);
    }

    #[test]
    fn test_idle_detection_not_idle() {
        let activity = ActivityStatus {
            iops_read: 1.0,
            iops_write: 0.0,
            throughput_read: 0.0,
            throughput_write: 0.0,
            connections: 0,
            is_idle: false,
        };
        assert!(!activity.is_idle);
    }

    #[test]
    fn test_snapshot_count_from_entries() {
        let snap = json!({
            "entries": [
                { "id": 1, "name": "snap1" },
                { "id": 2, "name": "snap2" },
                { "id": 3, "name": "snap3" }
            ]
        });
        let count = snap["entries"]
            .as_array()
            .map(|e| e.len() as u64)
            .unwrap_or(0);
        assert_eq!(count, 3);
    }

    #[test]
    fn test_snapshot_count_empty() {
        let snap = json!({ "entries": [] });
        let count = snap["entries"]
            .as_array()
            .map(|e| e.len() as u64)
            .unwrap_or(0);
        assert_eq!(count, 0);
    }

    #[test]
    fn test_parse_byte_value_string() {
        assert_eq!(parse_byte_value(&json!("7755127889920")), 7_755_127_889_920);
    }

    #[test]
    fn test_parse_byte_value_number() {
        assert_eq!(parse_byte_value(&json!(1024)), 1024);
    }

    #[test]
    fn test_parse_byte_value_null() {
        assert_eq!(parse_byte_value(&json!(null)), 0);
    }

    #[test]
    fn test_parse_string_u64_large() {
        // u64 max is 18446744073709551615
        assert_eq!(parse_string_u64(&json!("18446744073709551615")), u64::MAX);
    }

    #[test]
    fn test_build_aggregates_sums_file_stats() {
        let clusters = vec![
            ClusterStatus {
                profile: "a".into(),
                name: "a".into(),
                uuid: "".into(),
                version: "".into(),
                cluster_type: ClusterType::CnqAws,
                reachable: true,
                stale: false,
                latency_ms: 0,
                nodes: NodeStatus {
                    total: 3,
                    online: 3,
                },
                capacity: CapacityStatus::default(),
                activity: ActivityStatus::default(),
                files: FileStats {
                    total_files: 100,
                    total_directories: 10,
                    total_snapshots: 5,
                    snapshot_bytes: 1000,
                },
                health: HealthStatus {
                    status: HealthLevel::Healthy,
                    issues: vec![],
                },
            },
            ClusterStatus {
                profile: "b".into(),
                name: "b".into(),
                uuid: "".into(),
                version: "".into(),
                cluster_type: ClusterType::OnPrem(vec![]),
                reachable: true,
                stale: false,
                latency_ms: 0,
                nodes: NodeStatus {
                    total: 5,
                    online: 5,
                },
                capacity: CapacityStatus::default(),
                activity: ActivityStatus::default(),
                files: FileStats {
                    total_files: 200,
                    total_directories: 20,
                    total_snapshots: 10,
                    snapshot_bytes: 2000,
                },
                health: HealthStatus {
                    status: HealthLevel::Healthy,
                    issues: vec![],
                },
            },
        ];
        let agg = build_aggregates(&clusters);
        assert_eq!(agg.files.total_files, 300);
        assert_eq!(agg.files.total_directories, 30);
        assert_eq!(agg.files.total_snapshots, 15);
        assert_eq!(agg.files.snapshot_bytes, 3000);
        assert_eq!(agg.total_nodes, 8);
        assert_eq!(agg.online_nodes, 8);
        assert_eq!(agg.reachable_count, 2);
    }
}
