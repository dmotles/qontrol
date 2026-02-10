use std::time::Instant;

use anyhow::Result;
use serde_json::Value;

use crate::client::QumuloClient;
use crate::config::{Config, ProfileEntry};

use super::cache;
use super::capacity;
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

    // Fetch capacity history and compute projection
    capacity.projection = fetch_capacity_projection(
        &client,
        capacity.used_bytes,
        capacity.total_bytes,
        &cluster_type,
    );

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
        files: FileStats::default(),
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
