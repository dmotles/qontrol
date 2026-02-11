pub mod cache;
pub mod capacity;
pub mod collector;
pub mod detection;
pub mod health;
pub mod json;
pub mod renderer;
pub mod types;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;

use crate::config::Config;

use self::types::EnvironmentStatus;

/// State maintained between watch mode polls for NIC throughput delta computation.
struct WatchState {
    /// Previous NIC byte counters: (profile, node_id) → total_bytes
    previous_nic_counters: HashMap<(String, u64), u64>,
    /// Timestamp of the previous poll
    previous_timestamp: Instant,
}

/// Compute NIC throughput deltas from watch state and patch into the status.
/// Returns updated counters for the next poll.
fn apply_nic_deltas(
    status: &mut EnvironmentStatus,
    prev: &WatchState,
) -> HashMap<(String, u64), u64> {
    let elapsed = prev.previous_timestamp.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    let mut new_counters = HashMap::new();

    for cluster in &mut status.clusters {
        for node in &mut cluster.nodes.details {
            // Save current raw bytes for next poll
            if let Some(current_bytes) = node.nic_bytes_total {
                let key = (cluster.profile.clone(), node.node_id);
                new_counters.insert(key.clone(), current_bytes);

                // Compute delta from previous poll
                if let Some(&prev_bytes) = prev.previous_nic_counters.get(&key) {
                    if elapsed_secs > 0.0 {
                        let delta_bytes = current_bytes.saturating_sub(prev_bytes);
                        let throughput_bps = (delta_bytes as f64 * 8.0 / elapsed_secs) as u64;
                        node.nic_throughput_bps = Some(throughput_bps);

                        // Compute utilization if link speed is known
                        if let Some(link_speed) = node.nic_link_speed_bps {
                            if link_speed > 0 {
                                node.nic_utilization_pct =
                                    Some(throughput_bps as f64 / link_speed as f64 * 100.0);
                            }
                        }
                    }
                }
            }
        }
    }

    new_counters
}

/// Extract current NIC byte counters from an EnvironmentStatus.
fn extract_nic_counters(status: &EnvironmentStatus) -> HashMap<(String, u64), u64> {
    let mut counters = HashMap::new();
    for cluster in &status.clusters {
        for node in &cluster.nodes.details {
            if let Some(bytes) = node.nic_bytes_total {
                counters.insert((cluster.profile.clone(), node.node_id), bytes);
            }
        }
    }
    counters
}

/// Entry point for the `status` command.
pub fn run(
    config: &Config,
    profiles: &[String],
    json_mode: bool,
    watch: bool,
    interval: u64,
    no_cache: bool,
    timeout_secs: u64,
) -> Result<()> {
    // Set up Ctrl+C handler for graceful exit in watch mode
    let running = Arc::new(AtomicBool::new(true));
    if watch {
        let r = running.clone();
        ctrlc::set_handler(move || {
            r.store(false, Ordering::SeqCst);
        })
        .ok(); // Ignore if handler can't be set (e.g., already set)
    }

    let mut watch_state: Option<WatchState> = None;

    loop {
        let mut status = collector::collect_all(
            config, profiles, timeout_secs, no_cache, watch, json_mode,
        )?;

        // In watch mode, compute NIC throughput from deltas between polls
        if watch {
            if let Some(ref prev) = watch_state {
                let new_counters = apply_nic_deltas(&mut status, prev);
                watch_state = Some(WatchState {
                    previous_nic_counters: new_counters,
                    previous_timestamp: Instant::now(),
                });
            } else {
                // First poll: extract counters for next poll, throughput stays None
                let counters = extract_nic_counters(&status);
                watch_state = Some(WatchState {
                    previous_nic_counters: counters,
                    previous_timestamp: Instant::now(),
                });
            }
        }

        if json_mode {
            let json_output = json::JsonOutput::from_status(&status);
            println!(
                "{}",
                serde_json::to_string_pretty(&json_output).unwrap_or_else(|_| "{}".to_string())
            );
        } else {
            print!("{}", renderer::render(&status));
        }

        if !watch {
            break;
        }

        // Print watch footer
        if !json_mode {
            println!(
                "Refreshing every {}s \u{2014} press Ctrl+C to stop",
                interval
            );
        }

        // Sleep in small increments so Ctrl+C is responsive
        let sleep_end = Instant::now() + Duration::from_secs(interval);
        while Instant::now() < sleep_end {
            if !running.load(Ordering::SeqCst) {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(100));
        }

        if !running.load(Ordering::SeqCst) {
            return Ok(());
        }

        // Clear terminal before next render
        if !json_mode {
            print!("\x1B[2J\x1B[H");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::status::types::*;
    use std::collections::HashMap;

    fn make_node(
        node_id: u64,
        bytes_total: Option<u64>,
        link_speed: Option<u64>,
    ) -> NodeNetworkInfo {
        NodeNetworkInfo {
            node_id,
            connections: 10,
            connection_breakdown: HashMap::new(),
            nic_throughput_bps: None,
            nic_link_speed_bps: link_speed,
            nic_utilization_pct: None,
            nic_bytes_total: bytes_total,
        }
    }

    fn make_cluster(profile: &str, nodes: Vec<NodeNetworkInfo>) -> ClusterStatus {
        ClusterStatus {
            profile: profile.to_string(),
            name: format!("{}-cluster", profile),
            uuid: "uuid".to_string(),
            version: "7.0".to_string(),
            cluster_type: ClusterType::OnPrem(vec![]),
            reachable: true,
            stale: false,
            latency_ms: 10,
            nodes: NodeStatus {
                total: nodes.len(),
                online: nodes.len(),
                offline_nodes: vec![],
                details: nodes,
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
                unhealthy_disk_details: vec![],
                unhealthy_psu_details: vec![],
            },
        }
    }

    fn make_env(clusters: Vec<ClusterStatus>) -> EnvironmentStatus {
        EnvironmentStatus {
            aggregates: Aggregates {
                cluster_count: clusters.len(),
                reachable_count: clusters.len(),
                total_nodes: clusters.iter().map(|c| c.nodes.total).sum(),
                online_nodes: clusters.iter().map(|c| c.nodes.online).sum(),
                capacity: CapacityStatus::default(),
                files: FileStats::default(),
            },
            alerts: vec![],
            clusters,
        }
    }

    #[test]
    fn test_nic_delta_known_counters() {
        // Previous: node 1 had 1,000,000 bytes total
        // Current: node 1 has 1,250,000 bytes total
        // Elapsed: ~1 second
        // Delta: 250,000 bytes → 2,000,000 bps
        let mut status = make_env(vec![make_cluster(
            "test",
            vec![make_node(1, Some(1_250_000), Some(200_000_000_000))],
        )]);

        let mut prev_counters = HashMap::new();
        prev_counters.insert(("test".to_string(), 1u64), 1_000_000u64);

        let prev = WatchState {
            previous_nic_counters: prev_counters,
            previous_timestamp: Instant::now() - Duration::from_secs(1),
        };

        let new_counters = apply_nic_deltas(&mut status, &prev);

        let node = &status.clusters[0].nodes.details[0];
        assert!(node.nic_throughput_bps.is_some());
        let throughput = node.nic_throughput_bps.unwrap();
        // 250,000 bytes * 8 / ~1 sec ≈ 2,000,000 bps (allow some timing variance)
        assert!(
            throughput > 1_900_000 && throughput < 2_100_000,
            "expected ~2,000,000 bps, got {}",
            throughput
        );

        // Utilization should be computed: ~2Mbps / 200Gbps
        assert!(node.nic_utilization_pct.is_some());
        assert!(node.nic_utilization_pct.unwrap() < 0.01);

        // New counters should contain the current value
        assert_eq!(new_counters.get(&("test".to_string(), 1)), Some(&1_250_000));
    }

    #[test]
    fn test_nic_delta_counter_reset() {
        // Counter reset: previous was higher than current (e.g., node restarted)
        // saturating_sub should yield 0, so throughput = 0
        let mut status = make_env(vec![make_cluster(
            "test",
            vec![make_node(1, Some(500), None)],
        )]);

        let mut prev_counters = HashMap::new();
        prev_counters.insert(("test".to_string(), 1u64), 1_000_000u64);

        let prev = WatchState {
            previous_nic_counters: prev_counters,
            previous_timestamp: Instant::now() - Duration::from_secs(2),
        };

        apply_nic_deltas(&mut status, &prev);

        let node = &status.clusters[0].nodes.details[0];
        assert_eq!(node.nic_throughput_bps, Some(0));
    }

    #[test]
    fn test_nic_delta_first_poll_no_throughput() {
        // First poll: no previous state → throughput stays None
        let status = make_env(vec![make_cluster(
            "test",
            vec![make_node(1, Some(1_000_000), Some(200_000_000_000))],
        )]);

        let counters = extract_nic_counters(&status);
        assert_eq!(counters.get(&("test".to_string(), 1)), Some(&1_000_000));

        // Throughput should still be None (first poll, no delta applied)
        let node = &status.clusters[0].nodes.details[0];
        assert_eq!(node.nic_throughput_bps, None);
    }

    #[test]
    fn test_nic_delta_multi_cluster_multi_node() {
        let mut status = make_env(vec![
            make_cluster(
                "cluster_a",
                vec![
                    make_node(1, Some(2_000_000), Some(100_000_000_000)),
                    make_node(2, Some(3_000_000), None),
                ],
            ),
            make_cluster(
                "cluster_b",
                vec![make_node(1, Some(5_000_000), Some(200_000_000_000))],
            ),
        ]);

        let mut prev_counters = HashMap::new();
        prev_counters.insert(("cluster_a".to_string(), 1u64), 1_000_000u64);
        prev_counters.insert(("cluster_a".to_string(), 2u64), 2_000_000u64);
        prev_counters.insert(("cluster_b".to_string(), 1u64), 4_000_000u64);

        let prev = WatchState {
            previous_nic_counters: prev_counters,
            previous_timestamp: Instant::now() - Duration::from_secs(2),
        };

        let new_counters = apply_nic_deltas(&mut status, &prev);

        // cluster_a node 1: delta = 1,000,000 bytes → ~4,000,000 bps (over 2s)
        let n = &status.clusters[0].nodes.details[0];
        assert!(n.nic_throughput_bps.is_some());
        let tp = n.nic_throughput_bps.unwrap();
        assert!(tp > 3_800_000 && tp < 4_200_000, "cluster_a/1: got {}", tp);
        assert!(n.nic_utilization_pct.is_some()); // has link speed

        // cluster_a node 2: delta = 1,000,000 bytes → ~4,000,000 bps, no utilization (cloud)
        let n = &status.clusters[0].nodes.details[1];
        assert!(n.nic_throughput_bps.is_some());
        assert!(n.nic_utilization_pct.is_none()); // no link speed

        // cluster_b node 1: delta = 1,000,000 bytes → ~4,000,000 bps
        let n = &status.clusters[1].nodes.details[0];
        assert!(n.nic_throughput_bps.is_some());

        assert_eq!(new_counters.len(), 3);
    }
}
