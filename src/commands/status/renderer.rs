use console::Style;

use super::capacity;
use super::types::*;

const HEADER_WIDTH: usize = 80;
const CAPACITY_BAR_WIDTH: usize = 20;
const NIC_BAR_WIDTH: usize = 10;

// ── Public entry point ──────────────────────────────────────────────────────

/// Render the full terminal output for `qontrol status`.
///
/// Returns a styled string ready for printing. All ANSI escape codes for color
/// are embedded in the returned string via the `console` crate.
pub fn render(status: &EnvironmentStatus) -> String {
    let mut out = String::new();

    render_overview(&mut out, status);
    render_alerts(&mut out, status);
    render_clusters(&mut out, status);

    out
}

// ── Section renderers ───────────────────────────────────────────────────────

fn render_overview(out: &mut String, status: &EnvironmentStatus) {
    let bold = Style::new().bold();
    let agg = &status.aggregates;

    // Header bar
    let title = "═══ Environment Overview ";
    let padding = HEADER_WIDTH.saturating_sub(title.len());
    out.push_str(&format!(
        "{}\n",
        bold.apply_to(format!("{}{}", title, "═".repeat(padding)))
    ));

    // Cluster + Latency line
    let unreachable = agg.cluster_count.saturating_sub(agg.reachable_count);
    let cluster_info = if unreachable > 0 {
        format!(
            "  Clusters: {} ({} healthy, {} unreachable)",
            agg.cluster_count, agg.reachable_count, unreachable,
        )
    } else {
        format!("  Clusters: {} (all healthy)", agg.cluster_count)
    };
    let latency_info = format_latency_range(&status.clusters);
    if latency_info.is_empty() {
        out.push_str(&format!("{}\n", cluster_info));
    } else {
        let gap = 40_usize.saturating_sub(cluster_info.len());
        out.push_str(&format!(
            "{}{}Latency: {}\n",
            cluster_info,
            " ".repeat(gap.max(4)),
            latency_info,
        ));
    }

    // Nodes line
    let offline_nodes = agg.total_nodes.saturating_sub(agg.online_nodes);
    if offline_nodes > 0 {
        out.push_str(&format!(
            "  Nodes:    {} total ({} online, {} offline)\n",
            agg.total_nodes, agg.online_nodes, offline_nodes,
        ));
    } else {
        out.push_str(&format!(
            "  Nodes:    {} total ({} online)\n",
            agg.total_nodes, agg.online_nodes,
        ));
    }

    // Capacity line
    out.push_str(&format!(
        "  Capacity: {} / {} ({:.1}%)\n",
        format_bytes(agg.capacity.used_bytes),
        format_bytes(agg.capacity.total_bytes),
        agg.capacity.used_pct,
    ));

    // Files line
    out.push_str(&format!(
        "  Files:    {}    Dirs: {}    Snapshots: {} ({})\n",
        format_number(agg.files.total_files),
        format_number(agg.files.total_directories),
        format_number(agg.files.total_snapshots),
        format_bytes(agg.files.snapshot_bytes),
    ));
}

fn render_alerts(out: &mut String, status: &EnvironmentStatus) {
    let bold = Style::new().bold();
    let green = Style::new().green();
    let yellow = Style::new().yellow();
    let red = Style::new().red();

    let title = "═══ Alerts ";
    let padding = HEADER_WIDTH.saturating_sub(title.len());
    out.push_str(&format!(
        "{}\n",
        bold.apply_to(format!("{}{}", title, "═".repeat(padding)))
    ));

    if status.alerts.is_empty() {
        out.push_str(&format!("  {}\n", green.apply_to("No issues detected.")));
    } else {
        for alert in &status.alerts {
            let (icon, style) = match alert.severity {
                AlertSeverity::Critical => ("✗", &red),
                AlertSeverity::Warning => ("⚠", &yellow),
                AlertSeverity::Info => ("ℹ", &green),
            };
            out.push_str(&format!(
                "  {} {}: {}\n",
                style.apply_to(icon),
                alert.cluster,
                alert.message,
            ));
        }
    }
}

fn render_clusters(out: &mut String, status: &EnvironmentStatus) {
    for cluster in &status.clusters {
        out.push('\n');
        render_cluster_header(out, cluster);
        render_cluster_separator(out);

        if !cluster.reachable {
            render_unreachable_cluster(out, cluster, &status.alerts);
        } else {
            render_reachable_cluster(out, cluster);
        }
    }
}

fn render_cluster_header(out: &mut String, cluster: &ClusterStatus) {
    let bold = Style::new().bold();

    let type_info = match &cluster.cluster_type {
        ClusterType::OnPrem(models) => {
            if models.is_empty() {
                "on-prem".to_string()
            } else {
                format!("on-prem · {}", models.join(", "))
            }
        }
        ClusterType::CnqAws => "CNQ · AWS".to_string(),
        ClusterType::AnqAzure => "ANQ · Azure".to_string(),
    };

    let right_side = if cluster.reachable {
        format!(
            "{} · {} · {}ms",
            type_info, cluster.version, cluster.latency_ms
        )
    } else {
        format!("{} · {}", type_info, cluster.version)
    };

    // Show profile as primary label; append cluster name if different
    let label = if cluster.profile != cluster.name {
        format!("{} ({})", cluster.profile, cluster.name)
    } else {
        cluster.name.clone()
    };

    // Left-align label, right-align metadata with padding
    let name_str = format!("  {}", label);
    let gap = HEADER_WIDTH.saturating_sub(name_str.len() + right_side.len());
    out.push_str(&format!(
        "{}{}{}\n",
        bold.apply_to(&name_str),
        " ".repeat(gap.max(2)),
        right_side,
    ));
}

fn render_cluster_separator(out: &mut String) {
    let dim = Style::new().dim();
    out.push_str(&format!(
        "  {}\n",
        dim.apply_to("─".repeat(HEADER_WIDTH - 2))
    ));
}

fn render_unreachable_cluster(out: &mut String, cluster: &ClusterStatus, alerts: &[Alert]) {
    let red = Style::new().red();
    let dim = Style::new().dim();

    // Find the connectivity alert to get the "last seen" timestamp
    let last_seen = find_last_seen(cluster, alerts);
    if let Some((timestamp, relative)) = last_seen {
        out.push_str(&format!(
            "  {} UNREACHABLE — last seen {} ({})\n",
            red.apply_to("✗"),
            timestamp,
            relative,
        ));
    } else {
        out.push_str(&format!("  {} UNREACHABLE\n", red.apply_to("✗")));
    }

    // Show cached capacity/files
    render_capacity_line(out, cluster);
    render_files_line(out, cluster);

    out.push_str(&format!(
        "  {}\n",
        dim.apply_to("(stale data from last successful poll)")
    ));
}

fn render_reachable_cluster(out: &mut String, cluster: &ClusterStatus) {
    let yellow = Style::new().yellow();

    // Nodes line
    render_nodes_line(out, cluster);

    // Capacity with bar
    render_capacity_bar(out, cluster);

    // Files
    render_files_line(out, cluster);

    // Activity
    render_activity_line(out, cluster);

    // Capacity projection warning (inline in cluster section)
    if let Some(ref projection) = cluster.capacity.projection {
        if capacity::should_warn(projection, &cluster.cluster_type) {
            let msg = capacity::format_warning(projection, &cluster.cluster_type);
            out.push_str(&format!("  {} {}\n", yellow.apply_to("⚠"), msg,));
        }
    }

    // Connections + NIC throughput table
    if !cluster.nodes.details.is_empty() {
        render_network_table(out, cluster);
    }
}

fn render_nodes_line(out: &mut String, cluster: &ClusterStatus) {
    let red = Style::new().red();
    let nodes = &cluster.nodes;

    if !nodes.offline_nodes.is_empty() {
        let offline_list: Vec<String> = nodes
            .offline_nodes
            .iter()
            .map(|id| format!("node {}", id))
            .collect();
        out.push_str(&format!(
            "  Nodes:    {}/{} online ({})\n",
            nodes.online,
            nodes.total,
            red.apply_to(format!("{}: OFFLINE", offline_list.join(", "))),
        ));
    } else {
        out.push_str(&format!(
            "  Nodes:    {}/{} online\n",
            nodes.online, nodes.total,
        ));
    }
}

fn render_capacity_line(out: &mut String, cluster: &ClusterStatus) {
    let cap = &cluster.capacity;
    out.push_str(&format!(
        "  Capacity: {} / {} ({:.1}%)\n",
        format_bytes(cap.used_bytes),
        format_bytes(cap.total_bytes),
        cap.used_pct,
    ));
}

fn render_capacity_bar(out: &mut String, cluster: &ClusterStatus) {
    let cap = &cluster.capacity;
    let bar = progress_bar(cap.used_pct, CAPACITY_BAR_WIDTH);
    out.push_str(&format!(
        "  Capacity: {} / {} ({:.1}%) {}  snaps: {}\n",
        format_bytes(cap.used_bytes),
        format_bytes(cap.total_bytes),
        cap.used_pct,
        bar,
        format_bytes(cap.snapshot_bytes),
    ));
}

fn render_files_line(out: &mut String, cluster: &ClusterStatus) {
    let f = &cluster.files;
    out.push_str(&format!(
        "  Files:    {}    Dirs: {}    Snapshots: {}\n",
        format_number(f.total_files),
        format_number(f.total_directories),
        format_number(f.total_snapshots),
    ));
}

fn render_activity_line(out: &mut String, cluster: &ClusterStatus) {
    let activity = &cluster.activity;
    if activity.is_idle {
        out.push_str("  Activity: idle\n");
    } else {
        out.push_str(&format!(
            "  Activity: R: {:.0} IOPS / {}    W: {:.0} IOPS / {}\n",
            activity.iops_read,
            format_throughput(activity.throughput_read),
            activity.iops_write,
            format_throughput(activity.throughput_write),
        ));
    }
}

fn render_network_table(out: &mut String, cluster: &ClusterStatus) {
    let red = Style::new().red();
    let details = &cluster.nodes.details;
    let offline = &cluster.nodes.offline_nodes;

    // Find max connections for proportional bar sizing
    let max_conns = details
        .iter()
        .map(|n| n.connections)
        .max()
        .unwrap_or(1)
        .max(1);

    out.push('\n');
    out.push_str("  Connections            NIC Throughput\n");

    for node in details {
        let is_offline = offline.contains(&node.node_id);
        let node_label = format!("node{}:", node.node_id);

        if is_offline {
            // Offline node: show dashes on both sides
            out.push_str(&format!(
                "  {:<8}{}\n",
                node_label,
                red.apply_to("—  OFFLINE"),
            ));
        } else {
            // Connections side
            let conn_str = format!("{:>3}", node.connections);
            let conn_bar = if node.connections > 0 {
                format!("  {}", connection_bar(node.connections, max_conns))
            } else {
                String::new()
            };
            let left = format!("  {:<8}{}{}", node_label, conn_str, conn_bar);

            // NIC throughput side
            let right = format_nic_column(node, &cluster.cluster_type);

            // Pad left to align NIC column
            let left_width = 25;
            let padded_left = format!("{:<width$}", left, width = left_width);

            out.push_str(&format!("{}node{}: {}\n", padded_left, node.node_id, right,));
        }
    }
}

fn format_nic_column(node: &NodeNetworkInfo, cluster_type: &ClusterType) -> String {
    match (node.nic_throughput_bps, &cluster_type) {
        (Some(throughput_bps), ClusterType::OnPrem(_)) => {
            let throughput_gbps = throughput_bps as f64 / 1_000_000_000.0;
            if let Some(link_bps) = node.nic_link_speed_bps {
                let link_gbps = link_bps as f64 / 1_000_000_000.0;
                let utilization = node.nic_utilization_pct.unwrap_or(0.0);
                let bar = nic_bar(utilization);
                if utilization < 1.0 && utilization > 0.0 {
                    format!(
                        "{:>4.1} / {} Gbps {}  <1%",
                        throughput_gbps,
                        format_link_speed(link_gbps),
                        bar,
                    )
                } else {
                    format!(
                        "{:>4.1} / {} Gbps {}  {:.0}%",
                        throughput_gbps,
                        format_link_speed(link_gbps),
                        bar,
                        utilization,
                    )
                }
            } else {
                format!("{:.1} Gbps", throughput_gbps)
            }
        }
        (Some(throughput_bps), _) => {
            // Cloud: show throughput only
            let throughput_gbps = throughput_bps as f64 / 1_000_000_000.0;
            format!("{:>4.1} Gbps", throughput_gbps)
        }
        (None, _) => "—".to_string(),
    }
}

fn format_link_speed(gbps: f64) -> String {
    if gbps >= 1.0 && gbps == gbps.floor() {
        format!("{:.0}", gbps)
    } else {
        format!("{:.1}", gbps)
    }
}

/// Extract "last seen" info for an unreachable cluster from its connectivity alert.
fn find_last_seen(cluster: &ClusterStatus, alerts: &[Alert]) -> Option<(String, String)> {
    for alert in alerts {
        if alert.category == "connectivity" && alert.cluster == cluster.name {
            // Try to extract timestamp from the alert message
            // Format: "unreachable, using cached data from <RFC3339>"
            if let Some(pos) = alert.message.find("cached data from ") {
                let ts_str = &alert.message[pos + "cached data from ".len()..];
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(ts_str) {
                    let formatted = dt.format("%Y-%m-%d %H:%M").to_string();
                    let relative = format_duration_ago(dt.into());
                    return Some((formatted, relative));
                }
            }
        }
        // Also check by profile name
        if alert.category == "connectivity" && alert.cluster == cluster.profile {
            if let Some(pos) = alert.message.find("cached data from ") {
                let ts_str = &alert.message[pos + "cached data from ".len()..];
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(ts_str) {
                    let formatted = dt.format("%Y-%m-%d %H:%M").to_string();
                    let relative = format_duration_ago(dt.into());
                    return Some((formatted, relative));
                }
            }
        }
    }
    None
}

// ── Formatting helpers (public for testing) ─────────────────────────────────

/// Format bytes into human-readable units (B, KB, MB, GB, TB, PB).
/// Uses the highest whole unit that results in a value >= 1.
pub fn format_bytes(bytes: u64) -> String {
    const PB: f64 = 1_125_899_906_842_624.0;
    const TB: f64 = 1_099_511_627_776.0;
    const GB: f64 = 1_073_741_824.0;
    const MB: f64 = 1_048_576.0;
    const KB: f64 = 1_024.0;

    let b = bytes as f64;
    if b >= PB {
        format!("{:.2} PB", b / PB)
    } else if b >= TB {
        format!("{:.1} TB", b / TB)
    } else if b >= GB {
        format!("{:.1} GB", b / GB)
    } else if b >= MB {
        format!("{:.1} MB", b / MB)
    } else if b >= KB {
        format!("{:.1} KB", b / KB)
    } else {
        format!("{} B", bytes)
    }
}

/// Format a number with comma separators (e.g., 698,412,061).
pub fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

/// Format throughput in bytes/sec into human-readable units (B/s, KB/s, MB/s, GB/s).
pub fn format_throughput(bytes_per_sec: f64) -> String {
    const GB: f64 = 1_073_741_824.0;
    const MB: f64 = 1_048_576.0;
    const KB: f64 = 1_024.0;

    if bytes_per_sec >= GB {
        format!("{:.1} GB/s", bytes_per_sec / GB)
    } else if bytes_per_sec >= MB {
        format!("{:.1} MB/s", bytes_per_sec / MB)
    } else if bytes_per_sec >= KB {
        format!("{:.1} KB/s", bytes_per_sec / KB)
    } else {
        format!("{:.0} B/s", bytes_per_sec)
    }
}

/// Format a chrono DateTime as a relative duration from now (e.g., "2h ago", "3d ago").
pub fn format_duration_ago(dt: chrono::DateTime<chrono::Utc>) -> String {
    let now = chrono::Utc::now();
    let duration = now.signed_duration_since(dt);

    let total_secs = duration.num_seconds();
    if total_secs < 0 {
        return "just now".to_string();
    }

    let days = duration.num_days();
    let hours = duration.num_hours();
    let minutes = duration.num_minutes();

    if days > 0 {
        format!("{}d ago", days)
    } else if hours > 0 {
        format!("{}h ago", hours)
    } else if minutes > 0 {
        format!("{}m ago", minutes)
    } else {
        "just now".to_string()
    }
}

/// Render a capacity progress bar: `████████████████████░` (filled █ and empty ░).
pub fn progress_bar(pct: f64, width: usize) -> String {
    let clamped = pct.clamp(0.0, 100.0);
    let filled = ((clamped / 100.0) * width as f64).round() as usize;
    let empty = width.saturating_sub(filled);
    format!("{}{}", "█".repeat(filled), "░".repeat(empty))
}

/// Render a proportional connection bar using █ characters.
pub fn connection_bar(count: u32, max: u32) -> String {
    if max == 0 || count == 0 {
        return String::new();
    }
    let width = 8;
    let filled = ((count as f64 / max as f64) * width as f64).ceil() as usize;
    "█".repeat(filled.max(1))
}

/// Render a NIC utilization bar using ▸ (filled) and ░ (empty).
pub fn nic_bar(utilization_pct: f64) -> String {
    let clamped = utilization_pct.clamp(0.0, 100.0);
    let filled = ((clamped / 100.0) * NIC_BAR_WIDTH as f64).round() as usize;
    let empty = NIC_BAR_WIDTH.saturating_sub(filled);
    format!("{}{}", "▸".repeat(filled), "░".repeat(empty))
}

/// Compute latency range string (e.g., "8-142ms") from reachable clusters.
fn format_latency_range(clusters: &[ClusterStatus]) -> String {
    let latencies: Vec<u64> = clusters
        .iter()
        .filter(|c| c.reachable)
        .map(|c| c.latency_ms)
        .collect();

    if latencies.is_empty() {
        return String::new();
    }

    let min = *latencies.iter().min().unwrap();
    let max = *latencies.iter().max().unwrap();

    if min == max {
        format!("{}ms", min)
    } else {
        format!("{}-{}ms", min, max)
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // ── Formatting helper tests ─────────────────────────────────────────

    #[test]
    fn test_format_bytes_b() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1023), "1023 B");
    }

    #[test]
    fn test_format_bytes_kb() {
        assert_eq!(format_bytes(1_024), "1.0 KB");
        assert_eq!(format_bytes(500_000), "488.3 KB");
    }

    #[test]
    fn test_format_bytes_mb() {
        assert_eq!(format_bytes(1_048_576), "1.0 MB");
        assert_eq!(format_bytes(57_800_000), "55.1 MB");
    }

    #[test]
    fn test_format_bytes_gb() {
        assert_eq!(format_bytes(1_073_741_824), "1.0 GB");
        assert_eq!(format_bytes(980_000_000_000), "912.7 GB");
    }

    #[test]
    fn test_format_bytes_tb() {
        assert_eq!(format_bytes(1_099_511_627_776), "1.0 TB");
        assert_eq!(format_bytes(7_700_000_000_000), "7.0 TB");
        assert_eq!(format_bytes(594_000_000_000_000), "540.2 TB");
    }

    #[test]
    fn test_format_bytes_pb() {
        assert_eq!(format_bytes(1_125_899_906_842_624), "1.00 PB");
        assert_eq!(format_bytes(1_860_000_000_000_000), "1.65 PB");
        assert_eq!(format_bytes(2_170_000_000_000_000), "1.93 PB");
    }

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(999), "999");
        assert_eq!(format_number(1_000), "1,000");
        assert_eq!(format_number(12_847), "12,847");
        assert_eq!(format_number(48_231_004), "48,231,004");
        assert_eq!(format_number(698_412_061), "698,412,061");
    }

    #[test]
    fn test_format_throughput() {
        assert_eq!(format_throughput(0.0), "0 B/s");
        assert_eq!(format_throughput(512.0), "512 B/s");
        assert_eq!(format_throughput(1_600_000.0), "1.5 MB/s");
        assert_eq!(format_throughput(57_800_000.0), "55.1 MB/s");
        assert_eq!(format_throughput(1_200_000_000.0), "1.1 GB/s");
    }

    #[test]
    fn test_progress_bar() {
        assert_eq!(progress_bar(0.0, 20), "░░░░░░░░░░░░░░░░░░░░");
        assert_eq!(progress_bar(50.0, 20), "██████████░░░░░░░░░░");
        assert_eq!(progress_bar(100.0, 20), "████████████████████");
        assert_eq!(progress_bar(98.2, 20), "████████████████████");
        // Edge: clamping
        assert_eq!(progress_bar(-5.0, 20), "░░░░░░░░░░░░░░░░░░░░");
        assert_eq!(progress_bar(150.0, 20), "████████████████████");
    }

    #[test]
    fn test_connection_bar() {
        assert_eq!(connection_bar(0, 14), "");
        assert_eq!(connection_bar(14, 14), "████████");
        assert_eq!(connection_bar(3, 14), "██");
        assert_eq!(connection_bar(1, 14), "█");
    }

    #[test]
    fn test_nic_bar() {
        assert_eq!(nic_bar(0.0), "░░░░░░░░░░");
        assert_eq!(nic_bar(6.0), "▸░░░░░░░░░");
        assert_eq!(nic_bar(50.0), "▸▸▸▸▸░░░░░");
        assert_eq!(nic_bar(100.0), "▸▸▸▸▸▸▸▸▸▸");
    }

    #[test]
    fn test_format_latency_range() {
        let clusters = vec![
            make_cluster("a", true, 8),
            make_cluster("b", true, 42),
            make_cluster("c", true, 142),
            make_cluster("d", false, 0), // unreachable, excluded
        ];
        assert_eq!(format_latency_range(&clusters), "8-142ms");
    }

    #[test]
    fn test_format_latency_range_single() {
        let clusters = vec![make_cluster("a", true, 42)];
        assert_eq!(format_latency_range(&clusters), "42ms");
    }

    #[test]
    fn test_format_latency_range_none_reachable() {
        let clusters = vec![make_cluster("a", false, 0)];
        assert_eq!(format_latency_range(&clusters), "");
    }

    // ── Snapshot tests ──────────────────────────────────────────────────

    #[test]
    fn test_render_healthy_onprem_cluster() {
        let status = make_full_status_healthy_onprem();
        let output = render(&status);
        // Strip ANSI codes for comparison
        let plain = strip_ansi(&output);

        assert!(plain.contains("Environment Overview"));
        assert!(plain.contains("Clusters: 1 (all healthy)"));
        assert!(plain.contains("Nodes:    5 total (5 online)"));
        assert!(plain.contains("gravytrain (gravytrain-sg)"));
        assert!(plain.contains("on-prem · C192T, QCT_D52T"));
        assert!(plain.contains("5/5 online"));
        assert!(plain.contains("Activity: R: 140 IOPS"));
        assert!(plain.contains("Connections"));
        assert!(plain.contains("NIC Throughput"));
        assert!(plain.contains("No issues detected."));
    }

    #[test]
    fn test_render_healthy_cloud_cluster() {
        let status = make_full_status_healthy_cloud();
        let output = render(&status);
        let plain = strip_ansi(&output);

        // profile == name, so only shown once
        assert!(plain.contains("aws-gravytrain"));
        assert!(plain.contains("CNQ · AWS"));
        assert!(plain.contains("3/3 online"));
        assert!(plain.contains("Activity: idle"));
        // Cloud: no link speed in NIC column
        assert!(plain.contains("Gbps"));
        assert!(!plain.contains("/ 200 Gbps")); // no "X / Y Gbps" for cloud
    }

    #[test]
    fn test_render_offline_node_cluster() {
        let status = make_full_status_offline_node();
        let output = render(&status);
        let plain = strip_ansi(&output);

        assert!(plain.contains("5/6 online"));
        assert!(plain.contains("node 4: OFFLINE"));
        assert!(plain.contains("OFFLINE"));
        // Cluster header shows profile (name) since they differ
        assert!(plain.contains("iss (iss-sg)"));
    }

    #[test]
    fn test_render_unreachable_cluster() {
        let status = make_full_status_unreachable();
        let output = render(&status);
        let plain = strip_ansi(&output);

        assert!(plain.contains("UNREACHABLE"));
        assert!(plain.contains("stale data from last successful poll"));
        // No latency in header
        assert!(plain.contains("ANQ · Azure · v7.8.0\n"));
    }

    #[test]
    fn test_render_no_alerts() {
        let status = make_full_status_healthy_onprem();
        let output = render(&status);
        let plain = strip_ansi(&output);

        assert!(plain.contains("No issues detected."));
    }

    #[test]
    fn test_render_with_alerts() {
        let status = make_status_with_alerts();
        let output = render(&status);
        let plain = strip_ansi(&output);

        assert!(plain.contains("✗ iss-sg: node 4: OFFLINE"));
        assert!(plain.contains("⚠ gravytrain-sg: projected to fill"));
    }

    #[test]
    fn test_render_idle_activity() {
        let status = make_full_status_healthy_cloud();
        let output = render(&status);
        let plain = strip_ansi(&output);

        assert!(plain.contains("Activity: idle"));
    }

    #[test]
    fn test_render_capacity_bar_in_cluster() {
        let status = make_full_status_healthy_onprem();
        let output = render(&status);
        let plain = strip_ansi(&output);

        // Should contain the progress bar characters
        assert!(plain.contains("█"));
        assert!(plain.contains("snaps:"));
    }

    // ── Test helpers ────────────────────────────────────────────────────

    fn make_cluster(name: &str, reachable: bool, latency_ms: u64) -> ClusterStatus {
        ClusterStatus {
            profile: name.to_string(),
            name: name.to_string(),
            uuid: "test-uuid".to_string(),
            version: "v7.8.0".to_string(),
            cluster_type: ClusterType::OnPrem(vec![]),
            reachable,
            stale: !reachable,
            latency_ms,
            nodes: NodeStatus {
                total: 3,
                online: 3,
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
                protection_type: None,
                unhealthy_disk_details: vec![],
                unhealthy_psu_details: vec![],
            },
        }
    }

    fn make_full_status_healthy_onprem() -> EnvironmentStatus {
        let nodes_details = vec![
            NodeNetworkInfo {
                node_id: 1,
                connections: 14,
                connection_breakdown: HashMap::from([
                    ("NFS".to_string(), 8),
                    ("SMB".to_string(), 4),
                    ("REST".to_string(), 2),
                ]),
                nic_throughput_bps: Some(12_400_000_000),
                nic_link_speed_bps: Some(200_000_000_000),
                nic_utilization_pct: Some(6.0),
                nic_bytes_total: None,
            },
            NodeNetworkInfo {
                node_id: 2,
                connections: 3,
                connection_breakdown: HashMap::new(),
                nic_throughput_bps: Some(1_100_000_000),
                nic_link_speed_bps: Some(200_000_000_000),
                nic_utilization_pct: Some(1.0),
                nic_bytes_total: None,
            },
            NodeNetworkInfo {
                node_id: 3,
                connections: 3,
                connection_breakdown: HashMap::new(),
                nic_throughput_bps: Some(800_000_000),
                nic_link_speed_bps: Some(200_000_000_000),
                nic_utilization_pct: Some(0.4),
                nic_bytes_total: None,
            },
            NodeNetworkInfo {
                node_id: 4,
                connections: 0,
                connection_breakdown: HashMap::new(),
                nic_throughput_bps: Some(200_000_000),
                nic_link_speed_bps: Some(100_000_000_000),
                nic_utilization_pct: Some(0.2),
                nic_bytes_total: None,
            },
            NodeNetworkInfo {
                node_id: 5,
                connections: 1,
                connection_breakdown: HashMap::new(),
                nic_throughput_bps: Some(400_000_000),
                nic_link_speed_bps: Some(100_000_000_000),
                nic_utilization_pct: Some(0.4),
                nic_bytes_total: None,
            },
        ];

        let cluster = ClusterStatus {
            profile: "gravytrain".to_string(),
            name: "gravytrain-sg".to_string(),
            uuid: "f83b970e-1234".to_string(),
            version: "v7.8.0".to_string(),
            cluster_type: ClusterType::OnPrem(vec!["C192T".to_string(), "QCT_D52T".to_string()]),
            reachable: true,
            stale: false,
            latency_ms: 42,
            nodes: NodeStatus {
                total: 5,
                online: 5,
                offline_nodes: vec![],
                details: nodes_details,
            },
            capacity: CapacityStatus {
                total_bytes: 605_000_000_000_000,
                used_bytes: 594_000_000_000_000,
                free_bytes: 11_000_000_000_000,
                snapshot_bytes: 6_700_000_000_000,
                used_pct: 98.2,
                projection: None,
            },
            activity: ActivityStatus {
                iops_read: 140.0,
                iops_write: 122.0,
                throughput_read: 57_800_000.0,
                throughput_write: 1_600_000.0,
                connections: 21,
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
        };

        EnvironmentStatus {
            aggregates: Aggregates {
                cluster_count: 1,
                reachable_count: 1,
                total_nodes: 5,
                online_nodes: 5,
                capacity: cluster.capacity.clone(),
                files: cluster.files.clone(),
            },
            alerts: vec![],
            clusters: vec![cluster],
        }
    }

    fn make_full_status_healthy_cloud() -> EnvironmentStatus {
        let nodes_details = vec![
            NodeNetworkInfo {
                node_id: 1,
                connections: 0,
                connection_breakdown: HashMap::new(),
                nic_throughput_bps: Some(0),
                nic_link_speed_bps: None,
                nic_utilization_pct: None,
                nic_bytes_total: None,
            },
            NodeNetworkInfo {
                node_id: 2,
                connections: 1,
                connection_breakdown: HashMap::new(),
                nic_throughput_bps: Some(0),
                nic_link_speed_bps: None,
                nic_utilization_pct: None,
                nic_bytes_total: None,
            },
            NodeNetworkInfo {
                node_id: 3,
                connections: 0,
                connection_breakdown: HashMap::new(),
                nic_throughput_bps: Some(0),
                nic_link_speed_bps: None,
                nic_utilization_pct: None,
                nic_bytes_total: None,
            },
        ];

        let cluster = ClusterStatus {
            profile: "aws-gravytrain".to_string(),
            name: "aws-gravytrain".to_string(),
            uuid: "abc-456".to_string(),
            version: "v7.8.0".to_string(),
            cluster_type: ClusterType::CnqAws,
            reachable: true,
            stale: false,
            latency_ms: 142,
            nodes: NodeStatus {
                total: 3,
                online: 3,
                offline_nodes: vec![],
                details: nodes_details,
            },
            capacity: CapacityStatus {
                total_bytes: 454_700_000_000_000,
                used_bytes: 77_300_000_000_000,
                free_bytes: 377_400_000_000_000,
                snapshot_bytes: 0,
                used_pct: 17.0,
                projection: None,
            },
            activity: ActivityStatus {
                iops_read: 0.0,
                iops_write: 0.0,
                throughput_read: 0.0,
                throughput_write: 0.0,
                connections: 1,
                is_idle: true,
            },
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
                remaining_node_failures: Some(1),
                remaining_drive_failures: Some(2),
                protection_type: None,
                unhealthy_disk_details: vec![],
                unhealthy_psu_details: vec![],
            },
        };

        EnvironmentStatus {
            aggregates: Aggregates {
                cluster_count: 1,
                reachable_count: 1,
                total_nodes: 3,
                online_nodes: 3,
                capacity: cluster.capacity.clone(),
                files: cluster.files.clone(),
            },
            alerts: vec![],
            clusters: vec![cluster],
        }
    }

    fn make_full_status_offline_node() -> EnvironmentStatus {
        let nodes_details = vec![
            NodeNetworkInfo {
                node_id: 1,
                connections: 10,
                connection_breakdown: HashMap::new(),
                nic_throughput_bps: Some(5_000_000_000),
                nic_link_speed_bps: Some(200_000_000_000),
                nic_utilization_pct: Some(2.5),
                nic_bytes_total: None,
            },
            NodeNetworkInfo {
                node_id: 2,
                connections: 8,
                connection_breakdown: HashMap::new(),
                nic_throughput_bps: Some(3_000_000_000),
                nic_link_speed_bps: Some(200_000_000_000),
                nic_utilization_pct: Some(1.5),
                nic_bytes_total: None,
            },
            NodeNetworkInfo {
                node_id: 3,
                connections: 5,
                connection_breakdown: HashMap::new(),
                nic_throughput_bps: Some(1_000_000_000),
                nic_link_speed_bps: Some(200_000_000_000),
                nic_utilization_pct: Some(0.5),
                nic_bytes_total: None,
            },
            NodeNetworkInfo {
                node_id: 4,
                connections: 0,
                connection_breakdown: HashMap::new(),
                nic_throughput_bps: None,
                nic_link_speed_bps: None,
                nic_utilization_pct: None,
                nic_bytes_total: None,
            },
            NodeNetworkInfo {
                node_id: 5,
                connections: 3,
                connection_breakdown: HashMap::new(),
                nic_throughput_bps: Some(500_000_000),
                nic_link_speed_bps: Some(200_000_000_000),
                nic_utilization_pct: Some(0.3),
                nic_bytes_total: None,
            },
            NodeNetworkInfo {
                node_id: 6,
                connections: 2,
                connection_breakdown: HashMap::new(),
                nic_throughput_bps: Some(200_000_000),
                nic_link_speed_bps: Some(200_000_000_000),
                nic_utilization_pct: Some(0.1),
                nic_bytes_total: None,
            },
        ];

        let cluster = ClusterStatus {
            profile: "iss".to_string(),
            name: "iss-sg".to_string(),
            uuid: "iss-uuid".to_string(),
            version: "v7.8.0".to_string(),
            cluster_type: ClusterType::OnPrem(vec!["C192T".to_string(), "QCT_D52T".to_string()]),
            reachable: true,
            stale: false,
            latency_ms: 38,
            nodes: NodeStatus {
                total: 6,
                online: 5,
                offline_nodes: vec![4],
                details: nodes_details,
            },
            capacity: CapacityStatus {
                total_bytes: 736_000_000_000_000,
                used_bytes: 707_000_000_000_000,
                free_bytes: 29_000_000_000_000,
                snapshot_bytes: 980_000_000_000,
                used_pct: 96.1,
                projection: None,
            },
            activity: ActivityStatus {
                iops_read: 200.0,
                iops_write: 80.0,
                throughput_read: 30_000_000.0,
                throughput_write: 5_000_000.0,
                connections: 28,
                is_idle: false,
            },
            files: FileStats {
                total_files: 197_207_180,
                total_directories: 15_829_783,
                total_snapshots: 4_646,
                snapshot_bytes: 980_000_000_000,
            },
            health: HealthStatus {
                status: HealthLevel::Degraded,
                issues: vec!["node 4 offline".to_string()],
                disks_unhealthy: 0,
                psus_unhealthy: 0,
                data_at_risk: false,
                remaining_node_failures: Some(0),
                remaining_drive_failures: Some(2),
                protection_type: Some("PROTECTION_SYSTEM_TYPE_EC".to_string()),
                unhealthy_disk_details: vec![],
                unhealthy_psu_details: vec![],
            },
        };

        EnvironmentStatus {
            aggregates: Aggregates {
                cluster_count: 1,
                reachable_count: 1,
                total_nodes: 6,
                online_nodes: 5,
                capacity: cluster.capacity.clone(),
                files: cluster.files.clone(),
            },
            alerts: vec![Alert {
                severity: AlertSeverity::Critical,
                cluster: "iss-sg".to_string(),
                message: "node 4: OFFLINE".to_string(),
                category: "node_offline".to_string(),
            }],
            clusters: vec![cluster],
        }
    }

    fn make_full_status_unreachable() -> EnvironmentStatus {
        let cluster = ClusterStatus {
            profile: "az-dev".to_string(),
            name: "az-dev".to_string(),
            uuid: "az-uuid".to_string(),
            version: "v7.8.0".to_string(),
            cluster_type: ClusterType::AnqAzure,
            reachable: false,
            stale: true,
            latency_ms: 0,
            nodes: NodeStatus {
                total: 3,
                online: 3,
                offline_nodes: vec![],
                details: vec![],
            },
            capacity: CapacityStatus {
                total_bytes: 454_700_000_000_000,
                used_bytes: 22_100_000_000_000,
                free_bytes: 432_600_000_000_000,
                snapshot_bytes: 0,
                used_pct: 4.9,
                projection: None,
            },
            activity: ActivityStatus::default(),
            files: FileStats {
                total_files: 1_201,
                total_directories: 84,
                total_snapshots: 0,
                snapshot_bytes: 0,
            },
            health: HealthStatus {
                status: HealthLevel::Healthy,
                issues: vec![],
                disks_unhealthy: 0,
                psus_unhealthy: 0,
                data_at_risk: false,
                remaining_node_failures: Some(1),
                remaining_drive_failures: Some(2),
                protection_type: None,
                unhealthy_disk_details: vec![],
                unhealthy_psu_details: vec![],
            },
        };

        EnvironmentStatus {
            aggregates: Aggregates {
                cluster_count: 1,
                reachable_count: 0,
                total_nodes: 3,
                online_nodes: 3,
                capacity: cluster.capacity.clone(),
                files: cluster.files.clone(),
            },
            alerts: vec![Alert {
                severity: AlertSeverity::Critical,
                cluster: "az-dev".to_string(),
                message: "UNREACHABLE (last seen 2h ago)".to_string(),
                category: "connectivity".to_string(),
            }],
            clusters: vec![cluster],
        }
    }

    fn make_status_with_alerts() -> EnvironmentStatus {
        EnvironmentStatus {
            aggregates: Aggregates {
                cluster_count: 2,
                reachable_count: 2,
                total_nodes: 11,
                online_nodes: 10,
                capacity: CapacityStatus {
                    total_bytes: 1_300_000_000_000_000,
                    used_bytes: 1_100_000_000_000_000,
                    free_bytes: 200_000_000_000_000,
                    snapshot_bytes: 7_700_000_000_000,
                    used_pct: 84.6,
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
                    cluster: "gravytrain-sg".to_string(),
                    message: "projected to fill in ~62 days (+1.2 TB/day)".to_string(),
                    category: "capacity_projection".to_string(),
                },
            ],
            clusters: vec![],
        }
    }

    /// Strip ANSI escape codes for text-based assertions.
    fn strip_ansi(s: &str) -> String {
        let mut result = String::new();
        let mut in_escape = false;
        for c in s.chars() {
            if c == '\x1b' {
                in_escape = true;
            } else if in_escape {
                if c.is_ascii_alphabetic() {
                    in_escape = false;
                }
            } else {
                result.push(c);
            }
        }
        result
    }

    // ── Multi-cluster snapshot test ─────────────────────────────────────

    #[test]
    fn test_render_multi_cluster_full() {
        let gravytrain = ClusterStatus {
            profile: "gravytrain".to_string(),
            name: "gravytrain-sg".to_string(),
            uuid: "gt-uuid".to_string(),
            version: "v7.8.0".to_string(),
            cluster_type: ClusterType::OnPrem(vec!["C192T".to_string(), "QCT_D52T".to_string()]),
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
                    connection_breakdown: HashMap::new(),
                    nic_throughput_bps: Some(12_400_000_000),
                    nic_link_speed_bps: Some(200_000_000_000),
                    nic_utilization_pct: Some(6.0),
                    nic_bytes_total: None,
                }],
            },
            capacity: CapacityStatus {
                total_bytes: 605_000_000_000_000,
                used_bytes: 594_000_000_000_000,
                free_bytes: 11_000_000_000_000,
                snapshot_bytes: 6_700_000_000_000,
                used_pct: 98.2,
                projection: None,
            },
            activity: ActivityStatus {
                iops_read: 140.0,
                iops_write: 122.0,
                throughput_read: 57_800_000.0,
                throughput_write: 1_600_000.0,
                connections: 21,
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
                protection_type: None,
                unhealthy_disk_details: vec![],
                unhealthy_psu_details: vec![],
            },
        };

        let aws = ClusterStatus {
            profile: "aws-gravytrain".to_string(),
            name: "aws-gravytrain".to_string(),
            uuid: "aws-uuid".to_string(),
            version: "v7.8.0".to_string(),
            cluster_type: ClusterType::CnqAws,
            reachable: true,
            stale: false,
            latency_ms: 142,
            nodes: NodeStatus {
                total: 3,
                online: 3,
                offline_nodes: vec![],
                details: vec![],
            },
            capacity: CapacityStatus {
                total_bytes: 454_700_000_000_000,
                used_bytes: 77_300_000_000_000,
                free_bytes: 377_400_000_000_000,
                snapshot_bytes: 0,
                used_pct: 17.0,
                projection: None,
            },
            activity: ActivityStatus {
                iops_read: 0.0,
                iops_write: 0.0,
                throughput_read: 0.0,
                throughput_write: 0.0,
                connections: 0,
                is_idle: true,
            },
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
                remaining_node_failures: Some(1),
                remaining_drive_failures: Some(2),
                protection_type: None,
                unhealthy_disk_details: vec![],
                unhealthy_psu_details: vec![],
            },
        };

        let status = EnvironmentStatus {
            aggregates: Aggregates {
                cluster_count: 2,
                reachable_count: 2,
                total_nodes: 8,
                online_nodes: 8,
                capacity: CapacityStatus {
                    total_bytes: 1_059_700_000_000_000,
                    used_bytes: 671_300_000_000_000,
                    free_bytes: 388_400_000_000_000,
                    snapshot_bytes: 6_700_000_000_000,
                    used_pct: 63.3,
                    projection: None,
                },
                files: FileStats {
                    total_files: 501_240_560,
                    total_directories: 32_402_673,
                    total_snapshots: 8_201,
                    snapshot_bytes: 6_700_000_000_000,
                },
            },
            alerts: vec![],
            clusters: vec![gravytrain, aws],
        };

        let output = render(&status);
        let plain = strip_ansi(&output);

        // Overview section
        assert!(plain.contains("Clusters: 2 (all healthy)"));
        assert!(plain.contains("Latency: 42-142ms"));
        assert!(plain.contains("8 total (8 online)"));

        // Both cluster sections present — gravytrain has different profile/name
        assert!(plain.contains("gravytrain (gravytrain-sg)"));
        // aws-gravytrain has same profile/name, shown once
        assert!(plain.contains("aws-gravytrain"));
        assert!(plain.contains("on-prem · C192T, QCT_D52T"));
        assert!(plain.contains("CNQ · AWS"));
    }
}
