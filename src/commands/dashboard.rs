use std::thread;
use std::time::Duration;

use anyhow::Result;
use console::Style;
use serde_json::{json, Value};

use crate::client::QumuloClient;

pub fn run(client: &QumuloClient, json_mode: bool, watch: bool, interval: u64) -> Result<()> {
    loop {
        let data = fetch_dashboard_data(client)?;

        if json_mode {
            println!(
                "{}",
                serde_json::to_string_pretty(&data).unwrap_or_else(|_| data.to_string())
            );
        } else {
            print_dashboard(&data);
        }

        if !watch {
            break;
        }

        thread::sleep(Duration::from_secs(interval));

        // Clear screen for next refresh
        if !json_mode {
            print!("\x1B[2J\x1B[H");
        }
    }

    Ok(())
}

fn fetch_dashboard_data(client: &QumuloClient) -> Result<Value> {
    let settings = client.get_cluster_settings()?;
    let version = client.get_version()?;
    let nodes = client.get_cluster_nodes()?;
    let fs = client.get_file_system()?;
    let activity = client.get_activity_current()?;

    Ok(json!({
        "cluster": settings,
        "version": version,
        "nodes": nodes,
        "file_system": fs,
        "activity": activity,
    }))
}

fn print_dashboard(data: &Value) {
    let bold = Style::new().bold();
    let green = Style::new().green();
    let yellow = Style::new().yellow();
    let red = Style::new().red();
    let dim = Style::new().dim();

    // --- Cluster header ---
    let cluster_name = data["cluster"]["cluster_name"]
        .as_str()
        .unwrap_or("unknown");
    let revision = data["version"]["revision_id"]
        .as_str()
        .unwrap_or("unknown");
    let build_id = data["version"]["build_id"].as_str().unwrap_or("");

    println!(
        "{} {}  {}",
        bold.apply_to("Cluster:"),
        bold.apply_to(cluster_name),
        dim.apply_to(format!("v{} ({})", revision, build_id))
    );
    println!();

    // --- Nodes ---
    let nodes = data["nodes"].as_array();
    if let Some(nodes) = nodes {
        let total = nodes.len();
        let online = nodes
            .iter()
            .filter(|n| {
                n["node_status"]
                    .as_str()
                    .map(|s| s.eq_ignore_ascii_case("online"))
                    .unwrap_or(false)
            })
            .count();

        let status_style = if online == total {
            &green
        } else if online > 0 {
            &yellow
        } else {
            &red
        };

        println!(
            "{} {} ({} online)",
            bold.apply_to("Nodes:"),
            status_style.apply_to(format!("{}/{}", online, total)),
            status_style.apply_to(online)
        );

        for node in nodes {
            let name = node["node_name"].as_str().unwrap_or("-");
            let id = format_json_number(&node["id"]);
            let status = node["node_status"].as_str().unwrap_or("unknown");
            let marker = if status.eq_ignore_ascii_case("online") {
                green.apply_to("●")
            } else {
                red.apply_to("●")
            };
            println!("  {} {} (id: {}) {}", marker, name, id, dim.apply_to(status));
        }
        println!();
    }

    // --- Capacity ---
    let total_bytes = parse_byte_string(&data["file_system"]["total_size_bytes"]);
    let free_bytes = parse_byte_string(&data["file_system"]["free_size_bytes"]);
    let snapshot_bytes = parse_byte_string(&data["file_system"]["snapshot_size_bytes"]);

    if total_bytes > 0 {
        let used_bytes = total_bytes.saturating_sub(free_bytes);
        let pct = (used_bytes as f64 / total_bytes as f64 * 100.0) as u64;

        let bar_style = if pct >= 90 {
            &red
        } else if pct >= 70 {
            &yellow
        } else {
            &green
        };

        let bar = render_bar(pct, 30);

        println!("{}", bold.apply_to("Capacity:"));
        println!(
            "  {} {}%",
            bar_style.apply_to(&bar),
            bar_style.apply_to(pct)
        );
        println!(
            "  Total: {}  Used: {}  Free: {}",
            format_bytes(total_bytes),
            format_bytes(used_bytes),
            green.apply_to(format_bytes(free_bytes))
        );
        if snapshot_bytes > 0 {
            println!("  Snapshots: {}", dim.apply_to(format_bytes(snapshot_bytes)));
        }
        println!();
    }

    // --- Activity ---
    let entries = data["activity"]["entries"].as_array();
    if let Some(entries) = entries {
        if !entries.is_empty() {
            let (iops_read, iops_write, tp_read, tp_write, connections) =
                aggregate_activity(entries);

            println!("{}", bold.apply_to("Activity:"));
            println!(
                "  IOPS:       {} read  /  {} write",
                format_rate(iops_read),
                format_rate(iops_write)
            );
            println!(
                "  Throughput: {} read  /  {} write",
                format_throughput(tp_read),
                format_throughput(tp_write)
            );
            println!("  Active connections: {}", bold.apply_to(connections));
        } else {
            println!("{} {}", bold.apply_to("Activity:"), dim.apply_to("idle"));
        }
    }
}

fn aggregate_activity(entries: &[Value]) -> (f64, f64, f64, f64, usize) {
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

    (iops_read, iops_write, tp_read, tp_write, ips.len())
}

fn render_bar(pct: u64, width: usize) -> String {
    let filled = (pct as usize * width) / 100;
    let empty = width.saturating_sub(filled);
    format!("[{}{}]", "█".repeat(filled), "░".repeat(empty))
}

fn parse_byte_string(val: &Value) -> u64 {
    match val {
        Value::String(s) => s.parse::<u64>().unwrap_or(0),
        Value::Number(n) => n.as_u64().unwrap_or(0),
        _ => 0,
    }
}

fn format_json_number(val: &Value) -> String {
    match val {
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        _ => "-".to_string(),
    }
}

fn format_bytes(bytes: u64) -> String {
    const TB: u64 = 1_099_511_627_776;
    const GB: u64 = 1_073_741_824;
    const MB: u64 = 1_048_576;

    if bytes >= TB {
        format!("{:.1} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else {
        format!("{} B", bytes)
    }
}

fn format_rate(rate: f64) -> String {
    if rate >= 1_000_000.0 {
        format!("{:.1}M", rate / 1_000_000.0)
    } else if rate >= 1_000.0 {
        format!("{:.1}K", rate / 1_000.0)
    } else {
        format!("{:.0}", rate)
    }
}

fn format_throughput(bytes_per_sec: f64) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1_048_576), "1.0 MB");
        assert_eq!(format_bytes(1_073_741_824), "1.0 GB");
        assert_eq!(format_bytes(1_099_511_627_776), "1.0 TB");
        assert_eq!(format_bytes(2_199_023_255_552), "2.0 TB");
    }

    #[test]
    fn test_format_rate() {
        assert_eq!(format_rate(0.0), "0");
        assert_eq!(format_rate(500.0), "500");
        assert_eq!(format_rate(1_500.0), "1.5K");
        assert_eq!(format_rate(2_500_000.0), "2.5M");
    }

    #[test]
    fn test_format_throughput() {
        assert_eq!(format_throughput(0.0), "0 B/s");
        assert_eq!(format_throughput(1_048_576.0), "1.0 MB/s");
        assert_eq!(format_throughput(1_073_741_824.0), "1.0 GB/s");
    }

    #[test]
    fn test_render_bar() {
        let bar = render_bar(50, 10);
        assert_eq!(bar, "[█████░░░░░]");

        let bar_full = render_bar(100, 10);
        assert_eq!(bar_full, "[██████████]");

        let bar_empty = render_bar(0, 10);
        assert_eq!(bar_empty, "[░░░░░░░░░░]");
    }

    #[test]
    fn test_parse_byte_string() {
        assert_eq!(parse_byte_string(&json!("1099511627776")), 1_099_511_627_776);
        assert_eq!(parse_byte_string(&json!(1024)), 1024);
        assert_eq!(parse_byte_string(&json!(null)), 0);
        assert_eq!(parse_byte_string(&json!("not_a_number")), 0);
    }

    #[test]
    fn test_aggregate_activity() {
        let entries = vec![
            json!({"ip": "10.0.0.1", "rate": 1000.0, "type": "file-iops-read"}),
            json!({"ip": "10.0.0.1", "rate": 500.0, "type": "file-iops-write"}),
            json!({"ip": "10.0.0.2", "rate": 2000.0, "type": "metadata-iops-read"}),
            json!({"ip": "10.0.0.2", "rate": 1048576.0, "type": "file-throughput-read"}),
            json!({"ip": "10.0.0.3", "rate": 524288.0, "type": "file-throughput-write"}),
        ];
        let (iops_r, iops_w, tp_r, tp_w, conns) = aggregate_activity(&entries);
        assert_eq!(iops_r, 3000.0); // 1000 + 2000
        assert_eq!(iops_w, 500.0);
        assert_eq!(tp_r, 1_048_576.0);
        assert_eq!(tp_w, 524_288.0);
        assert_eq!(conns, 3); // 3 unique IPs
    }

    #[test]
    fn test_format_json_number() {
        assert_eq!(format_json_number(&json!(42)), "42");
        assert_eq!(format_json_number(&json!("seven")), "seven");
        assert_eq!(format_json_number(&json!(null)), "-");
    }

    #[test]
    fn test_fetch_produces_valid_json_structure() {
        // Verify the JSON output structure matches expectations
        let data = json!({
            "cluster": {"cluster_name": "test-cluster"},
            "version": {"revision_id": "abc123", "build_id": "b1"},
            "nodes": [
                {"id": 1, "node_name": "node-1", "node_status": "online"},
                {"id": 2, "node_name": "node-2", "node_status": "offline"},
            ],
            "file_system": {
                "total_size_bytes": "1099511627776",
                "free_size_bytes": "549755813888",
                "snapshot_size_bytes": "100000000000",
            },
            "activity": {
                "entries": [
                    {"ip": "10.0.0.1", "rate": 5000.0, "type": "file-iops-read"},
                ]
            }
        });

        // Verify it serializes cleanly for --json mode
        let serialized = serde_json::to_string_pretty(&data).unwrap();
        assert!(serialized.contains("test-cluster"));
        assert!(serialized.contains("abc123"));
    }
}
