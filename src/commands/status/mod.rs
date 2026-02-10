pub mod cache;
pub mod capacity;
pub mod collector;
pub mod detection;
pub mod types;

use std::thread;
use std::time::Duration;

use anyhow::Result;
use console::Style;

use crate::config::Config;

use types::*;

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
    loop {
        let status = collector::collect_all(config, profiles, timeout_secs, no_cache)?;

        if json_mode {
            println!(
                "{}",
                serde_json::to_string_pretty(&status).unwrap_or_else(|_| "{}".to_string())
            );
        } else {
            print_status(&status);
        }

        if !watch {
            break;
        }

        thread::sleep(Duration::from_secs(interval));
        if !json_mode {
            print!("\x1B[2J\x1B[H");
        }
    }

    Ok(())
}

fn print_status(status: &EnvironmentStatus) {
    let bold = Style::new().bold();
    let green = Style::new().green();
    let yellow = Style::new().yellow();
    let red = Style::new().red();
    let dim = Style::new().dim();

    // Header
    let agg = &status.aggregates;
    println!(
        "{} {} clusters ({} reachable), {} nodes ({} online)",
        bold.apply_to("Environment:"),
        agg.cluster_count,
        agg.reachable_count,
        agg.total_nodes,
        agg.online_nodes,
    );
    println!();

    // Alerts
    if !status.alerts.is_empty() {
        for alert in &status.alerts {
            let style = match alert.severity {
                AlertSeverity::Critical => &red,
                AlertSeverity::Warning => &yellow,
                AlertSeverity::Info => &dim,
            };
            let label = match alert.severity {
                AlertSeverity::Critical => "CRIT",
                AlertSeverity::Warning => "WARN",
                AlertSeverity::Info => "INFO",
            };
            println!(
                "  {} [{}] {}",
                style.apply_to(label),
                alert.cluster,
                alert.message,
            );
        }
        println!();
    }

    // Per-cluster summary
    for cluster in &status.clusters {
        let name_style = if cluster.reachable { &green } else { &red };
        let stale_marker = if cluster.stale {
            format!(" {}", dim.apply_to("(cached)"))
        } else {
            String::new()
        };

        println!(
            "  {} {}  {}  {}  {}/{}  {}ms{}",
            name_style.apply_to("●"),
            bold.apply_to(&cluster.name),
            dim.apply_to(&cluster.version),
            cluster.cluster_type,
            cluster.nodes.online,
            cluster.nodes.total,
            cluster.latency_ms,
            stale_marker,
        );

        // Show capacity projection warning if applicable
        if let Some(ref projection) = cluster.capacity.projection {
            if capacity::should_warn(projection, &cluster.cluster_type) {
                let msg = capacity::format_warning(projection, &cluster.cluster_type);
                println!("    {} {}", yellow.apply_to("⚠"), msg);
            }
        }
    }
    println!();

    // File/snapshot stats
    if agg.files.total_files > 0 || agg.files.total_directories > 0 {
        println!(
            "  Files: {}  Dirs: {}  Snapshots: {} ({})",
            format_number(agg.files.total_files),
            format_number(agg.files.total_directories),
            format_number(agg.files.total_snapshots),
            format_bytes(agg.files.snapshot_bytes),
        );
        println!();
    }

    // Aggregate capacity
    if agg.capacity.total_bytes > 0 {
        let pct = agg.capacity.used_pct;
        let bar_style = if pct >= 90.0 {
            &red
        } else if pct >= 70.0 {
            &yellow
        } else {
            &green
        };

        let bar = render_bar(pct as u64, 30);
        println!("{}", bold.apply_to("Capacity (total):"));
        println!("  {} {:.0}%", bar_style.apply_to(&bar), pct,);
        println!(
            "  Total: {}  Used: {}  Free: {}",
            format_bytes(agg.capacity.total_bytes),
            format_bytes(agg.capacity.used_bytes),
            green.apply_to(format_bytes(agg.capacity.free_bytes)),
        );
    }
    println!();

    // Per-cluster detail
    for cluster in &status.clusters {
        if !cluster.reachable {
            continue;
        }
        let activity = &cluster.activity;
        if activity.is_idle {
            println!("  {} Activity: {}", dim.apply_to("·"), dim.apply_to("idle"));
        } else {
            println!(
                "  {} Activity: R: {:.0} IOPS / {}/s  W: {:.0} IOPS / {}/s",
                dim.apply_to("·"),
                activity.iops_read,
                format_bytes(activity.throughput_read as u64),
                activity.iops_write,
                format_bytes(activity.throughput_write as u64),
            );
        }
    }
}

fn format_number(n: u64) -> String {
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

fn render_bar(pct: u64, width: usize) -> String {
    let filled = (pct as usize * width) / 100;
    let empty = width.saturating_sub(filled);
    format!("[{}{}]", "█".repeat(filled), "░".repeat(empty))
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
