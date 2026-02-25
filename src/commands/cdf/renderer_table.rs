use console::Style;
use petgraph::visit::EdgeRef;

use super::types::*;

/// Render the CDF graph as a grouped-by-cluster adjacency table with status columns.
///
/// Format: cluster heading, then indented rows showing target, type, mode, status, lag, throughput.
pub fn render_table(graph: &CdfGraph) -> String {
    if graph.node_count() == 0 {
        return "(no CDF relationships found)\n".to_string();
    }

    let mut out = String::new();
    render_header(&mut out, graph);

    // Collect all edges grouped by source node
    let mut source_nodes: Vec<petgraph::graph::NodeIndex> = graph.node_indices().collect();
    // Sort by node label for stable output
    source_nodes.sort_by(|a, b| node_label(&graph[*a]).cmp(&node_label(&graph[*b])));

    for src_idx in &source_nodes {
        let edges: Vec<_> = graph.edges(*src_idx).collect();
        if edges.is_empty() {
            // Skip nodes with no outgoing edges
            // unless they also have no incoming edges (isolated)
            let incoming: Vec<_> = graph
                .edges_directed(*src_idx, petgraph::Direction::Incoming)
                .collect();
            if !incoming.is_empty() {
                continue;
            }
        }

        let node = &graph[*src_idx];
        render_cluster_heading(&mut out, node);

        if edges.is_empty() {
            out.push_str(&format!(
                "  {}\n",
                Style::new().dim().apply_to("(no outgoing relationships)")
            ));
        } else {
            render_column_headers(&mut out);
            for edge_ref in &edges {
                let target_node = &graph[edge_ref.target()];
                let edge = edge_ref.weight();
                render_edge_row(&mut out, target_node, edge);
            }
        }
        out.push('\n');
    }

    out
}

fn render_header(out: &mut String, graph: &CdfGraph) {
    let bold = Style::new().bold();
    let title = "═══ Data Fabric Status ";
    let padding = 80_usize.saturating_sub(title.len());
    out.push_str(&format!(
        "{}\n",
        bold.apply_to(format!("{}{}", title, "═".repeat(padding)))
    ));
    out.push_str(&format!(
        "  {} clusters, {} relationships\n\n",
        graph.node_count(),
        graph.edge_count()
    ));
}

fn render_cluster_heading(out: &mut String, node: &CdfNode) {
    let bold = Style::new().bold();
    let label = node_label(node);
    let address = node_address(node);

    let heading = if address.is_empty() || label.contains(&address) {
        label
    } else {
        format!("{} ({})", label, address)
    };

    out.push_str(&format!("{}\n", bold.apply_to(heading)));
}

fn render_column_headers(out: &mut String) {
    let dim = Style::new().dim();
    out.push_str(&format!(
        "  {:<24} {:<8} {:<14} {:<14} {:<14} {}\n",
        dim.apply_to("TARGET"),
        dim.apply_to("TYPE"),
        dim.apply_to("MODE"),
        dim.apply_to("STATUS"),
        dim.apply_to("LAG"),
        dim.apply_to("THROUGHPUT"),
    ));
}

fn render_edge_row(out: &mut String, target: &CdfNode, edge: &CdfEdge) {
    let target_name = node_label(target);
    let (edge_type, mode, status, lag, throughput) = extract_edge_fields(edge);

    let style = edge_style(edge);
    let status_style = status_color(&status);

    out.push_str(&format!(
        "  {:<24} {:<8} {:<14} {} {:<14} {}\n",
        style.apply_to(truncate(&target_name, 24)),
        style.apply_to(&edge_type),
        mode,
        pad_styled(&status_style.apply_to(&status).to_string(), &status, 14),
        lag,
        throughput,
    ));
}

fn extract_edge_fields(edge: &CdfEdge) -> (String, String, String, String, String) {
    match edge {
        CdfEdge::Portal {
            portal_type,
            status,
            ..
        } => {
            let short_type = portal_type
                .strip_prefix("PORTAL_")
                .unwrap_or(portal_type)
                .to_lowercase()
                .replace('_', "-");
            let mode = short_type.clone();
            let display_status = status.to_lowercase();
            ("portal".into(), mode, display_status, "-".into(), "-".into())
        }
        CdfEdge::Replication {
            mode,
            enabled,
            state,
            job_state,
            recovery_point,
            replication_job_status,
            ..
        } => {
            let short_mode = shorten_mode(
                &mode
                    .as_deref()
                    .and_then(|m| m.strip_prefix("REPLICATION_"))
                    .unwrap_or(mode.as_deref().unwrap_or("?"))
                    .to_lowercase(),
            );

            let status = if !enabled {
                "disabled".into()
            } else {
                format_replication_status(state.as_deref(), job_state.as_deref())
            };

            let lag = format_recovery_lag(recovery_point.as_deref());
            let throughput = format_throughput(replication_job_status.as_ref());

            ("repl".into(), short_mode, status, lag, throughput)
        }
        CdfEdge::ObjectReplication {
            direction,
            state,
            ..
        } => {
            let dir = direction.as_deref().unwrap_or("?");
            let short_dir = match dir {
                "COPY_TO_OBJECT" => "copy-to",
                "COPY_FROM_OBJECT" => "copy-from",
                other => other,
            };
            let status = shorten_status(
                &state.as_deref().unwrap_or("?").to_lowercase(),
            );
            (
                "S3".into(),
                short_dir.into(),
                status,
                "-".into(),
                "-".into(),
            )
        }
    }
}

fn shorten_mode(mode: &str) -> String {
    match mode {
        "snapshot_policy_with_continuous" => "snap+cont".into(),
        "snapshot_policy" => "snapshot".into(),
        other => other.into(),
    }
}

fn shorten_status(status: &str) -> String {
    status
        .strip_prefix("replication_")
        .unwrap_or(status)
        .replace('_', " ")
}

fn format_replication_status(state: Option<&str>, job_state: Option<&str>) -> String {
    match (state, job_state) {
        (_, Some(js)) => {
            let short = js
                .strip_prefix("REPLICATION_")
                .unwrap_or(js)
                .to_lowercase()
                .replace('_', " ");
            short
        }
        (Some(s), None) => s.to_lowercase(),
        (None, None) => "?".into(),
    }
}

fn format_recovery_lag(recovery_point: Option<&str>) -> String {
    let Some(rp) = recovery_point else {
        return "-".into();
    };

    // Parse ISO 8601 timestamp and compute time since
    let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(rp) else {
        return rp.to_string();
    };

    let now = chrono::Utc::now();
    let delta = now.signed_duration_since(parsed);

    if delta.num_seconds() < 0 {
        return "0s".into();
    }

    if delta.num_days() > 0 {
        format!("{}d ago", delta.num_days())
    } else if delta.num_hours() > 0 {
        format!("{}h ago", delta.num_hours())
    } else if delta.num_minutes() > 0 {
        format!("{}m ago", delta.num_minutes())
    } else {
        format!("{}s ago", delta.num_seconds())
    }
}

fn format_throughput(job_status: Option<&ReplicationJobStatus>) -> String {
    let Some(js) = job_status else {
        return "-".into();
    };

    if let Some(tp) = &js.throughput_current {
        if let Ok(bytes_per_sec) = tp.parse::<f64>() {
            return format_bytes_per_sec(bytes_per_sec);
        }
        return tp.clone();
    }

    "-".into()
}

fn format_bytes_per_sec(bps: f64) -> String {
    if bps >= 1_073_741_824.0 {
        format!("{:.1} GB/s", bps / 1_073_741_824.0)
    } else if bps >= 1_048_576.0 {
        format!("{:.1} MB/s", bps / 1_048_576.0)
    } else if bps >= 1024.0 {
        format!("{:.1} KB/s", bps / 1024.0)
    } else {
        format!("{:.0} B/s", bps)
    }
}

fn node_label(node: &CdfNode) -> String {
    match node {
        CdfNode::ProfiledCluster { name, .. } => name.clone(),
        CdfNode::UnknownCluster { address, .. } => {
            if address.is_empty() {
                "unknown".to_string()
            } else {
                format!("{} (unknown)", address)
            }
        }
        CdfNode::S3Bucket { bucket, .. } => format!("s3://{}", bucket),
    }
}

fn node_address(node: &CdfNode) -> String {
    match node {
        CdfNode::ProfiledCluster { address, .. } => address.clone(),
        CdfNode::UnknownCluster { address, .. } => address.clone(),
        CdfNode::S3Bucket { address, .. } => address.clone(),
    }
}

fn edge_style(edge: &CdfEdge) -> Style {
    match edge {
        CdfEdge::Portal { .. } => Style::new().green(),
        CdfEdge::Replication { .. } => Style::new().blue(),
        CdfEdge::ObjectReplication { .. } => Style::new().yellow(),
    }
}

fn status_color(status: &str) -> Style {
    let lower = status.to_lowercase();
    if lower.contains("running") || lower == "active" || lower == "established" {
        Style::new().green()
    } else if lower.contains("disabled") || lower.contains("not running") || lower == "inactive" {
        Style::new().red()
    } else if lower.contains("pending") || lower.contains("waiting") {
        Style::new().yellow()
    } else {
        Style::new()
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max - 1])
    }
}

/// Pad a styled string (with ANSI codes) to a visible width.
fn pad_styled(styled: &str, raw: &str, width: usize) -> String {
    let visible = raw.len();
    if visible >= width {
        styled.to_string()
    } else {
        format!("{}{}", styled, " ".repeat(width - visible))
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use petgraph::graph::DiGraph;

    fn make_test_graph() -> CdfGraph {
        let mut graph = DiGraph::new();
        let n1 = graph.add_node(CdfNode::ProfiledCluster {
            name: "gravytrain".into(),
            uuid: "uuid-aaaa-1111".into(),
            address: "10.0.0.1".into(),
        });
        let n2 = graph.add_node(CdfNode::ProfiledCluster {
            name: "iss".into(),
            uuid: "uuid-bbbb-2222".into(),
            address: "10.0.1.1".into(),
        });
        let n3 = graph.add_node(CdfNode::S3Bucket {
            address: "s3.us-west-2.amazonaws.com".into(),
            bucket: "backup-bucket".into(),
            region: Some("us-west-2".into()),
        });

        graph.add_edge(
            n1,
            n2,
            CdfEdge::Replication {
                source_path: Some("/data".into()),
                target_path: Some("/replica".into()),
                mode: Some("REPLICATION_CONTINUOUS".into()),
                enabled: true,
                state: Some("ESTABLISHED".into()),
                job_state: Some("REPLICATION_RUNNING".into()),
                recovery_point: None,
                error_from_last_job: None,
                replication_job_status: Some(ReplicationJobStatus {
                    percent_complete: Some("50.0".into()),
                    estimated_seconds_remaining: None,
                    bytes_transferred: Some("1024000".into()),
                    bytes_unchanged: None,
                    bytes_remaining: None,
                    bytes_deleted: None,
                    bytes_total: Some("2048000".into()),
                    files_transferred: None,
                    files_unchanged: None,
                    files_remaining: None,
                    files_deleted: None,
                    files_total: None,
                    throughput_overall: None,
                    throughput_current: Some("131072000".into()),
                }),
            },
        );
        graph.add_edge(
            n1,
            n2,
            CdfEdge::Portal {
                hub_id: 1,
                spoke_id: 5,
                portal_type: "PORTAL_READ_WRITE".into(),
                state: "ACCEPTED".into(),
                status: "ACTIVE".into(),
            },
        );
        graph.add_edge(
            n2,
            n3,
            CdfEdge::ObjectReplication {
                direction: Some("COPY_TO_OBJECT".into()),
                bucket: Some("backup-bucket".into()),
                folder: Some("daily/".into()),
                state: Some("ACTIVE".into()),
            },
        );
        graph
    }

    #[test]
    fn test_render_table_empty() {
        let graph = CdfGraph::new();
        let output = render_table(&graph);
        assert_eq!(output, "(no CDF relationships found)\n");
    }

    #[test]
    fn test_render_table_header() {
        let graph = make_test_graph();
        let output = render_table(&graph);
        assert!(output.contains("Data Fabric Status"));
        assert!(output.contains("3 clusters, 3 relationships"));
    }

    #[test]
    fn test_render_table_cluster_headings() {
        let graph = make_test_graph();
        let output = render_table(&graph);
        assert!(output.contains("gravytrain"));
        assert!(output.contains("iss"));
    }

    #[test]
    fn test_render_table_edge_rows() {
        let graph = make_test_graph();
        let output = render_table(&graph);
        // Check replication row
        assert!(output.contains("repl"));
        assert!(output.contains("continuous"));
        assert!(output.contains("running"));
        // Check portal row
        assert!(output.contains("portal"));
        assert!(output.contains("read-write"));
        assert!(output.contains("active"));
        // Check S3 row
        assert!(output.contains("S3"));
        assert!(output.contains("copy-to"));
    }

    #[test]
    fn test_render_table_throughput() {
        let graph = make_test_graph();
        let output = render_table(&graph);
        assert!(output.contains("MB/s"));
    }

    #[test]
    fn test_format_bytes_per_sec() {
        assert_eq!(format_bytes_per_sec(500.0), "500 B/s");
        assert_eq!(format_bytes_per_sec(1500.0), "1.5 KB/s");
        assert_eq!(format_bytes_per_sec(1_500_000.0), "1.4 MB/s");
        assert_eq!(format_bytes_per_sec(1_500_000_000.0), "1.4 GB/s");
    }

    #[test]
    fn test_format_replication_status() {
        assert_eq!(
            format_replication_status(Some("ESTABLISHED"), Some("REPLICATION_RUNNING")),
            "running"
        );
        assert_eq!(
            format_replication_status(Some("ESTABLISHED"), None),
            "established"
        );
        assert_eq!(format_replication_status(None, None), "?");
    }

    #[test]
    fn test_status_color_variants() {
        // Just verify these don't panic
        let _ = status_color("active");
        let _ = status_color("disabled");
        let _ = status_color("pending");
        let _ = status_color("unknown");
    }

    #[test]
    fn test_truncate() {
        assert_eq!(truncate("short", 10), "short");
        assert_eq!(truncate("a-very-long-name-here", 10), "a-very-lo…");
    }

    #[test]
    fn test_disabled_replication_status() {
        let mut graph = CdfGraph::new();
        let n1 = graph.add_node(CdfNode::ProfiledCluster {
            name: "src".into(),
            uuid: "u1".into(),
            address: "10.0.0.1".into(),
        });
        let n2 = graph.add_node(CdfNode::ProfiledCluster {
            name: "dst".into(),
            uuid: "u2".into(),
            address: "10.0.0.2".into(),
        });
        graph.add_edge(
            n1,
            n2,
            CdfEdge::Replication {
                source_path: None,
                target_path: None,
                mode: None,
                enabled: false,
                state: None,
                job_state: None,
                recovery_point: None,
                error_from_last_job: None,
                replication_job_status: None,
            },
        );
        let output = render_table(&graph);
        assert!(output.contains("disabled"));
    }
}
