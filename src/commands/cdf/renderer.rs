use console::Style;
use petgraph::visit::EdgeRef;
use std::collections::BTreeMap;

use super::types::*;

// ── Public entry point ──────────────────────────────────────────────────────

/// Render the CDF graph as styled terminal output.
///
/// Returns a string with embedded ANSI codes (via `console` crate).
/// `detail` enables per-edge metadata like paths, modes, states.
pub fn render(graph: &CdfGraph, detail: bool) -> String {
    if graph.node_count() == 0 {
        return "(no CDF relationships found)\n".to_string();
    }

    let mut out = String::new();
    render_header(&mut out, graph);

    if graph.node_count() == 1 && graph.edge_count() == 0 {
        render_single_node(&mut out, graph);
        return out;
    }

    render_tree(&mut out, graph, detail);
    out
}

// ── Styles ──────────────────────────────────────────────────────────────────

fn style_bold() -> Style {
    Style::new().bold()
}

fn style_portal() -> Style {
    Style::new().green()
}

fn style_replication() -> Style {
    Style::new().blue()
}

fn style_object() -> Style {
    Style::new().yellow()
}

fn style_disabled() -> Style {
    Style::new().red()
}

// ── Node labels ─────────────────────────────────────────────────────────────

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
        CdfNode::S3Bucket { bucket, .. } => format!("S3:{}", bucket),
    }
}

#[cfg(test)]
fn node_label_full(node: &CdfNode) -> String {
    match node {
        CdfNode::ProfiledCluster { name, .. } => name.clone(),
        CdfNode::UnknownCluster { address, .. } => {
            if address.is_empty() {
                "unknown".to_string()
            } else {
                format!("{} (unknown)", address)
            }
        }
        CdfNode::S3Bucket {
            bucket, address, ..
        } => format!("S3: {} @ {}", bucket, address),
    }
}

fn node_type_label(node: &CdfNode) -> &'static str {
    match node {
        CdfNode::ProfiledCluster { .. } => "cluster",
        CdfNode::UnknownCluster { .. } => "cluster",
        CdfNode::S3Bucket { .. } => "s3",
    }
}

// ── Rendering ───────────────────────────────────────────────────────────────

fn render_header(out: &mut String, graph: &CdfGraph) {
    let bold = style_bold();
    let title = "═══ Data Fabric Topology ";
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

fn render_single_node(out: &mut String, graph: &CdfGraph) {
    for idx in graph.node_indices() {
        let node = &graph[idx];
        let label = node_label(node);
        let type_label = node_type_label(node);
        out.push_str(&format!(
            "  {} ({})\n",
            style_bold().apply_to(&label),
            Style::new().dim().apply_to(type_label)
        ));
    }
}

/// Key for grouping duplicate edges: (edge_type_tag, short_label, disabled).
/// Edges with the same key to the same target get collapsed with a count.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct EdgeGroupKey {
    type_tag: &'static str, // "portal", "replication", "S3"
    short_label: String,    // e.g. "continuous", "read write", "copy-to"
    disabled: bool,
}

struct EdgeGroupValue {
    count: usize,
    /// One representative edge for detail rendering
    representative: usize, // index into edges vec
}

fn edge_group_key(edge: &CdfEdge) -> EdgeGroupKey {
    match edge {
        CdfEdge::Portal { portal_type, .. } => {
            let short = portal_type
                .strip_prefix("PORTAL_")
                .unwrap_or(portal_type)
                .to_lowercase()
                .replace('_', " ");
            EdgeGroupKey {
                type_tag: "portal",
                short_label: short,
                disabled: false,
            }
        }
        CdfEdge::Replication { mode, enabled, .. } => {
            let short = mode
                .as_deref()
                .and_then(|m| m.strip_prefix("REPLICATION_"))
                .unwrap_or(mode.as_deref().unwrap_or("?"))
                .to_lowercase();
            EdgeGroupKey {
                type_tag: "replication",
                short_label: short,
                disabled: !enabled,
            }
        }
        CdfEdge::ObjectReplication { direction, .. } => {
            let dir = direction.as_deref().unwrap_or("?");
            let short = match dir {
                "COPY_TO_OBJECT" => "copy-to",
                "COPY_FROM_OBJECT" => "copy-from",
                other => other,
            };
            EdgeGroupKey {
                type_tag: "S3",
                short_label: short.to_string(),
                disabled: false,
            }
        }
    }
}

fn style_for_edge(edge: &CdfEdge) -> Style {
    match edge {
        CdfEdge::Portal { .. } => style_portal(),
        CdfEdge::Replication { enabled, .. } if !enabled => style_disabled(),
        CdfEdge::Replication { .. } => style_replication(),
        CdfEdge::ObjectReplication { .. } => style_object(),
    }
}

fn render_tree(out: &mut String, graph: &CdfGraph, detail: bool) {
    // Collect all source nodes that have outgoing edges, ordered by label
    let mut source_nodes: Vec<petgraph::graph::NodeIndex> = graph
        .node_indices()
        .filter(|&idx| graph.edges(idx).next().is_some())
        .collect();
    source_nodes.sort_by(|a, b| node_label(&graph[*a]).cmp(&node_label(&graph[*b])));

    // Also collect isolated nodes (no outgoing edges, but may have incoming)
    // and truly isolated nodes (no edges at all)
    let mut mentioned_as_target: std::collections::HashSet<petgraph::graph::NodeIndex> =
        std::collections::HashSet::new();
    for idx in graph.node_indices() {
        for edge in graph.edges(idx) {
            mentioned_as_target.insert(edge.target());
        }
    }

    for (src_i, &src_idx) in source_nodes.iter().enumerate() {
        let src_node = &graph[src_idx];
        let src_label = node_label(src_node);
        let type_label = node_type_label(src_node);

        // Source header
        out.push_str(&format!(
            "{} ({})\n",
            style_bold().apply_to(&src_label),
            Style::new().dim().apply_to(type_label)
        ));

        // Group edges by target, then by edge key
        // Use BTreeMap for stable ordering
        let mut targets: BTreeMap<
            petgraph::graph::NodeIndex,
            BTreeMap<EdgeGroupKey, EdgeGroupValue>,
        > = BTreeMap::new();
        let mut edge_store: Vec<CdfEdge> = Vec::new();

        for edge_ref in graph.edges(src_idx) {
            let tgt_idx = edge_ref.target();
            let edge = edge_ref.weight().clone();
            let key = edge_group_key(&edge);
            let edge_i = edge_store.len();
            edge_store.push(edge);

            let target_groups = targets.entry(tgt_idx).or_default();
            target_groups
                .entry(key)
                .and_modify(|v| v.count += 1)
                .or_insert(EdgeGroupValue {
                    count: 1,
                    representative: edge_i,
                });
        }

        // Sort targets by label for stable output
        let mut target_entries: Vec<_> = targets.into_iter().collect();
        target_entries.sort_by(|a, b| node_label(&graph[a.0]).cmp(&node_label(&graph[b.0])));

        let num_targets = target_entries.len();
        for (tgt_i, (tgt_idx, groups)) in target_entries.into_iter().enumerate() {
            let tgt_label = node_label(&graph[tgt_idx]);
            let is_last_target = tgt_i == num_targets - 1;

            // Collect all edge group descriptions for this target into one line
            let mut edge_parts: Vec<String> = Vec::new();
            let mut edge_styles: Vec<Style> = Vec::new();

            let groups_vec: Vec<_> = groups.into_iter().collect();
            for (key, value) in &groups_vec {
                let edge = &edge_store[value.representative];
                let style = style_for_edge(edge);

                let mut part = format!("{} ({})", key.type_tag, key.short_label);
                if key.disabled {
                    part.push_str(" [DISABLED]");
                }
                if value.count > 1 {
                    part.push_str(&format!(" x{}", value.count));
                }
                edge_parts.push(part);
                edge_styles.push(style);
            }

            // Build the line
            let connector = if is_last_target { "└── " } else { "├── " };
            out.push_str(connector);
            out.push_str(&format!("→ {}: ", style_bold().apply_to(&tgt_label)));

            for (i, (part, style)) in edge_parts.iter().zip(edge_styles.iter()).enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                out.push_str(&format!("{}", style.apply_to(part)));
            }
            out.push('\n');

            // Detail lines for each edge group
            if detail {
                for (key, value) in &groups_vec {
                    let edge = &edge_store[value.representative];
                    if let Some(detail_str) = format_edge_detail(edge) {
                        let prefix = if is_last_target {
                            "    "
                        } else {
                            "│   "
                        };
                        out.push_str(&format!(
                            "{}  {} ({}): {}\n",
                            prefix,
                            key.type_tag,
                            key.short_label,
                            Style::new().dim().apply_to(&detail_str)
                        ));
                    }
                }
            }
        }

        // Blank line between source clusters (except after last)
        if src_i < source_nodes.len() - 1 {
            out.push('\n');
        }
    }

    // Show isolated nodes that are only targets (no outgoing edges) and not sources
    let source_set: std::collections::HashSet<_> = source_nodes.iter().copied().collect();
    let mut isolated: Vec<_> = graph
        .node_indices()
        .filter(|idx| !source_set.contains(idx) && !mentioned_as_target.contains(idx))
        .collect();
    isolated.sort_by(|a, b| node_label(&graph[*a]).cmp(&node_label(&graph[*b])));

    if !isolated.is_empty() {
        if !source_nodes.is_empty() {
            out.push('\n');
        }
        for idx in &isolated {
            let node = &graph[*idx];
            let label = node_label(node);
            let type_label = node_type_label(node);
            out.push_str(&format!(
                "{} ({}) — no relationships\n",
                style_bold().apply_to(&label),
                Style::new().dim().apply_to(type_label),
            ));
        }
    }
}

fn format_edge_detail(edge: &CdfEdge) -> Option<String> {
    match edge {
        CdfEdge::Portal { state, status, .. } => {
            Some(format!("state={}, status={}", state, status))
        }
        CdfEdge::Replication {
            source_path,
            target_path,
            state,
            job_state,
            recovery_point,
            error_from_last_job,
            ..
        } => {
            let mut parts = Vec::new();
            if let (Some(sp), Some(tp)) = (source_path.as_deref(), target_path.as_deref()) {
                parts.push(format!("{} → {}", sp, tp));
            }
            if let Some(s) = state.as_deref() {
                parts.push(format!("state={}", s));
            }
            if let Some(js) = job_state.as_deref() {
                parts.push(format!("job={}", js));
            }
            if let Some(rp) = recovery_point.as_deref() {
                parts.push(format!("rp={}", rp));
            }
            if let Some(err) = error_from_last_job.as_deref() {
                parts.push(format!("error={}", err));
            }
            if parts.is_empty() {
                None
            } else {
                Some(parts.join(", "))
            }
        }
        CdfEdge::ObjectReplication {
            bucket,
            folder,
            state,
            ..
        } => {
            let mut parts = Vec::new();
            if let Some(b) = bucket.as_deref() {
                parts.push(format!("bucket={}", b));
            }
            if let Some(f) = folder.as_deref() {
                parts.push(format!("folder={}", f));
            }
            if let Some(s) = state.as_deref() {
                parts.push(format!("state={}", s));
            }
            if parts.is_empty() {
                None
            } else {
                Some(parts.join(", "))
            }
        }
    }
}

#[cfg(test)]
fn format_edge_label(edge: &CdfEdge, detail: bool) -> String {
    match edge {
        CdfEdge::Portal {
            portal_type,
            state,
            status,
            ..
        } => {
            let short_type = portal_type
                .strip_prefix("PORTAL_")
                .unwrap_or(portal_type)
                .to_lowercase()
                .replace('_', " ");
            if detail {
                format!("portal ({}) [state={}, status={}]", short_type, state, status)
            } else {
                format!("portal ({})", short_type)
            }
        }
        CdfEdge::Replication {
            source_path,
            target_path,
            mode,
            enabled,
            ..
        } => {
            let short_mode = mode
                .as_deref()
                .and_then(|m| m.strip_prefix("REPLICATION_"))
                .unwrap_or(mode.as_deref().unwrap_or("?"))
                .to_lowercase();
            if detail {
                format!(
                    "replication ({}) {}:{} → {}{}",
                    short_mode,
                    source_path.as_deref().unwrap_or("?"),
                    "",
                    target_path.as_deref().unwrap_or("?"),
                    if *enabled { "" } else { " [DISABLED]" }
                )
            } else {
                let mut label = format!("replication ({})", short_mode);
                if !enabled {
                    label.push_str(" [DISABLED]");
                }
                label
            }
        }
        CdfEdge::ObjectReplication {
            direction,
            bucket,
            folder,
            ..
        } => {
            let dir = direction.as_deref().unwrap_or("?");
            let short_dir = match dir {
                "COPY_TO_OBJECT" => "copy-to",
                "COPY_FROM_OBJECT" => "copy-from",
                other => other,
            };
            if detail {
                format!(
                    "S3 {} bucket={} folder={}",
                    short_dir,
                    bucket.as_deref().unwrap_or("?"),
                    folder.as_deref().unwrap_or("/"),
                )
            } else {
                format!("S3 {}", short_dir)
            }
        }
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
                replication_job_status: None,
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

    fn strip_ansi(s: &str) -> String {
        console::strip_ansi_codes(s).to_string()
    }

    #[test]
    fn test_render_empty_graph() {
        let graph = CdfGraph::new();
        let output = render(&graph, false);
        assert_eq!(output, "(no CDF relationships found)\n");
    }

    #[test]
    fn test_render_single_node() {
        let mut graph = CdfGraph::new();
        graph.add_node(CdfNode::ProfiledCluster {
            name: "lonely".into(),
            uuid: "uuid-1234".into(),
            address: "10.0.0.1".into(),
        });
        let output = render(&graph, false);
        let plain = strip_ansi(&output);
        assert!(plain.contains("lonely"));
        assert!(plain.contains("Data Fabric Topology"));
        assert!(plain.contains("1 clusters, 0 relationships"));
    }

    #[test]
    fn test_render_compact_mode() {
        let graph = make_test_graph();
        let output = render(&graph, false);
        let plain = strip_ansi(&output);

        // Verify header
        assert!(plain.contains("Data Fabric Topology"));
        assert!(plain.contains("3 clusters, 3 relationships"));

        // Verify source cluster names appear as headers
        assert!(plain.contains("gravytrain (cluster)"));
        assert!(plain.contains("iss (cluster)"));

        // Verify edge labels with target names
        assert!(plain.contains("→ iss:"));
        assert!(plain.contains("replication (continuous)"));
        assert!(plain.contains("portal (read write)"));
        assert!(plain.contains("→ S3:backup-bucket:"));
        assert!(plain.contains("S3 (copy-to)"));

        // Should NOT contain detail info in compact mode
        assert!(!plain.contains("/data"));
        assert!(!plain.contains("state=ACCEPTED"));
    }

    #[test]
    fn test_render_detail_mode() {
        let graph = make_test_graph();
        let output = render(&graph, true);
        let plain = strip_ansi(&output);

        // Detail mode should include paths, states, and target names
        assert!(plain.contains("replication (continuous)"));
        assert!(plain.contains("portal (read write)"));
        assert!(plain.contains("→ iss:"));
        // Detail edge info
        assert!(plain.contains("state=ACCEPTED"));
        assert!(plain.contains("status=ACTIVE"));
    }

    #[test]
    fn test_render_unknown_cluster() {
        let mut graph = CdfGraph::new();
        let n1 = graph.add_node(CdfNode::ProfiledCluster {
            name: "known".into(),
            uuid: "uuid-aaaa".into(),
            address: "10.0.0.1".into(),
        });
        let n2 = graph.add_node(CdfNode::UnknownCluster {
            address: "10.0.0.99".into(),
            uuid: Some("uuid-unknown".into()),
        });
        graph.add_edge(
            n1,
            n2,
            CdfEdge::Replication {
                source_path: Some("/src".into()),
                target_path: Some("/dst".into()),
                mode: Some("REPLICATION_SNAPSHOT".into()),
                enabled: true,
                state: None,
                job_state: None,
                recovery_point: None,
                error_from_last_job: None,
                replication_job_status: None,
            },
        );
        let output = render(&graph, false);
        let plain = strip_ansi(&output);
        assert!(plain.contains("10.0.0.99 (unknown)"));
        assert!(plain.contains("→ 10.0.0.99 (unknown):"));
        assert!(plain.contains("replication (snapshot)"));
    }

    #[test]
    fn test_render_disabled_replication() {
        let mut graph = CdfGraph::new();
        let n1 = graph.add_node(CdfNode::ProfiledCluster {
            name: "src".into(),
            uuid: "uuid-1".into(),
            address: "10.0.0.1".into(),
        });
        let n2 = graph.add_node(CdfNode::ProfiledCluster {
            name: "dst".into(),
            uuid: "uuid-2".into(),
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
        let output = render(&graph, false);
        let plain = strip_ansi(&output);
        assert!(plain.contains("[DISABLED]"));
    }

    #[test]
    fn test_render_s3_only() {
        let mut graph = CdfGraph::new();
        let n1 = graph.add_node(CdfNode::ProfiledCluster {
            name: "cluster-a".into(),
            uuid: "uuid-a".into(),
            address: "10.0.0.1".into(),
        });
        let n2 = graph.add_node(CdfNode::S3Bucket {
            address: "s3.amazonaws.com".into(),
            bucket: "my-bucket".into(),
            region: Some("us-east-1".into()),
        });
        graph.add_edge(
            n1,
            n2,
            CdfEdge::ObjectReplication {
                direction: Some("COPY_TO_OBJECT".into()),
                bucket: Some("my-bucket".into()),
                folder: Some("backups/".into()),
                state: None,
            },
        );
        let output = render(&graph, false);
        let plain = strip_ansi(&output);
        assert!(plain.contains("cluster-a"));
        assert!(plain.contains("S3:my-bucket"));
        assert!(plain.contains("S3 (copy-to)"));
    }

    #[test]
    fn test_render_output_valid_utf8() {
        let graph = make_test_graph();
        let output = render(&graph, false);
        // If we got here, it's valid UTF-8 (Rust strings are always valid UTF-8)
        assert!(!output.is_empty());

        // Verify tree-drawing characters are present
        let plain = strip_ansi(&output);
        let has_tree_chars = plain.contains('├') || plain.contains('└') || plain.contains('│');
        assert!(
            has_tree_chars,
            "Output should contain tree-drawing characters"
        );
    }

    #[test]
    fn test_render_all_unknown_clusters() {
        let mut graph = CdfGraph::new();
        let n1 = graph.add_node(CdfNode::UnknownCluster {
            address: "10.0.0.1".into(),
            uuid: None,
        });
        let n2 = graph.add_node(CdfNode::UnknownCluster {
            address: "10.0.0.2".into(),
            uuid: None,
        });
        graph.add_edge(
            n1,
            n2,
            CdfEdge::Replication {
                source_path: None,
                target_path: None,
                mode: None,
                enabled: true,
                state: None,
                job_state: None,
                recovery_point: None,
                error_from_last_job: None,
                replication_job_status: None,
            },
        );
        let output = render(&graph, false);
        let plain = strip_ansi(&output);
        assert!(plain.contains("10.0.0.1 (unknown)"));
        assert!(plain.contains("10.0.0.2 (unknown)"));
    }

    #[test]
    fn test_node_label() {
        assert_eq!(
            node_label(&CdfNode::ProfiledCluster {
                name: "test".into(),
                uuid: "u".into(),
                address: "a".into(),
            }),
            "test"
        );
        assert_eq!(
            node_label(&CdfNode::UnknownCluster {
                address: "10.0.0.1".into(),
                uuid: None,
            }),
            "10.0.0.1 (unknown)"
        );
        assert_eq!(
            node_label(&CdfNode::UnknownCluster {
                address: String::new(),
                uuid: None,
            }),
            "unknown"
        );
        assert_eq!(
            node_label(&CdfNode::S3Bucket {
                address: "s3.aws.com".into(),
                bucket: "bkt".into(),
                region: None,
            }),
            "S3:bkt"
        );
    }

    #[test]
    fn test_edge_label_compact() {
        let portal = CdfEdge::Portal {
            hub_id: 1,
            spoke_id: 2,
            portal_type: "PORTAL_READ_WRITE".into(),
            state: "ACCEPTED".into(),
            status: "ACTIVE".into(),
        };
        assert_eq!(format_edge_label(&portal, false), "portal (read write)");

        let repl = CdfEdge::Replication {
            source_path: Some("/a".into()),
            target_path: Some("/b".into()),
            mode: Some("REPLICATION_CONTINUOUS".into()),
            enabled: true,
            state: None,
            job_state: None,
            recovery_point: None,
            error_from_last_job: None,
            replication_job_status: None,
        };
        assert_eq!(format_edge_label(&repl, false), "replication (continuous)");

        let obj = CdfEdge::ObjectReplication {
            direction: Some("COPY_TO_OBJECT".into()),
            bucket: Some("bkt".into()),
            folder: Some("f/".into()),
            state: None,
        };
        assert_eq!(format_edge_label(&obj, false), "S3 copy-to");
    }

    #[test]
    fn test_edge_label_detail() {
        let portal = CdfEdge::Portal {
            hub_id: 1,
            spoke_id: 2,
            portal_type: "PORTAL_READ_ONLY".into(),
            state: "PENDING".into(),
            status: "INACTIVE".into(),
        };
        let label = format_edge_label(&portal, true);
        assert!(label.contains("state=PENDING"));
        assert!(label.contains("status=INACTIVE"));

        let obj = CdfEdge::ObjectReplication {
            direction: Some("COPY_FROM_OBJECT".into()),
            bucket: Some("my-bkt".into()),
            folder: Some("data/".into()),
            state: None,
        };
        let label = format_edge_label(&obj, true);
        assert!(label.contains("copy-from"));
        assert!(label.contains("bucket=my-bkt"));
        assert!(label.contains("folder=data/"));
    }

    #[test]
    fn test_edges_show_target_node_name() {
        let mut graph = CdfGraph::new();
        let n1 = graph.add_node(CdfNode::ProfiledCluster {
            name: "source".into(),
            uuid: "uuid-1".into(),
            address: "10.0.0.1".into(),
        });
        let n2 = graph.add_node(CdfNode::ProfiledCluster {
            name: "target".into(),
            uuid: "uuid-2".into(),
            address: "10.0.0.2".into(),
        });
        graph.add_edge(
            n1,
            n2,
            CdfEdge::Replication {
                source_path: None,
                target_path: None,
                mode: Some("REPLICATION_CONTINUOUS".into()),
                enabled: true,
                state: None,
                job_state: None,
                recovery_point: None,
                error_from_last_job: None,
                replication_job_status: None,
            },
        );
        let output = render(&graph, false);
        let plain = strip_ansi(&output);
        // Edge label must include target name
        assert!(
            plain.contains("→ target:"),
            "Edge label should include target node name, got:\n{}",
            plain
        );
        assert!(plain.contains("replication (continuous)"));
    }

    #[test]
    fn test_duplicate_edges_collapsed() {
        let mut graph = CdfGraph::new();
        let n1 = graph.add_node(CdfNode::ProfiledCluster {
            name: "gravytrain-sg".into(),
            uuid: "uuid-1".into(),
            address: "10.0.0.1".into(),
        });
        let n2 = graph.add_node(CdfNode::ProfiledCluster {
            name: "iss-sg".into(),
            uuid: "uuid-2".into(),
            address: "10.0.0.2".into(),
        });

        // Add 3 identical replication edges
        for _ in 0..3 {
            graph.add_edge(
                n1,
                n2,
                CdfEdge::Replication {
                    source_path: None,
                    target_path: None,
                    mode: Some("REPLICATION_CONTINUOUS".into()),
                    enabled: true,
                    state: None,
                    job_state: None,
                    recovery_point: None,
                    error_from_last_job: None,
                    replication_job_status: None,
                },
            );
        }

        // Add 1 portal
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

        let output = render(&graph, false);
        let plain = strip_ansi(&output);

        // Should show "x3" for collapsed edges
        assert!(
            plain.contains("x3"),
            "Should collapse 3 duplicate edges with x3, got:\n{}",
            plain
        );
        // Should show portal without count (only 1)
        assert!(plain.contains("portal (read write)"));
        assert!(!plain.contains("portal (read write) x"));
    }

    #[test]
    fn test_s3_label_abbreviated() {
        let node = CdfNode::S3Bucket {
            address: "s3-us-west-2.amazonaws.com".into(),
            bucket: "speqtrum-demo".into(),
            region: Some("us-west-2".into()),
        };
        // Short label should be "S3:speqtrum-demo", not "S3: speqtrum-demo @ s3-us-west-2.amazonaws.com"
        assert_eq!(node_label(&node), "S3:speqtrum-demo");
        // Full label preserves address
        assert_eq!(
            node_label_full(&node),
            "S3: speqtrum-demo @ s3-us-west-2.amazonaws.com"
        );
    }

    #[test]
    fn test_output_width_under_120() {
        let graph = make_test_graph();
        let output = render(&graph, false);
        let plain = strip_ansi(&output);
        for line in plain.lines() {
            let width = console::measure_text_width(line);
            assert!(
                width <= 120,
                "Line exceeds 120 display cols ({} cols): {}",
                width,
                line
            );
        }
    }
}
