use ascii_dag::graph::DAG;
use petgraph::visit::EdgeRef;
use std::collections::HashSet;

use super::types::*;

/// Render the CDF graph using the ascii-dag crate.
///
/// Returns a plain-text ASCII DAG visualization.
/// The CDF graph may contain cycles (bidirectional replication), so we break
/// them by dropping reverse edges and annotating the forward edge as "⇄".
pub fn render(graph: &CdfGraph, detail: bool) -> String {
    if graph.node_count() == 0 {
        return "(no CDF relationships found)\n".to_string();
    }

    // Build node labels
    let node_labels: Vec<String> = graph
        .node_indices()
        .map(|idx| node_label(&graph[idx]))
        .collect();

    // Build edge list, breaking cycles by dropping back-edges.
    // If we see A→B and later B→A, we drop B→A and mark A→B as bidirectional.
    let mut seen_pairs: HashSet<(usize, usize)> = HashSet::new();
    let mut edges: Vec<(usize, usize, String)> = Vec::new();

    for e in graph.edge_references() {
        let from = e.source().index();
        let to = e.target().index();
        let label = edge_label(e.weight(), detail);

        if seen_pairs.contains(&(to, from)) {
            // Back-edge: find the forward edge and mark it bidirectional
            if let Some(fwd) = edges.iter_mut().find(|(f, t, _)| *f == to && *t == from) {
                if !fwd.2.contains('⇄') {
                    fwd.2 = format!("{} ⇄ {}", fwd.2, label);
                }
            }
        } else {
            seen_pairs.insert((from, to));
            edges.push((from, to, label));
        }
    }

    // Construct ascii-dag DAG
    let nodes: Vec<(usize, &str)> = node_labels
        .iter()
        .enumerate()
        .map(|(i, label)| (i, label.as_str()))
        .collect();

    let edge_refs: Vec<(usize, usize, Option<&str>)> = edges
        .iter()
        .map(|(from, to, label)| (*from, *to, Some(label.as_str())))
        .collect();

    let dag = DAG::from_edges_labeled(&nodes, &edge_refs);

    let mut out = String::new();

    // Header
    out.push_str(&format!(
        "=== Data Fabric Topology ({} nodes, {} edges) ===\n\n",
        graph.node_count(),
        graph.edge_count()
    ));

    out.push_str(&dag.render());
    out.push('\n');

    out
}

fn node_label(node: &CdfNode) -> String {
    match node {
        CdfNode::ProfiledCluster { name, address, .. } => {
            format!("{} ({})", name, address)
        }
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

fn edge_label(edge: &CdfEdge, detail: bool) -> String {
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
                format!("portal ({}) [{}/{}]", short_type, state, status)
            } else {
                format!("portal ({})", short_type)
            }
        }
        CdfEdge::Replication {
            mode,
            enabled,
            source_path,
            target_path,
            ..
        } => {
            let short_mode = mode
                .as_deref()
                .and_then(|m| m.strip_prefix("REPLICATION_"))
                .unwrap_or(mode.as_deref().unwrap_or("?"))
                .to_lowercase();
            if detail {
                format!(
                    "repl ({}) {}->{}{}",
                    short_mode,
                    source_path.as_deref().unwrap_or("?"),
                    target_path.as_deref().unwrap_or("?"),
                    if *enabled { "" } else { " [OFF]" }
                )
            } else {
                let mut label = format!("repl ({})", short_mode);
                if !enabled {
                    label.push_str(" [OFF]");
                }
                label
            }
        }
        CdfEdge::ObjectReplication {
            direction, bucket, ..
        } => {
            let dir = match direction.as_deref() {
                Some("COPY_TO_OBJECT") => "to",
                Some("COPY_FROM_OBJECT") => "from",
                Some(other) => other,
                None => "?",
            };
            if detail {
                format!(
                    "S3 {} ({})",
                    dir,
                    bucket.as_deref().unwrap_or("?")
                )
            } else {
                format!("S3 {}", dir)
            }
        }
    }
}

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
            address: "s3-us-west-2.amazonaws.com".into(),
            bucket: "speqtrum-demo".into(),
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
                bucket: Some("speqtrum-demo".into()),
                folder: Some("daily/".into()),
                state: Some("ACTIVE".into()),
            },
        );
        graph
    }

    #[test]
    fn test_render_empty() {
        let graph = CdfGraph::new();
        let output = render(&graph, false);
        assert_eq!(output, "(no CDF relationships found)\n");
    }

    #[test]
    fn test_render_basic() {
        let graph = make_test_graph();
        let output = render(&graph, false);
        assert!(output.contains("Data Fabric Topology"));
        assert!(output.contains("3 nodes"));
        assert!(output.contains("3 edges"));
        // Node labels should appear
        assert!(output.contains("gravytrain"));
        assert!(output.contains("iss"));
        assert!(output.contains("speqtrum-demo"));
    }

    #[test]
    fn test_render_detail() {
        let graph = make_test_graph();
        let output = render(&graph, true);
        // Detail mode should include paths
        assert!(output.contains("/data"));
        assert!(output.contains("/replica"));
    }

    #[test]
    fn test_render_long_s3_label() {
        // Key concern from the spike: how does it handle long labels?
        let mut graph = CdfGraph::new();
        let n1 = graph.add_node(CdfNode::ProfiledCluster {
            name: "cluster-a".into(),
            uuid: "uuid-a".into(),
            address: "10.0.0.1".into(),
        });
        let n2 = graph.add_node(CdfNode::S3Bucket {
            address: "s3-us-west-2.amazonaws.com".into(),
            bucket: "speqtrum-demo".into(),
            region: Some("us-west-2".into()),
        });
        graph.add_edge(
            n1,
            n2,
            CdfEdge::ObjectReplication {
                direction: Some("COPY_TO_OBJECT".into()),
                bucket: Some("speqtrum-demo".into()),
                folder: Some("backups/".into()),
                state: Some("ACTIVE".into()),
            },
        );
        let output = render(&graph, false);
        assert!(output.contains("S3: speqtrum-demo @ s3-us-west-2.amazonaws.com"));
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
        assert!(output.contains("lonely"));
        assert!(output.contains("1 nodes"));
    }
}
