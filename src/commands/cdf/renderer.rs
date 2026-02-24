use console::Style;
use petgraph::visit::EdgeRef;

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

    let layout = compute_layout(graph);
    render_graph(&mut out, graph, &layout, detail);
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

fn style_unknown() -> Style {
    Style::new().dim()
}

fn style_s3() -> Style {
    Style::new().yellow()
}

// ── Layout computation ──────────────────────────────────────────────────────

/// Assigned position for a node in the layered layout.
struct NodeLayout {
    layer: usize,
    pos: usize, // position within layer (0-based)
}

/// Full layout result.
struct Layout {
    nodes: Vec<NodeLayout>, // indexed by petgraph NodeIndex
    num_layers: usize,
    _max_per_layer: usize,
}

fn compute_layout(graph: &CdfGraph) -> Layout {
    let node_count = graph.node_count();
    if node_count == 0 {
        return Layout {
            nodes: vec![],
            num_layers: 0,
            _max_per_layer: 0,
        };
    }

    // Build edge list for rust-sugiyama (u32 pairs)
    let edges: Vec<(u32, u32)> = graph
        .edge_references()
        .map(|e| (e.source().index() as u32, e.target().index() as u32))
        .collect();

    let config = rust_sugiyama::configure::Config::default();

    // rust-sugiyama returns Vec<(Vec<(usize, (f64, f64))>, f64, f64)>
    // Each element is a connected component: (node_coords, width, height)
    let components = if edges.is_empty() {
        // No edges — each node is its own component, place in single layer
        vec![]
    } else {
        rust_sugiyama::from_edges(&edges, &config)
    };

    // Build a map from node index -> (x, y) coordinates
    let mut coord_map: Vec<Option<(f64, f64)>> = vec![None; node_count];

    // Offset components horizontally so they don't overlap
    let mut x_offset = 0.0_f64;
    for (coords, width, _height) in &components {
        for &(node_idx, (x, y)) in coords {
            if node_idx < node_count {
                coord_map[node_idx] = Some((x + x_offset, y));
            }
        }
        x_offset += width + 2.0; // gap between components
    }

    // Assign any unplaced nodes (isolated nodes not in edges)
    let mut isolated_x = x_offset;
    for i in 0..node_count {
        if coord_map[i].is_none() {
            coord_map[i] = Some((isolated_x, 0.0));
            isolated_x += 1.0;
        }
    }

    // Convert floating coordinates to discrete layers.
    // Y-axis = layer (top to bottom), X-axis = position within layer.
    // Quantize Y values to layer indices.
    let mut y_values: Vec<f64> = coord_map
        .iter()
        .filter_map(|c| c.map(|(_, y)| y))
        .collect();
    y_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    y_values.dedup_by(|a, b| (*a - *b).abs() < 0.5);

    let num_layers = y_values.len().max(1);

    let mut nodes = Vec::with_capacity(node_count);
    let mut layer_counts = vec![0usize; num_layers];

    // First pass: assign layers
    let mut layer_assignments: Vec<usize> = Vec::with_capacity(node_count);
    for i in 0..node_count {
        let (_, y) = coord_map[i].unwrap_or((0.0, 0.0));
        let layer = y_values
            .iter()
            .position(|&yv| (yv - y).abs() < 0.5)
            .unwrap_or(0);
        layer_assignments.push(layer);
    }

    // Collect nodes per layer and sort by x within each layer
    let mut layer_nodes: Vec<Vec<(usize, f64)>> = vec![vec![]; num_layers];
    for i in 0..node_count {
        let (x, _) = coord_map[i].unwrap_or((0.0, 0.0));
        layer_nodes[layer_assignments[i]].push((i, x));
    }
    for layer in &mut layer_nodes {
        layer.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    }

    // Assign positions
    let mut node_positions = vec![(0usize, 0usize); node_count]; // (layer, pos)
    for (layer_idx, layer) in layer_nodes.iter().enumerate() {
        for (pos, &(node_idx, _)) in layer.iter().enumerate() {
            node_positions[node_idx] = (layer_idx, pos);
            layer_counts[layer_idx] = pos + 1;
        }
    }

    let max_per_layer = layer_counts.iter().copied().max().unwrap_or(1);

    for i in 0..node_count {
        let (layer, pos) = node_positions[i];
        nodes.push(NodeLayout { layer, pos });
    }

    Layout {
        nodes,
        num_layers,
        _max_per_layer: max_per_layer,
    }
}

// ── Rendering ───────────────────────────────────────────────────────────────

const NODE_MIN_WIDTH: usize = 20;
const NODE_SPACING: usize = 6;

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
        out.push_str(&format!("  {}\n", format_node_box(node, false)));
    }
}

fn render_graph(out: &mut String, graph: &CdfGraph, layout: &Layout, detail: bool) {
    // Collect nodes per layer
    let mut layers: Vec<Vec<petgraph::graph::NodeIndex>> = vec![vec![]; layout.num_layers];
    for idx in graph.node_indices() {
        let nl = &layout.nodes[idx.index()];
        layers[nl.layer].push(idx);
    }

    // Sort nodes within each layer by position
    for layer in &mut layers {
        layer.sort_by_key(|idx| layout.nodes[idx.index()].pos);
    }

    // Calculate the width each node needs
    let node_widths: Vec<usize> = graph
        .node_indices()
        .map(|idx| node_display_width(&graph[idx], detail))
        .collect();

    // Render layer by layer
    for (layer_idx, layer_nodes) in layers.iter().enumerate() {
        // Render node boxes for this layer
        render_layer_nodes(out, graph, layer_nodes, &node_widths, detail);

        // Render edges from this layer to next layers
        if layer_idx < layout.num_layers - 1 {
            render_layer_edges(out, graph, layout, layer_nodes, &node_widths, detail);
        }
    }
}

fn node_display_width(node: &CdfNode, _detail: bool) -> usize {
    let label = node_label(node);
    // Box adds 4 chars (│ + space on each side + │), min width
    (label.len() + 4).max(NODE_MIN_WIDTH)
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

fn format_node_box(node: &CdfNode, detail: bool) -> String {
    let label = node_label(node);
    let type_label = node_type_label(node);
    let inner_width = label.len().max(type_label.len()).max(NODE_MIN_WIDTH - 4);

    let (top, bot, side) = match node {
        CdfNode::ProfiledCluster { .. } => ("─", "─", "│"),
        CdfNode::UnknownCluster { .. } => ("╌", "╌", "┊"),
        CdfNode::S3Bucket { .. } => ("─", "─", "│"),
    };

    let mut lines = Vec::new();

    // Top border
    match node {
        CdfNode::S3Bucket { .. } => {
            lines.push(format!("〔{}〕", top.repeat(inner_width + 2)));
        }
        _ => {
            lines.push(format!("┌{}┐", top.repeat(inner_width + 2)));
        }
    }

    // Label line
    let padded = format!("{:^width$}", label, width = inner_width);
    lines.push(format!("{} {} {}", side, padded, side));

    // Type line
    let type_padded = format!("{:^width$}", type_label, width = inner_width);
    lines.push(format!(
        "{} {} {}",
        side,
        Style::new().dim().apply_to(type_padded),
        side
    ));

    // Detail lines
    if detail {
        if let Some(detail_line) = node_detail_line(node) {
            let det_padded = format!("{:^width$}", detail_line, width = inner_width);
            lines.push(format!("{} {} {}", side, det_padded, side));
        }
    }

    // Bottom border
    match node {
        CdfNode::S3Bucket { .. } => {
            lines.push(format!("〔{}〕", bot.repeat(inner_width + 2)));
        }
        _ => {
            lines.push(format!("└{}┘", bot.repeat(inner_width + 2)));
        }
    }

    lines.join("\n")
}

fn node_detail_line(node: &CdfNode) -> Option<String> {
    match node {
        CdfNode::ProfiledCluster { address, uuid, .. } => {
            Some(format!("{} ({})", address, &uuid[..8.min(uuid.len())]))
        }
        CdfNode::UnknownCluster { uuid, .. } => {
            uuid.as_ref().map(|u| format!("uuid: {}", &u[..8.min(u.len())]))
        }
        CdfNode::S3Bucket { region, .. } => {
            region.as_ref().map(|r| format!("region: {}", r))
        }
    }
}

fn render_layer_nodes(
    out: &mut String,
    graph: &CdfGraph,
    layer_nodes: &[petgraph::graph::NodeIndex],
    node_widths: &[usize],
    detail: bool,
) {
    if layer_nodes.is_empty() {
        return;
    }

    // Build multi-line boxes for each node
    let boxes: Vec<Vec<String>> = layer_nodes
        .iter()
        .map(|&idx| {
            let node = &graph[idx];
            let box_str = format_node_box(node, detail);
            box_str.lines().map(String::from).collect()
        })
        .collect();

    // Find max height across all boxes in this layer
    let max_height = boxes.iter().map(|b| b.len()).max().unwrap_or(0);

    // Render line by line, placing boxes side by side
    for line_idx in 0..max_height {
        let mut line = String::from("  "); // left margin
        for (i, (box_lines, &node_idx)) in boxes.iter().zip(layer_nodes.iter()).enumerate() {
            let w = node_widths[node_idx.index()];
            if line_idx < box_lines.len() {
                let raw = &box_lines[line_idx];
                // Apply node-specific styling
                let styled = style_node_line(&graph[node_idx], raw);
                line.push_str(&styled);
                // Pad to fixed width (accounting for ANSI codes in styled text)
                let visible_len = console::measure_text_width(raw);
                if visible_len < w {
                    line.push_str(&" ".repeat(w - visible_len));
                }
            } else {
                line.push_str(&" ".repeat(w));
            }
            if i < layer_nodes.len() - 1 {
                line.push_str(&" ".repeat(NODE_SPACING));
            }
        }
        out.push_str(&line);
        out.push('\n');
    }
}

fn style_node_line(node: &CdfNode, line: &str) -> String {
    match node {
        CdfNode::ProfiledCluster { .. } => {
            // Bold the label line (contains the name), dim the type line
            line.to_string()
        }
        CdfNode::UnknownCluster { .. } => {
            style_unknown().apply_to(line).to_string()
        }
        CdfNode::S3Bucket { .. } => {
            style_s3().apply_to(line).to_string()
        }
    }
}

fn render_layer_edges(
    out: &mut String,
    graph: &CdfGraph,
    layout: &Layout,
    layer_nodes: &[petgraph::graph::NodeIndex],
    node_widths: &[usize],
    detail: bool,
) {
    // Collect edges from this layer's nodes to lower layers
    let mut edges_to_render: Vec<(petgraph::graph::NodeIndex, petgraph::graph::NodeIndex, &CdfEdge)> = Vec::new();

    for &src_idx in layer_nodes {
        for edge in graph.edges(src_idx) {
            let tgt_idx = edge.target();
            if layout.nodes[tgt_idx.index()].layer > layout.nodes[src_idx.index()].layer {
                edges_to_render.push((src_idx, tgt_idx, edge.weight()));
            }
        }
    }

    // Also check incoming edges (for edges going upward in the layout — reversed by sugiyama)
    for &src_idx in layer_nodes {
        for edge in graph.edges_directed(src_idx, petgraph::Direction::Incoming) {
            let from_idx = edge.source();
            if layout.nodes[from_idx.index()].layer > layout.nodes[src_idx.index()].layer {
                // This edge goes from a lower layer to us — render it here too
                edges_to_render.push((src_idx, from_idx, edge.weight()));
            }
        }
    }

    if edges_to_render.is_empty() {
        out.push('\n');
        return;
    }

    // Calculate center positions of each node in character columns
    let node_centers: Vec<(petgraph::graph::NodeIndex, usize)> = {
        let mut centers = Vec::new();
        let mut col = 2; // left margin
        for (i, &node_idx) in layer_nodes.iter().enumerate() {
            let w = node_widths[node_idx.index()];
            centers.push((node_idx, col + w / 2));
            col += w;
            if i < layer_nodes.len() - 1 {
                col += NODE_SPACING;
            }
        }
        centers
    };

    // Render each edge as a labeled connection line
    for (src_idx, _tgt_idx, edge) in &edges_to_render {
        let src_center = node_centers
            .iter()
            .find(|(idx, _)| idx == src_idx)
            .map(|(_, c)| *c)
            .unwrap_or(2);

        let edge_label = format_edge_label(edge, detail);
        let styled_label = style_edge_label(edge, &edge_label);

        // Draw: vertical connector from source, then arrow and label
        let connector = " ".repeat(src_center) + "│";
        out.push_str(&style_edge_connector(edge, &connector));
        out.push('\n');

        let arrow_line = " ".repeat(src_center) + "├── ";
        out.push_str(&style_edge_connector(edge, &arrow_line));
        out.push_str(&styled_label);
        out.push('\n');

        let down_arrow = " ".repeat(src_center) + "▼";
        out.push_str(&style_edge_connector(edge, &down_arrow));
        out.push('\n');
    }
}

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

fn style_edge_label(edge: &CdfEdge, label: &str) -> String {
    match edge {
        CdfEdge::Portal { .. } => style_portal().apply_to(label).to_string(),
        CdfEdge::Replication { .. } => style_replication().apply_to(label).to_string(),
        CdfEdge::ObjectReplication { .. } => style_object().apply_to(label).to_string(),
    }
}

fn style_edge_connector(edge: &CdfEdge, text: &str) -> String {
    let style = match edge {
        CdfEdge::Portal { .. } => style_portal(),
        CdfEdge::Replication { .. } => style_replication(),
        CdfEdge::ObjectReplication { .. } => style_object(),
    };
    style.apply_to(text).to_string()
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
            },
        );
        graph
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
        assert!(output.contains("lonely"));
        assert!(output.contains("Data Fabric Topology"));
        assert!(output.contains("1 clusters, 0 relationships"));
    }

    #[test]
    fn test_render_compact_mode() {
        let graph = make_test_graph();
        let output = render(&graph, false);

        // Verify header
        assert!(output.contains("Data Fabric Topology"));
        assert!(output.contains("3 clusters, 3 relationships"));

        // Verify node names appear
        assert!(output.contains("gravytrain"));
        assert!(output.contains("iss"));
        assert!(output.contains("backup-bucket"));

        // Verify edge labels appear (compact)
        assert!(output.contains("replication (continuous)"));
        assert!(output.contains("portal (read write)"));
        assert!(output.contains("S3 copy-to"));

        // Should NOT contain detail info in compact mode
        assert!(!output.contains("/data"));
        assert!(!output.contains("state=ACCEPTED"));
    }

    #[test]
    fn test_render_detail_mode() {
        let graph = make_test_graph();
        let output = render(&graph, true);

        // Detail mode should include paths and states
        assert!(output.contains("replication (continuous)"));
        assert!(output.contains("portal (read write)"));
        // Detail edge info
        assert!(output.contains("state=ACCEPTED"));
        assert!(output.contains("status=ACTIVE"));
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
            },
        );
        let output = render(&graph, false);
        assert!(output.contains("10.0.0.99 (unknown)"));
        assert!(output.contains("replication (snapshot)"));
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
            },
        );
        let output = render(&graph, false);
        assert!(output.contains("[DISABLED]"));
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
            },
        );
        let output = render(&graph, false);
        assert!(output.contains("cluster-a"));
        assert!(output.contains("my-bucket"));
        assert!(output.contains("S3 copy-to"));
    }

    #[test]
    fn test_render_output_valid_utf8() {
        let graph = make_test_graph();
        let output = render(&graph, false);
        // If we got here, it's valid UTF-8 (Rust strings are always valid UTF-8)
        assert!(!output.is_empty());

        // Verify box-drawing characters are present
        let has_box_chars = output.contains('┌')
            || output.contains('└')
            || output.contains('│')
            || output.contains('─');
        assert!(has_box_chars, "Output should contain box-drawing characters");
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
            },
        );
        let output = render(&graph, false);
        assert!(output.contains("10.0.0.1 (unknown)"));
        assert!(output.contains("10.0.0.2 (unknown)"));
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
            "S3: bkt @ s3.aws.com"
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
        };
        assert_eq!(format_edge_label(&repl, false), "replication (continuous)");

        let obj = CdfEdge::ObjectReplication {
            direction: Some("COPY_TO_OBJECT".into()),
            bucket: Some("bkt".into()),
            folder: Some("f/".into()),
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
        };
        let label = format_edge_label(&obj, true);
        assert!(label.contains("copy-from"));
        assert!(label.contains("bucket=my-bkt"));
        assert!(label.contains("folder=data/"));
    }

    #[test]
    fn test_compute_layout_empty() {
        let graph = CdfGraph::new();
        let layout = compute_layout(&graph);
        assert_eq!(layout.num_layers, 0);
    }

    #[test]
    fn test_compute_layout_linear() {
        let mut graph = CdfGraph::new();
        let n1 = graph.add_node(CdfNode::ProfiledCluster {
            name: "a".into(),
            uuid: "u1".into(),
            address: "1".into(),
        });
        let n2 = graph.add_node(CdfNode::ProfiledCluster {
            name: "b".into(),
            uuid: "u2".into(),
            address: "2".into(),
        });
        graph.add_edge(
            n1,
            n2,
            CdfEdge::Replication {
                source_path: None,
                target_path: None,
                mode: None,
                enabled: true,
            },
        );
        let layout = compute_layout(&graph);
        assert!(layout.num_layers >= 2);
        // Source should be in an earlier (or equal) layer than target
        assert!(layout.nodes[0].layer <= layout.nodes[1].layer);
    }
}
