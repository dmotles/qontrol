use console::Style;
use petgraph::visit::EdgeRef;
use std::collections::{BTreeMap, BTreeSet, HashMap};

use super::types::*;

// ── Public entry point ──────────────────────────────────────────────────────

/// Render the CDF graph as a visually striking cluster-centric topology map.
///
/// Returns a string with embedded ANSI codes (via `console` crate).
/// `detail` enables per-edge metadata like paths, modes, states.
pub fn render(graph: &CdfGraph, detail: bool) -> String {
    if graph.node_count() == 0 {
        return "(no CDF relationships found)\n".to_string();
    }

    let mut out = String::new();
    let topo = build_topology(graph);
    render_header(&mut out, &topo);

    if graph.node_count() == 1 && graph.edge_count() == 0 {
        render_single_node(&mut out, graph);
        return out;
    }

    render_cluster_connections(&mut out, &topo, detail);
    render_s3_section(&mut out, &topo);
    render_remote_peers_section(&mut out, &topo);
    render_legend(&mut out, &topo);

    out
}

// ── Topology model ──────────────────────────────────────────────────────────

/// Direction of edges within a cluster pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Direction {
    Forward,  // A → B
    Backward, // B → A
    Both,     // A ↔ B
}

/// A bundle of edges between the same undirected cluster pair.
#[derive(Debug)]
struct ClusterPairBundle {
    cluster_a: String, // alphabetically first
    cluster_b: String,
    /// Grouped by edge type: (type_tag, count, direction, disabled_count, representative edge)
    edge_groups: Vec<EdgeSummary>,
}

#[derive(Debug)]
struct EdgeSummary {
    type_tag: &'static str,
    count: usize,
    direction: Direction,
    disabled_count: usize,
}

/// Satellites (S3 or remote peer) grouped by owning cluster.
#[derive(Debug)]
struct SatelliteGroup {
    cluster_name: String,
    satellites: Vec<SatelliteEntry>,
}

#[derive(Debug)]
struct SatelliteEntry {
    label: String,
    direction: Direction,
    count: usize,
    disabled_count: usize,
}

/// Full topology extracted from the graph.
struct Topology {
    #[allow(dead_code)]
    profiled_clusters: BTreeSet<String>,
    cluster_pairs: Vec<ClusterPairBundle>,
    s3_groups: Vec<SatelliteGroup>,
    remote_peer_groups: Vec<SatelliteGroup>,
    isolated_clusters: Vec<String>,
    cluster_count: usize,
    s3_count: usize,
    remote_peer_count: usize,
    has_replication: bool,
    has_portal: bool,
    has_s3: bool,
    has_remote: bool,
}

fn build_topology(graph: &CdfGraph) -> Topology {
    let mut profiled_clusters = BTreeSet::new();
    let mut s3_nodes = BTreeSet::new();
    let mut unknown_nodes = BTreeSet::new();

    // Classify nodes
    let mut node_labels: HashMap<petgraph::graph::NodeIndex, String> = HashMap::new();
    for idx in graph.node_indices() {
        let node = &graph[idx];
        let label = node_label(node);
        node_labels.insert(idx, label.clone());
        match node {
            CdfNode::ProfiledCluster { .. } => {
                profiled_clusters.insert(label);
            }
            CdfNode::S3Bucket { .. } => {
                s3_nodes.insert(label);
            }
            CdfNode::UnknownCluster { .. } => {
                unknown_nodes.insert(label);
            }
        }
    }

    // Build cluster pair bundles (profiled ↔ profiled edges)
    // Key: (min_name, max_name) → Vec of (edge, direction_relative_to_pair)
    let mut pair_edges: BTreeMap<(String, String), Vec<(CdfEdge, Direction)>> = BTreeMap::new();
    let mut s3_edges: BTreeMap<String, Vec<(String, CdfEdge)>> = BTreeMap::new();
    let mut remote_edges: BTreeMap<String, Vec<(String, CdfEdge)>> = BTreeMap::new();
    let mut clusters_with_connections: BTreeSet<String> = BTreeSet::new();

    for idx in graph.node_indices() {
        let src_node = &graph[idx];
        let src_label = &node_labels[&idx];

        for edge_ref in graph.edges(idx) {
            let tgt_idx = edge_ref.target();
            let tgt_node = &graph[tgt_idx];
            let tgt_label = &node_labels[&tgt_idx];
            let edge = edge_ref.weight().clone();

            let src_is_profiled = matches!(src_node, CdfNode::ProfiledCluster { .. });
            let tgt_is_profiled = matches!(tgt_node, CdfNode::ProfiledCluster { .. });
            let tgt_is_s3 = matches!(tgt_node, CdfNode::S3Bucket { .. });
            let tgt_is_unknown = matches!(tgt_node, CdfNode::UnknownCluster { .. });
            let src_is_unknown = matches!(src_node, CdfNode::UnknownCluster { .. });

            if src_is_profiled && tgt_is_profiled {
                // Cluster-to-cluster edge
                clusters_with_connections.insert(src_label.clone());
                clusters_with_connections.insert(tgt_label.clone());

                let (a, b) = if src_label <= tgt_label {
                    (src_label.clone(), tgt_label.clone())
                } else {
                    (tgt_label.clone(), src_label.clone())
                };
                let dir = if src_label <= tgt_label {
                    Direction::Forward
                } else {
                    Direction::Backward
                };
                pair_edges.entry((a, b)).or_default().push((edge, dir));
            } else if src_is_profiled && tgt_is_s3 {
                clusters_with_connections.insert(src_label.clone());
                s3_edges
                    .entry(src_label.clone())
                    .or_default()
                    .push((tgt_label.clone(), edge));
            } else if src_is_profiled && tgt_is_unknown {
                clusters_with_connections.insert(src_label.clone());
                remote_edges
                    .entry(src_label.clone())
                    .or_default()
                    .push((tgt_label.clone(), edge));
            } else if src_is_unknown && tgt_is_profiled {
                clusters_with_connections.insert(tgt_label.clone());
                remote_edges
                    .entry(tgt_label.clone())
                    .or_default()
                    .push((src_label.clone(), edge));
            } else if src_is_unknown && tgt_is_unknown {
                // Unknown-to-unknown: group under "?"
                remote_edges
                    .entry("?".to_string())
                    .or_default()
                    .push((tgt_label.clone(), edge));
            } else {
                // S3 source or other combos - group under source
                if tgt_is_s3 || tgt_is_unknown {
                    remote_edges
                        .entry(src_label.clone())
                        .or_default()
                        .push((tgt_label.clone(), edge));
                }
            }
        }
    }

    // Build cluster pair bundles
    let mut cluster_pairs = Vec::new();
    let mut has_replication = false;
    let mut has_portal = false;

    for ((a, b), edges) in &pair_edges {
        let edge_groups = summarize_edge_groups(edges);
        for eg in &edge_groups {
            match eg.type_tag {
                "repl" => has_replication = true,
                "portal" => has_portal = true,
                _ => {}
            }
        }
        cluster_pairs.push(ClusterPairBundle {
            cluster_a: a.clone(),
            cluster_b: b.clone(),
            edge_groups,
        });
    }

    // Build S3 satellite groups
    let mut has_s3 = false;
    let mut s3_groups = Vec::new();
    let mut unique_s3: BTreeSet<String> = BTreeSet::new();
    for (cluster, edges) in &s3_edges {
        has_s3 = true;
        let mut entries = build_satellite_entries(edges);
        for e in &entries {
            unique_s3.insert(e.label.clone());
        }
        entries.sort_by(|a, b| a.label.cmp(&b.label));
        s3_groups.push(SatelliteGroup {
            cluster_name: cluster.clone(),
            satellites: entries,
        });
    }
    s3_groups.sort_by(|a, b| a.cluster_name.cmp(&b.cluster_name));

    // Build remote peer groups
    let mut has_remote = false;
    let mut remote_peer_groups = Vec::new();
    let mut unique_remotes: BTreeSet<String> = BTreeSet::new();
    for (cluster, edges) in &remote_edges {
        has_remote = true;
        let mut entries = build_satellite_entries(edges);
        for e in &entries {
            unique_remotes.insert(e.label.clone());
        }
        entries.sort_by(|a, b| a.label.cmp(&b.label));
        remote_peer_groups.push(SatelliteGroup {
            cluster_name: cluster.clone(),
            satellites: entries,
        });
    }
    remote_peer_groups.sort_by(|a, b| a.cluster_name.cmp(&b.cluster_name));

    // Isolated profiled clusters (no connections at all)
    let isolated: Vec<String> = profiled_clusters
        .iter()
        .filter(|c| !clusters_with_connections.contains(*c))
        .cloned()
        .collect();

    Topology {
        cluster_count: profiled_clusters.len(),
        s3_count: unique_s3.len(),
        remote_peer_count: unique_remotes.len(),
        profiled_clusters,
        cluster_pairs,
        s3_groups,
        remote_peer_groups,
        isolated_clusters: isolated,
        has_replication,
        has_portal,
        has_s3,
        has_remote,
    }
}

fn summarize_edge_groups(edges: &[(CdfEdge, Direction)]) -> Vec<EdgeSummary> {
    // Group by (type_tag, short_label)
    #[derive(Ord, PartialOrd, Eq, PartialEq, Clone)]
    struct GroupKey {
        type_tag: &'static str,
        short_label: String,
    }

    struct GroupAccum {
        count: usize,
        forward: usize,
        backward: usize,
        disabled: usize,
    }

    let mut groups: BTreeMap<GroupKey, GroupAccum> = BTreeMap::new();

    for (edge, dir) in edges {
        let (type_tag, short_label, disabled) = classify_edge(edge);
        let key = GroupKey {
            type_tag,
            short_label,
        };
        let acc = groups.entry(key).or_insert(GroupAccum {
            count: 0,
            forward: 0,
            backward: 0,
            disabled: 0,
        });
        acc.count += 1;
        match dir {
            Direction::Forward => acc.forward += 1,
            Direction::Backward => acc.backward += 1,
            Direction::Both => {
                acc.forward += 1;
                acc.backward += 1;
            }
        }
        if disabled {
            acc.disabled += 1;
        }
    }

    groups
        .into_iter()
        .map(|(key, acc)| {
            let direction = if acc.forward > 0 && acc.backward > 0 {
                Direction::Both
            } else if acc.backward > 0 {
                Direction::Backward
            } else {
                Direction::Forward
            };
            EdgeSummary {
                type_tag: key.type_tag,
                count: acc.count,
                direction,
                disabled_count: acc.disabled,
            }
        })
        .collect()
}

fn classify_edge(edge: &CdfEdge) -> (&'static str, String, bool) {
    match edge {
        CdfEdge::Portal { portal_type, .. } => {
            let short = portal_type
                .strip_prefix("PORTAL_")
                .unwrap_or(portal_type)
                .to_lowercase()
                .replace('_', " ");
            ("portal", short, false)
        }
        CdfEdge::Replication { mode, enabled, .. } => {
            let _short = mode
                .as_deref()
                .and_then(|m| m.strip_prefix("REPLICATION_"))
                .unwrap_or(mode.as_deref().unwrap_or("?"))
                .to_lowercase();
            ("repl", "repl".to_string(), !enabled)
        }
        CdfEdge::ObjectReplication { .. } => ("S3", "s3".to_string(), false),
    }
}

fn build_satellite_entries(edges: &[(String, CdfEdge)]) -> Vec<SatelliteEntry> {
    // Group by target label
    let mut groups: BTreeMap<String, (usize, usize, Direction)> = BTreeMap::new();

    for (label, edge) in edges {
        let disabled = match edge {
            CdfEdge::Replication { enabled, .. } => !enabled,
            _ => false,
        };
        let dir = match edge {
            CdfEdge::ObjectReplication { direction, .. } => {
                match direction.as_deref() {
                    Some("COPY_TO_OBJECT") => Direction::Forward,
                    Some("COPY_FROM_OBJECT") => Direction::Backward,
                    _ => Direction::Forward,
                }
            }
            _ => Direction::Forward,
        };
        let entry = groups.entry(label.clone()).or_insert((0, 0, dir));
        entry.0 += 1;
        if disabled {
            entry.1 += 1;
        }
        // Merge directions
        if entry.2 != dir {
            entry.2 = Direction::Both;
        }
    }

    groups
        .into_iter()
        .map(|(label, (count, disabled, dir))| SatelliteEntry {
            label,
            direction: dir,
            count,
            disabled_count: disabled,
        })
        .collect()
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

fn style_dim() -> Style {
    Style::new().dim()
}

// ── Node labels ─────────────────────────────────────────────────────────────

fn node_label(node: &CdfNode) -> String {
    match node {
        CdfNode::ProfiledCluster { name, .. } => name.clone(),
        CdfNode::UnknownCluster { address, .. } => {
            if address.is_empty() {
                "unknown".to_string()
            } else {
                address.clone()
            }
        }
        CdfNode::S3Bucket { bucket, .. } => bucket.clone(),
    }
}

// ── Rendering ───────────────────────────────────────────────────────────────

fn render_header(out: &mut String, topo: &Topology) {
    let bold = style_bold();
    let title = "═══ Data Fabric Topology ";
    let padding = 75_usize.saturating_sub(title.len());
    out.push_str(&format!(
        "{}\n",
        bold.apply_to(format!("{}{}", title, "═".repeat(padding)))
    ));

    // Summary line
    let mut parts = Vec::new();
    parts.push(format!("{} clusters", topo.cluster_count));
    if topo.s3_count > 0 {
        parts.push(format!("{} S3 buckets", topo.s3_count));
    }
    if topo.remote_peer_count > 0 {
        parts.push(format!("{} remote peers", topo.remote_peer_count));
    }
    out.push_str(&format!("  {}\n", parts.join(" \u{00b7} ")));
}

fn render_single_node(out: &mut String, graph: &CdfGraph) {
    for idx in graph.node_indices() {
        let node = &graph[idx];
        let label = node_label(node);
        out.push_str(&format!(
            "\n  {}  {}\n",
            style_bold().apply_to(&label),
            style_dim().apply_to("(no connections)")
        ));
    }
}

fn render_cluster_connections(out: &mut String, topo: &Topology, detail: bool) {
    if topo.cluster_pairs.is_empty() && topo.isolated_clusters.is_empty() {
        return;
    }

    out.push_str(&format!(
        "\n  {}\n",
        style_bold().apply_to("CLUSTER CONNECTIONS")
    ));
    out.push_str(&format!(
        "  {}\n",
        style_dim().apply_to("─".repeat(67))
    ));

    for bundle in &topo.cluster_pairs {
        render_pair_line(out, bundle, detail);
        out.push('\n');
    }

    // Isolated clusters
    for name in &topo.isolated_clusters {
        out.push_str(&format!(
            "  {}{}(no cluster peers)\n\n",
            style_bold().apply_to(name),
            " ".repeat(50_usize.saturating_sub(name.len())),
        ));
    }
}

fn render_pair_line(out: &mut String, bundle: &ClusterPairBundle, _detail: bool) {
    let a = &bundle.cluster_a;
    let b = &bundle.cluster_b;

    // Determine dominant edge type for line character
    let dominant = dominant_type(&bundle.edge_groups);
    let (line_char, line_style) = match dominant {
        "repl" => ("═", style_replication()),
        "portal" => ("─", style_portal()),
        "S3" => ("╌", style_object()),
        _ => ("┄", style_dim()),
    };

    // Build the connection line
    let name_space: usize = 75 - 4; // 2-char indent each side + margins
    let a_len = a.len();
    let b_len = b.len();
    let line_len = name_space.saturating_sub(a_len + b_len + 2); // +2 for spaces around line
    let line = line_char.repeat(line_len.max(4));

    out.push_str(&format!(
        "  {} {} {}\n",
        style_bold().apply_to(a),
        line_style.apply_to(&line),
        style_bold().apply_to(b),
    ));

    // Annotation line
    let annotation = format_edge_annotation(&bundle.edge_groups);
    let indent = 4;
    out.push_str(&format!("{}{}\n", " ".repeat(indent), annotation));
}

fn dominant_type(groups: &[EdgeSummary]) -> &'static str {
    // Priority: repl > portal > S3
    let mut has_repl = false;
    let mut has_portal = false;
    for g in groups {
        match g.type_tag {
            "repl" => has_repl = true,
            "portal" => has_portal = true,
            _ => {}
        }
    }
    if has_repl {
        "repl"
    } else if has_portal {
        "portal"
    } else if !groups.is_empty() {
        groups[0].type_tag
    } else {
        "repl"
    }
}

fn format_edge_annotation(groups: &[EdgeSummary]) -> String {
    let mut parts = Vec::new();
    for g in groups {
        let mut part = String::new();
        part.push_str(g.type_tag);

        if g.count > 1 {
            part.push_str(&format!(" \u{00d7}{}", g.count));
        }

        let dir_symbol = match g.direction {
            Direction::Forward => " \u{2192}",
            Direction::Backward => " \u{2190}",
            Direction::Both => " \u{2194}",
        };
        part.push_str(dir_symbol);

        if g.disabled_count > 0 {
            if g.disabled_count == g.count {
                part.push_str(&format!(
                    " {}",
                    style_disabled().apply_to("[ALL DISABLED]")
                ));
            } else {
                part.push_str(&format!(
                    " {}",
                    style_disabled()
                        .apply_to(format!("[{} disabled]", g.disabled_count))
                ));
            }
        }
        parts.push(part);
    }
    parts.join(" + ")
}

fn render_s3_section(out: &mut String, topo: &Topology) {
    if topo.s3_groups.is_empty() {
        return;
    }

    out.push_str(&format!(
        "  {}\n",
        style_bold().apply_to("S3 BUCKETS \u{2601}")
    ));
    out.push_str(&format!(
        "  {}\n",
        style_dim().apply_to("─".repeat(67))
    ));

    for group in &topo.s3_groups {
        let cluster = &group.cluster_name;
        let dashes = "╌╌╌╌╌";

        let mut bucket_parts: Vec<String> = Vec::new();
        for sat in &group.satellites {
            let mut part = sat.label.clone();
            let dir = match sat.direction {
                Direction::Forward => "\u{2192}",
                Direction::Backward => "\u{2190}",
                Direction::Both => "\u{2194}",
            };
            if sat.count > 1 {
                part.push_str(&format!(" ({}\u{00d7}{})", dir, sat.count));
            } else {
                part.push_str(&format!(" ({})", dir));
            }
            if sat.disabled_count > 0 {
                part.push_str(" [off]");
            }
            bucket_parts.push(part);
        }

        // Render with wrapping
        let prefix_visible_len = cluster.len() + 2 + dashes.len() + 2;
        let continuation_indent = " ".repeat(prefix_visible_len);

        let joined = bucket_parts.join(", ");
        let max_right = 75 - prefix_visible_len;

        if joined.len() <= max_right {
            out.push_str(&format!(
                "  {} {} {}\n",
                style_bold().apply_to(cluster),
                style_object().apply_to(dashes),
                style_object().apply_to(&joined)
            ));
        } else {
            // Wrap
            let mut lines = Vec::new();
            let mut current_line = String::new();
            for (i, part) in bucket_parts.iter().enumerate() {
                let addition = if current_line.is_empty() {
                    part.clone()
                } else {
                    format!(", {}", part)
                };
                if !current_line.is_empty() && current_line.len() + addition.len() > max_right {
                    lines.push(current_line);
                    current_line = part.clone();
                } else {
                    current_line.push_str(&addition);
                }
                if i == bucket_parts.len() - 1 {
                    lines.push(current_line.clone());
                }
            }

            for (i, line) in lines.iter().enumerate() {
                if i == 0 {
                    out.push_str(&format!(
                        "  {} {} {}\n",
                        style_bold().apply_to(cluster),
                        style_object().apply_to(dashes),
                        style_object().apply_to(line)
                    ));
                } else {
                    out.push_str(&format!(
                        "{}{}\n",
                        continuation_indent,
                        style_object().apply_to(line)
                    ));
                }
            }
        }
    }
    out.push('\n');
}

fn render_remote_peers_section(out: &mut String, topo: &Topology) {
    if topo.remote_peer_groups.is_empty() {
        return;
    }

    out.push_str(&format!(
        "  {}\n",
        style_bold().apply_to("REMOTE PEERS \u{25CB}")
    ));
    out.push_str(&format!(
        "  {}\n",
        style_dim().apply_to("─".repeat(67))
    ));

    for group in &topo.remote_peer_groups {
        let cluster = &group.cluster_name;
        let dashes = "┄┄┄┄┄";

        let mut peer_parts: Vec<String> = Vec::new();
        for sat in &group.satellites {
            let mut part = sat.label.clone();
            if sat.count > 1 {
                let off_note = if sat.disabled_count > 0 {
                    format!(", {} off", sat.disabled_count)
                } else {
                    String::new()
                };
                part.push_str(&format!(" (\u{00d7}{}{})", sat.count, off_note));
            } else if sat.disabled_count > 0 {
                part.push_str(" [off]");
            }
            peer_parts.push(part);
        }

        let prefix_visible_len = cluster.len() + 2 + dashes.len() + 2;
        let continuation_indent = " ".repeat(prefix_visible_len);
        let joined = peer_parts.join(", ");
        let max_right = 75 - prefix_visible_len;

        if joined.len() <= max_right {
            out.push_str(&format!(
                "  {} {} {}\n",
                style_bold().apply_to(cluster),
                style_dim().apply_to(dashes),
                style_dim().apply_to(&joined)
            ));
        } else {
            let mut lines = Vec::new();
            let mut current_line = String::new();
            for (i, part) in peer_parts.iter().enumerate() {
                let addition = if current_line.is_empty() {
                    part.clone()
                } else {
                    format!(", {}", part)
                };
                if !current_line.is_empty() && current_line.len() + addition.len() > max_right {
                    lines.push(current_line);
                    current_line = part.clone();
                } else {
                    current_line.push_str(&addition);
                }
                if i == peer_parts.len() - 1 {
                    lines.push(current_line.clone());
                }
            }

            for (i, line) in lines.iter().enumerate() {
                if i == 0 {
                    out.push_str(&format!(
                        "  {} {} {}\n",
                        style_bold().apply_to(cluster),
                        style_dim().apply_to(dashes),
                        style_dim().apply_to(line)
                    ));
                } else {
                    out.push_str(&format!(
                        "{}{}\n",
                        continuation_indent,
                        style_dim().apply_to(line)
                    ));
                }
            }
        }
    }
    out.push('\n');
}

fn render_legend(out: &mut String, topo: &Topology) {
    let mut parts = Vec::new();
    if topo.has_replication {
        parts.push(format!(
            "{} repl",
            style_replication().apply_to("══")
        ));
    }
    if topo.has_portal {
        parts.push(format!(
            "{} portal",
            style_portal().apply_to("──")
        ));
    }
    if topo.has_s3 {
        parts.push(format!("{} S3", style_object().apply_to("╌╌")));
    }
    if topo.has_remote {
        parts.push(format!("{} peer", style_dim().apply_to("┄┄")));
    }
    parts.push("\u{2192} one-way".to_string());
    parts.push("\u{2194} both".to_string());
    parts.push(format!("{} disabled", style_disabled().apply_to("[off]")));

    out.push_str(&format!("  {}\n", parts.join("   ")));
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
        assert!(plain.contains("1 clusters"));
    }

    #[test]
    fn test_render_compact_mode() {
        let graph = make_test_graph();
        let output = render(&graph, false);
        let plain = strip_ansi(&output);

        // Verify header
        assert!(plain.contains("Data Fabric Topology"));
        assert!(plain.contains("2 clusters"));

        // Verify cluster connection section
        assert!(plain.contains("CLUSTER CONNECTIONS"));
        assert!(plain.contains("gravytrain"));
        assert!(plain.contains("iss"));

        // Verify edge annotations
        assert!(plain.contains("repl"));
        assert!(plain.contains("portal"));

        // Verify S3 section
        assert!(plain.contains("S3 BUCKETS"));
        assert!(plain.contains("backup-bucket"));

        // Verify legend
        assert!(plain.contains("one-way"));
    }

    #[test]
    fn test_render_detail_mode() {
        let graph = make_test_graph();
        let output = render(&graph, true);
        let plain = strip_ansi(&output);

        // Detail mode should still show the topology
        assert!(plain.contains("CLUSTER CONNECTIONS"));
        assert!(plain.contains("gravytrain"));
        assert!(plain.contains("iss"));
        assert!(plain.contains("repl"));
        assert!(plain.contains("portal"));
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
        assert!(plain.contains("REMOTE PEERS"));
        assert!(plain.contains("10.0.0.99"));
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
        assert!(
            plain.contains("DISABLED") || plain.contains("disabled"),
            "Output should contain disabled indicator, got:\n{}",
            plain
        );
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
        assert!(plain.contains("my-bucket"));
        assert!(plain.contains("S3 BUCKETS"));
    }

    #[test]
    fn test_render_output_valid_utf8() {
        let graph = make_test_graph();
        let output = render(&graph, false);
        assert!(!output.is_empty());

        // Verify box-drawing characters
        let plain = strip_ansi(&output);
        let has_box_chars =
            plain.contains('═') || plain.contains('─') || plain.contains('╌') || plain.contains('┄');
        assert!(
            has_box_chars,
            "Output should contain box-drawing characters"
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
        assert!(plain.contains("10.0.0.1") || plain.contains("10.0.0.2"));
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
            "10.0.0.1"
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
            "bkt"
        );
    }

    #[test]
    fn test_render_duplicate_edges_collapsed() {
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

        // Should show "×3" for collapsed edges
        assert!(
            plain.contains("\u{00d7}3"),
            "Should collapse 3 duplicate edges with \u{00d7}3, got:\n{}",
            plain
        );
        // Should show portal
        assert!(plain.contains("portal"));
    }

    #[test]
    fn test_output_width_under_80() {
        let graph = make_test_graph();
        let output = render(&graph, false);
        let plain = strip_ansi(&output);
        for line in plain.lines() {
            let width = console::measure_text_width(line);
            assert!(
                width <= 80,
                "Line exceeds 80 display cols ({} cols): {}",
                width,
                line
            );
        }
    }

    #[test]
    fn test_legend_present() {
        let graph = make_test_graph();
        let output = render(&graph, false);
        let plain = strip_ansi(&output);
        assert!(plain.contains("repl"));
        assert!(plain.contains("portal"));
        assert!(plain.contains("one-way"));
        assert!(plain.contains("both"));
    }

    #[test]
    fn test_isolated_cluster_shown() {
        let mut graph = CdfGraph::new();
        let n1 = graph.add_node(CdfNode::ProfiledCluster {
            name: "connected-a".into(),
            uuid: "uuid-1".into(),
            address: "10.0.0.1".into(),
        });
        let n2 = graph.add_node(CdfNode::ProfiledCluster {
            name: "connected-b".into(),
            uuid: "uuid-2".into(),
            address: "10.0.0.2".into(),
        });
        graph.add_node(CdfNode::ProfiledCluster {
            name: "lonely-cluster".into(),
            uuid: "uuid-3".into(),
            address: "10.0.0.3".into(),
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
        assert!(
            plain.contains("lonely-cluster"),
            "Isolated cluster should be shown, got:\n{}",
            plain
        );
        assert!(plain.contains("no cluster peers"));
    }

    #[test]
    fn test_bidirectional_detection() {
        let mut graph = CdfGraph::new();
        let n1 = graph.add_node(CdfNode::ProfiledCluster {
            name: "alpha".into(),
            uuid: "uuid-1".into(),
            address: "10.0.0.1".into(),
        });
        let n2 = graph.add_node(CdfNode::ProfiledCluster {
            name: "beta".into(),
            uuid: "uuid-2".into(),
            address: "10.0.0.2".into(),
        });
        // Edge from alpha to beta
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
        // Edge from beta to alpha
        graph.add_edge(
            n2,
            n1,
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
        // Should detect bidirectional
        assert!(
            plain.contains("\u{2194}"),
            "Bidirectional edges should show \u{2194}, got:\n{}",
            plain
        );
    }
}
